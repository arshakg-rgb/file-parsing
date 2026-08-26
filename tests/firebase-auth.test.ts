/**
 * Unit tests for Firebase authentication: FirebaseAuthMiddleware, AuthError
 * status mapping, and FirebaseAuthRouter's session/logout handlers.
 *
 * Run with: npx tsx --test tests/firebase-auth.test.ts
 *
 * FirebaseAdmin.getInstance() is monkey-patched per-test so no real GCP
 * credentials or network calls are required.
 */

import test from "node:test";
import assert from "node:assert/strict";
import { Request, Response, NextFunction } from "express";
import { verifyFirebaseToken } from "@common/middleware/FirebaseAuthMiddleware.js";
import { AuthError } from "@errors/AuthError.js";
import { FirebaseAdmin } from "@config/firebase/FirebaseAdmin.js";
import { FirebaseAuthRouter } from "@service/auth/FirebaseAuthRouter.js";

type Mutable<T> = { -readonly [K in keyof T]: T[K] };

/**
 * Builds a fake Express Request with the given Authorization header value.
 */
function makeReq(authorization?: string): Request
{
  return { headers: { authorization }, user: undefined } as unknown as Request;
}

/**
 * Stubs FirebaseAdmin.getInstance() so `.auth().verifyIdToken(...)` and
 * `.auth().revokeRefreshTokens(...)` resolve/reject as configured. Returns a
 * restore function to put the original implementation back.
 */
function stubFirebaseAdmin(opts: {
  verifyIdToken?: (token: string, checkRevoked?: boolean) => Promise<unknown>;
  revokeRefreshTokens?: (uid: string) => Promise<void>;
}): () => void
{
  const original = FirebaseAdmin.getInstance;

  (FirebaseAdmin as unknown as Mutable<typeof FirebaseAdmin>).getInstance = (() => ({
    auth: () => ({
      verifyIdToken: opts.verifyIdToken ?? (async () => { throw new Error("verifyIdToken not stubbed"); }),
      revokeRefreshTokens: opts.revokeRefreshTokens ?? (async () => { throw new Error("revokeRefreshTokens not stubbed"); }),
    }),
  })) as typeof FirebaseAdmin.getInstance;

  return () => {
    (FirebaseAdmin as unknown as Mutable<typeof FirebaseAdmin>).getInstance = original;
  };
}

/**
 * Runs the middleware and captures whatever `next` was called with.
 */
function runMiddleware(req: Request): Promise<unknown>
{
  return new Promise((resolve) => {
    const next: NextFunction = ((err?: unknown) => resolve(err)) as NextFunction;
    void verifyFirebaseToken(req, {} as Response, next);
  });
}

// ---------------------------------------------------------------------------
// Missing / malformed Authorization header
// ---------------------------------------------------------------------------

test("verifyFirebaseToken: no Authorization header -> TOKEN_MISSING (401)", async () => {
  const err = await runMiddleware(makeReq(undefined)) as AuthError;
  assert.ok(err instanceof AuthError);
  assert.equal(err.code, AuthError.TOKEN_MISSING);
  assert.equal(err.status, 401);
});

test("verifyFirebaseToken: header without 'Bearer ' prefix -> TOKEN_MISSING", async () => {
  const err = await runMiddleware(makeReq("Basic abc123")) as AuthError;
  assert.ok(err instanceof AuthError);
  assert.equal(err.code, AuthError.TOKEN_MISSING);
});

test("verifyFirebaseToken: 'Bearer' with empty/whitespace token -> TOKEN_MISSING", async () => {
  const err = await runMiddleware(makeReq("Bearer    ")) as AuthError;
  assert.ok(err instanceof AuthError);
  assert.equal(err.code, AuthError.TOKEN_MISSING);
});

// ---------------------------------------------------------------------------
// Successful verification
// ---------------------------------------------------------------------------

test("verifyFirebaseToken: valid token -> req.user set, next() called with no error", async () => {
  const decoded = { uid: "user-123", email: "user@example.com" };
  let capturedToken: string | undefined;
  let capturedCheckRevoked: boolean | undefined;

  const restore = stubFirebaseAdmin({
    verifyIdToken: async (token, checkRevoked) => {
      capturedToken = token;
      capturedCheckRevoked = checkRevoked;
      return decoded;
    },
  });

  try
  {
    const req = makeReq("Bearer good-token");
    const result = await runMiddleware(req);

    assert.equal(result, undefined, "next() should be called with no error");
    assert.deepEqual(req.user, decoded);
    assert.equal(capturedToken, "good-token");
    assert.equal(capturedCheckRevoked, true, "verifyIdToken must check revocation");
  }
  finally
  {
    restore();
  }
});

// ---------------------------------------------------------------------------
// Firebase error code mapping
// ---------------------------------------------------------------------------

const errorCodeCases: Array<{ firebaseCode: string; expectedCode: string; expectedStatus: number }> = [
  { firebaseCode: "auth/id-token-expired", expectedCode: AuthError.TOKEN_EXPIRED, expectedStatus: 401 },
  { firebaseCode: "auth/id-token-revoked", expectedCode: AuthError.TOKEN_REVOKED, expectedStatus: 401 },
  { firebaseCode: "auth/user-disabled", expectedCode: AuthError.USER_DISABLED, expectedStatus: 403 },
  { firebaseCode: "auth/argument-error", expectedCode: AuthError.TOKEN_INVALID, expectedStatus: 401 },
  { firebaseCode: "auth/invalid-id-token", expectedCode: AuthError.TOKEN_INVALID, expectedStatus: 401 },
  { firebaseCode: "auth/some-unmapped-code", expectedCode: AuthError.TOKEN_INVALID, expectedStatus: 401 },
];

for (const { firebaseCode, expectedCode, expectedStatus } of errorCodeCases)
{
  test(`verifyFirebaseToken: firebase error '${firebaseCode}' -> ${expectedCode} (${expectedStatus})`, async () => {
    const restore = stubFirebaseAdmin({
      verifyIdToken: async () => {
        const err = new Error(`firebase error ${firebaseCode}`) as Error & { code: string };
        err.code = firebaseCode;
        throw err;
      },
    });

    try
    {
      const err = await runMiddleware(makeReq("Bearer bad-token")) as AuthError;
      assert.ok(err instanceof AuthError);
      assert.equal(err.code, expectedCode);
      assert.equal(err.status, expectedStatus);
    }
    finally
    {
      restore();
    }
  });
}

test("verifyFirebaseToken: error with no .code property -> TOKEN_INVALID", async () => {
  const restore = stubFirebaseAdmin({
    verifyIdToken: async () => { throw new Error("network blew up"); },
  });

  try
  {
    const err = await runMiddleware(makeReq("Bearer bad-token")) as AuthError;
    assert.ok(err instanceof AuthError);
    assert.equal(err.code, AuthError.TOKEN_INVALID);
    assert.equal(err.status, 401);
  }
  finally
  {
    restore();
  }
});

// ---------------------------------------------------------------------------
// AuthError status mapping
// ---------------------------------------------------------------------------

test("AuthError: unknown code falls back to 401", () => {
  const err = new AuthError("SOME_UNKNOWN_CODE", "oops");
  assert.equal(err.status, 401);
});

// ---------------------------------------------------------------------------
// FirebaseAuthRouter handlers (session / logout)
// ---------------------------------------------------------------------------

test("FirebaseAuthRouter.getSession: returns uid/email from req.user", () => {
  const router = FirebaseAuthRouter.getInstance() as unknown as {
    getSession: (req: Request, res: Response) => void;
  };

  const req = { user: { uid: "abc", email: "a@b.com" } } as unknown as Request;
  let jsonBody: unknown;
  const res = { json: (body: unknown) => { jsonBody = body; } } as unknown as Response;

  router.getSession(req, res);

  assert.deepEqual(jsonBody, { success: true, data: { uid: "abc", email: "a@b.com" } });
});

test("FirebaseAuthRouter.logout: revokes refresh tokens for req.user.uid and responds success", async () => {
  const router = FirebaseAuthRouter.getInstance() as unknown as {
    logout: (req: Request, res: Response, next: NextFunction) => Promise<void>;
  };

  let revokedUid: string | undefined;
  const restore = stubFirebaseAdmin({
    revokeRefreshTokens: async (uid) => { revokedUid = uid; },
  });

  try
  {
    const req = { user: { uid: "user-xyz" } } as unknown as Request;
    let jsonBody: unknown;
    const res = { json: (body: unknown) => { jsonBody = body; } } as unknown as Response;
    let nextErr: unknown = "not-called";
    const next: NextFunction = ((err?: unknown) => { nextErr = err; }) as NextFunction;

    await router.logout(req, res, next);

    assert.equal(revokedUid, "user-xyz");
    assert.deepEqual(jsonBody, { success: true, message: "Logged out successfully" });
    assert.equal(nextErr, "not-called", "next() should not be called on success");
  }
  finally
  {
    restore();
  }
});

test("FirebaseAuthRouter.logout: missing req.user -> forwards NOT_AUTHENTICATED to next()", async () => {
  const router = FirebaseAuthRouter.getInstance() as unknown as {
    logout: (req: Request, res: Response, next: NextFunction) => Promise<void>;
  };

  const req = { user: undefined } as unknown as Request;
  const res = { json: () => { throw new Error("res.json should not be called"); } } as unknown as Response;
  let nextErr: unknown;
  const next: NextFunction = ((err?: unknown) => { nextErr = err; }) as NextFunction;

  await router.logout(req, res, next);

  assert.ok(nextErr instanceof AuthError);
  assert.equal((nextErr as AuthError).code, AuthError.NOT_AUTHENTICATED);
});

test("FirebaseAuthRouter.logout: revokeRefreshTokens throws -> error forwarded to next()", async () => {
  const router = FirebaseAuthRouter.getInstance() as unknown as {
    logout: (req: Request, res: Response, next: NextFunction) => Promise<void>;
  };

  const restore = stubFirebaseAdmin({
    revokeRefreshTokens: async () => { throw new Error("gcp is down"); },
  });

  try
  {
    const req = { user: { uid: "user-xyz" } } as unknown as Request;
    const res = { json: () => { throw new Error("res.json should not be called"); } } as unknown as Response;
    let nextErr: unknown;
    const next: NextFunction = ((err?: unknown) => { nextErr = err; }) as NextFunction;

    await router.logout(req, res, next);

    assert.ok(nextErr instanceof Error);
    assert.equal((nextErr as Error).message, "gcp is down");
  }
  finally
  {
    restore();
  }
});
