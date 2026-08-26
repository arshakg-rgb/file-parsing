/**
 * Unit tests for FirebaseAuthClient (signup/login/refresh REST proxy) and
 * the corresponding FirebaseAuthRouter public handlers.
 *
 * Run with: npx tsx --test tests/firebase-auth-signup-login.test.ts
 *
 * `global.fetch` is stubbed per-test so no real network calls to Firebase's
 * Identity Toolkit are made. `settings.FIREBASE_WEB_API_KEY` is stubbed to
 * a dummy value so `FirebaseAuthClient` doesn't throw on missing config.
 */

import test from "node:test";
import assert from "node:assert/strict";
import { Request, Response as ExpressResponse, NextFunction } from "express";
import { FirebaseAuthClient } from "@config/firebase/FirebaseAuthClient.js";
import { AuthError } from "@errors/AuthError.js";
import { ValidationError } from "@errors/ValidationError.js";
import { settings } from "@shared/Settings.js";
import { FirebaseAuthRouter } from "@service/auth/FirebaseAuthRouter.js";

type MutableSettings = { -readonly [K in keyof typeof settings]: (typeof settings)[K] };

(settings as unknown as MutableSettings).FIREBASE_WEB_API_KEY = "test-api-key";

/**
 * Builds a fake `fetch` Response object.
 */
function fakeResponse(ok: boolean, body: unknown): globalThis.Response
{
  return { ok, json: async () => body } as unknown as globalThis.Response;
}

/**
 * Stubs `global.fetch` for the duration of a test. Returns a restore function.
 */
function stubFetch(handler: (url: string, init?: RequestInit) => globalThis.Response): () => void
{
  const original = global.fetch;
  global.fetch = (async (url: string, init?: RequestInit) => handler(url, init)) as typeof fetch;
  return () => { global.fetch = original; };
}

// ---------------------------------------------------------------------------
// FirebaseAuthClient.signUp
// ---------------------------------------------------------------------------

test("FirebaseAuthClient.signUp: success returns token pair", async () => {
  let capturedUrl = "";
  let capturedBody: Record<string, unknown> = {};

  const restore = stubFetch((url, init) => {
    capturedUrl = url;
    capturedBody = JSON.parse(init!.body as string);
    return fakeResponse(true, {
      idToken: "id-token-1", refreshToken: "refresh-1", expiresIn: "3600", localId: "uid-1", email: "a@b.com",
    });
  });

  try
  {
    const result = await FirebaseAuthClient.getInstance().signUp("a@b.com", "password123");
    assert.equal(result.idToken, "id-token-1");
    assert.equal(result.localId, "uid-1");
    assert.ok(capturedUrl.includes("accounts:signUp"));
    assert.ok(capturedUrl.includes("key=test-api-key"));
    assert.deepEqual(capturedBody, { email: "a@b.com", password: "password123", returnSecureToken: true });
  }
  finally
  {
    restore();
  }
});

test("FirebaseAuthClient.signUp: EMAIL_EXISTS -> AuthError 409", async () => {
  const restore = stubFetch(() => fakeResponse(false, { error: { code: 400, message: "EMAIL_EXISTS" } }));

  try
  {
    await assert.rejects(
      () => FirebaseAuthClient.getInstance().signUp("dup@b.com", "password123"),
      (err: unknown) => {
        assert.ok(err instanceof AuthError);
        assert.equal((err as AuthError).code, AuthError.EMAIL_EXISTS);
        assert.equal((err as AuthError).status, 409);
        return true;
      }
    );
  }
  finally
  {
    restore();
  }
});

test("FirebaseAuthClient.signUp: WEAK_PASSWORD -> AuthError 400", async () => {
  const restore = stubFetch(() => fakeResponse(false, {
    error: { code: 400, message: "WEAK_PASSWORD : Password should be at least 6 characters" },
  }));

  try
  {
    await assert.rejects(
      () => FirebaseAuthClient.getInstance().signUp("a@b.com", "123"),
      (err: unknown) => {
        assert.ok(err instanceof AuthError);
        assert.equal((err as AuthError).code, AuthError.WEAK_PASSWORD);
        assert.equal((err as AuthError).status, 400);
        return true;
      }
    );
  }
  finally
  {
    restore();
  }
});

// ---------------------------------------------------------------------------
// FirebaseAuthClient.signInWithPassword
// ---------------------------------------------------------------------------

const loginErrorCases: Array<{ message: string; expectedCode: string; expectedStatus: number }> = [
  { message: "EMAIL_NOT_FOUND", expectedCode: AuthError.INVALID_CREDENTIALS, expectedStatus: 401 },
  { message: "INVALID_PASSWORD", expectedCode: AuthError.INVALID_CREDENTIALS, expectedStatus: 401 },
  { message: "INVALID_LOGIN_CREDENTIALS", expectedCode: AuthError.INVALID_CREDENTIALS, expectedStatus: 401 },
  { message: "USER_DISABLED", expectedCode: AuthError.USER_DISABLED, expectedStatus: 403 },
  { message: "TOO_MANY_ATTEMPTS_TRY_LATER", expectedCode: AuthError.TOO_MANY_REQUESTS, expectedStatus: 429 },
  { message: "INVALID_EMAIL", expectedCode: AuthError.INVALID_EMAIL, expectedStatus: 400 },
];

for (const { message, expectedCode, expectedStatus } of loginErrorCases)
{
  test(`FirebaseAuthClient.signInWithPassword: '${message}' -> ${expectedCode} (${expectedStatus})`, async () => {
    const restore = stubFetch(() => fakeResponse(false, { error: { code: 400, message } }));

    try
    {
      await assert.rejects(
        () => FirebaseAuthClient.getInstance().signInWithPassword("a@b.com", "wrong"),
        (err: unknown) => {
          assert.ok(err instanceof AuthError);
          assert.equal((err as AuthError).code, expectedCode);
          assert.equal((err as AuthError).status, expectedStatus);
          return true;
        }
      );
    }
    finally
    {
      restore();
    }
  });
}

test("FirebaseAuthClient.signInWithPassword: success returns token pair", async () => {
  const restore = stubFetch(() => fakeResponse(true, {
    idToken: "id-2", refreshToken: "refresh-2", expiresIn: "3600", localId: "uid-2", email: "a@b.com",
  }));

  try
  {
    const result = await FirebaseAuthClient.getInstance().signInWithPassword("a@b.com", "password123");
    assert.equal(result.idToken, "id-2");
  }
  finally
  {
    restore();
  }
});

// ---------------------------------------------------------------------------
// FirebaseAuthClient.refresh
// ---------------------------------------------------------------------------

test("FirebaseAuthClient.refresh: success maps snake_case fields", async () => {
  const restore = stubFetch(() => fakeResponse(true, {
    id_token: "id-3", refresh_token: "refresh-3", expires_in: "3600", user_id: "uid-3",
  }));

  try
  {
    const result = await FirebaseAuthClient.getInstance().refresh("old-refresh-token");
    assert.equal(result.idToken, "id-3");
    assert.equal(result.refreshToken, "refresh-3");
    assert.equal(result.localId, "uid-3");
  }
  finally
  {
    restore();
  }
});

test("FirebaseAuthClient.refresh: INVALID_REFRESH_TOKEN -> AuthError TOKEN_INVALID / 401", async () => {
  const restore = stubFetch(() => fakeResponse(false, { error: { code: 400, message: "INVALID_REFRESH_TOKEN" } }));

  try
  {
    await assert.rejects(
      () => FirebaseAuthClient.getInstance().refresh("bad-refresh-token"),
      (err: unknown) => {
        assert.ok(err instanceof AuthError);
        assert.equal((err as AuthError).code, AuthError.TOKEN_INVALID);
        assert.equal((err as AuthError).status, 401);
        return true;
      }
    );
  }
  finally
  {
    restore();
  }
});

// ---------------------------------------------------------------------------
// FirebaseAuthRouter public handlers (signup / login / refresh)
// ---------------------------------------------------------------------------

/**
 * Runs a router handler and captures the response / forwarded error.
 */
async function runHandler(
  handler: (req: Request, res: ExpressResponse, next: NextFunction) => Promise<void>,
  body: unknown
): Promise<{ status: number; jsonBody: unknown; nextErr: unknown }>
{
  let status = 200;
  let jsonBody: unknown;
  let nextErr: unknown = "not-called";

  const req = { body } as unknown as Request;
  const res = {
    status: (code: number) => { status = code; return res; },
    json: (b: unknown) => { jsonBody = b; },
  } as unknown as ExpressResponse;
  const next: NextFunction = ((err?: unknown) => { nextErr = err; }) as NextFunction;

  await handler(req, res, next);

  return { status, jsonBody, nextErr };
}

test("FirebaseAuthRouter.signup: missing password -> ValidationError forwarded, no response sent", async () => {
  const router = FirebaseAuthRouter.getInstance() as unknown as {
    signup: (req: Request, res: ExpressResponse, next: NextFunction) => Promise<void>;
  };

  const { nextErr } = await runHandler(router.signup, { email: "a@b.com" });

  assert.ok(nextErr instanceof ValidationError);
  assert.equal((nextErr as ValidationError).code, ValidationError.INPUT);
});

test("FirebaseAuthRouter.signup: valid body -> 201 with token payload", async () => {
  const restore = stubFetch(() => fakeResponse(true, {
    idToken: "id-4", refreshToken: "refresh-4", expiresIn: "3600", localId: "uid-4", email: "new@b.com",
  }));

  try
  {
    const router = FirebaseAuthRouter.getInstance() as unknown as {
      signup: (req: Request, res: ExpressResponse, next: NextFunction) => Promise<void>;
    };

    const { status, jsonBody, nextErr } = await runHandler(router.signup, { email: "new@b.com", password: "password123" });

    assert.equal(status, 201);
    assert.equal(nextErr, "not-called");
    assert.deepEqual(jsonBody, {
      success: true,
      data: { uid: "uid-4", email: "new@b.com", idToken: "id-4", refreshToken: "refresh-4", expiresIn: "3600" },
    });
  }
  finally
  {
    restore();
  }
});

test("FirebaseAuthRouter.login: wrong password -> INVALID_CREDENTIALS forwarded to next()", async () => {
  const restore = stubFetch(() => fakeResponse(false, { error: { code: 400, message: "INVALID_PASSWORD" } }));

  try
  {
    const router = FirebaseAuthRouter.getInstance() as unknown as {
      login: (req: Request, res: ExpressResponse, next: NextFunction) => Promise<void>;
    };

    const { nextErr } = await runHandler(router.login, { email: "a@b.com", password: "wrong" });

    assert.ok(nextErr instanceof AuthError);
    assert.equal((nextErr as AuthError).code, AuthError.INVALID_CREDENTIALS);
    assert.equal((nextErr as AuthError).status, 401);
  }
  finally
  {
    restore();
  }
});

test("FirebaseAuthRouter.login: valid credentials -> 200 with token payload", async () => {
  const restore = stubFetch(() => fakeResponse(true, {
    idToken: "id-5", refreshToken: "refresh-5", expiresIn: "3600", localId: "uid-5", email: "a@b.com",
  }));

  try
  {
    const router = FirebaseAuthRouter.getInstance() as unknown as {
      login: (req: Request, res: ExpressResponse, next: NextFunction) => Promise<void>;
    };

    const { status, jsonBody, nextErr } = await runHandler(router.login, { email: "a@b.com", password: "password123" });

    assert.equal(status, 200);
    assert.equal(nextErr, "not-called");
    assert.deepEqual(jsonBody, {
      success: true,
      data: { uid: "uid-5", email: "a@b.com", idToken: "id-5", refreshToken: "refresh-5", expiresIn: "3600" },
    });
  }
  finally
  {
    restore();
  }
});

test("FirebaseAuthRouter.refreshToken: missing refreshToken -> ValidationError forwarded", async () => {
  const router = FirebaseAuthRouter.getInstance() as unknown as {
    refreshToken: (req: Request, res: ExpressResponse, next: NextFunction) => Promise<void>;
  };

  const { nextErr } = await runHandler(router.refreshToken, {});

  assert.ok(nextErr instanceof ValidationError);
});

test("FirebaseAuthRouter.refreshToken: valid refresh token -> 200 with new token pair", async () => {
  const restore = stubFetch(() => fakeResponse(true, {
    id_token: "id-6", refresh_token: "refresh-6", expires_in: "3600", user_id: "uid-6",
  }));

  try
  {
    const router = FirebaseAuthRouter.getInstance() as unknown as {
      refreshToken: (req: Request, res: ExpressResponse, next: NextFunction) => Promise<void>;
    };

    const { status, jsonBody, nextErr } = await runHandler(router.refreshToken, { refreshToken: "old-refresh" });

    assert.equal(status, 200);
    assert.equal(nextErr, "not-called");
    assert.deepEqual(jsonBody, {
      success: true,
      data: { uid: "uid-6", email: undefined, idToken: "id-6", refreshToken: "refresh-6", expiresIn: "3600" },
    });
  }
  finally
  {
    restore();
  }
});
