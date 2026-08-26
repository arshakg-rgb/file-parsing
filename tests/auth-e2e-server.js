/**
 * Standalone end-to-end auth server for manual API testing.
 *
 * It patches `FirebaseAdmin.getInstance()` so no real GCP credentials are
 * required. Any token equal to "valid-token" is accepted; anything else is
 * rejected with `auth/invalid-id-token`.
 *
 * Run: node tests/auth-e2e-server.js
 * Endpoints:
 *   POST /v1/auth/signup  (public)  { email, password }
 *   POST /v1/auth/login   (public)  { email, password }
 *   POST /v1/auth/refresh (public)  { refreshToken }
 *   GET  /v1/auth/session (protected)
 *   POST /v1/auth/logout  (protected)
 *   GET  /v1/jobs         (protected)
 *
 * Note: signup/login/refresh are backed by an in-memory fake user store
 * (FirebaseAuthClient mock) that always issues the literal token
 * "valid-token", which FirebaseAdmin's mock always resolves to
 * uid "user-123" -- fine for exercising the routes/status codes, but not a
 * real multi-user session model.
 */

const { FirebaseAdmin } = require("../dist/src/config/firebase/FirebaseAdmin.js");
const { FirebaseAuthClient } = require("../dist/src/config/firebase/FirebaseAuthClient.js");
const { verifyFirebaseToken } = require("../dist/src/common/middleware/FirebaseAuthMiddleware.js");
const { FirebaseAuthRouter } = require("../dist/src/services/auth/FirebaseAuthRouter.js");
const express = require("express");

// In-memory fake user store so POST /v1/auth/signup + /v1/auth/login work
// end-to-end without calling the real Identity Toolkit REST API.
const fakeUsers = new Map();
let nextUid = 1;

FirebaseAuthClient.getInstance = function () {
  return {
    signUp: async (email, password) => {
      if (fakeUsers.has(email)) {
        const err = new Error("EMAIL_EXISTS");
        err.code = "EMAIL_EXISTS";
        // Mirror the real AuthError the router expects
        const { AuthError } = require("../dist/src/errors/AuthError.js");
        throw new AuthError(AuthError.EMAIL_EXISTS, "An account with this email already exists.");
      }
      if (password.length < 6) {
        const { AuthError } = require("../dist/src/errors/AuthError.js");
        throw new AuthError(AuthError.WEAK_PASSWORD, "Password should be at least 6 characters.");
      }
      const uid = `uid-${nextUid++}`;
      fakeUsers.set(email, { uid, password });
      return { idToken: "valid-token", refreshToken: `refresh-${uid}`, expiresIn: "3600", localId: uid, email };
    },
    signInWithPassword: async (email, password) => {
      const user = fakeUsers.get(email);
      const { AuthError } = require("../dist/src/errors/AuthError.js");
      if (!user || user.password !== password) {
        throw new AuthError(AuthError.INVALID_CREDENTIALS, "Invalid email or password.");
      }
      return { idToken: "valid-token", refreshToken: `refresh-${user.uid}`, expiresIn: "3600", localId: user.uid, email };
    },
    refresh: async (refreshToken) => {
      const { AuthError } = require("../dist/src/errors/AuthError.js");
      const entry = [...fakeUsers.entries()].find(([, u]) => `refresh-${u.uid}` === refreshToken);
      if (!entry) {
        throw new AuthError(AuthError.TOKEN_INVALID, "Refresh token is invalid or expired. Please log in again.");
      }
      const [email, user] = entry;
      return { idToken: "valid-token", refreshToken, expiresIn: "3600", localId: user.uid, email };
    },
  };
};

FirebaseAdmin.getInstance = function () {
  return {
    auth: function () {
      return {
        verifyIdToken: async function (token) {
          if (token === "valid-token") {
            return { uid: "user-123", email: "test@example.com" };
          }
          if (token === "expired-token") {
            const err = new Error("token expired");
            err.code = "auth/id-token-expired";
            throw err;
          }
          const err = new Error("invalid token");
          err.code = "auth/invalid-id-token";
          throw err;
        },
        revokeRefreshTokens: async function (uid) {
          console.log(`revoked refresh tokens for uid=${uid}`);
        },
      };
    },
  };
};

const app = express();

app.get("/health", (_req, res) => {
  res.json({ status: "healthy", timestamp: new Date().toISOString() });
});

app.use(express.json());
app.use("/v1", FirebaseAuthRouter.getInstance().getRouter());

app.get("/v1/jobs", verifyFirebaseToken, (req, res) => {
  res.json({
    success: true,
    jobs: [],
    user: { uid: req.user.uid, email: req.user.email },
  });
});

app.use((err, _req, res, _next) => {
  res.status(err.status || 500).json({ detail: err.message });
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
  console.log(`Auth e2e server listening on http://localhost:${PORT}`);
});
