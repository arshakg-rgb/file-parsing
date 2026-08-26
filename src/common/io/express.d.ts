import { auth } from "firebase-admin";

declare global {
  namespace Express {
    interface Request {
      /**
       * The decoded Firebase ID token for the authenticated request,
       * attached by the `verifyFirebaseToken` middleware. Undefined on
       * routes that are not protected by that middleware.
       */
      user?: auth.DecodedIdToken;
    }
  }
}

export {};
