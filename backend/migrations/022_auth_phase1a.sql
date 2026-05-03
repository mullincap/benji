-- 022_auth_phase1a.sql
-- ===========================================================================
-- Phase 1a auth additions:
--
-- - User profile fields (first_name, last_name, firm, role) populated by
--   the accept-invite flow. All nullable for backward-compat with the two
--   existing rows.
--
-- - first_login flag — drives the welcome onboarding redirect. Defaults
--   true so newly-created users land on /auth/welcome on first sign-in;
--   POST /api/auth/welcome/complete flips it to false. Existing users
--   will see the welcome screen once after deploy (acceptable in v1a).
--
-- - email_verified flag — defaults true for v1a since we have no
--   verification email infra. Phase 1b will flip the default to false
--   and add the verification flow + send.
--
-- - Failed-login lockout (failed_login_count + locked_until). 5 strikes,
--   15-minute lock. Counter resets on a successful login.
--
-- - Invitations table for accept-invite. Tokens are stored as SHA-256
--   hashes; the full 32-byte secrets.token_urlsafe() value only ever
--   exists in the invite URL. Same pattern reset tokens will use in
--   Phase 1b.
-- ===========================================================================

BEGIN;

ALTER TABLE user_mgmt.users
  ADD COLUMN IF NOT EXISTS email_verified     boolean     NOT NULL DEFAULT true,
  ADD COLUMN IF NOT EXISTS first_name         text,
  ADD COLUMN IF NOT EXISTS last_name          text,
  ADD COLUMN IF NOT EXISTS firm               text,
  ADD COLUMN IF NOT EXISTS role               text,
  ADD COLUMN IF NOT EXISTS first_login        boolean     NOT NULL DEFAULT true,
  ADD COLUMN IF NOT EXISTS failed_login_count integer     NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS locked_until       timestamptz;

CREATE TABLE IF NOT EXISTS user_mgmt.invitations (
    invitation_id    uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    token_hash       text        NOT NULL UNIQUE,
    invited_email    text        NOT NULL,
    inviter_user_id  uuid        NOT NULL REFERENCES user_mgmt.users(user_id),
    inviter_name     text        NOT NULL,
    inviter_firm     text        NOT NULL,
    created_at       timestamptz NOT NULL DEFAULT now(),
    expires_at       timestamptz NOT NULL,
    accepted_at      timestamptz,
    accepted_user_id uuid        REFERENCES user_mgmt.users(user_id)
);

CREATE INDEX IF NOT EXISTS idx_invitations_pending_email
  ON user_mgmt.invitations(invited_email)
  WHERE accepted_at IS NULL;

COMMIT;
