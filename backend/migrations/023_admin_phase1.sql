-- 023_admin_phase1.sql
-- ===========================================================================
-- Admin module Phase 1.
--
-- Adds:
--   user_mgmt.users
--     + is_admin                — gate for /api/admin/* and /admin/* routes
--     + password_is_temporary   — true when admin issued a temp password,
--                                 drives the site-wide amber banner until
--                                 the user changes it via /api/auth/change-password
--     + password_set_at         — timestamp of the most recent password set
--     + password_changed_by     — admin user_id when the last change was an
--                                 admin reset; NULL when self-changed
--   user_mgmt.admin_actions     — append-only audit log (full table)
--
-- Bootstraps j@mullincap.com as the first admin via a DO block that
-- raises if the user row doesn't exist — safer than a silent no-op
-- UPDATE.
-- ===========================================================================

BEGIN;

ALTER TABLE user_mgmt.users
  ADD COLUMN IF NOT EXISTS is_admin              boolean     NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS password_is_temporary boolean     NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS password_set_at       timestamptz NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS password_changed_by   uuid        REFERENCES user_mgmt.users(user_id);

CREATE TABLE IF NOT EXISTS user_mgmt.admin_actions (
  action_id        uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  admin_user_id    uuid        NOT NULL REFERENCES user_mgmt.users(user_id),
  subject_user_id  uuid        REFERENCES user_mgmt.users(user_id),
  action_type      text        NOT NULL,
  action_metadata  jsonb       NOT NULL DEFAULT '{}'::jsonb,
  ip_address       inet,
  created_at       timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_admin_actions_admin_time
  ON user_mgmt.admin_actions (admin_user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_admin_actions_subject_time
  ON user_mgmt.admin_actions (subject_user_id, created_at DESC)
  WHERE subject_user_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_admin_actions_type_time
  ON user_mgmt.admin_actions (action_type, created_at DESC);

-- Bootstrap j@mullincap.com as the first admin. Fail loudly if the row
-- doesn't exist — silent no-op would ship an admin module with no
-- admins and no obvious symptom.
DO $$
BEGIN
  UPDATE user_mgmt.users SET is_admin = TRUE WHERE email = 'j@mullincap.com';
  IF NOT FOUND THEN
    RAISE EXCEPTION 'Bootstrap admin user j@mullincap.com not found in user_mgmt.users';
  END IF;
END $$;

COMMIT;
