-- 025_invitation_suggested_profile.sql
-- ===========================================================================
-- Persist the firm + role values an admin enters when issuing an invitation,
-- so the invitee's accept-form can prefill them. Pre-Phase-1c the admin's
-- /admin/invitations form collected both fields but neither was wired to the
-- database — body.firm and body.role on POST /api/admin/invitations were
-- read but dropped. The form's mental model ("I'm setting up a Trader at
-- Acme Corp") never reached the invitee.
--
-- Both columns are NULLABLE — pre-existing invitation rows weren't issued
-- with suggestions, and the CLI tool (issue_invite.py) doesn't set them
-- either, so a NULL read is the legitimate "no suggestion" path. The
-- accept-form treats null suggested_role as "fall back to Trader" and
-- null suggested_firm as "leave blank" (firm is optional anyway since
-- PR #36).
--
-- Naming: "suggested_*" rather than "invitee_*" to be explicit that these
-- are non-binding hints — the invitee can override either field on the
-- accept form. The accepted values flow into user_mgmt.users.firm /
-- .role exactly as before.
-- ===========================================================================

BEGIN;

ALTER TABLE user_mgmt.invitations
  ADD COLUMN IF NOT EXISTS suggested_firm  text,
  ADD COLUMN IF NOT EXISTS suggested_role  text;

COMMIT;
