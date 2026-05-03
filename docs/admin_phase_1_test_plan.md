# Admin Phase 1 — Manual Test Plan

After merging `admin-phase-1` and redeploying frontend + backend (the
latter is required since this PR ships new endpoints), walk this
sequence end-to-end. Order matters — later steps depend on state
created earlier.

Prerequisites:
- Migration 023 already applied (verified at the start of the
  admin-phase-1 build session — see commit `8d3be64` log).
- `j@mullincap.com` is bootstrapped as the first admin (also via 023).

## 1. Admin issues invitation → user accepts

1. Sign in as `j@mullincap.com` at <https://mullincap.com/auth/signin>.
2. Top-bar shows `ADMIN` tab + amber "Admin Mode" pill once you're
   on `/admin/*`. Click `ADMIN` (or ⌘6).
3. Land at `/admin/users`. Click `+ New Invitation`.
4. Fill: email = `phase1-test+invite@example.invalid`,
   firm = `Test Co`, role = `Allocator`, expires = 7 days.
5. Click **Generate Invite Link**. Modal flips to stage 2 with a
   green-bordered URL, auto-copied to clipboard.
6. Open the URL in incognito → fill the accept-invite form → submit.
7. Lands on `/auth/welcome`. Click **Enter platform**.
8. Back in the admin tab, click `Invitations` filter or refresh
   `/admin/invitations` — the row's status should be **Accepted**.
9. Navigate to `/admin/users` — the new user should appear in the
   list. Click the row → user detail page renders.

## 2. Admin resets password → user signs in with temp → sees banner

1. From the user detail page (still on the test user), click **Reset
   Password**. Modal opens at the confirm stage.
2. Click **Generate Temp Password**. Modal flips to stage 2 with the
   temp password (e.g. `slate-Ravine-5875`).
3. Copy the temp password. Close the modal.
4. **Verify in DB** (psql via SSH):
   ```sql
   SELECT password_is_temporary FROM user_mgmt.users
    WHERE email = 'phase1-test+invite@example.invalid';
   -- expect: t
   ```
5. **Verify the admin's own session is unaffected** — refresh any
   admin page; you should still be signed in.
6. **Verify the test user's sessions are wiped** — back in the test
   user's incognito tab, refresh any page → bounces to
   `/auth/signin`.
7. Sign in as the test user with the **temp password**. Lands on
   their default route (allocator/trader overview).
8. Site-wide amber banner reads "You're using a temporary password.
   Change it now →".

## 3. User changes password → banner disappears

1. Click the banner link → lands on `/settings/security`.
2. The page renders WITHOUT a "Current password" field (because
   `password_is_temporary === true`).
3. Enter a new password (12+ chars, mixed case, number, symbol).
   The PasswordStrengthMeter should show "Good" or "Strong".
4. Confirm matches.
5. Click **Update password**. Success state shows. Click **Continue**.
6. Banner is gone site-wide.
7. **Verify in DB**:
   ```sql
   SELECT password_is_temporary, password_changed_by IS NOT NULL AS by_admin
   FROM user_mgmt.users WHERE email = 'phase1-test+invite@example.invalid';
   -- expect: f | f   (self-change clears password_changed_by)
   ```

## 4. Admin locks user → next login returns 423

1. Back in the admin tab → user detail for the test user.
2. Click **Lock Account (24h)** → confirm.
3. Toast: "Account locked for 24 hours." Identity card should
   refresh; the action button now reads **Unlock Account**.
4. Sign out the test user (top-bar SIGN OUT). Try to sign in
   again with their (newly-changed) password → response should be
   HTTP 423 with a "too many failed attempts" / lock banner.
5. Curl-equivalent for the UI-skeptic:
   ```bash
   curl -i -X POST https://mullincap.com/api/auth/login \
     -H 'Content-Type: application/json' \
     -d '{"email":"phase1-test+invite@example.invalid","password":"<new-password>"}'
   # expect: HTTP/1.1 423 Locked
   ```
6. Back in the admin tab, click **Unlock Account** → confirm. Test
   user can now sign in normally.

## 5. Non-admin navigates to /admin/users → bounced

1. Sign out as admin. Sign in as the test user (who is NOT admin).
2. Manually navigate to `https://mullincap.com/admin/users`.
3. The admin layout should bounce to `/` (the middleware passes the
   cookie-present user through; the layout's `is_admin` check denies).
4. Verify there's no flash of admin content during the bounce.
5. **Audit row check**:
   ```sql
   SELECT action_type, action_metadata->>'path', created_at
     FROM user_mgmt.admin_actions
    WHERE action_type = 'admin_login_attempt_denied'
    ORDER BY created_at DESC LIMIT 3;
   -- expect: a row with path='/admin/users' from this attempt
   ```

## 6. Audit log shows everything

1. Sign back in as admin. Navigate to `/admin/audit`.
2. Filter to "All Actions" (default). Recent rows should include:
   - `Admin Denied` — from step 5
   - `Account Unlocked` / `Account Locked` — from step 4
   - `Password Changed` (self-initiated) — from step 3
   - `Password Reset` (admin-issued) — from step 2
   - `Invite Sent` — from step 1
3. Click the "Resets" filter pill → only password reset rows.
4. Search for the test user's email → narrows to their lifecycle.
5. Click "Last 24h" → confirms the time filter works.

## Cleanup

```sql
-- run as admin/ops on prod psql
DELETE FROM user_mgmt.invitations
 WHERE invited_email LIKE 'phase1-test+%@example.invalid';
DELETE FROM user_mgmt.users
 WHERE email LIKE 'phase1-test+%@example.invalid';
-- expect: DELETE 1 / DELETE 1

-- audit_actions rows referencing the test user are intentionally
-- preserved (they reference admin_user_id = J's row, which exists)
-- and represent legitimate audit history.
```

## Known follow-ups (deferred)

- **`last_ip` on user detail** — `user_sessions` doesn't carry IP
  today; needs migration 024 + login-side INSERT change.
- **Sessions tab IP/UA** — same root cause as above.
- **`/settings/` shell** — `/settings/security` is currently a
  standalone route. When/if a unified `/settings/` shell ships,
  fold this page under it.
- **"Copy Link" on pending invites** — not implementable while
  tokens are SHA-256 hashed at storage; revoke + reissue is the
  pattern. UI surfaces "Why no link?" on accepted/expired rows.
- **Suspend/reactivate** — out of scope per Phase 1 spec; use
  `is_active = false` via psql until 1.5.
- **Impersonate** — out of scope per Phase 1 spec.
