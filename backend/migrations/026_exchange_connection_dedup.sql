-- 026_exchange_connection_dedup.sql
-- ===========================================================================
-- Prevent the same user from accidentally adding the same exchange API key
-- twice. Bug found in prod: a user added the same Binance key twice via the
-- link wizard, dashboard then double-counted balance from both connection
-- rows.
--
-- Constraint shape: (user_id, exchange, api_key_hash) UNIQUE among non-
-- revoked connections. Per-user partition is intentional — different users
-- (e.g. J's test accounts, firms sharing accounts across operators) can
-- legitimately link the same exchange API key. The bug is the SAME user
-- adding the SAME key twice; this migration fixes that case only.
--
-- Why a hash column rather than GENERATED ALWAYS on api_key_enc:
-- exchange_connections.api_key_enc is a Fernet ciphertext, and Fernet is
-- non-deterministic — encrypting the same plaintext twice produces
-- different ciphertexts. Hashing the ciphertext therefore would NOT catch
-- duplicates. The hash must come from the plaintext, which only the
-- application has at INSERT time. New schema column populated by the
-- application before the row is INSERT'ed.
--
-- Pre-existing rows have NULL api_key_hash. They aren't covered by the
-- partial unique index until backfilled. The accompanying CLI script
-- backend/app/cli/dedupe_exchange_connections.py decrypts existing rows
-- to populate api_key_hash AND marks any duplicates as 'revoked'. Run
-- it once after applying this migration; the index becomes fully
-- effective on existing data after that.
-- ===========================================================================

BEGIN;

ALTER TABLE user_mgmt.exchange_connections
  ADD COLUMN IF NOT EXISTS api_key_hash text;

-- Partial unique index. Three filters intentionally:
--   api_key_hash IS NOT NULL  → excludes pre-backfill rows so the
--                                migration itself can't fail on existing
--                                duplicates; CLI script handles those.
--   status != 'revoked'       → revoked rows shouldn't block re-linking
--                                a previously-removed connection.
--   user_id partition         → cross-user key reuse stays legal.
CREATE UNIQUE INDEX IF NOT EXISTS exchange_connections_user_exchange_keyhash_uniq
  ON user_mgmt.exchange_connections (user_id, exchange, api_key_hash)
  WHERE api_key_hash IS NOT NULL AND status != 'revoked';

COMMIT;
