# Credentials

Account tokens are the only long-lived secrets AutoClaimer handles. They are
stored **Fernet-encrypted** in the shared `state.db` (table
`account_credentials`, schema v5) and never exist in plaintext in the
database, configuration files, logs, or command lines.

## Key setup (first time)

1. In the CLI, run:

   ```
   credential generate-key
   ```

   This prints one fresh Fernet key. It never writes, never overwrites, and
   never invalidates anything — losing the *active* key makes existing
   credentials unrecoverable, so treat it carefully.

2. Store the key **outside the database** — a secret manager or a
   restricted-permission environment file. Export it as:

   ```
   AUTOCLAIMER_FERNET_KEY=<key>
   ```

3. Restart the application. Credential commands are enabled; without a valid
   key they fail closed (credentials are simply unavailable — there is no
   plaintext fallback and no automatic key generation).

## Storing and managing tokens

Interactive CLI commands (the database lives next to `config.json`):

| Command | Effect |
|---|---|
| `credential set <account_id>` | Prompts for the token (**hidden input** via getpass; never a command argument, so it stays out of shell history), encrypts it with the primary key, and upserts the row. |
| `credential has <account_id>` | Whether a credential is stored (no decryption). |
| `credential delete <account_id>` | Explicitly deletes the stored token. |
| `credential accounts` | Lists accounts with stored (encrypted) credentials. |
| `credential rotate` | Re-encrypts all credentials under the primary key (below). |
| `credential export <path> confirm` | Writes all credentials to a JSON **ciphertext bundle** (format v2: each record's ciphertext AUTHENTICATES the account id together with the token — editing the bundle's JSON account mapping is rejected at import); never plaintext; the explicit `confirm` argument prevents accidents. Export writes atomically (temp + rename) and filesystem failures are typed CLI errors. |
| `credential import <path> confirm` | Restores from a bundle: every record is validated by decrypting it with the configured keys and checking its authenticated account id against the metadata **before anything is written** (non-matching keys, tampered account mappings, and duplicate account ids are rejected whole with zero rows written), then re-encrypted under the current primary key on worker threads (the event loop never blocks on SQLite). Each upsert's read and write share one transaction, so a concurrent update is never silently discarded mid-import — the restore is authoritative for its accounts; a later `credential set` wins. Idempotent. v1 bundles (pre-merge, unauthenticated metadata) are rejected with instructions to re-export.

  **Restore contract:** validation is all-or-nothing (malformed, wrong-key, tampered, duplicate, or empty-token records reject the whole bundle with zero rows written). Storage failures during phase-2 writes are **per-record resumable**: a database error after some rows have committed leaves the committed prefix; retrying the same bundle converges to the full restore (already-restored rows are skipped). |

Without a controlling terminal, token entry falls back to echoed input **with
an explicit warning** — piped/automated use stays possible, visibly.

How tokens are used: the Discord adapter requests each account's token at
login through the `CredentialProvider` boundary. Task handlers never see
keys, ciphertext, or credential rows. A missing or undecryptable credential
is an **account-fatal** error: that account stops, others continue. The
message never contains the token, ciphertext, or key material.

## Key rotation

Rotation is implemented and tested, including interrupted-and-resumed
rotation.

1. Generate a new key (`credential generate-key`).
2. Set the environment:
   - `AUTOCLAIMER_FERNET_KEY=<new key>`
   - `AUTOCLAIMER_FERNET_RETIRED_KEYS=<old key>` (comma-separated if several)
3. Restart, then run:

   ```
   credential rotate
   ```

Each row is decrypted (any loaded key), re-encrypted under the new primary,
**verified by decrypting the new ciphertext back**, and committed
row-by-row — an interruption is safely resumable, and rows not yet migrated
stay decryptable via the retired key. Rotation reports the number of rows it
re-encrypted; it is complete when a re-run reports `0`. Only then may the old
key be discarded. Rotation fails loudly (typed error) if a row cannot be
decrypted — it never deletes or overwrites credentials it cannot read.

## Backups

- Back up **`state.db` and the key material together**. A database without
  its key is unrecoverable ciphertext; a key without the database is useless.
- Store both with equivalent secrecy (the backup of an encrypted store is
  only as safe as the key backup).
- `VACUUM INTO`/file-copy backups are safe while the application is stopped;
  for hot backups use SQLite's backup API or the maintenance procedures
  planned in the [roadmap](../ROADMAP.md) (SQLite maintenance, item 14).

## Failure modes (fail-closed)

| Situation | Behavior |
|---|---|
| `AUTOCLAIMER_FERNET_KEY` unset/invalid | Credential store unavailable; credential commands explain; no fallback. |
| Wrong key configured | Existing credentials fail decryption with a typed, secret-free error (account-fatal at login). |
| Corrupted ciphertext | Typed `CredentialDecryptError` identifying the account — never a raw crash. |
| Token updated during rotation | Preserved — rotation's read and write share one transaction; a concurrent update can never be reverted. |

## Key loss, replacement, and recovery

### What depends on the key — and what does not

| Data | Key needed? | Survives key loss? |
|---|---|---|
| Account tokens (`account_credentials`) | yes — encrypted with it | **No.** Without the key the ciphertext is cryptographically unrecoverable (Fernet is authenticated encryption; there is no bypass, by design). |
| Task instances, retry schedules | no | Yes. |
| Cooldowns, learned Mudae windows | no | Yes. |
| Configuration files | no | Yes. |

Losing `AUTOCLAIMER_FERNET_KEY` therefore never endangers scheduling state —
only the stored tokens become dead ciphertext.

### Consequences of losing the key

- Existing tokens cannot be decrypted by anyone, including this codebase.
- Login for affected accounts fails account-fatal (typed, secret-free
  errors) until new tokens are registered.
- The dead ciphertext rows remain inspectable (`credential accounts`) and
  removable (`credential delete`) but never decryptable.

### Replacing a lost key (recovery procedure)

1. Generate a new key: `credential generate-key` (display-only; never
   overwrites anything).
2. Store it securely (below) and set `AUTOCLAIMER_FERNET_KEY` to it.
3. Restart. Do **not** list the old key as retired — it is useless now and
   would only mask the failure.
4. Re-register each account's token: `credential set <account_id>` per
   account (get fresh tokens from Discord as you would for a new setup —
   the old ones are unrecoverable, though they may still be valid).
5. Remove the dead rows if you want a clean slate: `credential delete
   <account_id>` (optional; upserting via `credential set` replaces them
   anyway).
6. `credential accounts` / `credential has <id>` to verify.

### Restoring from a backup (preferred, when one exists)

See [backup and restore](#backup-and-restore) — a backup pairs `state.db`
(or an exported ciphertext bundle) with the key that encrypted it.
Restoring the pair recovers access without re-registering tokens.

### What can and cannot be recovered

- **Recoverable with a backup (db + key, or export + key):** all tokens.
- **Recoverable without any backup:** everything except tokens — and
  tokens simply re-register (`credential set`); nothing else needs action.
- **Never recoverable:** tokens whose key is lost and for which no
  ciphertext backup + key pair exists.

These instructions never require — and these docs never contain —
plaintext credentials: every artifact handled above is ciphertext, a key,
or metadata.

### Secure key storage and backup recommendations

- Store the key in a secret manager or a root-owned file with `0400`
  permissions; never in `config.json`, `accounts.json`, the database,
  logs, screenshots, or chat.
- Back up the key separately from the database but with equivalent
  secrecy — an encrypted database backed up next to its key is only as
  protected as that location.
- Two independent key copies (e.g., password manager + offline medium)
  is the practical minimum; test that a restore actually works (item 4's
  `credential export`/`import` round-trip in a scratch environment).

## Backup and restore

`credential export` / `credential import` move **ciphertext only** —
plaintext tokens are never exported, by design. Both commands require an
explicit `confirm` argument so they can never run by accident. Full
reference and tested procedures: see the restore section above and the
command table at the top of this page.

Key/backup compatibility: an export is only usable together with a key
that can decrypt it — the primary key at export time or any retired key
still configured. Importing validates every record by decrypting it
**before** writing anything; a bundle that does not match the configured
keys is rejected whole, with a typed error and no partial import.
