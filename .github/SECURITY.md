# Security Policy

## Reporting a vulnerability or a data concern

**Do not** open a public GitHub issue for security problems or for credentials or personal data you find in this repository.

Instead, email support@promptingcompany.com with:

- what you found and where (file and line, or commit)
- how to reproduce it, if it applies
- the potential impact

We respond within 48 hours and agree a timeline for the fix with you.

## What this repository holds

- Code, SQL, settings and raw result files for the Databricks vs Snowflake pipeline cost benchmark.
- No credentials. Runs read the Databricks token, the Snowflake account and the path to a Snowflake private key from your own `.env`, which is git-ignored. `env.example` holds placeholders only.
- No personal data. The ETL workload's people and email fields are synthetic, generated from a seed with `example.com` addresses (`scripts/di_gen.py`).
- No generated data. TPC-DS data is generated locally with `dsdgen` and is not committed.
- Result files contain job run ids, timings, row counts, column sums and billed usage. They contain no account identifiers, hostnames or warehouse ids.

## Security measures

- **Secrets stay local:** `.env`, `*.p8` and `*.pem` are git-ignored. Snowflake uses key-pair login, not a password.
- **Secret scan before publishing:** the repository was scanned for private keys, tokens, cloud keys, account identifiers and email addresses before the first push.
- **Least privilege:** `setup/` lists the minimum grants the code needs, including read-only access to the billing tables. Our own runs used an admin identity, as the setup files note.

## Bug bounty

We do not currently run a bug bounty program.

---

**Contact:** support@promptingcompany.com
