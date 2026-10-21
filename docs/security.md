# Security and threat model

This project is built for one person on one machine. Flask and Dagster have no login of their own. This page explains why that is acceptable for this setup, what protects it instead, and what would need to change before it could be hosted for anyone else.

## Contents

- [Assets](#assets)
- [Trust boundary](#trust-boundary)
- [Threats and controls](#threats-and-controls)
- [Known gaps](#known-gaps)
- [Before hosting it](#before-hosting-it)

## Assets

| Asset | Where it lives | Impact if exposed |
| --- | --- | --- |
| Watch and reading history, ratings | Postgres (`raw`, `marts`) | Personal viewing and reading habits |
| API credentials (TMDB, Simkl, Hardcover, SMTP) | `.env`, container environment | Account misuse with those providers |
| Pipeline control | Dagster UI and daemon | Runs launched, schedules switched off, data reloaded |
| Pick dismissals and settings | `meta.not_interested`, `meta.user_config` | Picks changed without the owner knowing |

## Trust boundary

The boundary is the host machine. Every published port in `docker-compose.yml` is bound to `127.0.0.1`, so the services can be reached only from the machine itself:

| Service | Port | Own authentication |
| --- | --- | --- |
| Flask (picks, `/ask`, `/health`) | 5000 | None |
| Dagster UI | 3000 | None |
| Metabase | 4000 | Yes, its own accounts |
| Postgres | 5432 | Password from `.env` |
| Ollama | 11434 | None |

Anyone who can run code on the host as the owner is already past the boundary. They can read `.env` and the Docker volumes directly, so a login screen on Flask or Dagster would not protect anything from them.

## Threats and controls

**Another device on the network opens the app.**
Ports are bound to loopback, so connections from other devices are refused. Inside Docker, Flask listens on `0.0.0.0`, but that address applies only to the container's own network; the host publishes it on `127.0.0.1` only.

**A question to `/ask` is used to change or read data it should not.**
The model writes SQL, so the SQL is treated as untrusted input:

- `_is_safe_select` in `flask/agent.py` accepts exactly one `SELECT` or `WITH ... SELECT` statement and rejects write and DDL keywords.
- The query runs as `ask_readonly`, a separate Postgres role created in `dagster/init_db.py`. It has `SELECT` on `marts` and `meta` only, no access to `raw` or `staging`, `default_transaction_read_only = on`, and a 5-second `statement_timeout`.
- The keyword check can be fooled; the database role is the control that matters. Even SQL that passes the check cannot write, cannot read the source tables, and cannot run long enough to tie up the database.
- Questions longer than 500 characters are rejected before any model call.

**A web page in the owner's browser sends requests to `localhost:5000`.**
The one route that writes data, `POST /not-interested`, reads only a JSON body (`request.get_json`). A cross-site request with a JSON content type needs a CORS preflight, and Flask never answers one with an allow header, so the browser blocks the request. A plain form post arrives without JSON and is rejected with a 400. `POST /ask` works the same way and is read-only in any case.

**Text from external APIs steers the model (prompt injection).**
Titles and overviews from TMDB and Open Library reach the model in result rows. The worst outcome is a misleading answer or reason. Each pick is validated in Python against the candidate list before it is saved, and `/ask` answers are built from rows returned by the read-only role, so injected text cannot cause writes or invent titles that reach the page.

**Secrets leak through the repository.**
`.env` is gitignored, and `.env.example` contains placeholders only. The exported Metabase dashboard in `metabase/` holds questions and layout only, no connection details.

**Data loss.**
`postgres-backup` writes a daily `pg_dump` to `./backups/postgres` and keeps 14. These backups are on the same disk, so they cover a damaged database, not a lost machine (see the README's Limitations).

## Known gaps

These are accepted for single-user local use and listed so they are not mistaken for oversights:

- **No authentication on Flask, Dagster, or Ollama.** Any process on the host can call them.
- **No Host header check (DNS rebinding).** A malicious site could, in principle, rebind its domain to `127.0.0.1` and read Flask or Dagster responses from the owner's browser. Dagster's UI can launch runs. Neither app restricts the `Host` header today.
- **No rate limiting on `/ask`.** Each question runs the local model for up to a few minutes, so a flood of requests from the host would tie up the CPU.
- **Containers run as root** with default Docker capabilities.
- **Metabase uses its built-in H2 application database**, which Metabase does not recommend for production.

## Before hosting it

If this ever runs anywhere other than a personal machine, these are the minimum changes:

1. Put Flask, Dagster, and Metabase behind a reverse proxy with authentication (for example oauth2-proxy or Cloudflare Access), and stop publishing Postgres and Ollama ports at all.
2. Serve over HTTPS and restrict accepted `Host` headers.
3. Add CSRF tokens to the write route and rate limits to `/ask`.
4. Move secrets from `.env` into a secret store, and give each service its own database role with least privilege, as `ask_readonly` already does for `/ask`.
5. Move Metabase to a Postgres application database, and copy backups off the machine.
