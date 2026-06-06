# Operations Runbook

## Production deployment flow

Use this order for production deployments:

1. Run a deployment-time database backup.
2. Deploy the new application code.
3. Run database migrations.
4. Run schema verification.
5. Run seed/bootstrap for default data.
6. Restart web and worker processes.
7. Run smoke tests.

## systemd unit generation

Use the repo script to render or install the systemd unit files:

- `uo_regulations.service`
- `uo_regulations_jobs_worker.service`
- `uo_regulations_flow_worker.service`
- `uo_regulations_batch_worker.service`
- `adoption-standard-update.service`
- `adoption-standard-update.timer`

Render locally for review:

```bash
bash scripts/install_systemd_units.sh --output-dir /tmp/uo-systemd
```

Install on the target host:

```bash
sudo bash scripts/install_systemd_units.sh --install
```

Useful overrides:

```bash
sudo bash scripts/install_systemd_units.sh \
  --install \
  --app-root /home/NE025/UO_MDR \
  --env-file /home/NE025/UO_MDR/.env \
  --web-bind 127.0.0.1:8000 \
  --web-workers 2 \
  --update-on-calendar 'daily'
```

If `--app-user` is not provided, the script uses the owner of `--app-root` for systemd `User=`.
Use `--app-user USER` only when the service should run as a different account.

After install:

```bash
sudo systemctl enable uo_regulations uo_regulations_jobs_worker uo_regulations_flow_worker uo_regulations_batch_worker adoption-standard-update.timer
```

The rendered unit files should be reviewed for the expected deployment values:

```bash
rg -n "User=|WorkingDirectory=|EnvironmentFile=" /tmp/uo-systemd
```

`adoption-standard-update.service` is installed as a `oneshot` unit. The recurring schedule is enabled through `adoption-standard-update.timer`; start the service itself only when you want to run the update immediately:

```bash
sudo systemctl status adoption-standard-update.service --no-pager
sudo systemctl start adoption-standard-update.service
```

## Nginx site config

The app deployment manages only the UO MDR site config, not the global nginx config.

- Keep `/etc/nginx/nginx.conf` as a manually maintained host-level file.
- Ensure `/etc/nginx/nginx.conf` includes `/etc/nginx/sites-enabled/*`.
- Keep the app site template in `deploy/nginx-site.conf.template`.
- Run `ENABLE_NGINX=1 bash deploy.sh`; it calls `sudo bash scripts/install_nginx_site.sh --install ...` to render `build/nginx/uo_regulations`, install it into `/etc/nginx/sites-available/uo_regulations`, update the enabled-site symlink, run `nginx -t`, and reload nginx.

For deployments outside the default path:

```bash
APP_ROOT=/home/NE025/UO_MDR ENABLE_NGINX=1 bash deploy.sh
```

Render the nginx site config for review without installing:

```bash
bash scripts/install_nginx_site.sh \
  --app-root /home/NE025/UO_MDR \
  --output-file /tmp/uo_regulations
```

Recommended command sequence:

```bash
export ALEMBIC_DATABASE_URL="$DATABASE_URL"
export ALEMBIC_CONFIG_NAME=production
cd /home/NE025/UO_MDR
/home/NE025/UO_MDR/.venv/bin/alembic upgrade head
export FLASK_APP=app.py
/home/NE025/UO_MDR/.venv/bin/flask schema-preflight
/home/NE025/UO_MDR/.venv/bin/flask seed-bootstrap
```

`schema-preflight` checks that all required tables already exist for the current app configuration. In production this is the guardrail that replaces startup `create_all()`.

`seed-bootstrap` only inserts default data:
- auth roles
- bootstrap admins from `BOOTSTRAP_ADMIN`
- default system settings
- default regulation sync state

`seed-bootstrap` does **not** create tables. If schema is missing, it exits non-zero.

## Auth modes

By default the web app uses LDAP authentication:

```bash
AUTH_ENABLED=1
AUTH_MODE=ldap
```

For use outside the company intranet, switch to local database authentication:

```bash
AUTH_ENABLED=1
AUTH_MODE=local
LOCAL_AUTH_BOOTSTRAP_PASSWORD='change-me-first'
BOOTSTRAP_ADMIN=NE025
```

Run migrations and seed data after changing the mode:

```bash
alembic upgrade head
flask seed-bootstrap
```

`AUTH_MODE=local` does not connect to LDAP during app startup or login. Users still need to exist in the system database and have roles. Admins can set or reset a user's local password from the user admin page. `LOCAL_AUTH_DEFAULT_PASSWORD` can be used temporarily when adding local users from the account search page; remove it after initial setup.

## Alembic migration flow

This repository now includes Alembic baseline migration support.

For a brand-new database:

```bash
export ALEMBIC_DATABASE_URL="$DATABASE_URL"
export ALEMBIC_CONFIG_NAME=production
cd /home/NE025/UO_MDR
/home/NE025/UO_MDR/.venv/bin/alembic upgrade head
```

For an existing production database that was historically managed by startup `create_all()`:

1. Take a full database backup first.
2. Deploy code that contains Alembic and `schema-preflight`.
3. Verify the current schema is already complete enough for the app.
4. Stamp the existing schema to the baseline revision.

Example:

```bash
export ALEMBIC_DATABASE_URL="$DATABASE_URL"
export ALEMBIC_CONFIG_NAME=production
cd /home/NE025/UO_MDR
/home/NE025/UO_MDR/.venv/bin/flask schema-preflight
/home/NE025/UO_MDR/.venv/bin/alembic stamp 0001_baseline_schema
```

After baseline adoption, future schema changes should use:

```bash
/home/NE025/UO_MDR/.venv/bin/alembic upgrade head
```

## MSSQL backup

Use [scripts/backup_mssql_full.sh](/home/NE025/UO_MDR/scripts/backup_mssql_full.sh) for a deployment-time `COPY_ONLY` full backup.

Requirements:
- `sqlcmd` installed on the machine running the script
- SQL Server login with backup permission
- `MSSQL_BACKUP_DIR` must be a path writable by the SQL Server service account

Required environment variables:
- `DATABASE_URL` or the split `SQLCMD_SERVER`/`SQLCMD_USER`/`SQLCMD_PASSWORD`/`MSSQL_DATABASE` values
- `MSSQL_BACKUP_DIR`

The script auto-loads the project `.env` and can derive the `sqlcmd` connection values from a SQLAlchemy
`mssql+pyodbc://...` `DATABASE_URL`.

Optional:
- `BACKUP_FILE_NAME`
- `SQLCMD_BIN`
- `SQLCMD_TRUST_CERT` defaults to `1`

Example:

```bash
export SQLCMD_SERVER='sqlhost,1433'
export SQLCMD_USER='backup_user'
export SQLCMD_PASSWORD='***'
export MSSQL_DATABASE='uo_mdr'
export MSSQL_BACKUP_DIR='D:\MSSQL\Backup'

bash scripts/backup_mssql_full.sh
```

Sample cron entry for daily full backup:

```cron
0 2 * * * cd /home/NE025/UO_MDR && ./.venv/bin/bash scripts/backup_mssql_full.sh >> logs/mssql_backup.log 2>&1
```

For tighter RPO, add SQL Server-native differential and transaction-log backups through SQL Agent or a DBA-managed schedule.

## MSSQL restore

Use [scripts/restore_mssql_replace.sh](/home/NE025/UO_MDR/scripts/restore_mssql_replace.sh) only for same-database rollback on the same SQL Server instance.

This script:
- forces the target DB into `SINGLE_USER`
- restores with `REPLACE`
- returns the DB to `MULTI_USER`

Required environment variables:
- `DATABASE_URL` or the split `SQLCMD_SERVER`/`SQLCMD_USER`/`SQLCMD_PASSWORD`/`MSSQL_DATABASE` values
- `MSSQL_BACKUP_FILE`

The script auto-loads the project `.env` and can derive the `sqlcmd` connection values from a SQLAlchemy
`mssql+pyodbc://...` `DATABASE_URL`.

Example:

```bash
export SQLCMD_SERVER='sqlhost,1433'
export SQLCMD_USER='restore_user'
export SQLCMD_PASSWORD='***'
export MSSQL_DATABASE='uo_mdr'
export MSSQL_BACKUP_FILE='D:\MSSQL\Backup\uo_mdr_2026-05-29_021500_copyonly_full.bak'

bash scripts/restore_mssql_replace.sh --yes
```

This is intended for deployment rollback. For restoring to a different server, different DB name, or different data/log file locations, use a dedicated restore script with `MOVE` clauses.

## Rollback procedure

If deployment fails after schema or app rollout:

1. Stop web and worker services.
2. Restore the deployment-time full backup.
3. Revert application code to the previous release.
4. Start services.
5. Verify login, task listing, file operations, and background worker health.

## File restore

Use [scripts/restore_files.sh](/home/NE025/UO_MDR/scripts/restore_files.sh) to restore a file archive created by
[scripts/backup.sh](/home/NE025/UO_MDR/scripts/backup.sh).

Example:

```bash
bash scripts/restore_files.sh backups/files/host_files_2026-06-02_120000.tar.gz --yes
```

The script verifies the adjacent `.sha256` file when present, creates a current-state file backup before restore,
clears the managed file restore paths, then extracts the archive into `APP_ROOT`.

To skip the current-state backup:

```bash
SKIP_PRE_RESTORE_BACKUP=1 bash scripts/restore_files.sh backups/files/host_files_2026-06-02_120000.tar.gz --yes
```

## Validation checklist

After each production deployment:

1. `flask schema-preflight` returns success.
2. `flask seed-bootstrap` returns success.
3. Web login works.
4. Existing tasks load correctly.
5. A worker can claim and complete a test job.
6. Backup filename and deployment timestamp are recorded together for rollback traceability.
