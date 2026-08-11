# Deployment Guide: Streamlit Cloud + Supabase Postgres

This guide walks you through deploying the Madrid Real Estate Tracker
to Streamlit Community Cloud, backed by a Supabase Postgres database.

## Prerequisites

- GitHub account
- Supabase account (free tier is enough — DB stays well under 500 MB)
- Local copy of `real_estate.db` (only for the one-time cutover load —
  see "Cutover from SQLite" below if you are migrating an existing
  deployment)

---

## Step 1: Provision Postgres (Supabase)

1. **Create a project:**
   - Sign in at [supabase.com](https://supabase.com) → "New project"
   - Pick the region closest to your traffic (Europe West for Madrid)
   - Set a strong database password — you will paste it into the URL
     below.  **Note:** Supabase often generates passwords that
     contain `+`; that's fine, the runtime URL-encodes them.

2. **Grab the connection URL.**  Supabase exposes two pooler ports:

   | Use case | Port | When to use it |
   |---|---|---|
   | Long-running process (Streamlit, scraper) | **6543** (transaction pooler) | Default — pgbouncer in front, free IPv4, scales fine for our concurrency |
   | Migrations / admin (`alembic`, ad-hoc `CREATE INDEX CONCURRENTLY`, `COPY`) | **5432** (session pooler) | Required for statements pgbouncer can't multiplex |

   Both URLs look like:
   ```
   postgresql://postgres.<ref>:<PASSWORD>@aws-1-<region>.pooler.supabase.com:<PORT>/postgres
   ```

3. **Apply the schema:**
   ```bash
   export DATABASE_URL='postgresql://postgres.<ref>:<PWD>@aws-1-<region>.pooler.supabase.com:5432/postgres'
   alembic upgrade head
   ```

   (Use port `5432` here — Alembic runs DDL.)

If you are deploying for the first time with no historical data, you
are done — skip the cutover section and continue at Step 2.

---

## Cutover from SQLite (one-time, only when migrating an existing deploy)

If you already have a `real_estate.db` you want to preserve:

```bash
# Schema first (as above).
export DATABASE_URL='postgresql://postgres.<ref>:<PWD>@…:5432/postgres'
alembic upgrade head

# Load every row.  ~96k rows / ~15s on a clean target.
python migration_sqlite_to_postgres.py

# Sanity-check via Streamlit before flipping any environment.
DB_BACKEND=postgres DATABASE_URL="$DATABASE_URL" streamlit run app.py
```

The backfill is single-transaction; on any error it rolls back and
exits non-zero.  `--truncate` re-runs are idempotent.  See the
docstring at the top of `migration_sqlite_to_postgres.py` for the
full mode list and data-quality notes.

Once the Streamlit smoke looks healthy, flip the runtime by following
Step 5 (Configure Secrets) and re-enable the Daily Scraper cron in
`.github/workflows/daily_scraper.yml` (uncomment the `schedule` block).

---

## Step 2: Initialize Git Repository

```bash
cd /Users/luisnuno/Downloads/workspace/inmobiliario

# Initialize git
git init

# Add all files
git add .

# Create initial commit
git commit -m "Initial commit: Madrid Real Estate Tracker"
```

**Verify secrets are not included:**
```bash
# These should NOT appear in git status
git status | grep -E "\.env|\.db|secrets\.toml"
```

---

## Step 3: Create GitHub Repository

1. **Go to GitHub:**
   - Visit [github.com/new](https://github.com/new)
   - Repository name: `madrid-real-estate-tracker` (or your choice)
   - Set to **Private** (recommended)
   - Do NOT initialize with README (we already have one)
   - Click "Create repository"

2. **Push local code to GitHub:**
   ```bash
   # Add remote
   git remote add origin https://github.com/YOUR_USERNAME/madrid-real-estate-tracker.git
   
   # Push code
   git branch -M main
   git push -u origin main
   ```

3. **Verify on GitHub:**
   - Check that files are uploaded
   - Confirm `.env` and `*.db` files are NOT visible
   - Verify `.gitignore` is working

---

## Step 4: Deploy to Streamlit Cloud

1. **Go to Streamlit Cloud:**
   - Visit [share.streamlit.io](https://share.streamlit.io)
   - Click "Sign in with GitHub"
   - Authorize Streamlit Cloud to access your repositories

2. **Create new app:**
   - Click "New app"
   - Select your repository: `madrid-real-estate-tracker`
   - Branch: `main`
   - Main file path: `app.py`
   - Click "Deploy!"

3. **Wait for deployment:**
   - Initial deployment takes 2-5 minutes
   - You'll see build logs in real-time
   - App will show error initially (secrets not configured yet)

---

## Step 5: Configure Secrets

1. **Open app settings:**
   - In Streamlit Cloud dashboard, click on your app
   - Click the "⋮" menu → "Settings"
   - Go to "Secrets" tab

2. **Add secrets:**
   - Paste the following TOML configuration:

   ```toml
   [postgres]
   # Transaction pooler URL (port 6543) — works for Streamlit's
   # long-running process.  Use the Session pooler (port 5432) for
   # one-off admin tasks (alembic, COPY) only.
   url = "postgresql://postgres.<ref>:<PASSWORD>@aws-1-<region>.pooler.supabase.com:6543/postgres"

   [auth.users_hashed]
   # Generate with: python gen_password_hash.py
   luis  = "$2b$12$EXAMPLE_REPLACE_ME"
   ```

   - Replace the `<ref>`, `<PASSWORD>`, and `<region>` placeholders
     with the values from your Supabase project.  Passwords
     containing `+` are accepted as-is; the runtime percent-encodes
     them defensively before handing the URL to libpq (see
     `db/connection_pg._normalise_url`).
   - Replace each `$2b$12$EXAMPLE_REPLACE_ME` with a real bcrypt hash:
     ```bash
     python gen_password_hash.py
     ```
     Run once per user. Copy the resulting line into the secrets blob.
   - Click "Save"

   **Rotating a password:**
   1. `python gen_password_hash.py` → generates a new hash.
   2. Update the line under `[auth.users_hashed]` in Streamlit Cloud secrets.
   3. Save → Streamlit Cloud auto-restarts with the new hash. Existing
      sessions stay valid until they expire (12 h) — log out manually if
      you need to invalidate them immediately.

   **Backwards compatibility:** if you still have a `[auth.users]` block
   with plaintext passwords, the app keeps working but emits a
   `⚠️ DEPRECATED` warning to stderr on every login. Migrate to
   `[auth.users_hashed]` and delete the old block.

3. **No extra step needed.** ``app.py`` detects the ``[postgres]``
   block in ``st.secrets`` at boot and sets ``DB_BACKEND=postgres``
   for the rest of the process.  (Streamlit Community Cloud's free
   tier doesn't expose a UI for arbitrary env vars, so we bridge
   the secret to the env var in code.)  An explicit ``DB_BACKEND``
   env var still wins if you ever need to force SQLite — but
   normally there's nothing else to configure.

4. **Reboot app:**
   - Click "Reboot app" button
   - App will restart, the connection pool opens lazily on first
     query, and the dashboard renders directly from Supabase.

---

## Step 6: Test Authentication

**Default authentication (GitHub OAuth):**
- Only you (repository owner) can access the app by default
- Streamlit Cloud automatically requires GitHub login
- No additional configuration needed

**To grant access to others:**
1. Go to your GitHub repository settings
2. Add collaborators under "Manage access"
3. They can now access the deployed app

**To make app public:**
1. In Streamlit Cloud app settings
2. Go to "Sharing" tab
3. Toggle "Make this app public"
4. ⚠️ **Warning:** Anyone with the link can access

---

## Step 7: Verify Deployment

**Check these items:**

- [ ] App loads without errors
- [ ] Database downloads successfully (check logs)
- [ ] All metrics display correctly
- [ ] Charts render properly
- [ ] Filters work as expected
- [ ] Data table shows listings
- [ ] Sidebar shows "☁️ Deployed on Streamlit Cloud"

**If there are errors:**
- Check app logs in Streamlit Cloud dashboard
- Verify Google Drive file ID is correct
- Ensure database file is publicly accessible
- Check that all dependencies are in `requirements.txt`

---

## Maintenance Workflow

### Updating Data (Daily Scraping)

Both the scraper and the public exporter run on GitHub Actions and
write straight to the same Supabase Postgres that Streamlit Cloud
reads from.  There is no DB file to ship anywhere.

**Required GitHub secrets** (Repo → Settings → Secrets and variables → Actions):

| Secret | Used by | Notes |
|---|---|---|
| `DATABASE_URL` | both workflows | Same Supabase URL as Streamlit, port `6543` (transaction pooler) — pgbouncer fronts both readers and writers fine |
| `BRIGHTDATA_*` | `daily_scraper.yml` | Existing — proxy creds |
| `GMAIL_APP_PASSWORD` | `daily_scraper.yml` | Existing — for `email_report.py` |
| `THERMOMETER_DEPLOY_KEY` | `export-metrics.yml` | SSH **deploy key** (private half) for the cross-repo push to `softniric-cyber/market-thermometer`. Register the public half on that repo (Settings → Deploy keys, *Allow write access*). Replaces the old `THERMOMETER_PAT`, which expired and stalled the public site (Aug 2026); deploy keys never expire and work across accounts. |

The `GOOGLE_DRIVE_FILE_ID` and `GOOGLE_SA_CREDENTIALS` secrets are
**no longer used by either workflow** post-cutover; delete them when
convenient.

**Re-enabling the scraper cron** (currently paused — see banner at
the top of `.github/workflows/daily_scraper.yml`): uncomment the
`schedule` block.  Do this **only after** the Postgres backfill has
been verified by a local Streamlit run.

### Updating Code

1. **Make changes locally:**
   ```bash
   # Edit files
   vim app.py
   
   # Test locally
   streamlit run app.py
   ```

2. **Deploy changes:**
   ```bash
   git add .
   git commit -m "Update: description of changes"
   git push
   ```

3. **Streamlit Cloud auto-deploys:**
   - Detects new commit automatically
   - Rebuilds and redeploys (~2 minutes)
   - No manual intervention needed

### Monitoring

**Check app health:**
- Streamlit Cloud dashboard shows:
  - App status (running/stopped)
  - Resource usage (CPU, memory)
  - Recent logs
  - Error notifications

**View logs:**
- Click "Manage app" → "Logs"
- Shows real-time application output
- Useful for debugging issues

---

## Troubleshooting

### "No Postgres connection URL configured"

**Problem:** `db.connection_pg._resolve_url` couldn't find a URL in
either `st.secrets["postgres"]["url"]`, `DATABASE_URL`, or `POSTGRES_URL`.

**Solution:**
1. In Streamlit Cloud, verify `[postgres]` block is in secrets with a
   `url` field.
2. Confirm `DB_BACKEND=postgres` is set under Advanced settings —
   without it the runtime stays on SQLite and never inspects the
   `[postgres]` block.
3. For GitHub Actions, confirm `DATABASE_URL` is set under repo
   secrets and the workflow's `env:` block exposes it.

### "password authentication failed for user 'postgres'"

**Problem:** Most commonly a `+` in the password that some URL parser
decoded as a space.  The runtime handles this defensively
(`db.connection_pg._normalise_url`), but if you are running ad-hoc
queries with another tool, you may hit it.

**Solution:**
1. Percent-encode the password component: `+` → `%2B`.
2. Test with `psql "<URL>" -c 'SELECT 1'` before pasting it into a
   secret.

### "duplicate key value violates unique constraint" on first cron run

**Problem:** The Postgres schema enforces UNIQUE on
`price_history(listing_id, date_recorded)`.  An older SQLite snapshot
had a few same-day duplicates that pre-dated
`migration_dedupe_price_history.py`.

**Solution:** the Phase C backfill (`migration_sqlite_to_postgres.py`)
already dedupes at read time, so this should not happen after a clean
cutover.  If you see it from a *running* scraper, it is a writer bug
in `insert_listing` — file an issue, do not silence the constraint.

### App is slow or crashes

**Problem:** Resource limits exceeded

**Solution:**
- Streamlit Cloud free tier: 1GB RAM, 1 CPU
- Optimize data loading (use caching)
- Consider upgrading to paid tier if needed
- Reduce database size if too large

---

## Security Checklist

Before going live, verify:

- [ ] `.env` file is in `.gitignore`
- [ ] `*.db` files are in `.gitignore`
- [ ] `.streamlit/secrets.toml` is in `.gitignore`
- [ ] No secrets in commit history (`git log --all --full-history --source -- .env`)
- [ ] GitHub repository is private (or secrets removed if public)
- [ ] Supabase project is in the right region and uses a strong password
- [ ] Supabase RLS / network restrictions configured if you expect
      anything other than this codebase to talk to the DB
- [ ] Streamlit app access is restricted to authorized users

---

## Cost Summary

| Service | Plan | Cost |
|---------|------|------|
| Streamlit Cloud | Free tier | $0/month |
| Supabase | Free tier (500 MB DB, 2 GB transfer) | $0/month |
| GitHub | Free (private repos) | $0/month |
| **Total** | | **$0/month** |

The current DB is ~25 MB (~96k rows), so the 500 MB Supabase free
tier has years of headroom at the current scraping cadence.

**Upgrade options:**
- Streamlit Cloud Pro: $20/month (more resources, custom domains)
- Supabase Pro: $25/month (8 GB DB, daily backups, no auto-pause)

---

## Next Steps

After successful deployment:

1. **Share the app:**
   - Copy app URL from Streamlit Cloud
   - Share with authorized users
   - They'll need GitHub access to view

2. **Set up monitoring:**
   - Enable email notifications in Streamlit Cloud
   - Monitor app health regularly

3. **Automate scraping:**
   - Uncomment the `schedule` block in
     `.github/workflows/daily_scraper.yml` to re-enable the
     thrice-weekly cron (Mon/Thu 06:00 UTC) once the cutover is
     verified.

4. **Optimize performance:**
   - Monitor resource usage
   - Adjust cache TTL if needed
   - Consider database optimization

---

## Support

**Streamlit Documentation:**
- [Streamlit Cloud Docs](https://docs.streamlit.io/streamlit-community-cloud)
- [Secrets Management](https://docs.streamlit.io/streamlit-community-cloud/deploy-your-app/secrets-management)

**Need help?**
- Check Streamlit Cloud logs for errors
- Review this guide's troubleshooting section
- Contact support if issues persist
