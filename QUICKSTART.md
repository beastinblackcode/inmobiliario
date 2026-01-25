# Quick Start: Deploying to Streamlit Cloud

This is a condensed version of the full deployment guide. For detailed instructions, see [DEPLOYMENT.md](DEPLOYMENT.md).

## Prerequisites

- [ ] GitHub account
- [ ] Google Drive account
- [ ] Database file (`real_estate.db`) with data

## Steps

### 1. Upload Database to Google Drive

1. Upload `real_estate.db` to Google Drive
2. Share → "Anyone with link can view"
3. Copy file ID from URL: `https://drive.google.com/file/d/{FILE_ID}/view`

### 2. Initialize Git & Push to GitHub

```bash
cd /Users/luisnuno/Downloads/workspace/inmobiliario

# Initialize git
git init
git add .
git commit -m "Initial commit: Madrid Real Estate Tracker"

# Create GitHub repo (private recommended)
# Then push:
git remote add origin https://github.com/YOUR_USERNAME/REPO_NAME.git
git branch -M main
git push -u origin main
```

### 3. Deploy to Streamlit Cloud

1. Go to [share.streamlit.io](https://share.streamlit.io)
2. Sign in with GitHub
3. New app → Select your repository
4. Main file: `app.py`
5. Deploy!

### 4. Configure Secrets

In Streamlit Cloud app settings → Secrets:

```toml
[database]
google_drive_file_id = "YOUR_FILE_ID_HERE"
```

Save and reboot app.

## Done! 🎉

Your app is now live with GitHub authentication. Only users with repository access can view it.

## Updating Data

After running scraper locally:
1. Replace `real_estate.db` in Google Drive (keep same file ID)
2. Streamlit Cloud will use updated data automatically

## Updating Code

```bash
git add .
git commit -m "Update: description"
git push
```

Streamlit Cloud auto-deploys in ~2 minutes.

---

**Full documentation:** [DEPLOYMENT.md](DEPLOYMENT.md)
