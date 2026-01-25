# Deployment Guide: Streamlit Cloud

This guide walks you through deploying the Madrid Real Estate Tracker to Streamlit Community Cloud.

## Prerequisites

- GitHub account
- Google Drive account (for database hosting)
- Local copy of `real_estate.db` with data

---

## Step 1: Upload Database to Google Drive

1. **Upload the database file:**
   - Go to [Google Drive](https://drive.google.com)
   - Upload `real_estate.db` to your Drive
   - Right-click the file → "Share" → "Get link"
   - Set permissions to "Anyone with the link can view"

2. **Extract the file ID:**
   - Copy the share link (format: `https://drive.google.com/file/d/{FILE_ID}/view`)
   - Extract the `FILE_ID` part (long alphanumeric string)
   - Save this ID - you'll need it later

**Example:**
```
Link: https://drive.google.com/file/d/1a2b3c4d5e6f7g8h9i0j/view
File ID: 1a2b3c4d5e6f7g8h9i0j
```

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
   [database]
   google_drive_file_id = "YOUR_FILE_ID_HERE"
   ```
   
   - Replace `YOUR_FILE_ID_HERE` with the Google Drive file ID from Step 1
   - Click "Save"

3. **Reboot app:**
   - Click "Reboot app" button
   - App will restart and download the database
   - Should load successfully now

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

**On your local machine:**

1. Run the scraper:
   ```bash
   cd /Users/luisnuno/Downloads/workspace/inmobiliario
   source venv/bin/activate
   python scraper.py
   ```

2. Upload updated database to Google Drive:
   - Go to Google Drive
   - Delete old `real_estate.db`
   - Upload new `real_estate.db`
   - **Important:** Keep the same file ID (replace, don't create new)

3. Streamlit Cloud will automatically use the updated database:
   - Next user visit will download fresh data
   - Cache expires after 5 minutes

**Automated sync (optional):**

Install `rclone` to automate Google Drive sync:
```bash
brew install rclone
rclone config  # Configure Google Drive
rclone copy real_estate.db gdrive:/ --update
```

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

### "Database configuration missing in secrets"

**Problem:** Secrets not configured correctly

**Solution:**
1. Go to app settings → Secrets
2. Verify TOML format is correct
3. Ensure `[database]` section exists
4. Check file ID has no extra spaces

### "Failed to download database"

**Problem:** Google Drive file not accessible

**Solution:**
1. Check file sharing settings (must be "Anyone with link")
2. Verify file ID is correct
3. Try downloading manually: `https://drive.google.com/uc?id={FILE_ID}`
4. Check file size (Streamlit Cloud has 1GB limit)

### "ModuleNotFoundError: No module named 'gdown'"

**Problem:** Dependencies not installed

**Solution:**
1. Verify `gdown>=4.7.1` is in `requirements.txt`
2. Push updated requirements to GitHub
3. Streamlit Cloud will rebuild automatically

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
- [ ] Google Drive file permissions are appropriate
- [ ] Streamlit app access is restricted to authorized users

---

## Cost Summary

| Service | Plan | Cost |
|---------|------|------|
| Streamlit Cloud | Free tier | $0/month |
| Google Drive | Free (15GB) | $0/month |
| GitHub | Free (private repos) | $0/month |
| **Total** | | **$0/month** |

**Upgrade options:**
- Streamlit Cloud Pro: $20/month (more resources, custom domains)
- Google Workspace: $6/month (more storage)

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
   - Consider setting up cron job for daily scraping
   - Use `rclone` for automatic database sync

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
