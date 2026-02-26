"""
Upload real_estate.db to Google Drive using a Service Account.
Designed for use in CI/CD pipelines (GitHub Actions, etc.)

Required environment variables:
  GOOGLE_SA_CREDENTIALS   JSON string with service account credentials
                          (obtained from Google Cloud Console)
  GOOGLE_DRIVE_FILE_ID    Drive file ID to update (same as in Streamlit secrets)

Setup (one-time):
  1. Google Cloud Console → IAM & Admin → Service Accounts → Create
  2. Give it no special roles (it only needs Drive access to specific files)
  3. Create a JSON key → download it
  4. In Google Drive, share real_estate.db file with the service account email
     (the email looks like name@project.iam.gserviceaccount.com) → Editor role
  5. In GitHub → Settings → Secrets → New secret:
       Name:  GOOGLE_SA_CREDENTIALS
       Value: (paste the full JSON content of the downloaded key file)
"""

import os
import sys
import json
from pathlib import Path

DB_PATH = Path("real_estate.db")


def main():
    # ── Validate env ─────────────────────────────────────────────────────────
    sa_creds_json = os.environ.get("GOOGLE_SA_CREDENTIALS", "")
    file_id       = os.environ.get("GOOGLE_DRIVE_FILE_ID", "").strip()

    if not sa_creds_json:
        print("❌ GOOGLE_SA_CREDENTIALS not set. Skipping upload.")
        sys.exit(1)
    if not file_id:
        print("❌ GOOGLE_DRIVE_FILE_ID not set. Skipping upload.")
        sys.exit(1)
    if not DB_PATH.exists():
        print(f"❌ Database not found: {DB_PATH}")
        sys.exit(1)

    size_mb = DB_PATH.stat().st_size / 1024 / 1024
    print(f"📤 Uploading {DB_PATH.name} ({size_mb:.1f} MB) → Drive file {file_id}")

    # ── Auth with service account ────────────────────────────────────────────
    try:
        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaFileUpload
    except ImportError:
        print(
            "❌ Missing Google client libraries.\n"
            "   pip install google-api-python-client google-auth"
        )
        sys.exit(1)

    try:
        sa_info = json.loads(sa_creds_json)
    except json.JSONDecodeError as e:
        print(f"❌ GOOGLE_SA_CREDENTIALS is not valid JSON: {e}")
        sys.exit(1)

    scopes = ["https://www.googleapis.com/auth/drive"]
    creds  = Credentials.from_service_account_info(sa_info, scopes=scopes)

    service = build("drive", "v3", credentials=creds)

    # ── Find or create the file ───────────────────────────────────────────────
    # Search for real_estate.db in files accessible to the service account
    try:
        results = service.files().list(
            q=f"name='{DB_PATH.name}' and trashed=false",
            spaces="drive",
            fields="files(id, name)",
            pageSize=5,
        ).execute()
        found_files = results.get("files", [])
    except Exception as exc:
        print(f"⚠ Could not search Drive: {exc}")
        found_files = []

    # Resolve the actual file ID to use
    if found_files:
        actual_file_id = found_files[0]["id"]
        print(f"  Found existing file: {actual_file_id}")
        if actual_file_id != file_id:
            print(f"  ⚠ Note: using found ID ({actual_file_id}) instead of secret ({file_id})")
            print(f"  → Update your GOOGLE_DRIVE_FILE_ID secret to: {actual_file_id}")
    else:
        actual_file_id = None
        print("  File not found in Drive — will create a new one.")

    # ── Upload ────────────────────────────────────────────────────────────────
    media = MediaFileUpload(
        str(DB_PATH),
        mimetype="application/octet-stream",
        resumable=True,
    )

    try:
        if actual_file_id:
            request = service.files().update(fileId=actual_file_id, media_body=media)
        else:
            request = service.files().create(
                body={"name": DB_PATH.name},
                media_body=media,
                fields="id",
            )

        response = None
        while response is None:
            status, response = request.next_chunk()
            if status:
                print(f"\r  ▶ {int(status.progress() * 100)}%", end="", flush=True)

        result_id = actual_file_id or response.get("id")
        print(f"\r✅ Upload complete ({size_mb:.1f} MB)          ")
        print(f"   https://drive.google.com/file/d/{result_id}/view")

        if not actual_file_id:
            print(f"\n⚠ New file created. Update your secrets:")
            print(f"   GOOGLE_DRIVE_FILE_ID = {result_id}")

    except Exception as exc:
        print(f"\n❌ Upload failed: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
