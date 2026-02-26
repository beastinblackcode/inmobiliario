"""
Upload real_estate.db to Google Drive using a Service Account.
Designed for use in CI/CD pipelines (GitHub Actions, etc.)

The service account uploads to its own Drive (not the user's personal Drive)
and makes the file publicly readable so gdown can download it from Streamlit.

The file ID is persisted in drive_file_id.txt so subsequent runs update the
same file instead of creating a new one each time.

Required environment variables:
  GOOGLE_SA_CREDENTIALS   JSON string with service account credentials
  GOOGLE_DRIVE_FILE_ID    (optional) fallback file ID if drive_file_id.txt missing

Setup (one-time):
  1. Google Cloud Console → IAM & Admin → Service Accounts → Create
  2. Create a JSON key → download it
  3. In GitHub → Settings → Secrets → New secret:
       Name:  GOOGLE_SA_CREDENTIALS
       Value: (paste the full JSON content)
"""

import os
import sys
import json
from pathlib import Path

DB_PATH         = Path("real_estate.db")
FILE_ID_PATH    = Path("drive_file_id.txt")


def _load_file_id() -> str:
    """Read persisted Drive file ID from drive_file_id.txt."""
    if FILE_ID_PATH.exists():
        return FILE_ID_PATH.read_text().strip()
    return os.environ.get("GOOGLE_DRIVE_FILE_ID", "").strip()


def _save_file_id(file_id: str) -> None:
    FILE_ID_PATH.write_text(file_id)
    print(f"  💾 File ID saved to {FILE_ID_PATH}: {file_id}")


def _make_public(service, file_id: str) -> None:
    """Grant 'anyone with the link' read access."""
    try:
        service.permissions().create(
            fileId=file_id,
            body={"type": "anyone", "role": "reader"},
        ).execute()
        print("  🌐 File set to public (anyone with link can read)")
    except Exception as exc:
        print(f"  ⚠ Could not set public permission: {exc}")


def main():
    # ── Validate env ─────────────────────────────────────────────────────────
    sa_creds_json = os.environ.get("GOOGLE_SA_CREDENTIALS", "")
    if not sa_creds_json:
        print("❌ GOOGLE_SA_CREDENTIALS not set. Skipping upload.")
        sys.exit(1)
    if not DB_PATH.exists():
        print(f"❌ Database not found: {DB_PATH}")
        sys.exit(1)

    size_mb = DB_PATH.stat().st_size / 1024 / 1024
    print(f"📤 Uploading {DB_PATH.name} ({size_mb:.1f} MB) to service account Drive...")

    # ── Auth ──────────────────────────────────────────────────────────────────
    try:
        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaFileUpload
        from googleapiclient.errors import HttpError
    except ImportError:
        print("❌ pip install google-api-python-client google-auth")
        sys.exit(1)

    try:
        sa_info = json.loads(sa_creds_json)
    except json.JSONDecodeError as e:
        print(f"❌ GOOGLE_SA_CREDENTIALS is not valid JSON: {e}")
        sys.exit(1)

    scopes  = ["https://www.googleapis.com/auth/drive"]
    creds   = Credentials.from_service_account_info(sa_info, scopes=scopes)
    service = build("drive", "v3", credentials=creds)

    # ── Resolve file ID ───────────────────────────────────────────────────────
    file_id = _load_file_id()

    # ── Upload ────────────────────────────────────────────────────────────────
    media = MediaFileUpload(
        str(DB_PATH),
        mimetype="application/octet-stream",
        resumable=True,
    )

    def _do_upload(request):
        response = None
        while response is None:
            status, response = request.next_chunk()
            if status:
                print(f"\r  ▶ {int(status.progress() * 100)}%", end="", flush=True)
        return response

    try:
        if file_id:
            # Try to update existing file owned by the service account
            try:
                _do_upload(service.files().update(fileId=file_id, media_body=media))
                print(f"\r✅ Updated ({size_mb:.1f} MB)          ")
                print(f"   https://drive.google.com/file/d/{file_id}/view")
                return  # success
            except HttpError as e:
                if e.resp.status == 404:
                    print(f"\r  ⚠ File {file_id} not found — creating new file...")
                else:
                    raise

        # Create new file in service account's Drive
        response = _do_upload(
            service.files().create(
                body={"name": DB_PATH.name},
                media_body=media,
                fields="id",
            )
        )
        new_id = response.get("id")
        print(f"\r✅ Created ({size_mb:.1f} MB)          ")

        # Make public so gdown can download it
        _make_public(service, new_id)

        _save_file_id(new_id)
        print(f"\n⚠ New file ID: {new_id}")
        print(f"   Update your Streamlit secret and GOOGLE_DRIVE_FILE_ID:")
        print(f"   google_drive_file_id = \"{new_id}\"")
        print(f"   https://drive.google.com/file/d/{new_id}/view")

    except Exception as exc:
        print(f"\n❌ Upload failed: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
