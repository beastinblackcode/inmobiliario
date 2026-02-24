"""
Upload real_estate.db to Google Drive after each scrape.

SETUP (one-time, ~5 minutes):
──────────────────────────────
1. Ve a https://console.cloud.google.com
2. Crea un proyecto (o usa uno existente)
3. Activa la API: APIs & Services → Enable APIs → "Google Drive API" → Enable
4. Crea credenciales: APIs & Services → Credentials
   → Create Credentials → OAuth client ID → Desktop app → Download JSON
5. Guarda el archivo descargado como: credentials.json (en esta carpeta)
6. Ejecuta una vez manualmente para autorizar:
       python upload_to_drive.py
   Se abrirá el navegador, acepta los permisos.
   Esto genera token.json (no lo borres).

A partir de ese momento el scraper sube la DB automáticamente al terminar.

EJECUCIÓN DIRECTA:
    python upload_to_drive.py                  # sube real_estate.db
    python upload_to_drive.py --dry-run        # comprueba auth sin subir
    python upload_to_drive.py --path otra.db   # sube otro archivo
"""

import os
import sys
import argparse
from pathlib import Path
from datetime import datetime

# ── Config ────────────────────────────────────────────────────────────────────

BASE_DIR         = Path(__file__).parent
DB_PATH          = BASE_DIR / "real_estate.db"
CREDENTIALS_FILE = BASE_DIR / "credentials.json"
TOKEN_FILE       = BASE_DIR / "token.json"

# Google Drive file ID (same as in Streamlit secrets)
# Find it in the URL of the file: drive.google.com/file/d/<FILE_ID>/view
DRIVE_FILE_ID = "1ajdgLaneXwb6OWl_S727gwyYZUfrdF7p"

SCOPES = ["https://www.googleapis.com/auth/drive.file"]


# ── Auth ──────────────────────────────────────────────────────────────────────

def get_credentials():
    """
    Return valid Google OAuth credentials.
    Refreshes the token automatically if expired.
    On first run (no token.json), opens the browser for authorization.
    """
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request

    creds = None

    if TOKEN_FILE.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            print("🔄 Renovando token de acceso...")
            creds.refresh(Request())
        else:
            if not CREDENTIALS_FILE.exists():
                print(
                    "\n❌ No se encontró credentials.json\n"
                    "\nPasos para obtenerlo:\n"
                    "  1. Ve a https://console.cloud.google.com\n"
                    "  2. APIs & Services → Credentials\n"
                    "  3. Create Credentials → OAuth client ID → Desktop app\n"
                    "  4. Descarga el JSON y guárdalo como: credentials.json\n"
                    "  5. Vuelve a ejecutar este script.\n"
                )
                sys.exit(1)

            print("🌐 Abriendo navegador para autorizar acceso a Google Drive...")
            flow = InstalledAppFlow.from_client_secrets_file(
                str(CREDENTIALS_FILE), SCOPES
            )
            creds = flow.run_local_server(port=0)

        # Persist token for future runs
        TOKEN_FILE.write_text(creds.to_json())
        print(f"✅ Token guardado en {TOKEN_FILE.name}")

    return creds


# ── Upload ────────────────────────────────────────────────────────────────────

def upload_db(db_path: Path, dry_run: bool = False) -> bool:
    """
    Update the existing Google Drive file with the local DB.
    Returns True on success, False on error.
    """
    if not db_path.exists():
        print(f"❌ Archivo no encontrado: {db_path}")
        return False

    size_mb = db_path.stat().st_size / 1024 / 1024
    print(f"\n📤 Subiendo {db_path.name} ({size_mb:.1f} MB) a Google Drive...")

    if dry_run:
        print("🔍 Dry-run activado — autenticación OK, no se sube nada.")
        return True

    try:
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaFileUpload
        from googleapiclient.errors import HttpError

        creds   = get_credentials()
        service = build("drive", "v3", credentials=creds)

        media = MediaFileUpload(
            str(db_path),
            mimetype="application/octet-stream",
            resumable=True,        # resumable upload handles large files safely
        )

        start = datetime.now()

        request = service.files().update(
            fileId=DRIVE_FILE_ID,
            media_body=media,
        )

        # Execute with progress reporting
        response = None
        while response is None:
            status, response = request.next_chunk()
            if status:
                pct = int(status.progress() * 100)
                print(f"\r  ▶ Progreso: {pct}%", end="", flush=True)

        elapsed = (datetime.now() - start).total_seconds()
        speed   = size_mb / elapsed if elapsed > 0 else 0

        print(f"\r  ✅ Subida completada en {elapsed:.1f}s ({speed:.1f} MB/s)       ")
        print(f"  🔗 https://drive.google.com/file/d/{DRIVE_FILE_ID}/view")

        return True

    except ImportError:
        print(
            "\n❌ Faltan dependencias. Instálalas con:\n"
            "   pip install google-api-python-client google-auth-httplib2 "
            "google-auth-oauthlib\n"
        )
        return False

    except Exception as exc:
        print(f"\n❌ Error subiendo a Drive: {exc}")
        return False


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Sube real_estate.db a Google Drive"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Comprueba la autenticación sin subir nada",
    )
    parser.add_argument(
        "--path",
        type=Path,
        default=DB_PATH,
        help=f"Ruta del archivo a subir (default: {DB_PATH})",
    )
    args = parser.parse_args()

    # Auth check first (even in dry-run, validates credentials exist)
    get_credentials()

    success = upload_db(args.path, dry_run=args.dry_run)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
