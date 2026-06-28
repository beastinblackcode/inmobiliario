"""
Send a failure-notification email when a GitHub Actions workflow fails.

Reuses ``email_report.send_report`` (Gmail SMTP via GMAIL_APP_PASSWORD), so
silent CI failures — an expired push token, a DB outage, a broken exporter —
reach the inbox instead of sitting unnoticed in the Actions tab.

Designed to be called from an ``if: failure()`` step:

    - name: Notify on failure
      if: failure()
      env:
        GMAIL_APP_PASSWORD: ${{ secrets.GMAIL_APP_PASSWORD }}
      run: python notify_failure.py "Export Public Metrics (clean)"

Reads the run context from the standard env vars the Actions runner sets
(GITHUB_REPOSITORY, GITHUB_RUN_ID, GITHUB_SERVER_URL, …). Always exits 0: a
problem sending the notification must never mask the original failure or fail
the job further.
"""

import os
import sys


def main() -> int:
    workflow = sys.argv[1] if len(sys.argv) > 1 else os.getenv("GITHUB_WORKFLOW", "Workflow")
    repo = os.getenv("GITHUB_REPOSITORY", "?")
    run_id = os.getenv("GITHUB_RUN_ID", "")
    server = os.getenv("GITHUB_SERVER_URL", "https://github.com")
    sha = (os.getenv("GITHUB_SHA", "") or "")[:7]
    ref = os.getenv("GITHUB_REF_NAME", "")
    run_url = f"{server}/{repo}/actions/runs/{run_id}" if run_id else f"{server}/{repo}/actions"

    subject = f"🔴 CI falló: {workflow}"
    html = f"""\
<h2>🔴 Workflow fallido: {workflow}</h2>
<p>Repositorio: <code>{repo}</code>{f' · rama <code>{ref}</code>' if ref else ''}{f' · commit <code>{sha}</code>' if sha else ''}</p>
<p><a href="{run_url}">Ver el run en GitHub Actions →</a></p>
<p>Revisa los logs del run para el detalle del fallo.</p>
"""

    try:
        from email_report import send_report
        send_report(html, subject)
    except Exception as exc:  # noqa: BLE001 — notification must not crash the step
        print(f"⚠️  No se pudo enviar la notificación de fallo: {exc}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
