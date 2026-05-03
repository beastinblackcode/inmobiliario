"""
Export barrio profiles to barrios_profiles.json.

Usage:
    python export_barrio_profiles.py                         # stdout
    python export_barrio_profiles.py -o barrios_profiles.json
    python export_barrio_profiles.py -o barrios_profiles.json --push
"""

import json
import os
import sys
import traceback
from datetime import datetime
from typing import Optional


def export_barrio_profiles(output_path: Optional[str] = None) -> bool:
    try:
        from barrio_profiles import build_all_barrio_profiles
        data   = build_all_barrio_profiles()
        pretty = json.dumps(data, ensure_ascii=False, indent=2, default=str)

        if output_path:
            os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(pretty)
            size_kb = os.path.getsize(output_path) / 1024
            print(f"📁 Written to {output_path} ({size_kb:.0f} KB)")
        else:
            print(pretty)

        return True
    except Exception as exc:
        print(f"❌ Export failed: {exc}")
        traceback.print_exc()
        return False


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Export barrio profiles JSON")
    parser.add_argument("-o", "--output", help="Output JSON file path")
    parser.add_argument(
        "--push", action="store_true",
        help="Git commit + push after writing (requires -o and THERMOMETER_PAT)"
    )
    args = parser.parse_args()

    ok = export_barrio_profiles(args.output)

    if ok and args.push and args.output:
        import subprocess, os
        pat = os.environ.get("THERMOMETER_PAT", "")
        if not pat:
            print("⚠️  THERMOMETER_PAT not set — skipping push")
        else:
            print("🚀 Pushing barrios_profiles.json to market-thermometer…")
            subprocess.run([
                "git", "clone", "--depth", "1",
                f"https://x-access-token:{pat}@github.com/softniric-cyber/market-thermometer.git",
                "_thermometer_tmp"
            ], check=True)
            import shutil
            dest = os.path.join("_thermometer_tmp", "public", os.path.basename(args.output))
            shutil.copy(args.output, dest)
            subprocess.run(["git", "-C", "_thermometer_tmp", "config",
                            "user.email", "github-actions[bot]@users.noreply.github.com"])
            subprocess.run(["git", "-C", "_thermometer_tmp", "config",
                            "user.name", "github-actions[bot]"])
            subprocess.run(["git", "-C", "_thermometer_tmp", "add",
                            f"public/{os.path.basename(args.output)}"])
            result = subprocess.run(
                ["git", "-C", "_thermometer_tmp", "diff", "--staged", "--quiet"]
            )
            if result.returncode != 0:
                subprocess.run([
                    "git", "-C", "_thermometer_tmp", "commit", "-m",
                    f"Update barrios_profiles {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}"
                ], check=True)
                subprocess.run(["git", "-C", "_thermometer_tmp", "push",
                                "origin", "main"], check=True)
                print("✅ Pushed barrios_profiles.json")
            else:
                print("ℹ️  No changes to push")
            shutil.rmtree("_thermometer_tmp", ignore_errors=True)

    sys.exit(0 if ok else 1)
