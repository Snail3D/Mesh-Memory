#!/usr/bin/env python3
"""Apply the always-on configuration profile to a locally attached RAK4631."""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PROFILE = REPO_ROOT / "hardware_profiles" / "rak4631_always_on.yaml"
DEFAULT_CONFIG = REPO_ROOT / "config.json"


def load_serial_port(config_path: Path) -> str | None:
    try:
        data = json.loads(config_path.read_text())
    except FileNotFoundError:
        return None
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive logging hook
        raise SystemExit(f"config.json is not valid JSON: {exc}") from exc
    port = data.get("serial_port") or data.get("serialPort")
    if isinstance(port, str) and port.strip():
        return port.strip()
    return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Push the RAK4631 always-on profile via the Meshtastic CLI")
    parser.add_argument("--port", help="Serial device path for the RAK4631. Defaults to config.json:serial_port")
    parser.add_argument(
        "--profile",
        type=Path,
        default=DEFAULT_PROFILE,
        help="YAML profile to push. Defaults to hardware_profiles/rak4631_always_on.yaml",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the Meshtastic command without executing it",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    profile_path = args.profile if args.profile.is_absolute() else (REPO_ROOT / args.profile)
    if not profile_path.exists():
        raise SystemExit(f"Profile file not found: {profile_path}")

    port = args.port or load_serial_port(DEFAULT_CONFIG)
    if not port:
        raise SystemExit("Serial port not provided and config.json has no serial_port entry")

    cmd = [sys.executable, "-m", "meshtastic", "--port", port, "--configure", str(profile_path)]

    print("Running:", " ".join(cmd))
    if args.dry_run:
        return

    try:
        subprocess.run(cmd, check=True)
    except FileNotFoundError as exc:
        raise SystemExit("Could not invoke Meshtastic CLI. Ensure the meshtastic Python package is installed.") from exc
    except subprocess.CalledProcessError as exc:
        raise SystemExit(f"Meshtastic CLI exited with status {exc.returncode}") from exc


if __name__ == "__main__":
    main()
