#!/usr/bin/env bash
# Starter script for MESH-MASTER: activates venv, starts the app, and optionally opens the dashboard
set -euo pipefail

# Resolve script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Single instance enforcement
LOCK_FILE="$SCRIPT_DIR/mesh-master.lock"
PID_FILE="$SCRIPT_DIR/mesh-master.pid"

# Function to clean up lock file (only for foreground mode)
cleanup_lock() {
  if [ "${FOREGROUND:-0}" = "1" ]; then
    rm -f "$LOCK_FILE"
  fi
}

# Set trap to clean up on exit (only applies to foreground mode)
trap cleanup_lock EXIT

# Check if another instance is already running
if [ -f "$LOCK_FILE" ]; then
  LOCK_PID=$(cat "$LOCK_FILE" 2>/dev/null || echo "")
  if [ -n "$LOCK_PID" ] && kill -0 "$LOCK_PID" 2>/dev/null; then
    echo "ERROR: mesh-master is already running (PID: $LOCK_PID)"
    echo "If this is incorrect, remove the lock file: $LOCK_FILE"
    exit 1
  else
    echo "Removing stale lock file..."
    rm -f "$LOCK_FILE"
  fi
fi

# Activate virtualenv if present (.venv preferred). Fallback to common names.
if [ -f "$SCRIPT_DIR/.venv/bin/activate" ]; then
  # shellcheck disable=SC1091
  . "$SCRIPT_DIR/.venv/bin/activate"
elif [ -f "$SCRIPT_DIR/mesh-master/bin/activate" ]; then
  # shellcheck disable=SC1091
  . "$SCRIPT_DIR/mesh-master/bin/activate"
elif [ -f "$SCRIPT_DIR/venv/bin/activate" ]; then
  # shellcheck disable=SC1091
  . "$SCRIPT_DIR/venv/bin/activate"
fi

# Ensure log directory/file exist (used only in background mode)
LOG_FILE="$SCRIPT_DIR/mesh-master.log"
PID_FILE="$SCRIPT_DIR/mesh-master.pid"
touch "$LOG_FILE"

# Start the app in background and write pid
# If config.json defines a port, export it so the app (and browser) use the same port
if [ -f "$SCRIPT_DIR/config.json" ]; then
  cfg_port=$(jq -r '.web_port // .flask_port // .port // empty' "$SCRIPT_DIR/config.json" 2>/dev/null || true)
  if [ -n "$cfg_port" ] && [ "$cfg_port" != "null" ]; then
    export MESH_MASTER_PORT="$cfg_port"
  fi
fi

if [ "${FOREGROUND:-0}" = "1" ]; then
  # Run in foreground for systemd; skip browser
  echo "Starting mesh-master (foreground)"
  export NO_BROWSER=1
  # Create lock file with current process PID
  echo $$ > "$LOCK_FILE"
  # Exec so systemd tracks this process
  exec python "$SCRIPT_DIR/mesh-master.py"
else
  nohup python "$SCRIPT_DIR/mesh-master.py" >> "$LOG_FILE" 2>&1 &
  PID=$!
  echo "$PID" > "$PID_FILE"
  # Update lock file with actual Python process PID
  echo "$PID" > "$LOCK_FILE"
  echo "Started mesh-master (pid=$PID), log=$LOG_FILE"
fi

# Give the server a moment to start then open dashboard in default browser unless NO_BROWSER=1
# Wait for the app to announce the Flask port in the log (timeout after 10s)
if [ "${FOREGROUND:-0}" != "1" ] && [ "${NO_BROWSER:-0}" != "1" ]; then
  # Prefer explicit port exported via MESH_MASTER_PORT (from config.json or env)
  port=${MESH_MASTER_PORT:-}
  if [ -z "$port" ]; then
    # Try to parse the port the app announced in the log (compat fallback)
    for i in $(seq 1 10); do
      port=$(grep -m1 -Eo "Launching Flask in the background on port [0-9]+" "$LOG_FILE" | grep -Eo "[0-9]+" || true)
      if [ -n "$port" ]; then
        break
      fi
      sleep 1
    done
  fi
  if [ -n "$port" ]; then
    flask_url="http://localhost:$port/dashboard"
    xdg-open "$flask_url" || true
  else
    # fallback to default port 5000
    xdg-open http://localhost:5000/dashboard || true
  fi
fi

exit 0
