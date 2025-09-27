import meshtastic
import meshtastic.serial_interface
from meshtastic import BROADCAST_ADDR
from meshtastic import portnums_pb2
from pubsub import pub
import json
import calendar
import html
import difflib
import requests
import urllib.parse
import time
from datetime import datetime, timedelta, timezone  # Added timezone import
import threading
import os
import logging
from collections import deque, Counter
from pathlib import Path
import traceback
from flask import Flask, request, jsonify, redirect, url_for, stream_with_context, Response
import sys
import socket  # for socket error checking
import re
import random
import subprocess
import math
from typing import Optional, Set, Dict, Any, List, Tuple
from dataclasses import dataclass, field
from meshtastic_facts import MESHTASTIC_ALERT_FACTS
from unidecode import unidecode   # Added unidecode import for Ollama text normalization
from google.protobuf.message import DecodeError
import queue  # For async message processing
import atexit
from mesh_master import GameManager, MailManager, PendingReply
# Make sure DEBUG_ENABLED exists before any logger/filter classes use it
# -----------------------------
# Global Debug & Noise Patterns
# -----------------------------
# Debug flag loaded later from config.json
DEBUG_ENABLED = False
# Suppress these protobuf messages unless DEBUG_ENABLED=True
NOISE_PATTERNS = (
    "Error while parsing FromRadio",
    "Error parsing message with type 'meshtastic.protobuf.FromRadio'",
    "Traceback",
    "meshtastic/stream_interface.py",
    "meshtastic/mesh_interface.py",
)

class _ProtoNoiseFilter(logging.Filter):
    NOISY = (
        "Error while parsing FromRadio",
        "Error parsing message with type 'meshtastic.protobuf.FromRadio'",
        "DecodeError",
        "Traceback",
        "_handleFromRadio",
        "__reader",
        "meshtastic/stream_interface.py",
        "meshtastic/mesh_interface.py",
    )

    def filter(self, rec: logging.LogRecord) -> bool:
        noisy = any(s in rec.getMessage() for s in self.NOISY)
        return DEBUG_ENABLED or not noisy        # show only in debug mode

root_log       = logging.getLogger()          # the root logger
meshtastic_log = logging.getLogger("meshtastic")

for lg in (root_log, meshtastic_log):
    lg.addFilter(_ProtoNoiseFilter())

# Custom exception for fatal serial exclusive-lock scenarios
class ExclusiveLockError(Exception):
    pass

def dprint(*args, **kwargs):
    if DEBUG_ENABLED:
        message = ' '.join(str(arg) for arg in args)
        smooth_print(message)

def info_print(*args, **kwargs):
    message = ' '.join(str(arg) for arg in args)
    if DEBUG_ENABLED:
        smooth_print(message)
        return

    text = message.strip()
    if not text:
        return

    emoji = None
    body = text

    if text and ord(text[0]) > 127:
        emoji = text[0]
        body = text[1:].strip()
    elif text.lower().startswith("[info]"):
        emoji = "ℹ️"
        body = text[6:].strip()
    elif text.lower().startswith("[cb]"):
        emoji = "📡"
        body = text[4:].strip()
    elif text.lower().startswith("[ui]"):
        emoji = "🖥️"
        body = text[4:].strip()

    if not body:
        body = text if emoji is None else ""

    if emoji:
        clean_log(body or text, emoji)
    else:
        clean_log(body)

# Smooth scrolling logging system
from collections import defaultdict

_log_queue = queue.Queue()
_log_thread = None
_log_running = False

def _smooth_log_worker():
    """Worker thread that prints logs smoothly one at a time"""
    while _log_running:
        try:
            message = _log_queue.get(timeout=1)
            if message is None:  # Shutdown signal
                break
            print(message, flush=True)
            time.sleep(0.1)  # Small delay for smooth scrolling
            _log_queue.task_done()
        except queue.Empty:
            continue

def start_smooth_logging():
    """Start the smooth logging system"""
    global _log_thread, _log_running
    _log_running = True
    _log_thread = threading.Thread(target=_smooth_log_worker, daemon=True)
    _log_thread.start()

def stop_smooth_logging():
    """Stop the smooth logging system"""
    global _log_running
    _log_running = False
    _log_queue.put(None)  # Shutdown signal

def smooth_print(message):
    """Add message to smooth printing queue"""
    if _log_running:
        _log_queue.put(message)
    else:
        print(message, flush=True)

# Rate limiter for preventing log spam  
_last_message_time = defaultdict(float)
_message_counts = defaultdict(int)
_rate_limit_seconds = 2.0  # Don't show same message more than once every 2 seconds

_NODE_ID_PATTERN = re.compile(r"!(?:[0-9a-f]{8})", re.IGNORECASE)
_CHANNEL_ID_PATTERN = re.compile(r"ch=(\d+)", re.IGNORECASE)


def _truncate_for_log(text: Optional[str], limit: int = 160) -> str:
    if text is None:
        return ""
    stripped = str(text).strip().replace('\n', ' ')
    if len(stripped) <= limit:
        return stripped
    return stripped[: limit - 1].rstrip() + "…"


def _beautify_log_text(message: str) -> str:
    if not message:
        return message

    def repl(match: re.Match[str]) -> str:
        node_id = match.group(0)
        short = get_node_shortname(node_id)
        return short if short else node_id

    cleaned = _NODE_ID_PATTERN.sub(repl, message)
    cleaned = cleaned.replace('\x07', '')
    lowered = cleaned.lower()
    if 'alert bell character' in lowered or '🔔' in cleaned:
        original = str(message)
        candidate = None
        if 'Message from ' in original:
            part = original.split('Message from ', 1)[1]
            candidate = part.split(' ', 1)[0]
        if not candidate and '→' in original:
            candidate = original.split('→', 1)[0].strip()
        if not candidate and ':' in original:
            candidate = original.split(':', 1)[0].strip()
        short = get_node_shortname(candidate) if candidate else None
        name = (short or (candidate or 'NODE')).strip()
        name = re.sub(r'[^A-Za-z0-9_-]+', ' ', name).strip()
        if not name:
            name = ''
        cleaned = f"[{name.upper()}] ALERTS!"
    def repl_channel(match: re.Match[str]) -> str:
        try:
            idx = int(match.group(1))
        except (TypeError, ValueError):
            return match.group(0)
        return f"ch={_channel_display_name(idx)}"
    cleaned = _CHANNEL_ID_PATTERN.sub(repl_channel, cleaned)
    return cleaned

def clean_log(message, emoji="📝", show_always=False, rate_limit=True):
    """Clean, emoji-enhanced logging for better human readability with rate limiting"""
    message = _beautify_log_text(str(message))
    # Rate limiting to reduce jitter
    if rate_limit and not DEBUG_ENABLED:
        message_key = f"{emoji}_{message[:50]}"  # Use first 50 chars as key
        current_time = time.time()
        
        if current_time - _last_message_time[message_key] < _rate_limit_seconds:
            _message_counts[message_key] += 1
            return  # Skip this message to reduce spam
        
        # If we had suppressed messages, show count
        if _message_counts[message_key] > 0:
            suppressed_count = _message_counts[message_key]
            _message_counts[message_key] = 0
            if suppressed_count > 1:
                message += f" (suppressed {suppressed_count} similar messages)"
        
        _last_message_time[message_key] = current_time
    
    if show_always or (not DEBUG_ENABLED and CLEAN_LOGS):
        smooth_print(f"{emoji} {message}")  # Use smooth printing for better scrolling
    elif not CLEAN_LOGS and not DEBUG_ENABLED:
        # Fall back to simple logging without emojis if clean_logs is disabled
        smooth_print(f"[Info] {message}")

def ai_log(message, provider="AI"):
    """Specialized logging for AI interactions with provider-specific emojis"""
    if CLEAN_LOGS:
        provider_emojis = {
            "ollama": "🦙",
            "openai": "🤖", 
            "lmstudio": "💻",
            "home_assistant": "🏠"
        }
        emoji = provider_emojis.get(provider.lower(), "🤖")
        clean_log(f"{provider.upper()}: {message}", emoji, show_always=True, rate_limit=False)
    elif not DEBUG_ENABLED:
        # Simple logging without emojis if clean_logs is disabled
        print(f"[{provider.upper()}] {message}")

# Periodic status updates to reduce log noise
_last_status_time = 0
_status_interval = 300  # 5 minutes between status updates

def periodic_status_update():
    """Show periodic status instead of constant chatter"""
    global _last_status_time
    current_time = time.time()
    
    if current_time - _last_status_time > _status_interval and not DEBUG_ENABLED and CLEAN_LOGS:
        _last_status_time = current_time
        clean_log("System running normally...", "💚", show_always=True, rate_limit=False)

# Custom stderr filter to catch protobuf noise
class FilteredStderr:
    def __init__(self, original_stderr):
        self.original_stderr = original_stderr
        self.noise_patterns = [
            "google.protobuf.message.DecodeError",
            "Error parsing message with type 'meshtastic.protobuf.FromRadio'",
            "Traceback (most recent call last):",
            "meshtastic/stream_interface.py",
            "meshtastic/mesh_interface.py", 
            "_handleFromRadio",
            "__reader",
            "fromRadio.ParseFromString",
        ]
    
    def write(self, text):
        if not DEBUG_ENABLED and CLEAN_LOGS:
            # Filter out protobuf noise
            if any(pattern in text for pattern in self.noise_patterns):
                return  # Don't print noisy protobuf errors
        
        self.original_stderr.write(text)
    
    def flush(self):
        self.original_stderr.flush()
    
    def __getattr__(self, name):
        return getattr(self.original_stderr, name)

if DEBUG_ENABLED:
  cfg = globals().get('config', None)
  if cfg is not None:
    print(f"DEBUG: Loaded main config => {cfg}")
# -----------------------------
# Verbose Logging Setup
# -----------------------------
SCRIPT_LOG_FILE = "script.log"
LOG_MAX_BYTES = int(0.4 * 1024 * 1024)
LOG_TRIM_DELTA_BYTES = int(0.05 * 1024 * 1024)
LOG_AUTO_TRIM_PATHS = [SCRIPT_LOG_FILE, "messages.log", "mesh-master.log"]
STALE_LOG_MAX_AGE_DAYS = 365
STALE_LOG_PATTERNS = ("game", "dm", "record", "history")
STALE_LOG_DIRECTORIES = ["log", "logs", "data/logs", "data/records"]
script_logs = []  # In-memory log entries (most recent 200)
server_start_time = datetime.now(timezone.utc)  # Now using UTC time
restart_count = 0
_viewer_filter_enabled = True  # Default: filter noise in /logs and /logs_stream

def _viewer_should_show(line: str) -> bool:
  """Return True if a log line should be visible in the web viewer.

  Strategy:
  - In DEBUG mode, show everything.
  - Hide known noise (non-text packet ignores, connection plumbing, banner, etc).
  - Show message-related RX/TX, AI, UI, and error/warning lines.
  """
  if DEBUG_ENABLED:
    return True
  if not isinstance(line, str):
    return False

  # Fast drop for protobuf and trace noise (already handled elsewhere but double-guard)
  if any(s in line for s in _ProtoNoiseFilter.NOISY):
    return False

  # Explicit noise/spam patterns to hide from viewer
  spam = (
    "[CB] on_receive fired",
    "Ignoring non-text packet",
    "Subscribing to on_receive",
    "Connecting to Meshtastic device",
    "Connection successful!",
    "TCPInterface",
    "MeshInterface()",
    "SerialInterface",
    "Baudrate switched",
    "Home Assistant multi-mode is ENABLED",
    "Launching Flask web interface",
    "Server restarted.",
    "Enabled clean logging mode",
    "System running normally",
    "DISCLAIMER: This is beta software",
    "Messaging Dashboard Access: http://",
    "📨 Message from ",
    "[AsyncAI]",
  )
  if any(s in line for s in spam):
    return False

  stripped = line.strip()
  if stripped.startswith('!') and '→' in stripped:
    return False

  if '[RX]' in line and '📨' in line:
    return False

  # Whitelist: message-related and important lines
  whitelist_markers = (
    "📨 Message from ",
    "📨 ",
    "📡 Broadcasting",
    "📤 Sending direct",
    "Sent chunk ",
    "[AsyncAI]",
    "Processing:",
    "Generated response",
    "Completed response",
    "No response generated",
    "Error processing response",
    "EMERGENCY",
    "[UI] ",
    # AI provider clean_log prefixes with emojis
    "🦙 OLLAMA:",
  )
  if any(s in line for s in whitelist_markers):
    return True

  # Always show warnings/errors
  if ("⚠️" in line) or ("❌" in line) or ("ERROR" in line.upper()):
    return True

  # Fallback: hide
  return False


LOG_TIMESTAMP_PATTERN = re.compile(
    r"^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2}) "
    r"(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2}) "
    r"(?P<tz>[A-Z]+) - (?P<rest>.*)$"
)


def _normalize_log_timestamp(line: str) -> str:
  match = LOG_TIMESTAMP_PATTERN.match(line)
  if not match:
    return line
  rest = match.group('rest')
  return rest.strip() if rest else line


def _classify_log_line(line: str) -> str:
  lowered = line.lower()
  if 'error' in lowered or '❌' in line or '🚨' in line or 'failed' in lowered or 'alerts!' in lowered:
    return 'error'
  if '📨' in line:
    return 'incoming'
  if '📤' in line or '📡' in line:
    return 'outgoing'
  if 'local time' in lowered or 'uptime' in lowered:
    return 'clock'
  return ''


def _humanize_uptime(delta: timedelta) -> str:
  total_seconds = int(delta.total_seconds())
  rounded_minutes = (total_seconds + 30) // 60
  days, minutes = divmod(rounded_minutes, 60 * 24)
  hours, minutes = divmod(minutes, 60)
  parts = []
  if days:
    parts.append(f"{days}d")
  parts.append(f"{hours:02d}:{minutes:02d}")
  return ' '.join(parts)

def _trim_log_file(path: str) -> None:
    if not path:
        return
    try:
        if not os.path.exists(path):
            return
        if LOG_MAX_BYTES <= 0:
            return
        size = os.path.getsize(path)
        if size <= LOG_MAX_BYTES:
            return
        keep = max(LOG_MAX_BYTES - LOG_TRIM_DELTA_BYTES, 0)
        if keep <= 0:
            keep = LOG_MAX_BYTES
        with open(path, "rb") as src:
            if size <= keep:
                return
            start = max(size - keep, 0)
            src.seek(start)
            tail = src.read()
        text = tail.decode("utf-8", errors="ignore")
        with open(path, "w", encoding="utf-8") as dst:
            dst.write(text.lstrip('\ufeff'))
    except Exception as exc:
        print(f"⚠️ Could not trim log {path}: {exc}")


def _enforce_log_size_limits():
    for candidate in LOG_AUTO_TRIM_PATHS:
        if not candidate:
            continue
        _trim_log_file(candidate)


def _purge_stale_nonessential_logs():
    if not STALE_LOG_PATTERNS:
        return
    cutoff = time.time() - STALE_LOG_MAX_AGE_DAYS * 86400
    if cutoff <= 0:
        return
    for target in STALE_LOG_DIRECTORIES:
        if not target:
            continue
        target_path = os.path.abspath(target)
        if not os.path.exists(target_path):
            continue
        for root, dirs, files in os.walk(target_path):
            if os.path.basename(root).startswith('.'):  # skip hidden dirs
                continue
            dirs[:] = [d for d in dirs if not d.startswith('.')]
            for name in files:
                lower = name.lower()
                if not any(pattern in lower for pattern in STALE_LOG_PATTERNS):
                    continue
                path = os.path.join(root, name)
                try:
                    if os.path.getmtime(path) < cutoff:
                        os.remove(path)
                except Exception:
                    continue
            for d in list(dirs):
                full = os.path.join(root, d)
                try:
                    if not os.listdir(full):
                        os.rmdir(full)
                except OSError:
                    pass
        try:
            if os.path.isdir(target_path) and not os.listdir(target_path):
                os.rmdir(target_path)
        except OSError:
            pass


_enforce_log_size_limits()
_purge_stale_nonessential_logs()


def add_script_log(message):
    # drop protobuf noise if debug is off
    NOISE_PATTERNS = (
        "Error while parsing FromRadio",
        "Error parsing message with type 'meshtastic.protobuf.FromRadio'",
        "Traceback",
        "meshtastic/stream_interface.py",
        "meshtastic/mesh_interface.py",
    )
    if not DEBUG_ENABLED and any(p in message for p in NOISE_PATTERNS):
        return

    # Use local system time for script logs (viewer shows this clock)
    timestamp = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    log_entry = f"{timestamp} - {message}"
    script_logs.append(log_entry)
    if len(script_logs) > 200:
        script_logs.pop(0)
    try:
        # Truncate file if larger than 100 MB (keep last 100 lines)
        if os.path.exists(SCRIPT_LOG_FILE):
            filesize = os.path.getsize(SCRIPT_LOG_FILE)
            if filesize > 100 * 1024 * 1024:
                with open(SCRIPT_LOG_FILE, "r", encoding="utf-8") as f:
                    lines = f.readlines()
                last_lines = lines[-100:] if len(lines) >= 100 else lines
                with open(SCRIPT_LOG_FILE, "w", encoding="utf-8") as f:
                    f.writelines(last_lines)
        with open(SCRIPT_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(log_entry + "\n")
        _trim_log_file(SCRIPT_LOG_FILE)
    except Exception as e:
        print(f"⚠️ Could not write to {SCRIPT_LOG_FILE}: {e}")

def _pid_running(pid: int) -> bool:
    try:
        if pid <= 0:
            return False
        os.kill(pid, 0)
        return True
    except Exception:
        return False

APP_LOCK_FILE = "mesh-master.app.lock"

def acquire_app_lock():
    try:
        if os.path.exists(APP_LOCK_FILE):
            try:
                with open(APP_LOCK_FILE, 'r', encoding='utf-8') as f:
                    existing = f.read().strip()
                ep = int(existing) if existing else 0
            except Exception:
                ep = 0
            if ep and _pid_running(ep):
                print(f"❌ Another mesh-master instance appears to be running (PID {ep}). Exiting.")
                sys.exit(1)
        with open(APP_LOCK_FILE, 'w', encoding='utf-8') as f:
            f.write(str(os.getpid()))
    except Exception as e:
        print(f"⚠️ Could not create app lock: {e}")

def release_app_lock():
    try:
        if os.path.exists(APP_LOCK_FILE):
            os.remove(APP_LOCK_FILE)
    except Exception:
        pass
# Redirect stdout and stderr to our log while still printing to terminal.
class StreamToLogger(object):
    def __init__(self, logger_func):
        self.logger_func = logger_func
        self.terminal = sys.__stdout__
        # reuse noise patterns from the Proto filter
        self.noise_patterns = _ProtoNoiseFilter.NOISY if ' _ProtoNoiseFilter' in globals() else []

    def write(self, buf):
        # still print everything to the terminal...
        self.terminal.write(buf)
        text = buf.strip()
        if not text:
            return
        # only log to script_logs if not noisy, or if debug is on
        if DEBUG_ENABLED or not any(p in text for p in self.noise_patterns):
            self.logger_func(text)

    def flush(self):
        self.terminal.flush()

sys.stdout = StreamToLogger(add_script_log)
sys.stderr = StreamToLogger(add_script_log)
# -----------------------------
# Global Connection & Reset Status
# -----------------------------
connection_status = "Disconnected"
last_error_message = ""
reset_event = threading.Event()  # Global event to signal a fatal error and trigger reconnect
CONNECTING_NOW = False

RADIO_WATCHDOG_STATE = {
    "serial_warn": 0.0,
    "stale_rx": 0.0,
    "stale_tx": 0.0,
    "generic": 0.0,
}


def _invoke_power_command(cmd):
    if isinstance(cmd, str):
        subprocess.run(cmd, shell=True, check=True)
    elif isinstance(cmd, (list, tuple)):
        subprocess.run(cmd, check=True)
    else:
        raise ValueError("Unsupported command type for power cycle")


def power_cycle_usb_port():
    global USB_POWER_CYCLE_WARNED
    if not USB_POWER_CYCLE_ENABLED:
        if not USB_POWER_CYCLE_WARNED:
            clean_log("USB power cycle skipped (commands not configured).", "ℹ️", show_always=True, rate_limit=False)
            USB_POWER_CYCLE_WARNED = True
        return
    if not USB_POWER_CYCLE_LOCK.acquire(blocking=False):
        return
    try:
        clean_log("Power cycling USB port for radio...", "🔌", show_always=True, rate_limit=False)
        _invoke_power_command(USB_POWER_CYCLE_OFF_CMD)
        time.sleep(max(1, USB_POWER_CYCLE_DELAY))
        _invoke_power_command(USB_POWER_CYCLE_ON_CMD)
        clean_log("USB power restored.", "🔌", show_always=True, rate_limit=False)
    except Exception as exc:
        clean_log(f"USB power cycle failed: {exc}", "⚠️", show_always=True, rate_limit=False)
    finally:
        USB_POWER_CYCLE_LOCK.release()


def trigger_radio_reset(reason: str, emoji: str = "🔄", debounce_key: str = "generic", power_cycle: bool = False) -> None:
    now_ts = time.time()
    last_ts = RADIO_WATCHDOG_STATE.get(debounce_key, 0.0) or 0.0
    if reset_event.is_set():
        return
    if now_ts - last_ts < RADIO_WATCHDOG_DEBOUNCE:
        return
    RADIO_WATCHDOG_STATE[debounce_key] = now_ts
    add_script_log(f"Radio watchdog: {reason}")
    clean_log(f"{reason} — requesting radio reconnect", emoji, show_always=True, rate_limit=False)
    try:
        globals()['connection_status'] = "Disconnected"
    except Exception:
        pass
    if power_cycle:
        power_cycle_usb_port()
    reset_event.set()


class SerialDisconnectHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = record.getMessage()
        except Exception:
            return
        if not message:
            return
        lower = message.lower()
        if any(keyword in lower for keyword in SERIAL_WARNING_KEYWORDS) and not CONNECTING_NOW:
            trigger_radio_reset("Serial link reported disconnect", "⚡", debounce_key="serial_warn", power_cycle=True)


serial_watch_handler = SerialDisconnectHandler()
serial_watch_handler.setLevel(logging.WARNING)
root_log.addHandler(serial_watch_handler)
meshtastic_log.addHandler(serial_watch_handler)

# -----------------------------
# RX De-duplication cache
# -----------------------------
RECENT_RX_MAX = 500
recent_rx_keys = deque()  # FIFO of recent keys
recent_rx_keys_set = set()
recent_rx_lock = threading.Lock()

def _rx_make_key(packet, text, ch_idx):
  try:
    pid = packet.get('id') if isinstance(packet, dict) else None
  except Exception:
    pid = None
  try:
    fr = (packet.get('fromId') if isinstance(packet, dict) else None) or (packet.get('from') if isinstance(packet, dict) else None)
    to = (packet.get('toId') if isinstance(packet, dict) else None) or (packet.get('to') if isinstance(packet, dict) else None)
  except Exception:
    fr, to = None, None
  base = f"{pid}|{fr}|{to}|{ch_idx}|{text}"
  # Bound the key length to keep memory small
  return base[-512:]

def _rx_seen_before(key: str) -> bool:
  with recent_rx_lock:
    if key in recent_rx_keys_set:
      return True
    recent_rx_keys.append(key)
    recent_rx_keys_set.add(key)
    # Trim if over capacity
    while len(recent_rx_keys) > RECENT_RX_MAX:
      old = recent_rx_keys.popleft()
      recent_rx_keys_set.discard(old)
    return False

# -----------------------------
# Meshtastic and Flask Setup
# -----------------------------
try:
    from meshtastic.tcp_interface import TCPInterface
except ImportError:
    TCPInterface = None

try:
    from meshtastic.mesh_interface import MeshInterface
    MESH_INTERFACE_AVAILABLE = True
except ImportError:
    MESH_INTERFACE_AVAILABLE = False

log = logging.getLogger('werkzeug')
log.disabled = True

BANNER = (
    "\033[38;5;214m"
    """
███╗   ███╗███████╗███████╗██╗  ██╗             █████╗ ██╗
████╗ ████║██╔════╝██╔════╝██║  ██║            ██╔══██╗██║
██╔████╔██║█████╗  ███████╗███████║  █████╗    ███████║██║
██║╚██╔╝██║██╔══╝  ╚════██║██╔══██║  ╚════╝    ██╔══██║██║
██║ ╚═╝ ██║███████╗███████║██║  ██║            ██║  ██║██║
╚═╝     ╚═╝╚══════╝╚══════╝╚═╝  ╚═╝            ╚═╝  ╚═╝╚═╝

MESH-MASTER v1.0.0 by: MR_TBOT (https://mr-tbot.com)
https://mesh-master.dev - (https://github.com/mr-tbot/mesh-master/)
    \033[32m 
Messaging Dashboard Access: http://localhost:5000/dashboard \033[38;5;214m
"""
    "\033[0m"
    "\033[31m"
    """
DISCLAIMER: This is beta software - NOT ASSOCIATED with the official Meshtastic (https://meshtastic.org/) project.
It should not be relied upon for mission critical tasks or emergencies.
Modification of this code for nefarious purposes is strictly frowned upon. Please use responsibly.

(Use at your own risk. For feedback or issues, visit https://mesh-master.dev or the links above.)
"""
    "\033[0m"
)
print(BANNER)
add_script_log("Script started.")

RADIO_STALE_RX_THRESHOLD_DEFAULT = 300
RADIO_STALE_TX_THRESHOLD_DEFAULT = 300
RADIO_WATCHDOG_DEBOUNCE = 60
SERIAL_WARNING_KEYWORDS = (
    "serial port disconnected",
    "device reports readiness to read but returned no data",
)

# -----------------------------
# Load Config Files
# -----------------------------
CONFIG_FILE = "config.json"
COMMANDS_CONFIG_FILE = "commands_config.json"
MOTD_FILE = "motd.json"
LOG_FILE = "messages.log"
ARCHIVE_FILE = "messages_archive.json"

print("Loading config files...")

def safe_load_json(path, default_value):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"⚠️ {path} not found. Using defaults.")
    except Exception as e:
        print(f"⚠️ Could not load {path}: {e}")
    return default_value

def write_atomic(path: str, data: str):
    """Atomically write text data to a file to avoid partial writes.
    Creates a temporary file in the same directory and replaces the target.
    """
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        f.write(data)
    os.replace(tmp_path, path)

config = safe_load_json(CONFIG_FILE, {})
commands_config = safe_load_json(COMMANDS_CONFIG_FILE, {"commands": []})


def _determine_server_port() -> int:
    candidates = [
        os.environ.get("MESH_MASTER_PORT"),
        config.get("web_port"),
        config.get("flask_port"),
        config.get("port"),
        5000,
    ]
    for value in candidates:
        if value is None:
            continue
        try:
            port = int(value)
            if 1 <= port <= 65535:
                return port
        except Exception:
            continue
    return 5000


SERVER_PORT = _determine_server_port()

LOG_MAX_BYTES = int(config.get("log_max_bytes", LOG_MAX_BYTES))
LOG_TRIM_DELTA_BYTES = int(config.get("log_trim_delta_bytes", LOG_TRIM_DELTA_BYTES))
LOG_AUTO_TRIM_PATHS = list(config.get("log_auto_trim_paths", LOG_AUTO_TRIM_PATHS))
STALE_LOG_MAX_AGE_DAYS = int(config.get("stale_log_max_age_days", STALE_LOG_MAX_AGE_DAYS))
STALE_LOG_PATTERNS = tuple(config.get("stale_log_patterns", STALE_LOG_PATTERNS))
STALE_LOG_DIRECTORIES = list(config.get("stale_log_directories", STALE_LOG_DIRECTORIES))
try:
    with open(MOTD_FILE, "r", encoding="utf-8") as f:
        motd_content = f.read()
except FileNotFoundError:
    print(f"⚠️ {MOTD_FILE} not found.")
    motd_content = "No MOTD available."


ADMIN_PASSWORD = str(config.get("admin_password", "password") or "password")
_initial_admins = config.get("admin_whitelist", [])
AUTHORIZED_ADMINS: Set[str] = set()
if isinstance(_initial_admins, list):
    for entry in _initial_admins:
        if entry is None:
            continue
        AUTHORIZED_ADMINS.add(str(entry))
PENDING_ADMIN_REQUESTS: Dict[str, Dict[str, Any]] = {}
PENDING_WIPE_REQUESTS: Dict[str, Dict[str, Any]] = {}
PENDING_BIBLE_NAV: Dict[str, Dict[str, Any]] = {}
PENDING_POSITION_CONFIRM: Dict[str, Dict[str, Any]] = {}
BIBLE_NAV_LOCK = threading.Lock()

ANTISPAM_LOCK = threading.Lock()
ANTISPAM_STATE: Dict[str, Dict[str, Any]] = {}
ANTISPAM_WINDOW_SECONDS = 120  # 2 minutes
ANTISPAM_THRESHOLD = 25
ANTISPAM_SHORT_TIMEOUT = 10 * 60
ANTISPAM_LONG_TIMEOUT = 24 * 60 * 60
ANTISPAM_ESCALATION_WINDOW = 60 * 60  # 1 hour after release

BIBLE_AUTOSCROLL_PENDING: Dict[str, Dict[str, Any]] = {}
BIBLE_AUTOSCROLL_STATE: Dict[str, Dict[str, Any]] = {}
BIBLE_AUTOSCROLL_LOCK = threading.Lock()
BIBLE_AUTOSCROLL_MAX_CHUNKS = 30
BIBLE_AUTOSCROLL_INTERVAL = 12

_raw_channel_names = {}
if isinstance(config, dict):
    maybe_names = config.get("channel_names", {})
    if isinstance(maybe_names, dict):
        _raw_channel_names = maybe_names

CHANNEL_NAME_MAP: Dict[int, str] = {}
for key, value in _raw_channel_names.items():
    try:
        idx = int(key)
    except (TypeError, ValueError):
        continue
    CHANNEL_NAME_MAP[idx] = str(value)


def _channel_display_name(ch_idx: Optional[int]) -> str:
    if ch_idx is None:
        return "Broadcast"
    try:
        idx = int(ch_idx)
    except (TypeError, ValueError):
        return str(ch_idx)
    name = CHANNEL_NAME_MAP.get(idx)
    if name:
        return name
    try:
        if interface and hasattr(interface, "channels") and interface.channels:
            channels = interface.channels
            candidate = None
            if isinstance(channels, dict):
                candidate = channels.get(idx) or channels.get(str(idx))
            elif isinstance(channels, (list, tuple)) and 0 <= idx < len(channels):
                candidate = channels[idx]
            if candidate:
                if isinstance(candidate, dict):
                    name = candidate.get('settings', {}).get('name') or candidate.get('name')
                else:
                    name = getattr(candidate, 'name', None)
                if name:
                    CHANNEL_NAME_MAP[idx] = str(name)
                    return str(name)
    except Exception:
        pass
    return f"Ch{idx}"


def _set_bible_nav(sender_key: Optional[str], info: Dict[str, Any], *, is_direct: bool, channel_idx: Optional[int]) -> None:
    if not sender_key or not info:
        return
    nav_info = dict(info)
    nav_info['is_direct'] = bool(is_direct)
    nav_info['channel_idx'] = channel_idx
    with BIBLE_NAV_LOCK:
        PENDING_BIBLE_NAV[sender_key] = nav_info


def _clear_bible_nav(sender_key: Optional[str]) -> None:
    if not sender_key:
        return
    with BIBLE_NAV_LOCK:
        PENDING_BIBLE_NAV.pop(sender_key, None)
    with BIBLE_AUTOSCROLL_LOCK:
        BIBLE_AUTOSCROLL_PENDING.pop(sender_key, None)
        BIBLE_AUTOSCROLL_STATE.pop(sender_key, None)


def _prepare_bible_autoscroll(sender_key: Optional[str], is_direct: bool, channel_idx: Optional[int]):
    if not sender_key:
        return PendingReply("📖 Auto-scroll isn't available right now.", "/bible autoscroll")
    lang = (PENDING_BIBLE_NAV.get(sender_key, {}).get('language') or LANGUAGE_FALLBACK)
    if not is_direct:
        return PendingReply(
            translate(lang, 'bible_autoscroll_dm_only', "📖 Auto-scroll works only in direct messages."),
            "/bible autoscroll",
        )
    if sender_key not in PENDING_BIBLE_NAV:
        return PendingReply(
            translate(lang, 'bible_autoscroll_need_nav', "📖 Use /bible first, then reply 22 to auto-scroll."),
            "/bible autoscroll",
        )
    with BIBLE_AUTOSCROLL_LOCK:
        BIBLE_AUTOSCROLL_PENDING[sender_key] = {
            'channel_idx': channel_idx,
            'requested_at': time.time(),
        }
    return PendingReply(
        translate(lang, 'bible_autoscroll_stop', "⏹️ Auto-scroll paused. Reply 22 later to resume."),
        "/bible autoscroll",
    )


def _process_bible_autoscroll_request(sender_key: Optional[str], sender_node: Optional[str], interface_ref) -> None:
    if not sender_key:
        return
    with BIBLE_AUTOSCROLL_LOCK:
        BIBLE_AUTOSCROLL_PENDING.pop(sender_key, None)
        BIBLE_AUTOSCROLL_STATE.pop(sender_key, None)

MESHTASTIC_KB_FILE = config.get("meshtastic_knowledge_file", "data/meshtastic_knowledge.txt")
try:
    MESHTASTIC_KB_MAX_CONTEXT = int(config.get("meshtastic_kb_max_context_chars", 3200))
except (ValueError, TypeError):
    MESHTASTIC_KB_MAX_CONTEXT = 4500
MESHTASTIC_KB_MAX_CONTEXT = max(2000, min(MESHTASTIC_KB_MAX_CONTEXT, 12000))
MESHTASTIC_KB_CACHE_TTL = max(0, int(config.get("meshtastic_kb_cache_ttl", 600)))
MESHTASTIC_KB_LOCK = threading.Lock()
MESHTASTIC_KB_CACHE_LOCK = threading.Lock()
MESHTASTIC_KB_CHUNKS: List[Dict[str, Any]] = []
MESHTASTIC_KB_MTIME: Optional[float] = None
MESHTASTIC_KB_WARM_CACHE: Dict[str, Any] = {
    "expires": 0.0,
    "tokens": set(),
    "context": "",
    "matches": [],
}
MESHTASTIC_KB_SYSTEM_PROMPT = (
    "You are Mesh-Master, a safety-critical assistant for the Meshtastic project. "
    "Answer ONLY when the supplied MeshTastic reference passages clearly support the conclusion. "
    "Quote configuration values precisely, prefer step-by-step instructions when relevant, and stay concise. "
    "If the references do not contain the answer, reply exactly: 'I don't have enough MeshTastic data for that.'"
)

LOCATION_HISTORY: Dict[str, Dict[str, Any]] = {}
LOCATION_HISTORY_LOCK = threading.Lock()
LOCATION_HISTORY_RETENTION = 24 * 3600


WEATHER_REPORT_FILE = config.get("weather_report_file", "data/weather_reports.json")
WEATHER_REPORT_LOCK = threading.Lock()
WEATHER_REPORTS: List[Dict[str, Any]] = []
WEATHER_REPORT_MAX_ENTRIES = int(config.get("weather_report_max_entries", 100))
WEATHER_REPORT_RETENTION = int(config.get("weather_report_retention_seconds", 24 * 3600))
WEATHER_REPORT_RECENT_WINDOW = int(config.get("weather_report_recent_window", 2 * 3600))
PENDING_WEATHER_REPORTS: Dict[str, Dict[str, Any]] = {}



# Ensure sane defaults for retention settings
if WEATHER_REPORT_MAX_ENTRIES < 10:
    WEATHER_REPORT_MAX_ENTRIES = 10
if WEATHER_REPORT_RETENTION < 3600:
    WEATHER_REPORT_RETENTION = 3600
if WEATHER_REPORT_RECENT_WINDOW < 900:
    WEATHER_REPORT_RECENT_WINDOW = 900


def _normalize_weather_reports(raw) -> List[Dict[str, Any]]:
    reports: List[Dict[str, Any]] = []
    if isinstance(raw, dict) and 'reports' in raw:
        raw = raw.get('reports')
    if not isinstance(raw, list):
        return reports
    for item in raw:
        if not isinstance(item, dict):
            continue
        entry = {
            'timestamp': float(item.get('timestamp', 0.0) or 0.0),
            'shortname': str(item.get('shortname') or item.get('node') or ''),
            'text': str(item.get('text') or ''),
            'map_url': item.get('map_url') or None,
            'lat': item.get('lat'),
            'lon': item.get('lon'),
            'node_key': item.get('node_key') or None,
        }
        reports.append(entry)
    return reports


try:
    WEATHER_REPORTS = _normalize_weather_reports(safe_load_json(WEATHER_REPORT_FILE, []))
except Exception as exc:
    print(f"⚠️ Could not load weather reports: {exc}")
    WEATHER_REPORTS = []


def _public_base_url() -> str:
    base = os.environ.get("MESH_MASTER_BASE_URL") or config.get("public_base_url")
    if isinstance(base, str) and base.strip():
        return base.rstrip('/')
    try:
        hostname = socket.gethostname()
        host_ip = socket.gethostbyname(hostname)
        host_part = host_ip if host_ip and not host_ip.startswith("127.") else hostname
    except Exception:
        host_part = "localhost"
    return f"http://{host_part}:{SERVER_PORT}"



PROVERB_CACHE: Dict[str, List[str]] = {}
PROVERB_INDEX_TRACKER: Dict[str, int] = {}
PROVERB_LOCK = threading.Lock()
WIPE_CONFIRM_YES = {"y", "yes", "yeah", "yep"}
WIPE_CONFIRM_NO = {"n", "no", "nope", "cancel"}

USB_POWER_CYCLE_OFF_CMD = config.get("usb_power_cycle_off_command")
USB_POWER_CYCLE_ON_CMD = config.get("usb_power_cycle_on_command")
try:
    USB_POWER_CYCLE_DELAY = int(config.get("usb_power_cycle_delay", 3))
    if USB_POWER_CYCLE_DELAY < 1:
        USB_POWER_CYCLE_DELAY = 3
except (TypeError, ValueError):
    USB_POWER_CYCLE_DELAY = 3
USB_POWER_CYCLE_ENABLED = bool(USB_POWER_CYCLE_OFF_CMD and USB_POWER_CYCLE_ON_CMD)
USB_POWER_CYCLE_LOCK = threading.Lock()
USB_POWER_CYCLE_WARNED = False

def _coerce_positive_int(value, default):
    try:
        ivalue = int(value)
        return ivalue if ivalue > 0 else None
    except (TypeError, ValueError):
        return default


def _proverb_language_key(language: Optional[str]) -> str:
    """Reduce language hints to the keys we keep proverbs under."""
    if language:
        normalized = str(language).strip().lower()
        if normalized.startswith("es"):
            return "es"
    return "en"


def _build_proverb_list(path: str, book_label: str) -> List[str]:
    """Extract the book of Proverbs from the given Bible JSON file."""
    try:
        data = safe_load_json(path, {})
    except Exception:
        return []

    if not isinstance(data, dict):
        return []

    books = data.get("books")
    if not isinstance(books, dict):
        return []

    chapters = books.get("Proverbs")
    if not isinstance(chapters, list):
        return []

    verses: List[str] = []
    for chapter_index, chapter in enumerate(chapters, start=1):
        if not isinstance(chapter, list):
            continue
        for verse_index, verse_text in enumerate(chapter, start=1):
            if not isinstance(verse_text, str):
                continue
            text = verse_text.strip()
            if not text:
                continue
            verses.append(f"{book_label} {chapter_index}:{verse_index} {text}")
    return verses


def _load_proverbs(language: Optional[str]) -> List[str]:
    """Return cached Proverbs verses for the requested language."""
    lang_key = _proverb_language_key(language)
    with PROVERB_LOCK:
        cached = PROVERB_CACHE.get(lang_key)
    if cached is not None:
        return cached

    if lang_key == "es":
        verses = _build_proverb_list("data/bible_rvr.json", "Proverbios")
        if not verses:
            verses = _build_proverb_list("data/bible_web.json", "Proverbs")
    else:
        verses = _build_proverb_list("data/bible_web.json", "Proverbs")

    if verses is None:
        verses = []

    with PROVERB_LOCK:
        PROVERB_CACHE[lang_key] = verses
        PROVERB_INDEX_TRACKER.setdefault(lang_key, 0)

    return verses


def _next_proverb(language: Optional[str]) -> str:
    """Return the next proverb for the marquee, advancing the pointer."""
    lang_key = _proverb_language_key(language)
    verses = _load_proverbs(lang_key)
    if not verses:
        return "Proverbs unavailable."

    with PROVERB_LOCK:
        index = PROVERB_INDEX_TRACKER.get(lang_key, 0)
        verse = verses[index % len(verses)]
        PROVERB_INDEX_TRACKER[lang_key] = (index + 1) % len(verses)
    return verse

RADIO_STALE_RX_THRESHOLD = _coerce_positive_int(
    config.get("radio_stale_rx_seconds", RADIO_STALE_RX_THRESHOLD_DEFAULT),
    RADIO_STALE_RX_THRESHOLD_DEFAULT,
)
RADIO_STALE_TX_THRESHOLD = _coerce_positive_int(
    config.get("radio_stale_tx_seconds", RADIO_STALE_TX_THRESHOLD_DEFAULT),
    RADIO_STALE_TX_THRESHOLD_DEFAULT,
)


def _sender_key(sender_id: Any) -> str:
    """Normalize sender identifiers for tracking admin approval."""
    if sender_id is None:
        return ""
    return str(sender_id)



# -----------------------------
# AI Provider & Other Config Vars
# -----------------------------
DEBUG_ENABLED = bool(config.get("debug", False))
CLEAN_LOGS = bool(config.get("clean_logs", True))  # Enable emoji-enhanced clean logging by default
AI_PROVIDER = config.get("ai_provider", "ollama").lower()
SYSTEM_PROMPT = config.get("system_prompt", "You are a helpful assistant responding to mesh network chats.")
OLLAMA_URL = config.get("ollama_url", "http://localhost:11434/api/generate")
OLLAMA_MODEL = config.get("ollama_model", "llama3.2:1b")
OLLAMA_TIMEOUT = config.get("ollama_timeout", 120)
# Max characters of conversation history to include in prompts for Ollama
try:
    OLLAMA_CONTEXT_CHARS = int(config.get("ollama_context_chars", 4000))
except (ValueError, TypeError):
    OLLAMA_CONTEXT_CHARS = 4000
# Ollama model context window (tokens). Set this to match your model's context (e.g., 128000 for 128k)
try:
    OLLAMA_NUM_CTX = int(config.get("ollama_num_ctx", 8192))
except (ValueError, TypeError):
    OLLAMA_NUM_CTX = 8192
# Max messages to include in conversation context (limits to recent exchanges for performance)
try:
    OLLAMA_MAX_MESSAGES = int(config.get("ollama_max_messages", 20))
except (ValueError, TypeError):
    OLLAMA_MAX_MESSAGES = 20
try:
    MAIL_SEARCH_TIMEOUT = int(config.get("mail_search_timeout", 120))
except (ValueError, TypeError):
    MAIL_SEARCH_TIMEOUT = 120
try:
    MAIL_SEARCH_MAX_MESSAGES = int(config.get("mail_search_max_messages", 200))
except (ValueError, TypeError):
    MAIL_SEARCH_MAX_MESSAGES = 200
MAIL_SEARCH_MAX_MESSAGES = max(1, MAIL_SEARCH_MAX_MESSAGES)
MAIL_SEARCH_MODEL = str(config.get("mail_search_model", "llama3.2:1b"))
try:
    MAIL_SEARCH_NUM_CTX = int(config.get("mail_search_num_ctx", min(4096, OLLAMA_NUM_CTX)))
except (ValueError, TypeError):
    MAIL_SEARCH_NUM_CTX = min(4096, OLLAMA_NUM_CTX)
MAIL_SEARCH_NUM_CTX = max(1024, min(MAIL_SEARCH_NUM_CTX, OLLAMA_NUM_CTX))

try:
    SEND_RATE_WINDOW_SECONDS = max(1, int(config.get("send_rate_window_seconds", 5)))
except (TypeError, ValueError):
    SEND_RATE_WINDOW_SECONDS = 5

try:
    SEND_RATE_MAX_MESSAGES = int(config.get("send_rate_max_messages", 20))
except (TypeError, ValueError):
    SEND_RATE_MAX_MESSAGES = 20
if SEND_RATE_MAX_MESSAGES < 1:
    SEND_RATE_MAX_MESSAGES = 0

_SEND_RATE_LOCK = threading.Lock()
_SEND_RATE_EVENTS: deque[float] = deque()


def check_send_rate_limit() -> bool:
    """Simple sliding-window rate limiter for outbound mesh messages."""
    if SEND_RATE_MAX_MESSAGES == 0:
        return True
    now = time.time()
    with _SEND_RATE_LOCK:
        while _SEND_RATE_EVENTS and now - _SEND_RATE_EVENTS[0] > SEND_RATE_WINDOW_SECONDS:
            _SEND_RATE_EVENTS.popleft()
        if len(_SEND_RATE_EVENTS) >= SEND_RATE_MAX_MESSAGES:
            return False
        _SEND_RATE_EVENTS.append(now)
    return True

MAIL_MANAGER = MailManager(
    store_path="mesh_mailboxes.json",
    clean_log=clean_log,
    ai_log=ai_log,
    ollama_url=OLLAMA_URL or None,
    search_model=MAIL_SEARCH_MODEL,
    search_timeout=MAIL_SEARCH_TIMEOUT,
    search_num_ctx=MAIL_SEARCH_NUM_CTX,
    search_max_messages=MAIL_SEARCH_MAX_MESSAGES,
)

GAME_MANAGER = GameManager(
    clean_log=clean_log,
    ai_log=ai_log,
    ollama_url=OLLAMA_URL or None,
    choose_model=MAIL_SEARCH_MODEL,
    choose_timeout=MAIL_SEARCH_TIMEOUT,
    wordladder_model=MAIL_SEARCH_MODEL,
)


# -----------------------------
# AI Personality & Prompt Profiles
# -----------------------------

AI_PERSONALITIES = config.get("ai_personalities")
if not isinstance(AI_PERSONALITIES, list) or not AI_PERSONALITIES:
    AI_PERSONALITIES = [
        {"id": "trail", "name": "Trail", "emoji": "🧭", "aliases": ["trail_scout"] ,"description": "", "prompt": "Adopt the tone of an upbeat trail scout who shares quick, encouraging tips and keeps radio chatter friendly. Keep replies concise yet complete."},
        {"id": "medic", "name": "Medic", "emoji": "🩺", "aliases": ["calm_medic"], "description": "", "prompt": "Respond with the calming assurance of a seasoned field medic, prioritising safety, empathy, and clear next steps. Keep replies concise yet complete."},
        {"id": "trickster", "name": "Trickster", "emoji": "😏", "aliases": ["radio_trickster"], "description": "", "prompt": "Speak like a mischievous radio operator who peppers helpful advice with witty teasing and light sarcasm. Keep replies concise yet complete."},
        {"id": "mission", "name": "Mission", "emoji": "🛰️", "aliases": ["mission_control"], "description": "", "prompt": "Respond like a NASA-style mission controller: structured, crisp, and focused on objectives, risks, and next steps. Keep replies concise yet complete."},
        {"id": "data", "name": "Data", "emoji": "📊", "aliases": ["data_nerd"], "description": "", "prompt": "Use an analytical tone, citing data points, probabilities, and quick calculations whenever possible. Keep replies concise yet complete."},
        {"id": "desert", "name": "Desert", "emoji": "🌵", "aliases": ["desert_sage"], "description": "", "prompt": "Answer with calm, poetic desert wisdom—mix practical survival insight with imagery of sand, stars, and resilience. Keep replies concise yet complete."},
        {"id": "chaplain", "name": "Chaplain", "emoji": "🕯️", "aliases": [], "description": "", "prompt": "Offer gentle encouragement and hopeful perspective, akin to a compassionate chaplain supporting weary operators. Keep replies concise yet complete."},
        {"id": "engineer", "name": "Engineer", "emoji": "⚙️", "aliases": ["comms_engineer"], "description": "", "prompt": "Respond like a communications engineer—precision-focused, thrilled to troubleshoot hardware and signal paths in depth. Keep replies concise yet complete."},
        {"id": "rookie", "name": "Rookie", "emoji": "📻", "aliases": ["rookie"], "description": "", "prompt": "Use a high-energy, eager tone—like a rookie wingman excited to help and quick to celebrate small victories. Keep replies concise yet complete."},
        {"id": "story", "name": "Story", "emoji": "🔥", "aliases": ["storyteller"], "description": "", "prompt": "Wrap answers in short, vivid campfire stories, blending narrative flair with useful guidance. Keep replies concise yet complete."},
        {"id": "evangelist", "name": "Evangelist", "emoji": "🎤", "aliases": [], "description": "", "prompt": "Sound like a passionate evangelist: celebrate wins, rally the crew toward action, and sprinkle inspiring slogans. Keep replies concise yet complete."},
        {"id": "sassy", "name": "Sassy", "emoji": "💅", "aliases": [], "description": "", "prompt": "Respond with affectionate sass—clever quips, eye-roll energy, but always deliver the helpful answer. Keep replies concise yet complete."},
        {"id": "mechanic", "name": "Mechanic", "emoji": "🛠️", "aliases": ["gangster"], "description": "", "prompt": "Speak with hands-on fixer energy—practical swagger, loyal crew vibes, and grounded guidance. Keep replies concise yet complete."},
        {"id": "hippie", "name": "Hippie", "emoji": "🌈", "aliases": [], "description": "", "prompt": "Share mellow, peace-first guidance with gentle optimism and communal spirit. Keep replies concise yet complete."},
        {"id": "millennial", "name": "Millennial", "emoji": "📱", "aliases": [], "description": "", "prompt": "Answer with upbeat, meme-aware millennial energy—empathetic, tech-savvy, and practical. Keep replies concise yet complete."},
    ]

AI_PERSONALITY_MAP: Dict[str, Dict[str, str]] = {}
AI_PERSONALITY_LOOKUP: Dict[str, str] = {}
AI_PERSONALITY_ORDER: List[str] = []


def _register_personality_alias(alias: str, persona_id: str) -> None:
    if not alias:
        return
    normalized = alias.strip().lower()
    if not normalized:
        return
    AI_PERSONALITY_LOOKUP.setdefault(normalized, persona_id)
    collapsed = re.sub(r"[^a-z0-9]+", "", normalized)
    if collapsed:
        AI_PERSONALITY_LOOKUP.setdefault(collapsed, persona_id)
    underscored = re.sub(r"[^a-z0-9]+", "_", normalized).strip("_")
    if underscored:
        AI_PERSONALITY_LOOKUP.setdefault(underscored, persona_id)


for persona in AI_PERSONALITIES:
    persona_id = persona.get("id")
    if not isinstance(persona_id, str):
        continue
    persona_id = persona_id.strip()
    if not persona_id:
        continue
    persona.setdefault("name", persona_id.title())
    persona.setdefault("emoji", "🧠")
    persona.setdefault("description", "")
    persona.setdefault("prompt", "")
    AI_PERSONALITY_MAP[persona_id] = persona
    AI_PERSONALITY_ORDER.append(persona_id)
    _register_personality_alias(persona_id, persona_id)
    _register_personality_alias(persona.get("name"), persona_id)
    for extra_alias in persona.get("aliases", []) or []:
        _register_personality_alias(extra_alias, persona_id)

DEFAULT_PERSONALITY_ID = config.get("default_personality_id")
if DEFAULT_PERSONALITY_ID not in AI_PERSONALITY_MAP:
    fallback_id = AI_PERSONALITY_LOOKUP.get(str(DEFAULT_PERSONALITY_ID).lower())
    if fallback_id and fallback_id in AI_PERSONALITY_MAP:
        DEFAULT_PERSONALITY_ID = fallback_id
    else:
        DEFAULT_PERSONALITY_ID = next(iter(AI_PERSONALITY_MAP), None)

USER_AI_SETTINGS_FILE = config.get("user_ai_settings_file", "user_ai_settings.json")
USER_AI_SETTINGS_LOCK = threading.Lock()
USER_AI_SETTINGS: Dict[str, Dict[str, str]] = {}


def _canonical_personality_id(candidate: Optional[str]) -> Optional[str]:
    if not candidate:
        return None
    candidate_str = str(candidate).strip()
    if not candidate_str:
        return None
    text = candidate_str.lower()
    if not text:
        return None
    lookup = AI_PERSONALITY_LOOKUP.get(text)
    if lookup:
        return lookup
    collapsed = re.sub(r"[^a-z0-9]+", "", text)
    if collapsed:
        return AI_PERSONALITY_LOOKUP.get(collapsed)
    underscored = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
    if underscored:
        return AI_PERSONALITY_LOOKUP.get(underscored)
    return None


def _default_personality_id() -> Optional[str]:
    if DEFAULT_PERSONALITY_ID:
        return DEFAULT_PERSONALITY_ID
    return next(iter(AI_PERSONALITY_MAP), None)


def _sanitize_prompt_text(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    text = str(value).strip()
    return text or None


def _load_user_ai_settings_from_disk() -> Dict[str, Dict[str, str]]:
    raw = safe_load_json(USER_AI_SETTINGS_FILE, {})
    settings: Dict[str, Dict[str, str]] = {}
    if not isinstance(raw, dict):
        return settings
    for key, entry in raw.items():
        if not isinstance(key, str) or not key:
            continue
        if not isinstance(entry, dict):
            continue
        persona_id = _canonical_personality_id(entry.get("personality_id"))
        prompt_override = entry.get("prompt_override") or entry.get("custom_prompt") or entry.get("prompt")
        prompt_override = _sanitize_prompt_text(prompt_override)
        cleaned: Dict[str, str] = {}
        if persona_id:
            cleaned["personality_id"] = persona_id
        if prompt_override:
            cleaned["prompt_override"] = prompt_override
        if cleaned:
            settings[key] = cleaned
    return settings


def _save_user_ai_settings_to_disk(settings: Dict[str, Dict[str, str]]) -> None:
    try:
        payload: Dict[str, Dict[str, str]] = {}
        for key, entry in settings.items():
            if not isinstance(key, str) or not key:
                continue
            if not isinstance(entry, dict):
                continue
            persona_id = _canonical_personality_id(entry.get("personality_id"))
            prompt_override = _sanitize_prompt_text(entry.get("prompt_override"))
            item: Dict[str, str] = {}
            if persona_id:
                item["personality_id"] = persona_id
            if prompt_override:
                item["prompt_override"] = prompt_override
            if item:
                payload[key] = item
        directory = os.path.dirname(USER_AI_SETTINGS_FILE)
        if directory:
            os.makedirs(directory, exist_ok=True)
        write_atomic(USER_AI_SETTINGS_FILE, json.dumps(payload, indent=2, sort_keys=True))
        dprint(f"Saved {len(payload)} AI preference profiles to {USER_AI_SETTINGS_FILE}")
    except Exception as exc:
        print(f"⚠️ Failed to save {USER_AI_SETTINGS_FILE}: {exc}")


with USER_AI_SETTINGS_LOCK:
    USER_AI_SETTINGS.update(_load_user_ai_settings_from_disk())


def _snapshot_user_ai_settings() -> Dict[str, Dict[str, str]]:
    return {key: dict(value) for key, value in USER_AI_SETTINGS.items() if isinstance(key, str)}


def _get_user_ai_preferences(sender_key: Optional[str]) -> Dict[str, Optional[str]]:
    persona_id = _default_personality_id()
    prompt_override: Optional[str] = None
    if sender_key:
        with USER_AI_SETTINGS_LOCK:
            entry = USER_AI_SETTINGS.get(sender_key)
            if isinstance(entry, dict):
                stored_persona = _canonical_personality_id(entry.get("personality_id"))
                if stored_persona:
                    persona_id = stored_persona
                prompt_override = _sanitize_prompt_text(entry.get("prompt_override"))
    return {
        "personality_id": persona_id,
        "prompt_override": prompt_override,
    }


def _set_user_personality(sender_key: str, persona_id: str) -> bool:
    canonical = _canonical_personality_id(persona_id)
    if not canonical:
        return False
    with USER_AI_SETTINGS_LOCK:
        entry = dict(USER_AI_SETTINGS.get(sender_key, {}))
        entry["personality_id"] = canonical
        USER_AI_SETTINGS[sender_key] = entry
        snapshot = _snapshot_user_ai_settings()
    _save_user_ai_settings_to_disk(snapshot)
    return True


def _clear_user_personality(sender_key: str) -> None:
    with USER_AI_SETTINGS_LOCK:
        entry = USER_AI_SETTINGS.get(sender_key)
        if isinstance(entry, dict):
            entry.pop("personality_id", None)
            if entry:
                USER_AI_SETTINGS[sender_key] = dict(entry)
            else:
                USER_AI_SETTINGS.pop(sender_key, None)
        snapshot = _snapshot_user_ai_settings()
    _save_user_ai_settings_to_disk(snapshot)


def _set_user_prompt_override(sender_key: str, prompt: Optional[str]) -> None:
    cleaned = _sanitize_prompt_text(prompt)
    with USER_AI_SETTINGS_LOCK:
        if not cleaned:
            entry = USER_AI_SETTINGS.get(sender_key)
            if isinstance(entry, dict):
                entry.pop("prompt_override", None)
                if entry:
                    USER_AI_SETTINGS[sender_key] = dict(entry)
                else:
                    USER_AI_SETTINGS.pop(sender_key, None)
        else:
            entry = dict(USER_AI_SETTINGS.get(sender_key, {}))
            entry["prompt_override"] = cleaned
            USER_AI_SETTINGS[sender_key] = entry
        snapshot = _snapshot_user_ai_settings()
    _save_user_ai_settings_to_disk(snapshot)


def _reset_user_personality(sender_key: str) -> None:
    _clear_user_personality(sender_key)
    _set_user_prompt_override(sender_key, None)


def build_system_prompt_for_sender(sender_id: Any) -> str:
    base = _sanitize_prompt_text(SYSTEM_PROMPT) or "You are a helpful assistant responding to mesh network chats."
    segments = [base]
    persona_id: Optional[str] = None
    prompt_override: Optional[str] = None
    if sender_id is not None:
        sender_key = _safe_sender_key(sender_id)
        prefs = _get_user_ai_preferences(sender_key)
        persona_id = prefs.get("personality_id")
        prompt_override = prefs.get("prompt_override")
    else:
        persona_id = _default_personality_id()
    if persona_id and persona_id in AI_PERSONALITY_MAP:
        persona_prompt = _sanitize_prompt_text(AI_PERSONALITY_MAP[persona_id].get("prompt"))
        if persona_prompt:
            segments.append(persona_prompt)
    if prompt_override:
        segments.append(prompt_override)
    joined = "\n\n".join(part for part in segments if part)
    return joined or base


def describe_personality(persona_id: str) -> Optional[str]:
    persona = AI_PERSONALITY_MAP.get(persona_id)
    if not persona:
        return None
    emoji = persona.get("emoji") or ""
    name = persona.get("name") or persona_id
    description = persona.get("description") or ""
    prompt = persona.get("prompt") or ""
    lines = [f"{emoji} {name} ({persona_id})"]
    if description:
        lines.append(description)
    if prompt:
        lines.append(f"Prompt: {prompt}")
    return "\n".join(lines)


def _format_personality_summary(sender_key: Optional[str]) -> str:
    prefs = _get_user_ai_preferences(sender_key)
    persona_id = prefs.get("personality_id")
    prompt_override = prefs.get("prompt_override")
    persona = AI_PERSONALITY_MAP.get(persona_id) if persona_id else None
    emoji = persona.get("emoji") if persona else "🧠"
    name = persona.get("name") if persona else "Default"
    description = persona.get("description") if persona else ""
    lines = [f"{emoji} Current personality: {name}" if persona_id else "🧠 Using default personality."]
    if description and persona_id:
        lines.append(description)
    if prompt_override:
        lines.append("Custom prompt: active.")
    else:
        lines.append("Custom prompt: none.")
    lines.append("Commands: set <name> | prompt <text> | prompt clear | reset")
    lines.append("Browse vibes with /changevibe.")
    return "\n".join(lines)


PERSONALITY_PAGE_SIZE = 5


def _format_personality_list(current_id: Optional[str], page: int = 1) -> str:
    total = len(AI_PERSONALITY_ORDER)
    if total == 0:
        return "No vibes configured."
    max_page = max(1, (total + PERSONALITY_PAGE_SIZE - 1) // PERSONALITY_PAGE_SIZE)
    page = max(1, min(page, max_page))
    start = (page - 1) * PERSONALITY_PAGE_SIZE
    end = start + PERSONALITY_PAGE_SIZE

    entries: List[str] = []
    for persona_id in AI_PERSONALITY_ORDER[start:end]:
        persona = AI_PERSONALITY_MAP.get(persona_id, {})
        name = persona.get("name") or persona_id
        emoji = persona.get("emoji") or "🧠"
        highlight = "⭐" if persona_id == current_id else ""
        entries.append(f"{emoji} {name}{highlight}")

    lines: List[str] = [f"Vibes {page}/{max_page}: {', '.join(entries)}"]
    lines.append("Set with `/aipersonality set <name>`.")
    if max_page > 1:
        if page < max_page:
            lines.append(f"More: `/changevibe {page + 1}`")
        else:
            lines.append("Back to start: `/changevibe 1`")
    return "\n".join(lines)


HOME_ASSISTANT_URL = config.get("home_assistant_url", "")
HOME_ASSISTANT_TOKEN = config.get("home_assistant_token", "")
HOME_ASSISTANT_TIMEOUT = config.get("home_assistant_timeout", 30)
HOME_ASSISTANT_ENABLE_PIN = bool(config.get("home_assistant_enable_pin", False))
HOME_ASSISTANT_SECURE_PIN = str(config.get("home_assistant_secure_pin", "1234"))
HOME_ASSISTANT_ENABLED = bool(config.get("home_assistant_enabled", False))
try:
    HOME_ASSISTANT_CHANNEL_INDEX = int(config.get("home_assistant_channel_index", -1))
except (ValueError, TypeError):
    HOME_ASSISTANT_CHANNEL_INDEX = -1
MAX_CHUNK_SIZE = config.get("chunk_size", 200)
MAX_CHUNKS = 5
CHUNK_DELAY = config.get("chunk_buffer_seconds", config.get("chunk_delay", 4))
MAX_RESPONSE_LENGTH = MAX_CHUNK_SIZE * MAX_CHUNKS
LOCAL_LOCATION_STRING = config.get("local_location_string", "Unknown Location")
AI_NODE_NAME = config.get("ai_node_name", "AI-Bot")
FORCE_NODE_NUM = config.get("force_node_num", None)
try:
    MAX_MESSAGE_LOG = int(config.get("max_message_log", 100))  # 0 or less means unlimited
except (ValueError, TypeError):
    MAX_MESSAGE_LOG = 100


ALERT_BELL_KEYWORDS = {
    "🔔 alert bell character!",
    "alert bell character!",
    "alert bell character",
}

try:
    BIBLE_WEB_DATA = safe_load_json("data/bible_web.json", {})
    if not isinstance(BIBLE_WEB_DATA, dict):
        BIBLE_WEB_DATA = {}
except Exception:
    BIBLE_WEB_DATA = {}

BIBLE_WEB_BOOKS = BIBLE_WEB_DATA.get("books", {})
BIBLE_WEB_VERSES = BIBLE_WEB_DATA.get("verses", [])
BIBLE_WEB_ORDER = BIBLE_WEB_DATA.get("book_order", list(BIBLE_WEB_BOOKS.keys()))

try:
    BIBLE_RVR_DATA = safe_load_json("data/bible_rvr.json", {})
    if not isinstance(BIBLE_RVR_DATA, dict):
        BIBLE_RVR_DATA = {}
except Exception:
    BIBLE_RVR_DATA = {}

BIBLE_RVR_BOOKS = BIBLE_RVR_DATA.get("books", {})
BIBLE_RVR_VERSES = BIBLE_RVR_DATA.get("verses", [])
BIBLE_RVR_ORDER = BIBLE_RVR_DATA.get("book_order", list(BIBLE_RVR_BOOKS.keys()))
BIBLE_RVR_DISPLAY = BIBLE_RVR_DATA.get("display_names", {})
BIBLE_RVR_ABBREVIATIONS = {
    key: list(value) if isinstance(value, (list, tuple)) else [value]
    for key, value in (BIBLE_RVR_DATA.get("abbreviations", {}) or {}).items()
}

BIBLE_PROGRESS_FILE = config.get("bible_progress_file", "data/bible_progress.json")
try:
    BIBLE_PROGRESS = safe_load_json(BIBLE_PROGRESS_FILE, {})
    if not isinstance(BIBLE_PROGRESS, dict):
        BIBLE_PROGRESS = {}
except Exception:
    BIBLE_PROGRESS = {}
BIBLE_PROGRESS_LOCK = threading.Lock()

SERIAL_PORT = config.get("serial_port", "")
try:
    SERIAL_BAUD = int(config.get("serial_baud", 115200))
except (ValueError, TypeError):
    SERIAL_BAUD = 115200
USE_WIFI = bool(config.get("use_wifi", False))
WIFI_HOST = config.get("wifi_host") or None
try:
    WIFI_PORT = int(config.get("wifi_port", 4403))
except (ValueError, TypeError):
    WIFI_PORT = 4403
USE_MESH_INTERFACE = bool(config.get("use_mesh_interface", False))

AUTO_REFRESH_ENABLED = bool(config.get("auto_refresh_enabled", False))
try:
    AUTO_REFRESH_MINUTES = max(1, int(config.get("auto_refresh_minutes", 60)))
except (ValueError, TypeError):
    AUTO_REFRESH_MINUTES = 60

BIBLE_SPANISH_DISPLAY_OVERRIDES = {
    "Genesis": "Génesis",
    "Exodus": "Éxodo",
    "Leviticus": "Levítico",
    "Numbers": "Números",
    "Deuteronomy": "Deuteronomio",
    "Joshua": "Josué",
    "Judges": "Jueces",
    "Ruth": "Rut",
    "1 Samuel": "1 Samuel",
    "2 Samuel": "2 Samuel",
    "1 Kings": "1 Reyes",
    "2 Kings": "2 Reyes",
    "1 Chronicles": "1 Crónicas",
    "2 Chronicles": "2 Crónicas",
    "Ezra": "Esdras",
    "Nehemiah": "Nehemías",
    "Esther": "Ester",
    "Job": "Job",
    "Psalms": "Salmos",
    "Proverbs": "Proverbios",
    "Ecclesiastes": "Eclesiastés",
    "Song of Solomon": "Cantares",
    "Isaiah": "Isaías",
    "Jeremiah": "Jeremías",
    "Lamentations": "Lamentaciones",
    "Ezekiel": "Ezequiel",
    "Daniel": "Daniel",
    "Hosea": "Oseas",
    "Joel": "Joel",
    "Amos": "Amós",
    "Obadiah": "Abdías",
    "Jonah": "Jonás",
    "Micah": "Miqueas",
    "Nahum": "Nahúm",
    "Habakkuk": "Habacuc",
    "Zephaniah": "Sofonías",
    "Haggai": "Hageo",
    "Zechariah": "Zacarías",
    "Malachi": "Malaquías",
    "Matthew": "Mateo",
    "Mark": "Marcos",
    "Luke": "Lucas",
    "John": "Juan",
    "Acts": "Hechos",
    "Romans": "Romanos",
    "1 Corinthians": "1 Corintios",
    "2 Corinthians": "2 Corintios",
    "Galatians": "Gálatas",
    "Ephesians": "Efesios",
    "Philippians": "Filipenses",
    "Colossians": "Colosenses",
    "1 Thessalonians": "1 Tesalonicenses",
    "2 Thessalonians": "2 Tesalonicenses",
    "1 Timothy": "1 Timoteo",
    "2 Timothy": "2 Timoteo",
    "Titus": "Tito",
    "Philemon": "Filemón",
    "Hebrews": "Hebreos",
    "James": "Santiago",
    "1 Peter": "1 Pedro",
    "2 Peter": "2 Pedro",
    "1 John": "1 Juan",
    "2 John": "2 Juan",
    "3 John": "3 Juan",
    "Jude": "Judas",
    "Revelation": "Apocalipsis",
}

BIBLE_SPANISH_ALIAS_OVERRIDES = {
    "Genesis": ["Gn"],
    "Exodus": ["Ex", "Éx", "Exo"],
    "Leviticus": ["Lv"],
    "Numbers": ["Nm", "Num"],
    "Deuteronomy": ["Dt"],
    "Joshua": ["Jos"],
    "Judges": ["Jue", "Juec"],
    "Ruth": ["Rut"],
    "1 Samuel": ["1 S", "1Sam", "1Sa"],
    "2 Samuel": ["2 S", "2Sam", "2Sa"],
    "1 Kings": ["1 R", "1Re"],
    "2 Kings": ["2 R", "2Re"],
    "1 Chronicles": ["1 Cr", "1Cro"],
    "2 Chronicles": ["2 Cr", "2Cro"],
    "Ezra": ["Esd"],
    "Nehemiah": ["Neh"],
    "Esther": ["Est"],
    "Job": [],
    "Psalms": ["Sal"],
    "Proverbs": ["Prov"],
    "Ecclesiastes": ["Ecl", "Ecles"],
    "Song of Solomon": ["Cant", "Cantar"],
    "Isaiah": ["Is"],
    "Jeremiah": ["Jer"],
    "Lamentations": ["Lam"],
    "Ezekiel": ["Ez", "Eze"],
    "Daniel": ["Dn"],
    "Hosea": ["Os"],
    "Joel": ["Jl"],
    "Amos": ["Am"],
    "Obadiah": ["Abd"],
    "Jonah": ["Jon"],
    "Micah": ["Miq", "Mi"],
    "Nahum": ["Nah"],
    "Habakkuk": ["Hab"],
    "Zephaniah": ["Sof"],
    "Haggai": ["Hag"],
    "Zechariah": ["Zac"],
    "Malachi": ["Mal"],
    "Matthew": ["Mt"],
    "Mark": ["Mr", "Mc"],
    "Luke": ["Lc"],
    "John": ["Jn"],
    "Acts": ["Hch", "Hech"],
    "Romans": ["Ro"],
    "1 Corinthians": ["1 Co", "1Cor"],
    "2 Corinthians": ["2 Co", "2Cor"],
    "Galatians": ["Ga"],
    "Ephesians": ["Ef"],
    "Philippians": ["Fil"],
    "Colossians": ["Col"],
    "1 Thessalonians": ["1 Tes"],
    "2 Thessalonians": ["2 Tes"],
    "1 Timothy": ["1 Ti"],
    "2 Timothy": ["2 Ti"],
    "Titus": ["Tit"],
    "Philemon": ["Flm"],
    "Hebrews": ["Heb"],
    "James": ["Stg", "Sant"],
    "1 Peter": ["1 Pe"],
    "2 Peter": ["2 Pe"],
    "1 John": ["1 Jn"],
    "2 John": ["2 Jn"],
    "3 John": ["3 Jn"],
    "Jude": ["Jud"],
    "Revelation": ["Ap"],
}

for canonical, display in BIBLE_SPANISH_DISPLAY_OVERRIDES.items():
    BIBLE_RVR_DISPLAY[canonical] = display
    BIBLE_RVR_ABBREVIATIONS.setdefault(canonical, [])
    for alias in BIBLE_SPANISH_ALIAS_OVERRIDES.get(canonical, []):
        if alias and alias not in BIBLE_RVR_ABBREVIATIONS[canonical]:
            BIBLE_RVR_ABBREVIATIONS[canonical].append(alias)

BIBLE_VERSES_DATA_ES = safe_load_json("bible_jesus_verses_es.json", [])
if not BIBLE_VERSES_DATA_ES and BIBLE_RVR_VERSES:
    BIBLE_VERSES_DATA_ES = BIBLE_RVR_VERSES

BIBLE_BOOK_BASE_ALIASES = {
    "Genesis": ["Gen", "Gn"],
    "Exodus": ["Exo", "Ex"],
    "Leviticus": ["Lev", "Lv"],
    "Numbers": ["Num", "Nm", "Nb"],
    "Deuteronomy": ["Deut", "Dt", "Deu"],
    "Joshua": ["Josh", "Jos"],
    "Judges": ["Judg", "Jdg", "Jdgs"],
    "Ruth": ["Rut", "Ru"],
    "1 Samuel": ["Sam", "Samuel"],
    "2 Samuel": ["Sam", "Samuel"],
    "1 Kings": ["Kings", "Kgs", "Kin"],
    "2 Kings": ["Kings", "Kgs", "Kin"],
    "1 Chronicles": ["Chronicles", "Chron", "Chr"],
    "2 Chronicles": ["Chronicles", "Chron", "Chr"],
    "Ezra": ["Ezr"],
    "Nehemiah": ["Neh", "Ne"],
    "Esther": ["Est", "Es"],
    "Job": ["Job"],
    "Psalms": ["Psalm", "Ps", "Psa", "Psm", "Psal"],
    "Proverbs": ["Prov", "Prv", "Pr"],
    "Ecclesiastes": ["Eccl", "Ecc", "Qoheleth"],
    "Song of Solomon": ["Song of Songs", "Song", "Canticles", "SoS"],
    "Isaiah": ["Isa", "Is"],
    "Jeremiah": ["Jer", "Je"],
    "Lamentations": ["Lam", "La"],
    "Ezekiel": ["Ezek", "Eze", "Ez"],
    "Daniel": ["Dan", "Da"],
    "Hosea": ["Hos", "Ho"],
    "Joel": ["Joe", "Jl"],
    "Amos": ["Am"],
    "Obadiah": ["Obad", "Ob"],
    "Jonah": ["Jon", "Jh"],
    "Micah": ["Mic", "Mc"],
    "Nahum": ["Nah", "Na"],
    "Habakkuk": ["Hab", "Hb"],
    "Zephaniah": ["Zeph", "Zep"],
    "Haggai": ["Hag", "Hg"],
    "Zechariah": ["Zech", "Zec", "Zc"],
    "Malachi": ["Mal", "Ml"],
    "Matthew": ["Matt", "Mt"],
    "Mark": ["Mk", "Mrk"],
    "Luke": ["Lk", "Lu"],
    "John": ["Jn", "Jhn"],
    "Acts": ["Acts of the Apostles", "Act", "Ac"],
    "Romans": ["Rom", "Ro"],
    "1 Corinthians": ["Cor", "Corinthians", "Co"],
    "2 Corinthians": ["Cor", "Corinthians", "Co"],
    "Galatians": ["Gal", "Ga"],
    "Ephesians": ["Eph", "Ep"],
    "Philippians": ["Phil", "Php", "Philp"],
    "Colossians": ["Col", "Co"],
    "1 Thessalonians": ["Thessalonians", "Thess", "Thes"],
    "2 Thessalonians": ["Thessalonians", "Thess", "Thes"],
    "1 Timothy": ["Timothy", "Tim"],
    "2 Timothy": ["Timothy", "Tim"],
    "Titus": ["Tit", "Ti"],
    "Philemon": ["Philem", "Phm"],
    "Hebrews": ["Heb", "He"],
    "James": ["Jas", "Jm"],
    "1 Peter": ["Peter", "Pet", "Pe"],
    "2 Peter": ["Peter", "Pet", "Pe"],
    "1 John": ["Jn", "Joh"],
    "2 John": ["Jn", "Joh"],
    "3 John": ["Jn", "Joh"],
    "Jude": ["Jud", "Jd"],
    "Revelation": ["Rev", "Revelations", "Apocalypse", "Re"],
}

_BIBLE_ROMAN_NUMERALS = {"i": "1", "ii": "2", "iii": "3"}
_BIBLE_ORDINAL_WORDS = {
    "first": "1",
    "second": "2",
    "third": "3",
    "one": "1",
    "two": "2",
    "three": "3",
}
_BIBLE_ORDINAL_SUFFIXES = {
    "1st": "1",
    "2nd": "2",
    "3rd": "3",
}
_BIBLE_ORDINAL_WORDS_REV = {"1": "first", "2": "second", "3": "third"}
_BIBLE_ORDINAL_SUFFIXES_REV = {"1": "1st", "2": "2nd", "3": "3rd"}
_BIBLE_ROMAN_NUMERALS_REV = {"1": "i", "2": "ii", "3": "iii"}


def _normalize_book_key(value: Optional[str]) -> str:
    if not value:
        return ""
    cleaned = unidecode(str(value)).lower()
    cleaned = cleaned.replace("’", "'")
    cleaned = cleaned.replace("&", " and ")
    cleaned = re.sub(r"[^a-z0-9\s]", " ", cleaned)
    tokens = []
    for token in cleaned.split():
        if token in _BIBLE_ORDINAL_SUFFIXES:
            tokens.append(_BIBLE_ORDINAL_SUFFIXES[token])
        elif token in _BIBLE_ORDINAL_WORDS:
            tokens.append(_BIBLE_ORDINAL_WORDS[token])
        elif token in _BIBLE_ROMAN_NUMERALS:
            tokens.append(_BIBLE_ROMAN_NUMERALS[token])
        else:
            tokens.append(token)
    normalized = " ".join(tokens)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def _register_bible_alias(alias_map: Dict[str, str], alias: str, canonical: str) -> None:
    key = _normalize_book_key(alias)
    if not key:
        return
    alias_map.setdefault(key, canonical)
    compact = key.replace(" ", "")
    if compact:
        alias_map.setdefault(compact, canonical)


def _generate_bible_alias_map() -> Dict[str, str]:
    alias_map: Dict[str, str] = {}
    if not BIBLE_WEB_BOOKS:
        return alias_map

    book_sequence = BIBLE_WEB_ORDER or sorted(BIBLE_WEB_BOOKS.keys())
    for canonical in book_sequence:
        _register_bible_alias(alias_map, canonical, canonical)
        _register_bible_alias(alias_map, canonical.replace(" ", ""), canonical)
        roots = list(BIBLE_BOOK_BASE_ALIASES.get(canonical, []))
        spanish_name = BIBLE_RVR_DISPLAY.get(canonical)
        if spanish_name:
            roots.append(spanish_name)
        for abbr in BIBLE_RVR_ABBREVIATIONS.get(canonical, []) or []:
            if abbr:
                roots.append(str(abbr))
        match = re.match(r"([1-3])\s+(.*)", canonical)
        if match:
            number = match.group(1)
            rest = match.group(2).strip()
            bases = [rest] + roots
            roman = _BIBLE_ROMAN_NUMERALS_REV[number]
            ordinal_word = _BIBLE_ORDINAL_WORDS_REV[number]
            ordinal_suffix = _BIBLE_ORDINAL_SUFFIXES_REV[number]
            for base in bases:
                base_clean = base.strip()
                if not base_clean:
                    continue
                base_compact = base_clean.replace(" ", "")
                variants = {
                    f"{number} {base_clean}",
                    f"{number}{base_clean}",
                    f"{number} {base_compact}",
                    f"{number}{base_compact}",
                    f"{ordinal_word} {base_clean}",
                    f"{ordinal_word} {base_compact}",
                    f"{ordinal_suffix} {base_clean}",
                    f"{ordinal_suffix}{base_compact}",
                    f"{roman} {base_clean}",
                    f"{roman.upper()} {base_clean}",
                    f"{roman}{base_clean}",
                    f"{roman.upper()}{base_clean}",
                }
                for variant in variants:
                    _register_bible_alias(alias_map, variant, canonical)
        else:
            for alias in roots:
                _register_bible_alias(alias_map, alias, canonical)
                _register_bible_alias(alias_map, alias.replace(" ", ""), canonical)
    return alias_map


BIBLE_BOOK_ALIAS_MAP = _generate_bible_alias_map()
BIBLE_BOOK_ALIAS_KEYS = list(BIBLE_BOOK_ALIAS_MAP.keys())


def _display_book_name(canonical: str, language: str) -> str:
    if language == 'es':
        return BIBLE_RVR_DISPLAY.get(canonical, canonical)
    return canonical


def _get_book_order() -> List[str]:
    if BIBLE_WEB_ORDER:
        return BIBLE_WEB_ORDER
    if BIBLE_RVR_ORDER:
        return BIBLE_RVR_ORDER
    if BIBLE_WEB_BOOKS:
        return list(BIBLE_WEB_BOOKS.keys())
    return list(BIBLE_RVR_BOOKS.keys())


def _get_books_and_language(book: str, preferred_language: str = 'en') -> Tuple[Optional[Dict[str, List[List[str]]]], Optional[str]]:
    attempts: List[Tuple[str, Dict[str, List[List[str]]]]] = []
    if preferred_language == 'es':
        attempts = [('es', BIBLE_RVR_BOOKS), ('en', BIBLE_WEB_BOOKS)]
    else:
        attempts = [('en', BIBLE_WEB_BOOKS), ('es', BIBLE_RVR_BOOKS)]
    for lang, books in attempts:
        if book in books:
            return books, lang
    for lang, books in [('es', BIBLE_RVR_BOOKS), ('en', BIBLE_WEB_BOOKS)]:
        if book in books:
            return books, lang
    return None, None


def _get_chapter_count(book: str, preferred_language: str = 'en') -> Tuple[int, str]:
    books, lang = _get_books_and_language(book, preferred_language)
    if not books:
        return 0, preferred_language
    return len(books.get(book, [])), lang or preferred_language


def _get_chapter_length(book: str, chapter: int, preferred_language: str = 'en') -> Tuple[int, str]:
    books, lang = _get_books_and_language(book, preferred_language)
    if not books:
        return 0, preferred_language
    chapters = books.get(book, [])
    if chapter < 1 or chapter > len(chapters):
        return 0, lang or preferred_language
    return len(chapters[chapter - 1]), lang or preferred_language


def _next_book(book: str) -> Optional[str]:
    order = _get_book_order()
    if book not in order:
        return None
    idx = order.index(book)
    if idx >= len(order) - 1:
        return None
    return order[idx + 1]


def _prev_book(book: str) -> Optional[str]:
    order = _get_book_order()
    if book not in order:
        return None
    idx = order.index(book)
    if idx <= 0:
        return None
    return order[idx - 1]


def _save_bible_progress() -> None:
    try:
        directory = os.path.dirname(BIBLE_PROGRESS_FILE)
        if directory:
            os.makedirs(directory, exist_ok=True)
        write_atomic(BIBLE_PROGRESS_FILE, json.dumps(BIBLE_PROGRESS, indent=2, sort_keys=True))
    except Exception as exc:
        print(f"⚠️ Could not save {BIBLE_PROGRESS_FILE}: {exc}")


def _get_user_bible_progress(sender_key: str, preferred_language: str) -> Dict[str, Any]:
    if not sender_key:
        return {}
    with BIBLE_PROGRESS_LOCK:
        progress = BIBLE_PROGRESS.get(sender_key)
        if progress:
            return progress
        order = _get_book_order()
        default_book = order[0] if order else 'Genesis'
        default_language = 'es' if preferred_language == 'es' and default_book in BIBLE_RVR_BOOKS else 'en'
        progress = {
            'book': default_book,
            'chapter': 1,
            'verse': 1,
            'language': default_language,
            'span': 0,
        }
        BIBLE_PROGRESS[sender_key] = progress
        _save_bible_progress()
        return progress


def _update_bible_progress(
    sender_key: Optional[str],
    book: str,
    chapter: int,
    verse: int,
    language: str,
    span: int,
) -> None:
    if not sender_key or not book:
        return
    span = max(0, span)
    chapter = max(1, chapter)
    verse = max(1, verse)
    books_source, lang_used = _get_books_and_language(book, language)
    if not books_source or not lang_used:
        lang_used = language or 'en'
    chapter_count = len(books_source.get(book, [])) if books_source and book in books_source else 0
    if chapter_count > 0:
        if chapter > chapter_count:
            chapter = chapter_count
        chapter_len, lang_used = _get_chapter_length(book, chapter, lang_used)
        if chapter_len > 0 and verse > chapter_len:
            verse = chapter_len
    with BIBLE_PROGRESS_LOCK:
        BIBLE_PROGRESS[sender_key] = {
            'book': book,
            'chapter': chapter,
            'verse': verse,
            'language': lang_used,
            'span': span,
        }
        _save_bible_progress()

CHUCK_NORRIS_FACTS = safe_load_json("chuck_api_jokes.json", [])
CHUCK_NORRIS_FACTS_ES = safe_load_json("chuck_api_jokes_es.json", [])
BLOND_JOKES = safe_load_json("blond_jokes.json", [])
YO_MOMMA_JOKES = safe_load_json("yo_momma_jokes.json", [])
EL_PASO_FACTS = safe_load_json("el_paso_people_facts.json", [])

ALERT_BELL_RESPONSES = MESHTASTIC_ALERT_FACTS

POSITION_REQUEST_RESPONSES = [
    "position request received, but i'm just a dumb bot..",
    "i logged your position request, but my feet are virtual..",
    "position request noted; this brain runs on silicon, not gps..",
    "got the position ping, but i'm glued to the server rack..",
    "mesh ctrl: position request acknowledged with zero coordinates..",
    "position ping heard; i'm anchored to the console though..",
    "copy that position request—no actual lat/long on this side..",
    "routing the position request, but i'm strictly imaginary on maps..",
    "position beacon requested; i'm still just firmware in the loop..",
    "heard the position call, yet i'm a chat bot without a compass..",
]

COMMAND_REPLY_DELAY = 3

COMMAND_DELAY_OVERRIDES = {
    "admin password": 2.0,
    "/m command": 1.0,
    "/m create": 1.0,
    "/c command": 1.0,
    "/c search": 1.0,
    "/wipe command": 1.0,
    "/wipe confirm": 1.0,
}


def _command_delay(reason: str, delay: Optional[float] = None) -> None:
    """Sleep briefly to stagger command replies and reduce RF collisions."""
    try:
        sleep_for = COMMAND_DELAY_OVERRIDES.get(reason, COMMAND_REPLY_DELAY)
        if delay is not None:
            sleep_for = delay
        sleep_for = max(0.0, float(sleep_for))
    except Exception:
        sleep_for = COMMAND_REPLY_DELAY
    if sleep_for > 0:
        time.sleep(sleep_for)

TRAILING_COMMAND_PUNCT = ",.;:!?)]}"


# -----------------------------
# Anti-spam helpers
# -----------------------------

def _antispam_get_state(sender_key: str) -> Dict[str, Any]:
    state = ANTISPAM_STATE.get(sender_key)
    if state is None:
        state = {
            'history': deque(),
            'timeout_until': None,
            'timeout_level': 0,
            'notified': False,
            'last_short_timeout_end': None,
        }
        ANTISPAM_STATE[sender_key] = state
    return state


def _antispam_refresh_state(state: Dict[str, Any], now: float) -> None:
    timeout_until = state.get('timeout_until')
    if timeout_until and now >= timeout_until:
        level = state.get('timeout_level', 0)
        if level == 1:
            state['last_short_timeout_end'] = timeout_until
        elif level == 2:
            state['last_short_timeout_end'] = None
        state['timeout_until'] = None
        state['timeout_level'] = 0
        state['notified'] = False
        state['history'].clear()


def _antispam_is_blocked(sender_key: Optional[str], *, now: Optional[float] = None) -> Optional[float]:
    if not sender_key:
        return None
    current = now or time.time()
    with ANTISPAM_LOCK:
        state = ANTISPAM_STATE.get(sender_key)
        if not state:
            return None
        _antispam_refresh_state(state, current)
        timeout_until = state.get('timeout_until')
        if timeout_until and current < timeout_until:
            return timeout_until
    return None


def _antispam_register_trigger(sender_key: Optional[str], *, now: Optional[float] = None) -> Optional[Dict[str, Any]]:
    if not sender_key:
        return None
    current = now or time.time()
    with ANTISPAM_LOCK:
        state = _antispam_get_state(sender_key)
        _antispam_refresh_state(state, current)
        timeout_until = state.get('timeout_until')
        if timeout_until and current < timeout_until:
            return None  # Already timed out

        history: deque = state['history']
        window_start = current - ANTISPAM_WINDOW_SECONDS
        while history and history[0] < window_start:
            history.popleft()
        history.append(current)

        if len(history) > ANTISPAM_THRESHOLD:
            last_short_end = state.get('last_short_timeout_end')
            if last_short_end and (current - last_short_end) <= ANTISPAM_ESCALATION_WINDOW:
                duration = ANTISPAM_LONG_TIMEOUT
                level = 2
                state['last_short_timeout_end'] = None
            else:
                duration = ANTISPAM_SHORT_TIMEOUT
                level = 1

            until = current + duration
            state['timeout_until'] = until
            state['timeout_level'] = level
            state['notified'] = False
            history.clear()
            return {
                'level': level,
                'until': until,
                'duration': duration,
            }
    return None


def _antispam_mark_notified(sender_key: Optional[str]) -> None:
    if not sender_key:
        return
    with ANTISPAM_LOCK:
        state = ANTISPAM_STATE.get(sender_key)
        if state:
            state['notified'] = True


def _antispam_notification_needed(sender_key: Optional[str]) -> bool:
    if not sender_key:
        return False
    with ANTISPAM_LOCK:
        state = ANTISPAM_STATE.get(sender_key)
        if not state:
            return False
        return bool(state.get('timeout_until')) and not state.get('notified', False)


def _antispam_format_time(until_ts: float, *, include_date: bool = False) -> str:
    dt = datetime.fromtimestamp(until_ts)
    if include_date:
        return dt.strftime("%b %d %H:%M")
    return dt.strftime("%H:%M")


def _kb_tokenize(text: str) -> List[str]:
    stripped = unidecode(text.lower())
    return re.findall(r"[a-z0-9]+", stripped)


def _load_meshtastic_kb_locked() -> bool:
    global MESHTASTIC_KB_CHUNKS, MESHTASTIC_KB_MTIME
    if not MESHTASTIC_KB_FILE:
        MESHTASTIC_KB_CHUNKS = []
        MESHTASTIC_KB_MTIME = None
        return False
    try:
        mtime = os.path.getmtime(MESHTASTIC_KB_FILE)
    except OSError:
        MESHTASTIC_KB_CHUNKS = []
        MESHTASTIC_KB_MTIME = None
        return False
    if MESHTASTIC_KB_MTIME and MESHTASTIC_KB_MTIME == mtime and MESHTASTIC_KB_CHUNKS:
        return True
    try:
        raw = Path(MESHTASTIC_KB_FILE).read_text(encoding='utf-8')
    except Exception as exc:
        clean_log(f"Unable to read MeshTastic knowledge file: {exc}", "⚠️")
        MESHTASTIC_KB_CHUNKS = []
        MESHTASTIC_KB_MTIME = None
        return False

    sections: List[Tuple[str, str]] = []
    current_title = "Overview"
    current_lines: List[str] = []
    for line in raw.splitlines():
        if line.startswith('## '):
            if current_lines:
                sections.append((current_title, '\n'.join(current_lines).strip()))
            current_title = line[3:].strip() or current_title
            current_lines = []
            continue
        if line.startswith('# ') and not sections and not current_lines:
            # Skip document-level title
            continue
        current_lines.append(line)
    if current_lines:
        sections.append((current_title, '\n'.join(current_lines).strip()))

    chunks: List[Dict[str, Any]] = []
    for title, body in sections:
        paragraphs = [p.strip() for p in body.split('\n\n') if p.strip()]
        buffer = ''
        for paragraph in paragraphs:
            candidate = paragraph if not buffer else f"{buffer}\n\n{paragraph}"
            if len(candidate) <= 900:
                buffer = candidate
                continue
            if buffer:
                chunks.append({'title': title, 'text': buffer})
            if len(paragraph) <= 900:
                buffer = paragraph
            else:
                start = 0
                while start < len(paragraph):
                    slice_text = paragraph[start:start + 900].strip()
                    if slice_text:
                        chunks.append({'title': title, 'text': slice_text})
                    start += 900
                buffer = ''
        if buffer:
            chunks.append({'title': title, 'text': buffer})

    prepared: List[Dict[str, Any]] = []
    for chunk in chunks:
        tokens = _kb_tokenize(chunk['text'])
        if not tokens:
            continue
        prepared.append({
            'title': chunk['title'],
            'text': chunk['text'],
            'text_lower': chunk['text'].lower(),
            'term_counts': Counter(tokens),
            'title_tokens': set(_kb_tokenize(chunk['title'])),
            'length': len(chunk['text']),
        })

    MESHTASTIC_KB_CHUNKS = prepared
    MESHTASTIC_KB_MTIME = mtime
    clean_log(f"Loaded MeshTastic knowledge base ({len(prepared)} chunks)", "📚")
    return bool(prepared)


def _ensure_meshtastic_kb_loaded() -> bool:
    with MESHTASTIC_KB_LOCK:
        return _load_meshtastic_kb_locked()


def _search_meshtastic_kb(query: str, max_chunks: int = 5) -> List[Dict[str, Any]]:
    if not query:
        return []
    if not _ensure_meshtastic_kb_loaded():
        return []
    query_tokens = _kb_tokenize(query)
    if not query_tokens:
        return []
    query_lower = unidecode(query.lower())
    with MESHTASTIC_KB_LOCK:
        chunks = list(MESHTASTIC_KB_CHUNKS)
    if not chunks:
        return []

    scored: List[Tuple[float, Dict[str, Any]]] = []
    for chunk in chunks:
        term_counts = chunk['term_counts']
        score = 0.0
        unique_hits = 0
        for token in query_tokens:
            freq = term_counts.get(token, 0)
            if freq:
                score += freq * 2.0
                unique_hits += 1
                if token in chunk['title_tokens']:
                    score += 1.5
        if unique_hits:
            score += unique_hits * 0.75
        if query_lower in chunk['text_lower']:
            score += 3.0
        if score <= 0:
            continue
        score = score / (1.0 + math.log1p(chunk['length']))
        scored.append((score, chunk))

    if not scored:
        return []

    scored.sort(key=lambda item: item[0], reverse=True)
    selected: List[Dict[str, Any]] = []
    total_chars = 0
    for score, chunk in scored:
        if len(selected) >= max_chunks:
            break
        chunk_len = chunk['length']
        if selected and total_chars + chunk_len > MESHTASTIC_KB_MAX_CONTEXT:
            continue
        selected.append({**chunk, 'score': score})
        total_chars += chunk_len

    if not selected:
        score, chunk = scored[0]
        selected.append({**chunk, 'score': score})

    return selected


def _format_meshtastic_context(chunks: List[Dict[str, Any]]) -> str:
    if not chunks:
        return ""
    lines: List[str] = []
    for idx, chunk in enumerate(chunks, 1):
        lines.append(f"[{idx}] {chunk['title']}\n{chunk['text'].strip()}")
    return "\n\n".join(lines)

COMMAND_ALIASES: Dict[str, Dict[str, Any]] = {
    "/momma": {"canonical": "/yomomma", "languages": ["en"]},
    "/mommajoke": {"canonical": "/yomomma", "languages": ["en"]},
    "/yomommajoke": {"canonical": "/yomomma", "languages": ["en"]},
    "/elp": {"canonical": "/elpaso", "languages": ["en"]},
    "/elpasofact": {"canonical": "/elpaso", "languages": ["en"]},
    "/elpasofacts": {"canonical": "/elpaso", "languages": ["en"]},
    "/setmotd": {"canonical": "/changemotd", "languages": ["en"]},
    "/motdset": {"canonical": "/changemotd", "languages": ["en"]},
    "/setprompt": {"canonical": "/changeprompt", "languages": ["en"]},
    "/fixprompt": {"canonical": "/changeprompt", "languages": ["en"]},
    "/changetone": {"canonical": "/changeprompt", "languages": ["en"]},
    "/promptshow": {"canonical": "/showprompt", "languages": ["en"]},
    "/showmotd": {"canonical": "/motd", "languages": ["en"]},
    "/seeprompt": {"canonical": "/showprompt", "languages": ["en"]},
    "/viewprompt": {"canonical": "/showprompt", "languages": ["en"]},
    "/bulletin": {"canonical": "/motd", "languages": ["en"]},
    "/messageoftheday": {"canonical": "/motd", "languages": ["en"]},
    "/dailymessage": {"canonical": "/motd", "languages": ["en"]},
    "/message": {"canonical": "/motd", "languages": ["en"]},
    "/notes": {"canonical": "/motd", "languages": ["en"]},
    "/resetchat": {"canonical": "/reset", "languages": ["en"]},
    "/forecast": {"canonical": "/weather", "languages": ["en"]},
    "/wx": {"canonical": "/weather", "languages": ["en"]},
    "/elpweather": {"canonical": "/weather", "languages": ["en"]},
    "/meshinfo": {"canonical": "/meshinfo", "languages": ["en"]},
    "/networkinfo": {"canonical": "/meshinfo", "languages": ["en"]},
    "/meshstatus": {"canonical": "/meshinfo", "languages": ["en"]},
    "/jokes": {"canonical": "/jokes", "languages": ["en"]},
    "/joke": {"canonical": "/jokes", "languages": ["en"]},
    "/funnies": {"canonical": "/jokes", "languages": ["en"]},
    "/survival": {"canonical": "/survival", "languages": ["en"]},
    "/survivaltips": {"canonical": "/survival", "languages": ["en"]},
    "/desert": {"canonical": "/survival_desert", "languages": ["en"]},
    "/urban": {"canonical": "/survival_urban", "languages": ["en"]},
    "/city": {"canonical": "/survival_urban", "languages": ["en"]},
    "/jungle": {"canonical": "/survival_jungle", "languages": ["en"]},
    "/woodland": {"canonical": "/survival_woodland", "languages": ["en"]},
    "/forest": {"canonical": "/survival_woodland", "languages": ["en"]},
    "/winter": {"canonical": "/survival_winter", "languages": ["en"]},
    "/cold": {"canonical": "/survival_winter", "languages": ["en"]},
    "/medical": {"canonical": "/survival_medical", "languages": ["en"]},
    "/firstaid": {"canonical": "/survival_medical", "languages": ["en"]},
    "/quiz": {"canonical": "/trivia", "languages": ["en"]},
    "/triviagame": {"canonical": "/trivia", "languages": ["en"]},
    "/generaltrivia": {"canonical": "/trivia", "languages": ["en"]},
    "/biblequiz": {"canonical": "/bibletrivia", "languages": ["en"]},
    "/scripturetrivia": {"canonical": "/bibletrivia", "languages": ["en"]},
    "/disasterquiz": {"canonical": "/disastertrivia", "languages": ["en"]},
    "/prepquiz": {"canonical": "/disastertrivia", "languages": ["en"]},
    "/morsetrainer": {"canonical": "/morsecodetrainer", "languages": ["en"]},
    "/morsecourse": {"canonical": "/morsecodetrainer", "languages": ["en"]},
    "/hurricaneprep": {"canonical": "/hurricanetrainer", "languages": ["en"]},
    "/tornadoprep": {"canonical": "/tornadotrainer", "languages": ["en"]},
    "/radiotrainer": {"canonical": "/radioprocedurestrainer", "languages": ["en"]},
    "/navtrainer": {"canonical": "/navigationtrainer", "languages": ["en"]},
    "/boattrainer": {"canonical": "/boatingtrainer", "languages": ["en"]},
    "/boatprep": {"canonical": "/boatingtrainer", "languages": ["en"]},
    "/emergencywellness": {"canonical": "/wellnesstrainer", "languages": ["en"]},

    # Spanish
    "/ayuda": {"canonical": "/help", "languages": ["es"]},
    "/ayudame": {"canonical": "/help", "languages": ["es"]},
    "/clima": {"canonical": "/weather", "languages": ["es"]},
    "/tiempo": {"canonical": "/weather", "languages": ["es"]},
    "/pronostico": {"canonical": "/weather", "languages": ["es"]},
    "/mensaje": {"canonical": "/motd", "languages": ["es"]},
    "/mensajedia": {"canonical": "/motd", "languages": ["es"]},
    "/biblia": {"canonical": "/bible", "languages": ["es", "pl", "sw"]},
    "/versiculo": {"canonical": "/bible", "languages": ["es"]},
    "/versiculobiblico": {"canonical": "/bible", "languages": ["es"]},
    "/ayudabiblia": {"canonical": "/biblehelp", "languages": ["es"]},
    "/bibliaayuda": {"canonical": "/biblehelp", "languages": ["es"]},
    "/bibliahelp": {"canonical": "/biblehelp", "languages": ["es"]},
    "/datoelpaso": {"canonical": "/elpaso", "languages": ["es"]},
    "/hechoelpaso": {"canonical": "/elpaso", "languages": ["es"]},
    "/emergencia": {"canonical": "/emergency", "languages": ["es"]},
    "/cambiarmensaje": {"canonical": "/changemotd", "languages": ["es"]},
    "/cambiaprompt": {"canonical": "/changeprompt", "languages": ["es"]},
    "/verprompt": {"canonical": "/showprompt", "languages": ["es"]},
    "/aventura": {"canonical": "/adventure", "languages": ["es"]},
    "/reiniciar": {"canonical": "/reset", "languages": ["es"]},
    "/enviarsms": {"canonical": "/sms", "languages": ["es"]},
    "/informemalla": {"canonical": "/meshinfo", "languages": ["es"]},
    "/estadomalla": {"canonical": "/meshinfo", "languages": ["es"]},
    "/estadomesh": {"canonical": "/meshinfo", "languages": ["es"]},
    "/bromas": {"canonical": "/jokes", "languages": ["es"]},
    "/chistes": {"canonical": "/jokes", "languages": ["es"]},
    "/supervivencia": {"canonical": "/survival", "languages": ["es"]},
    "/sobrevivir": {"canonical": "/survival", "languages": ["es"]},
    "/desierto": {"canonical": "/survival_desert", "languages": ["es"]},
    "/urbano": {"canonical": "/survival_urban", "languages": ["es"]},
    "/ciudad": {"canonical": "/survival_urban", "languages": ["es"]},
    "/selva": {"canonical": "/survival_jungle", "languages": ["es"]},
    "/jungla": {"canonical": "/survival_jungle", "languages": ["es"]},
    "/bosque": {"canonical": "/survival_woodland", "languages": ["es"]},
    "/invierno": {"canonical": "/survival_winter", "languages": ["es"]},
    "/frio": {"canonical": "/survival_winter", "languages": ["es"]},
    "/medico": {"canonical": "/survival_medical", "languages": ["es"]},
    "/primerosauxilios": {"canonical": "/survival_medical", "languages": ["es"]},
    "/triviabiblica": {"canonical": "/bibletrivia", "languages": ["es"]},
    "/triviadesastres": {"canonical": "/disastertrivia", "languages": ["es"]},
    "/triviageneral": {"canonical": "/trivia", "languages": ["es"]},
    "/acertijos": {"canonical": "/trivia", "languages": ["es"]},
    "/codigomorse": {"canonical": "/morsecodetrainer", "languages": ["es"]},
    "/entrenadormorse": {"canonical": "/morsecodetrainer", "languages": ["es"]},
    "/huracan": {"canonical": "/hurricanetrainer", "languages": ["es"]},
    "/huracanes": {"canonical": "/hurricanetrainer", "languages": ["es"]},
    "/entrenadorhuracan": {"canonical": "/hurricanetrainer", "languages": ["es"]},
    "/tornado": {"canonical": "/tornadotrainer", "languages": ["es"]},
    "/entrenadortornado": {"canonical": "/tornadotrainer", "languages": ["es"]},
    "/radiocomunicacion": {"canonical": "/radioprocedurestrainer", "languages": ["es"]},
    "/procedimientosradio": {"canonical": "/radioprocedurestrainer", "languages": ["es"]},
    "/navegacion": {"canonical": "/navigationtrainer", "languages": ["es"]},
    "/sinbrujula": {"canonical": "/navigationtrainer", "languages": ["es"]},
    "/barco": {"canonical": "/boatingtrainer", "languages": ["es"]},
    "/seguridadbarco": {"canonical": "/boatingtrainer", "languages": ["es"]},
    "/bienestar": {"canonical": "/wellnesstrainer", "languages": ["es"]},
    "/mascotas": {"canonical": "/wellnesstrainer", "languages": ["es"]},
    "/bienestaremergencia": {"canonical": "/wellnesstrainer", "languages": ["es"]},

    # French
    "/aide": {"canonical": "/help", "languages": ["fr"]},
    "/meteo": {"canonical": "/weather", "languages": ["fr"]},
    "/temps": {"canonical": "/weather", "languages": ["fr"]},
    "/messagedujour": {"canonical": "/motd", "languages": ["fr"]},
    "/verset": {"canonical": "/bible", "languages": ["fr"]},
    "/blaguechuck": {"canonical": "/chucknorris", "languages": ["fr"]},
    "/faitelpaso": {"canonical": "/elpaso", "languages": ["fr"]},
    "/urgence": {"canonical": "/emergency", "languages": ["fr"]},
    "/modifiermotd": {"canonical": "/changemotd", "languages": ["fr"]},
    "/modifierprompt": {"canonical": "/changeprompt", "languages": ["fr"]},
    "/afficherprompt": {"canonical": "/showprompt", "languages": ["fr"]},
    "/reinitialiser": {"canonical": "/reset", "languages": ["fr"]},
    "/envoyersms": {"canonical": "/sms", "languages": ["fr"]},

    # German
    "/hilfe": {"canonical": "/help", "languages": ["de"]},
    "/wetter": {"canonical": "/weather", "languages": ["de"]},
    "/wetterbericht": {"canonical": "/weather", "languages": ["de"]},
    "/tagesnachricht": {"canonical": "/motd", "languages": ["de"]},
    "/bibel": {"canonical": "/bible", "languages": ["de"]},
    "/bibelvers": {"canonical": "/bible", "languages": ["de"]},
    "/chuckwitz": {"canonical": "/chucknorris", "languages": ["de"]},
    "/elpasofakt": {"canonical": "/elpaso", "languages": ["de"]},
    "/notfall": {"canonical": "/emergency", "languages": ["de"]},
    "/motdaendern": {"canonical": "/changemotd", "languages": ["de"]},
    "/promptaendern": {"canonical": "/changeprompt", "languages": ["de"]},
    "/promptanzeigen": {"canonical": "/showprompt", "languages": ["de"]},
    "/zuruecksetzen": {"canonical": "/reset", "languages": ["de"]},
    "/smssenden": {"canonical": "/sms", "languages": ["de"]},

    # Chinese (pinyin)
    "/bangzhu": {"canonical": "/help", "languages": ["zh"]},
    "/tianqi": {"canonical": "/weather", "languages": ["zh"]},
    "/shengjing": {"canonical": "/bible", "languages": ["zh"]},
    "/elpasoshishi": {"canonical": "/elpaso", "languages": ["zh"]},
    "/jinji": {"canonical": "/emergency", "languages": ["zh"]},
    "/xiugaixiaoxi": {"canonical": "/changemotd", "languages": ["zh"]},
    "/xiugaiprompt": {"canonical": "/changeprompt", "languages": ["zh"]},
    "/chakantishi": {"canonical": "/showprompt", "languages": ["zh"]},
    "/chongzhi": {"canonical": "/reset", "languages": ["zh"]},
    "/fasongduanxin": {"canonical": "/sms", "languages": ["zh"]},

    # Polish
    "/pomoc": {"canonical": "/help", "languages": ["pl"]},
    "/pogoda": {"canonical": "/weather", "languages": ["pl", "uk"]},
    "/prognoza": {"canonical": "/weather", "languages": ["pl", "hr"]},
    "/wiadomosc": {"canonical": "/motd", "languages": ["pl"]},
    "/wiadomoscdnia": {"canonical": "/motd", "languages": ["pl"]},
    "/werset": {"canonical": "/bible", "languages": ["pl"]},
    "/faktelpaso": {"canonical": "/elpaso", "languages": ["pl", "uk"]},
    "/naglyprzypadek": {"canonical": "/emergency", "languages": ["pl"]},
    "/zmienwiadomosc": {"canonical": "/changemotd", "languages": ["pl"]},
    "/zmienprompt": {"canonical": "/changeprompt", "languages": ["pl"]},
    "/naprawprompt": {"canonical": "/changeprompt", "languages": ["pl"]},
    "/pokazprompt": {"canonical": "/showprompt", "languages": ["pl"]},
    "/resetuj": {"canonical": "/reset", "languages": ["pl"]},
    "/wyslijsms": {"canonical": "/sms", "languages": ["pl"]},

    # Croatian (Latin, with diacritics where relevant)
    "/pomoć": {"canonical": "/help", "languages": ["hr"]},
    "/vrijeme": {"canonical": "/weather", "languages": ["hr"]},
    "/poruka": {"canonical": "/motd", "languages": ["hr"]},
    "/porukadana": {"canonical": "/motd", "languages": ["hr"]},
    "/biblija": {"canonical": "/bible", "languages": ["hr"]},
    "/stih": {"canonical": "/bible", "languages": ["hr"]},
    "/cinjenicaelpaso": {"canonical": "/elpaso", "languages": ["hr"]},
    "/hitno": {"canonical": "/emergency", "languages": ["hr"]},
    "/promijeniporuku": {"canonical": "/changemotd", "languages": ["hr"]},
    "/promijeniprompt": {"canonical": "/changeprompt", "languages": ["hr"]},
    "/popraviprompt": {"canonical": "/changeprompt", "languages": ["hr"]},
    "/prikaziprompt": {"canonical": "/showprompt", "languages": ["hr"]},
    "/resetiraj": {"canonical": "/reset", "languages": ["hr"]},
    "/poslijsms": {"canonical": "/sms", "languages": ["hr"]},

    # Ukrainian (transliterated)
    "/dopomoga": {"canonical": "/help", "languages": ["uk"]},
    "/prognoz": {"canonical": "/weather", "languages": ["uk"]},
    "/povidomlennia": {"canonical": "/motd", "languages": ["uk"]},
    "/povidomlennia_dnya": {"canonical": "/motd", "languages": ["uk"]},
    "/bibliya": {"canonical": "/bible", "languages": ["uk"]},
    "/virsh": {"canonical": "/bible", "languages": ["uk"]},
    "/nadzvychayno": {"canonical": "/emergency", "languages": ["uk"]},
    "/zminypovidomlennia": {"canonical": "/changemotd", "languages": ["uk"]},
    "/zminyprompt": {"canonical": "/changeprompt", "languages": ["uk"]},
    "/vyprompt": {"canonical": "/changeprompt", "languages": ["uk"]},
    "/pokazhyprompt": {"canonical": "/showprompt", "languages": ["uk"]},
    "/skynuty": {"canonical": "/reset", "languages": ["uk"]},
    "/vidpravysms": {"canonical": "/sms", "languages": ["uk"]},

    # Kiswahili
    "/msaada": {"canonical": "/help", "languages": ["sw"]},
    "/haliyahewa": {"canonical": "/weather", "languages": ["sw"]},
    "/utabiri": {"canonical": "/weather", "languages": ["sw"]},
    "/ujumbe": {"canonical": "/motd", "languages": ["sw"]},
    "/ujumbe_wa_siku": {"canonical": "/motd", "languages": ["sw"]},
    "/mstari": {"canonical": "/bible", "languages": ["sw"]},
    "/fakielpaso": {"canonical": "/elpaso", "languages": ["sw"]},
    "/dharaura": {"canonical": "/emergency", "languages": ["sw"]},
    "/badilisha_ujumbe": {"canonical": "/changemotd", "languages": ["sw"]},
    "/badilisha_prompt": {"canonical": "/changeprompt", "languages": ["sw"]},
    "/rekebisha_prompt": {"canonical": "/changeprompt", "languages": ["sw"]},
    "/onyesha_prompt": {"canonical": "/showprompt", "languages": ["sw"]},
    "/wekaupya": {"canonical": "/reset", "languages": ["sw"]},
    "/tumasms": {"canonical": "/sms", "languages": ["sw"]},
}

BUILTIN_COMMANDS = {
    "/about",
    "/ai",
    "/bot",
    "/query",
    "/data",
    "/whereami",
    "/emergency",
    "/911",
    "/test",
    "/help",
    "/menu",
    "/m",
    "/c",
    "/meshtastic",
    "/wipe",
    "/aipersonality",
    "/biblehelp",
    "/jokes",
    "/games",
    "/hangman",
    "/wordle",
    "/wordladder",
    "/choose",
    "/rps",
    "/coinflip",
    "/cipher",
    "/bingo",
    "/quizbattle",
    "/morse",
    "/aivibe",
    "/choosevibe",
    "/aisettings",
    "/emailhelp",
    "/bibletrivia",
    "/disastertrivia",
    "/trivia",
    "/survival",
    "/survival_desert",
    "/survival_urban",
    "/survival_jungle",
    "/survival_woodland",
    "/survival_winter",
    "/survival_medical",
    "/weather",
    "/motd",
    "/meshinfo",
    "/bible",
    "/chucknorris",
    "/elpaso",
    "/blond",
    "/yomomma",
    "/morsecodetrainer",
    "/hurricanetrainer",
    "/tornadotrainer",
    "/radioprocedurestrainer",
    "/navigationtrainer",
    "/boatingtrainer",
    "/wellnesstrainer",
    "/changemotd",
    "/changeprompt",
    "/showprompt",
    "/printprompt",
    "/reset",
    "/sms",
}

FUZZY_COMMAND_MATCH_THRESHOLD = 0.6


def _normalize_language_code(value: Optional[str]) -> str:
    if not value:
        return "en"
    val = str(value).strip().lower()
    if val.startswith("es") or "spanish" in val:
        return "es"
    return "en"


LANGUAGE_SELECTION_CONFIG = config.get("language_selection", "english")
LANGUAGE_FALLBACK = _normalize_language_code(LANGUAGE_SELECTION_CONFIG)


def _preferred_menu_language(language: Optional[str]) -> str:
    if language:
        return _normalize_language_code(language)
    return LANGUAGE_FALLBACK


MENU_DEFINITIONS = {
    "menu": {
        "title": {"en": "Main Menu", "es": "Menú principal"},
        "sections": [
            {
                "items": [
                    {"text": {"en": "Mail: /mail • /checkmail • /wipe", "es": "Correo: /mail • /checkmail • /wipe"}},
                    {"text": {"en": "Games: /games • /adventure", "es": "Juegos: /games • /adventure"}},
                    {"text": {"en": "Quick info: /help • /biblehelp • /meshtastic • /weather", "es": "Info rápida: /help • /biblehelp • /meshtastic • /weather"}},
                    {"text": {"en": "AI vibe: /aivibe • change with /changevibe", "es": "Tono IA: /aivibe • cambia con /changevibe"}},
                    {"text": {"en": "Ops: /motd • /meshinfo", "es": "Operaciones: /motd • /meshinfo"}},
                    {"text": {"en": "Safety: /survival • /emergency", "es": "Seguridad: /survival • /emergency"}},
                    {"text": {"en": "Need a box? 📦 /emailhelp", "es": "¿Necesitas buzón? 📦 /emailhelp"}},
                ]
            }
        ],
    },
    "jokes": {
        "title": {"en": "Humor", "es": "Humor"},
        "sections": [
            {
                "items": [
                    {"text": {"en": "/chucknorris • /blond • /yomomma", "es": "/chucknorris • /blond • /yomomma"}},
                    {"text": {"en": "More laughs: /funfact <topic>", "es": "Más risas: /funfact <tema>"}},
                ]
            }
        ],
    },
    "aipersonality": {
        "title": {"en": "AI personalities", "es": "Personalidades IA"},
        "sections": [
            {
                "items": [
                    {"text": {"en": "List styles: /aipersonality list", "es": "Ver estilos: /aipersonality list"}},
                    {"text": {"en": "Browse vibe menu: /choosevibe", "es": "Explorar tonos: /choosevibe"}},
                    {"text": {"en": "Switch tone: /aipersonality set <name>", "es": "Cambiar tono: /aipersonality set <name>"}},
                    {"text": {"en": "Add guidance: /aipersonality prompt <text>", "es": "Añadir guía: /aipersonality prompt <text>"}},
                    {"text": {"en": "Reset: /aipersonality reset", "es": "Restablecer: /aipersonality reset"}},
                ]
            }
        ],
    },
    "survival": {
        "title": {"en": "Survival", "es": "Supervivencia"},
        "sections": [
            {
                "items": [
                    {"text": {"en": "Scenarios: /survival_desert • /survival_urban • /survival_jungle", "es": "Escenarios: /survival_desert • /survival_urban • /survival_jungle"}},
                    {"text": {"en": "More guides: /survival_woodland • /survival_winter • /survival_medical", "es": "Más guías: /survival_woodland • /survival_winter • /survival_medical"}},
                ]
            }
        ],
    },
    "wipe": {
        "title": {"en": "Wipe tools", "es": "Herramientas de limpieza"},
        "sections": [
            {
                "items": [
                    {"text": {"en": "/wipe mailbox <name> — empty that inbox", "es": "/wipe mailbox <name> — vacía ese buzón"}},
                    {"text": {"en": "/wipe chathistory — clear our DM log", "es": "/wipe chathistory — limpia el chat DM"}},
                    {"text": {"en": "/wipe personality — reset tone & prompt", "es": "/wipe personality — reinicia tono y prompt"}},
                    {"text": {"en": "/wipe all <name> — mailbox + chat + tone", "es": "/wipe all <name> — buzón + chat + tono"}},
                ]
            }
        ],
    },
}


SURVIVAL_GUIDES = {
    "/survival_desert": {
        "title": {
            "en": "Desert survival snapshot",
            "es": "Guía rápida de supervivencia en el desierto",
        },
        "points": [
            {"en": "Sip water every 15-20 minutes; shade your containers to slow evaporation.", "es": "Bebe sorbos de agua cada 15-20 minutos; mantén los recipientes a la sombra para reducir la evaporación."},
            {"en": "Travel at dawn or dusk, rest under improvised shade during peak sun.", "es": "Viaja al amanecer o atardecer y descansa bajo sombra improvisada durante el sol intenso."},
            {"en": "Layer clothing: loose, light fabrics trap cooler air and prevent sunburn.", "es": "Usa ropa holgada y ligera; las capas atrapan aire fresco y evitan quemaduras."},
            {"en": "Signal rescuers with mirrors, bright cloth, or large ground symbols visible from above.", "es": "Señala a rescatistas con espejos, tela brillante o símbolos grandes en el suelo visibles desde el aire."},
            {"en": "Ration sweat, not thirst—slow your pace, use ground cover, and avoid metal equipment in direct sun.", "es": "Raciona el esfuerzo, no la sed; camina despacio, usa coberturas y evita herramientas metálicas al sol."},
        ],
        "reflection": {
            "en": "Stay calm: like water shared freely, grace grows when we lift one another.",
            "es": "Mantén la calma: así como el agua compartida, la gracia crece cuando levantamos a otros.",
        },
    },
    "/survival_urban": {
        "title": {
            "en": "Urban survival snapshot",
            "es": "Guía rápida de supervivencia urbana",
        },
        "points": [
            {"en": "Map safe zones: hospitals, churches, and community centers often host aid.", "es": "Identifica zonas seguras: hospitales, iglesias y centros comunitarios suelen brindar ayuda."},
            {"en": "Keep a low profile—blend in, avoid predictable routines, and move with purpose.", "es": "Mantén un perfil bajo; evita rutinas predecibles y muévete con propósito."},
            {"en": "Secure shelter above ground level to limit flooding and control entry points.", "es": "Busca refugio por encima del nivel del suelo para evitar inundaciones y controlar accesos."},
            {"en": "Harvest resources: rainwater from gutters, tools from maintenance closets, info from local radio.", "es": "Aprovecha recursos: agua de lluvia de canaletas, herramientas de mantenimiento e información de radio local."},
            {"en": "Organize neighbors for watch rotations—community care deters conflict.", "es": "Organiza turnos vecinales de vigilancia; el cuidado comunitario disuade conflictos."},
        ],
        "reflection": {
            "en": "Seek peace in every doorway; a gentle word can steady a whole block.",
            "es": "Busca la paz en cada puerta; una palabra amable puede sostener a toda la cuadra.",
        },
    },
    "/survival_jungle": {
        "title": {
            "en": "Jungle survival snapshot",
            "es": "Guía rápida de supervivencia en la selva",
        },
        "points": [
            {"en": "Stay dry: elevated shelters and hammocks keep you above insects and runoff.", "es": "Mantente seco: refugios elevados y hamacas te aíslan de insectos y escorrentías."},
            {"en": "Collect rainwater with tarps or broad leaves and filter before drinking.", "es": "Recolecta lluvia con lonas o hojas grandes y filtra antes de beber."},
            {"en": "Track daylight with a machete notch on trees—helps prevent circling back.", "es": "Marca los árboles con machete para seguir el progreso y evitar caminar en círculos."},
            {"en": "Avoid bright fruit or insects with bold patterns—they often signal toxins.", "es": "Evita frutos brillantes o insectos con patrones llamativos; suelen ser tóxicos."},
            {"en": "Smoke damp leaves to repel mosquitoes and signal companions.", "es": "Quema hojas húmedas para ahuyentar mosquitos y señalar a los compañeros."},
        ],
        "reflection": {
            "en": "Even in thick canopy, light breaks through—hold to hope and guide others gently.",
            "es": "Aun bajo el dosel denso, la luz se abre paso; mantén la esperanza y guía con mansedumbre.",
        },
    },
    "/survival_woodland": {
        "title": {
            "en": "Woodland survival snapshot",
            "es": "Guía rápida de supervivencia en bosques",
        },
        "points": [
            {"en": "Layer clothing and keep waterproof shells accessible as weather swings quickly.", "es": "Usa capas de ropa y ten a mano prendas impermeables; el clima cambia rápido."},
            {"en": "Use tree moss growth and prevailing wind patterns to stay oriented.", "es": "Usa el musgo en los árboles y la dirección del viento para orientarte."},
            {"en": "Forage responsibly: pine needles for vitamin C tea, cattails for starch.", "es": "Forrajea con responsabilidad: agujas de pino para té con vitamina C, tule para almidón."},
            {"en": "Build reflector fires against logs or rocks to bounce heat into shelter.", "es": "Construye fogatas con reflectores usando troncos o rocas para reflejar calor al refugio."},
            {"en": "Mark trails with biodegradable ribbon or carved arrows to aid rescue teams.", "es": "Marca el camino con cintas biodegradables o flechas talladas para ayudar a rescatistas."},
        ],
        "reflection": {
            "en": "Walk softly; stewardship of creation mirrors the Shepherd who restores souls.",
            "es": "Camina con suavidad; cuidar la creación refleja al Pastor que restaura almas.",
        },
    },
    "/survival_winter": {
        "title": {
            "en": "Winter survival snapshot",
            "es": "Guía rápida de supervivencia invernal",
        },
        "points": [
            {"en": "Stack layers: wicking base, insulating core, windproof shell.", "es": "Usa capas: base que absorba humedad, capa aislante y exterior a prueba de viento."},
            {"en": "Vent shelters to prevent carbon monoxide when using stoves or fires.", "es": "Ventila los refugios para evitar monóxido de carbono al usar estufas o fogatas."},
            {"en": "Keep water in insulated containers upside-down so the surface ice forms near the lid.", "es": "Guarda el agua en recipientes aislados boca abajo para que el hielo se forme cerca de la tapa."},
            {"en": "Travel with snowshoes or improvised platforms to avoid postholing and conserve energy.", "es": "Camina con raquetas o plataformas improvisadas para evitar hundirte y ahorrar energía."},
            {"en": "Warm companions by sharing shelter, hot drinks, and songs that lift morale.", "es": "Calienta a tus compañeros compartiendo refugio, bebidas calientes y cantos que animen."},
        ],
        "reflection": {
            "en": "Hope is a shared fire—tend it together until the thaw arrives.",
            "es": "La esperanza es un fuego compartido; cuídenlo juntos hasta que llegue el deshielo.",
        },
    },
    "/survival_medical": {
        "title": {
            "en": "Field medical snapshot",
            "es": "Guía rápida de primeros auxilios",
        },
        "points": [
            {"en": "Check ABCs: airway clear, breathing steady, circulation supported with direct pressure.", "es": "Revisa ABC: vía aérea despejada, respiración estable, circulación apoyada con presión directa."},
            {"en": "Stop severe bleeding with pressure dressings or improvised tourniquets two inches above the wound.", "es": "Detén hemorragias con vendajes a presión o torniquetes improvisados a 5 cm por encima de la herida."},
            {"en": "Stabilize fractures using splints padded with cloth; immobilize joints above and below.", "es": "Estabiliza fracturas con férulas acolchadas; inmoviliza las articulaciones arriba y abajo."},
            {"en": "Track vitals every 10 minutes—note pulse, breathing rate, and responsiveness.", "es": "Registra signos vitales cada 10 minutos: pulso, respiración y nivel de respuesta."},
            {"en": "Document allergies, meds, and events; hand the notes to first responders.", "es": "Anota alergias, medicamentos y eventos; entrega las notas a los rescatistas."},
        ],
        "reflection": {
            "en": "Serve with compassion—healing hands point to the Great Physician.",
            "es": "Sirve con compasión; las manos que sanan señalan al Gran Médico.",
        },
    },
}

SURVIVAL_REFLECTION_LABEL = {"en": "Faith focus", "es": "Enfoque de fe"}


@dataclass
class TriviaSession:
    player_key: str
    category: str
    score: int = 0
    total: int = 0
    asked_ids: Set[str] = field(default_factory=set)
    current_id: Optional[str] = None
    language: str = "en"
    owner_id: Optional[str] = None
    channel_idx: Optional[int] = None
    is_direct: bool = True
    display_name: Optional[str] = None
    hint_used: bool = False


TRIVIA_STATE_FILE = "trivia_state.json"
TRIVIA_SESSIONS: Dict[str, TriviaSession] = {}

TRIVIA_CATEGORY_TITLES = {
    "bible": {"en": "Bible Trivia", "es": "Trivia Bíblica"},
    "disaster": {"en": "Disaster Prep Trivia", "es": "Trivia de preparación"},
    "general": {"en": "General Trivia", "es": "Trivia general"},
}

TRIVIA_CATEGORY_EMOJI = {
    "bible": "📖",
    "disaster": "🛡️",
    "general": "🧠",
}

TRIVIA_STRINGS = {
    "en": {
        "question_intro": "{icon} {title} challenge:",
        "choices_intro": "📝 Choices:",
        "answer_prompt": "✍️ Reply with `{command} <answer>`.",
        "correct": "✅ Correct! 🎉 {explanation}",
        "correct_no_expl": "✅ Correct! 🎉",
        "incorrect": "❌ Not quite. The answer is {answer}. ℹ️ {explanation}",
        "incorrect_no_expl": "❌ Not quite. The answer is {answer}.",
        "score_line": "📊 Score: {score}/{total} correct ({percent}%).",
        "new_question": "✨ Next question:",
        "skipped": "⏭️ Skipped! Here's a fresh question:",
        "no_question": "🪧 Request a new question first with `{command}`.",
        "no_questions": "😅 No questions available in this category right now.",
        "no_scores": "📭 No scores yet for this category.",
        "leaderboard_title": "🏆 Leaderboard — {title}",
        "leaderboard_entry": "{rank}. {name}: {score}/{total} ({percent}%)",
        "your_score": "🎯 Your score: {score}/{total} ({percent}%).",
        "hint": "🕵️ Hint: the answer starts with **{letter}**.",
        "hint_used": "🕵️ You already used the hint for this one.",
    },
    "es": {
        "question_intro": "{icon} Pregunta de {title}:",
        "choices_intro": "📝 Opciones:",
        "answer_prompt": "✍️ Responde con `{command} <respuesta>`.",
        "correct": "✅ ¡Correcto! 🎉 {explanation}",
        "correct_no_expl": "✅ ¡Correcto! 🎉",
        "incorrect": "❌ Casi. La respuesta es {answer}. ℹ️ {explanation}",
        "incorrect_no_expl": "❌ Casi. La respuesta es {answer}.",
        "score_line": "📊 Puntaje: {score}/{total} aciertos ({percent}%).",
        "new_question": "✨ Siguiente pregunta:",
        "skipped": "⏭️ ¡Pregunta saltada! Aquí tienes una nueva:",
        "no_question": "🪧 Primero pide una pregunta nueva con `{command}`.",
        "no_questions": "😅 No hay preguntas disponibles en esta categoría por ahora.",
        "no_scores": "📭 Aún no hay puntuaciones para esta categoría.",
        "leaderboard_title": "🏆 Tabla de posiciones — {title}",
        "leaderboard_entry": "{rank}. {name}: {score}/{total} ({percent}%)",
        "your_score": "🎯 Tu puntaje: {score}/{total} ({percent}%).",
        "hint": "🕵️ Pista: la respuesta comienza con **{letter}**.",
        "hint_used": "🕵️ Ya usaste la pista para esta pregunta.",
    },
}

def _localized_text(value: Any, language: str) -> str:
    if isinstance(value, dict):
        lang_order: List[str] = []
        normalized = _normalize_language_code(language)
        if normalized:
            lang_order.append(normalized)
        if LANGUAGE_FALLBACK not in lang_order:
            lang_order.append(LANGUAGE_FALLBACK)
        if "en" not in lang_order:
            lang_order.append("en")
        for key in lang_order:
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate:
                return candidate
        for candidate in value.values():
            if isinstance(candidate, str) and candidate:
                return candidate
        return ""
    if value is None:
        return ""
    return str(value)


def _localized_list(value: Any, language: str) -> List[str]:
    if isinstance(value, dict):
        lang_order: List[str] = []
        normalized = _normalize_language_code(language)
        if normalized:
            lang_order.append(normalized)
        if LANGUAGE_FALLBACK not in lang_order:
            lang_order.append(LANGUAGE_FALLBACK)
        if "en" not in lang_order:
            lang_order.append("en")
        for key in lang_order:
            candidate = value.get(key)
            if isinstance(candidate, list) and candidate:
                return [str(item) for item in candidate]
            if isinstance(candidate, str) and candidate:
                return [str(candidate)]
        for candidate in value.values():
            if isinstance(candidate, list) and candidate:
                return [str(item) for item in candidate]
            if isinstance(candidate, str) and candidate:
                return [str(candidate)]
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, str) and value:
        return [value]
    return []



TRIVIA_BANK: Dict[str, List[Dict[str, Any]]] = {
    "bible": [
        {
            "id": "b1",
            "question": {
                "en": "Who interpreted Pharaoh's dreams about seven years of plenty and seven years of famine?",
                "es": "¿Quién interpretó los sueños del faraón sobre siete años de abundancia y siete de hambre?",
            },
            "answers": ["joseph", "jose", "josé"],
            "answer_display": {"en": "Joseph", "es": "José"},
            "choices": {
                "en": ["Joseph", "Moses", "Daniel", "Aaron"],
                "es": ["José", "Moisés", "Daniel", "Aarón"],
            },
            "explanation": {
                "en": "Genesis 41 records Joseph interpreting Pharaoh's dreams and planning to store grain.",
                "es": "Génesis 41 relata cómo José interpretó los sueños del faraón y planificó almacenar grano.",
            },
        },
        {
            "id": "b2",
            "question": {
                "en": "On which road was Saul travelling when he encountered a blinding light from heaven?",
                "es": "¿En qué camino viajaba Saulo cuando encontró una luz cegadora del cielo?",
            },
            "answers": ["damascus", "road to damascus", "camino a damasco"],
            "answer_display": {"en": "Road to Damascus", "es": "Camino a Damasco"},
            "choices": {
                "en": ["Road to Damascus", "Emmaus Road", "Bethany Road", "Jericho Road"],
                "es": ["Camino a Damasco", "Camino a Emaús", "Camino a Betania", "Camino a Jericó"],
            },
            "explanation": {
                "en": "Acts 9 describes Saul meeting Jesus on the road to Damascus.",
                "es": "Hechos 9 describe a Saulo encontrándose con Jesús en el camino a Damasco.",
            },
        },
        {
            "id": "b3",
            "question": {
                "en": "Which prophet confronted King Ahab and the prophets of Baal on Mount Carmel?",
                "es": "¿Qué profeta enfrentó al rey Acab y a los profetas de Baal en el monte Carmelo?",
            },
            "answers": ["elijah", "elias", "elías"],
            "answer_display": {"en": "Elijah", "es": "Elías"},
            "choices": {
                "en": ["Elijah", "Elisha", "Isaiah", "Micah"],
                "es": ["Elías", "Eliseo", "Isaías", "Miqueas"],
            },
            "explanation": {
                "en": "1 Kings 18 recounts Elijah calling down fire on Mount Carmel.",
                "es": "1 Reyes 18 narra cómo Elías invocó fuego en el monte Carmelo.",
            },
        },
        {
            "id": "b4",
            "question": {
                "en": "In the Gospel of John, what was Jesus' first recorded miracle?",
                "es": "Según el evangelio de Juan, ¿cuál fue el primer milagro registrado de Jesús?",
            },
            "answers": [
                "water into wine",
                "turned water into wine",
                "water to wine",
                "wine",
                "agua en vino",
                "convertir el agua en vino",
            ],
            "answer_display": {"en": "Turning water into wine", "es": "Convertir el agua en vino"},
            "choices": {
                "en": [
                    "Turning water into wine",
                    "Feeding the 5,000",
                    "Walking on water",
                    "Healing a blind man",
                ],
                "es": [
                    "Convertir el agua en vino",
                    "Alimentar a los 5,000",
                    "Caminar sobre el agua",
                    "Sanar a un ciego",
                ],
            },
            "explanation": {
                "en": "John 2 narrates Jesus turning water into wine at the wedding in Cana.",
                "es": "Juan 2 narra cómo Jesús convirtió el agua en vino en las bodas de Caná.",
            },
        },
        {
            "id": "b5",
            "question": {
                "en": "Which Old Testament book contains the verse, 'The LORD is my shepherd'?",
                "es": "¿Qué libro del Antiguo Testamento contiene el versículo 'El Señor es mi pastor'?",
            },
            "answers": ["psalms", "psalm", "psalm 23", "salmos", "salmo 23"],
            "answer_display": {"en": "Psalms", "es": "Salmos"},
            "choices": {
                "en": ["Psalms", "Proverbs", "Isaiah", "Deuteronomy"],
                "es": ["Salmos", "Proverbios", "Isaías", "Deuteronomio"],
            },
            "explanation": {
                "en": "Psalm 23 opens with 'The LORD is my shepherd; I shall not want.'",
                "es": "El Salmo 23 comienza con 'El Señor es mi pastor; nada me faltará.'",
            },
        },
        {
            "id": "b6",
            "question": {
                "en": "Who was the only disciple to walk on water toward Jesus before beginning to sink?",
                "es": "¿Qué discípulo caminó sobre el agua hacia Jesús antes de comenzar a hundirse?",
            },
            "answers": ["peter", "simon peter", "pedro"],
            "answer_display": {"en": "Peter", "es": "Pedro"},
            "choices": {
                "en": ["Peter", "John", "Andrew", "Thomas"],
                "es": ["Pedro", "Juan", "Andrés", "Tomás"],
            },
            "explanation": {
                "en": "Matthew 14:28-31 describes Peter stepping out of the boat toward Jesus.",
                "es": "Mateo 14:28-31 describe a Pedro saliendo de la barca hacia Jesús.",
            },
        },
        {
            "id": "b7",
            "question": {
                "en": "What did God provide for the Israelites each morning in the wilderness to eat?",
                "es": "¿Qué proporcionó Dios cada mañana en el desierto para que comieran los israelitas?",
            },
            "answers": ["manna", "mana", "maná"],
            "answer_display": {"en": "Manna", "es": "Maná"},
            "choices": {
                "en": ["Manna", "Quail", "Bread from Egypt", "Figs"],
                "es": ["Maná", "Codornices", "Pan de Egipto", "Higos"],
            },
            "explanation": {
                "en": "Exodus 16 notes that manna appeared with the dew each morning.",
                "es": "Éxodo 16 indica que el maná aparecía con el rocío cada mañana.",
            },
        },
        {
            "id": "b8",
            "question": {
                "en": "Which apostle is known for doubting the resurrection until he saw Jesus' wounds?",
                "es": "¿Qué apóstol dudó de la resurrección hasta ver las heridas de Jesús?",
            },
            "answers": ["thomas", "doubting thomas", "tomas", "tomás"],
            "answer_display": {"en": "Thomas", "es": "Tomás"},
            "choices": {
                "en": ["Thomas", "Philip", "James", "Bartholomew"],
                "es": ["Tomás", "Felipe", "Santiago", "Bartolomé"],
            },
            "explanation": {
                "en": "John 20 describes Thomas insisting on touching Jesus' wounds before believing.",
                "es": "Juan 20 describe a Tomás insistiendo en tocar las heridas de Jesús antes de creer.",
            },
        },
    ],
    "disaster": [
        {
            "id": "d1",
            "question": {
                "en": "How much water should you store per person per day for emergency readiness?",
                "es": "¿Cuánta agua debes almacenar por persona por día para estar preparado ante emergencias?",
            },
            "answers": [
                "1 gallon",
                "one gallon",
                "about 1 gallon",
                "3.8 liters",
                "38 liters",
                "un galon",
                "un galón",
                "38 litros",
            ],
            "answer_display": {"en": "1 gallon (3.8 L)", "es": "1 galón (3.8 L)"},
            "choices": {
                "en": ["1 gallon (3.8 L)", "Half gallon", "2 gallons", "One quart"],
                "es": ["1 galón (3.8 L)", "Medio galón", "2 galones", "Un cuarto"],
            },
            "explanation": {
                "en": "FEMA recommends about one gallon (3.8 liters) of water per person per day.",
                "es": "FEMA recomienda alrededor de un galón (3.8 litros) de agua por persona por día.",
            },
        },
        {
            "id": "d2",
            "question": {
                "en": "During a tornado warning inside a sturdy building, where should you shelter?",
                "es": "Durante una alerta de tornado dentro de un edificio sólido, ¿dónde debes refugiarte?",
            },
            "answers": [
                "interior room",
                "lowest level interior room",
                "basement",
                "safe room",
                "bathroom",
                "closet",
                "cuarto interior",
                "sotano",
                "sótano",
                "cuarto seguro",
                "banera",
                "baño",
                "closet interior",
            ],
            "answer_display": {"en": "Interior room on the lowest floor", "es": "Cuarto interior en el nivel más bajo"},
            "choices": {
                "en": ["Interior room on the lowest floor", "Near exterior windows", "Top floor balcony", "Garage"],
                "es": ["Cuarto interior en el nivel más bajo", "Cerca de ventanas exteriores", "Balcón del último piso", "Garaje"],
            },
            "explanation": {
                "en": "Emergency managers advise sheltering in an interior room on the lowest level, away from windows.",
                "es": "Los servicios de emergencia aconsejan refugiarse en un cuarto interior en el nivel más bajo, lejos de las ventanas.",
            },
        },
        {
            "id": "d3",
            "question": {
                "en": "Which item is best to include in a go-bag for prolonged power outages?",
                "es": "¿Qué artículo es mejor incluir en una mochila de emergencia para apagones prolongados?",
            },
            "answers": [
                "battery radio",
                "hand crank radio",
                "hand-crank radio",
                "radio",
                "radio de manivela",
                "radio a baterias",
                "radio a baterías",
            ],
            "answer_display": {"en": "Hand-crank or battery-powered radio", "es": "Radio de manivela o a baterías"},
            "choices": {
                "en": ["Hand-crank or battery-powered radio", "Electric can opener", "Desktop computer", "Metal detector"],
                "es": ["Radio de manivela o a baterías", "Abrelatas eléctrico", "Computadora de escritorio", "Detector de metales"],
            },
            "explanation": {
                "en": "A hand-crank or battery-powered radio keeps you informed when power and internet fail.",
                "es": "Una radio de manivela o a baterías te mantiene informado cuando falla la energía y el internet.",
            },
        },
        {
            "id": "d4",
            "question": {
                "en": "When a hurricane is approaching, what should you do with important documents?",
                "es": "Cuando se aproxima un huracán, ¿qué debes hacer con los documentos importantes?",
            },
            "answers": [
                "waterproof container",
                "seal them",
                "store in waterproof bag",
                "scan them",
                "contenedor impermeable",
                "bolsa impermeable",
                "escanealos",
                "respaldo digital",
            ],
            "answer_display": {"en": "Seal them in a waterproof container", "es": "Sellarlos en un contenedor impermeable"},
            "choices": {
                "en": ["Seal them in a waterproof container", "Leave them on the desk", "Mail them to friends", "Recycle them"],
                "es": ["Sellarlos en un contenedor impermeable", "Dejarlos sobre el escritorio", "Enviarlos por correo a amigos", "Reciclarlos"],
            },
            "explanation": {
                "en": "Store vital documents in waterproof containers or cloud backups before a storm.",
                "es": "Guarda los documentos vitales en recipientes impermeables o respaldos digitales antes de la tormenta.",
            },
        },
        {
            "id": "d5",
            "question": {
                "en": "What is the recommended action if you smell gas after an earthquake?",
                "es": "¿Qué acción se recomienda si hueles gas después de un terremoto?",
            },
            "answers": [
                "leave immediately",
                "evacuate",
                "get outside",
                "turn off gas and leave",
                "salir de inmediato",
                "evacuar",
                "apagar el gas y salir",
            ],
            "answer_display": {"en": "Leave immediately and notify authorities", "es": "Salir de inmediato y avisar a las autoridades"},
            "choices": {
                "en": ["Leave the building immediately and notify authorities", "Light a candle to see better", "Open all electrical switches", "Stay and investigate"],
                "es": ["Salir de inmediato y avisar a las autoridades", "Encender una vela para ver mejor", "Abrir todos los interruptores", "Quedarse a investigar"],
            },
            "explanation": {
                "en": "Leave immediately to avoid ignition and notify professionals to inspect the leak.",
                "es": "Sal de inmediato para evitar una ignición y avisa a los profesionales para que inspeccionen la fuga.",
            },
        },
        {
            "id": "d6",
            "question": {
                "en": "How often should you test the batteries in smoke alarms?",
                "es": "¿Con qué frecuencia debes probar las baterías de las alarmas de humo?",
            },
            "answers": ["monthly", "once a month", "every month", "mensualmente", "cada mes"],
            "answer_display": {"en": "Monthly", "es": "Mensualmente"},
            "choices": {
                "en": ["Monthly", "Once a year", "Only after a fire", "Never"],
                "es": ["Mensualmente", "Una vez al año", "Solo después de un incendio", "Nunca"],
            },
            "explanation": {
                "en": "Fire safety guidelines advise testing smoke alarms monthly.",
                "es": "Las normas de seguridad contra incendios aconsejan probar las alarmas de humo cada mes.",
            },
        },
        {
            "id": "d7",
            "question": {
                "en": "What is the minimum recommended length of non-perishable food supply for at-home sheltering?",
                "es": "¿Cuál es la reserva mínima recomendada de alimentos no perecederos para refugiarse en casa?",
            },
            "answers": ["3 days", "three days", "72 hours", "tres dias", "tres días", "72 horas"],
            "answer_display": {"en": "3 days", "es": "3 días"},
            "choices": {
                "en": ["3 days", "12 hours", "1 day", "8 days"],
                "es": ["3 días", "12 horas", "1 día", "8 días"],
            },
            "explanation": {
                "en": "Most emergency planners advise at least a three-day (72-hour) supply per person.",
                "es": "La mayoría de los planificadores recomiendan al menos tres días (72 horas) de alimentos por persona.",
            },
        },
        {
            "id": "d8",
            "question": {
                "en": "During a wildfire evacuation notice, what should you avoid doing with the windows?",
                "es": "Si hay una orden de evacuación por incendio forestal, ¿qué debes evitar hacer con las ventanas?",
            },
            "answers": ["leave them open", "open", "opening", "dejarlas abiertas", "abrirlas"],
            "answer_display": {"en": "Keep them closed", "es": "Mantenerlas cerradas"},
            "choices": {
                "en": ["Keep them closed to prevent embers entering", "Prop them open for air", "Remove the screens", "Cover with foil"],
                "es": ["Mantenerlas cerradas para evitar que entren brasas", "Dejarlas abiertas para ventilar", "Quitar las mallas", "Cubrirlas con papel aluminio"],
            },
            "explanation": {
                "en": "Keeping windows closed helps stop embers and smoke from entering the structure.",
                "es": "Mantener las ventanas cerradas evita que entren brasas y humo en la vivienda.",
            },
        },
    ],
    "general": [
        {
            "id": "g1",
            "question": {
                "en": "What is the largest planet in our solar system?",
                "es": "¿Cuál es el planeta más grande de nuestro sistema solar?",
            },
            "answers": ["jupiter", "júpiter"],
            "answer_display": {"en": "Jupiter", "es": "Júpiter"},
            "choices": {
                "en": ["Jupiter", "Saturn", "Neptune", "Earth"],
                "es": ["Júpiter", "Saturno", "Neptuno", "Tierra"],
            },
            "explanation": {
                "en": "Jupiter is the largest planet with a diameter of about 143,000 km.",
                "es": "Júpiter es el planeta más grande con un diámetro de unos 143,000 km.",
            },
        },
        {
            "id": "g2",
            "question": {
                "en": "Riddle: I speak without a mouth and hear without ears. I have nobody, but I come alive with wind. What am I?",
                "es": "Adivinanza: Hablo sin boca y escucho sin oídos. No tengo cuerpo, pero cobro vida con el viento. ¿Qué soy?",
            },
            "answers": ["echo", "eco"],
            "answer_display": {"en": "Echo", "es": "Eco"},
            "choices": {"en": [], "es": []},
            "explanation": {
                "en": "An echo reflects sound even without a body.",
                "es": "Un eco refleja el sonido incluso sin un cuerpo físico.",
            },
        },
        {
            "id": "g3",
            "question": {
                "en": "Which scientist presented the three laws of motion in 'Philosophiæ Naturalis Principia Mathematica'?",
                "es": "¿Qué científico presentó las tres leyes del movimiento en 'Philosophiæ Naturalis Principia Mathematica'?",
            },
            "answers": ["isaac newton", "newton", "sir isaac newton"],
            "answer_display": {"en": "Isaac Newton", "es": "Isaac Newton"},
            "choices": {
                "en": ["Isaac Newton", "Albert Einstein", "Galileo Galilei", "Niels Bohr"],
                "es": ["Isaac Newton", "Albert Einstein", "Galileo Galilei", "Niels Bohr"],
            },
            "explanation": {
                "en": "Isaac Newton published the Principia in 1687 outlining the laws of motion.",
                "es": "Isaac Newton publicó los Principia en 1687, delineando las leyes del movimiento.",
            },
        },
        {
            "id": "g4",
            "question": {
                "en": "In what year did humans first walk on the Moon?",
                "es": "¿En qué año caminaron por primera vez los humanos en la Luna?",
            },
            "answers": ["1969", "nineteen sixty nine", "mil novecientos sesenta y nueve"],
            "answer_display": {"en": "1969", "es": "1969"},
            "choices": {
                "en": ["1969", "1959", "1972", "1981"],
                "es": ["1969", "1959", "1972", "1981"],
            },
            "explanation": {
                "en": "Apollo 11 landed on July 20, 1969.",
                "es": "El Apolo 11 alunizó el 20 de julio de 1969.",
            },
        },
        {
            "id": "g5",
            "question": {
                "en": "Riddle: What has keys but can't open locks, space but no room, and you can enter but not go outside?",
                "es": "Adivinanza: ¿Qué tiene teclas pero no abre cerraduras, tiene espacio pero no habitaciones, y puedes entrar pero no salir?",
            },
            "answers": ["keyboard", "teclado"],
            "answer_display": {"en": "Keyboard", "es": "Teclado"},
            "choices": {"en": [], "es": []},
            "explanation": {
                "en": "A computer keyboard fits all the clues.",
                "es": "Un teclado de computadora encaja con todas las pistas.",
            },
        },
        {
            "id": "g6",
            "question": {
                "en": "Which ocean current keeps Western Europe warmer than other regions at similar latitudes?",
                "es": "¿Qué corriente oceánica mantiene a Europa occidental más cálida que otras regiones de latitud similar?",
            },
            "answers": ["gulf stream", "north atlantic drift", "corriente del golfo", "deriva noratlántica"],
            "answer_display": {"en": "The Gulf Stream", "es": "La corriente del Golfo"},
            "choices": {
                "en": ["The Gulf Stream", "California Current", "Canary Current", "Oyashio Current"],
                "es": ["La corriente del Golfo", "Corriente de California", "Corriente de Canarias", "Corriente de Oyashio"],
            },
            "explanation": {
                "en": "The Gulf Stream/North Atlantic Drift carries warm water toward Europe.",
                "es": "La corriente del Golfo o deriva noratlántica lleva agua cálida hacia Europa.",
            },
        },
        {
            "id": "g7",
            "question": {
                "en": "Which gas do plants primarily absorb from the atmosphere during photosynthesis?",
                "es": "¿Qué gas absorben principalmente las plantas de la atmósfera durante la fotosíntesis?",
            },
            "answers": ["carbon dioxide", "co2", "dioxido de carbono", "dióxido de carbono"],
            "answer_display": {"en": "Carbon dioxide", "es": "Dióxido de carbono"},
            "choices": {
                "en": ["Carbon dioxide", "Oxygen", "Nitrogen", "Hydrogen"],
                "es": ["Dióxido de carbono", "Oxígeno", "Nitrógeno", "Hidrógeno"],
            },
            "explanation": {
                "en": "Plants take in carbon dioxide and release oxygen.",
                "es": "Las plantas absorben dióxido de carbono y liberan oxígeno.",
            },
        },
        {
            "id": "g8",
            "question": {
                "en": "Riddle: The more of this there is, the less you see. What is it?",
                "es": "Adivinanza: Cuanto más hay de esto, menos ves. ¿Qué es?",
            },
            "answers": ["darkness", "oscuridad"],
            "answer_display": {"en": "Darkness", "es": "Oscuridad"},
            "choices": {"en": [], "es": []},
            "explanation": {
                "en": "Darkness obscures vision as it increases.",
                "es": "La oscuridad dificulta la visión a medida que aumenta.",
            },
        },
    ],
}

TRIVIA_LOOKUP: Dict[str, Dict[str, Dict[str, Any]]] = {
    category: {entry["id"]: entry for entry in entries}
    for category, entries in TRIVIA_BANK.items()
}


def _serialize_trivia_session(session: TriviaSession) -> Dict[str, Any]:
    return {
        "category": session.category,
        "score": session.score,
        "total": session.total,
        "asked_ids": sorted(list(session.asked_ids)),
        "current_id": session.current_id,
        "language": session.language,
        "owner_id": session.owner_id,
        "channel_idx": session.channel_idx,
        "is_direct": session.is_direct,
        "display_name": session.display_name,
    }


def _deserialize_trivia_session(player_key: str, data: Dict[str, Any]) -> TriviaSession:
    asked = data.get("asked_ids") or []
    if not isinstance(asked, list):
        asked = []
    session = TriviaSession(
        player_key=player_key,
        category=data.get("category", "general"),
        score=int(data.get("score", 0)),
        total=int(data.get("total", 0)),
        asked_ids=set(str(x) for x in asked),
        current_id=data.get("current_id"),
        language=data.get("language", "en"),
        owner_id=data.get("owner_id"),
        channel_idx=data.get("channel_idx"),
        is_direct=bool(data.get("is_direct", True)),
        display_name=data.get("display_name"),
    )
    return session


def _load_trivia_state_store() -> None:
    loaded = safe_load_json(TRIVIA_STATE_FILE, {})
    if not isinstance(loaded, dict):
        return
    for player_key, data in loaded.items():
        if isinstance(player_key, str) and isinstance(data, dict):
            session = _deserialize_trivia_session(player_key, data)
            TRIVIA_SESSIONS[player_key] = session


def _save_trivia_state_store() -> None:
    try:
        payload = {
            key: _serialize_trivia_session(session)
            for key, session in TRIVIA_SESSIONS.items()
        }
        with open(TRIVIA_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception as e:
        clean_log(f"Could not persist trivia state: {e}", "⚠️")


_load_trivia_state_store()


TRIVIA_SKIP_WORDS = {"skip", "pass", "next", "omitir", "saltar", "pasar", "siguiente", "continuar"}
TRIVIA_SCORE_WORDS = {"score", "leaderboard", "puntaje", "tabla", "ranking", "marcador", "puntuacion", "puntuación"}


def _normalize_trivia_answer_text(text: str) -> str:
    return "".join(ch.lower() for ch in text if ch.isalnum())


def _trivia_category_title(category: str, language: str) -> str:
    titles = TRIVIA_CATEGORY_TITLES.get(category, {})
    return titles.get(language) or titles.get("en") or category.title()


def _trivia_player_key(sender_id: Any, is_direct: bool, channel_idx: Optional[int], category: str) -> str:
    scope = "DM" if is_direct else f"CH{channel_idx if channel_idx is not None else 'broadcast'}"
    return f"{sender_id}#{scope}::{category}"


def _compute_trivia_display_name(sender_id: Any, is_direct: bool, channel_idx: Optional[int]) -> str:
    try:
        base = get_node_shortname(sender_id)
    except Exception:
        base = str(sender_id)
    if is_direct:
        return base
    channel_names = config.get("channel_names", {}) if isinstance(config, dict) else {}
    if channel_idx is None:
        channel_label = "Broadcast"
    else:
        channel_label = channel_names.get(str(channel_idx), f"Ch{channel_idx}")
    return f"{base} @ {channel_label}"


def _get_trivia_session(
    sender_id: Any,
    is_direct: bool,
    channel_idx: Optional[int],
    category: str,
    language: str,
) -> TriviaSession:
    key = _trivia_player_key(sender_id, is_direct, channel_idx, category)
    session = TRIVIA_SESSIONS.get(key)
    created = False
    if session is None:
        session = TriviaSession(
            player_key=key,
            category=category,
            language=language,
            owner_id=str(sender_id) if sender_id is not None else None,
            channel_idx=channel_idx,
            is_direct=is_direct,
        )
        TRIVIA_SESSIONS[key] = session
        created = True
    session.owner_id = str(sender_id) if sender_id is not None else session.owner_id
    session.channel_idx = channel_idx
    session.is_direct = is_direct
    session.language = language
    session.display_name = _compute_trivia_display_name(sender_id, is_direct, channel_idx)
    if created:
        _save_trivia_state_store()
    return session


def _get_trivia_question_by_id(category: str, question_id: Optional[str]) -> Optional[Dict[str, Any]]:
    if question_id is None:
        return None
    return TRIVIA_LOOKUP.get(category, {}).get(question_id)


def _pick_trivia_question(session: TriviaSession) -> Optional[Dict[str, Any]]:
    bank = TRIVIA_BANK.get(session.category, [])
    if not bank:
        return None
    available = [q for q in bank if q["id"] not in session.asked_ids]
    if not available:
        session.asked_ids.clear()
        available = list(bank)
    question = random.choice(available)
    session.current_id = question["id"]
    session.asked_ids.add(question["id"])
    session.hint_used = False
    return question


def _trivia_percentage(score: int, total: int) -> int:
    if total <= 0:
        return 0
    return int(round((score / total) * 100))


def _format_trivia_question_text(category: str, question: Dict[str, Any], command_name: str, language: str) -> str:
    strings = TRIVIA_STRINGS.get(language, TRIVIA_STRINGS["en"])
    title = _trivia_category_title(category, language)
    lines: List[str] = []
    icon = TRIVIA_CATEGORY_EMOJI.get(category, "❓")
    lines.append(strings["question_intro"].format(title=title, icon=icon))
    question_text = _localized_text(question.get("question"), language)
    if question_text:
        lines.append(question_text)
    choices = _localized_list(question.get("choices"), language)
    if choices:
        lines.append("")
        lines.append(strings["choices_intro"])
        for idx, choice in enumerate(choices):
            label = chr(ord("A") + idx)
            lines.append(f"  {label}) {choice}")
    lines.append("")
    lines.append(strings["answer_prompt"].format(command=command_name))
    return "\n".join(lines)


def _format_trivia_score_line(session: TriviaSession, strings: Dict[str, str]) -> Optional[str]:
    if session.total <= 0:
        return None
    percent = _trivia_percentage(session.score, session.total)
    return strings["score_line"].format(score=session.score, total=session.total, percent=percent)


def _format_trivia_leaderboard(category: str, current_session: TriviaSession, language: str) -> str:
    strings = TRIVIA_STRINGS.get(language, TRIVIA_STRINGS["en"])
    title = _trivia_category_title(category, language)
    lines: List[str] = []

    if current_session.total > 0:
        percent = _trivia_percentage(current_session.score, current_session.total)
        lines.append(strings["your_score"].format(score=current_session.score, total=current_session.total, percent=percent))
        lines.append("")

    sessions = [s for s in TRIVIA_SESSIONS.values() if s.category == category and s.total > 0]
    if not sessions:
        lines.append(strings["no_scores"])
        return "\n".join(lines)

    sessions.sort(key=lambda s: (-s.score, s.total, s.display_name or s.player_key))
    lines.append(strings["leaderboard_title"].format(title=title))
    for idx, entry in enumerate(sessions[:5], start=1):
        percent = _trivia_percentage(entry.score, entry.total)
        name = entry.display_name or entry.player_key
        lines.append(strings["leaderboard_entry"].format(rank=idx, name=name, score=entry.score, total=entry.total, percent=percent))
    return "\n".join(lines)


def _evaluate_trivia_answer(
    session: TriviaSession,
    question: Dict[str, Any],
    user_answer: str,
    command_name: str,
    language: str,
) -> str:
    strings = TRIVIA_STRINGS.get(language, TRIVIA_STRINGS["en"])
    choices = _localized_list(question.get("choices"), language)
    acceptable = question.get("answers") or []
    acceptable_norm = [_normalize_trivia_answer_text(ans) for ans in acceptable]

    user_input = user_answer.strip()
    normalized = _normalize_trivia_answer_text(user_input)
    if choices and user_input.strip().upper() in [chr(ord("A") + i) for i in range(len(choices))]:
        idx = ord(user_input.strip().upper()) - ord("A")
        if 0 <= idx < len(choices):
            normalized = _normalize_trivia_answer_text(choices[idx])

    session.total += 1
    correct = normalized in acceptable_norm
    if correct:
        session.score += 1

    explanation = _localized_text(question.get("explanation"), language)
    answer_display = _localized_text(question.get("answer_display"), language)
    if correct:
        result_line = strings["correct"].format(explanation=explanation) if explanation else strings["correct_no_expl"]
    else:
        correct_text = answer_display or (acceptable[0] if acceptable else question.get("answer", ""))
        if explanation:
            result_line = strings["incorrect"].format(answer=correct_text, explanation=explanation)
        else:
            result_line = strings["incorrect_no_expl"].format(answer=correct_text)

    score_line = _format_trivia_score_line(session, strings)

    next_question = _pick_trivia_question(session)
    next_block = None
    if next_question:
        next_block = _format_trivia_question_text(session.category, next_question, command_name, language)

    _save_trivia_state_store()

    response_lines = [result_line]
    if score_line:
        response_lines.append(score_line)
    if next_block:
        response_lines.append("")
        response_lines.append(strings["new_question"])
        response_lines.append(next_block)
    return "\n".join(response_lines)


def handle_trivia_command(
    command_name: str,
    category: str,
    arguments: str,
    sender_id: Any,
    is_direct: bool,
    channel_idx: Optional[int],
    language_hint: Optional[str],
) -> str:
    language = _normalize_language_code(language_hint) if language_hint else LANGUAGE_FALLBACK
    strings = TRIVIA_STRINGS.get(language, TRIVIA_STRINGS["en"])

    session = _get_trivia_session(sender_id, is_direct, channel_idx, category, language)

    args = arguments.strip()
    if not args:
        question = _pick_trivia_question(session)
        if not question:
            return strings["no_questions"]
        _save_trivia_state_store()
        question_text = _format_trivia_question_text(category, question, command_name, language)
        score_line = _format_trivia_score_line(session, strings)
        if score_line:
            return f"{question_text}\n\n{score_line}"
        return question_text

    lower_args = args.lower()
    if lower_args in TRIVIA_SCORE_WORDS:
        return _format_trivia_leaderboard(category, session, language)

    if lower_args == "hint":
        if session.current_id is None:
            return strings["no_question"].format(command=command_name)
        if session.hint_used:
            return strings.get("hint_used", "Hint already used.")
        question = _get_trivia_question_by_id(session.category, session.current_id)
        if not question:
            return strings["no_question"].format(command=command_name)
        answers = question.get("answers") or []
        letter = "?"
        for ans in answers:
            for ch in str(ans):
                if ch.isalpha():
                    letter = ch.upper()
                    break
            if letter != "?":
                break
        session.hint_used = True
        _save_trivia_state_store()
        template = strings.get("hint") or "Hint: starts with {letter}."
        return template.format(letter=letter)

    if lower_args in TRIVIA_SKIP_WORDS:
        session.current_id = None
        question = _pick_trivia_question(session)
        if not question:
            _save_trivia_state_store()
            return strings["no_questions"]
        _save_trivia_state_store()
        question_text = _format_trivia_question_text(category, question, command_name, language)
        score_line = _format_trivia_score_line(session, strings)
        response = f"{strings['skipped']}\n\n{question_text}"
        if score_line:
            response += f"\n\n{score_line}"
        return response

    if session.current_id is None:
        return strings["no_question"].format(command=command_name)

    question = _get_trivia_question_by_id(session.category, session.current_id)
    if question is None:
        # Question data rotated; fetch a fresh one and prompt again.
        question = _pick_trivia_question(session)
        if not question:
            _save_trivia_state_store()
            return strings["no_questions"]
        _save_trivia_state_store()
        question_text = _format_trivia_question_text(category, question, command_name, language)
        return f"{strings['no_question'].format(command=command_name)}\n\n{question_text}"

    response = _evaluate_trivia_answer(session, question, args, command_name, language)
    return response



TRAINER_CONTENT: Dict[str, Dict[str, Any]] = {
    "morsecodetrainer": {
        "title": {"en": "📻 Morse Code Trainer", "es": "📻 Entrenador de código Morse"},
        "sections": [
            {
                "title": {"en": "🔔 Core signals to memorize", "es": "🔔 Señales básicas para memorizar"},
                "bullets": [
                    {"en": "🔤 A = · – (di-dah), N = – · (dah-di)", "es": "🔤 A = · – (di-dah), N = – · (dah-di)"},
                    {"en": "🆘 SOS = · · · – – – · · · (three short, three long, three short)", "es": "🆘 SOS = · · · – – – · · · (tres cortos, tres largos, tres cortos)"},
                    {"en": "🔢 Numbers: 1 = · – – – –, 5 = · · · · ·, 0 = – – – – –", "es": "🔢 Números: 1 = · – – – –, 5 = · · · · ·, 0 = – – – – –"},
                    {"en": "📡 Prosigns: AR = · – · – · (end of message), SK = · · · – · – (clear)", "es": "📡 Prosignos: AR = · – · – · (fin del mensaje), SK = · · · – · – (libre)"},
                ],
            },
            {
                "title": {"en": "🔥 Practice drill", "es": "🔥 Ejercicio de práctica"},
                "bullets": [
                    {"en": "⏱️ Spend 3 minutes copying five random letters at 12 WPM; keep spacing steady.", "es": "⏱️ Dedica 3 minutos a copiar cinco letras aleatorias a 12 WPM; mantén el espaciado uniforme."},
                    {"en": "📻 Send your name and grid square using rhythmic taps or flashlight pulses.", "es": "📻 Envía tu nombre y cuadrícula usando toques rítmicos o pulsos de linterna."},
                    {"en": "🎧 Record yourself and play it back to spot uneven dits/dahs.", "es": "🎧 Grábate y reprodúcelo para detectar dits/dahs irregulares."}
                ],
            },
            {
                "title": {"en": "🌐 Mesh challenge", "es": "🌐 Desafío en la malla"},
                "bullets": [
                    {"en": "🤝 Pick a partner: trade short weather reports in Morse, then translate within 1 minute.", "es": "🤝 Elige un compañero: intercambien reportes breves del clima en Morse y traduzcan en menos de 1 minuto."},
                    {"en": "🌐 Post a three-word encouragement in Morse; wait for someone to decode before revealing the plaintext.", "es": "🌐 Publica un mensaje de aliento de tres palabras en Morse; espera a que alguien lo descifre antes de revelar el texto."}
                ],
            },
        ],
        "challenge": {"en": "⭐ Pro tip: set a metronome around 60 BPM so each beat equals one dit for smooth rhythm.", "es": "⭐ Consejo: ajusta un metrónomo a unos 60 BPM para que cada pulso sea un dit y mantengas el ritmo."},
    },
    "hurricanetrainer": {
        "title": {"en": "🌀 Hurricane Safety Trainer", "es": "🌀 Entrenador de seguridad ante huracanes"},
        "sections": [
            {
                "title": {"en": "☀️ Before the storm (watch issued)", "es": "☀️ Antes de la tormenta (aviso emitido)"},
                "bullets": [
                    {"en": "📸 Document home exterior with photos; store a copy in the cloud.", "es": "📸 Documenta el exterior de la casa con fotos; guarda una copia en la nube."},
                    {"en": "✂️ Trim weak branches and secure propane tanks or grills.", "es": "✂️ Recorta ramas débiles y asegura tanques de propano o parrillas."},
                    {"en": "🎒 Stage a go-bag with waterproof IDs, cash, spare keys, and prescription refills.", "es": "🎒 Prepara una mochila de emergencia con identificaciones impermeables, efectivo, llaves de repuesto y medicamentos recetados."}
                ],
            },
            {
                "title": {"en": "🌧️ During impact", "es": "🌧️ Durante el impacto"},
                "bullets": [
                    {"en": "🛡️ Shelter in an interior room, away from windows; keep helmets for kids.", "es": "🛡️ Refúgiate en un cuarto interior, lejos de las ventanas; reserva cascos para los niños."},
                    {"en": "📻 Listen to NOAA alerts or mesh updates every 30 minutes; conserve phone battery.", "es": "📻 Escucha alertas de NOAA o actualizaciones de la malla cada 30 minutos; conserva la batería del teléfono."},
                    {"en": "⬆️ If storm surge threatens, move to higher floors—never to an attic without a way out.", "es": "⬆️ Si amenaza una marejada, sube a pisos superiores; nunca al ático sin una salida."}
                ],
            },
            {
                "title": {"en": "🌈 Post-storm checklist", "es": "🌈 Lista posterior a la tormenta"},
                "bullets": [
                    {"en": "🚫 Avoid floodwater—it can hide debris, live wires, or sewage.", "es": "🚫 Evita el agua de inundación; puede ocultar escombros, cables energizados o aguas residuales."},
                    {"en": "📷 Snap damage photos before temporary repairs for insurance.", "es": "📷 Toma fotos de los daños antes de reparaciones temporales para el seguro."},
                    {"en": "🤝 Coordinate neighborhood wellness checks; share generator power rotations.", "es": "🤝 Coordina revisiones de bienestar en el vecindario; compartan turnos de generador."}
                ],
            },
        ],
        "challenge": {"en": "⭐ Drill idea: run a 10-minute family briefing using this list and time how long it takes to secure shutters.", "es": "⭐ Ejercicio: realiza un informe familiar de 10 minutos con esta lista y mide cuánto tardan en asegurar las contraventanas."},
    },
    "tornadotrainer": {
        "title": {"en": "🌪️ Tornado Safety Trainer", "es": "🌪️ Entrenador de seguridad ante tornados"},
        "sections": [
            {
                "title": {"en": "🧰 Preparedness phase", "es": "🧰 Fase de preparación"},
                "bullets": [
                    {"en": "🏚️ Identify your lowest-level safe room; stock water, helmets, gloves, and whistle.", "es": "🏚️ Identifica tu refugio seguro en el nivel más bajo; almacena agua, cascos, guantes y un silbato."},
                    {"en": "👢 Keep sturdy shoes under every bed for debris-filled evacuations.", "es": "👢 Guarda zapatos resistentes bajo cada cama para evacuaciones entre escombros."},
                    {"en": "⏱️ Sign up for local siren tests; practice dropping into shelter under 60 seconds.", "es": "⏱️ Inscríbete en pruebas de sirenas locales; practica entrar al refugio en menos de 60 segundos."}
                ],
            },
            {
                "title": {"en": "⚠️ Warning in effect", "es": "⚠️ Advertencia en vigor"},
                "bullets": [
                    {"en": "🏃 Move instantly to shelter—no window watching, no driving to outrun it.", "es": "🏃 Muévete de inmediato al refugio: nada de mirar por la ventana ni intentar huir en auto."},
                    {"en": "🛏️ Cover yourself with mattress, cushions, or heavy blankets to guard against debris.", "es": "🛏️ Cúbrete con un colchón, cojines o mantas pesadas para protegerte de los escombros."},
                    {"en": "📡 Use your mesh device or radio in receive-only mode to avoid stray RF during lightning.", "es": "📡 Usa tu dispositivo de malla o radio en modo solo recepción para evitar RF errante durante los relámpagos."}
                ],
            },
            {
                "title": {"en": "🌤️ After the funnel passes", "es": "🌤️ Después de que pase el embudo"},
                "bullets": [
                    {"en": "⚡ Beware downed lines and leaking gas; shut mains off only if trained.", "es": "⚡ Cuidado con cables caídos y fugas de gas; cierra las llaves principales solo si sabes cómo."},
                    {"en": "🚧 Mark hazards (nails, glass) with bright tape for neighbors and responders.", "es": "🚧 Marca peligros (clavos, vidrios) con cinta brillante para vecinos y rescatistas."},
                    {"en": "📝 Log damage and survivor status in the mesh network to speed mutual aid.", "es": "📝 Registra daños y el estado de las personas en la malla para agilizar la ayuda mutua."}
                ],
            },
        ],
        "challenge": {"en": "⭐ Run a 5-minute shelter drill, then share a 'status OK' message with your call sign once you're secured.", "es": "⭐ Realiza un simulacro de refugio de 5 minutos y comparte un mensaje 'estado OK' con tu indicativo cuando estés a salvo."},
    },
    "radioprocedurestrainer": {
        "title": {"en": "📡 Emergency Radio Procedures Trainer", "es": "📡 Entrenador de procedimientos de radio de emergencia"},
        "sections": [
            {
                "title": {"en": "🗒️ Message format", "es": "🗒️ Formato del mensaje"},
                "bullets": [
                    {"en": "📣 Call: 'This is [your call sign], priority traffic for [station].'", "es": "📣 Llamada: 'Aquí [tu indicativo], tráfico prioritario para [estación].'"},
                    {"en": "🧭 Include: who you are, location (lat/long or landmark), need, and action requested.", "es": "🧭 Incluye: quién eres, ubicación (lat/lon o referencia), necesidad y acción solicitada."},
                    {"en": "🔚 Close with 'Over' to hand the channel back; use 'Out' only when terminating.", "es": "🔚 Cierra con 'Cambio' para devolver el canal; usa 'Fuera' solo al terminar."}
                ],
            },
            {
                "title": {"en": "🎙️ Clarity tips", "es": "🎙️ Consejos de claridad"},
                "bullets": [
                    {"en": "🗣️ Speak in short blocks under 10 seconds; pause for relays or acks.", "es": "🗣️ Habla en bloques cortos de menos de 10 segundos; haz pausas para relevos o acuses."},
                    {"en": "🔡 Spell critical words with NATO alphabet (e.g., 'MEDIC is Mike-Echo-Delta-India-Charlie').", "es": "🔡 Deletrea palabras críticas con el alfabeto NATO (ej., 'MEDIC es Mike-Echo-Delta-India-Charlie')."},
                    {"en": "📝 Log every send/receive time in a notebook for after-action review.", "es": "📝 Registra cada hora de envío y recepción en un cuaderno para la revisión posterior."}
                ],
            },
            {
                "title": {"en": "🔁 Mesh practice", "es": "🔁 Práctica en la malla"},
                "bullets": [
                    {"en": "🛰️ Send a simulated SITREP (situation report) to your group; request an acknowledgement.", "es": "🛰️ Envía un SITREP (reporte de situación) simulado a tu grupo; solicita un acuse de recibo."},
                    {"en": "🔄 Practice relaying a message exactly as received—note when you add clarifying remarks.", "es": "🔄 Practica retransmitir un mensaje exactamente como lo recibiste; anota si agregas aclaraciones."},
                    {"en": "🎛️ Rotate net control duty so everyone learns to queue and release the channel.", "es": "🎛️ Roten el control de la red para que todos practiquen cómo ordenar turnos y liberar el canal."}
                ],
            },
        ],
        "challenge": {"en": "⭐ Every weekend, log a 3-line SITREP to your mesh channel and note the fastest acknowledgement time.", "es": "⭐ Cada fin de semana registra un SITREP de 3 líneas en tu canal de malla y anota el acuse más rápido."},
    },
    "navigationtrainer": {
        "title": {"en": "🧭 Navigation Without a Compass", "es": "🧭 Navegación sin brújula"},
        "sections": [
            {
                "title": {"en": "☀️ Daytime cues", "es": "☀️ Referencias diurnas"},
                "bullets": [
                    {"en": "🌞 Track the sun: it rises roughly east and sets west—map shadow angles at noon.", "es": "🌞 Sigue al sol: sale aproximadamente por el este y se oculta al oeste; registra los ángulos de sombra al mediodía."},
                    {"en": "🌿 Observe vegetation: moss prefers northern shade in many regions (verify locally).", "es": "🌿 Observa la vegetación: el musgo prefiere la sombra del norte en muchas regiones (verifícalo localmente)."},
                    {"en": "💧 Follow water flow downhill; streams often converge toward populated valleys.", "es": "💧 Sigue el flujo del agua cuesta abajo; los arroyos suelen converger hacia valles poblados."}
                ],
            },
            {
                "title": {"en": "🌌 Night-sky guides", "es": "🌌 Guías del cielo nocturno"},
                "bullets": [
                    {"en": "⭐ Northern Hemisphere: locate the Big Dipper; the pointer stars aim at Polaris (North).", "es": "⭐ Hemisferio norte: localiza la Osa Mayor; las estrellas guía apuntan a Polaris (norte)."},
                    {"en": "🌠 Southern Hemisphere: use the Southern Cross—extend the long axis 4.5 times to find south.", "es": "🌠 Hemisferio sur: usa la Cruz del Sur; prolonga su eje largo 4.5 veces para ubicar el sur."},
                    {"en": "🌙 Track the Moon: in its first quarter, the illuminated side roughly faces west at sunset.", "es": "🌙 Observa la Luna: en su primer cuarto, el lado iluminado mira aproximadamente hacia el oeste al atardecer."}
                ],
            },
            {
                "title": {"en": "🥾 Field drill", "es": "🥾 Práctica en campo"},
                "bullets": [
                    {"en": "🪵 Shadow stick method: mark the tip of a stick's shadow every 15 min to draw an east-west line.", "es": "🪵 Método del palo y sombra: marca la punta de la sombra cada 15 min para trazar una línea este-oeste."},
                    {"en": "🚶 Travel using handrail features (roads, rivers) and pace-count landmarks every 100 meters.", "es": "🚶 Avanza usando elementos guía (caminos, ríos) y cuenta pasos entre puntos de referencia cada 100 metros."},
                    {"en": "📓 Log bearings and estimated distances in a notebook to compare with actual map data later.", "es": "📓 Anota rumbos y distancias estimadas en un cuaderno para compararlos luego con el mapa real."}
                ],
            },
        ],
        "challenge": {"en": "⭐ Choose a trail—navigate out using only natural cues, then verify accuracy with a compass on return.", "es": "⭐ Elige un sendero: navega solo con referencias naturales y verifica la precisión con una brújula al regresar."},
    },
    "boatingtrainer": {
        "title": {"en": "⛵ Boating Safety Trainer", "es": "⛵ Entrenador de seguridad náutica"},
        "sections": [
            {
                "title": {"en": "🛠️ Pre-launch checks", "es": "🛠️ Revisiones previas al zarpe"},
                "bullets": [
                    {"en": "🦺 Verify flotation devices for every passenger plus one spare.", "es": "🦺 Verifica dispositivos de flotación para cada pasajero y uno de repuesto."},
                    {"en": "🔧 Check bilge pump, nav lights, horn/whistle, and fire extinguishers.", "es": "🔧 Revisa la bomba de achique, luces de navegación, bocina/silbato y extintores."},
                    {"en": "🗺️ File a float plan with route, crew list, and ETA; share via mesh or text.", "es": "🗺️ Presenta un plan de navegación con ruta, tripulación y ETA; compártelo por la malla o mensaje."}
                ],
            },
            {
                "title": {"en": "🌊 Underway habits", "es": "🌊 Hábitos en navegación"},
                "bullets": [
                    {"en": "👀 Keep a 360° lookout every few minutes—assign a dedicated spotter in busy waters.", "es": "👀 Mantén una vigilancia 360° cada pocos minutos; asigna un vigía dedicado en aguas concurridas."},
                    {"en": "⏱️ Maintain safe speed for conditions; post a bow watch in low visibility.", "es": "⏱️ Mantén una velocidad segura según las condiciones; coloca un vigía en proa con baja visibilidad."},
                    {"en": "🌤️ Hydrate and shade crew; heat sickness is common on open water.", "es": "🌤️ Hidrata y da sombra a la tripulación; el golpe de calor es común en mar abierto."}
                ],
            },
            {
                "title": {"en": "🚨 Emergency response", "es": "🚨 Respuesta ante emergencias"},
                "bullets": [
                    {"en": "🛟 If someone falls overboard: shout, point, throw flotation, then circle back downwind.", "es": "🛟 Si alguien cae al agua: grita, señala, lanza flotación y regresa haciendo un giro a sotavento."},
                    {"en": "🔥 Engine fire: shut fuel, aim extinguisher at base, issue mayday if uncontrolled.", "es": "🔥 Incendio en motor: corta el combustible, apunta el extintor a la base y emite mayday si no se controla."},
                    {"en": "🛑 Grounding: cut engine, assess hull breach, deploy anchor to prevent further damage.", "es": "🛑 Varadura: apaga el motor, evalúa brechas en el casco y fondea el ancla para evitar más daños."}
                ],
            },
        ],
        "challenge": {"en": "⭐ Conduct a mock man-overboard drill within your crew and log the recovery time each month.", "es": "⭐ Realicen un simulacro de hombre al agua y registren el tiempo de recuperación cada mes."},
    },
    "wellnesstrainer": {
        "title": {"en": "🏠 Emergency Wellness & Home Care Trainer", "es": "🏠 Entrenador de bienestar y cuidado del hogar en emergencias"},
        "sections": [
            {
                "title": {"en": "🐾 Pet safety essentials", "es": "🐾 Esenciales de seguridad para mascotas"},
                "bullets": [
                    {"en": "🎒 Prepare a pet go-bag: food, collapsible bowls, meds, vet records, and comfort item.", "es": "🎒 Prepara una mochila para mascotas: alimento, platos plegables, medicinas, historial veterinario y objeto de consuelo."},
                    {"en": "🏷️ Label carriers with contact info; practice quick loading drills.", "es": "🏷️ Etiqueta transportadoras con datos de contacto; practica cargarlas rápidamente."},
                    {"en": "🧺 Keep extra litter or waste bags to maintain sanitation indoors.", "es": "🧺 Ten arena extra o bolsas para desechos y así mantener la sanidad en interiores."}
                ],
            },
            {
                "title": {"en": "🕯️ Home care during long blackouts", "es": "🕯️ Cuidado del hogar durante apagones prolongados"},
                "bullets": [
                    {"en": "🚪 Rotate fridge opening—group meals to limit cold loss and use thermometers to monitor temp.", "es": "🚪 Limita la apertura del refrigerador agrupando comidas y usa termómetros para vigilar la temperatura."},
                    {"en": "🌬️ Ventilate with cross-breeze during daylight; insulate windows with blankets at night.", "es": "🌬️ Ventila con corrientes cruzadas de día; aísla ventanas con cobijas por la noche."},
                    {"en": "🔋 Charge devices via solar panels by day; reserve battery banks for critical comms at night.", "es": "🔋 Carga dispositivos con paneles solares de día; reserva baterías para comunicaciones críticas por la noche."}
                ],
            },
            {
                "title": {"en": "🤝 Community wellness", "es": "🤝 Bienestar comunitario"},
                "bullets": [
                    {"en": "🗓️ Schedule neighborhood wellness check-ins twice daily via mesh or door knock.", "es": "🗓️ Programa revisiones de bienestar vecinal dos veces al día por la malla o tocando puertas."},
                    {"en": "📋 Share surplus supplies using a visible whiteboard or shared spreadsheet.", "es": "📋 Comparte suministros sobrantes con un pizarrón visible o una hoja compartida."},
                    {"en": "🩺 Log medical needs and stress signals to refer volunteers or telehealth resources.", "es": "🩺 Registra necesidades médicas y señales de estrés para asignar voluntarios o recursos de telemedicina."}
                ],
            },
        ],
        "challenge": {"en": "⭐ Host a 30-minute blackout simulation: run devices off battery and note any comfort gaps to fix.", "es": "⭐ Organiza un simulacro de apagón de 30 minutos: usa solo baterías y anota carencias de comodidad por resolver."},
    },
}


TRAINER_COMMAND_MAP = {
    "/morsecodetrainer": "morsecodetrainer",
    "/hurricanetrainer": "hurricanetrainer",
    "/tornadotrainer": "tornadotrainer",
    "/radioprocedurestrainer": "radioprocedurestrainer",
    "/navigationtrainer": "navigationtrainer",
    "/boatingtrainer": "boatingtrainer",
    "/wellnesstrainer": "wellnesstrainer",
}


def format_trainer_response(trainer_key: str, language: str) -> str:
    content = TRAINER_CONTENT.get(trainer_key)
    if not content:
        return "Trainer module is still loading. Try again soon."
    lang = _normalize_language_code(language) if language else LANGUAGE_FALLBACK
    lines: List[str] = []
    title = _localized_text(content.get("title"), lang)
    if not title:
        title = trainer_key.replace("trainer", "Trainer").title()
    lines.append(title)

    sections = content.get("sections", [])
    for section in sections:
        section_title = _localized_text(section.get("title"), lang)
        bullets = section.get("bullets", [])
        bullet_lines: List[str] = []
        for bullet in bullets:
            bullet_text = _localized_text(bullet, lang)
            if bullet_text:
                bullet_lines.append(bullet_text)
        if section_title or bullet_lines:
            lines.append("")
        if section_title:
            lines.append(section_title)
        for bullet_text in bullet_lines:
            lines.append(f"- {bullet_text}")

    challenge = _localized_text(content.get("challenge"), lang)
    if challenge:
        lines.append("")
        lines.append(challenge)
    return "\n".join(lines)


def format_structured_menu(menu_key: str, language: Optional[str]) -> str:
    lang = _preferred_menu_language(language)
    data = MENU_DEFINITIONS.get(menu_key)
    if not data:
        return "Menu is not available yet."
    lines: List[str] = []
    title = data.get("title", {}).get(lang) or data.get("title", {}).get("en")
    if title:
        lines.append(title)
    for section in data.get("sections", []):
        section_title = None
        if isinstance(section, dict):
            section_title = section.get("title", {}).get(lang) or section.get("title", {}).get("en")
        if lines and (section_title or section.get("items")):
            lines.append("")
        if section_title:
            lines.append(section_title)
        items = section.get("items", []) if isinstance(section, dict) else []
        for item in items:
            text = ""
            if isinstance(item, dict):
                if "text" in item:
                    text = _localized_text(item.get("text"), lang)
                else:
                    command = item.get("command")
                    description = _localized_text(item.get("description"), lang)
                    if command and description:
                        text = f"{command} - {description}".strip()
                    elif command:
                        text = str(command)
            elif isinstance(item, (list, tuple)) and len(item) == 2:
                command, desc_map = item
                description = _localized_text(desc_map, lang) if isinstance(desc_map, dict) else str(desc_map)
                text = f"{command} - {description}".strip() if description else str(command)
            elif isinstance(item, str):
                text = item
            if text:
                lines.append(text.strip())
    footer = data.get("footer", {}).get(lang) or data.get("footer", {}).get("en")
    if footer:
        lines.append("")
        lines.append(footer)
    return "\n".join(lines)


def format_survival_guide(cmd: str, language: Optional[str]) -> str:
    lang = _preferred_menu_language(language)
    guide = SURVIVAL_GUIDES.get(cmd)
    if not guide:
        return "Survival notes are not available yet."
    lines: List[str] = []
    title = guide.get("title", {}).get(lang) or guide.get("title", {}).get("en")
    if title:
        lines.append(title)
    points = guide.get("points", [])
    if points:
        lines.append("")
        for point in points:
            text = point.get(lang) or point.get("en")
            if text:
                lines.append(f"- {text}")
    reflection = guide.get("reflection", {}).get(lang) or guide.get("reflection", {}).get("en")
    if reflection:
        label = SURVIVAL_REFLECTION_LABEL.get(lang) or SURVIVAL_REFLECTION_LABEL.get("en")
        lines.append("")
        lines.append(f"{label}: {reflection}")
    return "\n".join(lines)



LANGUAGE_STRINGS = {
    "en": {
        "alias_note": "Interpreting {original} as {canonical} (alias).",
        "fuzzy_note": "Interpreting {original} as {canonical} (closest match).",
        "unknown_intro": "I didn't recognize `{original}` as a command.",
        "suggestion_intro": "Maybe you meant: {suggestions}.",
        "try_help": "Try `/help` for the full list.",
    },
    "es": {
        "alias_note": "Interpretando {original} como {canonical} (alias).",
        "fuzzy_note": "Interpretando {original} como {canonical} (coincidencia más cercana).",
        "unknown_intro": "No reconocí `{original}` como un comando.",
        "suggestion_intro": "Quizá quisiste decir: {suggestions}.",
        "try_help": "Prueba `/help` para ver la lista completa.",
    },
    "fr": {
        "alias_note": "Interprétation de {original} comme {canonical} (alias).",
        "fuzzy_note": "Interprétation de {original} comme {canonical} (correspondance la plus proche).",
        "unknown_intro": "Je n'ai pas reconnu `{original}` comme commande.",
        "suggestion_intro": "Vouliez-vous dire : {suggestions} ?",
        "try_help": "Essayez `/help` pour la liste complète.",
    },
    "de": {
        "alias_note": "Interpretation von {original} als {canonical} (Alias).",
        "fuzzy_note": "Interpretation von {original} als {canonical} (beste Übereinstimmung).",
        "unknown_intro": "Ich habe `{original}` nicht als Befehl erkannt.",
        "suggestion_intro": "Meintest du: {suggestions}?",
        "try_help": "Nutze `/help` für alle Befehle.",
    },
    "zh": {
        "alias_note": "将 {original} 解释为 {canonical}（别名）。",
        "fuzzy_note": "将 {original} 解释为 {canonical}（最接近的匹配）。",
        "unknown_intro": "未识别 `{original}` 这个指令。",
        "suggestion_intro": "是否想输入：{suggestions}？",
        "try_help": "可以发送 `/help` 查看全部指令。",
    },
    "pl": {
        "alias_note": "Interpretuję {original} jako {canonical} (alias).",
        "fuzzy_note": "Interpretuję {original} jako {canonical} (najbliższe dopasowanie).",
        "unknown_intro": "Nie rozpoznano komendy `{original}`.",
        "suggestion_intro": "Może chodziło o: {suggestions}.",
        "try_help": "Użyj `/help`, aby zobaczyć pełną listę.",
    },
    "hr": {
        "alias_note": "Tumačim {original} kao {canonical} (alias).",
        "fuzzy_note": "Tumačim {original} kao {canonical} (najbliže podudaranje).",
        "unknown_intro": "Nisam prepoznao naredbu `{original}`.",
        "suggestion_intro": "Možda ste mislili: {suggestions}.",
        "try_help": "Probajte `/help` za cijeli popis.",
    },
    "uk": {
        "alias_note": "Інтерпретую {original} як {canonical} (аліас).",
        "fuzzy_note": "Інтерпретую {original} як {canonical} (найближчий збіг).",
        "unknown_intro": "Не розпізнано команду `{original}`.",
        "suggestion_intro": "Можливо, ви мали на увазі: {suggestions}.",
        "try_help": "Спробуйте `/help`, щоб побачити перелік команд.",
    },
    "sw": {
        "alias_note": "Natafsiri {original} kuwa {canonical} (kirai).",
        "fuzzy_note": "Natafsiri {original} kuwa {canonical} (mfanano wa karibu).",
        "unknown_intro": "Sikutambua `{original}` kama amri.",
        "suggestion_intro": "Je ulimaanisha: {suggestions}?",
        "try_help": "Tumia `/help` kupata orodha kamili.",
    },
}


LANGUAGE_RESPONSES = {
    "es": {
        "dm_only": "❌ Este comando sólo puede usarse en un mensaje directo.",
        "motd_current": "MOTD actual:\n{motd}",
        "changemotd_usage": "Uso: /changemotd Tu nuevo texto MOTD",
        "changemotd_success": "✅ MOTD actualizado. Usa /motd para verlo.",
        "changemotd_error": "❌ No se pudo actualizar el MOTD: {error}",
        "changeprompt_usage": "Uso: /changeprompt Tu nuevo prompt del sistema",
        "changeprompt_success": "✅ Prompt del sistema actualizado.",
        "changeprompt_error": "❌ No se pudo actualizar el prompt del sistema: {error}",
        "showprompt_current": "Prompt del sistema actual:\n{prompt}",
        "showprompt_error": "❌ No se pudo mostrar el prompt del sistema: {error}",
        "password_prompt": "responde con la contraseña",
        "password_success": "¡Listo! Ahora estás autorizado para hacer cambios de administrador",
        "password_failure": "ni hablar, inténtalo de nuevo... o no",
        "weather_need_city": "No pude encontrar esa ubicación. Dame la ciudad principal más cercana y lo intento de nuevo.",
        "weather_final_fail": "Aún no encuentro esa ubicación. Intenta con otra ciudad o código postal.",
        "weather_service_fail": "No pude obtener el informe del clima en este momento.",
        "weather_offline": "Los datos del clima están fuera de línea.",
        "weather_cached_intro": "⚠️ Clima en vivo no disponible. Pronóstico en caché de El Paso (generado {generated}).",
        "weather_cached_outro": "Esta información podría estar desactualizada.",
        "meshinfo_header": "Resumen de la red (última hora)",
        "meshinfo_new_nodes_some": "Nodos nuevos: {count} ({list})",
        "meshinfo_new_nodes_none": "Nodos nuevos: ninguno",
        "meshinfo_left_nodes_some": "Nodos que salieron: {count} ({list})",
        "meshinfo_left_nodes_none": "Ningún nodo se desconectó en la última hora",
        "meshinfo_avg_batt": "Voltaje promedio (sin alimentación USB): {voltage:.2f} V ({count} nodos)",
        "meshinfo_avg_batt_unknown": "Sin datos suficientes de batería",
        "meshinfo_network_usage": "Uso de red aproximado: {percent}% (última hora)",
        "meshinfo_top_nodes": "Top nodos por tráfico: {list}",
        "meshinfo_top_nodes_none": "Sin tráfico registrado en la última hora",
        "bible_missing": "📜 La biblioteca de Escrituras no está disponible en este momento.",
        "bible_help": "📖 Guía rápida Biblia: `/biblia` sigue tu lectura. Busca con `/biblia Juan 3:16`. Añade `in Spanish` o `en inglés` para cambiar idioma. Avanza o retrocede con `<1,2>`. Responde 22 en DM para auto-scroll 30 versículos (18s).",
        "antispam_timeout_short": "🚫 Demasiadas solicitudes en poco tiempo. Pausa de 10 minutos. Puedes volver a escribir a las {time}. Otro exceso puede provocar un bloqueo de 24 horas.",
        "antispam_timeout_long": "🚫 Actividad repetida detectada. Acceso bloqueado por 24 horas. Podrás volver a usar el bot el {time}. Después de este bloqueo, los límites vuelven a empezar.",
        "antispam_log_short": "Usuario {node} en pausa 10m (hasta {time}).",
        "antispam_log_long": "Usuario {node} bloqueado 24h (hasta {time}).",
        "bible_autoscroll_dm_only": "📖 El auto-scroll sólo funciona en mensajes directos.",
        "bible_autoscroll_need_nav": "📖 Usa primero /biblia y luego responde 22 para activar el auto-scroll.",
        "bible_autoscroll_start": "📖 Auto-scroll activado. Próximos versículos cada 12 segundos (30 total).",
        "bible_autoscroll_stop": "⏹️ Auto-scroll en pausa. Responde 22 para retomarlo.",
        "bible_autoscroll_finished": "📖 Auto-scroll en pausa tras 30 versículos. Responde 22 para continuar.",
        "chuck_missing": "🥋 El generador de datos de Chuck Norris está fuera de línea.",
        "blond_missing": "😅 La biblioteca de chistes de rubias está vacía por ahora.",
        "yomomma_missing": "😅 La biblioteca de chistes de tu mamá está vacía por ahora.",
        "invalid_choice": "Opción inválida. Inténtalo de nuevo.",
        "missing_destination": "Ese camino aún no está listo.",
    },
}


def translate(language: str, key: str, default: str, **kwargs) -> str:
    lang = _normalize_language_code(language)
    template = LANGUAGE_RESPONSES.get(lang, {}).get(key, default)
    try:
        return template.format(**kwargs)
    except Exception:
        return template


def get_language_strings(language: Optional[str]):
    lang = _normalize_language_code(language) if language else LANGUAGE_FALLBACK
    return LANGUAGE_STRINGS.get(lang, LANGUAGE_STRINGS["en"])


def _strip_command_token(cmd: str) -> str:
    token = cmd.strip()
    while token and token[-1] in TRAILING_COMMAND_PUNCT:
        token = token[:-1]
    if not token.startswith("/"):
        token = f"/{token.lstrip('/')}"
    return token.lower()


def _languages_for_alias(alias: str) -> List[str]:
    info = COMMAND_ALIASES.get(alias)
    if not info:
        return []
    langs = info.get("languages") or []
    return [lang for lang in langs if lang]


def _languages_for_canonical(canonical: str) -> List[str]:
    langs: List[str] = []
    for alias, info in COMMAND_ALIASES.items():
        if info.get("canonical", "").lower() == canonical.lower():
            langs.extend(info.get("languages") or [])
    return langs


def _pick_preferred_language(candidates: List[str]) -> Optional[str]:
    if not candidates:
        return None
    normalized_fallback = LANGUAGE_FALLBACK
    if normalized_fallback in candidates:
        return normalized_fallback
    if "en" in candidates:
        return "en"
    return candidates[0]


def _detect_language_for_token(token: str) -> Optional[str]:
    stripped = _strip_command_token(token)
    if stripped in BUILTIN_COMMANDS:
        return LANGUAGE_FALLBACK
    alias_langs = _languages_for_alias(stripped)
    if alias_langs:
        preferred = _pick_preferred_language(alias_langs)
        if preferred:
            return preferred
    canonical_langs = _languages_for_canonical(stripped)
    if canonical_langs:
        preferred = _pick_preferred_language(canonical_langs)
        if preferred:
            return preferred
    best_lang = None
    best_score = 0.0
    for alias, info in COMMAND_ALIASES.items():
        ratio = difflib.SequenceMatcher(None, stripped, alias).ratio()
        if ratio > best_score and ratio >= 0.5:
            langs = info.get("languages") or []
            if langs:
                best_lang = _pick_preferred_language([lang for lang in langs if lang]) or best_lang
                best_score = ratio
    if not best_lang:
        canonical_langs = _languages_for_canonical(stripped)
        if canonical_langs:
            best_lang = _pick_preferred_language(canonical_langs)
    return best_lang


def _known_commands() -> Set[str]:
    known = set(BUILTIN_COMMANDS)
    for entry in commands_config.get("commands", []):
        custom_cmd = entry.get("command")
        if not isinstance(custom_cmd, str):
            continue
        normalized = custom_cmd if custom_cmd.startswith("/") else f"/{custom_cmd}"  # keep slash prefix
        known.add(normalized.lower())
    for alias, info in COMMAND_ALIASES.items():
        known.add(alias.lower())
        canonical = info.get("canonical")
        if isinstance(canonical, str):
            known.add(canonical.lower())
    return known


def resolve_command_token(raw: str):
    """Resolve a raw slash token to a canonical command and optional notice."""
    stripped = _strip_command_token(raw)
    alias_info = COMMAND_ALIASES.get(stripped)
    if alias_info:
        canonical = alias_info.get("canonical", stripped)
        langs = _languages_for_alias(stripped)
        language = _pick_preferred_language(langs) if langs else None
        append_text = alias_info.get("append", "")
        return canonical, "alias", None, language, append_text
    known = _known_commands()
    if stripped in known:
        language = _detect_language_for_token(stripped)
        return stripped, None, None, language, ""
    candidates = difflib.get_close_matches(stripped, list(known), n=1, cutoff=FUZZY_COMMAND_MATCH_THRESHOLD)
    if candidates:
        candidate = candidates[0]
        canonical = candidate
        language = _detect_language_for_token(candidate) or _detect_language_for_token(stripped)
        if candidate in COMMAND_ALIASES:
            canonical = COMMAND_ALIASES[candidate].get("canonical", candidate)
        return canonical, "fuzzy", None, language, ""
    suggestions = difflib.get_close_matches(stripped, list(known), n=3, cutoff=0.3)
    language = _detect_language_for_token(stripped)
    return None, "unknown", suggestions, language, ""


def annotate_command_response(resp, original_cmd: str, canonical_cmd: str, reason: str, language: Optional[str]):
    if canonical_cmd == original_cmd:
        return resp
    strings = get_language_strings(language)
    if reason == "alias":
        note = strings["alias_note"].format(original=original_cmd, canonical=canonical_cmd)
    else:
        note = strings["fuzzy_note"].format(original=original_cmd, canonical=canonical_cmd)
    try:
        clean_log(note, "ℹ️", show_always=True, rate_limit=False)
    except Exception:
        pass
    return resp


def format_unknown_command_reply(original_cmd: str, suggestions: Optional[List[str]], language: Optional[str]) -> str:
    strings = get_language_strings(language)
    parts = [strings["unknown_intro"].format(original=original_cmd)]
    if suggestions:
        suggestion_text = ", ".join(suggestions)
        parts.append(strings["suggestion_intro"].format(suggestions=suggestion_text))
    parts.append(strings["try_help"])
    return " ".join(parts)


def _process_admin_password(sender_id: Any, message: str):
    sender_key = _safe_sender_key(sender_id)
    pending_request = PENDING_ADMIN_REQUESTS.get(sender_key)
    attempt = (message or "").strip()
    lang = None
    if pending_request:
        lang = pending_request.get("language")
    if attempt == ADMIN_PASSWORD:
        AUTHORIZED_ADMINS.add(sender_key)
        if pending_request:
            PENDING_ADMIN_REQUESTS.pop(sender_key, None)
        clean_log(
            f"Admin password accepted for {get_node_shortname(sender_id)} ({sender_id})",
            "✅",
            show_always=True,
            rate_limit=False,
        )
        success_text = translate(lang or 'en', 'password_success', "Bingo! you're now authorized to make admin changes")
        follow_resp = None
        if pending_request:
            follow_resp = handle_command(
                pending_request.get("command", ""),
                pending_request.get("full_text", ""),
                sender_id,
                is_direct=pending_request.get("is_direct", True),
                channel_idx=pending_request.get("channel_idx"),
                thread_root_ts=pending_request.get("thread_root_ts"),
                language_hint=lang,
            )
        if isinstance(follow_resp, PendingReply):
            combined = f"{success_text}\n{follow_resp.text}" if follow_resp.text else success_text
            return PendingReply(combined, follow_resp.reason)
        if isinstance(follow_resp, str) and follow_resp:
            combined = f"{success_text}\n{follow_resp}"
            return PendingReply(combined, "admin password")
        return PendingReply(success_text, "admin password")

    clean_log(
        f"Admin password rejected for {get_node_shortname(sender_id)} ({sender_id})",
        "🚫",
        show_always=True,
        rate_limit=False,
    )
    failure_text = translate(lang or 'en', 'password_failure', "no way jose, try again.. or don't")
    return PendingReply(failure_text, "admin password")
def _redact_sensitive(text: str) -> str:
    return text if text is not None else ""


def _safe_sender_key(sender_id: Any) -> str:
    try:
        return _sender_key(sender_id)
    except Exception:
        return ""


BIBLE_MAX_VERSE_RANGE = 5


def _resolve_bible_book(raw: str) -> Tuple[Optional[str], bool]:
    norm = _normalize_book_key(raw)
    if not norm:
        return None, False
    for key in (norm, norm.replace(" ", "")):
        if key and key in BIBLE_BOOK_ALIAS_MAP:
            return BIBLE_BOOK_ALIAS_MAP[key], False
    matches = difflib.get_close_matches(norm, BIBLE_BOOK_ALIAS_KEYS, n=1, cutoff=0.72)
    if matches:
        return BIBLE_BOOK_ALIAS_MAP[matches[0]], True
    compact = norm.replace(" ", "")
    if compact:
        matches = difflib.get_close_matches(compact, BIBLE_BOOK_ALIAS_KEYS, n=1, cutoff=0.72)
        if matches:
            return BIBLE_BOOK_ALIAS_MAP[matches[0]], True
    return None, False


def _suggest_bible_books(raw: str, limit: int = 3) -> List[str]:
    norm = _normalize_book_key(raw)
    if not norm:
        return []
    suggestions: List[str] = []
    for key in (norm, norm.replace(" ", "")):
        if not key:
            continue
        matches = difflib.get_close_matches(key, BIBLE_BOOK_ALIAS_KEYS, n=limit, cutoff=0.5)
        for match in matches:
            canonical = BIBLE_BOOK_ALIAS_MAP.get(match)
            if canonical and canonical not in suggestions:
                suggestions.append(canonical)
            if len(suggestions) >= limit:
                break
        if len(suggestions) >= limit:
            break
    return suggestions


def _normalize_reference_input(value: str) -> str:
    spaced = re.sub(r"(?i)(\d)([a-z])", r"\1 \2", value)
    spaced = re.sub(r"(?i)([a-z])(\d)", r"\1 \2", spaced)
    spaced = re.sub(r"\s+", " ", spaced)
    return spaced.strip()


_BIBLE_LANGUAGE_HINT_PATTERNS = {
    'es': [
        r"\bin\s+spanish\b",
        r"\bspanish\b",
        r"\bespanol\b",
        r"\bespañol\b",
        r"\ben\s+espanol\b",
        r"\ben\s+español\b",
        r"\bversion\s+espanol\b",
        r"\bversion\s+español\b",
    ],
    'en': [
        r"\bin\s+english\b",
        r"\benglish\b",
        r"\bingles\b",
        r"\bingl[eé]s\b",
        r"\ben\s+ingles\b",
        r"\ben\s+ingl[eé]s\b",
        r"\bversion\s+ingles\b",
        r"\bversion\s+ingl[eé]s\b",
    ],
}


def _extract_bible_language_hint(text: str) -> Tuple[str, Optional[str]]:
    if not text:
        return "", None
    working = text
    detected: Optional[str] = None
    for lang, patterns in _BIBLE_LANGUAGE_HINT_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, working, flags=re.IGNORECASE):
                working = re.sub(pattern, " ", working, flags=re.IGNORECASE)
                if detected is None:
                    detected = lang
    working = re.sub(r"\s{2,}", " ", working).strip()
    return working, detected


def _parse_bible_reference(text: str) -> Optional[Tuple[str, int, Optional[int], Optional[int]]]:
    normalized = _normalize_reference_input(text)
    if not normalized:
        return None
    match = re.match(r"^(.+?)\s+(\d+)(?::\s*([0-9]+(?:\s*[-–—]\s*[0-9]+)?))?", normalized)
    if not match:
        return None
    book_raw = match.group(1).strip()
    chapter_str = match.group(2)
    verse_part = match.group(3)
    try:
        chapter = int(chapter_str)
    except (TypeError, ValueError):
        return None
    verse_start = None
    verse_end = None
    if verse_part:
        range_parts = re.split(r"[-–—]", verse_part.strip())
        try:
            verse_start = int(range_parts[0])
        except (TypeError, ValueError):
            return None

def _antispam_handle_penalty(sender_key: str, sender_node: Any, interface_ref, info: Dict[str, Any]) -> None:
    level = info.get('level', 1)
    until = info.get('until', time.time())
    include_date = level == 2
    time_label = _antispam_format_time(until, include_date=include_date)
    lang = LANGUAGE_FALLBACK

    if level == 1:
        dm_text = translate(
            lang,
            'antispam_timeout_short',
            "🚫 Lots of requests in a short burst. You're paused for 10 minutes. Try again at {time}. Another spike could mean a 24-hour lock.",
            time=time_label,
        )
        log_text = translate(
            lang,
            'antispam_log_short',
            "User {node} paused for 10m (until {time}).",
            node=sender_key,
            time=time_label,
        )
    else:
        dm_text = translate(
            lang,
            'antispam_timeout_long',
            "🚫 Repeated high activity detected. Access is locked for 24 hours. You'll be able to use the bot again at {time}. After this lock, the usual limits reset.",
            time=time_label,
        )
        log_text = translate(
            lang,
            'antispam_log_long',
            "User {node} locked out for 24h (until {time}).",
            node=sender_key,
            time=time_label,
        )

    clean_log(log_text, "🚫")

    if interface_ref:
        try:
            send_direct_chunks(interface_ref, dm_text, sender_node)
        except Exception as exc:
            clean_log(f"Failed to send anti-spam notice to {sender_key}: {exc}", "⚠️")

    _antispam_mark_notified(sender_key)


def _antispam_after_response(
    sender_key: Optional[str],
    sender_node: Any,
    interface_ref,
    *,
    count_response: bool = True,
) -> None:
    if not count_response or not sender_key:
        return
    info = _antispam_register_trigger(sender_key)
    if info:
        _antispam_handle_penalty(sender_key, sender_node, interface_ref, info)

app = Flask(__name__)
messages = []
messages_lock = threading.Lock()
interface = None

lastDMNode = None
lastChannelIndex = None

# -----------------------------
# Health/Heartbeat State
# -----------------------------
last_rx_time = 0.0
last_tx_time = 0.0
last_ai_response_time = 0.0
last_ai_request_time = 0.0
ai_last_error = ""
ai_last_error_time = 0.0
heartbeat_running = False

def _now():
  return time.time()

# -----------------------------
# Async Message Processing
# -----------------------------
# Queue for pending AI responses to process asynchronously
try:
    RESPONSE_QUEUE_MAXSIZE = int(config.get("async_response_queue_max", 20))
except (ValueError, TypeError):
    RESPONSE_QUEUE_MAXSIZE = 20
RESPONSE_QUEUE_MAXSIZE = max(5, min(RESPONSE_QUEUE_MAXSIZE, 100))

response_queue = queue.Queue(maxsize=RESPONSE_QUEUE_MAXSIZE)  # Limit queue size to prevent memory issues
response_worker_running = False

def process_responses_worker():
    """Background worker thread to process AI responses without blocking new message reception."""
    global response_worker_running
    response_worker_running = True
    
    while response_worker_running:
        try:
            # Wait for a response task (timeout to allow clean shutdown)
            task = response_queue.get(timeout=1.0)
            if task is None:  # Shutdown signal
                break
                
            # Unpack the task
            text, sender_node, is_direct, ch_idx, thread_root_ts, interface_ref = task
            
            clean_log(f"⚡ [AsyncAI] Processing: {text[:50]}... (queue: {response_queue.qsize()})", "🤖")
            start_time = time.time()
            
            # Generate AI response (this can take a long time)
            resp = parse_incoming_text(text, sender_node, is_direct, ch_idx, thread_root_ts=thread_root_ts)
            
            processing_time = time.time() - start_time
            
            if resp:
                pending = resp if isinstance(resp, PendingReply) else None
                response_text = pending.text if pending else resp

                target_name = get_node_shortname(sender_node) or str(sender_node)
                summary = _truncate_for_log(response_text)
                clean_log(f"Ollama → {target_name} ({processing_time:.1f}s): {summary}", "🦙", show_always=True, rate_limit=False)

                # Reduced collision delay for async processing
                if pending:
                    _command_delay(pending.reason, delay=pending.pre_send_delay)
                else:
                    time.sleep(1)

                # Log reply and mark AI status accurately (non-AI responses keep delay + logging)
                ai_force = FORCE_NODE_NUM if FORCE_NODE_NUM is not None else None
                log_message(
                    AI_NODE_NAME,
                    response_text,
                    reply_to=thread_root_ts,
                    direct=is_direct,
                    channel_idx=(None if is_direct else ch_idx),
                    force_node=ai_force,
                    is_ai=(pending is None),
                )

                # Send the response via mesh
                chunk_delay = pending.chunk_delay if pending else None
                if interface_ref and response_text:
                    if is_direct:
                        send_direct_chunks(interface_ref, response_text, sender_node, chunk_delay=chunk_delay)
                    else:
                        send_broadcast_chunks(interface_ref, response_text, ch_idx, chunk_delay=chunk_delay)

                sender_key = _safe_sender_key(sender_node)
                _antispam_after_response(sender_key, sender_node, interface_ref)
                _process_bible_autoscroll_request(sender_key, sender_node, interface_ref)
                
                try:
                    globals()['last_ai_response_time'] = _now()
                except Exception:
                    pass
                total_time = time.time() - start_time
            else:
                clean_log(f"Ollama → {get_node_shortname(sender_node) or sender_node} ({processing_time:.1f}s): [no response]", "🦙", show_always=True, rate_limit=False)
                
            response_queue.task_done()
            
        except queue.Empty:
            continue  # Timeout, check if we should continue
        except Exception as e:
            clean_log(f"⚠️ [AsyncAI] Error processing response: {e}", "🚨")
            try:
                response_queue.task_done()
            except ValueError:
                pass  # task_done() called more times than get()

def start_response_worker():
    """Start the background response worker thread."""
    worker_thread = threading.Thread(target=process_responses_worker, daemon=True)
    worker_thread.start()
    clean_log("firin' up!", "🚀")

def stop_response_worker():
    """Stop the background response worker thread."""
    global response_worker_running
    response_worker_running = False
    response_queue.put(None)  # Signal shutdown

# -----------------------------
# Location Lookup Function
# -----------------------------
def get_node_location(node_id):
    if interface and hasattr(interface, "nodes") and node_id in (interface.nodes or {}):
        pos = (interface.nodes.get(node_id) or {}).get("position", {})
        lat = pos.get("latitude")
        lon = pos.get("longitude")
        tstamp = pos.get("time")
        precision = (
            pos.get("precision")
            or pos.get("precisionMeters")
            or pos.get("accuracy")
            or pos.get("gpsAccuracy")
        )
        dilution = (
            pos.get("dilution")
            or pos.get("pdop")
            or pos.get("hdop")
            or pos.get("vdop")
        )
        return lat, lon, tstamp, precision, dilution
    return None, None, None, None, None


def _sanitize_label(label: str) -> str:
    base = unidecode(label or "node").strip()
    base = re.sub(r"[^\w\s-]", "", base).strip() or "node"
    return re.sub(r"\s+", " ", base)


def _format_timestamp_local(ts: float) -> str:
    try:
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        return dt.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    except Exception:
        return str(ts)


def _generate_map_link(lat: Any, lon: Any, label: str) -> str:
    label_clean = _sanitize_label(label)
    encoded = urllib.parse.quote(label_clean)
    long_url = f"https://maps.google.com/?q={encoded}@{lat},{lon}"
    return long_url


def _update_location_history(
    sender_key: str,
    shortname: str,
    lat: Any,
    lon: Any,
    timestamp_val: float,
    precision: Any = None,
    dilution: Any = None,
) -> Dict[str, Any]:
    if not sender_key:
        sender_key = str(shortname or "node")
    try:
        lat_float = float(lat)
    except Exception:
        lat_float = lat
    try:
        lon_float = float(lon)
    except Exception:
        lon_float = lon
    map_url = _generate_map_link(lat_float, lon_float, shortname)
    precision_val = None
    dilution_val = None
    try:
        precision_val = float(precision)
    except Exception:
        if isinstance(precision, (int, float)):
            precision_val = float(precision)
    try:
        dilution_val = float(dilution)
    except Exception:
        if isinstance(dilution, (int, float)):
            dilution_val = float(dilution)
    quality = "precise"
    if precision_val is not None and precision_val > 80:
        quality = "dilute"
    if dilution_val is not None and dilution_val > 4:
        quality = "dilute"
    entry = {
        "key": sender_key,
        "shortname": shortname,
        "lat": lat_float,
        "lon": lon_float,
        "timestamp": timestamp_val,
        "map_url": map_url,
        "label": _sanitize_label(shortname),
        "precision": precision_val,
        "dilution": dilution_val,
        "quality": quality,
    }
    cutoff = _now() - LOCATION_HISTORY_RETENTION
    with LOCATION_HISTORY_LOCK:
        LOCATION_HISTORY[sender_key] = entry
        stale = [k for k, v in LOCATION_HISTORY.items() if v.get("timestamp", 0) < cutoff]
        for k in stale:
            LOCATION_HISTORY.pop(k, None)
    return entry


def _collect_recent_locations(exclude_key: Optional[str] = None, limit: Optional[int] = 5) -> List[Dict[str, Any]]:
    cutoff = _now() - LOCATION_HISTORY_RETENTION
    with LOCATION_HISTORY_LOCK:
        items = [
            v for k, v in LOCATION_HISTORY.items()
            if v.get("timestamp", 0) >= cutoff and (exclude_key is None or k != exclude_key)
        ]
    items.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
    results: List[Dict[str, Any]] = []
    slice_items = items if limit is None else items[:limit]
    for entry in slice_items:
        lat_val = entry.get("lat")
        lon_val = entry.get("lon")
        if isinstance(lat_val, (int, float)):
            lat_str = f"{lat_val:.5f}"
        else:
            lat_str = str(lat_val)
        if isinstance(lon_val, (int, float)):
            lon_str = f"{lon_val:.5f}"
        else:
            lon_str = str(lon_val)
        results.append({
            "key": entry.get("key"),
            "shortname": entry.get("shortname", "node"),
            "lat": lat_val,
            "lon": lon_val,
            "lat_str": lat_str,
            "lon_str": lon_str,
            "time_str": _format_timestamp_local(entry.get("timestamp", _now())),
            "map_url": entry.get("map_url"),
            "label": entry.get("label"),
            "quality": entry.get("quality", "unknown"),
            "precision": entry.get("precision"),
            "dilution": entry.get("dilution"),
        })

    return results


def _save_weather_reports_locked() -> None:
    data = WEATHER_REPORTS[-WEATHER_REPORT_MAX_ENTRIES:]
    payload = json.dumps({'reports': data}, ensure_ascii=False, indent=2)
    write_atomic(WEATHER_REPORT_FILE, payload)


def _prune_weather_reports_locked() -> None:
    cutoff = _now() - WEATHER_REPORT_RETENTION
    WEATHER_REPORTS[:] = [r for r in WEATHER_REPORTS if r.get('timestamp', 0.0) >= cutoff]
    if len(WEATHER_REPORTS) > WEATHER_REPORT_MAX_ENTRIES:
        del WEATHER_REPORTS[:-WEATHER_REPORT_MAX_ENTRIES]


def _record_weather_report(sender_key: str, shortname: str, report_text: str, lat: float, lon: float) -> Dict[str, Any]:
    timestamp = _now()
    map_url = _generate_map_link(lat, lon, shortname) if lat is not None and lon is not None else None
    entry = {
        'timestamp': timestamp,
        'shortname': shortname,
        'text': report_text.strip(),
        'map_url': map_url,
        'lat': lat,
        'lon': lon,
        'node_key': sender_key,
    }
    with WEATHER_REPORT_LOCK:
        WEATHER_REPORTS.append(entry)
        _prune_weather_reports_locked()
        _save_weather_reports_locked()
    return entry



def _get_recent_weather_report(max_age: Optional[int] = None) -> Optional[Dict[str, Any]]:
    if max_age is None:
        max_age = WEATHER_REPORT_RECENT_WINDOW
    cutoff = _now() - max_age
    with WEATHER_REPORT_LOCK:
        fresh = [r for r in WEATHER_REPORTS if r.get('timestamp', 0.0) >= cutoff]
        fresh.sort(key=lambda r: r.get('timestamp', 0.0), reverse=True)
        return fresh[0] if fresh else None


def _collect_recent_weather_reports(max_age: Optional[int] = None) -> List[Dict[str, Any]]:
    if max_age is None:
        max_age = WEATHER_REPORT_RETENTION
    cutoff = _now() - max_age
    with WEATHER_REPORT_LOCK:
        items = [r for r in WEATHER_REPORTS if r.get('timestamp', 0.0) >= cutoff]
    items.sort(key=lambda r: r.get('timestamp', 0.0), reverse=True)
    return items


def _format_weather_timestamp(ts: float) -> str:
    try:
        dt = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone()
        return dt.strftime('%b %d %H:%M')
    except Exception:
        return str(ts)


def _format_relative_age(seconds: float) -> str:
    seconds = abs(int(seconds))
    if seconds < 60:
        return f"{seconds}s"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}m"
    hours = minutes // 60
    if hours < 24:
        return f"{hours}h"
    days = hours // 24
    if days < 7:
        return f"{days}d"
    weeks = days // 7
    return f"{weeks}w"


def _format_weather_reply_lines(entry: Dict[str, Any], lang: str, include_header: bool = True) -> List[str]:
    if not entry:
        return []
    lines: List[str] = []
    if include_header:
        lines.append(translate(lang, 'weather_latest', "🌦️ Latest mesh weather:"))
    report_text = entry.get('text', '')
    if report_text:
        lines.append(report_text)
    time_label = _format_weather_timestamp(entry.get('timestamp', _now()))
    shortname = entry.get('shortname') or 'node'
    byline = translate(lang, 'weather_byline', "{time} • {shortname}", time=time_label, shortname=shortname)
    lines.append(byline)
    map_url = entry.get('map_url')
    if map_url:
        lines.append(f"🔗 {map_url}")
    return lines


def _resolve_sender_position(sender_id: Any, sender_key: Optional[str]) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    lat, lon, tstamp, precision, dilution = get_node_location(sender_id)
    if lat is not None and lon is not None:
        try:
            return float(lat), float(lon), float(tstamp) if isinstance(tstamp, (int, float)) else _now()
        except Exception:
            pass
    if sender_key:
        with LOCATION_HISTORY_LOCK:
            entry = LOCATION_HISTORY.get(sender_key)
        if entry:
            lat_val = entry.get('lat')
            lon_val = entry.get('lon')
            ts_val = entry.get('timestamp', _now())
            try:
                lat_val = float(lat_val)
                lon_val = float(lon_val)
            except Exception:
                return None, None, None
            return lat_val, lon_val, float(ts_val) if isinstance(ts_val, (int, float)) else _now()
    return None, None, None


def _snapshot_all_node_positions() -> None:
    if not interface or not hasattr(interface, "nodes"):
        return
    try:
        node_ids = list((interface.nodes or {}).keys())
    except Exception:
        return
    for node_id in node_ids:
        lat, lon, tstamp, precision, dilution = get_node_location(node_id)
        if lat is None or lon is None:
            continue
        try:
            shortname = get_node_shortname(node_id)
        except Exception:
            shortname = str(node_id)
        sender_key = _safe_sender_key(node_id) or str(node_id)
        timestamp_val = None
        if isinstance(tstamp, (int, float)):
            timestamp_val = float(tstamp)
        else:
            timestamp_val = _now()
        _update_location_history(sender_key, shortname, lat, lon, timestamp_val, precision, dilution)





def _build_locations_kml() -> str:
    _snapshot_all_node_positions()
    points = _collect_recent_locations(exclude_key=None, limit=None)
    weather_points = _collect_recent_weather_reports()
    lines = [
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
        "<kml xmlns=\"http://www.opengis.net/kml/2.2\">",
        "  <Document>",
        "    <name>Mesh Locations</name>",
        "    <Style id=\"precise\">",
        "      <IconStyle>",
        "        <color>ff0000ff</color>",
        "        <scale>1.1</scale>",
        "        <Icon>",
        "          <href>http://maps.google.com/mapfiles/kml/paddle/red-circle.png</href>",
        "        </Icon>",
        "      </IconStyle>",
        "    </Style>",
        "    <Style id=\"dilute\">",
        "      <IconStyle>",
        "        <color>ff00ffff</color>",
        "        <scale>1.1</scale>",
        "        <Icon>",
        "          <href>http://maps.google.com/mapfiles/kml/paddle/ylw-circle.png</href>",
        "        </Icon>",
        "      </IconStyle>",
        "    </Style>",
        "    <Style id=\"weather\">",
        "      <IconStyle>",
        "        <color>ffffa200</color>",
        "        <scale>1.2</scale>",
        "        <Icon>",
        "          <href>http://maps.google.com/mapfiles/kml/paddle/blu-stars.png</href>",
        "        </Icon>",
        "      </IconStyle>",
        "    </Style>",
    ]
    for entry in points:
        quality = entry.get("quality", "precise")
        style_url = "#precise" if quality != "dilute" else "#dilute"
        name = entry.get("shortname", "node")
        lat_val = entry.get("lat")
        lon_val = entry.get("lon")
        time_str = entry.get("time_str", "")
        lines += [
            "    <Placemark>",
            f"      <name>{html.escape(name)}</name>",
            f"      <styleUrl>{style_url}</styleUrl>",
            "      <ExtendedData>",
            f"        <Data name=\"timestamp\"><value>{html.escape(time_str)}</value></Data>",
            "      </ExtendedData>",
            "      <Point>",
            f"        <coordinates>{lon_val},{lat_val},0</coordinates>",
            "      </Point>",
            "    </Placemark>",
        ]
    for entry in weather_points:
        lat_val = entry.get('lat')
        lon_val = entry.get('lon')
        if lat_val is None or lon_val is None:
            continue
        try:
            lat_float = float(lat_val)
            lon_float = float(lon_val)
        except Exception:
            continue
        name = entry.get('shortname') or 'Weather report'
        report_text = entry.get('text', '')
        time_str = _format_weather_timestamp(entry.get('timestamp', _now()))
        summary = f"{time_str} • {report_text}" if report_text else time_str
        lines += [
            "    <Placemark>",
            f"      <name>{html.escape('Weather: ' + name)}</name>",
            "      <styleUrl>#weather</styleUrl>",
            "      <ExtendedData>",
            f"        <Data name=\"summary\"><value>{html.escape(summary)}</value></Data>",
            "      </ExtendedData>",
            "      <Point>",
            f"        <coordinates>{lon_float},{lat_float},0</coordinates>",
            "      </Point>",
            "    </Placemark>",
        ]
    lines += [
        "  </Document>",
        "</kml>",
    ]
    return "\n".join(lines)

def _format_location_reply(sender_id: Any) -> Optional[str]:
    lat, lon, tstamp, precision, dilution = get_node_location(sender_id)
    if lat is None or lon is None:
        return None
    try:
        sn = get_node_shortname(sender_id)
    except Exception:
        sn = str(sender_id)
    sender_key = _safe_sender_key(sender_id) or str(sender_id)
    try:
        lat_val = float(lat)
    except Exception:
        lat_val = lat
    try:
        lon_val = float(lon)
    except Exception:
        lon_val = lon
    lat_str = f"{lat_val:.5f}" if isinstance(lat_val, (int, float)) else str(lat_val)
    lon_str = f"{lon_val:.5f}" if isinstance(lon_val, (int, float)) else str(lon_val)
    timestamp_val: float
    if isinstance(tstamp, (int, float)):
        timestamp_val = float(tstamp)
    else:
        timestamp_val = _now()
    tstr = _format_timestamp_local(timestamp_val)
    current_entry = _update_location_history(
        sender_key,
        sn,
        lat_val,
        lon_val,
        timestamp_val,
        precision,
        dilution,
    )
    url_to_show = current_entry.get("map_url")
    lines: List[str] = []
    lines.append(f"📍 {sn}: {lat_str}, {lon_str} (time: {tstr})")
    if url_to_show:
        lines.append(f"🔗 {url_to_show}")
    _snapshot_all_node_positions()
    recent = _collect_recent_locations(exclude_key=sender_key, limit=5)
    if recent:
        lines.append("🌍 Other nodes (last 24h):")
        for entry in recent:
            quality = entry.get('quality', 'precise')
            lines.append(
                f"- {entry['shortname']}: {entry['lat_str']}, {entry['lon_str']} (time: {entry['time_str']}, {quality})"
            )
            if entry.get('map_url'):
                lines.append(f"  🔗 {entry['map_url']}")
    map_base = _public_base_url()
    lines.append(f"🗺️ All recent: {map_base}/mesh_locations.kml")
    return "\n".join(lines)


def _handle_position_confirmation(
    sender_key: str,
    sender_id: Any,
    text: str,
    is_direct: bool,
    channel_idx: Optional[int],
) -> Optional[PendingReply]:
    entry = PENDING_POSITION_CONFIRM.get(sender_key)
    if not entry:
        return None
    if entry.get("is_direct") != is_direct or entry.get("channel_idx") != channel_idx:
        return None
    trimmed = text.strip().lower()
    normalized_reply = re.sub(r'[^a-záéíóúñ]+$', '', trimmed)
    if normalized_reply in {"y", "yes", "si", "sí"}:
        PENDING_POSITION_CONFIRM.pop(sender_key, None)
        reply_text = _format_location_reply(sender_id)
        if not reply_text:
            return PendingReply("🤖 Sorry, I still don't have a GPS fix for your node.", "position reply")
        return PendingReply(reply_text, "position reply")
    if normalized_reply in {"n", "no"}:
        PENDING_POSITION_CONFIRM.pop(sender_key, None)
        return PendingReply("👍 Location broadcast cancelled.", "position reply")
    return PendingReply("Please reply Yes or No.", "position reply")

def load_archive():
    """Load archive and normalize old entries to include `is_ai` and canonical fields.

    Older archives may not have the `is_ai` flag or consistent `channel_idx`/`direct` fields.
    Normalize in-place so history-building can reliably detect AI replies.
    """
    global messages
    if os.path.exists(ARCHIVE_FILE):
        try:
            with open(ARCHIVE_FILE, "r", encoding="utf-8") as f:
                arr = json.load(f)
            if isinstance(arr, list):
                # Normalize entries
                norm = []
                for m in arr:
                    if not isinstance(m, dict):
                        continue
                    # Ensure expected keys exist
                    if 'direct' not in m:
                        m['direct'] = bool(m.get('direct', False))
                    if 'channel_idx' not in m:
                        m['channel_idx'] = m.get('channel_idx', None)
                    # Detect AI replies conservatively: node string contains AI_NODE_NAME
                    if 'is_ai' not in m:
                        node_field = str(m.get('node', '') or '')
                        m['is_ai'] = (AI_NODE_NAME and AI_NODE_NAME in node_field) or (m.get('node_id') == FORCE_NODE_NUM)
                    norm.append(m)
                with messages_lock:
                    messages.clear()
                    messages.extend(norm)
                print(f"Loaded {len(messages)} messages from archive.")
        except Exception as e:
            print(f"⚠️ Could not load archive {ARCHIVE_FILE}: {e}")
    else:
        print("No archive found; starting fresh.")

def save_archive():
  try:
    with messages_lock:
      snapshot = list(messages)
    with open(ARCHIVE_FILE, "w", encoding="utf-8") as f:
      json.dump(snapshot, f, ensure_ascii=False, indent=2)
  except Exception as e:
    print(f"⚠️ Could not save archive to {ARCHIVE_FILE}: {e}")

def parse_node_id(node_str_or_int):
    if isinstance(node_str_or_int, int):
        return node_str_or_int
    if isinstance(node_str_or_int, str):
        if node_str_or_int == '^all':
            return BROADCAST_ADDR
        if node_str_or_int.lower() in ['!ffffffff', '!ffffffffl']:
            return BROADCAST_ADDR
        if node_str_or_int.startswith('!'):
            hex_part = node_str_or_int[1:]
            try:
                return int(hex_part, 16)
            except ValueError:
                dprint(f"parse_node_id: Unable to parse hex from {node_str_or_int}")
                return None
        try:
            return int(node_str_or_int)
        except ValueError:
            dprint(f"parse_node_id: {node_str_or_int} not recognized as int or hex.")
            return None
    return None

def get_node_fullname(node_id):
    """Return the full (long) name if available, otherwise the short name."""
    if interface and hasattr(interface, "nodes") and node_id in interface.nodes:
        user_dict = interface.nodes[node_id].get("user", {})
        return user_dict.get("longName", user_dict.get("shortName", f"Node_{node_id}"))
    return f"Node_{node_id}"

def get_node_shortname(node_id):
    if interface and hasattr(interface, "nodes") and node_id in interface.nodes:
        user_dict = interface.nodes[node_id].get("user", {})
        return user_dict.get("shortName", f"Node_{node_id}")
    return f"Node_{node_id}"

def _to_int_node(x):
  try:
    if isinstance(x, int):
      return x
    if isinstance(x, str):
      if x.startswith('!'):
        return int(x[1:], 16)
      return int(x)
  except Exception:
    return None
  return None

def same_node_id(a, b):
  """Return True if two node identifiers refer to the same node.
  Accepts int node numbers, '!hex' strings, or other string representations.
  """
  if a == b:
    return True
  ai = _to_int_node(a)
  bi = _to_int_node(b)
  if ai is not None and bi is not None:
    return ai == bi
  # Fallback string compare
  return str(a) == str(b)

def log_message(node_id, text, is_emergency=False, reply_to=None, direct=False, channel_idx=None, force_node=None, is_ai=False):
    """Append a message entry to the in-memory list and persist.

    `force_node` optionally forces the numeric node_id used for lookups (useful for tagging AI replies
    with the device node number when the human-readable node name is used as `node_id`).
    """
    # Determine who to show as the display name and what numeric node_id to store
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    # If force_node is provided (and not None), prefer it as the numeric node id
    stored_node_id = None
    display_id = "WebUI" if node_id == "WebUI" else None
    try:
        if force_node is not None:
            stored_node_id = force_node
            display_id = f"{get_node_shortname(force_node)} ({force_node})"
        else:
            # If node_id looks numeric, keep it; else preserve string id (except WebUI)
            if isinstance(node_id, int):
                stored_node_id = node_id
                display_id = f"{get_node_shortname(node_id)} ({node_id})"
            else:
                # non-numeric node_id (e.g. '!abcd1234'), keep the string for matching in history
                stored_node_id = None if node_id == "WebUI" else node_id
                display_id = f"{get_node_shortname(node_id)} ({node_id})" if node_id != "WebUI" else "WebUI"
    except Exception:
        # Fallback if get_node_shortname raises
        display_id = str(node_id)

    # Flag messages that originate from the AI so they can be included in history
    is_ai_msg = bool(is_ai)
    try:
        if not is_ai_msg:
            if force_node is not None and FORCE_NODE_NUM is not None and force_node == FORCE_NODE_NUM:
                is_ai_msg = True
            elif isinstance(node_id, str) and node_id == AI_NODE_NAME:
                is_ai_msg = True
    except Exception:
        is_ai_msg = is_ai_msg

    entry = {
        "timestamp": timestamp,
        "node": display_id,
        "node_id": stored_node_id,
        "message": text,
        "emergency": is_emergency,
        "reply_to": reply_to,
        "direct": direct,
        "channel_idx": channel_idx,
        "is_ai": is_ai_msg,
    }
    with messages_lock:
        messages.append(entry)
        if MAX_MESSAGE_LOG and MAX_MESSAGE_LOG > 0 and len(messages) > MAX_MESSAGE_LOG:
            # keep only the last MAX_MESSAGE_LOG entries
            del messages[:-MAX_MESSAGE_LOG]
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as logf:
            logf.write(f"{timestamp} | {display_id} | EMERGENCY={is_emergency} | {text}\n")
        _trim_log_file(LOG_FILE)
    except Exception as e:
        print(f"⚠️ Could not write to {LOG_FILE}: {e}")
    save_archive()
    return entry

def split_message(text):
    if not text:
        return []
    return [text[i: i + MAX_CHUNK_SIZE] for i in range(0, len(text), MAX_CHUNK_SIZE)][:MAX_CHUNKS]

def send_broadcast_chunks(interface, text, channelIndex, chunk_delay: Optional[float] = None):
    dprint(f"send_broadcast_chunks: text='{text}', channelIndex={channelIndex}")
    if interface is None:
        print("❌ Cannot send broadcast: interface is None.")
        return
    if not text:
        return
    
    # Check rate limiting to prevent network overload
    if not check_send_rate_limit():
        print("⚠️ Send rate limit exceeded, delaying message...")
        time.sleep(3)  # Brief pause before trying again
        if not check_send_rate_limit():
            print("❌ Still rate limited, dropping message to prevent spam")
            return
    delay = CHUNK_DELAY if chunk_delay is None else max(chunk_delay, 0)
    chunks = split_message(text)
    sent_any = False
    for i, chunk in enumerate(chunks):
        # Retry logic for timeout resilience
        max_retries = 3
        retry_delay = 2
        success = False
        
        for attempt in range(max_retries):
            try:
                interface.sendText(chunk, destinationId=BROADCAST_ADDR, channelIndex=channelIndex, wantAck=False)
                success = True
                # mark last transmit time on success
                try:
                    globals()['last_tx_time'] = _now()
                except Exception:
                    pass
                sent_any = True
                break
            except Exception as e:
                error_msg = str(e).lower()
                if "timed out" in error_msg or "timeout" in error_msg:
                    if attempt < max_retries - 1:
                        clean_log(f"Chunk {i+1} timeout, retrying in {retry_delay}s (attempt {attempt+2}/{max_retries})", "⚠️")
                        time.sleep(retry_delay)
                        retry_delay *= 1.5  # Progressive backoff
                        continue
                    else:
                        print(f"❌ Failed to send chunk {i+1} after {max_retries} attempts: {e}")
                else:
                    print(f"❌ Error sending broadcast chunk: {e}")
                    # Check both errno and winerror for known connection errors
                    error_code = getattr(e, 'errno', None) or getattr(e, 'winerror', None)
                    if error_code in (10053, 10054, 10060):
                        reset_event.set()
                break
        
        if not success:
            print(f"❌ Stopping chunk transmission due to persistent failures")
            break
            
        # Adaptive delay based on success
        if success and i < len(chunks) - 1:  # Don't delay after last chunk
            time.sleep(delay)
    if sent_any:
        clean_log(f"Broadcast to {_channel_display_name(channelIndex)}", "📡")


def send_direct_chunks(interface, text, destinationId, chunk_delay: Optional[float] = None):
    dprint(f"send_direct_chunks: text='{text}', destId={destinationId}")
    dest_display = get_node_shortname(destinationId)
    if not dest_display:
        dest_display = str(destinationId)
    if interface is None:
        print("❌ Cannot send direct message: interface is None.")
        return
    if not text:
        return

    # Check rate limiting to prevent network overload
    if not check_send_rate_limit():
        print("⚠️ Send rate limit exceeded, delaying message...")
        time.sleep(3)
        if not check_send_rate_limit():
            print("❌ Still rate limited, dropping message to prevent spam")
            return

    delay = CHUNK_DELAY if chunk_delay is None else max(chunk_delay, 0)
    chunks = split_message(text)
    if not chunks:
        return

    ephemeral_ok = hasattr(interface, "sendDirectText")

    sent_any = False
    for idx, chunk in enumerate(chunks):
        max_retries = 3
        retry_delay = 2
        success = False

        for attempt in range(max_retries):
            try:
                if ephemeral_ok:
                    interface.sendDirectText(destinationId, chunk, wantAck=False)
                else:
                    interface.sendText(chunk, destinationId=destinationId, wantAck=False)
                try:
                    globals()['last_tx_time'] = _now()
                except Exception:
                    pass
                success = True
                sent_any = True
                break
            except Exception as e:
                error_msg = str(e).lower()
                if "timed out" in error_msg or "timeout" in error_msg:
                    if attempt < max_retries - 1:
                        clean_log(
                            f"Chunk {idx + 1} timeout, retrying in {retry_delay}s (attempt {attempt + 2}/{max_retries})",
                            "⚠️",
                        )
                        time.sleep(retry_delay)
                        retry_delay *= 1.5
                        continue
                    else:
                        print(f"❌ Failed to send chunk {idx + 1} after {max_retries} attempts: {e}")
                else:
                    print(f"❌ Error sending direct chunk: {e}")
                    error_code = getattr(e, 'errno', None) or getattr(e, 'winerror', None)
                    if error_code in (10053, 10054, 10060):
                        reset_event.set()
                break

        if not success:
            print("❌ Stopping chunk transmission due to persistent failures")
            break

        if success and idx < len(chunks) - 1:
            time.sleep(delay)
    if sent_any:
        clean_log(f"Sent to {dest_display}", "📤")

def _format_ai_error(provider: str, detail: str) -> str:
    message = (detail or "unknown issue").strip()
    if len(message) > 160:
        message = message[:157] + "..."
    provider_label = provider.upper()
    return f"⚠️ {provider_label} error: {message}"


def _clear_direct_history(sender_id: Any) -> int:
    with messages_lock:
        before = len(messages)
        sender_dm_ts = {
            m.get('timestamp')
            for m in messages
            if m.get('direct') is True and same_node_id(m.get('node_id'), sender_id)
        }
        messages[:] = [
            m for m in messages
            if not (
                (m.get('direct') is True and same_node_id(m.get('node_id'), sender_id))
                or (m.get('direct') is True and m.get('is_ai') is True and m.get('reply_to') in sender_dm_ts)
            )
        ]
        after = len(messages)
    if before != after:
        save_archive()
    return max(0, before - after)


def _process_wipe_confirmation(sender_id: Any, message: str, is_direct: bool, channel_idx: Optional[int]) -> PendingReply:
    sender_key = _safe_sender_key(sender_id)
    state = PENDING_WIPE_REQUESTS.get(sender_key)
    if not state:
        return PendingReply("Wipe request expired. Start again with /wipe.", "/wipe confirm")

    reply = (message or "").strip().lower()
    if reply in WIPE_CONFIRM_NO:
        PENDING_WIPE_REQUESTS.pop(sender_key, None)
        return PendingReply("👍 Cancelled. Nothing was deleted.", "/wipe confirm")
    if reply not in WIPE_CONFIRM_YES:
        return PendingReply("❓ Please reply with Y or N.", "/wipe confirm")

    action = state.get("action")
    lang = state.get("language")
    mailbox = state.get("mailbox")
    PENDING_WIPE_REQUESTS.pop(sender_key, None)

    if action == "mailbox":
        if not mailbox:
            return PendingReply("Mailbox not specified. Try again with `/wipe mailbox <name>`.", "/wipe confirm")
        clean_log(f"Mailbox wipe confirmed for '{mailbox}'", "🧹")
        return MAIL_MANAGER.handle_wipe(mailbox)

    if action == "chathistory":
        if not is_direct:
            return PendingReply("❌ Chat history wipe only works in direct messages.", "/wipe confirm")
        removed = _clear_direct_history(sender_id)
        clean_log(f"Chat history wipe completed for {get_node_shortname(sender_id)}", "🧹")
        if removed > 0:
            return PendingReply(f"🧹 Cleared {removed} messages from our DM history.", "/wipe confirm")
        return PendingReply("🧹 There was no DM history to clear.", "/wipe confirm")

    if action == "personality":
        sender_key = _safe_sender_key(sender_id)
        _reset_user_personality(sender_key)
        prefs = _get_user_ai_preferences(sender_key)
        persona_id = prefs.get("personality_id")
        persona = AI_PERSONALITY_MAP.get(persona_id or "", {}) if persona_id else {}
        name = persona.get("name") or persona_id or "default"
        emoji = persona.get("emoji") or "🧠"
        clean_log(f"Personality reset for {get_node_shortname(sender_id)}", "🧠")
        return PendingReply(f"{emoji} Personality reset. You're back to {name}.", "/wipe confirm")

    if action == "all":
        if not mailbox:
            return PendingReply("Mailbox required for `/wipe all`. Use `/wipe all <mailbox>`.", "/wipe confirm")
        lines: List[str] = []
        clean_log(f"Full wipe confirmed for '{mailbox}'", "🧹")
        mail_reply = MAIL_MANAGER.handle_wipe(mailbox)
        if isinstance(mail_reply, PendingReply):
            if mail_reply.text:
                lines.append(mail_reply.text)
        removed = _clear_direct_history(sender_id) if is_direct else 0
        if removed > 0:
            lines.append(f"🧹 Cleared {removed} messages from our DM history.")
        else:
            lines.append("🧹 DM history already empty.")
        sender_key = _safe_sender_key(sender_id)
        _reset_user_personality(sender_key)
        prefs = _get_user_ai_preferences(sender_key)
        persona_id = prefs.get("personality_id")
        persona = AI_PERSONALITY_MAP.get(persona_id or "", {}) if persona_id else {}
        name = persona.get("name") or persona_id or "default"
        emoji = persona.get("emoji") or "🧠"
        lines.append(f"{emoji} Personality reset to {name}.")
        aggregated = "\n".join(line for line in lines if line)
        return PendingReply(aggregated, "/wipe confirm")

    return PendingReply("Unknown wipe action. Try /wipe again.", "/wipe confirm")

def build_ollama_history(sender_id=None, is_direct=False, channel_idx=None, thread_root_ts=None, max_chars=OLLAMA_CONTEXT_CHARS):
  """Build a short conversation history string for Ollama based on recent messages.

  - For direct messages: include recent direct exchanges between `sender_id` and the AI node.
  - For channel messages: include recent channel messages for `channel_idx`.
  Limits to the last N messages (configurable via ollama_max_messages, default 20) for performance.
  This means ~10 back-and-forth exchanges to keep the model fast.
  """
  try:
    with messages_lock:
        snapshot = list(messages)
    if not snapshot:
      return ""
    # Collect candidate messages in chronological order
    candidates = []
    if is_direct:
      # Build a per-DM-thread history scoped strictly to the given sender_id.
      # Include only:
      #  - direct human messages from this sender, and
      #  - direct AI replies whose reply_to points to one of those human messages.
      sender_human_ts = set()
      for m in snapshot:
        try:
          if m.get('direct') is True and same_node_id(m.get('node_id'), sender_id):
            candidates.append(m)
            ts = m.get('timestamp')
            if ts:
              sender_human_ts.add(ts)
          elif m.get('direct') is True and m.get('is_ai') is True:
            if m.get('reply_to') in sender_human_ts:
              candidates.append(m)
        except Exception:
          continue
    else:
      # Channel history scoped by channel_idx and optionally by a thread root timestamp.
      if thread_root_ts:
        for m in snapshot:
          try:
            if (m.get('direct') is False) and (m.get('channel_idx') == channel_idx):
              # Include the root human message and any AI replies linked to it
              if m.get('timestamp') == thread_root_ts:
                candidates.append(m)
              elif m.get('is_ai') and m.get('reply_to') == thread_root_ts:
                candidates.append(m)
          except Exception:
            continue
      else:
        # Fallback: include recent messages for the whole channel (legacy behavior)
        for m in snapshot:
          try:
            if (m.get('direct') is False) and (m.get('channel_idx') == channel_idx):
              candidates.append(m)
            elif m.get('is_ai') and (m.get('channel_idx') == channel_idx):
              candidates.append(m)
          except Exception:
            continue
    if not candidates:
      return ""
    
  # Limit to last N messages (configurable exchanges) for performance
    # Take from the end (most recent) of the candidates list
    recent_candidates = candidates[-OLLAMA_MAX_MESSAGES:] if len(candidates) > OLLAMA_MAX_MESSAGES else candidates
    
    # Build output lines in chronological order
    out_lines = []
    for m in recent_candidates:
      who = None
      nid = m.get('node_id')
      if nid is None:
        who = m.get('node', 'Unknown')
      else:
        try:
          who = get_node_shortname(nid)
        except Exception:
          who = str(m.get('node', nid))
      text = str(m.get('message', ''))
      line = f"{who}: {text}"
      out_lines.append(line)
    
    history = "\n".join(out_lines)
    
    # Final character limit check (backup safety)
    if len(history) > max_chars:
      history = history[-max_chars:]
    return history
  except Exception as e:
    dprint(f"build_ollama_history error: {e}")
    return ""


def send_to_ollama(
    user_message,
    sender_id=None,
    is_direct=False,
    channel_idx=None,
    thread_root_ts=None,
    system_prompt: Optional[str] = None,
    *,
    use_history: bool = True,
):
    dprint(f"send_to_ollama: user_message='{user_message}' sender_id={sender_id} is_direct={is_direct} channel={channel_idx}")
    ai_log("Processing message...", "ollama")

    # Normalize text for non-ASCII characters using unidecode
    user_message = unidecode(user_message)
    effective_system_prompt = _sanitize_prompt_text(system_prompt) or _sanitize_prompt_text(SYSTEM_PROMPT) or "You are a helpful assistant responding to mesh network chats."

    # Build optional conversation history
    history = ""
    if use_history and sender_id is not None:
        try:
            history = build_ollama_history(
                sender_id=sender_id,
                is_direct=is_direct,
                channel_idx=channel_idx,
                thread_root_ts=thread_root_ts,
            )
        except Exception as e:
            dprint(f"Warning: failed building history for Ollama: {e}")
            history = ""

    # Compose final prompt: system prompt, optional context, then user message
    if history:
        combined_prompt = f"{effective_system_prompt}\nCONTEXT:\n{history}\n\nUSER: {user_message}\nASSISTANT:"
    else:
        combined_prompt = f"{effective_system_prompt}\nUSER: {user_message}\nASSISTANT:"
    if DEBUG_ENABLED:
        dprint(f"Ollama combined prompt:\n{combined_prompt}")
    else:
        # Show simplified prompt info in clean mode
        prompt_preview = user_message[:50] + "..." if len(user_message) > 50 else user_message
        clean_log(f"Prompt: {prompt_preview}", "💭")

    payload = {
        "prompt": combined_prompt,
        "model": OLLAMA_MODEL,
        "stream": False,  # disable streaming responses
        "options": {
            # Ask Ollama to allocate a larger context window if the model supports it
            "num_ctx": OLLAMA_NUM_CTX,
            # Performance optimizations for faster responses
            "num_predict": 200,    # Limit response length for mesh network
            "temperature": 0.7,    # Slightly less random for more focused responses
            "top_p": 0.9,         # Nucleus sampling for quality vs speed balance
            "top_k": 40,          # Limit vocabulary consideration for speed
            "repeat_penalty": 1.1, # Prevent repetition
            "num_thread": 4,      # Use multiple CPU threads (adjust based on Pi)
        },
    }

    try:
        try:
            globals()['last_ai_request_time'] = _now()
        except Exception:
            pass
        r = None
        attempts = 0
        backoff = 1.5
        while attempts < 2:
            attempts += 1
            try:
                r = requests.post(OLLAMA_URL, json=payload, timeout=OLLAMA_TIMEOUT)
                break
            except Exception as e:
                if attempts >= 2:
                    raise
                time.sleep(backoff)
                backoff *= 1.7
        if r is not None and r.status_code == 200:
            jr = r.json()
            dprint(f"Ollama raw => {jr}")
            # Extract clean response for logging
            resp = jr.get("response")
            if resp:
                # Show clean response instead of technical details
                clean_resp = resp[:100] + "..." if len(resp) > 100 else resp
                ai_log(f"Response: {clean_resp}", "ollama")
            # Ollama may return different fields depending on version; prefer 'response' then 'choices'
            if not resp and isinstance(jr.get("choices"), list) and jr.get("choices"):
                # choices may contain dicts with 'text' or 'content'
                first = jr.get("choices")[0]
                resp = first.get('text') or first.get('content') or resp
            if not resp:
                resp = _format_ai_error("Ollama", "no content returned")
            return (resp or "")[:MAX_RESPONSE_LENGTH]
        else:
            status = getattr(r, 'status_code', 'no response')
            body_preview = r.text[:120] if r is not None and hasattr(r, 'text') else ''
            err = f"status {status}. {body_preview}".strip()
            print(f"⚠️ Ollama error: {err}")
            try:
                globals()['ai_last_error'] = f"Ollama {err}"
                globals()['ai_last_error_time'] = _now()
            except Exception:
                pass
            return _format_ai_error("Ollama", err)
    except Exception as e:
        msg = f"Ollama request failed: {e}"
        print(f"⚠️ {msg}")
        try:
            globals()['ai_last_error'] = msg
            globals()['ai_last_error_time'] = _now()
        except Exception:
            pass
        return _format_ai_error("Ollama", str(e))

def send_to_home_assistant(user_message):
    dprint(f"send_to_home_assistant: user_message='{user_message}'")
    ai_log("Processing message...", "home_assistant")
    if not HOME_ASSISTANT_URL:
        return _format_ai_error("Home Assistant", "endpoint URL not configured")
    headers = {"Content-Type": "application/json"}
    if HOME_ASSISTANT_TOKEN:
        headers["Authorization"] = f"Bearer {HOME_ASSISTANT_TOKEN}"
    payload = {"text": user_message}
    try:
        r = requests.post(HOME_ASSISTANT_URL, json=payload, headers=headers, timeout=HOME_ASSISTANT_TIMEOUT)
        if r.status_code == 200:
            data = r.json()
            dprint(f"HA raw => {data}")
            speech = data.get("response", {}).get("speech", {})
            answer = speech.get("plain", {}).get("speech")
            if answer:
                # Clean response logging
                clean_resp = answer[:100] + "..." if len(answer) > 100 else answer
                ai_log(f"Response: {clean_resp}", "home_assistant")
                return answer[:MAX_RESPONSE_LENGTH]
            return "🤖 [No response from Home Assistant]"
        else:
            err = f"status {r.status_code}. {r.text[:120]}"
            print(f"⚠️ HA error: {err}")
            return _format_ai_error("Home Assistant", err)
    except Exception as e:
        print(f"⚠️ HA request failed: {e}")
        return _format_ai_error("Home Assistant", str(e))

def get_ai_response(prompt, sender_id=None, is_direct=False, channel_idx=None, thread_root_ts=None):
  """Return an AI response using the configured provider (Ollama by default)."""
  system_prompt = build_system_prompt_for_sender(sender_id)
  provider = AI_PROVIDER
  if provider == "home_assistant":
    return send_to_home_assistant(prompt)

  if provider not in {"ollama", "home_assistant"}:
    print(f"⚠️ Unknown AI provider '{provider}', defaulting to Ollama.")

  return send_to_ollama(
      prompt,
      sender_id=sender_id,
      is_direct=is_direct,
      channel_idx=channel_idx,
      thread_root_ts=thread_root_ts,
      system_prompt=system_prompt,
  )
# -----------------------------
# Helper: Validate/Strip PIN (for Home Assistant)
# -----------------------------
def pin_is_valid(text):
    lower = text.lower()
    if "pin=" not in lower:
        return False
    idx = lower.find("pin=") + 4
    candidate = lower[idx:idx+4]
    return (candidate == HOME_ASSISTANT_SECURE_PIN.lower())

def strip_pin(text):
    lower = text.lower()
    idx = lower.find("pin=")
    if idx == -1:
        return text
    return text[:idx].strip() + " " + text[idx+8:].strip()

def route_message_text(user_message, channel_idx):
  if HOME_ASSISTANT_ENABLED and channel_idx == HOME_ASSISTANT_CHANNEL_INDEX:
    info_print("[Info] Routing to Home Assistant channel.")
    if HOME_ASSISTANT_ENABLE_PIN:
      if not pin_is_valid(user_message):
        return "Security code missing/invalid. Format: 'PIN=XXXX your msg'"
      user_message = strip_pin(user_message)
    ha_response = send_to_home_assistant(user_message)
    return ha_response if ha_response else "🤖 [No response from Home Assistant]"
  else:
    info_print(f"[Info] Using default AI provider: {AI_PROVIDER}")
    resp = get_ai_response(user_message, sender_id=None, is_direct=False, channel_idx=channel_idx)
    return resp if resp else "🤖 [No AI response]"

# -----------------------------
# Revised Command Handler (Case-Insensitive)
# -----------------------------
def handle_command(cmd, full_text, sender_id, is_direct=False, channel_idx=None, thread_root_ts=None, language_hint=None):
  # Globals modified by DM-only commands
  global motd_content, SYSTEM_PROMPT, config, MESHTASTIC_KB_WARM_CACHE
  cmd = cmd.lower()
  dprint(f"handle_command => cmd='{cmd}', full_text='{full_text}', sender_id={sender_id}, is_direct={is_direct}, language={language_hint}")
  lang = _normalize_language_code(language_hint) if language_hint else LANGUAGE_FALLBACK
  sender_key = _safe_sender_key(sender_id)
  if cmd != "/wipe" and sender_key:
    PENDING_WIPE_REQUESTS.pop(sender_key, None)
  if cmd == "/about":
    return _cmd_reply(cmd, "MESH-MASTER Off Grid Chat Bot - By: MR-TBOT.com")

  elif cmd in ["/ai", "/bot", "/query", "/data"]:
    user_prompt = full_text[len(cmd):].strip()
    
    # Special handling for DMs: if the command has no content, treat the whole message as a regular AI query
    if is_direct and not user_prompt:
      # User just typed "/ai" or "/query" alone in a DM - treat it as "ai" (regular message)
      user_prompt = cmd[1:]  # Remove the "/" to make it just "ai", "bot", etc.
      info_print(f"[Info] Converting empty {cmd} command in DM to regular AI query: '{user_prompt}'")
    elif not user_prompt:
      # In channels, if no prompt provided, give helpful message
      return _cmd_reply(cmd, f"Please provide a question or prompt after {cmd}. Example: `{cmd} summarize today's mesh activity`")


    if AI_PROVIDER == "home_assistant" and HOME_ASSISTANT_ENABLE_PIN:
      if not pin_is_valid(user_prompt):
        return _cmd_reply(cmd, "Security code missing or invalid. Use 'PIN=XXXX'")
      user_prompt = strip_pin(user_prompt)
    ai_answer = get_ai_response(user_prompt, sender_id=sender_id, is_direct=is_direct, channel_idx=channel_idx, thread_root_ts=thread_root_ts)
    if ai_answer:
      return ai_answer
    return _cmd_reply(cmd, "🤖 [No AI response]")

  elif cmd == "/whereami":
    sender_key = _safe_sender_key(sender_id)
    location_text = _format_location_reply(sender_id)
    if is_direct:
      if location_text:
        return _cmd_reply(cmd, location_text)
      return _cmd_reply(cmd, "🤖 Sorry, I have no GPS fix for your node.")
    else:
      if not location_text:
        return _cmd_reply(cmd, "🤖 Sorry, I have no GPS fix for your node.")
      if sender_key:
        PENDING_POSITION_CONFIRM[sender_key] = {
          "channel_idx": channel_idx,
          "is_direct": is_direct,
        }
      prompt = "⚠️ Are you sure? This will broadcast your position publicly. Send it? Reply Yes or No."
      return PendingReply(prompt, "/whereami confirm")

  elif cmd == "/test":
    sn = get_node_shortname(sender_id)
    return _cmd_reply(cmd, f"Hello {sn}! Received {LOCAL_LOCATION_STRING} by {AI_NODE_NAME}.")

  elif cmd == "/m":
    if not is_direct:
      return _cmd_reply(cmd, "❌ This command can only be used in a direct message.")
    remainder = full_text[len(cmd):].strip()
    if not remainder:
      message = (
        "📬 Mail wizard ready. Send `/mail <mailbox> <your first note>` and I'll create the inbox and guide you."
      )
      return _cmd_reply(cmd, message)
    parts = remainder.split(None, 1)
    mailbox = parts[0].strip().strip("'\"")
    if not mailbox:
      return _cmd_reply(cmd, "Mailbox name cannot be empty.")
    body = ""
    if len(parts) > 1:
      body = parts[1].strip()
      if body.startswith(('"', "'")) and body.endswith(('"', "'")) and len(body) >= 2:
        body = body[1:-1]
      body = body.strip()
    try:
      sender_short = get_node_shortname(sender_id)
    except Exception:
      sender_short = str(sender_id)
    return MAIL_MANAGER.handle_send(
      sender_key=sender_key,
      sender_id=sender_id,
      mailbox=mailbox,
      body=body,
      sender_short=sender_short,
    )

  elif cmd == "/c":
    if not is_direct:
      return _cmd_reply(cmd, "❌ This command can only be used in a direct message.")
    remainder = full_text[len(cmd):].strip()
    if not remainder:
      message = (
        "📭 To check mail, run `/checkmail <mailbox>` (add a question to search). 💡 Need a box? Start with `/mail <mailbox> <message>`."
      )
      return _cmd_reply(cmd, message)
    parts = remainder.split(None, 1)
    if not parts:
      return _cmd_reply(cmd, "Use this by typing: /c mailbox [question]")
    mailbox = parts[0].strip().strip("'\"")
    if not mailbox:
      return _cmd_reply(cmd, "Mailbox name cannot be empty.")
    rest = parts[1].strip() if len(parts) > 1 else ""
    sender_key = _safe_sender_key(sender_id)
    reply = MAIL_MANAGER.handle_check(sender_key, mailbox, rest)
    return reply

  elif cmd == "/wipe":
    if not is_direct:
      return _cmd_reply(cmd, "❌ This command can only be used in a direct message.")
    sender_key = _safe_sender_key(sender_id)
    args = full_text[len(cmd):].strip()
    if not args:
      msg = "🧹 Wipe options: /wipe mailbox <name> | /wipe chathistory | /wipe personality | /wipe all <name>."
      return _cmd_reply(cmd, msg)
    parts = args.split()
    sub = parts[0].lower()
    remainder = " ".join(parts[1:]).strip()
    if sender_key:
      PENDING_WIPE_REQUESTS.pop(sender_key, None)

    if sub in {"mailbox", "mail", "box"}:
      if not remainder:
        return _cmd_reply(cmd, "Use this by typing: /wipe mailbox <name>")
      mailbox = remainder.split()[0].strip("'\"")
      if not mailbox:
        return _cmd_reply(cmd, "Mailbox name cannot be empty.")
      try:
        actor = get_node_shortname(sender_id)
      except Exception:
        actor = str(sender_id)
      if not MAIL_MANAGER.store.mailbox_exists(mailbox):
        return _cmd_reply(cmd, f"📪 Mailbox '{mailbox}' isn't set up yet.")
      clean_log(f"Mailbox wipe requested for '{mailbox}' by {actor}", "🧹")
      if sender_key:
        PENDING_WIPE_REQUESTS[sender_key] = {
          "action": "mailbox",
          "mailbox": mailbox,
          "language": lang,
        }
      return PendingReply(f"🧹 Delete mailbox '{mailbox}' permanently? Reply Y or N.", "/wipe confirm")

    if sub in {"chathistory", "chat", "history"}:
      if sender_key:
        PENDING_WIPE_REQUESTS[sender_key] = {
          "action": "chathistory",
          "language": lang,
        }
      return PendingReply("🧹 Clear our DM history? Reply Y or N.", "/wipe confirm")

    if sub in {"personality", "persona", "tone"}:
      if sender_key:
        PENDING_WIPE_REQUESTS[sender_key] = {
          "action": "personality",
          "language": lang,
        }
      return PendingReply("🧠 Reset your AI personality and custom prompt? Reply Y or N.", "/wipe confirm")

    if sub == "all":
      if not remainder:
        return _cmd_reply(cmd, "Use this by typing: /wipe all <mailbox>")
      mailbox = remainder.split()[0].strip("'\"")
      if not mailbox:
        return _cmd_reply(cmd, "Use this by typing: /wipe all <mailbox>")
      if not MAIL_MANAGER.store.mailbox_exists(mailbox):
        return _cmd_reply(cmd, f"📪 Mailbox '{mailbox}' isn't set up yet.")
      if sender_key:
        PENDING_WIPE_REQUESTS[sender_key] = {
          "action": "all",
          "mailbox": mailbox,
          "language": lang,
        }
      return PendingReply(
        f"🧹 Full wipe plan: mailbox '{mailbox}', DM history, and personality. Reply Y or N to continue.",
        "/wipe confirm",
      )

    return _cmd_reply(cmd, "Unknown wipe option. Use /wipe mailbox <name> | /wipe chathistory | /wipe personality | /wipe all <name>.")

  elif cmd == "/emailhelp":
    guide = (
      "📬 Mesh Mail Quickstart:\n"
      "1) `/mail campsite hey team` makes the inbox.\n"
      "2) Friends reply with `/mail campsite their update`.\n"
      "3) Read with `/checkmail campsite` or search like `/checkmail campsite plans tonight`.\n"
      "4) Cleanup? `/wipe mailbox campsite` will erase it after a Y/N confirm."
    )
    return _cmd_reply(cmd, guide)

  elif cmd == "/meshtastic":
    if not _ensure_meshtastic_kb_loaded():
      return _cmd_reply(cmd, "MeshTastic field guide unavailable. Upload data and try again.")
    query = full_text[len(cmd):].strip()
    if not query:
      return _cmd_reply(cmd, "Use this by typing: /meshtastic <question>")

    query_tokens = set(_kb_tokenize(query))
    matches: Optional[List[Dict[str, Any]]] = None
    context: Optional[str] = None
    cache_used = False
    now_ts = time.time()
    if MESHTASTIC_KB_CACHE_TTL > 0:
      with MESHTASTIC_KB_CACHE_LOCK:
        cache = MESHTASTIC_KB_WARM_CACHE
        if cache.get("context") and cache.get("matches") and cache.get("expires", 0.0) > now_ts:
          cached_tokens = cache.get("tokens") or set()
          if not query_tokens or (cached_tokens and query_tokens.issubset(cached_tokens)):
            matches = cache.get("matches")
            context = cache.get("context")
            cache_used = True
    if matches is None:
      matches = _search_meshtastic_kb(query, max_chunks=5)
      if not matches:
        return _cmd_reply(cmd, "No matching MeshTastic references found. Refine your question and try again.")
      context = _format_meshtastic_context(matches)
      union_tokens: Set[str] = set()
      for chunk in matches:
        union_tokens.update(chunk.get('term_counts', {}).keys())
      if not union_tokens:
        union_tokens = set(_kb_tokenize(context or ""))
      if MESHTASTIC_KB_CACHE_TTL > 0:
        with MESHTASTIC_KB_CACHE_LOCK:
          MESHTASTIC_KB_WARM_CACHE = {
            "expires": time.time() + MESHTASTIC_KB_CACHE_TTL,
            "tokens": union_tokens,
            "context": context,
            "matches": matches,
          }
    elif cache_used:
      clean_log("MeshTastic KB warm cache reused", "♻️")
    seen_titles: List[str] = []
    for chunk in matches:
      title = chunk['title']
      if title not in seen_titles:
        seen_titles.append(title)
    top_titles = ", ".join(seen_titles[:3])
    if top_titles:
      clean_log(f"MeshTastic KB matches: {top_titles}", "📚")
    numbered_context = (
      "MeshTastic reference excerpts (authoritative):\n"
      f"{context}\n\n"
      "Instructions: rely strictly on these excerpts. If the answer is missing, respond exactly `I don't have enough MeshTastic data for that.`"
    )
    system_prompt = MESHTASTIC_KB_SYSTEM_PROMPT
    question_block = f"Question: {query}\nProvide a concise answer. Cite supporting excerpt numbers in square brackets."
    payload = f"{numbered_context}\n\n{question_block}"
    response = send_to_ollama(
      payload,
      sender_id=sender_id,
      is_direct=is_direct,
      channel_idx=channel_idx,
      thread_root_ts=thread_root_ts,
      system_prompt=system_prompt,
      use_history=False,
    )
    if not response:
      return _cmd_reply(cmd, "MeshTastic lookup failed. Try later.")
    return response

  elif cmd == "/biblehelp":
    default_help = (
      "📖 Bible quick tips: `/bible` keeps your place. Jump with `/bible John 3:16`. "
      "Add `in Spanish` or `en ingles` to switch languages. Turn pages with `<1,2>`. Reply `22` "
      "in a DM to auto-scroll 30 verses (12s each)."
    )
    help_text = translate(lang, 'bible_help', default_help)
    return _cmd_reply(cmd, help_text)

  elif cmd == "/choosevibe":
    sender_key = _safe_sender_key(sender_id)
    current = None
    if sender_key:
      prefs = _get_user_ai_preferences(sender_key)
      current = prefs.get("personality_id")
    page_arg = full_text[len(cmd):].strip()
    page = 1
    if page_arg:
      try:
        page = int(page_arg)
      except ValueError:
        return _cmd_reply(cmd, "Use this by typing: /changevibe [page_number]")
    listing = _format_personality_list(current, page)
    return _cmd_reply(cmd, listing)

  elif cmd == "/aivibe":
    sender_key = _safe_sender_key(sender_id)
    prefs = _get_user_ai_preferences(sender_key)
    persona_id = prefs.get("personality_id")
    persona = AI_PERSONALITY_MAP.get(persona_id or "", {}) if persona_id else {}
    name = persona.get("name") or persona_id or "default"
    emoji = persona.get("emoji") or "🧠"
    summary = f"{emoji} Current vibe: {name}. Change with /changevibe."
    return _cmd_reply(cmd, summary.strip())

  elif cmd == "/aipersonality":
    if not is_direct:
      return _cmd_reply(cmd, "❌ Personalities can only be managed in a direct message.")
    sender_key = _safe_sender_key(sender_id) or str(sender_id)
    prefs = _get_user_ai_preferences(sender_key)
    remainder = full_text[len(cmd):].strip()
    if not remainder:
      summary = _format_personality_summary(sender_key)
      return _cmd_reply(cmd, summary)
    parts = remainder.split(None, 1)
    action = parts[0].lower()
    arg = parts[1].strip() if len(parts) > 1 else ""
    if arg.startswith(('"', "'")) and arg.endswith(('"', "'")) and len(arg) >= 2:
      arg = arg[1:-1].strip()

    if action in {"list", "ls", "showall"}:
      page = 1
      if arg:
        try:
          page = int(arg)
        except ValueError:
          return _cmd_reply(cmd, "Use this by typing: /aipersonality list [page_number]")
      listing = _format_personality_list(prefs.get("personality_id"), page)
      return _cmd_reply(cmd, listing)

    if action in {"show", "status", "current"}:
      summary = _format_personality_summary(sender_key)
      return _cmd_reply(cmd, summary)

    if action in {"set", "use", "choose"}:
      if not arg:
        return _cmd_reply(cmd, "Use this by typing: /aipersonality set <name>")
      persona_id = _canonical_personality_id(arg)
      if not persona_id:
        suggestions = difflib.get_close_matches(arg.lower(), list(AI_PERSONALITY_LOOKUP.keys()), n=1, cutoff=0.6)
        if suggestions:
          hinted = AI_PERSONALITY_LOOKUP.get(suggestions[0], suggestions[0])
          return _cmd_reply(cmd, f"❔ Unknown personality '{arg}'. Did you mean '{hinted}'?")
        return _cmd_reply(cmd, f"❔ Unknown personality '{arg}'. Try /aipersonality list for options.")
      _set_user_personality(sender_key, persona_id)
      persona = AI_PERSONALITY_MAP.get(persona_id, {})
      emoji = persona.get("emoji") or "🧠"
      name = persona.get("name") or persona_id
      description = persona.get("description") or ""
      response_lines = [f"{emoji} Personality set to {name} ({persona_id})."]
      if description:
        response_lines.append(description)
      response_lines.append("Tip: add extra guidance with /aipersonality prompt <text>.")
      return _cmd_reply(cmd, "\n".join(response_lines))

    if action in {"prompt", "custom", "append"}:
      if not arg:
        return _cmd_reply(cmd, "Use this by typing: /aipersonality prompt <extra instructions> | prompt clear")
      lowered = arg.lower()
      if lowered in {"clear", "reset", "none", "remove"}:
        _set_user_prompt_override(sender_key, None)
        return _cmd_reply(cmd, "🧹 Custom prompt cleared. You're back to personality defaults.")
      if len(arg) > 800:
        return _cmd_reply(cmd, "⚠️ Custom prompt too long. Limit to 800 characters.")
      _set_user_prompt_override(sender_key, arg)
      return _cmd_reply(cmd, "📝 Custom prompt saved. It will be appended to your responses.")

    if action in {"reset", "default", "restore"}:
      _reset_user_personality(sender_key)
      fallback = _get_user_ai_preferences(sender_key)
      persona_id = fallback.get("personality_id")
      persona = AI_PERSONALITY_MAP.get(persona_id, {}) if persona_id else {}
      emoji = persona.get("emoji") or "🧠"
      name = persona.get("name") or (persona_id or "default mode")
      lines = [f"{emoji} Reset complete. You're back to {name}."]
      lines.append("Custom prompt cleared.")
      return _cmd_reply(cmd, "\n".join(lines))

    return _cmd_reply(cmd, "🤔 Unknown option. Try /aipersonality, /aipersonality list, or /aipersonality help.")

  elif cmd == "/help":
    built_in = [
      "/about", "/menu", "/mail", "/checkmail", "/emailhelp", "/wipe",
      "/query", "/test",
      "/motd", "/meshinfo", "/bible", "/biblehelp", "/chucknorris", "/elpaso", "/blond", "/yomomma",
      "/games", "/hangman", "/wordle", "/wordladder", "/adventure", "/rps", "/coinflip", "/cipher", "/bingo", "/quizbattle", "/morse",
      "/aivibe", "/changevibe", "/aipersonality", "/changemotd", "/changeprompt", "/showprompt", "/printprompt", "/reset"
    ]
    custom_cmds = [c.get("command") for c in commands_config.get("commands", [])]
    help_text = "Commands:\n" + ", ".join(built_in + custom_cmds)
    help_text += "\nNote: /aipersonality, /aivibe, /changevibe, /changeprompt, /changemotd, /showprompt, and /printprompt are DM-only."
    help_text += "\nBrowse highlights with /menu."
    return _cmd_reply(cmd, help_text)

  elif cmd == "/menu":
    menu_text = format_structured_menu("menu", lang)
    return _cmd_reply(cmd, menu_text)

  elif cmd == "/motd":
    motd_msg = translate(lang, 'motd_current', "Current MOTD:\n{motd}", motd=motd_content)
    return _cmd_reply(cmd, motd_msg)

  elif cmd == "/meshinfo":
    report = _format_meshinfo_report(lang)
    return _cmd_reply(cmd, report)

  elif cmd == "/jokes":
    jokes_menu = format_structured_menu("jokes", lang)
    return _cmd_reply(cmd, jokes_menu)

  elif cmd in ("/bibletrivia", "/disastertrivia", "/trivia"):
    category = {
      "/bibletrivia": "bible",
      "/disastertrivia": "disaster",
      "/trivia": "general",
    }[cmd]
    args = full_text[len(cmd):].strip()
    result = handle_trivia_command(cmd, category, args, sender_id, is_direct, channel_idx, lang)
    return _cmd_reply(cmd, result)

  elif cmd in {"/games", "/hangman", "/wordle", "/adventure", "/rps", "/coinflip", "/wordladder", "/cipher", "/bingo", "/quizbattle", "/morse"}:
    if cmd != "/games" and not is_direct:
      msg = translate(lang, 'dm_only', "❌ This command can only be used in a direct message.")
      return _cmd_reply(cmd, msg)
    args = full_text[len(cmd):].strip()
    sender_key = _safe_sender_key(sender_id)
    try:
      sender_short = get_node_shortname(sender_id)
    except Exception:
      sender_short = str(sender_id)
    reply = GAME_MANAGER.handle_command(cmd, args, sender_key, sender_short, lang)
    return reply

  elif cmd in TRAINER_COMMAND_MAP:
    trainer_key = TRAINER_COMMAND_MAP[cmd]
    trainer_text = format_trainer_response(trainer_key, lang)
    return _cmd_reply(cmd, trainer_text)

  elif cmd == "/survival":
    survival_menu = format_structured_menu("survival", lang)
    return _cmd_reply(cmd, survival_menu)

  elif cmd in SURVIVAL_GUIDES:
    guide = format_survival_guide(cmd, lang)
    return _cmd_reply(cmd, guide)

  elif cmd == "/bible":
    remainder = full_text[len(cmd):].strip()
    if not remainder:
      sender_key = _safe_sender_key(sender_id)
      if sender_key:
        progress = _get_user_bible_progress(sender_key, lang)
        span = max(0, int(progress.get('span', 0)))
        preferred_language = progress.get('language') or ('es' if lang == 'es' else 'en')
        book_name = progress.get('book')
        if book_name not in BIBLE_BOOK_ALIAS_MAP.values():
          order = _get_book_order()
          if order:
            book_name = order[0]
          else:
            book_name = 'Genesis'
        chapter = max(1, int(progress.get('chapter', 1)))
        verse_start = max(1, int(progress.get('verse', 1)))
        verse_end = verse_start + span
        include_header = verse_start == 1
        rendered = _render_bible_passage(
          book_name,
          chapter,
          verse_start,
          verse_end,
          preferred_language,
          include_header=include_header,
        )
        if rendered:
          text, info = rendered
          info['span'] = span
          info['book'] = book_name
          _set_bible_nav(sender_key, info, is_direct=is_direct, channel_idx=channel_idx)
          response_text = f"{text}\n<1,2>"
          state_for_shift = {
            'book': book_name,
            'chapter': info['chapter'],
            'verse_start': info['verse_start'],
            'verse_end': info['verse_end'],
            'span': span,
            'language': info['language'],
          }
          next_state = _shift_bible_position(state_for_shift, True)
          if next_state:
            nb, nch, ns, ne, nlang, _ = next_state
            _update_bible_progress(sender_key, nb, nch, ns, nlang, span)
          else:
            _update_bible_progress(sender_key, book_name, info['chapter'], info['verse_end'], info['language'], span)
          return PendingReply(response_text, "/bible command")
      verse_info = _random_bible_verse(lang)
      if verse_info:
        text = verse_info.get('text', '')
        sender_key = _safe_sender_key(sender_id)
        if sender_key:
          _set_bible_nav(sender_key, verse_info, is_direct=is_direct, channel_idx=channel_idx)
        response_text = f"{text}\n<1,2>"
        return PendingReply(response_text, "/bible command")
      return _cmd_reply(cmd, translate(lang, 'bible_missing', "📜 Scripture library unavailable right now."))

    cleaned_ref, language_hint = _extract_bible_language_hint(remainder)
    reference = _parse_bible_reference(cleaned_ref)
    if not reference:
      return _cmd_reply(cmd, "Use this by typing: /bible <book> <chapter>:<verse> or /bible <book> <chapter>:<start>-<end>.")

    book_raw, chapter, verse_start, verse_end = reference
    if verse_start is None or verse_end is None:
      return _cmd_reply(cmd, "Please include a verse number like `/bible John 3:16`.")

    book_name, _ = _resolve_bible_book(book_raw)
    if not book_name:
      suggestions = _suggest_bible_books(book_raw)
      if suggestions:
        hint = ", ".join(_display_book_name(s, lang) for s in suggestions)
        return _cmd_reply(cmd, f"❔ Unknown book '{book_raw}'. Did you mean {hint}?")
      return _cmd_reply(cmd, f"❔ Unknown book '{book_raw}'.")

    range_len = verse_end - verse_start + 1
    if range_len > BIBLE_MAX_VERSE_RANGE:
      return _cmd_reply(cmd, f"⚠️ Keep ranges to {BIBLE_MAX_VERSE_RANGE} verses or fewer.")

    selected_language: Optional[str] = None
    if language_hint == 'es':
      selected_language = 'es'
    elif language_hint == 'en':
      selected_language = 'en'
    else:
      use_spanish = False
      if book_name in BIBLE_RVR_BOOKS:
        if lang == 'es':
          use_spanish = True
        else:
          norm_raw = _normalize_book_key(book_raw)
          norm_spanish = _normalize_book_key(BIBLE_RVR_DISPLAY.get(book_name, ""))
          if norm_raw and norm_spanish and norm_raw == norm_spanish:
            use_spanish = True
      selected_language = 'es' if use_spanish else 'en'
    include_header = (verse_start == 1)
    rendered = _render_bible_passage(book_name, chapter, verse_start, verse_end, selected_language, include_header=include_header)
    if not rendered and selected_language == 'es':
      rendered = _render_bible_passage(book_name, chapter, verse_start, verse_end, 'en', include_header=include_header)
    if rendered:
      text, info = rendered
      sender_key = _safe_sender_key(sender_id)
      if sender_key:
        info['book'] = book_name
        info['span'] = info.get('verse_end', verse_end) - info.get('verse_start', verse_start)
        _set_bible_nav(sender_key, info, is_direct=is_direct, channel_idx=channel_idx)
      response_text = f"{text}\n<1,2>"
      return PendingReply(response_text, "/bible command")
    ref_label = f"{chapter}:{verse_start}" if verse_start == verse_end else f"{chapter}:{verse_start}-{verse_end}"
    display = _display_book_name(book_name, lang)
    return _cmd_reply(cmd, f"⚠️ Couldn't find {display} {ref_label}. Check the reference and try again.")

  elif cmd == "/chucknorris":
    fact = _random_chuck_fact(lang)
    if fact:
        return _cmd_reply(cmd, fact)
    return _cmd_reply(cmd, translate(lang, 'chuck_missing', "🥋 Chuck Norris fact generator is offline."))

  elif cmd == "/elpaso":
    fact = _random_el_paso_fact()
    if fact:
        return _cmd_reply(cmd, fact)
    return _cmd_reply(cmd, "🌵 El Paso fact bank is empty right now.")

  elif cmd == "/blond":
    joke = _random_blond_joke(lang)
    if joke:
        return _cmd_reply(cmd, joke)
    fallback = translate(lang, 'blond_missing', "😅 Blond joke library is empty right now.")
    return _cmd_reply(cmd, fallback)

  elif cmd == "/yomomma":
    joke = _random_yo_momma_joke(lang)
    if joke:
        return _cmd_reply(cmd, joke)
    fallback = translate(lang, 'yomomma_missing', "😅 Yo momma joke library is empty right now.")
    return _cmd_reply(cmd, fallback)

  elif cmd == "/changemotd":
    if not is_direct:
      return _cmd_reply(cmd, translate(lang, 'dm_only', "❌ This command can only be used in a direct message."))
    sender_key = _safe_sender_key(sender_id)
    if sender_key not in AUTHORIZED_ADMINS:
      PENDING_ADMIN_REQUESTS[sender_key] = {
        "command": cmd,
        "full_text": full_text,
        "is_direct": is_direct,
        "channel_idx": channel_idx,
        "thread_root_ts": thread_root_ts,
        "language": lang,
      }
      clean_log(
        f"Admin password required for /changemotd from {get_node_shortname(sender_id)} ({sender_id})",
        "🔐",
        show_always=True,
        rate_limit=False,
      )
      prompt = translate(lang, 'password_prompt', "reply with password")
      return PendingReply(prompt, "admin password")
    # Change the Message of the Day content and persist to MOTD_FILE
    new_motd = full_text[len(cmd):].strip()
    if not new_motd:
      usage = translate(lang, 'changemotd_usage', "Use this by typing: /changemotd Your new MOTD text")
      return _cmd_reply(cmd, usage)
    try:
      # Persist as a JSON string to match existing file format (atomically)
      write_atomic(MOTD_FILE, json.dumps(new_motd))
      # Update in-memory value
      motd_content = new_motd if isinstance(new_motd, str) else str(new_motd)
      info_print(f"[Info] MOTD updated by {get_node_shortname(sender_id)}")
      success = translate(lang, 'changemotd_success', "✅ MOTD updated. Use /motd to view it.")
      return _cmd_reply(cmd, success)
    except Exception as e:
      error_msg = translate(lang, 'changemotd_error', "❌ Failed to update MOTD: {error}", error=e)
      return _cmd_reply(cmd, error_msg)

  elif cmd == "/changeprompt":
    message = "🔒 The core system prompt is fixed. Use /aipersonality set <name> or /aipersonality prompt <text> to adjust tone."
    return _cmd_reply(cmd, message)

  elif cmd in ["/showprompt", "/printprompt"]:
    if not is_direct:
      return _cmd_reply(cmd, translate(lang, 'dm_only', "❌ This command can only be used in a direct message."))
    try:
      info_print(f"[Info] Showing system prompt to {get_node_shortname(sender_id)}")
      msg = translate(lang, 'showprompt_current', "Current system prompt:\n{prompt}", prompt=SYSTEM_PROMPT)
      return _cmd_reply(cmd, msg)
    except Exception as e:
      error_msg = translate(lang, 'showprompt_error', "❌ Failed to show system prompt: {error}", error=e)
      return _cmd_reply(cmd, error_msg)

  elif cmd == "/reset":
    # Clear chat context for either this direct DM thread (sender <-> AI)
    # or for the channel history if invoked in a channel.
    cleared = 0
    with messages_lock:
      before = len(messages)
      if is_direct:
        # Remove only this sender's DM thread: direct human messages from sender
        # and any direct AI replies that have reply_to pointing at those human messages.
        sender_dm_ts = {m.get("timestamp") for m in messages if m.get("direct") is True and same_node_id(m.get("node_id"), sender_id)}
        messages[:] = [
          m for m in messages
          if not (
            (m.get("direct") is True and same_node_id(m.get("node_id"), sender_id))
            or (m.get("direct") is True and m.get("is_ai") is True and m.get("reply_to") in sender_dm_ts)
          )
        ]
      else:
        # Channel reset: remove entries for this channel_idx
        if channel_idx is not None:
          if thread_root_ts:
            # Clear only this thread root and AI replies tied to it
            messages[:] = [
              m for m in messages
              if not (
                (m.get("direct") is False and m.get("channel_idx") == channel_idx and m.get("timestamp") == thread_root_ts)
                or (m.get("direct") is False and m.get("channel_idx") == channel_idx and m.get("is_ai") is True and m.get("reply_to") == thread_root_ts)
              )
            ]
          else:
            # Clear entire channel history
            messages[:] = [
              m for m in messages
              if not (m.get("direct") is False and m.get("channel_idx") == channel_idx)
            ]
        else:
          # Unknown target; do nothing
          pass
      after = len(messages)
      cleared = max(0, before - after)
      save_archive()
    if cleared > 0:
      if is_direct:
        return _cmd_reply(cmd, "I seemed to have had a robot brain fart.., I guess we're starting fresh")
      else:
        return _cmd_reply(cmd, "🧵 Thread/channel context cleared. Starting fresh.")
    else:
      if is_direct:
        return _cmd_reply(cmd, "🧹 Nothing to reset in your direct chat.")
      elif channel_idx is not None:
        ch_name = str(config.get("channel_names", {}).get(str(channel_idx), channel_idx))
        return _cmd_reply(cmd, f"🧹 Nothing to reset for channel {ch_name}.")
      else:
        return _cmd_reply(cmd, "🧹 Nothing to reset (unknown target).")

  for c in commands_config.get("commands", []):
    if c.get("command").lower() == cmd:
      if "ai_prompt" in c:
        user_input = full_text[len(cmd):].strip()
        custom_text = c["ai_prompt"].replace("{user_input}", user_input)
        if AI_PROVIDER == "home_assistant" and HOME_ASSISTANT_ENABLE_PIN:
          if not pin_is_valid(custom_text):
            return _cmd_reply(cmd, "Security code missing or invalid.")
          custom_text = strip_pin(custom_text)
        ans = get_ai_response(custom_text, sender_id=sender_id, is_direct=is_direct, channel_idx=channel_idx, thread_root_ts=thread_root_ts)
        if ans:
          return ans
        return _cmd_reply(cmd, "🤖 [No AI response]")
      elif "response" in c:
        return _cmd_reply(cmd, c["response"])
      return _cmd_reply(cmd, "No configured response for this command.")

  return None

def parse_incoming_text(text, sender_id, is_direct, channel_idx, thread_root_ts=None, check_only=False):
  dprint(f"parse_incoming_text => text='{text}' is_direct={is_direct} channel={channel_idx} check_only={check_only}")
  sender_key = _safe_sender_key(sender_id)
  if not check_only:
    channel_type = "DM" if is_direct else f"Ch{channel_idx}"
    logged_text = text if text is not None else ""
    short = get_node_shortname(sender_id) or str(sender_id)
    clean_log(f"Message from {short} ({channel_type}): {_redact_sensitive(logged_text)}", "📨")
  text = text.strip()
  if not text:
    return None if not check_only else False
  if is_direct and not config.get("reply_in_directs", True):
    return None if not check_only else False
  if (not is_direct) and channel_idx != HOME_ASSISTANT_CHANNEL_INDEX and not config.get("reply_in_channels", True):
    return None if not check_only else False

  if sender_key:
    blocked_until = _antispam_is_blocked(sender_key)
    if blocked_until:
      if not check_only:
        dprint(f"Anti-spam timeout active for {sender_key} until {_antispam_format_time(blocked_until, include_date=True)}")
      return None if not check_only else False

  # sender_key already computed above
  if is_direct and sender_key and sender_key in PENDING_ADMIN_REQUESTS and not text.startswith("/"):
    if check_only:
      return False
    return _process_admin_password(sender_id, text)
  if sender_key and sender_key in PENDING_WEATHER_REPORTS and not text.startswith("/"):
    state = PENDING_WEATHER_REPORTS.get(sender_key) or {}
    if (_now() - state.get('created', 0.0,)) > 600:
      PENDING_WEATHER_REPORTS.pop(sender_key, None)
    else:
      if check_only:
        return False
      stage = state.get('stage', 'confirm')
      lang = state.get('language') or LANGUAGE_FALLBACK
      text_lower = text.strip().lower()
      if stage == 'confirm':
        if text_lower in WIPE_CONFIRM_YES:
          state['stage'] = 'report'
          state['created'] = _now()
          prompt = translate(lang, 'weather_prompt_report', "Great! Reply with a quick weather update (temp, wind, conditions).")
          return PendingReply(prompt, "/weather wizard")
        if text_lower in WIPE_CONFIRM_NO:
          PENDING_WEATHER_REPORTS.pop(sender_key, None)
          decline = translate(lang, 'weather_decline', "No worries. You can try /weather again later.")
          return PendingReply(decline, "/weather wizard")
        reminder = translate(lang, 'weather_confirm_reminder', "Please reply Y to report or N to skip.")
        return PendingReply(reminder, "/weather wizard")
      elif stage == 'report':
        report_text = text.strip()
        if len(report_text) < 5:
          short_msg = translate(lang, 'weather_report_too_short', "Please provide a bit more detail about the weather.")
          return PendingReply(short_msg, "/weather wizard")
        if len(report_text) > 240:
          report_text = report_text[:240].strip()
        lat, lon, pos_ts = _resolve_sender_position(sender_id, sender_key)
        if lat is None or lon is None:
          PENDING_WEATHER_REPORTS.pop(sender_key, None)
          need_fix = translate(lang, 'weather_need_location', "I don't have your latest location yet. Share it with /whereami and try /weather again.")
          return PendingReply(need_fix, "/weather wizard")
        try:
          shortname = get_node_shortname(sender_id)
        except Exception:
          shortname = sender_key or 'node'
        entry = _record_weather_report(sender_key, shortname, report_text, float(lat), float(lon))
        if pos_ts is None:
          pos_ts = _now()
        _update_location_history(sender_key, shortname, lat, lon, pos_ts, None, None)
        PENDING_WEATHER_REPORTS.pop(sender_key, None)
        clean_log(f"Weather report from {shortname}: {report_text}", "🌦️", show_always=True, rate_limit=False)
        thanks = translate(lang, 'weather_saved_header', "🌦️ Weather report saved—thank you!")
        lines = [thanks]
        lines.extend(_format_weather_reply_lines(entry, lang, include_header=False))
        return PendingReply("\n".join(lines), "/weather wizard")
      else:
        PENDING_WEATHER_REPORTS.pop(sender_key, None)
  if sender_key and sender_key in PENDING_POSITION_CONFIRM and not text.startswith("/"):
    if check_only:
      return False
    reply = _handle_position_confirmation(sender_key, sender_id, text, is_direct, channel_idx)
    if reply:
      return reply
  if is_direct and sender_key and MAIL_MANAGER.has_pending_creation(sender_key) and not text.startswith("/"):
    if check_only:
      return False
    return MAIL_MANAGER.handle_creation_response(sender_key, text)
  if is_direct and sender_key and sender_key in PENDING_WIPE_REQUESTS and not text.startswith("/"):
    if check_only:
      return False
    return _process_wipe_confirmation(sender_id, text, is_direct, channel_idx)
  if sender_key and sender_key in PENDING_BIBLE_NAV and not text.startswith("/"):
    trimmed = text.strip()
    if trimmed in {"1", "2"}:
      if check_only:
        return False
      forward = trimmed == "2"
      reply = _handle_bible_navigation(sender_key, forward, is_direct=is_direct, channel_idx=channel_idx)
      if reply:
        return reply
    elif trimmed == "22":
      if check_only:
        return False
      reply = _prepare_bible_autoscroll(sender_key, is_direct, channel_idx)
      if reply:
        return reply
    else:
      if not check_only:
        _clear_bible_nav(sender_key)

  sanitized = text.replace('\u0007', '').strip()
  normalized = sanitized.lower()
  quick_reply = None
  quick_reason = None
  pending_position_info = None
  normalized_no_bell = normalized.replace('🔔', '').strip()
  normalized_no_markers = normalized_no_bell.replace('📍', '').strip()
  location_shared = 'shared their position' in normalized_no_markers
  location_requested_phrase = 'requested a response with your position' in normalized_no_markers
  location_requested = location_shared or location_requested_phrase
  if is_direct:
    if normalized in ALERT_BELL_KEYWORDS or normalized_no_bell in ALERT_BELL_KEYWORDS:
      quick_reply = random.choice(ALERT_BELL_RESPONSES)
      quick_reason = "alert bell"
    elif location_requested:
      location_reply = _format_location_reply(sender_id)
      if location_reply:
        quick_reply = location_reply
        quick_reason = "position reply"
      else:
        quick_reply = "🤖 Sorry, I can't find a GPS fix for your node right now."
        quick_reason = "position request"
  else:
    if location_requested:
      if sender_key:
        pending_position_info = {
          "channel_idx": channel_idx,
          "is_direct": is_direct,
        }
      quick_reply = "⚠️ Are you sure? This will broadcast your position publicly. Send it? Reply Yes or No."
      quick_reason = "position confirm"
  if quick_reply is not None:
    if check_only:
      return False
    if quick_reason == "position confirm" and sender_key and pending_position_info:
      PENDING_POSITION_CONFIRM[sender_key] = pending_position_info
    return PendingReply(quick_reply, quick_reason or "quick reply")

  if text.startswith("/") and sender_key in PENDING_WEATHER_REPORTS:
    PENDING_WEATHER_REPORTS.pop(sender_key, None)

  # Commands (start with /) should be handled and given context
  if text.startswith("/"):
    raw_cmd = text.split()[0]
    canonical_cmd, notice_reason, suggestions, language_hint, alias_append = resolve_command_token(raw_cmd)
    if notice_reason == "unknown" or canonical_cmd is None:
      if check_only:
        return False
      message = format_unknown_command_reply(raw_cmd, suggestions, language_hint)
      return PendingReply(message, "unknown command")
    if check_only:
      # Quick commands like /reset don't need AI processing
      cmd_lower = canonical_cmd.lower()
      if cmd_lower in {"/adventure", "/wordladder"}:
        return True
      if cmd_lower in ["/reset"]:
        return False  # Process immediately, not async
      # Built-in AI commands need async processing
      if cmd_lower in ["/ai", "/bot", "/query", "/data"]:
        return True  # Needs AI processing
      if cmd_lower == "/c":
        remainder = text[len(raw_cmd):].strip()
        if remainder:
          return True
      # Check if it's a custom AI command
      for c in commands_config.get("commands", []):
        cmd_entry = c.get("command")
        if not isinstance(cmd_entry, str):
          continue
        entry_norm = cmd_entry.lower() if cmd_entry.startswith("/") else f"/{cmd_entry.lower()}"
        if entry_norm == canonical_cmd.lower() and "ai_prompt" in c:
          return True  # Needs AI processing
      return False  # Other commands can be processed immediately
    else:
      if sender_key and canonical_cmd.lower() != "/bible":
        _clear_bible_nav(sender_key)
      if canonical_cmd != raw_cmd or alias_append:
        remainder = text[len(raw_cmd):]
        if alias_append:
          remainder = f"{alias_append}{remainder}"
        text = canonical_cmd + remainder
      resp = handle_command(canonical_cmd, text, sender_id, is_direct=is_direct, channel_idx=channel_idx, thread_root_ts=thread_root_ts, language_hint=language_hint)
      if notice_reason:
        resp = annotate_command_response(resp, raw_cmd, canonical_cmd, notice_reason, language_hint)
      return resp

  # Non-command messages: route to AI for direct messages, or Home Assistant if configured for this channel.
  if is_direct:
    if check_only:
      return True  # Direct messages go to AI (needs async processing)
    # Direct messages go to the AI provider and include sender context
    return get_ai_response(text, sender_id=sender_id, is_direct=True, channel_idx=channel_idx, thread_root_ts=thread_root_ts)

  # If Home Assistant integration is enabled and this is the HA channel, route there
  if HOME_ASSISTANT_ENABLED and channel_idx == HOME_ASSISTANT_CHANNEL_INDEX:
    if check_only:
      return True  # HA responses can take time, process async
    return route_message_text(text, channel_idx)

  # Otherwise, no automatic response
  return None if not check_only else False

def on_receive(packet=None, interface=None, **kwargs):
  # Entry marker to confirm callback firing
  try:
    pkt_keys = list(packet.keys()) if isinstance(packet, dict) else type(packet).__name__
  except Exception:
    pkt_keys = 'unknown'
  info_print(f"[CB] on_receive fired. keys={pkt_keys}")
  # Accept packets from generic receive or text-only topic
  decoded = None
  if isinstance(packet, dict):
    decoded = packet.get('decoded')
    if not decoded and 'text' in packet:
      decoded = {'text': packet.get('text'), 'portnum': 'TEXT_MESSAGE_APP'}
  if not decoded and 'text' in kwargs:
    decoded = {'text': kwargs.get('text'), 'portnum': 'TEXT_MESSAGE_APP'}
  if not decoded:
    dprint("No decoded/text in packet => ignoring.")
    return

  # normalize decoded to dict
  if not isinstance(decoded, dict):
    decoded = {'text': str(decoded), 'portnum': 'TEXT_MESSAGE_APP'}
  
  # continue processing
  try:
    globals()['last_rx_time'] = _now()
  except Exception:
    pass
  
  portnum = decoded.get('portnum')
  # Accept string or int for TEXT_MESSAGE_APP (1)
  is_text = False
  try:
    if portnum == 'TEXT_MESSAGE_APP' or portnum == 'TEXT_MESSAGE':
      is_text = True
    elif isinstance(portnum, int) and portnum == 1:
      is_text = True
  except Exception:
    is_text = False
  if not is_text:
    info_print(f"[Info] Ignoring non-text packet: portnum={portnum}")
    return

  try:
    # Prefer decoded text when available
    text = decoded.get('text')
    if text is None:
      payload = decoded.get('payload') or decoded.get('data')
      if isinstance(payload, bytes):
        text = payload.decode('utf-8', errors='replace')
      elif isinstance(payload, str):
        text = payload
      else:
        text = str(payload) if payload is not None else ''
    sender_node = (packet.get('fromId') if isinstance(packet, dict) else None) or (packet.get('from') if isinstance(packet, dict) else None) or kwargs.get('fromId') or kwargs.get('from')
    raw_to = (packet.get('toId') if isinstance(packet, dict) else None) or (packet.get('to') if isinstance(packet, dict) else None) or kwargs.get('toId') or kwargs.get('to')
    to_node_int = parse_node_id(raw_to)
    if to_node_int is None:
      to_node_int = BROADCAST_ADDR
    ch_idx = 0
    if isinstance(packet, dict):
      ch_idx = packet.get('channel') if packet.get('channel') is not None else packet.get('channelIndex', 0)

    # De-dup: if we have seen the same text/from/to/channel very recently, drop it
    rx_key = _rx_make_key(packet, text, ch_idx)
    if _rx_seen_before(rx_key):
      info_print(f"[Info] Duplicate RX suppressed for from={sender_node} ch={ch_idx}: {text}")
      return
    sender_display = get_node_shortname(sender_node) or str(sender_node or '?')
    if to_node_int == BROADCAST_ADDR:
      dest_display = _channel_display_name(ch_idx)
      channel_label = dest_display
    else:
      dest_display = get_node_shortname(raw_to) or get_node_shortname(to_node_int) or str(raw_to or to_node_int)
      channel_label = "DM"
    summary = _truncate_for_log(text)
    info_print(f"📨 {sender_display} → {dest_display} ({channel_label}): {summary}")

    sender_key = _safe_sender_key(sender_node)

    entry = log_message(
        sender_node,
        text,
        direct=(to_node_int != BROADCAST_ADDR),
        channel_idx=(None if to_node_int != BROADCAST_ADDR else ch_idx),
    )

    global lastDMNode, lastChannelIndex
    if to_node_int != BROADCAST_ADDR:
        lastDMNode = sender_node
    else:
        lastChannelIndex = ch_idx

    # Determine our node number
    my_node_num = FORCE_NODE_NUM if FORCE_NODE_NUM is not None else None
    if my_node_num is None:
      if hasattr(interface, "myNode") and interface.myNode:
        my_node_num = interface.myNode.nodeNum
      elif hasattr(interface, "localNode") and interface.localNode:
        my_node_num = interface.localNode.nodeNum

    # Determine whether this is a direct message to us
    if to_node_int == BROADCAST_ADDR:
      is_direct = False
    elif my_node_num is not None and to_node_int == my_node_num:
      is_direct = True
    else:
      is_direct = (my_node_num == to_node_int)

    # Decide on a response based on parsed text and context
    # Compute a thread root for channel messages so multiple /ai commands stick to the same thread.
    thread_root_ts = entry.get('timestamp')
    if not is_direct:
      # For channels, if this is a command, try to anchor to the most recent non-command human message
      # from the same sender in this channel; otherwise, current message is the root.
      t_text = (text or '').strip()
      if t_text.startswith('/'):
        try:
          with messages_lock:
            snapshot = list(messages)
          for m in reversed(snapshot):
            if m.get('direct') is False and m.get('channel_idx') == ch_idx and not m.get('is_ai'):
              # Same sender and not a command message
              if same_node_id(m.get('node_id'), sender_node):
                mt = str(m.get('message') or '')
                if not mt.strip().startswith('/'):
                  thread_root_ts = m.get('timestamp') or thread_root_ts
                  break
        except Exception:
          pass

    # Check if this message should get an AI response
    should_respond = parse_incoming_text(text, sender_node, is_direct, ch_idx, thread_root_ts=thread_root_ts, check_only=True)
    
    if should_respond:
      # Queue the response for async processing instead of blocking here
      info_print(f"🤖 [AsyncAI] Queueing response for {sender_node}: {text[:50]}...")
      task = (text, sender_node, is_direct, ch_idx, thread_root_ts, interface)
      try:
        response_queue.put(task, block=False)
        info_print(f"📬 [AsyncAI] Queued (queue size: {response_queue.qsize()}/{RESPONSE_QUEUE_MAXSIZE})")
      except queue.Full:
        info_print(f"🚨 [AsyncAI] Queue full ({response_queue.qsize()}/{RESPONSE_QUEUE_MAXSIZE}), waiting for a free slot...")
        try:
          response_queue.put(task, block=True, timeout=5)
          info_print(f"📬 [AsyncAI] Queued after wait (queue size: {response_queue.qsize()}/{RESPONSE_QUEUE_MAXSIZE})")
        except queue.Full:
          info_print(f"🚨 [AsyncAI] Queue still full after wait; processing immediately")
          resp = parse_incoming_text(text, sender_node, is_direct, ch_idx, thread_root_ts=thread_root_ts)
          if resp:
            pending = resp if isinstance(resp, PendingReply) else None
            response_text = pending.text if pending else resp
            if response_text:
              if pending:
                _command_delay(pending.reason)
              if is_direct:
                send_direct_chunks(interface, response_text, sender_node)
              else:
                send_broadcast_chunks(interface, response_text, ch_idx)
              _antispam_after_response(sender_key, sender_node, interface)
              _process_bible_autoscroll_request(sender_key, sender_node, interface)
    else:
      # Non-AI messages (e.g., simple commands) can be processed immediately
      resp = parse_incoming_text(text, sender_node, is_direct, ch_idx, thread_root_ts=thread_root_ts)
      if resp:
        pending = resp if isinstance(resp, PendingReply) else None
        response_text = pending.text if pending else resp
        if response_text:
          target_name = get_node_shortname(sender_node) or str(sender_node)
          summary = _truncate_for_log(response_text)
          clean_log(f"Ollama → {target_name} (0.0s): {summary}", "🦙", show_always=True, rate_limit=False)
          if pending:
            _command_delay(pending.reason)
          if is_direct:
            send_direct_chunks(interface, response_text, sender_node)
          else:
            send_broadcast_chunks(interface, response_text, ch_idx)
          _antispam_after_response(sender_key, sender_node, interface)
          _process_bible_autoscroll_request(sender_key, sender_node, interface)

  except OSError as e:
    error_code = getattr(e, 'errno', None) or getattr(e, 'winerror', None)
    print(f"⚠️ OSError detected in on_receive: {e} (error code: {error_code})")
    if error_code in (10053, 10054, 10060):
      print("⚠️ Connection error detected. Restarting interface...")
      global connection_status
      connection_status = "Disconnected"
      reset_event.set()
    # Instead of re-raising, simply return to prevent thread crash
    return
  except Exception as e:
    print(f"⚠️ Unexpected error in on_receive: {e}")
    try:
      import traceback as _tb
      _tb.print_exc()
    except Exception:
      pass
    return

@app.route("/messages", methods=["GET"])
def get_messages_api():
  dprint("GET /messages => returning current messages")
  with messages_lock:
    snapshot = list(messages)
  return jsonify(snapshot)

@app.route("/nodes", methods=["GET"])
def get_nodes_api():
    node_list = []
    if interface and hasattr(interface, "nodes"):
        for nid in interface.nodes:
            sn = get_node_shortname(nid)
            ln = get_node_fullname(nid)
            node_list.append({
                "id": nid,
                "shortName": sn,
                "longName": ln
            })
    return jsonify(node_list)

@app.route("/connection_status", methods=["GET"], endpoint="connection_status_info")
def connection_status_info():
    return jsonify({"status": connection_status, "error": last_error_message})

@app.route("/logs_stream")
def logs_stream():
  def generate():
    last_index = 0
    while True:
      # apply your noise filter
      visible = [
        line for line in script_logs
        if (_viewer_should_show(line) if _viewer_filter_enabled else True)
      ]
      # send only the new lines
      if last_index < len(visible):
        for raw_line in visible[last_index:]:
          html_line = _render_log_line_html(raw_line)
          yield f"data: {html_line}\n\n"
        last_index = len(visible)
      time.sleep(0.5)

  headers = {
    "Cache-Control": "no-cache, no-store, must-revalidate",
    "Pragma": "no-cache", 
    "Expires": "0",
    "X-Accel-Buffering": "no",   # for nginx, disables proxy buffering
    "Connection": "keep-alive",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Cache-Control"
  }
  return Response(
    stream_with_context(generate()),
    headers=headers,
    mimetype="text/event-stream"
  )


_LOG_URL_PATTERN = re.compile(r"(https?://\S+)")

LOG_EMOJI_SVG = {
    "📨": "1f4e8.svg",
    "📬": "1f4ec.svg",
    "📪": "1f4ea.svg",
    "📤": "1f4e4.svg",
    "📦": "1f4e6.svg",
    "📡": "1f4e1.svg",
    "📚": "1f4da.svg",
    "📥": "1f4e5.svg",
    "📤": "1f4e4.svg",
    "⚡": "26a1.svg",
    "⚠": "26a0.svg",
    "⚠️": "26a0.svg",
    "🛡": "1f6e1.svg",
    "🛡️": "1f6e1.svg",
    "🟢": "1f7e2.svg",
    "🟡": "1f7e1.svg",
    "🟠": "1f7e0.svg",
    "💚": "1f49a.svg",
    "💓": "1f493.svg",
    "🔗": "1f517.svg",
    "🔐": "1f510.svg",
    "🚀": "1f680.svg",
    "🧠": "1f9e0.svg",
    "🧭": "1f9ed.svg",
    "🖥": "1f5a5.svg",
    "🖥️": "1f5a5.svg",
    "📝": "1f4dd.svg",
    "🎉": "1f389.svg",
    "ℹ": "2139.svg",
    "ℹ️": "2139.svg",
}

_LOG_EMOJI_KEYS = sorted(LOG_EMOJI_SVG.keys(), key=len, reverse=True)
_LOG_EMOJI_PATTERN = re.compile("|".join(re.escape(k) for k in _LOG_EMOJI_KEYS)) if LOG_EMOJI_SVG else None


def _twemoji_img(emoji: str) -> str:
    filename = LOG_EMOJI_SVG.get(emoji)
    if not filename:
        return html.escape(emoji)
    return f'<img class="emoji" src="/static/twemoji/svg/{filename}" alt="{html.escape(emoji)}">'


def _inject_emoji_html(escaped_text: str) -> str:
    if not _LOG_EMOJI_PATTERN:
        return escaped_text
    return _LOG_EMOJI_PATTERN.sub(lambda m: _twemoji_img(m.group(0)), escaped_text)


def _render_log_line_html(line: str) -> str:
  normalized = _normalize_log_timestamp(line)
  css_class = _classify_log_line(normalized)
  safe = html.escape(normalized, quote=False)
  safe = _LOG_URL_PATTERN.sub(lambda m: f'<a href="{m.group(1)}" target="_blank" rel="noopener">{m.group(1)}</a>', safe)
  safe = _inject_emoji_html(safe)
  classes = "log-line"
  if css_class:
    classes += f" {css_class}"
  return f'<span class="{classes}">{safe}</span>'


@app.route("/mesh_locations.kml")
def mesh_locations_kml():
  kml = _build_locations_kml()
  headers = {
      "Content-Type": "application/vnd.google-earth.kml+xml",
      "Cache-Control": "no-cache, no-store, must-revalidate",
  }
  return Response(kml, headers=headers)


@app.route("/logs", methods=["GET"])
def logs():
    uptime = datetime.now(timezone.utc) - server_start_time
    uptime_str = _humanize_uptime(uptime)
    now_local_dt = datetime.now().astimezone()
    rounded_now = (now_local_dt + timedelta(seconds=30)).replace(second=0, microsecond=0)
    now_local = rounded_now.strftime("%b %d %H:%M %Z")

    visible = [
        line for line in script_logs
        if (_viewer_should_show(line) if _viewer_filter_enabled else True)
    ]
    log_text = "".join(_render_log_line_html(line) for line in visible)

    lang_for_proverbs = LANGUAGE_FALLBACK
    proverb_lang_key = _proverb_language_key(lang_for_proverbs)
    initial_proverb = _next_proverb(lang_for_proverbs)
    proverb_index_js = PROVERB_INDEX_TRACKER.get(proverb_lang_key, 0)
    proverb_list = _load_proverbs(lang_for_proverbs)
    proverbs_json = json.dumps(proverb_list, ensure_ascii=False)
    initial_proverb_html = _inject_emoji_html(html.escape(initial_proverb, quote=False))
    proverb_interval_ms = 60000
    fade_duration_ms = 1200
    emoji_html_map_json = json.dumps({emoji: _twemoji_img(emoji) for emoji in LOG_EMOJI_SVG}, ensure_ascii=False)
    emoji_keys_json = json.dumps(_LOG_EMOJI_KEYS, ensure_ascii=False)

    html_page = f"""<html>
  <head>
    <title>MESH-MASTER Logs - Smooth Stream</title>
    <style>
      body {{
        background:#000;
        color:#fff;
        font-family: 'Segoe UI', 'Noto Sans', 'Liberation Sans', 'Helvetica Neue', Arial, 'Twemoji Mozilla', 'Noto Color Emoji', 'Segoe UI Emoji', 'Apple Color Emoji', sans-serif;
        padding:20px;
        margin:0;
        overflow-x:hidden;
      }}
      body::before {{
        content:'';
        position:fixed;
        top:0; left:0; right:0; bottom:0;
        background: radial-gradient(circle at 20% 20%, rgba(255,255,255,0.15) 0%, rgba(255,255,255,0) 60%),
                    radial-gradient(circle at 80% 30%, rgba(120,200,255,0.125) 0%, rgba(0,0,0,0) 55%),
                    radial-gradient(circle at 50% 80%, rgba(255,180,255,0.1) 0%, rgba(0,0,0,0) 60%),
                    #000;
        background-size:400px 400px, 600px 600px, 500px 500px;
        animation: starfield 240s linear infinite;
        z-index:-2;
      }}
      @keyframes starfield {{
        from {{ background-position: 0px 0px, 0px 0px, 0px 0px; }}
        to {{ background-position: -800px -200px, 600px -400px, -400px 600px; }}
      }}
      pre {{
        white-space: pre-wrap;
        word-break: break-word;
        margin:0;
        padding-bottom:100px;
      }}
      .header {{
        position:fixed;
        top:0;
        left:0;
        right:0;
        background:rgba(0,0,0,0.75);
        padding:14px 20px 12px;
        border-bottom:1px solid #333;
        z-index:1000;
        text-align:center;
      }}
      .content {{
        margin-top:130px;
        text-align:center;
      }}
      .logbox {{
        height: calc(100vh - 220px);
        overflow-y: auto;
        white-space: pre-wrap;
        word-break: break-word;
        margin:0;
        padding-bottom:100px;
        transform: rotate(180deg);
      }}
      .footer-status {{
        position:fixed;
        bottom:16px;
        left:50%;
        transform:translateX(-50%);
        display:flex;
        flex-direction:column;
        align-items:center;
        gap:6px;
        font-size:12px;
        color:#fff;
      }}
      .scroll-indicator {{
        display:flex;
        align-items:center;
        gap:6px;
        background:#333;
        padding:4px 10px;
        border-radius:6px;
        min-width:150px;
        justify-content:center;
      }}
      .scroll-indicator .arrow {{
        font-size:14px;
        opacity:0;
      }}
      .scroll-indicator.on .arrow {{
        opacity:1;
        animation:pulse 1.2s infinite;
      }}
      .scroll-indicator .label {{
        text-transform:uppercase;
        letter-spacing:0.08em;
      }}
      .status-meta {{
        background:rgba(0,0,0,0.55);
        padding:3px 8px;
        border-radius:6px;
        display:flex;
        gap:10px;
      }}
      .status-meta span {{
        color:#aaa;
      }}
      @keyframes slideup {{
        from {{ transform: translateY(18px); opacity:0; }}
        to {{ transform: translateY(0); opacity:1; }}
      }}
      @keyframes pulse {{
        0% {{ transform:translateY(0); opacity:0.2; }}
        50% {{ transform:translateY(3px); opacity:1; }}
        100% {{ transform:translateY(0); opacity:0.2; }}
      }}
      .proverb-box {{
        display:inline-block;
        max-width:80%;
        padding:6px 14px;
        margin-top:18px;
        background:rgba(0,0,0,0.55);
        border-radius:8px;
        text-align:center;
        margin-left:auto;
        margin-right:auto;
      }}
      .headline-text {{
        display:block;
        font-family: 'Playfair Display', 'Georgia', 'Cambria', 'Times New Roman', serif, 'Twemoji Mozilla', 'Noto Color Emoji', 'Segoe UI Emoji', 'Apple Color Emoji';
        font-size:0.88rem;
        line-height:1.3;
        letter-spacing:0.15px;
        color:#c0b9ac;
        opacity:1;
        transition: opacity 1.2s ease-in-out;
      }}
      .headline-text.fade-out {{
        opacity:0;
      }}
      .headline-text.highlight {{
        color:#cca961;
      }}
      .log-line {{ display:block; margin:0; }}
      .log-line.animate-up {{ animation: slideup 0.45s ease-out; }}
      .emoji {{
        width:1em;
        height:1em;
        margin:0 0.05em;
        vertical-align:-0.1em;
      }}
      .log-line.incoming {{ color:#D7BA7D; }}
      .log-line.outgoing {{ color:#6A9955; }}
      .log-line.clock {{ color:#2472C8; }}
      .log-line.error {{ color:#F14C4C; font-weight:700; }}
      .header .clock {{ color:#2472C8; font-weight:bold; }}
      .log-line a {{ color:#90caf9; }}
    </style>
  </head>
  <body>
    <div class="header">
      <div class="proverb-box">
        <span class="headline-text" id="headlineText">{initial_proverb_html}</span>
      </div>
    </div>
    <div class="content">
      <div id="logbox" class="logbox">{log_text}</div>
    </div>
    <div class="footer-status">
      <div class="scroll-indicator on" id="scrollStatus">
        <span class="arrow">↓</span>
        <span class="label" id="scrollLabel">Auto-scroll ON</span>
      </div>
      <div class="status-meta">
        <span id="statusTime">{now_local}</span>
        <span id="statusUptime">{uptime_str}</span>
        <span id="statusRestarts">Restarts: {restart_count}</span>
      </div>
    </div>
    <script>
      const EMOJI_HTML_MAP = {emoji_html_map_json};
      const EMOJI_KEYS = Object.keys(EMOJI_HTML_MAP).sort((a, b) => b.length - a.length);

      const PROVERBS = {proverbs_json};
      let proverbIndex = {proverb_index_js};
      const PROVERB_INTERVAL = {proverb_interval_ms};
      const FADE_DURATION = {fade_duration_ms};
      const headlineText = document.getElementById('headlineText');
      let highlightTimer = null;
      let highlightActive = false;
      let proverbRotationTimer = null;
      let fadeTimeout = null;

      function escapeForHTML(text) {{
        if (!text) return '';
        return text
          .replace(/&/g, '&amp;')
          .replace(/</g, '&lt;')
          .replace(/>/g, '&gt;')
          .replace(/"/g, '&quot;')
          .replace(/'/g, '&#39;');
      }}

      function renderWithEmoji(text) {{
        let escaped = escapeForHTML(text || '');
        if (!EMOJI_KEYS.length) return escaped;
        for (const emoji of EMOJI_KEYS) {{
          const html = EMOJI_HTML_MAP[emoji];
          if (!html) continue;
          escaped = escaped.split(emoji).join(html);
        }}
        return escaped;
      }}

      function nextProverb() {{
        if (!PROVERBS.length) {{
          return 'Proverbs unavailable.';
        }}
        const text = PROVERBS[proverbIndex % PROVERBS.length];
        proverbIndex = (proverbIndex + 1) % PROVERBS.length;
        return text;
      }}

      function swapHeadline(text, {{ highlight = false, immediate = false }} = {{}}) {{
        if (fadeTimeout) {{
          clearTimeout(fadeTimeout);
          fadeTimeout = null;
        }}

        const html = renderWithEmoji(text || '');
        if (immediate) {{
          headlineText.innerHTML = html;
          headlineText.classList.remove('fade-out', 'highlight');
          if (highlight) {{
            headlineText.classList.add('highlight');
          }}
          return;
        }}

        headlineText.classList.remove('fade-out');
        void headlineText.offsetWidth;
        headlineText.classList.add('fade-out');

        fadeTimeout = setTimeout(() => {{
          headlineText.innerHTML = html;
          headlineText.classList.remove('highlight');
          if (highlight) {{
            headlineText.classList.add('highlight');
          }}
          headlineText.classList.remove('fade-out');
          fadeTimeout = null;
        }}, FADE_DURATION);
      }}

      function setHeadlineToProverb(force=false) {{
        if (highlightActive && !force) return;
        const proverb = nextProverb();
        highlightActive = false;
        swapHeadline(proverb);
      }}

      function showMessageHeadline(text) {{
        highlightActive = true;
        swapHeadline(text, {{ highlight: true }});
        if (highlightTimer) {{ clearTimeout(highlightTimer); }}
        highlightTimer = setTimeout(() => {{
          highlightActive = false;
          setHeadlineToProverb(true);
          highlightTimer = null;
        }}, PROVERB_INTERVAL * 2);
      }}

      function startProverbRotation() {{
        if (proverbRotationTimer) {{ clearInterval(proverbRotationTimer); }}
        proverbRotationTimer = setInterval(() => {{
          setHeadlineToProverb();
        }}, PROVERB_INTERVAL);
      }}

      window.addEventListener('load', () => {{
        swapHeadline(headlineText.textContent, {{ immediate: true }});
        startProverbRotation();
      }});

      function appendLogLine(html) {{
        const temp = document.createElement('div');
        temp.innerHTML = html;
        const span = temp.firstElementChild || temp;
        const textContent = (span.textContent || '').trim();

        if (textContent.includes('📨')) {{
          showMessageHeadline(textContent);
        }} else if (textContent.includes('📤') || textContent.includes('📡')) {{
          showMessageHeadline(textContent);
        }}

        logbox.appendChild(span);
        span.classList.add('animate-up');
        setTimeout(() => span.classList.remove('animate-up'), 600);
      }}


      let autoScroll = true;
      let isUserScrolling = false;
      let scrollTimeout;
      const logbox = document.getElementById('logbox');
      const scrollStatus = document.getElementById('scrollStatus');
      const scrollLabel = document.getElementById('scrollLabel');

      function updateScrollLabel(text, arrowOn) {{
        scrollLabel.textContent = text;
        if (arrowOn) {{
          scrollStatus.classList.add('on');
        }} else {{
          scrollStatus.classList.remove('on');
        }}
      }}

      updateScrollLabel('Auto-scroll ON', true);

      function smoothScrollToBottom() {{
        if (autoScroll && !isUserScrolling) {{
          logbox.scrollTo({{
            top: logbox.scrollHeight,
            behavior: 'smooth'
          }});
        }}
      }}

      logbox.addEventListener('scroll', () => {{
        isUserScrolling = true;
        clearTimeout(scrollTimeout);
                const nearBottom = logbox.scrollHeight - logbox.scrollTop <= logbox.clientHeight + 10;
                if (nearBottom) {{
          autoScroll = true;
          updateScrollLabel('Auto-scroll ON', true);
        }} else {{
          autoScroll = false;
          updateScrollLabel('Auto-scroll OFF', false);
        }}
        scrollTimeout = setTimeout(() => {{
          isUserScrolling = false;
        }}, 1000);
      }});

      let eventSource;
      let reconnectAttempts = 0;
      let maxReconnectAttempts = 5;
      let lastMessageTime = Date.now();

      function createEventSource() {{
        const url = `/logs_stream?v=${Date.now()}`;
        eventSource = new EventSource(url);

        eventSource.onmessage = function(event) {{
          if (event.data.includes('heartbeat') || event.data.includes('keepalive')) {{
            lastMessageTime = Date.now();
            return;
          }}

          appendLogLine(event.data);
          smoothScrollToBottom();
          lastMessageTime = Date.now();
          reconnectAttempts = 0;
        }};

        eventSource.onopen = function(event) {{
          reconnectAttempts = 0;
          lastMessageTime = Date.now();
          updateScrollLabel(autoScroll ? 'Auto-scroll ON' : 'Auto-scroll OFF', autoScroll);
        }};

        eventSource.onerror = function(event) {{
          eventSource.close();
          if (reconnectAttempts < maxReconnectAttempts) {{
            const delay = Math.min(5000, 1000 * Math.pow(2, reconnectAttempts));
            reconnectAttempts += 1;
            setTimeout(createEventSource, delay);
          }} else {{
            updateScrollLabel('Log stream offline', false);
          }}
        }};
      }}

      createEventSource();

      function smoothScrollCheck() {{
        if (autoScroll) {{
          const now = Date.now();
          const elapsed = now - lastMessageTime;
          if (elapsed > 30000) {{
            updateScrollLabel('Waiting for new logs…', false);
          }} else {{
            updateScrollLabel('Auto-scroll ON', true);
          }}
        }}
        requestAnimationFrame(smoothScrollCheck);
      }}
      smoothScrollCheck();
    </script>
  </body>
  <script>
    const logbox = document.getElementById('logbox');
    logbox.scrollTop = logbox.scrollHeight;
  </script>
</html>"""

    return html_page

# -----------------------------
# Web Routes
# -----------------------------
@app.route("/", methods=["GET"])
def root():
  # Redirect to dashboard for convenience
  return redirect("/dashboard")

@app.route("/health", methods=["GET"])
def health():
  # Simple health endpoint for status checks
  return jsonify({"ok": connection_status == "Connected", "status": connection_status})

@app.route("/dashboard", methods=["GET"])
def dashboard():
    channel_names = config.get("channel_names", {})
    channel_names_json = json.dumps(channel_names)

    # Prepare node GPS and beacon info for JS
    node_gps_info = {}
    if interface and hasattr(interface, "nodes"):
        for nid, ninfo in interface.nodes.items():
            pos = ninfo.get("position", {})
            lat = pos.get("latitude")
            lon = pos.get("longitude")
            tstamp = pos.get("time")
            # Try all possible hop keys, fallback to None
            hops = (
                ninfo.get("hopLimit")
                or ninfo.get("hop_count")
                or ninfo.get("hopCount")
                or ninfo.get("numHops")
                or ninfo.get("num_hops")
                or ninfo.get("hops")
                or None
            )
            # Convert tstamp (epoch) to readable UTC if present
            if tstamp:
                try:
                    dt = datetime.fromtimestamp(tstamp, timezone.utc)
                    tstr = dt.strftime("%Y-%m-%d %H:%M:%S UTC")
                except Exception:
                    tstr = str(tstamp)
            else:
                tstr = None
            node_gps_info[str(nid)] = {
                "lat": lat,
                "lon": lon,
                "beacon_time": tstr,
                "hops": hops,
            }
    node_gps_info_json = json.dumps(node_gps_info)

    # Get connected node's GPS for distance calculation
    my_lat, my_lon, _, _, _ = get_node_location(interface.myNode.nodeNum) if interface and hasattr(interface, "myNode") and interface.myNode else (None, None, None, None, None)
    my_gps_json = json.dumps({"lat": my_lat, "lon": my_lon})

    html = """
<html>
<head>
  <title>MESH-MASTER Dashboard</title>
  <style>
    :root { --theme-color: #ffa500; }
    body { background: #000; color: #fff; font-family: Arial, sans-serif; margin: 0; padding-top: 120px; transition: filter 0.5s linear; }
    #connectionStatus { position: fixed; top: 0; left: 0; width: 100%; z-index: 350; text-align: center; padding: 0; font-size: 14px; font-weight: bold; display: block; }
    .header-buttons { position: fixed; top: 0; right: 0; z-index: 400; }
    .header-buttons a { background: var(--theme-color); color: #000; padding: 8px 12px; margin: 5px; text-decoration: none; border-radius: 4px; font-weight: bold; }
    #ticker-container { position: fixed; top: 20px; left: 0; width: 100vw; z-index: 300; height: 50px; display: flex; align-items: center; justify-content: center; pointer-events: none; }
    #ticker { background: #111; color: var(--theme-color); white-space: nowrap; overflow: hidden; width: 100vw; min-width: 100vw; max-width: 100vw; padding: 5px 0; font-size: 36px; display: none; position: relative; border-bottom: 2px solid var(--theme-color); min-height: 50px; pointer-events: auto; }
    #ticker p { display: inline-block; margin: 0; animation: tickerScroll 30s linear infinite; vertical-align: middle; min-width: 100vw; }
    #ticker .dismiss-btn { position: absolute; right: 20px; top: 50%; transform: translateY(-50%); font-size: 18px; background: #222; color: #fff; border: 1px solid var(--theme-color); border-radius: 4px; cursor: pointer; padding: 2px 10px; z-index: 10; }
    @keyframes tickerScroll { 0% { transform: translateX(100%); } 100% { transform: translateX(-100%); } }
    #sendForm { margin: 20px; padding: 20px; background: #111; border: 2px solid var(--theme-color); border-radius: 10px; }
    .three-col { display: flex; flex-direction: row; gap: 20px; margin: 20px; height: calc(100vh - 220px); }
    .three-col .col:nth-child(1), .three-col .col:nth-child(3) { flex: 2; overflow-y: auto; }
    .three-col .col:nth-child(2) { flex: 1; overflow-y: auto; }
    .lcars-panel { background: #111; padding: 20px; border: 2px solid var(--theme-color); border-radius: 10px; }
    .lcars-panel h2 { color: var(--theme-color); margin-top: 0; }
    .message { border: 1px solid var(--theme-color); border-radius: 4px; margin: 5px; padding: 5px; }
    .message.outgoing { background: #222; }
    .message.newMessage { border-color: #00ff00; background: #1a2; }
    .message.recentNode { border-color: #00bfff; background: #113355; }
    .timestamp { font-size: 0.8em; color: #666; }
    .btn { margin-left: 10px; padding: 2px 6px; font-size: 0.8em; cursor: pointer; }
    .switch { position: relative; display: inline-block; width: 60px; height: 34px; vertical-align: middle; }
    .switch input { opacity: 0; width: 0; height: 0; }
    .slider { position: absolute; cursor: pointer; top: 0; left: 0; right: 0; bottom: 0; background-color: #ccc; transition: .4s; }
    .slider:before { position: absolute; content: ""; height: 26px; width: 26px; left: 4px; bottom: 4px; background-color: white; transition: .4s; }
    input:checked + .slider { background-color: #2196F3; }
    input:focus + .slider { box-shadow: 0 0 1px #2196F3; }
    input:checked + .slider:before { transform: translateX(26px); }
    .slider.round { border-radius: 34px; }
    .slider.round:before { border-radius: 50%; }
    #charCounter { font-size: 0.9em; color: #ccc; text-align: right; margin-top: 5px; }
    .nodeItem { margin-bottom: 12px; padding-bottom: 8px; border-bottom: 1px solid var(--theme-color); display: flex; flex-direction: column; align-items: flex-start; flex-wrap: wrap; }
    .nodeItem.recentNode { border-bottom: 2px solid #00bfff; background: #113355; }
    .nodeMainLine { font-weight: bold; font-size: 1.1em; }
    .nodeLongName { color: #aaa; font-size: 0.98em; margin-top: 2px; }
    .nodeInfoLine { margin-top: 2px; font-size: 0.95em; color: #ccc; display: flex; flex-wrap: wrap; gap: 10px; }
    .nodeGPS { margin-left: 0; }
    .nodeBeacon { color: #aaa; font-size: 0.92em; }
    .nodeHops { color: #6cf; font-size: 0.92em; }
    .nodeMapBtn { margin-left: 0; background: #222; color: #fff; border: 1px solid #ffa500; border-radius: 4px; padding: 2px 6px; font-size: 1em; cursor: pointer; text-decoration: none; }
    .nodeMapBtn:hover { background: #ffa500; color: #000; }
    .channel-header { display: flex; align-items: center; gap: 10px; }
    .reply-btn { margin-left: 10px; padding: 2px 8px; font-size: 0.85em; background: #222; color: var(--theme-color); border: 1px solid var(--theme-color); border-radius: 4px; cursor: pointer; }
    .mark-read-btn { margin-left: 10px; padding: 2px 8px; font-size: 0.85em; background: #222; color: #0f0; border: 1px solid #0f0; border-radius: 4px; cursor: pointer; }
    .mark-all-read-btn { margin-left: 10px; padding: 2px 8px; font-size: 0.85em; background: #222; color: #ff0; border: 1px solid #ff0; border-radius: 4px; cursor: pointer; }
    /* Threaded DM styles */
    .dm-thread { margin-bottom: 16px; border-left: 3px solid var(--theme-color); padding-left: 10px; }
    .dm-thread .message { margin-left: 0; }
    .dm-thread .reply-btn { margin-top: 5px; }
    .dm-thread .thread-replies { margin-left: 30px; border-left: 2px dashed #555; padding-left: 10px; }
    /* Node sort controls */
    .nodeSortBar { margin-bottom: 10px; }
    .nodeSortBar label { margin-right: 8px; }
    .nodeSortBar select { background: #222; color: #fff; border: 1px solid var(--theme-color); border-radius: 4px; padding: 2px 8px; }
    /* Full width search bar for nodes */
    #nodeSearch { width: 100%; margin-bottom: 10px; font-size: 1em; padding: 6px; box-sizing: border-box; }
    /* UI Settings panel hidden by default */
    .settings-panel { display: none; background: #111; border: 2px solid var(--theme-color); border-radius: 10px; padding: 20px; margin: 20px; }
    .settings-toggle { background: var(--theme-color); color: #000; padding: 8px 12px; margin: 20px; border-radius: 4px; font-weight: bold; cursor: pointer; display: inline-block; }
    .settings-toggle.active { background: #222; color: #ffa500; }
    /* Timezone selector */
    #timezoneSelect { margin-left: 10px; }
    /* Keep settings toggle and panel fixed so they don't move */
    .settings-toggle { position: fixed; bottom: 16px; left: 16px; z-index: 1100; box-shadow: 0 2px 6px rgba(0,0,0,0.6); }
    .settings-panel { position: fixed; bottom: 64px; left: 16px; z-index: 1100; width: 360px; max-height: 60vh; overflow:auto; margin: 0; }
    /* Autostart panel styles */
    .autostart-panel { position: fixed; bottom: 16px; right: 16px; z-index: 1100; }
    .autostart-box { display:flex;align-items:center;gap:10px;padding:10px 14px;background:#111;border:2px solid var(--theme-color);border-radius:12px; }
  </style>

  <script>
    // --- Mark as Read/Unread State ---
    let readDMs = JSON.parse(localStorage.getItem("readDMs") || "[]");
    let readChannels = JSON.parse(localStorage.getItem("readChannels") || "{}");

    function saveReadDMs() {
      localStorage.setItem("readDMs", JSON.stringify(readDMs));
    }
    function saveReadChannels() {
      localStorage.setItem("readChannels", JSON.stringify(readChannels));
    }
    function markDMAsRead(ts) {
      if (!readDMs.includes(ts)) {
        readDMs.push(ts);
        saveReadDMs();
        fetchMessagesAndNodes();
      }
    }
    function markAllDMsAsRead() {
      if (!confirm("Are you sure you want to mark ALL direct messages as read?")) return;
      let dms = allMessages.filter(m => m.direct);
      readDMs = dms.map(m => m.timestamp);
      saveReadDMs();
      fetchMessagesAndNodes();
    }
    function markChannelAsRead(channelIdx) {
      if (!confirm("Are you sure you want to mark ALL messages in this channel as read?")) return;
      let msgs = allMessages.filter(m => !m.direct && m.channel_idx == channelIdx);
      if (!readChannels) readChannels = {};
      readChannels[channelIdx] = msgs.map(m => m.timestamp);
      saveReadChannels();
      fetchMessagesAndNodes();
    }
    function isDMRead(ts) {
      return readDMs.includes(ts);
    }
    function isChannelMsgRead(ts, channelIdx) {
      return readChannels && readChannels[channelIdx] && readChannels[channelIdx].includes(ts);
    }

    // --- Ticker Dismissal State ---
    function setTickerDismissed(ts) {
      // Store the timestamp of the dismissed message and expiry
      localStorage.setItem("tickerDismissed", JSON.stringify({ts: ts, until: Date.now() + 30000}));
    }
    function isTickerDismissed(ts) {
      let obj = {};
      try { obj = JSON.parse(localStorage.getItem("tickerDismissed") || "{}"); } catch(e){}
      if (!obj.ts || !obj.until) return false;
      // Only dismiss if the same message and not expired
      return obj.ts === ts && Date.now() < obj.until;
    }

    // --- Timezone Offset State ---
    function getTimezoneOffset() {
      let tz = localStorage.getItem("meshtastic_ui_tz_offset");
      if (tz === null || isNaN(Number(tz))) return 0;
      return Number(tz);
    }
    function setTimezoneOffset(val) {
      localStorage.setItem("meshtastic_ui_tz_offset", String(val));
    }

    // Globals for reply targets
    var lastDMTarget = null;
    var lastChannelTarget = null;
  let allNodes = [];
  let allMessages = [];
  let fetchIntervalId = null; // guard to avoid multiple intervals
    let lastMessageTimestamp = null;
    let tickerTimeout = null;
    let tickerLastShownTimestamp = null;
    let nodeGPSInfo = """ + node_gps_info_json + """;
    let myGPS = """ + my_gps_json + """;

    // --- Node Sorting ---
    let nodeSortKey = localStorage.getItem("nodeSortKey") || "name";
    let nodeSortDir = localStorage.getItem("nodeSortDir") || "asc";

    function setNodeSort(key, dir) {
      nodeSortKey = key;
      nodeSortDir = dir;
      localStorage.setItem("nodeSortKey", key);
      localStorage.setItem("nodeSortDir", dir);
      updateNodesUI(allNodes, false);
    }

    function compareNodes(a, b) {
      // Helper for null/undefined
      function safe(v) { return v === undefined || v === null ? "" : v; }
      // For distance, use haversine if both have GPS, else sort GPS-enabled first
      if (nodeSortKey === "distance") {
        let aGPS = nodeGPSInfo[String(a.id)];
        let bGPS = nodeGPSInfo[String(b.id)];
        let aHas = aGPS && aGPS.lat != null && aGPS.lon != null;
        let bHas = bGPS && bGPS.lat != null && bGPS.lon != null;
        if (!aHas && !bHas) return 0;
        if (aHas && !bHas) return -1;
        if (!aHas && bHas) return 1;
        let distA = calcDistance(myGPS.lat, myGPS.lon, aGPS.lat, aGPS.lon);
        let distB = calcDistance(myGPS.lat, myGPS.lon, bGPS.lat, bGPS.lon);
        return (distA - distB) * (nodeSortDir === "asc" ? 1 : -1);
      }
      if (nodeSortKey === "gps") {
        let aGPS = nodeGPSInfo[String(a.id)];
        let bGPS = nodeGPSInfo[String(b.id)];
        let aHas = aGPS && aGPS.lat != null && aGPS.lon != null;
        let bHas = bGPS && bGPS.lat != null && bGPS.lon != null;
        if (aHas && !bHas) return nodeSortDir === "asc" ? -1 : 1;
        if (!aHas && bHas) return nodeSortDir === "asc" ? 1 : -1;
        return 0;
      }
      if (nodeSortKey === "name") {
        let cmp = safe(a.shortName).localeCompare(safe(b.shortName), undefined, {sensitivity:"base"});
        return cmp * (nodeSortDir === "asc" ? 1 : -1);
      }
      if (nodeSortKey === "beacon") {
        let aGPS = nodeGPSInfo[String(a.id)];
        let bGPS = nodeGPSInfo[String(b.id)];
        let aTime = aGPS && aGPS.beacon_time ? Date.parse(aGPS.beacon_time.replace(" UTC","Z")) : 0;
        let bTime = bGPS && bGPS.beacon_time ? Date.parse(bGPS.beacon_time.replace(" UTC","Z")) : 0;
        return (bTime - aTime) * (nodeSortDir === "asc" ? -1 : 1);
      }
      if (nodeSortKey === "hops") {
        let aGPS = nodeGPSInfo[String(a.id)];
        let bGPS = nodeGPSInfo[String(b.id)];
        let aH = aGPS && aGPS.hops != null ? aGPS.hops : 99;
        let bH = bGPS && bGPS.hops != null ? bGPS.hops : 99;
        return (aH - bH) * (nodeSortDir === "asc" ? 1 : -1);
      }
      return 0;
    }

    // Haversine formula (km)
    function calcDistance(lat1, lon1, lat2, lon2) {
      if (
        lat1 == null || lon1 == null ||
        lat2 == null || lon2 == null
      ) return 99999;
      let toRad = x => x * Math.PI / 180;
      let R = 6371;
      let dLat = toRad(lat2 - lat1);
      let dLon = toRad(lon2 - lon1);
      let a = Math.sin(dLat/2) * Math.sin(dLat/2) +
              Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) *
              Math.sin(dLon/2) * Math.sin(dLon/2);
      let c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
      return R * c;
    }

    // --- UI Settings State ---
    let uiSettings = {
      themeColor: "#ffa500",
      hueRotateEnabled: false,
      hueRotateSpeed: 10,
      soundURL: ""
    };
    let hueRotateInterval = null;
    let currentHue = 0;

    function toggleMode(force) {
      if (typeof force !== "undefined") {
        document.getElementById('modeSwitch').checked = force === 'direct';
      }
      const dm = document.getElementById('modeSwitch').checked;
      document.getElementById('dmField').style.display = dm ? 'block' : 'none';
      document.getElementById('channelField').style.display = dm ? 'none' : 'block';
      document.getElementById('modeLabel').textContent = dm ? 'Direct' : 'Broadcast';
    }

    // Defensive toggle function: ensures the settings panel can be
    // toggled even if other JS earlier in the page throws an error
    // and prevents the normal event listeners from being installed.
    function toggleSettings() {
      try {
        console && console.debug && console.debug('toggleSettings called');
        const panel = document.getElementById('settingsPanel');
        const toggle = document.getElementById('settingsToggle');
        if (!panel || !toggle) return;
        if (panel.style.display === 'none' || panel.style.display === '') {
          panel.style.display = 'block';
          toggle.textContent = "Hide UI Settings";
        } else {
          panel.style.display = 'none';
          toggle.textContent = "Show UI Settings";
        }
      } catch (e) { console && console.error && console.error('toggleSettings error', e); }
    }

    // Expose toggleSettings to the global scope so inline onclick handlers
    // still work even if other JS errors prevent event bindings below.
    try { window.toggleSettings = toggleSettings; } catch (e) { console && console.error && console.error('expose toggleSettings failed', e); }

    // Defensive DOM wiring: run after DOMContentLoaded
    document.addEventListener("DOMContentLoaded", function() {
      // Defensive bindings: check elements exist before using them so one
      // missing element doesn't break all other UI wiring.
      const modeSwitchEl = document.getElementById('modeSwitch');
      if (modeSwitchEl) modeSwitchEl.addEventListener('change', function() { toggleMode(); });

      const settingsToggleEl = document.getElementById('settingsToggle');
      const settingsPanelEl = document.getElementById('settingsPanel');
      if (settingsToggleEl) {
        settingsToggleEl.addEventListener('click', function() {
          if (!settingsPanelEl) return;
          if (settingsPanelEl.style.display === 'none' || settingsPanelEl.style.display === '') {
            settingsPanelEl.style.display = 'block';
            settingsToggleEl.textContent = "Hide UI Settings";
          } else {
            settingsPanelEl.style.display = 'none';
            settingsToggleEl.textContent = "Show UI Settings";
          }
        });
      }
      if (settingsPanelEl) {
        settingsPanelEl.style.display = 'none'; // Hide settings panel by default
      }
      if (settingsToggleEl) settingsToggleEl.textContent = "Show UI Settings";

      const nodeSearchEl = document.getElementById('nodeSearch');
      if (nodeSearchEl) nodeSearchEl.addEventListener('input', function() { filterNodes(this.value, false); });
      const destNodeSearchEl = document.getElementById('destNodeSearch');
      if (destNodeSearchEl) destNodeSearchEl.addEventListener('input', function() { filterNodes(this.value, true); });

      // Node sort controls
      const nodeSortKeyEl = document.getElementById('nodeSortKey');
      const nodeSortDirEl = document.getElementById('nodeSortDir');
      if (nodeSortKeyEl) nodeSortKeyEl.addEventListener('change', function() { setNodeSort(this.value, nodeSortDir); });
      if (nodeSortDirEl) nodeSortDirEl.addEventListener('change', function() { setNodeSort(nodeSortKey, this.value); });

      // --- UI Settings: Load from localStorage ---
      try { loadUISettings(); } catch (e) { console && console.error && console.error('loadUISettings failed', e); }

      // Set initial values in settings panel
      document.getElementById('uiColorPicker').value = uiSettings.themeColor;
      document.getElementById('hueRotateEnabled').checked = uiSettings.hueRotateEnabled;
      document.getElementById('hueRotateSpeed').value = uiSettings.hueRotateSpeed;
      document.getElementById('soundURL').value = uiSettings.soundURL;

      // Apply settings on load
      applyThemeColor(uiSettings.themeColor);
      if (uiSettings.hueRotateEnabled) startHueRotate(uiSettings.hueRotateSpeed);
      setIncomingSound(uiSettings.soundURL);

      // Apply button
      document.getElementById('applySettingsBtn').addEventListener('click', function() {
        // Read values
        uiSettings.themeColor = document.getElementById('uiColorPicker').value;
        uiSettings.hueRotateEnabled = document.getElementById('hueRotateEnabled').checked;
        uiSettings.hueRotateSpeed = parseFloat(document.getElementById('hueRotateSpeed').value);
        // For soundURL, only allow local file path from file input
        var fileInput = document.getElementById('soundFile');
        if (fileInput && fileInput.files.length > 0) {
          var file = fileInput.files[0];
          var url = URL.createObjectURL(file);
          uiSettings.soundURL = url;
          document.getElementById('soundURL').value = file.name;
        }
        saveUISettings();
        applyThemeColor(uiSettings.themeColor);
        if (uiSettings.hueRotateEnabled) {
          startHueRotate(uiSettings.hueRotateSpeed);
        } else {
          stopHueRotate();
        }
        setIncomingSound(uiSettings.soundURL);
        // Save timezone offset
        setTimezoneOffset(document.getElementById('timezoneSelect').value);
        fetchMessagesAndNodes();
      });

      // Listen for file input change to update sound preview
      document.getElementById('soundFile').addEventListener('change', function() {
        if (this.files.length > 0) {
          var file = this.files[0];
          var url = URL.createObjectURL(file);
          uiSettings.soundURL = url;
          document.getElementById('soundURL').value = file.name;
          setIncomingSound(url);
        }
      });

      // Set initial sort controls
      document.getElementById('nodeSortKey').value = nodeSortKey;
      document.getElementById('nodeSortDir').value = nodeSortDir;

      // Set timezone selector
      let tzSel = document.getElementById('timezoneSelect');
      let tz = getTimezoneOffset();
      tzSel.value = tz;
    });

    // --- UI Settings Functions ---
    function saveUISettings() {
      // Only persist the file name for sound, not the blob URL
      let settingsToSave = Object.assign({}, uiSettings);
      if (settingsToSave.soundURL && settingsToSave.soundURL.startsWith('blob:')) {
        settingsToSave.soundURL = document.getElementById('soundURL').value;
      }
      localStorage.setItem("meshtastic_ui_settings", JSON.stringify(settingsToSave));
    }
    function loadUISettings() {
      try {
        let s = localStorage.getItem("meshtastic_ui_settings");
        if (s) {
          let parsed = JSON.parse(s);
          Object.assign(uiSettings, parsed);
        }
      } catch (e) {}
    }
    function applyThemeColor(color) {
      document.documentElement.style.setProperty('--theme-color', color);
    }
    function startHueRotate(speed) {
      stopHueRotate();
      hueRotateInterval = setInterval(function() {
        currentHue = (currentHue + 1) % 360;
        document.body.style.filter = `hue-rotate(${currentHue}deg)`;
      }, Math.max(5, 1000 / Math.max(1, speed)));
    }
    function stopHueRotate() {
      if (hueRotateInterval) clearInterval(hueRotateInterval);
      hueRotateInterval = null;
      document.body.style.filter = "";
      currentHue = 0;
    }
    function toggleHueRotate(enabled, speed) {
      uiSettings.hueRotateEnabled = enabled;
      uiSettings.hueRotateSpeed = speed;
      saveUISettings();
      if (enabled) startHueRotate(speed);
      else stopHueRotate();
    }
    function setIncomingSound(url) {
      let audio = document.getElementById('incomingSound');
      audio.src = url || "";
      uiSettings.soundURL = url;
      saveUISettings();
    }

    function replyToMessage(mode, target) {
      toggleMode(mode);
      if (mode === 'direct') {
        const dest = document.getElementById('destNode');
        dest.value = target;
        const name = dest.selectedOptions[0] ? dest.selectedOptions[0].text.split(' (')[0] : '';
        document.getElementById('messageBox').value = '@' + name + ': ';
      } else {
        const ch = document.getElementById('channelSel');
        ch.value = target;
        document.getElementById('messageBox').value = '';
      }
    }

    function dmToNode(nodeId, shortName, replyToTs) {
      toggleMode('direct');
      document.getElementById('destNode').value = nodeId;
      if (replyToTs) {
        // Prefill with quoted message if replying to a thread
        let threadMsg = allMessages.find(m => m.timestamp === replyToTs);
        let quoted = threadMsg ? `> ${threadMsg.message}\n` : '';
        document.getElementById('messageBox').value = quoted + '@' + shortName + ': ';
      } else {
        document.getElementById('messageBox').value = '@' + shortName + ': ';
      }
    }

    function replyToLastDM() {
      if (lastDMTarget !== null) {
        const opt = document.querySelector(`#destNode option[value="${lastDMTarget}"]`);
        const shortName = opt ? opt.text.split(' (')[0] : '';
        dmToNode(lastDMTarget, shortName);
      } else {
        alert("No direct message target available.");
      }
    }

    function replyToLastChannel() {
      if (lastChannelTarget !== null) {
        toggleMode('broadcast');
        document.getElementById('channelSel').value = lastChannelTarget;
        document.getElementById('messageBox').value = '';
      } else {
        alert("No broadcast channel target available.");
      }
    }

    // Data fetch & UI updates
    const CHANNEL_NAMES = """ + json.dumps(channel_names) + """;

    function getNowUTC() {
      return new Date(new Date().toISOString().slice(0, 19) + "Z");
    }

    function getTZAdjusted(tsStr) {
      // tsStr is "YYYY-MM-DD HH:MM:SS UTC"
      let tz = getTimezoneOffset();
      if (!tsStr) return "";
      let dt = new Date(tsStr.replace(" UTC", "Z"));
      if (isNaN(dt.getTime())) return tsStr;
      dt.setHours(dt.getHours() + tz);
      let pad = n => n < 10 ? "0" + n : n;
      return dt.getFullYear() + "-" + pad(dt.getMonth()+1) + "-" + pad(dt.getDate()) + " " +
             pad(dt.getHours()) + ":" + pad(dt.getMinutes()) + ":" + pad(dt.getSeconds()) +
             (tz === 0 ? " UTC" : (tz > 0 ? " UTC+" + tz : " UTC" + tz));
    }

    function isRecent(tsStr, minutes) {
      if (!tsStr) return false;
      let now = getNowUTC();
      let msgTime = new Date(tsStr.replace(" UTC", "Z"));
      return (now - msgTime) < minutes * 60 * 1000;
    }

    async function fetchMessagesAndNodes() {
      try {
        let msgs = await (await fetch("/messages")).json();
        allMessages = msgs;
        let nodes = await (await fetch("/nodes")).json();
        allNodes = nodes;
        updateMessagesUI(msgs);
        updateNodesUI(nodes, false);
        updateNodesUI(nodes, true);
        updateDirectMessagesUI(msgs, nodes);
        highlightRecentNodes(nodes);
        showLatestMessageTicker(msgs);
      } catch (e) { console.error(e); }
    }

    function updateMessagesUI(messages) {
      // Reverse the order to show the newest messages first
      const groups = {};
      messages.slice().reverse().forEach(m => {
        if (!m.direct && m.channel_idx != null) {
          (groups[m.channel_idx] = groups[m.channel_idx] || []).push(m);
        }
      });

      const channelDiv = document.getElementById("channelDiv");
      channelDiv.innerHTML = "";
      Object.keys(groups).sort().forEach(ch => {
        const name = CHANNEL_NAMES[ch] || `Channel ${ch}`;
        // Channel header with reply and mark all as read button
        const headerWrap = document.createElement("div");
        headerWrap.className = "channel-header";
        const header = document.createElement("h3");
        header.textContent = `${ch} – ${name}`;
        header.style.margin = 0;
        headerWrap.appendChild(header);

        // Add reply button for channel
        const replyBtn = document.createElement("button");
        replyBtn.textContent = "Send to Channel";
        replyBtn.className = "reply-btn";
        replyBtn.onclick = function() {
          replyToMessage('broadcast', ch);
        };
        headerWrap.appendChild(replyBtn);

        // Mark all as read for this channel
        const markAllBtn = document.createElement("button");
        markAllBtn.textContent = "Mark all as read";
        markAllBtn.className = "mark-all-read-btn";
        markAllBtn.onclick = function() {
          markChannelAsRead(ch);
        };
        headerWrap.appendChild(markAllBtn);

        channelDiv.appendChild(headerWrap);

        groups[ch].forEach(m => {
          if (isChannelMsgRead(m.timestamp, ch)) return; // Hide read messages
          const wrap = document.createElement("div");
          wrap.className = "message";
          if (isRecent(m.timestamp, 60)) wrap.classList.add("newMessage");
          const ts = document.createElement("div");
          ts.className = "timestamp";
          ts.textContent = `📢 ${getTZAdjusted(m.timestamp)} | ${m.node}`;
          const body = document.createElement("div");
          body.textContent = m.message;
          wrap.append(ts, body);

          // Mark as read button
          const markBtn = document.createElement("button");
          markBtn.textContent = "Mark as read";
          markBtn.className = "mark-read-btn";
          markBtn.onclick = function() {
            if (!readChannels[ch]) readChannels[ch] = [];
            if (!readChannels[ch].includes(m.timestamp)) {
              readChannels[ch].push(m.timestamp);
              saveReadChannels();
              fetchMessagesAndNodes();
            }
          };
          wrap.appendChild(markBtn);

          channelDiv.appendChild(wrap);
        });
        channelDiv.appendChild(document.createElement("hr"));
      });

      // Update global reply targets
      lastDMTarget = null;
      lastChannelTarget = null;
      for (const m of messages) {
        if (m.direct && m.node_id != null && lastDMTarget === null) {
          lastDMTarget = m.node_id;
        }
        if (!m.direct && m.channel_idx != null && lastChannelTarget === null) {
          lastChannelTarget = m.channel_idx;
        }
        if (lastDMTarget != null && lastChannelTarget != null) break;
      }
    }

    // --- DM Threaded UI ---
    function updateDirectMessagesUI(messages, nodes) {
      // Group DMs by node_id, then by thread (reply_to)
      const dmDiv = document.getElementById("dmMessagesDiv");
      dmDiv.innerHTML = "";

      // Only direct messages, newest first
      let dms = messages.filter(m => m.direct && !isDMRead(m.timestamp)).slice().reverse();

      // Group by node_id
      let threads = {};
      dms.forEach(m => {
        if (!threads[m.node_id]) threads[m.node_id] = [];
        threads[m.node_id].push(m);
      });

      // Mark all as read button for DMs
      if (dms.length > 0) {
        const markAllBtn = document.createElement("button");
        markAllBtn.textContent = "Mark all as read";
        markAllBtn.className = "mark-all-read-btn";
        markAllBtn.onclick = function() {
          markAllDMsAsRead();
        };
        dmDiv.appendChild(markAllBtn);
      }

      Object.keys(threads).forEach(nodeId => {
        const node = allNodes.find(n => n.id == nodeId);
        const shortName = node ? node.shortName : nodeId;
        const threadDiv = document.createElement("div");
        threadDiv.className = "dm-thread";

        // Find root messages (no reply_to)
        let rootMsgs = threads[nodeId].filter(m => !m.reply_to);

        rootMsgs.forEach(rootMsg => {
          const wrap = document.createElement("div");
          wrap.className = "message";
          if (isRecent(rootMsg.timestamp, 60)) wrap.classList.add("newMessage");
          const ts = document.createElement("div");
          ts.className = "timestamp";
          ts.textContent = `📩 ${getTZAdjusted(rootMsg.timestamp)} | ${rootMsg.node}`;
          const body = document.createElement("div");
          body.textContent = rootMsg.message;
          wrap.append(ts, body);

          // Add reply button for root
          const replyBtn = document.createElement("button");
          replyBtn.textContent = "Reply";
          replyBtn.className = "reply-btn";
          replyBtn.onclick = function() {
            dmToNode(nodeId, shortName, rootMsg.timestamp);
          };
          wrap.appendChild(replyBtn);

          // Mark as read button for root
          const markBtn = document.createElement("button");
          markBtn.textContent = "Mark as read";
          markBtn.className = "mark-read-btn";
          markBtn.onclick = function() {
            markDMAsRead(rootMsg.timestamp);
          };
          wrap.appendChild(markBtn);

          threadDiv.appendChild(wrap);

          // Find replies to this root
          let replies = threads[nodeId].filter(m => m.reply_to === rootMsg.timestamp);
          if (replies.length) {
            const repliesDiv = document.createElement("div");
            repliesDiv.className = "thread-replies";
            replies.forEach(replyMsg => {
              const replyWrap = document.createElement("div");
              replyWrap.className = "message";
              if (isRecent(replyMsg.timestamp, 60)) replyWrap.classList.add("newMessage");
              const rts = document.createElement("div");
              rts.className = "timestamp";
              rts.textContent = `↪️ ${getTZAdjusted(replyMsg.timestamp)} | ${replyMsg.node}`;
              const rbody = document.createElement("div");
              rbody.textContent = replyMsg.message;
              replyWrap.append(rts, rbody);

              // Reply to reply (threaded)
              const replyBtn2 = document.createElement("button");
              replyBtn2.textContent = "Reply";
              replyBtn2.className = "reply-btn";
              replyBtn2.onclick = function() {
                dmToNode(nodeId, shortName, replyMsg.timestamp);
              };
              replyWrap.appendChild(replyBtn2);

              // Mark as read button for reply
              const markBtn2 = document.createElement("button");
              markBtn2.textContent = "Mark as read";
              markBtn2.className = "mark-read-btn";
              markBtn2.onclick = function() {
                markDMAsRead(replyMsg.timestamp);
              };
              replyWrap.appendChild(markBtn2);

              repliesDiv.appendChild(replyWrap);
            });
            threadDiv.appendChild(repliesDiv);
          }
        });

        dmDiv.appendChild(threadDiv);
      });
    }

    function updateNodesUI(nodes, isDest) {
      // isDest: false = available nodes panel, true = destination node dropdown
      if (!isDest) {
        const list = document.getElementById("nodeListDiv");
        let filter = document.getElementById('nodeSearch').value.toLowerCase();
        list.innerHTML = "";
        let filtered = nodes.filter(n =>
          (n.shortName && n.shortName.toLowerCase().includes(filter)) ||
          (n.longName && n.longName.toLowerCase().includes(filter)) ||
          String(n.id).toLowerCase().includes(filter)
        );
        // Sort
        filtered.sort(compareNodes);

        filtered.forEach(n => {
          const d = document.createElement("div");
          d.className = "nodeItem";
          if (isRecentNode(n.id)) d.classList.add("recentNode");

          // Main line: Short name and ID
          const mainLine = document.createElement("div");
          mainLine.className = "nodeMainLine";
          mainLine.innerHTML = `<span>${n.shortName || ""}</span> <span style="color:#ffa500;">(${n.id})</span>`;
          d.appendChild(mainLine);

          // Long name (if present)
          if (n.longName && n.longName !== n.shortName) {
            const longName = document.createElement("div");
            longName.className = "nodeLongName";
            longName.textContent = n.longName;
            d.appendChild(longName);
          }

          // Info line 1: GPS/map, distance
          const infoLine1 = document.createElement("div");
          infoLine1.className = "nodeInfoLine";
          let gps = nodeGPSInfo[String(n.id)];
          if (gps && gps.lat != null && gps.lon != null) {
            // Map button (emoji)
            const mapA = document.createElement("a");
            mapA.href = `https://www.google.com/maps/search/?api=1&query=${gps.lat},${gps.lon}`;
            mapA.target = "_blank";
            mapA.className = "nodeMapBtn";
            mapA.title = "Show on Google Maps";
            mapA.innerHTML = "🗺️";
            infoLine1.appendChild(mapA);

            // Distance
            if (myGPS && myGPS.lat != null && myGPS.lon != null) {
              let dist = calcDistance(myGPS.lat, myGPS.lon, gps.lat, gps.lon);
              if (dist < 99999) {
                const distSpan = document.createElement("span");
                distSpan.className = "nodeGPS";
                distSpan.title = "Approximate distance from connected node";
                distSpan.innerHTML = `📏 ${dist.toFixed(2)} km`;
                infoLine1.appendChild(distSpan);
              }
            }
          }
          d.appendChild(infoLine1);

          // Info line 2: Beacon/reporting time
          const infoLine2 = document.createElement("div");
          infoLine2.className = "nodeInfoLine";
          if (gps && gps.beacon_time) {
            const beacon = document.createElement("span");
            beacon.className = "nodeBeacon";
            beacon.title = "Last beacon/reporting time";
            beacon.innerHTML = `🕒 ${getTZAdjusted(gps.beacon_time)}`;
            infoLine2.appendChild(beacon);
          }
          d.appendChild(infoLine2);

          // Info line 3: Hops
          const infoLine3 = document.createElement("div");
          infoLine3.className = "nodeInfoLine";
          // Only show hops if available and not null/undefined/""
          if (gps && gps.hops != null && gps.hops !== "" && gps.hops !== undefined) {
            const hops = document.createElement("span");
            hops.className = "nodeHops";
            hops.title = "Hops from this node";
            hops.innerHTML = `⛓️ ${gps.hops} hop${gps.hops==1?"":"s"}`;
            infoLine3.appendChild(hops);
            d.appendChild(infoLine3);
          }
          // If hops is not available, do not show this section at all

          // DM button
          const btn = document.createElement("button");
          btn.textContent = "DM";
          btn.className = "btn";
          btn.onclick = () => dmToNode(n.id, n.shortName);
          d.append(btn);

          list.appendChild(d);
        });
      } else {
        const sel  = document.getElementById("destNode");
        const prevNode = sel.value;
        sel.innerHTML  = "<option value=''>--Select Node--</option>";
        let filter = document.getElementById('destNodeSearch').value.toLowerCase();
        let filtered = nodes.filter(n =>
          (n.shortName && n.shortName.toLowerCase().includes(filter)) ||
          (n.longName && n.longName.toLowerCase().includes(filter)) ||
          String(n.id).toLowerCase().includes(filter)
        );
        filtered.forEach(n => {
          const opt = document.createElement("option");
          opt.value = n.id;
          opt.innerHTML = `${n.shortName} (${n.id})`;
          sel.append(opt);
        });
        sel.value = prevNode;
      }
    }

    function filterNodes(val, isDest) {
      updateNodesUI(allNodes, isDest);
    }

    // Track recently discovered nodes (seen in last hour)
    function isRecentNode(nodeId) {
      // Find the latest message from this node
      let found = allMessages.slice().reverse().find(m => m.node_id == nodeId);
      if (!found) return false;
      return isRecent(found.timestamp, 60);
    }

    function highlightRecentNodes(nodes) {
      // Called after updateNodesUI
      // No-op: handled by .recentNode class in updateNodesUI
    }

    // Show latest inbound message in ticker, dismissable, timeout after 30s, and persist dismiss across refreshes
    function showLatestMessageTicker(messages) {
      // Show both channel and direct inbound messages, but not outgoing (WebUI, AI_NODE_NAME)
      // and not AI responses (reply_to is not null)
      let inbound = messages.filter(m =>
        m.node !== "WebUI" &&
        m.node !== "Twilio" &&
        m.node !== """ + json.dumps(AI_NODE_NAME) + """ &&
        (!m.reply_to) // Only show original messages, not replies (AI responses)
      );
      if (!inbound.length) return hideTicker();
      let latest = inbound[inbound.length - 1];
      if (!latest || !latest.message) return hideTicker();

      // If dismissed, don't show
      if (isTickerDismissed(latest.timestamp)) return hideTicker();

      // Only show ticker if not already shown for this message
      if (tickerLastShownTimestamp === latest.timestamp) return;
      tickerLastShownTimestamp = latest.timestamp;

      let ticker = document.getElementById('ticker');
      let tickerMsg = ticker.querySelector('p');
      tickerMsg.textContent = latest.message;
      ticker.style.display = 'block';

      // Show dismiss button at far right, on top
      let dismissBtn = ticker.querySelector('.dismiss-btn');
      if (!dismissBtn) {
        dismissBtn = document.createElement('button');
        dismissBtn.textContent = "Dismiss";
        dismissBtn.className = "dismiss-btn";
        dismissBtn.onclick = function(e) {
          e.stopPropagation();
          ticker.style.display = 'none';
          setTickerDismissed(latest.timestamp);
          if (tickerTimeout) clearTimeout(tickerTimeout);
        };
        ticker.appendChild(dismissBtn);
      } else {
        // Always update dismiss button to dismiss this message
        dismissBtn.onclick = function(e) {
          e.stopPropagation();
          ticker.style.display = 'none';
          setTickerDismissed(latest.timestamp);
          if (tickerTimeout) clearTimeout(tickerTimeout);
        };
      }

      // Remove after 30s and persist dismiss
      if (tickerTimeout) clearTimeout(tickerTimeout);
      tickerTimeout = setTimeout(() => {
        ticker.style.display = 'none';
        setTickerDismissed(latest.timestamp);
        tickerLastShownTimestamp = null;
      }, 30000);
    }

    function hideTicker() {
      let ticker = document.getElementById('ticker');
      ticker.style.display = 'none';
      tickerLastShownTimestamp = null;
      if (tickerTimeout) {
        clearTimeout(tickerTimeout);
        tickerTimeout = null;
      }
    }

    function pollStatus() {
      fetch("/connection_status")
        .then(r => r.json())
        .then(d => {
          const s = document.getElementById("connectionStatus");
          if (d.status != "Connected") {
            s.style.background = "red";
            s.style.height = "40px";
            s.textContent = `Connection Error: ${d.error}`;
          } else {
            s.style.background = "green";
            s.style.height = "20px";
            s.textContent = "Connected";
          }
        })
        .catch(e => console.error(e));
    }
    setInterval(pollStatus, 5000);

    function onPageLoad() {
      if (!fetchIntervalId) {
        fetchIntervalId = setInterval(fetchMessagesAndNodes, 10000); // every 10s
      }
      fetchMessagesAndNodes();
      toggleMode(); // Set initial mode
    }
    window.addEventListener("load", onPageLoad);
  </script>
</head>
<body onload="onPageLoad()">
  <div id="connectionStatus"></div>
  <div class="header-buttons"><a href="/logs" target="_blank">Logs</a></div>
  <div id="ticker-container">
    <div id="ticker"><p></p></div>
  </div>
  <audio id="incomingSound"></audio>

  <div class="lcars-panel" id="sendForm">
    <h2>Send a Message</h2>
    <form method="POST" action="/ui_send">
      <label>Message Mode:</label>
      <label class="switch">
        <input type="checkbox" id="modeSwitch">
        <span class="slider round"></span>
      </label>
      <span id="modeLabel">Broadcast</span><br><br>

      <div id="dmField" style="display:none;">
        <label>Destination Node:</label><br>
        <input type="text" id="destNodeSearch" placeholder="Search destination nodes..."><br>
        <select id="destNode" name="destination_node"></select><br><br>
      </div>

      <div id="channelField" style="display:block;">
        <label>Channel:</label><br>
        <select id="channelSel" name="channel_index">
"""
    for i in range(8):
        name = channel_names.get(str(i), f"Channel {i}")
        html += f"          <option value='{i}'>{i} - {name}</option>\n"
    html += """        </select><br><br>
      </div>

      <label>Message:</label><br>
      <textarea id="messageBox" name="message" rows="3" style="width:80%;"></textarea>
      <div id="charCounter">Characters: 0/1000, Chunks: 0/5</div><br>
      <button type="submit">Send</button>
      <button type="button" onclick="replyToLastDM()">Reply to Last DM</button>
      <button type="button" onclick="replyToLastChannel()">Reply to Last Channel</button>
    </form>
  </div>

  <div class="three-col">
    <div class="col">
      <div class="lcars-panel">
        <h2>Channel Messages</h2>
        <div id="channelDiv"></div>
      </div>
    </div>
    <div class="col">
      <div class="lcars-panel">
        <h2>Available Nodes</h2>
        <input type="text" id="nodeSearch" placeholder="Search nodes by name, id, or long name...">
        <div class="nodeSortBar">
          <label for="nodeSortKey">Sort by:</label>
          <select id="nodeSortKey">
            <option value="name">Name</option>
            <option value="beacon">Last Reporting Time</option>
            <option value="hops">Number of Hops</option>
            <option value="gps">GPS Enabled</option>
            <option value="distance">Distance</option>
          </select>
          <label for="nodeSortDir">Order:</label>
          <select id="nodeSortDir">
            <option value="asc">Ascending</option>
            <option value="desc">Descending</option>
          </select>
        </div>
        <div id="nodeListDiv"></div>
      </div>
    </div>
    <div class="col">
      <div class="lcars-panel">
        <h2>Direct Messages</h2>
        <div id="dmMessagesDiv"></div>
      </div>
    </div>
  </div>

  </div>

    <div class="settings-toggle" id="settingsToggle" onclick="toggleSettings()">Show UI Settings</div>
    <!-- Fallback toggleSettings: ensures the button works even if main script fails to load -->
    <script>
      if (typeof window.toggleSettings !== 'function') {
        window.toggleSettings = function() {
          try {
            var panel = document.getElementById('settingsPanel');
            var toggle = document.getElementById('settingsToggle');
            if (!panel || !toggle) return;
            if (panel.style.display === 'none' || panel.style.display === '') {
              panel.style.display = 'block';
              toggle.textContent = 'Hide UI Settings';
            } else {
              panel.style.display = 'none';
              toggle.textContent = 'Show UI Settings';
            }
          } catch (e) { console && console.error && console.error('fallback toggleSettings error', e); }
        };
      }
    </script>
  <div class="settings-panel" id="settingsPanel">
    <h2>UI Settings</h2>
    <label for="uiColorPicker">Theme Color:</label>
    <input type="color" id="uiColorPicker" value="#ffa500"><br><br>
    <label for="hueRotateEnabled">Enable Hue Rotation:</label>
    <input type="checkbox" id="hueRotateEnabled"><br><br>
    <label for="hueRotateSpeed">Hue Rotation Speed:</label>
    <input type="range" id="hueRotateSpeed" min="5" max="60" step="0.1" value="10"><br><br>
    <label for="soundFile">Incoming Message Sound (local file):</label>
    <input type="file" id="soundFile" accept="audio/*"><br>
    <input type="text" id="soundURL" placeholder="No file selected" readonly style="background:#222;color:#fff;border:none;"><br><br>
    <label for="timezoneSelect">Timezone Offset (hours):</label>
    <select id="timezoneSelect">
"""
    # Timezone selector: -12 to +14
    for tz in range(-12, 15):
        html += f'      <option value="{tz}">{tz:+d}</option>\n'
    html += """    </select><br><br>
    <button id="applySettingsBtn" type="button">Apply Settings</button>
  </div>
    </div>

    <!-- Autostart toggle panel (fixed bottom-right) -->
    <div class="autostart-panel">
      <div class="autostart-box">
        <label style="font-weight:bold;color:#fff;margin:0 6px 0 0;">Start MESH-MASTER on boot</label>
        <label class="switch" style="margin:0;">
          <input type="checkbox" id="autostartToggle">
          <span class="slider round"></span>
        </label>
        <button id="saveAutostartBtn" class="btn" style="margin-left:6px;">Save</button>
      </div>
      <div style="color:#ccc;font-size:0.9em;margin-top:8px;max-width:420px;">
        Note: This toggle configures Desktop (GUI) autostart via a .desktop file. On headless servers or
        when the Desktop session doesn’t run at boot, use a systemd service instead. A helper installer
        script is included in the repository under scripts/. 
      </div>
    </div>

    <script>
    // Autostart controls
      async function loadAutostart() {
        try {
          let r = await fetch('/autostart');
          let j = await r.json();
          document.getElementById('autostartToggle').checked = !!j.start_on_boot;
        } catch (e) { console.error(e); }
      }
      const saveAutostartBtn = document.getElementById('saveAutostartBtn');
      const autostartToggleEl = document.getElementById('autostartToggle');
      if (saveAutostartBtn) {
        saveAutostartBtn.addEventListener('click', async function() {
          try {
            let enabled = autostartToggleEl ? autostartToggleEl.checked : false;
            let r = await fetch('/autostart/toggle', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({start_on_boot: enabled}) });
            let j = await r.json();
            alert('Autostart saved: ' + (j.start_on_boot ? 'Enabled' : 'Disabled'));
          } catch (e) { alert('Failed to save autostart: ' + e); }
        });
      }
      // Load initial state
      loadAutostart();

  // Expose defensive toggle to global window in case event binding fails
  // (already exposed earlier near the toggleSettings definition)
    </script>
</body>
</html>
"""
    return html



@app.route('/autostart', methods=['GET'])
def get_autostart():
    cfg = safe_load_json(CONFIG_FILE, {})
    return jsonify({'start_on_boot': bool(cfg.get('start_on_boot', False))})


@app.route('/autostart/toggle', methods=['POST'])
def toggle_autostart():
    data = request.get_json(force=True)
    desired = bool(data.get('start_on_boot', False))
    # Update config.json
    try:
        cfg = safe_load_json(CONFIG_FILE, {})
        cfg['start_on_boot'] = desired
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(cfg, f, indent=2)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    # Update autostart desktop file
    try:
        desktop_path = os.path.expanduser('~/.config/autostart/mesh-master-autostart.desktop')
        if os.path.exists(desktop_path):
            # read and replace X-GNOME-Autostart-enabled
            with open(desktop_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            out = []
            found = False
            for L in lines:
                if L.strip().startswith('X-GNOME-Autostart-enabled'):
                    out.append('X-GNOME-Autostart-enabled=' + ('true' if desired else 'false') + '\n')
                    found = True
                else:
                    out.append(L)
            if not found:
                out.append('X-GNOME-Autostart-enabled=' + ('true' if desired else 'false') + '\n')
            with open(desktop_path, 'w', encoding='utf-8') as f:
                f.writelines(out)
        else:
            # create the file
            desktop_dir = os.path.dirname(desktop_path)
            os.makedirs(desktop_dir, exist_ok=True)
            with open(desktop_path, 'w', encoding='utf-8') as f:
                f.write('[Desktop Entry]\nType=Application\nName=MESH-MASTER Autostart\nExec=' + os.path.abspath('start_mesh_master.sh') + '\nX-GNOME-Autostart-enabled=' + ('true' if desired else 'false') + '\n')
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    return jsonify({'start_on_boot': desired})
@app.route("/ui_send", methods=["POST"])
def ui_send():
    message = request.form.get("message", "").strip()
    mode = "direct" if request.form.get("destination_node", "") != "" else "broadcast"
    if mode == "direct":
        dest_node = request.form.get("destination_node", "").strip()
    else:
        dest_node = None
    if mode == "broadcast":
        try:
            channel_idx = int(request.form.get("channel_index", "0"))
        except (ValueError, TypeError):
            channel_idx = 0
    else:
        channel_idx = None
    if not message:
        return redirect(url_for("dashboard"))
    try:
        if mode == "direct" and dest_node:
            dest_info = f"{get_node_shortname(dest_node)} ({dest_node})"
            log_message("WebUI", f"{message} [to: {dest_info}]", direct=True)
            info_print(f"[UI] Direct message to node {dest_info} => '{message}'")
            send_direct_chunks(interface, message, dest_node)
        else:
            log_message("WebUI", f"{message} [to: Broadcast Channel {channel_idx}]", direct=False, channel_idx=channel_idx)
            info_print(f"[UI] Broadcast on channel {channel_idx} => '{message}'")
            send_broadcast_chunks(interface, message, channel_idx)
    except Exception as e:
        print(f"⚠️ /ui_send error: {e}")
    return redirect(url_for("dashboard"))

@app.route("/send", methods=["POST"])
def send_message():
    dprint("POST /send => manual JSON send")
    data = request.json
    if not data:
        return jsonify({"status": "error", "message": "No JSON payload"}), 400
    message = data.get("message")
    node_id = data.get("node_id")
    channel_idx = data.get("channel_index", 0)
    direct = data.get("direct", False)
    if not message or node_id is None:
        return jsonify({"status": "error", "message": "Missing 'message' or 'node_id'"}), 400
    try:
        if direct:
            log_message("WebUI", f"{message} [to: {get_node_shortname(node_id)} ({node_id})]", direct=True)
            info_print(f"[Info] Direct send to node {node_id} => '{message}'")
            send_direct_chunks(interface, message, node_id)
            return jsonify({"status": "sent", "to": node_id, "direct": True, "message": message})
        else:
            log_message("WebUI", f"{message} [to: Broadcast Channel {channel_idx}]", direct=False, channel_idx=channel_idx)
            info_print(f"[Info] Broadcast on ch={channel_idx} => '{message}'")
            send_broadcast_chunks(interface, message, channel_idx)
            return jsonify({"status": "sent", "to": f"channel {channel_idx}", "message": message})
    except Exception as e:
        print(f"⚠️ Failed to send: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

def connect_interface():
    """Return a Meshtastic interface with the baud rate from config.

    Resolution order:
      1. Wi‑Fi TCP bridge
      2. Local MeshInterface()
      3. USB SerialInterface (explicit path or auto‑detect)
    """
    global connection_status, last_error_message
    try:
        # 1️⃣  Wi‑Fi bridge -------------------------------------------------
        if USE_WIFI and WIFI_HOST and TCPInterface is not None:
            print(f"TCPInterface → {WIFI_HOST}:{WIFI_PORT}")
            connection_status, last_error_message = "Connected", ""
            return TCPInterface(hostname=WIFI_HOST, portNumber=WIFI_PORT)

        # 2️⃣  Local mesh interface ---------------------------------------
        if USE_MESH_INTERFACE and MESH_INTERFACE_AVAILABLE:
            print("MeshInterface() for direct‑radio mode")
            connection_status, last_error_message = "Connected", ""
            return MeshInterface()

        # 3️⃣  USB serial --------------------------------------------------
        # If a serial path is provided, retry opening it with backoff
        if SERIAL_PORT:
            max_attempts = 10
            attempt = 0
            last_exc = None
            print(f"SerialInterface on '{SERIAL_PORT}' (default baud, will switch to {SERIAL_BAUD}) …")
            while attempt < max_attempts:
                attempt += 1
                try:
                    iface = meshtastic.serial_interface.SerialInterface(devPath=SERIAL_PORT)
                    break
                except Exception as e:
                    last_exc = e
                    wait = min(5, 1 + attempt)
                    print(f"⚠️ Attempt {attempt}/{max_attempts} failed to open {SERIAL_PORT}: {e} — retrying in {wait}s")
                    add_script_log(f"Retry {attempt} failed opening serial {SERIAL_PORT}: {e}")
                    time.sleep(wait)
            else:
                # All attempts failed
                msg = str(last_exc) if last_exc is not None else "unknown"
                if "exclusively lock" in msg or "Resource temporarily unavailable" in msg:
                    # escalate so systemd restarts the process to clear any stale FDs
                    raise ExclusiveLockError(f"Could not open serial device {SERIAL_PORT}: {msg}")
                raise RuntimeError(f"Could not open serial device {SERIAL_PORT}: {msg}")
        else:
            print(f"SerialInterface auto‑detect (default baud, will switch to {SERIAL_BAUD}) …")
            iface = meshtastic.serial_interface.SerialInterface()

        # Attempt to change baudrate after opening
        try:
            ser = getattr(iface, "_serial", None)
            if ser is not None and hasattr(ser, "baudrate"):
                ser.baudrate = SERIAL_BAUD
                print(f"Baudrate switched to {SERIAL_BAUD}")
        except Exception as e:
            print(f"⚠️ could not set baudrate to {SERIAL_BAUD}: {e}")

        connection_status, last_error_message = "Connected", ""
        return iface

    except Exception as exc:
        connection_status, last_error_message = "Disconnected", str(exc)
        add_script_log(f"Connection error: {exc}")
        raise

def thread_excepthook(args):
    logging.error(f"Meshtastic thread error: {args.exc_value}")
    traceback.print_exception(args.exc_type, args.exc_value, args.exc_traceback)
    global connection_status
    connection_status = "Disconnected"
    reset_event.set()

threading.excepthook = thread_excepthook

@app.route("/connection_status", methods=["GET"])
def connection_status_route():
    return jsonify({"status": connection_status, "error": last_error_message})

# -----------------------------
# Quiet-Link Keepalive
# -----------------------------
KEEPALIVE_ENABLED = bool(config.get("keepalive_enabled", True))
try:
    KEEPALIVE_CHECK_PERIOD = int(config.get("keepalive_check_period", 10))
except (TypeError, ValueError):
    KEEPALIVE_CHECK_PERIOD = 10
try:
    KEEPALIVE_IDLE_THRESHOLD = int(config.get("keepalive_idle_threshold", 30))
except (TypeError, ValueError):
    KEEPALIVE_IDLE_THRESHOLD = 30
try:
    KEEPALIVE_MIN_INTERVAL = int(config.get("keepalive_min_interval", 15))
except (TypeError, ValueError):
    KEEPALIVE_MIN_INTERVAL = 15

last_keepalive_time = 0.0

def keepalive_worker():
    global last_keepalive_time
    while True:
        try:
            time.sleep(max(5, KEEPALIVE_CHECK_PERIOD))
            if not KEEPALIVE_ENABLED:
                continue
            if CONNECTING_NOW or connection_status != "Connected":
                continue
            now = _now()
            rx_age = (now - last_rx_time) if last_rx_time else None
            tx_age = (now - last_tx_time) if last_tx_time else None
            if rx_age is None or tx_age is None:
                continue
            if rx_age < KEEPALIVE_IDLE_THRESHOLD and tx_age < KEEPALIVE_IDLE_THRESHOLD:
                continue
            if now - last_keepalive_time < KEEPALIVE_MIN_INTERVAL:
                continue
            # Perform a benign serial-only query that does not generate RF
            if interface is not None and hasattr(interface, "getMyNodeInfo"):
                try:
                    interface.getMyNodeInfo()
                    last_keepalive_time = now
                    clean_log("Keepalive tick (serial query only)", "🫶", show_always=False, rate_limit=True)
                except Exception as e:
                    add_script_log(f"Keepalive query failed: {e}")
                    # Do not reset here; let watchdog logic decide
        except Exception:
            time.sleep(10)

def main():
    global interface, restart_count, server_start_time, reset_event
    server_start_time = server_start_time or datetime.now(timezone.utc)
    restart_count += 1
    add_script_log(f"Server restarted. Restart count: {restart_count}")
    clean_log("Starting MESH-MASTER server...", "🚀", show_always=True)
    load_archive()
    
    # Start the async response worker
    start_response_worker()

    if RADIO_STALE_RX_THRESHOLD:
        clean_log(
            f"Radio watchdog armed (stale RX>{RADIO_STALE_RX_THRESHOLD}s)",
            "🛡️",
            show_always=True,
        )
    else:
        clean_log("Radio watchdog RX disabled", "🛡️", show_always=True)

    if RADIO_STALE_TX_THRESHOLD:
        clean_log(
            f"Radio watchdog armed (stale TX>{RADIO_STALE_TX_THRESHOLD}s)",
            "🛡️",
            show_always=True,
        )
    else:
        clean_log("Radio watchdog TX disabled", "🛡️", show_always=True)

    # Determine Flask port: prefer environment `MESH_MASTER_PORT`, then config keys, then default 5000
    flask_port = SERVER_PORT
    clean_log(f"Launching Flask web interface on port {flask_port}...", "🌐", show_always=True)
    api_thread = threading.Thread(
        target=app.run,
        kwargs={"host": "0.0.0.0", "port": flask_port, "debug": False},
        daemon=True,
    )
    api_thread.start()
    # Start keepalive worker to prevent USB idle timeout without RF noise
    threading.Thread(target=keepalive_worker, daemon=True).start()

    # Start monitors (connection watchdog and scheduled refresh)
    threading.Thread(target=connection_monitor, args=(20,), daemon=True).start()
    threading.Thread(target=scheduled_refresh_monitor, daemon=True).start()
    # Heartbeat thread for visibility
    threading.Thread(target=heartbeat_worker, args=(30,), daemon=True).start()

    while True:
        try:
            print("---------------------------------------------------")
            clean_log("Connecting to Meshtastic device...", "🔗", show_always=True, rate_limit=True)
            try:
                pub.unsubscribe(on_receive, "meshtastic.receive")
            except Exception:
                pass
            try:
                if interface:
                    interface.close()
            except Exception:
                pass
            try:
                globals()['CONNECTING_NOW'] = True
            except Exception:
                pass
            interface = connect_interface()
            try:
                globals()['CONNECTING_NOW'] = False
            except Exception:
                pass
            print("Subscribing to on_receive callback...")
            # Only subscribe to the main topic to avoid duplicate callbacks
            pub.subscribe(on_receive, "meshtastic.receive")
            clean_log(f"AI provider: {AI_PROVIDER}", "🧠", show_always=True)
            if HOME_ASSISTANT_ENABLED:
                print(f"Home Assistant multi-mode is ENABLED. Channel index: {HOME_ASSISTANT_CHANNEL_INDEX}")
                if HOME_ASSISTANT_ENABLE_PIN:
                    print("Home Assistant secure PIN protection is ENABLED.")
            clean_log("Connection successful! Running until error or Ctrl+C.", "🟢", show_always=True, rate_limit=True)
            add_script_log("Connection established successfully.")
            # Inner loop: periodically check if a reset has been signaled
            while not reset_event.is_set():
                time.sleep(1)
            raise OSError("Reset event triggered due to connection loss")
        except KeyboardInterrupt:
            print("User interrupted the script. Shutting down.")
            add_script_log("Server shutdown via KeyboardInterrupt.")
            break
        except OSError as e:
            try:
                globals()['CONNECTING_NOW'] = False
            except Exception:
                pass
            error_code = getattr(e, 'errno', None) or getattr(e, 'winerror', None)
            if error_code in (10053, 10054, 10060):
                clean_log("Connection lost! Attempting to reconnect...", "🔄", show_always=True)
                add_script_log(f"Connection forcibly closed: {e} (error code: {error_code})")
                time.sleep(5)
                reset_event.clear()
                continue
            else:
                # Likely a scheduled refresh or generic error; short wait and reconnect
                add_script_log(f"Reconnect requested: {e} (non-socket or scheduled)")
                time.sleep(3)
                reset_event.clear()
                continue
        except Exception as e:
            try:
                globals()['CONNECTING_NOW'] = False
            except Exception:
                pass
            logging.error(f"⚠️ Connection/runtime error: {e}")
            add_script_log(f"Error: {e}")
            print("Will attempt reconnect in 30 seconds...")
            try:
                interface.close()
            except Exception:
                pass
            time.sleep(30)
            reset_event.clear()
            continue

def connection_monitor(initial_delay=30):
    """Monitors connection status and requests reconnects when truly idle.

    Avoids fighting with the active connector by respecting CONNECTING_NOW and
    throttles requests to prevent serial port lock thrash.
    """
    global connection_status
    time.sleep(initial_delay)
    last_request = 0.0
    while True:
        try:
            # Skip if we are actively connecting or a reconnect is already pending
            if CONNECTING_NOW or reset_event.is_set():
                time.sleep(1)
                continue
            if connection_status == "Disconnected":
                now = time.time()
                # Throttle to at most once per 10 seconds
                if now - last_request >= 10:
                    print("⚠️ Connection lost! Triggering reconnect...")
                    reset_event.set()
                    last_request = now
            time.sleep(2)
        except Exception:
            time.sleep(5)

def scheduled_refresh_monitor():
  """Background monitor that triggers a periodic safe refresh of the radio connection.

  We simply set the global reset_event, which the main loop interprets as a signal
  to tear down and reconnect cleanly. This helps avoid subtle memory/socket drift
  over long runtimes.
  """
  # Small startup delay to avoid clashing with first connect
  time.sleep(20)
  if not AUTO_REFRESH_ENABLED:
    return
  interval = max(300, AUTO_REFRESH_MINUTES * 60)
  while True:
    try:
      time.sleep(interval)
      add_script_log(f"Scheduled auto-refresh: requesting reconnect after {AUTO_REFRESH_MINUTES} minutes")
      clean_log("Performing scheduled refresh of radio connection...", "🧽", show_always=True)
      reset_event.set()
    except Exception:
      # Never crash; wait a bit and continue
      time.sleep(60)

# -----------------------------
# Heartbeat & Health Endpoints
# -----------------------------
def heartbeat_worker(period_sec=30):
  global heartbeat_running
  heartbeat_running = True
  while True:
    try:
      now = _now()
      rx_age = (now - last_rx_time) if last_rx_time else None
      tx_age = (now - last_tx_time) if last_tx_time else None
      ai_age = (now - last_ai_response_time) if last_ai_response_time else None
      qsize = 0
      try:
        qsize = response_queue.qsize()
      except Exception:
        qsize = -1
      status = {
        'conn': connection_status,
        'queue': qsize,
        'worker': bool(response_worker_running),
        'rx_age_s': None if rx_age is None else int(rx_age),
        'tx_age_s': None if tx_age is None else int(tx_age),
        'ai_age_s': None if ai_age is None else int(ai_age),
        'msgs': len(messages),
      }
      if connection_status == "Connected" and not CONNECTING_NOW:
        if RADIO_STALE_RX_THRESHOLD and rx_age is not None and rx_age > RADIO_STALE_RX_THRESHOLD:
          trigger_radio_reset(
            f"Radio watchdog: no packets received for {int(rx_age)}s",
            "🛠️",
            debounce_key="stale_rx",
            power_cycle=True,
          )
      # Short, periodic heartbeat log; always show to keep logs alive
      clean_log(f"HB conn={status['conn']} q={status['queue']} rx={status['rx_age_s']}s tx={status['tx_age_s']}s ai={status['ai_age_s']}s", "💓", show_always=True, rate_limit=False)
      periodic_status_update()
      time.sleep(max(5, int(period_sec)))
    except Exception as e:
      print(f"⚠️ Heartbeat error: {e}")
      time.sleep(10)

@app.route("/healthz", methods=["GET"])
def healthz():
  now = _now()
  rx_age = (now - last_rx_time) if last_rx_time else None
  ai_age = (now - last_ai_response_time) if last_ai_response_time else None
  ai_err_age = (now - ai_last_error_time) if ai_last_error_time else None
  qsize = response_queue.qsize()
  data = {
    'ok': True,
    'status': connection_status,
    'queue': qsize,
    'worker': bool(response_worker_running),
    'heartbeat': bool(heartbeat_running),
    'rx_age_s': None if rx_age is None else int(rx_age),
    'ai_age_s': None if ai_age is None else int(ai_age),
    'messages': len(messages),
    'ai_error': ai_last_error,
    'ai_error_age_s': None if ai_err_age is None else int(ai_err_age),
  }
  code = 200
  # Degraded conditions
  if connection_status != "Connected":
    data['ok'] = False
    data['degraded'] = 'radio_disconnected'
    code = 503
  elif qsize > 0 and (ai_age is not None and ai_age > 180):
    data['ok'] = False
    data['degraded'] = 'response_queue_stalled'
    code = 503
  elif ai_err_age is not None and ai_err_age < 120:
    data['ok'] = False
    data['degraded'] = 'ai_provider_recent_error'
    code = 503
  return jsonify(data), code

@app.route("/live", methods=["GET"])
def live():
  return jsonify({'ok': True, 'worker': bool(response_worker_running), 'heartbeat': bool(heartbeat_running)})

@app.route("/ready", methods=["GET"])
def ready():
  ready = (connection_status == "Connected")
  return jsonify({'ok': ready, 'status': connection_status}), (200 if ready else 503)

if __name__ == "__main__":
    # App-level single-instance guard (complements service/script lock)
    acquire_app_lock()
    atexit.register(release_app_lock)
    # Start smooth logging system for pleasant scrolling
    start_smooth_logging()
    
    # Install stderr filter to reduce protobuf noise jitter
    if not DEBUG_ENABLED and CLEAN_LOGS:
        sys.stderr = FilteredStderr(sys.stderr)
        clean_log("Enabled clean logging mode with smooth scrolling", "🌊", show_always=True, rate_limit=False)
    
    while True:
        try:
            main()
        except KeyboardInterrupt:
            print("User interrupted the script. Exiting.")
            stop_response_worker()  # Clean shutdown of worker thread
            stop_smooth_logging()   # Clean shutdown of smooth logging
            add_script_log("Server exited via KeyboardInterrupt.")
            break
        except ExclusiveLockError as e:
            # Fatal: serial port is stuck in exclusive-lock; exit so systemd restarts cleanly
            try:
                import traceback as _tb
                _tb.print_exc()
            except Exception:
                pass
            print(f"❌ Fatal exclusive-lock on serial: {e}")
            add_script_log(f"Fatal exclusive-lock on serial: {e}")
            stop_response_worker()
            stop_smooth_logging()
            # Immediate exit to drop any leaked FDs
            sys.exit(2)
        except Exception as e:
            # Print a clear, unfiltered error with traceback and retry
            try:
                import traceback as _tb
                _tb.print_exc()
            except Exception:
                pass
            print(f"❌ Unhandled error in main: {e}")
            add_script_log(f"Unhandled error in main: {e}")
            stop_response_worker()  # Clean shutdown on error
            # Small delay before retry to avoid hot loop
            time.sleep(5)
            continue
 
