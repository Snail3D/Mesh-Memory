import meshtastic
import meshtastic.serial_interface
from meshtastic import BROADCAST_ADDR
from meshtastic import portnums_pb2
from pubsub import pub
import json
import difflib
import requests
import time
from datetime import datetime, timedelta, timezone  # Added timezone import
import threading
import os
import smtplib
from email.mime.text import MIMEText
import logging
from collections import deque, Counter
import traceback
from flask import Flask, request, jsonify, redirect, url_for, stream_with_context, Response
import sys
import socket  # for socket error checking
import re
import random
import subprocess
from typing import Optional, Set, Dict, Any, List, Tuple
from dataclasses import dataclass, field
from meshtastic_facts import MESHTASTIC_ALERT_FACTS
from twilio.rest import Client  # for Twilio SMS support
from unidecode import unidecode   # Added unidecode import for Ollama text normalization
from google.protobuf.message import DecodeError
import queue  # For async message processing
import atexit
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
    if not DEBUG_ENABLED:
        message = ' '.join(str(arg) for arg in args)
        smooth_print(message)

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

def clean_log(message, emoji="📝", show_always=False, rate_limit=True):
    """Clean, emoji-enhanced logging for better human readability with rate limiting"""
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
    "Discord configuration",
    "Twilio is ",
    "SMTP is ",
    "Launching Flask web interface",
    "Server restarted.",
    "Enabled clean logging mode",
    "System running normally",
    "DISCLAIMER: This is beta software",
    "Messaging Dashboard Access: http://",
  )
  if any(s in line for s in spam):
    return False

  # Whitelist: message-related and important lines
  whitelist_markers = (
    "📨 Message from ",
    "[RX] ",
    "📡 Broadcasting",
    "📤 Sending direct",
    "Sent chunk ",
    "Immediate response:",
    "[AsyncAI]",
    "Processing:",
    "Generated response",
    "Completed response",
    "No response generated",
    "Error processing response",
    "EMERGENCY",
    "Routed Discord message",
    "Polled and routed Discord",
    "[UI] ",
    # AI provider clean_log prefixes with emojis
    "🦙 OLLAMA:",
    "🤖 OPENAI:",
    "💻 LMSTUDIO:",
    "🏠 HOME_ASSISTANT:",
  )
  if any(s in line for s in whitelist_markers):
    return True

  # Always show warnings/errors
  if ("⚠️" in line) or ("❌" in line) or ("ERROR" in line.upper()):
    return True

  # Fallback: hide
  return False

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
            # append a real newline
            f.write(log_entry + "\n")
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

APP_LOCK_FILE = "mesh-ai.app.lock"

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
                print(f"❌ Another mesh-ai instance appears to be running (PID {ep}). Exiting.")
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

MESH-AI v1.0.0 by: MR_TBOT (https://mr-tbot.com)
https://mesh-ai.dev - (https://github.com/mr-tbot/mesh-ai/)
    \033[32m 
Messaging Dashboard Access: http://localhost:5000/dashboard \033[38;5;214m
"""
    "\033[0m"
    "\033[31m"
    """
DISCLAIMER: This is beta software - NOT ASSOCIATED with the official Meshtastic (https://meshtastic.org/) project.
It should not be relied upon for mission critical tasks or emergencies.
Modification of this code for nefarious purposes is strictly frowned upon. Please use responsibly.

(Use at your own risk. For feedback or issues, visit https://mesh-ai.dev or the links above.)
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
PENDING_WEATHER_REQUESTS: Dict[str, Dict[str, Any]] = {}

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
AI_PROVIDER = config.get("ai_provider", "lmstudio").lower()
SYSTEM_PROMPT = config.get("system_prompt", "You are a helpful assistant responding to mesh network chats.")
LMSTUDIO_URL = config.get("lmstudio_url", "http://localhost:1234/v1/chat/completions")
LMSTUDIO_TIMEOUT = config.get("lmstudio_timeout", 60)
LMSTUDIO_CHAT_MODEL = config.get(
    "lmstudio_chat_model",
    "llama-3.2-1b-instruct-uncensored",
)
LMSTUDIO_EMBEDDING_MODEL = config.get(
    "lmstudio_embedding_model",
    "text-embedding-nomic-embed-text-v1.5",	
)	
OPENAI_API_KEY = config.get("openai_api_key", "")
OPENAI_MODEL = config.get("openai_model", "gpt-3.5-turbo")
OPENAI_TIMEOUT = config.get("openai_timeout", 30)
OLLAMA_URL = config.get("ollama_url", "http://localhost:11434/api/generate")
OLLAMA_MODEL = config.get("ollama_model", "llama2")
OLLAMA_TIMEOUT = config.get("ollama_timeout", 60)
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

ENABLE_DISCORD = config.get("enable_discord", False)
DISCORD_WEBHOOK_URL = config.get("discord_webhook_url", None)
DISCORD_SEND_EMERGENCY = config.get("discord_send_emergency", False)
DISCORD_SEND_AI = config.get("discord_send_ai", False)
DISCORD_SEND_ALL = config.get("discord_send_all", False)

ALERT_BELL_KEYWORDS = {
    "🔔 alert bell character!",
    "alert bell character!",
    "alert bell character",
}

try:
    BIBLE_VERSES_DATA = safe_load_json("bible_jesus_verses.json", [])
except Exception:
    BIBLE_VERSES_DATA = []

BIBLE_VERSES_DATA_ES = safe_load_json("bible_jesus_verses_es.json", [])

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

TRAILING_COMMAND_PUNCT = ",.;:!?)]}"

EL_PASO_LAT = 31.761877
EL_PASO_LON = -106.485022
EL_PASO_WEATHER_TTL = 600  # seconds
EL_PASO_WEATHER_API = "https://api.open-meteo.com/v1/forecast"
EL_PASO_WEATHER_CACHE: Dict[str, Any] = {"timestamp": 0.0, "text": None}
WEATHER_DYNAMIC_CACHE: Dict[str, Dict[str, Any]] = {}
WEATHER_CITY_SYNONYMS = {
    "cdmx": "Ciudad de México",
    "ciudad de mexico": "Ciudad de México",
    "mexico df": "Ciudad de México",
    "df": "Ciudad de México",
    "cd juarez": "Ciudad Juárez",
    "cdjuarez": "Ciudad Juárez",
    "juarez": "Ciudad Juárez",
    "gdl": "Guadalajara",
    "guadalajara": "Guadalajara",
    "mty": "Monterrey",
    "monterrey": "Monterrey",
    "tijuana": "Tijuana",
    "tij": "Tijuana",
    "bogota": "Bogotá",
    "bogotá": "Bogotá",
    "santiago": "Santiago",
    "nyc": "New York",
    "new york city": "New York",
    "la": "Los Angeles",
    "los angeles": "Los Angeles",
    "chis": "Chihuahua",
    "chihuahua": "Chihuahua",
}

COMMAND_ALIASES = {
    # English shortcuts / typos
    "/menu": {"canonical": "/menu", "languages": ["en", "es"]},
    "/commands": {"canonical": "/help", "languages": ["en"]},
    "/command": {"canonical": "/help", "languages": ["en"]},
    "/h": {"canonical": "/help", "languages": ["en"]},
    "/bibleverse": {"canonical": "/bible", "languages": ["en"]},
    "/scripture": {"canonical": "/bible", "languages": ["en"]},
    "/verses": {"canonical": "/bible", "languages": ["en"]},
    "/biblefact": {"canonical": "/bible", "languages": ["en"]},
    "/biblefacts": {"canonical": "/bible", "languages": ["en"]},
    "/chuck": {"canonical": "/chucknorris", "languages": ["en"]},
    "/norris": {"canonical": "/chucknorris", "languages": ["en"]},
    "/chuckfacts": {"canonical": "/chucknorris", "languages": ["en"]},
    "/chuckfact": {"canonical": "/chucknorris", "languages": ["en"]},
    "/facts": {"canonical": "/chucknorris", "languages": ["en"]},
    "/blondjoke": {"canonical": "/blond", "languages": ["en"]},
    "/blonde": {"canonical": "/blond", "languages": ["en"]},
    "/blondejoke": {"canonical": "/blond", "languages": ["en"]},
    "/yomama": {"canonical": "/yomomma", "languages": ["en"]},
    "/yomamma": {"canonical": "/yomomma", "languages": ["en"]},
    "/momma": {"canonical": "/yomomma", "languages": ["en"]},
    "/mommajoke": {"canonical": "/yomomma", "languages": ["en"]},
    "/yomommajoke": {"canonical": "/yomomma", "languages": ["en"]},
    "/elp": {"canonical": "/elpaso", "languages": ["en"]},
    "/elpasofact": {"canonical": "/elpaso", "languages": ["en"]},
    "/elpasofacts": {"canonical": "/elpaso", "languages": ["en"]},
    "/where": {"canonical": "/whereami", "languages": ["en"]},
    "/location": {"canonical": "/whereami", "languages": ["en"]},
    "/locate": {"canonical": "/whereami", "languages": ["en"]},
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
    "/mudgame": {"canonical": "/mud", "languages": ["en"]},
    "/cavalry": {"canonical": "/mud", "languages": ["en"]},
    "/adventure": {"canonical": "/mud", "languages": ["en"]},
    "/mudstart": {"canonical": "/mud", "languages": ["en"], "append": " start"},
    "/mudstatus": {"canonical": "/mud", "languages": ["en"], "append": " status"},
    "/mudrestart": {"canonical": "/mud", "languages": ["en"], "append": " restart"},
    "/mudrules": {"canonical": "/mud", "languages": ["en"], "append": " rules"},
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
    "/dondeestoy": {"canonical": "/whereami", "languages": ["es"]},
    "/clima": {"canonical": "/weather", "languages": ["es"]},
    "/tiempo": {"canonical": "/weather", "languages": ["es"]},
    "/pronostico": {"canonical": "/weather", "languages": ["es"]},
    "/mensaje": {"canonical": "/motd", "languages": ["es"]},
    "/mensajedia": {"canonical": "/motd", "languages": ["es"]},
    "/biblia": {"canonical": "/bible", "languages": ["es", "pl", "sw"]},
    "/versiculo": {"canonical": "/bible", "languages": ["es"]},
    "/versiculobiblico": {"canonical": "/bible", "languages": ["es"]},
    "/datoelpaso": {"canonical": "/elpaso", "languages": ["es"]},
    "/hechoelpaso": {"canonical": "/elpaso", "languages": ["es"]},
    "/emergencia": {"canonical": "/emergency", "languages": ["es"]},
    "/cambiarmensaje": {"canonical": "/changemotd", "languages": ["es"]},
    "/cambiaprompt": {"canonical": "/changeprompt", "languages": ["es"]},
    "/verprompt": {"canonical": "/showprompt", "languages": ["es"]},
    "/reiniciar": {"canonical": "/reset", "languages": ["es"]},
    "/enviarsms": {"canonical": "/sms", "languages": ["es"]},
    "/informemalla": {"canonical": "/meshinfo", "languages": ["es"]},
    "/estadomalla": {"canonical": "/meshinfo", "languages": ["es"]},
    "/estadomesh": {"canonical": "/meshinfo", "languages": ["es"]},
    "/bromas": {"canonical": "/jokes", "languages": ["es"]},
    "/chistes": {"canonical": "/jokes", "languages": ["es"]},
    "/aventura": {"canonical": "/mud", "languages": ["es"]},
    "/caballeria": {"canonical": "/mud", "languages": ["es"]},
    "/juego": {"canonical": "/mud", "languages": ["es"]},
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
    "/oujesuis": {"canonical": "/whereami", "languages": ["fr"]},
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
    "/woichbin": {"canonical": "/whereami", "languages": ["de"]},
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
    "/wozainali": {"canonical": "/whereami", "languages": ["zh"]},
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
    "/gdziejestem": {"canonical": "/whereami", "languages": ["pl"]},
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
    "/gdjesam": {"canonical": "/whereami", "languages": ["hr"]},
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
    "/deya": {"canonical": "/whereami", "languages": ["uk"]},
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
    "/nipo_wapi": {"canonical": "/whereami", "languages": ["sw"]},
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
    "/jokes",
    "/mud",
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
        "title": {
            "en": "Main Menu - choose a tag to open a section",
            "es": "Menú principal - elige una etiqueta para abrir una sección",
        },
        "sections": [
            {
                "title": {"en": "Getting started", "es": "Para comenzar"},
                "items": [
                    ("/help", {"en": "Complete list of commands.", "es": "Lista completa de comandos."}),
                    ("/whereami", {"en": "Check your last known position.", "es": "Revisa tu última ubicación conocida."}),
                    ("/weather <city>", {"en": "Quick weather briefing (default: El Paso).", "es": "Reporte rápido del clima (predeterminado: El Paso)."}),
                    ("/motd", {"en": "Current message of the day.", "es": "Mensaje del día actual."}),
                ],
            },
            {
                "title": {"en": "Story & fun", "es": "Historia y diversión"},
                "items": [
                    ("/mud", {"en": "Cavalry choose-your-own-adventure in 1850s El Paso.", "es": "Aventura interactiva de caballería en El Paso de 1850."}),
                    ("/jokes", {"en": "Humor submenu: Chuck Norris, blond, yo momma.", "es": "Submenú de humor: Chuck Norris, rubias, tu mamá."}),
                    ("/bible", {"en": "Verse focused on Jesus and hope.", "es": "Versículo centrado en Jesús y la esperanza."}),
                    ("/elpaso", {"en": "Local fact from the El Paso archives.", "es": "Dato local de los archivos de El Paso."}),
                    ("/bibletrivia", {"en": "Score-keeping Bible trivia challenges.", "es": "Trivia bíblica con marcador."}),
                    ("/disastertrivia", {"en": "Disaster preparedness quiz with scoring.", "es": "Trivia de preparación ante desastres con puntaje."}),
                    ("/trivia", {"en": "General knowledge and riddles with live scoreboard.", "es": "Trivia general y acertijos con puntuación."}),
                ],
            },
            {
                "title": {"en": "Preparedness", "es": "Preparación"},
                "items": [
                    ("/survival", {"en": "Survival scenarios and medical guide.", "es": "Escenarios de supervivencia y guía médica."}),
                    ("/meshinfo", {"en": "Mesh network health snapshot.", "es": "Estado de la red mesh."}),
                    ("/emergency", {"en": "Broadcast an urgent alert.", "es": "Envía una alerta urgente."}),
                ],
            },
            {
                "title": {"en": "Skill trainers", "es": "Entrenadores de habilidades"},
                "items": [
                    ("/morsecodetrainer", {"en": "Short Morse code drills and challenges.", "es": "Ejercicios cortos de código Morse."}),
                    ("/hurricanetrainer", {"en": "Hurricane prep: pre, during, and post checklists.", "es": "Entrenador para huracanes: antes, durante y después."}),
                    ("/tornadotrainer", {"en": "Tornado safety rehearsal guidance.", "es": "Guía para ensayar seguridad ante tornados."}),
                    ("/radioprocedurestrainer", {"en": "Emergency radio procedure drills.", "es": "Entrenador de procedimientos de radio de emergencia."}),
                    ("/navigationtrainer", {"en": "Navigate without a compass practice routines.", "es": "Rutinas para navegar sin brújula."}),
                    ("/boatingtrainer", {"en": "Boating safety briefings and drills.", "es": "Entrenador de seguridad náutica."}),
                    ("/wellnesstrainer", {"en": "Emergency wellness for pets and homes in long outages.", "es": "Bienestar en emergencias para mascotas y hogar."}),
                ],
            },
        ],
        "footer": {
            "en": "Tip: enter the tag (for example /survival) to open that submenu.",
            "es": "Consejo: escribe la etiqueta (por ejemplo /survival) para abrir ese submenú.",
        },
    },
    "jokes": {
        "title": {"en": "Humor submenu", "es": "Submenú de humor"},
        "sections": [
            {
                "title": {"en": "Pick a flavor", "es": "Elige un estilo"},
                "items": [
                    ("/chucknorris", {"en": "Legendary Chuck Norris fact.", "es": "Dato legendario de Chuck Norris."}),
                    ("/blond", {"en": "Light-hearted blond joke.", "es": "Chiste ligero de rubias."}),
                    ("/yomomma", {"en": "Classic yo momma joke.", "es": "Chiste clásico de tu mamá."}),
                ],
            },
        ],
        "footer": {
            "en": "Need more laughs? Try adding your own with /funfact <topic>.",
            "es": "¿Quieres más risas? Agrega las tuyas con /funfact <tema>.",
        },
    },
    "survival": {
        "title": {"en": "Survival submenu", "es": "Submenú de supervivencia"},
        "sections": [
            {
                "title": {"en": "Choose a scenario", "es": "Elige un escenario"},
                "items": [
                    ("/survival_desert", {"en": "Beat the heat and ration water wisely.", "es": "Supera el calor y raciona el agua con sabiduría."}),
                    ("/survival_urban", {"en": "Navigate cities during disruption.", "es": "Navega la ciudad durante una crisis."}),
                    ("/survival_jungle", {"en": "Stay dry, avoid hazards, find clean water.", "es": "Mantente seco, evita riesgos y encuentra agua limpia."}),
                    ("/survival_woodland", {"en": "Use forests for cover, food, and orientation.", "es": "Usa el bosque para cobertura, comida y orientación."}),
                    ("/survival_winter", {"en": "Fight hypothermia and manage snow shelter.", "es": "Combate la hipotermia y gestiona refugios en nieve."}),
                    ("/survival_medical", {"en": "Field-ready first aid essentials.", "es": "Primeros auxilios esenciales en campo."}),
                ],
            },
        ],
        "footer": {
            "en": "Carry these notes offline and share them freely.",
            "es": "Lleva estas notas sin conexión y compártelas con quien lo necesite.",
        },
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

CAVALRY_SCENE_ICONS = {
    "fort_bliss_muster": "🏇",
    "chapel_reflection": "🕯️",
    "rio_patrol": "🌊",
    "mountain_recon": "🗻",
    "mesilla_market": "🛍️",
    "contraband_shootout": "⚠️",
}

CAVALRY_ASCII_BANNER = (
    "    /\\",
    "   /::\\   Frontier Riders",
    "  /::::\\  Keep hope alive",
    " /::::::\\ Choose the peaceful trail",
    "/::::::::\\"
)

CAVALRY_CHOICE_MARKERS = ("🌵", "🪶", "🛤️", "🔥")

CAVALRY_INTRO_LINES = {
    "en": [
        "🏜️ Scenario: 1858 Fort Bliss cavalry patrols guarding the new border around El Paso.",
        "🧭 How to move: reply with `/mud <number>` to follow a choice, `/mud status` to reread, `/mud restart` to begin anew.",
        "🎖️ Goal: gather gold with integrity—mercy often unlocks redemption. Use `/mud rules` for full guidance.",
    ],
    "es": [
        "🏜️ Escenario: caballería de Fort Bliss en 1858 cuidando la nueva frontera de El Paso.",
        "🧭 Cómo moverte: responde con `/mud <numero>` para escoger, `/mud status` para releer, `/mud restart` para reiniciar.",
        "🎖️ Meta: reúne oro con integridad; la misericordia abre finales de redención. Usa `/mud rules` para la guía completa.",
    ],
}

CAVALRY_RULES_TEXT = {
    "en": "MUD Rules:\n- 📅 Timeline: 1858 Fort Bliss cavalry posts across the Rio Grande valley.\n- 🎯 Aim: collect gold ethically; decisions adjust your Gold and Integrity meters. Integrity below zero risks bleak endings.\n- 🧭 Navigation: `/mud start`, then answer with `/mud <number>`. Use `/mud status` to reread, `/mud restart` for a new run, `/mud rules` for this recap.\n- 🤝 Etiquette: wait for each reply; chunks pace about every 5s to respect the mesh bandwidth.",
    "es": "Reglas del MUD:\n- 📅 Época: caballería de Fort Bliss en 1858 a lo largo del valle del Río Grande.\n- 🎯 Objetivo: reunir oro con ética; las decisiones ajustan tus medidores de Oro e Integridad. Con integridad negativa llegan finales duros.\n- 🧭 Navegación: `/mud start` y luego responde con `/mud <numero>`. Usa `/mud status` para releer, `/mud restart` para reiniciar, `/mud rules` para este resumen.\n- 🤝 Etiqueta: espera cada respuesta; los fragmentos se envían cada 5 s para cuidar el ancho de banda de la malla.",
}

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
        section_title = section.get("title", {}).get(lang) or section.get("title", {}).get("en")
        if lines:
            lines.append("")
        if section_title:
            lines.append(section_title)
        for command, desc_map in section.get("items", []):
            description = desc_map.get(lang) or desc_map.get("en") or ""
            lines.append(f"  {command} - {description}")
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


@dataclass
class CavalryGameState:
    player_key: str
    current_scene: str
    gold: int = 0
    integrity: int = 3
    history: List[str] = field(default_factory=list)
    completed: bool = False
    language: str = "en"
    start_scene: str = ""
    last_note: Optional[str] = None
    intro_shown: bool = False


def _cavalry_language(language: Optional[str]) -> str:
    return _preferred_menu_language(language)


def _cavalry_player_key(sender_id: str, is_direct: bool, channel_idx: Optional[int]) -> str:
    channel_label = "DM" if is_direct else f"CH{channel_idx if channel_idx is not None else 'broadcast'}"
    return f"{sender_id}#{channel_label}"


CAVALRY_STATE_FILE = "cavalry_game_states.json"
CAVALRY_STATE_LOCK = threading.Lock()


CAVALRY_SCENES = {
    "fort_bliss_muster": {
        "start": True,
        "title": {
            "en": "Parade Ground at Fort Bliss, 1858",
            "es": "Plaza de armas de Fort Bliss, 1858",
        },
        "text": {
            "en": "Magoffinsville hums with bugles and dust as the relocated post readies Dragoons and cavalry troopers. The Franklin Mountains watch over the adobe buildings and cottonwood shade. Captain orders riders to choose their duty for the week.",
            "es": "Magoffinsville vibra entre cornetas y polvo mientras el puesto trasladado alista a dragones y jinetes. Las montanas Franklin vigilan los edificios de adobe y los alamos. El capitan ordena a los soldados elegir su deber para la semana.",
        },
        "fact": {
            "en": "Fort Bliss moved near Magoffinsville in 1854 to guard the new US-Mexico boundary set by the Treaty of Guadalupe Hidalgo.",
            "es": "Fort Bliss se traslado cerca de Magoffinsville en 1854 para proteger la nueva frontera nacida del Tratado de Guadalupe Hidalgo.",
        },
        "choices": [
            {
                "description": {
                    "en": "Inspect the quartermaster wagons arriving from San Antonio.",
                    "es": "Inspeccionar los vagones del intendente llegados de San Antonio.",
                },
                "next": "supply_yard",
                "effects": {"gold": 2, "note": {
                    "en": "You log two dollars in spare tack legally redistributed to the unit.",
                    "es": "Registras dos dolares en equipo sobrante repartido legalmente a la unidad.",
                }},
            },
            {
                "description": {
                    "en": "Attend evening prayer with Chaplain Lathrop before night guard.",
                    "es": "Asistir a la oracion vespertina con el capellan Lathrop antes de la guardia nocturna.",
                },
                "next": "chapel_reflection",
                "effects": {"integrity": 1, "note": {
                    "en": "Quiet hymns steady your resolve to serve with mercy.",
                    "es": "Los himnos tranquilos afianzan tu decision de servir con misericordia.",
                }},
            },
            {
                "description": {
                    "en": "Volunteer for a Rio Grande boundary patrol toward Ysleta.",
                    "es": "Ser voluntario para una patrulla del rio Grande rumbo a Ysleta.",
                },
                "next": "rio_patrol",
                "effects": {"gold": 1, "note": {
                    "en": "The adjutant slips you a bonus coin for quick readiness.",
                    "es": "El ayudante te entrega una moneda extra por la rapidez.",
                }},
            },
            {
                "description": {
                    "en": "Ride with scouts into the Franklin Mountains to chart passes.",
                    "es": "Cabalgar con los exploradores en las montanas Franklin para trazar pasos.",
                },
                "next": "mountain_recon",
                "effects": {"note": {
                    "en": "You saddle the grey mare famed for sure footing on shale.",
                    "es": "Ensillas a la yegua gris famosa por su pisada firme en la pizarra.",
                }},
            },
        ],
        "start_aliases": ["fort", "muster", "bliss"],
    },
    "chapel_reflection": {
        "title": {
            "en": "Adobe Chapel Beside the Parade",
            "es": "Capilla de adobe junto a la plaza",
        },
        "text": {
            "en": "Chaplain Lathrop recounts how the garrison tends not only sabers but souls. Candles flicker against earthen walls as you kneel among troopers weary from frontier rides.",
            "es": "El capellan Lathrop recuerda que la guarnicion cuida no solo sables sino almas. Las velas titilan contra las paredes de tierra mientras te arrodillas entre jinetes cansados de la frontera.",
        },
        "choices": [
            {
                "description": {
                    "en": "Write letters home for privates who cannot read.",
                    "es": "Escribir cartas para los soldados que no saben leer.",
                },
                "next": "mesilla_market",
                "effects": {"gold": 1, "integrity": 1, "note": {
                    "en": "Families send gratitude coins tucked in Mesilla parcels.",
                    "es": "Las familias envian monedas de agradecimiento en paquetes de Mesilla.",
                }},
            },
            {
                "description": {
                    "en": "Carry hymnbooks to the hospital tents on the riverbank.",
                    "es": "Llevar himnarios a las carpas del hospital en la ribera.",
                },
                "next": "river_hospital",
                "effects": {"integrity": 2, "note": {
                    "en": "Patients whisper thanks as scripture gives them courage.",
                    "es": "Los pacientes susurran gracias mientras la Escritura les da valor.",
                }},
            },
            {
                "description": {
                    "en": "Return to the parade ground renewed for duty.",
                    "es": "Regresar a la plaza renovado para el servicio.",
                },
                "next": "fort_bliss_muster",
                "effects": {"integrity": 1, "note": {
                    "en": "Your calm demeanor lifts the morale of the watch detail.",
                    "es": "Tu calma eleva la moral de la guardia.",
                }},
            },
        ],
    },
    "supply_yard": {
        "title": {
            "en": "Quartermaster Yard at Magoffinsville",
            "es": "Patio del intendente en Magoffinsville",
        },
        "text": {
            "en": "Crates marked SAN ANTONIO arrive with oats, repeater parts, and mail. Sergeant Juarez notes the Butterfield Overland Mail will depend on honest tallies.",
            "es": "Cajas marcadas SAN ANTONIO traen avena, piezas de repetidor y correo. El sargento Juarez recuerda que la ruta Butterfield depende de cuentas honestas.",
        },
        "choices": [
            {
                "description": {
                    "en": "Audit the grain and share surplus with acequia farmers at Ysleta.",
                    "es": "Auditar el grano y compartir el excedente con los agricultores de la acequia en Ysleta.",
                },
                "next": "ysleta_farms",
                "effects": {"gold": 3, "integrity": 1, "note": {
                    "en": "The farmers repay you with a pouch of trade pesos.",
                    "es": "Los agricultores te devuelven una bolsa de pesos de trueque.",
                }},
            },
            {
                "description": {
                    "en": "Sell captured muskets to a Comanchero trader lurking nearby.",
                    "es": "Vender mosquetes capturados a un comerciante comanchero cercano.",
                },
                "next": "outlaw_camp",
                "effects": {"gold": 4, "integrity": -2, "note": {
                    "en": "Gold jingles, yet your conscience notes the weapons may spill innocent blood.",
                    "es": "Oro tintinea, pero tu conciencia advierte que las armas podrian causar sangre inocente.",
                }},
            },
            {
                "description": {
                    "en": "Volunteer to break remount horses for the frontier companies.",
                    "es": "Ofrecerte para domar caballos de reemplazo para las companias de frontera.",
                },
                "next": "livestock_care",
                "effects": {"gold": 2, "integrity": 1, "note": {
                    "en": "You earn hazard pay and the respect of the remount sergeant.",
                    "es": "Ganas paga de riesgo y el respeto del sargento de remonta.",
                }},
            },
        ],
    },
    "livestock_care": {
        "title": {
            "en": "Remount Corrals on the Franklin Foothills",
            "es": "Corrales de remonta en las faldas Franklin",
        },
        "text": {
            "en": "Spooked cavalry mounts snort as you brush them down. Fort Bliss relies on steady horses to patrol the Chihuahua Trail and the Camino Real.",
            "es": "Las monturas se agitan mientras las cepillas. Fort Bliss depende de caballos firmes para patrullar el Camino Real y la ruta a Chihuahua.",
        },
        "choices": [
            {
                "description": {
                    "en": "Lead the herd to a hidden spring the Mescalero scouts mentioned.",
                    "es": "Guiar la manada a un manantial oculto que mencionaron los exploradores mescaleros.",
                },
                "next": "hidden_spring",
                "effects": {"gold": 1, "note": {
                    "en": "Cool water keeps the horses strong for coming rides.",
                    "es": "El agua fresca mantiene fuertes a los caballos.",
                }},
            },
            {
                "description": {
                    "en": "Teach recruits humane handling before night picket duty.",
                    "es": "Ensenar a los reclutas trato humano antes de la guardia nocturna.",
                },
                "next": "peace_camp_end",
                "effects": {"integrity": 2, "note": {
                    "en": "Their gratitude reminds you that gentleness calms the frontier.",
                    "es": "Su gratitud te recuerda que la mansedumbre calma la frontera.",
                }},
            },
            {
                "description": {
                    "en": "Report the herd condition to headquarters for extra pay.",
                    "es": "Informar el estado de la manada al cuartel para obtener paga extra.",
                },
                "next": "gold_tally_end",
                "effects": {"gold": 3, "note": {
                    "en": "The paymaster credits you for safeguarding army property.",
                    "es": "El pagador te acredita por proteger bienes del ejercito.",
                }},
            },
        ],
    },
    "rio_patrol": {
        "start": True,
        "title": {
            "en": "Rio Grande Boundary Patrol",
            "es": "Patrulla de la frontera del rio Grande",
        },
        "text": {
            "en": "Dusty levees mark the 1848 line. Farmers from Ysleta and El Paso del Norte trade news of bandits and cross-border tensions. Your squad watches the ferry crossings.",
            "es": "Los diques polvorientos marcan la linea de 1848. Agricultores de Ysleta y El Paso del Norte comparten noticias de bandoleros y tensiones fronterizas. Tu escuadron vigila los cruces de balsa.",
        },
        "fact": {
            "en": "The Treaty of Guadalupe Hidalgo made the Rio Grande the international boundary, demanding new patrols in the 1850s.",
            "es": "El Tratado de Guadalupe Hidalgo hizo del rio Grande la frontera internacional, exigiendo nuevas patrullas en la decada de 1850.",
        },
        "choices": [
            {
                "description": {
                    "en": "Reassure Ysleta farmers and inspect their acequia gates.",
                    "es": "Reafirmar a los agricultores de Ysleta e inspeccionar sus compuertas.",
                },
                "next": "ysleta_farms",
                "effects": {"integrity": 1, "note": {
                    "en": "They share tamales and stories of the Tigua mission.",
                    "es": "Comparten tamales e historias de la mision Tigua.",
                }},
            },
            {
                "description": {
                    "en": "Investigate Comanchero smugglers sighted near the sand hills.",
                    "es": "Investigar a los contrabandistas comancheros vistos cerca de las dunas.",
                },
                "next": "contraband_shootout",
                "effects": {"gold": 1, "note": {
                    "en": "Your patrol pockets cartridge bounties before the chase.",
                    "es": "La patrulla guarda recompensas de cartuchos antes de la persecucion.",
                }},
            },
            {
                "description": {
                    "en": "Hold ferry watch near El Paso del Norte with the customs agent.",
                    "es": "Vigilar el transbordador cerca de El Paso del Norte con el agente de aduanas.",
                },
                "next": "river_hospital",
                "effects": {"integrity": 1, "note": {
                    "en": "A fever outbreak diverts you toward compassion duty.",
                    "es": "Un brote de fiebre te desvía hacia el deber de compasion.",
                }},
            },
        ],
        "start_aliases": ["rio", "patrol", "river"],
    },
    "ysleta_farms": {
        "title": {
            "en": "Acequias of Ysleta del Sur",
            "es": "Acequias de Ysleta del Sur",
        },
        "text": {
            "en": "Tigua elders welcome you with blue corn at the 1680s mission. Irrigation channels need repairs after spring floods.",
            "es": "Los ancianos Tigua te reciben con maiz azul en la mision de 1680. Las acequias requieren reparacion tras las crecidas de primavera.",
        },
        "choices": [
            {
                "description": {
                    "en": "Organize a fair water rotation and pray with the farmers.",
                    "es": "Organizar un reparto justo de agua y orar con los agricultores.",
                },
                "next": "peace_camp_end",
                "effects": {"gold": 4, "integrity": 1, "note": {
                    "en": "They gift silver pesos and promise to alert you of raids.",
                    "es": "Te obsequian pesos de plata y prometen avisar sobre incursiones.",
                }},
            },
            {
                "description": {
                    "en": "Buy chile ristras to resell at the post canteen.",
                    "es": "Comprar ristras de chile para revender en la cantina del puesto.",
                },
                "next": "mesilla_market",
                "effects": {"gold": 3, "note": {
                    "en": "Spice sales promise tidy profit on payday.",
                    "es": "Las ventas picantes prometen ganancias en dia de paga.",
                }},
            },
            {
                "description": {
                    "en": "Return to the river patrol line before nightfall.",
                    "es": "Regresar a la linea de patrulla antes del anochecer.",
                },
                "next": "rio_patrol",
                "effects": {"note": {
                    "en": "You carry Tigua blessings back to camp.",
                    "es": "Llevas bendiciones Tigua de regreso al campamento.",
                }},
            },
        ],
    },
    "contraband_shootout": {
        "title": {
            "en": "Skirmish at the Sand Hills",
            "es": "Escaramuza en las dunas",
        },
        "text": {
            "en": "Shots crack as Comanchero riders trade lead for kegs of black powder. A Mescalero scout watches from the ridgeline, uncertain whether to join.",
            "es": "Los disparos resuenan mientras jinetes comancheros intercambian plomo por barriles de polvora. Un explorador mescalero observa desde la cresta, dudando si unirse.",
        },
        "choices": [
            {
                "description": {
                    "en": "Call for parley, trading blankets for their surrender.",
                    "es": "Pedir parlamento ofreciendo mantas a cambio de su rendicion.",
                },
                "next": "peace_camp_end",
                "effects": {"gold": 2, "integrity": 2, "note": {
                    "en": "A peaceful surrender spares lives and earns you commendation.",
                    "es": "Una rendicion pacifica salva vidas y te gana elogios.",
                }},
            },
            {
                "description": {
                    "en": "Fire warning shots and seize their contraband outright.",
                    "es": "Disparar de advertencia y confiscar el contrabando.",
                },
                "next": "gold_tally_end",
                "effects": {"gold": 5, "integrity": -1, "note": {
                    "en": "Spoils pile high, yet rumors spread of harsh tactics.",
                    "es": "El botin se amontona, pero corren rumores de tacticas duras.",
                }},
            },
            {
                "description": {
                    "en": "Pursue the Mescalero scout into the foothills.",
                    "es": "Perseguir al explorador mescalero hacia las lomas.",
                },
                "next": "apache_encounter",
                "effects": {"note": {
                    "en": "You chase hoofprints toward the Franklin ridges.",
                    "es": "Sigues huellas hacia las crestas Franklin.",
                }},
            },
        ],
    },
    "mesilla_market": {
        "title": {
            "en": "Market Square at Mesilla",
            "es": "Plaza del mercado en Mesilla",
        },
        "text": {
            "en": "The 1850s plaza bustles with traders selling copper ore, beef, and rosaries. News of the Gadsden Purchase still shapes loyalties across the valley.",
            "es": "La plaza de 1850 bulle con comerciantes de cobre, carne y rosarios. La Compra de Gadsden aun moldea las lealtades del valle.",
        },
        "choices": [
            {
                "description": {
                    "en": "Invest in a prospector's claim near the Organ Mountains.",
                    "es": "Invertir en una concesion minera cerca de las montanas Organ.",
                },
                "next": "mining_claim",
                "effects": {"gold": 1, "note": {
                    "en": "A miner hands you a share certificate payable in dusted nuggets.",
                    "es": "Un minero te entrega un certificado pagadero en pepitas.",
                }},
            },
            {
                "description": {
                    "en": "Donate half your pay to rebuild the Socorro chapel roof.",
                    "es": "Donar la mitad de tu paga para reparar el techo de la capilla de Socorro.",
                },
                "next": "peace_camp_end",
                "effects": {"gold": -1, "integrity": 2, "note": {
                    "en": "The priest blesses you, reminding that treasure serves people.",
                    "es": "El sacerdote te bendice recordando que el tesoro sirve a la gente.",
                }},
            },
            {
                "description": {
                    "en": "Carry the supplies back to Fort Bliss before reveille.",
                    "es": "Llevar los suministros de regreso a Fort Bliss antes de la diana.",
                },
                "next": "fort_bliss_muster",
                "effects": {"note": {
                    "en": "Your pack mules clatter across the Camino Real.",
                    "es": "Tus mulas resuenan sobre el Camino Real.",
                }},
            },
        ],
    },
    "mining_claim": {
        "title": {
            "en": "Claim Shacks near the Organ Mountains",
            "es": "Campamentos mineros en las montanas Organ",
        },
        "text": {
            "en": "Prospectors pan for placer gold where rumors say Apache once traded for copper. Your investment partners await your decision.",
            "es": "Los buscadores lavan oro aluvial donde se dice que los apaches comerciaban cobre. Tus socios esperan tu decision.",
        },
        "choices": [
            {
                "description": {
                    "en": "Work alongside Mexican vaqueros, splitting the yield equally.",
                    "es": "Trabajar junto a vaqueros mexicanos, dividiendo el rendimiento por igual.",
                },
                "next": "gold_tally_end",
                "effects": {"gold": 6, "integrity": 1, "note": {
                    "en": "Shared labor teaches respect and fills your saddlebag with honest ore.",
                    "es": "El trabajo compartido ensena respeto y llena tus alforjas con mineral honesto.",
                }},
            },
            {
                "description": {
                    "en": "Hire mercenaries to drive away nearby Mescalero families.",
                    "es": "Contratar mercenarios para expulsar a familias mescaleras cercanas.",
                },
                "next": "apache_encounter",
                "effects": {"integrity": -2, "note": {
                    "en": "Fear shadows the claim as displaced families vanish into the hills.",
                    "es": "El temor cubre la mina mientras las familias desplazadas desaparecen en las colinas.",
                }},
            },
            {
                "description": {
                    "en": "Sell the claim and fund a schoolroom at the mission.",
                    "es": "Vender la concesion y financiar un aula en la mision.",
                },
                "next": "peace_camp_end",
                "effects": {"gold": 4, "integrity": 2, "note": {
                    "en": "Children will learn to read with the wealth you redirected.",
                    "es": "Los ninos aprenderan a leer con la riqueza que redirigiste.",
                }},
            },
        ],
    },
    "mountain_recon": {
        "start": True,
        "title": {
            "en": "Franklin Mountain Recon Patrol",
            "es": "Patrulla de reconocimiento en las montanas Franklin",
        },
        "text": {
            "en": "High basalt ridges hide caves and springs. Mescalero Apache scouts know every pass, and a storm brews toward Hueco Tanks.",
            "es": "Las crestas de basalto ocultan cuevas y manantiales. Los exploradores mescaleros conocen cada paso y se acerca una tormenta hacia Hueco Tanks.",
        },
        "choices": [
            {
                "description": {
                    "en": "Follow the old Mescalero trail toward a hidden spring.",
                    "es": "Seguir la antigua senda mescalera hacia un manantial oculto.",
                },
                "next": "hidden_spring",
                "effects": {"note": {
                    "en": "You mark petroglyphs that guide your path.",
                    "es": "Marcas petroglifos que guian tu camino.",
                }},
            },
            {
                "description": {
                    "en": "Shadow a rumored bandit camp near Hueco Tanks.",
                    "es": "Seguir un campamento de bandidos cerca de Hueco Tanks.",
                },
                "next": "outlaw_camp",
                "effects": {"gold": 1, "note": {
                    "en": "Scavenged spurs jingle as you track the outlaws.",
                    "es": "Espuelas recuperadas tintinean mientras rastreas a los bandidos.",
                }},
            },
            {
                "description": {
                    "en": "Survey routes for the Butterfield Overland Mail engineers.",
                    "es": "Levant ar rutas para los ingenieros del Butterfield Overland Mail.",
                },
                "next": "stagecoach_run",
                "effects": {"integrity": 1, "note": {
                    "en": "Your maps may speed the mail between St. Louis and San Francisco.",
                    "es": "Tus mapas podrian acelerar el correo entre St. Louis y San Francisco.",
                }},
            },
        ],
        "start_aliases": ["mountain", "scout", "franklin"],
    },
    "hidden_spring": {
        "title": {
            "en": "Hidden Spring below Mount Cristo Rey",
            "es": "Manantial oculto bajo el monte Cristo Rey",
        },
        "text": {
            "en": "Water seeps from volcanic rock, feeding sotol and cottonwood. Mescalero scouts sometimes leave offerings here for safe passage.",
            "es": "El agua brota de la roca volcanica alimentando sotol y alamos. Los exploradores mescaleros dejan ofrendas para un paso seguro.",
        },
        "choices": [
            {
                "description": {
                    "en": "Invite nearby scouts to share water and trade news.",
                    "es": "Invitar a los exploradores cercanos a compartir agua y noticias.",
                },
                "next": "peace_camp_end",
                "effects": {"gold": 2, "integrity": 2, "note": {
                    "en": "Trust grows and they guide you to safer canyons.",
                    "es": "La confianza crece y te guian a canones mas seguros.",
                }},
            },
            {
                "description": {
                    "en": "Chart the spring precisely and report to headquarters.",
                    "es": "Trazar el manantial con precision y reportarlo al cuartel.",
                },
                "next": "gold_tally_end",
                "effects": {"gold": 3, "note": {
                    "en": "Your map earns an exploration bonus.",
                    "es": "Tu mapa obtiene una bonificacion de exploracion.",
                }},
            },
            {
                "description": {
                    "en": "Explore deeper toward the Organ Mountains.",
                    "es": "Explorar mas a fondo hacia las montanas Organ.",
                },
                "next": "outlaw_camp",
                "effects": {"note": {
                    "en": "Storm clouds gather as you press farther north.",
                    "es": "Nubes de tormenta se reúnen mientras avanzas al norte.",
                }},
            },
        ],
    },
    "outlaw_camp": {
        "title": {
            "en": "Bandit Hideout near Hueco Tanks",
            "es": "Guarida de bandidos cerca de Hueco Tanks",
        },
        "text": {
            "en": "Stagecoach robbers warm beans over a mesquite fire. They clutch coins stolen from the Butterfield Overland strongbox.",
            "es": "Ladrones de diligencias calientan frijoles sobre mezquite. Empunan monedas del cofre del Butterfield Overland.",
        },
        "choices": [
            {
                "description": {
                    "en": "Listen unseen to learn who they plan to rob next.",
                    "es": "Escuchar sin ser visto para saber a quien planean asaltar.",
                },
                "next": "stagecoach_run",
                "effects": {"integrity": 1, "note": {
                    "en": "You overhear a plot against the next mail coach.",
                    "es": "Escuchas un complot contra la proxima diligencia.",
                }},
            },
            {
                "description": {
                    "en": "Charge the camp, sabers flashing in the moonlight.",
                    "es": "Cargar contra el campamento con sables al claro de luna.",
                },
                "next": "gold_tally_end",
                "effects": {"gold": 4, "integrity": -2, "note": {
                    "en": "Victory is swift, yet wounded bandits cry out for mercy.",
                    "es": "La victoria es rapida, pero los bandidos heridos claman misericordia.",
                }},
            },
            {
                "description": {
                    "en": "Offer them amnesty through the chaplain if they lay down arms.",
                    "es": "Ofrecer amnistia mediante el capellan si depone las armas.",
                },
                "next": "peace_camp_end",
                "effects": {"gold": 1, "integrity": 2, "note": {
                    "en": "Several accept, handing over loot for restitution.",
                    "es": "Varios aceptan y entregan el botin para restitucion.",
                }},
            },
        ],
    },
    "apache_encounter": {
        "title": {
            "en": "Mescalero Camp on the Foothills",
            "es": "Campamento mescalero en las lomas",
        },
        "text": {
            "en": "A small Mescalero band observes you warily. Elders remember treaties broken, yet a young scout studies the Christian medallion on your chest.",
            "es": "Una pequena banda mescalera te observa con cautela. Los mayores recuerdan tratados rotos, pero un joven explorador mira el medallon cristiano en tu pecho.",
        },
        "choices": [
            {
                "description": {
                    "en": "Lower your carbine and share coffee beside the fire.",
                    "es": "Bajar el fusil y compartir cafe junto al fuego.",
                },
                "next": "peace_camp_end",
                "effects": {"gold": 1, "integrity": 3, "note": {
                    "en": "Stories of hardship are traded, and a new friendship is forged.",
                    "es": "Intercambian historias de dificultad y nace una amistad.",
                }},
            },
            {
                "description": {
                    "en": "Set a pre-dawn trap to capture their ponies.",
                    "es": "Tender una trampa antes del amanecer para capturar sus ponis.",
                },
                "next": "gold_tally_end",
                "effects": {"gold": 5, "integrity": -3, "note": {
                    "en": "You seize livestock, yet guilt weighs heavier than the saddlebags.",
                    "es": "Capturas ganado, pero la culpa pesa mas que las alforjas.",
                }},
            },
            {
                "description": {
                    "en": "Ask them to guide you through safer mountain passes.",
                    "es": "Pedir que te guien por pasos mas seguros.",
                },
                "next": "hidden_spring",
                "effects": {"gold": 2, "integrity": 1, "note": {
                    "en": "They accept gifts and share knowledge of the land.",
                    "es": "Aceptan obsequios y comparten conocimiento del territorio.",
                }},
            },
        ],
    },
    "stagecoach_run": {
        "title": {
            "en": "Butterfield Overland Escort",
            "es": "Escolta del Butterfield Overland",
        },
        "text": {
            "en": "The Overland Mail began service in 1858, racing through El Paso with passengers bound from St. Louis to San Francisco. Drivers beg for protection over the Jornada del Muerto stretch.",
            "es": "El Overland Mail inicio servicio en 1858, cruzando El Paso con pasajeros de St. Louis a San Francisco. Los conductores suplican proteccion en la Jornada del Muerto.",
        },
        "choices": [
            {
                "description": {
                    "en": "Escort the coach through San Elizario and back to the fort.",
                    "es": "Escoltar la diligencia por San Elizario y regresar al fuerte.",
                },
                "next": "gold_tally_end",
                "effects": {"gold": 4, "integrity": 1, "note": {
                    "en": "Safe mail earns a commendation and a courier bonus.",
                    "es": "El correo seguro te gana un elogio y bonificacion de correo.",
                }},
            },
            {
                "description": {
                    "en": "Ride ahead to warn settlers of looming bandits.",
                    "es": "Cab algar por delante para avisar a los colonos de bandidos.",
                },
                "next": "outlaw_camp",
                "effects": {"integrity": 1, "note": {
                    "en": "Families along the Camino Real light lanterns in thanks.",
                    "es": "Las familias del Camino Real encienden faroles agradecidas.",
                }},
            },
            {
                "description": {
                    "en": "Share gospel pamphlets with weary passengers.",
                    "es": "Compartir folletos del evangelio con pasajeros cansados.",
                },
                "next": "peace_camp_end",
                "effects": {"integrity": 2, "note": {
                    "en": "Hope rises among travelers facing the desert night.",
                    "es": "La esperanza crece entre los viajeros ante la noche del desierto.",
                }},
            },
        ],
    },
    "river_hospital": {
        "title": {
            "en": "Hospital Tents by the Rio Grande",
            "es": "Carpas del hospital junto al rio Grande",
        },
        "text": {
            "en": "Cholera and rifle wounds keep surgeons busy on the riverbank. Volunteers boil water and read scripture for comfort.",
            "es": "El colera y las balas mantienen ocupados a los cirujanos en la ribera. Voluntarios hierven agua y leen Escritura para consolar.",
        },
        "choices": [
            {
                "description": {
                    "en": "Tend the wounded and sing psalms of peace.",
                    "es": "Atender a los heridos y entonar salmos de paz.",
                },
                "next": "peace_camp_end",
                "effects": {"integrity": 3, "note": {
                    "en": "Patients call you Chaplain's right hand as calm settles.",
                    "es": "Los pacientes te llaman la mano derecha del capellan mientras llega la calma.",
                }},
            },
            {
                "description": {
                    "en": "Requisition silverware to sell for extra medical supplies.",
                    "es": "Requisar cubiertos de plata para vender y comprar suministros.",
                },
                "next": "gold_tally_end",
                "effects": {"gold": 3, "integrity": -2, "note": {
                    "en": "Supplies arrive, yet some whisper about heavy-handed methods.",
                    "es": "Llegan suministros, aunque algunos murmuran sobre metodos duros.",
                }},
            },
            {
                "description": {
                    "en": "Return to the fort when the surgeons relieve you.",
                    "es": "Regresar al fuerte cuando los cirujanos te relevan.",
                },
                "next": "fort_bliss_muster",
                "effects": {"note": {
                    "en": "You carry prayer requests back to the chapel ledger.",
                    "es": "Llevas peticiones de oracion al registro de la capilla.",
                }},
            },
        ],
    },
    "peace_camp_end": {
        "title": {
            "en": "Camp of Peace",
            "es": "Campamento de paz",
        },
        "text": {
            "en": "Dusk settles over El Paso as those you served break bread together. From Mescalero scouts to Tigua farmers, reconciliation circles the campfires.",
            "es": "El atardecer cubre El Paso mientras quienes serviste comparten el pan. Desde exploradores mescaleros hasta agricultores Tigua, la reconciliacion rodea las fogatas.",
        },
        "end": True,
        "choices": [],
    },
    "gold_tally_end": {
        "title": {
            "en": "Paymaster's Ledger",
            "es": "Libro mayor del pagador",
        },
        "text": {
            "en": "At Fort Bliss headquarters, Lieutenant DeRosey tallies your deeds. Gold clinks upon the desk as reports of your conduct spread across the frontier.",
            "es": "En el cuartel de Fort Bliss, el teniente DeRosey registra tus hechos. El oro tintinea sobre el escritorio mientras los informes de tu conducta recorren la frontera.",
        },
        "end": True,
        "choices": [],
    },
}


CAVALRY_START_SCENES = [scene for scene, data in CAVALRY_SCENES.items() if data.get("start")]
CAVALRY_GAME_STATES: Dict[str, CavalryGameState] = {}


def _serialize_cavalry_state(state: CavalryGameState) -> Dict[str, Any]:
    return {
        "current_scene": state.current_scene,
        "gold": state.gold,
        "integrity": state.integrity,
        "history": list(state.history),
        "completed": state.completed,
        "language": state.language,
        "start_scene": state.start_scene,
        "last_note": state.last_note,
        "intro_shown": state.intro_shown,
    }


def _deserialize_cavalry_state(player_key: str, data: Dict[str, Any]) -> Optional[CavalryGameState]:
    try:
        current_scene = data.get("current_scene")
        if not current_scene or current_scene not in CAVALRY_SCENES:
            return None
        history = data.get("history") or [current_scene]
        if not isinstance(history, list):
            history = [current_scene]
        language = data.get("language") or "en"
        state = CavalryGameState(
            player_key=player_key,
            current_scene=current_scene,
            gold=int(data.get("gold", 0)),
            integrity=int(data.get("integrity", 3)),
            history=[str(item) for item in history],
            completed=bool(data.get("completed", False)),
            language=language,
            start_scene=data.get("start_scene", current_scene),
            last_note=data.get("last_note"),
            intro_shown=bool(data.get("intro_shown", False)),
        )
        return state
    except Exception:
        return None


def _load_cavalry_state_store() -> None:
    loaded = safe_load_json(CAVALRY_STATE_FILE, {})
    if not isinstance(loaded, dict):
        return
    with CAVALRY_STATE_LOCK:
        for player_key, entry in loaded.items():
            if not isinstance(player_key, str) or not isinstance(entry, dict):
                continue
            state = _deserialize_cavalry_state(player_key, entry)
            if state:
                CAVALRY_GAME_STATES[player_key] = state


def _save_cavalry_state_store() -> None:
    try:
        with CAVALRY_STATE_LOCK:
            payload = {pk: _serialize_cavalry_state(st) for pk, st in CAVALRY_GAME_STATES.items()}
        with open(CAVALRY_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception as e:
        clean_log(f"Could not persist cavalry states: {e}", "⚠️")


_load_cavalry_state_store()


def _cavalry_scene_title(scene_id: str, language: str) -> str:
    data = CAVALRY_SCENES.get(scene_id, {})
    return data.get("title", {}).get(language) or data.get("title", {}).get("en") or scene_id


def _start_cavalry_game(player_key: str, language: str, requested: Optional[str] = None) -> CavalryGameState:
    chosen = None
    if requested:
        request_norm = requested.lower()
        for scene_id in CAVALRY_START_SCENES:
            data = CAVALRY_SCENES.get(scene_id, {})
            aliases = data.get("start_aliases", [])
            if request_norm == scene_id or request_norm in aliases:
                chosen = scene_id
                break
        if not chosen:
            for scene_id in CAVALRY_START_SCENES:
                if request_norm in scene_id:
                    chosen = scene_id
                    break
    if not chosen:
        chosen = random.choice(CAVALRY_START_SCENES)
    state = CavalryGameState(
        player_key=player_key,
        current_scene=chosen,
        gold=0,
        integrity=3,
        history=[chosen],
        completed=False,
        language=language,
        start_scene=chosen,
        last_note=None,
        intro_shown=False,
    )
    with CAVALRY_STATE_LOCK:
        CAVALRY_GAME_STATES[player_key] = state
    _save_cavalry_state_store()
    return state


def _apply_choice(state: CavalryGameState, choice_idx: int, language: str) -> Optional[str]:
    scene = CAVALRY_SCENES.get(state.current_scene)
    if not scene:
        return "Scene data missing."
    choices = scene.get("choices") or []
    if choice_idx < 1 or choice_idx > len(choices):
        return translate(language, 'invalid_choice', "Invalid choice number. Try again.")
    choice = choices[choice_idx - 1]
    effects = choice.get("effects", {})
    note_map = effects.get("note")
    if isinstance(effects.get("gold"), (int, float)):
        state.gold += int(effects.get("gold"))
    if isinstance(effects.get("integrity"), (int, float)):
        state.integrity += int(effects.get("integrity"))
    state.last_note = None
    if isinstance(note_map, dict):
        state.last_note = note_map.get(language) or note_map.get("en")
    next_scene = choice.get("next")
    if not next_scene:
        return translate(language, 'missing_destination', "That path is not ready yet.")
    state.current_scene = next_scene
    state.history.append(next_scene)
    state.completed = CAVALRY_SCENES.get(next_scene, {}).get("end", False)
    with CAVALRY_STATE_LOCK:
        CAVALRY_GAME_STATES[state.player_key] = state
    _save_cavalry_state_store()
    return None


def _format_cavalry_status(state: CavalryGameState, language: str) -> str:
    lang = _cavalry_language(language or state.language)
    state.language = lang
    scene = CAVALRY_SCENES.get(state.current_scene)
    if not scene:
        return "Adventure data missing."
    lines: List[str] = []
    raw_title = scene.get("title", {}).get(lang) or scene.get("title", {}).get("en") or state.current_scene
    icon = CAVALRY_SCENE_ICONS.get(state.current_scene)
    title_line = f"{icon} {raw_title}" if icon else raw_title
    lines.append(title_line)
    text = scene.get("text", {}).get(lang) or scene.get("text", {}).get("en")
    if text:
        lines.append("")
        lines.append(text)
    fact = scene.get("fact", {}).get(lang) or scene.get("fact", {}).get("en")
    if fact:
        lines.append("")
        fact_label = "Historical note" if lang == "en" else "Nota historica"
        lines.append(f"📝 {fact_label}: {fact}")
    status_label = "Status" if lang == "en" else "Estado"
    gold_label = "Gold" if lang == "en" else "Oro"
    integrity_label = "Integrity" if lang == "en" else "Integridad"
    lines.append("")
    lines.append(f"🛡️ {status_label}: {gold_label} {state.gold} | {integrity_label} {state.integrity}")
    if len(state.history) > 1:
        path_label = "Trail so far" if lang == "en" else "Ruta hasta ahora"
        history_titles = [
            _cavalry_scene_title(scene_id, lang)
            for scene_id in state.history[:-1]
        ]
        lines.append(f"🧭 {path_label}: {', '.join(history_titles)}")
    if state.last_note:
        update_label = "Update" if lang == "en" else "Actualizacion"
        lines.append("")
        lines.append(f"🗞️ {update_label}: {state.last_note}")
        state.last_note = None
    if scene.get("end"):
        lines.append("")
        blessing = "Peaceful choices brought folks together." if lang == "en" else "Las decisiones pacificas unieron a la gente."
        if state.integrity < 0:
            blessing = "Riches feel heavy without integrity." if lang == "en" else "Las riquezas pesan sin integridad."
        summary_label = "Finale" if lang == "en" else "Final"
        lines.append(f"🙏 {summary_label}: {blessing}")
        outcome_label = "Outcome" if lang == "en" else "Resultado"
        lines.append(f"📜 {outcome_label}: {gold_label} {state.gold} | {integrity_label} {state.integrity}")
        invitation = "Type `/mud restart` to ride again with a new starting post." if lang == "en" else "Escribe `/mud restart` para cabalgar de nuevo desde otro puesto."
        lines.append(f"🐎 {invitation}")
    else:
        choices = scene.get("choices") or []
        if choices:
            prompt = "🤠 Choose a path:" if lang == "en" else "🤠 Elige un camino:"
            lines.append("")
            lines.append(prompt)
            for idx, choice in enumerate(choices, start=1):
                desc = choice.get("description", {}).get(lang) or choice.get("description", {}).get("en") or ""
                marker = CAVALRY_CHOICE_MARKERS[(idx - 1) % len(CAVALRY_CHOICE_MARKERS)]
                lines.append(f"  {idx}. {marker} {desc}")
            instruction = "➡️ Reply with `/mud <number>`" if lang == "en" else "➡️ Responde con `/mud <numero>`"
            lines.append(instruction)

    if not state.intro_shown:
        guidance = CAVALRY_INTRO_LINES.get(lang) or CAVALRY_INTRO_LINES.get("en")
        if guidance:
            lines.append("")
            lines.extend(CAVALRY_ASCII_BANNER)
            lines.append("")
            lines.extend(guidance)
        state.intro_shown = True
        with CAVALRY_STATE_LOCK:
            CAVALRY_GAME_STATES[state.player_key] = state
        _save_cavalry_state_store()
    return "\n".join(lines)


def handle_cavalry_command(arguments: str, sender_id: Optional[str], is_direct: bool, channel_idx: Optional[int], language_hint: Optional[str]) -> str:
    language = _cavalry_language(language_hint)
    if not sender_id:
        return "Mud tracking requires a sender id."
    player_key = _cavalry_player_key(str(sender_id), is_direct, channel_idx)
    state = CAVALRY_GAME_STATES.get(player_key)
    args = arguments.strip()
    lower_args = args.lower()
    parts = lower_args.split()
    if not args:
        if not state:
            if language == "en":
                intro_lines = [
                    "El Paso Cavalry Adventure",
                    "",
                    "Step into the 1858 Fort Bliss frontier as a US cavalry trooper.",
                    "Collect gold honorably, favor peace, and see how the frontier remembers you.",
                    "",
                    "Commands:",
                    "  /mud start - begin a new story (random starting post).",
                    "  /mud start fort|rio|mountain - begin at a specific assignment.",
                    "  /mud <number> - choose an option when presented.",
                    "  /mud status - review your current scene.",
                    "  /mud restart - close this run and start fresh.",
                    "  /mud rules - review the full rules and etiquette.",
                    "",
                    "Type `/mud start` to saddle up.",
                ]
            else:
                intro_lines = [
                    "Aventura de caballeria en El Paso",
                    "",
                    "Adentrate en los fuertes y fronteras de 1858 como soldado de caballeria estadounidense.",
                    "Reune oro con honor, favorece la paz y descubre como la frontera te recuerda.",
                    "",
                    "Comandos:",
                    "  /mud start - iniciar una historia (puesto aleatorio).",
                    "  /mud start fort|rio|mountain - iniciar en una asignacion especifica.",
                    "  /mud <numero> - elegir una opcion cuando aparezca.",
                    "  /mud status - revisar tu escena actual.",
                    "  /mud restart - cerrar esta partida y comenzar otra.",
                    "  /mud rules - repasar todas las reglas y la etiqueta.",
                    "",
                    "Escribe `/mud start` para ensillar.",
                ]
            return "\n".join(intro_lines)
        return _format_cavalry_status(state, language)
    if parts and parts[0] == "rules":
        rules_text = CAVALRY_RULES_TEXT.get(language) or CAVALRY_RULES_TEXT.get("en")
        return rules_text
    if parts and parts[0] == "start":
        requested = " ".join(parts[1:]).strip()
        state = _start_cavalry_game(player_key, language, requested or None)
        return _format_cavalry_status(state, language)
    if parts and parts[0] == "restart" and len(parts) == 1:
        with CAVALRY_STATE_LOCK:
            CAVALRY_GAME_STATES.pop(player_key, None)
        _save_cavalry_state_store()
        state = _start_cavalry_game(player_key, language, None)
        return _format_cavalry_status(state, language)
    if parts and parts[0] == "status" and len(parts) == 1:
        if not state:
            return "No adventure in progress. Use `/mud start`." if language == "en" else "No hay aventura en curso. Usa `/mud start`."
        return _format_cavalry_status(state, language)
    if not state:
        return "Start the adventure first with `/mud start`." if language == "en" else "Inicia la aventura primero con `/mud start`."
    if state.completed:
        return "Story complete. Use `/mud restart` for a new ride." if language == "en" else "Historia concluida. Usa `/mud restart` para otra cabalgata."
    try:
        choice_number = int(args.split()[0])
    except ValueError:
        return "Provide a choice number like `/mud 1`." if language == "en" else "Indica un numero como `/mud 1`."
    err = _apply_choice(state, choice_number, language)
    if err:
        return err
    return _format_cavalry_status(state, language)


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
    sender_key = _sender_key(sender_id)
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


@dataclass
class PendingReply:
    text: str
    reason: str = "command"
    chunk_delay: Optional[float] = None
    pre_send_delay: Optional[float] = None


def _command_delay(reason: str, delay: Optional[float] = None) -> None:
    wait = COMMAND_REPLY_DELAY if delay is None else max(delay, 0)
    try:
        clean_log(f"Buffering {wait}s before replying to {reason}", "⏳", show_always=True, rate_limit=False)
    except Exception:
        pass
    time.sleep(wait)


def _format_bible_verse(language: str = 'en') -> Optional[str]:
    dataset = BIBLE_VERSES_DATA
    if language == 'es' and BIBLE_VERSES_DATA_ES:
        dataset = BIBLE_VERSES_DATA_ES
    if not dataset:
        return None
    verse = random.choice(dataset)
    if isinstance(verse, dict):
        ref = verse.get("reference") or verse.get("ref")
        text = verse.get("text") or verse.get("verse")
        if ref and text:
            return f"{ref}: {text}"
    if isinstance(verse, str):
        return verse
    return None


def _random_chuck_fact(language: str = 'en') -> Optional[str]:
    dataset = CHUCK_NORRIS_FACTS
    if language == 'es' and CHUCK_NORRIS_FACTS_ES:
        dataset = CHUCK_NORRIS_FACTS_ES
    if not dataset:
        return None
    return random.choice(dataset)


def _random_blond_joke(language: str = 'en') -> Optional[str]:
    if not BLOND_JOKES:
        return None
    return random.choice(BLOND_JOKES)


def _random_yo_momma_joke(language: str = 'en') -> Optional[str]:
    if not YO_MOMMA_JOKES:
        return None
    return random.choice(YO_MOMMA_JOKES)


def _weather_code_description(code: Optional[int], language: str = 'en') -> str:
    mapping_en = {
        0: "clear sky",
        1: "mainly clear",
        2: "partly cloudy",
        3: "overcast",
        45: "fog",
        48: "depositing rime fog",
        51: "light drizzle",
        53: "moderate drizzle",
        55: "dense drizzle",
        56: "light freezing drizzle",
        57: "dense freezing drizzle",
        61: "light rain",
        63: "moderate rain",
        65: "heavy rain",
        66: "light freezing rain",
        67: "heavy freezing rain",
        71: "light snow",
        73: "moderate snow",
        75: "heavy snow",
        77: "snow grains",
        80: "light rain showers",
        81: "moderate rain showers",
        82: "violent rain showers",
        85: "light snow showers",
        86: "heavy snow showers",
        95: "thunderstorm",
        96: "thunderstorm with light hail",
        99: "thunderstorm with heavy hail",
    }
    mapping_es = {
        0: "cielo despejado",
        1: "mayormente despejado",
        2: "parcialmente nublado",
        3: "cubierto",
        45: "niebla",
        48: "niebla helada",
        51: "llovizna ligera",
        53: "llovizna moderada",
        55: "llovizna densa",
        56: "llovizna helada ligera",
        57: "llovizna helada intensa",
        61: "lluvia ligera",
        63: "lluvia moderada",
        65: "lluvia intensa",
        66: "lluvia helada ligera",
        67: "lluvia helada intensa",
        71: "nieve ligera",
        73: "nieve moderada",
        75: "nieve intensa",
        77: "granitos de nieve",
        80: "chubascos ligeros",
        81: "chubascos moderados",
        82: "chubascos violentos",
        85: "chubascos de nieve ligeros",
        86: "chubascos de nieve intensos",
        95: "tormenta",
        96: "tormenta con granizo ligero",
        99: "tormenta con granizo fuerte",
    }
    try:
        key = int(code) if code is not None else None
    except (TypeError, ValueError):
        key = None
    if language == 'es':
        return mapping_es.get(key, "condiciones locales")
    return mapping_en.get(key, "local conditions")


def _wind_direction_cardinal(degrees: Optional[float]) -> Optional[str]:
    if degrees is None:
        return None
    try:
        deg = float(degrees) % 360.0
    except (TypeError, ValueError):
        return None
    directions = [
        "N",
        "NNE",
        "NE",
        "ENE",
        "E",
        "ESE",
        "SE",
        "SSE",
        "S",
        "SSW",
        "SW",
        "WSW",
        "W",
        "WNW",
        "NW",
        "NNW",
    ]
    idx = int((deg / 22.5) + 0.5) % len(directions)
    return directions[idx]


def _format_el_paso_weather() -> Optional[str]:
    clean_log("Fetching El Paso weather snapshot", "🌤️", show_always=True, rate_limit=False)
    summary = _format_weather_report("El Paso, TX", EL_PASO_LAT, EL_PASO_LON, language='en', timezone="America/Denver", cache_token="el_paso_en")
    if summary:
        EL_PASO_WEATHER_CACHE["timestamp"] = time.time()
        EL_PASO_WEATHER_CACHE["text"] = summary
    return summary


def _random_el_paso_fact() -> Optional[str]:
    if not EL_PASO_FACTS:
        return None
    return random.choice(EL_PASO_FACTS)


def _normalize_weather_query(query: str) -> str:
    cleaned = query.strip().lower()
    return WEATHER_CITY_SYNONYMS.get(cleaned, query.strip())


def _geocode_location(query: str, language: Optional[str] = None) -> Optional[Dict[str, Any]]:
    normalized = _normalize_weather_query(query)
    params = {
        "count": 1,
    }
    lang_param = None
    if language == 'es':
        lang_param = 'es'
    if lang_param:
        params["language"] = lang_param
    lat = lon = None
    base_url = "https://geocoding-api.open-meteo.com/v1/search"
    query_str = normalized.strip()
    is_postal = bool(re.fullmatch(r"[0-9A-Za-z\- ]{3,12}", query_str)) and any(char.isdigit() for char in query_str)
    try:
        if is_postal:
            params_postal = dict(params)
            params_postal["postal_code"] = query_str
            resp = requests.get(base_url, params=params_postal, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            results = data.get("results") or []
            if results:
                best = results[0]
                lat = best.get("latitude")
                lon = best.get("longitude")
                if lat is not None and lon is not None:
                    name = best.get("name") or query_str
                    country = best.get("country")
                    display = f"{name}, {country}" if country else name
                    return {
                        "name": display,
                        "latitude": float(lat),
                        "longitude": float(lon),
                        "timezone": best.get("timezone") or "auto",
                    }
        params_name = dict(params)
        params_name["name"] = query_str
        resp = requests.get(base_url, params=params_name, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results") or []
        if not results:
            return None
        best = results[0]
        lat = best.get("latitude")
        lon = best.get("longitude")
        if lat is None or lon is None:
            return None
        name = best.get("name") or query_str
        admin1 = best.get("admin1")
        country = best.get("country")
        parts = [name]
        if admin1 and admin1.lower() != name.lower():
            parts.append(admin1)
        if country and country.lower() not in [p.lower() for p in parts]:
            parts.append(country)
        display = ", ".join(parts)
        return {
            "name": display,
            "latitude": float(lat),
            "longitude": float(lon),
            "timezone": best.get("timezone") or "auto",
        }
    except Exception:
        return None


def _format_weather_report(location_name: str, latitude: float, longitude: float, language: str = 'en', timezone: Optional[str] = None, cache_token: Optional[str] = None) -> Optional[str]:
    key = cache_token or f"{round(latitude, 3)},{round(longitude, 3)}:{language}"
    entry = WEATHER_DYNAMIC_CACHE.get(key)
    now = time.time()
    if entry and now - entry.get("timestamp", 0.0) < EL_PASO_WEATHER_TTL:
        return entry.get("text")

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "current_weather": "true",
        "hourly": "relativehumidity_2m,apparent_temperature",
        "timezone": timezone or "auto",
    }
    if language == 'es':
        params["language"] = "es"
    try:
        response = requests.get(EL_PASO_WEATHER_API, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
    except Exception:
        return None

    current = data.get("current_weather") or {}
    if not current:
        return None

    temp_c = current.get("temperature")
    temp_f = (temp_c * 9 / 5 + 32) if isinstance(temp_c, (int, float)) else None
    wind_speed = current.get("windspeed")
    wind_mph = wind_speed * 0.621371 if isinstance(wind_speed, (int, float)) else None
    wind_dir_text = _wind_direction_cardinal(current.get("winddirection"))
    weather_desc = _weather_code_description(current.get("weathercode"), language)
    observed_time = str(current.get("time") or "").replace("T", " ").strip()

    humidity = None
    feels_like_c = None
    hourly = data.get("hourly") or {}
    hourly_times = hourly.get("time") or []
    try:
        idx = hourly_times.index(current.get("time"))
    except ValueError:
        idx = None
    if idx is not None:
        rel_humidity = hourly.get("relativehumidity_2m") or []
        if idx < len(rel_humidity):
            humidity = rel_humidity[idx]
        apparent = hourly.get("apparent_temperature") or []
        if idx < len(apparent):
            feels_like_c = apparent[idx]

    feels_like_f = (feels_like_c * 9 / 5 + 32) if isinstance(feels_like_c, (int, float)) else None

    if language == 'es':
        temp_bits = []
        if isinstance(temp_c, (int, float)):
            temp_bits.append(f"{temp_c:.1f}°C")
        if isinstance(temp_f, (int, float)):
            temp_bits.append(f"{temp_f:.0f}°F")
        parts = []
        if temp_bits:
            parts.append(" / ".join(temp_bits))
        if isinstance(feels_like_c, (int, float)):
            parts.append(f"sensación {feels_like_c:.1f}°C")
        elif isinstance(feels_like_f, (int, float)):
            parts.append(f"sensación {feels_like_f:.0f}°F")
        if isinstance(humidity, (int, float)):
            parts.append(f"humedad {humidity:.0f}%")
        if isinstance(wind_mph, (int, float)):
            if wind_dir_text:
                parts.append(f"viento {wind_mph:.0f} mph {wind_dir_text}")
            else:
                parts.append(f"viento {wind_mph:.0f} mph")
        summary = f"Clima en {location_name}: {weather_desc}"
        if parts:
            summary += " • " + "; ".join(parts)
        if observed_time:
            summary += f" • actualizado {observed_time}"
    else:
        temp_bits = []
        if isinstance(temp_f, (int, float)):
            temp_bits.append(f"{temp_f:.0f}°F")
        if isinstance(temp_c, (int, float)):
            temp_bits.append(f"{temp_c:.1f}°C")
        parts = []
        if temp_bits:
            parts.append(" / ".join(temp_bits))
        if isinstance(feels_like_f, (int, float)):
            parts.append(f"feels like {feels_like_f:.0f}°F")
        elif isinstance(feels_like_c, (int, float)):
            parts.append(f"feels like {feels_like_c:.1f}°C")
        if isinstance(humidity, (int, float)):
            parts.append(f"humidity {humidity:.0f}%")
        if isinstance(wind_mph, (int, float)):
            if wind_dir_text:
                parts.append(f"wind {wind_mph:.0f} mph {wind_dir_text}")
            else:
                parts.append(f"wind {wind_mph:.0f} mph")
        summary = f"Weather for {location_name}: {weather_desc}"
        if parts:
            summary += " • " + "; ".join(parts)
        if observed_time:
            summary += f" • updated {observed_time}"

    WEATHER_DYNAMIC_CACHE[key] = {"timestamp": now, "text": summary}
    return summary


def _handle_weather_lookup(sender_key: Optional[str], query: str, language: Optional[str]) -> PendingReply:
    lang = language or 'en'
    location = _geocode_location(query, lang)
    if location:
        report = _format_weather_report(location["name"], location["latitude"], location["longitude"], language=lang, timezone=location.get("timezone"))
        if report:
            if sender_key:
                PENDING_WEATHER_REQUESTS.pop(sender_key, None)
            return PendingReply(report, "/weather command")
        failure = translate(lang, 'weather_service_fail', "🌤️ Weather service unavailable right now.")
        if sender_key:
            PENDING_WEATHER_REQUESTS.pop(sender_key, None)
        return PendingReply(failure, "/weather command")
    if sender_key:
        info = PENDING_WEATHER_REQUESTS.setdefault(sender_key, {"language": lang, "attempts": 0})
        info["language"] = lang
        info["attempts"] = info.get("attempts", 0) + 1
        if info["attempts"] >= 2:
            PENDING_WEATHER_REQUESTS.pop(sender_key, None)
            final_msg = translate(lang, 'weather_final_fail', "I still can't find that location. Try another city or ZIP.")
            return PendingReply(final_msg, "weather prompt")
    retry_msg = translate(lang, 'weather_need_city', "I couldn't find that location. Tell me the nearest major city and I'll try again.")
    return PendingReply(retry_msg, "weather prompt")


def _parse_timestamp(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S %Z").replace(tzinfo=timezone.utc)
    except ValueError:
        try:
            return datetime.fromisoformat(ts)
        except Exception:
            return None


def _format_node_label(node_key: Any) -> str:
    if node_key is None:
        return "Unknown"
    try:
        if isinstance(node_key, int):
            return get_node_shortname(node_key)
        if isinstance(node_key, str):
            cleaned = node_key
            if '(' in cleaned:
                cleaned = cleaned.split('(')[0].strip()
            try:
                return get_node_shortname(node_key)
            except Exception:
                return cleaned or str(node_key)
    except Exception:
        pass
    return str(node_key)


def _compute_average_battery_voltage() -> Tuple[Optional[float], int]:
    if interface is None or not hasattr(interface, "nodes"):
        return None, 0
    nodes = getattr(interface, "nodes", {}) or {}
    total = 0.0
    count = 0
    for info in nodes.values():
        telemetry = info.get("telemetry") or {}
        voltage = None
        if isinstance(telemetry, dict):
            for key in ("batteryVoltage", "voltage", "Voltage"):
                if key in telemetry:
                    voltage = telemetry.get(key)
                    break
            if voltage is None:
                battery_block = telemetry.get("battery")
                if isinstance(battery_block, dict):
                    for key in ("voltage", "voltageMv", "voltage_mv"):
                        if key in battery_block:
                            voltage = battery_block.get(key)
                            if key.endswith("Mv") or key.endswith("mv"):
                                try:
                                    voltage = float(voltage) / 1000.0
                                except Exception:
                                    pass
                            break
        if voltage is None:
            continue
        try:
            voltage_val = float(voltage)
        except (TypeError, ValueError):
            continue
        if voltage_val >= BATTERY_PLUGGED_THRESHOLD:
            continue
        total += voltage_val
        count += 1
    if count == 0:
        return None, 0
    return total / count, count


def _format_meshinfo_report(language: str) -> str:
    lang = language or 'en'
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=1)
    prev_cutoff = cutoff - timedelta(hours=1)

    with messages_lock:
        snapshot = list(messages)

    node_first: Dict[Any, datetime] = {}
    node_last: Dict[Any, datetime] = {}
    node_counts: Counter = Counter()
    recent_messages = 0

    for entry in snapshot:
        ts = _parse_timestamp(entry.get("timestamp"))
        if ts is None:
            continue
        node_key = entry.get("node_id")
        if node_key is None:
            node_key = entry.get("node")
        if node_key is None:
            continue
        is_ai = bool(entry.get("is_ai"))
        if not is_ai:
            first = node_first.get(node_key)
            if first is None or ts < first:
                node_first[node_key] = ts
            last = node_last.get(node_key)
            if last is None or ts > last:
                node_last[node_key] = ts
            if ts >= cutoff:
                node_counts[node_key] += 1
        if ts >= cutoff:
            recent_messages += 1

    new_nodes = [node for node, first in node_first.items() if first >= cutoff]
    left_nodes = [node for node, last in node_last.items() if prev_cutoff <= last < cutoff]

    avg_voltage, battery_count = _compute_average_battery_voltage()
    capacity = MAX_SENDS_PER_MINUTE * 60 if MAX_SENDS_PER_MINUTE else 1200
    usage_percent = min(100.0, (recent_messages / capacity) * 100 if capacity else 0.0)
    usage_percent = round(usage_percent, 1)
    top_nodes = node_counts.most_common(3)

    lines = [translate(lang, 'meshinfo_header', "Mesh network summary (last hour)")]

    if new_nodes:
        names = ", ".join(_format_node_label(node) for node in new_nodes)
        lines.append(translate(lang, 'meshinfo_new_nodes_some', "New nodes: {count} ({list})", count=len(new_nodes), list=names))
    else:
        lines.append(translate(lang, 'meshinfo_new_nodes_none', "New nodes: none"))

    if left_nodes:
        names = ", ".join(_format_node_label(node) for node in left_nodes)
        lines.append(translate(lang, 'meshinfo_left_nodes_some', "Nodes left: {count} ({list})", count=len(left_nodes), list=names))
    else:
        lines.append(translate(lang, 'meshinfo_left_nodes_none', "No nodes departed in the last hour."))

    if avg_voltage is not None and battery_count:
        lines.append(translate(lang, 'meshinfo_avg_batt', "Average battery voltage (off-grid): {voltage:.2f} V ({count} nodes)", voltage=avg_voltage, count=battery_count))
    else:
        lines.append(translate(lang, 'meshinfo_avg_batt_unknown', "No battery data available."))

    lines.append(translate(lang, 'meshinfo_network_usage', "Approximate network usage: {percent}% (last hour)", percent=f"{usage_percent:.1f}"))

    if top_nodes:
        formatted = ", ".join(f"{_format_node_label(node)} ({count})" for node, count in top_nodes)
        lines.append(translate(lang, 'meshinfo_top_nodes', "Top nodes by traffic: {list}", list=formatted))
    else:
        lines.append(translate(lang, 'meshinfo_top_nodes_none', "No traffic recorded in the last hour."))

    return "\n".join(lines)

def _cmd_reply(cmd_name: str, message: str) -> PendingReply:
    label = f"{cmd_name} command" if cmd_name else "command"
    return PendingReply(message, label)

DISCORD_RESPONSE_CHANNEL_INDEX = config.get("discord_response_channel_index", None)
DISCORD_RECEIVE_ENABLED = config.get("discord_receive_enabled", True)
# New variable for inbound routing
DISCORD_INBOUND_CHANNEL_INDEX = config.get("discord_inbound_channel_index", None)
if DISCORD_INBOUND_CHANNEL_INDEX is not None:
    try:
        DISCORD_INBOUND_CHANNEL_INDEX = int(DISCORD_INBOUND_CHANNEL_INDEX)
    except (ValueError, TypeError):
        DISCORD_INBOUND_CHANNEL_INDEX = None
# For polling Discord messages (optional)
DISCORD_BOT_TOKEN = config.get("discord_bot_token", None)
DISCORD_CHANNEL_ID = config.get("discord_channel_id", None)

ENABLE_TWILIO = config.get("enable_twilio", False)
ENABLE_SMTP = config.get("enable_smtp", False)
ALERT_PHONE_NUMBER = config.get("alert_phone_number", None)
TWILIO_SID = config.get("twilio_sid", None)
TWILIO_AUTH_TOKEN = config.get("twilio_auth_token", None)
TWILIO_FROM_NUMBER = config.get("twilio_from_number", None)
SMTP_HOST = config.get("smtp_host", None)
SMTP_PORT = config.get("smtp_port", 587)
SMTP_USER = config.get("smtp_user", None)
SMTP_PASS = config.get("smtp_pass", None)
ALERT_EMAIL_TO = config.get("alert_email_to", None)

SERIAL_PORT = config.get("serial_port", "")
try:
    # SERIAL_BAUD = int(config.get("serial_baud", 921600))  # ← COMMENTED OUT - fast baud causing issues
    SERIAL_BAUD = int(config.get("serial_baud", 115200))  # ← NEW ● default 115200 (slower for stability)
except (ValueError, TypeError):
    # SERIAL_BAUD = 921600  # ← COMMENTED OUT - fast baud causing issues  
    SERIAL_BAUD = 115200  # ← NEW ● default 115200 (slower for stability)
USE_WIFI = bool(config.get("use_wifi", False))
WIFI_HOST = config.get("wifi_host", None)
try:
    WIFI_PORT = int(config.get("wifi_port", 4403))
except (ValueError, TypeError):
    WIFI_PORT = 4403
USE_MESH_INTERFACE = bool(config.get("use_mesh_interface", False))

# Auto-refresh to improve long-term stability
AUTO_REFRESH_ENABLED = bool(config.get("auto_refresh_enabled", True))
try:
  AUTO_REFRESH_MINUTES = int(config.get("auto_refresh_minutes", 60))
  if AUTO_REFRESH_MINUTES < 5:
    AUTO_REFRESH_MINUTES = 60  # guard: don't thrash
except (ValueError, TypeError):
  AUTO_REFRESH_MINUTES = 60

# Sending rate limiting to prevent mesh network overload
from collections import deque
send_timestamps = deque()
send_rate_lock = threading.Lock()
MAX_SENDS_PER_MINUTE = 20  # Configurable limit to prevent spam overload

def check_send_rate_limit():
    """Check if we're under the sending rate limit. Returns True if OK to send."""
    with send_rate_lock:
        now = time.time()
        # Remove timestamps older than 1 minute
        while send_timestamps and send_timestamps[0] < now - 60:
            send_timestamps.popleft()
        
        if len(send_timestamps) >= MAX_SENDS_PER_MINUTE:
            return False
        
        send_timestamps.append(now)
        return True

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
response_queue = queue.Queue(maxsize=10)  # Limit queue size to prevent memory issues
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
                clean_log(f"✅ [AsyncAI] Generated response in {processing_time:.1f}s, preparing to send...", "🤖")

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

                # If message originated on Discord inbound channel, send back to Discord
                if ENABLE_DISCORD and DISCORD_SEND_AI and DISCORD_INBOUND_CHANNEL_INDEX is not None and ch_idx == DISCORD_INBOUND_CHANNEL_INDEX:
                    disc_msg = f"🤖 **{AI_NODE_NAME}**: {response_text}"
                    send_discord_message(disc_msg)
                    try:
                        log_message("Discord", disc_msg, direct=False, channel_idx=DISCORD_INBOUND_CHANNEL_INDEX, is_ai=(pending is None))
                    except Exception:
                        pass

                # Send the response via mesh
                chunk_delay = pending.chunk_delay if pending else None
                if interface_ref and response_text:
                    if is_direct:
                        send_direct_chunks(interface_ref, response_text, sender_node, chunk_delay=chunk_delay)
                    else:
                        send_broadcast_chunks(interface_ref, response_text, ch_idx, chunk_delay=chunk_delay)
                        
                try:
                    globals()['last_ai_response_time'] = _now()
                except Exception:
                    pass
                total_time = time.time() - start_time
                clean_log(f"🎯 [AsyncAI] Completed response for {sender_node} (total: {total_time:.1f}s)", "✅")
            else:
                clean_log(f"❌ [AsyncAI] No response generated for {sender_node} ({processing_time:.1f}s)", "🤖")
                
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
    clean_log("🚀 [AsyncAI] Response worker thread started", "⚡")

def stop_response_worker():
    """Stop the background response worker thread."""
    global response_worker_running
    response_worker_running = False
    response_queue.put(None)  # Signal shutdown

# -----------------------------
# Location Lookup Function
# -----------------------------
def get_node_location(node_id):
    if interface and hasattr(interface, "nodes") and node_id in interface.nodes:
        pos = interface.nodes[node_id].get("position", {})
        lat = pos.get("latitude")
        lon = pos.get("longitude")
        tstamp = pos.get("time")
        return lat, lon, tstamp
    return None, None, None

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
    clean_log(f"Broadcasting on Ch{channelIndex}: {text}", "📡")
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
                clean_log(f"Sent chunk {i+1}/{len(chunks)} on Ch{channelIndex}", "📡")
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


def send_direct_chunks(interface, text, destinationId, chunk_delay: Optional[float] = None):
    dprint(f"send_direct_chunks: text='{text}', destId={destinationId}")
    dest_display = get_node_shortname(destinationId)
    if not dest_display:
        dest_display = str(destinationId)
    clean_log(f"Sending direct to {dest_display}: {text}", "📤")
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
                clean_log(f"Sent chunk {idx + 1}/{len(chunks)} to {dest_display}", "📤")
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

def send_to_lmstudio(user_message: str):
    """Chat/completion request to LM Studio with explicit model name."""
    dprint(f"send_to_lmstudio: user_message='{user_message}'")
    ai_log("Processing message...", "lmstudio")
    payload = {
        "model": LMSTUDIO_CHAT_MODEL,  # **mandatory when multiple models loaded**
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": user_message},
        ],
        "max_tokens": MAX_RESPONSE_LENGTH,
    }
    try:
        # Track last AI request time
        try:
            globals()['last_ai_request_time'] = _now()
        except Exception:
            pass
        # Simple retry loop
        attempts = 0
        backoff = 1.5
        response = None
        while attempts < 2:
            attempts += 1
            try:
                response = requests.post(LMSTUDIO_URL, json=payload, timeout=LMSTUDIO_TIMEOUT)
                break
            except Exception as e:
                if attempts >= 2:
                    raise
                time.sleep(backoff)
                backoff *= 1.7
        if response is not None and response.status_code == 200:
            j = response.json()
            dprint(f"LMStudio raw ⇒ {j}")
            ai_resp = (
                j.get("choices", [{}])[0]
                 .get("message", {})
                 .get("content", "🤖 [No response]")
            )
            # Clean response logging
            if ai_resp and ai_resp != "🤖 [No response]":
                clean_resp = ai_resp[:100] + "..." if len(ai_resp) > 100 else ai_resp
                ai_log(f"Response: {clean_resp}", "lmstudio")
            return ai_resp[:MAX_RESPONSE_LENGTH]
        else:
            err = f"LMStudio error: {getattr(response, 'status_code', 'no response')}"
            print(f"⚠️ {err}")
            try:
                globals()['ai_last_error'] = err
                globals()['ai_last_error_time'] = _now()
            except Exception:
                pass
            return None
    except Exception as e:
        msg = f"LMStudio request failed: {e}"
        print(f"⚠️ {msg}")
        try:
            globals()['ai_last_error'] = msg
            globals()['ai_last_error_time'] = _now()
        except Exception:
            pass
        return None
def lmstudio_embed(text: str):
    """Return an embedding vector (if you ever need it)."""
    payload = {
        "model": LMSTUDIO_EMBEDDING_MODEL,
        "input": text,
															   
    }
    try:
        r = requests.post(
            "http://localhost:1234/v1/embeddings",
            json=payload,
            timeout=LMSTUDIO_TIMEOUT,
        )
        if r.status_code == 200:
            vec = r.json().get("data", [{}])[0].get("embedding")
            return vec
        else:
            dprint(f"LMStudio embed error {r.status_code}: {r.text}")
					   
    except Exception as exc:
        dprint(f"LMStudio embed exception: {exc}")
    return None
def send_to_openai(user_message):
    dprint(f"send_to_openai: user_message='{user_message}'")
    ai_log("Processing message...", "openai")
    if not OPENAI_API_KEY:
        print("⚠️ No OpenAI API key provided.")
        return None
    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {OPENAI_API_KEY}"
    }
    payload = {
        "model": OPENAI_MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_message}
        ],
        "max_tokens": MAX_RESPONSE_LENGTH
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
                r = requests.post(url, headers=headers, json=payload, timeout=OPENAI_TIMEOUT)
                break
            except Exception as e:
                if attempts >= 2:
                    raise
                time.sleep(backoff)
                backoff *= 1.7
        if r is not None and r.status_code == 200:
            jr = r.json()
            dprint(f"OpenAI raw => {jr}")
            content = (
                jr.get("choices", [{}])[0]
                  .get("message", {})
                  .get("content", "🤖 [No response]")
            )
            # Clean response logging
            if content and content != "🤖 [No response]":
                clean_resp = content[:100] + "..." if len(content) > 100 else content
                ai_log(f"Response: {clean_resp}", "openai")
            return content[:MAX_RESPONSE_LENGTH]
        else:
            err = f"OpenAI error: {getattr(r, 'status_code', 'no response')}"
            print(f"⚠️ {err}")
            try:
                globals()['ai_last_error'] = err
                globals()['ai_last_error_time'] = _now()
            except Exception:
                pass
            return None
    except Exception as e:
        msg = f"OpenAI request failed: {e}"
        print(f"⚠️ {msg}")
        try:
            globals()['ai_last_error'] = msg
            globals()['ai_last_error_time'] = _now()
        except Exception:
            pass
        return None

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


def send_to_ollama(user_message, sender_id=None, is_direct=False, channel_idx=None, thread_root_ts=None):
    dprint(f"send_to_ollama: user_message='{user_message}' sender_id={sender_id} is_direct={is_direct} channel={channel_idx}")
    ai_log("Processing message...", "ollama")

    # Normalize text for non-ASCII characters using unidecode
    user_message = unidecode(user_message)

    # Build optional conversation history
    history = ""
    try:
        if sender_id is not None:
            history = build_ollama_history(sender_id=sender_id, is_direct=is_direct, channel_idx=channel_idx, thread_root_ts=thread_root_ts)
    except Exception as e:
        dprint(f"Warning: failed building history for Ollama: {e}")
        history = ""

    # Compose final prompt: system prompt, optional context, then user message
    if history:
        combined_prompt = f"{SYSTEM_PROMPT}\nCONTEXT:\n{history}\n\nUSER: {user_message}\nASSISTANT:"
    else:
        combined_prompt = f"{SYSTEM_PROMPT}\nUSER: {user_message}\nASSISTANT:"
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
                resp = "🤖 [No response]"
            return (resp or "")[:MAX_RESPONSE_LENGTH]
        else:
            err = f"Ollama error: {getattr(r, 'status_code', 'no response')}"
            print(f"⚠️ {err}")
            try:
                globals()['ai_last_error'] = err
                globals()['ai_last_error_time'] = _now()
            except Exception:
                pass
            return None
    except Exception as e:
        msg = f"Ollama request failed: {e}"
        print(f"⚠️ {msg}")
        try:
            globals()['ai_last_error'] = msg
            globals()['ai_last_error_time'] = _now()
        except Exception:
            pass
        return None

def send_to_home_assistant(user_message):
    dprint(f"send_to_home_assistant: user_message='{user_message}'")
    ai_log("Processing message...", "home_assistant")
    if not HOME_ASSISTANT_URL:
        return None
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
            print(f"⚠️ HA error: {r.status_code} => {r.text}")
            return None
    except Exception as e:
        print(f"⚠️ HA request failed: {e}")
        return None

def get_ai_response(prompt, sender_id=None, is_direct=False, channel_idx=None, thread_root_ts=None):
  """Get AI response from configured provider. Optional context (sender/is_direct/channel_idx)
  is forwarded to the provider integration so it can include history/context when available."""
  if AI_PROVIDER == "lmstudio":
    return send_to_lmstudio(prompt)
  elif AI_PROVIDER == "openai":
    return send_to_openai(prompt)
  elif AI_PROVIDER == "ollama":
    return send_to_ollama(prompt, sender_id=sender_id, is_direct=is_direct, channel_idx=channel_idx, thread_root_ts=thread_root_ts)
  elif AI_PROVIDER == "home_assistant":
    return send_to_home_assistant(prompt)
  else:
    print(f"⚠️ Unknown AI provider: {AI_PROVIDER}")
    return None

def send_discord_message(content):
    if not (ENABLE_DISCORD and DISCORD_WEBHOOK_URL):
        return
    try:
        requests.post(DISCORD_WEBHOOK_URL, json={"content": content}, timeout=10)
    except Exception as e:
        print(f"⚠️ Discord webhook error: {e}")

# -----------------------------
# Revised Emergency Notification Function
# -----------------------------
def send_emergency_notification(node_id, user_msg, lat=None, lon=None, position_time=None):
    info_print("[Info] Sending emergency notification...")

    sn = get_node_shortname(node_id)
    fullname = get_node_fullname(node_id)
    full_msg = f"EMERGENCY from {sn} ({fullname}) [Node {node_id}]:\n"
    if lat is not None and lon is not None:
        maps_url = f"https://www.google.com/maps/search/?api=1&query={lat},{lon}"
        full_msg += f" - Location: {maps_url}\n"
    if position_time:
        full_msg += f" - Last GPS time: {position_time}\n"
    if user_msg:
        full_msg += f" - Message: {user_msg}\n"
    
    # Attempt to send SMS via Twilio if configured.
    try:
        if ENABLE_TWILIO and TWILIO_SID and TWILIO_AUTH_TOKEN and ALERT_PHONE_NUMBER and TWILIO_FROM_NUMBER:
            client = Client(TWILIO_SID, TWILIO_AUTH_TOKEN)
            client.messages.create(
                body=full_msg,
                from_=TWILIO_FROM_NUMBER,
                to=ALERT_PHONE_NUMBER
            )
            print("✅ Emergency SMS sent via Twilio.")
        else:
            print("Twilio not properly configured for SMS.")
    except Exception as e:
        print(f"⚠️ Twilio error: {e}")

    # Attempt to send email via SMTP if configured.
    try:
        if ENABLE_SMTP and SMTP_HOST and SMTP_USER and SMTP_PASS and ALERT_EMAIL_TO:
            if isinstance(ALERT_EMAIL_TO, list):
                email_to = ", ".join(ALERT_EMAIL_TO)
            else:
                email_to = ALERT_EMAIL_TO
            msg = MIMEText(full_msg)
            msg["Subject"] = f"EMERGENCY ALERT from {sn} ({fullname}) [Node {node_id}]"
            msg["From"] = SMTP_USER
            msg["To"] = email_to
            if SMTP_PORT == 465:
                s = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT)
            else:
                s = smtplib.SMTP(SMTP_HOST, SMTP_PORT)
                s.starttls()
            s.login(SMTP_USER, SMTP_PASS)
            s.sendmail(SMTP_USER, email_to, msg.as_string())
            s.quit()
            print("✅ Emergency email sent via SMTP.")
        else:
            print("SMTP not properly configured for email alerts.")
    except Exception as e:
        print(f"⚠️ SMTP error: {e}")

    # Attempt to post emergency alert to Discord if enabled.
    try:
        if DISCORD_SEND_EMERGENCY and ENABLE_DISCORD and DISCORD_WEBHOOK_URL:
            requests.post(DISCORD_WEBHOOK_URL, json={"content": full_msg}, timeout=10)
            print("✅ Emergency alert posted to Discord.")
        else:
            print("Discord emergency notifications disabled or not configured.")
    except Exception as e:
        print(f"⚠️ Discord webhook error: {e}")

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
  global motd_content, SYSTEM_PROMPT, config
  cmd = cmd.lower()
  dprint(f"handle_command => cmd='{cmd}', full_text='{full_text}', sender_id={sender_id}, is_direct={is_direct}, language={language_hint}")
  lang = _normalize_language_code(language_hint) if language_hint else LANGUAGE_FALLBACK
  if cmd == "/about":
    return _cmd_reply(cmd, "MESH-AI Off Grid Chat Bot - By: MR-TBOT.com")

  elif cmd in ["/ai", "/bot", "/query", "/data"]:
    user_prompt = full_text[len(cmd):].strip()
    
    # Special handling for DMs: if the command has no content, treat the whole message as a regular AI query
    if is_direct and not user_prompt:
      # User just typed "/ai" or "/query" alone in a DM - treat it as "ai" (regular message)
      user_prompt = cmd[1:]  # Remove the "/" to make it just "ai", "bot", etc.
      info_print(f"[Info] Converting empty {cmd} command in DM to regular AI query: '{user_prompt}'")
    elif not user_prompt:
      # In channels, if no prompt provided, give helpful message
      return _cmd_reply(cmd, f"Please provide a question or prompt after {cmd}. Example: `{cmd} What's the weather?`")
    
    if AI_PROVIDER == "home_assistant" and HOME_ASSISTANT_ENABLE_PIN:
      if not pin_is_valid(user_prompt):
        return _cmd_reply(cmd, "Security code missing or invalid. Use 'PIN=XXXX'")
      user_prompt = strip_pin(user_prompt)
    ai_answer = get_ai_response(user_prompt, sender_id=sender_id, is_direct=is_direct, channel_idx=channel_idx, thread_root_ts=thread_root_ts)
    if ai_answer:
      return ai_answer
    return _cmd_reply(cmd, "🤖 [No AI response]")

  elif cmd == "/whereami":
    lat, lon, tstamp = get_node_location(sender_id)
    sn = get_node_shortname(sender_id)
    if lat is None or lon is None:
      return _cmd_reply(cmd, f"🤖 Sorry {sn}, I have no GPS fix for your node.")
    tstr = str(tstamp) if tstamp else "Unknown"
    return _cmd_reply(cmd, f"Node {sn} GPS: {lat}, {lon} (time: {tstr})")

  elif cmd in ["/emergency", "/911"]:
    lat, lon, tstamp = get_node_location(sender_id)
    user_msg = full_text[len(cmd):].strip()
    send_emergency_notification(sender_id, user_msg, lat, lon, tstamp)
    log_message(sender_id, f"EMERGENCY TRIGGERED: {full_text}", is_emergency=True)
    return _cmd_reply(cmd, "🚨 Emergency alert sent. Stay safe.")

  elif cmd == "/test":
    sn = get_node_shortname(sender_id)
    return _cmd_reply(cmd, f"Hello {sn}! Received {LOCAL_LOCATION_STRING} by {AI_NODE_NAME}.")

  elif cmd == "/help":
    built_in = [
      "/about", "/menu", "/query", "/whereami", "/emergency", "/911", "/test",
      "/motd", "/weather", "/meshinfo", "/bible", "/chucknorris", "/elpaso", "/blond", "/yomomma", "/sms",
      "/changemotd", "/changeprompt", "/showprompt", "/printprompt", "/reset"
    ]
    custom_cmds = [c.get("command") for c in commands_config.get("commands", [])]
    help_text = "Commands:\n" + ", ".join(built_in + custom_cmds)
    help_text += "\nNote: /changeprompt, /changemotd, /showprompt, and /printprompt are DM-only."
    help_text += "\nBrowse highlights with /menu."
    return _cmd_reply(cmd, help_text)

  elif cmd == "/menu":
    menu_text = format_structured_menu("menu", lang)
    return _cmd_reply(cmd, menu_text)

  elif cmd == "/motd":
    motd_msg = translate(lang, 'motd_current', "Current MOTD:\n{motd}", motd=motd_content)
    return _cmd_reply(cmd, motd_msg)

  elif cmd == "/weather":
    sender_key = _sender_key(sender_id)
    query = full_text[len(cmd):].strip()
    if not query:
      default_report = _format_weather_report("El Paso, TX", EL_PASO_LAT, EL_PASO_LON, language=lang, timezone="America/Denver", cache_token=f"el_paso_{lang}")
      if default_report:
        return _cmd_reply(cmd, default_report)
      fallback = translate(lang, 'weather_service_fail', "🌤️ Weather service unavailable right now.")
      return _cmd_reply(cmd, fallback)
    PENDING_WEATHER_REQUESTS.pop(sender_key, None)
    reply = _handle_weather_lookup(sender_key, query, lang)
    return reply

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

  elif cmd == "/mud":
    if not is_direct:
      msg = translate(lang, 'dm_only', "❌ This command can only be used in a direct message.")
      return _cmd_reply(cmd, msg)
    args = full_text[len(cmd):].strip()
    adventure = handle_cavalry_command(args, sender_id, is_direct, channel_idx, lang)
    if isinstance(adventure, str):
        return PendingReply(adventure, "/mud command")
    return adventure

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
    verse = _format_bible_verse(lang)
    if verse:
        return _cmd_reply(cmd, verse)
    return _cmd_reply(cmd, translate(lang, 'bible_missing', "📜 Scripture library unavailable right now."))

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
    sender_key = _sender_key(sender_id)
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
      usage = translate(lang, 'changemotd_usage', "Usage: /changemotd Your new MOTD text")
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
    if not is_direct:
      return _cmd_reply(cmd, translate(lang, 'dm_only', "❌ This command can only be used in a direct message."))
    sender_key = _sender_key(sender_id)
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
        f"Admin password required for /changeprompt from {get_node_shortname(sender_id)} ({sender_id})",
        "🔐",
        show_always=True,
        rate_limit=False,
      )
      prompt = translate(lang, 'password_prompt', "reply with password")
      return PendingReply(prompt, "admin password")
    # Change the system prompt for AI providers and persist to config.json
    new_prompt = full_text[len(cmd):].strip()
    if not new_prompt:
      usage = translate(lang, 'changeprompt_usage', "Usage: /changeprompt Your new system prompt")
      return _cmd_reply(cmd, usage)
    try:
      SYSTEM_PROMPT = new_prompt
      # Update config dict and persist (atomically)
      if not isinstance(config, dict):
        return _cmd_reply(cmd, "❌ Internal error: config not loaded")
      config["system_prompt"] = new_prompt
      write_atomic(CONFIG_FILE, json.dumps(config, indent=2))
      info_print(f"[Info] System prompt updated by {get_node_shortname(sender_id)}")
      success = translate(lang, 'changeprompt_success', "✅ System prompt updated.")
      return _cmd_reply(cmd, success)
    except Exception as e:
      error_msg = translate(lang, 'changeprompt_error', "❌ Failed to update system prompt: {error}", error=e)
      return _cmd_reply(cmd, error_msg)

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

  elif cmd == "/sms":
    parts = full_text.split(" ", 2)
    if len(parts) < 3:
      return _cmd_reply(cmd, "Invalid syntax. Use: /sms <phone_number> <message>")
    phone_number = parts[1]
    message_text = parts[2]
    try:
      client = Client(TWILIO_SID, TWILIO_AUTH_TOKEN)
      client.messages.create(
        body=message_text,
        from_=TWILIO_FROM_NUMBER,
        to=phone_number,
      )
      print(f"✅ SMS sent to {phone_number}")
      return _cmd_reply(cmd, "SMS sent successfully.")
    except Exception as e:
      print(f"⚠️ Failed to send SMS: {e}")
      return _cmd_reply(cmd, "Failed to send SMS.")

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
  if not check_only:
    channel_type = "DM" if is_direct else f"Ch{channel_idx}"
    clean_log(f"Message from {sender_id} ({channel_type}): {text}", "📨")
  text = text.strip()
  if not text:
    return None if not check_only else False
  if is_direct and not config.get("reply_in_directs", True):
    return None if not check_only else False
  if (not is_direct) and channel_idx != HOME_ASSISTANT_CHANNEL_INDEX and not config.get("reply_in_channels", True):
    return None if not check_only else False

  sender_key = _sender_key(sender_id)
  if is_direct and sender_key in PENDING_ADMIN_REQUESTS and not text.startswith("/"):
    if check_only:
      return False
    return _process_admin_password(sender_id, text)
  if is_direct and sender_key in PENDING_WEATHER_REQUESTS and not text.startswith("/"):
    info = PENDING_WEATHER_REQUESTS.get(sender_key) or {}
    lang = info.get("language")
    if check_only:
      return False
    return _handle_weather_lookup(sender_key, text, lang)

  sanitized = text.replace('\u0007', '').strip()
  normalized = sanitized.lower()
  quick_reply = None
  quick_reason = None
  if is_direct:
    normalized_no_bell = normalized.replace('🔔', '').strip()
    if normalized in ALERT_BELL_KEYWORDS or normalized_no_bell in ALERT_BELL_KEYWORDS:
      quick_reply = random.choice(ALERT_BELL_RESPONSES)
      quick_reason = "alert bell"
    else:
      normalized_no_markers = normalized_no_bell.replace('📍', '').strip()
      if ('shared their position' in normalized_no_markers
          and 'requested a response with your position' in normalized_no_markers):
        quick_reply = random.choice(POSITION_REQUEST_RESPONSES)
        quick_reason = "position request"
  if quick_reply is not None:
    if check_only:
      return False
    return PendingReply(quick_reply, quick_reason or "quick reply")

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
      if cmd_lower in ["/reset", "/sms"]:
        return False  # Process immediately, not async
      # Built-in AI commands need async processing
      if cmd_lower in ["/ai", "/bot", "/query", "/data"]:
        return True  # Needs AI processing
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
    info_print(f"📨 [RX] from {sender_node or '?'} to {raw_to or '^all'} (ch={ch_idx}): {text}")

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

    # Only forward messages on the configured Discord inbound channel to Discord.
    if ENABLE_DISCORD and DISCORD_SEND_ALL and DISCORD_INBOUND_CHANNEL_INDEX is not None and ch_idx == DISCORD_INBOUND_CHANNEL_INDEX:
        sender_info = f"{get_node_shortname(sender_node)} ({sender_node})"
        disc_content = f"**{sender_info}**: {text}"
        send_discord_message(disc_content)

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
      try:
        response_queue.put((text, sender_node, is_direct, ch_idx, thread_root_ts, interface), block=False)
        info_print(f"📬 [AsyncAI] Queued (queue size: {response_queue.qsize()})")
      except queue.Full:
        info_print(f"🚨 [AsyncAI] Response queue full ({response_queue.qsize()}), processing immediately to avoid drop")
        # Fall back to immediate processing if queue is full
        resp = parse_incoming_text(text, sender_node, is_direct, ch_idx, thread_root_ts=thread_root_ts)
        if resp:
          pending = resp if isinstance(resp, PendingReply) else None
          response_text = pending.text if pending else resp
          if response_text:
            if pending:
              _command_delay(pending.reason)
            info_print(f"[Info] Immediate fallback response: {response_text}")
            if is_direct:
              send_direct_chunks(interface, response_text, sender_node)
            else:
              send_broadcast_chunks(interface, response_text, ch_idx)
    else:
      # Non-AI messages (e.g., simple commands) can be processed immediately
      resp = parse_incoming_text(text, sender_node, is_direct, ch_idx, thread_root_ts=thread_root_ts)
      if resp:
        pending = resp if isinstance(resp, PendingReply) else None
        response_text = pending.text if pending else resp
        if response_text:
          info_print(f"[Info] Immediate response: {response_text}")
          if pending:
            _command_delay(pending.reason)
          if is_direct:
            send_direct_chunks(interface, response_text, sender_node)
          else:
            send_broadcast_chunks(interface, response_text, ch_idx)

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
        for line in visible[last_index:]:
          # each SSE “data:” is one log line
          yield f"data: {line}\n\n"
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

@app.route("/logs", methods=["GET"])
def logs():
    uptime = datetime.now(timezone.utc) - server_start_time
    uptime_str = str(uptime).split('.')[0]
    now_local = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")

    # include only viewer-filtered lines unless disabled
    visible = [
        line for line in script_logs
        if (_viewer_should_show(line) if _viewer_filter_enabled else True)
    ]
    log_text = "\n".join(visible)

    html = f"""<html>
  <head>
    <title>MESH-AI Logs - Smooth Scrolling</title>
    <style>
      body {{ 
        background:#000; 
        color:#fff; 
        font-family:monospace; 
        padding:20px; 
        margin:0;
        overflow-x:hidden;
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
        background:#000;
        padding:10px 20px;
        border-bottom:1px solid #333;
        z-index:1000;
      }}
      .content {{
        margin-top:80px;
      }}
      .scroll-indicator {{
        position:fixed;
        bottom:20px;
        right:20px;
        background:#333;
        color:#fff;
        padding:5px 10px;
        border-radius:5px;
        font-size:12px;
      }}
    </style>
  </head>
  <body>
    <div class="header">
  <h1>🌊 MESH-AI Logs - Smooth Stream</h1>
  <div><strong>Local Time:</strong> {now_local} | <strong>Uptime:</strong> {uptime_str} | <strong>Restarts:</strong> {restart_count}</div>
    </div>
    <div class="content">
      <pre id="logbox">{log_text}</pre>
    </div>
    <div class="scroll-indicator" id="scrollStatus">🟢 Auto-scroll ON</div>
    <script>
      let autoScroll = true;
      let isUserScrolling = false;
      let scrollTimeout;
      const logbox = document.getElementById('logbox');
      const scrollStatus = document.getElementById('scrollStatus');
      
      // Smooth auto-scroll function
      function smoothScrollToBottom() {{
        if (autoScroll && !isUserScrolling) {{
          window.scrollTo({{
            top: document.body.scrollHeight,
            behavior: 'smooth'
          }});
        }}
      }}
      
      // Detect user scrolling
      window.addEventListener('scroll', () => {{
        isUserScrolling = true;
        clearTimeout(scrollTimeout);
        
        // Check if user scrolled to bottom
        const isAtBottom = window.innerHeight + window.scrollY >= document.body.scrollHeight - 10;
        
        if (isAtBottom) {{
          autoScroll = true;
          scrollStatus.innerHTML = '🟢 Auto-scroll ON';
          scrollStatus.style.background = '#333';
        }} else {{
          autoScroll = false;
          scrollStatus.innerHTML = '🔴 Auto-scroll OFF (scroll to bottom to enable)';
          scrollStatus.style.background = '#660000';
        }}
        
        // Resume auto-scroll detection after user stops scrolling
        scrollTimeout = setTimeout(() => {{
          isUserScrolling = false;
        }}, 1000);
      }});
      
      // SSE for real-time log updates with robust reconnection
      let eventSource;
      let reconnectAttempts = 0;
      let maxReconnectAttempts = 5;
      let lastMessageTime = Date.now();
      
      function createEventSource() {{
        eventSource = new EventSource('/logs_stream');
        
        eventSource.onmessage = function(event) {{
          // Skip heartbeat messages but reset timeout
          if (event.data.includes('heartbeat') || event.data.includes('keepalive')) {{
            lastMessageTime = Date.now();
            return;
          }}
          
          logbox.textContent += event.data + '\\n';
          smoothScrollToBottom();
          lastMessageTime = Date.now();
          reconnectAttempts = 0; // Reset on successful message
        }};
        
        eventSource.onopen = function(event) {{
          console.log('SSE connection established');
          reconnectAttempts = 0;
          lastMessageTime = Date.now();
        }};
        
        eventSource.onerror = function(event) {{
          console.log('SSE connection error, attempt', reconnectAttempts + 1);
          eventSource.close();
          
          if (reconnectAttempts < maxReconnectAttempts) {{
            reconnectAttempts++;
            setTimeout(createEventSource, Math.min(1000 * reconnectAttempts, 5000));
          }} else {{
            console.log('Max reconnect attempts reached, reloading page...');
            location.reload();
          }}
        }};
      }}
      
      // Monitor for stale connections (no activity for 60 seconds)
      setInterval(() => {{
        if (Date.now() - lastMessageTime > 60000) {{
          console.log('Connection appears stale, reconnecting...');
          eventSource.close();
          reconnectAttempts = 0;
          createEventSource();
          lastMessageTime = Date.now();
        }}
      }}, 30000); // Check every 30 seconds
      
      // Initialize connection
      createEventSource();
      
      // Initial scroll to bottom
      document.addEventListener("DOMContentLoaded", () => {{
        smoothScrollToBottom();
      }});
    </script>
  </body>
</html>"""
    return html
# -----------------------------
# Revised Discord Webhook Route for Inbound Messages
# -----------------------------
@app.route("/discord_webhook", methods=["POST"])
def discord_webhook():
    if not DISCORD_RECEIVE_ENABLED:
        return jsonify({"status": "disabled", "message": "Discord receive is disabled"}), 200
    data = request.json
    if not data:
        return jsonify({"status": "error", "message": "No JSON payload provided"}), 400

    # Extract the username (default if not provided)
    username = data.get("username", "DiscordUser")
    channel_index = DISCORD_INBOUND_CHANNEL_INDEX
    message_text = data.get("message")
    if message_text is None:
        return jsonify({"status": "error", "message": "Missing message"}), 400

    # Prepend username to the message
    formatted_message = f"**{username}**: {message_text}"

    try:
        log_message("Discord", formatted_message, direct=False, channel_idx=int(channel_index))
        if interface is None:
            print("❌ Cannot route Discord message: interface is None.")
        else:
            send_broadcast_chunks(interface, formatted_message, int(channel_index))
        print(f"✅ Routed Discord message back on channel {channel_index}")
        return jsonify({"status": "sent", "channel_index": channel_index, "message": formatted_message})
    except Exception as e:
        print(f"⚠️ Discord webhook error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

# -----------------------------
# New Twilio SMS Webhook Route for Inbound SMS
# -----------------------------
@app.route("/twilio_webhook", methods=["POST"])
def twilio_webhook():
    sms_body = request.form.get("Body")
    from_number = request.form.get("From")
    if not sms_body:
        return "No SMS body received", 400
    target = config.get("twilio_inbound_target", "channel")
    if target == "channel":
        channel_index = config.get("twilio_inbound_channel_index")
        if channel_index is None:
            return "No inbound channel index configured", 400
        log_message("Twilio", f"From {from_number}: {sms_body}", direct=False, channel_idx=int(channel_index))
        send_broadcast_chunks(interface, sms_body, int(channel_index))
        print(f"✅ Routed incoming SMS from {from_number} to channel {channel_index}")
    elif target == "node":
        node_id = config.get("twilio_inbound_node")
        if node_id is None:
            return "No inbound node configured", 400
        log_message("Twilio", f"From {from_number}: {sms_body}", direct=True)
        send_direct_chunks(interface, sms_body, node_id)
        print(f"✅ Routed incoming SMS from {from_number} to node {node_id}")
    else:
        return "Invalid twilio_inbound_target config", 400
    return "SMS processed", 200

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
    my_lat, my_lon, _ = get_node_location(interface.myNode.nodeNum) if interface and hasattr(interface, "myNode") and interface.myNode else (None, None, None)
    my_gps_json = json.dumps({"lat": my_lat, "lon": my_lon})

    html = """
<html>
<head>
  <title>MESH-AI Dashboard</title>
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
    /* Hide Discord section by default */
    #discordSection { display: none; }
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
        updateDiscordMessagesUI(msgs);
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
      // Show both channel and direct inbound messages, but not outgoing (WebUI, Discord, Twilio, DiscordPoll, AI_NODE_NAME)
      // and not AI responses (reply_to is not null)
      let inbound = messages.filter(m =>
        m.node !== "WebUI" &&
        m.node !== "Discord" &&
        m.node !== "Twilio" &&
        m.node !== "DiscordPoll" &&
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

    // --- Discord Messages Section ---
    function updateDiscordMessagesUI(messages) {
      // Only show Discord messages if any exist
      let discordMsgs = messages.filter(m => m.node === "Discord" || m.node === "DiscordPoll");
      let discordSection = document.getElementById("discordSection");
      let discordDiv = document.getElementById("discordMessagesDiv");
      if (discordMsgs.length === 0) {
        discordSection.style.display = "none";
        discordDiv.innerHTML = "";
        return;
      }
      discordSection.style.display = "block";
      discordDiv.innerHTML = "";
      discordMsgs.forEach(m => {
        const wrap = document.createElement("div");
        wrap.className = "message";
        if (isRecent(m.timestamp, 60)) wrap.classList.add("newMessage");
        const ts = document.createElement("div");
        ts.className = "timestamp";
        ts.textContent = `💬 ${getTZAdjusted(m.timestamp)} | ${m.node}`;
        const body = document.createElement("div");
        body.textContent = m.message;
        wrap.append(ts, body);
        discordDiv.appendChild(wrap);
      });
    }
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

  <div class="lcars-panel" id="discordSection" style="margin:20px;">
    <h2>Discord Messages</h2>
    <div id="discordMessagesDiv"></div>
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
        <label style="font-weight:bold;color:#fff;margin:0 6px 0 0;">Start MESH-AI on boot</label>
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
        desktop_path = os.path.expanduser('~/.config/autostart/mesh-ai-autostart.desktop')
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
                f.write('[Desktop Entry]\nType=Application\nName=MESH-AI Autostart\nExec=' + os.path.abspath('start_mesh_ai.sh') + '\nX-GNOME-Autostart-enabled=' + ('true' if desired else 'false') + '\n')
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
    clean_log("Starting MESH-AI server...", "🚀", show_always=True)
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

    # Additional startup info:
    if ENABLE_DISCORD:
        print(f"Discord configuration enabled: Inbound channel index: {DISCORD_INBOUND_CHANNEL_INDEX}, Webhook URL is {'set' if DISCORD_WEBHOOK_URL else 'not set'}, Bot Token is {'set' if DISCORD_BOT_TOKEN else 'not set'}, Channel ID is {'set' if DISCORD_CHANNEL_ID else 'not set'}.")
    else:
        print("Discord configuration disabled.")
    if ENABLE_TWILIO:
        if TWILIO_SID and TWILIO_AUTH_TOKEN and ALERT_PHONE_NUMBER and TWILIO_FROM_NUMBER:
            print("Twilio is configured for emergency SMS.")
        else:
            print("Twilio is not properly configured for emergency SMS.")
    else:
        print("Twilio is disabled.")
    if ENABLE_SMTP:
        if SMTP_HOST and SMTP_USER and SMTP_PASS and ALERT_EMAIL_TO:
            print("SMTP is configured for emergency email alerts.")
        else:
            print("SMTP is not properly configured for emergency email alerts.")
    else:
        print("SMTP is disabled.")
    # Determine Flask port: prefer environment `MESH_AI_PORT`, then config keys, then default 5000
    try:
        flask_port = int(
            os.environ.get("MESH_AI_PORT")
            or (config.get("web_port") if isinstance(config.get("web_port"), int) else None)
            or (config.get("flask_port") if isinstance(config.get("flask_port"), int) else None)
            or (config.get("port") if isinstance(config.get("port"), int) else None)
            or 5000
        )
    except Exception:
        try:
            flask_port = int(os.environ.get("MESH_AI_PORT", "5000"))
        except Exception:
            flask_port = 5000

    clean_log(f"Launching Flask web interface on port {flask_port}...", "🌐", show_always=True)
    api_thread = threading.Thread(
        target=app.run,
        kwargs={"host": "0.0.0.0", "port": flask_port, "debug": False},
        daemon=True,
    )
    api_thread.start()
    # Start keepalive worker to prevent USB idle timeout without RF noise
    threading.Thread(target=keepalive_worker, daemon=True).start()
    # If Discord polling is configured, start that thread.
    if DISCORD_BOT_TOKEN and DISCORD_CHANNEL_ID:
        threading.Thread(target=poll_discord_channel, daemon=True).start()

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

# Start the watchdog thread after 20 seconds to give node a chance to connect
def poll_discord_channel():
    """Polls the Discord channel for new messages using the Discord API."""
    # Wait a short period for interface to be set up
    time.sleep(5)
    last_message_id = None
    headers = {"Authorization": f"Bot {DISCORD_BOT_TOKEN}"}
    url = f"https://discord.com/api/v9/channels/{DISCORD_CHANNEL_ID}/messages"
    while True:
        try:
            params = {"limit": 10}
            if last_message_id:
                params["after"] = last_message_id
            response = requests.get(url, headers=headers, params=params, timeout=10)
            if response.status_code == 200:
                msgs = response.json()
                msgs = sorted(msgs, key=lambda m: int(m["id"]))
                for msg in msgs:
                    if msg["author"].get("bot"):
                        continue
                    # Only process messages that arrived after the script started
                    if last_message_id is None:
                        msg_timestamp_str = msg.get("timestamp")
                        if msg_timestamp_str:
                            msg_time = datetime.fromisoformat(msg_timestamp_str.replace("Z", "+00:00"))
                            if msg_time < server_start_time:
                                continue
                    username = msg["author"].get("username", "DiscordUser")
                    content = msg.get("content")
                    if content:
                        formatted = f"**{username}**: {content}"
                        log_message("DiscordPoll", formatted, direct=False, channel_idx=DISCORD_INBOUND_CHANNEL_INDEX)
                        if interface is None:
                            print("❌ Cannot send polled Discord message: interface is None.")
                        else:
                            send_broadcast_chunks(interface, formatted, DISCORD_INBOUND_CHANNEL_INDEX)
                        print(f"Polled and routed Discord message: {formatted}")
                        last_message_id = msg["id"]
            else:
                print(f"Discord poll error: {response.status_code} {response.text}")
        except Exception as e:
            print(f"Error polling Discord: {e}")
        time.sleep(10)

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
 
