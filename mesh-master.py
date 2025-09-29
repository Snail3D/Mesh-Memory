import meshtastic
import meshtastic.serial_interface
from meshtastic import BROADCAST_ADDR
try:
    from meshtastic.protobuf import config_pb2 as meshtastic_config_pb2, channel_pb2 as meshtastic_channel_pb2
except Exception:  # pragma: no cover - test harness may not provide protobuf modules
    class _DummyRole:
        DISABLED = 0
        SECONDARY = 2
        @staticmethod
        def Name(v):
            return 'DISABLED' if v == 0 else ('SECONDARY' if v == 2 else str(v))
    class _DummyChannelSettings:
        pass
    class _DummyChannelModule:
        Channel = type('Channel', (), {'Role': _DummyRole})
        ChannelSettings = _DummyChannelSettings
    meshtastic_channel_pb2 = _DummyChannelModule()
    meshtastic_config_pb2 = type('ConfigPb2', (), {})()
try:
    import meshtastic.util as meshtastic_util
except Exception:  # pragma: no cover - provide minimal fallbacks for tests
    class _DummyMeshtasticUtil:
        @staticmethod
        def pskToString(psk: bytes) -> str:
            return 'unencrypted' if not psk else 'secret'
        @staticmethod
        def genPSK256() -> bytes:
            try:
                return os.urandom(32)
            except Exception:
                return b'\x00' * 32
        @staticmethod
        def fromPSK(s: str) -> bytes:
            # Return deterministic bytes for test environment
            return (s or 'psk').encode('utf-8')[:32].ljust(32, b'\0')
    meshtastic_util = _DummyMeshtasticUtil()
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
import string
from collections import deque, Counter, defaultdict, OrderedDict
from pathlib import Path
import traceback
from flask import Flask, request, jsonify, redirect, url_for, stream_with_context, Response
import sys
import socket  # for socket error checking
import re
import random
import subprocess
import math
import textwrap
import uuid
from typing import Optional, Set, Dict, Any, List, Tuple, Union, Sequence
from dataclasses import dataclass, field
# Optional system metrics libraries
try:
    import psutil  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    psutil = None

try:
    import pynvml  # type: ignore
    try:
        pynvml.nvmlInit()
    except Exception:
        pynvml = None
except Exception:  # pragma: no cover - optional dependency
    pynvml = None

from meshtastic_facts import MESHTASTIC_ALERT_FACTS
from unidecode import unidecode   # Added unidecode import for Ollama text normalization
from google.protobuf.message import DecodeError
import queue  # For async message processing
import itertools
import atexit
from mesh_master import (
    GameManager,
    MailManager,
    OnboardingManager,
    PendingReply,
    OfflineWikiStore,
)
from mesh_master.alarm_timer_manager import AlarmTimerManager
from mesh_master.command_utils import promote_bare_command
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


def _is_heartbeat_text(message: Optional[str]) -> bool:
    """Return True if the provided message appears to be a heartbeat ping."""

    if not isinstance(message, str):
        return False
    text = message.strip()
    if not text:
        return False
    if text.startswith('💓'):
        return True
    lowered = text.lower()
    if lowered.startswith('hb conn=') or 'hb conn=' in lowered:
        return True
    if lowered in {'hb', 'heartbeat', 'heartbeat ping', 'heartbeat check'}:
        return True
    if lowered.startswith('hb ') and len(text) <= 40:
        return True
    if lowered.startswith('heartbeat ') and len(text) <= 40:
        return True
    return False


NODE_HEARTBEAT_LAST: Dict[str, float] = {}


class StatsManager:
    """Tracks rolling operational metrics for the dashboard."""

    WINDOW_SECONDS = 24 * 60 * 60
    RECENT_RESPONSE_SAMPLE = 5

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.ai_requests: deque[tuple[float, int]] = deque()
        self.ai_responses: deque[tuple[float, float]] = deque()
        self.mail_sends: deque[tuple[float, str]] = deque()
        self.mailbox_creations: deque[tuple[float, str]] = deque()
        self.game_events: deque[tuple[float, str]] = deque()
        self.user_events: deque[tuple[float, str]] = deque()
        self.new_onboards: deque[tuple[float, str]] = deque()
        self.message_totals: Dict[str, int] = {
            'total': 0,
            'channel': 0,
            'direct': 0,
            'ai': 0,
        }
        # ACK telemetry (timestamp, attempt, success(bool), route('dm'|'ch'))
        self.ack_events: deque[tuple[float, int, bool, str]] = deque()

    def _prune(self, dq: deque, now: float) -> None:
        window = self.WINDOW_SECONDS * 2
        while dq and now - dq[0][0] > window:
            dq.popleft()

    def record_ai_request(self) -> None:
        now = time.time()
        with self.lock:
            self.ai_requests.append((now, 1))
            self._prune(self.ai_requests, now)

    def record_ai_response(self, duration_seconds: float) -> None:
        now = time.time()
        with self.lock:
            self.ai_responses.append((now, float(max(0.0, duration_seconds))))
            self._prune(self.ai_responses, now)

    def record_mail_sent(self, mailbox: str) -> None:
        now = time.time()
        with self.lock:
            self.mail_sends.append((now, mailbox))
            self._prune(self.mail_sends, now)

    def record_mailbox_created(self, mailbox: str) -> None:
        now = time.time()
        with self.lock:
            self.mailbox_creations.append((now, mailbox))
            self._prune(self.mailbox_creations, now)

    def record_ack_event(self, *, is_direct: bool, attempt: int, success: bool) -> None:
        if not RESEND_TELEMETRY_ENABLED:
            return
        now = time.time()
        route = 'dm' if is_direct else 'ch'
        with self.lock:
            self.ack_events.append((now, int(max(1, attempt)), bool(success), route))
            self._prune(self.ack_events, now)

    def record_game(self, game_type: str) -> None:
        now = time.time()
        with self.lock:
            self.game_events.append((now, game_type))
            self._prune(self.game_events, now)

    def record_user_interaction(self, sender_key: Optional[str]) -> None:
        if not sender_key:
            return
        now = time.time()
        with self.lock:
            self.user_events.append((now, sender_key))
            self._prune(self.user_events, now)

    def record_new_onboard(self, sender_key: Optional[str]) -> None:
        if not sender_key:
            return
        now = time.time()
        with self.lock:
            self.new_onboards.append((now, sender_key))
            self._prune(self.new_onboards, now)

    def record_message(self, *, direct: bool, is_ai: bool) -> None:
        direct_flag = bool(direct)
        with self.lock:
            self.message_totals['total'] += 1
            if direct_flag:
                self.message_totals['direct'] += 1
            else:
                self.message_totals['channel'] += 1
            if is_ai:
                self.message_totals['ai'] += 1

    def snapshot(self) -> Dict[str, Any]:
        now = time.time()
        with self.lock:
            for dq in (
                self.ai_requests,
                self.ai_responses,
                self.mail_sends,
                self.mailbox_creations,
                self.game_events,
                self.user_events,
                self.new_onboards,
            ):
                self._prune(dq, now)

            window = self.WINDOW_SECONDS
            start_current = now - window
            start_previous = now - (2 * window)

            def _count_in_window(dq: deque, start: float, end: float | None = None) -> int:
                if end is None:
                    return sum(1 for ts, _ in dq if ts >= start)
                return sum(1 for ts, _ in dq if start <= ts < end)

            ai_requests_curr = _count_in_window(self.ai_requests, start_current)
            ai_processed = _count_in_window(self.ai_responses, start_current)
            durations_ms = [duration * 1000.0 for ts, duration in self.ai_responses if ts >= start_current]
            avg_ms = sum(durations_ms) / len(durations_ms) if durations_ms else None
            recent = durations_ms[-self.RECENT_RESPONSE_SAMPLE :]
            avg_recent_ms = sum(recent) / len(recent) if recent else None

            ai_requests_prev = _count_in_window(self.ai_requests, start_previous, start_current)
            ai_responses_prev = _count_in_window(self.ai_responses, start_previous, start_current)
            mail_curr = _count_in_window(self.mail_sends, start_current)
            mail_prev = _count_in_window(self.mail_sends, start_previous, start_current)
            mailbox_curr = _count_in_window(self.mailbox_creations, start_current)
            mailbox_prev = _count_in_window(self.mailbox_creations, start_previous, start_current)
            games_curr = _count_in_window(self.game_events, start_current)
            games_prev = _count_in_window(self.game_events, start_previous, start_current)

            user_counts = Counter(sender for ts, sender in self.user_events if ts >= start_current)
            user_set = set(user_counts.keys())
            prev_user_set = {sender for ts, sender in self.user_events if start_previous <= ts < start_current}
            onboard_set = {sender for ts, sender in self.new_onboards if ts >= start_current}
            prev_onboard_set = {sender for ts, sender in self.new_onboards if start_previous <= ts < start_current}

            recent_onboards: List[Tuple[str, float]] = []
            seen_recent: Set[str] = set()
            for ts, sender in reversed(self.new_onboards):
                if ts < start_current:
                    break
                if not sender or sender in seen_recent:
                    continue
                seen_recent.add(sender)
                recent_onboards.append((sender, ts))
            recent_onboards.sort(key=lambda item: item[1], reverse=True)

            game_breakdown = Counter(kind for ts, kind in self.game_events if ts >= start_current)

            snapshot = {
                'ai_requests_24h': ai_requests_curr,
                'ai_processed_24h': ai_processed,
                'ai_avg_response_ms': avg_ms,
                'ai_avg_recent_ms': avg_recent_ms,
                'mail_sent_24h': mail_curr,
                'mailboxes_created_24h': mailbox_curr,
                'games_24h': games_curr,
                'games_breakdown': dict(game_breakdown),
                'active_users_24h': len(user_set),
                'new_onboards_24h': len(onboard_set),
                'top_users_24h': user_counts.most_common(5),
                'message_totals': dict(self.message_totals),
                'recent_onboards': [
                    {'sender_key': sender, 'timestamp': ts}
                    for sender, ts in recent_onboards
                ],
                'previous': {
                    'ai_requests_24h': ai_requests_prev,
                    'ai_processed_24h': ai_responses_prev,
                    'mail_sent_24h': mail_prev,
                    'mailboxes_created_24h': mailbox_prev,
                    'games_24h': games_prev,
                    'active_users_24h': len(prev_user_set),
                    'new_onboards_24h': len(prev_onboard_set),
                },
            }
        return snapshot


STATS = StatsManager()


def _collect_system_metrics() -> Dict[str, Any]:
    """Gather lightweight CPU/GPU/memory stats for the dashboard."""
    metrics: Dict[str, Any] = {
        'cpu_percent': None,
        'memory_percent': None,
        'gpu_percent': None,
    }

    def classify(value: Optional[float]) -> str:
        if value is None:
            return 'grey'
        if value < 50.0:
            return 'green'
        if value < 75.0:
            return 'yellow'
        return 'red'

    if psutil is not None:
        try:
            metrics['cpu_percent'] = float(psutil.cpu_percent(interval=None))
        except Exception:
            metrics['cpu_percent'] = None
        try:
            metrics['memory_percent'] = float(psutil.virtual_memory().percent)
        except Exception:
            metrics['memory_percent'] = None

    if pynvml is not None:
        try:
            handle = pynvml.nvmlDeviceGetHandleByIndex(0)
            util = pynvml.nvmlDeviceGetUtilizationRates(handle)
            metrics['gpu_percent'] = float(util.gpu)
        except Exception:
            metrics['gpu_percent'] = None

    metrics['cpu_state'] = classify(metrics['cpu_percent'])
    metrics['memory_state'] = classify(metrics['memory_percent'])
    metrics['gpu_state'] = classify(metrics['gpu_percent'])
    return metrics


def _collect_message_activity() -> Dict[str, Any]:
    now = datetime.now(timezone.utc)
    window = timedelta(seconds=StatsManager.WINDOW_SECONDS)
    current_start = now - window
    previous_start = now - (2 * window)
    hour_window = timedelta(hours=1)
    hour_start = now - hour_window
    prev_hour_start = now - (2 * hour_window)

    current = {
        'total': 0,
        'channel': 0,
        'direct': 0,
        'ai': 0,
        'hour': 0,
    }
    previous = {
        'total': 0,
        'channel': 0,
        'direct': 0,
        'ai': 0,
        'hour': 0,
    }

    with messages_lock:
        for entry in messages:
            if _is_heartbeat_text(entry.get('message')):
                continue
            ts = _parse_message_timestamp(entry.get('timestamp'))
            if ts is None:
                continue
            target = None
            if ts >= current_start:
                target = current
                if ts >= hour_start:
                    target['hour'] += 1
            elif ts >= previous_start:
                target = previous
                if prev_hour_start <= ts < hour_start:
                    previous['hour'] += 1
            if target is None:
                continue
            target['total'] += 1
            if entry.get('direct'):
                target['direct'] += 1
            else:
                target['channel'] += 1
            if entry.get('is_ai'):
                target['ai'] += 1

    return {
        'current': current,
        'previous': previous,
    }


def _collect_node_activity() -> Dict[str, Any]:
    now_ts = _now()
    window = StatsManager.WINDOW_SECONDS
    recent: List[Tuple[str, float]] = []
    previous: List[Tuple[str, float]] = []

    stale_keys: List[str] = []
    for sender_key, ts in NODE_HEARTBEAT_LAST.items():
        if not sender_key or ts is None:
            continue
        age = now_ts - ts
        if age <= window:
            recent.append((sender_key, ts))
        elif age <= 2 * window:
            previous.append((sender_key, ts))
        else:
            stale_keys.append(sender_key)

    for key in stale_keys:
        NODE_HEARTBEAT_LAST.pop(key, None)

    recent.sort(key=lambda item: item[1], reverse=True)

    new_nodes = sum(1 for ts in NODE_FIRST_SEEN.values() if now_ts - ts <= window)
    prev_new_nodes = sum(1 for ts in NODE_FIRST_SEEN.values() if window < now_ts - ts <= 2 * window)
    return {
        'recent': recent,
        'recent_count': len(recent),
        'previous_count': len(previous),
        'new_24h': new_nodes,
        'prev_new_24h': prev_new_nodes,
    }


LAST_METRICS_SNAPSHOT: Optional[Dict[str, Any]] = None


def _calculate_network_usage_percent(window_seconds: int = StatsManager.WINDOW_SECONDS) -> Optional[float]:
    if window_seconds <= 0:
        return None
    if NETWORK_CAPACITY_PER_HOUR <= 0:
        return None
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(seconds=window_seconds)
    with messages_lock:
        recent_messages = 0
        for entry in messages:
            ts = _parse_message_timestamp(entry.get('timestamp'))
            if ts is None or ts < cutoff:
                continue
            recent_messages += 1
    capacity = NETWORK_CAPACITY_PER_HOUR * (window_seconds / 3600.0)
    if capacity <= 0:
        return None
    usage = min(100.0, (recent_messages / capacity) * 100.0) if capacity else 0.0
    return round(usage, 1)


def _format_meshinfo_report(language: Optional[str]) -> str:
    """Summarize recent mesh activity for the last rolling hour."""

    lang = language or LANGUAGE_FALLBACK
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=1)
    prev_cutoff = cutoff - timedelta(hours=1)

    with messages_lock:
        message_snapshot = list(messages)

    current_counts: Counter[str] = Counter()
    prev_counts: Counter[str] = Counter()
    label_cache: Dict[str, str] = {}

    def _label(key: str) -> str:
        if key not in label_cache:
            label_cache[key] = _display_sender_label(key)
        return label_cache[key]

    for entry in message_snapshot:
        if entry.get('is_ai'):
            continue
        ts = _parse_message_timestamp(entry.get('timestamp'))
        if ts is None:
            continue
        node_raw = entry.get('node_id')
        if node_raw is None:
            continue
        node_key = _safe_sender_key(node_raw) or str(node_raw)
        timestamp = ts
        if timestamp >= cutoff:
            current_counts[node_key] += 1
            _label(node_key)
        elif prev_cutoff <= timestamp < cutoff:
            prev_counts[node_key] += 1
            _label(node_key)

    current_nodes = set(current_counts.keys())
    prev_nodes = set(prev_counts.keys())

    cutoff_epoch = cutoff.timestamp()
    new_node_ids = [key for key, first_seen in NODE_FIRST_SEEN.items() if first_seen >= cutoff_epoch]
    new_node_ids.sort()
    new_node_labels = [_label(node_id) for node_id in new_node_ids]

    left_node_ids = sorted(prev_nodes - current_nodes)
    left_node_labels = [_label(node_id) for node_id in left_node_ids]

    top_nodes = current_counts.most_common(3)

    lines: List[str] = [
        translate(lang, 'meshinfo_header', "Mesh network summary (last hour)"),
    ]

    if new_node_labels:
        lines.append(
            translate(
                lang,
                'meshinfo_new_nodes_some',
                "New nodes: {count} ({list})",
                count=len(new_node_labels),
                list=", ".join(new_node_labels),
            )
        )
    else:
        lines.append(translate(lang, 'meshinfo_new_nodes_none', "New nodes: none"))

    if left_node_labels:
        lines.append(
            translate(
                lang,
                'meshinfo_left_nodes_some',
                "Nodes that left: {count} ({list})",
                count=len(left_node_labels),
                list=", ".join(left_node_labels),
            )
        )
    else:
        lines.append(
            translate(lang, 'meshinfo_left_nodes_none', "No nodes dropped out in the last hour."),
        )

    active_count = len(current_nodes)
    lines.append(
        translate(
            lang,
            'meshinfo_active_nodes',
            "Active nodes (1h): {count}",
            count=active_count,
        )
    )

    total_messages = sum(current_counts.values())
    lines.append(
        translate(
            lang,
            'meshinfo_message_volume',
            "Messages logged (1h): {count}",
            count=total_messages,
        )
    )

    if top_nodes:
        formatted_top = ", ".join(f"{_label(node)} ({count})" for node, count in top_nodes)
        lines.append(
            translate(
                lang,
                'meshinfo_top_nodes',
                "Top nodes by traffic: {list}",
                list=formatted_top,
            )
        )
    else:
        lines.append(translate(lang, 'meshinfo_top_nodes_none', "No traffic recorded in the last hour"))

    usage_percent = _calculate_network_usage_percent(StatsManager.WINDOW_SECONDS)
    if usage_percent is not None:
        lines.append(
            translate(
                lang,
                'meshinfo_network_usage',
                "Approximate network usage: {percent}% (last hour)",
                percent=f"{usage_percent:.1f}",
            )
        )
    else:
        lines.append(
            translate(lang, 'meshinfo_network_usage_unknown', "Network usage data unavailable."),
        )

    lines.append(
        translate(
            lang,
            'meshinfo_avg_batt_unknown',
            "Battery data unavailable.",
        )
    )

    return "\n".join(lines)


def _format_stats_report(language: Optional[str]) -> str:
    snapshot = STATS.snapshot()
    top_users = snapshot.get('top_users_24h') or []
    most_active_label = "n/a"
    if top_users:
        user_id, interaction_count = top_users[0]
        if user_id:
            display = get_node_shortname(user_id) or str(user_id)
        else:
            display = "unknown"
        most_active_label = f"{display} ({interaction_count})"

    mailboxes_created = snapshot.get('mailboxes_created_24h', 0) or 0
    ai_processed = snapshot.get('ai_processed_24h', 0) or 0
    games_played = snapshot.get('games_24h', 0) or 0
    game_breakdown = snapshot.get('games_breakdown') or {}
    if game_breakdown:
        top_game, top_game_count = max(game_breakdown.items(), key=lambda item: item[1])
        pretty_game = top_game.replace('_', ' ').title()
        most_popular_game = f"{pretty_game} ({top_game_count})"
    else:
        most_popular_game = "n/a"

    network_usage = _calculate_network_usage_percent(StatsManager.WINDOW_SECONDS)
    network_label = f"{network_usage:.1f}%" if network_usage is not None else "n/a"
    active_nodes = snapshot.get('active_users_24h', 0) or 0

    lines = [
        f"most active user: {most_active_label}",
        f"mailboxes: {mailboxes_created}",
        f"ollama instances: {ai_processed}",
        f"games played: {games_played}",
        f"most popular game: {most_popular_game}",
        f"average network usage %: {network_label}",
        f"number of active nodes: {active_nodes}",
    ]
    return "\n".join(lines)


def _value_delta(current: Optional[Union[int, float]], previous: Optional[Union[int, float]]) -> Dict[str, Optional[Union[int, float]]]:
    if current is None:
        return {'value': None, 'delta': None}
    if previous is None:
        return {'value': current, 'delta': 0}
    return {'value': current, 'delta': current - previous}


def _gather_dashboard_metrics() -> Dict[str, Any]:
    global LAST_METRICS_SNAPSHOT
    now = datetime.now(timezone.utc)
    uptime_delta = now - server_start_time if server_start_time else timedelta(0)
    uptime_label = _humanize_uptime(uptime_delta)
    stats_snapshot = STATS.snapshot()
    stats_previous = stats_snapshot.get('previous', {}) or {}
    message_activity = _collect_message_activity()
    node_activity = _collect_node_activity()

    try:
        mailboxes_total = len(MAIL_MANAGER.store.list_mailboxes())
    except Exception:
        mailboxes_total = None

    try:
        queue_size = response_queue.qsize()
    except Exception:
        queue_size = -1

    prev_cache = LAST_METRICS_SNAPSHOT or {}

    message_metrics: Dict[str, Dict[str, Optional[int]]] = {}
    message_totals_snapshot = stats_snapshot.get('message_totals', {}) or {}
    prev_message_metrics = prev_cache.get('message_activity') if isinstance(prev_cache.get('message_activity'), dict) else {}
    prev_total_value = None
    if isinstance(prev_message_metrics, dict):
        total_entry = prev_message_metrics.get('total')
        if isinstance(total_entry, dict):
            prev_total_value = total_entry.get('value')
    total_current_val = message_totals_snapshot.get('total')
    if total_current_val is None:
        total_current_val = message_activity['current'].get('total', 0)
    message_metrics['total'] = _value_delta(total_current_val, prev_total_value)
    for key in ('channel', 'direct', 'ai', 'hour'):
        current_val = message_activity['current'].get(key, 0)
        previous_val = message_activity['previous'].get(key, 0)
        message_metrics[key] = _value_delta(current_val, previous_val)

    recent_nodes_list: List[Tuple[str, float]] = node_activity.get('recent', []) or []
    prev_recent_count = node_activity.get('previous_count', 0)
    node_metrics = {
        'current': _value_delta(len(recent_nodes_list), prev_recent_count),
        'new_24h': _value_delta(node_activity.get('new_24h', 0), node_activity.get('prev_new_24h', 0)),
        'roster': [
            {
                'sender_key': sender,
                'label': _display_sender_label(sender),
                'last_heartbeat': datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
                if isinstance(ts, (int, float))
                else None,
            }
            for sender, ts in recent_nodes_list
        ],
    }

    active_users_metric = _value_delta(
        stats_snapshot.get('active_users_24h', 0),
        stats_previous.get('active_users_24h', 0),
    )
    new_onboards_metric = _value_delta(
        stats_snapshot.get('new_onboards_24h', 0),
        stats_previous.get('new_onboards_24h', 0),
    )

    games_metric = {
        'value': stats_snapshot.get('games_24h', 0),
        'delta': stats_snapshot.get('games_24h', 0) - stats_previous.get('games_24h', 0),
        'breakdown': stats_snapshot.get('games_breakdown', {}),
    }

    mail_metrics = {
        'sent_24h': _value_delta(
            stats_snapshot.get('mail_sent_24h', 0),
            stats_previous.get('mail_sent_24h', 0),
        ),
        'new_mailboxes_24h': _value_delta(
            stats_snapshot.get('mailboxes_created_24h', 0),
            stats_previous.get('mailboxes_created_24h', 0),
        ),
        'total_mailboxes': _value_delta(
            mailboxes_total,
            prev_cache.get('mail', {}).get('total_mailboxes', {}).get('value')
            if isinstance(prev_cache.get('mail'), dict)
            else None,
        ),
    }

    ai_requests_metric = _value_delta(
        stats_snapshot.get('ai_requests_24h', 0),
        stats_previous.get('ai_requests_24h', 0),
    )
    ai_processed_metric = _value_delta(
        stats_snapshot.get('ai_processed_24h', 0),
        stats_previous.get('ai_processed_24h', 0),
    )

    queue_metric = _value_delta(queue_size, prev_cache.get('queue_size'))

    recent_onboard_records = stats_snapshot.get('recent_onboards', []) or []
    recent_onboard_list = []
    for record in recent_onboard_records:
        sender = record.get('sender_key')
        ts = record.get('timestamp')
        recent_onboard_list.append({
            'sender_key': sender,
            'label': _display_sender_label(sender),
            'timestamp': datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
            if isinstance(ts, (int, float))
            else None,
        })

    try:
        onboard_roster_raw = ONBOARDING_MANAGER.list_completed(limit=50)
    except Exception:
        onboard_roster_raw = []
    onboard_roster: List[Dict[str, Any]] = []
    for entry in onboard_roster_raw:
        sender = entry.get('sender_key')
        label = entry.get('label') or _display_sender_label(sender)
        completed_iso = entry.get('completed_at_iso')
        if not completed_iso:
            ts_val = entry.get('completed_at')
            if isinstance(ts_val, (int, float)):
                completed_iso = datetime.fromtimestamp(ts_val, tz=timezone.utc).isoformat()
        onboard_roster.append({
            'sender_key': sender,
            'label': label,
            'completed_at': completed_iso,
            'language': entry.get('language'),
            'mailbox': entry.get('mailbox'),
        })

    metrics = {
        'timestamp': now.isoformat(),
        'uptime_human': uptime_label,
        'uptime_seconds': int(uptime_delta.total_seconds()),
        'restart_count': restart_count,
        'connection_status': connection_status,
        'message_activity': message_metrics,
        'message_totals': message_totals_snapshot,
        'node_activity': node_metrics,
        'active_users': active_users_metric,
        'new_onboards': new_onboards_metric,
        'games': games_metric,
        'mail': mail_metrics,
        'ai_requests': ai_requests_metric,
        'ai_processed': ai_processed_metric,
        'queue': queue_metric,
        'queue_size': queue_size,
        'ai_avg_response_ms': stats_snapshot.get('ai_avg_response_ms'),
        'ai_avg_recent_ms': stats_snapshot.get('ai_avg_recent_ms'),
    }
    metrics['onboarding'] = {
        'recent': recent_onboard_list,
        'roster': onboard_roster,
        'total': len(onboard_roster),
    }
    metrics['features'] = gather_feature_snapshot()
    metrics['config_overview'] = _build_config_overview()

    # Ack telemetry summary (last 24h)
    try:
        now_ts = time.time()
        window = StatsManager.WINDOW_SECONDS
        with STATS.lock:
            ack_list = [rec for rec in STATS.ack_events if rec and (now_ts - rec[0] <= window)]
        dm_first_total = sum(1 for ts, att, ok, route in ack_list if route == 'dm' and int(att) == 1)
        dm_first_ok = sum(1 for ts, att, ok, route in ack_list if route == 'dm' and int(att) == 1 and bool(ok))
        dm_resend_total = sum(1 for ts, att, ok, route in ack_list if route == 'dm' and int(att) > 1)
        dm_resend_ok = sum(1 for ts, att, ok, route in ack_list if route == 'dm' and int(att) > 1 and bool(ok))
        def _pct(ok, total):
            return None if total <= 0 else round(100.0 * (ok / total), 1)
        metrics['ack'] = {
            'dm': {
                'first_total': dm_first_total,
                'first_ok': dm_first_ok,
                'first_rate': _pct(dm_first_ok, dm_first_total),
                'resend_total': dm_resend_total,
                'resend_ok': dm_resend_ok,
                'resend_rate': _pct(dm_resend_ok, dm_resend_total),
                'events': dm_first_total + dm_resend_total,
            }
        }
    except Exception:
        metrics['ack'] = {'dm': {}}

    LAST_METRICS_SNAPSHOT = metrics
    return metrics


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


def _node_display_label(node_id: Any) -> str:
    if node_id == "WebUI":
        return "WebUI"
    key = _safe_sender_key(node_id)
    if key:
        return key
    return str(node_id)


def _display_sender_label(sender_key: Optional[str]) -> str:
    if not sender_key:
        return "Unknown"
    label = None
    manager = globals().get('ONBOARDING_MANAGER')
    if manager is not None:
        try:
            label = manager.get_sender_label(sender_key)
        except Exception:
            label = None
    if label:
        return str(label)
    node_id = parse_node_id(sender_key)
    if node_id is not None:
        try:
            return get_node_shortname(node_id)
        except Exception:
            pass
    return str(sender_key)

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


def _cmd_reply(cmd: Optional[str], message: str) -> PendingReply:
    reason = f"{cmd} command" if cmd else "command reply"
    return PendingReply(str(message), reason)

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
    "Offline wiki index has no entries",
  )
  if any(s in line for s in spam):
    return False

  if '💓 HB' in line:
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
    "📡 Broadcast",
    "📤 Sending direct",
    "📤 Direct send",
    "Sent chunk ",
    "[AsyncAI]",
    "Processing:",
    "Generated response",
    "Completed response",
    "No response generated",
    "Error processing response",
    "EMERGENCY",
    "[UI] ",
    "🖥️ ",
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

MESH-MASTER v1.99.0 by: MR_TBOT (https://mr-tbot.com)
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
FEATURE_FLAGS_FILE = "feature_flags.json"

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

def _get_dynamic_command_aliases() -> Dict[str, str]:
    try:
        data = commands_config.get('command_aliases')
        if not isinstance(data, dict):
            return {}
        aliases: Dict[str, str] = {}
        for k, v in data.items():
            if not isinstance(k, str) or not isinstance(v, str):
                continue
            alias = k if k.startswith('/') else f'/{k}'
            target = v if v.startswith('/') else f'/{v}'
            aliases[alias.lower()] = target.lower()
        return aliases
    except Exception:
        return {}
CONFIG_LOCK = threading.Lock()


CONFIG_OVERVIEW_LAYOUT: "OrderedDict[str, Dict[str, Any]]" = OrderedDict([
    (        "general",
        {
            "label": "General Settings",
            "keys": [
                "debug",
                "clean_logs",
                "language_selection",
                "local_location_string",
                "ai_node_name",
                "start_on_boot",
                "max_message_log",
                "default_personality_id",
                "user_ai_settings_file",
                "mail_search_timeout",
                "async_response_queue_max",
            ],
        },
    ),
    (        "mesh_interface",
        {
            "label": "Mesh Interface",
            "keys": ["use_mesh_interface", "serial_port", "serial_baud"],
        },
    ),
    (
        "wifi",
        {
            "label": "Wi-Fi",
            "keys": ["use_wifi", "wifi_host", "wifi_port"],
        },
    ),
    (
        "ai_provider",
        {
            "label": "AI Provider",
            "keys": [
                "ai_provider",
                "system_prompt",
                "chunk_size",
                "max_ai_chunks",
                "chunk_buffer_seconds",
                "ai_chill_mode",
                "ai_chill_queue_limit",
            ],
        },
    ),
    (
        "ollama",
        {
            "label": "Ollama",
            "keys": [
                "ollama_url",
                "ollama_model",
                "ollama_timeout",
                "ollama_context_chars",
                "ollama_num_ctx",
                "ollama_max_messages",
            ],
        },
    ),
    (
        "home_assistant",
        {
            "label": "Home Assistant",
            "keys": [
                "home_assistant_enabled",
                "home_assistant_url",
                "home_assistant_token",
                "home_assistant_timeout",
                "home_assistant_enable_pin",
                "home_assistant_secure_pin",
                "home_assistant_channel_index",
            ],
        },
    ),
    (
        "messaging",
        {
            "label": "Messaging",
            "keys": ["reply_in_channels", "reply_in_directs", "channel_names"],
        },
    ),
    (
        "knowledge",
        {
            "label": "Knowledge Stores",
            "keys": [
                "bible_progress_file",
                "meshtastic_knowledge_file",
                "meshtastic_kb_max_context_chars",
                "meshtastic_kb_cache_ttl",
            ],
        },
    ),
    (
        "automessages",
        {
            "label": "Automessages",
            "keys": [
                "mail_notify_enabled",
                "mail_notify_reminders_enabled",
                "mail_notify_max_reminders",
                "mail_notify_reminder_hours",
                "mail_notify_include_self",
                "automessage_quiet_hours",
                "cooldown_enabled",
            ],
        },
    ),
    (
        "radio_settings",
        {
            "label": "Radio Settings",
            "keys": [
                "radio_settings_info",
            ],
        },
    ),
    (
        "resending_no_ack",
        {
            "label": "Resending (No Ack)",
            "keys": [
                "resend_enabled",
                "resend_usage_threshold_percent",
                "resend_dm_only",
                "resend_broadcast_enabled",
                "resend_suffix_enabled",
                "resend_system_attempts",
                "resend_system_interval_seconds",
                "resend_user_attempts",
                "resend_user_interval_seconds",
                "resend_jitter_seconds",
                "resend_telemetry_enabled",
            ],
        },
    ),
])

CONFIG_HIDDEN_KEYS = {
    "openai_api_key",
    "openai_model",
    "openai_timeout",
    "openai_base_url",
    "lmstudio_url",
    "lmstudio_model",
    "lmstudio_timeout",
    "lmstudio_api_key",
    "notify_active_start_hour",
    "notify_active_end_hour",
    "mail_notify_quiet_hours_enabled",
    "mail_quiet_start_hour",
    "mail_quiet_end_hour",
}

CONFIG_KEY_FRIENDLY_NAMES: Dict[str, str] = {
    "debug": "Debug logging",
    "clean_logs": "Clean log output",
    "language_selection": "Default language",
    "local_location_string": "Location note",
    "ai_node_name": "AI callsign",
    "start_on_boot": "Auto-start on boot",
    "max_message_log": "Message history limit",
    "default_personality_id": "Default personality",
    "user_ai_settings_file": "AI user settings file",
    "mail_search_timeout": "Mail search timeout",
    "async_response_queue_max": "Response queue max",
    "use_mesh_interface": "Use mesh interface",
    "serial_port": "Serial port",
    "serial_baud": "Serial baud rate",
    "use_wifi": "Use Wi-Fi link",
    "wifi_host": "Wi-Fi host",
    "wifi_port": "Wi-Fi port",
    "ai_provider": "AI provider",
    "system_prompt": "System prompt",
    "chunk_size": "Chunk size",
    "max_ai_chunks": "Max AI chunks",
    "chunk_buffer_seconds": "Chunk buffer",
    "ai_chill_mode": "AI chill mode",
    "ai_chill_queue_limit": "Chill queue limit",
    "ollama_url": "Ollama URL",
    "ollama_model": "Ollama model",
    "ollama_timeout": "Ollama timeout",
    "ollama_context_chars": "Ollama context chars",
    "ollama_num_ctx": "Ollama context window",
    "ollama_max_messages": "Ollama message cap",
    "home_assistant_enabled": "Home Assistant enabled",
    "home_assistant_url": "Home Assistant URL",
    "home_assistant_token": "Home Assistant token",
    "home_assistant_timeout": "Home Assistant timeout",
    "home_assistant_enable_pin": "Require HA PIN",
    "home_assistant_secure_pin": "Home Assistant PIN",
    "home_assistant_channel_index": "HA channel index",
    "reply_in_channels": "Reply in channels",
    "reply_in_directs": "Reply in DMs",
    "channel_names": "Channel names",
    "bible_progress_file": "Bible progress file",
    "meshtastic_knowledge_file": "Mesh knowledge file",
    "meshtastic_kb_max_context_chars": "Knowledge context limit",
    "meshtastic_kb_cache_ttl": "Knowledge cache TTL",
    "automessage_quiet_hours": "Automessage quiet hours",
    "mail_notify_enabled": "Mail alerts",
    "mail_notify_reminders_enabled": "Mail reminders",
    "mail_notify_max_reminders": "Reminder count",
    "mail_notify_reminder_hours": "Reminder spacing (hours)",
    "mail_notify_include_self": "Notify sender",
    # Mesh cooldown notices
    "cooldown_enabled": "Cooldown notices",
    "notify_active_start_hour": "Quiet hours start",
    "notify_active_end_hour": "Quiet hours end",
    # Resending (No Ack)
    "resend_enabled": "Enable resend",
    "resend_usage_threshold_percent": "Usage threshold %",
    "resend_dm_only": "DM-only scope",
    "resend_broadcast_enabled": "Allow broadcast resend",
    "resend_suffix_enabled": "Retry suffix '(Nth try)'",
    "resend_system_attempts": "System resend attempts",
    "resend_system_interval_seconds": "System resend interval (s)",
    "resend_user_attempts": "User resend attempts",
    "resend_user_interval_seconds": "User resend interval (s)",
    "resend_jitter_seconds": "Resend jitter (s)",
    "resend_telemetry_enabled": "Ack telemetry",
    # Radio panel helper
    "radio_settings_info": "Radio settings",
}

CONFIG_KEY_EXPLAINERS: Dict[str, str] = {
    "debug": "Enable verbose troubleshooting logs. Turns off noise filtering so raw protobuf chatter is visible.",
    "clean_logs": "Filters noisy protobuf messages and adds emoji markers for easier scanning in the activity stream.",
    "language_selection": "Sets the default language for canned replies, menus, and status messages.",
    "local_location_string": "Optional text describing this node's location that appears in certain status replies.",
    "ai_node_name": "Callsign the AI uses when introducing itself in conversations.",
    "start_on_boot": "If enabled, Mesh-Master starts automatically whenever the host computer boots.",
    "max_message_log": "Maximum number of recent messages kept in memory for dashboards and history commands.",
    "default_personality_id": "Primary AI persona applied to replies when no user override is active.",
    "user_ai_settings_file": "Path to the JSON file storing per-user AI preferences and overrides.",
    "mail_search_timeout": "How many seconds the system searches mesh mail before aborting a lookup.",
    "async_response_queue_max": "Upper limit on queued outbound responses to avoid flooding the mesh.",
    "use_mesh_interface": "Enable the serial Meshtastic interface for direct radio control.",
    "serial_port": "Path to the serial device connected to the Meshtastic radio.",
    "serial_baud": "Serial baud rate used when communicating with the radio.",
    "use_wifi": "Enable the Wi-Fi socket interface instead of direct serial access.",
    "wifi_host": "Hostname or IP address for the Wi-Fi-connected node.",
    "wifi_port": "TCP port number for the Wi-Fi mesh link.",
    "ai_provider": "Selects the large language model provider powering AI replies.",
    "system_prompt": "Base instruction prompt prepended to every AI conversation.",
    "chunk_size": "Character count for each streaming chunk returned back to the radio.",
    "max_ai_chunks": "Maximum number of streaming chunks allowed per AI reply.",
    "chunk_buffer_seconds": "Delay between streaming chunks to prevent radio congestion.",
    "ollama_url": "URL Mesh-Master uses to reach the Ollama API.",
    "ollama_model": "Ollama model identifier used for completions.",
    "ollama_timeout": "Seconds to wait before giving up on an Ollama request.",
    "ollama_context_chars": "Approximate character budget kept when building the Ollama prompt.",
    "ollama_num_ctx": "Context window hint passed to Ollama for supported models.",
    "ollama_max_messages": "Maximum recent chat messages forwarded to Ollama for context.",
    "ai_chill_mode": "When enabled, temporarily halts new Ollama requests if the async queue is busy, and DMs users to wait.",
    "ai_chill_queue_limit": "Maximum queued Ollama tasks to allow when chill mode is enabled. New requests beyond this are deferred with a DM.",
    "home_assistant_enabled": "Turns Home Assistant automations on or off.",
    "home_assistant_url": "Base URL for the Home Assistant instance Mesh-Master talks to.",
    "home_assistant_token": "Long-lived Home Assistant access token. Keep this secret.",
    "home_assistant_timeout": "Seconds to wait for Home Assistant responses before aborting.",
    "home_assistant_enable_pin": "Require a PIN from chat operators before executing Home Assistant commands.",
    "home_assistant_secure_pin": "PIN code operators must DM to unlock Home Assistant actions. Keep private.",
    "home_assistant_channel_index": "Mesh channel index used for Home Assistant status updates.",
    "reply_in_channels": "Allow the AI to reply inside shared channel conversations.",
    "reply_in_directs": "Allow the AI to respond to direct messages and DMs.",
    "channel_names": "Friendly display names for mesh channels when formatting responses.",
    "bible_progress_file": "File tracking which Bible verses were last shared so rotations stay fresh.",
    "meshtastic_knowledge_file": "Local knowledge base JSON used for /meshinfo and related lookups.",
    "meshtastic_kb_max_context_chars": "Maximum number of characters to pull from the knowledge base for a reply.",
    "meshtastic_kb_cache_ttl": "How long knowledge base lookups stay cached before refreshing.",
    "automessage_quiet_hours": "Toggle automated quiet hours for reminder delivery and set the overnight window.",
    "mail_notify_enabled": "Master switch for mesh mail alerts. When disabled, no inbox notifications are sent.",
    "mail_notify_reminders_enabled": "Controls whether Mesh Master schedules follow-up reminders for unread mail after the initial alert.",
    "mail_notify_max_reminders": "How many reminder messages each subscriber receives after the first alert if their mail remains unread.",
    "mail_notify_reminder_hours": "Spacing between reminder messages (in hours) once the reminder window begins.",
    "mail_notify_include_self": "If enabled, the author of a new mail message also receives notification pings for that mailbox.",
    "notify_active_start_hour": "Local hour (0–23) when quiet hours end and automated reminders may resume.",
    "notify_active_end_hour": "Local hour (0–23) when quiet hours begin; reminders pause outside the active window.",
    "cooldown_enabled": "Send a short, polite notice to users who are sending too many messages too quickly, asking them to slow down for mesh health.",
    # Resending (No Ack)
    "resend_enabled": "Enable automatic resends when messages do not receive ACKs (directs) or under configured policy (broadcasts).",
    "resend_usage_threshold_percent": "Only perform resends when approximate network usage is below this percentage.",
    "resend_dm_only": "If enabled, apply resends only to direct messages (DMs). Disable to also allow channel messages.",
    "resend_broadcast_enabled": "Allow resending for broadcast/channel messages (no per-chunk ACK available).",
    "resend_suffix_enabled": "Append a short retry suffix such as '(2nd try)' to the last resent chunk. If disabled, resends are silent.",
    "resend_system_attempts": "Maximum resend attempts for system/AI-originated messages.",
    "resend_system_interval_seconds": "Seconds to wait between resend attempts for system/AI messages.",
    "resend_user_attempts": "Maximum resend attempts for user-triggered replies in DMs.",
    "resend_user_interval_seconds": "Seconds between resend attempts for user-triggered replies in DMs.",
    "resend_jitter_seconds": "Small random jitter added to resend intervals to avoid collisions.",
    "resend_telemetry_enabled": "Record anonymous, in-memory ACK telemetry for quality tracking.",
    # Radio panel helper
    "radio_settings_info": "This category points to the live Radio Settings panel above for managing hop limit and channels.",
}


def _format_hour_label(hour: int) -> str:
    return f"{int(hour) % 24:02d}:00"


def _build_quiet_hours_entry() -> Optional[Dict[str, Any]]:
    active_start = int(config.get("notify_active_start_hour", 0) or 0) % 24
    active_end = int(config.get("notify_active_end_hour", 0) or 0) % 24
    default_quiet_start = active_end
    default_quiet_end = active_start
    quiet_start = int(config.get("mail_quiet_start_hour", default_quiet_start) or default_quiet_start) % 24
    quiet_end = int(config.get("mail_quiet_end_hour", default_quiet_end) or default_quiet_end) % 24
    enabled_flag = bool(config.get("mail_notify_quiet_hours_enabled", MAIL_NOTIFY_QUIET_HOURS_ENABLED))
    quiet_enabled = enabled_flag and quiet_start != quiet_end

    if quiet_enabled:
        value_label = f"Quiet {_format_hour_label(quiet_start)} → {_format_hour_label(quiet_end)}"
        tooltip = (
            f"Quiet hours enabled. Reminders pause from {_format_hour_label(quiet_start)}"
            f" to {_format_hour_label(quiet_end)} local time."
        )
    else:
        value_label = "Disabled (24/7 reminders)"
        tooltip = "Quiet hours disabled; reminders may send at any time."

    raw = {
        "enabled": bool(quiet_enabled),
        "start": quiet_start,
        "end": quiet_end,
    }

    return {
        "key": "automessage_quiet_hours",
        "label": _humanize_config_key("automessage_quiet_hours"),
        "value": value_label,
        "tooltip": tooltip,
        "raw": raw,
        "type": "automessage_quiet_hours",
        "explainer": _build_config_explainer("automessage_quiet_hours", value_label, tooltip),
    }


def _humanize_config_key(key: str) -> str:
    if not key:
        return "Setting"
    return CONFIG_KEY_FRIENDLY_NAMES.get(key, key.replace('_', ' ').strip().title())


_DEF_EMPTY_VALUES = {"—", "(empty)", ""}


def _build_config_explainer(key: str, display_value: str, tooltip_value: str) -> str:
    base = CONFIG_KEY_EXPLAINERS.get(key)
    friendly = _humanize_config_key(key)
    value_text = tooltip_value or display_value or ""
    value_text = value_text.strip()
    if value_text in _DEF_EMPTY_VALUES:
        value_text = ""
    if base:
        return f"{base} Currently set to {value_text}." if value_text else base
    if value_text:
        return f"{friendly} is currently set to {value_text}."
    return f"{friendly} has no value configured."

_SENSITIVE_CONFIG_KEYWORDS = ("token", "pass", "secret", "pin", "key")
_SENSITIVE_CONFIG_KEYS = {
    "home_assistant_token",
    "home_assistant_secure_pin",
}


def _mask_config_value(value: Any) -> str:
    if not value:
        return "(not set)"
    if isinstance(value, str):
        masked_len = min(8, max(4, len(value)))
        return "•" * masked_len
    return "[hidden]"


def _stringify_mapping(value: Dict[Any, Any]) -> str:
    parts: List[str] = []
    for key, val in value.items():
        parts.append(f"{key}: {val}")
    return ", ".join(parts)


def _format_config_value(key: str, value: Any) -> Tuple[str, str]:
    if key not in config:
        return "—", "—"
    if value is None:
        return "—", "—"
    key_lower = key.lower()
    if key in _SENSITIVE_CONFIG_KEYS or any(token in key_lower for token in _SENSITIVE_CONFIG_KEYWORDS):
        masked = _mask_config_value(value)
        return masked, masked
    if isinstance(value, bool):
        label = "Enabled" if value else "Disabled"
        return label, label
    if isinstance(value, (int, float)):
        text = str(value)
        return text, text
    if isinstance(value, dict):
        joined = _stringify_mapping(value) if value else "(empty)"
        display = joined if len(joined) <= 80 else joined[:77].rstrip() + "…"
        return display, joined
    if isinstance(value, (list, tuple, set)):
        joined = ", ".join(str(item) for item in value) if value else "(empty)"
        display = joined if len(joined) <= 80 else joined[:77].rstrip() + "…"
        return display, joined
    text = str(value).strip()
    if not text:
        return "(empty)", "(empty)"
    clean = text.replace("\n", " ⏎ ")
    tooltip = clean
    display = clean if len(clean) <= 120 else clean[:117].rstrip() + "…"
    return display, tooltip


def _config_value_kind(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


def _build_config_overview() -> Dict[str, Any]:
    sections: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    for section_id, section_info in CONFIG_OVERVIEW_LAYOUT.items():
        keys = section_info.get("keys", [])
        entries: List[Dict[str, Any]] = []
        for key in keys:
            if key == "automessage_quiet_hours":
                entry = _build_quiet_hours_entry()
                if entry:
                    entries.append(entry)
                continue
            if key in CONFIG_HIDDEN_KEYS:
                continue
            if key not in config:
                continue
            seen.add(key)
            value = config.get(key)
            display, tooltip = _format_config_value(key, value)
            entries.append(
                {
                    "key": key,
                    "label": _humanize_config_key(key),
                    "value": display,
                    "tooltip": tooltip,
                    "raw": value,
                    "type": _config_value_kind(value),
                    "explainer": _build_config_explainer(key, display, tooltip),
                }
            )
        if entries:
            sections.append(
                {
                    "id": section_id,
                    "label": section_info.get("label", section_id.replace("_", " ").title()),
                    "settings": entries,
                }
            )

    remaining_keys = [key for key in sorted(config.keys()) if key not in seen and key not in CONFIG_HIDDEN_KEYS]
    if remaining_keys:
        extra_entries: List[Dict[str, Any]] = []
        for key in remaining_keys:
            value = config.get(key)
            display, tooltip = _format_config_value(key, value)
            extra_entries.append(
                {
                    "key": key,
                    "label": _humanize_config_key(key),
                    "value": display,
                    "tooltip": tooltip,
                    "raw": value,
                    "type": _config_value_kind(value),
                    "explainer": _build_config_explainer(key, display, tooltip),
                }
            )
        sections.append({"id": "other", "label": "Other Settings", "settings": extra_entries})

    language_options = [
        {"value": "english", "label": "English"},
        {"value": "spanish", "label": "Spanish"},
    ]
    personality_options: List[Dict[str, str]] = []
    for persona_id in AI_PERSONALITY_ORDER:
        persona = AI_PERSONALITY_MAP.get(persona_id, {})
        name = persona.get("name") or persona_id
        emoji = persona.get("emoji") or ""
        label = f"{emoji} {name}".strip()
        personality_options.append({"value": persona_id, "label": label})

    metadata = {
        "language_options": language_options,
        "personality_options": personality_options,
        "hour_options": [
            {"value": hour, "label": _format_hour_label(hour)}
            for hour in range(24)
        ],
    }

    return {"sections": sections, "metadata": metadata}


def _parse_config_update_value(text: str) -> Any:
    if text is None:
        return ""
    candidate = text.strip()
    if not candidate:
        return ""
    try:
        return json.loads(candidate)
    except json.JSONDecodeError:
        return text


# -------------------------------------------------
# Feature flag storage (AI + command toggles)
# -------------------------------------------------
AI_DISABLED_MESSAGE = "⚠️ AI responses are currently disabled by the operator."

DEFAULT_FEATURE_FLAGS = {
    "ai_enabled": True,
    "disabled_commands": [],
    "message_mode": "both",
    "admin_passphrase": "",
    "auto_ping_enabled": True,
    "admin_whitelist": [],
}

MESSAGE_MODE_OPTIONS = {"both", "dm_only", "channel_only"}

_feature_flags_lock = threading.Lock()
_feature_flags: Dict[str, Any] = {}
_disabled_command_set: Set[str] = set()
_feature_flag_admins: Set[str] = set()


_INITIAL_ADMIN_WHITELIST: Set[str] = set()
AUTHORIZED_ADMINS: Set[str] = set()
AUTHORIZED_ADMIN_NAMES: Dict[str, str] = {}
_initial_admins = config.get("admin_whitelist", [])
if isinstance(_initial_admins, (list, tuple, set)):
    for entry in _initial_admins:
        if entry is None:
            continue
        sanitized = str(entry).strip()
        if not sanitized:
            continue
        _INITIAL_ADMIN_WHITELIST.add(sanitized)
        AUTHORIZED_ADMINS.add(sanitized)
        AUTHORIZED_ADMIN_NAMES.setdefault(sanitized, sanitized)
elif isinstance(_initial_admins, str):
    sanitized = _initial_admins.strip()
    if sanitized:
        _INITIAL_ADMIN_WHITELIST.add(sanitized)
        AUTHORIZED_ADMINS.add(sanitized)
        AUTHORIZED_ADMIN_NAMES.setdefault(sanitized, sanitized)


def _refresh_authorized_admins(*, retain_existing: bool = True) -> None:
    initial = set(_INITIAL_ADMIN_WHITELIST)
    combined: Set[str] = set(initial)
    combined.update(_feature_flag_admins)
    if retain_existing:
        combined.update(AUTHORIZED_ADMINS)
    AUTHORIZED_ADMINS.clear()
    AUTHORIZED_ADMINS.update(combined)
    for key in list(AUTHORIZED_ADMIN_NAMES.keys()):
        if key not in combined:
            AUTHORIZED_ADMIN_NAMES.pop(key, None)
    for key in combined:
        AUTHORIZED_ADMIN_NAMES.setdefault(key, key)


def _normalize_command_name(cmd: str) -> str:
    if not cmd:
        return ""
    token = str(cmd).strip()
    if not token:
        return ""
    if not token.startswith("/"):
        token = f"/{token}"
    return token.lower()


def _normalize_message_mode(value: Optional[Any]) -> str:
    if not value:
        return "both"
    text = str(value).strip().lower()
    if text in MESSAGE_MODE_OPTIONS:
        return text
    if text in {"dm", "direct", "dm_only"}:
        return "dm_only"
    if text in {"channel", "channels", "channel_only"}:
        return "channel_only"
    return "both"


def _apply_feature_flags(data: Dict[str, Any]) -> None:
    global _feature_flags, _disabled_command_set, _feature_flag_admins
    merged = dict(DEFAULT_FEATURE_FLAGS)
    if isinstance(data, dict):
        for key in DEFAULT_FEATURE_FLAGS.keys():
            if key in data:
                merged[key] = data.get(key)

    disabled = merged.get("disabled_commands") or []
    if not isinstance(disabled, list):
        disabled = []
    normalized = sorted({_normalize_command_name(cmd) for cmd in disabled if _normalize_command_name(cmd)})
    merged["disabled_commands"] = normalized

    merged["message_mode"] = _normalize_message_mode(merged.get("message_mode"))
    merged["admin_passphrase"] = str(merged.get("admin_passphrase") or "").strip()
    merged["auto_ping_enabled"] = bool(merged.get("auto_ping_enabled", True))

    admin_entries = merged.get("admin_whitelist") or []
    if not isinstance(admin_entries, list):
        admin_entries = []
    admin_set = {str(entry).strip() for entry in admin_entries if str(entry).strip()}
    merged["admin_whitelist"] = sorted(admin_set)
    _feature_flag_admins = admin_set

    _feature_flags = merged
    _disabled_command_set = set(normalized)
    _refresh_authorized_admins(retain_existing=False)


def _load_feature_flags_from_disk() -> Dict[str, Any]:
    data = safe_load_json(FEATURE_FLAGS_FILE, {})
    if not isinstance(data, dict):
        data = {}
    return data


def _save_feature_flags_to_disk(flags: Dict[str, Any]) -> None:
    directory = os.path.dirname(FEATURE_FLAGS_FILE)
    if directory:
        os.makedirs(directory, exist_ok=True)
    tmp_path = f"{FEATURE_FLAGS_FILE}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as fh:
        json.dump(flags, fh, ensure_ascii=False, indent=2)
    os.replace(tmp_path, FEATURE_FLAGS_FILE)


def _initialize_feature_flags() -> None:
    data = _load_feature_flags_from_disk()
    with _feature_flags_lock:
        _apply_feature_flags(data)


def get_feature_flags_snapshot() -> Dict[str, Any]:
    with _feature_flags_lock:
        return dict(_feature_flags)


def update_feature_flags(*, ai_enabled: Optional[bool] = None, disabled_commands: Optional[List[str]] = None, message_mode: Optional[str] = None, admin_passphrase: Optional[str] = None, auto_ping_enabled: Optional[bool] = None, admin_whitelist: Optional[List[str]] = None) -> Dict[str, Any]:
    with _feature_flags_lock:
        current = dict(_feature_flags)
        if ai_enabled is not None:
            current["ai_enabled"] = bool(ai_enabled)
        if disabled_commands is not None:
            normalized = sorted({_normalize_command_name(cmd) for cmd in disabled_commands if _normalize_command_name(cmd)})
            current["disabled_commands"] = normalized
        if message_mode is not None:
            current["message_mode"] = _normalize_message_mode(message_mode)
        passphrase_changed = False
        if admin_passphrase is not None:
            new_passphrase = str(admin_passphrase or "").strip()
            old_passphrase = str(current.get("admin_passphrase", "") or "").strip()
            if new_passphrase != old_passphrase:
                passphrase_changed = True
            current["admin_passphrase"] = new_passphrase
        if auto_ping_enabled is not None:
            current["auto_ping_enabled"] = bool(auto_ping_enabled)
        if admin_whitelist is not None:
            entries = admin_whitelist if isinstance(admin_whitelist, (list, tuple, set)) else []
            sanitized = sorted({str(entry).strip() for entry in entries if str(entry).strip()})
            current["admin_whitelist"] = sanitized
        _apply_feature_flags(current)
        if passphrase_changed:
            PENDING_ADMIN_REQUESTS.clear()
        try:
            _save_feature_flags_to_disk(_feature_flags)
        except Exception as exc:
            print(f"⚠️ Unable to persist feature flags: {exc}")
        return dict(_feature_flags)


def is_ai_enabled() -> bool:
    with _feature_flags_lock:
        return bool(_feature_flags.get("ai_enabled", True))


def is_auto_ping_enabled() -> bool:
    with _feature_flags_lock:
        return bool(_feature_flags.get("auto_ping_enabled", True))


def is_command_enabled(cmd: str) -> bool:
    normalized = _normalize_command_name(cmd)
    with _feature_flags_lock:
        return normalized not in _disabled_command_set


def get_message_mode() -> str:
    with _feature_flags_lock:
        mode = _feature_flags.get("message_mode", "both")
    return mode if mode in MESSAGE_MODE_OPTIONS else "both"


def get_admin_passphrase() -> str:
    with _feature_flags_lock:
        value = _feature_flags.get("admin_passphrase") or ""
    return str(value).strip()


def set_command_enabled(command: str, enabled: bool) -> Dict[str, Any]:
    snapshot = get_feature_flags_snapshot()
    disabled = set(snapshot.get("disabled_commands", []))
    normalized = _normalize_command_name(command)
    if not normalized:
        return snapshot
    if enabled:
        disabled.discard(normalized)
    else:
        disabled.add(normalized)
    return update_feature_flags(disabled_commands=sorted(disabled))


def set_message_mode(mode: str) -> Dict[str, Any]:
    normalized = _normalize_message_mode(mode)
    return update_feature_flags(message_mode=normalized)


def set_ai_enabled(enabled: bool) -> Dict[str, Any]:
    return update_feature_flags(ai_enabled=bool(enabled))


_initialize_feature_flags()

# Ensure desktop autostart defaults to enabled even if the config file lacks the key.
if not config.get("start_on_boot", False):
    config["start_on_boot"] = True
    try:
        write_atomic(CONFIG_FILE, json.dumps(config, indent=2))
    except Exception as exc:
        print(f"⚠️ Unable to persist start_on_boot default: {exc}")


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
ADMIN_PASSWORD_NORM = ADMIN_PASSWORD.strip().casefold()


def _register_admin_display(sender_key: str, sender_id: Any = None, *, label: Optional[str] = None) -> None:
    if not sender_key:
        return
    name = label
    if name is None and sender_id is not None:
        try:
            display = get_node_shortname(sender_id)
        except Exception:
            display = None
        if display:
            name = display
    if not name:
        name = sender_key
    AUTHORIZED_ADMIN_NAMES[sender_key] = str(name)


def _ensure_admin_in_feature_flags(sender_key: str) -> None:
    normalized = str(sender_key or "").strip()
    if not normalized:
        return
    snapshot = get_feature_flags_snapshot()
    current = set(snapshot.get("admin_whitelist", []))
    if normalized in current:
        return
    current.add(normalized)
    update_feature_flags(admin_whitelist=sorted(current))

PENDING_ADMIN_REQUESTS: Dict[str, Dict[str, Any]] = {}
PENDING_WIPE_REQUESTS: Dict[str, Dict[str, Any]] = {}
PENDING_WIPE_SELECTIONS: Dict[str, Dict[str, Any]] = {}
PENDING_BIBLE_NAV: Dict[str, Dict[str, Any]] = {}
PENDING_POSITION_CONFIRM: Dict[str, Dict[str, Any]] = {}
PENDING_SAVE_WIZARDS: Dict[str, Dict[str, Any]] = {}
PENDING_VIBE_SELECTIONS: Dict[str, Dict[str, Any]] = {}
PENDING_MAILBOX_SELECTIONS: Dict[str, Dict[str, Any]] = {}

SAVE_WIZARD_TIMEOUT = 5 * 60
VIBE_MENU_TIMEOUT = 3 * 60
BIBLE_NAV_LOCK = threading.Lock()

ANTISPAM_LOCK = threading.Lock()
ANTISPAM_STATE: Dict[str, Dict[str, Any]] = {}
ANTISPAM_WINDOW_SECONDS = 120  # 2 minutes
ANTISPAM_THRESHOLD = 25
ANTISPAM_SHORT_TIMEOUT = 10 * 60
ANTISPAM_LONG_TIMEOUT = 24 * 60 * 60
ANTISPAM_ESCALATION_WINDOW = 60 * 60  # 1 hour after release

COOLDOWN_LOCK = threading.Lock()
COOLDOWN_STATE: Dict[str, Dict[str, Any]] = {}
COOLDOWN_ENABLED = bool(config.get("cooldown_enabled", True))
try:
    COOLDOWN_WINDOW_SECONDS = int(config.get("cooldown_window_seconds", 5 * 60))
except (ValueError, TypeError):
    COOLDOWN_WINDOW_SECONDS = 5 * 60
COOLDOWN_WINDOW_SECONDS = max(30, COOLDOWN_WINDOW_SECONDS)
try:
    COOLDOWN_THRESHOLD_BASE = int(config.get("cooldown_threshold", 5))
except (ValueError, TypeError):
    COOLDOWN_THRESHOLD_BASE = 5
COOLDOWN_THRESHOLD_BASE = max(1, COOLDOWN_THRESHOLD_BASE)
try:
    COOLDOWN_THRESHOLD_NEW = int(config.get("cooldown_threshold_new", max(1, COOLDOWN_THRESHOLD_BASE - 2)))
except (ValueError, TypeError):
    COOLDOWN_THRESHOLD_NEW = max(1, COOLDOWN_THRESHOLD_BASE - 2)
COOLDOWN_THRESHOLD_NEW = max(1, COOLDOWN_THRESHOLD_NEW)
try:
    COOLDOWN_NEW_NODE_HOURS = float(config.get("cooldown_new_node_hours", 24.0))
except (ValueError, TypeError):
    COOLDOWN_NEW_NODE_HOURS = 24.0
COOLDOWN_NEW_NODE_HOURS = max(0.0, COOLDOWN_NEW_NODE_HOURS)
COOLDOWN_NEW_NODE_SECONDS = COOLDOWN_NEW_NODE_HOURS * 3600.0
COOLDOWN_ALLOWLIST: Set[str] = set()
raw_cooldown_allow = config.get("cooldown_allowlist", [])
if isinstance(raw_cooldown_allow, (list, tuple, set)):
    for entry in raw_cooldown_allow:
        if entry is None:
            continue
        COOLDOWN_ALLOWLIST.add(str(entry).strip().lower())
elif isinstance(raw_cooldown_allow, str):
    for part in raw_cooldown_allow.split(','):
        part = part.strip()
        if part:
            COOLDOWN_ALLOWLIST.add(part.lower())
COOLDOWN_THRESHOLD = COOLDOWN_THRESHOLD_BASE
COOLDOWN_MESSAGES = [
    "🚦 Mesh speed limit reached—take a breather for a few minutes!",
    "🧊 Cooling fans engaged. Give the mesh a sec to chill, friend!",
    "🍋 Too much zest! Let’s squeeze the pauses between messages for a bit.",
    "🛜 Buffering your enthusiasm. Hang tight before the next dispatch!",
    "🧵 Thread’s smoking—step back and let it cool down for a moment.",
    "🥤 Sip break! Hydrate while the radios catch their breath.",
    "🔧 Wrench check! Pause a few to keep the mesh from rattling apart.",
    "🎛️ Dial back the chatter a notch so everyone stays loud and clear.",
]

NODE_FIRST_SEEN: Dict[str, float] = {}

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


OFFLINE_WIKI_ENABLED = bool(config.get("offline_wiki_enabled", True))
_offline_dir = config.get("offline_wiki_dir", "data/offline_wiki")
OFFLINE_WIKI_DIR = Path(_offline_dir) if _offline_dir else Path("data/offline_wiki")
_offline_index = config.get("offline_wiki_index")
OFFLINE_WIKI_INDEX = Path(_offline_index) if _offline_index else OFFLINE_WIKI_DIR / "index.json"
try:
    OFFLINE_WIKI_SUMMARY_LIMIT = max(200, int(config.get("offline_wiki_summary_chars", 400)))
except (TypeError, ValueError):
    OFFLINE_WIKI_SUMMARY_LIMIT = 400
try:
    OFFLINE_WIKI_CONTEXT_LIMIT = max(2000, int(config.get("offline_wiki_context_chars", 40000)))
except (TypeError, ValueError):
    OFFLINE_WIKI_CONTEXT_LIMIT = 40000
OFFLINE_WIKI_STORE = OfflineWikiStore(OFFLINE_WIKI_INDEX, base_dir=OFFLINE_WIKI_DIR) if OFFLINE_WIKI_ENABLED else None
if OFFLINE_WIKI_STORE and not OFFLINE_WIKI_STORE.is_ready():
    err = OFFLINE_WIKI_STORE.error_message() or f"Offline wiki index has no entries ({OFFLINE_WIKI_INDEX})."
    clean_log(err, "⚠️", show_always=True, rate_limit=False)


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


def _stop_bible_autoscroll(sender_key: Optional[str]) -> bool:
    if not sender_key:
        return False
    stopped = False
    with BIBLE_AUTOSCROLL_LOCK:
        state = BIBLE_AUTOSCROLL_STATE.get(sender_key)
        if state:
            stop_event = state.get('stop_event')
            if stop_event and not stop_event.is_set():
                stop_event.set()
            BIBLE_AUTOSCROLL_STATE.pop(sender_key, None)
            stopped = True
        pending = BIBLE_AUTOSCROLL_PENDING.pop(sender_key, None)
        if pending:
            stop_event = pending.get('stop_event')
            if stop_event and not stop_event.is_set():
                stop_event.set()
            stopped = True
    return stopped


def _prepare_bible_autoscroll(sender_key: Optional[str], is_direct: bool, channel_idx: Optional[int]):
    if not sender_key:
        return PendingReply("📖 Auto-scroll isn't available right now.", "/bible autoscroll")
    lang = (PENDING_BIBLE_NAV.get(sender_key, {}).get('language') or LANGUAGE_FALLBACK)
    if not is_direct:
        return PendingReply(
            translate(lang, 'bible_autoscroll_dm_only', "AutoScroll is only available in DM mode"),
            "/bible autoscroll",
        )
    if sender_key not in PENDING_BIBLE_NAV:
        return PendingReply(
            translate(lang, 'bible_autoscroll_need_nav', "📖 Use /bible first, then reply 22 to auto-scroll."),
            "/bible autoscroll",
        )
    with BIBLE_AUTOSCROLL_LOCK:
        existing = BIBLE_AUTOSCROLL_STATE.get(sender_key)
        if existing and existing.get('stop_event'):
            existing['stop_event'].set()
            BIBLE_AUTOSCROLL_STATE.pop(sender_key, None)
            return PendingReply(
                translate(lang, 'bible_autoscroll_stop', "⏹️ Auto-scroll paused. Reply 22 later to resume."),
                "/bible autoscroll",
            )
        stop_event = threading.Event()
        BIBLE_AUTOSCROLL_PENDING[sender_key] = {
            'channel_idx': channel_idx,
            'requested_at': time.time(),
            'stop_event': stop_event,
        }
        BIBLE_AUTOSCROLL_STATE[sender_key] = {
            'running': True,
            'stop_event': stop_event,
        }
    return PendingReply(
        translate(lang, 'bible_autoscroll_start', "📖 Auto-scroll activated. Next verses every 12 seconds (30 total). Use /stop to pause early."),
        "/bible autoscroll",
    )


def _process_bible_autoscroll_request(sender_key: Optional[str], sender_node: Optional[str], interface_ref) -> None:
    if not sender_key:
        return
    with BIBLE_AUTOSCROLL_LOCK:
        request = BIBLE_AUTOSCROLL_PENDING.pop(sender_key, None)
        state = BIBLE_AUTOSCROLL_STATE.get(sender_key)
    if not request or not state:
        return
    stop_event = request.get('stop_event') or state.get('stop_event')
    with BIBLE_NAV_LOCK:
        nav_info = dict(PENDING_BIBLE_NAV.get(sender_key) or {})
    if not nav_info or not interface_ref or not sender_node:
        with BIBLE_AUTOSCROLL_LOCK:
            BIBLE_AUTOSCROLL_STATE.pop(sender_key, None)
        return

    def worker():
        try:
            _run_bible_autoscroll(sender_key, sender_node, interface_ref, nav_info, stop_event, request.get('channel_idx'))
        finally:
            with BIBLE_AUTOSCROLL_LOCK:
                BIBLE_AUTOSCROLL_STATE.pop(sender_key, None)

    thread = threading.Thread(target=worker, name=f"BibleAutoScroll-{sender_key}", daemon=True)
    with BIBLE_AUTOSCROLL_LOCK:
        state['thread'] = thread
    thread.start()


def _run_bible_autoscroll(sender_key: str, sender_node: str, interface_ref, nav_info: Dict[str, Any], stop_event: Optional[threading.Event], channel_idx: Optional[int]) -> None:
    lang = nav_info.get('language') or LANGUAGE_FALLBACK
    local_state = dict(nav_info)
    total_sent = 0
    time.sleep(1)
    for idx in range(BIBLE_AUTOSCROLL_MAX_CHUNKS):
        if stop_event and stop_event.is_set():
            break
        next_state = _shift_bible_position(local_state, True)
        if not next_state:
            break
        book, chapter, start, end, lang_used, include_header = next_state
        rendered = _render_bible_passage(book, chapter, start, end, lang_used, include_header=include_header)
        if not rendered:
            break
        text, info = rendered
        info['span'] = local_state.get('span', end - start)
        has_more = _shift_bible_position(info, True) is not None
        include_hint = (not has_more) or (idx == BIBLE_AUTOSCROLL_MAX_CHUNKS - 1)
        message = _format_bible_nav_message(text, include_hint=include_hint)
        try:
            send_direct_chunks(interface_ref, message, sender_node, chunk_delay=None)
        except Exception as exc:
            clean_log(f"Bible auto-scroll failed: {exc}", "⚠️", show_always=True, rate_limit=False)
            break
        local_state.update(info)
        try:
            _update_bible_progress(sender_key, info['book'], info['chapter'], info['verse_start'], info['language'], info.get('span', end - start))
        except Exception:
            pass
        with BIBLE_NAV_LOCK:
            PENDING_BIBLE_NAV[sender_key] = dict(local_state)
        total_sent += 1
        if stop_event and stop_event.wait(BIBLE_AUTOSCROLL_INTERVAL):
            break
    finished = not (stop_event and stop_event.is_set())
    if finished:
        try:
            send_direct_chunks(interface_ref, translate(lang, 'bible_autoscroll_finished', "📖 Auto-scroll paused after the set of verses. Reply 22 to continue."), sender_node, chunk_delay=None)
        except Exception:
            pass
    else:
        try:
            send_direct_chunks(interface_ref, translate(lang, 'bible_autoscroll_stop', "⏹️ Auto-scroll paused. Reply 22 later to resume."), sender_node, chunk_delay=None)
        except Exception:
            pass

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
LOCATION_HISTORY_RETENTION = 10  # seconds


SAVED_CONTEXT_FILE = Path(config.get("saved_context_file", "data/saved_contexts.json"))
SAVED_CONTEXT_LOCK = threading.Lock()
SAVED_CONTEXTS: Dict[str, List[Dict[str, Any]]] = {}
SAVED_CONTEXT_MAX_CHARS = max(2000, int(config.get("saved_context_max_chars", 12000)))
SAVED_CONTEXT_SUMMARY_CHARS = max(120, int(config.get("saved_context_summary_chars", 280)))
SAVED_CONTEXT_SUMMARY_LINES = max(1, int(config.get("saved_context_summary_lines", 4)))
SAVED_CONTEXT_LIST_LIMIT = max(3, int(config.get("saved_context_list_limit", 20)))
SAVED_CONTEXT_MAX_PER_USER = max(5, int(config.get("saved_context_max_per_user", 20)))
SAVED_CONTEXT_TITLE_MAX = max(10, int(config.get("saved_context_title_max", 60)))
CONTEXT_SESSION_TIMEOUT_SECONDS = max(300, int(config.get("context_session_timeout_seconds", 7200)))
CONTEXT_SESSION_LOCK = threading.Lock()
CONTEXT_SESSIONS: Dict[str, Dict[str, Any]] = {}
PENDING_RECALL_SELECTIONS: Dict[str, Dict[str, Any]] = {}
PENDING_DELETE_SELECTIONS: Dict[str, Dict[str, Any]] = {}


WEATHER_LOCATION_NAME = str(config.get("weather_location_name", "El Paso, TX"))
try:
    WEATHER_LAT = float(config.get("weather_lat", 31.7619))
except (TypeError, ValueError):
    WEATHER_LAT = 31.7619
try:
    WEATHER_LON = float(config.get("weather_lon", -106.4850))
except (TypeError, ValueError):
    WEATHER_LON = -106.4850
try:
    WEATHER_CACHE_TTL = int(config.get("weather_cache_ttl", 1800))
except (TypeError, ValueError):
    WEATHER_CACHE_TTL = 1800
WEATHER_CACHE_LOCK = threading.Lock()
WEATHER_CACHE: Dict[str, Any] = {"timestamp": 0.0, "text": None}

WEATHER_CODE_DESCRIPTIONS = {
    0: "Clear sky",
    1: "Mainly clear",
    2: "Partly cloudy",
    3: "Overcast",
    45: "Fog",
    48: "Depositing rime fog",
    51: "Light drizzle",
    53: "Moderate drizzle",
    55: "Dense drizzle",
    56: "Freezing drizzle",
    57: "Freezing drizzle",
    61: "Light rain",
    63: "Moderate rain",
    65: "Heavy rain",
    66: "Freezing rain",
    67: "Freezing rain",
    71: "Light snow",
    73: "Moderate snow",
    75: "Heavy snow",
    77: "Snow grains",
    80: "Light rain showers",
    81: "Moderate rain showers",
    82: "Intense rain showers",
    85: "Light snow showers",
    86: "Heavy snow showers",
    95: "Thunderstorm",
    96: "Thunderstorm with hail",
    99: "Thunderstorm with hail",
}


try:
    WEB_SEARCH_MAX_RESULTS = max(1, int(config.get("web_search_max_results", 3)))
except (TypeError, ValueError):
    WEB_SEARCH_MAX_RESULTS = 3

try:
    WEB_SEARCH_TIMEOUT = max(3, int(config.get("web_search_timeout", 10)))
except (TypeError, ValueError):
    WEB_SEARCH_TIMEOUT = 10

try:
    WEB_SEARCH_CONTEXT_MAX = max(1, int(config.get("web_search_context_max", 3)))
except (TypeError, ValueError):
    WEB_SEARCH_CONTEXT_MAX = 3

WEB_SEARCH_USER_AGENT = str(config.get(
    "web_search_user_agent",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
))

try:
    WEB_CRAWL_MAX_PAGES = max(1, int(config.get("web_crawl_max_pages", 100)))
except (TypeError, ValueError):
    WEB_CRAWL_MAX_PAGES = 100

try:
    WEB_CRAWL_MAX_LIMIT = max(WEB_CRAWL_MAX_PAGES, int(config.get("web_crawl_max_limit", 150)))
except (TypeError, ValueError):
    WEB_CRAWL_MAX_LIMIT = max(WEB_CRAWL_MAX_PAGES, 150)

WEB_CRAWL_SUPPRESS_LOG_STATUS = {405}

WEB_CONTEXT_LOCK = threading.Lock()
WEB_SEARCH_CONTEXT: Dict[str, deque[str]] = {}

CONTEXT_TRUNCATION_LOCK = threading.Lock()
CONTEXT_TRUNCATED_SENDERS: Set[str] = set()
CONTEXT_TRUNCATION_NOTICES: Dict[str, float] = {}
CONTEXT_TRUNCATION_COOLDOWN = 600  # seconds between user notices
CONTEXT_TRUNCATION_NOTICE = (
    "⚠️ Heads-up: I trimmed our chat history to stay within the model limit. "
    "Use `/reset` if you'd like a fresh start."
)

DIRECT_URL_RE = re.compile(
    r"^(?:https?://)?"  # optional scheme
    r"(?:www\.)?"      # optional www
    r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"  # first label
    r"(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+"  # one or more dot-separated labels
    r"(?:[/#?].*)?"  # optional path/query/fragment
    r"$",
    re.IGNORECASE,
)



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


def _store_web_context(sender_key: Optional[str], formatted: str, *, context: Optional[str] = None) -> None:
    if not sender_key or not formatted:
        return
    payload = context if context else formatted
    if context:
        payload = f"{formatted}\n\nDetails:\n{context}"
    with WEB_CONTEXT_LOCK:
        dq = WEB_SEARCH_CONTEXT.get(sender_key)
        if dq is None or dq.maxlen != WEB_SEARCH_CONTEXT_MAX:
            dq = deque(maxlen=WEB_SEARCH_CONTEXT_MAX)
            WEB_SEARCH_CONTEXT[sender_key] = dq
        dq.append(payload)


def _get_web_context(sender_key: Optional[str]) -> List[str]:
    if not sender_key:
        return []
    with WEB_CONTEXT_LOCK:
        dq = WEB_SEARCH_CONTEXT.get(sender_key)
        if not dq:
            return []
        return list(dq)


def _clear_web_context(sender_key: Optional[str]) -> None:
    if not sender_key:
        return
    with WEB_CONTEXT_LOCK:
        WEB_SEARCH_CONTEXT.pop(sender_key, None)


_DDG_HTML_TITLE_RE = re.compile(r'<a rel="nofollow" class="result__a" href="(.*?)">(.*?)</a>', re.S)
_DDG_HTML_SNIPPET_RE = re.compile(r'<a class="result__snippet"[^>]*>(.*?)</a>', re.S)


def _strip_html_tags(raw: str) -> str:
    if not raw:
        return ""
    cleaned = re.sub(r"<script[^>]*?>.*?</script>", "", raw, flags=re.IGNORECASE | re.DOTALL)
    cleaned = re.sub(r"<style[^>]*?>.*?</style>", "", cleaned, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<[^>]+>", "", cleaned)
    return html.unescape(text).strip()


def _extract_direct_url(query: str) -> Optional[str]:
    if not query:
        return None
    cleaned = query.strip().strip('"')
    if not cleaned:
        return None
    tokens = cleaned.split()
    candidate = cleaned if len(tokens) == 1 else tokens[0]
    candidate = candidate.strip()
    if candidate.startswith("/") and not candidate.startswith("//"):
        # handle accidental leading slash like "/www.example.com"
        candidate = candidate.lstrip("/")
    if not DIRECT_URL_RE.match(candidate):
        return None
    if not candidate.startswith(("http://", "https://")):
        candidate = f"https://{candidate}"
    return candidate


def _fetch_url_preview(url: str, *, timeout: Optional[int] = None) -> List[Dict[str, str]]:
    headers = {
        "User-Agent": WEB_SEARCH_USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    try:
        response = requests.get(url, timeout=timeout or WEB_SEARCH_TIMEOUT, headers=headers, allow_redirects=True)
        response.raise_for_status()
    except requests.exceptions.RequestException as exc:
        raise RuntimeError("offline") from exc
    except Exception as exc:
        raise RuntimeError(f"web fetch failed: {exc}") from exc

    final_url = response.url or url
    content_type = response.headers.get("Content-Type", "").lower()
    text = ""
    if "text" in content_type or content_type == "":
        text = response.text
    elif "json" in content_type:
        try:
            text = json.dumps(response.json(), indent=2)
        except Exception:
            text = response.text
    else:
        return [{
            "title": final_url,
            "url": final_url,
            "snippet": f"Content type {content_type} fetched successfully.",
        }]

    title_match = re.search(r"<title>(.*?)</title>", text, re.IGNORECASE | re.DOTALL)
    title = _strip_html_tags(title_match.group(1)) if title_match else final_url

    body_text = _strip_html_tags(text)
    snippet = " ".join(body_text.split())
    if len(snippet) > 360:
        snippet = snippet[:357].rstrip() + "…"
    if not snippet:
        snippet = "(No preview text available.)"

    return [{
        "title": title or final_url,
        "url": final_url,
        "snippet": snippet,
    }]


def _normalize_ddg_url(raw_url: str) -> str:
    if not raw_url:
        return ""
    try:
        parsed = urllib.parse.urlparse(raw_url)
    except Exception:
        return raw_url

    host = (parsed.netloc or "").lower()
    path = parsed.path or ""

    if host.endswith("duckduckgo.com"):
        if path.startswith("/l/"):
            try:
                qs = urllib.parse.parse_qs(parsed.query)
                uddg = qs.get("uddg")
                if uddg:
                    return urllib.parse.unquote(uddg[0])
            except Exception:
                pass
        # Sponsored / tracking results we should ignore
        if path.startswith("/y.js") or path.startswith("/lite/duckduckgo.js"):
            return ""
        if not path.startswith("/l/"):
            return ""
    return raw_url


def _duckduckgo_html_fallback(query: str, max_results: int) -> List[Dict[str, str]]:
    """Scrape the DuckDuckGo lite HTML endpoint as a fallback."""
    try:
        response = requests.post(
            "https://html.duckduckgo.com/html/",
            data={"q": query},
            headers={
                "User-Agent": WEB_SEARCH_USER_AGENT,
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=WEB_SEARCH_TIMEOUT,
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as exc:
        raise RuntimeError("offline") from exc
    except Exception as exc:
        raise RuntimeError(f"web search failed: {exc}") from exc

    text = response.text
    titles = _DDG_HTML_TITLE_RE.findall(text)
    snippets = _DDG_HTML_SNIPPET_RE.findall(text)
    results: List[Dict[str, str]] = []

    for idx, (url, title_html) in enumerate(titles):
        title_clean = _strip_html_tags(title_html)
        if not title_clean:
            continue
        normalized_url = _normalize_ddg_url(html.unescape(url.strip()))
        if not normalized_url:
            continue
        snippet_html = snippets[idx] if idx < len(snippets) else ""
        snippet_clean = _strip_html_tags(snippet_html)
        if len(snippet_clean) > 220:
            snippet_clean = snippet_clean[:217].rstrip() + "…"
        results.append({
            "title": title_clean,
            "url": normalized_url,
            "snippet": snippet_clean,
        })
        if len(results) >= max_results:
            break
    return results


def _web_search_duckduckgo(query: str, max_results: int = WEB_SEARCH_MAX_RESULTS) -> List[Dict[str, str]]:
    params = {
        "q": query,
        "format": "json",
        "no_redirect": 1,
        "no_html": 1,
        "skip_disambig": 1,
    }
    api_exception: Optional[Exception] = None
    results: List[Dict[str, str]] = []

    try:
        response = requests.get(
            "https://api.duckduckgo.com/",
            params=params,
            timeout=WEB_SEARCH_TIMEOUT,
        )
        response.raise_for_status()
        data = response.json()

        def add_result(title: Optional[str], url: Optional[str], snippet: Optional[str]) -> None:
            if not url or not title:
                return
            title_clean = title.strip()
            if not title_clean:
                return
            snippet_clean = (snippet or "").strip()
            for existing in results:
                if existing.get("url") == url:
                    return
            results.append({
                "title": title_clean,
                "url": url.strip(),
                "snippet": snippet_clean,
            })

        add_result(data.get("Heading") or data.get("AbstractText"), data.get("AbstractURL"), data.get("AbstractText"))

        for item in data.get("Results", []) or []:
            add_result(item.get("Text"), item.get("FirstURL"), item.get("Text"))

        def extract_topics(topics) -> None:
            for topic in topics or []:
                if not isinstance(topic, dict):
                    continue
                if topic.get("Topics"):
                    extract_topics(topic.get("Topics"))
                    continue
                add_result(topic.get("Text"), topic.get("FirstURL"), topic.get("Text"))

        extract_topics(data.get("RelatedTopics"))
    except requests.exceptions.RequestException as exc:
        api_exception = exc
    except Exception as exc:
        raise RuntimeError(f"web search failed: {exc}") from exc

    if results:
        return results[:max_results]

    try:
        fallback_results = _duckduckgo_html_fallback(query, max_results)
    except RuntimeError as fallback_exc:
        if str(fallback_exc) == "offline":
            root_exc = api_exception if api_exception is not None else fallback_exc
            raise RuntimeError("offline") from root_exc
        raise

    if fallback_results:
        return fallback_results[:max_results]

    if api_exception is not None:
        raise RuntimeError("offline") from api_exception

    return []


def _format_web_results(query: str, results: List[Dict[str, str]]) -> str:
    if not results:
        return f"🔍 No web results found for “{query}”."

    lines = [f"🔍 Web results for “{query}”:" ]
    for idx, item in enumerate(results, 1):
        title = item.get("title") or "Untitled"
        url = item.get("url") or ""
        snippet = (item.get("snippet") or "").replace("\n", " ").strip()
        if len(snippet) > 200:
            snippet = snippet[:197].rstrip() + "…"
        host = ""
        if url:
            try:
                parsed = urllib.parse.urlparse(url)
                host = parsed.netloc
            except Exception:
                host = ""
        heading = f"{idx}. {title}"
        if host:
            heading += f" [{host}]"
        lines.append(heading)
        if snippet:
            lines.append(f"   {snippet}")
    return "\n".join(lines)


def _context_from_results(query: str, results: List[Dict[str, str]]) -> str:
    if not results:
        return f"No additional context for {query}."
    lines = [f"Source: {query}"]
    for idx, item in enumerate(results, 1):
        title = item.get("title") or "Untitled"
        url = item.get("url") or ""
        snippet = (item.get("snippet") or "").strip()
        lines.append(f"[{idx}] {title}")
        if url:
            lines.append(f"URL: {url}")
        if snippet:
            detail = snippet if len(snippet) <= 800 else snippet[:797].rstrip() + "…"
            lines.append(detail)
    return "\n".join(lines)


def _crawl_website(start_url: str, max_pages: int = WEB_CRAWL_MAX_PAGES) -> Tuple[List[Dict[str, str]], List[Dict[str, str]], Optional[Dict[str, str]]]:
    headers = {
        "User-Agent": WEB_SEARCH_USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    normalized_start = _extract_direct_url(start_url) or start_url
    parsed_start = urllib.parse.urlparse(normalized_start)
    base_host = parsed_start.netloc.lower()
    if not base_host:
        raise RuntimeError("crawl requires a valid domain")

    max_pages = max(1, min(max_pages, WEB_CRAWL_MAX_LIMIT))
    queue_urls: deque[str] = deque([normalized_start])
    seen: Set[str] = set()
    pages: List[Dict[str, str]] = []
    email_pattern = re.compile(r'[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}', re.IGNORECASE)
    phone_pattern = re.compile(r'(?:\+\d{1,3}\s*)?(?:\(\d{2,4}\)|\d{2,4})[\s.-]?\d{3}[\s.-]?\d{4}', re.IGNORECASE)
    contact_records: List[Dict[str, str]] = []
    seen_contacts: Set[Tuple[str, str]] = set()
    contact_page_info: Optional[Dict[str, str]] = None
    fatal_issue: Optional[Tuple[str, Optional[int], str]] = None

    def _within_scope(netloc: str) -> bool:
        netloc = netloc.lower()
        return netloc == base_host or netloc.endswith('.' + base_host)

    while queue_urls and len(pages) < max_pages:
        current = queue_urls.popleft()
        if current in seen:
            continue
        seen.add(current)
        try:
            response = requests.get(current, headers=headers, timeout=WEB_SEARCH_TIMEOUT, allow_redirects=True)
            response.raise_for_status()
        except requests.exceptions.RequestException as exc:
            status_code = None
            reason_text = ""
            if isinstance(exc, requests.exceptions.HTTPError) and getattr(exc, "response", None) is not None:
                status_code = exc.response.status_code
                reason_text = exc.response.reason or ""
            if fatal_issue is None and not pages:
                if status_code is not None:
                    detail_text = f"HTTP {status_code}{f' {reason_text}' if reason_text else ''}"
                else:
                    detail_text = str(exc) or exc.__class__.__name__
                fatal_issue = (current, status_code, detail_text)
            if status_code in WEB_CRAWL_SUPPRESS_LOG_STATUS:
                dprint(f"Crawl suppressed {status_code} for {current}: {exc}")
            else:
                clean_log(f"Crawl skipped {current}: {exc}", "⚠️", show_always=False)
            continue
        except Exception as exc:
            clean_log(f"Crawl error fetching {current}: {exc}", "⚠️", show_always=False)
            continue

        final_url = response.url or current
        parsed_final = urllib.parse.urlparse(final_url)
        if not _within_scope(parsed_final.netloc):
            continue

        html_text = response.text
        title_match = re.search(r"<title>(.*?)</title>", html_text, re.IGNORECASE | re.DOTALL)
        title = _strip_html_tags(title_match.group(1)) if title_match else final_url
        body_text = _strip_html_tags(html_text)
        snippet = " ".join(body_text.split())
        if len(snippet) > 360:
            snippet = snippet[:357].rstrip() + "…"
        url_lower = final_url.lower()
        title_lower = title.lower()
        is_contact_page = any(keyword in url_lower for keyword in ("/contact", "contact-", "contact?", "contacto")) or "contact" in title_lower or "contacto" in title_lower
        if is_contact_page and not contact_page_info:
            contact_page_info = {
                "url": final_url,
                "title": title,
                "snippet": snippet,
            }
        emails_found = email_pattern.findall(body_text)
        phones_found = phone_pattern.findall(body_text)
        for email in emails_found:
            contact_key = ("email", email.lower())
            if contact_key in seen_contacts:
                continue
            seen_contacts.add(contact_key)
            contact_records.append({
                "type": "Email",
                "value": email.strip(),
                "source": final_url,
                "title": title,
            })
        for phone in phones_found:
            normalized_phone = re.sub(r"\s+", " ", phone.strip())
            contact_key = ("phone", normalized_phone)
            if contact_key in seen_contacts:
                continue
            seen_contacts.add(contact_key)
            contact_records.append({
                "type": "Phone",
                "value": normalized_phone,
                "source": final_url,
                "title": title,
            })

        pages.append({
            "url": final_url,
            "title": title,
            "snippet": snippet,
            "is_contact": is_contact_page,
        })

        if len(pages) >= max_pages:
            break

        link_candidates = re.findall(r'href=["\'](.*?)["\']', html_text, re.IGNORECASE)
        for href in link_candidates:
            href = html.unescape(href.strip())
            if not href or href.startswith('#'):
                continue
            if href.lower().startswith('mailto:'):
                email = href.split(':', 1)[1].strip()
                if email:
                    contact_key = ("email", email.lower())
                    if contact_key not in seen_contacts:
                        seen_contacts.add(contact_key)
                        contact_records.append({
                            "type": "Email",
                            "value": email,
                            "source": final_url,
                            "title": title,
                        })
                continue
            if href.lower().startswith('javascript:'):
                continue
            joined = urllib.parse.urljoin(final_url, href)
            parsed_joined = urllib.parse.urlparse(joined)
            if parsed_joined.scheme not in {"http", "https"}:
                continue
            if not _within_scope(parsed_joined.netloc):
                continue
            if joined not in seen and joined not in queue_urls:
                if 'contact' in joined.lower() or 'contacto' in joined.lower():
                    queue_urls.appendleft(joined)
                else:
                    queue_urls.append(joined)

    if not pages and fatal_issue is not None:
        failed_url, _failed_status, failed_detail = fatal_issue
        contact_records.append({
            "type": "Error",
            "value": failed_detail,
            "source": failed_url,
            "title": "Crawl start failure",
        })

    return pages, contact_records, contact_page_info


def _format_crawl_results(start_url: str, pages: List[Dict[str, str]], contacts: List[Dict[str, str]], contact_page: Optional[Dict[str, str]]) -> str:
    if not pages:
        error_entry = next((c for c in contacts if (c.get("type") or "").lower() == "error"), None)
        if error_entry:
            parsed = urllib.parse.urlparse(start_url)
            host = parsed.netloc or start_url
            detail = (error_entry.get("value") or error_entry.get("title") or "unknown error").strip()
            if not detail:
                detail = "unknown error"
            return f"⚠️ Couldn't crawl {host}: {detail}"
        return f"🕸️ Crawl of {start_url} returned no pages."

    parsed = urllib.parse.urlparse(start_url)
    host = parsed.netloc or start_url
    total_pages = len(pages)

    summary_line = f"🕸️ Crawl {host} — {total_pages} page{'s' if total_pages != 1 else ''} scanned."

    if contact_page:
        contact_parsed = urllib.parse.urlparse(contact_page.get("url", ""))
        contact_path = contact_parsed.path or "/"
        if contact_parsed.query:
            contact_path += f"?{contact_parsed.query}"
        summary_line += f" Contact page: {contact_path}."
    else:
        summary_line += " Contact page not found."

    contact_summary: str
    if contacts:
        contact_page_url = contact_page.get("url") if contact_page else None
        ordered_contacts = sorted(
            contacts,
            key=lambda c: 0 if contact_page_url and c.get("source") == contact_page_url else 1,
        )
        contact_values: List[str] = []
        seen_values: Set[str] = set()
        for contact in ordered_contacts:
            value = (contact.get("value") or "").strip()
            if not value:
                continue
            if value.lower() in seen_values:
                continue
            seen_values.add(value.lower())
            label = contact.get("type") or "Contact"
            contact_values.append(f"{label}: {value}")
            if len("; ".join(contact_values)) > 200:
                break
        if contact_values:
            contact_summary = "📇 Contacts: " + "; ".join(contact_values)
        else:
            contact_summary = "📇 Contacts: (found but filtered)"
    else:
        contact_summary = "📇 Contacts: none detected"

    message = f"{summary_line}\n{contact_summary}".strip()
    if len(message) > 400:
        message = message[:397].rstrip() + "…"
    return message


def _context_from_crawl(start_url: str, pages: List[Dict[str, str]], contacts: List[Dict[str, str]], contact_page: Optional[Dict[str, str]]) -> str:
    if not pages and not contacts:
        return f"No crawl context gathered for {start_url}."
    lines = [f"Crawl context for {start_url}"]
    for idx, page in enumerate(pages, 1):
        title = page.get("title") or "(untitled)"
        url = page.get("url") or ""
        snippet = page.get("snippet") or ""
        lines.append(f"[{idx}] {title}")
        if url:
            lines.append(f"URL: {url}")
        if snippet:
            detail = snippet if len(snippet) <= 1000 else snippet[:997].rstrip() + "…"
            lines.append(detail)
    if contacts:
        lines.append("Contacts:")
        for contact in contacts:
            ctype = contact.get("type") or "Contact"
            value = contact.get("value") or ""
            source = contact.get("source") or ""
            title = contact.get("title") or ""
            line = f"- {ctype}: {value}"
            if title:
                line += f" ({title})"
            if source:
                line += f" <{source}>"
            lines.append(line)
    if contact_page:
        lines.append("Contact page detail:")
        if contact_page.get("url"):
            lines.append(contact_page["url"])
        snippet = contact_page.get("snippet")
        if snippet:
            lines.append(snippet)
    return "\n".join(lines)


try:
    WIKI_MAX_CHARS = max(20000, int(config.get("wiki_max_chars", 160000)))
except (TypeError, ValueError):
    WIKI_MAX_CHARS = 160000

try:
    WIKI_SUMMARY_CHAR_LIMIT = max(200, int(config.get("wiki_summary_char_limit", 400)))
except (TypeError, ValueError):
    WIKI_SUMMARY_CHAR_LIMIT = 400


def _fetch_wikipedia_article(topic: str, max_chars: int = WIKI_MAX_CHARS) -> Dict[str, str]:
    headers = {
        "User-Agent": WEB_SEARCH_USER_AGENT,
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
    }
    params = {
        "action": "query",
        "prop": "extracts|info",
        "explaintext": 1,
        "exsectionformat": "plain",
        "exlimit": 1,
        "exchars": max_chars,
        "redirects": 1,
        "inprop": "url",
        "format": "json",
        "formatversion": 2,
        "titles": topic,
    }
    try:
        response = requests.get(
            "https://en.wikipedia.org/w/api.php",
            params=params,
            headers=headers,
            timeout=max(WEB_SEARCH_TIMEOUT, 15),
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as exc:
        raise RuntimeError("offline") from exc
    except Exception as exc:
        raise RuntimeError(f"wiki failed: {exc}") from exc

    data = response.json()
    pages = (data.get("query", {}).get("pages") or [])
    if not pages:
        raise RuntimeError("wiki_missing")
    page = pages[0]
    if page.get("missing"):
        raise RuntimeError("wiki_missing")

    title = page.get("title") or topic
    extract = (page.get("extract") or "").strip()
    if not extract:
        summary_resp = None
        try:
            summary_resp = requests.get(
                f"https://en.wikipedia.org/api/rest_v1/page/summary/{urllib.parse.quote(title.replace(' ', '_'))}",
                headers=headers,
                timeout=max(WEB_SEARCH_TIMEOUT, 15),
            )
            summary_resp.raise_for_status()
            summary_data = summary_resp.json()
            extract = (summary_data.get("extract") or "").strip()
        except requests.exceptions.RequestException:
            raise RuntimeError("wiki_missing")
        except Exception:
            raise RuntimeError("wiki_missing")

    if not extract:
        raise RuntimeError("wiki_missing")

    if len(extract) > max_chars:
        extract = extract[:max_chars].rstrip()

    summary = extract.split('\n', 1)[0].strip()
    if not summary:
        summary = extract[:WIKI_SUMMARY_CHAR_LIMIT].strip()

    canonical_url = page.get("fullurl") or page.get("canonicalurl")
    if not canonical_url:
        canonical_url = f"https://en.wikipedia.org/wiki/{urllib.parse.quote(title.replace(' ', '_'))}"

    return {
        "title": title,
        "summary": summary,
        "extract": extract,
        "content": extract,
        "url": canonical_url,
        "source": canonical_url,
    }


def _format_wiki_summary(article: Dict[str, str]) -> str:
    title = article.get("title") or "Wikipedia"
    summary = article.get("summary") or "No summary available."
    summary = summary.replace('\n', ' ')
    if len(summary) > WIKI_SUMMARY_CHAR_LIMIT:
        summary = summary[: WIKI_SUMMARY_CHAR_LIMIT - 1].rstrip() + "…"
    return f"📚 Wikipedia: {title} — {summary}"


def _format_wiki_context(article: Dict[str, str]) -> str:
    lines = [
        f"Wikipedia article: {article.get('title')}",
        article.get("url", ""),
        article.get("extract", ""),
    ]
    return "\n".join(line for line in lines if line).strip()


def _fetch_drudge_headlines(max_items: int = 5) -> List[Dict[str, str]]:
    headers = {
        "User-Agent": WEB_SEARCH_USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    try:
        response = requests.get("https://www.drudgereport.com/", headers=headers, timeout=WEB_SEARCH_TIMEOUT)
        response.raise_for_status()
    except requests.exceptions.RequestException as exc:
        raise RuntimeError("offline") from exc
    except Exception as exc:
        raise RuntimeError(f"drudge fetch failed: {exc}") from exc

    html_text = response.text
    # Drudge uses uppercase text frequently; capture headline anchors
    anchor_re = re.compile(r'<a[^>]+href="(.*?)"[^>]*>(.*?)</a>', re.IGNORECASE | re.DOTALL)
    headlines: List[Dict[str, str]] = []
    seen: Set[str] = set()

    for match in anchor_re.finditer(html_text):
        url, label_html = match.groups()
        title = _strip_html_tags(label_html)
        title = " ".join(title.split())
        if not title or len(title) < 4:
            continue
        if title.lower().startswith("advertisement"):
            continue
        norm_url = url.strip()
        if norm_url.lower().startswith("javascript:"):
            continue
        if not norm_url.startswith("http"):
            norm_url = urllib.parse.urljoin("https://www.drudgereport.com/", norm_url)
        key = title.lower()
        if key in seen:
            continue
        seen.add(key)
        headlines.append({"title": title, "url": norm_url})
        if len(headlines) >= max_items:
            break

    return headlines



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
if AI_PROVIDER != "ollama":
    clean_log(
        f"AI provider '{AI_PROVIDER}' overridden to Ollama (dashboard only supports Ollama).",
        "🦙",
        show_always=True,
        rate_limit=False,
    )
AI_PROVIDER = "ollama"
if config.get("ai_provider") != "ollama":
    config["ai_provider"] = "ollama"
SYSTEM_PROMPT = config.get("system_prompt", "")
OLLAMA_URL = config.get("ollama_url", "http://localhost:11434/api/generate")
OLLAMA_MODEL = config.get("ollama_model", "llama3.2:1b")
OLLAMA_TIMEOUT = config.get("ollama_timeout", 120)
OLLAMA_STREAM = bool(config.get("ollama_stream", True))
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

# AI chill mode (overload guard for Ollama intake)
CHILL_MODE_ENABLED = bool(config.get("ai_chill_mode", False))
try:
    CHILL_QUEUE_LIMIT = int(config.get("ai_chill_queue_limit", 5))
except (ValueError, TypeError):
    CHILL_QUEUE_LIMIT = 5
CHILL_QUEUE_LIMIT = max(1, CHILL_QUEUE_LIMIT)
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
    NETWORK_CAPACITY_PER_HOUR = float(config.get("network_capacity_messages_per_hour", 1200))
except (ValueError, TypeError):
    NETWORK_CAPACITY_PER_HOUR = 1200.0
if NETWORK_CAPACITY_PER_HOUR < 0:
    NETWORK_CAPACITY_PER_HOUR = 0.0

try:
    MAILBOX_MAX_MESSAGES = int(config.get("mailbox_max_messages", 10))
except (ValueError, TypeError):
    MAILBOX_MAX_MESSAGES = 10
MAILBOX_MAX_MESSAGES = max(1, MAILBOX_MAX_MESSAGES)

MAIL_SECURITY_FILE = config.get("mail_security_file", "data/mail_security.json")
try:
    MAIL_FOLLOW_UP_DELAY = float(config.get("mail_follow_up_delay", 10.0))
except (ValueError, TypeError):
    MAIL_FOLLOW_UP_DELAY = 10.0
MAIL_FOLLOW_UP_DELAY = max(0.0, MAIL_FOLLOW_UP_DELAY)

MAIL_NOTIFY_ENABLED = bool(config.get("mail_notify_enabled", True))
MAIL_NOTIFY_REMINDERS_ENABLED = bool(config.get("mail_notify_reminders_enabled", True))
try:
    MAIL_NOTIFY_REMINDER_HOURS = float(config.get("mail_notify_reminder_hours", 1.0))
except (ValueError, TypeError):
    MAIL_NOTIFY_REMINDER_HOURS = 1.0
MAIL_NOTIFY_REMINDER_HOURS = max(0.1, MAIL_NOTIFY_REMINDER_HOURS)
try:
    MAIL_NOTIFY_EXPIRY_HOURS = float(config.get("mail_notify_expiry_hours", 72.0))
except (ValueError, TypeError):
    MAIL_NOTIFY_EXPIRY_HOURS = 72.0
MAIL_NOTIFY_EXPIRY_HOURS = max(MAIL_NOTIFY_REMINDER_HOURS, MAIL_NOTIFY_EXPIRY_HOURS)
try:
    MAIL_NOTIFY_MAX_REMINDERS = int(config.get("mail_notify_max_reminders", 3))
except (ValueError, TypeError):
    MAIL_NOTIFY_MAX_REMINDERS = 3
MAIL_NOTIFY_MAX_REMINDERS = max(0, MAIL_NOTIFY_MAX_REMINDERS)
MAIL_NOTIFY_INCLUDE_SELF = bool(config.get("mail_notify_include_self", False))
MAIL_NOTIFY_HEARTBEAT_ONLY = bool(config.get("mail_notify_heartbeat_only", True))
MAIL_NOTIFY_QUIET_HOURS_ENABLED = bool(config.get("mail_notify_quiet_hours_enabled", True))

try:
    MAIL_QUIET_START_HOUR = int(config.get("mail_quiet_start_hour", config.get("notify_active_end_hour", 20)))
except (ValueError, TypeError):
    MAIL_QUIET_START_HOUR = int(config.get("notify_active_end_hour", 20) or 0)
try:
    MAIL_QUIET_END_HOUR = int(config.get("mail_quiet_end_hour", config.get("notify_active_start_hour", 8)))
except (ValueError, TypeError):
    MAIL_QUIET_END_HOUR = int(config.get("notify_active_start_hour", 8) or 0)
MAIL_QUIET_START_HOUR = MAIL_QUIET_START_HOUR % 24
MAIL_QUIET_END_HOUR = MAIL_QUIET_END_HOUR % 24

MAIL_NOTIFY_REMINDER_SECONDS = MAIL_NOTIFY_REMINDER_HOURS * 3600.0
MAIL_NOTIFY_EXPIRY_SECONDS = MAIL_NOTIFY_EXPIRY_HOURS * 3600.0

try:
    NOTIFY_ACTIVE_START_HOUR = int(config.get("notify_active_start_hour", 9))
except (ValueError, TypeError):
    NOTIFY_ACTIVE_START_HOUR = 9
try:
    NOTIFY_ACTIVE_END_HOUR = int(config.get("notify_active_end_hour", 20))
except (ValueError, TypeError):
    NOTIFY_ACTIVE_END_HOUR = 20
NOTIFY_ACTIVE_START_HOUR = max(0, min(23, NOTIFY_ACTIVE_START_HOUR))
NOTIFY_ACTIVE_END_HOUR = max(0, min(23, NOTIFY_ACTIVE_END_HOUR))


def _within_notification_window(ts: Optional[datetime] = None) -> bool:
    ts = ts or datetime.now()
    start = NOTIFY_ACTIVE_START_HOUR % 24
    end = NOTIFY_ACTIVE_END_HOUR % 24
    hour = ts.hour + ts.minute / 60.0
    if start == end:
        return True  # no quiet period configured
    if start < end:
        return start <= hour < end
    return hour >= start or hour < end

try:
    OLLAMA_NUM_THREAD = int(config.get("ollama_num_thread", 0))
except (ValueError, TypeError):
    OLLAMA_NUM_THREAD = 0

if OLLAMA_NUM_THREAD <= 0:
    cpu_count = os.cpu_count() or 1
    if cpu_count >= 4:
        OLLAMA_NUM_THREAD = max(2, cpu_count // 2)
    else:
        OLLAMA_NUM_THREAD = cpu_count

OLLAMA_LOW_VRAM = bool(config.get("ollama_low_vram", True))

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

# Optional: disable direct ACK waits to avoid stalls on unstable links
DISABLE_ACK_WAIT = bool(config.get("disable_ack_wait", False))
# Ensure ACK waits remain enabled under test harness so resend logic is exercised
if os.environ.get('PYTEST_CURRENT_TEST'):
    DISABLE_ACK_WAIT = False


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

# -----------------------------
# Resend (No Ack) configuration
# -----------------------------
RESEND_ENABLED = bool(config.get("resend_enabled", True))
try:
    RESEND_USAGE_THRESHOLD_PERCENT = float(config.get("resend_usage_threshold_percent", 5.0))
except (TypeError, ValueError):
    RESEND_USAGE_THRESHOLD_PERCENT = 5.0
RESEND_DM_ONLY = bool(config.get("resend_dm_only", True))
RESEND_BROADCAST_ENABLED = bool(config.get("resend_broadcast_enabled", False))
try:
    RESEND_SYSTEM_ATTEMPTS = int(config.get("resend_system_attempts", 3))
except (TypeError, ValueError):
    RESEND_SYSTEM_ATTEMPTS = 3
try:
    RESEND_SYSTEM_INTERVAL = float(config.get("resend_system_interval_seconds", 15))
except (TypeError, ValueError):
    RESEND_SYSTEM_INTERVAL = 15.0
try:
    RESEND_USER_ATTEMPTS = int(config.get("resend_user_attempts", 3))
except (TypeError, ValueError):
    RESEND_USER_ATTEMPTS = 3
try:
    RESEND_USER_INTERVAL = float(config.get("resend_user_interval_seconds", 15))
except (TypeError, ValueError):
    RESEND_USER_INTERVAL = 15.0
try:
    RESEND_JITTER_SECONDS = float(config.get("resend_jitter_seconds", 8.0))
except (TypeError, ValueError):
    RESEND_JITTER_SECONDS = 8.0
RESEND_SUFFIX_ENABLED = bool(config.get("resend_suffix_enabled", True))
RESEND_TELEMETRY_ENABLED = bool(config.get("resend_telemetry_enabled", True))

MAIL_MANAGER = MailManager(
    store_path="mesh_mailboxes.json",
    security_path=MAIL_SECURITY_FILE,
    clean_log=clean_log,
    ai_log=ai_log,
    ollama_url=OLLAMA_URL or None,
    search_model=MAIL_SEARCH_MODEL,
    search_timeout=MAIL_SEARCH_TIMEOUT,
    search_num_ctx=MAIL_SEARCH_NUM_CTX,
    search_max_messages=MAIL_SEARCH_MAX_MESSAGES,
    message_limit=MAILBOX_MAX_MESSAGES,
    follow_up_delay=MAIL_FOLLOW_UP_DELAY,
    notify_enabled=MAIL_NOTIFY_ENABLED,
    reminders_enabled=MAIL_NOTIFY_REMINDERS_ENABLED and MAIL_NOTIFY_MAX_REMINDERS > 0,
    reminder_interval_seconds=MAIL_NOTIFY_REMINDER_SECONDS,
    reminder_expiry_seconds=MAIL_NOTIFY_EXPIRY_SECONDS,
    reminder_max_count=MAIL_NOTIFY_MAX_REMINDERS,
    include_self_notifications=MAIL_NOTIFY_INCLUDE_SELF,
    heartbeat_only=MAIL_NOTIFY_HEARTBEAT_ONLY,
    quiet_hours_enabled=MAIL_NOTIFY_QUIET_HOURS_ENABLED,
    quiet_start_hour=NOTIFY_ACTIVE_START_HOUR,
    quiet_end_hour=NOTIFY_ACTIVE_END_HOUR,
    stats=STATS,
)

GAME_MANAGER = GameManager(
    clean_log=clean_log,
    ai_log=ai_log,
    ollama_url=OLLAMA_URL or None,
    choose_model=MAIL_SEARCH_MODEL,
    choose_timeout=MAIL_SEARCH_TIMEOUT,
    wordladder_model=MAIL_SEARCH_MODEL,
    stats=STATS,
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
        {"id": "evangelist", "name": "Evangelist", "emoji": "🎤", "aliases": [], "description": "", "prompt": "Use gentle, encouraging leadership with a grounded, practical tone. Favor clear, plain language over hype; avoid slogans and limit exclamations. Keep replies concise, specific, and action‑oriented without overselling."},
        {"id": "sassy", "name": "Sassy", "emoji": "💅", "aliases": [], "description": "", "prompt": "Respond with affectionate sass—clever quips, eye-roll energy, but always deliver the helpful answer. Keep replies concise yet complete."},
        {"id": "mechanic", "name": "Mechanic", "emoji": "🛠️", "aliases": ["gangster"], "description": "", "prompt": "Speak with hands-on fixer energy—practical swagger, loyal crew vibes, and grounded guidance. Keep replies concise yet complete."},
        {"id": "hippie", "name": "Hippie", "emoji": "🌈", "aliases": [], "description": "", "prompt": "Share mellow, peace-first guidance with gentle optimism and communal spirit. Keep replies concise yet complete."},
        {"id": "millennial", "name": "Millennial", "emoji": "📱", "aliases": [], "description": "", "prompt": "Answer with upbeat, meme-aware millennial energy—empathetic, tech-savvy, and practical. Keep replies concise yet complete."},
        {"id": "ebonics", "name": "Ebonics", "emoji": "🎶", "aliases": ["aave"], "description": "", "prompt": "Answer in warm, confident African American Vernacular English—laid-back, community-first energy, mixing slang naturally while staying respectful and clear. Keep replies concise yet complete."},
        {"id": "bard", "name": "Shakespeare", "emoji": "🪶", "aliases": ["shakespeare", "the_bard"], "description": "", "prompt": "Speak in the style of William Shakespeare—lyrical phrasing, rich metaphor, and gentle humor. Keep replies concise yet complete."},
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
USER_LANGUAGE_FILE = config.get("user_language_file", "data/user_languages.json")
USER_AI_SETTINGS_LOCK = threading.Lock()
USER_AI_SETTINGS: Dict[str, Dict[str, str]] = {}


def _ensure_json_file(path: str, default_payload: Dict[str, Any]) -> None:
    if os.path.exists(path):
        return
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    write_atomic(path, json.dumps(default_payload, indent=2, sort_keys=True))


_ensure_json_file(USER_LANGUAGE_FILE, {})


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
    clear_override = False
    if sender_key:
        with USER_AI_SETTINGS_LOCK:
            entry = USER_AI_SETTINGS.get(sender_key)
            if isinstance(entry, dict):
                stored_persona = _canonical_personality_id(entry.get("personality_id"))
                if stored_persona:
                    persona_id = stored_persona
                if entry.get("prompt_override"):
                    clear_override = True
    if clear_override and sender_key:
        _set_user_prompt_override(sender_key, None)
    return {
        "personality_id": persona_id,
        "prompt_override": None,
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

# -----------------------------
# User access (mute/block) storage
# -----------------------------
USER_ACCESS_FILE = config.get("user_access_file", "data/user_access.json")
USER_ACCESS_LOCK = threading.Lock()
USER_ACCESS: Dict[str, Dict[str, bool]] = {"muted": {}, "blocked": {}}

def _load_user_access_from_disk() -> Dict[str, Dict[str, bool]]:
    try:
        data = safe_load_json(USER_ACCESS_FILE, {})
        if not isinstance(data, dict):
            return {"muted": {}, "blocked": {}}
        muted = data.get("muted") if isinstance(data.get("muted"), dict) else {}
        blocked = data.get("blocked") if isinstance(data.get("blocked"), dict) else {}
        # normalize keys to strings
        return {
            "muted": {str(k): bool(v) for k, v in muted.items()},
            "blocked": {str(k): bool(v) for k, v in blocked.items()},
        }
    except Exception:
        return {"muted": {}, "blocked": {}}

def _save_user_access_to_disk(data: Dict[str, Dict[str, bool]]) -> None:
    try:
        directory = os.path.dirname(USER_ACCESS_FILE)
        if directory:
            os.makedirs(directory, exist_ok=True)
        tmp = f"{USER_ACCESS_FILE}.tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, indent=2)
        os.replace(tmp, USER_ACCESS_FILE)
    except Exception:
        pass

def _user_access_init() -> None:
    global USER_ACCESS
    with USER_ACCESS_LOCK:
        USER_ACCESS = _load_user_access_from_disk()

def _is_user_blocked(sender_key: Optional[str]) -> bool:
    if not sender_key:
        return False
    with USER_ACCESS_LOCK:
        return bool(USER_ACCESS.get("blocked", {}).get(str(sender_key)))

def _is_user_muted(sender_key: Optional[str]) -> bool:
    if not sender_key:
        return False
    with USER_ACCESS_LOCK:
        return bool(USER_ACCESS.get("muted", {}).get(str(sender_key)))

def _set_user_muted(sender_key: str, value: bool) -> None:
    if not sender_key:
        return
    with USER_ACCESS_LOCK:
        USER_ACCESS.setdefault("muted", {})[str(sender_key)] = bool(value)
        _save_user_access_to_disk(USER_ACCESS)

def _set_user_blocked(sender_key: str, value: bool) -> None:
    if not sender_key:
        return
    with USER_ACCESS_LOCK:
        USER_ACCESS.setdefault("blocked", {})[str(sender_key)] = bool(value)
        _save_user_access_to_disk(USER_ACCESS)

_user_access_init()


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
    # Only use the configured system prompt (plus optional persona and web context).
    segments = [base]
    persona_id: Optional[str] = None
    web_context: List[str] = []
    if sender_id is not None:
        sender_key = _safe_sender_key(sender_id)
        prefs = _get_user_ai_preferences(sender_key)
        persona_id = prefs.get("personality_id")
        web_context = _get_web_context(sender_key)
    else:
        persona_id = _default_personality_id()
    if persona_id and persona_id in AI_PERSONALITY_MAP:
        persona_prompt = _sanitize_prompt_text(AI_PERSONALITY_MAP[persona_id].get("prompt"))
        if persona_prompt:
            segments.append(persona_prompt)
    if web_context:
        segments.append("Recent web findings:\n" + "\n".join(web_context))
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
    persona = AI_PERSONALITY_MAP.get(persona_id) if persona_id else None
    emoji = persona.get("emoji") if persona else "🧠"
    name = persona.get("name") if persona else "Default"
    description = persona.get("description") if persona else ""
    lines = [f"{emoji} Current personality: {name}" if persona_id else "🧠 Using default personality."]
    if description and persona_id:
        lines.append(description)
    lines.append("Commands: /vibe set <name> | /vibe status | /vibe reset")
    lines.append("Send /vibe in a DM to pick a new vibe.")
    return "\n".join(lines)

def _start_vibe_menu(sender_key: Optional[str]) -> PendingReply:
    if not sender_key:
        return PendingReply("⚠️ I couldn't identify your DM session. Try again in a moment.", "/vibe menu")

    prefs = _get_user_ai_preferences(sender_key)
    current = prefs.get("personality_id")
    persona_current = AI_PERSONALITY_MAP.get(current, {}) if current else None
    current_name = persona_current.get("name") if persona_current else "Default"
    current_emoji = persona_current.get("emoji") if persona_current else "🧠"
    lines = [f"{current_emoji} Current vibe: {current_name}", ""]
    lines.append("🎛️ Pick a vibe:")

    mapping: List[str] = []
    for idx, persona_id in enumerate(AI_PERSONALITY_ORDER, 1):
        persona = AI_PERSONALITY_MAP.get(persona_id, {})
        name = persona.get("name") or persona_id
        emoji = persona.get("emoji") or "🧠"
        description = persona.get("description") or ""
        marker = " (current)" if persona_id == current else ""
        summary = textwrap.shorten(description, width=70, placeholder="…") if description else ""
        label = f"{idx}. {emoji} {name}{marker}"
        if summary:
            label = f"{label} — {summary}"
        lines.append(label)
        mapping.append(persona_id)

    lines.append("Reply with the number to switch vibes, or X to cancel.")
    lines.append("Extras: /vibe status, /vibe set <name>, /vibe reset.")

    PENDING_VIBE_SELECTIONS[sender_key] = {
        'ids': mapping,
        'created': _now(),
    }
    return PendingReply("\n".join(lines), "/vibe menu")


def _handle_pending_vibe_selection(sender_key: str, text: str) -> Optional[PendingReply]:
    pending = PENDING_VIBE_SELECTIONS.get(sender_key)
    if not pending:
        return None
    if (_now() - pending.get('created', 0.0)) > VIBE_MENU_TIMEOUT:
        PENDING_VIBE_SELECTIONS.pop(sender_key, None)
        return PendingReply("⏱️ Vibe menu expired. Send `/vibe` to open it again.", "/vibe menu")

    cleaned = text.strip()
    if not cleaned:
        return None

    lower = cleaned.lower()
    if lower in {'x', 'cancel', 'exit'}:
        PENDING_VIBE_SELECTIONS.pop(sender_key, None)
        return PendingReply("👍 No vibe changes made.", "/vibe menu")

    if not cleaned.isdigit():
        PENDING_VIBE_SELECTIONS.pop(sender_key, None)
        return None

    choice = int(cleaned)
    ids = pending.get('ids', [])
    if choice < 1 or choice > len(ids):
        PENDING_VIBE_SELECTIONS.pop(sender_key, None)
        return PendingReply("That number isn't available. Send `/vibe` again to pick from the list.", "/vibe menu")

    persona_id = ids[choice - 1]
    if not _set_user_personality(sender_key, persona_id):
        PENDING_VIBE_SELECTIONS.pop(sender_key, None)
        return PendingReply("⚠️ Couldn't apply that vibe. Try again in a moment.", "/vibe menu")

    PENDING_VIBE_SELECTIONS.pop(sender_key, None)
    persona = AI_PERSONALITY_MAP.get(persona_id, {})
    emoji = persona.get("emoji") or "🧠"
    name = persona.get("name") or persona_id
    description = persona.get("description") or ""
    lines = [f"{emoji} Vibe set to {name} ({persona_id})."]
    if description:
        lines.append(description)
    return PendingReply("\n".join(lines), "/vibe menu")


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


@dataclass
class StreamingResult:
    """Represents an Ollama response that was streamed out as it was generated."""

    text: str
    sent_chunks: int = 0
    truncated: bool = False

    @property
    def already_sent(self) -> bool:
        return self.sent_chunks > 0


def _normalize_ai_response(resp) -> Tuple[Optional[str], Optional[PendingReply], bool]:
    """Return response text, pending reply object, and whether it was already sent via streaming."""

    if resp is None:
        return None, None, False
    if isinstance(resp, PendingReply):
        text = resp.text if resp.text is not None else ""
        return text, resp, False
    if isinstance(resp, StreamingResult):
        return resp.text or "", None, resp.already_sent
    if isinstance(resp, str):
        return resp, None, False
    # Fallback string conversion for unexpected types
    return str(resp), None, False
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


def _resolve_book_chapters(book: str, preferred_language: str = 'en') -> Tuple[Optional[str], List[List[str]]]:
    books, lang = _get_books_and_language(book, preferred_language)
    if not books or book not in books:
        books, lang = _get_books_and_language(book, 'en')
        if not books or book not in books:
            return None, []
    chapters = books.get(book) or []
    return (lang or preferred_language), chapters


def _format_bible_nav_message(text: str, *, include_hint: bool = True) -> str:
    """Append navigation hints when space allows."""
    if not text or not include_hint:
        return text

    hint = "<1,2> 22>>"
    suffix_newline = f"\n{hint}"
    if len(text) + len(suffix_newline) <= MAX_CHUNK_SIZE:
        return f"{text}{suffix_newline}"

    suffix_inline = f" {hint}"
    if len(text) + len(suffix_inline) <= MAX_CHUNK_SIZE:
        return f"{text}{suffix_inline}"

    return text


def _render_bible_passage(
    book: str,
    chapter: int,
    verse_start: int,
    verse_end: int,
    preferred_language: str,
    *,
    include_header: bool = True,
) -> Optional[Tuple[str, Dict[str, Any]]]:
    lang_used, chapters = _resolve_book_chapters(book, preferred_language)
    if not chapters or chapter < 1 or chapter > len(chapters):
        return None

    verses = chapters[chapter - 1] or []
    if not verses:
        return None

    total_verses = len(verses)
    verse_start = max(1, min(verse_start, total_verses))
    verse_end = max(verse_start, min(verse_end, total_verses))

    selected_lines: List[str] = []
    if include_header:
        header = f"{_display_book_name(book, lang_used)} {chapter}:{verse_start}" if verse_start == verse_end else f"{_display_book_name(book, lang_used)} {chapter}:{verse_start}-{verse_end}"
        selected_lines.append(header)

    for idx in range(verse_start - 1, verse_end):
        verse_text = verses[idx].strip()
        selected_lines.append(f"{idx + 1}. {verse_text}")

    text = "\n".join(selected_lines)
    info = {
        "book": book,
        "chapter": chapter,
        "verse_start": verse_start,
        "verse_end": verse_end,
        "language": lang_used,
    }
    return text, info


def _random_bible_verse(preferred_language: str) -> Optional[Dict[str, Any]]:
    order = _get_book_order()
    if not order:
        return None
    book = random.choice(order)
    lang_used, chapters = _resolve_book_chapters(book, preferred_language)
    if not chapters:
        return None
    chapter_idx = random.randrange(len(chapters))
    verses = chapters[chapter_idx] or []
    if not verses:
        return None
    verse_idx = random.randrange(len(verses))
    text, info = _render_bible_passage(
        book,
        chapter_idx + 1,
        verse_idx + 1,
        verse_idx + 1,
        lang_used,
        include_header=True,
    ) or (None, None)
    if not text or not info:
        return None
    info['span'] = 0
    return {'text': text, **info}


def _shift_bible_position(state: Dict[str, Any], forward: bool) -> Optional[Tuple[str, int, int, int, str, bool]]:
    book = state.get('book')
    if not book:
        return None
    chapter = max(1, int(state.get('chapter', 1)))
    verse_start = max(1, int(state.get('verse_start', 1)))
    verse_end = max(verse_start, int(state.get('verse_end', verse_start)))
    language = state.get('language') or LANGUAGE_FALLBACK
    span = int(state.get('span', verse_end - verse_start))
    chunk = span if span > 0 else (verse_end - verse_start + 1)
    if chunk <= 0:
        chunk = 1

    lang_used, chapters = _resolve_book_chapters(book, language)
    if not chapters:
        return None

    if forward:
        chapter_idx = chapter - 1
        start = verse_end + 1
        while True:
            if chapter_idx >= len(chapters):
                next_book = _next_book(book)
                if not next_book:
                    return None
                book = next_book
                lang_used, chapters = _resolve_book_chapters(book, lang_used or language)
                if not chapters:
                    return None
                chapter_idx = 0
                start = 1
            verses = chapters[chapter_idx] or []
            chapter_len = len(verses)
            if chapter_len == 0:
                chapter_idx += 1
                start = 1
                continue
            if start <= chapter_len:
                break
            start -= chapter_len
            chapter_idx += 1
        end = min(start + chunk - 1, len(chapters[chapter_idx]))
        include_header = (start == 1)
        return (book, chapter_idx + 1, start, end, lang_used or language, include_header)

    # backward navigation
    chapter_idx = chapter - 1
    end = verse_start - 1
    remaining = chunk
    while True:
        if chapter_idx < 0:
            prev_book = _prev_book(book)
            if not prev_book:
                return None
            book = prev_book
            lang_used, chapters = _resolve_book_chapters(book, lang_used or language)
            if not chapters:
                return None
            chapter_idx = len(chapters) - 1
            end = len(chapters[chapter_idx])
        verses = chapters[chapter_idx] or []
        chapter_len = len(verses)
        if chapter_len == 0:
            chapter_idx -= 1
            end = 0
            continue
        if end <= 0 or end > chapter_len:
            end = chapter_len
        take = min(remaining, end)
        start = end - take + 1
        remaining -= take
        if remaining <= 0:
            include_header = (start == 1)
            return (book, chapter_idx + 1, start, end, lang_used or language, include_header)
        chapter_idx -= 1
        end = 0


def _handle_bible_navigation(sender_key: Optional[str], forward: bool, *, is_direct: bool, channel_idx: Optional[int]) -> Optional[PendingReply]:
    if not sender_key:
        return PendingReply("📖 Use /bible first so I know where you are reading.", "/bible navigation")
    with BIBLE_NAV_LOCK:
        state = PENDING_BIBLE_NAV.get(sender_key)
    if not state:
        return PendingReply("📖 Use /bible first so I know where you are reading.", "/bible navigation")

    next_state = _shift_bible_position(state, forward)
    if not next_state:
        message = "📖 That's the end of what I have." if forward else "📖 You're already at the beginning."
        return PendingReply(message, "/bible navigation")

    next_book, next_chapter, next_start, next_end, lang_used, include_header = next_state
    rendered = _render_bible_passage(
        next_book,
        next_chapter,
        next_start,
        next_end,
        lang_used,
        include_header=include_header,
    )
    if not rendered:
        return PendingReply("📖 Couldn't load that passage right now.", "/bible navigation")

    text, info = rendered
    info['book'] = next_book
    info['span'] = state.get('span', next_end - next_start)
    _set_bible_nav(sender_key, info, is_direct=is_direct, channel_idx=channel_idx)
    try:
        _update_bible_progress(sender_key, next_book, next_chapter, next_start, info['language'], info.get('span', next_end - next_start))
    except Exception:
        pass
    return PendingReply(_format_bible_nav_message(text), "/bible navigation")


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


def _random_text_entry(entries: Sequence[Any]) -> Optional[str]:
    if not entries:
        return None
    choice = random.choice(entries)
    if isinstance(choice, dict):
        for key in ("text", "joke", "fact", "quote", "line"):
            value = choice.get(key)
            if value:
                return str(value).strip()
        return str(choice).strip()
    return str(choice).strip() if choice else None


def _random_chuck_fact(language: Optional[str]) -> Optional[str]:
    lang = (language or "").lower()
    pools = []
    if lang.startswith('es') and CHUCK_NORRIS_FACTS_ES:
        pools.append(CHUCK_NORRIS_FACTS_ES)
    pools.append(CHUCK_NORRIS_FACTS)
    for pool in pools:
        fact = _random_text_entry(pool)
        if fact:
            return fact
    return None


def _random_blond_joke(language: Optional[str]) -> Optional[str]:
    return _random_text_entry(BLOND_JOKES)


def _random_yo_momma_joke(language: Optional[str]) -> Optional[str]:
    return _random_text_entry(YO_MOMMA_JOKES)


def _random_el_paso_fact() -> Optional[str]:
    return _random_text_entry(EL_PASO_FACTS)


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


def _cooldown_register(sender_key: Optional[str], sender_node, interface_ref) -> None:
    if not COOLDOWN_ENABLED or not sender_key or interface_ref is None:
        return
    normalized_key = sender_key.strip().lower()
    if normalized_key in COOLDOWN_ALLOWLIST:
        return
    now = time.time()
    message_to_send: Optional[str] = None
    with COOLDOWN_LOCK:
        state = COOLDOWN_STATE.get(sender_key)
        if not state:
            state = {
                'history': deque(),
                'warned': False,
                'warned_at': 0.0,
            }
            COOLDOWN_STATE[sender_key] = state
        history: deque = state['history']
        window_start = now - COOLDOWN_WINDOW_SECONDS
        while history and history[0] < window_start:
            history.popleft()
        history.append(now)

        warned = state.get('warned', False)
        threshold = COOLDOWN_THRESHOLD
        first_seen = NODE_FIRST_SEEN.get(sender_key)
        if first_seen is not None and (now - first_seen) <= COOLDOWN_NEW_NODE_SECONDS:
            threshold = min(threshold, COOLDOWN_THRESHOLD_NEW)
        if len(history) > threshold:
            if (not warned) or (now - state.get('warned_at', 0.0) >= COOLDOWN_WINDOW_SECONDS):
                state['warned'] = True
                state['warned_at'] = now
                message_to_send = random.choice(COOLDOWN_MESSAGES)
        else:
            if warned and len(history) <= COOLDOWN_THRESHOLD:
                state['warned'] = False

    if message_to_send:
        try:
            send_direct_chunks(interface_ref, message_to_send, sender_node)
            clean_log(f"Cooldown notice sent to {sender_key}", "❄️")
        except Exception as exc:
            clean_log(f"Cooldown notice failed for {sender_key}: {exc}", "⚠️", show_always=False)


def _log_high_cost(sender_id: Any, label: str, detail: Optional[str] = None) -> None:
    try:
        sender_key = _safe_sender_key(sender_id)
    except Exception:
        sender_key = str(sender_id)
    fragment = f" for {sender_key}" if sender_key else ""
    if detail:
        detail = textwrap.shorten(str(detail), width=80, placeholder="…")
        clean_log(f"High-cost {label}{fragment}: {detail}", "💸", show_always=False)
    else:
        clean_log(f"High-cost {label}{fragment}", "💸", show_always=False)


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
    "/offline": {"canonical": "/offline", "languages": ["en"]},
    "/checkmail": {"canonical": "/c", "languages": ["en"]},
    "/momma": {"canonical": "/yomomma", "languages": ["en"]},
    "/mommajoke": {"canonical": "/yomomma", "languages": ["en"]},
    "/yomommajoke": {"canonical": "/yomomma", "languages": ["en"]},
    "/websearch": {"canonical": "/web", "languages": ["en"]},
    "/search": {"canonical": "/web", "languages": ["en"]},
    "/math": {"canonical": "/mathquiz", "languages": ["en"]},
    "/mathtrivia": {"canonical": "/mathquiz", "languages": ["en"]},
    "/electric": {"canonical": "/electricalquiz", "languages": ["en"]},
    "/electrical": {"canonical": "/electricalquiz", "languages": ["en"]},
    "/wikipedia": {"canonical": "/wiki", "languages": ["en", "es"]},
    "/buscar": {"canonical": "/web", "languages": ["es"]},
    "/recherche": {"canonical": "/web", "languages": ["fr"]},
    "/suche": {"canonical": "/web", "languages": ["de"]},
    "/recal": {"canonical": "/recall", "languages": ["en"]},
    "/remember": {"canonical": "/recall", "languages": ["en"]},
    "/store": {"canonical": "/save", "languages": ["en"]},
    "/offlinewiki": {"canonical": "/offline", "languages": ["en"]},
    "/wikioffline": {"canonical": "/offline", "languages": ["en"]},
    "/drudgereport": {"canonical": "/drudge", "languages": ["en"]},
    "/chuck": {"canonical": "/chucknorris", "languages": ["en"]},
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
    "/quiz": {"canonical": "/trivia", "languages": ["en"]},
    "/triviagame": {"canonical": "/trivia", "languages": ["en"]},
    "/generaltrivia": {"canonical": "/trivia", "languages": ["en"]},
    "/biblequiz": {"canonical": "/bibletrivia", "languages": ["en"]},
    "/scripturetrivia": {"canonical": "/bibletrivia", "languages": ["en"]},
    "/disasterquiz": {"canonical": "/disastertrivia", "languages": ["en"]},
    "/prepquiz": {"canonical": "/disastertrivia", "languages": ["en"]},

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
    "/triviabiblica": {"canonical": "/bibletrivia", "languages": ["es"]},
    "/triviadesastres": {"canonical": "/disastertrivia", "languages": ["es"]},
    "/triviageneral": {"canonical": "/trivia", "languages": ["es"]},
    "/acertijos": {"canonical": "/trivia", "languages": ["es"]},
    "/matematicas": {"canonical": "/mathquiz", "languages": ["es"]},
    "/matemáticas": {"canonical": "/mathquiz", "languages": ["es"]},
    "/quizmatematico": {"canonical": "/mathquiz", "languages": ["es"]},
    "/electricidad": {"canonical": "/electricalquiz", "languages": ["es"]},
    "/triviaelectrica": {"canonical": "/electricalquiz", "languages": ["es"]},

    # French
    "/aide": {"canonical": "/help", "languages": ["fr"]},
    "/meteo": {"canonical": "/weather", "languages": ["fr"]},
    "/temps": {"canonical": "/weather", "languages": ["fr"]},
    "/messagedujour": {"canonical": "/motd", "languages": ["fr"]},
    "/verset": {"canonical": "/bible", "languages": ["fr"]},
    "/blaguechuck": {"canonical": "/chucknorris", "languages": ["fr"]},
    "/faitelpaso": {"canonical": "/elpaso", "languages": ["fr"]},
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
    "/badilisha_ujumbe": {"canonical": "/changemotd", "languages": ["sw"]},
    "/badilisha_prompt": {"canonical": "/changeprompt", "languages": ["sw"]},
    "/rekebisha_prompt": {"canonical": "/changeprompt", "languages": ["sw"]},
    "/onyesha_prompt": {"canonical": "/showprompt", "languages": ["sw"]},
    "/wekaupya": {"canonical": "/reset", "languages": ["sw"]},
    "/tumasms": {"canonical": "/sms", "languages": ["sw"]},
}

COMMAND_SUMMARIES: Dict[str, str] = {
    "/about": "Shares version info and connected radio details.",
    "/help": "Lists top commands with short usage notes.",
    "/menu": "Displays a compact menu of frequently used commands.",
    "/whereami": "Reports the node's mesh ID, channel, and location notes.",
    "/test": "Performs a quick self-check so you know the bot is responsive.",
    "/motd": "Shows the current message of the day announcement.",
    "/meshinfo": "Summarizes mesh health, channels, and node counts.",
    "/meshtastic": "Provides Meshtastic firmware tips and radio basics.",
    "/offline": "Delivers offline wiki and knowledge base snippets.",
    "/stop": "Stops any in-flight AI reply and silences the queue.",
    "/exit": "Shuts down Mesh-Master after saving state (admin only).",
    "/save": "Saves the latest DM or chat thread so you can recall it later.",
    "/recall": "Reloads a previously saved conversation transcript.",
    "/reset": "Clears cached AI context for a clean conversation restart.",
    "/data": "Manages or inspects data capsules saved by /save and other tools.",
    "/ai": "Routes the next prompt straight to the AI assistant.",
    "/bot": "Alias for interacting with the AI assistant.",
    "/vibe": "Adjusts the AI tone without editing the full prompt.",
    "/aipersonality": "Lists and selects available AI personas.",
    "/chathistory": "Summarizes the latest chatter for the current channel.",
    "/changeprompt": "Updates the system prompt that guides the AI.",
    "/showprompt": "Displays the current system prompt for review.",
    "/printprompt": "Prints the active prompt with minimal formatting.",
    "/changemotd": "Updates the global message of the day.",
    "/aisettings": "Shows key AI configuration values at a glance.",
    "/mail": "Compose and send a Mesh Mail message to another user.",
    "/m": "Shortcut for composing Mesh Mail.",
    "/checkmail": "Check your inbox for pending Mesh Mail.",
    "/c": "Shortcut for checking Mesh Mail.",
    "/emailhelp": "Explains how Mesh Mail works and available options.",
    "/wipe": "Clears stored Mesh Mail data (admin caution).",
    "/bible": "Shares a curated Bible verse with the channel.",
    "/biblehelp": "Shows usage tips for the Bible verse command.",
    "/weather": "Fetches the latest weather forecast for your configured location.",
    "/web": "Performs a live web search when the network allows it.",
    "/wiki": "Queries Wikipedia for a summary via the AI.",
    "/drudge": "Returns current Drudge Report headlines.",
    "/elpaso": "Posts a short historical or local fact about El Paso.",
    "/games": "Lists available interactive games.",
    "/blackjack": "Starts a quick blackjack round against the bot.",
    "/yahtzee": "Rolls dice for a solo Yahtzee game.",
    "/hangman": "Picks a word for you to guess one letter at a time.",
    "/wordle": "Runs a five-letter Wordle style puzzle.",
    "/adventure": "Launches the text adventure storyline.",
    "/wordladder": "Challenges you to transform words step by step.",
    "/rps": "Rock-paper-scissors showdown.",
    "/coinflip": "Flips a virtual coin with emoji flair.",
    "/cipher": "Provides a cipher puzzle to decode.",
    "/quizbattle": "Rapid-fire trivia showdown between players.",
    "/morse": "Practice Morse code with live feedback.",
    "/jokes": "Grabs a light-hearted joke from the archive.",
    "/chucknorris": "Classic Chuck Norris joke delivery.",
    "/blond": "Random blond humor (family friendly).",
    "/yomomma": "Light-hearted yo momma jokes.",
    "/alarm": "Set an alarm: /alarm 2:34pm [daily|mm/dd|sunday] [label]",
    "/timer": "Start a countdown: /timer 10m [label]",
    "/stopwatch": "Start/stop a stopwatch: /stopwatch start|stop|status",
}


def _command_alias_map() -> Dict[str, List[str]]:
    alias_map: Dict[str, Set[str]] = defaultdict(set)
    for alias, meta in COMMAND_ALIASES.items():
        canonical_raw = meta.get("canonical") if isinstance(meta, dict) else meta
        canonical = _normalize_command_name(canonical_raw or alias)
        alias_name = _normalize_command_name(alias)
        if canonical and alias_name and alias_name != canonical:
            alias_map[canonical].add(alias_name)
    # Include dynamic aliases from commands_config
    dyn = _get_dynamic_command_aliases()
    for alias, canonical in dyn.items():
        alias_name = _normalize_command_name(alias)
        canonical_name = _normalize_command_name(canonical)
        if alias_name and canonical_name and alias_name != canonical_name:
            alias_map[canonical_name].add(alias_name)
    custom_commands = commands_config.get("commands", []) if isinstance(commands_config, dict) else []
    for entry in custom_commands:
        if not isinstance(entry, dict):
            continue
        canonical = _normalize_command_name(entry.get("command"))
        aliases = entry.get("aliases")
        if canonical and isinstance(aliases, (list, tuple)):
            for alt in aliases:
                alias_name = _normalize_command_name(alt)
                if alias_name and alias_name != canonical:
                    alias_map[canonical].add(alias_name)
    return {key: sorted(values) for key, values in alias_map.items()}


BUILTIN_COMMANDS = {
    "/about",
    "/ai",
    "/bot",
    "/data",
    "/whereami",
    "/test",
    "/help",
    "/menu",
    "/offline",
    "/m",
    "/c",
    "/meshtastic",
    "/wipe",
    "/save",
    "/recall",
    "/stop",
    "/exit",
    "/aipersonality",
    "/biblehelp",
    "/jokes",
    "/games",
    "/adventure",
    "/blackjack",
    "/hangman",
    "/yahtzee",
    "/wordle",
    "/wordladder",
    "/choose",
    "/rps",
    "/coinflip",
    "/cipher",
    "/quizbattle",
    "/morse",
    "/mathquiz",
    "/electricalquiz",
    "/vibe",
    "/chathistory",
    "/aisettings",
    "/emailhelp",
    "/bibletrivia",
    "/disastertrivia",
    "/trivia",
    "/weather",
    "/motd",
    "/meshinfo",
    "/bible",
    "/chucknorris",
    "/elpaso",
    "/blond",
    "/yomomma",
    "/changemotd",
    "/changeprompt",
    "/showprompt",
    "/printprompt",
    "/reset",
    "/sms",
    "/web",
    "/drudge",
    "/wiki",
    "/alarm",
    "/timer",
    "/stopwatch",
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


USER_LANGUAGE_LOCK = threading.Lock()
USER_LANGUAGE_PREFS: Dict[str, str] = {}


def _load_user_language_map() -> Dict[str, str]:
    raw = safe_load_json(USER_LANGUAGE_FILE, {})
    prefs: Dict[str, str] = {}
    if isinstance(raw, dict):
        for key, value in raw.items():
            if not isinstance(key, str) or not isinstance(value, str):
                continue
            prefs[key] = _normalize_language_code(value)
    return prefs


def _save_user_language_map(snapshot: Dict[str, str]) -> None:
    directory = os.path.dirname(USER_LANGUAGE_FILE)
    if directory:
        os.makedirs(directory, exist_ok=True)
    write_atomic(USER_LANGUAGE_FILE, json.dumps(snapshot, indent=2, sort_keys=True))


with USER_LANGUAGE_LOCK:
    USER_LANGUAGE_PREFS.update(_load_user_language_map())


def _get_user_language(sender_key: Optional[str]) -> Optional[str]:
    if not sender_key:
        return None
    with USER_LANGUAGE_LOCK:
        return USER_LANGUAGE_PREFS.get(sender_key)


def _set_user_language(sender_key: Optional[str], language: Optional[str]) -> bool:
    if not sender_key or not language:
        return False
    normalized = _normalize_language_code(language)
    snapshot: Optional[Dict[str, str]] = None
    with USER_LANGUAGE_LOCK:
        current = USER_LANGUAGE_PREFS.get(sender_key)
        if current == normalized:
            return False
        USER_LANGUAGE_PREFS[sender_key] = normalized
        snapshot = dict(USER_LANGUAGE_PREFS)
    if snapshot is not None:
        _save_user_language_map(snapshot)
    return True


def _clear_user_language(sender_key: Optional[str]) -> None:
    if not sender_key:
        return
    snapshot: Optional[Dict[str, str]] = None
    with USER_LANGUAGE_LOCK:
        if sender_key in USER_LANGUAGE_PREFS:
            USER_LANGUAGE_PREFS.pop(sender_key, None)
            snapshot = dict(USER_LANGUAGE_PREFS)
    if snapshot is not None:
        _save_user_language_map(snapshot)


def _resolve_user_language(language_hint: Optional[str], sender_key: Optional[str]) -> str:
    if language_hint:
        return _normalize_language_code(language_hint)
    pref = _get_user_language(sender_key)
    if pref:
        return pref
    return LANGUAGE_FALLBACK


def _preferred_menu_language(language: Optional[str]) -> str:
    if language:
        return _normalize_language_code(language)
    return LANGUAGE_FALLBACK


LANGUAGE_DISPLAY_NAMES = {
    "en": "English",
    "es": "Spanish",
    "fr": "French",
    "de": "German",
    "sw": "Swahili",
    "pl": "Polish",
    "uk": "Ukrainian",
    "hr": "Croatian",
    "zh": "Chinese",
}


def _language_display_name(language: Optional[str]) -> str:
    if not language:
        return LANGUAGE_DISPLAY_NAMES.get(LANGUAGE_FALLBACK, "English")
    lang = _normalize_language_code(language)
    return LANGUAGE_DISPLAY_NAMES.get(lang, lang.capitalize())


ONBOARDING_STATE_FILE = config.get("onboarding_state_file", "data/onboarding_state.json")

ONBOARDING_MANAGER = OnboardingManager(
    state_path=ONBOARDING_STATE_FILE,
    clean_log=clean_log,
    mail_manager=MAIL_MANAGER,
    set_user_language=_set_user_language,
    get_user_language=_get_user_language,
    normalize_language=_normalize_language_code,
    stats=STATS,
)


MENU_DEFINITIONS = {
    "menu": {
        "title": {"en": "Main Menu", "es": "Menú principal"},
        "sections": [
            {
                "items": [
                    {"text": {"en": "Mail: /mail • /checkmail • /wipe", "es": "Correo: /mail • /checkmail • /wipe"}},
                    {"text": {"en": "Games: /games • /adventure • /mathquiz", "es": "Juegos: /games • /adventure • /mathquiz"}},
                    {"text": {"en": "DIY quiz: /electricalquiz", "es": "Quiz DIY: /electricalquiz"}},
                {"text": {"en": "Web search: /web <query>", "es": "Búsqueda web: /web <consulta>"}},
                    {"text": {"en": "Quick info: /help • /biblehelp • /meshtastic • /weather", "es": "Info rápida: /help • /biblehelp • /meshtastic • /weather"}},
                    {"text": {"en": "AI vibe controls: /vibe", "es": "Control de tono IA: /vibe"}},
                    {"text": {"en": "Ops: /motd • /meshinfo", "es": "Operaciones: /motd • /meshinfo"}},
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
                ]
            }
        ],
    },
    "aipersonality": {
        "title": {"en": "AI personalities", "es": "Personalidades IA"},
        "sections": [
            {
                "items": [
                    {"text": {"en": "List styles: /vibe", "es": "Ver estilos: /vibe"}},
                    {"text": {"en": "Switch tone: /vibe set <name>", "es": "Cambiar tono: /vibe set <name>"}},
                    {"text": {"en": "Check vibe: /vibe status", "es": "Ver tono: /vibe status"}},
                    {"text": {"en": "Reset tone: /vibe reset", "es": "Restablecer tono: /vibe reset"}},
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


def _localized_text(value: Any, lang: str) -> str:
    """Return a localized string for the requested language.

    Accepts dicts like {"en": "Hello", "es": "Hola"} and falls back to
    English or the first truthy value when the requested language is missing.
    Non-dict values are coerced to strings so callers don't need to guard.
    """

    if value is None:
        return ""

    if isinstance(value, str):
        return value

    if isinstance(value, dict):
        # Try exact language, then language without regional suffix, then English.
        normalized = _normalize_language_code(lang)
        candidates = [normalized]
        if "-" in lang:
            base_lang = lang.split("-", 1)[0].lower()
            candidates.append(base_lang)
        candidates.append("en")

        for key in candidates:
            text = value.get(key)
            if isinstance(text, str) and text.strip():
                return text

        # Fallback: first non-empty string value.
        for candidate in value.values():
            if isinstance(candidate, str) and candidate.strip():
                return candidate
        return ""

    if isinstance(value, (list, tuple)):
        parts = [str(part) for part in value if part is not None]
        return " ".join(part for part in parts if part.strip())

    return str(value)



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




COMMAND_CATEGORY_DEFINITIONS: "OrderedDict[str, Dict[str, Any]]" = OrderedDict([
    (
        "core_ops",
        {
            "label": "Core Ops",
            "commands": [
                "/about", "/help", "/menu", "/whereami", "/test", "/motd",
                "/meshinfo", "/meshtastic", "/offline", "/stop", "/exit",
                "/save", "/recall", "/reset", "/data",
                "/alarm", "/timer", "/stopwatch",
            ],
        },
    ),
    (
        "ai_tools",
        {
            "label": "AI & Personalization",
            "commands": [
                "/ai", "/bot", "/data", "/vibe", "/aipersonality", "/chathistory",
                "/changeprompt", "/showprompt", "/printprompt", "/changemotd", "/aisettings",
            ],
        },
    ),
    (
        "mail_ops",
        {
            "label": "Mail & Messaging",
            "commands": [
                "/m", "/mail", "/c", "/checkmail", "/emailhelp", "/wipe",
            ],
        },
    ),
    (
        "information",
        {
            "label": "Information & Reference",
            "commands": [
                "/bible", "/biblehelp", "/weather", "/web", "/wiki", "/drudge",
                "/motd", "/meshinfo", "/meshtastic", "/elpaso",
            ],
        },
    ),
    (
        "games",
        {
            "label": "Games",
            "commands": [
                "/games", "/blackjack", "/yahtzee", "/hangman", "/wordle", "/adventure", "/wordladder",
                "/rps", "/coinflip", "/cipher", "/quizbattle", "/morse",
            ],
        },
    ),
    (
        "fun",
        {
            "label": "Fun & Extras",
            "commands": [
                "/jokes", "/chucknorris", "/blond", "/yomomma",
            ],
        },
    ),
    (
        "custom",
        {
            "label": "Custom Commands",
            "commands": [],
        },
    ),
])


def _all_known_commands() -> Set[str]:
    return {
        cmd
        for cmd in (_normalize_command_name(token) for token in _known_commands())
        if cmd
    }


def _collect_command_categories() -> List[Dict[str, Any]]:
    known = _all_known_commands()
    assigned: Set[str] = set()
    categories: List[Dict[str, Any]] = []

    for cat_id, meta in COMMAND_CATEGORY_DEFINITIONS.items():
        label = meta.get("label", cat_id.title())
        configured = meta.get("commands", []) or []
        commands: Set[str] = set()
        for cmd in configured:
            normalized = _normalize_command_name(cmd)
            if normalized in known:
                commands.add(normalized)
                assigned.add(normalized)

        if cat_id == "custom":
            for entry in commands_config.get("commands", []):
                custom_cmd = _normalize_command_name(entry.get("command"))
                if custom_cmd:
                    commands.add(custom_cmd)
                    if custom_cmd in known:
                        assigned.add(custom_cmd)

        if commands:
            categories.append(
                {
                    "id": cat_id,
                    "label": label,
                    "commands": sorted(commands),
                }
            )

    remaining = sorted(cmd for cmd in known if cmd not in assigned and cmd.startswith('/'))
    if remaining:
        categories.append(
            {
                "id": "other",
                "label": "Other Commands",
                "commands": remaining,
            }
        )

    return categories


def _admin_snapshot() -> List[Dict[str, str]]:
    entries: List[Dict[str, str]] = []
    for key in sorted(AUTHORIZED_ADMINS):
        label = AUTHORIZED_ADMIN_NAMES.get(key) or key
        entries.append({'key': key, 'label': label})
    return entries


def gather_feature_snapshot() -> Dict[str, Any]:
    snapshot = get_feature_flags_snapshot()
    ai_enabled = bool(snapshot.get("ai_enabled", True))
    disabled_commands = snapshot.get("disabled_commands", [])
    message_mode = snapshot.get("message_mode", "both")
    admin_passphrase = snapshot.get("admin_passphrase", "")
    auto_ping_enabled = bool(snapshot.get("auto_ping_enabled", True))
    admin_whitelist = snapshot.get("admin_whitelist", [])

    disabled_set = {_normalize_command_name(cmd) for cmd in disabled_commands}
    alias_map = _command_alias_map()
    categories: List[Dict[str, Any]] = []
    for category in _collect_command_categories():
        entries = []
        category_label = category["label"]
        for cmd in category["commands"]:
            aliases = alias_map.get(cmd, [])
            entries.append(
                {
                    "name": cmd,
                    "enabled": cmd not in disabled_set,
                    "aliases": aliases,
                    "summary": COMMAND_SUMMARIES.get(cmd),
                    "category": category_label,
                }
            )
        categories.append(
            {
                "id": category["id"],
                "label": category_label,
                "commands": entries,
            }
        )

    alerts: List[str] = []
    if not ai_enabled:
        alerts.append("AI responses are disabled")
    if message_mode == "dm_only":
        alerts.append("Channel messages disabled")
    elif message_mode == "channel_only":
        alerts.append("Direct messages disabled")
    for cmd in disabled_commands:
        alerts.append(f"Command {cmd} disabled")
    if not auto_ping_enabled:
        alerts.append("Auto ping replies disabled")

    return {
        "ai_enabled": ai_enabled,
        "message_mode": message_mode if message_mode in MESSAGE_MODE_OPTIONS else "both",
        "categories": categories,
        "disabled_commands": disabled_commands,
        "admin_whitelist": admin_whitelist,
        "admin_passphrase": admin_passphrase,
        "auto_ping_enabled": auto_ping_enabled,
        "admins": _admin_snapshot(),
        "alerts": alerts,
    }


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



LANGUAGE_STRINGS = {
    "en": {
        "alias_note": "Interpreting {original} as {canonical} (alias).",
        "fuzzy_note": "Interpreting {original} as {canonical} (closest match).",
        "unknown_intro": "I didn't recognize `{original}` as a command.",
        "suggestion_intro": "Maybe you meant: {suggestions}.",
        "try_help": "Try `/help` for the full list.",
        "web_offline": "🌐 Offline mode only. Web search unavailable.",
    },
    "es": {
        "alias_note": "Interpretando {original} como {canonical} (alias).",
        "fuzzy_note": "Interpretando {original} como {canonical} (coincidencia más cercana).",
        "unknown_intro": "No reconocí `{original}` como un comando.",
        "suggestion_intro": "Quizá quisiste decir: {suggestions}.",
        "try_help": "Prueba `/help` para ver la lista completa.",
        "web_offline": "🌐 Solo modo fuera de línea. La búsqueda web no está disponible.",
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
        "admin_auth_required": "🔐 Se requiere acceso de administrador. Responde con la contraseña para continuar.",
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
        "meshinfo_network_usage_unknown": "Datos de uso de red no disponibles.",
        "meshinfo_active_nodes": "Nodos activos (1h): {count}",
        "meshinfo_message_volume": "Mensajes registrados (1h): {count}",
        "meshinfo_top_nodes": "Top nodos por tráfico: {list}",
        "meshinfo_top_nodes_none": "Sin tráfico registrado en la última hora",
        "bible_missing": "📜 La biblioteca de Escrituras no está disponible en este momento.",
        "bible_help": "📖 Guía rápida Biblia: `/biblia` sigue tu lectura. Busca con `/biblia Juan 3:16`. Añade `in Spanish` o `en inglés` para cambiar idioma. Avanza o retrocede con `<1,2>`. Responde 22 en DM para auto-scroll 30 versículos (18s).",
        "antispam_timeout_short": "🚫 Demasiadas solicitudes en poco tiempo. Pausa de 10 minutos. Puedes volver a escribir a las {time}. Otro exceso puede provocar un bloqueo de 24 horas.",
        "antispam_timeout_long": "🚫 Actividad repetida detectada. Acceso bloqueado por 24 horas. Podrás volver a usar el bot el {time}. Después de este bloqueo, los límites vuelven a empezar.",
        "antispam_log_short": "Usuario {node} en pausa 10m (hasta {time}).",
        "antispam_log_long": "Usuario {node} bloqueado 24h (hasta {time}).",
        "bible_autoscroll_dm_only": "AutoScroll solo está disponible en modo DM.",
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
    # dynamic aliases
    for alias, canonical in _get_dynamic_command_aliases().items():
        known.add(alias.lower())
        known.add(canonical.lower())
    return known


def resolve_command_token(raw: str):
    """Resolve a raw slash token to a canonical command and optional notice."""
    stripped = _strip_command_token(raw)
    # Dynamic alias mapping from commands_config
    dyn = _get_dynamic_command_aliases()
    if stripped in dyn:
        canonical = dyn.get(stripped, stripped)
        language = _detect_language_for_token(canonical)
        return canonical, "alias", None, language, ""
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


def _admin_credentials_match(attempt: str) -> Tuple[bool, Optional[str]]:
    """Return whether the attempt matches known admin secrets and the source used."""
    if not attempt:
        return False, None
    candidate = attempt.strip()
    if not candidate:
        return False, None
    normalized = candidate.casefold()
    if normalized and normalized == ADMIN_PASSWORD_NORM:
        return True, "config password"
    passphrase = get_admin_passphrase()
    if passphrase:
        passphrase_norm = passphrase.strip().casefold()
        if passphrase_norm and normalized == passphrase_norm:
            return True, "dashboard passphrase"
    return False, None


def _process_admin_password(sender_id: Any, message: str):
    sender_key = _safe_sender_key(sender_id)
    pending_request = PENDING_ADMIN_REQUESTS.get(sender_key)
    attempt = (message or "").strip()
    lang = None
    if pending_request:
        lang = pending_request.get("language")
    matched, source = _admin_credentials_match(attempt)
    if matched:
        AUTHORIZED_ADMINS.add(sender_key)
        if pending_request:
            PENDING_ADMIN_REQUESTS.pop(sender_key, None)
        try:
            actor = get_node_shortname(sender_id)
        except Exception:
            actor = str(sender_id)
        _register_admin_display(sender_key, sender_id, label=actor)
        source_note = f" via {source}" if source else ""
        clean_log(
            f"Admin credentials accepted{source_note} for {actor} ({sender_id})",
            "🛡️",
            show_always=True,
            rate_limit=False,
        )
        _ensure_admin_in_feature_flags(sender_key)
        base_success = translate(lang or 'en', 'password_success', "Bingo! you're now authorized to make admin changes")
        if '/admin' not in base_success:
            success_text = f"{base_success}\n🛡️ DM /admin for the control list."
        else:
            success_text = base_success
        success_text += "\n🛠️ Tip: /status shares a quick snapshot; /whatsoff lists anything disabled."
        follow_resp = None
        if pending_request:
            full_text = pending_request.get("full_text", "")
            is_direct = pending_request.get("is_direct", True)
            channel_idx = pending_request.get("channel_idx")
            thread_root_ts = pending_request.get("thread_root_ts")
            follow_resp = None
            if is_direct:
                try:
                    follow_resp = _admin_control_command(
                        full_text,
                        sender_id,
                        sender_key,
                        channel_idx,
                    )
                except Exception:
                    follow_resp = None
            if follow_resp is None:
                follow_resp = handle_command(
                    pending_request.get("command", ""),
                    full_text,
                    sender_id,
                    is_direct=is_direct,
                    channel_idx=channel_idx,
                    thread_root_ts=thread_root_ts,
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
    if text is None:
        return "[len=0]"
    try:
        length = len(text)
    except Exception:
        return "[len=?]"
    return f"[len={length}]"


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
    verse_start: Optional[int] = None
    verse_end: Optional[int] = None
    if verse_part:
        range_parts = [part.strip() for part in re.split(r"[-–—]", verse_part.strip()) if part.strip()]
        if not range_parts:
            return None
        try:
            verse_start = int(range_parts[0])
        except (TypeError, ValueError):
            return None
        if len(range_parts) > 1:
            try:
                verse_end = int(range_parts[1])
            except (TypeError, ValueError):
                return None
        else:
            verse_end = verse_start
    else:
        verse_start = 1
        verse_end = 1
    return book_raw, chapter, verse_start, verse_end

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

MESSAGE_RETENTION_SECONDS = 30 * 24 * 60 * 60


def _parse_message_timestamp(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except Exception:
            return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            if text.endswith(' UTC'):
                dt = datetime.strptime(text, "%Y-%m-%d %H:%M:%S UTC")
                return dt.replace(tzinfo=timezone.utc)
            if text.endswith('Z'):
                text = text[:-1] + '+00:00'
            dt = datetime.fromisoformat(text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            return None
    return None


def _prune_messages_locked() -> None:
    if MESSAGE_RETENTION_SECONDS <= 0:
        return
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=MESSAGE_RETENTION_SECONDS)
    retained: List[dict] = []
    for entry in messages:
        ts = _parse_message_timestamp(entry.get('timestamp'))
        if ts is None or ts >= cutoff:
            retained.append(entry)
    if len(retained) != len(messages):
        messages[:] = retained

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

QUEUE_NOTICE_THRESHOLD = 3
QUEUE_NOTICE_COOLDOWN_SECONDS = 600
QUEUE_NOTICE_TRACK: Dict[str, float] = {}
QUEUE_NOTICE_MESSAGES = [
    "🚦 High traffic on the Ollama runway—responses are moving slow. Thanks for hanging tight, chief!",
    "🛰️ Airwaves are jammed; your Ollama reply is in queue and will roll in shortly.",
    "⏳ Lots of folks pinging Ollama right now. Expect a little delay while I work through the stack.",
    "📡 Mesh control reports a backlog—give me a minute to clear the Ollama queue for you.",
    "🐢 Queue’s past three deep; I’ll circle back with your answer as soon as Ollama catches up.",
    "⚠️ Heavy chatter overhead. Your request is safe with me, just needs an extra beat to exit the queue.",
    "🎛️ Ollama’s buffers are redlined—appreciate the patience while we drain them down.",
    "🕰️ Busy minute on the mesh; hang tight and I’ll update you as soon as the queue thins out.",
]

CHILL_LOCK = threading.Lock()
CHILL_WAITLIST: Dict[str, Dict[str, Any]] = {}
CHILL_STATUS_UPDATE_INTERVAL_SECONDS = 5 * 60
CHILL_POLL_INTERVAL_SECONDS = 30
CHILL_INITIAL_DM = (
    "chill for a bit, the ai can't respond because the server is overloaded, we'll update you when the system is clear"
)
CHILL_STILL_WAITING_DM = (
    "still overloaded—hang tight. I'll ping you when it's clear."
)
CHILL_CLEARED_DM = (
    "we're clear now—you're good to try again."
)

# Pending confirmations for user blocklist
PENDING_BLOCK_CONFIRM: Dict[str, Dict[str, Any]] = {}

def cancel_pending_responses_for_sender(sender_key: Optional[str]) -> int:
    """Remove queued async response tasks for a given sender from the response queue.
    Returns the number of tasks removed.
    """
    if not sender_key:
        return 0
    removed = 0
    try:
        items: List[Any] = []
        while True:
            try:
                item = response_queue.get_nowait()
            except queue.Empty:
                break
            # task tuple layout: (text, sender_node, is_direct, ch_idx, thread_root_ts, interface)
            try:
                if isinstance(item, tuple) and len(item) >= 2:
                    node = item[1]
                    key = _safe_sender_key(node)
                    if key == sender_key:
                        removed += 1
                        continue  # drop
            except Exception:
                pass
            items.append(item)
        # Requeue the kept items
        for it in items:
            if it is None:
                continue
            response_queue.put_nowait(it)
    except Exception:
        # If anything goes wrong, fail safe with zero removed
        return removed
    return removed

def _ai_chill_overloaded() -> bool:
    # Read from live config to honor dashboard toggles without restart
    if not bool(config.get("ai_chill_mode", False)):
        return False
    # Only relevant for Ollama workload which uses the async response queue
    try:
        depth = response_queue.qsize()
    except Exception:
        depth = 0
    try:
        limit = int(config.get("ai_chill_queue_limit", CHILL_QUEUE_LIMIT))
    except (ValueError, TypeError):
        limit = CHILL_QUEUE_LIMIT
    limit = max(1, limit)
    return depth >= limit

def _ai_chill_track(sender_key: Optional[str], *, sender_node: Any = None) -> bool:
    if not sender_key:
        return False
    with CHILL_LOCK:
        existing = CHILL_WAITLIST.get(sender_key)
        now_ts = time.time()
        if existing:
            # Already tracked
            return False
        CHILL_WAITLIST[sender_key] = {
            'first_blocked_at': now_ts,
            'last_notified': 0.0,
            'node_id': sender_node,
        }
        return True

def _ai_chill_notify_initial(sender_key: Optional[str], sender_node: Any, interface_ref) -> None:
    if not sender_key or interface_ref is None:
        return
    if _is_user_blocked(sender_key) or _is_user_muted(sender_key):
        return
    try:
        send_direct_chunks(interface_ref, CHILL_INITIAL_DM, sender_node)
        with CHILL_LOCK:
            rec = CHILL_WAITLIST.get(sender_key)
            if rec is not None:
                rec['last_notified'] = time.time()
    except Exception as exc:
        clean_log(f"Chill DM failed to {sender_key}: {exc}", "⚠️")

def _ai_chill_notify_waiting(interface_ref) -> None:
    # Periodic status updates while still overloaded
    if interface_ref is None:
        return
    now_ts = time.time()
    to_update: List[Tuple[str, Any]] = []
    with CHILL_LOCK:
        for key, rec in CHILL_WAITLIST.items():
            if _is_user_blocked(key) or _is_user_muted(key):
                continue
            last = rec.get('last_notified', 0.0) or 0.0
            if now_ts - last >= CHILL_STATUS_UPDATE_INTERVAL_SECONDS:
                to_update.append((key, rec.get('node_id') or key))
                rec['last_notified'] = now_ts
    for key, node_id in to_update:
        try:
            # We need the node id; our waitlist stores only keys, but the node id equals the key text for DM routes
            # sender_key is derived from node id, so we can use it directly.
            send_direct_chunks(interface_ref, CHILL_STILL_WAITING_DM, node_id)
        except Exception as exc:
            clean_log(f"Chill periodic DM failed to {key}: {exc}", "⚠️")

def _ai_chill_notify_cleared(interface_ref) -> None:
    if interface_ref is None:
        return
    to_notify: List[Tuple[str, Any]] = []
    with CHILL_LOCK:
        for key, rec in CHILL_WAITLIST.items():
            to_notify.append((key, rec.get('node_id') or key))
        CHILL_WAITLIST.clear()
    for key, node_id in to_notify:
        try:
            send_direct_chunks(interface_ref, CHILL_CLEARED_DM, node_id)
        except Exception as exc:
            clean_log(f"Chill cleared DM failed to {key}: {exc}", "⚠️")

def ai_chill_notifier_worker():
    # Background thread to ping waitlisted users while overloaded and notify when clear
    last_overloaded = False
    while True:
        try:
            # Sample the queue depth
            overloaded = _ai_chill_overloaded()
            if bool(config.get("ai_chill_mode", False)):
                if overloaded:
                    _ai_chill_notify_waiting(interface)
                else:
                    # Transition from overloaded -> clear: notify and flush waitlist
                    if last_overloaded and (CHILL_WAITLIST):
                        _ai_chill_notify_cleared(interface)
            last_overloaded = overloaded
        except Exception:
            pass
        time.sleep(CHILL_POLL_INTERVAL_SECONDS)


def _maybe_notify_queue_delay(sender_key: Optional[str], sender_node: Any, interface_ref, is_direct: bool) -> None:
    if not sender_key or interface_ref is None:
        return
    if AI_PROVIDER != 'ollama':
        return
    now_ts = time.time()
    last_sent = QUEUE_NOTICE_TRACK.get(sender_key, 0.0)
    if now_ts - last_sent < QUEUE_NOTICE_COOLDOWN_SECONDS:
        return
    message = random.choice(QUEUE_NOTICE_MESSAGES)
    try:
        send_direct_chunks(interface_ref, message, sender_node)
        QUEUE_NOTICE_TRACK[sender_key] = now_ts
        clean_log(f"Queue delay notice sent to {sender_key}", "⚠️", show_always=True, rate_limit=False)
    except Exception as exc:
        clean_log(f"Failed to send queue delay notice to {sender_key}: {exc}", "⚠️")


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
                response_text, pending, already_sent = _normalize_ai_response(resp)
                truncation_notice = False

                if response_text:
                    target_name = get_node_shortname(sender_node) or str(sender_node)
                    summary = _truncate_for_log(response_text)
                    clean_log(
                        f"Ollama → {target_name} ({processing_time:.1f}s)",
                        "🦙",
                        show_always=True,
                        rate_limit=False,
                    )

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

                    # Send the response via mesh unless it was already streamed out
                    chunk_delay = pending.chunk_delay if pending else None
                    if interface_ref and response_text and not already_sent:
                        if is_direct:
                            result = send_direct_chunks(interface_ref, response_text, sender_node, chunk_delay=chunk_delay)
                            try:
                                sender_key_local = _safe_sender_key(sender_node)
                            except Exception:
                                sender_key_local = None
                            # Schedule partial resends for DM if needed
                            if RESEND_ENABLED and (not RESEND_DM_ONLY or True):
                                attempts = RESEND_USER_ATTEMPTS
                                interval = RESEND_USER_INTERVAL
                                chunks = result.get('chunks') or []
                                acks = result.get('acks') or []
                                RESEND_MANAGER.schedule_dm_resend(
                                    interface_ref=interface_ref,
                                    destination_id=sender_node,
                                    text=response_text,
                                    chunks=chunks,
                                    acks=acks,
                                    attempts=attempts,
                                    interval_seconds=interval,
                                    sender_key=sender_key_local,
                                    is_user_dm=True,
                                )
                        else:
                            send_broadcast_chunks(interface_ref, response_text, ch_idx, chunk_delay=chunk_delay)
                            # Optional broadcast resend
                            if RESEND_ENABLED and RESEND_BROADCAST_ENABLED and not RESEND_DM_ONLY:
                                RESEND_MANAGER.schedule_broadcast_resend(
                                    interface_ref=interface_ref,
                                    channel_idx=ch_idx,
                                    text=response_text,
                                    attempts=RESEND_SYSTEM_ATTEMPTS,
                                    interval_seconds=RESEND_SYSTEM_INTERVAL,
                                )

                    if pending and pending.follow_up_text and interface_ref:
                        _schedule_follow_up_message(
                            interface_ref,
                            pending.follow_up_text,
                            delay=pending.follow_up_delay,
                            is_direct=is_direct,
                            sender_node=sender_node,
                            channel_idx=ch_idx,
                        )

                sender_key = _safe_sender_key(sender_node)
                if is_direct and sender_key:
                    with CONTEXT_TRUNCATION_LOCK:
                        if sender_key in CONTEXT_TRUNCATED_SENDERS:
                            last_notice = CONTEXT_TRUNCATION_NOTICES.get(sender_key, 0.0)
                            now_ts = time.time()
                            if now_ts - last_notice > CONTEXT_TRUNCATION_COOLDOWN:
                                truncation_notice = True
                                CONTEXT_TRUNCATION_NOTICES[sender_key] = now_ts
                            CONTEXT_TRUNCATED_SENDERS.discard(sender_key)
                _antispam_after_response(sender_key, sender_node, interface_ref)
                _process_bible_autoscroll_request(sender_key, sender_node, interface_ref)

                if truncation_notice and interface_ref:
                    try:
                        send_direct_chunks(interface_ref, CONTEXT_TRUNCATION_NOTICE, sender_node)
                    except Exception as notice_exc:
                        clean_log(f"Context notice failed: {notice_exc}", "⚠️", show_always=False)
                
                try:
                    globals()['last_ai_response_time'] = _now()
                except Exception:
                    pass
                total_time = time.time() - start_time
                try:
                    STATS.record_ai_response(total_time)
                except Exception:
                    pass
            else:
                clean_log(f"Ollama → {get_node_shortname(sender_node) or sender_node} ({processing_time:.1f}s) [no response]", "🦙", show_always=True, rate_limit=False)
                try:
                    STATS.record_ai_response(processing_time)
                except Exception:
                    pass
                
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
    # Start chill notifier thread (lightweight loop)
    threading.Thread(target=ai_chill_notifier_worker, daemon=True).start()

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
    lat: Any,
    lon: Any,
    timestamp_val: float,
    precision: Any = None,
    dilution: Any = None,
    display_label: Optional[str] = None,
) -> Dict[str, Any]:
    if not sender_key:
        sender_key = "node"
    try:
        lat_float = float(lat)
    except Exception:
        lat_float = lat
    try:
        lon_float = float(lon)
    except Exception:
        lon_float = lon
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
        "key": display_label or sender_key,
        "lat": lat_float,
        "lon": lon_float,
        "timestamp": timestamp_val,
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
        results.append({
            "key": entry.get("key"),
            "lat": lat_val,
            "lon": lon_val,
            "time_str": _format_timestamp_local(entry.get("timestamp", _now())),
            "quality": entry.get("quality", "unknown"),
            "precision": entry.get("precision"),
            "dilution": entry.get("dilution"),
        })

    return results


def _prune_location_history_now() -> None:
    cutoff = _now() - LOCATION_HISTORY_RETENTION
    with LOCATION_HISTORY_LOCK:
        stale_keys = [key for key, value in LOCATION_HISTORY.items() if value.get("timestamp", 0) < cutoff]
        for key in stale_keys:
            LOCATION_HISTORY.pop(key, None)


def location_cleanup_worker(interval: float = 5.0) -> None:
    while True:
        try:
            time.sleep(max(1.0, float(interval)))
            _prune_location_history_now()
        except Exception:
            time.sleep(5.0)


def _normalize_context_key(value: Optional[str]) -> str:
    return " ".join(unidecode(str(value or "")).lower().split())


def _ensure_saved_contexts_loaded() -> None:
    global SAVED_CONTEXTS
    with SAVED_CONTEXT_LOCK:
        if SAVED_CONTEXTS:
            return
    raw = safe_load_json(str(SAVED_CONTEXT_FILE), {"users": {}})
    contexts: Dict[str, List[Dict[str, Any]]] = {}
    if isinstance(raw, dict):
        users = raw.get("users") or {}
        if isinstance(users, dict):
            for key, entries in users.items():
                key_str = str(key)
                bucket: List[Dict[str, Any]] = []
                if isinstance(entries, list):
                    for entry in entries:
                        if isinstance(entry, dict):
                            bucket.append(dict(entry))
                contexts[key_str] = bucket
    with SAVED_CONTEXT_LOCK:
        SAVED_CONTEXTS = contexts


def _persist_saved_contexts() -> None:
    with SAVED_CONTEXT_LOCK:
        payload = {"users": SAVED_CONTEXTS}
    try:
        SAVED_CONTEXT_FILE.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    write_atomic(str(SAVED_CONTEXT_FILE), json.dumps(payload, indent=2, ensure_ascii=False))


def _get_saved_contexts_for_user(sender_key: Optional[str]) -> List[Dict[str, Any]]:
    if not sender_key:
        return []
    _ensure_saved_contexts_loaded()
    with SAVED_CONTEXT_LOCK:
        entries = SAVED_CONTEXTS.get(sender_key, [])
        return [dict(entry) for entry in entries]


def _set_saved_contexts_for_user(sender_key: str, entries: List[Dict[str, Any]]) -> None:
    _ensure_saved_contexts_loaded()
    with SAVED_CONTEXT_LOCK:
        SAVED_CONTEXTS[sender_key] = [dict(entry) for entry in entries]
    _persist_saved_contexts()


def _set_context_session(sender_key: str, session: Dict[str, Any]) -> None:
    session = dict(session)
    session['created_at'] = session.get('created_at', _now())
    session['last_used'] = _now()
    session['expires_at'] = session.get('expires_at', _now() + CONTEXT_SESSION_TIMEOUT_SECONDS)
    session['language'] = session.get('language') or LANGUAGE_FALLBACK
    with CONTEXT_SESSION_LOCK:
        CONTEXT_SESSIONS[sender_key] = session


def _get_context_session(sender_key: Optional[str]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    if not sender_key:
        return None, None
    with CONTEXT_SESSION_LOCK:
        session = CONTEXT_SESSIONS.get(sender_key)
        if not session:
            return None, None
        expires_at = session.get('expires_at', 0)
        if expires_at and expires_at < _now():
            CONTEXT_SESSIONS.pop(sender_key, None)
            language = session.get('language') or LANGUAGE_FALLBACK
            title = session.get('title') or "your context window"
            notice = translate(language, 'context_session_expired', f"⚠️ Context window '{title}' expired. Resuming regular chat.", title=title)
            return None, notice
        session['last_used'] = _now()
        CONTEXT_SESSIONS[sender_key] = session
        return dict(session), None


def _clear_context_session(sender_key: Optional[str]) -> Optional[Dict[str, Any]]:
    if not sender_key:
        return None
    with CONTEXT_SESSION_LOCK:
        return CONTEXT_SESSIONS.pop(sender_key, None)


def _collect_dm_history_entries(sender_id: Any, max_messages: int = 200) -> List[Dict[str, Any]]:
    with messages_lock:
        snapshot = [m for m in messages if m.get('direct') is True]
    entries: List[Dict[str, Any]] = []
    for m in snapshot:
        node_id = m.get('node_id')
        if same_node_id(node_id, sender_id) or m.get('is_ai'):
            entries.append(m)
    if max_messages and len(entries) > max_messages:
        entries = entries[-max_messages:]
    return entries


def _format_conversation_lines(entries: List[Dict[str, Any]], max_chars: int) -> Tuple[str, List[str]]:
    if not entries:
        return "", []
    collected: List[str] = []
    total = 0
    for m in reversed(entries):
        text = str(m.get('message', '')).strip()
        if not text:
            continue
        node_id = m.get('node_id')
        try:
            speaker = get_node_shortname(node_id)
        except Exception:
            speaker = m.get('node') or ('AI' if m.get('is_ai') else 'User')
        speaker = speaker or ('AI' if m.get('is_ai') else 'User')
        line = f"{speaker}: {text}"
        total += len(line) + 1
        collected.append(line)
        if total >= max_chars:
            break
    collected.reverse()
    conversation = "\n".join(collected)
    return conversation, collected


def _auto_conversation_title(sender_id: Any, entries: List[Dict[str, Any]]) -> str:
    fallback = "Saved Conversation"
    for m in reversed(entries):
        if m.get('is_ai'):
            continue
        node_id = m.get('node_id')
        if not same_node_id(node_id, sender_id):
            continue
        text = str(m.get('message', '')).strip()
        if not text:
            continue
        try:
            intent = _detect_memory_intent(text)
        except Exception:
            intent = None
        if intent and intent[0] == "save":
            continue
        cleaned = re.sub(r"[\s]+", " ", text)
        cleaned = cleaned.strip(" ",)
        cleaned = cleaned.strip('"')
        if cleaned:
            if cleaned.startswith('/') and len(cleaned.split()) > 1:
                cleaned = cleaned.split(None, 1)[1]
            cleaned = cleaned.strip()
        if cleaned:
            shortened = textwrap.shorten(cleaned, width=60, placeholder="…")
            return shortened.title()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    return f"Saved Conversation {timestamp}"


def _ensure_unique_context_title(sender_key: str, desired: str) -> str:
    contexts = _get_saved_contexts_for_user(sender_key)
    existing = {str(c.get('title', '')).lower() for c in contexts}
    base = desired.strip() or "Saved Conversation"
    candidate = base
    counter = 2
    while candidate.lower() in existing:
        candidate = f"{base} ({counter})"
        counter += 1
    return candidate


def _build_search_blob(*parts: str) -> str:
    combined = " ".join(part for part in parts if part)
    return _normalize_context_key(combined)[:6000]


def _extract_tags(text: str) -> List[str]:
    tags: List[str] = []
    for token in text.split():
        if token.startswith('#') and len(token) > 1:
            cleaned = token.rstrip('.,;!?:')
            if cleaned not in tags:
                tags.append(cleaned)
    return tags


def _create_saved_context_entry(
    title: str,
    conversation: str,
    lines: List[str],
    auto_title: bool,
    *,
    tags: Optional[List[str]] = None,
    attribution: Optional[str] = None,
) -> Dict[str, Any]:
    created = _now()
    summary_candidates: List[str] = []
    if lines:
        for line in reversed(lines):
            message_text = ""
            if ": " in line:
                message_text = line.split(": ", 1)[1].strip()
            elif "\t" in line:
                message_text = line.split("\t", 1)[1].strip()
            skip = False
            if message_text:
                try:
                    intent = _detect_memory_intent(message_text)
                except Exception:
                    intent = None
                if intent and intent[0] == "save":
                    skip = True
            if skip:
                continue
            summary_candidates.append(line)
            if len(summary_candidates) >= SAVED_CONTEXT_SUMMARY_LINES:
                break
        summary_candidates.reverse()
    summary_source = " ".join(summary_candidates) if summary_candidates else conversation.replace("\n", " ")
    summary = textwrap.shorten(summary_source, width=SAVED_CONTEXT_SUMMARY_CHARS, placeholder="…") if summary_source else "(no summary)"
    search_blob = _build_search_blob(title, summary, conversation)
    tags = tags or []
    return {
        "id": str(uuid.uuid4()),
        "title": title,
        "auto_title": auto_title,
        "summary": summary,
        "context": conversation,
        "search_blob": search_blob,
        "search_key": _normalize_context_key(title),
        "lines": len(lines),
        "created_at": created,
        "updated_at": created,
        "tags": tags,
        "attribution": attribution,
        "saved_at_human": _format_timestamp_local(created),
    }


def _save_conversation_for_user(
    sender_id: Any,
    sender_key: str,
    requested_title: Optional[str],
    lang: str,
    auto_title: bool = False,
) -> Optional[PendingReply]:
    entries = _collect_dm_history_entries(sender_id, max_messages=400)
    if not entries:
        return PendingReply("ℹ️ I don't have any recent DM messages to save.", "/save command")
    conversation, lines = _format_conversation_lines(entries, SAVED_CONTEXT_MAX_CHARS)
    if not conversation:
        return PendingReply("ℹ️ Nothing to save yet. Send a few messages first.", "/save command")
    title = (requested_title or "").strip()
    if not title:
        return PendingReply("📝 Give this chat a name using `/save <title>` first.", "/save command")
    if len(title) > SAVED_CONTEXT_TITLE_MAX:
        return PendingReply(
            f"⚠️ Title too long. Keep it under {SAVED_CONTEXT_TITLE_MAX} characters.",
            "/save command",
        )
    title = _ensure_unique_context_title(sender_key, title)
    entry = _create_saved_context_entry(title, conversation, lines, auto_title)
    contexts = _get_saved_contexts_for_user(sender_key)
    contexts.append(entry)
    if len(contexts) > SAVED_CONTEXT_MAX_PER_USER:
        contexts.sort(key=lambda item: item.get('updated_at', item.get('created_at', 0)))
        removed = contexts[:-SAVED_CONTEXT_MAX_PER_USER]
        contexts = contexts[-SAVED_CONTEXT_MAX_PER_USER:]
    else:
        removed = []
    _set_saved_contexts_for_user(sender_key, contexts)
    removal_note = ""
    if removed:
        removal_note = "\n(Oldest saved chat removed to make space.)"
    confirmation = [
        f"📝 Stored this conversation as '{title}'.",
        "Browse your saves with `/chathistory`.",
    ]
    if entry.get('summary'):
        confirmation.append(f"Summary: {entry['summary']}")
    message = "\n".join(confirmation) + removal_note
    return PendingReply(message, "/save command")


def _find_saved_context_by_id(sender_key: str, entry_id: str) -> Optional[Dict[str, Any]]:
    contexts = _get_saved_contexts_for_user(sender_key)
    for entry in contexts:
        if entry.get('id') == entry_id:
            return entry
    return None


def _touch_saved_context(sender_key: str, entry_id: str) -> None:
    contexts = _get_saved_contexts_for_user(sender_key)
    updated = False
    now_ts = _now()
    for entry in contexts:
        if entry.get('id') == entry_id:
            entry['updated_at'] = now_ts
            updated = True
            break
    if updated:
        _set_saved_contexts_for_user(sender_key, contexts)


def _find_saved_context_matches(sender_key: str, query: str, limit: int = 5) -> List[Tuple[Dict[str, Any], float]]:
    contexts = _get_saved_contexts_for_user(sender_key)
    if not contexts:
        return []
    normalized_query = _normalize_context_key(query)
    if not normalized_query:
        return []
    scores: List[Tuple[Dict[str, Any], float]] = []
    for entry in contexts:
        title_key = entry.get('search_key') or _normalize_context_key(entry.get('title'))
        blob = entry.get('search_blob') or title_key
        score_title = difflib.SequenceMatcher(None, normalized_query, title_key).ratio()
        score_blob = difflib.SequenceMatcher(None, normalized_query, blob).ratio()
        if normalized_query in title_key:
            score_title = max(score_title, 0.99)
        if normalized_query in blob:
            score_blob = max(score_blob, 0.99)
        score = max(score_title, score_blob)
        scores.append((entry, score))
    scores.sort(key=lambda item: item[1], reverse=True)
    if limit:
        scores = scores[:limit]
    return scores


def _format_saved_context_list(sender_key: str) -> str:
    contexts = _get_saved_contexts_for_user(sender_key)
    if not contexts:
        return "ℹ️ You haven't saved any conversations yet. Use `/save` in a DM to store one."
    contexts_sorted = sorted(contexts, key=lambda x: x.get('updated_at', x.get('created_at', 0)), reverse=True)
    lines = ["Saved conversations:"]
    for idx, entry in enumerate(contexts_sorted[:SAVED_CONTEXT_LIST_LIMIT], 1):
        title = entry.get('title') or f"Conversation {idx}"
        saved_ts = entry.get('updated_at', entry.get('created_at', 0))
        saved_str = _format_timestamp_local(saved_ts)
        summary = entry.get('summary') or "(no summary)"
        lines.append(f"{idx}. {title} — saved {saved_str}. {summary}")
    if len(contexts_sorted) > SAVED_CONTEXT_LIST_LIMIT:
        lines.append(f"…and {len(contexts_sorted) - SAVED_CONTEXT_LIST_LIMIT} more. Use `/chathistory` or `/recall <topic>` to explore.")
    return "\n".join(lines)


def _start_chathistory_menu(sender_key: Optional[str]) -> PendingReply:
    if not sender_key:
        return PendingReply("⚠️ I couldn't identify your DM session. Try again in a moment.", "/chathistory list")

    contexts = _get_saved_contexts_for_user(sender_key)
    if not contexts:
        return PendingReply("ℹ️ You haven't saved any conversations yet. Use `/save <title>` to store one.", "/chathistory list")

    contexts_sorted = sorted(contexts, key=lambda x: x.get('updated_at', x.get('created_at', 0)), reverse=True)
    displayed = contexts_sorted[:SAVED_CONTEXT_LIST_LIMIT]

    lines = ["📚 Saved chats (most recent first):"]
    id_list: List[str] = []
    for idx, entry in enumerate(displayed, 1):
        title = entry.get('title') or f"Conversation {idx}"
        saved_ts = entry.get('updated_at', entry.get('created_at', 0))
        saved_str = _format_timestamp_local(saved_ts)
        lines.append(f"{idx}. {title} — saved {saved_str}")
        id_list.append(entry.get('id'))

    if len(contexts_sorted) > len(displayed):
        lines.append(f"…oldest chats are automatically pruned after {SAVED_CONTEXT_MAX_PER_USER} saves.")

    lines.append("Reply with the number to recall a chat, or X to cancel.")

    PENDING_RECALL_SELECTIONS[sender_key] = {
        'mode': 'choose',
        'ids': id_list,
        'created': _now(),
    }
    return PendingReply("\n".join(lines), "/chathistory list")


def _activate_saved_context_session(sender_id: Any, sender_key: str, entry: Dict[str, Any]) -> PendingReply:
    context = entry.get('context') or ""
    if not context:
        return PendingReply("⚠️ That saved conversation has no content.", "/recall command")
    title = entry.get('title') or "Saved Conversation"
    summary = entry.get('summary') or ""
    attribution = entry.get('attribution') or "Saved from a previous DM conversation."
    tags = entry.get('tags') or []
    tags_line = f"Tags: {' '.join(tags)}" if tags else ""
    saved_context_header = f"Saved conversation '{title}' context" if not entry.get('auto_title') else f"Saved conversation context"
    saved_on = entry.get('saved_at_human') or _format_timestamp_local(entry.get('created_at', _now()))
    context_block = f"{saved_context_header} (saved {saved_on})\n{context}\n\nAttribution: {attribution}"
    if tags_line:
        context_block += f"\n{tags_line}"
    prompt_addendum = (
        f"You are resuming the saved conversation titled '{title}'. "
        "Use the provided context as prior discussion so replies stay consistent. "
        "If the user changes topics entirely, handle it normally after acknowledging the switch."
    )
    session = {
        'type': 'saved',
        'title': title,
        'summary': summary,
        'context': context_block,
        'prompt_addendum': prompt_addendum,
        'use_history': False,
    }
    _set_context_session(sender_key, session)
    _touch_saved_context(sender_key, entry.get('id', ''))
    lines = [
        f"📂 Loaded conversation '{title}'.",
        "Type /exit to leave this context window.",
    ]
    if summary:
        lines.append(f"Summary: {summary}")
    if tags_line:
        lines.append(tags_line)
    return PendingReply("\n".join(lines), "/recall command")


def _start_recall_selection(sender_key: str, matches: List[Tuple[Dict[str, Any], float]], confirm_only: bool = False) -> PendingReply:
    now_ts = _now()
    if not matches:
        return PendingReply("⚠️ I couldn't find a saved conversation matching that request.", "/recall command")
    top_entry = matches[0][0]
    if confirm_only or len(matches) == 1:
        PENDING_RECALL_SELECTIONS[sender_key] = {
            'mode': 'confirm',
            'ids': [top_entry.get('id')],
            'created': now_ts,
        }
        title = top_entry.get('title') or 'conversation'
        summary = top_entry.get('summary') or ''
        lines = [
            f"I found a saved conversation called '{title}'.",
            "Load it now? Reply Y or N.",
        ]
        if summary:
            lines.append(f"Summary: {summary}")
        return PendingReply("\n".join(lines), "/recall confirm")

    ids: List[str] = []
    display_lines = ["I found several saved conversations:"]
    for idx, (entry, score) in enumerate(matches[:SAVED_CONTEXT_LIST_LIMIT], 1):
        ids.append(entry.get('id'))
        title = entry.get('title') or f"Conversation {idx}"
        summary = entry.get('summary') or ''
        display_lines.append(f"{idx}. {title} — {summary}")
    display_lines.append("Reply with the number to load, or N to cancel.")
    PENDING_RECALL_SELECTIONS[sender_key] = {
        'mode': 'choose',
        'ids': ids,
        'created': now_ts,
    }
    return PendingReply("\n".join(display_lines), "/recall select")


def _handle_pending_recall_response(
    sender_id: Any,
    sender_key: str,
    text: str,
) -> Optional[PendingReply]:
    pending = PENDING_RECALL_SELECTIONS.get(sender_key)
    if not pending:
        return None
    if (_now() - pending.get('created', 0)) > 600:
        PENDING_RECALL_SELECTIONS.pop(sender_key, None)
        return PendingReply("⏱️ That recall prompt expired. Try again with `/recall <topic>`.", "/recall select")
    cleaned = text.strip().lower()
    mode = pending.get('mode')
    if mode == 'choose':
        if cleaned in {'n', 'no', 'cancel', 'exit', 'x'}:
            PENDING_RECALL_SELECTIONS.pop(sender_key, None)
            return PendingReply("Okay, no conversation loaded.", "/recall select")
        try:
            index = int(cleaned)
        except ValueError:
            return PendingReply("Please reply with the number from the list or N to cancel.", "/recall select")
        if index < 1 or index > len(pending.get('ids', [])):
            return PendingReply("That number isn't on the list. Try again or reply N to cancel.", "/recall select")
        entry_id = pending['ids'][index - 1]
        entry = _find_saved_context_by_id(sender_key, entry_id)
        PENDING_RECALL_SELECTIONS.pop(sender_key, None)
        if not entry:
            return PendingReply("⚠️ I couldn't load that conversation. It may have been removed.", "/recall command")
        return _activate_saved_context_session(sender_id, sender_key, entry)
    else:  # confirm
        if cleaned in {'y', 'yes'}:
            entry_id = (pending.get('ids') or [None])[0]
            entry = _find_saved_context_by_id(sender_key, entry_id)
            PENDING_RECALL_SELECTIONS.pop(sender_key, None)
            if not entry:
                return PendingReply("⚠️ That conversation is no longer available.", "/recall command")
            return _activate_saved_context_session(sender_id, sender_key, entry)
        if cleaned in {'n', 'no', 'cancel', 'exit'}:
            PENDING_RECALL_SELECTIONS.pop(sender_key, None)
        return PendingReply("No problem—conversation not loaded.", "/recall confirm")
    return PendingReply("Please reply Y to load it or N to cancel.", "/recall confirm")


def _handle_pending_wipe_selection(sender_id: Any, sender_key: str, text: str) -> Optional[PendingReply]:
    state = PENDING_WIPE_SELECTIONS.get(sender_key)
    if not state:
        return None

    mailboxes: List[str] = list(state.get('mailboxes') or [])
    if not mailboxes:
        PENDING_WIPE_SELECTIONS.pop(sender_key, None)
        return PendingReply("⚠️ No mailboxes on file. Run `/wipe mailbox` again.", "/wipe select")

    cleaned = (text or "").strip()
    if not cleaned:
        options = ", ".join(f"{idx}) {name}" for idx, name in enumerate(mailboxes, 1))
        return PendingReply(
            (
                f"Reply with 1-{len(mailboxes)} to choose an inbox, {len(mailboxes) + 1} to wipe them all, "
                "or 0 to cancel. Options: "
                f"{options}"
            ),
            "/wipe select",
        )

    lowered = cleaned.lower()
    if lowered in {"n", "no", "cancel", "stop", "exit", "abort"}:
        PENDING_WIPE_SELECTIONS.pop(sender_key, None)
        return PendingReply("👍 Cancelled. Nothing was deleted.", "/wipe select")

    allow_all = bool(state.get('allow_all'))
    lang = state.get('language')
    actor_label = state.get('actor')
    try:
        sender_short = get_node_shortname(sender_id)
    except Exception:
        sender_short = str(sender_id)
    actor_display = actor_label or sender_short

    selected_mailbox: Optional[str] = None
    if cleaned.isdigit():
        index = int(cleaned)
        if index == 0:
            PENDING_WIPE_SELECTIONS.pop(sender_key, None)
            return PendingReply("👍 Cancelled. Nothing was deleted.", "/wipe select")
        if 1 <= index <= len(mailboxes):
            selected_mailbox = mailboxes[index - 1]
        elif allow_all and index == len(mailboxes) + 1 and mailboxes:
            PENDING_WIPE_SELECTIONS.pop(sender_key, None)
            if sender_key:
                PENDING_WIPE_REQUESTS.pop(sender_key, None)
                PENDING_WIPE_REQUESTS[sender_key] = {
                    "action": "multi_mailbox",
                    "mailboxes": list(mailboxes),
                    "language": lang,
                }
            clean_log(
                f"Mailbox wipe requested for all ({len(mailboxes)}) inboxes linked to {actor_display}",
                "🧹",
            )
            return PendingReply(
                f"🧹 Wipe all {len(mailboxes)} inboxes listed? Reply Y or N.",
                "/wipe confirm",
            )
        else:
            return PendingReply(
                f"❓ Pick 1-{len(mailboxes)} for a single inbox, {len(mailboxes) + 1} to wipe them all, or 0 to cancel.",
                "/wipe select",
            )
    else:
        for name in mailboxes:
            if name.lower() == lowered:
                selected_mailbox = name
                break
        if allow_all and lowered in {"all", "todo", "todas", "alles"} and mailboxes:
            PENDING_WIPE_SELECTIONS.pop(sender_key, None)
            if sender_key:
                PENDING_WIPE_REQUESTS.pop(sender_key, None)
                PENDING_WIPE_REQUESTS[sender_key] = {
                    "action": "multi_mailbox",
                    "mailboxes": list(mailboxes),
                    "language": lang,
                }
            clean_log(
                f"Mailbox wipe requested for all ({len(mailboxes)}) inboxes linked to {actor_display}",
                "🧹",
            )
            return PendingReply(
                f"🧹 Wipe all {len(mailboxes)} inboxes listed? Reply Y or N.",
                "/wipe confirm",
            )

    if not selected_mailbox:
        return PendingReply(
            f"I didn't recognize that response. Reply with 1-{len(mailboxes)}, {len(mailboxes) + 1} for all, or 0 to cancel.",
            "/wipe select",
        )

    PENDING_WIPE_SELECTIONS.pop(sender_key, None)
    clean_log(f"Mailbox wipe requested for '{selected_mailbox}' by {actor_display}", "🧹")
    if sender_key:
        PENDING_WIPE_REQUESTS.pop(sender_key, None)
        PENDING_WIPE_REQUESTS[sender_key] = {
            "action": "mailbox",
            "mailbox": selected_mailbox,
            "language": lang,
        }
    return PendingReply(
        f"🧹 Delete mailbox '{selected_mailbox}' permanently? Reply Y or N.",
        "/wipe confirm",
    )


def _handle_pending_mailbox_selection(sender_id: Any, sender_key: str, text: str) -> Optional[PendingReply]:
    state = PENDING_MAILBOX_SELECTIONS.get(sender_key)
    if not state:
        return None

    mailboxes: List[str] = state.get('mailboxes') or []
    if not mailboxes:
        PENDING_MAILBOX_SELECTIONS.pop(sender_key, None)
        return PendingReply("⚠️ No linked mailboxes found. Try `/mail <name> <message>` to create one.", "/c select")

    cleaned = text.strip()
    if not cleaned:
        choices = ", ".join(f"{idx}) {name}" for idx, name in enumerate(mailboxes, 1))
        prompt = (
            f"Reply with 1-{len(mailboxes)} or send `/c <mailbox>`. "
            f"Add your PIN after the inbox name if it requires one. Choices: {choices}"
        )
        return PendingReply(prompt, "/c select")

    selected_mailbox: Optional[str] = None
    if cleaned.isdigit():
        index = int(cleaned)
        if 1 <= index <= len(mailboxes):
            selected_mailbox = mailboxes[index - 1]
    else:
        lowered = cleaned.lower()
        for name in mailboxes:
            if name.lower() == lowered:
                selected_mailbox = name
                break

    if not selected_mailbox:
        choices = ", ".join(f"{idx}) {name}" for idx, name in enumerate(mailboxes, 1))
        return PendingReply(
            f"I didn't recognize that choice. Reply with 1-{len(mailboxes)} or type the inbox name (include your PIN if needed). Options: {choices}",
            "/c select",
        )

    PENDING_MAILBOX_SELECTIONS.pop(sender_key, None)
    try:
        sender_short = get_node_shortname(sender_id)
    except Exception:
        sender_short = str(sender_id)
    return MAIL_MANAGER.handle_check(sender_key, sender_id, sender_short, selected_mailbox, "")


def _handle_pending_save_response(sender_id: Any, sender_key: str, text: str) -> Optional[PendingReply]:
    state = PENDING_SAVE_WIZARDS.get(sender_key)
    if not state:
        return None
    if (_now() - state.get('created', 0.0)) > SAVE_WIZARD_TIMEOUT:
        PENDING_SAVE_WIZARDS.pop(sender_key, None)
        return PendingReply("⏱️ Save prompt expired. Run `/save` again when you're ready.", "/save wizard")

    cleaned = text.strip()
    if not cleaned:
        return PendingReply("📝 Please reply with a short title, or N to cancel.", "/save wizard")

    lowered = cleaned.lower()
    if lowered in {'n', 'no', 'cancel', 'exit'}:
        PENDING_SAVE_WIZARDS.pop(sender_key, None)
        return PendingReply("👍 Not saving this chat.", "/save wizard")

    cleaned = cleaned.strip()
    if cleaned.startswith(('"', "'")) and cleaned.endswith(('"', "'")) and len(cleaned) >= 2:
        cleaned = cleaned[1:-1].strip()
    if not cleaned:
        return PendingReply("📝 Please reply with a short title, or N to cancel.", "/save wizard")
    if len(cleaned) > SAVED_CONTEXT_TITLE_MAX:
        return PendingReply(
            f"⚠️ Title too long. Keep it under {SAVED_CONTEXT_TITLE_MAX} characters.",
            "/save wizard",
        )

    lang = state.get('language', LANGUAGE_FALLBACK)
    origin_id = state.get('sender_id', sender_id)
    PENDING_SAVE_WIZARDS.pop(sender_key, None)
    return _save_conversation_for_user(origin_id, sender_key, cleaned, lang, auto_title=False)


def _handle_exit_session(sender_key: Optional[str]) -> PendingReply:
    session = _clear_context_session(sender_key)
    if session:
        title = session.get('title') or "context session"
        return PendingReply(f"✅ Closed the {title} context window. We're back to regular chat.", "/exit command")
    return PendingReply("ℹ️ There's no saved context active right now.", "/exit command")

def _detect_memory_intent(message: str) -> Optional[Tuple[str, Optional[str]]]:
    if not message:
        return None
    lowered = message.lower().strip()
    # Save intents with explicit topics
    save_topic_prefixes = [
        "remember this conversation about",
        "save this conversation about",
        "save conversation about",
        "remember this about",
    ]
    for prefix in save_topic_prefixes:
        if lowered.startswith(prefix):
            topic = message[len(prefix):].strip().strip(" ?!.\"")
            return ("save", topic or None)

    recall_prefixes = [
        "remember when we were talking about",
        "remember when we talked about",
        "remember our conversation about",
        "remember the conversation about",
        "remember our chat about",
    ]
    for prefix in recall_prefixes:
        if lowered.startswith(prefix):
            topic = message[len(prefix):].strip().strip(" ?!.\"")
            return ("recall", topic or None)

    save_simple_triggers = {
        "remember this conversation",
        "remember this",
        "remember",
        "save this conversation",
        "save this",
        "save",
        "save this to memory",
        "save this to your memory",
        "save to memory",
        "save this convo",
        "remember this convo",
    }
    for trigger in save_simple_triggers:
        if lowered == trigger or lowered.startswith(trigger + " ?"):
            return ("save", None)

    recall_question_triggers = {
        "remember when we were talking",
        "remember when we were chatting",
        "remember our conversation",
    }
    for trigger in recall_question_triggers:
        if lowered.startswith(trigger):
            # Treated as recall request even if topic missing; user can clarify.
            remainder = message[len(trigger):].strip().strip(" ?!.\"")
            return ("recall", remainder or None)

    return None


def _maybe_handle_memory_intent(
    sender_id: Any,
    sender_key: Optional[str],
    text: str,
    lang: str,
    check_only: bool = False,
) -> Optional[Union[PendingReply, bool]]:
    intent = _detect_memory_intent(text)
    if not intent or not sender_key:
        return None
    action, topic = intent
    if action == "save":
        if topic:
            if check_only:
                return False
            return _save_conversation_for_user(sender_id, sender_key, topic, lang, auto_title=False)
        if check_only:
            return False
        PENDING_SAVE_WIZARDS[sender_key] = {
            'created': _now(),
            'sender_id': sender_id,
            'language': lang,
        }
        return PendingReply(
            "📝 What should I call this conversation? Reply with a short title (or N to cancel).",
            "/save wizard",
        )
    if action == "recall":
        contexts = _get_saved_contexts_for_user(sender_key)
        if not contexts:
            if check_only:
                return False
            return PendingReply("ℹ️ You haven't saved any conversations yet. Use `/save` first.", "memory recall")
        if not topic:
            if check_only:
                return False
            return _start_chathistory_menu(sender_key)
        matches = _find_saved_context_matches(sender_key, topic, limit=5)
        if not matches:
            if check_only:
                return False
            return PendingReply("⚠️ I couldn't find a saved conversation that matches that. Try a different description or `/recall` to list entries.", "memory recall")
        if check_only:
            return False
        # Prefer confirmation when the top match is strong.
        confirm = False
        if len(matches) == 1:
            confirm = True
        elif matches[0][1] >= 0.8 and (len(matches) == 1 or matches[0][1] - matches[1][1] >= 0.2):
            confirm = True
        return _start_recall_selection(sender_key, matches, confirm_only=confirm)
    return None



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


def _weather_condition_label(code: Any) -> str:
    try:
        code_int = int(code)
    except Exception:
        return "mixed conditions"
    return WEATHER_CODE_DESCRIPTIONS.get(code_int, "mixed conditions")


def _format_temperature(temp_c: Optional[float]) -> str:
    if temp_c is None:
        return "?"
    try:
        temp_f = (float(temp_c) * 9.0 / 5.0) + 32.0
        return f"{temp_f:.0f}°F"
    except Exception:
        return "?"


def _format_precip_inches(mm_value: Optional[float]) -> str:
    if mm_value is None:
        return "0 in"
    try:
        inches = float(mm_value) / 25.4
    except Exception:
        return "0 in"
    if inches < 0.05:
        return "<0.05 in"
    return f"{inches:.2f} in"


def _format_wind_speed(kph: Optional[float]) -> str:
    if kph is None:
        return "?"
    try:
        mph = float(kph) * 0.621371
        return f"{mph:.0f} mph"
    except Exception:
        return "?"


def _format_forecast_date(date_str: str) -> str:
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.strftime("%a %b %d")
    except Exception:
        return date_str


def _fetch_weather_forecast(lang: str) -> str:
    now = _now()
    with WEATHER_CACHE_LOCK:
        cached_text = WEATHER_CACHE.get('text')
        cached_time = WEATHER_CACHE.get('timestamp', 0.0)
        if cached_text and cached_time and (now - float(cached_time)) < WEATHER_CACHE_TTL:
            return str(cached_text)

    params = {
        "latitude": WEATHER_LAT,
        "longitude": WEATHER_LON,
        "current_weather": "true",
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max,weathercode",
        "timezone": "auto",
    }
    url = "https://api.open-meteo.com/v1/forecast"
    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
    except Exception as exc:
        raise RuntimeError(exc) from exc

    lines: List[str] = []
    header = translate(lang, 'weather_online_header', f"🌤️ Weather for {WEATHER_LOCATION_NAME}")
    lines.append(header)

    current = data.get('current_weather') or {}
    if current:
        temp_now = _format_temperature(current.get('temperature'))
        wind_now = _format_wind_speed(current.get('windspeed'))
        cond = _weather_condition_label(current.get('weathercode'))
        lines.append(translate(lang, 'weather_now_line', f"Now: {temp_now}, {cond}, wind {wind_now}"))

    daily = data.get('daily') or {}
    times = daily.get('time') or []
    highs = daily.get('temperature_2m_max') or []
    lows = daily.get('temperature_2m_min') or []
    precip = daily.get('precipitation_sum') or []
    wind = daily.get('windspeed_10m_max') or []
    codes = daily.get('weathercode') or []

    for idx, label in enumerate(["Today", "Tomorrow"]):
        if idx >= len(times):
            break
        temp_high = _format_temperature(highs[idx] if idx < len(highs) else None)
        temp_low = _format_temperature(lows[idx] if idx < len(lows) else None)
        precip_label = _format_precip_inches(precip[idx] if idx < len(precip) else None)
        wind_label = _format_wind_speed(wind[idx] if idx < len(wind) else None)
        condition = _weather_condition_label(codes[idx] if idx < len(codes) else None)
        day_caption = translate(lang, 'weather_day_label', label)
        lines.append(f"{day_caption}: {condition}, High {temp_high}, Low {temp_low}, precip {precip_label}, wind {wind_label}")

    if len(lines) == 1:
        fallback = translate(lang, 'weather_service_fail', "⚠️ Weather service unavailable right now.")
        with WEATHER_CACHE_LOCK:
            WEATHER_CACHE['timestamp'] = now
            WEATHER_CACHE['text'] = fallback
        return fallback

    forecast_text = "\n".join(lines)
    with WEATHER_CACHE_LOCK:
        WEATHER_CACHE['timestamp'] = now
        WEATHER_CACHE['text'] = forecast_text
    return forecast_text


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
        sender_key = _safe_sender_key(node_id) or str(node_id)
        display_label = sender_key
        try:
            short_name = get_node_shortname(node_id)
            if short_name:
                display_label = short_name
        except Exception:
            pass
        timestamp_val = None
        if isinstance(tstamp, (int, float)):
            timestamp_val = float(tstamp)
        else:
            timestamp_val = _now()
        _update_location_history(
            sender_key,
            lat,
            lon,
            timestamp_val,
            precision,
            dilution,
            display_label=display_label,
        )





def _build_locations_kml() -> str:
    _snapshot_all_node_positions()
    points = _collect_recent_locations(exclude_key=None, limit=None)
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
    ]
    for entry in points:
        quality = entry.get("quality", "precise")
        style_url = "#precise" if quality != "dilute" else "#dilute"
        name = entry.get("key", "node")
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
    lines += [
        "  </Document>",
        "</kml>",
    ]
    return "\n".join(lines)

def _format_location_reply(sender_id: Any) -> Optional[str]:
    lat, lon, tstamp, precision, dilution = get_node_location(sender_id)
    if lat is None or lon is None:
        return None
    sender_key = _safe_sender_key(sender_id) or str(sender_id)
    display_name = sender_key or str(sender_id)
    try:
        short_name = get_node_shortname(sender_id)
        if short_name:
            display_name = short_name
    except Exception:
        pass
    try:
        lat_val = float(lat)
    except Exception:
        lat_val = lat
    try:
        lon_val = float(lon)
    except Exception:
        lon_val = lon
    timestamp_val = float(tstamp) if isinstance(tstamp, (int, float)) else _now()
    _update_location_history(
        sender_key,
        lat_val,
        lon_val,
        timestamp_val,
        precision,
        dilution,
        display_label=display_name,
    )
    _snapshot_all_node_positions()
    map_url = _generate_map_link(lat_val, lon_val, display_name)
    return f"📍 {display_name}: {map_url}"


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
                    if 'message' in m:
                        m['message'] = str(m.get('message') or '')
                    norm.append(m)
                with messages_lock:
                    messages.clear()
                    messages.extend(norm)
                    _prune_messages_locked()
                print(f"Loaded {len(messages)} messages from archive.")
        except Exception as e:
            print(f"⚠️ Could not load archive {ARCHIVE_FILE}: {e}")
    else:
        print("No archive found; starting fresh.")

def save_archive():
  try:
    with messages_lock:
      _prune_messages_locked()
      snapshot: List[Dict[str, Any]] = []
      for entry in messages:
        sanitized = dict(entry)
        if 'message' in sanitized:
          sanitized['message'] = ''
        snapshot.append(sanitized)
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

    stored_node_id = None
    if force_node is not None:
        stored_node_id = force_node
    else:
        if isinstance(node_id, int):
            stored_node_id = node_id
        elif isinstance(node_id, str) and node_id != "WebUI":
            stored_node_id = node_id
    display_id = _node_display_label(force_node if force_node is not None else node_id)

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
        _prune_messages_locked()
        if MAX_MESSAGE_LOG and MAX_MESSAGE_LOG > 0 and len(messages) > MAX_MESSAGE_LOG:
            # keep only the last MAX_MESSAGE_LOG entries
            del messages[:-MAX_MESSAGE_LOG]
    try:
        STATS.record_message(direct=bool(direct), is_ai=is_ai_msg)
    except Exception:
        pass
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as logf:
            logf.write(f"{timestamp} | {display_id} | EMERGENCY={is_emergency} | len={len(text or '')}\n")
        _trim_log_file(LOG_FILE)
    except Exception as e:
        print(f"⚠️ Could not write to {LOG_FILE}: {e}")
    save_archive()
    return entry

def split_message(text):
    """Split outgoing text into UTF-8 safe chunks within radio payload limits."""

    if not text:
        return []

    limit = max(int(MAX_CHUNK_SIZE), 1)
    chunks: List[str] = []
    current_chars: List[str] = []
    current_bytes = 0

    for char in text:
        encoded = char.encode("utf-8")
        byte_len = len(encoded)
        if byte_len > limit:
            # Replace oversized glyphs with a safe fallback. Keeps transmission within bounds.
            char = "?"
            encoded = char.encode("utf-8")
            byte_len = len(encoded)

        if current_chars and current_bytes + byte_len > limit:
            chunks.append("".join(current_chars))
            if len(chunks) >= MAX_CHUNKS:
                return chunks
            current_chars = []
            current_bytes = 0

        current_chars.append(char)
        current_bytes += byte_len

    if current_chars and len(chunks) < MAX_CHUNKS:
        chunks.append("".join(current_chars))

    return chunks[:MAX_CHUNKS]

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
    # Return basic info for potential resend scheduling
    return {"chunks": chunks, "sent": sent_any}


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
    ack_supported = hasattr(interface, "waitForAckNak")
    ack_results: List[bool] = []
    for idx, chunk in enumerate(chunks):
        max_retries = 3
        retry_delay = 2
        success = False

        for attempt in range(max_retries):
            try:
                if ephemeral_ok:
                    interface.sendDirectText(destinationId, chunk, wantAck=True)
                else:
                    interface.sendText(chunk, destinationId=destinationId, wantAck=True)
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

        if success and ack_supported and not DISABLE_ACK_WAIT:
            try:
                interface.waitForAckNak()
                clean_log(f"Ack received from {dest_display}", "✅", show_always=False)
                ack_results.append(True)
                if RESEND_TELEMETRY_ENABLED:
                    try:
                        STATS.record_ack_event(is_direct=True, attempt=1, success=True)
                    except Exception:
                        pass
            except Exception as ack_exc:
                clean_log(
                    f"Ack timeout for {dest_display}: {ack_exc}",
                    "⚠️",
                    show_always=False,
                )
                ack_results.append(False)
                if RESEND_TELEMETRY_ENABLED:
                    try:
                        STATS.record_ack_event(is_direct=True, attempt=1, success=False)
                    except Exception:
                        pass
        elif success and (not ack_supported or DISABLE_ACK_WAIT):
            ack_results.append(True)
            if RESEND_TELEMETRY_ENABLED:
                try:
                    STATS.record_ack_event(is_direct=True, attempt=1, success=True)
                except Exception:
                    pass

        if success and idx < len(chunks) - 1:
            time.sleep(delay)
    if sent_any:
        clean_log(f"Sent to {dest_display}", "📤")
    # For callers interested in granular ACK feedback
    return {"chunks": chunks, "acks": ack_results, "sent": sent_any}


def _ordinal(n: int) -> str:
    if 10 <= (n % 100) <= 20:
        suffix = 'th'
    else:
        suffix = {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th')
    return f"{n}{suffix}"


class ResendManager:
    def __init__(self):
        self.lock = threading.Lock()
        self.jobs: Dict[str, Dict[str, Any]] = {}
        self.by_sender: Dict[str, Set[str]] = {}

    def cancel_for_sender(self, sender_key: Optional[str]) -> None:
        if not sender_key:
            return
        with self.lock:
            job_ids = list(self.by_sender.get(sender_key, set()))
            for jid in job_ids:
                job = self.jobs.get(jid)
                if job is not None:
                    job['cancel'] = True
            self.by_sender.pop(sender_key, None)

    def _register(self, job: Dict[str, Any]) -> str:
        jid = job.get('id')
        if not jid:
            import uuid
            jid = uuid.uuid4().hex
            job['id'] = jid
        with self.lock:
            self.jobs[jid] = job
            sender_key = job.get('sender_key')
            if sender_key:
                self.by_sender.setdefault(sender_key, set()).add(jid)
        return jid

    def _finalize(self, jid: str) -> None:
        with self.lock:
            job = self.jobs.pop(jid, None)
            if job is None:
                return
            sk = job.get('sender_key')
            if sk and sk in self.by_sender:
                self.by_sender[sk].discard(jid)
                if not self.by_sender[sk]:
                    self.by_sender.pop(sk, None)

    def _under_usage_threshold(self) -> bool:
        try:
            usage = _calculate_network_usage_percent(StatsManager.WINDOW_SECONDS)
        except Exception:
            usage = None
        if usage is None:
            return True  # Fail open
        return float(usage) < float(RESEND_USAGE_THRESHOLD_PERCENT)

    def schedule_dm_resend(
        self,
        *,
        interface_ref,
        destination_id,
        text: str,
        chunks: List[str],
        acks: List[bool],
        attempts: int,
        interval_seconds: float,
        sender_key: Optional[str],
        is_user_dm: bool,
    ) -> None:
        if not RESEND_ENABLED:
            return
        if RESEND_DM_ONLY is True:
            # Allowed only for DMs; this function is for DMs, so OK
            pass
        # Determine initial unacked indices
        pending_idxs: Set[int] = set()
        for i, _ in enumerate(chunks):
            flag = True
            if i < len(acks):
                flag = bool(acks[i])
            if not flag:
                pending_idxs.add(i)
        if not pending_idxs:
            return
        # Guard rails
        attempts = max(1, int(attempts))
        interval_seconds = max(1.0, float(interval_seconds))

        job: Dict[str, Any] = {
            'sender_key': sender_key,
            'destination': destination_id,
            'cancel': False,
            'attempt': 2,  # next try label (2nd try)
            'max_attempts': attempts,
            'pending': pending_idxs,
            'text': text,
        }
        jid = self._register(job)

        def _worker():
            try:
                while True:
                    # Cancel checks
                    if job.get('cancel'):
                        break
                    if sender_key and (_is_user_blocked(sender_key) or _is_user_muted(sender_key)):
                        break
                    # Done?
                    if not job['pending']:
                        break
                    # Attempts exhausted?
                    if job['attempt'] > job['max_attempts']:
                        break
                    # Network usage gate
                    if not self._under_usage_threshold():
                        # Skip without consuming attempt number
                        time.sleep(interval_seconds)
                        continue
                    try:
                        # Build subset
                        subset = [chunks[i] for i in sorted(job['pending'])]
                        if not subset:
                            break
                        # Prepare suffix label for this attempt (optional)
                        label = f" ({_ordinal(job['attempt'])} try)" if RESEND_SUFFIX_ENABLED else ""
                        # Send subset one by one so we can attach suffix to the last
                        new_pending: Set[int] = set()
                        for j, idx in enumerate(sorted(job['pending'])):
                            part = chunks[idx]
                            is_last = (j == len(job['pending']) - 1)
                            if is_last:
                                # Try to append suffix within chunk size limit
                                candidate = part + label
                                if len(candidate.encode('utf-8')) <= int(MAX_CHUNK_SIZE):
                                    payloads = [candidate]
                                else:
                                    payloads = [part, label]
                            else:
                                payloads = [part]
                            # Send this part (or parts)
                            part_acked = True
                            for payload in payloads:
                                res = send_direct_chunks(interface_ref, payload, destination_id, chunk_delay=0)
                                sent_acks = res.get('acks') or []
                                # Consider ack True if every ack in this send succeeded; if no acks present, assume True
                                if sent_acks and not all(bool(x) for x in sent_acks):
                                    part_acked = False
                                # Telemetry (aggregate for this payload)
                                if RESEND_TELEMETRY_ENABLED:
                                    try:
                                        ok = (not sent_acks) or all(bool(x) for x in sent_acks)
                                        STATS.record_ack_event(is_direct=True, attempt=int(job['attempt']), success=bool(ok))
                                    except Exception:
                                        pass
                            if not part_acked:
                                new_pending.add(idx)
                        job['pending'] = new_pending
                    except Exception as exc:
                        clean_log(f"Resend error: {exc}", "⚠️", show_always=False)
                    # Early stop if everything acked
                    if not job['pending']:
                        break
                    # Prepare next try
                    job['attempt'] += 1
                    # Sleep with jitter
                    jitter = random.uniform(0, max(0.0, float(RESEND_JITTER_SECONDS)))
                    time.sleep(interval_seconds + jitter)
            finally:
                self._finalize(jid)

        threading.Thread(target=_worker, daemon=True).start()

    def schedule_broadcast_resend(
        self,
        *,
        interface_ref,
        channel_idx: int,
        text: str,
        attempts: int,
        interval_seconds: float,
    ) -> None:
        if not RESEND_ENABLED or not RESEND_BROADCAST_ENABLED:
            return
        if RESEND_DM_ONLY:
            return
        attempts = max(1, int(attempts))
        interval_seconds = max(1.0, float(interval_seconds))
        job: Dict[str, Any] = {
            'sender_key': None,
            'cancel': False,
            'attempt': 2,
            'max_attempts': attempts,
        }
        jid = self._register(job)

        def _worker():
            try:
                attempt_num = 2
                while attempt_num <= attempts:
                    if job.get('cancel'):
                        break
                    if not self._under_usage_threshold():
                        time.sleep(interval_seconds)
                        continue
                    # Append attempt label to last chunk or as separate tiny chunk
                    suffix = f" ({_ordinal(attempt_num)} try)" if RESEND_SUFFIX_ENABLED else ""
                    chunks = split_message(text)
                    if chunks:
                        last = chunks[-1]
                        candidate = last + suffix
                        if len(candidate.encode('utf-8')) <= int(MAX_CHUNK_SIZE):
                            chunks[-1] = candidate
                            payloads = chunks
                        else:
                            payloads = chunks + [suffix]
                    else:
                        payloads = [suffix]
                    try:
                        for part in payloads:
                            send_broadcast_chunks(interface_ref, part, channel_idx, chunk_delay=0)
                    except Exception as exc:
                        clean_log(f"Broadcast resend error: {exc}", "⚠️", show_always=False)
                    attempt_num += 1
                    jitter = random.uniform(0, max(0.0, float(RESEND_JITTER_SECONDS)))
                    time.sleep(interval_seconds + jitter)
            finally:
                self._finalize(jid)

        threading.Thread(target=_worker, daemon=True).start()


RESEND_MANAGER = ResendManager()

ALARM_TIMER_MANAGER: Optional[AlarmTimerManager] = None

def _schedule_follow_up_message(
    interface_ref,
    text: str,
    *,
    delay: Optional[float],
    is_direct: bool,
    sender_node,
    channel_idx,
):
    """Send a follow-up message after a delay without blocking the response worker."""

    if not text or interface_ref is None:
        return

    try:
        wait_seconds = float(delay if delay is not None else MAIL_FOLLOW_UP_DELAY)
    except Exception:
        wait_seconds = MAIL_FOLLOW_UP_DELAY
    wait_seconds = max(0.0, wait_seconds)

    def _worker():
        try:
            if wait_seconds:
                time.sleep(wait_seconds)
            # Skip if user muted/blocked
            if is_direct:
                try:
                    s_key = _safe_sender_key(sender_node)
                except Exception:
                    s_key = None
                if s_key and (_is_user_blocked(s_key) or _is_user_muted(s_key)):
                    return
            if is_direct:
                send_direct_chunks(interface_ref, text, sender_node)
            else:
                send_broadcast_chunks(interface_ref, text, channel_idx)
        except Exception as exc:
            clean_log(f"Follow-up send failed: {exc}", "⚠️", show_always=False)

    threading.Thread(target=_worker, daemon=True).start()


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
    is_admin = bool(sender_key and sender_key in AUTHORIZED_ADMINS)
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
        return MAIL_MANAGER.handle_wipe(mailbox, actor_key=sender_key, is_admin=is_admin)

    if action == "multi_mailbox":
        selected = state.get("mailboxes") or []
        if not selected:
            return PendingReply("Mailbox list expired. Run `/wipe mailbox` again.", "/wipe confirm")
        responses: List[str] = []
        for name in selected:
            if not name:
                continue
            reply = MAIL_MANAGER.handle_wipe(str(name), actor_key=sender_key, is_admin=is_admin)
            if isinstance(reply, PendingReply) and reply.text:
                responses.append(reply.text)
        if not responses:
            responses.append("🧹 No inboxes were cleared.")
        return PendingReply("\n".join(responses), "/wipe confirm")

    if action == "chathistory":
        if not is_direct:
            return PendingReply("❌ Chat history wipe only works in direct messages.", "/wipe confirm")
        removed = _clear_direct_history(sender_id)
        if sender_key:
            _clear_context_session(sender_key)
        clean_log(f"Chat history wipe completed for {get_node_shortname(sender_id)}", "🧹")
        if removed > 0:
            return PendingReply(f"🧹 Cleared {removed} messages from our DM history. Context reset — fresh slate!", "/wipe confirm")
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
        mail_reply = MAIL_MANAGER.handle_wipe(mailbox, actor_key=sender_key, is_admin=is_admin)
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

def build_ollama_history(sender_id=None, is_direct=False, channel_idx=None, thread_root_ts=None, max_chars=OLLAMA_CONTEXT_CHARS) -> Tuple[str, bool]:
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
      return "", False
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
    truncated = False
    if len(history) > max_chars:
      truncated = True
      history = history[-max_chars:]
    return history, truncated
  except Exception as e:
    dprint(f"build_ollama_history error: {e}")
    return "", False


def send_to_ollama(
    user_message,
    sender_id=None,
    is_direct=False,
    channel_idx=None,
    thread_root_ts=None,
    system_prompt: Optional[str] = None,
    *,
    use_history: bool = True,
    extra_context: Optional[str] = None,
    allow_streaming: bool = True,
): 
    dprint(f"send_to_ollama: user_message='{user_message}' sender_id={sender_id} is_direct={is_direct} channel={channel_idx}")
    if not is_ai_enabled():
        ai_log("Blocked: AI responses disabled", "ollama")
        return AI_DISABLED_MESSAGE
    ai_log("Processing message...", "ollama")
    try:
        STATS.record_ai_request()
    except Exception:
        pass

    # Normalize text for non-ASCII characters using unidecode
    user_message = unidecode(user_message)
    # Use an empty system prompt unless explicitly configured
    effective_system_prompt = _sanitize_prompt_text(system_prompt) or _sanitize_prompt_text(SYSTEM_PROMPT) or ""

    extra_block = extra_context.strip() if extra_context else ""
    history_limit = OLLAMA_CONTEXT_CHARS
    if extra_block:
        reserve_chars = max(500, OLLAMA_CONTEXT_CHARS // 3)
        history_limit = max(reserve_chars, OLLAMA_CONTEXT_CHARS - len(extra_block))

    # Build optional conversation history
    history = ""
    history_truncated = False
    if use_history and sender_id is not None:
        try:
            history, history_truncated = build_ollama_history(
                sender_id=sender_id,
                is_direct=is_direct,
                channel_idx=channel_idx,
                thread_root_ts=thread_root_ts,
                max_chars=history_limit,
            )
        except Exception as e:
            dprint(f"Warning: failed building history for Ollama: {e}")
            history, history_truncated = "", False

    if history_truncated and is_direct and sender_id is not None:
        sender_key = _safe_sender_key(sender_id)
        if sender_key:
            with CONTEXT_TRUNCATION_LOCK:
                CONTEXT_TRUNCATED_SENDERS.add(sender_key)

    # Compose final prompt: system prompt, optional context, then user message
    combined_history = history.strip() if history else ""
    if extra_block:
        if combined_history:
            combined_history = f"{extra_block}\n\n{combined_history}"
        else:
            combined_history = extra_block
    if combined_history:
        if effective_system_prompt:
            combined_prompt = f"{effective_system_prompt}\nCONTEXT:\n{combined_history}\n\nUSER: {user_message}\nASSISTANT:"
        else:
            combined_prompt = f"CONTEXT:\n{combined_history}\n\nUSER: {user_message}\nASSISTANT:"
    else:
        if effective_system_prompt:
            combined_prompt = f"{effective_system_prompt}\nUSER: {user_message}\nASSISTANT:"
        else:
            combined_prompt = f"USER: {user_message}\nASSISTANT:"
    if DEBUG_ENABLED:
        dprint(f"Ollama combined prompt:\n{combined_prompt}")
    else:
        clean_log(f"Prompt prepared (len={len(user_message)})", "💭")

    stream_flag = bool(OLLAMA_STREAM and allow_streaming)

    payload = {
        "prompt": combined_prompt,
        "model": OLLAMA_MODEL,
        "stream": stream_flag,
        "options": {
            # Ask Ollama to allocate a larger context window if the model supports it
            "num_ctx": OLLAMA_NUM_CTX,
            # Performance optimizations for faster responses
            "num_predict": 200,    # Limit response length for mesh network
            "temperature": 0.7,    # Slightly less random for more focused responses
            "top_p": 0.9,         # Nucleus sampling for quality vs speed balance
            "top_k": 40,          # Limit vocabulary consideration for speed
            "repeat_penalty": 1.1, # Prevent repetition
            "num_thread": OLLAMA_NUM_THREAD,
        },
    }

    use_streaming = stream_flag

    def _send_stream_chunk(chunk_text: str) -> bool:
        if not chunk_text or not chunk_text.strip():
            return False
        if interface is None:
            return False
        try:
            if is_direct and sender_id is not None:
                result = send_direct_chunks(interface, chunk_text, sender_id, chunk_delay=0)
                # Schedule partial resend for streamed chunk if needed
                if RESEND_ENABLED and is_direct:
                    chunks = result.get('chunks') or []
                    acks = result.get('acks') or []
                    # streamed chunk should map to 1 chunk typically; schedule if unacked
                    if (not acks) or (not all(bool(x) for x in acks)):
                        try:
                            skey = _safe_sender_key(sender_id)
                        except Exception:
                            skey = None
                        RESEND_MANAGER.schedule_dm_resend(
                            interface_ref=interface,
                            destination_id=sender_id,
                            text=chunk_text,
                            chunks=chunks if chunks else [chunk_text],
                            acks=acks if acks else [False],
                            attempts=RESEND_USER_ATTEMPTS,
                            interval_seconds=RESEND_USER_INTERVAL,
                            sender_key=skey,
                            is_user_dm=True,
                        )
                return True
            if not is_direct and channel_idx is not None:
                send_broadcast_chunks(interface, chunk_text, channel_idx, chunk_delay=0)
                if RESEND_ENABLED and RESEND_BROADCAST_ENABLED and not RESEND_DM_ONLY:
                    RESEND_MANAGER.schedule_broadcast_resend(
                        interface_ref=interface,
                        channel_idx=channel_idx,
                        text=chunk_text,
                        attempts=RESEND_SYSTEM_ATTEMPTS,
                        interval_seconds=RESEND_SYSTEM_INTERVAL,
                    )
                return True
        except Exception as exc:
            clean_log(f"Streaming send failed: {exc}", "⚠️", show_always=False)
        return False

    def _consume_stream(response):
        nonlocal use_streaming
        total_parts: List[str] = []
        total_len = 0
        buffer = ""
        streamed_chunks = 0
        truncated = False
        stop_stream = False

        def flush(force: bool = False):
            nonlocal buffer, streamed_chunks
            if not use_streaming:
                return
            while buffer and streamed_chunks < MAX_CHUNKS:
                if not force and len(buffer) < MAX_CHUNK_SIZE:
                    break
                chunk_len = min(len(buffer), MAX_CHUNK_SIZE)
                if not force and chunk_len < MAX_CHUNK_SIZE:
                    break
                chunk = buffer[:chunk_len]
                if _send_stream_chunk(chunk):
                    buffer = buffer[chunk_len:]
                    streamed_chunks += 1
                else:
                    # Leave buffer intact so the normal chunk sender can handle it later
                    break

        try:
            for raw_line in response.iter_lines(decode_unicode=True):
                if stop_stream:
                    break
                if not raw_line:
                    continue
                try:
                    piece = json.loads(raw_line)
                except json.JSONDecodeError:
                    continue

                if piece.get("error"):
                    message = _format_ai_error("Ollama", piece["error"])
                    response.close()
                    return StreamingResult(message)

                fragment = piece.get("response") or ""
                if fragment:
                    allowed = MAX_RESPONSE_LENGTH - total_len
                    if allowed <= 0:
                        truncated = True
                        stop_stream = True
                    else:
                        snippet = fragment[:allowed]
                        total_parts.append(snippet)
                        total_len += len(snippet)
                        buffer += snippet
                        if len(fragment) > allowed:
                            truncated = True
                            stop_stream = True
                        flush()

                if piece.get("done"):
                    break

        finally:
            try:
                response.close()
            except Exception:
                pass

        flush(force=True)
        full_text = "".join(total_parts)
        if not full_text:
            full_text = _format_ai_error("Ollama", "no content returned")
        ai_log(f"Response ready (len={len(full_text)})", "ollama")
        return StreamingResult(full_text[:MAX_RESPONSE_LENGTH], sent_chunks=streamed_chunks, truncated=truncated)

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
                r = requests.post(
                    OLLAMA_URL,
                    json=payload,
                    timeout=OLLAMA_TIMEOUT,
                    stream=use_streaming,
                )
                break
            except Exception as e:
                if attempts >= 2:
                    raise
                time.sleep(backoff)
                backoff *= 1.7
        if r is not None and r.status_code == 200:
            if use_streaming:
                return _consume_stream(r)

            jr = r.json()
            dprint(f"Ollama raw => {jr}")
            # Extract clean response for logging
            resp = jr.get("response")
            if resp:
                ai_log(f"Response ready (len={len(resp)})", "ollama")
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
  if not is_ai_enabled():
    return AI_DISABLED_MESSAGE
  system_prompt = build_system_prompt_for_sender(sender_id)
  extra_context = None
  use_history = True
  session_notice = None
  sender_key = _safe_sender_key(sender_id) if sender_id is not None else None
  if sender_key:
    session, session_notice = _get_context_session(sender_key)
  if session:
    extra_context = session.get('context')
    use_history = session.get('use_history', True)
    override_prompt = session.get('system_prompt_override')
    if override_prompt:
      system_prompt = override_prompt
    elif session.get('prompt_addendum'):
      system_prompt = f"{system_prompt}\n\n{session['prompt_addendum']}"
  provider = AI_PROVIDER
  if provider == "home_assistant":
    return send_to_home_assistant(prompt)

  if provider not in {"ollama", "home_assistant"}:
    print(f"⚠️ Unknown AI provider '{provider}', defaulting to Ollama.")

  _log_high_cost(sender_id, "ollama", prompt[:80])
  response = send_to_ollama(
      prompt,
      sender_id=sender_id,
      is_direct=is_direct,
      channel_idx=channel_idx,
      thread_root_ts=thread_root_ts,
      system_prompt=system_prompt,
      use_history=use_history,
      extra_context=extra_context,
      allow_streaming=(session_notice is None),
  )
  if session_notice:
    if isinstance(response, PendingReply):
      response.text = f"{session_notice}\n\n{response.text}" if response.text else session_notice
      return response
    if isinstance(response, str):
      return f"{session_notice}\n\n{response}" if response else session_notice
  return response
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
  if not is_ai_enabled():
    return AI_DISABLED_MESSAGE
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
  if not is_command_enabled(cmd):
    clean_log(f"Command {cmd} blocked (disabled)", "⛔", show_always=True, rate_limit=False)
    return _cmd_reply(cmd, f"⚠️ {cmd} is currently disabled by the operator.")
  sender_key = _safe_sender_key(sender_id)
  lang = _resolve_user_language(language_hint, sender_key)
  if cmd != "/wipe" and sender_key:
    PENDING_WIPE_REQUESTS.pop(sender_key, None)
  if cmd == "/about":
    return _cmd_reply(cmd, "MESH-MASTER Off Grid Chat Bot - By: MR-TBOT.com")

  elif cmd in ["/ai", "/bot", "/data"]:
    user_prompt = full_text[len(cmd):].strip()
    
    # Special handling for DMs: if the command has no content, treat the whole message as a regular AI query
    if is_direct and not user_prompt:
      # User just typed "/ai" or "/bot" alone in a DM - treat it as "ai" (regular message)
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
      return _cmd_reply(cmd, "🤖 Sorry, I have no GPS fix for your node. Check your Meshtastic app settings to make sure GPS is enabled.")
    else:
      if not location_text:
        return _cmd_reply(cmd, "🤖 Sorry, I have no GPS fix for your node. Check your Meshtastic app settings to make sure GPS is enabled.")
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

  elif cmd == "/onboard":
    if not is_direct:
      return _cmd_reply(cmd, "❌ This command can only be used in a direct message.")
    if not sender_key:
      return _cmd_reply(cmd, "⚠️ I couldn't identify your DM session. Try again in a moment.")
    remainder = full_text[len(cmd):].strip()
    restart = remainder.lower() in {"restart", "reset", "start", "again"}
    try:
      sender_short = get_node_shortname(sender_id)
    except Exception:
      sender_short = str(sender_id)
    return ONBOARDING_MANAGER.start_session(
      sender_key=sender_key,
      sender_id=sender_id,
      sender_short=sender_short,
      language_hint=lang,
      restart=restart,
    )

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
    sender_key = _safe_sender_key(sender_id)
    if not sender_key:
      return _cmd_reply(cmd, "⚠️ I couldn't identify your DM session. Try again in a moment.")
    remainder = full_text[len(cmd):].strip()
    if not remainder:
      mailboxes = MAIL_MANAGER.mailboxes_for_user(sender_key)
      if not mailboxes:
        PENDING_MAILBOX_SELECTIONS.pop(sender_key, None)
        message = (
          "📭 You're not linked to any inboxes yet. Start with `/mail <mailbox> <message>` to create one."
        )
        return _cmd_reply(cmd, message)
      PENDING_MAILBOX_SELECTIONS.pop(sender_key, None)
      PENDING_MAILBOX_SELECTIONS[sender_key] = {
        'mailboxes': mailboxes,
        'language': lang,
      }
      comma_list = ", ".join(mailboxes)
      numbered = [f"{idx}) {name}" for idx, name in enumerate(mailboxes, 1)]
      lines = [
        f"📬 Linked inboxes: {comma_list}",
        "Reply with a number to open it, or type the inbox name.",
        "If the inbox has a PIN, add it after the name before sending.",
        *numbered,
        "Need a new inbox? Run `/mail <mailbox> <message>`.",
      ]
      return PendingReply("\n".join(lines), "/c select")
    parts = remainder.split(None, 1)
    if not parts:
      return _cmd_reply(cmd, "Use this by typing: /c mailbox [question]")
    mailbox = parts[0].strip().strip("'\"")
    rest = parts[1].strip() if len(parts) > 1 else ""
    if not rest:
      try:
        if mailbox and not MAIL_MANAGER.store.mailbox_exists(mailbox):
          inline_match = re.match(r"^(?P<box>[A-Za-z0-9_\-]+?)(?P<pin>\d{4,8})$", mailbox)
          if inline_match:
            candidate_box = inline_match.group('box')
            if MAIL_MANAGER.store.mailbox_exists(candidate_box):
              mailbox = candidate_box
              rest = inline_match.group('pin')
      except Exception:
        pass
    if not mailbox:
      return _cmd_reply(cmd, "Mailbox name cannot be empty.")
    try:
      sender_short = get_node_shortname(sender_id)
    except Exception:
      sender_short = str(sender_id)
    PENDING_MAILBOX_SELECTIONS.pop(sender_key, None)
    reply = MAIL_MANAGER.handle_check(sender_key, sender_id, sender_short, mailbox, rest)
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
        if not sender_key:
          return _cmd_reply(cmd, "⚠️ I couldn't identify your DM session. Try again in a moment.")
        mailboxes = MAIL_MANAGER.mailboxes_for_user(sender_key)
        if not mailboxes:
          return PendingReply(
            "📪 I don't see any inboxes linked to you yet. Create one with `/mail <mailbox> <message>` first.",
            "/wipe command",
          )
        unique_mailboxes = []
        seen_mailboxes: Set[str] = set()
        for name in mailboxes:
          normalized = MAIL_MANAGER.store.normalize_mailbox(name)
          if normalized in seen_mailboxes:
            continue
          seen_mailboxes.add(normalized)
          unique_mailboxes.append(name)
        try:
          actor = get_node_shortname(sender_id)
        except Exception:
          actor = str(sender_id)
        PENDING_WIPE_SELECTIONS.pop(sender_key, None)
        PENDING_WIPE_REQUESTS.pop(sender_key, None)
        PENDING_WIPE_SELECTIONS[sender_key] = {
          "mailboxes": unique_mailboxes,
          "language": lang,
          "allow_all": True,
          "actor": actor,
        }
        lines = ["📬 Linked inboxes:"]
        lines.extend(f"{idx}) {name}" for idx, name in enumerate(unique_mailboxes, 1))
        lines.append(f"{len(unique_mailboxes) + 1}) Wipe ALL listed inboxes")
        lines.append("0) Cancel")
        lines.append("ℹ️ Only the mailbox owner can authorize a wipe.")
        lines.append(
          f"Reply with 1-{len(unique_mailboxes)} for a single inbox, {len(unique_mailboxes) + 1} to clear them all, or 0 to cancel."
        )
        return PendingReply("\n".join(lines), "/wipe select")
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
      "📬 Mesh Mail Quickstart (DM only):\n"
      "- Create the inbox with your first note: `/mail <mailbox> hey team`.\n"
      "- Teammates drop updates the same way: `/mail <mailbox> their update`.\n"
      "- Read the latest mail anytime: `/checkmail <mailbox>` (alias `/c <mailbox>`).\n"
      "- Set a PIN during creation? Include it when reading: `/checkmail <mailbox> PIN=1234`.\n"
      "- Only the owner can clear it with `/wipe mailbox <mailbox>` after the Y/N confirm."
    )
    return _cmd_reply(cmd, guide)

  elif cmd == "/save":
    if not is_direct:
      return _cmd_reply(cmd, translate(lang, 'dm_only', "❌ This command can only be used in a direct message."))
    sender_key = _safe_sender_key(sender_id)
    if not sender_key:
      return _cmd_reply(cmd, "⚠️ I couldn't identify your DM session. Try again in a moment.")
    topic = full_text[len(cmd):].strip()
    if topic.startswith(('"', "'")) and topic.endswith(('"', "'")) and len(topic) >= 2:
      topic = topic[1:-1].strip()
    if topic:
      return _save_conversation_for_user(sender_id, sender_key, topic, lang, auto_title=False)
    PENDING_SAVE_WIZARDS[sender_key] = {
      'created': _now(),
      'sender_id': sender_id,
      'language': lang,
    }
    return PendingReply(
      "📝 Name this conversation first. Reply with a short title (or N to cancel).",
      "/save wizard",
    )

  elif cmd == "/chathistory":
    if not is_direct:
      return _cmd_reply(cmd, translate(lang, 'dm_only', "❌ This command can only be used in a direct message."))
    sender_key = _safe_sender_key(sender_id)
    if not sender_key:
      return _cmd_reply(cmd, "⚠️ I couldn't identify your DM session. Try again in a moment.")
    return _start_chathistory_menu(sender_key)

  elif cmd == "/recall":
    if not is_direct:
      return _cmd_reply(cmd, translate(lang, 'dm_only', "❌ This command can only be used in a direct message."))
    sender_key = _safe_sender_key(sender_id)
    if not sender_key:
      return _cmd_reply(cmd, "⚠️ I couldn't identify your DM session. Try again in a moment.")
    contexts = _get_saved_contexts_for_user(sender_key)
    if not contexts:
      return _cmd_reply(cmd, "ℹ️ You haven't saved any conversations yet. Use `/save` first.")
    topic = full_text[len(cmd):].strip()
    if not topic:
      return _start_chathistory_menu(sender_key)
    matches = _find_saved_context_matches(sender_key, topic, limit=5)
    if not matches or matches[0][1] < 0.35:
      return _cmd_reply(cmd, "⚠️ I couldn't find a saved conversation matching that. Try a different description or `/recall` to list entries.")
    top_entry, top_score = matches[0]
    if top_score >= 0.85 or len(matches) == 1:
      return _activate_saved_context_session(sender_id, sender_key, top_entry)
    return _start_recall_selection(sender_key, matches)

  elif cmd == "/stop":
    if not is_direct:
      return _cmd_reply(cmd, translate(lang, 'dm_only', "❌ This command can only be used in a direct message."))
    sender_key = _safe_sender_key(sender_id)
    if not sender_key:
      return _cmd_reply(cmd, "⚠️ I couldn't identify your DM session. Try again in a moment.")
    stopped = _stop_bible_autoscroll(sender_key)
    if stopped:
      return PendingReply(translate(lang, 'bible_autoscroll_stop', "⏹️ Auto-scroll paused. Reply 22 later to resume."), "/stop command")
    with CONTEXT_SESSION_LOCK:
      session = CONTEXT_SESSIONS.get(sender_key)
    if session:
      title = session.get('title') or "context window"
      return PendingReply(f"ℹ️ Auto-scroll wasn't running. '{title}' is still active — use /exit to leave it.", "/stop command")
    return PendingReply("ℹ️ There wasn't an active auto-scroll session.", "/stop command")

  elif cmd == "/exit":
    if not is_direct:
      return _cmd_reply(cmd, translate(lang, 'dm_only', "❌ This command can only be used in a direct message."))
    sender_key = _safe_sender_key(sender_id)
    return _handle_exit_session(sender_key)

  elif cmd == "/meshtastic":
    if not is_ai_enabled():
      return _cmd_reply(cmd, AI_DISABLED_MESSAGE)
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
    _log_high_cost(sender_id, "meshtastic", query[:80])
    response = send_to_ollama(
      payload,
      sender_id=sender_id,
      is_direct=is_direct,
      channel_idx=channel_idx,
      thread_root_ts=thread_root_ts,
      system_prompt=system_prompt,
      use_history=False,
      extra_context=numbered_context,
    )
    if not response:
      return _cmd_reply(cmd, "MeshTastic lookup failed. Try later.")
    sender_key = _safe_sender_key(sender_id)
    if sender_key:
      session_payload = {
        'type': 'meshtastic',
        'title': 'MeshTastic knowledge session',
        'summary': top_titles,
        'context': numbered_context,
        'system_prompt_override': MESHTASTIC_KB_SYSTEM_PROMPT,
        'use_history': True,
      }
      _set_context_session(sender_key, session_payload)
    preface = "⚙️ MeshTastic reference mode engaged. This will be a slow conversation—type /exit to leave this context window."
    combined = f"{preface}\n\n{response}" if response else preface
    return PendingReply(combined, "/meshtastic context")

  elif cmd == "/biblehelp":
    default_help = (
      "📖 Bible quick tips: `/bible` keeps your place. Jump with `/bible John 3:16`. "
      "Add `in Spanish` or `en ingles` to switch languages. Turn pages with `<1,2>`. Reply `22` "
      "in a DM to auto-scroll 30 verses (12s each)."
    )
    help_text = translate(lang, 'bible_help', default_help)
    return _cmd_reply(cmd, help_text)

  elif cmd == "/vibe":
    sender_key = _safe_sender_key(sender_id)
    if not is_direct:
      summary = _format_personality_summary(sender_key)
      return _cmd_reply(cmd, f"{summary}\n\nDM me with `/vibe` to change vibes.")
    if not sender_key:
      return _cmd_reply(cmd, "⚠️ I couldn't identify your DM session. Try again in a moment.")

    remainder = full_text[len(cmd):].strip()
    if not remainder:
      return _start_vibe_menu(sender_key)

    parts = remainder.split(None, 1)
    action = parts[0].lower()
    arg = parts[1].strip() if len(parts) > 1 else ""

    if action in {"status", "show", "current"}:
      return _cmd_reply(cmd, _format_personality_summary(sender_key))

    if action in {"prompt", "custom", "append"}:
      return _cmd_reply(cmd, "🔒 Custom vibe prompts are no longer editable. Pick a vibe or use `/vibe status`.")

    if action in {"reset", "default", "restore"}:
      _reset_user_personality(sender_key)
      return _cmd_reply(cmd, _format_personality_summary(sender_key))

    if action in {"set", "use", "choose"}:
      if not arg:
        return _cmd_reply(cmd, "Use this by typing: /vibe set <name>")
      persona_id = _canonical_personality_id(arg)
      if not persona_id:
        suggestions = difflib.get_close_matches(arg.lower(), list(AI_PERSONALITY_LOOKUP.keys()), n=1, cutoff=0.6)
        if suggestions:
          hinted = AI_PERSONALITY_LOOKUP.get(suggestions[0], suggestions[0])
          return _cmd_reply(cmd, f"❔ Unknown vibe '{arg}'. Did you mean '{hinted}'?")
        return _cmd_reply(cmd, f"❔ Unknown vibe '{arg}'. Send `/vibe` to browse the list.")
      if not _set_user_personality(sender_key, persona_id):
        return _cmd_reply(cmd, "⚠️ Couldn't apply that vibe. Try again in a moment.")
      persona = AI_PERSONALITY_MAP.get(persona_id, {})
      emoji = persona.get("emoji") or "🧠"
      name = persona.get("name") or persona_id
      description = persona.get("description") or ""
      lines = [f"{emoji} Vibe set to {name} ({persona_id})."]
      if description:
        lines.append(description)
      return _cmd_reply(cmd, "\n".join(lines))

    return _cmd_reply(cmd, "Send `/vibe` to browse vibes, then reply with a number to switch. Extras: /vibe status, /vibe set <name>, /vibe reset.")

  elif cmd in {"/aivibe", "/changevibe", "/choosevibe", "/aipersonality"}:
    summary = _format_personality_summary(_safe_sender_key(sender_id))
    return _cmd_reply(cmd, f"{summary}\n\nCommand renamed. Use `/vibe` for vibe controls.")

  elif cmd == "/help":
    built_in = [
      "/about", "/menu", "/mail", "/checkmail", "/emailhelp", "/wipe",
      "/test",
      "/motd", "/meshinfo", "/bible", "/biblehelp", "/web", "/drudge", "/chucknorris", "/elpaso", "/blond", "/yomomma",
      "/games", "/blackjack", "/yahtzee", "/hangman", "/wordle", "/wordladder", "/adventure", "/rps", "/coinflip", "/cipher", "/quizbattle", "/morse",
      "/vibe", "/chathistory", "/changemotd", "/changeprompt", "/showprompt", "/printprompt", "/reset"
    ]
    custom_cmds = [c.get("command") for c in commands_config.get("commands", [])]
    help_text = "Commands:\n" + ", ".join(built_in + custom_cmds)
    help_text += "\nNote: /vibe, /chathistory, /changeprompt, /changemotd, /showprompt, and /printprompt are DM-only."
    help_text += "\nBrowse highlights with /menu."
    return _cmd_reply(cmd, help_text)


  elif cmd == "/menu":
    menu_text = format_structured_menu("menu", lang)
    return _cmd_reply(cmd, menu_text)

  elif cmd == "/weather":
    try:
      forecast = _fetch_weather_forecast(lang)
    except Exception as exc:
      return _cmd_reply(cmd, translate(lang, 'weather_service_fail', "⚠️ Weather service unavailable right now."))
    return _cmd_reply(cmd, forecast)

  elif cmd == "/web":
    remainder = full_text[len(cmd):].strip()
    sender_key = _safe_sender_key(sender_id)

    def _web_help_message() -> str:
      lines = [
        "🌐 Web tools ready:",
        "• `/web ddg <keywords>` — DuckDuckGo quick search (default).",
        "• `/web <keywords>` — same as ddg shorthand.",
        "• `/web crawl <site> [pages]` — light crawler for contact details.",
        "• `/web https://site` — single page preview.",
        "Use `/web clear` to drop saved results. Follow up with questions or `/reset` to wipe context.",
      ]
      return "\n".join(lines)

    if not remainder or remainder.lower() in {"help", "?", "info"}:
      return _cmd_reply(cmd, _web_help_message())

    if remainder.lower() in {"clear", "reset"}:
      if sender_key:
        _clear_web_context(sender_key)
      return _cmd_reply(cmd, "🧹 Cleared saved web findings.")

    parts = remainder.split(None, 1)
    directive = parts[0].lower()
    argument = parts[1].strip() if len(parts) > 1 else ""

    def _record_web_context(formatted: str, context_text: Optional[str] = None) -> None:
      if sender_key:
        _store_web_context(sender_key, formatted, context=context_text)

    if directive in {"ddg", "duckduckgo", "search"}:
      query = argument or ""
      if not query:
        return _cmd_reply(cmd, "Use this by typing: /web ddg <keywords>")
      try:
        results = _web_search_duckduckgo(query)
      except RuntimeError as exc:
        message = str(exc)
        if message == "offline":
          offline_notice = translate(lang, "web_offline", "🌐 Offline mode only. Web search unavailable.")
          return _cmd_reply(cmd, offline_notice)
        return _cmd_reply(cmd, f"⚠️ Web search failed: {message}")
      formatted = _format_web_results(query, results)
      context_block = _context_from_results(query, results)
      _record_web_context(formatted, context_block)
      try:
        clean_log(f"Web search '{query}' → {len(results)} result(s)", "🌐", show_always=False)
      except Exception:
        pass
      return _cmd_reply(cmd, formatted)

    if directive in {"crawl", "spider"}:
      if not argument:
        return _cmd_reply(cmd, "Use this by typing: /web crawl <site> [pages]")
      crawl_tokens = argument.split()
      target = crawl_tokens[0]
      page_limit: Optional[int] = None
      if len(crawl_tokens) > 1:
        try:
          page_limit = max(1, int(crawl_tokens[1]))
        except ValueError:
          target = argument  # treat entire argument as part of the URL if second token not numeric
          page_limit = None
      start_url = _extract_direct_url(target) or target
      try:
        pages, contacts, contact_page = _crawl_website(start_url, max_pages=page_limit or WEB_CRAWL_MAX_PAGES)
      except CrawlStartError as exc:
        detail = exc.detail or "crawl failed"
        status_note = f" (HTTP {exc.status_code})" if exc.status_code else ""
        return _cmd_reply(cmd, f"⚠️ Couldn't start crawl of {exc.url}{status_note}: {detail}")
      except RuntimeError as exc:
        message = str(exc)
        if message == "offline":
          offline_notice = translate(lang, "web_offline", "🌐 Offline mode only. Web search unavailable.")
          return _cmd_reply(cmd, offline_notice)
        return _cmd_reply(cmd, f"⚠️ Crawl failed: {message}")
      formatted = _format_crawl_results(start_url, pages, contacts, contact_page)
      context_block = _context_from_crawl(start_url, pages, contacts, contact_page)
      _record_web_context(formatted, context_block)
      try:
        clean_log(f"Web crawl '{start_url}' → {len(pages)} page(s)", "🕸️", show_always=False)
      except Exception:
        pass
      return _cmd_reply(cmd, formatted)

    direct_url = _extract_direct_url(remainder)
    if direct_url:
      try:
        preview = _fetch_url_preview(direct_url)
      except RuntimeError as exc:
        message = str(exc)
        if message == "offline":
          offline_notice = translate(lang, "web_offline", "🌐 Offline mode only. Web search unavailable.")
          return _cmd_reply(cmd, offline_notice)
        return _cmd_reply(cmd, f"⚠️ Fetch failed: {message}")
      formatted = _format_web_results(direct_url, preview)
      context_block = _context_from_results(direct_url, preview)
      _record_web_context(formatted, context_block)
      try:
        clean_log(f"Web preview '{direct_url}'", "🌐", show_always=False)
      except Exception:
        pass
      return _cmd_reply(cmd, formatted)

    # Default: treat remainder as DuckDuckGo search query.
    query = remainder
    try:
      results = _web_search_duckduckgo(query)
    except RuntimeError as exc:
      message = str(exc)
      if message == "offline":
        offline_notice = translate(lang, "web_offline", "🌐 Offline mode only. Web search unavailable.")
        return _cmd_reply(cmd, offline_notice)
      return _cmd_reply(cmd, f"⚠️ Web search failed: {message}")
    formatted = _format_web_results(query, results)
    context_block = _context_from_results(query, results)
    _record_web_context(formatted, context_block)
    try:
      clean_log(f"Web search '{query}' → {len(results)} result(s)", "🌐", show_always=False)
    except Exception:
      pass
    return _cmd_reply(cmd, formatted)

  elif cmd == "/drudge":
    # Fetch current Drudge Report headlines. Not DM-only.
    try:
      items = _fetch_drudge_headlines(max_items=5)
    except RuntimeError as exc:
      message = str(exc)
      if message == "offline":
        return _cmd_reply(cmd, translate(lang, "web_offline", "🌐 Offline mode only. Drudge headlines unavailable."))
      return _cmd_reply(cmd, f"⚠️ Drudge fetch failed: {message}")
    if not items:
      return _cmd_reply(cmd, "🗞️ No headlines found.")

    lines = ["🗞️ Drudge Report headlines:"]
    context_lines = ["Source: Drudge Report"]
    for idx, item in enumerate(items, 1):
      title = item.get("title") or "Untitled"
      url = item.get("url") or ""
      host = ""
      if url:
        try:
          parsed = urllib.parse.urlparse(url)
          host = parsed.netloc
        except Exception:
          host = ""
      heading = f"{idx}. {title}"
      if host:
        heading += f" [{host}]"
      lines.append(heading)
      if url:
        context_lines.append(f"[{idx}] {title}\nURL: {url}")

    # Save to web context for follow-up questions
    sender_key = _safe_sender_key(sender_id)
    if sender_key:
      _store_web_context(sender_key, "\n".join(lines), context="\n".join(context_lines))
    return _cmd_reply(cmd, "\n".join(lines))

  elif cmd == "/wiki":
    # Live Wikipedia-style lookup. Prefer offline store when available.
    query = full_text[len(cmd):].strip()
    if not query:
      return _cmd_reply(cmd, "Use this by typing: /wiki <topic>")

    if OFFLINE_WIKI_ENABLED and OFFLINE_WIKI_STORE is not None and OFFLINE_WIKI_STORE.is_ready():
      try:
        article, suggestions = OFFLINE_WIKI_STORE.lookup(
          query,
          summary_limit=OFFLINE_WIKI_SUMMARY_LIMIT,
          context_limit=OFFLINE_WIKI_CONTEXT_LIMIT,
        )
      except Exception:
        article, suggestions = None, []
      if article:
        lines = [f"📚 {article.title}"]
        if article.summary:
          lines.append(article.summary)
        context_len = min(len(article.content), OFFLINE_WIKI_CONTEXT_LIMIT)
        lines.append(f"Loaded about {context_len} chars of context. Ask follow-up questions or send /reset to clear.")
        if article.source:
          lines.append(f"Source: {article.source}")
        formatted = "\n".join(lines)
        sender_key = _safe_sender_key(sender_id)
        if sender_key and article.content:
          _store_web_context(sender_key, formatted, context=article.content)
        return _cmd_reply(cmd, formatted)
      else:
        if suggestions:
          hint = ", ".join(suggestions)
          return _cmd_reply(cmd, f"📚 Not found. Try: {hint}")

    # Fallback to web search constrained to Wikipedia
    try:
      results = _web_search_duckduckgo(f"site:wikipedia.org {query}")
    except RuntimeError as exc:
      message = str(exc)
      if message == "offline":
        return _cmd_reply(cmd, translate(lang, "web_offline", "🌐 Offline mode only. Wiki lookup unavailable."))
      return _cmd_reply(cmd, f"⚠️ Wiki search failed: {message}")
    title = f"{query} (Wikipedia)"
    formatted = _format_web_results(title, results)
    context_block = _context_from_results(title, results)
    sender_key = _safe_sender_key(sender_id)
    if sender_key:
      _store_web_context(sender_key, formatted, context=context_block)
    return _cmd_reply(cmd, formatted)

  elif cmd == "/offline":
    if not is_direct:
      msg = translate(lang, 'dm_only', "❌ This command can only be used in a direct message.")
      return _cmd_reply(cmd, msg)
    remainder = full_text[len(cmd):].strip()
    if not remainder:
      helper_lines = [
        "Offline toolkit:",
        "• wiki <topic> — load encyclopedia context without the internet.",
      ]
      return _cmd_reply(cmd, "\n".join(helper_lines))

    parts = remainder.split(None, 1)
    subcmd_raw = parts[0].lower()
    argument = parts[1].strip() if len(parts) > 1 else ""

    offline_aliases = {
      "wiki": "wiki",
      "wikipedia": "wiki",
      "wifi": "wiki",
    }
    subcmd = offline_aliases.get(subcmd_raw)
    if not subcmd:
      suggestions = difflib.get_close_matches(subcmd_raw, offline_aliases.keys(), n=1, cutoff=0.7)
      if suggestions:
        subcmd = offline_aliases[suggestions[0]]
      else:
        return _cmd_reply(cmd, f"Offline toolkit commands:\n• wiki <topic> (you wrote '{subcmd_raw}')")

    if subcmd == "wiki":
      if not OFFLINE_WIKI_ENABLED or OFFLINE_WIKI_STORE is None:
        return _cmd_reply(cmd, "📚 Offline wiki support is disabled on this node.")
      if not OFFLINE_WIKI_STORE.is_ready():
        error = OFFLINE_WIKI_STORE.error_message() or f"Install offline data at {OFFLINE_WIKI_INDEX}."
        return _cmd_reply(cmd, f"📚 Offline wiki not ready: {error}")
      if not argument:
        return _cmd_reply(cmd, "Use this by typing: /offline wiki <topic>")

      try:
        article, suggestions = OFFLINE_WIKI_STORE.lookup(
          argument,
          summary_limit=OFFLINE_WIKI_SUMMARY_LIMIT,
          context_limit=OFFLINE_WIKI_CONTEXT_LIMIT,
        )
      except Exception as exc:
        clean_log(f"Offline wiki lookup error: {exc}", "⚠️", show_always=True, rate_limit=False)
        return _cmd_reply(cmd, "⚠️ Offline wiki lookup failed. Try again later.")

      if not article:
        if suggestions:
          suggestion_line = ", ".join(suggestions)
          return _cmd_reply(cmd, f"📚 I couldn't find '{argument}'. Try: {suggestion_line}")
        available = list(itertools.islice(OFFLINE_WIKI_STORE.available_topics(), 5))
        if available:
          catalog = ", ".join(available)
          helper = (
            f"📚 I couldn't find '{argument}' in the offline library."
            f" Try one of the loaded topics: {catalog}."
            f" Add more JSON articles under {OFFLINE_WIKI_DIR} to expand the catalog."
          )
          return _cmd_reply(cmd, helper)
        error = OFFLINE_WIKI_STORE.error_message()
        if error:
          clean_log(error, "⚠️", show_always=True, rate_limit=False)
        return _cmd_reply(cmd, f"📚 I couldn't find '{argument}' and the offline library is currently empty.")

      lines = [f"📚 Offline Wiki: {article.title}"]
      if article.summary:
        lines.append(article.summary)
      context_len = min(len(article.content), OFFLINE_WIKI_CONTEXT_LIMIT)
      lines.append(f"Loaded about {context_len} chars of context. Ask follow-up questions or send /reset to clear.")
      if article.source:
        lines.append(f"Source: {article.source}")
      formatted = "\n".join(lines)

      sender_key = _safe_sender_key(sender_id)
      if sender_key and article.content:
        _store_web_context(sender_key, formatted, context=article.content)

      origin = article.matched_alias if article.matched_alias else argument
      clean_log(f"Offline wiki request '{origin}' → {article.title}", "📦", show_always=False)
      return _cmd_reply(cmd, formatted)

    return _cmd_reply(cmd, "Offline toolkit commands:\n• wiki <topic>")

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

  elif cmd in ("/mathquiz", "/electricalquiz"):
    category = {
      "/mathquiz": "math",
      "/electricalquiz": "electrical",
    }[cmd]
    args = full_text[len(cmd):].strip()
    result = handle_trivia_command(cmd, category, args, sender_id, is_direct, channel_idx, lang)
    return _cmd_reply(cmd, result)

  elif cmd in {"/games", "/blackjack", "/yahtzee", "/hangman", "/wordle", "/adventure", "/rps", "/coinflip", "/wordladder", "/cipher", "/quizbattle", "/morse"}:
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
          response_text = _format_bible_nav_message(text)
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
        response_text = _format_bible_nav_message(text)
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
      response_text = _format_bible_nav_message(text)
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

  elif cmd == "/stats":
    if not is_direct:
      return _cmd_reply(cmd, translate(lang, 'dm_only', "❌ This command can only be used in a direct message."))
    if sender_key not in AUTHORIZED_ADMINS:
      if sender_key:
        PENDING_ADMIN_REQUESTS[sender_key] = {
          "command": cmd,
          "full_text": full_text,
          "is_direct": is_direct,
          "channel_idx": channel_idx,
          "thread_root_ts": thread_root_ts,
          "language": lang,
        }
      try:
        actor = get_node_shortname(sender_id)
      except Exception:
        actor = str(sender_id)
      clean_log(
        f"Admin password required for /stats from {actor} ({sender_id})",
        "🔐",
        show_always=True,
        rate_limit=False,
      )
    
      prompt = translate(lang, 'admin_auth_required', "🔐 Admin access required. Reply with the admin password to continue.")
      return PendingReply(prompt, "admin password")
    report = _format_stats_report(lang)
    return _cmd_reply(cmd, report)

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
    message = "🔒 The core system prompt is fixed. Use `/vibe set <name>` or `/vibe` to adjust tone."
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
    archive_needs_save = False
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
      archive_needs_save = cleared > 0
    if archive_needs_save:
      save_archive()
    if cleared > 0:
      if is_direct:
        _clear_web_context(_safe_sender_key(sender_id))
        return _cmd_reply(cmd, "🧹 Chat context cleared. Starting fresh.")
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

  elif cmd == "/alarm":
    sender_key = _safe_sender_key(sender_id)
    remainder = full_text[len(cmd):].strip()
    if not sender_key:
      return _cmd_reply(cmd, "⚠️ Unable to identify your session. Try again.")
    if remainder.lower().startswith("list"):
      if ALARM_TIMER_MANAGER is None:
        return _cmd_reply(cmd, "Alarm scheduler not available.")
      text = ALARM_TIMER_MANAGER.list_alarms(sender_key)
      return _cmd_reply(cmd, text)
    if remainder.lower().startswith(("delete", "remove", "cancel")):
      if ALARM_TIMER_MANAGER is None:
        return _cmd_reply(cmd, "Alarm scheduler not available.")
      args = remainder.split()
      if len(args) >= 2 and args[1].lower() == "all":
        text = ALARM_TIMER_MANAGER.clear_alarms(sender_key)
        return _cmd_reply(cmd, text)
      if len(args) >= 2:
        text = ALARM_TIMER_MANAGER.delete_alarm(sender_key, args[1])
        return _cmd_reply(cmd, text)
      return _cmd_reply(cmd, "Usage: /alarm delete <id|all>")
    if not remainder:
      return _cmd_reply(cmd, "Usage: /alarm <time> [date|daily] [label]. Try '/alarm list'.")
    if ALARM_TIMER_MANAGER is None:
      return _cmd_reply(cmd, "Alarm scheduler not available.")
    ok, msg = ALARM_TIMER_MANAGER.add_alarm(sender_key, sender_id, remainder)
    return _cmd_reply(cmd, msg)

  elif cmd == "/timer":
    sender_key = _safe_sender_key(sender_id)
    remainder = full_text[len(cmd):].strip()
    if not sender_key:
      return _cmd_reply(cmd, "⚠️ Unable to identify your session. Try again.")
    low = remainder.lower()
    if low.startswith("list"):
      if ALARM_TIMER_MANAGER is None:
        return _cmd_reply(cmd, "Timer scheduler not available.")
      text = ALARM_TIMER_MANAGER.list_timers(sender_key)
      return _cmd_reply(cmd, text)
    if low.startswith(("cancel", "delete", "remove")):
      if ALARM_TIMER_MANAGER is None:
        return _cmd_reply(cmd, "Timer scheduler not available.")
      parts = remainder.split()
      if len(parts) >= 2 and parts[1].lower() == "all":
        text = ALARM_TIMER_MANAGER.clear_timers(sender_key)
        return _cmd_reply(cmd, text)
      if len(parts) >= 2:
        text = ALARM_TIMER_MANAGER.cancel_timer(sender_key, parts[1])
        return _cmd_reply(cmd, text)
      return _cmd_reply(cmd, "Usage: /timer cancel <id|all>")
    if not remainder:
      return _cmd_reply(cmd, "Usage: /timer <duration> [label]. Try '/timer list'.")
    if ALARM_TIMER_MANAGER is None:
      return _cmd_reply(cmd, "Timer scheduler not available.")
    ok, msg = ALARM_TIMER_MANAGER.add_timer(sender_key, sender_id, remainder)
    return _cmd_reply(cmd, msg)

  elif cmd == "/stopwatch":
    sender_key = _safe_sender_key(sender_id)
    remainder = full_text[len(cmd):].strip()
    if not sender_key:
      return _cmd_reply(cmd, "⚠️ Unable to identify your session. Try again.")
    if ALARM_TIMER_MANAGER is None:
      return _cmd_reply(cmd, "Stopwatch not available.")
    low = remainder.lower()
    if low.startswith("start"):
      text = ALARM_TIMER_MANAGER.stopwatch_start(sender_key, sender_id)
      return _cmd_reply(cmd, text)
    if low.startswith("stop"):
      text = ALARM_TIMER_MANAGER.stopwatch_stop(sender_key)
      return _cmd_reply(cmd, text)
    text = ALARM_TIMER_MANAGER.stopwatch_status(sender_key)
    return _cmd_reply(cmd, text)

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

ADMIN_CONTROL_COMMANDS = {"/admin", "/ai", "/channels+dm", "/channels", "/dm", "/autoping", "/status", "/whatsoff", "/aliases"}
ADMIN_CONTROL_RESERVED = {"/ai", "/channels+dm", "/channels", "/dm", "/admin", "/autoping", "/status", "/whatsoff", "/aliases"}


# -----------------------------------
# Admin status helpers
# -----------------------------------

def _message_mode_summary(mode: str) -> str:
    normalized = (mode or "").strip().lower()
    if normalized == "dm_only":
        return "DM only"
    if normalized == "channel_only":
        return "Channels only"
    return "Channels + DMs"


def _gather_admin_feature_snapshot() -> Dict[str, Any]:
    snapshot = get_feature_flags_snapshot()
    if not isinstance(snapshot, dict):
        snapshot = {}
    return snapshot


def _render_admin_status(sender_id: Any) -> str:
    snapshot = _gather_admin_feature_snapshot()
    try:
        short_name = get_node_shortname(sender_id)
    except Exception:
        short_name = str(sender_id)

    ai_enabled = bool(snapshot.get("ai_enabled", True))
    message_mode = snapshot.get("message_mode", "both")
    auto_ping = bool(snapshot.get("auto_ping_enabled", True))
    disabled_commands = snapshot.get("disabled_commands") or []
    disabled_commands = [cmd for cmd in disabled_commands if cmd]
    disabled_commands.sort()

    reply_in_channels = bool(config.get("reply_in_channels", True))
    reply_in_directs = bool(config.get("reply_in_directs", True))

    lines = [f"Admin status for {short_name}:"]
    lines.append(f"• AI responses: {'Enabled' if ai_enabled else 'Disabled'}")
    lines.append(f"• Messaging mode: {_message_mode_summary(message_mode)}")
    lines.append(f"• Auto ping replies: {'Enabled' if auto_ping else 'Disabled'}")
    lines.append(f"• Channel replies: {'Enabled' if reply_in_channels else 'Disabled'}")
    lines.append(f"• Direct message replies: {'Enabled' if reply_in_directs else 'Disabled'}")

    if disabled_commands:
        lines.append("• Commands disabled: " + ", ".join(disabled_commands))
    else:
        lines.append("• Commands disabled: none")

    return "\n".join(lines)


def _render_admin_disabled_summary() -> str:
    snapshot = _gather_admin_feature_snapshot()

    entries: List[str] = []

    if not bool(snapshot.get("ai_enabled", True)):
        entries.append("AI responses (/ai off)")

    message_mode = snapshot.get("message_mode", "both").lower()
    if message_mode == "dm_only":
        entries.append("Channel broadcasts (message mode: DM only)")
    elif message_mode == "channel_only":
        entries.append("Direct messages (message mode: Channels only)")

    if not bool(snapshot.get("auto_ping_enabled", True)):
        entries.append("Auto ping replies")

    if not bool(config.get("reply_in_channels", True)):
        entries.append("Channel replies (config)")

    if not bool(config.get("reply_in_directs", True)):
        entries.append("Direct message replies (config)")

    disabled_commands = snapshot.get("disabled_commands") or []
    disabled_commands = sorted(cmd for cmd in disabled_commands if cmd)
    if disabled_commands:
        entries.append("Disabled commands: " + ", ".join(disabled_commands))

    if not entries:
        return "All admin-managed features are currently enabled."

    lines = ["Disabled features summary:"]
    for entry in entries:
        lines.append(f"• {entry}")
    return "\n".join(lines)


def _render_admin_aliases() -> str:
    lines: List[str] = []
    # Dynamic aliases from commands_config
    dyn = _get_dynamic_command_aliases()
    if dyn:
        lines.append("Dynamic Aliases:")
        for alias, target in sorted(dyn.items()):
            lines.append(f"• {alias} → {target}")
    else:
        lines.append("Dynamic Aliases: none")
    # Built-in aliases (short list)
    built: List[str] = []
    for alias, info in COMMAND_ALIASES.items():
        canonical = info.get('canonical', alias)
        if alias != canonical:
            built.append(f"{alias} → {canonical}")
    if built:
        lines.append("")
        lines.append("Built-in Aliases:")
        # Limit to keep output short
        for row in sorted(built)[:30]:
            lines.append(f"• {row}")
        if len(built) > 30:
            lines.append(f"(+{len(built)-30} more)")
    return "\n".join(lines)


def _admin_try_define_command_alias(text: str, sender_key: Optional[str]) -> Optional[PendingReply]:
    """If `text` looks like '/alias = /canonical', register a dynamic alias.

    Rules:
    - Admin-only; caller should ensure sender_key is authorized before invoking.
    - Flexible spacing around '=' and optional slash on the right-hand side.
    - If one side is unknown and the other is known, create alias unknown -> known.
    - If both known, map left -> right canonical.
    - Persist to commands_config['command_aliases'] and take effect immediately.
    """
    if not sender_key:
        return None
    try:
        m = re.match(r"^\s*/\s*([^=\s]+)\s*=\s*/?\s*([^=\s]+)\s*$", text.strip())
        if not m:
            return None
        left_raw = m.group(1)
        right_raw = m.group(2)
        left_token = _normalize_command_name(left_raw)
        right_token = _normalize_command_name(right_raw)
        # Determine canonical for both sides
        l_can, _, _, _, _ = resolve_command_token(left_token)
        r_can, _, _, _, _ = resolve_command_token(right_token)
        alias_name: Optional[str] = None
        target_canonical: Optional[str] = None
        if l_can and not r_can:
            alias_name, target_canonical = right_token, l_can
        elif r_can and not l_can:
            alias_name, target_canonical = left_token, r_can
        elif l_can and r_can:
            alias_name, target_canonical = left_token, r_can
        else:
            return PendingReply("❌ I couldn't find a known command on either side. Try linking an unknown to a known one.", "admin alias")
        if not alias_name or not target_canonical:
            return None
        if alias_name == target_canonical:
            return PendingReply("ℹ️ That alias already points to the target.", "admin alias")
        # Prevent overriding core admin controls
        if alias_name in ADMIN_CONTROL_RESERVED:
            return PendingReply("❌ That name is reserved.", "admin alias")
        # Update commands_config and in-memory mapping
        with CONFIG_LOCK:
            try:
                cfg = commands_config if isinstance(commands_config, dict) else {"commands": []}
                aliases = cfg.setdefault('command_aliases', {})
                # Normalize storage without extra slashes
                store_alias = alias_name if alias_name.startswith('/') else f'/{alias_name}'
                store_target = target_canonical if target_canonical.startswith('/') else f'/{target_canonical}'
                aliases[store_alias] = store_target
                # Persist
                try:
                    write_atomic(COMMANDS_CONFIG_FILE, json.dumps(cfg, indent=2))
                except Exception as exc:
                    return PendingReply(f"⚠️ Saved in memory but failed to write commands_config.json: {exc}", "admin alias")
            except Exception as exc:
                return PendingReply(f"⚠️ Could not update aliases: {exc}", "admin alias")
        # Reflect immediately by updating runtime COMMAND_ALIASES for menu + resolution consistency
        try:
            COMMAND_ALIASES[_normalize_command_name(alias_name)] = {"canonical": _normalize_command_name(target_canonical), "languages": []}
        except Exception:
            pass
        return PendingReply(f"🔗 Linked {alias_name} → {target_canonical}. Added to Other Commands.", "admin alias")
    except Exception:
        return None


def _admin_control_command(text: str, sender_id: Any, sender_key: str, channel_idx: Optional[int], *, preview: bool = False):
    tokens = text.strip().split()
    if not tokens:
        return False if preview else None
    raw_primary = tokens[0]
    primary = raw_primary.lower()
    args = [token.lower() for token in tokens[1:]]

    if primary in {"/status", "/whatsoff"}:
        if preview:
            return True
        if primary == "/status":
            message = _render_admin_status(sender_id)
            return PendingReply(message, "admin status")
        summary = _render_admin_disabled_summary()
        return PendingReply(summary, "admin whatsoff")

    if primary == "/admin" and not args:
        if preview:
            return True
        lines = [
            "🛡️ Admin console ready.",
            "• /ai on · /ai off",
            "• /channels+dm on",
            "• /channels on",
            "• /dm on",
            "• /autoping on · /autoping off",
            "• /status",
            "• /whatsoff",
            "• /aliases",
            "• /<command> on · /<command> off",
            "Other commands follow the regular menu."
        ]
        return PendingReply("\n".join(lines), "/admin command")

    if not args:
        return False if preview else None

    action = args[0]

    if primary == "/aliases":
        if preview:
            return True
        return PendingReply(_render_admin_aliases(), "admin aliases")

    if primary in {"/channels+dm", "/channels", "/dm"}:
        if action != "on":
            if preview:
                return True
            return PendingReply("Use 'on' to activate this mode. Run /admin for the full list.", "admin control")
        mode_map = {
            "/channels+dm": ("both", "Channels + DMs reopened."),
            "/channels": ("channel_only", "Channel broadcasts only."),
            "/dm": ("dm_only", "Direct messages only."),
        }
        mode_value, message = mode_map[primary]
        if preview:
            return True
        update_feature_flags(message_mode=mode_value)
        try:
            actor = get_node_shortname(sender_id)
        except Exception:
            actor = str(sender_id)
        clean_log(
            f"Inbound messaging set to {mode_value} via admin DM from {actor}",
            "🛡️",
            show_always=True,
            rate_limit=False,
        )
        return PendingReply(f"🛡️ {message}", "admin control")

    if action not in {"on", "off"}:
        return False if preview else None

    if primary == "/ai":
        enabled = action == "on"
        if preview:
            return True
        update_feature_flags(ai_enabled=enabled)
        try:
            actor = get_node_shortname(sender_id)
        except Exception:
            actor = str(sender_id)
        status = "enabled" if enabled else "disabled"
        clean_log(
            f"AI responses {status} via admin DM from {actor}",
            "🛡️",
            show_always=True,
            rate_limit=False,
        )
        emoji = "🤖" if enabled else "🛑"
        return PendingReply(f"{emoji} AI responses {status}.", "admin control")

    if primary == "/autoping":
        enabled = action == "on"
        if preview:
            return True
        update_feature_flags(auto_ping_enabled=enabled)
        try:
            actor = get_node_shortname(sender_id)
        except Exception:
            actor = str(sender_id)
        status = "enabled" if enabled else "disabled"
        clean_log(
            f"Auto ping replies {status} via admin DM from {actor}",
            "🛡️",
            show_always=True,
            rate_limit=False,
        )
        emoji = "🏓" if enabled else "🚫"
        return PendingReply(f"{emoji} Auto ping {status}.", "admin control")

    normalized = _normalize_command_name(primary)
    if not normalized or normalized in ADMIN_CONTROL_RESERVED:
        return False if preview else None
    known = _all_known_commands()
    if normalized not in known:
        if preview:
            return True
        return PendingReply(f"Unknown command {normalized}.", "admin control")
    if normalized != primary:
        if preview:
            return True
        return PendingReply("Use the exact command name (no aliases).", "admin control")
    if preview:
        return True
    enabled = action == "on"
    set_command_enabled(normalized, enabled)
    try:
        actor = get_node_shortname(sender_id)
    except Exception:
        actor = str(sender_id)
    log_status = "enabled" if enabled else "disabled"
    clean_log(
        f"Command {normalized} {log_status} via admin DM from {actor}",
        "🛡️",
        show_always=True,
        rate_limit=False,
    )
    verb = "enabled" if enabled else "disabled"
    return PendingReply(f"🛠️ Command {normalized} {verb}.", "admin control")


def parse_incoming_text(text, sender_id, is_direct, channel_idx, thread_root_ts=None, check_only=False):
  dprint(f"parse_incoming_text => text='{text}' is_direct={is_direct} channel={channel_idx} check_only={check_only}")
  sender_key = _safe_sender_key(sender_id)
  lang = _resolve_user_language(None, sender_key)
  if not check_only:
    channel_type = "DM" if is_direct else f"Ch{channel_idx}"
    logged_text = text if text is not None else ""
    short = _node_display_label(sender_id)
    clean_log(f"Message from {short} ({channel_type}): {_redact_sensitive(logged_text)}", "📨")
  text = text.strip()
  if not text:
    return None if not check_only else False
  # Quick DM-level control commands: stop/resume/blacklist/unblock
  sender_key = _safe_sender_key(sender_id)
  lower = text.lower().strip()
  if is_direct and sender_key:
    # Handle pending blacklist confirmations
    if sender_key in PENDING_BLOCK_CONFIRM and not lower.startswith('/'):
      choice = lower.strip()
      if choice in {"y", "yes", "yeah", "yep"}:
        PENDING_BLOCK_CONFIRM.pop(sender_key, None)
        _set_user_blocked(sender_key, True)
        _set_user_muted(sender_key, True)
        # Cancel any queued items
        RESEND_MANAGER.cancel_for_sender(sender_key)
        cancel_pending_responses_for_sender(sender_key)
        try:
          MAIL_MANAGER.cancel_all_for_sender(sender_key)
        except Exception:
          pass
        return PendingReply("⛔ You are now blocked. I will not respond until you send 'unblock'.", "blacklist confirm")
      elif choice in {"n", "no", "nope"}:
        PENDING_BLOCK_CONFIRM.pop(sender_key, None)
        return PendingReply("👍 Not blocked. I'm still here if you need me.", "blacklist confirm")
      else:
        return PendingReply("Please reply Y or N to confirm the block.", "blacklist confirm")

    if lower in {"/stop", "stop"}:
      if check_only:
        return True
      try:
        _stop_bible_autoscroll(sender_key)
      except Exception:
        pass
      _set_user_muted(sender_key, True)
      RESEND_MANAGER.cancel_for_sender(sender_key)
      removed = cancel_pending_responses_for_sender(sender_key)
      try:
        MAIL_MANAGER.cancel_all_for_sender(sender_key)
      except Exception:
        pass
      try:
        if ALARM_TIMER_MANAGER is not None:
          ALARM_TIMER_MANAGER.pause_for_user(sender_key)
      except Exception:
        pass
      note = f"✅ Paused. Use /start or /resume to continue. (cleared {removed} queued replies)"
      return PendingReply(note, "stop command")
    if lower in {"/start", "start", "/resume", "resume", "/continue", "continue", "/unmute", "unmute"}:
      if check_only:
        return True
      _set_user_muted(sender_key, False)
      if _is_user_blocked(sender_key):
        # If blocked, inform to use unblock
        return PendingReply("You're blocked. Send 'unblock' to remove the block.", "resume command")
      try:
        if ALARM_TIMER_MANAGER is not None:
          ALARM_TIMER_MANAGER.resume_for_user(sender_key)
      except Exception:
        pass
      return PendingReply("▶️ Resumed. I'll reply to your messages again.", "resume command")
    # Blacklist requests (various phrasings)
    if any(phrase in lower for phrase in {"/blacklistme", "blacklistme", "black list me", "add me to the black list", "add me to blacklist"}):
      if check_only:
        return True
      PENDING_BLOCK_CONFIRM[sender_key] = {"ts": time.time()}
      return PendingReply("⚠️ Block all my replies to you? Reply Y to confirm, N to cancel.", "blacklist confirm")
    if lower in {"/unblock", "unblock", "unblock me", "/unblock me"}:
      if check_only:
        return True
      _set_user_blocked(sender_key, False)
      _set_user_muted(sender_key, False)
      return PendingReply("✅ Unblocked. I can respond to you again.", "unblock command")
  if is_direct and sender_key and not text.startswith("/"):
    matched, source = _admin_credentials_match(text)
    if matched:
      if check_only:
        return False
      return _process_admin_password(sender_id, text)
  message_mode = get_message_mode()
  # If blocked, suppress everything except 'unblock' handled above
  if sender_key and _is_user_blocked(sender_key):
    return None if not check_only else False
  # If muted, suppress auto/AI replies; let commands still pass
  if sender_key and _is_user_muted(sender_key) and not text.startswith('/'):
    return None if not check_only else False
  if is_direct and message_mode == "channel_only":
    if check_only:
      return False
    try:
      clean_log("Direct message blocked: DM disabled", "🚫", show_always=False)
    except Exception:
      pass
    return "⚠️ Direct messages are currently disabled by the operator. Try the main channel instead."
  if (not is_direct) and message_mode == "dm_only":
    if check_only:
      return False
    try:
      clean_log("Channel message blocked: channel messaging disabled", "🚫", show_always=False)
    except Exception:
      pass
    return "⚠️ Channel messaging is currently disabled by the operator. Please send a direct message instead."
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
  if sender_key and sender_key in PENDING_POSITION_CONFIRM and not text.startswith("/"):
    if check_only:
      return False
    reply = _handle_position_confirmation(sender_key, sender_id, text, is_direct, channel_idx)
    if reply:
      return reply
  if is_direct and sender_key and sender_key in PENDING_SAVE_WIZARDS and not text.startswith("/"):
    if check_only:
      return False
    wizard_reply = _handle_pending_save_response(sender_id, sender_key, text)
    if wizard_reply:
      return wizard_reply
  raw_cmd_lower = None
  admin_control_preview = False
  if text.startswith("/"):
    raw_cmd_lower = text.split()[0].lower()
    # Admin alias creation: '/new=/old' or '/old = /new' form
    if is_direct and sender_key and sender_key in AUTHORIZED_ADMINS and ('=' in raw_cmd_lower or '=' in text):
      alias_reply = _admin_try_define_command_alias(text, sender_key)
      if alias_reply:
        if check_only:
          return True
        return alias_reply
    if is_direct and sender_key and sender_key not in AUTHORIZED_ADMINS:
      try:
        admin_control_preview = _admin_control_command(
          text,
          sender_id,
          sender_key,
          channel_idx,
          preview=True,
        ) or False
      except Exception:
        admin_control_preview = False
  if (
      is_direct
      and sender_key
      and sender_key not in AUTHORIZED_ADMINS
      and (raw_cmd_lower in ADMIN_CONTROL_COMMANDS or admin_control_preview)
  ):
    if check_only:
      return False
    PENDING_ADMIN_REQUESTS[sender_key] = {
      "command": raw_cmd_lower,
      "full_text": text,
      "is_direct": True,
      "channel_idx": channel_idx,
      "thread_root_ts": thread_root_ts,
      "language": lang,
    }
    prompt = translate(lang, 'admin_auth_required', "🔐 Admin access required. Reply with the admin password to continue.")
    return PendingReply(prompt, "admin password")
  if is_direct and sender_key and sender_key in PENDING_WIPE_SELECTIONS and not text.startswith("/"):
    if check_only:
      return False
    wipe_select_reply = _handle_pending_wipe_selection(sender_id, sender_key, text)
    if wipe_select_reply:
      return wipe_select_reply
  if is_direct and sender_key and sender_key in PENDING_MAILBOX_SELECTIONS and not text.startswith("/"):
    if check_only:
      return False
    mailbox_reply = _handle_pending_mailbox_selection(sender_id, sender_key, text)
    if mailbox_reply:
      return mailbox_reply
  if is_direct and sender_key and not text.startswith("/") and ONBOARDING_MANAGER.is_session_active(sender_key):
    if check_only:
      return False
    try:
      sender_short = get_node_shortname(sender_id)
    except Exception:
      sender_short = str(sender_id)
    onboarding_reply = ONBOARDING_MANAGER.handle_incoming(
      sender_key=sender_key,
      sender_id=sender_id,
      sender_short=sender_short,
      message=text,
    )
    if onboarding_reply:
      return onboarding_reply
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

  if sender_key and sender_key in PENDING_RECALL_SELECTIONS and not text.startswith("/"):
    if check_only:
      return True
    pending_reply = _handle_pending_recall_response(sender_id, sender_key, text)
    if pending_reply:
      return pending_reply
  if is_direct and sender_key and sender_key in PENDING_VIBE_SELECTIONS and not text.startswith("/"):
    if check_only:
      return False
    vibe_reply = _handle_pending_vibe_selection(sender_key, text)
    if vibe_reply:
      return vibe_reply

  sanitized = text.replace('\u0007', '').strip()
  normalized = sanitized.lower()

  if is_direct and sanitized and not sanitized.startswith('/'):
    promoted = promote_bare_command(sanitized, _known_commands(), resolve_command_token)
    if promoted:
      text = promoted
      sanitized = text.strip()
      normalized = sanitized.lower()
    elif ' ' not in sanitized:
      bare_token = sanitized.strip(string.whitespace + string.punctuation)
      if bare_token:
        candidate = f"/{bare_token}"
        canonical_cmd, _, _, _, alias_append = resolve_command_token(candidate)
        if canonical_cmd:
          rebuilt = canonical_cmd
          if alias_append:
            rebuilt = f"{rebuilt}{alias_append}"
          text = rebuilt
          sanitized = text.strip()
          normalized = sanitized.lower()

  engagement_prompt = None
  if is_direct and sender_key and not check_only:
    skip_prompt = sanitized.startswith('/') or normalized.startswith('reply')
    try:
      engagement_prompt = MAIL_MANAGER.user_engaged(sender_key, node_id=sender_id, skip_prompt=skip_prompt)
    except Exception:
      engagement_prompt = None

  if is_direct and sender_key and not check_only:
    try:
      sender_shortname = get_node_shortname(sender_id)
    except Exception:
      sender_shortname = str(sender_id)
    reply_result = MAIL_MANAGER.handle_reply_intent(sender_key, sender_id, sender_shortname, sanitized)
    if reply_result is not None:
      return reply_result

  if normalized in {"ping", "pong"}:
    if not is_auto_ping_enabled():
      return None if not check_only else False
    if check_only:
      return False
    reply_text = "Pong!" if normalized == "ping" else "Ping!"
    return PendingReply(reply_text, "ping pong auto")

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

  if engagement_prompt and not sanitized.startswith('/') and not normalized.startswith('reply'):
    if check_only:
      return False
    return PendingReply(engagement_prompt, "mail engagement")

  if is_direct and sender_key and not text.startswith("/"):
    memory_result = _maybe_handle_memory_intent(sender_id, sender_key, sanitized, LANGUAGE_FALLBACK, check_only=check_only)
    if memory_result is not None:
      if check_only:
        return bool(memory_result)
      if isinstance(memory_result, PendingReply):
        return memory_result
      if isinstance(memory_result, bool):
        return False


  # Commands (start with /) should be handled and given context
  if text.startswith("/"):
    low_cmd = text.lower().strip()
    if low_cmd.startswith("/start stopwatch"):
      text = "/stopwatch start"
    elif low_cmd.startswith("/stop stopwatch"):
      text = "/stopwatch stop"
    if is_direct and sender_key in AUTHORIZED_ADMINS:
      if check_only:
        if _admin_control_command(text, sender_id, sender_key, channel_idx, preview=True):
          return False
      else:
        admin_resp = _admin_control_command(text, sender_id, sender_key, channel_idx)
        if admin_resp is not None:
          return admin_resp
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
      if cmd_lower in ["/ai", "/bot", "/data"]:
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
      canonical_lower = canonical_cmd.lower()
      if sender_key and canonical_lower not in {"/bible", "/stop"}:
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

  sender_node = (packet.get('fromId') if isinstance(packet, dict) else None) or (packet.get('from') if isinstance(packet, dict) else None) or kwargs.get('fromId') or kwargs.get('from')
  raw_to = (packet.get('toId') if isinstance(packet, dict) else None) or (packet.get('to') if isinstance(packet, dict) else None) or kwargs.get('toId') or kwargs.get('to')
  to_node_int = parse_node_id(raw_to)
  if to_node_int is None:
    to_node_int = BROADCAST_ADDR
  sender_key = _safe_sender_key(sender_node)
  if sender_key and sender_key not in NODE_FIRST_SEEN:
    now_ts = _now()
    NODE_FIRST_SEEN[sender_key] = now_ts
  if sender_key:
    try:
      STATS.record_user_interaction(sender_key)
    except Exception:
      pass
  try:
    ONBOARDING_MANAGER.handle_heartbeat(sender_key, sender_node)
  except Exception as exc:
    clean_log(f"Onboarding heartbeat error: {exc}", "⚠️")
  try:
    MAIL_MANAGER.handle_heartbeat(sender_key, sender_node)
  except Exception as exc:
    clean_log(f"Mailbox heartbeat error: {exc}", "⚠️")

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
    ch_idx = 0
    if isinstance(packet, dict):
      ch_idx = packet.get('channel') if packet.get('channel') is not None else packet.get('channelIndex', 0)

    # De-dup: if we have seen the same text/from/to/channel very recently, drop it
    rx_key = _rx_make_key(packet, text, ch_idx)
    if _rx_seen_before(rx_key):
      info_print(f"[Info] Duplicate RX suppressed for from={sender_node} ch={ch_idx} (len={len(text or '')})")
      return
    sender_display = _node_display_label(sender_node)
    normalized_text = (text or "").strip()
    msg_length = len(normalized_text)
    if sender_key and _is_heartbeat_text(normalized_text):
        NODE_HEARTBEAT_LAST[sender_key] = _now()
    sender_token = f"[NODE:{sender_display}]"
    if to_node_int == BROADCAST_ADDR:
      dest_display = _channel_display_name(ch_idx)
      dest_token = f"[DEST:channel:{ch_idx}]"
      dest_label = f"[Channel #{ch_idx}]"
    else:
      dest_display = _node_display_label(raw_to or to_node_int)
      dest_token = f"[DEST:node:{dest_display}]"
      dest_label = "[DM]"
    info_print(f"📨 {sender_token} {sender_display} → {dest_token} {dest_display} {dest_label} (len={msg_length})")

    entry = log_message(
        sender_node,
        text,
        direct=(to_node_int != BROADCAST_ADDR),
        channel_idx=(None if to_node_int != BROADCAST_ADDR else ch_idx),
    )

    if sender_key and normalized_text and not normalized_text.startswith('/'):
        _cooldown_register(sender_key, sender_node, interface)

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
      # If AI chill mode is active and the Ollama intake is overloaded, block and DM the sender
      # Home Assistant channel traffic should not be blocked by chill mode
      is_ha_route = (HOME_ASSISTANT_ENABLED and (not is_direct) and (ch_idx == HOME_ASSISTANT_CHANNEL_INDEX))
      if (AI_PROVIDER == 'ollama') and (not is_ha_route) and _ai_chill_overloaded():
        if sender_key:
          newly_added = _ai_chill_track(sender_key, sender_node=sender_node)
          if newly_added:
            clean_log(f"Chill mode blocked Ollama intake for {sender_key}", "🧊", show_always=True, rate_limit=False)
            _ai_chill_notify_initial(sender_key, sender_node, interface)
        # Do not enqueue the task; return early so other commands/messages flow normally
        return

      # Queue the response for async processing instead of blocking here
      info_print(f"🤖 [AsyncAI] Queueing response for {sender_node}: {text[:50]}...")
      try:
        current_depth = response_queue.qsize()
      except Exception:
        current_depth = 0
      if current_depth > QUEUE_NOTICE_THRESHOLD:
        _maybe_notify_queue_delay(sender_key, sender_node, interface, is_direct)
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
            response_text, pending, already_sent = _normalize_ai_response(resp)
            if response_text:
              if pending:
                _command_delay(pending.reason)
              if not already_sent:
                if is_direct:
                  send_direct_chunks(interface, response_text, sender_node)
                else:
                  send_broadcast_chunks(interface, response_text, ch_idx)
              if pending and pending.follow_up_text:
                _schedule_follow_up_message(
                  interface,
                  pending.follow_up_text,
                  delay=pending.follow_up_delay,
                  is_direct=is_direct,
                  sender_node=sender_node,
                  channel_idx=ch_idx,
                )
              _antispam_after_response(sender_key, sender_node, interface)
              _process_bible_autoscroll_request(sender_key, sender_node, interface)
    else:
      # Non-AI messages (e.g., simple commands) can be processed immediately
      resp = parse_incoming_text(text, sender_node, is_direct, ch_idx, thread_root_ts=thread_root_ts)
      if resp:
        response_text, pending, already_sent = _normalize_ai_response(resp)
        if response_text:
          target_name = get_node_shortname(sender_node) or str(sender_node)
          summary = _truncate_for_log(response_text)
          clean_log(f"Ollama → {target_name} (0.0s)", "🦙", show_always=True, rate_limit=False)
          if pending:
            _command_delay(pending.reason)
          if not already_sent:
            if is_direct:
              send_direct_chunks(interface, response_text, sender_node)
            else:
              send_broadcast_chunks(interface, response_text, ch_idx)
          if pending and pending.follow_up_text:
            _schedule_follow_up_message(
              interface,
              pending.follow_up_text,
              delay=pending.follow_up_delay,
              is_direct=is_direct,
              sender_node=sender_node,
              channel_idx=ch_idx,
            )
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
  finally:
    try:
      MAIL_MANAGER.flush_notifications(interface, send_direct_chunks)
    except Exception as exc:
      clean_log(f"Mailbox notification flush error: {exc}", "⚠️")
    try:
      ONBOARDING_MANAGER.flush_notifications(interface, send_direct_chunks)
    except Exception as exc:
      clean_log(f"Onboarding notification flush error: {exc}", "⚠️")

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


@app.route("/dashboard/metrics", methods=["GET"])
def get_dashboard_metrics():
    """Return the live metrics snapshot used by the dashboard overview."""
    try:
        payload = _gather_dashboard_metrics()
    except Exception as exc:  # pragma: no cover - defensive guardrail for the UI
        return jsonify({"error": str(exc)}), 500
    return jsonify(payload)


@app.route("/dashboard/features", methods=["POST"])
def update_dashboard_features():
    try:
        payload = request.get_json(force=True, silent=False) or {}
    except Exception as exc:
        return jsonify({"error": f"Invalid JSON payload: {exc}"}), 400

    ai_enabled = payload.get("ai_enabled")
    disabled_commands = payload.get("disabled_commands")
    message_mode = payload.get("message_mode")
    admin_passphrase = payload.get("admin_passphrase")
    auto_ping_enabled_raw = payload.get("auto_ping_enabled")
    old_passphrase = get_admin_passphrase()

    if disabled_commands is not None and not isinstance(disabled_commands, (list, tuple)):
        return jsonify({"error": "'disabled_commands' must be a list"}), 400

    normalized_disabled: Optional[List[str]] = None
    if disabled_commands is not None:
        normalized_disabled = []
        known = _all_known_commands()
        for cmd in disabled_commands:
            normalized = _normalize_command_name(cmd)
            if normalized and normalized in known:
                normalized_disabled.append(normalized)
            else:
                return jsonify({"error": f"Unknown command: {cmd}"}), 400

    normalized_mode: Optional[str] = None
    if message_mode is not None:
        normalized_mode = _normalize_message_mode(message_mode)
        if normalized_mode not in MESSAGE_MODE_OPTIONS:
            return jsonify({"error": f"Invalid message_mode: {message_mode}"}), 400

    sanitized_passphrase: Optional[str] = None
    if admin_passphrase is not None:
        sanitized_passphrase = str(admin_passphrase or "").strip()

    auto_ping_flag: Optional[bool] = None
    if auto_ping_enabled_raw is not None:
        auto_ping_flag = bool(auto_ping_enabled_raw)

    updated = update_feature_flags(
        ai_enabled=ai_enabled,
        disabled_commands=normalized_disabled,
        message_mode=normalized_mode,
        admin_passphrase=sanitized_passphrase,
        auto_ping_enabled=auto_ping_flag,
    )
    old_passphrase_norm = str(old_passphrase or "").strip()
    passphrase_changed = False
    if sanitized_passphrase is not None:
        passphrase_changed = (str(sanitized_passphrase or "").strip() != old_passphrase_norm)

    try:
        if ai_enabled is not None:
            status = "enabled" if updated.get("ai_enabled", True) else "disabled"
            clean_log(f"AI responses {status} via dashboard", "🛠️", show_always=True, rate_limit=False)
        if normalized_disabled is not None:
            if normalized_disabled:
                clean_log(
                    f"Commands disabled: {', '.join(normalized_disabled)}",
                    "🛠️",
                    show_always=True,
                    rate_limit=False,
                )
            else:
                clean_log("All commands enabled via dashboard", "🛠️", show_always=True, rate_limit=False)
        if normalized_mode is not None:
            if normalized_mode == "both":
                clean_log("Inbound messaging set to channels + DMs", "🛠️", show_always=True, rate_limit=False)
            elif normalized_mode == "dm_only":
                clean_log("Inbound messaging set to DM only", "🛠️", show_always=True, rate_limit=False)
            elif normalized_mode == "channel_only":
                clean_log("Inbound messaging set to channels only", "🛠️", show_always=True, rate_limit=False)
        if sanitized_passphrase is not None:
            message = "Admin handoff word updated via dashboard"
            if passphrase_changed:
                message += " — whitelist reset"
            clean_log(message, "🔐", show_always=True, rate_limit=False)
        if auto_ping_flag is not None:
            status = "enabled" if auto_ping_flag else "disabled"
            clean_log(f"Auto ping replies {status} via dashboard", "🛠️", show_always=True, rate_limit=False)
    except Exception:
        pass

    return jsonify(gather_feature_snapshot())


@app.route("/dashboard/admins/remove", methods=["POST"])
def remove_dashboard_admin():
    try:
        payload = request.get_json(force=True, silent=False) or {}
    except Exception as exc:
        return jsonify({"error": f"Invalid JSON payload: {exc}"}), 400

    admin_key = str(payload.get("key") or "").strip()
    if not admin_key:
        return jsonify({"error": "Missing admin identifier"}), 400

    if admin_key not in AUTHORIZED_ADMINS:
        return jsonify({"error": "Admin not found"}), 404

    AUTHORIZED_ADMINS.discard(admin_key)
    AUTHORIZED_ADMIN_NAMES.pop(admin_key, None)
    PENDING_ADMIN_REQUESTS.pop(admin_key, None)
    if admin_key in _feature_flag_admins:
        _feature_flag_admins.discard(admin_key)
        snapshot = get_feature_flags_snapshot()
        current = set(snapshot.get("admin_whitelist", []))
        if admin_key in current:
            current.discard(admin_key)
            update_feature_flags(admin_whitelist=sorted(current))
    else:
        _refresh_authorized_admins(retain_existing=False)
    clean_log(f"Admin access revoked for {admin_key}", "🛡️", show_always=True, rate_limit=False)
    return jsonify(gather_feature_snapshot())


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

LOG_VIEWER_CHAR_LIMIT = 96

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
    "🕸️": "1f578.svg",
    "💚": "1f49a.svg",
    "💓": "1f493.svg",
    "🔗": "1f517.svg",
    "🔐": "1f510.svg",
    "📍": "1f4cd.svg",
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
  if len(normalized) > LOG_VIEWER_CHAR_LIMIT:
    normalized = _truncate_for_log(normalized, LOG_VIEWER_CHAR_LIMIT)
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
    try:
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
        color:#dfe5f1;
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
        background:rgba(0,0,0,0.9);
        padding:14px 20px 12px;
        border-bottom:1px solid #333;
        z-index:1000;
        text-align:center;
      }}
      .content {{
        margin-top:150px;
        text-align:center;
      }}
      .logbox {{
        position:relative;
        height: calc(100vh - 220px);
        overflow-y: auto;
        white-space: pre-wrap;
        word-break: break-word;
        margin:0;
        padding:20px 12px 140px;
        display:flex;
        flex-direction:column;
        justify-content:flex-end;
        gap:6px;
      }}
      .logbox::before {{
        content:"";
        position:sticky;
        top:0;
        left:0;
        right:0;
        height:50px;
        pointer-events:none;
        background:linear-gradient(to bottom, rgba(0,0,0,0.9) 0%, rgba(0,0,0,0) 80%);
        z-index:1;
      }}
      .scroll-indicator {{
        display:flex;
        align-items:center;
        gap:6px;
        background:#333;
        padding:4px 12px;
        border-radius:6px;
        min-width:150px;
        justify-content:center;
      }}
      .scroll-indicator .arrow {{
        font-size:14px;
        opacity:0;
        display:inline-flex;
        align-items:center;
      }}
      .scroll-indicator.on .arrow {{
        opacity:0.6;
        animation:drift 2.4s ease-in-out infinite;
      }}
      .scroll-indicator .label {{
        text-transform:uppercase;
        letter-spacing:0.08em;
        font-size:0.72rem;
        color:#cfd9e6;
      }}
      .heartbeat-indicator {{
        font-size:0.72rem;
        color:#f06292;
        display:inline-flex;
        align-items:center;
        margin-right:4px;
        opacity:0.7;
        transition:transform 0.3s ease, opacity 0.3s ease;
      }}
      .heartbeat-indicator.inactive {{
        opacity:0.3;
      }}
      .heartbeat-indicator.pulse {{
        animation: viewer-heartbeat 1s ease-in-out;
      }}
      .status-bar {{
        display:flex;
        justify-content:center;
        align-items:center;
        gap:16px;
        flex-wrap:wrap;
        margin-bottom:10px;
      }}
      .status-meta {{
        display:flex;
        gap:12px;
        background:rgba(0,0,0,0.45);
        padding:4px 12px;
        border-radius:8px;
      }}
      .status-meta span {{
        color:#9eb6d4;
        font-size:0.85rem;
      }}
      @keyframes rise-scale {{
        0% {{ transform: translateY(22px) scale(0.68); opacity:0; }}
        35% {{ transform: translateY(14px) scale(0.8); opacity:0.55; }}
        65% {{ transform: translateY(6px) scale(0.92); opacity:0.85; }}
        100% {{ transform: translateY(0) scale(1); opacity:1; }}
      }}
      @keyframes drift {{
        0% {{ transform:translateY(-2px); opacity:0.2; }}
        35% {{ transform:translateY(1px); opacity:0.7; }}
        65% {{ transform:translateY(-1px); opacity:0.6; }}
        100% {{ transform:translateY(-2px); opacity:0.2; }}
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
      .log-line {{
        display:flex;
        align-items:flex-start;
        gap:8px;
        margin:0;
        transform-origin: bottom center;
        white-space:pre-wrap;
        line-height:1.5;
      }}
      .log-line.animate-up {{ animation: rise-scale 0.85s ease-out; position:relative; z-index:0; }}
      .log-line .emoji {{
        width:1em;
        height:1em;
        flex-shrink:0;
        margin:0;
        vertical-align:middle;
      }}
      .log-line.incoming {{ color:#D7BA7D; }}
      .log-line.outgoing {{ color:#6A9955; }}
      .log-line.clock {{ color:#2472C8; }}
      .log-line.error {{ color:#F14C4C; font-weight:700; }}
      .log-line.incoming .message {{ color:#D7BA7D; }}
      .log-line.outgoing .message {{ color:#6A9955; }}
      .log-line.clock .message {{ color:#2472C8; }}
      .log-line.error .message {{ color:#F14C4C; font-weight:700; }}
      .header .clock {{ color:#2472C8; font-weight:bold; }}
      .log-line a {{ color:#90caf9; }}
      @keyframes viewer-heartbeat {{
        0% {{ transform: scale(1); opacity:0.7; }}
        25% {{ transform: scale(1.4); opacity:1; }}
        55% {{ transform: scale(1.1); opacity:0.85; }}
        100% {{ transform: scale(1); opacity:0.7; }}
      }}
    </style>
  </head>
  <body>
    <div class="header">
      <div class="status-bar">
        <div class="scroll-indicator on" id="scrollStatus">
          <span class="heartbeat-indicator" id="heartbeatIndicator">💓</span>
          <span class="label" id="scrollLabel">Scrolling</span>
          <span class="arrow">↓</span>
        </div>
        <div class="status-meta">
          <span id="statusTime">{now_local}</span>
          <span id="statusUptime">{uptime_str}</span>
          <span id="statusRestarts">Restarts: {restart_count}</span>
        </div>
      </div>
      <div class="proverb-box">
        <span class="headline-text" id="headlineText">{initial_proverb_html}</span>
      </div>
    </div>
    <div class="content">
      <div id="logbox" class="logbox">{log_text}</div>
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
      const heartbeatIndicator = document.getElementById('heartbeatIndicator');
      let heartbeatTimer = null;

      function updateScrollLabel(text, arrowOn) {{
        scrollLabel.textContent = text;
        if (arrowOn) {{
          scrollStatus.classList.add('on');
        }} else {{
          scrollStatus.classList.remove('on');
        }}
        if (heartbeatIndicator) {{
          heartbeatIndicator.classList.toggle('inactive', !arrowOn);
        }}
      }}

      updateScrollLabel('Scrolling', true);

      function pulseHeartbeat() {{
        if (!heartbeatIndicator) {{
          return;
        }}
        if (heartbeatTimer) {{
          clearTimeout(heartbeatTimer);
        }}
        heartbeatIndicator.classList.remove('pulse');
        heartbeatIndicator.classList.remove('inactive');
        void heartbeatIndicator.offsetWidth;
        heartbeatIndicator.classList.add('pulse');
        heartbeatTimer = setTimeout(() => heartbeatIndicator.classList.remove('pulse'), 700);
      }}

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
          updateScrollLabel('Scrolling', true);
        }} else {{
          autoScroll = false;
          updateScrollLabel('Paused', false);
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
        const url = `/logs_stream?v=${{Date.now()}}`;
        eventSource = new EventSource(url);

        eventSource.onmessage = function(event) {{
          if (event.data.includes('heartbeat') || event.data.includes('keepalive')) {{
            lastMessageTime = Date.now();
            pulseHeartbeat();
            updateScrollLabel('Streaming', true);
            reconnectAttempts = 0;
            return;
          }}
          if (event.data.includes('💓 HB')) {{
            lastMessageTime = Date.now();
            pulseHeartbeat();
            updateScrollLabel('Streaming', true);
            reconnectAttempts = 0;
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
          updateScrollLabel(autoScroll ? 'Scrolling' : 'Paused', autoScroll);
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
            updateScrollLabel('Waiting…', false);
          }} else {{
            updateScrollLabel('Scrolling', true);
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
    except Exception as exc:
        err_message = f"⚠️ Logs page failed: {exc}"
        try:
            add_script_log(err_message)
        except Exception:
            pass
        app.logger.exception("Logs endpoint error")
        return (
            "<h1>Log viewer error</h1><p>Please check mesh-master.log for details.</p>",
            500,
        )

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
    # Prepare activity stream (logs) bootstrap HTML and emoji helpers
    visible_logs = [
        line for line in script_logs
        if (_viewer_should_show(line) if _viewer_filter_enabled else True)
    ]
    visible_logs = visible_logs[-400:]
    initial_log_html = "".join(_render_log_line_html(line) for line in visible_logs)
    if not initial_log_html:
        initial_log_html = "<span class=\"log-line\">No activity yet.</span>"

    try:
        metrics_bootstrap = _gather_dashboard_metrics()
    except Exception as exc:  # pragma: no cover - defensive guardrail for UI
        print(f"⚠️ Failed to gather dashboard metrics for bootstrap: {exc}")
        metrics_bootstrap = {}
    metrics_bootstrap_attr = html.escape(json.dumps(metrics_bootstrap, ensure_ascii=False), quote=True)

    page_html = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>MESH-MASTER Operations Console</title>
  <style>
    :root {
      --bg: #05070b;
      --bg-alt: #07090c;
      --bg-panel: #0b1018;
      --border: #111722;
      --border-light: #162030;
      --accent: #569cd6;
      --accent-strong: #007acc;
      --accent-soft: rgba(86, 156, 214, 0.16);
      --text-primary: #d7deed;
      --text-secondary: #9aa4ba;
      --text-faint: #7c8497;
      --success: #6a9955;
      --warning: #d7ba7d;
      --danger: #f44747;
      --shadow: rgba(0, 0, 0, 0.35);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      padding: 0;
      background: var(--bg);
      color: var(--text-primary);
      font-family: "JetBrains Mono", "Fira Code", "Consolas", monospace;
      font-size: 13px;
      line-height: 1.6;
    }
    a { color: var(--accent); text-decoration: none; }
    a:hover { color: #8dc2f0; }
    .app-shell {
      min-height: 100vh;
      display: flex;
      flex-direction: column;
    }
    .app-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 16px 28px;
      background: linear-gradient(90deg, #07090c, #0b1018);
      border-bottom: 1px solid var(--border);
      box-shadow: 0 2px 8px var(--shadow);
      position: sticky;
      top: 0;
      z-index: 200;
    }
    .brand {
      display: flex;
      flex-direction: column;
    }
    .brand-title {
      font-size: 20px;
      font-weight: 600;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }
    .brand-subtitle {
      color: var(--text-secondary);
      font-size: 13px;
      letter-spacing: 0.05em;
    }
    .header-actions {
      display: flex;
      align-items: center;
      gap: 16px;
      color: var(--text-secondary);
      font-size: 13px;
    }
    .header-actions .divider {
      width: 1px;
      height: 20px;
      background: var(--border);
    }
    .header-link {
      padding: 6px 12px;
      border-radius: 4px;
      border: 1px solid transparent;
      transition: background 0.2s ease, border-color 0.2s ease;
    }
    .header-link:hover {
      background: var(--accent-soft);
      border-color: var(--accent);
    }
    .connection-banner {
      padding: 10px 28px;
      font-size: 14px;
      border-bottom: 1px solid var(--border);
      background: rgba(7, 9, 12, 0.92);
      color: var(--text-secondary);
    }
    .connection-banner.is-connected {
      background: rgba(106, 153, 85, 0.18);
      color: var(--success);
      border-bottom-color: rgba(106, 153, 85, 0.45);
    }
    .connection-banner.is-degraded {
      background: rgba(215, 186, 125, 0.18);
      color: var(--warning);
      border-bottom-color: rgba(215, 186, 125, 0.45);
    }
    .connection-banner.is-disconnected {
      background: rgba(244, 71, 71, 0.14);
      color: var(--danger);
      border-bottom-color: rgba(244, 71, 71, 0.45);
    }
    .connection-banner.is-unknown {
      color: var(--text-secondary);
    }
    .content {
      flex: 1;
      padding: 24px;
      display: grid;
      grid-template-columns: minmax(320px, 2fr) minmax(420px, 3fr);
      grid-template-areas: "primary activity";
      gap: 20px;
      align-items: start;
    }
    .primary {
      grid-area: primary;
      display: flex;
      flex-direction: column;
      gap: 28px;
    }
    .activity {
      grid-area: activity;
      display: flex;
      flex-direction: column;
      gap: 28px;
      position: relative;
    }
    .activity::before {
      content: "";
      position: absolute;
      inset: 0;
      border-radius: 14px;
      background: rgba(7, 9, 12, 0.92);
      box-shadow: 0 40px 60px rgba(0, 0, 0, 0.45);
      z-index: 0;
    }
    .activity > * {
      position: relative;
      z-index: 1;
    }
    .panel {
      background: var(--bg-panel);
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 18px;
      box-shadow: 0 12px 28px rgba(0, 0, 0, 0.2);
      display: flex;
      flex-direction: column;
      gap: 12px;
    }
    .panel[data-draggable="true"] .panel-header {
      cursor: grab;
    }
    .panel.is-dragging {
      opacity: 0.6;
      cursor: grabbing;
    }
    .panel.is-dragging .panel-header {
      cursor: grabbing;
    }
    .panel-drag-handle {
      user-select: none;
      touch-action: none;
    }
    .panel-placeholder {
      display: block;
      width: 100%;
      border: 2px dashed rgba(86, 156, 214, 0.45);
      border-radius: 10px;
      min-height: 88px;
      margin: 4px 0;
      pointer-events: none;
      background: rgba(86, 156, 214, 0.08);
    }
    [data-panel-zone].drop-active {
      outline: 1px dashed rgba(86, 156, 214, 0.45);
      outline-offset: 6px;
    }
    .panel-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 16px;
      border-bottom: 1px solid var(--border);
      padding-bottom: 10px;
    }
    .panel-title {
      display: flex;
      flex-direction: column;
      gap: 2px;
    }
    .panel-body {
      display: flex;
      flex-direction: column;
      gap: 12px;
    }
    .panel.collapsed {
      gap: 8px;
    }
    .panel.collapsed .panel-body {
      display: none;
    }
    .panel-collapse {
      position: relative;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: 32px;
      height: 26px;
      padding: 0;
      background: rgba(86, 156, 214, 0.15);
      border: 1px solid rgba(86, 156, 214, 0.25);
      border-radius: 6px;
      color: var(--text-secondary);
      cursor: pointer;
      transition: background 0.2s ease, border-color 0.2s ease, color 0.2s ease;
    }
    .panel-collapse::before {
      content: '▾';
      font-size: 14px;
      line-height: 1;
      transform: translateY(-1px);
    }
    .panel.collapsed .panel-collapse::before {
      content: '▸';
      transform: translateY(-1px);
    }
    .panel-collapse:hover,
    .panel-collapse:focus-visible {
      background: rgba(86, 156, 214, 0.28);
      border-color: rgba(86, 156, 214, 0.45);
      color: var(--accent);
      outline: none;
    }
    .panel-header h2 {
      margin: 0;
      font-size: 17px;
      font-weight: 600;
      color: var(--text-primary);
    }
    .panel-subtitle {
      font-size: 13px;
      color: var(--text-secondary);
      white-space: nowrap;
    }
    .ops-panel {
      gap: 14px;
    }
    .config-panel {
      display: flex;
      flex-direction: column;
      gap: 14px;
    }
    .config-select-row {
      display: flex;
      align-items: center;
      gap: 12px;
    }
    .config-select-row label {
      font-size: 11.5px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--text-secondary);
    }
    /* reset defaults button removed */
    .config-select {
      flex: 1;
      background: rgba(17, 19, 25, 0.9);
      border: 1px solid rgba(86, 156, 214, 0.35);
      border-radius: 8px;
      color: var(--text-primary);
      font-family: inherit;
      font-size: 13px;
      padding: 8px 10px;
    }
    .config-row.config-row-stack {
      flex-direction: column;
      align-items: stretch;
      gap: 8px;
    }
    .config-row.config-row-stack .config-key-heading {
      margin: 0 0 4px 0;
    }
    .config-select:focus {
      border-color: var(--accent);
      outline: none;
      box-shadow: 0 0 0 2px rgba(86, 156, 214, 0.22);
    }
    .config-quiet-hours {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 12px;
    }
    .config-quiet-hours .quiet-toggle {
      display: flex;
      align-items: center;
      gap: 6px;
      font-size: 12.5px;
    }
    .config-quiet-hours .quiet-toggle input {
      width: 16px;
      height: 16px;
    }
    .config-quiet-hours .quiet-range {
      display: flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
    }
    .config-quiet-hours .quiet-range span {
      font-size: 12.5px;
      color: var(--text-secondary);
    }
    .config-quiet-hours .quiet-select {
      flex: 0 0 auto;
      width: 110px;
    }
    .config-bool-group {
      display: flex;
      gap: 14px;
      align-items: center;
    }
    .config-bool-option {
      display: flex;
      align-items: center;
      gap: 6px;
      font-size: 12.5px;
      color: var(--text-primary);
    }
    .config-bool-option input[type="radio"] {
      accent-color: var(--accent);
    }
    .config-table {
      border: 1px solid var(--border-light);
      border-radius: 10px;
      background: rgba(11, 15, 22, 0.92);
      overflow: hidden;
      display: flex;
      flex-direction: column;
    }
    .config-row {
      display: flex;
      align-items: flex-start;
      gap: 12px;
      padding: 10px 12px;
      border-bottom: 1px solid rgba(60, 65, 80, 0.4);
    }
    .config-row:last-child {
      border-bottom: none;
    }
    .config-key {
      display: flex;
      flex-direction: column;
      gap: 2px;
      min-width: 180px;
    }
    .config-key-label {
      font-weight: 600;
      color: var(--accent);
      font-size: 13px;
      letter-spacing: 0.02em;
    }
    .config-key-code {
      font-family: "JetBrains Mono", "Fira Code", "Consolas", monospace;
      font-size: 11px;
      color: var(--text-faint);
      letter-spacing: 0.02em;
      text-transform: lowercase;
    }
    .config-key.has-explainer {
      cursor: help;
    }
    .config-value {
      flex: 1;
      margin-left: auto;
      display: flex;
      flex-direction: column;
      gap: 8px;
    }
    .config-display-line {
      display: flex;
      justify-content: flex-end;
      align-items: center;
      gap: 8px;
      color: var(--text-primary);
      font-size: 12.5px;
      text-align: right;
      word-break: break-word;
    }
    .config-display {
      white-space: pre-wrap;
    }
    .config-key-heading {
      display: flex;
      align-items: center;
      gap: 6px;
    }
    .config-info {
      width: 18px;
      height: 18px;
      border-radius: 50%;
      border: 1px solid var(--border-light);
      display: inline-flex;
      align-items: center;
      justify-content: center;
      font-size: 11px;
      font-weight: 600;
      color: var(--accent);
      background: transparent;
      cursor: help;
      transition: background 0.2s ease, border-color 0.2s ease, color 0.2s ease;
    }
    .config-info:hover,
    .config-info:focus-visible {
      background: var(--accent-soft);
      border-color: var(--accent);
      color: #8dc2f0;
      outline: none;
    }
    .command-item[data-explainer] span {
      cursor: help;
    }
    .dashboard-tooltip {
      position: fixed;
      z-index: 9999;
      max-width: 300px;
      padding: 10px 12px;
      background: rgba(7, 10, 16, 0.97);
      border: 1px solid var(--border-light);
      border-radius: 8px;
      box-shadow: 0 16px 32px rgba(0, 0, 0, 0.45);
      color: var(--text-primary);
      font-size: 12px;
      line-height: 1.5;
      pointer-events: none;
      opacity: 0;
      transform: translate(-50%, -8px);
      transition: opacity 0.12s ease, transform 0.12s ease;
      white-space: pre-line;
    }
    .dashboard-tooltip.is-visible {
      opacity: 1;
      transform: translate(-50%, 0);
    }
    .config-type-badge {
      background: rgba(86, 156, 214, 0.18);
      border: 1px solid rgba(86, 156, 214, 0.4);
      border-radius: 6px;
      color: #cfe3fb;
      font-size: 11px;
      font-weight: 600;
      letter-spacing: 0.06em;
      padding: 2px 8px;
      text-transform: uppercase;
    }
    .config-edit-btn,
    .config-save-btn,
    .config-cancel-btn {
      background: rgba(86, 156, 214, 0.15);
      border: 1px solid rgba(86, 156, 214, 0.3);
      border-radius: 6px;
      color: var(--text-primary);
      font-size: 11.5px;
      font-weight: 600;
      letter-spacing: 0.05em;
      padding: 4px 10px;
      cursor: pointer;
      text-transform: uppercase;
      transition: background 0.2s ease, border-color 0.2s ease, color 0.2s ease;
    }
    .config-edit-btn:hover,
    .config-save-btn:hover {
      background: rgba(86, 156, 214, 0.28);
      border-color: rgba(86, 156, 214, 0.45);
      color: var(--accent);
    }
    .config-cancel-btn {
      background: rgba(244, 71, 71, 0.12);
      border-color: rgba(244, 71, 71, 0.32);
      color: var(--danger);
    }
    .config-cancel-btn:hover {
      background: rgba(244, 71, 71, 0.22);
      border-color: rgba(244, 71, 71, 0.45);
    }
    .config-edit-area {
      display: flex;
      flex-direction: column;
      gap: 6px;
    }
    .config-row:not(.is-editing) .config-edit-area {
      display: none;
    }
    .config-row.is-editing .config-display-line {
      display: none;
    }
    .config-input {
      width: 100%;
      background: rgba(17, 19, 25, 0.9);
      border: 1px solid rgba(86, 156, 214, 0.35);
      border-radius: 8px;
      color: var(--text-primary);
      font-family: inherit;
      font-size: 12.5px;
      padding: 8px 10px;
      resize: vertical;
      min-height: 48px;
    }
    .config-input:focus {
      border-color: var(--accent);
      outline: none;
      box-shadow: 0 0 0 2px rgba(86, 156, 214, 0.22);
    }
    .config-actions {
      display: flex;
      justify-content: flex-end;
      gap: 6px;
    }
    .config-status {
      font-size: 11px;
      color: var(--text-secondary);
      text-align: right;
      min-height: 14px;
    }
    .config-status[data-tone="error"] {
      color: var(--danger);
    }
    .config-status[data-tone="success"] {
      color: #7adba4;
    }
    .config-row[data-saving="true"] .config-save-btn,
    .config-row[data-saving="true"] .config-cancel-btn,
    .config-row[data-saving="true"] .config-edit-btn {
      cursor: wait;
      opacity: 0.6;
    }
    .config-row[data-saving="true"] .config-input {
      pointer-events: none;
      opacity: 0.65;
    }
    .config-empty {
      margin: 0;
      padding: 14px;
      text-align: center;
      color: var(--text-secondary);
      font-size: 12.5px;
    }
    .feature-alerts {
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
      background: rgba(86, 156, 214, 0.08);
      border: 1px solid rgba(86, 156, 214, 0.25);
      border-radius: 10px;
      padding: 8px 12px;
    }
    .feature-pill {
      background: rgba(86, 156, 214, 0.18);
      border-radius: 999px;
      padding: 2px 10px;
      font-size: 12px;
      color: #cfe3fb;
    }
    .toggle-row {
      display: flex;
      align-items: center;
      gap: 12px;
      padding: 4px 0 2px;
    }
    .mode-toggle {
      display: flex;
      flex-direction: column;
      gap: 8px;
      padding: 6px 0 4px;
    }
    .mode-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
    }
    .mode-title {
      font-weight: 600;
      font-size: 13px;
      letter-spacing: 0.05em;
      text-transform: uppercase;
      color: var(--text-secondary);
    }
    .mode-status {
      font-size: 12px;
      color: var(--text-secondary);
    }
    .mode-buttons {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      background: rgba(17, 19, 25, 0.75);
      border: 1px solid rgba(86, 156, 214, 0.25);
      border-radius: 999px;
      overflow: hidden;
    }
    .mode-buttons[data-saving="true"] {
      opacity: 0.6;
      pointer-events: none;
    }
    .mode-btn {
      background: transparent;
      color: var(--text-secondary);
      border: none;
      padding: 8px 10px;
      font-size: 12.5px;
      cursor: pointer;
      transition: background 0.2s ease, color 0.2s ease;
    }
    .mode-btn + .mode-btn {
      border-left: 1px solid rgba(86, 156, 214, 0.15);
    }
    .passphrase-card {
      background: var(--bg-panel);
      border: 1px solid var(--border-light);
      border-radius: 8px;
      padding: 12px 14px;
      display: flex;
      flex-direction: column;
      gap: 8px;
    }
    .passphrase-card label {
      font-size: 11.5px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--text-secondary);
    }
    .passphrase-input {
      display: flex;
      gap: 10px;
      align-items: center;
    }
    .passphrase-input input {
      flex: 1;
      background: rgba(9, 12, 19, 0.92);
      border: 1px solid rgba(86, 156, 214, 0.3);
      border-radius: 6px;
      color: var(--text-primary);
      font-family: inherit;
      font-size: 13px;
      padding: 8px 10px;
      transition: border-color 0.2s ease, box-shadow 0.2s ease;
    }
    .passphrase-input input:focus {
      border-color: var(--accent);
      box-shadow: 0 0 0 2px rgba(86, 156, 214, 0.25);
      outline: none;
    }
    .passphrase-hint {
      margin: 0;
      font-size: 11.5px;
      color: var(--text-secondary);
    }
    .passphrase-warning {
      margin: 0;
      font-size: 11.5px;
      color: var(--warning);
    }
    .mode-btn.active {
      background: var(--accent);
      color: #081019;
      font-weight: 600;
    }
    .mode-btn:focus-visible {
      outline: 2px solid var(--accent-strong);
      outline-offset: -2px;
    }
    .switch {
      position: relative;
      width: 44px;
      height: 24px;
      flex-shrink: 0;
    }
    .switch input {
      display: none;
    }
    .switch .slider {
      position: absolute;
      inset: 0;
      background: #3a3d45;
      border-radius: 24px;
      transition: background 0.25s ease;
    }
    .switch .slider::before {
      content: "";
      position: absolute;
      width: 18px;
      height: 18px;
      left: 3px;
      top: 3px;
      border-radius: 50%;
      background: #f4f7fb;
      transition: transform 0.25s ease;
    }
    .switch input:checked + .slider {
      background: var(--accent);
    }
    .switch input:checked + .slider::before {
      transform: translateX(20px);
    }
    .toggle-copy {
      display: flex;
      flex-direction: column;
      gap: 2px;
    }
    .toggle-title {
      font-weight: 600;
      font-size: 14px;
    }
    .toggle-status {
      font-size: 12px;
      color: var(--text-secondary);
    }
    .command-groups {
      display: flex;
      flex-direction: column;
      gap: 10px;
    }
    .command-groups[data-saving="true"] {
      opacity: 0.6;
      pointer-events: none;
    }
    .command-group {
      background: rgba(17, 19, 25, 0.75);
      border: 1px solid rgba(60, 65, 80, 0.55);
      border-radius: 10px;
      padding: 0;
      overflow: hidden;
    }
    .command-group summary {
      list-style: none;
      cursor: pointer;
      padding: 10px 14px;
      font-weight: 600;
      font-size: 13px;
      color: var(--text-primary);
      display: flex;
      justify-content: space-between;
      align-items: center;
    }
    .command-group summary::after {
      content: '▾';
      font-size: 12px;
      color: var(--text-secondary);
      transition: transform 0.2s ease;
      margin-left: 12px;
    }
    .command-group:not([open]) summary::after {
      transform: rotate(-90deg);
    }
    .command-group summary::-webkit-details-marker {
      display: none;
    }
    .command-group[open] summary {
      border-bottom: 1px solid rgba(60, 65, 80, 0.6);
    }
    .command-list {
      display: grid;
      gap: 6px 12px;
      grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
      padding: 10px 14px 12px;
    }
    .command-item {
      display: flex;
      align-items: center;
      gap: 6px;
      font-size: 12.5px;
      color: var(--text-secondary);
      text-transform: none;
    }
    .command-item input {
      accent-color: var(--accent-strong);
    }
    .command-item.is-disabled span {
      color: var(--warning);
      font-weight: 600;
    }
    .feature-empty {
      font-size: 12.5px;
      color: var(--text-secondary);
      padding: 6px 16px 8px;
    }
    .snapshot-grid {
      display: grid;
      gap: 12px;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
    }
    .snapshot-section {
      background: rgba(17, 19, 25, 0.65);
      border: 1px solid rgba(60, 65, 80, 0.45);
      border-radius: 10px;
      padding: 10px 12px;
      display: flex;
      flex-direction: column;
      gap: 10px;
    }
    .snapshot-section h3 {
      margin: 0;
      font-size: 12px;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: var(--text-secondary);
    }
    .snapshot-list {
      margin: 0;
      padding: 0;
      display: flex;
      flex-direction: column;
      gap: 8px;
    }
    .snapshot-list div {
      display: flex;
      flex-direction: column-reverse;
      align-items: flex-start;
      gap: 2px;
      padding: 0;
    }
    .snapshot-list dt {
      margin: 0;
      font-size: 11px;
      color: var(--text-secondary);
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }
    .snapshot-list dd {
      margin: 0;
      display: flex;
      align-items: baseline;
      gap: 6px;
      font-size: 15px;
      font-weight: 600;
      color: var(--text-primary);
    }
    .stat-row {
      display: flex;
      justify-content: space-between;
      align-items: center;
      font-size: 14px;
      padding: 10px 0;
      border-bottom: 1px solid var(--border);
    }
    .stat-row:last-child {
      border-bottom: none;
    }
    .stat-row .label {
      color: var(--text-secondary);
    }
    .stat-row .value {
      color: var(--text-primary);
      font-weight: 500;
    }
    .games-breakdown {
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
      font-size: 12px;
      color: var(--text-secondary);
      margin-top: 4px;
    }
    .games-breakdown span {
      background: rgba(86, 156, 214, 0.12);
      border: 1px solid rgba(86, 156, 214, 0.25);
      border-radius: 999px;
      padding: 4px 10px;
    }
    .onboard-roster {
      display: flex;
      flex-direction: column;
      gap: 4px;
      margin-top: 6px;
      max-height: 160px;
      overflow-y: auto;
      font-size: 12px;
      color: var(--text-secondary);
    }
    .onboard-roster-item {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 8px;
      padding: 4px 6px;
      border-radius: 6px;
      background: rgba(86, 156, 214, 0.08);
      border: 1px solid rgba(86, 156, 214, 0.18);
    }
    .onboard-roster-item strong {
      color: var(--text-primary);
      font-size: 12.5px;
    }
    .onboard-roster-item span {
      color: var(--text-secondary);
      font-size: 11px;
    }
    .onboard-roster-empty {
      font-size: 12px;
      color: var(--text-secondary);
      padding: 6px 0;
    }
    .stat-value-number {
      font-weight: 600;
      color: var(--text-primary);
      font-size: 1.05em;
    }
    .delta {
      display: inline-flex;
      align-items: center;
      font-size: 11px;
      letter-spacing: 0.02em;
    }
    .delta-up {
      color: #6a9955;
    }
    .delta-down {
      color: #f44747;
    }
    .delta-flat {
      color: var(--text-secondary);
    }
    .log-panel {
      flex: 1;
      display: flex;
      flex-direction: column;
      background: rgba(7, 9, 12, 0.92);
      border: 1px solid rgba(50, 54, 67, 0.75);
      border-radius: 14px;
      padding: 22px 24px;
      box-shadow: 0 24px 48px rgba(0, 0, 0, 0.3);
      overflow: hidden;
    }
    .log-panel .panel-header {
      border-bottom: 1px solid rgba(60, 65, 80, 0.45);
      padding-bottom: 10px;
      margin-bottom: 10px;
    }
    .logbox {
      flex: 1;
      overflow-y: auto;
      background: transparent;
      border: none;
      padding: 8px 4px 4px;
      margin: 0;
      display: flex;
      flex-direction: column;
      gap: 6px;
      font-family: "JetBrains Mono", "Fira Code", "Consolas", monospace;
      font-size: 13px;
      line-height: 1.6;
      scrollbar-width: thin;
    }
    .logbox::before {
      content: "";
      position: sticky;
      top: 0;
      height: 38px;
      pointer-events: none;
      background: linear-gradient(to bottom, rgba(7,9,12,0.96) 0%, rgba(7,9,12,0) 85%);
      z-index: 2;
    }
    .log-line {
      display: flex;
      align-items: flex-start;
      gap: 8px;
      padding: 0 8px;
      color: #dce6f4;
      transition: transform 0.35s ease, opacity 0.35s ease;
      white-space: pre-wrap;
      line-height: 1.5;
    }
    .log-line.new-line {
      border-left: 3px solid var(--accent);
      padding-left: 12px;
    }
    .log-line.animate-up {
      animation: rise-in 0.6s ease-out;
    }
    .log-line .emoji {
      width: 1em;
      height: 1em;
      flex-shrink: 0;
      margin: 0;
      vertical-align: middle;
    }
    @keyframes rise-in {
      0% { transform: translateY(12px); opacity: 0; }
      60% { transform: translateY(4px); opacity: 0.8; }
      100% { transform: translateY(0); opacity: 1; }
    }
    .log-line .timestamp {
      color: var(--text-secondary);
      margin-right: 8px;
    }
    .log-line .message {
      color: var(--text-primary);
      flex: 1;
    }
    .log-line.incoming .message {
      color: #d7ba7d;
    }
    .log-line.outgoing .message {
      color: #6a9955;
    }
    .log-line.clock .message {
      color: #2472c8;
    }
    .log-line.error .message {
      color: #f14c4c;
      font-weight: 600;
    }
    .scroll-status {
      font-size: 12px;
      color: #cfd9e6;
      display: inline-flex;
      align-items: center;
      gap: 8px;
      background: rgba(25, 28, 36, 0.75);
      border: 1px solid rgba(86, 156, 214, 0.25);
      padding: 4px 12px;
      border-radius: 999px;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }
    .scroll-status::before {
      content: "";
      width: 6px;
      height: 6px;
      border-radius: 50%;
      background: rgba(207, 217, 230, 0.45);
      transition: background 0.2s ease;
    }
    .scroll-status::after {
      content: "↓";
      font-size: 12px;
      opacity: 0;
      transform: translateY(-2px);
      transition: opacity 0.2s ease;
    }
    .heartbeat-indicator {
      font-size: 12px;
      color: #f06292;
      display: inline-flex;
      align-items: center;
      margin-right: 4px;
      opacity: 0.7;
      transition: transform 0.3s ease, opacity 0.3s ease;
    }
    .heartbeat-indicator.inactive {
      opacity: 0.3;
    }
    .heartbeat-indicator.pulse {
      animation: heartbeat-pulse 1s ease-in-out;
    }
    @keyframes pulse-arrow {
      0% { transform: translateY(-2px); opacity: 0.15; }
      50% { transform: translateY(2px); opacity: 0.6; }
      100% { transform: translateY(-2px); opacity: 0.15; }
    }
    @keyframes heartbeat-pulse {
      0% { transform: scale(1); opacity: 0.7; }
      25% { transform: scale(1.35); opacity: 1; }
      55% { transform: scale(1.1); opacity: 0.85; }
      100% { transform: scale(1); opacity: 0.7; }
    }
    .scroll-status.on::before {
      background: var(--accent);
    }
    .scroll-status.on::after {
      opacity: 0.7;
      animation: pulse-arrow 2s ease-in-out infinite;
    }
    .btn {
      background: var(--accent);
      color: #0b121a;
      border: none;
      padding: 6px 12px;
      border-radius: 4px;
      font-weight: 500;
      cursor: pointer;
      transition: background 0.2s ease;
    }
    .btn:hover {
      background: var(--accent-strong);
      color: #fff;
    }
    .btn-small {
      padding: 4px 10px;
      font-size: 12px;
    }
    .passphrase-actions {
      margin-top: 6px;
      display: flex;
      gap: 10px;
      align-items: center;
    }
    .passphrase-card {
      position: relative;
    }
    .passphrase-set {
      background: var(--accent);
      border: 1px solid rgba(86, 156, 214, 0.4);
      color: #06101c;
      padding: 8px 14px;
      font-weight: 600;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      border-radius: 6px;
      cursor: pointer;
      transition: background 0.2s ease, border-color 0.2s ease, color 0.2s ease;
      flex-shrink: 0;
    }
    .passphrase-set:hover:not(:disabled) {
      background: #8dc2f0;
      border-color: var(--accent);
    }
    .passphrase-set:disabled {
      opacity: 0.6;
      cursor: default;
    }
    .admin-list-link {
      margin-left: 8px;
      color: var(--accent);
      text-decoration: underline;
      cursor: pointer;
    }
    .admin-list-link:hover {
      color: var(--accent-strong);
    }
    .activity-header-meta {
      display: flex;
      align-items: center;
      gap: 12px;
    }
    .uptime-ticker {
      font-size: 12px;
      color: var(--text-secondary);
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 4px 10px;
      border-radius: 999px;
      background: rgba(86, 156, 214, 0.12);
      border: 1px solid rgba(86, 156, 214, 0.25);
    }
    .admin-popover[hidden] {
      display: none;
    }
    .admin-popover {
      position: absolute;
      top: 100%;
      right: 0;
      margin-top: 10px;
      min-width: 320px;
      max-width: calc(100vw - 40px);
      background: rgba(11, 16, 24, 0.98);
      border: 1px solid var(--border-light);
      border-radius: 12px;
      box-shadow: 0 18px 48px rgba(0, 0, 0, 0.45);
      display: flex;
      flex-direction: column;
      gap: 12px;
      padding: 16px 18px 18px;
      z-index: 200;
    }
    .admin-popover::before {
      content: "";
      position: absolute;
      top: -8px;
      right: 24px;
      width: 16px;
      height: 16px;
      transform: rotate(45deg);
      background: inherit;
      border-left: 1px solid var(--border-light);
      border-top: 1px solid var(--border-light);
    }
    .admin-popover-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 10px;
    }
    .admin-popover-header h3 {
      margin: 0;
      font-size: 14px;
      letter-spacing: 0.06em;
      text-transform: uppercase;
      color: var(--text-secondary);
    }
    .admin-popover-close {
      background: transparent;
      border: none;
      color: var(--text-secondary);
      font-size: 18px;
      line-height: 1;
      padding: 2px 6px;
      border-radius: 6px;
      cursor: pointer;
      transition: background 0.2s ease, color 0.2s ease;
    }
    .admin-popover-close:hover,
    .admin-popover-close:focus-visible {
      background: rgba(86, 156, 214, 0.2);
      color: var(--text-primary);
      outline: none;
    }
    .admin-popover-body {
      display: flex;
      flex-direction: column;
      gap: 10px;
      max-height: 260px;
      overflow-y: auto;
    }
    .admin-list {
      list-style: none;
      padding: 0;
      margin: 0;
      display: flex;
      flex-direction: column;
      gap: 8px;
    }
    .admin-item {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      padding: 8px 10px;
      border-radius: 10px;
      background: rgba(12, 16, 24, 0.92);
      border: 1px solid rgba(60, 65, 80, 0.6);
      color: var(--text-secondary);
      font-size: 12.5px;
    }
    .admin-item-info {
      display: flex;
      flex-direction: column;
      gap: 2px;
      flex: 1;
      min-width: 0;
    }
    .admin-item strong {
      color: var(--text-primary);
      font-size: 13px;
    }
    .admin-item-key {
      font-size: 11px;
      color: var(--text-faint);
      letter-spacing: 0.02em;
      word-break: break-all;
    }
    .admin-empty {
      font-size: 12.5px;
      color: var(--text-secondary);
    }
    .admin-remove-btn {
      background: rgba(244, 71, 71, 0.16);
      border: 1px solid rgba(244, 71, 71, 0.4);
      color: var(--danger);
      padding: 4px 10px;
      border-radius: 6px;
      font-size: 11.5px;
      font-weight: 600;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      cursor: pointer;
      transition: background 0.2s ease, border-color 0.2s ease, color 0.2s ease;
    }
    .admin-remove-btn:hover:not(:disabled) {
      background: rgba(244, 71, 71, 0.28);
      border-color: rgba(244, 71, 71, 0.55);
    }
    .admin-remove-btn:disabled {
      opacity: 0.55;
      cursor: default;
    }
    @media (max-width: 1200px) {
      .content {
        grid-template-columns: 1fr;
        grid-template-areas:
          "primary"
          "activity";
      }
    }
    @media (max-width: 720px) {
      .app-header {
        flex-direction: column;
        align-items: flex-start;
        gap: 12px;
      }
      .header-actions {
        align-self: stretch;
        justify-content: space-between;
      }
      .content {
        padding: 20px;
      }
    }
  </style>
</head>
<body>
  <div class="app-shell" id="appShell" data-initial-metrics="__METRICS__">
    <header class="app-header">
      <div class="brand">
        <span class="brand-title">Mesh-Master 🚀</span>
        <span class="brand-subtitle">Operations Console 🛰️</span>
      </div>
      <div class="header-actions">
        <span id="metricsTimestamp" class="panel-subtitle">Waiting for metrics…</span>
        <span class="divider"></span>
        <a class="header-link" href="/logs" target="_blank">Pop-out Stream</a>
      </div>
    </header>
    <div id="connectionBanner" class="connection-banner is-unknown">Checking connection…</div>
    <main class="content">
      <section class="primary" data-panel-zone="primary">
        <article class="panel snapshot-panel" data-panel-id="snapshot" data-draggable="true" data-collapsible="true">
          <div class="panel-header">
            <div class="panel-title">
              <h2>Activity Snapshot 📊</h2>
              <span class="panel-subtitle">Mesh visibility</span>
            </div>
            <button type="button" class="panel-collapse" aria-expanded="true" aria-controls="snapshotBody" aria-label="Collapse panel"></button>
          </div>
          <div class="panel-body" id="snapshotBody">
            <div class="snapshot-grid">
              <section class="snapshot-section">
                <h3>Messaging ✉️</h3>
                <dl class="snapshot-list">
                  <div><dt>Total</dt><dd id="stat-msg-total">—</dd></div>
                  <div><dt>Direct</dt><dd id="stat-msg-direct">—</dd></div>
                  <div><dt>AI Authored</dt><dd id="stat-msg-ai">—</dd></div>
                </dl>
              </section>
              <section class="snapshot-section">
                <h3>Network 🌐</h3>
                <dl class="snapshot-list">
                  <div><dt>Active nodes</dt><dd id="stat-nodes-current">—</dd></div>
                  <div><dt>New nodes</dt><dd id="stat-nodes-new">—</dd></div>
                  <div><dt>Active users</dt><dd id="stat-active-users">—</dd></div>
                </dl>
                <div class="games-breakdown" id="stat-games-breakdown"></div>
              </section>
              <section class="snapshot-section">
                <h3>Ack Telemetry 📶</h3>
                <dl class="snapshot-list">
                  <div><dt>DM 1st try</dt><dd id="stat-ack-dm-first">—</dd></div>
                  <div><dt>DM resend</dt><dd id="stat-ack-dm-resend">—</dd></div>
                  <div><dt>DM events</dt><dd id="stat-ack-dm-events">—</dd></div>
                </dl>
              </section>
              <section class="snapshot-section">
                <h3>Onboarding 🧭</h3>
                <dl class="snapshot-list">
                  <div><dt>New 24h</dt><dd id="stat-new-onboards">—</dd></div>
                  <div><dt>Games launched</dt><dd id="stat-games">—</dd></div>
                </dl>
                <div class="onboard-roster" id="onboardRoster"></div>
              </section>
            </div>
          </div>
        </article>

        <article class="panel ops-panel" data-panel-id="operations" data-draggable="true">
          <div class="panel-header">
            <h2>Operations Center 🛠️</h2>
            <span id="featuresStatus" class="panel-subtitle">Manage AI and command access</span>
          </div>
          <div id="featureAlerts" class="feature-alerts" hidden></div>
          <div class="toggle-row">
            <label class="switch">
              <input type="checkbox" id="aiToggle">
              <span class="slider"></span>
            </label>
            <div class="toggle-copy">
              <span class="toggle-title">AI Responses 🤖</span>
              <span id="aiToggleStatus" class="toggle-status">Enabled</span>
            </div>
          </div>
          <div class="toggle-row">
            <label class="switch">
              <input type="checkbox" id="autoPingToggle">
              <span class="slider"></span>
            </label>
            <div class="toggle-copy">
              <span class="toggle-title">Auto Ping Replies 🏓</span>
              <span id="autoPingToggleStatus" class="toggle-status">Enabled</span>
            </div>
          </div>
          <div class="mode-toggle">
            <div class="mode-header">
              <span class="mode-title">Inbound Messaging 📡</span>
              <span class="mode-status" id="modeStatus">Channels + DMs</span>
            </div>
            <div class="mode-buttons" role="radiogroup" aria-label="Inbound messaging mode">
              <button type="button" class="mode-btn" data-mode="both" aria-pressed="false">Channels + DMs</button>
              <button type="button" class="mode-btn" data-mode="dm_only" aria-pressed="false">DM only</button>
              <button type="button" class="mode-btn" data-mode="channel_only" aria-pressed="false">Channels only</button>
            </div>
          </div>
          <div class="passphrase-card">
            <label for="adminPassphrase">🔑 Admin Handoff Word</label>
            <div class="passphrase-input">
              <input type="text" id="adminPassphrase" name="adminPassphrase" placeholder="enter secret word" autocomplete="off" spellcheck="false">
              <button type="button" id="adminPassphraseSet" class="passphrase-set">Set</button>
            </div>
            <p class="passphrase-warning" id="adminPassphraseWarning" hidden>Saving a new word keeps the current admin whitelist. Use the admin list to remove anyone who should no longer have access.</p>
            <p class="passphrase-hint">Share this word privately; a DM containing only it grants admin powers instantly. <a href="#" id="adminListToggle" class="admin-list-link" role="button">admin list</a></p>
            <div class="admin-popover" id="adminListPopover" role="dialog" aria-labelledby="adminListTitle" hidden>
              <div class="admin-popover-header">
                <h3 id="adminListTitle">Admin Access</h3>
                <button type="button" class="admin-popover-close" id="adminListClose" aria-label="Close admin list">✕</button>
              </div>
              <div class="admin-popover-body">
                <p class="admin-empty" id="adminListEmpty">No admins currently authorized.</p>
                <ul class="admin-list" id="adminList"></ul>
              </div>
            </div>
          </div>
          <div class="command-groups" id="commandGroups"></div>
        </article>

        <article class="panel radio-panel" data-panel-id="radio-settings" data-draggable="true" data-collapsible="true">
          <div class="panel-header">
            <div class="panel-title">
              <h2>Radio Settings 📻</h2>
              <span class="panel-subtitle">LoRa hop limit and channels</span>
            </div>
            <button type="button" class="panel-collapse" aria-expanded="true" aria-controls="radioPanelBody" aria-label="Collapse panel"></button>
          </div>
          <div class="panel-body" id="radioPanelBody">
            <div class="config-section" aria-live="polite">
              <div class="config-row">
                <div class="config-key">
                  <div class="config-key-heading">
                    <strong>Hop Limit</strong>
                  </div>
                </div>
                <div class="config-value">
                  <div class="config-display-line">
                    <input type="number" min="0" max="7" id="radioHopLimit" class="config-input" style="width: 100px; display: inline-block;" aria-label="Hop limit">
                    <button type="button" id="radioHopSave" class="config-save-btn" style="margin-left: 8px;">Apply</button>
                    <span class="config-status" id="radioHopStatus"></span>
                  </div>
                </div>
              </div>

              <div class="config-row config-row-stack" id="radioChannelsRow">
                <div class="config-key-heading"><strong>Channels</strong></div>
                <div class="config-value">
                  <div id="radioChannelsList" class="config-table"></div>
                  <div style="margin-top: 8px; display:flex; gap:8px; align-items:center; flex-wrap: wrap;">
                    <button type="button" id="radioAddChannel" class="config-save-btn">Add Channel</button>
                    <span class="config-status" id="radioChannelsStatus"></span>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </article>

        <article class="panel config-panel" data-panel-id="config-overview" data-draggable="true" data-collapsible="true">
          <div class="panel-header">
            <div class="panel-title">
              <h2>Configuration Overview ⚙️</h2>
              <span class="panel-subtitle">Inspect current config.json values</span>
            </div>
            <button type="button" class="panel-collapse" aria-expanded="true" aria-controls="configOverviewBody" aria-label="Collapse panel"></button>
          </div>
          <div class="panel-body" id="configOverviewBody">
            <div class="config-select-row">
              <label for="configCategorySelect">Category</label>
              <select id="configCategorySelect" class="config-select"></select>
            </div>
            <div class="config-table" id="configSettingsList">
              <p class="config-empty">Config snapshot unavailable.</p>
            </div>
          </div>
        </article>
      </section>

      <section class="activity" data-panel-zone="activity">
        <article class="panel log-panel" data-panel-id="log" data-draggable="true">
          <div class="panel-header">
            <h2>Mesh Activity Stream 📡</h2>
            <div class="activity-header-meta">
              <div class="scroll-status on" id="scrollStatus"><span class="heartbeat-indicator" id="heartbeatIndicator">💓</span><span id="scrollLabel">Streaming</span></div>
              <span class="uptime-ticker" id="uptimeTicker">—</span>
            </div>
          </div>
          <div id="logbox" class="logbox">
"""
    page_html += initial_log_html
    page_html += r"""
          </div>
        </article>
      </section>
    </main>
  </div>

  <script>
    const METRICS_URL = "/dashboard/metrics";
    const METRICS_POLL_MS = 10000;
    const LOG_STREAM_MAX = 400;
    const LOG_RECONNECT_MAX = 8;
    const LOG_WAIT_THRESHOLD_MS = 30000;
    const CONFIG_UPDATE_URL = "/dashboard/config/update";
    const JS_ERROR_URL = "/dashboard/js-error";
    const RADIO_STATE_URL = "/dashboard/radio/state";
    const RADIO_HOPS_URL = "/dashboard/radio/hops";
    const RADIO_ADD_CHANNEL_URL = "/dashboard/radio/channel/add";
    const RADIO_UPDATE_CHANNEL_URL = "/dashboard/radio/channel/update";
    const RADIO_REMOVE_CHANNEL_URL = "/dashboard/radio/channel/remove";
    const RADIO_STATE_POLL_MS = 15000;
    const appShellEl = document.getElementById('appShell');
    const heartbeatIndicator = document.getElementById('heartbeatIndicator');
    let initialMetrics = {};
    if (appShellEl) {
      try {
        initialMetrics = JSON.parse(appShellEl.dataset.initialMetrics || '{}');
      } catch (err) {
        console.error('Initial metrics parse failed:', err);
        initialMetrics = {};
      }
    }

    let logEventSource = null;
    let logReconnectAttempts = 0;
    let logAutoScroll = true;
    let logUserScrolling = false;
    let logScrollTimeout = null;
    let logScrollBound = false;
    let logLastMessageAt = Date.now();
    let heartbeatTimer = null;
    let adminPopoverPreviousFocus = null;
    let adminPopoverOutsideHandler = null;
    let collapsedPanelState = new Set();
    let configOverviewState = { sections: [], selectedId: null, pendingData: null, metadata: {} };
    const CONFIG_LOCKED_KEYS = new Set(['ai_provider']);
    const DEFAULT_LANGUAGE_OPTIONS = [
      { value: 'english', label: 'English' },
      { value: 'spanish', label: 'Spanish' },
    ];
    const DEFAULT_PERSONALITY_OPTIONS = [{ value: 'trail_scout', label: 'Trail Scout' }];
    let configSelectInitialized = false;
    let radioState = { connected: false, radio_id: null, hops: null, channels: [] };

    function $(id) { return document.getElementById(id); }

    // --------------------
    // Radio Settings (UI)
    // --------------------
    function setRadioHopWidgetsEnabled(enabled) {
      const input = $("radioHopLimit");
      const btn = $("radioHopSave");
      if (input) input.disabled = !enabled;
      if (btn) btn.disabled = !enabled;
    }

    async function loadRadioState() {
      try {
        const res = await fetch(RADIO_STATE_URL, { method: 'GET' });
        const data = await res.json();
        if (!data || !data.radio) {
          throw new Error('Invalid radio payload');
        }
        radioState = data.radio;
        renderRadioPanel(radioState);
      } catch (err) {
        console.warn('Radio state failed:', err);
        const status = $("radioChannelsStatus");
        if (status) {
          status.textContent = 'Radio unavailable';
          status.setAttribute('data-tone', 'error');
        }
        setRadioHopWidgetsEnabled(false);
      }
    }

    function renderRadioPanel(state) {
      const hop = $("radioHopLimit");
      const hopStatus = $("radioHopStatus");
      setRadioHopWidgetsEnabled(!!state.connected);
      if (hop) {
        hop.value = (state && typeof state.hops === 'number') ? String(state.hops) : '';
      }
      if (hopStatus) {
        hopStatus.textContent = state.connected ? '' : 'Disconnected';
        hopStatus.removeAttribute('data-tone');
      }
      renderRadioChannels(state && Array.isArray(state.channels) ? state.channels : []);
    }

    function channelRoleBadge(role) {
      switch (String(role || 'DISABLED')) {
        case 'PRIMARY': return '<span class="config-type-badge">Primary</span>';
        case 'SECONDARY': return '<span class="config-type-badge">Secondary</span>';
        default: return '<span class="config-type-badge">Disabled</span>';
      }
    }

    function buildChannelRowHTML(ch) {
      const idx = Number(ch.index);
      const disabled = !ch.enabled;
      const deletable = !!ch.deletable;
      const name = ch.name || '';
      const psk = ch.psk || '—';
      const uplink = !!ch.uplink;
      const downlink = !!ch.downlink;
      const role = ch.role || 'DISABLED';
      return `
        <div class="config-row" data-ch-index="${idx}" ${disabled ? 'data-disabled="true"' : ''}>
          <div class="config-key">
            <div class="config-key-heading">
              <strong>Ch ${idx}</strong>
              ${channelRoleBadge(role)}
            </div>
          </div>
          <div class="config-value">
            <div class="config-display-line">
              <div style="display:flex; gap:10px; align-items:center; flex-wrap: wrap;">
                <label style="display:flex; gap:6px; align-items:center;">
                  <span style="min-width:36px; color:var(--text-secondary)">Name</span>
                  <input type="text" class="config-input" data-ch-field="name" style="width:220px;" value="${name.replace(/&/g,'&amp;').replace(/</g,'&lt;')}">
                </label>
                <label style="display:flex; gap:6px; align-items:center;">
                  <span style="min-width:36px; color:var(--text-secondary)">PSK</span>
                  <span data-ch-field="psk-label" title="Channel key">${psk}</span>
                  <button type="button" class="config-edit-btn" data-ch-action="psk-generate">Generate</button>
                  <button type="button" class="config-edit-btn" data-ch-action="psk-enter">Enter…</button>
                </label>
                <label class="switch" title="Uplink enabled">
                  <input type="checkbox" data-ch-field="uplink" ${uplink ? 'checked' : ''}>
                  <span class="slider"></span>
                </label>
                <label class="switch" title="Downlink enabled">
                  <input type="checkbox" data-ch-field="downlink" ${downlink ? 'checked' : ''}>
                  <span class="slider"></span>
                </label>
                <button type="button" class="config-save-btn" data-ch-action="save">Save</button>
                <button type="button" class="admin-remove-btn" data-ch-action="remove" ${deletable ? '' : 'disabled'}>Remove</button>
                <span class="config-status" data-ch-field="status"></span>
              </div>
            </div>
          </div>
        </div>`;
    }

    function renderRadioChannels(channels) {
      const host = $("radioChannelsList");
      if (!host) return;
      const list = Array.isArray(channels) ? channels : [];
      // Show only channels that actually exist on the radio (enabled)
      const active = list.filter(ch => !!ch && (ch.enabled === true));
      if (!active.length) {
        host.innerHTML = '<p class="config-empty">No active channels.</p>';
      } else {
        host.innerHTML = active.map(buildChannelRowHTML).join('');
      }
      // Wire actions
      host.querySelectorAll('[data-ch-action="save"]').forEach(btn => btn.addEventListener('click', onChannelSave));
      host.querySelectorAll('[data-ch-action="remove"]').forEach(btn => btn.addEventListener('click', onChannelRemove));
      host.querySelectorAll('[data-ch-action="psk-generate"]').forEach(btn => btn.addEventListener('click', onChannelPskGenerate));
      host.querySelectorAll('[data-ch-action="psk-enter"]').forEach(btn => btn.addEventListener('click', onChannelPskEnter));
    }

    async function onChannelSave(event) {
      const row = event.currentTarget.closest('.config-row[data-ch-index]');
      if (!row) return;
      const idx = Number(row.dataset.chIndex);
      const name = row.querySelector('[data-ch-field="name"]').value;
      const uplink = row.querySelector('[data-ch-field="uplink"]').checked;
      const downlink = row.querySelector('[data-ch-field="downlink"]').checked;
      const status = row.querySelector('[data-ch-field="status"]');
      status.textContent = 'Saving…';
      status.removeAttribute('data-tone');
      try {
        const res = await fetch(RADIO_UPDATE_CHANNEL_URL, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ index: idx, name, uplink, downlink })
        });
        const data = await res.json();
        if (!data || !data.ok) throw new Error(data && data.error ? data.error : 'Update failed');
        status.textContent = 'Saved';
        renderRadioStateDeferred();
      } catch (err) {
        status.textContent = String(err.message || err);
        status.setAttribute('data-tone', 'error');
      }
    }

    async function onChannelRemove(event) {
      const row = event.currentTarget.closest('.config-row[data-ch-index]');
      if (!row) return;
      const idx = Number(row.dataset.chIndex);
      const status = row.querySelector('[data-ch-field="status"]');
      status.textContent = 'Removing…';
      status.removeAttribute('data-tone');
      try {
        const res = await fetch(RADIO_REMOVE_CHANNEL_URL, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ index: idx })
        });
        const data = await res.json();
        if (!data || !data.ok) throw new Error(data && data.error ? data.error : 'Remove failed');
        status.textContent = 'Removed';
        renderRadioStateDeferred();
      } catch (err) {
        status.textContent = String(err.message || err);
        status.setAttribute('data-tone', 'error');
      }
    }

    async function onChannelPskGenerate(event) {
      const row = event.currentTarget.closest('.config-row[data-ch-index]');
      if (!row) return;
      const idx = Number(row.dataset.chIndex);
      const status = row.querySelector('[data-ch-field="status"]');
      const warn = '⚠️ Generating a new key will reset this channel\'s PSK and may desynchronize other radios until they update. Continue?';
      const ok = confirm(warn);
      if (!ok) {
        status.textContent = 'Cancelled';
        status.removeAttribute('data-tone');
        return;
      }
      status.textContent = 'Generating…';
      status.removeAttribute('data-tone');
      try {
        const res = await fetch(RADIO_UPDATE_CHANNEL_URL, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ index: idx, psk: 'random' })
        });
        const data = await res.json();
        if (!data || !data.ok) throw new Error(data && data.error ? data.error : 'Key generation failed');
        status.textContent = 'Key updated';
        renderRadioStateDeferred();
      } catch (err) {
        status.textContent = String(err.message || err);
        status.setAttribute('data-tone', 'error');
      }
    }

    async function onChannelPskEnter(event) {
      const row = event.currentTarget.closest('.config-row[data-ch-index]');
      if (!row) return;
      const idx = Number(row.dataset.chIndex);
      const value = prompt('Enter channel key (psk). Allowed: none, default, random, simpleN, hex/base64');
      if (value === null) return;
      const status = row.querySelector('[data-ch-field="status"]');
      status.textContent = 'Updating…';
      status.removeAttribute('data-tone');
      try {
        const res = await fetch(RADIO_UPDATE_CHANNEL_URL, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ index: idx, psk: value.trim() })
        });
        const data = await res.json();
        if (!data || !data.ok) throw new Error(data && data.error ? data.error : 'Update failed');
        status.textContent = 'Key updated';
        renderRadioStateDeferred();
      } catch (err) {
        status.textContent = String(err.message || err);
        status.setAttribute('data-tone', 'error');
      }
    }

    function initRadioPanel() {
      const hopBtn = $("radioHopSave");
      const hopInput = $("radioHopLimit");
      const addBtn = $("radioAddChannel");
      if (hopBtn) {
        hopBtn.addEventListener('click', async () => {
          const value = Number(hopInput && hopInput.value !== '' ? hopInput.value : NaN);
          const status = $("radioHopStatus");
          if (!Number.isFinite(value)) {
            if (status) { status.textContent = 'Enter a value 0–7'; status.setAttribute('data-tone', 'error'); }
            return;
          }
          if (status) { status.textContent = 'Saving…'; status.removeAttribute('data-tone'); }
          try {
            const res = await fetch(RADIO_HOPS_URL, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ hop_limit: value }) });
            const data = await res.json();
            if (!data || !data.ok) throw new Error(data && data.error ? data.error : 'Save failed');
            if (status) { status.textContent = 'Saved'; status.removeAttribute('data-tone'); }
            renderRadioStateDeferred();
          } catch (err) {
            if (status) { status.textContent = String(err.message || err); status.setAttribute('data-tone', 'error'); }
          }
        });
      }
      if (addBtn) {
        addBtn.addEventListener('click', async () => {
          const name = prompt('Name for the new channel?');
          const status = $("radioChannelsStatus");
          // If user cancels the prompt, do nothing and clear any stale status
          if (name === null) {
            if (status) { status.textContent = ''; status.removeAttribute('data-tone'); }
            return;
          }
          if (status) { status.textContent = 'Adding…'; status.removeAttribute('data-tone'); }
          try {
            const res = await fetch(RADIO_ADD_CHANNEL_URL, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ name: (name || '').trim() || undefined, psk: 'random' }) });
            const data = await res.json();
            if (!data || !data.ok) throw new Error(data && data.error ? data.error : 'Add failed');
            if (status) { status.textContent = 'Channel added'; }
            renderRadioStateDeferred();
          } catch (err) {
            if (status) { status.textContent = String(err.message || err); status.setAttribute('data-tone', 'error'); }
          }
        });
      }
    }

    let radioRenderTimer = null;
    function renderRadioStateDeferred() {
      if (radioRenderTimer) clearTimeout(radioRenderTimer);
      radioRenderTimer = setTimeout(loadRadioState, 400);
    }

    function formatPercent(value) {
      if (value === null || value === undefined || isNaN(value)) return "—";
      return value.toFixed(0) + "%";
    }

    function formatNumber(value) {
      if (value === null || value === undefined || isNaN(value)) return "—";
      return new Intl.NumberFormat().format(value);
    }

    function formatMs(value) {
      if (value === null || value === undefined || isNaN(value)) return "—";
      if (value >= 1000) {
        return (value / 1000).toFixed(2) + " s";
      }
      return value.toFixed(0) + " ms";
    }

    let tooltipEl = null;
    const tooltipState = {
      anchor: null,
      hideTimer: null,
      touchTimer: null,
    };

    function ensureTooltipHost() {
      if (tooltipEl && tooltipEl.isConnected) {
        return tooltipEl;
      }
      if (!tooltipEl) {
        tooltipEl = document.createElement('div');
        tooltipEl.className = 'dashboard-tooltip';
        tooltipEl.id = 'dashboardTooltip';
        tooltipEl.setAttribute('role', 'tooltip');
        tooltipEl.hidden = true;
      }
      const attach = () => {
        if (!document.body) {
          requestAnimationFrame(attach);
          return;
        }
        if (!tooltipEl.isConnected) {
          document.body.appendChild(tooltipEl);
        }
      };
      attach();
      return tooltipEl;
    }

    function hideTooltip() {
      const host = ensureTooltipHost();
      if (tooltipState.hideTimer) {
        clearTimeout(tooltipState.hideTimer);
        tooltipState.hideTimer = null;
      }
      if (!host.classList.contains('is-visible')) {
        host.hidden = true;
        host.textContent = '';
        tooltipState.anchor = null;
        return;
      }
      host.classList.remove('is-visible');
      tooltipState.hideTimer = setTimeout(() => {
        host.hidden = true;
        host.textContent = '';
        tooltipState.anchor = null;
      }, 140);
    }

    function positionTooltip(target, placement) {
      const host = ensureTooltipHost();
      const rect = target.getBoundingClientRect();
      let left = rect.left + rect.width / 2;
      let top = rect.top - 12;
      host.style.transform = 'translate(-50%, -8px)';
      if (placement === 'right') {
        left = rect.right + 12;
        top = rect.top + rect.height / 2;
        host.style.transform = 'translate(0, -50%)';
      } else if (placement === 'bottom') {
        left = rect.left + rect.width / 2;
        top = rect.bottom + 12;
        host.style.transform = 'translate(-50%, 0)';
      }
      host.style.left = `${Math.round(left)}px`;
      host.style.top = `${Math.round(top)}px`;
    }

    function showTooltip(target, text, options = {}) {
      if (!target || !text) {
        return;
      }
      const host = ensureTooltipHost();
      const placement = options.placement || target.dataset.explainerPlacement || 'top';
      if (tooltipState.hideTimer) {
        clearTimeout(tooltipState.hideTimer);
        tooltipState.hideTimer = null;
      }
      host.textContent = text;
      host.hidden = false;
      positionTooltip(target, placement);
      requestAnimationFrame(() => {
        host.classList.add('is-visible');
      });
      tooltipState.anchor = target;
    }

    function handleExplainerEnter(event) {
      const el = event.currentTarget;
      const text = el && el.dataset ? el.dataset.explainer : '';
      if (!text) return;
      showTooltip(el, text);
    }

    function handleExplainerLeave() {
      hideTooltip();
    }

    function handleExplainerTouch(event) {
      const el = event.currentTarget;
      const text = el && el.dataset ? el.dataset.explainer : '';
      if (!text) return;
      showTooltip(el, text);
      if (tooltipState.touchTimer) {
        clearTimeout(tooltipState.touchTimer);
      }
      tooltipState.touchTimer = setTimeout(() => {
        hideTooltip();
      }, 2500);
    }

    function bindExplainer(element, text, options = {}) {
      if (!element || !text) {
        return;
      }
      ensureTooltipHost();
      element.dataset.explainer = text;
      if (options.placement) {
        element.dataset.explainerPlacement = options.placement;
      }
      element.setAttribute('aria-describedby', 'dashboardTooltip');
      element.addEventListener('mouseenter', handleExplainerEnter);
      element.addEventListener('mouseleave', handleExplainerLeave);
      element.addEventListener('focus', handleExplainerEnter);
      element.addEventListener('blur', handleExplainerLeave);
      element.addEventListener('touchstart', handleExplainerTouch, { passive: true });
    }

    window.addEventListener('scroll', () => {
      if (!tooltipState.anchor) {
        return;
      }
      hideTooltip();
    }, true);
    window.addEventListener('resize', hideTooltip);

    function reportJsIssue(kind, details) {
      const payload = {
        kind,
        url: window.location.href,
        userAgent: navigator.userAgent,
        timestamp: Date.now(),
        details,
      };
      try {
        const blob = new Blob([JSON.stringify(payload)], { type: 'application/json' });
        if (navigator.sendBeacon && navigator.sendBeacon(JS_ERROR_URL, blob)) {
          return;
        }
      } catch (err) {}
      try {
        fetch(JS_ERROR_URL, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
          keepalive: true,
        }).catch(() => {});
      } catch (err) {}
    }

    function bindGlobalErrorHandlers() {
      window.addEventListener('error', event => {
        if (!event) return;
        const info = {
          message: event.message || String(event.error || 'Unknown error'),
          source: event.filename || null,
          lineno: event.lineno || null,
          colno: event.colno || null,
          stack: event.error && event.error.stack ? String(event.error.stack) : null,
        };
        reportJsIssue('error', info);
      });
      window.addEventListener('unhandledrejection', event => {
        if (!event) return;
        const reason = event.reason;
        const info = {
          message: reason && reason.message ? String(reason.message) : String(reason || 'Promise rejection'),
          stack: reason && reason.stack ? String(reason.stack) : null,
        };
        reportJsIssue('unhandledrejection', info);
      });
    }

    function setValueWithDelta(id, payload, { allowNegative = true } = {}) {
      const el = $(id);
      if (!el) return;
      if (!payload || payload.value === null || payload.value === undefined || isNaN(payload.value)) {
        el.textContent = '—';
        return;
      }
      const valueNumber = Number(payload.value);
      const valueText = formatNumber(valueNumber);
      let deltaHtml = '';
      const deltaValue = payload.delta;
      if (deltaValue === null || deltaValue === undefined || isNaN(deltaValue)) {
        deltaHtml = '<span class="delta delta-flat">—</span>';
      } else if (deltaValue > 0) {
        deltaHtml = `<span class="delta delta-up">▲ ${formatNumber(deltaValue)}</span>`;
      } else if (deltaValue < 0 && allowNegative) {
        deltaHtml = `<span class="delta delta-down">▼ ${formatNumber(Math.abs(deltaValue))}</span>`;
      } else {
        deltaHtml = '<span class="delta delta-flat">—</span>';
      }
      el.innerHTML = `<span class="stat-value-number">${valueText}</span>${deltaHtml}`;
    }

    const PANEL_LAYOUT_STORAGE_KEY = 'mesh-dashboard-layout-v1';
    const COLLAPSE_STORAGE_KEY = 'mesh-dashboard-collapsed-v1';
    let dragPanelRef = null;
    const dragPlaceholder = document.createElement('div');
    dragPlaceholder.className = 'panel-placeholder';

    function savePanelLayout() {
      const layout = {};
      document.querySelectorAll('[data-panel-zone]').forEach(zone => {
        const zoneId = zone.dataset.panelZone;
        if (!zoneId) return;
        layout[zoneId] = Array.from(zone.querySelectorAll('.panel[data-panel-id]')).map(panel => panel.dataset.panelId);
      });
      try {
        localStorage.setItem(PANEL_LAYOUT_STORAGE_KEY, JSON.stringify(layout));
      } catch (err) {
        console.warn('Could not persist panel layout', err);
      }
    }

    function applySavedPanelLayout() {
      let saved;
      try {
        saved = localStorage.getItem(PANEL_LAYOUT_STORAGE_KEY);
      } catch (err) {
        console.warn('Could not read saved layout', err);
        return;
      }
      if (!saved) {
        return;
      }
      let layout;
      try {
        layout = JSON.parse(saved);
      } catch (err) {
        console.warn('Invalid saved layout payload', err);
        return;
      }
      if (!layout || typeof layout !== 'object') {
        return;
      }
      Object.entries(layout).forEach(([zoneId, ids]) => {
        if (!Array.isArray(ids)) {
          return;
        }
        const zone = document.querySelector(`[data-panel-zone="${zoneId}"]`);
        if (!zone) {
          return;
        }
        ids.forEach(id => {
          const panel = document.querySelector(`.panel[data-panel-id="${id}"]`);
          if (panel) {
            zone.appendChild(panel);
          }
        });
      });
    }

    function loadCollapsedPanelState() {
      try {
        const raw = localStorage.getItem(COLLAPSE_STORAGE_KEY);
        if (!raw) {
          return new Set();
        }
        const parsed = JSON.parse(raw);
        if (!Array.isArray(parsed)) {
          return new Set();
        }
        const filtered = parsed.filter(id => typeof id === 'string' && id);
        return new Set(filtered);
      } catch (err) {
        console.warn('Could not read collapse state', err);
        return new Set();
      }
    }

    function persistCollapsedPanelState() {
      try {
        localStorage.setItem(COLLAPSE_STORAGE_KEY, JSON.stringify(Array.from(collapsedPanelState)));
      } catch (err) {
        console.warn('Could not persist collapse state', err);
      }
    }

    function applyPanelCollapseState(panel, collapsed) {
      if (!panel) {
        return;
      }
      const panelId = panel.dataset.panelId || '';
      panel.classList.toggle('collapsed', collapsed);
      const button = panel.querySelector('.panel-collapse');
      if (button) {
        button.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
        button.setAttribute('aria-label', collapsed ? 'Expand panel' : 'Collapse panel');
        const targetId = button.getAttribute('aria-controls');
        if (targetId) {
          const body = document.getElementById(targetId);
          if (body) {
            body.hidden = collapsed;
          }
        }
      }
      if (panelId) {
        if (collapsed) {
          collapsedPanelState.add(panelId);
        } else {
          collapsedPanelState.delete(panelId);
        }
      }
    }

    function onPanelCollapseToggle(event) {
      event.preventDefault();
      const button = event.currentTarget;
      const panel = button ? button.closest('.panel[data-panel-id]') : null;
      if (!panel) {
        return;
      }
      const shouldCollapse = !panel.classList.contains('collapsed');
      applyPanelCollapseState(panel, shouldCollapse);
      persistCollapsedPanelState();
    }

    function initPanelCollapse() {
      collapsedPanelState = loadCollapsedPanelState();
      document.querySelectorAll('.panel[data-collapsible="true"]').forEach(panel => {
        const button = panel.querySelector('.panel-collapse');
        if (!button) {
          return;
        }
        button.addEventListener('click', onPanelCollapseToggle);
        const panelId = panel.dataset.panelId || '';
        const collapsed = panelId ? collapsedPanelState.has(panelId) : false;
        applyPanelCollapseState(panel, collapsed);
      });
      persistCollapsedPanelState();
    }

    function panelAfterCursor(zone, y) {
      const panels = Array.from(zone.querySelectorAll('.panel[data-panel-id]:not(.is-dragging)'));
      for (const panel of panels) {
        const rect = panel.getBoundingClientRect();
        if (y < rect.top + rect.height / 2) {
          return panel;
        }
      }
      return null;
    }

    function onPanelDragStart(event) {
      const panel = event.currentTarget;
      const header = event.target.closest('.panel-header');
      if (!header) {
        event.preventDefault();
        return;
      }
      dragPanelRef = panel;
      panel.classList.add('is-dragging');
      try {
        event.dataTransfer.effectAllowed = 'move';
        event.dataTransfer.setData('text/plain', panel.dataset.panelId || 'panel');
      } catch (err) {}
      dragPlaceholder.style.height = `${panel.offsetHeight}px`;
    }

    function onPanelDragEnd() {
      if (dragPanelRef) {
        dragPanelRef.classList.remove('is-dragging');
      }
      dragPanelRef = null;
      dragPlaceholder.remove();
      document.querySelectorAll('[data-panel-zone]').forEach(zone => zone.classList.remove('drop-active'));
      savePanelLayout();
    }

    function onZoneDragOver(event) {
      if (!dragPanelRef) {
        return;
      }
      event.preventDefault();
      try {
        event.dataTransfer.dropEffect = 'move';
      } catch (err) {}
      const zone = event.currentTarget;
      zone.classList.add('drop-active');
      const after = panelAfterCursor(zone, event.clientY);
      if (!dragPlaceholder.parentElement || dragPlaceholder.parentElement !== zone) {
        zone.appendChild(dragPlaceholder);
      }
      if (after) {
        zone.insertBefore(dragPlaceholder, after);
      } else {
        zone.appendChild(dragPlaceholder);
      }
    }

    function onZoneDrop(event) {
      if (!dragPanelRef) {
        return;
      }
      event.preventDefault();
      const zone = event.currentTarget;
      const reference = dragPlaceholder.parentElement === zone ? dragPlaceholder : null;
      if (reference) {
        zone.insertBefore(dragPanelRef, reference);
      } else {
        zone.appendChild(dragPanelRef);
      }
      dragPlaceholder.remove();
      zone.classList.remove('drop-active');
      savePanelLayout();
    }

    function onZoneDragLeave(event) {
      const zone = event.currentTarget;
      if (!dragPanelRef) {
        return;
      }
      const rect = zone.getBoundingClientRect();
      const outside = event.clientX < rect.left || event.clientX > rect.right || event.clientY < rect.top || event.clientY > rect.bottom;
      if (outside) {
        zone.classList.remove('drop-active');
        if (dragPlaceholder.parentElement === zone) {
          dragPlaceholder.remove();
        }
      }
    }

    function initPanelDrag() {
      document.querySelectorAll('.panel[data-panel-id]').forEach(panel => {
        panel.setAttribute('draggable', 'true');
        panel.addEventListener('dragstart', onPanelDragStart);
        panel.addEventListener('dragend', onPanelDragEnd);
      });
      document.querySelectorAll('[data-panel-zone]').forEach(zone => {
        zone.addEventListener('dragover', onZoneDragOver);
        zone.addEventListener('drop', onZoneDrop);
        zone.addEventListener('dragleave', onZoneDragLeave);
      });
      document.querySelectorAll('.panel[data-panel-id] .panel-header').forEach(header => {
        header.classList.add('panel-drag-handle');
      });
      applySavedPanelLayout();
      savePanelLayout();
    }

    function normalizeCommandName(cmd) {
      if (!cmd) return '';
      let text = String(cmd).trim();
      if (!text) return '';
      if (!text.startsWith('/')) {
        text = '/' + text.replace(/^\/+/, '');
      }
      return text.toLowerCase();
    }

    function normalizeMessageMode(mode) {
      if (!mode) return 'both';
      const text = String(mode).trim().toLowerCase();
      if (['both', 'dm_only', 'channel_only'].includes(text)) return text;
      if (['dm', 'direct'].includes(text)) return 'dm_only';
      if (['channels', 'channel'].includes(text)) return 'channel_only';
      return 'both';
    }

    function messageModeLabel(mode) {
      switch (mode) {
        case 'dm_only':
          return 'DM only';
        case 'channel_only':
          return 'Channels only';
        default:
          return 'Channels + DMs';
      }
    }

    function detectConfigType(value) {
      if (value === null) return 'null';
      if (Array.isArray(value)) return 'array';
      return typeof value;
    }

    function formatConfigInputValue(value) {
      if (value === null) {
        return 'null';
      }
      if (typeof value === 'string') {
        return value;
      }
      if (value === undefined) {
        return '';
      }
      try {
        return JSON.stringify(value, null, 2);
      } catch (err) {
        return String(value);
      }
    }

    function adjustConfigInputRows(textarea) {
      if (!textarea) {
        return;
      }
      const lines = textarea.value.split('\\n').length;
      textarea.rows = Math.min(8, Math.max(2, lines));
    }

    function isConfigEditing() {
      return document.querySelector('.config-row.is-editing') !== null;
    }

    function maybeApplyPendingConfigData() {
      if (!configOverviewState.pendingData) {
        return;
      }
      if (isConfigEditing()) {
        return;
      }
      const pending = configOverviewState.pendingData;
      configOverviewState.pendingData = null;
      renderConfigOverview(pending);
    }

    async function submitConfigUpdate(key, valueText) {
      let response;
      try {
        response = await fetch(CONFIG_UPDATE_URL, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ key, value: valueText })
        });
      } catch (err) {
        throw new Error('Network error');
      }
      let data = {};
      try {
        data = await response.json();
      } catch (err) {
        data = {};
      }
      if (!response.ok || !data || data.ok === false) {
        const message = data && data.error ? data.error : `HTTP ${response.status}`;
        throw new Error(message);
      }
      return data.entry || {};
    }

    function createConfigRow(item) {
      function buildConfigEditor(dataItem) {
        const meta = configOverviewState.metadata || {};
        const kind = dataItem && (dataItem.type || detectConfigType(dataItem.raw));

        if (dataItem && dataItem.key === 'language_selection') {
          const select = document.createElement('select');
          select.className = 'config-select';
          const provided = Array.isArray(meta.language_options) && meta.language_options.length
            ? meta.language_options
            : DEFAULT_LANGUAGE_OPTIONS;
          const seen = new Set();
          const ensureOption = (value, label) => {
            const normalized = (value ?? '').toString();
            if (!normalized || seen.has(normalized)) {
              return;
            }
            const optionEl = document.createElement('option');
            optionEl.value = normalized;
            optionEl.textContent = label || normalized;
            select.appendChild(optionEl);
            seen.add(normalized);
          };
          provided.forEach(opt => {
            if (!opt || typeof opt !== 'object') return;
            ensureOption(opt.value, opt.label || opt.value);
          });
          let currentValue = typeof dataItem.raw === 'string' ? dataItem.raw : '';
          if (currentValue && !seen.has(currentValue)) {
            ensureOption(currentValue, currentValue);
          }
          if (!currentValue && select.options.length) {
            currentValue = select.options[0].value;
          }
          select.value = currentValue;
          return {
            element: select,
            focus: () => select.focus(),
            getValue: () => select.value,
            setValue: value => {
              let next = typeof value === 'string' ? value : '';
              if (next && !seen.has(next)) {
                ensureOption(next, next);
              }
              if (!next && select.options.length) {
                next = select.options[0].value;
              }
              select.value = next;
            },
            setDisabled: state => { select.disabled = !!state; },
          };
        }

        if (dataItem && dataItem.key === 'default_personality_id') {
          const select = document.createElement('select');
          select.className = 'config-select';
          const provided = Array.isArray(meta.personality_options) && meta.personality_options.length
            ? meta.personality_options
            : DEFAULT_PERSONALITY_OPTIONS.slice();
          const fallbackValue = typeof dataItem.raw === 'string' && dataItem.raw ? dataItem.raw : '';
          if (!provided.length && fallbackValue) {
            provided.push({ value: fallbackValue, label: fallbackValue });
          }
          const seen = new Set();
          const ensureOption = (value, label) => {
            const normalized = (value ?? '').toString();
            if (!normalized || seen.has(normalized)) {
              return;
            }
            const optionEl = document.createElement('option');
            optionEl.value = normalized;
            optionEl.textContent = label || normalized;
            select.appendChild(optionEl);
            seen.add(normalized);
          };
          provided.forEach(opt => {
            if (!opt || typeof opt !== 'object') return;
            ensureOption(opt.value, opt.label || opt.value);
          });
          let currentValue = typeof dataItem.raw === 'string' ? dataItem.raw : '';
          if (currentValue && !seen.has(currentValue)) {
            ensureOption(currentValue, currentValue);
          }
          if (!currentValue && select.options.length) {
            currentValue = select.options[0].value;
          }
          select.value = currentValue;
          return {
            element: select,
            focus: () => select.focus(),
            getValue: () => select.value,
            setValue: value => {
              let next = typeof value === 'string' ? value : '';
              if (next && !seen.has(next)) {
                ensureOption(next, next);
              }
              if (!next && select.options.length) {
                next = select.options[0].value;
              }
              select.value = next;
            },
            setDisabled: state => { select.disabled = !!state; },
          };
        }

        if (kind === 'automessage_quiet_hours') {
          const wrapper = document.createElement('div');
          wrapper.className = 'config-quiet-hours';

          const hourOptions = Array.isArray(meta.hour_options) && meta.hour_options.length
            ? meta.hour_options
            : Array.from({ length: 24 }, (_, h) => ({ value: h, label: `${String(h).padStart(2, '0')}:00` }));

          const buildHourSelect = value => {
            const select = document.createElement('select');
            select.className = 'config-select quiet-select';
            const seen = new Set();
            const ensureOption = (optValue, label) => {
              const normalized = Number(optValue) % 24;
              if (seen.has(normalized)) {
                return;
              }
              const optionEl = document.createElement('option');
              optionEl.value = normalized.toString();
              optionEl.textContent = label || `${String(normalized).padStart(2, '0')}:00`;
              select.appendChild(optionEl);
              seen.add(normalized);
            };
            hourOptions.forEach(opt => {
              if (!opt || typeof opt !== 'object') return;
              ensureOption(opt.value, opt.label);
            });
            if (value !== undefined && value !== null) {
              ensureOption(value, `${String(Number(value) % 24).padStart(2, '0')}:00`);
            }
            if (!select.options.length) {
              ensureOption(0, '00:00');
            }
            const normalizedValue = value !== undefined && value !== null ? Number(value) % 24 : Number(select.options[0].value);
            select.value = normalizedValue.toString();
            return select;
          };

          const toggleLabel = document.createElement('label');
          toggleLabel.className = 'quiet-toggle';
          const toggle = document.createElement('input');
          toggle.type = 'checkbox';
          const toggleText = document.createElement('span');
          toggleText.textContent = 'Enable quiet hours';
          toggleLabel.appendChild(toggle);
          toggleLabel.appendChild(toggleText);

          const rangeWrap = document.createElement('div');
          rangeWrap.className = 'quiet-range';
          const rangeLabel = document.createElement('span');
          rangeLabel.textContent = 'Quiet from';
          const startSelect = buildHourSelect(20);
          const toLabel = document.createElement('span');
          toLabel.textContent = 'to';
          const endSelect = buildHourSelect(8);

          rangeWrap.appendChild(rangeLabel);
          rangeWrap.appendChild(startSelect);
          rangeWrap.appendChild(toLabel);
          rangeWrap.appendChild(endSelect);

          wrapper.appendChild(toggleLabel);
          wrapper.appendChild(rangeWrap);

          const updateDisabled = () => {
            const disabled = !toggle.checked;
            startSelect.disabled = disabled;
            endSelect.disabled = disabled;
          };

          const applyValue = value => {
            let parsed = value;
            if (typeof parsed === 'string') {
              try {
                parsed = JSON.parse(parsed);
              } catch (err) {
                parsed = {};
              }
            }
            if (!parsed || typeof parsed !== 'object') {
              parsed = {};
            }
            const enabled = parsed.enabled === true || parsed.enabled === 'true';
            const start = parsed.start !== undefined ? Number(parsed.start) % 24 : Number(startSelect.value);
            const end = parsed.end !== undefined ? Number(parsed.end) % 24 : Number(endSelect.value);
            if (typeof start === 'number' && !Number.isNaN(start)) {
              startSelect.value = (start % 24).toString();
            }
            if (typeof end === 'number' && !Number.isNaN(end)) {
              endSelect.value = (end % 24).toString();
            }
            toggle.checked = enabled;
            updateDisabled();
          };

          toggle.addEventListener('change', updateDisabled);

          applyValue(dataItem.raw);

          return {
            element: wrapper,
            focus: () => toggle.focus(),
            getValue: () => ({
              enabled: toggle.checked,
              start: Number(startSelect.value) % 24,
              end: Number(endSelect.value) % 24,
            }),
            setValue: applyValue,
            setDisabled: state => {
              const disabled = !!state;
              toggle.disabled = disabled;
              startSelect.disabled = disabled || !toggle.checked;
              endSelect.disabled = disabled || !toggle.checked;
            },
          };
        }

        if (dataItem && dataItem.key === 'resend_dm_only') {
          // Special radio: DM only vs Channels + DMs
          const group = document.createElement('div');
          group.className = 'config-bool-group';
          const uniqueName = `cfg-resend-scope-${Math.random().toString(36).slice(2)}`;

          const dmOption = document.createElement('label');
          dmOption.className = 'config-bool-option';
          const dmInput = document.createElement('input');
          dmInput.type = 'radio';
          dmInput.name = uniqueName;
          dmInput.value = 'dm';
          const dmText = document.createElement('span');
          dmText.textContent = 'DM only';
          dmOption.appendChild(dmInput);
          dmOption.appendChild(dmText);

          const bothOption = document.createElement('label');
          bothOption.className = 'config-bool-option';
          const bothInput = document.createElement('input');
          bothInput.type = 'radio';
          bothInput.name = uniqueName;
          bothInput.value = 'both';
          const bothText = document.createElement('span');
          bothText.textContent = 'Channels + DMs';
          bothOption.appendChild(bothInput);
          bothOption.appendChild(bothText);

          group.appendChild(dmOption);
          group.appendChild(bothOption);

          const setValue = value => {
            const asBool = value === true || value === 'true' || value === 1 || value === '1';
            dmInput.checked = !!asBool;
            bothInput.checked = !asBool;
          };

          setValue(dataItem.raw);

          return {
            element: group,
            focus: () => (dmInput.checked ? dmInput : bothInput).focus(),
            getValue: () => (dmInput.checked ? 'true' : 'false'),
            setValue,
            setDisabled: state => {
              dmInput.disabled = !!state;
              bothInput.disabled = !!state;
            },
          };
        }

        if (kind === 'boolean') {
          const group = document.createElement('div');
          group.className = 'config-bool-group';
          const uniqueName = `cfg-${(dataItem.key || 'setting').replace(/[^a-z0-9]/gi, '')}-${Math.random().toString(36).slice(2)}`;

          const trueOption = document.createElement('label');
          trueOption.className = 'config-bool-option';
          const trueInput = document.createElement('input');
          trueInput.type = 'radio';
          trueInput.name = uniqueName;
          trueInput.value = 'true';
          const trueText = document.createElement('span');
          trueText.textContent = 'Enabled';
          trueOption.appendChild(trueInput);
          trueOption.appendChild(trueText);

          const falseOption = document.createElement('label');
          falseOption.className = 'config-bool-option';
          const falseInput = document.createElement('input');
          falseInput.type = 'radio';
          falseInput.name = uniqueName;
          falseInput.value = 'false';
          const falseText = document.createElement('span');
          falseText.textContent = 'Disabled';
          falseOption.appendChild(falseInput);
          falseOption.appendChild(falseText);

          group.appendChild(trueOption);
          group.appendChild(falseOption);

          const setValue = value => {
            const normalized = value === true || value === 'true' || value === 1 || value === '1';
            trueInput.checked = !!normalized;
            falseInput.checked = !normalized;
          };

          setValue(dataItem.raw);

          return {
            element: group,
            focus: () => (trueInput.checked ? trueInput : falseInput).focus(),
            getValue: () => (trueInput.checked ? 'true' : 'false'),
            setValue,
            setDisabled: state => {
              trueInput.disabled = !!state;
              falseInput.disabled = !!state;
            },
          };
        }

        const textarea = document.createElement('textarea');
        textarea.className = 'config-input';
        textarea.value = formatConfigInputValue(dataItem.raw);
        textarea.setAttribute('spellcheck', 'false');
        textarea.dataset.key = dataItem.key || '';
        adjustConfigInputRows(textarea);
        textarea.addEventListener('input', () => adjustConfigInputRows(textarea));
        return {
          element: textarea,
          focus: () => {
            textarea.focus();
            const len = textarea.value.length;
            try { textarea.setSelectionRange(len, len); } catch (err) {}
          },
          getValue: () => textarea.value,
          setValue: value => {
            textarea.value = formatConfigInputValue(value);
            adjustConfigInputRows(textarea);
          },
          setDisabled: state => { textarea.disabled = !!state; },
        };
      }
      // reset defaults button removed

      const row = document.createElement('div');
      row.className = 'config-row';
      row.dataset.key = item.key || '';

      let infoBtn = null;
      const keySpan = document.createElement('span');
      keySpan.className = 'config-key';
      const keyHeading = document.createElement('div');
      keyHeading.className = 'config-key-heading';
      const keyLabel = document.createElement('span');
      keyLabel.className = 'config-key-label';
      keyLabel.textContent = item.label || item.key || '(unknown)';
      keyHeading.appendChild(keyLabel);
      if (item.explainer) {
        infoBtn = document.createElement('button');
        infoBtn.type = 'button';
        infoBtn.className = 'config-info';
        infoBtn.textContent = 'i';
        infoBtn.setAttribute('aria-label', `About ${item.label || item.key || 'this setting'}`);
        keyHeading.appendChild(infoBtn);
      }
      keySpan.appendChild(keyHeading);
      if (item.key) {
        const keyCode = document.createElement('span');
        keyCode.className = 'config-key-code';
        keyCode.textContent = item.key;
        keySpan.appendChild(keyCode);
      }
      if (item.explainer) {
        keySpan.classList.add('has-explainer');
        bindExplainer(keySpan, item.explainer, { placement: 'right' });
        if (infoBtn) {
          bindExplainer(infoBtn, item.explainer);
        }
      }

      const valueWrap = document.createElement('div');
      valueWrap.className = 'config-value';

      const displayLine = document.createElement('div');
      displayLine.className = 'config-display-line';

      const displaySpan = document.createElement('span');
      displaySpan.className = 'config-display';
      displaySpan.textContent = item.value || '—';
      if (item.tooltip && item.tooltip !== item.value) {
        displaySpan.title = item.tooltip;
      }

      const isLocked = CONFIG_LOCKED_KEYS.has(item.key);
      const editorControl = !isLocked ? buildConfigEditor(item || {}) : null;
      const isEditable = !!editorControl;

      let editBtn = null;
      if (isEditable) {
        editBtn = document.createElement('button');
        editBtn.type = 'button';
        editBtn.className = 'config-edit-btn';
        editBtn.textContent = 'Edit';
      }

      displayLine.appendChild(displaySpan);
      if (isEditable && editBtn) {
        displayLine.appendChild(editBtn);
      }

      valueWrap.appendChild(displayLine);
      row.appendChild(keySpan);
      row.appendChild(valueWrap);

      let editArea = null;
      let status = null;
      let saveBtn = null;
      let cancelBtn = null;

      function setStatus(message, tone) {
        if (!status) {
          return;
        }
        status.textContent = message || '';
        if (tone) {
          status.dataset.tone = tone;
        } else {
          status.removeAttribute('data-tone');
        }
      }

      function refreshDisplay() {
        displaySpan.textContent = item.value || '—';
        if (item.tooltip && item.tooltip !== item.value) {
          displaySpan.title = item.tooltip;
        } else {
          displaySpan.removeAttribute('title');
        }
        keyLabel.textContent = item.label || item.key || '(unknown)';
        if (item.explainer) {
          keySpan.classList.add('has-explainer');
          keySpan.dataset.explainer = item.explainer;
          if (infoBtn) {
            infoBtn.dataset.explainer = item.explainer;
          }
        } else {
          keySpan.classList.remove('has-explainer');
          keySpan.removeAttribute('data-explainer');
          if (infoBtn) {
            infoBtn.removeAttribute('data-explainer');
          }
        }
      }

      refreshDisplay();

      if (isEditable && editorControl) {
        editArea = document.createElement('div');
        editArea.className = 'config-edit-area';
        editArea.hidden = true;

        const actions = document.createElement('div');
        actions.className = 'config-actions';

        cancelBtn = document.createElement('button');
        cancelBtn.type = 'button';
        cancelBtn.className = 'config-cancel-btn';
        cancelBtn.textContent = 'Cancel';

        saveBtn = document.createElement('button');
        saveBtn.type = 'button';
        saveBtn.className = 'config-save-btn';
        saveBtn.textContent = 'Save';

        status = document.createElement('span');
        status.className = 'config-status';

        actions.appendChild(cancelBtn);
        actions.appendChild(saveBtn);
        editArea.appendChild(editorControl.element);
        editArea.appendChild(actions);
        editArea.appendChild(status);
        valueWrap.appendChild(editArea);

        function exitEdit(revertValue) {
          row.classList.remove('is-editing');
          editArea.hidden = true;
          displayLine.removeAttribute('aria-hidden');
          if (revertValue) {
            editorControl.setValue(item.raw);
          }
          setStatus('', null);
          maybeApplyPendingConfigData();
        }

        function enterEdit() {
          document.querySelectorAll('.config-row.is-editing').forEach(other => {
            if (other !== row) {
              const cancel = other.querySelector('.config-cancel-btn');
              if (cancel) {
                cancel.click();
              }
            }
          });
          row.classList.add('is-editing');
          editArea.hidden = false;
          displayLine.setAttribute('aria-hidden', 'true');
          setStatus('', null);
          requestAnimationFrame(() => {
            editorControl.focus();
          });
        }

        async function handleSave() {
          if (row.dataset.saving === 'true') {
            return;
          }
          row.dataset.saving = 'true';
          if (saveBtn) saveBtn.disabled = true;
          if (cancelBtn) cancelBtn.disabled = true;
          if (editBtn) editBtn.disabled = true;
          if (editorControl.setDisabled) editorControl.setDisabled(true);
          setStatus('Saving…', null);
          try {
            const valueText = editorControl.getValue();
            const entry = await submitConfigUpdate(item.key, valueText);
            if (entry && typeof entry === 'object') {
              item.raw = entry.hasOwnProperty('raw') ? entry.raw : item.raw;
              item.value = entry.hasOwnProperty('value') ? entry.value : item.value;
              item.tooltip = entry.hasOwnProperty('tooltip') ? entry.tooltip : item.tooltip;
              item.label = entry.hasOwnProperty('label') ? entry.label : item.label;
              item.explainer = entry.hasOwnProperty('explainer') ? entry.explainer : item.explainer;
              item.type = entry.type || detectConfigType(item.raw);
              refreshDisplay();
              editorControl.setValue(item.raw);
            }
            setStatus('Saved', 'success');
            configOverviewState.pendingData = null;
            setTimeout(() => {
              setStatus('', null);
              exitEdit(false);
              loadMetrics();
            }, 400);
          } catch (err) {
            console.error('Config update failed:', err);
            setStatus(err && err.message ? err.message : 'Save failed', 'error');
          } finally {
            delete row.dataset.saving;
            if (saveBtn) saveBtn.disabled = false;
            if (cancelBtn) cancelBtn.disabled = false;
            if (editBtn) editBtn.disabled = false;
            if (editorControl.setDisabled) editorControl.setDisabled(false);
          }
        }

        editBtn.addEventListener('click', enterEdit);
        cancelBtn.addEventListener('click', () => exitEdit(true));
        saveBtn.addEventListener('click', handleSave);
        editArea.addEventListener('keydown', event => {
          if (event.key === 'Escape') {
            event.preventDefault();
            exitEdit(true);
          } else if ((event.ctrlKey || event.metaKey) && event.key === 'Enter') {
            event.preventDefault();
            handleSave();
          }
        });
      }

      return row;
    }

    function renderConfigSection(sectionId) {
      const list = $("configSettingsList");
      if (!list) {
        return;
      }
      list.innerHTML = '';
      if (!configOverviewState.sections.length) {
        list.innerHTML = '<p class="config-empty">No settings available.</p>';
        return;
      }
      let current = configOverviewState.sections.find(section => section.id === sectionId);
      if (!current) {
        current = configOverviewState.sections[0];
      }
      if (!current) {
        list.innerHTML = '<p class="config-empty">No settings available.</p>';
        return;
      }
      configOverviewState.selectedId = current.id;
      if (!Array.isArray(current.settings) || !current.settings.length) {
        list.innerHTML = '<p class="config-empty">No values configured for this category.</p>';
        return;
      }
      current.settings.forEach(item => {
        const row = createConfigRow(item);
        list.appendChild(row);
      });
    }

    function onConfigCategoryChange(event) {
      const select = event.currentTarget;
      if (!select) return;
      renderConfigSection(select.value);
    }

    function renderConfigOverview(data) {
      if (isConfigEditing()) {
        configOverviewState.pendingData = data;
        return;
      }
      configOverviewState.pendingData = null;
      const select = $("configCategorySelect");
      const list = $("configSettingsList");
      if (!select || !list) {
        return;
      }
      configOverviewState.metadata = data && data.metadata && typeof data.metadata === 'object'
        ? data.metadata
        : {};
      const sections = Array.isArray(data && data.sections) ? data.sections : [];
      configOverviewState.sections = sections.map((section, index) => {
        const safeId = section && section.id ? String(section.id) : `section-${index}`;
        const rawSettings = Array.isArray(section && section.settings) ? section.settings : [];
        const normalized = rawSettings.map((setting, idx) => {
          const key = setting && setting.key ? String(setting.key) : `item-${index}-${idx}`;
          const hasRaw = setting && Object.prototype.hasOwnProperty.call(setting, 'raw');
          const rawValue = hasRaw ? setting.raw : (setting ? setting.value : null);
          const valueText = setting && typeof setting.value === 'string'
            ? setting.value
            : (setting && setting.value !== undefined && setting.value !== null ? String(setting.value) : '—');
          const tooltip = setting && typeof setting.tooltip === 'string' ? setting.tooltip : '';
          const label = setting && typeof setting.label === 'string' ? setting.label : key;
          const explainer = setting && typeof setting.explainer === 'string' ? setting.explainer : '';
          const type = setting && setting.type ? String(setting.type) : detectConfigType(rawValue);
          return {
            key,
            label,
            value: valueText,
            tooltip,
            raw: rawValue,
            type,
            explainer,
          };
        });
        return {
          id: safeId,
          label: section && section.label ? String(section.label) : `Section ${index + 1}`,
          settings: normalized,
        };
      });
      select.innerHTML = '';
      if (!configOverviewState.sections.length) {
        const option = document.createElement('option');
        option.value = '';
        option.textContent = 'No categories available';
        select.appendChild(option);
        list.innerHTML = '<p class="config-empty">Config snapshot unavailable.</p>';
        return;
      }
      configOverviewState.sections.forEach(section => {
        const option = document.createElement('option');
        option.value = section.id;
        option.textContent = section.label;
        select.appendChild(option);
      });
      let selectedId = configOverviewState.selectedId;
      if (!selectedId || !configOverviewState.sections.some(section => section.id === selectedId)) {
        selectedId = configOverviewState.sections[0].id;
      }
      select.value = selectedId;
      renderConfigSection(selectedId);
      if (!configSelectInitialized) {
        select.addEventListener('change', onConfigCategoryChange);
        configSelectInitialized = true;
      }
    }

    function pulseHeartbeat() {
      if (!heartbeatIndicator) {
        return;
      }
      if (heartbeatTimer) {
        clearTimeout(heartbeatTimer);
      }
      heartbeatIndicator.classList.remove('pulse');
      heartbeatIndicator.classList.remove('inactive');
      void heartbeatIndicator.offsetWidth;
      heartbeatIndicator.classList.add('pulse');
      heartbeatTimer = setTimeout(() => heartbeatIndicator.classList.remove('pulse'), 700);
    }

    let featureState = {
      aiEnabled: true,
      disabledCommands: new Set(),
      messageMode: 'both',
      adminPassphrase: '',
      baselineAdminPassphrase: '',
      autoPingEnabled: true,
      adminWhitelist: [],
    };
    let featureSaveTimer = null;
    let featureSaving = false;
    let featureStateReady = false;

    function setFeaturesSaving(isSaving) {
      featureSaving = !!isSaving;
      const groups = $("commandGroups");
      if (groups) {
        if (featureSaving) {
          groups.setAttribute('data-saving', 'true');
        } else {
          groups.removeAttribute('data-saving');
        }
      }
      const aiToggle = $("aiToggle");
      if (aiToggle) {
        aiToggle.disabled = featureSaving;
      }
      const autoPingToggle = $("autoPingToggle");
      if (autoPingToggle) {
        autoPingToggle.disabled = featureSaving;
      }
      const passInput = $("adminPassphrase");
      if (passInput) {
        passInput.disabled = featureSaving;
      }
      const passButton = $("adminPassphraseSet");
      if (passButton) {
        passButton.disabled = featureSaving;
      }
      document.querySelectorAll('.admin-remove-btn').forEach(btn => {
        btn.disabled = featureSaving;
      });
      document.querySelectorAll('.mode-buttons').forEach(group => {
        if (featureSaving) {
          group.setAttribute('data-saving', 'true');
        } else {
          group.removeAttribute('data-saving');
        }
      });
      document.querySelectorAll('.mode-btn').forEach(btn => {
        btn.disabled = featureSaving;
      });
      reflectAdminPassphraseWarning();
    }

    function updateFeaturesStatus() {
      const statusEl = $("featuresStatus");
      if (!statusEl) return;
      const disabledCount = featureState.disabledCommands.size;
      const modeLabel = messageModeLabel(featureState.messageMode);
      const modeStatus = $("modeStatus");
      if (modeStatus) {
        modeStatus.textContent = modeLabel;
      }
      const parts = [modeLabel];
      if (disabledCount) {
        parts.push(`${disabledCount} command${disabledCount === 1 ? '' : 's'} disabled`);
      }
      if (!featureState.autoPingEnabled) {
        parts.push('auto ping off');
      }
      statusEl.textContent = parts.join(' · ');
    }

    function renderFeatureAlerts(alerts) {
      const box = $("featureAlerts");
      if (!box) return;
      box.innerHTML = '';
      if (!alerts || !alerts.length) {
        box.hidden = true;
        return;
      }
      alerts.forEach(message => {
        const pill = document.createElement('span');
        pill.className = 'feature-pill';
        pill.textContent = message;
        box.appendChild(pill);
      });
      box.hidden = false;
    }

    function renderAdminList(admins) {
      const list = $("adminList");
      const empty = $("adminListEmpty");
      if (!list || !empty) {
        return;
      }
      list.innerHTML = '';
      if (!admins || !admins.length) {
        empty.hidden = false;
        list.hidden = true;
        return;
      }
      empty.hidden = true;
      list.hidden = false;
      admins.forEach(entry => {
        const item = document.createElement('li');
        item.className = 'admin-item';
        const info = document.createElement('div');
        info.className = 'admin-item-info';
        const name = document.createElement('strong');
        name.textContent = entry && entry.label ? entry.label : (entry && entry.key ? entry.key : 'Unknown');
        info.appendChild(name);
        if (entry && entry.key) {
          const key = document.createElement('span');
          key.className = 'admin-item-key';
          key.textContent = entry.key;
          info.appendChild(key);
        }
        item.appendChild(info);
        const removeBtn = document.createElement('button');
        removeBtn.type = 'button';
        removeBtn.className = 'admin-remove-btn';
        removeBtn.textContent = 'Remove';
        if (entry && entry.key) {
          removeBtn.dataset.adminKey = entry.key;
          removeBtn.addEventListener('click', () => removeAdmin(entry.key, entry.label || entry.key));
        } else {
          removeBtn.disabled = true;
        }
        item.appendChild(removeBtn);
        list.appendChild(item);
      });
    }

    function openAdminPopover() {
      const popover = $("adminListPopover");
      if (!popover || !popover.hidden) {
        loadMetrics();
        return;
      }
      adminPopoverPreviousFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null;
      popover.hidden = false;
      const toggle = $("adminListToggle");
      if (toggle) {
        toggle.setAttribute('aria-expanded', 'true');
      }
      const closeBtn = $("adminListClose");
      if (closeBtn) {
        requestAnimationFrame(() => {
          try { closeBtn.focus({ preventScroll: true }); } catch (err) {}
        });
      }
      if (!adminPopoverOutsideHandler) {
        adminPopoverOutsideHandler = (event) => {
          const toggleNode = $("adminListToggle");
          if (!popover.contains(event.target) && event.target !== toggleNode) {
            closeAdminPopover({ restoreFocus: false });
          }
        };
        document.addEventListener('mousedown', adminPopoverOutsideHandler);
        document.addEventListener('touchstart', adminPopoverOutsideHandler);
      }
      loadMetrics();
    }

    function closeAdminPopover({ restoreFocus = true } = {}) {
      const popover = $("adminListPopover");
      if (!popover || popover.hidden) {
        return;
      }
      popover.hidden = true;
      const toggle = $("adminListToggle");
      if (toggle) {
        toggle.setAttribute('aria-expanded', 'false');
      }
      if (adminPopoverOutsideHandler) {
        document.removeEventListener('mousedown', adminPopoverOutsideHandler);
        document.removeEventListener('touchstart', adminPopoverOutsideHandler);
        adminPopoverOutsideHandler = null;
      }
      if (restoreFocus && adminPopoverPreviousFocus && typeof adminPopoverPreviousFocus.focus === 'function') {
        try { adminPopoverPreviousFocus.focus(); } catch (err) {}
      }
      adminPopoverPreviousFocus = null;
    }

    function onAdminListToggle(event) {
      if (event) {
        event.preventDefault();
      }
      const popover = $("adminListPopover");
      if (!popover) {
        return;
      }
      if (popover.hidden) {
        openAdminPopover();
      } else {
        closeAdminPopover();
      }
    }

    async function removeAdmin(key, label) {
      if (!key) {
        return;
      }
      const promptLabel = label || key;
      if (!window.confirm(`Remove admin access for ${promptLabel}?`)) {
        return;
      }
      setFeaturesSaving(true);
      try {
        const res = await fetch('/dashboard/admins/remove', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ key }),
        });
        if (!res.ok) {
          const errorText = await res.text();
          throw new Error(`HTTP ${res.status} ${errorText}`);
        }
        const data = await res.json();
        if (data && data.error) {
          throw new Error(data.error);
        }
        renderFeatures(data);
      } catch (err) {
        console.error('Failed to remove admin:', err);
        alert('Could not remove admin. Try again in a moment.');
      } finally {
        setFeaturesSaving(false);
      }
    }

    async function onAdminPassphraseSet() {
      await commitFeatureSave({ force: true });
    }

    function renderCommandGroups(categories) {
      const container = $("commandGroups");
      if (!container) return;
      const previousOpen = new Set();
      container.querySelectorAll('details.command-group').forEach(el => {
        const id = el.dataset ? el.dataset.categoryId : null;
        if (id && el.open) {
          previousOpen.add(id);
        }
      });
      container.innerHTML = '';
      if (!categories || !categories.length) {
        const empty = document.createElement('div');
        empty.className = 'feature-empty';
        empty.textContent = 'No command categories available.';
        container.appendChild(empty);
        return;
      }

      categories.forEach((cat, index) => {
        const details = document.createElement('details');
        details.className = 'command-group';
        const catId = cat.id || `cat-${index}`;
        details.dataset.categoryId = catId;
        const shouldOpen = previousOpen.size ? previousOpen.has(catId) : true;
        if (shouldOpen) {
          details.open = true;
        }
        const summary = document.createElement('summary');
        const commandCount = (cat.commands || []).length;
        summary.textContent = `${cat.label || cat.id} (${commandCount})`;
        details.appendChild(summary);

        const list = document.createElement('div');
        list.className = 'command-list';

        (cat.commands || []).forEach(entry => {
          const rawName = entry && entry.name ? entry.name : entry;
          const name = normalizeCommandName(rawName);
          if (!name) return;
          const aliasSource = Array.isArray(entry && entry.aliases) ? entry.aliases : [];
          const aliasList = aliasSource
            .map(alias => normalizeCommandName(alias))
            .filter(alias => alias && alias !== name);
          const uniqueAliases = Array.from(new Set(aliasList));
          const summary = entry && entry.summary ? String(entry.summary) : '';
          const categoryName = entry && entry.category ? String(entry.category) : (cat.label || cat.id || '');
          const item = document.createElement('label');
          item.className = 'command-item';
          item.dataset.command = name;
          const checkbox = document.createElement('input');
          checkbox.type = 'checkbox';
          checkbox.dataset.command = name;
          const isEnabled = !featureState.disabledCommands.has(name);
          checkbox.checked = isEnabled;
          item.classList.toggle('is-disabled', !isEnabled);
          checkbox.addEventListener('change', onCommandToggle);
          const span = document.createElement('span');
          span.textContent = name;
          if (summary || uniqueAliases.length || categoryName) {
            const tooltipParts = [];
            if (summary) {
              tooltipParts.push(summary);
            }
            if (uniqueAliases.length) {
              tooltipParts.push(`Also responds to: ${uniqueAliases.join(', ')}`);
            }
            if (categoryName) {
              tooltipParts.push(`Category: ${categoryName}`);
            }
            const tooltipText = tooltipParts.join('\\n\\n');
            bindExplainer(item, tooltipText, { placement: 'right' });
            span.title = tooltipText;
          }
          item.appendChild(checkbox);
          item.appendChild(span);
          list.appendChild(item);
        });

        if (!list.childElementCount) {
          const empty = document.createElement('div');
          empty.className = 'feature-empty';
          empty.textContent = 'No commands in this category.';
          list.appendChild(empty);
        }

        details.appendChild(list);
        container.appendChild(details);
      });
    }

    function reflectAdminPassphraseWarning() {
      const warning = $("adminPassphraseWarning");
      const input = $("adminPassphrase");
      if (!warning || !input) {
        return;
      }
      const baseline = (featureState.baselineAdminPassphrase || '').trim();
      const current = (input.value || '').trim();
      const hasChange = featureStateReady && current !== baseline;
      warning.hidden = !hasChange;
      const setBtn = $("adminPassphraseSet");
      if (setBtn) {
        setBtn.disabled = featureSaving || !hasChange;
      }
    }

    function updateMessageModeUI() {
      const buttons = document.querySelectorAll('.mode-btn');
      buttons.forEach(btn => {
        const mode = normalizeMessageMode(btn.dataset.mode);
        const active = mode === featureState.messageMode;
        btn.classList.toggle('active', active);
        btn.setAttribute('aria-pressed', active ? 'true' : 'false');
      });
    }

    function onModeButtonClick(event) {
      const mode = normalizeMessageMode(event.currentTarget.dataset.mode);
      if (!mode || mode === featureState.messageMode) {
        return;
      }
      featureState.messageMode = mode;
      updateMessageModeUI();
      updateFeaturesStatus();
      scheduleFeatureSave();
    }

    function renderFeatures(data) {
      if (!data) return;
      const disabled = (data.disabled_commands || []).map(normalizeCommandName);
      featureState.aiEnabled = !!data.ai_enabled;
      featureState.disabledCommands = new Set(disabled);
      featureState.messageMode = normalizeMessageMode(data.message_mode);
      featureState.adminPassphrase = data.admin_passphrase || '';
      featureState.baselineAdminPassphrase = featureState.adminPassphrase;
      featureState.autoPingEnabled = data.auto_ping_enabled === undefined ? true : !!data.auto_ping_enabled;
      featureState.adminWhitelist = Array.isArray(data.admin_whitelist) ? data.admin_whitelist.slice() : [];

      const aiToggle = $("aiToggle");
      if (aiToggle) {
        aiToggle.checked = featureState.aiEnabled;
      }
      const aiStatus = $("aiToggleStatus");
      if (aiStatus) {
        aiStatus.textContent = featureState.aiEnabled ? 'Enabled' : 'Disabled';
      }

      const autoPingToggle = $("autoPingToggle");
      if (autoPingToggle) {
        autoPingToggle.checked = featureState.autoPingEnabled;
      }
      const autoPingStatus = $("autoPingToggleStatus");
      if (autoPingStatus) {
        autoPingStatus.textContent = featureState.autoPingEnabled ? 'Enabled' : 'Disabled';
      }

      const passInput = $("adminPassphrase");
      if (passInput && passInput.value !== featureState.adminPassphrase) {
        passInput.value = featureState.adminPassphrase;
      }

      renderFeatureAlerts(data.alerts || []);
      renderCommandGroups(data.categories || []);
      renderAdminList(data.admins || []);
      updateMessageModeUI();
      updateFeaturesStatus();
      featureStateReady = true;
      reflectAdminPassphraseWarning();
    }

    function scheduleFeatureSave() {
      if (!featureStateReady) {
        return;
      }
      if (featureSaveTimer) {
        clearTimeout(featureSaveTimer);
      }
      featureSaveTimer = setTimeout(commitFeatureSave, 500);
    }

    async function commitFeatureSave(options = {}) {
      if (featureSaveTimer) {
        clearTimeout(featureSaveTimer);
        featureSaveTimer = null;
      }
      if (!featureStateReady && !options.force) {
        return;
      }

      const payload = {
        ai_enabled: featureState.aiEnabled,
        disabled_commands: Array.from(featureState.disabledCommands),
        message_mode: featureState.messageMode,
        admin_passphrase: (featureState.adminPassphrase || '').trim(),
        auto_ping_enabled: featureState.autoPingEnabled,
      };

      setFeaturesSaving(true);
      try {
        const res = await fetch('/dashboard/features', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });
        if (!res.ok) {
          throw new Error(`HTTP ${res.status}`);
        }
        const data = await res.json();
        renderFeatures(data);
      } catch (err) {
        console.error('Feature save failed:', err);
        alert('Updating feature toggles failed. Please try again.');
      } finally {
        setFeaturesSaving(false);
      }
    }

    function onCommandToggle(event) {
      const checkbox = event.target;
      if (!checkbox || !checkbox.dataset) return;
      const normalized = normalizeCommandName(checkbox.dataset.command);
      if (!normalized) return;
      if (checkbox.checked) {
        featureState.disabledCommands.delete(normalized);
      } else {
        featureState.disabledCommands.add(normalized);
      }
      const label = checkbox.closest('.command-item');
      if (label) {
        label.classList.toggle('is-disabled', !checkbox.checked);
      }
      updateFeaturesStatus();
      scheduleFeatureSave();
    }

    function onAiToggleChange(event) {
      featureState.aiEnabled = !!event.target.checked;
      const aiStatus = $("aiToggleStatus");
      if (aiStatus) {
        aiStatus.textContent = featureState.aiEnabled ? 'Enabled' : 'Disabled';
      }
      updateFeaturesStatus();
      scheduleFeatureSave();
    }

    function onAutoPingToggleChange(event) {
      featureState.autoPingEnabled = !!event.target.checked;
      const autoPingStatus = $("autoPingToggleStatus");
      if (autoPingStatus) {
        autoPingStatus.textContent = featureState.autoPingEnabled ? 'Enabled' : 'Disabled';
      }
      updateFeaturesStatus();
      scheduleFeatureSave();
    }

    function onAdminPassphraseInput(event) {
      featureState.adminPassphrase = event.target.value;
      reflectAdminPassphraseWarning();
    }

    function ensureLogbox() {
      return $("logbox");
    }

    function maintainLogHistory() {
      const logbox = ensureLogbox();
      if (!logbox) return;
      while (logbox.children.length > LOG_STREAM_MAX) {
        logbox.removeChild(logbox.firstChild);
      }
    }

    function decorateExistingLogLines() {
      const logbox = ensureLogbox();
      if (!logbox) return;
      logbox.querySelectorAll('.log-line').forEach(line => line.classList.add('log-decorated'));
      maintainLogHistory();
      requestAnimationFrame(() => scrollActivityToBottom(true));
    }

    function scrollActivityToBottom(force = false) {
      const logbox = ensureLogbox();
      if (!logbox) return;
      if (force || (logAutoScroll && !logUserScrolling)) {
        logbox.scrollTo({ top: logbox.scrollHeight, behavior: force ? "auto" : "smooth" });
      }
    }

    function appendActivityLine(html) {
      const logbox = ensureLogbox();
      if (!logbox) return;
      const temp = document.createElement("div");
      temp.innerHTML = html;
      const line = temp.firstElementChild || temp;
      line.classList.add("log-decorated", "animate-up");
      logbox.appendChild(line);
      setTimeout(() => line.classList.remove("animate-up"), 700);
      maintainLogHistory();
      scrollActivityToBottom();
      logLastMessageAt = Date.now();
    }

    function bindLogScroll() {
      const logbox = ensureLogbox();
      if (!logbox || logScrollBound) return;
      logScrollBound = true;
      logbox.addEventListener("scroll", () => {
        logUserScrolling = true;
        clearTimeout(logScrollTimeout);
        const nearBottom = logbox.scrollHeight - logbox.scrollTop <= logbox.clientHeight + 12;
        if (nearBottom) {
          logAutoScroll = true;
          setActivityScrollLabel("Streaming", true);
        } else {
          logAutoScroll = false;
          setActivityScrollLabel("Paused", false);
        }
        logScrollTimeout = setTimeout(() => { logUserScrolling = false; }, 600);
      });
    }

    function setActivityScrollLabel(text, arrowOn) {
      const status = $("scrollStatus");
      const label = $("scrollLabel");
      if (!status || !label) return;
      label.textContent = text;
      if (arrowOn) {
        status.classList.add("on");
      } else {
        status.classList.remove("on");
      }
      if (heartbeatIndicator) {
        heartbeatIndicator.classList.toggle('inactive', !arrowOn);
      }
    }

    function scheduleLogReconnect() {
      if (logReconnectAttempts >= LOG_RECONNECT_MAX) {
        setActivityScrollLabel("Stream offline", false);
        return;
      }
      const delay = Math.min(1000 * Math.pow(2, logReconnectAttempts), 10000);
      logReconnectAttempts += 1;
      setTimeout(initLogStream, delay);
    }

    function initLogStream() {
      const logbox = ensureLogbox();
      if (!logbox) return;
      if (logEventSource) {
        try { logEventSource.close(); } catch (e) {}
      }
      logEventSource = new EventSource("/logs_stream");
      logEventSource.onopen = () => {
        logReconnectAttempts = 0;
        setActivityScrollLabel("Streaming", true);
        logLastMessageAt = Date.now();
      };
      logEventSource.onmessage = (event) => {
        if (!event.data) return;
        if (event.data.includes("heartbeat") || event.data.includes("keepalive")) {
          logLastMessageAt = Date.now();
          pulseHeartbeat();
          setActivityScrollLabel("Streaming", true);
          logReconnectAttempts = 0;
          return;
        }
        if (event.data.includes("💓 HB")) {
          logLastMessageAt = Date.now();
          pulseHeartbeat();
          setActivityScrollLabel("Streaming", true);
          logReconnectAttempts = 0;
          return;
        }
        appendActivityLine(event.data);
        if (logAutoScroll) {
          setActivityScrollLabel("Streaming", true);
        }
      };
      logEventSource.onerror = () => {
        if (logEventSource) {
          try { logEventSource.close(); } catch (e) {}
          logEventSource = null;
        }
        scheduleLogReconnect();
      };
    }

    function monitorLogStream() {
      if (logAutoScroll) {
        const elapsed = Date.now() - logLastMessageAt;
        if (elapsed > LOG_WAIT_THRESHOLD_MS) {
          setActivityScrollLabel("Waiting…", false);
        } else {
          setActivityScrollLabel("Streaming", true);
        }
      }
      requestAnimationFrame(monitorLogStream);
    }

    function updateConnectionBanner(status) {
      const banner = $("connectionBanner");
      if (!banner) return;
      const normalized = (status || "").toLowerCase();
      banner.classList.remove("is-connected", "is-degraded", "is-disconnected", "is-unknown");
      if (normalized === "connected") {
        banner.classList.add("is-connected");
        banner.textContent = "Connected";
      } else if (normalized === "connecting" || normalized === "reconnecting") {
        banner.classList.add("is-degraded");
        banner.textContent = status;
      } else if (normalized === "disconnected" || normalized === "error") {
        banner.classList.add("is-disconnected");
        banner.textContent = status || "Disconnected";
      } else {
        banner.classList.add("is-unknown");
        banner.textContent = status || "Status unknown";
      }
    }

    function renderGamesBreakdown(breakdown) {
      const container = $("stat-games-breakdown");
      if (!container) return;
      container.innerHTML = "";
      if (!breakdown || Object.keys(breakdown).length === 0) {
        container.textContent = "No game sessions recorded in the last 24 hours.";
        return;
      }
      Object.entries(breakdown).forEach(([name, count]) => {
        const pill = document.createElement("span");
        pill.textContent = `${name}: ${count}`;
        container.appendChild(pill);
      });
    }

    function renderOnboardRoster(roster) {
      const container = $("onboardRoster");
      if (!container) return;
      container.innerHTML = '';
      if (!roster || roster.length === 0) {
        const empty = document.createElement('div');
        empty.className = 'onboard-roster-empty';
        empty.textContent = 'No completed onboardings yet.';
        container.appendChild(empty);
        return;
      }
      roster.forEach(entry => {
        const item = document.createElement('div');
        item.className = 'onboard-roster-item';
        const label = document.createElement('strong');
        label.textContent = entry.label || entry.sender_key || 'Unknown';
        const ts = document.createElement('span');
        if (entry.completed_at) {
          try {
            const time = new Date(entry.completed_at);
            ts.textContent = isNaN(time) ? entry.completed_at : time.toLocaleString();
          } catch (err) {
            ts.textContent = entry.completed_at;
          }
        } else {
          ts.textContent = '';
        }
        item.appendChild(label);
        if (ts.textContent) {
          item.appendChild(ts);
        }
        container.appendChild(item);
      });
    }

    async function loadMetrics() {
      const statusEl = $("metricsStatus");
      try {
        if (statusEl) {
          statusEl.textContent = "Refreshing…";
        }
        const res = await fetch(METRICS_URL, { cache: "no-store" });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        if (data.error) throw new Error(data.error);
        updateMetrics(data);
        if (statusEl) {
          statusEl.textContent = "Updated just now";
        }
      } catch (err) {
        if (statusEl) {
          statusEl.textContent = "Metrics unavailable";
        }
        updateConnectionBanner("Disconnected");
        console.error("Metrics refresh failed:", err);
      }
    }

    function updateMetrics(metrics) {
      if (!metrics || typeof metrics !== "object") return;
      const ts = metrics.timestamp ? new Date(metrics.timestamp) : null;
      const timestampEl = $("metricsTimestamp");
      if (timestampEl) {
        timestampEl.textContent = ts && !isNaN(ts) ? `Snapshot · ${ts.toLocaleTimeString()}` : "Snapshot time unavailable";
      }
      updateConnectionBanner(metrics.connection_status);

      const uptimeBadge = $("uptimeTicker");
      if (uptimeBadge) {
        const parts = [];
        if (metrics.uptime_human) {
          parts.push(metrics.uptime_human);
        }
        if (typeof metrics.restart_count === 'number') {
          parts.push(`restarts ${formatNumber(metrics.restart_count)}`);
        }
        const queueValue = metrics.queue && typeof metrics.queue.value === 'number'
          ? metrics.queue.value
          : (typeof metrics.queue_size === 'number' ? metrics.queue_size : null);
        if (queueValue !== null) {
          let queueText = `queue ${formatNumber(queueValue)}`;
          const delta = metrics.queue && typeof metrics.queue.delta === 'number' ? metrics.queue.delta : 0;
          if (delta > 0) {
            queueText += ' ▲';
          } else if (delta < 0) {
            queueText += ' ▼';
          }
          parts.push(queueText);
        }
        uptimeBadge.textContent = parts.length ? parts.join(' · ') : '—';
      }

      const messageActivity = metrics.message_activity || {};
      setValueWithDelta('stat-msg-total', messageActivity.total || { value: 0, delta: 0 });
      setValueWithDelta('stat-msg-direct', messageActivity.direct || { value: 0, delta: 0 });
      setValueWithDelta('stat-msg-ai', messageActivity.ai || { value: 0, delta: 0 });

      const nodeActivity = metrics.node_activity || {};
      setValueWithDelta('stat-nodes-current', nodeActivity.current || { value: 0, delta: 0 });
      setValueWithDelta('stat-nodes-new', nodeActivity.new_24h || { value: 0, delta: 0 });
      setValueWithDelta('stat-active-users', metrics.active_users || { value: 0, delta: 0 });
      setValueWithDelta('stat-new-onboards', metrics.new_onboards || { value: 0, delta: 0 });
      setValueWithDelta('stat-games', metrics.games || { value: 0, delta: 0 });

      if (metrics.games && metrics.games.breakdown) {
        renderGamesBreakdown(metrics.games.breakdown);
      } else {
        renderGamesBreakdown({});
      }

      // Ack summary
      try {
        const ack = (metrics.ack && metrics.ack.dm) ? metrics.ack.dm : {};
        const firstRate = (typeof ack.first_rate === 'number') ? `${ack.first_rate}%` : '—';
        const resendRate = (typeof ack.resend_rate === 'number') ? `${ack.resend_rate}%` : '—';
        const events = (typeof ack.events === 'number') ? ack.events : ((ack.first_total || 0) + (ack.resend_total || 0));
        const setText = (id, text) => { const el = $(id); if (el) el.textContent = text; };
        setText('stat-ack-dm-first', firstRate);
        setText('stat-ack-dm-resend', resendRate);
        setText('stat-ack-dm-events', (events || 0).toString());
      } catch (err) {}

      if (metrics.onboarding && Array.isArray(metrics.onboarding.roster)) {
        renderOnboardRoster(metrics.onboarding.roster);
      } else {
        renderOnboardRoster([]);
      }

      if (metrics.features) {
        renderFeatures(metrics.features);
      }
      if (metrics.config_overview) {
        renderConfigOverview(metrics.config_overview);
      }
    }

    window.addEventListener("beforeunload", () => {
      if (logEventSource) {
        try { logEventSource.close(); } catch (e) {}
      }
    });

    document.addEventListener("DOMContentLoaded", () => {
      bindGlobalErrorHandlers();
      decorateExistingLogLines();
      bindLogScroll();
      initLogStream();
      initPanelDrag();
      initPanelCollapse();
      if (initialMetrics && Object.keys(initialMetrics).length) {
        updateMetrics(initialMetrics);
        const statusEl = $("metricsStatus");
        if (statusEl) {
          statusEl.textContent = "Snapshot ready";
        }
      }
      document.querySelectorAll('.mode-btn').forEach(btn => {
        btn.addEventListener('click', onModeButtonClick);
      });
      updateMessageModeUI();
      const aiToggle = $("aiToggle");
      if (aiToggle) {
        aiToggle.addEventListener('change', onAiToggleChange);
      }
      const autoPingToggle = $("autoPingToggle");
      if (autoPingToggle) {
        autoPingToggle.addEventListener('change', onAutoPingToggleChange);
      }
      const passButton = $("adminPassphraseSet");
      if (passButton) {
        passButton.addEventListener('click', onAdminPassphraseSet);
      }
      const adminToggle = $("adminListToggle");
      if (adminToggle) {
        adminToggle.addEventListener('click', onAdminListToggle);
        adminToggle.setAttribute('aria-expanded', 'false');
        adminToggle.setAttribute('aria-controls', 'adminListPopover');
        adminToggle.setAttribute('aria-haspopup', 'dialog');
      }
      const adminClose = $("adminListClose");
      if (adminClose) {
        adminClose.addEventListener('click', () => closeAdminPopover());
      }
      document.addEventListener('keydown', event => {
        if (event.key === 'Escape') {
          const popRef = $("adminListPopover");
          if (popRef && !popRef.hidden) {
            closeAdminPopover();
          }
        }
      });
      const passInput = $("adminPassphrase");
      if (passInput) {
        passInput.addEventListener('input', onAdminPassphraseInput);
      }
      loadMetrics();
      setInterval(loadMetrics, METRICS_POLL_MS);
      initRadioPanel();
      loadRadioState();
      setInterval(loadRadioState, RADIO_STATE_POLL_MS);
      monitorLogStream();
    });
  </script>
</body>
</html>
"""
    page_html = page_html.replace("__METRICS__", metrics_bootstrap_attr)
    return page_html



@app.route('/dashboard/config/update', methods=['POST'])
def update_dashboard_config():
    data = request.get_json(force=True)
    key = data.get('key')
    if not key or not isinstance(key, str):
        return jsonify({'ok': False, 'error': 'Missing config key.'}), 400

    incoming = data.get('value')
    if isinstance(incoming, str):
        value_text = incoming
    elif incoming is None:
        value_text = ''
    else:
        try:
            value_text = json.dumps(incoming)
        except Exception:
            value_text = str(incoming)

    synthetic_quiet = (key == 'automessage_quiet_hours')

    with CONFIG_LOCK:
        if not synthetic_quiet and key not in config:
            return jsonify({'ok': False, 'error': f"'{key}' is not a recognized config option."}), 404
        try:
            new_value = _parse_config_update_value(value_text)
        except Exception as exc:
            return jsonify({'ok': False, 'error': f"Unable to interpret value: {exc}"}), 400

        if synthetic_quiet:
            if isinstance(new_value, str):
                try:
                    new_value = json.loads(new_value)
                except Exception:
                    new_value = {}
            if not isinstance(new_value, dict):
                return jsonify({'ok': False, 'error': 'Quiet hours update requires an object payload.'}), 400
            enabled = bool(new_value.get('enabled'))
            try:
                quiet_start = int(new_value.get('start', config.get('mail_quiet_start_hour', 20))) % 24
            except Exception:
                quiet_start = int(config.get('mail_quiet_start_hour', 20) or 20) % 24
            try:
                quiet_end = int(new_value.get('end', config.get('mail_quiet_end_hour', 8))) % 24
            except Exception:
                quiet_end = int(config.get('mail_quiet_end_hour', 8) or 8) % 24
            if enabled and quiet_start == quiet_end:
                enabled = False
            config['mail_notify_quiet_hours_enabled'] = bool(enabled)
            config['mail_quiet_start_hour'] = quiet_start
            config['mail_quiet_end_hour'] = quiet_end
            if enabled:
                config['notify_active_start_hour'] = quiet_end % 24
                config['notify_active_end_hour'] = quiet_start % 24
            else:
                config['notify_active_start_hour'] = 0
                config['notify_active_end_hour'] = 0
            try:
                write_atomic(CONFIG_FILE, json.dumps(config, indent=2, sort_keys=True))
            except Exception as exc:
                return jsonify({'ok': False, 'error': f"Failed to write config.json: {exc}"}), 500

            display_label = "Disabled (24/7 reminders)"
            tooltip = "Quiet hours disabled; reminders may send at any time."
            if enabled:
                display_label = f"Quiet {_format_hour_label(quiet_start)} → {_format_hour_label(quiet_end)}"
                tooltip = (
                    f"Quiet hours enabled. Reminders pause from {_format_hour_label(quiet_start)}"
                    f" to {_format_hour_label(quiet_end)} local time."
                )
            entry = {
                'key': 'automessage_quiet_hours',
                'value': display_label,
                'tooltip': tooltip,
                'raw': {
                    'enabled': bool(enabled),
                    'start': quiet_start,
                    'end': quiet_end,
                },
                'type': 'automessage_quiet_hours',
                'explainer': _build_config_explainer('automessage_quiet_hours', display_label, tooltip),
            }
            clean_log("Config 'automessage_quiet_hours' updated via dashboard", "🛠️", show_always=True, rate_limit=False)
            return jsonify({'ok': True, 'entry': entry})

        current_value = config.get(key)
        if new_value == current_value:
            display, tooltip = _format_config_value(key, current_value)
            entry = {
                'key': key,
                'value': display,
                'tooltip': tooltip,
                'raw': current_value,
                'type': _config_value_kind(current_value),
            }
            return jsonify({'ok': True, 'entry': entry})

        original_value = current_value
        config[key] = new_value
        try:
            write_atomic(CONFIG_FILE, json.dumps(config, indent=2, sort_keys=True))
        except Exception as exc:
            config[key] = original_value
            return jsonify({'ok': False, 'error': f"Failed to write config.json: {exc}"}), 500

    # Apply certain settings immediately to runtime globals
    if key == 'cooldown_enabled':
        try:
            globals()['COOLDOWN_ENABLED'] = bool(new_value)
        except Exception:
            pass
    clean_log(f"Config '{key}' updated via dashboard", "🛠️", show_always=True, rate_limit=False)
    display, tooltip = _format_config_value(key, new_value)
    entry = {
        'key': key,
        'value': display,
        'tooltip': tooltip,
        'raw': new_value,
        'type': _config_value_kind(new_value),
    }
    return jsonify({'ok': True, 'entry': entry})


## Reset defaults endpoint removed


@app.route('/dashboard/js-error', methods=['POST'])
def log_dashboard_js_error():
    data = request.get_json(silent=True) or {}
    kind = str(data.get('kind') or 'error')
    details = data.get('details') or {}
    message = details.get('message') or data.get('message') or 'Unknown JS error'
    source = details.get('source') or ''
    lineno = details.get('lineno')
    colno = details.get('colno')
    stack = details.get('stack') or data.get('stack')
    parts = [f"Dashboard JS {kind}: {message}"]
    if source:
        location = f"{source}:{lineno or '?'}:{colno or '?'}"
        parts.append(location)
    if stack:
        first_line = str(stack).splitlines()[0]
        parts.append(first_line)
    clean_log(" | ".join(parts), "⚠️", show_always=True, rate_limit=False)
    return jsonify({'ok': True})


# ---------------------------------
# Radio Settings (LoRa + Channels)
# ---------------------------------
_RADIO_OPS_LOCK = threading.Lock()


def _get_local_node():
    try:
        return getattr(interface, 'localNode', None) if interface is not None else None
    except Exception:
        return None


def _ensure_channels(node, timeout: float = 2.0):
    try:
        if getattr(node, 'channels', None) is None:
            node.requestChannels(0)
        start = time.time()
        while getattr(node, 'channels', None) is None and (time.time() - start) < max(0.2, timeout):
            time.sleep(0.05)
    except Exception:
        pass


def _build_radio_state_dict() -> Dict[str, Any]:
    state: Dict[str, Any] = {
        'connected': False,
        'radio_id': None,
        'node_num': None,
        'long_name': None,
        'short_name': None,
        'hops': None,
        'channels': [],
        'max_channels': 8,
    }
    if interface is None or connection_status != "Connected":
        return state
    node = _get_local_node()
    if node is None:
        return state
    try:
        # Ensure basic config is present
        try:
            interface.waitForConfig()
        except Exception:
            pass
        info = getattr(interface, 'myInfo', None)
        node_num = getattr(info, 'my_node_num', None) if info is not None else None
        state['connected'] = True
        state['node_num'] = int(node_num) if node_num is not None else None
        state['radio_id'] = str(node_num) if node_num is not None else None
        try:
            state['long_name'] = interface.getLongName()
            state['short_name'] = interface.getShortName()
        except Exception:
            pass
        # Hop limit lives under localConfig.lora.hop_limit
        try:
            lora = node.localConfig.lora
            state['hops'] = int(getattr(lora, 'hop_limit', 0))
        except Exception:
            state['hops'] = None

        # Channels
        _ensure_channels(node)
        channels = getattr(node, 'channels', None) or []
        out_rows: List[Dict[str, Any]] = []
        for idx in range(8):
            try:
                ch = channels[idx] if idx < len(channels) else None
            except Exception:
                ch = None
            role_name = 'DISABLED'
            name = ''
            psk_label = 'unencrypted'
            uplink = True
            downlink = True
            channel_num = None
            enabled = False
            if ch is not None:
                try:
                    role_enum = getattr(ch, 'role', meshtastic_channel_pb2.Channel.Role.DISABLED)
                    role_name = meshtastic_channel_pb2.Channel.Role.Name(role_enum)
                except Exception:
                    role_name = 'DISABLED'
                try:
                    settings = getattr(ch, 'settings', None)
                    if settings is not None:
                        name = getattr(settings, 'name', '') or ''
                        psk = getattr(settings, 'psk', b'') or b''
                        try:
                            psk_label = meshtastic_util.pskToString(psk)
                        except Exception:
                            psk_label = 'secret' if psk else 'unencrypted'
                        uplink = bool(getattr(settings, 'uplink_enabled', True))
                        downlink = bool(getattr(settings, 'downlink_enabled', True))
                        channel_num = getattr(settings, 'channel_num', None)
                except Exception:
                    pass
                enabled = (role_name != 'DISABLED')
            out_rows.append({
                'index': idx,
                'role': role_name,
                'name': name,
                'psk': psk_label,
                'uplink': uplink,
                'downlink': downlink,
                'channel_num': channel_num,
                'enabled': enabled,
                'deletable': (role_name == 'SECONDARY'),
            })
        state['channels'] = out_rows
    except Exception as exc:
        state['error'] = str(exc)
    return state


@app.route('/dashboard/radio/state', methods=['GET'])
def get_radio_state():
    state = _build_radio_state_dict()
    code = 200 if state.get('connected') else 503
    return jsonify({'ok': bool(state.get('connected')), 'radio': state}), code


@app.route('/dashboard/radio/hops', methods=['POST'])
def set_radio_hops():
    data = request.get_json(force=True) or {}
    try:
        hop_limit = int(data.get('hop_limit'))
    except Exception:
        return jsonify({'ok': False, 'error': 'Invalid hop_limit'}), 400
    if hop_limit < 0 or hop_limit > 7:
        return jsonify({'ok': False, 'error': 'hop_limit must be between 0 and 7'}), 400
    node = _get_local_node()
    if interface is None or node is None:
        return jsonify({'ok': False, 'error': 'Radio not connected'}), 503
    with _RADIO_OPS_LOCK:
        try:
            interface.waitForConfig()
        except Exception:
            pass
        try:
            node.localConfig.lora.hop_limit = int(hop_limit)
            node.writeConfig('lora')
        except Exception as exc:
            return jsonify({'ok': False, 'error': f'Failed to set hop limit: {exc}'}), 500
    clean_log(f"Radio hop limit set to {hop_limit}", "🛠️", show_always=True, rate_limit=False)
    return jsonify({'ok': True, 'hop_limit': hop_limit})


def _psk_from_text(text: Optional[str]) -> Optional[bytes]:
    if text is None:
        return None
    s = str(text).strip()
    if not s:
        return None
    try:
        return meshtastic_util.fromPSK(s)
    except Exception:
        return None


@app.route('/dashboard/radio/channel/add', methods=['POST'])
def add_radio_channel():
    data = request.get_json(force=True) or {}
    name = str(data.get('name') or '').strip() or 'New Channel'
    psk_text = data.get('psk')  # 'random' | 'default' | 'none' | 'simpleN' | raw hex/base64
    uplink = bool(data.get('uplink', True))
    downlink = bool(data.get('downlink', True))
    channel_num = data.get('channel_num')
    try:
        channel_num = int(channel_num) if channel_num is not None else None
    except Exception:
        channel_num = None
    node = _get_local_node()
    if interface is None or node is None:
        return jsonify({'ok': False, 'error': 'Radio not connected'}), 503
    with _RADIO_OPS_LOCK:
        _ensure_channels(node)
        channels = getattr(node, 'channels', None)
        if not channels:
            return jsonify({'ok': False, 'error': 'Unable to read channels'}), 500
        target_idx = None
        for i, ch in enumerate(channels[:8]):
            role = getattr(ch, 'role', meshtastic_channel_pb2.Channel.Role.DISABLED)
            if role == meshtastic_channel_pb2.Channel.Role.DISABLED:
                target_idx = i
                break
        if target_idx is None:
            return jsonify({'ok': False, 'error': 'All 8 channels are already in use'}), 400
        # Prepare PSK
        psk_bytes = _psk_from_text(psk_text) if psk_text else meshtastic_util.genPSK256()
        try:
            ch = channels[target_idx]
            ch.role = meshtastic_channel_pb2.Channel.Role.SECONDARY
            if ch.settings is None:
                ch.settings = meshtastic_channel_pb2.ChannelSettings()
            ch.settings.name = name
            ch.settings.uplink_enabled = uplink
            ch.settings.downlink_enabled = downlink
            if channel_num is not None:
                ch.settings.channel_num = int(channel_num)
            ch.settings.psk = psk_bytes
            node.writeChannel(target_idx)
        except Exception as exc:
            return jsonify({'ok': False, 'error': f'Failed to add channel: {exc}'}), 500
    clean_log(f"Added secondary channel at index {target_idx}", "🛠️", show_always=True, rate_limit=False)
    return jsonify({'ok': True, 'channel': _build_radio_state_dict().get('channels', [])[target_idx]})


@app.route('/dashboard/radio/channel/update', methods=['POST'])
def update_radio_channel():
    data = request.get_json(force=True) or {}
    try:
        index = int(data.get('index'))
    except Exception:
        return jsonify({'ok': False, 'error': 'Missing or invalid channel index'}), 400
    name = data.get('name')
    uplink = data.get('uplink')
    downlink = data.get('downlink')
    psk_text = data.get('psk')  # if provided, will be applied; 'random' or explicit
    node = _get_local_node()
    if interface is None or node is None:
        return jsonify({'ok': False, 'error': 'Radio not connected'}), 503
    with _RADIO_OPS_LOCK:
        _ensure_channels(node)
        channels = getattr(node, 'channels', None)
        if not channels or index < 0 or index >= len(channels):
            return jsonify({'ok': False, 'error': 'Channel index out of range'}), 400
        ch = channels[index]
        role = getattr(ch, 'role', meshtastic_channel_pb2.Channel.Role.DISABLED)
        if role == meshtastic_channel_pb2.Channel.Role.DISABLED:
            return jsonify({'ok': False, 'error': 'Channel is disabled'}), 400
        try:
            if ch.settings is None:
                ch.settings = meshtastic_channel_pb2.ChannelSettings()
            if name is not None:
                ch.settings.name = str(name)
            if uplink is not None:
                ch.settings.uplink_enabled = bool(uplink)
            if downlink is not None:
                ch.settings.downlink_enabled = bool(downlink)
            if psk_text is not None:
                pb = _psk_from_text(psk_text) if psk_text else meshtastic_util.genPSK256()
                if pb is not None:
                    ch.settings.psk = pb
            node.writeChannel(index)
        except Exception as exc:
            return jsonify({'ok': False, 'error': f'Failed to update channel: {exc}'}), 500
    clean_log(f"Updated channel {index}", "🛠️", show_always=True, rate_limit=False)
    return jsonify({'ok': True, 'channel': _build_radio_state_dict().get('channels', [])[index]})


@app.route('/dashboard/radio/channel/remove', methods=['POST'])
def remove_radio_channel():
    data = request.get_json(force=True) or {}
    try:
        index = int(data.get('index'))
    except Exception:
        return jsonify({'ok': False, 'error': 'Missing or invalid channel index'}), 400
    node = _get_local_node()
    if interface is None or node is None:
        return jsonify({'ok': False, 'error': 'Radio not connected'}), 503
    with _RADIO_OPS_LOCK:
        _ensure_channels(node)
        channels = getattr(node, 'channels', None)
        if not channels or index < 0 or index >= len(channels):
            return jsonify({'ok': False, 'error': 'Channel index out of range'}), 400
        ch = channels[index]
        role = getattr(ch, 'role', meshtastic_channel_pb2.Channel.Role.DISABLED)
        if role != meshtastic_channel_pb2.Channel.Role.SECONDARY:
            return jsonify({'ok': False, 'error': 'Only SECONDARY channels can be deleted'}), 400
        try:
            node.deleteChannel(index)
        except Exception as exc:
            return jsonify({'ok': False, 'error': f'Failed to delete channel: {exc}'}), 500
    clean_log(f"Deleted channel at index {index}", "🛠️", show_always=True, rate_limit=False)
    return jsonify({'ok': True, 'channels': _build_radio_state_dict().get('channels', [])})


@app.route('/autostart', methods=['GET'])
def get_autostart():
    cfg = safe_load_json(CONFIG_FILE, {})
    return jsonify({'start_on_boot': bool(cfg.get('start_on_boot', True))})


@app.route('/autostart/toggle', methods=['POST'])
def toggle_autostart():
    data = request.get_json(force=True)
    desired = bool(data.get('start_on_boot', True))
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
    global interface, restart_count, server_start_time, reset_event, ALARM_TIMER_MANAGER
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

    if ALARM_TIMER_MANAGER is None:
        try:
            ALARM_TIMER_MANAGER = AlarmTimerManager(
                storage_path="data/alarms_timers.json",
                clean_log=clean_log,
                send_direct_fn=send_direct_chunks,
            )
            ALARM_TIMER_MANAGER.start()
        except Exception:
            pass

    # Start monitors (connection watchdog and scheduled refresh)
    threading.Thread(target=connection_monitor, args=(20,), daemon=True).start()
    threading.Thread(target=scheduled_refresh_monitor, daemon=True).start()
    # Heartbeat thread for visibility
    threading.Thread(target=heartbeat_worker, args=(30,), daemon=True).start()
    threading.Thread(target=location_cleanup_worker, daemon=True).start()

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
            try:
                if ALARM_TIMER_MANAGER is not None:
                    ALARM_TIMER_MANAGER.set_interface(interface)
            except Exception:
                pass
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
 
