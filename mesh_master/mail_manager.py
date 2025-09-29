from __future__ import annotations

import hashlib
import json
import os
import random
import threading
import time
from collections import deque
from datetime import datetime, timezone, timedelta
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from mesh_master_mail import MailStore, MAIL_RETENTION_SECONDS
from .replies import PendingReply

MAIL_TIME_DISPLAY = "%m-%d %H:%M"

EMPTY_MAILBOX_RESPONSES = [
    "📭 Inbox '{mailbox}' is empty. Try `/m {mailbox} hello` to get things started.",
    "📭 Nothing in '{mailbox}' yet. Send `/m {mailbox} your message` to break the silence.",
]

MISSING_MAILBOX_RESPONSES = [
    "📪 Mailbox '{mailbox}' isn't set up yet. Create it with `/m {mailbox} your message`.",
    "📪 No mailbox named '{mailbox}' so far. Kick things off with `/m {mailbox} hi there`.",
]

YES_RESPONSES = {"y", "yes", "yeah", "yep"}
NO_RESPONSES = {"n", "no", "nope"}
CANCEL_RESPONSES = {"cancel", "stop", "abort"}

PIN_WARNING_THRESHOLD = 15
PIN_LOCK_THRESHOLD = 20


class MailManager:
    def __init__(
        self,
        *,
        store_path: str,
        security_path: str,
        clean_log: Callable[..., None],
        ai_log: Callable[..., None],
        ollama_url: Optional[str],
        search_model: str,
        search_timeout: int,
        search_num_ctx: int,
        search_max_messages: int,
        message_limit: int,
        follow_up_delay: float,
        notify_enabled: bool,
        reminders_enabled: bool,
        reminder_interval_seconds: float,
        reminder_expiry_seconds: float,
        reminder_max_count: int,
        include_self_notifications: bool,
        heartbeat_only: bool,
        quiet_hours_enabled: bool,
        quiet_start_hour: int,
        quiet_end_hour: int,
        stats: Optional[Any] = None,
    ) -> None:
        self.store = MailStore(store_path, limit=message_limit)
        self.clean_log = clean_log
        self.ai_log = ai_log
        self.ollama_url = ollama_url
        self.search_model = search_model
        self.search_timeout = search_timeout
        self.search_num_ctx = search_num_ctx
        self.search_max_messages = search_max_messages
        self.pending_creation: Dict[str, Dict[str, Any]] = {}
        self.security_path = security_path
        self.security_lock = threading.Lock()
        self.security: Dict[str, Dict[str, Any]] = self._load_security()
        self.display_max_messages = max(1, int(message_limit))
        self.follow_up_delay = max(0.0, float(follow_up_delay))
        self.notify_enabled = bool(notify_enabled)
        self.reminder_interval = max(60.0, float(reminder_interval_seconds))
        self.reminder_expiry = max(self.reminder_interval, float(reminder_expiry_seconds))
        reminder_count = int(reminder_max_count)
        self.reminder_max_count = max(0, reminder_count)
        self.reminders_enabled = bool(reminders_enabled) and self.reminder_max_count > 0
        self.include_self_notifications = bool(include_self_notifications)
        self.heartbeat_only = bool(heartbeat_only)
        self.quiet_hours_enabled = bool(quiet_hours_enabled)
        self.quiet_start_hour = int(quiet_start_hour) % 24
        self.quiet_end_hour = int(quiet_end_hour) % 24
        self.active_window_all_day = (not self.quiet_hours_enabled) or self.quiet_start_hour == self.quiet_end_hour
        self.events = deque()
        self.stats = stats
        self.reply_contexts: Dict[str, Dict[str, Any]] = {}
        self._reply_lock = threading.Lock()
        self.active_auto_notifications: Dict[str, Dict[str, Any]] = {}
        self.last_engagement_prompt: Dict[str, float] = {}

    # Utility helpers -------------------------------------------------
    def _local_datetime(self, ts: float) -> datetime:
        try:
            return datetime.fromtimestamp(ts)
        except Exception:
            return datetime.now()

    def _within_active_window(self, ts: float) -> bool:
        if self.active_window_all_day:
            return True
        dt = self._local_datetime(ts)
        hour_fraction = dt.hour + dt.minute / 60.0
        start = self.quiet_start_hour
        end = self.quiet_end_hour
        if start == end:
            return True
        if start < end:
            return start <= hour_fraction < end
        return hour_fraction >= start or hour_fraction < end

    def _next_window_start(self, ts: float, include_today: bool = False) -> float:
        if self.active_window_all_day:
            return ts
        reference = self._local_datetime(ts)
        for days_ahead in range(0, 3):
            candidate = reference + timedelta(days=days_ahead)
            candidate = candidate.replace(
                hour=self.quiet_start_hour,
                minute=0,
                second=0,
                microsecond=0,
            )
            if not include_today and days_ahead == 0 and candidate < reference:
                continue
            if candidate < reference:
                continue
            candidate_ts = candidate.timestamp()
            if self._within_active_window(candidate_ts):
                return candidate_ts
        fallback = (reference + timedelta(days=1)).replace(
            hour=self.quiet_start_hour,
            minute=0,
            second=0,
            microsecond=0,
        )
        return fallback.timestamp()

    def _first_reminder_time(self, base_ts: float) -> Optional[float]:
        if not self.reminders_enabled:
            return None
        base_dt = self._local_datetime(base_ts) + timedelta(days=1)
        if self.active_window_all_day:
            candidate = base_dt.replace(minute=0, second=0, microsecond=0)
            return candidate.timestamp()
        candidate = base_dt.replace(
            hour=self.quiet_start_hour,
            minute=0,
            second=0,
            microsecond=0,
        )
        candidate_ts = candidate.timestamp()
        if self._within_active_window(candidate_ts):
            return candidate_ts
        return self._next_window_start(candidate_ts, include_today=True)

    def _compute_next_reminder_time(self, after_ts: float) -> Optional[float]:
        if not self.reminders_enabled:
            return None
        candidate = after_ts + self.reminder_interval
        if self._within_active_window(candidate):
            return candidate
        return self._next_window_start(candidate)

    def _seed_reminders(self, sub: Dict[str, Any], now: float) -> None:
        if not self.reminders_enabled:
            sub.pop('reminders', None)
            return
        next_ts = self._first_reminder_time(now)
        if next_ts is None:
            sub['reminders'] = {}
            return
        expiry_ts = now + self.reminder_expiry if self.reminder_expiry else None
        sub['reminders'] = {
            'base_ts': now,
            'next_ts': next_ts,
            'count': 0,
            'last_sent_ts': None,
            'expiry_ts': expiry_ts,
        }
        node_id = sub.get('node_id')
        if node_id is not None:
            sub['reminders']['node_id'] = node_id

    def _clear_reminders(self, sub: Dict[str, Any]) -> None:
        if 'reminders' in sub:
            sub['reminders'] = {}

    def cancel_all_for_sender(self, sender_key: Optional[str]) -> int:
        """Clear pending notices and reminders for a subscriber across all mailboxes.
        Returns the number of mailboxes touched.
        """
        if not sender_key:
            return 0
        touched = 0
        with self.security_lock:
            for key, entry in list(self.security.items()):
                subscribers, _ = self._ensure_mailbox_state(entry)
                sub = subscribers.get(sender_key)
                if not isinstance(sub, dict):
                    continue
                changed = False
                if sub.get('pending_notice'):
                    sub['pending_notice'] = False
                    changed = True
                if 'reminders' in sub and sub['reminders']:
                    sub['reminders'] = {}
                    changed = True
                if changed:
                    touched += 1
            if touched:
                try:
                    self._save_security()
                except Exception:
                    pass
        return touched

    def _format_mail_timestamp(self, ts: str) -> str:
        try:
            normalized = ts
            if normalized.endswith('Z'):
                normalized = normalized[:-1] + '+00:00'
            dt = datetime.fromisoformat(normalized)
            return dt.strftime(MAIL_TIME_DISPLAY)
        except Exception:
            return ts[:16]

    def _parse_mail_timestamp(self, value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            text = value.strip()
            if text.endswith('Z'):
                text = text[:-1] + '+00:00'
            dt = datetime.fromisoformat(text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            return None

    def _format_deletion_eta(self, timestamp: Optional[str]) -> str:
        parsed = self._parse_mail_timestamp(timestamp)
        if parsed is None:
            parsed = datetime.now(timezone.utc)
        deadline = parsed + timedelta(seconds=MAIL_RETENTION_SECONDS)
        return self._format_mail_timestamp(deadline.isoformat())

    def _shorten_sender(self, sender: str, limit: int = 14) -> str:
        cleaned = (sender or "unknown").strip()
        if len(cleaned) <= limit:
            return cleaned
        return cleaned[: limit - 1] + "…"

    def _format_mail_line(self, index: int, message: dict) -> str:
        sender_raw = message.get('sender_short') or message.get('sender_id') or 'unknown'
        sender = self._shorten_sender(str(sender_raw), limit=14)
        body = str(message.get('body', '') or '').strip()
        timestamp = self._format_mail_timestamp(message.get('timestamp', ''))
        return f"{index}) {timestamp} {sender}: {body}"

    def _strip_quotes(self, text: str) -> str:
        if not text:
            return text
        if len(text) >= 2 and text[0] == text[-1] and text[0] in {'"', "'"}:
            return text[1:-1].strip()
        return text

    # Security helpers ------------------------------------------------
    def _load_security(self) -> Dict[str, Dict[str, Any]]:
        if not self.security_path:
            return {}
        try:
            with open(self.security_path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            if isinstance(data, dict):
                return data
        except Exception:
            pass
        return {}

    def _save_security(self) -> None:
        if not self.security_path:
            return
        directory = os.path.dirname(self.security_path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        tmp = f"{self.security_path}.tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(self.security, fh, ensure_ascii=False, indent=2)
        os.replace(tmp, self.security_path)

    def _security_key(self, mailbox: str) -> str:
        return self.store.normalize_mailbox(mailbox)

    def _hash_pin(self, pin: str) -> str:
        return hashlib.sha256(pin.encode("utf-8")).hexdigest()

    def _get_security_entry(self, mailbox: str) -> Dict[str, Any]:
        key = self._security_key(mailbox)
        with self.security_lock:
            entry = self.security.setdefault(
                key,
                {
                    "pin_hash": None,
                    "owner": None,
                    "created": time.time(),
                    "failures": {},
                },
            )
            return entry

    def _ensure_mailbox_state(self, entry: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        subscribers = entry.setdefault('subscribers', {})
        messages = entry.setdefault('messages', {})
        return subscribers, messages

    def _ensure_subscriber_entry(
        self,
        entry: Dict[str, Any],
        subscriber_key: str,
        *,
        node_id: Any = None,
        short: Optional[str] = None,
    ) -> Dict[str, Any]:
        subscribers, _ = self._ensure_mailbox_state(entry)
        now = time.time()
        sub = subscribers.setdefault(
            subscriber_key,
            {
                'first_seen': now,
                'last_check': 0.0,
                'node_id': node_id,
                'short': short or subscriber_key,
                'unread': [],
                'pending_notice': False,
            },
        )
        if node_id is not None:
            sub['node_id'] = node_id
        if short:
            sub['short'] = short
        sub.setdefault('unread', [])
        sub.setdefault('pending_notice', False)
        sub.setdefault('last_check', now)
        sub.setdefault('first_seen', now)
        sub.setdefault('reminders', {})
        return sub

    def _queue_event(self, event: Dict[str, Any]) -> None:
        self.events.append(event)

    def _prune_reply_contexts(self) -> None:
        cutoff = time.time() - 3600  # keep contexts for up to 1 hour
        with self._reply_lock:
            stale_keys = [key for key, ctx in self.reply_contexts.items() if ctx.get('captured', 0) < cutoff]
            for key in stale_keys:
                self.reply_contexts.pop(key, None)

    def _store_reply_context(
        self,
        sender_key: Optional[str],
        entries: List[Dict[str, Any]],
    ) -> None:
        if not sender_key:
            return
        self._prune_reply_contexts()
        usable: List[Dict[str, Any]] = []
        name_map: Dict[str, Dict[str, Any]] = {}
        seen_nodes: Set[str] = set()
        for entry in entries:
            node_id = str(entry.get('sender_id') or "").strip()
            if not node_id:
                continue
            short = str(entry.get('sender_short') or node_id).strip()
            index = int(entry.get('index', 0))
            if index <= 0:
                continue
            key = short.lower()
            record = {
                'index': index,
                'short': short,
                'node_id': node_id,
                'mailbox': entry.get('mailbox'),
                'message_id': entry.get('message_id'),
            }
            usable.append(record)
            if node_id not in seen_nodes:
                seen_nodes.add(node_id)
                name_map[key] = record
        if not usable:
            with self._reply_lock:
                self.reply_contexts.pop(sender_key, None)
            return
        usable.sort(key=lambda item: item['index'])
        with self._reply_lock:
            self.reply_contexts[sender_key] = {
                'captured': time.time(),
                'entries': usable,
                'names': name_map,
            }

    def _lookup_reply_context(self, sender_key: Optional[str]) -> Optional[Dict[str, Any]]:
        if not sender_key:
            return None
        self._prune_reply_contexts()
        with self._reply_lock:
            ctx = self.reply_contexts.get(sender_key)
            return dict(ctx) if ctx else None

    def _iso_to_timestamp(self, iso_ts: str) -> float:
        try:
            normalized = iso_ts
            if normalized.endswith('Z'):
                normalized = normalized[:-1] + '+00:00'
            return datetime.fromisoformat(normalized).timestamp()
        except Exception:
            try:
                return float(iso_ts)
            except Exception:
                return time.time()

    def _set_mailbox_security(self, mailbox: str, owner: Optional[str], pin: Optional[str]) -> None:
        entry = self._get_security_entry(mailbox)
        with self.security_lock:
            subscribers, _ = self._ensure_mailbox_state(entry)
            entry['owner'] = owner or entry.get('owner')
            if pin:
                entry['pin_hash'] = self._hash_pin(pin)
                for key, sub in subscribers.items():
                    if not key or (owner and key == owner):
                        continue
                    sub['trusted'] = False
            else:
                entry['pin_hash'] = None
                for sub in subscribers.values():
                    sub['trusted'] = True
            entry.setdefault('failures', {})
            if owner:
                owner_entry = self._ensure_subscriber_entry(entry, owner)
                owner_entry['trusted'] = True
            self._save_security()

    def _record_failure(self, entry: Dict[str, Any], sender_key: str) -> int:
        failures = entry.setdefault('failures', {})
        info = failures.setdefault(sender_key, {"count": 0, "blocked": False})
        info['count'] = int(info.get('count', 0)) + 1
        info['last'] = time.time()
        if info['count'] >= PIN_LOCK_THRESHOLD:
            info['blocked'] = True
        self._save_security()
        return info['count']

    def _reset_failures(self, entry: Dict[str, Any], sender_key: str) -> None:
        failures = entry.setdefault('failures', {})
        if sender_key in failures:
            failures[sender_key] = {"count": 0, "blocked": False, "last": time.time()}
            self._save_security()

    def _is_blacklisted(self, entry: Dict[str, Any], sender_key: str) -> bool:
        failures = entry.get('failures', {})
        info = failures.get(sender_key)
        if not info:
            return False
        return bool(info.get('blocked'))

    def _verify_pin(self, entry: Dict[str, Any], submitted_pin: str) -> bool:
        stored = entry.get('pin_hash')
        if not stored:
            return True
        try:
            return stored == self._hash_pin(submitted_pin)
        except Exception:
            return False

    def _extract_pin(self, text: str) -> Tuple[Optional[str], str]:
        if not text:
            return None, ""
        tokens = text.strip().split()
        pin_value: Optional[str] = None
        remainder: List[str] = []
        expecting_pin = False
        remainder_started = False

        for token in tokens:
            cleaned = token.strip(',.;:')
            lowered = cleaned.lower()
            candidate: Optional[str] = None

            if expecting_pin:
                candidate = cleaned
                expecting_pin = False
            elif lowered == "pin":
                expecting_pin = True
                continue
            elif lowered.startswith("pin="):
                candidate = cleaned[4:]
            elif lowered.startswith("pin") and not remainder_started:
                candidate = cleaned[3:]
            elif not remainder_started and cleaned.isdigit():
                candidate = cleaned

            if candidate and not pin_value:
                candidate = candidate.strip()
                if candidate.isdigit() and 4 <= len(candidate) <= 8:
                    pin_value = candidate
                    continue
                if lowered.startswith("pin") and candidate:
                    pin_value = candidate
                    continue

            remainder.append(token)
            remainder_started = True

        if expecting_pin and pin_value is None:
            remainder.append("PIN")

        return pin_value, " ".join(remainder).strip()

    def _authorise_mailbox(self, sender_key: str, mailbox: str, provided_pin: Optional[str]) -> Optional[PendingReply]:
        entry = self._get_security_entry(mailbox)
        self._ensure_mailbox_state(entry)
        if not entry.get('pin_hash'):
            self._reset_failures(entry, sender_key)
            return None

        if not sender_key:
            return PendingReply(
                "⚠️ Secure mailboxes require a known sender ID.",
                "/c command",
            )

        with self.security_lock:
            subscribers, _ = self._ensure_mailbox_state(entry)
            sub = self._ensure_subscriber_entry(entry, sender_key)
            trusted = bool(sub.get('trusted'))
        if trusted:
            self._reset_failures(entry, sender_key)
            return None

        if provided_pin and self._verify_pin(entry, provided_pin):
            with self.security_lock:
                subscribers, _ = self._ensure_mailbox_state(entry)
                sub = self._ensure_subscriber_entry(entry, sender_key)
                sub['trusted'] = True
                self._save_security()
            self._reset_failures(entry, sender_key)
            return None

        if self._is_blacklisted(entry, sender_key):
            if provided_pin and self._verify_pin(entry, provided_pin):
                with self.security_lock:
                    subscribers, _ = self._ensure_mailbox_state(entry)
                    sub = self._ensure_subscriber_entry(entry, sender_key)
                    sub['trusted'] = True
                    self._save_security()
                self._reset_failures(entry, sender_key)
                return None
            return PendingReply(
                "⛔ Access permanently blocked after repeated incorrect PIN attempts.",
                "/c command",
            )

        if not provided_pin:
            return PendingReply(
                f"🔐 Mailbox '{mailbox}' requires a PIN. Add your PIN after the inbox name (example: `/c {mailbox} PIN`).",
                "/c command",
            )

        if not self._verify_pin(entry, provided_pin):
            count = self._record_failure(entry, sender_key)
            if count >= PIN_LOCK_THRESHOLD:
                return PendingReply(
                    "⛔ Too many incorrect PIN attempts. Access locked.",
                    "/c command",
                )
            if count >= PIN_WARNING_THRESHOLD:
                return PendingReply(
                    f"⚠️ {count} incorrect PIN attempts. One more mistake will lock this mailbox.",
                    "/c command",
                )
            return PendingReply("❌ Incorrect PIN. Try again.", "/c command")

        with self.security_lock:
            subscribers, _ = self._ensure_mailbox_state(entry)
            sub = self._ensure_subscriber_entry(entry, sender_key)
            sub['trusted'] = True
            self._save_security()
        self._reset_failures(entry, sender_key)
        return None

    def _record_message_append(
        self,
        mailbox: str,
        message: Dict[str, Any],
        sender_key: str,
        sender_id: Any,
        sender_short: str,
    ) -> None:
        if not self.notify_enabled:
            return
        entry = self._get_security_entry(mailbox)
        now = time.time()
        needs_save = False
        with self.security_lock:
            subscribers, messages = self._ensure_mailbox_state(entry)
            entry.setdefault('mailbox_name', mailbox)
            msg_id = message.get('id')
            if not msg_id:
                msg_id = message['id'] = hashlib.sha1(
                    f"{message.get('timestamp', now)}-{message.get('body', '')}".encode('utf-8')
                ).hexdigest()
            meta = messages.setdefault(msg_id, {})
            meta.update(
                {
                    'id': msg_id,
                    'mailbox': mailbox,
                    'sender_key': sender_key,
                    'sender_node': sender_id,
                    'sender_short': sender_short,
                    'timestamp': message.get('timestamp'),
                    'body': message.get('body', ''),
                }
            )
            meta.setdefault('readers', {})
            needs_save = True

            owner_key = entry.get('owner')
            if owner_key:
                self._ensure_subscriber_entry(entry, owner_key)

            for sub_key, sub in subscribers.items():
                if not sub_key:
                    continue
                if not self.include_self_notifications and sub_key == sender_key:
                    continue
                unread = sub.setdefault('unread', [])
                if msg_id not in unread:
                    unread.append(msg_id)
                    needs_save = True
                if sub.get('pending_notice') is not True:
                    sub['pending_notice'] = True
                    needs_save = True
                if self.reminders_enabled:
                    self._seed_reminders(sub, now)
                    needs_save = True
                else:
                    sub.pop('reminders', None)
        
        if needs_save:
            self._save_security()
        # Notifications are delivered on the recipient's next heartbeat.

    def _record_mailbox_view(
        self,
        mailbox: str,
        sender_key: Optional[str],
        sender_id: Any,
        sender_short: str,
    ) -> None:
        if not self.notify_enabled or not sender_key:
            return
        entry = self._get_security_entry(mailbox)
        now = time.time()
        needs_save = False
        with self.security_lock:
            subscribers, messages = self._ensure_mailbox_state(entry)
            entry.setdefault('mailbox_name', mailbox)
            sub = self._ensure_subscriber_entry(entry, sender_key, node_id=sender_id, short=sender_short)
            sub['last_check'] = now
            sub.setdefault('first_seen', now)
            unread = sub.setdefault('unread', [])

            for msg_id, meta in list(messages.items()):
                readers = meta.setdefault('readers', {})
                if sender_key not in readers:
                    readers[sender_key] = now
                if msg_id in unread:
                    unread.remove(msg_id)
                    needs_save = True

            sub['unread'] = [msg_id for msg_id in unread if msg_id in messages]
            if not sub['unread']:
                if sub.get('pending_notice'):
                    sub['pending_notice'] = False
                    needs_save = True
                if sub.get('reminders'):
                    self._clear_reminders(sub)
                    needs_save = True

        if needs_save:
            self._save_security()

    def _mark_all_mailboxes_checked(
        self,
        sender_key: Optional[str],
        sender_id: Any,
        sender_short: str,
        *,
        exclude: Optional[str] = None,
    ) -> None:
        if not sender_key:
            return
        try:
            mailbox_names = self.mailboxes_for_user(sender_key)
        except Exception:
            return
        exclude_norm = self.store.normalize_mailbox(exclude) if exclude else None
        for other in mailbox_names:
            if exclude_norm and self.store.normalize_mailbox(other) == exclude_norm:
                continue
            try:
                self._record_mailbox_view(other, sender_key, sender_id, sender_short)
            except Exception:
                continue

    def _compose_engagement_message(self) -> str:
        suggestions = [
            "🎲 Need a break? Try `/games` for quick challenges.",
            "🧠 Test your brain with `/trivia` whenever you're ready.",
            "📖 Looking for inspiration? `/bible` shares a verse on demand.",
            "😂 Want a laugh? `/jokes` has something light-hearted.",
            "🌤️ Curious about the weather? `/weather` has the latest update.",
            "📋 Curious about everything else? `/menu` shows the highlights.",
        ]
        random.shuffle(suggestions)
        picked = suggestions[:3]
        intro = "👍 Got your reply — mail alerts are paused for now."
        return "\n".join([intro, *picked])

    def user_engaged(
        self,
        sender_key: Optional[str],
        node_id: Any = None,
        *,
        skip_prompt: bool = False,
    ) -> Optional[str]:
        if not sender_key:
            return None
        now = time.time()
        cleared = False
        with self.security_lock:
            for entry in self.security.values():
                subscribers = entry.get('subscribers') or {}
                messages = entry.get('messages') or {}
                sub = subscribers.get(sender_key)
                if not sub:
                    continue
                unread = sub.get('unread') or []
                if unread:
                    filtered = [mid for mid in unread if mid in messages]
                    if filtered != unread:
                        sub['unread'] = filtered
                        cleared = True
                if sub.get('pending_notice'):
                    sub['pending_notice'] = False
                    cleared = True
                if sub.get('reminders'):
                    self._clear_reminders(sub)
                    cleared = True
                sub['last_check'] = now
                if node_id is not None:
                    sub['node_id'] = node_id
            if cleared:
                self._save_security()
        had_auto = sender_key in self.active_auto_notifications
        if had_auto:
            self.active_auto_notifications.pop(sender_key, None)
        if node_id is not None:
            retains = deque()
            while self.events:
                event = self.events.popleft()
                if event.get('node_id') == node_id or event.get('sender_key') == sender_key:
                    continue
                retains.append(event)
            while retains:
                self.events.appendleft(retains.pop())
        should_prompt = (had_auto or cleared) and not skip_prompt and node_id is not None
        if should_prompt:
            last = self.last_engagement_prompt.get(sender_key, 0.0)
            if now - last >= 60:
                message = self._compose_engagement_message()
                if message:
                    self.last_engagement_prompt[sender_key] = now
                    return message
        return None

    # Public API ------------------------------------------------------
    def mailboxes_for_user(self, sender_key: Optional[str]) -> List[str]:
        if not sender_key:
            return []
        normalized_sender = str(sender_key).strip()
        if not normalized_sender:
            return []

        results: List[str] = []
        seen: Set[str] = set()
        with self.security_lock:
            for secure_key, entry in self.security.items():
                if not isinstance(entry, dict):
                    continue
                mailbox_name = entry.get('mailbox_name') or secure_key
                if not mailbox_name:
                    continue
                owner_key = entry.get('owner')
                owner_normalized = str(owner_key).strip() if owner_key else ""
                subscribers = entry.get('subscribers') or {}
                subscriber_keys = {str(key).strip() for key in subscribers.keys() if key}
                if normalized_sender != owner_normalized and normalized_sender not in subscriber_keys:
                    continue
                normalized_mailbox = self.store.normalize_mailbox(mailbox_name)
                if normalized_mailbox in seen:
                    continue
                seen.add(normalized_mailbox)
                results.append(mailbox_name)

        results.sort(key=lambda name: name.lower())
        return results

    def handle_reply_intent(
        self,
        sender_key: Optional[str],
        sender_id: Any,
        sender_short: str,
        text: str,
    ) -> Optional[PendingReply]:
        if not sender_key or not text:
            return None
        stripped = text.strip()
        if not stripped:
            return None
        lower = stripped.lower()
        if not lower.startswith('reply'):
            return None

        context = self._lookup_reply_context(sender_key)
        if not context:
            return PendingReply(
                "No recent inbox senders to reply to. Run `/c <mailbox>` first, then try `reply <number> <message>`.",
                "mail reply",
                chunk_delay=2.0,
            )

        parts = stripped.split()
        if len(parts) < 2:
            return PendingReply(
                "Usage: `reply <number> <message>` or `reply to <name> <message>`.",
                "mail reply",
            )

        idx = 1
        if parts[idx].lower() == 'to':
            idx += 1
            if idx >= len(parts):
                return PendingReply(
                    "Usage: `reply to <name> <message>`.",
                    "mail reply",
                )

        target_token = parts[idx]
        idx += 1
        if idx >= len(parts):
            return PendingReply(
                "Reply needs a message. Example: `reply 1 Thank you!`.",
                "mail reply",
            )

        message_text = " ".join(parts[idx:]).strip()
        if not message_text:
            return PendingReply(
                "Reply needs a message. Example: `reply 1 Thank you!`.",
                "mail reply",
            )

        entries = context.get('entries', [])
        entry = None
        if target_token.isdigit():
            try:
                desired = int(target_token)
            except Exception:
                desired = None
            if desired is not None:
                for item in entries:
                    if item.get('index') == desired:
                        entry = item
                        break
        if entry is None:
            names = context.get('names', {})
            lookup = target_token.lower()
            entry = names.get(lookup)
            if entry is None:
                for key, item in names.items():
                    if key.startswith(lookup):
                        entry = item
                        break
        if not entry:
            hints = ", ".join(f"{item.get('index')}: {item.get('short')}" for item in entries)
            return PendingReply(
                f"I couldn't match `{target_token}` to a recent sender. Options right now: {hints}.",
                "mail reply",
            )

        node_id = entry.get('node_id')
        if not node_id:
            return PendingReply(
                "I couldn't locate that user's radio ID. Ask them to send another message first.",
                "mail reply",
            )

        mailbox_name = entry.get('mailbox') or "their inbox"
        header = f"📬 Reply from {sender_short or sender_key} (via '{mailbox_name}')"
        outbound = f"{header}\n{message_text}"
        self._queue_event({
            'type': 'dm',
            'node_id': node_id,
            'text': outbound,
            'meta': {
                'kind': 'mail-reply',
                'from': sender_key,
                'mailbox': mailbox_name,
            },
        })
        ack_lines = [
            f"📨 Sent your reply directly to {entry.get('short')}.",
            "👍 Mail alerts are paused for now. Want more? Try `/games`, `/trivia`, or `/bible`.",
        ]
        self.user_engaged(sender_key, node_id=sender_id, skip_prompt=True)
        self.clean_log(
            f"Forwarded mailbox reply from {sender_short or sender_key} to {entry.get('short')} ({node_id})",
            "📨",
            show_always=False,
        )
        return PendingReply("\n".join(ack_lines), "mail reply", chunk_delay=2.0)

    def handle_send(
        self,
        *,
        sender_key: str,
        sender_id: Any,
        mailbox: str,
        body: str,
        sender_short: str,
    ) -> PendingReply:
        if not mailbox:
            return PendingReply("Mailbox name cannot be empty.", "/m command")

        entry = None
        if body:
            entry = {
                "body": body,
                "sender_id": str(sender_id),
                "sender_short": sender_short,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "mailbox": mailbox,
            }

        mailbox_exists = self.store.mailbox_exists(mailbox)
        if not mailbox_exists:
            self.pending_creation[sender_key] = {
                "mailbox": mailbox,
                "entry": dict(entry) if entry else None,
                "sender_short": sender_short,
                "sender_id": sender_id,
                "stage": "confirm",
            }
            prompt = f"📬 Oops, mailbox '{mailbox}' doesn't exist yet. Launch it now? Reply Y or N"
            return PendingReply(prompt, "/m create")

        if not entry:
            example = f"/mail {mailbox} your message here"
            return PendingReply(
                f"Mailbox '{mailbox}' already exists. Send mail by typing `{example}` with your own words.",
                "/m command",
            )

        try:
            _count, _, stored_message = self.store.append(mailbox, entry, allow_create=False)
        except KeyError:
            self.pending_creation[sender_key] = {
                "mailbox": mailbox,
                "entry": dict(entry),
                "sender_short": sender_short,
                "sender_id": sender_id,
                "stage": "confirm",
            }
            prompt = f"📬 Oops, mailbox '{mailbox}' doesn't exist yet. Launch it now? Reply Y or N"
            return PendingReply(prompt, "/m create")
        except Exception as exc:
            self.clean_log(f"Mail store write failed: {exc}")
            return PendingReply("Failed to store message. Please try again.", "/m command")

        self.clean_log(f"Stored mail for '{mailbox}' from {sender_short}")
        lines = [
            f"Saved message to '{mailbox}'.",
            f"Use /c {mailbox} to check the latest messages.",
        ]
        deletion_eta = self._format_deletion_eta(stored_message.get('timestamp'))
        lines.append(f"🗑️ This message auto-deletes around {deletion_eta}.")
        try:
            self._record_message_append(mailbox, stored_message, sender_key, sender_id, sender_short)
        except Exception as exc:
            self.clean_log(f"Mail notification error: {exc}", "⚠️")
        if self.stats:
            try:
                self.stats.record_mail_sent(mailbox)
            except Exception:
                pass
        return PendingReply("\n".join(lines), "/m command")

    def has_pending_creation(self, sender_key: str) -> bool:
        return sender_key in self.pending_creation

    def handle_creation_response(self, sender_key: str, text: str) -> PendingReply:
        state = self.pending_creation.get(sender_key)
        if not state:
            return PendingReply("Mailbox setup expired. Please start again with /m.", "/m command")

        response = (text or "").strip()
        if not response:
            return PendingReply("❓ Please reply with Y or N.", "/m create")

        lower = response.lower()
        mailbox = state.get("mailbox", "")
        stage = state.get("stage", "confirm")

        if stage == "confirm":
            if lower in YES_RESPONSES:
                state['stage'] = 'set_pin'
                self.pending_creation[sender_key] = state
                return PendingReply(
                    "🔐 Pick a PIN for this mailbox (4-8 digits) or reply SKIP to leave it open.",
                    "/m create",
                )
            if lower in NO_RESPONSES or lower in CANCEL_RESPONSES:
                self.pending_creation.pop(sender_key, None)
                return PendingReply(f"👍 No problem, '{mailbox}' was not created.", "/m create")
            return PendingReply("❓ Please reply with Y or N to create the mailbox.", "/m create")

        if stage == "set_pin":
            if lower in CANCEL_RESPONSES:
                self.pending_creation.pop(sender_key, None)
                return PendingReply(f"👍 Cancelled mailbox setup for '{mailbox}'.", "/m create")
            if lower == "skip":
                return self._finalize_mailbox_creation(sender_key, state, pin=None)
            candidate = response.strip()
            if not candidate.isdigit() or len(candidate) < 4 or len(candidate) > 8:
                return PendingReply(
                    "🔢 PIN must be 4-8 digits. Reply with numbers only or SKIP.",
                    "/m create",
                )
            return self._finalize_mailbox_creation(sender_key, state, pin=candidate)

        self.pending_creation.pop(sender_key, None)
        return PendingReply("Mailbox setup expired. Please start again with /m.", "/m command")

    def handle_check(
        self,
        sender_key: str,
        sender_id: Any,
        sender_short: str,
        mailbox: str,
        remainder: str,
    ) -> PendingReply:
        if not mailbox:
            return PendingReply("Mailbox name cannot be empty.", "/c command")

        pin_value, remainder = self._extract_pin(remainder)

        existed = self.store.mailbox_exists(mailbox)
        if not existed:
            fun_reply = random.choice(MISSING_MAILBOX_RESPONSES).format(mailbox=mailbox)
            return PendingReply(fun_reply, "/c command", chunk_delay=4.0)

        auth_error = self._authorise_mailbox(sender_key, mailbox, pin_value)
        if auth_error:
            return auth_error

        remainder = remainder.strip()
        if not remainder:
            return self._build_mailbox_result(
                mailbox,
                existed,
                sender_key=sender_key,
                sender_id=sender_id,
                sender_short=sender_short,
                is_search=False,
            )

        rest_lower = remainder.lower()
        if rest_lower.startswith("search "):
            query = remainder[6:].strip()
        elif rest_lower == "search":
            return PendingReply("Use this by typing: /c mailbox your question", "/c command")
        else:
            query = remainder.strip()

        query = self._strip_quotes(query)
        if not query:
            return PendingReply("Use this by typing: /c mailbox your question", "/c command")

        return self._build_mailbox_result(
            mailbox,
            existed,
            sender_key=sender_key,
            sender_id=sender_id,
            sender_short=sender_short,
            is_search=True,
            query=query,
        )

    # Internal helpers ------------------------------------------------
    def _summarize_mail_search(self, mailbox: str, query: str, messages: List[dict]) -> str:
        if not messages:
            return f"No matches found for '{query}'."

        query_norm = (query or "").strip().lower()
        if not query_norm:
            return f"No matches found for '{query}'."

        limited = messages[-self.search_max_messages:]
        matches: List[str] = []
        for idx, message in enumerate(reversed(limited), start=1):
            body = str(message.get('body', '') or '')
            sender = message.get('sender_short') or message.get('sender_id') or 'unknown'
            text_norm = body.lower()
            sender_norm = str(sender).lower()
            if query_norm in text_norm or query_norm in sender_norm:
                timestamp = self._format_mail_timestamp(message.get('timestamp', ''))
                matches.append(f"{idx}) {timestamp} {sender}: {body.strip()}")
            if len(matches) >= 5:
                break

        if not matches:
            return f"No matches found for '{query}'."
        lines = [f"🔍 Matches in '{mailbox}' (newest first)"] + matches
        return "\n".join(lines)

    def _build_mailbox_result(
        self,
        mailbox: str,
        existed: bool,
        *,
        sender_key: Optional[str],
        sender_id: Any,
        sender_short: str,
        is_search: bool = False,
        query: Optional[str] = None,
    ) -> PendingReply:
        try:
            self._record_mailbox_view(mailbox, sender_key, sender_id, sender_short)
        except Exception as exc:
            self.clean_log(f"Mailbox reminder error: {exc}", "⚠️")
        if is_search:
            messages = self.store.get_all(mailbox)
            if not messages:
                replies = MISSING_MAILBOX_RESPONSES if not existed else EMPTY_MAILBOX_RESPONSES
                fun_reply = random.choice(replies).format(mailbox=mailbox)
                return PendingReply(fun_reply, "/c search", chunk_delay=4.0)
            summary = self._summarize_mail_search(mailbox, query or "", messages)
            self.clean_log(f"Mailbox search '{mailbox}' query '{(query or '').strip()}'", "🔎")
            return PendingReply(
                summary,
                "/c search",
                chunk_delay=4.0,
                follow_up_text=f"🧹 Clear '{mailbox}' anytime with /wipe mailbox {mailbox}",
                follow_up_delay=self.follow_up_delay,
            )

        messages = self.store.get_last(mailbox, self.display_max_messages)
        if not messages:
            replies = MISSING_MAILBOX_RESPONSES if not existed else EMPTY_MAILBOX_RESPONSES
            fun_reply = random.choice(replies).format(mailbox=mailbox)
            return PendingReply(fun_reply, "/c command", chunk_delay=4.0)
        ordered = list(reversed(messages))
        lines = [self._format_mail_line(idx, msg) for idx, msg in enumerate(ordered, start=1)]
        mailbox_label = ordered[0].get("mailbox") or mailbox
        header = f"📥 Inbox '{mailbox_label}' (newest first, showing {len(ordered)} messages)"
        response_sections = [header] + lines
        reply_entries: List[Dict[str, Any]] = []
        for idx, msg in enumerate(ordered, start=1):
            reply_entries.append(
                {
                    'index': idx,
                    'sender_id': msg.get('sender_id'),
                    'sender_short': msg.get('sender_short') or msg.get('sender_id'),
                    'message_id': msg.get('id'),
                    'mailbox': mailbox_label,
                }
            )
        self._store_reply_context(sender_key, reply_entries)
        if reply_entries:
            response_sections.append(
                ""
            )
            response_sections.append(
                "Reply with `reply <number> <message>` or `reply to <name> <message>` to DM the sender directly."
            )
        response_sections.append(
            "Checking any inbox pauses all mail alerts until a new message arrives."
        )
        response_text = "\n".join(response_sections)
        if sender_key:
            self._mark_all_mailboxes_checked(sender_key, sender_id, sender_short, exclude=mailbox)
            self.user_engaged(sender_key, node_id=sender_id, skip_prompt=True)
        self.clean_log(f"Mailbox '{mailbox}' checked")
        return PendingReply(
            response_text,
            "/c command",
            chunk_delay=4.0,
            follow_up_text=f"🧹 Clear '{mailbox}' with /wipe mailbox {mailbox}",
            follow_up_delay=self.follow_up_delay,
        )

    def handle_heartbeat(self, sender_key: Optional[str], sender_id: Any, *, allow_send: bool = True) -> None:
        if not self.notify_enabled or not sender_key:
            return
        now = time.time()
        aggregated: List[Tuple[str, int]] = []
        reminder_actions: List[Dict[str, Any]] = []
        pending_notice_subs: List[Dict[str, Any]] = []
        needs_save = False
        node_id_candidate = sender_id
        with self.security_lock:
            for mailbox_key, entry in self.security.items():
                subscribers = entry.get('subscribers')
                if not subscribers:
                    continue
                sub = subscribers.get(sender_key)
                if not sub:
                    continue
                sub['last_heartbeat'] = now
                if sender_id is not None and sub.get('node_id') != sender_id:
                    sub['node_id'] = sender_id
                    needs_save = True
                messages = entry.get('messages') or {}
                unread_ids = [mid for mid in sub.get('unread', []) if mid in messages]
                sub['unread'] = unread_ids
                if not unread_ids:
                    if sub.get('pending_notice'):
                        sub['pending_notice'] = False
                        needs_save = True
                    if sub.get('reminders'):
                        self._clear_reminders(sub)
                        needs_save = True
                    continue
                mailbox_name = entry.get('mailbox_name', mailbox_key)
                aggregated.append((mailbox_name, len(unread_ids)))
                if node_id_candidate is None:
                    node_id_candidate = sub.get('node_id')
                if sub.get('pending_notice'):
                    pending_notice_subs.append(sub)
                if self.reminders_enabled and self.reminder_max_count > 0:
                    reminders = sub.get('reminders')
                    if not isinstance(reminders, dict) or not reminders:
                        self._seed_reminders(sub, now)
                        reminders = sub.get('reminders')
                        needs_save = True
                    if isinstance(reminders, dict) and reminders:
                        count_sent = int(reminders.get('count', 0))
                        next_ts = reminders.get('next_ts')
                        expiry_ts = reminders.get('expiry_ts')
                        if next_ts is not None and count_sent < self.reminder_max_count:
                            if expiry_ts is None or now <= expiry_ts:
                                if now >= next_ts and self._within_active_window(now):
                                    reminder_actions.append(
                                        {
                                            'reminders': reminders,
                                            'sub': sub,
                                            'mailbox': mailbox_name,
                                            'next_count': count_sent + 1,
                                        }
                                    )
                            else:
                                reminders['next_ts'] = None
                                needs_save = True
        if not aggregated:
            if sender_key in self.active_auto_notifications:
                self.active_auto_notifications.pop(sender_key, None)
            if needs_save:
                self._save_security()
            return

        if not allow_send:
            if needs_save:
                self._save_security()
            return

        if node_id_candidate is None:
            if needs_save:
                self._save_security()
            return

        should_send = bool(pending_notice_subs or reminder_actions)
        if not should_send:
            if needs_save:
                self._save_security()
            return

        reminder_max_index = 0
        for sub in pending_notice_subs:
            sub['pending_notice'] = False
            sub['node_id'] = node_id_candidate
            needs_save = True
        for action in reminder_actions:
            reminders = action['reminders']
            sub = action['sub']
            next_count = action['next_count']
            reminders['count'] = next_count
            reminders['last_sent_ts'] = now
            reminders['node_id'] = node_id_candidate
            sub['node_id'] = node_id_candidate
            if next_count >= self.reminder_max_count:
                reminders['next_ts'] = None
            else:
                reminders['next_ts'] = self._compute_next_reminder_time(now)
            reminder_max_index = max(reminder_max_index, next_count)
            needs_save = True

        if needs_save:
            self._save_security()

        capped = aggregated[:6]
        summary = ", ".join(f"{name} ({count})" for name, count in capped)
        if len(aggregated) > len(capped):
            summary += ", …"
        example_mailbox = aggregated[0][0]
        text_lines = [
            f"📬 Unread mail waiting: {summary}.",
            f"Reply `/c <mailbox>` (try `/c {example_mailbox}`) to read — checking any inbox pauses alerts until new mail arrives.",
        ]
        if reminder_max_index:
            text_lines.append(f"⏰ Reminder {reminder_max_index}/{self.reminder_max_count}.")
        message_text = "\n".join(text_lines)
        self._queue_event(
            {
                'type': 'dm',
                'node_id': node_id_candidate,
                'text': message_text,
                'sender_key': sender_key,
                'meta': {'kind': 'mail-alert'},
            }
        )
        try:
            self.clean_log(
                f"Mailbox alert for {sender_key}: {summary}",
                "📬",
                show_always=False,
            )
        except Exception:
            pass
        self.active_auto_notifications[sender_key] = {
            'timestamp': now,
            'counts': aggregated,
            'node_id': node_id_candidate,
        }

    def flush_notifications(self, interface, send_fn, can_send: bool = True) -> None:
        if not self.events:
            return
        processed_any = False
        buffer = deque()
        while self.events:
            event = self.events.popleft()
            if event.get('type') != 'dm':
                continue
            if not can_send:
                buffer.append(event)
                continue
            node_id = event.get('node_id')
            text = event.get('text')
            if interface is None or node_id is None or not text or send_fn is None:
                continue
            try:
                send_fn(interface, text, node_id)
                processed_any = True
            except Exception as exc:
                self.clean_log(f"Mail notification send failed: {exc}", "⚠️")
        while buffer:
            self.events.appendleft(buffer.pop())
        if processed_any:
            self.clean_log("Mailbox notifications flushed", "📬", show_always=False)

    def _finalize_mailbox_creation(self, sender_key: str, state: Dict[str, Any], pin: Optional[str]) -> PendingReply:
        mailbox = state.get("mailbox")
        raw_entry = state.get("entry")
        entry = dict(raw_entry) if raw_entry else None
        if not mailbox:
            self.pending_creation.pop(sender_key, None)
            return PendingReply("Mailbox setup information expired. Please start again with /m.", "/m command")

        sender_short = (entry or {}).get("sender_short") or state.get("sender_short") or mailbox

        try:
            if entry:
                stored_entry = entry
                entry.setdefault("mailbox", mailbox)
                _, created, stored_entry = self.store.append(mailbox, entry, allow_create=True)
            else:
                created = self.store.create_mailbox(mailbox)
        except Exception as exc:
            self.clean_log(f"Mail store write failed while creating '{mailbox}': {exc}")
            self.pending_creation.pop(sender_key, None)
            return PendingReply("Failed to create mailbox. Please try again with /m.", "/m command")

        if created:
            self.clean_log(f"New mailbox '{mailbox}' created by {sender_short}", "🗂️")
            if self.stats:
                try:
                    self.stats.record_mailbox_created(mailbox)
                except Exception:
                    pass
        if entry:
            self.clean_log(f"Stored mail for '{mailbox}' from {sender_short}", "✉️")

        self._set_mailbox_security(mailbox, sender_key, pin)
        try:
            security_entry = self._get_security_entry(mailbox)
            owner_key = security_entry.get('owner')
            if owner_key:
                with self.security_lock:
                    self._ensure_subscriber_entry(
                        security_entry,
                        owner_key,
                        node_id=state.get('sender_id'),
                        short=sender_short,
                    )
                    security_entry.setdefault('mailbox_name', mailbox)
                    self._save_security()
        except Exception:
            pass
        self.pending_creation.pop(sender_key, None)

        lines = [f"🎉 Mailbox '{mailbox}' ready."]
        if pin:
            lines.append("🔐 PIN set. Share it carefully!")
        if entry:
            lines.append("✉️ Message saved—recipients will be notified.")
            deletion_eta = self._format_deletion_eta(stored_entry.get('timestamp') if stored_entry else None)
            lines.append(f"🗑️ Auto-deletes around {deletion_eta}.")
            try:
                self._record_message_append(mailbox, stored_entry, sender_key, state.get('sender_id'), sender_short)
            except Exception as exc:
                self.clean_log(f"Mail notification error: {exc}", "⚠️")
        else:
            lines.append("📭 Inbox created with no mail yet.")
        lines.append(f"📥 Read: /c {mailbox}")
        lines.append(f"🔍 Search: /c {mailbox} tomorrow plans")
        lines.append(f"🧹 Wipe later: /wipe mailbox {mailbox}")
        lines.append("📸 Screenshot this so you don't lose it.")
        return PendingReply("\n".join(lines), "/m command")

    def handle_wipe(
        self,
        mailbox: str,
        *,
        actor_key: Optional[str] = None,
        is_admin: bool = False,
    ) -> PendingReply:
        if not mailbox:
            return PendingReply("Mailbox name cannot be empty.", "/wipe command")
        existed = self.store.mailbox_exists(mailbox)
        if not existed:
            fun_reply = random.choice(MISSING_MAILBOX_RESPONSES).format(mailbox=mailbox)
            return PendingReply(fun_reply, "/wipe command")

        normalized_actor = (actor_key or "").strip()
        if not is_admin:
            if not normalized_actor:
                self.clean_log(
                    f"Mailbox wipe denied for '{mailbox}' — missing actor key",
                    "⛔",
                )
                return PendingReply(
                    "⛔ I couldn't verify you're the mailbox owner. Try again from the same device or ask an admin to help.",
                    "/wipe command",
                )
            owner_key = ""
            with self.security_lock:
                entry = self.security.get(self._security_key(mailbox))
                if entry:
                    owner_key = str(entry.get('owner') or "").strip()
            if owner_key:
                if normalized_actor != owner_key:
                    self.clean_log(
                        f"Mailbox wipe denied for '{mailbox}' — {normalized_actor} is not owner ({owner_key})",
                        "⛔",
                    )
                    return PendingReply(
                        "⛔ Only the mailbox owner can wipe this inbox.",
                        "/wipe command",
                    )
            else:
                self.clean_log(
                    f"Mailbox wipe denied for '{mailbox}' — no owner recorded",
                    "⛔",
                )
                return PendingReply(
                    "⛔ This inbox doesn't have an owner on file. Ask an admin to help clear it if needed.",
                    "/wipe command",
                )

        try:
            cleared = self.store.clear_mailbox(mailbox)
        except Exception as exc:
            self.clean_log(f"Mail wipe failed for '{mailbox}': {exc}")
            return PendingReply("Failed to wipe mailbox. Please try again.", "/wipe command")

        if cleared:
            self.clean_log(f"Mailbox '{mailbox}' wiped", "🧹")
            with self.security_lock:
                entry = self.security.get(self._security_key(mailbox))
                if entry:
                    entry.setdefault('messages', {}).clear()
                    subscribers = entry.setdefault('subscribers', {})
                    for sub in subscribers.values():
                        sub['unread'] = []
                        sub['reminders'] = {}
                        sub['pending_notice'] = False
                    self._save_security()
            return PendingReply(f"🧹 Mailbox '{mailbox}' is now empty.", "/wipe command")
        fun_reply = random.choice(MISSING_MAILBOX_RESPONSES).format(mailbox=mailbox)
        return PendingReply(fun_reply, "/wipe command")
