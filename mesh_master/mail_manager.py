from __future__ import annotations

import hashlib
import json
import os
import random
import threading
import time
import textwrap
from collections import deque
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from mesh_master_mail import MailStore
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
        reminder_interval_seconds: float,
        reminder_expiry_seconds: float,
        reminder_max_count: int,
        include_self_notifications: bool,
        heartbeat_only: bool,
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
        self.reminder_max_count = max(1, int(reminder_max_count))
        self.include_self_notifications = bool(include_self_notifications)
        self.heartbeat_only = bool(heartbeat_only)
        self.events = deque()

    # Utility helpers -------------------------------------------------
    def _format_mail_timestamp(self, ts: str) -> str:
        try:
            normalized = ts
            if normalized.endswith('Z'):
                normalized = normalized[:-1] + '+00:00'
            dt = datetime.fromisoformat(normalized)
            return dt.strftime(MAIL_TIME_DISPLAY)
        except Exception:
            return ts[:16]

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

    def _queue_event(self, event: Dict[str, Any]) -> None:
        self.events.append(event)

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
            entry['owner'] = owner or entry.get('owner')
            if pin:
                entry['pin_hash'] = self._hash_pin(pin)
            else:
                entry['pin_hash'] = None
            entry.setdefault('failures', {})
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
        for token in tokens:
            lower = token.lower()
            if lower.startswith("pin=") and pin_value is None:
                pin_candidate = token[4:].strip().rstrip(',.;:')
                if pin_candidate:
                    pin_value = pin_candidate
                continue
            remainder.append(token)
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

        if provided_pin and self._verify_pin(entry, provided_pin):
            self._reset_failures(entry, sender_key)
            return None

        if self._is_blacklisted(entry, sender_key):
            if provided_pin and self._verify_pin(entry, provided_pin):
                self._reset_failures(entry, sender_key)
                return None
            return PendingReply(
                "⛔ Access permanently blocked after repeated incorrect PIN attempts.",
                "/c command",
            )

        if not provided_pin:
            return PendingReply(
                f"🔐 Mailbox '{mailbox}' requires a PIN. Use `/c {mailbox} PIN=1234`.",
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
        notifications: List[Dict[str, Any]] = []
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
            meta.setdefault('notified_sender', False)
            needs_save = True

            for sub_key, sub in subscribers.items():
                if not sub_key:
                    continue
                if not self.include_self_notifications and sub_key == sender_key:
                    continue
                node_id = sub.get('node_id')
                if node_id is None:
                    continue
                unread = sub.setdefault('unread', [])
                if msg_id not in unread:
                    unread.append(msg_id)
                    needs_save = True
                reminders = sub.setdefault('reminders', {})
                if msg_id in reminders:
                    reminders.pop(msg_id, None)
                    needs_save = True
                notifications.append({'node_id': node_id})

        if needs_save:
            self._save_security()

        if not notifications:
            return

        snippet = textwrap.shorten(str(message.get('body', '') or ''), width=70, placeholder="…")
        base_text = f"📬 New mail in '{mailbox}' from {sender_short}"
        text = f"{base_text}: {snippet}" if snippet else base_text
        for note in notifications:
            node_id = note.get('node_id')
            if node_id is None:
                continue
            self._queue_event({'type': 'dm', 'node_id': node_id, 'text': text})

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
        sender_notifications: List[Dict[str, Any]] = []
        with self.security_lock:
            subscribers, messages = self._ensure_mailbox_state(entry)
            entry.setdefault('mailbox_name', mailbox)
            sub = subscribers.setdefault(
                sender_key,
                {
                    'first_seen': now,
                    'last_check': now,
                    'node_id': sender_id,
                    'short': sender_short,
                    'unread': [],
                    'reminders': {},
                },
            )
            sub['last_check'] = now
            sub['node_id'] = sender_id
            sub['short'] = sender_short
            sub.setdefault('first_seen', now)
            unread = sub.setdefault('unread', [])
            reminders = sub.setdefault('reminders', {})

            for msg_id, meta in list(messages.items()):
                readers = meta.setdefault('readers', {})
                if sender_key not in readers:
                    readers[sender_key] = now
                    sender_key_meta = meta.get('sender_key')
                    if sender_key_meta and sender_key_meta != sender_key and not meta.get('notified_sender'):
                        node_id = meta.get('sender_node')
                        if node_id is not None:
                            reader_name = sender_short or sender_key
                            text = (
                                f"✅ {reader_name} read your message in '{mailbox}'. You won't receive more alerts about it."
                            )
                            sender_notifications.append({'node_id': node_id, 'text': text})
                            meta['notified_sender'] = True
                            needs_save = True
                if msg_id in unread:
                    unread.remove(msg_id)
                    needs_save = True
                if msg_id in reminders:
                    reminders.pop(msg_id, None)
                    needs_save = True

            sub['unread'] = [msg_id for msg_id in unread if msg_id in messages]
            sub['reminders'] = reminders

        if needs_save:
            self._save_security()

        for note in sender_notifications:
            node_id = note.get('node_id')
            text = note.get('text')
            if node_id is None or not text:
                continue
            self._queue_event({'type': 'dm', 'node_id': node_id, 'text': text})

    # Public API ------------------------------------------------------
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
            return PendingReply(
                f"Mailbox '{mailbox}' already exists. Include a message after the mailbox name to send mail.",
                "/m command",
            )

        try:
            count, _, stored_message = self.store.append(mailbox, entry, allow_create=False)
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
        suffix = "" if count == 1 else "s"
        lines = [
            f"Saved message to '{mailbox}'. Inbox now has {count} message{suffix}.",
            f"Use /c {mailbox} to check the latest messages.",
        ]
        try:
            self._record_message_append(mailbox, stored_message, sender_key, sender_id, sender_short)
        except Exception as exc:
            self.clean_log(f"Mail notification error: {exc}", "⚠️")
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
        response_text = "\n".join([header] + lines)
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
        notifications: List[Dict[str, Any]] = []
        needs_save = False
        with self.security_lock:
            for mailbox_key, entry in self.security.items():
                subscribers = entry.get('subscribers')
                if not subscribers or sender_key not in subscribers:
                    continue
                sub = subscribers[sender_key]
                sub['last_heartbeat'] = now
                unread_ids = [mid for mid in sub.get('unread', []) if mid in entry.get('messages', {})]
                if not unread_ids:
                    continue
                node_id = sub.get('node_id')
                if node_id is None:
                    continue
                reminders = sub.setdefault('reminders', {})
                messages = entry.get('messages', {})
                for msg_id in unread_ids:
                    meta = messages.get(msg_id)
                    if not meta:
                        continue
                    if not self.include_self_notifications and sender_key == meta.get('sender_key'):
                        continue
                    msg_time = self._iso_to_timestamp(meta.get('timestamp', now))
                    if now - msg_time > self.reminder_expiry:
                        continue
                    info = reminders.setdefault(msg_id, {'last': 0.0, 'count': 0})
                    if info.get('count', 0) >= self.reminder_max_count:
                        continue
                    if now - info.get('last', 0.0) < self.reminder_interval:
                        continue
                    if not allow_send:
                        continue
                    info['last'] = now
                    info['count'] = info.get('count', 0) + 1
                    needs_save = True
                    mailbox_name = entry.get('mailbox_name', mailbox_key)
                    notifications.append(
                        {
                            'node_id': node_id,
                            'text': f"⏰ Inbox '{mailbox_name}' still has unread mail. Reply `/c {mailbox_name}` to check.",
                        }
                    )
        if needs_save:
            self._save_security()
        for notice in notifications:
            node_id = notice.get('node_id')
            text = notice.get('text')
            if node_id is None or not text:
                continue
            self._queue_event({'type': 'dm', 'node_id': node_id, 'text': text})

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
                count, created, stored_entry = self.store.append(mailbox, entry, allow_create=True)
            else:
                created = self.store.create_mailbox(mailbox)
                count = self.store.mailbox_count(mailbox)
        except Exception as exc:
            self.clean_log(f"Mail store write failed while creating '{mailbox}': {exc}")
            self.pending_creation.pop(sender_key, None)
            return PendingReply("Failed to create mailbox. Please try again with /m.", "/m command")

        if created:
            self.clean_log(f"New mailbox '{mailbox}' created by {sender_short}", "🗂️")
        if entry:
            self.clean_log(f"Stored mail for '{mailbox}' from {sender_short}", "✉️")

        self._set_mailbox_security(mailbox, sender_key, pin)
        self.pending_creation.pop(sender_key, None)

        suffix = "" if count == 1 else "s"
        lines = [f"🎉 Mailbox '{mailbox}' ready."]
        if pin:
            lines.append("🔐 PIN set. Share it carefully!")
        if entry:
            lines.append(f"✉️ Message saved—now {count} message{suffix} inside.")
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

    def handle_wipe(self, mailbox: str) -> PendingReply:
        if not mailbox:
            return PendingReply("Mailbox name cannot be empty.", "/wipe command")
        existed = self.store.mailbox_exists(mailbox)
        if not existed:
            fun_reply = random.choice(MISSING_MAILBOX_RESPONSES).format(mailbox=mailbox)
            return PendingReply(fun_reply, "/wipe command")
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
                    self._save_security()
            return PendingReply(f"🧹 Mailbox '{mailbox}' is now empty.", "/wipe command")
        fun_reply = random.choice(MISSING_MAILBOX_RESPONSES).format(mailbox=mailbox)
        return PendingReply(fun_reply, "/wipe command")
