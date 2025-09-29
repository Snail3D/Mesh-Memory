"""Lightweight JSON-backed mail store for the simplified mesh mail system."""

from __future__ import annotations

import json
import os
import threading
import uuid
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

LockType = threading.Lock

MAIL_RETENTION_SECONDS = 30 * 24 * 60 * 60  # 30 days


class MailStore:
    """Persist mailbox messages to a JSON file with simple helpers."""

    def __init__(self, path: str, *, limit: int = 10):
        self._path = path
        self._lock: LockType = threading.Lock()
        self._data: Dict[str, List[dict]] = {}
        try:
            self._limit = max(1, int(limit))
        except Exception:
            self._limit = 10
        self._load()

    def _mailbox_key(self, mailbox: str) -> str:
        return mailbox.strip().lower()

    def _load(self) -> None:
        if not os.path.exists(self._path):
            self._data = {}
            return
        try:
            with open(self._path, "r", encoding="utf-8") as fh:
                raw = json.load(fh)
            if isinstance(raw, dict):
                clean: Dict[str, List[dict]] = {}
                for key, value in raw.items():
                    if not isinstance(key, str):
                        continue
                    if isinstance(value, list):
                        clean[key] = [entry for entry in value if isinstance(entry, dict)]
                self._data = clean
            else:
                self._data = {}
        except Exception:
            self._data = {}
        with self._lock:
            if self._prune_locked():
                self._persist()

    def _parse_timestamp(self, value: Any) -> Optional[datetime]:
        if not value:
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
                if text.endswith('Z'):
                    text = text[:-1] + '+00:00'
                dt = datetime.fromisoformat(text)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except Exception:
                return None
        return None

    def _is_mail_expired(self, timestamp: Any) -> bool:
        ts = self._parse_timestamp(timestamp)
        if ts is None:
            return False
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=MAIL_RETENTION_SECONDS)
        return ts < cutoff

    def _prune_locked(self) -> bool:
        """Remove messages older than the retention window. Returns True if data changed."""
        changed = False
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=MAIL_RETENTION_SECONDS)
        for mailbox, messages in list(self._data.items()):
            if not isinstance(messages, list):
                continue
            filtered: List[dict] = []
            for msg in messages:
                timestamp = msg.get('timestamp')
                ts = self._parse_timestamp(timestamp)
                if ts is not None and ts < cutoff:
                    changed = True
                    continue
                filtered.append(msg)
            if len(filtered) != len(messages):
                self._data[mailbox] = filtered
        return changed

    def _persist(self) -> None:
        tmp_path = f"{self._path}.tmp"
        directory = os.path.dirname(self._path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        with open(tmp_path, "w", encoding="utf-8") as fh:
            json.dump(self._data, fh, ensure_ascii=True, indent=2)
        os.replace(tmp_path, self._path)

    def create_mailbox(self, mailbox: str) -> bool:
        mailbox_name = mailbox.strip()
        if not mailbox_name:
            raise ValueError("Mailbox name cannot be empty")
        key = self._mailbox_key(mailbox_name)
        with self._lock:
            if key in self._data:
                return False
            self._data[key] = []
            self._persist()
            return True

    def normalize_mailbox(self, mailbox: str) -> str:
        return self._mailbox_key(mailbox)

    def append(
        self,
        mailbox: str,
        message: Dict[str, Any],
        allow_create: bool = True,
    ) -> Tuple[int, bool, dict]:
        """Append a message to a mailbox, creating it when allowed."""
        mailbox_name = mailbox.strip()
        if not mailbox_name:
            raise ValueError("Mailbox name cannot be empty")
        entry = dict(message)
        entry.setdefault("mailbox", mailbox_name)
        entry.setdefault("timestamp", datetime.now(timezone.utc).isoformat())
        entry.setdefault("id", str(uuid.uuid4()))
        key = self._mailbox_key(mailbox_name)
        with self._lock:
            if self._prune_locked():
                self._persist()
            messages = self._data.get(key)
            created = False
            if messages is None:
                if not allow_create:
                    raise KeyError(f"Mailbox '{mailbox_name}' does not exist")
                messages = []
                self._data[key] = messages
                created = True
            messages.append(entry)
            if self._limit and len(messages) > self._limit:
                excess = len(messages) - self._limit
                if excess > 0:
                    del messages[:excess]
            self._persist()
            return len(messages), created, dict(entry)

    def get_last(self, mailbox: str, limit: int = 3) -> List[dict]:
        key = self._mailbox_key(mailbox)
        with self._lock:
            if self._prune_locked():
                self._persist()
            items = self._data.get(key, [])
            if limit <= 0:
                return []
            return [dict(item) for item in items[-limit:]]

    def get_all(self, mailbox: str) -> List[dict]:
        key = self._mailbox_key(mailbox)
        with self._lock:
            if self._prune_locked():
                self._persist()
            items = self._data.get(key, [])
            return [dict(item) for item in items]

    def mailbox_exists(self, mailbox: str) -> bool:
        key = self._mailbox_key(mailbox)
        with self._lock:
            return key in self._data

    def mailbox_count(self, mailbox: str) -> int:
        key = self._mailbox_key(mailbox)
        with self._lock:
            if self._prune_locked():
                self._persist()
            return len(self._data.get(key, []))

    def clear_mailbox(self, mailbox: str) -> bool:
        key = self._mailbox_key(mailbox)
        with self._lock:
            if key not in self._data:
                return False
            if not self._data[key]:
                return True
            self._data[key] = []
            self._persist()
            return True

    def delete_mailbox(self, mailbox: str) -> bool:
        """Remove a mailbox entirely. Currently unused but handy for cleanup."""
        key = self._mailbox_key(mailbox)
        with self._lock:
            if key not in self._data:
                return False
            self._data.pop(key, None)
            self._persist()
            return True

    def list_mailboxes(self) -> List[str]:
        with self._lock:
            return list(self._data.keys())
