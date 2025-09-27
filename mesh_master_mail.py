"""Lightweight JSON-backed mail store for the simplified mesh mail system."""

from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

LockType = threading.Lock


class MailStore:
    """Persist mailbox messages to a JSON file with simple helpers."""

    def __init__(self, path: str):
        self._path = path
        self._lock: LockType = threading.Lock()
        self._data: Dict[str, List[dict]] = {}
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
        key = self._mailbox_key(mailbox_name)
        with self._lock:
            messages = self._data.get(key)
            created = False
            if messages is None:
                if not allow_create:
                    raise KeyError(f"Mailbox '{mailbox_name}' does not exist")
                messages = []
                self._data[key] = messages
                created = True
            messages.append(entry)
            self._persist()
            return len(messages), created, dict(entry)

    def get_last(self, mailbox: str, limit: int = 3) -> List[dict]:
        key = self._mailbox_key(mailbox)
        with self._lock:
            items = self._data.get(key, [])
            if limit <= 0:
                return []
            return [dict(item) for item in items[-limit:]]

    def get_all(self, mailbox: str) -> List[dict]:
        key = self._mailbox_key(mailbox)
        with self._lock:
            items = self._data.get(key, [])
            return [dict(item) for item in items]

    def mailbox_exists(self, mailbox: str) -> bool:
        key = self._mailbox_key(mailbox)
        with self._lock:
            return key in self._data

    def mailbox_count(self, mailbox: str) -> int:
        key = self._mailbox_key(mailbox)
        with self._lock:
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
