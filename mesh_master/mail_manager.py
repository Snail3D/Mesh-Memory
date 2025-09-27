from __future__ import annotations

import random
import textwrap
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

import requests

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


class MailManager:
    def __init__(
        self,
        *,
        store_path: str,
        clean_log: Callable[..., None],
        ai_log: Callable[..., None],
        ollama_url: Optional[str],
        search_model: str,
        search_timeout: int,
        search_num_ctx: int,
        search_max_messages: int,
    ) -> None:
        self.store = MailStore(store_path)
        self.clean_log = clean_log
        self.ai_log = ai_log
        self.ollama_url = ollama_url
        self.search_model = search_model
        self.search_timeout = search_timeout
        self.search_num_ctx = search_num_ctx
        self.search_max_messages = search_max_messages
        self.pending_creation: Dict[str, Dict[str, Any]] = {}

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

    def _format_mail_line(self, index: int, message: dict, body_limit: int = 24) -> str:
        sender_raw = message.get('sender_short') or message.get('sender_id') or 'unknown'
        sender = self._shorten_sender(str(sender_raw), limit=14)
        body = self._shorten_mail_body(message.get('body', ''), limit=body_limit)
        timestamp = self._format_mail_timestamp(message.get('timestamp', ''))
        return f"{index}) {timestamp} {sender}: {body}"

    def _shorten_mail_body(self, text: str, limit: int = 280) -> str:
        cleaned = " ".join((text or "").split())
        if len(cleaned) > limit:
            return cleaned[: limit - 3] + "..."
        return cleaned

    def _format_error(self, detail: str) -> str:
        message = (detail or "unknown error").strip()
        if len(message) > 120:
            message = message[:117] + "..."
        return f"⚠️ Mail search failed: {message}"

    def _strip_quotes(self, text: str) -> str:
        if not text:
            return text
        if len(text) >= 2 and text[0] == text[-1] and text[0] in {'"', "'"}:
            return text[1:-1].strip()
        return text

    def _summarize_mail_search(self, mailbox: str, query: str, messages: List[dict]) -> str:
        if not self.ollama_url:
            return "Mail search requires Ollama to be configured."
        if not messages:
            return "Sorry, your inbox is empty."

        limited = messages[-self.search_max_messages:]
        start_index = len(messages) - len(limited) + 1
        lines: List[str] = []
        for offset, message in enumerate(limited, start=start_index):
            iso_ts = message.get('timestamp', '')
            display_ts = self._format_mail_timestamp(iso_ts)
            sender = message.get('sender_short') or message.get('sender_id') or 'unknown'
            body = self._shorten_mail_body(message.get('body', ''))
            lines.append(f"{offset}. {display_ts} ({iso_ts}) {sender}: {body}")

        transcript = "\n".join(lines)
        prompt = textwrap.dedent(
            f"""
            You are assisting with searches over an offline mailbox archive.
            Each message is numbered and includes the display timestamp, ISO timestamp, sender, and body.
            Use the query to find relevant information and respond with a concise summary (no more than 80 words).
            If you mention specific messages, reference them using their numbers in square brackets (e.g. [12]).
            If nothing in the messages helps answer the query, respond with exactly: No matches found for '{query}'.

            Mailbox: {mailbox}
            Query: {query}
            Messages:
            {transcript}

            Answer:
            """
        ).strip()

        payload = {
            "model": self.search_model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": 0.2,
                "num_ctx": self.search_num_ctx,
            },
        }

        try:
            self.ai_log(f"Mail search '{query}'", "Ollama")
            response = requests.post(
                self.ollama_url,
                json=payload,
                timeout=(15, self.search_timeout),
            )
            if response.status_code != 200:
                detail = response.text[:120]
                self.clean_log(f"Mail search error {response.status_code}: {detail}")
                return self._format_error(f"status {response.status_code}. {detail}")
            data = response.json()
            result = data.get('response')
            if not result and isinstance(data.get('choices'), list) and data['choices']:
                choice = data['choices'][0]
                result = choice.get('text') or choice.get('content')
            if not result:
                return self._format_error("no response from search model")
            text = str(result).strip()
            self.clean_log(f"Mail search served from {mailbox}")
            return text or self._format_error("empty response")
        except Exception as exc:
            self.clean_log(f"Mail search exception: {exc}")
            return self._format_error(str(exc))

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
            }
            prompt = f"📬 Oops, mailbox '{mailbox}' doesn't exist yet. Launch it now? Reply Y or N"
            return PendingReply(prompt, "/m create")

        if not entry:
            return PendingReply(
                f"Mailbox '{mailbox}' already exists. Include a message after the mailbox name to send mail.",
                "/m command",
            )

        try:
            count, _, _ = self.store.append(mailbox, entry, allow_create=False)
        except KeyError:
            self.pending_creation[sender_key] = {
                "mailbox": mailbox,
                "entry": dict(entry),
                "sender_short": sender_short,
                "sender_id": sender_id,
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
        if lower in YES_RESPONSES:
            return self._finalize_mailbox_creation(sender_key, state)
        if lower in NO_RESPONSES or lower in CANCEL_RESPONSES:
            self.pending_creation.pop(sender_key, None)
            return PendingReply(f"👍 No problem, '{mailbox}' was not created.", "/m create")
        return PendingReply("❓ Please reply with Y or N to create the mailbox.", "/m create")

    def handle_check(self, sender_key: str, mailbox: str, remainder: str) -> PendingReply:
        if not mailbox:
            return PendingReply("Mailbox name cannot be empty.", "/c command")

        existed = self.store.mailbox_exists(mailbox)
        if not existed:
            fun_reply = random.choice(MISSING_MAILBOX_RESPONSES).format(mailbox=mailbox)
            return PendingReply(fun_reply, "/c command", chunk_delay=4.0)

        remainder = remainder.strip()
        if not remainder:
            return self._build_mailbox_result(mailbox, existed, is_search=False)

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

        return self._build_mailbox_result(mailbox, existed, is_search=True, query=query)

    # Internal helpers ------------------------------------------------
    def _build_mailbox_result(
        self,
        mailbox: str,
        existed: bool,
        *,
        is_search: bool = False,
        query: Optional[str] = None,
    ) -> PendingReply:
        if is_search:
            messages = self.store.get_all(mailbox)
            if not messages:
                replies = MISSING_MAILBOX_RESPONSES if not existed else EMPTY_MAILBOX_RESPONSES
                fun_reply = random.choice(replies).format(mailbox=mailbox)
                return PendingReply(fun_reply, "/c search", chunk_delay=4.0)
            summary = self._summarize_mail_search(mailbox, query or "", messages)
            self.clean_log(f"Mailbox search '{mailbox}' query '{(query or '').strip()}'", "🔎")
            return PendingReply(summary, "/c search", chunk_delay=4.0)

        messages = self.store.get_last(mailbox, 3)
        if not messages:
            replies = MISSING_MAILBOX_RESPONSES if not existed else EMPTY_MAILBOX_RESPONSES
            fun_reply = random.choice(replies).format(mailbox=mailbox)
            return PendingReply(fun_reply, "/c command", chunk_delay=4.0)
        ordered = list(reversed(messages))
        lines = [
            self._format_mail_line(idx, msg)
            for idx, msg in enumerate(ordered, start=1)
        ]
        mailbox_label = ordered[0].get("mailbox") or mailbox
        header = f"📥 Inbox '{mailbox_label}' (newest)"
        response_text = "\n".join([header] + lines)
        self.clean_log(f"Mailbox '{mailbox}' checked")
        return PendingReply(response_text, "/c command", chunk_delay=4.0)

    def _finalize_mailbox_creation(self, sender_key: str, state: Dict[str, Any]) -> PendingReply:
        mailbox = state.get("mailbox")
        raw_entry = state.get("entry")
        entry = dict(raw_entry) if raw_entry else None
        if not mailbox:
            self.pending_creation.pop(sender_key, None)
            return PendingReply("Mailbox setup information expired. Please start again with /m.", "/m command")

        sender_short = (entry or {}).get("sender_short") or state.get("sender_short") or mailbox

        try:
            if entry:
                entry.setdefault("mailbox", mailbox)
                count, created, _ = self.store.append(mailbox, entry, allow_create=True)
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

        self.pending_creation.pop(sender_key, None)

        suffix = "" if count == 1 else "s"
        lines = [f"🎉 Mailbox '{mailbox}' ready."]
        if entry:
            lines.append(f"✉️ Message saved—now {count} message{suffix} inside.")
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
            return PendingReply(f"🧹 Mailbox '{mailbox}' is now empty.", "/wipe command")
        fun_reply = random.choice(MISSING_MAILBOX_RESPONSES).format(mailbox=mailbox)
        return PendingReply(fun_reply, "/wipe command")
