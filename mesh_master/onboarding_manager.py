"""Onboarding workflow manager for Mesh Master users."""

from __future__ import annotations

import json
import os
import re
import threading
import time
from datetime import datetime, timezone
from collections import deque
from typing import Any, Callable, Deque, Dict, List, Optional, Set

from .replies import PendingReply


LanguageSetter = Callable[[str, str], bool]
LanguageGetter = Callable[[str], Optional[str]]
LanguageNormalizer = Callable[[Optional[str]], str]
LogFn = Callable[..., None]

YES_RESPONSES = {"y", "yes", "yeah", "yep", "affirmative", "si", "sí"}
NO_RESPONSES = {"n", "no", "nope", "skip", "later"}
CANCEL_RESPONSES = {"cancel", "stop", "abort", "salir"}

MAILBOX_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]{1,23}$")


def _write_json_atomic(path: str, payload: Dict[str, Any]) -> None:
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, sort_keys=True)
    os.replace(tmp_path, path)


class OnboardingManager:
    """Coordinate onboarding prompts, state persistence, and reminders."""

    def __init__(
        self,
        *,
        state_path: str,
        clean_log: LogFn,
        mail_manager: Any,
        set_user_language: LanguageSetter,
        get_user_language: LanguageGetter,
        normalize_language: LanguageNormalizer,
        stats: Optional[Any] = None,
    ) -> None:
        self.state_path = state_path or "data/onboarding_state.json"
        self.clean_log = clean_log
        self.mail_manager = mail_manager
        self.set_user_language = set_user_language
        self.get_user_language = get_user_language
        self.normalize_language = normalize_language
        self.stats = stats

        self._lock = threading.Lock()
        self._sessions: Dict[str, Dict[str, Any]] = self._load_state()
        self._events_lock = threading.Lock()
        self._events: Deque[Dict[str, Any]] = deque()

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------

    def is_session_active(self, sender_key: Optional[str]) -> bool:
        if not sender_key:
            return False
        with self._lock:
            session = self._sessions.get(sender_key)
            if not session:
                return False
            return session.get("status") == "active" and session.get("stage") != "complete"

    def start_session(
        self,
        *,
        sender_key: str,
        sender_id: Any,
        sender_short: str,
        language_hint: Optional[str],
        restart: bool = False,
    ) -> PendingReply:
        if not sender_key:
            return PendingReply("⚠️ I couldn't identify your DM session. Try again in a moment.", "onboard start")

        now = time.time()
        with self._lock:
            session = self._sessions.get(sender_key)

            if restart or not session:
                session = self._create_session_locked(sender_key, sender_id, language_hint, sender_short)
            elif session.get("status") == "complete" and not restart:
                lang = self._session_language(session)
                overview = self._command_overview(lang)
                text = self._message("already_complete", lang) + "\n\n" + overview
                return PendingReply(text, "onboard start")
            elif session.get("status") == "complete" and restart:
                session = self._create_session_locked(sender_key, sender_id, language_hint, sender_short)
            else:
                session['updated_at'] = now
                session['node_id'] = sender_id or session.get('node_id')
                session['sender_label'] = sender_short or session.get('sender_label')
                session.pop('reminder_stage', None)
                self._save_state_locked()

            snapshot = dict(session)
            snapshot.setdefault('sender_label', sender_short)

        return self._prompt_for_stage(snapshot)

    def handle_incoming(
        self,
        *,
        sender_key: str,
        sender_id: Any,
        sender_short: str,
        message: str,
    ) -> Optional[PendingReply]:
        if not sender_key:
            return None
        text = (message or "").strip()
        if not text:
            return None

        with self._lock:
            session = self._sessions.get(sender_key)
            if not session or session.get('status') != 'active':
                return None
            stage = session.get('stage', 'language')

        if stage == 'language':
            return self._handle_language(sender_key, sender_id, sender_short, text)
        if stage == 'mail_offer':
            return self._handle_mail_offer(sender_key, sender_id, sender_short, text)
        if stage == 'mail_name':
            return self._handle_mail_name(sender_key, sender_id, sender_short, text)
        if stage == 'mail_message':
            return self._handle_mail_message(sender_key, sender_id, sender_short, text)
        if stage == 'mail_wizard':
            return self._handle_mail_wizard(sender_key, sender_id, sender_short, text)
        return None

    def handle_heartbeat(self, sender_key: Optional[str], node_id: Any) -> None:
        if not sender_key:
            return
        with self._lock:
            session = self._sessions.get(sender_key)
            if not session or session.get('status') != 'active':
                return
            stage = session.get('stage')
            if not stage or stage == 'complete':
                return
            current_stage = stage
            last_stage = session.get('reminder_stage')
            if last_stage == current_stage:
                return
            lang = self._session_language(session)
            reminder_text = self._message('reminder', lang, stage_label=self._stage_label(current_stage, lang))
            if not reminder_text:
                return
            target_node = node_id or session.get('node_id')
            if target_node is None:
                session['node_id'] = node_id
                self._save_state_locked()
                return
            self._queue_dm(target_node, reminder_text)
            session['reminder_stage'] = current_stage
            session['reminder_sent_at'] = time.time()
            session['node_id'] = target_node
            self._save_state_locked()

    def flush_notifications(self, interface: Any, send_fn: Callable[[Any, str, Any], None]) -> None:
        if interface is None or send_fn is None:
            return
        buffer: Deque[Dict[str, Any]] = deque()
        with self._events_lock:
            while self._events:
                buffer.append(self._events.popleft())
        if not buffer:
            return
        sent_any = False
        while buffer:
            event = buffer.popleft()
            node_id = event.get('node_id')
            text = event.get('text')
            if node_id is None or not text:
                continue
            try:
                send_fn(interface, text, node_id)
                sent_any = True
            except Exception as exc:
                self.clean_log(f"Onboarding reminder send failed: {exc}", "⚠️")
        if sent_any:
            self.clean_log("Onboarding reminders flushed", "🧭", show_always=False)

    # ------------------------------------------------------------------
    # Stage handlers
    # ------------------------------------------------------------------

    def _handle_language(
        self,
        sender_key: str,
        sender_id: Any,
        sender_short: str,
        text: str,
    ) -> Optional[PendingReply]:
        choice = self._interpret_language_choice(text)
        if not choice:
            prompt = self._message('language_retry', 'en')
            return PendingReply(prompt, "onboard language")

        self.set_user_language(sender_key, choice)
        now = time.time()
        with self._lock:
            session = self._sessions.get(sender_key)
            if not session:
                return None
            session['language'] = choice
            session['stage'] = 'mail_offer'
            session['status'] = 'active'
            session['updated_at'] = now
            session['node_id'] = sender_id or session.get('node_id')
            session['sender_label'] = sender_short or session.get('sender_label')
            session.pop('reminder_stage', None)
            self._save_state_locked()

        ack = self._message('language_ack', choice, name=sender_short)
        offer = self._message('mail_offer', choice)
        combined = f"{ack}\n\n{offer}" if ack else offer
        return PendingReply(combined, "onboard language")

    def _handle_mail_offer(
        self,
        sender_key: str,
        sender_id: Any,
        sender_short: str,
        text: str,
    ) -> Optional[PendingReply]:
        normalized = text.strip().lower()
        lang = self._session_language_key(sender_key)
        if normalized in CANCEL_RESPONSES:
            return self._complete_session(sender_key, lang, cancelled=True)
        if normalized in YES_RESPONSES:
            with self._lock:
                session = self._sessions.get(sender_key)
                if not session:
                    return None
                session['stage'] = 'mail_name'
                session['updated_at'] = time.time()
                session['node_id'] = sender_id or session.get('node_id')
                session['sender_label'] = sender_short or session.get('sender_label')
                session.pop('reminder_stage', None)
                self._save_state_locked()
            prompt = self._message('mail_name', lang)
            return PendingReply(prompt, "onboard mail")
        if normalized in NO_RESPONSES:
            skip_note = self._message('mail_skip', lang)
            return self._complete_session(sender_key, lang, extra_text=skip_note)

        retry = self._message('mail_offer_retry', lang)
        return PendingReply(retry, "onboard mail")

    def _handle_mail_name(
        self,
        sender_key: str,
        sender_id: Any,
        sender_short: str,
        text: str,
    ) -> Optional[PendingReply]:
        normalized = text.strip().lower()
        lang = self._session_language_key(sender_key)
        if normalized in CANCEL_RESPONSES:
            return self._complete_session(sender_key, lang, cancelled=True)
        if normalized in NO_RESPONSES:
            skip_note = self._message('mail_skip', lang)
            return self._complete_session(sender_key, lang, extra_text=skip_note)

        mailbox = self._normalize_mailbox_name(normalized)
        if not mailbox:
            return PendingReply(self._message('mail_name_retry', lang), "onboard mail")

        with self._lock:
            session = self._sessions.get(sender_key)
            if not session:
                return None
            session['mailbox'] = mailbox
            session['stage'] = 'mail_message'
            session['updated_at'] = time.time()
            session['node_id'] = sender_id or session.get('node_id')
            session['sender_label'] = sender_short or session.get('sender_label')
            session.pop('reminder_stage', None)
            self._save_state_locked()

        prompt = self._message('mail_message', lang, mailbox=mailbox)
        return PendingReply(prompt, "onboard mail")

    def _handle_mail_message(
        self,
        sender_key: str,
        sender_id: Any,
        sender_short: str,
        text: str,
    ) -> Optional[PendingReply]:
        lang = self._session_language_key(sender_key)
        normalized = text.strip().lower()
        if normalized in CANCEL_RESPONSES or normalized in NO_RESPONSES:
            skip_note = self._message('mail_skip', lang)
            return self._complete_session(sender_key, lang, extra_text=skip_note)

        mailbox = self._session_mailbox(sender_key)
        if not mailbox:
            return PendingReply(self._message('mail_name_retry', lang), "onboard mail")

        mail_reply = self.mail_manager.handle_send(
            sender_key=sender_key,
            sender_id=sender_id,
            mailbox=mailbox,
            body=text,
            sender_short=sender_short,
        ) if self.mail_manager else None

        has_pending = self.mail_manager.has_pending_creation(sender_key) if self.mail_manager else False

        if has_pending and mail_reply:
            with self._lock:
                session = self._sessions.get(sender_key)
            if session:
                session['stage'] = 'mail_wizard'
                session['updated_at'] = time.time()
                session['sender_label'] = sender_short or session.get('sender_label')
                session.pop('reminder_stage', None)
                self._save_state_locked()
            hint = self._message('mail_wizard_hint', lang, mailbox=mailbox)
            return self._merge_reply(mail_reply, hint)

        if mail_reply:
            with self._lock:
                session = self._sessions.get(sender_key)
                if session:
                    session['stage'] = 'complete'
                    session['status'] = 'complete'
                    session['updated_at'] = time.time()
                    session['completed_at'] = time.time()
                    session.pop('reminder_stage', None)
                    self._save_state_locked()
            overview = self._command_overview(lang)
            return self._merge_reply(mail_reply, overview)

        return PendingReply(self._message('mail_error', lang), "onboard mail")

    def _handle_mail_wizard(
        self,
        sender_key: str,
        sender_id: Any,
        sender_short: str,
        text: str,
    ) -> Optional[PendingReply]:
        lang = self._session_language_key(sender_key)
        if not self.mail_manager:
            return self._complete_session(sender_key, lang)

        mail_reply = self.mail_manager.handle_creation_response(sender_key, text)
        has_pending = self.mail_manager.has_pending_creation(sender_key)

        if has_pending:
            hint = self._message('mail_wizard_hint', lang, mailbox=self._session_mailbox(sender_key))
            return self._merge_reply(mail_reply, hint)

        with self._lock:
            session = self._sessions.get(sender_key)
            if session:
                session['stage'] = 'complete'
                session['status'] = 'complete'
                session['updated_at'] = time.time()
                session['completed_at'] = time.time()
                session['sender_label'] = sender_short or session.get('sender_label')
                session.pop('reminder_stage', None)
                self._save_state_locked()

        overview = self._command_overview(lang)
        return self._merge_reply(mail_reply, overview)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_state(self) -> Dict[str, Dict[str, Any]]:
        try:
            with open(self.state_path, "r", encoding="utf-8") as fh:
                raw = json.load(fh)
        except FileNotFoundError:
            return {}
        except Exception as exc:
            self.clean_log(f"Onboarding state load failed: {exc}", "⚠️")
            return {}

        sessions_raw = raw.get('sessions') if isinstance(raw, dict) else raw
        sessions: Dict[str, Dict[str, Any]] = {}
        if isinstance(sessions_raw, dict):
            for key, value in sessions_raw.items():
                if isinstance(key, str) and isinstance(value, dict):
                    sessions[key] = dict(value)
        return sessions

    def _save_state_locked(self) -> None:
        try:
            _write_json_atomic(self.state_path, {"sessions": self._sessions})
        except Exception as exc:
            self.clean_log(f"Onboarding state save failed: {exc}", "⚠️")

    def _create_session_locked(
        self,
        sender_key: str,
        sender_id: Any,
        language_hint: Optional[str],
        sender_label: Optional[str] = None,
    ) -> Dict[str, Any]:
        now = time.time()
        normalized_hint = None
        if language_hint:
            normalized_hint = self.normalize_language(language_hint)
        session = {
            'sender_key': sender_key,
            'stage': 'language',
            'status': 'active',
            'language': None,
            'mailbox': None,
            'started_at': now,
            'updated_at': now,
            'node_id': sender_id,
            'sender_label': sender_label,
        }
        if normalized_hint in {'en', 'es'}:
            session['language_hint'] = normalized_hint
        self._sessions[sender_key] = session
        self._save_state_locked()
        return session

    def _prompt_for_stage(self, session: Dict[str, Any]) -> PendingReply:
        stage = session.get('stage', 'language')
        lang = self._session_language(session)
        if stage == 'language':
            return PendingReply(self._message('language_prompt', 'en'), "onboard language")
        if stage == 'mail_offer':
            text = self._message('mail_offer', lang)
            return PendingReply(text, "onboard mail")
        if stage == 'mail_name':
            return PendingReply(self._message('mail_name', lang), "onboard mail")
        if stage == 'mail_message':
            mailbox = session.get('mailbox') or 'your mailbox'
            return PendingReply(self._message('mail_message', lang, mailbox=mailbox), "onboard mail")
        if stage == 'mail_wizard':
            pending = None
            if self.mail_manager:
                pending = self.mail_manager.pending_creation.get(session.get('sender_key'))
            if pending:
                if pending.get('stage') == 'set_pin':
                    return PendingReply(
                        self._message('mail_wizard_pin', lang, mailbox=session.get('mailbox') or 'mailbox'),
                        "onboard mail",
                    )
                return PendingReply(
                    self._message('mail_wizard_confirm', lang, mailbox=session.get('mailbox') or 'mailbox'),
                    "onboard mail",
                )
            key = session.get('sender_key')
            if key:
                return self._complete_session(key, lang)
            overview = self._command_overview(lang)
            return PendingReply(overview, "onboard mail")
        return PendingReply(self._command_overview(lang), "onboard mail")

    def _complete_session(
        self,
        sender_key: str,
        lang: str,
        *,
        extra_text: Optional[str] = None,
        cancelled: bool = False,
    ) -> PendingReply:
        with self._lock:
            session = self._sessions.get(sender_key)
            if not session:
                session = self._create_session_locked(sender_key, None, None)
            session['stage'] = 'complete'
            session['status'] = 'complete'
            session['updated_at'] = time.time()
            session['completed_at'] = time.time()
            session.pop('reminder_stage', None)
            self._save_state_locked()

        if self.stats is not None and sender_key:
            try:
                self.stats.record_new_onboard(sender_key)
            except Exception:
                pass

        lines = []
        if cancelled:
            lines.append(self._message('cancelled', lang))
        if extra_text:
            lines.append(extra_text)
        lines.append(self._command_overview(lang))
        text = "\n\n".join(line for line in lines if line)
        return PendingReply(text, "onboard complete")

    def _merge_reply(self, reply: PendingReply, extra_text: Optional[str]) -> PendingReply:
        if not extra_text:
            return reply
        combined_text = f"{reply.text}\n\n{extra_text}" if reply.text else extra_text
        return PendingReply(
            combined_text,
            reply.reason,
            chunk_delay=reply.chunk_delay,
            pre_send_delay=reply.pre_send_delay,
            follow_up_text=reply.follow_up_text,
            follow_up_delay=reply.follow_up_delay,
        )

    def _queue_dm(self, node_id: Any, text: str) -> None:
        if not text or node_id is None:
            return
        with self._events_lock:
            self._events.append({'node_id': node_id, 'text': text})

    def get_sender_label(self, sender_key: Optional[str]) -> Optional[str]:
        if not sender_key:
            return None
        with self._lock:
            session = self._sessions.get(sender_key)
            if not session:
                return None
            label = session.get('sender_label') or session.get('sender_short')
            if label:
                return str(label)
        return None

    def list_completed(self, *, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        with self._lock:
            rows: List[Dict[str, Any]] = []
            for key, session in self._sessions.items():
                if not isinstance(session, dict):
                    continue
                if session.get('status') != 'complete':
                    continue
                completed_at = session.get('completed_at') or session.get('updated_at') or session.get('started_at')
                label = session.get('sender_label') or session.get('sender_short')
                rows.append({
                    'sender_key': key,
                    'label': str(label) if label else None,
                    'completed_at': completed_at,
                    'language': session.get('language'),
                    'mailbox': session.get('mailbox'),
                })
        rows.sort(key=lambda item: item.get('completed_at') or 0.0, reverse=True)
        if limit is not None and limit > 0:
            rows = rows[:limit]
        for item in rows:
            ts = item.get('completed_at')
            if isinstance(ts, (int, float)):
                item['completed_at_iso'] = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
            else:
                item['completed_at_iso'] = None
        return rows

    def _interpret_language_choice(self, value: str) -> Optional[str]:
        cleaned = value.strip().lower()
        if cleaned in {"1", "english", "en"}:
            return 'en'
        if cleaned in {"2", "es", "spanish", "espanol", "español"}:
            return 'es'
        return None

    def _normalize_mailbox_name(self, value: str) -> Optional[str]:
        cleaned = value.strip().lower()
        cleaned = cleaned.replace(' ', '-')
        if MAILBOX_PATTERN.fullmatch(cleaned):
            return cleaned
        return None

    def _session_language(self, session: Dict[str, Any]) -> str:
        lang = session.get('language') or session.get('language_hint')
        if lang in {'en', 'es'}:
            return lang
        return 'en'

    def _session_language_key(self, sender_key: str) -> str:
        with self._lock:
            session = self._sessions.get(sender_key)
            if session:
                return self._session_language(session)
        pref = self.get_user_language(sender_key)
        if pref in {'en', 'es'}:
            return pref
        return 'en'

    def _session_mailbox(self, sender_key: str) -> Optional[str]:
        with self._lock:
            session = self._sessions.get(sender_key)
            if session:
                return session.get('mailbox')
        return None

    def _stage_label(self, stage: str, lang: str) -> str:
        labels = {
            'language': {'en': 'choosing a language', 'es': 'elegir un idioma'},
            'mail_offer': {'en': 'deciding on Mesh Mail', 'es': 'decidir sobre Mesh Mail'},
            'mail_name': {'en': 'picking a mailbox name', 'es': 'elegir un buzón'},
            'mail_message': {'en': 'writing the first mailbox note', 'es': 'enviar la primera nota'},
            'mail_wizard': {'en': 'finishing the mailbox wizard', 'es': 'terminar el asistente del buzón'},
        }
        return labels.get(stage, {}).get(lang, labels.get(stage, {}).get('en', 'setup'))

    def _command_overview(self, lang: str) -> str:
        if lang == 'es':
            lines = [
                "📡 Comandos rápidos:",
                "• `/ai <mensaje>` — pregunta al asistente.",
                "• `/mail <buzon> <nota>` — crea o envía Mesh Mail.",
                "• `/c <buzon>` — revisa o busca mensajes (ej: `/c campamento planes`).",
                "• `/games` — juegos como hangman, wordle y aventura.",
                "• `/help` — lista completa cuando la necesites.",
                "Puedes reiniciar el onboarding con `/onboard restart`.",
            ]
        else:
            lines = [
                "📡 Quick command highlights:",
                "• `/ai <message>` — ask the assistant anything.",
                "• `/mail <box> <note>` — create or send Mesh Mail.",
                "• `/c <box>` — check or search mail (e.g. `/c team status`).",
                "• `/games` — try hangman, wordle, adventure, and more.",
                "• `/help` — full command list anytime.",
                "Run `/onboard restart` if you ever want this tour again.",
            ]
        return "\n".join(lines)

    def _message(self, key: str, lang: str, **kwargs: Any) -> str:
        table = {
            'language_prompt': {
                'en': "👋 Let's get you set up. Choose a language:\n1) English\n2) Español\nReply with 1 or 2.",
            },
            'language_retry': {
                'en': "Please reply with 1 for English or 2 for Español to continue onboarding.",
                'es': "Responde 1 para English o 2 para Español para continuar.",
            },
            'language_ack': {
                'en': "Great, {name}! I'll stick to English from here.",
                'es': "Perfecto, {name}! Seguiré en español.",
            },
            'mail_offer': {
                'en': "Want to set up a Mesh Mail inbox now so longer notes have a home? Reply YES to walk through it or SKIP to continue.",
                'es': "¿Quieres crear un buzón Mesh Mail ahora? Responde YES para configurarlo o SKIP para seguir.",
            },
            'mail_offer_retry': {
                'en': "Reply YES to start Mesh Mail setup or SKIP to move on.",
                'es': "Responde YES para crear el buzón o SKIP para continuar.",
            },
            'mail_name': {
                'en': "What mailbox name should we use? Letters/numbers only (e.g. camp). Reply SKIP to do this later.",
                'es': "¿Cómo llamamos al buzón? Solo letras/números (ej. camp). Responde SKIP para hacerlo después.",
            },
            'mail_name_retry': {
                'en': "Try a mailbox name using letters, numbers, dashes, or underscores (3-24 chars).",
                'es': "Usa letras, números, guiones o guiones bajos (3-24 caracteres).",
            },
            'mail_message': {
                'en': "Nice. Send the first note for '{mailbox}' (even a quick hi) so I can create it. Reply SKIP to handle it later.",
                'es': "Perfecto. Envía la primera nota para '{mailbox}' (aunque sea un hola) para crearlo. Responde SKIP si prefieres después.",
            },
            'mail_skip': {
                'en': "No problem—we can set up Mesh Mail anytime with `/mail <box> <note>`.",
                'es': "Sin problema. Puedes crear Mesh Mail luego con `/mail <buzon> <nota>`.",
            },
            'mail_wizard_hint': {
                'en': "Almost there—answer the Mesh Mail prompt to finish setting up '{mailbox}'.",
                'es': "Ya casi. Responde la pregunta del asistente Mesh Mail para terminar '{mailbox}'.",
            },
            'mail_wizard_confirm': {
                'en': "Reply Y to create the mailbox now or N to cancel.",
                'es': "Responde Y para crear el buzón ahora o N para cancelar.",
            },
            'mail_wizard_pin': {
                'en': "Pick a 4-8 digit PIN for '{mailbox}' or reply SKIP to leave it open.",
                'es': "Elige un PIN de 4-8 dígitos para '{mailbox}' o responde SKIP para dejarlo abierto.",
            },
            'mail_error': {
                'en': "Something went wrong saving that mail. Try `/mail <box> <note>` later to finish setup.",
                'es': "Algo falló guardando ese mensaje. Prueba `/mail <buzon> <nota>` más tarde.",
            },
            'cancelled': {
                'en': "Onboarding paused. Holler with `/onboard` when you want to finish.",
                'es': "Onboarding en pausa. Usa `/onboard` cuando quieras continuar.",
            },
            'already_complete': {
                'en': "You're already fully set up! Here's a quick refresher.",
                'es': "¡Ya estás listo! Aquí un repaso rápido.",
            },
            'reminder': {
                'en': "👋 I noticed you didn't finish onboarding ({stage_label}). Reply `/onboard` to pick up where we left off.",
                'es': "👋 Parece que no terminaste el onboarding ({stage_label}). Escribe `/onboard` para continuar donde lo dejamos.",
            },
        }
        lang_map = table.get(key, {})
        template = lang_map.get(lang) or lang_map.get('en') or ''
        try:
            return template.format(**kwargs)
        except Exception:
            return template
