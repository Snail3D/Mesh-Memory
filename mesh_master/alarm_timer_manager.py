from __future__ import annotations

import json
import os
import re
import threading
import time
import uuid
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple


def _now_ts() -> float:
    return time.time()


def _write_atomic(path: str, data: str) -> None:
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(data)
    os.replace(tmp, path)


def _safe_load_json(path: str, default: Any) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return default


def _fmt_hms(seconds: int) -> str:
    seconds = max(0, int(seconds))
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


WEEKDAYS = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}


class AlarmTimerManager:
    def __init__(
        self,
        *,
        storage_path: str,
        clean_log: Callable[[str, str, bool, bool], None],
        send_direct_fn: Callable[[Any, str, Any, Optional[float]], Any],
        interface_ref: Optional[Any] = None,
    ) -> None:
        self.storage_path = storage_path
        self.clean_log = clean_log
        self._send_direct = send_direct_fn
        self._interface = interface_ref
        self._lock = threading.Lock()
        self._paused_users: Dict[str, float] = {}
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        # State shape: {"alarms": {user_key: [..]}, "timers": {user_key: [..]}, "stopwatches": {user_key: {..}}}
        self.state: Dict[str, Any] = {
            "alarms": {},
            "timers": {},
            "stopwatches": {},
        }
        self._load()

    # Persistence ------------------------------------------------------
    def _load(self) -> None:
        data = _safe_load_json(self.storage_path, {})
        if not isinstance(data, dict):
            data = {}
        with self._lock:
            self.state["alarms"] = data.get("alarms") or {}
            self.state["timers"] = data.get("timers") or {}
            self.state["stopwatches"] = data.get("stopwatches") or {}

    def _save(self) -> None:
        with self._lock:
            payload = json.dumps(self.state, indent=2, ensure_ascii=False, sort_keys=True)
        _write_atomic(self.storage_path, payload)

    def set_interface(self, interface: Any) -> None:
        self._interface = interface

    # Scheduling helpers -----------------------------------------------
    def _send_dm(self, node_id: Any, text: str) -> None:
        if not text:
            return
        try:
            interface = self._interface
            if interface is None:
                return
            self._send_direct(interface, text, node_id, None)
        except Exception:
            pass

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._worker, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        t = self._thread
        if t and t.is_alive():
            try:
                t.join(timeout=1.0)
            except Exception:
                pass

    # Parsing ----------------------------------------------------------
    def _parse_time_of_day(self, token: str) -> Optional[Tuple[int, int]]:
        text = (token or "").strip().lower().replace(" ", "")
        if not text:
            return None
        dm = re.match(r"^(\d{3,4})(am|pm)?$", text)
        if dm:
            digits = dm.group(1)
            ap = dm.group(2)
            if len(digits) == 3:
                hh = int(digits[0])
                mm = int(digits[1:])
            else:
                hh = int(digits[:2])
                mm = int(digits[2:])
            if ap:
                if hh == 12:
                    hh = 0
                if ap == "pm":
                    hh += 12
            if 0 <= hh < 24 and 0 <= mm < 60:
                return hh, mm
        m = re.match(r"^(\d{1,2}):?(\d{2})(am|pm)?$", text)
        if m:
            hh = int(m.group(1))
            mm = int(m.group(2))
            ap = m.group(3)
            if ap:
                if hh == 12:
                    hh = 0
                if ap == "pm":
                    hh += 12
            if 0 <= hh < 24 and 0 <= mm < 60:
                return hh, mm
        m2 = re.match(r"^(\d{1,2})(am|pm)$", text)
        if m2:
            hh = int(m2.group(1))
            ap = m2.group(2)
            if ap:
                if hh == 12:
                    hh = 0
                if ap == "pm":
                    hh += 12
            return hh, 0
        return None

    def _parse_date_hint(self, token: str, now: datetime) -> Optional[datetime]:
        text = (token or "").strip().lower()
        if not text:
            return None
        if text in ("today",):
            return now
        if text in ("tomorrow",):
            return now + timedelta(days=1)
        if text in WEEKDAYS:
            target = WEEKDAYS[text]
            today = now.weekday()
            delta = (target - today) % 7
            delta = 7 if delta == 0 else delta
            return now + timedelta(days=delta)
        m = re.match(r"^(\d{1,2})/(\d{1,2})(?:/(\d{2,4}))?$", text)
        if m:
            month = int(m.group(1))
            day = int(m.group(2))
            year = now.year
            if m.group(3):
                y = int(m.group(3))
                if y < 100:
                    year = 2000 + y
                else:
                    year = y
            try:
                return now.replace(year=year, month=month, day=day)
            except Exception:
                return None
        m2 = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})$", text)
        if m2:
            try:
                return now.replace(year=int(m2.group(1)), month=int(m2.group(2)), day=int(m2.group(3)))
            except Exception:
                return None
        return None

    def _parse_duration(self, token: str) -> Optional[int]:
        text = (token or "").strip().lower()
        if not text:
            return None
        m = re.match(r"^(\d+):(\d{2})(?::(\d{2}))?$", text)
        if m:
            h = int(m.group(1) or 0)
            m2 = int(m.group(2) or 0)
            s = int(m.group(3) or 0)
            return h * 3600 + m2 * 60 + s
        total = 0
        found = False
        for val, unit in re.findall(r"(\d+)\s*([hms])", text):
            n = int(val)
            if unit == "h":
                total += n * 3600
            elif unit == "m":
                total += n * 60
            elif unit == "s":
                total += n
            found = True
        if found:
            return total
        if text.isdigit():
            return int(text)
        return None

    # Public API -------------------------------------------------------
    def pause_for_user(self, sender_key: str) -> None:
        if not sender_key:
            return
        with self._lock:
            self._paused_users[sender_key] = _now_ts()
            timers = list(self.state.get("timers", {}).get(sender_key, []))
            now = _now_ts()
            for t in timers:
                if not t.get("paused") and t.get("due_ts"):
                    remaining = max(0, int(t["due_ts"] - now))
                    t["paused"] = True
                    t["remaining"] = remaining
            sw = self.state.get("stopwatches", {}).get(sender_key)
            if isinstance(sw, dict) and sw.get("running") and not sw.get("paused"):
                sw["paused"] = True
                sw["paused_at"] = now
        self._save()

    def resume_for_user(self, sender_key: str) -> None:
        if not sender_key:
            return
        resume_time = _now_ts()
        with self._lock:
            self._paused_users.pop(sender_key, None)
            timers = list(self.state.get("timers", {}).get(sender_key, []))
            for t in timers:
                if t.get("paused"):
                    remaining = int(t.get("remaining") or 0)
                    t["due_ts"] = resume_time + max(0, remaining)
                    t["paused"] = False
                    t.pop("remaining", None)
            sw = self.state.get("stopwatches", {}).get(sender_key)
            if isinstance(sw, dict) and sw.get("paused"):
                paused_at = float(sw.get("paused_at") or resume_time)
                delta = resume_time - paused_at
                sw["paused"] = False
                if sw.get("running") and sw.get("start_ts"):
                    sw["start_ts"] = float(sw["start_ts"]) + max(0.0, float(delta))
        self._save()

    def add_alarm(self, sender_key: str, node_id: Any, text: str) -> Tuple[bool, str]:
        parts = [p for p in (text or "").strip().split() if p]
        if not parts:
            return False, "Usage: /alarm <time> [date|daily] [label]"
        now_local = datetime.now().astimezone()
        hhmm = self._parse_time_of_day(parts[0])
        if not hhmm:
            return False, "Time format not recognized. Examples: 1434, 2:34pm, 14:34"
        hour, minute = hhmm
        date_hint: Optional[datetime] = None
        repeat = "once"
        label_tokens: List[str] = []
        for token in parts[1:]:
            if token.lower() == "daily":
                repeat = "daily"
                continue
            guess = self._parse_date_hint(token, now_local)
            if guess is not None:
                date_hint = guess
            else:
                label_tokens.append(token)
        target = now_local
        if date_hint is not None:
            target = date_hint
        target = target.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if target <= now_local and repeat == "once" and date_hint is None:
            target = target + timedelta(days=1)
        alarm = {
            "id": uuid.uuid4().hex[:6],
            "node_id": node_id,
            "next_ts": target.timestamp(),
            "repeat": repeat,
            "label": " ".join(label_tokens)[:64] if label_tokens else "",
            "created_ts": _now_ts(),
        }
        with self._lock:
            self.state.setdefault("alarms", {}).setdefault(sender_key, []).append(alarm)
        self._save()
        when = datetime.fromtimestamp(alarm["next_ts"]).astimezone().strftime("%a %H:%M")
        return True, f"⏰ Alarm set for {when}. ID: {alarm['id']}"

    def list_alarms(self, sender_key: str) -> str:
        with self._lock:
            items = list(self.state.get("alarms", {}).get(sender_key, []))
        if not items:
            return "No alarms set. Use /alarm <time> [date|daily]"
        lines = ["Your alarms:"]
        for a in items:
            when = datetime.fromtimestamp(float(a.get("next_ts"))).astimezone().strftime("%a %H:%M")
            rep = a.get("repeat") or "once"
            label = f" — {a.get('label')}" if a.get("label") else ""
            lines.append(f"• {a.get('id')}: {when} ({rep}){label}")
        return "\n".join(lines)

    def delete_alarm(self, sender_key: str, alarm_id: str) -> str:
        with self._lock:
            items = list(self.state.get("alarms", {}).get(sender_key, []))
            new_items = [a for a in items if str(a.get("id")) != str(alarm_id)]
            removed = len(items) - len(new_items)
            if removed:
                self.state["alarms"][sender_key] = new_items
                self._save()
        if removed:
            return f"Deleted {removed} alarm(s)."
        return "No matching alarm found."

    def clear_alarms(self, sender_key: str) -> str:
        with self._lock:
            count = len(self.state.get("alarms", {}).get(sender_key, []))
            self.state.get("alarms", {}).pop(sender_key, None)
            if count:
                self._save()
        return f"Cleared {count} alarm(s)."

    def add_timer(self, sender_key: str, node_id: Any, text: str) -> Tuple[bool, str]:
        parts = [p for p in (text or "").strip().split() if p]
        if not parts:
            return False, "Usage: /timer <duration> [label]"
        secs = self._parse_duration(parts[0])
        if not secs or secs <= 0:
            return False, "Duration not recognized. Examples: 10m, 1h30m, 90s, 1:30:00"
        label = " ".join(parts[1:])[:64] if len(parts) > 1 else ""
        now = _now_ts()
        timer = {
            "id": uuid.uuid4().hex[:6],
            "node_id": node_id,
            "due_ts": now + int(secs),
            "original": int(secs),
            "label": label,
            "paused": False,
        }
        with self._lock:
            self.state.setdefault("timers", {}).setdefault(sender_key, []).append(timer)
        self._save()
        return True, f"⏲️ Timer set for {_fmt_hms(int(secs))}. ID: {timer['id']}"

    def list_timers(self, sender_key: str) -> str:
        now = _now_ts()
        with self._lock:
            items = list(self.state.get("timers", {}).get(sender_key, []))
        if not items:
            return "No active timers. Use /timer <duration>"
        lines = ["Your timers:"]
        for t in items:
            if t.get("paused"):
                remain = int(t.get("remaining") or 0)
            else:
                remain = max(0, int(float(t.get("due_ts", now)) - now))
            label = f" — {t.get('label')}" if t.get("label") else ""
            lines.append(f"• {t.get('id')}: {_fmt_hms(remain)}{label}")
        return "\n".join(lines)

    def cancel_timer(self, sender_key: str, timer_id: str) -> str:
        with self._lock:
            items = list(self.state.get("timers", {}).get(sender_key, []))
            new_items = [a for a in items if str(a.get("id")) != str(timer_id)]
            removed = len(items) - len(new_items)
            if removed:
                self.state["timers"][sender_key] = new_items
                self._save()
        if removed:
            return f"Cancelled {removed} timer(s)."
        return "No matching timer found."

    def clear_timers(self, sender_key: str) -> str:
        with self._lock:
            count = len(self.state.get("timers", {}).get(sender_key, []))
            self.state.get("timers", {}).pop(sender_key, None)
            if count:
                self._save()
        return f"Cleared {count} timer(s)."

    def stopwatch_status(self, sender_key: str) -> str:
        with self._lock:
            sw = self.state.get("stopwatches", {}).get(sender_key)
        if not isinstance(sw, dict) or not sw.get("running"):
            return "No active stopwatch. Use '/stopwatch start' or '/start stopwatch'."
        if sw.get("paused"):
            elapsed = int(sw.get("elapsed") or 0)
        else:
            elapsed = int(_now_ts() - float(sw.get("start_ts") or _now_ts()))
        return f"⏱️ Running: {_fmt_hms(elapsed)}. Use '/stopwatch stop' or '/stop stopwatch'."

    def stopwatch_start(self, sender_key: str, node_id: Any) -> str:
        now = _now_ts()
        with self._lock:
            sw = self.state.setdefault("stopwatches", {}).get(sender_key)
            if not isinstance(sw, dict):
                sw = {}
                self.state["stopwatches"][sender_key] = sw
            sw["running"] = True
            sw["paused"] = False
            sw["start_ts"] = now
            sw["node_id"] = node_id
            sw.pop("elapsed", None)
            sw.pop("paused_at", None)
        self._save()
        return "⏱️ Stopwatch started."

    def stopwatch_stop(self, sender_key: str) -> str:
        with self._lock:
            sw = self.state.get("stopwatches", {}).get(sender_key)
            if not isinstance(sw, dict) or not sw.get("running"):
                return "No active stopwatch."
            if sw.get("paused"):
                elapsed = int(sw.get("elapsed") or 0)
            else:
                elapsed = int(_now_ts() - float(sw.get("start_ts") or _now_ts()))
            self.state.get("stopwatches", {}).pop(sender_key, None)
        self._save()
        return f"⏹️ Stopwatch stopped at {_fmt_hms(elapsed)}."

    # Background worker ------------------------------------------------
    def _tick_alarms(self, now: float) -> None:
        with self._lock:
            items_by_user = dict(self.state.get("alarms", {}))
            paused = set(self._paused_users.keys())
        for user_key, items in list(items_by_user.items()):
            if not items:
                continue
            for a in list(items):
                due = float(a.get("next_ts") or 0)
                if not due:
                    continue
                if user_key in paused:
                    continue
                if due <= now:
                    label = f" — {a.get('label')}" if a.get("label") else ""
                    self._send_dm(a.get("node_id"), f"⏰ Alarm!{label}")
                    repeat = a.get("repeat") or "once"
                    if repeat == "daily":
                        next_dt = datetime.fromtimestamp(due).astimezone() + timedelta(days=1)
                        next_dt = next_dt.replace(second=0, microsecond=0)
                        a["next_ts"] = next_dt.timestamp()
                    else:
                        with self._lock:
                            arr = self.state.get("alarms", {}).get(user_key, [])
                            arr[:] = [x for x in arr if x.get("id") != a.get("id")]
                        self._save()
        # Persist updated daily alarms if any were moved forward
        self._save()

    def _tick_timers(self, now: float) -> None:
        with self._lock:
            items_by_user = dict(self.state.get("timers", {}))
            paused = set(self._paused_users.keys())
        for user_key, items in list(items_by_user.items()):
            if not items:
                continue
            for t in list(items):
                if t.get("paused"):
                    continue
                if user_key in paused:
                    continue
                due_ts = float(t.get("due_ts") or 0)
                if due_ts and due_ts <= now:
                    label = f" — {t.get('label')}" if t.get("label") else ""
                    self._send_dm(t.get("node_id"), f"⏲️ Timer finished{label}.")
                    with self._lock:
                        arr = self.state.get("timers", {}).get(user_key, [])
                        arr[:] = [x for x in arr if x.get("id") != t.get("id")]
                    self._save()

    def _tick_stopwatches(self, now: float) -> None:
        with self._lock:
            items = dict(self.state.get("stopwatches", {}))
            paused = dict(self._paused_users)
        for user_key, sw in list(items.items()):
            if not isinstance(sw, dict) or not sw.get("running"):
                continue
            if sw.get("paused"):
                sw["elapsed"] = int(sw.get("elapsed") or 0)
                continue
            start_ts = float(sw.get("start_ts") or now)
            elapsed = now - start_ts
            if elapsed >= 24 * 3600:
                self._send_dm(sw.get("node_id"), "⏱️ Stopwatch reached 24:00:00 and was cleared.")
                with self._lock:
                    self.state.get("stopwatches", {}).pop(user_key, None)
                self._save()
            else:
                if user_key in paused:
                    with self._lock:
                        sw["paused"] = True
                        sw["paused_at"] = now
                        sw["elapsed"] = int(elapsed)
                    self._save()

    def _worker(self) -> None:
        self.clean_log("Alarm/timer scheduler started", "⏱️", True, False)
        while not self._stop_event.is_set():
            try:
                now = _now_ts()
                self._tick_alarms(now)
                self._tick_timers(now)
                self._tick_stopwatches(now)
            except Exception:
                time.sleep(1)
            time.sleep(1)
