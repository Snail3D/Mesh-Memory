from __future__ import annotations

import random
import string
import textwrap
from collections import Counter, deque
from dataclasses import dataclass, field
from typing import Callable, Deque, Dict, List, Optional, Tuple

import requests

from mesh_master.replies import PendingReply


# ----------------------------
# Utility + Shared Data
# ----------------------------

def _format_lines(lines: List[str]) -> str:
    return "\n".join([line.rstrip() for line in lines if line is not None])


def _friendly_language(code: str) -> str:
    lookup = {
        "en": "English",
        "es": "Spanish",
    }
    return lookup.get((code or "").lower(), code or "the player's language")


HANGMAN_WORDS = [
    "antenna", "backpack", "beacon", "cavalry", "campfire", "compass", "desert",
    "evac", "field", "frontier", "garrison", "gear", "harbor", "meshnet",
    "outpost", "oxygen", "patrol", "radio", "ranger", "resupply", "shelter",
    "sierra", "signal", "squad", "summit", "survival", "tunnel", "uplink",
    "waypoint", "wildfire",
]

WORDLE_WORDS = [
    "trail", "nodes", "ridge", "radio", "torch", "caves", "hydrate", "scout",
    "storm", "flint", "gorse", "pulse", "wires", "crows", "badge", "morse",
    "watch", "flank", "intel", "gourd",
]
WORDLE_WORDS = [word for word in WORDLE_WORDS if len(word) == 5]

WORD_LADDER_PAIRS = [
    ("cold", "warm"),
    ("mesh", "node"),
    ("fire", "camp"),
    ("dust", "rain"),
    ("east", "west"),
    ("safe", "camp"),
    ("hope", "amen"),
]

CIPHER_PHRASES = [
    "stay alert", "check batteries", "secure perimeter", "signal ready",
    "share water", "mark location", "prep supplies", "hold position",
]

BINGO_TASKS = [
    "Share SOS in Morse", "Relay a weather update", "Scan channel 1",
    "Test your antenna", "Send GPS coordinates", "Log a received packet",
    "Thank a relay node", "Report battery level", "Ping the group",
    "Share a survival tip", "Record wind speed", "Call for radio check",
    "Send a meme", "Log sunrise time", "Report signal strength",
]

QUIZ_QUESTIONS = [
    {
        "question": "Which Meshtastic module boosts range the most?",
        "choices": ["Internal antenna", "Directional Yagi", "Rubber duck"],
        "answer": 1,
    },
    {
        "question": "How many dits are in the Morse letter S?",
        "choices": ["1", "2", "3"],
        "answer": 2,
    },
    {
        "question": "Best first step when lightning hits nearby?",
        "choices": ["Drop antenna mast", "Increase transmit power", "Send SOS"],
        "answer": 0,
    },
    {
        "question": "Ideal number of nodes to keep a flood mesh alive?",
        "choices": ["1", "2", "3+"],
        "answer": 2,
    },
    {
        "question": "What does APRS primarily carry?",
        "choices": ["Voice", "Packet data", "Video"],
        "answer": 1,
    },
]

MORSE_TABLE = {
    "a": ".-", "b": "-...", "c": "-.-.", "d": "-..", "e": ".",
    "f": "..-.", "g": "--.", "h": "....", "i": "..", "j": ".---",
    "k": "-.-", "l": ".-..", "m": "--", "n": "-.", "o": "---",
    "p": ".--.", "q": "--.-", "r": ".-.", "s": "...", "t": "-",
    "u": "..-", "v": "...-", "w": ".--", "x": "-..-", "y": "-.--",
    "z": "--..", "1": ".----", "2": "..---", "3": "...--",
    "4": "....-", "5": ".....", "6": "-....", "7": "--...",
    "8": "---..", "9": "----.", "0": "-----",
}
MORSE_WORDS = [
    "mesh", "radio", "rescue", "signal", "shelter", "sos", "supply",
    "water", "tools", "fire", "evac", "status",
]

RPS_RESPONSES = [
    "🪨 Rock! I just crumbled scissors into glitter, {player}.",
    "📄 Paper slides in with a smug grin—rocks never saw it coming, {player}.",
    "✂️ Scissors are snipping victory ribbons today, {player}!",
    "🪨 Rock thumps the table and flexes. Care to rematch, {player}?",
    "📄 Paper floats down like a command order—compliance accepted, {player}.",
    "✂️ Scissors twirl and shout ‘next challenger please’, {player}!",
]

COIN_FLIP_RESPONSES = [
    "🪙 The coin twinkles in the air for {player}… it lands flat with a grin: HEADS!",
    "🪙 A swift toss, a dusty thud—the coin declares TAILS for {player}!",
    "🪙 Beacon light catches the spin; it clinks to rest on HEADS!",
    "🪙 Gusts swirl around the flip… the coin settles on TAILS!",
    "🪙 Flung skyward, spinning wildly—when it drops, it's HEADS!",
    "🪙 Campfire sparks reflect off the coin—TAILS stares back!",
]


# ----------------------------
# Hangman
# ----------------------------

@dataclass
class HangmanSession:
    word: str
    guessed: set = field(default_factory=set)
    misses: set = field(default_factory=set)
    max_misses: int = 6

    def mask(self) -> str:
        return " ".join([c.upper() if c in self.guessed else "_" for c in self.word])

    def is_won(self) -> bool:
        return all(c in self.guessed for c in self.word)

    def is_lost(self) -> bool:
        return len(self.misses) >= self.max_misses

    def status_lines(self) -> List[str]:
        return [
            f"Word: {self.mask()}",
            f"Wrong: {' '.join(sorted(self.misses)) or '—'}",
            f"Lives: {self.max_misses - len(self.misses)}",
        ]


# ----------------------------
# Wordle
# ----------------------------

@dataclass
class WordleSession:
    target: str
    attempts: List[Tuple[str, str]] = field(default_factory=list)
    max_attempts: int = 6

    def add_attempt(self, guess: str, pattern: str) -> None:
        self.attempts.append((guess, pattern))

    def is_complete(self) -> bool:
        return any(g == self.target for g, _ in self.attempts) or len(self.attempts) >= self.max_attempts


# ----------------------------
# Choose-your-own adventure
# ----------------------------

@dataclass
class ChooseSession:
    topic: str
    language: str = "en"
    history: Deque[str] = field(default_factory=lambda: deque(maxlen=6))
    last_options: List[str] = field(default_factory=list)
    turn: int = 0

    def append(self, text: str) -> None:
        self.history.append(text)

    def build_prompt(self, request: str) -> str:
        past = "\n".join(self.history)
        language_name = _friendly_language(self.language)
        return textwrap.dedent(
            f"""
            You are an interactive narrator running a choose-your-own-adventure game for a radio mesh network operator.
            Keep scenes to 3 paragraphs max, describe vivid but concise imagery, and always offer exactly three numbered options.
            Each option must be actionable and distinct. Respond in under 180 words.
            Respond entirely in {language_name}. If the player mixes languages, follow their lead.

            Running Adventure Topic: {self.topic}
            Prior Log:
            {past or '[Start new branch]'}

            Player request / choice: {request}

            Format strictly as:
            Scene: <description>
            Options:
            1. <option one>
            2. <option two>
            3. <option three>
            """
        ).strip()


# ----------------------------
# Word Ladder
# ----------------------------


@dataclass
class WordLadderSession:
    start: str
    target: str
    current: str
    path: List[str] = field(default_factory=list)
    max_steps: int = 10

    def steps_used(self) -> int:
        return max(0, len(self.path) - 1)

    def remaining(self) -> int:
        return max(0, self.max_steps - self.steps_used())


# ----------------------------
# Cipher
# ----------------------------

@dataclass
class CipherSession:
    plain: str
    shift: int
    encoded: str
    attempts: int = 0
    hint_revealed: bool = False


# ----------------------------
# Bingo
# ----------------------------

@dataclass
class BingoSession:
    grid: Dict[str, str]
    completed: set = field(default_factory=set)

    def render(self) -> str:
        lines = ["🎯 Mesh Bingo", ""]
        cols = ["A", "B", "C"]
        for row in range(1, 4):
            row_cells = []
            for col in cols:
                key = f"{col}{row}"
                label = "✅" if key in self.completed else "⬜"
                row_cells.append(f"{label} {key}")
            lines.append("  ".join(row_cells))
        lines.append("")
        for key in sorted(self.grid.keys()):
            marker = "✅" if key in self.completed else "▫️"
            lines.append(f"{marker} {key}: {self.grid[key]}")
        return "\n".join(lines)

    def is_bingo(self) -> bool:
        rows = [
            {f"A{r}", f"B{r}", f"C{r}"} for r in range(1, 4)
        ]
        cols = [
            {f"{c}1", f"{c}2", f"{c}3"} for c in "ABC"
        ]
        diag = [{"A1", "B2", "C3"}, {"A3", "B2", "C1"}]
        lines = rows + cols + diag
        return any(line.issubset(self.completed) for line in lines)


# ----------------------------
# Quiz Battle
# ----------------------------

@dataclass
class QuizBattleSession:
    queue: Deque[Dict[str, object]]
    current: Optional[Dict[str, object]] = None
    correct: int = 0
    total: int = 0


# ----------------------------
# Morse Trainer
# ----------------------------

@dataclass
class MorseSession:
    word: str
    pattern: str
    attempts: int = 0
    hint_used: bool = False


class GameManager:
    """Container for all mini-games."""

    def __init__(
        self,
        *,
        clean_log: Callable[..., None],
        ai_log: Callable[..., None],
        ollama_url: Optional[str],
        choose_model: str,
        choose_timeout: int,
        wordladder_model: Optional[str] = None,
    ) -> None:
        self.clean_log = clean_log
        self.ai_log = ai_log
        self.ollama_url = ollama_url
        self.choose_model = choose_model
        self.choose_timeout = choose_timeout
        self.wordladder_model = wordladder_model or choose_model

        self.hangman_sessions: Dict[str, HangmanSession] = {}
        self.wordle_sessions: Dict[str, WordleSession] = {}
        self.choose_sessions: Dict[str, ChooseSession] = {}
        self.wordladder_sessions: Dict[str, WordLadderSession] = {}
        self.cipher_sessions: Dict[str, CipherSession] = {}
        self.bingo_sessions: Dict[str, BingoSession] = {}
        self.quiz_sessions: Dict[str, QuizBattleSession] = {}
        self.quiz_scores: Dict[str, int] = {}
        self.morse_sessions: Dict[str, MorseSession] = {}

    # ------------------------
    # Public dispatcher
    # ------------------------
    def handle_command(
        self,
        cmd: str,
        arguments: str,
        sender_key: str,
        sender_short: str,
        language: str,
    ) -> PendingReply:
        cmd = cmd.lower()
        args = arguments.strip()
        if cmd == "/games":
            return self._games_menu()
        if cmd == "/hangman":
            return self._handle_hangman(sender_key, args)
        if cmd == "/wordle":
            return self._handle_wordle(sender_key, args)
        if cmd in {"/choose", "/adventure"}:
            return self._handle_choose(sender_key, sender_short, args, language)
        if cmd == "/wordladder":
            return self._handle_wordladder(sender_key, sender_short, args)
        if cmd == "/rps":
            return self._handle_rps(sender_short)
        if cmd == "/coinflip":
            return self._handle_coinflip(sender_short)
        if cmd == "/cipher":
            return self._handle_cipher(sender_key, args)
        if cmd == "/bingo":
            return self._handle_bingo(sender_key, args)
        if cmd == "/quizbattle":
            return self._handle_quizbattle(sender_key, sender_short, args)
        if cmd == "/morse":
            return self._handle_morse(sender_key, args)
        return PendingReply("Game not recognized.", "games")

    # ------------------------
    # Menu
    # ------------------------
    def _games_menu(self) -> PendingReply:
        lines = [
            "🎮 Mesh Master Games Hub",
            "",
            "• /hangman start — classic word rescue",
            "• /wordle start — five-letter daily challenge",
            "• /wordladder start [from] [to] — llama-guided ladder",
            "• /adventure start <theme> — llama-guided story",
            "• /rps — rock · paper · scissors with attitude",
            "• /coinflip — dramatic mesh coin toss",
            "• /cipher start — decode the field transmission",
            "• /bingo start — mesh mission bingo",
            "• /quizbattle start — rapid-fire quiz",
            "• /morse start — decode dits and dahs",
            "",
            "Tips: use `status`, `guess`, `answer`, or `stop` with each game for more controls.",
        ]
        return PendingReply(_format_lines(lines), "games menu")

    # ------------------------
    # Hangman logic
    # ------------------------
    def _handle_hangman(self, sender_key: str, args: str) -> PendingReply:
        session = self.hangman_sessions.get(sender_key)
        if not args or args.lower() == "status":
            if not session:
                return PendingReply("Type `/hangman start` to begin a new round.", "hangman status")
            status = session.status_lines()
            if session.is_won():
                self.hangman_sessions.pop(sender_key, None)
                status.append("🎉 You already cracked that code. Start again with `/hangman start`.")
            elif session.is_lost():
                word = session.word.upper()
                self.hangman_sessions.pop(sender_key, None)
                status.append(f"💀 Out of lives! The word was {word}. Start over with `/hangman start`.")
            return PendingReply(_format_lines(status), "hangman status")

        if args.lower() == "start":
            word = random.choice(HANGMAN_WORDS)
            session = HangmanSession(word=word)
            self.hangman_sessions[sender_key] = session
            lines = [
                "🎯 Hangman online — rescue the word before the mesh goes quiet!",
                *session.status_lines(),
                "Guess letters with `/hangman guess a` or the full word with `/hangman solve word`.",
            ]
            return PendingReply(_format_lines(lines), "hangman start")

        if args.lower().startswith("guess"):
            if not session:
                return PendingReply("Start a game first with `/hangman start`.", "hangman guess")
            parts = args.split()
            if len(parts) < 2 or len(parts[1]) != 1 or not parts[1].isalpha():
                return PendingReply("Use `/hangman guess <letter>`.", "hangman guess")
            letter = parts[1].lower()
            if letter in session.guessed or letter in session.misses:
                return PendingReply("You already tried that letter.", "hangman guess")
            if letter in session.word:
                session.guessed.add(letter)
                if session.is_won():
                    mask = session.mask()
                    self.hangman_sessions.pop(sender_key, None)
                    return PendingReply(
                        _format_lines([f"🎉 {mask}", "Victory! Start another with `/hangman start`."]),
                        "hangman guess",
                    )
                return PendingReply(_format_lines(["✅ Nice hit!", *session.status_lines()]), "hangman guess")
            session.misses.add(letter)
            if session.is_lost():
                word = session.word.upper()
                self.hangman_sessions.pop(sender_key, None)
                return PendingReply(
                    _format_lines(["💀 No more lives.", f"The word was {word}.", "Try again with `/hangman start`."]),
                    "hangman guess",
                )
            return PendingReply(_format_lines(["❌ Miss.", *session.status_lines()]), "hangman guess")

        if args.lower().startswith("solve"):
            if not session:
                return PendingReply("Start a game first with `/hangman start`.", "hangman solve")
            guess = args.split(None, 1)[1].strip().lower() if len(args.split(None, 1)) > 1 else ""
            if not guess:
                return PendingReply("Use `/hangman solve <word>`.", "hangman solve")
            if guess == session.word:
                self.hangman_sessions.pop(sender_key, None)
                return PendingReply(
                    _format_lines([f"🎉 Correct! The word was {session.word.upper()}.", "Play again with `/hangman start`."]),
                    "hangman solve",
                )
            session.misses.add("⚡")
            return PendingReply(_format_lines(["⚠️ Not quite — keep guessing letters!", *session.status_lines()]), "hangman solve")

        if args.lower() == "stop":
            self.hangman_sessions.pop(sender_key, None)
            return PendingReply("Hangman cleared. Come back anytime with `/hangman start`.", "hangman stop")

        return PendingReply("Commands: `start`, `guess <letter>`, `solve <word>`, `status`, `stop`.", "hangman help")

    # ------------------------
    # Wordle logic
    # ------------------------
    def _handle_wordle(self, sender_key: str, args: str) -> PendingReply:
        session = self.wordle_sessions.get(sender_key)
        if not args or args.lower() == "status":
            if not session:
                return PendingReply("Type `/wordle start` for a fresh puzzle.", "wordle status")
            lines = ["🟩 Mesh-Wordle attempts:"]
            if session.attempts:
                for guess, pattern in session.attempts:
                    lines.append(f"{pattern}  {guess.upper()}")
            else:
                lines.append("No guesses yet. Use `/wordle guess <word>`.")
            if session.is_complete():
                if any(g == session.target for g, _ in session.attempts):
                    lines.append(f"🎉 Solved! The word was {session.target.upper()}.")
                else:
                    lines.append(f"💀 Out of tries. The word was {session.target.upper()}.")
                lines.append("Start again with `/wordle start`.")
            return PendingReply(_format_lines(lines), "wordle status")

        if args.lower() == "start":
            target = random.choice(WORDLE_WORDS)
            session = WordleSession(target=target)
            self.wordle_sessions[sender_key] = session
            return PendingReply(
                "🟨 Wordle online! Guess a five-letter word with `/wordle guess <word>`.",
                "wordle start",
            )

        if args.lower().startswith("guess"):
            if not session:
                return PendingReply("Start a puzzle first with `/wordle start`.", "wordle guess")
            parts = args.split()
            if len(parts) < 2:
                return PendingReply("Use `/wordle guess <word>`.", "wordle guess")
            guess = parts[1].lower()
            if len(guess) != 5 or not guess.isalpha():
                return PendingReply("Guesses must be five letters.", "wordle guess")
            pattern = self._wordle_pattern(session.target, guess)
            session.add_attempt(guess, pattern)
            lines = [f"{pattern}  {guess.upper()}"]
            if guess == session.target:
                self.wordle_sessions.pop(sender_key, None)
                lines.append("🎉 Nailed it! `/wordle start` for another round.")
            elif len(session.attempts) >= session.max_attempts:
                self.wordle_sessions.pop(sender_key, None)
                lines.append(f"💀 All attempts used. The word was {session.target.upper()}.")
            else:
                remaining = session.max_attempts - len(session.attempts)
                lines.append(f"Attempts left: {remaining}.")
            return PendingReply(_format_lines(lines), "wordle guess")

        if args.lower() == "stop":
            self.wordle_sessions.pop(sender_key, None)
            return PendingReply("Wordle session cleared.", "wordle stop")

        return PendingReply("Commands: `start`, `guess <word>`, `status`, `stop`.", "wordle help")

    def _wordle_pattern(self, target: str, guess: str) -> str:
        squares = []
        target_counts = Counter(target)
        # First pass for greens
        for idx, letter in enumerate(guess):
            if letter == target[idx]:
                squares.append("🟩")
                target_counts[letter] -= 1
            else:
                squares.append(None)
        # Second pass for yellows/whites
        for idx, letter in enumerate(guess):
            if squares[idx] is not None:
                continue
            if target_counts[letter] > 0:
                squares[idx] = "🟨"
                target_counts[letter] -= 1
            else:
                squares[idx] = "⬜"
        return "".join(squares)

    # ------------------------
    # Choose adventure
    # ------------------------
    def _handle_choose(self, sender_key: str, sender_short: str, args: str, language: str) -> PendingReply:
        if not args:
            if sender_key not in self.choose_sessions:
                return PendingReply("Use `/adventure start <setting>` to launch an adventure.", "choose status")
            return PendingReply("Pick an option with `/adventure 1`, `/adventure 2`, or `/adventure 3`.", "choose status")

        tokens = args.split(None, 1)
        verb = tokens[0].lower()
        rest = tokens[1].strip() if len(tokens) > 1 else ""

        if verb == "start":
            topic = rest or "mysterious signal"
            session = ChooseSession(topic=topic, language=language)
            self.choose_sessions[sender_key] = session
            return self._choose_advance(session, sender_key, f"Start an adventure about {topic} for operator {sender_short}.")

        if verb == "stop":
            self.choose_sessions.pop(sender_key, None)
            return PendingReply("Adventure paused. Come back with `/adventure start <theme>`.", "choose stop")

        if sender_key not in self.choose_sessions:
            return PendingReply("No active story. Begin with `/adventure start <theme>`.", "choose choice")

        session = self.choose_sessions[sender_key]
        session.language = language or session.language

        if verb in {"1", "2", "3"}:
            idx = int(verb) - 1
            if idx < 0 or idx >= len(session.last_options):
                return PendingReply("Invalid option. Choose 1, 2, or 3.", "choose invalid")
            choice_text = session.last_options[idx]
            player_line = f"Player {sender_short} chose option {verb}: {choice_text}"
            return self._choose_advance(session, sender_key, player_line)

        return PendingReply("Choose with `1`, `2`, or `3`, or stop with `/adventure stop`.", "choose help")

    def _choose_advance(self, session: ChooseSession, sender_key: str, player_request: str) -> PendingReply:
        session.turn += 1
        prompt = session.build_prompt(player_request)
        text = self._invoke_llama(prompt)
        if not text:
            return PendingReply("Story engine unavailable right now.", "choose error")
        session.append(text)
        options = self._extract_options(text)
        session.last_options = options
        self.choose_sessions[sender_key] = session
        return PendingReply(text, "choose scene", chunk_delay=4.0)

    def _invoke_llama(self, prompt: str) -> Optional[str]:
        if not self.ollama_url:
            return None
        payload = {
            "model": self.choose_model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": 0.7,
                "num_ctx": 2048,
                "num_predict": 220,
            },
        }
        try:
            self.ai_log("Spinning adventure", "Ollama")
            resp = requests.post(self.ollama_url, json=payload, timeout=self.choose_timeout)
            if resp.status_code != 200:
                self.clean_log(f"Choose engine error {resp.status_code}: {resp.text[:80]}", "⚠️")
                return None
            data = resp.json()
            text = data.get("response")
            if not text and isinstance(data.get("choices"), list):
                text = data["choices"][0].get("text")
            return text.strip() if text else None
        except Exception as exc:
            self.clean_log(f"Choose engine exception: {exc}", "⚠️")
            return None

    def _extract_options(self, text: str) -> List[str]:
        options: List[str] = []
        for line in text.splitlines():
            stripped = line.strip()
            if stripped.startswith("1.") or stripped.startswith("2.") or stripped.startswith("3."):
                options.append(stripped)
        return options

    # ------------------------
    # Word Ladder logic
    # ------------------------
    def _handle_wordladder(self, sender_key: str, sender_short: str, args: str) -> PendingReply:
        session = self.wordladder_sessions.get(sender_key)
        if not args or args.lower() == "status":
            if not session:
                return PendingReply(
                    "Use `/wordladder start <from> <to>` to launch a ladder or `/wordladder start` for a surprise pair.",
                    "wordladder status",
                )
            lines = [
                "🪜 Word Ladder",
                f"Start: {session.start.upper()} → Goal: {session.target.upper()}",
                f"Current: {session.current.upper()}",
                f"Path: {' → '.join(word.upper() for word in session.path)}",
                f"Steps remaining: {session.remaining()}",
                "Commands: `/wordladder guess <word>`, `/wordladder hint`, `/wordladder stop`.",
            ]
            return PendingReply(_format_lines(lines), "wordladder status")

        lower = args.lower()
        if lower.startswith("start"):
            tokens = args.split()
            if len(tokens) >= 3:
                start_word = tokens[1].lower()
                target_word = tokens[2].lower()
            else:
                start_word, target_word = random.choice(WORD_LADDER_PAIRS)
            if not start_word.isalpha() or not target_word.isalpha():
                return PendingReply("Words must be alphabetic only.", "wordladder start")
            if len(start_word) != len(target_word):
                return PendingReply("Start and goal must have the same length.", "wordladder start")
            if start_word == target_word:
                return PendingReply("Pick two different words for the ladder.", "wordladder start")
            session = WordLadderSession(
                start=start_word,
                target=target_word,
                current=start_word,
                path=[start_word],
            )
            self.wordladder_sessions[sender_key] = session
            lines = [
                "🪜 Word ladder deployed!",
                f"Start at {start_word.upper()} and reach {target_word.upper()} in {session.max_steps} steps or fewer.",
                "Change exactly one letter per move and keep every result a real word.",
                "Use `/wordladder guess <word>` to add your own rung or `/wordladder hint` for a llama suggestion.",
            ]
            return PendingReply(_format_lines(lines), "wordladder start")

        if lower.startswith("guess"):
            if not session:
                return PendingReply("Start a ladder first with `/wordladder start`.", "wordladder guess")
            parts = args.split(None, 1)
            guess = parts[1].strip().lower() if len(parts) > 1 else ""
            if not guess.isalpha():
                return PendingReply("Guesses must be letters only.", "wordladder guess")
            if not self._is_valid_ladder_step(session, guess):
                return PendingReply(
                    f"Need a new word of length {len(session.current)} that changes exactly one letter and isn't reused.",
                    "wordladder guess",
                )
            session.path.append(guess)
            session.current = guess
            self.wordladder_sessions[sender_key] = session
            if guess == session.target:
                text = _format_lines([
                    f"🎉 {guess.upper()}!", "Word ladder complete:",
                    " → ".join(word.upper() for word in session.path),
                ])
                self.clean_log(f"Word ladder solved by {sender_short}", "🪜")
                self.wordladder_sessions.pop(sender_key, None)
                return PendingReply(text, "wordladder guess")
            if session.remaining() == 0:
                trail = " → ".join(word.upper() for word in session.path)
                self.wordladder_sessions.pop(sender_key, None)
                return PendingReply(
                    _format_lines([
                        "💀 No more steps left.",
                        f"Trail: {trail}",
                        f"Goal was {session.target.upper()}.",
                    ]),
                    "wordladder guess",
                )
            return PendingReply(
                _format_lines([
                    "✅ Logged!",
                    f"Current: {session.current.upper()} → Goal: {session.target.upper()}",
                    f"Steps remaining: {session.remaining()}",
                ]),
                "wordladder guess",
            )

        if lower in {"hint", "step", "llama"}:
            if not session:
                return PendingReply("Start a ladder first with `/wordladder start`.", "wordladder hint")
            suggestion = self._invoke_wordladder(session)
            if not suggestion:
                return PendingReply("🤖 No valid hint available right now.", "wordladder hint")
            session.path.append(suggestion)
            session.current = suggestion
            self.wordladder_sessions[sender_key] = session
            if suggestion == session.target:
                text = _format_lines([
                    f"🎉 {suggestion.upper()}! Llama closed the ladder:",
                    " → ".join(word.upper() for word in session.path),
                ])
                self.wordladder_sessions.pop(sender_key, None)
                return PendingReply(text, "wordladder hint")
            if session.remaining() == 0:
                trail = " → ".join(word.upper() for word in session.path)
                self.wordladder_sessions.pop(sender_key, None)
                return PendingReply(
                    _format_lines([
                        "💀 Llama used the last rung.",
                        f"Trail: {trail}",
                        f"Goal was {session.target.upper()}.",
                    ]),
                    "wordladder hint",
                )
            return PendingReply(
                _format_lines([
                    f"🦙 Suggested: {suggestion.upper()}",
                    f"Next target: {session.target.upper()} (steps left: {session.remaining()})",
                ]),
                "wordladder hint",
            )

        if lower == "stop":
            self.wordladder_sessions.pop(sender_key, None)
            return PendingReply("Word ladder cleared.", "wordladder stop")

        return PendingReply("Commands: `start [from] [to]`, `guess <word>`, `hint`, `status`, `stop`.", "wordladder help")

    def _is_valid_ladder_step(self, session: WordLadderSession, guess: str) -> bool:
        if len(guess) != len(session.current):
            return False
        if guess == session.current:
            return False
        if guess in session.path:
            return False
        return self._differs_by_one_letter(session.current, guess)

    def _differs_by_one_letter(self, a: str, b: str) -> bool:
        if len(a) != len(b):
            return False
        diffs = sum(1 for x, y in zip(a, b) if x != y)
        return diffs == 1

    def _invoke_wordladder(self, session: WordLadderSession) -> Optional[str]:
        if not self.ollama_url or not self.wordladder_model:
            return None
        history = ", ".join(word.upper() for word in session.path)
        prompt = textwrap.dedent(
            f"""
            You are solving a word ladder puzzle for radio operators. Provide one English word.
            Each step must:
            • Be the same length as the current word.
            • Differ by exactly one letter from the current word.
            • Not repeat any word already used.
            • Move toward the final goal if possible.

            Current word: {session.current.upper()}
            Target word: {session.target.upper()}
            Words used so far: {history or 'NONE'}

            Respond with a single lowercase word that obeys the rules. If the target word is one letter away, respond with the target itself. Do not add punctuation or explanations.
            """
        ).strip()
        payload = {
            "model": self.wordladder_model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": 0.5,
                "num_ctx": 1024,
                "num_predict": 40,
            },
        }
        try:
            self.ai_log("Word ladder hint", "Ollama")
            response = requests.post(self.ollama_url, json=payload, timeout=self.choose_timeout)
            if response.status_code != 200:
                self.clean_log(f"Word ladder error {response.status_code}: {response.text[:80]}", "⚠️")
                return None
            data = response.json()
            text = data.get("response")
            if not text and isinstance(data.get("choices"), list) and data["choices"]:
                text = data["choices"][0].get("text")
            if not text:
                return None
            word = text.strip().split()[0].lower()
            if word == session.target and self._differs_by_one_letter(session.current, word):
                return word
            if self._is_valid_ladder_step(session, word):
                return word
            self.clean_log(f"Word ladder hint rejected: {word}", "⚠️")
            return None
        except Exception as exc:
            self.clean_log(f"Word ladder exception: {exc}", "⚠️")
            return None

    # ------------------------
    # Quick randomizers
    # ------------------------
    def _handle_rps(self, sender_short: str) -> PendingReply:
        taunt = random.choice(RPS_RESPONSES).format(player=sender_short)
        return PendingReply(taunt, "rps")

    def _handle_coinflip(self, sender_short: str) -> PendingReply:
        story = random.choice(COIN_FLIP_RESPONSES).format(player=sender_short)
        return PendingReply(story, "coinflip")

    # ------------------------
    # Cipher
    # ------------------------
    def _handle_cipher(self, sender_key: str, args: str) -> PendingReply:
        session = self.cipher_sessions.get(sender_key)
        if not args or args.lower() == "status":
            if not session:
                return PendingReply("`/cipher start` for a new scrambled message.", "cipher status")
            lines = [
                "🔐 Cipher challenge:",
                f"Message: {session.encoded}",
                "Use `/cipher answer <plaintext>` or `/cipher hint`.",
            ]
            return PendingReply(_format_lines(lines), "cipher status")

        if args.lower() == "start":
            plain = random.choice(CIPHER_PHRASES)
            shift = random.randint(1, 25)
            encoded = self._caesar(plain, shift)
            session = CipherSession(plain=plain, shift=shift, encoded=encoded)
            self.cipher_sessions[sender_key] = session
            return PendingReply(
                _format_lines(["🔐 New cipher loaded!", f"Message: {encoded}", "Reply with `/cipher answer ...`."]),
                "cipher start",
            )

        if args.lower().startswith("answer"):
            if not session:
                return PendingReply("Start a puzzle first with `/cipher start`.", "cipher answer")
            guess = args.split(None, 1)[1].strip().lower() if len(args.split(None, 1)) > 1 else ""
            if not guess:
                return PendingReply("Include your plaintext guess, e.g. `/cipher answer stay alert`.", "cipher answer")
            session.attempts += 1
            if guess == session.plain:
                self.cipher_sessions.pop(sender_key, None)
                return PendingReply("✅ Correct! Cipher cleared. `/cipher start` for another.", "cipher answer")
            return PendingReply("❌ Negative copy. Try again or `/cipher hint`.", "cipher answer")

        if args.lower() == "hint":
            if not session:
                return PendingReply("No active cipher. `/cipher start` first.", "cipher hint")
            if not session.hint_revealed:
                hint = session.plain.split()[0]
                session.hint_revealed = True
                return PendingReply(f"🔎 First word hint: {hint}", "cipher hint")
            return PendingReply("Hint already shared. Decode the rest!", "cipher hint")

        if args.lower() == "stop":
            self.cipher_sessions.pop(sender_key, None)
            return PendingReply("Cipher session cleared.", "cipher stop")

        return PendingReply("Commands: `start`, `answer <text>`, `hint`, `status`, `stop`.", "cipher help")

    def _caesar(self, text_value: str, shift: int) -> str:
        result_chars = []
        for ch in text_value:
            if ch.isalpha():
                base = ord('a') if ch.islower() else ord('A')
                result_chars.append(chr(base + ((ord(ch) - base + shift) % 26)))
            else:
                result_chars.append(ch)
        return ''.join(result_chars)

    # ------------------------
    # Bingo
    # ------------------------
    def _handle_bingo(self, sender_key: str, args: str) -> PendingReply:
        session = self.bingo_sessions.get(sender_key)
        if args.lower() == "start" or not args:
            if not session or args.lower() == "start":
                tiles = random.sample(BINGO_TASKS, 9)
                grid = {}
                letters = "ABC"
                idx = 0
                for row in range(1, 4):
                    for col in letters:
                        grid[f"{col}{row}"] = tiles[idx]
                        idx += 1
                session = BingoSession(grid=grid)
                self.bingo_sessions[sender_key] = session
                return PendingReply(session.render(), "bingo start", chunk_delay=2.5)
            return PendingReply(session.render(), "bingo status", chunk_delay=2.0)

        if args.lower().startswith("mark"):
            if not session:
                return PendingReply("No bingo card yet. `/bingo start` first.", "bingo mark")
            parts = args.split()
            if len(parts) < 2:
                return PendingReply("Use `/bingo mark B2`.", "bingo mark")
            cell = parts[1].upper()
            if cell not in session.grid:
                return PendingReply("That tile isn't on your board.", "bingo mark")
            session.completed.add(cell)
            text = session.render()
            if session.is_bingo():
                self.bingo_sessions.pop(sender_key, None)
                text += "\n🎉 BINGO! Start a new sheet with `/bingo start`."
            return PendingReply(text, "bingo mark", chunk_delay=2.0)

        if args.lower() == "stop":
            self.bingo_sessions.pop(sender_key, None)
            return PendingReply("Bingo card cleared.", "bingo stop")

        return PendingReply("Commands: `start`, `mark <cell>`, `stop`.", "bingo help")

    # ------------------------
    # Quiz Battle
    # ------------------------
    def _handle_quizbattle(self, sender_key: str, sender_short: str, args: str) -> PendingReply:
        session = self.quiz_sessions.get(sender_key)
        if not args or args.lower() == "status":
            if not session:
                return PendingReply("`/quizbattle start` to begin a rapid-fire round.", "quiz status")
            if session.current:
                question = session.current["question"]
                choices = session.current["choices"]
                lines = ["⚡ Current question:", question]
                for idx, choice in enumerate(choices, start=1):
                    lines.append(f" {idx}. {choice}")
                lines.append("Answer with `/quizbattle answer <number>`." )
                return PendingReply(_format_lines(lines), "quiz status")
            return PendingReply("No active question. Use `/quizbattle next`.", "quiz status")

        if args.lower() == "start":
            queue = deque(random.sample(QUIZ_QUESTIONS, len(QUIZ_QUESTIONS)))
            session = QuizBattleSession(queue=queue)
            self.quiz_sessions[sender_key] = session
            return self._quiz_next(session, sender_key)

        if args.lower() == "next":
            if not session:
                return PendingReply("Start a round with `/quizbattle start`.", "quiz next")
            return self._quiz_next(session, sender_key)

        if args.lower().startswith("answer"):
            if not session or not session.current:
                return PendingReply("No question pending. `/quizbattle start` first.", "quiz answer")
            parts = args.split()
            if len(parts) < 2 or not parts[1].isdigit():
                return PendingReply("Use `/quizbattle answer <number>`.", "quiz answer")
            guess_idx = int(parts[1]) - 1
            choices = session.current["choices"]
            if guess_idx < 0 or guess_idx >= len(choices):
                return PendingReply("That option isn't available.", "quiz answer")
            correct_idx = session.current["answer"]
            session.total += 1
            if guess_idx == correct_idx:
                session.correct += 1
                reply = "✅ Correct!"
            else:
                answer_text = choices[correct_idx]
                reply = f"❌ Nope — answer was '{answer_text}'."
            session.current = None
            result = self._quiz_next(session, sender_key)
            self.quiz_scores[sender_short] = max(self.quiz_scores.get(sender_short, 0), session.correct)
            prefix = _format_lines([reply])
            return PendingReply(f"{prefix}\n\n{result.text}", "quiz answer")

        if args.lower() == "score":
            score = self.quiz_scores.get(sender_short)
            if score is None:
                return PendingReply("No high score logged yet.", "quiz score")
            return PendingReply(f"🏆 Best streak: {score} correct in a round.", "quiz score")

        if args.lower() == "stop":
            self.quiz_sessions.pop(sender_key, None)
            return PendingReply("Quiz round cleared.", "quiz stop")

        return PendingReply("Commands: `start`, `answer <num>`, `next`, `score`, `stop`.", "quiz help")

    def _quiz_next(self, session: QuizBattleSession, sender_key: str) -> PendingReply:
        if not session.queue:
            text = f"Round complete — {session.correct}/{session.total} correct!"
            self.quiz_sessions.pop(sender_key, None)
            return PendingReply(text, "quiz complete")
        session.current = session.queue.popleft()
        question = session.current["question"]
        choices = session.current["choices"]
        lines = ["⚡ Quiz Battle:", question]
        for idx, choice in enumerate(choices, start=1):
            lines.append(f" {idx}. {choice}")
        lines.append("Answer with `/quizbattle answer <number>`.")
        self.quiz_sessions[sender_key] = session
        return PendingReply(_format_lines(lines), "quiz next")

    # ------------------------
    # Morse trainer
    # ------------------------
    def _handle_morse(self, sender_key: str, args: str) -> PendingReply:
        session = self.morse_sessions.get(sender_key)
        if not args or args.lower() == "status":
            if not session:
                return PendingReply("`/morse start` to receive a pattern.", "morse status")
            return PendingReply(
                _format_lines([
                    "📻 Morse pattern ready:",
                    session.pattern,
                    "Guess with `/morse answer <word>` or `/morse hint`.",
                ]),
                "morse status",
            )

        if args.lower() == "start":
            word = random.choice(MORSE_WORDS)
            pattern = " ".join(MORSE_TABLE[ch] for ch in word)
            session = MorseSession(word=word, pattern=pattern)
            self.morse_sessions[sender_key] = session
            return PendingReply(
                _format_lines(["📻 New Morse challenge:", pattern, "Translate with `/morse answer <word>`."]),
                "morse start",
            )

        if args.lower().startswith("answer"):
            if not session:
                return PendingReply("No signal yet. `/morse start` first.", "morse answer")
            guess = args.split(None, 1)[1].strip().lower() if len(args.split(None, 1)) > 1 else ""
            if not guess:
                return PendingReply("Include your guess, e.g. `/morse answer mesh`.", "morse answer")
            session.attempts += 1
            if guess == session.word:
                self.morse_sessions.pop(sender_key, None)
                return PendingReply("✅ Correct decode! `/morse start` for another signal.", "morse answer")
            return PendingReply("❌ Not the message. Try again or `/morse hint`.", "morse answer")

        if args.lower() == "hint":
            if not session:
                return PendingReply("No active Morse signal.", "morse hint")
            if session.hint_used:
                return PendingReply("Hint already used — decode the rest!", "morse hint")
            session.hint_used = True
            return PendingReply(f"🔍 First letter: {session.word[0].upper()}", "morse hint")

        if args.lower() == "stop":
            self.morse_sessions.pop(sender_key, None)
            return PendingReply("Morse trainer cleared.", "morse stop")

        return PendingReply("Commands: `start`, `answer <word>`, `hint`, `status`, `stop`.", "morse help")
