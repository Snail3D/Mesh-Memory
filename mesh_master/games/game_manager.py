from __future__ import annotations

import json
import random
import string
import textwrap
from collections import Counter, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, time
from pathlib import Path
from typing import Any, Callable, Deque, Dict, List, Optional, Set, Tuple

import chess
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

WORDLE_STATE_PATH = Path("data/wordle_state.json")

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
    "Share SOS in Morse", "Relay a mesh status", "Scan channel 1",
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
    {
        "question": "Preferred battery chemistry for cold-weather field kits?",
        "choices": ["NiMH", "LiFePO4", "Lead-acid"],
        "answer": 1,
    },
    {
        "question": "What is the default Meshtastic channel modem speed?",
        "choices": ["LongFast", "ShortSlow", "Medium"],
        "answer": 0,
    },
    {
        "question": "Which band does Meshtastic typically use in North America?",
        "choices": ["433 MHz", "5150 MHz", "915 MHz"],
        "answer": 2,
    },
    {
        "question": "First survival priority when you're stranded overnight?",
        "choices": ["Build shelter", "Signal for help", "Start hiking"],
        "answer": 0,
    },
    {
        "question": "What's the minimum GPS fix quality recommended before broadcasting location?",
        "choices": ["2D", "3D", "Any"],
        "answer": 1,
    },
    {
        "question": "Which phonetic word represents the letter 'N'?",
        "choices": ["November", "Neptune", "North"],
        "answer": 0,
    },
    {
        "question": "What does the 'PTT' button on a handset do?",
        "choices": ["Boosts volume", "Activates transmit", "Stores a channel"],
        "answer": 1,
    },
    {
        "question": "How often should you rotate stored drinking water?",
        "choices": ["Every month", "Every six months", "Every two years"],
        "answer": 1,
    },
    {
        "question": "Best way to extend radio range without new hardware?",
        "choices": ["Increase retries", "Raise antenna elevation", "Lower bitrate"],
        "answer": 1,
    },
    {
        "question": "Which file extension stores Meshtastic configs exported from the app?",
        "choices": [".json", ".cfg", ".mesh"],
        "answer": 0,
    },
    {
        "question": "What's the universal distress signal on a whistle?",
        "choices": ["Two short blasts", "Three short blasts", "One long blast"],
        "answer": 1,
    },
    {
        "question": "Which satellite system augments GPS accuracy for aviation in North America?",
        "choices": ["GLONASS", "WAAS", "Galileo"],
        "answer": 1,
    },
    {
        "question": "Mesh repeaters should be spaced so packets arrive in roughly what time frame?",
        "choices": ["<1s", "2-5s", ">30s"],
        "answer": 1,
    },
    {
        "question": "What is the single most effective wildfire defense for a campsite?",
        "choices": ["Digging a trench", "Creating a firebreak", "Stacking wood"],
        "answer": 1,
    },
    {
        "question": "Which connector is standard on most Meshtastic development boards for antennas?",
        "choices": ["SMA", "MCX", "BNC"],
        "answer": 0,
    },
    {
        "question": "During a hasty evacuation, what's the minimum you should log for your mesh node?",
        "choices": ["Signal strength", "Last known coordinates", "Firmware version"],
        "answer": 1,
    },
    {
        "question": "What does LoRa stand for?",
        "choices": ["Long Range", "Local Radio", "Low Raster"],
        "answer": 0,
    },
    {
        "question": "In quiz battle, what command lets you see the current score streak?",
        "choices": ["/quizbattle stats", "/quizbattle score", "/quizbattle status"],
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


# ----------------------------
# Chess AI setup
# ----------------------------

CHESS_PIECE_VALUES = {
    chess.PAWN: 100,
    chess.KNIGHT: 320,
    chess.BISHOP: 330,
    chess.ROOK: 500,
    chess.QUEEN: 900,
    chess.KING: 0,
}

_PST_PAWN = [
    0, 0, 0, 0, 0, 0, 0, 0,
    50, 50, 50, 50, 50, 50, 50, 50,
    10, 10, 20, 30, 30, 20, 10, 10,
    5, 5, 10, 25, 25, 10, 5, 5,
    0, 0, 0, 20, 20, 0, 0, 0,
    5, -5, -10, 0, 0, -10, -5, 5,
    5, 10, 10, -20, -20, 10, 10, 5,
    0, 0, 0, 0, 0, 0, 0, 0,
]

_PST_KNIGHT = [
    -50, -40, -30, -30, -30, -30, -40, -50,
    -40, -20, 0, 0, 0, 0, -20, -40,
    -30, 0, 10, 15, 15, 10, 0, -30,
    -30, 5, 15, 20, 20, 15, 5, -30,
    -30, 0, 15, 20, 20, 15, 0, -30,
    -30, 5, 10, 15, 15, 10, 5, -30,
    -40, -20, 0, 5, 5, 0, -20, -40,
    -50, -40, -30, -30, -30, -30, -40, -50,
]

_PST_BISHOP = [
    -20, -10, -10, -10, -10, -10, -10, -20,
    -10, 0, 0, 0, 0, 0, 0, -10,
    -10, 0, 5, 10, 10, 5, 0, -10,
    -10, 5, 5, 10, 10, 5, 5, -10,
    -10, 0, 10, 10, 10, 10, 0, -10,
    -10, 10, 10, 10, 10, 10, 10, -10,
    -10, 5, 0, 0, 0, 0, 5, -10,
    -20, -10, -10, -10, -10, -10, -10, -20,
]

_PST_ROOK = [
    0, 0, 5, 10, 10, 5, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0,
    5, 10, 10, 10, 10, 10, 10, 5,
    5, 10, 10, 10, 10, 10, 10, 5,
    5, 10, 10, 10, 10, 10, 10, 5,
    5, 10, 10, 10, 10, 10, 10, 5,
    0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 5, 10, 10, 5, 0, 0,
]

_PST_QUEEN = [
    -20, -10, -10, -5, -5, -10, -10, -20,
    -10, 0, 0, 0, 0, 0, 0, -10,
    -10, 0, 5, 5, 5, 5, 0, -10,
    -5, 0, 5, 5, 5, 5, 0, -5,
    0, 0, 5, 5, 5, 5, 0, -5,
    -10, 5, 5, 5, 5, 5, 0, -10,
    -10, 0, 5, 0, 0, 0, 0, -10,
    -20, -10, -10, -5, -5, -10, -10, -20,
]

_PST_KING_MID = [
    -30, -40, -40, -50, -50, -40, -40, -30,
    -30, -40, -40, -50, -50, -40, -40, -30,
    -30, -40, -40, -50, -50, -40, -40, -30,
    -30, -40, -40, -50, -50, -40, -40, -30,
    -20, -30, -30, -40, -40, -30, -30, -20,
    -10, -20, -20, -20, -20, -20, -20, -10,
    20, 20, 0, 0, 0, 0, 20, 20,
    20, 30, 10, 0, 0, 10, 30, 20,
]

_PST_KING_END = [
    -50, -30, -30, -30, -30, -30, -30, -50,
    -30, -30, 0, 0, 0, 0, -30, -30,
    -30, 0, 10, 15, 15, 10, 0, -30,
    -30, 5, 15, 20, 20, 15, 5, -30,
    -30, 0, 15, 20, 20, 15, 0, -30,
    -30, 5, 10, 15, 15, 10, 5, -30,
    -30, 0, 0, 0, 0, 0, 0, -30,
    -50, -30, -30, -30, -30, -30, -30, -50,
]

CHESS_PST = {
    chess.PAWN: _PST_PAWN,
    chess.KNIGHT: _PST_KNIGHT,
    chess.BISHOP: _PST_BISHOP,
    chess.ROOK: _PST_ROOK,
    chess.QUEEN: _PST_QUEEN,
    "king_mid": _PST_KING_MID,
    "king_end": _PST_KING_END,
}

CHESS_MATE_SCORE = 100000


# ----------------------------
# Blackjack setup
# ----------------------------

BLACKJACK_SUITS = ["S", "H", "D", "C"]
BLACKJACK_RANKS = ["A", "2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K"]


# ----------------------------
# Checkers setup
# ----------------------------

CHECKERS_FILES = "abcdefgh"
CHECKERS_RANKS = "12345678"
CHECKERS_DIRECTIONS = {
    'w': [(-1, -1), (-1, 1)],
    'b': [(1, -1), (1, 1)],
}
CHECKERS_KING_DIRECTIONS = [(-1, -1), (-1, 1), (1, -1), (1, 1)]
CHECKERS_PROMOTION_ROW = {'w': 0, 'b': 7}
CHECKERS_MAN_VALUE = 120
CHECKERS_KING_VALUE = 200
CHECKERS_DEPTH_EARLY = 6
CHECKERS_DEPTH_LATE = 8


# ----------------------------
# Tic-Tac-Toe setup
# ----------------------------

TICTACTOE_FILES = "abc"
TICTACTOE_RANKS = "123"
TICTACTOE_LINES = [
    # rows
    [(0, 0), (0, 1), (0, 2)],
    [(1, 0), (1, 1), (1, 2)],
    [(2, 0), (2, 1), (2, 2)],
    # cols
    [(0, 0), (1, 0), (2, 0)],
    [(0, 1), (1, 1), (2, 1)],
    [(0, 2), (1, 2), (2, 2)],
    # diagonals
    [(0, 0), (1, 1), (2, 2)],
    [(0, 2), (1, 1), (2, 0)],
]


# ----------------------------
# Yahtzee setup
# ----------------------------

YAHTZEE_CATEGORIES = [
    "ones",
    "twos",
    "threes",
    "fours",
    "fives",
    "sixes",
    "three_kind",
    "four_kind",
    "full_house",
    "small_straight",
    "large_straight",
    "chance",
    "yahtzee",
]

YAHTZEE_LABELS = {
    "ones": "Ones",
    "twos": "Twos",
    "threes": "Threes",
    "fours": "Fours",
    "fives": "Fives",
    "sixes": "Sixes",
    "three_kind": "Three of a Kind",
    "four_kind": "Four of a Kind",
    "full_house": "Full House",
    "small_straight": "Small Straight",
    "large_straight": "Large Straight",
    "chance": "Chance",
    "yahtzee": "Yahtzee",
}


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
            You are the Mesh Master field narrator running a choose-your-own adventure for a radio mesh operator.
            Work only in {language_name}. If the player mixes languages, mirror their latest turn but otherwise stay in {language_name}.
            Keep the story to at most two short paragraphs with no more than three sentences each.
            Keep the entire reply under 140 words and avoid filler, recaps, or meta commentary.
            Offer exactly three numbered options, each under twelve words, action-focused, and mutually exclusive.

            Current Adventure Topic: {self.topic}
            Previous Log:
            {past or '[Start new branch]'}

            Player request / choice: {request}

            Format strictly as:
            Scene: <1-2 short paragraphs>
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


@dataclass
class ChessSession:
    board: chess.Board
    player_color: chess.Color
    ai_color: chess.Color
    move_history: List[str] = field(default_factory=list)
    last_player_move: Optional[str] = None
    last_ai_move: Optional[str] = None


@dataclass
class BlackjackSession:
    deck: List[str]
    player_hand: List[str]
    dealer_hand: List[str]
    finished: bool = False
    player_stood: bool = False
    outcome: Optional[str] = None


@dataclass
class CheckersSession:
    board: List[List[str]]
    player_color: str
    ai_color: str
    turn: str
    move_history: List[str] = field(default_factory=list)
    last_player_move: Optional[str] = None
    last_ai_move: Optional[str] = None


@dataclass
class TicTacToeSession:
    board: List[List[str]]
    player_symbol: str
    ai_symbol: str
    turn: str
    finished: bool = False
    result: Optional[str] = None


@dataclass
class YahtzeeSession:
    dice: List[int]
    rolls_left: int
    round_number: int
    scorecard: Dict[str, Optional[int]]
    finished: bool = False


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
        stats: Optional[Any] = None,
    ) -> None:
        self.clean_log = clean_log
        self.ai_log = ai_log
        self.ollama_url = ollama_url
        self.choose_model = choose_model
        self.choose_timeout = choose_timeout
        self.wordladder_model = wordladder_model or choose_model
        self.stats = stats

        self.hangman_sessions: Dict[str, HangmanSession] = {}
        self.wordle_sessions: Dict[str, WordleSession] = {}
        self.choose_sessions: Dict[str, ChooseSession] = {}
        self.wordladder_sessions: Dict[str, WordLadderSession] = {}
        self.cipher_sessions: Dict[str, CipherSession] = {}
        self.quiz_sessions: Dict[str, QuizBattleSession] = {}
        self.quiz_scores: Dict[str, int] = {}
        self.morse_sessions: Dict[str, MorseSession] = {}
        self.chess_sessions: Dict[str, ChessSession] = {}
        self.blackjack_sessions: Dict[str, BlackjackSession] = {}
        self.checkers_sessions: Dict[str, CheckersSession] = {}
        self.tictactoe_sessions: Dict[str, TicTacToeSession] = {}
        self.yahtzee_sessions: Dict[str, YahtzeeSession] = {}

        self.wordle_state_path = WORDLE_STATE_PATH
        self.wordle_daily_state: Dict[str, Any] = self._load_wordle_state()
        self._ensure_wordle_daily_record()

    def _record_game_play(self, game_type: str) -> None:
        if self.stats:
            try:
                self.stats.record_game(game_type)
            except Exception:
                pass

    # ------------------------
    # Wordle persistence helpers
    # ------------------------

    def _load_wordle_state(self) -> Dict[str, Any]:
        path = self.wordle_state_path
        try:
            if path.is_file():
                with path.open("r", encoding="utf-8") as fp:
                    data = json.load(fp)
                    if isinstance(data, dict):
                        return data
        except Exception:
            self.clean_log("Failed to load wordle state; starting fresh", "⚠️")
        return {}

    def _save_wordle_state(self) -> None:
        path = self.wordle_state_path
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("w", encoding="utf-8") as fp:
                json.dump(self.wordle_daily_state, fp, indent=2, sort_keys=True)
        except Exception as exc:
            self.clean_log(f"Wordle state save failed: {exc}", "⚠️")

    def _wordle_day_key(self, now: Optional[datetime] = None) -> str:
        now = (now or datetime.now().astimezone())
        return (now - timedelta(hours=7)).date().isoformat()

    def _select_wordle_target(self, day_key: str) -> str:
        chooser = random.Random(day_key)
        if not WORDLE_WORDS:
            return "radio"
        return chooser.choice(WORDLE_WORDS)

    def _ensure_wordle_daily_record(self) -> Dict[str, Any]:
        key = self._wordle_day_key()
        current = self.wordle_daily_state.get("current") if isinstance(self.wordle_daily_state, dict) else None
        if not current or current.get("day_key") != key:
            if isinstance(self.wordle_daily_state, dict):
                history = self.wordle_daily_state.get("history")
                if not isinstance(history, list):
                    history = []
                if current:
                    history.append(current)
                    if len(history) > 30:
                        history = history[-30:]
                self.wordle_daily_state["history"] = history
            else:
                self.wordle_daily_state = {}
            record = {
                "day_key": key,
                "target": self._select_wordle_target(key),
                "winners": [],
                "completed": [],
            }
            if not isinstance(self.wordle_daily_state, dict):
                self.wordle_daily_state = {}
            self.wordle_daily_state["current"] = record
            self.wordle_sessions.clear()
            self._save_wordle_state()
            return record
        # normalize keys
        current.setdefault("winners", [])
        current.setdefault("completed", [])
        return current

    # ------------------------
    # Shared stats utilities
    # ------------------------

    def _recent_game_count(self, game_type: str) -> Optional[int]:
        if not self.stats:
            return None
        try:
            snapshot = self.stats.snapshot()
        except Exception:
            return None
        breakdown = snapshot.get('games_breakdown') if isinstance(snapshot, dict) else None
        if not isinstance(breakdown, dict):
            return None
        value = breakdown.get(game_type)
        if value is None:
            return 0
        try:
            return int(value)
        except Exception:
            return None

    def _basic_game_stats(self, game_type: str, title: str) -> List[str]:
        lines = [f"📊 {title} stats (last 24h)"]
        plays = self._recent_game_count(game_type)
        if plays is None:
            lines.append("Stats unavailable right now.")
        else:
            lines.append(f"Sessions started: {plays}")
        return lines

    def _wordle_has_completed(self, record: Dict[str, Any], sender_key: str) -> bool:
        completed = record.get("completed", [])
        return sender_key in completed if isinstance(completed, list) else False

    def _wordle_has_win(self, record: Dict[str, Any], sender_key: str) -> bool:
        winners = record.get("winners", [])
        if not isinstance(winners, list):
            return False
        return any(entry.get("sender_key") == sender_key for entry in winners if isinstance(entry, dict))

    def _mark_wordle_done(self, sender_key: str, record: Optional[Dict[str, Any]] = None) -> None:
        record = record or self._ensure_wordle_daily_record()
        completed = record.setdefault("completed", [])
        if sender_key not in completed:
            completed.append(sender_key)
            self._save_wordle_state()

    def _record_wordle_win(self, sender_key: str, sender_short: str, record: Optional[Dict[str, Any]] = None) -> int:
        record = record or self._ensure_wordle_daily_record()
        winners = record.setdefault("winners", [])
        for idx, entry in enumerate(winners, start=1):
            if entry.get("sender_key") == sender_key:
                return idx
        entry = {
            "sender_key": sender_key,
            "player": sender_short or sender_key,
            "time": datetime.now().astimezone().isoformat(),
        }
        winners.append(entry)
        self._mark_wordle_done(sender_key, record)
        self._save_wordle_state()
        return len(winners)

    def _wordle_next_reset_datetime(self, now: Optional[datetime] = None) -> datetime:
        now = (now or datetime.now().astimezone())
        base = now - timedelta(hours=7)
        next_day = base.date() + timedelta(days=1)
        return datetime.combine(next_day, time(hour=7), tzinfo=now.tzinfo)

    def _wordle_next_reset_label(self) -> str:
        nxt = self._wordle_next_reset_datetime()
        try:
            return nxt.strftime("%b %d · %I:%M %p")
        except Exception:
            return nxt.isoformat()

    def _wordle_day_label(self, record: Dict[str, Any]) -> str:
        key = record.get("day_key")
        if not key:
            return "today"
        try:
            day_obj = datetime.fromisoformat(key)
        except ValueError:
            try:
                day_obj = datetime.strptime(key, "%Y-%m-%d")
            except Exception:
                return key
        return day_obj.strftime("%b %d")

    def _wordle_stats_lines(self) -> List[str]:
        record = self._ensure_wordle_daily_record()
        lines = [f"📊 Wordle stats — {self._wordle_day_label(record)}"]
        lines.append(f"Next puzzle unlocks at {self._wordle_next_reset_label()}")
        winners = record.get("winners", [])
        if winners:
            lines.append("Today's solvers:")
            for idx, entry in enumerate(winners, start=1):
                label = entry.get("player") or entry.get("sender_key") or "Unknown"
                stamp = entry.get("time")
                if stamp:
                    try:
                        dt = datetime.fromisoformat(stamp)
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=datetime.now().astimezone().tzinfo)
                        stamp = dt.astimezone().strftime("%H:%M")
                    except Exception:
                        pass
                    lines.append(f"{idx}. {label} — {stamp}")
                else:
                    lines.append(f"{idx}. {label}")
        else:
            lines.append("No correct solves yet. Be the first to crack it!")
        plays = self._recent_game_count('wordle')
        if plays is not None:
            lines.append(f"Wordle sessions started (24h): {plays}")
        return lines

    def _ordinal(self, value: int) -> str:
        if 10 <= value % 100 <= 20:
            suffix = 'th'
        else:
            suffix = {1: 'st', 2: 'nd', 3: 'rd'}.get(value % 10, 'th')
        return f"{value}{suffix}"

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
        if cmd == "/blackjack":
            return self._handle_blackjack(sender_key, sender_short, args)
        if cmd == "/yahtzee":
            return self._handle_yahtzee(sender_key, sender_short, args)
        if cmd == "/hangman":
            return self._handle_hangman(sender_key, args)
        if cmd == "/wordle":
            return self._handle_wordle(sender_key, sender_short, args)
        if cmd in {"/choose", "/adventure"}:
            return self._handle_choose(sender_key, sender_short, args, language)
        if cmd == "/wordladder":
            return self._handle_wordladder(sender_key, sender_short, args)
        if cmd == "/rps":
            if args.lower() == "stats":
                lines = self._basic_game_stats('rps', 'Rock · Paper · Scissors')
                return PendingReply(_format_lines(lines), "rps stats")
            return self._handle_rps(sender_short)
        if cmd == "/coinflip":
            if args.lower() == "stats":
                lines = self._basic_game_stats('coinflip', 'Coin Flip')
                return PendingReply(_format_lines(lines), "coinflip stats")
            return self._handle_coinflip(sender_short)
        if cmd == "/cipher":
            return self._handle_cipher(sender_key, args)
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
            "• /blackjack start — race to 21 in the camp casino",
            "• /yahtzee start — thirteen rounds of dice strategy",
            "• /hangman start — classic word rescue",
            "• /wordle start — five-letter daily challenge",
            "• /wordladder start [from] [to] — llama-guided ladder",
            "• /adventure start <theme> — llama-guided story",
            "• /rps — rock · paper · scissors with attitude",
            "• /coinflip — dramatic mesh coin toss",
            "• /cipher start — decode the field transmission",
            "• /quizbattle start — rapid-fire quiz",
            "• /morse start — decode dits and dahs",
            "",
            "Tips: use `status`, `guess`, `answer`, or `stop` with each game for more controls.",
        ]
        return PendingReply(_format_lines(lines), "games menu")

    # ------------------------
    # Chess logic
    # ------------------------
    def _handle_chess(self, sender_key: str, sender_short: str, args: str) -> PendingReply:
        arg_text = (args or "").strip()
        arg_lower = arg_text.lower()

        if arg_lower == "stats":
            lines = self._basic_game_stats('chess', 'Chess Duel')
            return PendingReply(_format_lines(lines), "chess stats")

        session = self.chess_sessions.get(sender_key)

        if not arg_text or arg_lower in {"status", "board"}:
            if not session:
                return PendingReply("`/chess start [white|black]` to begin a match.", "chess status")
            lines = self._chess_status_lines(session)
            return PendingReply(_format_lines(lines), "chess status")

        if arg_lower.startswith("start"):
            player_color = chess.WHITE
            if "black" in arg_lower:
                player_color = chess.BLACK
            elif "white" in arg_lower:
                player_color = chess.WHITE
            board = chess.Board()
            session = ChessSession(board=board, player_color=player_color, ai_color=not player_color)
            self.chess_sessions[sender_key] = session
            self._record_game_play('chess')

            lines = ["Chess duel initialized."]
            if player_color == chess.WHITE:
                lines.append("You play White. Use `/chess move e2e4` or SAN like `/chess move Nf3`.")
                lines.extend(self._chess_board_lines(board))
                lines.extend(self._chess_recent_moves(session))
                if board.is_check():
                    lines.append("Check on the board.")
                lines.append("Your move.")
            else:
                lines.append("You play Black. Mesh AI moves first.")
                lines.append("Board is shown from White's perspective.")
                ai_lines, finished = self._chess_ai_turn(session)
                lines.extend(ai_lines)
                if finished:
                    self.chess_sessions.pop(sender_key, None)
            return PendingReply(_format_lines(lines), "chess start")

        if arg_lower.startswith("move"):
            if not session:
                return PendingReply("No active chess match. `/chess start` to begin.", "chess move")
            if session.board.turn != session.player_color:
                return PendingReply("Hold up—mesh AI still has the move.", "chess move")
            parts = arg_text.split(None, 1)
            if len(parts) < 2:
                return PendingReply("Use `/chess move e2e4` or SAN like `/chess move Nf3`.", "chess move")
            move_text = parts[1].strip()
            move = self._chess_parse_move(session.board, move_text)
            if move is None:
                return PendingReply("Illegal move. Try coordinate like `e2e4` or SAN like `Nf3`.", "chess move")
            san = session.board.san(move)
            session.last_player_move = san
            session.board.push(move)
            session.move_history.append(san)
            if session.board.is_game_over():
                lines = [f"You played {san}."]
                lines.extend(self._chess_board_lines(session.board))
                lines.extend(self._chess_recent_moves(session))
                lines.extend(self._chess_completion_lines(session))
                self.chess_sessions.pop(sender_key, None)
                return PendingReply(_format_lines(lines), "chess move")
            ai_lines, finished = self._chess_ai_turn(session)
            lines = [f"You played {san}."]
            lines.extend(ai_lines)
            if finished:
                self.chess_sessions.pop(sender_key, None)
            return PendingReply(_format_lines(lines), "chess move")

        if arg_lower in {"resign", "stop", "quit"}:
            if not session:
                return PendingReply("No active chess match to close.", "chess stop")
            self.chess_sessions.pop(sender_key, None)
            return PendingReply("Chess session cleared. `/chess start` anytime for a rematch.", "chess stop")

        if arg_lower == "help":
            lines = [
                "Chess commands:",
                "  /chess start [white|black]",
                "  /chess move <algebraic>  (e.g. e2e4 or Nf3)",
                "  /chess status",
                "  /chess resign",
                "Moves accept SAN or long algebraic; promotions like e7e8q.",
            ]
            return PendingReply(_format_lines(lines), "chess help")

        return PendingReply("Commands: `start [white|black]`, `move <notation>`, `status`, `resign`, `stats`, `help`.", "chess help")

    def _chess_status_lines(self, session: ChessSession) -> List[str]:
        board = session.board
        lines = self._chess_board_lines(board)
        lines.extend(self._chess_recent_moves(session))
        if board.is_game_over():
            lines.extend(self._chess_completion_lines(session))
        else:
            if board.turn == session.player_color:
                if board.is_check():
                    lines.append("Check against you.")
                lines.append("Your move. Use `/chess move <algebraic>`.")
            else:
                if board.is_check():
                    lines.append("Check against the mesh AI.")
                lines.append("Mesh AI is thinking...")
        if session.player_color == chess.BLACK:
            lines.append("Board is shown from White's perspective.")
        if session.last_player_move:
            lines.append(f"Your last move: {session.last_player_move}.")
        if session.last_ai_move:
            lines.append(f"Mesh last move: {session.last_ai_move}.")
        return lines

    def _chess_board_lines(self, board: chess.Board) -> List[str]:
        header = "  a b c d e f g h"
        lines = [header]
        for rank in range(7, -1, -1):
            row_symbols: List[str] = []
            for file_index in range(8):
                square = chess.square(file_index, rank)
                piece = board.piece_at(square)
                row_symbols.append(piece.symbol() if piece else ".")
            lines.append(f"{rank + 1} {' '.join(row_symbols)} {rank + 1}")
        lines.append(header)
        return lines

    def _chess_recent_moves(self, session: ChessSession, max_pairs: int = 6) -> List[str]:
        moves = session.move_history
        if not moves:
            return []
        pairs: List[str] = []
        idx = 0
        move_number = 1
        while idx < len(moves):
            white_move = moves[idx] if idx < len(moves) else ""
            black_move = moves[idx + 1] if idx + 1 < len(moves) else ""
            entry = f"{move_number}. {white_move}" if white_move else f"{move_number}. ..."
            if black_move:
                entry = f"{entry} {black_move}"
            pairs.append(entry)
            idx += 2
            move_number += 1
        display_pairs = pairs[-max_pairs:]
        return ["Moves: " + " | ".join(display_pairs)]

    def _chess_ai_turn(self, session: ChessSession) -> Tuple[List[str], bool]:
        board = session.board
        if board.is_game_over():
            lines = self._chess_board_lines(board)
            lines.extend(self._chess_recent_moves(session))
            lines.extend(self._chess_completion_lines(session))
            return lines, True
        if board.turn != session.ai_color:
            lines = self._chess_board_lines(board)
            lines.extend(self._chess_recent_moves(session))
            if board.turn == session.player_color:
                if board.is_check():
                    lines.append("Check against you.")
                lines.append("Your move.")
            else:
                lines.append("Mesh AI to move.")
            return lines, False
        depth = self._chess_select_depth(board)
        best_move = self._chess_best_move(board, session.ai_color, depth)
        if best_move is None:
            lines = self._chess_board_lines(board)
            lines.extend(self._chess_recent_moves(session))
            lines.extend(self._chess_completion_lines(session))
            return lines, True
        san = board.san(best_move)
        session.last_ai_move = san
        board.push(best_move)
        session.move_history.append(san)
        lines = [f"Mesh AI played {san}."]
        lines.extend(self._chess_board_lines(board))
        lines.extend(self._chess_recent_moves(session))
        if board.is_game_over():
            lines.extend(self._chess_completion_lines(session))
            return lines, True
        if board.turn == session.player_color:
            if board.is_check():
                lines.append("Check against you.")
            lines.append("Your move.")
        else:
            lines.append("Mesh AI holds the move.")
        if session.player_color == chess.BLACK:
            lines.append("Board is shown from White's perspective.")
        return lines, False

    def _chess_completion_lines(self, session: ChessSession) -> List[str]:
        board = session.board
        if board.is_checkmate():
            # The side to move is checkmated
            if board.turn == session.player_color:
                outcome = "Checkmate — mesh AI wins."
            else:
                outcome = "Checkmate — you win!"
            lines = [outcome]
        elif board.is_stalemate():
            lines = ["Stalemate — draw."]
        elif board.is_insufficient_material():
            lines = ["Draw — insufficient material."]
        elif board.is_seventyfive_moves():
            lines = ["Draw — seventy-five move rule triggered."]
        elif board.is_fivefold_repetition():
            lines = ["Draw — fivefold repetition detected."]
        elif board.can_claim_draw():
            lines = ["Draw available by rule."]
        else:
            lines = ["Game complete."]
        result = board.result(claim_draw=True)
        if result:
            lines.append(f"Result: {result}")
        return lines

    def _chess_select_depth(self, board: chess.Board) -> int:
        piece_count = len(board.piece_map())
        if piece_count <= 10:
            return 4
        if piece_count <= 18:
            return 3
        return 3

    def _chess_is_endgame(self, board: chess.Board) -> bool:
        queens = len(board.pieces(chess.QUEEN, chess.WHITE)) + len(board.pieces(chess.QUEEN, chess.BLACK))
        heavy = sum(len(board.pieces(pt, chess.WHITE)) + len(board.pieces(pt, chess.BLACK)) for pt in (chess.ROOK, chess.BISHOP, chess.KNIGHT))
        return queens == 0 or heavy <= 6

    def _chess_static_eval(self, board: chess.Board, ai_color: chess.Color) -> int:
        material_score = 0
        endgame = self._chess_is_endgame(board)
        bishop_pair_bonus = 30
        white_bishops = len(board.pieces(chess.BISHOP, chess.WHITE))
        black_bishops = len(board.pieces(chess.BISHOP, chess.BLACK))
        for square, piece in board.piece_map().items():
            piece_value = CHESS_PIECE_VALUES.get(piece.piece_type, 0)
            if piece.piece_type == chess.KING:
                table = CHESS_PST["king_end"] if endgame else CHESS_PST["king_mid"]
            else:
                table = CHESS_PST.get(piece.piece_type)
            pst_bonus = 0
            if table is not None:
                index = square if piece.color == chess.WHITE else chess.square_mirror(square)
                pst_bonus = table[index]
            total_piece_value = piece_value + pst_bonus
            if piece.color == chess.WHITE:
                material_score += total_piece_value
            else:
                material_score -= total_piece_value

        if white_bishops >= 2:
            material_score += bishop_pair_bonus
        if black_bishops >= 2:
            material_score -= bishop_pair_bonus

        return material_score if ai_color == chess.WHITE else -material_score

    def _chess_terminal_eval(self, board: chess.Board, ai_color: chess.Color, color_sign: int, depth: int) -> int:
        if board.is_checkmate():
            return -color_sign * (CHESS_MATE_SCORE - depth)
        if board.is_stalemate() or board.is_insufficient_material() or board.is_seventyfive_moves() or board.is_fivefold_repetition() or board.can_claim_draw():
            return 0
        return color_sign * self._chess_static_eval(board, ai_color)

    def _chess_negamax(self, board: chess.Board, depth: int, alpha: int, beta: int, ai_color: chess.Color) -> int:
        color_sign = 1 if board.turn == ai_color else -1
        if depth == 0 or board.is_game_over():
            return self._chess_terminal_eval(board, ai_color, color_sign, depth)
        best_value = -CHESS_MATE_SCORE
        for move in self._chess_order_moves(board):
            board.push(move)
            value = -self._chess_negamax(board, depth - 1, -beta, -alpha, ai_color)
            board.pop()
            if value > best_value:
                best_value = value
            if best_value > alpha:
                alpha = best_value
            if alpha >= beta:
                break
        return best_value

    def _chess_best_move(self, board: chess.Board, ai_color: chess.Color, depth: int) -> Optional[chess.Move]:
        best_score = -CHESS_MATE_SCORE
        best_move: Optional[chess.Move] = None
        alpha, beta = -CHESS_MATE_SCORE, CHESS_MATE_SCORE
        for move in self._chess_order_moves(board):
            board.push(move)
            score = -self._chess_negamax(board, depth - 1, -beta, -alpha, ai_color)
            board.pop()
            if score > best_score:
                best_score = score
                best_move = move
            if score > alpha:
                alpha = score
        return best_move

    def _chess_order_moves(self, board: chess.Board) -> List[chess.Move]:
        moves = list(board.legal_moves)

        def score(move: chess.Move) -> int:
            value = 0
            if board.is_capture(move):
                captured = board.piece_at(move.to_square)
                mover = board.piece_at(move.from_square)
                if captured:
                    value += 10 * CHESS_PIECE_VALUES.get(captured.piece_type, 0)
                if mover:
                    value -= CHESS_PIECE_VALUES.get(mover.piece_type, 0)
            if board.gives_check(move):
                value += 50
            if move.promotion:
                value += 80
            return -value

        moves.sort(key=score)
        return moves

    def _chess_parse_move(self, board: chess.Board, text: str) -> Optional[chess.Move]:
        cleaned = (text or "").strip()
        if not cleaned:
            return None
        try:
            move = board.parse_san(cleaned)
            if move in board.legal_moves:
                return move
        except ValueError:
            pass
        try:
            move = chess.Move.from_uci(cleaned.lower())
            if move in board.legal_moves:
                return move
        except ValueError:
            return None
        return None

    # ------------------------
    # Blackjack logic
    # ------------------------
    def _handle_blackjack(self, sender_key: str, sender_short: str, args: str) -> PendingReply:
        arg_text = (args or "").strip()
        arg_lower = arg_text.lower()

        if arg_lower == "stats":
            lines = self._basic_game_stats('blackjack', 'Blackjack Camp')
            return PendingReply(_format_lines(lines), "blackjack stats")

        session = self.blackjack_sessions.get(sender_key)

        if not arg_text or arg_lower == "status":
            if not session:
                return PendingReply("`/blackjack start` to shuffle up.", "blackjack status")
            lines = self._blackjack_status_lines(session, reveal_dealer=session.finished or session.player_stood)
            return PendingReply(_format_lines(lines), "blackjack status")

        if arg_lower.startswith("start"):
            session = self._blackjack_start_session()
            self.blackjack_sessions[sender_key] = session
            self._record_game_play('blackjack')
            lines = ["Blackjack table is live."]
            lines.extend(self._blackjack_status_lines(session, reveal_dealer=False))
            if session.finished:
                lines.append(session.outcome or "Round complete.")
                self.blackjack_sessions.pop(sender_key, None)
            else:
                lines.append("Hit with `/blackjack hit` or stand with `/blackjack stand`.")
            return PendingReply(_format_lines(lines), "blackjack start")

        if arg_lower == "hit":
            if not session or session.finished:
                return PendingReply("Start a new round with `/blackjack start`.", "blackjack hit")
            session.player_hand.append(self._blackjack_draw(session.deck))
            if self._blackjack_value(session.player_hand) > 21:
                session.finished = True
                session.outcome = "Bust — mesh dealer wins."
            lines = ["You draw:", self._blackjack_render_hand(session.player_hand, reveal=True)]
            lines.extend(self._blackjack_status_lines(session, reveal_dealer=session.finished))
            if session.finished:
                if session.outcome:
                    lines.append(session.outcome)
                self.blackjack_sessions.pop(sender_key, None)
            else:
                lines.append("Hit again or `/blackjack stand`.")
            return PendingReply(_format_lines(lines), "blackjack hit")

        if arg_lower == "stand":
            if not session or session.finished:
                return PendingReply("No active round. `/blackjack start` to play.", "blackjack stand")
            session.player_stood = True
            self._blackjack_finish_round(session)
            lines = ["You stand."]
            lines.extend(self._blackjack_status_lines(session, reveal_dealer=True))
            if session.outcome:
                lines.append(session.outcome)
            self.blackjack_sessions.pop(sender_key, None)
            return PendingReply(_format_lines(lines), "blackjack stand")

        if arg_lower == "double":
            if not session or session.finished:
                return PendingReply("No active round to double.", "blackjack double")
            if len(session.player_hand) != 2:
                return PendingReply("Double down only available on the first decision.", "blackjack double")
            session.player_hand.append(self._blackjack_draw(session.deck))
            session.player_stood = True
            self._blackjack_finish_round(session)
            lines = ["Double down card:", self._blackjack_render_hand(session.player_hand, reveal=True)]
            lines.extend(self._blackjack_status_lines(session, reveal_dealer=True))
            if session.outcome:
                lines.append(session.outcome)
            self.blackjack_sessions.pop(sender_key, None)
            return PendingReply(_format_lines(lines), "blackjack double")

        if arg_lower in {"stop", "quit"}:
            if not session:
                return PendingReply("No blackjack round to clear.", "blackjack stop")
            self.blackjack_sessions.pop(sender_key, None)
            return PendingReply("Blackjack table cleared.", "blackjack stop")

        if arg_lower == "help":
            lines = [
                "Blackjack commands:",
                "  /blackjack start",
                "  /blackjack hit",
                "  /blackjack stand",
                "  /blackjack double",
                "  /blackjack status",
            ]
            return PendingReply(_format_lines(lines), "blackjack help")

        return PendingReply("Commands: `start`, `hit`, `stand`, `double`, `status`, `stop`, `stats`, `help`.", "blackjack help")

    def _blackjack_start_session(self) -> BlackjackSession:
        deck = [f"{rank}{suit}" for suit in BLACKJACK_SUITS for rank in BLACKJACK_RANKS]
        random.shuffle(deck)
        player = [self._blackjack_draw(deck), self._blackjack_draw(deck)]
        dealer = [self._blackjack_draw(deck), self._blackjack_draw(deck)]
        session = BlackjackSession(deck=deck, player_hand=player, dealer_hand=dealer)
        player_value = self._blackjack_value(player)
        dealer_value = self._blackjack_value(dealer)
        if player_value == 21 and dealer_value == 21:
            session.finished = True
            session.outcome = "Push — both hit blackjack."
        elif player_value == 21:
            session.finished = True
            session.outcome = "Blackjack! You win."
        elif dealer_value == 21:
            session.finished = True
            session.outcome = "Dealer blackjack — mesh wins."
        return session

    def _blackjack_finish_round(self, session: BlackjackSession) -> None:
        while not session.finished and self._blackjack_value(session.dealer_hand) < 17:
            session.dealer_hand.append(self._blackjack_draw(session.deck))
        player_total = self._blackjack_value(session.player_hand)
        dealer_total = self._blackjack_value(session.dealer_hand)
        if player_total > 21:
            session.outcome = "Bust — mesh dealer wins."
        elif dealer_total > 21:
            session.outcome = "Dealer busts — you win."
        elif player_total > dealer_total:
            session.outcome = "You win the hand."
        elif player_total < dealer_total:
            session.outcome = "Mesh dealer wins."
        else:
            session.outcome = "Push — tie game."
        session.finished = True

    def _blackjack_status_lines(self, session: BlackjackSession, *, reveal_dealer: bool) -> List[str]:
        lines = [
            f"Your hand ({self._blackjack_value(session.player_hand)}):",
            self._blackjack_render_hand(session.player_hand, reveal=True),
        ]
        if reveal_dealer:
            dealer_value = self._blackjack_value(session.dealer_hand)
            lines.append(f"Dealer hand ({dealer_value}):")
            lines.append(self._blackjack_render_hand(session.dealer_hand, reveal=True))
        else:
            hidden = session.dealer_hand[:1] + ["??"]
            lines.append("Dealer shows:")
            lines.append(self._blackjack_render_hand(hidden, reveal=False))
        return lines

    def _blackjack_draw(self, deck: List[str]) -> str:
        if not deck:
            deck.extend([f"{rank}{suit}" for suit in BLACKJACK_SUITS for rank in BLACKJACK_RANKS])
            random.shuffle(deck)
        return deck.pop()

    def _blackjack_value(self, hand: List[str]) -> int:
        total = 0
        aces = 0
        for card in hand:
            rank = card[:-1] if len(card) > 2 else card[0]
            if rank == "A":
                total += 11
                aces += 1
            elif rank in {"K", "Q", "J"}:
                total += 10
            else:
                total += int(rank)
        while total > 21 and aces:
            total -= 10
            aces -= 1
        return total

    def _blackjack_render_hand(self, hand: List[str], *, reveal: bool) -> str:
        if not hand:
            return "(empty)"
        return " ".join(hand if reveal else [card if card != "??" else "??" for card in hand])

    # ------------------------
    # Checkers logic
    # ------------------------
    def _handle_checkers(self, sender_key: str, sender_short: str, args: str) -> PendingReply:
        arg_text = (args or "").strip()
        arg_lower = arg_text.lower()

        if arg_lower == "stats":
            lines = self._basic_game_stats('checkers', 'Checkers')
            return PendingReply(_format_lines(lines), "checkers stats")

        session = self.checkers_sessions.get(sender_key)

        if not arg_text or arg_lower in {"status", "board"}:
            if not session:
                return PendingReply("`/checkers start [white|black] [second]` to begin a match.", "checkers status")
            lines = self._checkers_status_lines(session)
            return PendingReply(_format_lines(lines), "checkers status")

        if arg_lower.startswith("start"):
            player_color = 'w'
            if "black" in arg_lower:
                player_color = 'b'
            elif "white" in arg_lower:
                player_color = 'w'
            ai_color = 'b' if player_color == 'w' else 'w'
            board = self._checkers_initial_board()
            turn = player_color
            if "second" in arg_lower or "ai" in arg_lower:
                turn = ai_color
            session = CheckersSession(board=board, player_color=player_color, ai_color=ai_color, turn=turn)
            self.checkers_sessions[sender_key] = session
            self._record_game_play('checkers')

            lines = ["Checkers match ready."]
            color_label = "White" if player_color == 'w' else "Black"
            lines.append(f"You control {color_label} pieces.")
            lines.append("Moves use coordinates like `/checkers move b6 d5` or `/checkers move c3-e5`. Captures must include every landing square.")
            if session.turn == session.player_color:
                lines.append("Your move first.")
                lines.extend(self._checkers_board_lines(board))
            else:
                lines.append("Mesh AI moves first.")
                ai_lines, finished = self._checkers_ai_turn(session)
                lines.extend(ai_lines)
                if finished:
                    self.checkers_sessions.pop(sender_key, None)
            return PendingReply(_format_lines(lines), "checkers start")

        if arg_lower.startswith("move"):
            if not session:
                return PendingReply("No active checkers match. `/checkers start` to begin.", "checkers move")
            if session.turn != session.player_color:
                return PendingReply("Hold tight—the mesh AI still has the move.", "checkers move")
            parts = arg_text.split(None, 1)
            if len(parts) < 2:
                return PendingReply("Use `/checkers move b6 d5` or `/checkers move b6-d5-f7` for captures.", "checkers move")
            path = self._checkers_parse_move(parts[1])
            if not path:
                return PendingReply("Could not parse that move. Use coordinates like `b6 d5`.", "checkers move")
            legal_moves = self._checkers_generate_moves(session.board, session.player_color)
            legal_paths = {tuple(move): move for move in legal_moves}
            key = tuple(path)
            if key not in legal_paths:
                tips = "Captures are mandatory" if any(self._checkers_move_is_capture(m) for m in legal_moves) else ""
                message = "Move not legal."
                if tips:
                    message = f"{message} {tips}"
                return PendingReply(message.strip(), "checkers move")
            move = legal_paths[key]
            san = self._checkers_notation(move)
            session.board = self._checkers_apply_move(session.board, move, session.player_color)
            session.move_history.append(f"{san}")
            session.last_player_move = san
            session.turn = session.ai_color
            lines = [f"You played {san}."]
            if self._checkers_is_game_over(session):
                lines.extend(self._checkers_board_lines(session.board))
                lines.extend(self._checkers_result_lines(session))
                self.checkers_sessions.pop(sender_key, None)
                return PendingReply(_format_lines(lines), "checkers move")
            ai_lines, finished = self._checkers_ai_turn(session)
            lines.extend(ai_lines)
            if finished:
                self.checkers_sessions.pop(sender_key, None)
            return PendingReply(_format_lines(lines), "checkers move")

        if arg_lower in {"resign", "stop", "quit"}:
            if not session:
                return PendingReply("No active checkers match to close.", "checkers stop")
            self.checkers_sessions.pop(sender_key, None)
            return PendingReply("Checkers session cleared. `/checkers start` for a rematch.", "checkers stop")

        if arg_lower == "help":
            lines = [
                "Checkers commands:",
                "  /checkers start [white|black] [second]",
                "  /checkers move <path>  (e.g. b6 d5 or b6-d5-f7)",
                "  /checkers status",
                "  /checkers resign",
                "Captures are mandatory. Include every landing square in multi-jumps.",
            ]
            return PendingReply(_format_lines(lines), "checkers help")

        return PendingReply("Commands: `start [white|black] [second]`, `move <path>`, `status`, `resign`, `stats`, `help`.", "checkers help")

    def _checkers_initial_board(self) -> List[List[str]]:
        board = [["." for _ in range(8)] for _ in range(8)]
        for row in range(3):
            for col in range(8):
                if (row + col) % 2 == 1:
                    board[row][col] = 'b'
        for row in range(5, 8):
            for col in range(8):
                if (row + col) % 2 == 1:
                    board[row][col] = 'w'
        return board

    def _checkers_status_lines(self, session: CheckersSession) -> List[str]:
        lines = self._checkers_board_lines(session.board)
        if session.move_history:
            moves = " | ".join(session.move_history[-8:])
            lines.append(f"Moves: {moves}")
        if session.turn == session.player_color:
            lines.append("Your move. Use `/checkers move <path>`.")
        else:
            lines.append("Mesh AI is thinking...")
        if session.last_player_move:
            lines.append(f"Your last move: {session.last_player_move}.")
        if session.last_ai_move:
            lines.append(f"Mesh last move: {session.last_ai_move}.")
        lines.append("Board shows White at the bottom.")
        return lines

    def _checkers_board_lines(self, board: List[List[str]]) -> List[str]:
        header = "  " + " ".join(CHECKERS_FILES)
        lines = [header]
        for display_rank in range(8, 0, -1):
            row = 8 - display_rank
            pieces = []
            for col in range(8):
                val = board[row][col]
                pieces.append(val if val != "." else ".")
            lines.append(f"{display_rank} {' '.join(pieces)} {display_rank}")
        lines.append(header)
        return lines

    def _checkers_ai_turn(self, session: CheckersSession) -> Tuple[List[str], bool]:
        board = session.board
        if self._checkers_is_game_over(session):
            lines = self._checkers_board_lines(board)
            lines.extend(self._checkers_result_lines(session))
            return lines, True
        if session.turn != session.ai_color:
            lines = self._checkers_board_lines(board)
            lines.append("Your move.")
            return lines, False
        pieces = sum(1 for row in board for cell in row if cell in {'w', 'b', 'W', 'B'})
        depth = CHECKERS_DEPTH_EARLY if pieces > 10 else CHECKERS_DEPTH_LATE
        move = self._checkers_best_move(board, session.ai_color, depth)
        if move is None:
            session.turn = session.player_color
            lines = self._checkers_board_lines(board)
            lines.extend(self._checkers_result_lines(session))
            return lines, True
        san = self._checkers_notation(move)
        board = self._checkers_apply_move(board, move, session.ai_color)
        session.board = board
        session.move_history.append(san)
        session.last_ai_move = san
        session.turn = session.player_color
        lines = [f"Mesh AI played {san}."]
        lines.extend(self._checkers_board_lines(board))
        if self._checkers_is_game_over(session):
            lines.extend(self._checkers_result_lines(session))
            return lines, True
        lines.append("Your move.")
        return lines, False

    def _checkers_is_game_over(self, session: CheckersSession) -> bool:
        board = session.board
        player_moves = self._checkers_generate_moves(board, session.player_color)
        ai_moves = self._checkers_generate_moves(board, session.ai_color)
        player_pieces = any(cell.lower() == session.player_color for row in board for cell in row if cell != ".")
        ai_pieces = any(cell.lower() == session.ai_color for row in board for cell in row if cell != ".")
        if not player_pieces or not player_moves or not ai_pieces or not ai_moves:
            return True
        return False

    def _checkers_result_lines(self, session: CheckersSession) -> List[str]:
        board = session.board
        player_moves = self._checkers_generate_moves(board, session.player_color)
        ai_moves = self._checkers_generate_moves(board, session.ai_color)
        player_pieces = any(cell.lower() == session.player_color for row in board for cell in row if cell != ".")
        ai_pieces = any(cell.lower() == session.ai_color for row in board for cell in row if cell != ".")
        if not ai_pieces or not ai_moves:
            return ["You win! Mesh AI is out of moves."]
        if not player_pieces or not player_moves:
            return ["Mesh AI wins — no remaining moves."]
        return ["Drawn position."]

    def _checkers_parse_move(self, move_text: str) -> Optional[List[Tuple[int, int]]]:
        normalized = move_text.replace('x', ' ').replace('-', ' ').replace('>', ' ')
        parts = [chunk for chunk in normalized.split() if chunk]
        result: List[Tuple[int, int]] = []
        for token in parts:
            if len(token) != 2:
                return None
            file_char, rank_char = token[0].lower(), token[1]
            if file_char not in CHECKERS_FILES or rank_char not in CHECKERS_RANKS:
                return None
            col = CHECKERS_FILES.index(file_char)
            row = 8 - int(rank_char)
            result.append((row, col))
        return result if len(result) >= 2 else None

    def _checkers_notation(self, move: List[Tuple[int, int]]) -> str:
        symbols = []
        capture = self._checkers_move_is_capture(move)
        for row, col in move:
            file_char = CHECKERS_FILES[col]
            rank_char = str(8 - row)
            symbols.append(f"{file_char}{rank_char}")
        sep = 'x' if capture else '-'
        return sep.join(symbols)

    def _checkers_move_is_capture(self, move: List[Tuple[int, int]]) -> bool:
        for (r1, c1), (r2, c2) in zip(move, move[1:]):
            if abs(r1 - r2) == 2 and abs(c1 - c2) == 2:
                return True
        return False

    def _checkers_generate_moves(self, board: List[List[str]], color: str) -> List[List[Tuple[int, int]]]:
        captures: List[List[Tuple[int, int]]] = []
        moves: List[List[Tuple[int, int]]] = []
        for row in range(8):
            for col in range(8):
                piece = board[row][col]
                if piece == "." or piece.lower() != color:
                    continue
                capture_paths = self._checkers_captures_from(board, row, col, piece)
                if capture_paths:
                    captures.extend(capture_paths)
                elif not captures:
                    moves.extend(self._checkers_simple_moves(board, row, col, piece))
        return captures if captures else moves

    def _checkers_captures_from(self, board: List[List[str]], row: int, col: int, piece: str) -> List[List[Tuple[int, int]]]:
        color = piece.lower()
        directions = CHECKERS_KING_DIRECTIONS if piece.isupper() else CHECKERS_DIRECTIONS[color]
        results: List[List[Tuple[int, int]]] = []
        for dr, dc in directions:
            mid_r, mid_c = row + dr, col + dc
            land_r, land_c = row + 2 * dr, col + 2 * dc
            if not self._checkers_in_bounds(land_r, land_c) or not self._checkers_in_bounds(mid_r, mid_c):
                continue
            mid_piece = board[mid_r][mid_c]
            if mid_piece == "." or mid_piece.lower() == color:
                continue
            if board[land_r][land_c] != ".":
                continue
            clone = self._checkers_clone_board(board)
            clone[row][col] = "."
            clone[mid_r][mid_c] = "."
            promoted_piece = piece
            if not piece.isupper() and land_r == CHECKERS_PROMOTION_ROW[color]:
                promoted_piece = piece.upper()
            clone[land_r][land_c] = promoted_piece
            further = []
            if promoted_piece == piece:
                further = self._checkers_captures_from(clone, land_r, land_c, promoted_piece)
            if further:
                for continuation in further:
                    results.append([(row, col)] + continuation)
            else:
                results.append([(row, col), (land_r, land_c)])
        return results

    def _checkers_simple_moves(self, board: List[List[str]], row: int, col: int, piece: str) -> List[List[Tuple[int, int]]]:
        color = piece.lower()
        directions = CHECKERS_KING_DIRECTIONS if piece.isupper() else CHECKERS_DIRECTIONS[color]
        moves: List[List[Tuple[int, int]]] = []
        for dr, dc in directions:
            dest_r, dest_c = row + dr, col + dc
            if not self._checkers_in_bounds(dest_r, dest_c):
                continue
            if board[dest_r][dest_c] != ".":
                continue
            moves.append([(row, col), (dest_r, dest_c)])
        return moves

    def _checkers_apply_move(self, board: List[List[str]], move: List[Tuple[int, int]], color: str) -> List[List[str]]:
        clone = self._checkers_clone_board(board)
        start_r, start_c = move[0]
        piece = clone[start_r][start_c]
        clone[start_r][start_c] = "."
        for (r1, c1), (r2, c2) in zip(move, move[1:]):
            if abs(r1 - r2) == 2:
                mid_r, mid_c = (r1 + r2) // 2, (c1 + c2) // 2
                clone[mid_r][mid_c] = "."
        end_r, end_c = move[-1]
        if not piece.isupper() and end_r == CHECKERS_PROMOTION_ROW[color]:
            piece = piece.upper()
        clone[end_r][end_c] = piece
        return clone

    def _checkers_clone_board(self, board: List[List[str]]) -> List[List[str]]:
        return [row[:] for row in board]

    def _checkers_in_bounds(self, row: int, col: int) -> bool:
        return 0 <= row < 8 and 0 <= col < 8

    def _checkers_opponent(self, color: str) -> str:
        return 'b' if color == 'w' else 'w'

    def _checkers_evaluate(self, board: List[List[str]], color: str) -> int:
        score = 0
        for row in range(8):
            for col in range(8):
                cell = board[row][col]
                if cell == ".":
                    continue
                value = CHECKERS_KING_VALUE if cell.isupper() else CHECKERS_MAN_VALUE
                advancement = 0
                if cell.islower():
                    progression = (7 - row) if cell.lower() == 'w' else row
                    advancement = progression * 5
                if cell.lower() == color:
                    score += value + advancement
                else:
                    score -= value + advancement
        return score

    def _checkers_best_move(self, board: List[List[str]], color: str, depth: int) -> Optional[List[Tuple[int, int]]]:
        legal_moves = self._checkers_generate_moves(board, color)
        if not legal_moves:
            return None
        alpha = -10_000_000
        beta = 10_000_000
        best_score = -10_000_000
        best_move = legal_moves[0]
        for move in legal_moves:
            child = self._checkers_apply_move(board, move, color)
            score = -self._checkers_search(child, self._checkers_opponent(color), depth - 1, -beta, -alpha, color)
            if score > best_score:
                best_score = score
                best_move = move
            if score > alpha:
                alpha = score
        return best_move

    def _checkers_search(self, board: List[List[str]], turn: str, depth: int, alpha: int, beta: int, root_color: str) -> int:
        legal_moves = self._checkers_generate_moves(board, turn)
        if depth == 0 or not legal_moves:
            if not legal_moves:
                if turn == root_color:
                    return -50_000
                return 50_000
            return self._checkers_evaluate(board, root_color)
        best_value = -10_000_000
        for move in legal_moves:
            child = self._checkers_apply_move(board, move, turn)
            score = -self._checkers_search(child, self._checkers_opponent(turn), depth - 1, -beta, -alpha, root_color)
            if score > best_value:
                best_value = score
            if best_value > alpha:
                alpha = best_value
            if alpha >= beta:
                break
        return best_value

    # ------------------------
    # Tic-Tac-Toe logic
    # ------------------------
    def _handle_tictactoe(self, sender_key: str, sender_short: str, args: str) -> PendingReply:
        arg_text = (args or "").strip()
        arg_lower = arg_text.lower()

        if arg_lower == "stats":
            lines = self._basic_game_stats('tictactoe', 'Tic-Tac-Toe')
            return PendingReply(_format_lines(lines), "tictactoe stats")

        session = self.tictactoe_sessions.get(sender_key)

        if not arg_text or arg_lower == "status":
            if not session:
                return PendingReply("`/tictactoe start [second|o]` to open a grid.", "tictactoe status")
            lines = self._tictactoe_status_lines(session)
            return PendingReply(_format_lines(lines), "tictactoe status")

        if arg_lower.startswith("start"):
            player_symbol = 'X'
            if " o" in f" {arg_lower}" or arg_lower.endswith(" o") or " o " in arg_lower:
                player_symbol = 'O'
            ai_symbol = 'O' if player_symbol == 'X' else 'X'
            turn = player_symbol
            if "second" in arg_lower or "ai" in arg_lower:
                turn = ai_symbol
            board = [[" " for _ in range(3)] for _ in range(3)]
            session = TicTacToeSession(board=board, player_symbol=player_symbol, ai_symbol=ai_symbol, turn=turn)
            self.tictactoe_sessions[sender_key] = session
            self._record_game_play('tictactoe')

            lines = ["Tic-Tac-Toe grid ready."]
            lines.append(f"You are {player_symbol}. Coordinates use `/tictactoe move b2`.")
            if session.turn == session.player_symbol:
                lines.extend(self._tictactoe_board_lines(session.board))
                lines.append("Your move first.")
            else:
                lines.append("Mesh AI moves first.")
                ai_lines, finished = self._tictactoe_ai_turn(session)
                lines.extend(ai_lines)
                if finished:
                    self.tictactoe_sessions.pop(sender_key, None)
            return PendingReply(_format_lines(lines), "tictactoe start")

        if arg_lower.startswith("move"):
            if not session:
                return PendingReply("No active game. `/tictactoe start` to begin.", "tictactoe move")
            if session.finished:
                self.tictactoe_sessions.pop(sender_key, None)
                return PendingReply("That round is already over. `/tictactoe start` to play again.", "tictactoe move")
            if session.turn != session.player_symbol:
                return PendingReply("Mesh AI holds the turn right now.", "tictactoe move")
            parts = arg_text.split(None, 1)
            if len(parts) < 2:
                return PendingReply("Use `/tictactoe move b2`.", "tictactoe move")
            square = self._tictactoe_parse_square(parts[1])
            if square is None:
                return PendingReply("Could not read that square. Try letters a-c with numbers 1-3.", "tictactoe move")
            row, col = square
            if session.board[row][col] != " ":
                return PendingReply("That square is occupied. Pick another.", "tictactoe move")
            session.board[row][col] = session.player_symbol
            winner = self._tictactoe_check_winner(session.board)
            if winner or self._tictactoe_is_full(session.board):
                session.finished = True
                session.result = self._tictactoe_result_label(winner, session)
                lines = self._tictactoe_board_lines(session.board)
                if session.result:
                    lines.append(session.result)
                self.tictactoe_sessions.pop(sender_key, None)
                return PendingReply(_format_lines(lines), "tictactoe move")
            session.turn = session.ai_symbol
            ai_lines, finished = self._tictactoe_ai_turn(session)
            if finished:
                self.tictactoe_sessions.pop(sender_key, None)
            return PendingReply(_format_lines(ai_lines), "tictactoe move")

        if arg_lower in {"stop", "quit"}:
            if not session:
                return PendingReply("No Tic-Tac-Toe grid to clear.", "tictactoe stop")
            self.tictactoe_sessions.pop(sender_key, None)
            return PendingReply("Tic-Tac-Toe session cleared.", "tictactoe stop")

        if arg_lower == "help":
            lines = [
                "Tic-Tac-Toe commands:",
                "  /tictactoe start [second|o]",
                "  /tictactoe move <square>",
                "  /tictactoe status",
                "  /tictactoe stop",
            ]
            return PendingReply(_format_lines(lines), "tictactoe help")

        return PendingReply("Commands: `start`, `move <square>`, `status`, `stop`, `stats`, `help`.", "tictactoe help")

    def _tictactoe_status_lines(self, session: TicTacToeSession) -> List[str]:
        lines = self._tictactoe_board_lines(session.board)
        if session.finished and session.result:
            lines.append(session.result)
        else:
            if session.turn == session.player_symbol:
                lines.append("Your move.")
            else:
                lines.append("Mesh AI thinking...")
        return lines

    def _tictactoe_board_lines(self, board: List[List[str]]) -> List[str]:
        header = "  " + " ".join(TICTACTOE_FILES)
        lines = [header]
        for display_rank in range(3, 0, -1):
            row = 3 - display_rank
            symbols = []
            for col in range(3):
                cell = board[row][col]
                symbols.append(cell if cell != " " else ".")
            lines.append(f"{display_rank} {' '.join(symbols)} {display_rank}")
        lines.append(header)
        return lines

    def _tictactoe_ai_turn(self, session: TicTacToeSession) -> Tuple[List[str], bool]:
        board = session.board
        if session.finished:
            return self._tictactoe_board_lines(board) + ([session.result] if session.result else []), True
        if session.turn != session.ai_symbol:
            lines = self._tictactoe_board_lines(board)
            lines.append("Your move.")
            return lines, False
        move = self._tictactoe_best_move(board, session.ai_symbol, session.player_symbol)
        if move is None:
            session.finished = True
            session.result = "Draw."
            lines = self._tictactoe_board_lines(board)
            lines.append(session.result)
            return lines, True
        row, col = move
        board[row][col] = session.ai_symbol
        winner = self._tictactoe_check_winner(board)
        if winner or self._tictactoe_is_full(board):
            session.finished = True
            session.result = self._tictactoe_result_label(winner, session)
            lines = self._tictactoe_board_lines(board)
            if session.result:
                lines.append(session.result)
            return lines, True
        session.turn = session.player_symbol
        lines = self._tictactoe_board_lines(board)
        lines.append("Your move.")
        return lines, False

    def _tictactoe_result_label(self, winner: Optional[str], session: TicTacToeSession) -> Optional[str]:
        if not winner:
            return "Draw."
        if winner == session.player_symbol:
            return "You win!"
        if winner == session.ai_symbol:
            return "Mesh AI wins."
        return None

    def _tictactoe_best_move(self, board: List[List[str]], ai_symbol: str, player_symbol: str) -> Optional[Tuple[int, int]]:
        best_score = -float('inf')
        best_move: Optional[Tuple[int, int]] = None
        for row in range(3):
            for col in range(3):
                if board[row][col] != " ":
                    continue
                board[row][col] = ai_symbol
                score = self._tictactoe_minimax(board, False, ai_symbol, player_symbol, depth=1)
                board[row][col] = " "
                if score > best_score:
                    best_score = score
                    best_move = (row, col)
        return best_move

    def _tictactoe_minimax(self, board: List[List[str]], maximizing: bool, ai_symbol: str, player_symbol: str, depth: int) -> int:
        winner = self._tictactoe_check_winner(board)
        if winner == ai_symbol:
            return 10 - depth
        if winner == player_symbol:
            return depth - 10
        if self._tictactoe_is_full(board):
            return 0
        if maximizing:
            best = -float('inf')
            for row in range(3):
                for col in range(3):
                    if board[row][col] != " ":
                        continue
                    board[row][col] = ai_symbol
                    score = self._tictactoe_minimax(board, False, ai_symbol, player_symbol, depth + 1)
                    board[row][col] = " "
                    best = max(best, score)
            return best
        best = float('inf')
        for row in range(3):
            for col in range(3):
                if board[row][col] != " ":
                    continue
                board[row][col] = player_symbol
                score = self._tictactoe_minimax(board, True, ai_symbol, player_symbol, depth + 1)
                board[row][col] = " "
                best = min(best, score)
        return best

    def _tictactoe_check_winner(self, board: List[List[str]]) -> Optional[str]:
        for line in TICTACTOE_LINES:
            symbols = {board[r][c] for r, c in line}
            if len(symbols) == 1:
                symbol = symbols.pop()
                if symbol != " ":
                    return symbol
        return None

    def _tictactoe_is_full(self, board: List[List[str]]) -> bool:
        return all(cell != " " for row in board for cell in row)

    def _tictactoe_parse_square(self, text: str) -> Optional[Tuple[int, int]]:
        token = text.strip().lower()
        if len(token) != 2:
            return None
        file_char, rank_char = token[0], token[1]
        if file_char not in TICTACTOE_FILES or rank_char not in TICTACTOE_RANKS:
            return None
        col = TICTACTOE_FILES.index(file_char)
        row = 3 - int(rank_char)
        return row, col

    # ------------------------
    # Yahtzee logic
    # ------------------------
    def _handle_yahtzee(self, sender_key: str, sender_short: str, args: str) -> PendingReply:
        arg_text = (args or "").strip()
        arg_lower = arg_text.lower()

        if arg_lower == "stats":
            lines = self._basic_game_stats('yahtzee', 'Yahtzee')
            return PendingReply(_format_lines(lines), "yahtzee stats")

        session = self.yahtzee_sessions.get(sender_key)

        if not arg_text or arg_lower == "status":
            if not session:
                return PendingReply("`/yahtzee start` to roll five dice.", "yahtzee status")
            lines = self._yahtzee_status_lines(session)
            return PendingReply(_format_lines(lines), "yahtzee status")

        if arg_lower.startswith("start"):
            session = self._yahtzee_start_session()
            self.yahtzee_sessions[sender_key] = session
            self._record_game_play('yahtzee')
            lines = ["Yahtzee underway — thirteen categories to fill."]
            roll_lines = self._yahtzee_roll(session, set())
            lines.extend(roll_lines)
            lines.append("Hold dice with `/yahtzee roll 1 3` (positions 1-5) or score with `/yahtzee score <category>`.")
            return PendingReply(_format_lines(lines), "yahtzee start")

        if arg_lower.startswith("roll"):
            if not session or session.finished:
                return PendingReply("No active scorecard. `/yahtzee start` to begin.", "yahtzee roll")
            if session.rolls_left <= 0:
                return PendingReply("No rolls left. Score this turn with `/yahtzee score <category>`.", "yahtzee roll")
            holds = self._yahtzee_parse_holds(arg_text)
            if holds is None:
                return PendingReply("Use `/yahtzee roll` or `/yahtzee roll 1 3 5` to hold positions.", "yahtzee roll")
            lines = self._yahtzee_roll(session, holds)
            lines.extend(self._yahtzee_status_lines(session))
            return PendingReply(_format_lines(lines), "yahtzee roll")

        if arg_lower.startswith("score"):
            if not session or session.finished:
                return PendingReply("No active scorecard. `/yahtzee start` to begin.", "yahtzee score")
            category_key = self._yahtzee_parse_category(arg_text)
            if not category_key:
                readable = ", ".join(YAHTZEE_LABELS[c] for c in YAHTZEE_CATEGORIES)
                return PendingReply(f"Category not recognized. Options: {readable}.", "yahtzee score")
            if session.scorecard.get(category_key) is not None:
                return PendingReply("That category is already filled.", "yahtzee score")
            if session.rolls_left == 3 and all(d == 0 for d in session.dice):
                return PendingReply("Roll the dice at least once before scoring.", "yahtzee score")
            points = self._yahtzee_score_category(session.dice, category_key)
            session.scorecard[category_key] = points
            session.rolls_left = 0
            total = self._yahtzee_total(session)
            label = YAHTZEE_LABELS[category_key]
            lines = [f"Scored {points} in {label}.", f"Total so far: {total}."]
            remaining = [cat for cat, val in session.scorecard.items() if val is None]
            if not remaining:
                session.finished = True
                lines.append("All categories complete!")
                lines.append("Final card:")
                lines.extend(self._yahtzee_scorecard_lines(session))
                lines.append(f"Final score: {total}")
                self.yahtzee_sessions.pop(sender_key, None)
                return PendingReply(_format_lines(lines), "yahtzee score")
            session.round_number += 1
            session.rolls_left = 3
            session.dice = [0, 0, 0, 0, 0]
            roll_lines = self._yahtzee_roll(session, set())
            lines.append("")
            lines.extend(roll_lines)
            lines.extend(self._yahtzee_status_lines(session))
            lines.append("Hold dice with `/yahtzee roll <positions>` or score a category.")
            return PendingReply(_format_lines(lines), "yahtzee score")

        if arg_lower in {"stop", "quit"}:
            if not session:
                return PendingReply("No Yahtzee scorecard to clear.", "yahtzee stop")
            self.yahtzee_sessions.pop(sender_key, None)
            return PendingReply("Yahtzee session cleared.", "yahtzee stop")

        if arg_lower == "help":
            lines = [
                "Yahtzee commands:",
                "  /yahtzee start",
                "  /yahtzee roll [positions...]",
                "  /yahtzee score <category>",
                "  /yahtzee status",
                "  /yahtzee stop",
            ]
            return PendingReply(_format_lines(lines), "yahtzee help")

        return PendingReply("Commands: `start`, `roll [holds]`, `score <category>`, `status`, `stop`, `stats`, `help`.", "yahtzee help")

    def _yahtzee_start_session(self) -> YahtzeeSession:
        scorecard = {category: None for category in YAHTZEE_CATEGORIES}
        return YahtzeeSession(dice=[0, 0, 0, 0, 0], rolls_left=3, round_number=1, scorecard=scorecard)

    def _yahtzee_status_lines(self, session: YahtzeeSession) -> List[str]:
        lines = [f"Round {session.round_number}/13"]
        dice_display = " ".join(str(d) for d in session.dice)
        lines.append(f"Dice: {dice_display} (rolls left: {session.rolls_left})")
        lines.extend(self._yahtzee_scorecard_lines(session))
        return lines

    def _yahtzee_scorecard_lines(self, session: YahtzeeSession) -> List[str]:
        lines = ["Scorecard:"]
        for category in YAHTZEE_CATEGORIES:
            label = YAHTZEE_LABELS[category]
            value = session.scorecard.get(category)
            lines.append(f"  {label}: {value if value is not None else '—'}")
        lines.append(f"Total: {self._yahtzee_total(session)}")
        return lines

    def _yahtzee_roll(self, session: YahtzeeSession, holds: Set[int]) -> List[str]:
        if session.rolls_left <= 0:
            return ["No rolls left this round."]
        if any(pos < 1 or pos > 5 for pos in holds):
            return ["Hold positions must be between 1 and 5."]
        for idx in range(5):
            if (idx + 1) in holds and session.dice[idx] != 0:
                continue
            if (idx + 1) in holds and session.dice[idx] == 0:
                # Holding a die that doesn't exist yet -> ignore hold
                pass
            if (idx + 1) not in holds:
                session.dice[idx] = random.randint(1, 6)
        session.rolls_left -= 1
        dice_display = " ".join(str(d) for d in session.dice)
        return [f"Rolled: {dice_display} (rolls left: {session.rolls_left})"]

    def _yahtzee_parse_holds(self, text: str) -> Optional[Set[int]]:
        parts = text.split()
        if len(parts) <= 1:
            return set()
        holds: Set[int] = set()
        for token in parts[1:]:
            if not token.isdigit():
                return None
            holds.add(int(token))
        return holds

    def _yahtzee_parse_category(self, text: str) -> Optional[str]:
        parts = text.split(None, 1)
        if len(parts) < 2:
            return None
        raw = parts[1].strip().lower()
        normalized = raw.replace(" ", "_")
        for category in YAHTZEE_CATEGORIES:
            if normalized == category:
                return category
        for category in YAHTZEE_CATEGORIES:
            label = YAHTZEE_LABELS[category].lower().replace(" ", "_")
            if normalized == label:
                return category
        return None

    def _yahtzee_score_category(self, dice: List[int], category: str) -> int:
        counts = Counter(dice)
        dice_sum = sum(dice)
        if category == "ones":
            return counts[1] * 1
        if category == "twos":
            return counts[2] * 2
        if category == "threes":
            return counts[3] * 3
        if category == "fours":
            return counts[4] * 4
        if category == "fives":
            return counts[5] * 5
        if category == "sixes":
            return counts[6] * 6
        if category == "three_kind":
            return dice_sum if any(c >= 3 for c in counts.values()) else 0
        if category == "four_kind":
            return dice_sum if any(c >= 4 for c in counts.values()) else 0
        if category == "full_house":
            if sorted(counts.values()) == [2, 3]:
                return 25
            return 0
        if category == "small_straight":
            straights = [
                {1, 2, 3, 4},
                {2, 3, 4, 5},
                {3, 4, 5, 6},
            ]
            dice_set = set(dice)
            return 30 if any(straight.issubset(dice_set) for straight in straights) else 0
        if category == "large_straight":
            dice_set = set(dice)
            if dice_set == {1, 2, 3, 4, 5} or dice_set == {2, 3, 4, 5, 6}:
                return 40
            return 0
        if category == "chance":
            return dice_sum
        if category == "yahtzee":
            return 50 if any(c == 5 for c in counts.values()) else 0
        return 0

    def _yahtzee_total(self, session: YahtzeeSession) -> int:
        return sum(value for value in session.scorecard.values() if value is not None)

    # ------------------------
    # Hangman logic
    # ------------------------
    def _handle_hangman(self, sender_key: str, args: str) -> PendingReply:
        arg_lower = (args or "").strip().lower()
        if arg_lower == "stats":
            lines = self._basic_game_stats('hangman', 'Hangman')
            return PendingReply(_format_lines(lines), "hangman stats")

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
            self._record_game_play('hangman')
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
    def _handle_wordle(self, sender_key: str, sender_short: str, args: str) -> PendingReply:
        record = self._ensure_wordle_daily_record()
        args_clean = (args or "").strip()
        args_lower = args_clean.lower()

        if args_lower == "stats":
            return PendingReply(_format_lines(self._wordle_stats_lines()), "wordle stats")

        session = self.wordle_sessions.get(sender_key)
        target = record.get("target", "radio")
        if session and session.target != target:
            self.wordle_sessions.pop(sender_key, None)
            session = None

        completed = self._wordle_has_completed(record, sender_key)

        if not args_clean or args_lower == "status":
            if session:
                lines = ["🟩 Mesh-Wordle attempts:"]
                if session.attempts:
                    for guess, pattern in session.attempts:
                        lines.append(f"{pattern}  {guess.upper()}")
                else:
                    lines.append("No guesses yet. Use `/wordle guess <word>`.")
                remaining = session.max_attempts - len(session.attempts)
                lines.append(f"Attempts remaining: {remaining}")
                return PendingReply(_format_lines(lines), "wordle status")
            if completed:
                lines = ["🎯 You're done with today's Wordle."]
                lines.append("New Wordle question at 7 AM!")
                lines.append("")
                lines.extend(self._wordle_stats_lines())
                return PendingReply(_format_lines(lines), "wordle status")
            return PendingReply("Type `/wordle start` to take on today's puzzle.", "wordle status")

        if args_lower == "start":
            if session and not session.is_complete():
                lines = ["🟨 Wordle already in progress."]
                if session.attempts:
                    for guess, pattern in session.attempts:
                        lines.append(f"{pattern}  {guess.upper()}")
                remaining = session.max_attempts - len(session.attempts)
                lines.append(f"Attempts remaining: {remaining}")
                return PendingReply(_format_lines(lines), "wordle start")
            if completed:
                lines = ["🌅 You've already tackled today's Wordle.", "New Wordle question at 7 AM!"]
                return PendingReply(_format_lines(lines), "wordle start")
            session = WordleSession(target=target)
            self.wordle_sessions[sender_key] = session
            self._record_game_play('wordle')
            lines = [
                "🟨 Daily Wordle is live!",
                "Guess the five-letter word with `/wordle guess <word>`.",
                f"You have {session.max_attempts} attempts. Next puzzle unlocks at {self._wordle_next_reset_label()}.",
            ]
            return PendingReply(_format_lines(lines), "wordle start")

        if args_lower.startswith("guess"):
            if completed and not session:
                return PendingReply("New Wordle question at 7 AM! You've already finished today's game.", "wordle guess")
            if not session:
                return PendingReply("Start today's puzzle first with `/wordle start`.", "wordle guess")
            parts = args_clean.split()
            if len(parts) < 2:
                return PendingReply("Use `/wordle guess <word>`.", "wordle guess")
            guess = parts[1].lower()
            if len(guess) != 5 or not guess.isalpha():
                return PendingReply("Guesses must be five letters.", "wordle guess")
            pattern = self._wordle_pattern(session.target, guess)
            session.add_attempt(guess, pattern)
            lines = [f"{pattern}  {guess.upper()}"]
            if guess == session.target:
                rank = self._record_wordle_win(sender_key, sender_short, record)
                self.wordle_sessions.pop(sender_key, None)
                lines.append("🎉 Nailed it!")
                lines.append(f"You're the {self._ordinal(rank)} solver today.")
                lines.append(f"Next puzzle unlocks at {self._wordle_next_reset_label()}.")
                lines.append("Check `/wordle stats` for today's leaderboard.")
            elif len(session.attempts) >= session.max_attempts:
                self.wordle_sessions.pop(sender_key, None)
                self._mark_wordle_done(sender_key, record)
                lines.append(f"💀 All attempts used. The word was {session.target.upper()}.")
                lines.append("New Wordle question at 7 AM!")
            else:
                remaining = session.max_attempts - len(session.attempts)
                lines.append(f"Attempts left: {remaining}.")
            return PendingReply(_format_lines(lines), "wordle guess")

        if args_lower == "stop":
            self.wordle_sessions.pop(sender_key, None)
            return PendingReply("Wordle session cleared. Come back anytime before 7 AM for another try.", "wordle stop")

        return PendingReply("Commands: `start`, `guess <word>`, `status`, `stats`, `stop`.", "wordle help")

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
        if (args or "").strip().lower() == "stats":
            lines = self._basic_game_stats('adventure', 'Adventure')
            return PendingReply(_format_lines(lines), "adventure stats")

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
            self._record_game_play('adventure')
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
            "system": (
                "You are the Mesh Master choose-your-own adventure narrator. "
                "Follow the prompt instructions exactly, stay concise, and do not add extra sections."
            ),
            "stream": False,
            "options": {
                "temperature": 0.7,
                "num_ctx": 2048,
                "num_predict": 160,
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
        arg_lower = (args or "").strip().lower()
        if arg_lower == "stats":
            lines = self._basic_game_stats('wordladder', 'Word Ladder')
            return PendingReply(_format_lines(lines), "wordladder stats")

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
            self._record_game_play('wordladder')
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
        self._record_game_play('rps')
        taunt = random.choice(RPS_RESPONSES).format(player=sender_short)
        return PendingReply(taunt, "rps")

    def _handle_coinflip(self, sender_short: str) -> PendingReply:
        self._record_game_play('coinflip')
        story = random.choice(COIN_FLIP_RESPONSES).format(player=sender_short)
        return PendingReply(story, "coinflip")

    # ------------------------
    # Cipher
    # ------------------------
    def _handle_cipher(self, sender_key: str, args: str) -> PendingReply:
        if (args or "").strip().lower() == "stats":
            lines = self._basic_game_stats('cipher', 'Cipher')
            return PendingReply(_format_lines(lines), "cipher stats")

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
            self._record_game_play('cipher')
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

    # ------------------------
    # Quiz Battle
    # ------------------------
    def _handle_quizbattle(self, sender_key: str, sender_short: str, args: str) -> PendingReply:
        if (args or "").strip().lower() == "stats":
            lines = self._basic_game_stats('quizbattle', 'Quiz Battle')
            return PendingReply(_format_lines(lines), "quiz stats")

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
            questions = list(QUIZ_QUESTIONS)
            random.shuffle(questions)
            queue = deque(questions)
            session = QuizBattleSession(queue=queue)
            self.quiz_sessions[sender_key] = session
            self._record_game_play('quizbattle')
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
        if (args or "").strip().lower() == "stats":
            lines = self._basic_game_stats('morse', 'Morse Trainer')
            return PendingReply(_format_lines(lines), "morse stats")

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
            self._record_game_play('morse')
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
