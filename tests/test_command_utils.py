from __future__ import annotations

import importlib.util
import tempfile
import types
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
import sys

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _load_command_utils():
    module_path = PROJECT_ROOT / "mesh_master" / "command_utils.py"
    spec = importlib.util.spec_from_file_location("mesh_master.command_utils", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


command_utils = _load_command_utils()
promote_bare_command = command_utils.promote_bare_command


class CommandPromotionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.known_commands = [
            "/wipe",
            "/c",
            "/weather",
            "/whereami",
            "/help",
            "/menu",
        ]
        self.resolve_calls = []

        def resolver(token: str):
            self.resolve_calls.append(token)
            if token.lower() in {cmd.lower() for cmd in self.known_commands}:
                return token.lower(), None, None, None, ""
            return None, "unknown", None, None, ""

        self.resolver = resolver

    def test_exact_match(self):
        result = promote_bare_command("wipe mailbox alpha", self.known_commands, self.resolver)
        self.assertEqual(result, "/wipe mailbox alpha")

    def test_case_insensitive(self):
        result = promote_bare_command("WeAtHer tomorrow", self.known_commands, self.resolver)
        self.assertEqual(result, "/weather tomorrow")

    def test_trailing_punctuation(self):
        result = promote_bare_command("wipe, mailbox bravo", self.known_commands, self.resolver)
        self.assertEqual(result, "/wipe mailbox bravo")

    def test_fuzzy_typo(self):
        result = promote_bare_command("whereami?", self.known_commands, self.resolver)
        self.assertEqual(result, "/whereami")
        result2 = promote_bare_command("wheather update", self.known_commands, self.resolver)
        self.assertEqual(result2, "/weather update")

    def test_short_words_do_not_match_randomly(self):
        result = promote_bare_command("hi there", self.known_commands, self.resolver)
        self.assertIsNone(result)

    def test_non_command_text_returns_none(self):
        self.assertIsNone(promote_bare_command("this is normal text", self.known_commands, self.resolver))

    def test_already_slash_command_ignored(self):
        self.assertIsNone(promote_bare_command("/wipe mailbox", self.known_commands, self.resolver))


if __name__ == "__main__":
    unittest.main()
