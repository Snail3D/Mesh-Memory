"""Helpers for lightweight command parsing and normalization."""

from __future__ import annotations

import difflib
import string
from typing import Callable, Iterable, Optional, Sequence, Tuple

# Characters to trim from bare command tokens (common punctuation + whitespace)
_COMMAND_PUNCT = string.punctuation + "\u2013\u2014"  # include dash variants

ResolveResult = Tuple[Optional[str], Optional[str], Optional[Sequence[str]], Optional[str], Optional[str]]


def _sanitize_token(token: str) -> str:
    """Strip punctuation around a token and lowercase it."""
    return token.strip(_COMMAND_PUNCT + " ").lower()


def _pick_best_match(candidate: str, options: Sequence[str]) -> Tuple[Optional[str], float]:
    """Return the best matching option and the similarity score."""
    if not options:
        return None, 0.0
    best_name = None
    best_ratio = 0.0
    for opt in options:
        ratio = difflib.SequenceMatcher(None, candidate, opt).ratio()
        if ratio > best_ratio:
            best_ratio = ratio
            best_name = opt
    return best_name, best_ratio


def promote_bare_command(
    text: str,
    known_commands: Iterable[str],
    resolve_func: Callable[[str], ResolveResult],
    *,
    fuzzy: bool = True,
) -> Optional[str]:
    """Convert bare input like "wipe mailbox" into a slash command string.

    Parameters
    ----------
    text: str
        User-provided text (not already starting with a slash).
    known_commands: Iterable[str]
        Collection of known command or alias tokens (with leading slash).
    resolve_func: Callable[[str], ResolveResult]
        Function mirroring ``resolve_command_token`` that accepts a slash-prefixed
        token and returns (canonical, reason, suggestions, language_hint, append).
    fuzzy: bool
        Allow fuzzy matching for near-miss tokens when True.
    """

    if not text:
        return None

    stripped = text.lstrip()
    if not stripped or stripped.startswith('/'):
        return None

    parts = stripped.split(None, 1)
    first_raw = parts[0]
    remainder_raw = parts[1] if len(parts) > 1 else ''
    token = _sanitize_token(first_raw)
    if not token:
        return None

    known_list = list(known_commands)
    if not known_list:
        return None
    known_bare_map = {}
    for cmd in known_list:
        if not isinstance(cmd, str):
            continue
        bare = cmd.lstrip('/')
        if not bare:
            continue
        # prefer first occurrence to keep canonical association stable
        known_bare_map.setdefault(bare.lower(), cmd)

    direct_candidate = token
    canonical_cmd = None
    alias_append = None

    if direct_candidate in known_bare_map:
        candidate_slash = '/' + known_bare_map[direct_candidate].lstrip('/')
        res = resolve_func(candidate_slash)
        if res:
            canonical_cmd, reason, _, _, alias_append = res
            if canonical_cmd:
                pass
            else:
                canonical_cmd = None
    elif fuzzy:
        bare_names = list(known_bare_map.keys())
        best_name, score = _pick_best_match(direct_candidate, bare_names)
        if best_name:
            min_ratio = 0.82
            length = len(direct_candidate)
            if length <= 3:
                min_ratio = 1.0
            elif length == 4:
                min_ratio = 0.92
            if score >= min_ratio:
                candidate_slash = '/' + known_bare_map[best_name].lstrip('/')
                res = resolve_func(candidate_slash)
                if res:
                    canonical_cmd, reason, _, _, alias_append = res
                    if not canonical_cmd:
                        canonical_cmd = None
    if not canonical_cmd:
        return None

    remainder = ''
    if remainder_raw:
        remainder = ' ' + remainder_raw.strip()
    if alias_append:
        remainder = f"{alias_append}{remainder}" if remainder else alias_append
    return canonical_cmd + remainder


__all__ = ["promote_bare_command"]
