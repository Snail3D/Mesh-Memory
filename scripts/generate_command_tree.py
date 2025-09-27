#!/usr/bin/env python3
"""Generate a tree view of Mesh Master commands and aliases."""
from __future__ import annotations

import ast
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter

ROOT = Path(__file__).resolve().parent.parent
SOURCE_FILE = ROOT / "mesh-master.py"
OUTPUT_TEXT = ROOT / "docs" / "mesh_master_command_tree.txt"
OUTPUT_PDF = ROOT / "docs" / "mesh_master_command_tree.pdf"


class CommandData:
    def __init__(self) -> None:
        self.aliases: Dict[str, List[str]] = defaultdict(list)
        self.builtin: List[str] = []


def _extract_command_data() -> Tuple[Dict[str, List[Tuple[str, Tuple[str, ...]]]], List[str]]:
    source = SOURCE_FILE.read_text(encoding="utf-8")
    module = ast.parse(source)

    command_aliases: Optional[Dict[str, Dict[str, object]]] = None
    builtin_commands: Optional[Iterable[str]] = None

    for node in module.body:
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            if node.target.id == "COMMAND_ALIASES":
                command_aliases = ast.literal_eval(node.value)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "BUILTIN_COMMANDS":
                    builtin_commands = ast.literal_eval(node.value)

    if command_aliases is None:
        raise RuntimeError("COMMAND_ALIASES definition not found in mesh-master.py")
    if builtin_commands is None:
        raise RuntimeError("BUILTIN_COMMANDS definition not found in mesh-master.py")

    canonical_map: Dict[str, List[Tuple[str, Tuple[str, ...]]]] = defaultdict(list)

    for alias, info in command_aliases.items():
        canonical = info.get("canonical", alias)
        languages = tuple(sorted(set(info.get("languages") or [])))
        if alias != canonical:
            canonical_map[canonical].append((alias, languages))

    all_canonicals = set(canonical_map.keys()) | set(builtin_commands)

    for canonical in all_canonicals:
        alias_entries = canonical_map.setdefault(canonical, [])
        alias_entries.sort(key=lambda pair: pair[0])

    ordered_canonicals = sorted(all_canonicals)

    return canonical_map, ordered_canonicals


def _format_tree(canonical_map: Dict[str, List[Tuple[str, Tuple[str, ...]]]], ordered: List[str]) -> List[str]:
    lines: List[str] = ["Commands"]
    total = len(ordered)
    for idx, canonical in enumerate(ordered):
        is_last = idx == total - 1
        branch = "└─" if is_last else "├─"
        lines.append(f"{branch} {canonical}")

        aliases = canonical_map.get(canonical, [])
        if not aliases:
            child_prefix = "    " if is_last else "│   "
            lines.append(f"{child_prefix}└─ (no aliases)")
            continue

        for jdx, (alias, languages) in enumerate(aliases):
            alias_last = jdx == len(aliases) - 1
            child_prefix = "    " if is_last else "│   "
            leaf = "└─" if alias_last else "├─"
            if languages:
                lang_note = ", ".join(languages)
                line_text = f"{alias} [langs: {lang_note}]"
            else:
                line_text = alias
            lines.append(f"{child_prefix}{leaf} {line_text}")
    return lines


def _ensure_docs_dir() -> None:
    docs_dir = OUTPUT_TEXT.parent
    docs_dir.mkdir(parents=True, exist_ok=True)


def _write_text(lines: List[str]) -> None:
    OUTPUT_TEXT.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _register_font() -> str:
    candidates = [
        Path("/usr/share/fonts/truetype/noto/NotoSans-Regular.ttf"),
        Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
    ]
    for font_path in candidates:
        if font_path.exists():
            font_name = font_path.stem
            pdfmetrics.registerFont(TTFont(font_name, str(font_path)))
            return font_name
    # fallback to standard Helvetica
    return "Helvetica"


def _write_pdf(lines: List[str]) -> None:
    font_name = _register_font()
    page_width, page_height = letter
    margin = 40
    line_height = 12

    c = canvas.Canvas(str(OUTPUT_PDF), pagesize=letter)
    c.setFont(font_name, 10)

    y = page_height - margin
    for line in lines:
        if y < margin:
            c.showPage()
            c.setFont(font_name, 10)
            y = page_height - margin
        c.drawString(margin, y, line)
        y -= line_height
    c.save()


def main() -> None:
    canonical_map, ordered = _extract_command_data()
    lines = _format_tree(canonical_map, ordered)
    _ensure_docs_dir()
    _write_text(lines)
    _write_pdf(lines)
    print(f"Wrote {OUTPUT_TEXT}")
    print(f"Wrote {OUTPUT_PDF}")


if __name__ == "__main__":
    main()
