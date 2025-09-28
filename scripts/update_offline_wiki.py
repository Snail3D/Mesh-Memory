#!/usr/bin/env python3
"""Fetch Wikipedia topics and store them in the offline_wiki catalog.

Examples:
    python scripts/update_offline_wiki.py "Apollo 13" "American Revolutionary War"
    python scripts/update_offline_wiki.py --topics-file topics.txt --overwrite

Each article is stored as ``data/offline_wiki/<slug>.json`` and ``index.json``
is updated (created if missing). Existing entries are left untouched unless
``--overwrite`` is supplied.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Set, Tuple

import requests
from unidecode import unidecode

DEFAULT_DIR = Path("data/offline_wiki")
DEFAULT_SUMMARY_CHARS = 400
DEFAULT_CONTEXT_CHARS = 40000

WIKI_USER_AGENT = "MeshMasterOffline/1.0 (+https://github.com/mr-tbot/mesh-master)"
WIKI_API_URL = "https://en.wikipedia.org/w/api.php"
WIKI_SUMMARY_URL = "https://en.wikipedia.org/api/rest_v1/page/summary/{}"


class OfflineWikiBuilder:
    def __init__(
        self,
        directory: Path,
        *,
        summary_chars: int = DEFAULT_SUMMARY_CHARS,
        context_chars: int = DEFAULT_CONTEXT_CHARS,
        overwrite: bool = False,
    ) -> None:
        self.directory = directory
        self.summary_chars = max(50, summary_chars)
        self.context_chars = max(500, context_chars)
        self.overwrite = overwrite
        self.index_file = self.directory / "index.json"
        self.entries: Dict[str, Dict[str, object]] = {}
        self._load_index()

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------
    def ensure_directory(self) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)

    def add_topic(self, topic: str) -> Tuple[str, bool, int]:
        article = fetch_wikipedia_article(topic, max_chars=self.context_chars)
        key = normalize_key(article["title"])
        if not key:
            raise RuntimeError(f"Could not derive a key for '{topic}'")
        slug = slugify(article["title"])
        path = f"{slug}.json"
        entry_exists = key in self.entries and not self.overwrite
        if entry_exists:
            return article["title"], False, 0

        payload = {
            "title": article["title"],
            "summary": clip(article.get("summary") or article["content"], self.summary_chars),
            "source": article.get("source"),
            "content": clip(article["content"], self.context_chars),
        }
        self._write_article(path, payload)
        self.entries[key] = {
            "key": key,
            "title": article["title"],
            "path": path,
            "summary": payload.get("summary") or None,
            "aliases": article.get("aliases") or [],
        }
        approx_bytes = len(payload["content"])
        approx_bytes += len(payload.get("summary") or "")
        return article["title"], True, approx_bytes

    def save_index(self) -> None:
        ordered = sorted(self.entries.values(), key=lambda item: item["title"].lower())
        metadata = {
            "schema": 1,
            "entries": ordered,
        }
        self.index_file.write_text(json.dumps(metadata, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------
    def _load_index(self) -> None:
        if not self.index_file.exists():
            self.entries = {}
            return
        try:
            data = json.loads(self.index_file.read_text(encoding="utf-8"))
        except Exception as exc:  # pragma: no cover - malformed user file
            print(f"⚠️  Could not read existing index: {exc}. Starting fresh.")
            self.entries = {}
            return
        entries = data.get("entries") if isinstance(data, dict) else None
        if not isinstance(entries, list):
            print("⚠️  Existing index is malformed (missing entries list). Starting fresh.")
            self.entries = {}
            return
        temp: Dict[str, Dict[str, object]] = {}
        for raw in entries:
            if not isinstance(raw, dict):
                continue
            key = normalize_key(raw.get("key") or raw.get("title"))
            path = raw.get("path")
            if not key or not isinstance(path, str):
                continue
            entry = {
                "key": key,
                "title": str(raw.get("title") or raw.get("key") or path),
                "path": path,
                "summary": raw.get("summary") or None,
                "aliases": list(raw.get("aliases", [])) if isinstance(raw.get("aliases"), list) else [],
            }
            temp[key] = entry
        self.entries = temp

    def _write_article(self, relative_path: str, payload: Dict[str, object]) -> None:
        target = self.directory / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


# ----------------------------------------------------------------------
# Wikipedia helpers
# ----------------------------------------------------------------------

def fetch_wikipedia_article(topic: str, *, max_chars: int) -> Dict[str, object]:
    headers = {
        "User-Agent": WIKI_USER_AGENT,
        "Accept": "application/json",
    }
    params = {
        "action": "query",
        "prop": "extracts|info",
        "explaintext": 1,
        "exsectionformat": "plain",
        "exlimit": 1,
        "exchars": max_chars,
        "redirects": 1,
        "inprop": "url",
        "format": "json",
        "formatversion": 2,
        "titles": topic,
    }
    response = requests.get(WIKI_API_URL, params=params, headers=headers, timeout=30)
    response.raise_for_status()
    payload = response.json()
    pages = (payload.get("query", {}).get("pages") or [])
    if not pages:
        raise RuntimeError(f"Wikipedia query returned no data for '{topic}'")
    page = pages[0]
    if page.get("missing"):
        raise RuntimeError(f"Wikipedia has no article for '{topic}'")
    title = page.get("title") or topic
    extract = (page.get("extract") or "").strip()
    if not extract:
        # Fallback to REST summary endpoint
        safe_title = title.replace(" ", "_")
        summary_resp = requests.get(WIKI_SUMMARY_URL.format(safe_title), headers=headers, timeout=30)
        summary_resp.raise_for_status()
        summary_data = summary_resp.json()
        extract = (summary_data.get("extract") or "").strip()
    first_line = extract.split("\n", 1)[0]
    summary = clip(first_line, DEFAULT_SUMMARY_CHARS)
    canonical_url = page.get("fullurl") or page.get("canonicalurl")
    return {
        "title": title,
        "content": extract,
        "summary": summary,
        "source": canonical_url,
        "aliases": [],
    }


# ----------------------------------------------------------------------
# Utility helpers
# ----------------------------------------------------------------------

def normalize_key(value: str) -> str:
    return " ".join(unidecode(value or "").lower().split())


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", unidecode(value or "").lower()).strip("-")
    return slug or "article"


def clip(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"


def read_topics(args: argparse.Namespace) -> List[str]:
    topics: List[str] = []
    if args.topics:
        topics.extend(args.topics)
    if args.topics_file:
        file_topics = Path(args.topics_file).read_text(encoding="utf-8").splitlines()
        topics.extend(line.strip() for line in file_topics if line.strip())
    deduped: List[str] = []
    seen = set()
    for topic in topics:
        norm = topic.strip()
        if not norm:
            continue
        key = normalize_key(norm)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(norm)
    return deduped


def collect_category_topics(
    categories: Iterable[str],
    *,
    depth: int,
    max_pages: int,
) -> List[str]:
    topics: List[str] = []
    seen_keys: Set[str] = set()
    for raw_category in categories:
        if not raw_category:
            continue
        cat = raw_category.strip()
        if not cat:
            continue
        if not cat.lower().startswith("category:"):
            cat = f"Category:{cat}"
        pages = fetch_category_topics(cat, depth=depth, max_pages=max_pages)
        for title in pages:
            key = normalize_key(title)
            if key and key not in seen_keys:
                seen_keys.add(key)
                topics.append(title)
    return topics


def fetch_category_topics(
    category_title: str,
    *,
    depth: int,
    max_pages: int,
) -> List[str]:
    headers = {"User-Agent": WIKI_USER_AGENT, "Accept": "application/json"}
    queue: List[Tuple[str, int]] = [(category_title, 0)]
    seen_categories: Set[str] = set()
    titles: List[str] = []
    while queue:
        current, level = queue.pop(0)
        if current in seen_categories:
            continue
        seen_categories.add(current)
        params = {
            "action": "query",
            "list": "categorymembers",
            "cmtitle": current,
            "cmtype": "page|subcat",
            "cmlimit": "500",
            "format": "json",
        }
        cont = {}
        pages_pulled = 0
        while True:
            merged = {**params, **cont}
            resp = requests.get(WIKI_API_URL, params=merged, headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            members = data.get("query", {}).get("categorymembers", [])
            for member in members:
                ns = member.get("ns")
                title = member.get("title")
                if not title:
                    continue
                if ns == 0:  # article
                    titles.append(title)
                    pages_pulled += 1
                    if max_pages and pages_pulled >= max_pages:
                        break
                elif ns == 14 and level < depth:
                    sub_name = member.get("title")
                    if sub_name:
                        queue.append((sub_name, level + 1))
            if max_pages and pages_pulled >= max_pages:
                break
            cont = data.get("continue") or {}
            if not cont:
                break
    return titles


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Wikipedia topics for offline use.")
    parser.add_argument("topics", nargs="*", help="Topics to fetch (quoted if containing spaces)")
    parser.add_argument("--topics-file", help="Path to a newline-delimited list of topics")
    parser.add_argument(
        "--category",
        action="append",
        default=[],
        help="Wikipedia category to crawl for topics (can be repeated).",
    )
    parser.add_argument(
        "--category-depth",
        type=int,
        default=1,
        help="How many levels of subcategories to follow (default: 1).",
    )
    parser.add_argument(
        "--max-pages-per-category",
        type=int,
        default=0,
        help="Optional limit on pages pulled from each category (0 = no limit).",
    )
    parser.add_argument(
        "--target-bytes",
        type=int,
        default=0,
        help="Stop once roughly this many content bytes have been added (0 = no target).",
    )
    parser.add_argument(
        "--max-articles",
        type=int,
        default=0,
        help="Stop after adding this many new articles (0 = no limit).",
    )
    parser.add_argument("--dir", type=Path, default=DEFAULT_DIR, help="Target directory (default: data/offline_wiki)")
    parser.add_argument("--summary-chars", type=int, default=DEFAULT_SUMMARY_CHARS, help="Max characters to store in summary")
    parser.add_argument("--context-chars", type=int, default=DEFAULT_CONTEXT_CHARS, help="Max characters to store in article content")
    parser.add_argument("--overwrite", action="store_true", help="Refresh existing entries if they already exist")
    return parser.parse_args(argv)


def main(argv: List[str]) -> int:
    args = parse_args(argv)
    topics = read_topics(args)
    if args.category:
        category_topics = collect_category_topics(
            args.category,
            depth=args.category_depth,
            max_pages=args.max_pages_per_category,
        )
        topics.extend(category_topics)
    if not topics:
        print("No topics provided. Add arguments or use --topics-file.")
        return 1

    builder = OfflineWikiBuilder(
        args.dir,
        summary_chars=args.summary_chars,
        context_chars=args.context_chars,
        overwrite=args.overwrite,
    )
    builder.ensure_directory()

    successes: List[str] = []
    skipped: List[str] = []
    failures: Dict[str, str] = {}
    total_new_bytes = 0
    total_new_articles = 0
    target_bytes = max(0, args.target_bytes)
    max_articles = max(0, args.max_articles)
    for topic in topics:
        try:
            title, created, approx = builder.add_topic(topic)
            if created:
                successes.append(title)
                total_new_articles += 1
                total_new_bytes += approx
                if target_bytes and total_new_bytes >= target_bytes:
                    print(f"Target size reached (~{total_new_bytes:,} bytes). Stopping.")
                    break
                if max_articles and total_new_articles >= max_articles:
                    print(f"Article limit reached ({total_new_articles}). Stopping.")
                    break
            else:
                skipped.append(title)
        except Exception as exc:
            failures[topic] = str(exc)

    builder.save_index()

    if successes:
        print("Added:")
        for title in successes:
            print(f"  • {title}")
        print(f"Total new articles: {total_new_articles}")
        print(f"Approximate content bytes added: {total_new_bytes:,}")
    if skipped:
        print("Skipped (already present):")
        for title in skipped:
            print(f"  • {title}")
    if failures:
        print("Failed:")
        for topic, reason in failures.items():
            print(f"  • {topic}: {reason}")
        return 2
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry
    sys.exit(main(sys.argv[1:]))
