from __future__ import annotations

import json
import shutil
import sys
import tempfile
from pathlib import Path
import unittest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from mesh_master.offline_wiki import OfflineWikiStore, OfflineWikiArticle


class OfflineWikiStoreTest(unittest.TestCase):
    def _make_dataset(self) -> tuple[Path, Path]:
        tmp_dir = Path(tempfile.mkdtemp(prefix="offline_wiki_test_"))
        self.addCleanup(lambda: shutil.rmtree(tmp_dir, ignore_errors=True))
        index = tmp_dir / "index.json"
        article_path = tmp_dir / "apollo-11.json"
        payload = {
            "title": "Apollo 11",
            "summary": "First crewed Moon landing mission.",
            "content": "Apollo 11 was the spaceflight that first landed humans on the Moon." * 20,
            "source": "https://en.wikipedia.org/wiki/Apollo_11",
        }
        article_path.write_text(json.dumps(payload), encoding="utf-8")
        index_payload = {
            "entries": [
                {
                    "key": "apollo 11",
                    "title": "Apollo 11",
                    "path": "apollo-11.json",
                    "summary": "First crewed Moon landing mission.",
                    "aliases": ["apollo11", "moon landing"],
                }
            ]
        }
        index.write_text(json.dumps(index_payload), encoding="utf-8")
        return index, tmp_dir

    def test_lookup_with_exact_match(self):
        index, base_dir = self._make_dataset()
        store = OfflineWikiStore(index, base_dir=base_dir)

        article, suggestions = store.lookup("Apollo 11", summary_limit=200, context_limit=500)

        self.assertIsInstance(article, OfflineWikiArticle)
        self.assertEqual(article.title, "Apollo 11")
        self.assertTrue(article.summary.startswith("First crewed Moon landing"))
        self.assertIn("Moon", article.content)
        self.assertEqual(suggestions, [])

        alias_article, alias_suggestions = store.lookup("apollo11", summary_limit=200, context_limit=500)
        self.assertIsInstance(alias_article, OfflineWikiArticle)
        self.assertEqual(alias_suggestions, [])
        self.assertEqual(alias_article.matched_alias, "apollo11")

    def test_lookup_returns_suggestions_for_near_miss(self):
        index, base_dir = self._make_dataset()
        store = OfflineWikiStore(index, base_dir=base_dir)

        article, suggestions = store.lookup("apolo 12", summary_limit=200, context_limit=500)

        self.assertIsNone(article)
        self.assertTrue(suggestions)
        self.assertIn("Apollo 11", suggestions)

    def test_store_article_creates_index_and_content(self):
        with tempfile.TemporaryDirectory(prefix="offline_wiki_store_") as tmp:
            base_dir = Path(tmp)
            index = base_dir / "index.json"
            store = OfflineWikiStore(index, base_dir=base_dir)

            created = store.store_article(
                title="Custom Topic",
                content="Important offline content.",
                summary="Brief summary",
                source="https://example.com/custom",
                aliases=["custom topic", "custom"],
            )

            self.assertTrue(created)
            self.assertTrue(store.is_ready())

            article, suggestions = store.lookup("custom", summary_limit=50, context_limit=200)
            self.assertIsNotNone(article)
            self.assertEqual(article.title, "Custom Topic")
            self.assertEqual(article.matched_alias, "custom")
            self.assertEqual(suggestions, [])

            index_payload = json.loads(index.read_text(encoding="utf-8"))
            self.assertEqual(index_payload["entries"][0]["title"], "Custom Topic")

            self.assertFalse(store.store_article(title="Custom Topic", content="x", overwrite=False))

    def test_missing_index_sets_error_state(self):
        with tempfile.TemporaryDirectory(prefix="offline_wiki_missing_") as tmp:
            base_dir = Path(tmp)
            index = base_dir / "missing.json"
            store = OfflineWikiStore(index, base_dir=base_dir)

            self.assertFalse(store.is_ready())
            msg = store.error_message() or ""
            self.assertIn("missing", msg.lower())


if __name__ == "__main__":
    unittest.main()
