from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
import sys

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import importlib.util
import types


def _load_mail_manager():
    module_path = PROJECT_ROOT / "mesh_master" / "mail_manager.py"
    package_name = "mesh_master"
    if package_name not in sys.modules:
        package = types.ModuleType(package_name)
        package.__path__ = [str(PROJECT_ROOT / "mesh_master")]
        sys.modules[package_name] = package
    spec = importlib.util.spec_from_file_location("mesh_master.mail_manager", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


MailManager = _load_mail_manager().MailManager


class MailCommandTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory(prefix="mail_manager_test_")
        self.addCleanup(self.tmpdir.cleanup)
        self._manager_counter = 0
        self.manager = self._make_manager(name="base")

    def _make_manager(self, *, name: str | None = None, **overrides) -> MailManager:
        self._manager_counter += 1
        base_name = name or f"manager_{self._manager_counter}"
        base_path = Path(self.tmpdir.name) / base_name
        base_path.mkdir(parents=True, exist_ok=True)
        params = dict(
            store_path=str(base_path / "mailboxes.json"),
            security_path=str(base_path / "security.json"),
            clean_log=lambda *args, **kwargs: None,
            ai_log=lambda *args, **kwargs: None,
            ollama_url=None,
            search_model="test-model",
            search_timeout=1,
            search_num_ctx=64,
            search_max_messages=5,
            message_limit=50,
            follow_up_delay=0.0,
            notify_enabled=False,
            reminders_enabled=False,
            reminder_interval_seconds=300,
            reminder_expiry_seconds=900,
            reminder_max_count=1,
            include_self_notifications=False,
            heartbeat_only=False,
            quiet_hours_enabled=False,
            quiet_start_hour=0,
            quiet_end_hour=0,
            stats=None,
        )
        params.update(overrides)
        return MailManager(**params)

    def _seed_mailbox(self, name: str, owner: str | None = None) -> None:
        self.manager.store.create_mailbox(name)
        self.manager.store.append(
            name,
            {
                "body": "message body",
                "sender_short": "tester",
                "sender_key": owner or "tester-key",
            },
        )
        if owner is not None:
            self.manager._set_mailbox_security(name, owner, pin=None)

    def test_handle_wipe_allows_owner(self):
        owner_key = "!owner123"
        mailbox = "alpha"
        self._seed_mailbox(mailbox, owner=owner_key)

        reply = self.manager.handle_wipe(mailbox, actor_key=owner_key, is_admin=False)

        self.assertIn("now empty", reply.text)
        self.assertEqual(self.manager.store.mailbox_count(mailbox), 0)

    def test_handle_wipe_denies_non_owner(self):
        owner_key = "!owner123"
        mailbox = "bravo"
        self._seed_mailbox(mailbox, owner=owner_key)

        reply = self.manager.handle_wipe(mailbox, actor_key="!intruder", is_admin=False)

        self.assertIn("Only the mailbox owner", reply.text)
        self.assertGreater(self.manager.store.mailbox_count(mailbox), 0)

    def test_handle_wipe_requires_actor_key_for_non_admin(self):
        owner_key = "!owner789"
        mailbox = "charlie"
        self._seed_mailbox(mailbox, owner=owner_key)

        reply = self.manager.handle_wipe(mailbox, actor_key=None, is_admin=False)

        self.assertIn("verify you're the mailbox owner", reply.text)
        self.assertGreater(self.manager.store.mailbox_count(mailbox), 0)

    def test_handle_wipe_denies_when_owner_missing(self):
        mailbox = "delta"
        self._seed_mailbox(mailbox, owner=None)

        reply = self.manager.handle_wipe(mailbox, actor_key="!caller", is_admin=False)

        self.assertIn("doesn't have an owner", reply.text)
        self.assertGreater(self.manager.store.mailbox_count(mailbox), 0)

    def test_handle_wipe_admin_override(self):
        owner_key = "!owner555"
        mailbox = "echo"
        self._seed_mailbox(mailbox, owner=owner_key)

        reply = self.manager.handle_wipe(mailbox, actor_key="!different", is_admin=True)

        self.assertIn("now empty", reply.text)
        self.assertEqual(self.manager.store.mailbox_count(mailbox), 0)

    def test_mailboxes_for_user_includes_owner_and_subscriber(self):
        owner_key = "!ownerabc"
        subscriber_key = "!sub123"
        mailbox = "golf"

        self.manager.store.create_mailbox(mailbox)
        self.manager._set_mailbox_security(mailbox, owner_key, pin=None)
        entry = self.manager._get_security_entry(mailbox)
        with self.manager.security_lock:
            self.manager._ensure_subscriber_entry(entry, subscriber_key)
            self.manager._save_security()

        owner_mailboxes = self.manager.mailboxes_for_user(owner_key)
        subscriber_mailboxes = self.manager.mailboxes_for_user(subscriber_key)

        self.assertIn(mailbox, owner_mailboxes)
        self.assertIn(mailbox, subscriber_mailboxes)

    def test_notification_aggregation_and_reply_flow(self):
        viewer_key = "!viewer"
        viewer_node = "node-view"
        sender_key = "!sender"
        sender_node = "node-sender"
        owner_key = "!owner"

        manager = self._make_manager(
            name="notify",
            notify_enabled=True,
            reminders_enabled=True,
            reminder_max_count=3,
        )

        mailboxes = ["alpha", "bravo"]
        for idx, mailbox in enumerate(mailboxes):
            manager.store.create_mailbox(mailbox)
            manager._set_mailbox_security(mailbox, owner_key, pin=None)
            entry = manager._get_security_entry(mailbox)
            with manager.security_lock:
                manager._ensure_subscriber_entry(
                    entry,
                    viewer_key,
                    node_id=viewer_node,
                    short="Viewer",
                )
                manager._save_security()
            message = {
                "id": f"msg-{idx}",
                "body": f"hello {mailbox}",
                "mailbox": mailbox,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "sender_short": "Sender",
                "sender_id": sender_node,
            }
            manager.store.append(mailbox, dict(message), allow_create=False)
            manager._record_message_append(
                mailbox,
                message,
                sender_key=sender_key,
                sender_id=sender_node,
                sender_short="Sender",
            )

        manager.handle_heartbeat(viewer_key, viewer_node, allow_send=True)
        self.assertIn(viewer_key, manager.active_auto_notifications)
        self.assertEqual(len(manager.events), 1)
        alert_event = manager.events[0]
        self.assertEqual(alert_event.get("type"), "dm")
        self.assertEqual(alert_event.get("node_id"), viewer_node)
        alert_text = alert_event.get("text", "")
        for mailbox in mailboxes:
            self.assertIn(mailbox, alert_text)

        inbox_reply = manager._build_mailbox_result(
            mailboxes[0],
            existed=True,
            sender_key=viewer_key,
            sender_id=viewer_node,
            sender_short="Viewer",
        )
        reply_text = inbox_reply.text.lower()
        self.assertIn("reply", reply_text)
        self.assertIn(mailboxes[0], inbox_reply.text)
        self.assertNotIn(viewer_key, manager.active_auto_notifications)
        self.assertEqual(len(manager.events), 0)
        context = manager._lookup_reply_context(viewer_key)
        self.assertIsNotNone(context)
        self.assertGreater(len(context.get("entries", [])), 0)

        reply_result = manager.handle_reply_intent(
            viewer_key,
            viewer_node,
            "Viewer",
            "reply 1 Thanks for the note!",
        )
        self.assertIsNotNone(reply_result)
        self.assertIn("📨", reply_result.text)
        self.assertEqual(len(manager.events), 1)
        dm_event = manager.events[0]
        self.assertEqual(dm_event.get("node_id"), sender_node)
        self.assertIn("Reply from", dm_event.get("text", ""))
        self.assertNotIn(viewer_key, manager.active_auto_notifications)


if __name__ == "__main__":
    unittest.main()
