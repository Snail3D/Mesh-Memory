# Mesh Master Mail System

Mesh Master includes a mesh-native mail system that lets operators exchange longer
messages asynchronously. This guide captures how it works today, with a focus on
PIN-protected inboxes.

## Core Commands

- `/m <mailbox> <message>` — send mail (and create the mailbox if needed).
- `/c <mailbox> [question]` — read or search an inbox (`PIN=1234` can be appended).
- `/wipe mailbox <mailbox>` — clear an inbox when you are done.
- `/emailhelp` — quick primer that can be broadcast to new users.

All commands are direct-message only unless noted in the on-device help.

## Mailbox Creation Flow

1. `/m <mailbox> ...` against a non-existent inbox launches a guided wizard.
2. After confirmation, the wizard prompts for an optional PIN (4–8 digits, `SKIP`
   leaves the inbox open to anyone who knows the name).
3. The mailbox is created and the first message is stored immediately.
4. Mesh Master records the creator's sender key as the owner for future PIN
   changes and notifications.

Security settings live in `data/mail_security.json`. Each mailbox entry carries
metadata such as:

- `pin_hash`: SHA-256 hash of the chosen PIN (or `null` for open inboxes).
- `owner`: normalized sender key that created the mailbox.
- `failures`: per-sender attempt counters for brute-force protection.
- `subscribers` and `messages`: track unread items and heartbeat-triggered
  notifications.

## PIN Handling During Reads

When a user runs `/c <mailbox> ...`, the handler:

1. Parses any `PIN=` token (e.g. `/c team PIN=0420 search logistics`).
2. Gets or creates the security record for that mailbox.
3. If no PIN is set, access is granted and failure counters are reset.
4. Otherwise the caller must have a known sender key and provide the correct PIN.
5. On success the PIN hash is verified, the failure counter resets, and the
   request continues with the normal inbox view or search flow.

## Brute-force Protection

PIN attempts are tracked per sender key:

- The first 14 bad attempts return `❌ Incorrect PIN. Try again.`
- Attempt 15 triggers `⚠️` warnings that the next failure will lock the user out.
- Attempt 20 marks that sender as blocked until they eventually present the
  correct PIN.

The warning and lock thresholds are configurable via constants in
`mesh_master/mail_manager.py` (`PIN_WARNING_THRESHOLD` and `PIN_LOCK_THRESHOLD`).
Successful authentication always clears the counter so legitimate users regain
access immediately.

## Notifications

If mailbox notifications are enabled (`mail_notify_enabled` in `config.json`),
Mesh Master keeps a watcher list for each inbox and now supports automated
follow-up reminders:

- The owner and anyone who has previously opened the inbox are flagged when new
  mail arrives. They receive an immediate DM the next time a heartbeat comes in
  (even during quiet hours) so the news lands right away.
- After the initial ping, Mesh Master schedules follow-up reminders for anyone
  who still has unread messages. By default the first reminder fires the next
  morning at the start of the active window (`notify_active_start_hour`), then
  repeats hourly for up to three additional nudges.
- Reminders only send while heartbeats are received during the active window;
  quiet hours (`notify_active_end_hour` → `notify_active_start_hour`) suppress
  follow-ups until the window re-opens.
- Reading the inbox, wiping it, or exhausting the configured reminder count
  clears the schedule immediately.

Useful knobs:

- `mail_notify_reminders_enabled` — flip to `false` to disable follow-up
  reminders entirely (initial alerts still send).
- `mail_notify_quiet_hours_enabled` — master switch for applying the quiet-hour
  window; leave it on to respect the drop-down schedule, or turn it off for
  24/7 reminder delivery.
- `mail_notify_max_reminders` — number of reminder messages after the first
  alert; default is `3`.
- `mail_notify_reminder_hours` — spacing, in hours, between reminders once they
  begin (default `1.0`).
- `notify_active_start_hour` / `notify_active_end_hour` — define the local time
  window when reminders are allowed (use the same hour to run 24/7).
- `mail_notify_include_self` — when `false`, the author of the new mail is
  excluded from notifications even if they are subscribed to the inbox.
- `mail_notify_heartbeat_only` — legacy flag that keeps reminder delivery tied
  to heartbeat traffic.

## File Locations & Persistence

- Mail content: `mesh_mailboxes.json`
- Security and PIN metadata: `data/mail_security.json`
- Offline context for saved conversations: `data/saved_contexts.json`

Both JSON files are updated atomically. If you snapshot or back up the node,
copy these files together to preserve mailbox content alongside security rules.

## Operational Tips

- Encourage users to screenshot the creation summary so they retain the mailbox
  name and PIN.
- When sharing a PIN, send it out-of-band or rotate it afterward to reduce
  exposure.
- Consider running `/wipe mailbox <name>` after events or incidents to remove
  stale content.
- For public bulletin-board style boxes, skip the PIN entirely and rely on the
  natural obscurity of the mailbox name.

For additional command references, open the global README or run `/emailhelp`
from your node.
