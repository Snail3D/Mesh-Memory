# MESH MASTER v1.99.0 — Off-Grid AI Operations Suite

**MESH MASTER 1.99** is the next evolution of the Mesh-AI project: a resilient AI copilot for Meshtastic LoRa meshes that remembers conversations, coordinates teams, and keeps the network moving even when the wider internet is gone. Version 1.99 doubles down on collaboration with the new Mesh Mail hub, a suite of llama-powered games for morale and training, richer offline knowledge, and a refreshed web command center.

> **Disclaimer**  
> This project is an independent community effort and is **not associated** with the official Meshtastic project. Always maintain backup communication paths for real emergencies.

![Mesh Master 2.0 hero](docs/mesh-master-2.0-hero.svg)

---

## 1.99 Headline Upgrades
- **Mesh Mail** — PIN-protected inboxes, multi-user notifications, and one-shot llama summaries keep longer messages flowing across the mesh.
- **Game Hub** — Chess & Checkers duels, Blackjack, Yahtzee rounds, Tic-Tac-Toe, Hangman, Wordle, Word Ladder, Adventure stories, Cipher drills, Bingo, Morse, Rock–Paper–Scissors, Coinflip, Quiz Battle, and more—all DM-friendly and multilingual.
- **Adaptive Personalities & Context Capsules** — `/aipersonality` and `/save`/`/recall` tune the assistant instantly while persistent archives keep continuity across restarts.
- **Offline Knowledge on Tap** — Trimmed MeshTastic handbook, offline wiki lookups, and cached expert answers deliver verified guidance without leaving the mesh.
- **Refreshed Ops Console** — Live SSE log stream with emoji tagging, command shortcuts, and mesh-aware send forms for both broadcasts and DMs.
- **Hardening for the Field** — Larger async queues, smarter retry logic, strict single-instance locks, and heartbeat-driven health reporting for container or bare-metal deployments.

---

## Feature Overview

### Persistent Mesh Intelligence
- End-to-end message history survives restarts (`messages_archive.json`) with configurable limits.  
- Background async workers keep RX/TX responsive while Ollama generates replies.  
- Tone and personalities can be adjusted at runtime with `/vibe`; the core system prompt is fixed. MOTD can be updated via DM-only admin commands.

### Mesh Mail & Collaboration
- Direct-message `/m mailbox message` to drop mail; guided flow creates boxes, sets optional PINs, and captures owner metadata.  
- `/c mailbox [question]` shows the latest entries and, when a question is provided, uses the bundled `llama3.2:1b` model to pull a concise answer.  
- `/wipe mailbox`, `/wipe chathistory`, `/wipe personality`, and `/wipe all <mailbox>` keep things tidy.  
- Notification engine flags subscribers on heartbeat with unread counts while respecting PIN security and brute-force throttling.  
- See `docs/mail_readme.md` for deep-dive internals.

### Game Hub & Morale Tools
- `/games` lists every title with quick descriptions and command hints.  
- Story-driven `/adventure` adapts to the chat language and offers branching outcomes.  
- `/wordladder` teammates can collaboratively bridge start/end words, asking the llama for hints on demand.  
- Manage risk in `/blackjack`, push streaks in `/yahtzee`, or rally the squad with `/games` for the full list.  
- Fast laughs with `/rps`, `/coinflip`, and `/quizbattle`; puzzle practice with `/cipher`, `/morse`, `/hangman`, `/wordle`.

### Knowledge & Research Aids
- `/meshtastic <question>` consults a curated ~25k token field guide with a warm cache for instant follow-ups.  
- `/offline wiki <topic>` or `/offline wiki <topic> PIN=1234` taps locally mirrored reference articles.  
- `/save` captures conversation context capsules for later `/recall`—perfect for mission hand-offs.

### Web Dashboard & APIs
- Real-time log viewer with emoji categories (📡 connection, 📨 messages, 🤖 AI, ⚠️ warnings, 🔧 admin).  
- Three-column mesh console surfaces broadcasts, direct messages, and nearby nodes; quick-send form handles DM routing and chunking.  
- Health endpoints: `GET /ready`, `/live`, `/healthz`, `/heartbeat`, plus `/dashboard` and `/logs` frontends.  
- `/send` and `/ui_send` POST endpoints enable automated workflows; optional `/discord_webhook` bridge for cross-platform relays.

### Integrations & Extensibility
- Native Ollama support tuned for low-bandwidth meshes (`llama3.2:1b` by default) with adjustable context size, chunk delays, and timeout controls.  
- Home Assistant relay can forward a dedicated channel (with optional PIN requirement) to the Conversation API.  
- Feature flags (`feature_flags.json`) let operators disable specific commands or restrict replies to DMs/broadcasts.

---

## Quick Start (Python)

1. **Clone & enter the repository**
   ```bash
   git clone https://github.com/snailpi/mesh-ai.git
   cd mesh-ai
   ```
2. **Create a virtual environment and install dependencies**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install --upgrade pip
   pip install -r requirements.txt
   ```
3. **Configure your node**
   - Edit `config.json` and update connection details (`serial_port` or `wifi_host`), `ai_provider` settings, and channel preferences.  
   - Adjust `commands_config.json`, `motd.json`, and any feature flags as desired.
4. **Launch Mesh Master**
   ```bash
   NO_BROWSER=1 python mesh-master.py
   ```
5. **Open the dashboard**
   - Visit `http://localhost:5000/dashboard` for live logs and controls.

---

## Container Workflow

Build the 2.0 image locally so you run the exact code in this repository:

```bash
docker build -t mesh-master:2.0 .
```

Minimal compose example:

```yaml
services:
  mesh-master:
    image: mesh-master:2.0
    container_name: mesh-master
    privileged: true
    ports:
      - "5000:5000"
    volumes:
      - ./config.json:/app/config.json:ro
      - ./commands_config.json:/app/commands_config.json:ro
      - ./motd.json:/app/motd.json:ro
      - ./data:/app/data
      - ./state/mesh-master.log:/app/mesh-master.log
      - ./state/messages.log:/app/messages.log
      - ./state/messages_archive.json:/app/messages_archive.json
      - ./state/script.log:/app/script.log
      - ./state/mesh_mailboxes.json:/app/mesh_mailboxes.json
      - ./state/mesh_mail.db:/app/mesh_mail.db
    restart: unless-stopped
```

If you rely on USB serial, also bind `/dev/serial/by-id` (read-only) and `/dev` as needed, and set `MESH_INTERFACE`/`serial_port` accordingly.

Create the `state` directory (and `touch` the files listed above) before the first run so Docker can mount them successfully.

---

## Everyday Commands

- **AI conversations** — `/ai`, `/bot`, `/query`, or `/data` (DM or configured channels).  
- **Mesh mail** — `/m <mailbox> <message>`, `/c <mailbox> [question]`, `/emailhelp`, `/wipe ...`.  
- **Quick knowledge** — `/bible`, `/chucknorris`, `/elpaso`, `/meshtastic`, `/offline wiki`, `/web <query>`, `/wiki <topic>`, `/drudge`, `/weather`.  
- **Personality & context** — `/aipersonality` (list/set/prompt/reset), `/save [topic]`, `/recall <topic>`, `/exit`.  
- **Games** — `/games`, `/hangman start`, `/wordle start`, `/wordladder start cold warm`, `/adventure start`, `/cipher start`, `/quizbattle start`, `/morse start`, `/rps`, `/coinflip`.  
- **Location & status** — `/test`, `/motd`, Meshtastic “Request Position,” `/reset`, `/about`.  
- **Admin (DM-only)** — `/vibe`, `/changemotd`, `/showprompt`, `/printprompt`.

All commands are case-insensitive. Special commands buffer ~3 seconds before responding to reduce radio congestion.

---

## Dashboard & Monitoring

- **Logs:** `/logs` (HTML) and `/logs/raw` (plain text); streaming SSE feed powers the dashboard in real time.  
- **Health probes:**
  - `GET /ready` → HTTP 200 only when the radio link is up (503 otherwise).  
  - `GET /live` → process liveness.  
  - `GET /healthz` → full JSON snapshot (connection, queue depth, worker status, AI timing, last error).  
- **Heartbeat:** Watch `mesh-master.log` for the `💓 HB` line every ~30 seconds summarizing RX/TX/AI ages.  
- **REST hooks:** `POST /send` and `POST /ui_send` accept JSON payloads for automations; `/discord_webhook` bridges Discord events into the mesh when enabled.

---

## Configuration Essentials

Key fields from `config.json` (trimmed for brevity):

```json
{
  "serial_port": "/dev/serial/by-id/usb-RAKwireless_WisCore_RAK4631_Board_XXXX",
  "serial_baud": 38400,
  "ai_provider": "ollama",
  "system_prompt": "You are an offline chatbot serving a local mesh network...",
  "ollama_model": "llama3.2:1b",
  "ollama_timeout": 120,
  "ollama_context_chars": 4000,
  "async_response_queue_max": 25,
  "meshtastic_kb_max_context_chars": 3200,
  "meshtastic_kb_cache_ttl": 600,
  "default_personality_id": "trail_scout",
  "mail_search_timeout": 120,
  "reply_in_channels": true,
  "reply_in_directs": true,
  "chunk_size": 200,
  "chunk_buffer_seconds": 1,
  "home_assistant_enabled": false,
  "home_assistant_channel_index": 1
}
```

Additional knobs:
- **Mesh Mail:** `mailbox_max_messages`, `mail_follow_up_delay`, `mail_notify_enabled`, `mail_notify_reminders_enabled`, `mail_notify_quiet_hours_enabled`, `mail_notify_reminder_hours`, `mail_notify_expiry_hours`, `mail_notify_max_reminders`, `mail_notify_include_self`, `mail_notify_heartbeat_only`, `mail_search_model`, `mail_search_max_messages`, `mail_search_num_ctx`, `mail_search_timeout`, `notify_active_start_hour`, `notify_active_end_hour`, and `mail_security_file`.  
- **Saved context:** `saved_context_max_chars`, `saved_context_summary_chars`, `context_session_timeout_seconds`.  
- **Feature toggles:** `feature_flags.json` can disable commands or switch `message_mode` to `broadcast`, `dm`, or `both`.  
- **Offline wiki:** configure `offline_wiki_dir`, `offline_wiki_summary_chars`, and `offline_wiki_context_chars` to control local article size.

Remember to restart the service after editing configs that lack runtime setters.

---

## Hardware Tips

- **RAK4631 Always-On Profile:**
  1. Copy `99-rak-no-autosuspend.rules` into `/etc/udev/rules.d/`, reload udev, and replug the device.  
  2. Apply `hardware_profiles/rak4631_always_on.yaml` with `./scripts/apply_rak4631_profile.py` (add `--dry-run` to preview).  
  3. Confirm `role: ROUTER_CLIENT` and `is_power_saving: false` with `meshtastic --info`.
- **Log Hygiene:** `CLEAN_LOGGING.md` outlines how to rotate logs safely when running unattended.  
- **Back up frequently:** snapshot `config.json`, `commands_config.json`, `motd.json`, `messages.log`, `messages_archive.json`, `mesh_mailboxes.json`, and `data/mail_security.json` together.

---

## Upgrade Notes from 1.x

- Single-instance PID locks prevent accidental double starts. Stop older services before launching 2.0.  
- Mesh Mail subsystems replace ad-hoc DM forwarding—migrate workflows to `/m`/`/c` commands.  
- New async response queue defaults to 25 messages; adjust `async_response_queue_max` if running on limited hardware.  
- Knowledge base trimmed and cached; tune `meshtastic_kb_max_context_chars` when using larger models.  
- Dashboard styling and APIs remain compatible, but cached assets moved to `static/`.

---

## Documentation & Support

- Mesh Mail internals: `docs/mail_readme.md`  
- Command map: `docs/mesh_master_command_tree.pdf`  
- Service management: `README_SERVICE.md`  
- Security practices: `SECURITY.md`

Issue reports and contributions are welcome via GitHub pull requests.

---

## Acknowledgements

- Original Mesh Master project by [MR_TBOT](https://github.com/mr-tbot/mesh-master); this fork builds on that foundation with a focus on fully offline resilience.  
- Thanks to the Meshtastic community researchers, testers, and field operators who supplied feedback, hardware profiles, and localization tweaks.

---

## License

MESH MASTER is distributed under the terms of the [MIT License](LICENSE).

The Meshtastic name and logo remain trademarks of Meshtastic LLC.
