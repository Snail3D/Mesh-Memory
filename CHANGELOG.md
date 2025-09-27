# Changelog

All notable changes to this project will be documented in this file.

## [1.1.0] - 2025-09-27

Highlights
- Retired the `/weather` command and all external location datasets to keep Mesh Master fully offline.
- Trimmed the bundled MeshTastic knowledge base to a focused core (~25k tokens) for faster responses.
- Added a warm cache for `/meshtastic` lookups (configurable via `meshtastic_kb_cache_ttl`) so follow-up questions reuse the loaded context.

Notes
- The knowledge base still reloads when the source file changes or cache TTL expires.
- Set `meshtastic_kb_max_context_chars` in `config.json` (defaults to 3200) to cap the prompt size if needed.

## [1.0.0] - 2025-09-25

Highlights
- DM-only admin commands
  - `/changeprompt <text>`: Update AI system prompt (persists to `config.json`).
  - `/changemotd <text>`: Update MOTD (persists to `motd.json`).
  - `/showprompt` and `/printprompt`: Display current system prompt.
- Health and heartbeat
  - Endpoints: `/healthz` (detailed), `/live` (liveness), `/ready` (readiness).
  - Heartbeat log line every ~30s summarizing status and activity ages.
- Stability and robustness
  - Atomic writes for config/MOTD to avoid partial files.
  - App-level PID lock to prevent multiple instances.

Notes
- Admin commands are DM-only to avoid channel misuse.
- Health reports degraded states (disconnected radio, stalled queue, recent AI error).

