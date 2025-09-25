# Changelog

All notable changes to this project will be documented in this file.

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
  - Light retries and error surfacing for LM Studio, OpenAI, and Ollama.
  - App-level PID lock to prevent multiple instances.

Notes
- Admin commands are DM-only to avoid channel misuse.
- Health reports degraded states (disconnected radio, stalled queue, recent AI error).

