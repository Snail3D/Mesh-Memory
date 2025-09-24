How to create a branch, apply these changes, and open a pull request

1) In your forked repo clone or local repo, create a new branch:

```bash
git checkout -b feat/ollama-chat-history
```

2) Replace `mesh-ai.py` in the repo with the modified `mesh-ai.py` from this workspace (copy file contents).

3) Add the test harness file to `tests/run_ai_integration_test.py` (copy from `changes_for_pr/run_ai_integration_test.py`).

4) Commit and push:

```bash
git add mesh-ai.py tests/run_ai_integration_test.py
git commit -m "feat: include Ollama chat-history context and add test harness"
git push origin feat/ollama-chat-history
```

5) Open a PR from `feat/ollama-chat-history` in your fork into `mr-tbot/mesh-ai:main` with title:

"feat: include Ollama chat-history context and test harness"

Suggested PR body:

This PR adds support for including recent chat history when sending prompts to Ollama (direct messages and channel-specific context). It also adds a small test harness `tests/run_ai_integration_test.py` to simulate messages and capture generated prompts without requiring an active Ollama or LMStudio instance.

Files changed:
- `mesh-ai.py` — include `build_ollama_history` and updated `send_to_ollama`/`get_ai_response` flows. Added defensive UI toggle wiring and autostart panel fixes.
- `tests/run_ai_integration_test.py` — sandboxed test harness that loads `config.json` and captures outgoing Ollama prompt payloads (mocked network).

Notes:
- Please ensure `config.json` contains `ollama_model` set to an Ollama-available model (e.g. `llama3.2:latest`) and that Ollama is running locally when testing end-to-end.
- The test harness neutralizes Flask and route decorators to avoid starting the server during unit tests; it's intended for local developer testing.

If you'd like, I can prepare a proper patch or fork/PR from this environment if you provide remote repository write access (or push credentials).