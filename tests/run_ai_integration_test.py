#!/usr/bin/env python3
"""
Temporary test harness to call parse_incoming_text and send_to_ollama
without interfering with the running service. This imports functions
from the module by reading the source and exec'ing in a fresh namespace
so module-level side effects (starting Flask/threads) don't run.

This will simulate:
- a direct message from node 42 to the AI
- a channel message in channel 2

It prints the combined prompt that would be sent to Ollama (from DEBUG output)
and prints the returned AI response (if any).
"""
import runpy
import sys
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parents[1] / 'mesh-ai.py'
if not MODULE_PATH.exists():
    print('mesh-ai.py not found at expected path:', MODULE_PATH)
    sys.exit(1)

# Read the source and execute in an isolated namespace
ns = {}
src = MODULE_PATH.read_text(encoding='utf-8')
# Avoid creating Flask app / starting threads when executing the source here.
# Replace Flask init and route decorators so module-level route definitions do not run.
src_mod = src.replace("app = Flask(__name__)", "app = None  # patched by test harness to avoid Flask init", 1)
src_mod = src_mod.replace("@app.route", "@_no_op_route")

# Provide a sane module identity and file path for Flask-related helpers
ns['__name__'] = 'mesh_ai_test_module'
ns['__file__'] = str(MODULE_PATH)

# Provide a no-op decorator to replace @app.route usages
def _no_op_route(*a, **k):
    def _decorator(f):
        return f
    return _decorator

ns['_no_op_route'] = _no_op_route

# Load project config (so OLLAMA_MODEL and related values are used)
import json
cfg_path = MODULE_PATH.parent / 'config.json'
cfg = {}
if cfg_path.exists():
    try:
        cfg = json.loads(cfg_path.read_text(encoding='utf-8'))
        print('Loaded config.json for test harness.')
    except Exception as e:
        print('Failed to read config.json:', e)
else:
    print('config.json not found; using defaults in test harness.')

# Inject config-derived vars into namespace before exec so module code can pick them up
if 'ollama_model' in cfg:
    ns['OLLAMA_MODEL'] = cfg.get('ollama_model')
if 'ollama_url' in cfg:
    ns['OLLAMA_URL'] = cfg.get('ollama_url')
if 'system_prompt' in cfg:
    ns['SYSTEM_PROMPT'] = cfg.get('system_prompt')
ns['DEBUG_ENABLED'] = bool(cfg.get('debug', False))

# Monkeypatch requests.post in the exec namespace to capture payloads and avoid network calls
class _MockResponse:
    def __init__(self, status_code=200, body=None):
        self.status_code = status_code
        self._body = body or {"response": "MOCKED_AI_REPLY"}
    def json(self):
        return self._body

def _mock_requests_post(url, json=None, timeout=None, headers=None):
    print('\n--- MOCKED requests.post called ---')
    print('URL:', url)
    print('Timeout:', timeout)
    print('Payload model:', json.get('model') if isinstance(json, dict) else None)
    prompt = None
    if isinstance(json, dict):
        prompt = json.get('prompt')
    if prompt:
        # Print only the last ~800 chars to avoid huge output
        snippet = prompt[-800:]
        print('Prompt (tail, up to 800 chars):\n', snippet)
    else:
        print('No prompt found in payload')
    return _MockResponse(status_code=200, body={"response": "MOCKED_AI_REPLY"})

ns['requests'] = type('r', (), {'post': _mock_requests_post})

exec(compile(src_mod, str(MODULE_PATH), 'exec'), ns)

# Grab functions
parse_incoming_text = ns.get('parse_incoming_text')
send_to_ollama = ns.get('send_to_ollama')
build_ollama_history = ns.get('build_ollama_history')

if not parse_incoming_text or not send_to_ollama:
    print('Required functions not found in mesh-ai.py')
    sys.exit(1)

# Ensure runtime globals reflect the test config and that network calls are mocked
ns['requests'] = type('r', (), {'post': _mock_requests_post})
if 'ollama_model' in cfg:
    ns['OLLAMA_MODEL'] = cfg.get('ollama_model')
if 'ollama_url' in cfg:
    ns['OLLAMA_URL'] = cfg.get('ollama_url')
ns['AI_PROVIDER'] = cfg.get('ai_provider', 'ollama').lower()
ns['SYSTEM_PROMPT'] = cfg.get('system_prompt', ns.get('SYSTEM_PROMPT', 'You are a helpful assistant.'))
ns['DEBUG_ENABLED'] = bool(cfg.get('debug', False))

print('Running test: direct message -> AI')
resp = parse_incoming_text('Hello AI, remember my last message', sender_id=42, is_direct=True, channel_idx=None)
print('parse_incoming_text returned:', resp)

print('\nDirectly invoking send_to_ollama for observation (no network call if OLLAMA_URL unset)')
try:
    out = send_to_ollama('Test message from unit test', sender_id=42, is_direct=True, channel_idx=None)
    print('send_to_ollama returned:', out)
except Exception as e:
    print('send_to_ollama raised:', e)

print('\nRunning test: channel message -> HA or AI')
resp2 = parse_incoming_text('Status update for the channel', sender_id=99, is_direct=False, channel_idx=2)
print('parse_incoming_text (channel) returned:', resp2)
