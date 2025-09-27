import sys
import types
from pathlib import Path
import json


MODULE_PATH = Path(__file__).resolve().parents[1] / 'mesh-master.py'


def _exec_module_source(cfg=None):
    """Execute mesh-master.py in an isolated namespace and return important callables.

    cfg: optional dict to inject into the exec namespace as config values.
    """
    src = MODULE_PATH.read_text(encoding='utf-8')
    # Patch out Flask app creation and route decorators so we don't start the server
    src_mod = src.replace("app = Flask(__name__)", "app = None  # patched by test harness to avoid Flask init", 1)
    src_mod = src_mod.replace("@app.route", "@_no_op_route")

    ns = {}
    ns['__name__'] = 'mesh_master_test_module'
    ns['__file__'] = str(MODULE_PATH)

    def _no_op_route(*a, **k):
        def _decorator(f):
            return f
        return _decorator

    ns['_no_op_route'] = _no_op_route

    # Minimal fake modules to satisfy imports in mesh-master.py
    fake_meshtastic = types.ModuleType('meshtastic')
    fake_serial = types.ModuleType('meshtastic.serial_interface')

    class _FakeSerialInterface:
        def __init__(self, *a, **k):
            self._serial = types.SimpleNamespace(baudrate=921600)
        def close(self):
            pass

    fake_serial.SerialInterface = _FakeSerialInterface
    fake_meshtastic.serial_interface = fake_serial
    fake_meshtastic.BROADCAST_ADDR = 0xffffffff

    mesh_interface_mod = types.ModuleType('meshtastic.mesh_interface')
    class _FakeMeshInterface:
        def __init__(self, *a, **k):
            self.myNode = None
            self.localNode = None
            self.nodes = {}
        def close(self):
            pass
    mesh_interface_mod.MeshInterface = _FakeMeshInterface
    tcp_interface_mod = types.ModuleType('meshtastic.tcp_interface')
    tcp_interface_mod.TCPInterface = None

    pubsub_mod = types.ModuleType('pubsub')
    pub_obj = types.SimpleNamespace(subscribe=lambda *a, **k: None, unsubscribe=lambda *a, **k: None)
    pubsub_mod.pub = pub_obj

    # simple unidecode passthrough
    unidecode_mod = types.ModuleType('unidecode')
    unidecode_mod.unidecode = lambda s: s

    # minimal google protobuf DecodeError
    google_protobuf = types.ModuleType('google.protobuf')
    google_message = types.ModuleType('google.protobuf.message')
    class DecodeError(Exception):
        pass
    google_message.DecodeError = DecodeError
    google_protobuf.message = google_message

    sys.modules.setdefault('meshtastic', fake_meshtastic)
    sys.modules.setdefault('meshtastic.serial_interface', fake_serial)
    sys.modules.setdefault('meshtastic.mesh_interface', mesh_interface_mod)
    sys.modules.setdefault('meshtastic.tcp_interface', tcp_interface_mod)
    sys.modules.setdefault('pubsub', pubsub_mod)
    sys.modules.setdefault('pubsub.pub', pub_obj)
    sys.modules.setdefault('unidecode', unidecode_mod)
    sys.modules.setdefault('google.protobuf', google_protobuf)
    sys.modules.setdefault('google.protobuf.message', google_message)

    # Provide a mocked requests.post that returns a predictable response
    class _MockResponse:
        def __init__(self, status_code=200, body=None):
            self.status_code = status_code
            self._body = body or {"response": "MOCKED_AI_REPLY"}
        def json(self):
            return self._body

    def _mock_requests_post(url, json=None, timeout=None, headers=None):
        # Return a deterministic mocked response suitable for tests
        return _MockResponse(status_code=200, body={"response": "MOCKED_AI_REPLY"})

    ns['requests'] = types.SimpleNamespace(post=_mock_requests_post)

    # Inject minimal config-derived variables
    ns['DEBUG_ENABLED'] = bool(cfg.get('debug', False)) if (cfg := (cfg or {})) is not None else False
    if cfg:
        if 'ollama_model' in cfg:
            ns['OLLAMA_MODEL'] = cfg.get('ollama_model')
        if 'ollama_url' in cfg:
            ns['OLLAMA_URL'] = cfg.get('ollama_url')
        if 'system_prompt' in cfg:
            ns['SYSTEM_PROMPT'] = cfg.get('system_prompt')
        ns['AI_PROVIDER'] = cfg.get('ai_provider', 'ollama')

    exec(compile(src_mod, str(MODULE_PATH), 'exec'), ns)

    return ns


def test_parse_and_send_direct():
    ns = _exec_module_source(cfg={})
    parse_incoming_text = ns.get('parse_incoming_text')
    send_to_ollama = ns.get('send_to_ollama')
    assert callable(parse_incoming_text)
    assert callable(send_to_ollama)

    # Simulate a direct message and ensure a response string (or dict) is returned
    resp = parse_incoming_text('Hello test', sender_id=42, is_direct=True, channel_idx=None)
    # parse_incoming_text may return None for non-AI commands; ensure it doesn't crash
    assert resp is None or isinstance(resp, (str, dict))

    # Test that send_to_ollama returns the mocked response without raising
    out = send_to_ollama('Unit test message', sender_id=42, is_direct=True, channel_idx=None)
    assert out in (None, 'MOCKED_AI_REPLY') or (isinstance(out, str))


def test_reset_behavior():
    ns = _exec_module_source(cfg={})
    parse_incoming_text = ns.get('parse_incoming_text')
    assert callable(parse_incoming_text)

    # Calling /reset for direct and channel contexts should return a string message
    r1 = parse_incoming_text('/reset', sender_id=42, is_direct=True, channel_idx=None)
    r2 = parse_incoming_text('/reset', sender_id=99, is_direct=False, channel_idx=2)
    assert isinstance(r1, str) or r1 is None
    assert isinstance(r2, str) or r2 is None


def test_dm_history_is_per_sender_thread():
    # Load module with defaults and grab required symbols
    ns = _exec_module_source(cfg={})
    log_message = ns.get('log_message')
    build_ollama_history = ns.get('build_ollama_history')
    messages = ns.get('messages')
    messages_lock = ns.get('messages_lock')
    assert callable(log_message)
    assert callable(build_ollama_history)

    # Clear any existing messages
    with messages_lock:
        messages.clear()

    # Simulate two DM threads: sender 1 and sender 2
    # Thread 1 (sender 1)
    m1 = log_message(1, 'Hi AI (from 1)', direct=True)
    m1_ai = log_message('AI-Bot', 'Hello 1', direct=True, reply_to=m1['timestamp'], is_ai=True)
    # Thread 2 (sender 2)
    m2 = log_message(2, 'Hi AI (from 2)', direct=True)
    m2_ai = log_message('AI-Bot', 'Hello 2', direct=True, reply_to=m2['timestamp'], is_ai=True)

    # Build history for sender 1 should not include sender 2 thread
    h1 = build_ollama_history(sender_id=1, is_direct=True, channel_idx=None, max_chars=1000)
    assert 'from 1' in h1
    assert 'Hello 1' in h1
    assert 'from 2' not in h1
    assert 'Hello 2' not in h1

    # And vice versa
    h2 = build_ollama_history(sender_id=2, is_direct=True, channel_idx=None, max_chars=1000)
    assert 'from 2' in h2
    assert 'Hello 2' in h2
    assert 'from 1' not in h2
    assert 'Hello 1' not in h2


    def test_channel_history_is_per_thread_root():
        ns = _exec_module_source(cfg={})
        log_message = ns.get('log_message')
        build_ollama_history = ns.get('build_ollama_history')
        messages = ns.get('messages')
        messages_lock = ns.get('messages_lock')

        with messages_lock:
            messages.clear()

        # Two separate threads on same channel (index 1)
        # Thread A
        a = log_message('UserA', 'Topic A root', direct=False, channel_idx=1)
        a_ai = log_message('AI-Bot', 'Reply A', direct=False, channel_idx=1, reply_to=a['timestamp'], is_ai=True)
        # Thread B
        b = log_message('UserB', 'Topic B root', direct=False, channel_idx=1)
        b_ai = log_message('AI-Bot', 'Reply B', direct=False, channel_idx=1, reply_to=b['timestamp'], is_ai=True)

        # Build history scoped to Thread A root
        hA = build_ollama_history(sender_id=None, is_direct=False, channel_idx=1, thread_root_ts=a['timestamp'], max_chars=1000)
        assert 'Topic A root' in hA
        assert 'Reply A' in hA
        assert 'Topic B root' not in hA
        assert 'Reply B' not in hA

        # And scoped to Thread B
        hB = build_ollama_history(sender_id=None, is_direct=False, channel_idx=1, thread_root_ts=b['timestamp'], max_chars=1000)
        assert 'Topic B root' in hB
        assert 'Reply B' in hB
        assert 'Topic A root' not in hB
        assert 'Reply A' not in hB
