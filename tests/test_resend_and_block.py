import sys
import time
import types
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / 'mesh-master.py'


def _exec_module_source():
    """Execute mesh-master.py in an isolated namespace suitable for unit tests.

    Patches Flask route decorators and stubs third-party modules to avoid side effects.
    """
    src = MODULE_PATH.read_text(encoding='utf-8')
    # Patch out Flask app creation and route decorators so we don't start the server
    src_mod = src.replace("app = Flask(__name__)", "app = None  # patched by test harness", 1)
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

    exec(compile(src_mod, str(MODULE_PATH), 'exec'), ns)
    return ns


def test_partial_resend_dm_only():
    ns = _exec_module_source()

    # Make chunks small so we can split into 3 parts
    ns['MAX_CHUNK_SIZE'] = 5
    ns['RESEND_ENABLED'] = True
    ns['RESEND_DM_ONLY'] = True
    ns['RESEND_USER_ATTEMPTS'] = 2
    ns['RESEND_USER_INTERVAL'] = 0.05
    ns['RESEND_JITTER_SECONDS'] = 0.0
    ns['RESEND_USAGE_THRESHOLD_PERCENT'] = 100.0

    # Fake interface that simulates per-chunk ack outcomes
    class FakeInterface:
        def __init__(self):
            self.sent = []  # record payloads
            # First call sequence: True, False, True (so middle chunk fails)
            self._ack_seq = [True, False, True]
        def sendDirectText(self, destinationId, text, wantAck=True):
            self.sent.append((destinationId, text))
        def waitForAckNak(self):
            if self._ack_seq:
                ok = self._ack_seq.pop(0)
            else:
                ok = True
            if not ok:
                raise RuntimeError('timeout')

    iface = FakeInterface()
    text = 'ABCDEFGHIJKLMNO'  # 15 chars -> 3 chunks of 5
    # Initial send with one failed chunk
    result = ns['send_direct_chunks'](iface, text, destinationId=123)
    assert result.get('sent') is True
    chunks = result.get('chunks')
    acks = result.get('acks')
    assert len(chunks) == 3
    assert acks.count(False) == 1

    # Schedule partial resend (only failed chunk)
    ns['RESEND_MANAGER'].schedule_dm_resend(
        interface_ref=iface,
        destination_id=123,
        text=text,
        chunks=chunks,
        acks=acks,
        attempts=2,
        interval_seconds=0.05,
        sender_key='!user',
        is_user_dm=True,
    )

    # Allow background thread to run one attempt
    time.sleep(0.2)

    # The last resent payload should have the suffix " (2nd try)" appended to the failed chunk
    resent_payloads = [p for (_dest, p) in iface.sent if str(p).endswith('try)')]
    assert resent_payloads, 'Expected a resent payload with retry marker'


def test_stop_blacklist_unblock_flow():
    ns = _exec_module_source()
    parse = ns['parse_incoming_text']
    is_muted = ns['_is_user_muted']
    is_blocked = ns['_is_user_blocked']

    # Stop mutes user
    r = parse('stop', sender_id=7, is_direct=True, channel_idx=None)
    assert hasattr(r, 'text') and 'Paused' in r.text
    assert is_muted('7') is True or is_muted('!7') is True

    # Resume unmutes (but not if blocked)
    r2 = parse('resume', sender_id=7, is_direct=True, channel_idx=None)
    assert hasattr(r2, 'text') and 'Resumed' in r2.text

    # Blacklist requires confirmation first
    r3 = parse('blacklistme', sender_id=7, is_direct=True, channel_idx=None)
    assert hasattr(r3, 'text') and 'confirm' in r3.text.lower()
    # Confirm
    r4 = parse('yes', sender_id=7, is_direct=True, channel_idx=None)
    assert hasattr(r4, 'text') and 'blocked' in r4.text
    assert is_blocked('7') is True or is_blocked('!7') is True

    # Unblock restores
    r5 = parse('unblock', sender_id=7, is_direct=True, channel_idx=None)
    assert hasattr(r5, 'text') and 'Unblocked' in r5.text
    assert (is_blocked('7') or is_blocked('!7')) is False


def test_broadcast_resend_respects_dm_only():
    ns = _exec_module_source()
    ns['RESEND_ENABLED'] = True
    ns['RESEND_DM_ONLY'] = True  # DM-only should prevent broadcast resends
    ns['RESEND_BROADCAST_ENABLED'] = True
    ns['RESEND_SYSTEM_ATTEMPTS'] = 2
    ns['RESEND_SYSTEM_INTERVAL'] = 0.05
    ns['RESEND_JITTER_SECONDS'] = 0.0
    ns['RESEND_USAGE_THRESHOLD_PERCENT'] = 100.0

    class FakeInterface:
        def __init__(self):
            self.sent = []
        def sendText(self, text, destinationId=None, channelIndex=None, wantAck=False):
            self.sent.append((destinationId, channelIndex, text))

    iface = FakeInterface()
    text = 'hello world'
    ns['send_broadcast_chunks'](iface, text, channelIndex=1)
    # Try to schedule broadcast resends; should no-op under DM-only
    ns['RESEND_MANAGER'].schedule_broadcast_resend(
        interface_ref=iface,
        channel_idx=1,
        text=text,
        attempts=2,
        interval_seconds=0.05,
    )
    # Wait briefly and ensure no additional payloads beyond initial send
    import time as _t
    _t.sleep(0.2)
    assert len(iface.sent) == 1, f"Expected no broadcast resends when DM-only, got {len(iface.sent)}"


def test_broadcast_resend_enabled_sends_suffix():
    ns = _exec_module_source()
    ns['RESEND_ENABLED'] = True
    ns['RESEND_DM_ONLY'] = False
    ns['RESEND_BROADCAST_ENABLED'] = True
    ns['RESEND_SYSTEM_ATTEMPTS'] = 2
    ns['RESEND_SYSTEM_INTERVAL'] = 0.05
    ns['RESEND_JITTER_SECONDS'] = 0.0
    ns['RESEND_USAGE_THRESHOLD_PERCENT'] = 100.0

    class FakeInterface:
        def __init__(self):
            self.sent = []
        def sendText(self, text, destinationId=None, channelIndex=None, wantAck=False):
            self.sent.append((destinationId, channelIndex, text))

    iface = FakeInterface()
    text = 'broadcast message'
    ns['send_broadcast_chunks'](iface, text, channelIndex=2)
    ns['RESEND_MANAGER'].schedule_broadcast_resend(
        interface_ref=iface,
        channel_idx=2,
        text=text,
        attempts=2,
        interval_seconds=0.05,
    )
    import time as _t
    _t.sleep(0.25)
    # Expect >1 sends and a try marker in one of the payloads
    assert len(iface.sent) > 1
    assert any('try)' in payload for (_d, _c, payload) in iface.sent)
