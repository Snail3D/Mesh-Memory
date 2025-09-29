#!/usr/bin/env python3
"""Lightweight smoke test for resend (no-ack) and stop/blacklist controls.

Runs without pytest and does not start Flask or require meshtastic installed.
"""
import sys
import time
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / 'mesh-master.py'


def load_module():
    # Ensure project root is importable
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    src = MODULE_PATH.read_text(encoding='utf-8')
    src_mod = src.replace("app = Flask(__name__)", "app = None  # patched by smoke test", 1)
    src_mod = src_mod.replace("@app.route", "@_no_op_route")
    ns = {'__name__': 'mesh_master_test_module', '__file__': str(MODULE_PATH)}

    def _no_op_route(*a, **k):
        def _decorator(f):
            return f
        return _decorator

    ns['_no_op_route'] = _no_op_route

    # Minimal stubs
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
    portnums_pb2_mod = types.ModuleType('meshtastic.portnums_pb2')
    # Stub protobuf modules used in dashboard radio page
    proto_pkg = types.ModuleType('meshtastic.protobuf')
    config_pb2_mod = types.ModuleType('meshtastic.protobuf.config_pb2')
    channel_pb2_mod = types.ModuleType('meshtastic.protobuf.channel_pb2')
    class _Channel:
        class Role:
            DISABLED = 0
            SECONDARY = 1
            @staticmethod
            def Name(v):
                return 'DISABLED' if v == 0 else 'SECONDARY'
    class _ChannelSettings:
        def __init__(self):
            self.name = ''
            self.psk = b''
            self.uplink_enabled = True
            self.downlink_enabled = True
            self.channel_num = 0
    channel_pb2_mod.Channel = _Channel
    channel_pb2_mod.ChannelSettings = _ChannelSettings
    proto_pkg.config_pb2 = config_pb2_mod
    proto_pkg.channel_pb2 = channel_pb2_mod
    # Stub util helpers
    util_mod = types.ModuleType('meshtastic.util')
    util_mod.pskToString = lambda psk: ('unencrypted' if not psk else 'secret')
    util_mod.fromPSK = lambda s: b''
    util_mod.genPSK256 = lambda: b'\x00' * 32
    pubsub_mod = types.ModuleType('pubsub')
    pub_obj = types.SimpleNamespace(subscribe=lambda *a, **k: None, unsubscribe=lambda *a, **k: None)
    pubsub_mod.pub = pub_obj
    unidecode_mod = types.ModuleType('unidecode')
    unidecode_mod.unidecode = lambda s: s
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
    sys.modules.setdefault('meshtastic.portnums_pb2', portnums_pb2_mod)
    sys.modules.setdefault('meshtastic.protobuf', proto_pkg)
    sys.modules.setdefault('meshtastic.protobuf.config_pb2', config_pb2_mod)
    sys.modules.setdefault('meshtastic.protobuf.channel_pb2', channel_pb2_mod)
    sys.modules.setdefault('meshtastic.util', util_mod)
    sys.modules.setdefault('pubsub', pubsub_mod)
    sys.modules.setdefault('pubsub.pub', pub_obj)
    sys.modules.setdefault('unidecode', unidecode_mod)
    sys.modules.setdefault('google.protobuf', google_protobuf)
    sys.modules.setdefault('google.protobuf.message', google_message)
    # Optional libraries used by games – provide minimal stub
    chess_mod = types.ModuleType('chess')
    chess_mod.PAWN = 1
    chess_mod.KNIGHT = 2
    chess_mod.BISHOP = 3
    chess_mod.ROOK = 4
    chess_mod.QUEEN = 5
    chess_mod.KING = 6
    chess_mod.WHITE = True
    chess_mod.BLACK = False
    class _FakeBoard:
        def __init__(self, *a, **k):
            pass
    chess_mod.Board = _FakeBoard
    class _FakeMove:
        def __init__(self, *a, **k):
            pass
    chess_mod.Move = _FakeMove
    sys.modules.setdefault('chess', chess_mod)
    exec(compile(src_mod, str(MODULE_PATH), 'exec'), ns)
    return ns


def check_resend(ns) -> None:
    ns['MAX_CHUNK_SIZE'] = 5
    ns['RESEND_ENABLED'] = True
    ns['RESEND_DM_ONLY'] = True
    ns['RESEND_USER_ATTEMPTS'] = 2
    ns['RESEND_USER_INTERVAL'] = 0.05
    ns['RESEND_JITTER_SECONDS'] = 0.0
    ns['RESEND_USAGE_THRESHOLD_PERCENT'] = 100.0

    class FakeInterface:
        def __init__(self):
            self.sent = []
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
    text = 'ABCDEFGHIJKLMNO'
    result = ns['send_direct_chunks'](iface, text, destinationId=123)
    chunks = result.get('chunks') or []
    acks = result.get('acks') or []
    assert acks.count(False) == 1, 'Expected one unacked chunk initially'
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
    time.sleep(0.2)
    # Expect more sends than initial 3 chunks
    assert len(iface.sent) > 3, f'Expected resend to add sends, got {len(iface.sent)}'
    # Optional: look for try marker
    found_try = any('try)' in p for (_d, p) in iface.sent)
    assert found_try, 'Expected a resend marker in payload'


def check_block_unblock(ns) -> None:
    parse = ns['parse_incoming_text']
    is_muted = ns['_is_user_muted']
    is_blocked = ns['_is_user_blocked']

    r = parse('stop', sender_id=7, is_direct=True, channel_idx=None)
    assert hasattr(r, 'text') and 'Paused' in r.text
    assert is_muted('7') or is_muted('!7')
    r2 = parse('resume', sender_id=7, is_direct=True, channel_idx=None)
    assert hasattr(r2, 'text') and ('Resumed' in r2.text or 'blocked' in r2.text)
    r3 = parse('blacklistme', sender_id=7, is_direct=True, channel_idx=None)
    assert hasattr(r3, 'text') and 'confirm' in r3.text.lower()
    r4 = parse('yes', sender_id=7, is_direct=True, channel_idx=None)
    assert hasattr(r4, 'text') and 'blocked' in r4.text
    assert is_blocked('7') or is_blocked('!7')
    r5 = parse('unblock', sender_id=7, is_direct=True, channel_idx=None)
    assert hasattr(r5, 'text') and 'Unblocked' in r5.text
    assert not (is_blocked('7') or is_blocked('!7'))


def main():
    ns = load_module()
    check_resend(ns)
    check_block_unblock(ns)
    print('Smoke test passed.')


if __name__ == '__main__':
    main()
