import os
import sys
import time
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

# Mock pika if not installed locally
try:
    import pika
    import pika.exceptions
except ImportError:
    class AMQPError(Exception): pass
    class AMQPConnectionError(AMQPError): pass
    class ConnectionClosed(AMQPConnectionError): pass
    class ConnectionClosedByBroker(ConnectionClosed): pass
    class StreamLostError(AMQPConnectionError): pass
    class ChannelWrongStateError(AMQPError): pass

    mock_pika = MagicMock()
    mock_pika_exceptions = MagicMock()
    mock_pika_exceptions.AMQPError = AMQPError
    mock_pika_exceptions.AMQPConnectionError = AMQPConnectionError
    mock_pika_exceptions.ConnectionClosed = ConnectionClosed
    mock_pika_exceptions.ConnectionClosedByBroker = ConnectionClosedByBroker
    mock_pika_exceptions.StreamLostError = StreamLostError
    mock_pika_exceptions.ChannelWrongStateError = ChannelWrongStateError

    mock_pika.exceptions = mock_pika_exceptions
    sys.modules["pika"] = mock_pika
    sys.modules["pika.exceptions"] = mock_pika_exceptions

# Configuración de variables de entorno requeridas para el módulo watchdog.main
REQUIRED_ENV_VARS = {
    "RABBITMQ_HOST": "localhost",
    "HEARTBEAT_INTERVAL": "1.0",
    "HEARTBEAT_TIMEOUT": "3.0",
    "INITIAL_GRACE_PERIOD": "5.0",
    "REVIVE_GRACE_PERIOD": "5.0",
    "WATCHLIST": "worker_0,worker_1",
    "REPLICA_ID": "0",
    "TOTAL_WATCHDOGS": "3",
    "ELECTION_TIMEOUT": "1.0",
    "HEARTBEAT_EXCHANGE_NAME": "heartbeat_exchange",
    "WATCHDOG_EXCHANGE_NAME": "watchdog_exchange",
}
for key, val in REQUIRED_ENV_VARS.items():
    os.environ.setdefault(key, val)

from watchdog.main import Watchdog
from common.message_protocol.internal import TLVDeserializer, TLVSerializer
from common.message_protocol.msgtype import BullyMessageType, HeartbeatMessageType
from common.utils import get_watchdog_config


class DummyMiddlewareHandler:
    def __init__(self):
        self.subscribed_exchanges = []
        self.scheduled_callbacks = []
        self.sent_messages = []
        self.closed = False
        self.started_consuming = False

    def subscribe_to_exchange(self, exchange_name, routing_keys, callback, exchange_type="direct", queue_name=None):
        self.subscribed_exchanges.append({
            "exchange": exchange_name,
            "routing_keys": routing_keys,
            "callback": callback,
            "type": exchange_type,
            "queue_name": queue_name,
        })
        return queue_name

    def call_later(self, delay, callback):
        self.scheduled_callbacks.append((delay, callback))

    def send(self, destination, payload, routing_keys=None):
        self.sent_messages.append((destination, payload, routing_keys))

    def broadcast(self, exchange, payload):
        self.sent_messages.append((exchange, payload, [""]))

    def start_consuming(self):
        self.started_consuming = True

    def close(self):
        self.closed = True


class TestWatchdog(unittest.TestCase):
    def setUp(self):
        self.handler = DummyMiddlewareHandler()
        self.mock_docker = MagicMock()
        self.mock_container = MagicMock()
        self.mock_docker.containers.get.return_value = self.mock_container

    def _create_watchdog(
        self,
        replica_id=0,
        total_watchdogs=3,
        heartbeat_interval=1.0,
        heartbeat_timeout=3.0,
        election_timeout=1.0,
        watchlist=None,
        initial_grace_period=5.0,
        revive_grace_period=5.0,
    ):
        return Watchdog(
            rabbitmq_host="localhost",
            heartbeat_interval=heartbeat_interval,
            heartbeat_timeout=heartbeat_timeout,
            watchlist=watchlist if watchlist is not None else ["worker_0"],
            replica_id=replica_id,
            total_watchdogs=total_watchdogs,
            election_timeout=election_timeout,
            heartbeat_exchange_name="hb_ex",
            watchdog_exchange_name="wd_ex",
            initial_grace_period=initial_grace_period,
            revive_grace_period=revive_grace_period,
            middleware_handler=self.handler,
            docker_client=self.mock_docker,
        )

    def test_start_subscribes_to_both_exchanges(self):
        wd = self._create_watchdog(replica_id=1, total_watchdogs=3)
        wd.start()

        # Debe haberse suscripto al exchange fanout de heartbeats y al direct de control
        self.assertEqual(len(self.handler.subscribed_exchanges), 2)

        hb_sub = self.handler.subscribed_exchanges[0]
        self.assertEqual(hb_sub["exchange"], "hb_ex")
        self.assertEqual(hb_sub["type"], "fanout")
        self.assertEqual(hb_sub["queue_name"], "watchdog_hb_1")

        ctrl_sub = self.handler.subscribed_exchanges[1]
        self.assertEqual(ctrl_sub["exchange"], "wd_ex")
        self.assertEqual(ctrl_sub["type"], "direct")
        self.assertEqual(ctrl_sub["queue_name"], "watchdog_ctrl_1")
        self.assertEqual(ctrl_sub["routing_keys"], ["watchdog_1", "watchdog_broadcast"])

    def test_startup_highest_id_becomes_leader_directly(self):
        # Replica 2 es la mayor (total_watchdogs=3)
        wd = self._create_watchdog(replica_id=2, total_watchdogs=3)
        wd.start_election()

        # Al ser el mayor ID, gana directamente sin esperar
        self.assertTrue(wd.is_leader)
        self.assertEqual(wd.leader_id, 2)

        # Debe haber emitido COORDINATOR a watchdog_broadcast
        self.assertTrue(len(self.handler.sent_messages) > 0)
        dest, payload, rks = self.handler.sent_messages[-1]
        self.assertEqual(dest, "wd_ex")
        self.assertEqual(rks, ["watchdog_broadcast"])

        msg = TLVDeserializer.deserialize_message(payload)
        self.assertIsInstance(msg, BullyMessageType)
        self.assertEqual(msg.action, "COORDINATOR")
        self.assertEqual(msg.sender_id, 2)

    def test_startup_lower_id_sends_election_and_becomes_leader_on_timeout(self):
        # Replica 1 inicia elección; tiene a replica 2 por encima
        wd = self._create_watchdog(replica_id=1, total_watchdogs=3, election_timeout=1.0)
        wd.start_election()

        self.assertTrue(wd.is_electing)
        self.assertFalse(wd.is_leader)

        # Debe haber enviado ELECTION con routing_key "watchdog_2"
        election_sent = False
        for dest, payload, rks in self.handler.sent_messages:
            if dest == "wd_ex" and rks == ["watchdog_2"]:
                msg = TLVDeserializer.deserialize_message(payload)
                if msg.action == "ELECTION" and msg.sender_id == 1:
                    election_sent = True
                    break
        self.assertTrue(election_sent)

        # Disparamos el timer de timeout de elección (nadie respondió OK)
        _, timeout_cb = self.handler.scheduled_callbacks[-1]
        timeout_cb()

        # Replica 1 se proclama líder
        self.assertTrue(wd.is_leader)
        self.assertEqual(wd.leader_id, 1)

        # Y revive al nodo 2 que estaba caído
        self.mock_docker.containers.get.assert_called_with("watchdog_2")
        self.mock_container.restart.assert_called()

    def test_election_message_from_lower_id_replies_ok_and_starts_election(self):
        # Replica 1 recibe ELECTION de replica 0
        wd = self._create_watchdog(replica_id=1, total_watchdogs=3)

        election_msg = BullyMessageType(payload=["ELECTION", 0])
        payload = TLVSerializer.serialize_message(election_msg)

        mock_ack = MagicMock()
        wd._handle_election_message(payload, ack=mock_ack)

        mock_ack.assert_called_once()

        # Debe haber respondido OK a watchdog_0
        ok_sent = False
        for dest, p, rks in self.handler.sent_messages:
            if dest == "wd_ex" and rks == ["watchdog_0"]:
                msg = TLVDeserializer.deserialize_message(p)
                if msg.action == "OK" and msg.sender_id == 1:
                    ok_sent = True
                    break
        self.assertTrue(ok_sent)

        # Y Replica 1 debió iniciar su propia elección hacia Replica 2
        self.assertTrue(wd.is_electing)

    def test_ok_message_received_marks_ok_received(self):
        wd = self._create_watchdog(replica_id=0, total_watchdogs=3)
        wd.is_electing = True
        wd.ok_received = False

        ok_msg = BullyMessageType(payload=["OK", 1])
        payload = TLVSerializer.serialize_message(ok_msg)

        wd._handle_election_message(payload)

        self.assertTrue(wd.ok_received)

    def test_coordinator_announcement_sets_leader_and_updates_role(self):
        wd = self._create_watchdog(replica_id=0, total_watchdogs=3)
        wd.is_electing = True
        wd.is_leader = True  # simulamos estado previo

        coord_msg = BullyMessageType(payload=["COORDINATOR", 2])
        payload = TLVSerializer.serialize_message(coord_msg)

        wd._handle_election_message(payload)

        self.assertFalse(wd.is_electing)
        self.assertFalse(wd.is_leader)
        self.assertEqual(wd.leader_id, 2)

    def test_lower_id_claiming_coordinator_triggers_bully_election(self):
        # Replica 2 recibe COORDINATOR de replica 1 (menor ID)
        wd = self._create_watchdog(replica_id=2, total_watchdogs=3)

        coord_msg = BullyMessageType(payload=["COORDINATOR", 1])
        payload = TLVSerializer.serialize_message(coord_msg)

        wd._handle_election_message(payload)

        # Replica 2 no lo acepta, gana de inmediato y se proclama líder
        self.assertTrue(wd.is_leader)
        self.assertEqual(wd.leader_id, 2)

    def test_leader_heartbeat_timeout_triggers_election(self):
        # Replica 0 tiene a replica 2 como líder
        wd = self._create_watchdog(
            replica_id=0,
            total_watchdogs=3,
            heartbeat_timeout=2.0,
            initial_grace_period=0.0,
        )
        wd.leader_id = 2

        # Simulamos que pasaron 5s sin heartbeats del líder
        wd.received_heartbeats["watchdog_2"] = time.time() - 5.0

        wd._check_timeouts()

        # Debe haber disparado una elección
        self.assertTrue(wd.is_electing)

    def test_non_leader_death_revived_by_leader(self):
        # Replica 2 es el líder
        wd = self._create_watchdog(
            replica_id=2,
            total_watchdogs=3,
            heartbeat_timeout=2.0,
            initial_grace_period=0.0,
        )
        wd.is_leader = True
        wd.leader_id = 2

        # Replica 0 deja de emitir latidos
        wd.received_heartbeats["watchdog_0"] = time.time() - 5.0

        wd._check_timeouts()

        # Como es líder, revive a watchdog_0
        self.mock_docker.containers.get.assert_called_with("watchdog_0")
        self.mock_container.restart.assert_called()

    def test_handle_heartbeat_updates_timestamp_and_acks(self):
        wd = self._create_watchdog(watchlist=["worker_0"], initial_grace_period=0.0)

        old_timestamp = wd.received_heartbeats.get("worker_0")
        time.sleep(0.01)

        heartbeat = HeartbeatMessageType(payload=["worker_0"])
        payload = TLVSerializer.serialize_message(heartbeat)
        mock_ack = MagicMock()

        wd._handle_heartbeat(payload, ack=mock_ack)

        new_timestamp = wd.received_heartbeats["worker_0"]
        self.assertGreater(new_timestamp, old_timestamp)
        mock_ack.assert_called_once()

    def test_missing_environment_variable_raises_key_error(self):
        for required_key in REQUIRED_ENV_VARS.keys():
            partial_env = {k: v for k, v in REQUIRED_ENV_VARS.items() if k != required_key}
            with patch.dict(os.environ, partial_env, clear=True):
                with self.assertRaises(KeyError):
                    get_watchdog_config()


if __name__ == "__main__":
    unittest.main()
