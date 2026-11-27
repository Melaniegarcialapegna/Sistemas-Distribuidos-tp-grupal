import os
import sys
import unittest
from unittest.mock import MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

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

from common.heartbeat.heartbeat_sender import HeartbeatSender
from common.message_protocol.internal import TLVDeserializer
from common.message_protocol.msgtype import HeartbeatMessageType


class DummyMiddlewareHandler:
    def __init__(self):
        self.broadcasts = []
        self.scheduled_callbacks = []

    def broadcast(self, exchange, payload):
        self.broadcasts.append((exchange, payload))

    def call_later(self, delay, callback):
        self.scheduled_callbacks.append((delay, callback))


class TestHeartbeatSender(unittest.TestCase):
    def test_start_sends_heartbeat_and_schedules_next(self):
        handler = DummyMiddlewareHandler()
        sender = HeartbeatSender(
            middleware_handler=handler,
            service_name="test_worker_0",
            exchange_name="heartbeat_exchange",
            interval=2.0,
        )

        sender.start()

        # Debe haber emitido un broadcast
        self.assertEqual(len(handler.broadcasts), 1)
        exchange, payload = handler.broadcasts[0]
        self.assertEqual(exchange, "heartbeat_exchange")
        
        message = TLVDeserializer.deserialize_message(payload)
        self.assertIsInstance(message, HeartbeatMessageType)
        self.assertEqual(message.service_name, "test_worker_0")
        self.assertEqual(message.payload, ["test_worker_0"])

        # Debe haber agendado el siguiente llamado
        self.assertEqual(len(handler.scheduled_callbacks), 1)
        delay, callback = handler.scheduled_callbacks[0]
        self.assertEqual(delay, 2.0)
        self.assertEqual(callback, sender._send)

    def test_start_is_idempotent(self):
        handler = DummyMiddlewareHandler()
        sender = HeartbeatSender(
            middleware_handler=handler,
            service_name="test_worker_0",
            exchange_name="heartbeat_exchange",
            interval=1.0,
        )

        sender.start()
        sender.start()  # Segundo llamado debe ser ignorado

        self.assertEqual(len(handler.broadcasts), 1)
        self.assertEqual(len(handler.scheduled_callbacks), 1)

    def test_subsequent_ticks_send_heartbeats(self):
        handler = DummyMiddlewareHandler()
        sender = HeartbeatSender(
            middleware_handler=handler,
            service_name="test_service",
            exchange_name="hb_ex",
            interval=1.5,
        )

        sender.start()
        self.assertEqual(len(handler.broadcasts), 1)

        # Simular el disparo del timer ejecutando el callback
        _, next_tick = handler.scheduled_callbacks.pop(0)
        next_tick()

        self.assertEqual(len(handler.broadcasts), 2)
        self.assertEqual(len(handler.scheduled_callbacks), 1)
        self.assertEqual(handler.scheduled_callbacks[0][0], 1.5)

    def test_stop_prevents_further_heartbeats(self):
        handler = DummyMiddlewareHandler()
        sender = HeartbeatSender(
            middleware_handler=handler,
            service_name="test_service",
            exchange_name="hb_ex",
            interval=1.0,
        )

        sender.start()
        self.assertEqual(len(handler.broadcasts), 1)
        
        # Obtenemos el callback agendado
        _, next_tick = handler.scheduled_callbacks.pop(0)

        # Detenemos el sender
        sender.stop()

        # Si el timer de pika llegara a dispararse después del stop, no debe emitir
        next_tick()
        self.assertEqual(len(handler.broadcasts), 1)
        self.assertEqual(len(handler.scheduled_callbacks), 0)


if __name__ == "__main__":
    unittest.main()
