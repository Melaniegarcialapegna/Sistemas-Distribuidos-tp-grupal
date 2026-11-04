import os
import sys
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

from hasher.main import HasherWorker
from common.utils import fnv1a_32


class TestHasherWorker(unittest.TestCase):
    @patch("common.middleware.RabbitMQHandler")
    def setUp(self, mock_handler_cls):
        self.mock_output_handler = MagicMock()
        self.mock_input_handler = MagicMock()
        mock_handler_cls.side_effect = [self.mock_output_handler, self.mock_input_handler]

        self.worker = HasherWorker(
            batch_size=10,
            host="localhost",
            input_queue_name="input_queue",
            control_routing_key="ctrl_key",
            control_exchange_name="ctrl_ex",
            output_exchange_name="output_ex",
            output_routing_key_prefix="words",
            replica_id="test_replica",
            previous_stage_replicas=1,
            next_stage_replicas=3,
        )

    def test_init_creates_exchange_with_routing_keys(self):
        self.mock_output_handler.declare_exchange.assert_called_once_with("output_ex")
        self.assertEqual(
            self.worker.output_exchanges["output_ex"],
            ["words_0", "words_1", "words_2"],
        )

    def test_process_data_routes_by_hash(self):
        with patch.object(self.worker, "emit") as mock_emit:
            message = [["hello", "world"], "en"]
            self.worker.process_data(client_id=1, message=message)

            self.assertEqual(mock_emit.call_count, 2)

            for word in ["hello", "world"]:
                expected_index = fnv1a_32(word.encode("utf-8")) % 3
                expected_routing_key = f"words_{expected_index}"
                mock_emit.assert_any_call(
                    1,
                    "output_ex",
                    [word, "en"],
                    routing_keys=[expected_routing_key],
                )

    def test_fnv1a_32_deterministic(self):
        self.assertEqual(fnv1a_32(b"test"), fnv1a_32(b"test"))
        self.assertIsInstance(fnv1a_32(b"test"), int)


if __name__ == "__main__":
    unittest.main()
