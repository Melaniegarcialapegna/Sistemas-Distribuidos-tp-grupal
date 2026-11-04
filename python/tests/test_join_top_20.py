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

from join_top_20.main import JoinTop20Worker
from common.word_item import WordItem
from common.message_protocol.external import Query


class TestJoinTop20Worker(unittest.TestCase):
    @patch("common.middleware.RabbitMQHandler")
    def setUp(self, mock_handler_cls):
        self.mock_output_handler = MagicMock()
        self.mock_input_handler = MagicMock()
        mock_handler_cls.side_effect = [self.mock_output_handler, self.mock_input_handler]

        self.worker = JoinTop20Worker(
            batch_size=10,
            host="localhost",
            input_exchange_name="top_ex",
            input_exchange_prefix="top_rk",
            replica_id="0",
            results_queue_name="final_top_20_queue",
            top_size=3,
            previous_stage_replicas=2,
        )

    def test_init_configures_queues_and_handlers(self):
        self.mock_output_handler.declare_queue.assert_called_once_with("final_top_20_queue")
        self.assertIn("final_top_20_queue", self.worker.output_queues)
        self.mock_input_handler.subscribe_to_exchange.assert_called_once_with(
            "top_ex", ["top_rk_0"], callback=self.worker._handle_message
        )

    def test_process_data_merges_and_keeps_top_size(self):
        # Primer lote parcial de una réplica
        batch1 = [[
            WordItem("apple", 10, 0),
            WordItem("banana", 5, 0),
        ]]
        self.worker.process_data(client_id=1, message=batch1)
        self.assertEqual([w.word for w in self.worker.data[1]], ["apple", "banana"])

        # Segundo lote parcial de otra réplica
        batch2 = [[
            WordItem("cherry", 15, 0),
            WordItem("date", 8, 0),
            WordItem("elderberry", 1, 0),
        ]]
        self.worker.process_data(client_id=1, message=batch2)

        # Con top_size = 3, debe conservar cherry (15), apple (10), date (8)
        self.assertEqual(len(self.worker.data[1]), 3)
        self.assertEqual([w.word for w in self.worker.data[1]], ["cherry", "apple", "date"])
        self.assertEqual([w.total_count() for w in self.worker.data[1]], [15, 10, 8])

    def test_on_barrier_emits_to_results_and_flushes(self):
        self.worker.data[1] = [
            WordItem("cherry", 15, 0),
            WordItem("apple", 10, 0),
        ]

        with patch.object(self.worker, "emit") as mock_emit, \
             patch.object(self.worker, "_flush_all") as mock_flush_all:

            self.worker._on_barrier(client_id=1)

            # Debe haber eliminado los datos del cliente 1
            self.assertNotIn(1, self.worker.data)

            # Debe haber emitido al results_queue
            mock_emit.assert_called_once()
            client_id, queue, payload = mock_emit.call_args[0]
            self.assertEqual(client_id, 1)
            self.assertEqual(queue, "final_top_20_queue")
            self.assertEqual(len(payload), 2)
            self.assertEqual(payload[0], Query.Q4)
            self.assertEqual([w.word for w in payload[1]], ["cherry", "apple"])

            # Debe haber llamado a _flush_all
            mock_flush_all.assert_called_once_with(1)

    def test_on_barrier_empty_data(self):
        with patch.object(self.worker, "emit") as mock_emit, \
             patch.object(self.worker, "_flush_all") as mock_flush_all:

            self.worker._on_barrier(client_id=42)
            mock_emit.assert_called_once_with(42, "final_top_20_queue", [Query.Q4, []])
            mock_flush_all.assert_called_once_with(42)


if __name__ == "__main__":
    unittest.main()
