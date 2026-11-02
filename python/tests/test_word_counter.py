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

from word_counter.main import WordCounterWorker
from common.word_item import WordItem
from common.utils import fnv1a_32


class TestWordCounterWorker(unittest.TestCase):
    @patch.dict(os.environ, {
        "NEXT_STAGE_REPLICAS": "2",
        "PREVIOUS_STAGE_REPLICAS": "1",
        "REPLICA_ID": "0"
    })
    @patch("common.middleware.RabbitMQHandler")
    def setUp(self, mock_handler_cls):
        self.mock_output_handler = MagicMock()
        self.mock_input_handler = MagicMock()
        mock_handler_cls.side_effect = [self.mock_output_handler, self.mock_input_handler]

        self.worker = WordCounterWorker(
            batch_size=10,
            host="localhost",
            input_exchange_name="words_ex",
            input_exchange_prefix="words",
            replica_id="0",
            results_queue_name="results_q3",
            top_exchange_name="top_ex",
            top_exchange_rk_prefix="top_rk",
            top_size=3
        )

    def test_init_configures_queues_and_exchanges(self):
        self.mock_output_handler.declare_queue.assert_called_once_with("results_q3")
        self.mock_output_handler.declare_exchange.assert_called_once_with("top_ex")
        self.assertIn("results_q3", self.worker.output_queues)
        self.assertEqual(
            self.worker.output_exchanges["top_ex"],
            ["top_rk_0", "top_rk_1"]
        )
        self.mock_input_handler.subscribe_to_exchange.assert_called_once_with(
            "words_ex", ["words_0"], callback=self.worker._handle_message
        )

    def test_process_data_accumulates_counts_without_intersection(self):
        with patch.object(self.worker, "emit") as mock_emit:
            self.worker.process_data(client_id=1, message=["apple", "en"])
            self.worker.process_data(client_id=1, message=["apple", "en"])
            mock_emit.assert_not_called()

            item, sent = self.worker.data[1]["apple"]
            self.assertEqual(item.eng_count, 2)
            self.assertEqual(item.fre_count, 0)
            self.assertFalse(sent)

    def test_process_data_emits_to_results_on_intersection_once(self):
        with patch.object(self.worker, "emit") as mock_emit:
            # Primero llega en inglés
            self.worker.process_data(client_id=1, message=["apple", "en"])
            mock_emit.assert_not_called()

            # Luego llega en francés -> intersection reached!
            self.worker.process_data(client_id=1, message=["apple", "fr"])
            mock_emit.assert_called_once_with(1, "results_q3", ["apple"])

            # Llega de nuevo en inglés -> no se re-emite porque ya fue sent
            mock_emit.reset_mock()
            self.worker.process_data(client_id=1, message=["apple", "en"])
            mock_emit.assert_not_called()

            item, sent = self.worker.data[1]["apple"]
            self.assertEqual(item.eng_count, 2)
            self.assertEqual(item.fre_count, 1)
            self.assertTrue(sent)

    def test_on_barrier_emits_top_k_and_flushes(self):
        # Cargamos datos para el cliente 1
        # apple: 5 (en=3, fr=2)
        # banana: 2 (en=2, fr=0)
        # cherry: 8 (en=4, fr=4)
        # date: 1 (en=0, fr=1)
        for _ in range(3): self.worker.process_data(1, ["apple", "en"])
        for _ in range(2): self.worker.process_data(1, ["apple", "fr"])
        for _ in range(2): self.worker.process_data(1, ["banana", "en"])
        for _ in range(4): self.worker.process_data(1, ["cherry", "en"])
        for _ in range(4): self.worker.process_data(1, ["cherry", "fr"])
        self.worker.process_data(1, ["date", "fr"])

        with patch.object(self.worker, "emit") as mock_emit, \
             patch.object(self.worker, "_flush_all") as mock_flush_all:

            self.worker._on_barrier(client_id=1)

            # Los datos del cliente 1 deben haberse eliminado del diccionario
            self.assertNotIn(1, self.worker.data)

            # Debe haberse emitido el top 3: cherry (8), apple (5), banana (2)
            mock_emit.assert_called_once()
            call_client_id, exchange, payload = mock_emit.call_args[0]
            routing_keys = mock_emit.call_args[1]["routing_keys"]

            self.assertEqual(call_client_id, 1)
            self.assertEqual(exchange, "top_ex")
            self.assertEqual(len(payload), 1)

            top_items = payload[0]
            self.assertEqual(len(top_items), 3)
            self.assertEqual([w.word for w in top_items], ["cherry", "apple", "banana"])
            self.assertEqual([w.total_count() for w in top_items], [8, 5, 2])

            expected_rk_index = fnv1a_32((1).to_bytes(1, "big")) % 2
            self.assertEqual(routing_keys, [f"top_rk_{expected_rk_index}"])

            mock_flush_all.assert_called_once_with(1)

    def test_on_barrier_empty_data(self):
        with patch.object(self.worker, "emit") as mock_emit, \
             patch.object(self.worker, "_flush_all") as mock_flush_all:

            self.worker._on_barrier(client_id=99)
            mock_emit.assert_called_once()
            self.assertEqual(mock_emit.call_args[0][2], [[]])
            mock_flush_all.assert_called_once_with(99)


if __name__ == "__main__":
    unittest.main()

