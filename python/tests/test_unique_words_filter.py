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

from unique_words_filter.main import UniqueWordsFilterWorker


class TestUniqueWordsFilterWorker(unittest.TestCase):
    @patch("common.middleware.RabbitMQHandler")
    def setUp(self, mock_handler_cls):
        self.mock_output_handler = MagicMock()
        self.mock_input_handler = MagicMock()
        mock_handler_cls.side_effect = [self.mock_output_handler, self.mock_input_handler]

        self.worker = UniqueWordsFilterWorker(
            batch_size=10,
            host="localhost",
            input_queue_name="input_queue",
            control_routing_key="ctrl_key",
            control_exchange_name="ctrl_ex",
            q3_4_output_queue_name="q3_4_queue",
            replica_id="test_replica",
            previous_stage_replicas=1,
        )

    def test_init_configures_queues(self):
        self.assertIn("q3_4_queue", self.worker.output_queues)
        self.mock_input_handler.subscribe_to_queue.assert_called_once_with(
            "input_queue", self.worker._handle_message
        )
        self.mock_input_handler.subscribe_to_exchange.assert_called_once_with(
            "ctrl_ex", ["ctrl_key"], callback=self.worker._handle_message
        )

    def test_process_data_deduplicates_words(self):
        with patch.object(self.worker, "emit") as mock_emit:
            message = [["apple", "banana", "apple", "cherry", "banana"], "en"]
            self.worker.process_data(client_id=1, message=message)

            mock_emit.assert_called_once()
            client_id, queue, payload = mock_emit.call_args[0]
            self.assertEqual(client_id, 1)
            self.assertEqual(queue, "q3_4_queue")
            words, lang = payload
            self.assertEqual(lang, "en")
            self.assertEqual(set(words), {"apple", "banana", "cherry"})

    def test_process_data_empty_list(self):
        with patch.object(self.worker, "emit") as mock_emit:
            message = [[], "fr"]
            self.worker.process_data(client_id=1, message=message)
            mock_emit.assert_not_called()

