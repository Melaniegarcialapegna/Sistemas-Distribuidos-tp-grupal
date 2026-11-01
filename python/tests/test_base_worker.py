import os
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src/common")))

try:
    import pika
    import pika.exceptions
except ImportError:
    mock_pika = MagicMock()
    mock_pika_exceptions = MagicMock()
    mock_pika.exceptions = mock_pika_exceptions
    sys.modules["pika"] = mock_pika
    sys.modules["pika.exceptions"] = mock_pika_exceptions

try:
    from src.client import USABLE_COLUMNS, ROW_BATCH_SIZE
except (ImportError, ModuleNotFoundError):
    mock_client = MagicMock()
    mock_client.USABLE_COLUMNS = ["name", "url", "date_modified", "sections", "image", "abstract"]
    mock_client.ROW_BATCH_SIZE = 1000
    sys.modules["src.client"] = mock_client

from src.common.base_worker.base_worker import BaseWorker


class ConcreteWorker(BaseWorker):
    def __init__(self, batch_size=3, host="localhost"):
        super().__init__(batch_size=batch_size, host=host)

    def process_data(self, client_id, payload):
        pass


class TestBaseWorker(unittest.TestCase):
    @patch("src.common.base_worker.base_worker.middleware.RabbitMQHandler")
    def setUp(self, mock_handler_cls):
        self.mock_output_handler = MagicMock()
        mock_handler_cls.return_value = self.mock_output_handler
        self.worker = ConcreteWorker(batch_size=3)

    def test_emit_to_queue_batches_and_flushes(self):
        client_id = 1
        queue_name = "output_queue"
        self.worker._create_output_queue(queue_name)

        self.worker.emit(client_id, queue_name, ["item1"])
        self.worker.emit(client_id, queue_name, ["item2"])
        self.mock_output_handler.send.assert_not_called()

        self.worker.emit(client_id, queue_name, ["item3"])
        self.assertEqual(self.mock_output_handler.send.call_count, 1)

        dest = self.mock_output_handler.send.call_args[0][0]
        kwargs = self.mock_output_handler.send.call_args[1]
        self.assertEqual(dest, queue_name)
        self.assertIsNone(kwargs.get("routing_keys"))

    def test_emit_to_exchange_batches_per_routing_key(self):
        client_id = 1
        exchange_name = "data_exchange"
        self.worker._create_output_exchange(exchange_name, ["rk_a", "rk_b"])

        self.worker.emit(client_id, exchange_name, ["item_a1"], routing_keys=["rk_a"])
        self.worker.emit(client_id, exchange_name, ["item_b1"], routing_keys=["rk_b"])
        self.worker.emit(client_id, exchange_name, ["item_a2"], routing_keys=["rk_a"])
        self.mock_output_handler.send.assert_not_called()

        # El 3er elemento para rk_a alcanza batch_size y se flushea solo rk_a
        self.worker.emit(client_id, exchange_name, ["item_a3"], routing_keys=["rk_a"])
        self.assertEqual(self.mock_output_handler.send.call_count, 1)

        dest = self.mock_output_handler.send.call_args[0][0]
        kwargs = self.mock_output_handler.send.call_args[1]
        self.assertEqual(dest, exchange_name)
        self.assertEqual(kwargs.get("routing_keys"), ["rk_a"])

    def test_flush_all_sends_eof_to_queues_and_exchanges(self):
        client_id = 1
        queue_name = "q1"
        exchange_name = "ex1"
        self.worker._create_output_queue(queue_name)
        self.worker._create_output_exchange(exchange_name, ["rk1", "rk2"])

        # Enviamos 2 a queue (no llega a batch_size=3)
        self.worker.emit(client_id, queue_name, ["q_item1"])
        self.worker.emit(client_id, queue_name, ["q_item2"])

        # Enviamos 1 a rk1, 0 a rk2
        self.worker.emit(client_id, exchange_name, ["ex_item1"], routing_keys=["rk1"])

        self.mock_output_handler.reset_mock()
        self.worker._flush_all(client_id)

        # q1: 1 batch restante + 1 EOF = 2 llamadas
        # rk1: 1 batch restante + 1 EOF = 2 llamadas
        # rk2: 0 batch + 1 EOF = 1 llamada
        # Total send calls: 5
        self.assertEqual(self.mock_output_handler.send.call_count, 5)

        # Verificamos llamadas de queue_name
        queue_calls = [c for c in self.mock_output_handler.send.call_args_list if c[0][0] == queue_name]
        self.assertEqual(len(queue_calls), 2)

        # Verificamos llamadas de exchange_name
        exchange_calls = [c for c in self.mock_output_handler.send.call_args_list if c[0][0] == exchange_name]
        self.assertEqual(len(exchange_calls), 3)

        rk1_calls = [c for c in exchange_calls if c[1].get("routing_keys") == ["rk1"]]
        rk2_calls = [c for c in exchange_calls if c[1].get("routing_keys") == ["rk2"]]
        self.assertEqual(len(rk1_calls), 2)  # batch + EOF
        self.assertEqual(len(rk2_calls), 1)  # EOF
