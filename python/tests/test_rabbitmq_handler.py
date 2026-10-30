import os
import sys
import unittest
from unittest.mock import MagicMock, patch

# Asegura que la carpeta 'python' esté en el path de imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Si pika no está instalado en el entorno local (ej. solo está dentro de Docker),
# creamos stubs de sus excepciones y módulos en sys.modules para poder correr los tests unitarios.
try:
    import pika
    import pika.exceptions
    PIKA_INSTALLED = True
except ImportError:
    PIKA_INSTALLED = False

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
    import pika
    import pika.exceptions

from src.common.middleware.rabbitmq_handler import RabbitMQHandler
from src.common.middleware.middleware_handler import (
    MiddlewareDisconnectedError,
    MiddlewareMessageError,
)


class TestRabbitMQHandler(unittest.TestCase):

    def setUp(self):
        # Parcheamos pika.BlockingConnection para aislar completamente de la red
        self.pika_patcher = patch("src.common.middleware.rabbitmq_handler.pika.BlockingConnection")
        self.mock_connection_cls = self.pika_patcher.start()

        # Mocks para la conexión y el canal
        self.mock_connection = MagicMock()
        self.mock_channel = MagicMock()
        self.mock_connection_cls.return_value = self.mock_connection
        self.mock_connection.channel.return_value = self.mock_channel

        # Estado inicial abierto
        self.mock_connection.is_closed = False
        self.mock_channel.is_closed = False

        self.handler = RabbitMQHandler(host="test_host")

    def tearDown(self):
        self.pika_patcher.stop()

    # --- Inicialización ---

    def test_init_success(self):
        self.assertEqual(self.handler.host, "test_host")
        self.mock_connection_cls.assert_called_once()
        self.mock_connection.channel.assert_called_once()
        self.assertEqual(self.handler.queue_names, [])
        self.assertEqual(self.handler.exchange_names, {})

    @patch("src.common.middleware.rabbitmq_handler.pika.BlockingConnection")
    def test_init_connection_error_raises_disconnected(self, mock_conn):
        mock_conn.side_effect = pika.exceptions.AMQPConnectionError("Connection refused")
        with self.assertRaises(MiddlewareDisconnectedError):
            RabbitMQHandler(host="unreachable_host")

    @patch("src.common.middleware.rabbitmq_handler.pika.BlockingConnection")
    def test_init_channel_error_closes_connection(self, mock_conn_cls):
        mock_conn = MagicMock()
        mock_conn.channel.side_effect = pika.exceptions.AMQPError("Channel error")
        mock_conn_cls.return_value = mock_conn

        with self.assertRaises(MiddlewareMessageError):
            RabbitMQHandler(host="test_host")

        mock_conn.close.assert_called_once()

    # --- Suscripción a Colas ---

    def test_subscribe_to_queue_declares_and_consumes(self):
        dummy_callback = MagicMock()
        self.handler.subscribe_to_queue("my_test_queue", dummy_callback)

        self.mock_channel.queue_declare.assert_called_once_with(
            queue="my_test_queue",
            durable=True,
        )
        self.mock_channel.basic_consume.assert_called_once()
        self.assertIn("my_test_queue", self.handler.queue_names)

    def test_subscribe_to_queue_invalid_callback(self):
        with self.assertRaises(ValueError):
            self.handler.subscribe_to_queue("queue_x", None)

    # --- Suscripción a Exchanges ---

    def test_subscribe_to_exchange_single_key(self):
        dummy_callback = MagicMock()
        mock_decl_queue = MagicMock()
        mock_decl_queue.method.queue = "amq.gen-1234"
        self.mock_channel.queue_declare.return_value = mock_decl_queue

        actual_queue = self.handler.subscribe_to_exchange(
            exchange_name="test_exchange",
            routing_keys=["my_route"],
            callback=dummy_callback,
        )

        self.assertEqual(actual_queue, "amq.gen-1234")
        self.mock_channel.exchange_declare.assert_called_once_with(
            exchange="test_exchange",
            exchange_type="direct",
            durable=True,
        )
        self.mock_channel.queue_bind.assert_called_once_with(
            exchange="test_exchange",
            queue="amq.gen-1234",
            routing_key="my_route",
        )
        self.assertIn("test_exchange", self.handler.exchange_names)
        self.assertIn("amq.gen-1234", self.handler.queue_names)

    def test_subscribe_to_exchange_none_routing_keys(self):
        dummy_callback = MagicMock()
        mock_decl_queue = MagicMock()
        mock_decl_queue.method.queue = "amq.gen-5678"
        self.mock_channel.queue_declare.return_value = mock_decl_queue

        actual_queue = self.handler.subscribe_to_exchange(
            exchange_name="fanout_exchange",
            routing_keys=None,
            callback=dummy_callback,
            exchange_type="fanout",
        )

        self.assertEqual(actual_queue, "amq.gen-5678")
        self.mock_channel.queue_bind.assert_called_once_with(
            exchange="fanout_exchange",
            queue="amq.gen-5678",
            routing_key="",
        )

    # --- Envío de Mensajes (send) ---

    def test_send_direct_to_queue(self):
        payload = b"test_payload"
        self.handler.send("my_destination_queue", payload)

        self.mock_channel.basic_publish.assert_called_once_with(
            exchange="",
            routing_key="my_destination_queue",
            body=payload,
        )

    def test_send_with_multiple_routing_keys(self):
        payload = b"multi_route_payload"
        routes = ["route_a", "route_b", "route_c"]

        self.handler.send(
            destination="articles_exchange",
            payload=payload,
            routing_keys=routes,
        )

        self.assertEqual(self.mock_channel.basic_publish.call_count, 3)
        calls = self.mock_channel.basic_publish.call_args_list

        for i, route in enumerate(routes):
            _, kwargs = calls[i]
            self.assertEqual(kwargs["exchange"], "articles_exchange")
            self.assertEqual(kwargs["routing_key"], route)
            self.assertEqual(kwargs["body"], payload)

    def test_send_to_registered_exchange_without_explicit_keys(self):
        # Registramos un exchange previamente
        self.handler.exchange_names["my_ex"] = ["key_1", "key_2"]
        payload = b"exchange_data"

        self.handler.send(destination="my_ex", payload=payload)

        self.assertEqual(self.mock_channel.basic_publish.call_count, 2)
        calls = self.mock_channel.basic_publish.call_args_list
        self.assertEqual(calls[0][1]["routing_key"], "key_1")
        self.assertEqual(calls[1][1]["routing_key"], "key_2")

    def test_send_raises_disconnected_on_pika_error(self):
        self.mock_channel.basic_publish.side_effect = pika.exceptions.StreamLostError("Stream lost")

        with self.assertRaises(MiddlewareDisconnectedError):
            self.handler.send("any_queue", b"payload")

    # --- Broadcast ---

    def test_broadcast(self):
        payload = b"broadcast_payload"
        self.handler.broadcast("notifications", payload)

        self.mock_channel.exchange_declare.assert_called_once_with(
            exchange="notifications",
            exchange_type="fanout",
            durable=True,
        )
        self.mock_channel.basic_publish.assert_called_once_with(
            exchange="notifications",
            routing_key="",
            body=payload,
        )

    # --- Callbacks: Ack y Nack ---

    def test_callback_ack(self):
        received_data = []

        def sample_callback(body, ack, nack):
            received_data.append(body)
            ack()

        self.handler.subscribe_to_queue("q", sample_callback)

        # Extraer el wrapper
        _, kwargs = self.mock_channel.basic_consume.call_args
        wrapped_callback = kwargs["on_message_callback"]

        mock_method = MagicMock()
        mock_method.delivery_tag = 101
        wrapped_callback(self.mock_channel, mock_method, None, b"article_1")

        self.assertEqual(received_data, [b"article_1"])
        self.mock_channel.basic_ack.assert_called_once_with(delivery_tag=101)

    def test_callback_nack_requeue(self):
        def failing_callback(body, ack, nack):
            nack(requeue=True)

        self.handler.subscribe_to_queue("q", failing_callback)

        _, kwargs = self.mock_channel.basic_consume.call_args
        wrapped_callback = kwargs["on_message_callback"]

        mock_method = MagicMock()
        mock_method.delivery_tag = 202
        wrapped_callback(self.mock_channel, mock_method, None, b"bad_message")

        self.mock_channel.basic_nack.assert_called_once_with(delivery_tag=202, requeue=True)

    # --- Consumo: start_consuming & stop_consuming ---

    def test_start_consuming_without_queues_raises_error(self):
        with self.assertRaises(MiddlewareMessageError):
            self.handler.start_consuming()

    def test_start_consuming_closes_connection_on_exception(self):
        self.handler.queue_names.append("some_queue")
        self.mock_channel.start_consuming.side_effect = pika.exceptions.AMQPConnectionError("Broker died")

        with self.assertRaises(MiddlewareDisconnectedError):
            self.handler.start_consuming()

        self.assertFalse(self.handler.consuming)
        self.mock_connection.close.assert_called_once()

    def test_stop_consuming(self):
        self.handler.consuming = True
        self.mock_channel.is_open = True

        self.handler.stop_consuming()

        self.mock_channel.stop_consuming.assert_called_once()
        self.assertFalse(self.handler.consuming)

    # --- Cierre Limpio (close) ---

    def test_close_guarantees_connection_close_when_channel_fails(self):
        self.mock_channel.is_open = True
        self.mock_connection.is_open = True

        # Simulamos que cerrar el canal falla
        self.mock_channel.close.side_effect = pika.exceptions.ChannelWrongStateError("Channel already closing")

        self.handler.close()

        # Debe intentar cerrar ambos y terminar en None
        self.mock_channel.close.assert_called_once()
        self.mock_connection.close.assert_called_once()
        self.assertIsNone(self.handler.channel)
        self.assertIsNone(self.handler.connection)

    def test_close_idempotent(self):
        self.handler.close()
        # Segunda llamada consecutiva no debe fallar
        self.handler.close()
        self.assertIsNone(self.handler.channel)
        self.assertIsNone(self.handler.connection)

    # --- Test de callbacks según origen del mensaje ---

    def test_executes_correct_callback_according_to_message_source(self):
        # dos callbacks espías independientes
        callback_cola = MagicMock()
        callback_exchange = MagicMock()

        # Simulamos el nombre de la cola generada para el exchange
        mock_decl_queue = MagicMock()
        mock_decl_queue.method.queue = "amq.gen-cola-exchange"
        self.mock_channel.queue_declare.return_value = mock_decl_queue

        # Nos suscribimos a la cola y al exchange
        self.handler.subscribe_to_queue("mi_cola", callback_cola)
        self.handler.subscribe_to_exchange("mi_exchange", callback=callback_exchange)

        # 3. Mapeamos las colas a sus callbacks internos inspeccionando basic_consume
        # basic_consume fue llamado 2 veces, una para cada suscripción
        callbacks_por_cola = {}
        for call in self.mock_channel.basic_consume.call_args_list:
            cola = call.kwargs["queue"]
            cb_interno = call.kwargs["on_message_callback"]
            callbacks_por_cola[cola] = cb_interno

        # Verificamos que se hayan registrado ambas colas con callbacks distintos
        self.assertIn("mi_cola", callbacks_por_cola)
        self.assertIn("amq.gen-cola-exchange", callbacks_por_cola)
        self.assertIsNot(callbacks_por_cola["mi_cola"], callbacks_por_cola["amq.gen-cola-exchange"])

        # --- CASO A: Llega un mensaje a 'mi_cola' ---
        mock_method_1 = MagicMock(delivery_tag=1)
        callbacks_por_cola["mi_cola"](self.mock_channel, mock_method_1, None, b"mensaje_de_cola")

        # Verificación: solo debió ejecutarse callback_cola
        callback_cola.assert_called_once()
        self.assertEqual(callback_cola.call_args[0][0], b"mensaje_de_cola")
        callback_exchange.assert_not_called()

        # Limpiamos los contadores para la siguiente prueba
        callback_cola.reset_mock()
        callback_exchange.reset_mock()

        # --- CASO B: Llega un mensaje a la cola del exchange ---
        mock_method_2 = MagicMock(delivery_tag=2)
        callbacks_por_cola["amq.gen-cola-exchange"](self.mock_channel, mock_method_2, None, b"mensaje_de_exchange")

        # Verificación: solo debió ejecutarse callback_exchange
        callback_exchange.assert_called_once()
        self.assertEqual(callback_exchange.call_args[0][0], b"mensaje_de_exchange")
        callback_cola.assert_not_called()


if __name__ == "__main__":
    unittest.main()
