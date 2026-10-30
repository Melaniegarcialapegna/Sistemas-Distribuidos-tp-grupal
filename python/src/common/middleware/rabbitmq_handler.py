import logging
from typing import Callable, Dict, List, Optional
import pika
import pika.exceptions

from .middleware_handler import (
    MiddlewareHandler,
    MiddlewareMessageError,
    MiddlewareDisconnectedError,
    MiddlewareCloseError,
)

logger = logging.getLogger(__name__)


class RabbitMQHandler(MiddlewareHandler):
    """
    Implementación de MiddlewareHandler sobre RabbitMQ utilizando la biblioteca pika.
    Permite suscripción y consumo concurrente de múltiples colas en una misma conexión y canal,
    así como publicación a colas, exchanges directos y broadcasts.
    """

    PIKA_DISCONNECTION_ERRORS = (
        pika.exceptions.AMQPConnectionError,
        pika.exceptions.ConnectionClosed,
        pika.exceptions.ConnectionClosedByBroker,
        pika.exceptions.StreamLostError,
    )

    def __init__(
        self,
        host: str = "localhost",
        connection_params: Optional[pika.ConnectionParameters] = None,
    ):
        """
        Inicializa la conexión y canal con RabbitMQ.

        :param host: Dirección del broker RabbitMQ.
        :param connection_params: Instancia opcional de pika.ConnectionParameters.
        """
        self.host = host
        self.consuming: bool = False
        self.queue_names: List[str] = []
        self.exchange_names: Dict[str, List[str]] = {}
        self.connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[pika.adapters.blocking_connection.BlockingChannel] = None

        try:
            params = connection_params or pika.ConnectionParameters(host=host)
            self.connection = pika.BlockingConnection(params)
            self.channel = self.connection.channel()
        except self.PIKA_DISCONNECTION_ERRORS as error:
            logger.error(f"Error connecting to RabbitMQ broker at '{self.host}': {error}")
            self.close()
            raise MiddlewareDisconnectedError(str(error)) from error
        except Exception as error:
            logger.error(f"Error initializing RabbitMQ handler: {error}")
            self.close()
            raise MiddlewareMessageError(str(error)) from error

    def _ensure_connected(self) -> None:
        """Verifica que la conexión y el canal sigan abiertos."""
        if not self.connection or self.connection.is_closed:
            raise MiddlewareDisconnectedError("RabbitMQ connection is closed")
        if not self.channel or self.channel.is_closed:
            raise MiddlewareDisconnectedError("RabbitMQ channel is closed")

    def _wrap_callback(self, callback: Callable) -> Callable:
        """
        Adapta el callback de pika (channel, method, properties, body) al formato
        del middleware callback(body, ack, nack).
        """
        def _on_message_callback_internal(channel, method, properties, body):
            def ack():
                try:
                    channel.basic_ack(delivery_tag=method.delivery_tag)
                except self.PIKA_DISCONNECTION_ERRORS as err:
                    raise MiddlewareDisconnectedError(str(err)) from err
                except pika.exceptions.AMQPError as err:
                    raise MiddlewareMessageError(str(err)) from err

            def nack(requeue: bool = True):
                try:
                    channel.basic_nack(delivery_tag=method.delivery_tag, requeue=requeue)
                except self.PIKA_DISCONNECTION_ERRORS as err:
                    raise MiddlewareDisconnectedError(str(err)) from err
                except pika.exceptions.AMQPError as err:
                    raise MiddlewareMessageError(str(err)) from err

            callback(body, ack, nack)

        return _on_message_callback_internal

    def subscribe_to_queue(
        self,
        queue_name: str,
        callback: Callable
    ) -> None:
        """
        Suscribe el middleware a una cola con nombre asociándole su función de callback específica.
        Si la cola no existe, la declara como durable.
        Puede invocarse múltiples veces para escuchar múltiples colas, cada una con su propio callback.
        """
        self._ensure_connected()
        if callback is None or not callable(callback):
            raise ValueError(f"A valid callback function must be provided for queue '{queue_name}'")

        try:
            self.channel.queue_declare(queue=queue_name, durable=True)
            if queue_name not in self.queue_names:
                self.queue_names.append(queue_name)

            self.channel.basic_consume(
                queue=queue_name,
                on_message_callback=self._wrap_callback(callback)
            )
        except self.PIKA_DISCONNECTION_ERRORS as error:
            raise MiddlewareDisconnectedError(str(error)) from error
        except pika.exceptions.AMQPError as error:
            raise MiddlewareMessageError(str(error)) from error

    def subscribe_to_exchange(
        self,
        exchange_name: str,
        routing_keys: Optional[List[str]] = None,
        callback: Callable = None,
        exchange_type: str = "direct",
    ) -> str:
        """
        Suscribe el middleware a un exchange asociándole su función de callback específica.
        Declara el exchange y vincula una cola exclusiva, conectándola directamente al callback.
        """
        self._ensure_connected()
        if callback is None or not callable(callback):
            raise ValueError(f"A valid callback function must be provided for exchange '{exchange_name}'")

        r_keys = routing_keys if routing_keys is not None else [""]

        try:
            self.channel.exchange_declare(exchange=exchange_name, exchange_type=exchange_type, durable=True)
            if exchange_name not in self.exchange_names:
                self.exchange_names[exchange_name] = r_keys

            result_queue = self.channel.queue_declare(queue="", exclusive=True)
            actual_queue = result_queue.method.queue

            if actual_queue not in self.queue_names:
                self.queue_names.append(actual_queue)

            for rk in r_keys:
                self.channel.queue_bind(exchange=exchange_name, queue=actual_queue, routing_key=rk)

            self.channel.basic_consume(
                queue=actual_queue,
                on_message_callback=self._wrap_callback(callback)
            )
            return actual_queue

        except self.PIKA_DISCONNECTION_ERRORS as error:
            raise MiddlewareDisconnectedError(str(error)) from error
        except pika.exceptions.AMQPError as error:
            raise MiddlewareMessageError(str(error)) from error

    def start_consuming(self) -> None:
        """
        Comienza el loop bloqueante de consumo de todas las colas suscriptas.
        Cada mensaje recibido se deriva automáticamente al callback específico asociado a su cola.
        """
        self._ensure_connected()

        if not self.queue_names:
            raise MiddlewareMessageError("No queues registered to consume from")

        try:
            self.consuming = True
            self.channel.start_consuming()
        except self.PIKA_DISCONNECTION_ERRORS as error:
            self.close()
            raise MiddlewareDisconnectedError(str(error)) from error
        except pika.exceptions.AMQPError as error:
            self.close()
            raise MiddlewareMessageError(str(error)) from error
        finally:
            self.consuming = False

    def stop_consuming(self) -> None:
        """
        Detiene la escucha de mensajes en el canal.
        """
        if not self.consuming:
            return

        try:
            if self.channel and self.channel.is_open:
                self.channel.stop_consuming()
        except self.PIKA_DISCONNECTION_ERRORS as error:
            raise MiddlewareDisconnectedError(str(error)) from error
        except pika.exceptions.AMQPError as error:
            raise MiddlewareMessageError(str(error)) from error
        finally:
            self.consuming = False

    def send(
        self,
        destination: str,
        payload: bytes,
        routing_keys: Optional[List[str]] = None,
    ) -> None:
        """
        Envía un mensaje al destino especificado.
        - Si routing_keys está presente, publica al exchange destination con cada una de las routing keys.
        - Si routing_keys no está presente y destination es un exchange conocido, publica con las routing keys registradas del exchange.
        - Si destination no es un exchange registrado y no hay routing_keys, publica directamente a la cola destination utilizando el default exchange de RabbitMQ.
        """
        self._ensure_connected()

        try:
            if routing_keys is not None:
                for rk in routing_keys:
                    self.channel.basic_publish(
                        exchange=destination,
                        routing_key=rk,
                        body=payload,
                    )
            elif destination in self.exchange_names:
                r_keys = self.exchange_names.get(destination, [""])
                for rk in r_keys:
                    self.channel.basic_publish(
                        exchange=destination,
                        routing_key=rk,
                        body=payload,
                    )
            else:
                self.channel.basic_publish(
                    exchange="",
                    routing_key=destination,
                    body=payload,
                )
        except self.PIKA_DISCONNECTION_ERRORS as error:
            raise MiddlewareDisconnectedError(str(error)) from error
        except pika.exceptions.AMQPError as error:
            raise MiddlewareMessageError(str(error)) from error

    def broadcast(
        self,
        exchange: str,
        payload: bytes
    ) -> None:
        """
        Envía un mensaje en broadcast a todos los suscriptores del exchange fanout.
        Si el exchange no fue registrado previamente, lo declara como fanout durable.
        """
        self._ensure_connected()

        try:
            if exchange not in self.exchange_names:
                self.channel.exchange_declare(
                    exchange=exchange,
                    exchange_type="fanout",
                    durable=True
                )
                self.exchange_names[exchange] = [""]

            self.channel.basic_publish(
                exchange=exchange,
                routing_key="",
                body=payload,
            )
        except self.PIKA_DISCONNECTION_ERRORS as error:
            raise MiddlewareDisconnectedError(str(error)) from error
        except pika.exceptions.AMQPError as error:
            raise MiddlewareMessageError(str(error)) from error

    def close(self) -> None:
        """
        Cierra canal y conexión liberando recursos.
        Garantiza que la conexión se cierre limpiamente incluso si cerrar el canal falla.
        Es idempotente y seguro de llamar múltiples veces.
        """
        if self.consuming:
            try:
                self.stop_consuming()
            except Exception as e:
                logger.warning(f"Error stopping consumption during close: {e}")

        # Intentar cerrar el canal
        if self.channel:
            try:
                if self.channel.is_open:
                    self.channel.close()
            except Exception as e:
                logger.warning(f"Error closing RabbitMQ channel: {e}")
            finally:
                self.channel = None

        # Intentar cerrar la conexión SIEMPRE, aun si el canal arrojó error
        if self.connection:
            try:
                if self.connection.is_open:
                    self.connection.close()
            except Exception as e:
                logger.warning(f"Error closing RabbitMQ connection: {e}")
            finally:
                self.connection = None
