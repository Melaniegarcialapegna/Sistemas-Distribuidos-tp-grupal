from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from common.message_protocol.internal import TLVSerializer
from common.message_protocol.msgtype import HeartbeatMessageType

if TYPE_CHECKING:
    from common.middleware.middleware_handler import MiddlewareHandler

logger = logging.getLogger(__name__)


class HeartbeatSender:
    """
    Componente reutilizable para emitir heartbeats periódicos a través de RabbitMQ.
    Puede ser utilizado por Workers, Gateways y Watchdogs sin duplicación de lógica.
    """

    def __init__(
        self,
        middleware_handler: MiddlewareHandler,
        service_name: str,
        exchange_name: str,
        interval: float = 1.0,
    ):
        """
        :param middleware_handler: Instancia de MiddlewareHandler (RabbitMQHandler).
        :param service_name: Nombre o ID del servicio emisor (ej: 'word_counter_0', 'gateway').
        :param exchange_name: Nombre del exchange de destino (típicamente fanout).
        :param interval: Intervalo entre latidos en segundos.
        """
        self.middleware_handler = middleware_handler
        self.service_name = service_name
        self.exchange_name = exchange_name
        self.interval = float(interval)
        self._running: bool = False
        self._stopping: bool = False

    def start(self) -> None:
        """
        Inicia el ciclo de envío periódico de heartbeats.
        Es idempotente: si ya está corriendo, no inicia un segundo ciclo.
        """
        if self._running:
            return
        self._running = True
        self._stopping = False
        self._send()

    def stop(self) -> None:
        """
        Detiene el ciclo periódico de heartbeats.
        """
        self._stopping = True
        self._running = False

    def _send(self) -> None:
        if self._stopping:
            return

        try:
            message = HeartbeatMessageType(
                payload=[self.service_name],
                from_client_id=0,
            )
            payload = TLVSerializer.serialize_message(message)
            self.middleware_handler.broadcast(self.exchange_name, payload)
        except Exception as e:
            logger.error(f"[{self.service_name}] Error enviando heartbeat: {e}")
        finally:
            if not self._stopping:
                self.middleware_handler.call_later(self.interval, self._send)
