import logging
import os
import signal
import sys
import time
from typing import Any, Callable, List, Optional

from common import middleware
from common.heartbeat import HeartbeatSender
from common.message_protocol.internal import TLVDeserializer, TLVSerializer
from common.message_protocol.msgtype import BullyMessageType, HeartbeatMessageType
from common.middleware.middleware_handler import MiddlewareHandler
from common.utils import get_watchdog_config

logger = logging.getLogger(__name__)


class Watchdog:
    """
    Supervisor de fallos para los nodos del pipeline con Algoritmo de Bully (Líder / Réplicas Pasivas).

    - Todos los watchdogs emiten heartbeats al HEARTBEAT_EXCHANGE_NAME como cualquier otro nodo.
    - Todos los watchdogs reciben pasivamente los heartbeats de los workers y de los demás watchdogs.
    - El watchdog vivo con mayor ID asume el rol de Líder.
    - Solo el Líder tiene la potestad de reiniciar contenedores caídos.
    - Si se cae un watchdog no-líder, el líder lo revive.
    - Si se cae el líder, los seguidores detectan el timeout y disparan una elección en WATCHDOG_EXCHANGE_NAME.
    - El nuevo líder revive a los watchdogs caídos de mayor ID, quienes al arrancar reclaman el liderazgo pacíficamente.
    """

    def __init__(
        self,
        rabbitmq_host: str,
        heartbeat_interval: float,
        heartbeat_timeout: float,
        watchlist: List[str],
        replica_id: int,
        total_watchdogs: int,
        election_timeout: float,
        heartbeat_exchange_name: str,
        watchdog_exchange_name: str,
        initial_grace_period: float,
        revive_grace_period: float,
        middleware_handler: Optional[MiddlewareHandler] = None,
        docker_client: Optional[Any] = None,
        heartbeat_sender: Optional[HeartbeatSender] = None,
    ):
        self.rabbitmq_host = rabbitmq_host
        self.heartbeat_interval = float(heartbeat_interval)
        self.heartbeat_timeout = float(heartbeat_timeout)
        self.replica_id = int(replica_id)
        self.total_watchdogs = int(total_watchdogs)
        self.election_timeout = float(election_timeout)
        self.heartbeat_exchange_name = heartbeat_exchange_name
        self.watchdog_exchange_name = watchdog_exchange_name
        self.initial_grace_period = float(initial_grace_period)
        self.revive_grace_period = float(revive_grace_period)

        # Inclusión de workers y de los demás watchdogs en la lista de monitoreo
        self.watchlist: List[str] = list(watchlist)
        for i in range(self.total_watchdogs):
            peer_service = f"watchdog_{i}"
            if i != self.replica_id and peer_service not in self.watchlist:
                self.watchlist.append(peer_service)

        # Estado de Liderazgo y Elección (Bully)
        self.is_leader: bool = False
        self.leader_id: int = self.total_watchdogs - 1  # Por defecto asumimos al mayor ID
        self.is_electing: bool = False
        self.ok_received: bool = False
        self._election_generation: int = 0

        # Estado en memoria: servicio => timestamp de último heartbeat recibido
        now = time.time()
        self.received_heartbeats = {
            service: now + self.initial_grace_period for service in self.watchlist
        }

        # Middleware handler (inyectable para tests o instanciado por defecto)
        self.rabbit_handler = (
            middleware_handler
            if middleware_handler is not None
            else middleware.RabbitMQHandler(self.rabbitmq_host)
        )

        # Docker client (inyectable para tests o conectado a /var/run/docker.sock)
        self.docker_client = (
            docker_client if docker_client is not None else self._init_docker_client()
        )

        # Emisor de Heartbeats propio del Watchdog
        self.heartbeat_sender = (
            heartbeat_sender
            if heartbeat_sender is not None
            else HeartbeatSender(
                middleware_handler=self.rabbit_handler,
                service_name=f"watchdog_{self.replica_id}",
                exchange_name=self.heartbeat_exchange_name,
                interval=self.heartbeat_interval,
            )
        )

        self._running: bool = False
        self._stopping: bool = False
        self._setup_signals()

    def _init_docker_client(self) -> Optional[Any]:
        try:
            import docker
            client = docker.DockerClient(base_url="unix://var/run/docker.sock")
            logger.info("Connected to Docker socket successfully.")
            return client
        except Exception as e:
            logger.warning(
                f"Could not connect to Docker socket (/var/run/docker.sock): {e}. "
                "Node revival will be disabled if Docker client is unavailable."
            )
            return None

    def _setup_signals(self) -> None:
        def _handle_signal(signum, frame):
            logger.info(f"[{self.replica_id}] Signal {signum} received. Stopping Watchdog...")
            self.stop()
            sys.exit(0)

        signal.signal(signal.SIGINT, _handle_signal)
        signal.signal(signal.SIGTERM, _handle_signal)

    def set_leader(self, is_leader: bool) -> None:
        """Cambia el rol del Watchdog (líder con potestad de revivir o réplica pasiva)."""
        logger.info(
            f"[{self.replica_id}] Leadership changed: is_leader = {is_leader} "
            f"(was {self.is_leader}, leader_id = {self.leader_id})"
        )
        self.is_leader = is_leader

    def start(self) -> None:
        """Inicia el Watchdog, suscribe a exchanges, inicia latidos y dispara la elección."""
        if self._running:
            return
        self._running = True
        self._stopping = False
        logger.info(
            f"[{self.replica_id}] Watchdog started. "
            f"Cluster size: {self.total_watchdogs}. Monitoring: {self.watchlist}"
        )

        # 1. Iniciar la emisión periódica de heartbeats de este watchdog
        self.heartbeat_sender.start()

        # 2. Suscribirse a los heartbeats de todos los nodos (exchange fanout)
        hb_queue = f"watchdog_hb_{self.replica_id}"
        self.rabbit_handler.subscribe_to_exchange(
            exchange_name=self.heartbeat_exchange_name,
            routing_keys=[""],
            callback=self._handle_heartbeat,
            exchange_type="fanout",
            queue_name=hb_queue,
        )

        # 3. Suscribirse al exchange direct de control entre Watchdogs
        # Recibe mensajes punto a punto (watchdog_{id}) y broadcasts (watchdog_broadcast)
        ctrl_queue = f"watchdog_ctrl_{self.replica_id}"
        self.rabbit_handler.subscribe_to_exchange(
            exchange_name=self.watchdog_exchange_name,
            routing_keys=[f"watchdog_{self.replica_id}", "watchdog_broadcast"],
            callback=self._handle_election_message,
            exchange_type="direct",
            queue_name=ctrl_queue,
        )

        # 4. Programar chequeo periódico de timeouts
        self.rabbit_handler.call_later(self.heartbeat_interval, self._check_timeouts)

        # 5. Todo nodo al arrancar dispara una elección (Bully)
        self.start_election()

        # Comenzar a consumir mensajes (bloqueante en Pika)
        try:
            self.rabbit_handler.start_consuming()
        except Exception as e:
            if not self._stopping:
                logger.error(f"[{self.replica_id}] Error in consuming loop: {e}")
        finally:
            self.stop()

    def stop(self) -> None:
        """Detiene el Watchdog y cierra conexiones."""
        if self._stopping:
            return
        self._stopping = True
        self._running = False
        self.is_electing = False
        logger.info(f"[{self.replica_id}] Stopping Watchdog...")
        try:
            self.heartbeat_sender.stop()
        except Exception as e:
            logger.debug(f"[{self.replica_id}] Error stopping heartbeat sender: {e}")
        try:
            self.rabbit_handler.close()
        except Exception as e:
            logger.debug(f"[{self.replica_id}] Error closing rabbit handler: {e}")

    # =========================================================================
    # Lógica del Algoritmo de Bully (Elección de Líder)
    # =========================================================================

    def start_election(self) -> None:
        """Inicia una elección de Bully enviando ELECTION a los nodos con ID estrictamente mayor."""
        if self._stopping:
            return

        higher_nodes = [i for i in range(self.replica_id + 1, self.total_watchdogs)]
        if not higher_nodes:
            # Somos el nodo de mayor ID posible en el cluster; asumimos el liderazgo directamente
            logger.info(f"[{self.replica_id}] Highest ID in cluster. Becoming leader directly.")
            self.become_leader()
            return

        logger.info(
            f"[{self.replica_id}] Starting election. Sending ELECTION to higher nodes: {higher_nodes}"
        )
        self.is_electing = True
        self.ok_received = False
        self._election_generation += 1
        current_gen = self._election_generation

        for higher_id in higher_nodes:
            self._send_bully_message(action="ELECTION", target_id=higher_id)

        # Programar timeout para esperar respuestas OK de nodos superiores
        self.rabbit_handler.call_later(
            self.election_timeout,
            lambda: self._on_election_timeout(current_gen),
        )

    def _on_election_timeout(self, generation: int) -> None:
        """Si ningún nodo de mayor ID respondió OK dentro del timeout, asumimos el liderazgo."""
        if self._stopping or not self.is_electing or generation != self._election_generation:
            return

        if not self.ok_received:
            logger.info(
                f"[{self.replica_id}] Election timeout: no OK received from higher nodes. "
                "Becoming new leader."
            )
            self.become_leader()
        else:
            logger.info(
                f"[{self.replica_id}] Election timeout: OK was received, standing by for coordinator."
            )

    def become_leader(self) -> None:
        """Proclama el liderazgo, emite COORDINATOR y revive a los nodos de mayor ID caídos."""
        self.is_electing = False
        self.ok_received = False
        self.leader_id = self.replica_id
        self.set_leader(True)

        # 1. Proclamar nuevo líder por broadcast
        logger.info(f"[{self.replica_id}] Broadcasting COORDINATOR announcement.")
        self._broadcast_coordinator()

        # 2. Revivir a las réplicas de mayor ID caídas (si existen)
        higher_nodes = [i for i in range(self.replica_id + 1, self.total_watchdogs)]
        if higher_nodes:
            logger.info(
                f"[{self.replica_id}] As new leader, reviving dead higher ID watchdogs: {higher_nodes}"
            )
            now = time.time()
            for higher_id in higher_nodes:
                higher_service = f"watchdog_{higher_id}"
                self._revive_service(higher_service)
                self.received_heartbeats[higher_service] = now + self.revive_grace_period

    def _handle_election_message(
        self,
        body: bytes,
        ack: Optional[Callable] = None,
        nack: Optional[Callable] = None,
    ) -> None:
        """Callback para mensajes de elección en el exchange direct WATCHDOG_EXCHANGE_NAME."""
        try:
            message = TLVDeserializer.deserialize_message(body)
            message.visit(self)
            if ack:
                ack()
        except Exception as e:
            logger.error(f"[{self.replica_id}] Error handling election message: {e}")
            if ack:
                ack()

    def visit_bully(self, bully_message: BullyMessageType) -> None:
        """Procesa un mensaje del protocolo Bully (ELECTION, OK, COORDINATOR)."""
        action = bully_message.action
        sender_id = bully_message.sender_id
        logger.info(f"[{self.replica_id}] Received Bully message '{action}' from replica {sender_id}")

        if action == "ELECTION":
            # Si un nodo de menor ID nos desafía, respondemos OK y nosotros iniciamos elección
            if sender_id < self.replica_id:
                logger.info(f"[{self.replica_id}] Answering OK to lower replica {sender_id}")
                self._send_bully_message(action="OK", target_id=sender_id)
                if not self.is_electing:
                    self.start_election()

        elif action == "OK":
            # Un nodo de mayor ID confirma que está vivo y se encarga de la elección
            if sender_id > self.replica_id:
                logger.info(
                    f"[{self.replica_id}] Node {sender_id} responded OK. Standing by for COORDINATOR."
                )
                self.ok_received = True

        elif action == "COORDINATOR":
            self._handle_coordinator(sender_id)

    def _handle_coordinator(self, leader_id: int) -> None:
        """Procesa el anuncio de un nuevo coordinador."""
        logger.info(f"[{self.replica_id}] Coordinator announced: replica {leader_id}")
        self.is_electing = False
        self.ok_received = False

        # Si alguien con MENOR ID se proclama líder mientras nosotros estamos vivos, lo desbancamos
        if leader_id < self.replica_id:
            logger.warning(
                f"[{self.replica_id}] Replica {leader_id} claimed leadership, "
                f"but my ID is higher. Starting election to bully it."
            )
            self.start_election()
            return

        self.leader_id = leader_id
        if leader_id == self.replica_id:
            self.set_leader(True)
        else:
            self.set_leader(False)

    def _send_bully_message(self, action: str, target_id: int) -> None:
        """Envía un mensaje punto a punto a la cola de un Watchdog específico."""
        msg = BullyMessageType(payload=[action, self.replica_id])
        payload = TLVSerializer.serialize_message(msg)
        self.rabbit_handler.send(
            destination=self.watchdog_exchange_name,
            payload=payload,
            routing_keys=[f"watchdog_{target_id}"],
        )

    def _broadcast_coordinator(self) -> None:
        """Emite el anuncio de coordinador a todos los Watchdogs vía routing key broadcast."""
        msg = BullyMessageType(payload=["COORDINATOR", self.replica_id])
        payload = TLVSerializer.serialize_message(msg)
        self.rabbit_handler.send(
            destination=self.watchdog_exchange_name,
            payload=payload,
            routing_keys=["watchdog_broadcast"],
        )

    # =========================================================================
    # Manejo de Heartbeats y Detección de Timeouts
    # =========================================================================

    def _handle_heartbeat(
        self,
        body: bytes,
        ack: Optional[Callable] = None,
        nack: Optional[Callable] = None,
    ) -> None:
        """Callback ejecutado al recibir un mensaje del exchange fanout de heartbeats."""
        try:
            message = TLVDeserializer.deserialize_message(body)
            message.visit(self)
            if ack:
                ack()
        except Exception as e:
            logger.error(f"[{self.replica_id}] Error processing heartbeat payload: {e}")
            if ack:
                ack()

    def visit_heartbeat(self, heartbeat_message: HeartbeatMessageType) -> None:
        """Procesa un HeartbeatMessageType recibido actualizando el timestamp local."""
        service = heartbeat_message.service_name
        if service:
            self.received_heartbeats[service] = time.time()
            logger.debug(f"[{self.replica_id}] Heartbeat received from '{service}'")

    def _check_timeouts(self) -> None:
        """Verifica timeouts de workers y watchdogs."""
        if self._stopping:
            return

        now = time.time()
        for service in self.watchlist:
            last_seen = self.received_heartbeats.get(service, 0)
            elapsed = now - last_seen

            if elapsed > self.heartbeat_timeout:
                # Caso A: ¿Se cayó el Líder de los Watchdogs?
                if service == f"watchdog_{self.leader_id}":
                    logger.warning(
                        f"[{self.replica_id}] TIMEOUT: Current leader 'watchdog_{self.leader_id}' "
                        f"silent for {elapsed:.2f}s > {self.heartbeat_timeout}s. Triggering election!"
                    )
                    # Otorgamos gracia para no disparar elecciones en cada tick
                    self.received_heartbeats[service] = now + self.revive_grace_period
                    self.start_election()

                # Caso B: Se cayó un worker o un watchdog no-líder
                elif self.is_leader:
                    logger.warning(
                        f"[{self.replica_id}] TIMEOUT: Service '{service}' "
                        f"silent for {elapsed:.2f}s > {self.heartbeat_timeout}s. "
                        f"Reviving container as LEADER..."
                    )
                    self._revive_service(service)
                    # Tambien otorgamos gracia para no disparar revivals constantemente
                    self.received_heartbeats[service] = now + self.revive_grace_period
                else:
                    logger.debug(
                        f"[{self.replica_id}] TIMEOUT: Service '{service}' "
                        f"silent for {elapsed:.2f}s. Passive replica (no action taken)."
                    )

        # Reprogramar siguiente chequeo
        if not self._stopping:
            self.rabbit_handler.call_later(self.heartbeat_interval, self._check_timeouts)

    def _revive_service(self, service: str) -> None:
        """Reinicia el contenedor Docker asociado al servicio caído."""
        if not self.docker_client:
            logger.error(
                f"[{self.replica_id}] Cannot revive '{service}': Docker client unavailable."
            )
            return

        try:
            container = self.docker_client.containers.get(service)
            container.restart()
            logger.info(f"[{self.replica_id}] Container '{service}' revived successfully.")
        except Exception as e:
            logger.error(f"[{self.replica_id}] Failed to revive container '{service}': {e}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    config = get_watchdog_config()
    watchdog = Watchdog(
        rabbitmq_host=config.service.rabbitmq_host,
        heartbeat_interval=config.heartbeat.interval,
        heartbeat_timeout=config.heartbeat_timeout,
        watchlist=config.watchlist,
        replica_id=config.service.replica_id,
        total_watchdogs=config.total_watchdogs,
        election_timeout=config.election_timeout,
        heartbeat_exchange_name=config.heartbeat.exchange_name,
        watchdog_exchange_name=config.watchdog_exchange_name,
        initial_grace_period=config.initial_grace_period,
        revive_grace_period=config.revive_grace_period,
    )
    watchdog.start()
