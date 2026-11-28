from abc import abstractmethod
import logging

from common.base_worker.base_worker import BaseWorker
from common.persistence import StateManager

_EOF_BATCH_ID = "eof"


class StatefulWorker(BaseWorker):
    def __init__(self, batch_size, host, state_dir, snapshot_every) -> None:
        super().__init__(batch_size, host)
        self.state_manager = StateManager(state_dir, snapshot_every)


    def start(self) -> None:
        self.recover_state()
        super().start()

    def stop(self) -> None:
        try:
            super().stop()
        finally:
            if getattr(self, "state_manager", None) is not None:
                try:
                    self.state_manager.close()
                except Exception as e:
                    logging.debug(f"Error while closing state manager: {e}")

    def recover_state(self) -> None:
        snapshot_state, wal_deltas = self.state_manager.start()
        if snapshot_state is not None:
            self.restore_snapshot(snapshot_state)
        for delta in wal_deltas:
            self.apply_delta(delta)

    def visit_batch(self, batch_message):
        client_id = batch_message.from_client_id
        batch_id = batch_message.batch_id

        if self.state_manager.already_processed(client_id, batch_id):
            self.eof_coordinator.on_data_processed(client_id, len(batch_message.payload))
            return

        for payload in batch_message.payload:
            self.process_data(client_id, payload)

        delta = self.build_batch_delta(client_id)
        if delta is not None:
            self.update_state(client_id, batch_id, delta)

        self.eof_coordinator.on_data_processed(client_id, len(batch_message.payload))

    def visit_eof(self, eof_message):
        client_id = eof_message.from_client_id

        if self.state_manager.already_processed(client_id, _EOF_BATCH_ID):
            return

        super().visit_eof(eof_message)

        self.state_manager.append(
            client_id,
            _EOF_BATCH_ID,
            {"op": "eof", "client_id": client_id},
        )

    def update_state(self, client_id, batch_id, delta):
        try:
            self.state_manager.append(client_id, batch_id, delta)
            if self.state_manager.should_snapshot():
                self.state_manager.create_snapshot(self.serialize_state)
        except Exception:
            logging.exception("update_state falló client=%s batch_id=%r",
                            client_id, batch_id)
            raise

    def force_snapshot(self):
        self.state_manager.create_snapshot(self.serialize_state)

    @abstractmethod
    def build_batch_delta(self, client_id):
        """Construye el delta a partir del estado acumulado en el batch actual."""
        pass

    @abstractmethod
    def serialize_state(self):
        """Devuelve una representación JSON del estado completo."""
        pass

    @abstractmethod
    def restore_snapshot(self, state):
        """Reconstruye el estado en memoria a partir de un snapshot cargado."""
        pass

    @abstractmethod
    def apply_delta(self, delta):
        """Aplica un delta del WAL al estado en memoria."""
        pass
