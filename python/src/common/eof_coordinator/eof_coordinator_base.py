from abc import ABC, abstractmethod
from typing import Callable

# callable is a anotation for a function that takes two arguments (str, int) and returns None
# receives the client_id and the count of messages sent by the previous stage
BarrierCallback = Callable[[str, int], None]

class EofCoordinator(ABC):
    def __init__(self, replica_id, amount_of_replicas, on_barrier: BarrierCallback):
        self.replica_id = replica_id
        self.amount_of_replicas = amount_of_replicas # amount of EOF to wait
        self._on_barrier = on_barrier
        self._clients = {} # {client_id: _ClientState}

    # Public API for Workers

    def on_eof(self,client_id, count):
        """
        Notifies that an EOF has been received from a specific client and the count of messages sent by the previous stage.
        """
        self._run(client_id, count)

    # Helpers
    def _run(self, client_id, count):
        pass

class _ClientState:
    def __init__(self):
        self.eof_received = 0
        self.expected_total = 0
        self.message_counts = {} # {replica_id: processed_messages count} 