from abc import ABC, abstractmethod
from typing import Callable

# callable is a anotation for a function that takes two arguments (str, int) and returns None
# receives the client_id and the count of messages sent by the previous stage
BarrierCallback = Callable[[str, int], None]

#["EOF", client_id, count] 

# ["PROCESSED", client_id, replica_id, count]
PublishControlCallback = Callable[[list], None]

# Constants
ERROR_MUST_IMPLEMENT = "must be implemented in subclasses"

# Class to hold the state of each client
class _ClientState:
    def __init__(self):
        self.eof_received = 0
        self.expected_total = 0
        self.message_counts = {} # {replica_id: processed_messages_count} 

class EofCoordinator(ABC):
    def __init__(self, replica_id, previous_stage_replicas, on_barrier: BarrierCallback):
        self.replica_id = replica_id # DOC: se guarda replica para ir pisando los valores q me van llegando y no sumar de mas
        self.eofs_expected = previous_stage_replicas # amount of EOF to wait
        self._clients = {} # {client_id: _ClientState}
        self._finished_clients = set()

        # worker callback
        self._on_barrier = on_barrier

    # Public API for Workers

    @abstractmethod
    def on_eof(self,client_id, count):
        raise NotImplementedError(ERROR_MUST_IMPLEMENT)

    def on_data_processed(self, client_id, amount):
        pass  

    def on_control(self, control_message):
        pass  

    # Helpers

    def _get_client_state(self, client_id):
        if client_id not in self._clients:
            self._clients[client_id] = _ClientState()
        return self._clients[client_id]
    

    def _is_finished_client(self, client_id):
        return client_id in self._finished_clients
    

    def _all_eofs_received(self, client_state):
        """
        Checks if all EOFs have been received for a specific client.
        """
        return client_state.eof_received == self.eofs_expected
    

    def _finish_client(self, client_id, total):
        self._clients.pop(client_id, None)
        self._finished_clients.add(client_id)
        self._on_barrier(client_id, total)

