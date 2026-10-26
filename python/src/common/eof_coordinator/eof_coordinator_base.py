from abc import ABC, abstractmethod
from typing import Callable

# callable is a anotation for a function that takes two arguments (str, int) and returns None
# receives the client_id and the count of messages sent by the previous stage
BarrierCallback = Callable[[str, int], None]

#["EOF", client_id, count] 

# ["PROCESSED", client_id, replica_id, count]
PublishControlCallback = Callable[[list], None]

# Contants
CONTROL_EOF = "EOF"
CONTROL_PROCESSED = "PROCESSED"

# Class to hold the state of each client
class _ClientState:
    def __init__(self):
        self.eof_received = 0
        self.expected_total = 0
        self.message_counts = {} # {replica_id: processed_messages count} 

class EofCoordinator(ABC):
    def __init__(self, replica_id, previous_stage_replicas, on_barrier: BarrierCallback, publish_control: PublishControlCallback):
        self.replica_id = replica_id # DOC: se guarda replica para ir pisando los valores q me van llegando y no sumar de mas
        self.eofs_expected = previous_stage_replicas # amount of EOF to wait
        self._clients = {} # {client_id: _ClientState}

        # worker callbacks
        self._on_barrier = on_barrier
        self._publish_control = publish_control # DOC: esto lo hago para invocar desde el coordinator una funcion del worker (que es el que tiene la comunicacion entre replicas)

    # Public API for Workers

    def on_eof(self,client_id, count):
        """
        Notifies that an EOF has been received from a specific client and the count of messages sent by the previous stage.
        """
        self._publish_control([CONTROL_EOF, client_id, count])

    def on_data_processed(self, client_id, amount):
        """
        Notifies that the worker finished processing a amount of messages of the client
        """
        if amount <=0: return

        client_state = self._get_client_state(client_id)
        client_state.message_counts[self.replica_id] = client_state.message_counts.get(self.replica_id, 0) + amount

        if self._all_eofs_received(client_state):
            self._publish_own_count(client_id, client_state)

    def on_control(self, control_message):
        """
        Notifies that a control message from the control exchange has been received."""
        type_message = control_message[0]
        if type_message == CONTROL_EOF:
            _, client_id, count = control_message
            client_state = self._get_client_state(client_id)
            client_state.eof_received += 1
            client_state.expected_total += count

            if self._all_eofs_received(client_state):
                self._publish_own_count(client_id, client_state)

    # Helpers

    def _get_client_state(self, client_id):
        if client_id not in self._clients:
            self._clients[client_id] = _ClientState()
        return self._clients[client_id]

    def _all_eofs_received(self, client_state):
        """
        Checks if all EOFs have been received for a specific client.
        """
        return client_state.eof_received == self.eofs_expected

    def _publish_own_count(self, client_id, state):
        """
        Publishes the count of messages processed by this replica for a specific client.
        """
        own_count = state.message_counts.get(self.replica_id, 0)
        self._publish_control([CONTROL_PROCESSED, client_id, self.replica_id, own_count])

    

