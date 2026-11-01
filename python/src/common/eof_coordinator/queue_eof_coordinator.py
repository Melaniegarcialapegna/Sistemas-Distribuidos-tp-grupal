from .eof_coordinator_base import EofCoordinator
from typing import Callable

# Constants
CONTROL_EOF = "EOF"
CONTROL_PROCESSED = "PROCESSED"

#["EOF", client_id, count] 

# ["PROCESSED", replica_id, count]
PublishControlCallback = Callable[[list, int], None]

class QueueEofCoordinator(EofCoordinator):
    def __init__(self, replica_id, previous_stage_replicas, on_barrier, publish_control:PublishControlCallback):
        super().__init__(replica_id, previous_stage_replicas, on_barrier)
        self._publish_control = publish_control # DOC: esto lo hago para invocar desde el coordinator una funcion del worker (que es el que tiene la comunicacion entre replicas)

    def on_eof(self,client_id, count):
        """
        Notifies that an EOF has been received from a specific client and the count of messages sent by the previous stage.
        """
        self._publish_control([CONTROL_EOF, count], client_id)  # Publish the EOF message to the control exchange


    def on_data_processed(self, client_id, amount):
        """
        Notifies that the worker finished processing a amount of messages of the client
        """
        if amount <=0: return

        client_state = self._get_client_state(client_id)
        client_state.message_counts[self.replica_id] = client_state.message_counts.get(self.replica_id, 0) + amount

        if self._all_eofs_received(client_state):
            self._publish_own_count(client_id, client_state)

        self._check_barrier(client_id, client_state)


    def on_control(self, control_message, client_id):
        """
        Notifies that a control message from the control exchange has been received."""
        type_message = control_message[0]

        if type_message == CONTROL_EOF:
            _, count = control_message
            client_state = self._get_client_state(client_id)
            client_state.eof_received += 1
            client_state.expected_total += count

            if self._all_eofs_received(client_state):
                self._publish_own_count(client_id, client_state)

            self._check_barrier(client_id, client_state)

        elif type_message == CONTROL_PROCESSED:
            _, replica_id, count = control_message
            if replica_id != self.replica_id: # ignore own messages
                client_state = self._get_client_state(client_id)
                register = client_state.message_counts.get(replica_id, 0)
                client_state.message_counts[replica_id] = max(register, count) # doc: evita duplicados o mensajes viejos

                self._check_barrier(client_id, client_state)


    def _publish_own_count(self, client_id, client_state):
            """
            Publishes the count of messages processed by this replica for a specific client.
            """
            own_count = client_state.message_counts.get(self.replica_id, 0)
            if own_count > 0:
                self._publish_control([CONTROL_PROCESSED, self.replica_id, own_count], client_id)

    
    def _check_barrier(self, client_id, client_state):
            """
            Checks if the barrier has been reached for a specific client.
            """
            if not self._all_eofs_received(client_state):
                return
    
            total_processed = sum(client_state.message_counts.values()) # of all the replicas, including this one
    
            if total_processed == client_state.expected_total:
                self._finish_client(client_id) 