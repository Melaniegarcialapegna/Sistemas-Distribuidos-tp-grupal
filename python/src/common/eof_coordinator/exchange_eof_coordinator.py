from eof_coordinator_base import EofCoordinator, _ClientState

class ExchangeEofCoordinator(EofCoordinator):
    def on_eof(self, client_id, count):
        """
        Notifies that an EOF has been received from a specific client and the count of messages sent by the previous stage.
        """
        if self._is_finished_client(client_id): return  
    
        client_state = self._get_client_state(client_id)
        client_state.eof_received += 1
        client_state.expected_total += count

        if self._all_eofs_received(client_state):
            self._finish_client(client_id, client_state.expected_total) 
