from common.message_protocol import external
from common.message_protocol.internal import TLVDeserializer
from common.message_protocol.msgtype import BatchMessageType, EOFMessageType

class ResultsDispatcher:
    def __init__(self,client_list, expected_eofs):
        self.client_list = client_list # proxy
        self.expected_eofs = expected_eofs # {query_id: expected_eofs}
        self.clients = {}
        self.eofs_received = {}
        self.queries_done = {} #{client_od : finished_queries}

    def handle(self, data):
        message = TLVDeserializer.deserialize_tlv(data)
        client_id = message.from_client_id
        client = self._find_client(client_id)
        query_id = message.query_id 

        if client is not None:
            client_socket, lock = client
            try:
                if message.message_type == BatchMessageType:
                    self._on_batch(message, client_socket, lock)
                elif message.message_type == EOFMessageType:
                    query_id = message.payload[0]
                    self._on_eof(client_id, query_id, client_socket, lock)
            except OSError:
                self._forget_client(client_id) # desconected client

    #Helpers
    def _find_client(self, client_id):
        if client_id not in self.clients:
            for handler, socket, lock in self.client_list:
                if handler.client_id == client_id:
                    self.clients[client_id] = (socket, lock)
                    break
        return self.clients.get(client_id)

    def _on_batch(self, message, client_socket, lock):
        rows_by_query ={}
        for row in message.payload:
            query_id = row[0]
            row_without_query_id = row[1:]
            rows_by_query.setdefault(query_id, []).append(row_without_query_id)
        with lock:
            for query_id, rows in rows_by_query.items():
                external.send_msg(client_socket,
                                  external.MsgType.QUERY_RESULT_BATCH,
                                  query_id, rows)

    def _on_eof(self, client_id, query_id, client_socket, lock):
        received = self.eofs_received.setdefault(query_id, {})
        received[client_id] = received.get(client_id, 0) + 1

        if received[client_id] >= self.expected_eofs:
            with lock:
                external.send_msg(client_socket, external.MsgType.QUERY_DONE, query_id)

            done = self.queries_done.setdefault(client_id, set())
            done.add(query_id)
            if len(done) >= len(self.expected_eofs):
                self._forget_client(client_id) # 5 queries finished

    def _forget_client(self, client_id):
        for i, (handler, socket, lock) in enumerate(self.client_list):
            if handler.client_id == client_id:
                self.client_list.pop(i)
                break
        for state in (self.clients, self.eofs_received, self.queries_done):
            state.pop(client_id, None)