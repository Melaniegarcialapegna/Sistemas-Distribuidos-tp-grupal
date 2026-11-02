import logging
from common import message_protocol

class InputStream:
    def __init__(self, name,coordinator, process_data):
        self.name = name
        self.coordinator = coordinator
        self._process_data = process_data

    def handle_message(self, raw,ack,nack):
        try:
            message = message_protocol.internal.TLVDeserializer.deserialize_message(raw)
            message.visit(self)
            ack()
        except Exception as e:
            logging.exception(f"[{self.name}] Error while handling message: {e}")
            nack()

    # Vuelvo a definir los visitors
    def visit_data(self, data_message):
        self._process_data(data_message.from_client_id, data_message.payload)
        self.coordinator.on_data_processed(data_message.from_client_id, 1)
 
    def visit_batch(self, batch_message):
        for payload in batch_message.payload:
            self._process_data(batch_message.from_client_id, payload)
        self.coordinator.on_data_processed(
            batch_message.from_client_id, len(batch_message.payload)
        )
 
    def visit_eof(self, eof_message):
        self.coordinator.on_eof(eof_message.from_client_id, eof_message.payload[0])
 
    def visit_control(self, control_message):
        self.coordinator.on_control(
            control_message.payload, control_message.from_client_id
        )  