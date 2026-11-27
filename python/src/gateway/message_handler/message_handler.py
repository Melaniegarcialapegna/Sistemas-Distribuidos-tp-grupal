from common.message_protocol.internal import TLVSerializer
from common.message_protocol.msgtype import BatchMessageType, EOFMessageType
from common.message_protocol.external import USABLE_COLUMNS
class MessageHandler:

    def __init__(self,client_id):
        self.client_id = client_id
        self.sent_batches = 0
    
    def serialize_article_batch(self, raw_bytes):
        self.sent_batches += 1
        return self._batch([[raw_bytes]])          

    def serialize_eof_message(self):
        return self._eof(self.sent_batches)

    # Helpers

    def _batch(self, items):
        return TLVSerializer.serialize_message(BatchMessageType(items, self.client_id))

    def _eof(self, count):
        return TLVSerializer.serialize_message(EOFMessageType([count], self.client_id))
