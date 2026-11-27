from common.message_protocol.internal import TLVSerializer
from common.message_protocol.msgtype import BatchMessageType, EOFMessageType
from common.message_protocol.external import USABLE_COLUMNS
class MessageHandler:

    def __init__(self,client_id):
        self.client_id = client_id
        # Counters of how many messages have been sent to each queue
        self.sent_articles = 0
    
    def serialize_article_batch(self, records):
        rows = []
        for record in records:
            for column in USABLE_COLUMNS:
                rows.append(record[column])
        self.sent_articles += len(records)
            
        return self._batch(rows)            

    def serialize_eof_message(self):
        return self._eof(self.sent_articles)

    # Helpers

    def _batch(self, items):
        return TLVSerializer.serialize_message(BatchMessageType(items, self.client_id))

    def _eof(self, count):
        return TLVSerializer.serialize_message(EOFMessageType([count], self.client_id))
