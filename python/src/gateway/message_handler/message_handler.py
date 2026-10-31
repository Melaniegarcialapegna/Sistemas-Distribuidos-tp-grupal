from common.message_protocol.internal import TLVSerializer
from common.message_protocol.msgtype import BatchMessageType, EOFMessageType
class MessageHandler:

    def __init__(self,client_id):
        self.client_id = client_id
        # Counters of how many messages have been sent to each queue
        self.sent_articles = 0
    
    def serialize_article_batch(self, records):
        year_items , tokenizer_items = [], []
        for r in records:
            year_items.append([r["year"], r["url"], r["image"], r["sections"],
                               r["name"], r["date_modified"], r["in_language"]])
            tokenizer_items.append([r["year"], r["url"], r["abstract"], r["in_language"]])
            
        self.sent_articles += len(records)
        return self._batch(year_items), self._batch(tokenizer_items)

    def serialize_eof_message(self):
        return self._eof(self.sent_articles)

    #TODO corregir
    def deserialize_result_message(self, message):
        """
        Deserialize the message and check if it is for this client.
        In case it is not for this client, returns None, otherwise returns the fruit and amount.
        """
        fields = message_protocol.internal.deserialize(message)
        [id_client,fruit_top] = fields

        if id_client != self.client_id:
            return None
        
        return fruit_top

    def _batch(self, items):
        return TLVSerializer.seralize_message(BatchMessageType(items, self.client_id))

    def _eof(self, count):
        return TLVSerializer.seralize_message(EOFMessageType([count], self.client_id))
