import json
import src.common.message_protocol.tlv_protocol as tlv
from src.common.message_protocol.msgtype import EOFMessageType, DataMessageType, BatchMessageType

MESSAGE_TYPE_REPRESENTATIONS = {
    "EOF": 1,
    "DATA": 2,
    "BATCH": 3
}

class TLVSerializator:
    """
    Clase que proporciona métodos para serializar y deserializar datos en formato TLV (Type-Length-Value).
    """

    @staticmethod
    def serialize_message(message):
        """
        Serializa un mensaje en formato TLV.

        :param message: Una lista de elementos a serializar.
        :return: Un objeto bytes que representa los datos serializados en formato TLV.
        """
        return message.visit(TLVSerializator())

    def visit_eof(self, eof_message):
        return MESSAGE_TYPE_REPRESENTATIONS['EOF'].to_bytes(1, "big") + self.serialize(eof_message.payload)

    def visit_data(self, data_message):
        return MESSAGE_TYPE_REPRESENTATIONS['DATA'].to_bytes(1, "big") + self.serialize(data_message.payload)

    def visit_batch(self, batch_message):
        return MESSAGE_TYPE_REPRESENTATIONS['BATCH'].to_bytes(1, "big") + self.serialize_batch(batch_message.payload)

    @staticmethod
    def serialize(payload):
        return tlv.serialize_tlv(payload)

    @staticmethod
    def serialize_batch(messages):
        """
        Serialize a batch of messages into a single TLV formatted bytes object.
        """
        serialized_messages = bytearray()
        for message in messages:
            serialized_message = TLVSerializator.serialize(message)
            serialized_messages.extend(len(serialized_message).to_bytes(4, 'big'))  # Prefix each message with its length
            serialized_messages.extend(serialized_message)
        return len(messages).to_bytes(4, 'big') + bytes(serialized_messages) # header with the number of messages in the batch
    
class TLVDeserializer:
    """
    Clase que proporciona métodos para deserializar datos en formato TLV (Type-Length-Value).
    """
    def __init__(self):
        self.tlv_deserializer = tlv.TLVDeserializer()

    @staticmethod
    def deserialize_message(data):
        """
        Deserializa un mensaje en formato TLV.

        :param data: Un objeto bytes que representa los datos serializados en formato TLV.
        :return: Una instancia de MessageType correspondiente al tipo de mensaje deserializado.
        """
        type_byte = data[0]
        if type_byte == MESSAGE_TYPE_REPRESENTATIONS['EOF']:
            payload = TLVDeserializer.deserialize(data[1:])  # Deserialize the payload
            return EOFMessageType(payload)
        elif type_byte == MESSAGE_TYPE_REPRESENTATIONS['DATA']:
            payload = TLVDeserializer.deserialize(data[1:])  # Deserialize the payload
            return DataMessageType(payload)
        elif type_byte == MESSAGE_TYPE_REPRESENTATIONS['BATCH']:
            payload = TLVDeserializer.deserialize_batch(data[1:])  # Deserialize the batch payload
            return BatchMessageType(payload)
        else:
            raise ValueError(f"Unknown type byte: {type_byte}")

    @staticmethod
    def deserialize(message):
        return tlv.deserialize_tlv(message)

    @staticmethod
    def deserialize_batch(batch_message):
        """
        Deserialize a batch of TLV formatted messages into a list of messages.
        """
        num_messages = int.from_bytes(batch_message[:4], 'big')
        messages = []
        offset = 4
        for _ in range(num_messages):
            # Deserialize each message and update the offset
            message_len = int.from_bytes(batch_message[offset:offset + 4], 'big')
            offset += 4
            message = tlv.deserialize_tlv(batch_message[offset:offset + message_len])
            messages.append(message)
            offset += message_len
        return messages