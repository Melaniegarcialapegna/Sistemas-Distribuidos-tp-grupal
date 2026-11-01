import json
from . import tlv_protocol as tlv
from .msgtype import ControlMessageType, EOFMessageType, DataMessageType, BatchMessageType

MESSAGE_TYPE_REPRESENTATIONS = {
    "EOF": 1,
    "DATA": 2,
    "BATCH": 3,
    "CONTROL": 4
}

class TLVSerializer:
    """
    Clase que proporciona métodos para serializar y deserializar datos en formato TLV (Type-Length-Value).
    """

    @staticmethod
    def serialize_message(message):
        """
        Serializa un mensaje en formato TLV.

        :param message: Mensaje a serializar.
        :return: Un objeto bytes que representa los datos serializados en formato TLV.
        """
        return message.visit(TLVSerializer())

    def visit_eof(self, eof_message):
        return MESSAGE_TYPE_REPRESENTATIONS['EOF'].to_bytes(1, "big") + eof_message.from_client_id.to_bytes(4, "big") + self.serialize(eof_message.payload)

    def visit_data(self, data_message):
        return MESSAGE_TYPE_REPRESENTATIONS['DATA'].to_bytes(1, "big") + data_message.from_client_id.to_bytes(4, "big") + self.serialize(data_message.payload)

    def visit_batch(self, batch_message):
        return MESSAGE_TYPE_REPRESENTATIONS['BATCH'].to_bytes(1, "big") + batch_message.from_client_id.to_bytes(4, "big") + self.serialize_batch(batch_message.payload)

    def visit_control(self, control_message):
        return MESSAGE_TYPE_REPRESENTATIONS['CONTROL'].to_bytes(1, "big") + control_message.from_client_id.to_bytes(4, "big") + self.serialize(control_message.payload) 

    @staticmethod
    def serialize(payload):
        """
        Serializa un payload en formato TLV.

        :param payload: El payload a serializar representado como una lista de objetos.
        :return: Un objeto bytes que representa los datos serializados en formato TLV.
        """
        return tlv.serialize_tlv(payload)

    @staticmethod
    def serialize_batch(payload):
        """
        Serializa un lote de mensajes en un solo objeto bytes con formato TLV.
        """
        serialized_messages = bytearray()
        for message in payload:
            serialized_message = TLVSerializer.serialize(message)
            serialized_messages.extend(len(serialized_message).to_bytes(4, 'big'))  # Prefix each message with its length
            serialized_messages.extend(serialized_message)
        return len(payload).to_bytes(4, 'big') + bytes(serialized_messages) # header with the number of messages in the batch
    
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
        client_id = int.from_bytes(data[1:5], 'big')  # Extract the client ID from the next 4 bytes
        if type_byte == MESSAGE_TYPE_REPRESENTATIONS['EOF']:
            payload = TLVDeserializer.deserialize(data[5:])  # Deserialize the payload
            return EOFMessageType(payload, client_id)
        elif type_byte == MESSAGE_TYPE_REPRESENTATIONS['DATA']:
            payload = TLVDeserializer.deserialize(data[5:])  # Deserialize the payload
            return DataMessageType(payload, client_id)
        elif type_byte == MESSAGE_TYPE_REPRESENTATIONS['BATCH']:
            payload = TLVDeserializer.deserialize_batch(data[5:])  # Deserialize the batch payload
            return BatchMessageType(payload, client_id)
        elif type_byte == MESSAGE_TYPE_REPRESENTATIONS['CONTROL']:
            payload = TLVDeserializer.deserialize(data[5:])  # Deserialize the payload
            return ControlMessageType(payload, client_id)
        else:
            raise ValueError(f"Unknown type byte: {type_byte}")

    @staticmethod
    def deserialize(message):
        return tlv.deserialize_tlv(message)

    @staticmethod
    def deserialize_batch(batch_message):
        """
        Deserializa un lote de mensajes en formato TLV.

        :param batch_message: Un objeto bytes que representa los datos serializados en formato TLV.
        :return: Una lista de mensajes deserializados.
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