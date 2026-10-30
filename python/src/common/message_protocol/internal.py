import json
import src.common.message_protocol.tlv_protocol as tlv

def serialize(message):
    return tlv.serialize_tlv(message)

def serialize_batch(messages):
    """
    Serialize a batch of messages into a single TLV formatted bytes object.
    """
    serialized_messages = bytearray()
    for message in messages:
        serialized_message = serialize(message)
        serialized_messages.extend(len(serialized_message).to_bytes(4, 'big'))  # Prefix each message with its length
        serialized_messages.extend(serialized_message)
    return len(messages).to_bytes(4, 'big') + bytes(serialized_messages) # header with the number of messages in the batch

def deserialize(message):
    return tlv.deserialize_tlv(message)

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