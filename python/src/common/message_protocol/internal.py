import json
import common.message_protocol.tlv_protocol as tlv

def serialize(message):
    return tlv.serialize_tlv(message)


def deserialize(message):
    return tlv.deserialize_tlv(message)