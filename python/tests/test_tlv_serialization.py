import unittest
from src.common.message_protocol.tlv_protocol import serialize_tlv, deserialize_tlv

class TestTLVSerialization(unittest.TestCase):
    def test_serialize_deserialize_int(self):
        original_data = [42, 100, 256]
        serialized_data = serialize_tlv(original_data)
        deserialized_data = deserialize_tlv(serialized_data)
        self.assertEqual(original_data, deserialized_data)

    def test_serialize_deserialize_string(self):
        original_data = ["hello", "world", "test"]
        serialized_data = serialize_tlv(original_data)
        deserialized_data = deserialize_tlv(serialized_data)
        self.assertEqual(original_data, deserialized_data)

    def test_serialize_deserialize_mixed(self):
        original_data = [42, "hello", 100, "world"]
        serialized_data = serialize_tlv(original_data)
        deserialized_data = deserialize_tlv(serialized_data)
        self.assertEqual(original_data, deserialized_data)

    def test_unsupported_type(self):
        with self.assertRaises(ValueError):
            serialize_tlv([42, 3.14])  # float is unsupported

    def test_empty_list(self):
        original_data = []
        serialized_data = serialize_tlv(original_data)
        deserialized_data = deserialize_tlv(serialized_data)
        self.assertEqual(original_data, deserialized_data)

    def test_byte_representation(self):
        original_data = [42, "hello"]
        serialized_data = serialize_tlv(original_data)
        expected_bytes = bytearray()
        expected_bytes.extend(b'\x01' + (42).to_bytes(4, 'big'))  # INT type
        expected_bytes.extend(b'\x02' + (5).to_bytes(4, 'big') + b'hello')  # STRING type
        self.assertEqual(serialized_data, bytes(expected_bytes))

class TestMessageProtocol(unittest.TestCase):
    def test_serialize_deserialize_message(self):
        from src.common.message_protocol.tlv_protocol import Message
        original_message = Message([42, "hello"])
        serialized_message = original_message.serialize()
        deserialized_message = Message.from_bytes(serialized_message)
        self.assertEqual(original_message.params, deserialized_message.params)

    def test_from_bytes(self):
        from src.common.message_protocol.tlv_protocol import Message
        original_data = [42, "hello"]
        serialized_data = serialize_tlv(original_data)
        message = Message.from_bytes(serialized_data)
        self.assertEqual(message.params, original_data)