import unittest

from src.common.message_protocol.tlv_protocol import serialize_tlv, deserialize_tlv, TYPE_REPRESENTATIONS, LENGTH_BYTES
from src.common.message_protocol.internal import serialize_batch, deserialize_batch

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
            serialize_tlv([42, {"key": "value"}])  # float is unsupported

    def test_empty_list(self):
        original_data = []
        serialized_data = serialize_tlv(original_data)
        deserialized_data = deserialize_tlv(serialized_data)
        self.assertEqual(original_data, deserialized_data)

    def test_byte_representation(self):
        original_data = [42, "hello"]
        serialized_data = serialize_tlv(original_data)
        expected_bytes = bytearray()
        int_byte = TYPE_REPRESENTATIONS['INT'].to_bytes(1, 'big')
        string_byte = TYPE_REPRESENTATIONS['STRING'].to_bytes(1, 'big')
        expected_bytes.extend(int_byte + (4).to_bytes(4, 'big') + (42).to_bytes(4, 'big'))
        expected_bytes.extend(string_byte + (5).to_bytes(4, 'big') + b'hello')
        self.assertEqual(serialized_data, bytes(expected_bytes))

    def test_deserialize_invalid_data(self):
        with self.assertRaises(ValueError):
            deserialize_tlv(TYPE_REPRESENTATIONS['STRING'].to_bytes(1, 'big') + (2).to_bytes(LENGTH_BYTES, 'big') + b'ok' + b'\x00\x00\x00')  # Invalid type byte

    def test_serialize_deserialize_bytes(self):
        original_data = [b'\x00\x01\x02', b'\x03\x04\x05']
        serialized_data = serialize_tlv(original_data)
        deserialized_data = deserialize_tlv(serialized_data)
        self.assertEqual(original_data, deserialized_data)

    def test_bytes_are_represented_correctly(self):
        original_data = [b'\x00\x01\x02']
        serialized_data = serialize_tlv(original_data)
        expected_bytes = bytearray()
        bytes_byte = TYPE_REPRESENTATIONS['BYTES'].to_bytes(1, 'big')
        expected_bytes.extend(bytes_byte + (3).to_bytes(4, 'big') + b'\x00\x01\x02')  # BYTES type
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

class TestBatchSerialization(unittest.TestCase):
    def test_serialize_deserialize_batch(self):
        messages = [
            [42, "hello"],
            [100, "world"],
            [256, "test"]
        ]
        serialized_batch = serialize_batch(messages)
        deserialized_messages = deserialize_batch(serialized_batch)
        self.assertEqual(messages, deserialized_messages)

    def test_empty_batch(self):
        messages = []
        serialized_batch = serialize_batch(messages)
        deserialized_messages = deserialize_batch(serialized_batch)
        self.assertEqual(messages, deserialized_messages)

    def test_single_message_batch(self):
        messages = [[42, "hello"]]
        serialized_batch = serialize_batch(messages)
        deserialized_messages = deserialize_batch(serialized_batch)
        self.assertEqual(messages, deserialized_messages)

    def test_batch_with_varied_messages(self):
        messages = [
            [42, "hello"],
            [100, "world", b'\x00\x01'],
            ["goodbye", 256, b'\x02\x03\x04', "test"]
        ]
        serialized_batch = serialize_batch(messages)
        deserialized_messages = deserialize_batch(serialized_batch)
        self.assertEqual(messages, deserialized_messages)


if __name__ == "__main__":
    unittest.main()
