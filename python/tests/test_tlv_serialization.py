import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from src.common.message_protocol.tlv_protocol import (
    serialize_tlv,
    deserialize_tlv,
    TYPE_REPRESENTATIONS,
    LENGTH_BYTES,
)
from common.word_item import WordItem
from src.common.message_protocol.internal import (
    TLVSerializer,
    TLVDeserializer,
    MESSAGE_TYPE_REPRESENTATIONS,
)
from src.common.message_protocol.msgtype import (
    DataMessageType,
    EOFMessageType,
    BatchMessageType,
    ControlMessageType,
)


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
            serialize_tlv([42, {"key": "value"}])  # dict is unsupported

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
        expected_bytes.extend(int_byte + (1).to_bytes(4, 'big') + (42).to_bytes(1, 'big'))
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

    def test_serialize_deserialize_list(self):
        original_data = [[42, "hello"], [100, "world"]]
        serialized_data = serialize_tlv(original_data)
        deserialized_data = deserialize_tlv(serialized_data)
        self.assertEqual(original_data, deserialized_data)

    def test_serialize_deserialize_nested_list(self):
        original_data = [[42, "hello"], [100, ["nested", "list"]]]
        serialized_data = serialize_tlv(original_data)
        deserialized_data = deserialize_tlv(serialized_data)
        self.assertEqual(original_data, deserialized_data)

    def test_serialize_deserialize_word_item(self):
        original_item = WordItem("apple", 3, 2)
        serialized_data = serialize_tlv([original_item])
        deserialized_data = deserialize_tlv(serialized_data)
        self.assertEqual(len(deserialized_data), 1)
        self.assertIsInstance(deserialized_data[0], WordItem)
        self.assertEqual(deserialized_data[0].word, "apple")
        self.assertEqual(deserialized_data[0].total_count(), 5)

    def test_serialize_deserialize_list_of_word_items(self):
        original_items = [
            WordItem("apple", 5, 0),
            WordItem("banana", 2, 3),
            WordItem("cherry", 1, 1),
        ]
        serialized_data = serialize_tlv([original_items])
        deserialized_data = deserialize_tlv(serialized_data)
        self.assertEqual(len(deserialized_data), 1)
        items = deserialized_data[0]
        self.assertEqual(len(items), 3)
        self.assertEqual([w.word for w in items], ["apple", "banana", "cherry"])
        self.assertEqual([w.total_count() for w in items], [5, 5, 2])


class TestInternalMessages(unittest.TestCase):
    def test_serialize_deserialize_data_message(self):
        original_payload = [42, "hello", 100]
        client_id = 1
        data_message = DataMessageType(original_payload, client_id)

        serialized = TLVSerializer.serialize_message(data_message)
        deserialized = TLVDeserializer.deserialize_message(serialized)

        self.assertIsInstance(deserialized, DataMessageType)
        self.assertEqual(deserialized.payload, original_payload)
        self.assertEqual(deserialized.from_client_id, client_id)

    def test_serialize_deserialize_eof_message(self):
        original_payload = [500]
        client_id = 123
        eof_message = EOFMessageType(original_payload, client_id)

        serialized = TLVSerializer.serialize_message(eof_message)
        deserialized = TLVDeserializer.deserialize_message(serialized)

        self.assertIsInstance(deserialized, EOFMessageType)
        self.assertEqual(deserialized.payload, original_payload)
        self.assertEqual(deserialized.from_client_id, client_id)

    def test_serialize_deserialize_batch_message(self):
        batch_payload = [
            [42, "hello"],
            [100, "world", b'\x01\x02'],
            ["bye", 256]
        ]
        client_id = 999
        batch_message = BatchMessageType(batch_payload, client_id)

        serialized = TLVSerializer.serialize_message(batch_message)
        deserialized = TLVDeserializer.deserialize_message(serialized)

        self.assertIsInstance(deserialized, BatchMessageType)
        self.assertEqual(deserialized.payload, batch_payload)
        self.assertEqual(deserialized.from_client_id, client_id)

    def test_serialize_deserialize_control_message(self):
        control_payload = ["EOF", 500]
        client_id = 42
        control_message = ControlMessageType(control_payload, client_id)

        serialized = TLVSerializer.serialize_message(control_message)
        deserialized = TLVDeserializer.deserialize_message(serialized)

        self.assertIsInstance(deserialized, ControlMessageType)
        self.assertEqual(deserialized.payload, control_payload)
        self.assertEqual(deserialized.from_client_id, client_id)

    def test_deserialize_invalid_message_type(self):
        invalid_type_byte = b'\x99' + (1).to_bytes(4, 'big') + serialize_tlv([42])
        with self.assertRaises(ValueError):
            TLVDeserializer.deserialize_message(invalid_type_byte)


class TestBatchSerialization(unittest.TestCase):
    def test_serialize_deserialize_batch(self):
        messages = [
            [42, "hello"],
            [100, "world"],
            [256, "test"]
        ]
        serialized_batch = TLVSerializer.serialize_batch(messages)
        deserialized_messages = TLVDeserializer.deserialize_batch(serialized_batch)
        self.assertEqual(messages, deserialized_messages)

    def test_empty_batch(self):
        messages = []
        serialized_batch = TLVSerializer.serialize_batch(messages)
        deserialized_messages = TLVDeserializer.deserialize_batch(serialized_batch)
        self.assertEqual(messages, deserialized_messages)

    def test_single_message_batch(self):
        messages = [[42, "hello"]]
        serialized_batch = TLVSerializer.serialize_batch(messages)
        deserialized_messages = TLVDeserializer.deserialize_batch(serialized_batch)
        self.assertEqual(messages, deserialized_messages)

    def test_batch_with_varied_messages(self):
        messages = [
            [42, "hello"],
            [100, "world", b'\x00\x01'],
            ["goodbye", 256, b'\x02\x03\x04', "test"]
        ]
        serialized_batch = TLVSerializer.serialize_batch(messages)
        deserialized_messages = TLVDeserializer.deserialize_batch(serialized_batch)
        self.assertEqual(messages, deserialized_messages)


if __name__ == "__main__":
    unittest.main()
