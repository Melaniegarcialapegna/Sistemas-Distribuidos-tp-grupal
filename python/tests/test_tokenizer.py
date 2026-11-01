import os
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

# Mock pika if not installed locally
try:
    import pika
    import pika.exceptions
except ImportError:
    class AMQPError(Exception): pass
    class AMQPConnectionError(AMQPError): pass
    class ConnectionClosed(AMQPConnectionError): pass
    class ConnectionClosedByBroker(ConnectionClosed): pass
    class StreamLostError(AMQPConnectionError): pass
    class ChannelWrongStateError(AMQPError): pass

    mock_pika = MagicMock()
    mock_pika_exceptions = MagicMock()
    mock_pika_exceptions.AMQPError = AMQPError
    mock_pika_exceptions.AMQPConnectionError = AMQPConnectionError
    mock_pika_exceptions.ConnectionClosed = ConnectionClosed
    mock_pika_exceptions.ConnectionClosedByBroker = ConnectionClosedByBroker
    mock_pika_exceptions.StreamLostError = StreamLostError
    mock_pika_exceptions.ChannelWrongStateError = ChannelWrongStateError

    mock_pika.exceptions = mock_pika_exceptions
    sys.modules["pika"] = mock_pika
    sys.modules["pika.exceptions"] = mock_pika_exceptions

# Mock nltk stopwords if nltk is not installed on the host
try:
    import nltk
    from nltk.corpus import stopwords
except (ImportError, LookupError):
    mock_nltk = MagicMock()
    mock_corpus = MagicMock()
    mock_stopwords = MagicMock()
    mock_stopwords.words.side_effect = lambda lang: (
        ["the", "is", "in", "and"] if lang == "english" else ["le", "c", "est"]
    )
    mock_corpus.stopwords = mock_stopwords
    mock_nltk.corpus = mock_corpus
    sys.modules["nltk"] = mock_nltk
    sys.modules["nltk.corpus"] = mock_corpus

from tokenizer.main import TokenizerWorker


class TestTokenizerWorker(unittest.TestCase):
    @patch("common.middleware.RabbitMQHandler")
    def setUp(self, mock_handler_cls):
        self.mock_output_handler = MagicMock()
        self.mock_input_handler = MagicMock()
        mock_handler_cls.side_effect = [self.mock_output_handler, self.mock_input_handler]

        self.worker = TokenizerWorker(
            batch_size=10,
            host="localhost",
            input_queue_name="input_queue",
            control_routing_key="ctrl_key",
            control_exchange_name="ctrl_ex",
            q3_4_output_queue="q3_4_queue",
            q5_output_queue="q5_queue",
        )

    def test_init_configures_queues_and_handlers(self):
        self.assertIn("q3_4_queue", self.worker.output_queues)
        self.assertIn("q5_queue", self.worker.output_queues)
        self.mock_input_handler.subscribe_to_queue.assert_called_once_with(
            "input_queue", self.worker._handle_message
        )
        self.mock_input_handler.subscribe_to_exchange.assert_called_once_with(
            "ctrl_ex", ["ctrl_key"], callback=self.worker._handle_message
        )
        self.assertTrue(len(self.worker.stopwords) > 0)

    def test_extract_non_stopwords_normal(self):
        abstract = "hello world today is the day 123"
        words = self.worker._extract_non_stopwords_from_abstract(abstract)
        self.assertNotIn("the", words)
        self.assertNotIn("is", words)
        self.assertNotIn("123", words)
        self.assertEqual(words, ["hello", "world", "today", "day"])

    def test_extract_non_stopwords_case_insensitive(self):
        abstract = "HELLO WORLD Today is the DaY"
        words = self.worker._extract_non_stopwords_from_abstract(abstract)
        self.assertEqual(words, ["hello", "world", "today", "day"])

    def test_extract_non_stopwords_french_accents(self):
        abstract = "Bonjour le monde, c'est aujourd'hui le grand jour."
        words = self.worker._extract_non_stopwords_from_abstract(abstract)
        # Con 'c' y 'est' filtradas como stopwords:
        self.assertEqual(words, ["bonjour", "monde", "aujourd", "hui", "grand", "jour"])

    def test_extract_non_stopwords_non_string(self):
        self.assertEqual(self.worker._extract_non_stopwords_from_abstract(None), [])
        self.assertEqual(self.worker._extract_non_stopwords_from_abstract(12345), [])

    def test_extract_non_stopwords_empty_or_only_stopwords(self):
        self.assertEqual(self.worker._extract_non_stopwords_from_abstract(""), [])
        self.assertEqual(self.worker._extract_non_stopwords_from_abstract("The is in and"), [])

    def test_process_data_emits_to_q5_and_q3_4(self):
        with patch.object(self.worker, "emit") as mock_emit:
            # "apple" está repetida: 4 palabras en total, 3 únicas
            message = [2021, "http://example.com/art1", "hello world today is today", "en"]
            self.worker.process_data(client_id=1, message=message)

            calls = mock_emit.call_args_list

            # Primer emit debe ser a Q5 con el conteo de palabras (incluyendo repetidas = 4)
            q5_calls = [c for c in calls if c[0][1] == "q5_queue"]
            self.assertEqual(len(q5_calls), 1)
            client_id, dest, payload = q5_calls[0][0]
            self.assertEqual(client_id, 1)
            self.assertEqual(dest, "q5_queue")
            self.assertEqual(payload, [2021, "http://example.com/art1", 4])

            # Siguientes emit deben ser a Q3_4 con palabras únicas
            q3_4_calls = [c for c in calls if c[0][1] == "q3_4_queue"]
            self.assertEqual(len(q3_4_calls), 3)

            emitted_words = {c[0][2][0] for c in q3_4_calls}
            self.assertEqual(emitted_words, {"hello", "world", "today"})

            # Cada par emitido a Q3_4 debe incluir el idioma
            for c in q3_4_calls:
                self.assertEqual(c[0][2][1], "en")

    def test_process_data_with_null_abstract(self):
        with patch.object(self.worker, "emit") as mock_emit:
            message = [2020, "http://example.com/null", None, "fr"]
            self.worker.process_data(client_id=1, message=message)

            calls = mock_emit.call_args_list
            # A Q5 se debe emitir con 0 palabras
            q5_calls = [c for c in calls if c[0][1] == "q5_queue"]
            self.assertEqual(len(q5_calls), 1)
            self.assertEqual(q5_calls[0][0][2], [2020, "http://example.com/null", 0])

            # A Q3_4 no se debe emitir ninguna palabra
            q3_4_calls = [c for c in calls if c[0][1] == "q3_4_queue"]
            self.assertEqual(len(q3_4_calls), 0)

    def test_process_data_with_only_stopwords(self):
        with patch.object(self.worker, "emit") as mock_emit:
            message = [2022, "http://example.com/stopwords", "the is in and", "en"]
            self.worker.process_data(client_id=1, message=message)

            calls = mock_emit.call_args_list
            # A Q5 con 0 palabras
            q5_calls = [c for c in calls if c[0][1] == "q5_queue"]
            self.assertEqual(len(q5_calls), 1)
            self.assertEqual(q5_calls[0][0][2], [2022, "http://example.com/stopwords", 0])

            # A Q3_4 no se emite ninguna
            q3_4_calls = [c for c in calls if c[0][1] == "q3_4_queue"]
            self.assertEqual(len(q3_4_calls), 0)

