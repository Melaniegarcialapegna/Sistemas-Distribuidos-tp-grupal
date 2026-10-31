from abc import ABC, abstractmethod
import logging
import middleware, message_protocol
import message_protocol.internal
import message_protocol.msgtype as msg


class BaseWorker(ABC):

    def __init__(self, batch_size, host):
        self.batch_size = batch_size
        self.outgoing_batches = {}
        self.output_queues = {}
        self.emitted_counts = {}  # {client_id: {queue_name: count}}
        self.host = host
        self.eof_coordinator = None  # This will be set by subclasses that need EOF coordination

    def start(self):
        try:
            self.input_handler.start_consuming()
        except Exception as e:
            logging.exception(f"Error while consuming messages: {e}")

    def stop(self):
        try:
            self.input_handler.stop_consuming()
            self.input_handler.close()
        except Exception as e:
            logging.exception(f"Error while stopping worker: {e}")
        

    def emit(self, client_id, queue_name, item):
        batch = self.outgoing_batches.setdefault(client_id, {}).setdefault(queue_name, []) # outgoing_batches -> {client_id: {queue_name: [item1, item2, ...]}}
        batch.append(item)
        queue_counts = self.emitted_counts.setdefault(client_id, {})
        queue_counts[queue_name] = queue_counts.get(queue_name, 0) + 1
        if len(batch) >= self.batch_size:
            self.flush_batch(client_id, queue_name)

    def flush_batch(self, client_id, queue_name):
        batch = self.outgoing_batches.get(client_id, {}).pop(queue_name, [])
        if not batch:
            return 
        queue = self.output_queues[queue_name]
        message = msg.BatchMessageType(batch, client_id)
        queue.send(message_protocol.internal.TLVSerializer.serialize_message(message))

    def _handle_message(self, msg, ack, nack):
        try:
            message = message_protocol.internal.TLVDeserializer.deserialize_message(msg)
            message.visit(self)
            ack()
        except Exception as e:
            logging.exception(f"Error while handling message: {e}")
            nack()

    def visit_data(self, data_message):
        self.process_data(data_message.from_client_id, data_message.payload)
        self.eof_coordinator.on_data_processed(data_message.from_client_id, 1)  # Notify that one message has been processed

    def visit_batch(self, batch_message):
        for payload in batch_message.payload:
            self.process_data(batch_message.from_client_id, payload)
        self.eof_coordinator.on_data_processed(batch_message.from_client_id, len(batch_message.payload))  # Notify that multiple messages have been processed

    def visit_eof(self, eof_message):
        self.eof_coordinator.on_eof(eof_message.from_client_id, eof_message.payload[0])  # Notify that an EOF has been received with the count of messages processed

    def visit_control(self, control_message):
        self.eof_coordinator.on_control(control_message.payload, control_message.from_client_id)

    def _flush_all(self, client_id):
        client_counts = self.emitted_counts.pop(client_id, {})
        for queue_name, queue in self.output_queues.items():
            self.flush_batch(client_id, queue_name)
            count = client_counts.get(queue_name, 0)
            eof_message = msg.EOFMessageType([count], client_id)
            queue.send(message_protocol.internal.TLVSerializer.serialize_message(eof_message))
        self.outgoing_batches.pop(client_id, None)

    def _create_output_queue(self, queue_name):
        if queue_name not in self.output_queues:
            self.output_queues[queue_name] = middleware.MessageMiddlewareQueueRabbitMQ(self.host, queue_name)
        return self.output_queues[queue_name]

    def _create_queue_input_handler(self, queue_name, control_exchange_name, control_routing_key):
        self.control_exchange_name = control_exchange_name
        self.control_routing_key = control_routing_key
        self.input_handler = middleware.RabbitMQHandler(self.host)
        self.input_handler.subscribe_to_queue(queue_name, self._handle_message)
        self.input_handler.subscribe_to_exchange(control_exchange_name, [control_routing_key], callback=self._handle_message)

    def _publish_control_message(self, payload, client_id):
        message = msg.ControlMessageType(payload, client_id)
        serialized_message = message_protocol.internal.TLVSerializer.serialize_message(message)

        self.input_handler.send(self.control_exchange_name, serialized_message, routing_keys=[self.control_routing_key])


    @abstractmethod
    def process_data(self, client_id, payload):
        pass


# Idea: Agrupar todos los filtros
class YearFilter(BaseWorker):
    @abstractmethod
    def __init__(self, batch_size, host):
        pass

class LanguageFilter(BaseWorker):
    @abstractmethod
    def __init__(self, batch_size, host):
        pass

class AverageFilter(BaseWorker):
    @abstractmethod
    def __init__(self, batch_size, host):
        pass

class BiographyFilter(BaseWorker):
    @abstractmethod
    def __init__(self, batch_size, host):
        pass

class UniqueWordsFilter(BaseWorker):
    @abstractmethod
    def __init__(self, batch_size, host):
        pass

class Tokenizer(BaseWorker):
    @abstractmethod
    def __init__(self, batch_size, host):
        pass

class WordCounter(BaseWorker):
    @abstractmethod
    def __init__(self, batch_size, host):
        pass

class Hasher(BaseWorker):
    @abstractmethod
    def __init__(self, batch_size, host):
        pass

class Sum(BaseWorker):
    @abstractmethod
    def __init__(self, batch_size, host):
        pass

class SumJoin(BaseWorker):
    @abstractmethod
    def __init__(self, batch_size, host):
        pass

class Top20Filter(BaseWorker):
    @abstractmethod
    def __init__(self, batch_size, host):
        pass

