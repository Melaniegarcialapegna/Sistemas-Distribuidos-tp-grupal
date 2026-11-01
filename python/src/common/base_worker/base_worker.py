from abc import ABC, abstractmethod
import logging

from common import middleware, message_protocol
from common.message_protocol import internal
from common.message_protocol import msgtype as msg


class BaseWorker(ABC):

    def __init__(self, batch_size, host):
        self.batch_size = batch_size
        self.outgoing_batches = {}  # {client_id: {target_key: [item1, item2, ...]}}
        self.output_queues = set()
        self.output_exchanges = {}  # {exchange_name: [routing_key1, routing_key2, ...]}
        self.emitted_counts = {}  # {client_id: {target_key: count}}
        self.host = host
        self.eof_coordinator = None  # SubclassResponsibility: cada subclase tiene que crear su EOFCoordinator
        self.output_handler = middleware.RabbitMQHandler(self.host)
        self.result_query_id = None 

    def start(self):
        try:
            self.input_handler.start_consuming()
        except Exception as e:
            logging.exception(f"Error while consuming messages: {e}")

    def stop(self):
        try:
            self.input_handler.stop_consuming()
            self.input_handler.close()
            self.output_handler.close()
        except Exception as e:
            logging.exception(f"Error while stopping worker: {e}")

    def emit(self, client_id, destination, item, routing_keys=None):
        target_key = (destination, tuple(routing_keys) if routing_keys else None)
        batch = self.outgoing_batches.setdefault(client_id, {}).setdefault(target_key, [])
        batch.append(item)
        queue_counts = self.emitted_counts.setdefault(client_id, {})
        queue_counts[target_key] = queue_counts.get(target_key, 0) + 1
        if len(batch) >= self.batch_size:
            self.flush_batch(client_id, destination, routing_keys)

    def flush_batch(self, client_id, destination, routing_keys=None):
        target_key = (destination, tuple(routing_keys) if routing_keys else None)
        batch = self.outgoing_batches.get(client_id, {}).pop(target_key, [])
        if not batch:
            return 
        message = msg.BatchMessageType(batch, client_id)
        self.output_handler.send(
            destination,
            message_protocol.internal.TLVSerializer.serialize_message(message),
            routing_keys=routing_keys,
        )

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

        for queue_name in self.output_queues:
            self.flush_batch(client_id, queue_name)
            count = client_counts.get((queue_name, None), 0)
            if self.result_query_id is None:
                eof_payload = [count]
            else:
                eof_payload = [self.result_query_id]
            eof_message = msg.EOFMessageType(eof_payload, client_id)
            
            self.output_handler.send(
                queue_name,
                message_protocol.internal.TLVSerializer.serialize_message(eof_message),
            )

        for exchange_name, r_keys in self.output_exchanges.items():
            for rk in r_keys:
                self.flush_batch(client_id, exchange_name, routing_keys=[rk])
                count = client_counts.get((exchange_name, (rk,)), 0)
                eof_message = msg.EOFMessageType([count], client_id)
                self.output_handler.send(
                    exchange_name,
                    message_protocol.internal.TLVSerializer.serialize_message(eof_message),
                    routing_keys=[rk],
                )
        self.outgoing_batches.pop(client_id, None)

    def _create_output_queue(self, queue_name):
        if queue_name not in self.output_queues:
            self.output_handler.declare_queue(queue_name)
            self.output_queues.add(queue_name)

    def _create_output_exchange(self, exchange_name, routing_keys):
        if exchange_name not in self.output_exchanges:
            self.output_handler.declare_exchange(exchange_name)
            self.output_exchanges[exchange_name] = routing_keys

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

