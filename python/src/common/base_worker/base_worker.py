from abc import ABC, abstractmethod
import logging

import middleware, message_protocol

class BaseWorker(ABC):

    def __init__(self, batch_size):
        self.batch_size = batch_size
        self.outgoing_batches = {}

    def start(self):
        try:
            self.input_queue.start_consuming(self._handle_message)
        except Exception as e:
            logging.exception(f"Error while consuming messages: {e}")

    def stop(self):
        try:
            self.input_queue.close()
            self.input_queue.stop_consuming()
        except Exception as e:
            logging.exception(f"Error while stopping worker: {e}")
        

    def emit(self, client_id, queue_name, item):
        batch = self.outgoing_batches.setdefault(client_id, {}).setdefault(queue_name, [])
        batch.append(item)
        if len(batch) >= self.batch_size:
            self.flush_batch(client_id, queue_name)

    def flush_batch(self, client_id, queue_name):
        batch = self.outgoing_batches.get(client_id, {}).pop(queue_name, [])
        if not batch:
            return 
        queue = self.output_queues[queue_name]
        queue.send(message_protocol.internal.serialize([client_id, batch]))

    #TODO: improve message handling
    def _handle_message(self, msg, ack, nack):
        fields = message_protocol.internal.deserialize(msg)
        if len(fields) == 2:
            self._flush_all(fields[0])
            # handle_eof()
        else: 
            for item in fields[1]: # the payload is a batch
                self.process_data(fields[0], item) 
        ack()

    def _flush_all(self, client_id):
        for queue_name in self.output_queues:
            self.flush_batch(client_id, queue_name)

    @abstractmethod
    def process_data(self, client_id, payload):
        pass


# Idea: Agrupar todos los filtros
class YearFilter(BaseWorker):
    @abstractmethod
    def __init__(self, batch_size):
        pass

class LanguageFilter(BaseWorker):
    @abstractmethod
    def __init__(self, batch_size):
        pass

class AverageFilter(BaseWorker):
    @abstractmethod
    def __init__(self, batch_size):
        pass

class BiographyFilter(BaseWorker):
    @abstractmethod
    def __init__(self, batch_size):
        pass

class UniqueWordsFilter(BaseWorker):
    @abstractmethod
    def __init__(self, batch_size):
        pass

class Tokenizer(BaseWorker):
    @abstractmethod
    def __init__(self, batch_size):
        pass

class WordCounter(BaseWorker):
    @abstractmethod
    def __init__(self, batch_size):
        pass

class Hasher(BaseWorker):
    @abstractmethod
    def __init__(self, batch_size):
        pass

class Sum(BaseWorker):
    @abstractmethod
    def __init__(self, batch_size):
        pass

class SumJoin(BaseWorker):
    @abstractmethod
    def __init__(self, batch_size):
        pass

class Top20Filter(BaseWorker):
    @abstractmethod
    def __init__(self, batch_size):
        pass

