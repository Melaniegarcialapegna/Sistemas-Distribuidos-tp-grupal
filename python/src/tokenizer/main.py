import logging
import os
from nltk.corpus import stopwords
import re
from common.base_worker import BaseWorker
from common.eof_coordinator.queue_eof_coordinator import QueueEofCoordinator

WORD_ONLY_REGEX = re.compile(r'\b[^\W\d_]+\b')
YEAR_FILTER = 2020
HOST = os.environ.get("RABBITMQ_HOST")
INPUT_QUEUE_NAME = os.environ.get("INPUT_QUEUE_NAME")
REPEATED_FILTER_OUTPUT_QUEUE = os.environ.get("REPEATED_FILTER_OUTPUT_QUEUE_NAME") or os.environ.get("Q3_4_OUTPUT_QUEUE_NAME")
Q5_SUM_QUEUE = os.environ.get("Q5_SUM_OUTPUT_QUEUE_NAME")
Q5_AVERAGE_FILTER_QUEUE = os.environ.get("Q5_AVERAGE_FILTER_OUTPUT_QUEUE_NAME")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", 10))
CONTROL_EXCHANGE_NAME = os.environ.get("CONTROL_EXCHANGE_NAME")
CONTROL_ROUTING_KEY = os.environ.get("CONTROL_ROUTING_KEY", "")
REPLICA_ID = os.environ.get("REPLICA_ID", "default_replica")
PREVIOUS_STAGE_REPLICAS = int(os.environ.get("PREVIOUS_STAGE_REPLICAS", "1"))

class TokenizerWorker(BaseWorker):
    def __init__(self, batch_size, host, input_queue_name, control_routing_key, control_exchange_name, repeated_filter_output_queue, q5_sum_queue, q5_average_filter_queue, replica_id, previous_stage_replicas):
        super().__init__(batch_size, host)
        self._create_output_queue(repeated_filter_output_queue)
        self._create_output_queue(q5_sum_queue)
        self._create_output_queue(q5_average_filter_queue)
        self.repeated_filter_output_queue = repeated_filter_output_queue
        self.q5_old_queue = q5_sum_queue # to Sum
        self.q5_recent_queue = q5_average_filter_queue # to Average filter

        self.eof_coordinator = QueueEofCoordinator(
            replica_id=replica_id,
            previous_stage_replicas=previous_stage_replicas,
            on_barrier=self._flush_all,
            publish_control=self._publish_control_message
        )
        self._create_queue_input_handler(input_queue_name, control_exchange_name, control_routing_key)
        self.stopwords = set(stopwords.words('english')) | set(stopwords.words('french'))


    def process_data(self, client_id, message):
        # Base worker llama a este metodo sobre cada articulo dentro del batch
        # message es una lista con los parametros necesarios para esta etapa del articulo
        year, url, abstract, in_language = message
        filtered_abstract = self._extract_non_stopwords_from_abstract(abstract)
        length = len(filtered_abstract)
        if year <= YEAR_FILTER:
            self.emit(client_id,self.q5_old_queue,[length]) 
        else:
            self.emit(client_id,self.q5_recent_queue,[url,length]) 
        if filtered_abstract:
            self.emit(client_id, self.repeated_filter_output_queue, [filtered_abstract, in_language])

    def _extract_non_stopwords_from_abstract(self, abstract):
        if not isinstance(abstract, str):
            return []
        words = WORD_ONLY_REGEX.findall(abstract.lower())
        return [word for word in words if word not in self.stopwords]


def main():
    logging.basicConfig(level=logging.INFO)

    tokenizer_worker = TokenizerWorker(
        BATCH_SIZE, HOST, INPUT_QUEUE_NAME, CONTROL_ROUTING_KEY,
        CONTROL_EXCHANGE_NAME, REPEATED_FILTER_OUTPUT_QUEUE,
        Q5_SUM_QUEUE, Q5_AVERAGE_FILTER_QUEUE, REPLICA_ID, PREVIOUS_STAGE_REPLICAS
    )
    tokenizer_worker.start()
    return 0

if __name__ == "__main__":
    main()