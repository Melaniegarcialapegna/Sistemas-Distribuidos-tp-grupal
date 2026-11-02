import logging
import os
from nltk.corpus import stopwords
import re
from common.base_worker import BaseWorker
from common.eof_coordinator.queue_eof_coordinator import QueueEofCoordinator

WORD_ONLY_REGEX = re.compile(r'\b[^\W\d_]+\b')

class TokenizerWorker(BaseWorker):
    def __init__(self, batch_size, host, input_queue_name, control_routing_key, control_exchange_name, repeated_filter_output_queue, q5_output_queue):
        super().__init__(batch_size, host)
        self._create_output_queue(repeated_filter_output_queue)
        self._create_output_queue(q5_output_queue)
        self.repeated_filter_output_queue = repeated_filter_output_queue
        self.q5_output_queue = q5_output_queue
        self.eof_coordinator = QueueEofCoordinator(
            replica_id=os.environ.get("REPLICA_ID", "default_replica"),
            previous_stage_replicas=int(os.environ.get("PREVIOUS_STAGE_REPLICAS", "1")),
            on_barrier=self._flush_all,
            publish_control=self._publish_control_message
        )
        self.input_queue_name = input_queue_name
        self._create_queue_input_handler(input_queue_name, control_exchange_name, control_routing_key)
        self.stopwords = set(stopwords.words('english')) | set(stopwords.words('french'))


    def process_data(self, client_id, message):
        # Base worker llama a este metodo sobre cada articulo dentro del batch
        # message es una lista con los parametros necesarios para esta etapa del articulo
        year, url, abstract, in_language = message
        filtered_abstract = self._extract_non_stopwords_from_abstract(abstract)
        self.emit(client_id, self.q5_output_queue, [year, url, len(filtered_abstract)])
        if filtered_abstract:
            self.emit(client_id, self.repeated_filter_output_queue, [filtered_abstract, in_language])

    def _extract_non_stopwords_from_abstract(self, abstract):
        if not isinstance(abstract, str):
            return []
        words = WORD_ONLY_REGEX.findall(abstract.lower())
        return [word for word in words if word not in self.stopwords]


def main():
    logging.basicConfig(level=logging.INFO)
    host = os.environ["RABBITMQ_HOST"]
    input_queue_name = os.environ["INPUT_QUEUE_NAME"]
    repeated_filter_output_queue = os.environ.get("REPEATED_FILTER_OUTPUT_QUEUE_NAME") or os.environ.get("Q3_4_OUTPUT_QUEUE_NAME")
    q5_output_queue = os.environ["Q5_OUTPUT_QUEUE_NAME"]
    batch_size = int(os.environ.get("BATCH_SIZE", 10))
    control_exchange_name = os.environ["CONTROL_EXCHANGE_NAME"]
    control_routing_key = os.environ["CONTROL_ROUTING_KEY"]

    tokenizer_worker = TokenizerWorker(batch_size, host, input_queue_name, control_routing_key, control_exchange_name, repeated_filter_output_queue, q5_output_queue)
    tokenizer_worker.start()
    return 0

if __name__ == "__main__":
    main()