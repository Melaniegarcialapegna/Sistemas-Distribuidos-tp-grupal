import logging
from nltk.corpus import stopwords
import re
from common.base_worker import BaseWorker
from common.eof_coordinator.queue_eof_coordinator import QueueEofCoordinator
from common.utils import get_env, get_worker_info

WORD_ONLY_REGEX = re.compile(r'\b[^\W\d_]+\b')
YEAR_FILTER = 2020


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
        self._create_queue_input_handler(input_queue_name, control_exchange_name, control_routing_key,replica_id)
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
    info = get_worker_info(require_input_queue=True)
    repeated_filter_output_queue = (
        get_env("REPEATED_FILTER_OUTPUT_QUEUE_NAME", default=None)
        or get_env("Q3_4_OUTPUT_QUEUE_NAME")
    )

    tokenizer_worker = TokenizerWorker(
        info.batch_size,
        info.service.rabbitmq_host,
        info.input_queue_name,
        info.control.control_routing_key,
        info.control.control_exchange_name,
        repeated_filter_output_queue,
        get_env("Q5_SUM_OUTPUT_QUEUE_NAME"),
        get_env("Q5_AVERAGE_FILTER_OUTPUT_QUEUE_NAME"),
        info.service.replica_id,
        info.control.previous_stage_replicas,
    )
    tokenizer_worker.start()
    return 0

if __name__ == "__main__":
    main()