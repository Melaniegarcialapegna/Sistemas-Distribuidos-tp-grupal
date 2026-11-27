import logging, os
from common.base_worker import BaseWorker
from common.eof_coordinator.queue_eof_coordinator import QueueEofCoordinator
from common.message_protocol import external

HOST = os.environ["RABBITMQ_HOST"]
INPUT_QUEUE_NAME = os.environ["INPUT_QUEUE_NAME"]
YEAR_OUTPUT_QUEUE_NAME = os.environ["YEAR_OUTPUT_QUEUE_NAME"]
TOKENIZER_OUTPUT_QUEUE_NAME = os.environ["TOKENIZER_OUTPUT_QUEUE_NAME"]
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", 10))
CONTROL_EXCHANGE_NAME = os.environ["CONTROL_EXCHANGE_NAME"]
CONTROL_ROUTING_KEY = os.environ.get("CONTROL_ROUTING_KEY", "")
REPLICA_ID = os.environ["REPLICA_ID"]
PREVIOUS_STAGE_REPLICAS = int(os.environ.get("PREVIOUS_STAGE_REPLICAS", "1"))


class ArticleSplitterWorker(BaseWorker):
    def __init__(self):
        super().__init__(BATCH_SIZE, HOST)
        self._create_output_queue(YEAR_OUTPUT_QUEUE_NAME)
        self._create_output_queue(TOKENIZER_OUTPUT_QUEUE_NAME)
        self.eof_coordinator = QueueEofCoordinator(
            replica_id=REPLICA_ID,
            previous_stage_replicas=PREVIOUS_STAGE_REPLICAS,
            on_barrier=self._flush_all,
            publish_control=self._publish_control_message,
        )
        self._create_queue_input_handler(INPUT_QUEUE_NAME, CONTROL_EXCHANGE_NAME,
                                         CONTROL_ROUTING_KEY, REPLICA_ID)

    def process_data(self, client_id, item):
        for row in external.parse_article_records(item[0]):
            year = int(row["date_modified"][:4])

            year_list = [year, row["url"], row["image"], row["sections"], row["name"], row["date_modified"], row["in_language"]]
            self.emit(client_id, YEAR_OUTPUT_QUEUE_NAME,year_list)

            tokenizer_list = [year, row["url"], row["abstract"], row["in_language"]]
            self.emit(client_id, TOKENIZER_OUTPUT_QUEUE_NAME,tokenizer_list)


def main():
    logging.basicConfig(level=logging.INFO)
    ArticleSplitterWorker().start()
    return 0

if __name__ == "__main__":
    main()