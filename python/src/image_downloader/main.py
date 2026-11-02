import json
import logging
import os

import urllib.request
from common.base_worker import BaseWorker
from common.eof_coordinator.queue_eof_coordinator import QueueEofCoordinator
from common.message_protocol.external import Query

BIOGRAPHY_SECTION_NAMES = {'biographie', 'biography'}

class ImageDownloaderWorker(BaseWorker):
    def __init__(self, batch_size, host, input_queue_name, control_routing_key, control_exchange_name, q2_output_queue_name):
        super().__init__(batch_size, host)
        self._create_output_queue(q2_output_queue_name)
        self.q2_output_queue_name = q2_output_queue_name
        self.eof_coordinator = QueueEofCoordinator(
            replica_id=os.environ.get("REPLICA_ID", "default_replica"),
            previous_stage_replicas=int(os.environ.get("PREVIOUS_STAGE_REPLICAS", "1")),
            on_barrier=self._flush_all,
            publish_control=self._publish_control_message
        )
        self.input_queue_name = input_queue_name
        self._create_queue_input_handler(input_queue_name, control_exchange_name, control_routing_key)
        self.result_query_id = Query.Q2

    def process_data(self, client_id, message):
        # Base worker llama a este metodo sobre cada articulo dentro del batch
        # message es una lista con los parametros necesarios para esta etapa del articulo
        name, url, img_url = message
        img_bytes = self._download_image_bytes(img_url)

        if img_bytes is None:
            img_bytes = b""

        q2_projection = [self.result_query_id, name, url, img_bytes]
        self.emit(client_id, self.q2_output_queue_name, q2_projection)

    def _download_image_bytes(self, img_url):
        if not img_url:
            return None
        try:
            headers = {
                "User-Agent": "ImageDownloader"
            }

            req = urllib.request.Request(img_url, headers=headers)

            with urllib.request.urlopen(req, timeout=10) as response:
                return response.read()
        except Exception as e:
            logging.error(f"Error downloading the image {e}")
            return None
        

def main():
    logging.basicConfig(level=logging.INFO)
    host = os.environ["RABBITMQ_HOST"]
    input_queue_name = os.environ["INPUT_QUEUE_NAME"]
    q2_output_queue_name = os.environ["Q2_OUTPUT_QUEUE_NAME"]
    batch_size = int(os.environ.get("BATCH_SIZE", 10))
    control_exchange_name = os.environ["CONTROL_EXCHANGE_NAME"]
    control_routing_key = os.environ["CONTROL_ROUTING_KEY"]

    year_filter_worker = ImageDownloaderWorker(batch_size, host, input_queue_name, control_routing_key, control_exchange_name, q2_output_queue_name)
    year_filter_worker.start()
    return 0

if __name__ == "__main__":
    main()