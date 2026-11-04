import logging
import os
import time
import urllib.error
import urllib.request
from common.base_worker import BaseWorker
from common.eof_coordinator.queue_eof_coordinator import QueueEofCoordinator
from common.message_protocol.external import Query

USER_AGENT = "TP-Distribuidos-FIUBA/1.0 (https://fi.uba.ar; pgillen@fi.uba.ar)"
MAX_RETRIES = 2
DEFAULT_BACKOFF = 1.0
MAX_WAIT_TIME = 5.0

HOST = os.environ.get("RABBITMQ_HOST")
INPUT_QUEUE_NAME = os.environ.get("INPUT_QUEUE_NAME")
Q2_OUTPUT_QUEUE_NAME = os.environ.get("Q2_OUTPUT_QUEUE_NAME")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", 10))
CONTROL_EXCHANGE_NAME = os.environ.get("CONTROL_EXCHANGE_NAME")
CONTROL_ROUTING_KEY = os.environ.get("CONTROL_ROUTING_KEY", "")
REPLICA_ID = os.environ.get("REPLICA_ID", "default_replica")
PREVIOUS_STAGE_REPLICAS = int(os.environ.get("PREVIOUS_STAGE_REPLICAS", "1"))

class ImageDownloaderWorker(BaseWorker):
    def __init__(self, batch_size, host, input_queue_name, control_routing_key, control_exchange_name, q2_output_queue_name, replica_id, previous_stage_replicas):
        super().__init__(batch_size, host)
        self._create_output_queue(q2_output_queue_name)
        self.q2_output_queue_name = q2_output_queue_name
        self.eof_coordinator = QueueEofCoordinator(
            replica_id=replica_id,
            previous_stage_replicas=previous_stage_replicas,
            on_barrier=self._flush_all,
            publish_control=self._publish_control_message
        )
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

    def _download_image_bytes(self, img_url, max_retries=MAX_RETRIES):
        if not img_url:
            return None

        headers = {
            "User-Agent": USER_AGENT,
        }

        req = urllib.request.Request(img_url, headers=headers)

        for attempt in range(max_retries + 1):
            try:
                with urllib.request.urlopen(req, timeout=10) as response:
                    return response.read()
            except urllib.error.HTTPError as e:
                if e.code == 429 and attempt < max_retries:
                    retry_after = e.headers.get("Retry-After")
                    try:
                        wait = float(retry_after) if retry_after else DEFAULT_BACKOFF * (2 ** attempt)
                    except (ValueError, TypeError):
                        wait = DEFAULT_BACKOFF * (2 ** attempt)
                    wait = min(wait, MAX_WAIT_TIME)
                    logging.warning(f"HTTP 429 al descargar {img_url}. Reintentando en {wait:.1f}s (intento {attempt + 1}/{max_retries})...")
                    time.sleep(wait)
                    continue
                logging.error(f"Error downloading the image {e} ({img_url})")
                return None
            except Exception as e:
                if attempt < max_retries:
                    time.sleep(DEFAULT_BACKOFF)
                    continue
                logging.error(f"Error downloading the image {e} ({img_url})")
                return None

        return None

def main():
    logging.basicConfig(level=logging.INFO)

    image_downloader_worker = ImageDownloaderWorker(
        BATCH_SIZE, HOST, INPUT_QUEUE_NAME, CONTROL_ROUTING_KEY,
        CONTROL_EXCHANGE_NAME, Q2_OUTPUT_QUEUE_NAME, REPLICA_ID, PREVIOUS_STAGE_REPLICAS
    )
    image_downloader_worker.start()
    return 0

if __name__ == "__main__":
    main()