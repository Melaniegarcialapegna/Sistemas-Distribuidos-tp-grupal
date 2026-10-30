import logging
import os
from common.base_worker import BaseWorker

class YearFilterWorker(BaseWorker):
    def __init__(self, host, input_queue_name, q1_output_queue_name, q2_output_queue_name):
        super().__init__(host, input_queue_name)
        self.q1_output_queue_name = q1_output_queue_name
        self.q2_output_queue_name = q2_output_queue_name

    def process_message(self, message):
        # Base worker llama a este metodo sobre cada articulo dentro del batch
        # message es una lista con los parametros necesarios para esta etapa del articulo
        client_id, year, url, img, sections, name, date, language = message
        if year <= 2020:
            q1_projection = [client_id, language, url, date]
            self.emit(client_id, self.q1_output_queue_name, q1_projection)            
            q2_projection = [client_id, sections, name, url, img]
            self.emit(client_id, self.q2_output_queue_name, q2_projection)

def main():
    logging.basicConfig(level=logging.INFO)
    host = os.environ["RABBITMQ_HOST"]
    input_queue_name = os.environ["INPUT_QUEUE_NAME"]
    q1_output_queue_name = os.environ["Q1_OUTPUT_QUEUE_NAME"]
    q2_output_queue_name = os.environ["Q2_OUTPUT_QUEUE_NAME"]

    year_filter_worker = YearFilterWorker(host, input_queue_name, q1_output_queue_name, q2_output_queue_name)
    year_filter_worker.start()
    return 0

if __name__ == "__main__":
    main()