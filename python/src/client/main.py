import os
import logging
import socket
import signal
import threading
import pyarrow.parquet as pq

from common import message_protocol

INPUT_FILE = os.environ["INPUT_FILE"]
SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])


ROW_BATCH_SIZE = 1000 # add to config
USABLE_COLUMNS = ["name", "url", "date_modified", "sections", "image", "abstract", "in_language"] 
EXPECTED_QUERIES = {message_protocol.external.Query.Q1} # add to config

class Client:

    def __init__(self):
        self.closed = False
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self.handle_sigterm)

    def handle_sigterm(self, signum, frame):
        logging.info("Recieved SIGTERM signal")
        self.closed = True
        self.disconnect()

        if self._prev_sigterm_handler:
            self._prev_sigterm_handler(signum, frame)

    def connect(self, server_host, server_port):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        import time
        
        # Aca intenta conectarse varias veces. Es medio raro habria que revisarlo. 
        while not self.closed:
            try:
                self.server_socket.connect((server_host, server_port))
                logging.info("¡Conectado exitosamente al Gateway!")
                break
            except socket.error:
                logging.info("Esperando a que el Gateway esté disponible...")
                time.sleep(1)

    def disconnect(self):
        if self.server_socket:
            self.server_socket.shutdown(socket.SHUT_RDWR)

    def send_article_info(self, input_file):
        logging.info("Sending article info")
        parquet_file = pq.ParquetFile(input_file)
        for batch in parquet_file.iter_batches(batch_size=ROW_BATCH_SIZE, columns=USABLE_COLUMNS):
                batch_data = []
                for r in batch.to_pylist():
                    r = self._flatten(r)
                    batch_data.append(r)

                message_protocol.external.send_msg(
                    self.server_socket,
                    message_protocol.external.MsgType.ARTICLE_RECORD_BATCH,
                    batch_data
                )

        message_protocol.external.send_msg(
            self.server_socket, message_protocol.external.MsgType.END_OF_RECORDS
        )

    def recv_results(self):
        external = message_protocol.external
        queries_done = set()
        rows_per_query = {}
        try:
            while not self.closed:
                msg_type, payload = external.recv_msg(self.server_socket)

                if msg_type == external.MsgType.QUERY_RESULT_BATCH:
                    query_id, rows = payload
                    rows_per_query[query_id] = rows_per_query.get(query_id, 0) + len(rows)
                    for row in rows:
                        print(f"Q{query_id}: {row}")
                elif msg_type == external.MsgType.QUERY_DONE:
                    logging.info(f"Consulta {payload} terminada: {rows_per_query.get(payload, 0)} filas") 
                    queries_done.add(payload)
                    if queries_done >= EXPECTED_QUERIES:
                        return
                elif msg_type == message_protocol.external.MsgType.ACK:
                    continue
        except Exception as e:
            if not self.closed:
                logging.error(f"Error in receiver: {e}")


    #Helpers

    def _flatten(self, record):
        lang = record.get("in_language")
        if isinstance(lang, dict):
            record["in_language"] = lang.get("identifier","")
        image = record.get("image")
        if isinstance(image, dict):
            record["image"] = image.get("content_url","")
        return record

def main() -> int:
    logging.basicConfig(level=logging.INFO)
    client = Client()

    try:
        logging.info("Conectando")
        client.connect(SERVER_HOST, SERVER_PORT)

        receiver_thread = threading.Thread(target=client.recv_results)
        receiver_thread.start()

        client.send_article_info(INPUT_FILE)

        receiver_thread.join()
    except socket.error:
        if not client.closed:
            logging.error("The connection with the server was lost")
            return 1
    except Exception as e:
        logging.error(e)
        return 2
    finally:
        if not client.closed:
            client.disconnect()

    return 0


if __name__ == "__main__":
    main()
