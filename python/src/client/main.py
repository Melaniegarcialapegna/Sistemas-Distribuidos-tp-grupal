import csv
import glob
import os
import logging
import socket
import signal
import sys
import threading
import time
import pyarrow.parquet as pq

from common import message_protocol

INPUT_FILE = os.environ["INPUT_FILE"]
SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
OUTPUT_DIR = os.environ["OUTPUT_DIR"]
ROW_BATCH_SIZE = int(os.environ.get("ROW_BATCH_SIZE", 1000))
USABLE_COLUMNS = ["name", "url", "date_modified", "sections", "image", "abstract", "in_language"] 
QUERIES_ENV = os.environ.get("QUERIES")
if QUERIES_ENV:
    _query_map = {
        1: message_protocol.external.Query.Q1,
        2: message_protocol.external.Query.Q2,
        3: message_protocol.external.Query.Q3,
        4: message_protocol.external.Query.Q4,
        5: message_protocol.external.Query.Q5,
    }
    EXPECTED_QUERIES = {_query_map[int(q.strip())] for q in QUERIES_ENV.split(",") if q.strip()}
else:
    EXPECTED_QUERIES = {
        message_protocol.external.Query.Q1,
        message_protocol.external.Query.Q2,
        message_protocol.external.Query.Q3,
        message_protocol.external.Query.Q4,
        message_protocol.external.Query.Q5,
    }


class Client:

    def __init__(self):
        self.closed = False
        self.server_socket = None
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self.handle_sigterm)
        self._prev_sigint_handler = signal.signal(signal.SIGINT, self.handle_sigterm)

    def handle_sigterm(self, signum, frame):
        if self.closed:
            return
        logging.info(f"Received signal {signum}, stopping client...")
        self.closed = True
        self.disconnect()

        prev_handler = self._prev_sigterm_handler if signum == signal.SIGTERM else self._prev_sigint_handler
        if callable(prev_handler):
            prev_handler(signum, frame)
        else:
            sys.exit(0)

    def connect(self, server_host, server_port):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        while not self.closed:
            try:
                self.server_socket.connect((server_host, server_port))
                logging.info("¡Conectado exitosamente al Gateway!")
                break
            except socket.error:
                if self.closed:
                    break
                logging.info("Esperando a que el Gateway esté disponible...")
                time.sleep(1)

    def disconnect(self):
        self.closed = True
        sock = self.server_socket
        self.server_socket = None
        if sock:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except Exception as e:
                logging.debug(f"Error shutting down socket: {e}")
            try:
                sock.close()
            except Exception as e:
                logging.debug(f"Error closing socket: {e}")

    def send_article_info(self, input_file):

        if os.path.isdir(input_file):
            parquet_files = sorted(glob.glob(os.path.join(input_file, "*.parquet")))
        elif os.path.isfile(input_file):
            parquet_files = [input_file]
        else:
            raise FileNotFoundError(f"Invalid file path: {input_file}")

        rows_sent = 0
        
        for parquet_file in parquet_files:
            if self.closed:
                return
            logging.info(f"Enviando datos de artículos desde el archivo: {parquet_file}")
            parquet = pq.ParquetFile(parquet_file)
            for batch in parquet.iter_batches(batch_size=ROW_BATCH_SIZE, columns=USABLE_COLUMNS):
                if self.closed:
                    return
                batch_data = []
                for r in batch.to_pylist():
                    r = self._flatten(r)
                    batch_data.append(r)

                message_protocol.external.send_msg(
                    self.server_socket,
                    message_protocol.external.MsgType.ARTICLE_RECORD_BATCH,
                    batch_data
                )
                rows_sent += len(batch_data)

        if not self.closed and self.server_socket:
            logging.info(f"Se enviaron {rows_sent} filas de artículos en total.")
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
                    self._save_rows(query_id, rows)
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

    def _save_rows(self, query_id, rows):
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        filename = os.path.join(OUTPUT_DIR, f"result_Q{query_id}.csv")

        if query_id == message_protocol.external.Query.Q2:
            images_dir = os.path.join(OUTPUT_DIR, "images")
            os.makedirs(images_dir, exist_ok=True)

            with open(filename, "a", newline="", encoding="utf-8") as csvfile:
                csv_writer = csv.writer(csvfile, delimiter=",", quotechar='"')
                for row in rows:
                    name = row[0]
                    url = row[1] if len(row) > 1 else ""
                    img_bytes = row[2] if len(row) > 2 else None

                    # Solo se guarda en disco si efectivamente se recibieron bytes de imagen
                    if img_bytes and isinstance(img_bytes, (bytes, bytearray)):
                        # Sanitizar el nombre para evitar caracteres inválidos como '/' en títulos
                        safe_name = "".join(c if c.isalnum() or c in " ._-" else "_" for c in name)
                        img_path = os.path.join(images_dir, f"{safe_name}.png")
                        try:
                            with open(img_path, "wb") as imgfile:
                                imgfile.write(img_bytes)
                        except OSError as e:
                            logging.error(f"Error guardando imagen {img_path}: {e}")

                    # En el CSV de Q2 se escribe [name, url]
                    csv_writer.writerow([name, url])
        else:
            with open(filename, "a", newline="", encoding="utf-8") as csvfile:
                csv_writer = csv.writer(csvfile, delimiter=",", quotechar='"')
                for row in rows:
                    csv_writer.writerow(row)

def main() -> int:
    logging.basicConfig(level=logging.INFO)
    client = Client()

    try:
        logging.info("Conectando")
        client.connect(SERVER_HOST, SERVER_PORT)
        if client.closed:
            return 0

        receiver_thread = threading.Thread(target=client.recv_results, daemon=True)
        receiver_thread.start()

        start = time.time()

        logging.info("Enviando informacion de articulos")

        client.send_article_info(INPUT_FILE)

        t_envio = time.time() - start
        logging.info(f"Tiempo de envio: {t_envio:.2f} segundos")

        logging.info("Esperando resultados")

        receiver_thread.join()

        logging.info("Todos los resultados recibidos, cerrando cliente. Tiempo total: {:.2f} segundos (Tiempo de envio: {:.2f} segundos)".format(time.time() - start, t_envio))
    except socket.error:
        if not client.closed:
            logging.error("The connection with the server was lost")
            return 1
    except Exception as e:
        if not client.closed:
            logging.error(e)
            return 2
    finally:
        if not client.closed:
            client.disconnect()

    return 0


if __name__ == "__main__":
    main()
