import os
import logging
import socket
import signal
import threading
import pyarrow.parquet as pq

from common import message_protocol

INPUT_FILE = os.environ["INPUT_FILE"]
OUTPUT_FILE = os.environ["OUTPUT_FILE"]
SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])


ROW_BATCH_SIZE = 1000 # add to config
USABLE_COLUMNS = ["name", "url", "date_modified", "sections", "image", "abstract"] 

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
        self.server_socket.connect((server_host, server_port))

    def disconnect(self):
        if self.server_socket:
            self.server_socket.shutdown(socket.SHUT_RDWR)

    def send_article_info(self, input_file):
        logging.info("Sending article info")
        parquet_file = pq.ParquetFile(input_file)
        for batch in parquet_file.iter_batches(batch_size=ROW_BATCH_SIZE, columns=USABLE_COLUMNS):
                batch_data = batch.to_pylist()
                message_protocol.external.send_msg(
                    self.server_socket,
                    message_protocol.external.MsgType.ARTICLE_RECORD_BATCH,
                    batch_data
                )

        message_protocol.external.send_msg(
            self.server_socket, message_protocol.external.MsgType.END_OF_RECODS
        )

    def recv_results(self):
        try:
            while not self.closed:
                logging.info("Receiving results")
                result_message = message_protocol.external.recv_msg(self.server_socket)
                
                msg_type = result_message[0]
                payload = result_message[1]

                if msg_type == message_protocol.external.MsgType.RESULT:
                    print(payload)
                    # send ACK
                    message_protocol.external.send_msg(
                        self.server_socket, message_protocol.external.MsgType.ACK
                    )
        except Exception as e:
            if not self.closed:
                logging.error(f"Error in receiver: {e}")


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    client = Client()

    try:
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
