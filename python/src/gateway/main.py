import os
import logging
import socket
import signal
import multiprocessing
import message_handler
from common import middleware, message_protocol
from results_dispatcher.result_dispatcher import ResultsDispatcher
import itertools

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])

MOM_HOST = os.environ["RABBITMQ_HOST"]
RESULTS_QUEUE = os.environ["RESULTS_QUEUE"]
YEAR_FILTER_QUEUE = os.environ["YEAR_FILTER_QUEUE"]
TOKENIZER_QUEUE = os.environ["TOKENIZER_QUEUE"]

QUERIES_ENV = os.environ.get("QUERIES")
if QUERIES_ENV:
    QUERIES = tuple(int(x.strip()) for x in QUERIES_ENV.split(",") if x.strip())
else:
    QUERIES = (1, 2, 3, 4, 5)

EOFS_EXPECTED = {}
for q in QUERIES:
    nombre_variable = f"Q{q}_EOF_EXPECTED"
    valor = os.environ.get(nombre_variable, "1")
    EOFS_EXPECTED[q] = int(valor)     

client_id_generator = itertools.count(1)


def handle_client_request(client_socket, message_handler,lock):
    year_filter_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, YEAR_FILTER_QUEUE)
    tokenizer_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, TOKENIZER_QUEUE)
    external = message_protocol.external

    try:
        while True:
            msg_type, data = external.recv_msg(client_socket)

            logging.info(f"Se recibio un mensaje del cliente {msg_type}")

            if msg_type == external.MsgType.ARTICLE_RECORD_BATCH:
                year_batch, tokenizer_batch = message_handler.serialize_article_batch(data)
                year_filter_queue.send(year_batch)
                tokenizer_queue.send(tokenizer_batch)
                with lock:
                    external.send_msg(client_socket, external.MsgType.ACK)

            elif msg_type == external.MsgType.END_OF_RECORDS:
                eof_message = message_handler.serialize_eof_message()
                year_filter_queue.send(eof_message)
                tokenizer_queue.send(eof_message)
                with lock:
                    external.send_msg(client_socket, external.MsgType.ACK)
                break
            
    except socket.error:
        logging.error("The connection with the server was lost")
    except Exception as e:
        logging.error(e)
    finally:
        year_filter_queue.close()
        tokenizer_queue.close()


def handle_client_response(client_list):
    results_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, RESULTS_QUEUE)
    dispatcher = ResultsDispatcher(client_list,EOFS_EXPECTED)

    def _consume_result(message, ack, nack):
        try:
            dispatcher.handle(message)
        #TODO: poner en dispatcher
        # except socket.error:
        #     logging.error("The connection with the server was lost")
        #     client_list.pop(client_index)
        #     ack()
        except Exception as e:
            logging.exception(e)
        ack()
    try:
        results_queue.start_consuming(_consume_result)
    except Exception as e:
        logging.exception(e) 
    finally:
        results_queue.close()


def handle_sigterm(server_socket, client_list, sigterm_received):
    logging.info("Signal received in gateway, shutting down gracefully...")
    sigterm_received.value = 1
    try:
        server_socket.shutdown(socket.SHUT_RDWR)
    except Exception as e:
        logging.debug(f"Error shutting down server socket: {e}")
    try:
        server_socket.close()
    except Exception as e:
        logging.debug(f"Error closing server socket: {e}")

    try:
        for item in list(client_list):
            try:
                client_socket = item[1]
                client_socket.shutdown(socket.SHUT_RDWR)
            except Exception as e:
                logging.debug(f"Error shutting down client socket: {e}")
            try:
                client_socket.close()
            except Exception as e:
                logging.debug(f"Error closing client socket: {e}")
    except Exception as e:
        logging.debug(f"Error closing client sockets in list: {e}")


def main():
    logging.basicConfig(level=logging.INFO)

    with multiprocessing.Manager() as manager:
        client_list = manager.list() # es un proxy de una lista compartida entre procesos
        sigterm_received = manager.Value("c_short", 0)
        with multiprocessing.Pool(processes=os.process_cpu_count()) as processes_pool:
            processes_pool.apply_async(handle_client_response, (client_list,))

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
                logging.info("Listening to connections")
                server_socket.bind((SERVER_HOST, SERVER_PORT))
                server_socket.listen()
                signal_handler = lambda signum, frame: handle_sigterm(
                    server_socket, client_list, sigterm_received
                )
                signal.signal(signal.SIGTERM, signal_handler)
                signal.signal(signal.SIGINT, signal_handler)
                while True:
                    try:
                        client_socket, _ = server_socket.accept()

                        logging.info("A new client has connected")
                        message_handler_instance = message_handler.MessageHandler(next(client_id_generator))
                        lock = manager.Lock()
                        client_list.append([message_handler_instance, client_socket,lock])
                        processes_pool.apply_async(
                            handle_client_request,
                            (client_socket, message_handler_instance,lock),
                        )
                    except socket.error:
                        if sigterm_received.value == 0:
                            logging.error("The connection with the client was lost")
                            return 1
                        else:
                            return 0
                    except Exception as e:
                        logging.error(e)
                        return 2
    return 0


if __name__ == "__main__":
    main()
