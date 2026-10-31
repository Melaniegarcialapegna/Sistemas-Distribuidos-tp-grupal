import os
import logging
import socket
import signal
import multiprocessing
import message_handler
from common import middleware, message_protocol
import itertools

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
YEAR_FILTER_QUEUE = os.environ["YEAR_FILTER_QUEUE"]
TOKENIZER_QUEUE = os.environ["TOKENIZER_QUEUE"]

client_id_generator = itertools.count(1)


def handle_client_request(client_socket, message_handler):
    year_filter_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, YEAR_FILTER_QUEUE)
    tokenizer_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, TOKENIZER_QUEUE)
    external = message_protocol.external

    try:
        while True:
            msg_type, data = external.recv_msg(client_socket)

            if msg_type == external.MsgType.ARTICLE_RECORD_BATCH:
                year_batch, tokenizer_batch = message_handler.serialize_article_batch(data)
                year_filter_queue.send(year_batch)
                tokenizer_queue.send(tokenizer_batch)
                external.send_msg(client_socket, external.MsgType.ACK)

            elif msg_type == external.MsgType.END_OF_RECORDS:
                eof_message = message_handler.serialize_eof_message()
                year_filter_queue.send(eof_message)
                tokenizer_queue.send(eof_message)
                external.send_msg(client_socket, external.MsgType.ACK)
                return
            
    except socket.error:
        logging.error("The connection with the server was lost")
    except Exception as e:
        logging.error(e)
    finally:
        year_filter_queue.close()
        tokenizer_queue.close()


def handle_client_response(client_list):
    input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)

    def _consume_result(message, ack, nack):
        client_index = 0
        try:
            for [message_handler_instance, client_socket] in client_list:
                deserialized_message = (
                    message_handler_instance.deserialize_result_message(message)
                )

                if not deserialized_message:
                    client_index += 1
                    continue

                message_protocol.external.send_msg(
                    client_socket,
                    message_protocol.external.MsgType.FRUIT_TOP,
                    deserialized_message,
                )
                message_protocol.external.recv_msg(client_socket)
                break
            client_list.pop(client_index)
            ack()
        except socket.error:
            logging.error("The connection with the server was lost")
            client_list.pop(client_index)
            ack()
        except Exception as e:
            logging.error(e)
            nack()
            input_queue.stop_consuming()

    input_queue.start_consuming(_consume_result)
    input_queue.close()


def handle_sigterm(server_socket, client_list, sigterm_received):
    server_socket.shutdown(socket.SHUT_RDWR)
    for [_, client_socket] in client_list:
        client_socket.shutdown(socket.SHUT_RDWR)
    sigterm_received.value = 1


def main():
    logging.basicConfig(level=logging.INFO)

    with multiprocessing.Manager() as manager:
        client_list = manager.list()
        sigterm_received = manager.Value("c_short", 0)
        with multiprocessing.Pool(processes=os.process_cpu_count()) as processes_pool:
            processes_pool.apply_async(handle_client_response, (client_list,))

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
                logging.info("Listening to connections")
                server_socket.bind((SERVER_HOST, SERVER_PORT))
                server_socket.listen()
                signal.signal(
                    signal.SIGTERM,
                    lambda signum, frame: handle_sigterm(
                        server_socket, client_list, sigterm_received
                    ),
                )
                while True:
                    try:
                        client_socket, _ = server_socket.accept()

                        logging.info("A new client has connected")
                        message_handler_instance = message_handler.MessageHandler(next(client_id_generator))
                        client_list.append([message_handler_instance, client_socket])
                        processes_pool.apply_async(
                            handle_client_request,
                            (client_socket, message_handler_instance),
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
