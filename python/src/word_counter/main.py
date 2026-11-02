import logging
import os
from common.base_worker import BaseWorker
from common.eof_coordinator.exchange_eof_coordinator import ExchangeEofCoordinator
from common.word_item import WordItem
from common.utils import fnv1a_32


class WordCounterWorker(BaseWorker):
    def __init__(self, batch_size, host, input_exchange_name, input_exchange_prefix, replica_id, results_queue_name, top_exchange_name, top_exchange_rk_prefix, top_size=20):
        super().__init__(batch_size, host)
        self.next_stage_replicas = int(os.environ.get("NEXT_STAGE_REPLICAS", "1"))
        top_routing_keys = [f"{top_exchange_rk_prefix}_{i}" for i in range(self.next_stage_replicas)]
        self._create_output_queue(results_queue_name)
        self._create_output_exchange(top_exchange_name, top_routing_keys)
        self.results_queue_name = results_queue_name
        self.top_exchange_name = top_exchange_name
        self.top_exchange_rk_prefix = top_exchange_rk_prefix
        self.top_size = top_size
        self.eof_coordinator = ExchangeEofCoordinator(
            replica_id=replica_id,
            previous_stage_replicas=int(os.environ.get("PREVIOUS_STAGE_REPLICAS", "1")),
            on_barrier=self._on_barrier
        )
        self.input_exchange_name = input_exchange_name
        self.input_exchange_prefix = input_exchange_prefix
        self.replica_id = replica_id
        routing_keys = [f"{input_exchange_prefix}_{replica_id}"]
        self._create_exchange_input_handler(input_exchange_name, routing_keys)
        self.data = {}  # Diccionario para almacenar los datos por cliente_id {client_id: {word: [word_count, sent]}}

    def process_data(self, client_id, message):
        # Base worker llama a este metodo sobre cada articulo dentro del batch
        # message es una lista con los parametros necesarios para esta etapa del articulo
        word, in_language = message
        word_item = WordItem.create_in_language_word_count(word, in_language)
        self.data.setdefault(client_id, {}).setdefault(word, [WordItem(word, 0, 0), False])
        self.data[client_id][word][0] = self.data[client_id][word][0] + word_item
        if (not self.data[client_id][word][-1]) and (self.data[client_id][word][0].intersection_reached()):
            self.data[client_id][word][-1] = True
            self.emit(client_id, self.results_queue_name, [word])

    def _on_barrier(self, client_id):
        logging.info(f"Client {client_id}: barrier reached, flushing data")
        client_counts = self.data.pop(client_id, {}) # Obtenemos y eliminamos los datos del cliente, nos devuelve un diccionario {word: [WordItem, sent]}
        word_items = [entry[0] for entry in client_counts.values()] # Obtenemos solo los WordItem, ignorando el flag de enviado
        sorted_counts = sorted(word_items, key=lambda count: count.total_count(), reverse=True)[:self.top_size] # Ordenamos y obtenemos los top k

        client_id_bytes = client_id.to_bytes((client_id.bit_length() + 7) // 8 or 1, "big")
        routing_key = f"{self.top_exchange_rk_prefix}_{fnv1a_32(client_id_bytes) % self.next_stage_replicas}" # Calculamos el routing key para enviar al top_queue

        self.emit(client_id, self.top_exchange_name, [sorted_counts], routing_keys=[routing_key]) # Enviamos los top k al top_queue

        self._flush_all(client_id)

def main():
    logging.basicConfig(level=logging.INFO)
    host = os.environ["RABBITMQ_HOST"]
    batch_size = int(os.environ.get("BATCH_SIZE", 10))
    input_exchange_name = os.environ["INPUT_EXCHANGE_NAME"]
    input_exchange_prefix = os.environ["INPUT_EXCHANGE_PREFIX"]
    replica_id = os.environ["REPLICA_ID"]
    results_queue_name = os.environ["RESULTS_QUEUE_NAME"]
    top_exchange_name = os.environ["TOP_EXCHANGE_NAME"]
    top_exchange_rk_prefix = os.environ["TOP_EXCHANGE_RK_PREFIX"]
    top_size = int(os.environ.get("TOP_SIZE", 20))

    word_counter_worker = WordCounterWorker(batch_size, host, input_exchange_name, input_exchange_prefix, replica_id, results_queue_name, top_exchange_name, top_exchange_rk_prefix, top_size)
    word_counter_worker.start()
    return 0

if __name__ == "__main__":
    main()