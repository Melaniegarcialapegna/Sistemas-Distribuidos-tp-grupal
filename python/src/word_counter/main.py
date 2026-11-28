import logging
import os
from common.base_worker import StatefulWorker
from common.eof_coordinator.exchange_eof_coordinator import ExchangeEofCoordinator
from common.message_protocol.external import Query
from common.word_item import WordItem
from common.utils import fnv1a_32


HOST = os.environ.get("RABBITMQ_HOST")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", 10))
INPUT_EXCHANGE_NAME = os.environ.get("INPUT_EXCHANGE_NAME")
INPUT_EXCHANGE_PREFIX = os.environ.get("INPUT_EXCHANGE_PREFIX")
REPLICA_ID = os.environ.get("REPLICA_ID", "default_replica")
RESULTS_QUEUE_NAME = os.environ.get("RESULTS_QUEUE_NAME")
TOP_EXCHANGE_NAME = os.environ.get("TOP_EXCHANGE_NAME")
TOP_EXCHANGE_RK_PREFIX = os.environ.get("TOP_EXCHANGE_RK_PREFIX")
TOP_SIZE = int(os.environ.get("TOP_SIZE", 20))
PREVIOUS_STAGE_REPLICAS = int(os.environ.get("PREVIOUS_STAGE_REPLICAS", "1"))
NEXT_STAGE_REPLICAS = int(os.environ.get("NEXT_STAGE_REPLICAS", "1"))
SNAPSHOT_EVERY = int(os.environ.get("SNAPSHOT_EVERY", 1000))
STATE_DIR = f"data/state/worker_{REPLICA_ID}"

class WordCounterWorker(StatefulWorker):
    def __init__(self, batch_size, host, input_exchange_name, input_exchange_prefix, replica_id, results_queue_name, top_exchange_name, top_exchange_rk_prefix, top_size, previous_stage_replicas, next_stage_replicas, state_dir, snapshot_every):
        super().__init__(batch_size, host, state_dir, snapshot_every)
        self.next_stage_replicas = next_stage_replicas
        top_routing_keys = [f"{top_exchange_rk_prefix}_{i}" for i in range(self.next_stage_replicas)]
        self._create_output_queue(results_queue_name)
        self._create_output_exchange(top_exchange_name, top_routing_keys)
        self.results_queue_name = results_queue_name
        self.top_exchange_name = top_exchange_name
        self.top_exchange_rk_prefix = top_exchange_rk_prefix
        self.top_size = top_size
        self.eof_coordinator = ExchangeEofCoordinator(
            replica_id=replica_id,
            previous_stage_replicas=previous_stage_replicas,
            on_barrier=self._on_barrier
        )
        routing_keys = [f"{input_exchange_prefix}_{replica_id}"]
        self._create_exchange_input_handler(input_exchange_name, routing_keys,replica_id)
        self.result_query_id = Query.Q3
        self.data = {}  # Diccionario para almacenar los datos por cliente_id {client_id: {word: [word_count, sent]}}
        self._pending_deltas = {} # {client_id: {word: {"eng": int, "fre": int, "sent": bool}}}

    def _accumulate_word(self, client_id, word, in_language):
        word_item = WordItem.create_in_language_word_count(word, in_language)
        entry = self.data.setdefault(client_id, {}).setdefault(
            word, [WordItem(word, 0, 0), False]
        )
        entry[0] = entry[0] + word_item
        return entry, word_item.eng_count, word_item.fre_count

    def process_data(self, client_id, message):
        # Base worker llama a este metodo sobre cada articulo dentro del batch
        # message es una lista con los parametros necesarios para esta etapa del articulo
        word, in_language = message
        entry, eng_count, fre_count = self._accumulate_word(client_id, word, in_language)
        if (not entry[1]) and entry[0].intersection_reached():
            entry[1] = True
            self.emit(client_id, self.results_queue_name, [self.result_query_id, word])
        self._record_batch_delta(client_id, word, eng_count, fre_count, entry[1])

    def _on_barrier(self, client_id):
        logging.info(f"Client {client_id}: barrier reached, flushing data")
        client_counts = self.data.pop(client_id, {}) # Obtenemos y eliminamos los datos del cliente, nos devuelve un diccionario {word: [WordItem, sent]}
        word_items = [entry[0] for entry in client_counts.values()] # Obtenemos solo los WordItem, ignorando el flag de enviado
        sorted_counts = sorted(word_items, key=lambda count: count.total_count(), reverse=True)[:self.top_size] # Ordenamos y obtenemos los top k

        client_id_bytes = client_id.to_bytes((client_id.bit_length() + 7) // 8 or 1, "big")
        routing_key = f"{self.top_exchange_rk_prefix}_{fnv1a_32(client_id_bytes) % self.next_stage_replicas}" # Calculamos el routing key para enviar al top_queue

        self.emit(client_id, self.top_exchange_name, [sorted_counts], routing_keys=[routing_key]) # Enviamos los top k al top_queue

        self._flush_all(client_id)

    def serialize_state(self):
        return {
            str(client_id): {
                word: [entry[0].eng_count, entry[0].fre_count, entry[1]]
                for word, entry in words.items()
            }
            for client_id, words in self.data.items()
        }

    def restore_snapshot(self, state):
        self.data = {
            int(client_id): {
                word: [WordItem(word, counts[0], counts[1]), counts[2]]
                for word, counts in words.items()
            }
            for client_id, words in state.items()
        }

    def apply_delta(self, delta):
        op = delta.get("op")
        client_id = delta["client_id"]
        if op == "eof":
            return
        client_data = self.data.setdefault(client_id, {})
        for word, (eng_delta, fre_delta, sent) in delta["words"].items():
            entry = client_data.setdefault(word, [WordItem(word, 0, 0), False])
            entry[0] = entry[0] + WordItem(word, eng_delta, fre_delta)
            entry[1] = entry[1] or sent

    def _record_batch_delta(self, client_id, word, eng_inc, fre_inc, sent):
        client_delta = self._pending_deltas.setdefault(client_id, {})
        word_delta = client_delta.setdefault(
            word, {"eng": 0, "fre": 0, "sent": False}
        )
        word_delta["eng"] += eng_inc
        word_delta["fre"] += fre_inc
        word_delta["sent"] = word_delta["sent"] or sent

    def build_batch_delta(self, client_id):
        client_delta = self._pending_deltas.pop(client_id, None)
        if not client_delta:
            return None

        return {
            "op": "batch_update",
            "client_id": client_id,
            "words": {
                word: [vals["eng"], vals["fre"], vals["sent"]]
                for word, vals in client_delta.items()
            },
        }

def main():
    logging.basicConfig(level=logging.INFO)

    word_counter_worker = WordCounterWorker(
        BATCH_SIZE, HOST, INPUT_EXCHANGE_NAME, INPUT_EXCHANGE_PREFIX,
        REPLICA_ID, RESULTS_QUEUE_NAME, TOP_EXCHANGE_NAME,
        TOP_EXCHANGE_RK_PREFIX, TOP_SIZE, PREVIOUS_STAGE_REPLICAS, NEXT_STAGE_REPLICAS,
        STATE_DIR, SNAPSHOT_EVERY,
    )
    word_counter_worker.start()
    return 0

if __name__ == "__main__":
    main()