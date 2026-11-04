import logging
import os
from common.base_worker import BaseWorker
from common.eof_coordinator.exchange_eof_coordinator import ExchangeEofCoordinator
from common.message_protocol.external import Query
from common.word_item import WordItem

HOST = os.environ.get("RABBITMQ_HOST")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", 10))
INPUT_EXCHANGE_NAME = os.environ.get("INPUT_EXCHANGE_NAME")
INPUT_EXCHANGE_PREFIX = os.environ.get("INPUT_EXCHANGE_PREFIX")
REPLICA_ID = os.environ.get("REPLICA_ID", "default_replica")
RESULTS_QUEUE_NAME = os.environ.get("RESULTS_QUEUE_NAME")
TOP_SIZE = int(os.environ.get("TOP_SIZE", 20))
PREVIOUS_STAGE_REPLICAS = int(os.environ.get("PREVIOUS_STAGE_REPLICAS", "1"))

class JoinTop20Worker(BaseWorker):
    def __init__(self, batch_size, host, input_exchange_name, input_exchange_prefix, replica_id, results_queue_name, top_size, previous_stage_replicas):
        super().__init__(batch_size, host)
        self._create_output_queue(results_queue_name)
        self.results_queue_name = results_queue_name
        self.eof_coordinator = ExchangeEofCoordinator(
            replica_id=replica_id,
            previous_stage_replicas=previous_stage_replicas,
            on_barrier=self._on_barrier
        )
        routing_keys = [f"{input_exchange_prefix}_{replica_id}"]
        self._create_exchange_input_handler(input_exchange_name, routing_keys)
        self.top_size = top_size
        self.result_query_id = Query.Q4
        self.data = {}  # Diccionario para almacenar los datos por cliente_id {client_id: [current_top_20]}

    def process_data(self, client_id, message):
        # Base worker llama a este metodo sobre cada articulo dentro del batch
        # message es una lista con los parametros necesarios para esta etapa del articulo
        partial_top = message[0]  # message es una lista con un solo elemento: la lista de WordItem
        new_top = []
        i, j = 0, 0
        # Metemos los elementos entre ambos arreglos en orden descendente
        while i < len(self.data.get(client_id, [])) and j < len(partial_top) and len(new_top) < self.top_size:
            if self.data[client_id][i].total_count() >= partial_top[j].total_count():
                new_top.append(self.data[client_id][i])
                i += 1
            else:
                new_top.append(partial_top[j])
                j += 1
        # Si no se llego al top size porque en el mensaje habia menos elementos, agregamos los restantes del arreglo viejo
        while i < len(self.data.get(client_id, [])) and len(new_top) < self.top_size:
            new_top.append(self.data[client_id][i])
            i += 1
        # Si no se llego al top size porque en el arreglo previo habia menos elementos, agregamos los restantes del mensaje
        while j < len(partial_top) and len(new_top) < self.top_size:
            new_top.append(partial_top[j])
            j += 1
        self.data[client_id] = new_top

    def _on_barrier(self, client_id):
        logging.info(f"Client {client_id}: barrier reached, flushing data")
        top_20 = self.data.pop(client_id, []) # Obtenemos y eliminamos los datos del cliente, nos devuelve un arreglo de WordItem
        self.emit(client_id, self.results_queue_name, [self.result_query_id, top_20]) # Enviamos los top k al results_queue
        self._flush_all(client_id)

def main():
    logging.basicConfig(level=logging.INFO)

    join_top_20_worker = JoinTop20Worker(
        BATCH_SIZE, HOST, INPUT_EXCHANGE_NAME, INPUT_EXCHANGE_PREFIX,
        REPLICA_ID, RESULTS_QUEUE_NAME, TOP_SIZE, PREVIOUS_STAGE_REPLICAS
    )
    join_top_20_worker.start()
    return 0

if __name__ == "__main__":
    main()