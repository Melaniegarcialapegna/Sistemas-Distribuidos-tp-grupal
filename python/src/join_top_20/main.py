import logging
import os
from common.base_worker import BaseWorker
from common.eof_coordinator.exchange_eof_coordinator import ExchangeEofCoordinator
from common.word_item import WordItem

class JoinTop20Worker(BaseWorker):
    def __init__(self, batch_size, host, input_exchange_name, input_exchange_prefix, replica_id, results_queue_name, top_size=20):
        super().__init__(batch_size, host)
        self._create_output_queue(results_queue_name)
        self.results_queue_name = results_queue_name
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
        self.top_size = top_size
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
        self.emit(client_id, self.results_queue_name, [top_20]) # Enviamos los top k al results_queue
        self._flush_all(client_id)

def main():
    logging.basicConfig(level=logging.INFO)
    host = os.environ["RABBITMQ_HOST"]
    batch_size = int(os.environ.get("BATCH_SIZE", 10))
    input_exchange_name = os.environ["INPUT_EXCHANGE_NAME"]
    input_exchange_prefix = os.environ["INPUT_EXCHANGE_PREFIX"]
    replica_id = os.environ["REPLICA_ID"]
    top_size = int(os.environ.get("TOP_SIZE", 20))
    results_queue_name = os.environ["RESULTS_QUEUE_NAME"]

    word_counter_worker = JoinTop20Worker(batch_size, host, input_exchange_name, input_exchange_prefix, replica_id, results_queue_name, top_size)
    word_counter_worker.start()
    return 0

if __name__ == "__main__":
    main()