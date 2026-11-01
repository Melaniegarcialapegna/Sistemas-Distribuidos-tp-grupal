import os
import logging
import threading
from common.base_worker import BaseWorker

class SumWorker(BaseWorker):
    def __init__(self, batch_size, host):
        super().__init__(batch_size,host)
        self.totals = {} #{client_id: [total_len, total_abstracts]}


    def process_data(self, client_id, message):
        total = self.totals.setdefault(client_id, [0, 0])
        total[0] += message[0] #len of the abstract
        total[1] += 1 # one abstract more


def main():
    logging.basicConfig(level=logging.INFO)
    sum_worker = SumWorker()
    sum_worker.start()
    return 0


if __name__ == "__main__":
    main()