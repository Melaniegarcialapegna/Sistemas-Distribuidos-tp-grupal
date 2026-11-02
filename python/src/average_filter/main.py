import os
import logging
from common.base_worker import BaseWorker

REPLICA_ID = os.environ["REPLICA_ID"]

class AverageFilterWorker(BaseWorker):
    def __init__(self, batch_size, host):
        super().__init__(batch_size,host)



def main():
    logging.basicConfig(level=logging.INFO)

    host = os.environ["RABBITMQ_HOST"]
    batch_size = int(os.environ.get("BATCH_SIZE", 10))

    worker = AverageFilterWorker(batch_size,host)

    worker.start()
    return 0


if __name__ == "__main__":
    main()