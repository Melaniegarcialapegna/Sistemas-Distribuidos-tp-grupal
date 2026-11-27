import json
import logging
import os
from pathlib import Path

class StateManager:
    SNAPSHOT_FILENAME = "snapshot.json"
    SNAPSHOT_TMP_FILENAME = "snapshot.json.tmp"
    WAL_FILENAME = "wal.log"

    def __init__(self, state_dir, snapshot_every):
        if snapshot_every <= 0:
            raise ValueError("snapshot_every must be a positive integer")

        self._state_dir = Path(state_dir)
        self._snapshot_path = self._state_dir / self.SNAPSHOT_FILENAME
        self._snapshot_tmp_path = self._state_dir / self.SNAPSHOT_TMP_FILENAME
        self._wal_path = self._state_dir / self.WAL_FILENAME
        self._snapshot_every = snapshot_every

        self._lsn = 0
        self._entries_since_snapshot = 0
        self._processed = set()  # {(client_id, batch_id)}
        self._wal_file = None

    def start(self):
        self._state_dir.mkdir(parents=True, exist_ok=True)
        snapshot_state = self._load_snapshot()
        wal_deltas = self._replay_wal()
        self._wal_file = open(self._wal_path, "a", encoding="utf-8")
        return snapshot_state, wal_deltas

    def already_processed(self, client_id, batch_id):
        return (client_id, batch_id) in self._processed

    def append(self, client_id, batch_id, delta):
        """Escribe un delta en el WAL y hace fsync. Registra el par como procesado."""
        if self._wal_file is None:
            raise RuntimeError("StateManager.start() must be called before append()")

        next_lsn = self._lsn + 1
        record = {
            "lsn": next_lsn,
            "client_id": client_id,
            "batch_id": batch_id,
            "delta": delta,
        }

        self._wal_file.write(json.dumps(record) + "\n")
        self._wal_file.flush()
        os.fsync(self._wal_file.fileno())

        self._lsn = next_lsn
        self._processed.add((client_id, batch_id))
        self._entries_since_snapshot += 1

    def should_snapshot(self) -> bool:
        return self._entries_since_snapshot >= self._snapshot_every

    def create_snapshot(self, state_callback) -> None:
        """Crea un snapshot atómico del estado y trunca el WAL."""
        if self._wal_file is None:
            raise RuntimeError("StateManager.start() must be called before create_snapshot()")

        snapshot_payload = {
            "lsn": self._lsn,
            "processed_batches": [list(pair) for pair in self._processed],
            "state": state_callback(),
        }

        with open(self._snapshot_tmp_path, "w", encoding="utf-8") as tmp_file:
            json.dump(snapshot_payload, tmp_file)
            tmp_file.flush()
            os.fsync(tmp_file.fileno())

        os.replace(self._snapshot_tmp_path, self._snapshot_path)
        self._clean_wal()

    def close(self):
        if self._wal_file is not None:
            self._wal_file.flush()
            self._wal_file.close()
            self._wal_file = None

    def _load_snapshot(self):
        if not self._snapshot_path.exists():
            return None

        with open(self._snapshot_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        self._lsn = data.get("lsn", 0)
        self._processed = {
            (pair[0], pair[1]) for pair in data.get("processed_batches", [])
        }
        return data.get("state")

    def _replay_wal(self):
        if not self._wal_path.exists():
            return []

        deltas = []
        with open(self._wal_path, "r", encoding="utf-8") as wal_file:
            for line_number, raw_line in enumerate(wal_file, start=1):
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    client_id = record["client_id"]
                    batch_id = record["batch_id"]
                    delta = record["delta"]
                    lsn = record["lsn"]
                except (json.JSONDecodeError, KeyError, TypeError) as err:
                    logging.warning("Skipping corrupt WAL line %d: %s", line_number, err)
                    continue

                self._lsn = max(self._lsn, lsn)
                self._processed.add((client_id, batch_id))
                self._entries_since_snapshot += 1
                deltas.append(delta)

        return deltas

    def _clean_wal(self):
        if self._wal_file is None:
            return
        self._wal_file.seek(0)
        self._wal_file.truncate(0)
        self._wal_file.flush()
        os.fsync(self._wal_file.fileno())
        self._entries_since_snapshot = 0
