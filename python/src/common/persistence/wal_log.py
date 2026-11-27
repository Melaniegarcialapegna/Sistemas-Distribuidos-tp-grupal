import json
import logging
import os
from pathlib import Path

# TODO: revisar si el json es el mejor formato para el snapshot

class StateManager:
    SNAPSHOT_FILENAME = "snapshot.json"
    SNAPSHOT_TMP_FILENAME = "snapshot.json.tmp"
    WAL_FILENAME = "wal.log"

    def __init__(self, state_dir, snapshot_every):
        if snapshot_every <= 0:
            raise ValueError("snapshot_every must be a positive integer")

        self._state_dir = Path(state_dir) # directorio donde se guarda el estado
        self._snapshot_path = self._state_dir / self.SNAPSHOT_FILENAME
        self._snapshot_tmp_path = self._state_dir / self.SNAPSHOT_TMP_FILENAME
        self._wal_path = self._state_dir / self.WAL_FILENAME
        self._snapshot_every = snapshot_every

        self._lsn = 0
        self._entries_since_snapshot = 0
        self._wal_file = None

    # Se encarga de cargar la persistencia cuando comienza un nodo (ya sea al inicio o despues de una caida)
    def start(self):
        # ver de crear para cada worker su propia carpeta state?
        self._state_dir.mkdir(parents=True, exist_ok=True)

        snapshot_state = self._load_snapshot() # carga el ultimo snapshot conocido
        wal_deltas = self._replay_wal() # le agrega las lineas que no llegaron al snapshot

        self._wal_file = open(self._wal_path, "a", encoding="utf-8")
        return snapshot_state, wal_deltas


    def append(self, client_id, batch_id, delta):
        if self._wal_file is None:
            raise RuntimeError("WALLog.load() must be called before append()")

        next_lsn = self._lsn + 1
        record = {
            "lsn": next_lsn,
            "client_id": client_id,
            "batch_id": batch_id,
            "delta": delta,
        }

        self._wal_file.write(json.dumps(record) + "\n")
        self._wal_file.flush()
        os.fsync(self._wal_file.fileno()) # Fuerza al SO a escribir inmediatamente en disco

        self._lsn = next_lsn
        self._entries_since_snapshot += 1

    def should_snapshot(self) -> bool:
        return self._entries_since_snapshot > self._snapshot_every

    def create_snapshot(self, state_callback) -> None:
        if self._wal_file is None:
            raise RuntimeError("WALLog.load() must be called before force_snapshot()")

        snapshot_payload = {
            "lsn": self._lsn,
            "state": state_callback(),
        }

        with open(self._snapshot_tmp_path, "w", encoding="utf-8") as tmp_file:
            json.dump(snapshot_payload, tmp_file)
            tmp_file.flush()
            os.fsync(tmp_file.fileno())

        os.replace(self._snapshot_tmp_path, self._snapshot_path) # reemplaza el archivo temporal por el oficial
        self._clean_wal()

    def close(self):
        if self._wal_file is not None:
            self._wal_file.flush()
            self._wal_file.close()
            self._wal_file = None

    def _load_snapshot(self):
        if not self._snapshot_path.exists():
            return None

        with open(self._snapshot_path, "r", encoding="utf-8") as snapshot_file:
            data = json.load(snapshot_file)

        self._lsn = data.get("lsn", 0)
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
                    logging.warning(
                        "Skipping corrupt WAL line %d in %s: %s",
                        line_number,
                        self._wal_path,
                        err,
                    )
                    continue

                self._lsn = max(self._lsn, lsn)
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
