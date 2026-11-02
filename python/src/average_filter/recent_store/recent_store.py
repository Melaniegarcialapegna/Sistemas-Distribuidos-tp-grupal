import os

class RecentStore:
    def __init__(self, path):
        self.path = path
        self._files = {} #{client_id: file_path}
        os.makedirs(path, exist_ok=True)

    def append(self, client_id, url, length):
        with open(self._get_file_path(client_id), "a", encoding="utf-8") as file:
            file.write(f"{url},{length}\n")

    def read_all(self, client_id):
        file_path = self._get_file_path(client_id)
        if not os.path.exists(file_path):
            return
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                url, length = line.rstrip("\n").rsplit(",", 1)
                yield url, int(length)

    def delete(self, client_id):
        self._close(client_id)
        file_path = self._get_file_path(client_id)
        if os.path.exists(file_path):
            os.remove(file_path)

    def close_all(self):
        for client_id in self._files:
            self._close(client_id)

    #Helpers
    
    def _get_file_path(self, client_id):
        return os.path.join(self.path, f"recent_{client_id}.txt")

    def _close(self, client_id):
        f = self._files.pop(client_id, None)
        if f is not None:
            f.close()