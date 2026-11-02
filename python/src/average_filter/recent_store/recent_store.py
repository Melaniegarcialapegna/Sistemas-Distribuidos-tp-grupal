import os

class RecentStore:
    def __init__(self, path):
        self.path = path
        self._files = {} #{client_id: file_path}

    def append(self, client_id, url, length):
        if client_id not in self._files:
            file_path = self._get_file_path(client_id) 
            self._files[client_id] = file_path
        else:
            file_path = self._files[client_id]

        with open(file_path, "a") as f:
            f.write(f"{url},{length}\n")

        self._files[client_id].close()

    def read_all(self, client_id):
        file_path = self._get_file_path(client_id)
        with open(file_path, "r") as f:
            for line in f:
                yield line.strip().split(",")

        os.remove(file_path)
        del self._files[client_id]

    #Helpers
    
    def _get_file_path(self, client_id):
        return os.path.join(self.path, f"recent_{client_id}.txt")