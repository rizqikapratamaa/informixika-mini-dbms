import pickle

class HashIndex:
    def __init__(self):
        self.index = {}

    def insert(self, key, row_offset):
        """
        memasukkan nilai indeks.
        args:
            key: nilai dari kolom yang diindeks.
            row_offset: offset atau lokasi baris pada file data.
        """
        if key not in self.index:
            self.index[key] = []
        self.index[key].append(row_offset)

    def search(self, key):
        """
        mencari nilai dalam indeks.
        args:
            key: nilai kolom yang dicari.
        returns:
            List[int]: offset baris yang sesuai dengan key.
        """
        return self.index.get(key, [])

    def save(self, filepath):
        """
        menyimpan indeks ke file.
        args:
            filepath: lokasi file untuk menyimpan indeks.
        """
        with open(filepath, 'wb') as f:
            pickle.dump(self.index, f)

    def load(self, filepath):
        """
        memuat indeks dari file.
        args:
            filepath: lokasi file tempat indeks disimpan.
        """
        with open(filepath, 'rb') as f:
            self.index = pickle.load(f)
