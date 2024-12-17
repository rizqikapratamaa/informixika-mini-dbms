class DummyDatabase:
    global activeDB
    
    def __init__(self):
        # initialize an empty dictionary to store data
        self.data = {}
        activeDB = self

    def write(self, id, value):
        self.data[id] = value
        print(f"Data written: ID={id}, Value={value}")

    def delete(self, id):
        if id in self.data:
            del self.data[id]
            print(f"Data with ID={id} deleted.")
        else:
            print(f"ID={id} not found in the database.")

    def get(self, id):
        value = self.data.get(id)
        if value is not None:
            print(f"Data retrieved: ID={id}, Value={value}")
        else:
            print(f"ID={id} not found in the database.")
        return value

    def __str__(self):
        """
        Returns a string representation of the database for quick inspection.
        """
        return f"DummyDatabase: {self.data}"

activeDB : DummyDatabase = None