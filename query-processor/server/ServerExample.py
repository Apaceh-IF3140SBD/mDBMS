from Server import ServerHandler, ServerRunner
from dataclasses import dataclass, asdict
import json

@dataclass
class PersonData:
    name: str
    favourite: list[str]
    age: int

# dummy data
fruit_mapping = {
    "Alice": PersonData(name="Alice", favourite=["Apple", "Avocado"], age=25),
    "Bob": PersonData(name="Bob", favourite=["Banana", "Blueberry"], age=30),
    "Charlie": PersonData(name="Charlie", favourite=["Cherry", "Coconut"], age=35),
    "Diana": PersonData(name="Diana", favourite=["Date", "Dragonfruit"], age=40)
}

class Handler(ServerHandler):
    def handle(self):
        print(f"Connection established with {self.client_address}")

        try:
            data = self.request.recv(1024).decode('utf-8').strip()
            print(f"Received request: {data}")

            person_data = fruit_mapping.get(data)
            if person_data:
                response = asdict(person_data)
            else:
                response = {"error": "Unknown person"}

            response_json = json.dumps(response)
            self.request.sendall(response_json.encode('utf-8'))
        except Exception as e:
            print(f"Error handling client {self.client_address}: {e}")
        finally:
            print(f"Connection closed with {self.client_address}")

if __name__ == "__main__":
    ServerRunner().run(Handler)