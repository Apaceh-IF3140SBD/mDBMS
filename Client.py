import socket
import json

class Client:
    
    HOST = "localhost"
    PORT = 65432
    
    def send_request(self, query: str = "", host: str = HOST, port: int = PORT):
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect((host, port))
            
            client.send(query.encode('utf-8'))
            
            response = client.recv(1024*1024).decode('utf-8')
            response_data = json.loads(response)
            
            if "header" in response_data:
                print(response_data["header"])
            
            if "data" in response_data:
                data = response_data["data"]
                for row in data:
                    print(row)
        except Exception as e:
            print(f"Error: {e}")
        finally:
            client.close()
    
    def run(self):
        while True:
            query = input("Enter a query: ").strip()
            if query.lower() == 'exit':
                break
            self.send_request(query)
    
if __name__ == "__main__":
    Client().run()