import socket
import json
import traceback

class Client:
    
    HOST = "localhost"
    PORT = 65432
    
    def __init__(self):
        self.begin_transaction = False
        self.running = True
        self.transaction_id = -3
    
    def send_request(self, query: str = ""):
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect((Client.HOST, Client.PORT))
            request = {
                "query": query,
                "transaction_id": self.transaction_id
            }
            dumps = json.dumps(request)
            client.send(dumps.encode('utf-8'))
            response = client.recv(1024*1024).decode('utf-8')
            tupples = json.loads(response)
            if (len(tupples) != 2):
                return tupples
            data, t_id = tupples
            self.transaction_id = t_id
            return data
        except Exception as e:
            print(f"Error: {e}")
            print("Error: ", e)
            traceback.print_exc()
        finally:
            client.close()
    
    def format_as_table(self, data):
        if not data or not isinstance(data, list):
            return "No data found."
        headers = [f"Column {i+1}" for i in range(len(data[0]))]
        header_line = " | ".join(f"{header:^15}" for header in headers)
        separator = "-+-".join("-" * 15 for _ in headers)

        # Format rows
        rows = []
        for row in data:
            row_line = " | ".join(f"{str(value):^15}" for value in row)
            rows.append(row_line)

        table = f"{header_line}\n{separator}\n" + "\n".join(rows)
        return table
    
    def run(self):
        query1 = "SELECT * FROM students JOIN courses"
        result = self.send_request(query1)         
        data_print = []

        data_print.append(result["header"])
        for data in result["data"]:
            data_print.append(data)
        formatted = self.format_as_table(data_print)
        print(formatted)                            
    
if __name__ == "__main__":
    Client().run()
