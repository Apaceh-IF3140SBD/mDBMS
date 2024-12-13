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
            print("Connected")
            request = {
                "query": query,
                "transaction_id": self.transaction_id
            }
            dumps = json.dumps(request)
            client.send(dumps.encode('utf-8'))
            response = client.recv(1024*1024).decode('utf-8')
            data, t_id = json.loads(response)
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
        query_buffer = ""
        prompt = "Apaceh=# "
        while self.running:
            try:
                input_query = input(prompt).strip()
                if input_query == r"\q":
                    self.running = False
                    print("Exiting...")
                    break

                query_buffer += input_query + ""

                if query_buffer.endswith(";"):
                    print(f"Executing query: {query_buffer.strip()}")
                    query = query_buffer.strip().removesuffix(";")
                    if query == "START TRANSACTION":
                        self.begin_transaction = True
                    elif query == 'COMMIT':
                        self.begin_transaction = False
                    result = self.send_request(query)
                    if (query_buffer.strip().lower().startswith("select")):
                        if "header" in result and "data" in result:
                            data_print = []
                            data_print.append(result["header"])
                            for data in result["data"]:
                                data_print.append(data)
                            formatted = self.format_as_table(data_print)
                            print(formatted)                            
                    else:
                        print(result)
                    query_buffer = ""
                    prompt = "Apaceh=# "  
                else:
                    prompt = "Apaceh-# "
            except Exception as e:
                print(f"Error: {e}")
            except KeyboardInterrupt:
                print("\nKeyboard Interrupt detected. Exiting...")
                if (self.begin_transaction):
                    self.send_request("ABORT")
                self.running = False
                break
        self.client.close()
    
if __name__ == "__main__":
    Client().run()
