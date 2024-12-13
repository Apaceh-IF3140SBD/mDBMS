import socket
import json
import traceback

class Client:
    
    HOST = "localhost"
    PORT = 65432
    
    def __init__(self):
        self.begin_transaction = False
        self.running = True
        try:
            self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.client.connect((Client.HOST, Client.PORT))
        except Exception as e:
            print("Error: ", e)
            traceback.print_exc()
            exit(1)
    
    def send_request(self, query: str = ""):
        try:
            self.client.send(query.encode('utf-8'))
            response = self.client.recv(1024*1024).decode('utf-8')
            response_data = json.loads(response)
            return response_data
        except Exception as e:
            print(f"Error: {e}")
    
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
                    # if isinstance(result, list):  # SELECT queries
                    #     print(format_as_table(result))
                    # else:  # Other commands (INSERT, UPDATE)
                    #     print(f"Message: {result}")
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
        # while True:
        #     query = input("Enter a query: ").strip()
        #     if query.lower() == 'exit':
        #         break
        #     self.send_request(query)
    
if __name__ == "__main__":
    Client().run()

########### formatting cli #############

# from storage.core.StorageEngine import StorageEngine
# from storage.core.TableSchema import TableSchema
# from storage.functions.DataWrite import DataWrite
# from classes.QueryProcessor import QueryProcessor


def format_as_table(data):
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



# def initialize_storage(storage_engine):
#     student_schema = TableSchema(
#         table_name="students",
#         columns={
#             "StudentID": "int",
#             "FullName": "varchar(50)",
#             "GPA": "float"
#         }
#     )
#     storage_engine.create_table(student_schema)

#     # Insert test data
#     test_data = [
#         (1, "Alice", 3.9),
#         (2, "Bob", 3.2),
#         (3, "Charlie", 3.6),
#         (4, "Diana", 2.8)
#     ]
#     for student in test_data:
#         storage_engine.write_block(
#             data_write=DataWrite(
#                 table="students",
#                 columns=["StudentID", "FullName", "GPA"],
#                 new_value=student,
#                 conditions=[]
#             )
#         )
#     print("Test data inserted into 'students' table.")


# def get_query():
#     dbms_on = True
#     query_buffer = ""
#     prompt = "Apaceh=# "
#     while dbms_on:
#         try:
#             input_query = input(prompt).strip()
#             if input_query == r"\q":
#                 dbms_on = False
#                 print("Exiting...")
#                 break

#             query_buffer += input_query + ""

#             if query_buffer.endswith(";"):
#                 print(f"Executing query: {query_buffer.strip()}")
#                 result = query_processor.execute_query(query_buffer.strip())
#                 if isinstance(result, list):  # SELECT queries
#                     print(format_as_table(result))
#                 else:  # Other commands (INSERT, UPDATE)
#                     print(f"Message: {result}")
#                 query_buffer = ""
#                 prompt = "Apaceh=# "  
#             else:
#                 prompt = "Apaceh-# "
#         except Exception as e:
#             print(f"Error: {e}")
#         except KeyboardInterrupt:
#             print("\nKeyboard Interrupt detected. Exiting...")
#             dbms_on = False
#             break


# if __name__ == "__main__":
#     get_query()
