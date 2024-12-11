# import sys
# import os

# # sys.path.append(os.path.dirname(os.path.abspath(__file__)))
# # sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "classes"))
# sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "processor"))
# # sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "processor", "classes"))
# sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "storage", "classes"))
# # sys.path.append(os.path.join(os.
# # path.dirname(os.path.abspath(__file__)), "storage", "utils"))


# from QueryProcessor import QueryProcessor
# from QueryOptimizer import QueryOptimizer
# from StorageManager import StorageManager
# from ConcurrencyControlManager import ConcurrencyControlManager
# from FailureRecoveryManager import FailureRecoveryManager



# def get_query():
#     processor = QueryProcessor(
#         QueryOptimizer(),
#         StorageManager(),
#         ConcurrencyControlManager(),
#         FailureRecoveryManager()
#     )

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
#                 try:# print(f"Executing query: {query_buffer.strip()}")
#                     result = processor.execute_query(query_buffer.strip())
#                     print(f"Message: {result.message}")
#                     if hasattr(result.data, "data"):
#                         print(f"Data: {result.data.data}")
#                 except Exception as e:
#                     print(f"Error: {e}")
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



from queryProcessor.classes.QueryProcessor import QueryProcessor
from server.Server import ServerRunner

# This should output something like <class '...ProjectionSimplification'>



if __name__ == "__main__":
    # from queryOptimizer.classes.optimizerRule import ProjectionSimplification
    # print(ProjectionSimplification())    
     
    ServerRunner().run(QueryProcessor)





