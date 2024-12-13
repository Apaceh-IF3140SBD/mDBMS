from datetime import datetime
from queryProcessor.classes.QueryTree import QueryTree
from queryProcessor.classes.Rows import Rows
from concurrencyControl.Utils import Action
from queryProcessor.functions.Helper import remove_duplicates, split_dot_contained_data
from concurrencyControl.CCWrapper import ConcurrencyControlWrapper
from failureRecovery.functions.DataPass import DataPass
from failureRecovery.core.FailureRecovery import FailureRecovery
from failureRecovery.functions.RecoverCriteria import RecoverCriteria
from failureRecovery.functions.ExecutionResult import ExecutionResult
from storageManager.core.TableSchema import TableSchema
from storageManager.core.StorageEngine import StorageEngine
from storageManager.functions.DataWrite import DataWrite
from storageManager.functions.DataRetrieval import DataRetrieval
from storageManager.functions.Condition import Condition
from storageManager.functions.DataDeletion import DataDeletion

class TreeHandler:
    def __init__(self, storage_engine: StorageEngine, concurrency_control:ConcurrencyControlWrapper, failure_recovery: FailureRecovery):
        self.storage_engine = storage_engine
        self.concurrency_control = concurrency_control
        self.failure_recovery = failure_recovery

        """
        experimental
        usable under this specific formatting
        query_tree:{
            child: {
                "student": {
                    "header": ["student.StudentID", "student.FullName", "student.GPA"],
                    "data": [
                        [1, 'Alice', 3.9],
                        [2, 'Bob', 3.2],
                        [3, 'Charlie', 3.6],
                        [4, 'Diana', 2.8]
                    ]
                },
                "classInfo": {
                    "header": ["classInfo.StudentID", "classInfo.Class"],
                    "data": [
                        [1, 'A'],
                        [2, 'B'],
                        [3, 'B'],
                        [4, 'A']
                    ]
                },
                "studentjoinclassinfo": {
                    "header": ["student.StudentID", "student.FullName", "student.GPA", "classInfo.StudentID", "classInfo.Class"],
                    "data": [
                        [1, 'Alice', 3.9],
                        [2, 'Bob', 3.2],
                        [3, 'Charlie', 3.6],
                        [4, 'Diana', 2.8]
                    ]
                    "nonambiguous": ["studentID"]
                },
            }
            join_conditions: {
                [student.StudentID, classInfo.StudentID]
            }
        }
        """
    def process_node(self, query_tree: QueryTree, transaction_id: int):
        handlers = {
            "PROJECTION": self._handle_projection,
            "TABLE": self._handle_table,
            "SELECTION": self._handle_selection,
            "JOIN": self._handle_join,
            "UPDATE": self._handle_update,
            "DELETE": self._handle_delete,
            "CREATE": self._handle_create,
            "INSERT": self._handle_insert,
        }
        handler = handlers.get(query_tree.type.upper())
        if handler:
            return handler(query_tree, transaction_id)
        else:
            raise ValueError(f"Unsupported command: {query_tree.type}")

    """
        projection:
            val: "ORDER BY", "LIMIT", "attributes"
            child: 1 node lain 
    """
    
    def _handle_projection(self, query_tree: QueryTree, transaction_id: int):
        print("1")
        child = self.process_node(query_tree.childs[0], transaction_id)
        print("2")
        child_table_names = list(child.keys())
        print("3")
        child_data = child[child_table_names[0]]
        print("4")
        new_data = child_data
        print("project: ",query_tree.childs, query_tree.val, query_tree.type)

        # print(new_data)

        # order
        if "ORDER BY" in query_tree.val:
            sorted_data = new_data
            order_by_tuples = list(query_tree.val["ORDER BY"])

            # default ascending
            if len(order_by_tuples) == 1:
                order_by_tuples.append("asc")

            sorted_data["data"].sort(
                key=lambda row: row[sorted_data["header"].index(order_by_tuples[0])],
                reverse=(order_by_tuples[1].lower() ==  "desc")
            )
            new_data = sorted_data
        

        # LIMIT
        if "LIMIT" in query_tree.val:
            print("limit:", query_tree.val["LIMIT"])
            new_data["data"] = new_data["data"][:int(query_tree.val["LIMIT"][0])]

        # column filtering
        if "attributes" in query_tree.val:

            attributes_with_table, attributes_without_table = split_dot_contained_data(query_tree.val["attributes"])

            # processing without table
            for attr in attributes_without_table:
                if attr == "*":
                    print("child_data:", child_data)
                    new_data["header"] = child_data["header"]
                    return new_data
                if attr not in child_data["nonambiguous"]:
                    raise Exception("Ambiguous column: " + attr)
                for col_name in child_data["header"]:
                    after_dot = col_name.split('.')[1]
                    if after_dot == attr:
                        selected_attr_indices.append(child_data["header"].index(col_name))
                        new_data["header"].append(after_dot)
                        break

            # processing with table
            selected_attr_indices = [
                child_data["header"].index(attr) for attr in 
                attributes_with_table if attr in child_data["header"]
            ]
            new_data["header"] = [child_data["header"][i] for i in selected_attr_indices]

            # changing new data by filtering out data without header
            new_data["data"] = [[row[i] for i in selected_attr_indices]for row in child_data["data"]]
        
        new_data["data"] = remove_duplicates(new_data["data"])
        
        return {
            str(child_table_names[0]):{
                "header": new_data["header"],
                "data": new_data["data"],
                "nonambiguous": child_data["nonambiguous"],
            }
        }

    def _handle_table(self, query_tree: QueryTree, transaction_id: int):
        print("table: ",query_tree.childs, query_tree.val, query_tree.type)
        table_name = query_tree.val
        print("11")
        # table_alias = query_tree.val.get("table_alias", table_name)

        columns = list(self.storage_engine.schemas[table_name].columns.keys())
        header = [f"{table_name}.{column}" for column in columns]
        metadata = [self.storage_engine.schemas[table_name].columns[column] for column in columns]
        data_retrieval = DataRetrieval(
            table=query_tree.val,
            columns= columns,
            conditions=[],
        )
        nonambiguous = columns
        result = self.storage_engine.select(data_retrieval)
        data = result

        # cc

        self.concurrency_control.log_object(Rows(data))
        response = self.concurrency_control.validate_object(Rows(data), transaction_id, Action.READ)
        if not response.allowed:
            abortion = RecoverCriteria(None, transaction_id)
            self.failure_recovery.recover(abortion)
        
        converted_data = []
        for row in data:
            converted_row = []
            for i, value in enumerate(row):
                if metadata[i] == "int":
                    converted_row.append(int(value))
                elif metadata[i] == "float":
                    converted_row.append(float(value))
                else:
                    converted_row.append(value)  # Keep as-is for other types
            converted_data.append(converted_row)
        
        print(converted_data)

        output = {
            table_name: {
                "header": header,
                "data": data,
                "nonambiguous": nonambiguous,
            }
        }
        print("14")
        return output

    def _handle_table_as_rows(self, query_tree: QueryTree, transaction_id: int) -> Rows:
        table_name = query_tree.val

        data_retrieval = DataRetrieval(
            table=table_name,
            columns=['StudentID', 'FullName', 'Nickname','GPA'], 
            conditions=[],
        )

        result = self.storage_engine.select(data_retrieval)

        data = result["data"]

        return Rows(data)
    


    """
    student.studentID sama sapi.studentID
    """

    """
    studentID atau student.studentID
    """


    """
    "selection":
        val:
            "conditions":[
                []
                []
            ]
    """
    
    def _handle_selection(self, query_tree: QueryTree, transaction_id: int):
        print("selection: ",query_tree.childs, query_tree.val, query_tree.type)
        child = list(query_tree.childs)
        child = self.process_node(child[0], transaction_id)
        child_table_names = list(child.keys())
        print("ini " , child_table_names)
        child_data = child[child_table_names[0]]
        new_data = {}
        new_data["header"] = child_data["header"]
        new_data["data"] = []
        or_separated_condition = list(query_tree.val["conditions"])
        print("sebelum seleksi: ", child_data["data"])
        for row in child_data["data"]:
            print(row)
            conditions_fulfilled = False

            for and_separated_condition in or_separated_condition:
                and_conditions_fulfilled = True
                for condition in and_separated_condition:
                    print("CONDITIONASDASDASD: ",condition)
                    try: 
                        leftSide = row[child_data["header"].index(condition[0])]
                    except (ValueError, IndexError) as e:
                        leftSide = condition[0]
                    try:
                        rightSide = row[child_data["header"].index(condition[2])]
                    except (ValueError, IndexError) as e:
                        rightSide = condition[2]
                    
                    # try convert to int
                    if(type(rightSide) == float or type(leftSide) == float):
                        rightSide = float(rightSide)
                        leftSide = float(leftSide)

                    print("kiri, kanan", leftSide, rightSide)
                        
                    # case: <, =, >, <=, >=
                    # handle NULL?
                    if condition[1] == '=':
                        and_conditions_fulfilled = and_conditions_fulfilled and leftSide == rightSide
                    elif condition[1] == '>':
                        and_conditions_fulfilled = and_conditions_fulfilled and  leftSide > rightSide
                    elif condition[1] == '<':
                        and_conditions_fulfilled = and_conditions_fulfilled and  leftSide < rightSide
                    elif condition[1] == '>=':
                        and_conditions_fulfilled = and_conditions_fulfilled and  leftSide >= rightSide
                    elif condition[1] == '<=':
                        and_conditions_fulfilled = and_conditions_fulfilled and  leftSide <= rightSide
                    elif condition[1] == '<>':
                        and_conditions_fulfilled = and_conditions_fulfilled and  leftSide != rightSide

                conditions_fulfilled = conditions_fulfilled or  and_conditions_fulfilled

            if conditions_fulfilled:
                new_data["data"].append(row)
        print("hasil seleksi: ", new_data)
        return {str(child_table_names):{
                    "header": new_data["header"],
                    "data": new_data["data"],
                    "nonambiguous": child_data["nonambiguous"],
            }       
        }
        

    """
    type: join
        val:
            "natural": true or false
            "conditions": ["students.fullname", "=", "13"],[]
    """

    def _handle_join(self, query_tree: QueryTree, transaction_id: int):
        childs = list(query_tree.childs)
        print("SIGMA BOY",childs)
        table1 = self.process_node(childs[0], transaction_id)
        table2 = self.process_node(childs[1], transaction_id)

        table1_name = list(table1.keys())[0]
        table2_name = list(table2.keys())[0]

        # Extract headers
        headers1 = table1[table1_name]["header"]
        headers2 = table2[table2_name]["header"]



        sorted_join_conditions = []
        if query_tree.val["natural"]:
            # natural
            for i, header1 in enumerate(headers1):
                for j, header2 in enumerate(headers2):
                    if header1.split('.')[1] == header2.split('.')[1]:
                        sorted_join_conditions.append([i,j])
        else:
            # not natural
            if "conditions" in query_tree.val:
                conditions = query_tree.val["conditions"][0]
                # Sort join conditions to determine column indices
                sorted_join_conditions = self._sort_join_conditions(headers1, headers2, conditions)

        # Create joined headers
        joined_headers = headers1 + headers2

        # Perform join
        joined_data = []
        for leftside_row in table1[table1_name]["data"]:
            for rightside_row in table2[table2_name]["data"]:
                match = True
                for left_index, right_index in sorted_join_conditions:
                    if leftside_row[left_index] != rightside_row[right_index]:
                        match = False
                        break
                if match:
                    joined_data.append(leftside_row + rightside_row)

        if query_tree.val["natural"]:        
            new_nonambiguous = [
                x for x in (table1[table1_name]["nonambiguous"] + table2[table2_name]["nonambiguous"])
                if (x in table1[table1_name]["nonambiguous"]) ^ (x in table2[table2_name]["nonambiguous"])
            ]

            for condition in conditions:
                if condition[0].split('.')[1] == condition[2].split('.')[1]:
                    new_nonambiguous.append(condition[0].split('.')[1])
                    
        else:
            new_nonambiguous = [
                x for x in (table1[table1_name]["nonambiguous"] + table2[table2_name]["nonambiguous"])
            ]
            new_nonambiguous = remove_duplicates(new_nonambiguous)
        
        # Return the joined result
        return {
                str(table1_name +" join " +table2_name):{
                "header": joined_headers,
                "data": joined_data,
                "nonambiguous": new_nonambiguous,
            }
        }

    # return array of 2 elements that represent the column for corresponding attribute
    def _sort_join_conditions(self, table1_header, table2_header, conditions):
        sorted_conditions = []
        for condition in conditions:
            left_attr, _, right_attr = condition
            print("left", left_attr, "right: ", right_attr)
            print(table1_header)
            print(table2_header)


            # Determine if attributes belong to table1 or table2
            if left_attr == right_attr:
                left_index = table1_header.index(left_attr)
                right_index = table2_header.index(left_attr)
            elif left_attr in table1_header:
                left_index = table1_header.index(left_attr)
                right_index = table2_header.index(right_attr)
            else:
                left_index = table1_header.index(right_attr)
                right_index = table2_header.index(left_attr)
            # Add sorted indices to the condition list
            sorted_conditions.append((left_index, right_index))

        return sorted_conditions
    
    def _handle_update(self, query_tree:QueryTree, transaction_id: int):
        table_name = query_tree.val["table_name"]

        columns = list(self.storage_engine.schemas[table_name].columns.keys())

        data_retrieval = DataRetrieval(
            table=query_tree.val["table"][0],
            columns=columns,
            conditions=[query_tree.val["conditions"]],
        )
        result = self.storage_engine.select(data_retrieval)
        header  = [column for column in columns]
        
        for row in result:
            for set in query_tree.val["set"]:
                


        table_name = query_tree.val["table_name"]

        data_retrieval = DataRetrieval(
            table=query_tree.val["table_name"],
            columns=[],
            conditions=[],
        )
        result = self.storage_engine.select(data_retrieval)
        header  = [f"{column}" for column in result["columns"]]

        data = result["data"]

        data_before = {
            table_name: {
                "header": header,
                "data": data,
            }
        }
        data_after = data_before

        # minta log ke cc

        # make changes
        changes = list(query_tree.val["set"])

        for change in changes:
            conrrespoding_index = data_after[table_name]["header"].index(change[0])
            for row in data_after[table_name]["data"]:
                row[conrrespoding_index] = change[1]

        # minta ijin ubah ke cc

        # sesuailam format dan minta ubah ke failure



    def _handle_delete(self, query_tree:QueryTree, transaction_id: int):
        table_name = query_tree.val["table_name"]

        columns = list(self.storage_engine.schemas[table_name].columns.keys())

        data_retrieval = DataRetrieval(
            table=query_tree.val["table"][0],
            columns=columns,
            conditions=[query_tree.val["conditions"]],
        )
        result = self.storage_engine.select(data_retrieval)
        header  = [column for column in columns]

        # cc
        self.concurrency_control.log_object(Rows(result))
        response = self.concurrency_control.validate_object(Rows(result), transaction_id, Action.WRITE)
        if not response.allowed:
            abortion = RecoverCriteria(None, transaction_id)
            self.failure_recovery.recover(abortion)

        data_pass_before = DataPass(
            "apache", table_name, columns, result, todo= None
        )
        data_pass_after = None
        execution_result = ExecutionResult(transaction_id= transaction_id,timestamp= datetime.now(), message= "DELETE", data_before= data_pass_before, data_after= data_pass_after, query= "DELETE")
        self.failure_recovery.write_log(execution_result)

    def _handle_create(self, query_tree:QueryTree, transaction_id: int):
        table = query_tree.val["table"]
        attributes = query_tree.val["attributes"]

        # Build schema from attributes
        schema = {
            attr["name"]: attr["type"] for attr in attributes
        }
        table_schema = TableSchema(table_name=table, columns=schema)

        # Use the `create_table` method of StorageEngine
        self.storage_engine.create_table(table_schema)
        return f"Table '{table}' created successfully."

    def _handle_insert(self, query_tree:QueryTree, transaction_id: int):
        table_name = query_tree.val["table_name"]

        data_retrieval = DataRetrieval(
            table=query_tree.val["table_name"],
            columns=[],
            conditions=[],
        )
        result = self.storage_engine.select(data_retrieval)
        header  = [f"{column}" for column in result["columns"]]

        data = result["data"]

        data_before = {
            table_name: {
                "header": header,
                "data": data,
            }
        }
        data_after = data_before

        # minta log ke cc

        # make changes
        for new_data in query_tree.val["new_data"]:
            data_after[table_name]["data"].append(new_data)

        # minta ijin ubah ke cc

        # sesuailam format dan minta ubah ke failure
