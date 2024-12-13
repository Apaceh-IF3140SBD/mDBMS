from queryProcessor.classes.QueryTree import QueryTree
from queryProcessor.classes.Rows import Rows
from queryProcessor.functions.Helper import remove_duplicates, split_dot_contained_data
from storageManager.core.TableSchema import TableSchema
from storageManager.core.StorageEngine import StorageEngine
from storageManager.functions.DataWrite import DataWrite
from storageManager.functions.DataRetrieval import DataRetrieval
from storageManager.functions.Condition import Condition
from storageManager.functions.DataDeletion import DataDeletion

class TreeHandler:
    def __init__(self, storage_engine: StorageEngine):
        self.storage_engine = storage_engine

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
        print("sapi: ",query_tree.childs, query_tree.val, query_tree.type)
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
            new_data["data"] = new_data["data"][:query_tree.val["LIMIT"]]

        # column filtering
        if "attributes" in query_tree.val:

            attributes_with_table, attributes_without_table = split_dot_contained_data(query_tree.val["attributes"])

            # processing without table
            for attr in attributes_without_table:
                if attr == "*":
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
        
        return new_data

    def _handle_table(self, query_tree: QueryTree, transaction_id: int):
        table_name = query_tree.val
        print("11")
        # table_alias = query_tree.val.get("table_alias", table_name)

        data_retrieval = DataRetrieval(
            table=query_tree.val,
            columns=['StudentID', 'FullName', 'Nickname','GPA'],
            conditions=[],
        )
        columns = list(self.storage_engine.schemas[table_name].columns.keys())
        header = columns
        nonambiguous = header  = [f"{table_name}.{column}" for column in columns]
        result = self.storage_engine.select(data_retrieval)
        data = result

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
        child = list(query_tree.childs)
        child = self.process_node(child[0], transaction_id)
        child_table_names = list(child.keys())
        child_data = child[child_table_names[0]]
        new_data = {}
        new_data["header"] = child_data["header"]
        new_data["data"] = []
        or_separated_condition = list(query_tree.val["conditions"])
        for row in child_data["data"]:
            conditions_fulfilled = False

            for and_separated_condition in or_separated_condition:
                and_conditions_fulfilled = True
                for condition in and_separated_condition:
                    try:
                        leftSide = row[child_data["header"].index(condition[0])]
                    except (ValueError, IndexError) as e:
                        leftSide = condition[0]
                    try:
                        rightSide = row[child_data["header"].index(condition[2])]
                    except (ValueError, IndexError) as e:
                        rightSide = condition[2]
                    
                    # try convert to int
                    try:
                        int_leftside = int(leftSide)
                        int_rightSide = int(rightSide)
                        leftSide = int_leftside
                        rightSide = int_rightSide
                    except ValueError as e:
                        pass
                        
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
        
        return new_data
        

    """
    type: join
        val:
            "natural": true or false
            "conditions": ["students.fullname", "=", "13"],[]
    """

    def _handle_join(self, query_tree: QueryTree, transaction_id: int):
        childs = list(query_tree.childs)
        table1 = self.process_node(childs[0], transaction_id)
        table2 = self.process_node(childs[1], transaction_id)
        conditions = query_tree.val["conditions"]

        table1_name = list(table1.keys())[0]
        table2_name = list(table2.keys())[0]

        # Extract headers
        headers1 = table1[table1_name]["header"]
        headers2 = table2[table2_name]["header"]

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

        
        new_nonambiguous = [
            x for x in (table1[table1_name]["nonambiguous"] + table2[table2_name]["nonambiguous"])
            if (x in table1[table1_name]["nonambiguous"]) ^ (x in table2[table2_name]["nonambiguous"])
        ]

        for condition in conditions:
            if condition[0].split('.')[1] == condition[1].split('.')[1]:
                new_nonambiguous.append(condition[0].split('.')[1])
        
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
            left_attr, right_attr = condition


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
        data_after[table_name]["data"] = []

        # minta ijin ubah ke cc

        # sesuailam format dan minta ubah ke failure

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
