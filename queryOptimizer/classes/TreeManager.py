from queryOptimizer.classes.Query import QueryTree
from storageManager.core.StorageEngine import StorageEngine
import copy

# from core.StorageEngine import StorageEngine

command_list = [
    "SELECT", "UPDATE", "SET", "FROM", "WHERE", "ORDER",
    "LIMIT", "BEGIN TRANSACTION", "COMMIT", "USING", "DELETE", "CREATE", "INSERT"
]

class TreeManager:
    def __init__(self,storageEngine, schemas):
        self.storage = storageEngine
        self.schemas = schemas
        self.astable = {} # nyimpen as table

    def convert_tree(self, query: dict):
        tree_node = {}
        stmt_type = ""
        # print(query)
        for command, detail in query.items():
            node = QueryTree()
            node.val = {}
            table_col = {}
            if command == "SELECT":
                node.type = "projection"
                # node.val = command
                for column in detail:
                    table_name = self.check_attribute_table(column)
                    if "." in column:
                        value = column.split('.')[0]
                        value2 = column.split('.')[1]
                        table_name = value
                        column = value2

                    if table_col and table_col.get(table_name)!=None:
                        column_list = table_col.get(table_name)
                        column_list.append(column)
                        table_col.update({table_name: column_list})
                    else:
                        table_col[table_name] = [column]

                    if table_name!=None:
                        if node.val and node.val.get("attributes"):
                            node.val.update({"attributes": table_col})
                        else:
                            node.val["attributes"] = [table_col] 
                    else:
                        # node.childs.append(attribute)
                        if node.val:
                            column_list = node.val.get("attributes")
                            column_list.append(column)
                            node.val.update({"attributes": column_list})
                        else:
                            node.val["attributes"] = [column]    
                stmt_type = command
            elif command == "UPDATE":
                node.type = "update"
                for column in detail:
                    node.val["table"] = [column]   
                stmt_type = command
            elif command == "DELETE":
                node.type = "delete"
                stmt_type = command
            elif command == "CREATE":
                node.type = "create"
                set_column = []
                for i, column in enumerate(detail):
                    try:
                        numeric_value = float(column)
                        if numeric_value.is_integer():
                            numeric_value = int(numeric_value)
                        set_column.append(numeric_value)
                    except ValueError:
                        if len(column) > 1 and ((column[0] == "'" and column[-1] == "'") or (column[0] == '"' and column[-1] == '"')):
                            column = column[1:-1]
                        set_column.append(column)
                node.val["value"] = set_column  
                stmt_type = command
            elif command == "INSERT":
                node.type = "insert"
                values_index = detail.index('VALUES')
                valuecol = detail[:values_index]  
                valuevals = detail[values_index + 1:]   
                node.val["attributes"] = valuecol[1:] 
                val_column = []
                for i, column in enumerate(valuevals):
                    try:
                        numeric_value = float(column)
                        if numeric_value.is_integer():
                            numeric_value = int(numeric_value)
                        val_column.append(numeric_value)
                    except ValueError:
                        if len(column) > 1 and ((column[0] == "'" and column[-1] == "'") or (column[0] == '"' and column[-1] == '"')):
                            column = column[1:-1]
                        val_column.append(column)
                node.val["values"] = val_column  
                stmt_type = command
            elif command == "SET":
                set_column = []
                for i, column in enumerate(detail):
                    try:
                        numeric_value = float(column)
                        if numeric_value.is_integer():
                            numeric_value = int(numeric_value)
                        set_column.append(numeric_value)
                    except ValueError:
                        if len(column) > 1 and ((column[0] == "'" and column[-1] == "'") or (column[0] == '"' and column[-1] == '"')):
                            column = column[1:-1]
                        set_column.append(column)
                tree_node["UPDATE"].val["set"] = set_column
            elif command == "FROM" or command == "JOIN" or command == "NATURAL":
                node.type = "join" if command == "JOIN" or command == "NATURAL" else "from"
                node.val = command
                if(tree_node.get("DELETE", None)):
                    parent_node = tree_node.get("DELETE", None)
                    stmt_type = "DELETE"
                elif(tree_node.get("WHERE", None)):
                    parent_node = tree_node.get("WHERE", None)
                else:
                    parent_node = tree_node.get("SELECT", None)
                node.parent = parent_node
                i = 0
                node.val = {}
                
                join_node_temp = None 

                while i < len(detail):
                    if isinstance(detail[i], dict):  
                        sub_tree = self.convert_tree(detail[i])
                        subquery_node = QueryTree("subquery", "SUBQUERY", sub_tree, node.parent)
                        subquery_node.childs.append(sub_tree)
                        node.childs.append(subquery_node)
                        i += 1 
                    elif detail[i] == "NATURAL" and i + 1 < len(detail) and detail[i + 1] == "JOIN":
                        # Handling a NATURAL JOIN
                        node.val["natural"] = True
                        join_node = QueryTree("join", node.val, None, node)
                        i += 2  # Skip "NATURAL" and "JOIN"
                        
                        if join_node_temp:
                            join_node.childs.append(join_node_temp)
                            join_node_temp = None 
                        
                
                        if i < len(detail):
                            table_node = QueryTree("table", detail[i], None, join_node)
                            join_node.childs.append(table_node)
                            node.childs.clear()
                            node.childs.append(join_node) 
                            i += 1

                        if i < len(detail) and detail[i] == "NATURAL" and i + 1 < len(detail) and detail[i + 1] == "JOIN":
                            join_node_temp = join_node

                    elif detail[i] == "JOIN":
                        node.val["natural"] = False
                        join_node = QueryTree("join", node.val, None, node)
                        i += 1

                        if join_node_temp:
                            join_node.childs.append(join_node_temp)
                            join_node_temp = None 
                        
                
                        if i < len(detail):
                            table_node = QueryTree("table", detail[i], None, join_node)
                            join_node.childs.append(table_node)
                            node.childs.clear()
                            node.childs.append(join_node) 
                            i += 1
         
                        if i<len(detail) and detail[i] == "ON":
                            listall = []
                            listand = []
                            list = []
                            i+=1
                            
                            while i<len(detail) and detail[i] != "JOIN":
                                if detail[i] in ['OR', 'AND']:
                                    if detail[i] == 'OR':
                                        if (list[-1][0]=="'" and list[-1][-1]=="'") or (list[-1][0]=="\"" and list[-1][-1]=="\""):
                                            list[-1] = list[-1][1:-1]
                                            listand.append(list)
                                        else:
                                            list[-1] = int(list[-1])
                                            listand.append(list)
                                        listall.append(listand)
                                        listand = []
                                        list = []
                                    if detail[i] == 'AND':
                                        if (list[-1][0]=="'" and list[-1][-1]=="'") or (list[-1][0]=="\"" and list[-1][-1]=="\""):
                                            list[-1] = list[-1][1:-1]
                                            listand.append(list)
                                        else:
                                            list[-1] = int(list[-1])
                                            listand.append(list)
                                    list = []
                                else:
                                    table_name = self.check_attribute_table(detail[i])
                                    if(table_name is not None):
                                        column = table_name + "." + detail[i]
                                    list.append(detail[i])
                                i+=1
                            if (list[-1][0]=="'" and list[-1][-1]=="'") or (list[-1][0]=="\"" and list[-1][-1]=="\""):
                                list[-1] = list[-1][1:-1]
                                listand.append(list)
                            else:
                                try:
                                    list[-1] = int(list[-1]) 
                                except ValueError:
                                    pass  
                                listand.append(list)
                            listall.append(listand)
                            node.val["conditions"] = listall
                            join_node.val = node.val

                        if i < len(detail) and detail[i] == "JOIN" :
                            join_node_temp = copy.deepcopy(join_node)
                            del node.val["condition"]

                    else:
                        table_node = QueryTree("table", detail[i], None, node)
                        if i + 1 < len(detail) and (detail[i + 1] == "JOIN" or detail[i + 1] == "NATURAL"):
                            join_node_temp = table_node
                        else:
                            node.childs.append(table_node)
                        i += 1
                if parent_node and parent_node.type == "delete":
                    parent_node.val["table"] = node.childs[0].val
                    node.parent = parent_node
                else:
                    for child in node.childs:
                        parent_node.childs.append(child)

            elif command == "WHERE":
                node.type = "selection"
                # node.val = command
                parent = ""
                if(tree_node.get("UPDATE", None)):
                    parent_node = tree_node.get("UPDATE", None)
                    parent= "UPDATE"
                    node.type = "update"
                    stmt_type="UPDATE"
                elif(tree_node.get("DELETE", None)):
                    parent_node = tree_node.get("DELETE", None)
                    parent= "DELETE"
                    node.type = "delete"
                    stmt_type="DELETE"
                else:
                    parent_node = tree_node.get("SELECT", None)
                    parent= "SELECT"
                table = "conditions"
                listall = []
                listand = []
                list = []
                for i, column in enumerate(detail):
                    if isinstance(column, dict): 
                        sub_tree = self.convert_tree(column)
                        attribute = QueryTree("subquery", "SUBQUERY", sub_tree, node)
                        attribute.childs.append(sub_tree)
                        node.childs.append(attribute)
                    elif column in ['OR', 'AND']:
                        if column == 'OR':
                            if (list[-1][0]=="'" and list[-1][-1]=="'") or (list[-1][0]=="\"" and list[-1][-1]=="\""):
                                list[-1] = list[-1][1:-1]
                                listand.append(list)
                            else:
                                list[-1] = int(list[-1])
                                listand.append(list)
                            listall.append(listand)
                            listand = []
                            list = []
                        if column == 'AND':
                            if (list[-1][0]=="'" and list[-1][-1]=="'") or (list[-1][0]=="\"" and list[-1][-1]=="\""):
                                list[-1] = list[-1][1:-1]
                                listand.append(list)
                            else:
                                list[-1] = int(list[-1])
                                listand.append(list)
                            list = []
                    else:
                        table_name = self.check_attribute_table(column)
                        if(table_name is not None):
                            column = table_name + "." + column
                        elif "." in column:
                            value = column.split('.')[0]
                            value2 = column.split('.')[1]
                            if value in self.astable:
                                realtable = self.astable[value]
                                column = realtable + "." + value2
                        list.append(column)
                if (list[-1][0]=="'" and list[-1][-1]=="'") or (list[-1][0]=="\"" and list[-1][-1]=="\""):
                    list[-1] = list[-1][1:-1]
                    listand.append(list)
                else:
                    try:
                        list[-1] = int(list[-1]) 
                    except ValueError:
                        pass  
                    listand.append(list)
                listall.append(listand)
                node.val.update({table: listall}) 
                if parent=="UPDATE":
                    tree_node["UPDATE"].val["conditions"] = node.val["conditions"]
                elif parent=="DELETE":
                    tree_node["DELETE"].val["conditions"] = node.val["conditions"]
                else: 
                    tree_node[parent].childs.append(node)
            elif command == "ORDER BY":
                order_column = []
                for column in detail:
                    order_column.append(column)
                tree_node["SELECT"].val[command] = order_column
            elif command == "LIMIT":
                order_column = []
                for column in detail:
                    order_column.append(int(column))
                tree_node["SELECT"].val[command] = order_column
            tree_node[command] = node
        return tree_node[stmt_type]

        
    def parse_tree_to_dict(self,parsed_query: list) -> dict:
        query_detail = {}
        idx = 0

        while idx < len(parsed_query):
            if parsed_query[idx].upper() in command_list:
                key = parsed_query[idx].upper() 
                value = []
                idx += 1
                while (idx < len(parsed_query)) and (parsed_query[idx].upper() not in command_list):
                    if parsed_query[idx] == "(":
                        # Handle subquery
                        subquery = []
                        balance = 1
                        idx += 1
                        while balance > 0 and idx < len(parsed_query):
                            if parsed_query[idx] == "(":
                                balance += 1
                            elif parsed_query[idx] == ")":
                                balance -= 1
                            if balance > 0: 
                                if len(subquery)==0 and parsed_query[idx].upper() not in command_list:
                                    value.append(parsed_query[idx])
                                else:
                                    subquery.append(parsed_query[idx])
                            idx += 1
                        
                        if len(subquery)!=0 :
                            subquery_dict = self.parse_tree_to_dict(subquery)
                            value.append(subquery_dict)
                        
                    else:
                        value.append(parsed_query[idx])
                        idx += 1

                if idx < len(parsed_query):
                    if parsed_query[idx] == "ORDER" and (idx + 1 < len(parsed_query)) and parsed_query[idx + 1] == "BY": 
                        query_detail[key] = value.copy()
                        value.clear()
                        key = "ORDER BY"
                        idx += 2
                        value.append(parsed_query[idx])
                        value.append(parsed_query[idx+1])
                        idx+=1

                if ';' in value:
                    value.remove(';')
                query_detail[key] = value
            else:
                idx += 1
            
        if "FROM" in list(query_detail.keys()):
            value = query_detail['FROM'].copy()
            del query_detail['FROM']
            items = list(query_detail.items())
            temp = []
            i=0
            while i < len(value):
                if i+2<len(value):
                    if value[i+1] =="AS" or value[i+1] =="as":
                        self.astable[value[i+2]] = value[i]
                        temp.append(value[i])
                        i+=3
                    else:
                        temp.append(value[i])
                        i+=1
                else:
                    temp.append(value[i])
                    i+=1

            items.append(('FROM', temp)) 
            query_detail = dict(items)
        
        if "LIMIT" in list(query_detail.keys()):
            value = query_detail['LIMIT'].copy()
            del query_detail['LIMIT']
            items = list(query_detail.items())
            items.insert(1,('LIMIT', value)) 
            query_detail = dict(items)
        
        if "ORDER BY" in list(query_detail.keys()):
            value = query_detail['ORDER BY'].copy()
            del query_detail['ORDER BY']
            items = list(query_detail.items())
            items.insert(1,('ORDER BY', value)) 
            query_detail = dict(items)

        return query_detail

    def display_tree(self, tree: "QueryTree", depth: int):
        if tree.val:
            print(f"{'--' * depth}{tree.val} {tree.type}")

        if len(tree.childs) != 0:
            for el in tree.childs:
                self.display_tree(el, depth=depth + 1)

    def check_attribute_table(self,attribute):
        tables = list(self.schemas.keys())
        found = None
        for table in tables:
            stats = StorageEngine.get_stats(self.storage, table)  
            column = stats.V_a_r  

            if attribute in column:
                found = table
                break
        return found
