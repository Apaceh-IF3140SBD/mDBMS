class QueryCost:
    def __init__(self, schemas, storage):
        self.schemas = schemas
        self.storage = storage
        self.processed_tables = set()

    def get_cost(self, parsed_query):
        self.processed_tables.clear() 

        def calculate_node_cost(node, stats):
            if node.type == "selection":
                cost = 0
                if(node.val.get('conditions')):
                    conditions = node.val.get('conditions')[0]
                    for cond in conditions:
                        column_name = cond[0]
                        for table in self.schemas.keys():
                            if table in column_name:
                                cost += stats[table].n_r + stats[table].l_r
                return cost
                

            elif node.type == "projection":
                cost = 0
                tuple_size = 0
                cols = node.val['attributes']
                
                if len(node.childs)>0:
                    if node.childs[0].type == "table":
                        table = node.childs[0].val
                        if table in self.processed_tables:
                            return 0

                        cols = cols[table]
                        for col in cols:
                            tuple_size += stats[table].l_r
                        cost = tuple_size * stats[table].n_r

                        self.processed_tables.add(table)
                    else:
                        for col in cols:
                            if col not in self.processed_tables:
                                tuple_size += stats[col].l_r
                                cost += tuple_size * stats[col].n_r
                else:
                    cost = 0

                return cost

            elif node.type == "join":
                left_cost = traverse_and_calculate(node.childs[0], stats)
                right_cost = traverse_and_calculate(node.childs[1], stats)

                left_stats = stats[node.childs[0].val] if node.childs[0].type == "table" else None
                right_stats = stats[node.childs[1].val] if node.childs[1].type == "table" else None

                join_cost = (left_stats.n_r * right_stats.b_r + left_stats.b_r) if left_stats and right_stats else 0
                return left_cost + right_cost + join_cost

            else:
                return 0

        def traverse_and_calculate(node, stats):
            total_cost = 0
            for child in node.childs:
                total_cost += traverse_and_calculate(child, stats)
            
            node_cost = calculate_node_cost(node, stats)
            total_cost += node_cost
            return total_cost
        

        stats = {table: self.storage.get_stats(table) for table in self.schemas}
        total_cost = traverse_and_calculate(parsed_query, stats)
        return total_cost
