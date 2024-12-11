class DataRetrieval:
    # index: "" (linear scan)
    # index: "hash" (hash index)
    def __init__(self, table, columns, conditions, index: str = ""):
        self.table = table
        self.columns = columns
        self.conditions = conditions
        self.index = index