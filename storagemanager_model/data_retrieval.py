class DataRetrieval:
    def __init__(self, table, column, conditions=None):
        self.table = table
        self.column = column
        self.conditions = conditions or []

        