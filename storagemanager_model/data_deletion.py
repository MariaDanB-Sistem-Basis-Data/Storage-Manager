class DataDeletion:
    def __init__(self, table, conditions=None):
        self.table = table
        self.conditions = conditions or []