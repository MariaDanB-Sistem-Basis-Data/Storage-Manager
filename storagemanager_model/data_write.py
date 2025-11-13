class DataWrite:
    def __init__(self, table, column, conditions=None, new_value=None):
        self.table = table
        self.column = column
        self.conditions = conditions or []
        self.new_value = new_value