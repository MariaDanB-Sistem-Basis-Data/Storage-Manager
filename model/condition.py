class Condition:
    def __init__(self, column, operation, operand):
        valid_ops = {'=', '<>', '>', '>=', '<', '<='}
        if operation not in valid_ops:
            raise ValueError(f"Invalid operation: {operation}")
        self.column = column
        self.operation = operation
        self.operand = operand