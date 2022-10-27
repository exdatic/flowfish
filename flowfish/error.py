class FlowError(Exception):
    def __init__(self, message):
        super().__init__(message)


class NodeNotFoundError(FlowError):
    def __init__(self, message):
        super().__init__(message)


class ScopeNotFoundError(FlowError):
    def __init__(self, message):
        super().__init__(message)
