class IngestionError(Exception):
    pass

class DataSourceError(IngestionError):
    pass

class InvalidInputError(IngestionError, ValueError):
    pass