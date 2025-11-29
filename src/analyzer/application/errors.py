class PipelineError(Exception):
    pass

class DataPreparationError(PipelineError):
    pass

class MissingDataError(DataPreparationError):
    pass

class DomainContractError(ValueError, PipelineError):
    pass

class InvalidInputError(PipelineError, ValueError):
    pass