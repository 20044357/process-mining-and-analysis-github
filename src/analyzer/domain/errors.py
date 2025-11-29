class DomainError(Exception):
    pass

class DomainContractError(DomainError):
    pass

class CalculationError(DomainError):
    pass
