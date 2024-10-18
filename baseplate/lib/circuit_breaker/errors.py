class BreakerTrippedError(Exception):
    def __init__(self) -> None:
        default_message = "Breaker tripped!"
        super().__init__(default_message)
