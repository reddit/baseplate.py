class BreakerTrippedError(Exception):
    def __init__(self):
        default_message = "Breaker tripped!"
        super(BreakerTrippedError, self).__init__(default_message)
