"""Circuit breakers for clients.

Notes:
right now each remote service will generally have its own breaker
failures in any of its endpoints will be treated equally
should we have some more fine grained control? separate counters per
endpoint (if desired) plus tracking of overall error rate?
"""
