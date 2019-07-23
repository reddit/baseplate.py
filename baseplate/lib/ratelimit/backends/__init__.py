import time


class RateLimitBackend:
    """An interface for rate limit backends to implement."""

    def consume(self, key: str, amount: int, allowance: int, interval: int) -> bool:
        """Consume the given `amount` from the allowance for the given `key`.

        This will return true if the `key` remains below the `allowance`
        after consuming the given `amount`.

        :param key: The name of the rate limit bucket to consume from.
        :param amount: The amount to consume from the rate limit bucket.
        :param allowance: The maximum allowance for the rate limit bucket.
        :param interval: The interval to reset the allowance.

        """
        raise NotImplementedError


def _get_current_bucket(bucket_size: int) -> str:
    current_timestamp_seconds = int(time.time())
    return str(current_timestamp_seconds // bucket_size)
