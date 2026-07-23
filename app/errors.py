"""Failure taxonomy.

The distinction drives the whole retry policy, so it is deliberately explicit
rather than inferred from exception types leaking out of libraries:

* `TransientError` — might succeed later (timeout, downstream 503, deadlock).
  Retried with exponential backoff, then dead-lettered.
* `PermanentError` — will never succeed (bad payload, unknown SKU, business
  rule violation). Dead-lettered immediately; retrying only wastes capacity.
"""


class ProcessingError(Exception):
    pass


class TransientError(ProcessingError):
    """Retryable."""


class PermanentError(ProcessingError):
    """Not retryable — straight to the DLQ."""
