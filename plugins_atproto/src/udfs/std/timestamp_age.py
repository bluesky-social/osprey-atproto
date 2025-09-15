import arrow
from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import UDFBase
from osprey.worker.lib.osprey_shared.logging import get_logger

logger = get_logger("timestamp_age")


class TimestampAgeArguments(ArgumentsBase):
    timestamp: str


class TimestampAge(UDFBase[TimestampAgeArguments, float]):
    """
    Takes an input RFC 3339 Nano timestamp and returns the number of seconds that have passed.
    """

    def execute(
        self, execution_context: ExecutionContext, arguments: TimestampAgeArguments
    ) -> float:
        try:
            timestamp = arrow.get(arguments.timestamp)
            seconds_elapsed = (arrow.now() - timestamp).total_seconds()
            return seconds_elapsed
        except Exception:
            # HACK: return a really big number for now if the account age doesn't show up
            return 999_999_999
