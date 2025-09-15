from typing import List

from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import UDFBase
from osprey.worker.lib.osprey_shared.logging import get_logger

logger = get_logger('stdout_logger', dynamic_log_sampler=None)


class LogArguments(ArgumentsBase):
    s: str
    when_all: List[bool] = []


class Log(UDFBase[LogArguments, None]):
    def execute(self, execution_context: ExecutionContext, arguments: LogArguments) -> None:
        if all(arguments.when_all) is False:
            return

        logger.info(arguments.s)
