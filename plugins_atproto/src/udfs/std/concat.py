from typing import List, Optional

from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import UDFBase


class ConcatStringListsArguments(ArgumentsBase):
    lists: List[List[str]]
    optional_lists: List[List[Optional[str]]] = []


class ConcatStringLists(UDFBase[ConcatStringListsArguments, List[str]]):
    def execute(self, execution_context: ExecutionContext, arguments: ConcatStringListsArguments) -> List[str]:
        final: List[str] = []

        for list in arguments.lists:
            for item in list:
                final.append(item)

        for list in arguments.optional_lists:  # type: ignore[assignment]
            for item in list:
                if item is None:
                    continue
                final.append(item)

        return final
