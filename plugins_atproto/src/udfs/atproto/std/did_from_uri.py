from typing import Optional

from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.stdlib.udfs.categories import UdfCategories
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import UDFBase
from osprey.worker.lib.osprey_shared.logging import get_logger
from udfs.atproto.lib.uri import AtUri

logger = get_logger('did_from_uri')


class DidFromUriArguments(ArgumentsBase):
    uri: Optional[str]


class DidFromUri(UDFBase[DidFromUriArguments, Optional[str]]):
    category = UdfCategories.STRING

    def execute(self, execution_context: ExecutionContext, arguments: DidFromUriArguments) -> Optional[str]:
        if arguments.uri is None:
            return None

        try:
            aturi = AtUri.from_str(arguments.uri)
            return aturi.did
        except Exception as e:
            logger.error(f'Failed to get DID from input aturi {arguments.uri}. {e}')
            raise e
