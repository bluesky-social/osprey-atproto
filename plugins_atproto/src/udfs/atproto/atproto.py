from typing import Any, Dict

from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.stdlib.udfs.categories import UdfCategories
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import UDFBase
from osprey.worker.lib.osprey_shared.logging import get_logger
from udfs.atproto.lib.audit_log import AuditLog
from udfs.atproto.lib.did_doc import DIDDocument

logger = get_logger('atproto_std')

PDS_SERVICE_ID = '#atproto_pds'


class GetRecordURIException(Exception):
    """Exception raised when unable to create a URI from input dictionary."""


def GetRecordURIFromData(data: Dict[str, Any]):
    return f'at://{data["did"]}/{data["collection"]}/{data["rkey"]}'


def GetRecordCIDFromData(data: Dict[str, Any]):
    return f'{data["cid"]}'


class GetRecordURI(UDFBase[ArgumentsBase, str]):
    category = UdfCategories.STRING

    def execute(self, execution_context: ExecutionContext, arguments: ArgumentsBase) -> str:
        data = execution_context.get_data()

        return GetRecordURIFromData(data)


class GetRecordCID(UDFBase[ArgumentsBase, str]):
    category = UdfCategories.STRING

    def execute(self, execution_context: ExecutionContext, arguments: ArgumentsBase) -> str:
        data = execution_context.get_data()
        return GetRecordCIDFromData(data)


class GetDIDCreatedAt(UDFBase[ArgumentsBase, str]):
    category = UdfCategories.STRING

    def execute(self, execution_context: ExecutionContext, arguments: ArgumentsBase) -> str:
        data = execution_context.get_data()
        did = data['did']

        try:
            audit_log = AuditLog.parse_obj(data['did_audit_log'])

            if len(audit_log.entries) == 0:
                logger.warning(f'Empty entries array in DID audit log for {did}.\n{data["did_audit_log"]}')
                return 'Error'

            first_entry = audit_log.entries[0]
            return first_entry.created_at
        except Exception as e:
            logger.error(f'Error validating DID audit log: {e} \n{data["did_audit_log"]}')
            return 'Error'


class GetPDSService(UDFBase[ArgumentsBase, str]):
    category = UdfCategories.STRING

    def execute(self, execution_context: ExecutionContext, arguments: ArgumentsBase) -> str:
        data = execution_context.get_data()
        did = data['did']

        try:
            did_doc = DIDDocument.parse_obj(data['did_doc'])

            if did_doc.service is None:
                logger.warning(f'DID document service array was missing for {did}.\n{data["did_doc"]}')
                return 'Error'

            for service in did_doc.service:
                if service.id == PDS_SERVICE_ID:
                    return service.service_endpoint

            logger.warning(f'Did not find a {PDS_SERVICE_ID} inside DID document for {did}.\n{data["did_doc"]}')
            return 'Error'
        except Exception as e:
            logger.error(f'Error validing DID document: {e}\n{data["did_doc"]}')
            return 'Error'


class GetHandle(UDFBase[ArgumentsBase, str]):
    category = UdfCategories.STRING

    def execute(self, execution_context: ExecutionContext, arguments: ArgumentsBase) -> str:
        data = execution_context.get_data()
        did = data['did']

        try:
            did_doc = DIDDocument.parse_obj(data['did_doc'])

            if did_doc.also_known_as is None:
                logger.warning(f'DID document alsoKnownAs array was missing for {did}.\n{data["did_doc"]}')
                return 'Error'

            for aka in did_doc.also_known_as:
                if aka.startswith('at://'):
                    return aka.lstrip('at://')

            logger.warning(f'Did not find a handle inside DID document for {did}.\n{data["did_doc"]}')
            return 'Error'
        except Exception as e:
            logger.error(f'Error validing DID document: {e}\n{data["did_doc"]}')
            return 'Error'
