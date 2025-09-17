from buf.validate import validate_pb2 as _validate_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class EventKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    EVENT_KIND_UNSPECIFIED: _ClassVar[EventKind]
    EVENT_KIND_COMMIT: _ClassVar[EventKind]
    EVENT_KIND_ACCOUNT: _ClassVar[EventKind]
    EVENT_KIND_IDENTITY: _ClassVar[EventKind]

class CommitOperation(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    COMMIT_OPERATION_UNSPECIFIED: _ClassVar[CommitOperation]
    COMMIT_OPERATION_CREATE: _ClassVar[CommitOperation]
    COMMIT_OPERATION_UPDATE: _ClassVar[CommitOperation]
    COMMIT_OPERATION_DELETE: _ClassVar[CommitOperation]
EVENT_KIND_UNSPECIFIED: EventKind
EVENT_KIND_COMMIT: EventKind
EVENT_KIND_ACCOUNT: EventKind
EVENT_KIND_IDENTITY: EventKind
COMMIT_OPERATION_UNSPECIFIED: CommitOperation
COMMIT_OPERATION_CREATE: CommitOperation
COMMIT_OPERATION_UPDATE: CommitOperation
COMMIT_OPERATION_DELETE: CommitOperation

class FirehoseEvent(_message.Message):
    __slots__ = ("did", "timestamp", "kind", "commit", "account", "identity")
    DID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    COMMIT_FIELD_NUMBER: _ClassVar[int]
    ACCOUNT_FIELD_NUMBER: _ClassVar[int]
    IDENTITY_FIELD_NUMBER: _ClassVar[int]
    did: str
    timestamp: _timestamp_pb2.Timestamp
    kind: EventKind
    commit: Commit
    account: bytes
    identity: bytes
    def __init__(self, did: _Optional[str] = ..., timestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., kind: _Optional[_Union[EventKind, str]] = ..., commit: _Optional[_Union[Commit, _Mapping]] = ..., account: _Optional[bytes] = ..., identity: _Optional[bytes] = ...) -> None: ...

class Commit(_message.Message):
    __slots__ = ("rev", "operation", "collection", "rkey", "record", "cid")
    REV_FIELD_NUMBER: _ClassVar[int]
    OPERATION_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_FIELD_NUMBER: _ClassVar[int]
    RKEY_FIELD_NUMBER: _ClassVar[int]
    RECORD_FIELD_NUMBER: _ClassVar[int]
    CID_FIELD_NUMBER: _ClassVar[int]
    rev: str
    operation: CommitOperation
    collection: str
    rkey: str
    record: bytes
    cid: str
    def __init__(self, rev: _Optional[str] = ..., operation: _Optional[_Union[CommitOperation, str]] = ..., collection: _Optional[str] = ..., rkey: _Optional[str] = ..., record: _Optional[bytes] = ..., cid: _Optional[str] = ...) -> None: ...

class Cursor(_message.Message):
    __slots__ = ("sequence", "saved_on_exit")
    SEQUENCE_FIELD_NUMBER: _ClassVar[int]
    SAVED_ON_EXIT_FIELD_NUMBER: _ClassVar[int]
    sequence: int
    saved_on_exit: bool
    def __init__(self, sequence: _Optional[int] = ..., saved_on_exit: bool = ...) -> None: ...
