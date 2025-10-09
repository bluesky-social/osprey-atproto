from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class AtprotoSubjectKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ATPROTO_SUBJECT_KIND_NONE: _ClassVar[AtprotoSubjectKind]
    ATPROTO_SUBJECT_KIND_ACTOR: _ClassVar[AtprotoSubjectKind]
    ATPROTO_SUBJECT_KIND_RECORD: _ClassVar[AtprotoSubjectKind]

class AtprotoLabel(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ATPROTO_LABEL_NONE: _ClassVar[AtprotoLabel]
    ATPROTO_LABEL_SPAM: _ClassVar[AtprotoLabel]
    ATPROTO_LABEL_RUDE: _ClassVar[AtprotoLabel]
    ATPROTO_LABEL_PORN: _ClassVar[AtprotoLabel]
    ATPROTO_LABEL_SEXUAL: _ClassVar[AtprotoLabel]
    ATPROTO_LABEL_WARN: _ClassVar[AtprotoLabel]
    ATPROTO_LABEL_HIDE: _ClassVar[AtprotoLabel]
    ATPROTO_LABEL_NEEDS_REVIEW: _ClassVar[AtprotoLabel]
    ATPROTO_LABEL_MISLEADING: _ClassVar[AtprotoLabel]

class AtprotoEffectKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ATPROTO_EFFECT_KIND_NONE: _ClassVar[AtprotoEffectKind]
    ATPROTO_EFFECT_KIND_ADD: _ClassVar[AtprotoEffectKind]
    ATPROTO_EFFECT_KIND_REMOVE: _ClassVar[AtprotoEffectKind]

class AtprotoEmail(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ATPROTO_EMAIL_NONE: _ClassVar[AtprotoEmail]
    ATPROTO_EMAIL_SPAM_REPLY: _ClassVar[AtprotoEmail]
    ATPROTO_EMAIL_SPAM_TAKEDOWN: _ClassVar[AtprotoEmail]
    ATPROTO_EMAIL_SPAM_FAKE: _ClassVar[AtprotoEmail]
    ATPROTO_EMAIL_SPAM_LABEL: _ClassVar[AtprotoEmail]
    ATPROTO_EMAIL_SPAM_LABEL_24_HOURS: _ClassVar[AtprotoEmail]
    ATPROTO_EMAIL_SPAM_LABEL_72_HOURS: _ClassVar[AtprotoEmail]
    ATPROTO_EMAIL_ID_REQUEST: _ClassVar[AtprotoEmail]
    ATPROTO_EMAIL_IMPERSONATION_LABEL: _ClassVar[AtprotoEmail]
    ATPROTO_EMAIL_AUTOMOD_TAKEDOWN: _ClassVar[AtprotoEmail]
    ATPROTO_EMAIL_REINSTATEMENT: _ClassVar[AtprotoEmail]
    ATPROTO_EMAIL_THREAT_POST_TAKEDOWN: _ClassVar[AtprotoEmail]
    ATPROTO_EMAIL_PEDO_ACCOUNT_TAKEDOWN: _ClassVar[AtprotoEmail]
    ATPROTO_EMAIL_DMS_DISABLED: _ClassVar[AtprotoEmail]
    ATPROTO_EMAIL_TOXIC_LIST_HIDE: _ClassVar[AtprotoEmail]

class AtprotoReportKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ATPROTO_REPORT_KIND_NONE: _ClassVar[AtprotoReportKind]
    ATPROTO_REPORT_KIND_SPAM: _ClassVar[AtprotoReportKind]
    ATPROTO_REPORT_KIND_VIOLATION: _ClassVar[AtprotoReportKind]
    ATPROTO_REPORT_KIND_MISLEADING: _ClassVar[AtprotoReportKind]
    ATPROTO_REPORT_KIND_SEXUAL: _ClassVar[AtprotoReportKind]
    ATPROTO_REPORT_KIND_RUDE: _ClassVar[AtprotoReportKind]
    ATPROTO_REPORT_KIND_OTHER: _ClassVar[AtprotoReportKind]

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
ATPROTO_SUBJECT_KIND_NONE: AtprotoSubjectKind
ATPROTO_SUBJECT_KIND_ACTOR: AtprotoSubjectKind
ATPROTO_SUBJECT_KIND_RECORD: AtprotoSubjectKind
ATPROTO_LABEL_NONE: AtprotoLabel
ATPROTO_LABEL_SPAM: AtprotoLabel
ATPROTO_LABEL_RUDE: AtprotoLabel
ATPROTO_LABEL_PORN: AtprotoLabel
ATPROTO_LABEL_SEXUAL: AtprotoLabel
ATPROTO_LABEL_WARN: AtprotoLabel
ATPROTO_LABEL_HIDE: AtprotoLabel
ATPROTO_LABEL_NEEDS_REVIEW: AtprotoLabel
ATPROTO_LABEL_MISLEADING: AtprotoLabel
ATPROTO_EFFECT_KIND_NONE: AtprotoEffectKind
ATPROTO_EFFECT_KIND_ADD: AtprotoEffectKind
ATPROTO_EFFECT_KIND_REMOVE: AtprotoEffectKind
ATPROTO_EMAIL_NONE: AtprotoEmail
ATPROTO_EMAIL_SPAM_REPLY: AtprotoEmail
ATPROTO_EMAIL_SPAM_TAKEDOWN: AtprotoEmail
ATPROTO_EMAIL_SPAM_FAKE: AtprotoEmail
ATPROTO_EMAIL_SPAM_LABEL: AtprotoEmail
ATPROTO_EMAIL_SPAM_LABEL_24_HOURS: AtprotoEmail
ATPROTO_EMAIL_SPAM_LABEL_72_HOURS: AtprotoEmail
ATPROTO_EMAIL_ID_REQUEST: AtprotoEmail
ATPROTO_EMAIL_IMPERSONATION_LABEL: AtprotoEmail
ATPROTO_EMAIL_AUTOMOD_TAKEDOWN: AtprotoEmail
ATPROTO_EMAIL_REINSTATEMENT: AtprotoEmail
ATPROTO_EMAIL_THREAT_POST_TAKEDOWN: AtprotoEmail
ATPROTO_EMAIL_PEDO_ACCOUNT_TAKEDOWN: AtprotoEmail
ATPROTO_EMAIL_DMS_DISABLED: AtprotoEmail
ATPROTO_EMAIL_TOXIC_LIST_HIDE: AtprotoEmail
ATPROTO_REPORT_KIND_NONE: AtprotoReportKind
ATPROTO_REPORT_KIND_SPAM: AtprotoReportKind
ATPROTO_REPORT_KIND_VIOLATION: AtprotoReportKind
ATPROTO_REPORT_KIND_MISLEADING: AtprotoReportKind
ATPROTO_REPORT_KIND_SEXUAL: AtprotoReportKind
ATPROTO_REPORT_KIND_RUDE: AtprotoReportKind
ATPROTO_REPORT_KIND_OTHER: AtprotoReportKind
EVENT_KIND_UNSPECIFIED: EventKind
EVENT_KIND_COMMIT: EventKind
EVENT_KIND_ACCOUNT: EventKind
EVENT_KIND_IDENTITY: EventKind
COMMIT_OPERATION_UNSPECIFIED: CommitOperation
COMMIT_OPERATION_CREATE: CommitOperation
COMMIT_OPERATION_UPDATE: CommitOperation
COMMIT_OPERATION_DELETE: CommitOperation

class OspreyInputEvent(_message.Message):
    __slots__ = ("data", "send_time")
    DATA_FIELD_NUMBER: _ClassVar[int]
    SEND_TIME_FIELD_NUMBER: _ClassVar[int]
    data: OspreyInputEventData
    send_time: _timestamp_pb2.Timestamp
    def __init__(self, data: _Optional[_Union[OspreyInputEventData, _Mapping]] = ..., send_time: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class OspreyInputEventData(_message.Message):
    __slots__ = ("action_name", "action_id", "data", "timestamp", "secret_data", "encoding")
    class SecretDataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    ACTION_NAME_FIELD_NUMBER: _ClassVar[int]
    ACTION_ID_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    SECRET_DATA_FIELD_NUMBER: _ClassVar[int]
    ENCODING_FIELD_NUMBER: _ClassVar[int]
    action_name: str
    action_id: int
    data: bytes
    timestamp: _timestamp_pb2.Timestamp
    secret_data: _containers.ScalarMap[str, str]
    encoding: str
    def __init__(self, action_name: _Optional[str] = ..., action_id: _Optional[int] = ..., data: _Optional[bytes] = ..., timestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., secret_data: _Optional[_Mapping[str, str]] = ..., encoding: _Optional[str] = ...) -> None: ...

class AtprotoLabelEffect(_message.Message):
    __slots__ = ("effect_kind", "subject_kind", "label", "comment", "email", "expiration_in_hours", "rules")
    EFFECT_KIND_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_KIND_FIELD_NUMBER: _ClassVar[int]
    LABEL_FIELD_NUMBER: _ClassVar[int]
    COMMENT_FIELD_NUMBER: _ClassVar[int]
    EMAIL_FIELD_NUMBER: _ClassVar[int]
    EXPIRATION_IN_HOURS_FIELD_NUMBER: _ClassVar[int]
    RULES_FIELD_NUMBER: _ClassVar[int]
    effect_kind: AtprotoEffectKind
    subject_kind: AtprotoSubjectKind
    label: AtprotoLabel
    comment: str
    email: AtprotoEmail
    expiration_in_hours: int
    rules: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, effect_kind: _Optional[_Union[AtprotoEffectKind, str]] = ..., subject_kind: _Optional[_Union[AtprotoSubjectKind, str]] = ..., label: _Optional[_Union[AtprotoLabel, str]] = ..., comment: _Optional[str] = ..., email: _Optional[_Union[AtprotoEmail, str]] = ..., expiration_in_hours: _Optional[int] = ..., rules: _Optional[_Iterable[str]] = ...) -> None: ...

class AtprotoTagEffect(_message.Message):
    __slots__ = ("effect_kind", "subject_kind", "tag", "comment", "rules")
    EFFECT_KIND_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_KIND_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    COMMENT_FIELD_NUMBER: _ClassVar[int]
    RULES_FIELD_NUMBER: _ClassVar[int]
    effect_kind: AtprotoEffectKind
    subject_kind: AtprotoSubjectKind
    tag: str
    comment: str
    rules: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, effect_kind: _Optional[_Union[AtprotoEffectKind, str]] = ..., subject_kind: _Optional[_Union[AtprotoSubjectKind, str]] = ..., tag: _Optional[str] = ..., comment: _Optional[str] = ..., rules: _Optional[_Iterable[str]] = ...) -> None: ...

class AtprotoTakedownEffect(_message.Message):
    __slots__ = ("effect_kind", "subject_kind", "comment", "email", "rules")
    EFFECT_KIND_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_KIND_FIELD_NUMBER: _ClassVar[int]
    COMMENT_FIELD_NUMBER: _ClassVar[int]
    EMAIL_FIELD_NUMBER: _ClassVar[int]
    RULES_FIELD_NUMBER: _ClassVar[int]
    effect_kind: AtprotoEffectKind
    subject_kind: AtprotoSubjectKind
    comment: str
    email: AtprotoEmail
    rules: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, effect_kind: _Optional[_Union[AtprotoEffectKind, str]] = ..., subject_kind: _Optional[_Union[AtprotoSubjectKind, str]] = ..., comment: _Optional[str] = ..., email: _Optional[_Union[AtprotoEmail, str]] = ..., rules: _Optional[_Iterable[str]] = ...) -> None: ...

class AtprotoEmailEffect(_message.Message):
    __slots__ = ("email", "comment", "rules")
    EMAIL_FIELD_NUMBER: _ClassVar[int]
    COMMENT_FIELD_NUMBER: _ClassVar[int]
    RULES_FIELD_NUMBER: _ClassVar[int]
    email: AtprotoEmail
    comment: str
    rules: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, email: _Optional[_Union[AtprotoEmail, str]] = ..., comment: _Optional[str] = ..., rules: _Optional[_Iterable[str]] = ...) -> None: ...

class AtprotoCommentEffect(_message.Message):
    __slots__ = ("subject_kind", "comment", "rules")
    SUBJECT_KIND_FIELD_NUMBER: _ClassVar[int]
    COMMENT_FIELD_NUMBER: _ClassVar[int]
    RULES_FIELD_NUMBER: _ClassVar[int]
    subject_kind: AtprotoSubjectKind
    comment: str
    rules: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, subject_kind: _Optional[_Union[AtprotoSubjectKind, str]] = ..., comment: _Optional[str] = ..., rules: _Optional[_Iterable[str]] = ...) -> None: ...

class AtprotoEscalateEffect(_message.Message):
    __slots__ = ("subject_kind", "comment", "rules")
    SUBJECT_KIND_FIELD_NUMBER: _ClassVar[int]
    COMMENT_FIELD_NUMBER: _ClassVar[int]
    RULES_FIELD_NUMBER: _ClassVar[int]
    subject_kind: AtprotoSubjectKind
    comment: str
    rules: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, subject_kind: _Optional[_Union[AtprotoSubjectKind, str]] = ..., comment: _Optional[str] = ..., rules: _Optional[_Iterable[str]] = ...) -> None: ...

class AtprotoAcknowledgeEffect(_message.Message):
    __slots__ = ("subject_kind", "comment", "rules")
    SUBJECT_KIND_FIELD_NUMBER: _ClassVar[int]
    COMMENT_FIELD_NUMBER: _ClassVar[int]
    RULES_FIELD_NUMBER: _ClassVar[int]
    subject_kind: AtprotoSubjectKind
    comment: str
    rules: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, subject_kind: _Optional[_Union[AtprotoSubjectKind, str]] = ..., comment: _Optional[str] = ..., rules: _Optional[_Iterable[str]] = ...) -> None: ...

class AtprotoReportEffect(_message.Message):
    __slots__ = ("subject_kind", "report_kind", "comment", "priority_score", "rules")
    SUBJECT_KIND_FIELD_NUMBER: _ClassVar[int]
    REPORT_KIND_FIELD_NUMBER: _ClassVar[int]
    COMMENT_FIELD_NUMBER: _ClassVar[int]
    PRIORITY_SCORE_FIELD_NUMBER: _ClassVar[int]
    RULES_FIELD_NUMBER: _ClassVar[int]
    subject_kind: AtprotoSubjectKind
    report_kind: AtprotoReportKind
    comment: str
    priority_score: int
    rules: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, subject_kind: _Optional[_Union[AtprotoSubjectKind, str]] = ..., report_kind: _Optional[_Union[AtprotoReportKind, str]] = ..., comment: _Optional[str] = ..., priority_score: _Optional[int] = ..., rules: _Optional[_Iterable[str]] = ...) -> None: ...

class BigQueryFlagEffect(_message.Message):
    __slots__ = ("subject_kind", "tag", "comment", "rules")
    SUBJECT_KIND_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    COMMENT_FIELD_NUMBER: _ClassVar[int]
    RULES_FIELD_NUMBER: _ClassVar[int]
    subject_kind: AtprotoSubjectKind
    tag: str
    comment: str
    rules: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, subject_kind: _Optional[_Union[AtprotoSubjectKind, str]] = ..., tag: _Optional[str] = ..., comment: _Optional[str] = ..., rules: _Optional[_Iterable[str]] = ...) -> None: ...

class ResultEvent(_message.Message):
    __slots__ = ("send_time", "action_name", "action_id", "did", "uri", "cid", "data", "labels", "tags", "takedowns", "emails", "comments", "escalations", "acknowledgements", "reports", "bigqueryFlags")
    SEND_TIME_FIELD_NUMBER: _ClassVar[int]
    ACTION_NAME_FIELD_NUMBER: _ClassVar[int]
    ACTION_ID_FIELD_NUMBER: _ClassVar[int]
    DID_FIELD_NUMBER: _ClassVar[int]
    URI_FIELD_NUMBER: _ClassVar[int]
    CID_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    LABELS_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    TAKEDOWNS_FIELD_NUMBER: _ClassVar[int]
    EMAILS_FIELD_NUMBER: _ClassVar[int]
    COMMENTS_FIELD_NUMBER: _ClassVar[int]
    ESCALATIONS_FIELD_NUMBER: _ClassVar[int]
    ACKNOWLEDGEMENTS_FIELD_NUMBER: _ClassVar[int]
    REPORTS_FIELD_NUMBER: _ClassVar[int]
    BIGQUERYFLAGS_FIELD_NUMBER: _ClassVar[int]
    send_time: _timestamp_pb2.Timestamp
    action_name: str
    action_id: int
    did: str
    uri: str
    cid: str
    data: bytes
    labels: _containers.RepeatedCompositeFieldContainer[AtprotoLabelEffect]
    tags: _containers.RepeatedCompositeFieldContainer[AtprotoTagEffect]
    takedowns: _containers.RepeatedCompositeFieldContainer[AtprotoTakedownEffect]
    emails: _containers.RepeatedCompositeFieldContainer[AtprotoEmailEffect]
    comments: _containers.RepeatedCompositeFieldContainer[AtprotoCommentEffect]
    escalations: _containers.RepeatedCompositeFieldContainer[AtprotoEscalateEffect]
    acknowledgements: _containers.RepeatedCompositeFieldContainer[AtprotoAcknowledgeEffect]
    reports: _containers.RepeatedCompositeFieldContainer[AtprotoReportEffect]
    bigqueryFlags: _containers.RepeatedCompositeFieldContainer[BigQueryFlagEffect]
    def __init__(self, send_time: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., action_name: _Optional[str] = ..., action_id: _Optional[int] = ..., did: _Optional[str] = ..., uri: _Optional[str] = ..., cid: _Optional[str] = ..., data: _Optional[bytes] = ..., labels: _Optional[_Iterable[_Union[AtprotoLabelEffect, _Mapping]]] = ..., tags: _Optional[_Iterable[_Union[AtprotoTagEffect, _Mapping]]] = ..., takedowns: _Optional[_Iterable[_Union[AtprotoTakedownEffect, _Mapping]]] = ..., emails: _Optional[_Iterable[_Union[AtprotoEmailEffect, _Mapping]]] = ..., comments: _Optional[_Iterable[_Union[AtprotoCommentEffect, _Mapping]]] = ..., escalations: _Optional[_Iterable[_Union[AtprotoEscalateEffect, _Mapping]]] = ..., acknowledgements: _Optional[_Iterable[_Union[AtprotoAcknowledgeEffect, _Mapping]]] = ..., reports: _Optional[_Iterable[_Union[AtprotoReportEffect, _Mapping]]] = ..., bigqueryFlags: _Optional[_Iterable[_Union[BigQueryFlagEffect, _Mapping]]] = ...) -> None: ...

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

class ModerationEnrichedFirehoseRecordEvent(_message.Message):
    __slots__ = ("did", "timestamp", "collection", "rkey", "operation", "record", "image_results", "ozone_repo_view_detail", "did_doc", "profile_view", "did_audit_log", "cid")
    class ImageResultsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: ImageDispatchResults
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[ImageDispatchResults, _Mapping]] = ...) -> None: ...
    DID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_FIELD_NUMBER: _ClassVar[int]
    RKEY_FIELD_NUMBER: _ClassVar[int]
    OPERATION_FIELD_NUMBER: _ClassVar[int]
    RECORD_FIELD_NUMBER: _ClassVar[int]
    IMAGE_RESULTS_FIELD_NUMBER: _ClassVar[int]
    OZONE_REPO_VIEW_DETAIL_FIELD_NUMBER: _ClassVar[int]
    DID_DOC_FIELD_NUMBER: _ClassVar[int]
    PROFILE_VIEW_FIELD_NUMBER: _ClassVar[int]
    DID_AUDIT_LOG_FIELD_NUMBER: _ClassVar[int]
    CID_FIELD_NUMBER: _ClassVar[int]
    did: str
    timestamp: _timestamp_pb2.Timestamp
    collection: str
    rkey: str
    operation: CommitOperation
    record: bytes
    image_results: _containers.MessageMap[str, ImageDispatchResults]
    ozone_repo_view_detail: bytes
    did_doc: bytes
    profile_view: bytes
    did_audit_log: bytes
    cid: str
    def __init__(self, did: _Optional[str] = ..., timestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., collection: _Optional[str] = ..., rkey: _Optional[str] = ..., operation: _Optional[_Union[CommitOperation, str]] = ..., record: _Optional[bytes] = ..., image_results: _Optional[_Mapping[str, ImageDispatchResults]] = ..., ozone_repo_view_detail: _Optional[bytes] = ..., did_doc: _Optional[bytes] = ..., profile_view: _Optional[bytes] = ..., did_audit_log: _Optional[bytes] = ..., cid: _Optional[str] = ...) -> None: ...

class ImageDispatchResults(_message.Message):
    __slots__ = ("cid", "abyss", "hive", "retina", "prescreen", "retina_hash")
    class AbyssResults(_message.Message):
        __slots__ = ("raw", "error", "is_abuse_match")
        RAW_FIELD_NUMBER: _ClassVar[int]
        ERROR_FIELD_NUMBER: _ClassVar[int]
        IS_ABUSE_MATCH_FIELD_NUMBER: _ClassVar[int]
        raw: bytes
        error: str
        is_abuse_match: bool
        def __init__(self, raw: _Optional[bytes] = ..., error: _Optional[str] = ..., is_abuse_match: bool = ...) -> None: ...
    class HiveResults(_message.Message):
        __slots__ = ("raw", "error", "classes")
        class ClassesEntry(_message.Message):
            __slots__ = ("key", "value")
            KEY_FIELD_NUMBER: _ClassVar[int]
            VALUE_FIELD_NUMBER: _ClassVar[int]
            key: str
            value: float
            def __init__(self, key: _Optional[str] = ..., value: _Optional[float] = ...) -> None: ...
        RAW_FIELD_NUMBER: _ClassVar[int]
        ERROR_FIELD_NUMBER: _ClassVar[int]
        CLASSES_FIELD_NUMBER: _ClassVar[int]
        raw: bytes
        error: str
        classes: _containers.ScalarMap[str, float]
        def __init__(self, raw: _Optional[bytes] = ..., error: _Optional[str] = ..., classes: _Optional[_Mapping[str, float]] = ...) -> None: ...
    class RetinaResults(_message.Message):
        __slots__ = ("raw", "error", "text")
        RAW_FIELD_NUMBER: _ClassVar[int]
        ERROR_FIELD_NUMBER: _ClassVar[int]
        TEXT_FIELD_NUMBER: _ClassVar[int]
        raw: bytes
        error: str
        text: str
        def __init__(self, raw: _Optional[bytes] = ..., error: _Optional[str] = ..., text: _Optional[str] = ...) -> None: ...
    class RetinaHashResults(_message.Message):
        __slots__ = ("raw", "error", "hash", "quality_too_low")
        RAW_FIELD_NUMBER: _ClassVar[int]
        ERROR_FIELD_NUMBER: _ClassVar[int]
        HASH_FIELD_NUMBER: _ClassVar[int]
        QUALITY_TOO_LOW_FIELD_NUMBER: _ClassVar[int]
        raw: bytes
        error: str
        hash: str
        quality_too_low: bool
        def __init__(self, raw: _Optional[bytes] = ..., error: _Optional[str] = ..., hash: _Optional[str] = ..., quality_too_low: bool = ...) -> None: ...
    class PrescreenResults(_message.Message):
        __slots__ = ("raw", "error", "decision")
        RAW_FIELD_NUMBER: _ClassVar[int]
        ERROR_FIELD_NUMBER: _ClassVar[int]
        DECISION_FIELD_NUMBER: _ClassVar[int]
        raw: bytes
        error: str
        decision: str
        def __init__(self, raw: _Optional[bytes] = ..., error: _Optional[str] = ..., decision: _Optional[str] = ...) -> None: ...
    CID_FIELD_NUMBER: _ClassVar[int]
    ABYSS_FIELD_NUMBER: _ClassVar[int]
    HIVE_FIELD_NUMBER: _ClassVar[int]
    RETINA_FIELD_NUMBER: _ClassVar[int]
    PRESCREEN_FIELD_NUMBER: _ClassVar[int]
    RETINA_HASH_FIELD_NUMBER: _ClassVar[int]
    cid: str
    abyss: ImageDispatchResults.AbyssResults
    hive: ImageDispatchResults.HiveResults
    retina: ImageDispatchResults.RetinaResults
    prescreen: ImageDispatchResults.PrescreenResults
    retina_hash: ImageDispatchResults.RetinaHashResults
    def __init__(self, cid: _Optional[str] = ..., abyss: _Optional[_Union[ImageDispatchResults.AbyssResults, _Mapping]] = ..., hive: _Optional[_Union[ImageDispatchResults.HiveResults, _Mapping]] = ..., retina: _Optional[_Union[ImageDispatchResults.RetinaResults, _Mapping]] = ..., prescreen: _Optional[_Union[ImageDispatchResults.PrescreenResults, _Mapping]] = ..., retina_hash: _Optional[_Union[ImageDispatchResults.RetinaHashResults, _Mapping]] = ...) -> None: ...
