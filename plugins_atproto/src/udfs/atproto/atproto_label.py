from dataclasses import dataclass
from typing import List, Optional, Self, cast

from ddtrace.internal.logger import get_logger
from osprey.bluesky.osprey_atproto_pb2 import (
    ATPROTO_EFFECT_KIND_ADD,
    ATPROTO_EFFECT_KIND_REMOVE,
    ATPROTO_EMAIL_AUTOMOD_TAKEDOWN,
    ATPROTO_EMAIL_DMS_DISABLED,
    ATPROTO_EMAIL_ID_REQUEST,
    ATPROTO_EMAIL_IMPERSONATION_LABEL,
    ATPROTO_EMAIL_PEDO_ACCOUNT_TAKEDOWN,
    ATPROTO_EMAIL_REINSTATEMENT,
    ATPROTO_EMAIL_SPAM_FAKE,
    ATPROTO_EMAIL_SPAM_LABEL,
    ATPROTO_EMAIL_SPAM_LABEL_24_HOURS,
    ATPROTO_EMAIL_SPAM_LABEL_72_HOURS,
    ATPROTO_EMAIL_SPAM_TAKEDOWN,
    ATPROTO_EMAIL_THREAT_POST_TAKEDOWN,
    ATPROTO_EMAIL_TOXIC_LIST_HIDE,
    ATPROTO_LABEL_HIDE,
    ATPROTO_LABEL_NEEDS_REVIEW,
    ATPROTO_LABEL_PORN,
    ATPROTO_LABEL_RUDE,
    ATPROTO_LABEL_SEXUAL,
    ATPROTO_LABEL_SPAM,
    ATPROTO_LABEL_WARN,
    ATPROTO_SUBJECT_KIND_ACTOR,
    ATPROTO_SUBJECT_KIND_RECORD,
    AtprotoEffectKind,
    AtprotoEmail,
    AtprotoLabel,
    AtprotoSubjectKind,
)
from osprey.engine.executor.custom_extracted_features import CustomExtractedFeature
from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.language_types.effects import EffectToCustomExtractedFeatureBase
from osprey.engine.stdlib.udfs.categories import UdfCategories
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import UDFBase
from osprey.engine.utils.types import add_slots

logger = get_logger('atproto_labels')


class StringToAtprotoLabelException(Exception):
    """Exception raised when the string passed to `StringToAtprotoLabel` is invalid."""


class EntityToSubjectKindException(Exception):
    """Exception raised when the string passed to `StringToSubjectKind` is invalid."""


def EntityToSubjectKind(entity: str):
    kind: AtprotoSubjectKind.ValueType
    if entity.startswith('did:'):
        kind = ATPROTO_SUBJECT_KIND_ACTOR
    elif entity.startswith('at://'):
        kind = ATPROTO_SUBJECT_KIND_RECORD
    else:
        raise EntityToSubjectKindException()
    return kind


def StringToAtprotoLabel(label_str: str):
    label: AtprotoLabel.ValueType
    if label_str == 'needs-review':
        label = ATPROTO_LABEL_NEEDS_REVIEW
    elif label_str == 'hide' or label_str == '!hide':
        label = ATPROTO_LABEL_HIDE
    elif label_str == 'warn' or label_str == '!warn':
        label = ATPROTO_LABEL_WARN
    elif label_str == 'porn':
        label = ATPROTO_LABEL_PORN
    elif label_str == 'sexual':
        label = ATPROTO_LABEL_SEXUAL
    elif label_str == 'rude':
        label = ATPROTO_LABEL_RUDE
    elif label_str == 'spam':
        label = ATPROTO_LABEL_SPAM
    else:
        raise StringToAtprotoLabelException()

    return label


class StringToAtprotoEmailException(Exception):
    """Exception raised when the string passed to `StringToAtprotoEmail` is invalid."""


def StringToAtprotoEmail(email_str: Optional[str]):
    if email_str == 'spam-label':
        return ATPROTO_EMAIL_SPAM_LABEL
    elif email_str == 'spam-label-24':
        return ATPROTO_EMAIL_SPAM_LABEL_24_HOURS
    elif email_str == 'spam-label-72':
        return ATPROTO_EMAIL_SPAM_LABEL_72_HOURS
    elif email_str == 'spam-takedown':
        return ATPROTO_EMAIL_SPAM_TAKEDOWN
    elif email_str == 'spam-fake':
        return ATPROTO_EMAIL_SPAM_FAKE
    elif email_str == 'id-request':
        return ATPROTO_EMAIL_ID_REQUEST
    elif email_str == 'impersonation-label':
        return ATPROTO_EMAIL_IMPERSONATION_LABEL
    elif email_str == 'automod-takedown':
        return ATPROTO_EMAIL_AUTOMOD_TAKEDOWN
    elif email_str == 'reinstatement':
        return ATPROTO_EMAIL_REINSTATEMENT
    elif email_str == 'threat-takedown':
        return ATPROTO_EMAIL_THREAT_POST_TAKEDOWN
    elif email_str == 'pedo-takedown':
        return ATPROTO_EMAIL_PEDO_ACCOUNT_TAKEDOWN
    elif email_str == 'dms-disabled':
        return ATPROTO_EMAIL_DMS_DISABLED
    elif email_str == 'toxic-list-hide':
        return ATPROTO_EMAIL_TOXIC_LIST_HIDE
    else:
        return None


class AddRemoveAtprotoLabelArguments(ArgumentsBase):
    entity: str
    label: str
    comment: str
    email: Optional[str]
    expiration_in_hours: Optional[int]


@dataclass
class AtprotoLabelEffect(EffectToCustomExtractedFeatureBase[List[str]]):
    """Stores a label effect of a WhenRules(...) invocation, which stores the label mutations that should occur once
    a given action has finished classification."""

    effect_kind: AtprotoEffectKind.ValueType
    """Whether this is an add or remove effect."""

    entity: str
    """The entity that the effect will be applied on."""

    label: AtprotoLabel.ValueType
    """The label that will be applied to the entity."""

    comment: str
    """The comment to add to the label event."""

    email: Optional[AtprotoEmail.ValueType]

    expiration_in_hours: Optional[int] = None
    """If set to true, the effect should not be applied."""

    def to_str(self) -> str:
        return f'{self.entity}|{self.label}|{self.comment}|{self.email}|{self.expiration_in_hours}'

    @classmethod
    def build_custom_extracted_feature_from_list(cls, values: List[Self]) -> CustomExtractedFeature[List[str]]:
        return AtprotoLabelEffectsExtractedFeature(effects=cast(List[AtprotoLabelEffect], values))


@add_slots
@dataclass
class AtprotoLabelEffectsExtractedFeature(CustomExtractedFeature[List[str]]):
    effects: List[AtprotoLabelEffect]

    @classmethod
    def feature_name(cls) -> str:
        return 'atproto_label'

    def get_serializable_feature(self) -> List[str] | None:
        return [effect.to_str() for effect in self.effects]


def synthesize_effect(
    arguments: AddRemoveAtprotoLabelArguments, effect_kind: AtprotoEffectKind.ValueType
) -> AtprotoLabelEffect:
    return AtprotoLabelEffect(
        effect_kind=effect_kind,
        entity=arguments.entity,
        label=StringToAtprotoLabel(arguments.label),
        comment=arguments.comment,
        email=StringToAtprotoEmail(arguments.email),
        expiration_in_hours=arguments.expiration_in_hours,
    )


class AddAtprotoLabel(UDFBase[AddRemoveAtprotoLabelArguments, AtprotoLabelEffect]):
    category = UdfCategories.ENGINE

    def execute(
        self, execution_context: ExecutionContext, arguments: AddRemoveAtprotoLabelArguments
    ) -> AtprotoLabelEffect:
        return synthesize_effect(arguments, ATPROTO_EFFECT_KIND_ADD)


class RemoveAtprotoLabel(UDFBase[AddRemoveAtprotoLabelArguments, AtprotoLabelEffect]):
    category = UdfCategories.ENGINE

    def execute(
        self, execution_context: ExecutionContext, arguments: AddRemoveAtprotoLabelArguments
    ) -> AtprotoLabelEffect:
        return synthesize_effect(arguments, ATPROTO_EFFECT_KIND_REMOVE)
