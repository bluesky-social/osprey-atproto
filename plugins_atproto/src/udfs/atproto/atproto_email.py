from dataclasses import dataclass
from typing import List, Optional, Self, cast

from ddtrace.internal.logger import get_logger
from osprey.bluesky.osprey_atproto_pb2 import ATPROTO_EFFECT_KIND_ADD, AtprotoEffectKind, AtprotoEmail
from osprey.engine.executor.custom_extracted_features import CustomExtractedFeature
from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.language_types.effects import EffectToCustomExtractedFeatureBase
from osprey.engine.stdlib.udfs.categories import UdfCategories
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import UDFBase
from osprey.engine.utils.types import add_slots
from udfs.atproto.atproto_label import StringToAtprotoEmail, StringToAtprotoLabelException

logger = get_logger('atproto_email')


class AtprotoEmailArguments(ArgumentsBase):
    entity: str
    email: str
    comment: Optional[str]


@dataclass
class AtprotoEmailEffect(EffectToCustomExtractedFeatureBase[List[str]]):
    """Stores a label effect of a WhenRules(...) invocation, which stores the label mutations that should occur once
    a given action has finished classification."""

    entity: str
    """The entity that the effect will be applied on."""

    email: AtprotoEmail.ValueType
    """The email that will be sent."""

    comment: Optional[str]
    """Comment to add."""

    def to_str(self) -> str:
        return f'{self.entity}|{self.comment}'

    @classmethod
    def build_custom_extracted_feature_from_list(cls, values: List[Self]) -> CustomExtractedFeature[List[str]]:
        return AtprotoEmailEffectsExtractedFeature(effects=cast(List[AtprotoEmailEffect], values))


@add_slots
@dataclass
class AtprotoEmailEffectsExtractedFeature(CustomExtractedFeature[List[str]]):
    effects: List[AtprotoEmailEffect]

    @classmethod
    def feature_name(cls) -> str:
        return 'atproto_tag'

    def get_serializable_feature(self) -> List[str] | None:
        return [effect.to_str() for effect in self.effects]


def synthesize_effect(arguments: AtprotoEmailArguments, effect_kind: AtprotoEffectKind.ValueType) -> AtprotoEmailEffect:
    email = StringToAtprotoEmail(arguments.email)
    if email is None:
        raise StringToAtprotoLabelException()

    return AtprotoEmailEffect(
        entity=arguments.entity,
        email=email,
        comment=arguments.comment,
    )


class AtprotoSendEmail(UDFBase[AtprotoEmailArguments, AtprotoEmailEffect]):
    category = UdfCategories.ENGINE

    def execute(self, execution_context: ExecutionContext, arguments: AtprotoEmailArguments) -> AtprotoEmailEffect:
        return synthesize_effect(arguments, ATPROTO_EFFECT_KIND_ADD)
