from dataclasses import dataclass
from typing import List, Optional, Self, cast

from ddtrace.internal.logger import get_logger
from osprey.engine.executor.custom_extracted_features import CustomExtractedFeature
from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.language_types.effects import EffectToCustomExtractedFeatureBase
from osprey.engine.stdlib.udfs.categories import UdfCategories
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import UDFBase
from osprey.engine.utils.types import add_slots
from rpc.osprey_atproto_pb2 import (
    ATPROTO_EFFECT_KIND_ADD,
    ATPROTO_EFFECT_KIND_REMOVE,
    AtprotoEffectKind,
    AtprotoEmail,
)
from udfs.atproto.atproto_label import StringToAtprotoEmail

logger = get_logger('atproto_takedowns')


class AddRemoveAtprotoTakedownArguments(ArgumentsBase):
    entity: str
    comment: str
    email: Optional[str]


@dataclass
class AtprotoTakedownEffect(EffectToCustomExtractedFeatureBase[List[str]]):
    """Stores a label effect of a WhenRules(...) invocation, which stores the label mutations that should occur once
    a given action has finished classification."""

    effect_kind: AtprotoEffectKind.ValueType

    entity: str
    """The entity that the takedown will be applied to."""

    comment: str
    """The comment that will be included with the takedown."""

    email: Optional[AtprotoEmail.ValueType]
    """The email that will be sent along with the takedown to the user."""

    def to_str(self) -> str:
        return f'{self.entity}|{self.comment}|{self.email}'

    @classmethod
    def build_custom_extracted_feature_from_list(cls, values: List[Self]) -> CustomExtractedFeature[List[str]]:
        return AtprotoTakedownEffectsExtractedFeature(effects=cast(List[AtprotoTakedownEffect], values))


@add_slots
@dataclass
class AtprotoTakedownEffectsExtractedFeature(CustomExtractedFeature[List[str]]):
    effects: List[AtprotoTakedownEffect]

    @classmethod
    def feature_name(cls) -> str:
        return 'atproto_takedown'

    def get_serializable_feature(self) -> List[str] | None:
        return [effect.to_str() for effect in self.effects]


def synthesize_effect(
    arguments: AddRemoveAtprotoTakedownArguments, effect_kind: AtprotoEffectKind.ValueType
) -> AtprotoTakedownEffect:
    return AtprotoTakedownEffect(
        effect_kind=effect_kind,
        entity=arguments.entity,
        comment=arguments.comment,
        email=StringToAtprotoEmail(arguments.email),
    )


class AddAtprotoTakedown(UDFBase[AddRemoveAtprotoTakedownArguments, AtprotoTakedownEffect]):
    category = UdfCategories.ENGINE

    def execute(
        self, execution_context: ExecutionContext, arguments: AddRemoveAtprotoTakedownArguments
    ) -> AtprotoTakedownEffect:
        return synthesize_effect(arguments, ATPROTO_EFFECT_KIND_ADD)


class RemoveAtprotoTakedown(UDFBase[AddRemoveAtprotoTakedownArguments, AtprotoTakedownEffect]):
    category = UdfCategories.ENGINE

    def execute(
        self, execution_context: ExecutionContext, arguments: AddRemoveAtprotoTakedownArguments
    ) -> AtprotoTakedownEffect:
        return synthesize_effect(arguments, ATPROTO_EFFECT_KIND_REMOVE)
