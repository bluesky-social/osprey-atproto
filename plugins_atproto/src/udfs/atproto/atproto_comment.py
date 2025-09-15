from dataclasses import dataclass
from typing import List, Self, cast

from ddtrace.internal.logger import get_logger
from osprey.engine.executor.custom_extracted_features import CustomExtractedFeature
from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.language_types.effects import EffectToCustomExtractedFeatureBase
from osprey.engine.stdlib.udfs.categories import UdfCategories
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import UDFBase
from osprey.engine.utils.types import add_slots
from rpc.osprey_atproto_pb2 import ATPROTO_EFFECT_KIND_ADD, AtprotoEffectKind

logger = get_logger('atproto_comment')


class AtprotoCommentArguments(ArgumentsBase):
    entity: str
    comment: str


@dataclass
class AtprotoCommentEffect(EffectToCustomExtractedFeatureBase[List[str]]):
    """Stores a label effect of a WhenRules(...) invocation, which stores the label mutations that should occur once
    a given action has finished classification."""

    entity: str
    """The entity that the effect will be applied on."""

    comment: str
    """Comment to add."""

    def to_str(self) -> str:
        return f'{self.entity}|{self.comment}'

    @classmethod
    def build_custom_extracted_feature_from_list(cls, values: List[Self]) -> CustomExtractedFeature[List[str]]:
        return AtprotoCommentEffectsExtractedFeature(effects=cast(List[AtprotoCommentEffect], values))


@add_slots
@dataclass
class AtprotoCommentEffectsExtractedFeature(CustomExtractedFeature[List[str]]):
    effects: List[AtprotoCommentEffect]

    @classmethod
    def feature_name(cls) -> str:
        return 'atproto_tag'

    def get_serializable_feature(self) -> List[str] | None:
        return [effect.to_str() for effect in self.effects]


def synthesize_effect(
    arguments: AtprotoCommentArguments, effect_kind: AtprotoEffectKind.ValueType
) -> AtprotoCommentEffect:
    return AtprotoCommentEffect(
        entity=arguments.entity,
        comment=arguments.comment,
    )


class AtprotoComment(UDFBase[AtprotoCommentArguments, AtprotoCommentEffect]):
    category = UdfCategories.ENGINE

    def execute(self, execution_context: ExecutionContext, arguments: AtprotoCommentArguments) -> AtprotoCommentEffect:
        return synthesize_effect(arguments, ATPROTO_EFFECT_KIND_ADD)
