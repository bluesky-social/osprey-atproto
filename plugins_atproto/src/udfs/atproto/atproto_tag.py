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
from rpc.osprey_atproto_pb2 import ATPROTO_EFFECT_KIND_ADD, ATPROTO_EFFECT_KIND_REMOVE, AtprotoEffectKind

logger = get_logger('atproto_tag')


class AddRemoveAtprotoTagArguments(ArgumentsBase):
    entity: str
    tag: str
    comment: Optional[str] = None


@dataclass
class AtprotoTagEffect(EffectToCustomExtractedFeatureBase[List[str]]):
    """Stores a label effect of a WhenRules(...) invocation, which stores the label mutations that should occur once
    a given action has finished classification."""

    effect_kind: AtprotoEffectKind.ValueType
    """Whether this is an add or remove effect kind."""

    entity: str
    """The entity that the effect will be applied on."""

    tag: str
    """The tag that will be applied to the entity."""

    comment: Optional[str]
    """Optional comment that will be included with the tag."""

    def to_str(self) -> str:
        return f'{self.entity}|{self.tag}'

    @classmethod
    def build_custom_extracted_feature_from_list(cls, values: List[Self]) -> CustomExtractedFeature[List[str]]:
        return AtprotoTagEffectsExtractedFeature(effects=cast(List[AtprotoTagEffect], values))


@add_slots
@dataclass
class AtprotoTagEffectsExtractedFeature(CustomExtractedFeature[List[str]]):
    effects: List[AtprotoTagEffect]

    @classmethod
    def feature_name(cls) -> str:
        return 'atproto_tag'

    def get_serializable_feature(self) -> List[str] | None:
        return [effect.to_str() for effect in self.effects]


def synthesize_effect(
    arguments: AddRemoveAtprotoTagArguments, effect_kind: AtprotoEffectKind.ValueType
) -> AtprotoTagEffect:
    return AtprotoTagEffect(
        effect_kind=effect_kind,
        entity=arguments.entity,
        tag=arguments.tag,
        comment=arguments.comment,
    )


class AddAtprotoTag(UDFBase[AddRemoveAtprotoTagArguments, AtprotoTagEffect]):
    category = UdfCategories.ENGINE

    def execute(self, execution_context: ExecutionContext, arguments: AddRemoveAtprotoTagArguments) -> AtprotoTagEffect:
        return synthesize_effect(arguments, ATPROTO_EFFECT_KIND_ADD)


class RemoveAtprotoTag(UDFBase[AddRemoveAtprotoTagArguments, AtprotoTagEffect]):
    category = UdfCategories.ENGINE

    def execute(self, execution_context: ExecutionContext, arguments: AddRemoveAtprotoTagArguments) -> AtprotoTagEffect:
        return synthesize_effect(arguments, effect_kind=ATPROTO_EFFECT_KIND_REMOVE)
