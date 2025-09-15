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
from rpc.osprey_atproto_pb2 import ATPROTO_EFFECT_KIND_ADD, AtprotoEffectKind

logger = get_logger('atproto_escalate')


class AtprotoEscalateArguments(ArgumentsBase):
    entity: str
    comment: Optional[str]


@dataclass
class AtprotoEscalateEffect(EffectToCustomExtractedFeatureBase[List[str]]):
    """Stores a label effect of a WhenRules(...) invocation, which stores the label mutations that should occur once
    a given action has finished classification."""

    entity: str
    """The entity that the effect will be applied on."""

    comment: Optional[str]
    """Comment to add."""

    def to_str(self) -> str:
        return f'{self.entity}|{self.comment}'

    @classmethod
    def build_custom_extracted_feature_from_list(cls, values: List[Self]) -> CustomExtractedFeature[List[str]]:
        return AtprotoEscalateEffectsExtractedFeature(effects=cast(List[AtprotoEscalateEffect], values))


@add_slots
@dataclass
class AtprotoEscalateEffectsExtractedFeature(CustomExtractedFeature[List[str]]):
    effects: List[AtprotoEscalateEffect]

    @classmethod
    def feature_name(cls) -> str:
        return 'atproto_tag'

    def get_serializable_feature(self) -> List[str] | None:
        return [effect.to_str() for effect in self.effects]


def synthesize_effect(
    arguments: AtprotoEscalateArguments, effect_kind: AtprotoEffectKind.ValueType
) -> AtprotoEscalateEffect:
    return AtprotoEscalateEffect(
        entity=arguments.entity,
        comment=arguments.comment,
    )


class AtprotoEscalate(UDFBase[AtprotoEscalateArguments, AtprotoEscalateEffect]):
    category = UdfCategories.ENGINE

    def execute(
        self, execution_context: ExecutionContext, arguments: AtprotoEscalateArguments
    ) -> AtprotoEscalateEffect:
        return synthesize_effect(arguments, ATPROTO_EFFECT_KIND_ADD)
