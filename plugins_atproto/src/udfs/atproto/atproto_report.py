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
    ATPROTO_REPORT_KIND_MISLEADING,
    ATPROTO_REPORT_KIND_OTHER,
    ATPROTO_REPORT_KIND_RUDE,
    ATPROTO_REPORT_KIND_SEXUAL,
    ATPROTO_REPORT_KIND_SPAM,
    ATPROTO_REPORT_KIND_VIOLATION,
    AtprotoEffectKind,
    AtprotoReportKind,
)

logger = get_logger('atproto_report')


def StringToAtprotoReportKind(report_kind_str: str):
    if report_kind_str == 'spam':
        return ATPROTO_REPORT_KIND_SPAM
    elif report_kind_str == 'violation':
        return ATPROTO_REPORT_KIND_VIOLATION
    elif report_kind_str == 'misleading':
        return ATPROTO_REPORT_KIND_MISLEADING
    elif report_kind_str == 'sexual':
        return ATPROTO_REPORT_KIND_SEXUAL
    elif report_kind_str == 'rude':
        return ATPROTO_REPORT_KIND_RUDE
    elif report_kind_str == 'other':
        return ATPROTO_REPORT_KIND_OTHER
    else:
        return ATPROTO_REPORT_KIND_OTHER


class AtprotoReportArguments(ArgumentsBase):
    entity: str
    report_kind: str
    comment: str
    priority_score: Optional[int] = None


@dataclass
class AtprotoReportEffect(EffectToCustomExtractedFeatureBase[List[str]]):
    """Stores a label effect of a WhenRules(...) invocation, which stores the label mutations that should occur once
    a given action has finished classification."""

    entity: str
    """The entity that the effect will be applied on."""

    report_kind: AtprotoReportKind.ValueType
    """The type of report to create."""

    comment: str
    """The comment to add to the report."""

    priority_score: Optional[int]
    """The priority score to optionally add to the report."""

    def to_str(self) -> str:
        return f'{self.entity}|{self.comment}'

    @classmethod
    def build_custom_extracted_feature_from_list(cls, values: List[Self]) -> CustomExtractedFeature[List[str]]:
        return AtprotoReportEffectsExtractedFeature(effects=cast(List[AtprotoReportEffect], values))


@add_slots
@dataclass
class AtprotoReportEffectsExtractedFeature(CustomExtractedFeature[List[str]]):
    effects: List[AtprotoReportEffect]

    @classmethod
    def feature_name(cls) -> str:
        return 'atproto_tag'

    def get_serializable_feature(self) -> List[str] | None:
        return [effect.to_str() for effect in self.effects]


def synthesize_effect(
    arguments: AtprotoReportArguments, effect_kind: AtprotoEffectKind.ValueType
) -> AtprotoReportEffect:
    return AtprotoReportEffect(
        entity=arguments.entity,
        report_kind=StringToAtprotoReportKind(arguments.report_kind),
        comment=arguments.comment,
        priority_score=arguments.priority_score,
    )


class AtprotoReport(UDFBase[AtprotoReportArguments, AtprotoReportEffect]):
    category = UdfCategories.ENGINE

    def execute(self, execution_context: ExecutionContext, arguments: AtprotoReportArguments) -> AtprotoReportEffect:
        return synthesize_effect(arguments, ATPROTO_EFFECT_KIND_ADD)
