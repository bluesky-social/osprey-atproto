from typing import Sequence

from osprey.worker.adaptor.plugin_manager import hookimpl_osprey
from osprey.worker.lib.config import Config
from osprey.worker.lib.osprey_shared.logging import DynamicLogSampler
from osprey.worker.sinks.sink.output_sink import BaseOutputSink, StdoutOutputSink
from output_sinks.kafka_output_sink import KafkaOutputSink
from udfs.atproto.atproto import (
    GetDIDCreatedAt,
    GetHandle,
    GetPDSService,
    GetRecordCID,
    GetRecordURI,
)
from udfs.atproto.atproto_acknowledge import AtprotoAcknowledge
from udfs.atproto.atproto_comment import AtprotoComment
from udfs.atproto.atproto_email import AtprotoSendEmail
from udfs.atproto.atproto_escalate import AtprotoEscalate
from udfs.atproto.atproto_label import AddAtprotoLabel, RemoveAtprotoLabel
from udfs.atproto.atproto_report import AtprotoReport
from udfs.atproto.atproto_tag import AddAtprotoTag, RemoveAtprotoTag
from udfs.atproto.atproto_takedown import AddAtprotoTakedown, RemoveAtprotoTakedown
from udfs.atproto.std.did_from_uri import DidFromUri
from udfs.std.cache import (
    CacheGetFloat,
    CacheGetInt,
    CacheGetStr,
    CacheSetFloat,
    CacheSetInt,
    CacheSetStr,
    GetWindowCount,
    IncrementWindow,
    cache_client,
)
from udfs.std.censorize import CleanString
from udfs.std.concat import ConcatStringLists
from udfs.std.list import CensorizedListMatch, ListContains, RegexListMatch, SimpleListContains
from udfs.std.log import Log
from udfs.std.text import ExtractDomains, ExtractEmoji, ForceString, TextContains
from udfs.std.timestamp_age import TimestampAge
from udfs.std.tokenize import Tokenize


@hookimpl_osprey
def register_udfs():
    return [
        #
        # Std
        #
        TextContains,
        Tokenize,
        CleanString,
        ExtractDomains,
        ExtractEmoji,
        ListContains,
        SimpleListContains,
        RegexListMatch,
        CensorizedListMatch,
        ForceString,
        CacheGetStr,
        CacheGetInt,
        CacheGetFloat,
        CacheSetStr,
        CacheSetInt,
        CacheSetFloat,
        IncrementWindow,
        GetWindowCount,
        TimestampAge,
        ConcatStringLists,
        Log,
        #
        # Atproto std
        #
        DidFromUri,
        GetRecordURI,
        GetRecordCID,
        GetDIDCreatedAt,
        GetPDSService,
        GetHandle,
        #
        # Atproto effects
        #
        AtprotoAcknowledge,
        AtprotoComment,
        AtprotoSendEmail,
        AtprotoEscalate,
        AtprotoReport,
        AddAtprotoLabel,
        RemoveAtprotoLabel,
        AddAtprotoTag,
        RemoveAtprotoTag,
        AddAtprotoTakedown,
        RemoveAtprotoTakedown,
    ]


@hookimpl_osprey
def register_output_sinks(config: Config) -> Sequence[BaseOutputSink]:
    bootstrap_servers = config.expect_str_list('OSPREY_KAFKA_OUTPUT_BOOTSTRAP_SERVERS')
    output_topic = config.expect_str('OSPREY_KAFKA_OUTPUT_TOPIC')
    output_client_id = config.get_optional_str('OSPREY_KAFKA_OUTPUT_CLIENT_ID')

    cache_client.initialize(config)

    return [
        StdoutOutputSink(log_sampler=DynamicLogSampler(20, 60)),
        KafkaOutputSink(
            bootstrap_servers=bootstrap_servers,
            output_topic=output_topic,
            client_id=output_client_id,
        ),
    ]
