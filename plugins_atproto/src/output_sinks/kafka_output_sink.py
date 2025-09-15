import json
import platform
from datetime import datetime
from typing import Any, List, Optional

from confluent_kafka import KafkaError, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from google.protobuf.timestamp_pb2 import Timestamp
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.lib.osprey_shared.metrics import worker_metrics
from osprey.worker.sinks.sink.output_sink import BaseOutputSink, DynamicLogSampler
from rpc.osprey_atproto_pb2 import (
    AtprotoAcknowledgeEffect as OutputAcknowledgeEffect,
)
from rpc.osprey_atproto_pb2 import (
    AtprotoCommentEffect as OutputCommentEffect,
)
from rpc.osprey_atproto_pb2 import (
    AtprotoEmailEffect as OutputEmailEffect,
)
from rpc.osprey_atproto_pb2 import (
    AtprotoEscalateEffect as OutputEscalateEffect,
)
from rpc.osprey_atproto_pb2 import (
    AtprotoLabelEffect as OutputLabelEffect,
)
from rpc.osprey_atproto_pb2 import (
    AtprotoReportEffect as OutputReportEffect,
)
from rpc.osprey_atproto_pb2 import (
    AtprotoTagEffect as OutputTagEffect,
)
from rpc.osprey_atproto_pb2 import (
    AtprotoTakedownEffect as OutputTakedownEffect,
)
from rpc.osprey_atproto_pb2 import (
    ResultEvent,
)
from udfs.atproto.atproto import GetRecordCIDFromData, GetRecordURIFromData
from udfs.atproto.atproto_acknowledge import AtprotoAcknowledgeEffect
from udfs.atproto.atproto_comment import AtprotoCommentEffect
from udfs.atproto.atproto_email import AtprotoEmailEffect
from udfs.atproto.atproto_escalate import AtprotoEscalateEffect
from udfs.atproto.atproto_label import AtprotoLabelEffect, EntityToSubjectKind
from udfs.atproto.atproto_report import AtprotoReportEffect
from udfs.atproto.atproto_tag import AtprotoTagEffect
from udfs.atproto.atproto_takedown import AtprotoTakedownEffect

DEFAULT_KAFKA_OUTPUT_SINK_CLIENT_ID = 'osprey-output-sink'

logger = get_logger('kafka-output-sink', dynamic_log_sampler=DynamicLogSampler(25, 100))


def acked(err: Any, msg: Any):
    if err is not None:
        if isinstance(err, KafkaError):
            code = ''
            if hasattr(err, 'code'):
                code = err.code

            if code == 'MSG_SIZE_TOO_LARGE':
                logger.error(f'Failed to deliver message for being too chonky. {msg}')
                worker_metrics.producer_acks.labels(status='error_too_large').inc()
            else:
                logger.error(f'Failed to deliver message: {msg.topic()}/{msg.partition()}/{msg.offset()}: {err}')
                worker_metrics.producer_acks.labels(status='error').inc()
        else:
            logger.error(f'Failed to deliver message for unknown reason, not a KafkaError: {err}')
            worker_metrics.producer_acks.labels(status='unknown_error').inc()
    else:
        logger.info(f'Message produced: {msg.topic()}/{msg.partition()}/{msg.offset()}')
        worker_metrics.producer_acks.labels(status='ok').inc()


class EmptyBootstrapServersException(Exception):
    """Exception raised when bootstrap server list provided to `KafkaOutputSink` is empty."""


class InvalidOutputTopicException(Exception):
    """Exception raised when output topic provided to `KafkaOutputSink` is invalid."""


class KafkaOutputSink(BaseOutputSink):
    """An output sink that produces results to a Kafka topic."""

    def __init__(
        self,
        bootstrap_servers: List[str],
        output_topic: str,
        client_id: Optional[str],
        poll_every: int = 20,
        max_retries: int = 5,
    ):
        if len(bootstrap_servers) == 0:
            raise EmptyBootstrapServersException()

        if output_topic == '':
            raise InvalidOutputTopicException()

        self.logger = get_logger('KafkaOutputSink')

        self.bootstrap_servers = bootstrap_servers
        self.output_topic = output_topic
        self.poll_every = poll_every
        self.max_retries = max_retries
        self.message_count = 0

        self.topic_ensured = False

        if client_id is None:
            client_hostname = platform.node()
            if client_hostname != '':
                client_id = f'{client_hostname};host_override={bootstrap_servers[0]}'
            else:
                client_id = f'{DEFAULT_KAFKA_OUTPUT_SINK_CLIENT_ID};host_override={bootstrap_servers[0]}'

        logger.info(f'Creating Kafka producer with client id {client_id}')

        conf = {
            'bootstrap.servers': bootstrap_servers[0],
            'client.id': client_id,
            'queue.buffering.max.messages': 1_000_000,
            'linger.ms': 10,
            'retries': 10,
            'request.timeout.ms': 30000,
            'socket.timeout.ms': 30000,
            'delivery.timeout.ms': 120000,
            'statistics.interval.ms': 10000,
            'log.connection.close': False,
            'enable.idempotence': True,
            'acks': 'all',
            'max.in.flight.requests.per.connection': 5,
            'message.max.bytes': 20_000_000,
        }

        self.ensure_topic()

        self.producer = Producer(conf)
        logger.info('Kafka producer created')

    def ensure_topic(self):
        """Create topic if it doesn't exist"""
        admin = AdminClient({'bootstrap.servers': self.bootstrap_servers[0]})

        try:
            metadata = admin.list_topics(timeout=10)
            if self.output_topic not in metadata.topics:
                logger.info(f'Creating topic {self.output_topic}')
                topic = NewTopic(self.output_topic, num_partitions=3, replication_factor=3)
                fs = admin.create_topics([topic])
                fs[self.output_topic].result()
                self.topic_ensured = True
            else:
                self.topic_ensured = True
        except Exception as e:
            if 'already exists' not in str(e).lower():
                logger.info(f'Topic creation error: {e}')
            self.topic_ensured = True  # Don't retry if it exists

    def _handle_polling(self) -> None:
        """Poll periodically based on message count"""
        self.message_count += 1
        if self.message_count % self.poll_every == 0:
            self.producer.poll(0)

    def will_do_work(self, result: ExecutionResult) -> bool:
        return True

    def push(self, result: ExecutionResult) -> None:
        data = result.action.data
        did = data['did']

        labels: List[OutputLabelEffect] = []
        tags: List[OutputTagEffect] = []
        takedowns: List[OutputTakedownEffect] = []
        acknowledgements: List[OutputAcknowledgeEffect] = []
        escalations: List[OutputEscalateEffect] = []
        emails: List[OutputEmailEffect] = []
        comments: List[OutputCommentEffect] = []
        reports: List[OutputReportEffect] = []

        for effects in result.effects.values():
            for effect in effects:
                rule_names = [rule.name for rule in effect.rules]

                if isinstance(effect, AtprotoLabelEffect):
                    labels.append(
                        OutputLabelEffect(
                            effect_kind=effect.effect_kind,
                            subject_kind=EntityToSubjectKind(effect.entity),
                            label=effect.label,
                            comment=effect.comment,
                            email=effect.email,
                            expiration_in_hours=effect.expiration_in_hours,
                            rules=rule_names,
                        )
                    )
                elif isinstance(effect, AtprotoTagEffect):
                    tags.append(
                        OutputTagEffect(
                            effect_kind=effect.effect_kind,
                            subject_kind=EntityToSubjectKind(effect.entity),
                            tag=effect.tag,
                            comment=effect.comment,
                            rules=rule_names,
                        )
                    )
                elif isinstance(effect, AtprotoTakedownEffect):
                    takedowns.append(
                        OutputTakedownEffect(
                            effect_kind=effect.effect_kind,
                            subject_kind=EntityToSubjectKind(effect.entity),
                            comment=effect.comment,
                            email=effect.email,
                            rules=rule_names,
                        )
                    )
                elif isinstance(effect, AtprotoAcknowledgeEffect):
                    acknowledgements.append(
                        OutputAcknowledgeEffect(
                            subject_kind=EntityToSubjectKind(effect.entity),
                            comment=effect.comment,
                            rules=rule_names,
                        )
                    )
                elif isinstance(effect, AtprotoEscalateEffect):
                    escalations.append(
                        OutputEscalateEffect(
                            subject_kind=EntityToSubjectKind(effect.entity),
                            comment=effect.comment,
                            rules=rule_names,
                        )
                    )
                elif isinstance(effect, AtprotoEmailEffect):
                    emails.append(OutputEmailEffect(email=effect.email, comment=effect.comment, rules=rule_names))
                elif isinstance(effect, AtprotoCommentEffect):
                    comments.append(
                        OutputCommentEffect(
                            subject_kind=EntityToSubjectKind(effect.entity),
                            comment=effect.comment,
                            rules=rule_names,
                        )
                    )
                elif isinstance(effect, AtprotoReportEffect):
                    reports.append(
                        OutputReportEffect(
                            subject_kind=EntityToSubjectKind(effect.entity),
                            report_kind=effect.report_kind,
                            comment=effect.comment,
                            priority_score=effect.priority_score,
                            rules=rule_names,
                        )
                    )

        ts = Timestamp()
        ts.FromDatetime(datetime.now())

        datab = json.dumps(data).encode('utf-8')

        osprey_result = ResultEvent(
            send_time=ts,
            action_name=result.action.action_name,
            action_id=result.action.action_id,
            did=did,
            uri=GetRecordURIFromData(result.action.data),
            cid=GetRecordCIDFromData(result.action.data),
            data=datab,
            labels=labels,
            tags=tags,
            takedowns=takedowns,
            emails=emails,
            comments=comments,
            escalations=escalations,
            acknowledgements=acknowledgements,
            reports=reports,
        )

        for attempt in range(self.max_retries):
            try:
                self.producer.produce(
                    self.output_topic,
                    value=osprey_result.SerializeToString(),
                    key=did.encode(),
                    callback=acked,
                )
                self._handle_polling()
                worker_metrics.producer_produced.labels(status='ok').inc()
                return  # Success, exit the method
            except BufferError:
                if attempt == self.max_retries - 1:
                    logger.error(f'Failed to produce after {self.max_retries} retries')
                    worker_metrics.producer_produced.labels(status='error').inc()
                    raise BufferError(f'Failed to produce message after {self.max_retries} retries')
                self.producer.poll(1)
            except Exception as e:
                logger.error(f'Error producing message: {e}')
                worker_metrics.producer_produced.labels(status='error').inc()
                raise

    def flush(self, timeout: float = 30) -> int:
        """Flush any pending messages. Returns number of messages still in queue."""
        return self.producer.flush(timeout)

    def stop(self) -> None:
        """Stop the output sink and flush remaining messages"""
        remaining = self.flush(timeout=10)
        if remaining > 0:
            logger.warning(f'{remaining} messages were not delivered')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
