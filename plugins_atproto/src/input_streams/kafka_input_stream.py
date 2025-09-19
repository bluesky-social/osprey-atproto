import base64
import json
import platform
from typing import Dict, Iterator

import sentry_sdk
from kafka.consumer.fetcher import ConsumerRecord
from osprey.engine.executor.execution_context import Action
from osprey.worker.lib.config import Config
from osprey.worker.lib.instruments import metrics
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.sinks.sink.input_stream import BaseInputStream
from osprey.worker.sinks.utils.acking_contexts import (
    BaseAckingContext,
    NoopAckingContext,
)
from osprey.worker.sinks.utils.kafka import PatchedKafkaConsumer
from output_sinks.kafka_output_sink import DEFAULT_KAFKA_OUTPUT_SINK_CLIENT_ID
from rpc.osprey_atproto_pb2 import OspreyInputEvent
from shared.metrics import worker_metrics

logger = get_logger('kafka-input-stream')

DEFAULT_KAFKA_INPUT_SINK_CLIENT_ID = 'osprey-input-stream'


class KafkaInputStream(BaseInputStream[BaseAckingContext[Action]]):
    """An input stream that consumes messages from a Kafka topic and yields Action objects wrapped in an AckingContext."""

    def __init__(self, config: Config):
        super().__init__()
        client_id = config.get_optional_str('OSPREY_KAFKA_INPUT_STREAM_CLIENT_ID')
        input_topic: str = config.get_str('OSPREY_KAFKA_INPUT_STREAM_TOPIC', 'osprey.actions_input')
        input_bootstrap_servers: list[str] = config.get_str_list('OSPREY_KAFKA_BOOTSTRAP_SERVERS', ['localhost'])
        group_id = config.get_optional_str('OSPREY_KAFKA_GROUP_ID')

        # sasl_username = config.get_str('OSPREY_KAFKA_SASL_USERNAME', '')
        # sasl_password = config.get_str('OSPREY_KAFKA_SASL_PASSWORD', '')

        if client_id is None:
            client_hostname = platform.node()
            if client_hostname != '':
                client_id = f'{client_hostname};host_override={input_bootstrap_servers[0]}'
            else:
                client_id = f'{DEFAULT_KAFKA_OUTPUT_SINK_CLIENT_ID};host_override={input_bootstrap_servers[0]}'

        logger.info(f'Creating Kafka consumer with client id {client_id}')

        self._consumer = PatchedKafkaConsumer(
            input_topic,
            bootstrap_servers=input_bootstrap_servers,
            client_id=client_id,
            group_id=group_id,
            reconnect_backoff_ms=100,
            reconnect_backoff_max_ms=10000,
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
            # security_protocol='SASL_PLAINTEXT',
            # sasl_mechanism='PLAIN',
            # sasl_plain_username=sasl_username,
            # sasl_plain_password=sasl_password,
        )

    def _gen(self) -> Iterator[BaseAckingContext[Action]]:
        while True:
            try:
                with metrics.timed('kafka_consumer.lock_time'):
                    with metrics.timed('kafka_consumer.poll_time'):
                        record: ConsumerRecord = next(self._consumer)

                worker_metrics.events_received.inc()

                osprey_input_event = OspreyInputEvent.FromString(record.value)

                event_data = osprey_input_event.data
                timestamp = osprey_input_event.send_time.ToDatetime()

                secret_data = {k: v for k, v in event_data.secret_data.items()}

                parsed_data = json.loads(event_data.data) if event_data.data else {}

                if parsed_data['operation'] != 1 and parsed_data['operation'] != 2:
                    continue

                if 'profile_view' in parsed_data and parsed_data['profile_view'] is not None:
                    json_bytes = base64.b64decode(parsed_data['profile_view'])
                    parsed_data['profile_view'] = json.loads(json_bytes)

                if 'ozone_repo_view_detail' in parsed_data and parsed_data['ozone_repo_view_detail'] is not None:
                    json_bytes = base64.b64decode(parsed_data['ozone_repo_view_detail'])
                    parsed_data['ozone_repo_view_detail'] = json.loads(json_bytes)

                    if 'threatSignatures' in parsed_data['ozone_repo_view_detail']:
                        threat_signatures: Dict[str, str] = {}
                        for sig in parsed_data['ozone_repo_view_detail']['threatSignatures']:
                            # Hack to get attestion IP in for registration IP
                            if sig['property'] == 'attestation_ip':
                                threat_signatures['registrationIp'] = sig['value']

                            threat_signatures[sig['property']] = sig['value']
                        parsed_data['ozone_repo_view_detail']['threatSignatures'] = threat_signatures

                if 'did_doc' in parsed_data and parsed_data['did_doc'] is not None:
                    json_bytes = base64.b64decode(parsed_data['did_doc'])
                    parsed_data['did_doc'] = json.loads(json_bytes)

                if 'did_audit_log' in parsed_data and parsed_data['did_audit_log'] is not None:
                    json_bytes = base64.b64decode(parsed_data['did_audit_log'])
                    parsed_data['did_audit_log'] = json.loads(json_bytes)

                if 'record' in parsed_data and parsed_data['record'] is not None:
                    json_bytes = base64.b64decode(parsed_data['record'])
                    parsed_data['record'] = json.loads(json_bytes)

                action = Action(
                    action_id=event_data.action_id,
                    action_name=event_data.action_name,
                    data=parsed_data,
                    secret_data=secret_data,
                    timestamp=timestamp,
                )

                worker_metrics.events_processed.labels(status='ok', action_type=event_data.action_name).inc()

                # Wrap in NoopAckingContext for now, or implement a KafkaAckingContext if needed
                yield NoopAckingContext(action)
            except Exception as e:
                logger.exception(f'Error while consuming from Kafka: {e}')
                sentry_sdk.capture_exception(e)
                worker_metrics.events_processed.labels(status='error', action_type='unk').inc()
                continue
