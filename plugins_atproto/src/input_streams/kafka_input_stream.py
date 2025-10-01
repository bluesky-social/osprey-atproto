import base64
import json
import platform
from dataclasses import dataclass, field
from time import time
from typing import Any, Dict, Iterator, List, Optional, Union, cast

from kafka.consumer.fetcher import ConsumerRecord
from osprey.engine.executor.execution_context import Action
from osprey.worker.lib.config import Config
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.sinks.sink.input_stream import BaseInputStream
from osprey.worker.sinks.utils.acking_contexts import (
    BaseAckingContext,
    NoopAckingContext,
)
from osprey.worker.sinks.utils.kafka import PatchedKafkaConsumer
from pymilvus import CollectionSchema, DataType, FieldSchema, MilvusClient
from pymilvus.milvus_client import IndexParams
from rpc.osprey_atproto_pb2 import OspreyInputEvent
from shared.metrics import worker_metrics

logger = get_logger('kafka-input-stream')
spam_logger = get_logger('spam')

DEFAULT_KAFKA_INPUT_SINK_CLIENT_ID = 'osprey-input-stream'


@dataclass
class ImageVectorRecord:
    hex: str
    did: str
    cid: str
    collection: str
    rkey: str
    timestamp: int = field(default_factory=lambda: int(time()))


class Milvus:
    def __init__(self):
        self.is_active = False
        self.collection_name = ''
        self.client: Optional[MilvusClient] = None
        self.logger = get_logger('milvus-client')

    def prepare(self, host: str, collection_name: str):
        """
        Prepare the milvus client
        """

        self.logger.info(f'Creating milvus client for {host}...')
        self.client = MilvusClient(host)
        self.collection_name = collection_name

        try:
            if not self.client.has_collection(collection_name):
                id_field = FieldSchema(name='id', dtype=DataType.INT64, is_primary=True, auto_id=True)
                did_field = FieldSchema(name='did', dtype=DataType.VARCHAR, max_length=100, nullable=False)
                cid_field = FieldSchema(name='cid', dtype=DataType.VARCHAR, max_length=100, nullable=False)
                collection_field = FieldSchema(
                    name='collection', dtype=DataType.VARCHAR, max_length=100, nullable=False
                )
                rkey_field = FieldSchema(name='rkey', dtype=DataType.VARCHAR, max_length=100, nullable=False)
                timestamp_field = FieldSchema(name='timestamp', dtype=DataType.INT64, nullable=False)
                binary_field = FieldSchema(name='vector', dtype=DataType.BINARY_VECTOR, dim=256)
                schema = CollectionSchema(
                    fields=[
                        id_field,
                        did_field,
                        cid_field,
                        collection_field,
                        rkey_field,
                        timestamp_field,
                        binary_field,
                    ],
                    description='Collection with binary vectors',
                )

                index_params = IndexParams()
                index_params.add_index(
                    field_name='vector',
                    index_type='BIN_IVF_FLAT',
                    index_name='vector_index',
                    metric_type='HAMMING',
                    params={
                        'nlist': 128,
                    },
                )
                index_params.add_index(field_name='timestamp', index_type='STL_SORT')

                self.client.create_collection(collection_name=collection_name, schema=schema, index_params=index_params)
                print(f"Collection '{collection_name}' created successfully")

            self.is_active = True
        except Exception as e:
            self.logger.error(f'Failed to ensure dataset was created: {e}')

    def insert(self, items: Union[ImageVectorRecord, List[ImageVectorRecord]]):
        """
        Insert one or more hashes into the dataset
        """

        start_time = time()

        if not self.is_active:
            self.logger.warning('Attempted to insert into Milvus with non-active client')
            return

        if isinstance(items, ImageVectorRecord):
            items = [items]

        data: List[Dict[str, Any]] = []

        try:
            for item in items:
                vector = self._hexToBinary(item.hex)
                data.append(
                    {
                        'vector': vector,
                        'did': item.did,
                        'cid': item.cid,
                        'collection': item.collection,
                        'rkey': item.rkey,
                        'timestamp': item.timestamp,
                    }
                )

            self.client.insert(collection_name=self.collection_name, data=data)

            duration = time() - start_time
            worker_metrics.image_vector_insert_duration.labels(status='ok').observe(duration)
            worker_metrics.image_vector_inserts.labels(status='ok').inc(amount=len(data))

            self.logger.info(f'Inserted {len(data)} vector(s) into database')

        except Exception as e:
            duration = time() - start_time
            worker_metrics.image_vector_insert_duration.labels(status='error').observe(duration)
            worker_metrics.image_vector_inserts.labels(status='error').inc(amount=len(data))

            self.logger.error(f'Error inserting data into milvus: {e}, data count: {len(data)}')
            raise

    def _hexToBinary(self, hex: str) -> bytes:
        return bytes.fromhex(hex)


milvus = Milvus()


class KafkaInputStream(BaseInputStream[BaseAckingContext[Action]]):
    """An input stream that consumes messages from a Kafka topic and yields Action objects wrapped in an AckingContext."""

    def __init__(self, config: Config):
        super().__init__()

        milvus_host = config.get_optional_str('OSPREY_MILVUS_HOST')
        milvus_collection = config.get_optional_str('OSPREY_MILVUS_COLLECTION')

        if milvus_host and milvus_collection:
            logger.info('Preparing milvus...')

            milvus.prepare(host=milvus_host, collection_name=milvus_collection)
        else:
            logger.warning(
                'No milvus host or collection provided. Tasks that require Milvus hash database will not be functional.'
            )

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
                client_id = f'{DEFAULT_KAFKA_INPUT_SINK_CLIENT_ID};host_override={input_bootstrap_servers[0]}'

        logger.info(f'Creating Kafka consumer with client id {client_id}...')

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
            start_time = time()
            action_name = 'unk'

            try:
                record: ConsumerRecord = next(self._consumer)

                worker_metrics.events_received.inc()

                osprey_input_event = OspreyInputEvent.FromString(record.value)

                event_data = osprey_input_event.data
                timestamp = osprey_input_event.send_time.ToDatetime()

                action_name = event_data.action_name

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

                if 'image_results' in parsed_data and parsed_data['image_results'] is not None:
                    image_vector_records: List[ImageVectorRecord] = []
                    for cid in parsed_data['image_results']:
                        if (
                            'retina_hash' in parsed_data['image_results'][cid]
                            and parsed_data['image_results'][cid] is not None
                        ):
                            retina_hash = parsed_data['image_results'][cid]['retina_hash']
                            if not retina_hash['quality_too_low']:
                                image_vector_records.append(
                                    ImageVectorRecord(
                                        hex=cast(str, retina_hash['hash']),
                                        did=cast(str, parsed_data['did']),
                                        cid=cast(str, cid),
                                        collection=cast(str, parsed_data['collection']),
                                        rkey=cast(str, parsed_data['rkey']),
                                    )
                                )

                    if len(image_vector_records) > 0:
                        try:
                            milvus.insert(image_vector_records)
                        except Exception as e:
                            logger.error(f'Error inserting into milvus: {e}')

                action = Action(
                    action_id=event_data.action_id,
                    action_name=event_data.action_name,
                    data=parsed_data,
                    secret_data=secret_data,
                    timestamp=timestamp,
                )

                duration = time() - start_time
                worker_metrics.events_processed_duration.labels(status='ok', action_type=action_name).observe(duration)
                worker_metrics.events_processed.labels(status='ok', action_type=action_name).inc()

                # Wrap in NoopAckingContext for now, or implement a KafkaAckingContext if needed
                yield NoopAckingContext(action)
            except Exception as e:
                duration = time() - start_time
                worker_metrics.events_processed_duration.labels(status='error', action_type=action_name).observe(
                    duration
                )
                worker_metrics.events_processed.labels(status='error', action_type='unk').inc()

                logger.exception(f'Error while consuming from Kafka: {e}')
                continue
