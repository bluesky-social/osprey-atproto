from ddtrace.internal.logger import get_logger
from prometheus_client import Counter, start_http_server

NAMESPACE = 'osprey_worker'

logger = get_logger('prometheus_metrics')


class WorkerMetrics:
    """Singleton class to manage the various Osprey worker Prometheus metrics."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self.events_received = Counter(
            name='events_received', namespace=NAMESPACE, documentation='Number of events received by input stream'
        )

        self.events_processed = Counter(
            name='events_processed',
            namespace=NAMESPACE,
            labelnames=['status', 'action_type'],
            documentation='Number of events processed by output sink',
        )

        self.producer_produced = Counter(
            name='producer_produced',
            namespace=NAMESPACE,
            labelnames=['status'],
            documentation='Number of events produced by Kafka output sink',
        )

        self.producer_acks = Counter(
            name='producer_acks',
            namespace=NAMESPACE,
            labelnames=['status'],
            documentation="Number of produced events that have been ack'd by status",
        )

        self.counter_gets = Counter(
            name='counter_gets',
            namespace=NAMESPACE,
            documentation='Number of gets (total, so count of getmanys) that are sent to memcached',
        )

        self.counter_increments = Counter(
            name='counter_increments',
            namespace=NAMESPACE,
            labelnames=['status'],
            documentation='Number of increments to counters',
        )

        self._initialized = True

    def start_http(self, port: int, addr: str = '127.0.0.1'):
        logger.info(f'Starting Proemtheus client on {addr}:{port}')
        start_http_server(port=port, addr=addr)
        logger.info(f'Prometheus client running on {addr}:{port}')


worker_metrics = WorkerMetrics()
