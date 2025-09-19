import time
from typing import Any, List, Optional, Tuple

from ddtrace.internal.logger import get_logger
from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import UDFBase
from osprey.worker.lib.config import Config
from pymemcache import HashClient
from shared.metrics import worker_metrics

logger = get_logger('cache_client')

SECOND = 1
MINUTE = SECOND * 60
FIVE_MINUTE = MINUTE * 5
TEN_MINUTE = MINUTE * 5
THIRTY_MINUTE = MINUTE * 30
HOUR = MINUTE * 60
DAY = HOUR * 24
WEEK = DAY * 7


class CacheClient:
    client: HashClient

    def __init__(self) -> None:
        self.initiazed = False

    def initialize(self, config: Config):
        servers: List[Tuple[str, int]] = []
        cfg_servers = config.get_str_list('OSPREY_MEMCACHED_SERVERS', [])

        for server_str in cfg_servers:
            pts = server_str.split(':')

            if len(pts) != 2:
                logger.error(f'Invalid server {server_str} included in OSPREY_MEMCACHED_SERVERS')
                continue

            servers.append((pts[0], int(pts[1])))

        self.client = HashClient(servers=servers)
        self.initialized = True

    def set(self, key: str, val: Any, ttl_seconds: float = 0):
        """
        Sets a value inside of the cache. By default, the TTL on an item is _indefinite_ (you generally should set a TTL).
        """
        if self.initialized is False:
            return

        try:
            self.client.set(
                key,
                val,
                expire=ttl_seconds,
            )
        except Exception as e:
            logger.error(f'Error value in cache: {e}')

    def get_str(self, key: str, default: str = ''):
        """
        Returns string value. Default value of empty string will be returned if it is not found and no default is specified.
        """
        if self.initialized is False:
            return default

        try:
            return str(
                self.client.get(
                    key,
                    default,
                )
            )
        except Exception as e:
            logger.error(f'Error getting string value in cache: {e}')
            return default

    def get_int(self, key: str, default: int = 0):
        """
        Returns int value. Default value of 0 will be returned if it is not found and no default is specified.
        """
        if self.initialized is False:
            return default

        try:
            return int(
                self.client.get(
                    key,
                    default,
                )
            )
        except Exception as e:
            logger.error(f'Error getting int value in cache: {e}')
            return default

    def get_float(self, key: str, default: float = 0.0):
        """
        Returns float value. Default value of 0.0 will be returned if it is not found and no default is specified.
        """
        if self.initialized is False:
            return default

        try:
            return float(
                self.client.get(
                    key,
                    default,
                )
            )
        except Exception as e:
            logger.error(f'Error getting float value in cache: {e}')
            return default

    def increment_window(
        self,
        key: str,
        window_seconds: float,
        max_ttl_seconds: Optional[float] = None,
        max_events_cap: int = 10_000,  # kept for interface compatibility
    ) -> int:
        """
        Increment a sliding-window counter using bucketed approach.
        Returns the count within the current window on success, else 0.
        """
        if not getattr(self, 'initialized', True):
            worker_metrics.counter_increments.labels(status='exit_early').inc()
            return 0

        if window_seconds <= 300:
            bucket_size = 1
        elif window_seconds <= 3600:
            bucket_size = 10
        elif window_seconds <= 86400:
            bucket_size = 60
        else:
            bucket_size = 600

        current_time = time.time()
        current_bucket = int(current_time / bucket_size)

        bucket_key = f'{key}:w{window_seconds}:b{current_bucket}'

        if max_ttl_seconds is None:
            max_ttl_seconds = window_seconds * 2
        expire = max(window_seconds + bucket_size, min(max_ttl_seconds, window_seconds * 2))

        try:
            count = self.client.incr(bucket_key)
            if count == 1:
                try:
                    self.client.touch(bucket_key, expire=expire)
                except AttributeError:
                    self.client.set(bucket_key, count, expire=expire)
        except Exception:
            try:
                if not self.client.add(bucket_key, 1, expire=expire):
                    try:
                        count = self.client.incr(bucket_key)
                    except Exception:
                        count = 1
                else:
                    count = 1
            except Exception as e:
                logger.error(f'memcached increment failed for {bucket_key}: {e}')
                worker_metrics.counter_increments.labels(status='error').inc()
                return 0

        window_start = current_time - window_seconds
        start_bucket = int(window_start / bucket_size)

        total = 0
        bucket_keys: List[str] = []

        for bucket_id in range(start_bucket, current_bucket + 1):
            bucket_keys.append(f'{key}:w{window_seconds}:b{bucket_id}')

        if bucket_keys:
            try:
                bucket_values = self.client.get_multi(bucket_keys)
                for bkey in bucket_keys:
                    if bkey in bucket_values:
                        try:
                            val = bucket_values[bkey]
                            if isinstance(val, bytes):
                                val = val.decode('utf-8')
                            total += int(val)
                        except (ValueError, TypeError):
                            pass
                worker_metrics.counter_gets.inc(amount=len(bucket_keys))
            except Exception as e:
                logger.error(f'memcached get_multi failed: {e}')
                for bkey in bucket_keys:
                    try:
                        val = self.client.get(bkey)
                        if val:
                            if isinstance(val, bytes):
                                val = val.decode('utf-8')
                            total += int(val)
                    except Exception:
                        pass
                worker_metrics.counter_gets.inc(amount=len(bucket_keys))

        worker_metrics.counter_increments.labels(status='ok').inc()
        return total

    def get_window_count(self, key: str, window_seconds: float) -> int:
        """
        Get the current count within the sliding window without incrementing.
        Returns the count within the current window on success, else 0.
        """
        if not getattr(self, 'initialized', True):
            return 0

        if window_seconds <= 300:
            bucket_size = 1
        elif window_seconds <= 3600:
            bucket_size = 10
        elif window_seconds <= 86400:
            bucket_size = 60
        else:
            bucket_size = 600

        current_time = time.time()
        current_bucket = int(current_time / bucket_size)
        window_start = current_time - window_seconds
        start_bucket = int(window_start / bucket_size)

        total = 0
        bucket_keys: List[str] = []

        for bucket_id in range(start_bucket, current_bucket + 1):
            bucket_keys.append(f'{key}:w{window_seconds}:b{bucket_id}')

        if bucket_keys:
            try:
                bucket_values = self.client.get_multi(bucket_keys)
                for bkey in bucket_keys:
                    if bkey in bucket_values:
                        try:
                            val = bucket_values[bkey]
                            if isinstance(val, bytes):
                                val = val.decode('utf-8')
                            total += int(val)
                        except (ValueError, TypeError):
                            pass

                worker_metrics.counter_gets.inc(amount=len(bucket_keys))
            except Exception as e:
                logger.error(f'memcached get_multi failed: {e}')
                for bkey in bucket_keys:
                    try:
                        val = self.client.get(bkey)
                        if val:
                            if isinstance(val, bytes):
                                val = val.decode('utf-8')
                            total += int(val)
                    except Exception:
                        pass

                worker_metrics.counter_gets.inc(amount=len(bucket_keys))

        return total


cache_client = CacheClient()


class CacheArgumentsBase(ArgumentsBase):
    key: str


class CacheWindowArgumentsBase(CacheArgumentsBase):
    window_seconds: float
    when_all: List[bool]


class IncrementWindowArguments(CacheWindowArgumentsBase):
    max_ttl_seconds: Optional[float] = None


class CacheSetStrArguments(CacheArgumentsBase):
    value: str
    when_all: List[bool]
    ttl_seconds: float = DAY


class CacheSetIntArguments(CacheArgumentsBase):
    value: int
    when_all: List[bool]
    ttl_seconds: float = DAY


class CacheSetFloatArguments(CacheArgumentsBase):
    value: float
    when_all: List[bool]
    ttl_seconds: float = DAY


class CacheGetStrArguments(CacheArgumentsBase):
    when_all: List[bool]
    default: str = ''


class CacheGetIntArguments(CacheArgumentsBase):
    when_all: List[bool]
    default: int = 0


class CacheGetFloatArguments(CacheArgumentsBase):
    when_all: List[bool]
    default: float = 0.0


class CacheSetStr(UDFBase[CacheSetStrArguments, None]):
    def execute(self, execution_context: ExecutionContext, arguments: CacheSetStrArguments):
        if all(arguments.when_all) is not True:
            return

        cache_client.set(arguments.key, arguments.value, arguments.ttl_seconds)


class CacheSetInt(UDFBase[CacheSetIntArguments, None]):
    def execute(self, execution_context: ExecutionContext, arguments: CacheSetIntArguments):
        if all(arguments.when_all) is not True:
            return

        cache_client.set(arguments.key, arguments.value, arguments.ttl_seconds)


class CacheSetFloat(UDFBase[CacheSetFloatArguments, None]):
    def execute(self, execution_context: ExecutionContext, arguments: CacheSetFloatArguments):
        if all(arguments.when_all) is not True:
            return

        cache_client.set(arguments.key, arguments.value, arguments.ttl_seconds)


class CacheGetStr(UDFBase[CacheGetStrArguments, str]):
    def execute(self, execution_context: ExecutionContext, arguments: CacheGetStrArguments) -> str:
        if all(arguments.when_all) is not True:
            return arguments.default

        return cache_client.get_str(arguments.key, arguments.default)


class CacheGetInt(UDFBase[CacheGetIntArguments, int]):
    def execute(self, execution_context: ExecutionContext, arguments: CacheGetIntArguments) -> int:
        if all(arguments.when_all) is not True:
            return arguments.default

        return cache_client.get_int(arguments.key, arguments.default)


class CacheGetFloat(UDFBase[CacheGetFloatArguments, float]):
    def execute(self, execution_context: ExecutionContext, arguments: CacheGetFloatArguments) -> float:
        if all(arguments.when_all) is not True:
            return arguments.default

        return cache_client.get_float(arguments.key, arguments.default)


class IncrementWindow(UDFBase[IncrementWindowArguments, int]):
    def execute(self, execution_context: ExecutionContext, arguments: IncrementWindowArguments) -> int:
        if all(arguments.when_all) is False:
            return 0

        return cache_client.increment_window(arguments.key, arguments.window_seconds, arguments.max_ttl_seconds)


class GetWindowCount(UDFBase[CacheWindowArgumentsBase, int]):
    def execute(self, execution_context: ExecutionContext, arguments: CacheWindowArgumentsBase) -> int:
        if all(arguments.when_all) is False:
            return 0

        return cache_client.get_window_count(arguments.key, arguments.window_seconds)
