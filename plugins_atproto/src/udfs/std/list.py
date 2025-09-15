import os
import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, cast

import yaml
from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import UDFBase
from osprey.worker.lib.osprey_shared.logging import get_logger

logger = get_logger('list')

RULES_PATH = os.getenv('OSPREY_RULES_PATH', 'example_rules/')


class ListCache:
    def __init__(self) -> None:
        self._cache: Dict[str, Set[str]] = {}
        self._regex_cache: Dict[str, List[re.Pattern[str]]] = {}

    def get_list(self, list_name: str, case_sensitive: bool) -> Set[str]:
        try:
            file_path = f'{RULES_PATH}lists/{list_name}.yaml'

            if case_sensitive:
                list_name = list_name + '-case-sensitive'

            if list_name not in self._cache:
                with open(file_path, 'r') as f:
                    data = yaml.safe_load(f)

                processed_set: Set[str] = set()

                if isinstance(data, list):
                    items = cast(List[str], data)
                    for item in items:
                        if not case_sensitive:
                            processed_set.add(item.lower())
                        else:
                            processed_set.add(item)

                self._cache[list_name] = processed_set

                logger.info(f'Loaded {list_name} from disk. Values are {list(processed_set)}')

            return self._cache[list_name]
        except Exception as e:
            logger.error(f'Error loading disk cache: {e}')
            return set()

    def get_simple_list(self, cache_name: str, list: List[str], case_sensitive: bool) -> Set[str]:
        cache_name = f'{cache_name}-simple'
        if case_sensitive:
            cache_name = f'{cache_name}-case-sensitive'

        if cache_name not in self._cache:
            processed_set: Set[str] = set()

            for item in list:
                if not case_sensitive:
                    processed_set.add(item.lower())
                else:
                    processed_set.add(item)

            self._cache[cache_name] = processed_set

            logger.info(f'Created cache list {cache_name}')

        return self._cache[cache_name]

    def get_regex_list(self, list_name: str, case_sensitive: bool) -> List[re.Pattern[str]]:
        file_path = f'{RULES_PATH}lists/{list_name}.yaml'
        cache_key = list_name if case_sensitive else list_name + '-case-insensitive'

        if cache_key not in self._regex_cache:
            with open(file_path, 'r') as f:
                data = yaml.safe_load(f)

            compiled_patterns: List[re.Pattern[str]] = []
            if isinstance(data, list):
                items = cast(List[str], data)
                for item in items:
                    try:
                        item = item.strip()
                        flags = 0 if case_sensitive else re.IGNORECASE
                        pattern = re.compile(item, flags)
                        compiled_patterns.append(pattern)
                    except re.error as e:
                        logger.warning(f'Invalid regex pattern "{item}": {e}')

            self._regex_cache[cache_key] = compiled_patterns
            logger.info(
                f'Loaded and compiled {len(compiled_patterns)} regex patterns from {list_name}. {compiled_patterns}'
            )

        return self._regex_cache[cache_key]


list_cache = ListCache()


class ListContainsArguments(ArgumentsBase):
    list: str
    phrases: List[Optional[str]]
    case_sensitive = False
    word_boundaries = True


class SimpleListContainsArguments(ArgumentsBase):
    cache_name: str
    list: List[str]
    phrases: List[Optional[str]]
    case_sensitive = False
    word_boundaries = True


@dataclass
class ListContainsResult:
    contains: bool
    word: Optional[str]


class ListContains(UDFBase[ListContainsArguments, Optional[str]]):
    def execute(self, execution_context: ExecutionContext, arguments: ListContainsArguments) -> Optional[str]:
        list_items = list_cache.get_list(arguments.list, case_sensitive=arguments.case_sensitive)

        for phrase in arguments.phrases:
            if phrase is None:
                continue

            phrase = phrase if arguments.case_sensitive else phrase.lower()

            if arguments.word_boundaries:
                for word in list_items:
                    escaped_word = re.escape(word)
                    flags = 0 if arguments.case_sensitive else re.IGNORECASE
                    if re.search(r'\b' + escaped_word + r'\b', phrase, flags):
                        return word
            else:
                for word in list_items:
                    if word in phrase:
                        return word

        return None


class SimpleListContains(UDFBase[SimpleListContainsArguments, Optional[str]]):
    def execute(self, execution_context: ExecutionContext, arguments: SimpleListContainsArguments) -> Optional[str]:
        list_items = list_cache.get_simple_list(arguments.cache_name, arguments.list, arguments.case_sensitive)

        for phrase in arguments.phrases:
            if phrase is None:
                continue

            phrase = phrase if arguments.case_sensitive else phrase
            if arguments.word_boundaries:
                for word in list_items:
                    escaped_word = re.escape(word)
                    flags = 0 if arguments.case_sensitive else re.IGNORECASE
                    if re.search(r'\b' + escaped_word + r'\b', phrase, flags):
                        return word
            else:
                for word in list_items:
                    if word in phrase:
                        return word

        return None


class RegexListMatchArguments(ArgumentsBase):
    list: str
    phrases: List[str]
    case_sensitive = False


class RegexListMatch(UDFBase[RegexListMatchArguments, Optional[str]]):
    def execute(self, execution_context: ExecutionContext, arguments: RegexListMatchArguments) -> Optional[str]:
        patterns = list_cache.get_regex_list(arguments.list, case_sensitive=arguments.case_sensitive)

        for phrase in arguments.phrases:
            for pattern in patterns:
                if pattern.search(phrase):
                    return pattern.pattern

        return None
