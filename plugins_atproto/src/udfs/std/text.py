import re
from typing import List, Optional, Set
from urllib.parse import urlparse

from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import UDFBase
from osprey.worker.lib.osprey_shared.logging import get_logger

logger = get_logger('text')

URL_PATTERN = r"""
    (?:https?://)?                                # Optional http:// or https://
    (?:www\.)?                                    # Optional www.
    (?:                                           # Domain name patterns:
        [a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.  # Subdomain or domain
    )+
    [a-zA-Z]{2,}                                 # TLD
    (?:[:/][^\s]*)?                              # Optional path/query
"""

EMOJI_PATTERN = re.compile(
    '['
    '\U0001f600-\U0001f64f'  # emoticons
    '\U0001f300-\U0001f5ff'  # symbols & pictographs
    '\U0001f680-\U0001f6ff'  # transport & map symbols
    '\U0001f1e0-\U0001f1ff'  # flags (iOS)
    '\U00002702-\U000027b0'  # dingbats
    '\U000024c2-\U0001f251'  # enclosed characters
    '\U0001f900-\U0001f9ff'  # supplemental symbols
    '\U0001fa70-\U0001faff'  # symbols and pictographs extended-a
    ']+',
    flags=re.UNICODE,
)


class StringArgumentsBase(ArgumentsBase):
    s: str


class TextContainsArguments(ArgumentsBase):
    text: str
    phrase: str
    case_sensitive = False


class TextContains(UDFBase[TextContainsArguments, bool]):
    def execute(self, execution_context: ExecutionContext, arguments: TextContainsArguments) -> bool:
        escaped = re.escape(arguments.phrase)

        pattern = rf'\b{escaped}\b'

        flags = 0 if arguments.case_sensitive else re.IGNORECASE
        regex = re.compile(pattern, flags)

        return bool(regex.search(arguments.text))


class ForceStringArguments(ArgumentsBase):
    """Takes an optional string and returns a string. String value will be <None> if it was None."""

    s: Optional[str]


class ForceString(UDFBase[ForceStringArguments, str]):
    def execute(self, execution_context: ExecutionContext, arguments: ForceStringArguments) -> str:
        if arguments.s is None:
            return '<None>'
        return arguments.s


class ExtractDomains(UDFBase[StringArgumentsBase, List[str]]):
    def execute(self, execution_context: ExecutionContext, arguments: StringArgumentsBase) -> List[str]:
        potential_urls = re.findall(URL_PATTERN, arguments.s, re.VERBOSE | re.IGNORECASE)

        domains: Set[str] = set()

        for url in potential_urls:
            url = url.strip()

            if not url.startswith(('http://', 'https://')):
                url = 'http://' + url

            try:
                parsed = urlparse(url)
                domain = str(parsed.netloc)

                if domain.startswith('www.'):
                    domain = domain[4:]

                if domain:
                    domains.add(domain.lower())
            except Exception as e:
                logger.error(f'Error extracting domains from text: {e}')

        return list(domains)


class ExtractEmoji(UDFBase[StringArgumentsBase, List[str]]):
    def execute(self, execution_context: ExecutionContext, arguments: StringArgumentsBase) -> List[str]:
        return EMOJI_PATTERN.findall(arguments.s)
