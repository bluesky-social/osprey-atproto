import re
import unicodedata
from typing import List, Optional

from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import UDFBase
from osprey.worker.lib.osprey_shared.logging import get_logger

logger = get_logger('tokenize')

punc_chars = re.compile(r'[^\w\s]+', re.UNICODE)
non_token_chars = re.compile(r'[^a-zA-Z0-9\s]+', re.UNICODE)
non_token_chars_skip_censor_chars = re.compile(r'[^a-zA-Z0-9\s#*_-]+', re.UNICODE)


def normalize_unicode(text: str) -> str:
    """
    Performs unicode normalization similar to Go's transform.Chain(norm.NFD, runes.Remove(runes.In(unicode.Mn)), norm.NFC)
    This removes combining marks and normalizes the text.
    """
    nfd = unicodedata.normalize('NFD', text)
    without_marks = ''.join(c for c in nfd if unicodedata.category(c) != 'Mn')
    return unicodedata.normalize('NFC', without_marks)


def tokenize_text_with_regex(text: str, non_token_chars_regex: re.Pattern[str]) -> List[str]:
    """
    Splits free-form text into tokens, including lower-case, unicode normalization, and some unicode folding.

    The intent is for this to work similarly to an NLP tokenizer, as might be used in a fulltext search engine,
    and enable fast matching to a list of known tokens.
    """
    split = non_token_chars_regex.sub(' ', text).lower()
    bare = non_token_chars_regex.sub('', split).lower()

    try:
        norm = normalize_unicode(bare)
    except Exception as e:
        logger.warning(f'unicode normalization error: {e}')
        norm = bare

    return norm.split()


def tokenize_text(text: str) -> List[str]:
    """
    Tokenizes text using the standard non-token characters regex.
    """
    return tokenize_text_with_regex(text, non_token_chars)


def tokenize_text_skipping_censor_chars(text: str) -> List[str]:
    """
    Tokenizes text while preserving certain censor characters (#, *, _, -).
    """
    return tokenize_text_with_regex(text, non_token_chars_skip_censor_chars)


def slugify(text: str) -> str:
    """
    Creates a slug from text - lowercase, normalized, alphanumeric only.
    This is a helper function for tokenize_identifier.
    """
    text = text.lower()
    text = normalize_unicode(text)
    text = re.sub(r'[^a-z0-9]', '', text)
    return text


class TokenizeArguments(ArgumentsBase):
    s: str
    skip_censor_chars: bool = False
    skip_regex: Optional[str] = None


class Tokenize(UDFBase[TokenizeArguments, List[str]]):
    def execute(self, execution_context: ExecutionContext, arguments: TokenizeArguments) -> List[str]:
        if arguments.skip_censor_chars:
            return tokenize_text_skipping_censor_chars(arguments.s)
        elif arguments.skip_regex is not None:
            pattern = re.compile(arguments.skip_regex)
            return tokenize_text_with_regex(arguments.s, non_token_chars_regex=pattern)
        else:
            return tokenize_text(arguments.s)
