import re
from typing import Iterable


def _test_pattern(pattern: re.Pattern[str], urlpaths: Iterable[str]):
    """
    Test a regex pattern against a list of strings. Only passes if said pattern
    matches the entire string for each string.
    """
    for path in urlpaths:
        # Assert full match exists
        assert pattern.fullmatch(path), f"{path} failed"
