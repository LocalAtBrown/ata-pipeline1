import re
from typing import Iterable, List, Set


def _test_pattern(pattern: re.Pattern[str], urlpaths: Iterable[str]):
    """
    Test a regex pattern against a list of strings. Only passes if said pattern
    matches the entire string for each string.
    """
    for path in urlpaths:
        re_match = pattern.search(path)
        # Assert match exists
        assert bool(re_match), f"{path} failed"
        index_start, index_end = re_match.span()
        # Assert match is entire string
        assert index_end - index_start == len(path), f"{path} failed"


def append_slash(urlpaths: List[str]) -> Set[str]:
    """
    Appends a slash to each URL paths in a list if it doesn't already have one and
    returns a set of unique URL paths.
    """
    urlpaths_with_slash = [f"{path}/" if path[-1] != "/" else path for path in urlpaths]
    return {*urlpaths, *urlpaths_with_slash}
