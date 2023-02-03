from typing import List, Set


def append_slash(urlpath: str) -> str:
    """
    Appends a slash to a URL path in a list if it doesn't already have one.
    """
    return f"{urlpath}/" if urlpath[-1] != "/" else urlpath


def bulk_append_slash(urlpaths: List[str]) -> Set[str]:
    """
    Appends a slash to each URL path in a list if it doesn't already have one and
    returns a set of unique URL paths.
    """
    urlpaths_with_slash = [append_slash(path) for path in urlpaths]
    return {*urlpaths, *urlpaths_with_slash}
