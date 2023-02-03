from typing import List, Set


def bulk_append_slash(urlpaths: List[str]) -> Set[str]:
    """
    Appends a slash to each URL path in a list if it doesn't already have one and
    returns a set of unique URL paths.
    """
    urlpaths_with_slash = [f"{path}/" if path[-1] != "/" else path for path in urlpaths]
    return {*urlpaths, *urlpaths_with_slash}
