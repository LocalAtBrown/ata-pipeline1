from enum import Enum


class SiteName(str, Enum):
    """
    Enum of partner slugs corresponding to the S3 buckets.
    """

    AFRO_LA = "afro-la"
    DALLAS_FREE_PRESS = "dallas-free-press"
    OPEN_VALLEJO = "open-vallejo"
    THE_19TH = "the-19th"
