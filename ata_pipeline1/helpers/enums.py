from enum import Enum


class SiteName(str, Enum):
    """
    Enum of partner slugs corresponding to the S3 buckets
    """

    AFRO_LA = "afro-la"
    DALLAS_FREE_PRESS = "dallas-free-press"
    OPEN_VALLEJO = "open-vallejo"
    THE_19TH = "the-19th"


class EventName(str, Enum):
    """
    Enum of snowplow event names that we expect to handle
    """

    CHANGE_FORM = "change_form"
    FOCUS_FORM = "focus_form"
    PAGE_PING = "page_ping"
    PAGE_VIEW = "page_view"
    SUBMIT_FORM = "submit_form"
