from enum import Enum


class EventName(str, Enum):
    """
    Enum of snowplow event names that we expect to handle
    """

    CHANGE_FORM = "change_form"
    FOCUS_FORM = "focus_form"
    PAGE_PING = "page_ping"
    PAGE_VIEW = "page_view"
    SUBMIT_FORM = "submit_form"
