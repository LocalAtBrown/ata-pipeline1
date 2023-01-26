from enum import Enum, auto
from typing import Any


class _StrEnum(str, Enum):
    """
    StrEnum class. Replace with built-in version after upgrading to Python 3.10.
    """

    @staticmethod
    def _generate_next_value_(name: str, *args: Any, **kwargs: Any) -> str:
        return name.lower()

    def __str__(self) -> str:
        return f"{self.value}"


# ---------- EVENT ENUMS ----------
class EventName(_StrEnum):
    """
    Enum of snowplow event names that we expect to handle
    """

    CHANGE_FORM = auto()
    FOCUS_FORM = auto()
    PAGE_PING = auto()
    PAGE_VIEW = auto()
    SUBMIT_FORM = auto()


# ---------- DATA FIELD ENUMS ----------
class FieldSnowplow(_StrEnum):
    """
    Enum for Snowplow fields of interest.
    Snowplow documentation of these fields can be found here: https://docs.snowplow.io/docs/understanding-your-pipeline/canonical-event/.

    This class also subclasses from str so that, e.g., Field.DERIVED_TSTAMP == "derived_tstamp"
    which means we can pass Field.DERIVED_TSTAMP into a pandas DataFrame directly without having
    to grab its string value (Field.DERIVED_TSTAMP.value) first. (See use case as well as caveats:
    https://stackoverflow.com/questions/58608361/string-based-enum-in-python.)
    """

    # [DATETIME] Timestamp making allowance for innaccurate device clock
    DERIVED_TSTAMP = auto()

    # [FLOAT] The page's height in pixels
    DOC_HEIGHT = auto()

    # [INT] Number of the current user session, e.g. first session is 1, next session is 2, etc. Dependent on domain_userid
    DOMAIN_SESSIONIDX = auto()

    # [STR, CATEGORICAL if needed] User ID set by Snowplow using 1st party cookie
    DOMAIN_USERID = auto()

    # [FLOAT] Screen height in pixels. Almost 1-to-1 relationship with domain_userid (there are exceptions)
    DVCE_SCREENHEIGHT = auto()

    # [FLOAT] Screen width in pixels. Almost 1-to-1 relationship with domain_userid (there are exceptions)
    DVCE_SCREENWIDTH = auto()

    # [STR] ID of event. This would be the primary key within the site DataFrame,
    # and part of the [site_name, event_id] composite key in the database table
    EVENT_ID = auto()

    # [STR, CATEGORICAL] Name of event. Can be "page_view", "page_ping", "focus_form", "change_form", "submit_form"
    EVENT_NAME = auto()

    # [STR, CATEGORICAL if needed] User ID set by Snowplow using 3rd party cookie
    NETWORK_USERID = auto()

    # [STR, CATEGORICAL if needed] Path to page, e.g., /event-directory/ in https://dallasfreepress.com/event-directory/
    PAGE_URLPATH = auto()

    # [STR] URL of the referrer
    PAGE_REFERRER = auto()

    # [FLOAT] Maximum page y-offset seen in the last ping period. Depends on event_name == "pageauto()
    PP_YOFFSET_MAX = auto()

    # [STR, CATEGORICAL] Type of referer. Can be "social", "search", "internal", "unknown", "email"
    # (read: https://docs.snowplow.io/docs/enriching-your-data/available-enrichments/referrer-parser-enrichment/)
    REFR_MEDIUM = auto()

    # [STR, CATEGORICAL] Name of referer if recognised, e.g., "Google" or "Bing"
    REFR_SOURCE = auto()

    # [STR (JSON)] Data/attributes of HTML input and its form in JSON format. Only present if event_name == "change_form"
    # (read: https://github.com/snowplow/iglu-central/blob/master/schemas/com.snowplowanalytics.snowplow/change_form/jsonschema/1-0-0)
    SEMISTRUCT_FORM_CHANGE = "unstruct_event_com_snowplowanalytics_snowplow_change_form_1"

    # [STR (JSON)] Data/attributes of HTML input and its form in JSON format. Only present if event_name == "focus_form"
    # (read: https://github.com/snowplow/iglu-central/blob/master/schemas/com.snowplowanalytics.snowplow/focus_form/jsonschema/1-0-0)
    SEMISTRUCT_FORM_FOCUS = "unstruct_event_com_snowplowanalytics_snowplow_focus_form_1"

    # [STR (JSON)] Data/attributes of HTML form and all its inputs in JSON format. Only present if event_name == "submit_form"
    # (read: https://github.com/snowplow/iglu-central/blob/master/schemas/com.snowplowanalytics.snowplow/submit_form/jsonschema/1-0-0)
    SEMISTRUCT_FORM_SUBMIT = "unstruct_event_com_snowplowanalytics_snowplow_submit_form_1"

    # [STR] Raw useragent
    USERAGENT = auto()


class FieldNew(_StrEnum):
    """
    Enum for non-Snowplow fields to be added.
    """

    # [BOOLEAN] Whether device where event was recorded is mobile
    DVCE_IS_MOBILE = auto()

    # [FLOAT] Dwell time in seconds
    DWELL_SECS = auto()

    # [STR] ID of parent event. Must be a valid ID in event_id column
    EVENT_PARENT_ID = auto()

    # [BOOLEAN] Whether form submission event is of a newsletter form. NULL if event isn't form submission
    FORM_SUBMIT_IS_NEWSLETTER = auto()

    # [BOOLEAN] Whether page is an "About us" page or something similar
    PAGE_IS_ABOUT_US = auto()

    # [BOOLEAN] Whether page is an article/story
    PAGE_IS_ARTICLE = auto()

    # [BOOLEAN] Whether page is an author bio page
    PAGE_IS_AUTHOR_PROFILE = auto()

    # [BOOLEAN] Whether page is a donation or membership-signup page
    PAGE_IS_DONATION = auto()

    # [BOOLEAN] Whether page is home
    PAGE_IS_HOME = auto()

    # [BOOLEAN] Whether page is a newsletter-signup page
    PAGE_IS_NEWSLETTER = auto()

    # [BOOLEAN] Whether page is a section/category/tag/topic page with a (paginated)
    # list of articles
    PAGE_IS_SECTION = auto()

    # [FLOAT] Maximum scroll depth as percentage
    SCROLL_DEPTH_MAX = auto()

    # [STR] Site partner's name (as a slug corresponding to its S3 bucket)
    SITE_NAME = auto()


# ---------- PAGE TYPES ----------
class PageType(_StrEnum):
    HOME = auto()
    ABOUT_US = auto()
    NEWSLETTER = auto()
    DONATION = auto()
    ARTICLE = auto()
    SECTION = auto()
    AUTHOR_PROFILE = auto()
