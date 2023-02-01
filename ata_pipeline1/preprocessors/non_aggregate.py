import ast
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Any, Dict, Optional, Set, Union
from urllib.parse import urlparse

import numpy as np
import pandas as pd
import user_agents as ua

from ata_pipeline1.helpers.enums import (
    EventName,
    EventReferrerMedium,
    FieldNew,
    FieldSnowplow,
)
from ata_pipeline1.helpers.logging import logging
from ata_pipeline1.helpers.typing import Field
from ata_pipeline1.preprocessors.base import Preprocessor
from ata_pipeline1.site import (
    SiteDomain,
    SiteName,
    SiteNewsletterSignupValidator,
    SitePageClassifier,
)

logger = logging.getLogger(__name__)


# ---------- NEW PREPROCESSORS ----------
@dataclass
class AddFieldFormSubmitIsNewsletter(Preprocessor):
    """
    Adds a field indicating whether a form-submission event is of a newsletter-signup form.
    """

    site_newsletter_signup_validator: SiteNewsletterSignupValidator
    field_event_name: FieldSnowplow = FieldSnowplow.EVENT_NAME
    field_form_submit_is_newsletter: FieldNew = FieldNew.FORM_SUBMIT_IS_NEWSLETTER

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # Make a copy of the original so that it's not affected, but can remove
        # this if memory is an issue
        df = df.copy()

        df[self.field_form_submit_is_newsletter] = df.apply(self._is_newsletter_signup, axis=1)

        return df

    def log_result(self, df_in=None, df_out=None) -> None:
        logger.info(
            f"Added a new field {self.field_form_submit_is_newsletter} to check if form-submission event is a newsletter signup"
        )

    def _is_newsletter_signup(self, event: pd.Series) -> Union[float, bool]:
        # Return np.nan if event is not a form-submission event
        if event.at[self.field_event_name] != EventName.SUBMIT_FORM:
            return np.nan

        return self.site_newsletter_signup_validator.validate(event)


@dataclass
class AddFieldDeviceIsMobile(Preprocessor):
    """
    Adds a new column indicating whether device where event was recorded is mobile.
    """

    field_useragent: FieldSnowplow = FieldSnowplow.USERAGENT
    field_device_is_mobile: FieldNew = FieldNew.DVCE_IS_MOBILE

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        df[self.field_device_is_mobile] = df[self.field_useragent].apply(lambda x: ua.parse(x).is_mobile)

        return df

    def log_result(self, df_in: pd.DataFrame, df_out: pd.DataFrame) -> None:
        logger.info("Added a new field indicating whether device is mobile")


@dataclass
class AddFieldMaxScrollDepth(Preprocessor):
    """
    Adds a new column showing maximum scroll-depth percentage.
    """

    field_dvce_screenheight: FieldSnowplow = FieldSnowplow.DVCE_SCREENHEIGHT
    field_offset_y: FieldSnowplow = FieldSnowplow.PP_YOFFSET_MAX
    field_doc_height: FieldSnowplow = FieldSnowplow.DOC_HEIGHT
    field_max_scroll_depth: FieldNew = FieldNew.SCROLL_DEPTH_MAX

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        df[self.field_max_scroll_depth] = (
            # Screen height + y-offset = total pixels user's scrolled to
            ((df[self.field_dvce_screenheight] + df[self.field_offset_y]) / df[self.field_doc_height])
            # First, try to fillna using (screen height / page height) since we should expect reader to go as far as here without any scrolling
            .fillna(df[self.field_dvce_screenheight] / df[self.field_doc_height])
            # Fallback fillna
            .fillna(0)
            # Clamp to [0, 1] because we want percentages
            .clip(0, 1)
        )

        return df

    def log_result(self, df_in=None, df_out=None) -> None:
        logger.info("Calculated scroll depths and added them to a new field")


@dataclass
class SortFieldTimestamp(Preprocessor):
    """
    Sorts events by ascending timestamp.
    """

    field_timestamp: FieldSnowplow = FieldSnowplow.DERIVED_TSTAMP

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.sort_values(self.field_timestamp)

    def log_result(self, df_in=None, df_out=None) -> None:
        logger.info("Sorted events by ascending timestamp")


@dataclass
class SetRowIndex(Preprocessor):
    """
    Set a column (almost always event ID) as DataFrame index.
    """

    field_index: Field = FieldSnowplow.EVENT_ID

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.set_index(self.field_index, verify_integrity=True)

    def log_result(self, df_in=None, df_out=None) -> None:
        logger.info(f"Set {self.field_index} as DataFrame index")


@dataclass
class AddFieldEventParentId(Preprocessor):
    """
    For a **single partner site**, adds a new field specifying the first page-view event
    (of a user) that predates all of that user's events within that page in the same session.
    Because the data is NOT perfect, the first page-view event might be missing,
    in which case the parent is whichever event that comes first.

    Parent events are necessary for aggregating page pings and form activities.

    TODO: When performing this transformation, it's possible that some parents are
    not present in the input DataFrame but still present in the overall database.
    (For example, if a SQL query asks only for events from 2023-01-01 00:00:00, a
    page-ping event happening at 2023-01-01 00:00:02 might have a parent (a page
    view or otherwise) before that 00:00:00 mark, which therefore isn't in the input
    DataFrame.) As a result, we need to make sure to include all possible parents;
    one way of doing so would be to add a session-index parameter to the query
    in addition to the timestamps.

    Example:
    .. code-block:: text
        event_id    site_name       derived_tstamp domain_userid  domain_sessionidx       page_urlpath event_name EVENT_PARENT_ID
              A0  daily-scoop  2023-01-01 00:00:00             A                  1                  /  page_view              A0
              B1  daily-scoop  2023-01-01 00:00:10             A                  1                  /  page_ping              A0
              C2  daily-scoop  2023-01-01 00:01:13             A                  1  /articles/story1/  page_ping              C2
              D3  daily-scoop  2023-01-01 00:01:23             A                  1  /articles/story1/  page_ping              C2
              E4  daily-scoop  2023-01-01 00:01:27             A                  1                  /  page_view              E4
              F5  daily-scoop  2023-01-01 00:45:00             A                  2                  /  page_view              F5
              G6  daily-scoop  2023-01-01 00:04:12             B                  1                  /  page_view              G6
    """

    field_timestamp: FieldSnowplow = FieldSnowplow.DERIVED_TSTAMP
    field_user_id: FieldSnowplow = FieldSnowplow.DOMAIN_USERID
    field_user_session_idx: FieldSnowplow = FieldSnowplow.DOMAIN_SESSIONIDX
    field_page_urlpath: FieldSnowplow = FieldSnowplow.PAGE_URLPATH
    field_event_id: FieldSnowplow = FieldSnowplow.EVENT_ID
    field_event_name: FieldSnowplow = FieldSnowplow.EVENT_NAME
    field_event_parent_id: FieldNew = FieldNew.EVENT_PARENT_ID
    ping_interval_seconds: int = 10
    ping_interval_noise_seconds: int = 50

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df_event_parent_mapping = df.groupby(
            [self.field_user_id, self.field_user_session_idx, self.field_page_urlpath],
            group_keys=False,
        ).apply(self._get_parent_events)

        return df.join(df_event_parent_mapping)

    def _get_parent_events(self, df_group: pd.DataFrame) -> pd.DataFrame:
        """
        Determines parent events among events within a pandas GroupBy object.
        """
        # First, extract only columns we need
        # ROWS SHOULD ALREADY HAVE BEEN SORTED BY ASCENDING TIMESTAMP
        df_group = df_group[[self.field_timestamp, self.field_event_name]]

        # Pointer: timestamp of previous event
        timestamp_prev = df_group.iloc[0][self.field_timestamp]
        # Current event ID
        event_id = df_group.iloc[0].name
        # Current parent ID
        event_parent_id = event_id

        results = [(event_id, event_parent_id)]

        # Iterate over sorted events
        for event_id, timestamp, event_name in df_group.iloc[1:].itertuples():
            # If current event is a page view or time from previous event exceeds ping interval
            # (with added noise), also indicating a page view, we have a new parent
            if (
                event_name == EventName.PAGE_VIEW
                or (timestamp - timestamp_prev).total_seconds()
                > self.ping_interval_seconds + self.ping_interval_noise_seconds
            ):
                # Assign current event to current parent
                event_parent_id = event_id

            # Append event ID in order to accurately join output DataFrame with original DataFrame
            results.append((event_id, event_parent_id))

            # Update timestamp of previous as current timestamp before moving on to
            # next event (and next timestamp)
            timestamp_prev = timestamp

        # Return result as joinable DataFrame
        # return pd.DataFrame(results, columns=[self.field_site_name, self.field_event_id, self.field_event_parent_id])
        return pd.DataFrame(results, columns=[self.field_event_id, self.field_event_parent_id]).set_index(
            self.field_event_id
        )

    def log_result(self, df_in, df_out) -> None:
        logger.info(f"Found and added {df_out[self.field_event_parent_id].nunique()} parent events as a new field.")


@dataclass
class AddFieldSessionEventIndex(Preprocessor):
    """
    Adds a new field which shows page-view index/order within a single session
    (starting with 1).
    """

    field_timestamp: FieldSnowplow = FieldSnowplow.DERIVED_TSTAMP
    field_user_id: FieldSnowplow = FieldSnowplow.DOMAIN_USERID
    field_user_session_idx: FieldSnowplow = FieldSnowplow.DOMAIN_SESSIONIDX
    field_user_session_event_idx: FieldNew = FieldNew.DOMAIN_SESSION_EVENTIDX

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        column_session_pageview_indices = (
            df.groupby([self.field_user_id, self.field_user_session_idx])[self.field_timestamp]
            .rank(method="first")
            .astype(int)
            .rename(self.field_user_session_event_idx)
        )
        return df.join(column_session_pageview_indices)

    def log_result(self, df_in=None, df_out=None) -> None:
        logger.info("Created a new column showing page-event order within a user's session")


@dataclass
class AddFieldsPageType(Preprocessor):
    """
    For a given site, determines whether or not a page associated with an event is:
    -  Home page
    -  About us/our team
    -  Newsletter form page (must have only newsletter forms and no other content, a.k.a. dedicated newsletter-signup page)
    -  Donation/membership (must have only donation forms and no other content, a.k.a. dedicated donation/membership page)
    -  Story/article
    -  Section/tag page
    -  Author profile & stories list

    Note that some of these categories are not mutually exclusive, e.g., a page
    can be both an "About us" page and a newsletter-form page. As a result, this
    preprocessor creates a new boolean field corresponding to each category (much
    in the style of one-hot encoding).
    """

    site_page_type_classifier: SitePageClassifier
    field_page_urlpath: FieldSnowplow = FieldSnowplow.PAGE_URLPATH
    field_page_is_home: FieldNew = FieldNew.PAGE_IS_HOME
    field_page_is_about_us: FieldNew = FieldNew.PAGE_IS_ABOUT_US
    field_page_is_newsletter: FieldNew = FieldNew.PAGE_IS_NEWSLETTER
    field_page_is_donation: FieldNew = FieldNew.PAGE_IS_DONATION
    field_page_is_article: FieldNew = FieldNew.PAGE_IS_ARTICLE
    field_page_is_section: FieldNew = FieldNew.PAGE_IS_SECTION
    field_page_is_author_profile: FieldNew = FieldNew.PAGE_IS_AUTHOR_PROFILE

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # Make a copy of the original so that it's not affected, but can remove
        # this if memory is an issue
        df = df.copy()

        self.fields_to_add = [
            self.field_page_is_home,
            self.field_page_is_about_us,
            self.field_page_is_newsletter,
            self.field_page_is_donation,
            self.field_page_is_article,
            self.field_page_is_section,
            self.field_page_is_author_profile,
        ]

        df[self.fields_to_add] = df.apply(self._classify_page, axis=1)

        return df

    def _classify_page(self, event: pd.Series) -> pd.Series:
        return pd.Series(
            [
                self.site_page_type_classifier.is_home(event),
                self.site_page_type_classifier.is_about_us(event),
                self.site_page_type_classifier.is_newsletter(event),
                self.site_page_type_classifier.is_donation(event),
                self.site_page_type_classifier.is_article(event),
                self.site_page_type_classifier.is_section(event),
                self.site_page_type_classifier.is_author_profile(event),
            ],
            index=self.fields_to_add,
        )

    def log_result(self, df_in, df_out) -> None:
        logger.info("Classified event page into one or more page types")


@dataclass
class ReplaceValues(Preprocessor):
    """
    Replaces values(s) across a subset of fields. If this subset of fields
    is not specified, performs replacement across all fields.
    """

    replace_what: Any
    replace_with: Any
    fields: Optional[Set[Field]] = dataclass_field(default_factory=lambda: {*FieldSnowplow, *FieldNew})

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        fields = df.columns.intersection([*self.fields])
        df[fields] = df[fields].replace(self.replace_what, self.replace_with)

        return df

    def log_result(self, df_in: pd.DataFrame, df_out: pd.DataFrame) -> None:
        logger.info(
            f'Replaced "{self.replace_what}" with "{self.replace_with}" '
            + f"for {'all fields' if self.fields == {*FieldSnowplow, *FieldNew} else 'fields: {f}'.format(f=', '.join(self.fields))}"
        )


@dataclass
class ReclassifyNullReferrals(Preprocessor):
    """
    Changes the `refr_medium` field so that the NaN or None values are turned
    into either our custom `NO_REFERRER` enum or something else.
    """

    site_domains: Set[SiteDomain]
    field_referral_url: FieldSnowplow = FieldSnowplow.PAGE_REFERRER
    field_referral_medium: FieldSnowplow = FieldSnowplow.REFR_MEDIUM
    num_false_positives: int = dataclass_field(default=0, repr=False)
    num_false_negatives: int = dataclass_field(default=0, repr=False)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # Make a copy of the original so that it's not affected, but can remove
        # this if memory is an issue
        df = df.copy()

        df[self.field_referral_medium] = df.apply(self._reclassify, axis=1)
        df[self.field_referral_medium] = pd.Categorical(
            df[self.field_referral_medium], categories=[*EventReferrerMedium]
        )

        return df

    def log_result(self, df_in: pd.DataFrame, df_out=None) -> None:
        logger.info(
            f"Replaced referral medium of {self.num_false_negatives} ({self.num_false_negatives / df_in.shape[0]:.1%}) non-null-medium events to 'no_referrer' and "
            + f"of {self.num_false_positives} ({self.num_false_positives / df_in.shape[0]:.1%}) of previously null-medium events to 'unknown'"
        )

    def _reclassify(self, event: pd.Series) -> EventReferrerMedium:
        """
        Row-level main reclassification logic.
        """
        referrer_medium = event.at[self.field_referral_medium]
        referrer_url = urlparse(event.at[self.field_referral_url])

        referrer_medium_is_truthy = bool(referrer_medium)
        referrer_url_is_valid = len(referrer_url.netloc) > 0  # There's probably a tighter way than checking length

        if not referrer_url_is_valid and not referrer_medium_is_truthy:
            # True positive: Medium is labeled NULL, and it in fact is NULL
            # Just need to convert to our custom enum value for NULL
            return EventReferrerMedium.NO_REFERRER
        elif not referrer_url_is_valid and referrer_medium_is_truthy:
            # False negative: Medium is labeled not NULL, but it in fact is NULL
            self.num_false_negatives += 1
            return EventReferrerMedium.NO_REFERRER
        elif referrer_url_is_valid and not referrer_medium_is_truthy:
            # False positive: Medium is labeled NULL, but it in fact is not NULL
            # There's probably a better way to handle false positives than assigning unknown,
            # but given the small percentage of this happening (< 0.1% of all aggregated events),
            # this is fine for now
            self.num_false_positives += 1
            return EventReferrerMedium.UNKNOWN
        else:
            return referrer_medium


@dataclass
class ReclassifyInternalReferrals(Preprocessor):
    """
    Changes the `refr_medium` field so that events whose referrer we consider
    to be part of a partner's domain is property classified as `internal`.
    """

    site_domains: Set[SiteDomain]
    field_referral_url: FieldSnowplow = FieldSnowplow.PAGE_REFERRER
    field_referral_medium: FieldSnowplow = FieldSnowplow.REFR_MEDIUM
    num_false_positives: int = dataclass_field(default=0, repr=False)
    num_false_negatives: int = dataclass_field(default=0, repr=False)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # Make a copy of the original so that it's not affected, but can remove
        # this if memory is an issue
        df = df.copy()

        df[self.field_referral_medium] = df.apply(self._reclassify, axis=1)
        df[self.field_referral_medium] = pd.Categorical(
            df[self.field_referral_medium], categories=[*EventReferrerMedium]
        )

        return df

    def log_result(self, df_in: pd.DataFrame, df_out=None) -> None:
        logger.info(
            f"Replaced referral medium of {self.num_false_negatives} ({self.num_false_negatives / df_in.shape[0]:.1%}) events to 'internal'."
        )

    def _reclassify(self, event: pd.Series) -> EventReferrerMedium:
        """
        Row-level main reclassification logic.
        """
        referrer_medium: EventReferrerMedium = event.at[self.field_referral_medium]

        # This means this preprocessor should be placed after ReclassifyNullReferrals
        if referrer_medium == EventReferrerMedium.NO_REFERRER:
            return referrer_medium

        referrer_url = urlparse(event.at[self.field_referral_url])
        referrer_url_matches_domain = any(
            [domain.pattern.search(referrer_url.netloc) is not None for domain in self.site_domains]
        )

        if referrer_url_matches_domain and referrer_medium != EventReferrerMedium.INTERNAL:
            # Handle false negatives: medium is labeled not internal, but it in fact is
            self.num_false_negatives += 1
            return EventReferrerMedium.INTERNAL
        elif not referrer_url_matches_domain and referrer_medium == EventReferrerMedium.INTERNAL:
            # False positive: medium is labeled internal, but it in fact is not
            # Dealing with this is complicated, so leaving it as-is for now
            return referrer_medium
        else:
            return referrer_medium


@dataclass
class ParseInternalReferrerUrl(Preprocessor):
    """
    For each event, given its referrer URL, returns the path if that URL
    if it's internal.
    """

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        pass

    def log_result(self, df_in=None, df_out=None) -> None:
        pass

    def _parse(self, event: pd.Series) -> str:
        pass


@dataclass
class AddFieldLeadsToNewsletterConversion(Preprocessor):
    """
    Adds the target-variable column showing whether (aggregated) page-view
    event leads to newsletter conversion.
    """

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        pass

    def log_result(self, df_in=None, df_out=None) -> None:
        logger.info("Created target-label column")


# ---------- PIPELINE 0 PREPROCESSORS ----------
@dataclass
class SelectFieldsRelevant(Preprocessor):
    """
    Select relevant fields from an events DataFrame. If a field doesn't exist,
    it'll be added to the result DataFrame as an empty column.
    """

    fields_relevant: Set[FieldSnowplow]

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # Sometimes, df doesn't have all the fields in fields_relevant, so we create
        # an empty DataFrame with all the fields we'd like to have and concatenate df to it
        df_empty_with_all_fields = pd.DataFrame(columns=[*self.fields_relevant])
        # Get a list of fields in fields_relevant that are actually in df, because we
        # don't want to query for nonexistent fields and have pandas raise a KeyError
        fields_available = df.columns.intersection([*self.fields_relevant])

        # Query for fields in fields_available and perform said concatenation, so that
        # the final DataFrame will have all the fields in fields_relevant
        df = pd.concat([df_empty_with_all_fields, df[[*fields_available]]])

        return df

    def log_result(self, df_in=None, df_out=None) -> None:
        logger.info("Selected relevant fields")


@dataclass
class DeleteRowsEmpty(Preprocessor):
    """
    Given a list of fields that cannot have empty or null data, remove all rows
    with null values in any of these fields.
    """

    fields_required: Set[FieldSnowplow]

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.dropna(subset=[*self.fields_required])

    def log_result(self, df_in: pd.DataFrame, df_out: pd.DataFrame) -> None:
        logger.info(
            f"Deleted {df_in.shape[0] - df_out.shape[0]} rows with at least 1 empty cell in a required field from staged DataFrame"
        )


@dataclass
class DeleteRowsDuplicateKey(Preprocessor):
    """
    Delete all rows whose primary key is repeated in the DataFrame.
    """

    field_primary_key: FieldSnowplow
    field_timestamp: FieldSnowplow

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # Sort values by timestamp so the first event kept is the earliest,
        # which is most likely to be a parent (if its key doesn't already exist
        # in the DB)
        # (see: https://snowplow.io/blog/dealing-with-duplicate-event-ids/)
        df = df.sort_values(self.field_timestamp)
        return df.drop_duplicates(subset=[self.field_primary_key], keep="first")

    def log_result(self, df_in: pd.DataFrame, df_out: pd.DataFrame) -> None:
        logger.info(
            f"Deleted {df_in.shape[0] - df_out.shape[0]} rows with duplicate {self.field_primary_key} from staged DataFrame"
        )


@dataclass
class DeleteRowsBot(Preprocessor):
    """
    Delete all rows where the event is made by a bot.
    """

    field_useragent: FieldSnowplow

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[df[self.field_useragent].apply(lambda x: not ua.parse(x).is_bot)]

    def log_result(self, df_in: pd.DataFrame, df_out: pd.DataFrame) -> None:
        logger.info(f"Deleted {df_in.shape[0] - df_out.shape[0]} rows whose event is made by a bot")


@dataclass
class ConvertFieldTypes(Preprocessor):

    """
    Changes data types in a Snowplow events DataFrame to those desired.
    """

    fields_int: Set[FieldSnowplow] = dataclass_field(default_factory=lambda: {FieldSnowplow.DOMAIN_SESSIONIDX})
    fields_float: Set[FieldSnowplow] = dataclass_field(
        default_factory=lambda: {
            FieldSnowplow.DOC_HEIGHT,
            FieldSnowplow.DVCE_SCREENHEIGHT,
            FieldSnowplow.DVCE_SCREENWIDTH,
            FieldSnowplow.PP_YOFFSET_MAX,
        }
    )
    fields_datetime: Set[FieldSnowplow] = dataclass_field(default_factory=lambda: {FieldSnowplow.DERIVED_TSTAMP})
    fields_categorical: Set[FieldSnowplow] = dataclass_field(
        default_factory=lambda: {
            FieldSnowplow.EVENT_NAME,
            FieldSnowplow.REFR_MEDIUM,
            FieldSnowplow.REFR_SOURCE,
        }
    )
    fields_json: Set[FieldSnowplow] = dataclass_field(
        default_factory=lambda: {
            FieldSnowplow.SEMISTRUCT_FORM_CHANGE,
            FieldSnowplow.SEMISTRUCT_FORM_FOCUS,
            FieldSnowplow.SEMISTRUCT_FORM_SUBMIT,
        }
    )

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # Make a copy of the original so that it's not affected, but can remove
        # this if memory is an issue
        df = df.copy()

        df[[*self.fields_int]] = df[[*self.fields_int]].astype(int)
        df[[*self.fields_float]] = df[[*self.fields_float]].astype(float)

        # pd.to_datetime can only turn pandas Series to datetime, so need to convert
        # one Series/column at a time
        # All timestamps should already be in UTC: https://discourse.snowplow.io/t/what-timezones-are-the-timestamps-set-in/622,
        # but setting utc=True just to be safe
        for field in self.fields_datetime:
            df[field] = pd.to_datetime(df[field], utc=True)

        df[[*self.fields_categorical]] = df[[*self.fields_categorical]].astype("category")

        # df = df.replace([np.nan], [None])

        for field in self.fields_json:
            df[field] = df[field].apply(self._convert_to_json)
        return df

    @staticmethod
    def _convert_to_json(value: str) -> Dict:
        try:
            # if valid json, will convert to a dictionary
            return ast.literal_eval(value)
        except ValueError:
            # if invalid, will throw a ValueError and we just want it to return None
            return None  # type: ignore

    def log_result(self, df_in=None, df_out=None) -> None:
        logger.info("Converted field data types")


@dataclass
class AddFieldSiteName(Preprocessor):
    """
    Adds a constant field holding partner's name to the Snowplow events DataFrame.
    """

    site_name: SiteName
    field_site_name: FieldNew

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # Make a copy of the original so that it's not affected, but can remove
        # this if memory is an issue
        df = df.copy()

        df[self.field_site_name] = self.site_name

        return df

    def log_result(self, df_in=None, df_out=None) -> None:
        logger.info(f"Added site name {self.site_name} as a new field")
