import ast
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union

import numpy as np
import pandas as pd
import user_agents as ua

from ata_pipeline1.helpers.dataclasses import FormSubmitData, parse_form_submit_dict
from ata_pipeline1.helpers.enums import (
    EventName,
    EventReferrerMedium,
    FieldNew,
    FieldSnowplow,
)
from ata_pipeline1.helpers.logging import logging
from ata_pipeline1.helpers.typing import Field
from ata_pipeline1.helpers.urllib import append_slash
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

    field_viewport_height: FieldSnowplow = FieldSnowplow.BR_VIEWHEIGHT
    field_screen_height: FieldSnowplow = FieldSnowplow.DVCE_SCREENHEIGHT
    field_page_height: FieldSnowplow = FieldSnowplow.DOC_HEIGHT
    field_offset_y: FieldSnowplow = FieldSnowplow.PP_YOFFSET_MAX
    field_max_scroll_depth: FieldNew = FieldNew.SCROLL_DEPTH_MAX

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        df[self.field_max_scroll_depth] = (
            # Screen height + y-offset = total pixels user's scrolled to
            ((df[self.field_viewport_height] + df[self.field_offset_y]) / df[self.field_page_height])
            # First, try to fillna using (viewport height / page height) since we should expect reader to go as far as here without any scrolling
            .fillna(df[self.field_viewport_height] / df[self.field_page_height])
            # Fallback fillna: Use device height as viewport height if viewport height's not available
            .fillna(df[self.field_screen_height] / df[self.field_page_height])
            # Fallback fillna
            .fillna(0)
            # Clamp to [0, 1] because we want percentages
            .clip(0, 1)
        )

        return df

    def log_result(self, df_in=None, df_out=None) -> None:
        logger.info("Calculated scroll depths and added them to a new field")


@dataclass
class ConvertFieldFormSubmitToDataclass(Preprocessor):
    """
    Convert the form-submission data field from dict to corresponding dataclass.
    """

    field_form_submit_data: FieldSnowplow = FieldSnowplow.SEMISTRUCT_FORM_SUBMIT

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # Make a copy of the original so that it's not affected, but can remove
        # this if memory is an issue
        df = df.copy()

        df[self.field_form_submit_data] = df[self.field_form_submit_data].apply(self._convert)

        return df

    def _convert(self, form_submit_data: Optional[Dict[str, Any]]) -> Optional[FormSubmitData]:
        if form_submit_data is None:
            return None

        return parse_form_submit_dict(form_submit_data)

    def log_result(self, df_in=None, df_out=None) -> None:
        logger.info(f"Converted the {self.field_form_submit_data} field to a Series of form-submission dataclasses")


@dataclass
class DeleteUsersTooManyNewsletterSubmissions(Preprocessor):
    """
    Delete any user who, in a single session, submits the newsletter form
    too often.

    This preprocessor was created in response to a bot problem The 19th was
    facing, but, theoretically, it can happen to any partner.
    """

    field_form_submit_is_newsletter: FieldNew = FieldNew.FORM_SUBMIT_IS_NEWSLETTER
    field_form_submit_data: FieldSnowplow = FieldSnowplow.SEMISTRUCT_FORM_SUBMIT
    field_user_id: FieldSnowplow = FieldSnowplow.DOMAIN_USERID
    field_user_session_idx: FieldSnowplow = FieldSnowplow.DOMAIN_SESSIONIDX
    # For demonstration of how the values below were derived,
    # refer to the Appendix_Multiple_Newsletter_Signup notebook
    max_newsletter_submissions_per_session: int = 3
    # The "uniqueness" of a newsletter-form submission is determined via its input email address
    max_unique_newsletter_submissions_per_session: int = 1

    # Private variables & constants
    _FIELD_NUM_SUBMISSIONS = "num_submissions"
    _FIELD_NUM_SUBMISSIONS_UNIQUE = "num_submissions_unique"
    _num_users_deleted: int = dataclass_field(init=False, repr=False)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # Count newsletter-form submissions per user-session
        df_submission_counts = (
            df.query(f"{self.field_form_submit_is_newsletter} == True")
            .groupby([self.field_user_id, self.field_user_session_idx])[self.field_form_submit_data]
            .aggregate(
                **{
                    self._FIELD_NUM_SUBMISSIONS: "size",
                    self._FIELD_NUM_SUBMISSIONS_UNIQUE: self._count_unique_submissions_in_session,
                }
            )
        )

        # Identify users to delete
        users_to_delete = df_submission_counts.query(
            f"({self._FIELD_NUM_SUBMISSIONS} > {self.max_newsletter_submissions_per_session}) or "
            + f"({self._FIELD_NUM_SUBMISSIONS_UNIQUE} > {self.max_unique_newsletter_submissions_per_session})"
        ).index.get_level_values(self.field_user_id)

        self._num_users_deleted = len(users_to_delete)

        # Delete users using indexing
        original_fields_index = df.index.names  # Remember what previous index is because gonna have to change it
        df = df.reset_index().set_index(self.field_user_id)
        return (
            df[~df.index.isin(users_to_delete)].reset_index().set_index(original_fields_index)
        )  # Set back to previous index

    @staticmethod
    def _count_unique_submissions_in_session(session_submission_data: "pd.Series[Dict]"):
        return session_submission_data.apply(lambda x: [e for e in x.elements if e.type == "email"][0].value).nunique()

    def log_result(self, df_in: pd.DataFrame, df_out: pd.DataFrame) -> None:
        num_users = df_in[self.field_user_id].nunique()
        num_rows = df_in.shape[0]
        num_rows_deleted = num_rows - df_out.shape[0]
        logger.info(
            f"Removed {self._num_users_deleted:,} users who submit a newsletter form "
            + f"more than {self.max_newsletter_submissions_per_session:,} times or "
            + f"with more than {self.max_unique_newsletter_submissions_per_session:,} unique email addresses in any one session. "
            + f"They account for {self._num_users_deleted / num_users:.1%} of all users "
            + f"and {num_rows_deleted:,} ({num_rows_deleted / num_rows:.1%}) rows in the input DataFrame."
        )


@dataclass
class DeleteRowsOutlier(Preprocessor):
    """
    Delete rows with at least one outlier value in or more quantitative fields,
    using specified minimum and maximum cutoff values for each field.

    Note that there exist more advanced techniques to deal with outliers, such as
    z-score or modified z-score, than simply using raw-value cutoffs. But the author
    believes they're better used in an exploratory environment (e.g., in a Jupyter
    notebook) than mechanically and in a production environment (e.g., this pipeline1
    source code). (See: https://colingorrie.github.io/outlier-detection.html.) These
    techniques can be performed as a step toward deriving sensible cutoff values.

    Where possible, try to prioritize specifying the min and max as conditions while
    fetching events from the SQL database over using this preprocessor.
    """

    # Bound tuple is in (min, max) format
    bounds: Dict[Field, Tuple[Optional[float], Optional[float]]]

    # Private variables
    _fields: Iterable[Field] = dataclass_field(init=False, repr=False)
    _statement: str = dataclass_field(init=False, repr=False)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # Get all fields
        self._fields = self.bounds.keys()

        # Build query
        self._statement = self._build_query_statement()

        # Run query
        return df.query(self._statement)

    def _build_query_statement(self) -> str:
        statement_components = [
            self._build_query_statement_component(field, self.bounds[field]) for field in self._fields
        ]
        return " and ".join([f"({c})" for c in statement_components])

    @staticmethod
    def _build_query_statement_component(field: Field, field_bounds: Tuple[float, float]) -> str:
        minimum, maximum = field_bounds

        condition_min = f"{field} >= {minimum}" if minimum is not None else None
        condition_max = f"{field} <= {maximum}" if maximum is not None else None

        return f"{condition_min or True} and {condition_max or True}"

    def log_result(self, df_in: pd.DataFrame, df_out: pd.DataFrame) -> None:
        num_rows_in = df_in.shape[0]
        num_rows_out = df_out.shape[0]
        num_rows_deleted = num_rows_in - num_rows_out
        logger.info(
            f"Deleted {num_rows_deleted:,} rows whose values are an outlier in at least "
            + f"one of the following fields: [{', '.join(self._fields)}]. "
            + f"They account for {num_rows_deleted / num_rows_in:.1%} of all input rows"
        )


@dataclass
class ClipRowsOutlier(Preprocessor):
    """
    In a given quantitative field, replaces outlier values on the left-hand side with
    a lower bound (if specified) and on the right-hand side with an upper bound (if
    specified).
    """

    # Bound tuple is in (min, max) format
    bounds: Dict[Field, Tuple[Optional[float], Optional[float]]]

    # Private variables
    _fields: Iterable[Field] = dataclass_field(init=False, repr=False)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # Get fields
        self._fields = self.bounds.keys()

        # Make a copy of the original so that it's not affected, but can remove
        # this if memory is an issue
        df = df.copy()

        for field in self._fields:
            minimum, maximum = self.bounds[field]
            df[field] = df[field].clip(lower=minimum, upper=maximum)

        return df

    def log_result(self, df_in: pd.DataFrame, df_out: pd.DataFrame) -> None:
        df_in = df_in[[*self._fields]]
        df_out = df_out[[*self._fields]]
        num_rows_clipped = (df_in != df_out).all(axis=1).sum()
        logger.info(
            f"Clipped outlier values in any of the following fields: [{', '.join(self._fields)}] "
            + f"in {num_rows_clipped:,} rows. "
            + f"They account for {num_rows_clipped / df_in.shape[0]:.1%} of all input rows"
        )


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
    Set a column or columns as DataFrame index.
    """

    fields_index: List[Field]  # List because we want to perserve the order
    reset_index: bool = False
    verify_integrity: bool = True
    sort_index: bool = True

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.reset_index:
            df = df.reset_index()

        df = df.set_index(self.fields_index, verify_integrity=self.verify_integrity)

        if self.sort_index:
            df = df.sort_index()

        return df

    def log_result(self, df_in=None, df_out=None) -> None:
        logger.info(f"Set ({', '.join(self.fields_index)}) as DataFrame index")


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
    field_referral_urlhost: FieldSnowplow = FieldSnowplow.REFR_URLHOST
    field_referral_medium: FieldSnowplow = FieldSnowplow.REFR_MEDIUM

    # Private variables
    _num_false_positives: int = dataclass_field(default=0, repr=False)
    _num_false_negatives: int = dataclass_field(default=0, repr=False)

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
            f"Replaced referral medium of {self._num_false_negatives} ({self._num_false_negatives / df_in.shape[0]:.1%}) non-null-medium events to 'no_referrer' and "
            + f"of {self._num_false_positives} ({self._num_false_positives / df_in.shape[0]:.1%}) of previously null-medium events to 'unknown'"
        )

    def _reclassify(self, event: pd.Series) -> EventReferrerMedium:
        """
        Row-level main reclassification logic.
        """
        referrer_medium = event.at[self.field_referral_medium]
        referrer_urlhost = event.at[self.field_referral_urlhost]

        referrer_medium_is_truthy = bool(referrer_medium)
        referrer_url_is_valid = referrer_urlhost is not None and len(referrer_urlhost) > 0

        if not referrer_url_is_valid and not referrer_medium_is_truthy:
            # True positive: Medium is labeled NULL, and it in fact is NULL
            # Just need to convert to our custom enum value for NULL
            return EventReferrerMedium.NO_REFERRER
        elif not referrer_url_is_valid and referrer_medium_is_truthy:
            # False negative: Medium is labeled not NULL, but it in fact is NULL
            self._num_false_negatives += 1
            return EventReferrerMedium.NO_REFERRER
        elif referrer_url_is_valid and not referrer_medium_is_truthy:
            # False positive: Medium is labeled NULL, but it in fact is not NULL
            # There's probably a better way to handle false positives than assigning unknown,
            # but given the small percentage of this happening (< 0.1% of all aggregated events),
            # this is fine for now
            self._num_false_positives += 1
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
    field_refferal_urlhost: FieldSnowplow = FieldSnowplow.REFR_URLHOST
    field_referral_medium: FieldSnowplow = FieldSnowplow.REFR_MEDIUM

    # Private variables
    _num_false_positives: int = dataclass_field(default=0, repr=False)
    _num_false_negatives: int = dataclass_field(default=0, repr=False)

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
            f"Replaced referral medium of {self._num_false_negatives} ({self._num_false_negatives / df_in.shape[0]:.1%}) events to 'internal'."
        )

    def _reclassify(self, event: pd.Series) -> EventReferrerMedium:
        """
        Row-level main reclassification logic.
        """
        referrer_medium: EventReferrerMedium = event.at[self.field_referral_medium]

        # This means this preprocessor should be placed after ReclassifyNullReferrals
        if referrer_medium == EventReferrerMedium.NO_REFERRER:
            return referrer_medium

        referrer_urlhost = event.at[self.field_refferal_urlhost]
        referrer_url_matches_domain = any(
            [
                referrer_urlhost is not None and domain.pattern.search(referrer_urlhost) is not None
                for domain in self.site_domains
            ]
        )

        if referrer_url_matches_domain and referrer_medium != EventReferrerMedium.INTERNAL:
            # Handle false negatives: medium is labeled not internal, but it in fact is
            self._num_false_negatives += 1
            return EventReferrerMedium.INTERNAL
        elif not referrer_url_matches_domain and referrer_medium == EventReferrerMedium.INTERNAL:
            # False positive: medium is labeled internal, but it in fact is not
            # Dealing with this is complicated, so leaving it as-is for now
            return referrer_medium
        else:
            return referrer_medium


@dataclass
class AddFieldLeadsToNewsletterConversion(Preprocessor):
    """
    Adds the target-variable column showing whether (aggregated) page-view
    event leads to newsletter conversion.

    Why? Because, for example, Dallas Free Press only has the actual newsletter
    form in its dedicated newsletter page. In other pages, such as articles,
    it shows an ask with a hyperlink to said newsletter page. If we use the
    `form_submit_is_newsletter` column as the target variable, it will only
    be True for events happening in said newsletter page; then any model that
    trains itself on these events will just prescribe True for events happening
    in a newsletter-dedicated page and False everywhere else, which is trivial.

    What we really want is to label the page (e.g. article) that _leads_ to the
    newsletter-page view where the newsletter form submission happens. Other sites
    may have pages that are not dedicated to newsletter sign-up (in which
    case we can just use `form_submit_is_newsletter`), but they also have
    dedicated newsletter pages.

    To identify the page-view event that leads to newsletter page view where the
    newsletter-form submission happens, we primarily look at the latter's
    referral URL.

    The logic is as follows:
    - First, query for page-view events where `form_submit_is_newsletter` is True
    - For each of these events,
        - Identify the page-view event that leads to it:
            - If the event happens in a non-newsletter-dedicated page, i.e.,
            `page_is_newsletter` is False, _make it the leading event_
            - Else, i.e., `page_is_newsletter` is True, try to identify the
            leading event:
                - If the original event's referral medium is NOT internal, traverse
                through precending events (a.k.a. predecessor) IN THE SAME SESSION,
                starting from most recent. For every predecessor:
                    - If predecessor's page is not a newsletter page,
                    _make predecessor leading_
                    - Else, continue to next predecessor
                - Else, i.e., the original event's referral medium is internal,
                    - Initialize some memo variable X as None
                    - Traverse through precending events (a.k.a. predecessor) IN THE SAME
                    (AND, IF SPECIFIED, PREVIOUS SESSIONS), starting from most recent.
                    For every predecessor:
                        - If [predecessor's page is not a newsletter page] and [predecessor
                        is in the same session as original] and [X is None],
                            - Assign predecessor to X so that it's the most recent event
                            happening in a non-newsletter-dedicated page and our default leading
                            event (more below).
                        - If [original's referrer points to a newsletter-dedicated page] and
                        [X is NOT None], _make X leading_.
                        - If [original's referrer does NOT point to a newsletter-dedicated page] and
                        [predecessor's page_urlpath matches (with total or enough confidence) original's
                        referral URL path], _make predecessor leading_.
                        - If predecessor is the first event of its session:
                            - If predecessor's referral medium is NOT internal, _make X leading_.
                        - Else, continue to next predecessor
        - Mark said identified event's target column as True
    """

    site_page_type_classifier: SitePageClassifier
    inspect_previous_sessions: bool = False  # When identifying the leading preprocessor of an internally referred event, whether to traverse to its previous session(s)
    field_event_id: FieldSnowplow = FieldSnowplow.EVENT_ID
    field_timestamp: FieldSnowplow = FieldSnowplow.DERIVED_TSTAMP
    field_user_id: FieldSnowplow = FieldSnowplow.DOMAIN_USERID
    field_user_session_idx: FieldSnowplow = FieldSnowplow.DOMAIN_SESSIONIDX
    field_referrer_urlpath: FieldSnowplow = FieldSnowplow.REFR_URLPATH
    field_referral_medium: FieldSnowplow = FieldSnowplow.REFR_MEDIUM
    field_page_urlpath: FieldSnowplow = FieldSnowplow.PAGE_URLPATH
    field_user_session_event_idx: FieldNew = FieldNew.DOMAIN_SESSION_EVENTIDX
    field_form_submit_is_newsletter: FieldNew = FieldNew.FORM_SUBMIT_IS_NEWSLETTER
    field_page_is_newsletter: FieldNew = FieldNew.PAGE_IS_NEWSLETTER
    field_leads_to_newsletter_conversion: FieldNew = FieldNew.LEAD_TO_NEWSLETTER_CONVERSION
    field_newsletter_leading_event: FieldNew = FieldNew.NEWSLETTER_LEADING_EVENT

    # Private variables
    _num_leading_events: int = dataclass_field(default=0, repr=False)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # Index should already be changed to user-session-event MultiIndex and
        # sorted in ascending order by SortRowIndex.

        # Create new target-label field, first set everything to False
        df[self.field_leads_to_newsletter_conversion] = False

        # Create new newsletter-leading-event ID field, first set everything to NaN
        df[self.field_newsletter_leading_event] = np.nan

        # Get a list of indices of events where a newsletter-form submission happens
        indices_newsletter_submission = df.query(self.field_form_submit_is_newsletter).index

        # Iterate through indices
        for event_index in indices_newsletter_submission:
            event_leading_index = self._identify_leading_event(df, event_index)
            if event_leading_index is not None:
                # Mark idenfitied predecessor as leading
                df.at[event_leading_index, self.field_leads_to_newsletter_conversion] = True
                # Mark leading ID in original event
                df.at[event_index, self.field_newsletter_leading_event] = df.at[
                    event_leading_index, self.field_event_id
                ]
                # Increment count
                self._num_leading_events += 1

        # Force type of newsletter-leading-event ID field to str in case they're all NaNs
        df[self.field_newsletter_leading_event] = df[self.field_newsletter_leading_event].astype(str)

        return df

    def _identify_leading_event(
        self, df: pd.DataFrame, event_index: Tuple[str, int, int]
    ) -> Optional[Tuple[str, int, int]]:
        """
        Main leading-event identification logic. Returns a
        (domain_userid, domain_sessionidx, domain_session_event) tuple.
        """
        # If the event happens in a non-newsletter-dedicated page, make it the leading event
        # and move on to next index
        # pandas store boolean values not as Python bool but of numpy.bool_.
        # (see: https://stackoverflow.com/questions/46002513/numpy-any-returns-true-but-is-true-comparison-fails)
        if df.at[event_index, self.field_page_is_newsletter] is np.False_:
            return event_index

        if df.at[event_index, self.field_referral_medium] == EventReferrerMedium.INTERNAL:
            return self._identify_leading_event_internally_referred(df, event_index)
        else:
            return self._identify_leading_event_externally_referred(df, event_index)

    def _identify_leading_event_externally_referred(
        self, df: pd.DataFrame, event_index: Tuple[str, int, int]
    ) -> Optional[Tuple[str, int, int]]:
        """
        Leading-event identification logic as applied to an externally
        (non-internally) referred event.
        """
        # Get individual index components
        user_id, user_session_idx, original_user_session_event_idx = event_index

        # Filter & reverse user-session-event indices to those before the original event
        # e.g. if original indices are [1, 2, 3, 4, 5] and event index is 4,
        # return [3, 2, 1]
        # MultiIndex should already have been sorted in ascending order
        for predecessor_user_session_event_idx in range(original_user_session_event_idx - 1, 0, -1):
            # Looking only at events from the same user and session
            predecessor_index = (user_id, user_session_idx, predecessor_user_session_event_idx)

            # If predecessor's page is not newsletter-dedicated-page, make it the leading event
            if df.at[predecessor_index, self.field_page_is_newsletter] is np.False_:
                return predecessor_index

    def _identify_leading_event_internally_referred(
        self, df: pd.DataFrame, event_index: Tuple[str, int, int]
    ) -> Optional[Tuple[str, int, int]]:
        """
        Leading-event identification logic as applied to an internally referred event.
        """
        original_index = event_index

        # Get individual index components
        user_id, original_user_session_idx, original_user_session_event_idx = original_index

        # Original's referral URL path
        original_referral_urlpath = df.at[original_index, self.field_referrer_urlpath]

        # Whether original's referral URL path points to a newsletter-dedicated page.
        # We create a small pandas Series mocking an event with timestamp & URL path and
        # pass it to the page-type classifier, which expects an event. If this is use case
        # becomes more frequent, the page-type classifier could be redesigned to make
        # it feel a little less hacky
        original_referral_urlpath_is_newsletter = self.site_page_type_classifier.is_newsletter(
            pd.Series(
                {
                    self.field_timestamp: df.at[original_index, self.field_timestamp],
                    self.field_page_urlpath: original_referral_urlpath,
                }
            )
        )

        # Default index
        default_index = None

        # MultiIndex should already have been sorted in ascending order
        for predecessor_user_session_idx, predecessor_user_session_event_idx in reversed(df.loc[user_id].index):
            predecessor_is_later_session = predecessor_user_session_idx > original_user_session_idx
            predecessor_is_same_session = predecessor_user_session_idx == original_user_session_idx
            predecessor_is_previous_session = predecessor_user_session_idx < original_user_session_idx

            # If predecessor doesn't happen before original, it's not really a
            # predecessor, isn't it?
            if predecessor_is_later_session or (
                predecessor_is_same_session and predecessor_user_session_event_idx >= original_user_session_event_idx
            ):
                continue

            # If predecessor happens in a previous session, and we choose not to
            # inspect previous sessions, we're unable to find a leading event
            if not self.inspect_previous_sessions and predecessor_is_previous_session:
                return

            # Predecessor's full index
            predecessor_index = (user_id, predecessor_user_session_idx, predecessor_user_session_event_idx)

            # Assign default if it isn't already assigned
            if (
                default_index is None
                and predecessor_is_same_session
                and df.at[predecessor_index, self.field_page_is_newsletter] is np.False_
            ):
                default_index = predecessor_index

            # Predecessor's URL path
            predecessor_urlpath = df.at[predecessor_index, self.field_page_urlpath]

            # If original's referral path points to a newsletter-dedicated page (i.e.,
            # we can't get any meaningful information out of it) and a default
            # preprocessor exists, make it leading
            if original_referral_urlpath_is_newsletter and default_index is not None:
                return default_index

            # If original's referral path does not point to a newsletter-dedicated page
            # and is the same as predecessor's path, make predecessor leading
            if (
                not original_referral_urlpath_is_newsletter
                and self.compare_urlpaths(original_referral_urlpath, predecessor_urlpath) is True
            ):
                return predecessor_index

            # Else, if predecessor is first event of its session and its referrer
            # medium is not internal, make default preprocessor leading index (even if
            # default index is None); else move on to next predecessor
            if (
                predecessor_user_session_event_idx == 1
                and df.at[predecessor_index, self.field_referral_medium] != EventReferrerMedium.INTERNAL
            ):
                return default_index

    @staticmethod
    def compare_urlpaths(urlpath_a: str, urlpath_b: str) -> bool:
        # Could use something more robust than hard equality check,
        # perhaps some similarity metric such as Levenshtein or Jaro-Winkler?
        # Although, if we do our due diligence in standardizing these URL
        # paths (either with pydantic or other preprocessors), simple
        # equality check is enough
        return append_slash(urlpath_a) == append_slash(urlpath_b)

    def log_result(self, df_in: pd.DataFrame, df_out: pd.DataFrame) -> None:
        logger.info(
            f"Identified {self._num_leading_events} events as leading to a newsletter subscription. "
            + f"They account for {self._num_leading_events / df_out.shape[0]:.1%} of all aggregated page events "
            + f"and {self._num_leading_events / df_in[self.field_form_submit_is_newsletter].sum():.1%} of aggregated page events where a newsletter form submission happens"
        )


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
            FieldSnowplow.BR_VIEWHEIGHT,
            FieldSnowplow.BR_VIEWWIDTH,
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
