import ast
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, Dict, Set, Tuple, Union

import numpy as np
import pandas as pd
import user_agents as ua

from ata_pipeline1.helpers.events import EventName
from ata_pipeline1.helpers.fields import Field, FieldNew, FieldPreAgg, FieldSnowplow
from ata_pipeline1.helpers.logging import logging
from ata_pipeline1.site.names import SiteName
from ata_pipeline1.site.newsletter import SiteNewsletterSignupValidator

logger = logging.getLogger(__name__)


class Preprocessor(ABC):
    """
    Base preprocessor abstract class. Its children should be dataclasses storing
    variables needed for the specific transformation.
    """

    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calls an instance of a child class as if it's a (preprocessing) function.
        """
        df_out = self.transform(df)
        self.log_result(df, df_out)
        return df_out

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms a Snowplow DataFrame using parameters predefined in the dataclass.
        """
        pass

    @abstractmethod
    def log_result(self, df_in: pd.DataFrame, df_out: pd.DataFrame) -> None:
        """
        Logs useful post-transformation messages.
        """


# ---------- NEW PREPROCESSORS ----------
@dataclass
class AddFieldFormSubmitIsNewsletter(Preprocessor):
    """
    Adds a field indicating whether a form-submission event is of a newsletter-signup form.
    """

    site_newsletter_signup_validator: SiteNewsletterSignupValidator
    field_event_name: FieldSnowplow
    field_form_submit_is_newsletter: FieldNew

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
        if event[self.field_event_name] != EventName.SUBMIT_FORM:
            return np.nan

        return self.site_newsletter_signup_validator.validate(event)


@dataclass
class AddFieldDeviceIsMobile(Preprocessor):
    """
    Adds a new column indicating whether device where event was recorded is mobile.
    """

    field_useragent: FieldSnowplow
    field_device_is_mobile: FieldNew

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

    field_offset_y: FieldSnowplow
    field_doc_height: FieldSnowplow
    field_max_scroll_depth: FieldNew

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        df[self.field_max_scroll_depth] = (df[self.field_offset_y] / df[self.field_doc_height]).clip(0, 1)

        return df

    def log_result(self, df_in=None, df_out=None) -> None:
        logger.info("Calculated scroll depths and added them to a new field")


@dataclass
class SortFieldTimestamp(Preprocessor):
    """
    Sorts events by ascending timestamp.
    """

    field_timestamp: FieldSnowplow

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.sort_values(self.field_timestamp)

    def log_result(self, df_in=None, df_out=None) -> None:
        logger.info("Sorted events by ascending timestamp")


@dataclass
class AddFieldEventParentId(Preprocessor):
    """
    Adds a new field specifying the first page-view event (for a user) that
    predates all of that user's events within that page in the same session.
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

    field_timestamp: FieldSnowplow
    field_user_id: FieldSnowplow
    field_user_session_idx: FieldSnowplow
    field_page_urlpath: FieldSnowplow
    field_event_id: FieldSnowplow
    field_event_name: FieldSnowplow
    field_site_name: FieldNew
    field_event_parent_id: FieldNew
    PING_INTERVAL_SECONDS = 10
    PING_INTERVAL_NOISE_SECONDS = 1

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df_event_parent_mapping = df.groupby(
            [self.field_site_name, self.field_user_id, self.field_user_session_idx, self.field_page_urlpath],
            group_keys=False,
        ).apply(self._get_parent_events)

        return df.merge(df_event_parent_mapping, on=[self.field_site_name, self.field_event_id])

    # def get_parent_events_cleaner_but_twice_as_slow(df_group: pd.DataFrame):
    #     result = (
    #         df_group[[FieldSnowplow.DERIVED_TSTAMP, FieldSnowplow.EVENT_ID, FieldSnowplow.EVENT_NAME]]
    #         .sort_values(FieldSnowplow.DERIVED_TSTAMP)
    #         .assign(
    #             seconds_from_previous_event=lambda df: (
    #                 (df[FieldSnowplow.DERIVED_TSTAMP].astype(int) / 1e9)
    #                 .rolling(2)
    #                 .apply(lambda x: 0 if len(x) <= 1 else x.iloc[1] - x.iloc[0])
    #             )
    #         )
    #         .assign(
    #             event_parent_id=lambda df: df.apply(
    #                 lambda x: x[FieldSnowplow.EVENT_ID]
    #                 if x["seconds_from_previous_event"] > PING_INTERVAL_SECONDS + PING_INTERVAL_EPSILON_SECONDS
    #                 or np.isnan(x["seconds_from_previous_event"])
    #                 or x[FieldSnowplow.EVENT_NAME] == Event.PAGE_VIEW
    #                 else np.nan,
    #                 axis=1,
    #             ).ffill()
    #         )
    #     )
    #     return result.drop(columns=["seconds_from_previous_event"])

    def _get_parent_events(self, df_group: pd.DataFrame) -> pd.DataFrame:
        """
        Determines parent events among events within a pandas GroupBy object.
        """
        # First, extract only columns we need
        # ROWS SHOULD ALREADY HAVE BEEN SORTED
        df_group = df_group[[self.field_site_name, self.field_timestamp, self.field_event_id, self.field_event_name]]

        # Pointer: timestamp of previous event
        timestamp_prev = df_group[self.field_timestamp].iloc[0]
        # Current event ID
        event_id = df_group[self.field_event_id].iloc[0]
        # Current parent ID
        event_parent_id = event_id

        results = []

        # Iterate over sorted events
        for site_name, timestamp, event_id, event_name in df_group.values:
            # If current event is a page view or time from previous event exceeds ping interval
            # (with added noise), also indicating a page view, we have a new parent
            if (
                event_name == EventName.PAGE_VIEW
                or (timestamp - timestamp_prev).total_seconds()
                > self.PING_INTERVAL_SECONDS + self.PING_INTERVAL_NOISE_SECONDS
            ):
                # Assign current event to current parent
                event_parent_id = event_id

            # Append site name and event ID in order to accurately join (merge) output
            # DataFrame with original DataFrame
            results.append((site_name, event_id, event_parent_id))

            # Update timestamp of previous as current timestamp before moving on to
            # next event (and next timestamp)
            timestamp_prev = timestamp

        # Return result as mergeable DataFrame
        return pd.DataFrame(results, columns=[self.field_site_name, self.field_event_id, self.field_event_parent_id])

    def log_result(self, df_in, df_out) -> None:
        logger.info(f"Found and added {df_out[self.field_event_parent_id].nunique()} parent events as a new field.")


@dataclass
class AggregatePageActivities(Preprocessor):
    """
    Combine events of the same parent and creates (or replaces existing fields with)
    new fields with aggregation/summary statistics (e.g., max scroll depth, dwell time).
    """

    field_event_parent_id: FieldNew
    agg_funcs: Dict[Field, Tuple[FieldPreAgg, Union[Callable, str]]]

    # TODO: When aggregating, create a new column showing index of visit within session
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.groupby(self.field_event_parent_id).aggregate(**self._create_named_agg_objects())

    def _create_named_agg_objects(self) -> Dict[Field, pd.NamedAgg]:
        return {
            field_agg: pd.NamedAgg(column=field_basis, aggfunc=func)
            for field_agg, (field_basis, func) in self.agg_funcs.items()
        }

    def log_result(self, df_in, df_out) -> None:
        pass


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

    fields_int: Set[FieldSnowplow]
    fields_float: Set[FieldSnowplow]
    fields_datetime: Set[FieldSnowplow]
    fields_categorical: Set[FieldSnowplow]
    fields_json: Set[FieldSnowplow]

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


@dataclass
class ReplaceNaNs(Preprocessor):
    """
    Replaces all `np.nan` instances in the DataFrame with a specified alternative.
    """

    replace_with: Any

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.replace([np.nan], [self.replace_with])

        return df

    def log_result(self, df_in: pd.DataFrame, df_out: pd.DataFrame) -> None:
        logger.info(f"Replaced all NaNs with {self.replace_with}")
