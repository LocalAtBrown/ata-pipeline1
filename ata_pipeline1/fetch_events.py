from datetime import datetime, timedelta
from typing import Sequence

import pandas as pd
from ata_db_models.models import Event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, aliased
from sqlalchemy.sql.functions import max as sqlmax
from sqlalchemy.sql.functions import min as sqlmin
from sqlmodel import select

from ata_pipeline1.helpers.enums import EventName
from ata_pipeline1.site.names import SiteName


def fetch_events(
    site_name: SiteName, event_types: Sequence[EventName], start: datetime, end: datetime, engine: Engine
) -> pd.DataFrame:
    """
    Grabs event data from postgres for a given site, event types, and datetime range.
    The site_name is here for convenience; in production, postgres roles with RLS enabled will automatically filter
    to make sure a given compute process only accesses the site it is allowed to access.
    """
    with Session(engine) as session:
        statement = select(Event).where(
            Event.site_name == site_name,
            Event.derived_tstamp >= start,
            Event.derived_tstamp < end,
            Event.event_name.in_(event_types),
        )
        results = session.execute(statement)
        data = [event[0].dict() for event in results]

    # convert to dataframe
    return pd.DataFrame(data=data)


def alt_fetch_events(
    site_name: SiteName, start: datetime, end: datetime, engine: Engine, mins_between_sessions: int = 30
) -> pd.DataFrame:
    """
    Grabs event data from postgres for a given site and datetime range. Gets all sessions that start during or after
    the start time and end by the end time.
    """
    with Session(engine) as session:
        # Select all users and session indices between start and end timestamps
        statement1 = (
            select(
                Event.domain_userid,
                sqlmin(Event.domain_sessionidx).label("minidx"),
                sqlmax(Event.domain_sessionidx).label("maxidx"),
            )
            .where(Event.site_name == site_name, Event.derived_tstamp >= start, Event.derived_tstamp < end)
            .group_by(Event.domain_userid)
        )
        sub1 = statement1.subquery("s1")

        # Select all events associated with said users and whose session index is between
        # respective user's min and max session indices (as returned above)
        statement2 = (
            select(Event)
            .join(
                sub1,
                (Event.domain_userid == sub1.c.domain_userid)
                & (Event.domain_sessionidx >= sub1.c.minidx)
                & (Event.domain_sessionidx <= sub1.c.maxidx),
            )
            .where(
                Event.site_name == site_name,
            )
            .order_by(Event.derived_tstamp)
        )
        sub2 = aliased(Event, statement2.subquery("s2"))

        # Select users whose max session index is ongoing by the end timestamp
        # If the last event of that session happens less than 30 minutes before
        # the end timestamp, that session is still ongoing. If it happens more than
        # 30 minutes before, that session has finished
        statement3 = (
            select(sub2.domain_userid, sqlmax(sub2.domain_sessionidx).label("maxidx"))
            .group_by(sub2.domain_userid)
            .having(sqlmax(sub2.derived_tstamp) >= end - timedelta(minutes=mins_between_sessions))
        )
        sub3 = statement3.subquery("s3")

        # Remove events associated with ongoing sessions from the events returned
        # by statement2 because we're only interested in completed/finished sessions.
        statement4 = (
            select(sub2)
            .join(sub3, sub2.domain_userid == sub3.c.domain_userid, isouter=True)
            .where((sub3.c.maxidx == None) | (sub2.domain_sessionidx < sub3.c.maxidx))  # noqa: E711
        )

        # Execute query
        results = session.execute(statement4)
        data = [event[0].dict() for event in results]

    return pd.DataFrame(data=data)
