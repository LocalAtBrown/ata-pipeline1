from datetime import datetime
from typing import Sequence

import pandas as pd
from ata_db_models.models import Event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from sqlalchemy.sql.functions import max as sqlmax
from sqlalchemy.sql.functions import min as sqlmin
from sqlmodel import select

from ata_pipeline1.helpers.enums import EventName, SiteName


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


def alt_fetch_events(site_name: SiteName, start: datetime, end: datetime, engine: Engine) -> pd.DataFrame:
    """
    Grabs event data from postgres for a given site and datetime range. Gets all sessions that start during or after
    the start time and end by the end time.
    """
    with Session(engine) as session:
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
        joined = (
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
        results = session.execute(joined)
        data = [event[0].dict() for event in results]

    return pd.DataFrame(data=data)
