from datetime import datetime
from typing import Sequence

import pandas as pd
from ata_db_models.models import Event
from sqlalchemy.orm import sessionmaker
from sqlmodel import select

from ata_pipeline1.helpers.enums import EventName, SiteName


def fetch_events(
    site_name: SiteName, event_types: Sequence[EventName], start: datetime, end: datetime, session_factory: sessionmaker
) -> pd.DataFrame:
    """
    Grabs event data from postgres for a given site, event types, and datetime range.
    The site_name is here for convenience; in production, postgres roles with RLS enabled will automatically filter
    to make sure a given compute process only accesses the site it is allowed to access.
    """
    with session_factory.begin() as session:  # type: ignore
        statement = select(Event).where(
            Event.site_name == site_name,
            Event.derived_tstamp >= start,  # type: ignore
            Event.derived_tstamp < end,
            Event.event_name.in_(event_types),
        )
        results = session.execute(statement)

    # convert to dataframe
    return pd.DataFrame(data=[event.dict() for event in results])
