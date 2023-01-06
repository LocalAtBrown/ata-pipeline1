from datetime import datetime
from typing import Sequence

from ata_pipeline1.helpers.enums import EventName, SiteName


def fetch_events(site: SiteName, event_types: Sequence[EventName], start: datetime, end: datetime):
    """
    Grabs event data from postgres for a given site, event types, and datetime range.
    """
    pass
