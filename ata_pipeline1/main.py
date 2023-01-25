import os
from datetime import datetime, timedelta

from ata_db_models.helpers import get_conn_string
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from ata_pipeline1.fetch_events import fetch_events
from ata_pipeline1.helpers.enums import EventName
from ata_pipeline1.process import process
from ata_pipeline1.write_prescriptions import write_prescriptions


def run() -> None:
    engine = create_engine(get_conn_string())
    session_factory = sessionmaker(engine)

    site_name = os.getenv("PARTNER", "")

    # TODO can change how we set this, this is just a placeholder
    end = datetime.now()
    start = end - timedelta(days=7)

    events = fetch_events(
        site_name=site_name,  # type: ignore
        event_types=[
            EventName.PAGE_VIEW,
            EventName.PAGE_PING,
            EventName.CHANGE_FORM,
            EventName.FOCUS_FORM,
            EventName.SUBMIT_FORM,
        ],
        start=start,
        end=end,
        session_factory=session_factory,
    )
    prescriptions = process(events)
    write_prescriptions(prescriptions, session_factory)
