from datetime import datetime
from uuid import UUID, uuid4

import pytest
from ata_db_models.models import Event, Prescription
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import select

from ata_pipeline1.helpers.enums import EventName, SiteName
from ata_pipeline1.main import run
from tests.helpers import create_and_drop_tables


@pytest.fixture
def many_uuids() -> list[UUID]:
    return [uuid4() for _ in range(10)]


@pytest.fixture
def many_events(current_timestamp: datetime, many_uuids: list[UUID]) -> list[Event]:
    return [
        Event(
            site_name=SiteName.AFRO_LA,
            derived_tstamp=current_timestamp,
            doc_height=100,
            domain_sessionidx=1,
            domain_userid=uuid4(),
            dvce_screenheight=100,
            dvce_screenwidth=100,
            event_id=uuid4(),
            event_name=EventName.PAGE_PING,
            network_userid=network_userid,
            page_urlpath="/",
            page_referrer="https://www.google.com/",
            unstruct_event_com_snowplowanalytics_snowplow_change_form_1=None,
            unstruct_event_com_snowplowanalytics_snowplow_focus_form_1=None,
            unstruct_event_com_snowplowanalytics_snowplow_submit_form_1=None,
            useragent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
        )
        for network_userid in many_uuids
    ]


def test_run(many_events: list[Event], many_uuids: list[UUID], engine: Engine, session_factory: sessionmaker) -> None:
    # set up db
    with create_and_drop_tables(engine):
        with session_factory.begin() as session:  # type: ignore
            # put in test data
            session.add_all(many_events)
            session.commit()

        # run program
        run()

        # verify info in Prescription
        with session_factory.begin() as session:  # type: ignore
            statement = select(Prescription)
            results = session.execute(statement)
            prescriptions = [prescription[0] for prescription in results]

            user_ids = [prescription.user_id for prescription in prescriptions]
            prescribes = [prescription.prescribe for prescription in prescriptions]
            # expect all user ids to be present
            assert sorted(user_ids) == sorted(many_uuids)
            # given current logic, expect every prescription to be True
            assert all(prescribes)
