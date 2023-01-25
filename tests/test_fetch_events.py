from datetime import datetime, timedelta
from uuid import uuid4

import pytest
from ata_db_models.models import Event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from ata_pipeline1.fetch_events import fetch_events
from ata_pipeline1.helpers.enums import EventName, SiteName
from tests.helpers import create_and_drop_tables


@pytest.fixture
def single_event(current_timestamp: datetime) -> Event:
    return Event(
        site_name=SiteName.AFRO_LA,
        derived_tstamp=current_timestamp,
        doc_height=100,
        domain_sessionidx=1,
        domain_userid=uuid4(),
        dvce_screenheight=100,
        dvce_screenwidth=100,
        event_id=uuid4(),
        event_name=EventName.PAGE_PING,
        network_userid=uuid4(),
        page_urlpath="/",
        page_referrer="https://www.google.com/",
        unstruct_event_com_snowplowanalytics_snowplow_change_form_1=None,
        unstruct_event_com_snowplowanalytics_snowplow_focus_form_1=None,
        unstruct_event_com_snowplowanalytics_snowplow_submit_form_1=None,
        useragent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    )


def test_fetch_events(
    single_event: Event, current_timestamp: datetime, engine: Engine, session_factory: sessionmaker
) -> None:
    # need to grab dict from event before putting in db, session and sqlmodel object mutate over time
    event_as_dict = single_event.dict()
    # create table in db
    with create_and_drop_tables(engine):
        # prepop with valid data
        with session_factory.begin() as session:  # type: ignore
            session.add(single_event)

        # use the fn to fetch
        df = fetch_events(
            site_name=SiteName.AFRO_LA,
            event_types=[EventName.PAGE_PING],
            start=current_timestamp - timedelta(minutes=5),
            end=current_timestamp + timedelta(minutes=5),
            session_factory=session_factory,
        )

        data = df.to_dict(orient="records")

    # assert it is as desired
    assert event_as_dict == data[0]


def test_fetch_events_wrong_site(
    single_event: Event, current_timestamp: datetime, engine: Engine, session_factory: sessionmaker
) -> None:
    # create table in db
    with create_and_drop_tables(engine):
        # prepop with valid data
        with session_factory.begin() as session:  # type: ignore
            session.add(single_event)

        # use the fn to fetch
        df = fetch_events(
            site_name=SiteName.DALLAS_FREE_PRESS,
            event_types=[EventName.PAGE_PING],
            start=current_timestamp - timedelta(minutes=5),
            end=current_timestamp + timedelta(minutes=5),
            session_factory=session_factory,
        )

    # assert it is as desired
    assert df.empty


def test_fetch_events_wrong_events(
    single_event: Event, current_timestamp: datetime, engine: Engine, session_factory: sessionmaker
) -> None:
    # create table in db
    with create_and_drop_tables(engine):
        # prepop with valid data
        with session_factory.begin() as session:  # type: ignore
            session.add(single_event)

        # use the fn to fetch
        df = fetch_events(
            site_name=SiteName.AFRO_LA,
            event_types=[EventName.PAGE_VIEW],
            start=current_timestamp - timedelta(minutes=5),
            end=current_timestamp + timedelta(minutes=5),
            session_factory=session_factory,
        )

    # assert it is as desired
    assert df.empty


def test_fetch_events_wrong_time(
    single_event: Event, current_timestamp: datetime, engine: Engine, session_factory: sessionmaker
) -> None:
    # create table in db
    with create_and_drop_tables(engine):
        # prepop with valid data
        with session_factory.begin() as session:  # type: ignore
            session.add(single_event)

        # use the fn to fetch
        df = fetch_events(
            site_name=SiteName.AFRO_LA,
            event_types=[EventName.PAGE_PING],
            start=current_timestamp + timedelta(minutes=5),
            end=current_timestamp + timedelta(minutes=10),
            session_factory=session_factory,
        )

    # assert it is as desired
    assert df.empty
