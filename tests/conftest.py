from datetime import datetime

import pytest
from ata_db_models.helpers import get_conn_string
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


@pytest.fixture(scope="module")
def db_name() -> str:
    # Assuming the local DB name is "postgres"
    return "postgres"


@pytest.fixture(scope="module")
def engine(db_name: str) -> Engine:
    return create_engine(get_conn_string(db_name))


@pytest.fixture
def current_timestamp() -> datetime:
    return datetime.now()
