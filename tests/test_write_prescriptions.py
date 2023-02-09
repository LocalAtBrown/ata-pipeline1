from datetime import datetime
from uuid import uuid4

import pandas as pd
import pytest
from ata_db_models.models import Prescription
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from sqlmodel import select

from ata_pipeline1.helpers.enums import SiteName
from ata_pipeline1.write_prescriptions import write_prescriptions
from tests.helpers import create_and_drop_tables


@pytest.fixture
def prescriptions() -> list[Prescription]:
    return [
        Prescription(user_id=uuid4(), site_name=SiteName.AFRO_LA, prescribe=True, last_updated=datetime.now())
        for _ in range(3)
    ]


@pytest.fixture
def prescriptions_as_df(prescriptions: list[Prescription]) -> pd.DataFrame:
    return pd.DataFrame(data=[p.dict() for p in prescriptions])


def test_write_prescriptions(prescriptions_as_df: pd.DataFrame, engine: Engine) -> None:
    with create_and_drop_tables(engine):
        rows_written = write_prescriptions(df=prescriptions_as_df, engine=engine)
        assert rows_written == prescriptions_as_df.shape[0]
        with Session(engine) as session:
            statement = select(Prescription)
            results = session.execute(statement)
            data = [prescription[0].dict() for prescription in results]
    # order the columns the same way as prescriptions_as_df
    from_db = pd.DataFrame(data=data)[["user_id", "site_name", "prescribe", "last_updated"]]
    assert prescriptions_as_df.equals(from_db)


def test_write_prescriptions_no_data(engine: Engine) -> None:
    with create_and_drop_tables(engine):
        rows_written = write_prescriptions(df=pd.DataFrame(), engine=engine)
        assert rows_written == 0
