from collections.abc import Generator
from contextlib import contextmanager

from ata_db_models.models import SQLModel
from sqlalchemy.engine import Engine


@contextmanager
def create_and_drop_tables(engine: Engine) -> Generator[None, None, None]:
    """
    Context manager to safely create and drop tables before and after each test.
    """
    SQLModel.metadata.create_all(engine)
    try:
        yield
    finally:
        SQLModel.metadata.drop_all(engine)
