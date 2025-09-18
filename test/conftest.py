import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os
from dotenv import load_dotenv

load_dotenv()

@pytest.fixture(scope="module")
def db_engine():
    engine = create_engine(os.getenv("POSTGRES_DATABASE"), isolation_level="SERIALIZABLE")
    yield engine
    engine.dispose()

@pytest.fixture(scope="function")
def db_connection_rollback(db_engine):
    """
    Crée une connexion avec une transaction explicite
    et rollback après le test (compat NeonDB).
    """
    connection = db_engine.connect()
    transaction = connection.begin()  # démarre une vraie transaction explicite

    yield connection

    transaction.rollback()
    connection.close()