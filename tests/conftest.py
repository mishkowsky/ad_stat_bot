import os
import signal
import pytest
import sqlalchemy
from loguru import logger
from pytest_postgresql import factories
from pytest_postgresql.janitor import DatabaseJanitor
from sqlalchemy import create_engine, Connection
from sqlalchemy.orm import sessionmaker
import src
from config import TEST_LOGGER_LEVEL
from src.dao.db_config import DBConfig, DBConfigInstance
from src.dao.mentions_db import Base as MentionsBase
from src.dao.users_db import Base as UsersBase

test_db = factories.postgresql_proc(port=None, dbname='test_db')


@pytest.fixture(scope='function')
def db_session(monkeypatch, test_db):
    """
    establishes connection to test_db
    :param monkeypatch: fixture to patch db config variables
    :param test_db: fixture with postgresql process properties
    """

    if os.name == 'nt':
        os.killpg = killpg_windows

    patched = DBConfigInstance(
        DBConfig(
            DBMS='postgresql',
            DRIVER='psycopg2',
            HOSTNAME=test_db.host,
            DATABASE=test_db.dbname,
            USERNAME=test_db.user,
            PASSWORD=test_db.password,
            PORT=test_db.port,
            config_name='debugging_config'
        ))
    monkeypatch.setattr('src.dao.db_config.DB_CONFIG', patched)

    with DatabaseJanitor(test_db.user, test_db.host, test_db.port, test_db.dbname, test_db.version, test_db.password):
        engine = create_engine(src.dao.db_config.DB_CONFIG.DB_URI)
        with engine.connect() as con:
            create_schema(con, 'users')
            create_schema(con, 'mentions')
            MentionsBase.metadata.create_all(con)
            UsersBase.metadata.create_all(con)
            con.commit()
            monkeypatch.setattr('src.dao.db_config.SessionLocal',
                                sessionmaker(bind=engine, autocommit=False, autoflush=False, expire_on_commit=False))
            yield sessionmaker(bind=engine, autocommit=False, autoflush=False, expire_on_commit=False)


@pytest.fixture(scope='function', autouse=True)
def logger_level_for_tests(monkeypatch) -> str:
    """
    patches logger level for tests
    :param monkeypatch:
    :return: logger level
    """
    monkeypatch.setattr('config.LOGGER_LEVEL', TEST_LOGGER_LEVEL)
    if TEST_LOGGER_LEVEL == 'OFF':
        logger.remove()
    return TEST_LOGGER_LEVEL


def killpg_windows(__pgid: int, __signal: int) -> None:
    """
    quick fix for Windows support issue (https://github.com/ClearcodeHQ/pytest-postgresql/issues/303) of
    pytest-postgresql plugin
    """
    os.kill(__pgid, signal.CTRL_C_EVENT)


def create_schema(connection: Connection, schema_name: str) -> None:
    """
    creates schema with schema_name
    :param connection: connection to database where to create schema
    :param schema_name: name of schema to create
    """
    if not connection.dialect.has_schema(connection, schema_name):
        connection.execute(sqlalchemy.schema.CreateSchema(schema_name))


def add_test_objs_to_db(db_session, test_objs):
    """
    populates the database with test_objs data
    :param db_session: generator of session object
    :param test_objs: list of objects to record
    """
    session = db_session()
    session.add_all(test_objs)
    session.commit()
