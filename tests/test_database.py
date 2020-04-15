# Copyright (c) 2020, Thomas Aglassinger.
# All rights reserved. Distributed under the BSD License.
from functools import lru_cache

import pytest
from sqlalchemy.sql import select

from pimdb.database import Database, ReportTable, engined
from tests._common import sqlite_engine, TESTS_DATA_PATH

_EXPECTED_KEY_VALUES = {"red", "green", "blue"}


@pytest.fixture
def memory_database() -> Database:
    return _create_database_with_tables("sqlite://")


@lru_cache(maxsize=1)
def _create_database_with_tables(engine_info: str) -> Database:
    result = Database(engine_info)
    result.create_imdb_dataset_tables()
    result.create_report_tables()
    return result


def test_can_build_key_table_from_values(memory_database):
    with memory_database.connection() as connection:
        memory_database.build_key_table_from_values(connection, ReportTable.GENRE, _EXPECTED_KEY_VALUES)
        genre_table = memory_database.report_table_for(ReportTable.GENRE)
        actual_colors = set(color for color, in connection.execute(select([genre_table.c.name])).fetchall())
    assert actual_colors == _EXPECTED_KEY_VALUES


def test_can_build_key_table_from_query(memory_database):
    test_can_build_key_table_from_values(memory_database)
    with memory_database.connection() as connection:
        memory_database.build_key_table_from_query(connection, ReportTable.PROFESSION, "select name from genre")
        profession_table = memory_database.report_table_for(ReportTable.PROFESSION)
        actual_colors = set(color for color, in connection.execute(select([profession_table.c.name])).fetchall())
    assert actual_colors == _EXPECTED_KEY_VALUES


def test_can_transfer_datasets():
    engine_info = sqlite_engine(test_can_transfer_datasets)
    database = _create_database_with_tables(engine_info)
    with database.connection() as connection:
        database.build_all_dataset_tables(connection, TESTS_DATA_PATH)


def test_can_enginite_path():
    assert engined("some.db") == "sqlite:///some.db"
    assert engined("/tmp/some.db") == "sqlite:////tmp/some.db"
    assert engined("sqlite:////tmp/some.db") == "sqlite:////tmp/some.db"
