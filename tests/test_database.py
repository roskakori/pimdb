# Copyright (c) 2020, Thomas Aglassinger.
# All rights reserved. Distributed under the BSD License.

import pytest
from sqlalchemy.sql import select

from pimdb.database import Database, NamePool, NormalizedTableKey, engined
from tests._common import TESTS_DATA_PATH, create_database_with_tables, sqlite_engine

_EXPECTED_KEY_VALUES = {"red", "green", "blue"}


@pytest.fixture
def memory_database() -> Database:
    return create_database_with_tables("sqlite://")


def test_can_build_key_table_from_values(memory_database):
    with memory_database.connection() as connection:
        memory_database.build_key_table_from_values(connection, NormalizedTableKey.GENRE, _EXPECTED_KEY_VALUES)
        genre_table = memory_database.normalized_table_for(NormalizedTableKey.GENRE)
        actual_colors = {color for (color,) in connection.execute(select([genre_table.c.name])).fetchall()}
    assert actual_colors == _EXPECTED_KEY_VALUES


def test_can_build_key_table_from_query(memory_database):
    test_can_build_key_table_from_values(memory_database)
    with memory_database.connection() as connection:
        memory_database.build_key_table_from_query(connection, NormalizedTableKey.PROFESSION, "select name from genre")
        profession_table = memory_database.normalized_table_for(NormalizedTableKey.PROFESSION)
        actual_colors = {color for (color,) in connection.execute(select([profession_table.c.name])).fetchall()}
    assert actual_colors == _EXPECTED_KEY_VALUES


def test_can_transfer_datasets(gzip_tsv_files):
    engine_info = sqlite_engine(test_can_transfer_datasets)
    database = create_database_with_tables(engine_info)
    with database.connection() as connection:
        database.build_all_dataset_tables(connection, TESTS_DATA_PATH)


def test_can_enginite_path():
    assert engined("some.db") == "sqlite:///some.db"
    assert engined("/tmp/some.db") == "sqlite:////tmp/some.db"
    assert engined("sqlite:////tmp/some.db") == "sqlite:////tmp/some.db"


def test_can_preserve_and_cut_name():
    name_pool = NamePool(10)

    assert name_pool.name("idx_a_b") == "idx_a_b"

    assert name_pool.name("idx_a_b") == "idx_a_b", "creating the same name multiple times must yield the same result"

    assert name_pool.name("idx_something_quite_long") == "idx_some_1", "long name must be cut"

    assert name_pool.name("idx_something_quite_long_but_different") == "idx_some_2", (
        "similar but different long name bust be cut differently"
    )

    # Add a valid name that will clash with a future shortened name.
    assert name_pool.name("idx_some_3") == "idx_some_3"
    assert name_pool.name("idx_something_quite_different") == "idx_some_4", "long name must advance past clash"
