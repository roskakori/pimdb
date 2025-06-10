"""
Run the examples and see they return at least one row.
"""

import os
from glob import glob

import pytest

from pimdb.database import Database
from tests._common import TEST_FULL_ENGINE

_EXAMPLES_FOLDER = os.path.join(os.path.dirname(os.path.dirname(__file__)), "docs", "examples")


@pytest.mark.skipif(
    TEST_FULL_ENGINE is None,
    reason=(
        "environment variable PIMDB_TEST_FULL_DATABASE must point to an SQLAlchemy engine "
        "with a IMDb from full datasets"
    ),
)
def test_examples():
    database = Database(TEST_FULL_ENGINE)
    example_sql_paths = glob(os.path.join(_EXAMPLES_FOLDER, "*.sql"))
    assert example_sql_paths
    with database.connection() as connection:
        for example_sql_path in example_sql_paths:
            with open(example_sql_path, encoding="utf-8") as example_sql_file:
                sql_statement = example_sql_file.read()
            assert sql_statement
            rows = list(connection.execute(sql_statement))
            assert rows
