"""Functions and constants commonly used by multiple tests."""

# Copyright (c) 2020, Thomas Aglassinger.
# All rights reserved. Distributed under the BSD License.
import gzip
import logging
import os
from functools import lru_cache
from typing import Callable

from pimdb.command import ImdbDataset
from pimdb.database import Database

TESTS_DATA_PATH = os.path.join(os.path.dirname(__file__), "data")
TESTS_OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "output")

_log = logging.getLogger("pimdb.test")


def output_path(name):
    result = os.path.join(TESTS_OUTPUT_PATH, name)
    os.makedirs(os.path.dirname(result), exist_ok=True)
    return result


#: Database engine to use for tests that do not have any special requirements
#: concerning the database. By default, this will use SQLite, but you can
#: override this using the environment variable :envvar:`PIMDB_TEST_DATABASE`.
DEFAULT_TEST_ENGINE = os.environ.get("PIMDB_TEST_DATABASE", "sqlite:///" + output_path("pimdb_test.db"))

#: Optional database engine to use for tests that require full IMDb datasets.
TEST_FULL_ENGINE = os.environ.get("PIMDB_TEST_FULL_DATABASE")

#: True if :data:`DEFAULT_TEST_ENGINE` is a PostgreSQL database.
IS_POSTGRES_DEFAULT_TEST_ENGINE = DEFAULT_TEST_ENGINE.startswith("postgres")


def sqlite_engine(test_function: Callable) -> str:
    database_path = os.path.abspath(output_path(test_function.__name__ + ".db"))
    return "sqlite:///" + database_path


def gzipped_tests_data_path(dataset: ImdbDataset) -> str:
    tsv_gz_path = os.path.join(TESTS_DATA_PATH, dataset.filename)
    tsv_path = tsv_gz_path[:-3]
    assert tsv_path.endswith(".tsv")

    tsv_modified_time = os.path.getmtime(tsv_path)
    try:
        tsv_gz_modified_time = os.path.getmtime(tsv_gz_path)
        has_to_build_gz = tsv_modified_time > tsv_gz_modified_time
    except FileNotFoundError:
        has_to_build_gz = True

    if has_to_build_gz:
        _log.info('creating compressed "%s" from "%s"', tsv_gz_path, tsv_path)
        with (
            gzip.open(tsv_gz_path, "wb") as target_tsv_gz_file,
            open(tsv_path, "rb") as source_tsv_file,
        ):
            # NOTE: This reads the entire source file into memory, which is fine for testing
            # but would be evil in a production environment.
            target_tsv_gz_file.write(source_tsv_file.read())
    return tsv_gz_path


@lru_cache(maxsize=1)
def create_database_with_tables(engine_info: str) -> Database:
    result = Database(engine_info, has_to_drop_tables=True)
    result.create_imdb_dataset_tables()
    result.create_normalized_tables()
    return result
