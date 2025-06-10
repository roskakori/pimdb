# Copyright (c) 2020, Thomas Aglassinger.
# All rights reserved. Distributed under the BSD License.
import os

import pytest

from pimdb.bulk import PostgresBulkLoad
from pimdb.common import ImdbDataset
from tests._common import (
    DEFAULT_TEST_ENGINE,
    IS_POSTGRES_DEFAULT_TEST_ENGINE,
    TESTS_DATA_PATH,
    create_database_with_tables,
)


@pytest.mark.skipif(
    not IS_POSTGRES_DEFAULT_TEST_ENGINE,
    reason="environment variable PIMDB_TEST_DATABASE must be set to postgres engine",
)
def test_can_postgres_bulk_load_tsv():
    database = create_database_with_tables(DEFAULT_TEST_ENGINE)
    for dataset_to_load in ImdbDataset:
        target_table = database.imdb_dataset_to_table_map[dataset_to_load]
        source_tsv_path = os.path.join(TESTS_DATA_PATH, dataset_to_load.tsv_filename)
        with (
            open(source_tsv_path, "rb") as source_tsv_file,
            PostgresBulkLoad(database.engine) as bulk_load,
        ):
            bulk_load.load(target_table, source_tsv_file)
        with database.connection() as connection:
            database.check_table_has_data(connection, target_table)


@pytest.mark.skipif(
    not IS_POSTGRES_DEFAULT_TEST_ENGINE,
    reason="environment variable PIMDB_TEST_DATABASE must be set to postgres engine",
)
def test_fails_on_postgres_bulk_load_tsv_with_duplicate():
    database = create_database_with_tables(DEFAULT_TEST_ENGINE)
    dataset_to_load = ImdbDataset.NAME_BASICS
    target_table = database.imdb_dataset_to_table_map[dataset_to_load]
    source_tsv_path = os.path.join(
        TESTS_DATA_PATH, test_fails_on_postgres_bulk_load_tsv_with_duplicate.__name__, dataset_to_load.tsv_filename
    )
    with pytest.raises(Exception, match=".*duplicate key.*"):
        with (
            open(source_tsv_path, "rb") as source_tsv_file,
            PostgresBulkLoad(database.engine) as bulk_load,
        ):
            bulk_load.load(target_table, source_tsv_file)
        with database.connection() as connection:
            database.check_table_has_data(connection, target_table)
