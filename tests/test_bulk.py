# Copyright (c) 2020, Thomas Aglassinger.
# All rights reserved. Distributed under the BSD License.
import os

import pytest

from pimdb.common import ImdbDataset
from pimdb.bulk import PostgresBulkLoad
from tests._common import (
    create_database_with_tables,
    DEFAULT_TEST_ENGINE,
    IS_POSTGRES_DEFAULT_TEST_ENGINE,
    TESTS_DATA_PATH,
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
        with open(source_tsv_path, "rb") as source_tsv_file:
            with PostgresBulkLoad(database.engine) as bulk_load:
                bulk_load.load(target_table, source_tsv_file)
