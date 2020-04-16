# Copyright (c) 2020, Thomas Aglassinger.
# All rights reserved. Distributed under the BSD License.
import pytest

from pimdb.common import ImdbDataset
from tests._common import gzipped_tests_data_path


@pytest.fixture
def gzip_tsv_files():
    """
    Ensure that the gzipped TSV files have been generated in ``TESTS_DATA_PATH``.
    """
    for imdb_dataset in ImdbDataset:
        gzipped_tests_data_path(imdb_dataset)
