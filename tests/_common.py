"""Functions commonly used by multiple tests."""
import gzip
import logging
import os
from typing import Callable

from pimdb.command import ImdbDataset

TESTS_DATA_PATH = os.path.join(os.path.dirname(__file__), "data")
TESTS_OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "output")

_log = logging.getLogger(__name__)


def output_path(name):
    result = os.path.join(TESTS_OUTPUT_PATH, name)
    os.makedirs(os.path.dirname(result), exist_ok=True)
    return result


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
        with gzip.open(tsv_gz_path, "wb") as target_tsv_gz_file:
            with open(tsv_path, "rb") as source_tsv_file:
                # NOTE: This reads the entire source file into memory, which is fine for testing
                # but would be evil in a production environment.
                target_tsv_gz_file.write(source_tsv_file.read())
    return tsv_gz_path
