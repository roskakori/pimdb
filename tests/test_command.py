import os

import pytest

from pimdb.command import ImdbDataset
from pimdb.command import main, CommandName
from tests._common import gzipped_tests_data_path, output_path, sqlite_engine, TESTS_DATA_PATH


@pytest.fixture
def gzip_tsv_files():
    """
    Ensure that the gzipped TSV files have been generated in ``TESTS_DATA_PATH``.
    """
    for imdb_dataset in ImdbDataset:
        gzipped_tests_data_path(imdb_dataset)


def test_can_show_help():
    with pytest.raises(SystemExit) as system_exit:
        main(["--help"])
        assert system_exit.code == 0


def test_can_show_command_help():
    for command_name in CommandName:
        with pytest.raises(SystemExit) as system_exit:
            main([command_name.value, "--help"])
            assert system_exit.code == 0


def test_can_show_version():
    with pytest.raises(SystemExit) as system_exit:
        main(["--version"])
        assert system_exit.code == 0


def test_can_transfer_all_datasets(gzip_tsv_files):
    database_engine = sqlite_engine(test_can_transfer_all_datasets)
    exit_code = main(["transfer", "--dataset-folder", TESTS_DATA_PATH, "--database", database_engine, "all"])
    assert exit_code == 0


def test_can_download_title_ratings():
    expected_target_path = output_path(ImdbDataset.TITLE_RATINGS.filename)
    exit_code = main(
        ["download", "--dataset-folder", os.path.dirname(expected_target_path), ImdbDataset.TITLE_RATINGS.value]
    )
    assert exit_code == 0
    assert os.path.exists(expected_target_path)


def test_can_build_report_tables(gzip_tsv_files):
    database_engine = sqlite_engine(test_can_build_report_tables)
    exit_code = main(["transfer", "--dataset-folder", TESTS_DATA_PATH, "--database", database_engine, "all"])
    assert exit_code == 0
    exit_code = main(["build", "--database", database_engine])
    assert exit_code == 0
