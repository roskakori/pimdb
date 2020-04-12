import os

import pytest

from pimdb.command import ImdbDataset
from pimdb.command import exit_code_for, CommandName
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
        exit_code_for(["--help"])
        assert system_exit.code == 0


def test_can_show_command_help():
    for command_name in CommandName:
        with pytest.raises(SystemExit) as system_exit:
            exit_code_for([command_name.value, "--help"])
            assert system_exit.code == 0


def test_can_show_version():
    with pytest.raises(SystemExit) as system_exit:
        exit_code_for(["--version"])
        assert system_exit.code == 0


def test_can_transfer_all_datasets(gzip_tsv_files):
    database_engine = sqlite_engine(test_can_transfer_all_datasets)
    exit_code = exit_code_for(
        ["transfer", "--dataset-folder", TESTS_DATA_PATH, "--database", database_engine, "--drop", "all"]
    )
    assert exit_code == 0


@pytest.mark.skip("see FIXME comment for details")
def test_can_download_title_ratings():
    # FIXME This test has several issues that should be addressed by mocking the download:
    #  1. Every time it runs, it actually downloads about 5 MB, which is wasteful.
    #  2. It fails in case the remote dataset file changes between the first and second download.
    expected_target_path = output_path(ImdbDataset.TITLE_RATINGS.filename)
    # Ensure that the file to be downloaded does not exist already.
    try:
        os.remove(expected_target_path)
    except FileNotFoundError:
        pass

    exit_code = exit_code_for(
        [
            "download",
            "--dataset-folder",
            os.path.dirname(expected_target_path),
            "--force",
            ImdbDataset.TITLE_RATINGS.value,
        ]
    )

    assert exit_code == 0
    assert os.path.exists(expected_target_path)

    target_path_modified_after_first_download = os.path.getmtime(expected_target_path)

    # Report download without "--force", which should reuse the already downloaded file.
    exit_code = exit_code_for(
        ["download", "--dataset-folder", os.path.dirname(expected_target_path), ImdbDataset.TITLE_RATINGS.value]
    )

    assert exit_code == 0
    assert os.path.exists(expected_target_path)

    target_path_modified_after_second_download = os.path.getmtime(expected_target_path)

    assert target_path_modified_after_first_download == pytest.approx(target_path_modified_after_second_download)


def test_can_build_report_tables(gzip_tsv_files):
    database_engine = sqlite_engine(test_can_build_report_tables)
    exit_code = exit_code_for(
        ["transfer", "--dataset-folder", TESTS_DATA_PATH, "--database", database_engine, "--drop", "all"]
    )
    assert exit_code == 0
    exit_code = exit_code_for(
        ["build", "--database", database_engine]
    )  # TODO: Limit --drop to report tables and add it.
    assert exit_code == 0
