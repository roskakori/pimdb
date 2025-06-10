# Copyright (c) 2020, Thomas Aglassinger.
# All rights reserved. Distributed under the BSD License.
import contextlib
import os

import pytest

from pimdb.command import CommandName, ImdbDataset, exit_code_for
from tests._common import TESTS_DATA_PATH, output_path, sqlite_engine


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


def test_can_transfer_normalized_datasets(gzip_tsv_files):
    database_engine = sqlite_engine(test_can_transfer_normalized_datasets)
    exit_code = exit_code_for(
        ["transfer", "--dataset-folder", TESTS_DATA_PATH, "--database", database_engine, "--drop", "normalized"]
    )
    assert exit_code == 0


def test_can_query_dataset(gzip_tsv_files):
    database_engine = sqlite_engine(test_can_transfer_all_datasets)
    exit_code = exit_code_for(["query", "--database", database_engine, "select count(1)"])
    assert exit_code == 0


def test_fails_on_too_small_bulk_size():
    with pytest.raises(SystemExit) as system_exit:
        exit_code_for([CommandName.TRANSFER.value, "--bulk", "0", "title.ratings"])
        assert system_exit.code == 1


def test_fails_on_missing_command():
    with pytest.raises(SystemExit) as system_exit:
        exit_code_for([])
        assert system_exit.code == 1


@pytest.mark.skip("see FIXME comment for details")
def test_can_download_title_ratings():
    # FIXME This test has several issues that should be addressed by mocking the download:
    #  1. Every time it runs, it actually downloads about 5 MB, which is wasteful.
    #  2. It fails in case the remote dataset file changes between the first and second download.
    expected_target_path = output_path(ImdbDataset.TITLE_RATINGS.filename)
    # Ensure that the file to be downloaded does not exist already.
    with contextlib.suppress(FileNotFoundError):
        os.remove(expected_target_path)

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
        ["transfer", "--dataset-folder", TESTS_DATA_PATH, "--database", database_engine, "--drop", "normalized"]
    )
    assert exit_code == 0
    exit_code = exit_code_for(
        ["build", "--database", database_engine]
    )  # TODO: Limit --drop to report tables and add it.
    assert exit_code == 0
