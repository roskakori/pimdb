import pytest

from pimdb.command import ImdbDataset
from pimdb.command import main, CommandName
from tests._common import gzipped_tests_data_path, TESTS_DATA_PATH


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
    main(["transfer", "--from", TESTS_DATA_PATH, "--database", "sqlite://", "all"])
