import pytest

from pimdb.command import main, CommandName


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


