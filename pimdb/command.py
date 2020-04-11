import argparse
import logging
import os
import sys
from enum import Enum
from typing import List, Optional

from sqlalchemy.engine import Connection

from pimdb import __version__
from pimdb.common import (
    download_imdb_dataset,
    log,
    ImdbDataset,
    IMDB_DATASET_NAMES,
)
from pimdb.database import Database, DEFAULT_BULK_SIZE

_DEFAULT_DATABASE = "sqlite:///pimdb.db"
_DEFAULT_LOG_LEVEL = "info"
_VALID_LOG_LEVELS = ["debug", "info", "sql", "warning"]
_LOG_LEVEL_MAP = {
    "info": logging.INFO,
}
_ALL_NAME = "all"
_VALID_NAMES = [_ALL_NAME] + IMDB_DATASET_NAMES


class CommandName(Enum):
    """Available command line sub commands."""

    BUILD = "build"
    DOWNLOAD = "download"
    TRANSFER = "transfer"


def _parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(prog="pimdb")

    def add_bulk_size(parser_to_extend: argparse.ArgumentParser):
        parser_to_extend.add_argument(
            "--bulk",
            "-b",
            dest="bulk_size",
            default=DEFAULT_BULK_SIZE,
            help=(
                "number of data for e.g. SQL insert to collect in memory "
                "before sending them to the database in a single operation; default: %(default)s"
            ),
        )

    def add_database(parser_to_extend: argparse.ArgumentParser):
        parser_to_extend.add_argument(
            "--database",
            "-d",
            default=_DEFAULT_DATABASE,
            help="database to connect to using SQLAlchemy engine syntax; default: %(default)s",
        )

    def add_dataset_folder(parser_to_extend: argparse.ArgumentParser):
        parser_to_extend.add_argument(
            "--dataset-folder",
            "-f",
            default="",
            help="folder where downloaded datasets are stored; default: current folder",
        )

    def add_dataset_names(parser_to_extend: argparse.ArgumentParser, action: str):
        parser_to_extend.add_argument(
            "names",
            metavar="NAME",
            nargs="+",
            choices=_VALID_NAMES,
            default=_ALL_NAME,
            help=f"name(s) of IMDb datasets to {action}; valid names: {', '.join(_VALID_NAMES)}; default: %(default)s",
        )

    def add_drop(parser_to_extend: argparse.ArgumentParser):
        parser_to_extend.add_argument(
            "--drop",
            "-D",
            action="store_true",
            help=(
                ""
                "drop target tables instead of just deleting all data; "
                "this is useful after an upgrade where the data model has changed"
            ),
        )

    result.add_argument(
        "--log",
        choices=_VALID_LOG_LEVELS,
        default=_DEFAULT_LOG_LEVEL,
        help=(
            f"level for logging messages; possible values: {', '.join(_VALID_LOG_LEVELS)}; "
            f"default: {_DEFAULT_LOG_LEVEL}"
        ),
    )
    result.add_argument("--version", action="version", version=f"%(prog)s {__version__}")

    subparsers = result.add_subparsers(dest="command", help="command to run")

    download_parser = subparsers.add_parser(CommandName.DOWNLOAD.value, help="download IMDb datasets")
    add_dataset_folder(download_parser)
    add_dataset_names(download_parser, "download")
    download_parser.add_argument("--force", "-F", action="store_true")

    transfer_parser = subparsers.add_parser(
        CommandName.TRANSFER.value, help="transfer downloaded IMDb dataset files into SQL tables"
    )
    add_bulk_size(transfer_parser)
    add_database(transfer_parser)
    add_dataset_folder(transfer_parser)
    add_dataset_names(transfer_parser, "transfer")
    add_drop(transfer_parser)

    build_parser = subparsers.add_parser(CommandName.BUILD.value, help="build sanitized tables for reporting")
    add_bulk_size(build_parser)
    add_database(build_parser)
    add_drop(build_parser)

    return result


class _DownloadCommand:
    def __init__(self, parser: argparse.ArgumentParser, args: argparse.Namespace):
        self._imdb_datasets = _checked_imdb_dataset_names(parser, args)
        self._dataset_folder = args.dataset_folder
        self._only_if_newer = not args.force

    def run(self):
        for dataset_name_to_download in self._imdb_datasets:
            target_path = os.path.join(self._dataset_folder, ImdbDataset(dataset_name_to_download).filename)
            download_imdb_dataset(ImdbDataset(dataset_name_to_download), target_path, self._only_if_newer)


def _checked_imdb_dataset_names(parser: argparse.ArgumentParser, args: argparse.Namespace) -> List[str]:
    if _ALL_NAME in args.names:
        if len(args.names) >= 2:
            parser.error(f'if NAME "{_ALL_NAME}" is specified, it must be the only NAME')
        result = IMDB_DATASET_NAMES
    else:
        # Remove possible duplicates and sort.
        result = sorted(set(args.names))
    return result


class _TransferCommand:
    def __init__(self, parser: argparse.ArgumentParser, args: argparse.Namespace):
        self._imdb_datasets = _checked_imdb_dataset_names(parser, args)
        self._database = Database(args.database, args.bulk_size, args.drop)
        self._database.create_imdb_dataset_tables()
        self._dataset_folder = args.dataset_folder

    def run(self):
        def log_progress(processed_count: int, duplicate_count: int):
            if duplicate_count == 0:
                log.info("  processed %d rows", processed_count)
            else:
                log.info("  processed %d rows, ignored %d duplicates", processed_count, duplicate_count)

        with self._database.connection() as connection:
            for imdb_dataset_name in self._imdb_datasets:
                self._database.build_dataset_table(connection, imdb_dataset_name, self._dataset_folder, log_progress)


class _BuildCommand:
    def __init__(self, _parser: argparse.ArgumentParser, args: argparse.Namespace):
        self._database = Database(args.database, args.bulk_size, args.drop)
        self._connection: Optional[Connection] = None

    def run(self):
        self._database.create_imdb_dataset_tables()
        self._database.create_report_tables()
        with self._database.connection() as self._connection:
            self._database.build_title_alias_type_table(self._connection)
            self._database.build_genre_table(self._connection)
            self._database.build_profession_table(self._connection)
            self._database.build_title_type_table(self._connection)
            self._database.build_name_table(self._connection)
            self._database.build_title_table(self._connection)
            self._database.build_title_alias_and_title_alias_to_title_alias_type_table(self._connection)
            self._database.build_participation_and_character_tables(self._connection)
            self._database.build_name_to_known_for_title_table(self._connection)
            self._database.build_title_to_genre_table(self._connection)
            self._database.build_title_to_director_table(self._connection)
            self._database.build_title_to_writer_table(self._connection)


_COMMAND_NAME_TO_COMMAND_CLASS_MAP = {
    CommandName.BUILD: _BuildCommand,
    CommandName.DOWNLOAD: _DownloadCommand,
    CommandName.TRANSFER: _TransferCommand,
}


def exit_code_for(arguments: Optional[List[str]] = None) -> int:
    """
    Exit code for running the command line with the specified ``arguments``,
    or ``sys.argv``if no arguments are specified.

    Unlike :py:func:`main`, logging has to be initialized before calling this
    function.

    Some command line options like "--help" and "--version" result in
    :py:exc:`SystemExit` that is just passed on.

    Unexpected errors are not handled with ``except`` but passed on.
    """
    result = 1
    command_name = None
    try:
        parser = _parser()
        args = parser.parse_args(arguments)

        pimdb_log_level = logging.getLevelName(args.log.upper()) if args.log != "sql" else logging.DEBUG
        log.setLevel(pimdb_log_level)
        if args.log == "sql":
            logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)

        command_name = CommandName(args.command)
        command_class = _COMMAND_NAME_TO_COMMAND_CLASS_MAP[command_name]
        command_class(parser, args).run()
        result = 0
    except OSError as error:
        if command_name is None:
            log.error(error)
        else:
            log.error('cannot perform command "%s": %s', command_name.value, error)
    except KeyboardInterrupt:
        log.error("interrupted by user")
    return result


def main():
    logging.basicConfig(level=logging.INFO)
    sys.exit(exit_code_for())


if __name__ == "__main__":
    main()
