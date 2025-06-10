# Copyright (c) 2020, Thomas Aglassinger.
# All rights reserved. Distributed under the BSD License.
import argparse
import logging
import os
import sys
from enum import Enum
from typing import Optional

from sqlalchemy import text
from sqlalchemy.engine import Connection

from pimdb import __version__
from pimdb.bulk import DEFAULT_BULK_SIZE
from pimdb.common import IMDB_DATASET_NAMES, ImdbDataset, PimdbError, download_imdb_dataset, log
from pimdb.database import Database

_DEFAULT_DATABASE = "sqlite:///pimdb.db"
_DEFAULT_LOG_LEVEL = "info"
_VALID_LOG_LEVELS = ["debug", "info", "sql", "warning"]
_LOG_LEVEL_MAP = {
    "info": logging.INFO,
}
_ALL_NAME = "all"
_NORMALIZED_NAME = "normalized"
_VALID_NAMES = [_ALL_NAME, _NORMALIZED_NAME, *IMDB_DATASET_NAMES]


class CommandName(Enum):
    """Available command line sub commands."""

    BUILD = "build"
    DOWNLOAD = "download"
    QUERY = "query"
    TRANSFER = "transfer"


def _parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(prog="pimdb")

    def add_bulk_size(parser_to_extend: argparse.ArgumentParser):
        parser_to_extend.add_argument(
            "--bulk",
            "-b",
            type=int,
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
    download_parser.add_argument(
        "--force",
        "-F",
        action="store_true",
        help="redownload even if file already exists and is newer than the online version",
    )
    add_dataset_names(download_parser, "download")

    transfer_parser = subparsers.add_parser(
        CommandName.TRANSFER.value, help="transfer downloaded IMDb dataset files into SQL tables"
    )
    add_bulk_size(transfer_parser)
    add_database(transfer_parser)
    add_dataset_folder(transfer_parser)
    add_dataset_names(transfer_parser, "transfer")
    add_drop(transfer_parser)

    build_parser = subparsers.add_parser(
        CommandName.BUILD.value, help="build normalized tables for more structured queries"
    )
    add_bulk_size(build_parser)
    add_database(build_parser)
    add_drop(build_parser)

    query_parser = subparsers.add_parser(
        CommandName.QUERY.value, help="perform SQL query on database and show results as tab separated values (TSV)"
    )
    add_database(query_parser)
    query_parser.add_argument("--file", "-f", action="store_true", help="")
    query_parser.add_argument("sql_query", metavar="SQL-QUERY", help="SQL code of the query to perform")

    return result


def _check_bulk_size(parser: argparse.ArgumentParser, parsed_arguments: argparse.Namespace):
    min_bulk_size = 1
    try:
        bulk_size = parsed_arguments.bulk_size
    except AttributeError:
        # No argument "--bulk" to check, just move on.
        pass
    else:
        if bulk_size < min_bulk_size:
            parser.error(f"--bulk is {bulk_size} but must be at least {min_bulk_size}")


class _DownloadCommand:
    def __init__(self, parser: argparse.ArgumentParser, args: argparse.Namespace):
        self._imdb_datasets = _checked_imdb_dataset_names(parser, args)
        self._dataset_folder = args.dataset_folder
        self._only_if_newer = not args.force

    def run(self):
        for dataset_name_to_download in self._imdb_datasets:
            target_path = os.path.join(self._dataset_folder, ImdbDataset(dataset_name_to_download).filename)
            download_imdb_dataset(ImdbDataset(dataset_name_to_download), target_path, self._only_if_newer)


def _checked_imdb_dataset_names(parser: argparse.ArgumentParser, args: argparse.Namespace) -> list[str]:
    def _check_special_name_is_only_name():
        if len(args.names) >= 2:
            parser.error(f'if NAME "{_ALL_NAME}" is specified, it must be the only NAME')

    if _ALL_NAME in args.names or _NORMALIZED_NAME in args.names:
        _check_special_name_is_only_name()
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
        self._database.create_normalized_tables()
        with self._database.connection() as self._connection:
            self._database.build_title_alias_type_table(self._connection)
            self._database.build_genre_table(self._connection)
            self._database.build_profession_table(self._connection)
            self._database.build_title_type_table(self._connection)
            self._database.build_name_table(self._connection)
            self._database.build_title_table(self._connection)
            self._database.build_title_alias_table(self._connection)
            self._database.build_title_alias_to_title_alias_type_table(self._connection)
            self._database.build_episode_table(self._connection)
            self._database.build_participation_table(self._connection)
            self._database.build_temp_characters_to_character_and_character_table(self._connection)
            self._database.build_participation_to_character_table(self._connection)
            self._database.build_name_to_known_for_title_table(self._connection)
            self._database.build_title_to_genre_table(self._connection)


class _QueryCommand:
    def __init__(self, _parser: argparse.ArgumentParser, args: argparse.Namespace):
        self._database = Database(args.database)
        if args.file:
            log.info('reading query from "%s"', args.sql_query)
            with open(args.sql_query, encoding="utf-8") as sql_query_file:
                self.sql_query = sql_query_file.read()
        else:
            self._sql_query = args.sql_query

    def run(self):
        with self._database.connection() as connection:
            sql_statement = text(self._sql_query)
            for row in connection.execute(sql_statement):
                print("\t".join(str(item) for item in row))  # noqa: T201


_COMMAND_NAME_TO_COMMAND_CLASS_MAP = {
    CommandName.BUILD: _BuildCommand,
    CommandName.DOWNLOAD: _DownloadCommand,
    CommandName.QUERY: _QueryCommand,
    CommandName.TRANSFER: _TransferCommand,
}


def exit_code_for(arguments: Optional[list[str]] = None) -> int:
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
        if args.command is None:
            possible_commands_text = ", ".join(command_name.value for command_name in CommandName)
            parser.error(f"COMMAND must be specified; possible commands are: {possible_commands_text}")
        _check_bulk_size(parser, args)

        pimdb_log_level = logging.getLevelName(args.log.upper()) if args.log != "sql" else logging.DEBUG
        log.setLevel(pimdb_log_level)
        if args.log == "sql":
            logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)

        command_name = CommandName(args.command)
        command_class = _COMMAND_NAME_TO_COMMAND_CLASS_MAP[command_name]
        command_class(parser, args).run()
        result = 0
    except (PimdbError, OSError) as error:
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
