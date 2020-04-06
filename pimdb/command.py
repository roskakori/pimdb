import argparse
import logging
import os
from enum import Enum
from typing import List, Optional

from sqlalchemy.engine import Connection

from pimdb import __version__
from pimdb.common import (
    download_imdb_dataset,
    log,
    ImdbDataset,
    IMDB_DATASET_NAMES,
    GzippedTsvReader,
    PimdbError,
    ReportTable,
)
from pimdb.database import typed_column_to_value_map, Database, sql_code

_DEFAULT_BULK_SIZE = 128
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
    download_parser.add_argument(
        "names",
        metavar="NAME",
        nargs="+",
        choices=_VALID_NAMES,
        default=_ALL_NAME,
        help=f"name(s) of IMDb datasets to download; valid names: {', '.join(_VALID_NAMES)}; default: %(default)s",
    )
    download_parser.add_argument("--out", "-o", default="", help="output folder; default: current folder")

    transfer_parser = subparsers.add_parser(
        CommandName.TRANSFER.value, help="transfer downloaded IMDb dataset files into SQL tables"
    )
    transfer_parser.add_argument(
        "names",
        metavar="NAME",
        nargs="+",
        choices=_VALID_NAMES,
        default=_ALL_NAME,
        help=f"name(s) of IMDb datasets to transfer; valid names: {', '.join(_VALID_NAMES)}; default: %(default)s",
    )
    transfer_parser.add_argument(
        "--bulk",
        "-b",
        dest="bulk_size",
        default=_DEFAULT_BULK_SIZE,
        help="bulk size of changes before they are flushed to the database; default: %(default)s",
    )
    transfer_parser.add_argument(
        "--database",
        "-d",
        default=_DEFAULT_DATABASE,
        help="database to connect to using SQLAlchemy engine syntax; default: %(default)s",
    )
    transfer_parser.add_argument(
        "--from",
        "-f",
        dest="from_folder",
        default="",
        help="source folder where the dataset files to transfer are located; default: current folder",
    )

    build_parser = subparsers.add_parser(CommandName.BUILD.value, help="build sanitized tables for reporting")
    build_parser.add_argument(
        "--database",
        "-d",
        default=_DEFAULT_DATABASE,
        help="database to connect to using SQLAlchemy engine syntax; default: %(default)s",
    )

    return result


def _download(parser, args: argparse.Namespace):
    for dataset_name_to_download in _checked_imdb_dataset_names(parser, args):
        target_path = os.path.join(args.out, ImdbDataset(dataset_name_to_download).filename)
        download_imdb_dataset(ImdbDataset(dataset_name_to_download), target_path)


def _checked_imdb_dataset_names(parser, args: argparse.Namespace) -> List[str]:
    if _ALL_NAME in args.names:
        if len(args.names) >= 2:
            parser.error(f'if NAME "{_ALL_NAME}" is specified, it must be the only NAME')
        result = IMDB_DATASET_NAMES
    else:
        # Remove possible duplicates and sort.
        result = sorted(set(args.names))
    return result


class _TransferCommand:
    def __init__(self, parser, args: argparse.Namespace):
        self._imdb_datasets = _checked_imdb_dataset_names(parser, args)
        self._database = Database(args.database)
        self._database.create_imdb_dataset_tables()
        self._bulk_size = args.bulk_size
        self._from_folder = args.from_folder
        self._insert_count = 0
        self._data_to_insert = None

    def run(self):
        def log_progress(processed_count: int, duplicate_count: int):
            if duplicate_count == 0:
                log.info("  processed %d rows", processed_count)
            else:
                log.info("  processed %d rows, ignored %d duplicates", processed_count, duplicate_count)

        with self._database.connection() as connection:
            for imdb_dataset_name in self._imdb_datasets:
                imdb_dataset = ImdbDataset(imdb_dataset_name)
                table_to_modify = self._database.imdb_dataset_to_table_map[imdb_dataset]

                # Clear the entire table.
                delete_statement = table_to_modify.delete().execution_options(autocommit=True)
                connection.execute(delete_statement)

                # Insert all rows from TSV.
                key_columns = self._database.key_columns(imdb_dataset)
                gzipped_tsv_path = os.path.join(self._from_folder, imdb_dataset.filename)
                gzipped_tsv_reader = GzippedTsvReader(gzipped_tsv_path, key_columns, log_progress)
                self._data_to_insert = []
                for raw_column_to_row_map in gzipped_tsv_reader.column_names_to_value_maps():
                    try:
                        self._data_to_insert.append(typed_column_to_value_map(table_to_modify, raw_column_to_row_map))
                    except PimdbError as error:
                        raise PimdbError(
                            f"{gzipped_tsv_path} ({gzipped_tsv_reader.row_number}): cannot process row: {error}"
                        )
                    self._checked_insert(connection, table_to_modify, False)
                self._checked_insert(connection, table_to_modify, True)

    def _checked_insert(self, connection, table, force: bool):
        data_count = len(self._data_to_insert)
        if (force and data_count >= 1) or (data_count >= self._bulk_size):
            with connection.begin() as transaction:
                connection.execute(table.insert(), self._data_to_insert)
                transaction.commit()
                self._data_to_insert.clear()


class _BuildCommand:
    def __init__(self, parser, args: argparse.Namespace):
        self._database = Database(args.database)
        self._connection: Optional[Connection] = None

    def run(self):
        self._database.create_imdb_dataset_tables()
        self._database.create_report_tables()
        with self._database.connection() as self._connection:
            self._database.build_title_type_table(self._connection)
            self._database.build_alias_type_table(self._connection)
            self._database.build_key_table_from_query(
                self._connection, ReportTable.GENRE, sql_code("select_genre_keys"), ","
            )
            self._database.build_key_table_from_query(
                self._connection, ReportTable.PROFESSION, sql_code("select_profession_keys"), ",",
            )
            # self._database.build_name_table(self._connection)
            self._database.build_title_table(self._connection)
            # self._database.build_title_to_director_table(self._connection)
            # self._database.build_title_to_writer_table(self._connection)
            pass  # TODO: Remove


def main(arguments: Optional[List[str]] = None):
    command_name = None
    try:
        parser = _parser()
        args = parser.parse_args(arguments)

        pimdb_log_level = logging.getLevelName(args.log.upper()) if args.log != "sql" else logging.DEBUG
        log.setLevel(pimdb_log_level)
        if args.log == "sql":
            logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)

        command_name = CommandName(args.command)
        if command_name == CommandName.BUILD:
            _BuildCommand(parser, args).run()
        elif command_name == CommandName.DOWNLOAD:
            _download(parser, args)
        elif command_name == CommandName.TRANSFER:
            _TransferCommand(parser, args).run()
        else:
            assert False, f"args={args}"
    except OSError as error:
        if command_name is None:
            log.error(error)
        else:
            log.error('cannot perform command "%s": %s', command_name.value, error)
    except KeyboardInterrupt:
        log.error("interrupted by user")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
