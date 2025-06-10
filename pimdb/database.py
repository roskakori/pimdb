# Copyright (c) 2020, Thomas Aglassinger.
# All rights reserved. Distributed under the BSD License.
import functools
import gzip
import json
import os
import time
from collections.abc import Sequence
from enum import Enum
from typing import Callable, Optional, Union

from sqlalchemy import (
    Boolean,
    Column,
    Float,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    and_,
    create_engine,
    text,
)
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import select
from sqlalchemy.sql.functions import coalesce
from sqlalchemy.sql.selectable import SelectBase

from pimdb.bulk import DEFAULT_BULK_SIZE, BulkInsert, PostgresBulkLoad
from pimdb.common import IMDB_DATASET_NAMES, GzippedTsvReader, ImdbDataset, NormalizedTableKey, PimdbError, log

_TCONST_LENGTH = 12  # current maximum: 10
_NCONST_LENGTH = 12  # current maximum: 10

IMDB_TITLE_ALIAS_TYPES = ["alternative", "dvd", "festival", "tv", "video", "working", "original", "imdbDisplay"]


class NamePool:
    def __init__(self, max_length: int):
        self.max_length = max_length
        self._parts_to_name_map = {}
        self._shortened_count = 0

    def name(self, raw_name: str) -> str:
        result = self._parts_to_name_map.get(raw_name)
        if result is None:
            result = raw_name
            if len(result) > self.max_length:
                preferred_name = result
                is_unique_new_name = False
                while not is_unique_new_name:
                    self._shortened_count += 1
                    short_text = f"_{self._shortened_count}"
                    result = result[: self.max_length][: -len(short_text)] + short_text
                    if result not in self._parts_to_name_map.values():
                        is_unique_new_name = True
                log.info("  shortened name: %s -> %s", preferred_name, result)
            self._parts_to_name_map[raw_name] = result
        return result


class DatabaseSystem(Enum):
    """
    The underlying database system for a SQLAlchemy engine in order to decide
    how to best optimize bulk operations.
    """

    SQLITE = "sqlite"
    POSTGRES = "postgres"
    OTHER = "other"


def database_system_from_engine_info(engine_info: str) -> DatabaseSystem:
    if engine_info.startswith("sqlite:///"):
        return DatabaseSystem.SQLITE
    if engine_info.startswith(("postgresql://", "postgresql+psycopg2://")):
        return DatabaseSystem.POSTGRES
    return DatabaseSystem.OTHER


def imdb_dataset_table_infos() -> list[tuple[ImdbDataset, list[Column]]]:
    """SQL tables that represent a direct copy of a TSV file (excluding duplicates)"""
    return [
        (
            ImdbDataset.TITLE_BASICS,
            [
                Column("tconst", String(_TCONST_LENGTH), nullable=False, primary_key=True),
                Column("titleType", Text, nullable=False),
                Column("primaryTitle", Text),
                Column("originalTitle", Text),
                Column("isAdult", Boolean, nullable=False),
                Column("startYear", Integer),
                Column("endYear", Integer),
                Column("runtimeMinutes", Integer),
                Column("genres", Text),
            ],
        ),
        (
            ImdbDataset.NAME_BASICS,
            [
                Column("nconst", String(_NCONST_LENGTH), nullable=False, primary_key=True),
                Column("primaryName", Text, nullable=False),
                Column("birthYear", Integer),
                Column("deathYear", Integer),
                Column("primaryProfession", Text),
                Column("knownForTitles", Text),
            ],
        ),
        (
            ImdbDataset.TITLE_AKAS,
            [
                Column("titleId", String(_TCONST_LENGTH), nullable=False, primary_key=True),
                Column("ordering", Integer, nullable=False, primary_key=True),
                Column("title", Text),
                Column("region", Text),
                Column("language", Text),
                Column("types", Text),
                Column("attributes", Text),
                # NOTE: isOriginalTitle sometimes actually is null.
                Column("isOriginalTitle", Boolean),
            ],
        ),
        (
            ImdbDataset.TITLE_CREW,
            [
                Column("tconst", String(_TCONST_LENGTH), nullable=False, primary_key=True),
                Column("directors", Text),
                Column("writers", Text),
            ],
        ),
        (
            ImdbDataset.TITLE_EPISODE,
            [
                Column("tconst", String(_TCONST_LENGTH), nullable=False, primary_key=True),
                Column("parentTconst", String(_TCONST_LENGTH), nullable=False),
                Column("seasonNumber", Integer),
                Column("episodeNumber", Integer),
            ],
        ),
        (
            ImdbDataset.TITLE_PRINCIPALS,
            [
                Column("tconst", String(_TCONST_LENGTH), nullable=False, primary_key=True),
                Column("ordering", Integer, nullable=False, primary_key=True),
                Column("nconst", String(_NCONST_LENGTH), index=True, nullable=False),
                Column("category", Text, nullable=False),
                Column("job", Text),
                Column("characters", Text),
            ],
        ),
        (
            ImdbDataset.TITLE_RATINGS,
            [
                Column("tconst", String(_TCONST_LENGTH), nullable=False, primary_key=True),
                Column("averageRating", Float, nullable=False),
                Column("numVotes", Integer, nullable=False),
            ],
        ),
    ]


def _key_table_info(normalized_table_key: NormalizedTableKey) -> tuple[NormalizedTableKey, list[Union[Column, Index]]]:
    assert isinstance(normalized_table_key, NormalizedTableKey)
    return (
        normalized_table_key,
        [
            Column("id", Integer, nullable=False, primary_key=True),
            Column("name", Text, index=True, nullable=False, unique=True),
        ],
    )


def _ordered_relation_table_info(
    index_name_pool: NamePool,
    table_to_create: NormalizedTableKey,
    from_table: NormalizedTableKey,
    to_table: NormalizedTableKey,
) -> tuple[NormalizedTableKey, list[Union[Column, Index]]]:
    """
    Information required to create a table representing an ordered relation
    pointing from ``from_table`` to ``to_table``, including the necessary
    indexes and constraints.
    """
    assert isinstance(table_to_create, NormalizedTableKey)
    assert isinstance(from_table, NormalizedTableKey)
    assert isinstance(to_table, NormalizedTableKey)
    report_table_name = table_to_create.value
    from_table_name = from_table.value
    to_table_name = to_table.value
    return (
        table_to_create,
        [
            Column(f"{from_table_name}_id", Integer, ForeignKey(f"{from_table_name}.id"), nullable=False),
            Column("ordering", Integer, nullable=False),
            Column(f"{to_table_name}_id", Integer, ForeignKey(f"{to_table_name}.id"), nullable=False),
            Index(
                index_name_pool.name(f"index__{report_table_name}__{from_table_name}_id__ordering"),
                f"{from_table_name}_id",
                "ordering",
                unique=True,
            ),
            Index(index_name_pool.name(f"index__{report_table_name}__{to_table_name}_id"), f"{to_table_name}_id"),
        ],
    )


def report_table_infos(index_name_pool: NamePool) -> list[tuple[NormalizedTableKey, list[Union[Column, Index]]]]:
    return [
        _key_table_info(NormalizedTableKey.CHARACTER),
        (
            NormalizedTableKey.EPISODE,
            [
                Column("title_id", Integer, ForeignKey("title.id"), nullable=False, primary_key=True),
                Column("parent_title_id", Integer, ForeignKey("title.id"), nullable=False),
                Column("season", Integer),
                Column("episode", Integer),
            ],
        ),
        _key_table_info(NormalizedTableKey.GENRE),
        _key_table_info(NormalizedTableKey.PROFESSION),
        _key_table_info(NormalizedTableKey.TITLE_TYPE),
        (
            NormalizedTableKey.NAME,
            [
                Column("id", Integer, nullable=False, primary_key=True),
                Column("nconst", String(_NCONST_LENGTH), index=True, nullable=False, unique=True),
                Column("primary_name", Text, nullable=False),
                Column("birth_year", Integer),
                Column("death_year", Integer),
                Column("primary_professions", Text),
            ],
        ),
        _ordered_relation_table_info(
            index_name_pool,
            NormalizedTableKey.NAME_TO_KNOWN_FOR_TITLE,
            NormalizedTableKey.NAME,
            NormalizedTableKey.TITLE,
        ),
        (
            NormalizedTableKey.PARTICIPATION,
            [
                Column("id", Integer, nullable=False, primary_key=True),
                Column("title_id", Integer, ForeignKey("title.id"), nullable=False),
                Column("ordering", Integer, nullable=False),
                Column("name_id", Integer, ForeignKey("name.id"), index=True, nullable=False),
                Column("profession_id", Integer, ForeignKey("profession.id")),
                Column("job", Text),
                Index(
                    index_name_pool.name("index__participation__title_id__ordering"),
                    "title_id",
                    "ordering",
                    unique=True,
                ),
            ],
        ),
        _ordered_relation_table_info(
            index_name_pool,
            NormalizedTableKey.PARTICIPATION_TO_CHARACTER,
            NormalizedTableKey.PARTICIPATION,
            NormalizedTableKey.CHARACTER,
        ),
        (
            NormalizedTableKey.TEMP_CHARACTERS_TO_CHARACTER,
            [
                Column("characters", Text, nullable=False),
                Column("ordering", Integer, nullable=False),
                Column("character_id", Integer, ForeignKey("character.id"), nullable=False),
                Index(index_name_pool.name("index__name__characters__ordering"), "characters", "ordering", unique=True),
            ],
        ),
        (
            NormalizedTableKey.TITLE,
            [
                Column("id", Integer, nullable=False, primary_key=True),
                Column("tconst", String(_TCONST_LENGTH), index=True, nullable=False, unique=True),
                Column("title_type_id", Integer, ForeignKey("title_type.id"), nullable=False),
                Column("primary_title", Text, nullable=False),
                Column("original_title", Text, nullable=False),
                Column("is_adult", Boolean, nullable=False),
                Column("start_year", Integer),
                Column("end_year", Integer),
                Column("runtime_minutes", Integer),
                Column("average_rating", Float, default=0.0, nullable=False),
                Column("rating_count", Integer, default=0, nullable=False),
            ],
        ),
        (
            NormalizedTableKey.TITLE_ALIAS,
            [
                Column("id", Integer, nullable=False, primary_key=True),
                Column("title_id", Integer, ForeignKey("title.id"), nullable=False),
                Column("ordering", Integer, nullable=False),
                Column("title", Text, nullable=False),
                Column("region_code", Text),
                Column("language_code", Text),
                # NOTE: is_original_title sometimes actually is null.
                Column("is_original_title", Boolean),
                Index(
                    index_name_pool.name("index__title_alias__title_id__ordering"), "title_id", "ordering", unique=True
                ),
            ],
        ),
        _ordered_relation_table_info(
            index_name_pool,
            NormalizedTableKey.TITLE_ALIAS_TO_TITLE_ALIAS_TYPE,
            NormalizedTableKey.TITLE_ALIAS,
            NormalizedTableKey.TITLE_ALIAS_TYPE,
        ),
        _key_table_info(NormalizedTableKey.TITLE_ALIAS_TYPE),
        _ordered_relation_table_info(
            index_name_pool, NormalizedTableKey.TITLE_TO_GENRE, NormalizedTableKey.TITLE, NormalizedTableKey.GENRE
        ),
    ]


def typed_column_to_value_map(
    table: Table, column_name_to_raw_value_map: dict[str, str]
) -> dict[str, Optional[Union[bool, float, int, str]]]:
    result = {}
    for column in table.columns:
        raw_value = column_name_to_raw_value_map[column.name]
        column_python_type = column.type.python_type
        if raw_value == "\\N":
            value = None
            if not column.nullable:
                if column_python_type is bool:
                    value = False
                elif column_python_type in (float, int):
                    value = 0
                elif column_python_type is str:
                    value = ""
                else:
                    raise AssertionError(f"column_python_type={column_python_type}")
                log.warning(
                    'column "%s" of python type %s should not be null, using "%s" instead; raw_value_map=%s',
                    column.name,
                    column_python_type.__name__,
                    value,
                    column_name_to_raw_value_map,
                )
        elif column_python_type is bool:
            if raw_value == "1":
                value = True
            elif raw_value == "0":
                value = False
            else:
                raise PimdbError(f'value for column "{column.name}" must be a boolean but is: "{raw_value}"')
        else:
            value = column_python_type(raw_value)
        result[column.name] = value
    return result


class TableBuildStatus:
    def __init__(self, connection: Connection, table: Table):
        self._connection = connection
        self._table = table
        log.info("building table %s", table.name)
        self._time = None
        self.reset_time()

    def reset_time(self):
        self._time = time.time()

    def clear_table(self):
        self._connection.execute(self._table.delete())
        self.reset_time()

    def log_time(self, message_template: str, count: Optional[int] = None):
        duration_in_seconds = time.time() - self._time
        minutes, seconds = divmod(duration_in_seconds, 60)
        duration = f"{int(minutes):02d}:{seconds:06.3f}"
        rows_per_second = int(count / max(duration_in_seconds, 0.000001)) if count is not None else None
        message = message_template.format(count=count, duration=duration, rows_per_second=rows_per_second)
        log.info(f"  {message}")
        self._time = None

    def log_added_rows(self, count: Optional[Union[int, Connection]] = None):
        if isinstance(count, int):
            actual_count = count
        elif isinstance(count, Connection):
            actual_count = table_count(count, self._table)
        else:
            actual_count = None
        self.log_time("added {count} rows in {duration} ({rows_per_second} rows per second)", count=actual_count)

    def __enter__(self):
        return self

    def __exit__(self, error_type, error_value, error_traceback):
        pass


def table_count(connection: Connection, table: Table) -> int:
    # TODO: Is there a way to use SQLAlchemy core queries for that?
    (result,) = connection.execute(text(f'select count(1) from "{table.name}"')).fetchone()
    return result


def engined(engine_info_or_path: str) -> str:
    return engine_info_or_path if "://" in engine_info_or_path else f"sqlite:///{engine_info_or_path}"


def max_name_length(engine_info: str) -> int:
    """
    Maximum length of a name in the database. This can be used to for example
    limit generated index names to a valid length.
    :param engine_info:
    :return:
    """
    # NOTE: Actually many of these limits depend on (compile time) parameters
    #   but as there does not seem to be an easy way to query them we use the
    #   defaults.
    # TODO: For some databases the length can be queried programmatically, so we might just do that here.
    prefix_to_max_length = {
        # See https://docs.microsoft.com/en-us/sql/sql-server/maximum-capacity-specifications-for-sql-server
        "mssql": 128,
        # See https://dev.mysql.com/doc/refman/8.0/en/identifier-length.html
        "mysql": 64,
        # Actually 128 starting with Oracle 12, see: https://oracle-base.com/articles/12c/long-identifiers-12cr2
        "oracle": 30,
        # See: https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
        "postgres": 63,
        # Actually no limit except the maximum length of SQL statements, see https://www.sqlite.org/limits.html
        "sqlite": 128,
    }
    default_max_length = min(prefix_to_max_length.values())
    return next(
        (length for prefix, length in prefix_to_max_length.items() if engine_info.startswith(prefix)),
        default_max_length,
    )


class Database:
    def __init__(self, engine_info: str, bulk_size: int = DEFAULT_BULK_SIZE, has_to_drop_tables: bool = False):
        # FIXME: Remove possible username and pass word from logged engine info.
        actual_engine_info = engined(engine_info)
        self._database_system = database_system_from_engine_info(actual_engine_info)
        log.info("connecting to database %s (%s)", actual_engine_info, self._database_system.value)
        self._engine = create_engine(actual_engine_info)
        self._engine_name = actual_engine_info.split(":")[0]
        self._bulk_size = bulk_size
        self._has_to_drop_tables = has_to_drop_tables
        self._metadata = MetaData(self._engine)
        self._imdb_dataset_to_table_map = None
        self._normalized_name_to_table_map = {}
        self._nconst_to_name_id_map = None
        self._tconst_to_title_id_map = None

        self._normalized_index_name_pool = NamePool(max_name_length(actual_engine_info))
        #: Remembers title_alias_types that have yet to be added to IMDB_TITLE_ALIAS_TYPES.
        self._unknown_title_alias_types = None

    @property
    def engine(self) -> Engine:
        return self._engine

    @property
    def metadata(self) -> MetaData:
        return self._metadata

    @property
    def imdb_dataset_to_table_map(self) -> dict[ImdbDataset, Table]:
        assert self._imdb_dataset_to_table_map is not None, f"call {self.create_imdb_dataset_tables.__name__} first"
        return self._imdb_dataset_to_table_map

    def normalized_table_for(self, report_table_key: NormalizedTableKey) -> Table:
        return self._normalized_name_to_table_map[report_table_key]

    def _add_report_table(self, table: Table):
        self._normalized_name_to_table_map[NormalizedTableKey(table.name)] = table

    def connection(self) -> Connection:
        return self._engine.connect()

    def nconst_to_name_id_map(self, connection: Connection):
        if self._nconst_to_name_id_map is None:
            self._nconst_to_name_id_map = self._natural_key_to_id_map(connection, NormalizedTableKey.NAME, "nconst")
        return self._nconst_to_name_id_map

    def tconst_to_title_id_map(self, connection: Connection):
        if self._tconst_to_title_id_map is None:
            self._tconst_to_title_id_map = self._natural_key_to_id_map(connection, NormalizedTableKey.TITLE, "tconst")
        return self._tconst_to_title_id_map

    def _natural_key_to_id_map(
        self,
        connection: Connection,
        normalized_table_key: NormalizedTableKey,
        natural_key_column: str = "name",
        id_column: str = "id",
    ) -> dict[str, int]:
        table = self.normalized_table_for(normalized_table_key)
        log.info("  building mapping from %s.%s to %s.%s", table.name, natural_key_column, table.name, id_column)
        name_id_select = select([getattr(table.columns, natural_key_column), getattr(table.columns, id_column)])
        # HACK: Ideally, we would just use `dict(...)` here, but this results in:
        #  TypeError: 'LegacyCursorResult' object is not subscriptable
        result = {name: id_ for name, id_ in connection.execute(name_id_select)}  # noqa: C416
        log.info("    found %d entries", len(result))
        return result

    def create_imdb_dataset_tables(self):
        log.info("creating imdb dataset tables")
        self._imdb_dataset_to_table_map = {
            table_name: Table(
                table_name.table_name, self.metadata, *columns, comment=f"IMDb dataset {table_name.filename}"
            )
            for table_name, columns in imdb_dataset_table_infos()
        }
        if self._has_to_drop_tables:
            self.metadata.drop_all()
        self.metadata.create_all()

    def build_all_dataset_tables(
        self, connection: Connection, dataset_folder: str, log_progress: Optional[Callable[[int, int], None]] = None
    ):
        for imdb_dataset_name in IMDB_DATASET_NAMES:
            self.build_dataset_table(connection, imdb_dataset_name, dataset_folder, log_progress)

    def build_dataset_table(
        self,
        connection: Connection,
        imdb_dataset_name: str,
        dataset_folder: str,
        log_progress: Optional[Callable[[int, int], None]] = None,
    ):
        imdb_dataset = ImdbDataset(imdb_dataset_name)
        table_to_modify = self.imdb_dataset_to_table_map[imdb_dataset]
        gzipped_tsv_path = os.path.join(dataset_folder, imdb_dataset.filename)
        has_been_inserted_quickly = False
        if self._database_system == DatabaseSystem.POSTGRES:
            try:
                with TableBuildStatus(connection, table_to_modify) as table_build_status:
                    with (
                        gzip.open(gzipped_tsv_path, "rb") as gzipped_tsv_file,
                        PostgresBulkLoad(self._engine) as bulk_load,
                    ):
                        bulk_load.load(table_to_modify, gzipped_tsv_file)
                    table_build_status.log_added_rows(connection)
                    has_been_inserted_quickly = True
            except Exception as error:
                log.warning("cannot quickly insert data, reverting to slower variant (reason: %s)", error)

        if not has_been_inserted_quickly:
            with (
                connection.begin(),
                TableBuildStatus(connection, table_to_modify) as table_build_status,
            ):
                table_build_status.clear_table()

                # Insert all rows from TSV.
                key_columns = self.key_columns(imdb_dataset)
                gzipped_tsv_reader = GzippedTsvReader(gzipped_tsv_path, key_columns, log_progress)
                with BulkInsert(connection, table_to_modify, self._bulk_size) as bulk_insert:
                    try:
                        for raw_column_to_row_map in gzipped_tsv_reader.column_names_to_value_maps():
                            bulk_insert.add(typed_column_to_value_map(table_to_modify, raw_column_to_row_map))
                    except PimdbError as error:
                        raise PimdbError(
                            f"{gzipped_tsv_path} ({gzipped_tsv_reader.row_number}): cannot process row: {error}"
                        ) from error
                    table_build_status.log_added_rows(bulk_insert.count)

    def create_normalized_tables(self):
        log.info("creating normalized tables")
        self._drop_obsolete_normalized_tables()
        try:
            for normalized_table_key, options in report_table_infos(self._normalized_index_name_pool):
                self._normalized_name_to_table_map[normalized_table_key] = Table(
                    normalized_table_key.value, self.metadata, *options
                )
        except SQLAlchemyError as error:
            raise PimdbError(f'cannot create report table "{normalized_table_key.value}": {error}') from error
        if self._has_to_drop_tables:
            self.metadata.drop_all()
        self.metadata.create_all()

    def _drop_obsolete_normalized_tables(self):
        obsolete_table_names = ["characters_to_character", "title_to_director", "title_to_writer"]
        for obsolete_table_name in obsolete_table_names:
            obsolete_table = Table(obsolete_table_name, self._metadata, Column("_dummy", Integer))
            obsolete_table.drop(self._engine, checkfirst=True)

    def key_columns(self, imdb_dataset: ImdbDataset) -> tuple:
        return tuple(
            column.name for column in self.imdb_dataset_to_table_map[imdb_dataset].columns if column.primary_key
        )

    def build_key_table_from_query(
        self,
        connection: Connection,
        normalized_table_key: NormalizedTableKey,
        query: SelectBase,
        delimiter: Optional[str] = None,
    ):
        table_to_build = self.normalized_table_for(normalized_table_key)
        with TableBuildStatus(connection, table_to_build) as table_build_status:
            single_line_query = " ".join(str(query).replace("\n", " ").split())
            log.debug("querying key values: %s", single_line_query)
            values = set()
            for (raw_value,) in connection.execute(query):
                if delimiter is None:
                    values.add(raw_value)
                elif delimiter == "json":
                    try:
                        values_from_json = json.loads(raw_value)
                    except Exception as error:
                        raise PimdbError(f"cannot extract JSON from {raw_value!r}: {error}") from error
                    if not isinstance(values_from_json, list):
                        raise PimdbError(f"JSON column must be a list but is: {raw_value!r}")
                    values.update(values_from_json)
                else:
                    values.update(raw_value.split(delimiter))
            table_build_status.clear_table()
            self._build_key_table_from_values(connection, table_to_build, values)
            table_build_status.log_added_rows(connection)

    def build_key_table_from_values(
        self, connection: Connection, normalized_table_key: NormalizedTableKey, values: Sequence[str]
    ):
        table_to_build = self.normalized_table_for(normalized_table_key)
        with TableBuildStatus(connection, table_to_build) as table_build_status:
            table_build_status.clear_table()
            self._build_key_table_from_values(connection, table_to_build, values)
            table_build_status.log_added_rows(connection)

    def _build_key_table_from_values(self, connection: Connection, table_to_build: Table, values: Sequence[str]):
        with BulkInsert(connection, table_to_build, self._bulk_size) as bulk_insert:
            for value in sorted(values):
                bulk_insert.add({"name": value})
        self.check_table_has_data(connection, table_to_build)

    def build_title_alias_type_table(self, connection: Connection) -> None:
        with connection.begin():
            self.build_key_table_from_values(connection, NormalizedTableKey.TITLE_ALIAS_TYPE, IMDB_TITLE_ALIAS_TYPES)

    def build_title_type_table(self, connection: Connection):
        title_basics_table = self.imdb_dataset_to_table_map[ImdbDataset.TITLE_BASICS]
        with connection.begin():
            self.build_key_table_from_query(
                connection, NormalizedTableKey.TITLE_TYPE, select([title_basics_table.c.titleType]).distinct()
            )

    def build_genre_table(self, connection: Connection):
        title_basics_table = self.imdb_dataset_to_table_map[ImdbDataset.TITLE_BASICS]
        genres_column = title_basics_table.c.genres
        with connection.begin():
            self.build_key_table_from_query(
                connection,
                NormalizedTableKey.GENRE,
                select([genres_column]).where(genres_column.isnot(None)).distinct(),
                ",",
            )

    def build_profession_table(self, connection: Connection):
        title_principals_table = self.imdb_dataset_to_table_map[ImdbDataset.TITLE_PRINCIPALS]
        category_column = title_principals_table.c.category
        with connection.begin():
            self.build_key_table_from_query(
                connection,
                NormalizedTableKey.PROFESSION,
                select([category_column]).distinct(),
            )

    def build_participation_table(self, connection: Connection):
        participation_table = self.normalized_table_for(NormalizedTableKey.PARTICIPATION)
        with TableBuildStatus(connection, participation_table) as table_build_status:
            name_table = self.normalized_table_for(NormalizedTableKey.NAME)
            title_table = self.normalized_table_for(NormalizedTableKey.TITLE)
            title_principals_table = self.imdb_dataset_to_table_map[ImdbDataset.TITLE_PRINCIPALS]
            profession_table = self.normalized_table_for(NormalizedTableKey.PROFESSION)

            with connection.begin():
                table_build_status.clear_table()
                insert_participation = participation_table.insert().from_select(
                    [
                        participation_table.c.title_id,
                        participation_table.c.ordering,
                        participation_table.c.name_id,
                        participation_table.c.profession_id,
                        participation_table.c.job,
                    ],
                    select(
                        [
                            title_table.c.id,
                            title_principals_table.c.ordering,
                            name_table.c.id,
                            profession_table.c.id,
                            title_principals_table.c.job,
                        ]
                    ).select_from(
                        title_principals_table.join(name_table, name_table.c.nconst == title_principals_table.c.nconst)
                        .join(title_table, title_table.c.tconst == title_principals_table.c.tconst)
                        .join(profession_table, profession_table.c.name == title_principals_table.c.category)
                    ),
                )
                connection.execute(insert_participation)
                table_build_status.log_added_rows(connection)
                self.check_table_count(connection, title_principals_table, participation_table)

    def build_temp_characters_to_character_and_character_table(self, connection: Connection):
        log.info("building characters json to character names map")
        title_principals_table = self.imdb_dataset_to_table_map[ImdbDataset.TITLE_PRINCIPALS]
        characters_json_to_character_names_map = {}
        character_names = set()
        with connection.begin():
            characters_json_column = title_principals_table.c.characters
            select_characters_jsons = (
                select([characters_json_column]).where(characters_json_column.isnot(None)).distinct()
            )
            for (characters_json,) in connection.execute(select_characters_jsons):
                try:
                    character_names_from_json = json.loads(characters_json)
                except Exception as error:
                    raise PimdbError(
                        f"cannot JSON parse {title_principals_table.name}.{characters_json_column.name}: "
                        f"{characters_json!r}: {error}"
                    ) from error
                if not isinstance(character_names_from_json, list):
                    raise PimdbError(
                        f"{title_principals_table.name}.{characters_json_column.name} must be a JSON list but is: "
                        f"{characters_json!r}"
                    )
                characters_json_to_character_names_map[characters_json] = character_names_from_json
                character_names.update(character_names_from_json)
        character_name_to_character_id_map = {
            character_name: character_id for character_id, character_name in enumerate(sorted(character_names), start=1)
        }
        log.info(
            "  found %d characters jsons with %d names",
            len(characters_json_to_character_names_map),
            len(character_names),
        )

        character_table = self.normalized_table_for(NormalizedTableKey.CHARACTER)
        with (
            TableBuildStatus(connection, character_table) as character_build_status,
            connection.begin(),
        ):
            character_build_status.clear_table()
            with BulkInsert(connection, character_table, self._bulk_size) as character_bulk_insert:
                for character_name, character_id in character_name_to_character_id_map.items():
                    character_bulk_insert.add({"id": character_id, "name": character_name})
                character_build_status.log_added_rows(character_bulk_insert.count)

        temp_characters_to_character_table = self.normalized_table_for(NormalizedTableKey.TEMP_CHARACTERS_TO_CHARACTER)
        with (
            TableBuildStatus(connection, temp_characters_to_character_table) as table_build_status,
            connection.begin(),
        ):
            table_build_status.clear_table()
            with BulkInsert(connection, temp_characters_to_character_table, self._bulk_size) as bulk_insert:
                for character_json, character_names in characters_json_to_character_names_map.items():
                    for ordering, character_name in enumerate(character_names, start=1):
                        character_id = character_name_to_character_id_map[character_name]
                        bulk_insert.add(
                            {"characters": character_json, "character_id": character_id, "ordering": ordering}
                        )
                table_build_status.log_added_rows(bulk_insert.count)

    def build_participation_to_character_table(self, connection: Connection):
        participation_to_character_table = self.normalized_table_for(NormalizedTableKey.PARTICIPATION_TO_CHARACTER)
        with TableBuildStatus(connection, participation_to_character_table) as table_build_status:
            name_table = self.normalized_table_for(NormalizedTableKey.NAME)
            participation_table = self.normalized_table_for(NormalizedTableKey.PARTICIPATION)
            profession_table = self.normalized_table_for(NormalizedTableKey.PROFESSION)
            title_table = self.normalized_table_for(NormalizedTableKey.TITLE)
            title_principals_table = self.imdb_dataset_to_table_map[ImdbDataset.TITLE_PRINCIPALS]
            temp_characters_to_character = self.normalized_table_for(NormalizedTableKey.TEMP_CHARACTERS_TO_CHARACTER)

            with connection.begin():
                table_build_status.clear_table()
                insert_participation = participation_to_character_table.insert().from_select(
                    [
                        participation_to_character_table.c.participation_id,
                        participation_to_character_table.c.ordering,
                        participation_to_character_table.c.character_id,
                    ],
                    select(
                        [
                            participation_table.c.id,
                            temp_characters_to_character.c.ordering,
                            temp_characters_to_character.c.character_id,
                        ]
                    )
                    .select_from(
                        participation_table.join(name_table, name_table.c.id == participation_table.c.name_id)
                        .join(title_table, title_table.c.id == participation_table.c.title_id)
                        .join(
                            title_principals_table,
                            and_(
                                title_principals_table.c.nconst == name_table.c.nconst,
                                title_principals_table.c.tconst == title_table.c.tconst,
                                title_principals_table.c.ordering == participation_table.c.ordering,
                            ),
                        )
                        .join(
                            temp_characters_to_character,
                            temp_characters_to_character.c.characters == title_principals_table.c.characters,
                        )
                        .join(profession_table, profession_table.c.name == title_principals_table.c.category)
                    )
                    .distinct(),
                )
                connection.execute(insert_participation)
                table_build_status.log_added_rows(connection)
                self.check_table_has_data(connection, participation_to_character_table)

    @staticmethod
    def _log_building_table(table: Table) -> None:
        log.info("building %s table", table.name)

    def build_name_table(self, connection: Connection) -> None:
        name_table = self.normalized_table_for(NormalizedTableKey.NAME)
        with TableBuildStatus(connection, name_table) as table_build_status:
            name_basics_table = self.imdb_dataset_to_table_map[ImdbDataset.NAME_BASICS]
            with connection.begin():
                table_build_status.clear_table()
                insert_statement = name_table.insert().from_select(
                    [
                        name_table.c.nconst,
                        name_table.c.primary_name,
                        name_table.c.birth_year,
                        name_table.c.death_year,
                        name_table.c.primary_professions,
                    ],
                    select(
                        [
                            name_basics_table.c.nconst,
                            name_basics_table.c.primaryName,
                            name_basics_table.c.birthYear,
                            name_basics_table.c.deathYear,
                            name_basics_table.c.primaryProfession,
                        ]
                    ),
                )
                connection.execute(insert_statement)
                table_build_status.log_added_rows(connection)

    def build_name_to_known_for_title_table(self, connection: Connection):
        name_to_known_for_title_table = self.normalized_table_for(NormalizedTableKey.NAME_TO_KNOWN_FOR_TITLE)
        with TableBuildStatus(connection, name_to_known_for_title_table) as table_build_status:
            name_basics_table = self.imdb_dataset_to_table_map[ImdbDataset.NAME_BASICS]
            name_table = self.normalized_table_for(NormalizedTableKey.NAME)
            known_for_titles_column = name_basics_table.c.knownForTitles
            select_known_for_title_tconsts = (
                select([name_table.c.id, name_table.c.nconst, known_for_titles_column])
                .select_from(name_table.join(name_basics_table, name_basics_table.c.nconst == name_table.c.nconst))
                .where(known_for_titles_column.isnot(None))
            )
            with connection.begin():
                tconst_to_title_id_map = self.tconst_to_title_id_map(connection)
                table_build_status.clear_table()
                with BulkInsert(connection, name_to_known_for_title_table, self._bulk_size) as bulk_insert:
                    for name_id, nconst, known_for_titles_tconsts in connection.execute(select_known_for_title_tconsts):
                        ordering = 0
                        for tconst in known_for_titles_tconsts.split(","):
                            title_id = tconst_to_title_id_map.get(tconst)
                            if title_id is not None:
                                ordering += 1
                                bulk_insert.add({"name_id": name_id, "ordering": ordering, "title_id": title_id})
                            else:
                                log.debug(
                                    'ignored unknown %s.%s "%s" for name "%s"',
                                    name_basics_table.name,
                                    known_for_titles_column.name,
                                    tconst,
                                    nconst,
                                )
                    table_build_status.log_added_rows(bulk_insert.count)

    def build_title_table(self, connection: Connection) -> None:
        title_table = self.normalized_table_for(NormalizedTableKey.TITLE)
        with TableBuildStatus(connection, title_table) as table_build_status:
            title_basics_table = self.imdb_dataset_to_table_map[ImdbDataset.TITLE_BASICS]
            title_ratings_table = self.imdb_dataset_to_table_map[ImdbDataset.TITLE_RATINGS]
            title_type_table = self.normalized_table_for(NormalizedTableKey.TITLE_TYPE)
            with connection.begin():
                table_build_status.clear_table()
                insert_statement = title_table.insert().from_select(
                    [
                        title_table.c.tconst,
                        title_table.c.title_type_id,
                        title_table.c.primary_title,
                        title_table.c.original_title,
                        title_table.c.is_adult,
                        title_table.c.start_year,
                        title_table.c.end_year,
                        title_table.c.runtime_minutes,
                        title_table.c.average_rating,
                        title_table.c.rating_count,
                    ],
                    select(
                        [
                            title_basics_table.c.tconst,
                            title_type_table.c.id,
                            title_basics_table.c.primaryTitle,
                            title_basics_table.c.originalTitle,
                            title_basics_table.c.isAdult,
                            title_basics_table.c.startYear,
                            title_basics_table.c.endYear,
                            title_basics_table.c.runtimeMinutes,
                            coalesce(title_ratings_table.c.averageRating, 0),
                            coalesce(title_ratings_table.c.numVotes, 0),
                        ]
                    ).select_from(
                        title_basics_table.join(
                            title_type_table, title_type_table.c.name == title_basics_table.c.titleType
                        ).join(
                            # Not all titles are rated so we need to use an outer join here.
                            title_ratings_table,
                            title_ratings_table.c.tconst == title_basics_table.c.tconst,
                            isouter=True,
                        )
                    ),
                )
                connection.execute(insert_statement)
                table_build_status.log_added_rows(connection)
                self.check_table_count(connection, title_basics_table, title_table)

    def check_table_count(self, connection, source_table, target_table):
        source_table_count = table_count(connection, source_table)
        target_table_count = table_count(connection, target_table)
        if target_table_count != source_table_count:
            log.warning(
                'target table "%s" has %d rows but should have %d same as source table "%s"',
                target_table.name,
                target_table_count,
                source_table_count,
                source_table.name,
            )

    def check_table_has_data(self, connection: Connection, target_table: Table):
        target_table_count = table_count(connection, target_table)
        if target_table_count == 0:
            log.warning(
                'target table "%s" should contain rows but is empty',
            )

    def build_episode_table(self, connection: Connection):
        episode_table = self.normalized_table_for(NormalizedTableKey.EPISODE)
        with TableBuildStatus(connection, episode_table) as table_build_status:
            title_for_title_alias = self.normalized_table_for(NormalizedTableKey.TITLE).alias("title_for_title")
            title_for_parent_title_alias = self.normalized_table_for(NormalizedTableKey.TITLE).alias(
                "title_for_parent_title"
            )
            title_episode_table = self.imdb_dataset_to_table_map[ImdbDataset.TITLE_EPISODE]

            insert_episode = episode_table.insert().from_select(
                [
                    episode_table.c.title_id,
                    episode_table.c.parent_title_id,
                    episode_table.c.season,
                    episode_table.c.episode,
                ],
                select(
                    [
                        title_for_title_alias.c.id,
                        title_for_parent_title_alias.c.id,
                        title_episode_table.c.seasonNumber,
                        title_episode_table.c.episodeNumber,
                    ]
                ).select_from(
                    title_episode_table.join(
                        title_for_title_alias, title_for_title_alias.c.tconst == title_episode_table.c.tconst
                    ).join(
                        title_for_parent_title_alias,
                        title_for_parent_title_alias.c.tconst == title_episode_table.c.parentTconst,
                    )
                ),
            )

            with connection.begin():
                table_build_status.clear_table()
                connection.execute(insert_episode)
                table_build_status.log_added_rows(connection)

    def build_title_to_genre_table(self, connection: Connection):
        title_to_genre_table = self.normalized_table_for(NormalizedTableKey.TITLE_TO_GENRE)
        with TableBuildStatus(connection, title_to_genre_table) as table_build_status:
            title_basics_table = self.imdb_dataset_to_table_map[ImdbDataset.TITLE_BASICS]
            title_table = self.normalized_table_for(NormalizedTableKey.TITLE)
            genres_column = title_basics_table.c.genres
            select_genre_data = (
                select([title_table.c.id, genres_column])
                .select_from(title_table.join(title_basics_table, title_basics_table.c.tconst == title_table.c.tconst))
                .where(genres_column.isnot(None))
            )
            genre_name_to_id_map = self._natural_key_to_id_map(connection, NormalizedTableKey.GENRE)
            with connection.begin():
                table_build_status.clear_table()
                with BulkInsert(connection, title_to_genre_table, self._bulk_size) as bulk_insert:
                    for title_id, genres in connection.execute(select_genre_data):
                        for ordering, genre in enumerate(genres.split(","), start=1):
                            genre_id = genre_name_to_id_map[genre]
                            bulk_insert.add({"genre_id": genre_id, "ordering": ordering, "title_id": title_id})
                    table_build_status.log_added_rows(bulk_insert.count)

    # FIXME Redesign caching so it does not cause memory leaks. See
    #  <https://docs.astral.sh/ruff/rules/cached-instance-method/>.
    #  For now, this is no issue in practice because only one instance
    #  of `Database` exists during the runtime of the program.
    @functools.lru_cache(None)  # noqa: B019
    def mappable_title_alias_types(self, raw_title_types: str) -> list[str]:
        # TODO: Make inner function of build_title_alias_to_title_alias_type_table().
        result = []
        if raw_title_types:
            remaining_raw_title_alias_types = raw_title_types
            for title_alias_type_to_check in IMDB_TITLE_ALIAS_TYPES:
                if title_alias_type_to_check in remaining_raw_title_alias_types:
                    result.append(title_alias_type_to_check)
                    remaining_raw_title_alias_types = remaining_raw_title_alias_types.replace(
                        title_alias_type_to_check, ""
                    )
            if (
                remaining_raw_title_alias_types
                and remaining_raw_title_alias_types not in self._unknown_title_alias_types
            ):
                self._unknown_title_alias_types.add(remaining_raw_title_alias_types)
                log.warning(
                    'cannot map %s.types "%s" to a known type: IMDB_TITLE_ALIAS_TYPES should be extended accordingly',
                    ImdbDataset.TITLE_AKAS.table_name,
                    remaining_raw_title_alias_types,
                )
        return result

    def build_title_alias_table(self, connection: Connection):
        title_alias_table = self.normalized_table_for(NormalizedTableKey.TITLE_ALIAS)
        with TableBuildStatus(connection, title_alias_table) as table_build_status:
            title_akas_table = self.imdb_dataset_to_table_map[ImdbDataset.TITLE_AKAS]
            title_table = self.normalized_table_for(NormalizedTableKey.TITLE)

            with connection.begin():
                table_build_status.clear_table()
                insert_title_alias_table = title_alias_table.insert().from_select(
                    [
                        title_alias_table.c.title_id,
                        title_alias_table.c.ordering,
                        title_alias_table.c.title,
                        title_alias_table.c.region_code,
                        title_alias_table.c.language_code,
                        title_alias_table.c.is_original_title,
                    ],
                    select(
                        [
                            title_table.c.id,
                            title_akas_table.c.ordering,
                            title_akas_table.c.title,
                            title_akas_table.c.region,  # TODO: use lower()
                            title_akas_table.c.language,  # TODO: use lower()
                            title_akas_table.c.isOriginalTitle,
                        ]
                    ).select_from(
                        title_table.join(title_akas_table, title_akas_table.c.titleId == title_table.c.tconst)
                    ),
                )
                connection.execute(insert_title_alias_table)
                table_build_status.log_added_rows(connection)
                self.check_table_has_data(connection, title_alias_table)

    def build_title_alias_to_title_alias_type_table(self, connection: Connection):
        # TODO: Improve performance by using helper table similar to characters_to_character.
        title_alias_to_title_alias_type_table = self.normalized_table_for(
            NormalizedTableKey.TITLE_ALIAS_TO_TITLE_ALIAS_TYPE
        )
        with TableBuildStatus(connection, title_alias_to_title_alias_type_table) as table_build_status:
            title_table = self.normalized_table_for(NormalizedTableKey.TITLE)
            title_akas_table = self.imdb_dataset_to_table_map[ImdbDataset.TITLE_AKAS]
            title_alias_table = self.normalized_table_for(NormalizedTableKey.TITLE_ALIAS)
            title_alias_type_name_to_id_map = self._natural_key_to_id_map(
                connection, NormalizedTableKey.TITLE_ALIAS_TYPE
            )
            self._unknown_title_alias_types = set()

            title_akas_types_column = title_akas_table.c.types
            select_title_akas_data = (
                select([title_alias_table.c.id, title_akas_table.c.ordering, title_akas_types_column])
                .select_from(
                    title_alias_table.join(title_table, title_table.c.id == title_alias_table.c.title_id).join(
                        title_akas_table,
                        and_(
                            title_akas_table.c.titleId == title_table.c.tconst,
                            title_akas_table.c.ordering == title_alias_table.c.ordering,
                        ),
                    )
                )
                .where(title_akas_types_column.isnot(None))
            )
            with connection.begin():
                table_build_status.clear_table()
                with BulkInsert(connection, title_alias_to_title_alias_type_table, self._bulk_size) as bulk_insert:
                    for (
                        title_alias_id,
                        _title_alias_ordering,
                        raw_title_alias_types,
                    ) in connection.execute(select_title_akas_data):
                        for title_alias_type_ordering, title_alias_type_name in enumerate(
                            self.mappable_title_alias_types(raw_title_alias_types), start=1
                        ):
                            title_alias_type_id = title_alias_type_name_to_id_map[title_alias_type_name]
                            bulk_insert.add(
                                {
                                    "title_alias_id": title_alias_id,
                                    "ordering": title_alias_type_ordering,
                                    "title_alias_type_id": title_alias_type_id,
                                }
                            )
                table_build_status.log_added_rows(bulk_insert.count)
