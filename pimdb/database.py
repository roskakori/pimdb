from typing import List, Dict, Tuple, Union, Optional

from sqlalchemy import Column, Boolean, Float, Integer, MetaData, String, Table, create_engine
from sqlalchemy.engine import Engine, Connection

from pimdb.common import log, ImdbDataset, PimdbError

_metadata = MetaData()

#: SQL Tables that represent a direct copy of a TSV file (excluding duplicates)
IMDB_DATSET_TABLE_INFO = [
    [
        ImdbDataset.TITLE_BASICS,
        Column("tconst", String, nullable=False, primary_key=True),
        Column("titleType", String),
        Column("primaryTitle", String),
        Column("originalTitle", String),
        Column("isAdult", Boolean, nullable=False),
        Column("startYear", Integer),
        Column("endYear", Integer),
        Column("runtimeMinutes", Integer),
        Column("genres", String),
    ],
    [
        ImdbDataset.NAME_BASICS,
        Column("nconst", String, nullable=False, primary_key=True),
        Column("primaryName", String, nullable=False),
        Column("birthYear", Integer),
        Column("deathYear", Integer),
        Column("primaryProfession", String),
        Column("knownForTitles", String),
    ],
    [
        ImdbDataset.TITLE_AKAS,
        Column("titleId", String, nullable=False, primary_key=True),
        Column("ordering", Integer, nullable=False, primary_key=True),
        Column("title", String),
        Column("region", String),
        Column("language", String),
        Column("types", String),
        Column("attributes", String),
        Column("isOriginalTitle", Boolean, nullable=False),
    ],
    [
        ImdbDataset.TITLE_CREW,
        Column("tconst", String, nullable=False, primary_key=True),
        Column("directors", String),
        Column("writers", String),
    ],
    [
        ImdbDataset.TITLE_PRINCIPALS,
        Column("tconst", String, nullable=False, primary_key=True),
        Column("ordering", Integer, nullable=False, primary_key=True),
        Column("nconst", String),
        Column("category", String),
        Column("job", String),
        Column("characters", String),
    ],
    [
        ImdbDataset.TITLE_RATINGS,
        Column("tconst", String, nullable=False, primary_key=True),
        Column("averageRating", Float, nullable=False),
        Column("numVotes", Integer, nullable=False),
    ],
]


def typed_column_to_value_map(
    table: Table, column_name_to_raw_value_map: Dict[str, str]
) -> Dict[str, Optional[Union[bool, float, int, str]]]:
    result = {}
    for column in table.columns:
        raw_value = column_name_to_raw_value_map[column.name]
        column_python_type = column.type.python_type
        if raw_value == "\\N":
            value = None
            if not column.nullable:
                if column_python_type == bool:
                    value = False
                elif column_python_type in (float, int):
                    value = 0
                elif column_python_type == str:
                    value = ""
                else:
                    assert False, f"column_python_type={column_python_type}"
                log.warning(
                    'column "%s" of python type %s should not be null, using "%s" instead; raw_value_map=%s',
                    column.name,
                    column_python_type.__name__,
                    value,
                    column_name_to_raw_value_map,
                )
        elif column_python_type == bool:
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


class Database:
    def __init__(self, engine_info: str):
        # FIXME: Remove possible username and password from logged engine info.
        log.info("connecting to database %s", engine_info)
        self._engine = create_engine(engine_info)
        self._metadata = MetaData(self._engine)
        self._imdb_dataset_to_table_map = None

    @property
    def engine(self) -> Engine:
        return self._engine

    @property
    def metadata(self) -> MetaData:
        return self._metadata

    @property
    def imdb_dataset_to_table_map(self) -> Dict[ImdbDataset, Table]:
        assert self._imdb_dataset_to_table_map is not None, f"call {self.create_imdb_dataset_tables.__name__} first"
        return self._imdb_dataset_to_table_map

    def connection(self) -> Connection:
        return self._engine.connect()

    def create_imdb_dataset_tables(self):
        self._imdb_dataset_to_table_map = {
            table_info[0]: self._create_imdb_dataset_table(table_info[0], table_info[1:])
            for table_info in IMDB_DATSET_TABLE_INFO
        }
        self.metadata.create_all()

    def _create_imdb_dataset_table(self, imdb_dataset: ImdbDataset, columns: List[Column]) -> Table:
        table_name = imdb_dataset.table_name
        result = Table(table_name, self.metadata, *columns)
        return result

    def key_columns(self, imdb_dataset: ImdbDataset) -> Tuple[str]:
        return tuple(
            column.name for column in self.imdb_dataset_to_table_map[imdb_dataset].columns if column.primary_key
        )
