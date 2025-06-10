# Copyright (c) 2020, Thomas Aglassinger.
# All rights reserved. Distributed under the BSD License.
import csv
import gzip
import json
import logging
import os.path
import time
from collections.abc import Generator
from enum import Enum
from typing import Any, Callable, Optional

import requests

_MEGABYTE = 1048576

#: Logger for all output of the pimdb module.
log = logging.getLogger("pimdb")


class PimdbError(Exception):
    """Error representing that something went wrong during an pimdb operation."""


class PimdbTsvError(Exception):
    def __init__(self, path: str, row_number: int, base_message: str):
        self.path = path
        self.row_number = row_number
        self.message = f"{os.path.basename(self.path)} ({row_number}: {base_message}"


class ImdbDataset(Enum):
    """Names of all IMDb datasets available."""

    NAME_BASICS = "name.basics"
    TITLE_AKAS = "title.akas"
    TITLE_BASICS = "title.basics"
    TITLE_CREW = "title.crew"
    TITLE_EPISODE = "title.episode"
    TITLE_PRINCIPALS = "title.principals"
    TITLE_RATINGS = "title.ratings"

    @property
    def tsv_filename(self):
        """
        The uncompressed file name mostly used for testing, for example:

        >>> ImdbDataset("name.basics").tsv_filename
        'name.basics.tsv'
        """
        return f"{self.value}.tsv"

    @property
    def filename(self):
        """
        The compressed file name for the URL, for example:

        >>> ImdbDataset("name.basics").filename
        'name.basics.tsv.gz'
        """
        return f"{self.value}.tsv.gz"

    @property
    def table_name(self):
        """
        Name for use in SQL tables, for example:

        >>> ImdbDataset("name.basics").table_name
        'NameBasics'
        """
        return camelized_dot_name(self.value)


class NormalizedTableKey(Enum):
    CHARACTER = "character"
    TEMP_CHARACTERS_TO_CHARACTER = "temp_characters_to_character"
    EPISODE = "episode"
    GENRE = "genre"
    NAME = "name"
    NAME_TO_KNOWN_FOR_TITLE = "name_to_known_for_title"
    PARTICIPATION = "participation"
    PARTICIPATION_TO_CHARACTER = "participation_to_character"
    PROFESSION = "profession"  # Extracted from title.principal.category
    TITLE = "title"
    TITLE_ALIAS = "title_alias"
    TITLE_ALIAS_TO_TITLE_ALIAS_TYPE = "title_alias_to_title_alias_type"
    TITLE_ALIAS_TYPE = "title_alias_type"
    TITLE_TO_GENRE = "title_to_genre"
    TITLE_TYPE = "title_type"


#: Names of all available IMDb datasets.
IMDB_DATASET_NAMES = [dataset.value for dataset in ImdbDataset]

#: Names of datasets required to build normalized tables.
IMDB_DATASET_NAMES_FOR_NORMALIZED_TABLES = list(set(IMDB_DATASET_NAMES).difference([ImdbDataset.TITLE_CREW.name]))

IMDB_DATASET_TO_KEY_COLUMNS_MAP = {
    ImdbDataset.NAME_BASICS: ["nconst"],
    ImdbDataset.TITLE_AKAS: ["titleId", "ordering"],
    ImdbDataset.TITLE_BASICS: ["tconst"],
    ImdbDataset.TITLE_EPISODE: ["tconst"],
    ImdbDataset.TITLE_CREW: ["tconst"],
    ImdbDataset.TITLE_PRINCIPALS: ["nconst", "tconst"],
    ImdbDataset.TITLE_RATINGS: ["tconst"],
}
assert len(IMDB_DATASET_NAMES) == len(IMDB_DATASET_TO_KEY_COLUMNS_MAP)

_DOWNLOAD_BUFFER_SIZE = 8192


class Settings:
    def __init__(self, data_folder: Optional[str] = None):
        self._data_folder = data_folder if data_folder is not None else ".pimdb"

    def pimdb_path(self, relative_path: str) -> str:
        """Path to a file or folder inside the pimdb data folder."""
        return os.path.join(self._data_folder, relative_path)


class LastModifiedMap:
    def __init__(self, last_modified_map_path: str):
        self._last_modified_map_path = last_modified_map_path
        self._url_to_last_modified_map = {}
        try:
            log.debug('reading "last modified" map from "%s"', self._last_modified_map_path)
            with open(self._last_modified_map_path, encoding="utf-8") as last_modified_file:
                self._url_to_last_modified_map = json.load(last_modified_file)
        except FileNotFoundError:
            # If we never cached anything before, just move on.
            log.debug('cannot find last modified map "%s", enforcing downloads', self._last_modified_map_path)
        except Exception as error:
            log.warning(
                'cannot process last modified map "%s", enforcing downloads: %s', self._last_modified_map_path, error
            )

    def is_modified(self, url: str, current_last_modified: str) -> bool:
        previous_last_modified = self._url_to_last_modified_map.get(url)
        log.debug(
            'checking last modified: previous=%r, current=%r, url="%s"',
            previous_last_modified,
            current_last_modified,
            url,
        )
        return current_last_modified != previous_last_modified

    def update(self, url: str, last_modified: str) -> None:
        self._url_to_last_modified_map[url] = last_modified

    def write(self) -> None:
        with open(self._last_modified_map_path, "w", encoding="utf-8") as last_modified_file:
            json.dump(self._url_to_last_modified_map, last_modified_file)


def download_imdb_dataset(imdb_dataset: ImdbDataset, target_path: str, only_if_newer: bool = True) -> None:
    source_url = f"https://datasets.imdbws.com/{imdb_dataset.filename}"
    last_modified_storage_path = os.path.join(os.path.dirname(target_path), ".pimdb_last_modified.json")
    last_modified_map = LastModifiedMap(last_modified_storage_path) if only_if_newer else None

    with requests.get(source_url, stream=True) as response:
        response.raise_for_status()
        if only_if_newer:
            current_last_modified = response.headers.get("last-modified")
            has_to_be_downloaded = last_modified_map.is_modified(source_url, current_last_modified)
        else:
            has_to_be_downloaded = True

        if has_to_be_downloaded:
            megabyte_to_download = int(response.headers.get("content-length", "0")) / _MEGABYTE
            length_text = f"{megabyte_to_download:.1f} MB " if megabyte_to_download > 0 else ""
            log.info('downloading %sfrom "%s" to "%s"', length_text, source_url, target_path)
            with open(target_path, "wb") as target_file:
                for chunk in response.iter_content(chunk_size=_DOWNLOAD_BUFFER_SIZE):
                    if chunk:  # filter out keep-alive new chunks
                        target_file.write(chunk)
            if only_if_newer:
                last_modified_map.update(source_url, current_last_modified)
                last_modified_map.write()
        else:
            log.info('dataset "%s" is up to date, skipping download of "%s"', imdb_dataset.value, source_url)


class GzippedTsvReader:
    def __init__(
        self,
        gzipped_tsv_path: str,
        key_columns: tuple[str],
        indicate_progress: Optional[Callable[[int, int], None]] = None,
        seconds_between_progress_update: float = 3.0,
        filtered_name_to_values_map: Optional[dict[str, set[str]]] = None,
    ):
        self._gzipped_tsv_path = gzipped_tsv_path
        self._row_number = None
        self._key_columns = key_columns
        self._duplicate_count = None
        self._indicate_progress = indicate_progress
        self._seconds_between_progress_update = seconds_between_progress_update
        self._filtered_name_to_values_map = filtered_name_to_values_map

    @property
    def gzipped_tsv_path(self) -> str:
        return self._gzipped_tsv_path

    @property
    def row_number(self) -> int:
        assert self._row_number is not None
        return self._row_number

    @property
    def location(self) -> str:
        row_number_text = f" ({self.row_number})" if self.row_number is not None else ""
        return f"{os.path.basename(self.gzipped_tsv_path)}{row_number_text}"

    @property
    def duplicate_count(self) -> int:
        return self._duplicate_count

    def column_names_to_value_maps(self) -> Generator[dict[str, str], None, None]:
        log.info('  reading IMDb dataset file "%s"', self.gzipped_tsv_path)
        with gzip.open(self.gzipped_tsv_path, "rt", encoding="utf-8", newline="") as tsv_file:
            last_progress_time = time.time()
            last_progress_row_number = None
            existing_keys = set()
            self._duplicate_count = 0
            self._row_number = 0
            tsv_reader = csv.DictReader(tsv_file, delimiter="\t", quoting=csv.QUOTE_NONE, strict=True)
            try:
                for result in tsv_reader:
                    self._row_number += 1
                    try:
                        key = tuple(result[key_column] for key_column in self._key_columns)
                    except KeyError as error:
                        raise PimdbTsvError(
                            self.gzipped_tsv_path,
                            self.row_number,
                            f'cannot find key "{error}" for key columns {self._key_columns}: row_map={result}',
                        ) from error
                    if key not in existing_keys:
                        existing_keys.add(key)
                        try:
                            is_filter_match = self._filtered_name_to_values_map is None or all(
                                result[name_to_filter] in values_to_filter
                                for name_to_filter, values_to_filter in self._filtered_name_to_values_map.items()
                            )
                        except KeyError as error:
                            raise PimdbTsvError(
                                self.gzipped_tsv_path,
                                self.row_number,
                                f"cannot evaluate filter: key_columns={self._key_columns}, "
                                f"filtered_name_to_values_map={self._filtered_name_to_values_map}",
                            ) from error
                        if is_filter_match:
                            yield result
                    else:
                        log.debug("%s: ignoring duplicate %s=%s", self.location, self._key_columns, key)
                        self._duplicate_count += 1
                    if self._indicate_progress is not None:
                        current_time = time.time()
                        if current_time - last_progress_time > self._seconds_between_progress_update:
                            self._indicate_progress(self.row_number, self.duplicate_count)
                            last_progress_time = current_time
                if self._duplicate_count != last_progress_row_number and self._indicate_progress is not None:
                    self._indicate_progress(self.row_number, self.duplicate_count)
            except csv.Error as error:
                raise PimdbTsvError(self.gzipped_tsv_path, self.row_number, str(error)) from error


class TsvDictWriter:
    def __init__(self, target_file):
        self._target_file = target_file
        self._line_number = None
        self._column_names = None

    @property
    def line_number(self) -> int:
        assert self._line_number is not None
        return self._line_number

    def write(self, name_to_value_map: dict[str, Any]):
        if self._column_names is None:
            self._column_names = list(name_to_value_map.keys())
            self._line_number = 1
            heading = "\t".join(self._column_names) + "\n"
            self._target_file.write(heading)
        self._line_number += 1
        try:
            self._target_file.write(
                "\t".join(name_to_value_map[column_name] for column_name in self._column_names) + "\n"
            )
        except Exception as error:
            raise PimdbTsvError(
                self._target_file,
                self.line_number,
                f"cannot write TSV row: {error}; name_to_value_map={name_to_value_map}",
            ) from error


def camelized_dot_name(name: str) -> str:
    assert name == name.lower()
    result = ""
    change_to_upper = True
    for char in name:
        if char == ".":
            change_to_upper = True
        else:
            if change_to_upper:
                actual_char = char.upper()
                change_to_upper = False
            else:
                actual_char = char
            result += actual_char
    return result
