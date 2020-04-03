import json
import logging
import os.path
from enum import Enum
from typing import Optional

import requests

_MEGABYTE = 1048576

#: Logger for all output of the pimdb module.
log = logging.getLogger("pimdb")


class ImdbDataset(Enum):
    """Names of all IMDb datasets available."""

    NAME_BASICS = "name.basics"
    TITLE_AKAS = "title.akas"
    TITLE_BASICS = "title.basics"
    TITLE_CREW = "title.crew"
    TITLE_PRINCIPALS = "title.principals"
    TITLE_RATINGS = "title.ratings"

    @property
    def filename(self):
        """
        The compressed file name for the URL, for example:

        >>> ImdbDataset("name.basics").filename
        'name.basics.tsv.gz'
        """
        return f"{self.value}.tsv.gz"


#: Names of all available IMDb datasets.
IMDB_DATASET_NAMES = [dataset.value for dataset in ImdbDataset]

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
            pass
        except Exception as error:
            log.warning(
                'cannot process last modified map "%s", enforcing downloads: %s', self._last_modified_map_path, error
            )

    def is_changed(self, url: str, current_last_modified: str) -> bool:
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
            has_to_be_downloaded = last_modified_map.is_changed(source_url, current_last_modified)
        else:
            has_to_be_downloaded = True

        if has_to_be_downloaded:
            megabyte_to_download = int(response.headers.get("content-length", "0")) / _MEGABYTE
            length_text = f"{megabyte_to_download:.1f} MB " if megabyte_to_download > 0 else ""
            log.info('downloading %s"%s" to "%s"', length_text, source_url, target_path)
            with open(target_path, "wb") as target_file:
                for chunk in response.iter_content(chunk_size=_DOWNLOAD_BUFFER_SIZE):
                    if chunk:  # filter out keep-alive new chunks
                        target_file.write(chunk)
            if only_if_newer:
                last_modified_map.update(source_url, current_last_modified)
                last_modified_map.write()
        else:
            log.info('dataset "%s" is up to date, skipping download of "%s"', imdb_dataset.value, source_url)
