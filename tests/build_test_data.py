import argparse
import gzip
import logging
import os
import re
from typing import List, Optional, Generator, Set

from pimdb import __version__
from pimdb.common import ImdbDataset

TEST_NCONSTS = [
    # "nm0000616",  # Eric Roberts
    # "nm0001376",  # Isabelle Huppert
    # "nm0233757",  # Jaco Van Dormael
    # "nm0567408",  # Hattie McDaniel
    # "nm1382571",  # Michael Ostrowski
    # "nm1801453",  # Achita Sikamana
    "nm3658287",  # Bianca Bradey
]

_DEFAULT_TARGET_FOLDER = os.path.join(os.path.dirname(__file__), "data")

log = logging.getLogger(__name__)


class GzippedTsvLineReader:
    def __init__(self, gzipped_tsv_path: str, consts_to_filter: Set[str], include_heading=True):
        self._gzipped_tsv_path = gzipped_tsv_path
        self._line_number = None
        self._include_heading = include_heading
        # FIXME: The current line based reading and regex based filtering does not scale well.
        #  A faster way would be to use a DictReader and check for the columns nconst or tconst
        #  to be within const_to_filter.
        self._filter_regex = re.compile(f".*({'|'.join(consts_to_filter)}).*")

    @property
    def gzipped_tsv_path(self) -> str:
        return self._gzipped_tsv_path

    @property
    def line_number(self) -> int:
        assert self._line_number is not None
        return self._line_number

    @property
    def location(self) -> str:
        row_number_text = f" ({self.line_number})" if self.line_number is not None else ""
        return f"{os.path.basename(self.gzipped_tsv_path)}{row_number_text}"

    def filtered_lines(self) -> Generator[str, None, None]:
        log.info('filtering IMDb dataset from "%s"', self.gzipped_tsv_path)
        with gzip.open(self.gzipped_tsv_path, "rt", encoding="utf-8", newline="") as tsv_file:
            self._line_number = 0
            for line in tsv_file:
                self._line_number += 1
                is_heading = self._include_heading and self._line_number == 1
                if is_heading or self._filter_regex.match(line) is not None:
                    yield line


def _parsed_arguments(args: Optional[List[str]]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "create filtered IMDb datasets that contain only a selected few names, "
            "the titles they contributed to and all other names being part of these titles"
        )
    )
    parser.add_argument(
        "dataset_folder",
        metavar="FOLDER",
        nargs="?",
        default="",
        help="folder containing gzipped complete IMDb datasets to be used as source; default: current folder",
    )
    parser.add_argument(
        "--out",
        "-o",
        dest="target_folder",
        metavar="FOLDER",
        default=_DEFAULT_TARGET_FOLDER,
        help="folder where to store the filtered TSV files; default: %(default)s",
    )
    parser.add_argument(
        "--quick", "-q", action="store_true", help="use a hardcoded minimal set of source names for quick testing"
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    return parser.parse_args(args)


def gzipped_tsv_line_reader(
    folder: str, imdb_dataset: ImdbDataset, consts_to_filter: List[str], include_heading=True
) -> GzippedTsvLineReader:
    return GzippedTsvLineReader(os.path.join(folder, imdb_dataset.filename), consts_to_filter, include_heading)


def extracted_tconsts(gzipped_tsv_folder: str, regex_prefix: str, consts_to_filter: List[str]) -> Set[str]:
    assert regex_prefix in ["nm", "tt"]
    result = set()
    tconst_regex = re.compile(fr".*(?P<tconst>{regex_prefix}\w+).*", re.ASCII)
    line_reader = gzipped_tsv_line_reader(gzipped_tsv_folder, ImdbDataset.TITLE_PRINCIPALS, consts_to_filter, False)
    for line in line_reader.filtered_lines():
        tconst_match = tconst_regex.match(line)
        if tconst_match is not None:
            tconst = tconst_match.group("tconst")
            result.add(tconst)
    return result


def main(args: Optional[List[str]] = None):
    arguments = _parsed_arguments(args)
    log.info("collecting tconsts to filter for")
    tconsts = (
        extracted_tconsts(arguments.dataset_folder, "tt", TEST_NCONSTS)
        if not arguments.quick
        else {"tt0468569", "tt0089941", "tt11029976"}
    )
    log.info("  found %d titles", len(tconsts))
    log.info("collecting nconsts to filter for")
    nconsts = (
        extracted_tconsts(arguments.dataset_folder, "nm", tconsts)
        if not arguments.quick
        else {"nm0000616", "nm0001173"}
    )
    log.info("  found %d names", len(nconsts))
    for imdb_dataset in ImdbDataset:
        target_path = os.path.join(arguments.target_folder, imdb_dataset.filename[:-3])
        log.info("writing %s", target_path)
        line_count = 0
        with open(target_path, "w", encoding="utf-8") as target_file:
            consts_to_filter = nconsts if imdb_dataset == ImdbDataset.NAME_BASICS else tconsts
            reader = gzipped_tsv_line_reader(arguments.dataset_folder, imdb_dataset, consts_to_filter)
            for line in reader.filtered_lines():
                target_file.write(line)
                line_count += 1
        log.info("  lines written: %d", line_count)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
