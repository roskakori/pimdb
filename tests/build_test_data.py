# Copyright (c) 2020, Thomas Aglassinger.
# All rights reserved. Distributed under the BSD License.
import argparse
import logging
import os
from typing import Any, Optional

from pimdb import __version__
from pimdb.common import IMDB_DATASET_TO_KEY_COLUMNS_MAP, GzippedTsvReader, ImdbDataset, TsvDictWriter

TEST_NCONSTS = [
    # "nm0000616",  # Eric Roberts
    # "nm0001376",  # Isabelle Huppert
    # "nm0233757",  # Jaco Van Dormael
    # "nm0567408",  # Hattie McDaniel
    # "nm0707425",  # Rajinikanth
    # "nm1382571",  # Michael Ostrowski
    # "nm1801453",  # Achita Sikamana
    "nm3658287",  # Bianca Bradey
    # "nm5148470",  # Terry DeCastro
]

_DEFAULT_TARGET_FOLDER = os.path.join(os.path.dirname(__file__), "data")

log = logging.getLogger("pimdb.tests." + os.path.splitext(os.path.basename(__file__))[0])


def _parsed_arguments(args: Optional[list[str]]) -> argparse.Namespace:
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


def gzipped_tsv_reader(
    folder: str, imdb_dataset: ImdbDataset, filtered_names_to_values_map: dict[str, Any]
) -> GzippedTsvReader:
    return GzippedTsvReader(
        os.path.join(folder, imdb_dataset.filename),
        IMDB_DATASET_TO_KEY_COLUMNS_MAP[imdb_dataset],
        _log_progress,
        10,
        filtered_name_to_values_map=filtered_names_to_values_map,
    )


def _log_progress(processed_count, _):
    log.info("  processed %d rows", processed_count)


def extracted_tconsts(
    gzipped_tsv_folder: str,
    dataset: ImdbDataset,
    result_column_name: str,
    filtered_column_name: str,
    filtered_values: set[str],
) -> set[str]:
    tsv_reader = gzipped_tsv_reader(
        gzipped_tsv_folder,
        dataset,
        filtered_names_to_values_map={filtered_column_name: filtered_values},
    )
    result = {name_to_value_map[result_column_name] for name_to_value_map in tsv_reader.column_names_to_value_maps()}
    return result


def main(args: Optional[list[str]] = None):
    arguments = _parsed_arguments(args)
    log.info("collecting principals tconsts to filter for")
    principal_tconsts = (
        extracted_tconsts(arguments.dataset_folder, ImdbDataset.TITLE_PRINCIPALS, "tconst", "nconst", TEST_NCONSTS)
        if not arguments.quick
        else {"tt2535470", "tt3471694", "tt5635850"}
    )
    log.info("  found %d titles", len(principal_tconsts))
    log.info("collecting episode tconsts to filter for")
    episode_tconsts = (
        extracted_tconsts(arguments.dataset_folder, ImdbDataset.TITLE_EPISODE, "parentTconst", "tconst", TEST_NCONSTS)
        if not arguments.quick
        else {"tt3456370"}
    )
    log.info("  found %d titles", len(episode_tconsts))
    tconsts = principal_tconsts | episode_tconsts
    log.info("collecting nconsts to filter for")
    nconsts = (
        extracted_tconsts(arguments.dataset_folder, ImdbDataset.TITLE_PRINCIPALS, "nconst", "tconst", tconsts)
        if not arguments.quick
        else {"nm3658287", "nm3737504", "nm5713118"}
    )
    log.info("  found %d names", len(nconsts))
    for imdb_dataset in ImdbDataset:
        target_path = os.path.join(arguments.target_folder, imdb_dataset.filename[:-3])
        log.info("writing %s", target_path)
        line_count = 0
        with open(target_path, "w", newline="", encoding="utf-8") as target_file:
            tsv_writer = TsvDictWriter(target_file)
            filtered_name_to_values_map = {}
            if imdb_dataset == ImdbDataset.TITLE_AKAS:
                filtered_name_to_values_map["titleId"] = tconsts
            else:
                if imdb_dataset != ImdbDataset.NAME_BASICS:
                    filtered_name_to_values_map["tconst"] = tconsts
                if imdb_dataset in [ImdbDataset.NAME_BASICS, ImdbDataset.TITLE_PRINCIPALS]:
                    filtered_name_to_values_map["nconst"] = nconsts
            reader = gzipped_tsv_reader(arguments.dataset_folder, imdb_dataset, filtered_name_to_values_map)
            for name_to_value_map in reader.column_names_to_value_maps():
                tsv_writer.write(name_to_value_map)
                line_count += 1
        log.info("  lines written: %d", line_count)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
