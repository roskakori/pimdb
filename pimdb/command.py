import argparse
import logging
import os
from typing import List, Optional

from pimdb.common import download_imdb_dataset, log, ImdbDataset, IMDB_DATASET_NAMES

_DEFAULT_LOG_LEVEL = "info"
_VALID_LOG_LEVELS = ["debug", "info", "warning"]
_LOG_LEVEL_MAP = {
    "info": logging.INFO,
}
_ALL_NAME = "all"
_VALID_NAMES = [_ALL_NAME] + IMDB_DATASET_NAMES


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

    subparsers = result.add_subparsers(dest="command", help="command to run")

    download_parser = subparsers.add_parser("download", help="download IMDb datasets")
    download_parser.add_argument(
        "names",
        metavar="NAME",
        nargs="+",
        choices=_VALID_NAMES,
        default=_ALL_NAME,
        help=f"name(s) of IMDb datasets to download; valid names: {', '.join(_VALID_NAMES)}; default: %(default)s",
    )
    download_parser.add_argument("--out", "-o", default="", help="output folder; default: current folder")
    return result


def _download(parser, args):
    if _ALL_NAME in args.names:
        if len(args.names) >= 2:
            parser.error(f'if NAME "{_ALL_NAME}" is specified, it must be the only NAME')
        dataset_names_to_download = IMDB_DATASET_NAMES
    else:
        # Remove possible duplicates and sort.
        dataset_names_to_download = sorted(set(args.names))

    for dataset_name_to_download in dataset_names_to_download:
        target_path = os.path.join(args.out, ImdbDataset(dataset_name_to_download).filename)
        download_imdb_dataset(ImdbDataset(dataset_name_to_download), target_path)


def main(arguments: Optional[List[str]] = None):
    parser = _parser()
    args = parser.parse_args(arguments)
    log.setLevel(logging.getLevelName(args.log.upper()))
    if args.command == "download":
        _download(parser, args)
    else:
        assert False, f"args={args}"


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
