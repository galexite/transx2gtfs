import argparse
from collections.abc import Sequence
from pathlib import Path

from . import convert


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="")
    parser.add_argument(
        "input",
        type=Path,
        help="Path to a TransXChange XML file or a zip file containing those files",
    )
    parser.add_argument(
        "-o",
        "--output",
        default=Path.cwd() / "gtfs.zip",
        type=Path,
        help="Output path for the generated GTFS zip file",
    )
    parser.add_argument(
        "-a",
        "--append",
        action="store_true",
        help="If specified, append to an existing GTFS zip file",
    )
    parser.add_argument(
        "-j",
        "--workers",
        default=None,
        type=int,
        help="Number of workers to use when processing the data",
    )
    parser.add_argument(
        "--max-file-size",
        default=2000,
        type=int,
        help="Maximum input file size, in megabytes",
    )

    args = parser.parse_args(argv)

    convert(args.input, args.output, args.append, args.workers, args.max_file_size)


if __name__ == "__main__":
    main()
