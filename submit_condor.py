import os
import glob
import argparse
from pathlib import Path
from condor.utils import submit_condor
from analysis.configs.load_config import load_config
from analysis.filesets.utils import build_filesets


def main(args):
    args = vars(args)
    build_filesets(args)
    filesets_path = f"{Path.cwd()}/analysis/filesets/{args['year']}"
    filesets = glob.glob(f"{filesets_path}/{args['sample']}*.json")

    dataset_config = load_config(
        config_type="dataset", config_name=args["sample"], year=args["year"]
    )    
    
    output_path = Path(Path.cwd() / "outputs" / args["processor"] / args["year"])
    if not output_path.exists():
        output_path.mkdir(parents=True)
    args["output_path"] = str(output_path)

    for partition, fileset in enumerate(filesets, start=1):
        args["fileset"] = fileset
        args["cmd"] = (
            "python3 submit.py "
            f"--processor {args['processor']} "
            f"--sample {args['sample']} "
            f"--year {args['year']} "
            f"--output_path {args['output_path']} "
            f"--workers {args['workers']} "
            f"--nfiles {args['nfiles']} "
            f"--executor {args['executor']} "
            f"--fileset {args['fileset']} "
        )
        submit_condor(args)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--processor",
        dest="processor",
        type=str,
        default="tag_eff",
        help="processor to be used {tag_eff, signal}",
    )
    parser.add_argument(
        "--sample",
        dest="sample",
        type=str,
        default="",
        help="sample to be processed",
    )
    parser.add_argument(
        "--year",
        dest="year",
        type=str,
        default="2022EE",
        help="year of the data {2022EE, 2022, 2023}",
    )
    parser.add_argument(
        "--executor",
        dest="executor",
        type=str,
        default="futures",
        help="executor to be used {iterative, futures, dask} (default iterative)",
    )
    parser.add_argument(
        "--workers",
        dest="workers",
        type=int,
        default=4,
        help="number of workers to use with futures executor (default 4)",
    )
    parser.add_argument(
        "--nfiles",
        dest="nfiles",
        type=int,
        default=-1,
        help="number of .root files to be processed by sample. To run all files use -1 (default 1)",
    )
    parser.add_argument(
        "--tagger",
        dest="tagger",
        type=str,
        default="pnet",
        help="tagger {pnet, part, deepjet}",
    )
    parser.add_argument(
        "--wp",
        dest="wp",
        type=str,
        default="tight",
        help="working point {loose, medium, tight}",
    )
    parser.add_argument(
        "--flavor",
        dest="flavor",
        type=str,
        default="c",
        help="Hadron flavor {c, b}",
    )
    args = parser.parse_args()
    main(args)