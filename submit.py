import json
import time
import pickle
import argparse
from coffea import processor
from humanfriendly import format_timespan
from analysis.processors.signal import SignalProcessor
from analysis.processors.tag_eff import TaggingEfficiencyProcessor
from coffea.nanoevents import NanoEventsFactory, PFNanoAODSchema


def main(args):
    # define processors and executors
    processors = {
        "tag_eff": TaggingEfficiencyProcessor,
        "signal": SignalProcessor,
    }
    processor_args = {
        "tag_eff": {
            "year": args.year,
            "tagger": args.tagger,
            "flavor": args.flavor,
            "wp": args.wp,
        },
        "signal": {
            "year": args.year,
        }
    }
    executors = {
        "iterative": processor.iterative_executor,
        "futures": processor.futures_executor,
        "dask": processor.dask_executor,
    }
    executor_args = {
        "schema": PFNanoAODSchema,
    }
    if args.executor == "futures":
        executor_args.update({"workers": 4})
        
    # load fileset and execute the processor
    with open(args.fileset) as f:
        fileset = json.load(f)
    fileset_key = args.fileset.split("/")[-1].replace(".json", "")
    
    t0 = time.monotonic()
    out = processor.run_uproot_job(
        fileset,
        treename="Events",
        processor_instance=processors[args.processor](**processor_args[args.processor]),
        executor=executors[args.executor],
        executor_args=executor_args,
    )
    exec_time = format_timespan(time.monotonic() - t0)

    # save processor output and metadata
    metadata = {"walltime": exec_time}
    metadata.update({"fileset": fileset[fileset_key]})
    if "metadata" in out[fileset_key]:
        output_metadata = out[fileset_key]["metadata"]
        metadata.update({"sumw": float(output_metadata["sumw"])})
    
    with open(f"{args.output_path}/{fileset_key}_metadata.json", "w") as f:
        f.write(json.dumps(metadata))
    with open(f"{args.output_path}/{fileset_key}.pkl", "wb") as handle:
        pickle.dump(out[fileset_key]["histograms"], handle, protocol=pickle.HIGHEST_PROTOCOL)


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
        "--executor",
        dest="executor",
        type=str,
        default="futures",
        help="executor to run the processor {iterative, futures}",
    )
    parser.add_argument(
        "--fileset",
        dest="fileset",
        type=str,
        default="",
        help="fileset path",
    )
    parser.add_argument(
        "--year",
        dest="year",
        type=str,
        default="2022EE",
        help="year of the data {2022EE}",
    )
    parser.add_argument(
        "--output_path",
        dest="output_path",
        type=str,
        default="",
        help="output path",
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

    args = parser.parse_args()
    main(args)