import json
import glob
from pathlib import Path
from collections import OrderedDict
from analysis.configs.load_config import load_config


def divide_list(lst: list, n: int) -> list:
    """Divide a list into n sublists"""
    size = len(lst) // n
    remainder = len(lst) % n
    result = []
    start = 0
    for i in range(n):
        if i < remainder:
            end = start + size + 1
        else:
            end = start + size
        result.append(lst[start:end])
        start = end
    return result


def build_filesets(args: dict) -> None:
    """
    build filesets partitions for an specific sample fileset
    """
    main_dir = Path.cwd()

    # make output filesets directory
    fileset_path = Path(f"{main_dir}/analysis/filesets")
    output_directory = Path(f"{fileset_path}/{args['year']}/")
    if output_directory.exists():
        for file in output_directory.glob(f"{args['sample']}*"):
            if file.is_file():
                file.unlink()
    else:
        output_directory.mkdir(parents=True)
    # read json file with PFNano fileset
    json_file = f"{fileset_path}/fileset_{args['year']}_PFNANO.json"
    with open(json_file, "r") as handle:
        root_files = json.load(handle)[args["sample"]]
    # generate and save fileset partitions
    filesets = {}
    dataset_config = load_config(
        config_type="dataset", config_name=args["sample"], year=args["year"]
    )
    if dataset_config.partitions == 1:
        filesets[args["sample"]] = f"{output_directory}/{args['sample']}.json"
        sample_data = {args["sample"]: root_files}
        with open(f"{output_directory}/{args['sample']}.json", "w") as json_file:
            json.dump(sample_data, json_file, indent=4, sort_keys=True)
    else:
        root_files_list = divide_list(root_files, dataset_config.partitions)
        keys = ".".join(
            f"{args['sample']}_{i}" for i in range(1, dataset_config.partitions + 1)
        ).split(".")
        for key, value in zip(keys, root_files_list):
            sample_data = {}
            sample_data[key] = list(value)

            filesets[key] = f"{output_directory}/{key}.json"
            with open(f"{output_directory}/{key}.json", "w") as json_file:
                json.dump(sample_data, json_file, indent=4, sort_keys=True)