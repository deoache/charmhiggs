import json
import glob
from pathlib import Path
from analysis.configs.load_config import load_config


if __name__ == "__main__":
    main_dir = Path.cwd()
    fileset_path = Path(f"{main_dir}/analysis/filesets")
        
    years_folders = glob.glob(f"{main_dir}/analysis/configs/dataset/*")
    years = [y.split("/")[-1] for y in years_folders]
    
    for year in years:
        filesets = {}
        dataset_path = f"{main_dir}/analysis/configs/dataset/{year}/"
        dataset_names = [
            f.split("/")[-1].replace(".py", "")
            for f in glob.glob(f"{dataset_path}*.py", recursive=True)
        ]
        dataset_names.remove("__init__")

        for dataset in dataset_names:
            dataset_config = load_config(config_type="dataset", config_name=dataset, year=year)
            json_file = {
                dataset_config.name: 
                    [
                        dataset_config.path + root_file
                        for root_file in dataset_config.filenames
                    ]
            }
            filesets.update(json_file)
            
        with open(f"{fileset_path}/fileset_{year}_PFNANO.json", "w") as json_file:
            json.dump(filesets, json_file, indent=4, sort_keys=True)