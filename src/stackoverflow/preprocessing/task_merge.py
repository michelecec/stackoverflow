from clearml import Task, Dataset, TaskTypes
import yaml
import os
import polars as pl
import tempfile
import shutil
import glob

def get_dataset_name(name: str, num: int)->str:
    return f"{name}_v{num}"

def get_parsed_dataset_name(name: str)->str:
    return f"{name}_merged"

if __name__ == "__main__":

    with open('params.yaml', 'r') as file:
        config = yaml.safe_load(file)

    task = Task.init(project_name=config["CLEARML"]["PREPROCESS"]["PROJECT_NAME"],
                    task_name=config["CLEARML"]["PREPROCESS"]["TASK_MERGE"],
                    task_type=TaskTypes.data_processing)

    args = {
        "data_name": "answers",
    }
    args = task.connect(args)
    task.execute_remotely()

    data_name = args["data_name"].upper()
    file_list = config["FILE_REPO"][data_name]

    print("get the dataset")
    ds = Dataset.get(dataset_name=get_dataset_name(data_name, len(file_list)),
                     alias=get_dataset_name(data_name, len(file_list)))
    ds_dir =ds.get_mutable_local_copy(os.path.join(tempfile.gettempdir(),get_dataset_name(data_name, len(file_list))),
                                      overwrite=True)

    print("merge files")
    df = pl.scan_parquet(os.path.join(ds_dir, "*.parquet"))
    file_parsed_url = os.path.join(ds_dir, f"{get_parsed_dataset_name(data_name)}.parquet")
    merged_dir = os.path.join(ds_dir,"merged")
    os.makedirs(merged_dir, exist_ok=True)
    #save new merged chunked parquet
    df.sink_parquet(pl.PartitionMaxSize(merged_dir, max_size=config["GENERAL"]["MAX_ROW_SIZE"]))
    #remove old parquet from the dataset
    for filepath in glob.glob(os.path.join(ds_dir, "*.parquet")):
        os.remove(filepath)

    print("upload new dataset")
    ds_new = Dataset.create(dataset_name=get_parsed_dataset_name(data_name), 
                            dataset_project=config["CLEARML"]["PREPROCESS"]["PROJECT_NAME"],
                            parent_datasets=[ds.id],
                            description=f"Parsed version of {get_dataset_name(data_name, len(file_list))} dataset")
    ds_new.sync_folder(ds_dir)
    ds_new.finalize(auto_upload=True)

    shutil.rmtree(ds_dir)

    print("files merged")
