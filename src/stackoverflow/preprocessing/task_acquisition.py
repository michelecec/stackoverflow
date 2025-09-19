from clearml import Task, Dataset, TaskTypes
import yaml
import tempfile
import os
import polars as pl
import shutil
import requests
import zipfile
from bs4 import BeautifulSoup
import uuid

#Class for managing the download, conversion, and upload of a Dataset
class DatasetGatherer():
    def __init__(self, project, name, number):
        self.project = project
        self.name = name
        self.number = number
        self.tempdir = os.path.join(tempfile.gettempdir(),"stackoverflow", str(uuid.uuid1()))
        os.makedirs(self.tempdir, exist_ok=True)

    def __del__(self):
        import shutil
        shutil.rmtree(self.tempdir)

    def gettempfile(self, ext: str)->str:
        return os.path.join(self.tempdir, f"{self.name}_{self.number}.{ext}")

    def get_dataset_name(self, parent: bool = False):
        num = self.number-1 if parent else self.number
        return f"{self.name}_v{num}"

    def _download_file_from_google_drive(self, id, destination):
        def get_confirm_token(response):
            soup = BeautifulSoup(response.text, "html.parser")
            download_form = soup.find("form", {"id": "download-form"})
            if download_form and download_form.get("action"):
                # Extract action URL, which might be drive.usercontent.google.com/download
                download_url = download_form["action"]
                # Collect all hidden inputs
                hidden_inputs = download_form.find_all("input", {"type": "hidden"})
                form_params = {}
                for inp in hidden_inputs:
                    if inp.get("name") and inp.get("value") is not None:
                        form_params[inp["name"]] = inp["value"]

                return download_url, form_params

            return None,None

        def save_response_content(response, destination):
            CHUNK_SIZE = 32768

            with open(destination, "wb") as f:
                for chunk in response.iter_content(CHUNK_SIZE):
                    if chunk: # filter out keep-alive new chunks
                        f.write(chunk)

        URL = "https://docs.google.com/uc?export=download"

        session = requests.Session()

        response = session.get(URL, params = { 'id' : id }, stream = True)
        download_url, form_params = get_confirm_token(response)

        if download_url and form_params:
            response = session.get(download_url, params = form_params, stream = True)

        save_response_content(response, destination)  

    #download new dataset chunck
    def download_file (self, file_url: str):
        print("Download file")
        if not os.path.exists(self.gettempfile("zip")):
            #shutil.copyfile(file_url, self.gettempfile("csv"))
            self._download_file_from_google_drive(file_url,self.gettempfile("zip"))

    def unzip_file(self):
        print("unzip the file")
        with zipfile.ZipFile(self.gettempfile("zip"), 'r') as zip_ref:
            zip_ref.extractall(self.tempdir)

    def csv_to_parquet(self):
        print("Convert csv to parquet")
        df = pl.scan_csv(os.path.join(self.tempdir, f"{self.name.title()}_chunk_{self.number}.csv"), null_values="NA")
        df = df.rename(lambda col_name: col_name.lower())
        if "body" in df.collect_schema().names():
            df = df.with_columns(parsed_body=pl.col("body").str.replace("<[^>]+>", ""))
        df.sink_parquet(self.gettempfile("parquet"))

    #create dataset, and add files
    def create_dataset(self):
        print("Create dataset")
        if self.number>1:
            ds_parent = Dataset.get(dataset_name=self.get_dataset_name(parent=True), alias=self.get_dataset_name(parent=True))
        
        ds = Dataset.create(dataset_name=self.get_dataset_name(), 
                            dataset_project=self.project,
                            parent_datasets=None if self.number==1 else [ds_parent.id])
        ds.add_files(self.gettempfile("parquet"))
        ds.finalize(auto_upload=True)

    def check_if_exist(self):
        try:
            ds = Dataset.get(dataset_name=self.get_dataset_name())
            return True
        except:
            return False

#Task for downloading a csv file, convert in parquet, and save it as clearml dataset
if __name__ == "__main__":
    with open('params.yaml', 'r') as file:
        config = yaml.safe_load(file)

    task = Task.init(project_name=config["CLEARML"]["PROJECT"],
                    task_name=config["CLEARML"]["TASK_ACQUISITION"],
                    task_type=TaskTypes.data_processing)

    args = {
        "data_name": "answers",
    }
    args = task.connect(args)
    task.execute_remotely()

    data_name = args["data_name"].upper()

    file_list = config["FILE_REPO"][data_name]
    n = 1

    for file in file_list:
        dg = DatasetGatherer(config["CLEARML"]["PROJECT"], data_name, n)
        if not dg.check_if_exist():
            dg.download_file(file_list[n-1])
            dg.unzip_file()
            dg.csv_to_parquet()
            dg.create_dataset()
        n = n+1

        print("Dataset acquired")

    print("Acquisition terminated")