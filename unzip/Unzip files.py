# Databricks notebook source
# MAGIC %md
# MAGIC #Unzip files in a volume
# MAGIC Informe the location of the file: <catalog>.<schema>.<volume_name> and execute.
# MAGIC At this time overwrite the files and can be used to unzip many files at the same time. 

# COMMAND ----------

# DBTITLE 1,Prepare
import zipfile
import os
import glob

dbutils.widgets.text("catalog", defaultValue="default_catalog", label="Catalog")
dbutils.widgets.text("schema", defaultValue="default_schema", label="Schema")
dbutils.widgets.text("volume_name", defaultValue="default_volume", label="Volume Name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume_name = dbutils.widgets.get("volume_name")

path = f"/Volumes/{catalog}/{schema}/{volume_name}"

# COMMAND ----------

# Define the path to the zip file and the extraction directory
files_to_extract = glob.glob(f'{path}/*.zip')

print(f"Extracting {len(files_to_extract)} files")
counter = len(files_to_extract)

for one_file in files_to_extract:
    prefix = (one_file.split("/")[-1])[0:2]
    extract_dir = f'{"/".join(one_file.split("/")[0:-1])}/__extracted/'
    print(f"Now extracting: {one_file} into {extract_dir}")

    # Check if the extraction directory exists, create if it doesn't
    if not os.path.exists(extract_dir):
        os.makedirs(extract_dir)

    # Unzip the file
    with zipfile.ZipFile(one_file, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
    counter -= 1
    print(f"Extracted {len(files_to_extract)-counter} files")
