# ProcMine: Database Processing Tool

## Overview
ProcMine is a mineral site database processing tool, which maps structured databases (i.e., tables) to the [MinMod KG schema](https://github.com/DARPA-CRITICALMAAS/ta2-minmod-kg/tree/main/schema).

## Requirements
To run ProcMine, ensure the following requirements are met:

- **Hardware**: A local device with GPU support is recommended.
- **Software**: Docker must be installed. Follow the installation steps on the official [Docker website](https://docs.docker.com/engine/install/).
- **Memory**: At least 10 GB of available memory is suggested (actual requirements depend on the data size).
- **Network**: An active internet connection is required during execution.

ProcMine simplifies usage by providing shell scripts for common workflows. The script:

1. Clone the [MinMod data repository](https://github.com/DARPA-CRITICALMAAS/ta2-minmod-data) (if not already cloned).
2. Create and run a Docker container.
3. Export the processed data from the container
4. Push the data to the data repository.

Ensure you are running these scripts on a system capable of executing Bash scripts. If you would like to run through Docker, use follow the commands under Command (with Docker).

## Installation
1. Clone the repository
```
git clone git@github.com:DARPA-CRITICALMAAS/umn-ta2-database-processing.git
cd umn-ta2-database-processing
```

## Usage
ProcMine requires a manually curated attribute map file, which maps the headers of the raw structured data to the MinMod KG schema. An example of an attribute map CSV is provided: [sample_mapfile.csv](https://github.com/DARPA-CRITICALMAAS/umn-ta2-database-processing/blob/main/sample_mapfile.csv). Attributes that ProcMine can map to are listed under 'attribute_label'. If there is no label in the raw data representing certain label (e.g., there is no column representing 'deposit_type') leave the 'corresponding_attribute_label' empty.

### Running ProcMine (with Shell Script)
```
./run_processing.sh <path_to_raw_data> <path_to_attribute_map>
```
- `path_to_raw_data`: Path (either file or directory) to the raw mineral site database.
- `path_to_attribute_map`: Path to the CSV file containing label mapping information (refer to [sample_mapfile.csv](https://github.com/DARPA-CRITICALMAAS/umn-ta2-database-processing/blob/main/sample_mapfile.csv) for guidance)

### Running ProcMine (with Docker)
1. Build and start the Docker container
```
docker build -t ta2-processing .
docker run -dit ta2-processing
docker exec -it <container_id> /bin/bash
```
- `container_id`: Container ID of the container. Find by running `docker ps`

2. Move data into the container
```
docker cp <path_to_raw_data> <container_id>:/umn-ta2-mineral-site-linkage
docker cp <path_to_attribute_map> <container_id>:/umn-ta2-mineral-site-linkage
```
- `path_to_raw_data`: Path (either file or directory) to the raw mineral site database.
- `path_to_raw_data`: Path (either file or directory) to the raw mineral site database.
- `path_to_attribute_map`: Path to the CSV file containing label mapping information (refer to [sample_mapfile.csv](https://github.com/DARPA-CRITICALMAAS/umn-ta2-database-processing/blob/main/sample_mapfile.csv) for guidance)

3. Run the command
```
poetry run python3 process_data_to_schema.py --raw_data ./<raw_data_filename> --attribute_map ./<attribute_map_filename> --schema_output_filename <file_name>
```
- `file_name`: Desired filename of the processed raw data

4. Move the data from Docker container to local
```
exit
docker cp <container_id>:/umn-ta2-mineral-site-linkage/outputs/<file_name>.json <save_path>
```
- `save_path`: Path of directory to store the processed raw data

*Processing Time*: Several minutes depending on the size of the raw data.

## Schema
The pipeline processes the data to adhere with this [schema](https://github.com/DARPA-CRITICALMAAS/ta2-minmod-kg/blob/main/schema/README.md). We populate all possible data that can be mapped to that of the MineralSite.