# Ordnance Survey Ingestion Pipeline
Creating a data pipeline to ingest open Ordnance Survey products

## Table of contents
- [Tech Stack](https://github.com/SuperSalcedo22/os_open_pipeline#Ordnance-Survey)
- [Ordnance Survey](https://github.com/SuperSalcedo22/os_open_pipeline#Ordnance-Survey)
- [Ingestion Pipeline](https://github.com/SuperSalcedo22/os_open_pipeline#Ingestion-Pipeline)
- [Included in this repo](https://github.com/SuperSalcedo22/os_open_pipeline#Included-in-this-repo)

## Tech stack
#### Python
- Build on Object-oriented programming skills with a more complex use case
- Strong synergy with airflow 
#### Airflow
- Scheduling and monitoring of workflows
- Sensors allows for automation of tasks using external triggers
#### Linux for bash scripting and [Geospatial Data Abstraction Library (GDAL)](https://gdal.org/index.html)
- Operating system used for automated shell scripts
- GDAL allows for easier manipulation of geospatial (GIS) data
#### Postgres and [postgis](https://postgis.net/)
- For data storage and easy integration of geospatial data using postgis

![alt text](https://github.com/SuperSalcedo22/os_open_pipeline/blob/main/pipeline_tech_stack.png "Tech Stack")

## Ordnance Survey
The [Ordnance Survey (OS)](https://www.ordnancesurvey.co.uk/) is the national mapping agency for Great Britain (GB) which surveys and provides data for a multitude of purposes, such as urban planning, infrastructure development, environmental management and emergency response. This data can come in the following formats:
- Vector (Geography Markup Language (GML), Shapefile (SHP) and GeoJSON)
- Raster (Image datasets: TIFF and PNG)
- Tabular data (CSV)

OS provide subscription/paid for products as well as free open products that can be accessed by anyone that can create an account and connect to their API or website. The following pipeline will make use of access to their open products to download them onto a system and postgres database

## Ingestion Pipeline
### Extraction 
- Using python, connect to the API of OS and compare the system version against the latest OS version
- This is done by looping through the all the products
- If the versions don't match, the formats file is used to download the correct files for the product e.g. area size and file format
- The data is loaded onto Azure storage within a download "staging folder" before the data is transformed
### Transform data
- A lot of the files are zipped for compression, extracting all the files from these and move to the correct format
- e.g. Some data comes in tiles and needs to be moved to a grid folder rather than being all in one
- Once transformed, they're moved to the correct folder and airflow stores the value of the folder in the metabase to ensure the update is completed before it gets uploaded
- This allows a "physical" copy of the file to be kept as a backup if needed
### Loading data
- For each product, a specific dag is initialised
- This dag is externally triggered using the open dag manager that compares the current month to update month, if matching - triggers the dag
- This pokes the metabase to look for the product name and update folder
- Once found, the shell script is invoked to edit the data if needed and upload to the postgres database 

This logic is detailed in the diagram below:
![alt text](https://github.com/SuperSalcedo22/os_open_pipeline/blob/main/pipeline_logic.png "Pipeline logic")

## Included in this repo
### os_open_pipeline.py
- Full script that details the entire pipeline created as different objects
- Imported into the dag to be scheduled
### os_open_pipeline_dag.py
- Script that details the settings for the pipeline dag
- Creates the pipeline by importing the functions from os_open_pipeline.py as tasks
### os_open_formats.py
- Details the settings for multiple products
- This is read by the pipeline to ensure the correct formats and functions are used when downloading and processing each product
### os_opennames_airflow.sh
- Example shell script of os open names to be downloaded onto the postgres database
- Takes the password and update folder name from the airflow metabase as inputs to run the script
- As the password is encrypted, it won't show on the logs
### os_open_names_dag.py
- Script that details the settings for the os open names dag
- Details the poke and connects to the shell script to upload the items to the postgres database
### os_open_manager_dag.py
- Details the dag manager which externally triggers other dags
- Looks at the schedule compared to the current month and then switches on the required dags to ensure the poke process is started
### check_counts_within_10_percent_csv.sql
- SQL function that is run as a data quality check
- Checks if the csv count loaded onto the database matches the count reported by the shell script
- Following on from this, checks if the count is within 10% of the previous data, to ensure the data is somewhat congruent with a previous dataset

[Back to top](https://github.com/SuperSalcedo22/os_open_pipeline)
