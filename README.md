# Ordnance Survey Ingestion Pipeline
Creating a data pipeline to ingest open Ordnance Survey products

## Table of contents
- [Tech Stack](https://github.com/SuperSalcedo22/os_open_pipeline#Ordnance-Survey)
- [Ordnance Survey](https://github.com/SuperSalcedo22/os_open_pipeline#Ordnance-Survey)
- [Ingestion Pipeline](https://github.com/SuperSalcedo22/os_open_pipeline#Ingestion-Pipeline)


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
- Extract
- Transform
- Load


[Back to top](https://github.com/SuperSalcedo22/os_open_pipeline)
