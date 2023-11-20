'''
The master file for the os open pipeline 
'''

import os
from dotenv import load_dotenv
import requests
import sys
import logging
import pprint
import datetime as dt
import ast
import math
import urllib.parse
import zipfile
import shutil
import subprocess

from airflow import models

class BaseClass:
    '''Main class that contains global attributes and functions that the rest of the script will inherit'''

    ### class attributes

    # encrypted variables taken from airflow
    svc_p=models.Variable.get('svc_passwd')
    api_key=models.Variable.get('os_apikey')
    api_secret=models.Variable.get('os_apisecret')

    # loading environment variables
    master_path = convert_path(os.path.abspath(__file__)) # assumes the file is in the correct directory
    default_path=convert_path(f"{master_path}/os_open")
    load_dotenv(dotenv_path=convert_path(f"{master_path}/.env"))
    download_url=os.getenv("opendownload_url")
    api_open=f'{download_url}?key={api_key}'
    chunk_size=int(os.getenv("chunk"))
    networkpath=os.getenv("networkpath")

    # format data is a json format containing data about the products needed to ingest properly
    with open(convert_path(f"{default_path}/osopen_formats.txt"), 'r') as file:
        formats_str = file.read()
        formats = ast.literal_eval(formats_str)

    def __init__(self):
        self.logger = logging.getLogger(__name__) 

    def convert_path(path):
        '''Converts path to one that will work with the system being used'''
        return os.path.normpath(path)

    def list_products(self):
        '''Assuming each folder is an individual product, creates a list of the products'''
          
        # Lists all the folder within the script
        directories = [item for item in os.listdir(self.default_path) if os.path.isdir(convert_path(f"{self.default_path}/{item}"))]
  
        # removes the logs folder from the list of products
        remove=["logs"]
        folders = [x for x in directories if x not in remove]
        return folders
        
class DownloadStage(BaseClass):
    '''Class that contains the functions for the update check and download stage of the pipeline'''

    def __init__(self,product=None,os_ver=None,emap_ver=None,multiple_formats=False):
        self.multiple_formats=multiple_formats
        self.product=product
        self.os_ver=os_ver
        self.emap_ver=emap_ver

        # all os versions into one dictionary
        os_file_path = convert_path(f"{self.default_path}/osopen_update.txt")
        with open(os_file_path, 'r') as file:
            os_json_versions = file.read()
        self.product_version = ast.literal_eval(os_json_versions)

    def update_check(self):
        '''Connects to API and downloads the most recent file dates for all products. Resulting document is a text file in JSON format called osopen_update.txt'''

        # connect to API
        response = requests.get(self.api_open)

        # checks the response and exits the code if it can't connect
        if response.status_code == 200:
            data = response.json()
        else:
            raise Exception(f"Failed to connect to api, status code {response.status_code}")

        # keys are as follows
        # id, name. description, version, url

        # creates dictionary with all products and the most recent date as the key value pair
        prod = []
        version =[]
        for line in data:
            prod.append(line['id'])
            version.append(line['version'])
        update = {key: value for key, value in zip(prod, version)}

        # making the dictionary a readable format 
        pretty_dic = pprint.pformat(update)

        # writing the file to a text file that can be accessed by other scripts
        file_path = convert_path(f"{self.default_path}/osopen_update.txt")
        with open(file_path, 'w') as file:
            file.write(str(pretty_dic))

        self.logger.info("Update versions written to file")    

    def get_prod_info(self,product):
        '''Get product variables needed for download '''

        # os version date for the product
        os_ver=self.product_version[product]
        
        # emapsite version for the product
        with open(convert_path(f"{self.default_path}/{product}/{product}_latest_ver.txt"), 'r') as file:
            emap_ver = file.read()

        # checks if the format has multiple files that need to be downloaded
        multiple_formats = any("alternate" in key for key in self.formats[product].keys())

        return os_ver, emap_ver, multiple_formats

    def process_download(self):
        '''Checks if the product is up to date by comparing osopen_update.txt to {product}_latest_ver.txt'''

        self.logger.info(f"Processing {self.product}")
        # Equality comparison (case-sensitive)
        if self.os_ver == self.emap_ver:
            # If up to date, continue to next product in the loop
            self.logger.info(f"{self.product} up to date")
            return 0
        else:
            self.logger.info(f"{self.product} not up to date")
            # returns a 1 to add to counter to indicate something was downloaded
            return 1
    
    def download_version(self,alternate=None):
        '''Connects to API and downloads the file for the product, can take a secondary input if the product requires multiple formats'''

        # connect to API
        response = requests.get(f"{self.download_url}/{self.product}/downloads?key={self.api_key}")

        # checks the response and exits if not possible
        if response.status_code == 200:
            os_json = response.json()
        else:
            return self.logger.error(f"Failed to connect to api for {self.product}, status code {response.status_code}")
        
        # keys are as follows
        # md5, size, url, format, area, filename

        # Looks at different parts of a dictionary depending on number of arguments
        if alternate == None:
            search_dict=self.formats[self.product]
        else:
            search_dict=self.formats[self.product][alternate]

        # List of specific keys to be encoded
        keys_to_encode = ['area', 'format','subformat']
        # Filter the dictionary to include only the specific keys
        filtered_params = {
            key: value.encode('utf-8') if isinstance(value, str) else value
            for key, value in search_dict.items() if key in keys_to_encode
        }
        # creates the url string for the product that will be used to search for the correct url
        search_string = urllib.parse.urlencode(filtered_params, doseq=True)
        # creates the format string that will be added to the filename
        format_string=urllib.parse.quote(search_dict['format'], encoding='utf-8')
        filename=f"{self.default_path}/{self.product}/{self.product}_{self.os_ver}_{format_string}"

        # finishes the file name to include a subformat if needed
        if "subformat" in search_dict.keys():
            subformat_string=urllib.parse.quote(search_dict['subformat'], encoding='utf-8')
            filename=convert_path(f"{filename}_{subformat_string}.zip")
        else:
            filename=convert_path(f"{filename}.zip")

        # Using the url created, searches the API JSON to find the correct download link
        found = False
        for index, dict in enumerate(os_json):
            for key, value in dict.items():
                if isinstance(value, str) and search_string in value:
                    found = True
                    found_index, found_key = index, key
                    # if found breaks the looop
                    break
            if found:
                break

        # with the index found, the variables required are extracted from the dictionary
        product_download=os_json[index]['url']
        product_size=os_json[index]['size']

        # connects to the api and begins download of the product
        file_download = requests.get(product_download, stream=True)
        with open(filename, 'wb') as f:
            # Chunks the file to ensure less ram is used by the script
            total_chunks=math.ceil(product_size/self.chunk_size)
            self.logger.info(f"{self.product} split into {total_chunks} chunks")
            for chunk in file_download.iter_content(chunk_size=self.chunk_size):
                f.write(chunk)
            self.logger.info(f"{self.product} finished downloading")

        # checks the size of the file compared to the estimated size
        file_size=os.path.getsize(filename)
        if file_size < product_size:
            # this helps for debugging if the it fails
            self.logger.warning(f"{filename} not correct file size, check file isn't corrupted\nExpected size: {product_size}, File size: {file_size}\n{product_download}") 

        # Once the download is finished, updates the text file in the product folder to reflect the most recent update
        with open(convert_path(f"{self.default_path}/{self.product}/{self.product}_latest_ver.txt"), 'w') as file:
            file.write(self.os_ver)
