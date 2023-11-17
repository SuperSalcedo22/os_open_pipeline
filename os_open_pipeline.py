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

################################################################
# General functions for the whole script

def convert_path(path):
    '''Converts path to one that will work with the system being used'''
    return os.path.normpath(path)

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

    def list_products(self):
        '''Assuming each folder is an individual product, creates a list of the products'''
          
        # Lists all the folder within the script
        directories = [item for item in os.listdir(self.default_path) if os.path.isdir(convert_path(f"{self.default_path}/{item}"))]
  
        # removes the logs folder from the list of products
        remove=["logs"]
        folders = [x for x in directories if x not in remove]
        return folders
