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
    master_path = self.convert_path(os.path.abspath(__file__)) # assumes the file is in the correct directory
    default_path=self.convert_path(f"{master_path}/os_open")
    load_dotenv(dotenv_path=self.convert_path(f"{master_path}/.env"))
    download_url=os.getenv("opendownload_url")
    api_open=f'{download_url}?key={api_key}'
    chunk_size=int(os.getenv("chunk"))
    networkpath=os.getenv("networkpath")

    # format data is a json format containing data about the products needed to ingest properly
    with open(self.convert_path(f"{default_path}/osopen_formats.txt"), 'r') as file:
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
        directories = [item for item in os.listdir(self.default_path) if os.path.isdir(self.convert_path(f"{self.default_path}/{item}"))]
  
        # removes the logs folder from the list of products
        remove=["logs"]
        folders = [x for x in directories if x not in remove]
        return folders
        
class DownloadStage(BaseClass):
    '''Class that contains the functions for the update check and download stage of the pipeline'''

    def __init__(self,product=None,os_ver=None,system_ver=None,multiple_formats=False):
        self.multiple_formats=multiple_formats
        self.product=product
        self.os_ver=os_ver
        self.system_ver=system_ver

        # all os versions into one dictionary
        os_file_path = self.convert_path(f"{self.default_path}/osopen_update.txt")
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
        file_path = self.convert_path(f"{self.default_path}/osopen_update.txt")
        with open(file_path, 'w') as file:
            file.write(str(pretty_dic))

        self.logger.info("Update versions written to file")    

    def get_prod_info(self,product):
        '''Get product variables needed for download '''

        # os version date for the product
        os_ver=self.product_version[product]
        
        # emapsite version for the product
        with open(self.convert_path(f"{self.default_path}/{product}/{product}_latest_ver.txt"), 'r') as file:
            system_ver = file.read()

        # checks if the format has multiple files that need to be downloaded
        multiple_formats = any("alternate" in key for key in self.formats[product].keys())

        return os_ver, system_ver, multiple_formats

    def process_download(self):
        '''Checks if the product is up to date by comparing osopen_update.txt to {product}_latest_ver.txt'''

        self.logger.info(f"Processing {self.product}")
        # Equality comparison (case-sensitive)
        if self.os_ver == self.system_ver:
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
            filename=self.convert_path(f"{filename}_{subformat_string}.zip")
        else:
            filename=self.convert_path(f"{filename}.zip")

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
        with open(self.convert_path(f"{self.default_path}/{self.product}/{self.product}_latest_ver.txt"), 'w') as file:
            file.write(self.os_ver)

class UploadStage(BaseClass):
    '''Class reponsible for processing downloaded items and uploading them to the network drive'''

    def __init__(self,item_name=None,product=None):
        self.product=product
        self.item_name=item_name

    def backup_files(self,initialFolder,backupFolder,destFolder):
        '''Creates backups of files in a backup folder before moving to a destination folder'''
        # lists all the files in the inital folder
        allFiles = os.listdir(initialFolder)

        # looks for files that would be overwritten in the destination folder
        # Move each old file to the backup folder incase of an issue
        for file in allFiles:
            dest_path = self.convert_path(f"{destFolder}/{file}")
            backup_path = self.convert_path(f"{backupFolder}/{file}")
            try:
                shutil.move(dest_path, backup_path)
            except FileNotFoundError:
                # if the file is in the inital folder but not the destination folder, will throw up an error, but that doesn't affect the script from running
                pass

        self.logger.info(f"Backup for {destFolder} created in {backupFolder}")
        
        # moves the files to the destination folder
        for file in allFiles:
            inital_path = self.convert_path(f"{initialFolder}/{file}")
            destination_file_path = self.convert_path(f"{destFolder}/{file}")
            shutil.move(inital_path, destination_file_path)

    def extract_to_one_folder(self,datafolder,rollbackfolder,rootPath,stagingPath):
        '''Extracts all files from subdirectories and puts them into one new folder'''

        # looks for all files within the path
        for root, _, files in os.walk(rootPath):
            for file in files:
                # creates the full path of the file
                filepath = self.convert_path(f"{root}/{file}")
                destination_file_path = self.convert_path(f"{stagingPath}/{file}")
                # copies all files into the staging folder
                shutil.move(filepath, destination_file_path)

        self.logger.info(f"Extracted to {stagingPath}")

        # onces the staging folder is populated, starts the function that creates a backup within the rollbackfolder
        self.backup_files(stagingPath,rollbackfolder,datafolder)

    def unzip_to_one_folder(self,datafolder,rollbackfolder,rootPath,unzipPath):
        '''Specific for os terrain 50, unzips every file and puts into a different folder'''

        # unzips the files
        for root, _, files in os.walk(rootPath):
            # looks for all files within the folder that ends in zip
            for file in files:
                if file.lower().endswith('.zip'):
                    # creates the full path of the file
                    zip_file_path = self.convert_path(f"{root}/{file}")
                    # extracts all items to the destination path (the unzip folder)
                    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                        zip_ref.extractall(unzipPath)

        self.logger.info(f"Extracted to {unzipPath}")

        # onces the unzip folder is populated, starts the function that creates a backup within the rollbackfolder
        self.backup_files(unzipPath,rollbackfolder,datafolder)

    def choose_process(self,**kwargs):
        '''Chooses the correct processing function for a product'''
        process=kwargs.get("p")
        # map that determines the function to run based on the process input
        function_map = {
            "staging": self.staging_processing,
            "multiple": self.multiple_processing,
            "unzip": self.unzip_processing,
            "rename": self.rename_processing,
            "backup": self.backup_processing
        }
        # Using the dictionary and input, runs the correct function 
        # !! the kwargs variable formed is passed through into the correct function
        return function_map[process](kwargs)

    def staging_processing(self,kwargs):
        '''For products that require items to be extracted to a staging folder before being put into the correct folder'''
        xcom_basename=kwargs.get('xb')
        self.logger.info(f"Staging {xcom_basename}")
        # products that require files to be moved into one single folder from multiple subdirectories
        # products will be moved to a staging folder before being copied onto the most recent datasets
        stagingfolder=self.convert_path(f"{self.networkpath}/{kwargs.get('fd')['staging']}")
        self.extract_to_one_folder(kwargs.get('df'),kwargs.get('rf'),kwargs.get('uf'),stagingfolder)
        return xcom_basename

    def multiple_processing(self,kwargs):
        '''Miniscale has multiple instances that need to be backed up in different folders'''
        xcom_basename=kwargs.get('xb')
        self.logger.info(f"Processing multiple instances for {xcom_basename}")
        # miniscale requires multiple different folders that need to be processed
        # simple move and backup repeated
        for subfolder in kwargs.get('fd')['data_folder']:
            MSupdatefolder=self.convert_path(f"{kwargs.get('up')}/{kwargs.get('st')}/{subfolder}")
            MSdatafolder=self.convert_path(f"{self.networkpath}/{kwargs.get('nf')}/{subfolder}")
            MSrollbackfolder=self.convert_path(f"{kwargs.get('rf')}/{subfolder}")
            # runs the update and backup everytime for all subfolders
            self.backup_files(MSupdatefolder,MSrollbackfolder,MSdatafolder)
        return xcom_basename

    def unzip_processing(self,kwargs):
        '''For products that require files to be unzipped before backing up'''
        xcom_basename=kwargs.get('xb')
        self.logger.info(f"Unzipping {xcom_basename}")
        # Terrain 50 requires all files to be unzipped into one folder before being copied
        unzipfolder=self.convert_path(f"{self.networkpath}/{kwargs.get('fd')['unzip']}")
        self.unzip_to_one_folder(kwargs.get('df'),kwargs.get('rf'),kwargs.get('uf'),unzipfolder)
        return xcom_basename

    def rename_processing(self,kwargs):
        '''Open Names requires the file name to be changed for the bash script to run'''
        xcom_basename=kwargs.get('xb')
        self.logger.info(f"Renaming {xcom_basename}")
        # OpenNames doesn't require a rollback, just a rename of the update file
        filedate=self.item_name.split("_")[1]
        # renames the directory to conform with the bash script made
        newpath=self.convert_path(f"{self.networkpath}/{kwargs.get('nf')}/Updates/opname_csv_gb_{filedate}")
        os.rename(kwargs.get('up'),newpath)
        # as the file is renamed, wil have to change the xcom
        xcom_basename=os.path.basename(newpath)
        return xcom_basename

    def backup_processing(self,kwargs):
        '''For products that only require the files to be backed up in a folder before copying to correct path'''
        xcom_basename=kwargs.get('xb')
        self.logger.info(f"Backing up {xcom_basename}")
        # most of the folders will only require a simple move and backup
        self.backup_files(kwargs.get('uf'),kwargs.get('rf'),kwargs.get('df'))
        return xcom_basename

    def preprocess_upload(self,folder):
        '''Processes a folder by unzipping downloaded folders and generates list of items to be uploaded'''

        # finds the zip files within the folder
        zips = [files for files in os.listdir(self.convert_path(f"{self.default_path}/{folder}")) if files.endswith(".zip")]
        current_path=f"{self.default_path}/{folder}"
        # Unzip all the files into sub folders with the same name
        for zip in zips:
            folder_name=zip.replace(".zip","")
            # Create the destination folder without the zip extension
            folder_create=self.convert_path(f"{current_path}/{folder_name}")
            if not os.path.exists(folder_create):
                os.makedirs(folder_create)
            # Unzip the ZIP file into the folder
            with zipfile.ZipFile(self.convert_path(f"{current_path}/{zip}"), 'r') as zip_ref:
                zip_ref.extractall(folder_create)
            self.logger.info(f"{folder_create} extracted")
        
        # listing all the items not including the text file (that contains the update dates)
        folder_items = [folders for folders in os.listdir(self.convert_path(f"{self.default_path}/{folder}")) if ".txt" not in str(folders)]

        return folder_items

    def extract_to_network_drive(self):
        '''Uploads file to network drive'''

        #############################
        # Uploads the file to the updates folder of the network drive to ensure a copy is retained

        # checks if the format has multiple files that affect the dictionary
        multiple_formats = any("alternate" in key for key in self.formats[self.product].keys())

        # If contains multiple formats, will run a slightly different set of code
        found=False
        # extracts the correct network folder from the dictionary (it doesn't match the folder names within the auto download folder
        if multiple_formats == False:
            format_dictionary=self.formats[self.product]
            found=True
        else:
            # removes the .zip to make finding the items within the dictionary easier
            item_search = self.item_name.replace(".zip","")
            # changes URl named folder back to a normal string to find the network folder
            # removes the product name and date from the file, leaving only the formats and subformats to search
            search_string=urllib.parse.unquote('_'.join(item_search.split("_")[2:]),encoding='utf-8')

            # looks for the string in the dictionary
            # if the string is found, will break the for loop and use the alternative as an index within the dictionary
            for alternative in self.formats[self.product]:
                search_area=str(self.formats[self.product][alternative])
                if search_string in search_area:
                    found=True
                    break
                elif len(item_search.split("_")) == 4:
                    # checks if the folder name contains sub formats by checking the length of the split string
                    # will now check if the subformat and format is within the dictionary
                    search_string2=urllib.parse.unquote(search_string.split("_")[-1],encoding='utf-8')
                    if search_string2 in search_area and search_string.split("_",1)[0] in search_area:
                        found=True
                        break

            # with the index found, the specific sub dictionary is assigned for the file
            format_dictionary=self.formats[self.product][alternative]
        
        # breaks code if the variable isn't found
        if not found:
            raise Exception("Could not find variables from dictionary")

        # defines the variables needed from the dictionary
        networkfolder=format_dictionary['network_folder']
        structure=format_dictionary['structure']

        # shortening the names to make the function more readable
        folder_path=self.convert_path(f"{self.default_path}/{self.product}/{self.item_name}")
        upload_path=self.convert_path(f"{self.networkpath}/{networkfolder}/Updates/{self.item_name}")

        # check if the path already exists 
        if not os.path.exists(upload_path):
            # moves file, faster than copying
            shutil.move(folder_path, upload_path)
            self.logger.info(f"Copied to {upload_path}")
        else:
            # dont perform the move and say on the log
            self.logger.error(f"{upload_path} already exists")

        # exit the function if a zip file as the following actions cannot be performed on it
        if ".zip" in str(self.item_name):
            # returns zip to ensure the correct xcom is pushed
            return "zip"

        ##############################
        # Part of the function that begins to upload the data from the folder to the appropiate network folders

        # sorting out the xcoms to communicate if the write task needs to run
        xcom_basename=os.path.basename(upload_path)

        # defining variables needed for the backup
        datafolder=self.convert_path(f"{self.networkpath}/{format_dictionary['data_folder']}")
        rollbackfolder=self.convert_path(f"{self.networkpath}/{format_dictionary['rollback_folder']}")
        updatefolder=self.convert_path(f"{upload_path}/{structure}")
        process=format_dictionary['process']
        
        # chooses how the data will be processed
        xcom_basename=self.choose_process(p=process,xb=xcom_basename,fd=format_dictionary,df=datafolder,rf=rollbackfolder,uf=updatefolder,nf=networkfolder,st=structure,up=upload_path)
        
        self.logger.info(f"{datafolder} updated with latest data")
        
        # Appends to the text file as a separate log to keep in track
        current_time = dt.datetime.now()
        with open(self.convert_path(f"{self.default_path}/logs/download_log.txt"), 'a') as checker:
            checker.write(f"{current_time}: {self.product} synched \n{upload_path}\n CHECK {format_dictionary['rollback_folder']} \n \n")

        try:
            # deletes the unzipped folder from the network drive
            shutil.rmtree(upload_path)
        except FileNotFoundError:
            # if the file was renamed during the update, will throw up error
            # this just allows the script to keep running 
            pass

        # needs to return the network path for the xcom push
        return xcom_basename

################################################################
# Functions that actually run the code

def download_data(ti=None):
    # create the overall object and check for the most recent updates
    download_obj=DownloadStage()
    DownloadStage.update_check()

    # list products that need to be iterated through for download
    folders=download_obj.list_products()
    
    # creates a download counter to check if anything was downloaded
    download_counter=0
    for item in folders:
        # gets the dates for the specific product
        os_ver,system_ver,multiple_formats=download_obj.get_prod_info(item)
        # create an instance for each product with the above information
        product_instance=DownloadStage(item,os_ver,system_ver,multiple_formats)

        # checks if the product requires a download or not
        download_required=product_instance.process_download()
        download_counter+=download_required
        # begins the download process if it returns a non zero value
        if download_required != 0:
            if product_instance.multiple_formats == False:
                product_instance.download_version()
            else:
                # If contains multiple formats, will loop through all the alternate formats
                for prod_format in product_instance.formats[product_instance.product]:
                    product_instance.logger.info(f"Downloading {product_instance.formats[product_instance.product][prod_format]['format']} for {product_instance.product}")
                    product_instance.download_version(prod_format) 
    
    # pushes the xcom to check if the script needs to be skipped or not
    ti.xcom_push(key="download_count", value=download_counter)

def upload(ti=None):
    '''Main function for the upload task'''

    # create the overall object and list the products that need to be checked
    upload_obj=UploadStage()
    folders=upload_obj.list_products()

    for directory in folders:
        # finds all the items within each folder
        folder_items=upload_obj.preprocess_upload(directory)
        for item in folder_items:
            # creates an object for each item that is found
            upload_item=UploadStage(item,directory)

            # starts the process for each item within the folder
            output=upload_item.extract_to_network_drive()

            # Ensures the files are deleted from initial download location as sometimes they exist after the move
            local_path=upload_item.convert_path(f"{upload_item.default_path}/{directory}/{item}")
            try:
                # if a zip file, simple delete of the file
                os.remove(local_path)
            except PermissionError:
                # if a folder, will have to delete the entire path
                shutil.rmtree(local_path)
            except FileNotFoundError:
                # if the file was successfully moved it should no longer exist so pass to skip the error
                pass
            
            # wont push the xcom if the filename contains any of these words
            exclude=[".zip","geotiff",".tif"]
            # pushes the xcom value
            if any(s.lower() not in output.lower() for s in exclude):
                ti.xcom_push(key=directory, value=output)
