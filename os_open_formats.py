'''
Example format file that contains the details needed for the pipeline for each product
'''

import os
import urllib.parse
import sys
import pprint
import ast

# default variables
default_path= os.path.dirname(os.path.abspath(__file__)).replace("\\","/")

# Nested dictionary of formats and subformats
##############################
# 'area': ensures full gb coverage is downloaded
# 'format': specifying the correct file format
# 'network_folder': the name of the folder on the shared drive
# 'data_folder': where the most recent data is stored and accessed (data will be copied here)
# 'rollback_folder':  relative path that moves files are backed up to incase of a corrupted update
# 'structure': used to find the correct structure of the file when uploading to the shared drive
# 'process': used for the function finder to process file on the shared drive
# 'schedule': months that product should be updated, used to start the dag for automatic upload
# 'dag_id': used to identify the correct dag that automatically uploads the file to the postgres database
# will also contain product specific keys e.g. rename that helps the process of the file

nested_dict = {
    '250kScaleColourRaster': {
        'area': 'GB',
        'format': 'TIFF-LZW',
        'network_folder': '250k',
        'data_folder':'250k', 
        'rollback_folder': '250k/Rollback',
        'structure': 'ras250_gb/data', 
        'process': 'backup'
    },
    'BoundaryLine': {
        'area': 'GB',
        'format': 'ESRI® Shapefile',
        'network_folder': 'Boundary_Line',
        'data_folder':'Boundary_Line',
        'rollback_folder': 'Boundary_Line/Rollback',
        'structure': 'Data/GB',
        'process': 'backup',
        'schedule': ["May","October"], 
        'dag_id':'OS_BoundaryLine'
    },
    'CodePointOpen': {
        'area': 'GB',
        'format': 'CSV',
        'network_folder': 'Codepoint_Open',
        'data_folder':'Codepoint_Open/CSV',
        'rollback_folder': 'Codepoint_Open/Rollback',        
        'structure': 'Data/CSV',
        'process': 'backup'
    },
    'MiniScale': {
        'area': 'GB',
        'format': 'Zip file (containing EPS, Illustrator and TIFF-LZW)',
        'network_folder': 'MiniScale',
        'data_folder':["Editable_EPS","Illustrator_CC","RGB_TIF_compressed"],
        'rollback_folder': 'MiniScale/Rollback',        
        'structure': 'data',
        'process': 'multiple'
    },
    'OpenGreenspace': {
        'area': 'GB',
        'format': 'ESRI® Shapefile',
        'network_folder': 'Greenspace/Greenspace_Open',
        'data_folder': 'Greenspace/Greenspace_Open/Data',
        'rollback_folder': 'Greenspace/Greenspace_Open/Rollback',        
        'structure': 'OS Open Greenspace (ESRI Shape File) GB/data',
        'schedule': ["May","November"],
        'dag_id':'OS_OpenGreenspace',
        'process': 'backup'
    },
    'OpenMapLocal': {
        'alternate1' : {
            'area': 'GB',
            'format': 'GeoTIFF',
            'subformat': 'Full Colour',
            'network_folder': 'Openmap_Local_Raster',
            'data_folder': 'Openmap_Local_Raster/Full_Colour',
            'rollback_folder': 'Openmap_Local_Raster/Rollback',   
            'staging' : 'Openmap_Local_Raster/Updates/Staging',     
            'structure': 'data',
            'process': 'staging'
        },
        'alternate2' : {
            'area': 'GB',
            'format': 'ESRI® Shapefile',
            'network_folder': 'OpenMap_Local',
            'data_folder': 'OpenMap_Local/DATA',
            'rollback_folder': 'OpenMap_Local/Rollback',        
            'structure': 'data',
            'process': 'backup',
            'schedule': ["April","October"],
            'dag_id':'OS_OpenMapLocal'
        }
    },
    'OpenNames' : {
        'area' : 'GB',
        'format' : 'CSV',
        'network_folder': 'OpenNames',
        'rename': True,
        'structure': "",
        'data_folder':"",
        'rollback_folder': "",
        'process': 'rename',
        'schedule': ["February","June","October"],
        'dag_id':'OpenNames'
    },
    'OpenRivers': {
        'area': 'GB',
        'format': 'ESRI® Shapefile',
        'network_folder': 'OpenRivers',
        'data_folder': 'OpenRivers/data',
        'rollback_folder': 'OpenRivers/Rollback',        
        'structure': 'data',
        'process': 'backup',
        'schedule': ["April","October"],
        'dag_id':'OS_OpenRivers'
    },
    'OpenRoads': {
        'area': 'GB',
        'format': 'ESRI® Shapefile',
        'network_folder': 'OpenRoads',
        'data_folder': 'OpenRoads/Data',
        'rollback_folder': 'OpenRoads/Rollback',        
        'structure': 'data',
        'process': 'backup',
        'schedule': ["April","October"],
        'dag_id':'OS_OpenRoads'
    },
    'OpenZoomstack': {
        'area': 'GB',
        'format': 'GeoPackage',
        'network_folder': 'Zoomstack',
        'data_folder': 'Zoomstack',
        'rollback_folder': 'Zoomstack/Rollback',        
        'structure': "",
        'process': 'backup',
        'schedule': ["June","December"],
        'dag_id':'OS_OpenZoomstack'
    },
    'Terrain50': {
        'alternate1': {
            'area': 'GB',
            'format': 'ASCII Grid and GML (Grid)',
            'network_folder': 'Terrain_50',
            'data_folder': 'Terrain_50/ASC',
            'rollback_folder': 'Terrain_50/Rollback/ASC',
            'unzip': 'Terrain_50/Updates/Staging_unzip',        
            'structure': 'data',
            'process': 'unzip'
        },
        'alternate2': {
            'area': 'GB',
            'format': 'GML',
            'subformat': '(Contours)',
            'network_folder': 'Terrain_50',
            'data_folder': 'Terrain_50/Contours/ALL',
            'rollback_folder': 'Terrain_50/Rollback/Contours',
            'unzip': 'Terrain_50/Updates/Staging_unzip',        
            'structure': 'data',
            'process': 'backup'
        }
    },
    'VectorMapDistrict': {
        'alternate1': {
            'area': 'GB',
            'format': 'ESRI® Shapefile',
            'network_folder': 'VectorMapDistrict',
            'data_folder': 'VectorMapDistrict/SHP',
            'rollback_folder': 'VectorMapDistrict/Rollback',
            'staging' : 'VectorMapDistrict/Updates/Staging',        
            'structure': 'data',
            'process': 'staging',
            'schedule': ["May","November"],
            'dag_id':'OS_VectorMapDistrict'
        },
        'alternate2': {
            'area': 'GB',
            'format': 'GeoTIFF',
            'subformat': 'Backdrop',
            'network_folder': 'VectorMap_District_Raster',
            'data_folder': 'VectorMap_District_Raster/Backdrop',
            'rollback_folder': 'VectorMap_District_Raster/Rollback/Backdrop',
            'staging' : 'VectorMap_District_Raster/Updates/Staging',        
            'structure': 'data',
            'process': 'staging'
        },
        'alternate3': {
            'area': 'GB',
            'format': 'GeoTIFF',
            'subformat': 'Full Colour',
            'network_folder': 'VectorMap_District_Raster',
            'data_folder': 'VectorMap_District_Raster/Colour',
            'rollback_folder': 'VectorMap_District_Raster/Rollback/Colour',
            'staging' : 'VectorMap_District_Raster/Updates/Staging',        
            'structure': 'data',
            'process': 'staging'
        }
    }
}

# making the dictionary a readable format 
pretty_dic = pprint.pformat(nested_dict)

# writing the file to a text file that can be accessed by other scripts
file_path = f"{default_path}/osopen_formats.txt"
with open(file_path, 'w', encoding='utf-8') as file:
    file.write(str(pretty_dic))

# # checking the file is the correct format
# with open(f"{default_path}/osopen_formats.txt", 'r') as file:
#     emap_ver = file.read()
#     formats = ast.literal_eval(emap_ver)
#     print(formats)
