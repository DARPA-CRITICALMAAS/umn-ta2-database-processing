import os
import logging

import pickle
import polars as pl
import pandas as pd
import geopandas as gpd

from shapely import wkt

def check_mode(path_file: str) -> str:
    _, file_extension = os.path.splitext(path_file)

    if file_extension:
        return file_extension
    
    return 'dir'

def check_exist(path_file):
    if os.path.exists(path_file):
        return 1
    
    return -1

def load_data(path_file, mode_data):
    _, mode_data = os.path.splitext(path_file)

    if mode_data == '.csv':
        return pl.read_csv(path_file, encoding='utf8-lossy')
    
    if mode_data == '.tsv' or mode_data == '.txt':
        # TODO: Check if this is correct
        return pl.read_csv(path_file, separator='\t', encoding='utf8-lossy')
    
    if mode_data == '.json':
        return pl.read_json(path_file)
    
    if mode_data in ['.xls', '.xlsx']:
        return pl.read_excel(path_file)
    
    if mode_data == '.pkl':
        with open(path_file, 'rb') as handle:
            return pickle.load(handle)
        
    if mode_data in ['.gdb', '.geojson', '.shp']:
        # Setting driver for geo-dataframes
        if mode_data == '.gdb':
            driver = 'OpenFileGDB'
        elif mode_data == 'geojson':
            driver = 'GeoJSON'

        # Converting longitude, latitude, crs into value
        # TODO: Replace it such that it dumps the shape
        gpd_data = gpd.read_file(path_file, driver=driver)
        gpd_data['crs'] = str(gpd_data.crs)
        gpd_data['longitude'] = gpd_data.geometry.x
        gpd_data['latitude'] = gpd_data.geometry.y

        gpd_data = gpd_data.drop('geometry', axis=1)
        pd_data = pd.DataFrame(gpd_data)
        
        return pl.from_pandas(pd_data)
    
    if not mode_data:
        # CASE directory 
        print('hello')
        pass