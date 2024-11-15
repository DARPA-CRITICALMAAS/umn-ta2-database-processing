import os
import logging

import pickle
import polars as pl
import pandas as pd
import geopandas as gpd

from shapely import wkt

def check_file_path(path_file): 
    if os.path.isfile(path_file):
        return 1
    
    return 0

def load_data(path_file):
    _, filename_extension = os.path.splitext(path_file)

    if filename_extension == '.csv':
        return pl.read_csv(path_file)
    
    if filename_extension == '.json':
        return pl.read_json(path_file)
    
    if filename_extension in ['.xls', '.xlsx']:
        return pl.read_excel(path_file)
    
    if filename_extension == '.pkl':
        with open(path_file, 'rb') as handle:
            return pickle.load(handle)
        
    if filename_extension in ['.gdb', '.geojson', '.shp']:
        # Setting driver for geo-dataframes
        if filename_extension == '.gdb':
            driver = 'OpenFileGDB'
        elif filename_extension == 'geojson':
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
    
    return 0