import os
import logging

import pickle
import polars as pl
import pandas as pd
import geopandas as gpd

from shapely import wkt

def return_basename(path_file:str) -> str:
    file_name, _ = os.path.splitext(os.path.basename(path_file))

    return file_name

def check_mode(path_file: str) -> str:
    _, file_extension = os.path.splitext(path_file)

    if file_extension:
        return file_extension
    
    return 'dir'

def check_exist(path_file):
    if os.path.exists(path_file):
        return 1
    
    return -1

def load_data(path_file, mode_data) -> pl.DataFrame | dict:
    """
    Returns all data in form of pl DataFrame
    """
    if mode_data == '.csv':
        return pl.read_csv(path_file, encoding='utf8-lossy', ignore_errors=True)
    
    if mode_data == '.tsv' or mode_data == '.txt':
        # TODO: Check if this is correct
        return pl.read_csv(path_file, separator='\t', encoding='utf8-lossy', ignore_errors=True)
    
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

        gpd_data = gpd.read_file(path_file, driver=driver)
        gpd_data['crs'] = str(gpd_data.crs)
        gpd_data['location'] = gpd_data['geometry'].apply(wkt.dumps)

        gpd_data = gpd_data.drop('geometry', axis=1)
        pd_data = pd.DataFrame(gpd_data)
        
        return pl.from_pandas(pd_data)
    
    if mode_data == 'dir':
        list_subdata = []

        for i in os.listdir(path_file):
            pl_subdata = load_data(i)
            list_subdata.append(pl_subdata)

        pl_data = pl.concat(
            list_subdata,
            how='align'
        )

        return pl_data
        