import os
import pickle

import polars as pl
import geopandas as gpd

def check_directory_path(path_directory:str, bool_create=True) -> int:
    int_exists = 1

    if not os.path.exists(path_directory):
        if bool_create:
            os.makedirs(path_directory)
        else:
            int_exists = 0

    return int_exists

def save_data(input_data, path_directory:str, path_filename:str, type_extension:str) -> int:
    if type_extension.lower() in ['pkl', 'pickle']:
        file_extension = '.pkl'
        with open(os.path.join(path_directory, f'{path_filename}{file_extension}'), 'wb') as handle:
            pickle.dump(input_data, handle, protocol=pickle.HIGHEST_PROTOCOL)

    if type_extension.lower() == 'csv':
        file_extension = '.csv'

    if type_extension.lower() == 'json':
        file_extension = '.json'

    if type_extension.lower() in ['geojson', 'geo']:
        file_extension = '.geojson'

    else:
        return -1

    return 0