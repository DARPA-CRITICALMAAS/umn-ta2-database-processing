import os
import pickle
from json import loads, dump
# import orjson

import polars as pl
import geopandas as gpd

pl.Config.set_fmt_float("full") # Convert scientific notation to float

def check_directory_path(path_directory:str, bool_create=True) -> int:
    int_exists = 1

    if not os.path.exists(path_directory):
        if bool_create:
            os.makedirs(path_directory)
        else:
            int_exists = -1

    return int_exists

def drop_nones(input_object: dict | list) -> dict | list:
    """
    Recursively remove all None values from either a dictionary or a list, and returns a new dictionary or list without the None values

    Arguments:
    input_object = either a dictionary or a list type that may or may not consist of a None value
    
    Return
    either a dictionary or a list type (same as the input) that does not consists of any None values
    """

    # List case
    if isinstance(input_object, list):
        list_objects = []
        for x in input_object:
            if x is not None and x !="":
                cleaned_item = drop_nones(x)
                if cleaned_item:
                    list_objects.append(drop_nones(x))
        
        return list_objects
    
    # Dictionary case
    elif isinstance(input_object, dict):
        cleaned_dict = {
            key: drop_nones(value)
            for key, value in input_object.items()
            if value and value is not None and value != ""
        }

        if not cleaned_dict:
            return None

        return cleaned_dict

    else:
        return input_object

def save_data(input_data:pl.DataFrame, 
              path_save:str, save_format:str) -> None:
    
    if save_format.lower() in ['pkl', 'pickle']:
        with open(f'{path_save}.pkl', 'wb') as handle:
            pickle.dump(input_data, handle, protocol=pickle.HIGHEST_PROTOCOL)

    elif save_format.lower() == 'csv':
        try:
            input_data.write_csv(f'{path_save}.csv')
        except:
            # Try converting to pandas and saving in case data type is not supported by polars
            input_data = input_data.to_pandas()
            input_data.to_csv(f"{path_save}.csv")

    elif save_format.lower() == 'json':
        json_data = loads(input_data.write_json())
        json_data = drop_nones(drop_nones(drop_nones(json_data)))

        with open(f'{path_save}.json', 'w') as f:
            # json_str = orjson.dumps(json_data, option=orjson.OPT_INDENT_2)
            # f.write(json_str)
            dump(json_data, f, indent=4, default=str)

    elif save_format.lower() in ['geojson', 'geo']:
        # TODO: Create
        file_extension = '.geojson'