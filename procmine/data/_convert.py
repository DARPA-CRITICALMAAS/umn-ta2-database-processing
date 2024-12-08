from typing import Dict

import polars as pl
import pandas as pd
import geopandas as gpd
from shapely import wkt

def geo2non(gpd_data: gpd.GeoDataFrame) -> pl.DataFrame:
    """
    Converts geodataframe to polars data frame

    # TODO: fill in the information
    Arguments
    : gpd_data

    Return:
    """
    gpd_data['crs'] = str(gpd_data.crs)
    gpd_data['location'] = gpd_data['geometry'].apply(wkt.dumps)

    gpd_data = gpd_data.drop('geometry', axis=1)
    pl_data = pl.from_pandas(gpd_data)

    return pl_data

def non2geo(pl_data: pl.DataFrame,
            str_geo_col: str='location',
            crs_val: str='EPSG:4326') -> gpd.GeoDataFrame:
    """
    # TODO: fill in information

    Argument
    : pl_data:
    : str_geo_col:
    : crs_val:

    Return
    """
    df_data = pl_data.to_pandas()
    df_data[str_geo_col] = df_data[str_geo_col].apply(lambda x: wkt.loads(x))

    gpd_data = gpd.GeoDataFrame(
        df_data,
        geometry=str_geo_col,
        crs=crs_val)
    
    gpd_data = gpd_data.drop(str_geo_col, axis=1)

    return 0

def non2dict(pl_data: pl.DataFrame,
             key_col:str) -> Dict[str, str]:
    """
    Converts polars dataframe to dictionary. Each row is a key, val

    # TODO: fill in information
    Argument
    : pl_data:
    : key_col:

    Return
    """
    # Check dataframe is only two columns
    if pl_data.columns != 2:
        raise ValueError("To convert to dictionary, input must be two columns")
    
    # TODO: check if output is of desired form
    dict_data = pl_data.rows_by_key(key=[key_col])
    # print(dict_data)

    return dict_data