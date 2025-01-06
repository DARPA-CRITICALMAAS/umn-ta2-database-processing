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
    if 'crs' not in list(gpd_data.columns):
        gpd_data['crs'] = str(gpd_data.crs)
    gpd_data['location'] = gpd_data['geometry'].apply(wkt.dumps)

    gpd_data = gpd_data.drop('geometry', axis=1)
    pl_data = pl.from_pandas(gpd_data)

    return pl_data

def non2geo(pl_data: pl.DataFrame,
            str_lat_col: str=None, str_long_col: str=None,
            str_geo_col: str=None,
            crs_val: str='EPSG:4326') -> gpd.GeoDataFrame:
    """
    # TODO: fill in information

    Argument
    : pl_data:
    : str_geo_col:
    : crs_val:

    Return
    """

    if str_lat_col:
        df_data = pl_data.with_columns(pl.col([str_lat_col, str_long_col]).cast(pl.Float64)).to_pandas()
        gpd_data = gpd.GeoDataFrame(
            df_data,
            geometry=gpd.points_from_xy(df_data[str_long_col], df_data[str_lat_col]),
            crs=crs_val
        ).drop([str_lat_col, str_long_col], axis=1)

    if str_geo_col:
        df_data = pl_data.to_pandas()
        df_data[str_geo_col] = df_data[str_geo_col].apply(lambda x: wkt.loads(x))

        gpd_data = gpd.GeoDataFrame(
            df_data,
            geometry=str_geo_col,
            crs=crs_val)
        
        gpd_data = gpd_data.drop(str_geo_col, axis=1)

    return gpd_data

def non2dict(pl_data: pl.DataFrame,
             key_col:str=None,
             val_col:str=None) -> Dict[str, str]:
    """
    Converts polars dataframe to dictionary. Each row is a key, val

    # TODO: fill in information
    Argument
    : pl_data:
    : key_col:

    Return
    """
    if not key_col and not val_col:
        raise ValueError("Need to state either key column or value column")

    if key_col:
        dict_data = pl_data.rows_by_key(key=key_col)
        dict_data = {k: v[0][0] for k, v in dict_data.items()}

    if val_col:
        key_cols = list(pl_data.columns)
        key_cols.remove(val_col)

        dict_data = {}

        for k in key_cols:
            pl_tmp = pl_data.select(pl.col([val_col, k])).drop_nulls(subset=[k])
            tmp_dict_data = pl_tmp.rows_by_key(key=k)
            tmp_dict_data = {k: v[0][0] for k, v in tmp_dict_data.items()}

            dict_data.update(tmp_dict_data)  

    return dict_data