from typing import Dict
import polars as pl

def label2label(pl_data: pl.DataFrame,
                dict_label_map: Dict[str, str]) -> pl.DataFrame:
    """
    Maps label to that of the schema

    Arguments
    : pl_data:
    : dict_label_map

    Return
    """

    org_cols = list(dict_label_map.keys())

    pl_data = pl_data.select(
        pl.col(org_cols)
    ).rename(dict_label_map)

    return pl_data

def value2minmod(pl_data: pl.DataFrame,
                 list_attribute: list) -> pl.DataFrame:
    """
    TODO: fill up information
    Maps value to minmod format

    Argument
    : pl_data: 
    : list_attribute: 

    Return
    """
    
    
    return pl_data