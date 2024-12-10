from typing import Dict
import polars as pl
import pyspark

from ._entity import entity2id

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

    # Assumption all data inputted into ProcMine is database
    # source_id = f"database::{source}"

    # TODO: not select, for those that do not exist in label map info needs to be appended
    # pl_data = pl_data.select(
    #     pl.col(org_cols)
    # ).rename(dict_label_map)

    return pl_data

def data2schema(pl_data: pl.DataFrame,
                pl_entities: pl.DataFrame) -> pl.DataFrame:
    """
    TODO: fill up information
    Maps value to minmod format

    Argument
    : pl_data: 
    : list_attribute: 

    Return
    """
    # Add defaults


    # Sanity check
    pl_data = pl_data.select(
        pl.col(['name', 'aliases', 'deposit_type_candidate', 'location_info', 'mineral_inventory', 'modified_at', 'record_id', 'source_id', 'reference'])
    )
    
    return pl_data