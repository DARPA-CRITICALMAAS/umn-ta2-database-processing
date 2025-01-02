from typing import Dict
import polars as pl
import pyspark
from datetime import datetime, timezone

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
                pl_entities: pl.DataFrame,
                dict_defaults: Dict[str, str],) -> pl.DataFrame:
    """
    TODO: fill up information
    Maps value to minmod format

    Argument
    : pl_data: 
    : list_attribute: 

    Return
    """
    # Append default attributes: source, created_by, modified_at
    pl_data = data_default(pl_data=pl_data, dict_defaults=dict_defaults)

    # Initialize Spark object
    sc_conf = pyspark.SparkContext.getOrCreate()
    sc = pyspark.SparkContext(conf=sc_conf)
    sc.setLogLevel("OFF")

    # TODO: Load pl_data to spark with each processable component individually
    data_rdd = 0
    list_rdds = []
    # (Key, value) = ((source_id, record_id), ({comp_name:comps, source:source}))

    # TODO: join the four rdds
    location_rdd = location_rdd.map(lambda x: (('source_id', 'record_id'), {'country': x[0], 'state_or_province': x[1], 'crs': x[2], 'location': x[3]}))
    dep_type_rdd = dep_type_rdd.reduceByKey(lambda a, b: a + b)
    mineral_inventory_rdd = mineral_inventory_rdd.reduceByKey(lambda a, b: a + b)


    # Create objects
    pl_data = pl_data.with_columns(
        location_info = pl.struct(pl.col(['country', 'crs', 'location', 'state_or_province'])),
        mineral_inventory = pl.struct(pl.col(['commodity', 'grade', 'reference']))
    )
    # Sanity check
    pl_data = pl_data.select(
        pl.col(['name', 'aliases', 'deposit_type_candidate', 'location_info', 'mineral_inventory', 'modified_at', 'created_by', 'record_id', 'source_id', 'site_type', 'reference'])
    )

    return pl_data

def data_default(pl_data: pl.DataFrame,
                 dict_defaults: Dict[str, str]) -> pl.DataFrame:
    """
    Append default data to dataframe

    Argument
    : pl_data: 
    : dict_defaults:

    Return
    """
    pl_data = pl_data.with_columns(
        modified_at = pl.lit(datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')),
        source = pl.lit(dict_defaults['source']),
        source_id = pl.lit(f"database::{dict_defaults['source']}"),
        created_by = pl.lit(dict_defaults['created_by']),
        reference = pl.struct(pl.struct(pl.lit(dict_defaults['uri']).alias('uri')).alias('document')),
    )

    return pl_data