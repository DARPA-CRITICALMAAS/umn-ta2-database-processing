from typing import Dict, List
import polars as pl
import pyspark
from datetime import datetime, timezone

from ._entity import *
from ._crs import *
from ._datatype import *

def label2label(pl_data: pl.DataFrame,
                pl_label_map: pl.DataFrame):
    """
    TODO: fill up information
    Maps label to that of the schema

    Arguments
    : pl_data:
    : dict_label_map

    Return
    """
    # Identify attributes that exist in database and mapping file
    set_actual_cols = set(list(pl_data.columns))
    set_attrs_cols = set(pl_label_map['corresponding_attribute_label'].to_list())
    list_existing_cols = list(set_attrs_cols.intersection(set_actual_cols))
    del set_actual_cols, set_attrs_cols

    # Create dictionary for literals (i.e., those that do not exist in the data)
    pl_literals = pl_label_map.filter(
        ~pl.col('corresponding_attribute_label').is_in(list_existing_cols)
    )
    dict_literals = non2dict(pl_literals, 'attribute_label')
    del pl_literals

    # Create dictionary for those that exist in database
    pl_existing = pl_label_map.filter(
        pl.col('corresponding_attribute_label').is_in(list_existing_cols)
    )
    dict_existing = non2dict(pl_existing, 'corresponding_attribute_label')
    del pl_existing

    # Fill null with blank space to prevent any string concatenation error
    pl_data = pl_data.select(
        pl.col(list_existing_cols).fill_null("").str.to_titlecase()
    )

    for attribute, value in dict_existing.items():
        try:
            pl_data = pl_data.rename({attribute: value})
        except pl.DuplicateError:
            pl_data = pl_data.with_columns(
                pl.concat_str(
                    [
                        pl.col(value),
                        pl.col(attribute)
                    ],
                    separator="; ",
                )
            ).drop(attribute)

    return pl_data, dict_literals

def data2schema(pl_data: pl.DataFrame,
                dict_all_entities: Dict[str, Dict[str, str]],) -> pl.DataFrame:
    """
    TODO: fill up information
    Maps value to minmod format

    Argument
    : pl_data: 
    : list_attribute: 

    Return
    """

    set_actual_cols = set(list(pl_data.columns))

    # PLDF general
    list_general = list({'record_id', 'source_id', 'name', 'aliases', 'modified_at', 'created_by', 'site_type', 'reference'} & set_actual_cols)
    pl_general = pl_data.select(pl.col(list_general))
    # TODO: Split aliases to list

    # PLDF Deposit Type
    list_deposit_type = list({'record_id', 'deposit_type'} & set_actual_cols)
    pl_deposit_type = pl_data.select(pl.col(list_deposit_type))

    # TODO: Pop deposit type

    # pl_partial = pl_deposit_type.filter(
    #     pl.col('deposit_type').str.replace(r"\s", "") != ""
    # ).with_columns(
    #     pl.col('deposit_type').map_elements(lambda x: entity2id(x, dict_sub_entities=dict_all_entities['deposit_type']))
    # )

    # PLDF Location Info
    # list_location_info = list({'record_id', 'country', 'crs', 'location', 'state_or_province'} & set_actual_cols)
    list_location_info = list({'record_id', 'country', 'state_or_province'} & set_actual_cols)
    pl_location_info = pl_data.select(pl.col(list_location_info))
    dict_location_info = {}

    print(list_location_info)

    list_location_info.remove('record_id')
    for li in list_location_info:
        unique_items = pl_location_info.unique(subset=[li])[li].to_list()

        tmp_mapping_dict = {}

        for i in unique_items:
            tmp_mapping_dict[i] = entity2id(i, dict_sub_entities=dict_all_entities[li])

        pl_location_info = pl_location_info.with_columns(
            pl.col(li).replace(tmp_mapping_dict, return_dtype=pl.Struct)
        )

        print(pl_location_info)

        break
    # TODO: Check popping for country and state

    # PLDF Mineral Inventory
    list_mineral_inventory = list({'record_id', 'commodity', 'grade', 'grade_unit'} & set_actual_cols)
    pl_mineral_inventory = pl_data.select(pl.col(list_mineral_inventory))

    # # TODO: Load pl_data to spark with each processable component individually
    # data_rdd = 0
    # list_rdds = []
    # # (Key, value) = ((source_id, record_id), ({comp_name:comps, source:source}))

    # # TODO: join the four rdds
    # location_rdd = location_rdd.map(lambda x: (('source_id', 'record_id'), {'country': x[0], 'state_or_province': x[1], 'crs': x[2], 'location': x[3]}))
    # dep_type_rdd = dep_type_rdd.reduceByKey(lambda a, b: a + b)
    # mineral_inventory_rdd = mineral_inventory_rdd.reduceByKey(lambda a, b: a + b)

    # # Create objects
    # pl_data = pl_data.with_columns(
    #     location_info = pl.struct(pl.col(['country', 'crs', 'location', 'state_or_province'])),
    #     mineral_inventory = pl.struct(pl.col(['commodity', 'grade', 'reference']))
    # )
    # # Sanity check
    # pl_data = pl_data.select(
    #     pl.col(['name', 'aliases', 'deposit_type_candidate', 'location_info', 'mineral_inventory', 'modified_at', 'created_by', 'record_id', 'source_id', 'site_type', 'reference'])
    # )

    return pl_data

