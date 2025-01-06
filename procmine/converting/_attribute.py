from typing import Dict, List
import polars as pl
import pyspark
from datetime import datetime, timezone

from ._entity import *
from ._crs import *
from ._datatype import *

def add_attribute(pl_data: pl.DataFrame, 
                  attribute:str = None, value = None,
                  dict_attributes:dict = None) -> pl.DataFrame:
    
    if attribute and value:        
        pl_data = pl_data.with_columns(
            pl.lit(value).alias(attribute)
        )

    if dict_attributes:
        for attribute, value in dict_attributes.items():
            pl_data = pl_data.with_columns(
                tmp = pl.lit(value)
            )
            try:
                pl_data = pl_data.rename({"tmp": attribute})
            except pl.DuplicateError:
                pl_data = pl_data.with_columns(
                    pl.concat_str(
                        [
                            pl.col(attribute),
                            pl.col("tmp")
                        ],
                        separator="; ",
                    )
                ).drop("tmp")

    return pl_data

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

def data2schema(pl_input: pl.DataFrame,
                dict_all_entities: Dict[str, Dict[str, str]],) -> pl.DataFrame:
    """
    TODO: fill up information
    Maps value to minmod format

    Argument
    : pl_input: 
    : list_attribute: 

    Return
    """
    default_entity = {
        "confidence": 0.00,
        "normalized_uri": None,
        "observed_name": None,
        "source": None,
    }

    set_actual_cols = set(list(pl_input.columns))

    # PLDF general
    list_general = list({'record_id', 'source_id', 'name', 'aliases', 'modified_at', 'created_by', 'site_type', 'reference', 'location'} & set_actual_cols)
    pl_output = pl_input.select(pl.col(list_general))

    # PLDF need_to_map
    list_map = list({'deposit_type', 'country', 'state_or_province', 'commodity', 'epsg', 'grade_unit'} & set_actual_cols)
    pl_map = pl_input.select(
        pl.col('record_id'),
        pl.col(list_map)
    )

    for mi in list_map:
        pl_tmp = pl_map.select(pl.col(['record_id', mi]))
        bool_type_list = False
        if pl_tmp[mi].dtype == pl.List:
            pl_tmp = pl_tmp.with_columns(pl.col(mi).list.unique()).explode(mi)
            bool_type_list = True

        unique_items = pl_tmp.unique(subset=[mi])[mi].to_list()
        tmp_mapping_dict = {}

        for i in unique_items:
            tmp_mapping_dict[i] = entity2id(i, dict_sub_entities=dict_all_entities[mi])
            
        pl_tmp = pl_tmp.rename({mi: 'tmp'}).with_columns(
            pl.col('tmp').replace(tmp_mapping_dict, default=default_entity).alias(mi)
        ).drop('tmp')

        if bool_type_list:
            pl_tmp = pl_tmp.group_by('record_id').agg([pl.all()])

        pl_output.join(pl_tmp, on='record_id')


    print(pl_output)
        





    # pl_data = pl_data.select(pl.col('record_id'), pl.col(list_explodable))
    # for e in list_explodable:
    #     pl_data = pl_data.explode(e)
    # print(pl_data)

    # PLDF Deposit Type & Location Information
    # list_dep_locinfo = list({'record_id', 'deposit_type', 'country'} & set_actual_cols)
    # pl_dep_locinfo = pl_data.select(pl.col(list_dep_locinfo))

    # list_dep_locinfo.remove('record_id')
    # if len(list_dep_locinfo) >= 1:
    #     for li in list_dep_locinfo:
    #         unique_items = pl_dep_locinfo.unique(subset=[li])[li].to_list()
    #         tmp_mapping_dict = {}

    #         for i in unique_items:
    #             tmp_mapping_dict[i] = entity2id(i, dict_sub_entities=dict_all_entities[li])

    #             pl_dep_locinfo = pl_dep_locinfo.rename({li: 'tmp'}).with_columns(
    #                 pl.col('tmp').replace(tmp_mapping_dict, default=default_entity).alias(li)
    #             )

    # print(pl_dep_locinfo)


    # PLDF Location Info (geo, text)
    # TODO: Convert crs to epsg code
    # list_location_geo = list({'record_id', 'location', 'crs'} & set_actual_cols)
    # pl_location_geo = pl_data.select(pl.col(list_location_geo))

    # # crs2epsg(crs_value:str)

    # # TODO: crs range check

    # # list_location_info = list({'record_id', 'country', 'crs', 'location', 'state_or_province'} & set_actual_cols)

    # # TODO: add in country as hint for state_or_province
    # for li in list_location_info:
    #     unique_items = pl_location_info.unique(subset=[li])[li].to_list()

    #     tmp_mapping_dict = {}

    #     for i in unique_items:
    #         tmp_mapping_dict[i] = entity2id(i, dict_sub_entities=dict_all_entities[li])

    #     print(tmp_mapping_dict)

    #     pl_location_info = pl_location_info.rename({li: 'tmp'}).with_columns(
    #         pl.col('tmp').replace(tmp_mapping_dict, default=default_entity).alias(li)
    #     )

    #     print(pl_location_info)

    #     break
    # # TODO: Check popping for country and state

    # # PLDF Mineral Inventory
    # list_mineral_inventory = list({'record_id', 'commodity', 'grade', 'grade_unit'} & set_actual_cols)
    # pl_mineral_inventory = pl_data.select(pl.col(list_mineral_inventory))

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

