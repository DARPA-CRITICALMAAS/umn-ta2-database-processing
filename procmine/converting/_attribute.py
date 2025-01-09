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

        pl_output = pl_output.join(pl_tmp, on='record_id')

    # Replace column name epsg to crs
    try:
        pl_output = pl_output.rename({'epsg': 'crs'})
    except:
        # Means there was no epsg or crs information
        pass

    # Rename tonnage, grade, and grade unit to that in schema
    if 'tonnage' in list(pl_output.columns):
        pl_output = pl_output.rename({'tonnage': 'contained_metal'})
    if 'grade' in list(pl_output.columns):
        pl_output = pl_output.rename({'grade': 'value'})
    if 'grade_unit' in list(pl_output.columns):
        pl_output = pl_output.rename({'grade_unit': 'unit'})
    
    list_grade = list({'value', 'unit'} & set(list(pl_output.columns)))
    if len(list_grade) != 0:
        pl_output = pl_output.with_columns(
            grade = pl.struct(pl.col(list_grade))
        ).drop(list_grade)
    
    # Convert to schema
    set_actual_cols = set(list(pl_output.columns))
    list_others = list({'source_id', 'record_id', 'name', 'aliases' , 'modified_at', 'created_by', 'site_type', 'deposit_type_candidate'} & set_actual_cols)
    list_loc_info = list({'location', 'country', 'state_or_province', 'crs'} & set_actual_cols)
    list_min_inven = list({'commodity', 'grade', 'contained_metal', 'reference'} & set_actual_cols)

    pl_output = pl_output.select(
        pl.col(list_others),
        location_info = pl.struct(pl.col(list_loc_info)),
        mineral_inventory = pl.struct(pl.col(list_min_inven))
    )

    return pl_output

