from typing import Dict, List
import polars as pl
import pickle # Delete later
from datetime import datetime, timezone
import decimal

# from ._entity import *
from ._crs import *
from ._datatype import *
from ._schema import *

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
    list_existing_cols = list(set_attrs_cols & (set_actual_cols | {'category'}))    # TODO: check if category is being included
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

# def data2schema(pl_input: pl.DataFrame,
#                 dict_all_entities: Dict[str, Dict[str, str]],) -> pl.DataFrame:
def new_data2schema(pl_input: pl.DataFrame,
                    dict_all_entities: Dict[str, Dict[str, str]],) -> pl.DataFrame:
    """
    Maps value to minmod format

    Argument
    : pl_input:
    : dict_all_entities: 

    Return
    """
    # Define default entity (i.e., null entity)
    default_entity = {
        "confidence": 0.00,
        "normalized_uri": None,
        "observed_name": None,
        "source": None,
    }

    # list to store all the dataframe output from each schema
    list_pl_outputs = []

    # Create deposit type candidate schema
    pl_deptype = sch_dep_type_cand(pl_data=pl_input,
                                   dict_all_entities=dict_all_entities, default_entity=default_entity)
    if pl_deptype.is_empty(): pass
    else: list_pl_outputs.append(pl_deptype)
    print('deptype', pl_deptype.shape[0])

    # # Create location information schema
    pl_locinfo = sch_location_info(pl_data=pl_input,
                                   dict_all_entities=dict_all_entities, default_entity=default_entity)
    if pl_locinfo.is_empty(): pass
    else: list_pl_outputs.append(pl_locinfo)
    print('locinfo', pl_locinfo.shape[0])

    # Create geology info schema
    pl_geoinfo = sch_geology_info(pl_data=pl_input)
    if pl_geoinfo.is_empty(): pass
    else: list_pl_outputs.append(pl_geoinfo)
    print('geoinfo', pl_geoinfo.shape[0])

    print(pl_input.shape[0])

    # Create mineral inventory schema
    pl_mineralinventory = sch_mineral_inventory(pl_data=pl_input,
                                                dict_all_entities=dict_all_entities, default_entity=default_entity)
    if pl_mineralinventory.is_empty(): pass
    else: list_pl_outputs.append(pl_mineralinventory)

    # Wrap to mineral site schema
    pl_mineralsite = sch_mineral_site(pl_data=pl_input)
    list_pl_outputs.append(pl_mineralsite)
    print('mineralsite', pl_mineralsite.shape[0])

    pl_output = pl.concat(
        list_pl_outputs,
        how='align'
    )

    return pl_output