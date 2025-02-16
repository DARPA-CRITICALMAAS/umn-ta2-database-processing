from typing import Dict
import polars as pl

# from ._entity import entity_mapper

"""
Structure:
GeologyInfo:
    - alteration
    - concentration_process
    - ore_control
    - host_rock
    - associated_rock
    - structure
    - tectonic
"""

def sch_geology_info(pl_data: pl.DataFrame):
    """
    Argument
    : pl_data: 

    Return

    """
    list_geology_info = list({'alteration', 'concentration_process', 'ore_control', 'host_rock_type', 'host_rock_unit', 'associated_rock_type', 'associated_rock_unit', 'structure', 'tectonic'} & set(pl_data.columns))

    pl_geoinfo = pl_data.select(
        pl.col('record_id'),
        pl.col(list_geology_info)
    ).unique()

    # Struct host rock and associated rock
    pl_geoinfo = sch_type_value(pl_data=pl_geoinfo,
                                col_type='host_rock_type', col_unit='host_rock_unit', col_alias='host_rock')
    pl_geoinfo = sch_type_value(pl_data=pl_geoinfo,
                                col_type='associated_rock_type', col_unit='associated_rock_unit', col_alias='associated_rock')
    
    list_geology_info = list({'alteration', 'concentration_process', 'ore_control', 'host_rock', 'associated_rock', 'structure', 'tectonic'} & set(pl_geoinfo.columns))
    pl_geoinfo = pl_geoinfo.select(
        pl.col('record_id'),
        geology_info = pl.struct(pl.col(list_geology_info))
    ).group_by('record_id').agg([pl.all()]).with_columns(
        pl.exclude('record_id').list.drop_nulls().list.first()
    )

    return pl_geoinfo

def sch_type_value(pl_data: pl.DataFrame,
                   col_type: str,
                   col_unit: str,
                   col_alias: str,):
    """
    TODO: fill information

    Arguments
    : pl_data
    : col_value
    : col_unit
    : col_alias
    """
    
    if col_type in list(pl_data.columns):
        pl_data = pl_data.rename({col_type: 'type'})
    if col_unit in list(pl_data.columns):
        pl_data = pl_data.rename({col_unit: 'unit'})

    list_val_unit = list({'type', 'unit'} & set(list(pl_data.columns)))
    if len(list_val_unit) != 0:
        pl_data = pl_data.with_columns(
            pl.struct(pl.col(list_val_unit)).alias(col_alias)
        ).drop(list_val_unit)

    return pl_data