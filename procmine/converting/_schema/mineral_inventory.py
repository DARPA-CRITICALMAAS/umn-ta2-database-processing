from typing import List, Dict
from datetime import datetime
import polars as pl

from ._entity import entity_mapper

"""
Structure:
MineralInventory:
    - category: List[Category]
    - commodity: CommodityCandidate
    - grade: 
        - value: float
        - unit: Unit
    - ore: 
        - value: float
        - unit: Unit
    - date: datetime
    - reference: Reference

"""

def sch_mineral_inventory(pl_data: pl.DataFrame,
                          dict_all_entities: Dict[str, Dict[str, str]],
                          default_entity: dict):
    """
    TODO: fill in information

    Arguments
    : pl_data: 
    : dict_all_entities:
    : default_entity: 
    
    """
    list_mineral_inventory = list({'commodity', 'grade_value', 'grade_unit', 'tonnage_value', 'tonnage_unit', 'tonnage_year', 'grade_year', 'reference', 'category'} & set(list(pl_data.columns)))
    list_unique = list({'commodity', 'grade_value', 'grade_unit', 'tonnage_value', 'tonnage_unit', 'tonnage_year', 'grade_year'} & set(list_mineral_inventory))
    pl_min_inven = pl_data.select(
        pl.col('record_id'),
        pl.col(list_mineral_inventory)
    ).explode('commodity').unique(subset=list_unique)

    year_col = list({'tonnage_year', 'grade_year'} & set(list(pl_min_inven.columns)))[0]
    pl_min_inven = pl_min_inven.rename({year_col:'resource_year'})

    # Map commodity, grade unit, ore unit
    list_map = list(set(list_mineral_inventory) & {'commodity', 'grade_unit', 'tonnage_unit', 'category'})
    print(list_map)
    pl_mapped_min_inven = entity_mapper(pl_data=pl_min_inven, list_map=list_map,
                                        dict_all_entities=dict_all_entities, default_entity=default_entity)

    pl_min_inven = pl_min_inven.drop(list_map)
    pl_min_inven = pl.concat(
        [pl_min_inven, pl_mapped_min_inven],
        how='align'
    ).unique()

    # Grade
    pl_min_inven = sch_unit_value(pl_data=pl_min_inven,
                                  col_value='grade_value', col_unit='grade_unit', col_alias='grade')
    
    # Need to add the tonnage value
    try:
        pl_min_inven = pl_min_inven.with_columns(
            pl.col('tonnage_value').str.split('; ').list.eval(pl.element().filter(pl.element() != ""))
        ).with_columns(
            pl.struct(pl.col('tonnage_value')).map_elements(lambda x: [float(i) for i in x['tonnage_value']] if x['tonnage_value'] else [0]).list.sum()
        )
    except: pass

    pl_min_inven = pl_min_inven.with_columns(
        pl.col('tonnage_value').replace(0, None)
    )

    # Ore
    pl_min_inven = sch_unit_value(pl_data=pl_min_inven,
                                  col_value='tonnage_value', col_unit='tonnage_unit', col_alias='ore')

    # Date
    try: 
        pl_min_inven = pl_min_inven.with_columns(
            date = pl.struct(pl.col('resource_year')).map_elements(lambda x: datetime(year=x['resource_year']))
        ).drop('resource_year')
    except: pass

    list_mineral_inventory = list({'commodity', 'grade', 'ore', 'resource_year', 'reference', 'category'} & set(list(pl_min_inven.columns)))
    pl_min_inven = pl_min_inven.select(
        pl.col('record_id'),
        mineral_inventory = pl.struct(pl.col(list_mineral_inventory))
    ).group_by('record_id').agg([pl.all()])

    return pl_min_inven

def sch_unit_value(pl_data: pl.DataFrame,
                   col_value: str,
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
    
    if col_value in list(pl_data.columns):
        pl_data = pl_data.rename({col_value: 'value'}).with_columns(pl.col('value').cast(pl.Float64, strict=False))

    pl_data = pl_data.with_columns(
        pl.when(
            pl.col('value').is_null()
        ).then(None).otherwise(pl.col(col_unit)).alias('unit')
    ).drop(col_unit)

    list_val_unit = list({'value', 'unit'} & set(list(pl_data.columns)))
    if len(list_val_unit) != 0:
        pl_data = pl_data.with_columns(
            pl.struct(pl.col(list_val_unit)).alias(col_alias)
        ).drop(list_val_unit)

    return pl_data