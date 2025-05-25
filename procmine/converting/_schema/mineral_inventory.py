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
    list_unique = list({'record_id', 'commodity', 'grade_value', 'grade_unit', 'tonnage_value', 'tonnage_unit', 'tonnage_year', 'grade_year'} & set(list(pl_data.columns)))
    pl_min_inven = pl_data.select(
        pl.col('record_id'),
        pl.col(list_mineral_inventory)
    ).explode('commodity').filter(pl.col('commodity') != '').with_columns(
        tmp = pl.struct(list_unique)
    ).unique('tmp').drop('tmp')

    try:
        year_col = list({'tonnage_year', 'grade_year'} & set(list(pl_min_inven.columns)))[0]
        pl_min_inven = pl_min_inven.rename({year_col:'resource_year'})
    except: pass

    # Map commodity, grade unit, ore unit
    list_map = list(set(list_mineral_inventory) & {'commodity', 'grade_unit', 'tonnage_unit', 'category'})
        
    pl_mapped_min_inven = entity_mapper(pl_data=pl_min_inven, list_map=list_map,
                                        dict_all_entities=dict_all_entities, default_entity=default_entity)

    pl_min_inven = pl_min_inven.drop(list_map)
    pl_min_inven = pl.concat(
        [pl_min_inven, pl_mapped_min_inven],
        how='align'
    )

    # Grade
    try:
        # grade_is_safe = safe_grade(pl_min_inven)
        pl_min_inven = sch_unit_value(pl_data=pl_min_inven,
                                      col_value='grade_value', col_unit='grade_unit', col_alias='grade')
    except: pass
    
    # Need to add the tonnage value
    try:
        pl_min_inven = pl_min_inven.with_columns(
            pl.col('tonnage_value').str.split('; ').list.eval(pl.element().filter(pl.element() != ""))
        ).with_columns(
            pl.struct(pl.col('tonnage_value')).map_elements(lambda x: [float(i) for i in x['tonnage_value']] if x['tonnage_value'] else [0]).list.sum()
        ).with_columns(
            pl.col('tonnage_value').replace(0, None)
        )
    except: pass

    # Ore
    try:
        tonnage_is_safe = safe_tonnage(pl_min_inven)
        # print("Safe Tonnage, ", tonnage_is_safe)
        pl_min_inven = sch_unit_value(pl_data=pl_min_inven,
                                    col_value='tonnage_value', col_unit='tonnage_unit', col_alias='ore')
    except: pass

    # Date
    try: 
        pl_min_inven = pl_min_inven.with_columns(
            date = pl.struct(pl.col('resource_year')).map_elements(lambda x: datetime(year=x['resource_year']))
        ).drop('resource_year')
    except: pass

    pl_min_inven = pl_min_inven.unique()

    list_mineral_inventory = list({'commodity', 'grade', 'ore', 'resource_year', 'reference', 'category'} & set(list(pl_min_inven.columns)))

    if 'category' in list_mineral_inventory:
        pl_min_inven = pl_min_inven.with_columns(
            pl.col('category').list.unique()
        )

    pl_min_inven = pl_min_inven.filter(
        pl.col("commodity").struct["confidence"] > 0.01
    ).select(
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

def safe_grade(pl_data: pl.DataFrame) -> bool:
    """
    Checks whether those with grade unit wt-pct does not go over 100

    Saves incorrect to csv file
    """
    pl_data = pl_data.filter(
        pl.col('grade_value') != ""
    ).with_columns(
        pl.col('grade_value').cast(pl.Float64)
    )

    pl_tmp = pl_data.filter(
        (pl.col('grade_value') > 100),
        (pl.col('grade_unit').struct["normalized_uri"] == "https://minmod.isi.edu/resource/Q201")
    )

    if pl_tmp.shape[0] != 0:
        print('here')
        pl_tmp = pl_tmp.select(
            mrds_link = pl.lit('https://mrdata.usgs.gov/mrds/show-mrds.php?dep_id=') + pl.col('record_id'),
            error_point = pl.lit('GRADE'),
            commodity = pl.col('commodity').struct["observed_name"],
            value = pl.col('grade_value'),
            unit = pl.col('grade_unit').struct["observed_name"],
            year = pl.col('grade_year')
        )

        print('done')
        
        pl_tmp.write_csv('/users/2/pyo00005/HOME/CriticalMAAS/invalid_grade.csv')
        return False
    
    pl_tmp = pl_data.filter(
        ((pl.col('grade_value')/10000) > 100),
        (pl.col('grade_unit').struct["normalized_uri"] == "https://minmod.isi.edu/resource/Q220")
    )

    if pl_tmp.shape[0] != 0:
        print(pl_tmp.select(
            pl.col('grade_value'),
            pl.col('grade_unit').struct["normalized_uri"]))
        return False

    return True

def safe_tonnage(pl_data: pl.DataFrame) -> bool:
    pl_data = pl_data.filter(
        (pl.col('tonnage_value') > 1000000000000),
        (pl.col('tonnage_unit').struct["normalized_uri"] == "https://minmod.isi.edu/resource/Q200")
    )

    if pl_data.shape[0] == 0:
        return True
    else:
        pl_data = pl_data.select(
            mrds_link = pl.lit('https://mrdata.usgs.gov/mrds/show-mrds.php?dep_id=') + pl.col('record_id'),
            error_point = pl.lit('TONNAGE'),
            commodity = pl.col('commodity').struct["observed_name"],
            value = pl.col('tonnage_value'),
            unit = pl.col('tonnage_unit').struct["observed_name"],
            year = pl.col('tonnage_year')
        )
        
        pl_data.write_csv('/users/2/pyo00005/HOME/CriticalMAAS/invalid_tonnage.csv')
        return False