from typing import Dict
import polars as pl
from shapely import wkt

from ._entity import entity_mapper

"""
Structure:
LocationInfo:
    - country: List[CountryCandidate]
    - crs: List[crsCandidate]
    - location: geo.wkt.literal
    - state_or_province: List[StateCandidate]
"""

def sch_location_info(pl_data: pl.DataFrame,
                      dict_all_entities: Dict[str, Dict[str, str]],
                      default_entity: dict):
    
    # Replace column name crs to epsg
    try: pl_loc_data = pl_loc_data.rename({'crs': 'epsg'})
    except: pass

    list_location_info = list({'location', 'country', 'state_or_province', 'epsg'} & set(list(pl_data.columns)))

    pl_loc_data = pl_data.select(
        pl.col('record_id'),
        pl.col(list_location_info), 
    ).unique(subset=['record_id'], keep='first')

    list_map = list(set(list_location_info) & {'country', 'state_or_province', 'epsg'})
    pl_mapped_loc = entity_mapper(pl_data=pl_loc_data, list_map=list_map,
                                  dict_all_entities=dict_all_entities, default_entity=default_entity)
    
    pl_loc_data = pl_loc_data.drop(list_map)
    pl_loc_data = pl.concat(
        [pl_loc_data, pl_mapped_loc],
        how='align'
    )

    pl_loc_data = pl_loc_data.group_by('record_id').agg([pl.all()])
    try:
        pl_loc_data = pl_loc_data.with_columns(
            pl.col('location').list.first()
        )
    except: pass

    # Replace column name epsg to crs
    try: pl_loc_data = pl_loc_data.rename({'epsg': 'crs'})
    except: pass

    list_location_info = list({'location', 'country', 'state_or_province', 'crs'} & set(list(pl_loc_data.columns)))
    pl_loc_data = pl_loc_data.select(
        pl.col('record_id'),
        location_info = pl.struct(pl.col(list_location_info))
    )
    
    return pl_loc_data

def check_location_parsability():
    pass