from typing import Dict
import polars as pl

from ._entity import entity_mapper

"""
Structure:
DepositTypeCandidate: List[DepositTypeCandidate]
"""

def sch_dep_type_cand(pl_data: pl.DataFrame,
                      dict_all_entities: Dict[str, Dict[str, str]],
                      default_entity: dict) -> pl.DataFrame:
    """
    TODO: add information

    Arguments
    : pl_data: 
    : dict_all_entities: 
    : default_entity: 
    """
    list_map = list({'deposit_type'} & set(list(pl_data.columns)))

    if len(list_map) < 1:
        return pl.DataFrame()
    
    pl_deptype = pl_data.select(
        pl.col('record_id'),
        pl.col(list_map)
    ).unique()
    
    pl_deptype = entity_mapper(pl_data=pl_deptype, list_map=list_map,
                               dict_all_entities=dict_all_entities, default_entity=default_entity)
                               
    pl_deptype = pl_deptype.group_by('record_id').agg([pl.all()])

    try: pl_deptype = pl_deptype.rename({'deposit_type': 'deposit_type_candidate'})
    except: pass

    return pl_deptype
    