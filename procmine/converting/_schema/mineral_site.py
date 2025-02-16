from typing import List
import polars as pl

from ._entity import entity2id

"""
Structure:
MineralSite:
    - name: str
    - aliases: List[str]
    - modified_at: datetime
    - record_id: str
    - reference: Reference
    - site_rank: str
    - site_type: str
    - source_id: str
    - mineral_form: List[str]
    - discovered_year: int
"""

def sch_mineral_site(pl_data: pl.DataFrame):
    """
    TODO: fill information

    Argument
    : pl_data: 
    
    """
    list_mineral_site = list({'name', 'aliases', 'modified_at', 'record_id', 'source_id', 'reference', 'site_rank', 'site_type', 'source_id', 'mineral_form', 'discovered_year'} & set(list(pl_data.columns)))
    pl_mineralsite = pl_data.select(
        pl.col(list_mineral_site)
    ).unique()

    # Name & Alias
    
    pl_mineralsite = pl_mineralsite.with_columns(
        pl.col('name').list.unique().list.eval(pl.element().filter(pl.element() != "")),
    ).with_columns(
        pl.col('name').list.first(),
        name_additional = pl.col('name').list.slice(1,)
    )

    try:
        # Rename name addition al column to aliases
        pl_mineralsite = pl_mineralsite.rename({"name_additional": 'aliases'})
    except pl.DuplicateError:
        # In case renaming fails (due to existing aliases column) concat the list
        pl_mineralsite = pl_mineralsite.with_columns(
            pl.concat_list('aliases', 'name_additional')
        ).drop('name_additional')

    # Discovered year, mineral form
    pl_mineralsite = pl_mineralsite.with_columns(
        pl.col('discovered_year').cast(pl.Int64, strict=False),
    )

    # Groupby record_id, for non-list, get first item in list
    pl_mineralsite = pl_mineralsite.group_by('record_id').agg([pl.all()]).with_columns(
        pl.exclude(['record_id', 'aliases']).list.first()
    ).with_columns(
        pl.col('mineral_form').str.replace_all(r"\s*[\|,;]\s*", ";").str.split(';').list.eval(pl.element().filter(pl.element() != ""))
    )

    return pl_mineralsite