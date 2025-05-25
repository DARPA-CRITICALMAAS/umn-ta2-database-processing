from typing import Dict, List
import polars as pl
import pandas as pd

import regex as re
from strsimpy import normalized_levenshtein, jaro_winkler, metric_lcs, cosine, overlap_coefficient, sorensen_dice
import statistics

def entity_mapper(pl_data: pl.DataFrame,
                  list_map: List[str],
                  dict_all_entities: Dict[str, Dict[str, str]],
                  default_entity: dict) -> pl.DataFrame:
    """
    TODO: fill in information

    Argument
    : pl_data: 
    : list_map: 
    : dict_all_entities: 
    : default_entity: 

    Return

    """
    list_processed_data = []

    for mi in list_map:
        pl_data = pl_data.rename({mi: 'tmp'})
        pl_subset = pl_data.select(pl.col('tmp')).unique()

        bool_mi_list = (pl_subset['tmp'].dtype == pl.List)

        if bool_mi_list:
            pl_tmp = pl_subset.with_columns(pl.col('tmp').list.unique()).explode('tmp')
            pl_data = pl_data.with_columns(
                pl.col('tmp').list.join(";")
            )
        else:
            pl_tmp = pl_subset

        unique_items = pl_tmp.unique(subset=['tmp'])['tmp'].to_list()
        tmp_mapping_dict = {}

        for i in unique_items:
            try:
                tmp_mapping_dict[i] = entity2id(i, dict_sub_entities=dict_all_entities[mi])
            except:
                # Added to deal with different names of the unit (i.e., grade_unit, tonnage_unit)
                tmp_mapping_dict[i] = entity2id(i, dict_sub_entities=dict_all_entities['unit'])

        if bool_mi_list:
            pl_subset = pl_subset.with_columns(
                pl.when(pl.col('tmp').list.len() == 0)
                .then(pl.lit([default_entity]))
                .otherwise(pl.col('tmp').map_elements(lambda x: [tmp_mapping_dict[it] for it in x]))
                .alias(mi),
                pl.col('tmp').list.join(";")
            )
            # pl_subset = pl_subset.with_columns(
            #     pl.col('tmp').map_elements(lambda x: [tmp_mapping_dict[it] for it in x]).alias(mi)
            # )
        else:
            pl_subset = pl_subset.with_columns(
                pl.col('tmp').replace_strict(tmp_mapping_dict, default=default_entity).alias(mi)
            )

        pl_data = pl.concat(
            [pl_data, pl_subset],
            how='align'
        ).drop('tmp')

        # pl_subset = pl_data.select(pl.col(['record_id', mi]))
        # pl_tmp = pl_subset.unique()

        # bool_type_list = False

        # if pl_tmp[mi].dtype == pl.List:
        #     pl_tmp = pl_tmp.with_columns(pl.col(mi).list.unique()).explode(mi)

        #     if pl_tmp[mi].dtype == pl.List:
        #         pl_tmp = pl_tmp.with_columns(pl.col(mi).list.first())

        #     bool_type_list = True

        # unique_items = pl_tmp.unique(subset=[mi])[mi].to_list()
        # tmp_mapping_dict = {}

        # for i in unique_items:
        #     try:
        #         tmp_mapping_dict[i] = entity2id(i, dict_sub_entities=dict_all_entities[mi])
        #     except:
        #         # Added to deal with different names of the unit (i.e., grade_unit, tonnage_unit)
        #         tmp_mapping_dict[i] = entity2id(i, dict_sub_entities=dict_all_entities['unit'])

        # pl_tmp = pl_tmp.rename({mi: 'tmp'})
        # # pl_subset = pl_subset.rename({mi: 'tmp'})
        
        # # if bool_type_list:
        # #     pl_subset = pl_subset.with_columns(
        # #         pl.col('tmp').list.sort()
        # #     )

        # #     pl_partitioned = pl_subset.partition_by('tmp')
        # #     pl_subset = pl.DataFrame()
        # #     # pl_hold = pl.DataFrame()

        # #     list_plps = []
            
        # #     for idx, plp in enumerate(pl_partitioned):
        # #         list_items = plp.item(0, 'tmp') # 0 length or above
        # #         # print(list_items)

        # #         if len(list_items) == 0:
        # #             # Empty list
        # #             list_items = [default_entity]
        # #         else:
        # #             list_items = [tmp_mapping_dict[it] for it in list_items]

        # #         plp = plp.drop('tmp').with_columns(
        # #             pl.lit(list_items).alias(mi)
        # #         )

        # #         if idx == 0:
        # #             pl_subset = plp
        # #         else:
        # #             pl_subset = pl.concat(
        # #                 [pl_subset, plp],
        # #                 how='diagonal_relaxed'
        # #             )

        # #     # pl_subset = pl.concat(
        # #     #     list_plps,
        # #     #     how='diagonal'
        # #     # )
        # # else:
        # pl_tmp = pl_tmp.with_columns(
        #     pl.col('tmp').replace(tmp_mapping_dict, default=default_entity).alias(mi)
        # ).drop('tmp')

        # if bool_type_list:
        #     pl_tmp = pl_tmp.group_by('record_id').agg([pl.all()])
        # #     pl_subset = pl_subset.group_by('index').agg([pl.all()]).with_columns(
        # #         pl.col(mi).list.unique(),
        # #         pl.col('record_id').list.first()
        # #     ).drop('index')
        # #     print(pl_subset.filter(pl.col('record_id') == '10071363'))

        # # print(pl_subset.filter(pl.col('record_id') == '10071363'))

        # list_processed_data.append(pl_tmp)

    return pl_data

def entity2id(entity_name: str,
              dict_sub_entities:  Dict[str, str],) -> dict:
    """
    Converts string type of entity to that maps to minmod ID

    Argument
    : entity

    Return
    : dict_entity: Consists of four items. opt(confidence), opt(normalized_uri), opt(observed_name), opt(source)
    """    
    # Default entity dictionary

    dict_entity = {
        "confidence": 0.00,
        "normalized_uri": "",
        "observed_name": "",
        "source": ""
    }

    if entity_name == " " or entity_name == "" or not entity_name:
        return dict_entity

    entity_uri, confidence = identify_entity_id(observed_entity_name=entity_name, dict_entities=dict_sub_entities)

    if confidence <= 0.4:
        return dict_entity

    if entity_uri:
        dict_entity = {
            "confidence": confidence,
            "normalized_uri": f"https://minmod.isi.edu/resource/{entity_uri}",
            "observed_name": entity_name,
            "source": "UMN Matching System-ProcMinev2"
        }
    
    return dict_entity

def identify_entity_id(observed_entity_name:str, dict_entities:dict):
    # Set default values 
    entity_uri = None
    confidence = 0.0001

    # Filter out non-alphas and lowercase
    string1 = re.sub(r"[0-9]\s", '', observed_entity_name.lower())
    if not string1 or string1 == '':
        return entity_uri, confidence

    # If there exists exact match, takes first priority
    try:
        entity_uri = dict_entities[string1]
        confidence = 0.9999
    
    # Fuzzy matching scenario
    except:
        for entity, entity_id in dict_entities.items():
            string2 = re.sub(r"[0-9]\s", '', entity.lower())

            if not string2 or string2 == '':
                continue

            try:
                oc = overlap_coefficient.OverlapCoefficient(2).similarity(string1, string2)
            except:
                oc = 0

            list_text_similarity = [
                1 - normalized_levenshtein.NormalizedLevenshtein().distance(string1, string2), 
                jaro_winkler.JaroWinkler().similarity(string1, string2), 
                1 - metric_lcs.MetricLCS().distance(string1, string2), 
                cosine.Cosine(1).similarity(string1, string2), 
                oc,
                sorensen_dice.SorensenDice(2).similarity(string1, string2)
            ]

            # Average 6 difference string similarity score as confidence value
            try:
                tmp_confidence = statistics.mean(list_text_similarity)
            except:
                tmp_confidence = 0

            if tmp_confidence > confidence:
                confidence = tmp_confidence
                entity_uri = entity_id

            # Break out from iteration if confidence is almost 1.0
            if confidence > 0.9999:
                break

    return entity_uri, confidence