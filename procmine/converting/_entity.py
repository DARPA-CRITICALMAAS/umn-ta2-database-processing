from typing import Dict, List

import regex as re
from strsimpy import normalized_levenshtein, jaro_winkler, metric_lcs, cosine, overlap_coefficient, sorensen_dice
# import strsim
import statistics

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

    if entity_name == " " or entity_name == "":
        return dict_entity

    entity_uri, confidence = identify_entity_id(observed_entity_name=entity_name, dict_entities=dict_sub_entities)

    if entity_uri:
        dict_entity = {
            "confidence": confidence,
            "normalized_uri": entity_uri,
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

def clean_nones(input_object: dict | list) -> dict | list:
    """
    Recursively remove all None values from either a dictionary or a list, and returns a new dictionary or list without the None values

    Arguments:
    input_object = either a dictionary or a list type that may or may not consist of a None value
    
    Return
    either a dictionary or a list type (same as the input) that does not consists of any None values
    """

    # List case
    if isinstance(input_object, list):
        list_objects = []
        for x in input_object:
            if x is not None and x !="":
                cleaned_item = clean_nones(x)
                if cleaned_item:
                    list_objects.append(clean_nones(x))
        
        return list_objects
    
    # Dictionary case
    elif isinstance(input_object, dict):
        cleaned_dict = {
            key: clean_nones(value)
            for key, value in input_object.items()
            if value and value is not None and value != ""
        }

        if not cleaned_dict:
            return None

        return cleaned_dict

    else:
        return input_object