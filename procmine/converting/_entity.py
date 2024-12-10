from typing import Dict, List

import strsim

def entity2id(entity_name: str,
              entity_type: str,
              dict_entities: dict,) -> Dict[float, str, str, str]:
    """
    Converts string type of entity to that maps to minmod ID

    Argument
    : entity

    Return
    : dict_entity: Consists of four items. confidence, opt(normalized_uri), opt(observed_name), source
    """    
    entity_uri, confidence = identify_entity_id(entity_name)

    dict_entity = {
        'confidence': confidence,
        'normalized_uri': entity_uri,
        'observed_name': entity_name,
        'source': 'UMN Matching System v2'
    }
    
    return dict_entity

def identify_entity_id(entity_name: str, dict_entity: dict) -> List[str, float]:
    """
    Compares the entity name to that in the entities dictionary
    Returns the corresponding minmod id and the confidence score of the decision

    Arguments
    : entity_name:

    Return
    """
    try:
        # Exact match takes priority
        normalized_uri = f"https://minmod.isi.edu/resource/{dict_entity[entity_name]}"
        confidence = float(1.0) 

    except:
        # Run fuzzy matching
        # TODO: Migrate code from FuseMine
        normalized_uri = f"https://minmod.isi.edu/resource/{dict_entity[entity_name]}"
        confidence = float(0.0)

    if confidence < 0.4:
        # Confidence is too low to be linked to something
        # Report only the observed name
        return None, None

    return normalized_uri, confidence