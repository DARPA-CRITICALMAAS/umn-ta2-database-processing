from typing import Dict, List

import strsim

def entity2id(entity_name: str,) -> Dict[float, str, str, str]:
    """
    Converts string type of entity to that maps to minmod ID

    Argument
    : entity

    Return
    : dict_entity: Consists of four items. confidence, opt(normalized_uri), opt(observed_name), source
    """    
    entity_id = identify_entity_id(entity_name)

    dict_entity = {
        'confidence': float(0.0),
        'normalized_uri': f'https://minmod.isi.edu/resource/{entity_id}',
        'observed_name': entity_name,
        'source': 'UMN Matching System v2'
    }
    
    return dict_entity

def identify_entity_id(entity_name: str) -> List[str, float]:
    # Exact match takes priority

    pass