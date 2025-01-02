from typing import Dict, List

import regex as re
import strsim
import statistics

def entity2id(entity_name: str,
              info_source: str,
              dict_entities: dict,) -> dict:
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
        'source': info_source
    }
    
    return dict_entity

def identify_entity_id(entity_name: str, dict_entity: dict) -> list:
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
        normalized_uri = None
        final_confidence = 0.001

        # Run fuzzy matching
        chartok = strsim.CharacterTokenizer()
        charseqtok = strsim.WhitespaceCharSeqTokenizer()

        text_t1 = chartok.tokenize(entity_name)
        text_t2 = charseqtok.tokenize(entity_name)
        text_t3 = charseqtok.unique_tokenize(entity_name)

        for entity_label in list(dict_entity.keys()):
            entity_label_t1 = chartok.tokenize(entity_label)
            entity_label_t2 = charseqtok.tokenize(entity_label)
            entity_label_t3 = charseqtok.unique_tokenize(entity_label)

            out2 = [
                strsim.levenshtein_similarity(text_t1, entity_label_t1),
                strsim.jaro_winkler_similarity(text_t1, entity_label_t1),
                strsim.monge_elkan_similarity(text_t2, entity_label_t2),
                (
                    sym_monge_score := strsim.symmetric_monge_elkan_similarity(
                        text_t2, entity_label_t2
                    )
                ),
                (hyjac_score := strsim.hybrid_jaccard_similarity(text_t3, entity_label_t3)),
                does_ordinal_match(entity_name, entity_label, sym_monge_score, 0.7),
                does_ordinal_match(entity_name, entity_label, hyjac_score, 0.7),
            ]
            confidence = statistics.mean(out2)

            if (final_confidence < confidence) & (0.4 < confidence):
                normalized_uri = dict_entity[entity_label]
                final_confidence = confidence

        normalized_uri = f"https://minmod.isi.edu/resource/{normalized_uri}"
        confidence = final_confidence

    if confidence < 0.4:
        # Confidence is too low to be linked to something
        # Report only the observed name
        return None, None

    return normalized_uri, confidence

def does_ordinal_match(s1: str, s2: str, sim: float, threshold: float) -> float:
    """Test for strings containing ordinal categorical values such as Su-30 vs Su-25, 29th Awards vs 30th Awards. 
    (From entity linking in minmodkg)

    Args:
        s1: Cell Label
        s2: Entity Label
    """
    if sim < threshold:
        return 0.4

    digit_tokens_1 = re.findall(r"\d+", s1)
    digit_tokens_2 = re.findall(r"\d+", s2)

    if digit_tokens_1 == digit_tokens_2:
        return 1.0

    if len(digit_tokens_1) == 0 or len(digit_tokens_2) == 0:
        return 0.4

    return 0.0