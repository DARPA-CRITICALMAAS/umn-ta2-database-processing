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

# def identify_entity_id(string1, string2) -> Dict[str, str]|list[Dict[str, str]]:
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

############## ARCHIVED #################
# def identify_entity_id(entity_name: str, dict_entity: dict) -> list:
#     """
#     Compares the entity name to that in the entities dictionary
#     Returns the corresponding minmod id and the confidence score of the decision

#     Arguments
#     : entity_name:

#     Return
#     """
#     try:
#         # Exact match takes priority
#         normalized_uri = f"https://minmod.isi.edu/resource/{dict_entity[entity_name]}"
#         confidence = float(1.0) 

#     except:
#         normalized_uri = None
#         final_confidence = 0.001

#         # Run fuzzy matching
#         chartok = strsim.CharacterTokenizer()
#         charseqtok = strsim.WhitespaceCharSeqTokenizer()

#         text_t1 = chartok.tokenize(entity_name)
#         text_t2 = charseqtok.tokenize(entity_name)
#         text_t3 = charseqtok.unique_tokenize(entity_name)

#         for entity_label in list(dict_entity.keys()):
#             entity_label_t1 = chartok.tokenize(entity_label)
#             entity_label_t2 = charseqtok.tokenize(entity_label)
#             entity_label_t3 = charseqtok.unique_tokenize(entity_label)

#             out2 = [
#                 strsim.levenshtein_similarity(text_t1, entity_label_t1),
#                 strsim.jaro_winkler_similarity(text_t1, entity_label_t1),
#                 strsim.monge_elkan_similarity(text_t2, entity_label_t2),
#                 (
#                     sym_monge_score := strsim.symmetric_monge_elkan_similarity(
#                         text_t2, entity_label_t2
#                     )
#                 ),
#                 (hyjac_score := strsim.hybrid_jaccard_similarity(text_t3, entity_label_t3)),
#                 does_ordinal_match(entity_name, entity_label, sym_monge_score, 0.7),
#                 does_ordinal_match(entity_name, entity_label, hyjac_score, 0.7),
#             ]
#             confidence = statistics.mean(out2)

#             if (final_confidence < confidence) & (0.4 < confidence):
#                 normalized_uri = dict_entity[entity_label]
#                 final_confidence = confidence

#         normalized_uri = f"https://minmod.isi.edu/resource/{normalized_uri}"
#         confidence = final_confidence

#     if confidence < 0.4:
#         # Confidence is too low to be linked to something
#         # Report only the observed name
#         return None, None

#     return normalized_uri, confidence

# def does_ordinal_match(s1: str, s2: str, sim: float, threshold: float) -> float:
#     """Test for strings containing ordinal categorical values such as Su-30 vs Su-25, 29th Awards vs 30th Awards. 
#     (From entity linking in minmodkg)

#     Args:
#         s1: Cell Label
#         s2: Entity Label
#     """
#     if sim < threshold:
#         return 0.4

#     digit_tokens_1 = re.findall(r"\d+", s1)
#     digit_tokens_2 = re.findall(r"\d+", s2)

#     if digit_tokens_1 == digit_tokens_2:
#         return 1.0

#     if len(digit_tokens_1) == 0 or len(digit_tokens_2) == 0:
#         return 0.4

#     return 0.0