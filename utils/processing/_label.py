import os
import logging

import polars as pl

def label_mapping(pl_data, dict_mapping):
    # dict_mapping: {original_label: new_label}
    list_new_labels = dict_mapping.values()

    pl_data = pl_data.select(
        pl.col(list_new_labels)
    )
    pl_data = pl_data.rename(dict_mapping)

    return pl_data