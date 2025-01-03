import polars as pl

def add_attribute(pl_data: pl.DataFrame, 
                  attribute:str = None, value = None,
                  dict_attributes:dict = None) -> pl.DataFrame:
    
    if attribute and value:        
        pl_data = pl_data.with_columns(
            pl.lit(value).alias(attribute)
        )

    if dict_attributes:
        for attribute, value in dict_attributes.items():
            pl_data = pl_data.with_columns(
                tmp = pl.lit(value)
            )
            try:
                pl_data = pl_data.rename({"tmp": attribute})
            except pl.DuplicateError:
                pl_data = pl_data.with_columns(
                    pl.concat_str(
                        [
                            pl.col(attribute),
                            pl.col("tmp")
                        ],
                        separator="; ",
                    )
                ).drop("tmp")

    return pl_data