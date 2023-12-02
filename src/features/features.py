import polars as pl 

def clean_grounded(df: pl.LazyFrame) -> pl.LazyFrame:
    return (
        df
        .filter(
            (~(((pl.col("grounded") != pl.col("grounded")
                .shift(-1))
                .cast(pl.Int8)
                .cumsum() == 0)         
                & (pl.col("grounded").shift(-1))))
        )
    )