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

def get_partial_flight(df: pl.LazyFrame, 
                       time_treshold: int = 900,
                       alt_treshold: int = 4000
                       ) -> pl.LazyFrame:
    return (
        df 
        .filter(
            pl.col("alt_baro").is_not_null()
        )
        .filter(
            [
                (
                    (pl.col("grounded") != pl.col("grounded")
                    .shift(-1))
                    .cast(pl.Int8)
                    .cumsum() < 2
                ),
                (
                    ((pl.col("timestamp").diff() > time_treshold)
                     & (pl.col("alt_baro") < alt_treshold))
                    .shift(-1)
                    .cast(pl.Int8)
                    .cumsum() < 1
                )
            ]
            
        )
        .filter(~pl.col("grounded"))
    )
    
def generate_deltas(df: pl.LazyFrame):
    return (
        df
        .with_columns(
            [
                (pl.col("timestamp").diff().alias("dt")),
                (pl.col("alt_baro").diff().alias("da")),
                (pl.col("gs").diff().alias("dgs")),
                (pl.col("timestamp") - pl.col("timestamp").first()).alias("duration")
            ]
        )
        .with_columns(
            [
                (pl.col("da")/pl.col("dt")).alias("dadt"), 
                (pl.col("dgs")/pl.col("dt")).alias("dgsdt")
            ]
        )
    )
    
def filter_deltas(df: pl.LazyFrame,
                max_dadt: int = 500, 
                max_ga: int = 1_000_000
                ) -> pl.LazyFrame:
    return (
        df
        .filter(
            [
                (pl.col("dadt") < max_dadt),
                (pl.col("dgsdt") < max_ga)
            ]
        )
    )