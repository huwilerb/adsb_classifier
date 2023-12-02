import polars as pl 
import numpy as np 

input_types = list | pl.DataFrame | pl.DataFrame

class MinMaxNormalize:
    
   
    @staticmethod
    def _normalize_lists(data: list) -> list:
        d = np.array(data)
        min_v = np.min(d)
        max_v = np.max(d)
        scaled = (d - min_v) / (max_v - min_v)
        return scaled.tolist()
    
    def __call__(self, data: input_types):
        if isinstance(data, list):
            return self._normalize_lists(data)
        elif isinstance(data, pl.DataFrame):
            raise NotImplementedError
        elif isinstance(data, pl.LazyFrame):
            raise NotImplementedError
        else:
            raise NotImplementedError 
        
def min_max_normalize(df: pl.DataFrame | pl.LazyFrame, col: str ):
    return (
        df 
        .with_columns(
            ((pl.col(col) - pl.col(col).min()) /(pl.col(col).max() - pl.col(col).min())).alias(f"n_{col}")
        )        
        
    )


def main():
    data = {'col1': [1, 4, 7, 2, 5, 8, 3, 6, 9, 10]}
    df = pl.DataFrame(data)
    column = 'col1'
    df = (
        df.pipe(min_max_normalize, "col1")
    )
    print(df.head(10))
    
if __name__ == "__main__":
    main()