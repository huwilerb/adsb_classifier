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