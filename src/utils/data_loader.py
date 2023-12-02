import polars as pl 
from pathlib import Path

class DataLoader:
    def __init__(self, data_path: str):
        self.data_path = Path(data_path).resolve()
    
    def list_files(self, 
                   base_name: str, 
                   size: str | int, 
                   suffix: str
                   ) -> list:
        file_start = base_name + str(size)
        files = [f for f in self.data_path.iterdir() if f.suffix == suffix]
        return list(filter(lambda x: x.name.startswith(file_start), files))
    
    def load_list_lazy(self, files: list[Path]) -> list[pl.LazyFrame]: 
        """Load a list of parquet files into a list of Lazyframes
        This method works only for parquet files 
        """
        return [pl.scan_parquet(i) for i in files]
    
    def load_concat_lazy(self, files: list[Path]) -> pl.LazyFrame:
        return pl.concat(self.load_list_lazy(files))