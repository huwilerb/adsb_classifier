import yaml 
from pathlib import Path 
from typing import Optional 


class Settings:
    FILE_PATH: Path = Path(__file__).resolve().parents[2].joinpath("static", "settings.yaml")
    
    def __init__(self, file: Optional[str | Path] = None):
        kwargs = self.load_settings_file(file)
        self.__dict__.update(kwargs) 
    
    def load_settings_file(self, file: Optional[str | Path] = None) -> dict:
        if isinstance(file, str):
            file_path = Path(file)
        elif isinstance(file, Path):
            file_path = file 
        else:
            if file is None:
                file_path = self.FILE_PATH
            else: 
                raise ValueError 
        if not file_path.exists():
            raise FileExistsError
        
        with file_path.open("r") as fp: 
            data = yaml.safe_load(fp)
        return data 
        

