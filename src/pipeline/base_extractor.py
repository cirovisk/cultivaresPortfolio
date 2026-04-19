import pandas as pd
from abc import ABC, abstractmethod
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

class BaseExtractor(ABC):
    """
    Interface base para extratores ETL. Focada apenas em I/O.
    """
    
    def __init__(self):
        self.log = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def extract(self):
        """Extração: Coleta de dados brutos da fonte. Deve retornar um ou mais pd.DataFrame"""
        pass

    def is_file_stale(self, path: str, threshold_days: int = 30) -> bool:
        """
        Verifica se um arquivo local está desatualizado baseado na sua idade.
        """
        import os
        import time
        if not os.path.exists(path):
            return False
            
        file_age_days = (time.time() - os.path.getmtime(path)) / (24 * 3600)
        return file_age_days > threshold_days
