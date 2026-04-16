import pandas as pd
from abc import ABC, abstractmethod
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

class BaseExtractor(ABC):
    """
    Interface base para extratores ETL.
    """
    
    def __init__(self):
        self.log = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """Extração: Coleta de dados brutos."""
        pass

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transformação: Limpeza e normalização."""
        pass

    def run(self) -> pd.DataFrame:
        """Execução: Orquestração do workflow extract/transform."""
        self.log.info("Iniciando extração...")
        df = self.extract()
        if df.empty:
            self.log.warning("O DataFrame retornado da extração está vazio.")
            return df
        
        self.log.info("Iniciando transformação...")
        df = self.transform(df)
        return df

    def normalize_culture_name(self, series: pd.Series) -> pd.Series:
        """Normalização: Padronização de nomes de cultura."""
        import unicodedata
        
        def remove_accents(input_str):
            if not isinstance(input_str, str):
                return input_str
            nfkd_form = unicodedata.normalize('NFKD', input_str)
            return u"".join([c for c in nfkd_form if not unicodedata.combining(c)]).lower()

        return series.apply(remove_accents).str.strip()

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
