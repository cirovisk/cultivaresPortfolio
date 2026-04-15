import pandas as pd
from abc import ABC, abstractmethod
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

class BaseExtractor(ABC):
    """
    Classe base para todos os extratores do Pipeline de Agro-Dados.
    Garante que todos herdem os comportamentos de extract, transform e load.
    """
    
    def __init__(self):
        self.log = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """Coleta os dados brutos da fonte de dados."""
        pass

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica limpeza e normalização aos dados brutos."""
        pass

    def run(self) -> pd.DataFrame:
        """Executa extração e transformação, retornando o DataFrame limpo."""
        self.log.info("Iniciando extração...")
        df = self.extract()
        if df.empty:
            self.log.warning("O DataFrame retornado da extração está vazio.")
            return df
        
        self.log.info("Iniciando transformação...")
        df = self.transform(df)
        return df

    def normalize_culture_name(self, series: pd.Series) -> pd.Series:
        """
        Padroniza nomes de cultura para cruzamento: tudo minúsculo, sem acentos, etc.
        (Removendo acentos simples que variam muito).
        """
        import unicodedata
        
        def remove_accents(input_str):
            if not isinstance(input_str, str):
                return input_str
            nfkd_form = unicodedata.normalize('NFKD', input_str)
            return u"".join([c for c in nfkd_form if not unicodedata.combining(c)]).lower()

        return series.apply(remove_accents).str.strip()
