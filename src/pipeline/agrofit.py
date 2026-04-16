import pandas as pd
import requests
from .base_extractor import BaseExtractor
from io import BytesIO

class AgrofitExtractor(BaseExtractor):
    """
    Extrator Agrofit: Produtos Formulados / Agrotóxicos (MAPA).
    """
    
    DATA_URL = "https://dados.agricultura.gov.br/dataset/6c913699-e82e-4da3-a0a1-fb6c431e367f/resource/d30b30d7-e256-484e-9ab8-cd40974e1238/download/agrofitprodutosformulados.csv"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    }

    def extract(self) -> pd.DataFrame:
        self.log.info(f"Baixando dados do Agrofit (pode demorar devido ao tamanho): {self.DATA_URL}")
        try:
            # Desempenho: Alta volumetria (~400MB)
            
            resp = requests.get(self.DATA_URL, headers=self.HEADERS, timeout=300)
            resp.raise_for_status()
            
            # I/O: Parsing de CSV (delimiter=";")
            df = pd.read_csv(
                BytesIO(resp.content), 
                sep=";", 
                encoding="utf-8", 
                dtype=str, 
                on_bad_lines='skip',
                low_memory=False
            )
            return df
        except Exception as e:
            self.log.error(f"Falha ao extrair dados do Agrofit: {e}")
            return pd.DataFrame()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        self.log.info("Transformando dados do Agrofit...")
        
        # Transformação: Mapeamento de colunas
        renames = {
            "NR_REGISTRO": "nr_registro",
            "MARCA_COMERCIAL": "marca_comercial",
            "INGREDIENTE_ATIVO": "ingrediente_ativo",
            "TITULAR_DE_REGISTRO": "titular_registro",
            "CLASSE": "classe",
            "SITUACAO": "situacao",
            "CULTURA": "cultura_raw",
            "PRAGA_NOME_COMUM": "praga_comum"
        }
        
        df = df.rename(columns=renames)
        
        # Seleção: Filtro de colunas relevantes
        cols = list(renames.values())
        df = df[[c for c in cols if c in df.columns]]
        
        # Normalizar cultura
        if "cultura_raw" in df.columns:
            df["cultura"] = self.normalize_culture_name(df["cultura_raw"])
        
        return df
