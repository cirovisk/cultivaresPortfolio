import pandas as pd
import requests
from io import StringIO
from .base_extractor import BaseExtractor

class ConabExtractor(BaseExtractor):
    """
    Extrator CONAB: Série Histórica de Produção de Grãos.
    """
    
    DATA_URL = "https://portaldeinformacoes.conab.gov.br/downloads/arquivos/SerieHistoricaGraos.txt"

    def extract(self) -> pd.DataFrame:
        self.log.info(f"Baixando dados da CONAB de: {self.DATA_URL}")
        try:
            resp = requests.get(self.DATA_URL, timeout=60)
            resp.raise_for_status()
            
            # I/O: Parsing de CSV delimitado por ";"
            df = pd.read_csv(
                StringIO(resp.text),
                sep=";",
                encoding="utf-8",
                dtype=str,
                skipinitialspace=True
            )
            return df
        except Exception as e:
            self.log.error(f"Falha ao baixar dados da CONAB: {e}")
            return pd.DataFrame()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        self.log.info("Limpando e transformando dados da CONAB...")
        
        # Sanitização: Remoção de whitespaces
        df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

        # Transformação: Mapeamento de colunas
        renames = {
            "ano_agricola": "ano_agricola",
            "dsc_safra_previsao": "safra",
            "uf": "uf",
            "produto": "produto_raw",
            "area_plantada_mil_ha": "area_plantada_mil_ha",
            "producao_mil_t": "producao_mil_t",
            "produtividade_mil_ha_mil_t": "produtividade_t_ha"
        }
        
        df = df.rename(columns=renames)
        
        # Transformação: Casting de tipos numéricos
        cols_num = ["area_plantada_mil_ha", "producao_mil_t", "produtividade_t_ha"]
        for col in cols_num:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

        # Normalizar nome da cultura
        df["cultura"] = self.normalize_culture_name(df["produto_raw"])
        
        # Filtro básico (opcional: podemos filtrar no main ou aqui)
        # Seleção: Filtro de colunas relevantes
        cols_final = ["ano_agricola", "safra", "uf", "cultura", "area_plantada_mil_ha", "producao_mil_t", "produtividade_t_ha"]
        df = df[cols_final]

        return df
