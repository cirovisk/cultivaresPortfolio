import pandas as pd
import requests
import os
import io
from .base_extractor import BaseExtractor

class SigefExtractor(BaseExtractor):
    """
    Extrator SIGEF: Controle da Produção de Sementes e Mudas (MAPA).
    Campos de produção e Declarações de uso próprio.
    """

    RESOURCES = {
        "campos_producao": "https://dados.agricultura.gov.br/dataset/c7784a6e-f0ec-4196-a1ce-1d2d4784a58e/resource/6ab20c11-73a0-4ab0-8e13-2420d48dd6f5/download/sigefcamposproducaodesementes.csv",
        "uso_proprio": "https://dados.agricultura.gov.br/dataset/c7784a6e-f0ec-4196-a1ce-1d2d4784a58e/resource/3fc8e266-ec41-40b0-8d62-157b91b36b2c/download/sigefdeclaracaoareaproducaouseproprio.csv"
    }

    def __init__(self, data_dir="data/sigef", use_cache=True):
        super().__init__()
        self.data_dir = data_dir
        self.use_cache = use_cache
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir, exist_ok=True)

    def extract(self) -> dict:
        dataframes = {}
        for key, url in self.RESOURCES.items():
            filename = f"{key}.csv"
            local_path = os.path.join(self.data_dir, filename)

            if self.use_cache and os.path.exists(local_path) and not self.is_file_stale(local_path, 15):
                self.log.info(f"Usando cache SIGEF para {key}...")
                dataframes[key] = pd.read_csv(local_path, sep=';', encoding='utf-8', on_bad_lines='skip', dtype=str)
                continue

            self.log.info(f"Baixando SIGEF {key} de {url}...")
            try:
                resp = requests.get(url, timeout=60, verify=False) # Frequent SSL issues on MAPA
                resp.raise_for_status()
                with open(local_path, "wb") as f:
                    f.write(resp.content)
                dataframes[key] = pd.read_csv(io.BytesIO(resp.content), sep=';', encoding='utf-8', on_bad_lines='skip', dtype=str)
            except Exception as e:
                self.log.error(f"Erro ao baixar SIGEF {key}: {e}")
                if os.path.exists(local_path):
                    dataframes[key] = pd.read_csv(local_path, sep=';', encoding='utf-8', on_bad_lines='skip', dtype=str)

        return dataframes

    def transform(self, dataframes: dict) -> dict:
        self.log.info(f"Transformando SIGEF: {list(dataframes.keys())} dataset(s) recebidos.")
        processed = {}

        if "campos_producao" in dataframes:
            processed["campos_producao"] = self._transform_producao(dataframes["campos_producao"])
        
        if "uso_proprio" in dataframes:
            processed["uso_proprio"] = self._transform_uso_proprio(dataframes["uso_proprio"])

        return processed

    def _transform_producao(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty: return df
        df = df.copy()
        
        renames = {
            "DS_SAFRA": "safra",
            "DS_ESPECIE": "especie",
            "DS_CATEGORIA": "categoria",
            "DS_CULTIVAR": "cultivar_raw",
            "DS_MUNICIPIO": "municipio",
            "DS_UF": "uf",
            "DS_STATUS": "status",
            "DT_PLANTIO": "data_plantio",
            "DT_COLHEITA": "data_colheita",
            "NR_AREA": "area_ha",
            "NR_PRODUCAO_BRUTA": "producao_bruta_t",
            "NR_PRODUCAO_EST": "producao_est_t"
        }
        df = df.rename(columns=renames)
        
        # Tipagem e Limpeza
        num_cols = ["area_ha", "producao_bruta_t", "producao_est_t"]
        for col in num_cols:
            df[col] = pd.to_numeric(df[col].str.replace(",", "."), errors="coerce").fillna(0.0)
            
        date_cols = ["data_plantio", "data_colheita"]
        for col in date_cols:
            df[col] = pd.to_datetime(df[col], dayfirst=True, errors="coerce")

        df["cultura"] = self.normalize_string(df["especie"])
        
        return df

    def _transform_uso_proprio(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty: return df
        df = df.copy()

        renames = {
            "TIPOPERIODO": "tipo_periodo",
            "PERIODO": "periodo",
            "AREATOTAL": "area_total_ha",
            "MUNICIPIO": "municipio",
            "UF": "uf",
            "ESPECIE": "especie",
            "CULTIVAR": "cultivar_raw",
            "AREAPLANTADA": "area_plantada_ha",
            "AREAESTIMADA": "area_estimada_ha"
        }
        df = df.rename(columns=renames)

        num_cols = ["area_total_ha", "area_plantada_ha", "area_estimada_ha"]
        for col in num_cols:
            df[col] = pd.to_numeric(df[col].str.replace(",", "."), errors="coerce").fillna(0.0)

        df["cultura"] = self.normalize_string(df["especie"])

        return df

    def run(self) -> dict:
        self.log.info("Iniciando extração e transformação SIGEF...")
        data = self.extract()
        return self.transform(data)
