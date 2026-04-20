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
