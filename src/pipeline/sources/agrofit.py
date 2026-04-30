"""Pipeline Agrofit: Produtos Formulados / Agrotóxicos (MAPA)."""

import logging
import pandas as pd
import requests
from pathlib import Path

from pipeline.registry import register
from pipeline.base import BaseSource
from pipeline.utils import normalize_string, get_cultura_id, upsert_data
from db.manager import FatoAgrofit

log = logging.getLogger(__name__)


@register("agrofit")
class AgrofitPipeline(BaseSource):
    """
    Extrator Agrofit: Produtos Formulados / Agrotóxicos (MAPA).
    """

    DATA_URL = "https://dados.agricultura.gov.br/dataset/6c913699-e82e-4da3-a0a1-fb6c431e367f/resource/d30b30d7-e256-484e-9ab8-cd40974e1238/download/agrofitprodutosformulados.csv"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    }

    def __init__(self, use_cache: bool = True, cache_path: str = "data/agrofit_produtos.csv"):
        super().__init__()
        self.use_cache = use_cache
        self.cache_path = Path(cache_path).resolve()

    # ---- EXTRACT ----

    def extract(self, **kwargs) -> pd.DataFrame:
        if self.use_cache and self.cache_path.exists():
            if not self.is_file_stale(str(self.cache_path), threshold_days=30):
                self.log.info(f"Usando cache local Agrofit (atualizado): {self.cache_path}")
                return self._read_csv()
            self.log.info("Cache Agrofit expirado. Atualizando...")

        self.log.info(f"Baixando dados do Agrofit (volumetria alta): {self.DATA_URL}")
        try:
            resp = requests.get(self.DATA_URL, headers=self.HEADERS, timeout=300)
            resp.raise_for_status()

            self.cache_path.parent.mkdir(parents=True, exist_ok=True)
            self.cache_path.write_bytes(resp.content)
            self.log.info(f"Download concluído e salvo em: {self.cache_path}")

            return self._read_csv()
        except Exception as e:
            self.log.error(f"Falha ao extrair dados do Agrofit: {e}")
            if self.cache_path.exists():
                return self._read_csv()
            return pd.DataFrame()

    def _read_csv(self) -> pd.DataFrame:
        return pd.read_csv(
            self.cache_path,
            sep=";",
            encoding="utf-8",
            dtype=str,
            on_bad_lines='skip',
            low_memory=False
        )

    # ---- CLEAN ----

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        self.log.info(f"Cleaner Agrofit: {len(df)} linha(s) recebida(s).")

        # Mapeamento de colunas
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

        # Filtro de colunas relevantes
        cols = list(renames.values())
        df = df[[c for c in cols if c in df.columns]]

        # Normalizar cultura
        if "cultura_raw" in df.columns:
            df["cultura"] = normalize_string(df["cultura_raw"])
            culturas_distintas = df["cultura"].dropna().unique().tolist()
            self.log.info(
                f"Agrofit cleaner concluído: {len(df)} linha(s). "
                f"{len(culturas_distintas)} cultura(s) distinta(s) mapeada(s)."
            )

        return df

    # ---- LOAD ----

    def load(self, df: pd.DataFrame, lookups: dict) -> str:
        if df.empty:
            return "0 registros"

        df_f = df.copy()
        df_f["id_cultura"] = df_f["cultura"].apply(lambda x: get_cultura_id(x, lookups["culturas"]))
        df_f = df_f.dropna(subset=["id_cultura"])
        index = ['id_cultura', 'nr_registro', 'marca_comercial', 'praga_comum']
        upsert_data(FatoAgrofit, df_f, index_elements=index)
        result = f"{len(df_f)} registros upserted"
        self.log.info(f"Fato Agrofit: {result}.")
        return result
