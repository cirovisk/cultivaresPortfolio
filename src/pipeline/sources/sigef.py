"""Pipeline SIGEF: Controle da Produção de Sementes e Mudas (MAPA)."""

import os
import io
import logging
import pandas as pd
import requests

from pipeline.registry import register
from pipeline.base import BaseSource
from pipeline.utils import normalize_string, get_cultura_id, map_municipio_by_name, upsert_data
from db.manager import FatoSigefProducao, FatoSigefReservaSemente

log = logging.getLogger(__name__)


@register("sigef")
class SigefPipeline(BaseSource):
    """
    Extrator SIGEF: Controle da Produção de Sementes e Mudas (MAPA).
    Campos de produção e Declarações de uso próprio.
    """

    RESOURCES = {
        "campos_producao": "https://dados.agricultura.gov.br/dataset/c7784a6e-f0ec-4196-a1ce-1d2d4784a58e/resource/6ab20c11-73a0-4ab0-8e13-2420d48dd6f5/download/sigefcamposproducaodesementes.csv",
        "reserva_semente": "https://dados.agricultura.gov.br/dataset/c7784a6e-f0ec-4196-a1ce-1d2d4784a58e/resource/3fc8e266-ec41-40b0-8d62-157b91b36b2c/download/sigefdeclaracaoareaproducaouseproprio.csv"
    }

    def __init__(self, data_dir="data/sigef", use_cache=True):
        super().__init__()
        self.data_dir = data_dir
        self.use_cache = use_cache
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir, exist_ok=True)

    # ---- EXTRACT ----

    def extract(self, **kwargs) -> dict:
        dataframes = {}
        for key, url in self.RESOURCES.items():
            filename = f"{key}.csv"
            local_path = os.path.join(self.data_dir, filename)
            if key == "reserva_semente" and os.path.exists(os.path.join(self.data_dir, "uso_proprio.csv")):
                local_path = os.path.join(self.data_dir, "uso_proprio.csv")

            if self.use_cache and os.path.exists(local_path) and not self.is_file_stale(local_path, 15):
                self.log.info(f"Usando cache SIGEF para {key}...")
                dataframes[key] = pd.read_csv(local_path, sep=';', encoding='utf-8', on_bad_lines='skip', dtype=str)
                continue

            self.log.info(f"Baixando SIGEF {key} de {url}...")
            try:
                resp = requests.get(url, timeout=60, verify=False)  # Frequent SSL issues on MAPA
                resp.raise_for_status()
                with open(local_path, "wb") as f:
                    f.write(resp.content)
                dataframes[key] = pd.read_csv(io.BytesIO(resp.content), sep=';', encoding='utf-8', on_bad_lines='skip', dtype=str)
            except Exception as e:
                self.log.error(f"Erro ao baixar SIGEF {key}: {e}")
                if os.path.exists(local_path):
                    dataframes[key] = pd.read_csv(local_path, sep=';', encoding='utf-8', on_bad_lines='skip', dtype=str)

        return dataframes

    # ---- CLEAN ----

    def clean(self, dataframes: dict) -> dict:
        processed = {}
        if "campos_producao" in dataframes:
            processed["campos_producao"] = self._clean_producao(dataframes["campos_producao"])
        # Suporte a fontes de dados com chaves variadas ('reserva_semente' ou 'uso_proprio').
        reserva_df = dataframes.get("reserva_semente") if "reserva_semente" in dataframes else dataframes.get("uso_proprio")
        if reserva_df is not None:
            processed["reserva_semente"] = self._clean_reserva_semente(reserva_df)
        return processed

    def _clean_producao(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        df = df.copy()

        # Mapeamento atualizado conforme inspeção dos dados brutos (dados.agricultura.gov.br)
        renames = {
            "Safra": "safra", "Especie": "especie", "Categoria": "categoria",
            "Cultivar": "cultivar_raw", "Municipio": "municipio", "UF": "uf",
            "Status": "status", "Data do Plantio": "data_plantio",
            "Data de Colheita": "data_colheita", "Area": "area_ha",
            "Producao bruta": "producao_bruta_t", "Producao estimada": "producao_est_t",
            "DS_SAFRA": "safra", "DS_ESPECIE": "especie", "DS_CATEGORIA": "categoria",
            "DS_CULTIVAR": "cultivar_raw", "DS_MUNICIPIO": "municipio", "DS_UF": "uf",
            "DS_STATUS": "status", "DT_PLANTIO": "data_plantio", "DT_COLHEITA": "data_colheita",
            "NR_AREA": "area_ha", "NR_PRODUCAO_BRUTA": "producao_bruta_t",
            "NR_PRODUCAO_EST": "producao_est_t",
        }

        df.columns = [c.strip() for c in df.columns]
        df = df.rename(columns=renames)

        # Tipagem e Limpeza de Números
        num_cols = ["area_ha", "producao_bruta_t", "producao_est_t"]
        for col in num_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(",", "."), errors="coerce").fillna(0.0)

        date_cols = ["data_plantio", "data_colheita"]
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], dayfirst=True, errors="coerce")

        # Criação da coluna 'cultura' para mapeamento com a dimensão
        if "especie" in df.columns:
            df["cultura"] = normalize_string(df["especie"])
        elif "Especie" in df.columns:
            df["cultura"] = normalize_string(df["Especie"])

        return df

    def _clean_reserva_semente(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        df = df.copy()

        renames = {
            "TIPOPERIODO": "tipo_periodo", "PERIODO": "periodo",
            "AREATOTAL": "area_total_ha", "MUNICIPIO": "municipio", "UF": "uf",
            "ESPECIE": "especie", "CULTIVAR": "cultivar_raw",
            "AREAPLANTADA": "area_plantada_ha", "AREAESTIMADA": "area_estimada_ha",
            "QUANTRESERVADA": "quantidade_reservada_t",
            "DATAPLANTIA": "data_plantio", "DATAPLANTIO": "data_plantio"
        }
        df.columns = [c.strip() for c in df.columns]
        df = df.rename(columns=renames)

        num_cols = ["area_total_ha", "area_plantada_ha", "area_estimada_ha", "quantidade_reservada_t"]
        for col in num_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(",", "."), errors="coerce").fillna(0.0)

        if "especie" in df.columns:
            df["cultura"] = normalize_string(df["especie"])

        return df

    # ---- LOAD ----

    def load(self, df_dict: dict, lookups: dict) -> str:
        if not isinstance(df_dict, dict):
            return "0 registros (formato inválido)"

        map_cult = lookups["culturas"]
        map_mun_name = lookups["municipios_nome"]
        total = 0

        for key, df in df_dict.items():
            if df.empty:
                continue
            df_f = df.copy()
            df_f["id_cultura"] = df_f["cultura"].apply(lambda x: get_cultura_id(x, map_cult))
            df_f["id_municipio"] = map_municipio_by_name(df_f, map_mun_name)
            df_f = df_f.dropna(subset=["id_cultura", "id_municipio"])

            if key == "campos_producao":
                index = ['id_cultura', 'id_municipio', 'safra', 'especie', 'cultivar_raw', 'categoria']
                upsert_data(FatoSigefProducao, df_f, index_elements=index)
            elif key == "reserva_semente":
                index = ['id_cultura', 'id_municipio', 'periodo', 'especie', 'cultivar_raw']
                upsert_data(FatoSigefReservaSemente, df_f, index_elements=index)

            total += len(df_f)
            self.log.info(f"Fato SIGEF ({key}): Upsert concluído.")

        return f"{total} registros upserted (total)"
