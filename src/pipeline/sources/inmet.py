"""
Pipeline INMET: Dados Meteorológicos Automáticos.
Integração via API apitempo.inmet.gov.br.
"""

import os
import logging
import pandas as pd
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from pipeline.registry import register
from pipeline.base import BaseSource
from pipeline.utils import upsert_data
from db.manager import DimMunicipio, FatoMeteorologia

log = logging.getLogger(__name__)


@register("inmet")
class InmetPipeline(BaseSource):
    """
    Pipeline INMET: Extrai, limpa e carrega dados meteorológicos.
    Sobrescreve run() pois a lógica de matching estação↔município precisa
    ocorrer entre extract e load.
    """

    API_BASE = "https://apitempo.inmet.gov.br"

    def __init__(self, days_history=730, use_cache=True, data_dir="data/inmet"):
        """
        :param days_history: Quantidade de dias para buscar (default 2 anos).
        """
        super().__init__()
        self.days_history = days_history
        self.use_cache = use_cache
        self.data_dir = data_dir
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir, exist_ok=True)

    # ---- RUN (Override: matching estação↔município) ----

    def run(self, lookups: dict, **kwargs) -> str:
        """Override: orquestra matching estação↔município, extração e carga."""
        self.log.info("Iniciando pipeline Meteorologia...")
        db = lookups["db"]

        # 1. Buscar estações
        stations_df = self.get_stations()
        if stations_df.empty:
            return "0 registros (sem estações)"
        stations_df["name_norm"] = stations_df["DC_NOME"].str.lower().str.strip()

        # 2. Matching estação↔município
        all_muns = db.query(DimMunicipio).all()
        mun_to_station = {}
        for m in all_muns:
            match = stations_df[
                (stations_df["name_norm"] == m.nome.lower().strip()) &
                (stations_df["SG_ESTADO"] == m.uf)
            ]
            if match.empty:
                match = stations_df[
                    (stations_df["name_norm"].str.contains(m.nome.lower().strip())) &
                    (stations_df["SG_ESTADO"] == m.uf)
                ]
            if not match.empty:
                mun_to_station[m.id_municipio] = match.iloc[0]["CD_ESTACAO"]

        unique_stations = list(set(mun_to_station.values()))
        if not unique_stations:
            return "0 registros (nenhuma estação mapeada)"

        # 3. Extract
        raw_data = self.extract(station_ids=unique_stations)

        # 4. Clean
        df_meteo = self.clean(raw_data)

        if df_meteo.empty:
            return "0 registros (sem dados meteorológicos)"

        # 5. Merge estação→município e Load
        station_mun_rows = [
            {"estacao_id": sid, "id_municipio": mid}
            for mid, sid in mun_to_station.items()
        ]
        station_mun_df = pd.DataFrame(station_mun_rows)
        df_final = df_meteo.merge(station_mun_df, on="estacao_id", how="inner")

        if df_final.empty:
            return "0 registros (sem match estação↔município)"

        result = self.load(df_final, lookups)
        self.log.info(f"Pipeline Meteorologia concluído: {result}")
        return result

    # ---- EXTRACT ----

    def get_stations(self) -> pd.DataFrame:
        """Busca lista de estações automáticas (tipo T)."""
        cache_file = os.path.join(self.data_dir, "stations.csv")
        if self.use_cache and os.path.exists(cache_file) and not self.is_file_stale(cache_file, 30):
            return pd.read_csv(cache_file)

        self.log.info("Buscando metadados de estações INMET...")
        try:
            resp = requests.get(f"{self.API_BASE}/estacoes/T", timeout=30)
            resp.raise_for_status()
            df = pd.DataFrame(resp.json())
            if self.use_cache:
                df.to_csv(cache_file, index=False)
            return df
        except Exception as e:
            self.log.error(f"Erro ao buscar estações: {e}")
            return pd.read_csv(cache_file) if os.path.exists(cache_file) else pd.DataFrame()

    def extract(self, **kwargs) -> dict:
        """
        Busca dados históricos para uma lista de IDs de estação.
        Retorna dicionário {station_id: DataFrame}.
        Otimizado: requests paralelos via ThreadPoolExecutor (I/O-bound).
        """
        station_ids = kwargs.get("station_ids", [])
        if not station_ids:
            return {}

        end_date = datetime.now()
        start_date = end_date - timedelta(days=self.days_history)

        dataframes = {}
        max_workers = min(10, len(station_ids)) if station_ids else 1

        def _fetch_one(sid):
            return sid, self._fetch_station_data_in_chunks(sid, start_date, end_date)

        self.log.info(f"Buscando INMET: {len(station_ids)} estações com {max_workers} workers paralelos...")
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(_fetch_one, sid): sid for sid in station_ids}
            for future in as_completed(futures):
                try:
                    sid, df_station = future.result()
                    if not df_station.empty:
                        dataframes[sid] = df_station
                except Exception as e:
                    self.log.error(f"Erro paralelo em estação {futures[future]}: {e}")

        return dataframes

    def _fetch_station_data_in_chunks(self, sid, start, end) -> pd.DataFrame:
        all_chunks = []
        current_start = start

        while current_start < end:
            current_end = min(current_start + timedelta(days=365), end)
            s_str = current_start.strftime("%Y-%m-%d")
            e_str = current_end.strftime("%Y-%m-%d")

            url = f"{self.API_BASE}/estacao/{s_str}/{e_str}/{sid}"
            self.log.info(f"Buscando INMET {sid} no período {s_str} a {e_str}...")

            try:
                headers = {"User-Agent": "Mozilla/5.0"}
                resp = requests.get(url, headers=headers, timeout=60)
                if resp.status_code == 200:
                    data = resp.json()
                    if isinstance(data, list) and data:
                        all_chunks.append(pd.DataFrame(data))
                else:
                    self.log.warning(f"Erro {resp.status_code} para {sid} ({s_str})")
            except Exception as e:
                self.log.error(f"Erro ao buscar bloco para {sid}: {e}")

            current_start = current_end + timedelta(days=1)

        return pd.concat(all_chunks, ignore_index=True) if all_chunks else pd.DataFrame()

    # ---- CLEAN ----

    def clean(self, dataframes: dict) -> pd.DataFrame:
        all_dfs = []
        for sid, df in dataframes.items():
            if df.empty:
                continue

            df = df.copy()
            # Converter colunas numéricas (INMET retorna strings no JSON)
            num_cols = ["CHUVA", "TEM_MAX", "TEM_MIN", "TEM_INS", "UMD_INS"]
            for col in num_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            # Data/Hora
            if "DT_MEDICAO" in df.columns:
                # INMET usa DT_MEDICAO YYYY-MM-DD e HR_MEDICAO HHMM
                df["dt_hora"] = pd.to_datetime(
                    df["DT_MEDICAO"] + " " + df["HR_MEDICAO"].str.zfill(4),
                    format="%Y-%m-%d %H%M", errors="coerce"
                )
                df = df.dropna(subset=["dt_hora"])

                # Agregação Diária
                daily = df.groupby(df["dt_hora"].dt.date).agg(
                    precipitacao_total_mm=("CHUVA", "sum"),
                    temp_max_c=("TEM_MAX", "max"),
                    temp_min_c=("TEM_MIN", "min"),
                    temp_media_c=("TEM_INS", "mean"),
                    umidade_media=("UMD_INS", "mean")
                ).reset_index()

                daily = daily.rename(columns={"dt_hora": "data"})
                daily["estacao_id"] = sid
                all_dfs.append(daily)

        if not all_dfs:
            return pd.DataFrame()

        final_df = pd.concat(all_dfs, ignore_index=True)
        final_df["data"] = pd.to_datetime(final_df["data"])
        return final_df

    # ---- LOAD ----

    def load(self, df: pd.DataFrame, lookups: dict) -> str:
        if df.empty:
            return "0 registros"

        upsert_data(FatoMeteorologia, df, index_elements=['id_municipio', 'data'])
        result = f"{len(df)} registros upserted"
        self.log.info(f"Fato Meteorologia: {result}.")
        return result
