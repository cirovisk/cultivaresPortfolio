"""
Pipeline Open-Meteo: Dados Meteorológicos Históricos.
Substitui INMET devido a instabilidades.
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

@register("open_meteo")
class OpenMeteoPipeline(BaseSource):
    """
    Pipeline Open-Meteo: Extrai, limpa e carrega dados meteorológicos.
    """

    API_BASE = "https://archive-api.open-meteo.com/v1/archive"

    def __init__(self, days_history=730, use_cache=True, data_dir="data/open_meteo"):
        super().__init__()
        self.days_history = days_history
        self.use_cache = use_cache
        self.data_dir = data_dir
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir, exist_ok=True)

    def run(self, lookups: dict, **kwargs) -> str:
        self.log.info("Iniciando pipeline Open-Meteo...")
        db = lookups["db"]

        # 1. Carregar coordenadas dos municípios
        coords_df = self.get_municipios_coords()
        if coords_df.empty:
            return "0 registros (sem coordenadas)"
        
        # 2. Obter municípios do banco
        all_muns = db.query(DimMunicipio).all()
        
        mun_coords = {}
        for m in all_muns:
            # Codigo IBGE no CSV geralmente é os 7 digitos
            match = coords_df[coords_df["codigo_ibge"].astype(str) == str(m.codigo_ibge)]
            if not match.empty:
                mun_coords[m.id_municipio] = {
                    "lat": match.iloc[0]["latitude"],
                    "lon": match.iloc[0]["longitude"]
                }
        
        if not mun_coords:
            return "0 registros (nenhum município com coordenadas)"

        # Para fins de portfólio e evitar timeout na API, limitamos a 50 municípios aleatórios
        limit = kwargs.get("limit", 50)
        selected_muns = dict(list(mun_coords.items())[:limit])
        self.log.info(f"Selecionados {len(selected_muns)} municípios para buscar dados meteorológicos.")

        # 3. Extract
        raw_data = self.extract(mun_coords=selected_muns)

        # 4. Clean
        df_meteo = self.clean(raw_data)

        if df_meteo.empty:
            return "0 registros (sem dados meteorológicos)"

        # 5. Load
        result = self.load(df_meteo, lookups)
        self.log.info(f"Pipeline Open-Meteo concluído: {result}")
        return result

    def get_municipios_coords(self) -> pd.DataFrame:
        cache_file = os.path.join(self.data_dir, "municipios_coords.csv")
        if self.use_cache and os.path.exists(cache_file) and not self.is_file_stale(cache_file, 30):
            return pd.read_csv(cache_file)

        self.log.info("Baixando coordenadas de municípios...")
        try:
            url = "https://raw.githubusercontent.com/kelvins/Municipios-Brasileiros/main/csv/municipios.csv"
            df = pd.read_csv(url)
            if self.use_cache:
                df.to_csv(cache_file, index=False)
            return df
        except Exception as e:
            self.log.error(f"Erro ao buscar coordenadas: {e}")
            return pd.read_csv(cache_file) if os.path.exists(cache_file) else pd.DataFrame()

    def extract(self, **kwargs) -> dict:
        mun_coords = kwargs.get("mun_coords", {})
        if not mun_coords:
            return {}

        end_date = datetime.now() - timedelta(days=2) # API archive tem ~2 dias de lag
        start_date = end_date - timedelta(days=self.days_history)
        
        s_str = start_date.strftime("%Y-%m-%d")
        e_str = end_date.strftime("%Y-%m-%d")

        dataframes = {}
        max_workers = min(10, len(mun_coords)) if mun_coords else 1

        def _fetch_one(mid, coords):
            url = (
                f"{self.API_BASE}?"
                f"latitude={coords['lat']}&longitude={coords['lon']}"
                f"&start_date={s_str}&end_date={e_str}"
                f"&daily=precipitation_sum,temperature_2m_max,temperature_2m_min,temperature_2m_mean"
                f"&timezone=America/Sao_Paulo"
            )
            try:
                resp = requests.get(url, timeout=30)
                if resp.status_code == 200:
                    data = resp.json()
                    if "daily" in data:
                        return mid, pd.DataFrame(data["daily"])
            except Exception as e:
                self.log.error(f"Erro ao buscar Open-Meteo {mid}: {e}")
            return mid, pd.DataFrame()

        self.log.info(f"Buscando Open-Meteo: {len(mun_coords)} municípios com {max_workers} workers paralelos...")
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(_fetch_one, mid, coords): mid for mid, coords in mun_coords.items()}
            for future in as_completed(futures):
                try:
                    mid, df_mun = future.result()
                    if not df_mun.empty:
                        dataframes[mid] = df_mun
                except Exception as e:
                    self.log.error(f"Erro paralelo em município {futures[future]}: {e}")

        return dataframes

    def clean(self, dataframes: dict) -> pd.DataFrame:
        all_dfs = []
        for mid, df in dataframes.items():
            if df.empty:
                continue

            df = df.copy()
            df = df.rename(columns={
                "time": "data",
                "precipitation_sum": "precipitacao_total_mm",
                "temperature_2m_max": "temp_max_c",
                "temperature_2m_min": "temp_min_c",
                "temperature_2m_mean": "temp_media_c"
            })
            
            df["umidade_media"] = None
            df["estacao_id"] = "OPEN-METEO"
            df["id_municipio"] = mid
            all_dfs.append(df)

        if not all_dfs:
            return pd.DataFrame()

        final_df = pd.concat(all_dfs, ignore_index=True)
        final_df["data"] = pd.to_datetime(final_df["data"])
        final_df = final_df.dropna(subset=["data"])
        return final_df

    def load(self, df: pd.DataFrame, lookups: dict) -> str:
        if df.empty:
            return "0 registros"

        upsert_data(FatoMeteorologia, df, index_elements=['id_municipio', 'data'])
        result = f"{len(df)} registros upserted"
        self.log.info(f"Fato Meteorologia (Open-Meteo): {result}.")
        return result
