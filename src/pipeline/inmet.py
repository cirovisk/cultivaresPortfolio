import pandas as pd
import requests
import os
import io
from datetime import datetime, timedelta
from .base_extractor import BaseExtractor

class InmetExtractor(BaseExtractor):
    """
    Extrator INMET: Dados Meteorológicos Automáticos.
    Integração via API apitempo.inmet.gov.br.
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

    def extract(self, station_ids: list) -> dict:
        """
        Busca dados históricos para uma lista de IDs de estação.
        Retorna dicionário {station_id: DataFrame}.
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=self.days_history)
        
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")

        dataframes = {}
        for sid in station_ids:
            # Lógica de Chunking: INMET API falha em períodos muito longos (> 1 ano geralmente).
            # Vou buscar o período total em blocos de 365 dias.
            df_station = self._fetch_station_data_in_chunks(sid, start_date, end_date)
            if not df_station.empty:
                dataframes[sid] = df_station
        
        return dataframes

    def _fetch_station_data_in_chunks(self, sid, start, end) -> pd.DataFrame:
        all_chunks = []
        current_start = start
        
        while current_start < end:
            current_end = min(current_start + timedelta(days=365), end)
            s_str = current_start.strftime("%Y-%m-%d")
            e_str = current_end.strftime("%Y-%m-%d")
            
            url = f"{self.API_BASE}/estacao/data/{s_str}/{e_str}/{sid}"
            self.log.info(f"Buscando INMET {sid} no período {s_str} a {e_str}...")
            
            try:
                resp = requests.get(url, timeout=60)
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
