import pandas as pd
import requests
import os
import io
from pathlib import Path
from .base_extractor import BaseExtractor

class ZarcExtractor(BaseExtractor):
    """
    Extrator ZARC: Zoneamento Agrícola de Risco Climático (MAPA).
    Tabelas de risco por solo e decêndio com suporte a cache.
    """

    TARGET_CROPS = ["soja", "milho", "trigo", "algodao", "cana-de-acucar"]
    
    def __init__(self, use_cache: bool = True, data_dir: str = "data/zarc"):
        super().__init__()
        self.use_cache = use_cache
        self.data_dir = Path(data_dir).resolve()
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
    def extract(self) -> pd.DataFrame:
        all_dfs = []
        
        for crop in self.TARGET_CROPS:
            cache_file = self.data_dir / f"zarc_{crop}.csv"
            
            # Smart Refresh Logic
            if self.use_cache and cache_file.exists() and not self.is_file_stale(str(cache_file), 30):
                self.log.info(f"Usando cache ZARC para {crop}...")
                all_dfs.append(pd.read_csv(cache_file, sep=';', encoding='utf-8', on_bad_lines='skip'))
                continue

            self.log.info(f"Buscando recurso ZARC no CKAN para: {crop}")
            query = f'title:zarc AND ({crop} OR {crop.capitalize()})'
            ckan_url = f"https://dados.agricultura.gov.br/api/3/action/package_search?q={query}&rows=1"
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
            
            try:
                resp = requests.get(ckan_url, headers=headers, timeout=20)
                if resp.status_code == 200:
                    results = resp.json().get("result", {}).get("results", [])
                    if results:
                        csv_url = None
                        for resource in results[0].get("resources", []):
                            if resource.get("format", "").upper() in ["CSV", "CSV.GZ", "GZ"]:
                                csv_url = resource.get("url")
                                break
                                
                        if csv_url:
                            resp_csv = requests.get(csv_url, headers=headers, timeout=60)
                            resp_csv.raise_for_status()
                            
                            with open(cache_file, "wb") as f:
                                f.write(resp_csv.content)
                                
                            df_crop = pd.read_csv(
                                io.BytesIO(resp_csv.content), 
                                sep=';', 
                                encoding='utf-8', 
                                on_bad_lines='skip', 
                                compression='infer'
                            )
                            df_crop["cultura_raw"] = crop
                            all_dfs.append(df_crop)
            except Exception as e:
                self.log.error(f"Erro ao buscar ZARC para {crop}: {e}")
                if cache_file.exists():
                    all_dfs.append(pd.read_csv(cache_file, sep=';', encoding='utf-8'))

        if not all_dfs:
            return pd.DataFrame()
            
        return pd.concat(all_dfs, ignore_index=True)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
            
        df_clean = df.copy()
        
        df_clean.columns = (
            df_clean.columns.str.lower()
            .str.replace(" ", "_")
            .str.replace("-", "_")
            .str.normalize('NFKD')
            .str.encode('ascii', errors='ignore')
            .str.decode('utf-8')
        )
        
        ibge_cols = [c for c in df_clean.columns if "ibge" in c or "cd_mun" in c or "codigo_mun" in c]
        if ibge_cols:
            df_clean = df_clean.rename(columns={ibge_cols[0]: "cod_municipio_ibge"})
        
        if "cultura_raw" in df_clean.columns:
            df_clean["cultura"] = self.normalize_string(df_clean["cultura_raw"])
            df_clean = df_clean.drop(columns=["cultura_raw"])
            
        return df_clean
