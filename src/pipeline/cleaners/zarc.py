import pandas as pd
import logging
from .utils import normalize_string

log = logging.getLogger(__name__)

def clean_zarc(df: pd.DataFrame) -> pd.DataFrame:
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
    
    ibge_cols = [c for c in df_clean.columns if "ibge" in c or "cd_mun" in c or "codigo_mun" in c or "geocodigo" in c]
    if ibge_cols:
        df_clean = df_clean.rename(columns={ibge_cols[0]: "cod_municipio_ibge"})
    
    if "cultura_raw" in df_clean.columns:
        df_clean["cultura"] = normalize_string(df_clean["cultura_raw"])
        df_clean = df_clean.drop(columns=["cultura_raw"])
        
    return df_clean
