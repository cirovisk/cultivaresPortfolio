import pandas as pd
import logging
from .utils import normalize_string

log = logging.getLogger(__name__)

def clean_sigef_producao(df: pd.DataFrame) -> pd.DataFrame:
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
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(",", "."), errors="coerce").fillna(0.0)
        
    date_cols = ["data_plantio", "data_colheita"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], dayfirst=True, errors="coerce")

    if "especie" in df.columns:
        df["cultura"] = normalize_string(df["especie"])
    
    return df

def clean_sigef_uso_proprio(df: pd.DataFrame) -> pd.DataFrame:
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
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(",", "."), errors="coerce").fillna(0.0)

    if "especie" in df.columns:
        df["cultura"] = normalize_string(df["especie"])

    return df

def clean_sigef(dataframes: dict) -> dict:
    processed = {}
    if "campos_producao" in dataframes:
        processed["campos_producao"] = clean_sigef_producao(dataframes["campos_producao"])
    if "uso_proprio" in dataframes:
        processed["uso_proprio"] = clean_sigef_uso_proprio(dataframes["uso_proprio"])
    return processed
