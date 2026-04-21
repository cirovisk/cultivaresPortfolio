import pandas as pd
import logging
from .utils import normalize_string

log = logging.getLogger(__name__)

def clean_sigef_producao(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    df = df.copy()
    
    # Mapeamento atualizado conforme inspeção dos dados brutos (dados.agricultura.gov.br)
    renames = {
        "Safra": "safra",
        "Especie": "especie",
        "Categoria": "categoria",
        "Cultivar": "cultivar_raw",
        "Municipio": "municipio",
        "UF": "uf",
        "Status": "status",
        "Data do Plantio": "data_plantio",
        "Data de Colheita": "data_colheita",
        "Area": "area_ha",
        "Producao bruta": "producao_bruta_t",
        "Producao estimada": "producao_est_t",
        "DS_SAFRA": "safra",
        "DS_ESPECIE": "especie",
        "DS_CATEGORIA": "categoria",
        "DS_CULTIVAR": "cultivar_raw",
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
    elif "Especie" in df.columns: # Fallback se rename falhou por algum motivo
         df["cultura"] = normalize_string(df["Especie"])
    
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
        "AREAESTIMADA": "area_estimada_ha",
        "QUANTRESERVADA": "quantidade_reservada_t",
        "DATAPLANTIA": "data_plantio",
        "DATAPLANTIO": "data_plantio"
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

def clean_sigef(dataframes: dict) -> dict:
    processed = {}
    if "campos_producao" in dataframes:
        processed["campos_producao"] = clean_sigef_producao(dataframes["campos_producao"])
    if "uso_proprio" in dataframes:
        processed["uso_proprio"] = clean_sigef_uso_proprio(dataframes["uso_proprio"])
    return processed
