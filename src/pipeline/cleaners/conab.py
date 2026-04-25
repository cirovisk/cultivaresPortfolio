import pandas as pd
import logging
from .utils import normalize_string

log = logging.getLogger(__name__)

def clean_producao(df: pd.DataFrame) -> pd.DataFrame:
    log.info(f"Cleaner CONAB Produção: {len(df)} linha(s) brutas.")
    df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
    renames = {
        "ano_agricola": "ano_agricola",
        "dsc_safra_previsao": "safra",
        "uf": "uf",
        "produto": "produto_raw",
        "area_plantada_mil_ha": "area_plantada_mil_ha",
        "producao_mil_t": "producao_mil_t",
        "produtividade_mil_ha_mil_t": "produtividade_t_ha"
    }
    df = df.rename(columns=renames)
    cols_num = ["area_plantada_mil_ha", "producao_mil_t", "produtividade_t_ha"]
    for col in cols_num:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    
    df["cultura"] = normalize_string(df["produto_raw"])
    cols_final = ["ano_agricola", "safra", "uf", "cultura", "area_plantada_mil_ha", "producao_mil_t", "produtividade_t_ha"]
    
    # Missing columns handling
    available_cols = [c for c in cols_final if c in df.columns]
    df_out = df[available_cols]
    return df_out

def clean_precos(df: pd.DataFrame, freq="mensal") -> pd.DataFrame:
    log.info(f"Cleaner CONAB Preços ({freq}): {len(df)} linha(s) brutas.")
    df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
    renames = {
        "produto": "produto_raw",
        "uf": "uf",
        "nom_municipio": "municipio",
        "cod_ibge": "cod_municipio_ibge",
        "ano": "ano",
        "mes": "mes",
        "valor_produto_kg": "valor_kg",
        "dsc_nivel_comercializacao": "nivel_comercializacao",
        "semana": "semana",
        "data_inicial_final_semana": "data_referencia"
    }
    df = df.rename(columns=renames)
    
    # Casting e Limpeza
    if "valor_kg" in df.columns:
        df["valor_kg"] = pd.to_numeric(df["valor_kg"].str.replace(",", "."), errors="coerce").fillna(0.0)
    if "ano" in df.columns:
        df["ano"] = pd.to_numeric(df["ano"], errors="coerce").fillna(0).astype(int)
    if "mes" in df.columns:
        df["mes"] = pd.to_numeric(df["mes"], errors="coerce").fillna(0).astype(int)
    if "produto_raw" in df.columns:
        df["cultura"] = normalize_string(df["produto_raw"])
    
    cols = ["cultura", "uf", "ano", "mes", "valor_kg", "nivel_comercializacao"]
    if "cod_municipio_ibge" in df.columns:
        cols.append("cod_municipio_ibge")
        df["cod_municipio_ibge"] = df["cod_municipio_ibge"].str.strip()
        
    if freq == "semanal":
        cols.extend(["semana", "data_referencia"])
        if "semana" in df.columns:
            df["semana"] = pd.to_numeric(df["semana"], errors="coerce").fillna(0).astype(int)
    
    available_cols = [c for c in cols if c in df.columns]
    return df[available_cols]

def clean_conab(dataframes: dict) -> dict:
    """Entrypoint funcional para limpar o dict vindo do ConabExtractor."""
    processed = {}
    
    for key in ["producao_historica", "producao_estimativa"]:
        if key in dataframes:
            processed[key] = clean_producao(dataframes[key])
    
    for key in ["precos_uf_mensal", "precos_mun_mensal"]:
        if key in dataframes:
            processed[key] = clean_precos(dataframes[key], freq="mensal")

    for key in ["precos_uf_semanal", "precos_mun_semanal"]:
        if key in dataframes:
            processed[key] = clean_precos(dataframes[key], freq="semanal")

    return processed
