import pandas as pd
import logging
from .utils import normalize_string

log = logging.getLogger(__name__)

def clean_sidra(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    log.info(f"Cleaner PAM/SIDRA: {len(df)} linha(s) recebida(s).")

    col_map = {
        "D2N": "variavel",
        "V": "valor",
        "D1C": "cod_municipio_ibge",
        "D1N": "municipio_nome",
        "D3N": "ano",
        "cultura_raw": "cultura"
    }

    ausentes = [k for k in col_map if k not in df.columns]
    if ausentes:
        log.warning(f"PAM/SIDRA: colunas esperadas ausentes no DataFrame bruto: {ausentes}")

    df_clean = df.rename(columns=col_map)
    df_clean = df_clean[[c for c in col_map.values() if c in df_clean.columns]].copy()
    
    import numpy as np
    nulos_antes = df_clean["valor"].isna().sum()
    df_clean["valor"] = pd.to_numeric(df_clean["valor"].replace(['...', '-'], np.nan), errors='coerce')
    nulos_depois = df_clean["valor"].isna().sum()
    if nulos_depois > nulos_antes:
        log.info(f"PAM/SIDRA: {nulos_depois - nulos_antes} valor(es) não numérico(s) do IBGE ('...', '-') convertido(s) para NaN.")
    
    # Transformação: Pivoteamento de variáveis para colunas fato
    df_pivot = df_clean.pivot_table(
        index=["cod_municipio_ibge", "municipio_nome", "ano", "cultura"],
        columns="variavel",
        values="valor"
    ).reset_index()
    log.info(f"PAM/SIDRA pivot: {len(df_pivot)} combinação(ões) (município × cultura × ano).")
    
    df_pivot.columns.name = None
    
    var_renames = {
        "Área plantada": "area_plantada_ha",
        "Área colhida": "area_colhida_ha",
        "Quantidade produzida": "qtde_produzida_ton",
        "Valor da produção": "valor_producao_mil_reais"
    }
    
    actual_renames = {}
    for col in df_pivot.columns:
        for key, target in var_renames.items():
            if key in col:
                actual_renames[col] = target
    
    df_pivot = df_pivot.rename(columns=actual_renames)
    
    for target in var_renames.values():
        if target not in df_pivot.columns:
            df_pivot[target] = np.nan
    
    df_pivot["cultura"] = normalize_string(df_pivot["cultura"])
    
    return df_pivot
