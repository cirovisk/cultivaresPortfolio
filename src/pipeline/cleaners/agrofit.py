import pandas as pd
import logging
from .utils import normalize_string

log = logging.getLogger(__name__)

def clean_agrofit(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    log.info(f"Cleaner Agrofit: {len(df)} linha(s) recebida(s).")

    # Mapeamento de colunas
    renames = {
        "NR_REGISTRO": "nr_registro",
        "MARCA_COMERCIAL": "marca_comercial",
        "INGREDIENTE_ATIVO": "ingrediente_ativo",
        "TITULAR_DE_REGISTRO": "titular_registro",
        "CLASSE": "classe",
        "SITUACAO": "situacao",
        "CULTURA": "cultura_raw",
        "PRAGA_NOME_COMUM": "praga_comum"
    }

    df = df.rename(columns=renames)

    # Filtro de colunas relevantes
    cols = list(renames.values())
    df = df[[c for c in cols if c in df.columns]]

    # Normalizar cultura
    if "cultura_raw" in df.columns:
        df["cultura"] = normalize_string(df["cultura_raw"])
        culturas_distintas = df["cultura"].dropna().unique().tolist()
        log.info(
            f"Agrofit cleaner concluído: {len(df)} linha(s). "
            f"{len(culturas_distintas)} cultura(s) distinta(s) mapeada(s)."
        )

    return df
