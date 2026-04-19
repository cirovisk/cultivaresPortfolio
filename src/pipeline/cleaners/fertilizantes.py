import pandas as pd
import logging

log = logging.getLogger(__name__)

def clean_fertilizantes(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    log.info(f"Cleaner SIPEAGRO: {len(df)} linha(s) recebida(s).")

    renames = {
        "UNIDADE_DA_FEDERACAO": "uf",
        "MUNICIPIO": "municipio",
        "NUMERO_REGISTRO_ESTABELECIMENTO": "nr_registro_estabelecimento",
        "STATUS_DO_REGISTRO": "status_registro",
        "CNPJ": "cnpj",
        "RAZAO_SOCIAL": "razao_social",
        "NOME_FANTASIA": "nome_fantasia",
        "AREA_ATUACAO": "area_atuacao",
        "ATIVIDADE": "atividade",
        "CLASSIFICACAO": "classificacao"
    }

    df = df.rename(columns=renames)

    str_cols = [c for c in df.columns if df[c].dtype == object]
    for col in str_cols:
        df[col] = df[col].str.strip()

    sem_registro = df["nr_registro_estabelecimento"].isna().sum() if "nr_registro_estabelecimento" in df.columns else "N/A"
    log.info(
        f"Cleaner SIPEAGRO concluído: {len(df)} estabelecimento(s). "
        f"Sem número de registro: {sem_registro}."
    )
    return df
