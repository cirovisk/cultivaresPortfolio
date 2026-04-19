import pandas as pd
import numpy as np
import re
import logging
from .utils import normalize_string

log = logging.getLogger(__name__)

ACCENT_CORRECTIONS = {
    "Alocasia":   "Alocásia",
    "Amarilis":   "Amarílis",
    "Aralia":     "Arália",
    "Bicuiba":    "Bicuíba",
    "Bromelia":   "Bromélia",
    "Cainga":     "Caingá",
    "Catuaba":    "Catuába",
    "Croton":     "Cróton",
    "Euforbia":   "Eufórbia",
    "Gipsofila":  "Gipsófila",
    "Guaraiuva":  "Guaraiúva",
    "Magnolia":   "Magnólia",
    "Orquidea":   "Orquídea",
    "OrquÍdea":   "Orquídea",
    "Peperomia":  "Peperômia",
    "Pera":       "Pêra",
}

def clean_cultivares(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    log.info(f"Cleaner Cultivares: {len(df)} linha(s) recebida(s).")
    df_clean = df.copy()

    # Regexes de limpeza (Migradas do Legacy)
    _RE_HTML_TAGS = re.compile(r"<[^>]{0,30}>|</\\+>", re.IGNORECASE)
    _RE_ASPAS_ENVOLVENDO = re.compile(r"^['\"](.*)['\"]$")
    _RE_BACKSLASH = re.compile(r"\\+'")

    def _limpar_texto(serie: pd.Series) -> pd.Series:
        tmp = serie.copy().str.strip()
        tmp = tmp.str.replace(_RE_ASPAS_ENVOLVENDO, r"\1", regex=True)
        tmp = tmp.str.replace(_RE_HTML_TAGS, "", regex=True)
        tmp = tmp.str.replace(_RE_BACKSLASH, "'", regex=True)
        return tmp.str.strip().replace("", np.nan)

    colunas_texto = ["CULTIVAR", "NOME COMUM", "NOME CIENTÍFICO", "GRUPO DA ESPÉCIE", "SITUAÇÃO", "MANTENEDOR (REQUERENTE) (NOME)"]
    for col in colunas_texto:
        if col in df_clean.columns:
            df_clean[col] = _limpar_texto(df_clean[col])

    if "NOME COMUM" in df_clean.columns:
        antes = df_clean["NOME COMUM"].copy()
        df_clean["NOME COMUM"] = df_clean["NOME COMUM"].replace(ACCENT_CORRECTIONS)
        corrigidos = (df_clean["NOME COMUM"] != antes).sum()
        if corrigidos:
            log.info(f"Correção de acentos: {corrigidos} valor(es) corrigido(s) em 'NOME COMUM'.")

    if "CULTIVAR" in df_clean.columns:
        split_c = df_clean["CULTIVAR"].str.split("/", n=1, expand=True)
        df_clean["CULTIVAR"] = (
            split_c[0].str.strip()
            .str.replace(_RE_ASPAS_ENVOLVENDO, r"\1", regex=True)
            .str.strip()
        )
        if split_c.shape[1] > 1:
            df_clean["NOME SECUNDÁRIO"] = (
                split_c[1].str.strip()
                .str.replace(_RE_ASPAS_ENVOLVENDO, r"\1", regex=True)
                .str.strip()
            )
            df_clean["NOME SECUNDÁRIO"] = df_clean["NOME SECUNDÁRIO"].replace("", np.nan)
            n_sec = df_clean["NOME SECUNDÁRIO"].notna().sum()
            log.info(f"Parsing nomes secundários: {n_sec} cultivar(es) com nome alternativo (split '/').")
        else:
            df_clean["NOME SECUNDÁRIO"] = pd.NA

    # Normalização: Alinhamento de nomes para cruzamento
    if "NOME COMUM" in df_clean.columns:
        df_clean["CULTURA_NORMALIZADA"] = normalize_string(df_clean["NOME COMUM"])
    elif "GRUPO DA ESPÉCIE" in df_clean.columns:
        df_clean["CULTURA_NORMALIZADA"] = normalize_string(df_clean["GRUPO DA ESPÉCIE"])

    # Transformação de data (ISO 8601)
    for c in ["DATA DO REGISTRO", "DATA DE VALIDADE DO REGISTRO"]:
        if c in df_clean.columns:
            antes_nulos = df_clean[c].isna().sum()
            df_clean[c] = pd.to_datetime(df_clean[c], dayfirst=True, errors="coerce")
            novos_nulos = df_clean[c].isna().sum() - antes_nulos
            if novos_nulos > 0:
                log.warning(f"Parsing de data '{c}': {novos_nulos} valor(es) não convertido(s) → NaT.")

    if "DATA DO REGISTRO" in df_clean.columns:
        df_clean["ANO"] = df_clean["DATA DO REGISTRO"].dt.year.astype("Int64")

    # Enriquecimento: Classificação de mantenedor (Público/Privado), dicionário de palavras-chaves minerado manualmente
    if "MANTENEDOR (REQUERENTE) (NOME)" in df_clean.columns:
        _PUBL = ["EMBRAPA", "UNIVERSIDADE", "INSTITUTO", "EPAGRI", "PESAGRO", "IAPAR", "SECRETARIA", "FACULDADE"]
        def cat_setor(x):
            if pd.isna(x): return "Nulo"
            x_u = x.upper()
            if any(p in x_u for p in _PUBL): return "Público"
            return "Privado"
        df_clean["SETOR"] = df_clean["MANTENEDOR (REQUERENTE) (NOME)"].apply(cat_setor)

    renames = {
        "CULTIVAR": "cultivar",
        "NOME SECUNDÁRIO": "nome_secundario",
        "NOME COMUM": "nome_comum",
        "NOME CIENTÍFICO": "nome_cientifico",
        "GRUPO DA ESPÉCIE": "grupo_especie",
        "SITUAÇÃO": "situacao",
        "Nº FORMULÁRIO": "nr_formulario",
        "Nº REGISTRO": "nr_registro",
        "DATA DO REGISTRO": "data_reg",
        "DATA DE VALIDADE DO REGISTRO": "data_val",
        "MANTENEDOR (REQUERENTE) (NOME)": "mantenedor",
        "CULTURA_NORMALIZADA": "cultura"
    }

    df_clean = df_clean.rename(columns=renames)
    antes_dedup = len(df_clean)
    df_clean = df_clean.drop_duplicates()
    descartados = antes_dedup - len(df_clean)
    if descartados:
        log.info(f"drop_duplicates: {descartados} linha(s) duplicada(s) removida(s).")

    log.info(
        f"Cleaner concluído: {len(df_clean)} linha(s) resultantes. "
        f"Nulos em 'nr_registro': {df_clean.get('nr_registro', df_clean.get('Nº REGISTRO')).isna().sum() if 'nr_registro' in df_clean.columns or 'Nº REGISTRO' in df_clean.columns else 'N/A'}."
    )

    return df_clean
