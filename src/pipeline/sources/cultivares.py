"""Pipeline Cultivares: Registro Nacional de Cultivares (MAPA/SNPC)."""

import re
import numpy as np
import logging
import pandas as pd
import requests
from pathlib import Path

from pipeline.registry import register
from pipeline.base import BaseSource
from pipeline.utils import normalize_string, get_cultura_id, upsert_data
from pipeline.dimensions import preencher_dimensao_mantenedor
from db.manager import FatoCultivar

log = logging.getLogger(__name__)

# Constantes de limpeza (correção de acentos em nomes de espécies)
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

# Regexes de limpeza
_RE_HTML_TAGS = re.compile(r"<[^>]{0,30}>|</\\+>", re.IGNORECASE)
_RE_ASPAS_ENVOLVENDO = re.compile(r"^['\"](.*)['\"]$")
_RE_BACKSLASH = re.compile(r"\\+'")


@register("cultivares")
class CultivaresPipeline(BaseSource):
    """
    Extrator SNPC: Registro Nacional de Cultivares (MAPA).
    """

    SNPC_URL   = "https://sistemas.agricultura.gov.br/snpc/cultivarweb/cultivares_registradas.php"
    SNPC_QUERY = {"acao": "pesquisar", "postado": "1"}
    SNPC_DATA  = {"exportar": "csv"}
    HEADERS    = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
        "Referer": "https://sistemas.agricultura.gov.br/snpc/cultivarweb/cultivares_registradas.php?acao=pesquisar&postado=1",
        "Accept": "text/csv,text/html,*/*;q=0.9",
    }

    def __init__(self, use_cache: bool = True, cache_path: str = "data/relatorio_cultivares.csv"):
        super().__init__()
        self.use_cache = use_cache
        self.cache_path = Path(cache_path).resolve()

    # ---- EXTRACT ----

    def extract(self, **kwargs) -> pd.DataFrame:
        if self.use_cache and self.cache_path.exists():
            if not self.is_file_stale(str(self.cache_path), threshold_days=30):
                self.log.info(f"Usando cache local (atualizado): {self.cache_path}")
                return self._read_csv()
            self.log.info(f"Cache local expirado ({self.cache_path}). Atualizando...")

        self.log.info("Baixando CSV do SNPC/MAPA...")
        try:
            resp = requests.post(
                self.SNPC_URL,
                params=self.SNPC_QUERY,
                data=self.SNPC_DATA,
                headers=self.HEADERS,
                timeout=180,
                stream=True,
            )
            resp.raise_for_status()
            self.cache_path.parent.mkdir(parents=True, exist_ok=True)
            self.cache_path.write_bytes(resp.content)
            self.log.info(f"Download concluído: {self.cache_path}")
        except Exception as e:
            self.log.error(f"Falha no download: {e}")
            if self.cache_path.exists():
                return self._read_csv()
            return pd.DataFrame()

        return self._read_csv()

    def _read_csv(self) -> pd.DataFrame:
        return pd.read_csv(
            self.cache_path,
            dtype=str,
            encoding="utf-8",
            skipinitialspace=True,
            na_values=["", "NA", "N/A", "nan", "NaN", "NULL", "null", "-", "--"],
            keep_default_na=True,
        )

    # ---- CLEAN ----

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        self.log.info(f"Cleaner Cultivares: {len(df)} linha(s) recebida(s).")
        df_clean = df.copy()

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
                self.log.info(f"Correção de acentos: {corrigidos} valor(es) corrigido(s) em 'NOME COMUM'.")

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
                self.log.info(f"Parsing nomes secundários: {n_sec} cultivar(es) com nome alternativo (split '/').")
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
                    self.log.warning(f"Parsing de data '{c}': {novos_nulos} valor(es) não convertido(s) → NaT.")

        if "DATA DO REGISTRO" in df_clean.columns:
            df_clean["ANO"] = df_clean["DATA DO REGISTRO"].dt.year.astype("Int64")

        # Enriquecimento: Classificação de mantenedor (Público/Privado)
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
            self.log.info(f"drop_duplicates: {descartados} linha(s) duplicada(s) removida(s).")

        self.log.info(
            f"Cleaner concluído: {len(df_clean)} linha(s) resultantes. "
            f"Nulos em 'nr_registro': {df_clean.get('nr_registro', df_clean.get('Nº REGISTRO')).isna().sum() if 'nr_registro' in df_clean.columns or 'Nº REGISTRO' in df_clean.columns else 'N/A'}."
        )

        return df_clean

    # ---- LOAD ----

    def load(self, df: pd.DataFrame, lookups: dict) -> str:
        if df.empty:
            return "0 registros"

        # Cultivares precisa preencher DimMantenedor antes do load
        db = lookups["db"]
        lookups["mantenedores"] = preencher_dimensao_mantenedor(db, df)

        df_f = df.copy()
        df_f["id_cultura"] = df_f["cultura"].apply(lambda x: get_cultura_id(x, lookups["culturas"]))
        df_f["id_mantenedor"] = df_f["mantenedor"].map(lookups.get("mantenedores", {}))
        cols = ["nr_registro", "id_cultura", "id_mantenedor", "cultivar", "nome_secundario", "situacao", "nr_formulario", "data_reg", "data_val"]
        df_f = df_f[[c for c in cols if c in df_f.columns]].drop_duplicates(subset=["nr_registro"]).dropna(subset=["cultivar", "id_cultura"])
        upsert_data(FatoCultivar, df_f, index_elements=['nr_registro'])
        result = f"{len(df_f)} registros upserted"
        self.log.info(f"Fato Cultivares: {result}.")
        return result
