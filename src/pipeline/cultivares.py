import pandas as pd
import requests
from pathlib import Path
import numpy as np
import re
from .base_extractor import BaseExtractor

class CultivaresExtractor(BaseExtractor):
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

    # Dicionário de Correção de Acentos (Migrado do Legacy)
    # Mapeamento manual de variantes sem acento para a forma canônica (Dataset 2025-04).
    _AX_CORRECTIONS = {
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

    def __init__(self, use_cache: bool = True, cache_path: str = "data/relatorio_cultivares.csv"):
        super().__init__()
        self.use_cache = use_cache
        self.cache_path = Path(cache_path).resolve()

    def extract(self) -> pd.DataFrame:
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

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        self.log.info(f"Iniciando limpeza do dataset de Cultivares: {len(df)} linha(s) recebida(s).")
        s = df.copy()

        # Regexes de limpeza (Migradas do Legacy)
        _RE_HTML_TAGS = re.compile(r"<[^>]{0,30}>|</\\+>", re.IGNORECASE)
        _RE_ASPAS_ENVOLVENDO = re.compile(r"^['\"](.*)['\"]$")
        _RE_BACKSLASH = re.compile(r"\\+'")

        def _limpar_texto(serie: pd.Series) -> pd.Series:
            tmp = serie.copy().str.strip()
            # Limpeza profunda
            tmp = tmp.str.replace(_RE_ASPAS_ENVOLVENDO, r"\1", regex=True)
            tmp = tmp.str.replace(_RE_HTML_TAGS, "", regex=True)
            tmp = tmp.str.replace(_RE_BACKSLASH, "'", regex=True)
            return tmp.str.strip().replace("", np.nan)

        colunas_texto = ["CULTIVAR", "NOME COMUM", "NOME CIENTÍFICO", "GRUPO DA ESPÉCIE", "SITUAÇÃO", "MANTENEDOR (REQUERENTE) (NOME)"]
        for col in colunas_texto:
            if col in s.columns:
                s[col] = _limpar_texto(s[col])

        # Aplicação da Correção de Acentos
        if "NOME COMUM" in s.columns:
            antes = s["NOME COMUM"].copy()
            s["NOME COMUM"] = s["NOME COMUM"].replace(self._AX_CORRECTIONS)
            corrigidos = (s["NOME COMUM"] != antes).sum()
            if corrigidos:
                self.log.info(f"Correção de acentos: {corrigidos} valor(es) corrigido(s) em 'NOME COMUM'.")

        # Parsing: Extração de nome secundário (split '/')
        # Nota: o regex de aspas acima não remove aspas que envolvem apenas o
        # nome principal (ex: "'BONANZA' / BONA"), pois a string completa não
        # termina com aspas. Reaplicamos após o split em cada fragmento.
        if "CULTIVAR" in s.columns:
            split_c = s["CULTIVAR"].str.split("/", n=1, expand=True)
            s["CULTIVAR"] = (
                split_c[0].str.strip()
                .str.replace(_RE_ASPAS_ENVOLVENDO, r"\1", regex=True)
                .str.strip()
            )
            if split_c.shape[1] > 1:
                s["NOME SECUNDÁRIO"] = (
                    split_c[1].str.strip()
                    .str.replace(_RE_ASPAS_ENVOLVENDO, r"\1", regex=True)
                    .str.strip()
                )
                s["NOME SECUNDÁRIO"] = s["NOME SECUNDÁRIO"].replace("", np.nan)
                n_sec = s["NOME SECUNDÁRIO"].notna().sum()
                self.log.info(f"Parsing nomes secundários: {n_sec} cultivar(es) com nome alternativo (split '/').")
            else:
                s["NOME SECUNDÁRIO"] = pd.NA

        # Normalização: Alinhamento de nomes para cruzamento
        if "NOME COMUM" in s.columns:
            s["CULTURA_NORMALIZADA"] = self.normalize_culture_name(s["NOME COMUM"])
        elif "GRUPO DA ESPÉCIE" in s.columns:
            s["CULTURA_NORMALIZADA"] = self.normalize_culture_name(s["GRUPO DA ESPÉCIE"])

        # Transformação: Tipagem de data (ISO 8601)
        for c in ["DATA DO REGISTRO", "DATA DE VALIDADE DO REGISTRO"]:
            if c in s.columns:
                antes_nulos = s[c].isna().sum()
                s[c] = pd.to_datetime(s[c], dayfirst=True, errors="coerce")
                novos_nulos = s[c].isna().sum() - antes_nulos
                if novos_nulos > 0:
                    self.log.warning(f"Parsing de data '{c}': {novos_nulos} valor(es) não convertido(s) → NaT.")

        if "DATA DO REGISTRO" in s.columns:
            s["ANO"] = s["DATA DO REGISTRO"].dt.year.astype("Int64")

        # Enriquecimento: Classificação de mantenedor (Público/Privado)
        if "MANTENEDOR (REQUERENTE) (NOME)" in s.columns:
            _PUBL = ["EMBRAPA", "UNIVERSIDADE", "INSTITUTO", "EPAGRI", "PESAGRO", "IAPAR", "SECRETARIA", "FACULDADE"]
            def cat_setor(x):
                if pd.isna(x): return "Nulo"
                x_u = x.upper()
                if any(p in x_u for p in _PUBL): return "Público"
                return "Privado"
            s["SETOR"] = s["MANTENEDOR (REQUERENTE) (NOME)"].apply(cat_setor)

        # Transformação: Mapeamento de colunas para banco
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

        s = s.rename(columns=renames)
        antes_dedup = len(s)
        s = s.drop_duplicates()
        descartados = antes_dedup - len(s)
        if descartados:
            self.log.info(f"drop_duplicates: {descartados} linha(s) duplicada(s) removida(s).")

        self.log.info(
            f"Transform concluído: {len(s)} linha(s) resultantes. "
            f"Nulos em 'nr_registro': {s.get('nr_registro', s.get('Nº REGISTRO')).isna().sum() if 'nr_registro' in s.columns or 'Nº REGISTRO' in s.columns else 'N/A'}."
        )

        return s
