import logging
import re
from pathlib import Path

import numpy as np
import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Classificação de setor — centralizada aqui para uso do db_manager e notebook
# ---------------------------------------------------------------------------
# Palavras-chave que identificam entidades públicas brasileiras
_PALAVRAS_PUBLICO = [
    "EMBRAPA", "EPAMIG", "IAC", "UFLA", "UFV", "UFPR", "UFSC", "UFRGS",
    "UFRJ", "UFSM", "UFG", "UFMS", "UFMT", "UNB", "USP", "UNICAMP",
    "UNESP", "FEPAGRO", "IAPAR", "IPA", "ITAL", "CEPLAC", "CENARGEN",
    "CNPGC", "CNPAF", "PESAGRO", "EPAGRI", "EMPAER", "EMATER",
    "EMPRESA BRASILEIRA", "EMPRESA ESTADUAL", "EMPRESA PERNAMBUCANA",
    "UNIVERSIDADE FEDERAL", "UNIVERSIDADE ESTADUAL", "INSTITUTO FEDERAL",
    "INSTITUTO AGRONÔMICO", "INSTITUTO DE ZOOTECNIA", "GOVERNO",
    "SEC. ", "SECRETARIA", "PREFEITURA", "MINISTÉRIO",
    "CIA. ", "COMPANHIA", "COOPERATIVA",  # cooperativas tratadas como público
]

# Termos que indicam origem estrangeira
_PALAVRAS_ESTRANGEIRO = [
    "BAYER", "BASF", "SYNGENTA", "PIONEER", "MONSANTO", "CORTEVA",
    "LIMAGRAIN", "SAKATA", "TAKII", "ENZA ZADEN", "RIJK ZWAAN",
    "KWS", "DSV", "EURALIS", "SEMINIS",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

CSV_LOCAL  = Path(__file__).parent / "relatorio_cultivares.csv"
SNPC_URL   = "https://sistemas.agricultura.gov.br/snpc/cultivarweb/cultivares_registradas.php"
SNPC_QUERY = {"acao": "pesquisar", "postado": "1"}
SNPC_DATA  = {"exportar": "csv"}
HEADERS    = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": (
        "https://sistemas.agricultura.gov.br/snpc/cultivarweb/"
        "cultivares_registradas.php?acao=pesquisar&postado=1"
    ),
    "Accept": "text/csv,text/html,*/*;q=0.9",
}

COL_CULTIVAR   = "CULTIVAR"
COL_NOME_COM   = "NOME COMUM"
COL_NOME_CIEN  = "NOME CIENTÍFICO"
COL_GRUPO      = "GRUPO DA ESPÉCIE"
COL_SITUACAO   = "SITUAÇÃO"
COL_FORMULARIO = "Nº FORMULÁRIO"
COL_REGISTRO   = "Nº REGISTRO"
COL_DATA_REG   = "DATA DO REGISTRO"
COL_DATA_VAL   = "DATA DE VALIDADE DO REGISTRO"
COL_MANTENEDOR = "MANTENEDOR (REQUERENTE) (NOME)"
COL_SETOR      = "SETOR"


def _download_csv(destino: Path, timeout: int = 180) -> None:
    log.info("⬇  Baixando CSV do SNPC/MAPA…")
    try:
        resp = requests.post(
            SNPC_URL,
            params=SNPC_QUERY,
            data=SNPC_DATA,
            headers=HEADERS,
            timeout=timeout,
            stream=True,
        )
        resp.raise_for_status()

        content_type = resp.headers.get("Content-Type", "")
        if "csv" not in content_type and "text" not in content_type:
            raise ValueError(
                f"Resposta inesperada do servidor (Content-Type: {content_type}). "
                "O site pode ter mudado de estrutura."
            )

        destino.write_bytes(resp.content)
        size_kb = destino.stat().st_size / 1024
        log.info("✅ CSV salvo em '%s' (%.1f KB)", destino.name, size_kb)

    except requests.exceptions.Timeout:
        raise TimeoutError(f"O download excedeu {timeout}s. Verifique sua conexão.")
    except requests.exceptions.RequestException as exc:
        raise RuntimeError(f"Falha ao baixar o CSV: {exc}") from exc


def _ler_bruto(caminho: Path) -> pd.DataFrame:
    return pd.read_csv(
        caminho,
        dtype=str,
        encoding="utf-8",
        skipinitialspace=True,
        na_values=["", "NA", "N/A", "nan", "NaN", "NULL", "null", "-", "--"],
        keep_default_na=True,
    )


_RE_ASPAS_SIMPLES_ENVOLVENDO = re.compile(r"^'(.*)'$")
_RE_ASPAS_DUPLAS_ENVOLVENDO  = re.compile(r'^"(.*)"$')
_RE_HTML_TAGS                = re.compile(r"<[^>]{0,30}>|</\\+>", re.IGNORECASE)
_RE_BACKSLASH                = re.compile(r"\\+'")

# ---------------------------------------------------------------------------
# Correção de acentuação — NOME COMUM
# Mapeamento manual de variantes sem acento (ou acento errado) para a forma
# canônica. Gerado por varredura do dataset em 2025-04.
# Regra de ouro: mantém a forma com mais registros no dataset original.
# ---------------------------------------------------------------------------
_CORR_NOME_COM: dict[str, str] = {
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
    "OrquÍdea":   "Orquídea",   # typo com Í maiúsculo no meio
    "Peperomia":  "Peperômia",
    "Pera":       "Pêra",
}


def _limpar_coluna_texto(serie: pd.Series) -> pd.Series:
    s = serie.copy()
    s = s.str.strip()
    s = s.str.replace(_RE_ASPAS_SIMPLES_ENVOLVENDO, r"\1", regex=True)
    s = s.str.replace(_RE_ASPAS_DUPLAS_ENVOLVENDO,  r"\1", regex=True)
    s = s.str.replace(_RE_HTML_TAGS, "", regex=True).str.strip()
    s = s.str.replace(_RE_BACKSLASH, "'", regex=True)
    s = s.replace("", np.nan)
    return s


def _corrigir_acentos(df: pd.DataFrame) -> pd.DataFrame:
    """Substitui variantes com acento incorreto/ausente pela forma canônica."""
    if COL_NOME_COM in df.columns:
        n_before = df[COL_NOME_COM].nunique()
        df[COL_NOME_COM] = df[COL_NOME_COM].replace(_CORR_NOME_COM)
        n_after = df[COL_NOME_COM].nunique()
        corrigidos = n_before - n_after
        if corrigidos:
            log.info(
                "✏️  Acentuação corrigida em '%s': %d variante(s) unificada(s).",
                COL_NOME_COM, corrigidos,
            )
    return df


def _limpar(df: pd.DataFrame) -> pd.DataFrame:
    log.info("🧹 Iniciando limpeza dos dados…")

    colunas_texto = [COL_CULTIVAR, COL_NOME_COM, COL_NOME_CIEN, COL_GRUPO, COL_SITUACAO, COL_MANTENEDOR]
    for col in colunas_texto:
        if col in df.columns:
            df[col] = _limpar_coluna_texto(df[col])

    df = _corrigir_acentos(df)

    if COL_FORMULARIO in df.columns:
        df[COL_FORMULARIO] = (
            df[COL_FORMULARIO]
            .str.strip()
            .replace("", np.nan)
            .pipe(pd.to_numeric, errors="coerce")
            .astype("Int64")
        )

    if COL_REGISTRO in df.columns:
        df[COL_REGISTRO] = pd.to_numeric(df[COL_REGISTRO], errors="coerce").astype("Int64")

    for col_data in [COL_DATA_REG, COL_DATA_VAL]:
        if col_data in df.columns:
            df[col_data] = pd.to_datetime(
                df[col_data].str.strip(),
                dayfirst=True,
                errors="coerce",
            )

    if COL_DATA_REG in df.columns:
        df["ANO"] = df[COL_DATA_REG].dt.year.astype("Int64")

    if COL_GRUPO in df.columns:
        df[COL_GRUPO] = df[COL_GRUPO].str.strip().str.upper()

    antes = len(df)
    df = df.drop_duplicates()
    removidas = antes - len(df)
    if removidas:
        log.warning("⚠️  %d linha(s) duplicada(s) removida(s).", removidas)

    log.info("✅ Limpeza concluída — %d registros | %d colunas", df.shape[0], df.shape[1])
    return df.reset_index(drop=True)


def classificar_setor(nome: str | None) -> str:
    """
    Classifica um mantenedor em Público, Privado ou Misto com base em
    palavras-chave no nome.

    Returns:
        "Público"  — entidade pública (governo, universidade, empresa pública)
        "Privado"  — empresa privada ou pessoa física
        "Misto"    — quando 'nome' contém termos de ambos os grupos
        "Nulo"     — quando nome é None / NaN
    """
    if not nome or pd.isna(nome):
        return "Nulo"
    upper = nome.upper()
    eh_publico = any(p in upper for p in _PALAVRAS_PUBLICO)
    # Privado: não é público e não está na lista de estrangeiros conhecidos
    # (estrangeiros ainda são Privado — a coluna 'origem' em dim_mantenedor
    #  é reservada para diferenciação futura)
    if eh_publico:
        return "Público"
    return "Privado"


def _adicionar_setor(df: pd.DataFrame) -> pd.DataFrame:
    """Adiciona coluna SETOR ao DataFrame usando classificar_setor()."""
    if COL_MANTENEDOR in df.columns:
        df[COL_SETOR] = df[COL_MANTENEDOR].apply(classificar_setor)
        n_pub = (df[COL_SETOR] == "Público").sum()
        n_priv = (df[COL_SETOR] == "Privado").sum()
        n_nulo = (df[COL_SETOR] == "Nulo").sum()
        log.info(
            "🏷️  Setor classificado — Público: %d | Privado: %d | Nulo: %d",
            n_pub, n_priv, n_nulo,
        )
    return df


def relatorio_qualidade(df: pd.DataFrame) -> None:
    print("\n" + "=" * 60)
    print("QUALIDADE DOS DADOS PÓS-LIMPEZA")
    print("=" * 60)
    print(f"📦 Registros : {len(df):,}")
    print(f"📋 Colunas   : {df.shape[1]}")

    nulos = df.isnull().sum()
    nulos = nulos[nulos > 0].sort_values(ascending=False)
    if nulos.empty:
        print("✅ Nenhum valor nulo encontrado!")
    else:
        print("\nColunas com valores ausentes:")
        tabela = pd.DataFrame({
            "Total Nulos": nulos,
            "% Nulos": (nulos / len(df) * 100).round(2),
        })
        print(tabela.to_string())
    print("=" * 60 + "\n")


def carregar_dados(
    caminho: Path | str | None = None,
    forçar_download: bool = False,
    imprimir_qualidade: bool = True,
) -> pd.DataFrame:
    destino = Path(caminho) if caminho else CSV_LOCAL

    if forçar_download or not destino.exists():
        _download_csv(destino)
    else:
        log.info("📂 Arquivo local encontrado: '%s' — pulando download.", destino.name)

    df = _ler_bruto(destino)
    log.info("📥 Leitura bruta: %d registros | %d colunas", *df.shape)

    df = _limpar(df)
    df = _adicionar_setor(df)  # ← coluna SETOR adicionada aqui

    if imprimir_qualidade:
        relatorio_qualidade(df)

    return df


if __name__ == "__main__":
    df = carregar_dados()
    print(df.head())
    print("\nDtypes:")
    print(df.dtypes)
