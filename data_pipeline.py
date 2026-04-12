"""
Pipeline de Dados — Registro Nacional de Cultivares (RNC)
==========================================================
Responsabilidades:
  1. Baixar automaticamente o CSV do site do MAPA/SNPC se não existir localmente.
  2. Aplicar limpeza sistemática nos dados brutos.

Uso:
    from data_pipeline import carregar_dados
    df = carregar_dados()           # usa cache local se existir
    df = carregar_dados(forçar=True) # força novo download
"""

import io
import logging
import re
from pathlib import Path

import numpy as np
import pandas as pd
import requests

# ── Configuração ─────────────────────────────────────────────────────────────

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

# Nomes canônicos das colunas após renomeação
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


# ── 1. Download ───────────────────────────────────────────────────────────────

def _download_csv(destino: Path, timeout: int = 180) -> None:
    """Faz o POST no site do SNPC e salva o CSV em *destino*."""
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
        raise TimeoutError(
            f"O download excedeu {timeout}s. Verifique sua conexão."
        )
    except requests.exceptions.RequestException as exc:
        raise RuntimeError(f"Falha ao baixar o CSV: {exc}") from exc


# ── 2. Leitura bruta ─────────────────────────────────────────────────────────

def _ler_bruto(caminho: Path) -> pd.DataFrame:
    """Lê o CSV com opções robustas e retorna DataFrame sem limpeza."""
    df = pd.read_csv(
        caminho,
        dtype=str,          # tudo como string para inspecionar antes de converter
        encoding="utf-8",
        skipinitialspace=True,
        na_values=["", "NA", "N/A", "nan", "NaN", "NULL", "null", "-", "--"],
        keep_default_na=True,
    )
    return df


# ── 3. Limpeza ────────────────────────────────────────────────────────────────

# Padrão para aspas simples desnecessárias: circundando o valor inteiro
_RE_ASPAS_SIMPLES_ENVOLVENDO = re.compile(r"^'(.*)'$")
# Padrão para aspas duplas desnecessárias: circundando o valor inteiro
_RE_ASPAS_DUPLAS_ENVOLVENDO  = re.compile(r'^"(.*)"$')
# Tags HTML residuais (e.g. </I>, <i/>, </\\>)
_RE_HTML_TAGS = re.compile(r"<[^>]{0,30}>|</\\+>", re.IGNORECASE)
# Barras invertidas escapadas (artefato de exportação)
_RE_BACKSLASH = re.compile(r"\\+'")


def _limpar_coluna_texto(serie: pd.Series) -> pd.Series:
    """
    Aplica todas as limpezas de texto a uma Series string:
      - strip de espaços em branco
      - remoção de aspas simples/duplas que circundam o valor inteiro
      - remoção de tags HTML residuais
      - normalização de barras invertidas antes de aspas (\\'  → ' )
    """
    s = serie.copy()

    # 1. Strip
    s = s.str.strip()

    # 2. Aspas simples envolvendo o valor todo → remover
    s = s.str.replace(_RE_ASPAS_SIMPLES_ENVOLVENDO, r"\1", regex=True)

    # 3. Aspas duplas envolvendo o valor todo → remover
    s = s.str.replace(_RE_ASPAS_DUPLAS_ENVOLVENDO, r"\1", regex=True)

    # 4. Tags HTML residuais
    s = s.str.replace(_RE_HTML_TAGS, "", regex=True).str.strip()

    # 5. Sequência \\' (artefato de escape do exportador) → '
    s = s.str.replace(_RE_BACKSLASH, "'", regex=True)

    # 6. Valores que viraram string vazia após limpeza → NaN genuíno
    s = s.replace("", np.nan)

    return s


def _limpar(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica toda a bateria de limpeza e conversão de tipos."""
    log.info("🧹 Iniciando limpeza dos dados…")

    # ── 3a. Limpar colunas de texto ──────────────────────────────────────────
    colunas_texto = [
        COL_CULTIVAR, COL_NOME_COM, COL_NOME_CIEN,
        COL_GRUPO, COL_SITUACAO, COL_MANTENEDOR,
    ]
    for col in colunas_texto:
        if col in df.columns:
            df[col] = _limpar_coluna_texto(df[col])

    # ── 3b. Converter Nº FORMULÁRIO para Int64 (aceita NaN) ─────────────────
    if COL_FORMULARIO in df.columns:
        df[COL_FORMULARIO] = (
            df[COL_FORMULARIO]
            .str.strip()
            .replace("", np.nan)
            .pipe(pd.to_numeric, errors="coerce")
            .astype("Int64")   # Int64 nullable (suporta NaN)
        )

    # ── 3c. Converter Nº REGISTRO para int (sem nulos esperados) ────────────
    if COL_REGISTRO in df.columns:
        df[COL_REGISTRO] = pd.to_numeric(df[COL_REGISTRO], errors="coerce").astype("Int64")

    # ── 3d. Converter datas ──────────────────────────────────────────────────
    for col_data in [COL_DATA_REG, COL_DATA_VAL]:
        if col_data in df.columns:
            df[col_data] = pd.to_datetime(
                df[col_data].str.strip(),
                dayfirst=True,
                errors="coerce",
            )

    # ── 3e. Coluna auxiliar ANO ──────────────────────────────────────────────
    if COL_DATA_REG in df.columns:
        df["ANO"] = df[COL_DATA_REG].dt.year.astype("Int64")

    # ── 3f. Normalizar GRUPO DA ESPÉCIE: strip + title-case consistente ──────
    if COL_GRUPO in df.columns:
        df[COL_GRUPO] = df[COL_GRUPO].str.strip().str.upper()

    # ── 3g. Remover duplicatas exatas ────────────────────────────────────────
    antes = len(df)
    df = df.drop_duplicates()
    removidas = antes - len(df)
    if removidas:
        log.warning("⚠️  %d linha(s) duplicada(s) removida(s).", removidas)

    log.info("✅ Limpeza concluída — %d registros | %d colunas", df.shape[0], df.shape[1])
    return df.reset_index(drop=True)


# ── 4. Relatório de qualidade ─────────────────────────────────────────────────

def relatorio_qualidade(df: pd.DataFrame) -> None:
    """Imprime um resumo dos valores nulos e colunas após limpeza."""
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


# ── 5. Ponto de entrada público ───────────────────────────────────────────────

def carregar_dados(
    caminho: Path | str | None = None,
    forçar_download: bool = False,
    imprimir_qualidade: bool = True,
) -> pd.DataFrame:
    """
    Carrega e retorna o DataFrame limpo do RNC.

    Parâmetros
    ----------
    caminho : str ou Path, opcional
        Caminho do CSV local. Padrão: ``relatorio_cultivares.csv`` na mesma pasta.
    forçar_download : bool
        Se True, baixa novamente mesmo que o arquivo já exista.
    imprimir_qualidade : bool
        Se True, imprime o relatório de qualidade após limpeza.

    Retorno
    -------
    pd.DataFrame limpo e tipado.
    """
    destino = Path(caminho) if caminho else CSV_LOCAL

    if forçar_download or not destino.exists():
        _download_csv(destino)
    else:
        log.info("📂 Arquivo local encontrado: '%s' — pulando download.", destino.name)

    df = _ler_bruto(destino)
    log.info("📥 Leitura bruta: %d registros | %d colunas", *df.shape)

    df = _limpar(df)

    if imprimir_qualidade:
        relatorio_qualidade(df)

    return df


# ── Execução direta (teste/diagnóstico) ──────────────────────────────────────

if __name__ == "__main__":
    df = carregar_dados()
    print(df.head())
    print("\nDtypes:")
    print(df.dtypes)
