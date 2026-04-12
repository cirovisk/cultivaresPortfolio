"""
db_manager.py — Camada DuckDB para o pipeline RNC Cultivares
============================================================
Responsabilidades:
  • Criar / abrir cultivares.ddb com star schema (PORTFOLIO_SUGESTOES.md)
  • Fazer upsert incremental idempotente a partir de um DataFrame pandas
  • Verificar anualmente se o CSV precisa ser re-baixado (sync_anual)
  • Expor get_conn() para uso no notebook

Star Schema:
  dim_especie ─┐
  dim_mantenedor ─┤── fato_registro
  dim_tempo ──────┘

Uso rápido:
  python db_manager.py          # popula / atualiza o banco inteiro
  python db_manager.py --sync   # também verifica re-download anual
"""

import json
import logging
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd

from data_pipeline import (
    COL_CULTIVAR,
    COL_DATA_REG,
    COL_DATA_VAL,
    COL_FORMULARIO,
    COL_GRUPO,
    COL_MANTENEDOR,
    COL_NOME_CIEN,
    COL_NOME_COM,
    COL_REGISTRO,
    COL_SITUACAO,
    carregar_dados,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Caminhos
# ---------------------------------------------------------------------------
_BASE       = Path(__file__).parent
DB_PATH     = _BASE / "cultivares.ddb"
SYNC_STATE  = _BASE / "sync_state.json"

# ---------------------------------------------------------------------------
# DDL — Star Schema
# ---------------------------------------------------------------------------
_DDL = """
-- Dimensão Espécie
CREATE SEQUENCE IF NOT EXISTS seq_especie START 1;
CREATE TABLE IF NOT EXISTS dim_especie (
    id_especie      INTEGER PRIMARY KEY DEFAULT nextval('seq_especie'),
    nome_comum      TEXT    NOT NULL,
    nome_cientifico TEXT    NOT NULL,
    grupo           TEXT    NOT NULL,
    UNIQUE (nome_comum, nome_cientifico, grupo)
);

-- Dimensão Mantenedor
CREATE SEQUENCE IF NOT EXISTS seq_mantenedor START 1;
CREATE TABLE IF NOT EXISTS dim_mantenedor (
    id_mantenedor INTEGER PRIMARY KEY DEFAULT nextval('seq_mantenedor'),
    nome          TEXT UNIQUE,
    setor         TEXT,   -- Público / Privado / Misto / Nulo
    origem        TEXT    -- Nacional / Estrangeiro / Nulo (reservado)
);

-- Dimensão Tempo (surrogate key = YYYYMMDD)
CREATE TABLE IF NOT EXISTS dim_tempo (
    id_tempo  INTEGER PRIMARY KEY,  -- YYYYMMDD
    data      DATE,
    ano       INTEGER,
    trimestre INTEGER,
    decada    INTEGER
);

-- Fato Registro — PK natural = nr_registro
CREATE TABLE IF NOT EXISTS fato_registro (
    nr_registro   INTEGER PRIMARY KEY,
    id_especie    INTEGER REFERENCES dim_especie(id_especie),
    id_mantenedor INTEGER REFERENCES dim_mantenedor(id_mantenedor),
    id_data_reg   INTEGER REFERENCES dim_tempo(id_tempo),
    id_data_val   INTEGER REFERENCES dim_tempo(id_tempo),
    nr_formulario BIGINT,
    situacao      TEXT,
    duracao_anos  DOUBLE,
    expirado      BOOLEAN,
    cultivar      TEXT,
    atualizado_em TIMESTAMP DEFAULT now()
);
"""


# ---------------------------------------------------------------------------
# Conexão
# ---------------------------------------------------------------------------
def get_conn(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Retorna conexão ao banco DuckDB persistido em disco."""
    conn = duckdb.connect(str(DB_PATH), read_only=read_only)
    return conn


def _init_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Executa DDL idempotente (IF NOT EXISTS em todos os objetos)."""
    conn.execute(_DDL)
    log.info("✅ Schema DuckDB verificado / criado em '%s'", DB_PATH.name)


# ---------------------------------------------------------------------------
# Helpers de dimensão
# ---------------------------------------------------------------------------
def _upsert_dim_especie(conn: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> None:
    """Insere espécies únicas; ignora duplicatas pela chave natural."""
    especies = (
        df[[COL_NOME_COM, COL_NOME_CIEN, COL_GRUPO]]
        .dropna(subset=[COL_NOME_COM, COL_NOME_CIEN, COL_GRUPO])
        .drop_duplicates()
        .rename(columns={
            COL_NOME_COM:  "nome_comum",
            COL_NOME_CIEN: "nome_cientifico",
            COL_GRUPO:     "grupo",
        })
    )
    conn.register("_tmp_especies", especies)
    conn.execute("""
        INSERT INTO dim_especie (nome_comum, nome_cientifico, grupo)
        SELECT nome_comum, nome_cientifico, grupo FROM _tmp_especies
        ON CONFLICT (nome_comum, nome_cientifico, grupo) DO NOTHING
    """)
    conn.unregister("_tmp_especies")
    log.info("🌿 dim_especie: %d linhas únicas processadas", len(especies))


def _upsert_dim_mantenedor(conn: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> None:
    """Insere mantenedores únicos com setor classificado."""
    col_setor = "SETOR"
    cols = [COL_MANTENEDOR]
    if col_setor in df.columns:
        cols.append(col_setor)

    mants = (
        df[cols]
        .dropna(subset=[COL_MANTENEDOR])   # ← filtra nulos: NULL != NULL no SQL
        .drop_duplicates(subset=[COL_MANTENEDOR])
    )
    mants = mants.rename(columns={COL_MANTENEDOR: "nome", col_setor: "setor"} if col_setor in df.columns
                         else {COL_MANTENEDOR: "nome"})
    if "setor" not in mants.columns:
        mants["setor"] = None

    conn.register("_tmp_mants", mants)
    conn.execute("""
        INSERT INTO dim_mantenedor (nome, setor)
        SELECT nome, setor FROM _tmp_mants
        ON CONFLICT (nome) DO UPDATE SET setor = EXCLUDED.setor
    """)
    conn.unregister("_tmp_mants")
    log.info("🏢 dim_mantenedor: %d linhas únicas processadas", len(mants))



def _upsert_dim_tempo(conn: duckdb.DuckDBPyConnection, datas: pd.Series) -> None:
    """Insere datas únicas na dimensão tempo (surrogate key YYYYMMDD)."""
    datas_validas = datas.dropna().drop_duplicates()
    if datas_validas.empty:
        return
    tempo = pd.DataFrame({
        "id_tempo":  datas_validas.dt.strftime("%Y%m%d").astype(int),
        "data":      datas_validas.dt.date,
        "ano":       datas_validas.dt.year,
        "trimestre": datas_validas.dt.quarter,
        "decada":    (datas_validas.dt.year // 10 * 10),
    }).drop_duplicates(subset=["id_tempo"])
    conn.register("_tmp_tempo", tempo)
    conn.execute("""
        INSERT INTO dim_tempo
        SELECT id_tempo, data, ano, trimestre, decada FROM _tmp_tempo
        ON CONFLICT (id_tempo) DO NOTHING
    """)
    conn.unregister("_tmp_tempo")
    log.info("📅 dim_tempo: %d datas únicas processadas", len(tempo))


# ---------------------------------------------------------------------------
# Upsert principal: fato_registro
# ---------------------------------------------------------------------------
def upsert_from_df(df: pd.DataFrame, conn: duckdb.DuckDBPyConnection | None = None) -> None:
    """
    Insere ou atualiza fato_registro a partir de um DataFrame pandas limpo.
    Idempotente: pode ser chamado múltiplas vezes com os mesmos dados.
    """
    _own_conn = conn is None
    if _own_conn:
        conn = get_conn()

    try:
        _init_schema(conn)

        # --- Dimensões ---
        _upsert_dim_especie(conn, df)
        _upsert_dim_mantenedor(conn, df)

        todas_datas = pd.concat([
            df[COL_DATA_REG].dropna(),
            df[COL_DATA_VAL].dropna(),
        ]).drop_duplicates()
        _upsert_dim_tempo(conn, todas_datas)

        # --- Fato ---
        hoje = date.today()
        fato = df.copy()

        # Calcular duração e expirado
        fato["duracao_anos"] = (
            (fato[COL_DATA_VAL] - fato[COL_DATA_REG]).dt.days / 365.25
        ).round(4)
        fato["expirado"] = fato[COL_DATA_VAL].apply(
            lambda d: d.date() < hoje if pd.notna(d) else None
        )

        # Registrar DataFrame como view temporária e fazer JOIN com dims
        fato_raw = fato.rename(columns={
            COL_REGISTRO:   "nr_registro",
            COL_NOME_COM:   "nome_comum",
            COL_NOME_CIEN:  "nome_cientifico",
            COL_GRUPO:      "grupo",
            COL_MANTENEDOR: "mantenedor",
            COL_DATA_REG:   "data_reg",
            COL_DATA_VAL:   "data_val",
            COL_FORMULARIO: "nr_formulario",
            COL_SITUACAO:   "situacao",
            COL_CULTIVAR:   "cultivar",
        })
        conn.register("_tmp_fato_raw", fato_raw)
        conn.execute("""
            INSERT OR REPLACE INTO fato_registro
                (nr_registro, id_especie, id_mantenedor,
                 id_data_reg, id_data_val,
                 nr_formulario, situacao, duracao_anos, expirado,
                 cultivar, atualizado_em)
            SELECT
                CAST(f.nr_registro   AS INTEGER)  AS nr_registro,
                e.id_especie,
                m.id_mantenedor,
                TRY_CAST(strftime(f.data_reg::TIMESTAMP, '%Y%m%d') AS INTEGER) AS id_data_reg,
                TRY_CAST(strftime(f.data_val::TIMESTAMP, '%Y%m%d') AS INTEGER) AS id_data_val,
                CAST(f.nr_formulario AS BIGINT)   AS nr_formulario,
                f.situacao,
                f.duracao_anos,
                f.expirado,
                f.cultivar,
                now()
            FROM _tmp_fato_raw f
            LEFT JOIN dim_especie    e
                ON  e.nome_comum      = f.nome_comum
                AND e.nome_cientifico = f.nome_cientifico
                AND e.grupo           = f.grupo
            LEFT JOIN dim_mantenedor m
                ON m.nome = f.mantenedor
            WHERE f.nr_registro IS NOT NULL
        """)
        conn.unregister("_tmp_fato_raw")

        n_fato = conn.execute("SELECT COUNT(*) FROM fato_registro").fetchone()[0]
        log.info("⭐ fato_registro: %d registros totais no banco", n_fato)

    finally:
        if _own_conn:
            conn.close()


# ---------------------------------------------------------------------------
# Sync anual
# ---------------------------------------------------------------------------
def _ler_sync_state() -> dict:
    if SYNC_STATE.exists():
        return json.loads(SYNC_STATE.read_text())
    return {"ultimo_download": None}


def _salvar_sync_state(state: dict) -> None:
    SYNC_STATE.write_text(json.dumps(state, indent=2))


def sync_anual(forçar: bool = False) -> None:
    """
    Verifica se o CSV foi baixado neste ano. Se não, faz download + upsert.
    Idempotente: chamadas múltiplas no mesmo ano não re-baixam.

    Args:
        forçar: Se True, sempre re-baixa independente do estado salvo.
    """
    ano_atual = date.today().year
    state = _ler_sync_state()
    ultimo = state.get("ultimo_download")

    if not forçar and ultimo == ano_atual:
        log.info("🔄 sync_anual: banco já sincronizado em %d — pulando.", ano_atual)
        return

    log.info("🌐 sync_anual: iniciando re-download para o ano %d…", ano_atual)
    df = carregar_dados(forçar_download=True, imprimir_qualidade=False)
    upsert_from_df(df)

    state["ultimo_download"] = ano_atual
    _salvar_sync_state(state)
    log.info("✅ sync_anual: banco atualizado e estado salvo (%d).", ano_atual)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys

    modo_sync = "--sync" in sys.argv

    if modo_sync:
        log.info("🦆 Modo: sync anual (--sync)")
        sync_anual()
    else:
        log.info("🦆 Modo: carregar + upsert (sem re-download forçado)")
        df = carregar_dados(imprimir_qualidade=False)
        conn = get_conn()
        try:
            upsert_from_df(df, conn=conn)
            # Verificação rápida
            print("\n--- Verificação ---")
            for tabela in ["dim_especie", "dim_mantenedor", "dim_tempo", "fato_registro"]:
                n = conn.execute(f"SELECT COUNT(*) FROM {tabela}").fetchone()[0]
                print(f"  {tabela:<20}: {n:>7,} linhas")
        finally:
            conn.close()
