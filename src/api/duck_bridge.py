"""
DuckDB Bridge: Motor de consulta federada (Postgres ↔ Parquet).

Permite JOINs transparentes entre tabelas PostgreSQL (metadados dimensionais)
e arquivos Parquet (dados massivos de indicação ZARC e risco climático).

Arquitetura:
    FastAPI → duck_bridge → DuckDB ──┬── PostgreSQL (dim_municipio, dim_cultura)
                                     └── Parquet    (zarc_indicacoes, zarc_risco)
"""
import duckdb
import os
import logging
from pathlib import Path
from contextlib import contextmanager

log = logging.getLogger(__name__)

# Configuração de caminhos
PARQUET_BASE = Path(os.getenv("PARQUET_DIR", "/app/data/parquet"))
ZARC_INDICACOES_PATH = PARQUET_BASE / "zarc_indicacoes"
ZARC_RISCO_PATH = PARQUET_BASE / "zarc_risco"

# DICA DE ESCALA:
# O uso de glob ('**/*.parquet') permite que novos arquivos adicionados por 
# outras culturas ou safras sejam integrados automaticamente sem mudar o código.


# Credenciais Postgres
PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_USER = os.getenv("POSTGRES_USER", "api_reader")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "api_reader_pass")
PG_DB = os.getenv("POSTGRES_DB", "cultivares_db")


def _get_connection() -> duckdb.DuckDBPyConnection:
    """Cria conexão DuckDB com extensão Postgres carregada."""
    con = duckdb.connect()
    con.execute("INSTALL postgres;")
    con.execute("LOAD postgres;")
    con.execute(f"ATTACH 'dbname={PG_DB} host={PG_HOST} port={PG_PORT} user={PG_USER} password={PG_PASS}' AS pg (TYPE POSTGRES, READ_ONLY);")
    return con


@contextmanager
def get_duck_connection():
    con = _get_connection()
    try:
        yield con
    finally:
        con.close()


# ==========================================
# 1. INDICAÇÕES DE CULTIVARES (Dataset 196M)
# ==========================================

def query_indicacoes(uf: str = None, cultura: str = None, safra: str = None, 
                     limit: int = 100, offset: int = 0) -> list[dict]:
    parquet_glob = f"{ZARC_INDICACOES_PATH}/**/*.parquet"
    if not any(ZARC_INDICACOES_PATH.rglob("*.parquet")):
        return []
    
    conditions = []
    params = []
    if uf:
        conditions.append("UF = $1")
        params.append(uf.upper())
    if cultura:
        conditions.append(f"LOWER(cultura) LIKE '%' || LOWER(${len(params)+1}) || '%'")
        params.append(cultura)
    if safra:
        conditions.append(f"safra = ${len(params)+1}")
        params.append(safra)
    
    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    con = duckdb.connect()
    try:
        result = con.execute(f"""
            SELECT safra, cultura, obtentor_mantenedor, cultivar, UF as uf, grupo, regiao_adaptacao
            FROM read_parquet('{parquet_glob}', hive_partitioning=true)
            {where_clause}
            ORDER BY safra DESC, cultura, cultivar
            LIMIT {limit} OFFSET {offset}
        """, params).fetchdf()
        return result.to_dict(orient="records")
    finally:
        con.close()


def count_indicacoes(uf: str = None, cultura: str = None, safra: str = None) -> int:
    parquet_glob = f"{ZARC_INDICACOES_PATH}/**/*.parquet"
    if not any(ZARC_INDICACOES_PATH.rglob("*.parquet")):
        return 0
    conditions = []
    params = []
    if uf:
        conditions.append("UF = $1")
        params.append(uf.upper())
    if cultura:
        conditions.append(f"LOWER(cultura) LIKE '%' || LOWER(${len(params)+1}) || '%'")
        params.append(cultura)
    if safra:
        conditions.append(f"safra = ${len(params)+1}")
        params.append(safra)
    
    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    con = duckdb.connect()
    try:
        return con.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_glob}', hive_partitioning=true) {where_clause}", params).fetchone()[0]
    finally:
        con.close()


def get_indicacoes_stats() -> dict:
    parquet_glob = f"{ZARC_INDICACOES_PATH}/**/*.parquet"
    if not any(ZARC_INDICACOES_PATH.rglob("*.parquet")):
        return {"status": "sem_dados"}
    con = duckdb.connect()
    try:
        return con.execute(f"""
            SELECT COUNT(*) as total_registros, COUNT(DISTINCT UF) as total_ufs, 
                   COUNT(DISTINCT cultura) as total_culturas, COUNT(DISTINCT cultivar) as total_cultivares
            FROM read_parquet('{parquet_glob}', hive_partitioning=true)
        """).fetchdf().to_dict(orient="records")[0]
    finally:
        con.close()


def get_indicacoes_por_uf() -> list[dict]:
    parquet_glob = f"{ZARC_INDICACOES_PATH}/**/*.parquet"
    if not any(ZARC_INDICACOES_PATH.rglob("*.parquet")):
        return []
    con = duckdb.connect()
    try:
        return con.execute(f"""
            SELECT UF as uf, COUNT(*) as total_indicacoes
            FROM read_parquet('{parquet_glob}', hive_partitioning=true)
            GROUP BY UF ORDER BY total_indicacoes DESC
        """).fetchdf().to_dict(orient="records")
    finally:
        con.close()


# ==========================================
# 2. RISCO CLIMÁTICO (Dataset 1M)
# ==========================================

def query_risco(codigo_ibge: str = None, cultura: str = None, id_solo: int = None,
                limit: int = 100, offset: int = 0) -> list[dict]:
    parquet_glob = f"{ZARC_RISCO_PATH}/**/*.parquet"
    if not any(ZARC_RISCO_PATH.rglob("*.parquet")):
        return []
    
    conditions = []
    params = []
    if codigo_ibge:
        conditions.append("codigo_ibge = $1")
        params.append(codigo_ibge)
    if cultura:
        conditions.append(f"LOWER(cultura) LIKE '%' || LOWER(${len(params)+1}) || '%'")
        params.append(cultura)
    if id_solo:
        conditions.append("id_solo = $1")
        params.append(id_solo)
    
    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    con = duckdb.connect()
    try:
        result = con.execute(f"""
            SELECT cultura, ano_inicio, ano_fim, codigo_ibge, UF as uf, municipio, id_solo,
                   dec1, dec2, dec3, dec4, dec5, dec6, dec7, dec8, dec9, dec10,
                   dec11, dec12, dec13, dec14, dec15, dec16, dec17, dec18, dec19, dec20,
                   dec21, dec22, dec23, dec24, dec25, dec26, dec27, dec28, dec29, dec30,
                   dec31, dec32, dec33, dec34, dec35, dec36
            FROM read_parquet('{parquet_glob}', hive_partitioning=true)
            {where_clause}
            ORDER BY cultura, codigo_ibge, id_solo
            LIMIT {limit} OFFSET {offset}
        """, params).fetchdf()
        return result.to_dict(orient="records")
    finally:
        con.close()


def count_risco(codigo_ibge: str = None, cultura: str = None, id_solo: int = None) -> int:
    parquet_glob = f"{ZARC_RISCO_PATH}/**/*.parquet"
    if not any(ZARC_RISCO_PATH.rglob("*.parquet")):
        return 0
    conditions = []
    params = []
    if codigo_ibge:
        conditions.append("codigo_ibge = $1")
        params.append(codigo_ibge)
    if cultura:
        conditions.append(f"LOWER(cultura) LIKE '%' || LOWER(${len(params)+1}) || '%'")
        params.append(cultura)
    if id_solo:
        conditions.append("id_solo = $1")
        params.append(id_solo)
    
    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    con = duckdb.connect()
    try:
        return con.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_glob}', hive_partitioning=true) {where_clause}", params).fetchone()[0]
    finally:
        con.close()


def get_risco_stats() -> dict:
    parquet_glob = f"{ZARC_RISCO_PATH}/**/*.parquet"
    if not any(ZARC_RISCO_PATH.rglob("*.parquet")):
        return {"status": "sem_dados"}
    con = duckdb.connect()
    try:
        return con.execute(f"""
            SELECT COUNT(*) as total_registros, COUNT(DISTINCT UF) as total_ufs, 
                   COUNT(DISTINCT cultura) as total_culturas, COUNT(DISTINCT codigo_ibge) as total_municipios
            FROM read_parquet('{parquet_glob}', hive_partitioning=true)
        """).fetchdf().to_dict(orient="records")[0]
    finally:
        con.close()
