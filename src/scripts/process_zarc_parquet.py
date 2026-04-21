"""
Processamento ZARC → Parquet (Particionado por UF).

Converte o arquivo massivo zarc_soja.csv (gzip, ~196M linhas)
em arquivos Parquet particionados, otimizados para consultas analíticas.

Estratégia: Usa DuckDB como motor de ETL para processar o arquivo inteiro
sem carregar tudo na RAM (streaming nativo do DuckDB).
"""
import duckdb
import os
import shutil
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# Diretórios
DATA_DIR = Path("/app/data")
ZARC_RAW_DIR = DATA_DIR / "zarc"
PARQUET_DIR = DATA_DIR / "parquet" / "zarc_indicacoes"


def process_zarc_indicacoes():
    """
    Converte CSV gzip do ZARC (Indicação de Cultivares) em Parquet particionado por UF.
    
    Arquitetura de Saída:
        data/parquet/zarc_indicacoes/
        ├── UF=RS/
        │   └── data_0.parquet
        ├── UF=MT/
        │   └── data_0.parquet
        └── ...
    """
    source_file = ZARC_RAW_DIR / "zarc_soja.csv"
    
    if not source_file.exists():
        log.error(f"Arquivo fonte não encontrado: {source_file}")
        return False
    
    # Limpa diretório de saída (DuckDB 1.5.x não suporta OVERWRITE_OR_CREATE)
    if PARQUET_DIR.exists():
        shutil.rmtree(PARQUET_DIR)
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    
    log.info(f"Iniciando conversão: {source_file} → {PARQUET_DIR}")
    
    con = duckdb.connect()
    
    try:
        # DuckDB lê CSV gzip nativamente com streaming — sem carregar tudo na RAM
        con.execute(f"""
            COPY (
                SELECT 
                    TRIM(Safra) AS safra,
                    TRIM(Cultura) AS cultura,
                    TRIM(Obtentor_Mantenedor) AS obtentor_mantenedor,
                    TRIM(Cultivar) AS cultivar,
                    UPPER(TRIM(UF)) AS UF,
                    CAST(Grupo AS INTEGER) AS grupo,
                    TRIM(Regiao_de_Adaptacao) AS regiao_adaptacao
                FROM read_csv(
                    '{source_file}',
                    delim=';',
                    header=true,
                    compression='gzip',
                    ignore_errors=true
                )
                WHERE UF IS NOT NULL AND TRIM(UF) != ''
            )
            TO '{PARQUET_DIR}'
            (FORMAT PARQUET, PARTITION_BY (UF))
        """)
        
        # Conta registros para verificação
        count = con.execute(f"""
            SELECT COUNT(*) FROM read_parquet('{PARQUET_DIR}/**/*.parquet', hive_partitioning=true)
        """).fetchone()[0]
        
        # Tamanho total em disco
        total_size = sum(
            f.stat().st_size 
            for f in PARQUET_DIR.rglob("*.parquet")
        )
        total_size_mb = total_size / (1024 * 1024)
        
        # Contagem de partições (UFs)
        partitions = [d.name for d in PARQUET_DIR.iterdir() if d.is_dir()]
        
        log.info(f"Conversão concluída!")
        log.info(f"  Registros: {count:,}")
        log.info(f"  Tamanho Parquet: {total_size_mb:.1f} MB")
        log.info(f"  Partições (UFs): {len(partitions)} → {sorted(partitions)}")
        
        return True
        
    except Exception as e:
        log.error(f"Erro na conversão: {e}")
        raise
    finally:
        con.close()


def verify_parquet():
    """Roda uma query de verificação rápida nos Parquets gerados."""
    con = duckdb.connect()
    try:
        # Top 5 UFs por volume de indicações
        result = con.execute(f"""
            SELECT 
                UF, 
                COUNT(*) as total_indicacoes,
                COUNT(DISTINCT cultivar) as cultivares_distintos,
                COUNT(DISTINCT cultura) as culturas_distintas
            FROM read_parquet('{PARQUET_DIR}/**/*.parquet', hive_partitioning=true)
            GROUP BY UF
            ORDER BY total_indicacoes DESC
            LIMIT 10
        """).fetchdf()
        
        log.info("Top 10 UFs por indicações ZARC:")
        log.info(f"\n{result.to_string(index=False)}")
        
    finally:
        con.close()


if __name__ == "__main__":
    log.info("=== Processamento ZARC: Indicação de Cultivares ===")
    success = process_zarc_indicacoes()
    if success:
        verify_parquet()
    log.info("=== Fim ===")
