"""
Processamento Completo ZARC → Parquet (Indicação + Risco).

Transforma os dois datasets massivos do ZARC em formatos colunares
particionados, prontos para consultas federadas.

Datasets:
1. Indicações: Cultivares recomendados por UF/Safra (~196M linhas).
2. Risco: Tabela de risco climático (%) por Município/Cultura/Decêndio.
"""
import duckdb
import os
import shutil
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# Configurações de saída
INDICACOES_OUT = PARQUET_BASE / "zarc_indicacoes"
RISCO_OUT = PARQUET_BASE / "zarc_risco"

# DICA PARA EXPANSÃO:
# Para adicionar novas culturas (Milho, Trigo, etc):
# 1. Baixe o CSV correspondente no Portal de Dados Abertos do MAPA.
# 2. Coloque na pasta DATA_DIR / "zarc".
# 3. Adicione o nome do arquivo na lista abaixo para processamento em lote.
EXTRA_FILES = {
    "indicacoes": ["zarc_soja.csv"], # Adicione outros CSVs de indicação aqui
    "risco": ["zarc_risco.csv"]      # Adicione outros CSVs de risco aqui
}



def process_indicacoes():
    """Converte Indicação de Cultivares (Safra, Cultura, Cultivar, UF...)"""
    # Para processar múltiplas culturas, você pode transformar este bloco em um loop
    # percorrendo os arquivos em EXTRA_FILES["indicacoes"]
    source = ZARC_RAW_DIR / "zarc_soja.csv"

    if not source.exists():
        log.warning(f"Fonte de indicações não encontrada: {source}")
        return
    
    if INDICACOES_OUT.exists():
        shutil.rmtree(INDICACOES_OUT)
    INDICACOES_OUT.mkdir(parents=True, exist_ok=True)
    
    log.info(f"Processando Indicações: {source}...")
    con = duckdb.connect()
    try:
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
                    '{source}',
                    delim=';',
                    header=true,
                    compression='gzip',
                    ignore_errors=true
                )
                WHERE UF IS NOT NULL AND TRIM(UF) != ''
            )
            TO '{INDICACOES_OUT}'
            (FORMAT PARQUET, PARTITION_BY (UF))
        """)
        count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{INDICACOES_OUT}/**/*.parquet', hive_partitioning=true)").fetchone()[0]
        log.info(f"Sucesso: {count:,} registros em {INDICACOES_OUT}")
    finally:
        con.close()


def process_risco():
    """Converte Tabela de Risco (Município, Cultura, Solo, 36 Decêndios...)"""
    # Para processar múltiplas safras de risco, mude para um loop sobre
    # EXTRA_FILES["risco"] para concatenar no mesmo dataset Parquet.
    source = ZARC_RAW_DIR / "zarc_risco.csv"

    if not source.exists():
        log.warning(f"Fonte de risco não encontrada: {source}")
        return
    
    if RISCO_OUT.exists():
        shutil.rmtree(RISCO_OUT)
    RISCO_OUT.mkdir(parents=True, exist_ok=True)
    
    log.info(f"Processando Risco Climático: {source}...")
    con = duckdb.connect()
    try:
        # Mapeia dec1..dec36 para colunas numéricas
        dec_cols = ", ".join([f"CAST(dec{i} AS INTEGER) AS dec{i}" for i in range(1, 37)])
        
        con.execute(f"""
            COPY (
                SELECT 
                    TRIM(Nome_cultura) AS cultura,
                    CAST(SafraIni AS INTEGER) AS ano_inicio,
                    CAST(SafraFin AS INTEGER) AS ano_fim,
                    CAST(geocodigo AS VARCHAR) AS codigo_ibge,
                    UPPER(TRIM(UF)) AS UF,
                    TRIM(municipio) AS municipio,
                    CAST(Cod_Solo AS INTEGER) AS id_solo,
                    {dec_cols}
                FROM read_csv(
                    '{source}',
                    delim=';',
                    header=true,
                    ignore_errors=true
                )
                WHERE geocodigo IS NOT NULL
            )
            TO '{RISCO_OUT}'
            (FORMAT PARQUET, PARTITION_BY (UF))
        """)
        count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{RISCO_OUT}/**/*.parquet', hive_partitioning=true)").fetchone()[0]
        log.info(f"Sucesso: {count:,} registros em {RISCO_OUT}")
    finally:
        con.close()


if __name__ == "__main__":
    log.info("=== ETL ZARC INTEGRADO (Indicação + Risco) ===")
    process_indicacoes()
    process_risco()
    log.info("=== ETL Finalizado ===")
