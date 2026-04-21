import pandas as pd
import requests
import os
import io
from pathlib import Path
from .base_extractor import BaseExtractor

class ZarcExtractor(BaseExtractor):
    """
    Extrator ZARC: Zoneamento Agrícola de Risco Climático (MAPA).
    
    ESTRATÉGIAS DE OTIMIZAÇÃO PARA GRANDES VOLUMES:
    1. Streaming de Dados: Implementado via geradores (yield) para evitar o carregamento 
       de arquivos de múltiplos gigabytes na memória RAM.
    2. Processamento em Chunks: Utiliza a funcionalidade 'chunksize' do Pandas para 
       fatiar a leitura do CSV em blocos controlados (ex: 50k linhas).
    3. Detecção de Compressão: Identifica arquivos Gzip através da leitura de Magic Bytes 
       iniciais (b'\\x1f\\x8b'), garantindo a descompressão mesmo em arquivos com extensão .csv.
    4. Carga Seletiva: Método de extração de municípios lê apenas as colunas geográficas 
       necessárias, reduzindo drasticamente o overhead de I/O.
    """

    TARGET_CROPS = ["soja", "milho", "trigo", "algodao", "cana-de-acucar"]
    
    def __init__(self, use_cache: bool = True, data_dir: str = "data/zarc"):
        super().__init__()
        self.use_cache = use_cache
        self.data_dir = Path(data_dir).resolve()
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
    def extract(self, chunksize=50000):
        """
        Gera chunks de DataFrames para processamento sequencial.
        """
        for crop in self.TARGET_CROPS:
            cache_file = self.data_dir / f"zarc_{crop}.csv"
            
            if cache_file.exists():
                self.log.info(f"Iniciando streaming ZARC: {crop}")
                try:
                    with open(cache_file, "rb") as f:
                        is_gzip = f.read(2) == b'\x1f\x8b'

                    reader = pd.read_csv(
                        cache_file, 
                        sep=';', 
                        encoding='utf-8', 
                        on_bad_lines='skip', 
                        compression='gzip' if is_gzip else None, 
                        chunksize=chunksize,
                    )
                    for chunk in reader:
                        chunk["cultura_raw"] = crop
                        yield chunk
                except Exception as e:
                    self.log.error(f"Erro no processamento de chunk ({crop}): {e}")
            else:
                self.log.warning(f"Cache não encontrado para {crop}. Pule para próxima fonte.")
                continue

    def get_municipios_only(self):
        """
        Extrai municípios únicos utilizando leitura parcial de colunas para economia de RAM.
        """
        unique_muns = []
        for crop in self.TARGET_CROPS:
            cache_file = self.data_dir / f"zarc_{crop}.csv"
            if cache_file.exists():
                self.log.info(f"Extraindo metadados geográficos: {crop}")
                try:
                    with open(cache_file, "rb") as f:
                        is_gzip = f.read(2) == b'\x1f\x8b'
                    
                    # Carrega apenas o cabeçalho para validar colunas
                    header_check = pd.read_csv(cache_file, sep=';', nrows=0, compression='gzip' if is_gzip else None)
                    use_cols = ["cod_municipio_ibge", "municipio", "uf"] if "cod_municipio_ibge" in header_check.columns else ["UF"]
                    
                    reader = pd.read_csv(
                        cache_file, 
                        sep=';', 
                        usecols=use_cols,
                        compression='gzip' if is_gzip else None, 
                        chunksize=100000
                    )
                    for chunk in reader:
                        if "cod_municipio_ibge" in chunk.columns:
                            unique_muns.append(chunk[["cod_municipio_ibge", "municipio", "uf"]].drop_duplicates())
                except Exception as e:
                    self.log.error(f"Erro na extração seletiva de municípios ({crop}): {e}")
        
        if not unique_muns: return pd.DataFrame()
        return pd.concat(unique_muns).drop_duplicates(subset=["cod_municipio_ibge"])
