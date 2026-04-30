"""
Pipeline ZARC: Zoneamento Agrícola de Risco Climático (MAPA).

ESTRATÉGIAS DE OTIMIZAÇÃO PARA GRANDES VOLUMES:
1. Streaming de Dados: Implementado via geradores (yield) para evitar o carregamento
   de arquivos de múltiplos gigabytes na memória RAM.
2. Processamento em Chunks: Utiliza a funcionalidade 'chunksize' do Pandas para
   fatiar a leitura do CSV em blocos controlados (ex: 50k linhas).
3. Detecção de Compressão: Identifica arquivos Gzip através da leitura de Magic Bytes
   iniciais (b'\x1f\x8b'), garantindo a descompressão mesmo em arquivos com extensão .csv.
4. Carga Seletiva: Método de extração de municípios lê apenas as colunas geográficas
   necessárias, reduzindo drasticamente o overhead de I/O.
"""

import logging
import pandas as pd
from pathlib import Path

from pipeline.registry import register
from pipeline.base import BaseSource
from pipeline.utils import normalize_string, get_cultura_id, upsert_data
from db.manager import FatoRiscoZARC

log = logging.getLogger(__name__)


@register("zarc")
class ZarcPipeline(BaseSource):
    """
    Pipeline ZARC com processamento em streaming (chunks).
    Lê arquivos CSV locais por cultura.
    """

    TARGET_CROPS = ["soja", "milho", "trigo", "algodao", "cana-de-acucar"]

    def __init__(self, use_cache: bool = True, data_dir: str = "data/zarc", chunksize: int = 50000):
        super().__init__()
        self.use_cache = use_cache
        self.data_dir = Path(data_dir).resolve()
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.chunksize = chunksize

    # ---- RUN (Override: streaming por chunks) ----

    def run(self, lookups: dict, **kwargs) -> str:
        """Override: processa em chunks para economia de memória."""
        self.log.info("Iniciando pipeline ZARC (streaming)...")
        total = 0
        for chunk in self.extract():
            clean_chunk = self.clean(chunk)
            result = self.load(clean_chunk, lookups)
            # Extrai número do resultado
            try:
                total += int(result.split()[0])
            except (ValueError, IndexError):
                pass
        summary = f"{total} registros processados (streaming)"
        self.log.info(f"Pipeline ZARC concluído: {summary}")
        return summary

    # ---- EXTRACT (Generator) ----

    def extract(self, **kwargs):
        """
        Gera chunks de DataFrames para processamento sequencial a partir dos arquivos de risco.
        """
        for crop in self.TARGET_CROPS:
            cache_file = self.data_dir / f"zarc_{crop}.csv"

            if cache_file.exists():
                self.log.info(f"--- Cultura detectada: {crop.upper()} ---")
                self.log.info(f"Iniciando leitura do arquivo: {cache_file.name}")
                try:
                    with open(cache_file, "rb") as f:
                        is_gzip = f.read(2) == b'\x1f\x8b'
                        
                    reader = pd.read_csv(
                        cache_file,
                        sep=';',
                        encoding='utf-8',
                        on_bad_lines='skip',
                        chunksize=self.chunksize,
                        compression='gzip' if is_gzip else None
                    )
                    for chunk in reader:
                        # Tenta identificar a cultura no chunk se não houver coluna fixa
                        if "Nome_cultura" in chunk.columns:
                            chunk["cultura_raw"] = chunk["Nome_cultura"]
                        elif "cultura_raw" not in chunk.columns:
                            chunk["cultura_raw"] = crop
                        yield chunk
                except Exception as e:
                    self.log.error(f"Erro no processamento de chunk ZARC ({crop}): {e}")
            else:
                self.log.warning(f"Atenção: Arquivo para '{crop.upper()}' não encontrado em {self.data_dir}")
                self.log.info(f"Dica: Baixe o ZARC de {crop} no portal do MAPA e salve como zarc_{crop}.csv")

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

        if not unique_muns:
            return pd.DataFrame()
        return pd.concat(unique_muns).drop_duplicates(subset=["cod_municipio_ibge"])

    # ---- CLEAN ----

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        df_clean = df.copy()

        df_clean.columns = (
            df_clean.columns.str.lower()
            .str.replace(" ", "_")
            .str.replace("-", "_")
            .str.normalize('NFKD')
            .str.encode('ascii', errors='ignore')
            .str.decode('utf-8')
        )

        ibge_cols = [c for c in df_clean.columns if "ibge" in c or "cd_mun" in c or "codigo_mun" in c or "geocodigo" in c]
        if ibge_cols:
            df_clean = df_clean.rename(columns={ibge_cols[0]: "cod_municipio_ibge"})

        if "cultura_raw" in df_clean.columns:
            df_clean["cultura"] = normalize_string(df_clean["cultura_raw"])
            df_clean = df_clean.drop(columns=["cultura_raw"])

        return df_clean

    # ---- LOAD ----

    def load(self, df: pd.DataFrame, lookups: dict) -> str:
        if df.empty:
            return "0 registros"

        map_cult = lookups["culturas"]
        map_mun = lookups["municipios_ibge"]

        df_f = df.copy()
        df_f["id_cultura"] = df_f["cultura"].apply(lambda x: get_cultura_id(x, map_cult))

        if "cod_municipio_ibge" not in df_f.columns:
            return "0 registros (sem coluna município)"

        df_f["cod_municipio_ibge"] = df_f["cod_municipio_ibge"].astype(str).str[:7]
        df_f["id_municipio"] = df_f["cod_municipio_ibge"].map(map_mun)

        # Tratamento de colunas dec1...dec36 (Formato Largo para Longo)
        dec_cols = [c for c in df_f.columns if c.lower().startswith("dec")]
        if dec_cols:
            id_vars = [c for c in df_f.columns if c not in dec_cols]
            df_f = df_f.melt(id_vars=id_vars, value_vars=dec_cols, var_name="periodo_plantio", value_name="risco_climatico")

        # Mapeamento de Solo
        if "cod_solo" in df_f.columns:
            df_f = df_f.rename(columns={"cod_solo": "tipo_solo"})

        cols = ["id_cultura", "id_municipio", "tipo_solo", "periodo_plantio", "risco_climatico"]
        df_f = df_f[[c for c in cols if c in df_f.columns]].dropna(subset=["id_cultura", "id_municipio"])

        # Remove riscos nulos
        df_f = df_f[df_f["risco_climatico"].notna()]

        upsert_data(FatoRiscoZARC, df_f, index_elements=['id_cultura', 'id_municipio', 'tipo_solo', 'periodo_plantio'])
        result = f"{len(df_f)} registros processados neste chunk"
        self.log.info(f"Fato ZARC: {result}.")
        return result
