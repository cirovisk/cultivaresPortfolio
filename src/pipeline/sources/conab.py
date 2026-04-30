"""Pipeline CONAB: Produção e Preços agrícolas (CONAB)."""

import os
import logging
import pandas as pd
import requests

from pipeline.registry import register
from pipeline.base import BaseSource
from pipeline.utils import normalize_string, get_cultura_id, upsert_data
from db.manager import (
    engine, FatoProducaoConab, FatoPrecoConabMensal, FatoPrecoConabSemanal
)
from sqlalchemy import text

log = logging.getLogger(__name__)


@register("conab")
class ConabPipeline(BaseSource):
    """
    Extrator Multimodal CONAB: Produção e Preços.
    """

    BASE_URL = "https://portaldeinformacoes.conab.gov.br/downloads/arquivos/"

    FILES = {
        "producao_historica": "SerieHistoricaGraos.txt",
        "producao_estimativa": "LevantamentoGraos.txt",
        "precos_uf_mensal": "PrecosMensalUF.txt",
        "precos_mun_mensal": "PrecosMensalMunicipio.txt",
        "precos_uf_semanal": "PrecosSemanalUF.txt",
        "precos_mun_semanal": "PrecosSemanalMunicipio.txt"
    }

    def __init__(self, data_dir="data/conab", force_refresh=False):
        super().__init__()
        self.data_dir = data_dir
        self.force_refresh = force_refresh
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir, exist_ok=True)

    # ---- EXTRACT ----

    def extract(self, **kwargs) -> dict:
        """
        Extrai todos os arquivos configurados.
        Implementa atualização automática: se o arquivo não existe, baixa.
        """
        dataframes = {}
        for key, filename in self.FILES.items():
            local_path = os.path.join(self.data_dir, filename)

            # Lógica de atualização:
            is_stale = self.is_file_stale(local_path, threshold_days=(7 if "semanal" in key else 30))

            if self.force_refresh or not os.path.exists(local_path) or is_stale:
                reason = "forçado" if self.force_refresh else ("ausente" if not os.path.exists(local_path) else "desatualizado")
                self.log.info(f"Atualizando {filename} ({reason})...")
                self._download_file(filename, local_path)

            if os.path.exists(local_path):
                self.log.info(f"Carregando {filename} para memória...")
                try:
                    df = pd.read_csv(
                        local_path,
                        sep=";",
                        encoding="latin1",
                        dtype=str,
                        skipinitialspace=True
                    )
                    dataframes[key] = df
                except Exception as e:
                    self.log.error(f"Erro ao ler {filename}: {e}")

        return dataframes

    def _download_file(self, filename, local_path):
        url = self.BASE_URL + filename

        # Arquivamento (apenas para Mensais e Produção/Histórico)
        if os.path.exists(local_path) and "semanal" not in filename:
            import shutil
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_dir = os.path.join(self.data_dir, "archive")
            os.makedirs(archive_dir, exist_ok=True)
            archive_path = os.path.join(archive_dir, f"{os.path.splitext(filename)[0]}_{timestamp}.txt")
            shutil.move(local_path, archive_path)
            self.log.info(f"Arquivo antigo arquivado: {os.path.basename(archive_path)}")

        try:
            resp = requests.get(url, timeout=120)
            resp.raise_for_status()
            with open(local_path, "wb") as f:
                f.write(resp.content)
            self.log.info(f"Download concluído: {filename}")
        except Exception as e:
            self.log.error(f"Erro no download de {url}: {e}")

    # ---- CLEAN ----

    def clean(self, dataframes: dict) -> dict:
        """Entrypoint funcional para limpar o dict vindo do extract."""
        processed = {}

        for key in ["producao_historica", "producao_estimativa"]:
            if key in dataframes:
                processed[key] = self._clean_producao(dataframes[key])

        for key in ["precos_uf_mensal", "precos_mun_mensal"]:
            if key in dataframes:
                processed[key] = self._clean_precos(dataframes[key], freq="mensal")

        for key in ["precos_uf_semanal", "precos_mun_semanal"]:
            if key in dataframes:
                processed[key] = self._clean_precos(dataframes[key], freq="semanal")

        return processed

    def _clean_producao(self, df: pd.DataFrame) -> pd.DataFrame:
        self.log.info(f"Cleaner CONAB Produção: {len(df)} linha(s) brutas.")
        df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
        renames = {
            "ano_agricola": "ano_agricola",
            "dsc_safra_previsao": "safra",
            "uf": "uf",
            "produto": "produto_raw",
            "area_plantada_mil_ha": "area_plantada_mil_ha",
            "producao_mil_t": "producao_mil_t",
            "produtividade_mil_ha_mil_t": "produtividade_t_ha"
        }
        df = df.rename(columns=renames)
        cols_num = ["area_plantada_mil_ha", "producao_mil_t", "produtividade_t_ha"]
        for col in cols_num:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

        df["cultura"] = normalize_string(df["produto_raw"])
        cols_final = ["ano_agricola", "safra", "uf", "cultura", "area_plantada_mil_ha", "producao_mil_t", "produtividade_t_ha"]
        available_cols = [c for c in cols_final if c in df.columns]
        return df[available_cols]

    def _clean_precos(self, df: pd.DataFrame, freq="mensal") -> pd.DataFrame:
        self.log.info(f"Cleaner CONAB Preços ({freq}): {len(df)} linha(s) brutas.")
        df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
        renames = {
            "produto": "produto_raw",
            "uf": "uf",
            "nom_municipio": "municipio",
            "cod_ibge": "cod_municipio_ibge",
            "ano": "ano",
            "mes": "mes",
            "valor_produto_kg": "valor_kg",
            "dsc_nivel_comercializacao": "nivel_comercializacao",
            "semana": "semana",
            "data_inicial_final_semana": "data_referencia"
        }
        df = df.rename(columns=renames)

        # Casting e Limpeza
        if "valor_kg" in df.columns:
            df["valor_kg"] = pd.to_numeric(df["valor_kg"].str.replace(",", "."), errors="coerce").fillna(0.0)
        if "ano" in df.columns:
            df["ano"] = pd.to_numeric(df["ano"], errors="coerce").fillna(0).astype(int)
        if "mes" in df.columns:
            df["mes"] = pd.to_numeric(df["mes"], errors="coerce").fillna(0).astype(int)
        if "produto_raw" in df.columns:
            df["cultura"] = normalize_string(df["produto_raw"])

        cols = ["cultura", "uf", "ano", "mes", "valor_kg", "nivel_comercializacao"]
        if "cod_municipio_ibge" in df.columns:
            cols.append("cod_municipio_ibge")
            df["cod_municipio_ibge"] = df["cod_municipio_ibge"].str.strip()

        if freq == "semanal":
            cols.extend(["semana", "data_referencia"])
            if "semana" in df.columns:
                df["semana"] = pd.to_numeric(df["semana"], errors="coerce").fillna(0).astype(int)

        available_cols = [c for c in cols if c in df.columns]
        return df[available_cols]

    # ---- LOAD ----

    def load(self, df_dict: dict, lookups: dict) -> str:
        if not isinstance(df_dict, dict):
            return "0 registros (formato inválido)"

        map_cult = lookups["culturas"]
        map_mun = lookups["municipios_ibge"]
        total = 0

        for key, df in df_dict.items():
            if df.empty:
                continue
            df_f = df.copy()
            df_f["id_cultura"] = df_f["cultura"].apply(lambda x: get_cultura_id(x, map_cult))
            df_f = df_f.dropna(subset=["id_cultura"])
            if "cod_municipio_ibge" in df_f.columns:
                df_f["id_municipio"] = df_f["cod_municipio_ibge"].astype(str).str[:7].map(map_mun)
            else:
                df_f["id_municipio"] = None

            if "producao" in key:
                index = ['id_cultura', 'uf', 'ano_agricola', 'safra']
                upsert_data(FatoProducaoConab, df_f, index_elements=index)
            elif "mensal" in key:
                index = ['id_cultura', 'id_municipio', 'uf', 'ano', 'mes', 'nivel_comercializacao']
                upsert_data(FatoPrecoConabMensal, df_f, index_elements=index)
            elif "semanal" in key:
                # Política semanal de 4 semanas
                with engine.connect() as conn:
                    count = conn.execute(text("SELECT COUNT(DISTINCT semana) FROM fato_precos_conab_semanal")).scalar() or 0
                if count >= 4:
                    with engine.begin() as conn:
                        conn.execute(text("TRUNCATE TABLE fato_precos_conab_semanal"))
                index = ['id_cultura', 'id_municipio', 'uf', 'ano', 'mes', 'semana', 'nivel_comercializacao']
                upsert_data(FatoPrecoConabSemanal, df_f, index_elements=index)

            total += len(df_f)
            self.log.info(f"Fato CONAB ({key}): Upsert concluído.")

        return f"{total} registros upserted (total)"
