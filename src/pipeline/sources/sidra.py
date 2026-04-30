"""Pipeline SIDRA/PAM: Produção Agrícola Municipal (IBGE)."""

import os
import logging
import numpy as np
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

from pipeline.registry import register
from pipeline.base import BaseSource
from pipeline.utils import normalize_string, get_cultura_id, upsert_data
from db.manager import FatoProducaoPAM

log = logging.getLogger(__name__)


@register("sidra")
class SidraPipeline(BaseSource):
    """
    Extrator PAM: Produção Agrícola Municipal (SIDRA/IBGE).
    Tabela 5457: Lavouras Temporárias.
    """

    TARGET_CROPS = {
        "soja": 40124,
        "milho": 40122,
        "trigo": 40127,
        "algodão": 40100,
        "cana-de-açúcar": 40111
    }

    def __init__(self, ano: str = "2021", data_dir: str = "data/sidra", use_cache: bool = True):
        super().__init__()
        self.ano = ano
        self.data_dir = data_dir
        self.use_cache = use_cache
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir, exist_ok=True)

    # ---- EXTRACT ----

    def _map_culture_ids(self) -> dict:
        """Metadados: Consulta de IDs de categoria no IBGE."""
        self.log.info("Buscando metadados da tabela 5457 no IBGE...")
        url = "https://servicodados.ibge.gov.br/api/v3/agregados/5457/metadados"
        metadata_map = {}
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            for cls in data.get("classificacoes", []):
                if cls["id"] == "782":
                    for cat in cls.get("categorias", []):
                        name_norm = normalize_string(pd.Series([cat["nome"]])).iloc[0]
                        # Normalização: Remoção de sufixos (ex: "em grão")
                        name_clean = name_norm.split("(")[0].strip()
                        metadata_map[name_clean] = cat["id"]
        except Exception as e:
            self.log.warning(f"Falha ao buscar metadados, usando hardcoded. Erro: {e}")
            return self.TARGET_CROPS

        final_map = {}
        for target in self.TARGET_CROPS.keys():
            target_norm = normalize_string(pd.Series([target])).iloc[0]
            for clean_name, cid in metadata_map.items():
                if target_norm in clean_name:
                    final_map[target_norm] = cid
                    break
            if target_norm not in final_map:  # Fallback
                final_map[target_norm] = self.TARGET_CROPS[target]

        return final_map

    def extract(self, **kwargs) -> pd.DataFrame:
        cache_file = os.path.join(self.data_dir, f"pam_sidra_{self.ano}.csv")

        if self.use_cache and os.path.exists(cache_file):
            if not self.is_file_stale(cache_file, threshold_days=30):
                self.log.info(f"Carregando cache SIDRA: {cache_file}")
                return pd.read_csv(cache_file, dtype=str)

        crops_ids = self._map_culture_ids()

        # Variáveis Tabela 5457: 8331 (Área Plantada), 216 (Área Colhida), 214 (Produção), 215 (Valor da Produção)
        variables = "8331,216,214,215"

        def _fetch_crop(crop_name, crop_id):
            self.log.info(f"Buscando dados IBGE para {crop_name} (ID: {crop_id})")
            url = f"https://apisidra.ibge.gov.br/values/t/5457/n6/all/v/{variables}/p/{self.ano}/c782/{crop_id}"
            try:
                resp = requests.get(url, timeout=30)
                if resp.status_code == 200:
                    data = resp.json()
                    if data and len(data) > 1:
                        df_tmp = pd.DataFrame(data[1:], columns=data[0])
                        df_tmp["cultura_raw"] = crop_name
                        return df_tmp
                else:
                    self.log.warning(f"Erro {resp.status_code} na consulta de {crop_name}: {resp.text}")
            except Exception as e:
                self.log.error(f"Exceção ao buscar {crop_name}: {e}")
            return None

        # Requests paralelos: 5 culturas simultâneas (vs. sequencial)
        all_dfs = []
        with ThreadPoolExecutor(max_workers=len(crops_ids)) as pool:
            futures = {pool.submit(_fetch_crop, name, cid): name for name, cid in crops_ids.items()}
            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    all_dfs.append(result)

        if not all_dfs:
            self.log.warning("Extrator SIDRA: nenhum dado retornado para todas as culturas alvo.")
            return pd.DataFrame()

        final_df = pd.concat(all_dfs, ignore_index=True)
        self.log.info(f"Extrator SIDRA: {len(final_df)} linha(s) brutas consolidadas de {len(all_dfs)} cultura(s).")

        if self.use_cache:
            final_df.to_csv(cache_file, index=False)
            self.log.info(f"Cache salvo: {cache_file}")

        return final_df

    # ---- CLEAN ----

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        self.log.info(f"Cleaner PAM/SIDRA: {len(df)} linha(s) recebida(s).")

        col_map = {
            "D2N": "variavel",
            "V": "valor",
            "D1C": "cod_municipio_ibge",
            "D1N": "municipio_nome",
            "D3N": "ano",
            "cultura_raw": "cultura"
        }

        ausentes = [k for k in col_map if k not in df.columns]
        if ausentes:
            self.log.warning(f"PAM/SIDRA: colunas esperadas ausentes no DataFrame bruto: {ausentes}")

        df_clean = df.rename(columns=col_map)
        df_clean = df_clean[[c for c in col_map.values() if c in df_clean.columns]].copy()

        nulos_antes = df_clean["valor"].isna().sum()
        df_clean["valor"] = pd.to_numeric(df_clean["valor"].replace(['...', '-'], np.nan), errors='coerce')
        nulos_depois = df_clean["valor"].isna().sum()
        if nulos_depois > nulos_antes:
            self.log.info(f"PAM/SIDRA: {nulos_depois - nulos_antes} valor(es) não numérico(s) do IBGE ('...', '-') convertido(s) para NaN.")

        # Transformação: Pivoteamento de variáveis para colunas fato
        df_pivot = df_clean.pivot_table(
            index=["cod_municipio_ibge", "municipio_nome", "ano", "cultura"],
            columns="variavel",
            values="valor"
        ).reset_index()
        self.log.info(f"PAM/SIDRA pivot: {len(df_pivot)} combinação(ões) (município × cultura × ano).")

        df_pivot.columns.name = None

        var_renames = {
            "Área plantada": "area_plantada_ha",
            "Área colhida": "area_colhida_ha",
            "Quantidade produzida": "qtde_produzida_ton",
            "Valor da produção": "valor_producao_mil_reais"
        }

        actual_renames = {}
        for col in df_pivot.columns:
            for key, target in var_renames.items():
                if key in col:
                    actual_renames[col] = target

        df_pivot = df_pivot.rename(columns=actual_renames)

        for target in var_renames.values():
            if target not in df_pivot.columns:
                df_pivot[target] = np.nan

        df_pivot["cultura"] = normalize_string(df_pivot["cultura"])

        # Extrair UF do código IBGE (2 primeiros dígitos) para uso no municipio
        # Os dados do SIDRA não trazem coluna UF explícita

        return df_pivot

    # ---- LOAD ----

    def load(self, df: pd.DataFrame, lookups: dict) -> str:
        if df.empty:
            return "0 registros"

        df_f = df.copy()
        df_f["id_cultura"] = df_f["cultura"].apply(lambda x: get_cultura_id(x, lookups["culturas"]))
        df_f["cod_municipio_ibge"] = df_f["cod_municipio_ibge"].astype(str).str[:7]
        df_f["id_municipio"] = df_f["cod_municipio_ibge"].map(lookups["municipios_ibge"])
        cols = ["id_cultura", "id_municipio", "ano", "area_plantada_ha", "area_colhida_ha", "qtde_produzida_ton", "valor_producao_mil_reais"]
        df_f = df_f[cols].dropna(subset=["id_cultura", "id_municipio"])
        upsert_data(FatoProducaoPAM, df_f, index_elements=['id_cultura', 'id_municipio', 'ano'])
        result = f"{len(df_f)} registros upserted"
        self.log.info(f"Fato PAM: {result}.")
        return result
