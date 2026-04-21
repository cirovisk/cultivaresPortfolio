import pandas as pd
import requests
import os
from .base_extractor import BaseExtractor
from .cleaners.utils import normalize_string

class SidraExtractor(BaseExtractor):
    """
    Extrator PAM: Produção Agrícola Municipal (SIDRA/IBGE).
    Tabela 1612: Lavouras Temporárias.
    """

    TARGET_CROPS = {
        "soja": 40280,
        "milho": 39444, # Pode variar (milho na grão)
        "trigo": 40307,
        "algodão": 39433, # algodão herbácio
        "cana-de-açúcar": 39441
    }

    def __init__(self, ano: str = "2021", data_dir: str = "data/sidra", use_cache: bool = True):
        super().__init__()
        self.ano = ano
        self.data_dir = data_dir
        self.use_cache = use_cache
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir, exist_ok=True)

    def _map_culture_ids(self) -> dict:
        """
        Metadados: Consulta de IDs de categoria no IBGE.
        """
        self.log.info("Buscando metadados da tabela 1612 no IBGE...")
        url = "https://servicodados.ibge.gov.br/api/v3/agregados/1612/metadados"
        metadata_map = {}
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            for cls in data.get("classificacoes", []):
                if cls["id"] == "81":
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
            if target_norm not in final_map: # Fallback
                final_map[target_norm] = self.TARGET_CROPS[target]
                
        return final_map

    def extract(self) -> pd.DataFrame:
        cache_file = os.path.join(self.data_dir, f"pam_sidra_{self.ano}.csv")
        
        if self.use_cache and os.path.exists(cache_file):
            if not self.is_file_stale(cache_file, threshold_days=30):
                self.log.info(f"Carregando cache SIDRA: {cache_file}")
                return pd.read_csv(cache_file, dtype=str)
        
        crops_ids = self._map_culture_ids()
        all_dfs = []
        
        # Variáveis: 109 (Área Plantada), 216 (Área Colhida), 214 (Produção)
        variables = "109,216,214"
        
        for crop_name, crop_id in crops_ids.items():
            self.log.info(f"Buscando dados IBGE para {crop_name} (ID: {crop_id})")
            url = f"https://apisidra.ibge.gov.br/values/t/1612/n6/all/v/{variables}/p/{self.ano}/c81/{crop_id}"
            
            try:
                resp = requests.get(url, timeout=30)
                if resp.status_code == 200:
                    data = resp.json()
                    if data and len(data) > 1:
                        df_tmp = pd.DataFrame(data[1:], columns=data[0])
                        df_tmp["cultura_raw"] = crop_name
                        all_dfs.append(df_tmp)
                else:
                    self.log.warning(f"Erro {resp.status_code} na consulta de {crop_name}: {resp.text}")
            except Exception as e:
                self.log.error(f"Exceção ao buscar {crop_name}: {e}")
        
        if not all_dfs:
            self.log.warning("Extrator SIDRA: nenhum dado retornado para todas as culturas alvo. Verifique conectividade ou IDs de categoria.")
            return pd.DataFrame()

        final_df = pd.concat(all_dfs, ignore_index=True)
        self.log.info(f"Extrator SIDRA: {len(final_df)} linha(s) brutas consolidadas de {len(all_dfs)} cultura(s).")
        
        if self.use_cache:
            final_df.to_csv(cache_file, index=False)
            self.log.info(f"Cache salvo: {cache_file}")
            
        return final_df
