import pandas as pd
import requests
from .base_extractor import BaseExtractor
import io

class ZarcExtractor(BaseExtractor):
    """
    Extrator de Tabelas de Zoneamento Agrícola de Risco Climático (ZARC) via MAPA CKAN API.
    A Tábua de Risco traz Risco Climático (20%, 30%, 40%) por período e tipo de solo.
    """

    TARGET_CROPS = ["soja", "milho", "trigo", "algodao", "cana-de-acucar"]
    
    def __init__(self):
        super().__init__()
        
    def extract(self) -> pd.DataFrame:
        all_dfs = []
        
        for crop in self.TARGET_CROPS:
            self.log.info(f"Buscando recurso ZARC no CKAN (dados.agricultura.gov.br) para: {crop}")
            
            # API do CKAN do MAPA.
            # É comum usar termos mais amplos, como zarc e o nome da cultura
            query = f'title:zarc AND ({crop} OR {crop.capitalize()})'
            ckan_url = f"https://dados.agricultura.gov.br/api/3/action/package_search?q={query}&rows=1"
            
            try:
                resp = requests.get(ckan_url, timeout=20)
                if resp.status_code == 200:
                    data = resp.json()
                    results = data.get("result", {}).get("results", [])
                    if results:
                        package = results[0]
                        # Buscar resource CSV
                        csv_url = None
                        for resource in package.get("resources", []):
                            if resource.get("format", "").upper() == "CSV":
                                csv_url = resource.get("url")
                                break
                                
                        if csv_url:
                            self.log.info(f"Fazendo download de: {csv_url}")
                            # Ler direto usando Pandas. Geralmente separador é ';' no BR
                            df_crop = pd.read_csv(csv_url, sep=';', encoding='utf-8', on_bad_lines='skip')
                            df_crop["cultura_raw"] = crop
                            all_dfs.append(df_crop)
                        else:
                            self.log.warning(f"Nenhum CSV encontrado no pacote ZARC para: {crop}")
                    else:
                        self.log.warning(f"Nenhum dataset ZARC retornado para: {crop}")
            except Exception as e:
                self.log.error(f"Erro ao buscar ZARC para {crop}: {e}")

        if not all_dfs:
            return pd.DataFrame()
            
        # O MAPA nem sempre mantém colunas unificadas, então concatenamos tudo
        df_concat = pd.concat(all_dfs, ignore_index=True)
        return df_concat

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
            
        df_clean = df.copy()
        
        # Mapeamento genérico do ZARC
        # Geralmente contém Municipios (Cód IBGE), Cultivar, Solo, Risco
        # Vamos padronizar colunas para lower case e snake_case
        df_clean.columns = (
            df_clean.columns.str.lower()
            .str.replace(" ", "_")
            .str.replace("-", "_")
            .str.normalize('NFKD')
            .str.encode('ascii', errors='ignore')
            .str.decode('utf-8')
        )
        
        # O MAPA costuma usar "cd_mun", "codigo_municipio" ou "ibge"
        col_ibge = [c for c in df_clean.columns if "ibge" in c or "cd_mun" in c or "codigo_mun" in c]
        if col_ibge:
            df_clean = df_clean.rename(columns={col_ibge[0]: "cod_municipio_ibge"})
        
        # Vamos normalizar a cultura raw inserida no extract
        if "cultura_raw" in df_clean.columns:
            df_clean["cultura"] = self.normalize_culture_name(df_clean["cultura_raw"])
            df_clean = df_clean.drop(columns=["cultura_raw"])
            
        return df_clean
