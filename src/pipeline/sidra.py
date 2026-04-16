import pandas as pd
import requests
from .base_extractor import BaseExtractor

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

    def __init__(self, ano: str = "last"):
        super().__init__()
        self.ano = ano

    def _get_classification_codes(self) -> dict:
        """
        Metadados: Consulta de IDs de categoria no IBGE.
        """
        self.log.info("Buscando metadados da tabela 1612 no IBGE...")
        url = "https://servicodados.ibge.gov.br/api/v3/agregados/1612/metadados"
        crops_map = {}
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            for cls in data.get("classificacoes", []):
                if cls["id"] == "81":
                    for cat in cls.get("categorias", []):
                        name_norm = self.normalize_culture_name(pd.Series([cat["nome"]])).iloc[0]
                        # Normalização: Remoção de sufixos (ex: "em grão")
                        name_clean = name_norm.split("(")[0].strip()
                        crops_map[name_clean] = cat["id"]
        except Exception as e:
            self.log.warning(f"Falha ao buscar metadados, usando hardcoded. Erro: {e}")
            return self.TARGET_CROPS
        
        # Filtra pelo TARGET_CROPS
        final_map = {}
        for target in self.TARGET_CROPS.keys():
            t_norm = self.normalize_culture_name(pd.Series([target])).iloc[0]
            for clean_name, cid in crops_map.items():
                if t_norm in clean_name:
                    final_map[t_norm] = cid
                    break
            if t_norm not in final_map: # Fallback
                final_map[t_norm] = self.TARGET_CROPS[target]
                
        return final_map

    def extract(self) -> pd.DataFrame:
        crops_ids = self._get_classification_codes()
        all_dfs = []
        
        # Variáveis: 109 (Área Plantada), 216 (Área Colhida), 214 (Produção)
        variables = "109,216,214"
        
        for crop_name, crop_id in crops_ids.items():
            self.log.info(f"Buscando dados IBGE para {crop_name} (ID: {crop_id})")
            
            # Formato APISIDRA: /values/t/1612/n6/all/v/109,216,214/p/last/c81/{crop_id}
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
            return pd.DataFrame()
            
        return pd.concat(all_dfs, ignore_index=True)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
            
        # Transformação: Mapeamento de colunas SIDRA
        col_map = {
            "D2N": "variavel",
            "V": "valor",
            "D1C": "cod_municipio_ibge",
            "D1N": "municipio_nome",
            "D3N": "ano",
            "cultura_raw": "cultura"
        }
        
        df_clean = df.rename(columns=col_map)
        df_clean = df_clean[list(col_map.values())].copy()
        
        # Sanitização: Conversão de nulos IBGE ('...', '-') para NaN
        import numpy as np
        df_clean["valor"] = pd.to_numeric(df_clean["valor"].replace(['...', '-'], np.nan), errors='coerce')
        
        # Transformação: Pivoteamento de variáveis para colunas fato
        df_pivot = df_clean.pivot_table(
            index=["cod_municipio_ibge", "municipio_nome", "ano", "cultura"],
            columns="variavel",
            values="valor"
        ).reset_index()
        
        # Limpar o nome das variáveis para os nomes de colunas usando snake_case
        df_pivot.columns.name = None
        
        # Transformação: Normalização de nomes de variáveis fato
        var_renames = {
            "Área plantada": "area_plantada_ha",
            "Área colhida": "area_colhida_ha",
            "Quantidade produzida": "qtde_produzida_ton",
            "Valor da produção": "valor_producao_mil_reais"
        }
        
        # Encontra colunas que realmente existem e contêm os termos chave
        actual_renames = {}
        for col in df_pivot.columns:
            for key, target in var_renames.items():
                if key in col:
                    actual_renames[col] = target
        
        df_pivot = df_pivot.rename(columns=actual_renames)
        
        # Garante que as colunas finais existam (reindex)
        for target in var_renames.values():
            if target not in df_pivot.columns:
                df_pivot[target] = np.nan
        
        # Normaliza a cultura
        df_pivot["cultura"] = self.normalize_culture_name(df_pivot["cultura"])
        
        return df_pivot
