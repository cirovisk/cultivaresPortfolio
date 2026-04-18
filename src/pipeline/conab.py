import pandas as pd
import requests
import os
from io import StringIO
from .base_extractor import BaseExtractor

class ConabExtractor(BaseExtractor):
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

    def extract(self) -> dict:
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
                        encoding="utf-8",
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

    def transform(self, dataframes: dict) -> dict:
        """
        Transformação polimórfica baseada na chave do dataset.
        """
        self.log.info(f"Iniciando transformação CONAB: {list(dataframes.keys())} dataset(s) recebidos.")
        processed = {}
        
        # 1. Produção
        for key in ["producao_historica", "producao_estimativa"]:
            if key in dataframes:
                processed[key] = self._transform_producao(dataframes[key])
        
        # 2. Preços Mensais
        for key in ["precos_uf_mensal", "precos_mun_mensal"]:
            if key in dataframes:
                processed[key] = self._transform_precos(dataframes[key], freq="mensal")

        # 3. Preços Semanais
        for key in ["precos_uf_semanal", "precos_mun_semanal"]:
            if key in dataframes:
                processed[key] = self._transform_precos(dataframes[key], freq="semanal")

        return processed

    def _transform_producao(self, df):
        self.log.info(f"CONAB Produção: {len(df)} linha(s) brutas recebidas.")
        df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
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
        
        df["cultura"] = self.normalize_string(df["produto_raw"])
        cols_final = ["ano_agricola", "safra", "uf", "cultura", "area_plantada_mil_ha", "producao_mil_t", "produtividade_t_ha"]
        df_out = df[cols_final]
        self.log.info(f"CONAB Produção: {len(df_out)} linha(s) após normalização. Culturas: {sorted(df_out['cultura'].dropna().unique().tolist())}.")
        return df_out

    def _transform_precos(self, df, freq="mensal"):
        self.log.info(f"CONAB Preços ({freq}): {len(df)} linha(s) brutas recebidas.")
        df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
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
        df["valor_kg"] = pd.to_numeric(df["valor_kg"].str.replace(",", "."), errors="coerce").fillna(0.0)
        df["ano"] = pd.to_numeric(df["ano"], errors="coerce").fillna(0).astype(int)
        df["mes"] = pd.to_numeric(df["mes"], errors="coerce").fillna(0).astype(int)
        df["cultura"] = self.normalize_string(df["produto_raw"])
        
        cols = ["cultura", "uf", "ano", "mes", "valor_kg", "nivel_comercializacao"]
        if "cod_municipio_ibge" in df.columns:
            cols.append("cod_municipio_ibge")
            df["cod_municipio_ibge"] = df["cod_municipio_ibge"].str.strip()
            
        if freq == "semanal":
            cols.extend(["semana", "data_referencia"])
            df["semana"] = pd.to_numeric(df["semana"], errors="coerce").fillna(0).astype(int)
        
        return df[cols]

    def run(self) -> dict:
        """Override do BaseExtractor para suportar retorno múltiplo."""
        self.log.info("Iniciando extração e transformação CONAB...")
        data = self.extract()
        return self.transform(data)
