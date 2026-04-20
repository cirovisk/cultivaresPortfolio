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
