import pandas as pd
import requests
import os
from .base_extractor import BaseExtractor

class FertilizantesExtractor(BaseExtractor):
    """
    Extrator SIPEAGRO: Estabelecimentos de Fertilizantes.
    URL: https://dados.agricultura.gov.br/dataset/52a01565-72d6-410e-b21b-64035831a7be/resource/e0bbc9d5-f161-448b-a6d4-c7beb312ec33
    """
    
    DOWNLOAD_URL = "https://dados.agricultura.gov.br/dataset/52a01565-72d6-410e-b21b-64035831a7be/resource/e0bbc9d5-f161-448b-a6d4-c7beb312ec33/download/sipeagrofertilizante.csv"
    FILENAME = "sipeagrofertilizante.csv"

    def __init__(self, data_dir="data/fertilizantes", force_refresh=False):
        super().__init__()
        self.data_dir = data_dir
        self.force_refresh = force_refresh
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir, exist_ok=True)

    def extract(self) -> pd.DataFrame:
        """
        Extrai o arquivo de fertilizantes.
        """
        local_path = os.path.join(self.data_dir, self.FILENAME)
        is_stale = self.is_file_stale(local_path, threshold_days=30)

        if self.force_refresh or not os.path.exists(local_path) or is_stale:
            reason = "forçado" if self.force_refresh else ("ausente" if not os.path.exists(local_path) else "desatualizado")
            self.log.info(f"Iniciando download do SIPEAGRO ({reason}): {self.DOWNLOAD_URL}")
            self._download_file(local_path)
        else:
            self.log.info(f"Cache SIPEAGRO válido encontrado: {local_path}")

        if os.path.exists(local_path):
            try:
                df = pd.read_csv(
                    local_path,
                    sep=";",
                    encoding="latin1",
                    dtype=str,
                    skipinitialspace=True
                )
                self.log.info(f"SIPEAGRO carregado: {len(df)} linha(s), {len(df.columns)} coluna(s).")
                return df
            except Exception as e:
                self.log.error(f"Erro ao ler {self.FILENAME}: {e}")

        return pd.DataFrame()

    def _download_file(self, local_path):
        self.log.info(f"Fazendo download de {self.DOWNLOAD_URL}...")

        # Arquivamento (Idempotência/Histórico)
        if os.path.exists(local_path):
            import shutil
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_dir = os.path.join(self.data_dir, "archive")
            os.makedirs(archive_dir, exist_ok=True)
            archive_path = os.path.join(archive_dir, f"sipeagrofertilizante_{timestamp}.csv")
            shutil.move(local_path, archive_path)
            self.log.info(f"Arquivo antigo arquivado em {archive_path}")

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        try:
            # Primeira tentativa: com verificação SSL habilitada
            resp = requests.get(self.DOWNLOAD_URL, headers=headers, timeout=120, verify=True)
            resp.raise_for_status()
        except requests.exceptions.SSLError as ssl_err:
            # O servidor do MAPA/SIPEAGRO ocasionalmente apresenta problema de cadeia de certificado.
            # Segunda tentativa sem verificação, com log de aviso explícito.
            self.log.warning(
                f"SSL handshake falhou ({ssl_err}). "
                "Repetindo sem verificação de certificado. "
                "Recomendação: instale o certificado raiz do MAPA ou adicione-o ao bundle do certifi."
            )
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            try:
                resp = requests.get(self.DOWNLOAD_URL, headers=headers, timeout=120, verify=False)
                resp.raise_for_status()
            except Exception as e:
                self.log.error(f"Erro no download mesmo sem verificação SSL: {e}")
                return
        except Exception as e:
            self.log.error(f"Erro no download de {self.DOWNLOAD_URL}: {e}")
            return

        with open(local_path, "wb") as f:
            f.write(resp.content)
        self.log.info(f"Download concluído: {self.FILENAME} ({len(resp.content) / 1024:.1f} KB)")

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        self.log.info(f"Transformando SIPEAGRO: {len(df)} linha(s) recebida(s).")

        renames = {
            "UNIDADE_DA_FEDERACAO": "uf",
            "MUNICIPIO": "municipio",
            "NUMERO_REGISTRO_ESTABELECIMENTO": "nr_registro_estabelecimento",
            "STATUS_DO_REGISTRO": "status_registro",
            "CNPJ": "cnpj",
            "RAZAO_SOCIAL": "razao_social",
            "NOME_FANTASIA": "nome_fantasia",
            "AREA_ATUACAO": "area_atuacao",
            "ATIVIDADE": "atividade",
            "CLASSIFICACAO": "classificacao"
        }

        df = df.rename(columns=renames)

        str_cols = [c for c in df.columns if df[c].dtype == object]
        for col in str_cols:
            df[col] = df[col].str.strip()

        sem_registro = df["nr_registro_estabelecimento"].isna().sum() if "nr_registro_estabelecimento" in df.columns else "N/A"
        self.log.info(
            f"Transform SIPEAGRO concluído: {len(df)} estabelecimento(s). "
            f"Sem número de registro: {sem_registro}."
        )
        return df
