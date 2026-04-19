import pandas as pd
import requests
from pathlib import Path
from .base_extractor import BaseExtractor

class CultivaresExtractor(BaseExtractor):
    """
    Extrator SNPC: Registro Nacional de Cultivares (MAPA).
    """

    SNPC_URL   = "https://sistemas.agricultura.gov.br/snpc/cultivarweb/cultivares_registradas.php"
    SNPC_QUERY = {"acao": "pesquisar", "postado": "1"}
    SNPC_DATA  = {"exportar": "csv"}
    HEADERS    = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
        "Referer": "https://sistemas.agricultura.gov.br/snpc/cultivarweb/cultivares_registradas.php?acao=pesquisar&postado=1",
        "Accept": "text/csv,text/html,*/*;q=0.9",
    }

    def __init__(self, use_cache: bool = True, cache_path: str = "data/relatorio_cultivares.csv"):
        super().__init__()
        self.use_cache = use_cache
        self.cache_path = Path(cache_path).resolve()

    def extract(self) -> pd.DataFrame:
        if self.use_cache and self.cache_path.exists():
            if not self.is_file_stale(str(self.cache_path), threshold_days=30):
                self.log.info(f"Usando cache local (atualizado): {self.cache_path}")
                return self._read_csv()
            self.log.info(f"Cache local expirado ({self.cache_path}). Atualizando...")

        self.log.info("Baixando CSV do SNPC/MAPA...")
        try:
            resp = requests.post(
                self.SNPC_URL,
                params=self.SNPC_QUERY,
                data=self.SNPC_DATA,
                headers=self.HEADERS,
                timeout=180,
                stream=True,
            )
            resp.raise_for_status()
            self.cache_path.parent.mkdir(parents=True, exist_ok=True)
            self.cache_path.write_bytes(resp.content)
            self.log.info(f"Download concluído: {self.cache_path}")
        except Exception as e:
            self.log.error(f"Falha no download: {e}")
            if self.cache_path.exists():
                return self._read_csv()
            return pd.DataFrame()

        return self._read_csv()

    def _read_csv(self) -> pd.DataFrame:
        return pd.read_csv(
            self.cache_path,
            dtype=str,
            encoding="utf-8",
            skipinitialspace=True,
            na_values=["", "NA", "N/A", "nan", "NaN", "NULL", "null", "-", "--"],
            keep_default_na=True,
        )
