import pandas as pd
import unicodedata

def normalize_string(series: pd.Series) -> pd.Series:
    """Normalização: Padronização de nomes (remuneração de acentos, lowercase)."""
    def remove_accents(input_str):
        if not isinstance(input_str, str):
            return input_str
        nfkd_form = unicodedata.normalize('NFKD', input_str)
        return u"".join([c for c in nfkd_form if not unicodedata.combining(c)]).lower()

    return series.apply(remove_accents).str.strip()
