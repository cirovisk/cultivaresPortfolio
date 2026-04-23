import pytest
import pandas as pd
from src.pipeline.sigef import SigefExtractor
from src.pipeline.cleaners.sigef import clean_sigef_producao, clean_sigef_reserva_semente

@pytest.fixture
def mock_sigef_producao_raw():
    return pd.DataFrame([
        {
            "DS_SAFRA": "2023/2023",
            "DS_ESPECIE": "SOJA",
            "DS_CATEGORIA": "C1",
            "DS_CULTIVAR": "BRS 100",
            "DS_MUNICIPIO": "SORRISO",
            "DS_UF": "MT",
            "DS_STATUS": "ATIVO",
            "DT_PLANTIO": "01/10/2023",
            "DT_COLHEITA": "01/02/2024",
            "NR_AREA": "100,5",
            "NR_PRODUCAO_BRUTA": "300,0",
            "NR_PRODUCAO_EST": "280,0"
        }
    ])

@pytest.fixture
def mock_sigef_uso_proprio_raw():
    return pd.DataFrame([
        {
            "TIPOPERIODO": "ANO",
            "PERIODO": "2023",
            "AREATOTAL": "50,0",
            "MUNICIPIO": "SORRISO",
            "UF": "MT",
            "ESPECIE": "MILHO",
            "CULTIVAR": "DKB 255",
            "AREAPLANTADA": "40,0",
            "AREAESTIMADA": "45,0"
        }
    ])

def test_sigef_transform_producao(mock_sigef_producao_raw):
    df_clean = clean_sigef_producao(mock_sigef_producao_raw)
    
    assert not df_clean.empty
    row = df_clean.iloc[0]
    assert row["safra"] == "2023/2023"
    assert row["cultura"] == "soja"
    assert row["area_ha"] == 100.5
    assert row["producao_bruta_t"] == 300.0
    assert row["data_plantio"].year == 2023

def test_sigef_transform_uso_proprio(mock_sigef_uso_proprio_raw):
    df_clean = clean_sigef_reserva_semente(mock_sigef_uso_proprio_raw)
    
    assert not df_clean.empty
    row = df_clean.iloc[0]
    assert row["periodo"] == "2023"
    assert row["cultura"] == "milho"
    assert row["area_total_ha"] == 50.0
    assert row["area_plantada_ha"] == 40.0
