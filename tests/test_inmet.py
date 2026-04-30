import pytest
import pandas as pd
from datetime import datetime
from src.pipeline.sources.inmet import InmetPipeline

@pytest.fixture
def mock_inmet_hourly_data():
    return pd.DataFrame([
        {"DT_MEDICAO": "2024-01-01", "HR_MEDICAO": "0000", "CHUVA": "2.0", "TEM_MAX": "25.0", "TEM_MIN": "24.0", "TEM_INS": "24.5", "UMD_INS": "80"},
        {"DT_MEDICAO": "2024-01-01", "HR_MEDICAO": "0100", "CHUVA": "1.0", "TEM_MAX": "24.0", "TEM_MIN": "23.0", "TEM_INS": "23.5", "UMD_INS": "85"},
        {"DT_MEDICAO": "2024-01-02", "HR_MEDICAO": "0000", "CHUVA": "0.0", "TEM_MAX": "30.0", "TEM_MIN": "28.0", "TEM_INS": "29.0", "UMD_INS": "60"}
    ])

def test_inmet_aggregation(mock_inmet_hourly_data):
    # Simula dicionário retornado pelo extract
    dataframes = {"A101": mock_inmet_hourly_data}
    
    pipeline = InmetPipeline()
    df_daily = pipeline.clean(dataframes)
    
    assert not df_daily.empty
    assert len(df_daily) == 2
    
    # Valida Dia 1 (Soma e Médias)
    row1 = df_daily[df_daily["data"] == "2024-01-01"].iloc[0]
    assert row1["precipitacao_total_mm"] == 3.0
    assert row1["temp_max_c"] == 25.0
    assert row1["temp_min_c"] == 23.0
    assert row1["temp_media_c"] == 24.0 # (24.5 + 23.5) / 2
    assert row1["umidade_media"] == 82.5 # (80 + 85) / 2
    assert row1["estacao_id"] == "A101"

def test_inmet_fetch_chunks_logic():
    from unittest.mock import patch, MagicMock
    
    with patch("requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = [{"DT_MEDICAO": "2024-01-01", "HR_MEDICAO": "1200", "CHUVA": "0"}]
        
        ext = InmetPipeline(days_history=400) # Força 2 chunks (> 365 dias)
        start = datetime(2023, 1, 1)
        end = datetime(2024, 2, 5)
        
        df = ext._fetch_station_data_in_chunks("A101", start, end)
        
        # Deve fazer 2 chamadas (2023-01-01 a 2024-01-01 e 2024-01-02 a 2024-02-05)
        assert mock_get.call_count == 2
        assert not df.empty
