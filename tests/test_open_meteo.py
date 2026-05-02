import pytest
import pandas as pd
from datetime import datetime
from src.pipeline.sources.open_meteo import OpenMeteoPipeline

@pytest.fixture
def mock_open_meteo_daily_data():
    return pd.DataFrame([
        {"time": "2024-01-01", "precipitation_sum": 2.0, "temperature_2m_max": 25.0, "temperature_2m_min": 24.0, "temperature_2m_mean": 24.5},
        {"time": "2024-01-02", "precipitation_sum": 0.0, "temperature_2m_max": 30.0, "temperature_2m_min": 28.0, "temperature_2m_mean": 29.0}
    ])

def test_open_meteo_aggregation(mock_open_meteo_daily_data):
    dataframes = {1: mock_open_meteo_daily_data}
    
    pipeline = OpenMeteoPipeline()
    df_daily = pipeline.clean(dataframes)
    
    assert not df_daily.empty
    assert len(df_daily) == 2
    
    row1 = df_daily[df_daily["data"] == "2024-01-01"].iloc[0]
    assert row1["precipitacao_total_mm"] == 2.0
    assert row1["temp_max_c"] == 25.0
    assert row1["temp_min_c"] == 24.0
    assert row1["temp_media_c"] == 24.5
    assert row1["estacao_id"] == "OPEN-METEO"
    assert row1["id_municipio"] == 1

def test_open_meteo_fetch_logic():
    from unittest.mock import patch
    
    with patch("requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {
            "daily": {"time": ["2024-01-01"], "precipitation_sum": [0.0], "temperature_2m_max": [30.0], "temperature_2m_min": [20.0], "temperature_2m_mean": [25.0]}
        }
        
        ext = OpenMeteoPipeline(days_history=10)
        
        mun_coords = {1: {"lat": -23.0, "lon": -46.0}}
        df_dict = ext.extract(mun_coords=mun_coords)
        
        assert mock_get.call_count == 1
        assert 1 in df_dict
        assert not df_dict[1].empty
