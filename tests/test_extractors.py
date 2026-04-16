import pytest
import pandas as pd
from src.pipeline.cultivares import CultivaresExtractor
from src.pipeline.sidra import SidraExtractor
from src.pipeline.zarc import ZarcExtractor

def test_cultivares_transform(mock_cultivares_raw):
    # Inicializa extrator sem depender de cache_path para teste
    ext = CultivaresExtractor(use_cache=False, cache_path="")
    
    df_clean = ext.transform(mock_cultivares_raw)
    
    assert not df_clean.empty
    
    # Valida remocao de aspas e separacao de nome secundario
    row_soja = df_clean.iloc[0]
    assert row_soja["cultivar"] == "BONANZA"
    assert row_soja["nome_secundario"] == "BONA"
    
    row_milho = df_clean.iloc[1]
    assert row_milho["cultivar"] == "OURO"
    assert pd.isna(row_milho["nome_secundario"])
    
    # Valida setor
    assert row_soja["SETOR"] == "Público"
    assert row_milho["SETOR"] == "Privado"
    
    # Valida Datas e Anos
    assert row_soja["ANO"] == 2020
    assert row_soja["data_val"] > row_soja["data_reg"]
    
    # Valida normalizacao da cultura
    assert row_soja["cultura"] == "soja"
    assert row_milho["cultura"] == "milho"

def test_sidra_transform(mock_sidra_raw):
    ext = SidraExtractor(ano="2022")
    df_clean = ext.transform(mock_sidra_raw)
    
    assert not df_clean.empty
    # Pivot_table cria 1 linha por index, entao tem que ter 2 linhas (Mun A - soja, Mun B - trigo)
    assert len(df_clean) == 2
    
    row_mun_a = df_clean[df_clean["cod_municipio_ibge"] == "1200013"].iloc[0]
    
    # Valida conversao de '-' e '...' para NaN
    assert pd.isna(row_mun_a["area_colhida_ha"])
    assert pd.isna(row_mun_a["qtde_produzida_ton"])
    # Área plantada era 1000 numérico
    assert row_mun_a["area_plantada_ha"] == 1000.0

def test_zarc_transform(mock_zarc_raw):
    ext = ZarcExtractor()
    df_clean = ext.transform(mock_zarc_raw)
    
    assert not df_clean.empty
    
    # Valida renomeacao snake_case e encoding
    cols = df_clean.columns.tolist()
    assert "cod_municipio_ibge" in cols
    assert "riscoclima" in cols
    
    # Valida normalizacao cultura
    row = df_clean.iloc[0]
    assert row["cultura"] == "soja"
