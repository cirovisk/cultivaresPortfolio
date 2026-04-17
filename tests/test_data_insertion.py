import pytest
import pandas as pd
from src.db.manager import (
    DimCultura, DimMunicipio, DimMantenedor, 
    FatoCultivar, FatoProducaoPAM, FatoRiscoZARC, 
    FatoProducaoConab, FatoAgrofit, FatoPrecoConabMensal
)

def test_insertion_fato_cultivar(db_session):
    """Verifica se os campos do DataFrame são mapeados corretamente para FatoCultivar."""
    # Setup dims
    cult = DimCultura(nome_padronizado="soja")
    mant = DimMantenedor(nome="EMBRAPA", setor="Público")
    db_session.add_all([cult, mant])
    db_session.commit()
    
    # Simulação de DataFrame vindo do transform
    data = [{
        "nr_registro": 1001,
        "id_cultura": cult.id_cultura,
        "id_mantenedor": mant.id_mantenedor,
        "cultivar": "SOJA TESTE",
        "situacao": "REGISTRADA",
        "data_reg": pd.to_datetime("2020-01-01")
    }]
    
    # Simulação de inserção manual (equivalente ao que o upsert faria)
    fato = FatoCultivar(**data[0])
    db_session.add(fato)
    db_session.commit()
    
    res = db_session.query(FatoCultivar).filter_by(nr_registro=1001).first()
    assert res.cultivar == "SOJA TESTE"
    assert res.id_cultura == cult.id_cultura

def test_insertion_fato_pam(db_session):
    """Verifica inserção dos dados do IBGE/SIDRA."""
    cult = DimCultura(nome_padronizado="milho")
    mun = DimMunicipio(codigo_ibge="1200013", nome="Mun A", uf="AC")
    db_session.add_all([cult, mun])
    db_session.commit()
    
    data = {
        "id_cultura": cult.id_cultura,
        "id_municipio": mun.id_municipio,
        "ano": 2022,
        "area_plantada_ha": 500.0,
        "qtde_produzida_ton": 1500.0
    }
    
    fato = FatoProducaoPAM(**data)
    db_session.add(fato)
    db_session.commit()
    
    res = db_session.query(FatoProducaoPAM).filter_by(ano=2022).first()
    assert res.qtde_produzida_ton == 1500.0

def test_insertion_fato_zarc(db_session):
    """Verifica inserção dos dados de risco climático (ZARC)."""
    cult = DimCultura(nome_padronizado="soja")
    mun = DimMunicipio(codigo_ibge="1200013", nome="Mun A", uf="AC")
    db_session.add_all([cult, mun])
    db_session.commit()
    
    data = {
        "id_cultura": cult.id_cultura,
        "id_municipio": mun.id_municipio,
        "tipo_solo": "Tipo 1",
        "periodo_plantio": "11-20",
        "risco_climatico": "20%"
    }
    
    fato = FatoRiscoZARC(**data)
    db_session.add(fato)
    db_session.commit()
    
    res = db_session.query(FatoRiscoZARC).first()
    assert res.risco_climatico == "20%"

def test_insertion_fato_conab(db_session):
    """Verifica inserção dos dados de produção da CONAB."""
    cult = DimCultura(nome_padronizado="trigo")
    db_session.add(cult)
    db_session.commit()
    
    data = {
        "id_cultura": cult.id_cultura,
        "uf": "RS",
        "ano_agricola": "2023/24",
        "safra": "Inverno",
        "area_plantada_mil_ha": 10.5,
        "producao_mil_t": 30.0
    }
    
    fato = FatoProducaoConab(**data)
    db_session.add(fato)
    db_session.commit()
    
    res = db_session.query(FatoProducaoConab).first()
    assert res.uf == "RS"
    assert res.producao_mil_t == 30.0

def test_insertion_fato_agrofit(db_session):
    """Verifica inserção dos dados de defensivos (Agrofit)."""
    cult = DimCultura(nome_padronizado="algodão")
    db_session.add(cult)
    db_session.commit()
    
    data = {
        "id_cultura": cult.id_cultura,
        "nr_registro": "REG-123",
        "marca_comercial": "AGRO-X",
        "ingrediente_ativo": "Alfa",
        "praga_comum": "Tripes"
    }
    
    fato = FatoAgrofit(**data)
    db_session.add(fato)
    db_session.commit()
    
    res = db_session.query(FatoAgrofit).first()
    assert res.marca_comercial == "AGRO-X"

def test_insertion_fato_precos(db_session):
    """Verifica inserção dos dados de preços (CONAB)."""
    cult = DimCultura(nome_padronizado="soja")
    db_session.add(cult)
    db_session.commit()
    
    data = {
        "id_cultura": cult.id_cultura,
        "uf": "MT",
        "ano": 2024,
        "mes": 2,
        "valor_kg": 1.45,
        "nivel_comercializacao": "Produtor"
    }
    
    fato = FatoPrecoConabMensal(**data)
    db_session.add(fato)
    db_session.commit()
    
    res = db_session.query(FatoPrecoConabMensal).first()
    assert res.valor_kg == 1.45
