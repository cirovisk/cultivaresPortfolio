import pytest
import pandas as pd
from src.main import preencher_dimensao_cultura, preencher_dimensao_mantenedor, preencher_dimensao_municipio
from src.db.manager import DimCultura, DimMunicipio, DimMantenedor

def test_preencher_dimensao_cultura(db_session):
    """Testa se a lista de culturas alvo é inserida corretamente na dimensão."""
    culturas = ["Soja", "Milho", "Trigo"]
    mapping = preencher_dimensao_cultura(db_session, culturas)
    
    assert len(mapping) == 3
    assert "soja" in mapping
    assert "milho" in mapping
    
    # Verifica se persistiu
    rows = db_session.query(DimCultura).all()
    assert len(rows) == 3

def test_preencher_dimensao_mantenedor(db_session):
    """Testa o mapeamento de mantenedores a partir do DataFrame de cultivares."""
    df_cult = pd.DataFrame({
        "mantenedor": ["Empresa A", "Empresa B", "Empresa A"],
        "SETOR": ["Privado", "Público", "Privado"]
    })
    
    mapping = preencher_dimensao_mantenedor(db_session, df_cult)
    
    assert len(mapping) == 2
    assert mapping["Empresa A"] is not None
    
    rows = db_session.query(DimMantenedor).all()
    assert len(rows) == 2

def test_preencher_dimensao_municipio(db_session):
    """Testa o mapeamento de municípios a partir de PAM e ZARC."""
    df_pam = pd.DataFrame({
        "cod_municipio_ibge": ["1200013", "1200054"],
        "municipio_nome": ["Mun A", "Mun B"]
    })
    df_zarc = pd.DataFrame({
        "cod_municipio_ibge": ["1200013", "1300021"],
        "municipio": ["Mun A", "Mun C"]
    })
    
    mapping = preencher_dimensao_municipio(db_session, df_pam, df_zarc)
    
    # Mun A (comum), Mun B (PAM), Mun C (ZARC) -> 3 muns
    assert len(mapping) == 3
    assert "1200013" in mapping
    assert "1300021" in mapping
    
    rows = db_session.query(DimMunicipio).all()
    assert len(rows) == 3

def test_get_cultura_id_logic():
    """Testa a lógica de busca flexível de cultura por nome."""
    from src.main import get_cultura_id
    
    mapping = {
        "soja": 1,
        "milho": 2,
        "cana-de-açúcar": 3
    }
    
    # Match exato
    assert get_cultura_id("soja", mapping) == 1
    
    # Match com variação de case
    assert get_cultura_id("SOJA", mapping) == 1
    
    # Match com acentuação/flexibilidade
    assert get_cultura_id("Cana de Acucar", mapping) == 3
    
    # Match parcial
    assert get_cultura_id("Milho Verdin", mapping) == 2
    
    # Sem match
    assert get_cultura_id("Arroz", mapping) is None
