import pytest
from sqlalchemy.exc import IntegrityError
from src.db.manager import DimCultura, DimMunicipio, FatoCultivar

def test_inserir_dim_cultura(db_session):
    # Inserção de uma cultura limpa
    nova_cultura = DimCultura(nome_padronizado="soja")
    db_session.add(nova_cultura)
    db_session.commit()
    
    cult = db_session.query(DimCultura).filter_by(nome_padronizado="soja").first()
    assert cult is not None
    assert cult.id_cultura is not None

def test_dim_cultura_unique_constraint(db_session):
    # Testar unicidade do nome
    cult1 = DimCultura(nome_padronizado="milho")
    db_session.add(cult1)
    db_session.commit()
    
    cult2 = DimCultura(nome_padronizado="milho")
    db_session.add(cult2)
    with pytest.raises(IntegrityError):
        db_session.commit()
    
    db_session.rollback()

def test_star_schema_relationships(db_session):
    # Setup Dimensoes
    cultura = DimCultura(nome_padronizado="trigo")
    mun = DimMunicipio(codigo_ibge="1200054", nome="Cidade Teste", uf="AC")
    
    db_session.add(cultura)
    db_session.add(mun)
    db_session.commit()
    
    # Setup Fato com FKs corretas (utilizando o Fato original se existir)
    # Como FatoCultivar nao possui municipio, usaremos cultura
    
    # Import apropriado
    from src.db.manager import FatoCultivar
    
    fato = FatoCultivar(
        nr_registro=12345,
        id_cultura=cultura.id_cultura,
        cultivar="TRIGO MEGA",
        situacao="REGISTRADA"
    )
    
    db_session.add(fato)
    db_session.commit()
    
    # Valida integridade
    fato_db = db_session.query(FatoCultivar).filter_by(nr_registro=12345).first()
    assert fato_db.cultivar == "TRIGO MEGA"
    assert fato_db.id_cultura == cultura.id_cultura
