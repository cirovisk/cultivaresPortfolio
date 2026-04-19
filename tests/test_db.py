import pytest
from sqlalchemy.exc import IntegrityError
from src.db.manager import DimCultura, DimMunicipio, FatoCultivar, FatoProducaoConab, FatoAgrofit, FatoPrecoConabMensal

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

def test_fato_producao_conab(db_session):
    cultura = DimCultura(nome_padronizado="milho")
    db_session.add(cultura)
    db_session.commit()
    
    fato = FatoProducaoConab(
        id_cultura=cultura.id_cultura,
        uf="MT",
        ano_agricola="2023/24",
        safra="1ª Safra",
        area_plantada_mil_ha=100.5,
        producao_mil_t=500.0,
        produtividade_t_ha=4.97
    )
    db_session.add(fato)
    db_session.commit()
    
    res = db_session.query(FatoProducaoConab).first()
    assert res.uf == "MT"
    assert res.id_cultura == cultura.id_cultura

def test_fato_agrofit(db_session):
    cultura = DimCultura(nome_padronizado="soja")
    db_session.add(cultura)
    db_session.commit()
    
    fato = FatoAgrofit(
        id_cultura=cultura.id_cultura,
        nr_registro="789",
        marca_comercial="TESTE",
        ingrediente_ativo="X",
        titular_registro="Empresa Y",
        classe="Herbicida",
        situacao="Registrado",
        praga_comum="Nema"
    )
    db_session.add(fato)
    db_session.commit()
    
    res = db_session.query(FatoAgrofit).first()
    assert res.nr_registro == "789"

def test_fato_precos_conab(db_session):
    cultura = DimCultura(nome_padronizado="soja")
    db_session.add(cultura)
    db_session.commit()
    
    fato = FatoPrecoConabMensal(
        id_cultura=cultura.id_cultura,
        uf="PR",
        ano=2024,
        mes=5,
        valor_kg=2.5,
        nivel_comercializacao="Produtor"
    )
    db_session.add(fato)
    db_session.commit()
    
    res = db_session.query(FatoPrecoConabMensal).first()
    assert res.valor_kg == 2.5
