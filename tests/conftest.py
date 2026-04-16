import pytest
import pandas as pd
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.db.manager import Base, init_db, DimCultura

# Configurando banco In-Memory do SQLite p/ testes rápidos isolados do Postgres de produção
TEST_DATABASE_URL = "sqlite:///:memory:"

@pytest.fixture(scope="session")
def engine():
    _engine = create_engine(TEST_DATABASE_URL, echo=False)
    Base.metadata.create_all(bind=_engine)
    yield _engine
    Base.metadata.drop_all(bind=_engine)
    _engine.dispose()

@pytest.fixture(scope="function")
def db_session(engine):
    connection = engine.connect()
    # Ponto de savepoint do sqlite para rollback rapido
    transaction = connection.begin()
    Session = sessionmaker(bind=connection)
    session = Session()

    yield session

    session.close()
    transaction.rollback()
    connection.close()

@pytest.fixture
def mock_sidra_raw():
    """Mock do JSON cru retornado pela API SIDRA e jogado num Dataframe."""
    data = {
        "D2N": ["Área plantada", "Área colhida", "Quantidade produzida", "Área plantada"],
        "V": ["1000", "-", "...", "2000"],
        "D1C": ["1200013", "1200013", "1200013", "1200054"],
        "D1N": ["Municipio A", "Municipio A", "Municipio A", "Municipio B"],
        "D3N": ["2022", "2022", "2022", "2022"],
        "cultura_raw": ["soja", "soja", "soja", "trigo"]
    }
    return pd.DataFrame(data)

@pytest.fixture
def mock_zarc_raw():
    """Mock de CSV consolidado do MAPA/Zarc cru."""
    data = {
        "cd_mun": [1200013, 1200054],
        "municipio": ["Municipio A", "Municipio B"],
        "cultura_raw": ["Soja", "Algodao"],
        "SOLO": ["Tipo 1", "Tipo 2"],
        "PERIODO": ["Outubro", "Novembro"],
        "RiscoClima": ["20%", "30%"]
    }
    return pd.DataFrame(data)

@pytest.fixture
def mock_cultivares_raw():
    """Mock de CSV de cultivares baixado usando pandas direto do SNPC."""
    data = {
        "CULTIVAR": ["'BONANZA' / BONA", '"OURO"'],
        "NOME COMUM": ["Soja", "Milho"],
        "GRUPO DA ESPÉCIE": ["SOJA", "MILHO"],
        "SITUAÇÃO": ["REGISTRADA", "REGISTRADA"],
        "MANTENEDOR (REQUERENTE) (NOME)": ["EMBRAPA CERRADOS", "EMPRESA PRIVADA X"],
        "Nº FORMULÁRIO": ["123", "456"],
        "Nº REGISTRO": ["10", "11"],
        "DATA DO REGISTRO": ["02/01/2020", "15/10/2015"],
        "DATA DE VALIDADE DO REGISTRO": ["01/01/2035", "14/10/2030"]
    }
    return pd.DataFrame(data)
