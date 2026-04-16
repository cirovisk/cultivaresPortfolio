import os
import logging
from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, BigInteger, ForeignKey, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from sqlalchemy.sql import func

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# Configuração Padrão, mas pode ser sobrescrita por .env
DB_USER = os.getenv("POSTGRES_USER", "cultivares_user")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "cultivares_password")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "cultivares_db")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# ---------------------------------------------------------------------------
# ORM Models (Star Schema)
# ---------------------------------------------------------------------------

class DimCultura(Base):
    __tablename__ = "dim_cultura"
    id_cultura = Column(Integer, primary_key=True, index=True)
    nome_padronizado = Column(String, unique=True, index=True, nullable=False)
    
class DimMunicipio(Base):
    __tablename__ = "dim_municipio"
    id_municipio = Column(Integer, primary_key=True, index=True)
    codigo_ibge = Column(String, unique=True, index=True, nullable=False)
    nome = Column(String, nullable=False)
    uf = Column(String, nullable=True)

class DimMantenedor(Base):
    __tablename__ = "dim_mantenedor"
    id_mantenedor = Column(Integer, primary_key=True, autoincrement=True)
    nome = Column(String, unique=True, index=True, nullable=False)
    setor = Column(String, nullable=True)

class FatoCultivar(Base):
    __tablename__ = "fato_registro_cultivares"
    nr_registro = Column(BigInteger, primary_key=True, index=True)
    id_cultura = Column(Integer, ForeignKey("dim_cultura.id_cultura"))
    id_mantenedor = Column(Integer, ForeignKey("dim_mantenedor.id_mantenedor"))
    cultivar = Column(String, nullable=False)
    nome_secundario = Column(String, nullable=True)
    situacao = Column(String)
    nr_formulario = Column(BigInteger)
    data_reg = Column(DateTime)
    data_val = Column(DateTime)
    atualizado_em = Column(DateTime, server_default=func.now())

class FatoProducaoPAM(Base):
    __tablename__ = "fato_producao_pam"
    id_producao = Column(Integer, primary_key=True, autoincrement=True)
    id_cultura = Column(Integer, ForeignKey("dim_cultura.id_cultura"))
    id_municipio = Column(Integer, ForeignKey("dim_municipio.id_municipio"))
    ano = Column(Integer, nullable=False)
    area_plantada_ha = Column(Float)
    area_colhida_ha = Column(Float)
    qtde_produzida_ton = Column(Float)
    valor_producao_mil_reais = Column(Float)
    atualizado_em = Column(DateTime, server_default=func.now())

class FatoRiscoZARC(Base):
    __tablename__ = "fato_risco_zarc"
    id_zarc = Column(Integer, primary_key=True, autoincrement=True)
    id_cultura = Column(Integer, ForeignKey("dim_cultura.id_cultura"))
    id_municipio = Column(Integer, ForeignKey("dim_municipio.id_municipio"))
    tipo_solo = Column(String)
    periodo_plantio = Column(String)
    risco_climatico = Column(String)
    atualizado_em = Column(DateTime, server_default=func.now())

class FatoProducaoConab(Base):
    __tablename__ = "fato_producao_conab"
    id_conab = Column(Integer, primary_key=True, autoincrement=True)
    id_cultura = Column(Integer, ForeignKey("dim_cultura.id_cultura"))
    uf = Column(String(2))
    ano_agricola = Column(String(10))
    safra = Column(String(20))
    area_plantada_mil_ha = Column(Float)
    producao_mil_t = Column(Float)
    produtividade_t_ha = Column(Float)
    atualizado_em = Column(DateTime, server_default=func.now())

class FatoAgrofit(Base):
    __tablename__ = "fato_agrofit"
    id_agrofit = Column(Integer, primary_key=True, autoincrement=True)
    id_cultura = Column(Integer, ForeignKey("dim_cultura.id_cultura"))
    nr_registro = Column(String(20))
    marca_comercial = Column(String)
    ingrediente_ativo = Column(String)
    titular_registro = Column(String)
    classe = Column(String)
    situacao = Column(String)
    praga_comum = Column(String)
    atualizado_em = Column(DateTime, server_default=func.now())

# ---------------------------------------------------------------------------
# Funções de Gerenciamento
# ---------------------------------------------------------------------------

def init_db():
    """Cria as tabelas no banco de dados se não existirem."""
    log.info("Inicializando/Verificando banco de dados PostgreSQL...")
    try:
        Base.metadata.create_all(bind=engine)
        log.info("Schema validado com sucesso!")
    except Exception as e:
        log.error(f"Erro ao inicializar o banco de dados: {e}")
        raise

def get_db():
    """Generator de sessão SQLAlchemy."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
