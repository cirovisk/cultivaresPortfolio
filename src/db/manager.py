import os
import logging
from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, BigInteger, ForeignKey, DateTime, UniqueConstraint
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from sqlalchemy.sql import func

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# Configuração: Credenciais via variáveis de ambiente
DB_USER = os.getenv("POSTGRES_USER", "cultivares_user")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "cultivares_password")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "cultivares_db")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Schema: Modelos ORM (Star Schema)

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
    data_modificacao = Column(DateTime, server_default=func.now(), onupdate=func.now())

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
    data_modificacao = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    __table_args__ = (UniqueConstraint('id_cultura', 'id_municipio', 'ano', name='_cultura_municipio_ano_uc'),)

class FatoRiscoZARC(Base):
    __tablename__ = "fato_risco_zarc"
    id_zarc = Column(Integer, primary_key=True, autoincrement=True)
    id_cultura = Column(Integer, ForeignKey("dim_cultura.id_cultura"))
    id_municipio = Column(Integer, ForeignKey("dim_municipio.id_municipio"))
    tipo_solo = Column(String)
    periodo_plantio = Column(String)
    risco_climatico = Column(String)
    data_modificacao = Column(DateTime, server_default=func.now(), onupdate=func.now())

    __table_args__ = (UniqueConstraint('id_cultura', 'id_municipio', 'tipo_solo', 'periodo_plantio', name='_zarc_uc'),)

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
    data_modificacao = Column(DateTime, server_default=func.now(), onupdate=func.now())

    __table_args__ = (UniqueConstraint('id_cultura', 'uf', 'ano_agricola', 'safra', name='_conab_prod_uc'),)

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
    data_modificacao = Column(DateTime, server_default=func.now(), onupdate=func.now())

    __table_args__ = (UniqueConstraint('id_cultura', 'nr_registro', 'marca_comercial', 'praga_comum', name='_agrofit_uc'),)

class FatoPrecoConabMensal(Base):
    __tablename__ = "fato_precos_conab_mensal"
    id_preco = Column(Integer, primary_key=True, autoincrement=True)
    id_cultura = Column(Integer, ForeignKey("dim_cultura.id_cultura"))
    id_municipio = Column(Integer, ForeignKey("dim_municipio.id_municipio"), nullable=True)
    uf = Column(String(2))
    ano = Column(Integer, nullable=False)
    mes = Column(Integer, nullable=False)
    valor_kg = Column(Float)
    nivel_comercializacao = Column(String)
    data_modificacao = Column(DateTime, server_default=func.now(), onupdate=func.now())

    __table_args__ = (UniqueConstraint('id_cultura', 'id_municipio', 'uf', 'ano', 'mes', 'nivel_comercializacao', name='_conab_preco_mensal_uc'),)

class FatoPrecoConabSemanal(Base):
    __tablename__ = "fato_precos_conab_semanal"
    id_preco_semanal = Column(Integer, primary_key=True, autoincrement=True)
    id_cultura = Column(Integer, ForeignKey("dim_cultura.id_cultura"))
    id_municipio = Column(Integer, ForeignKey("dim_municipio.id_municipio"), nullable=True)
    uf = Column(String(2))
    ano = Column(Integer, nullable=False)
    mes = Column(Integer, nullable=False)
    semana = Column(Integer)
    data_referencia = Column(String)
    valor_kg = Column(Float)
    nivel_comercializacao = Column(String)
    data_modificacao = Column(DateTime, server_default=func.now(), onupdate=func.now())

    __table_args__ = (UniqueConstraint('id_cultura', 'id_municipio', 'uf', 'ano', 'mes', 'semana', 'nivel_comercializacao', name='_conab_preco_semanal_uc'),)

class FatoFertilizante(Base):
    __tablename__ = "fato_fertilizantes_estabelecimentos"
    id_fertilizante = Column(Integer, primary_key=True, autoincrement=True)
    uf = Column(String(2))
    municipio = Column(String)
    id_municipio = Column(Integer, ForeignKey("dim_municipio.id_municipio"), nullable=True)
    nr_registro_estabelecimento = Column(String, unique=True, index=True, nullable=False)
    status_registro = Column(String)
    cnpj = Column(String)
    razao_social = Column(String)
    nome_fantasia = Column(String)
    area_atuacao = Column(String)
    atividade = Column(String)
    classificacao = Column(String)
    data_modificacao = Column(DateTime, server_default=func.now(), onupdate=func.now())
 
class FatoSigefProducao(Base):
    __tablename__ = "fato_sigef_producao"
    id_sigef_producao = Column(Integer, primary_key=True, autoincrement=True)
    id_cultura = Column(Integer, ForeignKey("dim_cultura.id_cultura"))
    id_municipio = Column(Integer, ForeignKey("dim_municipio.id_municipio"))
    safra = Column(String(20))
    especie = Column(String)
    categoria = Column(String)
    cultivar_raw = Column(String)
    status = Column(String)
    data_plantio = Column(DateTime)
    data_colheita = Column(DateTime)
    area_ha = Column(Float)
    producao_bruta_t = Column(Float)
    producao_est_t = Column(Float)
    data_modificacao = Column(DateTime, server_default=func.now(), onupdate=func.now())
 
    __table_args__ = (UniqueConstraint('id_cultura', 'id_municipio', 'safra', 'especie', 'cultivar_raw', 'categoria', name='_sigef_prod_uc'),)
 
class FatoSigefUsoProprio(Base):
    __tablename__ = "fato_sigef_uso_proprio"
    id_sigef_uso_proprio = Column(Integer, primary_key=True, autoincrement=True)
    id_cultura = Column(Integer, ForeignKey("dim_cultura.id_cultura"))
    id_municipio = Column(Integer, ForeignKey("dim_municipio.id_municipio"))
    tipo_periodo = Column(String)
    periodo = Column(String)
    especie = Column(String)
    cultivar_raw = Column(String)
    area_total_ha = Column(Float)
    area_plantada_ha = Column(Float)
    area_estimada_ha = Column(Float)
    data_modificacao = Column(DateTime, server_default=func.now(), onupdate=func.now())
 
    __table_args__ = (UniqueConstraint('id_cultura', 'id_municipio', 'periodo', 'especie', 'cultivar_raw', name='_sigef_uso_uc'),)

# Operações: Gerenciamento de Conexão

def init_db():
    """DDL: Sincronização de tabelas com o banco."""
    log.info("Inicializando/Verificando banco de dados PostgreSQL...")
    try:
        Base.metadata.create_all(bind=engine)
        log.info("Schema validado com sucesso!")
    except Exception as e:
        log.error(f"Erro ao inicializar o banco de dados: {e}")
        raise

def get_db():
    """Session: Generator de conexão SQLAlchemy."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
