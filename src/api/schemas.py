from pydantic import BaseModel, ConfigDict
from typing import List, Dict, Optional, Generic, TypeVar
from datetime import date

T = TypeVar("T")

class PaginatedResponse(BaseModel, Generic[T]):
    total: int
    page: int
    page_size: int
    total_pages: int
    items: List[T]


# ==========================================
# 1. SCHEMAS SIMPLES (Pesquisas Rápidas e Listagens)
# ==========================================

class CulturaBaseSchema(BaseModel):
    id_cultura: int
    nome_padronizado: str
    
    model_config = ConfigDict(from_attributes=True)

class MunicipioBaseSchema(BaseModel):
    id_municipio: int
    codigo_ibge: str
    nome: str
    uf: Optional[str] = None
    
    model_config = ConfigDict(from_attributes=True)

class ProducaoPAMSimplesSchema(BaseModel):
    ano: int
    area_plantada_ha: Optional[float] = None
    qtde_produzida_ton: Optional[float] = None

class ProducaoPAMSchema(BaseModel):
    id_producao: int
    ano: int
    area_plantada_ha: Optional[float] = None
    area_colhida_ha: Optional[float] = None
    qtde_produzida_ton: Optional[float] = None
    valor_producao_mil_reais: Optional[float] = None
    # Adicionamos cultura e municipio resolvidos no router para facilitar a resposta
    cultura: str
    municipio_nome: str
    uf: Optional[str] = None
    
    model_config = ConfigDict(from_attributes=True)

class ProducaoConabSchema(BaseModel):
    id_conab: int
    uf: str
    ano_agricola: str
    safra: str
    area_plantada_mil_ha: Optional[float] = None
    producao_mil_t: Optional[float] = None
    produtividade_t_ha: Optional[float] = None
    cultura: str
    
    model_config = ConfigDict(from_attributes=True)

class SigefProducaoSchema(BaseModel):
    id_sigef_producao: int
    safra: str
    especie: str
    categoria: str
    area_ha: Optional[float] = None
    producao_bruta_t: Optional[float] = None
    producao_est_t: Optional[float] = None
    cultura: str
    municipio_nome: str
    uf: Optional[str] = None
    
    model_config = ConfigDict(from_attributes=True)

class AgrofitSchema(BaseModel):
    id_agrofit: int
    nr_registro: Optional[str] = None
    marca_comercial: Optional[str] = None
    classe: Optional[str] = None
    praga_comum: Optional[str] = None
    cultura: str
    
    model_config = ConfigDict(from_attributes=True)

class FertilizanteSchema(BaseModel):
    id_fertilizante: int
    uf: Optional[str] = None
    municipio: Optional[str] = None
    nr_registro_estabelecimento: str
    razao_social: Optional[str] = None
    area_atuacao: Optional[str] = None
    atividade: Optional[str] = None
    
    model_config = ConfigDict(from_attributes=True)

class MetereologiaSchema(BaseModel):
    id_meteo: int
    data: date
    precipitacao_total_mm: Optional[float] = None
    temp_media_c: Optional[float] = None
    umidade_media: Optional[float] = None
    municipio_nome: str
    uf: Optional[str] = None
    
    model_config = ConfigDict(from_attributes=True)

# ==========================================
# 2. SCHEMAS COMPOSTOS (Visões Analíticas Avançadas)
# ==========================================

# 2.1 RaioXAgroMunicipal (ZARC + PAM + Meteorologia)
class RaioXAgroMunicipalSchema(BaseModel):
    municipio: str
    uf: str
    cultura: str
    ano: int
    resultado_safra: ProducaoPAMSimplesSchema
    risco_zarc_predominante: Optional[str] = None
    resumo_climatico: Optional[Dict[str, float]] = None # Ex: {"temp_media": 25.5, "precipitacao_anual_mm": 1200.0}
    ocorreu_quebra_safra: Optional[bool] = None

# 2.2 DossieInsumosCultura (Cultivares + SIGEF + Agrofit)
class DossieInsumosCulturaSchema(BaseModel):
    cultura: str
    cultivares_ativos: int            
    volume_sementes_sigef_ton: Optional[float] = None  
    defensivos_recomendados: List[str] 
    principais_pragas_alvo: List[str] 
    grau_de_tecnologia: Optional[str] = None

# 2.3 ViabilidadeEconomica (PAM + Preços CONAB)
class ViabilidadeEconomicaSchema(BaseModel):
    ano: int
    cultura: str
    uf: str
    producao_total_ton: Optional[float] = None
    preco_medio_anual_ton: Optional[float] = None
    valor_teto_atingido: Optional[float] = None
    vgb_apurado_milhoes: Optional[float] = None
    renda_media_hectare: Optional[float] = None

# 2.4 JanelaDeAplicacao (Fertilizantes + Meteorologia)
class JanelaDeAplicacaoSchema(BaseModel):
    municipio: str
    uf: str
    mes_referencia: str
    estabelecimentos_insumos: int     
    acumulado_chuvas_mm: Optional[float] = None
    janela_perfeita_aplicacao: Optional[bool] = None   
    capacidade_de_atendimento: Optional[str] = None

# 2.5 AuditoriaEstimativas (PAM vs CONAB)
class AuditoriaEstimativasSchema(BaseModel):
    ano_safra: str
    uf: str
    cultura: str
    estimativa_conab_mil_t: Optional[float] = None
    realizado_ibge_pam_mil_t: Optional[float] = None
    variacao_margem_erro: Optional[float] = None
    acuracidade_relatorio: Optional[str] = None
