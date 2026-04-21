"""
Router ZARC: Endpoints para Indicação de Cultivares e Risco Climático.

Utiliza DuckDB + Parquet (via duck_bridge) para servir dados massivos
sem sobrecarregar o PostgreSQL.
"""
from fastapi import APIRouter, Query
from typing import Optional
from api.duck_bridge import (
    query_indicacoes, count_indicacoes, get_indicacoes_stats, get_indicacoes_por_uf,
    query_risco, count_risco, get_risco_stats
)

router = APIRouter(prefix="/zarc", tags=["ZARC - Zoneamento Agrícola"])

# ==========================================
# 1. INDICAÇÕES DE CULTIVARES
# ==========================================

@router.get("/indicacoes/stats")
def zarc_indicacoes_stats():
    """Estatísticas gerais do dataset de Indicações."""
    return get_indicacoes_stats()

@router.get("/indicacoes/por-uf")
def zarc_indicacoes_por_uf():
    """Distribuição de indicações agrupadas por UF."""
    return get_indicacoes_por_uf()

@router.get("/indicacoes")
def listar_indicacoes_zarc(
    uf: Optional[str] = Query(None, description="Filtro por UF (ex: MT, RS)"),
    cultura: Optional[str] = Query(None, description="Filtro por cultura"),
    safra: Optional[str] = Query(None, description="Filtro por safra"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500)
):
    """Lista indicações de cultivares ZARC (196M+ registros)."""
    offset = (page - 1) * page_size
    total = count_indicacoes(uf=uf, cultura=cultura, safra=safra)
    items = query_indicacoes(uf=uf, cultura=cultura, safra=safra, limit=page_size, offset=offset)
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size if total > 0 else 0,
        "items": items
    }

# ==========================================
# 2. RISCO CLIMÁTICO
# ==========================================

@router.get("/risco/stats")
def zarc_risco_stats():
    """Estatísticas gerais do dataset de Risco Climático."""
    return get_risco_stats()

@router.get("/risco")
def listar_risco_zarc(
    codigo_ibge: Optional[str] = Query(None, description="Código IBGE do município"),
    cultura: Optional[str] = Query(None, description="Filtro por cultura"),
    id_solo: Optional[int] = Query(None, description="Tipo de solo (1, 2 ou 3)"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500)
):
    """
    Lista zoneamento de risco climático ZARC (1M+ registros).
    Retorna os riscos (%) para os 36 decêndios do ano.
    """
    offset = (page - 1) * page_size
    total = count_risco(codigo_ibge=codigo_ibge, cultura=cultura, id_solo=id_solo)
    items = query_risco(codigo_ibge=codigo_ibge, cultura=cultura, id_solo=id_solo, limit=page_size, offset=offset)
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size if total > 0 else 0,
        "items": items
    }
