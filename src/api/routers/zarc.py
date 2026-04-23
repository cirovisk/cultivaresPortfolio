from fastapi import APIRouter, Query, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import Optional, List

from db.manager import FatoRiscoZARC, DimMunicipio, DimCultura
from api.dependencies import get_session

router = APIRouter(prefix="/zarc", tags=["ZARC - Zoneamento Agrícola"])

# ==========================================
# 1. INDICAÇÕES DE CULTIVARES
# ==========================================

@router.get("/indicacoes/stats")
def zarc_indicacoes_stats():
    """Endpoint desativado na migração para Postgres (Indicações Parquet removidas)."""
    return {"status": "indisponível", "msg": "Dados de indicações brutas (196M) removidos para simplificação do banco Postgres."}

@router.get("/indicacoes")
def listar_indicacoes_zarc():
    return {"items": [], "total": 0, "msg": "Este endpoint migrou para o PostgreSQL e foca apenas em riscos climáticos."}

# ==========================================
# 2. RISCO CLIMÁTICO
# ==========================================

@router.get("/risco")
def listar_risco_zarc(
    codigo_ibge: Optional[str] = Query(None, description="Código IBGE do município"),
    cultura: Optional[str] = Query(None, description="Filtro por cultura"),
    id_solo: Optional[str] = Query(None, description="Tipo de solo (1, 2 ou 3)"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_session)
):
    """
    Lista zoneamento de risco climático ZARC a partir do PostgreSQL.
    """
    query = db.query(
        FatoRiscoZARC.periodo_plantio,
        FatoRiscoZARC.tipo_solo,
        FatoRiscoZARC.risco_climatico,
        DimCultura.nome_padronizado.label("cultura"),
        DimMunicipio.nome.label("municipio"),
        DimMunicipio.uf
    ).join(DimCultura, FatoRiscoZARC.id_cultura == DimCultura.id_cultura)\
     .join(DimMunicipio, FatoRiscoZARC.id_municipio == DimMunicipio.id_municipio)

    if codigo_ibge:
        query = query.filter(DimMunicipio.codigo_ibge == codigo_ibge)
    if cultura:
        query = query.filter(DimCultura.nome_padronizado == cultura.lower())
    if id_solo:
        query = query.filter(FatoRiscoZARC.tipo_solo == str(id_solo))

    total = query.count()
    offset = (page - 1) * page_size
    items = query.offset(offset).limit(page_size).all()

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size if total > 0 else 0,
        "items": [dict(r._mapping) for r in items]
    }
