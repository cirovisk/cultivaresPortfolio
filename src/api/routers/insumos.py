from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional

from db.manager import FatoAgrofit, FatoFertilizante, DimCultura
from api.dependencies import get_session
from api.schemas import AgrofitSchema, FertilizanteSchema, PaginatedResponse
from api.utils import paginate_query

router = APIRouter(prefix="/insumos", tags=["Insumos"])

@router.get("/agrofit", response_model=PaginatedResponse[AgrofitSchema])
def get_agrofit(cultura: Optional[str] = None, classe: Optional[str] = None, page: int = 1, page_size: int = 20, db: Session = Depends(get_session)):
    query = db.query(
        FatoAgrofit.id_agrofit,
        FatoAgrofit.nr_registro,
        FatoAgrofit.marca_comercial,
        FatoAgrofit.classe,
        FatoAgrofit.praga_comum,
        DimCultura.nome_padronizado.label("cultura")
    ).join(DimCultura, FatoAgrofit.id_cultura == DimCultura.id_cultura)

    if cultura:
        query = query.filter(DimCultura.nome_padronizado == cultura.lower())
    if classe:
        query = query.filter(FatoAgrofit.classe == classe.upper())

    return paginate_query(query, page, page_size)

@router.get("/fertilizantes", response_model=PaginatedResponse[FertilizanteSchema])
def get_fertilizantes(uf: Optional[str] = None, atividade: Optional[str] = None, page: int = 1, page_size: int = 20, db: Session = Depends(get_session)):
    query = db.query(
        FatoFertilizante.id_fertilizante,
        FatoFertilizante.uf,
        FatoFertilizante.municipio,
        FatoFertilizante.nr_registro_estabelecimento,
        FatoFertilizante.razao_social,
        FatoFertilizante.area_atuacao,
        FatoFertilizante.atividade
    )

    if uf:
        query = query.filter(FatoFertilizante.uf == uf.upper())
    if atividade:
        query = query.filter(FatoFertilizante.atividade == atividade.upper())

    return paginate_query(query, page, page_size)
