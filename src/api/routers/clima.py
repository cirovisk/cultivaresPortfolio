from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from datetime import date
from typing import List, Optional

from db.manager import FatoMeteorologia, DimMunicipio
from api.dependencies import get_session
from api.schemas import MetereologiaSchema, PaginatedResponse
from api.utils import paginate_query

router = APIRouter(prefix="/clima", tags=["Clima"])

@router.get("/", response_model=PaginatedResponse[MetereologiaSchema])
def get_clima(codigo_ibge: Optional[str] = None, data_inicio: Optional[date] = None, data_fim: Optional[date] = None, page: int = 1, page_size: int = 20, db: Session = Depends(get_session)):
    query = db.query(
        FatoMeteorologia.id_meteo,
        FatoMeteorologia.data,
        FatoMeteorologia.precipitacao_total_mm,
        FatoMeteorologia.temp_media_c,
        FatoMeteorologia.umidade_media,
        DimMunicipio.nome.label("municipio_nome"),
        DimMunicipio.uf
    ).join(DimMunicipio, FatoMeteorologia.id_municipio == DimMunicipio.id_municipio)

    if codigo_ibge:
        query = query.filter(DimMunicipio.codigo_ibge == codigo_ibge)
    if data_inicio:
        query = query.filter(FatoMeteorologia.data >= data_inicio)
    if data_fim:
        query = query.filter(FatoMeteorologia.data <= data_fim)

    return paginate_query(query, page, page_size)
