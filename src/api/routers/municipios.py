from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional

from db.manager import DimMunicipio
from api.dependencies import get_session
from api.schemas import MunicipioBaseSchema, PaginatedResponse
from api.utils import paginate_query

router = APIRouter(prefix="/municipios", tags=["Municípios"])

@router.get("/", response_model=PaginatedResponse[MunicipioBaseSchema])
def list_municipios(uf: Optional[str] = None, page: int = 1, page_size: int = 50, db: Session = Depends(get_session)):
    query = db.query(DimMunicipio)
    if uf:
        query = query.filter(DimMunicipio.uf == uf.upper())
    return paginate_query(query, page, page_size)

@router.get("/{codigo_ibge}", response_model=MunicipioBaseSchema)
def get_municipio(codigo_ibge: str, db: Session = Depends(get_session)):
    mun = db.query(DimMunicipio).filter(DimMunicipio.codigo_ibge == codigo_ibge).first()
    if not mun:
        raise HTTPException(status_code=404, detail="Município não encontrado")
    return mun
