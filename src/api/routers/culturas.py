from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List

from db.manager import DimCultura
from api.dependencies import get_session
from api.schemas import CulturaBaseSchema, PaginatedResponse
from api.utils import paginate_query

router = APIRouter(prefix="/culturas", tags=["Culturas"])

@router.get("/", response_model=PaginatedResponse[CulturaBaseSchema])
def list_culturas(page: int = 1, page_size: int = 20, db: Session = Depends(get_session)):
    query = db.query(DimCultura)
    return paginate_query(query, page, page_size)

@router.get("/{cultura}", response_model=CulturaBaseSchema)
def get_cultura(cultura: str, db: Session = Depends(get_session)):
    cult = db.query(DimCultura).filter(DimCultura.nome_padronizado == cultura.lower().strip()).first()
    if not cult:
        raise HTTPException(status_code=404, detail="Cultura não encontrada")
    return cult
