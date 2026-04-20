from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import select
from typing import List, Optional

from db.manager import FatoProducaoPAM, FatoProducaoConab, FatoSigefProducao, DimCultura, DimMunicipio
from api.dependencies import get_session
from api.schemas import ProducaoPAMSchema, ProducaoConabSchema, SigefProducaoSchema, PaginatedResponse
from api.utils import paginate_query

router = APIRouter(prefix="/producao", tags=["Produção"])

@router.get("/pam", response_model=PaginatedResponse[ProducaoPAMSchema])
def get_pam(cultura: Optional[str] = None, uf: Optional[str] = None, ano: Optional[int] = None, page: int = 1, page_size: int = 20, db: Session = Depends(get_session)):
    query = db.query(
        FatoProducaoPAM.id_producao,
        FatoProducaoPAM.ano,
        FatoProducaoPAM.area_plantada_ha,
        FatoProducaoPAM.area_colhida_ha,
        FatoProducaoPAM.qtde_produzida_ton,
        FatoProducaoPAM.valor_producao_mil_reais,
        DimCultura.nome_padronizado.label("cultura"),
        DimMunicipio.nome.label("municipio_nome"),
        DimMunicipio.uf
    ).join(DimCultura, FatoProducaoPAM.id_cultura == DimCultura.id_cultura)\
     .join(DimMunicipio, FatoProducaoPAM.id_municipio == DimMunicipio.id_municipio)

    if cultura:
        query = query.filter(DimCultura.nome_padronizado == cultura.lower())
    if uf:
        query = query.filter(DimMunicipio.uf == uf.upper())
    if ano:
        query = query.filter(FatoProducaoPAM.ano == ano)

    return paginate_query(query, page, page_size)

@router.get("/conab", response_model=PaginatedResponse[ProducaoConabSchema])
def get_conab(cultura: Optional[str] = None, uf: Optional[str] = None, ano_agricola: Optional[str] = None, page: int = 1, page_size: int = 20, db: Session = Depends(get_session)):
    query = db.query(
        FatoProducaoConab.id_conab,
        FatoProducaoConab.uf,
        FatoProducaoConab.ano_agricola,
        FatoProducaoConab.safra,
        FatoProducaoConab.area_plantada_mil_ha,
        FatoProducaoConab.producao_mil_t,
        FatoProducaoConab.produtividade_t_ha,
        DimCultura.nome_padronizado.label("cultura")
    ).join(DimCultura, FatoProducaoConab.id_cultura == DimCultura.id_cultura)

    if cultura:
        query = query.filter(DimCultura.nome_padronizado == cultura.lower())
    if uf:
        query = query.filter(FatoProducaoConab.uf == uf.upper())
    if ano_agricola:
        query = query.filter(FatoProducaoConab.ano_agricola == ano_agricola)

    return paginate_query(query, page, page_size)

@router.get("/sigef", response_model=PaginatedResponse[SigefProducaoSchema])
def get_sigef(cultura: Optional[str] = None, uf: Optional[str] = None, safra: Optional[str] = None, page: int = 1, page_size: int = 20, db: Session = Depends(get_session)):
    query = db.query(
        FatoSigefProducao.id_sigef_producao,
        FatoSigefProducao.safra,
        FatoSigefProducao.especie,
        FatoSigefProducao.categoria,
        FatoSigefProducao.area_ha,
        FatoSigefProducao.producao_bruta_t,
        FatoSigefProducao.producao_est_t,
        DimCultura.nome_padronizado.label("cultura"),
        DimMunicipio.nome.label("municipio_nome"),
        DimMunicipio.uf
    ).join(DimCultura, FatoSigefProducao.id_cultura == DimCultura.id_cultura)\
     .join(DimMunicipio, FatoSigefProducao.id_municipio == DimMunicipio.id_municipio)

    if cultura:
        query = query.filter(DimCultura.nome_padronizado == cultura.lower())
    if uf:
        query = query.filter(DimMunicipio.uf == uf.upper())
    if safra:
        query = query.filter(FatoSigefProducao.safra == safra)

    return paginate_query(query, page, page_size)
