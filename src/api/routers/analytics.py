"""
Router de Analytics — Endpoints compostos que cruzam múltiplas tabelas do Star Schema.
Todos os endpoints são somente-leitura e agregam dados de 2+ tabelas fato.
"""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import List, Optional

from db.manager import (
    FatoProducaoPAM, FatoRiscoZARC, FatoMeteorologia,
    FatoProducaoConab, FatoPrecoConabMensal,
    FatoCultivar, FatoSigefProducao, FatoAgrofit,
    FatoFertilizante,
    DimCultura, DimMunicipio
)
from api.dependencies import get_session
from api.schemas import (
    RaioXAgroMunicipalSchema, ProducaoPAMSimplesSchema,
    DossieInsumosCulturaSchema,
    ViabilidadeEconomicaSchema,
    JanelaDeAplicacaoSchema,
    AuditoriaEstimativasSchema,
)
from api.duck_bridge import query_risco



router = APIRouter(prefix="/analytics", tags=["Analytics"])


# ---------------------------------------------------------------------------
# 2.1  Raio-X Agroclimático Municipal
#      Fontes: PAM (IBGE) + ZARC (MAPA) + Meteorologia (INMET)
# ---------------------------------------------------------------------------
@router.get(
    "/raio-x-municipal",
    response_model=RaioXAgroMunicipalSchema,
    summary="Raio-X Agroclimático de um município/cultura/ano",
)
def raio_x_municipal(
    codigo_ibge: str,
    cultura: str,
    ano: int,
    db: Session = Depends(get_session),
):
    """
    Cruza produção real (PAM), risco climático (ZARC) e clima observado (INMET)
    para um determinado município, cultura e ano.
    Retorna produção, risco predominante e resumo climático anual.
    """
    mun = db.query(DimMunicipio).filter(DimMunicipio.codigo_ibge == codigo_ibge).first()
    if not mun:
        raise HTTPException(status_code=404, detail=f"Município {codigo_ibge} não encontrado.")

    cult = db.query(DimCultura).filter(DimCultura.nome_padronizado == cultura.lower()).first()
    if not cult:
        raise HTTPException(status_code=404, detail=f"Cultura '{cultura}' não encontrada.")

    # --- Produção PAM ---
    pam = db.query(FatoProducaoPAM).filter(
        FatoProducaoPAM.id_municipio == mun.id_municipio,
        FatoProducaoPAM.id_cultura == cult.id_cultura,
        FatoProducaoPAM.ano == ano,
    ).first()

    resultado_safra = ProducaoPAMSimplesSchema(
        ano=ano,
        area_plantada_ha=pam.area_plantada_ha if pam else None,
        qtde_produzida_ton=pam.qtde_produzida_ton if pam else None,
    )

    # --- Risco ZARC predominante (Calculado via DuckDB/Parquet) ---
    risco_data = query_risco(codigo_ibge=mun.codigo_ibge, cultura=cultura)
    
    risco_predominante = None
    if risco_data:
        # Pega todos os valores de risco (dec1..dec36) de todas as linhas (solos)
        all_risks = []
        for row in risco_data:
            dec_values = [row.get(f"dec{i}") for i in range(1, 37)]
            all_risks.extend([v for v in dec_values if v is not None and v > 0])
        
        if all_risks:
            # Calcula o valor mais frequente (moda)
            from collections import Counter
            most_common = Counter(all_risks).most_common(1)
            risco_predominante = f"{most_common[0][0]}%" if most_common else None


    # --- Resumo Climático do ano (médias e acumulado INMET) ---
    clima_row = db.query(
        func.avg(FatoMeteorologia.temp_media_c).label("temp_media"),
        func.avg(FatoMeteorologia.umidade_media).label("umidade_media"),
        func.sum(FatoMeteorologia.precipitacao_total_mm).label("precipitacao_anual_mm"),
    ).filter(
        FatoMeteorologia.id_municipio == mun.id_municipio,
        func.extract("year", FatoMeteorologia.data) == ano,
    ).first()

    resumo_climatico = None
    if clima_row and clima_row.temp_media is not None:
        resumo_climatico = {
            "temp_media_c": round(clima_row.temp_media or 0, 2),
            "umidade_media": round(clima_row.umidade_media or 0, 2),
            "precipitacao_anual_mm": round(clima_row.precipitacao_anual_mm or 0, 2),
        }

    # Heurística de quebra de safra:
    # considera quebra se produção < 50% da área plantada * threshold mínimo (5 t/ha)
    ocorreu_quebra = None
    if pam and pam.area_plantada_ha and pam.qtde_produzida_ton:
        produtividade = pam.qtde_produzida_ton / pam.area_plantada_ha
        ocorreu_quebra = produtividade < 1.0  # < 1 t/ha = quebra severa

    return RaioXAgroMunicipalSchema(
        municipio=mun.nome,
        uf=mun.uf or "",
        cultura=cult.nome_padronizado,
        ano=ano,
        resultado_safra=resultado_safra,
        risco_zarc_predominante=risco_predominante,
        resumo_climatico=resumo_climatico,
        ocorreu_quebra_safra=ocorreu_quebra,
    )


# ---------------------------------------------------------------------------
# 2.2  Dossiê de Insumos por Cultura
#      Fontes: RNC (MAPA/Cultivares) + SIGEF + Agrofit
# ---------------------------------------------------------------------------
@router.get(
    "/dossie-insumos/{cultura}",
    response_model=DossieInsumosCulturaSchema,
    summary="Dossiê completo de insumos: cultivares registradas, sementes e defensivos",
)
def dossie_insumos(cultura: str, db: Session = Depends(get_session)):
    """
    Agrega para uma cultura:
    - Cultivares ativas no RNC
    - Volume total de sementes produzido (SIGEF)
    - Lista de defensivos (Agrofit) e principais pragas
    """
    cult = db.query(DimCultura).filter(DimCultura.nome_padronizado == cultura.lower()).first()
    if not cult:
        raise HTTPException(status_code=404, detail=f"Cultura '{cultura}' não encontrada.")

    # Cultivares ativas no RNC
    cultivares_ativos = db.query(func.count(FatoCultivar.nr_registro)).filter(
        FatoCultivar.id_cultura == cult.id_cultura,
        FatoCultivar.situacao.ilike("%REGISTRAD%"),
    ).scalar() or 0

    # Volume total sementes SIGEF (soma producao_bruta_t)
    vol_sementes = db.query(func.sum(FatoSigefProducao.producao_bruta_t)).filter(
        FatoSigefProducao.id_cultura == cult.id_cultura
    ).scalar()

    # Defensivos Agrofit — nomes únicos de marcas comerciais
    defensivos_rows = (
        db.query(FatoAgrofit.marca_comercial)
        .filter(FatoAgrofit.id_cultura == cult.id_cultura)
        .distinct()
        .limit(20)
        .all()
    )
    defensivos = [r[0] for r in defensivos_rows if r[0]]

    # Pragas alvo mais frequentes
    pragas_rows = (
        db.query(FatoAgrofit.praga_comum, func.count().label("n"))
        .filter(
            FatoAgrofit.id_cultura == cult.id_cultura,
            FatoAgrofit.praga_comum.isnot(None),
        )
        .group_by(FatoAgrofit.praga_comum)
        .order_by(func.count().desc())
        .limit(10)
        .all()
    )
    pragas = [r[0] for r in pragas_rows]

    # Grau de tecnologia: heurística a partir do número de defensivos registrados
    total_def = db.query(func.count(FatoAgrofit.id_agrofit)).filter(
        FatoAgrofit.id_cultura == cult.id_cultura
    ).scalar() or 0
    if total_def >= 100:
        grau = "Alto"
    elif total_def >= 30:
        grau = "Médio"
    else:
        grau = "Baixo"

    return DossieInsumosCulturaSchema(
        cultura=cult.nome_padronizado,
        cultivares_ativos=cultivares_ativos,
        volume_sementes_sigef_ton=round(float(vol_sementes), 2) if vol_sementes else None,
        defensivos_recomendados=defensivos,
        principais_pragas_alvo=pragas,
        grau_de_tecnologia=grau,
    )


# ---------------------------------------------------------------------------
# 2.3  Viabilidade Econômica
#      Fontes: PAM (produção IBGE) + Preços CONAB Mensal
# ---------------------------------------------------------------------------
@router.get(
    "/viabilidade-economica",
    response_model=ViabilidadeEconomicaSchema,
    summary="Estimativa de receita bruta por cultura/UF/ano cruzando produção e preços",
)
def viabilidade_economica(
    cultura: str,
    uf: str,
    ano: int,
    db: Session = Depends(get_session),
):
    """
    Cruza produção total dos municípios de uma UF (PAM) com o preço médio
    anual pago ao produtor (CONAB mensal) para estimar o VGB e a
    renda média por hectare.
    """
    cult = db.query(DimCultura).filter(DimCultura.nome_padronizado == cultura.lower()).first()
    if not cult:
        raise HTTPException(status_code=404, detail=f"Cultura '{cultura}' não encontrada.")

    # Produção total da UF no ano (soma de municípios pelo PAM)
    prod_row = (
        db.query(
            func.sum(FatoProducaoPAM.qtde_produzida_ton).label("producao_total_ton"),
            func.sum(FatoProducaoPAM.area_plantada_ha).label("area_total_ha"),
        )
        .join(DimMunicipio, FatoProducaoPAM.id_municipio == DimMunicipio.id_municipio)
        .filter(
            FatoProducaoPAM.id_cultura == cult.id_cultura,
            DimMunicipio.uf == uf.upper(),
            FatoProducaoPAM.ano == ano,
        )
        .first()
    )

    producao_ton = float(prod_row.producao_total_ton) if prod_row and prod_row.producao_total_ton else None
    area_ha = float(prod_row.area_total_ha) if prod_row and prod_row.area_total_ha else None

    # Preço médio anual ao produtor (CONAB mensal — nível produtor, convertido para /t)
    preco_row = (
        db.query(func.avg(FatoPrecoConabMensal.valor_kg).label("preco_medio_kg"))
        .filter(
            FatoPrecoConabMensal.id_cultura == cult.id_cultura,
            FatoPrecoConabMensal.uf == uf.upper(),
            FatoPrecoConabMensal.ano == ano,
        )
        .first()
    )
    preco_kg = float(preco_row.preco_medio_kg) if preco_row and preco_row.preco_medio_kg else None
    preco_ton = preco_kg * 1000 if preco_kg else None

    # Valor Bruto da Produção (VGB) em milhões de R$
    vgb = None
    if producao_ton and preco_ton:
        vgb = round((producao_ton * preco_ton) / 1_000_000, 2)

    # Renda média por hectare
    renda_ha = None
    if vgb and area_ha and area_ha > 0:
        renda_ha = round((vgb * 1_000_000) / area_ha, 2)

    return ViabilidadeEconomicaSchema(
        ano=ano,
        cultura=cult.nome_padronizado,
        uf=uf.upper(),
        producao_total_ton=producao_ton,
        preco_medio_anual_ton=round(preco_ton, 2) if preco_ton else None,
        valor_teto_atingido=round(preco_ton * 1.2, 2) if preco_ton else None,
        vgb_apurado_milhoes=vgb,
        renda_media_hectare=renda_ha,
    )


# ---------------------------------------------------------------------------
# 2.4  Janela de Aplicação de Insumos
#      Fontes: Fertilizantes (SIPEAGRO) + Meteorologia (INMET)
# ---------------------------------------------------------------------------
@router.get(
    "/janela-aplicacao",
    response_model=JanelaDeAplicacaoSchema,
    summary="Avalia disponibilidade de insumos e condições climáticas para aplicação",
)
def janela_aplicacao(
    codigo_ibge: str,
    ano: int,
    mes: int,
    db: Session = Depends(get_session),
):
    """
    Para um município e mês, verifica:
    - Número de estabelecimentos de fertilizantes/defensivos na região (UF)
    - Chuva acumulada do mês (INMET)
    - Se existe janela ideal: >3 estabelecimentos E precipitação entre 50–200 mm/mês
    """
    mun = db.query(DimMunicipio).filter(DimMunicipio.codigo_ibge == codigo_ibge).first()
    if not mun:
        raise HTTPException(status_code=404, detail=f"Município {codigo_ibge} não encontrado.")

    # Estabelecimentos ativos na UF
    estab_count = db.query(func.count(FatoFertilizante.id_fertilizante)).filter(
        FatoFertilizante.uf == mun.uf,
        FatoFertilizante.status_registro.ilike("%ATIVO%"),
    ).scalar() or 0

    # Precipitação acumulada do mês
    chuva_row = db.query(
        func.sum(FatoMeteorologia.precipitacao_total_mm).label("chuva_total")
    ).filter(
        FatoMeteorologia.id_municipio == mun.id_municipio,
        func.extract("year", FatoMeteorologia.data) == ano,
        func.extract("month", FatoMeteorologia.data) == mes,
    ).first()

    chuva_mm = float(chuva_row.chuva_total) if chuva_row and chuva_row.chuva_total else None

    # Heurística de janela perfeita: entre 50 e 200 mm e tem estabelecimentos suficientes
    janela_perfeita = None
    if chuva_mm is not None:
        janela_perfeita = (50.0 <= chuva_mm <= 200.0) and estab_count >= 3

    # Capacidade de atendimento baseada no número de estabelecimentos
    if estab_count >= 50:
        capacidade = "Alta"
    elif estab_count >= 15:
        capacidade = "Média"
    elif estab_count >= 1:
        capacidade = "Baixa"
    else:
        capacidade = "Sem cobertura"

    return JanelaDeAplicacaoSchema(
        municipio=mun.nome,
        uf=mun.uf or "",
        mes_referencia=f"{ano}-{mes:02d}",
        estabelecimentos_insumos=estab_count,
        acumulado_chuvas_mm=round(chuva_mm, 2) if chuva_mm is not None else None,
        janela_perfeita_aplicacao=janela_perfeita,
        capacidade_de_atendimento=capacidade,
    )


# ---------------------------------------------------------------------------
# 2.5  Auditoria de Estimativas
#      Fontes: CONAB (estimativa) × PAM/IBGE (realizado)
# ---------------------------------------------------------------------------
@router.get(
    "/auditoria-estimativas",
    response_model=List[AuditoriaEstimativasSchema],
    summary="Compara estimativas CONAB com resultados reais do IBGE/PAM por UF e safra",
)
def auditoria_estimativas(
    cultura: str,
    uf: Optional[str] = None,
    db: Session = Depends(get_session),
):
    """
    Para cada UF e safra, confronta:
    - Produção estimada pela CONAB (mil toneladas)
    - Produção realizada pelo IBGE/PAM (mil toneladas)
    - Variação percentual e classificação da acurácia
    """
    cult = db.query(DimCultura).filter(DimCultura.nome_padronizado == cultura.lower()).first()
    if not cult:
        raise HTTPException(status_code=404, detail=f"Cultura '{cultura}' não encontrada.")

    # CONAB: produção estimada — agrupada por UF e ano_agricola
    conab_q = (
        db.query(
            FatoProducaoConab.uf,
            FatoProducaoConab.ano_agricola,
            func.sum(FatoProducaoConab.producao_mil_t).label("estimativa_mil_t"),
        )
        .filter(FatoProducaoConab.id_cultura == cult.id_cultura)
        .group_by(FatoProducaoConab.uf, FatoProducaoConab.ano_agricola)
    )
    if uf:
        conab_q = conab_q.filter(FatoProducaoConab.uf == uf.upper())

    conab_rows = conab_q.limit(50).all()

    results = []
    for row in conab_rows:
        # Extraímos o ano de início da safra para buscar no PAM
        # Formato do ano_agricola: "2023/24" → ano=2023
        try:
            ano_pam = int(row.ano_agricola.split("/")[0])
        except (ValueError, AttributeError, IndexError):
            continue

        # PAM: produção realizada na UF naquele ano (soma municípios) em mil toneladas
        pam_row = (
            db.query(func.sum(FatoProducaoPAM.qtde_produzida_ton).label("prod_ton"))
            .join(DimMunicipio, FatoProducaoPAM.id_municipio == DimMunicipio.id_municipio)
            .filter(
                FatoProducaoPAM.id_cultura == cult.id_cultura,
                DimMunicipio.uf == row.uf,
                FatoProducaoPAM.ano == ano_pam,
            )
            .first()
        )

        realizado_ton = float(pam_row.prod_ton) if pam_row and pam_row.prod_ton else None
        realizado_mil_t = (realizado_ton / 1000.0) if realizado_ton else None
        estimativa_mil_t = float(row.estimativa_mil_t) if row.estimativa_mil_t else None

        # Variação percentual: (realizado - estimado) / estimado * 100
        variacao = None
        acuracidade = None
        if realizado_mil_t is not None and estimativa_mil_t and estimativa_mil_t > 0:
            variacao = round(((realizado_mil_t - estimativa_mil_t) / estimativa_mil_t) * 100, 2)
            abs_var = abs(variacao)
            if abs_var <= 5:
                acuracidade = "Excelente (≤5%)"
            elif abs_var <= 15:
                acuracidade = "Boa (5–15%)"
            elif abs_var <= 30:
                acuracidade = "Regular (15–30%)"
            else:
                acuracidade = "Fraca (>30%)"

        results.append(
            AuditoriaEstimativasSchema(
                ano_safra=row.ano_agricola,
                uf=row.uf,
                cultura=cult.nome_padronizado,
                estimativa_conab_mil_t=round(estimativa_mil_t, 2) if estimativa_mil_t else None,
                realizado_ibge_pam_mil_t=round(realizado_mil_t, 2) if realizado_mil_t else None,
                variacao_margem_erro=variacao,
                acuracidade_relatorio=acuracidade,
            )
        )

    return results
