"""
Funções de gerenciamento de dimensões do Star Schema.
Responsável por popular DimCultura, DimMunicipio e DimMantenedor.
"""

import logging
import requests
import pandas as pd
from db.manager import DimCultura, DimMunicipio, DimMantenedor

log = logging.getLogger(__name__)


def preencher_dimensao_cultura(db, culturas_lista):
    culturas_map = {}
    novos = []
    for c in culturas_lista:
        c_norm = c.strip().lower()
        db_cultura = db.query(DimCultura).filter(DimCultura.nome_padronizado == c_norm).first()
        if not db_cultura:
            db_cultura = DimCultura(nome_padronizado=c_norm)
            db.add(db_cultura)
            novos.append(c_norm)
        culturas_map[c_norm] = db_cultura

    if novos:
        try:
            db.commit()
            log.info(f"Dim Cultura: {len(novos)} novo(s) registro(s) inserido(s): {novos}")
        except Exception as e:
            db.rollback()
            log.error(f"Dim Cultura: falha no commit bulk — {e}")
            raise
    
    for c_norm, obj in culturas_map.items():
        db.refresh(obj)
        culturas_map[c_norm] = obj.id_cultura
    return culturas_map


def preencher_dimensao_mantenedor(db, df_cult):
    mant_map = {}
    if df_cult.empty or "mantenedor" not in df_cult.columns:
        return mant_map

    col_setor = "SETOR" if "SETOR" in df_cult.columns else "setor" if "setor" in df_cult.columns else None
    cols = ["mantenedor"] + ([col_setor] if col_setor else [])
    unique_mants = df_cult[cols].drop_duplicates().dropna(subset=["mantenedor"])
    
    # Bulk: busca todos os nomes existentes em 1 query (vs. N queries individuais)
    existing_names = set(r[0] for r in db.query(DimMantenedor.nome).all())
    
    novos_objetos = []
    for _, row in unique_mants.iterrows():
        nome = row["mantenedor"]
        if nome not in existing_names:
            setor = row[col_setor] if col_setor else None
            novos_objetos.append(DimMantenedor(nome=nome, setor=setor))

    if novos_objetos:
        db.bulk_save_objects(novos_objetos)
        db.commit()
        log.info(f"DimMantenedor: {len(novos_objetos)} novo(s) registro(s) inserido(s).")
    
    # Reconstroi mapa completo com 1 query
    for m in db.query(DimMantenedor).all():
        mant_map[m.nome] = m.id_mantenedor
    return mant_map


def carregar_municipios_completo_ibge(db):
    """
    Busca a lista oficial de todos os municípios do Brasil via API do IBGE 
    e popula a DimMunicipio de forma proativa.
    Otimizado: 1 SELECT bulk para códigos existentes + 1 bulk insert para novos.
    """
    log.info("Buscando lista completa de municípios na API do IBGE...")
    url = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        muns_data = resp.json()
    except Exception as e:
        log.error(f"Falha ao buscar municípios no IBGE: {e}")
        return preencher_dimensao_municipio(db)

    # Bulk: busca todos os códigos existentes em UMA query (vs. 5570 queries individuais)
    existing_codes = set(r[0] for r in db.query(DimMunicipio.codigo_ibge).all())
    
    novos_objetos = []
    for m in muns_data:
        cod = str(m["id"])
        if cod in existing_codes:
            continue
            
        nome = m["nome"]
        
        # Extração robusta da UF tentando diferentes caminhos na hierarquia IBGE
        uf = None
        try:
            # Caminho 1: Microrregião -> Mesorregião -> UF
            uf = m.get("microrregiao", {}).get("mesorregiao", {}).get("UF", {}).get("sigla")
            
            # Caminho 2 (Fallback): Região Imediata -> Região Intermediária -> UF
            if not uf:
                uf = m.get("regiao-imediata", {}).get("regiao-intermediaria", {}).get("UF", {}).get("sigla")
        except:
            uf = "XX" # Fallback extremo para não quebrar a carga
            
        uf = str(uf).upper() if uf else "XX"
        novos_objetos.append(DimMunicipio(codigo_ibge=cod, nome=nome, uf=uf))
    
    if novos_objetos:
        db.bulk_save_objects(novos_objetos)
        db.commit()
        log.info(f"DimMunicipio: {len(novos_objetos)} novos municípios importados do IBGE.")
    
    return preencher_dimensao_municipio(db)


def preencher_dimensao_municipio(db, df_pam=pd.DataFrame(), df_zarc=pd.DataFrame()):
    mun_map_ibge = {}
    mun_map_name = {}
    
    db_muns = db.query(DimMunicipio).all()
    for m in db_muns:
        mun_map_ibge[m.codigo_ibge] = m.id_municipio
        if m.uf:
            mun_map_name[(m.nome.lower().strip(), m.uf)] = m.id_municipio

    novos_objetos = []

    # 1. PAM
    if not df_pam.empty:
        pam_muns = df_pam[["cod_municipio_ibge", "municipio_nome", "uf"]].drop_duplicates().dropna(subset=["cod_municipio_ibge"])
        for _, row in pam_muns.iterrows():
            cod = str(row["cod_municipio_ibge"])[:7]
            uf = str(row["uf"]).strip().upper() if pd.notna(row["uf"]) else None
            db_mun = db.query(DimMunicipio).filter(DimMunicipio.codigo_ibge == cod).first()
            if not db_mun:
                db_mun = DimMunicipio(codigo_ibge=cod, nome=row["municipio_nome"], uf=uf)
                db.add(db_mun)
                novos_objetos.append((cod, db_mun, row["municipio_nome"].lower().strip(), uf))
            else:
                mun_map_ibge[cod] = db_mun.id_municipio
                if uf: mun_map_name[(row["municipio_nome"].lower().strip(), uf)] = db_mun.id_municipio
        db.commit()

    # 2. ZARC
    if not df_zarc.empty and "cod_municipio_ibge" in df_zarc.columns:
        zarc_muns = df_zarc[["cod_municipio_ibge", "municipio", "uf"]].drop_duplicates().dropna(subset=["cod_municipio_ibge"])
        for _, row in zarc_muns.iterrows():
            cod = str(row["cod_municipio_ibge"])[:7]
            if cod in mun_map_ibge: continue
            db_mun = db.query(DimMunicipio).filter(DimMunicipio.codigo_ibge == cod).first()
            if not db_mun:
                uf = str(row["uf"]).strip().upper() if pd.notna(row["uf"]) else None
                db_mun = DimMunicipio(codigo_ibge=cod, nome=row["municipio"], uf=uf)
                db.add(db_mun)
                novos_objetos.append((cod, db_mun, row["municipio"].lower().strip(), uf))
            else:
                mun_map_ibge[cod] = db_mun.id_municipio
        db.commit()

    for cod, obj, nome_norm, uf in novos_objetos:
        db.refresh(obj)
        mun_map_ibge[cod] = obj.id_municipio
        if uf: mun_map_name[(nome_norm, uf)] = obj.id_municipio
    
    return mun_map_ibge, mun_map_name
