import logging
import pandas as pd
from sqlalchemy import text, inspect, Integer, BigInteger, Numeric
from sqlalchemy.dialects.postgresql import insert
from db.manager import (
    engine, DimCultura, DimMunicipio, DimMantenedor, 
    FatoProducaoConab, FatoAgrofit, FatoPrecoConabMensal, 
    FatoPrecoConabSemanal, FatoCultivar, FatoProducaoPAM, 
    FatoRiscoZARC, FatoFertilizante, FatoSigefProducao, 
    FatoSigefUsoProprio, FatoMeteorologia
)

log = logging.getLogger(__name__)

def get_cultura_id(nome_cultura, mapping):
    if not nome_cultura: return None

    def norm(s):
        import unicodedata
        s = str(s).lower().strip()
        s = "".join(c for c in unicodedata.normalize('NFKD', s) if unicodedata.category(c) != 'Mn')
        return s.replace("-", " ").replace("_", " ")

    # Tenta match exato primeiro (antes de normalizar)
    if nome_cultura in mapping: return mapping[nome_cultura]

    nombre_norm = norm(nome_cultura)
    for alvo, cid in mapping.items():
        alvo_norm = norm(alvo)
        if alvo_norm in nombre_norm or nombre_norm in alvo_norm:
            return cid
    return None

def upsert_data(model, df, index_elements, chunk_size=1000):
    if df.empty: return
    
    # Garante que não haja duplicatas no set todo para evitar CardinalityViolation (Postgres)
    df = df.drop_duplicates(subset=index_elements, keep='last')
    
    # Identifica colunas que devem ser inteiras para conversão explícita
    mapper = inspect(model)
    pk_cols = [c.key for c in mapper.primary_key]
    model_int_cols = [c.key for c in mapper.column_attrs if isinstance(c.expression.type, (Integer, BigInteger))]
    model_cols = [c.key for c in mapper.column_attrs]
    
    for i in range(0, len(df), chunk_size):
        chunk_df = df.iloc[i : i + chunk_size]
        records = chunk_df.to_dict(orient="records")
        
        valid_records = []
        for r in records:
            valid_row = {}
            for k, v in r.items():
                if k in model_cols:
                    if pd.isna(v):
                        valid_row[k] = None
                    elif k in model_int_cols:
                        try:
                            # Garante que IDs e outros campos inteiros sejam int, não float (ex: 4.0 -> 4)
                            valid_row[k] = int(float(v))
                        except (ValueError, TypeError):
                            valid_row[k] = None
                    else:
                        valid_row[k] = v
            valid_records.append(valid_row)

        if not valid_records: continue

        stmt = insert(model).values(valid_records)
        
        # Colunas para atualizar em caso de conflito (todas exceto as do índice, PK e metadados automáticos)
        update_cols = {c: stmt.excluded[c] for c in model_cols if c not in index_elements and c not in pk_cols and c != 'data_modificacao'}
        
        upsert_stmt = stmt.on_conflict_do_update(
            index_elements=index_elements,
            set_=update_cols
        )
        
        with engine.begin() as conn:
            conn.execute(upsert_stmt)

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
    
    novos = []
    for _, row in unique_mants.iterrows():
        nome = row["mantenedor"]
        setor = row[col_setor] if col_setor else None
        db_mant = db.query(DimMantenedor).filter(DimMantenedor.nome == nome).first()
        if not db_mant:
            db_mant = DimMantenedor(nome=nome, setor=setor)
            db.add(db_mant)
            novos.append(nome)
        mant_map[nome] = db_mant

    if novos:
        db.commit()
    
    for nome, obj in mant_map.items():
        db.refresh(obj)
        mant_map[nome] = obj.id_mantenedor
    return mant_map

def preencher_dimensao_municipio(db, df_pam=pd.DataFrame(), df_zarc=pd.DataFrame()):
    mun_map_ibge = {}
    mun_map_name = {}
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

def load_fact_cultivares(db, df, map_cult, map_mant):
    if df.empty: return
    df_f = df.copy()
    df_f["id_cultura"] = df_f["cultura"].apply(lambda x: get_cultura_id(x, map_cult))
    df_f["id_mantenedor"] = df_f["mantenedor"].map(map_mant)
    cols = ["nr_registro", "id_cultura", "id_mantenedor", "cultivar", "nome_secundario", "situacao", "nr_formulario", "data_reg", "data_val"]
    df_f = df_f[[c for c in cols if c in df_f.columns]].drop_duplicates(subset=["nr_registro"]).dropna(subset=["cultivar", "id_cultura"])
    upsert_data(FatoCultivar, df_f, index_elements=['nr_registro'])
    log.info(f"Fato Cultivares: {len(df_f)} registros upserted.")

def load_fact_pam(db, df, map_cult, map_mun):
    if df.empty: return
    df_f = df.copy()
    df_f["id_cultura"] = df_f["cultura"].apply(lambda x: get_cultura_id(x, map_cult))
    df_f["cod_municipio_ibge"] = df_f["cod_municipio_ibge"].astype(str).str[:7]
    df_f["id_municipio"] = df_f["cod_municipio_ibge"].map(map_mun)
    cols = ["id_cultura", "id_municipio", "ano", "area_plantada_ha", "area_colhida_ha", "qtde_produzida_ton", "valor_producao_mil_reais"]
    df_f = df_f[cols].dropna(subset=["id_cultura", "id_municipio"])
    upsert_data(FatoProducaoPAM, df_f, index_elements=['id_cultura', 'id_municipio', 'ano'])
    log.info(f"Fato PAM: {len(df_f)} registros upserted.")

def load_fact_zarc(db, data, map_cult, map_mun):
    """
    Recebe um DataFrame ou um Gerador de DataFrames (Chunks).
    """
    if data is None: return
    
    # Se for um gerador, processa cada chunk
    if not isinstance(data, pd.DataFrame):
        for chunk in data:
            load_fact_zarc(db, chunk, map_cult, map_mun)
        return

    df = data
    if df.empty: return
    df_f = df.copy()
    df_f["id_cultura"] = df_f["cultura"].apply(lambda x: get_cultura_id(x, map_cult))
    df_f["cod_municipio_ibge"] = df_f["cod_municipio_ibge"].astype(str).str[:7]
    df_f["id_municipio"] = df_f["cod_municipio_ibge"].map(map_mun)
    renames = {"solo": "tipo_solo", "decendio": "periodo_plantio", "periodo": "periodo_plantio", "risco": "risco_climatico"}
    for k, v in renames.items():
        for col in df_f.columns:
            if k in col.lower(): df_f = df_f.rename(columns={col: v})
    cols = ["id_cultura", "id_municipio", "tipo_solo", "periodo_plantio", "risco_climatico"]
    df_f = df_f[[c for c in cols if c in df_f.columns]].dropna(subset=["id_cultura", "id_municipio"])
    upsert_data(FatoRiscoZARC, df_f, index_elements=['id_cultura', 'id_municipio', 'tipo_solo', 'periodo_plantio'])
    log.info(f"Fato ZARC: {len(df_f)} registros upserted.")

def load_fact_conab(db, df_dict, map_cult, map_mun):
    if not isinstance(df_dict, dict): return
    for key, df in df_dict.items():
        if df.empty: continue
        df_f = df.copy()
        df_f["id_cultura"] = df_f["cultura"].apply(lambda x: get_cultura_id(x, map_cult))
        df_f = df_f.dropna(subset=["id_cultura"])
        if "cod_municipio_ibge" in df_f.columns:
            df_f["id_municipio"] = df_f["cod_municipio_ibge"].astype(str).str[:7].map(map_mun)
        else:
            df_f["id_municipio"] = None

        if "producao" in key:
            index = ['id_cultura', 'uf', 'ano_agricola', 'safra']
            upsert_data(FatoProducaoConab, df_f, index_elements=index)
        elif "mensal" in key:
            index = ['id_cultura', 'id_municipio', 'uf', 'ano', 'mes', 'nivel_comercializacao']
            upsert_data(FatoPrecoConabMensal, df_f, index_elements=index)
        elif "semanal" in key:
            # Política semanal de 4 semanas
            with engine.connect() as conn:
                count = conn.execute(text("SELECT COUNT(DISTINCT semana) FROM fato_precos_conab_semanal")).scalar() or 0
            if count >= 4:
                with engine.begin() as conn: conn.execute(text("TRUNCATE TABLE fato_precos_conab_semanal"))
            index = ['id_cultura', 'id_municipio', 'uf', 'ano', 'mes', 'semana', 'nivel_comercializacao']
            upsert_data(FatoPrecoConabSemanal, df_f, index_elements=index)
        log.info(f"Fato CONAB ({key}): Upsert concluído.")

def load_fact_agrofit(db, df, map_cult):
    if df.empty: return
    df_f = df.copy()
    df_f["id_cultura"] = df_f["cultura"].apply(lambda x: get_cultura_id(x, map_cult))
    df_f = df_f.dropna(subset=["id_cultura"])
    index = ['id_cultura', 'nr_registro', 'marca_comercial', 'praga_comum']
    upsert_data(FatoAgrofit, df_f, index_elements=index)
    log.info(f"Fato Agrofit: {len(df_f)} registros upserted.")

def load_fact_fertilizantes(db, df, map_mun_name):
    if df.empty: return
    df_f = df.copy()
    df_f["id_municipio"] = df_f.apply(
        lambda x: map_mun_name.get((str(x["municipio"]).lower().strip(), str(x["uf"]).upper())) 
        if pd.notna(x.get("municipio")) and pd.notna(x.get("uf")) else None, 
        axis=1
    )
    df_f = df_f.drop_duplicates(subset=["nr_registro_estabelecimento"])
    upsert_data(FatoFertilizante, df_f, index_elements=['nr_registro_estabelecimento'])
    log.info(f"Fato Fertilizantes: {len(df_f)} estabelecimentos upserted.")

def load_fact_sigef(db, df_dict, map_cult, map_mun_name):
    if not isinstance(df_dict, dict): return
    for key, df in df_dict.items():
        if df.empty: continue
        df_f = df.copy()
        df_f["id_cultura"] = df_f["cultura"].apply(lambda x: get_cultura_id(x, map_cult))
        df_f["id_municipio"] = df_f.apply(
            lambda x: map_mun_name.get((str(x["municipio"]).lower().strip(), str(x["uf"]).upper())) 
            if pd.notna(x.get("municipio")) and pd.notna(x.get("uf")) else None, 
            axis=1
        )
        df_f = df_f.dropna(subset=["id_cultura", "id_municipio"])
        if key == "campos_producao":
            index = ['id_cultura', 'id_municipio', 'safra', 'especie', 'cultivar_raw', 'categoria']
            upsert_data(FatoSigefProducao, df_f, index_elements=index)
        elif key == "uso_proprio":
            index = ['id_cultura', 'id_municipio', 'periodo', 'especie', 'cultivar_raw']
            upsert_data(FatoSigefUsoProprio, df_f, index_elements=index)
        log.info(f"Fato SIGEF ({key}): Upsert concluído.")

def load_fact_meteorologia(db, df, extractor, all_muns):
    if df.empty: return
    stations_df = extractor.get_stations()
    stations_df["name_norm"] = stations_df["DC_NOME"].str.lower().str.strip()
    
    mun_to_station = {}
    for m in all_muns:
        match = stations_df[(stations_df["name_norm"] == m.nome.lower().strip()) & (stations_df["SG_ESTADO"] == m.uf)]
        if match.empty:
            match = stations_df[(stations_df["name_norm"].str.contains(m.nome.lower().strip())) & (stations_df["SG_ESTADO"] == m.uf)]
        if not match.empty:
            mun_to_station[m.id_municipio] = match.iloc[0]["CD_ESTACAO"]

    unique_stations = list(set(mun_to_station.values()))
    if not unique_stations: return
    
    raw_data = extractor.extract(unique_stations)
    from pipeline.cleaners.inmet import clean_inmet
    df_meteo = clean_inmet(raw_data)
    
    if df_meteo.empty: return
    
    station_to_muns = {}
    for mid, sid in mun_to_station.items():
        station_to_muns.setdefault(sid, []).append(mid)
    
    rows = []
    for _, row in df_meteo.iterrows():
        for mid in station_to_muns.get(row["estacao_id"], []):
            new_row = row.to_dict()
            new_row["id_municipio"] = mid
            rows.append(new_row)
    
    df_final = pd.DataFrame(rows)
    upsert_data(FatoMeteorologia, df_final, index_elements=['id_municipio', 'data'])
    log.info(f"Fato Meteorologia: {len(df_final)} registros upserted.")
