import argparse
import logging
import concurrent.futures
import pandas as pd
from sqlalchemy import text
from pipeline.cultivares import CultivaresExtractor
from pipeline.sidra import SidraExtractor
from pipeline.zarc import ZarcExtractor
from pipeline.conab import ConabExtractor
from pipeline.agrofit import AgrofitExtractor
from pipeline.fertilizantes import FertilizantesExtractor
from pipeline.sigef import SigefExtractor
from db.manager import init_db, get_db, engine, DimCultura, DimMunicipio, DimMantenedor, FatoProducaoConab, FatoAgrofit, FatoPrecoConabMensal, FatoPrecoConabSemanal, FatoCultivar, FatoProducaoPAM, FatoRiscoZARC, FatoFertilizante, FatoSigefProducao, FatoSigefUsoProprio
from sqlalchemy.dialects.postgresql import insert

EXTRACTORS = {
    "cultivares": CultivaresExtractor,
    "sidra": SidraExtractor,
    "zarc": ZarcExtractor,
    "conab": ConabExtractor,
    "agrofit": AgrofitExtractor,
    "fertilizantes": FertilizantesExtractor,
    "sigef": SigefExtractor
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

def preencher_dimensao_cultura(db, culturas_lista):
    """DDL: Bulk upsert de culturas alvo na dimensão. Uma transação por carga."""
    culturas_map = {}
    novos = []

    for c in culturas_lista:
        c_norm = c.strip().lower()
        db_cultura = db.query(DimCultura).filter(DimCultura.nome_padronizado == c_norm).first()
        if not db_cultura:
            db_cultura = DimCultura(nome_padronizado=c_norm)
            db.add(db_cultura)
            novos.append(c_norm)
        culturas_map[c_norm] = db_cultura  # Guardamos o objeto para refresh pós-commit

    if novos:
        try:
            db.commit()
            log.info(f"Dim Cultura: {len(novos)} novo(s) registro(s) inserido(s): {novos}")
        except Exception as e:
            db.rollback()
            log.error(f"Dim Cultura: falha no commit bulk — {e}")
            raise
    else:
        log.info("Dim Cultura: nenhum novo registro. Dimensão já atualizada.")

    # Refresh pós-commit para obter os IDs gerados
    for c_norm, obj in culturas_map.items():
        db.refresh(obj)
        culturas_map[c_norm] = obj.id_cultura

    return culturas_map


def preencher_dimensao_mantenedor(db, df_cult):
    """DDL: Bulk upsert de mantenedores únicos. Uma transação por carga."""
    mant_map = {}
    if "mantenedor" not in df_cult.columns:
        log.warning("Dim Mantenedor: coluna 'mantenedor' ausente no DataFrame de cultivares. Pulando.")
        return mant_map

    col_setor = "SETOR" if "SETOR" in df_cult.columns else "setor" if "setor" in df_cult.columns else None
    if col_setor is None:
        log.warning("Dim Mantenedor: coluna de setor não encontrada. Setor será nulo.")

    cols = ["mantenedor"] + ([col_setor] if col_setor else [])
    unique_mants = df_cult[cols].drop_duplicates().dropna(subset=["mantenedor"])
    log.info(f"Dim Mantenedor: {len(unique_mants)} mantenedor(es) único(s) encontrado(s) no DataFrame.")

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
        try:
            db.commit()
            log.info(f"Dim Mantenedor: {len(novos)} novo(s) registro(s) inserido(s).")
        except Exception as e:
            db.rollback()
            log.error(f"Dim Mantenedor: falha no commit bulk — {e}")
            raise
    else:
        log.info("Dim Mantenedor: nenhum novo registro. Dimensão já atualizada.")

    for nome, obj in mant_map.items():
        db.refresh(obj)
        mant_map[nome] = obj.id_mantenedor

    return mant_map


def preencher_dimensao_municipio(db, df_pam, df_zarc):
    """DDL: Bulk upsert de municípios a partir do PAM e do ZARC. Uma transação por fonte."""
    mun_map_ibge = {}
    mun_map_name = {}
    novos_objetos = []  # Acumula para refresh pós-commit

    # 1. Municípios do PAM/IBGE (Mais precisos, têm Código IBGE)
    if not df_pam.empty:
        cols_pam = [c for c in ["cod_municipio_ibge", "municipio_nome", "uf"] if c in df_pam.columns]
        pam_muns = df_pam[cols_pam].drop_duplicates().dropna(subset=["cod_municipio_ibge"])
        log.info(f"Dim Município (PAM): processando {len(pam_muns)} município(s) único(s).")
        novos_pam = 0

        for _, row in pam_muns.iterrows():
            cod = str(row["cod_municipio_ibge"])[:7]
            uf = str(row["uf"]).strip().upper() if "uf" in row and pd.notna(row["uf"]) else None
            db_mun = db.query(DimMunicipio).filter(DimMunicipio.codigo_ibge == cod).first()
            if not db_mun:
                db_mun = DimMunicipio(codigo_ibge=cod, nome=row["municipio_nome"], uf=uf)
                db.add(db_mun)
                novos_objetos.append((cod, db_mun, row["municipio_nome"].lower().strip(), uf))
                novos_pam += 1
            else:
                mun_map_ibge[cod] = db_mun.id_municipio
                if uf:
                    mun_map_name[(row["municipio_nome"].lower().strip(), uf)] = db_mun.id_municipio

        if novos_pam:
            try:
                db.commit()
                log.info(f"Dim Município (PAM): {novos_pam} novo(s) município(s) inserido(s).")
            except Exception as e:
                db.rollback()
                log.error(f"Dim Município (PAM): falha no commit bulk — {e}")
                raise

        for cod, obj, nome_norm, uf in novos_objetos:
            db.refresh(obj)
            mun_map_ibge[cod] = obj.id_municipio
            if uf:
                mun_map_name[(nome_norm, uf)] = obj.id_municipio
        novos_objetos.clear()

    # 2. Municípios do ZARC/MAPA (apenas os que ainda não estão no mapa)
    if not df_zarc.empty and "cod_municipio_ibge" in df_zarc.columns:
        cols_zarc = [c for c in ["cod_municipio_ibge", "municipio", "uf"] if c in df_zarc.columns]
        zarc_muns = df_zarc[cols_zarc].drop_duplicates().dropna(subset=["cod_municipio_ibge"])
        log.info(f"Dim Município (ZARC): processando {len(zarc_muns)} município(s) único(s).")
        novos_zarc = 0

        for _, row in zarc_muns.iterrows():
            cod = str(row["cod_municipio_ibge"])[:7]
            if cod in mun_map_ibge:
                continue  # Já carregado via PAM
            uf = str(row["uf"]).strip().upper() if "uf" in row and pd.notna(row["uf"]) else None
            db_mun = db.query(DimMunicipio).filter(DimMunicipio.codigo_ibge == cod).first()
            if not db_mun:
                db_mun = DimMunicipio(codigo_ibge=cod, nome=row["municipio"], uf=uf)
                db.add(db_mun)
                novos_objetos.append((cod, db_mun, row["municipio"].lower().strip(), uf))
                novos_zarc += 1
            else:
                mun_map_ibge[cod] = db_mun.id_municipio
                if uf:
                    mun_map_name[(row["municipio"].lower().strip(), uf)] = db_mun.id_municipio

        if novos_zarc:
            try:
                db.commit()
                log.info(f"Dim Município (ZARC): {novos_zarc} novo(s) município(s) inserido(s).")
            except Exception as e:
                db.rollback()
                log.error(f"Dim Município (ZARC): falha no commit bulk — {e}")
                raise

        for cod, obj, nome_norm, uf in novos_objetos:
            db.refresh(obj)
            mun_map_ibge[cod] = obj.id_municipio
            if uf:
                mun_map_name[(nome_norm, uf)] = obj.id_municipio

    log.info(f"Dim Município: total de {len(mun_map_ibge)} município(s) mapeado(s) (código IBGE).")
    return mun_map_ibge, mun_map_name

def clear_facts():
    # DDL: Limpeza de tabelas fato (Safe re-run)
    with engine.begin() as conn:
        # Tabelas de produção/registro podem ser truncadas pois os arquivos geralmente trazem o histórico completo.
        # Tabelas de preço são mantidas para acumular série histórica (conforme pedido pelo USER).
        conn.execute(text("TRUNCATE TABLE fato_registro_cultivares, fato_producao_pam, fato_risco_zarc, fato_producao_conab, fato_agrofit RESTART IDENTITY CASCADE;"))
        log.info("Tabelas Fato (exceto Preços) truncadas para recarga segura.")

def get_cultura_id(nome_cultura, mapping):
    if not nome_cultura: return None

    def norm(s):
        import unicodedata
        s = str(s).lower().strip()
        # Remove acentos
        s = "".join(c for c in unicodedata.normalize('NFKD', s) if unicodedata.category(c) != 'Mn')
        # Normaliza separadores: hífen e underline viram espaço
        return s.replace("-", " ").replace("_", " ")

    # Tenta match exato primeiro (antes de normalizar)
    if nome_cultura in mapping: return mapping[nome_cultura]

    nombre_norm = norm(nome_cultura)
    for alvo, cid in mapping.items():
        alvo_norm = norm(alvo)
        if alvo_norm in nombre_norm or nombre_norm in alvo_norm:
            return cid
    return None

def upsert_data(model, df, index_elements):
    if df.empty: return
    
    # Converte DF para dicts
    records = df.to_dict(orient="records")
    
    # Filtra colunas que existem no modelo
    from sqlalchemy import inspect
    model_cols = [c.key for c in inspect(model).mapper.column_attrs]
    valid_records = []
    for r in records:
        valid_records.append({k: v for k, v in r.items() if k in model_cols})

    stmt = insert(model).values(valid_records)
    
    # Colunas para atualizar em caso de conflito (todas exceto as do índice)
    update_cols = {c: stmt.excluded[c] for c in model_cols if c not in index_elements and c != 'data_modificacao'}
    
    upsert_stmt = stmt.on_conflict_do_update(
        index_elements=index_elements,
        set_=update_cols
    )
    
    with engine.begin() as conn:
        conn.execute(upsert_stmt)

def main():
    parser = argparse.ArgumentParser(description="Pipeline Agro-Dados")
    parser.add_argument("--sources", nargs="+", choices=EXTRACTORS.keys(), default=list(EXTRACTORS.keys()), help="Fontes de dados a serem extraídas")
    parser.add_argument("--refresh-conab", action="store_true", help="Força o download dos arquivos CONAB")
    args = parser.parse_args()

    log.info("--- Iniciando Orquestração do Pipeline Agro ---")
    log.info(f"Fontes selecionadas: {args.sources}")
    init_db()
    db = next(get_db())
    # clear_facts() # Removido para suportar carga incremental conforme pedido pelo USER
    
    culturas_alvo = ["soja", "milho", "trigo", "algodão", "cana-de-açúcar"]
    
    # ETL: Instanciação de extratores
    instances = {}
    for source in args.sources:
        if source == "cultivares":
            instances[source] = CultivaresExtractor(use_cache=True)
        elif source == "sidra":
            ext = SidraExtractor(ano="2022")
            ext.TARGET_CROPS = {k: ext.TARGET_CROPS[k] for k in culturas_alvo if k in ext.TARGET_CROPS}
            instances[source] = ext
        elif source == "zarc":
            ext = ZarcExtractor()
            ext.TARGET_CROPS = [c.replace("ç", "c").replace("ã", "a") for c in culturas_alvo]
            instances[source] = ext
        elif source == "conab":
            instances[source] = ConabExtractor(force_refresh=args.refresh_conab)
        elif source == "agrofit":
            instances[source] = AgrofitExtractor()
        elif source == "fertilizantes":
            instances[source] = FertilizantesExtractor()
        elif source == "sigef":
            instances[source] = SigefExtractor()

    # Extração: Processamento paralelo (Threads)
    log.info("Executando extrações em paralelo...")
    dfs = {}
    if instances:
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(instances)) as executor:
            future_to_source = {executor.submit(ext.run): source for source, ext in instances.items()}
            for future in concurrent.futures.as_completed(future_to_source):
                source = future_to_source[future]
                try:
                    dfs[source] = future.result()
                    log.info(f"Extração {source} finalizada.")
                except Exception as exc:
                    log.error(f"Extrator {source} gerou erro: {exc}")
                    dfs[source] = pd.DataFrame()

    df_cult = dfs.get("cultivares", pd.DataFrame())
    df_pam = dfs.get("sidra", pd.DataFrame())
    df_zarc = dfs.get("zarc", pd.DataFrame())
    df_conab = dfs.get("conab", pd.DataFrame())
    df_agrofit = dfs.get("agrofit", pd.DataFrame())
    df_fert = dfs.get("fertilizantes", pd.DataFrame())
    df_sigef = dfs.get("sigef", pd.DataFrame())
    
    # DML: Carga de tabelas dimensão
    log.info("Carregando Dimensões (Cultura, Mantenedor, Município)...")
    map_cult = preencher_dimensao_cultura(db, culturas_alvo)
    map_mant = preencher_dimensao_mantenedor(db, df_cult)
    map_mun, map_mun_name = preencher_dimensao_municipio(db, df_pam, df_zarc)
    
    # DML: Carga de tabelas fato (FK Lookup)
    log.info("Mapeando Chaves Estrangeiras nas Tabelas Fato...")

    # Fatos: Registros SNPC (MAPA)
    if not df_cult.empty:
        df_cult_f = df_cult.copy()
        df_cult_f["id_cultura"] = df_cult_f["cultura"].apply(lambda x: get_cultura_id(x, map_cult))
        df_cult_f["id_mantenedor"] = df_cult_f["mantenedor"].map(map_mant)
        
        cols_cult = ["nr_registro", "id_cultura", "id_mantenedor", "cultivar", "nome_secundario", 
                     "situacao", "nr_formulario", "data_reg", "data_val"]
        df_cult_f = df_cult_f[[c for c in cols_cult if c in df_cult_f.columns]]
        df_cult_f = df_cult_f.drop_duplicates(subset=["nr_registro"]).dropna(subset=["cultivar"])
        
        upsert_data(FatoCultivar, df_cult_f, index_elements=['nr_registro'])
        log.info(f"Fato Cultivares: Upsert concluído para {len(df_cult_f)} registros.")

    # Fatos: Produção Municipal (PAM/IBGE)
    if not df_pam.empty:
        df_pam_f = df_pam.copy()
        df_pam_f["id_cultura"] = df_pam_f["cultura"].apply(lambda x: get_cultura_id(x, map_cult))
        # Normalização: Código Municipal
        df_pam_f["cod_municipio_ibge"] = df_pam_f["cod_municipio_ibge"].astype(str).str[:7]
        df_pam_f["id_municipio"] = df_pam_f["cod_municipio_ibge"].map(map_mun)
        
        cols_pam = ["id_cultura", "id_municipio", "ano", "area_plantada_ha", "area_colhida_ha", "qtde_produzida_ton", "valor_producao_mil_reais"]
        for c in cols_pam:
            if c not in df_pam_f.columns: df_pam_f[c] = None
        df_pam_f = df_pam_f[cols_pam].dropna(subset=["id_cultura", "id_municipio"])
        
        upsert_data(FatoProducaoPAM, df_pam_f, index_elements=['id_cultura', 'id_municipio', 'ano'])
        log.info(f"Fato Producao PAM: Upsert concluído para {len(df_pam_f)} registros.")

    # Fatos: Zoneamento de Risco (ZARC/MAPA)
    if not df_zarc.empty:
        df_zarc_f = df_zarc.copy()
        df_zarc_f["id_cultura"] = df_zarc_f["cultura"].apply(lambda x: get_cultura_id(x, map_cult))
        df_zarc_f["cod_municipio_ibge"] = df_zarc_f["cod_municipio_ibge"].astype(str).str[:7]
        df_zarc_f["id_municipio"] = df_zarc_f["cod_municipio_ibge"].map(map_mun)
        
        # Transform: Mapeamento de colunas dinâmicas
        renames_zarc = {}
        for c in df_zarc_f.columns:
            if "solo" in c: renames_zarc[c] = "tipo_solo"
            elif "decendio" in c or "periodo" in c: renames_zarc[c] = "periodo_plantio"
            elif "risco" in c or "riscoclima" in c: renames_zarc[c] = "risco_climatico"
        df_zarc_f = df_zarc_f.rename(columns=renames_zarc)
        
        cols_zarc = ["id_cultura", "id_municipio", "tipo_solo", "periodo_plantio", "risco_climatico"]
        for c in cols_zarc:
            if c not in df_zarc_f.columns: df_zarc_f[c] = None
        df_zarc_f = df_zarc_f[cols_zarc].dropna(subset=["id_cultura", "id_municipio"])
        
        upsert_data(FatoRiscoZARC, df_zarc_f, index_elements=['id_cultura', 'id_municipio', 'tipo_solo', 'periodo_plantio'])
        log.info(f"Fato Risco ZARC: Upsert concluído para {len(df_zarc_f)} registros.")

    # Fatos: CONAB (Produção e Preços)
    if isinstance(df_conab, dict):
        for key, df in df_conab.items():
            if df.empty: continue
            
            df_f = df.copy()
            df_f["id_cultura"] = df_f["cultura"].apply(lambda x: get_cultura_id(x, map_cult))
            df_f = df_f.dropna(subset=["id_cultura"])
            
            if "cod_municipio_ibge" in df_f.columns:
                df_f["id_municipio"] = df_f["cod_municipio_ibge"].astype(str).str[:7].map(map_mun)
            else:
                df_f["id_municipio"] = None

            if key in ["producao_historica", "producao_estimativa"]:
                cols_conab = ["id_cultura", "uf", "ano_agricola", "safra", "area_plantada_mil_ha", "producao_mil_t", "produtividade_t_ha"]
                df_upsert = df_f[cols_conab]
                upsert_data(FatoProducaoConab, df_upsert, index_elements=['id_cultura', 'uf', 'ano_agricola', 'safra'])
                log.info(f"Produção CONAB ({key}): Upsert concluído.")
                
            elif key in ["precos_uf_mensal", "precos_mun_mensal"]:
                cols_p = ["id_cultura", "id_municipio", "uf", "ano", "mes", "valor_kg", "nivel_comercializacao"]
                for c in cols_p:
                    if c not in df_f.columns: df_f[c] = None
                
                upsert_data(FatoPrecoConabMensal, df_f[cols_p], index_elements=['id_cultura', 'id_municipio', 'uf', 'ano', 'mes', 'nivel_comercializacao'])
                log.info(f"Preços Mensais CONAB ({key}): Upsert concluído.")

            elif key in ["precos_uf_semanal", "precos_mun_semanal"]:
                # Política Semanal: Resetar a cada 4 semanas
                # Usa context manager para garantir fechamento da conexão
                with engine.connect() as _conn:
                    result = _conn.execute(text("SELECT COUNT(DISTINCT semana) FROM fato_precos_conab_semanal"))
                    count_semanas = result.scalar() or 0
                log.info(f"Política semanal: {count_semanas} semana(s) acumulada(s) na série.")
                if count_semanas >= 4:
                    log.info("Resetando série semanal (política 4 semanas atingida).")
                    with engine.begin() as _conn:
                        _conn.execute(text("TRUNCATE TABLE fato_precos_conab_semanal"))
                
                cols_s = ["id_cultura", "id_municipio", "uf", "ano", "mes", "semana", "data_referencia", "valor_kg", "nivel_comercializacao"]
                for c in cols_s:
                    if c not in df_f.columns: df_f[c] = None
                
                upsert_data(FatoPrecoConabSemanal, df_f[cols_s], index_elements=['id_cultura', 'id_municipio', 'uf', 'ano', 'mes', 'semana', 'nivel_comercializacao'])
                log.info(f"Preços Semanais CONAB ({key}): Upsert concluído.")
    elif not df_conab.empty:
        # Fallback para o comportamento antigo caso algo falhe
        df_conab_f = df_conab.copy()
        df_conab_f["id_cultura"] = df_conab_f["cultura"].apply(lambda x: get_cultura_id(x, map_cult))
        df_conab_f = df_conab_f.dropna(subset=["id_cultura"])
        cols_conab = ["id_cultura", "uf", "ano_agricola", "safra", "area_plantada_mil_ha", "producao_mil_t", "produtividade_t_ha"]
        df_conab_f[cols_conab].to_sql("fato_producao_conab", engine, if_exists="append", index=False)
        log.info(f"Fato Produção CONAB (Legacy): inseridos {len(df_conab_f)} registros.")

    # Fatos: Agrotóxicos (Agrofit/MAPA)
    if not df_agrofit.empty:
        df_agrofit_f = df_agrofit.copy()
        df_agrofit_f["id_cultura"] = df_agrofit_f["cultura"].apply(lambda x: get_cultura_id(x, map_cult))
        
        # Filtramos para as culturas que temos na dimensão (as alvo)
        df_agrofit_f = df_agrofit_f.dropna(subset=["id_cultura"])
        
        cols_agro = ["id_cultura", "nr_registro", "marca_comercial", "ingrediente_ativo", 
                     "titular_registro", "classe", "situacao", "praga_comum"]
        df_agrofit_f = df_agrofit_f[cols_agro]
        
        upsert_data(FatoAgrofit, df_agrofit_f, index_elements=['id_cultura', 'nr_registro', 'marca_comercial', 'praga_comum'])
        log.info(f"Fato Agrofit: Upsert concluído para {len(df_agrofit_f)} registros.")
    
    # Fatos: Fertilizantes (Estabelecimentos)
    if not df_fert.empty:
        df_fert_f = df_fert.copy()
        
        # Mapeamento de Município por Nome + UF
        df_fert_f["id_municipio"] = df_fert_f.apply(
            lambda x: map_mun_name.get((x["municipio"].lower().strip(), x["uf"].upper())), axis=1
        )
        
        # Filtros e Upsert
        df_fert_f = df_fert_f.drop_duplicates(subset=["nr_registro_estabelecimento"])
        
        upsert_data(FatoFertilizante, df_fert_f, index_elements=['nr_registro_estabelecimento'])
        log.info(f"Fato Fertilizantes: Upsert concluído para {len(df_fert_f)} estabelecimentos.")
 
    # Fatos: SIGEF (Produção e Uso Próprio)
    if isinstance(df_sigef, dict):
        for key, df in df_sigef.items():
            if df.empty: continue
            df_f = df.copy()
            df_f["id_cultura"] = df_f["cultura"].apply(lambda x: get_cultura_id(x, map_cult))
            df_f["id_municipio"] = df_f.apply(
                lambda x: map_mun_name.get((x["municipio"].lower().strip(), x["uf"].upper())), axis=1
            )
            df_f = df_f.dropna(subset=["id_cultura", "id_municipio"])
 
            if key == "campos_producao":
                index_cols = ['id_cultura', 'id_municipio', 'safra', 'especie', 'cultivar_raw', 'categoria']
                upsert_data(FatoSigefProducao, df_f, index_elements=index_cols)
                log.info(f"Fato SIGEF Produção: Upsert concluído para {len(df_f)} registros.")
            elif key == "uso_proprio":
                index_cols = ['id_cultura', 'id_municipio', 'periodo', 'especie', 'cultivar_raw']
                upsert_data(FatoSigefUsoProprio, df_f, index_elements=index_cols)
                log.info(f"Fato SIGEF Uso Próprio: Upsert concluído para {len(df_f)} registros.")

    log.info("--- Pipeline Concluído ---")

if __name__ == "__main__":
    main()
