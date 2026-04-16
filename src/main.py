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
from db.manager import init_db, get_db, engine, DimCultura, DimMunicipio, DimMantenedor, FatoProducaoConab, FatoAgrofit

EXTRACTORS = {
    "cultivares": CultivaresExtractor,
    "sidra": SidraExtractor,
    "zarc": ZarcExtractor,
    "conab": ConabExtractor,
    "agrofit": AgrofitExtractor
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

def preencher_dimensao_cultura(db, culturas_lista):
    culturas_map = {}
    for c in culturas_lista:
        c_norm = c.strip().lower()
        db_cultura = db.query(DimCultura).filter(DimCultura.nome_padronizado == c_norm).first()
        if not db_cultura:
            db_cultura = DimCultura(nome_padronizado=c_norm)
            db.add(db_cultura)
            db.commit()
            db.refresh(db_cultura)
        culturas_map[c_norm] = db_cultura.id_cultura
    return culturas_map

def preencher_dimensao_mantenedor(db, df_cult):
    mant_map = {}
    if "mantenedor" not in df_cult.columns: return mant_map
    
    unique_mants = df_cult[["mantenedor", "SETOR"]].drop_duplicates().dropna(subset=["mantenedor"])
    for _, row in unique_mants.iterrows():
        nome = row["mantenedor"]
        setor = row["SETOR"]
        db_mant = db.query(DimMantenedor).filter(DimMantenedor.nome == nome).first()
        if not db_mant:
            db_mant = DimMantenedor(nome=nome, setor=setor)
            db.add(db_mant)
            db.commit()
            db.refresh(db_mant)
        mant_map[nome] = db_mant.id_mantenedor
    return mant_map

def preencher_dimensao_municipio(db, df_pam, df_zarc):
    mun_map = {}
    
    # Extrair do PAM
    pam_muns = df_pam[["cod_municipio_ibge", "municipio_nome"]].drop_duplicates().dropna(subset=["cod_municipio_ibge"]) if not df_pam.empty else []
    
    # Insere do PAM
    for _, row in pam_muns.iterrows() if not df_pam.empty else []:
        cod = str(row["cod_municipio_ibge"])[:7] # Garantir 7 digitos
        if not cod: continue
        db_mun = db.query(DimMunicipio).filter(DimMunicipio.codigo_ibge == cod).first()
        if not db_mun:
            db_mun = DimMunicipio(codigo_ibge=cod, nome=row["municipio_nome"])
            db.add(db_mun)
            try: db.commit()
            except: db.rollback()
            db.refresh(db_mun)
        mun_map[cod] = db_mun.id_municipio
        
    # Validar do ZARC (as vezes tem municípios que não estão no PAM)
    if not df_zarc.empty and "cod_municipio_ibge" in df_zarc.columns:
        zarc_muns = df_zarc[["cod_municipio_ibge", "municipio"]].drop_duplicates().dropna(subset=["cod_municipio_ibge"])
        for _, row in zarc_muns.iterrows():
            cod = str(row["cod_municipio_ibge"])[:7]
            if not cod or cod in mun_map: continue
            
            db_mun = db.query(DimMunicipio).filter(DimMunicipio.codigo_ibge == cod).first()
            if not db_mun:
                db_mun = DimMunicipio(codigo_ibge=cod, nome=row["municipio"])
                db.add(db_mun)
                try: db.commit()
                except: db.rollback()
                db.refresh(db_mun)
            mun_map[cod] = db_mun.id_municipio
            
    return mun_map

def clear_facts():
    # Truncate tables to allow safe re-run
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE fato_registro_cultivares, fato_producao_pam, fato_risco_zarc, fato_producao_conab, fato_agrofit RESTART IDENTITY CASCADE;"))
        log.info("Tabelas Fato truncadas para recarga segura.")

def main():
    parser = argparse.ArgumentParser(description="Pipeline Agro-Dados")
    parser.add_argument("--sources", nargs="+", choices=EXTRACTORS.keys(), default=list(EXTRACTORS.keys()), help="Fontes de dados a serem extraídas")
    args = parser.parse_args()

    log.info("--- Iniciando Orquestração do Pipeline Agro ---")
    log.info(f"Fontes selecionadas: {args.sources}")
    init_db()
    db = next(get_db())
    clear_facts()
    
    culturas_alvo = ["soja", "milho", "trigo", "algodão", "cana-de-açúcar"]
    
    # Instanciar os extratores selecionados
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
            instances[source] = ConabExtractor()
        elif source == "agrofit":
            instances[source] = AgrofitExtractor()



    # Executar extrações em paralelo
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


    
    # 1. Carregar Dimensões e buscar Mapas de IDs
    log.info("Carregando Dimensões (Cultura, Mantenedor, Município)...")
    map_cult = preencher_dimensao_cultura(db, culturas_alvo)
    map_mant = preencher_dimensao_mantenedor(db, df_cult)
    map_mun = preencher_dimensao_municipio(db, df_pam, df_zarc)
    
    # 2. Transformar Fatos substituindo chaves naturais por chaves estrangeiras
    log.info("Mapeando Chaves Estrangeiras nas Tabelas Fato...")
    
    # Helper para mapeamento flexível de cultura
    def get_cultura_id(nome_cultura, mapping):
        if not nome_cultura: return None
        
        def norm(s):
            import unicodedata
            s = str(s).lower().strip()
            return "".join(c for c in unicodedata.normalize('NFKD', s) if unicodedata.category(c) != 'Mn')

        # Tenta match exato primeiro
        if nome_cultura in mapping: return mapping[nome_cultura]
        
        nombre_norm = norm(nome_cultura)
        for alvo, cid in mapping.items():
            alvo_norm = norm(alvo)
            if alvo_norm in nombre_norm or nombre_norm in alvo_norm:
                return cid
        return None

    # -- Cultivares --
    if not df_cult.empty:
        df_cult_f = df_cult.copy()
        df_cult_f["id_cultura"] = df_cult_f["cultura"].apply(lambda x: get_cultura_id(x, map_cult))
        df_cult_f["id_mantenedor"] = df_cult_f["mantenedor"].map(map_mant)
        
        cols_cult = ["nr_registro", "id_cultura", "id_mantenedor", "cultivar", "nome_secundario", 
                     "situacao", "nr_formulario", "data_reg", "data_val"]
        df_cult_f = df_cult_f[[c for c in cols_cult if c in df_cult_f.columns]]
        df_cult_f = df_cult_f.drop_duplicates(subset=["nr_registro"]).dropna(subset=["cultivar"])
        
        df_cult_f.to_sql("fato_registro_cultivares", engine, if_exists="append", index=False)
        log.info(f"Fato Cultivares: inseridos {len(df_cult_f)} registros.")

    # -- PAM --
    if not df_pam.empty:
        df_pam_f = df_pam.copy()
        df_pam_f["id_cultura"] = df_pam_f["cultura"].apply(lambda x: get_cultura_id(x, map_cult))
        # PAM IBGE -> codigo ibge = string, cut to 7 if necessary
        df_pam_f["cod_municipio_ibge"] = df_pam_f["cod_municipio_ibge"].astype(str).str[:7]
        df_pam_f["id_municipio"] = df_pam_f["cod_municipio_ibge"].map(map_mun)
        
        cols_pam = ["id_cultura", "id_municipio", "ano", "area_plantada_ha", "area_colhida_ha", "qtde_produzida_ton", "valor_producao_mil_reais"]
        for c in cols_pam:
            if c not in df_pam_f.columns: df_pam_f[c] = None
        df_pam_f = df_pam_f[cols_pam].dropna(subset=["id_cultura", "id_municipio"])
        
        df_pam_f.to_sql("fato_producao_pam", engine, if_exists="append", index=False)
        log.info(f"Fato Producao PAM: inseridos {len(df_pam_f)} registros.")

    # -- ZARC --
    if not df_zarc.empty:
        df_zarc_f = df_zarc.copy()
        df_zarc_f["id_cultura"] = df_zarc_f["cultura"].apply(lambda x: get_cultura_id(x, map_cult))
        df_zarc_f["cod_municipio_ibge"] = df_zarc_f["cod_municipio_ibge"].astype(str).str[:7]
        df_zarc_f["id_municipio"] = df_zarc_f["cod_municipio_ibge"].map(map_mun)
        
        # Mapeando colunas do CSV dinâmico do MAPA
        # 'solo', 'decendio', 'risco'
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
        
        df_zarc_f.to_sql("fato_risco_zarc", engine, if_exists="append", index=False)
        log.info(f"Fato Risco ZARC: inseridos {len(df_zarc_f)} registros.")

    # -- CONAB --
    if not df_conab.empty:
        df_conab_f = df_conab.copy()
        df_conab_f["id_cultura"] = df_conab_f["cultura"].apply(lambda x: get_cultura_id(x, map_cult))
        
        # Filtramos para as culturas que temos na dimensão (as alvo)
        df_conab_f = df_conab_f.dropna(subset=["id_cultura"])
        
        cols_conab = ["id_cultura", "uf", "ano_agricola", "safra", "area_plantada_mil_ha", "producao_mil_t", "produtividade_t_ha"]
        df_conab_f = df_conab_f[cols_conab]
        
        df_conab_f.to_sql("fato_producao_conab", engine, if_exists="append", index=False)
        log.info(f"Fato Produção CONAB: inseridos {len(df_conab_f)} registros.")

    # -- Agrofit --
    if not df_agrofit.empty:
        df_agrofit_f = df_agrofit.copy()
        df_agrofit_f["id_cultura"] = df_agrofit_f["cultura"].apply(lambda x: get_cultura_id(x, map_cult))
        
        # Filtramos para as culturas que temos na dimensão (as alvo)
        df_agrofit_f = df_agrofit_f.dropna(subset=["id_cultura"])
        
        cols_agro = ["id_cultura", "nr_registro", "marca_comercial", "ingrediente_ativo", 
                     "titular_registro", "classe", "situacao", "praga_comum"]
        df_agrofit_f = df_agrofit_f[cols_agro]
        
        # O Agrofit pode ter duplicatas devido ao cruzamento Cultura x Praga, vamos manter assim por enquanto
        df_agrofit_f.to_sql("fato_agrofit", engine, if_exists="append", index=False)
        log.info(f"Fato Agrofit: inseridos {len(df_agrofit_f)} registros.")

    log.info("--- Pipeline Concluído ---")

if __name__ == "__main__":
    main()
