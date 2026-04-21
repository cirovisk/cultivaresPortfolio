import argparse
import logging
import pandas as pd
import gc
from pipeline.cultivares import CultivaresExtractor
from pipeline.sidra import SidraExtractor
from pipeline.zarc import ZarcExtractor
from pipeline.conab import ConabExtractor
from pipeline.agrofit import AgrofitExtractor
from pipeline.fertilizantes import FertilizantesExtractor
from pipeline.sigef import SigefExtractor
from pipeline.inmet import InmetExtractor
from db.manager import init_db, get_db, DimMunicipio
from pipeline.loaders import (
    preencher_dimensao_cultura, preencher_dimensao_mantenedor, 
    preencher_dimensao_municipio, carregar_municipios_completo_ibge,
    load_fact_cultivares, load_fact_pam,
    load_fact_zarc, load_fact_conab, load_fact_agrofit, 
    load_fact_fertilizantes, load_fact_sigef, load_fact_meteorologia
)

from pipeline.cleaners.conab import clean_conab
from pipeline.cleaners.sigef import clean_sigef
from pipeline.cleaners.cultivares import clean_cultivares
from pipeline.cleaners.sidra import clean_sidra
from pipeline.cleaners.zarc import clean_zarc
from pipeline.cleaners.agrofit import clean_agrofit
from pipeline.cleaners.fertilizantes import clean_fertilizantes
from pipeline.cleaners.inmet import clean_inmet

EXTRACTORS = {
    "cultivares": CultivaresExtractor,
    "sidra": SidraExtractor,
    "zarc": ZarcExtractor,
    "conab": ConabExtractor,
    "agrofit": AgrofitExtractor,
    "fertilizantes": FertilizantesExtractor,
    "sigef": SigefExtractor,
    "inmet": InmetExtractor
}

CLEANERS = {
    "cultivares": clean_cultivares,
    "sidra": clean_sidra,
    "zarc": clean_zarc,
    "conab": clean_conab,
    "agrofit": clean_agrofit,
    "fertilizantes": clean_fertilizantes,
    "sigef": clean_sigef,
    "inmet": clean_inmet
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

def run_step(source_name, ext_instance):
    """Executa extração e limpeza para uma única fonte."""
    log.info(f"Processando fonte: [{source_name}]")
    try:
        raw_data = ext_instance.extract()
        cleaner_func = CLEANERS.get(source_name)
        if cleaner_func:
            df = cleaner_func(raw_data)
        else:
            df = raw_data
        return df
    except Exception as e:
        log.error(f"Erro ao processar [{source_name}]: {e}")
        return pd.DataFrame()

def main():
    parser = argparse.ArgumentParser(description="Pipeline Agro-Dados (Otimizado)")
    parser.add_argument("--sources", nargs="+", choices=EXTRACTORS.keys(), default=list(EXTRACTORS.keys()), help="Fontes de dados")
    parser.add_argument("--refresh-conab", action="store_true", help="Força download CONAB")
    args = parser.parse_args()

    log.info("--- Iniciando Orquestração do Pipeline Agro (Serial & Low Memory) ---")
    init_db()
    db = next(get_db())

    culturas_alvo = ["soja", "milho", "trigo", "algodão", "cana-de-açúcar"]
    
    # 1. DimCultura (Sempre primeiro)
    map_cult = preencher_dimensao_cultura(db, culturas_alvo)

    # 2. Dimensões baseadas em Município (Sempre garantindo carga completa primeiro)
    map_mun, map_mun_name = carregar_municipios_completo_ibge(db)

    # 3. Cultivares (Depende de Mantenedor)
    df_cult = pd.DataFrame()
    if "cultivares" in args.sources:
        df_cult = run_step("cultivares", CultivaresExtractor(use_cache=True))
        map_mant = preencher_dimensao_mantenedor(db, df_cult)
        load_fact_cultivares(db, df_cult, map_cult, map_mant)
        del df_cult
        gc.collect()

    # --- SIDRA e ZARC (Populam DimMunicipio) ---
    df_sidra = pd.DataFrame()
    if "sidra" in args.sources:
        ext = SidraExtractor()
        ext.TARGET_CROPS = {k: ext.TARGET_CROPS[k] for k in culturas_alvo if k in ext.TARGET_CROPS}
        df_sidra = run_step("sidra", ext)

    df_zarc_muns = pd.DataFrame()
    zarc_gen = None
    if "zarc" in args.sources:
        ext_zarc = ZarcExtractor()
        ext_zarc.TARGET_CROPS = [c.replace("ç", "c").replace("ã", "a") for c in culturas_alvo]
        df_zarc_muns = ext_zarc.get_municipios_only()
        zarc_gen = ext_zarc.extract()

    if not df_sidra.empty or zarc_gen:
        if not df_sidra.empty:
            load_fact_pam(db, df_sidra, map_cult, map_mun)
            del df_sidra
        
        if zarc_gen:
            log.info("Carregando Fatos ZARC via Streaming...")
            load_fact_zarc(db, zarc_gen, map_cult, map_mun)
            del zarc_gen
        
        gc.collect()

    # 3. Outras Fontes (Sequencial)
    
    # --- CONAB ---
    if "conab" in args.sources:
        df_conab = run_step("conab", ConabExtractor(force_refresh=args.refresh_conab))
        load_fact_conab(db, df_conab, map_cult, map_mun)
        del df_conab
        gc.collect()

    # --- AGROFIT (A mais pesada) ---
    if "agrofit" in args.sources:
        df_agrofit = run_step("agrofit", AgrofitExtractor())
        load_fact_agrofit(db, df_agrofit, map_cult)
        del df_agrofit
        gc.collect()

    # --- FERTILIZANTES ---
    if "fertilizantes" in args.sources:
        df_fert = run_step("fertilizantes", FertilizantesExtractor())
        load_fact_fertilizantes(db, df_fert, map_mun_name)
        del df_fert
        gc.collect()

    # --- SIGEF ---
    if "sigef" in args.sources:
        df_sigef = run_step("sigef", SigefExtractor())
        load_fact_sigef(db, df_sigef, map_cult, map_mun_name)
        del df_sigef
        gc.collect()

    # --- INMET ---
    if "inmet" in args.sources:
        log.info("Processando Meteorologia...")
        all_muns = db.query(DimMunicipio).all()
        ext_inmet = InmetExtractor(days_history=730)
        load_fact_meteorologia(db, pd.DataFrame(), ext_inmet, all_muns)
        gc.collect()

    log.info("--- Pipeline Concluído com Sucesso ---")

if __name__ == "__main__":
    main()
