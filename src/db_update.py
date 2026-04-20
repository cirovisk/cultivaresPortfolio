import logging
import os
import sys
import argparse
from datetime import datetime
from db.manager import init_db, get_db, DimMunicipio
from pipeline.loaders import (
    preencher_dimensao_cultura, preencher_dimensao_mantenedor, 
    preencher_dimensao_municipio, load_fact_cultivares, load_fact_pam,
    load_fact_zarc, load_fact_conab, load_fact_agrofit, 
    load_fact_fertilizantes, load_fact_sigef, load_fact_meteorologia
)
from pipeline.cultivares import CultivaresExtractor
from pipeline.sidra import SidraExtractor
from pipeline.zarc import ZarcExtractor
from pipeline.conab import ConabExtractor
from pipeline.agrofit import AgrofitExtractor
from pipeline.fertilizantes import FertilizantesExtractor
from pipeline.sigef import SigefExtractor
from pipeline.inmet import InmetExtractor

# Configuração de Logging Duplo
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
log_file = os.path.join(LOG_DIR, "update_history.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger("DB_Update")

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

def run_update(sources=None):
    if sources is None:
        sources = list(EXTRACTORS.keys())

    log.info(f"=== Iniciando Atualização Periódica: {datetime.now()} ===")
    log.info(f"Fontes alvo: {sources}")

    init_db()
    db = next(get_db())
    culturas_alvo = ["soja", "milho", "trigo", "algodão", "cana-de-açúcar"]

    for source in sources:
        try:
            log.info(f"Processando {source.upper()}...")
            
            # Instanciação específica para atualização
            if source == "cultivares":
                ext = CultivaresExtractor(use_cache=True)
                df = ext.run()
                map_cult = preencher_dimensao_cultura(db, culturas_alvo)
                map_mant = preencher_dimensao_mantenedor(db, df)
                load_fact_cultivares(db, df, map_cult, map_mant)
            
            elif source == "sidra":
                ext = SidraExtractor(ano="2022", use_cache=True)
                ext.TARGET_CROPS = {k: ext.TARGET_CROPS[k] for k in culturas_alvo if k in ext.TARGET_CROPS}
                df = ext.run()
                map_cult = preencher_dimensao_cultura(db, culturas_alvo)
                map_mun, _ = preencher_dimensao_municipio(db, df_pam=df)
                load_fact_pam(db, df, map_cult, map_mun)
            
            elif source == "zarc":
                ext = ZarcExtractor()
                ext.TARGET_CROPS = [c.replace("ç", "c").replace("ã", "a") for c in culturas_alvo]
                df = ext.run()
                map_cult = preencher_dimensao_cultura(db, culturas_alvo)
                map_mun, _ = preencher_dimensao_municipio(db, df_zarc=df)
                load_fact_zarc(db, df, map_cult, map_mun)
            
            elif source == "conab":
                ext = ConabExtractor(force_refresh=False) # Usa triggers internos (7/30 dias)
                df_dict = ext.run()
                map_cult = preencher_dimensao_cultura(db, culturas_alvo)
                # Municípios do CONAB geralmente já estão mapeados via PAM/ZARC na primeira carga
                # Mas carregamos as FKs
                map_mun, _ = preencher_dimensao_municipio(db)
                load_fact_conab(db, df_dict, map_cult, map_mun)
            
            elif source == "agrofit":
                ext = AgrofitExtractor()
                df = ext.run()
                map_cult = preencher_dimensao_cultura(db, culturas_alvo)
                load_fact_agrofit(db, df, map_cult)
            
            elif source == "fertilizantes":
                ext = FertilizantesExtractor()
                df = ext.run()
                _, map_mun_name = preencher_dimensao_municipio(db)
                load_fact_fertilizantes(db, df, map_mun_name)
            
            elif source == "sigef":
                ext = SigefExtractor()
                df_dict = ext.run()
                map_cult = preencher_dimensao_cultura(db, culturas_alvo)
                _, map_mun_name = preencher_dimensao_municipio(db)
                load_fact_sigef(db, df_dict, map_cult, map_mun_name)
            
            elif source == "inmet":
                ext = InmetExtractor(days_history=30) # Atualização rasteira (último mês)
                all_muns = db.query(DimMunicipio).all()
                load_fact_meteorologia(db, None, ext, all_muns)

            log.info(f"Fonte {source} finalizada com sucesso.")

        except Exception as e:
            log.error(f"Erro crítico ao atualizar {source}: {e}", exc_info=True)

    log.info("=== Atualização Finalizada ===")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sources", nargs="+", help="Fontes específicas para atualizar")
    args = parser.parse_args()
    
    run_update(sources=args.sources)
