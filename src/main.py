import argparse
import logging
import concurrent.futures
import pandas as pd
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
    preencher_dimensao_municipio, load_fact_cultivares, load_fact_pam,
    load_fact_zarc, load_fact_conab, load_fact_agrofit, 
    load_fact_fertilizantes, load_fact_sigef, load_fact_meteorologia
)

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

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Pipeline Agro-Dados")
    parser.add_argument("--sources", nargs="+", choices=EXTRACTORS.keys(), default=list(EXTRACTORS.keys()), help="Fontes de dados a serem extraídas")
    parser.add_argument("--refresh-conab", action="store_true", help="Força o download dos arquivos CONAB")
    args = parser.parse_args()

    log.info("--- Iniciando Orquestração do Pipeline Agro ---")
    log.info(f"Fontes selecionadas: {args.sources}")
    init_db()
    db = next(get_db())

    
    culturas_alvo = ["soja", "milho", "trigo", "algodão", "cana-de-açúcar"]
    
    # ETL: Instanciação
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
        elif source == "inmet":
            instances[source] = InmetExtractor(days_history=730)

    # Extração e Limpeza: Processamento paralelo Funcional
    log.info("Executando extrações e limpezas em cadeia paralela...")
    dfs = {}
    
    from pipeline.cleaners.conab import clean_conab
    from pipeline.cleaners.sigef import clean_sigef
    from pipeline.cleaners.cultivares import clean_cultivares
    from pipeline.cleaners.sidra import clean_sidra
    from pipeline.cleaners.zarc import clean_zarc
    from pipeline.cleaners.agrofit import clean_agrofit
    from pipeline.cleaners.fertilizantes import clean_fertilizantes
    from pipeline.cleaners.inmet import clean_inmet

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

    def pipeline_task(source_name, ext_instance):
        if source_name == "inmet":
            return pd.DataFrame()
            
        raw_data = ext_instance.extract()
        cleaner_func = CLEANERS.get(source_name)
        if cleaner_func:
            return cleaner_func(raw_data)
        return raw_data

    if instances:
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(instances)) as executor:
            future_to_source = {
                executor.submit(pipeline_task, source, ext): source 
                for source, ext in instances.items()
            }
            for future in concurrent.futures.as_completed(future_to_source):
                source = future_to_source[future]
                try:
                    dfs[source] = future.result()
                    log.info(f"Pipeline Módulo [{source}] finalizado com sucesso.")
                except Exception as exc:
                    log.error(f"Pipeline Módulo [{source}] gerou erro (I/O ou Limpeza): {exc}")
                    dfs[source] = pd.DataFrame()

    # DML: Carga de dimensões
    log.info("Carregando Dimensões...")
    map_cult = preencher_dimensao_cultura(db, culturas_alvo)
    map_mant = preencher_dimensao_mantenedor(db, dfs.get("cultivares", pd.DataFrame()))
    map_mun, map_mun_name = preencher_dimensao_municipio(db, dfs.get("sidra", pd.DataFrame()), dfs.get("zarc", pd.DataFrame()))
    
    # DML: Carga de fatos
    log.info("Carregando Tabelas Fato...")
    
    if "cultivares" in dfs: load_fact_cultivares(db, dfs["cultivares"], map_cult, map_mant)
    if "sidra" in dfs: load_fact_pam(db, dfs["sidra"], map_cult, map_mun)
    if "zarc" in dfs: load_fact_zarc(db, dfs["zarc"], map_cult, map_mun)
    if "conab" in dfs: load_fact_conab(db, dfs["conab"], map_cult, map_mun)
    if "agrofit" in dfs: load_fact_agrofit(db, dfs["agrofit"], map_cult)
    if "fertilizantes" in dfs: load_fact_fertilizantes(db, dfs["fertilizantes"], map_mun_name)
    if "sigef" in dfs: load_fact_sigef(db, dfs["sigef"], map_cult, map_mun_name)
    
    if "inmet" in instances and "inmet" in args.sources:
        all_muns = db.query(DimMunicipio).all()
        # Nota: load_fact_meteorologia faz o run interno se necessário
        load_fact_meteorologia(db, pd.DataFrame(), instances["inmet"], all_muns)

    log.info("--- Pipeline Concluído ---")

if __name__ == "__main__":
    main()
