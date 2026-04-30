"""
Orquestrador genérico.
Registry + BaseSource = sem imports hardcoded aqui.
Itera o registry. Chama `.run()`. Polimorfismo na prática.
Limpo. Fácil manutenção.
"""

import argparse
import logging
import gc

from db.manager import init_db, get_db
from pipeline.registry import get_sources
from pipeline.dimensions import (
    preencher_dimensao_cultura,
    carregar_municipios_completo_ibge,
)

# IMPORTANTE: importar o pacote sources para acionar os @register
import pipeline.sources  # noqa: F401

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

CULTURAS_ALVO = ["soja", "milho", "trigo", "algodão", "cana-de-açúcar"]


def main():
    sources = get_sources()

    parser = argparse.ArgumentParser(description="Pipeline AgroHarvest BR")
    parser.add_argument(
        "--sources", nargs="+", choices=sources.keys(),
        default=list(sources.keys()), help="Fontes de dados a processar"
    )
    parser.add_argument("--refresh", action="store_true", help="Força refresh de caches")
    args = parser.parse_args()

    log.info("--- Iniciando Pipeline AgroHarvest BR (Registry) ---")
    init_db()
    db = next(get_db())

    # Lookups compartilhados (construídos uma vez, usados por todos)
    lookups = {
        "db": db,
        "culturas": preencher_dimensao_cultura(db, CULTURAS_ALVO),
        "municipios_ibge": {},
        "municipios_nome": {},
    }
    map_ibge, map_nome = carregar_municipios_completo_ibge(db)
    lookups["municipios_ibge"] = map_ibge
    lookups["municipios_nome"] = map_nome

    success, failed = [], []

    for name in args.sources:
        source_cls = sources.get(name)
        if not source_cls:
            log.warning(f"Fonte '{name}' não registrada — pulando.")
            continue
        pipeline = source_cls()
        try:
            result = pipeline.run(lookups)
            success.append(name)
            log.info(f"✓ {name}: {result}")
        except Exception as e:
            failed.append(name)
            log.error(f"✗ {name}: {e}")
        finally:
            gc.collect()

    log.info("--- Pipeline Concluído ---")
    log.info(f"Sucesso: {success}")
    if failed:
        log.warning(f"Falhas: {failed}")


if __name__ == "__main__":
    main()
