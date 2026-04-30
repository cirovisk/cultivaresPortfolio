"""
Padrão Registry. 
Desacopla main das fontes. Fonte se auto-registra com @register.
Nova fonte? Só adicionar arquivo. Main não muda. 
Zero hardcode. Princípio OCP (Aberto/Fechado) aplicado.
Fácil de escalar.
"""
import logging

log = logging.getLogger(__name__)

_SOURCES: dict[str, type] = {}


def register(name: str):
    """Decorator que registra uma classe de pipeline no registry."""
    def wrapper(cls):
        if name in _SOURCES:
            log.warning(f"Source '{name}' já registrada — sobrescrevendo.")
        _SOURCES[name] = cls
        return cls
    return wrapper


def get_sources() -> dict[str, type]:
    """Retorna todas as fontes registradas."""
    return _SOURCES.copy()


def get_source(name: str):
    """Retorna uma fonte específica pelo nome."""
    return _SOURCES.get(name)
