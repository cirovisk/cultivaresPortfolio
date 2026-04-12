import sys
import os
from pathlib import Path
import pytest
import pandas as pd

# Add the parent directory to sys.path so we can import data_pipeline
sys.path.insert(0, str(Path(__file__).parent.parent))

from data_pipeline import carregar_dados

@pytest.fixture(scope="module")
def df():
    # Pass path directly to force the tests to be deterministic if needed, 
    # but here we can just use the natural local data load.
    return carregar_dados(imprimir_qualidade=False)

def test_sem_duplicatas(df):
    assert df["Nº REGISTRO"].duplicated().sum() == 0

def test_datas_coerentes(df):
    # dropna is important to test only existing bounds.
    mask = df["DATA DO REGISTRO"].notna() & df["DATA DE VALIDADE DO REGISTRO"].notna()
    assert (df.loc[mask, "DATA DO REGISTRO"] <= df.loc[mask, "DATA DE VALIDADE DO REGISTRO"]).all()

def test_duracao_positiva(df):
    mask = df["DATA DO REGISTRO"].notna() & df["DATA DE VALIDADE DO REGISTRO"].notna()
    dur = (df.loc[mask, "DATA DE VALIDADE DO REGISTRO"] - df.loc[mask, "DATA DO REGISTRO"]).dt.days
    assert (dur > 0).all()

def test_grupos_conhecidos(df):
    grupos_validos = {
        "OLERÍCOLAS", "FRUTÍFERAS", "FLORESTAIS", "ORNAMENTAIS", 
        "OUTROS", "FORRAGEIRAS", "GRANDES CULTURAS", "MEDICINAIS E AROMATICAS"
    }
    desconhecidos = set(df["GRUPO DA ESPÉCIE"].dropna().unique()) - grupos_validos
    assert not desconhecidos, f"Grupos inesperados: {desconhecidos}"

def test_sem_acentos_errados(df):
    formas_erradas = ["Orquidea", "Bromelia", "Alocasia", "Crotón"]
    for f in formas_erradas:
        assert (df["NOME COMUM"] == f).sum() == 0, f"'{f}' ainda presente, faltou limpeza no pipeline"

def test_classificacao_setores_validos(df):
    setores_esperados = {"Público", "Privado", "Misto", "Nulo"}
    if "SETOR" in df.columns:
        setores = set(df["SETOR"].dropna().unique())
        fora_do_padrao = setores - setores_esperados
        assert not fora_do_padrao, f"Setores fora do padrão: {fora_do_padrao}"
