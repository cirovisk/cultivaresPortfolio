"""
Caixa de Ferramentas (Toolbox) de Análise Estatística
=============================================================
Funções auxiliares para sumarizar e explorar os dados do Registro
Nacional de Cultivares (RNC) de forma concisa em Notebooks.
"""

import pandas as pd
from data_pipeline import (
    COL_MANTENEDOR,
    COL_CULTIVAR,
    COL_FORMULARIO,
)

def analise_nulos(df: pd.DataFrame) -> pd.DataFrame:
    """Retorna um DataFrame resumindo colunas com valores nulos."""
    nulos = df.isnull().sum()
    nulos = nulos[nulos > 0].sort_values(ascending=False).to_frame(name="Total Nulos")
    nulos["% Nulos"] = (nulos["Total Nulos"] / len(df) * 100).round(2)
    return nulos

def visao_geral(df: pd.DataFrame) -> pd.DataFrame:
    """Retorna um DataFrame de visão geral contendo tipos, nulos e valores únicos por coluna."""
    return pd.DataFrame({
        "Tipo"          : df.dtypes.astype(str),
        "Não-Nulos"     : df.notnull().sum(),
        "Nulos"         : df.isnull().sum(),
        "% Nulos"       : (df.isnull().sum() / len(df) * 100).round(2),
        "Únicos"        : df.nunique(),
    })

def crescimento_anual(df: pd.DataFrame) -> pd.DataFrame:
    """Agrupa os registros por ano e calcula crescimento percentual e acumulado."""
    if "ANO" not in df.columns:
        return pd.DataFrame()
        
    registros_ano = (
        df.groupby("ANO")
        .size()
        .reset_index(name="Registros")
        .sort_values("ANO")
    )
    registros_ano["Acumulado"]      = registros_ano["Registros"].cumsum()
    registros_ano["Var. Absoluta"]  = registros_ano["Registros"].diff().fillna(0).astype(int)
    registros_ano["Var. %"]         = registros_ano["Registros"].pct_change().mul(100).round(2)
    
    return registros_ano

def mantenedores_recorrentes(df: pd.DataFrame, top: int = 20) -> pd.DataFrame:
    """Retorna os principais mantenedores ordenados por volume de registros."""
    top_mantenedores = (
        df[COL_MANTENEDOR]
        .value_counts()
        .head(top)
        .reset_index()
    )
    top_mantenedores.columns = ["Mantenedor", "Registros"]
    top_mantenedores["% do Total"] = (top_mantenedores["Registros"] / len(df) * 100).round(2)
    return top_mantenedores

def analise_categoria(df: pd.DataFrame, keyword: str, col: str = COL_MANTENEDOR) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Filtra registros que contêm 'keyword' na coluna especificada.
    Retorna a tupla (DataFrame_Filtrado, Top_Agrupamentos).
    """
    mask  = df[col].str.contains(keyword, case=False, na=False)
    grupo = df[mask]
    contagem = grupo[col].value_counts().reset_index()
    contagem.columns = [col, "Registros"]
    return grupo, contagem

def nulos_por_ano(df: pd.DataFrame) -> pd.DataFrame:
    """Distribuição de valores nulos no MANTENEDOR por ano de registro."""
    if "ANO" not in df.columns:
        return pd.DataFrame()
        
    nulos = (
        df[df[COL_MANTENEDOR].isnull()]
        .groupby("ANO")
        .size()
        .reset_index(name="Nulos")
        .sort_values("ANO")
    )
    return nulos

def resumo_executivo(df: pd.DataFrame) -> pd.DataFrame:
    """
    Classifica de forma macro os mantenedores através de keywords e null-checks,
    gerando um pequeno dataframe executivo para fácil leitura.
    """
    grp_univ, _ = analise_categoria(df, "universidade")
    grp_emp, _  = analise_categoria(df, "empresa brasileira")
    grp_inst, _ = analise_categoria(df, "instituto")
    
    total = len(df)
    nulos = df[COL_MANTENEDOR].isnull().sum()
    
    qtd_univ = len(grp_univ)
    qtd_emp  = len(grp_emp)
    qtd_inst = len(grp_inst)
    
    outros = total - qtd_univ - qtd_emp - qtd_inst - nulos
    
    resumo_exec = pd.DataFrame({
        "Categoria" : ["Universidade", "Empresa Brasileira", "Instituto", "Sem Mantenedor", "Outros"],
        "Registros" : [qtd_univ, qtd_emp, qtd_inst, nulos, outros],
    })
    resumo_exec["% do Total"] = (resumo_exec["Registros"] / total * 100).round(2)
    return resumo_exec
