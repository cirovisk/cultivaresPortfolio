"""
Análise Estatística do Registro Nacional de Cultivares (RNC)
=============================================================
Fonte: relatorio_cultivares.csv
Autor: Análise automatizada com Pandas
"""

import pandas as pd
import warnings
warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────
# 1. CARREGAMENTO E PRÉ-PROCESSAMENTO
# ─────────────────────────────────────────────

df = pd.read_csv("relatorio_cultivares.csv")
print(f"✅ Dataset carregado: {df.shape[0]:,} registros | {df.shape[1]} colunas\n")

COL_DATA       = "DATA DO REGISTRO"
COL_MANTENEDOR = "MANTENEDOR (REQUERENTE) (NOME)"
COL_CULTIVAR   = "CULTIVAR"
COL_FORMULARIO = "Nº FORMULÁRIO"

# Converter datas
df[COL_DATA] = pd.to_datetime(df[COL_DATA], dayfirst=True, errors="coerce")
df["ANO"]    = df[COL_DATA].dt.year


# ─────────────────────────────────────────────
# 2. ANÁLISE DE VALORES NULOS
# ─────────────────────────────────────────────

print("=" * 60)
print("SEÇÃO 2 — ANÁLISE DE VALORES NULOS")
print("=" * 60)

nulos_colunas = df.isnull().sum()
nulos_colunas = nulos_colunas[nulos_colunas > 0].sort_values(ascending=False).to_frame(name="Total Nulos")
nulos_colunas["% Nulos"] = (nulos_colunas["Total Nulos"] / len(df) * 100).round(2)

print(f"📊 Total de registros: {len(df):,}")
print(f"❌ Nulos em Nº FORMULÁRIO: {df[COL_FORMULARIO].isnull().sum():,}")
print(f"❌ Nulos em MANTENEDOR:    {df[COL_MANTENEDOR].isnull().sum():,}")
print("\nColunas com valores ausentes:")
print(nulos_colunas.to_string())


# ─────────────────────────────────────────────
# 3. VISÃO GERAL DAS COLUNAS
# ─────────────────────────────────────────────

print("\n" + "=" * 60)
print("SEÇÃO 3 — VISÃO GERAL DO DATASET")
print("=" * 60)

resumo = pd.DataFrame({
    "Tipo"          : df.dtypes.astype(str),
    "Não-Nulos"     : df.notnull().sum(),
    "Nulos"         : df.isnull().sum(),
    "% Nulos"       : (df.isnull().sum() / len(df) * 100).round(2),
    "Únicos"        : df.nunique(),
})
print(resumo.to_string())


# ─────────────────────────────────────────────
# 4. CRESCIMENTO ANO A ANO
# ─────────────────────────────────────────────

print("\n" + "=" * 60)
print("SEÇÃO 4 — CRESCIMENTO ANUAL DE REGISTROS")
print("=" * 60)

registros_ano = (
    df.groupby("ANO")
    .size()
    .reset_index(name="Registros")
    .sort_values("ANO")
)
registros_ano["Acumulado"]      = registros_ano["Registros"].cumsum()
registros_ano["Var. Absoluta"]  = registros_ano["Registros"].diff().fillna(0).astype(int)
registros_ano["Var. %"]         = registros_ano["Registros"].pct_change().mul(100).round(2)

print(registros_ano.to_string(index=False))
print(f"\n📅 Período: {int(registros_ano['ANO'].min())} → {int(registros_ano['ANO'].max())}")
print(f"📈 Total de registros: {registros_ano['Registros'].sum():,}")
print(f"🏆 Ano com mais registros: {registros_ano.loc[registros_ano['Registros'].idxmax(), 'ANO']:.0f}"
      f" ({registros_ano['Registros'].max():,} registros)")


# ─────────────────────────────────────────────
# 5. MANTENEDORES MAIS RECORRENTES
# ─────────────────────────────────────────────

print("\n" + "=" * 60)
print("SEÇÃO 5 — MANTENEDORES MAIS RECORRENTES (Top 20)")
print("=" * 60)

top_mantenedores = (
    df[COL_MANTENEDOR]
    .value_counts()
    .head(20)
    .reset_index()
)
top_mantenedores.columns = ["Mantenedor", "Registros"]
top_mantenedores["% do Total"] = (top_mantenedores["Registros"] / len(df) * 100).round(2)
print(top_mantenedores.to_string(index=False))


# ─────────────────────────────────────────────
# 6. MANTENEDORES POR CATEGORIA (KEYWORD)
# ─────────────────────────────────────────────

def analise_categoria(df, col, keyword, label):
    mask  = df[col].str.contains(keyword, case=False, na=False)
    grupo = df[mask]
    contagem = grupo[col].value_counts().reset_index()
    contagem.columns = [col, "Registros"]
    return grupo, contagem

print("\n" + "=" * 60)
print("SEÇÃO 6 — MANTENEDORES COM 'UNIVERSIDADE'")
print("=" * 60)

grp_univ, top_univ = analise_categoria(df, COL_MANTENEDOR, "universidade", "Universidade")
print(f"🎓 Total de registros com 'universidade': {len(grp_univ):,}")
print(f"🎓 Mantenedores únicos com 'universidade': {grp_univ[COL_MANTENEDOR].nunique()}")
print("\nTop 15 universidades por nº de registros:")
print(top_univ.head(15).to_string(index=False))
print("\nLista completa de universidades (nomes únicos):")
for i, nome in enumerate(sorted(grp_univ[COL_MANTENEDOR].dropna().unique()), 1):
    print(f"  {i:3}. {nome}")

# ─── EMPRESA BRASILEIRA ───
print("\n" + "=" * 60)
print("SEÇÃO 7 — MANTENEDORES COM 'EMPRESA BRASILEIRA'")
print("=" * 60)

grp_emp, top_emp = analise_categoria(df, COL_MANTENEDOR, "empresa brasileira", "Empresa Brasileira")
print(f"🏢 Total de registros com 'empresa brasileira': {len(grp_emp):,}")
print(f"🏢 Mantenedores únicos com 'empresa brasileira': {grp_emp[COL_MANTENEDOR].nunique()}")
print("\nTop 15 por nº de registros:")
print(top_emp.head(15).to_string(index=False))
print("\nLista completa (nomes únicos):")
for i, nome in enumerate(sorted(grp_emp[COL_MANTENEDOR].dropna().unique()), 1):
    print(f"  {i:3}. {nome}")

# ─── INSTITUTO ───
print("\n" + "=" * 60)
print("SEÇÃO 8 — MANTENEDORES COM 'INSTITUTO'")
print("=" * 60)

grp_inst, top_inst = analise_categoria(df, COL_MANTENEDOR, "instituto", "Instituto")
print(f"🔬 Total de registros com 'instituto': {len(grp_inst):,}")
print(f"🔬 Mantenedores únicos com 'instituto': {grp_inst[COL_MANTENEDOR].nunique()}")
print("\nTop 15 institutos por nº de registros:")
print(top_inst.head(15).to_string(index=False))
print("\nLista completa (nomes únicos):")
for i, nome in enumerate(sorted(grp_inst[COL_MANTENEDOR].dropna().unique()), 1):
    print(f"  {i:3}. {nome}")

# ─── DETALHAMENTO DE NULOS ───
print("\n" + "=" * 60)
print("SEÇÃO 9 — DETALHAMENTO DE NULOS NA COLUNA MANTENEDOR")
print("=" * 60)

nulos_mantenedor = df[COL_MANTENEDOR].isnull().sum()
pct_nulos        = nulos_mantenedor / len(df) * 100
print(f"❌ Registros sem mantenedor informado: {nulos_mantenedor:,} ({pct_nulos:.2f}%)")
print(f"✅ Registros com mantenedor informado:  {len(df) - nulos_mantenedor:,} ({100 - pct_nulos:.2f}%)")

# Distribuição de nulos por ano
nulos_por_ano = (
    df[df[COL_MANTENEDOR].isnull()]
    .groupby("ANO")
    .size()
    .reset_index(name="Nulos")
    .sort_values("ANO")
)
print("\nNulos por ano:")
print(nulos_por_ano.to_string(index=False))


# ─────────────────────────────────────────────
# 6. RESUMO EXECUTIVO
# ─────────────────────────────────────────────

print("\n" + "=" * 60)
print("RESUMO EXECUTIVO")
print("=" * 60)

total = len(df)
categorias = {
    "Universidade"    : len(grp_univ),
    "Empresa Brasileira": len(grp_emp),
    "Instituto"       : len(grp_inst),
    "Sem Mantenedor"  : df[COL_MANTENEDOR].isnull().sum(),
    "Outros"          : total - len(grp_univ) - len(grp_emp) - len(grp_inst) - df[COL_MANTENEDOR].isnull().sum(),
}

resumo_exec = pd.DataFrame({
    "Categoria" : list(categorias.keys()),
    "Registros" : list(categorias.values()),
})
resumo_exec["% do Total"] = (resumo_exec["Registros"] / total * 100).round(2)
print(resumo_exec.to_string(index=False))
print(f"\n📦 Total geral: {total:,} registros")
print("\n✅ Análise concluída.")
