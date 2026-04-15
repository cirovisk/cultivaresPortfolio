# 📋 Sugestões para o Portfólio — RNC Cultivares

## Contexto

Projeto atual: pipeline automatizado de dados + EDA estatística em 21 seções.  
Alvo: recrutadores de **DBA · Data Scientist · Data Engineer**.

---

## O que cada perfil quer VER

| Habilidade | DBA | Data Engineer | Data Scientist |
|---|:---:|:---:|:---:|
| SQL avançado (CTEs, window functions) | ⭐⭐⭐ | ⭐⭐ | ⭐ |
| Modelagem relacional / star schema | ⭐⭐⭐ | ⭐⭐⭐ | ⭐ |
| Pipeline ETL robusto | ⭐ | ⭐⭐⭐ | ⭐ |
| Qualidade de dados / testes | ⭐⭐ | ⭐⭐⭐ | ⭐ |
| Machine Learning | — | ⭐ | ⭐⭐⭐ |
| Análise estatística / hipóteses | ⭐ | ⭐ | ⭐⭐⭐ |
| Visualização / storytelling | ⭐ | ⭐ | ⭐⭐⭐ |
| Orquestração (Airflow/Prefect) | ⭐ | ⭐⭐⭐ | — |
| Testes automatizados (pytest) | ⭐⭐ | ⭐⭐⭐ | ⭐ |
| Containerização (Docker) | ⭐ | ⭐⭐⭐ | ⭐ |

---

## 🔴 Alta Prioridade — Mostre que você pensa como profissional

### 1. `schema.sql` — Modelagem Relacional + SQL Analítico

Modele o dataset como um **data warehouse** (star schema) e acompanhe com
queries avançadas demonstrando CTEs e window functions.

#### Esquema sugerido

```sql
-- Dimensão Espécie
CREATE TABLE dim_especie (
    id_especie      SERIAL PRIMARY KEY,
    nome_comum      TEXT NOT NULL,
    nome_cientifico TEXT NOT NULL,
    grupo           TEXT NOT NULL
);

-- Dimensão Mantenedor
CREATE TABLE dim_mantenedor (
    id_mantenedor SERIAL PRIMARY KEY,
    nome          TEXT,
    setor         TEXT,   -- Público / Privado / Misto / Nulo
    origem        TEXT    -- Nacional / Estrangeiro / Nulo
);

-- Dimensão Tempo
CREATE TABLE dim_tempo (
    id_tempo  INT PRIMARY KEY,  -- YYYYMMDD
    data      DATE,
    ano       INT,
    trimestre INT,
    decada    INT
);

-- Tabela Fato
CREATE TABLE fato_registro (
    id_registro   SERIAL PRIMARY KEY,
    nr_registro   INT NOT NULL,
    id_especie    INT REFERENCES dim_especie(id_especie),
    id_mantenedor INT REFERENCES dim_mantenedor(id_mantenedor),
    id_data_reg   INT REFERENCES dim_tempo(id_tempo),
    id_data_val   INT REFERENCES dim_tempo(id_tempo),
    nr_formulario BIGINT,
    situacao      TEXT,
    duracao_anos  NUMERIC(5,2),
    expirado      BOOLEAN
);
```

#### Queries analíticas a mostrar

```sql
-- Window function: ranking de mantenedores dentro de cada espécie
SELECT nome_comum, nome AS mantenedor, total,
       RANK() OVER (PARTITION BY nome_comum ORDER BY total DESC) AS ranking
FROM (
    SELECT e.nome_comum, m.nome, COUNT(*) AS total
    FROM fato_registro f
    JOIN dim_especie    e USING (id_especie)
    JOIN dim_mantenedor m USING (id_mantenedor)
    GROUP BY 1, 2
) sub;

-- CTE: Índice Herfindahl-Hirschman (concentração de mercado) por espécie
WITH shares AS (
    SELECT e.nome_comum,
           m.nome,
           COUNT(*)::FLOAT / SUM(COUNT(*)) OVER (PARTITION BY e.nome_comum) AS share
    FROM fato_registro f
    JOIN dim_especie    e USING (id_especie)
    JOIN dim_mantenedor m USING (id_mantenedor)
    WHERE m.nome IS NOT NULL
    GROUP BY 1, 2
)
SELECT nome_comum,
       ROUND(SUM(share * share)::NUMERIC, 4) AS hhi
       -- 0 = concorrência perfeita | 1 = monopólio
FROM shares
GROUP BY 1
ORDER BY hhi DESC;
```

> **Por que isso importa?** SQL com window functions e CTEs é o diferencial
> número 1 em entrevistas de DBA/DE. Comentar as escolhas de design (índices,
> chaves, normalização) dobra o impacto.

---

### 2. `tests/test_pipeline.py` — Qualidade de Dados com pytest

```python
import pytest
import pandas as pd
from data_pipeline import carregar_dados

@pytest.fixture(scope="module")
def df():
    return carregar_dados(imprimir_qualidade=False)

def test_sem_duplicatas(df):
    assert df["Nº REGISTRO"].duplicated().sum() == 0

def test_datas_coerentes(df):
    assert (df["DATA DO REGISTRO"] <= df["DATA DE VALIDADE DO REGISTRO"]).all()

def test_duracao_positiva(df):
    dur = (df["DATA DE VALIDADE DO REGISTRO"] - df["DATA DO REGISTRO"]).dt.days
    assert (dur > 0).all()

def test_grupos_conhecidos(df):
    grupos_validos = {"OLERÍCOLAS", "FRUTÍFERAS", "FLORESTAIS", "ORNAMENTAIS"}
    desconhecidos = set(df["GRUPO DA ESPÉCIE"].dropna().unique()) - grupos_validos
    assert not desconhecidos, f"Grupos inesperados: {desconhecidos}"

def test_sem_acentos_errados(df):
    formas_erradas = ["Orquidea", "Bromelia", "Alocasia"]
    for f in formas_erradas:
        assert (df["NOME COMUM"] == f).sum() == 0, f"'{f}' ainda presente"
```

dd portfolio improvement guide and 

---

### 3. `README.md` Profissional

Estrutura mínima recomendada:

```markdown
# 🌱 Registro Nacional de Cultivares — Análise Exploratória

## Sobre o Projeto
[Contexto agrícola e político dos dados]

## Arquitetura
SNPC/MAPA → data_pipeline.py → relatorio_cultivares.csv → Notebook (EDA)
                                         ↓
                                    schema.sql (DW)

## Principais Achados
- 80% do mercado de Milho concentrado em 3 empresas (HHI = X)
- Florestais: 95% dos registros sem cultivar nomeada
- Formulários digitais só se tornaram universais a partir de 2022

## Como Executar
pip install -r requirements.txt
python data_pipeline.py
jupyter notebook relatorio_cultivares.ipynb

## Stack
Python 3.12 · Pandas · Matplotlib · SQLite · pytest
```

> O recrutador técnico vê o README **antes** de qualquer linha de código.
> Se ele for bom, já tem interesse. Se for ruim, dificilmente abre o notebook.

---

## 🟡 Média Prioridade — Diferencia candidatos sênior

### 4. Machine Learning

**Opção A — Clustering de mantenedores** (perfil: especialista vs. generalista)

```python
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

features = df.groupby("MANTENEDOR").agg(
    volume=("NOME CIENTÍFICO", "count"),
    diversidade=("NOME CIENTÍFICO", "nunique"),
    anos_ativos=("ANO", lambda x: x.max() - x.min()),
)
X = StandardScaler().fit_transform(features)
kmeans = KMeans(n_clusters=4, random_state=42).fit(X)
```

**Opção B — Classificação de setor** (prevê Público/Privado a partir do nome)

Útil para os ~10% de mantenedores não classificados.

> Se adicionar ML, **explique a escolha do modelo e as métricas** em markdown.
> 98% de acurácia sem análise de classes desbalanceadas sinaliza ingenuidade.

---

### 5. Série Temporal com Projeção

```python
from prophet import Prophet

df_ts = df.groupby("ANO").size().reset_index()
df_ts.columns = ["ds", "y"]
df_ts["ds"] = pd.to_datetime(df_ts["ds"].astype(str) + "-01-01")

m = Prophet(yearly_seasonality=False)
m.fit(df_ts)
future = m.make_future_dataframe(periods=5, freq="YS")
forecast = m.predict(future)
```

Projete o crescimento de registros para 2025–2030 por grupo de espécie.

---

### 6. Testes de Hipótese Estatísticos

| Pergunta | Teste |
|---|---|
| Setor privado registra mais espécies por mantenedor? | Mann-Whitney U |
| Crescimento anual é estacionário? | ADF (statsmodels) |
| Duração de registro difere entre grupos? | Kruskal-Wallis |

```python
from scipy import stats

pub  = df[df["SETOR"] == "Público"]["DURACAO_ANOS"].dropna()
priv = df[df["SETOR"] == "Privado"]["DURACAO_ANOS"].dropna()

stat, p = stats.mannwhitneyu(pub, priv, alternative="two-sided")
print(f"p = {p:.4f} → {'diferença significativa' if p < 0.05 else 'sem evidência'}")
```

---

## 🟢 Baixa Prioridade — Nice to have

### 7. Docker

```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["jupyter", "nbconvert", "--to", "html", "--execute",
     "relatorio_cultivares.ipynb", "--output", "report.html"]
```

Executa o notebook headless e gera HTML exportado — prova reprodutibilidade
em qualquer ambiente.

---

### 8. Dashboard Streamlit

Um app de uma página com filtros interativos (grupo, setor, ano) e gráfico
de concentração de mercado dinâmico permite que o recrutador explore os dados
sozinho — altamente memorável.

---

## Roadmap Sugerido

| Semana | Entrega |
|---|---|
| 1 | README profissional + `schema.sql` + 3 queries com window functions |
| 2 | `tests/test_pipeline.py` (5 testes mínimos) + CI no GitHub Actions |
| 3 | Teste de hipótese + projeção temporal simples |
| 4 | Clustering de mantenedores (ML) |
| 5 | Streamlit ou Docker |

---

## ⚠️ O que NÃO fazer

| Armadilha | Por que evitar |
|---|---|
| Modelo com 99% de acurácia sem análise de erros | Parece ingênuo |
| Gráficos sem título / legenda / unidade | Atenção ao detalhe fraca |
| README genérico | Não conta história |
| Commits todos no mesmo dia | Repositório fake não convence |
| Código sem comentários | Dificulta leitura em equipe |
| Células soltas sem funções reutilizáveis | Sinaliza falta de experiência |
