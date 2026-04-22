# Pipeline: Zoneamento Agrícola de Risco Climático (ZARC)

Extração e processamento massivo de cenários de risco climático e recomendações de cultivares.

## 📌 Fonte de Dados
- **Agência:** MAPA (Ministério da Agricultura e Pecuária)
- **Origem:** Portal de Dados Abertos / SISZARC.
- **Datasets integrados:**
    1. **Tábua de Risco:** Probabilidades de perda por Município/Solo/Decêndio.
    2. **Indicações de Cultivares:** Relação de variedades genéticas certificadas para cada zona de risco.

## 🛠️ Processo de Extração (Big Data)
Devido ao volume massivo (>200 milhões de registros), o projeto utiliza uma estratégia de **Data Lakehouse**:
1. **Streaming ETL:** O script `src/scripts/process_zarc_full.py` utiliza o **DuckDB** para ler CSVs gigantes sem estourar a memória RAM.
2. **Armazenamento Colunar:** Os dados são convertidos para **Apache Parquet**.
3. **Particionamento:** Dados organizados em pastas por **UF** (Hive Partitioning) para acelerar consultas regionais.

## 🔄 Motor de Consulta (DuckDB Bridge)
Diferente dos outros pipelines, o ZARC não reside no PostgreSQL. O acesso é feito via `src/api/duck_bridge.py`:
- Permite JOINs entre os arquivos Parquet e as dimensões no Postgres (Municípios/Culturas).
- Performance analítica superior para grandes volumes.

## 💾 Armazenamento (Camada Gold)
- **Parquet:** `data/parquet/zarc_indicacoes` e `data/parquet/zarc_risco`.
- **Híbrido:** Metadados e tabelas auxiliares permanecem no **PostgreSQL** para integridade referencial.

## 📥 Guia de Expansão: Como baixar outras culturas
Atualmente, o projeto contém dados de **Soja** (Indicações) e **Trigo/Algodão** (Risco 25/26). Para adicionar o ZARC de outras culturas (Milho, Feijão, Arroz, etc.), siga estes passos:

### 1. Acessar o Portal de Dados Abertos
Vá para o dataset oficial do MAPA:
- **[Portal de Dados Abertos - SISZARC](https://dados.agricultura.gov.br/dataset/siszarc-sistemas-de-zoneamento-agricola-e-risco-climatico)**

### 2. Escolher o Tipo de Dados
- **Indicação:** Escolha o recurso "Dados Abertos - Indicação de Cultivares por Safra" (Geralmente arquivos CSV.GZ de ~1.3GB).
- **Risco:** Escolha o recurso "Dados Abertos - Tábua de Risco" (Arquivos CSV por Safra).

### 3. Integração no Pipeline
1.  Baixe o arquivo CSV da cultura/safra desejada.
2.  Mova-o para a pasta `/data/zarc/` no projeto.
3.  No arquivo `src/scripts/process_zarc_full.py`, adicione o nome do novo arquivo no dicionário `EXTRA_FILES`.
4.  Execute a conversão: `docker-compose run --rm app python src/scripts/process_zarc_full.py`.

O sistema irá gerar as novas partições Parquet automaticamente e o endpoint `/analytics/raio-x-municipal` passará a enxergar esses dados instantaneamente.
