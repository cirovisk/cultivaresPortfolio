# Pipeline: Zoneamento Agrícola de Risco Climático (ZARC)

Extração e processamento massivo de cenários de risco climático e recomendações de cultivares.

## 📌 Fonte de Dados
- **Agência:** MAPA (Ministério da Agricultura e Pecuária)
- **Origem:** Portal de Dados Abertos / SISZARC.
- **Diferenciação:** Os arquivos de Risco Climático de cada cultura (ex: zarc_soja.csv, zarc_milho.csv) contêm as probabilidades de perda (dec1 a dec36) por Município/Solo e a lista de recomendações. O ETL consolida tudo.

## 🛠️ Processo de Extração (Otimizado)
Mesmo com o volume massivo, o projeto mantém a eficiência através de:
1. **Streaming ETL:** O script `src/pipeline/zarc.py` utiliza geradores (yield) do Python e o parâmetro `chunksize` do Pandas para ler CSVs gigantes sem estourar a memória RAM.
2. **Carga em Lotes (Bulk Upsert):** Os dados são inseridos no PostgreSQL em blocos de 1.000 a 5.000 linhas, utilizando a cláusula `ON CONFLICT DO UPDATE` para garantir a idempotência.

## 🔄 Alta Performance no PostgreSQL
Diferente de abordagens que exigem bancos OLAP separados, o ZARC aqui reside 100% no PostgreSQL. A performance é garantida por:
- **Índice B-Tree Composto:** Criado em `(id_municipio, id_cultura)` na tabela `fato_risco_zarc`.
- **Filtros de Partição:** Consultas no dashboard são otimizadas para filtrar primeiro pelo município, reduzindo o scan de milhões para poucas centenas de linhas.

## 💾 Armazenamento (Star Schema)
- **Fato:** `fato_risco_zarc`.
- **Relacionamentos:** Chaves estrangeiras para `dim_municipio` e `dim_cultura`.

## 📥 Guia de Expansão: Como baixar outras culturas
Atualmente, o projeto processa nativamente **Soja, Milho, Trigo, Algodão e Cana-de-Açúcar**. Para adicionar o ZARC de outras culturas adicionais (Café, Feijão, Arroz, etc.), siga estes passos:

### 1. Acessar o Portal de Dados Abertos
Vá para o dataset oficial do MAPA:
- **[Portal de Dados Abertos - SISZARC](https://dados.agricultura.gov.br/dataset/siszarc-sistemas-de-zoneamento-agricola-e-risco-climatico)**

### 2. Escolher o Recurso
Baixe o arquivo CSV da cultura/safra desejada. Os arquivos geralmente têm nomes como `indicacao_cultivar_soja_2324.csv`.

### 3. Integração no Pipeline
1.  Mova o arquivo baixado para a pasta `/data/zarc/` no projeto.
2.  No arquivo `src/pipeline/zarc.py`, adicione o nome da nova cultura à lista `TARGET_CROPS` se ela for diferente das padrões.
3.  Execute a carga: `docker-compose run --rm app python src/main.py --source zarc`.

O sistema irá processar o novo CSV e inserir os dados no Postgres automaticamente.
