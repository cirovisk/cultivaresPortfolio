# Pipeline: Zoneamento Agrícola de Risco Climático (ZARC)

Extração e processamento massivo de cenários de risco climático e recomendações de cultivares.

## 📌 Fonte de Dados
- **Agência:** MAPA
- **Origem:** [Portal Zarc (MAPA)](https://www.gov.br/agricultura/pt-br/assuntos/riscos-seguro/programa-nacional-de-zoneamento-agricola-de-risco-climatico)
- **Método de Acesso:** Arquivos CSV consolidados por cultura (Soja, Milho, Trigo, Algodão e Cana-de-Açúcar).

## 🛠️ Processo de Extração (Otimizado)
Devido ao tamanho dos arquivos (alguns CSVs superam 1.3GB), o pipeline utiliza uma estratégia de **Streaming e Chunks**:
1. **Magic Bytes:** O código identifica automaticamente se o arquivo está zipado (Gzip) lendo os primeiros bytes, permitindo ler `.csv.gz` ou `.csv` brutos sem alteração de código.
2. **Chunking:** O Pandas lê o arquivo em blocos de 50.000 linhas, processando cada bloco e liberando a memória antes de ler o próximo.
3. **Carga Seletiva:** Para popular a `dim_municipio`, o pipeline realiza uma leitura parcial (apenas colunas de ID e Nome), evitando carregar as 36 colunas de decêndios sem necessidade.

## 🔄 Alta Performance no PostgreSQL
Diferente de abordagens que exigem bancos OLAP separados, o ZARC aqui reside 100% no PostgreSQL. A performance é garantida por:
- **Índice B-Tree Composto:** Criado em `(id_municipio, id_cultura)` na tabela `fato_risco_zarc`.
- **Filtros de Partição:** Consultas no dashboard são otimizadas para filtrar primeiro pelo município, reduzindo o scan de milhões para poucas centenas de linhas.

## 💾 Armazenamento (Star Schema)
- **Fato:** `fato_risco_zarc`.
- **Relacionamentos:** Chaves estrangeiras para `dim_municipio` e `dim_cultura`.

## 📥 Guia de Expansão
Atualmente, o projeto processa nativamente **Soja, Milho, Trigo, Algodão e Cana-de-Açúcar**. Para adicionar uma nova cultura:
1. Baixe o CSV no portal do MAPA.
2. Salve na pasta `data/zarc/` com o nome `zarc_{cultura}.csv`.
3. Adicione o nome da cultura na lista `TARGET_CROPS` em `src/pipeline/sources/zarc.py`.
