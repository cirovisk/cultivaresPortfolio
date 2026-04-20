# Pipeline: Produção Agrícola Municipal (PAM/SIDRA)

Extração de dados de produção agrícola das lavouras temporárias através da API v3 do IBGE SIDRA.

## 📌 Fonte de Dados
- **Agregado:** Tabela 1612 (Produção Agrícola Municipal - Lavouras temporárias)
- **API:** [SIDRA API](https://apisidra.ibge.gov.br/)
- **Granularidade:** Municipal (Nível 6) e Cultura (C81).

## 🛠️ Processo de Extração
1.  **Metadados:** O pipeline consulta primeiramente os metadados da Tabela 1612 para buscar os IDs dinâmicos de cada cultura na classificação 81.
    - URL: `https://servicodados.ibge.gov.br/api/v3/agregados/1612/metadados`
2.  **Consulta:** Para cada `crop_id` identificado (ex: Soja = 40280), é feita uma chamada REST solicitando:
    - **Variáveis (`v`):** 109 (Área plantada), 216 (Área colhida), 214 (Quantidade produzida).
    - **Território (`n6`):** Todos os municípios do Brasil.
    - **Período (`p`):** Ano específico (configurado no extrator).
3.  **URL Exemplo:** `https://apisidra.ibge.gov.br/values/t/1612/n6/all/v/109,216,214/p/2022/c81/40280`

## 🔄 Transformações (Cleaners)
Lógica implementada em `src/pipeline/cleaners/sidra.py`:
- **Normalização de Colunas:** Mapeamento de códigos SIDRA (D2N, V, D1C) para nomes amigáveis.
- **Pivoteamento:** Transformação de variáveis (Linhas API) em colunas fato.
- **Tratamento de Nulos:** Conversão de símbolos do IBGE (`...`, `-`) para `NaN`.
- **Cultura Match:** Aplicação de `normalize_culture_name`.

## 💾 Armazenamento
Os dados são carregados na tabela `fato_producao_pam` no PostgreSQL, mantendo o histórico por ano e município.
