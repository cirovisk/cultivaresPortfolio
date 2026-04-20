# Pipeline: Agrofit (Defensivos Agrícolas)

Extração de dados dos produtos formulados (agrotóxicos e afins) registrados no MAPA.

## 📌 Fonte de Dados
- **Agência:** MAPA (Ministério da Agricultura e Pecuária)
- **Dataset:** [Agrofit - Dados Abertos](https://dados.agricultura.gov.br/dataset/agrofitprodutosformulados)

## 🛠️ Processo de Extração
1.  **Download CSV:** O pipeline faz o download do arquivo CSV consolidado de produtos formulados. Dada a alta volumetria, o extrator implementa uma lógica de **Incremental Cache** (checa se o arquivo local tem mais de 30 dias).
2.  **Granularidade:** Produto, Ingrediente Ativo e Praga-Alvo.

## 🔄 Transformações (Cleaners)
A lógica de limpeza reside em `src/pipeline/cleaners/agrofit.py`:
- **Mapeamento Praga-Cultura:** Conecta o produto à cultura recomendada e à praga comum.
- **Normalização de Strings:** Limpeza de registros de ingredientes ativos e classificação de toxicidade.
- **Cultura Match:** Uso de `normalize_culture_name` para vínculo com `dim_cultura`.

## 💾 Armazenamento
Os dados são persistidos na tabela `fato_agrofit`. É extensível para permitir análises de quais cultivares possuem biotecnologias de resistência a determinados ingredientes ativos listados aqui.
