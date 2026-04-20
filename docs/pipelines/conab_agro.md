# Pipeline: CONAB (Séries de Preços e Produção)

Extração de indicadores de mercado e estimativas de safra da Companhia Nacional de Abastecimento.

## 📌 Fonte de Dados
- **Portal de Dados:** [CONAB - Dados Abertos](https://www.conab.gov.br/)
- **Datasets:**
    - Séries Históricas de Preços (Mensais e Semanais).
    - Levantamentos de Safra (Produção e Produtividade).

## 🛠️ Processo de Extração
1.  **Download de Planilhas/ZIPs:** A CONAB disponibiliza muitos dados em arquivos `.xlsx` ou compactados. O pipeline faz o download direto dessas fontes.
2.  **Séries de Preços:** Captura de preços médios ao nível de UF ou praça de comercialização.
3.  **Levantamentos de Safra:** Extração de área plantada (mil ha), produção (mil t) e produtividade (t/ha).

## 🔄 Transformações (Cleaners)
Lógica centralizada em `src/pipeline/cleaners/conab.py`:
- **Normalização de Unidades:** Conversão de mil hectares/toneladas para unidades base (ha/t).
- **Tratamento de Strings:** Remoção de espaços e normalização de UFs.
- **Hierarquia Temporal:** Mapeamento de "Ano Agrícola" (ex: 2023/24) para o ano de início da safra.
- **Cultura Match:** Vínculo via `normalize_culture_name`.

## 💾 Armazenamento
Os dados são carrgados em três tabelas factuais:
- `fato_producao_conab`: Dados macro de safra e produtividade.
- `fato_precos_conab_mensal`: Séries históricas mensais de preços por KG.
- `fato_precos_conab_semanal`: Monitoramento de curto prazo de preços.
