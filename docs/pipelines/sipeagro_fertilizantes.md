# Pipeline: SIPEAGRO (Estabelecimentos de Fertilizantes)

Extração do cadastro de estabelecimentos que comercializam ou produzem fertilizantes no Brasil.

## 📌 Fonte de Dados
- **Agência:** MAPA (Ministério da Agricultura e Pecuária)
- **Dataset:** [SIPEAGRO - Estabelecimentos](https://dados.agricultura.gov.br/dataset/52a01565-72d6-410e-b21b-64035831a7be/resource/e0bbc9d5-f161-448b-a6d4-c7beb312ec33)

## 🛠️ Processo de Extração
1.  **Download CSV:** Download direto via HTTP.
2.  **Versioning:** O script mantém uma pasta `archive` para guardar versões anteriores do dataset antes de atualizar, garantindo a idempotência e histórico de mudanças nos estabelecimentos.

## 🔄 Transformações (Silver Layer)
- **Limpeza de CNPJ:** Padronização numérica para joins futuros.
- **Tratamento de Encoding:** O arquivo original usa `latin1`, o pipeline converte para `utf-8` durante o processamento.
- **Normalização Geográfica:** Vinculação dos nomes de municípios com a `dim_municipio`.

## 💾 Armazenamento
Os dados alimentam a tabela `fato_fertilizantes_estabelecimentos`, fornecendo uma visão clara da infraestrutura de insumos disponível em cada microrregião produtiva.
