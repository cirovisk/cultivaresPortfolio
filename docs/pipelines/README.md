# Documentação de Pipelines (ETL)

Esta pasta contém o detalhamento técnico de cada pipeline de extração e transformação (ETL) implementado no projeto **AgroHarvest BR**.

## 📑 Índice de Fontes

| Fonte | Entidade Responsável | Arquivo de Documentação |
| :--- | :--- | :--- |
| **SIDRA/PAM** | IBGE | [pam_sidra.md](./pam_sidra.md) |
| **ZARC** | MAPA | [zarc_mapa.md](./zarc_mapa.md) |
| **CONAB** | CONAB | [conab_agro.md](./conab_agro.md) |
| **Cultivares (RNC)** | MAPA/SNPC | [rnc_cultivares.md](./rnc_cultivares.md) |
| **Agrofit** | MAPA | [agrofit.md](./agrofit.md) |
| **Fertilizantes** | MAPA/SIPEAGRO | [sipeagro_fertilizantes.md](./sipeagro_fertilizantes.md) |

---

## 🏗️ Padrão de Engenharia
 
 O projeto utiliza uma arquitetura de pipeline desacoplada:

1.  **Extractors (`src/pipeline/`):** Classes que gerenciam a conexão com a fonte, o download dos dados e a persistência em cache. Não contêm lógica de negócio complexa.
2.  **Cleaners (`src/pipeline/cleaners/`):** Funções puras (sem estado) que recebem dados brutos (JSON/CSV) e devolvem DataFrames normalizados. Este desacoplamento permite testar a limpeza de dados sem precisar de internet ou banco de dados.
3.  **Normalização Centralizada:** Uso do método `normalize_culture_name` (`src/pipeline/utils.py`) para unificar as nomenclaturas de culturas entre IBGE, MAPA e CONAB.

