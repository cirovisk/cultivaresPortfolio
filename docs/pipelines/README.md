# Documentação de Pipelines (ETL)

Esta pasta contém o detalhamento técnico de cada pipeline de extração e transformação (ETL) implementado no projeto **Cultivares**.

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

Todos os extratores herdam da classe abstrata `BaseExtractor` (`src/pipeline/base_extractor.py`), garantindo:
1.  **Logs Padronizados:** Uso de logging estruturado para monitoramento.
2.  **Tratamento de Cache:** Verificação de arquivos locais antes de downloads pesados.
3.  **Normalização Centralizada:** Uso do método `normalize_culture_name` para evitar discrepâncias entre nomes de culturas (ex: transformando "Soja (em grão)" e "SOJA" em um identificador único).
