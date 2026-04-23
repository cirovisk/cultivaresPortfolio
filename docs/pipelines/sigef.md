# Pipeline SIGEF (Sementes e Mudas)

## 📌 Contexto
O SIGEF (Sistema de Gestão da Fiscalização de Sementes e Mudas) do MAPA fornece dados brutos sobre a produção e reserva de sementes no Brasil. Um dos principais desafios técnicos desta fonte é o uso de **Nomenclatura Botânica (Latim)** em vez de nomes populares.

## 🛠️ Solução de Interoperabilidade
Para garantir que os dados do SIGEF possam ser cruzados com outras fontes (como IBGE/PAM), implementamos um **Mapeador de Sinônimos Científicos** no núcleo do pipeline (`src/pipeline/loaders.py`).

### Mapeamento Implementado:
| Nome Científico (SIGEF) | Nome Popular (Banco) | Cultura Relacionada |
|-------------------------|----------------------|---------------------|
| `Glycine max`           | Soja                 | Soja                |
| `Zea mays`              | Milho                | Milho               |
| `Triticum aestivum`     | Trigo                | Trigo               |
| `Gossypium hirsutum`    | Algodão              | Algodão             |
| `Avena strigosa`        | Aveia                | Aveia               |
| `Saccharum`             | Cana-de-açúcar       | Cana-de-açúcar      |

## 🚀 Lógica de Normalização
O robô utiliza uma técnica de **Token Matching** com fronteiras de palavras. 
1.  O nome bruto é normalizado (remoção de acentos/lowercase).
2.  O dicionário de sinônimos é consultado.
3.  Se houver um match parcial de palavra inteira (ex: "Avena" em "Avena strigosa Schreb."), o registro é vinculado ao ID da cultura correspondente.
4.  Registros que não possuem correspondência botânica ou popular são descartados para manter a integridade referencial do Star Schema.

## 📊 Impacto no Dashboard
Esta implementação permitiu que o dashboard de "Reserva de Sementes" passasse de uma visão mono-cultura (apenas Trigo) para uma visão multi-cultura, abrangendo as principais commodities do agronegócio brasileiro.
