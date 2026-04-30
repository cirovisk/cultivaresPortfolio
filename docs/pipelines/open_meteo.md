# Pipeline: Open-Meteo (Meteorologia)

Extração e consolidação de dados climáticos e meteorológicos históricos. Anteriormente este pipeline utilizava o INMET, mas foi substituído devido à instabilidade e quedas frequentes dos servidores da agência.

## 📌 Fonte de Dados
- **Agência:** Open-Meteo (API Global de Dados Meteorológicos Abertos)
- **Origem:** [Open-Meteo Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api)
- **Latência:** D-2 (Dados consolidados com 2 dias de defasagem).

## 🛠️ Processo de Extração
1. **Coordenadas:** O pipeline primeiro cruza a lista oficial de municípios (IBGE) com uma base open-source do GitHub contendo Latitude e Longitude de todos os 5570 municípios do Brasil.
2. **Workers Paralelos:** Para acelerar a ingestão dos dados diários, utilizamos o `ThreadPoolExecutor` para requisições paralelas e simultâneas.
3. **Limite Diário:** Embora gratuito, a API limita em 10.000 chamadas diárias. O pipeline é configurável para fatiar requisições.

## 💾 Armazenamento (Star Schema)
- **Fato:** `fato_meteorologia`.
- **Relacionamentos:** Chaves estrangeiras para `dim_municipio`.

## 🔄 Indicadores Extraídos
- **Precipitação Total (mm)**
- **Temperatura Máxima (°C)**
- **Temperatura Mínima (°C)**
- **Temperatura Média (°C)**

*Nota: Ao contrário de estações INMET físicas, a API de satélites e modelos globais (Open-Meteo) fornece cobertura contínua e ininterrupta para todos os municípios, mesmo aqueles sem estações meteorológicas.*
