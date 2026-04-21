# Pipeline: INMET (Estações Meteorológicas)

Este pipeline integra dados meteorológicos históricos do **INMET (Instituto Nacional de Meteorologia)** para fornecer contexto climático às séries de produção e risco.

## Fontes de Dados

- **API INMET**: `https://apitempo.inmet.gov.br`
-   **Endpoints Utilizados**:
    -   `/estacoes/T`: Lista de estações automáticas (metadados).
    -   `/estacao/<INICIO>/<FIM>/<ID_ESTACAO>`: Dados horários por estação (Endpoint corrigido).


## Processo de Extração (ETL)

### 1. Descoberta de Estações
O sistema mapeia os municípios cadastrados (`dim_municipio`) para as estações do INMET:
- **Prioridade 1**: Match exato por nome do município e UF.
- **Prioridade 2**: Match parcial por nome.
- **Melhoria Futura**: Cálculo de distância geodésica (lat/long).

### 2. Extração de Dados Horários
O `InmetExtractor` busca dados em blocos anuais para evitar timeouts da API do INMET. 
- Parâmetro Configurável: `days_history` (Default: 730 dias / 2 anos).

### 3. Agregação e Limpeza (Cleaners)
Executado por `src/pipeline/cleaners/inmet.py`:
- **Precipitação**: Soma (Total mm/dia).
- **Temperatura**: Mínima, Máxima e Média aritmética.
- **Umidade**: Média aritmética.
- **Deduplicação**: Tratamento de registros duplicados vindos da API.

### 4. Carga (Idempotência)
- Utiliza **Bulk Upsert** com chave única `(id_municipio, data)`.

## Tabelas no Banco de Dados

- `fato_meteorologia`: Armazena os indicadores climáticos agregados por dia e município.

## Referência Técnica
- Localização do código: `src/pipeline/inmet.py`
- Testes: `tests/test_inmet.py`
