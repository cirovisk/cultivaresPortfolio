# Pipeline: SIGEF (Controle da Produção de Sementes e Mudas)

Este pipeline extrai e processa dados do **SIGEF (Sistema de Gestão da Fiscalização)** do MAPA, focando no controle da produção de sementes.

## Fontes de Dados

Os dados são extraídos do Portal de Dados Abertos do MAPA:
- **Campos de Produção de Sementes**: Registros de campos autorizados para produção comercial.
- **Declaração de Área para Uso Próprio**: Áreas declaradas por produtores para reserva de sementes para uso próprio.

## Processo de Extração (ETL)

### 1. Extração
- O extrator `SigefExtractor` baixa os arquivos CSV diretamente do portal.
- Implementa cache local (30 dias) e desabilita verificação SSL devido a instabilidades frequentes nos certificados do MAPA.

### 2. Transformação (Cleaners)
Lógica isolada em `src/pipeline/cleaners/sigef.py`:
- **Limpeza de Strings**: Aplica `normalize_string` para cruzamento.
- **Normalização de Municípios**: Busca via código IBGE ou Nome+UF.
- **Tipagem**: Conversão de áreas para `float` e datas para o padrão ISO.
- **Cultura Match**: Vínculo via `get_cultura_id`.

### 3. Carga (Idempotência)
- Utiliza **Bulk Upsert** (ON CONFLICT DO UPDATE).
- Chaves de unicidade:
    - Produção Comercial: `(id_cultura, id_municipio, safra, especie, cultivar_raw, categoria)`
    - Uso Próprio: `(id_cultura, id_municipio, periodo, especie, cultivar_raw)`

## Tabelas no Banco de Dados

- `fato_sigef_producao`: Dados de campos de produção comercial.
- `fato_sigef_uso_proprio`: Dados de reserva de sementes para uso próprio.

## Referência Técnica
- Localização do código: `src/pipeline/sigef.py`
- Testes: `tests/test_sigef.py`
