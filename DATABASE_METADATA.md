# Metadados do Banco de Dados - Cultivares

Este documento descreve a estrutura do banco de Dados PostgreSQL utilizado no projeto **Cultivares**, seguindo uma modelagem **Star Schema** (Modelo Estrela).

## 🏗️ Arquitetura de Dados

O banco de dados é composto por 3 tabelas de Dimensão e 11 tabelas de Fato, permitindo análises granulares por cultura, município e tempo.

---

## 📐 Dimensões

### `dim_cultura`
Armazena os nomes padronizados das culturas para garantir a integridade referencial entre diferentes fontes (SIDRA, CONAB, ZARC, Agrofit).
- `id_cultura` (PK): Identificador único.
- `nome_padronizado` (Unique): Nome da cultura em snake_case (ex: `soja`, `milho`).

### `dim_municipio`
Armazena informações geográficas baseadas no código IBGE.
- `id_municipio` (PK): Identificador único.
- `codigo_ibge` (Unique): Código de 7 dígitos do IBGE.
- `nome`: Nome do município.
- `uf`: Sigla da Unidade Federativa.

### `dim_mantenedor`
Cadastro de empresas ou instituições responsáveis pelo registro da cultivar.
- `id_mantenedor` (PK): Identificador único.
- `nome` (Unique): Razão social ou nome do requerente.
- `setor`: Classificação do mantenedor (Público, Privado ou Misto).

---

## 📊 Fatos

### `fato_registro_cultivares` (Fonte: MAPA/SNPC)
Registros oficiais de cultivares no Registro Nacional de Cultivares (RNC).
- `nr_registro` (PK): Número do registro no MAPA.
- `id_cultura` (FK): Chave para `dim_cultura`.
- `id_mantenedor` (FK): Chave para `dim_mantenedor`.
- `cultivar`: Nome comercial da cultivar.
- `nome_secundario`: Nome alternativo/apelido.
- `situacao`: Status do registro (Ex: REGISTRADA).
- `nr_formulario`: Número do formulário de submissão.
- `data_reg`: Data do registro oficial.
- `data_val`: Data de validade do registro.

### `fato_producao_pam` (Fonte: IBGE/SIDRA)
Série histórica da Produção Agrícola Municipal.
- `id_producao` (PK): Identificador único.
- `id_cultura` (FK): Chave para `dim_cultura`.
- `id_municipio` (FK): Chave para `dim_municipio`.
- `ano`: Ano de referência.
- `area_plantada_ha`: Área plantada em hectares.
- `area_colhida_ha`: Área colhida em hectares.
- `qtde_produzida_ton`: Produção total em toneladas.
- `valor_producao_mil_reais`: Valor da produção em mil reais.

### `fato_risco_zarc` (Fonte: MAPA/ZARC)
Zoneamento de Risco Climático por município e solo.
- `id_zarc` (PK): Identificador único.
- `id_cultura` (FK): Chave para `dim_cultura`.
- `id_municipio` (FK): Chave para `dim_municipio`.
- `tipo_solo`: Classificação do solo (Tipo 1, 2 ou 3).
- `periodo_plantio`: Decêndio/Período recomendado.
- `risco_climatico`: Percentual de risco (20%, 30%, 40%).

### `fato_producao_conab` (Fonte: CONAB)
Estimativas e histórico de produção por UF e Safra.
- `id_conab` (PK): Identificador único.
- `id_cultura` (FK): Chave para `dim_cultura`.
- `uf`: UF de referência.
- `ano_agricola`: Ciclo (ex: 2023/24).
- `safra`: Identificação da safra (1ª, 2ª ou 3ª).
- `area_plantada_mil_ha`: Área em mil hectares.
- `producao_mil_t`: Produção em mil toneladas.
- `produtividade_t_ha`: Rendimento médio (ton/ha).

### `fato_precos_conab_mensal` (Fonte: CONAB)
Série de preços médios mensais recebidos pelos produtores.
- `id_preco` (PK): Identificador único.
- `id_cultura` (FK): Chave para `dim_cultura`.
- `id_municipio` (FK): Chave opcional para `dim_municipio`.
- `uf`: UF de referência.
- `ano`: Ano civil.
- `mes`: Mês (1 a 12).
- `valor_kg`: Valor pago ao produtor por KG.
- `nivel_comercializacao`: Nível da transação (ex: Produtor).

### `fato_precos_conab_semanal` (Fonte: CONAB)
Dados de preços com granularidade semanal.
- `semana`: Número da semana no ano.
- `data_referencia`: Período da semana (Início/Fim).
- (Demais campos idênticos ao mensal)

### `fato_agrofit` (Fonte: MAPA/Agrofit)
Relacionamento entre culturas e agrotóxicos/defensivos registrados.
- `id_agrofit` (PK): Identificador único.
- `id_cultura` (FK): Chave para `dim_cultura`.
- `nr_registro`: Registro do produto no MAPA.
- `marca_comercial`: Nome comercial do defensivo.
- `ingrediente_ativo`: Princípio ativo.
- `titular_registro`: Empresa detentora do registro.
- `classe`: Classificação (Herbicida, Inseticida, etc).
- `praga_comum`: Nome comum da praga/alvo biológico.

### `fato_fertilizantes_estabelecimentos` (Fonte: MAPA/SIPEAGRO)
Cadastro de estabelecimentos produtores, importadores e comerciantes de fertilizantes.
- `id_fertilizante` (PK): Identificador único.
- `id_municipio` (FK): Chave para `dim_municipio` (via mapeamento de nome/UF).
- `uf`: UF do estabelecimento.
- `municipio`: Nome do município original.
- `nr_registro_estabelecimento` (Unique): Número de registro no SIPEAGRO.
- `status_registro`: Situação (Ativo, Cancelado, etc).
- `cnpj`: CNPJ do estabelecimento.
- `razao_social`: Razão social da empresa.
- `nome_fantasia`: Nome fantasia.
- `area_atuacao`: Área (ex: FERTILIZANTE, INOCULANTE).
- `atividade`: Atividade (ex: PRODUTOR, IMPORTADOR).
- `classificacao`: Detalhamento da classificação do estabelecimento.
 
### `fato_sigef_producao` (Fonte: MAPA/SIGEF)
Controle da produção comercial de sementes e mudas.
- `id_sigef_producao` (PK): Identificador único.
- `id_cultura` (FK): Chave para `dim_cultura`.
- `id_municipio` (FK): Chave para `dim_municipio`.
- `safra`: Ciclo de produção (ex: 2023/2023).
- `especie`: Nome da espécie original.
- `cultivar_raw`: Nome da cultivar original.
- `categoria`: Categoria da semente (C1, C2, S1, S2, etc).
- `status`: Situação do campo de produção.
- `data_plantio`: Data de plantio do campo.
- `data_colheita`: Data de colheita.
- `area_ha`: Área do campo em hectares.
- `producao_bruta_t`: Volume colhido bruto (ton).
- `producao_est_t`: Estimativa de produção (ton).
 
### `fato_sigef_uso_proprio` (Fonte: MAPA/SIGEF)
Declarações de reserva de sementes para uso próprio do produtor.
- `id_sigef_uso_proprio` (PK): Identificador único.
- `id_cultura` (FK): Chave para `dim_cultura`.
- `id_municipio` (FK): Chave para `dim_municipio`.
- `periodo`: Ano/Safra da declaração.
- `tipo_periodo`: Granularidade do período (ex: ANO).
- `cultivar_raw`: Cultivar reservada.
- `area_total_ha`: Área total declarada.
- `area_plantada_ha`: Área efetivamente plantada.
- `area_estimada_ha`: Área estimada de produção.
 
### `fato_meteorologia` (Fonte: INMET)
Dados meteorológicos diários agregados por município.
- `id_meteo` (PK): Identificador único.
- `id_municipio` (FK): Chave para `dim_municipio`.
- `data`: Data de referência.
- `precipitacao_total_mm`: Chuva acumulada no dia.
- `temp_max_c`: Temperatura máxima atingida.
- `temp_min_c`: Temperatura mínima atingida.
- `temp_media_c`: Temperatura média aritmética do dia.
- `umidade_media`: Umidade relativa média (%).
- `estacao_id`: Código da estação INMET de origem.

---

## 🔒 Segurança e Acesso

O banco de dados segue o princípio do privilégio mínimo para a camada de exposição:

1.  **Usuário de Aplicação (`postgres`):** Possui permissão de `OWNER`, utilizado exclusivamente pelo pipeline de ETL para criar tabelas e realizar `UPSERT`.
2.  **Usuário de API (`api_reader`):** Possui permissão restrita de `SELECT` em todas as tabelas. 
    - Toda a comunicação da API FastAPI é feita via este usuário.
    - Script de configuração: `docs/setup_api_reader.sql`.

## 🔄 Auditoria e Metadados Técnicos

Todas as tabelas de Fato possuem o campo:
- `data_modificacao`: Timestamp da última inserção ou atualização (UPSERT), facilitando cargas incrementais e auditoria de frescor dos dados.
