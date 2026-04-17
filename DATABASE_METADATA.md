# Metadados do Banco de Dados - Cultivares

Este documento descreve a estrutura do banco de Dados PostgreSQL utilizado no projeto **Cultivares**, seguindo uma modelagem **Star Schema** (Modelo Estrela).

## 🏗️ Arquitetura de Dados

O banco de dados é composto por 3 tabelas de Dimensão e 7 tabelas de Fato, permitindo análises granulares por cultura, município e tempo.

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

---

## 🔄 Auditoria e Metadados Técnicos

Todas as tabelas de Fato possuem o campo:
- `data_modificacao`: Timestamp da última inserção ou atualização (UPSERT), facilitando cargas incrementais.
