# AgroHarvest BR - Pipeline de Dados Agrícolas

![AgroHarvest BR Banner](./assets/banner_v2.png)

## 🏗️ Arquitetura do Sistema
![Arquitetura Híbrida - AgroHarvest BR](./assets/architecture_diagram_v2.png)

Este projeto é uma solução de **Engenharia de Dados** focada na integração de múltiplos datasets do setor agrícola brasileiro.
 Ele consolida informações de registros de cultivares (RNC), produção agrícola municipal (PAM) e zoneamento de risco climático (ZARC) em um data warehouse estruturado.

## 🚀 Objetivo

O objetivo principal é criar um ambiente unificado para análise de dados agro, permitindo correlacionar a oferta de tecnologias (cultivares registradas) com o desempenho produtivo (IBGE/SIDRA) e a viabilidade climática (MAPA/ZARC). O projeto foi concebido para ser escalável, com a visão de integrar diversas outras fontes de dados governamentais e privadas no futuro.

## 📊 Fontes de Dados

O pipeline extrai e processa dados das seguintes fontes:

1.  **MAPA/SNPC (CultivarWeb):** Registro Nacional de Cultivares (RNC). Fornece dados sobre variedades genéticas certificadas, mantenedores oficiais, portarias de registro e proteção de cultivares.
2.  **IBGE/SIDRA (PAM):** Produção Agrícola Municipal. Séries anuais consolidadas sobre área plantada, área colhida, quantidade produzida e valor da produção para 60+ culturas temporárias e permanentes.
3.  **MAPA/ZARC:** Zoneamento Agrícola de Risco Climático. Define as janelas de plantio ideais por município, cruzando tipos de solo (textura) e ciclos de cultivares para mitigar perdas climáticas.
4.  **CONAB:** Séries históricas de produção, produtividade e preços médios pagos ao produtor, fundamentais para análises de mercado e viabilidade econômica de safras.
5.  **MAPA/Agrofit:** Sistema de Agrotóxicos Fitossanitários. Base de dados sobre defensivos registrados no Brasil, incluindo alvos biológicos (pragas), formulações e orientações técnicas.
6.  **MAPA/SIPEAGRO:** Registro de estabelecimentos produtores e importadores de fertilizantes, corretivos e inoculantes, mapeando a infraestrutura de insumos nutricionais.
7.  **MAPA/SIGEF (Sementes):** Controle e fiscalização da produção de sementes e mudas, garantindo a rastreabilidade e a qualidade da tecnologia genética aplicada no campo.
8.  **INMET (Meteorologia):** Rede de 700+ estações automáticas que fornecem indicadores diários de precipitação, temperatura e umidade para cruzamento com o desempenho das safras.


## 🛠️ Tecnologias Utilizadas

-   **Linguagem:** Python 3.12+
-   **Análise de Dados:** Pandas, NumPy
-   **Banco de Dados:** PostgreSQL via SQLAlchemy (ORM)
-   **API:** FastAPI, Pydantic, Uvicorn
-   **Segurança:** SlowAPI (Rate Limiting)
-   **Testes Automáticos:** Pytest
-   **Infraestrutura:** Docker & Docker Compose
-   **Big Data:** DuckDB (OLAP Engine), Apache Parquet (Formato Colunar)

-   **CI/CD:** GitHub Actions

## 🏗️ Arquitetura do Projeto

O projeto segue uma arquitetura modular baseada em um modelo Estrela (Star Schema):

-   **Dimensões:** Cultura, Município, Mantenedor.
-   **Fatos:** Cadastro de Cultivares, Produção PAM, Risco ZARC, Produção/Preços CONAB, Agrofit, Fertilizantes, SIGEF e Meteorologia INMET.

## 📈 Escalabilidade e Big Data

O AgroHarvest BR foi desenhado para lidar com volumes massivos de dados típicos do agronegócio (Bilhões de linhas) utilizando uma **Arquitetura Híbrida**:

-   **Camada Relacional (PostgreSQL):** Gerencia metadados e entidades com alta integridade (Municípios, DimCultura, Estabelecimentos).
-   **Camada de Analytics (Parquet + DuckDB):** Gerencia os dados massivos e imutáveis. Atualmente, o módulo **ZARC** processa mais de **196 milhões de registros** usando arquivos Parquet particionados por UF, garantindo consultas analíticas em milissegundos sem o custo de um banco de dados tradicional.

### Expansão do ZARC
Atualmente, o dataset de indicações foca em **Soja**. No entanto, a arquitetura é agnóstica a cultura:
1.  **Novas Culturas:** É possível integrar Milho, Café, Arroz, etc., apenas adicionando os CSVs brutos do SISZARC na pasta `data/zarc/`.
2.  **Novas Safras:** O motor de consulta (DuckDB) utiliza *Discovery* automático (`**/*.parquet`), integrando novas safras assim que processadas pelo pipeline.


### Estrutura de Diretórios

```text
.
├── docker/                 # Configurações Docker
│   └── app.Dockerfile      # Imagem única para ETL, API e Testes
├── docs/                   # Documentação técnica e schemas
├── src/                    # Código-fonte (Python root via PYTHONPATH)
│   ├── api/           # Camada de API (Endpoints, Routers, Schemas)
│   ├── db/            # Modelagem Star Schema (SQLAlchemy)
│   ├── pipeline/      # Extractors e Cleaners
│   │   └── cleaners/  # Lógica de limpeza desacoplada (Funcional)
│   └── main.py        # Orquestrador do pipeline de dados
├── tests/              # Suite de testes (Unitários e Integração API)
├── docker-compose.yml  # Orquestração de serviços (app, api, test)
└── DATABASE_METADATA.md # Dicionário de dados do banco
```

## ⚙️ Como Executar

### Pré-requisitos
-   Docker e Docker Compose instalados.

### Passo a Passo

1.  **Configurar Variáveis de Ambiente:**
    ```bash
    cp .env.example .env
    ```
    *Edite o arquivo `.env` se desejar alterar as credenciais padrão do banco de dados.*

2.  **Subir o ambiente e Executar o Pipeline:**
    ```bash
    docker-compose run --rm app
    ```
    *Este comando inicializa o banco de dados PostgreSQL e executa o processo de extração completo.*

2.  **Subir a API e Acessar Documentação:**
    ```bash
    docker-compose up api
    ```
    *Acesse `http://localhost:8000/docs` para visualizar a documentação interativa (Swagger).*

3.  **Executar Testes de Integração e Unitários:**
    ```bash
    docker-compose run --rm test
    ```

## ⚖️ Licença e Uso de Dados

Este projeto utiliza bases de dados públicas regidas pela Lei de Acesso à Informação (LAI) e decretos federais de Dados Abertos. Ao utilizar este código para novos fins, respeite as seguintes atribuições:

-   **IBGE (SIDRA/PAM):** Dados públicos sob os [Termos de Uso do IBGE](https://www.ibge.gov.br/institucional/o-ibge/termos-de-uso.html). A citação da fonte é obrigatória.
-   **CONAB:** Dados sob licença [CC BY-ND 3.0](https://creativecommons.org/licenses/by-nd/3.0/br/). A reprodução é permitida para fins não lucrativos com citação obrigatória da fonte. 
-   **MAPA (ZARC, RNC, Agrofit, Fertilizantes, SIGEF):** Dados abertos conforme o [Decreto nº 8.777/2016](http://www.planalto.gov.br/ccivil_03/_ato2015-2018/2016/decreto/d8777.htm).
-   **INMET:** Dados públicos regidos pela LAI. A citação da fonte (**Instituto Nacional de Meteorologia - INMET**) é obrigatória conforme normas técnicas.

---
*Este projeto faz parte de um portfólio de engenharia de dados (AgroHarvest BR) focado em agronegócio.*
