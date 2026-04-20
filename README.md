# Cultivares - Pipeline de Dados Agrícolas

Este projeto é uma solução de **Engenharia de Dados** focada na integração de múltiplos datasets do setor agrícola brasileiro. Ele consolida informações de registros de cultivares, produção agrícola municipal e zoneamento de risco climático em um data warehouse estruturado.

## 🚀 Objetivo

O objetivo principal é criar um ambiente unificado para análise de dados agro, permitindo correlacionar a oferta de tecnologias (cultivares registradas) com o desempenho produtivo (IBGE/SIDRA) e a viabilidade climática (MAPA/ZARC). O projeto foi concebido para ser escalável, com a visão de integrar diversas outras fontes de dados governamentais e privadas no futuro.

## 📊 Fontes de Dados

O pipeline extrai e processa dados das seguintes fontes:

1.  **MAPA/SNPC (CultivarWeb):** Dados sobre cultivares registradas e protegidas no Brasil.
2.  **IBGE/SIDRA (PAM):** Produção Agrícola Municipal (área, quantidade, valor).
3.  **MAPA/ZARC:** Zoneamento de Risco Climático por município.
4.  **CONAB:** Séries históricas de produção e indicadores de preços.
5.  **MAPA/Agrofit:** Sistema de agrotóxicos fitossanitários.
6.  **MAPA/SIPEAGRO:** Dados de estabelecimentos de fertilizantes.
7.  **MAPA/SIGEF (Sementes):** Controle da produção de sementes e mudas.
8.  **INMET:** Rede de estações meteorológicas (dados diários).

## 🛠️ Tecnologias Utilizadas

-   **Linguagem:** Python 3.12+
-   **Análise de Dados:** Pandas, NumPy
-   **Banco de Dados:** PostgreSQL via SQLAlchemy (ORM)
-   **API:** FastAPI, Pydantic, Uvicorn
-   **Segurança:** SlowAPI (Rate Limiting)
-   **Testes Automáticos:** Pytest
-   **Infraestrutura:** Docker & Docker Compose
-   **CI/CD:** GitHub Actions

## 🏗️ Arquitetura do Projeto

O projeto segue uma arquitetura modular baseada em um modelo Estrela (Star Schema):

-   **Dimensões:** Cultura, Município, Mantenedor.
-   **Fatos:** Cadastro de Cultivares, Produção PAM, Risco ZARC, Produção/Preços CONAB, Agrofit, Fertilizantes, SIGEF e Meteorologia INMET.

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
*Este projeto faz parte de um portfólio de engenharia de dados focado em agronegócio.*
