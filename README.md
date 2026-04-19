# Cultivares - Pipeline de Dados Agrícolas

Este projeto é uma solução de **Engenharia de Dados** focada na integração de múltiplos datasets do setor agrícola brasileiro. Ele consolida informações de registros de cultivares, produção agrícola municipal e zoneamento de risco climático em um data warehouse estruturado.

## 🚀 Objetivo

O objetivo principal é criar um ambiente unificado para análise de dados agro, permitindo correlacionar a oferta de tecnologias (cultivares registradas) com o desempenho produtivo (IBGE/SIDRA) e a viabilidade climática (MAPA/ZARC). O projeto foi concebido para ser escalável, com a visão de integrar diversas outras fontes de dados governamentais e privadas no futuro.

## 📊 Fontes de Dados

O pipeline extrai e processa dados das seguintes fontes:

1.  **MAPA/SNPC (CultivarWeb):** Dados sobre cultivares registradas e protegidas no Brasil.
2.  **IBGE/SIDRA (PAM):** Produção Agrícola Municipal, contemplando área plantada, colhida, quantidade produzida e valor da produção.
3.  **MAPA/ZARC:** Zoneamento de Agrícola de Risco Climático, indicando períodos de plantio com menores riscos por município e cultura.

## 🛠️ Tecnologias Utilizadas

-   **Linguagem:** Python 3.10+
-   **Processamento de Dados:** Pandas, NumPy
-   **Banco de Dados:** PostgreSQL (armazenamento final) via SQLAlchemy
-   **Testes Automáticos:** Pytest
-   **Infraestrutura:** Docker & Docker Compose
-   **CI/CD:** GitHub Actions

## 🏗️ Arquitetura do Projeto

O projeto segue uma arquitetura modular baseada em um modelo Estrela (Star Schema):

-   **Dimensões:** Cultura, Município, Mantenedor.
-   **Fatos:** Cadastro de Cultivares, Produção PAM, Risco ZARC.

### Estrutura de Diretórios

```text
.
├── data/               # Dados brutos e persistência local (cache)
├── notebooks/          # Análises exploratórias e prototipagem
├── src/
│   ├── db/            # Definição do esquema e manager do banco
│   ├── pipeline/      # Classes Extratoras (ETL)
│   └── main.py        # Orquestrador principal do pipeline
├── tests/              # Suite de testes unitários e de integração
├── Dockerfile          # Definição do container da aplicação
└── docker-compose.yml  # Orquestração do banco e serviços
```

## ⚙️ Como Executar

### Pré-requisitos
-   Docker e Docker Compose instalados.

### Passo a Passo

1.  **Subir o ambiente (Banco de Dados):**
    ```bash
    docker-compose up -d
    ```

2.  **Configurar o ambiente virtual (opcional/local):**
    ```bash
    python -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt
    ```

3.  **Executar o pipeline:**
    ```bash
    python src/main.py
    ```

## 🧪 Testes

Para garantir a qualidade dos dados e a integridade do código, execute:

```bash
pytest tests/
```

## ⚖️ Licença e Uso de Dados

Este projeto utiliza bases de dados públicas regidas pela Lei de Acesso à Informação (LAI) e decretos federais de Dados Abertos. Ao utilizar este código para novos fins, respeite as seguintes atribuições:

-   **IBGE (SIDRA/PAM):** Dados públicos sob os [Termos de Uso do IBGE](https://www.ibge.gov.br/institucional/o-ibge/termos-de-uso.html). A citação da fonte é obrigatória.
-   **CONAB:** Dados sob licença [CC BY-ND 3.0](https://creativecommons.org/licenses/by-nd/3.0/br/). A reprodução é permitida para fins não lucrativos com citação obrigatória da fonte. 
    > *Nota: Este projeto tem fins estritamente educativos e de portfólio, respeitando a natureza não comercial dos dados da CONAB.*
-   **MAPA (ZARC, RNC, Agrofit, Fertilizantes):** Dados abertos conforme o [Decreto nº 8.777/2016](http://www.planalto.gov.br/ccivil_03/_ato2015-2018/2016/decreto/d8777.htm) (Política Nacional de Dados Abertos).

---
*Este projeto faz parte de um portfólio de engenharia de dados focado em agronegócio.*
