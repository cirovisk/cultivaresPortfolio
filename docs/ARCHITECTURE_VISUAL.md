# AgroHarvest BR - Arquitetura e Modelagem

Este documento detalha a estrutura de dados e o fluxo de informações do projeto AgroHarvest BR.

## 1. Modelo de Dados (Star Schema)

Abaixo está o diagrama Entidade-Relacionamento (ERD) que detalha como as dimensões e fatos se relacionam no PostgreSQL. Este modelo foi desenhado para otimizar consultas analíticas e reduzir a redundância de dados.

```mermaid
erDiagram
    DIM-CULTURA ||--o{ FATO-CULTIVAR : "possui"
    DIM-CULTURA ||--o{ FATO-PAM : "produz"
    DIM-CULTURA ||--o{ FATO-ZARC : "risco"
    DIM-CULTURA ||--o{ FATO-CONAB : "estimativa"
    DIM-CULTURA ||--o{ FATO-AGROFIT : "insuinos"
    DIM-CULTURA ||--o{ FATO-SIGEF : "sementes"
    
    DIM-MUNICIPIO ||--o{ FATO-PAM : "localização"
    DIM-MUNICIPIO ||--o{ FATO-ZARC : "localização"
    DIM-MUNICIPIO ||--o{ FATO-METEOROLOGIA : "clima"
    DIM-MUNICIPIO ||--o{ FATO-FERTILIZANTES : "estabelecimentos"
    DIM-MUNICIPIO ||--o{ FATO-SIGEF : "localização"
    
    DIM-MANTENEDOR ||--o{ FATO-CULTIVAR : "mantém"

    DIM-CULTURA {
        int id_cultura PK
        string nome_padronizado "Ex: soja, milho"
    }
    DIM-MUNICIPIO {
        int id_municipio PK
        string codigo_ibge "7 dígitos"
        string nome
        string uf
    }
    DIM-MANTENEDOR {
        int id_mantenedor PK
        string nome
        string setor
    }
    FATO-PAM {
        int id_cultura FK
        int id_municipio FK
        int ano PK
        float area_plantada_ha
        float qtde_produzida_ton
    }
    FATO-ZARC {
        int id_cultura FK
        int id_municipio FK
        string tipo_solo PK
        string periodo_plantio PK
        string risco_climatico
    }
    FATO-METEOROLOGIA {
        int id_municipio FK
        datetime data PK
        float precipitacao_mm
        float temp_media_c
    }
```

## 2. Fluxo de Dados (Pipeline ETL)

O pipeline segue uma arquitetura moderna de processamento sequencial, garantindo baixo consumo de memória (via Streaming de dados no ZARC) e alta fidelidade na padronização de nomes.

```mermaid
graph LR
    subgraph "Fontes Gov.br"
        MAPA["MAPA (ZARC, RNC, SIGEF)"]
        IBGE["IBGE (SIDRA/PAM)"]
        INMET["INMET (Clima)"]
        CONAB["CONAB (Safras)"]
    end

    subgraph "Engine de ETL (Python/Docker)"
        EX["Extractors"]
        CL["Cleaners"]
        LD["Loaders"]
    end

    subgraph "Storage & BI"
        PG[(PostgreSQL DW)]
        API["FastAPI"]
        MB["Metabase"]
    end

    MAPA --> EX
    IBGE --> EX
    INMET --> EX
    CONAB --> EX
    
    EX --> CL
    CL --> LD
    LD --> PG
    
    PG --> API
    PG --> MB
```

---
*Diagramas gerados para o portfólio AgroHarvest BR.*
