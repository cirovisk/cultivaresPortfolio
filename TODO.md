# Próximos Passos (To-do List) - Cultivares

## 🚀 Melhorias de Arquitetura e API
- [x] **Planejar Camada de API (FastAPI):**
    - [x] Criar endpoints estruturados para Fatos (ex: `/producao`, `/clima`).
    - [x] Implementar filtros básicos (ano, cultura, município) via query parameters.
    - [x] Adicionar documentação automática com Swagger/OpenAPI.
- [x] **Implementar Endpoints de Agregação:**
    - [x] Criar consultas pré-processadas que unam Clima e Produção (padrão "Híbrido RESTful").
- [ ] **Organização de Schemas Lógicos:**
    - Avaliar a separação das tabelas por schemas do PostgreSQL (ex: `raw_data`, `analytics`) para reduzir o aspecto monolítico.
- [x] **Segurança de Acesso da API:**
    - [x] Criar `Role` Read-Only (Apenas SELECT) no PostgreSQL (`api_reader`).
    - [x] Configurar FastAPI para usar esse usuário protegido para ler os schemas.

## ⚙️ Infraestrutura e Operações
- [ ] **Configurar Agendamento (Crontab):**
    - No servidor, configurar o disparo periódico do script de atualização:
      `00 03 * * * cd /path/to/project && docker-compose run --rm app python src/db_update.py`
- [ ] **Monitoramento de Logs:**
    - Implementar alerta básico caso o arquivo `logs/update_history.log` contenha erros críticos.

## 📊 Governança e Metadados
- [ ] Expandir o `DATABASE_METADATA.md` com descrições de tipos de dados e chaves estrangeiras.
- [ ] Criar um dicionário de dados para as culturas padronizadas na `dim_cultura`.

## 🧹 Débitos Técnicos (Refatoração)
- [x] **Desmembrar Funções Monolíticas de Data Cleaning:**
    - [x] Seguindo a modularização do `db_update`, separar a lógica de limpeza de dados (os métodos `transform` dos extratores) de modo que possam ser testados isoladamente.
