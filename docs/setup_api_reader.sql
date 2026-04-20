-- Script para criação de usuário read-only para a API
-- Executar como superusuário (ex: postgres) no banco cultivares_db

CREATE ROLE api_reader WITH LOGIN PASSWORD 'api_reader_pass';
GRANT CONNECT ON DATABASE cultivares_db TO api_reader;
GRANT USAGE ON SCHEMA public TO api_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO api_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO api_reader;
