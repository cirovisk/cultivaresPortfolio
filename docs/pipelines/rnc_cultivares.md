# Pipeline: Registro Nacional de Cultivares (RNC)

Extração do cadastro oficial de todas as cultivares (sementes/mudas) registradas para comercialização no Brasil.

## 📌 Fonte de Dados
- **Sistema:** CultivarWeb / MAPA SNPC.
- **Tipo:** Dados de Registro e Proteção (Lei nº 9.456/1997 e Lei nº 10.711/2003).

## 🛠️ Processo de Extração
1.  **Web Scraping / Download:** O pipeline acessa a base consolidada do CultivarWeb que lista as variedades por cultura.
2.  **Granularidade:** O dado vem ao nível de **Variedade (Cultivar)** e seu respectivo **Mantenedor** (empresa responsável pela genética).

## 🔄 Transformações (Cleaners)
Executado em `src/pipeline/cleaners/cultivares.py`:
- **Extração de Empresas:** Normaliza o nome do mantenedor e classifica o **Setor** (ex: Privado vs Público - EMBRAPA).
- **Datas de Validade:** Conversão de strings para objetos `date`.
- **Cultura Match:** Uso de `normalize_culture_name` e busca de IDs na dimensão.

## 💾 Armazenamento
- Os mantenedores alimentam a `dim_mantenedor`.
- Os registros detalhados das variedades alimentam a `fato_registro_cultivares`.
- Esta tabela é o **dimante** do projeto, permitindo saber quais tecnologias de semente estavam disponíveis em cada ano de safra.
