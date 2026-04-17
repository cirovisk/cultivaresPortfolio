# Pipeline: Zoneamento Agrícola de Risco Climático (ZARC)

Extração de cenários de risco climático para o plantio de diversas culturas por município.

## 📌 Fonte de Dados
- **Agência:** MAPA (Ministério da Agricultura e Pecuária)
- **Origem:** Dados do ZARC integrados via extrator específico.

## 🛠️ Processo de Extração
O processo de extração do ZARC no projeto foca em identificar os cenários de risco baseados nos parâmetros:
1.  **Cultura:** ID da cultura no sistema MAPA.
2.  **Município:** Código IBGE do município.
3.  **Tipo de Solo:** Identificação de solos (Tipo 1, 2 e 3).
4.  **Decêndios:** Períodos de 10 dias de plantio recomendados.

## 🔄 Transformações (Silver Layer)
- **Extração de Parâmetros:** Identificação de quais decêndios (janelas de plantio) possuem riscos de 20%, 30% ou 40%.
- **Padronização Geográfica:** Vinculação robusta com a `DimMunicipio` através do código IBGE.
- **Mapeamento de Solo:** Conversão de códigos brutos para descrições de textura de solo (Arenoso, Médio, Argiloso).

## 💾 Armazenamento
Os dados são persistidos na tabela `fato_risco_zarc`. Este dataset é crucial para correlacionar com a **PAM**, permitindo analisar se os agricultores estão plantando dentro ou fora das janelas de risco recomendadas.
