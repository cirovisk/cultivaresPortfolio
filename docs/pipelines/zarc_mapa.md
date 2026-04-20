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

## 🔄 Transformações (Cleaners)
Lógica centralizada em `src/pipeline/cleaners/zarc.py`:
- **Extração de Parâmetros:** Identificação de decêndios com riscos (20%, 30%, 40%).
- **Cultura Match:** Mapeamento via `get_cultura_id`.
- **Mapeamento de Solo:** Conversão para descrições de textura (Arenoso, Médio, Argiloso).

## 💾 Armazenamento
Os dados são persistidos na tabela `fato_risco_zarc`. Este dataset é crucial para correlacionar com a **PAM**, permitindo analisar se os agricultores estão plantando dentro ou fora das janelas de risco recomendadas.
