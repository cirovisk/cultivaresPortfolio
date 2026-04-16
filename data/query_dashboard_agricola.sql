-- Dashboard Analítico: Cultivares disponíveis × Produção × Risco ZARC
-- Essa query cruza a inteligência do SNPC, as métricas do IBGE e o Risco Climático do MAPA.

SELECT 
    m.uf,
    m.nome AS municipio,
    c.nome_padronizado AS cultura,
    
    -- Dados da FONTE 1 (IBGE SIDRA 1612)
    pam.area_plantada_ha,
    pam.area_colhida_ha,
    pam.qtde_produzida_ton,
    pam.valor_producao_mil_reais,
    
    -- Dados da FONTE 2 (MAPA ZARC)
    zarc.tipo_solo,
    zarc.risco_climatico,
    
    -- Dados da FONTE 3 (MAPA RNC Cultivares)
    COUNT(DISTINCT f_cult.nr_registro) AS qtd_cultivares_disponiveis
    
FROM dim_municipio m
-- Pivot de Produção (IBGE)
JOIN fato_producao_pam pam 
    ON pam.id_municipio = m.id_municipio
JOIN dim_cultura c 
    ON c.id_cultura = pam.id_cultura

-- Cruza com ZARC (Risco Baixo/Médio/Alto por tipo de solo e município)
LEFT JOIN fato_risco_zarc zarc 
    ON zarc.id_municipio = m.id_municipio 
    AND zarc.id_cultura = c.id_cultura

-- Agrupa com a base genética de Cultivares
LEFT JOIN fato_registro_cultivares f_cult 
    ON f_cult.id_cultura = c.id_cultura

WHERE c.nome_padronizado IN ('soja', 'milho') 
  -- AND m.uf = 'MT'
GROUP BY 
    m.uf, 
    m.nome, 
    c.nome_padronizado, 
    pam.area_plantada_ha,
    pam.area_colhida_ha,
    pam.qtde_produzida_ton,
    pam.valor_producao_mil_reais,
    zarc.tipo_solo,
    zarc.risco_climatico
ORDER BY 
    pam.qtde_produzida_ton DESC NULLS LAST
LIMIT 100;
