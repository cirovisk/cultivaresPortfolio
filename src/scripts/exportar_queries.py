import pandas as pd
from sqlalchemy import text
from db.manager import engine
from pathlib import Path

Path("../data").mkdir(exist_ok=True)

query = """
SELECT 
    m.nome AS municipio,
    m.uf,
    c.nome_padronizado AS cultura,
    pam.area_plantada_ha,
    pam.qtde_produzida_ton,
    zarc.risco_climatico,
    zarc.tipo_solo,
    COUNT(DISTINCT f_cult.nr_registro) AS qtd_cultivares_disponiveis
FROM dim_municipio m
JOIN fato_producao_pam pam 
    ON pam.id_municipio = m.id_municipio
JOIN dim_cultura c 
    ON c.id_cultura = pam.id_cultura
LEFT JOIN fato_risco_zarc zarc 
    ON zarc.id_municipio = m.id_municipio 
    AND zarc.id_cultura = c.id_cultura
LEFT JOIN fato_registro_cultivares f_cult 
    ON f_cult.id_cultura = c.id_cultura
WHERE c.nome_padronizado = 'soja'
GROUP BY 
    m.nome, 
    m.uf, 
    c.nome_padronizado, 
    pam.area_plantada_ha, 
    pam.qtde_produzida_ton,
    zarc.risco_climatico,
    zarc.tipo_solo
ORDER BY 
    pam.qtde_produzida_ton DESC NULLS LAST;
"""

print("Executando query unificada no PostgreSQL...")
df = pd.read_sql(text(query), engine)

caminho_saida = "data/view_dashboard_soja.csv"
df.to_csv(caminho_saida, index=False)
print(f"✅ Dados exportados com sucesso ({len(df)} linhas) para: {caminho_saida}")
