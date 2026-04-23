import sys
import os
from sqlalchemy import text
from tabulate import tabulate

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.manager import engine

def fetch_analysis_data(cultures=["soja", "milho"]):
    """
    Data Analysis: Cruzamento de volume de produção vs abundância de cultivares.
    """
    
    # Granularidade: Municipal (PAM/IBGE)
    pam_query = text("""
        WITH TopMunicipios AS (
            SELECT 
                m.nome as localizacao,
                c.id_cultura,
                c.nome_padronizado as cultura,
                p.qtde_produzida_ton as producao,
                'IBGE/PAM' as fonte
            FROM fato_producao_pam p
            JOIN dim_municipio m ON p.id_municipio = m.id_municipio
            JOIN dim_cultura c ON p.id_cultura = c.id_cultura
            WHERE c.nome_padronizado = ANY(:cultures)
            ORDER BY p.qtde_produzida_ton DESC
            LIMIT 20
        ),
        ContagemCultivares AS (
            SELECT id_cultura, count(*) as total FROM fato_registro_cultivares WHERE situacao = 'REGISTRADA' GROUP BY id_cultura
        )
        SELECT tm.*, cc.total FROM TopMunicipios tm JOIN ContagemCultivares cc ON tm.id_cultura = cc.id_cultura;
    """)
    
    with engine.connect() as conn:
        res = conn.execute(pam_query, {"cultures": list(cultures)}).fetchall()
        if res:
            return res, "MUNICÍPIOS (IBGE/PAM)"
        
        # Granularidade: Estadual (CONAB) - Fallback
        conab_query = text("""
            WITH TopUFs AS (
                SELECT 
                    uf as localizacao,
                    c.id_cultura,
                    c.nome_padronizado as cultura,
                    SUM(producao_mil_t) * 1000 as producao,
                    'CONAB' as fonte
                FROM fato_producao_conab p
                JOIN dim_cultura c ON p.id_cultura = c.id_cultura
                WHERE c.nome_padronizado = ANY(:cultures) 
                  AND (ano_agricola LIKE '2023%' OR ano_agricola LIKE '2024%')
                GROUP BY uf, c.nome_padronizado, c.id_cultura
                ORDER BY producao DESC
                LIMIT 20
            ),
            ContagemCultivares AS (
                SELECT id_cultura, count(*) as total FROM fato_registro_cultivares WHERE situacao = 'REGISTRADA' GROUP BY id_cultura
            )
            SELECT tu.*, cc.total FROM TopUFs tu JOIN ContagemCultivares cc ON tu.id_cultura = cc.id_cultura;
        """)
        res = conn.execute(conab_query, {"cultures": list(cultures)}).fetchall()
        if res:
            return res, "ESTADOS (CONAB)"
            
    return [], "NENHUM DADO"

def main():
    print("\n" + "="*80)
    print("ANÁLISE: PRODUÇÃO vs CULTIVARES REGISTRADAS")
    print("="*80)
    
    results, nivel = fetch_analysis_data()
    
    if not results:
        print("Nenhum dado encontrado nas tabelas PAM ou CONAB.")
        print("Execute o pipeline primeiro: python src/main.py --sources sidra conab cultivares")
        return

    print(f"Nível de Agregação: {nivel}\n")
    headers = ["Localização", "Cultura", "Produção (ton)", "Fonte", "Cultivares Reg."]
    table_data = [[r.localizacao, r.cultura.upper(), f"{r.producao:,.0f}".replace(",", "."), r.fonte, r.total] for r in results]
    
    print(tabulate(table_data, headers=headers, tablefmt="grid"))
    print("\n* Cruzamento realizado via Cultivares Pipeline")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()
