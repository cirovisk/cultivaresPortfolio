import requests
import sys

API_URL = "http://localhost:8000"

def check(name, path, params=None):
    try:
        resp = requests.get(f"{API_URL}{path}", params=params, timeout=10)
        if resp.status_code != 200:
            print(f"[FAIL] {name}: Status {resp.status_code}")
            return False
        
        data = resp.json()
        
        # Se for lista paginada
        if isinstance(data, dict) and "items" in data and "total" in data:
            count = data["total"]
            if count > 0:
                print(f"[OK  ] {name}: {count} rows")
                return True
            else:
                print(f"[WARN] {name}: Empty list (0 rows)")
                return False
        
        # Se for analytics ou detail
        print(f"[OK  ] {name}: Verified")
        return True
        
    except Exception as e:
        print(f"[CRIT] {name}: {str(e)}")
        return False

def run_all():
    print(f"--- 🦍 CULTIVARES API INTEGRITY ---")
    print(f"Target: {API_URL}\n")
    
    results = [
        check("Health", "/"),
        check("Culturas", "/culturas"),
        check("Municipios", "/municipios"),
        check("PAM (IBGE)", "/producao/pam"),
        check("CONAB", "/producao/conab"),
        check("SIGEF", "/producao/sigef"),
        check("Agrofit", "/insumos/agrofit"),
        check("Fertilizantes", "/insumos/fertilizantes"),
        check("Clima (INMET)", "/clima"),
        
        # Analytics
        check("Raio-X (Real Data)", "/analytics/raio-x-municipal", {"codigo_ibge": "1100015", "cultura": "soja", "ano": 2021}),
        check("Dossie Insumos", "/analytics/dossie-insumos/soja"),
        check("Viabilidade", "/analytics/viabilidade-economica", {"cultura": "soja", "uf": "RO", "ano": 2021}),
    ]
    
    success_count = sum(1 for r in results if r)
    total_count = len(results)
    
    print(f"\nFinal: {success_count}/{total_count} passing.")
    if success_count < total_count:
        print("Note: Clima (INMET) expected to fail if API is down.")

if __name__ == "__main__":
    run_all()
