import requests
import time
from typing import Dict, Any

API_URL = "http://localhost:8000"

def fetch_schema() -> Dict[str, Any]:
    try:
        response = requests.get(f"{API_URL}/openapi.json", timeout=5)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"FAILED: Connection to {API_URL} - {str(e)}")
        exit(1)

def run_integration_check():
    spec = fetch_schema()
    paths = spec.get("paths", {})
    
    # Amostras reais para validação
    REAL_CULTURA = "soja"
    REAL_IBGE = "1100015" # Alta Floresta D'Oeste
    REAL_UF = "RO"
    
    total = 0
    fail = 0
    
    print(f"--- API INTEGRITY VERIFICATION (REAL DATA) ---")
    print(f"Target: {API_URL}\n")
    
    for path, methods in paths.items():
        if "get" in methods:
            total += 1
            test_path = path
            
            # Substituição inteligente de parâmetros de path
            test_path = test_path.replace("{id_cultura}", REAL_CULTURA)
            test_path = test_path.replace("{codigo_ibge}", REAL_IBGE)
            test_path = test_path.replace("{cultura}", REAL_CULTURA)
            test_path = test_path.replace("{uf}", REAL_UF)
            
            url = f"{API_URL}{test_path}"
            
            # Parâmetros de query para evitar 422
            params = {
                "page": 1, 
                "page_size": 2,
                "codigo_ibge": REAL_IBGE,
                "cultura": REAL_CULTURA,
                "uf": REAL_UF,
                "ano": 2021,
                "mes": 1
            }
            
            try:
                start = time.time()
                resp = requests.get(url, params=params, timeout=5)
                elapsed = time.time() - start
                
                if resp.status_code >= 400:
                    fail += 1
                    print(f"[ERR] {resp.status_code} | {path} | {elapsed:.3f}s")
                    print(f"      Detail: {resp.text[:150]}")
                else:
                    print(f"[OK ] {resp.status_code} | {path} | {elapsed:.3f}s")
            except Exception as e:
                fail += 1
                print(f"[CRIT] FAILED | {path} | {str(e)}")
                
    print(f"\nRESULTS: {total} tested, {fail} failed.")
    if fail > 0:
        exit(1)

if __name__ == "__main__":
    run_integration_check()
