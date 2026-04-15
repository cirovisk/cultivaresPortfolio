import requests

url = "https://servicodados.ibge.gov.br/api/v3/agregados/1612/metadados"
try:
    resp = requests.get(url, timeout=10)
    data = resp.json()
    for classif in data.get("classificacoes", []):
        if classif["id"] == "81": # Lavoura temporária
            print("Categorias c81:")
            for cat in classif.get("categorias", []):
                print(f"{cat['id']}: {cat['nome']}")
except Exception as e:
    print(e)
