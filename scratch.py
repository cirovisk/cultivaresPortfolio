import requests

url = "https://apisidra.ibge.gov.br/values/t/1612/n6/all/v/all/p/last/c782/40280"
try:
    resp = requests.get(url, timeout=10)
    print("SIDRA Status:", resp.status_code)
    try:
        data = resp.json()
        print("SIDRA First 2:", data[:2])
    except:
        print("SIDRA Content:", resp.text[:200])
except Exception as e:
    print("SIDRA Error:", e)

# Test ZARC
zarc_url = "https://dados.agricultura.gov.br/dataset/zarc/resource/d4abdb35-9515-4ba8-9c4c-4e8c1ab6a258"
try:
    zresp = requests.get(zarc_url, timeout=10)
    print("ZARC Status:", zresp.status_code)
except Exception as e:
    print("ZARC Error:", e)

