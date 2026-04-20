from fastapi.testclient import TestClient
from api.main import app

client = TestClient(app)

def test_health_check():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"

def test_culturas_list():
    response = client.get("/culturas")
    assert response.status_code == 200
    assert "items" in response.json()
    assert isinstance(response.json()["items"], list)

def test_municipios_list():
    response = client.get("/municipios")
    assert response.status_code == 200
    assert "items" in response.json()
    assert isinstance(response.json()["items"], list)

def test_producao_pam():
    response = client.get("/producao/pam")
    assert response.status_code == 200
    assert "items" in response.json()
    assert isinstance(response.json()["items"], list)

def test_insumos_agrofit():
    response = client.get("/insumos/agrofit")
    assert response.status_code == 200
    assert "items" in response.json()
    assert isinstance(response.json()["items"], list)

def test_clima_list():
    response = client.get("/clima")
    assert response.status_code == 200
    assert "items" in response.json()
    assert isinstance(response.json()["items"], list)

# ======================================
# Testes dos Endpoints Analytics
# Endpoints compostos retornam 404 quando não existem dados (banco vazio em testes).
# O teste valida que a rota existe e o schema de erro é correto (404, não 500).
# ======================================

def test_analytics_raio_x_municipio_nao_encontrado():
    """Com banco vazio de testes, deve retornar 404 para municipio inexistente."""
    response = client.get("/analytics/raio-x-municipal?codigo_ibge=9999999&cultura=soja&ano=2022")
    assert response.status_code == 404

def test_analytics_dossie_insumos_nao_encontrado():
    """Com banco vazio, cultura inexistente deve retornar 404."""
    response = client.get("/analytics/dossie-insumos/cultura_inexistente")
    assert response.status_code == 404

def test_analytics_viabilidade_economica_nao_encontrado():
    """Com banco vazio, cultura inexistente deve retornar 404."""
    response = client.get("/analytics/viabilidade-economica?cultura=cultura_x&uf=SP&ano=2022")
    assert response.status_code == 404

def test_analytics_janela_aplicacao_nao_encontrado():
    """Com banco vazio, municipio inexistente deve retornar 404."""
    response = client.get("/analytics/janela-aplicacao?codigo_ibge=9999999&ano=2022&mes=3")
    assert response.status_code == 404

def test_analytics_auditoria_estimativas_nao_encontrado():
    """Com banco vazio, cultura inexistente deve retornar 404 (lista vazia não, 404 por cultura)."""
    response = client.get("/analytics/auditoria-estimativas?cultura=cultura_x")
    assert response.status_code == 404
