from fastapi import FastAPI
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

from api.routers import culturas, municipios, producao, insumos, clima, analytics

limiter = Limiter(key_func=get_remote_address, default_limits=["30 per minute"])

app = FastAPI(
    title="Cultivares API",
    description="API somente-leitura para dados agropecuários do projeto Cultivares.",
    version="1.0.0"
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)

app.include_router(culturas.router)
app.include_router(municipios.router)
app.include_router(producao.router)
app.include_router(insumos.router)
app.include_router(clima.router)
app.include_router(analytics.router)

@app.get("/")
def health_check():
    return {"status": "ok", "message": "Cultivares API rodando!"}
