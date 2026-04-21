import logging
import traceback
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

from api.routers import culturas, municipios, producao, insumos, clima, analytics, zarc

limiter = Limiter(key_func=get_remote_address, default_limits=["30 per minute"])

app = FastAPI(
    title="AgroHarvest API",
    description="API somente-leitura para dados agropecuários do projeto AgroHarvest BR.",
    version="1.0.0"
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logging.error(f"FATAL ERROR: {request.url}\n{traceback.format_exc()}")
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal Server Error",
            "detail": str(exc),
            "traceback": "Check server logs for technical details"
        }
    )

app.include_router(culturas.router)
app.include_router(municipios.router)
app.include_router(producao.router)
app.include_router(insumos.router)
app.include_router(clima.router)
app.include_router(analytics.router)
app.include_router(zarc.router)

@app.get("/")
def health_check():
    return {"status": "ok", "message": "AgroHarvest API rodando!"}
