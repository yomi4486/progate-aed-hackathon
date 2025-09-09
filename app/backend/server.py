from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .routers.rpc import rpc_router
from ..schema import HealthStatus

app = FastAPI()
app.include_router(rpc_router, prefix="/rpc")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def read_root() -> str:
    return "Hello, World!"


@app.get("/health")
async def health_check() -> HealthStatus:
    return HealthStatus(status="ok")
