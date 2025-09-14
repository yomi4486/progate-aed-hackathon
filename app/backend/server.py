from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from ..schema import HealthStatus
from .routers.rpc import rpc_router
from .routers.search import initialize_search_service
from .routers.search import router as search_router
from .routers.search import shutdown_search_service


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan manager."""
    # Startup
    await initialize_search_service()
    yield
    # Shutdown
    await shutdown_search_service()


app = FastAPI(lifespan=lifespan)

# Include routers
app.include_router(search_router, prefix="/rpc")  # Move search to /rpc for frontend compatibility
app.include_router(rpc_router, prefix="/api/v1")  # Move mock API to /api/v1

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
