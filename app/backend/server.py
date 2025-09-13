from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from ..schema import HealthStatus
from .routers.rpc import rpc_router
from .routers.search import initialize_search_service
from .routers.search import router as search_router
from .routers.search import shutdown_search_service

app = FastAPI()

# Include routers
app.include_router(rpc_router, prefix="/rpc")
app.include_router(search_router, prefix="/api/v1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Register startup and shutdown events
@app.on_event("startup")
async def startup_event():
    """Initialize services on startup."""
    await initialize_search_service()


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup services on shutdown."""
    await shutdown_search_service()


@app.get("/")
async def read_root() -> str:
    return "Hello, World!"


@app.get("/health")
async def health_check() -> HealthStatus:
    return HealthStatus(status="ok")
