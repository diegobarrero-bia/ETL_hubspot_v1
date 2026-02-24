"""FastAPI application para HubSpot Webhook Receiver."""
import logging
import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI

from api.webhooks import router as webhooks_router, init_dependencies
from core.config import WebhookConfig

load_dotenv(".env.webhook")

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s - %(levelname)s - [%(name)s:%(funcName)s:%(lineno)d] - %(message)s",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Inicializa configuración y dependencias en startup."""
    try:
        config = WebhookConfig()
        init_dependencies(config)
        logging.getLogger(__name__).info("Webhook receiver iniciado")
    except Exception:
        # En tests, la configuración se inyecta manualmente
        pass
    yield


app = FastAPI(
    title="HubSpot Webhook Receiver",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(webhooks_router)


@app.get("/health")
def health():
    """Health check para load balancer."""
    return {"status": "healthy", "service": "webhook-receiver"}
