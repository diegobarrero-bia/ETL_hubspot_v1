# HubSpot Webhook Receiver

Receptor de webhooks de HubSpot en tiempo real. Recibe eventos via HTTP, valida la firma HMAC-SHA256, los encola en AWS SQS, y un worker los procesa para sincronizar datos en PostgreSQL reutilizando la infraestructura ETL de `python-microservice/`.

## Estructura del proyecto

```
python-webhook-receiver/
├── main.py                      # Entry point FastAPI (webhook receiver)
├── Dockerfile                   # Imagen del receiver (puerto 8080)
├── Dockerfile.processor         # Imagen del worker (event processor)
├── requirements.txt
├── .env.webhook.example         # Template de variables de entorno
├── pytest.ini
│
├── api/
│   └── webhooks.py              # POST /webhooks/hubspot
│
├── core/
│   ├── config.py                # WebhookConfig (pydantic-settings)
│   ├── logging_config.py        # Structured JSON logging
│   ├── security.py              # Validacion de firma HubSpot (HMAC-SHA256)
│   └── sqs_client.py            # Productor SQS (boto3)
│
├── processor/
│   ├── __main__.py              # Entry point: python -m processor
│   ├── worker.py                # Loop principal (poll → batch → process)
│   ├── batcher.py               # WebhookEvent model + batching/dedup
│   ├── event_handler.py         # Logica de negocio (fetch, transform, upsert)
│   └── sqs_consumer.py          # Consumidor SQS (long-polling)
│
└── tests/                       # 75 tests
    ├── conftest.py
    ├── test_config.py
    ├── test_webhook_endpoint.py
    ├── test_signature_validation.py
    ├── test_sqs_client.py
    ├── test_sqs_consumer.py
    ├── test_batcher.py
    ├── test_event_handler.py
    ├── test_worker.py
    └── test_logging_config.py
```

## Arquitectura

El sistema se compone de dos procesos independientes conectados via SQS:

```
HubSpot Webhooks
       │
       ▼
┌──────────────────────┐
│   Webhook Receiver   │  FastAPI (puerto 8080)
│                      │
│  1. Valida firma     │
│  2. Envia a SQS      │
│  3. Retorna 200 OK   │
└──────────┬───────────┘
           │
           ▼
     ┌──────────┐
     │ AWS SQS  │
     └────┬─────┘
          │
          ▼
┌──────────────────────┐
│   Event Processor    │  python -m processor
│                      │
│  1. Poll SQS         │
│  2. Batch + dedup    │
│  3. Fetch HubSpot    │
│  4. Upsert PostgreSQL│
└──────────────────────┘
```

**Graceful degradation**: Si SQS falla, el receiver retorna 200 OK igualmente (loguea el error pero no bloquea a HubSpot).

## Inicio rapido

### 1. Configurar variables de entorno

```bash
cp .env.webhook.example .env.webhook
# Editar .env.webhook con credenciales reales
```

### 2. Instalar dependencias

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Ejecutar el webhook receiver

```bash
uvicorn main:app --host 0.0.0.0 --port 8080
```

### 4. Ejecutar el event processor

```bash
python -m processor
```

## API Endpoints

| Metodo | Ruta | Status | Descripcion |
|--------|------|--------|-------------|
| `POST` | `/webhooks/hubspot` | `200` | Recibe eventos de HubSpot, envia a SQS |
| `POST` | `/webhooks/hubspot` | `401` | Firma invalida o headers faltantes |
| `GET` | `/health` | `200` | Health check (`healthy` o `degraded` si SQS inaccesible) |

### POST `/webhooks/hubspot`

**Headers requeridos:**

| Header | Descripcion |
|--------|-------------|
| `X-HubSpot-Signature-v3` | Firma HMAC-SHA256 (base64) |
| `X-HubSpot-Request-Timestamp` | Timestamp en milisegundos |
| `Content-Type` | `application/json` |

**Request body:** Array JSON de eventos webhook de HubSpot.

**Response (200):**

```json
{"received": 3}
```

**Response con error de SQS (200 — graceful degradation):**

```json
{"received": 0, "sqs_error": "Connection refused"}
```

### GET `/health`

```json
{"status": "healthy", "service": "webhook-receiver", "queue_reachable": true}
```

## Tipos de evento soportados

| `subscriptionType` | `change_type` | Procesamiento |
|--------------------|---------------|---------------|
| `contact.creation` | `creation` | Fetch + upsert record |
| `contact.propertyChange` | `update` | Fetch + upsert record |
| `contact.deletion` | `deletion` | Soft delete (`fivetran_deleted = true`) |
| `contact.associationChange` | `association` | Re-fetch con asociaciones + upsert |
| `deal.*`, `company.*`, etc. | Misma logica | Todos los objetos CRM estandar |

## Procesamiento de eventos

### Batching y deduplicacion

`EventBatcher` acumula eventos hasta que se cumple **alguna** de estas condiciones:
- Se alcanza `BATCH_MAX_EVENTS` (default: 100)
- Pasan `BATCH_WAIT_SECONDS` (default: 60s)

`EventBatch.deduplicate()` mantiene solo el evento **mas reciente** por `(object_type, object_id)`, eliminando llamadas redundantes a la API.

### Procesamiento por tipo de cambio

Para cada `object_type` en el batch, los eventos se separan en tres categorias:

1. **Updates y creaciones** (`_process_updates`): Batch read desde HubSpot API → transform → upsert en PostgreSQL.

2. **Eliminaciones** (`_process_deletions`): Soft delete via `loader.mark_records_as_deleted()`. Sin llamada a API.

3. **Cambios de asociacion** (`_process_associations`): Re-fetch registros con asociaciones actualizadas desde HubSpot → upsert registros + flush de tablas bridge (`flush_associations(mode="incremental")`).

## SQS MessageAttributes

Cada mensaje enviado a SQS incluye MessageAttributes para filtrado y observabilidad:

| Atributo | Tipo | Ejemplo | Origen |
|----------|------|---------|--------|
| `eventType` | String | `contact.creation` | `subscriptionType` del evento |
| `objectType` | String | `contact` | Extraido del `subscriptionType` |
| `objectId` | String | `12345` | `objectId` del evento |
| `receivedAt` | String | `2026-02-24T16:00:00+00:00` | ISO 8601 UTC al momento de recepcion |

## Variables de entorno

### Requeridas

| Variable | Descripcion |
|----------|-------------|
| `HUBSPOT_CLIENT_SECRET` | Client secret de la app HubSpot (para validacion de firma) |
| `HUBSPOT_ACCESS_TOKEN` | Token de acceso privado (PAT) para la API de HubSpot |
| `WEBHOOK_URL` | URL publica del endpoint (usada en validacion de firma) |
| `SQS_QUEUE_URL` | URL de la cola SQS |
| `DB_HOST` | Host de PostgreSQL |
| `DB_NAME` | Nombre de la base de datos |
| `DB_USER` | Usuario de PostgreSQL |
| `DB_PASS` | Contrasena de PostgreSQL |

### Opcionales

| Variable | Default | Descripcion |
|----------|---------|-------------|
| `SQS_REGION` | `us-west-2` | Region AWS |
| `DB_PORT` | `5432` | Puerto de PostgreSQL |
| `DB_SCHEMA` | `hubspot_etl` | Schema de destino |
| `BATCH_MAX_EVENTS` | `100` | Eventos maximos por batch |
| `BATCH_WAIT_SECONDS` | `60` | Tiempo maximo de espera antes de flush |
| `POLL_WAIT_SECONDS` | `20` | Duracion del long-polling SQS |
| `LOG_LEVEL` | `INFO` | Nivel de logging |
| `SKIP_SIGNATURE_VALIDATION` | `false` | Desactivar validacion de firma (solo testing local) |

**LocalStack**: Si `SQS_QUEUE_URL` contiene `localhost`, el cliente SQS se conecta automaticamente a `http://localhost:4566`.

## Docker

### Webhook Receiver

```bash
docker build -t webhook-receiver .
docker run -p 8080:8080 --env-file .env.webhook webhook-receiver
```

### Event Processor

```bash
docker build -f Dockerfile.processor -t event-processor .
docker run \
  -v ./python-microservice/etl:/app/etl:ro \
  --env-file .env.webhook \
  event-processor
```

> **Nota:** El processor requiere los modulos ETL montados como volumen read-only en `/app/etl` desde `python-microservice/etl/`.

## Tests

El proyecto tiene **75 tests** organizados en:

| Archivo | Tests | Cobertura |
|---------|-------|-----------|
| `test_batcher.py` | 18 | Parsing de eventos, batching, deduplicacion |
| `test_event_handler.py` | 10 | Logica de procesamiento (updates, deletions, associations) |
| `test_sqs_client.py` | 10 | Productor SQS, health check, MessageAttributes |
| `test_webhook_endpoint.py` | 9 | Endpoint FastAPI, firma, graceful degradation |
| `test_worker.py` | 7 | Loop principal, graceful shutdown, signal handling |
| `test_signature_validation.py` | 7 | HMAC-SHA256, replay protection, timing-safe comparison |
| `test_sqs_consumer.py` | 6 | Consumidor SQS, parsing de mensajes, acknowledge |
| `test_config.py` | 5 | Carga de configuracion, validacion |
| `test_logging_config.py` | 3 | JSON logging, campos estructurados |

### Ejecutar tests

```bash
# Todos los tests
.venv/bin/python -m pytest tests/ -v

# Solo tests de un componente
.venv/bin/python -m pytest tests/test_event_handler.py -v

# Tests de asociaciones
.venv/bin/python -m pytest tests/test_event_handler.py -v -k "association"
```

## Integracion con python-microservice

El event processor reutiliza los modulos ETL de `python-microservice/`:

| Modulo | Uso |
|--------|-----|
| `etl/config.py` | `ETLConfig` para configurar cada object type |
| `etl/hubspot.py` | `HubSpotExtractor` para fetch de records y asociaciones |
| `etl/transform.py` | `process_batch()`, `extract_normalized_associations()` |
| `etl/database.py` | `DatabaseLoader` para upsert, soft delete, flush de asociaciones |
| `etl/monitor.py` | `ETLMonitor` para tracking de metricas |

En desarrollo local, `core/config.py` agrega `python-microservice/` al `sys.path` automaticamente. En Docker, los modulos se montan como volumen read-only.

## Seguridad

- **Validacion de firma**: HMAC-SHA256 (v3) con proteccion contra replay attacks (timestamp max 5 minutos)
- **Comparacion timing-safe**: `hmac.compare_digest()` para prevenir timing attacks
- **Graceful degradation**: Errores de SQS no bloquean la respuesta HTTP
- **Graceful shutdown**: Signals SIGTERM/SIGINT procesan el batch pendiente antes de terminar
