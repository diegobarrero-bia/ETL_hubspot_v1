# ETL HubSpot → PostgreSQL (Microservicio)

Microservicio FastAPI que sincroniza datos del CRM de HubSpot hacia PostgreSQL. Reemplaza la ejecución vía AWS Lambda con un servicio HTTP persistente que soporta ejecución asíncrona con tracking de jobs y un scheduler interno configurable.

## Estructura del proyecto

```
python-microservice/
├── main.py                     # Entry point FastAPI + lifecycle del scheduler
├── Dockerfile
├── requirements.txt
├── .env.example
├── pytest.ini
├── api/
│   └── v1/
│       └── etl.py              # Endpoints HTTP (trigger, status, listado)
├── models/
│   └── etl.py                  # Pydantic: request, response, job status
├── service/
│   ├── etl_runner.py           # Lógica core del ETL (extraído de handler.py)
│   ├── job_manager.py          # Tracking de jobs + control de concurrencia
│   └── scheduler.py            # APScheduler para ejecución periódica
├── etl/                        # Módulos core del ETL (sin cambios vs Lambda)
│   ├── config.py               # ETLConfig: configuración centralizada
│   ├── hubspot.py              # HubSpotExtractor: API de HubSpot
│   ├── transform.py            # Transformación de datos
│   ├── database.py             # DatabaseLoader: carga a PostgreSQL
│   └── monitor.py              # ETLMonitor: métricas y reportes
└── tests/                      # 76 tests (48 ETL core + 28 microservicio)
    ├── conftest.py
    ├── test_api_endpoints.py
    ├── test_job_manager.py
    ├── test_scheduler.py
    ├── test_phase1_associations.py
    ├── test_phase2_schema_cache.py
    ├── test_phase3_decoupled_fetch.py
    ├── test_phase4_parallelism.py
    ├── test_phase5_raw_upsert.py
    ├── test_phase6_incremental.py
    └── test_deleted_detection.py
```

## Inicio rápido

### 1. Configurar variables de entorno

```bash
cp .env.example .env
# Editar .env con credenciales reales
```

### 2. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 3. Ejecutar el servidor

```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

### 4. Trigger de ETL

```bash
# Lanzar ETL para contacts y deals
curl -X POST http://localhost:8000/api/v1/etl/run \
  -H "Content-Type: application/json" \
  -d '{"object_types": ["contacts", "deals"]}'

# Respuesta (202 Accepted):
# {"job_id": "a1b2c3d4-...", "status": "queued", "message": "ETL job queued for processing"}

# Consultar estado del job
curl http://localhost:8000/api/v1/etl/jobs/a1b2c3d4-...
```

## API Endpoints

| Método | Ruta | Status | Descripcion |
|--------|------|--------|-------------|
| `GET` | `/health` | `200` | Health check para load balancers |
| `POST` | `/api/v1/etl/run` | `202` | Lanzar ETL async. Retorna `job_id` |
| `POST` | `/api/v1/etl/run` | `409` | ETL ya en ejecucion |
| `POST` | `/api/v1/etl/run` | `422` | Validacion fallida (body invalido) |
| `GET` | `/api/v1/etl/jobs/{job_id}` | `200` | Estado de un job |
| `GET` | `/api/v1/etl/jobs/{job_id}` | `404` | Job no encontrado |
| `GET` | `/api/v1/etl/jobs?limit=20` | `200` | Lista de jobs recientes |

### POST `/api/v1/etl/run`

**Request body:**

```json
{
  "object_types": ["contacts", "companies", "deals"],
  "force_full_load": false,
  "log_level": "INFO"
}
```

| Campo | Tipo | Default | Descripcion |
|-------|------|---------|-------------|
| `object_types` | `list[str]` | (requerido) | Objetos de HubSpot a sincronizar |
| `force_full_load` | `bool` | `false` | Forzar carga completa (ignorar incremental) |
| `log_level` | `str` | `"INFO"` | Nivel de log: DEBUG, INFO, WARNING, ERROR, CRITICAL |

**Response (202):**

```json
{
  "job_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "status": "queued",
  "message": "ETL job queued for processing"
}
```

**Response (409) — ETL ya corriendo:**

```json
{
  "detail": {
    "error": "ETL_ALREADY_RUNNING",
    "message": "An ETL job is already running. Wait for it to finish or check /api/v1/etl/jobs for status."
  }
}
```

### GET `/api/v1/etl/jobs/{job_id}`

**Response (200):**

```json
{
  "job_id": "a1b2c3d4-...",
  "status": "completed",
  "progress": "3/3 objects completed",
  "object_types": ["contacts", "companies", "deals"],
  "results": [
    {
      "object_type": "contacts",
      "status": "success",
      "duration_seconds": 45.2,
      "records_fetched": 1500,
      "records_processed_ok": 1498,
      "records_failed": 2,
      "db_upserts": 1498,
      "records_deleted": 5,
      "sync_mode": "incremental"
    }
  ],
  "errors": [],
  "created_at": "2025-01-15T10:00:00+00:00",
  "started_at": "2025-01-15T10:00:01+00:00",
  "finished_at": "2025-01-15T10:02:30+00:00"
}
```

### GET `/api/v1/etl/jobs`

**Query params:** `limit` (1-100, default 20)

**Response (200):**

```json
{
  "jobs": [ ... ],
  "total": 5
}
```

## Ciclo de vida de un Job

```
POST /etl/run
       │
       ▼
   ┌────────┐    thread.start()    ┌─────────┐
   │ QUEUED │ ──────────────────► │ RUNNING │
   └────────┘                      └────┬────┘
                                        │
                          ┌─────────────┼─────────────┐
                          ▼                           ▼
                   ┌───────────┐               ┌────────┐
                   │ COMPLETED │               │ FAILED │
                   └───────────┘               └────────┘
```

1. `POST /etl/run` → el job se crea como **QUEUED** y se retorna `job_id` inmediatamente (202).
2. Un `threading.Thread` arranca en background y el job pasa a **RUNNING**.
3. Se ejecuta `run_etl(config)` por cada `object_type` de la lista.
4. Si un objeto falla, el error se registra y continua con el siguiente (**error isolation**).
5. Al terminar todos los objetos, el job pasa a **COMPLETED** (o **FAILED** si hubo un error fatal).
6. El progreso se actualiza en tiempo real: `"2/3 objects completed"`.

## Control de concurrencia

Solo se permite **un ETL a la vez**, protegido por `threading.Lock`:

- Si se lanza un `POST /etl/run` mientras otro ETL esta corriendo → `409 Conflict`.
- El scheduler interno tambien respeta esta restriccion: si el cron se activa y hay un ETL corriendo, se skipea con un log warning.
- El historial de jobs se mantiene en memoria (max 100 entries, FIFO eviction).

## Scheduler interno (APScheduler)

El servicio incluye un scheduler basado en APScheduler que puede lanzar ETLs periodicamente. Esta **deshabilitado por defecto**.

### Variables de entorno del scheduler

| Variable | Default | Descripcion |
|----------|---------|-------------|
| `SCHEDULER_ENABLED` | `false` | Activar el scheduler (`true` para habilitar) |
| `SCHEDULER_CRON` | `0 */6 * * *` | Expresion cron (default: cada 6 horas) |
| `SCHEDULER_OBJECT_TYPES` | `contacts` | Objetos a sincronizar (separados por coma) |
| `SCHEDULER_FORCE_FULL_LOAD` | `false` | Forzar full load en ejecuciones programadas |

### Ejemplos de expresiones cron

| Expresion | Frecuencia |
|-----------|-----------|
| `0 */6 * * *` | Cada 6 horas |
| `30 2 * * *` | Diario a las 2:30 AM |
| `0 0 * * 1` | Cada lunes a medianoche |
| `0 */2 * * *` | Cada 2 horas |
| `*/30 * * * *` | Cada 30 minutos |

## Variables de entorno

### Requeridas

| Variable | Descripcion |
|----------|-------------|
| `HUBSPOT_ACCESS_TOKEN` | Token de acceso privado (Private App) de HubSpot |
| `DB_HOST` | Host de PostgreSQL |
| `DB_PORT` | Puerto de PostgreSQL |
| `DB_NAME` | Nombre de la base de datos |
| `DB_USER` | Usuario de PostgreSQL |
| `DB_PASS` | Contrasena de PostgreSQL |
| `DB_SCHEMA` | Schema de destino (ej: `hubspot_etl`) |

### Opcionales

| Variable | Default | Descripcion |
|----------|---------|-------------|
| `LOG_LEVEL` | `INFO` | Nivel de logging |
| `SCHEDULER_ENABLED` | `false` | Habilitar scheduler interno |
| `SCHEDULER_CRON` | `0 */6 * * *` | Expresion cron |
| `SCHEDULER_OBJECT_TYPES` | `contacts` | Objetos para el scheduler |
| `SCHEDULER_FORCE_FULL_LOAD` | `false` | Forzar full load en scheduler |

## Proceso ETL

### Flujo de ejecucion

```
HubSpot API                        PostgreSQL
    │                                  │
    │  1. GET /properties              │
    │  2. GET /associations            │
    │  3. GET schema name              │
    │                                  │  4. CREATE SCHEMA / tables
    │  5. GET pipelines ──────────────►│  5. UPSERT pipelines + stages
    │                                  │
    │  6a. Full load:                  │
    │      GET /objects (paginated)    │
    │      + v4 batch associations     │
    │  6b. Incremental:               │
    │      POST /search (modified)     │
    │                                  │
    │  Records ───► Transform ────────►│  7. UPSERT records (batch)
    │                                  │  8. UPSERT associations
    │                                  │  9. Detect deleted records
    │                                  │ 10. Update sync metadata
```

### Modos de sincronizacion

- **Full load**: Extrae todos los registros via API de listado + asociaciones v4 batch. Detecta eliminados comparando IDs en BD vs HubSpot.
- **Incremental**: Usa Search API con filtro `hs_lastmodifieddate >= last_sync`. Solo procesa registros modificados. Detecta archivados via endpoint dedicado.
- **Fallback automatico**: Si hay mas de 10,000 cambios en modo incremental, el sistema cambia a full load automaticamente.

### Objetos soportados

Cualquier objeto estandar o custom de HubSpot:
- Estandar: `contacts`, `companies`, `deals`, `tickets`, `products`, `line_items`
- Custom: usar el nombre del schema (ej: `services`, `p50445259_meters`)

### Caracteristicas del ETL

- **Naming por schema**: El nombre de tabla se deriva del schema de HubSpot, no del `objectType`.
- **Evolucion de schema**: Las columnas nuevas en HubSpot se agregan automaticamente a PostgreSQL via `ALTER TABLE`.
- **Asociaciones v4 batch**: 2 fases — primero records, luego asociaciones en paralelo (3 workers).
- **Deteccion de eliminados**: Columna `fivetran_deleted` marcada como `true` para registros eliminados/archivados.
- **Upsert raw SQL**: `psycopg2.extras.execute_values` con `ON CONFLICT DO UPDATE` para performance.
- **Cache de schema**: `inspect()` se llama una sola vez y se cachea para evitar queries repetitivas.

## Docker

### Build y ejecucion

```bash
docker build -t etl-hubspot .
docker run -p 8000:8000 --env-file .env etl-hubspot
```

### Docker Compose (ejemplo)

```yaml
services:
  etl:
    build: .
    ports:
      - "8000:8000"
    env_file: .env
    depends_on:
      - postgres
    restart: unless-stopped

  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: hubspot_etl
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: your_password
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data

volumes:
  pgdata:
```

### Health check

El Dockerfile incluye un `HEALTHCHECK` integrado:

```
GET http://localhost:8000/health → {"status": "healthy", "service": "etl-hubspot-postgres"}
```

Compatible con Docker health checks, Kubernetes liveness/readiness probes y load balancers.

## Tests

El proyecto tiene **76 tests** organizados en:

| Archivo | Tests | Cobertura |
|---------|-------|-----------|
| `test_api_endpoints.py` | 9 | Endpoints HTTP (trigger, status, listado, validacion) |
| `test_job_manager.py` | 11 | Job tracking, concurrencia, error isolation, historial |
| `test_scheduler.py` | 8 | Start/stop scheduler, cron, manejo de conflictos |
| `test_phase1_associations.py` | 7 | Acumulacion y flush de asociaciones |
| `test_phase2_schema_cache.py` | 5 | Cache de schema + evolucion de columnas |
| `test_phase3_decoupled_fetch.py` | 7 | Fetch desacoplado records + asociaciones v4 |
| `test_phase4_parallelism.py` | 3 | Fetch paralelo + thread safety del monitor |
| `test_phase5_raw_upsert.py` | 6 | Upsert SQL, manejo de NaN/NaT, rollback |
| `test_phase6_incremental.py` | 10 | Search API, metadata, modos full/incremental |
| `test_deleted_detection.py` | 8 | Deteccion de eliminados y archivados |

### Ejecutar tests

```bash
# Todos los tests
python -m pytest tests/ -v

# Solo tests del microservicio
python -m pytest tests/test_api_endpoints.py tests/test_job_manager.py tests/test_scheduler.py -v

# Solo tests del ETL core
python -m pytest tests/ -v --ignore=tests/test_api_endpoints.py --ignore=tests/test_job_manager.py --ignore=tests/test_scheduler.py
```

## Arquitectura: Lambda vs Microservicio

| Aspecto | Lambda (`python-lambda/`) | Microservicio (`python-microservice/`) |
|---------|--------------------------|---------------------------------------|
| Trigger | EventBridge / CLI | HTTP endpoint + APScheduler |
| Ejecucion | Sincrona (hasta timeout) | Asincrona con job tracking |
| Concurrencia | `ReservedConcurrentExecutions: 1` | `threading.Lock` in-process |
| Estado | Sin estado | Jobs en memoria (max 100) |
| Monitoreo | CloudWatch Logs | API de status + logs |
| ETL core | `etl/` | `etl/` (identico, sin cambios) |
