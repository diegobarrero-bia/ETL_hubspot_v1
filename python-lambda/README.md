# ETL HubSpot → PostgreSQL (Lambda)

ETL serverless que extrae datos de la API de HubSpot y los carga en PostgreSQL.

## Estructura

```
python-lambda/
├── handler.py              # Entry point (Lambda handler + CLI)
├── etl/
│   ├── __init__.py
│   ├── config.py           # Configuración centralizada
│   ├── hubspot.py          # Extracción desde API HubSpot
│   ├── transform.py        # Transformaciones de datos
│   ├── database.py         # Operaciones PostgreSQL
│   └── monitor.py          # Métricas y reportes
├── requirements.txt
├── Dockerfile
└── README.md
```

## Endpoints de HubSpot utilizados

El ETL consume 6 endpoints de la API de HubSpot (v3 y v4):

### 1. Propiedades del objeto

```
GET /crm/v3/properties/{objectType}
```

- **Archivo:** `etl/hubspot.py` → `get_properties_with_types()`
- **Propósito:** Obtiene la lista completa de propiedades con sus tipos de dato (string, number, datetime, etc.)
- **Se usa para:** Saber qué columnas crear en PostgreSQL y qué tipo de dato asignar a cada una
- **Respuesta clave:** `results[].name`, `results[].type`

### 2. Schema del objeto (asociaciones disponibles)

```
GET /crm/v3/schemas/{objectType}
```

- **Archivo:** `etl/hubspot.py` → `get_associations()`
- **Propósito:** Obtiene el schema del objeto, incluyendo todas las asociaciones definidas
- **Se usa para:** Saber con qué otros objetos tiene relaciones (contacts, companies, deals, custom objects, etc.)
- **Respuesta clave:** `associations[].toObjectTypeId` (ej: `0-1` = contacts, `0-2` = companies, `2-XXXXX` = custom objects)

### 3. Pipelines y stages

```
GET /crm/v3/pipelines/{objectType}
```

- **Archivo:** `etl/hubspot.py` → `get_pipelines()`
- **Propósito:** Obtiene los pipelines del objeto con sus stages
- **Se usa para:** Cargar tablas `{objeto}_pipelines` y `{objeto}_pipeline_stages`, y para el smart mapping de columnas de pipeline
- **Nota:** No todos los objetos tienen pipelines. Si falla (404), se ignora silenciosamente

### 4. Listado paginado de registros

```
GET /crm/v3/objects/{objectType}?limit=100&properties=...&archived=false
```

- **Archivo:** `etl/hubspot.py` → `fetch_records_generator()`
- **Propósito:** Descarga todos los registros del objeto con sus propiedades
- **Paginación:** Usa el campo `paging.next.after` para recorrer página por página
- **Límites:** 100 registros por página. Si la URL supera ~8000 chars (muchas propiedades), puede fallar con 414
- **Se usa en:** Full load (Fase 1 de la estrategia de 2 fases)

### 5. Asociaciones batch v4

```
POST /crm/v4/associations/{fromObjectType}/{toObjectType}/batch/read
Body: {"inputs": [{"id": "123"}, {"id": "456"}, ...]}
```

- **Archivo:** `etl/hubspot.py` → `fetch_associations_batch()`
- **Propósito:** Obtiene asociaciones entre objetos en lote
- **Límites:** Máximo 1000 IDs por request. Si hay más, se envían en chunks
- **Respuesta:** HTTP 207 (Multi-Status) con `results[]` y `errors[]`
- **Formato de respuesta v4:** Cada resultado contiene `from.id`, `to[].toObjectId`, `to[].associationTypes[].typeId`
- **Se normaliza a formato v3** internamente: `{id, type, category}`
- **Se usa en:** Full load (Fase 2 — se ejecuta en paralelo con `ThreadPoolExecutor`, 3 workers por defecto)

### 6. Search API (carga incremental)

```
POST /crm/v3/objects/{objectType}/search
Body: {
  "filterGroups": [{"filters": [{"propertyName": "hs_lastmodifieddate", "operator": "GTE", "value": "<timestamp_ms>"}]}],
  "properties": [...],
  "sorts": [{"propertyName": "hs_lastmodifieddate", "direction": "ASCENDING"}],
  "limit": 200
}
```

- **Archivo:** `etl/hubspot.py` → `search_modified_records()`
- **Propósito:** Busca registros modificados desde la última sincronización
- **Límites:** Máximo 10,000 resultados totales, 200 por página
- **Fallback:** Si `total > 10000`, retorna `exceeded_limit=True` y el ETL cambia automáticamente a full load
- **Se usa en:** Carga incremental (cuando existe un `last_sync_timestamp` en la tabla `etl_sync_metadata`)

### Flujo de llamadas API

```
Full Load:
  GET  /properties/{type}        → obtener propiedades
  GET  /schemas/{type}           → obtener asociaciones disponibles
  GET  /pipelines/{type}         → obtener pipelines (opcional)
  GET  /objects/{type}?...       → descargar todos los registros (paginado)
  POST /v4/associations/.../batch/read  × N tipos de asociación (en paralelo)

Incremental:
  GET  /properties/{type}        → obtener propiedades
  GET  /schemas/{type}           → obtener asociaciones disponibles
  GET  /pipelines/{type}         → obtener pipelines (opcional)
  POST /objects/{type}/search    → buscar registros modificados desde última sync
```

### Rate Limits

El ETL maneja automáticamente los rate limits de HubSpot:

| Status Code | Comportamiento |
|-------------|----------------|
| 200 / 207 | Respuesta exitosa |
| 400 | Error fatal — se loguea el detalle y se aborta |
| 414 | URL demasiado larga — demasiadas propiedades en GET |
| 429 | Rate limit — espera 10 segundos y reintenta (máx 3 intentos) |
| 5xx | Error de servidor — espera progresiva y reintenta (máx 3 intentos) |

---

## Ejecución Local (CLI)

### Instalación

```bash
# 1. Instalar dependencias
cd python-lambda/
pip install -r requirements.txt

# 2. Configurar variables de entorno
# Crear archivo .env en la RAÍZ del proyecto (fuera de python-lambda/)
# con las siguientes variables:
#   ACCESS_TOKEN=xxx
#   DB_HOST=localhost
#   DB_PORT=5432
#   DB_NAME=mydb
#   DB_USER=postgres
#   DB_PASS=postgres
```

### Uso

```bash
# Ejecución básica (logs solo en pantalla)
python handler.py --object-type contacts

# Con nivel de log detallado
python handler.py --object-type deals --log-level DEBUG

# Guardar logs en archivo (se ven en pantalla Y se guardan)
python handler.py --object-type services --log-file etl_errors.log

# Guardar en archivo específico por objeto
python handler.py --object-type contacts --log-file logs/contacts.log

# Combinando opciones
python handler.py --object-type companies --log-level DEBUG --log-file debug.log
```

### Opciones CLI

| Opción | Requerida | Default | Descripción |
|--------|-----------|---------|-------------|
| `--object-type` | Sí | - | Tipo de objeto HubSpot (contacts, deals, companies, services, etc.) |
| `--log-level` | No | INFO | Nivel de logging (DEBUG, INFO, WARNING, ERROR, CRITICAL) |
| `--log-file` | No | - | Archivo para guardar logs. Si se proporciona, los logs se ven en pantalla Y se guardan |
| `--full-load` | No | false | Forzar carga completa ignorando sincronización incremental |

**Nota:** El parámetro `--log-file` solo funciona en modo local (CLI). En Lambda, los logs se envían automáticamente a CloudWatch.

## Logs y Reportes

### Logs

**Modo Local (CLI):**
- Por defecto, los logs **solo aparecen en la terminal**
- Para guardarlos en archivo, usa la opción `--log-file`:
  ```bash
  python handler.py --object-type contacts --log-file etl_errors.log
  ```
- Los logs se mostrarán en pantalla Y se guardarán en el archivo especificado

**Modo Lambda:**
- Los logs se envían automáticamente a **Amazon CloudWatch Logs**
- Consulta: AWS Console → CloudWatch → Log Groups → `/aws/lambda/nombre-de-tu-funcion`

### Comportamiento de Logs por Modo

| Modo | Logs en Consola | Logs en Archivo | Destino Final |
|------|----------------|-----------------|---------------|
| **Local sin `--log-file`** | ✅ Sí | ❌ No | Terminal |
| **Local con `--log-file`** | ✅ Sí | ✅ Sí | Terminal + Archivo especificado |
| **Lambda** | ✅ Sí (a stdout) | ❌ No aplica | Amazon CloudWatch Logs |

**Ejemplos prácticos:**

```bash
# Solo ver en pantalla (desarrollo rápido)
python handler.py --object-type contacts

# Guardar en archivo compartido (múltiples objetos)
python handler.py --object-type contacts --log-file etl_errors.log
python handler.py --object-type deals --log-file etl_errors.log

# Guardar en archivos separados por objeto (recomendado)
python handler.py --object-type contacts --log-file logs/contacts.log
python handler.py --object-type deals --log-file logs/deals.log
python handler.py --object-type services --log-file logs/services.log
```

### Reportes de Salud

Al finalizar la ejecución, el ETL retorna un resumen JSON con métricas:

```json
{
  "object_type": "contacts",
  "status": "healthy",
  "duration_seconds": 45.2,
  "records_fetched": 1500,
  "records_processed_ok": 1500,
  "records_failed": 0,
  "db_upserts": 1500,
  "db_insert_errors": 0,
  "api_calls": 16,
  "pipelines_loaded": 2,
  "stages_loaded": 8,
  "association_tables_created": 3,
  "associations_found": 4200,
  "schema_changes": 0,
  "columns_truncated": 0,
  "sync_mode": "full"
}
```

| Campo | Valores posibles |
|-------|-----------------|
| `status` | `healthy` (sin errores) / `unhealthy` (con errores parciales) |
| `sync_mode` | `full` (carga completa) / `incremental` (solo registros modificados) |

**Modo Local:** El resumen se imprime en la terminal al finalizar.

**Modo Lambda:** El resumen está en el campo `body` de la respuesta y se guarda en CloudWatch.

## Ejecución como Lambda

### Evento de invocación

```json
{
  "object_type": "contacts",
  "log_level": "INFO",
  "max_workers": 3,
  "force_full_load": false
}
```

### Variables de entorno (Lambda Configuration)

| Variable | Requerida | Default | Descripción |
|----------|-----------|---------|-------------|
| ACCESS_TOKEN | Si | - | Token de acceso HubSpot |
| DB_HOST | Si | - | Host PostgreSQL |
| DB_PORT | No | 5432 | Puerto PostgreSQL |
| DB_NAME | Si | - | Nombre de la BD |
| DB_USER | Si | - | Usuario BD |
| DB_PASS | Si | - | Contraseña BD |
| DB_SCHEMA | No | hubspot_etl | Schema de destino |
| OBJECT_TYPE | No | contacts | Tipo de objeto por defecto |
| LOG_LEVEL | No | INFO | Nivel de logging |
| ETL_MAX_WORKERS | No | 3 | Workers paralelos para descarga de asociaciones |
| FORCE_FULL_LOAD | No | false | Forzar carga completa (ignora incremental) |

### Respuesta Lambda

```json
{
  "statusCode": 200,
  "body": {
    "object_type": "contacts",
    "status": "healthy",
    "duration_seconds": 45.2,
    "records_fetched": 1500,
    "records_processed_ok": 1500,
    "records_failed": 0,
    "db_upserts": 1500,
    "db_insert_errors": 0,
    "api_calls": 16,
    "pipelines_loaded": 2,
    "stages_loaded": 8,
    "association_tables_created": 3,
    "associations_found": 4200,
    "schema_changes": 0,
    "columns_truncated": 0,
    "sync_mode": "full"
  }
}
```

### Códigos de estado

| Código | Significado |
|--------|-------------|
| 200 | ETL completado sin errores |
| 207 | ETL completado con errores parciales |
| 400 | Error de configuración |
| 500 | Error fatal |

## Deploy con Docker

```bash
# Build
docker build -t hubspot-etl .

# Test local con Lambda Runtime
docker run -p 9000:8080 \
  -e ACCESS_TOKEN=xxx \
  -e DB_HOST=host.docker.internal \
  -e DB_PORT=5432 \
  -e DB_NAME=mydb \
  -e DB_USER=postgres \
  -e DB_PASS=postgres \
  hubspot-etl

# Invocar
curl -X POST "http://localhost:9000/2015-03-31/functions/function/invocations" \
  -d '{"object_type": "contacts"}'
```

## Deploy AWS

```bash
# Build y push a ECR
aws ecr get-login-password --region <region> | \
  docker login --username AWS --password-stdin <account>.dkr.ecr.<region>.amazonaws.com

docker build -t hubspot-etl .
docker tag hubspot-etl:latest <account>.dkr.ecr.<region>.amazonaws.com/hubspot-etl:latest
docker push <account>.dkr.ecr.<region>.amazonaws.com/hubspot-etl:latest
```

## Cron con EventBridge

Para ejecutar automáticamente, crear una regla en EventBridge:

```json
{
  "schedule": "cron(0 2 * * ? *)",
  "targets": [
    {
      "input": "{\"object_type\": \"contacts\"}",
      "arn": "arn:aws:lambda:<region>:<account>:function:hubspot-etl"
    }
  ]
}
```

Para ejecutar múltiples objetos en paralelo, crear una regla por objeto:

```
Regla 1: cron(0 2 * * ? *) → {"object_type": "contacts"}
Regla 2: cron(0 2 * * ? *) → {"object_type": "deals"}
Regla 3: cron(0 2 * * ? *) → {"object_type": "companies"}
Regla 4: cron(0 2 * * ? *) → {"object_type": "services"}
```

## Exit Codes (CLI)

| Código | Significado |
|--------|-------------|
| 0 | Éxito total |
| 1 | Error de ejecución |
| 2 | Error de configuración |
| 130 | Interrupción manual (Ctrl+C) |

---

## Guía Rápida: Casos de Uso

### Desarrollo y Pruebas Locales

```bash
# 1. Primera vez: Probar con logs en pantalla
cd python-lambda/
python handler.py --object-type contacts

# 2. Debugging: Ver todos los detalles con archivo de log
python handler.py --object-type contacts --log-level DEBUG --log-file debug.log

# 3. Producción local: Múltiples objetos con logs separados
python handler.py --object-type contacts --log-file logs/contacts.log
python handler.py --object-type deals --log-file logs/deals.log
python handler.py --object-type companies --log-file logs/companies.log
python handler.py --object-type services --log-file logs/services.log

# 4. Forzar carga completa (ignorar sincronización incremental)
python handler.py --object-type contacts --full-load
```

### Modos de sincronización

- **Primera ejecución:** Siempre hace full load (no hay metadata previa)
- **Ejecuciones posteriores:** Automáticamente usa carga incremental (Search API, solo registros modificados)
- **Forzar full load:** Usa `--full-load` o la variable `FORCE_FULL_LOAD=true`
- **Fallback automático:** Si hay >10,000 cambios desde la última sync, cambia a full load

### Verificar que el Archivo `.env` está configurado

Tu archivo `.env` debe estar en la **raíz del proyecto** (un nivel arriba de `python-lambda/`):

```
test_api_hubspot/
├── .env                    ← Aquí
└── python-lambda/
    └── handler.py          ← Ejecutas desde aquí
```

Contenido mínimo de `.env`:
```env
ACCESS_TOKEN=tu_token_de_hubspot
DB_HOST=localhost
DB_PORT=5432
DB_NAME=nombre_bd
DB_USER=usuario
DB_PASS=contraseña
```

### Troubleshooting

**Problema:** No se encuentran las variables de entorno
```bash
# Solución: Asegúrate de que .env está en la raíz
ls ../.env  # Debe existir

# O ejecuta con variables inline
ACCESS_TOKEN=xxx DB_HOST=localhost ... python handler.py --object-type contacts
```

**Problema:** No se guarda el archivo de log
```bash
# Solución: Verifica que la carpeta exista
mkdir -p logs
python handler.py --object-type contacts --log-file logs/contacts.log
```

**Problema:** Error "module 'etl' not found"
```bash
# Solución: Ejecuta desde python-lambda/, no desde la raíz
cd python-lambda/
python handler.py --object-type contacts
```

