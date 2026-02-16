# ETL HubSpot → PostgreSQL (Lambda)

ETL serverless que extrae datos de la API de HubSpot y los carga en PostgreSQL. Soporta carga completa (full load) e incremental, detección de registros eliminados, y ejecución de múltiples objetos en una sola invocación.

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
├── tests/
│   ├── conftest.py         # Fixtures compartidos (fake_config, monitor, etc.)
│   ├── test_hubspot.py     # Tests de extracción API
│   ├── test_transform.py   # Tests de transformaciones
│   ├── test_database.py    # Tests de operaciones PostgreSQL
│   ├── test_monitor.py     # Tests de métricas y reportes
│   ├── test_config.py      # Tests de configuración
│   └── test_deleted_detection.py  # Tests de detección de eliminados
├── requirements.txt
├── Dockerfile
└── README.md
```

## Nombres de tablas (Schema-based naming)

Los nombres de las tablas en PostgreSQL se resuelven desde el **schema de HubSpot**, no directamente del `object_type`. Esto produce nombres más limpios y consistentes con las asociaciones de HubSpot.

El ETL consulta `GET /crm/v3/schemas/{objectType}` y extrae el campo `name` del schema. Ejemplos:

| object_type | Schema name | Tabla principal | Tabla pipelines | Tabla stages |
|-------------|-------------|-----------------|-----------------|--------------|
| `services` | `SERVICE` | `service` | `service_pipeline` | `service_pipeline_stage` |
| `p50445259_meters` | `METERS` | `meters` | — | — |
| `contacts` | `CONTACT` | `contact` | — | — |
| `deals` | `DEAL` | `deal` | `deal_pipeline` | `deal_pipeline_stage` |

**Nota:** No se aplica singularización automática. El nombre viene directo del schema de HubSpot. Por ejemplo, `meters` queda `meters` (no `meter`) porque las asociaciones en HubSpot usan `SERVICE_TO_METERS`.

Las tablas de asociaciones también usan estos nombres: `service_contact`, `service_company`, `service_meters`, etc.

---

## Endpoints de HubSpot utilizados

El ETL consume 7 endpoints de la API de HubSpot (v3 y v4):

### 1. Propiedades del objeto

```
GET /crm/v3/properties/{objectType}
```

- **Archivo:** `etl/hubspot.py` → `get_properties_with_types()`
- **Propósito:** Obtiene la lista completa de propiedades con sus tipos de dato (string, number, datetime, etc.)
- **Se usa para:** Saber qué columnas crear en PostgreSQL y qué tipo de dato asignar a cada una
- **Respuesta clave:** `results[].name`, `results[].type`

### 2. Schema del objeto (asociaciones y nombre canónico)

```
GET /crm/v3/schemas/{objectType}
```

- **Archivo:** `etl/hubspot.py` → `get_associations()`, `get_schema_name()`
- **Propósito:** Obtiene el schema del objeto, incluyendo asociaciones definidas y el nombre canónico del objeto
- **Se usa para:**
  - Saber con qué otros objetos tiene relaciones (contacts, companies, deals, custom objects, etc.)
  - Resolver el nombre de tabla en PostgreSQL (ej: `services` → `service`)
  - Generar nombres legibles para tablas de asociaciones (ej: `service_contact`)
- **Respuesta clave:** `associations[].toObjectTypeId`, `name` (ej: `"SERVICE"`)

### 3. Pipelines y stages

```
GET /crm/v3/pipelines/{objectType}
```

- **Archivo:** `etl/hubspot.py` → `get_pipelines()`
- **Propósito:** Obtiene los pipelines del objeto con sus stages
- **Se usa para:** Cargar tablas `{tabla}_pipeline` y `{tabla}_pipeline_stage`, y para el smart mapping de columnas de pipeline
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

### 7. Registros archivados (detección de eliminados)

```
GET /crm/v3/objects/{objectType}?archived=true&limit=100
```

- **Archivo:** `etl/hubspot.py` → `fetch_archived_record_ids()`
- **Propósito:** Obtiene los registros que fueron archivados/eliminados en HubSpot
- **Paginación:** Usa `paging.next.after` para recorrer página por página
- **Filtrado:** Client-side por campo `archivedAt` > timestamp de última sincronización
- **Nota:** Funciona tanto para objetos estándar (contacts, deals) como para custom objects (services, meters)
- **Se usa en:** Carga incremental — los IDs archivados se marcan con `fivetran_deleted = true` en PostgreSQL

### Flujo de llamadas API

```
Full Load:
  GET  /properties/{type}        → obtener propiedades
  GET  /schemas/{type}           → obtener asociaciones + nombre de tabla
  GET  /pipelines/{type}         → obtener pipelines (opcional)
  GET  /objects/{type}?...       → descargar todos los registros (paginado)
  POST /v4/associations/.../batch/read  × N tipos de asociación (en paralelo)
  → Reconciliación: marcar IDs ausentes como fivetran_deleted=true

Incremental:
  GET  /properties/{type}        → obtener propiedades
  GET  /schemas/{type}           → obtener asociaciones + nombre de tabla
  GET  /pipelines/{type}         → obtener pipelines (opcional)
  POST /objects/{type}/search    → buscar registros modificados desde última sync
  GET  /objects/{type}?archived=true → detectar registros archivados recientes
  → Marcar archivados como fivetran_deleted=true
```

### Rate Limits

El ETL maneja automáticamente los rate limits de HubSpot:

| Status Code | Comportamiento |
|-------------|----------------|
| 200 / 207 | Respuesta exitosa |
| 400 | Error fatal — se loguea el detalle y se aborta |
| 414 | URL demasiado larga — demasiadas propiedades en GET |
| 429 | Rate limit — espera 10 segundos y reintenta (max 3 intentos) |
| 5xx | Error de servidor — espera progresiva y reintenta (max 3 intentos) |

---

## Deteccion de registros eliminados (fivetran_deleted)

El ETL detecta registros eliminados/archivados en HubSpot y los marca con `fivetran_deleted = true` en PostgreSQL (sin borrarlos fisicamente). La estrategia depende del modo de sincronizacion:

### Full Load

1. Durante el upsert, se acumulan todos los `hs_object_id` cargados en `_loaded_ids`
2. Al finalizar, `reconcile_deleted_records()` ejecuta:
   ```sql
   UPDATE schema.tabla
   SET fivetran_deleted = true, fivetran_synced = NOW()
   WHERE hs_object_id != ALL(array_de_ids_cargados)
     AND (fivetran_deleted IS DISTINCT FROM true)
   ```
3. Esto marca como eliminados los registros que existen en PostgreSQL pero no vinieron en el full load

### Incremental

1. Despues de cargar los registros modificados, se consulta el endpoint de archivados:
   `GET /crm/v3/objects/{type}?archived=true`
2. Se filtran client-side los registros cuyo `archivedAt` es posterior al ultimo sync
3. `mark_records_as_deleted(ids)` ejecuta:
   ```sql
   UPDATE schema.tabla
   SET fivetran_deleted = true, fivetran_synced = NOW()
   WHERE hs_object_id = ANY(array_de_ids_archivados)
     AND (fivetran_deleted IS DISTINCT FROM true)
   ```

### Pipelines y stages

Los pipelines y stages tambien respetan el campo `archived` de la API de HubSpot. Si un pipeline o stage esta archivado, se carga con `fivetran_deleted = true`.

---

## Ejecucion Local (CLI)

### Instalacion

```bash
# 1. Instalar dependencias
cd python-lambda/
pip install -r requirements.txt

# 2. Configurar variables de entorno
# Crear archivo .env en la RAIZ del proyecto (fuera de python-lambda/)
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
# Un solo objeto
python handler.py --object-type contacts

# Multiples objetos (separados por coma)
python handler.py --object-type services,p50445259_meters,projects

# Con nivel de log detallado
python handler.py --object-type deals --log-level DEBUG

# Guardar logs en archivo (se ven en pantalla Y se guardan)
python handler.py --object-type services --log-file etl_errors.log

# Forzar carga completa (ignorar sincronizacion incremental)
python handler.py --object-type contacts --full-load

# Combinar opciones: multiples objetos con full load y logs
python handler.py --object-type services,p50445259_meters --full-load --log-file etl.log
```

### Opciones CLI

| Opcion | Requerida | Default | Descripcion |
|--------|-----------|---------|-------------|
| `--object-type` | Si | - | Tipo(s) de objeto HubSpot, separados por coma (ej: `services,p50445259_meters,projects`) |
| `--log-level` | No | INFO | Nivel de logging (DEBUG, INFO, WARNING, ERROR, CRITICAL) |
| `--log-file` | No | - | Archivo para guardar logs. Si se proporciona, los logs se ven en pantalla Y se guardan |
| `--full-load` | No | false | Forzar carga completa ignorando sincronizacion incremental |

**Nota:** El parametro `--log-file` solo funciona en modo local (CLI). En Lambda, los logs se envian automaticamente a CloudWatch.

### Ejecucion multi-objeto

Cuando se ejecutan multiples objetos, cada uno se procesa secuencialmente con su propio ciclo ETL completo. Si un objeto falla, el error se captura y la ejecucion continua con el siguiente.

Al finalizar se imprime un resumen consolidado:

```
============================================================
  RESUMEN MULTI-OBJETO (3 objetos)
============================================================
  service                   : healthy      (176 registros, 34.63s)
  meters                    : healthy      (9643 registros, 221.14s)
  project                   : healthy      (45 registros, 18.2s)
============================================================
```

## Logs y Reportes

### Logs

**Modo Local (CLI):**
- Por defecto, los logs **solo aparecen en la terminal**
- Para guardarlos en archivo, usa la opcion `--log-file`:
  ```bash
  python handler.py --object-type contacts --log-file etl_errors.log
  ```
- Los logs se mostraran en pantalla Y se guardaran en el archivo especificado

**Modo Lambda:**
- Los logs se envian automaticamente a **Amazon CloudWatch Logs**
- Consulta: AWS Console → CloudWatch → Log Groups → `/aws/lambda/nombre-de-tu-funcion`

### Comportamiento de Logs por Modo

| Modo | Logs en Consola | Logs en Archivo | Destino Final |
|------|----------------|-----------------|---------------|
| **Local sin `--log-file`** | Si | No | Terminal |
| **Local con `--log-file`** | Si | Si | Terminal + Archivo especificado |
| **Lambda** | Si (a stdout) | No aplica | Amazon CloudWatch Logs |

### Reportes de Salud

Al finalizar la ejecucion, el ETL retorna un resumen JSON con metricas:

```json
{
  "object_type": "services",
  "status": "healthy",
  "duration_seconds": 34.63,
  "records_fetched": 176,
  "records_processed_ok": 176,
  "records_failed": 0,
  "db_upserts": 176,
  "db_insert_errors": 0,
  "api_calls": 12,
  "pipelines_loaded": 1,
  "stages_loaded": 4,
  "association_tables_created": 3,
  "associations_found": 520,
  "schema_changes": 0,
  "columns_truncated": 0,
  "records_deleted": 2,
  "sync_mode": "full"
}
```

| Campo | Descripcion |
|-------|-------------|
| `status` | `healthy` (sin errores) / `with_errors` (con errores parciales) |
| `sync_mode` | `full` (carga completa) / `incremental` (solo registros modificados) |
| `records_deleted` | Registros marcados como `fivetran_deleted = true` en esta ejecucion |

---

## Ejecucion como Lambda

### Evento de invocacion

**Un solo objeto (formato original):**

```json
{
  "object_type": "contacts",
  "log_level": "INFO",
  "max_workers": 3,
  "force_full_load": false
}
```

**Multiples objetos (formato nuevo):**

```json
{
  "object_types": ["services", "p50445259_meters", "projects"],
  "log_level": "INFO",
  "max_workers": 3,
  "force_full_load": false
}
```

Ambos formatos son compatibles. Si se envian `object_types` (lista), se procesan secuencialmente. Si se envia `object_type` (string), se procesa uno solo. El formato `object_type` sigue funcionando exactamente igual que antes.

### Variables de entorno (Lambda Configuration)

| Variable | Requerida | Default | Descripcion |
|----------|-----------|---------|-------------|
| ACCESS_TOKEN | Si | - | Token de acceso HubSpot |
| DB_HOST | Si | - | Host PostgreSQL |
| DB_PORT | No | 5432 | Puerto PostgreSQL |
| DB_NAME | Si | - | Nombre de la BD |
| DB_USER | Si | - | Usuario BD |
| DB_PASS | Si | - | Contrasena BD |
| DB_SCHEMA | No | hubspot_etl | Schema de destino |
| OBJECT_TYPE | No | contacts | Tipo de objeto por defecto (solo si no viene en el evento) |
| LOG_LEVEL | No | INFO | Nivel de logging |
| ETL_MAX_WORKERS | No | 3 | Workers paralelos para descarga de asociaciones |
| FORCE_FULL_LOAD | No | false | Forzar carga completa (ignora incremental) |

### Respuesta Lambda

**Un solo objeto:**

```json
{
  "statusCode": 200,
  "body": {
    "object_type": "services",
    "status": "healthy",
    "duration_seconds": 34.63,
    "records_fetched": 176,
    "records_processed_ok": 176,
    "records_failed": 0,
    "db_upserts": 176,
    "db_insert_errors": 0,
    "api_calls": 12,
    "pipelines_loaded": 1,
    "stages_loaded": 4,
    "association_tables_created": 3,
    "associations_found": 520,
    "schema_changes": 0,
    "columns_truncated": 0,
    "records_deleted": 0,
    "sync_mode": "incremental"
  }
}
```

**Multiples objetos:**

```json
{
  "statusCode": 200,
  "body": {
    "results": [
      {"object_type": "services", "status": "healthy", "records_processed_ok": 176, "...": "..."},
      {"object_type": "p50445259_meters", "status": "healthy", "records_processed_ok": 9643, "...": "..."}
    ],
    "errors": [],
    "total_objects": 2
  }
}
```

Si algun objeto falla, aparece en `errors` con detalle:

```json
{
  "errors": [
    {"object_type": "projects", "error": "ConnectionError", "detail": "timeout connecting to DB"}
  ]
}
```

### Codigos de estado

| Codigo | Significado |
|--------|-------------|
| 200 | ETL completado sin errores (todos los objetos healthy) |
| 207 | ETL completado con errores parciales (algun objeto con errores) |
| 400 | Error de configuracion |

---

## Concurrencia y seguridad

### Ejecuciones simultaneas

El ETL **no tiene proteccion interna contra ejecuciones concurrentes**. Si dos invocaciones procesan el mismo objeto simultaneamente, pueden ocurrir problemas:

| Escenario | Riesgo |
|-----------|--------|
| Objetos diferentes | Sin riesgo de datos (solo mas rate limiting en HubSpot API) |
| Mismo objeto, ambas incremental | Bajo — timestamp puede sobreescribirse (trabajo redundante) |
| Mismo objeto, full load | **Alto** — `reconcile_deleted_records` puede marcar falsos positivos |

### Recomendacion para produccion

Configurar `ReservedConcurrentExecutions: 1` en la Lambda para garantizar que solo una instancia ejecuta a la vez:

```yaml
# SAM / CloudFormation
MyETLFunction:
  Type: AWS::Serverless::Function
  Properties:
    ReservedConcurrentExecutions: 1
    Timeout: 900  # 15 min (maximo Lambda)
```

Con esta configuracion, si EventBridge intenta invocar mientras la Lambda esta corriendo, la invocacion se encola y se ejecuta cuando la instancia actual termine.

---

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

# Invocar (un objeto)
curl -X POST "http://localhost:9000/2015-03-31/functions/function/invocations" \
  -d '{"object_type": "contacts"}'

# Invocar (multiples objetos)
curl -X POST "http://localhost:9000/2015-03-31/functions/function/invocations" \
  -d '{"object_types": ["services", "p50445259_meters", "projects"]}'
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

### Opcion 1: Todos los objetos en una sola invocacion (recomendado)

```json
{
  "schedule": "cron(0 */4 * * ? *)",
  "targets": [
    {
      "input": "{\"object_types\": [\"services\", \"p50445259_meters\", \"projects\"]}",
      "arn": "arn:aws:lambda:<region>:<account>:function:hubspot-etl"
    }
  ]
}
```

### Opcion 2: Full load diario + incremental frecuente

```
Regla 1 (diario, 2 AM): cron(0 2 * * ? *)
  → {"object_types": ["services", "p50445259_meters", "projects"], "force_full_load": true}

Regla 2 (cada 4 horas): cron(0 */4 * * ? *)
  → {"object_types": ["services", "p50445259_meters", "projects"]}
```

**Importante:** Con `ReservedConcurrentExecutions: 1`, si la regla incremental se dispara mientras el full load aun esta corriendo, la invocacion se encola automaticamente.

---

## Tests

El proyecto incluye 48 tests unitarios que cubren todos los modulos:

```bash
# Ejecutar todos los tests
cd python-lambda/
python -m pytest tests/ -v

# Ejecutar un archivo especifico
python -m pytest tests/test_deleted_detection.py -v

# Ejecutar con cobertura (requiere pytest-cov)
python -m pytest tests/ --cov=etl --cov-report=term-missing
```

### Distribucion de tests

| Archivo | Tests | Cobertura |
|---------|-------|-----------|
| `test_hubspot.py` | Extraccion API, paginacion, asociaciones batch v4, search incremental |
| `test_transform.py` | Transformacion de registros, pipelines, asociaciones normalizadas |
| `test_database.py` | Upserts, schema evolution, flush de asociaciones, metadata sync |
| `test_monitor.py` | Metricas, reportes de salud, estadisticas de nulos |
| `test_config.py` | Validacion de configuracion, constructores desde env/lambda |
| `test_deleted_detection.py` | Reconciliacion full load, marcado incremental, fetch de archivados |

---

## Exit Codes (CLI)

| Codigo | Significado |
|--------|-------------|
| 0 | Exito total (todos los objetos healthy) |
| 1 | Error de ejecucion (al menos un objeto con errores) |
| 2 | Error de configuracion |
| 130 | Interrupcion manual (Ctrl+C) |

---

## Guia rapida: Casos de uso

### Desarrollo y pruebas locales

```bash
# 1. Primera vez: Probar con logs en pantalla
cd python-lambda/
python handler.py --object-type contacts

# 2. Debugging: Ver todos los detalles con archivo de log
python handler.py --object-type contacts --log-level DEBUG --log-file debug.log

# 3. Multiples objetos (produccion local)
python handler.py --object-type services,p50445259_meters,projects

# 4. Forzar carga completa
python handler.py --object-type contacts --full-load

# 5. Full load de multiples objetos con logs
python handler.py --object-type services,p50445259_meters --full-load --log-file etl.log
```

### Modos de sincronizacion

- **Primera ejecucion:** Siempre hace full load (no hay metadata previa)
- **Ejecuciones posteriores:** Automaticamente usa carga incremental (Search API, solo registros modificados)
- **Forzar full load:** Usa `--full-load` o la variable `FORCE_FULL_LOAD=true`
- **Fallback automatico:** Si hay >10,000 cambios desde la ultima sync, cambia a full load

### Deteccion de eliminados por modo

| Modo | Estrategia | Metodo |
|------|-----------|--------|
| Full load | Reconciliacion: IDs en PG que no vinieron se marcan como eliminados | `reconcile_deleted_records()` |
| Incremental | Consulta registros archivados recientes via API | `fetch_archived_record_ids()` → `mark_records_as_deleted()` |

### Verificar que el archivo `.env` esta configurado

Tu archivo `.env` debe estar en la **raiz del proyecto** (un nivel arriba de `python-lambda/`):

```
test_api_hubspot/
├── .env                    ← Aqui
└── python-lambda/
    └── handler.py          ← Ejecutas desde aqui
```

Contenido minimo de `.env`:
```env
ACCESS_TOKEN=tu_token_de_hubspot
DB_HOST=localhost
DB_PORT=5432
DB_NAME=nombre_bd
DB_USER=usuario
DB_PASS=contrasena
```

### Troubleshooting

**Problema:** No se encuentran las variables de entorno
```bash
# Solucion: Asegurate de que .env esta en la raiz
ls ../.env  # Debe existir

# O ejecuta con variables inline
ACCESS_TOKEN=xxx DB_HOST=localhost ... python handler.py --object-type contacts
```

**Problema:** No se guarda el archivo de log
```bash
# Solucion: Verifica que la carpeta exista
mkdir -p logs
python handler.py --object-type contacts --log-file logs/contacts.log
```

**Problema:** Error "module 'etl' not found"
```bash
# Solucion: Ejecuta desde python-lambda/, no desde la raiz
cd python-lambda/
python handler.py --object-type contacts
```
