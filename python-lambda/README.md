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

## Ejecución Local (CLI)

```bash
# 1. Instalar dependencias
pip install -r requirements.txt

# 2. Configurar variables de entorno (archivo .env en la raíz)
#    ACCESS_TOKEN, DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS

# 3. Ejecutar
python handler.py --object-type contacts
python handler.py --object-type deals --log-level DEBUG
python handler.py --object-type services
python handler.py --object-type companies
```

## Ejecución como Lambda

### Evento de invocación

```json
{
  "object_type": "contacts",
  "log_level": "INFO"
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
    "columns_truncated": 0
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
