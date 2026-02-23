"""Fixtures compartidos para todos los tests del ETL."""
import pytest

from etl.config import ETLConfig
from etl.monitor import ETLMonitor


@pytest.fixture
def fake_config():
    """ETLConfig con valores dummy (no conecta a nada real)."""
    return ETLConfig(
        object_type="contacts",
        access_token="pat-fake-token-12345",
        db_host="localhost",
        db_port="5432",
        db_name="test_db",
        db_user="test_user",
        db_pass="test_pass",
        db_schema="hubspot_etl",
        log_level="INFO",
    )


@pytest.fixture
def monitor():
    """ETLMonitor fresco para cada test."""
    return ETLMonitor(
        object_type="contacts",
        db_schema="hubspot_etl",
        table_name="contacts",
    )


@pytest.fixture
def sample_hubspot_records():
    """5 registros fake de HubSpot con propiedades y asociaciones."""
    return [
        {
            "id": "101",
            "properties": {
                "firstname": "Juan",
                "lastname": "Pérez",
                "email": "juan@example.com",
                "hs_lastmodifieddate": "2024-06-01T10:00:00Z",
            },
            "associations": {
                "companies": {
                    "results": [
                        {"id": "201", "type": "1", "category": "HUBSPOT_DEFINED"},
                    ]
                },
                "deals": {
                    "results": [
                        {"id": "301", "type": "3", "category": "HUBSPOT_DEFINED"},
                    ]
                },
            },
            "archived": False,
        },
        {
            "id": "102",
            "properties": {
                "firstname": "María",
                "lastname": "García",
                "email": "maria@example.com",
                "hs_lastmodifieddate": "2024-06-02T10:00:00Z",
            },
            "associations": {
                "companies": {
                    "results": [
                        {"id": "202", "type": "1", "category": "HUBSPOT_DEFINED"},
                    ]
                },
            },
            "archived": False,
        },
        {
            "id": "103",
            "properties": {
                "firstname": "Carlos",
                "lastname": "López",
                "email": "carlos@example.com",
                "hs_lastmodifieddate": "2024-06-03T10:00:00Z",
            },
            "associations": {},
            "archived": False,
        },
        {
            "id": "104",
            "properties": {
                "firstname": "Ana",
                "lastname": "Martínez",
                "email": "ana@example.com",
                "hs_lastmodifieddate": "2024-06-04T10:00:00Z",
            },
            "associations": {
                "companies": {
                    "results": [
                        {"id": "201", "type": "1", "category": "HUBSPOT_DEFINED"},
                        {"id": "203", "type": "1", "category": "HUBSPOT_DEFINED"},
                    ]
                },
            },
            "archived": False,
        },
        {
            "id": "105",
            "properties": {
                "firstname": "Pedro",
                "lastname": "Sánchez",
                "email": "pedro@example.com",
                "hs_lastmodifieddate": "2024-06-05T10:00:00Z",
            },
            "associations": {
                "deals": {
                    "results": [
                        {"id": "302", "type": "3", "category": "HUBSPOT_DEFINED"},
                    ]
                },
            },
            "archived": True,
        },
    ]


@pytest.fixture
def sample_prop_types():
    """Mapa de propiedades con sus tipos de HubSpot."""
    return {
        "firstname": "string",
        "lastname": "string",
        "email": "string",
        "hs_lastmodifieddate": "datetime",
        "phone": "phone_number",
        "num_employees": "number",
        "hs_lead_status": "enumeration",
    }
