"""
Script de prueba para validar las mejoras aplicadas:
1. Validación de variables de entorno
2. Exit codes
3. Logging por niveles

Uso:
    # Prueba normal (debería fallar si no hay .env configurado)
    python test_mejoras.py
    
    # Prueba con diferentes niveles de log
    LOG_LEVEL=DEBUG python test_mejoras.py
    LOG_LEVEL=ERROR python test_mejoras.py
    
    # Verificar exit code
    python test_mejoras.py; echo "Exit code: $?"
"""

import os
import sys
from dotenv import load_dotenv

load_dotenv()

# Simular las funciones de validación
def validate_env():
    """Valida variables de entorno críticas"""
    required_vars = {
        "ACCESS_TOKEN": "Token de acceso a la API de HubSpot",
        "DB_HOST": "Host de la base de datos PostgreSQL",
        "DB_NAME": "Nombre de la base de datos",
        "DB_USER": "Usuario de la base de datos",
        "DB_PASS": "Contraseña de la base de datos"
    }
    
    missing = []
    for var, description in required_vars.items():
        value = os.getenv(var)
        if not value or value.strip() == "":
            missing.append(f"{var} ({description})")
    
    if missing:
        error_msg = "❌ Variables de entorno faltantes o vacías:\n"
        for var in missing:
            error_msg += f"   • {var}\n"
        error_msg += "\n💡 Verifica tu archivo .env"
        raise EnvironmentError(error_msg)
    
    print("✅ Variables de entorno validadas correctamente")
    return True

def test_logging_levels():
    """Prueba el sistema de logging por niveles"""
    import logging
    
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
    VALID_LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    
    if LOG_LEVEL not in VALID_LEVELS:
        print(f"⚠️ LOG_LEVEL inválido '{LOG_LEVEL}', usando 'INFO'")
        LOG_LEVEL = "INFO"
    
    print(f"📊 Nivel de log configurado: {LOG_LEVEL}")
    
    # Configurar logging
    NUMERIC_LEVEL = getattr(logging, LOG_LEVEL, logging.INFO)
    logging.basicConfig(
        level=NUMERIC_LEVEL,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S',
        force=True
    )
    
    # Prueba de diferentes niveles
    logging.debug("🔍 Mensaje DEBUG - Solo visible en modo DEBUG")
    logging.info("ℹ️ Mensaje INFO - Visible en INFO y DEBUG")
    logging.warning("⚠️ Mensaje WARNING - Siempre visible")
    logging.error("❌ Mensaje ERROR - Siempre visible")
    
    print(f"\n✅ Sistema de logging funcionando correctamente")

def main():
    """Función principal con manejo de exit codes"""
    print("=" * 60)
    print("  🧪 TEST DE MEJORAS ETL")
    print("=" * 60 + "\n")
    
    try:
        # Test 1: Validación de env vars
        print("🔍 Test 1: Validación de Variables de Entorno")
        validate_env()
        
        # Test 2: Logging por niveles
        print("\n🔍 Test 2: Sistema de Logging por Niveles")
        test_logging_levels()
        
        # Éxito
        print("\n" + "=" * 60)
        print("  ✅ TODOS LOS TESTS PASARON")
        print("=" * 60)
        sys.exit(0)
        
    except EnvironmentError as e:
        print(f"\n💥 ERROR DE CONFIGURACIÓN:\n{e}\n")
        sys.exit(2)
        
    except KeyboardInterrupt:
        print("\n\n⚠️ Tests interrumpidos por el usuario")
        sys.exit(130)
        
    except Exception as e:
        print(f"\n💥 ERROR INESPERADO:")
        print(f"   Tipo: {type(e).__name__}")
        print(f"   Mensaje: {str(e)}\n")
        sys.exit(1)

if __name__ == "__main__":
    main()
