# src/create_db_schema.py
import os
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# --- CONFIGURACIÓN SEGURA ---

# 1. Localizamos el archivo .env
# Como este script está en /src, el .env está una carpeta arriba (en la raíz)
BASE_DIR = Path(__file__).resolve().parent.parent
ENV_PATH = BASE_DIR / ".env"

# 2. Cargamos las variables
# override=True fuerza la recarga por si cambiaste algo en el archivo recientemente
load_dotenv(dotenv_path=ENV_PATH, override=True)

# 3. Leemos las variables (Si no existen, devolverán None)
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432") # 5432 por defecto si no está definido

def crear_tablas():
    print("🔐 Verificando credenciales...")
    
    # Check de seguridad: si falta alguna variable clave, paramos
    if not all([DB_NAME, DB_USER, DB_PASS, DB_HOST]):
        print(f"❌ Error: No se encontraron todas las variables en {ENV_PATH}")
        print("Revisa tu archivo .env. Necesitas: DB_NAME, DB_USER, DB_PASS, DB_HOST")
        return

    print(f"🔌 Conectando a PostgreSQL en {DB_HOST} (Base: {DB_NAME})...")

    # Construimos la cadena de conexión de forma segura
    connection_str = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    
    try:
        engine = create_engine(connection_str)
        
        # Sentencias SQL DDL
        queries = [
            """
            CREATE TABLE IF NOT EXISTS aforos_meta (
                id_estacion INT PRIMARY KEY,
                nombre VARCHAR(100),
                rio VARCHAR(100),
                lat FLOAT,
                lon FLOAT,
                concello VARCHAR(100),
                provincia VARCHAR(50)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS aforos_data (
                fecha TIMESTAMP,
                id_estacion INT,
                caudal FLOAT,  
                nivel FLOAT,   
                estado VARCHAR(20),
                PRIMARY KEY (fecha, id_estacion),
                FOREIGN KEY (id_estacion) REFERENCES aforos_meta(id_estacion)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS meteo_meta (
                id_estacion INT PRIMARY KEY,
                nombre VARCHAR(100),
                lat FLOAT,
                lon FLOAT,
                altitud FLOAT
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS meteo_data (
                fecha TIMESTAMP,
                id_estacion INT,
                precipitacion FLOAT,
                temperatura FLOAT,
                PRIMARY KEY (fecha, id_estacion),
                FOREIGN KEY (id_estacion) REFERENCES meteo_meta(id_estacion)
            );
            """
        ]

        with engine.connect() as conn:
            print("🏗️  Construyendo esquema...")
            for q in queries:
                conn.execute(text(q))
                conn.commit()
            print("✅ ¡Tablas creadas exitosamente usando credenciales seguras!")
            
    except Exception as e:
        print(f"❌ Error conectando a la base de datos: {e}")
        print("Pista: Verifica que la contraseña en .env sea correcta.")

if __name__ == "__main__":
    crear_tablas()