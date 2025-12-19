# src/seed_stations_manual.py
import pandas as pd
from pathlib import Path
import os

# --- CONFIGURACIÓN ---
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_RAW = BASE_DIR / "data" / "raw"
os.makedirs(DATA_RAW, exist_ok=True)
OUTPUT_FILE = DATA_RAW / "aforos_meta_raw.csv"

def generar_datos_manuales():
    print("🌱 Sembrando datos manuales CURADOS desde el Visor Web...")

    # NOTA: He cambiado las comas (,) por puntos (.) en todas las coordenadas.
    datos = [
        {
            "idEstacion": 140490, 
            "nomeEstacion": "o_con", 
            "rio": "rio do con",
            "lat": 42.5925, 
            "lon": -8.7627,
            "concello": "Vilagarcia de arousa"
        },
        {
            "idEstacion": 140445, 
            "nomeEstacion": "bermana_umia", 
            "rio": "bermana",
            "lat": 42.6038, 
            "lon": -8.64592,
            "concello": "Vilagarcia de arousa"
        },
        {
            "idEstacion": 140440, 
            "nomeEstacion": "umia_caldas", 
            "rio": "Umia",
            "lat": 42.6029, 
            "lon": -8.64249,
            "concello": "Caldas de Reis"
        },
        {
            "idEstacion": 140470, 
            "nomeEstacion": "Baixo_umia", 
            "rio": "Umia",
            "lat": 42.5154, 
            "lon": -8.76556,
            "concello": "Ribadumia"
        },
        {
            "idEstacion": 140545, 
            "nomeEstacion": "ulla_padron",
            "rio": "ulla",
            "lat": 42.7313,
            "lon": -8.62795,
            "concello": "Padron"
        },
        {
            "idEstacion": 140570, 
            "nomeEstacion": "sar_padron",
            "rio": "sar",
            "lat": 42.7457,
            "lon": -8.65923,
            "concello": "Padron"
        },
        {
            "idEstacion": 140540, # Asigné un ID provisorio distinto
            "nomeEstacion": "ulla_teo",
            "rio": "ulla",
            "lat": 42.7595,
            "lon": -8.54767,
            "concello": "teo"
        },
        {
            "idEstacion": 140560,
            "nomeEstacion": "sar_ames",
            "rio": "sar",
            "lat": 42.8220,
            "lon": -8.65198,
            "concello": "bertamirans"
        },
        {
            "idEstacion": 140555,
            "nomeEstacion": "sar_bertamirans",
            "rio": "sar",
            "lat": 42.8564,
            "lon": -8.64814,
            "concello": "bertamirans"
        },
        {
            "idEstacion": 140548,
            "nomeEstacion": "sar_santiago",
            "rio": "sar",
            "lat": 42.8770,
            "lon": -8.52871,
            "concello": "santiago"
        },
        {
            "idEstacion": 140530,
            "nomeEstacion": "deza",
            "rio": "deza",
            "lat": 42.7771,
            "lon": -8.33756,
            "concello": "touro"
        },
        {
            "idEstacion": 140520,
            "nomeEstacion": "ulla_touro",
            "rio": "ulla",
            "lat": 42.8241,
            "lon": -8.27212,
            "concello": "touro"
        }
    ]

    df = pd.DataFrame(datos)
    
    # Guardamos el CSV
    df.to_csv(OUTPUT_FILE, sep=';', encoding='utf-8-sig', index=False)
    
    print("✅ Archivo 'aforos_meta_raw.csv' generado exitosamente con PUNTOS decimales.")
    print(df[['nomeEstacion', 'lat', 'lon']].head())
    print(f"\n📂 Ubicación: {OUTPUT_FILE}")
    print("👉 Ahora vuelve a ejecutar 'src/visualize_coverage.py'")

if __name__ == "__main__":
    generar_datos_manuales()