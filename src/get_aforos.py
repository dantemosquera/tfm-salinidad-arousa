# src/get_aforos.py
import requests
import pandas as pd
import os
from pathlib import Path

# --- CONFIGURACIÓN ---
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_RAW_DIR = BASE_DIR / "data" / "raw"
os.makedirs(DATA_RAW_DIR, exist_ok=True)

# CAMBIO DE ESTRATEGIA: Usamos el endpoint de "Últimos Datos" que es más estable
# Este JSON trae la lectura actual de todas las estaciones activas
URL_AFOROS_LIVE = "https://servizos.meteogalicia.gal/mgrss/observacion/ultimoAforos.action"

def descargar_lista_aforos():
    print(f"📡 Conectando a {URL_AFOROS_LIVE}...")
    
    try:
        response = requests.get(URL_AFOROS_LIVE)
        response.raise_for_status()
        data = response.json()
        
        # En este endpoint, la lista suele llamarse 'listaAforos' o 'listUltimosAforos'
        # MeteoGalicia a veces cambia las claves, así que inspeccionamos:
        clave_lista = 'listaAforos' # Valor por defecto habitual
        if 'listUltimosAforos' in data:
            clave_lista = 'listUltimosAforos'
            
        estaciones = data.get(clave_lista, [])
        
        if not estaciones:
            print("⚠️ El JSON descargó pero la lista está vacía. Revisa la estructura.")
            print(f"Claves encontradas: {data.keys()}")
            return

        # Convertimos a DataFrame
        df = pd.DataFrame(estaciones)
        
        print(f"✅ Descargados datos de {len(df)} estaciones activas.")
        
        # --- LIMPIEZA DE COLUMNAS ---
        # Este endpoint trae datos mezclados con metadatos.
        # Seleccionamos solo lo que nos importa para identificar la estación.
        # Buscamos columnas probables (a veces vienen como 'estacion', 'identificador', etc)
        cols_deseadas = ['idEstacion', 'nomeEstacion', 'lat', 'lon', 'concello', 'provincia']
        
        # Filtramos solo las que existan en el JSON recibido
        cols_finales = [c for c in cols_deseadas if c in df.columns]
        df_meta = df[cols_finales].drop_duplicates()

        # --- FILTRADO TFM (Ría de Arousa) ---
        # Filtramos por ríos Ulla, Umia, Sar o concellos clave
        keywords = 'Ulla|Umia|Sar|Teo|Padron|Caldas|Catoira|Valga'
        filtro = df_meta['nomeEstacion'].str.contains(keywords, case=False, na=False) | \
                 df_meta['concello'].str.contains(keywords, case=False, na=False)
        
        df_arousa = df_meta[filtro].copy()
        
        print(f"💧 Estaciones detectadas en cuenca Arousa (Ulla/Umia): {len(df_arousa)}")
        
        if not df_arousa.empty:
            print(df_arousa[['idEstacion', 'nomeEstacion']].to_string(index=False))
            
            # Guardamos
            output_path = DATA_RAW_DIR / "aforos_meta_raw.csv"
            df_arousa.to_csv(output_path, index=False)
            print(f"\n💾 ¡Éxito! Archivo guardado en: {output_path}")
        else:
            print("⚠️ No se encontraron estaciones que coincidan con el filtro. Revisa los nombres en el print completo.")
            # Si falla el filtro, guardamos todo para que puedas inspeccionar manualmente
            df_meta.to_csv(DATA_RAW_DIR / "aforos_TODAS_debug.csv", index=False)
            print("Se guardó 'aforos_TODAS_debug.csv' con todas las estaciones para revisar nombres.")

    except Exception as e:
        print(f"❌ Error crítico: {e}")

if __name__ == "__main__":
    descargar_lista_aforos()