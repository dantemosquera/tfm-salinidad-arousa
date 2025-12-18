# src/visualize_coverage.py
import os
import sys
from pathlib import Path

# --- 1. ARREGLO PROJ (Solo Windows) ---
try:
    if os.name == 'nt': 
        conda_prefix = sys.prefix
        proj_lib = Path(conda_prefix) / "Library" / "share" / "proj"
        os.environ["PROJ_LIB"] = str(proj_lib)
except Exception:
    pass

# --- 2. IMPORTACIONES ---
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import contextily as ctx  # <--- AQUÍ la importamos
from shapely.geometry import box

# --- 3. CONFIGURACIÓN ---
BASE_DIR = Path(__file__).resolve().parent.parent
RIVERS_FILE = BASE_DIR / "data" / "processed" / "red_fluvial_arousa.geojson"
STATIONS_FILE = BASE_DIR / "data" / "raw" / "aforos_meta_raw.csv"

def validar_cobertura():
    print("🗺️  Cargando capas...")

    # Cargar Ríos
    if not RIVERS_FILE.exists():
        print(f"❌ Falta archivo: {RIVERS_FILE}")
        return
    gdf_rios = gpd.read_file(RIVERS_FILE)

    # Cargar Estaciones
    gdf_estaciones = None
    if STATIONS_FILE.exists():
        try:
            # Leemos con punto y coma
            df_st = pd.read_csv(STATIONS_FILE, sep=';', encoding='utf-8-sig')
            
            if 'lon' in df_st.columns and 'lat' in df_st.columns:
                gdf_estaciones = gpd.GeoDataFrame(
                    df_st, 
                    geometry=gpd.points_from_xy(df_st.lon, df_st.lat),
                    crs="EPSG:4326" # Original: GPS (Lat/Lon)
                )
                print(f"✅ Estaciones cargadas: {len(gdf_estaciones)}")
            else:
                print("⚠️ CSV sin columnas lat/lon")
        except Exception as e:
            print(f"⚠️ Error CSV: {e}")

    # --- PREPARACIÓN PARA MAPA WEB (MERCATOR) ---
    # Para superponer con Google Maps/OSM, todo debe estar en EPSG:3857
    print("🌍 Reproyectando a Web Mercator...")
    
    # 1. Definir caja (en Lat/Lon primero) y convertirla
    bbox_geo = box(-9.0, 42.45, -8.0, 42.90) # Ajustado a Ría de Arousa
    gdf_rios_clip = gdf_rios.clip(bbox_geo)
    
    # 2. Convertir geometrías a Metros (Web Mercator)
    gdf_rios_web = gdf_rios_clip.to_crs(epsg=3857)
    
    if gdf_estaciones is not None:
        gdf_est_web = gdf_estaciones.to_crs(epsg=3857)
        # Filtramos las que caen dentro de la caja visual
        # (Truco: usaremos los límites de los ríos para el zoom)

    # --- PLOTEO ---
    fig, ax = plt.subplots(figsize=(12, 12))

    # Capa 1: Ríos
    gdf_rios_web.plot(ax=ax, color='blue', linewidth=2, alpha=0.6, label='Ríos')

    # Capa 2: Estaciones
    if gdf_estaciones is not None:
        gdf_est_web.plot(ax=ax, color='red', markersize=100, edgecolors='white', zorder=5)
        
        # Etiquetas
        for x, y, label in zip(gdf_est_web.geometry.x, gdf_est_web.geometry.y, gdf_est_web['nomeEstacion']):
            ax.text(x + 500, y, str(label), fontsize=9, fontweight='bold',
                    bbox=dict(facecolor='white', alpha=0.7, edgecolor='none'))

    # Capa 3: Mapa Base (AQUÍ USAMOS ctx)
    print("🎨 Añadiendo mapa base...")
    try:
        # ctx.add_basemap descarga los tiles de internet y los pone en el fondo
        ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik)
    except Exception as e:
        print(f"⚠️ No se pudo cargar el mapa base: {e}")

    ax.set_axis_off()
    ax.set_title("Validación: Estaciones sobre Terreno Real")

    output_img = BASE_DIR / "docs" / "mapa_cobertura_final.png"
    plt.savefig(output_img, dpi=100, bbox_inches='tight')
    print(f"🖼️  Guardado en: {output_img}")
    plt.show()

if __name__ == "__main__":
    validar_cobertura()