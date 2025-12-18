# src/filter_rivers.py
import os
import sys
from pathlib import Path

# --- 🚑 ARREGLO DE EMERGENCIA PARA PROJ (WINDOWS) ---
# Esto debe ir ANTES de importar geopandas
try:
    # Detectamos dónde está instalado tu entorno Conda
    conda_prefix = sys.prefix
    
    # Rutas posibles donde Conda guarda los datos de proyección en Windows
    # Usualmente es: .../Library/share/proj
    proj_lib = Path(conda_prefix) / "Library" / "share" / "proj"
    
    # Forzamos la variable de entorno
    os.environ["PROJ_LIB"] = str(proj_lib)
    print(f"🔧 PROJ_LIB forzada a: {proj_lib}")
except Exception as e:
    print(f"⚠️ No pude aplicar el parche de PROJ: {e}")

# Ahora sí importamos las librerías geoespaciales
import geopandas as gpd
import matplotlib.pyplot as plt

# --- CONFIGURACIÓN ---
BASE_DIR = Path(__file__).resolve().parent.parent
# Apuntamos a la carpeta donde moviste todo
DATA_RAW = BASE_DIR / "data" / "raw" / "hidrografia"
DATA_PROCESSED = BASE_DIR / "data" / "processed"

# Aseguramos que la carpeta de salida exista
os.makedirs(DATA_PROCESSED, exist_ok=True)

def procesar_rios():
    print("🔍 Buscando archivo de ríos en:", DATA_RAW)
    
    archivos_rios = list(DATA_RAW.rglob("*rios*.shp"))
    
    if not archivos_rios:
        print(f"❌ Error: No encontré ningún archivo que contenga 'rios' y termine en '.shp' dentro de {DATA_RAW}")
        return

    input_file = archivos_rios[0]
    output_file = DATA_PROCESSED / "red_fluvial_arousa.geojson"
    
    print(f"🗺️  Procesando archivo: {input_file.name}")
    
    try:
        # Carga del Shapefile
        gdf = gpd.read_file(input_file)
        print(f"✅ Cargado. Geometrías totales: {len(gdf)}")
        
        # 2. Detección automática de columna de nombre
        col_nombre = None
        # He añadido 'nombre' en minúsculas para que te lo detecte solo
        candidatos = ['NOME', 'NOMBRE', 'nombre', 'RIO', 'TEXTO']
        for c in candidatos:
            if c in gdf.columns:
                col_nombre = c
                break
        
        if not col_nombre:
            print(f"⚠️ No detecté columna de nombre. Columnas disponibles: {gdf.columns.tolist()}")
            col_nombre = input("👉 Escribe el nombre de la columna manualmente: ")
        else:
            print(f"🎯 Filtrando por columna detectada: '{col_nombre}'")

        # 3. Filtrado por Cuenca (Ulla, Umia, Sar)
        keywords = ['ULLA', 'UMIA', 'SAR']
        filtro = gdf[col_nombre].astype(str).str.upper().apply(lambda x: any(k in x for k in keywords))
        gdf_filtered = gdf[filtro].copy()
        
        print(f"💧 Tramos seleccionados: {len(gdf_filtered)} de {len(gdf)}")

        if gdf_filtered.empty:
            print("⚠️ El filtro devolvió 0 ríos. Revisa los keywords.")
            return

        # 4. Transformación de Coordenadas (CRÍTICO: Aquí fallaba antes)
        if gdf_filtered.crs != "EPSG:4326":
            print("🌐 Intentando convertir coordenadas a Lat/Lon (WGS84)...")
            # Forzamos la regeneración del objeto CRS para asegurar que use el PROJ nuevo
            gdf_filtered = gdf_filtered.to_crs("EPSG:4326")

        # 5. Guardado
        gdf_filtered.to_file(output_file, driver='GeoJSON')
        print(f"💾 Archivo limpio guardado en: {output_file}")

        # 6. Visualización
        print("🖼️  Generando mapa...")
        fig, ax = plt.subplots(figsize=(10, 8))
        gdf_filtered.plot(ax=ax, color='blue', linewidth=1.5)
        ax.set_title(f"Red Fluvial Filtrada (Ulla-Umia)\nFuente: {input_file.name}")
        ax.set_axis_off()
        plt.show()

    except Exception as e:
        print(f"❌ Error crítico: {e}")
        print("\n💡 PISTA: Si sigue fallando por PROJ, intenta reinstalar pyproj:")
        print("conda install -c conda-forge pyproj --force-reinstall")

if __name__ == "__main__":
    procesar_rios()