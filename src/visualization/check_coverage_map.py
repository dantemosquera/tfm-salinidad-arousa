import xarray as xr
import matplotlib.pyplot as plt
import os
from pathlib import Path
import glob

# ==========================================
# CONFIGURACIÓN
# ==========================================
DATA_DIR = Path("data/raw/b3/wrf_prec")
AROUSA_COORDS = {'lat': 42.50, 'lon': -8.90}

def main():
    print(f"Buscando archivos NetCDF en: {DATA_DIR}")
    
    # Busca recursivamente cualquier archivo .nc
    files = list(DATA_DIR.rglob("*.nc"))
    
    if not files:
        print("Error: No hay archivos .nc descargados en la carpeta de datos.")
        return

    # Tomamos el último archivo encontrado (suele ser el más reciente)
    file_path = files[-1]
    print(f"Analizando archivo de muestra: {file_path.name}")

    try:
        ds = xr.open_dataset(file_path)
        
        # Validación Numérica
        lats = ds['lat'].values
        lons = ds['lon'].values
        
        print("-" * 40)
        print(f"Límites Latitud:  {lats.min():.4f} <-> {lats.max():.4f}")
        print(f"Límites Longitud: {lons.min():.4f} <-> {lons.max():.4f}")
        print("-" * 40)
        
        in_lat = lats.min() < AROUSA_COORDS['lat'] < lats.max()
        in_lon = lons.min() < AROUSA_COORDS['lon'] < lons.max()
        
        if in_lat and in_lon:
            print("ÉXITO: La Ría de Arousa está dentro del grid.")
        else:
            print("ALERTA CRÍTICA: La zona de estudio está FUERA del archivo.")

        # Visualización
        print("Generando mapa de comprobación...")
        plt.figure(figsize=(10, 8))
        
        # Ploteamos la suma total de lluvia del archivo para que se vea mejor el mapa
        if 'prec' in ds:
            total_prec = ds['prec'].sum(dim='time')
            total_prec.plot(x='lon', y='lat', cmap='Blues', cbar_kwargs={'label': 'Lluvia Total (mm)'})
        
        plt.plot(AROUSA_COORDS['lon'], AROUSA_COORDS['lat'], 'ro', markersize=10, label='Ría de Arousa')
        plt.legend()
        plt.title(f"Cobertura WRF: {file_path.name}")
        plt.grid(True, linestyle='--', alpha=0.3)
        
        output_img = "check_coverage.png"
        plt.savefig(output_img)
        print(f"Mapa guardado: {output_img}")

    except Exception as e:
        print(f"Error procesando el archivo: {e}")

if __name__ == "__main__":
    main()