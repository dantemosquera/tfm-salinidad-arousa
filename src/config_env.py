import os
import pyproj
import sys
from dotenv import load_dotenv

def inicializar_entorno():
    """
    Configura las variables de entorno para GDAL, PROJ y carga secretos.
    Debe ejecutarse al principio de cualquier notebook.
    """
    # 1. Parche DLL para Windows (evita muerte del Kernel)
    os.environ["KMP_DUPLICATE_LIB_OK"] = "TRUE"

    # 2. Configurar PROJ (Ruta fija de tu entorno Conda)
    # Nota: Si mueves el proyecto a otro PC, solo cambias esto aquí.
    ruta_proj = r"c:\Users\mosqu\.conda\envs\tfm_env\Library\share\proj"
    pyproj.datadir.set_data_dir(ruta_proj)

    # 3. Cargar .env
    # Buscamos el .env subiendo un nivel si estamos en src o notebooks
    # load_dotenv() busca automáticamente, pero a veces es bueno ser explícito
    if load_dotenv():
        print("✅ Variables de entorno (.env) cargadas.")
    else:
        print("⚠️ Advertencia: No se encontró el archivo .env")

    print(f"✅ Entorno configurado correctamente (PROJ: {ruta_proj})")