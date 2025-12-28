
import os
import logging
from pathlib import Path
from datetime import date, timedelta
from dataclasses import dataclass
from typing import Generator, Optional
import time
import signal
import sys
import xarray as xr
import requests
from tqdm import tqdm
from logging.handlers import RotatingFileHandler

# ============================================================================
# CONFIGURACIÓN
# ============================================================================
@dataclass
class Config:
    """Configuración centralizada con valores por defecto y validación"""
    START_DATE: date = date(2021, 9, 1)
    END_DATE: date = date.today()
    OUTPUT_DIR: Path = Path(os.getenv("OUTPUT_DIR", "data/raw/b3/wrf_prec"))
    BASE_URL: str = "https://mandeo.meteogalicia.es/thredds/dodsC/modelos/WRF_ARW_1KM_HIST_Novo"
    
    MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "3"))
    RETRY_BASE_DELAY: float = 2.0  # Segundos (con backoff exponencial)
    TIMEOUT_SECONDS: int = 60
    REQUEST_DELAY: float = 0.5  # Pausa entre descargas (Bajado a 0.5 para ir un poco más rápido)
    MIN_FILE_SIZE: int = 1000  # Bytes mínimos para considerar válido
    
    def __post_init__(self):
        """Validaciones post-inicialización"""
        if self.START_DATE > self.END_DATE:
            raise ValueError(f"START_DATE ({self.START_DATE}) debe ser anterior a END_DATE ({self.END_DATE})")
        
        self.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        
        if self.MAX_RETRIES < 1:
            raise ValueError("MAX_RETRIES debe ser al menos 1")

# ============================================================================
# LOGGING
# ============================================================================
def setup_logging(log_file: str = "descarga_wrf.log") -> logging.Logger:
    """Configura sistema de logging con rotación automática."""
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    
    # Evitar duplicados si se ejecuta múltiples veces
    if logger.handlers:
        return logger
    
    # Handler para archivo (con rotación: máx 10MB, 5 backups)
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(funcName)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    ))
    
    # Handler para consola (solo WARNING y superior para no ensuciar la barra tqdm)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.WARNING) 
    console_handler.setFormatter(logging.Formatter(
        '%(levelname)s: %(message)s'
    ))
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

# ============================================================================
# UTILIDADES
# ============================================================================
def date_range(start: date, end: date) -> Generator[date, None, None]:
    """Genera todas las fechas entre start y end (ambos inclusivos)."""
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)

def validate_netcdf_file(filepath: Path, logger: logging.Logger) -> bool:
    """Valida que un archivo NetCDF sea legible y contenga la variable 'prec'."""
    try:
        # Abrimos sin decodificar tiempos para ser rápidos en la validación
        with xr.open_dataset(filepath, decode_times=False, engine='netcdf4') as ds:
            if 'prec' not in ds:
                logger.warning(f" {filepath.name}: falta variable 'prec'")
                return False
            
            if ds['prec'].size == 0:
                logger.warning(f" {filepath.name}: variable 'prec' vacía")
                return False
            
            return True
            
    except Exception as e:
        logger.warning(f" {filepath.name}: corrupto o ilegible ({type(e).__name__})")
        return False

def check_remote_file_exists(url: str, timeout: int, logger: logging.Logger) -> Optional[bool]:
    """Verifica si un archivo remoto existe sin descargarlo completamente."""
    # Truco: Cambiamos dodsC (OPeNDAP) por fileServer (HTTP directo) para hacer HEAD request
    check_url = url.replace("dodsC", "fileServer")
    
    try:
        response = requests.head(
            check_url,
            timeout=timeout,
            allow_redirects=True
        )
        
        if response.status_code == 404:
            return False
        
        if response.status_code == 200:
            return True
        
        # Otros códigos: error temporal
        logger.warning(f" Status {response.status_code} al verificar {url}")
        return None
        
    except requests.Timeout:
        logger.warning(f" Timeout al verificar existencia de {url}")
        return None
    except requests.ConnectionError as e:
        logger.warning(f" Error de conexión al verificar {url}: {e}")
        return None

def download_precipitation_data(url: str, filepath: Path, timeout: int, logger: logging.Logger) -> bool:
    """Descarga la variable 'prec' desde servidor THREDDS y guarda como NetCDF."""
    try:
        # Configurar timeout en xarray
        os.environ['XARRAY_NETCDF4_TIMEOUT'] = str(timeout)
        
        # decode_times=False para descarga RAW más segura y rápida
        with xr.open_dataset(
            url,
            decode_times=False, 
            engine='netcdf4'
        ) as ds:
            
            if 'prec' not in ds:
                logger.error(f" Variable 'prec' no encontrada en {url}")
                return False
            
            da_prec = ds['prec']
            
            # Guardar con compresión
            encoding = {
                'prec': {
                    'zlib': True,
                    'complevel': 4, # 4 es un buen balance velocidad/compresión
                    'dtype': 'float32'
                }
            }
            
            da_prec.to_netcdf(filepath, encoding=encoding)
            
            file_size_kb = filepath.stat().st_size / 1024
            logger.info(f" Descargado: {filepath.name} ({file_size_kb:.1f} KB)")
            return True
            
    except Exception as e:
        logger.error(f" Error descargando desde {url}: {type(e).__name__}: {e}")
        
        # Limpiar archivo parcial si existe
        if filepath.exists():
            filepath.unlink()
            logger.debug(f" Eliminado archivo parcial: {filepath.name}")
        
        return False

def exponential_backoff_retry(func, max_retries: int, base_delay: float, logger: logging.Logger, *args, **kwargs):
    """Ejecuta una función con reintentos y backoff exponencial."""
    last_exception = None
    
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
            
        except (requests.Timeout, ConnectionError, OSError) as e:
            # Errores recuperables (red, timeout)
            last_exception = e
            
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                logger.warning(
                    f" Intento {attempt + 1}/{max_retries} falló: {type(e).__name__}. "
                    f"Reintentando en {delay:.1f}s..."
                )
                time.sleep(delay)
            else:
                logger.error(f" Falló tras {max_retries} intentos: {e}")
                
        except Exception as e:
            # Errores no recuperables
            logger.error(f" Error no recuperable: {type(e).__name__}: {e}")
            raise
            
    raise last_exception

# ============================================================================
# MANEJO DE SEÑALES
# ============================================================================
class GracefulKiller:
    kill_now = False
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)
    
    def exit_gracefully(self, signum, frame):
        self.logger.warning("\n Señal de interrupción recibida. Terminando limpiamente...")
        self.kill_now = True

# ============================================================================
# FUNCIÓN PRINCIPAL
# ============================================================================
def main():
    config = Config()
    logger = setup_logging()
    killer = GracefulKiller(logger)
    
    logger.info("=" * 80)
    logger.info(f" INICIANDO DESCARGA MASIVA WRF (MODO PRO)")
    logger.info(f" Período: {config.START_DATE} hasta {config.END_DATE}")
    logger.info(f" Destino: {config.OUTPUT_DIR.absolute()}")
    logger.info("=" * 80)
    
    all_dates = list(date_range(config.START_DATE, config.END_DATE))
    
    stats = {
        'existentes': 0,
        'descargados': 0,
        'no_disponibles': 0,
        'errores': 0,
        'corruptos_reparados': 0
    }
    
    # Barra de progreso
    with tqdm(all_dates, unit="día", desc="Progreso Global") as pbar:
        for current_date in pbar:
            
            if killer.kill_now:
                logger.warning(" Proceso interrumpido por usuario")
                break
            
            date_str = current_date.strftime("%Y%m%d")
            year = current_date.strftime("%Y")
            
            save_folder = config.OUTPUT_DIR / year
            save_folder.mkdir(exist_ok=True)
            
            filename = f"WRF_1km_prec_{date_str}.nc"
            filepath = save_folder / filename
            
            pbar.set_postfix_str(f"{date_str}")
            
            # 1. Verificar si ya existe y es válido
            if filepath.exists():
                if filepath.stat().st_size > config.MIN_FILE_SIZE:
                    if validate_netcdf_file(filepath, logger):
                        stats['existentes'] += 1
                        logger.debug(f" Saltando {date_str} (ya existe y es válido)")
                        continue
                    else:
                        logger.warning(f" Archivo corrupto detectado: {filename}")
                        filepath.unlink()
                        stats['corruptos_reparados'] += 1
                else:
                    logger.warning(f" Archivo incompleto: {filename}")
                    filepath.unlink()
            
            # 2. Construir URL y verificar servidor
            url = f"{config.BASE_URL}/{date_str}/wrf_arw_det_history_d02_{date_str}_0000.nc4"
            exists = check_remote_file_exists(url, config.TIMEOUT_SECONDS, logger)
            
            if exists is False:
                logger.debug(f" {date_str}: 404 No encontrado")
                stats['no_disponibles'] += 1
                continue
            
            if exists is None:
                logger.warning(f" {date_str}: Error de conexión al verificar")
                stats['errores'] += 1
                continue
            
            # 3. Descargar
            try:
                success = exponential_backoff_retry(
                    download_precipitation_data,
                    config.MAX_RETRIES,
                    config.RETRY_BASE_DELAY,
                    logger,
                    url,
                    filepath,
                    config.TIMEOUT_SECONDS,
                    logger
                )
                if success:
                    stats['descargados'] += 1
                else:
                    stats['errores'] += 1
                    
            except Exception as e:
                logger.error(f" {date_str}: Fallo crítico")
                stats['errores'] += 1
            
            # Pausa breve
            time.sleep(config.REQUEST_DELAY)

    # Resumen
    print("\n" + "=" * 60)
    print(" RESUMEN FINAL")
    print(f" Válidos previos:    {stats['existentes']}")
    print(f" Descargados hoy:    {stats['descargados']}")
    print(f" Corruptos reparados:{stats['corruptos_reparados']}")
    print(f" No disponibles:     {stats['no_disponibles']}")
    print(f" Errores:            {stats['errores']}")
    print("=" * 60)
    
    return 0

if __name__ == "__main__":
    main()