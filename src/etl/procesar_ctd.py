import pandas as pd
import numpy as np
import os
import glob
import logging
from pathlib import Path
from typing import Optional, Dict, List
import json
from dataclasses import dataclass
from datetime import datetime

# --- CONFIGURACIÓN ---
@dataclass
class Config:
    """Configuración centralizada del proyecto"""
    INPUT_PATH: Path = Path("data/raw/c1/")
    OUTPUT_PATH: Path = Path("data/interim/")
    OUTPUT_FILE: str = "ctd_arousa_historico_unificado.csv"
    COORDS_FILE: Path = Path("config/coordenadas_ctd.json")
    LOG_PATH: Path = Path("logs/")
    
    # Parámetros de calidad
    MAX_PROFUNDIDAD: float = 500.0  # metros
    MIN_SALINIDAD: float = 0.0
    MAX_SALINIDAD: float = 50.0
    MIN_TEMPERATURA: float = -2.0
    MAX_TEMPERATURA: float = 40.0

# Instancia global de configuración
config = Config()

# --- CONFIGURACIÓN DE LOGGING AVANZADA ---
def setup_logging():
    """Configura sistema de logging con rotación de archivos"""
    config.LOG_PATH.mkdir(parents=True, exist_ok=True)
    
    # Formato detallado
    log_format = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Handler para archivo
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    file_handler = logging.FileHandler(
        config.LOG_PATH / f'ctd_processing_{timestamp}.log',
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(log_format)
    
    # Handler para consola
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(log_format)
    
    # Configurar logger raíz
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logging()

# --- CARGA DE COORDENADAS DESDE ARCHIVO ---
def cargar_coordenadas() -> Dict[str, Dict[str, float]]:
    """
    Carga coordenadas desde archivo JSON externo.
    Si no existe, usa coordenadas por defecto y las guarda.
    """
    coordenadas_default = {
        'A0': {'lat': 42.5181, 'lon': -8.9818},
        'A1': {'lat': 42.5932, 'lon': -8.9329},
        'A2': {'lat': 42.6074, 'lon': -8.8893},
        'A3': {'lat': 42.6465, 'lon': -8.8413},
        'A4': {'lat': 42.5681, 'lon': -8.8894},
        'A5': {'lat': 42.5623, 'lon': -8.8042},
        'A6': {'lat': 42.5991, 'lon': -8.7765},
        'A7': {'lat': 42.4832, 'lon': -8.8724},
        'A8': {'lat': 42.4865, 'lon': -8.9371},
        'A9': {'lat': 42.5221, 'lon': -9.0065},
        'AC': {'lat': 42.5505, 'lon': -8.9102}
    }
    
    try:
        if config.COORDS_FILE.exists():
            with open(config.COORDS_FILE, 'r', encoding='utf-8') as f:
                coordenadas = json.load(f)
            logger.info(f"Coordenadas cargadas desde {config.COORDS_FILE}")
            return coordenadas
        else:
            # Crear archivo si no existe
            config.COORDS_FILE.parent.mkdir(parents=True, exist_ok=True)
            with open(config.COORDS_FILE, 'w', encoding='utf-8') as f:
                json.dump(coordenadas_default, f, indent=2, ensure_ascii=False)
            logger.warning(f"Archivo de coordenadas no encontrado. Creado en {config.COORDS_FILE}")
            return coordenadas_default
    except Exception as e:
        logger.error(f"Error cargando coordenadas: {e}. Usando valores por defecto.")
        return coordenadas_default

COORDENADAS_CTD = cargar_coordenadas()

# --- VALIDACIONES ---
class DataValidator:
    """Clase para validar datos oceanográficos"""
    
    @staticmethod
    def validar_rango(df: pd.DataFrame, col: str, min_val: float, max_val: float) -> pd.Series:
        """Retorna máscara booleana de valores fuera de rango"""
        if col not in df.columns:
            return pd.Series([False] * len(df))
        return (df[col] < min_val) | (df[col] > max_val)
    
    @staticmethod
    def generar_reporte_calidad(df: pd.DataFrame) -> Dict:
        """Genera métricas de calidad del dataset"""
        reporte = {
            'total_registros': len(df),
            'registros_completos': df.dropna().shape[0],
            'porcentaje_completo': (df.dropna().shape[0] / len(df) * 100) if len(df) > 0 else 0,
            'nulos_por_columna': df.isnull().sum().to_dict(),
            'porcentaje_nulos': (df.isnull().sum() / len(df) * 100).to_dict()
        }
        
        # Validaciones de rango
        if 'temperatura' in df.columns:
            temp_outliers = DataValidator.validar_rango(
                df, 'temperatura', config.MIN_TEMPERATURA, config.MAX_TEMPERATURA
            )
            reporte['temperatura_outliers'] = temp_outliers.sum()
        
        if 'salinidad' in df.columns:
            sal_outliers = DataValidator.validar_rango(
                df, 'salinidad', config.MIN_SALINIDAD, config.MAX_SALINIDAD
            )
            reporte['salinidad_outliers'] = sal_outliers.sum()
        
        return reporte

# --- PROCESAMIENTO MEJORADO ---
def detectar_inicio_datos(filepath: Path) -> int:
    """
    Detecta la línea donde comienzan los datos (Header).
    Estrategia robusta: Evita acentos y busca patrones clave.
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            
            for i, line in enumerate(lines):
                # Convertimos a minúsculas para comparar sin miedo
                line_lower = line.lower().strip()
                
                # ESTRATEGIA 1: Buscar cabecera estándar
                # Buscamos "odigo" (para saltar la tilde de Código) y "stacion"
                if "odigo" in line_lower and "stacion" in line_lower:
                    logger.debug(f"Header encontrado por patrón texto en línea {i}")
                    return i
                
                # ESTRATEGIA 2: Buscar por variables (VAR_0, VAR_1...)
                # A veces la cabecera es la línea que define las variables
                if "var_0" in line_lower and "var_1" in line_lower:
                    # En algunos formatos, la cabecera real está justo debajo de las variables
                    # Verificamos la siguiente línea
                    if i + 1 < len(lines) and "A0" in lines[i+1]:
                        return i
            
            # ESTRATEGIA 3 (DESESPERADA): Buscar el primer dato (A0)
            # Si encontramos "A0" al inicio de una línea, asumimos que la anterior es el header
            for i, line in enumerate(lines):
                if line.strip().startswith("A0") or line.strip().startswith("A1"):
                    logger.warning(f"Header deducido (encontrado dato A0) en línea {i-1}")
                    return max(0, i - 1)

        logger.error(f"❌ NO SE ENCONTRÓ HEADER en {filepath.name}. Revisar manualmente.")
        return -1 # Retornamos -1 para indicar fallo explícito

    except Exception as e:
        logger.error(f"Error leyendo {filepath.name}: {e}")
        return -1

def enriquecer_coordenadas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Añade coordenadas geográficas usando merge (más eficiente que apply).
    """
    if 'estacion_id' not in df.columns:
        logger.warning("Columna 'estacion_id' no encontrada. Saltando enriquecimiento geográfico.")
        df['lat'] = None
        df['lon'] = None
        return df
    
    # Crear DataFrame de coordenadas
    coords_df = pd.DataFrame([
        {'estacion_id': k, 'lat': v['lat'], 'lon': v['lon']}
        for k, v in COORDENADAS_CTD.items()
    ])
    
    # Limpiar IDs para mejor matching
    df['estacion_id_clean'] = df['estacion_id'].astype(str).str.strip().str.upper()
    coords_df['estacion_id_clean'] = coords_df['estacion_id'].str.strip().str.upper()
    
    # Merge
    df = df.merge(
        coords_df[['estacion_id_clean', 'lat', 'lon']],
        on='estacion_id_clean',
        how='left'
    )
    
    # Reportar estaciones sin coordenadas
    sin_coords = df[df['lat'].isnull()]['estacion_id'].unique()
    if len(sin_coords) > 0:
        logger.warning(f"Estaciones sin coordenadas: {list(sin_coords)}")
    
    df = df.drop(columns=['estacion_id_clean'])
    
    return df

def procesar_archivo_ctd(filepath: Path) -> Optional[pd.DataFrame]:
    """
    Procesa un archivo CTD individual con manejo robusto de errores.
    
    Args:
        filepath: Ruta al archivo .txt
        
    Returns:
        DataFrame procesado o None si falla
    """
    filename = filepath.name
    logger.info(f"{'='*60}")
    logger.info(f"Procesando: {filename}")
    logger.info(f"{'='*60}")
    
    # 1. DETECTAR INICIO DE DATOS
    start_line = detectar_inicio_datos(filepath)
    if start_line <= 0: 
        logger.error(f"Archivo {filename} SALTADO: No se pudo determinar el inicio de datos.")
        return None
    
   # 2. LEER DATOS RAW
    try:
        # Leer con advertencias capturadas
        with pd.option_context('mode.chained_assignment', None):
            df = pd.read_csv(
                filepath,
                skiprows=start_line,
                sep='\t',
                encoding='utf-8',
                decimal=',',
                # low_memory=False,  <-- ELIMINADA CORRECTAMENTE
                engine='python',
                on_bad_lines='warn' 
            )
        
        logger.info(f"Leídas {len(df)} filas del archivo raw")
        
    except pd.errors.ParserError as e:
        logger.error(f"Error de parseo en {filename}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error inesperado leyendo {filename}: {e}", exc_info=True)
        return None
    
    # Verificar que se leyeron datos
    if df.empty:
        logger.warning(f"Archivo {filename} resultó en DataFrame vacío")
        return None
    
    # 3. RENOMBRADO (ESTANDARIZACIÓN)
    df.columns = df.columns.str.strip()
    logger.info(f"Columnas originales: {list(df.columns)}")
    
    df.columns = df.columns.str.strip()
    
    rename_map = {
        # Identificadores
        'Código': 'estacion_id',
        'Estacion': 'estacion_nombre',
        'Data': 'fecha_hora',
        
        # Variables Físico-Químicas
        'VAR_0': 'temperatura',
        'VAR_1': 'salinidad',
        'VAR_2': 'presion_db',        # Decibares
        'VAR_3': 'ph',
        'VAR_4': 'oxigeno_ml_l',      # ml/l
        'VAR_5': 'transmitancia',
        'VAR_6': 'irradiancia',
        'VAR_7': 'fluorescencia_uv',
        'VAR_8': 'fluorescencia',     # Clorofila
        'VAR_9': 'densidad',          # Sigma-T
        'VAR_10': 'profundidad',
        'VAR_11': 'temperatura_its68',
        'VAR_12': 'conductividad',
        
        # Control de Calidad (QC) - Mapeamos los más críticos
        'CODVAL_0': 'qc_temperatura',
        'CODVAL_1': 'qc_salinidad',
        'CODVAL_4': 'qc_oxigeno',
        'CODVAL_8': 'qc_fluorescencia'
    }
    
    # Solo renombrar columnas que existan
    rename_map_existentes = {k: v for k, v in rename_map.items() if k in df.columns}
    df = df.rename(columns=rename_map_existentes)
    
    columnas_faltantes = set(rename_map.keys()) - set(rename_map_existentes.keys())
    if columnas_faltantes:
        logger.warning(f"Columnas esperadas no encontradas: {columnas_faltantes}")
    
    # 4. PARSEO DE TIPOS
    cols_numericas = ['salinidad', 'profundidad', 'temperatura', 'qc_salinidad', 'qc_temperatura']
    for col in cols_numericas:
        if col in df.columns:
            antes = df[col].notna().sum()
            df[col] = pd.to_numeric(df[col], errors='coerce')
            despues = df[col].notna().sum()
            perdidos = antes - despues
            if perdidos > 0:
                logger.warning(f"Columna '{col}': {perdidos} valores no numéricos convertidos a NaN")
    
    # 5. MERGE GEOGRÁFICO
    df = enriquecer_coordenadas(df)
    
    # 6. FORMATO FECHA
    if 'fecha_hora' in df.columns:
        antes = df['fecha_hora'].notna().sum()
        df['fecha_hora'] = pd.to_datetime(df['fecha_hora'], dayfirst=True, errors='coerce')
        despues = df['fecha_hora'].notna().sum()
        perdidos = antes - despues
        if perdidos > 0:
            logger.warning(f"Fechas inválidas: {perdidos} valores convertidos a NaT")
    
   # 7. METADATOS Y LIMPIEZA FINAL
    df['origen_archivo'] = filename
    df['fecha_procesamiento'] = pd.Timestamp.now()
    
    # Quitar espacios en IDs (ej: "A0   " -> "A0")
    if 'estacion_id' in df.columns:
        df['estacion_id'] = df['estacion_id'].astype(str).str.strip()

    # REORDENAMIENTO INTELIGENTE
    # Ponemos las columnas clave primero, y luego el resto de variables que existan
    cols_clave = ['estacion_id', 'estacion_nombre', 'lat', 'lon', 'fecha_hora', 'profundidad', 'salinidad', 'qc_salinidad', 'temperatura', 'qc_temperatura']
    
    # Detectamos qué otras columnas útiles tenemos (ph, oxigeno, etc.)
    cols_extra = [c for c in df.columns if c not in cols_clave and c in rename_map.values()]
    
    # Metadatos al final
    cols_meta = ['origen_archivo', 'fecha_procesamiento']
    
    # Construimos la lista final asegurando que existan
    cols_finales = [c for c in cols_clave if c in df.columns] + \
                   [c for c in cols_extra if c in df.columns] + \
                   cols_meta
                   
    df = df[cols_finales]

    # 8. REPORTE DE CALIDAD
    reporte = DataValidator.generar_reporte_calidad(df)
    
    # Log de resumen breve
    logger.info(f"Registros procesados: {len(df)}")
    
    return df

# --- MAIN MEJORADO ---
def main():
    """Función principal con manejo robusto de errores"""
    logger.info(f"{'#'*80}")
    logger.info(f"INICIANDO PROCESAMIENTO CTD - Ría de Arousa")
    logger.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"{'#'*80}")
    
    # Crear directorios
    try:
        config.OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
        logger.info(f"Directorio de salida: {config.OUTPUT_PATH.absolute()}")
    except Exception as e:
        logger.critical(f"No se pudo crear directorio de salida: {e}")
        return
    
    # Buscar archivos
    try:
        archivos = list(config.INPUT_PATH.glob("*.txt"))
        logger.info(f"Encontrados {len(archivos)} archivos .txt en {config.INPUT_PATH}")
        
        if not archivos:
            logger.error(f"No se encontraron archivos en {config.INPUT_PATH}")
            return
    except Exception as e:
        logger.critical(f"Error buscando archivos: {e}")
        return
    
    # Procesar archivos
    dfs_procesados = []
    archivos_fallidos = []
    
    for i, filepath in enumerate(archivos, 1):
        logger.info(f"\n[{i}/{len(archivos)}] Procesando archivo...")
        df_temp = procesar_archivo_ctd(filepath)
        
        if df_temp is not None and not df_temp.empty:
            dfs_procesados.append(df_temp)
            logger.info(f" Archivo procesado exitosamente")
        else:
            archivos_fallidos.append(filepath.name)
            logger.error(f" Archivo falló el procesamiento")
    
    # Consolidar resultados
    if not dfs_procesados:
        logger.critical(" NINGÚN ARCHIVO SE PROCESÓ EXITOSAMENTE")
        return
    
    logger.info(f"\n{'='*80}")
    logger.info(f"CONSOLIDANDO DATOS")
    logger.info(f"{'='*80}")
    
    try:
        master_df = pd.concat(dfs_procesados, ignore_index=True)
        logger.info(f"Total registros consolidados: {len(master_df):,}")
        
        # Ordenar
        if 'fecha_hora' in master_df.columns:
            master_df = master_df.sort_values(by=['fecha_hora', 'estacion_id'])
            logger.info("Datos ordenados por fecha y estación")
        
        # Guardar
        outfile = config.OUTPUT_PATH / config.OUTPUT_FILE
        master_df.to_csv(outfile, index=False, sep=';', decimal='.', encoding='utf-8')
        
        logger.info(f"\n{'='*80}")
        logger.info(f" PROCESAMIENTO COMPLETADO EXITOSAMENTE")
        logger.info(f"{'='*80}")
        logger.info(f"Archivo guardado: {outfile.absolute()}")
        logger.info(f"Tamaño del archivo: {outfile.stat().st_size / 1024 / 1024:.2f} MB")
        logger.info(f"Archivos procesados: {len(dfs_procesados)}/{len(archivos)}")
        
        if archivos_fallidos:
            logger.warning(f"Archivos fallidos: {archivos_fallidos}")
        
        # Reporte final de calidad
        reporte_final = DataValidator.generar_reporte_calidad(master_df)
        logger.info(f"\n REPORTE DE CALIDAD FINAL:")
        logger.info(f"  - Registros totales: {reporte_final['total_registros']:,}")
        logger.info(f"  - Registros completos: {reporte_final['registros_completos']:,} ({reporte_final['porcentaje_completo']:.1f}%)")
        logger.info(f"  - Rango temporal: {master_df['fecha_hora'].min()} a {master_df['fecha_hora'].max()}")
        logger.info(f"  - Estaciones únicas: {master_df['estacion_id'].nunique()}")
        
        # Mostrar muestra
        print("\n📋 MUESTRA DE DATOS:")
        print(master_df.head(10).to_string())
        
    except Exception as e:
        logger.critical(f"Error fatal en consolidación: {e}", exc_info=True)
        return

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("\n  Procesamiento interrumpido por el usuario")
    except Exception as e:
        logger.critical(f"Error fatal no manejado: {e}", exc_info=True)