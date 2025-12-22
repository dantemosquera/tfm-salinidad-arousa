import pandas as pd
import numpy as np
import glob
import os
import re
import logging
import json
from datetime import datetime
from pathlib import Path

# --- CONFIGURACIÓN DE LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('procesamiento_intecmar.log'),
        logging.StreamHandler()
    ]
)

# --- CONFIGURACIÓN ---
INPUT_PATH = "data/raw/c2/"
OUTPUT_PATH = "data/interim/"
Path(OUTPUT_PATH).mkdir(parents=True, exist_ok=True)

# Coordenadas WGS84 (Decimal Degrees)
COORDENADAS = {
    'ribeira':   {'lat': 42.551633, 'lon': -8.946442, 'profundidades': ['1_5m']},
    'cortegada': {'lat': 42.627583, 'lon': -8.782314, 'profundidades': ['1_5m', '3m']}
}

# Mapeo de códigos INTECMAR a variables
CODIGOS_VARIABLES = {
    '2079': 'salinidad',
    '2004': 'temperatura'
}

# Columnas finales esperadas (schema estricto)
COLUMNAS_ESPERADAS = [
    'fecha_hora',
    'salinidad_1_5m', 'qc_salinidad_1_5m',
    'temperatura_1_5m', 'qc_temperatura_1_5m',
    'salinidad_3m', 'qc_salinidad_3m',
    'temperatura_3m', 'qc_temperatura_3m'
]

# Rangos válidos para validación de calidad
RANGOS_VALIDOS = {
    'temperatura_1_5m': (-5, 35),   # °C en aguas costeras gallegas
    'temperatura_3m': (-5, 35),
    'salinidad_1_5m': (0, 40),      # PSU en estuarios
    'salinidad_3m': (0, 40)
}


def extraer_profundidad(col_name):
    """
    Versión 3.1: Prioridad Semántica.
    Si el nombre dice 'superficial', lo estandarizamos a '1_5m' 
    (aunque ponga 1m), para unificar todas las estaciones.
    """
    c_lower = col_name.lower()
    
    # --- ESTRATEGIA 1: Búsqueda Semántica (PRIORITARIA) ---
    # Normalizamos todo lo que sea "superficie" a la etiqueta estándar '1_5m'
    if 'superficial' in c_lower:
        return '1_5m'
    
    # Normalizamos "inferior" o "fondo" a '3m'
    if 'inferior' in c_lower or 'fondo' in c_lower:
        return '3m'

    # --- ESTRATEGIA 2: Búsqueda Numérica (FALLBACK) ---
    # Solo si no encontramos palabras clave, buscamos números explícitos.
    match = re.search(r'(\d+)[.,]?(\d*)\s*m', c_lower)
    if match:
        entero = match.group(1)
        decimal = match.group(2) if match.group(2) else '0'
        return f"{entero}_{decimal}m" if decimal != '0' else f"{entero}m"
        
    return None


def normalizar_columnas(df):
    """
    Versión 4.0 (Definitiva): 
    Mapeo de datos por semántica/regex.
    Mapeo de QC ESTRICTAMENTE POSICIONAL (QC de la columna anterior).
    """
    new_cols = {}
    cols_originales = list(df.columns) # Lista para acceder por índice
    
    for i, col in enumerate(cols_originales):
        c_lower = col.lower()
        
        # 1. FECHA
        if 'data' in c_lower or 'fecha' in c_lower:
            new_cols[col] = 'fecha_hora'
            continue
            
        # 2. DETECTAR SI ES UNA COLUMNA DE QC (Validación)
        is_qc = 'validacion' in c_lower or 'validación' in c_lower
        
        if is_qc:
            # LÓGICA POSICIONAL: Miramos la columna anterior (i-1)
            if i > 0:
                nombre_anterior_orig = cols_originales[i-1]
                # Si la anterior ya fue renombrada (ej: a 'salinidad_1_5m'), usamos ese nombre
                if nombre_anterior_orig in new_cols:
                    nombre_base = new_cols[nombre_anterior_orig]
                    new_cols[col] = f"qc_{nombre_base}"
            continue # Pasamos a la siguiente

        # 3. SI NO ES QC, ES DATO (Salinidad/Temperatura)
        profundidad = extraer_profundidad(col) # Usamos la v3.1 que ya funciona
        
        if profundidad:
            if 'salinidade' in c_lower or 'salinidad' in c_lower:
                new_cols[col] = f'salinidad_{profundidad}'
            elif 'temperatura' in c_lower:
                new_cols[col] = f'temperatura_{profundidad}'

    # Aplicar cambios
    renamed_df = df.rename(columns=new_cols)
    return renamed_df


def validar_rangos(df, station_name):
    """Valida que los datos estén en rangos físicamente posibles"""
    for col, (min_val, max_val) in RANGOS_VALIDOS.items():
        if col in df.columns:
            mask = (df[col] < min_val) | (df[col] > max_val)
            n_invalidos = mask.sum()
            if n_invalidos > 0:
                logging.warning(
                    f"{station_name} - {col}: {n_invalidos} valores fuera de rango "
                    f"[{min_val}, {max_val}]"
                )
                # Opcional: Marcar como NaN los valores inválidos
                # df.loc[mask, col] = np.nan
    return df


def procesar_archivo(filepath):
    """Procesa un archivo CSV individual con manejo robusto de errores"""
    try:
        filename = os.path.basename(filepath).lower()
        
        # Detectar estación
        station_name = None
        for estacion in COORDENADAS.keys():
            if estacion in filename:
                station_name = estacion
                break
        
        if not station_name:
            logging.warning(f"Saltando {filename}: No coincide con estaciones conocidas")
            return None

        # Leer CSV
        df = pd.read_csv(
            filepath, 
            sep=';', 
            decimal=',', 
            encoding='latin-1',
            low_memory=False 
        )
        
        # Limpiar espacios en nombres de columnas
        df.columns = df.columns.str.strip()
        
        # Normalizar columnas (Mapeo de nombres)
        df = normalizar_columnas(df)
        
        # Asegurar que TODAS las columnas esperadas existen
        for col in COLUMNAS_ESPERADAS:
            if col not in df.columns:
                df[col] = pd.NA
                # logging.debug(f"{station_name}: Añadida columna faltante '{col}'")
        
        # Seleccionar solo columnas de interés
        df = df[COLUMNAS_ESPERADAS]

        # --- CORRECCIÓN CRÍTICA: FORZAR NUMÉRICOS ---
        # Convertimos explícitamente las columnas de datos a números.
        # Si hay basura (texto, errores), 'coerce' lo convierte en NaN.
        cols_numericas = [c for c in df.columns if 'salinidad' in c or 'temperatura' in c]
        for col in cols_numericas:
            # Primero reemplazamos coma por punto por si se leyó como string
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.replace(',', '.')
            
            # Convertimos a número
            df[col] = pd.to_numeric(df[col], errors='coerce')
        # --------------------------------------------
        
        # Convertir fecha con manejo de errores
        len_antes = len(df)
        df['fecha_hora'] = pd.to_datetime(
            df['fecha_hora'], 
            format='%Y/%m/%d %H:%M',
            errors='coerce'
        )
        
        # Eliminar filas con fechas inválidas (NaN)
        df = df.dropna(subset=['fecha_hora'])

        # Validar rangos de datos (Ahora sí funcionará porque son números)
        df = validar_rangos(df, station_name)
        
        # Añadir metadatos geoespaciales
        df['estacion'] = station_name
        df['lat'] = COORDENADAS[station_name]['lat']
        df['lon'] = COORDENADAS[station_name]['lon']
        
        # Ordenar columnas
        cols_ordenadas = ['estacion', 'lat', 'lon', 'fecha_hora'] + \
                         [c for c in COLUMNAS_ESPERADAS if c != 'fecha_hora']
        df = df[cols_ordenadas]
        
        logging.info(f"[OK] Procesado {filename}: {len(df)} filas")
        return df

    except Exception as e:
        logging.error(f"Error procesando {filepath}: {e}", exc_info=True)
        return None


def generar_reporte_calidad(df):
    """Genera estadísticas de calidad del dataset unificado"""
    reporte = {
        'total_registros': len(df),
        'rango_temporal': {
            'inicio': df['fecha_hora'].min().isoformat() if not df['fecha_hora'].isna().all() else None,
            'fin': df['fecha_hora'].max().isoformat() if not df['fecha_hora'].isna().all() else None
        },
        'por_estacion': {}
    }
    
    for estacion in df['estacion'].unique():
        df_est = df[df['estacion'] == estacion]
        reporte['por_estacion'][estacion] = {
            'registros': len(df_est),
            'completitud': {
                col: f"{(1 - df_est[col].isna().sum() / len(df_est)) * 100:.1f}%"
                for col in COLUMNAS_ESPERADAS if col != 'fecha_hora'
            }
        }
    
    return reporte


# --- EJECUCIÓN PRINCIPAL ---
def main():
    logging.info("="*60)
    logging.info("INICIO DEL PROCESAMIENTO DE DATOS INTECMAR")
    logging.info("="*60)
    
    # Buscar archivos
    all_files = glob.glob(os.path.join(INPUT_PATH, "*.csv"))
    logging.info(f"Encontrados {len(all_files)} archivos CSV")
    
    if not all_files:
        logging.error(f"No se encontraron archivos en {INPUT_PATH}")
        return
    
    # Procesar archivos
    dfs = []
    for filepath in all_files:
        df_temp = procesar_archivo(filepath)
        if df_temp is not None:
            dfs.append(df_temp)
    
    if not dfs:
        logging.error("No se pudo procesar ningún archivo válido")
        return
    
    # Unificar
    logging.info("Unificando DataFrames...")
    master_df = pd.concat(dfs, ignore_index=True)
    
    # Eliminar duplicados temporales
    duplicados_antes = len(master_df)
    master_df = master_df.drop_duplicates(
        subset=['estacion', 'fecha_hora'], 
        keep='last'
    )
    duplicados_eliminados = duplicados_antes - len(master_df)
    if duplicados_eliminados > 0:
        logging.warning(f"Eliminados {duplicados_eliminados} registros duplicados")
    
    # Ordenar cronológicamente
    master_df = master_df.sort_values(by=['estacion', 'fecha_hora']).reset_index(drop=True)
    
    # Guardar archivos
    output_parquet = os.path.join(OUTPUT_PATH, "intecmar_master_unificado.parquet")
    output_csv = os.path.join(OUTPUT_PATH, "intecmar_master_unificado.csv")
    
    master_df.to_parquet(output_parquet, index=False)
    master_df.to_csv(output_csv, index=False, sep=';', decimal=',')
    
    logging.info(f"OK Datos guardados en: {output_parquet}")
    logging.info(f"OK CSV de inspección en: {output_csv}")
    
    # Generar reporte
    reporte = generar_reporte_calidad(master_df)
    reporte['fecha_procesamiento'] = datetime.now().isoformat()
    reporte['archivos_procesados'] = len(all_files)
    
    reporte_path = os.path.join(OUTPUT_PATH, "reporte_calidad.json")
    with open(reporte_path, 'w', encoding='utf-8') as f:
        json.dump(reporte, f, indent=2, ensure_ascii=False)
    
    logging.info(f"OK Reporte de calidad guardado en: {reporte_path}")
    
    # Mostrar resumen
    logging.info("\n" + "="*60)
    logging.info("RESUMEN DEL PROCESAMIENTO")
    logging.info("="*60)
    logging.info(f"Total de registros: {len(master_df):,}")
    logging.info(f"Estaciones: {master_df['estacion'].unique().tolist()}")
    logging.info("\nCompletitud de datos por estación:")
    
    for estacion, datos in reporte['por_estacion'].items():
        logging.info(f"\n  {estacion.upper()}:")
        logging.info(f"    Registros: {datos['registros']:,}")
        for var, pct in datos['completitud'].items():
            logging.info(f"    {var}: {pct}")
    
    logging.info("\n" + "="*60)
    logging.info("PROCESO COMPLETADO EXITOSAMENTE")
    logging.info("="*60)


if __name__ == "__main__":
    main()