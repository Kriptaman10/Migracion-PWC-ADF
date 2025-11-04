"""
Utilidades comunes para la herramienta de migración
Funciones helper para logging, file I/O, formateo y validaciones
"""

import logging
import sys
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime
import json


def setup_logging(verbose: bool = False, log_file: Optional[str] = None) -> logging.Logger:
    """
    Configura el sistema de logging para la aplicación.

    Args:
        verbose: Si True, activa nivel DEBUG. Si False, usa INFO
        log_file: Ruta opcional al archivo de log

    Returns:
        Logger configurado
    """
    log_level = logging.DEBUG if verbose else logging.INFO

    # Formato del log
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'

    # Configurar handlers
    handlers = [logging.StreamHandler(sys.stdout)]

    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding='utf-8'))

    logging.basicConfig(
        level=log_level,
        format=log_format,
        datefmt=date_format,
        handlers=handlers
    )

    logger = logging.getLogger('pc-to-adf')
    logger.setLevel(log_level)

    return logger


def validate_file_path(file_path: str, extension: str = '.xml') -> Path:
    """
    Valida que un archivo exista y tenga la extensión correcta.

    Args:
        file_path: Ruta al archivo
        extension: Extensión esperada (ej: '.xml')

    Returns:
        Path object del archivo validado

    Raises:
        FileNotFoundError: Si el archivo no existe
        ValueError: Si la extensión es incorrecta
    """
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"El archivo no existe: {file_path}")

    if not path.is_file():
        raise ValueError(f"La ruta no es un archivo: {file_path}")

    if path.suffix.lower() != extension.lower():
        raise ValueError(f"Extensión incorrecta. Esperado {extension}, obtenido {path.suffix}")

    return path


def create_output_directory(output_dir: str) -> Path:
    """
    Crea el directorio de salida si no existe.

    Args:
        output_dir: Ruta del directorio de salida

    Returns:
        Path object del directorio creado
    """
    path = Path(output_dir)
    path.mkdir(parents=True, exist_ok=True)
    return path


def save_json(data: Dict[str, Any], file_path: str, pretty: bool = True) -> None:
    """
    Guarda un diccionario como archivo JSON.

    Args:
        data: Diccionario a guardar
        file_path: Ruta del archivo de salida
        pretty: Si True, formatea el JSON con indentación
    """
    with open(file_path, 'w', encoding='utf-8') as f:
        if pretty:
            json.dump(data, f, indent=2, ensure_ascii=False)
        else:
            json.dump(data, f, ensure_ascii=False)


def load_json(file_path: str) -> Dict[str, Any]:
    """
    Carga un archivo JSON.

    Args:
        file_path: Ruta del archivo JSON

    Returns:
        Diccionario con los datos cargados
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def sanitize_name(name: str) -> str:
    """
    Sanitiza un nombre para uso en Azure Data Factory.
    Elimina caracteres especiales y espacios.

    Args:
        name: Nombre a sanitizar

    Returns:
        Nombre sanitizado
    """
    # Reemplazar espacios por guiones bajos
    sanitized = name.replace(' ', '_')

    # Eliminar caracteres no alfanuméricos (excepto _ y -)
    sanitized = ''.join(c for c in sanitized if c.isalnum() or c in ('_', '-'))

    return sanitized


def format_timestamp() -> str:
    """
    Retorna timestamp formateado para nombres de archivo.

    Returns:
        String con formato YYYYMMDD_HHMMSS
    """
    return datetime.now().strftime('%Y%m%d_%H%M%S')


def calculate_migration_stats(
    total_transformations: int,
    migrated_transformations: int,
    warnings: int,
    errors: int
) -> Dict[str, Any]:
    """
    Calcula estadísticas de la migración.

    Args:
        total_transformations: Total de transformaciones encontradas
        migrated_transformations: Transformaciones migradas exitosamente
        warnings: Número de advertencias
        errors: Número de errores

    Returns:
        Diccionario con estadísticas calculadas
    """
    success_rate = (migrated_transformations / total_transformations * 100) if total_transformations > 0 else 0

    return {
        'total_transformations': total_transformations,
        'migrated_transformations': migrated_transformations,
        'failed_transformations': total_transformations - migrated_transformations,
        'success_rate': round(success_rate, 2),
        'warnings': warnings,
        'errors': errors,
        'timestamp': datetime.now().isoformat()
    }


def print_banner() -> None:
    """Imprime el banner de la aplicación"""
    banner = """
===============================================================
  PowerCenter to Azure Data Factory Migrator v2.0
  Desarrollado por: Entix SpA

  Nueva v2.0: Soporte completo para transformaciones avanzadas
  - Joiner (Inner, Left, Right, Full Outer)
  - Aggregator (SUM, AVG, COUNT, MIN, MAX)
  - Lookup (Database, Flat File, SQL Override)
  - Router (Conditional Split con múltiples salidas)
  - Sorter (Ordenamiento múltiple)
  - Update Strategy (INSERT, UPDATE, DELETE)
===============================================================
    """
    print(banner)


class MigrationError(Exception):
    """Excepción personalizada para errores de migración"""
    pass


class ValidationError(Exception):
    """Excepción personalizada para errores de validación"""
    pass
