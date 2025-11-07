"""
PowerCenter to Azure Data Factory Migrator
Herramienta CLI para automatizar la migración de mappings ETL
"""

__version__ = "2.5.0"
__author__ = "Practicante Entix"
__license__ = "MIT"

# Exportar clases principales para la interfaz web
from .parser import PowerCenterParser
from .translator import PowerCenterTranslator
from .generator import ADFGenerator
from .validator import MappingValidator

__all__ = [
    'PowerCenterParser',
    'PowerCenterTranslator',
    'ADFGenerator',
    'MappingValidator'
]
