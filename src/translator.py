"""
Traductor de componentes PowerCenter a Azure Data Factory
Mapea transformaciones, expresiones y tipos de datos
"""

import re
import logging
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path

from .parser import (
    MappingMetadata,
    Transformation,
    TransformField,
    Source,
    Target
)
from .utils import load_json, MigrationError

logger = logging.getLogger('pc-to-adf.translator')


class PowerCenterToADFTranslator:
    """
    Traductor de componentes PowerCenter a Azure Data Factory.
    Convierte transformaciones, expresiones y tipos de datos.
    """

    def __init__(self, mapping_rules_path: Optional[str] = None):
        """
        Inicializa el traductor.

        Args:
            mapping_rules_path: Ruta al archivo JSON con reglas de mapeo.
                               Si no se provee, usa el archivo por defecto.
        """
        if mapping_rules_path is None:
            # Usar archivo de configuración por defecto
            config_dir = Path(__file__).parent.parent / 'config'
            mapping_rules_path = str(config_dir / 'mapping_rules.json')

        try:
            self.mapping_rules = load_json(mapping_rules_path)
            logger.info(f"Reglas de mapeo cargadas desde: {mapping_rules_path}")
        except FileNotFoundError:
            logger.warning("No se encontró archivo de reglas. Usando reglas por defecto.")
            self.mapping_rules = self._get_default_rules()

        self.transformation_mappings = self.mapping_rules.get('transformations', {})
        self.function_mappings = self.mapping_rules.get('functions', {})
        self.datatype_mappings = self.mapping_rules.get('datatypes', {})

        self.warnings: List[str] = []
        self.errors: List[str] = []

    def translate_mapping(self, metadata: MappingMetadata) -> Dict[str, Any]:
        """
        Traduce un mapping completo de PowerCenter a ADF.

        Args:
            metadata: Metadata del mapping parseado

        Returns:
            Diccionario con la estructura traducida para ADF
        """
        logger.info(f"Iniciando traducción del mapping: {metadata.name}")

        adf_structure = {
            'name': metadata.name,
            'description': metadata.description or f"Migrado desde PowerCenter: {metadata.name}",
            'sources': [],
            'transformations': [],
            'sinks': [],
            'warnings': [],
            'errors': []
        }

        # Traducir sources
        for source in metadata.sources:
            adf_source = self.translate_source(source)
            adf_structure['sources'].append(adf_source)

        # Traducir transformaciones
        for transformation in metadata.transformations:
            try:
                adf_trans = self.translate_transformation(transformation)
                if adf_trans:
                    adf_structure['transformations'].append(adf_trans)
            except MigrationError as e:
                error_msg = f"Error en transformación '{transformation.name}': {e}"
                logger.error(error_msg)
                self.errors.append(error_msg)

        # Traducir targets
        for target in metadata.targets:
            adf_sink = self.translate_target(target)
            adf_structure['sinks'].append(adf_sink)

        # Agregar warnings y errors acumulados
        adf_structure['warnings'] = self.warnings
        adf_structure['errors'] = self.errors

        logger.info(
            f"Traducción completada: {len(adf_structure['transformations'])} transformaciones, "
            f"{len(self.warnings)} warnings, {len(self.errors)} errors"
        )

        return adf_structure

    def translate_source(self, source: Source) -> Dict[str, Any]:
        """Traduce una fuente de PowerCenter a ADF Source"""
        return {
            'name': source.name,
            'type': 'Source',
            'dataset': {
                'type': self._map_database_type(source.database_type),
                'table': source.table_name
            },
            'schema': [
                {
                    'name': field.name,
                    'type': self.map_datatype(field.datatype)
                }
                for field in source.fields
            ]
        }

    def translate_target(self, target: Target) -> Dict[str, Any]:
        """Traduce un target de PowerCenter a ADF Sink"""
        return {
            'name': target.name,
            'type': 'Sink',
            'dataset': {
                'type': self._map_database_type(target.database_type),
                'table': target.table_name
            },
            'schema': [
                {
                    'name': field.name,
                    'type': self.map_datatype(field.datatype)
                }
                for field in target.fields
            ]
        }

    def translate_transformation(self, transformation: Transformation) -> Optional[Dict[str, Any]]:
        """
        Traduce una transformación de PowerCenter a su equivalente en ADF.

        Args:
            transformation: Objeto Transformation de PowerCenter

        Returns:
            Diccionario con la transformación en formato ADF, o None si no es soportada
        """
        trans_type = transformation.type
        adf_type = self.transformation_mappings.get(trans_type)

        if not adf_type:
            warning = f"Transformación '{trans_type}' no soportada: {transformation.name}"
            logger.warning(warning)
            self.warnings.append(warning)
            return None

        logger.debug(f"Traduciendo {trans_type} -> {adf_type}: {transformation.name}")

        # Delegar a método específico según el tipo
        translation_methods = {
            'Expression': self._translate_expression,
            'Filter': self._translate_filter,
            'Aggregator': self._translate_aggregator,
            'Joiner': self._translate_joiner,
            'Sorter': self._translate_sorter,
            'Source Qualifier': self._translate_source_qualifier,
            'Router': self._translate_router,
            'Lookup': self._translate_lookup
        }

        method = translation_methods.get(trans_type)
        if method:
            return method(transformation, adf_type)

        # Si no hay método específico, crear estructura básica
        return {
            'name': transformation.name,
            'type': adf_type,
            'description': transformation.description
        }

    def _translate_expression(self, trans: Transformation, adf_type: str) -> Dict[str, Any]:
        """Traduce Expression a Derived Column"""
        columns = []

        for field in trans.fields:
            if field.expression:
                columns.append({
                    'name': field.name,
                    'expression': self.translate_expression(field.expression)
                })

        return {
            'name': trans.name,
            'type': adf_type,
            'description': trans.description,
            'columns': columns
        }

    def _translate_filter(self, trans: Transformation, adf_type: str) -> Dict[str, Any]:
        """Traduce Filter a Filter"""
        filter_condition = trans.properties.get('filter_condition', 'true')

        return {
            'name': trans.name,
            'type': adf_type,
            'description': trans.description,
            'condition': self.translate_expression(filter_condition)
        }

    def _translate_aggregator(self, trans: Transformation, adf_type: str) -> Dict[str, Any]:
        """Traduce Aggregator a Aggregate"""
        group_by = trans.properties.get('group_by_fields', [])

        aggregates = []
        for field in trans.fields:
            if field.expression and any(agg in field.expression.upper()
                                       for agg in ['SUM', 'AVG', 'COUNT', 'MIN', 'MAX']):
                aggregates.append({
                    'name': field.name,
                    'expression': self.translate_expression(field.expression)
                })

        return {
            'name': trans.name,
            'type': adf_type,
            'description': trans.description,
            'groupBy': group_by,
            'aggregates': aggregates
        }

    def _translate_joiner(self, trans: Transformation, adf_type: str) -> Dict[str, Any]:
        """Traduce Joiner a Join"""
        join_type_map = {
            'Normal': 'inner',
            'Master Outer': 'left',
            'Detail Outer': 'right',
            'Full Outer': 'outer'
        }

        pc_join_type = trans.properties.get('join_type', 'Normal')
        adf_join_type = join_type_map.get(pc_join_type, 'inner')

        return {
            'name': trans.name,
            'type': adf_type,
            'description': trans.description,
            'joinType': adf_join_type,
            'joinCondition': trans.properties.get('join_condition', '')
        }

    def _translate_sorter(self, trans: Transformation, adf_type: str) -> Dict[str, Any]:
        """Traduce Sorter a Sort"""
        sort_fields = trans.properties.get('sort_fields', [])

        return {
            'name': trans.name,
            'type': adf_type,
            'description': trans.description,
            'orderBy': sort_fields
        }

    def _translate_source_qualifier(self, trans: Transformation, adf_type: str) -> Dict[str, Any]:
        """Traduce Source Qualifier a Source"""
        return {
            'name': trans.name,
            'type': adf_type,
            'description': trans.description
        }

    def _translate_router(self, trans: Transformation, adf_type: str) -> Dict[str, Any]:
        """Traduce Router a Conditional Split (parcial)"""
        warning = f"Router '{trans.name}' requiere configuración manual de condiciones"
        self.warnings.append(warning)

        return {
            'name': trans.name,
            'type': adf_type,
            'description': trans.description,
            'note': 'Configurar condiciones manualmente'
        }

    def _translate_lookup(self, trans: Transformation, adf_type: str) -> Dict[str, Any]:
        """Traduce Lookup a Lookup (parcial)"""
        warning = f"Lookup '{trans.name}' requiere configuración manual de query y dataset"
        self.warnings.append(warning)

        return {
            'name': trans.name,
            'type': adf_type,
            'description': trans.description,
            'note': 'Configurar lookup dataset y query manualmente'
        }

    def translate_expression(self, expression: str) -> str:
        """
        Traduce una expresión de PowerCenter a sintaxis de ADF.

        Args:
            expression: Expresión en sintaxis PowerCenter

        Returns:
            Expresión traducida a sintaxis ADF
        """
        if not expression:
            return ''

        translated = expression

        # Traducir funciones
        for pc_func, adf_func in self.function_mappings.items():
            # Case insensitive replacement
            pattern = re.compile(re.escape(pc_func), re.IGNORECASE)
            translated = pattern.sub(adf_func, translated)

        # Traducir operadores específicos
        translated = translated.replace('||', '+')  # Concatenación
        translated = translated.replace('!=', '<>')  # Diferente

        return translated

    def map_datatype(self, pc_datatype: str) -> str:
        """
        Mapea un tipo de dato de PowerCenter a ADF.

        Args:
            pc_datatype: Tipo de dato PowerCenter

        Returns:
            Tipo de dato ADF equivalente
        """
        pc_datatype_lower = pc_datatype.lower()

        # Buscar en mappings
        adf_type = self.datatype_mappings.get(pc_datatype_lower)

        if not adf_type:
            # Tipo por defecto
            logger.warning(f"Tipo de dato no mapeado: {pc_datatype}, usando String")
            return 'String'

        return adf_type

    def _map_database_type(self, db_type: str) -> str:
        """Mapea tipo de base de datos"""
        db_mappings = {
            'Oracle': 'OracleTable',
            'Microsoft SQL Server': 'AzureSqlTable',
            'Flat File': 'DelimitedText'
        }
        return db_mappings.get(db_type, 'AzureSqlTable')

    def _get_default_rules(self) -> Dict[str, Any]:
        """Retorna reglas de mapeo por defecto si no hay archivo de configuración"""
        return {
            'transformations': {
                'Source Qualifier': 'Source',
                'Expression': 'DerivedColumn',
                'Filter': 'Filter',
                'Aggregator': 'Aggregate',
                'Joiner': 'Join',
                'Sorter': 'Sort'
            },
            'functions': {
                'TO_DATE': 'toDate',
                'SYSDATE': 'currentTimestamp()',
                'SUBSTR': 'substring',
                'TRIM': 'trim',
                'UPPER': 'upper',
                'LOWER': 'lower',
                'LENGTH': 'length'
            },
            'datatypes': {
                'decimal': 'Int32',
                'number': 'Int32',
                'varchar2': 'String',
                'string': 'String',
                'date': 'DateTime',
                'timestamp': 'DateTime'
            }
        }

    def get_statistics(self) -> Dict[str, int]:
        """Retorna estadísticas de la traducción"""
        return {
            'warnings': len(self.warnings),
            'errors': len(self.errors)
        }
