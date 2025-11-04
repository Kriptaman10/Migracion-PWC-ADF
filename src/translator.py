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
            'Lookup': self._translate_lookup,
            'Lookup Procedure': self._translate_lookup,
            'Update Strategy': self._translate_update_strategy
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
        """
        Traduce Aggregator a Aggregate con soporte completo.

        Maneja:
        - Group by fields
        - Aggregate expressions (SUM, AVG, COUNT, MIN, MAX, FIRST, LAST)
        - Sorted input optimization
        """
        group_by = trans.properties.get('group_by_fields', [])
        sorted_input = trans.properties.get('sorted_input', False)

        # Extraer expresiones de agregación desde properties (más preciso)
        aggregates = []
        aggregate_expressions = trans.properties.get('aggregate_expressions', [])

        for agg_expr in aggregate_expressions:
            translated_expr = self.translate_expression(agg_expr['expression'])
            aggregates.append({
                'name': agg_expr['name'],
                'expression': translated_expr
            })

        # Si no hay expresiones en properties, intentar extraer de fields (fallback)
        if not aggregates:
            for field in trans.fields:
                if field.expression and any(agg in field.expression.upper()
                                           for agg in ['SUM', 'AVG', 'COUNT', 'MIN', 'MAX', 'FIRST', 'LAST']):
                    aggregates.append({
                        'name': field.name,
                        'expression': self.translate_expression(field.expression)
                    })

        result = {
            'name': trans.name,
            'type': adf_type,
            'description': trans.description,
            'groupBy': group_by,
            'aggregates': aggregates
        }

        # Agregar advertencia si sorted input está habilitado
        if sorted_input:
            warning = f"Aggregator '{trans.name}' has Sorted Input enabled. Ensure upstream Sort transformation exists."
            self.warnings.append(warning)
            result['sorted_input'] = True

        return result

    def _translate_joiner(self, trans: Transformation, adf_type: str) -> Dict[str, Any]:
        """
        Traduce Joiner a Join con soporte completo.

        Maneja:
        - Join types (inner, left, right, outer)
        - Join conditions (múltiples condiciones)
        - Master/Detail streams
        - Sorted input optimization
        """
        join_type_map = {
            'Normal Join': 'inner',
            'Normal': 'inner',
            'Master Outer': 'left',
            'Detail Outer': 'right',
            'Full Outer': 'outer'
        }

        pc_join_type = trans.properties.get('join_type', 'Normal Join')
        adf_join_type = join_type_map.get(pc_join_type, 'inner')
        join_condition = trans.properties.get('join_condition', '')
        sorted_input = trans.properties.get('sorted_input', False)
        master_fields = trans.properties.get('master_fields', [])
        detail_fields = trans.properties.get('detail_fields', [])

        # Parsear join conditions múltiples (separadas por AND)
        join_conditions = self._parse_join_conditions(join_condition)

        result = {
            'name': trans.name,
            'type': adf_type,
            'description': trans.description,
            'joinType': adf_join_type,
            'joinConditions': join_conditions,
            'masterFields': master_fields,
            'detailFields': detail_fields
        }

        # Agregar advertencia si sorted input está habilitado
        if sorted_input:
            warning = f"Joiner '{trans.name}' has Sorted Input enabled. Ensure both inputs are sorted."
            self.warnings.append(warning)
            result['sorted_input'] = True

        # Considerar broadcast si master tiene pocos campos
        if len(master_fields) < 10 and len(detail_fields) > 20:
            result['broadcast'] = 'left'
            info = f"Joiner '{trans.name}': Consider broadcast join for performance (small master table)"
            self.warnings.append(info)

        return result

    def _parse_join_conditions(self, condition_str: str) -> List[Dict[str, str]]:
        """
        Parsea join conditions múltiples separadas por AND.

        Ejemplo: "tabla1.campo1 = tabla2.campo1 AND tabla1.campo2 = tabla2.campo2"
        """
        conditions = []

        if not condition_str:
            return conditions

        # Separar por AND
        parts = condition_str.split(' AND ')

        for part in parts:
            part = part.strip()
            # Buscar operador de comparación
            for op in ['=', '==', '!=', '<>', '>', '<', '>=', '<=']:
                if op in part:
                    left, right = part.split(op, 1)
                    conditions.append({
                        'leftColumn': left.strip(),
                        'rightColumn': right.strip(),
                        'operator': '==' if op == '=' else op
                    })
                    break

        return conditions

    def _translate_sorter(self, trans: Transformation, adf_type: str) -> Dict[str, Any]:
        """
        Traduce Sorter a Sort con soporte completo.

        Maneja:
        - Sort keys múltiples con dirección
        - Distinct flag
        - Case sensitive
        """
        sort_keys = trans.properties.get('sort_keys', [])
        distinct = trans.properties.get('distinct', False)
        case_sensitive = trans.properties.get('case_sensitive', True)

        # Convertir sort_keys a formato ADF
        order_by = []
        for key in sort_keys:
            order_by.append({
                'name': key['name'],
                'order': 'asc' if key['direction'] == 'ASCENDING' else 'desc'
            })

        result = {
            'name': trans.name,
            'type': adf_type,
            'description': trans.description,
            'orderBy': order_by,
            'distinct': distinct
        }

        if not case_sensitive:
            warning = f"Sorter '{trans.name}' has case_sensitive=False. ADF Sort is case-sensitive by default."
            self.warnings.append(warning)

        return result

    def _translate_source_qualifier(self, trans: Transformation, adf_type: str) -> Dict[str, Any]:
        """Traduce Source Qualifier a Source"""
        return {
            'name': trans.name,
            'type': adf_type,
            'description': trans.description
        }

    def _translate_router(self, trans: Transformation, adf_type: str) -> Dict[str, Any]:
        """
        Traduce Router a Conditional Split completo.

        Maneja:
        - Múltiples output groups con expresiones
        - Default group
        - Campos por grupo con REF_FIELD
        """
        groups = trans.properties.get('groups', [])
        default_group = trans.properties.get('default_group')

        conditions = []
        for group in groups:
            if group['type'] == 'output' and group['expression']:
                conditions.append({
                    'name': group['name'],
                    'expression': self.translate_expression(group['expression']),
                    'fields': group.get('fields', [])
                })

        result = {
            'name': trans.name,
            'type': adf_type,
            'description': trans.description,
            'conditions': conditions,
            'defaultStream': default_group
        }

        if len(conditions) > 10:
            warning = f"Router '{trans.name}' has {len(conditions)} output groups. Consider simplifying."
            self.warnings.append(warning)

        return result

    def _translate_lookup(self, trans: Transformation, adf_type: str) -> Dict[str, Any]:
        """
        Traduce Lookup a Lookup completo.

        Maneja:
        - Database lookups
        - Flat File lookups
        - SQL Override
        - Lookup conditions
        - Cache configuration
        """
        lookup_table = trans.properties.get('lookup_table')
        source_type = trans.properties.get('source_type', 'Database')
        lookup_condition = trans.properties.get('lookup_condition', '')
        sql_override = trans.properties.get('sql_override')
        cache_enabled = trans.properties.get('cache_enabled', True)
        multiple_match_policy = trans.properties.get('multiple_match_policy', 'Use Any Value')
        return_fields = trans.properties.get('return_fields', [])

        # Parsear lookup conditions
        lookup_conditions = self._parse_join_conditions(lookup_condition)

        result = {
            'name': trans.name,
            'type': adf_type,
            'description': trans.description,
            'lookupDataset': lookup_table,
            'lookupConditions': lookup_conditions,
            'returnFields': return_fields
        }

        # Configurar cache mode
        if cache_enabled:
            result['cacheMode'] = 'static'
        else:
            result['cacheMode'] = 'none'

        # Manejar SQL Override
        if sql_override:
            result['sqlOverride'] = sql_override
            warning = f"Lookup '{trans.name}' has SQL Override. Review for ADF compatibility."
            self.warnings.append(warning)

        # Manejar Flat File
        if source_type == 'Flat File':
            flat_file_config = trans.properties.get('flat_file', {})
            result['sourceType'] = 'DelimitedText'
            result['flatFileConfig'] = flat_file_config
            info = f"Lookup '{trans.name}' uses Flat File. Ensure DelimitedText dataset is configured."
            self.warnings.append(info)

        # Manejar multiple match policy
        if multiple_match_policy != 'Use Any Value':
            warning = f"Lookup '{trans.name}' uses '{multiple_match_policy}'. ADF may behave differently."
            self.warnings.append(warning)

        return result

    def _translate_update_strategy(self, trans: Transformation, adf_type: str) -> Dict[str, Any]:
        """
        Traduce Update Strategy a Alter Row.

        Maneja:
        - DD_INSERT → insert
        - DD_UPDATE → update
        - DD_DELETE → delete
        - DD_REJECT → warning (no soportado)
        """
        strategy = trans.properties.get('strategy', 'DD_INSERT')
        strategy_expression = trans.properties.get('strategy_expression')

        # Mapeo de estrategias
        strategy_map = {
            'DD_INSERT': 'insert',
            'DD_UPDATE': 'update',
            'DD_DELETE': 'delete',
            'DD_REJECT': 'reject'
        }

        adf_action = strategy_map.get(strategy, 'insert')

        result = {
            'name': trans.name,
            'type': adf_type,
            'description': trans.description,
            'action': adf_action
        }

        # Agregar condición si existe expresión
        if strategy_expression:
            # Si la expresión es más compleja que solo DD_INSERT, agregar como condición
            if not strategy_expression.strip() == strategy:
                result['condition'] = self.translate_expression(strategy_expression)

        # Advertencia para DD_REJECT
        if strategy == 'DD_REJECT':
            warning = f"Update Strategy '{trans.name}' uses DD_REJECT. Consider using Router instead for error handling."
            self.warnings.append(warning)

        return result

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
                'Sorter': 'Sort',
                'Router': 'ConditionalSplit',
                'Lookup': 'Lookup',
                'Lookup Procedure': 'Lookup',
                'Update Strategy': 'AlterRow'
            },
            'functions': {
                'TO_DATE': 'toDate',
                'TO_CHAR': 'toString',
                'SYSDATE': 'currentTimestamp()',
                'SUBSTR': 'substring',
                'TRIM': 'trim',
                'UPPER': 'upper',
                'LOWER': 'lower',
                'LENGTH': 'length',
                'DECODE': 'case',
                'IIF': 'iif',
                'INSTR': 'indexOf',
                'CONCAT': 'concat',
                'SUM': 'sum',
                'AVG': 'avg',
                'COUNT': 'count',
                'MIN': 'min',
                'MAX': 'max',
                'FIRST': 'first',
                'LAST': 'last'
            },
            'datatypes': {
                'decimal': 'Int32',
                'number': 'Int32',
                'varchar2': 'String',
                'string': 'String',
                'char': 'String',
                'varchar': 'String',
                'date': 'DateTime',
                'timestamp': 'DateTime',
                'datetime': 'DateTime',
                'integer': 'Int32',
                'int': 'Int32',
                'bigint': 'Int64',
                'float': 'Double',
                'double': 'Double',
                'boolean': 'Boolean'
            }
        }

    def get_statistics(self) -> Dict[str, int]:
        """Retorna estadísticas de la traducción"""
        return {
            'warnings': len(self.warnings),
            'errors': len(self.errors)
        }
