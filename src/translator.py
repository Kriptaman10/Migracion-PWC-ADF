"""
Traductor de componentes PowerCenter a Azure Data Factory
Mapea transformaciones, expresiones y tipos de datos
Versión 2.0 con traductor de expresiones robusto
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
from .expression_translator import translate_expression as translate_expr_robust

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

        # Mapa de conexiones para rastrear el grafo
        self.connection_map: Dict[str, List[str]] = {}

        # Mapa de columnas con su casing original (case-sensitive tracking)
        # Formato: {nombre_lower: nombre_original}
        self.column_case_map: Dict[str, str] = {}

    def translate_mapping(self, metadata: MappingMetadata) -> Dict[str, Any]:
        """
        Traduce un mapping completo de PowerCenter a ADF.

        Args:
            metadata: Metadata del mapping parseado

        Returns:
            Diccionario con la estructura traducida para ADF
        """
        logger.info(f"Iniciando traducción del mapping: {metadata.name}")

        # Guardar referencia al metadata para usar en resoluciones
        self.metadata = metadata

        # Construir mapa de conexiones desde los connectors
        self._build_connection_map(metadata.connectors)

        # Construir mapa de columnas con su casing original desde sources
        self._build_column_case_map(metadata.sources)

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

    def _build_connection_map(self, connectors: List) -> None:
        """
        Construye un mapa de conexiones para rastrear el grafo de transformaciones.

        Args:
            connectors: Lista de conectores del mapping
        """
        self.connection_map = {}

        logger.info("=== CONSTRUYENDO CONNECTION MAP ===")

        for connector in connectors:
            to_instance = connector.to_instance
            from_instance = connector.from_instance

            if to_instance not in self.connection_map:
                self.connection_map[to_instance] = []

            self.connection_map[to_instance].append(from_instance)
            logger.debug(f"Connector: {from_instance} -> {to_instance}")

        # Log del connection map completo
        logger.info("=== CONNECTION MAP COMPLETO ===")
        for to_inst, from_insts in self.connection_map.items():
            logger.info(f"  {to_inst} <- {from_insts}")

    def _resolve_source_qualifier_to_source(self, transformation_name: str, metadata: 'MappingMetadata') -> str:
        """
        CRÍTICO: Resuelve el nombre de un Source Qualifier al nombre de su Source real.

        En PowerCenter, los Source Qualifiers (SQ_*) son transformaciones intermedias que
        leen de un Source. En ADF, debemos usar el nombre del Source directamente.

        Args:
            transformation_name: Nombre de la transformación (ej: "SQ_POLIZAS")
            metadata: Metadata del mapping con todas las transformaciones

        Returns:
            Nombre del source real si es un SQ, o el nombre original si no lo es
        """
        logger.info(f"=== RESOLVIENDO: '{transformation_name}' ===")

        # Verificar si es un Source Qualifier
        is_sq = False
        for trans in metadata.transformations:
            if trans.name == transformation_name:
                logger.info(f"  Encontrado en transformations: tipo='{trans.type}'")
                if trans.type == 'Source Qualifier':
                    is_sq = True
                    # Buscar el source que alimenta a este SQ
                    inputs = self.connection_map.get(transformation_name, [])
                    logger.info(f"  Inputs del SQ según connection_map: {inputs}")

                    # Listar todos los sources disponibles
                    source_names = [s.name for s in metadata.sources]
                    logger.info(f"  Sources disponibles: {source_names}")

                    for input_name in inputs:
                        logger.info(f"  Verificando input: '{input_name}'")
                        # Verificar si el input es un source
                        for source in metadata.sources:
                            if source.name == input_name:
                                logger.info(f"  ✓ MATCH! SQ '{transformation_name}' -> Source '{source.name}'")
                                return source.name
                            else:
                                logger.debug(f"    '{input_name}' != '{source.name}'")

                    # Si no encontramos el source, retornar el nombre original
                    logger.warning(f"  ✗ No se pudo resolver el Source para SQ '{transformation_name}'")
                    logger.warning(f"  Retornando nombre original: '{transformation_name}'")
                    return transformation_name
                break

        # No es un Source Qualifier, retornar el nombre original
        if not is_sq:
            logger.info(f"  No es un Source Qualifier, retornando '{transformation_name}'")

        return transformation_name

    def _build_column_case_map(self, sources: List[Source]) -> None:
        """
        Construye un mapa de columnas con su casing original para mantener consistencia.

        Args:
            sources: Lista de sources del mapping
        """
        self.column_case_map = {}
        for source in sources:
            for field in source.fields:
                col_name = field.name
                col_name_lower = col_name.lower()
                # Guardar el casing original
                if col_name_lower not in self.column_case_map:
                    self.column_case_map[col_name_lower] = col_name
                else:
                    # Si ya existe pero con diferente casing, generar warning
                    if self.column_case_map[col_name_lower] != col_name:
                        warning = (
                            f"Columna '{col_name}' encontrada con múltiples casings: "
                            f"'{self.column_case_map[col_name_lower]}' y '{col_name}'. "
                            f"Usando '{self.column_case_map[col_name_lower]}'."
                        )
                        self.warnings.append(warning)

    def _normalize_column_casing(self, expression: str) -> str:
        """
        Normaliza el casing de columnas en una expresión para mantener consistencia.

        Args:
            expression: Expresión con referencias a columnas

        Returns:
            Expresión con columnas normalizadas al casing original
        """
        if not expression:
            return expression

        # Crear copia de la expresión para modificar
        normalized_expr = expression

        # Buscar todas las palabras que podrían ser nombres de columnas
        # (palabras que no son funciones conocidas)
        import re
        words = re.findall(r'\b[A-Za-z_][A-Za-z0-9_]*\b', expression)

        for word in words:
            word_lower = word.lower()
            # Si la palabra está en nuestro mapa de columnas
            if word_lower in self.column_case_map:
                correct_casing = self.column_case_map[word_lower]
                # Si el casing actual es diferente, reemplazar
                if word != correct_casing:
                    # Usar word boundary para reemplazos exactos
                    pattern = r'\b' + re.escape(word) + r'\b'
                    normalized_expr = re.sub(pattern, correct_casing, normalized_expr)

        return normalized_expr

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
                # Normalizar casing de columnas en la expresión
                normalized_expr = self._normalize_column_casing(field.expression)
                translated_expr = self.translate_expression(normalized_expr)

                columns.append({
                    'name': field.name,
                    'expression': translated_expr
                })

                # Agregar la nueva columna al mapa de casing
                col_name_lower = field.name.lower()
                if col_name_lower not in self.column_case_map:
                    self.column_case_map[col_name_lower] = field.name

        return {
            'name': trans.name,
            'type': adf_type,
            'description': trans.description,
            'columns': columns
        }

    def _translate_filter(self, trans: Transformation, adf_type: str) -> Dict[str, Any]:
        """Traduce Filter a Filter"""
        filter_condition = trans.properties.get('filter_condition', 'true')

        # Normalizar casing de columnas en la condición
        normalized_condition = self._normalize_column_casing(filter_condition)

        return {
            'name': trans.name,
            'type': adf_type,
            'description': trans.description,
            'condition': self.translate_expression(normalized_condition)
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
            # Normalizar casing de columnas en la expresión de agregación
            normalized_expr = self._normalize_column_casing(agg_expr['expression'])
            translated_expr = self.translate_expression(normalized_expr)
            aggregates.append({
                'name': agg_expr['name'],
                'expression': translated_expr
            })

        # Si no hay expresiones en properties, intentar extraer de fields (fallback)
        if not aggregates:
            for field in trans.fields:
                if field.expression and any(agg in field.expression.upper()
                                           for agg in ['SUM', 'AVG', 'COUNT', 'MIN', 'MAX', 'FIRST', 'LAST']):
                    # Normalizar casing en la expresión
                    normalized_expr = self._normalize_column_casing(field.expression)
                    aggregates.append({
                        'name': field.name,
                        'expression': self.translate_expression(normalized_expr)
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

        # CRÍTICO: Determinar los dos inputs del join desde el connection_map
        inputs = self.connection_map.get(trans.name, [])
        left_input = None
        right_input = None

        logger.info(f"Procesando Joiner '{trans.name}', inputs raw: {inputs}")

        # CRÍTICO: Obtener inputs ÚNICOS (sin duplicados)
        # En PowerCenter, el connection_map tiene múltiples conexiones por cada campo,
        # pero para un Join solo nos interesan las 2 fuentes únicas
        unique_inputs = list(dict.fromkeys(inputs))  # Preserva orden, elimina duplicados
        logger.info(f"Inputs únicos para Joiner '{trans.name}': {unique_inputs}")

        if len(unique_inputs) >= 2:
            # El primero suele ser el Master, el segundo el Detail
            left_input_raw = unique_inputs[0]
            right_input_raw = unique_inputs[1]

            # CRÍTICO: Resolver Source Qualifiers a sus Sources reales
            # Esto evita que aparezca "SQ_POLIZAS, SQ_POLIZAS" en lugar de "VENTAS, POLIZAS"
            left_input = self._resolve_source_qualifier_to_source(left_input_raw, self.metadata)
            right_input = self._resolve_source_qualifier_to_source(right_input_raw, self.metadata)

            logger.info(f"Joiner '{trans.name}' resuelto: {left_input_raw} -> {left_input}, {right_input_raw} -> {right_input}")

            # VALIDACIÓN CRÍTICA: Detectar si la resolución falló y causó self-join
            if left_input == right_input and left_input_raw != right_input_raw:
                error = (
                    f"ERROR CRÍTICO: Joiner '{trans.name}' tiene inputs diferentes ({left_input_raw} vs {right_input_raw}) "
                    f"pero ambos se resolvieron al mismo stream: '{left_input}'. Esto causará un self-join erróneo."
                )
                logger.error(error)
                self.errors.append(error)
                # Intentar usar los nombres raw como fallback
                if left_input_raw != right_input_raw:
                    logger.warning(f"Usando nombres raw como fallback: {left_input_raw}, {right_input_raw}")
                    left_input = left_input_raw
                    right_input = right_input_raw

        elif len(unique_inputs) == 1:
            left_input_raw = unique_inputs[0]
            left_input = self._resolve_source_qualifier_to_source(left_input_raw, self.metadata)
            right_input = None
            error = f"ERROR: Joiner '{trans.name}' tiene solo 1 input único conectado: {left_input_raw}. Un Join requiere 2 inputs."
            logger.error(error)
            self.errors.append(error)
        else:
            error = f"ERROR CRÍTICO: Joiner '{trans.name}' no tiene inputs únicos conectados. Inputs raw: {inputs}"
            logger.error(error)
            self.errors.append(error)
            left_input = None
            right_input = None

        result = {
            'name': trans.name,
            'type': adf_type,
            'description': trans.description,
            'joinType': adf_join_type,
            'joinConditions': join_conditions,
            'masterFields': master_fields,
            'detailFields': detail_fields,
            'leftInput': left_input,  # NUEVO: Input real del Master
            'rightInput': right_input  # NUEVO: Input real del Detail
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

        # CRÍTICO: Determinar el input principal del Lookup desde el connection_map
        # Un Lookup tiene dos flujos de entrada:
        # 1. Pipeline input (main stream) - viene del connection_map
        # 2. Lookup table - viene de lookup_table property
        inputs = self.connection_map.get(trans.name, [])
        unique_inputs = list(dict.fromkeys(inputs))  # Eliminar duplicados

        main_input = None
        if unique_inputs:
            # El input principal es el primer (y usualmente único) input único
            main_input_raw = unique_inputs[0]
            # Resolver Source Qualifier a Source real
            main_input = self._resolve_source_qualifier_to_source(main_input_raw, self.metadata)
            logger.info(f"Lookup '{trans.name}': main_input={main_input_raw} -> {main_input}, lookup_table={lookup_table}")

        # CRÍTICO: Detectar columnas duplicadas y crear mapeo de desambiguación
        # PowerCenter añade sufijos cuando hay colisión de nombres:
        #   - Columna SIN sufijo = YA EXISTE en el flujo (de cualquier paso anterior)
        #   - Columna CON sufijo = NUEVA del LOOKUP actual (para evitar colisión)
        #
        # ADF requiere cualificación EXPLÍCITA cuando hay ambigüedad:
        #   - Columnas CON sufijo: mapear a lookup_table@base_name
        #   - Columnas SIN sufijo QUE COLISIONAN: mapear a main_input@column
        #
        # Ejemplo: lkp_DIM_PROMOTIONS tiene PROMO_ID, PROMO_ID1, PROMO_ID2
        #   - PROMO_ID (sin sufijo pero colisiona): mapear a main_input@PROMO_ID
        #   - PROMO_ID1 (con sufijo): mapear a lkp_DIM_DATES@PROMO_ID
        #   - PROMO_ID2 (con sufijo): mapear a DIM_PROMOTIONS@PROMO_ID
        column_disambiguation = {}

        # Get return fields (columns from lookup table)
        return_fields_names = [f['name'] if isinstance(f, dict) else f for f in trans.properties.get('return_fields', [])]

        # Analizar los campos del transformation para detectar renombramientos
        import re

        # Primero, identificar qué columnas tienen colisiones (tienen sufijos)
        collision_base_names = set()
        for field in trans.fields:
            match = re.match(r'^(.+?)(\d+)$', field.name)
            if match:
                collision_base_names.add(match.group(1))

        # Segundo, mapear las columnas
        for field in trans.fields:
            field_name = field.name

            # Detectar columnas con sufijos numéricos que indican colisión
            match = re.match(r'^(.+?)(\d+)$', field_name)

            if match:
                # Esta columna tiene sufijo (ej: DISCOUNT1, PROMO_ID2)
                # En PowerCenter, el sufijo indica que es NUEVA y VIENE DEL LOOKUP
                base_name = match.group(1)  # DISCOUNT, PROMO_ID
                suffix_num = match.group(2)  # 1, 2, etc.

                # Mapear: DISCOUNT1 → lookup_table@DISCOUNT (viene del lookup)
                # SOLO si el base_name está en los return_fields del lookup
                if lookup_table and base_name in return_fields_names:
                    column_disambiguation[field_name] = f"{lookup_table}@{base_name}"
                    logger.debug(f"Lookup '{trans.name}': Mapeando {field_name} → {lookup_table}@{base_name} (nuevo campo del lookup)")
                else:
                    logger.debug(f"Lookup '{trans.name}': Columna {field_name} tiene sufijo pero no está en return_fields, no se mapea")

            elif field_name in collision_base_names:
                # Esta columna NO tiene sufijo, PERO existe una versión con sufijo
                # Esto significa que hay AMBIGÜEDAD y necesita cualificación con main_input
                if main_input:
                    column_disambiguation[field_name] = f"{main_input}@{field_name}"
                    logger.debug(f"Lookup '{trans.name}': Mapeando {field_name} → {main_input}@{field_name} (columna ambigua del stream principal)")

        if column_disambiguation:
            logger.info(f"Lookup '{trans.name}': Desambiguación de columnas aplicada: {len(column_disambiguation)} mapeos")

        result = {
            'name': trans.name,
            'type': adf_type,
            'description': trans.description,
            'lookupDataset': lookup_table,
            'mainInput': main_input,  # Input principal resuelto
            'lookupConditions': lookup_conditions,
            'returnFields': return_fields,
            'columnDisambiguation': column_disambiguation  # NUEVO: Mapeo de columnas duplicadas
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
        Usa el traductor robusto con mapeo completo de funciones.

        Args:
            expression: Expresión en sintaxis PowerCenter

        Returns:
            Expresión traducida a sintaxis ADF
        """
        if not expression:
            return ''

        try:
            # Usar el traductor robusto con mapeo completo
            translated = translate_expr_robust(expression)
            return translated
        except Exception as e:
            # Fallback al traductor original si falla el robusto
            logger.warning(f"Error en traductor robusto, usando fallback: {e}")

            translated = expression

            # Traducir funciones (fallback antiguo)
            for pc_func, adf_func in self.function_mappings.items():
                # Case insensitive replacement
                pattern = re.compile(re.escape(pc_func), re.IGNORECASE)
                translated = pattern.sub(adf_func, translated)

            # Traducir operadores específicos
            translated = translated.replace('||', 'concat')  # Concatenación

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
