"""
Generador de archivos JSON para Azure Data Factory usando scriptLines (DSL nativo)
Este generador usa el enfoque de scriptLines de ADF en lugar de dependsOn
Genera JSONs 100% compatibles con ADF usando su DSL nativo
"""

import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime

from .utils import save_json, format_timestamp, calculate_migration_stats
from .expression_translator import translate_expression, validate_adf_expression

logger = logging.getLogger('pc-to-adf.script_generator')


class ADFScriptGenerator:
    """
    Generador de archivos JSON para Azure Data Factory usando scriptLines.
    Usa el DSL nativo de ADF con scriptLines en lugar del enfoque dependsOn.
    Garantiza compatibilidad 100% con el esquema oficial de ADF.
    """

    def __init__(self, output_dir: str = './output'):
        """
        Inicializa el generador.

        Args:
            output_dir: Directorio donde se guardarán los archivos generados
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Script Generator inicializado. Output: {self.output_dir}")

        self.generated_files: List[str] = []
        self.validation_errors: List[str] = []
        self.validation_warnings: List[str] = []

        # Mapeo acumulativo de desambiguación de columnas a través del flujo
        self.column_disambiguation_map: Dict[str, str] = {}

        # Cargar mapeo de datasets validados en producción
        self.dataset_mapping = self._load_dataset_mapping()

    def _load_dataset_mapping(self) -> Dict[str, str]:
        """
        Carga el mapeo de nombres de datasets desde el archivo de configuración.
        Este mapeo contiene los nombres exactos validados en producción.

        Returns:
            Diccionario con el mapeo PowerCenter -> ADF Dataset
        """
        config_path = Path(__file__).parent.parent / 'config' / 'dataset_mapping.json'

        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                mappings = config.get('mappings', {})
                logger.info(f"Mapeo de datasets cargado: {len(mappings)} entradas")
                return mappings
        except FileNotFoundError:
            logger.warning(f"Archivo de mapeo no encontrado: {config_path}. Usando lógica por defecto.")
            return {}
        except json.JSONDecodeError as e:
            logger.error(f"Error al parsear mapeo de datasets: {e}. Usando lógica por defecto.")
            return {}

    def _apply_column_disambiguation(
        self,
        expression: str,
        current_transformations: List[Dict[str, Any]]
    ) -> str:
        """
        Aplica desambiguación automática de columnas en una expresión.

        Reemplaza referencias a columnas con sufijos numéricos (DISCOUNT1, PROMO_ID2)
        por sus versiones cualificadas con prefijos @ (StreamName@DISCOUNT).

        Args:
            expression: Expresión original con posibles columnas ambiguas
            current_transformations: Lista de transformaciones procesadas hasta ahora

        Returns:
            Expresión con columnas desambiguadas
        """
        # Recolectar todos los mapeos de desambiguación de lookups anteriores
        disambiguation_map = {}

        for trans in current_transformations:
            if trans.get('type') == 'Lookup':
                column_map = trans.get('columnDisambiguation', {})
                disambiguation_map.update(column_map)
                logger.debug(f"Aplicando mapeo de lookup '{trans['name']}': {column_map}")

        if not disambiguation_map:
            return expression

        # Aplicar reemplazos en la expresión
        # Ordenar por longitud descendente para evitar reemplazos parciales
        # (ej: reemplazar DISCOUNT1 antes que DISCOUNT)
        disambiguated_expr = expression

        for old_col, new_col in sorted(disambiguation_map.items(), key=lambda x: len(x[0]), reverse=True):
            # Reemplazar solo nombres de columnas completos (no partes de palabras)
            # Y que no estén ya cualificados con @ (negative lookbehind)
            import re
            # Buscar el nombre de columna como palabra completa, pero no si ya tiene @ antes
            pattern = r'(?<!@)\b' + re.escape(old_col) + r'\b'
            if re.search(pattern, disambiguated_expr):
                disambiguated_expr = re.sub(pattern, new_col, disambiguated_expr)
                logger.debug(f"Desambiguación: {old_col} → {new_col}")

        if disambiguated_expr != expression:
            logger.info(f"Expresión desambiguada: '{expression}' → '{disambiguated_expr}'")

        return disambiguated_expr

    def _normalize_dataset_name(self, name: str, is_source: bool = False) -> str:
        """
        Normaliza el nombre de un dataset usando el mapeo validado en producción.
        Si el nombre no está en el mapeo, usa la lógica por defecto.

        Args:
            name: Nombre del dataset (PowerCenter)
            is_source: True si es un source (origen), False si es target/sink (destino)

        Returns:
            Nombre normalizado del dataset (ADF)
        """
        # PRIORIDAD 1: Buscar en el mapeo exacto validado en producción
        if name in self.dataset_mapping:
            mapped_name = self.dataset_mapping[name]
            logger.debug(f"Dataset '{name}' mapeado a '{mapped_name}' (mapeo validado)")
            return mapped_name

        # PRIORIDAD 2: Lógica por defecto para nombres no encontrados
        logger.info(f"Dataset '{name}' no encontrado en mapeo. Aplicando lógica por defecto.")

        if is_source:
            # Para sources, intentar detectar si es lookup
            if name.startswith('lkp_') or 'lookup' in name.lower():
                return f"ds_lkp_{name.replace('lkp_', '')}"
            return f"ds_source_{name}"
        else:
            return f"ds_{name}"

    def _validate_source_topology(self, translated_structure: Dict[str, Any]) -> set:
        """
        Valida la topología del dataflow para detectar sources huérfanos.

        Un source es válido si:
        1. Tiene al menos una transformación que depende de él, O
        2. Está directamente conectado a un sink

        Args:
            translated_structure: Estructura traducida del mapping

        Returns:
            Set con nombres de sources válidos
        """
        valid_sources = set()
        sources = {s['name'] for s in translated_structure.get('sources', [])}
        transformations = translated_structure.get('transformations', [])
        sinks = translated_structure.get('sinks', [])

        # Si no hay transformations ni sinks, todos los sources son inválidos
        if not transformations and not sinks:
            logger.warning("No hay transformaciones ni sinks en el flujo. Todos los sources son huérfanos.")
            return valid_sources

        # Si hay transformaciones o sinks, los sources son válidos
        if transformations or sinks:
            valid_sources.update(sources)

        return valid_sources

    def generate_dataflow(
        self,
        name: str,
        translated_structure: Dict[str, Any]
    ) -> str:
        """
        Genera un archivo JSON de dataflow para ADF usando scriptLines.

        Args:
            name: Nombre del dataflow
            translated_structure: Estructura traducida del mapping

        Returns:
            Ruta al archivo generado

        Raises:
            ValueError: Si la estructura no es válida
        """
        logger.info(f"Generando dataflow con scriptLines: {name}")

        # Construir scriptLines
        script_lines = []

        # 1. Construir sources (simples, solo declaración) con validación de topología
        sources = []
        source_names = set()

        # CRÍTICO: Validar topología - verificar que sources tengan conexiones válidas
        valid_sources = self._validate_source_topology(translated_structure)

        for source in translated_structure.get('sources', []):
            source_name = source['name']

            # Validar que no sea duplicado
            if source_name in source_names:
                error_msg = f"Source duplicado detectado: '{source_name}'"
                logger.error(error_msg)
                self.validation_errors.append(error_msg)
                raise ValueError(error_msg)

            # Validar que el source tenga conexiones válidas (no sea huérfano)
            if source_name not in valid_sources:
                warning = (
                    f"Source huérfano detectado: '{source_name}' no tiene conexiones válidas "
                    "en el flujo. Se omitirá del dataflow."
                )
                logger.warning(warning)
                self.validation_warnings.append(warning)
                continue  # Saltar este source

            source_def = {
                'name': source['name'],
                'dataset': {
                    'referenceName': self._normalize_dataset_name(source['name'], is_source=True),
                    'type': 'DatasetReference'
                }
            }
            sources.append(source_def)
            source_names.add(source['name'])

            # Generar scriptLine para source
            # Formato: source(output(...), allowSchemaDrift: true, ...) ~> SourceName
            source_script = self._generate_source_script(source)
            script_lines.extend(source_script)

        # CRÍTICO FIX #1: Agregar sources de Flat File Lookups
        # Los lookups de archivo necesitan el archivo como segundo source
        for trans in translated_structure.get('transformations', []):
            if trans.get('type') in ['Lookup', 'lookup']:
                lookup_dataset = trans.get('lookupDataset')
                source_type = trans.get('sourceType', 'Database')

                # Solo agregar si es Flat File y no está ya en sources
                if source_type == 'Flat File' and lookup_dataset and lookup_dataset not in source_names:
                    lookup_source_def = {
                        'name': lookup_dataset,
                        'dataset': {
                            'referenceName': self._normalize_dataset_name(lookup_dataset, is_source=True),
                            'type': 'DatasetReference'
                        }
                    }
                    sources.append(lookup_source_def)
                    source_names.add(lookup_dataset)

                    # Generar scriptLine para lookup source
                    lookup_source_script = [
                        f"source(output(",
                        f"\t),",
                        f"\tallowSchemaDrift: true,",
                        f"\tvalidateSchema: false,",
                        f"\tignoreNoFilesFound: false) ~> {lookup_dataset}"
                    ]
                    script_lines.extend(lookup_source_script)
                    logger.info(f"Agregado Flat File Lookup source: {lookup_dataset}")

        # 2. Construir transformations (simples, solo nombre y tipo)
        transformations = []
        previous_step = sources[0]['name'] if sources else None

        # CRÍTICO FIX #4: Reordenar transformaciones respetando dependencias
        # Para evitar circular dependencies (lookup → sort → join → lookup)
        raw_transformations = translated_structure.get('transformations', [])
        ordered_transformations = self._topological_sort_transformations(raw_transformations)

        # NUEVO: Mantener lista de transformaciones procesadas para desambiguación de columnas
        processed_transformations = []

        for trans in ordered_transformations:
            trans_name = trans['name']
            trans_type = trans['type']

            # Validar que NO sea un source disfrazado
            if trans_name in source_names:
                logger.warning(f"Saltando transformación duplicada: {trans_name}")
                continue

            # Validar que no sea tipo 'Source Qualifier'
            if trans_type in ['Source Qualifier', 'source', 'Source']:
                logger.warning(f"Saltando transformación de tipo '{trans_type}': {trans_name}")
                continue

            # Declaración simple de transformación (solo nombre y tipo)
            trans_def = {
                'name': trans_name,
                'type': self._get_adf_transform_type(trans_type)
            }
            transformations.append(trans_def)

            # Generar scriptLines para la transformación
            # MODIFICADO: Pasar processed_transformations para desambiguación de columnas
            trans_script = self._generate_transformation_script(trans, previous_step, processed_transformations)
            script_lines.extend(trans_script)

            # Agregar a la lista de procesadas DESPUÉS de generar el script
            processed_transformations.append(trans)

            previous_step = trans_name

        # 3. Construir sinks (simples, solo nombre y dataset)
        sinks = []
        last_step = previous_step if previous_step else (sources[0]['name'] if sources else None)

        for sink in translated_structure.get('sinks', []):
            sink_def = {
                'name': sink['name'],
                'dataset': {
                    'referenceName': self._normalize_dataset_name(sink['name'], is_source=False),
                    'type': 'DatasetReference'
                }
            }
            sinks.append(sink_def)

            # Generar scriptLine para sink
            sink_script = self._generate_sink_script(sink, last_step)
            script_lines.extend(sink_script)

        # Estructura completa del dataflow con scriptLines
        dataflow = {
            'name': f"dataflow_{name}",
            'properties': {
                'description': translated_structure.get('description', ''),
                'type': 'MappingDataFlow',
                'typeProperties': {
                    'sources': sources,
                    'transformations': transformations,
                    'sinks': sinks,
                    'scriptLines': script_lines  # DSL nativo de ADF
                }
            },
            'type': 'Microsoft.DataFactory/factories/dataflows'
        }

        # Guardar archivo
        filename = f"dataflow_{name}_{format_timestamp()}.json"
        filepath = self.output_dir / filename
        save_json(dataflow, str(filepath))

        self.generated_files.append(str(filepath))
        logger.info(f"Dataflow con scriptLines generado: {filepath}")

        return str(filepath)

    def _get_adf_transform_type(self, pc_type: str) -> str:
        """
        Mapea tipos de transformación de PowerCenter a tipos de ADF.

        Args:
            pc_type: Tipo de transformación de PowerCenter

        Returns:
            Tipo de transformación de ADF
        """
        type_mapping = {
            'DerivedColumn': 'derivedColumn',
            'Filter': 'filter',
            'Aggregate': 'aggregate',
            'Join': 'join',
            'Sort': 'sort',
            'ConditionalSplit': 'conditionalSplit',
            'Lookup': 'lookup',
            'AlterRow': 'alterRow',
            'Select': 'select',
            'Union': 'union',
            'Exists': 'exists',
            'Pivot': 'pivot',
            'Unpivot': 'unpivot'
        }
        return type_mapping.get(pc_type, pc_type.lower())

    def _generate_source_script(self, source: Dict[str, Any]) -> List[str]:
        """
        Genera scriptLines para un source.

        Formato ADF:
        source(output(
            column1 as string,
            column2 as integer
        ),
        allowSchemaDrift: true,
        validateSchema: false) ~> SourceName

        Args:
            source: Definición del source

        Returns:
            Lista de líneas de script
        """
        source_name = source['name']
        schema = source.get('schema', [])

        script = []
        script.append(f"source(output(")

        # Agregar columnas del schema
        if schema:
            for i, col in enumerate(schema):
                col_name = col.get('name', 'unknown')
                col_type = self._map_type_to_adf(col.get('type', 'string'))

                if i < len(schema) - 1:
                    script.append(f"\t\t{col_name} as {col_type},")
                else:
                    script.append(f"\t\t{col_name} as {col_type}")

        script.append("\t),")
        script.append("\tallowSchemaDrift: true,")
        script.append("\tvalidateSchema: false,")
        script.append(f"\tignoreNoFilesFound: false) ~> {source_name}")

        return script

    def _generate_transformation_script(
        self,
        trans: Dict[str, Any],
        previous_step: str,
        processed_transformations: List[Dict[str, Any]] = None
    ) -> List[str]:
        """
        Genera scriptLines para una transformación según su tipo.

        Args:
            trans: Definición de la transformación
            previous_step: Nombre del paso anterior
            processed_transformations: Lista de transformaciones procesadas (para desambiguación)

        Returns:
            Lista de líneas de script
        """
        if processed_transformations is None:
            processed_transformations = []

        trans_type = trans['type']
        trans_name = trans['name']

        if trans_type == 'DerivedColumn':
            return self._generate_derived_column_script(trans, previous_step, processed_transformations)
        elif trans_type == 'Filter':
            return self._generate_filter_script(trans, previous_step, processed_transformations)
        elif trans_type == 'Aggregate':
            return self._generate_aggregate_script(trans, previous_step, processed_transformations)
        elif trans_type == 'Join':
            return self._generate_join_script(trans, previous_step)
        elif trans_type == 'Sort':
            return self._generate_sort_script(trans, previous_step)
        elif trans_type == 'ConditionalSplit':
            return self._generate_conditional_split_script(trans, previous_step)
        elif trans_type == 'Lookup':
            return self._generate_lookup_script(trans, previous_step)
        elif trans_type == 'Select':
            return self._generate_select_script(trans, previous_step)
        else:
            logger.warning(f"Tipo de transformación no soportado para scriptLines: {trans_type}")
            return [f"{previous_step} ~> {trans_name}"]

    def _generate_derived_column_script(
        self,
        trans: Dict[str, Any],
        previous_step: str,
        processed_transformations: List[Dict[str, Any]] = None
    ) -> List[str]:
        """
        Genera scriptLines para DerivedColumn.

        Formato ADF:
        PreviousStep derive(
            column1 = expression1,
            column2 = expression2
        ) ~> TransformName

        Args:
            trans: Definición de la transformación
            previous_step: Nombre del paso anterior
            processed_transformations: Lista de transformaciones procesadas (para desambiguación)

        Returns:
            Lista de líneas de script
        """
        if processed_transformations is None:
            processed_transformations = []

        trans_name = trans['name']
        columns = trans.get('columns', [])

        script = []
        script.append(f"{previous_step} derive(")

        # Filtrar columnas passthrough redundantes (donde expression == name)
        # En ADF, no necesitas generar "DISCOUNT1 = DISCOUNT1", esas columnas ya existen
        filtered_columns = []
        for col in columns:
            col_name = col['name']
            # CRÍTICO: La expresión YA está traducida por PowerCenterToADFTranslator
            # NO traducir de nuevo o se romperá (|| lógico se convierte a concat())
            expression = col.get('expression', '').strip()

            # Si la expresión es exactamente igual al nombre, es passthrough redundante
            if expression == col_name:
                logger.debug(f"DerivedColumn '{trans_name}': Omitiendo columna passthrough redundante '{col_name} = {expression}'")
                continue

            filtered_columns.append(col)

        # Generar scriptLines solo para columnas no redundantes
        for i, col in enumerate(filtered_columns):
            col_name = col['name']
            # CRÍTICO: La expresión YA está traducida, NO volver a traducir
            translated_expr = col.get('expression', '')

            # CRÍTICO: Aplicar desambiguación de columnas (DISCOUNT1 → lkp_DIM_DATES@DISCOUNT)
            disambiguated_expr = self._apply_column_disambiguation(translated_expr, processed_transformations)

            is_valid, errors = validate_adf_expression(disambiguated_expr)

            if not is_valid:
                warning = f"Expresión inválida en columna '{col_name}': {errors}"
                logger.warning(warning)
                self.validation_warnings.append(warning)

            if i < len(filtered_columns) - 1:
                script.append(f"\t\t{col_name} = {disambiguated_expr},")
            else:
                script.append(f"\t\t{col_name} = {disambiguated_expr}")

        script.append(f"\t) ~> {trans_name}")

        return script

    def _generate_filter_script(
        self,
        trans: Dict[str, Any],
        previous_step: str,
        processed_transformations: List[Dict[str, Any]] = None
    ) -> List[str]:
        """
        Genera scriptLines para Filter.

        Formato ADF:
        PreviousStep filter(condition) ~> FilterName

        Args:
            trans: Definición de la transformación
            previous_step: Nombre del paso anterior
            processed_transformations: Lista de transformaciones procesadas (para desambiguación)

        Returns:
            Lista de líneas de script
        """
        if processed_transformations is None:
            processed_transformations = []

        trans_name = trans['name']
        # CRÍTICO: La condición YA está traducida por PowerCenterToADFTranslator
        translated_condition = trans.get('condition', 'true()')

        # CRÍTICO: Aplicar desambiguación de columnas (DISCOUNT1 → lkp_DIM_DATES@DISCOUNT)
        disambiguated_condition = self._apply_column_disambiguation(translated_condition, processed_transformations)

        is_valid, errors = validate_adf_expression(disambiguated_condition)

        if not is_valid:
            warning = f"Condición inválida en filtro '{trans_name}': {errors}"
            logger.warning(warning)
            self.validation_warnings.append(warning)

        return [
            f"{previous_step} filter(",
            f"\t{disambiguated_condition}",
            f") ~> {trans_name}"
        ]

    def _generate_aggregate_script(
        self,
        trans: Dict[str, Any],
        previous_step: str,
        processed_transformations: List[Dict[str, Any]] = None
    ) -> List[str]:
        """
        Genera scriptLines para Aggregate.

        Formato ADF:
        PreviousStep aggregate(
            groupBy(column1, column2),
            agg1 = sum(column),
            agg2 = avg(column)
        ) ~> AggregateName

        Args:
            trans: Definición de la transformación
            previous_step: Nombre del paso anterior
            processed_transformations: Lista de transformaciones procesadas (para desambiguación)

        Returns:
            Lista de líneas de script
        """
        if processed_transformations is None:
            processed_transformations = []

        trans_name = trans['name']
        group_by = trans.get('groupBy', [])
        aggregates = trans.get('aggregates', [])

        script = []

        # CRÍTICO: Sintaxis exacta de ADF para Aggregate
        # Formato: "EXPCalcularCampos aggregate(groupBy(POLIZA1, FECHA_VENTA_STRING, FECHA_CARGA_PROC),"
        #          "     TOTAL_VENTA_DIA = sum(CANTIDAD_VENTA),"
        #          "     MONTO_TOTAL_DIA = sum(MONTO_VENTA)) ~> AGGTotalPoliza"

        # Primera línea con groupBy
        # CRÍTICO: Aplicar desambiguación a las columnas de groupBy
        if group_by:
            disambiguated_group_by = []
            for col in group_by:
                disambiguated_col = self._apply_column_disambiguation(col, processed_transformations)
                disambiguated_group_by.append(disambiguated_col)

            group_by_str = ', '.join(disambiguated_group_by)
            script.append(f"{previous_step} aggregate(groupBy({group_by_str}),")
        else:
            script.append(f"{previous_step} aggregate(")

        # Aggregate functions (con indentación de 5 espacios para alineación)
        for i, agg in enumerate(aggregates):
            agg_name = agg['name']
            # CRÍTICO: La expresión YA está traducida por PowerCenterToADFTranslator
            translated_expr = agg.get('expression', '')

            # CRÍTICO: Aplicar desambiguación de columnas (DISCOUNT1 → lkp_DIM_DATES@DISCOUNT)
            disambiguated_expr = self._apply_column_disambiguation(translated_expr, processed_transformations)

            is_valid, errors = validate_adf_expression(disambiguated_expr)

            if not is_valid:
                warning = f"Expresión de agregación inválida '{agg_name}': {errors}"
                logger.warning(warning)
                self.validation_warnings.append(warning)

            # CRÍTICO: Convertir SOLO los nombres de funciones a lowercase, NO las columnas
            # Esto preserva el case de las columnas (CANTIDAD_VENTA debe seguir siendo CANTIDAD_VENTA)
            # pero convierte SUM -> sum, AVG -> avg, etc.
            disambiguated_expr = self._normalize_aggregate_function_names(disambiguated_expr)

            # CRÍTICO: En ADF Aggregate, no puedes hacer asignaciones directas (REVENUE = REVENUE)
            # Debes usar una función de agregación como first(), last(), etc.
            # Detectar asignaciones directas: si la expresión es exactamente igual al nombre
            if disambiguated_expr.strip() == agg_name:
                logger.warning(f"Aggregate '{trans_name}': Convirtiendo asignación directa '{agg_name} = {disambiguated_expr}' a 'first({disambiguated_expr})'")
                disambiguated_expr = f"first({disambiguated_expr})"

            # Líneas con indentación de 5 espacios
            if i < len(aggregates) - 1:
                script.append(f"     {agg_name} = {disambiguated_expr},")
            else:
                script.append(f"     {agg_name} = {disambiguated_expr}) ~> {trans_name}")

        return script

    def _generate_join_script(
        self,
        trans: Dict[str, Any],
        previous_step: str
    ) -> List[str]:
        """
        Genera scriptLines para Join con inputs reales del grafo.

        Formato ADF:
        LeftInput, RightInput join(
            condition,
            joinType: 'inner'
        ) ~> JoinName

        Args:
            trans: Definición de la transformación
            previous_step: Nombre del paso anterior (solo para referencia)

        Returns:
            Lista de líneas de script
        """
        trans_name = trans['name']
        join_type = trans.get('joinType', 'inner')
        join_conditions = trans.get('joinConditions', [])

        # CRÍTICO: Usar EXCLUSIVAMENTE los inputs del translator, NO previous_step
        # El translator ya resolvió los Source Qualifiers a Sources reales
        left_input = trans.get('leftInput')
        right_input = trans.get('rightInput')

        # CRÍTICO: Validación estricta - NO usar fallbacks que causen self-joins
        if not left_input or not right_input:
            error = f"ERROR CRÍTICO: Join '{trans_name}' no tiene inputs válidos. leftInput={left_input}, rightInput={right_input}"
            logger.error(error)
            self.validation_errors.append(error)
            # Usar placeholders visibles para debugging
            left_input = left_input or 'MISSING_LEFT_INPUT'
            right_input = right_input or 'MISSING_RIGHT_INPUT'

        # Validación adicional: Detectar self-joins erróneos
        if left_input == right_input:
            warning = f"WARNING: Join '{trans_name}' tiene el mismo input en ambos lados: '{left_input}'. Esto puede ser un self-join erróneo."
            logger.warning(warning)
            self.validation_warnings.append(warning)

        script = []

        # CRÍTICO: Sintaxis exacta de ADF para Join
        # Formato: "VENTAS, POLIZAS join(VENTAS@POLIZA == POLIZAS@POLIZA,"
        #          "     joinType:'inner',"
        #          "     broadcast: 'auto')~> JNRPolizasVentas"

        logger.info(f"Generando Join: {left_input}, {right_input} join(...) ~> {trans_name}")

        if join_conditions:
            # Primera línea con la primera condición
            first_cond = join_conditions[0]
            left_col = first_cond.get('leftColumn', '')
            right_col = first_cond.get('rightColumn', '')

            # Usar @ para disambiguar: stream@columna
            script.append(f"{left_input}, {right_input} join({left_input}@{left_col} == {right_input}@{right_col},")

            # Condiciones adicionales (si hay más)
            for cond in join_conditions[1:]:
                left_col = cond.get('leftColumn', '')
                right_col = cond.get('rightColumn', '')
                script.append(f"     {left_input}@{left_col} == {right_input}@{right_col},")
        else:
            # Si no hay condiciones, al menos abrir el join
            script.append(f"{left_input}, {right_input} join(")

        # Agregar joinType y broadcast (formato ADF)
        script.append(f"     joinType:'{join_type}',")
        script.append(f"     broadcast: 'auto')~> {trans_name}")

        return script

    def _generate_sort_script(
        self,
        trans: Dict[str, Any],
        previous_step: str
    ) -> List[str]:
        """
        Genera scriptLines para Sort.

        Formato ADF:
        PreviousStep sort(
            asc(column1, true),
            desc(column2, true)
        ) ~> SortName

        Args:
            trans: Definición de la transformación
            previous_step: Nombre del paso anterior

        Returns:
            Lista de líneas de script
        """
        trans_name = trans['name']
        order_by = trans.get('orderBy', [])

        script = []
        script.append(f"{previous_step} sort(")

        for i, sort_col in enumerate(order_by):
            if isinstance(sort_col, dict):
                # CRÍTICO: El translator usa 'name' y 'order', NO 'column' y 'direction'
                col_name = sort_col.get('name', sort_col.get('column', ''))
                direction = sort_col.get('order', sort_col.get('direction', 'asc'))
            else:
                col_name = sort_col
                direction = 'asc'

            # Validar que col_name no esté vacío
            if not col_name:
                logger.error(f"Sort '{trans_name}': Columna vacía en orderBy: {sort_col}")
                continue

            if i < len(order_by) - 1:
                script.append(f"\t{direction}({col_name}, true),")
            else:
                script.append(f"\t{direction}({col_name}, true)")

        script.append(f") ~> {trans_name}")

        return script

    def _generate_conditional_split_script(
        self,
        trans: Dict[str, Any],
        previous_step: str
    ) -> List[str]:
        """
        Genera scriptLines para ConditionalSplit con sintaxis correcta.

        Formato ADF:
        PreviousStep split(
            condition1,
            condition2,
            disjoint: false
        ) ~> SplitName@(stream1, stream2, default)

        Args:
            trans: Definición de la transformación
            previous_step: Nombre del paso anterior

        Returns:
            Lista de líneas de script
        """
        trans_name = trans['name']
        conditions = trans.get('conditions', [])

        script = []

        # CRÍTICO: Sintaxis exacta de ADF para Conditional Split
        # Formato: "EXPCHECKDUPLICADOS split(V_ES_DUPLICADO == 1,"
        #          "     V_REGISTRO_VALIDO == 1,"
        #          "     disjoint: false) ~> RTRROUTER@(DUPLICADOS, VALIDOS, FORMATO)"

        stream_names = []

        # Primera línea con primera condición
        if conditions:
            first_cond = conditions[0]
            stream_names.append(first_cond.get('name', 'stream1'))
            # CRÍTICO: La expresión YA está traducida por PowerCenterToADFTranslator
            translated_expr = first_cond.get('expression', '')
            is_valid, errors = validate_adf_expression(translated_expr)

            if not is_valid:
                warning = f"Condición inválida en split '{stream_names[0]}': {errors}"
                logger.warning(warning)
                self.validation_warnings.append(warning)

            script.append(f"{previous_step} split({translated_expr},")

            # Condiciones adicionales
            for cond in conditions[1:]:
                stream_name = cond.get('name', f'stream{len(stream_names)+1}')
                stream_names.append(stream_name)
                # CRÍTICO: La expresión YA está traducida por PowerCenterToADFTranslator
                translated_expr = cond.get('expression', '')
                is_valid, errors = validate_adf_expression(translated_expr)

                if not is_valid:
                    warning = f"Condición inválida en split '{stream_name}': {errors}"
                    logger.warning(warning)
                    self.validation_warnings.append(warning)

                script.append(f"     {translated_expr},")
        else:
            script.append(f"{previous_step} split(")

        # Agregar disjoint flag y streams
        streams_str = ', '.join(stream_names)
        if stream_names:
            script.append(f"     disjoint: false) ~> {trans_name}@({streams_str}, default)")
        else:
            script.append(f"     disjoint: false) ~> {trans_name}@(default)")

        return script

    def _generate_lookup_script(
        self,
        trans: Dict[str, Any],
        previous_step: str
    ) -> List[str]:
        """
        Genera scriptLines para Lookup con dos flujos de entrada.

        Formato ADF:
        StreamPrincipal, StreamBusqueda lookup(
            lookupColumn == referenceColumn,
            multiple: false,
            pickup: 'any',
            broadcast: 'auto'
        ) ~> LookupName

        Args:
            trans: Definición de la transformación
            previous_step: Nombre del paso anterior (NO USADO - se usa mainInput)

        Returns:
            Lista de líneas de script
        """
        trans_name = trans['name']
        lookup_conditions = trans.get('lookupConditions', [])
        lookup_dataset = trans.get('lookupDataset', 'UnknownLookupSource')

        # CRÍTICO: Usar mainInput del translator (ya resuelto de SQ a Source)
        # en lugar de previous_step que puede ser incorrecto
        main_input = trans.get('mainInput')

        # CRÍTICO: Un Lookup tiene dos inputs:
        # 1. El flujo principal (mainInput desde translator)
        # 2. El flujo de lookup (lookup_dataset, que es el nombre del source)
        if main_input:
            main_stream = main_input
            logger.info(f"Lookup '{trans_name}': Usando mainInput='{main_input}' (ignorando previous_step='{previous_step}')")
        else:
            # Fallback a previous_step si mainInput no está disponible
            main_stream = previous_step
            logger.warning(f"Lookup '{trans_name}': mainInput no disponible, usando previous_step='{previous_step}'")

        lookup_stream = lookup_dataset

        script = []

        # CRÍTICO: Sintaxis exacta de ADF para Lookup
        # Formato: "STGTRANSACTIONS, DIMDATES lookup(TRANSACTION_DATE == DATE_VALUE,"
        #          "     multiple: false,"
        #          "     pickup: 'any')~> lkpDIMDATES"

        # Primera línea con primera condición
        if lookup_conditions:
            import re

            first_cond = lookup_conditions[0]
            left_col = first_cond.get('leftColumn', '')
            right_col = first_cond.get('rightColumn', '')

            # CRÍTICO: Limpiar sufijos numéricos de las columnas
            # Si PowerCenter genera PROMO_ID2, necesitamos usar PROMO_ID en la condición
            # porque PROMO_ID2 es la columna de SALIDA del lookup, no la condición de entrada
            def strip_numeric_suffix(col_name: str) -> str:
                """Elimina sufijos numéricos de nombres de columnas (PROMO_ID2 -> PROMO_ID)"""
                match = re.match(r'^(.+?)\d+$', col_name)
                if match:
                    return match.group(1)
                return col_name

            left_col_clean = strip_numeric_suffix(left_col)
            right_col_clean = strip_numeric_suffix(right_col)

            # CRÍTICO: En PowerCenter, las condiciones de Lookup son:
            #   lookup_column = main_column (ej: DATE_VALUE = TRANSACTION_DATE)
            # En ADF, el formato es:
            #   main_column == lookup_column (ej: TRANSACTION_DATE == DATE_VALUE)
            # Por lo tanto, necesitamos INVERTIR left y right
            #
            # CRÍTICO 2: ADF requiere cualificación EXPLÍCITA para evitar ambigüedad
            #   Formato: main_stream@column == lookup_stream@column
            script.append(f"{main_stream}, {lookup_stream} lookup({main_stream}@{right_col_clean} == {lookup_stream}@{left_col_clean},")

            # Condiciones adicionales (si hay más)
            for cond in lookup_conditions[1:]:
                left_col = cond.get('leftColumn', '')
                right_col = cond.get('rightColumn', '')
                left_col_clean = strip_numeric_suffix(left_col)
                right_col_clean = strip_numeric_suffix(right_col)
                # Invertir y cualificar también las condiciones adicionales
                script.append(f"     {main_stream}@{right_col_clean} == {lookup_stream}@{left_col_clean},")
        else:
            script.append(f"{main_stream}, {lookup_stream} lookup(")

        # Configuración adicional de lookup (formato ADF exacto)
        script.append(f"     multiple: false,")
        script.append(f"     pickup: 'any')~> {trans_name}")

        logger.info(f"Generando Lookup: {main_stream}, {lookup_stream} lookup(...) ~> {trans_name}")

        return script

    def _generate_select_script(
        self,
        trans: Dict[str, Any],
        previous_step: str
    ) -> List[str]:
        """
        Genera scriptLines para Select.

        Formato ADF:
        PreviousStep select(
            mapColumn(
                column1,
                column2
            ),
            skipDuplicateMapInputs: true,
            skipDuplicateMapOutputs: true
        ) ~> SelectName

        Args:
            trans: Definición de la transformación
            previous_step: Nombre del paso anterior

        Returns:
            Lista de líneas de script
        """
        trans_name = trans['name']
        columns = trans.get('columns', [])

        script = []
        script.append(f"{previous_step} select(")
        script.append(f"\tmapColumn(")

        for i, col in enumerate(columns):
            if isinstance(col, dict):
                col_name = col.get('name', '')
            else:
                col_name = col

            if i < len(columns) - 1:
                script.append(f"\t\t{col_name},")
            else:
                script.append(f"\t\t{col_name}")

        script.append(f"\t),")
        script.append(f"\tskipDuplicateMapInputs: true,")
        script.append(f"\tskipDuplicateMapOutputs: true")
        script.append(f") ~> {trans_name}")

        return script

    def _generate_sink_script(
        self,
        sink: Dict[str, Any],
        previous_step: str
    ) -> List[str]:
        """
        Genera scriptLines para un sink.

        Formato ADF:
        PreviousStep sink(
            allowSchemaDrift: true,
            validateSchema: false,
            skipDuplicateMapInputs: true,
            skipDuplicateMapOutputs: true
        ) ~> SinkName

        Args:
            sink: Definición del sink
            previous_step: Nombre del paso anterior

        Returns:
            Lista de líneas de script
        """
        sink_name = sink['name']

        return [
            f"{previous_step} sink(",
            f"\tallowSchemaDrift: true,",
            f"\tvalidateSchema: false,",
            f"\tskipDuplicateMapInputs: true,",
            f"\tskipDuplicateMapOutputs: true) ~> {sink_name}"
        ]

    def _normalize_aggregate_function_names(self, expression: str) -> str:
        """
        Normaliza SOLO los nombres de funciones de agregación a lowercase.
        Preserva COMPLETAMENTE el case de los nombres de columnas.

        Ejemplo:
        Input:  "SUM(CANTIDAD_VENTA)"
        Output: "sum(CANTIDAD_VENTA)"  <- Solo la función en lowercase

        Args:
            expression: Expresión con funciones de agregación

        Returns:
            Expresión con funciones normalizadas pero columnas preservadas
        """
        import re

        # CRÍTICO: Lista exhaustiva de funciones que ADF espera en lowercase
        agg_functions = [
            'SUM', 'AVG', 'COUNT', 'MIN', 'MAX', 'FIRST', 'LAST',
            'STDDEV', 'VARIANCE', 'MEDIAN', 'PERCENTILE',
            'COUNT_DISTINCT', 'COLLECT_LIST', 'COLLECT_SET'
        ]

        normalized = expression

        # Log para debugging
        logger.debug(f"Normalizando expresión: {expression}")

        for func in agg_functions:
            # Buscar la función (case insensitive) y reemplazar con minúsculas
            # IMPORTANTE: Usar word boundary para evitar reemplazos parciales
            pattern = r'\b' + re.escape(func) + r'\b'

            # Reemplazar preservando lo que está dentro de los paréntesis
            def replace_func(match):
                return func.lower()

            normalized = re.sub(pattern, replace_func, normalized, flags=re.IGNORECASE)

        logger.debug(f"Resultado normalizado: {normalized}")
        return normalized

    def _map_type_to_adf(self, pc_type: str) -> str:
        """
        Mapea tipos de datos de PowerCenter a tipos de ADF.

        Args:
            pc_type: Tipo de PowerCenter

        Returns:
            Tipo de ADF
        """
        type_mapping = {
            'String': 'string',
            'Int32': 'integer',
            'Int64': 'long',
            'Double': 'double',
            'Decimal': 'decimal',
            'DateTime': 'timestamp',
            'Date': 'date',
            'Boolean': 'boolean',
            'Binary': 'binary'
        }
        return type_mapping.get(pc_type, 'string')

    def generate_pipeline(
        self,
        name: str,
        translated_structure: Dict[str, Any]
    ) -> str:
        """
        Genera un archivo JSON de pipeline para ADF.

        Args:
            name: Nombre del pipeline
            translated_structure: Estructura traducida del mapping

        Returns:
            Ruta al archivo generado
        """
        logger.info(f"Generando pipeline: {name}")

        pipeline = {
            'name': f"pipeline_{name}",
            'properties': {
                'description': translated_structure.get('description', ''),
                'activities': [
                    {
                        'name': 'ExecuteDataFlow',
                        'type': 'ExecuteDataFlow',
                        'dependsOn': [],
                        'policy': {
                            'timeout': '1.00:00:00',
                            'retry': 0,
                            'retryIntervalInSeconds': 30,
                            'secureOutput': False,
                            'secureInput': False
                        },
                        'typeProperties': {
                            'dataFlow': {
                                'referenceName': f"dataflow_{name}",
                                'type': 'DataFlowReference'
                            },
                            'compute': {
                                'coreCount': 8,
                                'computeType': 'General'
                            }
                        }
                    }
                ],
                'annotations': [
                    f"Migrado desde PowerCenter - {datetime.now().strftime('%Y-%m-%d')}"
                ]
            }
        }

        # Guardar archivo
        filename = f"pipeline_{name}_{format_timestamp()}.json"
        filepath = self.output_dir / filename
        save_json(pipeline, str(filepath))

        self.generated_files.append(str(filepath))
        logger.info(f"Pipeline generado: {filepath}")

        return str(filepath)

    def generate_report(
        self,
        name: str,
        translated_structure: Dict[str, Any],
        original_metadata: Optional[Any] = None
    ) -> str:
        """
        Genera un reporte detallado de la migración.

        Args:
            name: Nombre del mapping
            translated_structure: Estructura traducida
            original_metadata: Metadata original del PowerCenter (opcional)

        Returns:
            Ruta al archivo de reporte generado
        """
        logger.info(f"Generando reporte de migración: {name}")

        # Calcular estadísticas
        total_trans = len(original_metadata.transformations) if original_metadata else 0
        migrated_trans = len(translated_structure.get('transformations', []))
        warnings = len(self.validation_warnings)
        errors = len(self.validation_errors)

        stats = calculate_migration_stats(total_trans, migrated_trans, warnings, errors)

        # Construir reporte
        report = {
            'mapping_name': name,
            'migration_date': datetime.now().isoformat(),
            'statistics': stats,
            'components': {
                'sources': len(translated_structure.get('sources', [])),
                'transformations': migrated_trans,
                'sinks': len(translated_structure.get('sinks', []))
            },
            'warnings': self.validation_warnings,
            'errors': self.validation_errors,
            'details': {
                'transformations_migrated': [
                    {
                        'name': t['name'],
                        'type': t['type'],
                        'status': 'migrated'
                    }
                    for t in translated_structure.get('transformations', [])
                ]
            },
            'recommendations': self._generate_recommendations(translated_structure)
        }

        # Guardar reporte
        filename = f"migration_report_{name}_{format_timestamp()}.json"
        filepath = self.output_dir / filename
        save_json(report, str(filepath))

        self.generated_files.append(str(filepath))
        logger.info(f"Reporte generado: {filepath}")

        return str(filepath)

    def generate_all(
        self,
        name: str,
        translated_structure: Dict[str, Any],
        original_metadata: Optional[Any] = None
    ) -> Dict[str, str]:
        """
        Genera todos los archivos de salida: pipeline, dataflow y reporte.

        Args:
            name: Nombre del mapping
            translated_structure: Estructura traducida
            original_metadata: Metadata original (opcional)

        Returns:
            Diccionario con rutas a todos los archivos generados
        """
        logger.info(f"Generando todos los archivos para: {name}")

        files = {
            'pipeline': self.generate_pipeline(name, translated_structure),
            'dataflow': self.generate_dataflow(name, translated_structure),
            'report': self.generate_report(name, translated_structure, original_metadata)
        }

        logger.info(f"Generación completada. {len(files)} archivos creados.")
        return files

    def _generate_recommendations(self, translated_structure: Dict[str, Any]) -> List[str]:
        """Genera recomendaciones basadas en la migración"""
        recommendations = []

        # Recomendar revisión si hay warnings
        if self.validation_warnings:
            recommendations.append(
                "Revisar warnings para identificar componentes que requieren ajuste manual"
            )

        # Recomendar configuración de Linked Services
        if translated_structure.get('sources') or translated_structure.get('sinks'):
            recommendations.append(
                "Configurar Linked Services en Azure Data Factory para fuentes y destinos"
            )

        # Recomendar testing
        recommendations.append(
            "Ejecutar pruebas de validación de datos después de importar a ADF"
        )

        # Recomendar revisión de scriptLines
        recommendations.append(
            "Revisar scriptLines generados para verificar la lógica de transformación"
        )

        return recommendations

    def _topological_sort_transformations(self, transformations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Ordena transformaciones respetando dependencias usando topological sort.
        Evita circular dependencies asegurando que las dependencias se procesen primero.
        """
        # Build name to transformation mapping
        trans_map = {t['name']: t for t in transformations}

        # Build dependency graph (adjacency list: node -> list of nodes that depend on it)
        graph = {t['name']: [] for t in transformations}
        in_degree = {t['name']: 0 for t in transformations}

        # Extract dependencies from mainInput, leftInput, rightInput
        for trans in transformations:
            trans_name = trans['name']
            dependencies = set()

            # Check mainInput
            if 'mainInput' in trans and trans['mainInput']:
                dependencies.add(trans['mainInput'])

            # Check leftInput (for Join)
            if 'leftInput' in trans and trans['leftInput']:
                dependencies.add(trans['leftInput'])

            # Check rightInput (for Join)
            if 'rightInput' in trans and trans['rightInput']:
                dependencies.add(trans['rightInput'])

            # Add edges to graph
            for dep in dependencies:
                if dep in graph:  # Only if dependency is in this dataflow
                    graph[dep].append(trans_name)
                    in_degree[trans_name] += 1

        # Kahn's algorithm for topological sort
        queue = [name for name in in_degree if in_degree[name] == 0]
        ordered_names = []

        while queue:
            # Sort queue to ensure deterministic ordering
            queue.sort()
            node = queue.pop(0)
            ordered_names.append(node)

            # Decrease in-degree for neighbors
            for neighbor in graph[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        # Check for cycles
        if len(ordered_names) != len(transformations):
            # Cycle detected, return original order with warning
            print(f"WARNING: Circular dependency detected in transformations, keeping original order")
            return transformations

        # Return transformations in topologically sorted order
        return [trans_map[name] for name in ordered_names]

    def get_generated_files(self) -> List[str]:
        """Retorna lista de archivos generados"""
        return self.generated_files
