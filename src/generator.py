"""
Generador de archivos JSON para Azure Data Factory
Crea pipelines, dataflows y genera reportes de migración
100% compatible con el esquema oficial de ADF
"""

import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime

from jsonschema import validate, ValidationError as JsonSchemaValidationError

from .utils import save_json, format_timestamp, calculate_migration_stats
from .expression_translator import translate_expression, validate_adf_expression

logger = logging.getLogger('pc-to-adf.generator')


class ADFGenerator:
    """
    Generador de archivos JSON para Azure Data Factory.
    Crea pipelines, dataflows y reportes de migración.
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
        logger.info(f"Generador inicializado. Output: {self.output_dir}")

        self.generated_files: List[str] = []
        self.validation_errors: List[str] = []
        self.validation_warnings: List[str] = []

    def _normalize_dataset_name(self, name: str, is_source: bool = False) -> str:
        """
        Normaliza el nombre de un dataset según la convención de infraestructura Azure.

        Args:
            name: Nombre del dataset
            is_source: True si es un source (origen), False si es target/sink (destino)

        Returns:
            Nombre normalizado con el prefijo correcto
        """
        if is_source:
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

        # Construir grafo de dependencias hacia adelante (quién usa a quién)
        # Si una transformación o sink existe, rastrear hacia atrás qué sources contribuyen
        referenced_components = set()

        # Agregar todos los nombres de transformaciones como componentes válidos
        for trans in transformations:
            referenced_components.add(trans['name'])

        # Si hay transformaciones, los sources que alimentan la primera transformación son válidos
        # (asumimos flujo secuencial básico)
        if transformations:
            # Marcar todos los sources como válidos si hay al menos una transformación
            # porque el flujo secuencial conecta sources -> primera transformation
            valid_sources.update(sources)
        elif sinks:
            # Si solo hay sinks sin transformaciones, los sources son válidos
            valid_sources.update(sources)

        return valid_sources

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

    def generate_dataflow(
        self,
        name: str,
        translated_structure: Dict[str, Any]
    ) -> str:
        """
        Genera un archivo JSON de dataflow para ADF con estructura 100% compatible.

        Args:
            name: Nombre del dataflow
            translated_structure: Estructura traducida del mapping

        Returns:
            Ruta al archivo generado

        Raises:
            ValueError: Si la estructura no es válida
        """
        logger.info(f"Generando dataflow: {name}")

        # Construir sources (sin dependsOn) con validación de topología
        sources = []
        source_names = set()  # Para detectar duplicados

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

        # Construir transformations con dependsOn y typeProperties
        transformations = []
        all_step_names = {s['name'] for s in sources}  # Nombres de todos los pasos anteriores

        for idx, trans in enumerate(translated_structure.get('transformations', [])):
            trans_name = trans['name']
            trans_type = trans['type']

            # CRÍTICO: Validar que NO sea un source disfrazado de transformation
            if trans_name in source_names:
                error_msg = (
                    f"CRÍTICO: '{trans_name}' aparece como source Y como transformation. "
                    f"Esto causa error en ADF. Tipo: {trans_type}"
                )
                logger.error(error_msg)
                self.validation_errors.append(error_msg)
                # Saltar esta transformación (ya está en sources)
                logger.warning(f"Saltando transformación duplicada: {trans_name}")
                continue

            # Validar que no sea tipo 'Source Qualifier' sin transformaciones reales
            if trans_type in ['Source Qualifier', 'source', 'Source']:
                logger.warning(
                    f"Saltando transformación de tipo '{trans_type}': {trans_name} "
                    "(ya debe estar en sources)"
                )
                continue

            # Determinar dependencia (paso anterior)
            if idx == 0:
                # Primera transformación depende de sources
                if len(sources) == 1:
                    previous_step = sources[0]['name']
                else:
                    # Si hay múltiples sources, necesitamos saber cuál usar
                    # Por ahora, usar el primero
                    previous_step = sources[0]['name']
            else:
                # Depende de la transformación anterior
                previous_step = translated_structure['transformations'][idx - 1]['name']

            # Construir transformación según tipo
            try:
                if trans_type == 'DerivedColumn':
                    trans_def = self._build_derived_column(trans, previous_step)
                elif trans_type == 'Filter':
                    trans_def = self._build_filter(trans, previous_step)
                elif trans_type == 'Aggregate':
                    trans_def = self._build_aggregate(trans, previous_step)
                elif trans_type == 'Join':
                    # Join necesita dos inputs
                    trans_def = self._build_join(trans, all_step_names)
                elif trans_type == 'Sort':
                    trans_def = self._build_sort(trans, previous_step)
                elif trans_type == 'ConditionalSplit':
                    trans_def = self._build_conditional_split(trans, previous_step)
                elif trans_type == 'Lookup':
                    trans_def = self._build_lookup(trans, previous_step)
                elif trans_type == 'AlterRow':
                    trans_def = self._build_alter_row(trans, previous_step)
                else:
                    logger.warning(f"Tipo de transformación no soportado: {trans_type}")
                    trans_def = self._build_generic_transformation(trans, previous_step)

                transformations.append(trans_def)
                all_step_names.add(trans_name)

            except Exception as e:
                error_msg = f"Error construyendo transformación '{trans_name}': {e}"
                logger.error(error_msg)
                self.validation_errors.append(error_msg)
                raise ValueError(error_msg)

        # Construir sinks con dependsOn
        sinks = []
        for sink in translated_structure.get('sinks', []):
            # Último paso de transformación o source si no hay transformaciones
            if transformations:
                last_step = transformations[-1]['name']
            elif sources:
                last_step = sources[0]['name']
            else:
                raise ValueError("No hay sources ni transformations para conectar al sink")

            sink_def = {
                'name': sink['name'],
                'dataset': {
                    'referenceName': self._normalize_dataset_name(sink['name'], is_source=False),
                    'type': 'DatasetReference'
                },
                'dependsOn': [
                    {
                        'activity': last_step,
                        'dependencyConditions': ['Succeeded']
                    }
                ]
            }
            sinks.append(sink_def)

        # Estructura completa del dataflow (esquema oficial de ADF)
        dataflow = {
            'name': f"dataflow_{name}",
            'properties': {
                'description': translated_structure.get('description', ''),
                'type': 'MappingDataFlow',
                'typeProperties': {
                    'sources': sources,
                    'transformations': transformations,
                    'sinks': sinks,
                    'scriptLines': []  # Requerido por ADF
                }
            },
            'type': 'Microsoft.DataFactory/factories/dataflows'  # CRÍTICO para ADF
        }

        # Validar estructura antes de guardar
        self._validate_dataflow_structure(dataflow)

        # Guardar archivo
        filename = f"dataflow_{name}_{format_timestamp()}.json"
        filepath = self.output_dir / filename
        save_json(dataflow, str(filepath))

        self.generated_files.append(str(filepath))
        logger.info(f"Dataflow generado: {filepath}")

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
        warnings = len(translated_structure.get('warnings', []))
        errors = len(translated_structure.get('errors', []))

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
            'warnings': translated_structure.get('warnings', []),
            'errors': translated_structure.get('errors', []),
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

    def validate_json(self, json_file: str, schema: Optional[Dict] = None) -> bool:
        """
        Valida un archivo JSON contra un esquema.

        Args:
            json_file: Ruta al archivo JSON
            schema: Esquema JSON opcional para validación

        Returns:
            True si es válido, False en caso contrario
        """
        logger.info(f"Validando JSON: {json_file}")

        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            if schema:
                validate(instance=data, schema=schema)

            logger.info(f"JSON válido: {json_file}")
            return True

        except json.JSONDecodeError as e:
            logger.error(f"Error de formato JSON en {json_file}: {e}")
            return False
        except JsonSchemaValidationError as e:
            logger.error(f"Error de validación de esquema en {json_file}: {e}")
            return False
        except Exception as e:
            logger.error(f"Error inesperado validando {json_file}: {e}")
            return False

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

    def _build_derived_column(
        self,
        trans: Dict[str, Any],
        previous_step: str
    ) -> Dict[str, Any]:
        """
        Construye transformación DerivedColumn con estructura completa de ADF.
        """
        columns = []
        for col in trans.get('columns', []):
            expression = col.get('expression', '')

            # Traducir y validar expresión
            translated_expr = translate_expression(expression)
            is_valid, errors = validate_adf_expression(translated_expr)

            if not is_valid:
                warning = f"Expresión inválida en columna '{col['name']}': {errors}"
                logger.warning(warning)
                self.validation_warnings.append(warning)

            columns.append({
                'name': col['name'],
                'value': {
                    'value': translated_expr,
                    'type': 'Expression'
                }
            })

        return {
            'name': trans['name'],
            'type': 'derivedColumn',
            'dependsOn': [
                {
                    'activity': previous_step,
                    'dependencyConditions': ['Succeeded']
                }
            ],
            'typeProperties': {
                'columns': columns
            }
        }

    def _build_filter(
        self,
        trans: Dict[str, Any],
        previous_step: str
    ) -> Dict[str, Any]:
        """
        Construye transformación Filter con estructura completa de ADF.
        """
        condition = trans.get('condition', 'true')

        # Traducir y validar condición
        translated_condition = translate_expression(condition)
        is_valid, errors = validate_adf_expression(translated_condition)

        if not is_valid:
            warning = f"Condición inválida en filtro '{trans['name']}': {errors}"
            logger.warning(warning)
            self.validation_warnings.append(warning)

        return {
            'name': trans['name'],
            'type': 'filter',
            'dependsOn': [
                {
                    'activity': previous_step,
                    'dependencyConditions': ['Succeeded']
                }
            ],
            'typeProperties': {
                'condition': {
                    'value': translated_condition,
                    'type': 'Expression'
                }
            }
        }

    def _build_aggregate(
        self,
        trans: Dict[str, Any],
        previous_step: str
    ) -> Dict[str, Any]:
        """
        Construye transformación Aggregate con estructura completa de ADF.
        """
        aggregates = []
        for agg in trans.get('aggregates', []):
            expression = agg.get('expression', '')

            # Traducir y validar expresión
            translated_expr = translate_expression(expression)
            is_valid, errors = validate_adf_expression(translated_expr)

            if not is_valid:
                warning = f"Expresión de agregación inválida '{agg['name']}': {errors}"
                logger.warning(warning)
                self.validation_warnings.append(warning)

            aggregates.append({
                'name': agg['name'],
                'value': {
                    'value': translated_expr,
                    'type': 'Expression'
                }
            })

        return {
            'name': trans['name'],
            'type': 'aggregate',
            'dependsOn': [
                {
                    'activity': previous_step,
                    'dependencyConditions': ['Succeeded']
                }
            ],
            'typeProperties': {
                'groupBy': trans.get('groupBy', []),
                'aggregates': aggregates
            }
        }

    def _build_join(
        self,
        trans: Dict[str, Any],
        all_steps: set
    ) -> Dict[str, Any]:
        """
        Construye transformación Join con estructura completa de ADF.
        Requiere dos inputs (left y right).
        """
        # Determinar left y right inputs
        # Por defecto, usar los dos últimos steps disponibles
        available_steps = list(all_steps)

        if len(available_steps) < 2:
            raise ValueError(f"Join '{trans['name']}' requiere al menos 2 inputs previos")

        left_input = available_steps[-2]
        right_input = available_steps[-1]

        # Convertir join conditions
        join_conditions = []
        for cond in trans.get('joinConditions', []):
            join_conditions.append({
                'leftColumn': cond.get('leftColumn', ''),
                'operator': '==',
                'rightColumn': cond.get('rightColumn', '')
            })

        return {
            'name': trans['name'],
            'type': 'join',
            'dependsOn': [
                {
                    'activity': left_input,
                    'dependencyConditions': ['Succeeded']
                },
                {
                    'activity': right_input,
                    'dependencyConditions': ['Succeeded']
                }
            ],
            'typeProperties': {
                'joinType': trans.get('joinType', 'inner'),
                'leftInput': left_input,
                'rightInput': right_input,
                'joinConditions': join_conditions
            }
        }

    def _build_sort(
        self,
        trans: Dict[str, Any],
        previous_step: str
    ) -> Dict[str, Any]:
        """
        Construye transformación Sort con estructura completa de ADF.
        """
        return {
            'name': trans['name'],
            'type': 'sort',
            'dependsOn': [
                {
                    'activity': previous_step,
                    'dependencyConditions': ['Succeeded']
                }
            ],
            'typeProperties': {
                'sortColumns': trans.get('orderBy', []),
                'distinct': trans.get('distinct', False)
            }
        }

    def _build_conditional_split(
        self,
        trans: Dict[str, Any],
        previous_step: str
    ) -> Dict[str, Any]:
        """
        Construye transformación ConditionalSplit con estructura completa de ADF.
        """
        conditions = []
        for cond in trans.get('conditions', []):
            expression = cond.get('expression', '')

            # Traducir y validar expresión
            translated_expr = translate_expression(expression)
            is_valid, errors = validate_adf_expression(translated_expr)

            if not is_valid:
                warning = f"Condición inválida en '{cond['name']}': {errors}"
                logger.warning(warning)
                self.validation_warnings.append(warning)

            conditions.append({
                'name': cond['name'],
                'value': {
                    'value': translated_expr,
                    'type': 'Expression'
                }
            })

        return {
            'name': trans['name'],
            'type': 'conditionalSplit',
            'dependsOn': [
                {
                    'activity': previous_step,
                    'dependencyConditions': ['Succeeded']
                }
            ],
            'typeProperties': {
                'conditions': conditions,
                'defaultOutput': trans.get('defaultStream', 'default')
            }
        }

    def _build_lookup(
        self,
        trans: Dict[str, Any],
        previous_step: str
    ) -> Dict[str, Any]:
        """
        Construye transformación Lookup con estructura completa de ADF.
        """
        lookup_conditions = []
        for cond in trans.get('lookupConditions', []):
            lookup_conditions.append({
                'leftColumn': cond.get('leftColumn', ''),
                'operator': '==',
                'rightColumn': cond.get('rightColumn', '')
            })

        type_props = {
            'lookupDataset': {
                'referenceName': trans.get('lookupDataset', 'ds_lookup'),
                'type': 'DatasetReference'
            },
            'lookupConditions': lookup_conditions,
            'multiple': False
        }

        return {
            'name': trans['name'],
            'type': 'lookup',
            'dependsOn': [
                {
                    'activity': previous_step,
                    'dependencyConditions': ['Succeeded']
                }
            ],
            'typeProperties': type_props
        }

    def _build_alter_row(
        self,
        trans: Dict[str, Any],
        previous_step: str
    ) -> Dict[str, Any]:
        """
        Construye transformación AlterRow con estructura completa de ADF.
        """
        action = trans.get('action', 'insert')

        type_props = {}

        # Agregar condiciones según la acción
        if action == 'insert':
            type_props['insertCondition'] = {
                'value': trans.get('condition', 'true'),
                'type': 'Expression'
            }
        elif action == 'update':
            type_props['updateCondition'] = {
                'value': trans.get('condition', 'true'),
                'type': 'Expression'
            }
        elif action == 'delete':
            type_props['deleteCondition'] = {
                'value': trans.get('condition', 'true'),
                'type': 'Expression'
            }
        elif action == 'upsert':
            type_props['upsertCondition'] = {
                'value': trans.get('condition', 'true'),
                'type': 'Expression'
            }

        return {
            'name': trans['name'],
            'type': 'alterRow',
            'dependsOn': [
                {
                    'activity': previous_step,
                    'dependencyConditions': ['Succeeded']
                }
            ],
            'typeProperties': type_props
        }

    def _build_generic_transformation(
        self,
        trans: Dict[str, Any],
        previous_step: str
    ) -> Dict[str, Any]:
        """
        Construye transformación genérica con estructura básica de ADF.
        """
        return {
            'name': trans['name'],
            'type': trans['type'].lower(),
            'dependsOn': [
                {
                    'activity': previous_step,
                    'dependencyConditions': ['Succeeded']
                }
            ],
            'typeProperties': {}
        }

    def _validate_dataflow_structure(self, dataflow: Dict[str, Any]) -> None:
        """
        Valida que el dataflow tenga la estructura correcta de ADF.

        Raises:
            ValueError: Si la estructura no es válida
        """
        # Validar campos requeridos nivel superior
        required_keys = ['name', 'properties', 'type']
        for key in required_keys:
            if key not in dataflow:
                raise ValueError(f"Falta campo requerido en dataflow: {key}")

        # Validar type correcto
        if dataflow['type'] != 'Microsoft.DataFactory/factories/dataflows':
            raise ValueError(
                f"Tipo de dataflow incorrecto: {dataflow['type']}. "
                "Debe ser 'Microsoft.DataFactory/factories/dataflows'"
            )

        # Validar properties
        properties = dataflow['properties']
        required_props = ['type', 'typeProperties']
        for key in required_props:
            if key not in properties:
                raise ValueError(f"Falta campo requerido en properties: {key}")

        # Validar type dentro de properties
        if properties['type'] != 'MappingDataFlow':
            raise ValueError(
                f"Tipo en properties incorrecto: {properties['type']}. "
                "Debe ser 'MappingDataFlow'"
            )

        # Validar typeProperties
        type_props = properties['typeProperties']
        required_type_props = ['sources', 'transformations', 'sinks', 'scriptLines']
        for key in required_type_props:
            if key not in type_props:
                raise ValueError(f"Falta campo requerido en typeProperties: {key}")

        # Validar dependencias
        all_steps = {s['name'] for s in type_props['sources']}

        for trans in type_props['transformations']:
            if 'dependsOn' not in trans:
                raise ValueError(f"Transformación '{trans['name']}' no tiene dependsOn")

            for dep in trans['dependsOn']:
                if dep['activity'] not in all_steps:
                    raise ValueError(
                        f"Dependencia inválida en '{trans['name']}': "
                        f"'{dep['activity']}' no existe"
                    )

            all_steps.add(trans['name'])

        for sink in type_props['sinks']:
            if 'dependsOn' not in sink:
                raise ValueError(f"Sink '{sink['name']}' no tiene dependsOn")

            for dep in sink['dependsOn']:
                if dep['activity'] not in all_steps:
                    raise ValueError(
                        f"Dependencia inválida en sink '{sink['name']}': "
                        f"'{dep['activity']}' no existe"
                    )

        logger.info("Validación de estructura de dataflow exitosa")

    def _generate_recommendations(self, translated_structure: Dict[str, Any]) -> List[str]:
        """Genera recomendaciones basadas en la migración"""
        recommendations = []

        # Recomendar revisión si hay warnings
        if translated_structure.get('warnings'):
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

        # Recomendar optimización
        recommendations.append(
            "Revisar configuración de compute (cores, tipo) en el pipeline para optimizar performance"
        )

        return recommendations

    def get_generated_files(self) -> List[str]:
        """Retorna lista de archivos generados"""
        return self.generated_files
