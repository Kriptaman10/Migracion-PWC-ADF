"""
Generador de archivos JSON para Azure Data Factory
Crea pipelines, dataflows y genera reportes de migración
"""

import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime

from jsonschema import validate, ValidationError as JsonSchemaValidationError

from .utils import save_json, format_timestamp, calculate_migration_stats

logger = logging.getLogger('pc-to-adf.generator')


class ADFGenerator:
    """
    Generador de archivos JSON para Azure Data Factory.
    Crea pipelines, dataflows y reportes de migración.
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
        Genera un archivo JSON de dataflow para ADF.

        Args:
            name: Nombre del dataflow
            translated_structure: Estructura traducida del mapping

        Returns:
            Ruta al archivo generado
        """
        logger.info(f"Generando dataflow: {name}")

        # Construir sources
        sources = []
        for source in translated_structure.get('sources', []):
            sources.append({
                'name': source['name'],
                'type': 'source',
                'dataset': {
                    'referenceName': f"ds_{source['name']}",
                    'type': 'DatasetReference'
                },
                'schema': source.get('schema', [])
            })

        # Construir transformations
        transformations = []
        for trans in translated_structure.get('transformations', []):
            trans_def = {
                'name': trans['name'],
                'type': trans['type'].lower()
            }

            # Agregar propiedades específicas según el tipo
            trans_type = trans['type']

            if trans_type == 'DerivedColumn':
                trans_def['columns'] = trans.get('columns', [])

            elif trans_type == 'Filter':
                trans_def['condition'] = {
                    'value': trans.get('condition', 'true'),
                    'type': 'Expression'
                }

            elif trans_type == 'Aggregate':
                trans_def['groupBy'] = trans.get('groupBy', [])
                trans_def['aggregates'] = trans.get('aggregates', [])
                if trans.get('sorted_input'):
                    trans_def['sortedInput'] = True

            elif trans_type == 'Join':
                trans_def['joinType'] = trans.get('joinType', 'inner')
                trans_def['joinConditions'] = trans.get('joinConditions', [])
                trans_def['leftStream'] = trans.get('masterFields', [])
                trans_def['rightStream'] = trans.get('detailFields', [])
                if trans.get('sorted_input'):
                    trans_def['sortedInput'] = True
                if trans.get('broadcast'):
                    trans_def['broadcast'] = trans.get('broadcast')

            elif trans_type == 'Sort':
                trans_def['orderBy'] = trans.get('orderBy', [])
                trans_def['distinct'] = trans.get('distinct', False)

            elif trans_type == 'ConditionalSplit':
                trans_def['conditions'] = trans.get('conditions', [])
                trans_def['defaultStream'] = trans.get('defaultStream')

            elif trans_type == 'Lookup':
                trans_def['lookupDataset'] = trans.get('lookupDataset')
                trans_def['lookupConditions'] = trans.get('lookupConditions', [])
                trans_def['returnFields'] = trans.get('returnFields', [])
                trans_def['cacheMode'] = trans.get('cacheMode', 'static')
                if trans.get('sqlOverride'):
                    trans_def['sqlOverride'] = trans.get('sqlOverride')
                if trans.get('sourceType') == 'DelimitedText':
                    trans_def['sourceType'] = 'DelimitedText'
                    trans_def['flatFileConfig'] = trans.get('flatFileConfig', {})

            elif trans_type == 'AlterRow':
                trans_def['action'] = trans.get('action', 'insert')
                if trans.get('condition'):
                    trans_def['condition'] = {
                        'value': trans.get('condition'),
                        'type': 'Expression'
                    }

            transformations.append(trans_def)

        # Construir sinks
        sinks = []
        for sink in translated_structure.get('sinks', []):
            sinks.append({
                'name': sink['name'],
                'type': 'sink',
                'dataset': {
                    'referenceName': f"ds_{sink['name']}",
                    'type': 'DatasetReference'
                },
                'schema': sink.get('schema', [])
            })

        # Estructura completa del dataflow
        dataflow = {
            'name': f"dataflow_{name}",
            'properties': {
                'description': translated_structure.get('description', ''),
                'type': 'MappingDataFlow',
                'typeProperties': {
                    'sources': sources,
                    'transformations': transformations,
                    'sinks': sinks
                },
                'annotations': [
                    f"Migrado desde PowerCenter - {datetime.now().strftime('%Y-%m-%d')}"
                ]
            }
        }

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
