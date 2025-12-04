"""
Validador de mappings de PowerCenter
Realiza validaciones pre-migración para identificar problemas potenciales
"""

import logging
from typing import List, Tuple, Dict, Any
from collections import defaultdict

from .parser import MappingMetadata, Transformation

logger = logging.getLogger('pc-to-adf.validator')


class MappingValidator:
    """
    Valida mappings de PowerCenter antes de migrar a ADF.
    Identifica transformaciones no soportadas, dependencias circulares,
    y otras condiciones problemáticas.
    """

    # Transformaciones completamente soportadas en v2.0
    SUPPORTED_TRANSFORMATIONS = {
        'Source Qualifier',
        'Expression',
        'Filter',
        'Aggregator',
        'Joiner',
        'Sorter',
        'Router',
        'Lookup',
        'Lookup Procedure',
        'Update Strategy'
    }

    # Transformaciones no soportadas
    UNSUPPORTED_TRANSFORMATIONS = {
        'Sequence Generator',
        'Normalizer',
        'Rank',
        'Union',
        'XML Source Qualifier',
        'XML Target',
        'Custom Transformation',
        'Stored Procedure',
        'External Procedure',
        'HTTP Transformation',
        'Java Transformation'
    }

    def __init__(self):
        """Inicializa el validador"""
        self.errors: List[str] = []
        self.warnings: List[str] = []
        self.recommendations: List[str] = []

    def validate(self, metadata: MappingMetadata) -> Tuple[List[str], List[str]]:
        """
        Valida un mapping completo.

        Args:
            metadata: Metadata del mapping parseado

        Returns:
            Tupla de (errors, warnings)
        """
        logger.info(f"Validando mapping: {metadata.name}")

        # Resetear listas
        self.errors = []
        self.warnings = []
        self.recommendations = []

        # Validaciones básicas
        self._validate_structure(metadata)

        # Validar transformaciones
        self._validate_transformations(metadata.transformations)

        # Validar conectores y flujo
        self._validate_flow(metadata)

        # Validar casos especiales
        self._validate_special_cases(metadata)

        logger.info(
            f"Validación completa: {len(self.errors)} errores, "
            f"{len(self.warnings)} warnings, {len(self.recommendations)} recomendaciones"
        )

        return self.errors, self.warnings

    def _validate_structure(self, metadata: MappingMetadata) -> None:
        """Valida estructura básica del mapping"""
        # Verificar que haya al menos un source
        if not metadata.sources:
            self.errors.append("Mapping no tiene sources definidos")

        # Verificar que haya al menos un target
        if not metadata.targets:
            self.errors.append("Mapping no tiene targets definidos")

        # Verificar que haya transformaciones
        if not metadata.transformations:
            self.warnings.append("Mapping no tiene transformaciones")

    def _validate_transformations(self, transformations: List[Transformation]) -> None:
        """Valida cada transformación individualmente"""
        for trans in transformations:
            trans_type = trans.type

            # Verificar si es soportada
            if trans_type in self.UNSUPPORTED_TRANSFORMATIONS:
                self.errors.append(
                    f"Transformación '{trans.name}' de tipo '{trans_type}' no está soportada"
                )
                continue

            if trans_type not in self.SUPPORTED_TRANSFORMATIONS:
                self.warnings.append(
                    f"Transformación '{trans.name}' de tipo '{trans_type}' puede no estar completamente soportada"
                )

            # Validaciones específicas por tipo
            if trans_type == 'Joiner':
                self._validate_joiner(trans)
            elif trans_type == 'Aggregator':
                self._validate_aggregator(trans)
            elif trans_type == 'Lookup' or trans_type == 'Lookup Procedure':
                self._validate_lookup(trans)
            elif trans_type == 'Router':
                self._validate_router(trans)
            elif trans_type == 'Update Strategy':
                self._validate_update_strategy(trans)

    def _validate_joiner(self, trans: Transformation) -> None:
        """Valida Joiner Transformation"""
        join_condition = trans.properties.get('join_condition', '')

        if not join_condition:
            self.errors.append(f"Joiner '{trans.name}' no tiene join condition definida")

        master_fields = trans.properties.get('master_fields', [])
        detail_fields = trans.properties.get('detail_fields', [])

        if not master_fields:
            self.warnings.append(f"Joiner '{trans.name}' no tiene master fields identificados")

        if not detail_fields:
            self.warnings.append(f"Joiner '{trans.name}' no tiene detail fields identificados")

        # Validar sorted input
        sorted_input = trans.properties.get('sorted_input', False)
        if sorted_input:
            self.recommendations.append(
                f"Joiner '{trans.name}' usa Sorted Input. Verificar que exista Sorter upstream."
            )

    def _validate_aggregator(self, trans: Transformation) -> None:
        """Valida Aggregator Transformation"""
        group_by = trans.properties.get('group_by_fields', [])
        aggregates = trans.properties.get('aggregate_expressions', [])

        if not group_by and not aggregates:
            self.errors.append(
                f"Aggregator '{trans.name}' no tiene GROUP BY ni expresiones de agregación"
            )

        # Validar sorted input
        sorted_input = trans.properties.get('sorted_input', False)
        if sorted_input:
            self.recommendations.append(
                f"Aggregator '{trans.name}' usa Sorted Input. Verificar que exista Sorter upstream."
            )

    def _validate_lookup(self, trans: Transformation) -> None:
        """Valida Lookup Transformation"""
        lookup_table = trans.properties.get('lookup_table')
        lookup_condition = trans.properties.get('lookup_condition', '')
        source_type = trans.properties.get('source_type', 'Database')
        sql_override = trans.properties.get('sql_override')

        # CRÍTICO: Solo validar lookup_table para Database lookups, NO para Flat File
        if source_type == 'Flat File':
            # Validar configuración de Flat File
            flat_file_config = trans.properties.get('flat_file')
            if not flat_file_config:
                self.warnings.append(
                    f"Lookup '{trans.name}' usa Flat File pero no tiene configuración de archivo"
                )
            else:
                # Flat File lookup es válido, solo warning informativo
                self.warnings.append(
                    f"Lookup '{trans.name}' uses Flat File. Ensure DelimitedText dataset is configured."
                )
        else:
            # Para Database lookups, requerir lookup_table o sql_override
            if not lookup_table and not sql_override:
                self.errors.append(
                    f"Lookup '{trans.name}' no tiene lookup table ni SQL Override definido"
                )

        if not lookup_condition:
            self.warnings.append(
                f"Lookup '{trans.name}' no tiene lookup condition definida"
            )

        if sql_override:
            self.warnings.append(
                f"Lookup '{trans.name}' usa SQL Override. Revisar compatibilidad con ADF."
            )

    def _validate_router(self, trans: Transformation) -> None:
        """Valida Router Transformation"""
        groups = trans.properties.get('groups', [])

        if not groups:
            self.errors.append(f"Router '{trans.name}' no tiene output groups definidos")

        # Verificar que haya al menos un grupo default
        default_group = trans.properties.get('default_group')
        if not default_group:
            self.warnings.append(
                f"Router '{trans.name}' no tiene default group. Registros no coincidentes pueden perderse."
            )

        # Verificar que cada grupo tenga expresión (excepto default)
        for group in groups:
            if group['type'] == 'output' and not group.get('expression'):
                self.warnings.append(
                    f"Router '{trans.name}' - Group '{group['name']}' no tiene expresión definida"
                )

        # Advertir si hay muchos grupos
        if len(groups) > 10:
            self.recommendations.append(
                f"Router '{trans.name}' tiene {len(groups)} grupos. Considerar simplificar."
            )

    def _validate_update_strategy(self, trans: Transformation) -> None:
        """Valida Update Strategy Transformation"""
        strategy = trans.properties.get('strategy', 'DD_INSERT')

        if strategy == 'DD_REJECT':
            self.warnings.append(
                f"Update Strategy '{trans.name}' usa DD_REJECT. "
                "Considerar usar Router para manejo de errores en ADF."
            )

    def _validate_flow(self, metadata: MappingMetadata) -> None:
        """Valida el flujo de datos y conectores"""
        # Construir grafo de dependencias
        graph = self._build_dependency_graph(metadata)

        # Detectar ciclos
        if self._has_cycles(graph):
            self.errors.append("Mapping tiene dependencias circulares en el flujo")

        # Verificar que todas las transformaciones estén conectadas
        disconnected = self._find_disconnected_transformations(metadata, graph)
        for trans_name in disconnected:
            self.warnings.append(
                f"Transformación '{trans_name}' parece estar desconectada del flujo principal"
            )

    def _build_dependency_graph(self, metadata: MappingMetadata) -> Dict[str, List[str]]:
        """Construye grafo de dependencias entre transformaciones"""
        graph = defaultdict(list)

        for connector in metadata.connectors:
            from_instance = connector.from_instance
            to_instance = connector.to_instance
            graph[from_instance].append(to_instance)

        return graph

    def _has_cycles(self, graph: Dict[str, List[str]]) -> bool:
        """Detecta ciclos en el grafo de dependencias (DFS)"""
        visited = set()
        rec_stack = set()

        def visit(node):
            if node in rec_stack:
                return True
            if node in visited:
                return False

            visited.add(node)
            rec_stack.add(node)

            for neighbor in graph.get(node, []):
                if visit(neighbor):
                    return True

            rec_stack.remove(node)
            return False

        for node in graph:
            if visit(node):
                return True

        return False

    def _find_disconnected_transformations(
        self, metadata: MappingMetadata, graph: Dict[str, List[str]]
    ) -> List[str]:
        """Encuentra transformaciones no conectadas al flujo"""
        all_trans_names = {t.name for t in metadata.transformations}
        connected = set()

        # Agregar todas las transformaciones en el grafo
        for from_node in graph:
            connected.add(from_node)
            for to_node in graph[from_node]:
                connected.add(to_node)

        # También agregar sources y targets como conectados
        for source in metadata.sources:
            connected.add(source.name)
        for target in metadata.targets:
            connected.add(target.name)

        # Retornar transformaciones no conectadas
        return list(all_trans_names - connected)

    def _validate_special_cases(self, metadata: MappingMetadata) -> None:
        """Valida casos especiales y patrones conocidos"""
        # Detectar Sorter → Aggregator con Sorted Input
        self._check_sorted_input_pattern(metadata)

        # Detectar múltiples Lookups encadenados
        self._check_chained_lookups(metadata)

    def _check_sorted_input_pattern(self, metadata: MappingMetadata) -> None:
        """Verifica patrón Sorter → Aggregator/Joiner con Sorted Input"""
        graph = self._build_dependency_graph(metadata)
        trans_by_name = {t.name: t for t in metadata.transformations}

        for trans in metadata.transformations:
            if trans.type in ('Aggregator', 'Joiner'):
                sorted_input = trans.properties.get('sorted_input', False)
                if sorted_input:
                    # Buscar Sorter upstream
                    has_sorter_upstream = False
                    for from_node, to_nodes in graph.items():
                        if trans.name in to_nodes:
                            upstream_trans = trans_by_name.get(from_node)
                            if upstream_trans and upstream_trans.type == 'Sorter':
                                has_sorter_upstream = True
                                break

                    if not has_sorter_upstream:
                        self.warnings.append(
                            f"{trans.type} '{trans.name}' tiene Sorted Input habilitado "
                            "pero no se detectó Sorter upstream"
                        )

    def _check_chained_lookups(self, metadata: MappingMetadata) -> None:
        """Detecta Lookups encadenados que pueden afectar performance"""
        graph = self._build_dependency_graph(metadata)
        trans_by_name = {t.name: t for t in metadata.transformations}

        lookup_chains = []
        for from_node, to_nodes in graph.items():
            from_trans = trans_by_name.get(from_node)
            if from_trans and from_trans.type in ('Lookup', 'Lookup Procedure'):
                for to_node in to_nodes:
                    to_trans = trans_by_name.get(to_node)
                    if to_trans and to_trans.type in ('Lookup', 'Lookup Procedure'):
                        lookup_chains.append((from_node, to_node))

        if len(lookup_chains) > 2:
            self.recommendations.append(
                f"Detectados {len(lookup_chains)} lookups encadenados. "
                "Considerar combinar queries para mejor performance."
            )

    def get_recommendations(self) -> List[str]:
        """Retorna lista de recomendaciones"""
        return self.recommendations

    def get_validation_summary(self) -> Dict[str, Any]:
        """Retorna resumen de validación"""
        return {
            'total_errors': len(self.errors),
            'total_warnings': len(self.warnings),
            'total_recommendations': len(self.recommendations),
            'errors': self.errors,
            'warnings': self.warnings,
            'recommendations': self.recommendations,
            'is_valid': len(self.errors) == 0
        }
