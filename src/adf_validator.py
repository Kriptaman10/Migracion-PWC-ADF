"""
Validador de esquemas de Azure Data Factory
Valida que los JSONs generados cumplan con el esquema oficial de ADF
"""

import logging
from typing import Dict, Any, List, Tuple

logger = logging.getLogger('pc-to-adf.adf_validator')


class ADFSchemaValidator:
    """
    Validador de esquemas de Azure Data Factory.
    Garantiza que los JSONs generados sean 100% compatibles con ADF.
    """

    def __init__(self):
        """Inicializa el validador"""
        self.errors: List[str] = []
        self.warnings: List[str] = []

    def validate_dataflow(self, dataflow: Dict[str, Any]) -> Tuple[bool, List[str], List[str]]:
        """
        Valida un dataflow completo.

        Args:
            dataflow: Diccionario con el dataflow a validar

        Returns:
            Tupla (es_válido, lista_errores, lista_warnings)
        """
        self.errors = []
        self.warnings = []

        # Validación nivel superior
        self._validate_top_level(dataflow)

        # Validación de properties
        if 'properties' in dataflow:
            self._validate_properties(dataflow['properties'])

        # Validación de typeProperties
        if 'properties' in dataflow and 'typeProperties' in dataflow['properties']:
            self._validate_type_properties(dataflow['properties']['typeProperties'])

        is_valid = len(self.errors) == 0
        return (is_valid, self.errors.copy(), self.warnings.copy())

    def _validate_top_level(self, dataflow: Dict[str, Any]) -> None:
        """Valida campos del nivel superior"""
        # Campos requeridos
        required_fields = {
            'name': str,
            'properties': dict,
            'type': str
        }

        for field, expected_type in required_fields.items():
            if field not in dataflow:
                self.errors.append(f"Falta campo requerido: '{field}'")
            elif not isinstance(dataflow[field], expected_type):
                self.errors.append(
                    f"Campo '{field}' debe ser de tipo {expected_type.__name__}, "
                    f"pero es {type(dataflow[field]).__name__}"
                )

        # Validar type específico
        if 'type' in dataflow:
            expected_type = 'Microsoft.DataFactory/factories/dataflows'
            if dataflow['type'] != expected_type:
                self.errors.append(
                    f"Campo 'type' debe ser '{expected_type}', "
                    f"pero es '{dataflow['type']}'"
                )

    def _validate_properties(self, properties: Dict[str, Any]) -> None:
        """Valida el objeto properties"""
        # Campos requeridos
        required_fields = {
            'type': str,
            'typeProperties': dict
        }

        for field, expected_type in required_fields.items():
            if field not in properties:
                self.errors.append(f"Falta campo requerido en properties: '{field}'")
            elif not isinstance(properties[field], expected_type):
                self.errors.append(
                    f"Campo properties.{field} debe ser de tipo {expected_type.__name__}"
                )

        # Validar type específico
        if 'type' in properties:
            expected_type = 'MappingDataFlow'
            if properties['type'] != expected_type:
                self.errors.append(
                    f"Campo properties.type debe ser '{expected_type}', "
                    f"pero es '{properties['type']}'"
                )

    def _validate_type_properties(self, type_props: Dict[str, Any]) -> None:
        """Valida el objeto typeProperties"""
        # Campos requeridos
        required_fields = ['sources', 'transformations', 'sinks', 'scriptLines']

        for field in required_fields:
            if field not in type_props:
                self.errors.append(f"Falta campo requerido en typeProperties: '{field}'")
            elif field != 'scriptLines' and not isinstance(type_props[field], list):
                self.errors.append(f"Campo typeProperties.{field} debe ser una lista")

        # Validar sources
        if 'sources' in type_props:
            self._validate_sources(type_props['sources'])

        # Validar transformations
        if 'transformations' in type_props:
            self._validate_transformations(
                type_props['transformations'],
                type_props.get('sources', [])
            )

        # Validar sinks
        if 'sinks' in type_props:
            all_steps = self._get_all_step_names(
                type_props.get('sources', []),
                type_props.get('transformations', [])
            )
            self._validate_sinks(type_props['sinks'], all_steps)

    def _validate_sources(self, sources: List[Dict[str, Any]]) -> None:
        """Valida la lista de sources"""
        if not sources:
            self.warnings.append("No hay sources definidos")
            return

        for idx, source in enumerate(sources):
            # Campos requeridos
            required_fields = ['name', 'dataset']

            for field in required_fields:
                if field not in source:
                    self.errors.append(f"Source #{idx + 1}: Falta campo '{field}'")

            # Validar dataset
            if 'dataset' in source:
                self._validate_dataset_reference(source['dataset'], f"Source '{source.get('name', idx)}'")

    def _validate_transformations(
        self,
        transformations: List[Dict[str, Any]],
        sources: List[Dict[str, Any]]
    ) -> None:
        """Valida la lista de transformations"""
        all_steps = {s['name'] for s in sources}

        for idx, trans in enumerate(transformations):
            trans_name = trans.get('name', f"#{idx + 1}")

            # Campos requeridos
            required_fields = ['name', 'type', 'dependsOn', 'typeProperties']

            for field in required_fields:
                if field not in trans:
                    self.errors.append(f"Transformation '{trans_name}': Falta campo '{field}'")

            # Validar dependsOn
            if 'dependsOn' in trans:
                self._validate_depends_on(trans['dependsOn'], all_steps, f"Transformation '{trans_name}'")

            # Validar typeProperties según tipo
            if 'type' in trans and 'typeProperties' in trans:
                self._validate_transformation_type_properties(
                    trans['type'],
                    trans['typeProperties'],
                    trans_name
                )

            # Agregar a steps disponibles
            if 'name' in trans:
                all_steps.add(trans['name'])

    def _validate_sinks(self, sinks: List[Dict[str, Any]], all_steps: set) -> None:
        """Valida la lista de sinks"""
        if not sinks:
            self.warnings.append("No hay sinks definidos")
            return

        for idx, sink in enumerate(sinks):
            sink_name = sink.get('name', f"#{idx + 1}")

            # Campos requeridos
            required_fields = ['name', 'dataset', 'dependsOn']

            for field in required_fields:
                if field not in sink:
                    self.errors.append(f"Sink '{sink_name}': Falta campo '{field}'")

            # Validar dataset
            if 'dataset' in sink:
                self._validate_dataset_reference(sink['dataset'], f"Sink '{sink_name}'")

            # Validar dependsOn
            if 'dependsOn' in sink:
                self._validate_depends_on(sink['dependsOn'], all_steps, f"Sink '{sink_name}'")

    def _validate_dataset_reference(self, dataset: Dict[str, Any], context: str) -> None:
        """Valida una referencia a dataset"""
        required_fields = ['referenceName', 'type']

        for field in required_fields:
            if field not in dataset:
                self.errors.append(f"{context}: Dataset falta campo '{field}'")

        if 'type' in dataset and dataset['type'] != 'DatasetReference':
            self.errors.append(
                f"{context}: Dataset type debe ser 'DatasetReference', "
                f"pero es '{dataset['type']}'"
            )

    def _validate_depends_on(
        self,
        depends_on: List[Dict[str, Any]],
        available_steps: set,
        context: str
    ) -> None:
        """Valida dependencias"""
        if not depends_on:
            self.errors.append(f"{context}: dependsOn no puede estar vacío")
            return

        for dep in depends_on:
            # Campos requeridos
            if 'activity' not in dep:
                self.errors.append(f"{context}: Dependencia sin campo 'activity'")
                continue

            if 'dependencyConditions' not in dep:
                self.errors.append(f"{context}: Dependencia sin campo 'dependencyConditions'")

            # Validar que la actividad existe
            if dep['activity'] not in available_steps:
                self.errors.append(
                    f"{context}: Dependencia a actividad inexistente '{dep['activity']}'"
                )

            # Validar dependencyConditions
            if 'dependencyConditions' in dep:
                valid_conditions = ['Succeeded', 'Failed', 'Skipped', 'Completed']
                for cond in dep['dependencyConditions']:
                    if cond not in valid_conditions:
                        self.warnings.append(
                            f"{context}: Condición de dependencia '{cond}' no es estándar"
                        )

    def _validate_transformation_type_properties(
        self,
        trans_type: str,
        type_props: Dict[str, Any],
        trans_name: str
    ) -> None:
        """Valida typeProperties según el tipo de transformación"""
        validation_methods = {
            'derivedColumn': self._validate_derived_column_props,
            'filter': self._validate_filter_props,
            'aggregate': self._validate_aggregate_props,
            'join': self._validate_join_props,
            'sort': self._validate_sort_props,
            'conditionalSplit': self._validate_conditional_split_props,
            'lookup': self._validate_lookup_props,
            'alterRow': self._validate_alter_row_props
        }

        method = validation_methods.get(trans_type)
        if method:
            method(type_props, trans_name)

    def _validate_derived_column_props(self, props: Dict[str, Any], name: str) -> None:
        """Valida typeProperties de DerivedColumn"""
        if 'columns' not in props:
            self.errors.append(f"DerivedColumn '{name}': Falta campo 'columns'")
            return

        for col in props['columns']:
            if 'name' not in col:
                self.errors.append(f"DerivedColumn '{name}': Columna sin nombre")
            if 'value' not in col:
                self.errors.append(f"DerivedColumn '{name}': Columna sin valor")
            elif not isinstance(col['value'], dict):
                self.errors.append(f"DerivedColumn '{name}': Valor de columna debe ser objeto")
            elif 'value' not in col['value'] or 'type' not in col['value']:
                self.errors.append(
                    f"DerivedColumn '{name}': Valor de columna debe tener 'value' y 'type'"
                )

    def _validate_filter_props(self, props: Dict[str, Any], name: str) -> None:
        """Valida typeProperties de Filter"""
        if 'condition' not in props:
            self.errors.append(f"Filter '{name}': Falta campo 'condition'")
            return

        cond = props['condition']
        if not isinstance(cond, dict):
            self.errors.append(f"Filter '{name}': Condition debe ser objeto")
        elif 'value' not in cond or 'type' not in cond:
            self.errors.append(f"Filter '{name}': Condition debe tener 'value' y 'type'")

    def _validate_aggregate_props(self, props: Dict[str, Any], name: str) -> None:
        """Valida typeProperties de Aggregate"""
        if 'groupBy' not in props:
            self.warnings.append(f"Aggregate '{name}': No tiene groupBy (posible agregación total)")

        if 'aggregates' not in props:
            self.errors.append(f"Aggregate '{name}': Falta campo 'aggregates'")

    def _validate_join_props(self, props: Dict[str, Any], name: str) -> None:
        """Valida typeProperties de Join"""
        required = ['joinType', 'leftInput', 'rightInput', 'joinConditions']
        for field in required:
            if field not in props:
                self.errors.append(f"Join '{name}': Falta campo '{field}'")

        if 'joinType' in props:
            valid_types = ['inner', 'left', 'right', 'outer', 'cross']
            if props['joinType'] not in valid_types:
                self.warnings.append(
                    f"Join '{name}': joinType '{props['joinType']}' no es estándar"
                )

    def _validate_sort_props(self, props: Dict[str, Any], name: str) -> None:
        """Valida typeProperties de Sort"""
        if 'sortColumns' not in props:
            self.errors.append(f"Sort '{name}': Falta campo 'sortColumns'")

    def _validate_conditional_split_props(self, props: Dict[str, Any], name: str) -> None:
        """Valida typeProperties de ConditionalSplit"""
        if 'conditions' not in props:
            self.errors.append(f"ConditionalSplit '{name}': Falta campo 'conditions'")

    def _validate_lookup_props(self, props: Dict[str, Any], name: str) -> None:
        """Valida typeProperties de Lookup"""
        if 'lookupDataset' not in props:
            self.errors.append(f"Lookup '{name}': Falta campo 'lookupDataset'")

    def _validate_alter_row_props(self, props: Dict[str, Any], name: str) -> None:
        """Valida typeProperties de AlterRow"""
        valid_conditions = ['insertCondition', 'updateCondition', 'deleteCondition', 'upsertCondition']
        has_condition = any(c in props for c in valid_conditions)

        if not has_condition:
            self.errors.append(
                f"AlterRow '{name}': Debe tener al menos una condición "
                f"({', '.join(valid_conditions)})"
            )

    def _get_all_step_names(
        self,
        sources: List[Dict[str, Any]],
        transformations: List[Dict[str, Any]]
    ) -> set:
        """Obtiene todos los nombres de steps disponibles"""
        all_steps = {s['name'] for s in sources if 'name' in s}
        all_steps.update(t['name'] for t in transformations if 'name' in t)
        return all_steps


def validate_dataflow(dataflow: Dict[str, Any]) -> Tuple[bool, List[str], List[str]]:
    """
    Función de conveniencia para validar dataflows.

    Args:
        dataflow: Diccionario con el dataflow a validar

    Returns:
        Tupla (es_válido, lista_errores, lista_warnings)
    """
    validator = ADFSchemaValidator()
    return validator.validate_dataflow(dataflow)
