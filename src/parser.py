"""
Parser de archivos XML de PowerCenter
Extrae metadata de mappings, transformations, sources y targets
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from pathlib import Path
import logging
from lxml import etree

from .utils import ValidationError

logger = logging.getLogger('pc-to-adf.parser')


@dataclass
class TransformField:
    """Representa un campo de transformación"""
    name: str
    datatype: str
    precision: int = 0
    scale: int = 0
    expression: Optional[str] = None
    description: Optional[str] = None


@dataclass
class Transformation:
    """Representa una transformación de PowerCenter"""
    name: str
    type: str
    description: Optional[str] = None
    fields: List[TransformField] = field(default_factory=list)
    properties: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Source:
    """Representa una fuente de datos"""
    name: str
    database_type: str
    table_name: Optional[str] = None
    fields: List[TransformField] = field(default_factory=list)
    properties: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Target:
    """Representa un destino de datos"""
    name: str
    database_type: str
    table_name: Optional[str] = None
    fields: List[TransformField] = field(default_factory=list)
    properties: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Connector:
    """Representa una conexión entre transformaciones"""
    from_instance: str
    to_instance: str
    from_fields: List[str] = field(default_factory=list)
    to_fields: List[str] = field(default_factory=list)


@dataclass
class MappingMetadata:
    """Metadata completa de un mapping de PowerCenter"""
    name: str
    description: Optional[str] = None
    version: str = "10.x"
    sources: List[Source] = field(default_factory=list)
    targets: List[Target] = field(default_factory=list)
    transformations: List[Transformation] = field(default_factory=list)
    connectors: List[Connector] = field(default_factory=list)
    properties: Dict[str, Any] = field(default_factory=dict)


class PowerCenterXMLParser:
    """
    Parser de archivos XML de PowerCenter.
    Extrae metadata de mappings para su posterior traducción.
    """

    # Tipos de transformación soportados
    SUPPORTED_TRANSFORMATIONS = {
        'Source Qualifier': 'source_qualifier',
        'Expression': 'expression',
        'Filter': 'filter',
        'Aggregator': 'aggregator',
        'Joiner': 'joiner',
        'Sorter': 'sorter',
        'Router': 'router',
        'Lookup': 'lookup',
        'Lookup Procedure': 'lookup',
        'Update Strategy': 'update_strategy'
    }

    def __init__(self):
        """Inicializa el parser"""
        self.tree: Optional[etree._ElementTree] = None
        self.root: Optional[etree._Element] = None

    def parse_file(self, xml_file: Path) -> MappingMetadata:
        """
        Parsea un archivo XML de PowerCenter.

        Args:
            xml_file: Path al archivo XML

        Returns:
            MappingMetadata con toda la información extraída

        Raises:
            ValidationError: Si el XML es inválido
        """
        logger.info(f"Iniciando parseo de archivo: {xml_file}")

        try:
            self.tree = etree.parse(str(xml_file))
            self.root = self.tree.getroot()
        except etree.XMLSyntaxError as e:
            raise ValidationError(f"Error de sintaxis XML: {e}")

        # Extraer información del mapping
        mapping_name = self._extract_mapping_name()
        logger.info(f"Mapping encontrado: {mapping_name}")

        metadata = MappingMetadata(name=mapping_name)

        # Extraer componentes
        metadata.sources = self._extract_sources()
        logger.info(f"Fuentes extraídas: {len(metadata.sources)}")

        metadata.targets = self._extract_targets()
        logger.info(f"Destinos extraídos: {len(metadata.targets)}")

        metadata.transformations = self._extract_transformations()
        logger.info(f"Transformaciones extraídas: {len(metadata.transformations)}")

        metadata.connectors = self._extract_connectors()
        logger.info(f"Conectores extraídos: {len(metadata.connectors)}")

        logger.info("Parseo completado exitosamente")
        return metadata

    def _extract_mapping_name(self) -> str:
        """Extrae el nombre del mapping"""
        mapping_elem = self.root.find('.//MAPPING')
        if mapping_elem is not None:
            return mapping_elem.get('NAME', 'UnknownMapping')
        return 'UnknownMapping'

    def _extract_sources(self) -> List[Source]:
        """Extrae todas las fuentes del mapping"""
        sources = []
        source_elems = self.root.findall('.//SOURCE')

        for source_elem in source_elems:
            source = Source(
                name=source_elem.get('NAME', ''),
                database_type=source_elem.get('DATABASETYPE', 'Unknown'),
                table_name=source_elem.get('TABLENAME')
            )

            # Extraer campos
            source.fields = self._extract_fields(source_elem)

            sources.append(source)

        return sources

    def _extract_targets(self) -> List[Target]:
        """Extrae todos los targets del mapping"""
        targets = []
        target_elems = self.root.findall('.//TARGET')

        for target_elem in target_elems:
            target = Target(
                name=target_elem.get('NAME', ''),
                database_type=target_elem.get('DATABASETYPE', 'Unknown'),
                table_name=target_elem.get('TABLENAME')
            )

            # Extraer campos
            target.fields = self._extract_fields(target_elem)

            targets.append(target)

        return targets

    def _extract_transformations(self) -> List[Transformation]:
        """Extrae todas las transformaciones del mapping"""
        transformations = []
        trans_elems = self.root.findall('.//TRANSFORMATION')

        for trans_elem in trans_elems:
            trans_type = trans_elem.get('TYPE', '')
            trans_name = trans_elem.get('NAME', '')

            transformation = Transformation(
                name=trans_name,
                type=trans_type,
                description=trans_elem.get('DESCRIPTION')
            )

            # Extraer campos
            transformation.fields = self._extract_fields(trans_elem)

            # Extraer propiedades específicas por tipo
            transformation.properties = self._extract_transformation_properties(
                trans_elem, trans_type
            )

            transformations.append(transformation)

            # Log si la transformación no es soportada (solo para transformaciones realmente no soportadas)
            unsupported_transformations = {
                'Sequence Generator', 'Normalizer', 'Rank', 'Union',
                'XML Source Qualifier', 'XML Target', 'Custom Transformation'
            }
            if trans_type in unsupported_transformations:
                logger.warning(
                    f"Transformación '{trans_type}' en '{trans_name}' no está soportada en v2.0"
                )

        return transformations

    def _extract_fields(self, parent_elem: etree._Element) -> List[TransformField]:
        """Extrae campos de un elemento (source, target, transformation)"""
        fields = []
        field_elems = parent_elem.findall('.//TRANSFORMFIELD')

        for field_elem in field_elems:
            field = TransformField(
                name=field_elem.get('NAME', ''),
                datatype=field_elem.get('DATATYPE', 'string'),
                precision=int(field_elem.get('PRECISION', 0)),
                scale=int(field_elem.get('SCALE', 0)),
                expression=field_elem.get('EXPRESSION'),
                description=field_elem.get('DESCRIPTION')
            )
            fields.append(field)

        return fields

    def _extract_transformation_properties(
        self, trans_elem: etree._Element, trans_type: str
    ) -> Dict[str, Any]:
        """Extrae propiedades específicas de cada tipo de transformación"""
        properties = {}

        # Delegar a métodos especializados para cada tipo
        if trans_type == 'Aggregator':
            properties = self._parse_aggregator_properties(trans_elem)
        elif trans_type == 'Joiner':
            properties = self._parse_joiner_properties(trans_elem)
        elif trans_type == 'Filter':
            properties = self._parse_filter_properties(trans_elem)
        elif trans_type == 'Sorter':
            properties = self._parse_sorter_properties(trans_elem)
        elif trans_type == 'Router':
            properties = self._parse_router_properties(trans_elem)
        elif trans_type == 'Lookup Procedure':
            properties = self._parse_lookup_properties(trans_elem)
        elif trans_type == 'Update Strategy':
            properties = self._parse_update_strategy_properties(trans_elem)
        elif trans_type == 'Source Qualifier':
            properties = self._parse_source_qualifier_properties(trans_elem)

        return properties

    def _parse_sorter_properties(self, trans_elem: etree._Element) -> Dict[str, Any]:
        """
        Parsea propiedades del Sorter Transformation.

        Extrae:
        - Sort keys con dirección (ASC/DESC)
        - Distinct flag
        - Case sensitive flag
        - Null treated low
        """
        properties = {
            'sort_keys': [],
            'distinct': False,
            'case_sensitive': True,
            'null_treated_low': False
        }

        # Extraer campos con ISSORTKEY="YES"
        for field in trans_elem.findall('.//TRANSFORMFIELD'):
            if field.get('ISSORTKEY') == 'YES':
                properties['sort_keys'].append({
                    'name': field.get('NAME'),
                    'direction': field.get('SORTDIRECTION', 'ASCENDING'),
                    'order': int(field.get('SORTORDER', 0))
                })

        # Ordenar por SORTORDER
        properties['sort_keys'].sort(key=lambda x: x['order'])

        # Extraer atributos de tabla
        for attr in trans_elem.findall('.//TABLEATTRIBUTE'):
            attr_name = attr.get('NAME')
            attr_value = attr.get('VALUE', '')

            if attr_name == 'Distinct':
                properties['distinct'] = attr_value.upper() == 'YES'
            elif attr_name == 'Case Sensitive':
                properties['case_sensitive'] = attr_value.upper() == 'YES'
            elif attr_name == 'Null Treated Low':
                properties['null_treated_low'] = attr_value.upper() == 'YES'

        return properties

    def _parse_update_strategy_properties(self, trans_elem: etree._Element) -> Dict[str, Any]:
        """
        Parsea propiedades del Update Strategy Transformation.

        Extrae:
        - Update strategy expression (DD_INSERT, DD_UPDATE, DD_DELETE, DD_REJECT)
        - Forward rejected rows flag
        """
        properties = {
            'strategy': 'DD_INSERT',  # Default
            'strategy_expression': None,
            'forward_rejected_rows': False
        }

        # Extraer expresión de estrategia
        for field in trans_elem.findall('.//TRANSFORMFIELD'):
            if field.get('PORTTYPE') == 'OUTPUT':
                strategy_expr = field.get('EXPRESSION')
                if strategy_expr:
                    properties['strategy_expression'] = strategy_expr
                    # Detectar tipo de estrategia
                    if 'DD_INSERT' in strategy_expr:
                        properties['strategy'] = 'DD_INSERT'
                    elif 'DD_UPDATE' in strategy_expr:
                        properties['strategy'] = 'DD_UPDATE'
                    elif 'DD_DELETE' in strategy_expr:
                        properties['strategy'] = 'DD_DELETE'
                    elif 'DD_REJECT' in strategy_expr:
                        properties['strategy'] = 'DD_REJECT'

        # Extraer atributos
        for attr in trans_elem.findall('.//TABLEATTRIBUTE'):
            attr_name = attr.get('NAME')
            attr_value = attr.get('VALUE', '')

            if attr_name == 'Forward Rejected Rows':
                properties['forward_rejected_rows'] = attr_value.upper() == 'YES'
            elif attr_name == 'Update Strategy Expression':
                properties['strategy_expression'] = attr_value

        return properties

    def _parse_aggregator_properties(self, trans_elem: etree._Element) -> Dict[str, Any]:
        """
        Parsea propiedades mejoradas del Aggregator Transformation.

        Extrae:
        - Group by fields (EXPRESSIONTYPE="GROUPBY")
        - Aggregate expressions (SUM, AVG, COUNT, etc.)
        - Sorted input flag
        """
        properties = {
            'group_by_fields': [],
            'aggregate_expressions': [],
            'sorted_input': False
        }

        # Extraer campos
        for field in trans_elem.findall('.//TRANSFORMFIELD'):
            expression_type = field.get('EXPRESSIONTYPE', '')
            field_name = field.get('NAME')
            port_type = field.get('PORTTYPE', '')

            if expression_type == 'GROUPBY':
                properties['group_by_fields'].append(field_name)
            elif expression_type == 'GENERAL' and field.get('EXPRESSION'):
                # Campo con expresión de agregación
                properties['aggregate_expressions'].append({
                    'name': field_name,
                    'expression': field.get('EXPRESSION'),
                    'datatype': field.get('DATATYPE', 'string')
                })

        # Extraer atributos
        for attr in trans_elem.findall('.//TABLEATTRIBUTE'):
            if attr.get('NAME') == 'Sorted Input':
                properties['sorted_input'] = attr.get('VALUE', 'NO').upper() == 'YES'

        return properties

    def _parse_joiner_properties(self, trans_elem: etree._Element) -> Dict[str, Any]:
        """
        Parsea propiedades mejoradas del Joiner Transformation.

        Extrae:
        - Join condition (puede ser múltiple)
        - Join type (Normal, Master Outer, Detail Outer, Full Outer)
        - Master fields (PORTTYPE="INPUT/OUTPUT/MASTER")
        - Detail fields (PORTTYPE="INPUT/OUTPUT")
        - Sorted input flag
        """
        properties = {
            'join_type': 'Normal Join',
            'join_condition': '',
            'master_fields': [],
            'detail_fields': [],
            'sorted_input': False,
            'master_sort_order': 'Auto'
        }

        # Extraer campos master y detail
        for field in trans_elem.findall('.//TRANSFORMFIELD'):
            port_type = field.get('PORTTYPE', '')
            field_name = field.get('NAME')

            if 'MASTER' in port_type:
                properties['master_fields'].append(field_name)
            elif port_type in ('INPUT/OUTPUT', 'OUTPUT'):
                properties['detail_fields'].append(field_name)

        # Extraer atributos de tabla
        for attr in trans_elem.findall('.//TABLEATTRIBUTE'):
            attr_name = attr.get('NAME')
            attr_value = attr.get('VALUE', '')

            if attr_name == 'Join Condition':
                properties['join_condition'] = attr_value
            elif attr_name == 'Join Type':
                properties['join_type'] = attr_value
            elif attr_name == 'Sorted Input':
                properties['sorted_input'] = attr_value.upper() == 'YES'
            elif attr_name == 'Master Sort Order':
                properties['master_sort_order'] = attr_value

        return properties

    def _parse_lookup_properties(self, trans_elem: etree._Element) -> Dict[str, Any]:
        """
        Parsea propiedades del Lookup Transformation.

        Extrae:
        - Lookup table name
        - Source type (Database, Flat File)
        - Lookup condition
        - SQL Override
        - Return fields (PORTTYPE="LOOKUP/OUTPUT")
        - Caching enabled
        - Multiple match policy
        """
        properties = {
            'lookup_table': None,
            'source_type': 'Database',
            'lookup_condition': '',
            'sql_override': None,
            'return_fields': [],
            'cache_enabled': True,
            'multiple_match_policy': 'Use Any Value',
            'lookup_fields': []
        }

        # Extraer campos lookup
        for field in trans_elem.findall('.//TRANSFORMFIELD'):
            port_type = field.get('PORTTYPE', '')
            field_name = field.get('NAME')

            if 'LOOKUP' in port_type and 'OUTPUT' in port_type:
                properties['return_fields'].append({
                    'name': field_name,
                    'datatype': field.get('DATATYPE', 'string')
                })
            elif 'LOOKUP' in port_type:
                properties['lookup_fields'].append(field_name)

        # Extraer atributos de tabla
        for attr in trans_elem.findall('.//TABLEATTRIBUTE'):
            attr_name = attr.get('NAME')
            attr_value = attr.get('VALUE', '')

            if attr_name == 'Lookup table name':
                properties['lookup_table'] = attr_value
            elif attr_name == 'Source Type':
                properties['source_type'] = attr_value
            elif attr_name == 'Lookup condition':
                properties['lookup_condition'] = attr_value
            elif attr_name == 'Lookup Sql Override':
                properties['sql_override'] = attr_value
            elif attr_name == 'Lookup caching enabled':
                properties['cache_enabled'] = attr_value.upper() == 'YES'
            elif attr_name == 'Lookup policy on multiple match':
                properties['multiple_match_policy'] = attr_value

        # Extraer información de Flat File si aplica
        flatfile_elem = trans_elem.find('.//FLATFILE')
        if flatfile_elem is not None:
            properties['flat_file'] = {
                'delimited': flatfile_elem.get('DELIMITED') == 'YES',
                'delimiters': flatfile_elem.get('DELIMITERS', ','),
                'skip_rows': int(flatfile_elem.get('SKIPROWS', 0))
            }

        return properties

    def _parse_router_properties(self, trans_elem: etree._Element) -> Dict[str, Any]:
        """
        Parsea propiedades del Router Transformation.

        Extrae:
        - Output groups con sus expresiones
        - Default group
        - Campos por grupo con REF_FIELD
        """
        properties = {
            'groups': [],
            'default_group': None
        }

        # Extraer grupos
        for group in trans_elem.findall('.//GROUP'):
            group_name = group.get('NAME')
            group_type = group.get('TYPE', '')
            group_expression = group.get('EXPRESSION')

            if 'DEFAULT' in group_type:
                properties['default_group'] = group_name
                properties['groups'].append({
                    'name': group_name,
                    'type': 'default',
                    'expression': None,
                    'fields': []
                })
            elif 'OUTPUT' in group_type:
                group_info = {
                    'name': group_name,
                    'type': 'output',
                    'expression': group_expression,
                    'fields': []
                }
                properties['groups'].append(group_info)

        # Extraer campos por grupo
        for field in trans_elem.findall('.//TRANSFORMFIELD'):
            field_group = field.get('GROUP')
            field_name = field.get('NAME')
            ref_field = field.get('REF_FIELD')
            port_type = field.get('PORTTYPE', '')

            if field_group and 'OUTPUT' in port_type:
                # Buscar el grupo correspondiente
                for group in properties['groups']:
                    if group['name'] == field_group:
                        group['fields'].append({
                            'name': field_name,
                            'ref_field': ref_field,
                            'datatype': field.get('DATATYPE')
                        })
                        break

        return properties

    def _parse_filter_properties(self, trans_elem: etree._Element) -> Dict[str, Any]:
        """Parsea propiedades del Filter Transformation"""
        properties = {'filter_condition': ''}

        for attr in trans_elem.findall('.//TABLEATTRIBUTE'):
            if attr.get('NAME') == 'Filter Condition':
                properties['filter_condition'] = attr.get('VALUE', '')

        return properties

    def _parse_source_qualifier_properties(self, trans_elem: etree._Element) -> Dict[str, Any]:
        """
        Parsea propiedades del Source Qualifier Transformation.

        Extrae:
        - Source filter
        - SQL Override
        - User defined join
        """
        properties = {
            'source_filter': None,
            'sql_override': None,
            'user_defined_join': None
        }

        for attr in trans_elem.findall('.//TABLEATTRIBUTE'):
            attr_name = attr.get('NAME')
            attr_value = attr.get('VALUE', '')

            if attr_name == 'Source Filter':
                properties['source_filter'] = attr_value
            elif attr_name == 'Sql Query':
                properties['sql_override'] = attr_value
            elif attr_name == 'User Defined Join':
                properties['user_defined_join'] = attr_value

        return properties

    def _extract_connectors(self) -> List[Connector]:
        """Extrae todas las conexiones entre transformaciones"""
        connectors = []
        connector_elems = self.root.findall('.//CONNECTOR')

        for conn_elem in connector_elems:
            connector = Connector(
                from_instance=conn_elem.get('FROMINSTANCE', ''),
                to_instance=conn_elem.get('TOINSTANCE', '')
            )

            # Extraer campos conectados
            for field_map in conn_elem.findall('.//FIELDMAP'):
                connector.from_fields.append(field_map.get('FROMFIELD', ''))
                connector.to_fields.append(field_map.get('TOFIELD', ''))

            connectors.append(connector)

        return connectors


def parse_powercenter_xml(xml_file: str) -> MappingMetadata:
    """
    Función de conveniencia para parsear un archivo XML de PowerCenter.

    Args:
        xml_file: Ruta al archivo XML (string o Path)

    Returns:
        MappingMetadata con toda la información extraída

    Example:
        >>> metadata = parse_powercenter_xml('my_mapping.xml')
        >>> print(f"Mapping: {metadata.name}")
        >>> print(f"Transformaciones: {len(metadata.transformations)}")
    """
    parser = PowerCenterXMLParser()
    return parser.parse_file(Path(xml_file))
