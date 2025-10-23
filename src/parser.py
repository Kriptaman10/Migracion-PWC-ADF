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
        'Lookup': 'lookup'
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

            # Log si la transformación no es soportada
            if trans_type not in self.SUPPORTED_TRANSFORMATIONS:
                logger.warning(
                    f"Transformación '{trans_type}' en '{trans_name}' no está soportada"
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

        if trans_type == 'Aggregator':
            # Extraer group by fields
            properties['group_by_fields'] = []
            for field in trans_elem.findall('.//TRANSFORMFIELD[@PORTTYPE="INPUT/OUTPUT"]'):
                if field.get('GROUPBY') == 'YES':
                    properties['group_by_fields'].append(field.get('NAME'))

        elif trans_type == 'Joiner':
            # Extraer tipo de join
            properties['join_type'] = trans_elem.get('JOINTYPE', 'Normal')
            properties['join_condition'] = trans_elem.get('JOINCONDITION', '')

        elif trans_type == 'Filter':
            # Extraer condición de filtro
            properties['filter_condition'] = trans_elem.get('FILTERCONDITION', '')

        elif trans_type == 'Sorter':
            # Extraer campos de ordenamiento
            properties['sort_fields'] = []
            for field in trans_elem.findall('.//TRANSFORMFIELD[@SORTKEY]'):
                properties['sort_fields'].append({
                    'name': field.get('NAME'),
                    'order': field.get('SORTORDER', 'ASC')
                })

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
