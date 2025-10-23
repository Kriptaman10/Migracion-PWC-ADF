"""
Tests unitarios para el módulo parser
"""

import pytest
from pathlib import Path
from lxml import etree

from src.parser import (
    PowerCenterXMLParser,
    parse_powercenter_xml,
    MappingMetadata,
    Transformation,
    Source,
    Target,
    TransformField
)
from src.utils import ValidationError


class TestTransformField:
    """Tests para la clase TransformField"""

    def test_create_transform_field(self):
        """Verifica creación básica de campo"""
        field = TransformField(
            name="customer_id",
            datatype="number",
            precision=10,
            scale=0
        )

        assert field.name == "customer_id"
        assert field.datatype == "number"
        assert field.precision == 10
        assert field.scale == 0


class TestTransformation:
    """Tests para la clase Transformation"""

    def test_create_transformation(self):
        """Verifica creación de transformación"""
        trans = Transformation(
            name="EXP_CustomerData",
            type="Expression",
            description="Transformación de datos de cliente"
        )

        assert trans.name == "EXP_CustomerData"
        assert trans.type == "Expression"
        assert trans.description == "Transformación de datos de cliente"
        assert len(trans.fields) == 0


    def test_transformation_with_fields(self):
        """Verifica transformación con campos"""
        field1 = TransformField(name="field1", datatype="string")
        field2 = TransformField(name="field2", datatype="number")

        trans = Transformation(
            name="TEST_TRANS",
            type="Expression",
            fields=[field1, field2]
        )

        assert len(trans.fields) == 2
        assert trans.fields[0].name == "field1"
        assert trans.fields[1].name == "field2"


class TestPowerCenterXMLParser:
    """Tests para el parser de XML"""

    @pytest.fixture
    def sample_xml(self, tmp_path):
        """Crea un XML de ejemplo para testing"""
        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
        <POWERMART>
            <REPOSITORY>
                <FOLDER NAME="TEST_FOLDER">
                    <MAPPING NAME="m_Customer_ETL">
                        <SOURCE NAME="SRC_Customer" DATABASETYPE="Oracle" TABLENAME="CUSTOMERS">
                            <TRANSFORMFIELD NAME="CUSTOMER_ID" DATATYPE="number" PRECISION="10"/>
                            <TRANSFORMFIELD NAME="CUSTOMER_NAME" DATATYPE="varchar2" PRECISION="100"/>
                        </SOURCE>
                        <TARGET NAME="TGT_Customer" DATABASETYPE="Oracle" TABLENAME="DW_CUSTOMERS">
                            <TRANSFORMFIELD NAME="CUSTOMER_ID" DATATYPE="number" PRECISION="10"/>
                            <TRANSFORMFIELD NAME="CUSTOMER_NAME" DATATYPE="varchar2" PRECISION="100"/>
                        </TARGET>
                        <TRANSFORMATION NAME="EXP_Transform" TYPE="Expression">
                            <TRANSFORMFIELD NAME="CUSTOMER_ID" DATATYPE="number" PRECISION="10"/>
                            <TRANSFORMFIELD NAME="UPPER_NAME" DATATYPE="varchar2" PRECISION="100"
                                          EXPRESSION="UPPER(CUSTOMER_NAME)"/>
                        </TRANSFORMATION>
                    </MAPPING>
                </FOLDER>
            </REPOSITORY>
        </POWERMART>
        """

        xml_file = tmp_path / "test_mapping.xml"
        xml_file.write_text(xml_content)
        return xml_file

    def test_parser_initialization(self):
        """Verifica inicialización del parser"""
        parser = PowerCenterXMLParser()
        assert parser.tree is None
        assert parser.root is None

    def test_parse_valid_xml(self, sample_xml):
        """Verifica parseo de XML válido"""
        parser = PowerCenterXMLParser()
        metadata = parser.parse_file(sample_xml)

        assert isinstance(metadata, MappingMetadata)
        assert metadata.name == "m_Customer_ETL"

    def test_parse_invalid_xml(self, tmp_path):
        """Verifica manejo de XML inválido"""
        invalid_xml = tmp_path / "invalid.xml"
        invalid_xml.write_text("This is not valid XML")

        parser = PowerCenterXMLParser()

        with pytest.raises(ValidationError):
            parser.parse_file(invalid_xml)

    def test_extract_sources(self, sample_xml):
        """Verifica extracción de sources"""
        parser = PowerCenterXMLParser()
        metadata = parser.parse_file(sample_xml)

        assert len(metadata.sources) == 1
        source = metadata.sources[0]

        assert source.name == "SRC_Customer"
        assert source.database_type == "Oracle"
        assert source.table_name == "CUSTOMERS"
        assert len(source.fields) == 2

    def test_extract_targets(self, sample_xml):
        """Verifica extracción de targets"""
        parser = PowerCenterXMLParser()
        metadata = parser.parse_file(sample_xml)

        assert len(metadata.targets) == 1
        target = metadata.targets[0]

        assert target.name == "TGT_Customer"
        assert target.database_type == "Oracle"
        assert target.table_name == "DW_CUSTOMERS"

    def test_extract_transformations(self, sample_xml):
        """Verifica extracción de transformaciones"""
        parser = PowerCenterXMLParser()
        metadata = parser.parse_file(sample_xml)

        assert len(metadata.transformations) == 1
        trans = metadata.transformations[0]

        assert trans.name == "EXP_Transform"
        assert trans.type == "Expression"
        assert len(trans.fields) == 2


class TestParsePowerCenterXML:
    """Tests para la función de conveniencia"""

    def test_parse_function(self, tmp_path):
        """Verifica función parse_powercenter_xml"""
        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
        <POWERMART>
            <REPOSITORY>
                <FOLDER>
                    <MAPPING NAME="TestMapping"/>
                </FOLDER>
            </REPOSITORY>
        </POWERMART>
        """

        xml_file = tmp_path / "test.xml"
        xml_file.write_text(xml_content)

        metadata = parse_powercenter_xml(str(xml_file))

        assert isinstance(metadata, MappingMetadata)
        assert metadata.name == "TestMapping"


# Fixture para ejecutar antes de todos los tests
@pytest.fixture(autouse=True)
def setup_logging():
    """Configura logging para tests"""
    import logging
    logging.basicConfig(level=logging.DEBUG)
