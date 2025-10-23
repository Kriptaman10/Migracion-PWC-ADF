"""
Tests unitarios para el módulo translator
"""

import pytest
from src.translator import PowerCenterToADFTranslator
from src.parser import (
    MappingMetadata,
    Transformation,
    TransformField,
    Source,
    Target
)


class TestPowerCenterToADFTranslator:
    """Tests para el traductor PowerCenter a ADF"""

    @pytest.fixture
    def translator(self):
        """Crea instancia del traductor para tests"""
        return PowerCenterToADFTranslator()

    def test_translator_initialization(self, translator):
        """Verifica inicialización del traductor"""
        assert translator is not None
        assert translator.transformation_mappings is not None
        assert translator.function_mappings is not None
        assert translator.datatype_mappings is not None

    def test_map_datatype_string(self, translator):
        """Verifica mapeo de tipo de dato string"""
        assert translator.map_datatype('string') == 'String'
        assert translator.map_datatype('varchar2') == 'String'

    def test_map_datatype_number(self, translator):
        """Verifica mapeo de tipo de dato number"""
        assert translator.map_datatype('number') == 'Int32'
        assert translator.map_datatype('decimal') == 'Decimal'

    def test_map_datatype_date(self, translator):
        """Verifica mapeo de tipo de dato date"""
        assert translator.map_datatype('date') == 'DateTime'
        assert translator.map_datatype('timestamp') == 'DateTime'

    def test_map_datatype_unknown(self, translator):
        """Verifica manejo de tipo de dato desconocido"""
        # Debe retornar String por defecto
        result = translator.map_datatype('unknown_type')
        assert result == 'String'

    def test_translate_expression_to_date(self, translator):
        """Verifica traducción de función TO_DATE"""
        pc_expr = "TO_DATE('2024-01-01', 'YYYY-MM-DD')"
        adf_expr = translator.translate_expression(pc_expr)

        assert 'toDate' in adf_expr
        assert 'TO_DATE' not in adf_expr

    def test_translate_expression_sysdate(self, translator):
        """Verifica traducción de SYSDATE"""
        pc_expr = "SYSDATE"
        adf_expr = translator.translate_expression(pc_expr)

        assert 'currentTimestamp()' in adf_expr

    def test_translate_expression_substr(self, translator):
        """Verifica traducción de SUBSTR"""
        pc_expr = "SUBSTR(column_name, 1, 10)"
        adf_expr = translator.translate_expression(pc_expr)

        assert 'substring' in adf_expr

    def test_translate_expression_concatenation(self, translator):
        """Verifica traducción de concatenación (||)"""
        pc_expr = "column1 || column2"
        adf_expr = translator.translate_expression(pc_expr)

        # || debe convertirse a +
        assert '+' in adf_expr
        assert '||' not in adf_expr

    def test_translate_source(self, translator):
        """Verifica traducción de Source"""
        field1 = TransformField(name="id", datatype="number")
        field2 = TransformField(name="name", datatype="string")

        source = Source(
            name="SRC_Customer",
            database_type="Oracle",
            table_name="CUSTOMERS",
            fields=[field1, field2]
        )

        adf_source = translator.translate_source(source)

        assert adf_source['name'] == "SRC_Customer"
        assert adf_source['type'] == 'Source'
        assert adf_source['dataset']['table'] == "CUSTOMERS"
        assert len(adf_source['schema']) == 2

    def test_translate_target(self, translator):
        """Verifica traducción de Target"""
        target = Target(
            name="TGT_Customer",
            database_type="Oracle",
            table_name="DW_CUSTOMERS"
        )

        adf_sink = translator.translate_target(target)

        assert adf_sink['name'] == "TGT_Customer"
        assert adf_sink['type'] == 'Sink'

    def test_translate_expression_transformation(self, translator):
        """Verifica traducción de transformación Expression"""
        field1 = TransformField(
            name="upper_name",
            datatype="string",
            expression="UPPER(name)"
        )

        trans = Transformation(
            name="EXP_Transform",
            type="Expression",
            fields=[field1]
        )

        adf_trans = translator.translate_transformation(trans)

        assert adf_trans is not None
        assert adf_trans['name'] == "EXP_Transform"
        assert adf_trans['type'] == "DerivedColumn"
        assert len(adf_trans['columns']) == 1

    def test_translate_filter_transformation(self, translator):
        """Verifica traducción de transformación Filter"""
        trans = Transformation(
            name="FLT_ActiveCustomers",
            type="Filter",
            properties={'filter_condition': 'STATUS = 1'}
        )

        adf_trans = translator.translate_transformation(trans)

        assert adf_trans is not None
        assert adf_trans['type'] == "Filter"
        assert 'condition' in adf_trans

    def test_translate_aggregator_transformation(self, translator):
        """Verifica traducción de transformación Aggregator"""
        trans = Transformation(
            name="AGG_Sales",
            type="Aggregator",
            properties={
                'group_by_fields': ['CUSTOMER_ID']
            }
        )

        adf_trans = translator.translate_transformation(trans)

        assert adf_trans is not None
        assert adf_trans['type'] == "Aggregate"
        assert adf_trans['groupBy'] == ['CUSTOMER_ID']

    def test_translate_joiner_transformation(self, translator):
        """Verifica traducción de transformación Joiner"""
        trans = Transformation(
            name="JNR_CustomerOrders",
            type="Joiner",
            properties={
                'join_type': 'Normal',
                'join_condition': 'CUSTOMER_ID = ORDER_CUSTOMER_ID'
            }
        )

        adf_trans = translator.translate_transformation(trans)

        assert adf_trans is not None
        assert adf_trans['type'] == "Join"
        assert adf_trans['joinType'] == 'inner'

    def test_translate_unsupported_transformation(self, translator):
        """Verifica manejo de transformación no soportada"""
        trans = Transformation(
            name="UNS_Unsupported",
            type="UnsupportedType"
        )

        adf_trans = translator.translate_transformation(trans)

        # Debe retornar None y agregar warning
        assert adf_trans is None
        assert len(translator.warnings) > 0

    def test_translate_complete_mapping(self, translator):
        """Verifica traducción de mapping completo"""
        # Crear metadata de ejemplo
        source = Source(
            name="SRC_Test",
            database_type="Oracle",
            table_name="TEST_TABLE"
        )

        target = Target(
            name="TGT_Test",
            database_type="Oracle",
            table_name="TEST_TARGET"
        )

        trans = Transformation(
            name="EXP_Test",
            type="Expression"
        )

        metadata = MappingMetadata(
            name="m_TestMapping",
            sources=[source],
            targets=[target],
            transformations=[trans]
        )

        # Traducir
        result = translator.translate_mapping(metadata)

        assert result['name'] == "m_TestMapping"
        assert len(result['sources']) == 1
        assert len(result['sinks']) == 1
        assert len(result['transformations']) == 1

    def test_get_statistics(self, translator):
        """Verifica obtención de estadísticas"""
        # Agregar algunos warnings y errors
        translator.warnings.append("Test warning")
        translator.errors.append("Test error")

        stats = translator.get_statistics()

        assert stats['warnings'] == 1
        assert stats['errors'] == 1


@pytest.fixture(autouse=True)
def setup_logging():
    """Configura logging para tests"""
    import logging
    logging.basicConfig(level=logging.DEBUG)
