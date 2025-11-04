"""
Tests unitarios para componentes v2.0
Verifica parsing y traducción de transformaciones avanzadas
"""

import pytest
from pathlib import Path
from lxml import etree

from src.parser import PowerCenterXMLParser, Transformation
from src.translator import PowerCenterToADFTranslator
from src.validator import MappingValidator


class TestSorterParsing:
    """Tests para parsing de Sorter Transformation"""

    def test_parse_sorter_basic(self):
        """Test parsing básico de Sorter"""
        xml_content = """
        <TRANSFORMATION NAME="SRT_TEST" TYPE="Sorter">
            <TRANSFORMFIELD NAME="FIELD1" ISSORTKEY="YES" SORTDIRECTION="ASCENDING" SORTORDER="0"/>
            <TRANSFORMFIELD NAME="FIELD2" ISSORTKEY="YES" SORTDIRECTION="DESCENDING" SORTORDER="1"/>
            <TABLEATTRIBUTE NAME="Distinct" VALUE="NO"/>
            <TABLEATTRIBUTE NAME="Case Sensitive" VALUE="YES"/>
        </TRANSFORMATION>
        """
        trans_elem = etree.fromstring(xml_content)
        parser = PowerCenterXMLParser()
        properties = parser._parse_sorter_properties(trans_elem)

        assert len(properties['sort_keys']) == 2
        assert properties['sort_keys'][0]['name'] == 'FIELD1'
        assert properties['sort_keys'][0]['direction'] == 'ASCENDING'
        assert properties['sort_keys'][1]['name'] == 'FIELD2'
        assert properties['sort_keys'][1]['direction'] == 'DESCENDING'
        assert properties['distinct'] == False
        assert properties['case_sensitive'] == True

    def test_translate_sorter(self):
        """Test traducción de Sorter a Sort"""
        trans = Transformation(
            name='SRT_TEST',
            type='Sorter',
            properties={
                'sort_keys': [
                    {'name': 'FIELD1', 'direction': 'ASCENDING', 'order': 0},
                    {'name': 'FIELD2', 'direction': 'DESCENDING', 'order': 1}
                ],
                'distinct': False,
                'case_sensitive': True
            }
        )

        translator = PowerCenterToADFTranslator()
        result = translator._translate_sorter(trans, 'Sort')

        assert result['name'] == 'SRT_TEST'
        assert result['type'] == 'Sort'
        assert len(result['orderBy']) == 2
        assert result['orderBy'][0]['name'] == 'FIELD1'
        assert result['orderBy'][0]['order'] == 'asc'
        assert result['orderBy'][1]['order'] == 'desc'
        assert result['distinct'] == False


class TestUpdateStrategyParsing:
    """Tests para parsing de Update Strategy Transformation"""

    def test_parse_update_strategy_insert(self):
        """Test parsing de Update Strategy con DD_INSERT"""
        xml_content = """
        <TRANSFORMATION NAME="UPD_TEST" TYPE="Update Strategy">
            <TABLEATTRIBUTE NAME="Update Strategy Expression" VALUE="DD_INSERT"/>
        </TRANSFORMATION>
        """
        trans_elem = etree.fromstring(xml_content)
        parser = PowerCenterXMLParser()
        properties = parser._parse_update_strategy_properties(trans_elem)

        assert properties['strategy'] == 'DD_INSERT'

    def test_translate_update_strategy(self):
        """Test traducción de Update Strategy a AlterRow"""
        trans = Transformation(
            name='UPD_TEST',
            type='Update Strategy',
            properties={
                'strategy': 'DD_INSERT',
                'strategy_expression': 'DD_INSERT'
            }
        )

        translator = PowerCenterToADFTranslator()
        result = translator._translate_update_strategy(trans, 'AlterRow')

        assert result['name'] == 'UPD_TEST'
        assert result['type'] == 'AlterRow'
        assert result['action'] == 'insert'


class TestAggregatorParsing:
    """Tests para parsing mejorado de Aggregator Transformation"""

    def test_parse_aggregator_with_group_by(self):
        """Test parsing de Aggregator con GROUP BY y agregaciones"""
        xml_content = """
        <TRANSFORMATION NAME="AGG_TEST" TYPE="Aggregator">
            <TRANSFORMFIELD NAME="PRODUCT_ID" EXPRESSIONTYPE="GROUPBY" PORTTYPE="INPUT/OUTPUT"/>
            <TRANSFORMFIELD NAME="TOTAL_SALES" EXPRESSION="SUM(AMOUNT)" EXPRESSIONTYPE="GENERAL" PORTTYPE="OUTPUT"/>
            <TABLEATTRIBUTE NAME="Sorted Input" VALUE="NO"/>
        </TRANSFORMATION>
        """
        trans_elem = etree.fromstring(xml_content)
        parser = PowerCenterXMLParser()
        properties = parser._parse_aggregator_properties(trans_elem)

        assert 'PRODUCT_ID' in properties['group_by_fields']
        assert len(properties['aggregate_expressions']) == 1
        assert properties['aggregate_expressions'][0]['name'] == 'TOTAL_SALES'
        assert properties['aggregate_expressions'][0]['expression'] == 'SUM(AMOUNT)'
        assert properties['sorted_input'] == False

    def test_translate_aggregator_with_sorted_input(self):
        """Test traducción de Aggregator con Sorted Input"""
        trans = Transformation(
            name='AGG_TEST',
            type='Aggregator',
            properties={
                'group_by_fields': ['PRODUCT_ID'],
                'aggregate_expressions': [
                    {'name': 'TOTAL_SALES', 'expression': 'SUM(AMOUNT)', 'datatype': 'decimal'}
                ],
                'sorted_input': True
            }
        )

        translator = PowerCenterToADFTranslator()
        result = translator._translate_aggregator(trans, 'Aggregate')

        assert result['name'] == 'AGG_TEST'
        assert result['type'] == 'Aggregate'
        assert result['groupBy'] == ['PRODUCT_ID']
        assert len(result['aggregates']) == 1
        assert result['sorted_input'] == True
        assert len(translator.warnings) > 0  # Debe generar warning sobre sorted input


class TestJoinerParsing:
    """Tests para parsing mejorado de Joiner Transformation"""

    def test_parse_joiner_with_master_detail(self):
        """Test parsing de Joiner con Master/Detail fields"""
        xml_content = """
        <TRANSFORMATION NAME="JNR_TEST" TYPE="Joiner">
            <TRANSFORMFIELD NAME="MASTER_ID" PORTTYPE="INPUT/OUTPUT/MASTER"/>
            <TRANSFORMFIELD NAME="DETAIL_ID" PORTTYPE="INPUT/OUTPUT"/>
            <TABLEATTRIBUTE NAME="Join Condition" VALUE="MASTER_ID = DETAIL_ID"/>
            <TABLEATTRIBUTE NAME="Join Type" VALUE="Normal Join"/>
            <TABLEATTRIBUTE NAME="Sorted Input" VALUE="NO"/>
        </TRANSFORMATION>
        """
        trans_elem = etree.fromstring(xml_content)
        parser = PowerCenterXMLParser()
        properties = parser._parse_joiner_properties(trans_elem)

        assert 'MASTER_ID' in properties['master_fields']
        assert 'DETAIL_ID' in properties['detail_fields']
        assert properties['join_type'] == 'Normal Join'
        assert properties['join_condition'] == 'MASTER_ID = DETAIL_ID'
        assert properties['sorted_input'] == False

    def test_translate_joiner_with_multiple_conditions(self):
        """Test traducción de Joiner con múltiples condiciones"""
        trans = Transformation(
            name='JNR_TEST',
            type='Joiner',
            properties={
                'join_type': 'Normal Join',
                'join_condition': 'FIELD1 = FIELD1 AND FIELD2 = FIELD2',
                'master_fields': ['FIELD1', 'FIELD2'],
                'detail_fields': ['FIELD3', 'FIELD4'],
                'sorted_input': False
            }
        )

        translator = PowerCenterToADFTranslator()
        result = translator._translate_joiner(trans, 'Join')

        assert result['name'] == 'JNR_TEST'
        assert result['type'] == 'Join'
        assert result['joinType'] == 'inner'
        assert len(result['joinConditions']) == 2
        assert result['joinConditions'][0]['leftColumn'] == 'FIELD1'
        assert result['joinConditions'][0]['rightColumn'] == 'FIELD1'


class TestLookupParsing:
    """Tests para parsing completo de Lookup Transformation"""

    def test_parse_lookup_database(self):
        """Test parsing de Lookup con Database"""
        xml_content = """
        <TRANSFORMATION NAME="LKP_TEST" TYPE="Lookup Procedure">
            <TRANSFORMFIELD NAME="INPUT_KEY" PORTTYPE="INPUT/OUTPUT"/>
            <TRANSFORMFIELD NAME="OUTPUT_VALUE" PORTTYPE="LOOKUP/OUTPUT"/>
            <TABLEATTRIBUTE NAME="Lookup table name" VALUE="DIM_TABLE"/>
            <TABLEATTRIBUTE NAME="Source Type" VALUE="Database"/>
            <TABLEATTRIBUTE NAME="Lookup condition" VALUE="KEY = INPUT_KEY"/>
            <TABLEATTRIBUTE NAME="Lookup caching enabled" VALUE="YES"/>
        </TRANSFORMATION>
        """
        trans_elem = etree.fromstring(xml_content)
        parser = PowerCenterXMLParser()
        properties = parser._parse_lookup_properties(trans_elem)

        assert properties['lookup_table'] == 'DIM_TABLE'
        assert properties['source_type'] == 'Database'
        assert properties['lookup_condition'] == 'KEY = INPUT_KEY'
        assert properties['cache_enabled'] == True
        assert len(properties['return_fields']) == 1

    def test_translate_lookup_with_sql_override(self):
        """Test traducción de Lookup con SQL Override"""
        trans = Transformation(
            name='LKP_TEST',
            type='Lookup Procedure',
            properties={
                'lookup_table': 'DIM_TABLE',
                'source_type': 'Database',
                'lookup_condition': 'KEY = INPUT_KEY',
                'sql_override': 'SELECT * FROM DIM_TABLE WHERE ACTIVE = 1',
                'cache_enabled': True,
                'multiple_match_policy': 'Use Any Value',
                'return_fields': [{'name': 'VALUE', 'datatype': 'string'}]
            }
        )

        translator = PowerCenterToADFTranslator()
        result = translator._translate_lookup(trans, 'Lookup')

        assert result['name'] == 'LKP_TEST'
        assert result['type'] == 'Lookup'
        assert result['lookupDataset'] == 'DIM_TABLE'
        assert result['cacheMode'] == 'static'
        assert result['sqlOverride'] == 'SELECT * FROM DIM_TABLE WHERE ACTIVE = 1'
        assert len(translator.warnings) > 0  # Debe generar warning sobre SQL Override


class TestRouterParsing:
    """Tests para parsing completo de Router Transformation"""

    def test_parse_router_with_groups(self):
        """Test parsing de Router con múltiples grupos"""
        xml_content = """
        <TRANSFORMATION NAME="RTR_TEST" TYPE="Router">
            <GROUP NAME="INPUT" TYPE="INPUT"/>
            <GROUP NAME="HIGH_VALUE" TYPE="OUTPUT" EXPRESSION="AMOUNT &gt; 1000"/>
            <GROUP NAME="LOW_VALUE" TYPE="OUTPUT" EXPRESSION="AMOUNT &lt;= 1000"/>
            <GROUP NAME="DEFAULT1" TYPE="OUTPUT/DEFAULT"/>
            <TRANSFORMFIELD NAME="AMOUNT" GROUP="INPUT" PORTTYPE="INPUT"/>
            <TRANSFORMFIELD NAME="AMOUNT1" GROUP="HIGH_VALUE" PORTTYPE="OUTPUT" REF_FIELD="AMOUNT"/>
            <TRANSFORMFIELD NAME="AMOUNT2" GROUP="LOW_VALUE" PORTTYPE="OUTPUT" REF_FIELD="AMOUNT"/>
        </TRANSFORMATION>
        """
        trans_elem = etree.fromstring(xml_content)
        parser = PowerCenterXMLParser()
        properties = parser._parse_router_properties(trans_elem)

        assert len(properties['groups']) == 3  # HIGH_VALUE, LOW_VALUE, DEFAULT
        assert properties['default_group'] == 'DEFAULT1'

        # Verificar grupos
        high_value_group = next(g for g in properties['groups'] if g['name'] == 'HIGH_VALUE')
        assert 'AMOUNT' in high_value_group['expression'] and '1000' in high_value_group['expression']
        assert high_value_group['type'] == 'output'

    def test_translate_router(self):
        """Test traducción de Router a ConditionalSplit"""
        trans = Transformation(
            name='RTR_TEST',
            type='Router',
            properties={
                'groups': [
                    {
                        'name': 'HIGH_VALUE',
                        'type': 'output',
                        'expression': 'AMOUNT > 1000',
                        'fields': []
                    },
                    {
                        'name': 'LOW_VALUE',
                        'type': 'output',
                        'expression': 'AMOUNT <= 1000',
                        'fields': []
                    },
                    {
                        'name': 'DEFAULT',
                        'type': 'default',
                        'expression': None,
                        'fields': []
                    }
                ],
                'default_group': 'DEFAULT'
            }
        )

        translator = PowerCenterToADFTranslator()
        result = translator._translate_router(trans, 'ConditionalSplit')

        assert result['name'] == 'RTR_TEST'
        assert result['type'] == 'ConditionalSplit'
        assert len(result['conditions']) == 2  # Solo output groups, no default
        assert result['defaultStream'] == 'DEFAULT'


class TestValidator:
    """Tests para el validador de mappings"""

    def test_validator_detects_unsupported_transformation(self):
        """Test que el validador detecta transformaciones no soportadas"""
        from src.parser import MappingMetadata, Source, Target

        metadata = MappingMetadata(
            name='TEST_MAPPING',
            sources=[Source(name='SRC', database_type='Oracle')],
            targets=[Target(name='TGT', database_type='Oracle')],
            transformations=[
                Transformation(name='SEQ_GEN', type='Sequence Generator'),
                Transformation(name='EXP', type='Expression')
            ]
        )

        validator = MappingValidator()
        errors, warnings = validator.validate(metadata)

        assert len(errors) > 0
        assert any('Sequence Generator' in error for error in errors)

    def test_validator_checks_joiner_conditions(self):
        """Test que el validador verifica condiciones de Joiner"""
        from src.parser import MappingMetadata, Source, Target

        metadata = MappingMetadata(
            name='TEST_MAPPING',
            sources=[Source(name='SRC', database_type='Oracle')],
            targets=[Target(name='TGT', database_type='Oracle')],
            transformations=[
                Transformation(
                    name='JNR_TEST',
                    type='Joiner',
                    properties={'join_condition': ''}  # Sin condición
                )
            ]
        )

        validator = MappingValidator()
        errors, warnings = validator.validate(metadata)

        assert len(errors) > 0
        assert any('join condition' in error.lower() for error in errors)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
