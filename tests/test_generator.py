"""
Tests unitarios para el módulo generator
"""

import pytest
import json
from pathlib import Path

from src.generator import ADFGenerator
from src.parser import MappingMetadata, Source, Target, Transformation


class TestADFGenerator:
    """Tests para el generador de archivos ADF"""

    @pytest.fixture
    def output_dir(self, tmp_path):
        """Crea directorio temporal para output"""
        return tmp_path / "test_output"

    @pytest.fixture
    def generator(self, output_dir):
        """Crea instancia del generador para tests"""
        return ADFGenerator(str(output_dir))

    @pytest.fixture
    def sample_translated_structure(self):
        """Crea estructura traducida de ejemplo"""
        return {
            'name': 'TestMapping',
            'description': 'Test mapping description',
            'sources': [
                {
                    'name': 'SRC_Test',
                    'type': 'Source',
                    'dataset': {
                        'type': 'OracleTable',
                        'table': 'TEST_TABLE'
                    },
                    'schema': [
                        {'name': 'ID', 'type': 'Int32'},
                        {'name': 'NAME', 'type': 'String'}
                    ]
                }
            ],
            'transformations': [
                {
                    'name': 'EXP_Transform',
                    'type': 'DerivedColumn',
                    'columns': [
                        {'name': 'UPPER_NAME', 'expression': 'upper(NAME)'}
                    ]
                }
            ],
            'sinks': [
                {
                    'name': 'TGT_Test',
                    'type': 'Sink',
                    'dataset': {
                        'type': 'AzureSqlTable',
                        'table': 'TEST_TARGET'
                    },
                    'schema': [
                        {'name': 'ID', 'type': 'Int32'},
                        {'name': 'UPPER_NAME', 'type': 'String'}
                    ]
                }
            ],
            'warnings': ['Test warning'],
            'errors': []
        }

    def test_generator_initialization(self, generator, output_dir):
        """Verifica inicialización del generador"""
        assert generator.output_dir == Path(output_dir)
        assert generator.output_dir.exists()
        assert len(generator.generated_files) == 0

    def test_generate_pipeline(self, generator, sample_translated_structure):
        """Verifica generación de pipeline"""
        pipeline_file = generator.generate_pipeline(
            'TestMapping',
            sample_translated_structure
        )

        # Verificar que el archivo fue creado
        assert Path(pipeline_file).exists()

        # Leer y verificar contenido
        with open(pipeline_file, 'r', encoding='utf-8') as f:
            pipeline = json.load(f)

        assert 'pipeline_TestMapping' in pipeline['name']
        assert 'properties' in pipeline
        assert 'activities' in pipeline['properties']
        assert len(pipeline['properties']['activities']) > 0

    def test_generate_dataflow(self, generator, sample_translated_structure):
        """Verifica generación de dataflow"""
        dataflow_file = generator.generate_dataflow(
            'TestMapping',
            sample_translated_structure
        )

        # Verificar que el archivo fue creado
        assert Path(dataflow_file).exists()

        # Leer y verificar contenido
        with open(dataflow_file, 'r', encoding='utf-8') as f:
            dataflow = json.load(f)

        assert 'dataflow_TestMapping' in dataflow['name']
        assert 'properties' in dataflow
        assert 'typeProperties' in dataflow['properties']

        # Verificar sources, transformations y sinks
        type_props = dataflow['properties']['typeProperties']
        assert 'sources' in type_props
        assert 'transformations' in type_props
        assert 'sinks' in type_props

        assert len(type_props['sources']) == 1
        assert len(type_props['transformations']) == 1
        assert len(type_props['sinks']) == 1

    def test_generate_report(self, generator, sample_translated_structure):
        """Verifica generación de reporte"""
        report_file = generator.generate_report(
            'TestMapping',
            sample_translated_structure
        )

        # Verificar que el archivo fue creado
        assert Path(report_file).exists()

        # Leer y verificar contenido
        with open(report_file, 'r', encoding='utf-8') as f:
            report = json.load(f)

        assert report['mapping_name'] == 'TestMapping'
        assert 'statistics' in report
        assert 'components' in report
        assert 'warnings' in report
        assert 'errors' in report
        assert 'recommendations' in report

        # Verificar estadísticas
        assert 'success_rate' in report['statistics']

        # Verificar componentes
        components = report['components']
        assert components['sources'] == 1
        assert components['transformations'] == 1
        assert components['sinks'] == 1

    def test_generate_all(self, generator, sample_translated_structure):
        """Verifica generación de todos los archivos"""
        files = generator.generate_all(
            'TestMapping',
            sample_translated_structure
        )

        # Verificar que se generaron 3 archivos
        assert 'pipeline' in files
        assert 'dataflow' in files
        assert 'report' in files

        # Verificar que todos existen
        for file_path in files.values():
            assert Path(file_path).exists()

        # Verificar que se agregaron a la lista de archivos generados
        assert len(generator.generated_files) == 3

    def test_validate_json_valid(self, generator, sample_translated_structure):
        """Verifica validación de JSON válido"""
        # Generar un archivo
        pipeline_file = generator.generate_pipeline(
            'TestMapping',
            sample_translated_structure
        )

        # Validar
        is_valid = generator.validate_json(pipeline_file)
        assert is_valid is True

    def test_validate_json_invalid(self, generator, tmp_path):
        """Verifica validación de JSON inválido"""
        # Crear archivo con JSON inválido
        invalid_file = tmp_path / "invalid.json"
        invalid_file.write_text("This is not valid JSON")

        # Validar
        is_valid = generator.validate_json(str(invalid_file))
        assert is_valid is False

    def test_get_generated_files(self, generator, sample_translated_structure):
        """Verifica obtención de lista de archivos generados"""
        # Generar algunos archivos
        generator.generate_pipeline('Test1', sample_translated_structure)
        generator.generate_dataflow('Test2', sample_translated_structure)

        files = generator.get_generated_files()

        assert len(files) == 2
        assert all(Path(f).exists() for f in files)

    def test_recommendations_generation(self, generator, sample_translated_structure):
        """Verifica generación de recomendaciones"""
        report_file = generator.generate_report(
            'TestMapping',
            sample_translated_structure
        )

        with open(report_file, 'r', encoding='utf-8') as f:
            report = json.load(f)

        recommendations = report['recommendations']

        # Debe haber al menos algunas recomendaciones
        assert len(recommendations) > 0

        # Verificar que incluye recomendaciones comunes
        rec_text = ' '.join(recommendations)
        assert 'Linked Services' in rec_text or 'testing' in rec_text.lower()


@pytest.fixture(autouse=True)
def setup_logging():
    """Configura logging para tests"""
    import logging
    logging.basicConfig(level=logging.DEBUG)
