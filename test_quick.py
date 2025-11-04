#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de prueba rápida para verificar que v2.0 funciona correctamente
"""

import sys
import os
from pathlib import Path

# Configurar salida UTF-8 para Windows
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

# Agregar src al path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

def test_imports():
    """Verifica que todos los módulos se importan correctamente"""
    print("[*] Probando imports...")
    try:
        from src.parser import PowerCenterXMLParser, parse_powercenter_xml
        from src.translator import PowerCenterToADFTranslator
        from src.generator import ADFGenerator
        from src.validator import MappingValidator
        from src.utils import print_banner
        print("  [OK] Todos los imports funcionan correctamente")
        return True
    except Exception as e:
        print(f"  [ERROR] Error en imports: {e}")
        return False


def test_parser():
    """Prueba el parser con el XML de ejemplo"""
    print("\n[*] Probando Parser...")
    try:
        from src.parser import parse_powercenter_xml
        xml_file = Path(__file__).parent / 'examples' / 'test_mapping_simple.xml'

        if not xml_file.exists():
            print(f"  [WARNING]  Archivo XML no encontrado: {xml_file}")
            return False

        metadata = parse_powercenter_xml(str(xml_file))

        print(f"  [OK] Mapping parseado: {metadata.name}")
        print(f"     - Sources: {len(metadata.sources)}")
        print(f"     - Transformations: {len(metadata.transformations)}")
        print(f"     - Targets: {len(metadata.targets)}")

        # Verificar transformaciones v2.0
        trans_types = {t.type for t in metadata.transformations}
        expected = {'Source Qualifier', 'Sorter', 'Joiner', 'Expression',
                   'Aggregator', 'Router', 'Update Strategy'}

        found = trans_types & expected
        print(f"     - Transformaciones v2.0 encontradas: {len(found)}")
        for t_type in found:
            print(f"       - {t_type}")

        return True
    except Exception as e:
        print(f"  [ERROR] Error en parser: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_validator():
    """Prueba el validador"""
    print("\n[*] Probando Validator...")
    try:
        from src.parser import parse_powercenter_xml
        from src.validator import MappingValidator

        xml_file = Path(__file__).parent / 'examples' / 'test_mapping_simple.xml'
        metadata = parse_powercenter_xml(str(xml_file))

        validator = MappingValidator()
        errors, warnings = validator.validate(metadata)

        print(f"  [OK] Validación completada")
        print(f"     - Errores: {len(errors)}")
        print(f"     - Warnings: {len(warnings)}")

        if errors:
            print("     Errores encontrados:")
            for error in errors[:3]:
                print(f"       - {error}")

        if warnings:
            print("     Warnings encontrados:")
            for warning in warnings[:3]:
                print(f"       - {warning}")

        return True
    except Exception as e:
        print(f"  [ERROR] Error en validator: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_translator():
    """Prueba el translator"""
    print("\n[*] Probando Translator...")
    try:
        from src.parser import parse_powercenter_xml
        from src.translator import PowerCenterToADFTranslator

        xml_file = Path(__file__).parent / 'examples' / 'test_mapping_simple.xml'
        metadata = parse_powercenter_xml(str(xml_file))

        translator = PowerCenterToADFTranslator()
        translated = translator.translate_mapping(metadata)

        print(f"  [OK] Traducción completada")
        print(f"     - Transformaciones traducidas: {len(translated['transformations'])}")
        print(f"     - Warnings: {len(translated['warnings'])}")
        print(f"     - Errors: {len(translated['errors'])}")

        # Mostrar tipos traducidos
        trans_types = {t['type'] for t in translated['transformations']}
        print(f"     - Tipos de transformaciones ADF:")
        for t_type in trans_types:
            print(f"       - {t_type}")

        return True
    except Exception as e:
        print(f"  [ERROR] Error en translator: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_generator():
    """Prueba el generator"""
    print("\n[*] Probando Generator...")
    try:
        from src.parser import parse_powercenter_xml
        from src.translator import PowerCenterToADFTranslator
        from src.generator import ADFGenerator

        xml_file = Path(__file__).parent / 'examples' / 'test_mapping_simple.xml'
        metadata = parse_powercenter_xml(str(xml_file))

        translator = PowerCenterToADFTranslator()
        translated = translator.translate_mapping(metadata)

        output_dir = Path(__file__).parent / 'output' / 'test_quick'
        output_dir.mkdir(parents=True, exist_ok=True)

        generator = ADFGenerator(str(output_dir))
        files = generator.generate_all(metadata.name, translated, metadata)

        print(f"  [OK] Archivos generados:")
        for file_type, file_path in files.items():
            file_name = Path(file_path).name
            print(f"     - {file_type}: {file_name}")

            # Verificar que el archivo existe y tiene contenido
            if Path(file_path).exists():
                size = Path(file_path).stat().st_size
                print(f"       [*] Tamaño: {size} bytes")

        return True
    except Exception as e:
        print(f"  [ERROR] Error en generator: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_integration():
    """Prueba integración completa"""
    print("\n[*] Probando Integración Completa (E2E)...")
    try:
        # Import main function
        from src.main import run_migration

        xml_file = str(Path(__file__).parent / 'examples' / 'test_mapping_simple.xml')
        output_dir = str(Path(__file__).parent / 'output' / 'test_integration')

        # Ejecutar migración
        success = run_migration(
            input_file=xml_file,
            output_dir=output_dir,
            validate_only=False,
            skip_validation=False
        )

        if success:
            print(f"  [OK] Migración E2E completada exitosamente")
            return True
        else:
            print(f"  [ERROR] Migración E2E falló")
            return False

    except Exception as e:
        print(f"  [ERROR] Error en integración: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Ejecuta todas las pruebas"""
    print("=" * 70)
    print("  PowerCenter to ADF Migrator v2.0 - Quick Test")
    print("=" * 70)

    results = {
        'Imports': test_imports(),
        'Parser': test_parser(),
        'Validator': test_validator(),
        'Translator': test_translator(),
        'Generator': test_generator(),
        'Integration': test_integration()
    }

    print("\n" + "=" * 70)
    print("  RESUMEN DE PRUEBAS")
    print("=" * 70)

    for test_name, result in results.items():
        status = "[OK] PASS" if result else "[ERROR] FAIL"
        print(f"  {test_name:20s}: {status}")

    print("=" * 70)

    total_tests = len(results)
    passed_tests = sum(results.values())

    print(f"\n  Total: {passed_tests}/{total_tests} pruebas pasaron")

    if passed_tests == total_tests:
        print("\n  [SUCCESS] ¡TODAS LAS PRUEBAS PASARON! v2.0 está funcionando correctamente.\n")
        return 0
    else:
        print(f"\n  [WARNING]  {total_tests - passed_tests} prueba(s) fallaron. Revisa los errores arriba.\n")
        return 1


if __name__ == '__main__':
    sys.exit(main())
