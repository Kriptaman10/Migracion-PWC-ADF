"""
PowerCenter to Azure Data Factory Migrator
CLI principal para ejecutar la migración
"""

import sys
import argparse
from pathlib import Path
from typing import Optional
import logging

from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.panel import Panel
from rich import print as rprint

from .parser import parse_powercenter_xml
from .translator import PowerCenterToADFTranslator
from .script_generator import ADFScriptGenerator as ADFGenerator
from .validator import MappingValidator
from .utils import (
    setup_logging,
    validate_file_path,
    create_output_directory,
    print_banner,
    MigrationError,
    ValidationError
)

# Configurar consola para compatibilidad con Windows
console = Console(legacy_windows=False, force_terminal=True)
logger = logging.getLogger('pc-to-adf')


def parse_arguments() -> argparse.Namespace:
    """
    Parsea argumentos de línea de comandos.

    Returns:
        Namespace con los argumentos parseados
    """
    parser = argparse.ArgumentParser(
        description='Migra mappings de PowerCenter a Azure Data Factory',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  # Migración básica
  %(prog)s input/mapping.xml

  # Especificar directorio de salida
  %(prog)s input/mapping.xml --output adf_output/

  # Modo verbose para debugging
  %(prog)s input/mapping.xml --verbose

  # Solo validar sin generar archivos
  %(prog)s input/mapping.xml --validate-only

Para más información: https://github.com/entix/powercenter-to-adf
        """
    )

    parser.add_argument(
        'input_file',
        type=str,
        help='Archivo XML de PowerCenter a migrar'
    )

    parser.add_argument(
        '-o', '--output',
        type=str,
        default='./output',
        help='Directorio de salida para archivos generados (default: ./output)'
    )

    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Activa modo verbose para debugging'
    )

    parser.add_argument(
        '--validate-only',
        action='store_true',
        help='Solo valida el XML sin generar archivos'
    )

    parser.add_argument(
        '--log-file',
        type=str,
        default=None,
        help='Archivo para guardar logs (opcional)'
    )

    parser.add_argument(
        '--version',
        action='version',
        version='%(prog)s 2.0.0'
    )

    parser.add_argument(
        '--skip-validation',
        action='store_true',
        help='Omitir validaciones pre-migración'
    )

    return parser.parse_args()


def display_summary(
    mapping_name: str,
    stats: dict,
    generated_files: dict,
    warnings: list,
    errors: list
) -> None:
    """
    Muestra un resumen de la migración.

    Args:
        mapping_name: Nombre del mapping migrado
        stats: Estadísticas de la migración
        generated_files: Archivos generados
        warnings: Lista de warnings
        errors: Lista de errores
    """
    # Resumen
    print("\n" + "="*60)
    print(f"MIGRACION COMPLETADA: {mapping_name}")
    print("="*60)

    # Estadísticas
    print("\nEstadisticas de Migracion:")
    print("-"*60)
    print(f"  Total Transformaciones:     {stats['total_transformations']}")
    print(f"  Migradas Exitosamente:      {stats['migrated_transformations']}")
    print(f"  Tasa de Exito:              {stats['success_rate']}%")
    print(f"  Warnings:                   {stats['warnings']}")
    print(f"  Errores:                    {stats['errors']}")
    print("-"*60)

    # Archivos generados
    if generated_files:
        print("\nArchivos Generados:")
        print("-"*60)
        for file_type, file_path in generated_files.items():
            print(f"  {file_type.capitalize():12s}: {Path(file_path).name}")
        print("-"*60)

    # Warnings
    if warnings:
        print("\nWARNINGS:")
        print("-"*60)
        for warning in warnings[:5]:  # Mostrar solo primeros 5
            print(f"  - {warning}")
        if len(warnings) > 5:
            print(f"  ... y {len(warnings) - 5} mas (ver reporte completo)")
        print("-"*60)

    # Errores
    if errors:
        print("\nERRORES:")
        print("-"*60)
        for error in errors[:5]:  # Mostrar solo primeros 5
            print(f"  - {error}")
        if len(errors) > 5:
            print(f"  ... y {len(errors) - 5} mas (ver reporte completo)")
        print("-"*60)


def run_migration(
    input_file: str,
    output_dir: str,
    validate_only: bool = False,
    skip_validation: bool = False
) -> bool:
    """
    Ejecuta el proceso completo de migración.

    Args:
        input_file: Ruta al archivo XML de PowerCenter
        output_dir: Directorio de salida
        validate_only: Si True, solo valida sin generar archivos
        skip_validation: Si True, omite validaciones pre-migración

    Returns:
        True si la migración fue exitosa, False en caso contrario
    """
    try:
        # Validar archivo de entrada
        print("Validando archivo de entrada...")
        xml_path = validate_file_path(input_file, '.xml')
        print(f"[OK] Archivo válido: {xml_path}")

        # Parsear XML
        print("Parseando XML de PowerCenter...")
        metadata = parse_powercenter_xml(str(xml_path))
        print(f"[OK] Mapping parseado: {metadata.name}")
        print(f"  - Fuentes: {len(metadata.sources)}")
        print(f"  - Transformaciones: {len(metadata.transformations)}")
        print(f"  - Destinos: {len(metadata.targets)}")

        # Validar mapping (v2.0)
        if not skip_validation:
            print("\nValidando mapping...")
            validator = MappingValidator()
            errors, warnings = validator.validate(metadata)

            if errors:
                print(f"[ERROR] Validación falló con {len(errors)} errores:")
                for error in errors[:5]:
                    print(f"  - {error}")
                if len(errors) > 5:
                    print(f"  ... y {len(errors) - 5} más")
                return False

            if warnings:
                print(f"[WARNING] {len(warnings)} advertencias encontradas:")
                for warning in warnings[:3]:
                    print(f"  - {warning}")
                if len(warnings) > 3:
                    print(f"  ... y {len(warnings) - 3} más")

            print(f"[OK] Validación completada")

        if validate_only:
            print("\n[OK] Validacion exitosa")
            return True

        # Traducir a ADF
        print("\nTraduciendo a Azure Data Factory...")
        translator = PowerCenterToADFTranslator()
        translated = translator.translate_mapping(metadata)
        print(f"[OK] Traduccion completada")
        print(f"  - Transformaciones migradas: {len(translated['transformations'])}")
        print(f"  - Warnings: {len(translated['warnings'])}")
        print(f"  - Errores: {len(translated['errors'])}")

        # Generar archivos de salida
        create_output_directory(output_dir)

        print("\nGenerando archivos de ADF...")
        generator = ADFGenerator(output_dir)
        files = generator.generate_all(metadata.name, translated, metadata)
        print(f"[OK] Archivos generados en: {output_dir}")

        # Calcular estadísticas
        from .utils import calculate_migration_stats
        stats = calculate_migration_stats(
            len(metadata.transformations),
            len(translated['transformations']),
            len(translated['warnings']),
            len(translated['errors'])
        )

        # Mostrar resumen
        display_summary(
            metadata.name,
            stats,
            files,
            translated['warnings'],
            translated['errors']
        )

        # Próximos pasos
        print("\n" + "="*60)
        print("PROXIMOS PASOS:")
        print("="*60)
        print("1. Revisar el reporte de migracion generado")
        print("2. Configurar Linked Services en Azure Data Factory")
        print("3. Importar el pipeline y dataflow a ADF")
        print("4. Validar y ajustar transformaciones manualmente si es necesario")
        print("5. Ejecutar pruebas de validacion de datos")
        print("="*60)

        return True

    except ValidationError as e:
        print(f"\n[ERROR] Error de validacion: {e}")
        logger.error(f"Validation error: {e}")
        return False

    except MigrationError as e:
        print(f"\n[ERROR] Error de migracion: {e}")
        logger.error(f"Migration error: {e}")
        return False

    except Exception as e:
        print(f"\n[ERROR] Error inesperado: {e}")
        logger.exception("Unexpected error during migration")
        return False


def main() -> int:
    """
    Función principal del CLI.

    Returns:
        Código de salida (0 = éxito, 1 = error)
    """
    # Mostrar banner
    print_banner()

    # Parsear argumentos
    args = parse_arguments()

    # Configurar logging
    setup_logging(verbose=args.verbose, log_file=args.log_file)

    logger.info("=== Iniciando PowerCenter to ADF Migrator ===")
    logger.info(f"Input: {args.input_file}")
    logger.info(f"Output: {args.output}")
    logger.info(f"Validate only: {args.validate_only}")

    # Ejecutar migración
    success = run_migration(
        args.input_file,
        args.output,
        args.validate_only,
        args.skip_validation
    )

    if success:
        logger.info("=== Migración completada exitosamente ===")
        return 0
    else:
        logger.error("=== Migración falló ===")
        return 1


if __name__ == '__main__':
    sys.exit(main())
