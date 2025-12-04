# PowerCenter to Azure Data Factory Migrator

![Python Version](https://img.shields.io/badge/python-3.8%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)
![Version](https://img.shields.io/badge/version-2.0-brightgreen)

Herramienta automatizada para migrar mappings de Informatica PowerCenter a Azure Data Factory con interfaz CLI y Web.

---

## Descripción

Automatiza la migración de pipelines ETL desde Informatica PowerCenter a Azure Data Factory, reduciendo el tiempo de migración en un **70-80%** para transformaciones comunes. Genera pipelines y dataflows listos para importar en Azure con reportes detallados de migración.

**Versión 2.0** incluye:
- Soporte completo para transformaciones avanzadas (Lookup, Router, Update Strategy)
- Interfaz web interactiva con Streamlit
- Validación de formato y estructura de ADF
- Generación de scripts de despliegue
- Comparación lado a lado PowerCenter vs ADF

---

## Instalación Rápida

### Prerrequisitos
- Python 3.8+
- pip

### Instalación

```bash
# Clonar repositorio
git clone https://github.com/entix/powercenter-to-adf.git
cd powercenter-to-adf

# Crear entorno virtual
python -m venv venv

# Activar entorno
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

# Instalar dependencias
pip install -r requirements.txt

# Para interfaz web, instalar dependencias adicionales
pip install -r requirements-streamlit.txt
```

---

## Uso

### Interfaz Web (Recomendado)

```bash
# Ejecutar aplicación web
streamlit run app.py

# O usar el script incluido
# Windows:
run_web.bat
# Linux/Mac:
./run_web.sh
```

La aplicación web permite:
- Cargar múltiples archivos XML en batch
- Visualización lado a lado del mapping original y migrado
- Exportar resultados en formato ZIP
- Configuración de parámetros de migración
- Vista previa de archivos generados

### CLI

```bash
# Migración básica
python -m src.main input/mapping.xml

# Especificar directorio de salida
python -m src.main input/mapping.xml --output adf_output/

# Modo verbose
python -m src.main input/mapping.xml --verbose

# Solo validar sin generar archivos
python -m src.main input/mapping.xml --validate-only

# Ver ayuda
python -m src.main --help
```

### Flujo de Trabajo

1. **Exportar** mapping desde PowerCenter (formato XML)
2. **Ejecutar** migración (CLI o Web)
3. **Revisar** archivos generados:
   - `pipeline_[nombre].json` - Pipeline de ADF
   - `dataflow_[nombre].json` - Dataflow con transformaciones
   - `deployment_script.ps1` - Script de despliegue
   - `migration_report.json` - Reporte detallado
4. **Importar** a Azure Data Factory
5. **Validar** y ajustar según reporte

---

## Transformaciones Soportadas

| PowerCenter | Azure Data Factory | v2.0 |
|-------------|-------------------|------|
| Source Qualifier | Source | ✅ Completo |
| Expression | Derived Column | ✅ Completo |
| Filter | Filter | ✅ Completo |
| Aggregator | Aggregate | ✅ Completo |
| Joiner | Join | ✅ Completo |
| Sorter | Sort | ✅ Completo |
| Router | Conditional Split | ✅ Completo |
| Lookup | Lookup | ✅ Completo |
| Update Strategy | Alter Row | ✅ Completo |
| Target | Sink | ✅ Completo |

### Funciones Traducidas

PowerCenter → Azure Data Factory:
- `TO_DATE()` → `toDate()`
- `SYSDATE` → `currentTimestamp()`
- `SUBSTR()` → `substring()`
- `DECODE()` → `case when...then`
- `IIF()` → `iif()`
- `INSTR()` → `indexOf()`
- `TRIM()`, `UPPER()`, `LOWER()`, `LENGTH()`, `CONCAT()` y más

---

## Características v2.0

- **Parser XML robusto** para PowerCenter 10.x
- **Traducción automática** de expresiones y funciones
- **Validación de formato** JSON para Azure Data Factory
- **Interfaz web interactiva** con Streamlit
- **Migración batch** de múltiples mappings
- **Generación de scripts** de despliegue PowerShell
- **Reportes detallados** con estadísticas y warnings
- **Comparación lado a lado** de estructuras
- **Soporte completo** para transformaciones avanzadas

---

## Estructura del Proyecto

```
Migracion-PWC-ADF/
├── app.py                    # Aplicación web Streamlit
├── src/                      # Código fuente
│   ├── main.py              # CLI principal
│   ├── parser.py            # Parser XML PowerCenter
│   ├── translator.py        # Traductor PC → ADF
│   ├── generator.py         # Generador JSON ADF
│   ├── validator.py         # Validador de mappings
│   ├── script_generator.py  # Generador de scripts
│   └── utils.py             # Utilidades
├── components/              # Componentes Streamlit
│   ├── upload_component.py
│   ├── preview_component.py
│   ├── export_component.py
│   └── config_component.py
├── config/                  # Configuración
│   └── mapping_rules.json   # Reglas de mapeo
├── tests/                   # Tests unitarios
├── docs/                    # Documentación
└── examples/                # Ejemplos
```

---

## Limitaciones

### Requiere Ajuste Manual

- **Linked Services**: Configurar conexiones en Azure
- **SQL Overrides**: Revisar queries personalizados en Lookups
- **Expresiones complejas**: Validar funciones avanzadas
- **Performance tuning**: Ajustar partitioning y paralelismo

### No Soportado

- Mapplets (migrar como Data Flows separados)
- Workflows y sesiones
- Parámetros de sesión (se generan como valores fijos)
- Funciones personalizadas
- Normalizer y Custom Transformations

Ver [docs/LIMITACIONES.md](docs/LIMITACIONES.md) para más detalles.

---

## Testing

```bash
# Ejecutar tests
pytest

# Con coverage
pytest --cov=src

# Modo verbose
pytest -v
```

---

## Documentación

- [Manual de Uso](docs/MANUAL_USO.md)
- [Arquitectura](docs/ARQUITECTURA.md)
- [Mapeo de Transformaciones](docs/MAPEO_TRANSFORMACIONES.md)
- [Limitaciones](docs/LIMITACIONES.md)
- [Guía de Prueba](GUIA_PRUEBA.md)
- [Changelog](CHANGELOG.md)

---

## Licencia

MIT License - Copyright (c) 2025 Entix SpA

Ver [LICENSE](LICENSE) para más detalles.

---

## Créditos

Desarrollado por **Benjamín Riquelme** durante práctica profesional en **[Entix SpA](https://entix.cl)** en colaboración con la **Universidad Tecnológica Metropolitana (UTEM)**.

**Entix SpA** - Consultoría especializada en:
- Modernización de plataformas de datos
- Migraciones cloud (Azure, AWS, GCP)
- Data Lakes y Data Warehouses
- Integración de datos enterprise

---

## Contacto

- **Empresa**: Entix SpA
- **Web**: [https://entix.cl](https://entix.cl)
- **Email**: contacto@entix.cl

---

Si este proyecto te resulta útil, considera darle una estrella ⭐
