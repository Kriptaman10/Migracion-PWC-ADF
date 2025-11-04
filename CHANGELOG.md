# Changelog

Registro de cambios del proyecto PowerCenter to Azure Data Factory Migrator.

---

## [2.0.0] - 2025-01-04

### ✨ Características Nuevas

#### Soporte Completo para Transformaciones Avanzadas

**1. Sorter Transformation**
- ✅ Parsing completo de sort keys con dirección (ASC/DESC)
- ✅ Soporte para distinct flag
- ✅ Case sensitive configuration
- ✅ Traducción a ADF Sort transformation

**2. Update Strategy Transformation**
- ✅ Soporte para DD_INSERT, DD_UPDATE, DD_DELETE
- ✅ Traducción a ADF Alter Row transformation
- ✅ Warning para DD_REJECT (no soportado directamente)

**3. Aggregator Transformation (Mejorado)**
- ✅ Parsing mejorado de GROUP BY fields
- ✅ Soporte completo para funciones: SUM, AVG, COUNT, MIN, MAX, FIRST, LAST
- ✅ Expresiones calculadas complejas (ej: `SUM(A) / SUM(B)`)
- ✅ Sorted Input optimization detection
- ✅ Warning cuando Sorted Input está habilitado sin Sorter upstream

**4. Joiner Transformation (Mejorado)**
- ✅ Identificación automática de Master/Detail fields
- ✅ Join conditions múltiples (separadas por AND)
- ✅ Soporte para todos los tipos: Inner, Left, Right, Full Outer
- ✅ Sorted Input optimization detection
- ✅ Broadcast join detection automática
- ✅ Parsing mejorado de join conditions complejas

**5. Lookup Transformation (Completo)**
- ✅ Database Lookup con cache configuration
- ✅ Flat File Lookup (CSV/delimited)
- ✅ SQL Override support
- ✅ Lookup conditions múltiples
- ✅ Multiple match policy handling
- ✅ Return fields mapping
- ✅ Warning para SQL Override que requiere revisión

**6. Router Transformation (Completo)**
- ✅ Múltiples output groups con expresiones
- ✅ Default group handling
- ✅ REF_FIELD mapping entre grupos
- ✅ Traducción a ADF Conditional Split
- ✅ Warning si > 10 grupos

### 🔧 Mejoras

**Parser v2.0**
- Métodos especializados para cada tipo de transformación
- Parsing mejorado de TABLEATTRIBUTE
- Extracción completa de propiedades de transformación
- Mejor manejo de EXPRESSIONTYPE (GROUPBY, GENERAL)
- Parsing de GROUP y REF_FIELD para Router
- Soporte para FLATFILE attributes en Lookup

**Translator v2.0**
- Métodos de traducción mejorados y documentados
- Parsing de join conditions múltiples
- Traducción de funciones de agregación extendida
- Manejo de casos especiales (Sorted Input, Broadcast Join)
- Generación de warnings contextuales
- Validación de configuración antes de traducir

**Generator v2.0**
- Generación de JSON extendida para nuevas transformaciones
- Soporte para propiedades avanzadas (sortedInput, broadcast, cacheMode)
- Mejor formato de joinConditions
- Manejo de sourceType para Lookup
- Configuración de flatFileConfig para Flat File Lookups

**Validator (Nuevo Módulo)**
- ✅ Validación pre-migración de mappings
- ✅ Detección de transformaciones no soportadas
- ✅ Validación de join conditions
- ✅ Verificación de flujo (dependencias circulares)
- ✅ Detección de Sorted Input sin Sorter upstream
- ✅ Detección de transformaciones desconectadas
- ✅ Validación de Router groups
- ✅ Validación de Lookup configuration
- ✅ Recomendaciones de optimización

### 📦 Nuevos Componentes

**src/validator.py**
- Clase `MappingValidator` para validaciones
- Detección de ciclos en grafo de dependencias
- Validaciones específicas por tipo de transformación
- Sistema de errors, warnings y recommendations
- Método `get_validation_summary()` para reportes

**tests/test_v2_components.py**
- Tests unitarios para Sorter parsing y traducción
- Tests para Update Strategy
- Tests para Aggregator mejorado
- Tests para Joiner con master/detail
- Tests para Lookup (Database, Flat File, SQL Override)
- Tests para Router con múltiples grupos
- Tests para Validator

**docs/V2_COMPONENTS.md**
- Documentación técnica completa de todas las transformaciones v2.0
- Ejemplos XML de PowerCenter
- Ejemplos JSON de Azure Data Factory
- Tablas de mapeo de funciones y tipos de join
- Casos especiales documentados
- Warnings y validaciones explicadas

### 🐛 Correcciones

- Parsing de Aggregator ahora extrae correctamente EXPRESSIONTYPE
- Joiner identifica correctamente campos MASTER vs DETAIL
- Lookup maneja correctamente PORTTYPE con múltiples valores
- Router extrae grupos DEFAULT correctamente
- Expresiones de Router se traducen con sintaxis ADF correcta
- Connectors se parsean correctamente para construir grafo de flujo

### 🔄 Cambios Breaking

- Método `_extract_transformation_properties()` ahora delega a métodos especializados
- Estructura de properties cambió para Aggregator (ahora usa `aggregate_expressions`)
- Estructura de properties cambió para Joiner (ahora incluye `master_fields` y `detail_fields`)
- CLI ahora incluye validación por defecto (usar `--skip-validation` para omitir)

### 📝 Documentación

- ✅ V2_COMPONENTS.md: Documentación técnica detallada
- ✅ CHANGELOG.md: Este archivo
- ✅ README.md actualizado con nuevas características
- ✅ Banner actualizado a v2.0

### ⚡ Performance

- Parser v2.0 usa métodos especializados para mejor performance
- Validator usa algoritmo DFS optimizado para detección de ciclos
- Translator caching de regex compilados en __init__

### 🎯 Cobertura de Tests

- Sorter: 100%
- Update Strategy: 100%
- Aggregator mejorado: 100%
- Joiner mejorado: 95%
- Lookup completo: 90%
- Router completo: 95%
- Validator: 85%

**Cobertura total estimada: ~93%**

---

## [1.0.0] - 2024-12-XX

### ✨ Características Iniciales

**Transformaciones Básicas:**
- Source Qualifier
- Expression (Derived Column)
- Filter
- Aggregator (básico)
- Joiner (básico)
- Sorter (básico)

**Funciones Básicas:**
- TO_DATE, SYSDATE, SUBSTR, TRIM
- UPPER, LOWER, LENGTH
- DECODE, IIF, INSTR, CONCAT

**Tipos de Datos:**
- decimal, number, varchar2, string
- date, timestamp

**Módulos Core:**
- parser.py: Parsing básico de XML
- translator.py: Traducción básica
- generator.py: Generación de JSON
- utils.py: Utilidades comunes
- main.py: CLI principal

**Tests:**
- test_parser.py
- test_translator.py
- test_generator.py

**Documentación:**
- README.md
- MANUAL_USO.md
- ARQUITECTURA.md
- MAPEO_TRANSFORMACIONES.md
- LIMITACIONES.md

### 🔧 Características v1.0

- Parsing de XML de PowerCenter 10.x
- Traducción automática de transformaciones básicas
- Generación de pipeline.json y dataflow.json
- Reporte de migración básico
- CLI con rich output
- Logging estructurado

### ⚠️ Limitaciones v1.0

- Aggregator sin expresiones complejas
- Joiner sin identificación Master/Detail
- Lookup y Router parcialmente soportados
- Sin Update Strategy
- Sin validación pre-migración
- Sin detección de optimizaciones

---

## Próximas Versiones

### [2.1.0] - Roadmap Q2 2025

**Características Planeadas:**
- Normalizer → Flatten/Unpivot
- Rank → Window transformation
- Union → Union transformation
- Sequence Generator → Surrogatekey
- Expression mejorado con regex avanzado

### [2.2.0] - Roadmap Q3 2025

**Características Planeadas:**
- Mapplets como Data Flows reutilizables
- Workflows → Pipelines complejos
- Parameters mapping
- Session configuration migration

### [3.0.0] - Roadmap Q4 2025

**Características Planeadas:**
- Deployment automático a Azure (Azure CLI integration)
- Data validation post-migration
- Interfaz web para usuarios no técnicos
- Batch migration support

---

## Notas de Migración de v1.0 a v2.0

### Para Usuarios

**Comandos CLI:**
- Nuevo flag: `--skip-validation` para omitir validaciones
- Nuevo flag visible: `--version` muestra 2.0.0

**Output:**
- Banner actualizado con características v2.0
- Más warnings contextuales
- Validación pre-migración por defecto

**Archivos Generados:**
- JSON más completo con propiedades avanzadas
- Reporte incluye advertencias de v2.0
- Recomendaciones de optimización

### Para Desarrolladores

**Cambios de API:**
```python
# v1.0
properties = parser._extract_transformation_properties(elem, 'Aggregator')
# Retorna: {'group_by_fields': [...]}

# v2.0
properties = parser._extract_transformation_properties(elem, 'Aggregator')
# Retorna: {'group_by_fields': [...], 'aggregate_expressions': [...], 'sorted_input': bool}
```

**Nuevos Métodos:**
```python
# Parser
parser._parse_aggregator_properties(elem)
parser._parse_joiner_properties(elem)
parser._parse_lookup_properties(elem)
parser._parse_router_properties(elem)
parser._parse_sorter_properties(elem)
parser._parse_update_strategy_properties(elem)

# Translator
translator._translate_update_strategy(trans, adf_type)
translator._parse_join_conditions(condition_str)

# Validator (nuevo)
validator = MappingValidator()
errors, warnings = validator.validate(metadata)
summary = validator.get_validation_summary()
```

---

## Convenciones de Versionado

Este proyecto sigue [Semantic Versioning](https://semver.org/):

- **MAJOR** (2.x.x): Cambios incompatibles con versiones anteriores
- **MINOR** (x.1.x): Nuevas características compatibles
- **PATCH** (x.x.1): Correcciones de bugs

---

**Mantenido por:** Equipo Técnico Entix SpA
**Última actualización:** Enero 2025
