# 🧪 Guía de Prueba - PowerCenter to ADF Migrator v2.0

Guía paso a paso para probar todas las características de la versión 2.0.

---

## 📋 Pre-requisitos

✅ Python 3.8 o superior instalado
✅ pip funcionando correctamente

---

## 🚀 Paso 1: Instalar la Herramienta

```bash
# Navegar al directorio del proyecto
cd "C:\Users\benja\OneDrive\Documentos\GitHub\Migracion-PWC-ADF"

# Instalar dependencias
pip install -r requirements.txt

# Instalar la herramienta en modo desarrollo
pip install -e .

# Verificar instalación
pc-to-adf --version
```

**Salida esperada:**
```
pc-to-adf 2.0.0
```

---

## 🧪 Paso 2: Ejecutar Tests Unitarios

```bash
# Ejecutar todos los tests
pytest tests/test_v2_components.py -v

# O ejecutar con cobertura
pytest tests/test_v2_components.py -v --cov=src
```

**Salida esperada:**
```
============================= 14 passed in 0.15s ==============================
```

---

## 🎯 Paso 3: Probar con Mapping de Ejemplo

### Opción A: Migración Completa (Recomendado)

```bash
# Migrar el mapping de ejemplo
pc-to-adf examples/test_mapping_simple.xml --output output/prueba1

# Con verbose para ver detalles
pc-to-adf examples/test_mapping_simple.xml --output output/prueba1 --verbose
```

**Salida esperada:**
```
===============================================================
  PowerCenter to Azure Data Factory Migrator v2.0
  Desarrollado por: Entix SpA

  Nueva v2.0: Soporte completo para transformaciones avanzadas
  - Joiner (Inner, Left, Right, Full Outer)
  - Aggregator (SUM, AVG, COUNT, MIN, MAX)
  - Lookup (Database, Flat File, SQL Override)
  - Router (Conditional Split con múltiples salidas)
  - Sorter (Ordenamiento múltiple)
  - Update Strategy (INSERT, UPDATE, DELETE)
===============================================================

Validando archivo de entrada...
[OK] Archivo válido: examples\test_mapping_simple.xml

Parseando XML de PowerCenter...
[OK] Mapping parseado: m_Test_Simple_v2
  - Fuentes: 2
  - Transformaciones: 8
  - Destinos: 1

Validando mapping...
[WARNING] X advertencias encontradas:
  - ...
[OK] Validación completada

Traduciendo a Azure Data Factory...
[OK] Traduccion completada
  - Transformaciones migradas: 8
  - Warnings: X
  - Errores: 0

Generando archivos de ADF...
[OK] Archivos generados en: output/prueba1

==============================================================
MIGRACION COMPLETADA: m_Test_Simple_v2
==============================================================
```

### Opción B: Solo Validación

```bash
# Solo validar sin generar archivos
pc-to-adf examples/test_mapping_simple.xml --validate-only
```

### Opción C: Sin Validación (más rápido)

```bash
# Omitir validaciones pre-migración
pc-to-adf examples/test_mapping_simple.xml --output output/prueba2 --skip-validation
```

---

## 📁 Paso 4: Revisar Archivos Generados

Después de ejecutar la migración, encontrarás estos archivos en `output/prueba1/`:

```
output/prueba1/
├── pipeline_m_Test_Simple_v2_YYYYMMDD_HHMMSS.json
├── dataflow_m_Test_Simple_v2_YYYYMMDD_HHMMSS.json
└── migration_report_m_Test_Simple_v2_YYYYMMDD_HHMMSS.json
```

### Ver el Pipeline generado:

```bash
# Windows
type output\prueba1\pipeline_*.json

# Linux/Mac
cat output/prueba1/pipeline_*.json
```

### Ver el Dataflow generado:

```bash
# Windows
type output\prueba1\dataflow_*.json

# Linux/Mac
cat output/prueba1/dataflow_*.json
```

### Ver el Reporte de Migración:

```bash
# Windows
type output\prueba1\migration_report_*.json

# Linux/Mac
cat output/prueba1/migration_report_*.json
```

---

## 🔍 Paso 5: Verificar Transformaciones v2.0

El mapping de ejemplo incluye **TODAS las transformaciones v2.0**:

### 1. ✅ Sorter (`SRT_POR_PRODUCTO`)
- Ordena por PRODUCTO (ASCENDING)
- Distinct = NO

### 2. ✅ Joiner (`JNR_VENTAS_PRODUCTOS`)
- Inner Join
- Join Condition: PRODUCTO = PRODUCTO_ID
- Master: VENTAS, Detail: PRODUCTOS

### 3. ✅ Expression (`EXP_CALCULAR_TOTAL`)
- Calcula: TOTAL_VENTA = CANTIDAD * PRECIO
- Usa función SYSDATE

### 4. ✅ Aggregator (`AGG_POR_CATEGORIA`)
- GROUP BY: CATEGORIA
- Agregaciones: SUM(TOTAL_VENTA), SUM(CANTIDAD), AVG(PRECIO)

### 5. ✅ Router (`RTR_SEPARAR_CATEGORIAS`)
- Grupo 1: CATEGORIA_ALTA (TOTAL_VENTAS > 10000)
- Grupo 2: CATEGORIA_MEDIA (5000 < TOTAL_VENTAS <= 10000)
- Grupo 3: DEFAULT

### 6. ✅ Update Strategy (`UPD_STRATEGY`)
- Estrategia: DD_INSERT

---

## 📊 Paso 6: Verificar Validaciones

Para ver las validaciones en acción, puedes modificar el XML:

### Ejemplo 1: Joiner sin Join Condition

```bash
# Edita el XML y elimina la línea:
# <TABLEATTRIBUTE NAME="Join Condition" VALUE="PRODUCTO = PRODUCTO_ID"/>

# Ejecuta la migración
pc-to-adf examples/test_mapping_simple.xml --output output/test_error
```

**Salida esperada:**
```
[ERROR] Validación falló con 1 errores:
  - Joiner 'JNR_VENTAS_PRODUCTOS' no tiene join condition definida
```

### Ejemplo 2: Ver Warnings

El mapping de ejemplo ya genera warnings automáticamente. Revisa el reporte:

```json
{
  "warnings": [
    "Source Qualifier 'SQ_VENTAS' has Source Filter. Review for ADF compatibility.",
    "Aggregator 'AGG_POR_CATEGORIA' uses complex expressions. Verify translation."
  ]
}
```

---

## 🧮 Paso 7: Probar Casos Específicos

### Test 1: Sorter con Distinct

Crea un XML con:
```xml
<TABLEATTRIBUTE NAME="Distinct" VALUE="YES"/>
```

### Test 2: Joiner con Sorted Input

Crea un XML con:
```xml
<TABLEATTRIBUTE NAME="Sorted Input" VALUE="YES"/>
```

Deberías ver un warning:
```
[WARNING] Joiner 'XXX' has Sorted Input enabled. Ensure upstream Sort exists.
```

### Test 3: Aggregator con expresiones complejas

```xml
<TRANSFORMFIELD NAME="RATIO"
                EXPRESSION="SUM(REVENUE) / SUM(COST)"
                EXPRESSIONTYPE="GENERAL"/>
```

---

## 🎨 Paso 8: Probar con tus propios XMLs

Si tienes archivos XML de PowerCenter reales:

```bash
# Coloca tu XML en la carpeta examples/
# Por ejemplo: examples/mi_mapping.xml

# Ejecuta la migración
pc-to-adf examples/mi_mapping.xml --output output/mi_mapping

# Revisa el reporte generado
type output\mi_mapping\migration_report_*.json
```

---

## 🐛 Troubleshooting

### Error: "Module not found"

```bash
# Re-instalar dependencias
pip install -r requirements.txt
pip install -e .
```

### Error: "Permission denied"

```bash
# En Windows, ejecuta como Administrador
# O usa un directorio con permisos de escritura
pc-to-adf examples/test_mapping_simple.xml --output C:\temp\output
```

### Error: "lxml not found"

```bash
# Instalar lxml específicamente
pip install lxml
```

### Los tests fallan

```bash
# Verificar versión de Python
python --version  # Debe ser 3.8+

# Re-instalar desde cero
pip uninstall pc-to-adf
pip install -e .
pytest tests/test_v2_components.py -v
```

---

## 📚 Recursos Adicionales

- **Documentación Técnica**: `docs/V2_COMPONENTS.md`
- **Changelog**: `CHANGELOG.md`
- **Arquitectura**: `docs/ARQUITECTURA.md`
- **Limitaciones**: `docs/LIMITACIONES.md`

---

## ✅ Checklist de Prueba Completa

- [ ] Instalación exitosa (`pc-to-adf --version`)
- [ ] Tests unitarios pasan (14/14)
- [ ] Migración de ejemplo funciona
- [ ] Archivos JSON generados correctamente
- [ ] Reporte de migración tiene información completa
- [ ] Validaciones detectan errores correctamente
- [ ] Warnings se muestran apropiadamente
- [ ] Todas las 6 transformaciones v2.0 se traducen

---

## 🎉 Siguiente Paso

Una vez que hayas verificado que todo funciona:

1. **Importa a Azure Data Factory**:
   - Azure Portal → Data Factory
   - Author → Pipeline → Import from template
   - Sube el archivo `pipeline_*.json`

2. **Valida en ADF**:
   - Revisa transformaciones en el dataflow
   - Configura Linked Services
   - Ejecuta un test run

3. **Feedback**:
   - Reporta bugs en GitHub Issues
   - Sugiere mejoras para v2.1

---

**¡Feliz Migración! 🚀**
