# Manual de Uso - PowerCenter to Azure Data Factory Migrator

Guía completa paso a paso para utilizar la herramienta de migración.

---

## Tabla de Contenidos

1. [Requisitos Previos](#requisitos-previos)
2. [Instalación](#instalación)
3. [Exportar Mapping desde PowerCenter](#paso-1-exportar-mapping-desde-powercenter)
4. [Ejecutar la Herramienta](#paso-2-ejecutar-la-herramienta)
5. [Interpretar el Output](#paso-3-interpretar-el-output)
6. [Importar a Azure Data Factory](#paso-4-importar-a-azure-data-factory)
7. [Validar y Probar](#paso-5-validar-y-probar)
8. [Troubleshooting](#troubleshooting)
9. [Ejemplos Completos](#ejemplos-completos)

---

## Requisitos Previos

### Software Necesario

- **Python 3.8 o superior**
  ```bash
  python --version
  # Debe mostrar: Python 3.8.x o superior
  ```

- **PowerCenter Designer** (para exportar mappings)
  - Versión 10.x recomendada
  - Acceso a repository de PowerCenter

- **Azure Data Factory**
  - Suscripción de Azure activa
  - Data Factory ya creado
  - Permisos de Contributor o superior

### Conocimientos Requeridos

- Básico: Línea de comandos (cmd/PowerShell en Windows, bash en Linux)
- Intermedio: Informatica PowerCenter (conocer mappings)
- Intermedio: Azure Data Factory (crear pipelines y dataflows)

---

## Instalación

### Opción 1: Instalación desde Git

```bash
# 1. Clonar repositorio
git clone https://github.com/entix/powercenter-to-adf.git
cd powercenter-to-adf

# 2. Crear entorno virtual (recomendado)
python -m venv venv

# 3. Activar entorno virtual
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

# 4. Instalar dependencias
pip install -r requirements.txt

# 5. Instalar la herramienta
pip install -e .

# 6. Verificar instalación
pc-to-adf --version
```

### Opción 2: Instalación desde PyPI (cuando esté disponible)

```bash
pip install pc-to-adf
pc-to-adf --version
```

### Verificar Instalación

```bash
pc-to-adf --help
```

Debe mostrar:
```
usage: pc-to-adf [-h] [-o OUTPUT] [-v] [--validate-only] [--log-file LOG_FILE] [--version] input_file

Migra mappings de PowerCenter a Azure Data Factory
...
```

---

## Paso 1: Exportar Mapping desde PowerCenter

### 1.1 Abrir PowerCenter Designer

1. Iniciar **Informatica PowerCenter Designer**
2. Conectar al repositorio que contiene el mapping

### 1.2 Seleccionar Mapping

1. En el Repository Navigator, navegar hasta el mapping deseado
2. Ejemplo: `Mappings/ETL/m_Customer_Daily_Load`

### 1.3 Exportar a XML

1. Clic derecho en el mapping → **Export**
2. En el diálogo de exportación:
   - **Export Type**: Object Definition
   - **File Format**: XML
   - **File Name**: Elegir nombre descriptivo (ej: `m_Customer_Daily_Load.xml`)
   - **Location**: Directorio accesible (ej: `C:\exports\`)
   - ✅ **Include Dependencies**: Marcar si el mapping usa mapplets

3. Clic en **OK**

### 1.4 Verificar Archivo Exportado

```bash
# Verificar que el archivo existe y no está vacío
dir C:\exports\m_Customer_Daily_Load.xml

# En Linux/Mac:
ls -lh /path/to/m_Customer_Daily_Load.xml
```

**Archivo válido debe tener:**
- Tamaño mayor a 1 KB
- Formato XML (abrir con editor de texto para verificar)
- Contener tags como `<POWERMART>`, `<MAPPING>`, `<TRANSFORMATION>`

---

## Paso 2: Ejecutar la Herramienta

### 2.1 Comando Básico

```bash
pc-to-adf ruta/al/archivo.xml
```

**Ejemplo:**
```bash
pc-to-adf C:\exports\m_Customer_Daily_Load.xml
```

### 2.2 Especificar Directorio de Salida

```bash
pc-to-adf C:\exports\m_Customer_Daily_Load.xml --output C:\adf_output\
```

### 2.3 Modo Verbose (para debugging)

```bash
pc-to-adf C:\exports\m_Customer_Daily_Load.xml --verbose
```

**Útil para:**
- Ver detalles de cada transformación procesada
- Debugging de errores
- Entender qué está haciendo la herramienta

### 2.4 Modo Validación (sin generar archivos)

```bash
pc-to-adf C:\exports\m_Customer_Daily_Load.xml --validate-only
```

**Útil para:**
- Verificar que el XML es válido
- Ver si hay errores antes de generar archivos
- Análisis rápido de compatibilidad

### 2.5 Guardar Logs en Archivo

```bash
pc-to-adf C:\exports\m_Customer_Daily_Load.xml --log-file migration.log
```

### 2.6 Ejemplo Completo

```bash
pc-to-adf \
  C:\exports\m_Customer_Daily_Load.xml \
  --output C:\adf_output\ \
  --verbose \
  --log-file C:\logs\migration_customer.log
```

---

## Paso 3: Interpretar el Output

### 3.1 Output en Consola

Durante la ejecución verás:

```
╔═══════════════════════════════════════════════════════════════╗
║  PowerCenter to Azure Data Factory Migrator v1.0              ║
║  Desarrollado por: Entix SpA                                  ║
╚═══════════════════════════════════════════════════════════════╝

✓ Archivo válido: C:\exports\m_Customer_Daily_Load.xml
✓ Mapping parseado: m_Customer_Daily_Load
  • Fuentes: 2
  • Transformaciones: 8
  • Destinos: 1
✓ Traducción completada
  • Transformaciones migradas: 6
  • Warnings: 2
  • Errores: 0
✓ Archivos generados en: C:\adf_output\

┌─────────────────────────────────────────────────┐
│ Migración completada: m_Customer_Daily_Load     │
└─────────────────────────────────────────────────┘

Estadísticas de Migración
┌───────────────────────┬─────────┐
│ Métrica               │   Valor │
├───────────────────────┼─────────┤
│ Total Transformaciones│       8 │
│ Migradas Exitosamente │       6 │
│ Tasa de Éxito         │   75.0% │
│ Warnings              │       2 │
│ Errores               │       0 │
└───────────────────────┴─────────┘

Archivos Generados
┌──────────┬────────────────────────────────────────────┐
│ Tipo     │ Archivo                                    │
├──────────┼────────────────────────────────────────────┤
│ Pipeline │ pipeline_m_Customer_Daily_Load_20250123... │
│ Dataflow │ dataflow_m_Customer_Daily_Load_20250123... │
│ Report   │ migration_report_m_Customer_Daily_Load_... │
└──────────┴────────────────────────────────────────────┘

⚠️  Warnings:
  • Router 'RTR_CustomerType' requiere configuración manual de condiciones
  • Lookup 'LKP_CustomerDimension' requiere configuración manual de query y dataset
```

### 3.2 Archivos Generados

En el directorio de output encontrarás 3 archivos:

#### a) `pipeline_[nombre]_[timestamp].json`

Pipeline de Azure Data Factory que ejecuta el Data Flow.

**Estructura:**
```json
{
  "name": "pipeline_m_Customer_Daily_Load",
  "properties": {
    "activities": [
      {
        "name": "ExecuteDataFlow",
        "type": "ExecuteDataFlow",
        "typeProperties": {
          "dataFlow": {
            "referenceName": "dataflow_m_Customer_Daily_Load"
          }
        }
      }
    ]
  }
}
```

#### b) `dataflow_[nombre]_[timestamp].json`

Data Flow con todas las transformaciones migradas.

**Contiene:**
- Sources (fuentes de datos)
- Transformations (lógica ETL)
- Sinks (destinos)

#### c) `migration_report_[nombre]_[timestamp].json`

Reporte detallado de la migración.

**Contiene:**
```json
{
  "mapping_name": "m_Customer_Daily_Load",
  "migration_date": "2025-01-23T10:30:00",
  "statistics": {
    "total_transformations": 8,
    "migrated_transformations": 6,
    "success_rate": 75.0,
    "warnings": 2,
    "errors": 0
  },
  "warnings": [
    "Router 'RTR_CustomerType' requiere configuración manual...",
    "Lookup 'LKP_CustomerDimension' requiere configuración manual..."
  ],
  "recommendations": [
    "Configurar Linked Services en Azure Data Factory...",
    "Ejecutar pruebas de validación de datos..."
  ]
}
```

### 3.3 Interpretar Warnings y Errors

#### ✅ Success Rate 100%
- Todas las transformaciones se migraron
- Revisar solo configuración de Linked Services
- Proceder con importación a ADF

#### ⚠️ Warnings (Tasa 70-99%)
- Algunas transformaciones requieren ajuste manual
- Revisar cada warning en el reporte
- Completar configuración manualmente en ADF

#### ❌ Errors (Tasa < 70%)
- Problemas críticos de migración
- Revisar XML de PowerCenter
- Consultar [LIMITACIONES.md](LIMITACIONES.md)
- Considerar migración manual para componentes problemáticos

---

## Paso 4: Importar a Azure Data Factory

### 4.1 Acceder a Azure Portal

1. Ir a https://portal.azure.com
2. Navegar a tu Azure Data Factory
3. Clic en **Author & Monitor** (o **Crear y supervisar**)

### 4.2 Importar Data Flow

1. En ADF Studio, ir a **Author** (ícono de lápiz)
2. En el panel izquierdo, expandir **Data flows**
3. Clic en los tres puntos (...) → **Import from template**
4. Seleccionar `dataflow_[nombre].json`
5. Clic en **Import**

**Configurar Datasets:**
- ADF solicitará configurar datasets para sources y sinks
- Crear Linked Services si no existen
- Mapear cada source/sink al dataset correspondiente

### 4.3 Crear Linked Services

Para cada fuente de datos:

**Ejemplo: Oracle Source**
1. Ir a **Manage** (ícono de toolbox)
2. **Linked services** → **New**
3. Seleccionar **Oracle**
4. Configurar:
   - Name: `LS_Oracle_Production`
   - Connection string: `[servidor]:[puerto]/[servicio]`
   - Authentication: Usuario/contraseña o Azure Key Vault
5. **Test connection** → **Create**

**Repetir para:**
- Cada base de datos fuente
- Cada base de datos destino
- Archivos planos (Blob Storage, Data Lake)

### 4.4 Crear Datasets

**Ejemplo: Dataset de Tabla Oracle**
1. **Author** → **Datasets** → **New dataset**
2. Seleccionar **Oracle**
3. Linked Service: `LS_Oracle_Production`
4. Table: Seleccionar o especificar `SCHEMA.TABLE_NAME`
5. Schema: Importar o definir manualmente
6. Name: `ds_SRC_Customer`
7. **Publish**

### 4.5 Importar Pipeline

1. **Author** → **Pipelines** → Tres puntos (...) → **Import from template**
2. Seleccionar `pipeline_[nombre].json`
3. Mapear Data Flow referenciado
4. **Publish All**

---

## Paso 5: Validar y Probar

### 5.1 Validación Visual en ADF

1. Abrir el Data Flow importado
2. **Revisar cada transformación:**
   - ✅ Configuración completa
   - ✅ Expresiones correctas
   - ⚠️ Warnings del reporte resueltos

3. **Usar Data Preview:**
   - Activar **Debug mode** en el Data Flow
   - Seleccionar cada transformación
   - Clic en **Data preview** → **Refresh**
   - Verificar que los datos se transforman correctamente

### 5.2 Configurar Parámetros (si aplica)

Si el mapping original tenía parámetros:

1. En el Pipeline, agregar **Parameters**
2. Pasar parámetros al Data Flow
3. Usar en expresiones: `$parameter_name`

### 5.3 Ejecutar Test con Data Sampling

```bash
# Opción 1: Desde ADF UI
# - Clic en "Debug" en el pipeline
# - Configurar sampling (ej: 1000 rows)
# - Verificar output en destino

# Opción 2: Configurar sampling en Source
{
  "source": {
    "samplingType": "TopN",
    "samplingValue": 1000
  }
}
```

### 5.4 Comparar Resultados

**Método recomendado:**

1. Ejecutar mapping original en PowerCenter con dataset de prueba
2. Exportar resultados a CSV
3. Ejecutar Data Flow en ADF con mismo dataset
4. Exportar resultados de ADF
5. Comparar con herramienta de diff (Excel, Python pandas, SQL)

**Script Python de ejemplo:**
```python
import pandas as pd

# Cargar resultados
pc_results = pd.read_csv('powercenter_output.csv')
adf_results = pd.read_csv('adf_output.csv')

# Ordenar para comparar
pc_sorted = pc_results.sort_values(by='ID').reset_index(drop=True)
adf_sorted = adf_results.sort_values(by='ID').reset_index(drop=True)

# Comparar
differences = pd.concat([pc_sorted, adf_sorted]).drop_duplicates(keep=False)

if differences.empty:
    print("✅ Resultados idénticos")
else:
    print(f"⚠️ {len(differences)} diferencias encontradas")
    print(differences)
```

### 5.5 Performance Testing

1. Ejecutar con volumen real de datos (o subset representativo)
2. Monitorear tiempo de ejecución
3. Revisar **Monitoring** en ADF para ver:
   - Data read
   - Data written
   - Duration
   - Resource utilization

4. **Optimizar si es necesario:**
   - Ajustar partitioning en Data Flow
   - Incrementar compute (cores)
   - Habilitar staging para writes grandes

---

## Troubleshooting

### Problema 1: "Error de sintaxis XML"

**Síntoma:**
```
❌ Error de validación: Error de sintaxis XML: line 45: expected '>'
```

**Solución:**
1. Abrir XML con editor de texto
2. Verificar que esté bien formado
3. Re-exportar desde PowerCenter Designer
4. Asegurar encoding UTF-8

---

### Problema 2: "Transformación no soportada"

**Síntoma:**
```
⚠️ Warnings:
  • Transformación 'Update Strategy' no está soportada
```

**Solución:**
1. Consultar [LIMITACIONES.md](LIMITACIONES.md)
2. Implementar manualmente en ADF
3. Para Update Strategy: usar Alter Row transformation

---

### Problema 3: "Expresiones no se traducen correctamente"

**Síntoma:**
Expresión en ADF no funciona o da error

**Solución:**
1. Revisar sintaxis de expresión original
2. Consultar [MAPEO_TRANSFORMACIONES.md](MAPEO_TRANSFORMACIONES.md)
3. Ajustar manualmente en Data Flow
4. Usar **Expression Builder** en ADF para validar

---

### Problema 4: "No se pueden crear Linked Services"

**Síntoma:**
Test connection falla al crear Linked Service

**Solución:**
1. Verificar configuración de red (firewall, NSG)
2. Para bases de datos on-premise: instalar **Self-Hosted Integration Runtime**
3. Verificar credenciales
4. Revisar connection string

**Instalar Self-Hosted IR:**
```bash
# Descargar desde Azure Portal
# Manage → Integration Runtimes → New → Self-Hosted
# Seguir wizard de instalación en servidor on-premise
```

---

### Problema 5: "Data Flow falla en ejecución"

**Síntoma:**
Pipeline ejecuta pero Data Flow falla

**Pasos de debugging:**
1. Activar **Debug mode** en Data Flow
2. Ver error específico en **Output**
3. Revisar **Data preview** transformación por transformación
4. Verificar schemas de datasets

**Errores comunes:**
- Schema mismatch: Actualizar schema en dataset
- Null values: Agregar manejo de nulls en expresiones
- Type conversion: Usar funciones de cast explícitas

---

## Ejemplos Completos

### Ejemplo 1: Mapping Simple (Expression + Filter)

**Mapping PowerCenter:**
- Source: `CUSTOMERS` (Oracle)
- Expression: `EXP_FormatData`
  - `FULL_NAME = FIRST_NAME || ' ' || LAST_NAME`
  - `UPPER_EMAIL = UPPER(EMAIL)`
- Filter: `FLT_ActiveOnly`
  - Condition: `STATUS = 'A'`
- Target: `DW_CUSTOMERS` (SQL Server)

**Ejecutar migración:**
```bash
pc-to-adf mappings/m_Customer_Simple.xml --output adf_output/
```

**Resultado esperado:**
- ✅ 100% migrado
- 0 warnings
- Ajuste manual: Solo Linked Services

**Tiempo estimado:** 15-30 minutos (incluye importación a ADF)

---

### Ejemplo 2: Mapping con Aggregator y Joiner

**Mapping PowerCenter:**
- Source 1: `ORDERS`
- Source 2: `CUSTOMERS`
- Aggregator: `AGG_TotalByCustomer`
  - Group By: `CUSTOMER_ID`
  - Sum: `TOTAL_AMOUNT`
- Joiner: `JNR_CustomerOrders`
  - Type: Normal (Inner Join)
- Target: `CUSTOMER_SUMMARY`

**Ejecutar:**
```bash
pc-to-adf mappings/m_Customer_Orders_Summary.xml --verbose
```

**Resultado esperado:**
- ✅ 85-90% migrado
- ⚠️ 1-2 warnings (revisar condición de join)

**Tiempo estimado:** 45-60 minutos

---

### Ejemplo 3: Mapping Complejo con Lookup

**Mapping PowerCenter:**
- Source: `TRANSACTIONS`
- Lookup: `LKP_CustomerType`
- Expression: `EXP_Categorize`
- Router: `RTR_ByCategory`
- Targets múltiples

**Ejecutar:**
```bash
pc-to-adf mappings/m_Transaction_Categorization.xml --output adf_output/ --log-file transaction_migration.log
```

**Resultado esperado:**
- ⚠️ 60-70% migrado automáticamente
- Warnings: Lookup y Router requieren configuración
- Ajuste manual: 2-3 horas

**Tiempo total estimado:** 3-4 horas

---

## Mejores Prácticas

1. **Empezar con mappings simples** para familiarizarse con la herramienta
2. **Validar siempre en ambiente de desarrollo** antes de producción
3. **Documentar cambios manuales** para auditoría
4. **Usar control de versiones** para archivos JSON generados
5. **Ejecutar tests de regresión** comparando resultados PC vs ADF
6. **Monitorear performance** en primeras ejecuciones en producción

---

## Siguientes Pasos

Después de migrar tus mappings:

1. Configurar **Triggers** en pipelines de ADF
2. Implementar **Alertas y Monitoreo**
3. Configurar **CI/CD** para deployments automáticos
4. Documentar pipelines para el equipo
5. Capacitar usuarios finales

---

## Recursos Adicionales

- [Documentación oficial de Azure Data Factory](https://docs.microsoft.com/azure/data-factory/)
- [Expresiones en Data Flow](https://docs.microsoft.com/azure/data-factory/data-flow-expressions)
- [Mejores prácticas de ADF](https://docs.microsoft.com/azure/data-factory/concepts-data-flow-performance)

---

## Soporte

Si encuentras problemas o necesitas ayuda:

- **GitHub Issues**: https://github.com/entix/powercenter-to-adf/issues
- **Email**: contacto@entix.cl
- **Documentación**: Ver [README.md](../README.md) y otros docs en `/docs`

---

**Última actualización**: Enero 2025
**Versión del documento**: 1.0
**Autor**: Equipo Entix
