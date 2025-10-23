# Mapeo de Transformaciones PowerCenter a Azure Data Factory

Este documento detalla el mapeo entre componentes de Informatica PowerCenter y sus equivalentes en Azure Data Factory.

---

## Transformaciones

### Transformaciones Básicas

| Componente PowerCenter | Equivalente Azure Data Factory | Estado v1.0 | Notas de Implementación |
|------------------------|--------------------------------|-------------|-------------------------|
| **Source Qualifier** | Source | ✅ Soportado | Mapeo directo. Queries SQL se deben configurar en el dataset de ADF. |
| **Expression** | Derived Column | ✅ Soportado | Las expresiones se traducen automáticamente. Funciones complejas requieren revisión manual. |
| **Filter** | Filter | ✅ Soportado | Condiciones simples se traducen. Expresiones complejas con múltiples funciones anidadas requieren ajuste. |
| **Aggregator** | Aggregate | ✅ Soportado | Group By y funciones de agregación (SUM, AVG, COUNT, MIN, MAX) se mapean correctamente. |
| **Joiner** | Join | ✅ Soportado | Todos los tipos de join se mapean: Normal→Inner, Master Outer→Left, Detail Outer→Right, Full Outer→Outer. |
| **Sorter** | Sort | ✅ Soportado | Orden ascendente y descendente se mantiene. Configuración de distinct rows requiere ajuste. |

### Transformaciones Parcialmente Soportadas

| Componente PowerCenter | Equivalente Azure Data Factory | Estado v1.0 | Notas de Implementación |
|------------------------|--------------------------------|-------------|-------------------------|
| **Router** | Conditional Split | ⚠️ Parcial | Se genera estructura básica. Las condiciones de cada grupo deben configurarse manualmente en ADF. |
| **Lookup** | Lookup | ⚠️ Parcial | Se mapea la estructura. El dataset de lookup y el query deben configurarse manualmente. Cache policy requiere ajuste. |

### Transformaciones NO Soportadas

| Componente PowerCenter | Equivalente Azure Data Factory | Estado v1.0 | Alternativa Manual |
|------------------------|--------------------------------|-------------|--------------------|
| **Update Strategy** | Alter Row | ❌ No soportado | Configurar manualmente Alter Row en ADF con condiciones insert/update/delete/upsert. |
| **Sequence Generator** | - | ❌ No soportado | Usar secuencias de Azure SQL Database o generar IDs en Data Flow con row_number(). |
| **Normalizer** | Flatten / Unpivot | ❌ No soportado | Usar transformación Flatten para arrays o Unpivot para columnas a filas. |
| **Rank** | Window (Rank) | ❌ No soportado | Configurar Window transformation manualmente con funciones de ranking. |
| **Union** | Union | ❌ No soportado | Crear manualmente Union transformation en ADF Data Flow. |
| **XML Parser/Generator** | - | ❌ No soportado | Usar Azure Functions o lógica custom. |
| **Custom Transformation** | - | ❌ No soportado | Reimplementar lógica como Derived Column o Script transformation. |

---

## Funciones de Expresiones

### Funciones de String

| Función PowerCenter | Función Azure Data Factory | Estado | Ejemplo |
|---------------------|----------------------------|--------|---------|
| `SUBSTR(str, start, length)` | `substring(str, start, length)` | ✅ | `SUBSTR(NAME, 1, 10)` → `substring(NAME, 1, 10)` |
| `TRIM(str)` | `trim(str)` | ✅ | `TRIM(NAME)` → `trim(NAME)` |
| `LTRIM(str)` | `ltrim(str)` | ✅ | `LTRIM(NAME)` → `ltrim(NAME)` |
| `RTRIM(str)` | `rtrim(str)` | ✅ | `RTRIM(NAME)` → `rtrim(NAME)` |
| `UPPER(str)` | `upper(str)` | ✅ | `UPPER(NAME)` → `upper(NAME)` |
| `LOWER(str)` | `lower(str)` | ✅ | `LOWER(NAME)` → `lower(NAME)` |
| `LENGTH(str)` | `length(str)` | ✅ | `LENGTH(NAME)` → `length(NAME)` |
| `INSTR(str, search)` | `indexOf(str, search)` | ✅ | `INSTR(NAME, 'ABC')` → `indexOf(NAME, 'ABC')` |
| `CONCAT(str1, str2)` | `concat(str1, str2)` | ✅ | `CONCAT(F_NAME, L_NAME)` → `concat(F_NAME, L_NAME)` |
| `REPLACE(str, old, new)` | `replace(str, old, new)` | ✅ | `REPLACE(NAME, 'A', 'B')` → `replace(NAME, 'A', 'B')` |
| `LPAD(str, len, pad)` | - | ❌ | Usar expresión custom con `concat()` y `repeat()` |
| `RPAD(str, len, pad)` | - | ❌ | Usar expresión custom con `concat()` y `repeat()` |

### Funciones de Fecha

| Función PowerCenter | Función Azure Data Factory | Estado | Ejemplo |
|---------------------|----------------------------|--------|---------|
| `SYSDATE` | `currentTimestamp()` | ✅ | `SYSDATE` → `currentTimestamp()` |
| `TO_DATE(str, format)` | `toDate(str, format)` | ✅ | `TO_DATE('2024-01-01', 'YYYY-MM-DD')` → `toDate('2024-01-01', 'yyyy-MM-dd')` |
| `ADD_TO_DATE(date, unit, num)` | `addDays()` / `addMonths()` etc. | ⚠️ | Usar función específica según la unidad |
| `TRUNC(date, unit)` | `startOfDay()` / `startOfMonth()` | ⚠️ | Depende de la unidad de truncamiento |
| `DATE_DIFF(date1, date2, unit)` | `dateDiff()` | ✅ | Requiere ajuste del orden de parámetros |

### Funciones Numéricas

| Función PowerCenter | Función Azure Data Factory | Estado | Ejemplo |
|---------------------|----------------------------|--------|---------|
| `ROUND(num, decimals)` | `round(num, decimals)` | ✅ | `ROUND(PRICE, 2)` → `round(PRICE, 2)` |
| `CEIL(num)` | `ceil(num)` | ✅ | `CEIL(PRICE)` → `ceil(PRICE)` |
| `FLOOR(num)` | `floor(num)` | ✅ | `FLOOR(PRICE)` → `floor(PRICE)` |
| `ABS(num)` | `abs(num)` | ✅ | `ABS(VALUE)` → `abs(VALUE)` |
| `POWER(base, exp)` | `power(base, exp)` | ✅ | `POWER(2, 3)` → `power(2, 3)` |
| `SQRT(num)` | `sqrt(num)` | ✅ | `SQRT(NUM)` → `sqrt(NUM)` |

### Funciones de Agregación

| Función PowerCenter | Función Azure Data Factory | Estado | Notas |
|---------------------|----------------------------|--------|-------|
| `SUM(column)` | `sum(column)` | ✅ | Usar en Aggregate transformation |
| `AVG(column)` | `avg(column)` | ✅ | Usar en Aggregate transformation |
| `COUNT(*)` | `count()` | ✅ | Usar en Aggregate transformation |
| `MIN(column)` | `min(column)` | ✅ | Usar en Aggregate transformation |
| `MAX(column)` | `max(column)` | ✅ | Usar en Aggregate transformation |
| `STDDEV(column)` | `stddev(column)` | ✅ | Usar en Aggregate transformation |
| `VARIANCE(column)` | `variance(column)` | ✅ | Usar en Aggregate transformation |

### Funciones Condicionales

| Función PowerCenter | Función Azure Data Factory | Estado | Ejemplo |
|---------------------|----------------------------|--------|---------|
| `IIF(condition, true, false)` | `iif(condition, true, false)` | ✅ | `IIF(PRICE > 100, 'HIGH', 'LOW')` → `iif(PRICE > 100, 'HIGH', 'LOW')` |
| `DECODE(expr, val1, res1, val2, res2, default)` | `case when ... then ... else ... end` | ✅ | Se convierte a sintaxis CASE |

**Ejemplo DECODE:**
```sql
-- PowerCenter
DECODE(STATUS,
  'A', 'Active',
  'I', 'Inactive',
  'Unknown')

-- Azure Data Factory
case
  when STATUS = 'A' then 'Active'
  when STATUS = 'I' then 'Inactive'
  else 'Unknown'
end
```

### Operadores

| Operador PowerCenter | Operador Azure Data Factory | Estado | Notas |
|----------------------|----------------------------|--------|-------|
| `||` (concatenación) | `+` | ✅ | Traducción automática |
| `=` | `==` | ✅ | Para comparaciones |
| `!=` | `<>` o `!=` | ✅ | Ambos válidos en ADF |
| `AND`, `OR`, `NOT` | `&&`, `||`, `!` o palabras clave | ✅ | Ambas sintaxis válidas |

---

## Tipos de Datos

| PowerCenter | Azure Data Factory | Precisión | Notas |
|-------------|--------------------|-----------|-------|
| `string` | `String` | - | Mapeo directo |
| `varchar`, `varchar2` | `String` | - | Mapeo directo |
| `char` | `String` | - | Se convierte a String |
| `nstring`, `nvarchar` | `String` | - | Unicode soportado en ADF |
| `decimal`, `number` | `Decimal` o `Int32` | Según precisión | Si precision <= 10 y scale = 0 → Int32, sino → Decimal |
| `integer`, `smallint` | `Int32` | - | Mapeo directo |
| `bigint` | `Int64` | - | Mapeo directo |
| `double` | `Double` | - | Mapeo directo |
| `date` | `DateTime` | - | Mapeo directo |
| `timestamp` | `DateTime` | - | Incluye tiempo |
| `binary` | `Binary` | - | Mapeo directo |
| `boolean` | `Boolean` | - | Mapeo directo |

---

## Conectores / Sources y Targets

| PowerCenter Source/Target | Azure Data Factory Dataset | Estado | Notas |
|---------------------------|----------------------------|--------|-------|
| Oracle | Oracle / Azure Oracle | ✅ | Configurar Linked Service manualmente |
| SQL Server | Azure SQL Database | ✅ | Configurar Linked Service manualmente |
| Teradata | Teradata | ✅ | Requiere Self-Hosted Integration Runtime |
| DB2 | DB2 | ✅ | Requiere Self-Hosted Integration Runtime |
| Flat File (CSV) | Delimited Text | ✅ | Configurar formato y delimitadores |
| Flat File (Fixed Width) | - | ⚠️ | Requiere configuración manual compleja |
| XML | XML | ⚠️ | Parseo complejo requiere Data Flow custom |
| JSON | JSON | ✅ | Mapeo directo |
| SAP | SAP Table / SAP BW | ⚠️ | Requiere configuración específica |

---

## Recomendaciones

1. **Expresiones Complejas**: Revisar todas las expresiones con múltiples funciones anidadas
2. **Funciones Custom**: Reimplementar en ADF usando Derived Column o Script
3. **Variables de Sesión**: Usar Parameters en ADF Pipeline
4. **Reusable Transformations**: Convertir Mapplets a Data Flows separados
5. **Performance**: Configurar partitioning y compute según volumen de datos

---

**Última actualización**: Enero 2025
**Versión del documento**: 1.0
**Autor**: Equipo Entix
