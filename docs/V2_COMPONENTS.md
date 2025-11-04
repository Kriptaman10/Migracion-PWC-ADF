# Componentes v2.0 - PowerCenter to ADF Migrator

Documentación técnica de las transformaciones avanzadas soportadas en la versión 2.0.

---

## Índice

1. [Sorter → Sort](#sorter--sort)
2. [Update Strategy → Alter Row](#update-strategy--alter-row)
3. [Aggregator → Aggregate (Mejorado)](#aggregator--aggregate-mejorado)
4. [Joiner → Join (Mejorado)](#joiner--join-mejorado)
5. [Lookup → Lookup (Completo)](#lookup--lookup-completo)
6. [Router → Conditional Split (Completo)](#router--conditional-split-completo)
7. [Casos Especiales](#casos-especiales)

---

## Sorter → Sort

### PowerCenter: Sorter Transformation

**Propiedades Soportadas:**
- Sort keys con dirección (ASC/DESC)
- Distinct flag
- Case sensitive
- Null treated low

**Ejemplo XML:**
```xml
<TRANSFORMATION NAME="srt_EMPLOYEES" TYPE="Sorter">
    <TRANSFORMFIELD NAME="DEALERSHIP_ID" ISSORTKEY="YES" SORTDIRECTION="ASCENDING" SORTORDER="0"/>
    <TRANSFORMFIELD NAME="EMPLOYEE_ID" ISSORTKEY="YES" SORTDIRECTION="DESCENDING" SORTORDER="1"/>
    <TABLEATTRIBUTE NAME="Distinct" VALUE="NO"/>
    <TABLEATTRIBUTE NAME="Case Sensitive" VALUE="YES"/>
</TRANSFORMATION>
```

### Azure Data Factory: Sort Transformation

**JSON Generado:**
```json
{
  "name": "srt_EMPLOYEES",
  "type": "sort",
  "orderBy": [
    {"name": "DEALERSHIP_ID", "order": "asc"},
    {"name": "EMPLOYEE_ID", "order": "desc"}
  ],
  "distinct": false
}
```

**Consideraciones:**
- ADF Sort es case-sensitive por defecto
- Si PowerCenter tiene case_sensitive=False, se genera un warning
- Distinct se traduce directamente

---

## Update Strategy → Alter Row

### PowerCenter: Update Strategy Transformation

**Estrategias Soportadas:**
- `DD_INSERT` → `insert`
- `DD_UPDATE` → `update`
- `DD_DELETE` → `delete`
- `DD_REJECT` → `reject` (con warning)

**Ejemplo XML:**
```xml
<TRANSFORMATION NAME="UPD_Strategy" TYPE="Update Strategy">
    <TABLEATTRIBUTE NAME="Update Strategy Expression" VALUE="DD_INSERT"/>
</TRANSFORMATION>
```

### Azure Data Factory: Alter Row Transformation

**JSON Generado:**
```json
{
  "name": "UPD_Strategy",
  "type": "alterrow",
  "action": "insert"
}
```

**Consideraciones:**
- DD_REJECT no es directamente soportado en ADF
- Se recomienda usar Router + Error handling en su lugar
- Expresiones condicionales se traducen a conditions

---

## Aggregator → Aggregate (Mejorado)

### PowerCenter: Aggregator Transformation

**Propiedades Mejoradas v2.0:**
- Group by fields (`EXPRESSIONTYPE="GROUPBY"`)
- Funciones de agregación: SUM, AVG, COUNT, MIN, MAX, FIRST, LAST
- Sorted Input optimization
- Expresiones calculadas complejas

**Ejemplo XML:**
```xml
<TRANSFORMATION NAME="AGG_Total_Poliza" TYPE="Aggregator">
    <TRANSFORMFIELD NAME="POLIZA1" EXPRESSIONTYPE="GROUPBY" PORTTYPE="INPUT/OUTPUT"/>
    <TRANSFORMFIELD NAME="TOTAL_VENTA" EXPRESSION="SUM(CANTIDAD_VENTA)"
                    EXPRESSIONTYPE="GENERAL" PORTTYPE="OUTPUT"/>
    <TRANSFORMFIELD NAME="PROMEDIO" EXPRESSION="AVG(MONTO_VENTA)"
                    EXPRESSIONTYPE="GENERAL" PORTTYPE="OUTPUT"/>
    <TABLEATTRIBUTE NAME="Sorted Input" VALUE="YES"/>
</TRANSFORMATION>
```

### Azure Data Factory: Aggregate Transformation

**JSON Generado:**
```json
{
  "name": "AGG_Total_Poliza",
  "type": "aggregate",
  "groupBy": ["POLIZA1"],
  "aggregates": [
    {"name": "TOTAL_VENTA", "expression": "sum(CANTIDAD_VENTA)"},
    {"name": "PROMEDIO", "expression": "avg(MONTO_VENTA)"}
  ],
  "sortedInput": true
}
```

**Traducción de Funciones:**
| PowerCenter | Azure Data Factory |
|-------------|-------------------|
| `SUM(campo)` | `sum(campo)` |
| `AVG(campo)` | `avg(campo)` |
| `COUNT(*)` | `count()` |
| `MIN(campo)` | `min(campo)` |
| `MAX(campo)` | `max(campo)` |
| `FIRST(campo)` | `first(campo)` |
| `LAST(campo)` | `last(campo)` |

**Consideraciones:**
- Sorted Input requiere un Sorter upstream
- Expresiones complejas como `SUM(A) / SUM(B)` se traducen correctamente
- Aggregator sin funciones de agregación usa `first()` en ADF

---

## Joiner → Join (Mejorado)

### PowerCenter: Joiner Transformation

**Propiedades Mejoradas v2.0:**
- Master/Detail fields identificación automática
- Join conditions múltiples (separadas por AND)
- Sorted Input optimization
- Broadcast join detection

**Tipos de Join:**
| PowerCenter | Azure Data Factory |
|-------------|-------------------|
| Normal Join | inner |
| Master Outer | left |
| Detail Outer | right |
| Full Outer | outer |

**Ejemplo XML:**
```xml
<TRANSFORMATION NAME="JNR_Polizas_Ventas" TYPE="Joiner">
    <TRANSFORMFIELD NAME="POLIZA" PORTTYPE="INPUT/OUTPUT/MASTER"/>
    <TRANSFORMFIELD NAME="POLIZA1" PORTTYPE="INPUT/OUTPUT"/>

    <TABLEATTRIBUTE NAME="Join Condition" VALUE="POLIZA = POLIZA1"/>
    <TABLEATTRIBUTE NAME="Join Type" VALUE="Normal Join"/>
    <TABLEATTRIBUTE NAME="Sorted Input" VALUE="NO"/>
</TRANSFORMATION>
```

### Azure Data Factory: Join Transformation

**JSON Generado:**
```json
{
  "name": "JNR_Polizas_Ventas",
  "type": "join",
  "joinType": "inner",
  "joinConditions": [
    {
      "leftColumn": "POLIZA",
      "rightColumn": "POLIZA1",
      "operator": "=="
    }
  ],
  "masterFields": ["POLIZA"],
  "detailFields": ["POLIZA1"]
}
```

**Join Conditions Múltiples:**
```xml
<!-- PowerCenter -->
<TABLEATTRIBUTE NAME="Join Condition" VALUE="campo1 = campo1 AND campo2 = campo2"/>
```

```json
// Azure Data Factory
"joinConditions": [
  {"leftColumn": "campo1", "rightColumn": "campo1", "operator": "=="},
  {"leftColumn": "campo2", "rightColumn": "campo2", "operator": "=="}
]
```

**Optimizaciones:**
- Broadcast join si master < 10 campos y detail > 20 campos
- Sorted Input genera warning para verificar Sorter upstream

---

## Lookup → Lookup (Completo)

### PowerCenter: Lookup Transformation

**Tipos de Lookup Soportados:**
1. **Database Lookup**: Lookup contra tabla de base de datos
2. **Flat File Lookup**: Lookup contra archivo CSV/delimitado
3. **SQL Override**: Lookup con SQL custom

**Propiedades Soportadas:**
- Lookup table name
- Lookup condition (múltiples condiciones)
- Source type (Database, Flat File)
- SQL Override
- Cache configuration
- Multiple match policy
- Return fields

**Ejemplo 1: Database Lookup**
```xml
<TRANSFORMATION NAME="lkp_DIM_DATES" TYPE="Lookup Procedure">
    <TRANSFORMFIELD NAME="TRANSACTION_DATE" PORTTYPE="INPUT/OUTPUT"/>
    <TRANSFORMFIELD NAME="DATE_KEY" PORTTYPE="LOOKUP/OUTPUT"/>

    <TABLEATTRIBUTE NAME="Lookup table name" VALUE="DIM_DATES"/>
    <TABLEATTRIBUTE NAME="Source Type" VALUE="Database"/>
    <TABLEATTRIBUTE NAME="Lookup condition" VALUE="DATE_VALUE = TRANSACTION_DATE"/>
    <TABLEATTRIBUTE NAME="Lookup caching enabled" VALUE="YES"/>
</TRANSFORMATION>
```

**JSON Generado:**
```json
{
  "name": "lkp_DIM_DATES",
  "type": "lookup",
  "lookupDataset": "DIM_DATES",
  "lookupConditions": [
    {
      "leftColumn": "TRANSACTION_DATE",
      "rightColumn": "DATE_VALUE",
      "operator": "=="
    }
  ],
  "cacheMode": "static",
  "returnFields": [
    {"name": "DATE_KEY", "datatype": "string"}
  ]
}
```

**Ejemplo 2: Flat File Lookup**
```xml
<TRANSFORMATION NAME="lkp_salaries" TYPE="Lookup Procedure">
    <TABLEATTRIBUTE NAME="Source Type" VALUE="Flat File"/>
    <TABLEATTRIBUTE NAME="Lookup source is static" VALUE="YES"/>
    <FLATFILE DELIMITED="YES" DELIMITERS="," SKIPROWS="1"/>
</TRANSFORMATION>
```

**JSON Generado:**
```json
{
  "name": "lkp_salaries",
  "type": "lookup",
  "sourceType": "DelimitedText",
  "flatFileConfig": {
    "delimited": true,
    "delimiters": ",",
    "skip_rows": 1
  },
  "cacheMode": "static"
}
```

**Ejemplo 3: SQL Override**
```xml
<TABLEATTRIBUTE NAME="Lookup Sql Override" VALUE="SELECT KEY, VALUE FROM TABLE WHERE ACTIVE = 1"/>
```

**JSON Generado:**
```json
{
  "name": "lkp_custom",
  "type": "lookup",
  "sqlOverride": "SELECT KEY, VALUE FROM TABLE WHERE ACTIVE = 1",
  "cacheMode": "static"
}
```

**Consideraciones:**
- SQL Override genera warning para revisar compatibilidad
- Flat File requiere configurar DelimitedText dataset en ADF
- Multiple match policy puede comportarse diferente en ADF

---

## Router → Conditional Split (Completo)

### PowerCenter: Router Transformation

**Propiedades Soportadas:**
- Múltiples output groups con expresiones
- Default group
- Campos por grupo con REF_FIELD
- Expresiones condicionales complejas

**Ejemplo XML:**
```xml
<TRANSFORMATION NAME="RTR_SEPARAR_FLUJOS" TYPE="Router">
    <GROUP NAME="INPUT" TYPE="INPUT"/>
    <GROUP NAME="REGISTROS_VALIDOS" TYPE="OUTPUT"
           EXPRESSION="PRECIO_VENTA > 0 AND RUT_NUM IS NOT NULL"/>
    <GROUP NAME="REGISTROS_DUPLICADOS" TYPE="OUTPUT"
           EXPRESSION="V_ES_DUPLICADO = 1"/>
    <GROUP NAME="DEFAULT1" TYPE="OUTPUT/DEFAULT"/>

    <TRANSFORMFIELD NAME="PRECIO_VENTA" GROUP="INPUT" PORTTYPE="INPUT"/>
    <TRANSFORMFIELD NAME="PRECIO_VENTA1" GROUP="REGISTROS_VALIDOS"
                    PORTTYPE="OUTPUT" REF_FIELD="PRECIO_VENTA"/>
    <TRANSFORMFIELD NAME="PRECIO_VENTA2" GROUP="REGISTROS_DUPLICADOS"
                    PORTTYPE="OUTPUT" REF_FIELD="PRECIO_VENTA"/>
</TRANSFORMATION>
```

### Azure Data Factory: Conditional Split Transformation

**JSON Generado:**
```json
{
  "name": "RTR_SEPARAR_FLUJOS",
  "type": "conditionalsplit",
  "conditions": [
    {
      "name": "REGISTROS_VALIDOS",
      "expression": "PRECIO_VENTA > 0 and RUT_NUM is not null()",
      "fields": [
        {"name": "PRECIO_VENTA1", "ref_field": "PRECIO_VENTA"}
      ]
    },
    {
      "name": "REGISTROS_DUPLICADOS",
      "expression": "V_ES_DUPLICADO == 1",
      "fields": [
        {"name": "PRECIO_VENTA2", "ref_field": "PRECIO_VENTA"}
      ]
    }
  ],
  "defaultStream": "DEFAULT1"
}
```

**Consideraciones:**
- REF_FIELD mapea campos de entrada a salida por grupo
- Expresiones se traducen a sintaxis ADF
- Si > 10 grupos, se recomienda simplificar

---

## Casos Especiales

### 1. Sorted Input Optimization

**Patrón:**
```
Sorter → Aggregator (Sorted Input=YES)
Sorter → Joiner (Sorted Input=YES)
```

**Validación:**
- El validador verifica que exista Sorter upstream
- Genera warning si no se detecta
- ADF no requiere flag explícito, es implícito por orden

**Ejemplo:**
```xml
<!-- Sorter -->
<TRANSFORMATION NAME="SRT_DATA" TYPE="Sorter">
    <TRANSFORMFIELD NAME="GROUP_KEY" ISSORTKEY="YES"/>
</TRANSFORMATION>

<!-- Aggregator con Sorted Input -->
<TRANSFORMATION NAME="AGG_DATA" TYPE="Aggregator">
    <TABLEATTRIBUTE NAME="Sorted Input" VALUE="YES"/>
</TRANSFORMATION>
```

### 2. Lookup Encadenados

**Patrón:**
```
Source → Lookup1 → Lookup2 → Lookup3 → Target
```

**Consideración:**
- Puede afectar performance
- Validador recomienda combinar queries
- Considerar usar Join si es posible

### 3. Aggregator sin Funciones de Agregación

**Patrón:**
```xml
<TRANSFORMATION NAME="AGG_GET_FIRST" TYPE="Aggregator">
    <TRANSFORMFIELD NAME="KEY" EXPRESSIONTYPE="GROUPBY"/>
    <TRANSFORMFIELD NAME="VALUE" EXPRESSIONTYPE="GENERAL"/>
</TRANSFORMATION>
```

**Propósito:** Obtener el primer registro por grupo

**Traducción:**
```json
{
  "groupBy": ["KEY"],
  "aggregates": [
    {"name": "VALUE", "expression": "first(VALUE)"}
  ]
}
```

### 4. Join con Broadcast

**Heurística:**
- Master table: < 10 campos
- Detail table: > 20 campos

**Resultado:**
```json
{
  "name": "JNR_SMALL_LARGE",
  "type": "join",
  "joinType": "inner",
  "broadcast": "left"
}
```

### 5. Router sin Default Group

**Problema:** Registros no coincidentes se pierden

**Validación:** Genera warning

**Recomendación:** Siempre incluir default group para error handling

---

## Resumen de Warnings y Validaciones

### Warnings Generados

| Situación | Warning |
|-----------|---------|
| Sorter case_sensitive=False | ADF Sort es case-sensitive por defecto |
| Aggregator con Sorted Input | Verificar que exista Sorter upstream |
| Joiner con Sorted Input | Verificar que ambas entradas estén ordenadas |
| Lookup con SQL Override | Revisar compatibilidad con ADF |
| Lookup Flat File | Configurar DelimitedText dataset |
| Lookup multiple match policy != "Use Any Value" | ADF puede comportarse diferente |
| Router > 10 grupos | Considerar simplificar |
| Update Strategy DD_REJECT | Usar Router para manejo de errores |

### Errores Detectados

| Validación | Error |
|------------|-------|
| Joiner sin join condition | Join condition es obligatoria |
| Aggregator sin GROUP BY ni agregaciones | Al menos uno es requerido |
| Lookup sin tabla ni SQL Override | Configuración incompleta |
| Router sin output groups | Al menos un grupo es requerido |
| Transformación no soportada | Tipo no implementado en v2.0 |
| Dependencias circulares | Flujo inválido |

---

## Ejemplo Completo: Mapping Complejo

**PowerCenter Mapping:**
```
SQ_VENTAS ──┐
            ├──→ JNR_Join (Inner) → EXP_Calc → AGG_Group → UPD_Strategy → TARGET
SQ_POLIZAS ─┘
```

**Flujo de Migración:**

1. **Parser v2.0** extrae:
   - 2 Source Qualifiers
   - 1 Joiner con Master/Detail
   - 1 Expression
   - 1 Aggregator con GROUP BY y SUM
   - 1 Update Strategy

2. **Validator** verifica:
   - Join condition está definida
   - Aggregator tiene GROUP BY
   - No hay ciclos

3. **Translator** genera:
   - 2 Sources
   - 1 Join (inner) con joinConditions
   - 1 DerivedColumn
   - 1 Aggregate con groupBy y aggregates
   - 1 AlterRow (insert)

4. **Generator** crea:
   - pipeline.json
   - dataflow.json con todas las transformaciones
   - report.json con warnings y estadísticas

---

## Próximos Pasos (v2.1)

- **Normalizer** → Flatten/Unpivot
- **Rank** → Window (con row_number)
- **Union** → Union
- **Sequence Generator** → Surrogatekey
- **Expresiones complejas** con regex avanzado
- **Mapplets** como Data Flows reutilizables

---

**Versión del Documento:** 2.0
**Última actualización:** Enero 2025
**Autor:** Equipo Técnico Entix SpA
