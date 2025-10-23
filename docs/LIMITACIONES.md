# Limitaciones Conocidas - PowerCenter to ADF Migrator v1.0

Este documento detalla las limitaciones de la herramienta de migración en su versión 1.0 (MVP).

---

## Componentes NO Soportados

### 1. Update Strategy Transformation

**Razón Técnica:**
- PowerCenter usa flags (DD_INSERT, DD_UPDATE, DD_DELETE, DD_REJECT) que requieren lógica compleja de mapeo
- Azure Data Factory usa Alter Row con condiciones diferentes
- La lógica de negocio detrás de cada estrategia varía por proyecto

**Impacto:**
- Alto - Componente crítico para operaciones de data warehousing

**Alternativa Manual:**
1. Identificar la lógica de Update Strategy en PowerCenter
2. Crear Alter Row transformation en ADF Data Flow
3. Configurar condiciones:
   - Insert if: `isNull(existingKey)`
   - Update if: `not(isNull(existingKey)) && hash(newRow) != hash(oldRow)`
   - Delete if: `flag == 'D'`
   - Upsert if: combinación de insert/update

**Esfuerzo Estimado:** 2-4 horas por mapping con Update Strategy

**Roadmap:** Planificado para v2.0 con detección automática de patrones comunes

---

### 2. Sequence Generator Transformation

**Razón Técnica:**
- PowerCenter genera secuencias de manera nativa
- Azure Data Factory no tiene equivalente directo
- Soluciones varían según arquitectura (SQL sequences, UUID, row_number)

**Impacto:**
- Medio - Afecta generación de claves primarias

**Alternativa Manual:**

**Opción 1: Azure SQL Sequences**
```sql
-- Crear secuencia en Azure SQL
CREATE SEQUENCE dbo.CustomerID_Seq
START WITH 1000
INCREMENT BY 1;

-- Usar en Stored Procedure o Sink configuration
```

**Opción 2: Data Flow con Row Number**
```
// En ADF Data Flow - Derived Column
row_number_column = rowNumber()
```

**Opción 3: UUID/GUID**
```
// Para claves no secuenciales
uuid_column = uuid()
```

**Esfuerzo Estimado:** 1-2 horas por sequence

**Roadmap:** v2.0 - Generación automática de configuración según tipo de clave

---

### 3. Normalizer Transformation

**Razón Técnica:**
- Transformación compleja que convierte columnas en filas
- Azure Data Factory tiene Flatten (para arrays) y Unpivot (para columnas)
- Requiere análisis de estructura específica de cada caso

**Impacto:**
- Medio - Usado principalmente en procesos de normalización de datos

**Alternativa Manual:**

**Para Arrays:**
```json
// Usar Flatten transformation en ADF
{
  "type": "Flatten",
  "unroll": "array_column"
}
```

**Para Columnas a Filas:**
```json
// Usar Unpivot transformation
{
  "type": "Unpivot",
  "unpivotColumns": ["col1", "col2", "col3"]
}
```

**Esfuerzo Estimado:** 3-5 horas dependiendo de la complejidad

**Roadmap:** v2.0 - Soporte para patrones comunes de normalización

---

### 4. Rank Transformation

**Razón Técnica:**
- Requiere Window functions específicas
- Configuración de partitioning y ordering varía por caso

**Impacto:**
- Bajo - Usado en casos específicos

**Alternativa Manual:**
```
// ADF Data Flow - Window transformation
rank_column = rank(
  asc(AMOUNT, true),
  partitionBy(CUSTOMER_ID)
)
```

**Esfuerzo Estimado:** 1-2 horas

**Roadmap:** v2.1 - Soporte básico

---

### 5. Custom Transformations

**Razón Técnica:**
- Lógica específica implementada en código C/C++ o Java
- No hay equivalente directo en ADF

**Impacto:**
- Variable - Depende del uso en el proyecto

**Alternativa Manual:**
1. Reimplementar lógica como Derived Column expressions
2. Usar Azure Functions para lógica compleja
3. Implementar en Script transformation (Python/R)

**Esfuerzo Estimado:** 4-20 horas dependiendo de la complejidad

**Roadmap:** No planificado - Requiere análisis caso por caso

---

## Componentes Parcialmente Soportados

### 1. Router Transformation → Conditional Split

**Limitación:**
- Se genera estructura básica
- Condiciones de cada grupo deben configurarse manualmente

**Qué se migra:**
- ✅ Número de grupos
- ✅ Nombres de outputs
- ❌ Condiciones específicas de cada grupo

**Ajuste Manual Requerido:**
```json
// Configurar en ADF manualmente
{
  "conditions": [
    {
      "name": "HighValue",
      "condition": "AMOUNT > 1000"
    },
    {
      "name": "LowValue",
      "condition": "AMOUNT <= 1000"
    }
  ]
}
```

**Esfuerzo Estimado:** 30 minutos por Router

---

### 2. Lookup Transformation

**Limitación:**
- Se mapea estructura básica
- Dataset y query de lookup deben configurarse manualmente
- Cache policy requiere ajuste

**Qué se migra:**
- ✅ Nombre de la transformación
- ✅ Tipo (Lookup)
- ❌ Dataset de lookup
- ❌ Condiciones de join
- ❌ Configuración de cache

**Ajuste Manual Requerido:**
```json
// En ADF Data Flow
{
  "name": "LookupCustomer",
  "type": "Lookup",
  "source": "CustomerDimension",
  "conditions": {
    "left": "CustomerID",
    "right": "ID"
  },
  "broadcast": "auto"
}
```

**Esfuerzo Estimado:** 1-2 horas por Lookup complejo

---

## Limitaciones de Mappings Complejos

### 1. Mapplets

**Limitación:**
- No se procesan mapplets como componentes reutilizables
- Cada uso de mapplet debe migrarse individualmente

**Impacto:**
- Alto si hay muchos mapplets reutilizables

**Alternativa:**
1. Identificar mapplets únicos
2. Migrar cada mapplet como Data Flow separado
3. Referenciar desde pipelines

**Esfuerzo Estimado:** 2-4 horas por mapplet único

**Roadmap:** v2.0 - Detección y migración de mapplets como Data Flows reutilizables

---

### 2. Sesiones y Workflows

**Limitación:**
- Solo se migran mappings individuales
- Configuración de sesiones (commit interval, recovery, etc.) no se migra
- Workflows con múltiples mappings requieren orquestación manual

**Impacto:**
- Alto - Requiere reconstrucción de flujos de trabajo

**Alternativa:**
1. Migrar cada mapping individualmente
2. Crear pipelines de ADF manualmente
3. Configurar dependencias y triggers

**Esfuerzo Estimado:** 4-8 horas por workflow complejo

**Roadmap:** v2.0 - Exportar workflows como pipelines de ADF

---

### 3. Parámetros de Sesión

**Limitación:**
- Parámetros se generan como valores hardcodeados
- No se crean parámetros de pipeline automáticamente

**Impacto:**
- Medio - Afecta flexibilidad

**Alternativa:**
1. Identificar parámetros en PowerCenter
2. Crear Parameters en ADF Pipeline
3. Pasar a Data Flow como parameters

**Esfuerzo Estimado:** 1-2 horas

**Roadmap:** v2.0 - Mapeo automático de parámetros

---

## Limitaciones de Expresiones

### 1. Funciones No Estándar

**Limitación:**
- Funciones específicas de PowerCenter sin equivalente directo en ADF

**Ejemplos:**
- `REG_EXTRACT()` - Expresiones regulares complejas
- `METAPHONE()` - Algoritmos fonéticos
- `SOUNDEX()` - Búsqueda fonética
- `LPAD()`, `RPAD()` - Padding con caracteres

**Alternativa:**
- Reimplementar con funciones básicas combinadas
- Usar Script transformation con Python

**Esfuerzo:** Variable (1-4 horas por función)

---

### 2. Expresiones con Variables Internas

**Limitación:**
- Variables de transformación con scope interno no se mapean

**Ejemplo PowerCenter:**
```
// Variable con acumulación
v_RunningTotal = v_RunningTotal + AMOUNT
o_Total = v_RunningTotal
```

**Alternativa ADF:**
Usar Window transformation con running aggregates

**Esfuerzo:** 2-3 horas por expresión con variables

---

## Limitaciones de Conectores

### 1. Conectores Legacy

**Limitación:**
- Conectores antiguos o específicos pueden no tener equivalente en ADF
- Configuraciones propietarias no se migran

**Ejemplos Problemáticos:**
- FTP con configuraciones muy específicas
- Mainframe (VSAM, IMS)
- Aplicaciones propietarias

**Alternativa:**
1. Verificar conectores disponibles en ADF
2. Considerar Self-Hosted Integration Runtime
3. Evaluar migración de datos a fuentes modernas primero

**Esfuerzo:** Variable - puede requerir re-arquitectura

---

### 2. Configuraciones Avanzadas de Sources/Targets

**Limitación:**
- Configuraciones avanzadas de queries SQL en Source Qualifier
- Optimizaciones específicas de base de datos
- Partitioning hints

**Alternativa:**
- Configurar queries en datasets de ADF manualmente
- Ajustar partitioning en Data Flow settings

**Esfuerzo:** 1-2 horas por source/target complejo

---

## Limitaciones de Performance y Optimización

### 1. Partitioning Strategy

**Limitación:**
- Configuración de partitioning de PowerCenter no se transfiere

**Impacto:**
- Puede afectar performance en ADF

**Alternativa:**
1. Analizar volumen de datos
2. Configurar partitioning en ADF Data Flow manualmente
3. Opciones: Round Robin, Hash, Dynamic Range, Fixed Range

**Esfuerzo:** 1-2 horas para tuning inicial

---

### 2. Caching Strategy

**Limitación:**
- Configuraciones de cache en Lookup, Aggregator no se migran

**Alternativa:**
- Configurar broadcast en Lookup transformations
- Ajustar compute size en pipeline

**Esfuerzo:** 30 min - 1 hora

---

## Limitaciones de Validación

### 1. Validación de Datos

**Limitación:**
- No se generan tests de validación de datos automáticamente
- Comparación pre/post migración requiere herramientas externas

**Recomendación:**
1. Extraer samples de datos de PowerCenter
2. Ejecutar mismo mapping en ADF
3. Comparar resultados
4. Herramientas sugeridas: Great Expectations, dbt tests

**Esfuerzo:** 4-8 horas por mapping crítico

---

### 2. Completitud de Migración

**Limitación:**
- La herramienta reporta warnings y errors, pero verificación final es manual

**Recomendación:**
- Revisar reporte de migración completo
- Validar cada warning en detalle
- Ejecutar tests end-to-end

---

## Resumen de Esfuerzo Manual por Componente

| Componente | Soportado | Esfuerzo Manual Estimado |
|------------|-----------|--------------------------|
| Source Qualifier | ✅ Completo | 0-30 min (configuración de Linked Service) |
| Expression | ✅ Completo | 0-1 hora (validación de expresiones complejas) |
| Filter | ✅ Completo | 0-30 min (validación de condiciones) |
| Aggregator | ✅ Completo | 0-30 min (validación de group by) |
| Joiner | ✅ Completo | 0-1 hora (validación de condiciones de join) |
| Sorter | ✅ Completo | 0-15 min |
| Router | ⚠️ Parcial | 1-2 horas (configurar condiciones) |
| Lookup | ⚠️ Parcial | 1-3 horas (configurar dataset y cache) |
| Update Strategy | ❌ No soportado | 2-4 horas (crear Alter Row) |
| Sequence Generator | ❌ No soportado | 1-2 horas (implementar alternativa) |
| Normalizer | ❌ No soportado | 3-5 horas (recrear con Flatten/Unpivot) |
| Rank | ❌ No soportado | 1-2 horas (crear Window transformation) |
| Mapplet | ❌ No soportado | 2-4 horas por mapplet |
| Workflow | ❌ No soportado | 4-8 horas por workflow |

---

## Recomendaciones Generales

1. **Priorizar Mappings Simples**: Empezar por mappings con transformaciones básicas soportadas
2. **Validación Incremental**: Validar cada mapping migrado antes de continuar
3. **Documentar Cambios**: Mantener registro de ajustes manuales realizados
4. **Testing Exhaustivo**: Ejecutar tests con data real en ambiente de desarrollo
5. **Capacitación**: Equipo debe conocer diferencias entre PowerCenter y ADF

---

## Contacto y Soporte

Para reportar limitaciones adicionales o solicitar soporte:
- **GitHub Issues**: https://github.com/entix/powercenter-to-adf/issues
- **Email**: contacto@entix.cl

---

**Última actualización**: Enero 2025
**Versión del documento**: 1.0
**Autor**: Equipo Entix
