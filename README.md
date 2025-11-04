# 🔄 Migrador de PowerCenter a Azure Data Factory 

![Python Version](https://img.shields.io/badge/python-3.8%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)
![Status](https://img.shields.io/badge/status-MVP-yellow)

Herramienta CLI automatizada para migrar mappings de Informatica PowerCenter a Azure Data Factory.

---

## 📋 Descripción

**Migrador de PowerCenter a Azure Data Factory** es una herramienta de línea de comandos desarrollada para automatizar el proceso de migración de pipelines ETL desde Informatica PowerCenter (on-premise) hacia Azure Data Factory (cloud).

### El Problema

Las empresas que migran su infraestructura de datos a la nube enfrentan el desafío de **convertir cientos de mappings de PowerCenter manualmente**, un proceso que:
- ⏱️ Consume semanas o meses de trabajo
- 🐛 Es propenso a errores humanos
- 📊 Requiere conocimiento profundo de ambas plataformas
- 💰 Genera altos costos de consultoría

### La Solución

Esta herramienta **automatiza el 70-80% del trabajo de migración** para transformaciones básicas, generando:
- ✅ Pipelines de Azure Data Factory listos para importar
- ✅ Dataflows con transformaciones traducidas
- ✅ Reportes detallados de componentes migrados
- ✅ Identificación clara de elementos que requieren ajuste manual

---

## 🎯 Características

- ✅ **Parser robusto de XML** de PowerCenter 10.x
- ✅ **Traducción automática** de transformaciones básicas (Expression, Filter, Aggregator, Joiner, Sorter)
- ✅ **Conversión de expresiones** y funciones comunes (TO_DATE, SUBSTR, DECODE, IIF)
- ✅ **Mapeo de tipos de datos** entre plataformas
- ✅ **Generación de JSON** válido para Azure Data Factory v2
- ✅ **Reporte detallado** de migración con estadísticas
- ✅ **Validación de formato** JSON con jsonschema
- ✅ **Interfaz CLI moderna** con output colorizado (rich)
- ⚠️ **Alcance limitado** a transformaciones básicas en v1.0 (ver [Limitaciones](#-limitaciones))

---

## 🚀 Instalación

### Prerrequisitos

- Python 3.8 o superior
- pip (gestor de paquetes de Python)
- Git (opcional)

### Instalación rápida

```bash
# Clonar el repositorio
git clone https://github.com/entix/powercenter-to-adf.git
cd powercenter-to-adf

# Crear entorno virtual (recomendado)
python -m venv venv

# Activar entorno virtual
# En Windows:
venv\Scripts\activate
# En Linux/Mac:
source venv/bin/activate

# Instalar dependencias
pip install -r requirements.txt

# Instalar la herramienta
pip install -e .
```

### Verificar instalación

```bash
pc-to-adf --help
```

---

## 💻 Uso

### Comando básico

```bash
pc-to-adf input/mi_mapping.xml
```

### Opciones avanzadas

```bash
# Especificar directorio de salida
pc-to-adf input/mapping.xml --output custom_output/

# Modo verbose para debugging
pc-to-adf input/mapping.xml --verbose

# Solo validar sin generar archivos
pc-to-adf input/mapping.xml --validate-only

# Ver todas las opciones
pc-to-adf --help
```

### Flujo de trabajo completo

1. **Exportar mapping desde PowerCenter Designer**
   - File → Export → Select XML Format
   - Guardar archivo `.xml`

2. **Ejecutar la herramienta**
   ```bash
   pc-to-adf mappings/customer_etl.xml --output adf_pipelines/
   ```

3. **Revisar el output**
   - `pipeline_[nombre].json` - Pipeline de ADF
   - `dataflow_[nombre].json` - Dataflow con transformaciones
   - `migration_report.json` - Reporte detallado

4. **Importar a Azure Data Factory**
   - Abrir Azure Portal → Data Factory
   - Author → Pipeline → Import from template
   - Subir archivo JSON generado

5. **Validar y ajustar manualmente**
   - Revisar conexiones a fuentes de datos
   - Verificar expresiones complejas
   - Ejecutar pruebas

---

## 📊 Componentes Soportados

### Transformaciones

| Componente PowerCenter | Equivalente Azure Data Factory | Estado v1.0 |
|------------------------|--------------------------------|-------------|
| **Source Qualifier**   | Source                         | ✅ Soportado |
| **Expression**         | Derived Column                 | ✅ Soportado |
| **Filter**             | Filter                         | ✅ Soportado |
| **Aggregator**         | Aggregate                      | ✅ Soportado |
| **Joiner**             | Join                           | ✅ Soportado |
| **Sorter**             | Sort                           | ✅ Soportado |
| **Router**             | Conditional Split              | ⚠️ Parcial   |
| **Lookup**             | Lookup                         | ⚠️ Parcial   |
| **Update Strategy**    | Alter Row                      | ❌ No soportado |
| **Sequence Generator** | -                              | ❌ No soportado |
| **Normalizer**         | Flatten                        | ❌ No soportado |

### Funciones

| Función PowerCenter | Función Azure Data Factory | Estado |
|---------------------|----------------------------|--------|
| `TO_DATE()`         | `toDate()`                 | ✅ |
| `SYSDATE`           | `currentTimestamp()`       | ✅ |
| `SUBSTR()`          | `substring()`              | ✅ |
| `TRIM()`            | `trim()`                   | ✅ |
| `UPPER()` / `LOWER()` | `upper()` / `lower()`    | ✅ |
| `LENGTH()`          | `length()`                 | ✅ |
| `DECODE()`          | `case when ... then`       | ✅ |
| `IIF()`             | `iif()`                    | ✅ |
| `INSTR()`           | `indexOf()`                | ✅ |
| `CONCAT()`          | `concat()`                 | ✅ |

### Tipos de Datos

| PowerCenter | Azure Data Factory |
|-------------|--------------------|
| `decimal`   | `Int32`            |
| `number`    | `Int32`            |
| `varchar2`  | `String`           |
| `string`    | `String`           |
| `date`      | `DateTime`         |
| `timestamp` | `DateTime`         |

---

## 🗂️ Estructura del Proyecto

```
powercenter-to-adf/
├── README.md                      # Este archivo
├── requirements.txt               # Dependencias Python
├── setup.py                       # Configuración de instalación
├── LICENSE                        # Licencia MIT
│
├── src/                          # Código fuente
│   ├── main.py                   # CLI principal
│   ├── parser.py                 # Parser de XML PowerCenter
│   ├── translator.py             # Traductor PC → ADF
│   ├── generator.py              # Generador de JSON ADF
│   └── utils.py                  # Utilidades comunes
│
├── tests/                        # Tests unitarios
│   ├── test_parser.py
│   ├── test_translator.py
│   └── test_generator.py
│
├── examples/                     # Ejemplos de uso
│   ├── input/                    # XMLs de ejemplo
│   └── output/                   # JSONs generados
│
├── docs/                         # Documentación técnica
│   ├── MAPEO_TRANSFORMACIONES.md
│   ├── LIMITACIONES.md
│   ├── MANUAL_USO.md
│   └── ARQUITECTURA.md
│
└── config/
    └── mapping_rules.json        # Reglas de mapeo PC→ADF
```

---

## 🗺️ Roadmap v2.0

Características planificadas para futuras versiones:

- 🔲 Soporte para Lookup Transformation completo
- 🔲 Update Strategy → Alter Row
- 🔲 Sequence Generator con Azure SQL Sequences
- 🔲 Normalizer → Flatten/Unpivot
- 🔲 Expresiones complejas con regex
- 🔲 Parámetros y variables de sesión
- 🔲 Mapplets como Data Flow reutilizables
- 🔲 Deployment automático a Azure (Azure CLI integration)
- 🔲 Comparación pre/post migración (data validation)
- 🔲 Interfaz web para usuarios no técnicos
- 🔲 Soporte para PowerCenter 9.x

---

## ⚠️ Limitaciones

### No soportado en v1.0

- ❌ **Transformaciones complejas**: Update Strategy, Normalizer, Custom Transformations
- ❌ **Mapplets**: Se deben migrar manualmente como Data Flows separados
- ❌ **Sesiones y Workflows**: Solo mappings individuales
- ❌ **Parámetros de sesión**: Se generan como valores hardcodeados
- ❌ **Expresiones con funciones personalizadas**
- ❌ **Conectores legacy**: Solo se mapean tipos estándar (SQL, Oracle, Flat Files)

### Requiere ajuste manual

- ⚠️ **Conexiones**: Los Linked Services deben configurarse en Azure
- ⚠️ **Expresiones complejas**: Revisar sintaxis de funciones avanzadas
- ⚠️ **Performance tuning**: Partitioning, parallel processing
- ⚠️ **Lookup con queries complejos**

Ver [docs/LIMITACIONES.md](docs/LIMITACIONES.md) para detalles completos.

---

## 🧪 Testing

```bash
# Ejecutar todos los tests
pytest

# Tests con coverage
pytest --cov=src

# Tests en modo verbose
pytest -v
```

---

## 🤝 Contribución

Contribuciones son bienvenidas. Por favor:

1. Fork el proyecto
2. Crea una rama para tu feature (`git checkout -b feature/AmazingFeature`)
3. Commit tus cambios (`git commit -m 'Add: nueva funcionalidad'`)
4. Push a la rama (`git push origin feature/AmazingFeature`)
5. Abre un Pull Request

### Estándares de código

- Seguir PEP 8
- Usar Black para formateo: `black src/`
- Pasar linting: `pylint src/`
- Tests para nuevas funcionalidades

---

## 📚 Documentación

- [Manual de Uso Detallado](docs/MANUAL_USO.md)
- [Arquitectura del Sistema](docs/ARQUITECTURA.md)
- [Mapeo de Transformaciones](docs/MAPEO_TRANSFORMACIONES.md)
- [Limitaciones Conocidas](docs/LIMITACIONES.md)

---

## 🙏 Agradecimientos

Proyecto desarrollado durante práctica profesional en **[Entix SpA](https://entix.cl)** - Consultoría de Integración de Datos.

**Entix** es una empresa chilena especializada en:
- 🏢 Modernización de plataformas de datos
- ☁️ Migraciones cloud (Azure, AWS, GCP)
- 📊 Implementación de Data Lakes y Data Warehouses
- 🔄 Integración de datos enterprise (Informatica, Talend, SSIS)

Agradecimientos especiales al equipo técnico de Entix por la mentoría y soporte durante el desarrollo de esta herramienta.

---

## 📄 Licencia

MIT License - ver [LICENSE](LICENSE) para detalles.

Copyright (c) 2025 Entix SpA

---

## 📞 Contacto

- **Empresa**: Entix SpA
- **Web**: [https://entix.cl](https://entix.cl)
- **Email**: contacto@entix.cl

---

**⭐ Si este proyecto te resulta útil, considera darle una estrella en GitHub**
