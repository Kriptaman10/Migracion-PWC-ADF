# PowerCenter to Azure Data Factory Migrator - Web Interface v2.5

![Version](https://img.shields.io/badge/version-2.5.0-blue)
![Python](https://img.shields.io/badge/python-3.8+-blue)
![Streamlit](https://img.shields.io/badge/streamlit-1.31+-red)

Interfaz web interactiva para migrar mappings de Informatica PowerCenter a Azure Data Factory.

## 🎯 Características de la v2.5

### ✨ Nuevas Funcionalidades Web

- **📁 Carga Flexible**:
  - Drag & drop de archivos XML individuales
  - Carga batch desde carpeta externa
  - Validación automática de archivos

- **⚙️ Configuración Interactiva**:
  - Editor de parámetros pre-migración
  - Configuración de linked services
  - Settings avanzados (logging, error handling, paralelismo)
  - Exportación de configuración a YAML

- **🔍 Preview Comparativo**:
  - Comparación lado a lado PowerCenter vs ADF
  - Vista detallada de cada transformación
  - Navegación interactiva entre componentes
  - Generación de diagramas Mermaid

- **📊 Resultados y Exportación**:
  - Métricas visuales (transformaciones, errores, warnings, tiempo)
  - Visualización de Pipeline y DataFlow JSON
  - Copiar al portapapeles
  - Descarga individual de archivos
  - **Paquete ZIP completo** con todos los archivos generados

### 🔧 Transformaciones Soportadas (heredadas de v2.0)

- ✅ Source Qualifier
- ✅ Expression Transformation
- ✅ Filter Transformation
- ✅ Joiner Transformation (Inner, Left, Right, Full Outer)
- ✅ Aggregator Transformation (SUM, AVG, COUNT, MIN, MAX)
- ✅ Lookup Transformation (Database, Flat File, SQL Override)
- ✅ Router Transformation
- ✅ Sorter Transformation
- ✅ Update Strategy Transformation
- ✅ Target Definition

## 📋 Requisitos

### Software Necesario

- Python 3.8 o superior
- pip (gestor de paquetes de Python)

### Dependencias Python

Instalar en dos pasos:

```bash
# 1. Dependencias base (v2.0)
pip install -r requirements.txt

# 2. Dependencias web (v2.5)
pip install -r requirements-streamlit.txt
```

## 🚀 Instalación

### Opción 1: Instalación Rápida

```bash
# Clonar el repositorio
git clone https://github.com/yourusername/migracion-pwc-adf.git
cd migracion-pwc-adf

# Instalar todas las dependencias
pip install -r requirements.txt
pip install -r requirements-streamlit.txt

# Ejecutar la aplicación web
streamlit run app.py
```

### Opción 2: Entorno Virtual (Recomendado)

```bash
# Crear entorno virtual
python -m venv venv

# Activar entorno virtual
# En Windows:
venv\Scripts\activate
# En Linux/Mac:
source venv/bin/activate

# Instalar dependencias
pip install -r requirements.txt
pip install -r requirements-streamlit.txt

# Ejecutar
streamlit run app.py
```

## 💻 Uso de la Interfaz Web

### 1. Iniciar la Aplicación

```bash
streamlit run app.py
```

La aplicación se abrirá automáticamente en tu navegador en `http://localhost:8501`

### 2. Flujo de Trabajo

#### **Tab 1: Upload & Load** 📁

1. **Opción A - Archivo Individual**:
   - Arrastra y suelta un archivo XML
   - O usa el botón "Browse files"

2. **Opción B - Carga Batch**:
   - Ingresa la ruta de la carpeta con los XMLs
   - Ejemplo: `C:\Users\usuario\mappings\`
   - Click en "Load from folder"

3. Selecciona el archivo que quieres migrar (si cargaste varios)
4. Click en "Continue to Configuration"

#### **Tab 2: Configuration** ⚙️

Configura los parámetros de la migración:

**Pipeline Settings:**
- Nombre del pipeline de ADF
- Nombre del data flow
- Prefijo para datasets
- Carpeta en ADF

**Connection Settings:**
- Oracle Linked Service
- Azure SQL Linked Service
- Blob Storage Linked Service
- Integration Runtime

**Advanced Settings:**
- Enable logging
- Enable error handling
- Enable staging
- Max parallelism
- Data flow compute type

Cuando termines, click en "Save Configuration"

#### **Tab 3: Preview & Compare** 🔍

1. Espera a que se parsee el XML y se traduzca a ADF
2. Selecciona una transformación del dropdown
3. Compara lado a lado:
   - Panel izquierdo: PowerCenter
   - Panel derecho: Azure Data Factory
4. Expande "Detailed Transformation Mapping" para ver tabla comparativa
5. Expande "Complete Data Flow Diagram" para ver el flujo completo

#### **Tab 4: Results & Export** 📊

1. Click en "🚀 Run Migration"
2. Observa el progreso (4 pasos)
3. Revisa las métricas:
   - Total de transformaciones
   - Errores y warnings
   - Tiempo de procesamiento
4. Explora los tabs de resultados:
   - **Report**: Reporte detallado en Markdown
   - **Pipeline JSON**: Definición del pipeline
   - **DataFlow JSON**: Definición del data flow
   - **Datasets**: Datasets generados
5. Descarga:
   - Archivos individuales (botones de descarga)
   - **Paquete ZIP completo** (incluye todo)

## 📦 Contenido del Paquete ZIP

Al descargar el paquete ZIP obtendrás:

```
migration_<nombre>_<timestamp>.zip
├── pipeline.json          # Pipeline de ADF
├── dataflow.json          # Data Flow de ADF
├── report.md              # Reporte de migración
├── README.md              # Instrucciones de importación
└── datasets/              # Datasets (si aplica)
    ├── ds_source1.json
    └── ds_target1.json
```

## 🔧 Configuración Avanzada

### Archivo `config.yaml`

Puedes editar el archivo `config.yaml` para cambiar valores por defecto:

```yaml
migration:
  default_pipeline_prefix: "pl_"
  default_dataflow_prefix: "df_"
  oracle_linked_service: "ls_Oracle_OnPrem"
  blob_linked_service: "ls_AzureBlob_Storage"
  integration_runtime: "AutoResolveIntegrationRuntime"
  max_parallelism: 4
  # ... más opciones
```

### Personalización de UI

Edita `.streamlit/config.toml` para cambiar colores y configuración:

```toml
[theme]
primaryColor = "#0078D4"  # Color principal
backgroundColor = "#FFFFFF"
# ... más opciones
```

## 🐛 Troubleshooting

### Error: "ModuleNotFoundError: No module named 'streamlit'"

**Solución:**
```bash
pip install streamlit
```

### Error: "Cannot copy to clipboard"

**Causa:** La librería `pyperclip` no está instalada o no tiene permisos

**Solución:**
```bash
pip install pyperclip
```

En Linux, instala también:
```bash
sudo apt-get install xclip
```

### Error: "File not found" al cargar desde carpeta

**Causa:** Ruta incorrecta o permisos insuficientes

**Solución:**
- Verifica que la ruta sea absoluta y correcta
- Ejemplo correcto en Windows: `C:\Users\usuario\mappings\`
- Verifica permisos de lectura en la carpeta

### La aplicación es muy lenta

**Solución:**
- Verifica que no haya XMLs muy grandes (>50MB)
- Cierra otros tabs/aplicaciones que consuman recursos
- Considera usar la versión CLI para archivos muy grandes:
  ```bash
  python -m src.main --input mapping.xml --output ./output
  ```

## 📚 Diferencias entre v2.0 (CLI) y v2.5 (Web)

| Característica | v2.0 CLI | v2.5 Web |
|----------------|----------|----------|
| **Interfaz** | Línea de comandos | Navegador web |
| **Carga de archivos** | Parámetro `--input` | Drag & drop + folder browser |
| **Configuración** | Flags CLI | Formulario interactivo |
| **Preview** | No disponible | Comparación lado a lado |
| **Exportación** | Archivos a carpeta | ZIP descargable |
| **Métricas** | Log en consola | Dashboard visual |

### Cuándo usar cada versión:

- **Usa v2.0 (CLI)** si:
  - Quieres automatizar con scripts
  - Necesitas integrar en CI/CD
  - Trabajas con archivos muy grandes (>100MB)
  - Prefieres terminal

- **Usa v2.5 (Web)** si:
  - Quieres una experiencia visual
  - Necesitas comparar transformaciones
  - Trabajas en Windows sin conocimientos de terminal
  - Quieres explorar interactivamente

## 🤝 Soporte

### CLI v2.0 (sigue disponible):

```bash
python -m src.main --help
python -m src.main --input mapping.xml --output ./output --verbose
```

### Documentación adicional:

- [README.md](README.md) - Documentación general del proyecto
- [CHANGELOG.md](CHANGELOG.md) - Historial de cambios
- [docs/](docs/) - Documentación técnica detallada

## 👨‍💻 Desarrollo

Este proyecto fue desarrollado por:

- **Autor**: Benjamín Riquelme
- **Organización**: Entix SpA
- **Universidad**: Universidad Tecnológica Metropolitana (UTEM)
- **Versión**: 2.5.0
- **Fecha**: Enero 2025

## 📄 Licencia

Este proyecto está licenciado bajo MIT License. Ver archivo [LICENSE](LICENSE) para más detalles.

## 🎓 Agradecimientos

- Entix SpA por el soporte y la oportunidad de práctica profesional
- UTEM por el apoyo académico
- Comunidad de Azure Data Factory
- Comunidad de Informatica PowerCenter

---

**¿Preguntas o problemas?** Abre un issue en GitHub o contacta al equipo de desarrollo.

**¡Feliz migración!** 🚀
