"""
Export Component - PowerCenter to ADF Migrator v2.5

Componente de Streamlit para resultados y exportación de migración.
Incluye visualización de resultados, métricas, y descarga de archivos.

Author: Benjamín Riquelme
"""

import streamlit as st
import json
import time
import traceback
from pathlib import Path
from io import BytesIO
import zipfile

# Importar módulos del core
from src import ADFGenerator, MappingValidator


def render_export_tab():
    """
    Renderiza el tab de resultados y exportación
    """
    st.header("📊 Migration Results & Export")

    # Mostrar errores persistentes si existen
    if st.session_state.get('migration_error'):
        st.error(f"❌ **Previous Migration Failed**")
        st.error(f"**Error:** {st.session_state['migration_error']}")
        with st.expander("🔍 View Full Error Details", expanded=False):
            st.code(st.session_state.get('migration_error_trace', 'No trace available'), language='python')

        # Botón para limpiar el error y reintentar
        if st.button("🔄 Clear Error and Retry", type="secondary"):
            st.session_state['migration_error'] = None
            st.session_state['migration_error_trace'] = None
            st.session_state['migrated'] = False
            st.rerun()

        st.markdown("---")

    if not st.session_state.get('xml_loaded') or not st.session_state.get('configured'):
        st.warning("⚠️ Please complete upload and configuration first")
        st.stop()

    # =====================================================
    # BOTÓN PARA EJECUTAR MIGRACIÓN
    # =====================================================
    if not st.session_state.get('migrated'):
        st.info("ℹ️ Ready to migrate. Click the button below to start the migration process.")

        col_center = st.columns([1, 2, 1])[1]
        with col_center:
            if st.button("🚀 Run Migration", type="primary", use_container_width=True):
                run_migration()
                st.rerun()

        st.stop()

    # =====================================================
    # MOSTRAR MÉTRICAS
    # =====================================================
    render_migration_metrics()

    st.markdown("---")

    # =====================================================
    # TABS DE RESULTADOS
    # =====================================================
    result_tab1, result_tab2, result_tab3, result_tab4 = st.tabs([
        "📄 Report",
        "📋 Pipeline JSON",
        "🔄 DataFlow JSON",
        "📦 Datasets"
    ])

    with result_tab1:
        render_report_tab()

    with result_tab2:
        render_pipeline_json_tab()

    with result_tab3:
        render_dataflow_json_tab()

    with result_tab4:
        render_datasets_tab()

    st.markdown("---")

    # =====================================================
    # SECCIÓN DE DESCARGA COMPLETA
    # =====================================================
    render_download_section()


def render_migration_metrics():
    """Renderiza métricas de migración"""
    st.subheader("📈 Migration Summary")

    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric(
            label="📦 Transformations",
            value=len(st.session_state['parsed_data'].transformations),
            help="Total transformations migrated"
        )

    with col2:
        errors_count = st.session_state.get('errors_count', 0)
        st.metric(
            label="❌ Errors",
            value=errors_count,
            delta=None,
            delta_color="inverse",
            help="Validation errors found"
        )

    with col3:
        warnings_count = st.session_state.get('warnings_count', 0)
        st.metric(
            label="⚠️ Warnings",
            value=warnings_count,
            delta=None,
            delta_color="off",
            help="Warnings generated"
        )

    with col4:
        migration_time = st.session_state.get('migration_time', 0)
        st.metric(
            label="⏱️ Time",
            value=f"{migration_time:.2f}s",
            help="Total processing time"
        )

    with col5:
        files_count = 2 + len(st.session_state.get('datasets', []))  # pipeline + dataflow + datasets
        st.metric(
            label="📁 Files",
            value=files_count,
            help="Total files generated"
        )

    # Barra de progreso visual
    if errors_count == 0:
        st.success("✅ Migration completed successfully with no errors!")
    elif errors_count > 0:
        st.error(f"❌ Migration completed with {errors_count} errors")

    if warnings_count > 0:
        st.warning(f"⚠️ {warnings_count} warnings require attention")


def render_report_tab():
    """Renderiza el tab del reporte"""
    st.subheader("📄 Migration Report")

    report_md = st.session_state.get('report_md', '')

    if not report_md:
        st.info("ℹ️ No report generated yet")
        return

    # Opción de vista
    view_mode = st.radio(
        "View mode:",
        ["Rendered", "Markdown Source"],
        horizontal=True,
        key="report_view_mode"
    )

    if view_mode == "Rendered":
        st.markdown(report_md)
    else:
        st.code(report_md, language='markdown')

    st.markdown("---")

    # Botones de acción
    col1, col2, col3 = st.columns(3)

    with col1:
        if st.button("📋 Copy Markdown", use_container_width=True):
            try:
                import pyperclip
                pyperclip.copy(report_md)
                st.success("✅ Copied to clipboard!")
            except:
                st.warning("⚠️ Please install pyperclip to copy to clipboard")

    with col2:
        st.download_button(
            "📥 Download MD",
            data=report_md,
            file_name=f"report_{st.session_state['xml_name']}.md",
            mime="text/markdown",
            use_container_width=True,
            key="download_report_md"
        )

    with col3:
        # Nota: PDF generation requiere dependencias adicionales
        st.info("💡 PDF export available in ZIP package")


def render_pipeline_json_tab():
    """Renderiza el tab del pipeline JSON"""
    st.subheader("📋 Azure Data Factory Pipeline")

    pipeline_json = st.session_state.get('pipeline_json', {})

    if not pipeline_json:
        st.info("ℹ️ No pipeline JSON generated yet")
        return

    # Información del pipeline
    col_info1, col_info2 = st.columns(2)

    with col_info1:
        st.info(f"**Pipeline Name:** `{pipeline_json.get('name', 'N/A')}`")

    with col_info2:
        activities_count = len(pipeline_json.get('properties', {}).get('activities', []))
        st.info(f"**Activities:** `{activities_count}`")

    # Mostrar JSON
    st.json(pipeline_json)

    st.markdown("---")

    # Botones de acción
    col1, col2 = st.columns(2)

    with col1:
        if st.button("📋 Copy JSON", key="copy_pipeline", use_container_width=True):
            try:
                import pyperclip
                json_str = json.dumps(pipeline_json, indent=2)
                pyperclip.copy(json_str)
                st.success("✅ Copied to clipboard!")
            except:
                st.warning("⚠️ Please install pyperclip to copy to clipboard")

    with col2:
        st.download_button(
            "📥 Download JSON",
            data=json.dumps(pipeline_json, indent=2),
            file_name=f"pipeline_{st.session_state['xml_name']}.json",
            mime="application/json",
            use_container_width=True,
            key="download_pipeline"
        )


def render_dataflow_json_tab():
    """Renderiza el tab del dataflow JSON"""
    st.subheader("🔄 Azure Data Factory Data Flow")

    dataflow_json = st.session_state.get('dataflow_json', {})

    if not dataflow_json:
        st.info("ℹ️ No dataflow JSON generated yet")
        return

    # Información del dataflow
    properties = dataflow_json.get('properties', {})
    sources_count = len(properties.get('sources', []))
    transforms_count = len(properties.get('transformations', []))
    sinks_count = len(properties.get('sinks', []))

    col_info1, col_info2, col_info3 = st.columns(3)

    with col_info1:
        st.info(f"**Sources:** `{sources_count}`")

    with col_info2:
        st.info(f"**Transformations:** `{transforms_count}`")

    with col_info3:
        st.info(f"**Sinks:** `{sinks_count}`")

    # Mostrar JSON
    st.json(dataflow_json)

    st.markdown("---")

    # Botones de acción
    col1, col2 = st.columns(2)

    with col1:
        if st.button("📋 Copy JSON", key="copy_dataflow", use_container_width=True):
            try:
                import pyperclip
                json_str = json.dumps(dataflow_json, indent=2)
                pyperclip.copy(json_str)
                st.success("✅ Copied to clipboard!")
            except:
                st.warning("⚠️ Please install pyperclip to copy to clipboard")

    with col2:
        st.download_button(
            "📥 Download JSON",
            data=json.dumps(dataflow_json, indent=2),
            file_name=f"dataflow_{st.session_state['xml_name']}.json",
            mime="application/json",
            use_container_width=True,
            key="download_dataflow"
        )


def render_datasets_tab():
    """Renderiza el tab de datasets"""
    st.subheader("📦 Dataset Definitions")

    datasets = st.session_state.get('datasets', [])

    if not datasets:
        st.info("ℹ️ No separate dataset files generated (datasets may be embedded in pipeline/dataflow)")
        return

    st.markdown(f"**Total Datasets:** `{len(datasets)}`")

    # Selector de dataset
    dataset_names = [ds.get('name', f'dataset_{i}') for i, ds in enumerate(datasets)]
    selected_dataset = st.selectbox(
        "Select dataset to view:",
        options=dataset_names,
        key="dataset_selector"
    )

    # Mostrar dataset seleccionado
    dataset = next((ds for ds in datasets if ds.get('name') == selected_dataset), None)

    if dataset:
        st.json(dataset.get('content', dataset))

        st.markdown("---")

        # Botones de acción
        col1, col2 = st.columns(2)

        with col1:
            if st.button("📋 Copy JSON", key=f"copy_dataset_{selected_dataset}",
                        use_container_width=True):
                try:
                    import pyperclip
                    json_str = json.dumps(dataset.get('content', dataset), indent=2)
                    pyperclip.copy(json_str)
                    st.success("✅ Copied to clipboard!")
                except:
                    st.warning("⚠️ Please install pyperclip to copy to clipboard")

        with col2:
            st.download_button(
                "📥 Download JSON",
                data=json.dumps(dataset.get('content', dataset), indent=2),
                file_name=f"{selected_dataset}.json",
                mime="application/json",
                use_container_width=True,
                key=f"download_dataset_{selected_dataset}"
            )


def render_download_section():
    """Renderiza la sección de descarga completa"""
    st.subheader("📦 Download Complete Package")

    st.markdown("""
    Download all generated files in a single ZIP package:
    - Pipeline JSON
    - DataFlow JSON
    - All Dataset JSONs (if any)
    - Migration Report (Markdown)
    """)

    col_center = st.columns([1, 2, 1])[1]

    with col_center:
        # Generar ZIP
        try:
            zip_bytes = create_migration_package(
                st.session_state.get('pipeline_json', {}),
                st.session_state.get('dataflow_json', {}),
                st.session_state.get('report_md', ''),
                st.session_state.get('datasets', [])
            )

            st.download_button(
                "📥 Download ZIP Package",
                data=zip_bytes,
                file_name=f"migration_{st.session_state['xml_name']}_{int(time.time())}.zip",
                mime="application/zip",
                use_container_width=True,
                type="primary",
                key="download_zip_main"
            )

        except Exception as e:
            st.error(f"❌ Error creating ZIP package: {str(e)}")


def run_migration():
    """Ejecuta el proceso completo de migración"""
    start_time = time.time()

    progress_bar = st.progress(0)
    status_text = st.empty()

    try:
        # =====================================================
        # PASO 1: VALIDAR (25%)
        # =====================================================
        status_text.text("⏳ Step 1/4: Validating mapping...")
        progress_bar.progress(25)

        validator = MappingValidator()
        errors, warnings = validator.validate(st.session_state['parsed_data'])

        st.session_state['errors_count'] = len(errors)
        st.session_state['warnings_count'] = len(warnings)

        if errors:
            st.error(f"❌ Validation failed with {len(errors)} errors")
            for error in errors:
                st.error(f"  • {error}")
            progress_bar.empty()
            status_text.empty()
            return

        time.sleep(0.3)

        # =====================================================
        # PASO 2: VERIFICAR TRADUCCIÓN (50%)
        # =====================================================
        status_text.text("⏳ Step 2/4: Verifying translation...")
        progress_bar.progress(50)

        # La traducción ya está en session_state desde el preview
        adf_data = st.session_state['adf_data']

        time.sleep(0.3)

        # =====================================================
        # PASO 3: GENERAR ARCHIVOS (75%)
        # =====================================================
        status_text.text("⏳ Step 3/4: Generating ADF files...")
        progress_bar.progress(75)

        generator = ADFGenerator()
        config = st.session_state.get('config', {})
        mapping_name = st.session_state.get('xml_name', 'mapping')

        # Generar archivos ADF
        result = generator.generate_all(
            name=mapping_name,
            translated_structure=adf_data,
            original_metadata=st.session_state['parsed_data']
        )

        # Parsear resultados (son strings JSON, necesitamos convertir)
        import json
        st.session_state['pipeline_json'] = json.loads(result.get('pipeline', '{}'))
        st.session_state['dataflow_json'] = json.loads(result.get('dataflow', '{}'))
        st.session_state['report_md'] = result.get('report', '')
        st.session_state['datasets'] = []

        time.sleep(0.3)

        # =====================================================
        # PASO 4: FINALIZAR (100%)
        # =====================================================
        status_text.text("⏳ Step 4/4: Finalizing...")
        progress_bar.progress(100)

        end_time = time.time()
        st.session_state['migration_time'] = end_time - start_time
        st.session_state['migrated'] = True

        time.sleep(0.3)

        # Limpiar
        progress_bar.empty()
        status_text.empty()

        # Mostrar éxito
        st.success("✅ Migration completed successfully!")
        st.balloons()

    except Exception as e:
        progress_bar.empty()
        status_text.empty()

        # Guardar error en session_state para que persista
        error_msg = str(e)
        st.session_state['migration_error'] = error_msg
        st.session_state['migration_error_trace'] = traceback.format_exc()

        st.error(f"❌ **Migration Failed**")
        st.error(f"**Error:** {error_msg}")

        # Mostrar traceback completo en expander
        with st.expander("🔍 View Full Error Details"):
            st.code(st.session_state['migration_error_trace'], language='python')


def create_migration_package(pipeline_json, dataflow_json, report_md, datasets):
    """
    Crea un archivo ZIP con todos los archivos generados

    Args:
        pipeline_json: Pipeline JSON dict
        dataflow_json: DataFlow JSON dict
        report_md: Reporte en Markdown
        datasets: Lista de datasets

    Returns:
        Bytes del archivo ZIP
    """
    zip_buffer = BytesIO()

    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        # 1. Pipeline JSON
        if pipeline_json:
            zip_file.writestr(
                'pipeline.json',
                json.dumps(pipeline_json, indent=2)
            )

        # 2. DataFlow JSON
        if dataflow_json:
            zip_file.writestr(
                'dataflow.json',
                json.dumps(dataflow_json, indent=2)
            )

        # 3. Report Markdown
        if report_md:
            zip_file.writestr(
                'report.md',
                report_md
            )

        # 4. Datasets (en carpeta separada)
        if datasets:
            for dataset in datasets:
                dataset_name = dataset.get('name', 'dataset')
                dataset_content = dataset.get('content', dataset)
                zip_file.writestr(
                    f"datasets/{dataset_name}.json",
                    json.dumps(dataset_content, indent=2)
                )

        # 5. README con instrucciones
        readme_content = generate_readme()
        zip_file.writestr(
            'README.md',
            readme_content
        )

    zip_buffer.seek(0)
    return zip_buffer.getvalue()


def generate_readme():
    """Genera archivo README para el paquete ZIP"""
    return """# PowerCenter to Azure Data Factory Migration Package

## Contents

- **pipeline.json**: Azure Data Factory pipeline definition
- **dataflow.json**: Azure Data Factory data flow definition
- **datasets/**: Dataset definitions for sources and sinks (if any)
- **report.md**: Detailed migration report (Markdown)

## How to Import to Azure Data Factory

### Option 1: Using Azure Portal

1. Open Azure Data Factory Studio
2. Go to "Author" tab
3. Click "+" → "Pipeline" → "Import from pipeline template"
4. Upload `pipeline.json`
5. Repeat for data flow using `dataflow.json`

### Option 2: Using Azure CLI

```bash
# Set variables
RESOURCE_GROUP="your-resource-group"
FACTORY_NAME="your-adf-name"

# Import pipeline
az datafactory pipeline create \\
  --resource-group $RESOURCE_GROUP \\
  --factory-name $FACTORY_NAME \\
  --name "YourPipelineName" \\
  --pipeline @pipeline.json

# Import data flow
az datafactory data-flow create \\
  --resource-group $RESOURCE_GROUP \\
  --factory-name $FACTORY_NAME \\
  --name "YourDataFlowName" \\
  --data-flow @dataflow.json
```

### Option 3: Using PowerShell

```powershell
# Set variables
$ResourceGroup = "your-resource-group"
$FactoryName = "your-adf-name"

# Import pipeline
Set-AzDataFactoryV2Pipeline `
  -ResourceGroupName $ResourceGroup `
  -DataFactoryName $FactoryName `
  -Name "YourPipelineName" `
  -DefinitionFile "pipeline.json"
```

## Post-Import Checklist

- [ ] Verify all linked services exist in ADF
- [ ] Update dataset connection strings if needed
- [ ] Test pipeline with sample data
- [ ] Review and adjust integration runtime settings
- [ ] Configure pipeline triggers if needed
- [ ] Set up monitoring and alerts

## Support

For issues or questions, refer to the migration report or contact your data engineering team.

---
Generated by PowerCenter to ADF Migrator v2.5
Developed for Entix SpA | Universidad Tecnológica Metropolitana (UTEM)
"""
