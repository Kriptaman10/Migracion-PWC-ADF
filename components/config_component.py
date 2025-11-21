"""
Configuration Component - PowerCenter to ADF Migrator v2.5

Componente de Streamlit para configuración de parámetros de migración.
Permite editar nombres de pipelines, linked services, y settings avanzados.

Author: Benjamín Riquelme
"""

import streamlit as st
from pathlib import Path
import yaml


def render_config_tab():
    """
    Renderiza el tab de configuración de migración
    """
    st.header("⚙️ Migration Configuration")

    if not st.session_state.get('xml_loaded'):
        st.warning("⚠️ Please upload and select an XML file first")
        if st.button("← Go to Upload Tab"):
            st.info("Please click on the 'Upload & Load' tab above")
        st.stop()

    st.markdown("Configure Azure Data Factory settings before migration")

    # Cargar configuración por defecto si no existe
    if 'config' not in st.session_state or st.session_state['config'] is None:
        st.session_state['config'] = load_default_config()

    config = st.session_state['config']

    # =====================================================
    # SECCIÓN 1: PIPELINE SETTINGS
    # =====================================================
    st.subheader("🔷 Pipeline Settings")

    col1, col2 = st.columns(2)

    with col1:
        config['pipeline_name'] = st.text_input(
            "Pipeline Name",
            value=config.get('pipeline_name', f"pl_{st.session_state.get('xml_name', 'mapping')}"),
            help="Name for the Azure Data Factory pipeline",
            key="pipeline_name_input"
        )

        config['dataflow_name'] = st.text_input(
            "Data Flow Name",
            value=config.get('dataflow_name', f"df_{st.session_state.get('xml_name', 'mapping')}"),
            help="Name for the ADF data flow",
            key="dataflow_name_input"
        )

    with col2:
        config['dataset_prefix'] = st.text_input(
            "Dataset Prefix",
            value=config.get('dataset_prefix', 'ds_'),
            help="Prefix for all generated datasets",
            key="dataset_prefix_input"
        )

        config['pipeline_folder'] = st.text_input(
            "Pipeline Folder",
            value=config.get('pipeline_folder', 'Migrations'),
            help="Folder name in ADF to organize pipelines",
            key="pipeline_folder_input"
        )

    st.markdown("---")

    # =====================================================
    # SECCIÓN 2: CONNECTION SETTINGS
    # =====================================================
    st.subheader("🔷 Connection Settings")

    col3, col4 = st.columns(2)

    with col3:
        config['oracle_linked_service'] = st.text_input(
            "Oracle Linked Service",
            value=config.get('oracle_linked_service', 'ls_Oracle_OnPrem'),
            help="Name of Oracle linked service in ADF",
            key="oracle_ls_input"
        )

        config['sql_linked_service'] = st.text_input(
            "Azure SQL Linked Service",
            value=config.get('sql_linked_service', 'ls_AzureSQL_Database'),
            help="Name of Azure SQL Database linked service",
            key="sql_ls_input"
        )

    with col4:
        config['blob_linked_service'] = st.text_input(
            "Blob Storage Linked Service",
            value=config.get('blob_linked_service', 'ls_AzureBlob_Storage'),
            help="Name of Azure Blob Storage linked service",
            key="blob_ls_input"
        )

        config['integration_runtime'] = st.selectbox(
            "Integration Runtime",
            options=[
                "AutoResolveIntegrationRuntime",
                "IR-OnPrem",
                "IR-Azure-Custom",
                "IR-SHIR-OnPremises"
            ],
            index=0,
            help="Integration runtime for pipeline execution",
            key="ir_select"
        )

    st.markdown("---")

    # =====================================================
    # SECCIÓN 3: ADVANCED SETTINGS
    # =====================================================
    st.subheader("🔷 Advanced Settings")

    col5, col6, col7 = st.columns(3)

    with col5:
        config['enable_logging'] = st.checkbox(
            "Enable detailed logging",
            value=config.get('enable_logging', True),
            help="Add logging activities to pipeline",
            key="logging_check"
        )

        config['enable_error_handling'] = st.checkbox(
            "Enable error outputs",
            value=config.get('enable_error_handling', True),
            help="Create error output datasets for failed rows",
            key="error_check"
        )

    with col6:
        config['enable_staging'] = st.checkbox(
            "Enable staging",
            value=config.get('enable_staging', False),
            help="Use staging blob storage for large data transfers",
            key="staging_check"
        )

        config['enable_partitioning'] = st.checkbox(
            "Enable partitioning",
            value=config.get('enable_partitioning', False),
            help="Partition large datasets for parallel processing",
            key="partition_check"
        )

    with col7:
        config['max_parallelism'] = st.slider(
            "Max Parallelism",
            min_value=1,
            max_value=20,
            value=config.get('max_parallelism', 4),
            help="Maximum parallel activities in pipeline",
            key="parallelism_slider"
        )

        config['data_flow_compute'] = st.selectbox(
            "Data Flow Compute",
            options=["General Purpose", "Memory Optimized", "Compute Optimized"],
            index=["General Purpose", "Memory Optimized", "Compute Optimized"].index(
                config.get('data_flow_compute', 'General Purpose')
            ),
            help="Compute type for data flow execution",
            key="compute_select"
        )

    st.markdown("---")

    # =====================================================
    # SECCIÓN 4: NAMING CONVENTIONS (OPCIONAL)
    # =====================================================
    with st.expander("🔧 Naming Conventions (Optional)", expanded=False):
        st.markdown("Define custom naming patterns for generated objects")

        col8, col9 = st.columns(2)

        with col8:
            config['source_dataset_pattern'] = st.text_input(
                "Source Dataset Pattern",
                value=config.get('source_dataset_pattern', '{prefix}{source_name}'),
                help="Use {prefix}, {source_name}, {schema}",
                key="source_pattern"
            )

            config['target_dataset_pattern'] = st.text_input(
                "Target Dataset Pattern",
                value=config.get('target_dataset_pattern', '{prefix}{target_name}'),
                help="Use {prefix}, {target_name}, {schema}",
                key="target_pattern"
            )

        with col9:
            config['transformation_prefix'] = st.text_input(
                "Transformation Prefix",
                value=config.get('transformation_prefix', ''),
                help="Prefix for all transformation names",
                key="transform_prefix"
            )

            config['use_original_names'] = st.checkbox(
                "Preserve original names",
                value=config.get('use_original_names', True),
                help="Keep original PowerCenter names when possible",
                key="original_names_check"
            )

    st.markdown("---")

    # =====================================================
    # BOTONES DE ACCIÓN
    # =====================================================
    col_btn1, col_btn2, col_btn3 = st.columns([1, 1, 2])

    with col_btn1:
        if st.button("💾 Save Configuration", type="primary", use_container_width=True):
            st.session_state['config'] = config
            st.session_state['configured'] = True

            # Auto-navegación al tab de Preview
            st.session_state['active_tab'] = 2
            st.success("✅ Configuration saved successfully!")
            st.balloons()
            st.rerun()

    with col_btn2:
        if st.button("🔄 Reset to Defaults", use_container_width=True):
            st.session_state['config'] = load_default_config()
            st.info("ℹ️ Configuration reset to defaults")
            st.rerun()

    with col_btn3:
        # Botón para exportar configuración
        if st.button("📤 Export Config", use_container_width=True):
            config_yaml = yaml.dump(config, default_flow_style=False, sort_keys=False)
            st.download_button(
                label="📥 Download config.yaml",
                data=config_yaml,
                file_name="migration_config.yaml",
                mime="text/yaml",
                use_container_width=True,
                key="download_config"
            )

    # =====================================================
    # PREVIEW DE CONFIGURACIÓN
    # =====================================================
    if st.session_state.get('configured'):
        with st.expander("👁️ View Current Configuration", expanded=False):
            st.json(config)

        # Resumen visual
        st.markdown("### 📋 Configuration Summary")
        col_sum1, col_sum2, col_sum3 = st.columns(3)

        with col_sum1:
            st.info(f"""
            **Pipeline:** `{config.get('pipeline_name', 'N/A')}`

            **Data Flow:** `{config.get('dataflow_name', 'N/A')}`
            """)

        with col_sum2:
            st.info(f"""
            **Oracle LS:** `{config.get('oracle_linked_service', 'N/A')}`

            **Blob LS:** `{config.get('blob_linked_service', 'N/A')}`
            """)

        with col_sum3:
            st.info(f"""
            **Integration Runtime:** `{config.get('integration_runtime', 'N/A')}`

            **Max Parallelism:** `{config.get('max_parallelism', 'N/A')}`
            """)


def load_default_config():
    """
    Carga configuración por defecto para la migración

    Returns:
        Diccionario con configuración por defecto
    """
    # Intentar cargar desde config.yaml si existe
    config_path = Path('config.yaml')
    if config_path.exists():
        try:
            with open(config_path, 'r') as f:
                loaded_config = yaml.safe_load(f)
                if loaded_config and 'migration' in loaded_config:
                    return loaded_config['migration']
        except Exception:
            pass  # Si falla, usar defaults hardcoded

    # Configuración por defecto hardcoded
    xml_name = st.session_state.get('xml_name', 'mapping')

    return {
        # Pipeline settings
        'pipeline_name': f'pl_{xml_name}',
        'dataflow_name': f'df_{xml_name}',
        'dataset_prefix': 'ds_',
        'pipeline_folder': 'Migrations',

        # Connection settings
        'oracle_linked_service': 'ls_Oracle_OnPrem',
        'sql_linked_service': 'ls_AzureSQL_Database',
        'blob_linked_service': 'ls_AzureBlob_Storage',
        'integration_runtime': 'AutoResolveIntegrationRuntime',

        # Advanced settings
        'enable_logging': True,
        'enable_error_handling': True,
        'enable_staging': False,
        'enable_partitioning': False,
        'max_parallelism': 4,
        'data_flow_compute': 'General Purpose',

        # Naming conventions
        'source_dataset_pattern': '{prefix}{source_name}',
        'target_dataset_pattern': '{prefix}{target_name}',
        'transformation_prefix': '',
        'use_original_names': True
    }


def save_config_to_file(config, file_path='config.yaml'):
    """
    Guarda la configuración actual a un archivo YAML

    Args:
        config: Diccionario de configuración
        file_path: Ruta donde guardar el archivo
    """
    try:
        with open(file_path, 'w') as f:
            yaml.dump({'migration': config}, f, default_flow_style=False, sort_keys=False)
        return True
    except Exception as e:
        st.error(f"Error saving config: {str(e)}")
        return False


def load_config_from_file(file_path='config.yaml'):
    """
    Carga configuración desde un archivo YAML

    Args:
        file_path: Ruta del archivo de configuración

    Returns:
        Diccionario de configuración o None si falla
    """
    try:
        with open(file_path, 'r') as f:
            loaded = yaml.safe_load(f)
            if loaded and 'migration' in loaded:
                return loaded['migration']
        return None
    except Exception:
        return None
