"""
PowerCenter to Azure Data Factory Migrator - Web Interface v2.5

Aplicación web con Streamlit para migración de mappings de Informatica PowerCenter
a Azure Data Factory con interfaz interactiva y comparación lado a lado.

Author: Benjamín Riquelme
Organization: Entix SpA / Universidad Tecnológica Metropolitana (UTEM)
Version: 2.5.0
"""

import streamlit as st
import sys
from pathlib import Path
import json
import time

# Agregar src y components al path
sys.path.insert(0, str(Path(__file__).parent / 'src'))
sys.path.insert(0, str(Path(__file__).parent / 'components'))

# Importar módulos del core (v2.0)
from parser import PowerCenterParser
from translator import PowerCenterTranslator
from generator import ADFGenerator
from validator import MappingValidator

# Importar componentes de la UI
from upload_component import render_upload_tab
from config_component import render_config_tab
from preview_component import render_preview_tab
from export_component import render_export_tab

# ====================================================================
# CONFIGURACIÓN DE LA PÁGINA
# ====================================================================

st.set_page_config(
    page_title="PowerCenter to ADF Migrator v2.5",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://github.com/yourusername/pc-to-adf',
        'Report a bug': 'https://github.com/yourusername/pc-to-adf/issues',
        'About': """
        # PowerCenter to ADF Migrator v2.5

        Herramienta de migración automatizada de Informatica PowerCenter a Azure Data Factory.

        Desarrollado por Benjamín Riquelme para Entix SpA y UTEM.
        """
    }
)

# ====================================================================
# CSS PERSONALIZADO
# ====================================================================

st.markdown("""
<style>
    /* Header principal */
    .main-header {
        font-size: 2.5rem;
        color: #0078D4;
        text-align: center;
        margin-bottom: 1rem;
        font-weight: bold;
    }

    .sub-header {
        text-align: center;
        color: #666;
        font-size: 1rem;
        margin-bottom: 2rem;
    }

    /* Métricas */
    .stMetric {
        background-color: #f0f2f6;
        padding: 15px;
        border-radius: 8px;
        border-left: 4px solid #0078D4;
    }

    /* Boxes de estado */
    .success-box {
        background-color: #d4edda;
        border-left: 5px solid #28a745;
        padding: 15px;
        margin: 10px 0;
        border-radius: 4px;
    }

    .warning-box {
        background-color: #fff3cd;
        border-left: 5px solid #ffc107;
        padding: 15px;
        margin: 10px 0;
        border-radius: 4px;
    }

    .error-box {
        background-color: #f8d7da;
        border-left: 5px solid #dc3545;
        padding: 15px;
        margin: 10px 0;
        border-radius: 4px;
    }

    .info-box {
        background-color: #d1ecf1;
        border-left: 5px solid #0078D4;
        padding: 15px;
        margin: 10px 0;
        border-radius: 4px;
    }

    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }

    .stTabs [data-baseweb="tab"] {
        padding: 12px 24px;
        background-color: #f0f2f6;
        border-radius: 8px 8px 0 0;
    }

    .stTabs [aria-selected="true"] {
        background-color: #0078D4;
        color: white;
    }

    /* Botones */
    .stButton > button {
        border-radius: 6px;
        font-weight: 500;
    }

    /* Expanders */
    .streamlit-expanderHeader {
        font-weight: 600;
        font-size: 1.1rem;
    }

    /* File uploader */
    .uploadedFile {
        border: 2px dashed #0078D4;
        border-radius: 8px;
        padding: 20px;
    }

    /* JSON viewer */
    .stJson {
        background-color: #f8f9fa;
        border-radius: 6px;
        padding: 10px;
    }
</style>
""", unsafe_allow_html=True)

# ====================================================================
# INICIALIZACIÓN DE SESSION STATE
# ====================================================================

def initialize_session_state():
    """Inicializa todas las variables de session state necesarias"""

    # Estado de archivos
    if 'xml_files' not in st.session_state:
        st.session_state['xml_files'] = []

    # Estado del proceso
    if 'xml_loaded' not in st.session_state:
        st.session_state['xml_loaded'] = False

    if 'configured' not in st.session_state:
        st.session_state['configured'] = False

    if 'migrated' not in st.session_state:
        st.session_state['migrated'] = False

    # Datos del proceso
    if 'xml_path' not in st.session_state:
        st.session_state['xml_path'] = None

    if 'xml_name' not in st.session_state:
        st.session_state['xml_name'] = None

    if 'parsed_data' not in st.session_state:
        st.session_state['parsed_data'] = None

    if 'adf_data' not in st.session_state:
        st.session_state['adf_data'] = None

    if 'config' not in st.session_state:
        st.session_state['config'] = None

    # Resultados
    if 'pipeline_json' not in st.session_state:
        st.session_state['pipeline_json'] = None

    if 'dataflow_json' not in st.session_state:
        st.session_state['dataflow_json'] = None

    if 'report_md' not in st.session_state:
        st.session_state['report_md'] = None

    if 'datasets' not in st.session_state:
        st.session_state['datasets'] = []

    # Métricas
    if 'errors_count' not in st.session_state:
        st.session_state['errors_count'] = 0

    if 'warnings_count' not in st.session_state:
        st.session_state['warnings_count'] = 0

    if 'migration_time' not in st.session_state:
        st.session_state['migration_time'] = 0.0

# Ejecutar inicialización
initialize_session_state()

# ====================================================================
# HEADER PRINCIPAL
# ====================================================================

st.markdown('<h1 class="main-header">🔄 PowerCenter to Azure Data Factory Migrator</h1>',
            unsafe_allow_html=True)
st.markdown('<p class="sub-header">Version 2.5 - Web Interface | Entix SpA & UTEM</p>',
            unsafe_allow_html=True)

# ====================================================================
# SIDEBAR - NAVEGACIÓN Y ESTADO
# ====================================================================

with st.sidebar:
    st.title("📋 Navigation")
    st.markdown("---")

    # Estado del proceso
    st.subheader("📊 Process Status")

    if st.session_state.get('xml_loaded'):
        st.success("✅ XML Loaded")
        if st.session_state.get('xml_name'):
            st.caption(f"File: {st.session_state['xml_name']}")
    else:
        st.info("📁 Upload XML file")

    if st.session_state.get('configured'):
        st.success("✅ Configured")
        if st.session_state.get('config'):
            st.caption(f"Pipeline: {st.session_state['config'].get('pipeline_name', 'N/A')}")
    else:
        st.info("⚙️ Configure settings")

    if st.session_state.get('migrated'):
        st.success("✅ Migration Complete")
        st.caption(f"Time: {st.session_state.get('migration_time', 0):.2f}s")
    else:
        st.info("🚀 Ready to migrate")

    st.markdown("---")

    # Acciones rápidas (solo si hay migración completa)
    if st.session_state.get('migrated'):
        st.subheader("⚡ Quick Actions")

        # Importar función para crear ZIP
        try:
            from export_component import create_migration_package

            # Generar ZIP
            zip_bytes = create_migration_package(
                st.session_state['pipeline_json'],
                st.session_state['dataflow_json'],
                st.session_state['report_md'],
                st.session_state.get('datasets', [])
            )

            # Botón de descarga
            st.download_button(
                label="📥 Download ZIP",
                data=zip_bytes,
                file_name=f"migration_{st.session_state.get('xml_name', 'mapping')}_{int(time.time())}.zip",
                mime="application/zip",
                use_container_width=True,
                key="sidebar_download_zip"
            )

        except Exception as e:
            st.warning(f"⚠️ Quick download unavailable")

    st.markdown("---")

    # Información del sistema
    st.subheader("ℹ️ System Info")
    st.caption(f"**Version:** 2.5.0")
    st.caption(f"**Python:** {sys.version.split()[0]}")
    st.caption(f"**Streamlit:** {st.__version__}")

    st.markdown("---")

    # Reset
    if st.button("🔄 Reset Application", use_container_width=True):
        # Limpiar session state
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.rerun()

# ====================================================================
# TABS PRINCIPALES
# ====================================================================

tab1, tab2, tab3, tab4 = st.tabs([
    "📁 Upload & Load",
    "⚙️ Configuration",
    "🔍 Preview & Compare",
    "📊 Results & Export"
])

with tab1:
    render_upload_tab()

with tab2:
    render_config_tab()

with tab3:
    render_preview_tab()

with tab4:
    render_export_tab()

# ====================================================================
# FOOTER
# ====================================================================

st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #666; font-size: 0.85rem; padding: 20px 0;">
    <p style="margin: 5px 0;">
        <strong>PowerCenter to Azure Data Factory Migration Tool v2.5</strong>
    </p>
    <p style="margin: 5px 0;">
        Desarrollado para <strong>Entix SpA</strong> | <strong>Universidad Tecnológica Metropolitana (UTEM)</strong>
    </p>
    <p style="margin: 5px 0; color: #999;">
        © 2025 Benjamín Riquelme - Práctica Profesional & Tesis de Grado
    </p>
</div>
""", unsafe_allow_html=True)
