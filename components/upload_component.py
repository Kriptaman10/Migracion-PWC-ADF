"""
Upload Component - PowerCenter to ADF Migrator v2.5

Componente de Streamlit para carga de archivos XML de PowerCenter.
Soporta carga individual y batch desde carpeta.

Author: Benjamín Riquelme
"""

import streamlit as st
from pathlib import Path
import os
import xml.etree.ElementTree as ET


def render_upload_tab():
    """
    Renderiza el tab de carga de archivos XML
    """
    st.header("📁 Upload PowerCenter Mappings")
    st.markdown("Upload XML files individually or load from a folder containing multiple mappings")

    col1, col2 = st.columns(2)

    # =====================================================
    # COLUMNA 1: CARGA INDIVIDUAL
    # =====================================================
    with col1:
        st.subheader("🔹 Drag & Drop Multiple Files")

        st.markdown("""
        <div style="background-color: #f0f2f6; padding: 20px; border-radius: 10px; border: 2px dashed #E31837; text-align: center; margin-bottom: 10px;">
            <h4 style="color: #E31837; margin: 0;">📂 Drop your XML files here</h4>
            <p style="color: #666; margin: 5px 0 0 0; font-size: 0.9rem;">You can drag multiple files at once or click Browse Files below</p>
        </div>
        """, unsafe_allow_html=True)

        uploaded_files = st.file_uploader(
            "Browse Files",
            type=['xml'],
            help="Upload one or more PowerCenter mapping XML files",
            key="multiple_uploader",
            label_visibility="collapsed",
            accept_multiple_files=True
        )

        if uploaded_files is not None and len(uploaded_files) > 0:
            # Procesar cada archivo subido
            new_files_count = 0
            errors = []

            for uploaded_file in uploaded_files:
                # Verificar si ya existe
                if any(f['name'] == uploaded_file.name for f in st.session_state['xml_files']):
                    continue

                # Guardar archivo temporalmente
                temp_path = Path("temp") / uploaded_file.name
                temp_path.parent.mkdir(exist_ok=True)

                try:
                    # Escribir archivo
                    with open(temp_path, "wb") as f:
                        f.write(uploaded_file.getbuffer())

                    # Validar que sea XML válido
                    try:
                        ET.parse(str(temp_path))

                        # Agregar a lista de archivos
                        st.session_state['xml_files'].append({
                            'name': uploaded_file.name,
                            'path': str(temp_path),
                            'size': round(uploaded_file.size / 1024, 2)
                        })
                        new_files_count += 1

                    except ET.ParseError as e:
                        errors.append(f"Invalid XML '{uploaded_file.name}': {str(e)}")
                        # Eliminar archivo inválido
                        if temp_path.exists():
                            temp_path.unlink()

                except Exception as e:
                    errors.append(f"Error processing '{uploaded_file.name}': {str(e)}")

            # Mostrar resultados
            if new_files_count > 0:
                st.success(f"✅ Successfully added {new_files_count} file(s)")
                st.rerun()

            if errors:
                for error in errors:
                    st.error(f"❌ {error}")

            if new_files_count == 0 and len(errors) == 0:
                st.info(f"ℹ️ All {len(uploaded_files)} file(s) already loaded")

    # =====================================================
    # COLUMNA 2: CARGA DESDE CARPETA
    # =====================================================
    with col2:
        st.subheader("🔹 Batch Upload from Folder")

        folder_path = st.text_input(
            "Folder path",
            placeholder=r"C:\Users\username\powercentermappings",
            help="Enter the full path to a folder containing XML files",
            key="folder_path"
        )

        col_btn1, col_btn2 = st.columns([1, 2])

        with col_btn1:
            if st.button("📂 Browse", help="Tip: Copy folder path from explorer"):
                st.info("💡 Tip: Copy the folder path from Windows Explorer and paste it above")

        with col_btn2:
            if st.button("📥 Load from folder", type="primary"):
                if folder_path:
                    xml_files = load_xmls_from_folder(folder_path)
                    if xml_files:
                        # Agregar archivos que no estén ya en la lista
                        new_files = 0
                        for xml_file in xml_files:
                            if not any(f['name'] == xml_file['name']
                                     for f in st.session_state['xml_files']):
                                st.session_state['xml_files'].append(xml_file)
                                new_files += 1

                        if new_files > 0:
                            st.success(f"✅ Added {new_files} new XML files")
                            st.rerun()
                        else:
                            st.info("ℹ️ All files from folder already loaded")
                else:
                    st.error("⚠️ Please enter a folder path")

    st.markdown("---")

    # =====================================================
    # LISTA DE ARCHIVOS CARGADOS
    # =====================================================
    if st.session_state.get('xml_files'):
        st.subheader(f"📋 Loaded Files ({len(st.session_state['xml_files'])})")

        # Opciones de visualización
        view_mode = st.radio(
            "View mode:",
            ["Table", "Cards"],
            horizontal=True,
            key="view_mode_radio"
        )

        if view_mode == "Table":
            render_table_view()
        else:
            render_cards_view()

        st.markdown("---")

        # Botón para continuar
        col_continue = st.columns([1, 2, 1])[1]
        with col_continue:
            if st.button("▶️ Continue to Configuration", type="primary", use_container_width=True):
                if st.session_state['xml_files']:
                    # Establecer el primer archivo como activo si no hay ninguno seleccionado
                    if not st.session_state.get('xml_path'):
                        first_file = st.session_state['xml_files'][0]
                        st.session_state['xml_path'] = first_file['path']
                        st.session_state['xml_name'] = Path(first_file['name']).stem

                    st.session_state['xml_loaded'] = True

                    # Auto-navegación al tab de Configuration
                    st.session_state['active_tab'] = 1
                    st.success("✅ Ready to configure!")
                    st.rerun()
                else:
                    st.warning("⚠️ Please load at least one XML file")

    else:
        st.info("ℹ️ No files loaded yet. Upload a file or load from a folder to get started.")


def render_table_view():
    """Renderiza lista de archivos en formato tabla"""
    for idx, xml_file in enumerate(st.session_state['xml_files']):
        col1, col2, col3, col4 = st.columns([4, 2, 1, 1])

        with col1:
            # Mostrar indicador si es el archivo activo
            is_active = st.session_state.get('xml_path') == xml_file['path']
            icon = "▶️" if is_active else "📄"
            st.text(f"{icon} {xml_file['name']}")

        with col2:
            st.text(f"{xml_file['size']} KB")

        with col3:
            if st.button("👁️", key=f"view_{idx}", help="Select this file"):
                st.session_state['xml_path'] = xml_file['path']
                st.session_state['xml_name'] = Path(xml_file['name']).stem
                st.session_state['xml_loaded'] = True
                # Limpiar datos parseados previos
                st.session_state['parsed_data'] = None
                st.session_state['adf_data'] = None
                st.session_state['migrated'] = False
                st.success(f"✅ Selected: {xml_file['name']}")
                st.rerun()

        with col4:
            if st.button("🗑️", key=f"delete_{idx}", help="Remove this file"):
                # Si es el archivo activo, limpiar session state
                if st.session_state.get('xml_path') == xml_file['path']:
                    st.session_state['xml_path'] = None
                    st.session_state['xml_name'] = None
                    st.session_state['xml_loaded'] = False
                    st.session_state['parsed_data'] = None
                    st.session_state['adf_data'] = None
                    st.session_state['migrated'] = False

                st.session_state['xml_files'].pop(idx)
                st.rerun()


def render_cards_view():
    """Renderiza lista de archivos en formato cards"""
    cols = st.columns(3)

    for idx, xml_file in enumerate(st.session_state['xml_files']):
        with cols[idx % 3]:
            is_active = st.session_state.get('xml_path') == xml_file['path']
            border_color = "#28a745" if is_active else "#ddd"
            bg_color = "#d4edda" if is_active else "#f8f9fa"

            st.markdown(f"""
            <div style="border: 2px solid {border_color};
                        background-color: {bg_color};
                        padding: 15px;
                        border-radius: 8px;
                        margin-bottom: 15px;">
                <h4 style="margin: 0 0 10px 0;">
                    {'▶️' if is_active else '📄'} {xml_file['name']}
                </h4>
                <p style="margin: 0; color: #666;">
                    <strong>Size:</strong> {xml_file['size']} KB
                </p>
                {'<p style="margin: 5px 0 0 0; color: #28a745; font-weight: bold;">✓ Active</p>' if is_active else ''}
            </div>
            """, unsafe_allow_html=True)

            col_a, col_b = st.columns(2)

            with col_a:
                if st.button("👁️ Select", key=f"view_card_{idx}", use_container_width=True):
                    st.session_state['xml_path'] = xml_file['path']
                    st.session_state['xml_name'] = Path(xml_file['name']).stem
                    st.session_state['xml_loaded'] = True
                    # Limpiar datos previos
                    st.session_state['parsed_data'] = None
                    st.session_state['adf_data'] = None
                    st.session_state['migrated'] = False
                    st.rerun()

            with col_b:
                if st.button("🗑️ Remove", key=f"delete_card_{idx}", use_container_width=True):
                    # Si es el archivo activo, limpiar session state
                    if st.session_state.get('xml_path') == xml_file['path']:
                        st.session_state['xml_path'] = None
                        st.session_state['xml_name'] = None
                        st.session_state['xml_loaded'] = False
                        st.session_state['parsed_data'] = None
                        st.session_state['adf_data'] = None
                        st.session_state['migrated'] = False

                    st.session_state['xml_files'].pop(idx)
                    st.rerun()


def load_xmls_from_folder(folder_path):
    """
    Carga todos los archivos XML válidos de una carpeta

    Args:
        folder_path: Ruta a la carpeta con XMLs

    Returns:
        Lista de diccionarios con información de archivos XML válidos
    """
    xml_files = []

    # Verificar que la carpeta existe
    if not os.path.exists(folder_path):
        st.error(f"❌ Folder not found: {folder_path}")
        return []

    try:
        # Buscar todos los archivos XML
        path_obj = Path(folder_path)
        xml_found = list(path_obj.glob("*.xml"))

        if not xml_found:
            st.warning(f"⚠️ No XML files found in: {folder_path}")
            return []

        # Validar cada archivo
        valid_count = 0
        invalid_files = []

        for file_path in xml_found:
            try:
                # Intentar parsear el XML
                ET.parse(str(file_path))

                # Si es válido, agregar a la lista
                xml_files.append({
                    'name': file_path.name,
                    'path': str(file_path),
                    'size': round(file_path.stat().st_size / 1024, 2)
                })
                valid_count += 1

            except ET.ParseError:
                invalid_files.append(file_path.name)
            except Exception as e:
                st.warning(f"⚠️ Error reading {file_path.name}: {str(e)}")

        # Mostrar resumen
        if valid_count > 0:
            st.success(f"✅ Found {valid_count} valid XML file(s)")

        if invalid_files:
            with st.expander(f"⚠️ Skipped {len(invalid_files)} invalid XML file(s)"):
                for invalid in invalid_files:
                    st.text(f"  • {invalid}")

        return xml_files

    except PermissionError:
        st.error(f"❌ Permission denied: Cannot read folder {folder_path}")
        return []
    except Exception as e:
        st.error(f"❌ Error loading folder: {str(e)}")
        return []


def validate_xml_file(file_path):
    """
    Valida que un archivo XML sea válido y sea de PowerCenter

    Args:
        file_path: Ruta al archivo XML

    Returns:
        Tuple (is_valid, error_message)
    """
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()

        # Verificar que sea un archivo de PowerCenter
        # (Los archivos PC suelen tener un tag root específico)
        if root.tag not in ['POWERMART', 'Repository', 'Folder']:
            return False, "Not a valid PowerCenter XML file"

        return True, None

    except ET.ParseError as e:
        return False, f"XML Parse Error: {str(e)}"
    except Exception as e:
        return False, f"Error: {str(e)}"
