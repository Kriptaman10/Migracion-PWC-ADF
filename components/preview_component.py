"""
Preview Component - PowerCenter to ADF Migrator v2.5

Componente de Streamlit para preview y comparación lado a lado de transformaciones
PowerCenter vs Azure Data Factory.

Author: Benjamín Riquelme
"""

import streamlit as st

# Importar módulos del core
from src import PowerCenterParser, PowerCenterTranslator


def render_preview_tab():
    """
    Renderiza el tab de preview y comparación PC vs ADF
    """
    st.header("🔍 PowerCenter vs Azure Data Factory Comparison")

    if not st.session_state.get('xml_loaded'):
        st.warning("⚠️ Please upload and select an XML file first")
        st.stop()

    if not st.session_state.get('configured'):
        st.warning("⚠️ Please complete configuration first")
        st.stop()

    # =====================================================
    # PROCESAR XML SI NO SE HA HECHO
    # =====================================================
    if 'parsed_data' not in st.session_state or st.session_state['parsed_data'] is None:
        with st.spinner("🔄 Parsing PowerCenter XML..."):
            try:
                from pathlib import Path
                parser = PowerCenterParser()
                parsed_data = parser.parse_file(Path(st.session_state['xml_path']))
                st.session_state['parsed_data'] = parsed_data
                st.success("✅ XML parsed successfully")
            except Exception as e:
                st.error(f"❌ Failed to parse XML: {str(e)}")
                st.exception(e)
                st.stop()

    # =====================================================
    # TRADUCIR A ADF SI NO SE HA HECHO
    # =====================================================
    if 'adf_data' not in st.session_state or st.session_state['adf_data'] is None:
        with st.spinner("🔄 Translating to Azure Data Factory..."):
            try:
                translator = PowerCenterTranslator()
                adf_data = translator.translate_mapping(st.session_state['parsed_data'])
                st.session_state['adf_data'] = adf_data
                st.success("✅ Translation completed")
            except Exception as e:
                st.error(f"❌ Failed to translate: {str(e)}")
                st.exception(e)
                st.stop()

    # =====================================================
    # MOSTRAR RESUMEN
    # =====================================================
    col_summary1, col_summary2, col_summary3 = st.columns(3)

    parsed_data = st.session_state['parsed_data']

    with col_summary1:
        st.metric(
            "📦 Transformations",
            len(parsed_data.transformations)
        )

    with col_summary2:
        st.metric(
            "📥 Sources",
            len(parsed_data.sources)
        )

    with col_summary3:
        st.metric(
            "📤 Targets",
            len(parsed_data.targets)
        )

    st.markdown("---")

    # =====================================================
    # SELECTOR DE TRANSFORMACIÓN
    # =====================================================
    st.subheader("🎯 Select Transformation to Compare")

    transformations = parsed_data.transformations

    if not transformations:
        st.warning("⚠️ No transformations found in mapping")
        st.stop()

    transform_names = [f"{t.name} ({t.type})" for t in transformations]

    selected_transform_str = st.selectbox(
        "Choose a transformation:",
        options=transform_names,
        key="transform_selector"
    )

    selected_transform_name = selected_transform_str.split(" (")[0]

    st.markdown("---")

    # =====================================================
    # COMPARACIÓN LADO A LADO
    # =====================================================
    col_pc, col_adf = st.columns([1, 1])

    with col_pc:
        st.subheader("🔵 PowerCenter")
        render_pc_transformation(selected_transform_name)

    with col_adf:
        st.subheader("🟦 Azure Data Factory")
        render_adf_transformation(selected_transform_name)

    st.markdown("---")

    # =====================================================
    # VISTA DETALLADA EXPANDIBLE
    # =====================================================
    with st.expander("📊 Detailed Transformation Mapping", expanded=True):
        show_detailed_comparison(selected_transform_name)

    # =====================================================
    # VISTA DE FLUJO COMPLETO
    # =====================================================
    with st.expander("🔀 Complete Data Flow Diagram", expanded=False):
        render_flow_diagram()


def render_pc_transformation(transform_name):
    """Renderiza detalles de transformación de PowerCenter"""
    pc_data = st.session_state['parsed_data']
    transform = next(
        (t for t in pc_data.transformations if t.name == transform_name),
        None
    )

    if not transform:
        st.error("Transformation not found")
        return

    st.markdown(f"**Type:** `{transform.type}`")
    st.markdown(f"**Name:** `{transform.name}`")

    if transform.description:
        st.info(f"📝 {transform.description}")

    # Detalles específicos por tipo
    transform_type = transform.type
    props = transform.properties

    if transform_type == 'Source Qualifier':
        render_pc_source_qualifier(transform)
    elif transform_type == 'Joiner':
        render_pc_joiner(transform)
    elif transform_type == 'Aggregator':
        render_pc_aggregator(transform)
    elif transform_type == 'Lookup':
        render_pc_lookup(transform)
    elif transform_type == 'Router':
        render_pc_router(transform)
    elif transform_type == 'Sorter':
        render_pc_sorter(transform)
    elif transform_type == 'Expression':
        render_pc_expression(transform)
    elif transform_type == 'Filter':
        render_pc_filter(transform)
    else:
        # Mostrar campos genéricos
        with st.expander("View Fields"):
            st.json(transform)


def render_pc_source_qualifier(transform):
    """Renderiza Source Qualifier de PC"""
    props = transform.properties
    if props.get('source_filter'):
        st.code(f"WHERE {props['source_filter']}", language='sql')
    if props.get('sql_override'):
        with st.expander("View SQL Override"):
            st.code(props['sql_override'], language='sql')


def render_pc_joiner(transform):
    """Renderiza Joiner de PC"""
    props = transform.properties
    st.markdown(f"**Join Type:** `{props.get('join_type', 'N/A')}`")
    st.markdown("**Join Condition:**")
    st.code(props.get('join_condition', 'N/A'), language='sql')

    with st.expander("View Fields"):
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Master Fields:**")
            for field in props.get('master_fields', []):
                st.text(f"  • {field}")
        with col2:
            st.markdown("**Detail Fields:**")
            for field in props.get('detail_fields', []):
                st.text(f"  • {field}")


def render_pc_aggregator(transform):
    """Renderiza Aggregator de PC"""
    props = transform.properties
    st.markdown("**Group By:**")
    for field in props.get('group_by', []):
        st.code(field, language='sql')

    st.markdown("**Aggregations:**")
    for agg in props.get('aggregates', [])[:10]:
        st.code(f"{agg.get('name', 'N/A')} = {agg.get('expression', 'N/A')}", language='sql')

    if len(props.get('aggregates', [])) > 10:
        st.info(f"... and {len(props['aggregates']) - 10} more aggregations")


def render_pc_lookup(transform):
    """Renderiza Lookup de PC"""
    props = transform.properties
    st.markdown(f"**Lookup Table:** `{props.get('lookup_table', 'N/A')}`")
    st.markdown(f"**Source Type:** `{props.get('source_type', 'Database')}`")
    st.markdown("**Lookup Condition:**")
    st.code(props.get('lookup_condition', 'N/A'), language='sql')

    if props.get('sql_override'):
        with st.expander("View SQL Override"):
            st.code(props['sql_override'], language='sql')


def render_pc_router(transform):
    """Renderiza Router de PC"""
    props = transform.properties
    st.markdown("**Output Groups:**")
    for group in props.get('groups', []):
        if group.get('expression'):
            st.code(f"{group.get('name', 'N/A')}: {group['expression']}", language='sql')
        else:
            st.text(f"{group.get('name', 'DEFAULT')}: DEFAULT")


def render_pc_sorter(transform):
    """Renderiza Sorter de PC"""
    props = transform.properties
    st.markdown("**Sort Keys:**")
    for key in props.get('sort_keys', []):
        st.text(f"  • {key.get('name', 'N/A')} ({key.get('direction', 'ASC')})")


def render_pc_expression(transform):
    """Renderiza Expression de PC"""
    props = transform.properties
    st.markdown("**Derived Columns:**")
    expressions = props.get('expressions', [])
    for expr in expressions[:5]:
        st.code(f"{expr.get('name', 'N/A')} = {expr.get('expression', 'N/A')}", language='sql')

    if len(expressions) > 5:
        with st.expander(f"View all {len(expressions)} expressions"):
            for expr in expressions:
                st.code(f"{expr.get('name', 'N/A')} = {expr.get('expression', 'N/A')}", language='sql')


def render_pc_filter(transform):
    """Renderiza Filter de PC"""
    props = transform.properties
    st.markdown("**Filter Condition:**")
    st.code(props.get('filter_condition', 'N/A'), language='sql')


def render_adf_transformation(transform_name):
    """Renderiza detalles de transformación de ADF"""
    adf_data = st.session_state['adf_data']
    transform = next(
        (t for t in adf_data.get('transformations', [])
         if t.get('source_name') == transform_name or t.get('name') == transform_name),
        None
    )

    if not transform:
        st.warning("No ADF equivalent found")
        return

    st.markdown(f"**Type:** `{props.get('type', 'Unknown')}`")
    st.markdown(f"**Name:** `{props.get('name', 'Unknown')}`")

    # Renderizar JSON de la transformación
    with st.expander("📄 View JSON Configuration"):
        st.json(transform)

    # Detalles específicos por tipo
    transform_type = props.get('type', '')

    if transform_type == 'source':
        render_adf_source(transform)
    elif transform_type == 'join':
        render_adf_join(transform)
    elif transform_type == 'aggregate':
        render_adf_aggregate(transform)
    elif transform_type == 'lookup':
        render_adf_lookup(transform)
    elif transform_type == 'conditionalSplit':
        render_adf_conditional_split(transform)
    elif transform_type == 'sort':
        render_adf_sort(transform)
    elif transform_type == 'derivedColumn':
        render_adf_derived_column(transform)
    elif transform_type == 'filter':
        render_adf_filter(transform)


def render_adf_source(transform):
    """Renderiza Source de ADF"""
    st.markdown(f"**Dataset:** `{props.get('dataset', 'N/A')}`")
    if props.get('sourceFilter'):
        st.code(props['sourceFilter'], language='sql')


def render_adf_join(transform):
    """Renderiza Join de ADF"""
    st.markdown(f"**Join Type:** `{props.get('joinType', 'N/A')}`")
    st.markdown("**Join Conditions:**")
    for cond in props.get('joinConditions', []):
        st.code(
            f"{cond.get('leftColumn', 'N/A')} {cond.get('operator', '==')} {cond.get('rightColumn', 'N/A')}",
            language='python'
        )


def render_adf_aggregate(transform):
    """Renderiza Aggregate de ADF"""
    st.markdown("**Group By:**")
    for field in props.get('groupBy', []):
        st.code(field, language='python')

    st.markdown("**Aggregates:**")
    for agg in props.get('aggregates', []):
        st.code(f"{agg.get('name', 'N/A')} = {agg.get('expression', 'N/A')}", language='python')


def render_adf_lookup(transform):
    """Renderiza Lookup de ADF"""
    st.markdown(f"**Dataset:** `{props.get('lookupDataset', 'N/A')}`")
    st.markdown("**Lookup Conditions:**")
    for cond in props.get('lookupConditions', []):
        st.code(
            f"{cond.get('leftColumn', 'N/A')} {cond.get('operator', '==')} {cond.get('rightColumn', 'N/A')}",
            language='python'
        )


def render_adf_conditional_split(transform):
    """Renderiza Conditional Split de ADF"""
    st.markdown("**Conditions:**")
    for cond in props.get('conditions', []):
        st.code(f"{cond.get('name', 'N/A')}: {cond.get('expression', 'N/A')}", language='python')


def render_adf_sort(transform):
    """Renderiza Sort de ADF"""
    st.markdown("**Sort Columns:**")
    for col in props.get('sortColumns', []):
        st.text(f"  • {col.get('name', 'N/A')} ({col.get('order', 'asc')})")


def render_adf_derived_column(transform):
    """Renderiza Derived Column de ADF"""
    st.markdown("**Columns:**")
    columns = props.get('columns', [])
    for col in columns[:5]:
        st.code(f"{col.get('name', 'N/A')} = {col.get('expression', 'N/A')}", language='python')

    if len(columns) > 5:
        st.info(f"... and {len(columns) - 5} more columns")


def render_adf_filter(transform):
    """Renderiza Filter de ADF"""
    st.markdown("**Filter Expression:**")
    st.code(props.get('condition', 'N/A'), language='python')


def show_detailed_comparison(transform_name):
    """Muestra comparación detallada en tabla"""
    pc_data = st.session_state['parsed_data']
    adf_data = st.session_state['adf_data']

    pc_transform = next(
        (t for t in pc_data.transformations if t.name == transform_name),
        None
    )

    adf_transform = next(
        (t for t in adf_data.get('transformations', [])
         if t.get('source_name') == transform_name or t.get('name') == transform_name),
        None
    )

    if not pc_transform or not adf_transform:
        st.error("Cannot compare: transformation not found")
        return

    # Tabla de comparación
    comparison_data = []

    # Tipo
    comparison_data.append({
        "Attribute": "Type",
        "PowerCenter": pc_transform.type,
        "Azure Data Factory": adf_transform.get('type', 'N/A')
    })

    # Nombre
    comparison_data.append({
        "Attribute": "Name",
        "PowerCenter": pc_transform.name,
        "Azure Data Factory": adf_transform.get('name', 'N/A')
    })

    # Atributos específicos según tipo
    pc_type = pc_transform.type
    pc_props = pc_transform.properties

    if pc_type == 'Joiner':
        comparison_data.append({
            "Attribute": "Join Type",
            "PowerCenter": pc_props.get('join_type', 'N/A'),
            "Azure Data Factory": adf_props.get('joinType', 'N/A')
        })

    elif pc_type == 'Aggregator':
        comparison_data.append({
            "Attribute": "Group By Fields",
            "PowerCenter": ", ".join(pc_props.get('group_by', [])) or "N/A",
            "Azure Data Factory": ", ".join(adf_props.get('groupBy', [])) or "N/A"
        })
        comparison_data.append({
            "Attribute": "Aggregation Count",
            "PowerCenter": str(len(pc_props.get('aggregates', []))),
            "Azure Data Factory": str(len(adf_props.get('aggregates', [])))
        })

    elif pc_type == 'Lookup':
        comparison_data.append({
            "Attribute": "Lookup Source",
            "PowerCenter": pc_props.get('lookup_table', 'N/A'),
            "Azure Data Factory": adf_props.get('lookupDataset', 'N/A')
        })

    # Mostrar tabla
    st.table(comparison_data)

    # Equivalencia visual
    st.success(f"✅ **Mapping:** `{pc_transform.type}` → `{adf_transform.get('type', 'N/A')}`")


def render_flow_diagram():
    """Renderiza diagrama de flujo completo"""
    st.markdown("### Data Flow Architecture")

    pc_data = st.session_state['parsed_data']

    # Generar Mermaid diagram
    mermaid_code = generate_mermaid_diagram(pc_data)

    st.code(mermaid_code, language='mermaid')

    st.info("💡 Copy this Mermaid code to visualize in tools like mermaid.live")


def generate_mermaid_diagram(pc_data):
    """Genera código Mermaid para diagrama de flujo"""
    lines = ["graph LR"]

    # Sources
    for source in pc_data.sources:
        src_name = source.name.replace(' ', '_')
        lines.append(f"    SRC_{src_name}[📥 {source.name}]")

    # Transformations
    for transform in pc_data.transformations:
        trn_name = transform.name.replace(' ', '_')
        icon = get_transform_icon(transform.type)
        lines.append(f"    TRN_{trn_name}[{icon} {transform.name}]")

    # Targets
    for target in pc_data.targets:
        tgt_name = target.name.replace(' ', '_')
        lines.append(f"    TGT_{tgt_name}[📤 {target.name}]")

    # Connectors
    for connector in pc_data.connectors:
        from_inst = connector.from_instance.replace(' ', '_')
        to_inst = connector.to_instance.replace(' ', '_')
        if from_inst and to_inst:
            lines.append(f"    {from_inst} --> {to_inst}")

    return "\n".join(lines)


def get_transform_icon(transform_type):
    """Retorna icono para cada tipo de transformación"""
    icons = {
        'Source Qualifier': '🔍',
        'Expression': '📝',
        'Filter': '🔎',
        'Joiner': '🔗',
        'Aggregator': '📊',
        'Lookup': '🔍',
        'Router': '🔀',
        'Sorter': '⬆️',
        'Update Strategy': '♻️',
        'Union': '🔀',
        'Rank': '🏆'
    }
    return icons.get(transform_type, '⚙️')
