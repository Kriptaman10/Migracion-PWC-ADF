#!/bin/bash
# Script para iniciar PowerCenter to ADF Migrator Web Interface v2.5
# Para Linux/Mac

echo "==============================================================="
echo "  PowerCenter to Azure Data Factory Migrator v2.5"
echo "  Web Interface Launcher"
echo "==============================================================="
echo ""

# Verificar que Python esté instalado
if ! command -v python3 &> /dev/null; then
    echo "ERROR: Python3 no está instalado"
    echo "Por favor instala Python 3.8 o superior"
    exit 1
fi

echo "[OK] Python detectado: $(python3 --version)"

# Verificar que Streamlit esté instalado
if ! python3 -c "import streamlit" &> /dev/null; then
    echo ""
    echo "WARNING: Streamlit no está instalado"
    echo "Instalando dependencias..."
    echo ""

    pip3 install -r requirements.txt
    pip3 install -r requirements-streamlit.txt

    if [ $? -ne 0 ]; then
        echo "ERROR: No se pudieron instalar las dependencias"
        exit 1
    fi
fi

echo "[OK] Dependencias instaladas"
echo ""
echo "==============================================================="
echo "  Iniciando aplicación web..."
echo "  La aplicación se abrirá automáticamente en tu navegador"
echo "  URL: http://localhost:8501"
echo "==============================================================="
echo ""
echo "Presiona Ctrl+C para detener el servidor"
echo ""

# Iniciar Streamlit
streamlit run app.py
