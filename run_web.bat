@echo off
REM Script para iniciar PowerCenter to ADF Migrator Web Interface v2.5
REM Para Windows

echo ===============================================================
echo  PowerCenter to Azure Data Factory Migrator v2.5
echo  Web Interface Launcher
echo ===============================================================
echo.

REM Verificar que Python esté instalado
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python no esta instalado o no esta en el PATH
    echo Por favor instala Python 3.8 o superior desde python.org
    pause
    exit /b 1
)

echo [OK] Python detectado

REM Verificar que Streamlit esté instalado
python -c "import streamlit" >nul 2>&1
if errorlevel 1 (
    echo.
    echo WARNING: Streamlit no esta instalado
    echo Instalando dependencias...
    echo.

    pip install -r requirements.txt
    pip install -r requirements-streamlit.txt

    if errorlevel 1 (
        echo ERROR: No se pudieron instalar las dependencias
        pause
        exit /b 1
    )
)

echo [OK] Dependencias instaladas
echo.
echo ===============================================================
echo  Iniciando aplicacion web...
echo  La aplicacion se abrira automaticamente en tu navegador
echo  URL: http://localhost:8501
echo ===============================================================
echo.
echo Presiona Ctrl+C para detener el servidor
echo.

REM Iniciar Streamlit
streamlit run app.py

pause
