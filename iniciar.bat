@echo off
chcp 65001 >nul
title RPA Panel Cliente

echo ============================================
echo  RPA Panel Cliente - Iniciando...
echo ============================================
echo.

:: Verificar Python
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python no está instalado o no está en el PATH.
    echo Por favor instala Python 3.11 o superior desde https://www.python.org
    pause
    exit /b 1
)

:: Crear entorno virtual si no existe
if not exist ".venv\Scripts\activate.bat" (
    echo [1/3] Creando entorno virtual...
    python -m venv .venv
    if errorlevel 1 (
        echo [ERROR] No se pudo crear el entorno virtual.
        pause
        exit /b 1
    )
)

:: Activar entorno virtual
call .venv\Scripts\activate.bat

:: Instalar/actualizar dependencias
echo [2/3] Instalando dependencias...
pip install -r requirements.txt --quiet --disable-pip-version-check
if errorlevel 1 (
    echo [ERROR] No se pudieron instalar las dependencias.
    pause
    exit /b 1
)

:: Instalar Playwright Chromium si no está (idempotente: lo salta si ya existe)
echo [3/3] Verificando navegador Chromium...
python -m playwright install chromium
if errorlevel 1 (
    echo [ERROR] No se pudo instalar Chromium.
    pause
    exit /b 1
)

echo.
echo ============================================
echo  Iniciando aplicacion...
echo ============================================
echo.

python main.py

if errorlevel 1 (
    echo.
    echo [ERROR] La aplicacion cerro con un error. Revisa los logs.
    pause
)
