@echo off
chcp 65001 >nul
title Programar RPA Panel Cliente

echo ============================================
echo  Programar ejecucion automatica semanal
echo ============================================
echo.

set /p DIA="Dia de la semana (MONDAY/TUESDAY/.../FRIDAY) [MONDAY]: "
if "%DIA%"=="" set DIA=MONDAY

set /p HORA="Hora de inicio (HH:MM, formato 24h) [07:00]: "
if "%HORA%"=="" set HORA=07:00

set TASK_NAME=RPA_Panel_Cliente_Semanal
set SCRIPT_PATH=%~dp0iniciar.bat

echo.
echo Registrando tarea: %TASK_NAME%
echo Dia: %DIA% a las %HORA%
echo Script: %SCRIPT_PATH%
echo.

schtasks /create /tn "%TASK_NAME%" /tr "\"%SCRIPT_PATH%\"" /sc WEEKLY /d %DIA% /st %HORA% /f
if errorlevel 1 (
    echo [ERROR] No se pudo registrar la tarea. Ejecuta como Administrador.
    pause
    exit /b 1
)

echo.
echo [OK] Tarea programada correctamente.
echo Para ver la tarea: schtasks /query /tn "%TASK_NAME%"
echo Para eliminarla:   schtasks /delete /tn "%TASK_NAME%" /f
echo.
pause
