@echo off
chcp 65001 >nul
title Crear acceso directo RPA Panel Cliente

set SCRIPT_DIR=%~dp0
set SCRIPT_DIR=%SCRIPT_DIR:~0,-1%
set TARGET=%SCRIPT_DIR%\iniciar.bat
set SHORTCUT=%USERPROFILE%\Desktop\RPA Panel Cliente.lnk
set ICON=%SystemRoot%\System32\shell32.dll,162

powershell -NoProfile -Command ^
  "$ws = New-Object -ComObject WScript.Shell;" ^
  "$s = $ws.CreateShortcut('%SHORTCUT%');" ^
  "$s.TargetPath = '%TARGET%';" ^
  "$s.WorkingDirectory = '%SCRIPT_DIR%';" ^
  "$s.IconLocation = '%ICON%';" ^
  "$s.WindowStyle = 1;" ^
  "$s.Description = 'RPA Panel Cliente - Genomma Lab';" ^
  "$s.Save();"

if exist "%SHORTCUT%" (
    echo.
    echo [OK] Acceso directo creado en el escritorio.
    echo      Haz doble clic en "RPA Panel Cliente" para iniciar.
    echo.
) else (
    echo.
    echo [ERROR] No se pudo crear el acceso directo.
    echo.
)
pause
