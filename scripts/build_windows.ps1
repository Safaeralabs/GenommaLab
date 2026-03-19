[CmdletBinding()]
param(
    [switch]$SkipInstaller
)

$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
$venvPython = Join-Path $projectRoot ".venv\Scripts\python.exe"
$buildAssetsRoot = Join-Path $projectRoot "build_assets"
$browserCache = Join-Path $buildAssetsRoot "ms-playwright"
$specFile = Join-Path $projectRoot "rpa_panel_cliente.spec"
$installerScript = Join-Path $projectRoot "installer\rpa_panel_cliente.iss"
$distDir = Join-Path $projectRoot "dist"
$buildDir = Join-Path $projectRoot "build"

if (-not (Test-Path $venvPython)) {
    throw "No existe el entorno virtual en $venvPython"
}

New-Item -ItemType Directory -Force -Path $buildAssetsRoot | Out-Null
New-Item -ItemType Directory -Force -Path $browserCache | Out-Null

Write-Host "Instalando dependencias de empaquetado..."
& $venvPython -m pip install pyinstaller

Write-Host "Descargando Chromium de Playwright en build_assets..."
$env:PLAYWRIGHT_BROWSERS_PATH = $browserCache
& $venvPython -m playwright install chromium

Write-Host "Limpiando build anterior..."
if (Test-Path $distDir) {
    try {
        Remove-Item -Recurse -Force $distDir -ErrorAction Stop
    } catch {
        Write-Warning "No se pudo eliminar dist (archivos en uso). Se proseguirá."
    }
}
if (Test-Path $buildDir) {
    try {
        Remove-Item -Recurse -Force $buildDir -ErrorAction Stop
    } catch {
        Write-Warning "No se pudo eliminar build (archivos en uso). Se proseguirá."
    }
}

Write-Host "Compilando ejecutable con PyInstaller..."
& $venvPython -m PyInstaller --noconfirm $specFile

if ($SkipInstaller) {
    Write-Host "Build completado sin instalador."
    exit 0
}

$isccCandidates = @(
    "${env:ProgramFiles(x86)}\Inno Setup 6\ISCC.exe",
    "${env:ProgramFiles}\Inno Setup 6\ISCC.exe"
)

$isccPath = $isccCandidates | Where-Object { Test-Path $_ } | Select-Object -First 1

if (-not $isccPath) {
    Write-Warning "No se encontro Inno Setup. El ejecutable esta listo en dist\\rpa_panel_cliente."
    exit 0
}

Write-Host "Generando instalador con Inno Setup..."
& $isccPath $installerScript

Write-Host "Instalador generado en output\\"
