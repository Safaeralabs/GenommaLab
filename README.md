# RPA Panel Cliente

Aplicación de escritorio para automatizar la descarga de reportes de ventas e inventarios desde portales B2B de distribuidores.

## Inicio rápido

1. Instalar [Python 3.11+](https://www.python.org/downloads/) (marcar **"Add Python to PATH"**)
2. Copiar `data/providers.example.json` → `data/providers.json` y completar credenciales
3. Doble clic en `iniciar.bat`

El bat crea el entorno virtual, instala dependencias y descarga Chromium automáticamente en el primer arranque.

---

## Stack

- **UI**: Tkinter
- **Automatización web**: Playwright (Chromium)
- **Excel**: openpyxl
- **Paralelismo**: `concurrent.futures.ThreadPoolExecutor`

## Estructura

```
app/
  config/     configuración global y catálogo de proveedores
  core/       orquestador, modelos, postprocesado, homologación
  portals/    implementación por portal (uno por proveedor)
  ui/         ventana principal
  utils/      sincronización OneDrive, validación, notificaciones
data/
  providers.example.json
main.py
requirements.txt
```

## Instalación para desarrollo

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m playwright install chromium
python main.py
```

## Proveedores soportados

| Portal | Clase |
|---|---|
| Abako | `PortalA` |
| Xeon / TAT / Mensuli | `PortalXeon` |
| EOS Consultores | `PortalEOS` |
| Soluciones Prácticas | `PortalProvecol` |

Para añadir un portal nuevo: crear clase en `app/portals/` heredando `BasePortal` y registrarla en `portal_registry` del orquestador.

## Configuración

La fuente de proveedores se controla con `PROVIDERS_SOURCE`:

- `catalog` (default): lee `%LOCALAPPDATA%\RPA Panel Cliente\data\providers.json`
- `excel`: lee `Accesob2b.xlsx` de la misma carpeta (hoja `proveedores`)

Variables de entorno opcionales:

| Variable | Default | Descripción |
|---|---|---|
| `RPA_HEADLESS` | `0` | `1` para ejecutar sin ventana de browser |
| `RPA_MAX_WORKERS` | `4` | Workers paralelos |
| `RPA_MAX_RETRIES` | `2` | Reintentos por proveedor |
| `RPA_BROWSER_CHANNEL` | Chromium embebido | `msedge` o `chrome` para usar el del sistema |

## Flujo de ejecución

1. Se cargan los proveedores activos del catálogo o Excel
2. Se descargan en paralelo (N workers configurable)
3. En caso de fallo se reintenta con URL alternativa si existe
4. Los archivos se organizan en `postprocesado/` y se sincronizan a OneDrive
5. Se genera un Excel consolidado de homologación al final

## Empaquetado

```powershell
.\scripts\build_windows.ps1
```

Genera `dist/rpa_panel_cliente.exe` con Chromium incluido. Si Inno Setup está instalado, también produce el instalador en `output/`.
