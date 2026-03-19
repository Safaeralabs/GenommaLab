# RPA Panel Cliente

## Inicio rápido

1. Instala [Python 3.11 o superior](https://www.python.org/downloads/) (marca la opción **"Add Python to PATH"** durante la instalación)
2. Copia `data/providers.example.json` a `data/providers.json` y rellena las credenciales reales
3. Haz doble clic en **`iniciar.bat`**

El script se encarga automáticamente de crear el entorno virtual, instalar dependencias e instalar el navegador Chromium (solo la primera vez). Las siguientes veces arranca directamente.

---

Aplicación de escritorio en Python para ejecutar un robot de descargas desde portales web de proveedores. La solución usa `Tkinter` para la interfaz, `Playwright` para la automatización, `openpyxl` para leer Excel y una arquitectura modular preparada para sumar más portales.

## Características

- La lista de proveedores ya viene empaquetada como `providers.json` y se copia automáticamente a `%LOCALAPPDATA%\RPA Panel Cliente\data\` al arrancar; al cambiar `PROVIDERS_SOURCE` puedes alternar entre esa fuente y tu propio Excel.
- La interfaz no se congela durante la ejecución: el robot se ejecuta en un hilo separado.
- Se muestran logs en pantalla y se escriben en `logs\rpa_panel.log`.
- Barra de progreso por proveedor y botón `Detener` para cancelar antes de empezar el siguiente proveedor.
- Descargas organizadas por proveedor/fecha y consolidadas en una carpeta final.
- Humor robusto de errores: screenshots y mensajes en caso de fallos.
- Preparada para empaquetar con PyInstaller e incluir el runtime de Playwright.

## Estructura del proyecto

```text
rpa_panel_cliente/
├── app/
│   ├── config/
│   ├── core/
│   ├── portals/
│   ├── ui/
│   └── utils/
├── data/
├── downloads/
├── installer/
├── logs/
├── screenshots/
├── main.py
├── requirements.txt
├── rpa_panel_cliente.spec
└── README.md
```

## Requisitos

- Python 3.11 o superior
- Windows (recomendado para el empaquetado final)

## Instalación de desarrollo

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m playwright install chromium
```

## Ejecutar la aplicación

```powershell
python main.py
```

## Formato del catálogo JSON

La aplicación consume por defecto `providers.json`, que se empaqueta con el ejecutable y se copia a `%LOCALAPPDATA%\RPA Panel Cliente\data\providers.json` al iniciarse. Si necesitas ajustar los proveedores solo hay que editar ese JSON con objetos como:

```json
{
  "proveedor": "Ejemplo",
  "activo": true,
  "portal_tipo": "abako",
  "login_url": "...",
  "usuario": "user",
  "password": "pass",
  "carpeta": "Carpeta",
  "sede_subportal": "Sede",
  "tipo_acceso": "directo"
}
```

Si prefieres usar un Excel propio, fija la variable de entorno `PROVIDERS_SOURCE=excel`, guarda tu `Accesob2b.xlsx` en `%LOCALAPPDATA%\RPA Panel Cliente\data\Accesob2b.xlsx` y la app leerá la hoja `proveedores`. El binario ya incluye el `ExcelReader`, pero el instalador ya no entrega automáticamente el archivo; así el Excel se puede versionar y repartir de forma independiente.

El archivo debe mantener las columnas `proveedor`, `activo`, `portal_tipo`, `login_url`, `usuario`, `password`, `fecha_desde`, `fecha_hasta` y `carpeta` (en cualquier orden, en la fila de encabezado). La app también soporta el layout original de la hoja `01_Accesos_Estructurados` y sólo procesa las filas marcadas como activas.

Si esa hoja no existe, se lee la hoja `01_Accesos_Estructurados` con el layout original de Accesob2b. Solo se procesan las filas marcadas como activas (`activo` = `true`, `si`, `1`, `x`, etc.).

## Fuente alternativa: catálogo JSON

Si no quieres depender del Excel de proveedores, puedes fijar la variable de entorno `PROVIDERS_SOURCE=catalog`. En ese caso el panel:

- ignora el Excel y carga `%LOCALAPPDATA%\RPA Panel Cliente\data\providers.json`, que se copia automáticamente desde el instalador (también está incluido en el repositorio).
- si ese archivo no existe o no tiene datos, cae en el catálogo por defecto definido en `app/config/provider_catalog.py`.
- para adaptar la lista bastan unos pocos campos: `proveedor`, `portal_tipo`, `login_url`, `usuario`, `password`, `carpeta`, y puedes añadir `sede_subportal`, `requiere_revision`, `notas_operativas`, `conflictos_detectados`, `fuente` o `tipo_acceso`.

El JSON se distribuye con el `.exe`, así que el cliente solo debe editar `providers.json` (o crear uno nuevo en `%LOCALAPPDATA%\RPA Panel Cliente\data\`) si necesita cambiar la lista de proveedores sin tocar Excel.

## Rutas en equipo cliente

Cuando la app se instala en otro ordenador, crea las siguientes carpetas dentro de `%LOCALAPPDATA%\RPA Panel Cliente\` para evitar problemas de permisos:

```text
data\
downloads\
postprocesado\
logs\
screenshots\
```

## Selección de semana ISO

La interfaz incluye un desplegable con las semanas ISO del año (1-53) que se inicializa en la semana vigente. Debajo aparece el rango de fechas para esa semana (por ejemplo, “Semana 9: 23 Feb 2026 - 01 Mar 2026”). Esta selección determina las fechas que se pasan automáticamente a cada portal para configurar filtros de periodo.

Además, puedes filtrar qué portales ejecutar (por ejemplo: `Abako`, `EOS Consultores`, `Xeon`, `Soluciones Practicas` o `Todos`). La lista sale del valor `Portal` del Excel; si eliges uno distinto de “Todos”, se procesan únicamente las filas que coincidan.

## Control de ejecución

El botón `Ejecutar robot` arranca la orquestación en un hilo y habilita el botón `Detener`. Si se pulsa `Detener`, el orquestador marca una señal que evita iniciar al siguiente proveedor (el portal en ejecución termina la operación que tenga en curso).

## Postprocesamiento

Tras una descarga correcta, la app:

- conserva los archivos originales en `downloads\`
- copia el material a `postprocesado\<proveedor>\<timestamp>\`
- clasifica los ficheros por tipo (`ventas`, `inventario`, `otros`)
- genera un `manifest.json` para cada batch
- consolida la información en el archivo de homologación (plantilla `Homologacion.xlsx`)

### Archivo Homologaciones consolidado

Al final del proceso se genera un único Excel dentro de `%LOCALAPPDATA%\RPA Panel Cliente\postprocesado\` llamado `Homologaciones_SemanaXX_YYYYMMDD_HHMMSS.xlsx` (por ejemplo `Homologaciones_Semana09_20260317_145201.xlsx`). Ese archivo parte de la plantilla `Homologacion.xlsx` del repositorio y mantiene las columnas `año`, `Semana`, `Tipo`, `Fecha_Stock`, `Cadena`, `Cod_Prod`, `Descripcion_prod`, `Cod_Local`, `Descripcion_Local`, `Unidades`, `Zonalocal`. Las filas de ventas llevan `Tipo = SO`, las de inventario `Tipo = INV`, y la columna `Fecha_Stock` se rellena con la fecha de inicio de la semana seleccionada. Las columnas de local/zona se nutren de `carpeta` y `sede_subportal` del Excel de proveedores.

La plantilla `Homologacion.xlsx` permanece en la raíz del proyecto y se reutiliza cada ejecución para garantizar el mismo formato sin tocarla manualmente.

## Sincronización con OneDrive

Si el usuario tiene OneDrive en su equipo, la app detecta automáticamente las variables de entorno `ONEDRIVE`, `OneDriveCommercial` o `OneDriveConsumer` y copia cada descarga organizada (`postprocesado\<proveedor>\<timestamp>\`) y el Excel `Homologaciones_Semana...` dentro de una carpeta `RPA Panel Cliente` en OneDrive. De este modo las descargas quedan disponibles en la nube sin pasos manuales y el botón “Abrir OneDrive” de la UI abre directamente esa carpeta sincronizada.

Si necesitas apuntar a otra carpeta, configura la variable `ONEDRIVE` con la ruta deseada: la app creará automáticamente `RPA Panel Cliente` dentro de la ruta indicada y la mostrará en pantalla. De lo contrario, la sincronización queda desactivada y el botón aparece deshabilitado con un mensaje para definir la variable.

## Dependencias de Windows

El ejecutable requiere el Redistribuible de Visual C++ 2015-2022. Si aparece el error “Failed to load Python DLL … python314.dll” al iniciar el panel, instala `vc_redist.x64.exe` desde la página oficial de Microsoft (https://learn.microsoft.com/en-US/cpp/windows/latest-supported-vc-redist) y vuelve a ejecutar el `.exe`. Este componente asegura que `python314.dll` y sus dependencias (`VCRUNTIME140.dll`, etc.) estén disponibles en el sistema.

## PortalA y PortalB

### PortalA

Ejemplo operativo basado en Abako:

- abre el `login_url`, rellena usuario/contraseña y espera el dashboard
- navega hasta “Ventas Netas BI”, abre el modal de filtros y selecciona campos/fechas
- aplica filtro de proveedor escribiendo “genomma” y, si no aparece, prueba a recortar el término hasta llegar a coincidencias como “genom”
- exporta Excel usando `expect_download` y guarda las descargas en `downloads\`
- toma screenshots de éxito o error para trazabilidad

### PortalB

Clase placeholder lista para extender la lógica de otros portales (EOS Consultores, Xeon, Soluciones Practicas, etc.). Registra la implementación en `app/core/orchestrator.py` cuando esté lista.

## Empaquetado para entregar a otro cliente

### Empaquetado para cliente

Usa el script `scripts/build_windows.ps1` para obtener un `.exe`/instalador que ya contiene todo:

1. `pyinstaller` y sus dependencias se instalan automáticamente.
2. Chromium de Playwright se descarga en `build_assets\ms-playwright`.
3. `data\providers.json` y `Homologacion.xlsx` se incorporan al ejecutable como recursos (sin Excel).
4. Se genera `rpa_panel_cliente.exe` listo para distribuir.
5. Si Inno Setup está disponible, también se genera el instalador final.

#### Comando

```powershell
cd C:\ruta\al\proyecto\rpa_panel_cliente
.\scripts\build_windows.ps1
```

#### Resultado

- Ejecutable distribuible: `dist\rpa_panel_cliente\`
- Instalador final: `output\Instalador_RPA_Panel_Cliente.exe` (si Inno Setup está presente)

Si Inno Setup no está presente en la máquina de empaquetado, añade `-SkipInstaller` para generar solo el binario.

## Instalar Inno Setup

Instala Inno Setup 6 en la máquina de build. El script busca automáticamente `ISCC.exe` en:

- `C:\Program Files (x86)\Inno Setup 6\ISCC.exe`
- `C:\Program Files\Inno Setup 6\ISCC.exe`

## Qué incluye el instalador

- `rpa_panel_cliente.exe`
- dependencias Python empaquetadas
- recursos del proyecto
- Chromium de Playwright empaquetado
- acceso directo opcional en el escritorio

## Siguientes pasos

- Adaptar `app/portals/portal_a.py` a un portal real: reemplaza selectores placeholder, valida las pantallas después del login y confirma los pasos reales de descarga.
- Agregar `app/portals/portal_b.py`: crea un nuevo portal concreto basado en `BasePortal` y regístralo en el orquestador para cubrir clientes adicionales (EOS Consultores, Xeon, etc.).
- Pasar credenciales a un método más seguro: mueve usuario/contraseña fuera del Excel y utiliza variables de entorno, el Gestor de Credenciales de Windows, un vault corporativo o un archivo cifrado con control de acceso.
