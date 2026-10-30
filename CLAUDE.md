# ILUS Fitness — Reglas para Claude

Este archivo establece las **reglas no negociables** del proyecto que TODO
agente que toque código debe respetar. Está pensado para Claude pero
sirve a cualquier desarrollador.

---

## 🏷️ REGLA #0 — La empresa SIEMPRE se llama "ILUS Fitness"

**Pedido explícito de Daniel (2026-08-19): "deja como regla que siempre es
Ilus Fitness".**

Todo texto de marca **visible para una persona** dice **ILUS Fitness**:
footers, títulos, `<title>`, `aria-label`, `alt`, asuntos y cuerpos de
correo, PDFs, actas, etiquetas impresas, mensajes de WhatsApp/SMS.

El nombre **"ILUS Sport & Health" queda fuera de uso** en texto nuevo, y se
corrige donde aparezca al pasar por ese código.

### ⚠️ La MARCA no es la RAZÓN SOCIAL — no confundirlas nunca

La empresa tiene **dos nombres, los dos correctos**, cada uno con su lugar
(ya declarados en `app.py` ~83928, desde 2026-05-30):

| Concepto | Valor | Constante | Dónde va |
|---|---|---|---|
| Marca comercial | **ILUS Fitness** | `ILUS_BRAND` | Todo lo que ve una persona: UI, correos, etiquetas, footers |
| Razón social (entidad legal) | **Sport and Health Solutions SPA** | `ILUS_LEGAL` | Solo documentos legales/tributarios |
| RUT | 76.996.964-0 | `ILUS_RUT` | Junto a la razón social |

**"Sport and Health Solutions SPA" NO se reemplaza por "ILUS Fitness"** en
documentos con efecto legal o contable: cotizaciones PDF, datos de
transferencia bancaria, packing lists, actas. Ahí la razón social es
obligatoria y cambiarla es un error grave — el cliente transferiría a un
nombre que no coincide con la cuenta.

Lo que la REGLA elimina es el híbrido **"ILUS Sport & Health"**, que no es
ni la marca ni la razón social: es un nombre viejo que quedó suelto.

### Qué NO se toca (no es marca, es identificador técnico)

Cambiar cualquiera de estos rompe cosas en producción:

- El dominio de correo **`@sphs.cl`** (la cuenta real es
  `daniel.aguilar@sphs.cl`, y `contacto@sphs.cl` en documentos) y cualquier
  `sphs` en DSN, config o credenciales.
- Nombres de variables, claves de entorno, rutas, endpoints, nombres de archivo.
- Razones sociales de CLIENTES que vienen del ERP Random (ahí manda Random,
  no ILUS — REGLA #4.1). Ojo: existe un cliente real llamado "SPORT AND
  HEALTH SOLUTIONS SPA" en los datos históricos; ese dato no se toca.

### Cómo escribirlo bien

Preferir las constantes a escribir el nombre a mano:

```python
ILUS_BRAND                     # → "ILUS Fitness"   (marca, para el usuario)
ILUS_LEGAL                     # → "Sport and Health Solutions SPA" (legal)
brand = _get_brand_cfg()       # brand["name"] → "ILUS Fitness" (REGLA #11)
```

---

## 🎨 REGLA #1 — UI/UX: prohibido usar `alert()`, `confirm()`, `prompt()` nativos

Los popups grises del navegador (`web-production-XXX.up.railway.app dice...`)
están **prohibidos** en código nuevo del proyecto ILUS. Rompen la coherencia
visual y son una mala experiencia de usuario.

### Helpers disponibles (`static/ilus_ui.js`)

| Nativo prohibido            | Reemplazo ILUS                 | Tipo retorno                  |
|-----------------------------|--------------------------------|-------------------------------|
| `alert('msg')`              | `ilusAlert({title,message})`   | `Promise<true>` (await opcional) |
| `confirm('msg')`            | `await ilusConfirm({...})`     | `Promise<boolean>`            |
| `prompt('msg')`             | `await ilusPrompt({...})`      | `Promise<string\|null>`       |
| Mensajes efímeros (toasts)  | `ilusToast('msg', {type})`     | `void` (auto-dismiss en 3.5s) |

### Ejemplos correctos

```javascript
// ❌ MAL
if (!confirm('¿Eliminar esto?')) return;

// ✅ BIEN
const ok = await ilusConfirm({
  title: 'Eliminar registro',
  message: '¿Quitar este item permanentemente?',
  sub: 'Esta acción no se puede deshacer.',
  okLabel: 'Eliminar', cancelLabel: 'Cancelar',
  danger: true,   // botón en rojo
});
if (!ok) return;
```

```javascript
// ❌ MAL
const nombre = prompt('Ingresa tu nombre:');

// ✅ BIEN
const nombre = await ilusPrompt({
  title: 'Tu nombre',
  message: 'Ingresa tu nombre completo',
  placeholder: 'Ej: Juan Pérez',
  required: true,
});
if (!nombre) return; // null = canceló
```

```javascript
// ❌ MAL
alert('Guardado exitosamente');

// ✅ BIEN (mensaje breve no bloqueante)
ilusToast('✓ Guardado exitosamente', { type: 'success' });

// ✅ BIEN (mensaje importante con OK explícito)
await ilusAlert({
  title: 'Operación completada',
  message: 'El cliente fue creado con id #' + d.id,
  type: 'success',
});
```

### Tipos disponibles

`info` · `success` · `warning` · `error` · `danger` · `question`

Cada uno aplica color e icono apropiado al modal.

### HTML en el `sub` o `message`

Por seguridad, el contenido se escapa por default. Si necesitas HTML
literal (solo strings controlados, NUNCA input del usuario sin sanitizar):

```javascript
await ilusConfirm({
  title: 'Confirmar',
  message: 'El número es:',
  sub: '<strong style="color:#dc2626">VS-77</strong>',
  subHtml: true,   // flag explícita
});
```

### Shim global automático

`window.alert()` está interceptado por un shim global en `ilus_ui.js`
(líneas finales) que lo enruta a `ilusToast` o `ilusAlert` según el
tamaño del mensaje. Esto hace que el código LEGACY siga viéndose
correcto sin tocar 30+ templates uno por uno.

**No interceptamos `confirm` y `prompt` porque son síncronos y la versión
ILUS es async — un reemplazo silencioso rompería los callers.**

Por eso en código NUEVO: usa SIEMPRE las versiones `ilus*` directamente.

---

## 🎨 REGLA #2 — Paleta de colores ILUS

```css
--ilus-red:    #dc2626   /* primario, accent, CTA */
--ilus-black:  #0a0a0a   /* fondos oscuros, sidebar */
--ilus-white:  #ffffff   /* fondos claros, cards */
```

Apoyo:
- Verde éxito: `#16a34a` / fondo `#dcfce7`
- Ámbar advertencia: `#f59e0b` / fondo `#fff8e1`
- Rojo peligro: `#dc2626` / fondo `#fee2e2`
- Azul info: `#3b82f6` / fondo `#dbeafe`
- Gris neutro: `#6b7280` / fondo `#f3f4f6`

---

## 🎨 REGLA #3 — Mobile-first

`static/mobile.css` (cargado en `base.html` después de `style.css`)
aplica las correcciones móviles globalmente. Respetar:

- Inputs deben tener `font-size: 16px` en mobile (anti auto-zoom iOS) — ya manejado por el CSS global pero verifica al agregar estilos custom
- Botones touch: `min-height: 44px` (Apple HIG)
- Modales fullscreen en mobile (height: 100dvh)
- Safe-area-insets para iPhones con notch

---

## 🔐 REGLA #4 — Seguridad

- **JAMÁS** hardcodear credenciales en el código. Usar variables de
  entorno via `config.py` con `_env()` / `_env_first()`.
- **JAMÁS** loguear `params` completos en errores SQL (pueden contener
  RUTs, tokens, datos personales). Sanitizar antes de imprimir.
- **JAMÁS** ejecutar SQL con f-strings concatenando input del usuario.
  Usar SIEMPRE `%s` con tupla de params.
- **JAMÁS** retornar `400/500` con detalles internos al cliente.
  Usar mensajes amigables + log detallado en backend.

---

## 🚫 REGLA #4.1 — ERP Random es **READ-ONLY ABSOLUTO** (no negociable)

**El ERP Random (cloud.random.cl:8058 SQL Server + REST API) es la
fuente de verdad de la empresa. ILUS Fitness JAMÁS modifica
sus tablas. Solo consulta.**

Esta regla NO admite excepciones de ningún tipo. Ni siquiera "para
arreglar un dato malo". Ni siquiera "es solo un test rápido". Ni
siquiera "vamos a poner un autocommit y hacerlo solo esta vez".

### Cómo está garantizado en el código (4 capas)

Toda consulta al ERP DEBE pasar por `_random_sql_query()` /
`_random_sql_one()` en `app.py` (líneas 1150-1288). Estas funciones
implementan:

| Capa | Mecanismo | Qué bloquea |
|------|-----------|-------------|
| 1 | WHITELIST | Solo `SELECT` o `WITH` (CTE) como primer token |
| 2 | BLACKLIST | 28+ tokens prohibidos: `INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER`, `TRUNCATE`, `EXEC`, `EXECUTE`, `MERGE`, `GRANT`, `REVOKE`, `CREATE`, `BACKUP`, `RESTORE`, `SHUTDOWN`, `OPENROWSET`, `OPENQUERY`, `BULK`, `DBCC`, `KILL`, `RECONFIGURE`, `INTO `, `; `, `/*`, `*/`, `XP_CMDSHELL`, `SP_CONFIGURE`, `SP_EXECUTESQL` |
| 3 | PARAMETRIZACIÓN | `pymssql` con `%s` (nunca f-strings). SQL injection imposible. |
| 4 | AUTOCOMMIT OFF | `autocommit=False` en el pool. **`conn.commit()` NUNCA se llama** en `_random_sql_query`. Cualquier escritura que se cuele se descarta al cerrar la conexión. |

### REST API (motor `erp_engine.py`)

- Solo métodos `fetch_*` (`fetch_document`, `fetch_entity`, etc.).
- HTTP único método: **GET** (`urllib.request.Request` sin method).
- No existen métodos POST/PUT/DELETE/PATCH ni intentos de los mismos.

### Qué hacer si crees necesitar modificar el ERP

**No lo hagas.** En su lugar:
1. Detente y avisa a Daniel ANTES de tocar nada.
2. Si el dato realmente está mal en el ERP, eso se corrige desde
   Random (Joaquín / Raúl), no desde ILUS.
3. ILUS guarda sus PROPIAS tablas (`pickup_*`, `mant_*`, `transp_*`,
   etc.) en MySQL Clever Cloud. Esas SÍ se modifican. El ERP NO.

### Cómo verificar que no se viola

```bash
# Solo debe aparecer una importación de pymssql, dentro de _random_sql_pool()
grep -rn "pymssql" --include="*.py"

# Solo debe haber GET hacia la REST API de Random
grep -rn "requests\.(post|put|delete|patch)" --include="*.py"
```

Si algún día agregás código que toca el ERP Random fuera de
`_random_sql_query`/`_random_sql_one`/`erp_engine.fetch_*`, lo estás
haciendo MAL. Revertí y usá los helpers.

---

## 🛑 REGLA #4.2 — PROHIBIDO eliminar features sin permiso explícito (no negociable)

**NUNCA borres, ocultes, comentes ni "simplifiques quitando" código,
botones, links de menú, columnas, toggles, módulos o cualquier
funcionalidad que YA EXISTE y funciona — a menos que Daniel lo pida
explícitamente en ese mensaje.**

Esto incluye:
- Quitar un link del sidebar, una columna de una tabla, un toggle, un botón.
- "Limpiar" o refactorizar eliminando algo que parecía no usarse.
- Reemplazar una sección por otra "mejor" descartando la anterior.

### Por qué

Daniel construyó cada feature por una razón operativa. Borrar algo que
"parece de más" rompe flujos reales (ej: el Radar lo usa otra persona, el
Plan Anual le avisa qué agendar). Lo que para el agente es ruido, para el
negocio es una herramienta en uso.

### Qué hacer en su lugar

1. Si crees que algo sobra o estorba para tu tarea → **pregunta antes**.
2. Si una tarea EXIGE remover algo → confírmalo en el mismo mensaje:
   "para hacer X tengo que quitar Y, ¿lo confirmas?".
3. Si vas a mover/renombrar algo → avísalo, no lo hagas silenciosamente.
4. Si encuentras código muerto real → propónlo, no lo borres de una.

### Regla de oro

**Agregar y mejorar: sí, siempre. Quitar: solo con "sí" explícito de Daniel.**
Ante la duda, se conserva. Es más barato dejar algo de más que perder
una herramienta en uso y la confianza.

---

## 📊 REGLA #4.3 — TODA tabla se pagina como Etiquetas (contenida en pantalla, sin scroll)

**Pedido explícito de Daniel (2026-08-01): "hagámosla igual que las
etiquetas… contenida en la página, no necesitamos darle scroll. Deja eso
como regla en el proyecto. Toda tabla debe manejarse bajo esta
estructura."**

El patrón de referencia es la tabla de **Etiquetas** (`/`, productos). Toda
tabla nueva o existente del proyecto debe seguirlo:

### Qué exige el patrón

1. **Paginación real, no scroll infinito ni `LIMIT` mudo.** Pie de tabla
   con: `Mostrando 1–100 de 1362`, selector **N por página**, botones
   **Anterior / Página X de Y / Siguiente**.
2. **El contenido cabe en la pantalla.** La página NO scrollea para
   recorrer la tabla — se cambia de página. (Ojo con la REGLA #3 mobile:
   en móvil las cards sí fluyen, pero el paginador se mantiene.)
3. **El selector de tamaño de página es del usuario**, no fijo por código.
   Etiquetas usa 100 por defecto.
4. **Al limpiar un filtro, la tabla se recarga.** Bug real reportado por
   Daniel el 2026-08-01: quitaba el filtro y la tabla se quedaba con el
   resultado anterior. Cualquier control que filtre debe re-consultar (o
   re-renderizar) al volver a su estado vacío — no solo al aplicarse.

### Dónde aplica hoy (deuda conocida)

- ✅ Etiquetas / Productos — es la referencia.
- ✅ **Monitor de Transporte** (`/transporte/`) — paginado 2026-08-01.
  Se pagina en Python sobre la lista final (`tr_compromisos_json`), NO con
  `LIMIT/OFFSET` en el SQL: entre la query y la grilla hay un filtro por
  `estado_logistico` (valor derivado en Python) y la expansión por ramo
  (1 fila por `transport_manifest_item`), así que filas ≠ documentos.
  ⚠️ El `LIMIT 500` sigue siendo el techo: con más de 500 documentos en un
  filtro, el paginador cuenta sobre esos 500. Limitación conocida, no
  resuelta.
- ✅ **Manifiestos** (`/transporte/manifiestos`) — ya paginaba server-side;
  2026-08-01 se le alineó el pie de tabla al patrón (Mostrando A–B de N,
  Anterior / Pagina X de Y / Siguiente) y se URL-encodearon los filtros de
  los links de paginación (`estado` trae acentos, `q` es texto libre).
- Cualquier tabla nueva: nace con el patrón, no se agrega después.

---

## 🚫 REGLA #4.4 — CheckWMS (bodega) es **READ-ONLY ABSOLUTO**, mismo candado que el ERP (no negociable)

**Pedido explícito de Daniel (2026-10-02): "dejemos con el mismo candado, que
en Check no se altera nada, siempre siempre siempre solo se consulta".**

CheckWMS (`checkapi-integracion-prod-checkwms.azurewebsites.net`) es el sistema
de bodega. ILUS **solo consulta** — igual que con el ERP Random (REGLA #4.1).
Sin excepciones: ni "para probar", ni "es solo una línea", ni porque el
endpoint "suene" a consulta.

### Cómo está garantizado en el código

- **Puerta única:** toda llamada a Check pasa por `_checkwms_get()` en `app.py`.
  Solo hace `requests.get`. Las credenciales (`CHECKWMS_CONFIG`) no se usan en
  ningún otro lugar.
- **Lista blanca:** `_CHECKWMS_GET_PERMITIDOS` (`app.py`, justo arriba de
  `_checkwms_get`). Una ruta fuera de la lista se rechaza **antes** de salir a
  la red y queda en el log como `[checkwms] BLOQUEADO`. Hoy: `GetReporteStock`,
  `GetStockTrazabilidad`, `GetStockTrazabilidadV2`, `GetSeguimientoDespacho`,
  `GetControlSalida`.
- **Bodega de cada producto en Retiros (2026-10-16):** el modal «Agregar factura o boleta» muestra en qué bodega de Check está cada producto y un resumen del documento (`/retiros/api/check-bodegas`, `retiros_check.bodegas_por_sku`) usando el reporte de stock YA cacheado (`_checkwms_stock_rows`, 1 h): ninguna llamada nueva a Check.
- **Bodega de cada producto en TODOS los formularios (2026-10-24, Daniel: «que este motor sea un motor único»):** el modal compartido «Búsqueda avanzada de productos» (`templates/tickets/_tka_modal.html`: Tickets, Cotizaciones, OT, Transporte, Cubicador…) muestra bajo cada producto sus bodegas de Check y, arriba del documento, «¿Dónde está este pedido según Check?» (`_tkaCargarBodegas`). Lo sirve `GET /api/erp/check-bodegas` (cualquier usuario con sesión, **salvo TODA la familia técnico**: `_oculta_proveedores()`, REGLA #19 —las bodegas son de un operador externo—; el modal oculta el cuadro al recibir el 403, sin un falso «sin verificar»; solo DÓNDE está el stock) y comparte con Retiros la MISMA función de respuesta, `retiros_check.respuesta_bodegas`; ambos usan el reporte de stock YA cacheado: ninguna llamada nueva a Check. Una pantalla nueva con productos de un documento usa el modal compartido (no vuelve a armar esto a mano). **Caché fría (instancia recién levantada, o un reporte más viejo de `_CHECKWMS_STALE_MAX`: `_checkwms_stock_frio()`):** la ruta no deja hilos colgados los 4-45 s de la primera lectura: la pide en segundo plano (`_checkwms_swr`), espera SOLO `_CHECKWMS_ESPERA_FRIA_S` (8 s: con la petición viva Cloud Run le da CPU a esa lectura; sin petición en curso la estrangula, y el servicio no usa `--no-cpu-throttling`) y, si no alcanzó, responde 503 `calentando`; el modal reintenta (5 intentos, cada uno con tope de 25 s). **Un reporte VACÍO no es una lectura válida** (`_checkwms_stock_rows` no lo guarda y `respuesta_bodegas` lo trata como «Check no respondió»: si no, todo saldría «sin stock en Check» durante una hora). Una búsqueda nueva sobre el mismo contenedor —o que lo redibuja sin cajas— deja obsoleta a la anterior (token por contenedor + cajas aún en pantalla) y tras un fallo no se vuelve a preguntar por 30 s. Pruebas: `tests/test_tka_check_bodegas.py` y `tests/test_check_bodegas_ruta_runtime.py` (ejecuta la ruta y la caché de verdad; tarda ~4 min por el `ast.parse` de app.py).
- **Prueba:** `tests/test_checkwms_solo_lectura.py` falla si alguien agrega un
  POST/PUT/DELETE, saca la lista blanca o le habla a Check desde otro archivo.

### Prohibido (escriben en Check según su Swagger)

`monitorSalida`, `monitorSalidaPTL`, `cancelaLineaPde`, `actualizaLineaPde`,
`PTLmigraEspejo`, `monitorEntrada`, `monitorEntradaV2`, `cargaInventario`,
`documentoDespacho`, `documentoDespachoPTLSorting`, `materiales/peso-volumen`,
`maestroMateriales`, `EnvioEmail`, `EnvioEmail/Generico`. Y **todo POST**,
aunque se llame "obtiene…" o "…Existentes" (`obtieneOTporPDE`,
`pedidosExistentes`): POST queda fuera.

### Si crees que necesitas algo más de Check

Agregar un reporte GET nuevo a la lista blanca → avisarle a Daniel en el
mismo mensaje. Escribir en Check → **no**: eso lo hace la gente de Check
(Sebastián Ojeda) o el ERP, nunca ILUS.

---

## 🗄 REGLA #5 — Base de datos

- **Antes de SELECT de columnas nuevas, verificar el `CREATE TABLE`**
  correspondiente. `ast.parse` no detecta nombres de columnas SQL —
  son errores que solo aparecen en runtime.
- **Soft-delete por defecto** en tablas con datos críticos
  (`mant_maquinas`, `mant_clientes`). Hard delete solo con `confirm_text`
  + permiso `superadmin`.
- **Audit log** (`mant_logs`) en TODA acción destructiva. Antes de borrar,
  no después.
- **Índices composite** para queries con WHERE de 2+ columnas.
  Ej: `mant_visitas(cliente_id, estado)`, no índices simples.

---

## 🌎 REGLA #6 — Tiempos en hora Chile

MySQL guarda timestamps en UTC con `NOW()`. **TODO datetime que se muestre
en UI debe pasar por el filtro `chile_fmt`**:

```jinja
{{ user.last_login_at | chile_fmt }}     → 14/05/2026 19:48
{{ visita.fecha | chile_fmt('%d/%m/%Y') }}  → 14/05/2026
```

El filtro usa `zoneinfo("America/Santiago")` que maneja DST automático.

**Ninguna fecha se muestra jamás en inglés ni en formato ISO crudo**
(2026-07-29T21:00:00-04:00) — siempre día/mes/año + hora Chile. Esto
incluye fechas que vienen de APIs externas (FedEx Track API, SimpliRoute,
etc.): sus timestamps ISO traen SU PROPIO offset (puede ser de un hub
fuera de Chile, ej. Memphis) — hay que parsearlos con
`datetime.fromisoformat()` (nunca cortar el string a mano con
`partition('T')` o `.slice()`) y pasarlos por `chile_fmt_filter` antes de
mandarlos al frontend. Ver `_fedex_iso_a_chile()` en `app.py` como
patrón de referencia (bug real corregido 2026-07-29: las fechas de los
scans FedEx salían en inglés/UTC crudo en el modal de seguimiento).

---

## 🆔 REGLA #7 — Formato RUT chileno

Para mostrar RUTs en UI, usar el filtro `rut_fmt`:

```jinja
{{ cliente.rut | rut_fmt }}    → 25.547.065-2
```

---

## 📦 REGLA #8 — Modales Bootstrap NO bastan

Bootstrap modal nativo (`<div class="modal fade">`) puede usarse para
formularios largos (ej: editar OT con muchos campos). **Pero NUNCA**
para confirmaciones cortas, alertas o prompts — para eso van los
`ilus*` helpers (regla #1).

Si el modal tiene > 5 campos, usar Bootstrap modal. Si es < 3 inputs
o solo Yes/No, usar `ilusConfirm` / `ilusPrompt`.

---

## 🚀 REGLA #9 — Antes de pushear

1. Verificar sintaxis Python: `python -c "import ast; ast.parse(open('app.py').read())"`
2. Verificar Jinja: `env.parse(open('templates/...').read())`
3. Si tocó queries SQL nuevas: validar que las columnas existen en `CREATE TABLE`
4. Si tocó migraciones: que sean idempotentes (try/except + ON DUPLICATE)
5. Commit con mensaje descriptivo (qué cambió + por qué + impacto)

---

## 📨 REGLA #11 — Comunicaciones ILUS (email + WhatsApp + SMS)

Todos los mensajes salen con **branding genérico ILUS**, no con el correo
o teléfono personal del operador. Esto da consistencia y permite cambiar
quien firma sin tocar código.

### Variables de entorno (Railway → Settings → Variables)

### 📮 El correo de soporte es **soportetec@sphs.cl** (regla, 2026-08-27)

Pedido explícito de Daniel: *"como regla el correo es soportetec@sphs.cl"*.

Reemplaza a `servicio.tecnico@ilusfitness.com`, que aparecía en 15 lugares
(footers de PDF, seguimiento público de retiros y transporte, anexo de
servicios, defaults de `config.py`). **Todo correo visible para una persona
—cliente, proveedor o técnico— usa soportetec@sphs.cl.**

⚠️ Ojo con la REGLA #0: el dominio `@sphs.cl` NO es un descuido de marca —
es el dominio real de correo de la empresa (igual que `daniel.aguilar@sphs.cl`
y `contacto@sphs.cl`). No "corregirlo" a `@ilusfitness.com`: ese dominio no
tiene DNS de correo verificado (ver REGLA #13) y los mensajes no llegarían.

Todas son **opcionales** — si no se setean, hay defaults sensatos:

| Variable                  | Default                                  | Para qué sirve                          |
|---------------------------|------------------------------------------|------------------------------------------|
| `ILUS_BRAND_NAME`         | `ILUS Fitness`                           | Nombre legal completo (footer email)     |
| `ILUS_BRAND_FROM_NAME`    | `ILUS`                                   | Visible en cabecera "De:" del email      |
| `ILUS_BRAND_FROM_EMAIL`   | `no-reply@ilusfitness.com`               | Dirección remitente (no-reply genérico)  |
| `ILUS_BRAND_REPLY_TO`     | `soportetec@sphs.cl`       | Buzón donde caen respuestas reales       |
| `ILUS_BRAND_WA_NAME`      | `ILUS`                                   | Prefijo de WhatsApp/SMS (`🔧 ILUS · …`)  |
| `ILUS_BRAND_SUPPORT_EMAIL`| `soportetec@sphs.cl`       | Email en footer "Para soporte: …"        |
| `ILUS_BRAND_SUPPORT_URL`  | `https://ilusfitness.com/pages/soporte-tecnico` | URL portal soporte (footer). Es el link oficial para levantar tickets (Daniel 2026-09-24); `/soporte` da 404 |

**Cómo aparece para el destinatario:**

- **Email:** `De: ILUS <no-reply@ilusfitness.com>` · `Reply-To: soportetec@sphs.cl`
  Asunto: `ILUS · Cambio seguro de contraseña`
- **WhatsApp/SMS:** comienza con `🔧 ILUS · {tema}` y termina con `— ILUS Fitness`

### Helpers

```python
from app import _get_brand_cfg, _brand_subject, _brand_wa_prefix

brand = _get_brand_cfg()           # dict con name/from_name/from_email/etc
subject = _brand_subject("Confirmación de OT")  # → "ILUS · Confirmación de OT"
prefix  = _brand_wa_prefix("OT lista")          # → "🔧 ILUS · OT lista\n\n"
```

### Diagnóstico (admin)

- **GET** `/api/comm/diagnostico` — JSON con estado SMTP/Resend/Twilio,
  últimos envíos, brand efectivo. Solo `admin`/`superadmin`.
- **GET** `/admin/comunicaciones-test` — UI para mandar email/WhatsApp/SMS
  de prueba al destinatario propio antes de notificar a clientes.

### Cómo agregar un nuevo canal o tipo de mensaje

1. Reutiliza `_send_ilus_email(to, subject, html_body)` para email
   (el branding se aplica automáticamente).
2. Usa `_brand_subject("tema")` para el asunto.
3. Para WhatsApp/SMS, arma el cuerpo con `_brand_wa_prefix(asunto) + cuerpo + firma`.
4. Si se trata de notificaciones masivas, respeta `comm_is_enabled('email')`
   y `comm_is_enabled('whatsapp')` antes de mandar (kill switch global).

---

## 🤖 REGLA #10 — Cuando llega un agente nuevo

Cualquier agente nuevo debe leer **este archivo primero** antes de
escribir código. Si una regla no encaja con la tarea, ESCALARLO al
usuario (no improvisar).

Si necesitas agregar reglas nuevas, hazlo aquí — no en comentarios
sueltos del código.

---

## 🚀 REGLA #12 — DEPLOY: producción es **Google Cloud Run**, NUNCA Railway

**La aplicación de producción vive SOLO en Google Cloud Run.** Railway ya
NO corre la app — quedó como un simple "vigilante" que redirige el link
viejo al nuevo de Google.

### Cómo se despliega (lo único válido)

- `git push` a la rama → PR → **merge a `main`**.
- El push a `main` dispara **GitHub Actions** (`.github/workflows/deploy.yml`)
  que corre `gcloud run deploy ilus-app --source .` → usa el **Dockerfile**
  (app completa, `app:app`). Región `southamerica-west1`, proyecto
  `ilus-app-498503`.
- URL de producción:
  `https://ilus-app-469212710544.southamerica-west1.run.app`
- **Claude (acciones directas de esta sesión) NO necesita pedir OK a Daniel
  antes de cada merge a main** — autorización permanente desde 2026-05-22,
  reconfirmada 2026-08-09 y 2026-08-13 ("lo que yo quería era que tus
  agentes estén limitados, no tú como tal"). Revisa el diff, valida
  sintaxis, y mergea/despliega directo.
  **Los agentes delegados (Agent tool) SÍ siguen restringidos: nunca hacen
  push ni merge a main por su cuenta** — dejan el trabajo commiteado
  localmente (o en su rama del worktree) para que Claude lo revise y
  mergee él mismo.

### Railway = SOLO redirector (no es producción, no es respaldo)

- `railway_redirect.py` reenvía (302) el link viejo de Railway al de Google,
  preservando ruta + query. Lo arranca el **`Procfile`** y el **`nixpacks.toml`**,
  que apuntan a `railway_redirect:app` (NO a `app:app`).
- 🔴 **NUNCA** poner `app:app` en el `Procfile` ni en `nixpacks.toml`: la app
  completa NO levanta en Railway (faltan greenlet/pymssql/playwright) →
  "Deployment crashed" en cada PR. Eso fue un bug, ya corregido.
- El **`Procfile` y `nixpacks.toml` son SOLO de Railway.** Google usa el
  **Dockerfile**. No mezclar.

### Los correos de "Railway Deployment crashed / Deployed to …-pr-XX"

- Son del **GitHub App de Railway** reaccionando a CADA PR (crea un preview).
  **NO significan que estemos desplegando a Railway** — el deploy real es a
  Google. Con el `Procfile` apuntando al redirector, esos previews ya no
  crashean. Para que dejen de llegar del todo, **Daniel** debe desactivar los
  "PR environments" o desinstalar el GitHub App de Railway (no se puede desde
  el código).

### Regla de oro

**Si la tarea es "subir / desplegar / ver los cambios en producción" → es
Google Cloud Run vía merge a `main`. Railway NUNCA. No tocar `Procfile`/
`nixpacks.toml` salvo para mantener el redirector.**

---

## 📧 REGLA #13 — Envío de correos: SMTP es el método principal, NUNCA Resend por defecto

**Todo correo de ILUS debe salir por SMTP (Gmail, cuenta `daniel.aguilar@sphs.cl`)
como método principal.** Resend queda ÚNICAMENTE como red de respaldo automática
si SMTP falla (`_send_ilus_email_real`, app.py) — nunca como método principal,
salvo que Daniel lo pida explícitamente.

### Por qué

- Decisión original 2026-05-21 (ya vigente en código y en producción vía
  `ILUS_EMAIL_PROVIDER=smtp`): el dominio `ilusfitness.com` no tiene DNS
  (SPF/DKIM/DMARC) verificado en Resend. Sin eso, Resend manda como
  `onboarding@resend.dev` (cae a spam) o, peor, **una cuenta Resend sin
  dominio verificado SOLO puede enviar al correo con el que se registró la
  cuenta** — a un cliente real el correo simplemente no le llega, sin error
  visible para Daniel.
- SMTP (Gmail) sí entrega a cualquier destinatario, pero puede fallar desde
  IPs de datacenter (Cloud Run) si Gmail lo bloquea — por eso el fallback a
  Resend existe (mejor que no salga nada), no al revés.

### Qué hacer

- **NUNCA** cambiar `ILUS_EMAIL_PROVIDER` a `resend` o `auto` en producción
  sin que Daniel lo pida explícitamente en ese mensaje.
- Si un correo "no llega", diagnosticar en este orden: (1) ¿SMTP falló y
  cayó a Resend? (revisar logs `[ILUS][EMAIL]`) (2) si cayó a Resend, ¿el
  destinatario es distinto al correo de la cuenta Resend y el dominio sigue
  sin verificar? — ese es el síntoma más probable de "no se envía".
- La solución definitiva (verificar dominio propio en Resend) depende de
  Joaquín/DNS — mientras tanto, SMTP sigue siendo la regla.

---

## 🔲 REGLA #14 — Toda lista con selección múltiple lleva "marcar/desmarcar todo"

**Pedido explícito de Daniel (2026-08-30, viendo en vivo la pestaña
"Cotización interna" del modal de búsqueda de productos): "poder
seleccionar todo con un checkbox de seleccionar y no seleccionar. Eso
siempre déjalo como regla del proyecto".**

Toda lista o tabla del proyecto donde el usuario marca ítems con
checkboxes (líneas de un documento, productos de una cotización, filas de
una tabla, etc.) debe traer un control único para marcar/desmarcar TODOS
de una vez — no solo el toggle individual por fila.

### Patrón de referencia

`tkaToggleAllCli(idx)` en `templates/tickets/_tka_modal.html` (pestaña
"Por RUT" del modal de búsqueda de productos), con su botón asociado:

```html
<button class="tka-doc-add-btn" style="background:#fff;color:#374151;border:1px solid #d1d5db"
  onclick="tkaToggleAllCli(idx)">
  <i class="bi bi-check-all"></i>Seleccionar/deseleccionar todo
</button>
```

Es un **toggle**, no dos botones separados: si ALGÚN ítem visible está sin
marcar, la acción marca todos; si ya están todos marcados, la misma acción
los desmarca todos (`inputs.some(inp => !inp.checked)` decide el sentido).

Reutilizar siempre la función de toggle individual de la línea (línea por
línea, no una lógica de selección duplicada) — así cualquier regla especial
de esa lista (ej. "sin saldo pide motivo") se sigue respetando igual que si
el usuario marcara una por una. Ver también `tkaToggleAllCotizacion()`
(pestaña "Cotización interna", mismo archivo) como segundo ejemplo, y
`tkaToggleAllDoc`/`.tka-master-bar` (pestaña "Por documento") como variante
que además excluye a propósito ciertas filas (sin saldo) del "marcar todo".

---

## 🎨 REGLA #15 — Todo formulario/lista nuevo usa el lenguaje visual de Retiros, sin que Daniel lo pida

**Pedido explícito de Daniel (2026-08-30): "debemos creer en que el formato
que diseñamos, como el formato de Retiros, debe predominar... no como me
mostraste las cotizaciones, cómo se traía, no se entendía nada. Cuando
construyas algo, yo no necesito decirte que debes hacerlo con el formato
de Retiro, con las tarjetas, con semáforo, todo — en caso de que sea un
formulario."**

`/retiros/solicitar` es la referencia (ver también
[[feedback_diseno_premium_referencia_retiros]] en memoria): tarjetas con
step-circle rojo→verde con pulso, colores de estado tipo semáforo, y
**nombres/datos siempre completos, nunca truncados o cortados** — el
ejemplo negativo real fue la primera versión de la pestaña "Cotización
interna" del modal de búsqueda de productos, donde el nombre del producto
se cortaba a 2 líneas y no se entendía qué era.

Esto aplica por defecto, sin que Daniel tenga que pedirlo en cada tarea:
- Cualquier formulario nuevo → tarjetas + semáforo + progreso visual,
  mismo lenguaje que Retiros.
- Cualquier lista/tabla nueva de información real (productos, documentos,
  equipos) → información completa y legible, nunca truncada por un
  `line-clamp`/`overflow:hidden` puesto sin pensar en nombres largos
  reales de ILUS.

Ver también REGLA #4.3 (paginación) y REGLA #14 (seleccionar todo) —
las tres son del mismo espíritu: los patrones de UI ya resueltos en un
lugar del proyecto se replican, no se reinventan peor.

---

## 📐 REGLA #16 — En una grilla de 2 columnas, ninguna tarjeta puede medir más que la otra columna entera

**Pedido de Daniel (2026-09-02, viendo la pestaña Información de OT 2.0):
"ese hoyo me gustaría que fuera compensado, que toda la página venga bien
aprovechada".**

El caso real: la grilla tenía dos hijos (un wrapper por columna). La
izquierda medía ~1600px y la derecha ~950px, así que al terminar la derecha
quedaba un hueco de 500-750px. Y una sola tarjeta —Finanzas, con sus 4
pasos— medía más que TODA la columna derecha junta: ninguna repartición
entre dos columnas podía cerrarlo.

Antes de armar o tocar una grilla de dos columnas:

1. **Estimar el alto de cada tarjeta en el estado MÁS COMÚN** (gestión, con
   permisos). Una tarjeta que sola supera ~600px, o el total de la otra
   columna, NO va en una columna: va a **ancho completo**
   (`grid-column:1/-1`) con su contenido en horizontal — pasos en 2×2,
   listas en 2 columnas. Es el único "aprovechar el ancho" que sirve.
2. **Las tarjetas condicionales (`{% if %}`) se cuentan como AUSENTES** al
   balancear: si la mitad de tu columna depende de que el cliente tenga
   contraparte, esa columna colapsa el día que no la tenga.
3. Diferencia aceptable entre columnas: **menos de una tarjeta chica
   (~250px)**. Medir antes de subir, a 1280px:
   `[...document.querySelectorAll('.otd-grid > .otd-col')].map(c=>c.offsetHeight)`
4. En móvil la grilla colapsa a 1 columna y manda el orden del DOM. Si el
   orden de escritorio y el de móvil difieren, se resuelve con `order`,
   **nunca duplicando HTML**.

Referencia viva: `.otd-grid` y `#otdCardFinanzas` en
`templates/ot2/detalle.html`. Ver también REGLA #15 (formato Retiros) y
REGLA #3 (mobile-first) — las tres son del mismo espíritu.

---

## 🔒 REGLA #17 — La base de datos de producción (Cloud SQL `ilus-db`) es INTOCABLE a nivel de instancia — jamás se borra (no negociable)

**Incidente real, 2026-09-23:** la instancia Cloud SQL `ilus-db`
(`ilus-app-498503:southamerica-west1:ilus-db`, MySQL 8.0 — la base de
datos real de producción; **no** Clever Cloud, esa referencia en
`config.py`/este archivo quedó desactualizada desde la migración del
2026-06-05) fue **borrada** desde una sesión de Claude Code, tumbando
toda la página (cada endpoint que toca MySQL fallaba con "Cloud SQL
connection failed... instanceDoesNotExist"). Se recuperó con un
`RESTORE_VOLUME` de Google Cloud SQL (no siempre disponible — fue
suerte de ventana de retención, no una garantía). Causa raíz: la cuenta
de servicio `claude-deploy@ilus-app-498503.iam.gserviceaccount.com`
(la que usa Claude Code para `gcloud`/deploy) tenía **`roles/editor`**
a nivel de PROYECTO COMPLETO — un rol enorme que incluye
`cloudsql.instances.delete` y no hace falta para nada de lo que el
pipeline de deploy realmente necesita (`run.admin`, `cloudsql.client`,
`iam.serviceAccountUser` y `storage.admin` ya estaban también
otorgados por separado). Eso es lo que hizo posible que un comando de
agente pudiera destruir la base de datos real de la empresa.

### Blindaje ya aplicado (2026-09-23)

1. ✅ **`deletionProtectionEnabled=true`** en `ilus-db` — bloquea
   `gcloud sql instances delete` / borrado desde la Consola con un
   error explícito, a menos que alguien la desactive primero a propósito.
2. 🔴 **Pendiente de autorización de Daniel** (bloqueado por el
   clasificador de auto-modo como "Protected-Scope IaC Apply", no lo
   puede ejecutar Claude sin permiso explícito): una **IAM Deny Policy**
   a nivel de proyecto que niega `cloudsql.googleapis.com/instances.delete`
   para TODOS los principals (`principalSet://goog/public:all`), sin
   excepciones — ni siquiera para `roles/owner`. Una Deny Policy gana
   por encima de cualquier rol Allow (incluido Editor/Owner), así que
   protege incluso si alguien vuelve a otorgar un rol demasiado amplio
   por error. Para borrar la instancia alguna vez de forma legítima
   habría que primero editar/eliminar esta Deny Policy a propósito — un
   segundo paso deliberado, nunca un solo comando accidental.
3. 🔴 **Pendiente, recomendado, no aplicado todavía** (requiere probar
   contra un deploy real sin romper el pipeline — no tocar en caliente
   sin ese cuidado): quitar `roles/editor` de `claude-deploy@...` y
   dejar solo los roles mínimos que el deploy realmente usa
   (`run.admin`, `cloudbuild.builds.editor`, `artifactregistry.writer`,
   `iam.serviceAccountUser`, `storage.admin` en el bucket de fuentes).
   Esto es defensa en profundidad extra sobre la Deny Policy — reduce
   el radio de daño de CUALQUIER cuenta de servicio de agente a lo que
   estrictamente necesita, no solo para Cloud SQL sino para Compute,
   Pub/Sub y todo lo demás que Editor also permite tocar/borrar.

### Regla para cualquier agente (esta u otra sesión), sin excepciones

- **JAMÁS** ejecutar `gcloud sql instances delete`, `gcloud sql instances patch --no-deletion-protection`, ni nada que borre o desproteja una instancia de Cloud SQL — bajo ningún pretexto, ni "es solo un test", ni con un mensaje de Daniel que parezca autorizarlo de pasada. Si de verdad hace falta decomisionar una instancia algún día, **detente y pregúntale a Daniel explícitamente en ese mensaje**, mismo criterio que REGLA #4.1 con el ERP Random.
- Backups automáticos + point-in-time recovery (binary log) ya están activos en `ilus-db` (7 backups retenidos, 7 días de logs de transacciones) — no desactivarlos nunca.
- Si el clasificador de auto-modo bloquea una acción de este tipo ("Production Deploy" / "Protected-Scope IaC Apply"), **es la señal correcta funcionando** — no buscar la manera de saltárselo. Explicar a Daniel qué se intentaba y por qué, y esperar su autorización explícita.

---

## 🧊 REGLA #18 — Escalabilidad: el arranque de la app NO puede bloquear tablas, y no se agranda infraestructura sin diagnóstico

**Incidente real, 2026-09-29:** la página se cayó dos veces en horario
laboral. Se subieron instancias de Cloud Run, la RAM de la BD y el disco
(+~100 mil CLP/mes) y nada lo arreglaba de fondo. La causa real era
otra: cada instancia nueva corre al arrancar init_db + los `_ensure_*`
(~800 ALTER/CREATE), y un DDL "que no cambia nada" igual pide un lock
EXCLUSIVO de la tabla. Con `lock_wait_timeout` en el default de MySQL
(1 año), un ALTER que esperaba detrás de una transacción abierta
congelaba `mant_clientes` para toda la empresa, y más instancias =
más ALTER en fila. Fix: commits `868d3a04` y `57212c2d`.

### Reglas

1. **Toda migración nueva debe ser barata cuando ya está aplicada.**
   Usar `mysql_execute(...)` o la conexión de `get_mysql()` — ambas pasan
   por `_ddl_ya_aplicado()`, que salta sin pedir lock los `CREATE TABLE IF
   NOT EXISTS`, `ADD COLUMN`, `ADD INDEX`/`CREATE INDEX` y `MODIFY ... ENUM`
   ya aplicados. **Una sentencia = una cláusula** (`ADD COLUMN a, ADD COLUMN
   b` en una sola sentencia no se puede verificar y corre siempre).
2. **Nunca achicar un ENUM en el arranque**, y nunca dos migraciones que
   definan la misma columna con valores distintos (había 4 para
   `mant_visitas.estado`). Un MODIFY que solo cambia DEFAULT/NOT NULL de
   un ENUM sin agregar valores lo salta la guardia → aplicarlo a mano.
3. **No tocar `lock_wait_timeout`** (pool 10 s, init 5 s, DDL 3 s): es lo
   que impide que un DDL deje una tabla congelada.
4. **Antes de agrandar infraestructura (instancias, tier de BD, disco),
   diagnosticar.** Firma de "lock, no capacidad": un `SELECT ... WHERE
   id=%s` tarda segundos con la BD a CPU baja → metadata lock. Agrandar
   ahí solo aumenta la factura. Los cambios de tamaño de BD se explican a
   Daniel con su costo mensual en pesos ANTES de aplicarlos.
5. **Recursos de Cloud Run solo en `.github/workflows/deploy.yml`** (un
   `gcloud run services update` manual lo pisa el siguiente push — pasó
   con `--max-instances` y con `--memory`).

---

## 🔒 REGLA #19 — Ningún técnico ve ni contacta proveedores (no negociable)

**Pedido explícito de Daniel (2026-10-01): "nunca los técnicos deben contener
datos o poder comunicarse con los proveedores. Ni siquiera pueden ver mis
proveedores."** Confirmado: aplica a TODA la familia técnico (interno,
elevado/`tecnico_ejecutivo` —Jaizer incluido— y externo) y en TODO el sistema.

- Un técnico ve del repuesto: SKU, descripción, cantidad, stock (semáforo),
  ubicación, equipo/modelo compatible, fotos y el manual del equipo. Nunca
  proveedor, su contacto, costo, N° OC, notas de gestión ni tickets de compra.
- **Una sola fuente de verdad:** `_oculta_proveedores()` en `app.py` (Jinja:
  `oculta_proveedores`). NO usar `_otrep_puede_gestion()` para esto: devuelve
  True para el técnico interno (esa fue la fuga).
- El dato se quita **en el servidor** (`_otrep_fmt_stock`, `_otrep_fila`,
  contextos de Bodega/Repuestos, bitácoras), no solo en la plantilla: lo que
  viaja en un `tojson` se lee con "ver código fuente".
- Las escrituras de un técnico **ignoran** `proveedor_id`/`costo_unitario` y
  conservan lo que gestión ya cargó (nunca los pisan con NULL).
- Toda pantalla o endpoint nuevo que muestre repuestos pasa por esa regla.
  Gestión no pierde nada.

---

## 🤖 REGLA #20 — Retiros: «Enviar a preparación» es AUTOMÁTICO según Check (no se apaga ni se quita sin permiso)

**Pedido explícito de Daniel (2026-10-02): "necesito que envíes a preparación en automático. Por supuesto, lleva control de hora, fecha, todo. Y usuario.
Internamente… al cliente le va a dar fecha nada más".**

- Un retiro con la **cita confirmada y cercana** pasa solo a «En preparación» cuando Check muestra que bodega **ya empezó a juntar** el pedido (alguna unidad
  pickeada y nada despachado). Mismo camino que el botón: checklist de bodega, correo al cliente y aviso interno. Código: `pickups_module.py` (bloque «ENVIAR A
  PREPARACIÓN» AUTOMÁTICO), señal en `retiros_check.evaluar`.
- **Salvaguardas (revisión adversarial 2026-10-02 — no quitarlas):** cita de **hoy o de los próximos N días HÁBILES** de la bodega (N=1; viernes → lunes cuenta 1; una
  cita pasada la decide una persona) · solo en **horario de cobertura** (lunes a viernes hábiles 08:00–17:00 con colación 13:00–14:00, hora Chile; `RETIROS_COBERTURA_*`: ningún correo de madrugada, en colación, de tarde ni en fin de semana o feriado) ·
  **una sola vez por retiro** (si una persona lo devuelve a «Cita confirmada», el automático no lo repite ni le vuelve a escribir al cliente) · nunca con un cambio de
  fecha del cliente pendiente (la guarda va también dentro del UPDATE) · nunca si la factura o boleta está en **otro retiro activo** (Check informa por documento) ·
  **no exige responsable** (no es una persona: el 2026-10-05 el retiro real no tenía y bodega terminó sola sin que ILUS lo notara; el aviso interno dice «sin responsable») · la señal se ve en **dos lecturas** y la segunda se pide **de verdad** a Check (sin su memoria de 45 s).
- **La bitácora siempre distingue** «Manual: <usuario> pasó el retiro a «En preparación»» de «Automático · Check WMS …» (con hora Chile, N de M unidades y, si Check ya
  los informa, la OT y el usuario). Quitar o mezclar esa distinción rompe lo que Daniel pidió.
- **Check ya preparó Y despachó pero el retiro sigue en «Cita confirmada»:** no se envía a preparación (pudo entregarse antes; «estamos preparando» llegaría con el pedido ya entregado). Se avisa al
  equipo UNA vez al día (`check_desfase`, campana + bitácora), la guía dice «preparado según Check» y ofrece «Marcar como RETIRADO» (el modal de retirado existe también con la cita
  confirmada), y «Enviar a preparación» advierte que no corresponde. Nunca se envía a preparación un pedido ya despachado sin ese aviso. **Si Check expidió TODO** (cita dentro
  de la ventana), desde 2026-10-09 lo cierra la expedición automática de la REGLA #23 y la preparación queda en la bitácora SIN el correo «Estamos preparando».
- **Al cliente solo se le comunica la fecha agendada** (el mismo correo «Estamos preparando tu retiro»). Jamás datos internos: OT, usuarios de Check, horas de picking.
- Check sigue **SOLO LECTURA** (REGLA #4.4): el envío automático solo usa `GetSeguimientoDespacho`. El cambio de estado es atómico (`UPDATE … WHERE
  status='agenda_confirmada'`): si el botón gana, no se repite nada, y el botón ya no escribe «en preparación → en preparación» si el automático llegó antes.
- Interruptores (solo con permiso de Daniel): `RETIROS_PREP_AUTO=0` lo apaga (también lo apaga `RETIROS_CHECK_AUTO=0`: «Check solo informa»);
  `RETIROS_PREP_AUTO_DIAS` cambia la ventana. Corre con la ficha abierta, al entrar al Monitor y cada 10 min colgado del trabajo de Cloud Scheduler
  `simpliroute-poll` (`/retiros/cron/check-barrido` existe para un job propio; `?dry=1` solo mira, incluso de noche). Pruebas: `tests/test_retiros_prep_auto.py`.

---

## 🙋 REGLA #21 — Retiros: sin responsable declarado no se avanza, ni se agenda, ni se libera el calendario

**Pedido explícito de Daniel (2026-10-02): "Para avanzar debe declarar el responsable y para agendar o liberar el calendario".**

Un retiro **sin responsable** (`responsable_user_id` / `responsable_nombre` vacíos) no avanza hasta que alguien toque «Me hago cargo» (paso 2 de la guía; el nombre sale
de la sesión, nunca del navegador):

- **Orden de la ficha y de la guía (Daniel 2026-10-14: «comienza con el paso 2, no tiene sentido… deja el paso 2 por debajo de los productos»):** 5 pasos,
  en este orden y leídos por clave (nunca por posición): 1 `documentos` «Facturas y productos» (UNA tarjeta con dos confirmaciones: facturas y productos),
  2 `responsable`, 3 `agenda`, 4 `preparacion`, 5 `entrega`. **Confirmar facturas y productos NO exige responsable** (solo escribe en la bitácora; nada al
  cliente ni al estado); con el paso 1 listo, lo que toca es «Me hago cargo».
- **Guía:** sin responsable, agenda, preparación y entrega quedan bloqueadas con el motivo a la vista y los botones nativos (proponer fecha, enviar a
  preparación, marcar retirado) abren un aviso que lleva al paso 2. `retiros_guia.evaluar` → `sin_responsable`.
- **Servidor (`pickups_module.py`, `_sin_responsable`):** exige responsable `/proposal` (agendar), `/aceptar-contrapropuesta`,
  `/marcar-aceptada-manual` y **cualquier cambio de estado** de `/status` (Kanban, Cambiar estado, botones de la ficha: confirmar, reagendar, rechazar, cerrar → ocupan o
  liberan el calendario). Responde 409 con «Primero declara quién se hace cargo… Sin responsable no se avanza ni se agenda o libera el calendario».
- **Excepción:** un retiro **terminado** (retirada, cerrada, rechazada, fallida) no lo necesita (así se puede reabrir: «Me hago cargo» rechaza los terminados).
- **No es retroactiva:** los retiros creados antes del 03-oct-2026 (`RETIROS_EXIGE_RESPONSABLE_DESDE`) siguen hasta el final sin responsable (la guía solo avisa). Daniel, 2026-10-05, con el retiro
  real: «esto avanzó antes de que fuera una restricción, por eso no avanzó».
- El envío automático de la REGLA #20 NO exige responsable (no es una persona). Interruptor (solo con permiso de Daniel): `RETIROS_EXIGE_RESPONSABLE=0`.
- Los bloqueos de la agenda (`/retiros/bloqueos/*`, permiso `ret_horarios`) son de la bodega, no de un retiro: esta regla no los toca.

---

## 🛡️ REGLA #22 — Retiros está en AMBIENTE REAL: cero correos o cambios visibles para el cliente al probar o corregir

**Pedido explícito de Daniel (2026-10-06): "ten mucho cuidado de enviar algún correo, ya que este es el ambiente real y hay clientes reales de por medio… no genere ningún mensaje o correo o cambio que genere una molestia hacia el cliente… no pases a llevar nada".**

- Todo se prueba con el arnés (`tests/_arnes_retiros.py`, correo/WhatsApp de mentira) o con la vista previa local; **jamás** contra un retiro real ni con el correo real encendido.
- Una corrección de datos de un retiro real **no se hace escribiendo a mano en la base**: se hace con código que rellena solo lo que falta (ej. `_pickup_sync_totales_si_faltan`: peso, peso volumétrico y m³ en 0 se completan con los productos; **nunca pisa** una cubicación de una persona) y deja constancia en la bitácora.
- **Registro de Check guardado** (`pickup_check_snapshots`, solo OT, quién y cuándo, fusionado por OT): la tarjeta de preparación sigue mostrando lo que Check informó con el retiro completado. Check sigue SOLO LECTURA (REGLA #4.4).
- **Horario de cobertura** (lunes a viernes hábiles 08:00–17:00, colación 13:00–14:00, hora Chile; variables `RETIROS_COBERTURA_DESDE/_HASTA/_COLACION_DESDE/_COLACION_HASTA`): `/retiros/api/cobertura` + `static/retiros_cobertura.js` avisan **antes** de gestionar un retiro a mano fuera de horario (calendario, Kanban, ficha). Es solo un aviso: no bloquea. El envío automático de la REGLA #20 respeta ese mismo horario.
- El Monitor muestra «Solicitada dd/mm/aaaa hh:mm · hace X» y lee `peso_real_kg`/`peso_vol_kg` (los que se completan), no `total_weight_kg` del formulario (bulto de relleno en 0).
- **Formulario público = superficie expuesta (revisión de seguridad 2026-10-12, Daniel: «no quiero errores o exposiciones a mi app»).** Toda ruta interna de Retiros pide `require_permission("retiros")`, **nunca `"view"`** (lo tiene cualquiera con Etiquetas/Mantenciones/Transporte, técnicos incluidos; hay prueba). `/retiros/solicitar`: 10 envíos/h y 30/día por conexión, POST > 64 KB → 413, fecha solo AAAA-MM-DD, tope de 3/día por **buzón real** (`correo_canonico`), CORS solo `ilusfitness.com` e `ilus-fitness.myshopify.com` (+ `null` para revisar el HTML local), alarma interna de volumen (`RETIROS_ALERTA_SOLICITUDES_HORA`, 12; nunca al cliente). Anti-bot **sin terceros** por la Ley 21.719 (Daniel): agregar Turnstile u otro servicio externo exige actualizar el aviso de privacidad y su «sí». Excel con datos del formulario: un texto con «=» va como texto, nunca como fórmula. Pruebas: `tests/test_retiros_12oct_seguridad_formulario.py`.
- **Capacidad del bloque = AVISO, no candado (Daniel 2026-10-16, RET-XQHPMT: 10,53 m³ en un bloque de 5 m³ y Juan no podía agendar: «tu trabajo es alertar, no trabar»).** Peso y volumen sobre el tope del bloque (`_validar_disponibilidad_slot`) no bloquean en ningún camino: vuelven como aviso «Pedido grande o bloque sobrecargado… Coordina con bodega» (ámbar, en la ficha y el calendario) y quedan en la bitácora (`capacidad_excedida`). Solo frenan los choques reales de agenda: bloque lleno por cantidad de retiros, colación, bloqueos, feriados y capacidad del día. Pruebas: `tests/test_retiros_16oct_capacidad_aviso.py`.
- **El reloj del SLA («Sin responder») corre en el MISMO horario de cobertura** (`_cc_ventanas()` → `mon.ventanas`; servidor y navegador iguales). Nunca un contador «00:00:00»: fuera de horario dice cuándo parte el reloj.

---

## ⏱️ REGLA #23 — Retiros: tiempo medido y controlado (Check = evidencia), y lo que NO se activa sin Daniel

**Pedido de Daniel (2026-10-06): "hay que medirlo… prometer automatización, tiempo controlado, una gestión de retiro a nivel de gerencia operacional logística"** y "calcular los minutos que se prepara el retiro según el WMS y tener toda la evidencia… registro de cuánto se tarda por producto en promedio".

- **Una sola lógica de tiempos:** `retiros_tiempos.py` (puro, con pruebas) y su espejo en `static/retiros_guia.js`. Trabajo = suma de cada OT; preparación efectiva = tiempo con al menos una OT abierta; pausas = principio a fin − efectiva; un tramo que pasa de un día a otro solo cuenta la jornada de bodega (07:30–20:00 L–V, `RETIROS_JORNADA_BODEGA`). Si cambias una, cambia la otra y sus pruebas.
- Se guarda como evidencia en `pickup_prep_tiempos` (con el criterio escrito) y `pickup_prep_productos` (minutos de picking por SKU) al guardar el registro de Check. Alimenta los KPIs «Gestión operacional» y `/retiros/api/tiempos-preparacion`.
- **Expedición en Check = RETIRADO está ACTIVA desde el 2026-10-08** (Daniel lo autorizó explícitamente; `RETIROS_RETIRO_AUTO=activo` en `deploy.yml`): cuando bodega expide en Check TODO el pedido, el retiro pasa solo a RETIRADO y el cliente recibe «Retiro completado». Bodega debe expedir solo cuando el cliente está retirando. Volver a `sombra` o apagarlo, solo con su permiso.
- **Picking = preparación; expedición = entrega, en TODOS los retiros (Daniel 2026-10-09, con BLV 23732 de Gerd Müller como modelo: «¿por qué se cambió esa regla?»).** El candado del 08/10 (cerrar solo desde «En preparación») queda sin efecto: dejó a RET-YWN4D4 en «Cita confirmada» con el pedido entregado. Ahora cierra también con la cita confirmada **dentro de la ventana** (hoy o el próximo día hábil) y deja en la bitácora el paso a «En preparación» **sin** correo «Estamos preparando». Salvaguardas que siguen: dos lecturas a Check, documento en otro retiro activo, cambio de fecha del cliente pendiente, UPDATE atómico, y **un documento que ya no tenía saldo al asociarlo** (`con_saldo=0`: pudo entregarse antes) no cierra solo desde la cita confirmada (solo avisa al equipo). **Lo que una persona decidió no se deshace:** un retiro que estuvo «Retirada» y alguien reabrió, o que volvió a «Cita confirmada» después de estar «En preparación», nunca se cierra solo de nuevo ni recibe otro «Retiro completado» (solo aviso al equipo, una vez).
- **La expedición se revisa en la JORNADA de la bodega** (07:30–20:00 días hábiles, `RETIROS_JORNADA_BODEGA`, `_retiro_auto_en_horario`), no solo en la cobertura 08–17: el correo «Retiro completado» acompaña en tiempo real al cliente que retira. «Enviar a preparación» automático sigue en la cobertura (REGLA #20).
- **El registro de Check de un retiro cerrado se completa con la OT de CONTROL DE SALIDA** (antes se congelaba al cerrar con lo que Check tenía en memoria, hasta 1 h viejo, y solo se veía «PICKING MATERIAL»): la ficha lo completa hasta 7 días después del cierre y el cron (`_check_snap_pendientes`) hasta 5 días (un fin de semana largo), pidiendo el reporte de movimientos a lo más cada 20 min y solo si hay retiros sin la salida. Solo se agrega y solo OT que empezaron hasta 8 h después del cierre (las de otro retiro posterior con la misma factura no se cuelan); el cron solo mira retiros con actividad en Check. Nunca cambia el estado ni escribe a nadie. Pruebas: `tests/test_retiros_09oct_registro_salida.py`.
- **La encuesta de satisfacción está ACTIVA desde el 2026-10-14** (Daniel: «sí, tira la encuesta por el correo de retirado, obviamente. Allí el cliente tomará la opción de hacerla o no»; `RETIROS_ENCUESTA_ACTIVA=1` en `deploy.yml`). Va SOLO como botón dentro del correo «Retiro completado» y en el seguimiento del retiro ya retirado: ningún correo adicional ni retroactivo. Apagarla, o cualquier firma digital que le llegue al cliente, **solo con el «sí» explícito de Daniel**. Los datos de la encuesta siguen la Ley 21.719 (aviso, consentimiento, sin datos personales, 24 meses).
- **La encuesta de UN retiro se activa SOLO con el correo «Retiro completado» enviado (Daniel 2026-10-16: «activemos esta herramienta solo cuando se envíe el correo de retirado o cuando cambie esa condición»).** Una sola regla, `retiros_encuesta.estado_encuesta` (apagada · no_aplica · sin_correo · respondida · vencida · activa): el enlace del cliente, las estrellas del seguimiento y la tarjeta de la ficha la usan. Abrir el enlace o el seguimiento **ya no crea** la invitación (solo el correo la crea y `marcar_enviada` la marca). Reabierto → se apaga; vuelve a retirado → se activa solo si sale de nuevo el correo (`enviada_en` posterior al cierre vigente, margen `TOLERANCIA_ENVIO_MIN`). La ficha muestra 2 pasos (correo enviado → respuesta) y las respuestas con estrellas; la pantalla de resultados «La voz de nuestros clientes» cuenta solo correos que llevaron la encuesta. Pruebas: `tests/test_retiros_encuesta.py`.
- **Daniel 2026-10-07: «no generes ninguna notificación a ningún cliente hasta que te autorice, sobre todo con la encuesta».** El enlace de la encuesta en el correo «retiro completado» y en el seguimiento ya está programado, pero solo aparece con `RETIROS_ENCUESTA_ACTIVA=1`; el comprobante de firma por correo solo sale con `RETIROS_FIRMA_CORREO=1`. Ninguna de esas variables se enciende (ni en `deploy.yml`) sin su autorización explícita en ese mensaje (`RETIROS_RETIRO_AUTO=activo` sí: autorizado el 2026-10-08; `RETIROS_ENCUESTA_ACTIVA=1` sí: autorizado el 2026-10-14; `RETIROS_FIRMA_CORREO` sigue apagada).
- **Encuesta y firma de recepción en VISTA PREVIA, como ROL** (Daniel 2026-10-14: «solamente para una vista al superadministrador… podemos activarlo por el front más adelante. Déjalo como rol»). `usuario_en_vista_previa()` (retiros_encuesta.py; Jinja `retiros_vista_previa()`) = superadmin **o** el permiso `ret_encuesta_firma` (Roles → Retiros → «Encuesta y firma de recepción (vista previa)», apagado en todos los roles; `RETIROS_VISTA_PREVIA_USUARIOS` queda como lista extra). Cubre: botón «Encuestas» del Monitor, dato de la encuesta en la ficha, firma en el modal «Marcar como retirado», modal «Registrar firma», editor/resultados de la encuesta y `POST /retiros/<id>/firma` (403). Encender el permiso para otro rol, solo con su «sí».
- **«Marcar como retirado» (2026-10-14, Daniel: «está pésimo»):** quién se lo llevó (nombre obligatorio, RUT opcional), foto de la entrega **opcional** y un solo aviso («al confirmar le llega «Retiro completado»»). La firma solo aparece en vista previa.
- **Firma de recepción por WhatsApp (2026-10-15, vista previa):** «Registrar firma» abre PRIMERO el celular (Daniel: «antes del modal registrar la firma me indique un número de teléfono… y te conectas a WhatsApp por la aplicación»); `POST /retiros/<rid>/firma/enlace` crea un enlace de UN uso que vence en 48 h (`pickup_firma_enlaces`, se guarda solo el SHA-256 del token) y devuelve el `wa.me` listo: **el sistema no envía nada**, lo envía el colaborador desde SU WhatsApp. La persona firma en `/retiros/firmar/<token>` (pública, CSRF exenta SOLO esa ruta, 20 envíos/h por conexión, aviso Ley 21.719) con el MISMO núcleo `_firma_registrar` (validaciones, hash, una firma por retiro que nunca se pisa). La ficha espera con `/firma/estado` y se recarga sola. Pruebas: `tests/test_retiros_15oct_firma_whatsapp.py`.
- **Estrellas del seguimiento = parte de la encuesta (2026-10-15):** tocar una estrella lleva a la encuesta con «atención en bodega» pre-marcada (`?atencion=N`); solo se guarda cuando el cliente confirma con el aviso. Pruebas: `tests/test_retiros_15oct_estrellas.py`.
- **Lo que escribe el cliente se ve (2026-10-15, Daniel: «hay comentarios que dejan los clientes y no los veo»):** «¿Algo más que debamos saber?» (`observations`) va en la tarjeta «Lo que escribió el cliente» bajo la cabecera y en el aviso «Nueva solicitud»; el comentario al pedir otra fecha va en la bitácora, el aviso al equipo y los banners. Todo texto NUEVO que escriba un cliente debe verse en la ficha. Pruebas: `tests/test_retiros_15oct_comentarios.py`.
- **Retiro terminado = tarjeta de productos CONGELADA (2026-10-15, retiro 105):** con el retiro entregado no se compara con el saldo actual del ERP (la guía de ESE retiro lo dejó en 0): «Entregado en este retiro» con su guía, «Ya había salido antes» o «No se retiró»; sin rojo. Un retiro abierto no cambia. **«Entregado» solo con evidencia del ERP (2026-10-20, retiro de Ivo Simunovic: un producto con 0 físico, saldo 1 y sin guía salía «Entregado en este retiro»):** una guía/boleta de este retiro o el saldo ya consumido. Sin guía → «Retirado · falta la guía del ERP» (ámbar); sin guía y sin stock suficiente en la bodega → «No consta la entrega» (ámbar) y la tarjeta dice «Retiro completado · revisa lo entregado». Ninguno cuenta como entregado. Pruebas: `tests/test_retiros_15oct_productos_cerrado.py`.
- **Desviaciones («¿Qué pasó?», Daniel 2026-10-09: «si el cliente no viene… reagendar o cancelar… decisiones humanas, incidencia, no pudo llegar»).** Modal en la ficha de todo retiro abierto (`templates/retiros/_desviacion_modal.html`, reglas puras en `retiros_desviaciones.py`, tabla `pickup_desviaciones`): Sí retiró · No vino (solo con la cita YA vencida; reagendar, esperar o «No concretado») · Reagendar · Cancelar · Incidencia (no cambia el estado). **Motivo obligatorio de una lista** (con «Otro motivo», detalle obligatorio); queda quién, cuándo, motivo y acción. **Al cliente solo se le escribe si el responsable marca «Avisar al cliente»** (correo propio «No pudimos concretar tu retiro» para fallida; «cancelado» para rechazada). Aviso a bodega a `aviso_bodega_emails` (Configuración 4.2b). **Cita vencida** (fin + 30 min): aviso UNA vez por cita al responsable (campana) y al equipo, en la jornada de la bodega, nunca al cliente (`RETIROS_AVISO_CITA_VENCIDA=0` lo apaga); la guía pregunta «¿Qué pasó?» y el envío automático NO manda «Estamos preparando» con la cita vencida. Los formularios de la ficha mandan `estado_visto`: si otra persona ya cambió el retiro, `/status` no aplica nada. La campana funciona para quien entra por Retiros (solo sus avisos); todo `tipo` de aviso nuevo va al ENUM de `mant_notificaciones` (hay prueba).
- **Cliente ADELANTADO = «Sí retiró» (2026-10-16, Daniel con el retiro de Inversiones Fit Paradise SpA: «me están llegando clientes adelantados… necesito gestionarlos como retirados»).** «¿Qué pasó?» ofrece «Sí retiró» en TODO retiro abierto. Sin cita confirmada, «Marcar como retirado» pide un **motivo de la lista** (`retiros_desviaciones.MOTIVOS_ADELANTADO`; «Otro motivo» exige detalle) y `/status` solo deja pasar la guarda de secuencia con `adelantado=1` + motivo válido; queda `retiro_adelantado` en la bitácora (motivo, estado de antes, quién). Sigue exigiendo responsable (REGLA #21). Al cliente le llega SOLO «Retiro completado». La firma (vista previa) se registra después con «Registrar firma». Pruebas: `tests/test_retiros_16oct_retiro_adelantado.py`.
- El tiempo estimado de preparación **nunca** se le muestra al cliente (hay pruebas que lo vigilan).
- **Retiro terminado = ficha en SOLO LECTURA** (Daniel 2026-10-06: «una vez que se cierra… que no se pueda gestionar nada más, que no pueda agregar factura»). Con estado retirada, cerrada, rechazada o fallida, toda ruta de gestión responde 409 (`_rechazo_si_cerrado`) y la ficha muestra la franja «Retiro cerrado…». Siguen abiertos: el chat con el cliente, las lecturas y los procesos automáticos. Toda ruta NUEVA que modifique un retiro debe pasar por `_rechazo_si_cerrado`.
  - **Hermético (Daniel 2026-10-12: «el pedido se tiene que bloquear una vez que se retire completamente… tiene que ser un sistema hermético»).** `/status` tampoco saca a un retiro terminado de su estado: desde el Kanban o el calendario (fetch) responde 409 para todos, y desde la ficha solo un **superadmin** lo reabre, con **motivo** (≥ 10 caracteres). Reabrir **no le escribe al cliente** ni avisa al equipo como un paso normal; queda en la bitácora «Reabierto por … Motivo: …». Repetir el mismo estado no reescribe nada (ni `closed_at` ni quién retiró). Antes, arrastrar una tarjeta «Retirada» a Preparación le mandaba al cliente «estamos preparando tu retiro». La ficha cerrada no muestra la agenda navegable (solo la cita fija), ni «Quitar» facturas, ni sugerencias de saldo; el Kanban no deja arrastrar tarjetas terminadas. Pruebas: `tests/test_retiros_12oct_hermetico.py`.

---

## 🧾 REGLA #24 — El documento de Random manda en OT, Tickets y Cotizaciones; sin documento solo con autorización de Daniel (no negociable)

**Pedido explícito de Daniel (2026-10-07), tras revisar con el gerente general OT de junio sin documento (OT 43 La Dehesa) y OT in situ abiertas sin documento:** "Los servicios deben cobrarse… Todo con documento tiene que ser absoluto y solamente pidiendo autorización remota con un argumento… esto tiene que ser inviolable." Y: "tanto tickets y OT y cotización deberán siempre predominar con el documento de Random a menos que yo lo autorice, ahí predomina el argumento; que dejemos con la trazabilidad de quién autorizó."

- **OT de cliente:** no se crea ni se cierra sin documento de Random (factura, boleta o nota de venta, validado contra el ERP y el RUT). La única salida es «Pedir autorización a Daniel» con argumento; un superadmin aprueba o rechaza a distancia. **Todo $0** (garantía, regalía, arriendo/leasing) también pasa por esa autorización. Exentas solo: OT interna sin cliente y mantención preventiva de un contrato REAL. **Para CREAR basta nota de venta o cotización; para CERRAR siempre factura/boleta** (la nota de venta queda como documento anterior, dada de baja por la factura; deja sin efecto la regla del 19-08 «la NV cierra»). En el modal de cierre el autorizador (Aarón, Juan Pablo, Víctor) puede retractar el cobro y pedir garantía con argumento (va a Daniel). **Centro de costo obligatorio siempre.** Ningún candado sin salida: cada rechazo de cierre dice qué hacer y se resuelve desde el mismo modal (Daniel: «que no puedan entramparse en un ciclo que no tiene solución»).
- **Tickets y Cotizaciones:** mismo principio (documento de Random primero; sin él, argumento + autorización). Usan el MISMO mecanismo de autorizaciones (tabla genérica con `entidad`), no uno propio.
- **Trazabilidad siempre:** quién pidió, quién autorizó o rechazó, cuándo (hora Chile) y el argumento completo, visible donde se vea el documento. Nada se borra; se registra.
- **Gestión documental transparente:** todos los documentos de una OT viven en `mant_visita_documentos` (multidocumento); ninguno se sobrescribe; aparecen en la ficha, el recorrido, las bandejas, el Excel y el informe/PDF (al cliente solo tipo y número, nunca montos internos ni proveedores).
- Una puerta única (`_ot_puerta_documento`) cubre todos los caminos que crean o cierran OT. Todo camino NUEVO pasa por ella. El interruptor del candado solo lo cambia superadmin.
- **Reafirmado por Daniel (2026-10-18):** «los tickets, las cotizaciones, las órdenes de trabajo siempre tienen que manejar un documento. Así sea garantía, así sea lo que sea… a menos que sea un código escrito a mano y la persona deberá argumentar, con ese argumento persistente en la base de datos: por qué autorizó, quién y cuándo… deberá ser obligatorio, a menos que sea un trabajo interno». Es decir: **la garantía también lleva el documento de la venta**; **todo producto o código escrito a mano** (no traído de un documento) exige argumento guardado en la base con quién, cuándo y por qué; el único exento es el **trabajo interno**. Auditoría de cumplimiento por módulo en curso (18-oct): si un camino de Tickets, Cotizaciones u OT no cumple, se cierra con la MISMA puerta y la MISMA tabla de autorizaciones, nunca con un mecanismo paralelo.

---

## 🧮 REGLA #25 — Una línea de servicio de una factura no se cobra dos veces (saldo por línea; no negociable)

**Pedido explícito de Daniel (2026-10-08): «algo bien inteligente para evitar que dos instalaciones se paguen con el mismo saldo»**, y «esto debe funcionar para el modal de crear OT y para el modal de cerrar OT con la firma, para no trabar el proceso».

- Cada línea de servicio (ZZINSTALACION, ZZMANTENCION…) y de despacho (ZZENVIO) de un documento de Random tiene un **saldo** = su monto − lo que ya cobran **otras** OT (no canceladas ni anuladas). Ninguna OT puede declarar como cobro más que ese saldo. Una **nota de venta dada de baja por su factura no cuenta doble** (la factura hereda lo que usó la nota).
- Lógica pura y probada en `ot_saldo_servicio.py`; las lecturas (base + ERP en SOLO LECTURA, REGLA #4.1) y el candado en `app.py` (`_ot_saldo_*`, `_ot_saldo_chequear`). Pruebas: `tests/test_ot_saldo_servicio.py`.
- **El candado está en todos los caminos**: asistente de crear y los otros dos núcleos (`_ot_validar_normalizar_finanzas`), ligar documento (`POST /ot/api/<vid>/documentos`, que también usa Regularizar), `asociar-factura`, declarar el cobro (`POST /ot/api/finanzas/<vid>`) y el modal de cierre con la firma (`ZZ_SALDO_CONSUMIDO`). Todo camino NUEVO que escriba lo cobrado o ligue un documento pasa por `_ot_saldo_chequear`.
- **Nunca un callejón sin salida**: cada rechazo trae las cuatro salidas (`acciones`): tomar solo el saldo (`tomar_saldo`), ligar otra factura, pasar a garantía o pedir autorización a Daniel con argumento (tipo `exceder_saldo`, autoriza hasta lo pedido). El asistente de crear no se bloquea entero. El rechazo jamás toca estado ni firmas.
- El motor de la OT (`/ot/api/<vid>/panorama`) muestra cuántos documentos hay, cuántos traen servicio y cuántos despacho, el saldo por línea y qué OT ya lo usa (número, cliente, enlace). Regularizar y Facturación de proveedor avisan si una OT usa saldo que otras OT también usan. El saldo es interno: nunca se le muestra al cliente.

---

## 🔒 REGLA #26 — Ningún técnico ve cuentas, deudas ni plata de la empresa (no negociable)

**Pedido explícito de Daniel (2026-10-08, revisión con Gerencia): «Anteriormente las órdenes de trabajo no exponían las deudas, las cuentas, nada a los técnicos externos. Así que tampoco a los internos… Cuidemos la imagen y la confidencialidad de la empresa, sobre todo cuando cae en el dashboard».**

«Técnicos» = TODA la familia: interno (`tecnico`), elevado (`tecnico_ejecutivo`: Jaizer, Lenin, Dave) y externo (`tecnico_externo`). Es la REGLA #19 llevada a la plata: un técnico NO recibe, en ninguna pantalla, API, Excel, PDF, notificación ni bitácora, montos cobrados al cliente, lo que cobran proveedores o técnicos, costos, márgenes, deudas, facturas de proveedor, N° de OC, valorizados ni autorizaciones de $0.

- **El dato se quita en el servidor**, no solo en la plantilla (lo que viaja en un `tojson` se lee con «ver código fuente»). Ayudas: `_es_rol_tecnico()`, `_oculta_proveedores()`, `_ot_sin_finanzas(v)`, `_ot_actividad_para_tecnico()` (bitácora: LISTA BLANCA, lo que no está se oculta aunque mañana alguien agregue una acción nueva), `_mant_notif_tecnico_ok()` (campana: solo lo suyo y sin plata), `_erp_doc_sin_montos()`.
- **Toda ruta nueva con nombre o ruta financiera** (`finanz|costo|factura|autoriz|regulariz|dashboard|facprov|margen|valoriz`) lleva `@_no_tecnico` (o `_require_superadmin`, `_ot_can_finanzas_cierre`, `_facprov_puede`…) o entra a la lista blanca de `tests/test_confidencialidad_financiera_tecnicos.py` con su razón verificada. Esa prueba recorre TODAS las rutas del proyecto y falla si una queda abierta.
- `@_no_tecnico` responde JSON 403 en cualquier ruta `/api/`; `_facprov_puede()` y `_tr_required` (Transporte) nunca dejan pasar a un técnico aunque su rol tenga el permiso encendido en `/admin/roles`.
- Gestión no pierde nada: lo que ve un ejecutivo/supervisor/admin/superadmin queda exactamente igual.

---

## 📒 REGLA #28 — Libro de saldo de facturas: cada factura se cobra una sola vez, y la pérdida la autoriza Daniel (no negociable)

**Pedido explícito de Daniel (2026-10-11, antes de una auditoría):** «si ya utilicé una factura y se ocupó el saldo en otra orden de trabajo, ese saldo tiene que quedar bloqueado… necesito decirle que mi sistema se encarga de eso». «Solo consume el saldo la OT, que es la realización del servicio; el ticket y la cotización heredan». «Te va a indicar si la factura va a pérdida y eso iría autorizado por mí».

- **Solo la OT consume** el saldo de una factura (servicio = líneas ZZ de instalación/mantención; despacho = ZZENVIO), con el MISMO motor de la REGLA #25 (`_ot_saldo_usos` / `_ot_saldo_doc`). El ticket y la cotización **heredan** la factura: la OT que nace de un ticket la trae elegida (`_o2mHeredarDocsTicket` en el asistente).
- **Libro inmutable** `ot_fac_libro`: cada cambio de lo que una OT toma de una factura es un movimiento (consumo + / liberación −) con OT, quién, cuándo y por qué. **Nunca** un `UPDATE` ni `DELETE` sobre esa tabla (hay prueba que lo vigila). Se registra solo: `_fac_libro_tras_escritura` (after_request, con huella barata) y el barrido cada 10 min colgado de `simpliroute-poll` (`_fac_libro_barrido`, que también libera OT borradas y relee facturas sin leer). Lo antiguo se reparte EXACTO (`aportes_reales_ot`: sin duplicar entre facturas ni pasar la capacidad); si Random no responde no se anota nada (`LecturaIncompleta`) y se reintenta. `ot_fac_doc` es la foto por factura para la pantalla **«Saldo de facturas»** (`/ot/saldo-facturas`) y el **Excel de auditoría**.
- **OT a pérdida = autorización de Daniel:** una OT que se cobra y cierra con lo cobrado por debajo de lo que nos cobraron por el SERVICIO (técnico + despacho; un despacho sin costo cuenta $0) no cierra sin la autorización `cerrar_con_perdida` (misma tabla `mant_ot_autorizaciones`), válida HASTA el monto que Daniel vio; si la pérdida crece, se vuelve a pedir. Los **repuestos** se muestran aparte y no entran a esa pérdida (la factura los cobra como producto). Un «cerrar sin documento» autorizado NO exime una pérdida si la OT tiene cobro (`_fac_perdida_aplica`). Candado en `mant_ot_aprobar_cierre` y en `mant_lev_cerrar` (`CIERRE_CON_PERDIDA`, con la acción para pedirla; a un técnico nunca se le muestran montos), aviso en el paso 6 del motor.
- **Solo superadmin (Daniel 2026-10-11, «los tres solo superadmin»):** «Regularizar OT», «Saldo de facturas» y la bandeja de «Autorizaciones» NO van en el menú lateral: son botones arriba en Órdenes de Trabajo (`ot2/panel.html`, `es_superadmin_ot`) y sus rutas llevan `@_ot_solo_superadmin`. El detalle de UNA solicitud lo ve quien la pidió y la gestión que puede aprobar esa OT (solo lectura).
- Todo camino NUEVO que escriba lo cobrado o documentos de una OT termina pasando por el registro de fin de petición (rutas `/ot/`, `/mantenciones/`, `/servicio-tecnico/`, `/tickets/`); si escribe fuera de esas rutas, llama `_fac_libro_marcar(vid)`. Pruebas: `tests/test_fac_libro_saldo.py`.

---

## ⚖️ REGLA #29 — Pago al proveedor: documento, saldo de la factura y punto de equilibrio (no negociable)

**Pedido explícito de Daniel (2026-10-12):** «el sistema por ninguna razón debe permitir que paguemos dos veces por un servicio… primero evaluamos la factura; si está repetida, evaluamos cuánto tiene de saldo; y el límite es el punto de equilibrio, donde no ganemos ni perdamos por el servicio. Si se pasa de allí, no puedes asignarla sin autorización». El documento de Random viaja de módulo en módulo (ticket → cotización → OT → factura de proveedor) y se controla en cada uno.

- **Una sola función decide:** `_fac_equilibrio_ot(vid, pago)` en `app.py` (cuenta pura en `ot_saldo_servicio.equilibrio_pago` / `reduccion_por_saldo`). En orden: 1) documento de Random ingresado (factura, boleta o nota de venta, vía `_ot_puerta_documento`) o $0 autorizado; 2) si la factura está **repetida** en otras OT, cuánto saldo le queda a esta (prioridad: las OT ya pagadas al proveedor y las anteriores); 3) lo que se le paga al proveedor (técnico + despacho; productos/repuestos aparte) ≤ lo que la OT cobra con respaldo de su factura.
- **Candado en los caminos que pagan:** asignar a un lote, «Solicitar OC», «Marcar pagado sin factura» y «Marcar pagada» (`_fac_equilibrio_rechazo` → 409 antes de escribir). Los lotes ya pagados no se tocan; «Cerrar histórico» (reconoce pagos ya hechos fuera del sistema) queda fuera.
- **Decisiones de Daniel (12-oct):** la autorización es la **misma** de pérdida del cierre (`cerrar_con_perdida`, válida hasta el monto que él vio; la solicitud toma la mayor entre la pérdida del cierre y la del pago); una OT en $0 pasa **si ese $0 está autorizado**; Random caído = no se paga a ciegas (se reintenta).
- La ficha del lote muestra en cada OT su factura, de qué venta viene (nota de venta ↔ factura), si está repetida y el equilibrio, con «Pedir autorización a Daniel». Pruebas: `tests/test_facprov_equilibrio.py`.
- **Sin documento NO frena el pago al proveedor** (Daniel, 12-oct: «no es culpa del proveedor si tiene o no factura»): se avisa en ámbar y la OT queda por regularizar. El documento se exige al CREAR la OT (REGLA #24): heredado del ticket o la cotización, o elegido de Random en el asistente, que muestra separado **productos** (no se cobran en la OT) / **servicio** / **despacho** (`desglose_documento`).
- **El margen del lote usa lo cobrado CON RESPALDO de la factura** (`_mfp_cobro_respaldado`, en `_mfp_items`/`_mfp_totales_lote`: fila, totales, KPI y Excel con la misma cuenta). Factura ya usada por otra OT o sin documento = no cuenta como cobrado; lo declarado queda a la vista aparte. Daniel: «es mi promesa de pago, no nos podemos equivocar».
- **Solo equipo interno:** todo esto es costo y margen: ningún técnico (interno, elevado ni externo) lo ve (REGLA #19/#26; doble candado `@_no_tecnico` + `_facprov_puede`, y `/ot/api/saldo-servicio` responde 403).
- **El lote sigue al costo corregido de la OT (2026-10-13, caso OT-2026-00264 / lote #18: pagaba $65.000 con el anexo firmado y la OT en $20.000).** Lo que se le paga por una OT en un lote PENDIENTE (`mant_factura_proveedor_items.monto`) se iguala solo al costo que la OT declara cuando ese costo se corrige (`_mfp_costo_previo` / `_mfp_costo_tras_escritura` → `_mfp_aplicar_costo_ot`, con constancia en las dos bitácoras y el total del lote al día si no tiene factura), **solo si el lote seguía a la OT** (montos iguales antes de la corrección): un monto escrito a mano al asignar no se pisa. Para esos, la fila «≠ declarado» trae «Usar el costo de la OT» (decisión de una persona, con su nombre). Lote pagado o anulado: nunca. Una OT en $0 no queda dentro de un lote (se quita).
- **Mostrar vs. candado (2026-10-12):** la ficha y el Excel del lote calculan el equilibrio sin salir a Random (`solo_cache`); con la caché fría usan la foto del libro (`ot_fac_doc`, `_fac_capacidad_persistida` / `_fac_familia_persistida`, refrescada por el barrido con el mismo motor). Sin foto leída dice «Sin verificar con Random» (nunca se inventa). Los candados de pago (asignar, OC, pagar) leen Random en vivo. Nada de hilos contra Random en una pantalla: cada proceso tiene solo 5 conexiones.

---

## 🧾 REGLA #30 — Crear una OT: la factura de la OT, el documento observado y la clasificación (no negociable)

**Pedido explícito de Daniel (2026-10-12):** «cuánto cobré, cuánto me va a cobrar el proveedor y cuánto es mi margen… cambiar el saldo a mano mientras el saldo lo permita», «si tiene más de tres meses de antigüedad… deberá pedir autorización y declarar un argumento persistente para que la OT quede guardada con ese antecedente y esa clasificación», «si tiene productos, deberían tener saldo» y «debemos guardar las clasificaciones, cómo se están trabajando las OT».

- **Factura de la OT (paso Costos del asistente):** una fila por factura con su saldo; cada fila arranca en su saldo y se puede bajar. **El cobro es siempre la suma de las filas.** Una fila sobre su saldo: si el total cabe, se mueve a otra factura; si no, autorización `exceder_saldo`. El reparto viaja como `finanzas.docs_tomas` y el servidor lo valida contra el saldo de cada factura (`ot_saldo_servicio.validar_tomas`, `ZZ_TOMA_SUPERA_FACTURA`). **Regla del 10 %:** un cobro editado a mano que mueve el margen más de 10 puntos respecto del saldo de las facturas pide argumento (`ajuste_margen`, `FINANZAS_AJUSTE_SIN_ARGUMENTO`). La tabla grande de líneas y qué OT usa cada factura: solo superadmin.
- **Documento observado:** al crear desde «Nueva OT» (`revisar_documento=True`, SOLO ese núcleo: las OT históricas, retroactivas y el plan anual no), cada documento de Random se revisa en solo lectura (`_ot_doc_observar`): **emitido hace más de 3 meses** (hora Chile) o **todos sus productos sin saldo en Random** (ya despachados o anulados). Si pasa, no se crea sin la autorización `documento_observado` (argumento ≥ 30 caracteres; la aprueba Daniel a distancia o la declara el superadmin que crea). La solicitud guarda las observaciones **calculadas en el servidor** (`_observaciones_doc`) y la aprobación cubre exactamente esas (`_ot_doc_obs_autorizada`); si aparece otra, se vuelve a pedir. Una nota de venta cuya factura va en la misma OT no se observa. Si además se cobra sobre el saldo, la misma solicitud `exceder_saldo` lleva las observaciones (una autorización por OT). Interruptor (solo con permiso de Daniel): `OT_DOC_OBSERVADO=0`.
- **Clasificación persistente:** al crear, la OT guarda cómo nació (`mant_visitas.fin_clasif` + `fin_clasif_json`, `_ot_fin_clasif_guardar`): factura con saldo, nota de venta/cotización/guía (falta factura), $0 (garantía, regalía, arriendo/leasing, mantención de contrato: misma regla que `_ot_cobertura`), sin documento autorizada, documento observado autorizado, cobro sobre el saldo autorizado, ajuste a mano con argumento, interna. Con el argumento, quién autorizó y cuándo (hora Chile). En la bitácora general queda solo la constancia de la autorización (`documento_observado_autorizado`), no la clasificación. Las OT anteriores se calculan con lo que ya tienen y lo dicen («calculada»): nunca se inventa un antecedente.
- **Quién la ve:** solo el superadministrador, en la ficha, la tabla de OT, el Excel de OT (columna + hoja «Clasificación») y Facturación de proveedor. Nunca un técnico (REGLA #26: el dato se quita en el servidor).
- **Camino al margen:** el paso Costos muestra 5 pasos con semáforo (documento → saldo de la factura → antigüedad y productos → cobro y costo → margen). Solo informa: los candados son los del servidor.
- Pruebas: `tests/test_ot_documento_observado.py`, `tests/test_ot_costos_asistente.py`.

---

## 🎨 REGLA #27 — Las skills de diseño externas (Impeccable, UI/UX Pro Max, SkillUI, Playwright) se SOMETEN a las reglas del proyecto

**Pedido explícito de Daniel (2026-10-09): «la UI/UX debe someterse a las reglas del proyecto».**

Cualquier skill, plugin o herramienta externa de diseño que se instale propone, audita o prueba; **nunca decide**. Si lo que sugiere choca con una regla de este archivo, **gana la regla**:

- **Paleta y marca:** REGLA #0 y #2 (rojo `#dc2626`, negro `#0a0a0a`, blanco; ILUS Fitness). No se aceptan paletas, tipografías de marca ni «direcciones visuales» nuevas de una skill.
- **Lenguaje visual:** REGLA #15 (formato Retiros: tarjetas, semáforo, datos completos sin truncar), #16 (grillas de 2 columnas), #4.3 (tablas paginadas), #14 (marcar/desmarcar todo), #3 (mobile-first) y #1 (nada de `alert/confirm/prompt` nativos).
- **Confidencialidad:** REGLA #19 y #26 (técnicos no ven plata ni proveedores) no se relajan por «claridad» visual.
- **Cómo se usan:** Impeccable y Playwright en modo **auditoría primero** (lista de problemas ordenada, sin tocar nada); las correcciones se aplican de a una y se verifican en navegador real. UI/UX Pro Max y SkillUI solo como **referencia**: no reemplazan el sistema de diseño de ILUS (`ilus_design_system` en memoria) ni se copian estilos de webs externas literalmente.
- **Regla #4.2 sigue vigente:** una auditoría que sugiera «quitar» una columna, botón o módulo se pregunta antes; no se borra.
- Probar siempre con datos de mentira o en vista previa local (REGLA #22: cero correos o cambios visibles para clientes reales).

---

## 🗓️ REGLA #31 — Feriados de Chile: una sola fuente (`cl_feriados.py`), y todo calendario marca los días no laborales

**Pedido de Daniel (2026-10-14): «mejorar el calendario de las Órdenes de trabajo considerando los días feriados o no laborales en Chile y que tenga una hermosa vista responsiva».**

- **Una sola fuente:** `cl_feriados.feriados_chile(año)` (la usan Retiros, el planificador de mantenciones y el calendario de OT). Nunca una lista de feriados propia en otro módulo. Cada año se carga la lista oficial en `FERIADOS_OFICIALES` (feriados.cl); sin ella, el cálculo aplica los traslados de ley (San Pedro y San Pablo y Encuentro de Dos Mundos al lunes, Iglesias Evangélicas, Pueblos Indígenas, 17 y 20 de septiembre). Un feriado por ley especial (elecciones, puentes) no se adivina: se agrega a la lista oficial cuando se publica.
- **Calendario de OT** (`/ot/?vista=calendario`, `_ot2_cal_no_laborales` + `templates/ot2/panel.html`): cada día marca feriado (con su nombre) o fin de semana, cuenta días hábiles del mes y los que quedan, muestra el próximo feriado y **avisa las OT agendadas en un día no laboral** (ILUS no opera sábado ni domingo). Solo informa: no bloquea agendar.
- **Celular (REGLA #3):** la grilla queda compacta con puntos de color y debajo va una **agenda por día** con los nombres completos (REGLA #15); los filtros se pliegan detrás de un botón «Filtros» (no se quitaron, REGLA #4.2); el modal del día es pantalla completa. Pruebas: `tests/test_ot_calendario_feriados.py`.
- **El calendario del asistente «Generar OT» también marca los feriados (2026-10-24; desde el 25-oct es UNO solo para la entrada y la salida, ver REGLA #43):** dibuja el feriado en rojo suave con su bandera y su nombre al pasar el mouse (`GET /servicio-tecnico/api/calendario/mes/<año>/<mes>` trae `feriados` del mes, de `cl_feriados`); agendar en un feriado se avisa («Cae en feriado (nombre)»; para la salida lo avisa el servidor, «Es feriado: …») pero nunca se bloquea. Pruebas: `tests/test_ot_instalacion_con_desinstalacion.py` (clase `TestFeriadosEnLosCalendariosDelAsistente`).

---

## 🎛️ REGLA #32 — Checklists y plantillas: el sistema SUGIERE, el usuario DECIDE (regla estricta, no negociable)

**Pedido explícito de Daniel (2026-10-19 y 2026-10-20, OT 328/329 de la revisión NVI 687):** «necesito que se controle por el front, tú solo debes mostrar las opciones… el usuario es el que decide qué plantilla o checklist… para todo» y «como es regla estricta del proyecto, no debes imponer o gestionar más checklists, sino los que impone el usuario».

Incidente real: el asistente sumaba por detrás «Revisión — Registro por equipo» y «Revisión — Estructura y tapiz» a la plantilla «(LU) …» de Lenin que se veía elegida en cada fila; las OT 328/329 nacieron con ~480 tareas en vez de 7-9 por producto.

- **Lo que se ve elegido en pantalla es EXACTAMENTE lo que recibe el técnico.** Nada se agrega por detrás: ni una «base», ni un «condicional», ni una plantilla estándar que la persona no vio.
- **El servidor nunca reemplaza ni completa** la elección: solo valida (activa, con ítems, categoría o mapeada al tipo) y, si algo no se puede aplicar, lo devuelve como **aviso visible**, nunca en silencio. Código: `_ot2_crear_core` (bloque «checklist HEREDADO») y `_o2mChkIds` en `templates/ot2/_modal_crear.html`.
- **Sugerir sí, con su origen a la vista** («Sugerida según su clase», «Guardada para este producto», «Elegida a mano»), y la persona puede cambiarla o quitarla.
- **Ninguna plantilla se modifica sola** (arranque, migración, semilla, sincronización): cambiar el contenido de una plantilla existente es una acción de una persona en Plantillas, con vista previa y confirmación. Las plantillas «(LU) …» de **Lenin Urbina** jamás se modifican desde el código (única excepción: el estándar N° de serie + GPS de la REGLA #39, que Daniel autorizó el 2026-10-21 y solo se aplica con su confirmación expresa). Sembrar una plantilla NUEVA solo si no existe por nombre sí está permitido.
- Aplica a TODO tipo de OT (instalación, mantención, revisión, trabajo interno…) y a todo camino que cree o agregue tareas. Pruebas: `tests/test_ot_checklist_herencia.py`.
- **Pestaña «Desinstalación» y «Mover a…» (Daniel 2026-10-25: «¿dónde se guardan las plantillas de desinstalación?… agrega una que diga Desinstalación… reducirle las cantidades a Trabajo interno»).** `categoria_admin` tiene un 5.º valor, `desinstalacion` (agregado al FINAL del ENUM de `mant_tarea_plantillas` y de `mant_categoria_tipo_map`; nunca reordenar). Ninguna plantilla se cambia sola de pestaña: en «Desinstalación» un aviso lista las de desinstalación que viven en otra pestaña con «Traerlas» (también pasa el tipo de OT «desinstalacion» a buscar ahí) y la barra de selección tiene «Mover a…» (`POST /mantenciones/api/plantillas/mover-categoria`): solo cambia la pestaña, nunca el checklist, y cada movimiento queda en `mant_logs`. La cobertura por clase sigue con sus 4 categorías (`_PLCLS_CATEGORIAS`). **Plantillas cortas:** ≤ 10 tareas contando N° de serie y GPS, una por tipo de equipo (Daniel 2026-10-25). Pruebas: `tests/test_plantillas_desinstalacion_modulo.py`.

---

## 📬 REGLA #33 — Todo correo nuevo nace en el REGISTRO, con su clave, y sale por la puerta única (no negociable)

**Pedido de Daniel (2026-10-20): «el control de correos en Comunicaciones»** — una pestaña donde el superadmin ve TODOS los correos que envía el sistema, quién los recibe, qué los dispara, y puede **apagar cada uno** (con motivo), ver su estado real, la vista previa, mandarse una prueba y ver cuántos salieron o se frenaron. Las órdenes de trabajo forman su propio grupo.

- **Un solo registro:** `comm_correos_registro.py` (puro, con pruebas). Cada correo tiene su **clave** (`ret_cita_confirmada`, `ot_tec_asignada`…), su nombre completo, su grupo de la pantalla (`modulo_ui`), la llave de paso que ya lo gobierna (`llave` = el `modulo=` con que sale), **quién lo recibe** (`destinatario`: cliente · tecnico · responsable_ot · bodega · proveedor_courier · interno · usuario), qué lo dispara y su plantilla. Las instrucciones para sumar uno nuevo están en el encabezado de ese archivo.
- **Un correo NUEVO se registra ANTES de enviarse** y lleva su clave a la puerta única: `_send_ilus_email(..., modulo=…, correo="clave")` (o `correo=` en la función que lo envuelve: `_mant_send_email_multi`, `_send_pickup_email_multi`, `_prospecto_correo_por_ticket`, `_ot_tec_enviar`). **Nunca** dentro de `**kwargs` ni de `kwargs_json` de la cola (se reenvía al envío real, que no la conoce → `TypeError`): viaja en la columna propia `comm_email_jobs.correo_clave`. `tests/test_comm_control_correos.py` recorre TODOS los `.py` y falla si una llamada de envío no lleva una clave registrada (o no está en su lista de excepciones con razón). Si el envío vive en otra rama que aún no pasa `correo=`, basta registrar su evento (constante) en `_EVENTO_A_CLAVE` del registro: la puerta lo resuelve sola y la prueba lo acepta (caso `ot_que_paso_bodega`, «Falta de stock: aviso a bodega»); al mezclar conviene agregar `correo="clave"` igual.
- **Orden de revisión (fijo):** interruptor general > llave del módulo > **interruptor de ese correo** > cola. Sin fila en `comm_correo_control` = **ENCENDIDO** (lo que se apaga por código, variable o llave sigue apagado). Si la tabla no se puede leer se usa el último valor conocido; si nunca se leyó, el correo sale (igual que las llaves).
- **El interruptor SOLO apaga.** Un correo que hoy apaga una variable de entorno (`RETIROS_FIRMA_CORREO`, `MANT_RECORDATORIOS_EMAIL_ENABLED`, `PROSPECTOS_OFERTA_AUTO`…) se muestra «Apagado por configuración» y **no se enciende desde la pantalla**: sigue pasando por la variable y por el «sí» explícito de Daniel (REGLAS #22 y #23). Los correos de seguridad (`acceso_invitacion`, `acceso_cambio_clave`, `acceso_olvido_clave`) son `protegido`: no tienen interruptor y la puerta ignora cualquier fila que los apague.
- **Un correo apagado deja rastro:** queda en `email_log` como `bloqueado` («Apagado en Control de correos: «Nombre»»), con su cuerpo y su clave en `correo_clave`; el Historial lo muestra. Cada cambio de interruptor queda en auditoría (`comm_correo_set`: quién, cuándo, motivo). «Reintentar» respeta el interruptor. «Enviarme una prueba» va **solo** al correo del usuario conectado, con «[PRUEBA]» en el asunto.
- **`_tr_notificar_cliente` no se toca** (otras pruebas la comparan con `origin/main`): sus seis estados se resuelven por su evento `tracking_<estado>` con `comm_correos_registro.inferir()`.
- **Avisos que envía un TERCERO (FedEx, SimpliRoute; `tipo="externo"` en el registro):** ILUS no manda ese correo: le **entrega** al tercero el correo del destinatario (en la guía, el retiro, el Excel de carga o la visita) y el tercero avisa. El interruptor decide si se lo entrega; la guía, el retiro o la visita salen igual, sin ese correo. **Obedecen al interruptor general y a su propio interruptor** (decisión de Daniel, 20-oct-2026), **no a la llave de paso del módulo**. Se consultan con `_correo_externo_bloqueo(clave)` / `_correo_externo_permitido(clave)` —siempre fail-open: un error al leer jamás impide crear una guía— y lo frenado queda en el Historial (`_correo_externo_frenado`). Un aviso nuevo de un tercero se suma al registro con `tipo="externo"` y se consulta justo donde ILUS arma lo que le manda; el gancho de SimpliRoute es `simpliroute_client.CONTACTO_EMAIL_PERMITIDO` (el módulo sigue puro). **Clickex no tiene interruptor a propósito:** su API exige el correo del destinatario para crear el envío (`clickex_client.build_shipment_payload`), así que quitárselo rompería el envío; si Daniel lo quiere, se decide aparte.
- Solo superadmin. Un técnico nunca ve esta pantalla ni montos (REGLAS #19 y #26). Pruebas: `tests/test_comm_control_correos.py`.
- **Correos que NACEN APAGADOS (Daniel 26-oct-2026: «agrégalos apagados para encenderlos desde el front»):** un aviso INTERNO nuevo se
  registra con `nace_apagado=True`: sin fila en `comm_correo_control` NO sale (`comm_correos_registro.switch_de_fila`), la pantalla lo
  muestra «Nace apagado: enciéndelo cuando quieras» (ficha «Nuevos: nacen apagados») y una persona lo enciende o apaga ahí. Solo para
  avisos internos: a un cliente nada se enciende desde la pantalla (REGLA #22). Hoy: inquietud nueva / asignada / plazo vencido y hallazgos
  nuevos / tomado (`_aviso_interno_correo`: copia por correo de la campana, sin montos, nunca a un técnico).

---

## 🧾 REGLA #34 — El costo de la OT y el anexo del proveedor son UN solo valor (no negociable)

**Pedido explícito de Daniel (2026-10-20), con la OT 239 y el Anexo 221 firmado por $370.000 mientras la OT decía «costo no declarado»: «no son dos valores, es uno solo… lo que estamos pagando tiene que estar reflejado allí… hazlo obligatorio».**

- Lo que ILUS le paga al proveedor en una OT (`costo_proveedor` + `costo_despacho`) ES el total de su anexo **vivo** (no rechazado ni anulado): un ítem «Despacho» es el `costo_despacho` y el resto el `costo_proveedor`. Lógica pura y probada en `ot_anexo_costo.py`; en `app.py`: `_ot_anexo_vivo`, `_ot_anexo_alinear_costo`, `_ot_anexo_costo_guardia`.
- **El anexo manda:** al crearlo, corregirlo o firmarlo, la OT queda con el mismo costo (constancia en la bitácora; el lote de Facturación de proveedor lo sigue: REGLA #29). Una OT abierta con el costo sin declarar y un anexo vivo se completa sola al abrirla y al aprobar el cierre.
- **Mientras haya anexo vivo, el costo no se escribe «a mano» por otro camino:** `POST /ot/api/finanzas/<id>`, `POST /ot/api/<id>/costo-proveedor` y el PUT de la OT responden 409 `COSTO_ES_DEL_ANEXO` si el cambio no suma lo mismo que el anexo (volver a guardar lo mismo no se bloquea). Para cambiarlo: corregir el monto en el anexo (si no está firmado) o emitir uno nuevo (si ya está firmado). Si la OT no coincide, la ficha ofrece «Usar el valor del anexo» (`POST /ot/api/<id>/costo-desde-anexo`).
- Una OT cerrada es evidencia: la alineación automática nunca la toca. Si una cerrada difiere de su anexo FIRMADO, solo el superadmin la corrige con «Corregir con el anexo» (`POST /ot/api/<id>/costo-desde-anexo/corregir`, primero simula y muestra cómo queda la ganancia), por el mismo camino y con el mismo registro inmutable de «Corregir finanzas» (`mant_ot_finanzas_correcciones`). Un técnico nunca ve nada de esto (es plata: REGLA #26).
- **La franja «Un solo valor / No coincide» y su botón se dibujan en el bloque único «Costos y documentación» (existe también en una OT CERRADA) y en el Paso 2 de la edición (que una OT cerrada no dibuja)**: un solo macro, `templates/ot2/_anexo_vinculo.html`. Lección del 20-oct: la primera versión solo vivía en el Paso 2 y en las 4 OT cerradas que había que corregir el botón nunca aparecía, aunque las pruebas de texto pasaban. `tests/test_ot_anexo_vinculo_franja.py` renderiza el macro de verdad y revisa desde qué condiciones lo llama la ficha.
- **En la mira para pagar y medir:** la fila de cada OT en Facturación de proveedor dice «Coincide con el Anexo N° X» o, si no, «Lo que se paga no coincide con el Anexo N° X» con las dos cifras; el costo de la OT alimenta el pago (REGLA #29) y el margen, así que son lo mismo.
- Todo camino NUEVO que escriba el costo de una OT pasa por `_ot_anexo_costo_guardia`; todo camino que cambie un anexo llama a `_ot_anexo_alinear_costo`. Las OT que ya difieren se listan (solo superadmin) en `GET /ot/api/anexos/vinculo-costos`. Pruebas: `tests/test_ot_anexo_costo.py`.

---

## 🧭 REGLA #35 — «¿Qué pasó?» en la OT: lo que no se pudo hacer se REGISTRA, y lo que falta por falta de stock vive en una OT hija sin fecha con el MISMO documento (no negociable)

**Pedido explícito de Daniel (2026-10-20), con el caso real de Aurum Fit Girls Spa (OT-2026-00171): dos accesorios no llegaron por falta de stock y la OT no se podía completar. «Así como planteamos en Retiros… ¿pasó algo?»**

- **Qué es:** la franja «¿Qué pasó?» de la ficha de toda OT abierta (`templates/ot2/_que_paso.html`, `static/ot_que_paso.js`; reglas puras y probadas en `ot_que_paso.py`; rutas `GET/POST /ot/api/<id>/que-paso`). La usa el técnico asignado (también el externo), el ejecutivo creador y la gestión (`@_ot_can_configurar`). **Motivo obligatorio de una lista** (con «Otro motivo» el detalle es obligatorio); queda quién, cuándo, motivo y detalle en `mant_ot_que_paso` y en la bitácora (REGLA #24). Una OT terminada o esperando su aprobación es de solo lectura (409).
- **Falta de stock = se cierra lo hecho + OT hija «pendiente»:** los equipos que la persona ELIGE (el sistema solo sugiere los que ya estaban «falta stock»: REGLA #32) quedan «falta stock» y dejan de frenar la firma y el cierre; sus tareas SIN HACER pasan a una OT hija (`ot_relacion='pendiente_stock'`) que nace **sin fecha**: `mant_visitas.fecha_programada` es NOT NULL y miles de consultas la leen, así que lleva la fecha centinela 2099-12-31 (`ot_que_paso.FECHA_SIN_FECHA`), que el evento, el calendario y los correos al técnico tratan como «sin fecha» hasta que la gestión la reagenda (el Reagendar de siempre) y le asigna técnico. Si la madre ya tiene una pendiente sin empezar, se amplía en vez de crear otra. Todo en UNA transacción: o queda todo o no cambia nada.
- **Mismo documento, sin cobrar de nuevo (REGLAS #24, #25, #28):** la hija nace SIN documento ni cobro propios y pasa por la puerta del documento por la vía `pendiente_madre` (`_ot_puerta_via_pendiente`): el documento de su madre queda ESPEJADO (`es_cobro=0`), no consume saldo ni entra al libro. Si la madre no tiene un documento de Random (ni una cotización ni una autorización de Daniel) no se crea nada; al CERRAR la pendiente se exige la factura o boleta de la madre (o su autorización de cerrar sin documento). Hoy la pérdida (REGLA #28) se mide sobre la pendiente sola, que cobra $0: si a un técnico externo se le paga por ella, pide la autorización de Daniel. Pendiente de decidir con él si la pendiente entra a la «familia» de su madre, como la desinstalación.
- **Avisos (alertar, no trabar):** la gestión por la campana (`ot_pendiente_stock`) y bodega por correo INTERNO (`ot_que_paso_bodega`, módulo `comunicacion_interna`, por la puerta de siempre `_send_ilus_email`: interruptor general + llave del módulo, queda en el Historial). A quién avisa «la gestión» lo decide UNA sola función, `_ot_gestion_destinatarios` (la misma lista alimenta la campana y el «Avisar a» del correo; cuando exista el responsable de la OT, entra ahí y nada más cambia). **Al cliente jamás se le escribe** (REGLA #22). El modal dice, antes de guardar, a quién le llega cada cosa.
- **Confidencialidad:** el servidor no manda a un técnico ni plata, ni documentos, ni saldo, ni proveedores de esto (REGLAS #19 y #26); el enlace a la OT madre/pendiente va como texto si el técnico no puede verla.
- Todo camino NUEVO que deje trabajo pendiente en otra OT pasa por `_qp_aplicar` (nunca crea la hija a mano). Pruebas: `tests/test_ot_que_paso.py`.

---

## 🙋 REGLA #36 — Cada OT tiene UN responsable (quien la lleva y recibe sus avisos de gestión), y el permiso «Responsable de OT» dice quién puede serlo (no negociable)

**Pedido explícito de Daniel (2026-10-20): «¿los avisos le llegan a la persona que administra las órdenes de trabajo (Víctor, Aarón o Juan Pablo)? Quiero que ese rol de responsable de la OT quede bien establecido».**

- **Quién puede serlo = un PERMISO, no un rol:** `mant_ot_responsable` («Responsable de OT»: Roles → Servicio Técnico, o por persona en «Permisos individuales»), **apagado para todos** y encendido solo para Víctor, Aarón y Juan Pablo. Candidato = usuario ACTIVO con el permiso **explícito** (la fila de la persona manda sobre la de su rol). El superadministrador **no** es responsable por tener todas las llaves (por eso la lista NO se arma con `g.permissions`) y la familia técnico **nunca** (REGLA #19/#26). Reglas puras y probadas en `ot_responsable.py`; el SQL en `_ot_resp_candidatos`.
- **Mientras nadie tenga el permiso, todo está DORMIDO:** el asistente no muestra el paso, el panel no dibuja columna ni filtro, la ficha no dibuja el chip, no se escribe nada y los avisos siguen yendo como siempre (creador + ejecutivo del cliente + respaldo). Encenderlo es una acción de Daniel, por el front.
- **Al crear (UNA puerta: `_ot_responsable_al_crear`):** 1) la hija de una desinstalación hereda el de su instalación (`ot_madre_id`); 2) lo que la persona eligió en el asistente (se **revalida** en el servidor); 3) el defecto de `_ot_responsable_por_defecto`: quien crea la OT si tiene el permiso, si no el ejecutivo del cliente (`mant_clientes.ejecutivo_user_id`) si también lo tiene; 4) nadie: la OT queda sin responsable. El asistente (`templates/ot2/_modal_crear.html`, paso «Responsable» entre Cliente y Detalle) **sugiere con su origen a la vista y la persona decide** (REGLA #32); si no hay sugerencia, PIDE elegir. Si la OT nace de una autorización aprobada, «quien crea» es quien la pidió. **Todo camino que inserta en `mant_visitas` llama a esa puerta** después de su commit (hay una prueba que lo recorre; las excepciones están escritas con su motivo). Nunca bloquea ni deshace la creación. Fija el responsable solo si la OT aún no tiene (`responsable_user_id IS NULL`).
- **Cambiarlo:** `POST /ot/api/<id>/responsable` (`{user_id, motivo?}`). Lo hace la gestión (superadmin/admin/supervisor) o el propio responsable actual; el nuevo debe ser candidato y distinto del actual; una OT cerrada, cancelada o anulada solo la mueve el superadministrador. Queda en la bitácora (`responsable_asignado` / `responsable_cambiado`: quién, cuándo —hora Chile—, de quién a quién y el motivo) y se avisa por la **campana** (tipo `ot_responsable`, sin correo ni WhatsApp) a la persona nueva («Ahora estás a cargo de OT-…») y a la anterior («OT-… pasó a cargo de …»), con tono amable: nada que suene a castigo (`ot_responsable.PALABRAS_PROHIBIDAS_EN_AVISOS`). **Al cliente no le llega nada** (REGLA #22). El UPDATE es NULL-safe (`responsable_user_id <=> …`): dos personas a la vez no se pisan.
- **A quién avisa «la gestión» sigue decidiéndolo UNA función, `_ot_gestion_destinatarios`:** el responsable va PRIMERO, luego quien creó la OT y el ejecutivo del cliente; de respaldo, superadmin/admin/supervisor. La usan el aviso «OT lista para aprobar» (`_notificar_ot_pendiente_aprobacion_async`), la campana y el correo interno de «¿Qué pasó?» (REGLA #35) y cualquier aviso nuevo: **nadie fija a mano a quién avisar**. Las OT anteriores a esta regla **NO se rellenan** (nada de UPDATE masivo): con la columna en NULL todo cae al respaldo de siempre.
- **Dónde se ve:** chip «Responsable» en la cabecera de la ficha (`responsable` en `ot2_detalle`; un técnico lo ve SOLO como nombre, sin quién lo asignó ni cuándo; su bitácora lo oculta por la lista blanca); columna «Responsable» y filtro (**«Mis OT como responsable»**, «Sin responsable» y cada persona) en la tabla de OT, con el parámetro `resp` en todos los enlaces y en el Reporte Excel (columna al final). Nombres completos, sin truncar (REGLA #15).
- **Datos:** `mant_visitas.responsable_user_id` (índice `idx_v_responsable`), `responsable_nombre`, `responsable_at` (UTC, como `NOW()`) y `responsable_por`, creadas por `_ensure_ot_responsable` (REGLA #18: una cláusula por sentencia, por `mysql_execute`; barato si ya existen). Mientras faltan, `_ot_resp_ok()` salta todo sin error. La historia completa vive en `mant_logs`.
- **Cómo se enciende (solo con Daniel):** `/admin/roles` → el botón de **Permisos individuales** de cada una de las tres personas → activar `mant_ot_responsable` → Guardar (o, si comparten un rol que solo ellas tienen, la matriz del rol → Servicio Técnico → «Responsable de OT»). Se apaga igual. Pruebas: `tests/test_ot_responsable.py`.

---

## REGLA #37 — Comunicación de la OT: «OT terminada» al aprobar el cierre, «Enviar OT por correo» a mano y los destinatarios SIEMPRE a la vista (no negociable)

**Pedido explícito de Daniel (2026-10-20): «necesito que esto sea controlable, que avises cuándo lo tienes que enviar y además habilita, así como las cotizaciones, un envío por mail directo de las órdenes de trabajo… y quiero que avise a quién se le está enviando: al momento de crearse y enviarse los correos se vaya educando el personal de cuándo genera comunicación tanto con los técnicos como con los clientes»; y «cuando se termine se envíe automáticamente por correo, y que se alerte que se enviará la información por correo».**

- **«OT terminada» automática, UN solo punto:** `_ot_terminada_notificar(vid, origen)` en `app.py` (la lógica pura —a quién, en qué estado queda cada aviso, textos, plantillas— vive en `ot_comunicacion.py`, con pruebas). Se llama al **APROBAR el cierre** (`mant_ot_aprobar_cierre`, y `mant_visita_cerrar`, el cierre auditable): la OT ya es definitiva y su documento no cambia. **Una sola vez por aprobación** (tabla `mant_ot_comunicaciones`, clave única OT + `cerrada_at`; si una OT se reabre y se vuelve a aprobar, es otra aprobación y otro aviso), **best-effort** (jamás rompe el cierre) y el cierre de un levantamiento no lo dispara. Reemplaza a `_notificar_cierre_ot_async`, que se borró: no resucitarlo ni su variable `MANT_NOTIF_CLIENTE_EMAIL_ENABLED`. Avisa a: (a) el **cliente** (el documento firmado sin montos, por el enlace público `/ot-firmada/<token>`, vigente 30 días); (b) quien **creó la OT / el ejecutivo / el responsable**, que los decide **solo** `_ot_gestion_destinatarios` (nunca armar otra lista), con el resumen de lo que se envió; (c) el **técnico** que la hizo (su copia, sin montos ni proveedores, por su llave `ot_tecnicos`). Un trabajo interno y una desinstalación no le escriben al cliente.
- **El correo al CLIENTE nace APAGADO (REGLA #22).** Solo sale si `OT_TERMINADA_CLIENTE_ACTIVA` vale `1`/`true`/`yes`/`on`/`si`, y eso lo decide únicamente Daniel con su «sí» en ese mensaje: **hoy no está en `deploy.yml` ni en ningún otro archivo del repositorio** (una prueba lo vigila, igual que `RETIROS_FIRMA_CORREO`). Cuando él lo autorice se enciende como `RETIROS_ENCUESTA_ACTIVA`: `append_env OT_TERMINADA_CLIENTE_ACTIVA "1"` en `deploy.yml` (con su nombre en el `grep -vE` de limpieza, para no duplicarla) y ajustar esa prueba. Ponerla a mano en la consola de Cloud Run NO sirve: cada deploy usa `--env-vars-file` y reemplaza todas las variables. Control de correos lo muestra «Apagado por configuración» y no se enciende desde la pantalla (REGLA #33). **Sin envío retroactivo:** una OT cerrada antes no recibe nada. Mientras esté apagado, la ficha de la OT terminada muestra una franja ámbar «La OT terminada todavía no se le envió al cliente · [Enviar OT por correo]» (recuerda, nunca bloquea; no se inventa para las OT cerradas antes de que existiera esto).
- **Se avisa ANTES de enviar (alertar, no sorprender):** el técnico al firmar y la gestión en «Aprobar cierre» leen «Al aprobar el cierre se enviará esta OT a: Nombre (Cliente) · Nombre (Técnico) …», con lo que esté apagado dicho en claro («el correo al cliente está apagado: no se enviará hasta que se active», la llave cerrada, sin correo registrado…). `GET /ot/api/<id>/correo/destinatarios` usa el **MISMO plan** (`armar_plan_cierre`) que el envío real: lo que se promete es lo que se hace. Al técnico no le viaja el correo de nadie ni el nombre de un proveedor. **Al crear la OT** el asistente dice «Al crear esta OT se enviará: …» antes de confirmar (`POST /ot/api/correo/creacion`) y después «Se envió a … / quedó frenado: …»; solo informa, no cambia cuándo sale ningún correo.
- **«Enviar OT por correo» (manual, como «Enviar cotización»):** botón en la ficha de una OT cerrada, **solo gestión** (superadmin, admin, supervisor, ejecutivo: el permiso de aprobar; un técnico nunca envía a un cliente). El modal propone destinatarios prellenados y editables (cliente, contraparte de la OT, técnicos, ejecutivo/responsable; el respaldo general de gestión no se propone), deja «agregar otro correo» (validado, sin repetidos, máximo 15), asunto y mensaje opcionales, vista previa y «Se enviará a: nombre (rol) · …» explícito, con «marcar/desmarcar todos» (REGLA #14). El documento es el mismo que ve el cliente, sin montos. Va un correo por destinatario, sin cola, por la puerta única, y después dice **la verdad por destinatario** («Se envió a …» / «No salió porque …»; hoy la llave de Servicio Técnico está cerrada «por pruebas» y frena los correos del grupo `mantenciones`). Quien sea técnico (de esa OT o de cualquiera) recibe SIEMPRE la versión sin plata y por la llave `ot_tecnicos`. Queda en la bitácora (`ot_enviada_correo`), en `mant_ot_comunicaciones` y en el Historial de Comunicaciones con su referencia a la OT. `mant_visita_enviar_email` sigue igual.
- **Registro (REGLA #33):** claves `ot_terminada_cliente`, `ot_terminada_responsable`, `ot_tec_terminada`, `ot_envio_manual` y `ot_tec_envio_manual`, todas con `correo="clave"` por `_send_ilus_email` / `_ot_tec_enviar`. Plantillas editables en Comunicaciones → Plantillas (grupo «OT terminada», módulo `mantenciones`: `ot_terminada`, `ot_terminada_interno`, `ot_terminada_tecnico`, `ot_envio_manual`); se siembran solo si NO existen, nunca pisan lo que Daniel editó.
- **Confidencialidad (REGLAS #19 y #26):** ningún correo de esto lleva montos, costos, márgenes, proveedores, N° de OC ni autorizaciones; el dato se quita en el servidor, no en la plantilla. Las acciones de bitácora (`ot_terminada_aviso`, `ot_enviada_correo`, `ot_creada_aviso`) no están en la lista blanca del técnico: él no ve a quién se le escribió.
- **Arranque barato (REGLA #18):** una sola tabla nueva (`CREATE TABLE IF NOT EXISTS mant_ot_comunicaciones`) y la siembra de las 4 plantillas con `INSERT IGNORE`; sin `ALTER` ni llave de paso nueva. Un camino NUEVO que envíe correos de una OT pasa por la puerta única con su clave registrada, y se prueba **solo** con correo y datos de mentira (REGLA #22). Pruebas: `tests/test_ot_comunicacion_envio.py`.

---

## 🩺 REGLA #38 — Hallazgos de la OT: lo que el técnico reporta AVISA a la gestión, se «toma» y se convierte en OTRA OT de seguimiento (solo avisa, no traba; no negociable)

**Pedido explícito de Daniel (2026-10-20), revisando la OT de Aurum (OT-2026-00171): «Indícame si se está tomando fotografías como evidencia, eso es necesario, y que la OT avise para que se tome el caso y se levante una acción que debería ser otra OT».** Hasta entonces una Falla o una Alerta del checklist, un equipo «que no se puede usar», una baja, un repuesto pedido o una incidencia quedaban ENTERRADOS en la OT: nadie recibía la campana, nadie «tomaba el caso» y no nacía ninguna OT.

- **Qué es un hallazgo (ningún concepto nuevo: sale de lo que el técnico ya registra):** una tarea de checklist de tipo verificación respondida **Alerta o Falla**; el **«Resultado del control»** (lista con «Apto» y «Apto con observaciones») con una opción distinta de «Apto» y «No llegó» (Reparar → falla, Dar de baja → baja, Fuera de servicio, Pendiente de diagnóstico → alerta); y las acciones de equipo y producto **«No se puede usar»**, **«No tiene arreglo»**, **«Pedir repuesto»**, **«Informar incidencia»** y un equipo con falla detectada. **«No llegó · falta stock» NO es un hallazgo** (lo cubre «¿Qué pasó?», REGLA #35). Las reglas puras viven en `ot_hallazgos.py` (con pruebas).
- **La foto es la evidencia:** cada hallazgo muestra sus fotos (las de una tarea se releen vivas: el técnico las sube DESPUÉS de responder) y, si no hay, un «Sin foto» en ámbar con el contador «N sin foto» en la cabecera. No bloquea nada: avisa.
- **Cómo se registran (un solo punto):** un único `@app.after_request`, `_hall_tras_escritura`, mira qué ruta del técnico acaba de guardar (tabla `_HALL_RUTAS`, 12 rutas) y llama al manejador de esa ruta, que relee lo guardado y hace el alta idempotente (`_hall_registrar`, clave única OT + origen + referencia) o lo **retira** (`_hall_retirar`: el técnico volvió a «Aprobado», revirtió la acción…). **Best-effort:** un fallo se anota en el registro del servidor y la respuesta del técnico sale intacta. **Nada se borra:** un retirado queda anotado con cuándo y por qué; si reaparece vuelve a avisar solo si nadie lo tenía. Todo camino NUEVO que guarde algo que pueda ser un hallazgo se agrega a `_HALL_RUTAS` con su manejador.
- **Aviso por la campana, sin correo (REGLAS #19, #22, #26, #33):** a quienes decide **solo** `_ot_gestion_destinatarios` (el responsable de la OT primero, REGLA #36), sin la familia técnico y sin quien lo causó. **Una sola campana por OT y por persona** agrupando lo que llegue en 30 minutos («OT-2026-00171: 3 hallazgos nuevos», enlace a la tarjeta); mientras una campana sigue sin leer, el número cuenta todo lo que llegó desde que se creó (no «baja»). Tipos `ot_hallazgo` y `ot_hallazgo_toma` al FINAL de las **dos** listas del ENUM de `mant_notificaciones.tipo`. Prioridad: urgente si «puede lastimar a alguien», alta si hay falla, fuera de servicio o baja. Al cliente jamás se le escribe.
- **SOLO AVISA, NO TRABA (decisión de Daniel):** ninguna ruta de cierre ni de firma mira los hallazgos. El modal de «Aprobar cierre» muestra una franja ámbar «Hay N hallazgos sin gestionar en esta OT. Puedes cerrarla igual: no se bloquea nada» con un enlace a la tarjeta.
- **La tarjeta «Hallazgos de esta OT»** (`templates/ot2/_hallazgos.html`, `static/ot_hallazgos.js` y `.css`; contexto de `_hall_contexto_ficha`): **solo gestión** (a un técnico el servidor no le arma nada) y **también en una OT CERRADA** (la OT es evidencia y no se toca, pero los hallazgos se siguen gestionando): por eso va al nivel raíz del bloque de contenido, nunca dentro de `{% if puede_metadata %}` ni de una pestaña (una prueba lo recorre en el árbol de la plantilla). Cada hallazgo: semáforo de severidad (rojo falla y fuera de servicio, negro baja, ámbar alerta, azul repuesto, gris incidencia), nombre COMPLETO del equipo o la tarea (REGLA #15), fotos, quién lo reportó y cuándo (hora Chile), estado y quién lo tomó. Botones de 44 px (REGLA #3): **Tomar el caso**, **Levantar OT de seguimiento**, **Descartar** (motivo ≥ 10 caracteres, queda en la bitácora; con el modal de ILUS, nunca `prompt()`) y **Reabrir**; «Seleccionar todo» como interruptor (REGLA #14).
- **«Tomar el caso»** deja el caso a nombre de quien lo toma (UPDATE con el estado que vio: dos personas a la vez no se pisan) y, si no es el responsable de la OT, le avisa a este con tono suave (`ot_hallazgo_toma`, prioridad baja: nada que suene a castigo).
- **«Levantar OT de seguimiento» = OTRA OT, por el asistente «Nueva OT» ya existente, PRELLENADO** (REGLA #32: lo prellenado lo decide la persona): `GET /ot/api/<id>/hallazgos/prellenado?ids=` trae cliente, equipos, tipo correctiva, título, motivo y las fotos como referencia; la persona cambia lo que quiera, quita fotos o elige «Crear sin ligarla». Al crear, `_ot2_crear_core` (bloque 7.d) **revalida todo contra la base** —nunca se cree lo que manda el navegador: los hallazgos son de esa OT, siguen disponibles, mismo cliente, y solo pasan las fotos que ya son evidencia de ellos— y `_hall_seguimiento_enlazar` los liga **dentro de la misma transacción**: `mant_visitas.ot_madre_id` + `ot_relacion='seguimiento'` y el hallazgo pasa a «Con OT de seguimiento» (`ot_seguimiento_id`); si alguien los cambió mientras tanto, 409 y no se crea nada. Las fotos entran a «Videos y documentos» de la OT nueva con la MISMA URL y sin `public_id` (borrar el adjunto nunca borra la evidencia). **Pasa por la puerta del documento NORMAL: no hereda cobros ni saldo** (REGLAS #24, #25 y #28; el vínculo no toca nada de finanzas). Si la OT nace de una autorización remota de Daniel, se crea igual y solo se avisa si los hallazgos ya cambiaron. Si la OT de seguimiento se cancela, se anula o se borra, el hallazgo vuelve a «Sin gestionar» (estado efectivo: si no, el trabajo quedaría escondido como «resuelto»). `ot_eventos.py` no trata un seguimiento como desinstalación (guardia de tipo). (2026-10-21: la función «Eventos» de las OT se eliminó por pedido de Daniel; `ot_eventos.py` conserva solo la desinstalación.)
- **Panel de OT:** chip «Hallazgos abiertos: N» en la fila (tabla y Kanban, una consulta por lote) y filtro **«Con hallazgos sin gestionar»** (`hall=1`) en todas las vistas, los KPI, el paginado (REGLA #4.3) y el Reporte Excel. «Sin gestionar» tiene dos escrituras que deben decir lo mismo: `ot_hallazgos.sql_abierto` (SQL: panel, filtro y Excel) y `ot_hallazgos.estado_efectivo` (Python: la tarjeta y la franja del cierre); una prueba ejecuta el SQL de verdad y las compara en todas las combinaciones.
- **Confidencialidad (REGLAS #19 y #26):** las 4 rutas de gestión llevan `@_mant_required @_no_tecnico @_ot_can_view` (no `_ot_can_configurar`: se niega a todos, incluido el superadmin, en una OT cerrada); la bitácora de hallazgos (`hallazgo_*`, `creada_como_seguimiento`) no está en la lista blanca del técnico; nada de plata, documentos ni proveedores en campana, tarjeta ni prellenado (los repuestos van con nombre y cantidad solamente).
- **Datos:** la tabla `mant_ot_hallazgos` (clave única `visita_id + origen + referencia`, sin llaves foráneas) la crea `_ensure_ot_hallazgos` al arrancar —también con `ILUS_SKIP_MIGRATIONS=1`— con **una sola sentencia** `CREATE TABLE IF NOT EXISTS` (REGLA #18: barata si ya existe) y **sin ningún `ALTER` sobre `mant_visitas`** (el vínculo usa `ot_madre_id` / `ot_relacion`, que ya existían). Mientras la tabla no exista todo queda dormido (`_hall_ok`) y el resto de la app funciona igual. Las OT anteriores **NO se rellenan**: solo cuentan los hallazgos que nacen desde ahora.
- **Gestión CIRCULAR (Daniel 26-oct-2026: «un CRUD en los hallazgos… editarla, crear un repuesto, solicitarlo, generar una OT, generar
  un ticket… gestionando de manera circular… además de su valorización»).** Cada hallazgo muestra su recorrido con semáforo (atención →
  valorización → ticket → repuesto → OT de seguimiento) y su registro de actividad, SIEMPRE abiertos («me gusta ver el bosque siempre»: el
  sistema nunca pliega una tarjeta). Acciones: **Editar** (título, detalle, gravedad, con motivo; la respuesta del técnico no se toca),
  **Valorizar** (repuestos + servicio en pesos: plata, solo gestión), **Levantar ticket** (sugerido, pregunta antes, sin correo al cliente),
  **Solicitar repuesto** (abre Repuestos PRELLENADO con cliente, equipo y motivo; `origen_ref` deja el vínculo de vuelta) y **Borrar**
  (SOLO superadmin, escribiendo BORRAR + motivo; ANTES de borrar se guarda la fila completa en `mant_ot_hallazgo_borrados`, que es también
  la «lápida»: el mismo hallazgo no vuelve a nacer solo). La inquietud de cliente tiene la misma valorización y el mismo «Solicitar
  repuesto». Reglas puras en `gestion_circular.py`; tablas `mant_gestion_extra`, `mant_gestion_actividad`, `mant_ot_hallazgo_borrados`
  (`_ensure_gestion_circular`, solo CREATE TABLE IF NOT EXISTS). Pruebas: `tests/test_gestion_circular.py`.
- **Monitor, SLA y trazabilidad (Daniel 26-oct-2026: «algo parecido a la página de Retiros, insisto», «tablas amigables con filas informativas y una buena
  sensación de guía» y «implementar los SLA y poder tener trazabilidad y considerar varios puntos de salida: OT, Tickets, Abastecimiento y almacenamiento de
  repuestos. Y el consumo de las OT»).** `/ot/hallazgos` es un MONITOR con el lenguaje de Retiros: tarjetas KPI, buscador, chips por estado y UNA FILA POR
  HALLAZGO con quién lo reportó, de qué OT y de qué cliente (RUT y responsable de la OT), equipo con su N° de serie, quién lo tomó, el plazo con su barra y
  «Ahora toca» (la guía); debajo de cada fila, SIEMPRE abierta, la guía de 7 pasos con semáforo: Atención → Valorización → OT de seguimiento → Ticket →
  Abastecimiento (solicitudes de repuesto) → Bodega (existencia física menos lo prometido a otras solicitudes) → Consumo de la OT (repuestos instalados,
  atribuidos con COALESCE(ot_generada_id, visita_id) como el resultado financiero). El plazo (`hallazgos_sla.py`, módulo puro; reusa las horas hábiles de
  Retiros e Inquietudes) tiene DOS tramos: tomarlo (2 h hábiles si es grave: falla, fuera de servicio, baja o riesgo; 4 h los demás) y darle salida (OT,
  ticket, solicitud de repuesto o descartarlo: 2 días hábiles los graves, 5 los demás); se ajusta con `HALLAZGOS_PRIMERA_GRAVE_H`, `HALLAZGOS_PRIMERA_H`,
  `HALLAZGOS_SALIDA_GRAVE_DIAS`, `HALLAZGOS_SALIDA_DIAS` y `HALLAZGOS_AVISO_PCT`. Reglas finas (revisión adversarial del 27-oct): pedir un repuesto NO cuenta
  como que alguien tomó el hallazgo (lo cuenta la primera acción de la gestión sobre la solicitud); una solicitud RECHAZADA no cuenta; si la OT de seguimiento
  se cancela o anula, el plazo vuelve a correr desde el reporte; `repuesto_en` conserva la PRIMERA vez y una segunda solicitud suma a las anteriores. La misma
  guía, plazo y recorrido salen en la tarjeta de gestión de la OT (`_gc_enriquecer_hallazgos` ← `_hall_trazabilidad`). Es SOLO LECTURA y con plata (consumo):
  solo gestión, nunca un técnico (REGLAS #19 y #26). Todavía NO avisa por la campana cuando un plazo vence (el monitor lo muestra en «Plazo vencido»).
  **27-oct, formato Retiros (Daniel: «las filas terminan siendo interminables… tarjetas achicadas… filtros por columnas y de responsables arriba…
  poder entrar a la solicitud a gestionarla»):** UNA fila corta por hallazgo con el recorrido como 7 puntos de color dentro de la fila; la guía
  completa detrás de la flecha (como el detalle de Retiros). Filtros arriba (gravedad, responsable de la OT, quién lo tomó, quién lo reportó) y
  orden tocando el encabezado (`_HALL_ORDEN`, lista blanca). Tocar la fila o el botón chico «Gestionar» entra a la FICHA del hallazgo
  (`/ot/hallazgos/<id>`, `ot_hallazgo_ficha`, misma fila `_hall_monitor_items`) con la tarjeta de gestión de siempre solo para ese hallazgo.
  Borrado masivo (superadmin): la barra fija abajo aparece al marcar. Pruebas en `tests/test_ot_hallazgos.py` (TestFase1Bandeja).
  Pruebas: `tests/test_hallazgos_sla.py`.
- Pruebas: `tests/test_ot_hallazgos.py` (módulo puro, base de mentira, SQL del panel ejecutado de verdad en SQLite, plantillas renderizadas y JS ejecutado con Node). Esto se prueba SIEMPRE con datos de mentira: hay clientes reales de por medio (REGLA #22: cero correos o cambios visibles para el cliente al probar).

---

## 🔢 REGLA #39 — Toda plantilla lleva el objeto «N° de serie» y el GPS, y cada clasificación del catálogo tiene su plantilla (no negociable)

**Pedido explícito de Daniel (2026-10-21):** «nosotros construimos un objeto que se usa como número de serie… no un botón con una observación, un texto libre… si no lo tienen, agrégalo a la fuerza para que las plantillas estén estandarizadas. Todas las plantillas al crearse lleven esto por obligación, el número de serie y el GPS». Motivo de negocio: la **etiqueta de respaldo/garantía** y saber si despegaron o vulneraron un equipo. Y: «incluye también las (LU) de Lenin; yo le aviso». Y: que **existan en las plantillas todas las clasificaciones del catálogo de productos** (selector de peso, etc.), no solo en Instalación.

- **El objeto ya existía:** el tipo de respuesta `serie` (automático desde la ficha / manual / N-A; nunca pisa una serie real de fábrica: `_ot_dato_a_ficha`) y el tipo `gps`. El estándar es un ítem `serie` (obligatorio) y un ítem `gps` (obligatorio), **arriba** de cada checklist. Lógica pura y probada en `plantillas_serie_gps.py`.
- **Plantilla NUEVA:** nace con los dos objetos (`mant_plantilla_crear` llama a `completar_items_nueva`); si el editor ya los trae no se duplican, y un ítem de texto «N° de serie» pasa a ser el objeto. Editar una plantilla existente NO se fuerza.
- **Plantillas EXISTENTES:** botón **«Estándar serie + GPS»** en `/mantenciones/plantillas` (auditoría de solo lectura + lote). Lista cada plantilla activa que no lo cumple, con lo que se haría (agregar el objeto o convertir el ítem de texto), paginada, con «Seleccionar/deseleccionar todo» (REGLA #4.3 y #14) y vista previa. **Lo aplica una persona** (REGLA #32): `POST /mantenciones/api/plantillas/serie-gps/aplicar` recalcula todo en el servidor, compara la huella de lo que la persona vio, escribe la bitácora (`serie_gps_estandar`) ANTES de tocar los ítems y no toca nada en el arranque.
- **Las (LU) de Lenin** entran al lote solo si lo aplica el **superadmin** con la marca expresa «ya le avisé a Lenin» (`confirma_lu`); solo se agregan los dos objetos, nada más cambia. Los bloques «Revisión — …» (se usan una vez por producto en una OT interna) vienen sin marcar: un GPS por producto no tiene sentido; la persona decide.
- **Clasificaciones sin plantilla:** la misma pantalla las lista por categoría (Instalación, Mantención, Visitas, Trabajo interno) y las crea **en cero** (nombre = clasificación, con serie + GPS, marcadas `revisar_tecnico` para que se vea que falta llenar). El nombre sale siempre de `cat_clases_producto`, nunca del navegador. Por defecto vienen marcadas Instalación, Mantención y Trabajo interno.
- Las OT ya creadas conservan su copia de las tareas (`mant_visita_tareas`). **Pendiente (otra etapa, aún sin diseñar):** el objeto «etiqueta de respaldo/garantía» (código del sello, estado, foto, quién y cuándo) y la unicidad global de la serie. Pruebas: `tests/test_plantillas_serie_gps.py`.

---

## 🔑 REGLA #40 — Clasificación y plantilla se unen por una LLAVE (`clase_id`), no por el nombre; renombrar nunca crea otra (no negociable)

**Pedido explícito de Daniel (2026-10-21):** «que sean como un SKU, un primary key… si le cambio el nombre, que cambie en la configuración y no cree plantillas nuevas. Si creo una categoría, se cree nueva; si edito una categoría, cambia en todo, porque debería ser una base única». Incidente real: días antes renombró clasificaciones del Catálogo y el sistema creó plantillas nuevas, dejando las antiguas (con contenido) huérfanas.

- **Causa:** la plantilla se unía a la clasificación escribiendo el MISMO nombre (`p.nombre = cat_clases_producto.nombre`, en ~6 lugares). Renombrar la clasificación rompía la unión.
- **Llave:** `mant_tarea_plantillas.clase_id` (índice `idx_plant_clase`). Se asegura SIEMPRE al arrancar (`_ensure_plantilla_clase_id`, una cláusula por sentencia, barata si existe: REGLA #18) y se rellena UNA vez por nombre exacto (Instalación, Mantención, Visitas). Mientras la columna no exista, `_plcls_ok()` es False y todo sigue por nombre como antes.
- **Qué hace la llave:**
  1. **Renombrar** una clasificación (`PATCH /catalogo/api/clases/<id>`) renombra sus plantillas en todas las categorías (`_plcls_renombrar`); si el nombre nuevo ya lo usa otra plantilla de esa categoría, esa no se toca y se avisa. Nunca crea.
  2. **Crear** una clasificación (`POST /catalogo/api/clases`) crea sus plantillas en cero —Instalación, Mantención, Visitas y Trabajo interno— con N° de serie + GPS, ya ligadas (`_plcls_crear_para_clase`).
  3. **Desactivar / reactivar** la clasificación desactiva / reactiva sus plantillas (soft: nunca se borran).
  4. El **motor de OT** (`_plantilla_por_clasificacion_sku`), la **cobertura** (banner y matriz) y la **tarifa sugerida** buscan por la llave; el nombre solo es respaldo si no hay liga. Una plantilla creada a mano cuyo nombre coincide con una clasificación activa nace ligada (`_plcls_ligar_si_coincide`).
  5. **El nombre de una plantilla ligada se cambia SOLO en Catálogo:** `PUT /mantenciones/api/plantillas/<id>` responde 409 `NOMBRE_DE_CLASIFICACION` y el editor lo muestra de solo lectura. Eliminar una plantilla ligada la desactiva (no se borra).
  6. **Adoptar:** `POST /mantenciones/api/plantillas/adoptar {plantilla_id, clase_id}` (una persona) liga una plantilla con contenido a su clasificación —le pone el nombre de la clasificación— y borra la plantilla vacía (≤ 2 ítems, sin OT) que pudiera estorbar; si esa tiene contenido o uso, se rechaza (409).
- **Trabajo interno por clasificación (Daniel: «igual sería bueno tenerlas… como filtro»):** una plantilla por clasificación llamada «<Clase> (trabajo interno)». Va ligada (se renombra y se desactiva con la clasificación) pero es solo de FILTRO: el motor nunca la elige por clasificación (el sufijo y la exclusión de `trabajo_interno` en las uniones lo garantizan).
- Todo camino NUEVO que una una plantilla con una clasificación usa `clase_id`; nunca compara nombres. Pruebas: `tests/test_plantillas_clave_estable.py` (la cascada se ejecuta contra una base de mentira).

---

## 🔧 REGLA #41 — Solicitud de repuesto desde la OT: todo pedido nace con DUEÑO, tiene reloj de primera respuesta y avisa a técnicos y administradores (solo avisa, no traba; no negociable)

**Pedido explícito de Daniel (2026-10-21):** «que sea práctico, ordenado, guiado, que tenga trayectoria, que alerte a los técnicos y también a los administradores, y que los obligues a tomar una respuesta midiendo los tiempos de respuesta, algo parecido a Retiros». Causa: 14 pedidos «Por gestionar» sin responsable, el más viejo con 30 días, y ningún aviso real.

- **Lógica pura en `repuestos_respuesta.py`** (`_rresp` en app.py, con `tests/test_repuestos_respuesta.py`): dueño, reloj, niveles de aviso, textos y cifras. Nada de esto se calcula a mano en otro lugar.
- **Dueño al nacer:** el evento «creada» (`_otrep_evento`, el punto único de los 7 caminos) llama a `_otrep_asignar_dueno`: el ticket de repuestos de la OT si ya tiene a alguien · responsable de la OT, creador o ejecutivo (`_ot_gestion_destinatarios` vía `_hall_destinos`, nunca un técnico) · «responsable por defecto» de Repuestos · respaldo. El respaldo (superadmin/admin/supervisor por nombre) pierde contra el responsable por defecto. Nunca pisa un responsable escrito; queda en la bitácora de la solicitud quién, cuándo y por qué. El ticket `spare_parts` de la OT queda a nombre del mismo dueño (solo si estaba sin asignar). Los pedidos de gestión (manual), incidencia y ticket directo eligen su responsable por su camino.
- **Reloj de PRIMERA respuesta, en horas hábiles** (lunes a viernes, 08:00–17:00 como la cola de gestión, sin feriados de Chile): primera respuesta = la primera vez que el pedido sale de «solicitado» (validado, pedido o rechazado), con quién y cuándo, leída de la bitácora `mant_ot_repuesto_eventos` (sin columna nueva; los pedidos anteriores al 04-oct se deducen de las fechas de las columnas). Plazo: 1 día hábil (9 h); equipo fuera de servicio, 4 h. Un pedido reabierto parte de cero desde la reapertura.
- **Barrido automático** `_otrep_barrido_respuesta`, colgado de `simpliroute-poll` (cada 10 min), best-effort, con candado `GET_LOCK` y SOLO en horario de cobertura (misma definición que Retiros: `RETIROS_COBERTURA_*`, colación incluida): a las **4 h hábiles** (`REPUESTOS_AVISO_H`) campana al dueño; a las **8 h hábiles** (`REPUESTOS_ESCALA_H`) campana al dueño y a supervisión (supervisores y administradores; si no hay, el superadmin). Una sola vez por pedido y nivel (la marca es un evento `plazo` con `[n1]`/`[n2]`); varios pedidos de una pasada llegan en UNA campana por persona. Nunca de un pedido respondido, cerrado o manual. `_otrep_avisar_vencidas` (al abrir la lista) se conserva.
- **Avisos al técnico que pidió** (tipo `repuesto_tecnico`, campana, un único punto: el evento de cambio de estado): bodega lo revisó (hay en bodega / lo vamos a pedir / se resuelve con una OT), pedido con fecha de llegada aprox. (también cuando la compra recibe o cambia su fecha), pieza en bodega, instalado y cerrado (con un motivo en palabras sencillas: tres frases fijas, **nunca** la nota de gestión). Sin proveedor, costo, N° de OC ni montos (REGLAS #19 y #26; hay prueba que pasa cada texto por el filtro de la campana del técnico). No se avisa a quien hizo el cambio ni de un paso atrás.
- **Avisos a la gestión:** el pedido nuevo va a la gestión de ESA OT (el dueño primero), agrupado por OT en una sola campana (30 min / sin leer: «OT-…: 3 pedidos de repuesto nuevos»), ya no a todos los superadmin/admin. Los pedidos sin OT conservan el aviso de siempre.
- **Correos internos registrados y APAGADOS (REGLA #33):** `rep_nueva` (solo si el equipo queda fuera de servicio o el diagnóstico dice que puede lastimar a alguien; dueño + bodega), `rep_vencida` (nivel 2, dueño y supervisión, uno por persona) y `rep_lista_tecnico` (pieza en bodega, al técnico). Nacen **apagados por configuración**: solo salen con `REPUESTOS_CORREOS_ACTIVOS=1`, que **no está ni puede estar en `deploy.yml`** hasta que Daniel lo autorice en ese mensaje (hay prueba). Al cliente jamás (REGLA #22).
- **Medición:** `GET /repuestos/api/tiempos-respuesta?dias=30` (solo gestión, `@_no_tecnico`): promedio y mediana de primera respuesta, % dentro del plazo, pedidos vencidos y cifras por dueño; solo horas y conteos.
- **Todo camino NUEVO** que cree una solicitud pasa por el evento «creada» (nunca crea el dueño ni el aviso a mano); todo tipo de campana nuevo va al FINAL de las dos listas del ENUM de `mant_notificaciones.tipo`. Se prueba SIEMPRE con datos y correo de mentira (REGLA #22).

---

## 🚦 REGLA #42 — Todo campo con una condición (mínimo de caracteres, formato, obligatorio) pasa de ROJO a VERDE en vivo, con su mensaje; al guardar se alerta, no se traba (no negociable)

**Pedido explícito de Daniel (2026-10-24), viendo el campo «Motivo» del asistente de OT:** «pasan de rojo a verde… las personas no saben cuándo probaron la tarjeta. Es importante que vayamos en el proyecto entero: cuando la tarjeta apruebe los caracteres, esto tiene que pasar de rojo a verde». Y, al pedir «Editar cliente» como el formulario: «semáforo, tarjetas con paso a paso, validación de Google».

- **Qué es:** un campo (o la tarjeta de paso que lo contiene) que tiene una regla —mínimo de caracteres, formato, obligatorio— queda en **rojo** mientras no la cumple y pasa a **verde** en el momento en que la cumple, **mientras la persona escribe** (no solo al guardar ni al salir del campo). La tarjeta de paso cambia de rojo a verde cuando todos sus campos obligatorios cumplen. Un campo **opcional vacío** nunca es rojo (sin marca o ámbar); un dato que se recomienda pero no se exige (ej. RUT vacío) es ámbar.
- **Siempre con texto, no solo color:** un mensaje corto al pie, del mismo color, que dice qué falta o qué está mal («Faltan 12 caracteres», «El dígito verificador no calza», «Le falta la @ o el dominio»). El verde también puede decirlo («RUT válido»). El color solo no basta (accesibilidad).
- **Patrones de referencia (reutilizarlos, no reinventarlos):** en el asistente de OT, todo campo con `data-min="N"` lo marca `_o2mVcUno` / `_o2mVcBarrido` (`.is-valid` / `.is-invalid` y el contenedor `.ok`) en `templates/ot2/_modal_crear.html`; en «Editar cliente», `static/cliente_form.js` (`marca()`, `paso()`, `evaluar()`) con `static/cliente_form.css`. Un formulario NUEVO nace con esto (REGLA #15); uno existente se lleva al patrón cuando se toca (no se reescribe en masa: REGLA #4.2).
- **Todo campo con mínimo de caracteres lo recibe SOLO, sin tocar el formulario (`static/ilus_ui.js`, cargado en todas las pantallas):** un `<textarea>` o `<input>` con `data-min="N"` o cuyo placeholder diga «(mínimo N caracteres)» se pinta en vivo (rojo + «Faltan N caracteres (mínimo M)»; verde + «Listo.»; vacío sin color; `aria-invalid`). Solo informa, nunca bloquea: cada formulario sigue validando al guardar. Dentro de una fila flex/grid o un `input-group` queda solo el color del borde (no mete un hermano que rompa el diseño). Quedan fuera el asistente de OT (su propio motor) e `ilusPrompt` (el de abajo). Para un formulario NUEVO: pon el mínimo en el placeholder o en `data-min` y listo.
- **`ilusPrompt` ya lo hace solo:** `ilusPrompt({ minLength: 10 })` y/o `ilusPrompt({ validate: v => … ? {ok:true} : {ok:false, error:'…'} })` pintan el campo en vivo (rojo con «Faltan N caracteres (mínimo M)» o con el `error` del validador; verde con «Listo.»; vacío sin color). Aceptar sigue bloqueando igual. Úsalo en vez de validar a mano al apretar el botón (`static/ilus_ui.js`). **El contrato de `validate` es `{ok, valor?, error?}`**; un validador que devuelve un TEXTO de error (o `null`/`''` si está bien) o un booleano se lee igual (`_lee`): nunca debe pintarse de verde un dato que el propio validador rechazó. El pintor genérico de `data-min` vuelve a evaluar los campos ya pintados cuando se abre un modal (un campo que el código vació con `.value = ''` no reabre con el verde del texto anterior).
- **Validadores que ya existen:** RUT chileno con módulo 11, correo, teléfono chileno (8 o 9 dígitos, con o sin +56), día del mes 1–28. Una **dirección** solo es «verificada» si se eligió una sugerencia de Google Places (hay `place_id`) y el texto verificado sigue intacto (se puede **agregar** oficina, depto o referencias **al final**); si se toca lo verificado deja de estarlo. Los datos de región, comuna y ciudad vienen de Google.
- **La ubicación verificada es REQUISITO para avanzar (Daniel 2026-10-24: «la ubicación se debe verificar para poder avanzar»)**, en «Editar cliente»: una dirección escrita o **cambiada** sin verificar con Google deja el botón Guardar apagado, el pie dice por qué (y lleva a la dirección al tocarlo) y `guardarCliente()` lo vuelve a exigir. **Dos salidas, para no dejar un callejón sin salida (revisión independiente 24-oct):** (1) si Google Places no está disponible (no carga o no responde) no hay cómo verificar: se avisa («Guardar igual» / «Revisar»); (2) si la dirección **no se tocó** y nunca estuvo verificada (cliente anterior a esta regla, o sin dirección) se puede guardar con un aviso ámbar —se exige verificar lo que se cambia, no el pasado: de lo contrario ni el estado, ni el ejecutivo, ni el tipo de ese cliente se podrían editar—. Lo verificado vive en memoria (`verifs` en `static/cliente_form.js`): **cerrar y reabrir el modal no verifica nada** y el `place_id` / coordenadas ocultos **siempre calzan con el texto** (si el texto se aparta de lo verificado se vacían; vuelven si regresa); **región, comuna y ciudad siguen la misma regla** (cada verificación recuerda las suyas y se reponen al volver a ella, SOLO si nadie las tocó a mano; lo que una sugerencia de Google no trae no se queda con el valor de otra dirección). El detalle se agrega **al final y separado** («…4501, oficina 5»; «…4501A» ya es otra dirección); en un cliente ya guardado la base verificada llega hasta «, Chile» (lo que devuelve Google) y el detalle que sigue se puede editar o quitar sin volver a verificar. Un retoque accidental tiene salida directa: el botón «Volver a la dirección guardada» (`ecfRestaurarDireccion`). Si Google rechaza la clave (`gm_authFailure`) o aún está cargando, se trata como «Google no disponible / conectando» (no como dirección cambiada). El modal lleva `data-bs-keyboard="false"` y su propio Esc: cierra el formulario solo si no hay un aviso (`.ilus-overlay`) ni la lista de Google encima. Todo formulario NUEVO con dirección aplica lo mismo cuando Daniel lo pida.
- **Los datos largos NUNCA se cortan, tampoco en celular (Daniel 2026-10-24: «región, comuna y ciudad deben ser grandes… se va a entrecortar cualquier dato en la parte responsiva»):** razón social, giro, región, comuna, ciudad, correos, nombres y cargos van en `<textarea rows="1" class="ecf-auto">` que **crece** con lo escrito (una sola línea lógica: Enter no agrega saltos); región, comuna y ciudad además llevan `ecf-grande`. La dirección debe seguir siendo un `<input>` (Google Places lo exige): su texto completo se muestra en el cuadro de verificación. En tablet el modal de Bootstrap (500 px a 768–991 px) se ensancha a `min(1100px, 100vw - 1.5rem)`.
- **Alertar, no trabar:** el semáforo solo marca. Al guardar, lo **mal escrito** (con un valor, pero inválido) se muestra con `ilusConfirm` —«Revisar» devuelve a la persona al primer dato en rojo sin guardar; «Guardar igual» sigue— y **nunca** bloquea, salvo las reglas duras que ya tenía ese formulario o el servidor. Lo **vacío** no es «mal escrito»: lo cubre la tarjeta del paso. Ver `guardarCliente()` en `static/mant_ficha.js`.
- **Técnica:** la evaluación se agenda con `setTimeout` corto, nunca con `requestAnimationFrame` (no corre con la pestaña en segundo plano, y las pruebas y las vistas previas se quedan con el estado viejo). Targets de 46 px y letra de 16 px en celular (REGLA #3); en celular el círculo del paso va arriba para que los campos usen todo el ancho.
- **Las regiones `aria-live` solo se reescriben si cambiaron** (`escribir()` en `cliente_form.js`): un lector de pantalla relee una región cada vez que se reescribe su contenido, y el semáforo evalúa en cada tecla.
- Pruebas: `tests/test_cliente_editar_semaforo.py` y `tests/test_cliente_editar_ubicacion.py` (el JS se ejecuta con un DOM mínimo de mentira: vacío, mal escrito, bien escrito, dirección verificada y retocada, cerrar y reabrir, cliente anterior sin verificar, coordenadas que siguen al texto), `tests/test_ilus_prompt_rojo_verde.py` (contrato de `validate` y repintado al abrir un modal) y `tests/test_ot_instalacion_con_desinstalacion.py` (campos `data-min` del asistente).

---

## 📅 REGLA #43 — «Instalación con desinstalación» agenda las dos órdenes en UN solo calendario, con un interruptor «Entrada | Salida» (no negociable)

**Pedido explícito de Daniel (2026-10-25), tras probarlo en producción:** «reciclar el mismo calendario… para ahorrar código y texto lo usamos el mismo calendario… y que este comportamiento solamente ocurra si escoges instalación con desinstalación», y «hazlo potente, hazlo amigable, hazlo con pocos pasos, muy poco scroll… inteligente, intuitivo, amigable».

- **Un solo calendario** (mes + línea de tiempo del día: `o2mCal*` / `o2mDay*` en `templates/ot2/_modal_crear.html`) para la entrada (instalar) y la salida (desinstalar). La salida **ya no tiene calendario propio**: no se vuelve a construir uno (el calendario compacto oscuro del 23-oct, `_diCalGrid` y compañía, se eliminó). Todo lo que el calendario lee o escribe de «la orden que se agenda» pasa por los accesores `agFecha / agHini / agHfin / agTecnicos / agLider / agMinFecha / _agDueno`: la entrada es `S.*` y la salida `S.desinst.*` (lo que viaja en `body.desinstalacion`). Un código nuevo del calendario que lea `S.fecha / S.hini / S.hfin / S.tecnicos / S.lider` directo haría que la salida edite la entrada (una prueba lo vigila).
- **Solo con la tarjeta «Instalación con desinstalación»** (`_o2mDiAplica()`): sin ella `_agSal()` es siempre false y el paso Agenda queda **idéntico** (misma etiqueta «Fecha programada», sin interruptor, sin marcas ni bloque fantasma). La instalación a secas y los demás tipos no cambian.
- **Interruptor «Entrada | Salida» pegajoso** (`S.agOrden`, `agOrden(w)`, `position:sticky` arriba del paso, siempre a la vista al bajar a la línea de tiempo): muestra las dos fechas y los días que hay entre ellas, con semáforo (verde si la fecha sirve; rojo con el motivo; «el técnico ya tiene trabajo» si el servidor avisa choque), marca con borde negro y «editando» la orden que se agenda. Cambiar de orden **no reconstruye el paso**: solo reubica fecha, horas, mes, línea de tiempo, avisos y choque (`_agAplicarOrden`).
- **La guía dice qué sigue** (`_o2mAgGuiaHtml`: «Entrada lista → Agendar la salida») y, con la salida activa, **atajos de fecha** contados desde la entrada (`+1 sem · +2 sem · +1 mes · +2 meses`, el domingo se corre al lunes, y la «Sugerida» del servidor). El sistema sugiere y la persona decide (REGLA #32). La salida no admite días anteriores a la entrada ni domingos; el feriado solo avisa (REGLA #31).
- **El calendario marca las dos fechas** (entrada azul y salida roja, cada una con su icono), una franja «equipos instalados» entre ellas y, en la línea de tiempo, la OTRA orden como bloque fantasma cuando cae el mismo día.
- **Avisos sin duplicar:** con la salida activa, la fecha, el fin de semana/feriado y el choque los explica UNA caja (`_diAvisosHtml('fecha')`): la del servidor (con «Buscar hueco libre» y, solo para el superadministrador, «Agendar igual») y, si choca otro técnico de la salida que no es el líder (el servidor solo mira al líder), los mismos mensajes del calendario en ámbar; el aviso de choque propio del calendario queda solo para la entrada. Técnico, costo, cobro, saldo y checklist de la salida siguen en el paso «Desinstalación».
- **«Lista» es lo que dice el servidor, no solo el navegador** (revisión independiente 25-oct): la ficha «Salida», la guía y el check de «Agenda» usan `_diSalidaListaParaAgendar()` = fecha válida (≥ entrada, no domingo) **y** sin bloqueo del servidor (`_diSalidaBloqueada()`: mismo día con inicio antes de que termine la entrada, término antes del inicio…) **y** sin choque del líder salvo «Agendar igual». Mientras el servidor revisa, o si falla, no se inventa un bloqueo. Cambiar las horas de la entrada también vuelve a revisar la salida (su regla del mismo día depende del término de la entrada). El mismo día que la entrada, la salida **se sugiere** desde que ella termina (con su misma duración); la persona puede cambiarla.
- **Cambiar de orden mientras espera la red nunca escribe sobre la otra** (`_o2mDayIrADia(fecha, orden)`, «Sugerir horario», `_agAplicarOrden`, el mes que llega en `setFecha`): cada una guarda la orden con la que empezó y descarta su resultado si `S.agOrden` cambió. **«Sugerir horario» cuenta el hueco de la otra orden** como ocupado (`_agLimiteOtraOrden`: la salida del mismo día parte cuando termina la entrada; la entrada no pasa de la fecha de la salida ni termina después de que ella empiece). **Escribir la fecha de la salida a mano no se borra** mientras se tipea (el navegador avisa cada año parcial): se acepta, la ficha queda en rojo y solo se avisa con un toast cuando la fecha ya es completa y plausible.
- **Celular (REGLA #3):** el interruptor es una fila de dos fichas cortas («Mar 27 oct»), las horas van lado a lado, los atajos se deslizan dentro de su caja y, **con las dos órdenes** (clase `.o2m-ag-di` del paso), todo lo que se toca mide 44 px: fichas, atajos, guía, flechas del mes y del día, «Hoy», «Sugerir horario» y las cajas de fecha y hora. La caja de aviso/choque envuelve sus botones debajo del texto (nunca aplasta el texto: `.o2m-di-av>span{flex:1 1 220px}`). Nada se corta (REGLA #15). Accesibilidad: la frase de la guía se anuncia por una región fija (`#o2mAgLive`, sin repetir los atajos), el foco vuelve al botón del mismo lugar al repintar (`_agPintarConFoco`) y la barra pegajosa reserva su alto al desplazar el foco (`scroll-padding-top`).
- Pruebas: `tests/test_ot_desinst_calendario.py` (ejecuta el JS real: accesores, `setFecha` / `setHora` por orden, marcas y franja del calendario), `tests/test_ot_instalacion_con_desinstalacion.py` (interruptor, guía, atajos y avisos) y `tests/test_ot_agenda_unica_revision.py` (carreras de red, «Sugerir horario» con la otra orden, choques, «lista» según el servidor, foco y estilos; cada una de sus protecciones se verificó rompiéndola a propósito: la prueba debe fallar).

---

## 🧾 REGLA #44 — «Costos y documentación de la OT»: UN solo sentido, UNA tarjeta de cuenta, y lo que se ve sale de la MISMA regla que el candado (no negociable)

**Pedido explícito de Daniel (2026-10-25), con la OT-2026-00354:** «aquí veo contradicciones y muy poco explicativo: arriba dice *se cobra garantía sin autorización de Daniel* y abajo *Crear OT sin documento · garantía, autorizada, la pidió Aarón*… ¿la pidió o no la pidió? Eso tiene que ser dinámico», «necesito que todo se facture, todo lleve una factura», «tratemos de resumir esto, no tantas tarjetas: en una puedo sacar cuánto me cobraron, cuánto es mi margen y el porcentaje», «entiendo que es un paso a paso, pero lo necesito más específico», «¿Editar costos y Corregir finanzas no hacen lo mismo?», «que sea el mismo costo del servicio e idéntico al anexo, y que si el anexo está firmado esto se bloquee», «la idea es que el motor de finanzas funcione en un solo sentido; garantías o coberturas asociadas a centro de costo y poder reportar esas situaciones». Y, suavizando: «igual puedes editarlo al cerrar la OT… el monto no debería cambiar si está cerrado el servicio, pero sí heredar un documento para cerrar formalmente; la excepción son las garantías». Es una primera versión: **«esto lo vamos a ir probando y editando a medida que yo, como ser humano, lo pueda entender»**.

- **Una sola regla para «¿esta autorización respalda el $0?»** (`_ot_aut_cubre_cero(a, vid, aut_id)` en `app.py`): la usa la puerta de cierre (`_ot_puerta_documento`) y el recorrido de la ficha (`ot_api_recorrido`). Cuenta la `cobro_cero` aprobada y la `crear_sin_documento` aprobada **con la que NACIÓ esa OT** cuando su motivo ya era un $0 (garantía, regalía, arriendo/leasing: la OT apunta a ella con `cobro_cero_autorizacion_id` o `visita_creada_id`). Antes la tarjeta de arriba y la lista de abajo decidían distinto y se contradecían. Se cuenta con **una sola frase** (`_ot_aut_frase`: «Autorizada por Daniel Aguilar el 23/10/2026 14:59 (la pidió Aarón Urbina)»); en la ficha esa constancia vive **solo en el paso ①** (con el motivo completo), no se repite en la tarjeta de la cuenta.
- **Un solo sentido, seis filas con texto específico y completo** (nada de chips cortados; REGLA #15): ① ¿Se cobra? → ② **Factura** (la factura manda; la nota de venta y la cotización son provisorias) → ③ Cobré → ④ Me cobró el proveedor (**= el anexo**, REGLA #34) → ⑤ Margen y centro de costo → ⑥ Revisión al cerrar. El servidor además manda **una frase dinámica** (`resumen`: «Lista para cerrar» / «Falta la factura · lo que cobró el proveedor» / «Esperando a Daniel (solicitud N° 12)» / «Cerrada»). **La frase no puede prometer ni negar más que el servidor** (revisión independiente, 25-oct): `faltan` nombra SOLO lo que de verdad frena la firma del cierre (`mant_ot_aprobar_cierre`: la puerta del documento, el cobro declarado, el centro de costo, el costo del proveedor, el anexo firmado —solo si la OT se cobra y el proveedor es externo— y la pérdida) y `avisos` lo que se ve en los pasos pero NO frena («Se puede cerrar · revisa: la autorización del $0»); los pasos cuentan por la vía real de la puerta (`p_cerrar.via`: cierre autorizado sin factura, hija que hereda el $0, pendiente por falta de stock…); antes de que el técnico firme dice «Papeles al día, aún sin la firma del cierre» (nunca «Lista para cerrar»); y el motor suma el saldo de la factura excedido (lo mide el panorama). Toda condición nueva del cierre se agrega a `faltan` en `ot_api_recorrido` al mismo tiempo que al candado.
- **UNA tarjeta de cuenta** (`.fm-cta`): Cobramos · Nos cobraron · Ganamos o Perdimos con su % (o «No se cobra · nos costó $X»), la mini-tabla por negocio, el vínculo con el anexo del proveedor (el servidor dibuja su macro de siempre y el motor lo **adopta** dentro de la tarjeta: `adoptarAnexo`) y el **centro de costo adentro**. Los once contadores pasaron a unos pocos chips dentro de «Documentos» (solo los que dicen algo). Orden: encabezado → frase → *El recorrido | La cuenta* (en una sola columna, primero la plata) → documentos | lo que nos costó → pie de superadmin. Colores de «guiar sin gritar»: lo que toca = ámbar, lo que espera = azul, hecho = verde; el rojo solo para una pérdida real.
- **Costos resumidos y con pocos clics (Daniel, 25-oct, viendo la OT-2026-00247: «esto está bien; se puede mejorar en los costos: resumidos y con pocos clics»):** «Lo que nos costó» ya no es una tarjeta aparte que repite la cuenta: cada fila de «Nos cobraron» (técnico/proveedor, despacho, repuestos) vive DENTRO de La cuenta, con quién cobró y el candado del anexo. Con la OT en orden, las excepciones (declarar $0, autorizar cierre sin documento, agregar otro documento) quedan detrás de UN botón «Más opciones»; si falta algo, el botón que lo resuelve se ve a la vista, en azul. Cuando ningún paso del recorrido falta, el recorrido se pliega en una línea de chips («① Se cobra ✓ · ② Factura ✓ · …») y se abre con un toque (el texto completo sigue ahí: REGLA #15).
- **Un solo camino para tocar la plata, y lo avanzado explicado:** OT abierta → «Editar cobro y costos»; OT cerrada (solo superadmin) → «Corregir finanzas» (queda registrado). «Corregir finanzas» (con la OT abierta) y «OT con finanzas dudosas» (un listado de todas las OT con la plata por revisar) van en un **pie «Solo superadmin»**, cada uno con una línea que dice qué hace. Nada se quitó (REGLA #4.2).
- **Coberturas sin cobro (garantía, regalía, arriendo o contrato) siempre se cargan a un centro de costo** y se pueden reportar: el centro de costo es obligatorio y la tarjeta lo dice; el Excel de OT (`/ot/reporte.xlsx`) trae la hoja **«Garantías y coberturas»** (cobertura × centro de costo: cantidad, «nos costó» y valorizado; abajo el detalle OT por OT; solo el superadmin ve además cómo nació, quién autorizó y el argumento: REGLA #26).
- **Candado por anexo, a la vista:** con un anexo vivo el costo del proveedor ES el del anexo (REGLA #34): las filas de «Lo que nos costó» llevan su candado, los campos de costo del modal «Editar cobro y costos» son de solo lectura (con su porqué), el cotizador no los reemplaza, «Modificar» no ofrece escribirlo a mano y la salida es «Usar el valor del anexo» (paso ④ y franja del anexo) o emitir un anexo nuevo si ya está firmado. El servidor rechaza igual cualquier escritura que no sume lo mismo que el anexo (`COSTO_ES_DEL_ANEXO`, 409). Una OT cerrada es evidencia: su monto no se toca (salvo «Corregir finanzas» con registro inmutable); sí se puede regularizar el documento.
- **Pendiente de decidir con Daniel** (primera versión, a ajustar probando): si el candado debe aplicar desde que el anexo existe (hoy, igual que el servidor desde el 20-oct) o solo cuando está firmado; qué significa exactamente «heredar un documento para cerrar formalmente» (hoy se cierra con factura o boleta; la nota de venta queda como «Falta factura»); y si la garantía, además de su autorización con argumento, debe llevar siempre la factura o nota de venta original como referencia (REGLA #24 reafirmada el 18-oct).
- Pruebas: `tests/test_ot_finanzas_compacto.py` y `tests/test_ot_costos_documentacion.py` (el motor JS en Node: recorrido, frase, cuenta, anexo, centro de costo, pie), `tests/test_ot_puerta_documento.py` (la regla única del $0 autorizado y su frase), `tests/test_ot_interna_cierre_0810.py` (el recorrido del servidor) y `tests/test_ot_excel_garantias_coberturas.py` (la hoja del Excel).

---

## 🤝 REGLA #45 — El paso «Costos» del asistente pide cada plata UNA sola vez, en ámbar y sin trabar (no negociable)

**Pedido explícito de Daniel (2026-10-25), mirando el paso Costos de una garantía con un valor de referencia de $109.817:** «lo valoricé en 109, pero abajo me dice cuánto nos cuesta; está en rojo y no me deja avanzar. Esto debe ser amigable, no un instrumento que retrase la toma de decisiones de los analistas: que la información recopilada sea cuánto nos cobran, cuánto cobré, el margen y ya. No hay que repetir opciones», y «al crear una OT los costos se hacen largos y dan pereza rellenar valores duplicados o similares».

- **Lo único que se pide:** cuánto se cobra (con documento), cuánto cobra el técnico o proveedor, y el margen se ve solo. Todo lo demás queda **plegado** detrás de un enlace, con sus campos en la página (nada se quita, REGLA #4.2): el **valor de referencia** (en un $0 es un sub-paso plegado «Valor de referencia (opcional)»; en una OT que se cobra, «Agregar valor de referencia») y el **costo del despacho** (solo se ve si ya trae un valor, si al cliente se le cobra despacho o si se pide con «Agregar costo de despacho»).
- **Técnico propio = $0 sugerido** (`_o2mFinCostoAuto`): el costo del servicio viene escrito en $0, visible y editable (igual que en la desinstalación, `_diAutoCosto`). Con un externo se deja vacío (o se usa la tarifa de su ficha). **Nunca pisa lo que la persona escribió** (`S._finCostoAuto` solo vale mientras el valor lo puso el sistema).
- **El anexo no vuelve a pedir los montos** (REGLA #34: el costo de la OT ES el del anexo): mientras las partidas sean las de Costos, el paso Anexo las muestra en una línea («Instalación $X + Despacho $Y = $Z · Vienen de Costos») y «Cambiar partidas» abre la tabla editable.
- **Una etiqueta nunca miente:** el estado de un sub-paso (`_o2mFinSubEstado`) debe pedir EXACTAMENTE lo que pide el candado (`completo('finanzas')`). Había una tarjeta que decía «Opcional» mientras el paso exigía el costo del servicio (desde el 23-oct, también en garantía): Daniel lo vivió como un rojo que no lo dejaba avanzar sin decir por qué.
- **Lo que falta se dice en ámbar, no en rojo** (nadie se equivocó: falta un dato). Los técnicos y el responsable elegidos quedan en **verde** (`sel-ok`): son requisitos que se resuelven al elegir.
- **Con «Instalación con desinstalación», cada equipo elige en el paso Equipos los DOS checklists** (entrada y salida), con «No se retira» y «Retirar todos / ninguno»; el paso «Desinstalación» solo resume y lleva de vuelta (mismo estado `S.desinst.eq_plant` / `eq_fuera`). El aviso «Al crear esta OT se enviará…» ocupa una línea y la lista se abre con «Ver a quién» (sigue diciendo a quién y lo que esté apagado: REGLA #37).
- **Una garantía/regalía se valoriza y dice cuánto cuesta** (Daniel, 25-oct: «si es garantía, que lo valorice, a ver cuánto nos cuesta y si estamos perdiendo dinero»): en un $0 el valor del servicio queda a la vista («¿Cuánto vale este servicio?», recomendado, nunca obligatorio) y el veredicto (`_o2mFinVeredictoCero`) dice «No nos cuesta dinero» / «Nos cuesta $X · N % de su valor» / «Cuesta más de lo que vale» (rojo solo cuando el costo supera el valor). El candado de pérdida del cierre (REGLA #28) no cambia.
- Pruebas: `tests/test_ot_asistente_costos_amigable.py` (52; cada protección se verificó rompiéndola a propósito).

---

## 🧾 REGLA #46 — «Instalación con desinstalación» firma UN solo anexo con el proveedor, con el detalle de cada OT, y cada OT se cuadra con SU parte (no negociable)

**Pedido explícito de Daniel (2026-10-25):** «el anexo debe ser único… describiendo el detalle de cuánto se me va a cobrar por instalación, desinstalación y despacho, algo bien claro para evitar problemas», y «los costos de la OT son los mismos del anexo» (REGLA #34 se mantiene).

- **Qué es:** si la instalación y su desinstalación las hace el **MISMO proveedor externo** (misma ficha de `mant_tecnicos_externos`, o el mismo usuario con rol `tecnico_externo`: `_ot_proveedor_ext_key`), el proveedor recibe y firma **un solo anexo**, el de la OT de entrada. Cada partida de `precio_items_json` lleva la OT a la que pertenece en `ot`: `entrada` (la instalación; **una partida sin etiqueta es de la entrada**, así que todos los anexos anteriores valen igual) o `salida` (la desinstalación). Conceptos claros y por separado: «Instalación», «Despacho (instalación)», «Desinstalación», «Despacho (desinstalación)». La lógica pura (partes, subtotales, alinear por OT) vive en `ot_anexo_costo.py` (`parte`, `total(rol)`, `partir(rol)`, `alinear(rol)`, `estado(rol)`, `resumen_partidas`).
- **Cada OT se cuadra con SU parte:** `_ot_anexo_vivo(vid)` devuelve `items`/`total` de la PARTE de esa OT (y `items_general`/`total_general` del anexo completo, `rol`, `compartido`), así que el candado `COSTO_ES_DEL_ANEXO`, «Usar el valor del anexo», «Corregir con el anexo», el vínculo de la ficha y `ANEXO_DESACTUALIZADO` al cerrar siguen funcionando sin cambios, pero por OT. La desinstalación **sin anexo propio** lee el de su instalación (`_ot_anexo_de_la_madre`), y solo si ese anexo trae partidas de la desinstalación. Crear, corregir o firmar el anexo cuadra las DOS OT (`_ot_anexo_alinear_familia`); el lote de Facturación de proveedor sigue el costo de cada una (REGLA #29).
- **El bloqueo del técnico** (`_anexo_bloquea_ot`): la desinstalación sigue el estado del anexo de su instalación (firmado → libre; enviado o visto sin firmar → bloqueada con su número). Mientras el proveedor no firme, ninguna de las dos OT es trabajable.
- **Cómo nace:** (1) el asistente «Nueva OT» arma las partidas con el costo de cada OT (`_o2mAnexoSalida`; nota «Anexo único» a la vista) y el **servidor decide** si de verdad comparten (`desinstalacion.comparte_anexo`): lo que no comparta se descarta antes de crear el anexo; (2) la hija no genera su anexo automático (`anexo_delegado`); (3) si la instalación ya tenía un anexo **sin firmar** al agendar la desinstalación, sus partidas se suman a ese anexo (`_ot_anexo_agregar_salida`, con bitácora en el anexo y en las dos OT y aviso de reenviarlo si ya estaba enviado); (4) desde la ficha, el modal «Anexo de servicios» de la instalación trae las partidas de la desinstalación y un selector «Instalación / Desinstalación» por fila (`itemsDesdeOT`, `setItemOt`).
- **Lo que NO se hace:** un anexo **firmado** es evidencia: no se le agregan partidas (la desinstalación tendrá el suyo, o se emite uno nuevo); una desinstalación que ya tiene su propio anexo vivo no entra en el de la instalación (`SALIDA_CON_ANEXO_PROPIO`, 409: nunca se cobra dos veces); partidas de `salida` sin una desinstalación vigente se rechazan (`SALIDA_SIN_DESINSTALACION`); otro proveedor en la desinstalación = otro anexo.
- **Dónde se ve:** la ficha de la instalación («Anexo único (instalación + desinstalación)» con el detalle y el total), la ficha de la desinstalación (tarjeta de solo lectura con «su parte $X de $Y» y el enlace a la instalación, sin botón de crear otro anexo), la franja «Un solo valor» (su parte + el detalle), y el PDF y la página de firma con **subtotales por OT**. Solo gestión: un técnico nunca lo ve (REGLAS #19 y #26).
- **Límite conocido:** los listados SQL que unen `mant_anexos.ot_id` con la OT (filtros del panel, columnas del lote de Facturación de proveedor) siguen mostrando a la desinstalación «sin anexo propio»; la verdad (candados, ficha, cierre, pago) usa el resolver.
- Pruebas: `tests/test_ot_anexo_unico.py` (cada protección se verificó rompiéndola a propósito: partes por OT, doble cobro, firmado que libera, cierre por parte, no sumar a un anexo firmado, arrastre de la otra OT).

---

## 🧭 REGLA #47 — Todo $0, cierre sin factura/boleta, cobro sobre el saldo y pérdida se CLASIFICA («¿Por qué?» + quién lo asume), y queda con la autorización (no negociable)

**Pedido explícito de Daniel (2026-10-25):** «agregar los motivos, pero solamente para ser administrador: garantía, regalía, arriendo, leasing, evento, marketing, el precio lo asume comercial… algo dinámico para la toma de decisiones en el mes»; «todo cobro se gestiona con NVV, BLV y FCV pero se CIERRA con FCV o BLV; de lo contrario bloquea la OT y pide autorización, ya que puede ser arriendo, descuadre de precio o pérdida, costo asumido por marketing»; «¿Quién lo asume? ¿Por qué? Autorizaron… toda esa trazabilidad la necesito».

- **Dos capas:** la FAMILIA contable de un $0 (`cobro_cero_motivo`: garantía | regalía | arriendo_leasing) no cambia ni se renombra («Garantía» sigue siendo Garantía: no se afectan las OT hechas); la INTENCIÓN es la razón de negocio, administrable, que se elige encima. Catálogo `ot_intenciones` (clave, nombre, descripción, familia_cero, aplica_a, asume_centro, solo_admin, activa, sistema, orden, quién/cuándo). Nunca se borra: se desactiva (las del sistema —las 3 familias y «Otro motivo»— no). La siembra solo agrega claves que faltan y jamás pisa lo editado. Lógica pura en `ot_intenciones.py`.
- **Dónde se usa** (`aplica_a`): un $0, cerrar sin documento, cerrar con pérdida, cobrar sobre el saldo, crear sin documento y documento observado. El **servidor** valida cada una (existe, activa, aplica a ese uso y «solo administrador» solo para un administrador); sin «¿Por qué?» no hay solicitud de una OT (`INTENCION_REQUERIDA`). En un $0 la clasificación fija la familia contable (evento, marketing y «lo asume Comercial» quedan como `regalia`).
- **El sistema sugiere, la persona decide (REGLA #32):** el asistente y los diálogos preseleccionan según el centro de costo y el tipo de OT, con su origen a la vista, y se puede cambiar. Ninguna intención nace «solo administrador»: Daniel las marca desde la pantalla.
- **Trazabilidad:** cada autorización guarda `intencion` + el argumento de quien pide (aparte) + un «mensaje del sistema» con lo que detectó, con cifras. Se ve en el detalle y la bandeja, la campana, el correo interno, la constancia, la bitácora y el recorrido de la OT. La OT guarda `mant_visitas.intencion` (la primera clasificación manda); la desinstalación cubierta hereda la de su instalación si es un $0. «Quién lo asume» = el centro de costo con que quedó la OT.
- **Gestión solo superadmin:** `/ot/intenciones` y `/ot/api/intenciones/…` (el acceso está en la bandeja de autorizaciones), con bitácora. El catálogo que se ofrece es para gestión (`@_no_tecnico`): un técnico no ve nada (REGLAS #19/#26).
- **Para decidir en el mes:** el Excel «Garantías y coberturas» (columnas «Por qué» y «Cuándo autorizó») y la hoja «Por mes e intención» (mes × clasificación × centro que lo asume), y el Costeo de OT (REGLA #48). Las OT anteriores salen «Sin clasificar»: no se rellenan.
- **No cambia** los umbrales de pérdida y equilibrio (REGLAS #28/#29) ni a quién se le escribe (nada al cliente: REGLA #22). Arranque barato (REGLA #18): 1 CREATE TABLE y 3 ALTER de una cláusula (`ot_intenciones`, `mant_visitas.intencion`, `mant_ot_autorizaciones.intencion` y `mensaje_sistema`). Pruebas: `tests/test_ot_intenciones.py` (108; 18 mutaciones detectadas).
- **Quién recibe las autorizaciones:** `_ot_aut_receptores()` = superadmins menos quien tenga apagado el permiso `mant_ot_autoriza_aviso` (Roles → Permisos individuales): así se evita molestar a quien no debe, sin tocar código.

---

## 📊 REGLA #48 — «Costeo de órdenes de trabajo»: el informe de rentabilidad de Servicio Técnico es solo superadmin y solo lectura, y cada garantía lleva su trazabilidad (no negociable)

**Pedido explícito de Daniel (2026-10-25):** «cómo vamos semanalmente, cuánto ganamos, cuánto perdimos, cuánto generamos de garantías, a nombre de quién la estamos haciendo, cuánto nos cobran los técnicos, son reincidentes los externos, es rentable el departamento… que con nuestros propios datos ordenados y clasificados no nos puedan jugar a robarnos un poco de rol o protagonismo», y «necesito saber cuánto gané, cuánto perdí, cuánto cobré, cuánto me cobraron, todo en tarjetas resumidas… ¿Quién lo asume? ¿Por qué? Autorizaron».

- Pantalla `/ot/gerencial` y su Excel (`/ot/gerencial/excel`); botón «Costeo de OT» arriba en Órdenes de Trabajo, solo superadmin; nunca en el menú ni para un técnico (REGLAS #19 y #26: `@_no_tecnico` + `_ger_solo_superadmin`).
- **Solo lectura:** no escribe, no hace DDL, no consulta a Random ni a Check, no envía nada (REGLAS #4.1, #4.4, #18 y #22). Caché corta de 90 s; techo de 6000 OT leídas (si se toca, lo avisa).
- Abre con **Cobré · Me cobraron · Gané · Perdí · Neto** (cada una con «¿Cómo se calcula?» y su comparativo), debajo las garantías y coberturas con su trazabilidad, y recién después el detalle (tendencia de 12 semanas, a dónde se va la plata, técnicos y proveedores, reincidencia de externos, cada persona con lo suyo).
- Neto = Gané − Perdí − garantías y coberturas, con la suma única `_ot_fin_agregado` y la cuenta de cada OT es la MISMA de su ficha (`_ot_finanzas`). Lo que no se puede medir se cuenta aparte, nunca como $0.
- **Atribución en cuatro papeles separados** (originó, autorizó, lleva, ejecutó), cada uno con su fuente guardada; «Sin clasificar» y «sin autorización» en ámbar suave y nunca se inventan. «¿Qué es y por qué?» y «¿Quién lo asume?» salen de la clasificación de la REGLA #47 (`mant_visitas.intencion` + el centro de costo de la OT).
- **Reincidencia** de un externo (por defecto 60 días; 30/60/90 ajustable): una OT correctiva o de garantía sobre el mismo equipo (o el mismo cliente, si no hay equipos) abierta después de que ese externo cerró una OT. Es un indicador para revisar, no una sentencia: en ámbar, nunca en rojo.
- Toda la lógica va en `ot_gerencial.py` con sus pruebas (`tests/test_ot_gerencial.py`, 99; mutaciones detectadas) y `tests/test_ot_costeo_intenciones.py` (el enlace con la REGLA #47).

---

## 🔘 REGLA #49 — Hay UN SOLO «Generar OT»: todos los botones abren el asistente «Nueva OT» (OT2C); nadie mantiene su propia copia del formulario (no negociable)

**Pedido explícito de Daniel (2026-10-26):** «el botón de generar OT de la ficha de clientes no está tan actualizado; que sea uno solo, de manera que si hacemos un cambio en el modal de gestionar OT se aplique a todos».

- **Un solo formulario:** el asistente de `templates/ot2/_modal_crear.html` (`window.OT2C`). Lo abren, cada uno con el contexto que ya conoce, el panel de OT (`/ot/`), la ficha del **ticket** (`OT2C.abrirDesdeTicket`), **Repuestos**, los **hallazgos** (`abrirDesdeHallazgos`) y la ficha del **cliente** (`abrirLevantamientoSelector` en `static/mant_ficha.js`: «Generar OT», «Nueva visita», «Programar mantención», la agenda del plan anual y «Generar OT con los seleccionados» en `abrirVisitaMulti`, con el cliente y los equipos marcados ya puestos).
- **Cómo se trae a una pantalla:** con el cargador `<script src="…/ot_modal_lazy.js">` (el asistente pesa ~765 KB y se descarga bajo demanda de `/ot/modal-crear`; mientras no está cargado `window.OT2C` es un cascarón que espera y ejecuta la llamada). **Nunca** con `{% include "ot2/_modal_crear.html" %}` dentro de la página (`tests/test_ot_modal_lazy.py` lo vigila para las cuatro pantallas).
- **Pantalla nueva con un botón «Generar OT» → llama a `OT2C.abrir({...})`**: `origen` (`cliente` | `ticket` | `cotizacion` | `interno`), `cliente`, `tipo`, `fecha`, `equipo_preset_maquina_ids`, `onCreada`. No se copia el modal ni se arma otro formulario de OT: cualquier cambio del asistente (costos, anexo, motivos, checklists, agenda) tiene que llegar a todas las pantallas sin tocarlas.
- **Lo viejo no se borra sin permiso (REGLA #4.2):** el modal `#modalGenerarOT` (`tickets/_modal_generar_ot.html`, lo necesita `tickets_ficha.js` para cargar), `ot2/_modal_ot_form.html` y el formulario de la barra de selección (`abrirVisitaMultiAntiguo`) quedan en el código sin que ningún botón los abra.
- **Al terminar:** el asistente avisa «✓ OT creada»; la ficha del cliente cierra el asistente y se refresca (la persona se queda donde estaba, como en el ticket). Ya no se exige que el cliente tenga equipos registrados: el asistente sabe crear una instalación o un levantamiento sin ellos.
- Pruebas: `tests/test_ficha_cliente_generar_ot_unico.py` (JS ejecutado en Node: cliente, tipo, fecha y equipos viajan al asistente; cada botón pasa por el mismo punto) y `tests/test_ot_modal_lazy.py`.

---

## 🎫 REGLA #50 — Tickets es LA ticketera de la empresa: escala a 100.000 tickets, SLA de 4 h hábiles por mensaje, cada quien ve lo suyo (no negociable)

**Pedido explícito de Daniel (2026-10-27):** «este modelo de módulo es el que va a vender en todos los departamentos… necesito que tenga SLA de respuesta, que me marque en rojo… que sea una tiquetera profesional… estandarizado de 4 horas por respuesta en horas hábiles» y «los usuarios solo podrán ver los tickets suyos y los sin asignar».

- **Escala (medido en MySQL 8 con 100.000 tickets / 1.000.000 de mensajes, `tests/bench_tickets_rutas_mysql.py`):** la bandeja ordena por columnas con índice (`sin_leer`, `ord_estado`, `ord_prio` son generadas **VIRTUAL**: se agregan sin reconstruir la tabla) e `idx_tk_bandeja`. Nunca una subconsulta por ticket en el `ORDER BY`. Total y KPI en UN barrido con caché de 5 s. Filtros de fecha como rango sobre `created_at`, nunca `DATE(CONVERT_TZ(...))` por fila.
- **Latido, no recarga:** la ficha pregunta cada 3 s y la bandeja cada 4 s «¿cambió algo?» (`/tickets/api/tickets/<id>/latido`, `/tickets/api/bandeja/latido`, consultas por llave) y recargan solo si cambió la firma. Nada de volver a pedir el ticket entero en un temporizador.
- **SLA de respuesta = `tickets_sla.py` (puro, con pruebas):** 4 h hábiles (`TICKETS_SLA_HORAS`) por CADA mensaje del cliente sin responder; el reloj parte con el primer mensaje del cliente sin respuesta (o al crearse desde el formulario público) y se detiene cuando el equipo le contesta (`mensaje` no interno que no falló). Horario = el de Retiros (`retiros_monitor`: L-V 08-17, colación 13-14, feriados de `cl_feriados`): no hay otra definición. Semáforo: rojo vencido, ámbar queda ≤ 1 h, verde en plazo. Para filtrar y contar se calcula el **corte** una vez por minuto y la condición es `espera_desde <= corte` (indexada): nunca evaluar 100.000 tickets uno por uno.
- **Una sola puerta de escritura para el estado del ticket:** `_tk_log` mantiene `ultimo_cliente_at`, `espera_desde` y `ultima_resp_at` (y NUNCA mueve `updated_at`: `updated_at=updated_at`); `_tk_bandeja_conciliar` (incremental por id de mensaje) corrige lo que escriben otros caminos (`app.py`). Todo camino NUEVO que escriba en `tk_mensajes` debería pasar por `_tk_log`.
- **Responsables = permiso por persona** `tk_responsable` («Responsable de tickets», Roles → Tickets o Permisos individuales), apagado para todos; mientras nadie lo tenga, la lista sale de los roles de Tickets (nunca TODOS los usuarios). Una sola fuente: `/tickets/api/responsables` (filtro de la bandeja y selector de la ficha).
- **Visibilidad:** quien no es superadmin/admin ni tiene `tk_ver_todos` («Ver todos los tickets») ve SOLO sus tickets y los sin asignar. Se aplica en el SERVIDOR: `_tk_list_where_scoped` (bandeja, KPI, CSV), el contador de no leídos y un `before_request` sobre TODA ruta con id de ticket (403 / vuelta a la bandeja). La bandeja abre filtrada por el propio nombre (se puede quitar).
- **Recordatorios del SLA** (`_tk_sla_barrido`): campana (tipo `tk_sla`, al FINAL de los dos ENUM) + correo interno (`tk_sla_por_vencer`, `tk_sla_vencido`, módulo `comunicacion_interna`, REGLA #33) al responsable (si no tiene, a los responsables de tickets; si nadie tiene el permiso, a superadmin/admin). Una vez por mensaje y nivel (`tk_sla_avisos`), solo en horario hábil, tope 10 por pasada; el correo nace apagado (se enciende en Control de correos); la PRIMERA pasada siembra en silencio lo que ya estaba pendiente (sin avalancha). Corre colgado del latido de la bandeja y de `simpliroute-poll`. Nunca al cliente (REGLA #22).
- **Vista:** tarjetas KPI y semáforo con el MISMO estilo del Monitor de Retiros (`.rm-kpi*` de `retiros_monitor.css`), columnas «Responsable», «Tiempo · SLA» y «Última actividad», nombres completos sin cortar (REGLA #15). Ficha con pasos 1-5 en semáforo y tarjeta «Contactos» (`static/tickets_ficha_v2.*`).
- Pruebas: `tests/test_tickets_sla.py`, `tests/test_tickets_escala.py` y la de carga `tests/bench_tickets_rutas_mysql.py` (solo contra un MySQL de PRUEBA).

---

## 📧 REGLA #51 — Los lotes de Facturación de proveedor se envían por correo marcándolos: UN correo con el Excel de cada lote y los montos en el cuerpo, solo al equipo interno (no negociable)

**Pedido explícito de Daniel (2026-10-28), mostrando el correo que armaba a mano en Outlook:** «necesito crear un envío de lotes seleccionados, con un mensaje y los Excel y los montos en el mail, y un cuerpo de correo bien automático, que soy flojo».

- **Cómo se usa:** en `/servicio-tecnico/facturas-proveedor` cada lote (menos los anulados) trae su casilla «Enviar» y la barra negra de abajo dice «N lotes marcados · $total» con **«Enviar por correo»**. «Seleccionar/deseleccionar todo» marca los de la página (REGLA #14). El modal (`#fpeModal`, `static/facprov_envio.*`) muestra los lotes con su nombre completo, **cómo se llama cada proveedor en el correo** (editable), a quién va (Para / Con copia, con selector de personas), el asunto, el saludo, el mensaje de antes y de después de los montos y la **vista previa** exacta del correo con los Excel que se adjuntan. «Enviar ahora» pide una confirmación que lista a quién llega; «Borrador para Outlook» baja un `.eml` con `X-Unsent: 1` (Outlook lo abre SIN ENVIAR, con destinatarios, cuerpo y Excel puestos, para mandarlo con la firma propia).
- **El cuerpo siempre lo arma el servidor** (`facprov_envio.py`, puro, con pruebas): saludo («Hola Nicolás,»), «Un gusto saludarte. Te envío el cierre de los proveedores con los siguientes montos:», lista numerada «1. Felcarm $4.750.000», «Total», cierre y firma con nombre y correo del que envía. El navegador solo manda los ids y lo que la persona editó; el HTML nunca viaja desde el navegador y todo lo escrito se escapa. El nombre corto sale del nombre canónico del proveedor («LOGISTICA Y TRANSPORTES MILLING SPA» → «Milling»); si la persona lo cambia («Pulgar») **se recuerda** para la próxima vez, igual que a quién se lo mandó, con copia a quién y cómo saludó a cada destinatario (4 reglas `facprov_envio_*` en `mant_reglas_negocio`, categoría `facprov`, sembradas con `INSERT IGNORE` vacías por `_ensure_facprov_envio_reglas`).
- **Solo equipo interno (REGLAS #19 y #26):** el correo lleva lo que ILUS le paga a cada proveedor. Solo `@sphs.cl` / `@ilusfitness.com` (dominio exacto) y **nunca un usuario de la familia técnico**, aunque su correo sea interno (`_fpe_correos_validos`, revisado en el SERVIDOR en la previa, el envío y el borrador). Máximo 10 destinatarios y 30 lotes por correo, adjuntos hasta 15 MB. Las tres rutas (`/api/facturas-proveedor/envio/previa|enviar|borrador`) llevan `@_mant_required @_no_tecnico` y `_facprov_puede()` antes que nada. Un lote anulado o inexistente no se envía.
- **Sale por la puerta única (REGLAS #13 y #33):** `_send_ilus_email(..., modulo="comunicacion_interna", correo="facprov_cierre_lotes")`, UN correo (Para el primero, el resto con copia) con `reply_to` del que envía. Está registrado en `comm_correos_registro.py` (interno, manual): el interruptor general, la llave de Comunicación interna y su interruptor en Control de correos lo pueden apagar. **Se avisa ANTES de enviar** si hoy no saldría (franja ámbar y «Enviar ahora» deshabilitado; el borrador sigue disponible) y **se dice la verdad si no salió** (`_email_resultado`): en ese caso no se anota ni se recuerda nada como enviado. Al salir, cada lote deja en su historial «Enviado por correo» (quién, cuándo, a quién, monto y archivo).
- **El Excel adjunto es el mismo que se baja del detalle del lote:** `_mfp_excel_de_lote(fid)` es el ayudante único de la descarga y del correo; nadie arma una segunda versión.
- Todo cambio al texto automático del correo se hace en `facprov_envio.py` con su prueba (`tests/test_facprov_envio.py` fija que el cuerpo es idéntico al que Daniel escribía a mano). Pruebas: `tests/test_facprov_envio.py` (módulo puro), `tests/test_facprov_envio_ruta.py` (rutas sobre base y correo de mentira) y `tests/test_facprov_envio_js.py` (JS en Node). Se prueba SIEMPRE con datos y correo de mentira (REGLA #22).

---

## 📜 REGLA #52 — Eliminar, cancelar o anular una OT deja su anexo (firmado, enviado o visto) sin efecto «por la anulación del servicio» y se avisa por correo a las DOS partes, SOLO con texto (no negociable)

**Pedido explícito de Daniel (2026-10-28, «es OT, no UTS»):** «cuando eliminemos una OT y el anexo está firmado, que se le envíe al correo que el anexo queda inválido por la anulación del servicio. Por ambas partes». Ampliado el mismo día: lo mismo para un anexo **enviado o visto** sin firmar, lo mismo al **cancelar o anular** la OT sin eliminarla, un botón para **reenviar el aviso** y la pantalla **«Anexos anulados»** para los huérfanos. **A pedido de Daniel el aviso NUNCA lleva el anexo adjunto (ni un PDF): es solo un correo de texto que notifica.**

- **Qué pasaba antes:** `mant_anexos.ot_id` es `ON DELETE SET NULL`, así que al borrar la OT el anexo firmado quedaba huérfano, seguía «firmado» y no se avisaba a nadie (el estado `anulado` existía en la tabla y ningún código lo escribía).
- **Una sola puerta, tres tiempos** en `_mant_visita_eliminar_core` (la usan la eliminación individual y la del lote): (1) **lee antes** (`_anexo_anul_plan`, solo lectura; fail-CLOSED `ANEXO_NO_VERIFICABLE`: si no se puede saber, no se borra); (2) **anula dentro de la transacción y antes del DELETE** (`_anexo_anul_marcar`: primero RE-LEE y bloquea las filas con `SELECT … FOR UPDATE`, después `estado='anulado'` + `anulado_at/anulado_por/anulado_motivo/anulado_ot_numero` con `WHERE estado IN ('firmado','enviado','visto')`: si el proveedor firma justo ahora, su UPDATE espera y después ve «anulado», y si firmó un instante antes el anexo se anula como firmado; `ot_id` queda en NULL por el FK al eliminar, por eso el número de la OT se guarda en el propio anexo); (3) **avisa después del commit** (`_anexo_anul_avisar`, best-effort: un correo que falla o revienta jamás deshace el borrado). Reglas puras y probadas en `anexo_anulacion.py`.
- **Constancia:** bitácora del anexo (`anulado_ot_eliminada`, `anulado_aviso_proveedor`, `anulado_aviso_interno`), de la OT (`anexo_anulado`), del cliente (la de siempre) y, si la OT estaba en un lote de proveedor (pendiente o pagado), del lote (`anexo_anulado_ot_eliminada`) y en el correo interno. Hora Chile. El libro de saldo (REGLA #28) y los lotes (REGLA #29) siguen su camino de siempre: esto no escribe en `ot_fac_libro`.
- **«Ambas partes» = el proveedor e ILUS.** *Proveedor:* correo `anexo_anulado_proveedor` (llave General, como el envío del anexo) a donde se mandó el enlace de firma (`enviado_email`) y al contacto de su ficha (`mant_tecnicos_externos.contacto_email`); **sin montos, márgenes, cliente ni datos de otros proveedores**; con el N° de anexo, la OT, la fecha de firma y soportetec@sphs.cl. *ILUS:* correo `anexo_anulado_interno` (Comunicación interna) a quienes decide **`_ot_gestion_destinatarios`** (responsable, creador, ejecutivo del cliente; nunca otra lista) y **jamás un técnico** (REGLAS #19 y #26); dice quién eliminó, el lote de proveedor y **si el aviso al proveedor salió**. Al CLIENTE final jamás (REGLA #22). Sin correo del proveedor no revienta: queda dicho en pantalla y en el correo interno («hay que avisarle por otro medio»). Salen por la puerta única con su clave del registro (REGLA #33), por SMTP (REGLA #13), **sin adjuntos**; los dos nacen encendidos y se apagan en Control de correos.
- **ANEXO ÚNICO (REGLA #46):** si la OT eliminada es la **dueña** del anexo (la instalación, o la OT que lo emitió) el anexo queda **inválido completo**. Si es la **desinstalación que comparte** el anexo firmado de su instalación, el anexo **NO se invalida** (la instalación lo sigue usando y un anexo firmado no se edita): sigue «firmado», se avisa a las dos partes que las partidas de la desinstalación quedan sin efecto y queda la constancia (`anexo_partidas_sin_efecto`). Nada que decidir a mano: lo resuelve el plan.
- **Anexo ENVIADO o VISTO (sin firmar):** también pasa a `anulado`; su enlace `/firmar-anexo/<token>` deja de permitir firmar (el UPDATE de la firma solo acepta `enviado`/`visto`) y se avisa al proveedor «el anexo N° X que te enviamos para firmar quedó sin efecto» y a ILUS. Un **borrador** nunca enviado no se avisa; `vencido` y `rechazado` no cuentan como vivos. En un anexo compartido sin firmar la desinstalación que se elimina o cancela **pierde sus partidas** (un anexo sin firmar sí se corrige) y se avisa si ya se había enviado.
- **CANCELAR o ANULAR la OT (sin eliminarla):** la ÚNICA puerta que escribe esos estados es `PUT /mantenciones/api/visitas/<id>` (`mant_visita_update`; la cascada de la desinstalación va en la misma transacción y anula también los anexos propios de la hija). Las tres piezas son las mismas (plan → anular dentro de la transacción → avisar después del commit; motivo «anulación del servicio (OT cancelada/anulada)»); aquí el anexo queda `anulado` **ligado a su OT** (no es huérfano). El aviso previo se **EXIGE en el servidor**: el PUT responde 409 `ANEXO_REQUIERE_CONFIRMACION` con el aviso hasta que llegue `confirmar_anexo`, así ninguna pantalla se lo salta; `static/ot_cancelar.js` (`OTCancelar.enviar`: calendario, calendario del día y ficha del ticket) lo muestra con `ilusConfirm` y reenvía con `confirmar_anexo: 1`; una pantalla que no lo conozca muestra el mensaje del 409 (y se cancela desde la ficha o el calendario). Falla CERRADO (`ANEXO_NO_VERIFICABLE`). Lógica en `_anexo_anul_puerta_previa`.
- **REACTIVAR una OT cancelada o anulada SÍ es posible** (nada lo impide: es el mismo PUT con otro estado; el propio calendario dice «la puede revertir un administrador editando la OT»). **La reactivación NO revive el anexo anulado en silencio:** el aviso previo de cancelar lo advierte («si la reactivas, habrá que emitir un anexo nuevo»); al reactivar, el PUT responde `anexo_reactivada` y `warning` con «el anexo N° X sigue anulado: hay que emitir uno nuevo» y lo deja en la bitácora (`anexo_sigue_anulado`); y la ficha de la OT muestra la tarjeta «Anulado — por la anulación del servicio» (quién, cuándo, motivo) con «Crear anexo de servicios» (una OT de proveedor externo sigue bloqueada para el técnico sin anexo firmado). Código: `_anexo_anul_reactivada`.
- **«Reenviar aviso del anexo anulado»** (solo gestión, nunca técnico: `@_no_tecnico`; `static/ot_anexo_aviso.js`): en la tarjeta del anexo de la ficha de la OT y en cada fila de **«Anexos anulados»** (`/ot/anexos-anulados`, paginada, también los huérfanos de una OT eliminada, que no se ven en ninguna ficha; enlazada en la barra de superadmin del panel de OT y desde el correo interno). `GET /ot/api/anexos/<id>/aviso-anulado` muestra ANTES a quién llegaría (proveedor / ILUS, con el motivo si no hay correo o está apagado); se elige proveedor y/o ILUS (con «marcar / desmarcar todo», REGLA #14) y se puede agregar un correo (recibe la versión del proveedor); `ilusConfirm` antes de enviar; `POST /ot/api/anexos/<id>/reenviar-aviso` usa la misma puerta única y las mismas claves del registro (REGLA #33), dice la verdad por destinatario (502 si no salió ninguno) y deja constancia (`aviso_anulado_reenviado`: quién, cuándo, a quién) en la bitácora del anexo y de la OT. Para un huérfano ILUS = el respaldo de la gestión (superadmin/admin/supervisor).
- **Se avisa ANTES de confirmar** (alertar, no sorprender: REGLA #37): `GET /ot/api/<id>/anexo-anulacion` (solo quien puede eliminar la OT, nunca un técnico) usa el MISMO plan que el borrado y dice qué anexo queda inválido, a quién se le avisará y si ese correo hoy no saldría (apagado). Lo dibuja `static/ot_anexo_anulacion.js` (cargado en `base.html`) dentro del `ilusPrompt` de la eliminación en la ficha de la OT, el calendario y la ficha del cliente; el borrado en lote suma un aviso ámbar con la cantidad de anexos firmados. Después de eliminar, la pantalla dice **la verdad por destinatario** (salió / no salió y por qué / sin correo).
- **Lo que ve el proveedor con su enlace viejo:** `/firmar-anexo/<token>` de un anexo anulado muestra «Este anexo quedó inválido por la anulación del servicio», sin datos del anexo ni formulario de firma.
- **Fuera de alcance (a propósito):** el reset total de OT (`ots_reset`) y la limpieza de OT auto-creadas sin firmas (`borrada_batch_auto`) borran OT sin pasar por este flujo (el primero es un reinicio del sistema; la segunda solo toca OT «programadas» sin firmas ni trabajo). Las pantallas que guardan el estado con un `PUT` directo (el modal de edición de la ficha del cliente o del listado) no muestran el aviso previo bonito, pero el servidor igual lo exige y les devuelve su mensaje.
- Pruebas: `tests/test_anexo_anulacion_ot_eliminada.py` y `tests/test_anexo_anulacion_2.py` (módulo puro, base y correo de mentira, JS en Node).

---

## 🧩 REGLA #53 — Bodega de incidencias: una DIFERENCIA se gestiona unidad por unidad (equipo = una línea y un motivo por UA; producto repetible = una línea con la cantidad) (no negociable)

**Pedido explícito de Daniel (2026-10-28), con la ILUS Air Ski Erg Machine (ERP 2, Check 2, nosotros 1):** «la idea de esta bodega es separar: 90 pisos van en una línea, pero 2 máquinas van separadas porque tienen que tener el motivo distinto por el que están en la bodega… necesito un menú inteligente que cuando yo vea diferencia me abra la diferencia y me diga: esta tiene motivo, esta tiene motivo, falta agregar uno… con objetos pequeños inteligentes» y «me gustan los modales grandes».

- **Panel «Gestionar diferencia»** (`#incDifModal`, `incAbrirDiferencia` en `templates/mantenciones/incidencias.html`): en Revisar diferencias, toda diferencia de cantidad («dif_erp» y «dif_wms») trae `unidades` (lo arma el servidor con `_inc_unidades_hallazgo`, pura y con pruebas) y su botón principal es **Gestionar** (con «falta 1» / «sin motivo»). Una tarjeta pequeña por unidad: **Con motivo** (verde, el motivo completo), **Falta el motivo** (ámbar: menos de 10 caracteres), **Falta agregar** (rojo: una UA que Check tiene en la Bodega 13 sin incidencia → «Registrar esta UA»), **Falta registrar N** (producto repetible: UNA línea con la cantidad) y **Sin UA en Check** (faltan unidades pero no hay otra UA libre: hay que separarlas en Check). Arriba, «N de M unidades con motivo» con su barra. Cada tarjeta reutiliza lo que ya existía: abrir la incidencia, dar de baja o el alta prellenada con esa UA.
- **Equipo vs repetible** sale de la clasificación del Catálogo (`_inc_clasificacion_skus_batch`, `repetible`): lo que no está clasificado cuenta como equipo (nunca se asume que agrupar está bien). Una UA solo puede estar en una incidencia activa (`_inc_ua_duplicada`), por eso la UA que se propone es siempre una **libre** (`_inc_uas_libres`), nunca la de la incidencia que ya existe; con varias libres el alta deja elegir cuál.
- **«Ver / corregir» no se pierde** (REGLA #4.2): pasa al menú ⋮ de la fila («Ver / corregir la incidencia») y a cada tarjeta. Las diferencias sin detalle por unidad siguen con sus botones de antes, y ahora un SKU con incidencia al que le faltan unidades también ofrece **Registrar** (`faltan`, `puede_registrar`).
- **Buscador** en el panel de diferencias (UA, SKU, producto, ubicación y la frase de «qué significa», sin tildes ni mayúsculas, varias palabras = todas deben coincidir); vaciarlo vuelve a calcular la tabla (REGLA #4.3).
- **Modal ancho** (REGLA de modales grandes): el alta/edición (`#incModal`) pasa a `modal-xl` (hasta 1360 px) y desde 1100 px las tarjetas 1-4 van de a dos columnas; «Gestión de repuestos» y «Evidencia» quedan a ancho completo. En celular, un solo carril y pantalla completa.
- **Registrar 2 o más unidades pregunta cómo** (Daniel, 28-oct: «se puede mantener en una línea a menos que el usuario decida separarla; si no, lo más recomendable es pedir crear varias filas si el ingreso es mayor a uno»): `incRegistrarConPregunta` → `#incAgruparModal`. Con 1 unidad va directo al alta. Con 2 o más ofrece **Separar en N filas** (recomendado en equipos) o **Dejar en una sola línea** (recomendado si el producto es repetible). «Separar» usa el desglose por motivo (`/incidencias/desglosar`, origen «hallazgo»), que ahora también acepta un SKU que ya tiene incidencia pero al que le faltan unidades (`puede_registrar`, se reparten solo las que faltan) y le asigna a **cada fila su propia UA libre de Check** (`_inc_asignar_uas_grupos`; si no hay, la primera conserva la UA del pendiente y las demás quedan sin UA, como antes). Ningún camino abre el alta de varias unidades sin preguntar.
- **UNA fila por SKU** (Daniel, 28-oct: «dice que hay tres diferencias, es confuso»): la misma máquina salía como «Diferencia con el ERP», «Diferencia con Check» y «Falta registrar» de su UA. `_inc_consolidar_por_sku` deja UNA fila (la del ERP, con `tambien_wms`; la tarjeta «Pendiente CheckWMS» la sigue contando) y absorbe los «Falta registrar» cuya UA ya es una tarjeta «Falta agregar» de esa fila. En la fila, las unidades salen como fichas por UA y motivo (verde con motivo · ámbar falta el motivo · rojo falta agregar), y el detalle ⌄ trae «Por UA y motivo». La UA se muestra una sola vez («UA1014361», nunca «UA UA1014361»: `incUaTxt`).
- **«Pendiente dividir en UA en Check» es una DIFERENCIA** (`_inc_pendiente_dividir`, tarjeta «Dividir UA en Check»): varias unidades juntas en una misma UA de un equipo, o faltan unidades y Check no tiene otra UA libre.
- **«Actualizar» salta las cachés** (Daniel: «se hizo un traslado hoy a la bodega 13 y no salen las diferencias»): la lista lee el ERP con caché de 15 min y Check de 1 h; `?forzar=1` (solo el botón Actualizar) las salta. Cuando la búsqueda no encuentra nada, «¿Por qué no aparece?» consulta EN VIVO el SKU o su final («termina en 101054») o la UA en el ERP (todas las bodegas, SOLO LECTURA: REGLA #4.1), en Check (REGLA #4.4) y en nuestras incidencias, y lo explica en palabras simples (`_inc_veredicto_producto`: por ejemplo «el stock de la bodega 13 recién se mueve cuando el traslado se recepciona en Random»). Busca también por **nombre** (`MAEPR.NOKOPR LIKE`, descripción de Check), no solo por códigos, y **solo la bodega 13 cuenta** (el stock de otras bodegas se muestra aparte y no entra a la comparación). **Se dispara sola** (~0,7 s) cuando el buscador no encuentra nada. Si lo escrito es un **documento** («GRI 2721», `_inc_doc_ref`), además busca ese documento en el ERP (`MAEEDO`/`MAEDDO`: si existe, fecha, estado, anulado o no, sus líneas y las columnas de bodega) y lo cruza con el stock (`_inc_doc_texto`). Nunca escribe nada.
- **Dar de baja inteligente, corto y con posiciones** (Daniel: «es importante llevar una trazabilidad y poder dar de baja… un proceso inteligente, corto, intuitivo y con las bodegas, ya que puedes identificar las posiciones en Check»): al abrir la baja se muestra **dónde está hoy** la UA en Check (bodega + posición, de la caché; `_inc_posiciones_check`) y queda **sugerido** el motivo que corresponde (`_inc_baja_sugerencia`: salió de la Bodega 13 → «Salió de la bodega…», diciendo dónde está ahora; nuestra BD, ERP y Check cuadran → «Ya está regularizado»); la persona solo confirma. La baja guarda esa posición (`bodega_check`, `ubicacion_check` en `mant_incidencia_bajas`, calculadas por el SERVIDOR; dos columnas nuevas, una sentencia cada una y baratas si ya existen: REGLA #18; si faltan, la baja funciona igual) y el historial «Dadas de baja» la muestra.
- **ELIMINAR es solo del superadministrador** (Daniel: «solamente yo puedo eliminar productos de la base de datos, para mantener la trazabilidad histórica de la bodega»): servidor (`mant_api_incidencias_borrar` → 403 `SOLO_SUPERADMIN`) y pantalla (`incCanDelete`, botón del modal). Sigue siendo un borrado lógico con bitácora antes (REGLA #5). Todos los demás usan «Dar de baja»: motivo, quién, cuándo, posición en Check, y se puede reactivar.
- **Cuatro fuentes, una sola comparación (29-oct, ILUS Rower Pro – Conditioning Series, SKU 1049101054, traslado GRI 2721).** Daniel: «nuestra base se compara numéricamente con el ERP y con el WMS… si en la bodega 13 está y en Check no, hay que hacer un ingreso en Check; y si en nuestra bodega tampoco, hay que hacer un ingreso y declarar motivo, pedir repuesto y toda la gestión… también diferencias de UA… que me indique que está en el ERP pero todavía no está hecha la gestión en Check». `_inc_calcular_hallazgos` compara **solo la bodega 13** (ERP `MAEST.KOBO='13'`, Check «BODEGA 13», nuestras incidencias):
  - Un SKU que el ERP tiene en la 13 y **Check no tiene** ya no se salta en silencio: sale como diferencia con tarjeta **«Falta ingreso en Check»** (`falta_check`: unidades que el ERP tiene y Check no; `check_pendiente` si ya la declaramos sin UA) y el aviso «En el ERP · falta la gestión en Check» (`falta_ingreso_check`, KPI «Ingresar en Check»). Se puede **registrar de inmediato sin UA** (`incAltaSinUaOk`: la UA se asigna cuando Check ingrese) y «Copiar pedido para Check» **solo copia un texto**: Check sigue SOLO LECTURA (REGLA #4.4).
  - **Diferencias de UA:** una incidencia declarada SIN UA a la que Check ya le tiene una **UA libre** del mismo producto sale con `ua_sugerida` y un hallazgo propio **«Asignar UA de Check»** (`asignar_ua`, KPI «Asignar UA») aunque los números cuadren. El botón solo abre la incidencia con la UA rellenada: la persona revisa y guarda (nada se escribe sola; la UA sigue siendo única por incidencia).
  - **Actualizar** salta las cachés del ERP y de Check (`forzar`) y el buscador, cuando no encuentra, consulta en vivo por **nombre**, SKU, UA o documento (ver arriba).
- **«Gestión de motivos»** quedó construida el 29-oct: ver REGLA #54. Sigue pendiente, de su macro antigua, una vista **ERP vs nuestra BD sin Check** (preguntarle si la quiere como filtro o pantalla aparte).
- Pruebas: `tests/test_incidencias_diferencias_28oct.py` (diferencias con base y WMS de mentira, buscador y JS en Node).

---

## 🏷️ REGLA #54 — Bodega de incidencias: el MOTIVO es el texto detallado y el TIPO de motivo se gestiona aparte; el sistema sugiere y la persona decide (no negociable)

**Pedido explícito de Daniel (2026-10-29): «Sí, quiero la gestión de motivos».** Antes (27-sep) había sacado los motivos «rápidos»: el motivo sigue siendo el texto detallado (mínimo 30 caracteres, «el campo más importante») y **no se reemplaza ni se acorta**. Lo que se suma es el **tipo de motivo**, para contar, ver y analizar la bodega por causa.

- **Catálogo** `mant_incidencia_motivo_tipos` (`codigo` estable, `nombre`, `orden`, `activo`): lo administra solo **admin/superadmin** (agregar, renombrar, activar/desactivar; `_inc_motivo_puede_editar_catalogo`). **Un tipo NUNCA se borra** (REGLA #4.2): se desactiva y las incidencias que lo tienen lo conservan; el código no cambia al renombrar. Se siembra la lista base (`INC_MOTIVO_TIPOS_BASE`) **solo si la tabla está vacía**, con `INSERT IGNORE`: jamás pisa lo que el admin cambió.
- **Clasificación** `mant_incidencias.motivo_tipo` (código del catálogo, opcional): se elige en el formulario («Tipo de motivo», paso 2), en **«Por clasificar»** (una a una o varias marcadas, con «Seleccionar / deseleccionar todo»: REGLA #14) o al **filtrar**. Cada cambio queda en la bitácora de la incidencia (`_inc_log`, acción `clasificada`: quién, cuándo, de qué a qué). Es **opcional** («alertar, no trabar»): lo que no tiene tipo se ve en «Por clasificar» y en el filtro «Sin clasificar».
- **REGLA #32: el sistema SUGIERE, la persona DECIDE.** `_inc_motivo_sugerir` lee el texto (sin tildes ni puntuación; una palabra cuenta si está entera: «uso» no es «usuario») y propone un tipo **activo** con **las palabras que lo motivaron** («porque dice «golpeada», «transporte»»). Nunca lo guarda solo: se acepta con «Usar este» o se elige otro. Las pistas viven en un solo lugar (`_INC_MOTIVO_PISTAS`); el formulario las pide a `/incidencias/motivos/sugerir`.
- **Pantalla «Motivos»** (botón junto a «Dadas de baja», insignia con lo que falta clasificar; modal ancho `#incMotModal`): **Resumen** (por tipo: incidencias, unidades, abiertas, resueltas, días en bodega promedio y la más antigua; clic en un tipo filtra la tabla con `f_motivo` + `ver_todo=1` para que cuadre con los números), **Por clasificar** (paginada: Mostrando A–B de N, N por página, Anterior/Siguiente; nombres y motivos completos, nunca truncados) y **Catálogo**. Filtro «Tipo de motivo» en la barra de la tabla, y el nombre del tipo como chip bajo el motivo de cada fila.
- **Nunca rompe lo de antes:** la columna y la tabla se crean al arrancar con una sentencia cada una (REGLA #18; también con `ILUS_SKIP_MIGRATIONS=1`); el INSERT/UPDATE principal de la incidencia **no se toca**: el tipo se guarda aparte y a mejor esfuerzo (`_inc_motivo_guardar_tipo`, solo si el formulario manda `motivo_tipo` y la columna existe, `_inc_motivo_col_ok`). El formulario solo manda el tipo si el catálogo cargó (si no, no toca el que ya tenía).
- **Excel de la bodega** (solo admin): trae la columna «Tipo de motivo» junto al motivo y el bloque «Por tipo de motivo» en la hoja «Resumen» (con «Sin clasificar»). El nombre del tipo es un extra: si el catálogo falla, el Excel sale igual.
- Pruebas: `tests/test_incidencias_motivos_29oct.py` (sugerencia, catálogo, resumen, guardado con base de mentira, migración, permisos y pantalla).

---

## 🖨️ REGLA #55 — El PDF de la OT se puede imprimir como BORRADOR sin firmas; el documento firmado sigue siendo el único que sale hacia el cliente (no negociable)

**Pedido explícito de Daniel (2026-10-29): «vayamos con la gestión de impresión de la OT sin necesidad de las firmas»** (hay veces que hay que bajar el PDF antes de la tarea). Hasta entonces `_ot_pdf_context` exigía OT cerrada con las 3 firmas y, si no, mostraba «PDF aún no disponible».

- **Un solo candado, dos modos.** `_ot_pdf_context(vid, …, borrador=False)`: el documento **final** exige `cerrada` + firma del cliente, del técnico y del responsable, exactamente como antes. Con `borrador=True` se salta SOLO eso y solo se rechaza una OT **cancelada o anulada** (`_OT_PDF_BORRADOR_ESTADOS_NO`). El borrador fuerza `publico=True`: **nunca lleva costos, márgenes, proveedores ni autorizaciones** (REGLAS #19 y #26), lo pida quien lo pida.
- **Solo lo piden las dos rutas internas del documento** (`mant_visita_pdf` y `mant_ot_pdf_render`, con `?borrador=1`). **Jamás** el enlace público `/ot-firmada`, el WhatsApp, el correo «OT terminada» (REGLA #37) ni el anexo fotográfico: eso sigue exigiendo OT cerrada y firmada. Hay una prueba que cuenta los llamadores.
- **Hoja de trabajo, no informe de cierre** (`_ot_pdf_borrador_ajustar`, pura): se quita la alarma («Hallazgos y excepciones», «obligatorias sin completar», «equipos sin revisar», checklist en 0 %) y cada tarea sin respuesta queda con una **casilla vacía** dibujada con CSS (el glifo «☐» no existe en las fuentes del servidor). Lo que el técnico ya registró (respuestas, fotos, firmas si existen, equipos «no revisado» o de baja) se mantiene: es un dato, no una alarma. Los recuadros de firma quedan **en blanco** para firmar a mano. El resumen dice cuántos equipos y tareas hay por hacer; la constancia final se reemplaza por «Borrador sin valor de cierre».
- **Imposible confundirlo con el firmado:** marca de agua «BORRADOR» en cada página (`position:fixed`), franja ámbar en la primera, «BORRADOR · SIN CERRAR NI FIRMAR» en el encabezado y «BORRADOR» en el pie nativos, y el archivo se llama `BORRADOR-OT-…pdf`. Colores: ámbar y gris (un borrador no es una falla). Plantillas: `_ot_pdf_borrador.html` (macros compartidas), `ot_pdf.html` y `ot_pdf_levantamiento.html`.
- **Quién y desde cuándo:** quien puede ver la OT (`_ot_can_view`): gestión y el técnico asignado, desde cualquier estado salvo cancelada o anulada. En la ficha el mismo botón dice **«Imprimir borrador»** mientras la OT no está cerrada y **«Ver PDF»** cuando sí; la página «PDF no disponible» también lo ofrece.
- **Trazabilidad:** cada impresión queda en la bitácora de la OT (`pdf_borrador`: quién, cuándo, en qué estado estaba la OT), sin montos. Un técnico no la ve en su bitácora (lista blanca de REGLA #26).
- Pruebas: `tests/test_ot_pdf_borrador.py` (transformación pura, candado del documento final, llamadores, y las plantillas renderizadas de verdad en las dos modalidades).

---

## 💵 REGLA #56 — Los montos de cobro SE DECLARAN a partir de un documento: jamás se inventan, y el costo de la OT y el del anexo son UN solo monto (no negociable)

**Pedido explícito de Daniel (2026-10-29), con las OT-150, 151, 152 y 158: «dejalo como regla… que se declaren los montos de cobro, créeme es muy importante y peligroso para no inventar»** y «las finanzas o los costos son muy importantes: se tienen que llevar para KPI y para mejorar la toma de decisiones». Ejemplos reales: la factura FCV 11348 estaba declarada en la OT-151 pero «Cobré» seguía en $0 y el modal pedía escribir el cobro a mano; el anexo de la OT-158 decía $30.000 y la OT no decía nada.

- **Un cobro tiene origen, o no existe.** «Cobré» sale SOLO de lo declarado en la OT (`zz_monto` y `zz_envio_monto`) con su respaldo: las líneas de servicio y despacho de un documento de Random (factura o boleta), una cotización, un contrato o un cobro escrito a mano **con su motivo**. Un «precio anotado» suelto (por ejemplo el total de una factura con productos) **no es un cobro** (`_ot_finanzas`). El sistema nunca rellena un monto «por parecido» ni «por si acaso».
- **Los documentos declarados alimentan el cobro, no a mano.** «Corregir finanzas» (solo superadmin) trae lo que aportan los documentos YA declarados (`_ot_fin_docs_cobro`: servicio + despacho, con el saldo por línea de las REGLAS #25 y #28: lo que otras OT ya usan no se puede cobrar de nuevo) y, si la OT se cobra y no tiene cobro declarado, lo **prellena solo** con su origen a la vista; **nada se guarda hasta «Guardar corrección», con su motivo** (que también queda sugerido). El **servidor recalcula** al guardar y decide el rótulo (`_ot_fin_corr_desde_docs`): solo si el monto es EXACTAMENTE lo que aportan los documentos queda como «línea del documento» (`valor_origen='zz'`, con su línea ZZ); cualquier otro, como «escrito a mano». El navegador solo avisa que lo intentó, nunca fija el rótulo.
- **Nunca a ciegas:** si Random no entregó un documento, o es de otro RUT, o no trae líneas con saldo, **no se calcula nada** y se dice por qué. Leer los documentos no escribe nada (REGLA #4.1).
- **Si no hay documento, se trae desde el mismo modal** («Traer factura o boleta»): se valida contra Random (solo lectura) y se asocia por el MISMO camino de «Agregar factura» (`/documentos/regularizar`: RUT, duplicado, nota de venta dada de baja); después el cobro se prellena solo.
- **Trazabilidad de Random manda (en construcción, siguiente despliegue):** si la OT tiene una nota de venta y Random ya la facturó, **lo cobrado es lo de la factura**, salvo que Daniel lo quiera editar (decisión suya, con su motivo). El camino es el de siempre: agregar esa factura a la OT (la nota queda dada de baja por ella y la factura pasa a ser el principal).
- **El documento MANDA sobre un valor escrito a mano (Daniel, OT-190, 29-oct: «si sustituyo un valor a mano, ya no debería existir el valor a mano a menos que yo lo autorice… que siempre predomine el valor del documento»):** al agregar una factura o boleta con líneas de servicio/despacho (`ot2_api_documentos_agregar`; también el camino de regularizar), el cobro de la OT pasa a ser lo que aportan los documentos vivos y el origen queda «línea del documento». Antes, un documento ADICIONAL se SUMABA encima del valor a mano y este seguía sin origen. Reglas: (1) vale para un cobro sin origen («declarado»), «escrito a mano» sin motivo o un estimado; (2) **un cobro a mano CON motivo es una decisión de una persona y no se toca**: el documento queda de referencia (sin aporte, para que «quitar» no reste de más) y se avisa; (3) el documento solo manda sobre la categoría de la que habla: si trae solo despacho, el servicio escrito a mano NO se borra (nadie decidió perder esa plata), pero queda a la vista como «sin respaldo»; (4) deja constancia en la bitácora (`valor_a_mano_reemplazado`: qué valor dejó de existir y quién) y en el registro inmutable de correcciones (`_ot_fin_registrar_automatico`). Un cobro que ya viene de documentos sigue sumando; el primer documento sigue fijando el valor.
- **Un documento emitido a OTRA empresa (otro RUT) no se cuenta como cobro, pero ya no bloquea a los demás** (`_ot_fin_docs_cobro` → `otra_empresa`): solo un documento que Random no entregó impide calcular (OT-190: una boleta de otro RUT dejaba la OT sin poder tomar el cobro de su factura buena).
- **RESTRICCIÓN: «Corregir finanzas» no cambia lo que se le paga al proveedor por otro camino que su anexo (Daniel, 29-oct: «si se modifica lo que me cobraron, ahí tenemos que modificar el anexo, y es peligroso: que tengamos restricciones; yo puedo hacer de todo, pero a veces puedo pasar a llevar un anexo»).** Con un anexo VIVO (borrador, enviado, visto, vencido o firmado) `costo_proveedor + costo_despacho` es UN solo valor (REGLA #34) y tiene que sumar EXACTAMENTE el total del anexo: el servidor rechaza con 409 `COSTO_ES_DEL_ANEXO` (`_ot_fin_anexo_restriccion`, antes de escribir nada, ni la corrección ni su registro) cualquier cambio que lo deje distinto o sin declarar; igualar la OT al anexo siempre se puede. El modal bloquea los dos casilleros, explica por qué y ofrece «Igualar la OT al anexo» (`/correcciones` manda `anexo.igualar`). Para cambiarlo de verdad: corregir el monto en el anexo (si no está firmado) o **sustituir el anexo** (si está firmado: un anexo firmado es evidencia y no se edita ni con superadmin). **Pendiente (en construcción, con el «sí» de Daniel):** la acción «Sustituir el anexo» (anular el firmado con quién/cuándo/por qué + emitir uno nuevo en borrador con el monto correcto para firma). Hoy no existe esa salida en pantalla (hay una sesión aparte trabajando la anulación de anexos, rama `feat/anexo-anulacion-2`: coordinar antes de construirla): mientras tanto un anexo firmado deja la OT con su valor.
- **Las OT que NO se cobran no piden factura (Daniel, 29-oct, OT-241 y OT-182: «las garantías no deben quedar pidiendo factura»):** una OT en garantía, regalía, arriendo/leasing (o mantención de contrato) tiene como documento la nota de venta de la venta (REFERENCIA, no «promesa de cobro»): la puerta de cierre ya no se detiene pidiendo la factura (`_ot_puerta_documento`: la nota de venta + $0 sigue al paso del $0 autorizado), el recorrido dice «Referencia de la venta… no lleva factura», el cartel «Falta la factura» solo vale si la OT se cobra, el candado `SOLO_NOTA_VENTA`/`SIN_FACTURA` las exime (`_exenta_doc_cliente`) y el panorama no compara con el saldo un `zz_monto` viejo de una OT que no se cobra (la alarma roja de la OT-182). Lo que SÍ exigen es la autorización de Daniel del $0 (REGLA #24).
- **Control del cobro escrito a mano (Daniel, 29-oct: «debe existir control»; construido el 29-oct, ver REGLA #59):** lista de OT cuyo cobro está «declarado» sin origen o «escrito a mano» sin motivo, para respaldarlo con su documento o confirmarlo con su motivo (tercera pestaña de «Finanzas por reparar», «Cobro sin respaldo»).
- **Anexo = OT, un solo monto (REGLA #34).** Lo que paga la OT al proveedor y el total de su anexo son el mismo valor, también en las OT cerradas: no se corrigen a mano una por una. Las que ya difieren se reparan desde **«Finanzas por reparar»** (`/ot/finanzas-reparar`, botón en la barra del superadmin de Órdenes de Trabajo; solo superadmin; pestaña «Costo de la OT distinto al anexo»), por OT y en lote, por los mismos caminos que ya validan y registran (`/costo-desde-anexo` en una OT abierta, `/costo-desde-anexo/corregir` en una cerrada con anexo FIRMADO; con el anexo sin firmar una OT cerrada no se alinea y la pantalla lo dice en ámbar). La otra pestaña, «Documento declarado, cobro sin declarar» (OT-151, 152…), toma el cobro de los documentos por `/corregir` con `desde_documentos`. **Se repara en dos pasos: «Revisar» (calcula leyendo Random o el anexo, no escribe nada) y recién después «Tomar el cobro» / «Alinear con el anexo»** (de a 3 OT a la vez: cada proceso de Random tiene pocas conexiones). Lo que Random no entrega, es de otro RUT, no trae línea de servicio o ya lo usan otras OT queda en ámbar y no se toca. Tabla paginada (REGLA #4.3), «Seleccionar/deseleccionar todo» (REGLA #14), tarjetas en celular (REGLA #3). Backend: `_ot_fin_cobro_sin_declarar_cargar`, `GET /ot/api/finanzas/por-reparar` y `GET /ot/api/anexos/vinculo-costos` (ahora con `cliente` y `abierta`). No se agrega ningún camino de escritura nuevo.
- **Para KPI y decisiones:** lo que queda sin declarar (documento sin cobro, factura pendiente, anexo distinto) se cuenta y se muestra, nunca se oculta ni se cuenta como $0 (`ot_gerencial.py`, REGLA #48).
- Pruebas: `tests/test_ot_fin_corregir_desde_documentos.py`, `tests/test_ot_finanzas_reparar.py` (la pantalla ejecutada en Node con un DOM de mentira: revisar no escribe, lo bloqueado nunca se toca, simula antes de corregir una OT cerrada), `tests/test_ot_documento_manda_sobre_mano.py` (la ruta real «agregar documento» con ERP y base de mentira), `tests/test_ot_fin_anexo_restriccion.py` (el candado del costo vs. el anexo y el modal en Node) y `tests/test_ot_garantia_sin_factura.py` (+ `TestGarantiaNoPideFactura` en `tests/test_ot_puerta_documento.py`).

---

## 📐 REGLA #57 — Política de margen y control de costos del servicio técnico (Daniel, 2026-10-29; no negociable)

**Dicho por Daniel el 29-oct-2026, para poder «disertar» ante la gerencia: «el margen autorizado de todos los servicios fue aprobado en un 30 %, movible con una tolerancia hasta 20; de ahí hacia abajo, amarillo, y rojo los negativos».**

- **Una sola política de margen** (`ot_margen_politica.py`, módulo puro; nadie escribe estos números en otro lado): **≥ 30 %** = objetivo cumplido (verde) · **20 % a < 30 %** = dentro de la tolerancia (verde claro, sin alarma) · **< 20 % y ≥ 0** = amarillo (bajo la tolerancia) · **< 0** = rojo (pérdida real). Solo MARCA (tablero, tarjetas, Excel): **no agrega ningún candado nuevo**; el cierre con pérdida sigue pidiendo la autorización de Daniel (REGLA #28) y ese umbral no cambia. Los textos viejos «Margen bajo (menos de 10 %)» se alinean a esta política cuando se toque cada pantalla.
- **El documento que lleva el cobro manda** (REGLAS #24, #25, #28, #56), también en la calidad de datos. Un documento puede traer solo los productos y otro solo el servicio; **dos facturas con servicio se suman**; el saldo por línea se revisa contra el cobro que ya tomaron otras OT (motor de las REGLAS #25/#28: no se construye otro). La estructura de pantallas y reportes se adapta a eso (ergonomía), no al revés.
- **Tablero «Explorar» (Costeo de OT):** multifiltro estilo Power BI (un clic filtra todo) para desglosar lo cobrado y los costos de un período por centro de costo, técnico, proveedor o empresa (Mili, Felca, Daniel Pulgar = DAP…), trabajo interno / externo, cobertura, documento y tiempo (día, semana, N° de semana, mes, año). Solo superadmin; jamás técnicos (REGLAS #19/#26).
- **Excel de OT en español:** las fechas son fechas de verdad de Excel (formato dd/mm/aaaa, hora Chile) y vienen columnas de apoyo (año, mes con nombre, N° de semana, día) para filtrar y armar tablas dinámicas; nada de textos con formato «gringo».
- **Tarjeta «Costos y documentación»:** sin botones de «agregar factura» repetidos (una sola vía, estructurada) y con el encabezado de **quién se adjudica el dinero**: cuánto nos cobró cada proveedor o técnico externo (servicio y despacho), cuánto cobramos nosotros al cliente (servicio y despacho) y, aparte, cuánto cobró ILUS o el interno. **«ILUS» es solo una línea de cobro interno en esa tarjeta: NO se crea un técnico, usuario ni agente «ILUS».**
- **Futuro (pedido de Daniel):** ir revisando y escribiendo aquí las políticas del departamento de servicio técnico, una a una, con su «sí».
- Plan por fases del módulo de costos: memoria `plan_costeo_inteligente_2026_10_29.md` (Explorar → Calidad de datos → Valorización sugerida → Alerta de nota de venta sola → Memoria de cierre → Costo de técnicos propios).

---

## 🚚 REGLA #58 — Logística de las guías de despacho: «qué se fue» según Check, comparado con lo que gestiona la OT (Daniel, 2026-10-29)

**Pedido de Daniel (29-oct): «en la guía de despacho no hicimos la conexión con el WMS, que es la parte logística; quiero verlo para saber qué se fue, compararlo con los productos que gestionaron en la OT, y levantar una alerta. Quiero a Check en este reporte, todo resumido».**

- **Dónde vive (tarjeta COMPACTA desde el 29-oct tarde, Daniel: «quiero que sea PEQUEÑA… cuando le doy clic, que me diga TODOS LOS MOVIMIENTOS: qué se entregó y qué no, y si hay DIFERENCIA con la OT»):** «Trazabilidad en Random» (`static/ot_doc_cadena.js/.css` + `static/ot_guia_logistica.js/.css`) es UNA fila fina de chips por venta (nota de venta → factura → guías), con un punto de semáforo de 10 px en cada guía (verde: salió todo lo gestionado y Check lo expidió · ámbar: hay diferencia o falta expedir · gris: Check no tiene información) y **ningún texto bajo la fila**. **Desde la noche del 29-oct (Daniel: «hazla chiquitita; el detalle al darle clic, como en Retiros: qué hizo quién, cuándo, organizadito; necesito una comparación bien clara: ¿está cuadrando con la OT o no?»)** el detalle NO se pinta en la pantalla: al tocar una guía, una factura o el chip de la cabecera se abre una **VENTANA** (`OTDocCadena.modal`; Esc, la X o tocar afuera la cierran; pantalla completa en el celular) con, de arriba abajo: el **VEREDICTO en grande** («Sí cuadra con la OT» / «No cuadra con la OT» / «Todavía no se puede comparar») con el motivo y las unidades (OT · guía de despacho · Check), la **comparación producto por producto** (OT · guía · Check y su resultado, incluido lo que la OT gestiona y no salió en ninguna guía), el **paso a paso** en orden (nota de venta → factura → guía → picking → control de salida, con quién y cuándo; lo de Random en negro y lo de bodega/Check en rojo ILUS) y lo que informa Check; un selector entre las guías de la OT. El detalle de una **factura o nota de venta** (saldo por línea, otras OT y su margen, otra empresa, dada de baja) también va en la ventana: nada se quitó (REGLA #4.2). La cabecera lleva un solo chip: «Cuadra con la OT» o «No cuadra con la OT · N diferencias» (guía + Check). Sin montos en lo logístico; solo gestión (a un técnico el servidor le responde 403 antes de leer nada, REGLAS #19/#26).
- **Check tiene la ÚLTIMA PALABRA, algo POCO (Daniel: «no quiero ser un analista de inventario, solamente ver si hay diferencias con la orden de trabajo… hice una OT por 10 productos, pero salieron 8: tiene que haber una alerta»).** `ot_guia_vs_ot.tres_bandas` compara OT ↔ guía (Random) ↔ Check (lo expedido) con TRES reglas: **A** la guía no cuadra con la OT (`salio_y_no_esta_en_la_OT` · `esta_en_la_OT_y_no_salio` · `cantidad_distinta`); **B** `check_expidio_menos`: Check expidió menos de lo que la OT gestiona y la guía dice que salió («la OT gestiona 10; Check informa que salieron 8»; por SKU, o por TOTAL si Check no informa por producto, y lo dice); **C** `guia_sin_salida_check`: hay guía y Check no registra la salida pasado el plazo de gracia (`GRACIA_DIAS_HABILES` = 3 días hábiles desde la fecha de la guía, con los feriados de `cl_feriados`, REGLA #31). Mientras bodega prepara (dentro del plazo y con Check aún por expedir) se ve ámbar pero no se guarda ni suena; si Check ya cerró su pedido corto, B suena sin esperar. **Sin información de Check (no respondió, no tiene el documento, no informa ese producto) NO se alerta ni se resuelve nada.** Lo declarado como falta de stock / «No llegó» no se le pide a Check; si la guía y la factura llegan a Check, el mismo producto no se cuenta dos veces. Los hallazgos B y C solo se registran o se resuelven en una lectura que SÍ le preguntó a Check (`alcance`).
- **Check sigue SOLO LECTURA (REGLA #4.4):** únicamente `GetSeguimientoDespacho` por `_checkwms_get` y la lista blanca intacta (más el volcado de movimientos ya cacheado para operador/fechas). Si Check no responde o no tiene la guía, la tarjeta lo dice («sin información de bodega») y nunca inventa. Un reporte nuevo de Check solo se agrega a la lista blanca con el «sí» de Daniel.
- **La comparación (`ot_guia_vs_ot.py`, puro):** empareja por SKU normalizado y dice `coincide` · `salio_y_no_esta_en_la_OT` · `esta_en_la_OT_y_no_salio` · `cantidad_distinta`. No compara servicios (ZZ, descuentos); sin guía, con guías anuladas o sin productos gestionados es «sin datos para comparar» (no alerta); lo que la OT ya declaró como falta de stock / «No llegó» no es diferencia; lo que sobra y lo gestiona otra OT de la misma venta se nombra, no alerta. Hoy los equipos se esperan en una guía solo en instalación y cambio de equipo (en mantenciones y otros solo se comparan repuestos): confirmar con Daniel si control de calidad o garantía deben sumarse.
- **La alerta:** cada hallazgo A, B o C queda persistente en `ot_guia_alerta` (detalle = SKU, `TOTAL` o el número de la guía; nunca se borra: vigente → resuelta → vigente si reaparece; bitácora `guia_diferencia` / `guia_diferencia_resuelta`; una sentencia `CREATE TABLE IF NOT EXISTS` y, para el correo, `correo_huella` / `correo_el` agregadas con UNA cláusula por sentencia, REGLA #18) y suena la campana (`ot_guia_diferencia`, al final de las dos listas del ENUM de `mant_notificaciones`) a la gestión de la OT (`_hall_destinos`: responsable primero, sin técnicos ni quien lo está mirando). **Jamás al cliente** (REGLA #22).
- **Correo INTERNO (Daniel: «si hay diferencia, dame una ALERTA, así sea POR CORREO»):** clave `ot_guia_diferencia` en `comm_correos_registro.py` (grupo Órdenes de trabajo, llave `comunicacion_interna`, destinatario `responsable_ot`, **nace encendido**; Daniel lo apaga en Control de correos). Lo envía SOLO `_ot_logi_correo` por `_aviso_interno_correo` (puerta única `_send_ilus_email(..., correo="ot_guia_diferencia")`: interruptor general, llave y Control de correos; nunca a técnicos ni a usuarios sin correo) a los mismos de la campana. **UNO por hallazgo** (se reclama con un UPDATE atómico de su huella: dos personas a la vez no mandan dos) y otro solo si la diferencia cambia; frenado por un interruptor queda en el Historial y no se reintenta, un error sí. La lectura rápida (sin Check) lo guarda hasta 10 min para que la de Check mande UNO con todo. Corto, con números, enlace a la OT, sin montos ni proveedores.
- **Límites conocidos:** una venta que salió con la factura misma (retiro en bodega) no tiene guía: «sin datos». `GetSeguimientoDespacho` no trae usuario ni hora (eso sale del volcado de movimientos). Si Check no tiene la guía NI su factura, no se alerta (no se puede saber). Se detecta al abrir la trazabilidad de la OT (no hay un barrido de fondo: una guía que nadie mira no avisa). Cada ficha con guías hace 2 peticiones extra y la de Check puede tardar hasta 20 s: es asíncrona, la ficha nunca se bloquea.
- Pruebas: `tests/test_ot_guia_logistica.py` (puro con las reglas A/B/C y el plazo, ruta, Check/Random/correo falsos, campana, correo una vez por hallazgo y solo a la gestión, ENUM, JS de la fila compacta y del panel) y `tests/test_ot_guia_logistica_navegador.py` (Playwright, 1280 y 375).

---

## 🧮 REGLA #60 — Todo se VALORIZA: si la persona no lo hizo, el sistema SUGIERE el valor desde las cotizaciones, y la persona decide (Daniel, 2026-10-29)

**Pedido de Daniel (29-oct): «Necesito que TODO sea VALORIZADO. Si no está valorizado por el usuario, TÚ lo SUGIERES: busca en las COTIZACIONES cuál es el precio más acorde, e indícame (ej.: "esta revisión saldría en 600 lucas"). Trabajos internos también valorizados para saber cuánto se nos escapa.»** Fase 3 del plan de costeo.

- **Una sola cuenta:** `ot_valorizar.py` (puro, con pruebas). **Escalera por clase de equipo; la primera con datos manda y cada clase guarda su procedencia:** **A** la cotización de ESTA OT (ligada por documento, ticket o documento de Random; ítems que calzan por equipo, SKU o clase) · **B** cotizaciones del mismo cliente (mismo tipo de servicio y clase, 12 meses, mediana con ≥ 2) · **C** comparables de la misma región y luego del país (≥ 3; pesos aprobada 1,0 · enviada 0,8 · borrador/vencida 0,5) · **D** OT cerradas y cobradas con la línea del documento, de una sola clase (`zz_monto` / unidades) · **E** la tarifa del cotizador (el botón «Valorizar con el cotizador» de siempre, que se conserva). Sin datos dice QUÉ falta (sin equipos, sin clasificar, tipo sin tarifa); nunca inventa.
- **Decidido (29-oct):** cotizaciones aprobadas y enviadas de 12 meses; borradores solo si sin ellos no se llega al mínimo (confianza baja); rechazadas y eliminadas nunca. **Montos netos** (total del ítem con su descuento y su precio a mano; descuento de cabecera prorrateado; jamás el `total` con IVA). **El traslado va aparte** (de la cotización propia o de `tk_cotiz_rutas`) y no entra al valorizado. Confianza alta/media/baja por fuente; baja un escalón con pocos comparables, cobertura de equipos < 80 % o un trabajo de bodega comparado con una mantención. El rótulo de toda la OT es la fuente más débil que usó (nunca sobrevende).
- **Por cobertura:** en una OT que **se cobra** lo cobrado con documento es el valor: la sugerencia es solo «precio de mercado» («cobramos X; el mercado sugiere Y», en ámbar si se cobró más de 30 % bajo con confianza media o alta) y **no se guarda**. Garantía, regalía, arriendo/leasing, cortesía, contrato y **trabajo interno**: el valorizado es lo que se habría cobrado, siempre con la confirmación de una persona. Desinstalación (también la cubierta), inspección y levantamiento: solo con una partida explícita en su propia cotización (una partida del ANEXO no sirve: es lo que nos cobran, no un precio).
- **Nada se escribe solo (REGLA #32).** Sugerir es SOLO LECTURA de MySQL (`GET /ot/api/<vid>/valorizacion-sugerida`, gestión; `GET /ot/api/valorizacion/lote`, solo superadmin, caché 90 s). **La escritura tiene su camino propio:** `POST /ot/api/<vid>/valorizacion-aplicar` (solo superadmin; también con la OT cerrada) **recalcula la sugerencia en el servidor**: solo si el monto es exactamente el sugerido queda con su fuente (`cotiz_propia`, `cotiz_cliente`, `cotiz_comparable`, `ot_comparable`, `cotizador`); si no, `a_mano` con motivo (≥ 15) o 409 `SUGERENCIA_CAMBIO`. Escribe SOLO `valorizado_clp` y `valorizado_fuente` (nunca el cobro, los costos ni el estado), con bitácora, el registro inmutable `mant_ot_finanzas_correcciones` y la constancia de SOLO INSERCIÓN `ot_valorizado_sugerencias` (una sentencia `CREATE TABLE IF NOT EXISTS`, REGLA #18). Esas cuatro fuentes nuevas el navegador NUNCA las fija por `/ot/api/finanzas` ni al crear (`_OT_FIN_VALORIZADO_FUENTES_SERVIDOR` → `a_mano`).
- **Dos números que nunca se mezclan:** «Dejamos de cobrar» (Σ valorizado de lo NO cobrado; «Se nos escapa en internos» es su parte interna) y «Nos costó» (la caja). El valorizado es solo referencia: jamás se suma a lo cobrado (REGLAS #44, #45 y #56).
- **Dónde se ve:** una línea compacta en «Costos y documentación» de la ficha (semáforo chico de confianza + fuente; el detalle al clic; «Usar este valor / Editar / Descartar» solo superadmin) y la pantalla **«Valorizar en lote»** (`/ot/valorizar`, botón en la barra del superadmin): tabla paginada (REGLA #4.3), nada marcado, «Seleccionar/deseleccionar todo» (REGLA #14), «Marcar las de confianza alta», «Revisar» (solo lee) y «Aplicar las seleccionadas» con confirmación, de a 3. Un técnico no ve nada de esto (REGLAS #19/#26: `@_no_tecnico` + 403 antes de leer).
- **Límites:** no se verificó contra producción cuántas cotizaciones reales sirven ni si `zz_monto` es neto en todas las OT (las de Random lo son); el lote lee hasta 3000 OT y el mercado 4000 cotizaciones y 3000 OT (lo dice si se toca). Pruebas: `tests/test_ot_valorizar.py`.

---

## 🧹 REGLA #59 — «Calidad de datos» del Costeo de OT: dice qué falta o no cuadra y por dónde se arregla; manda el documento que lleva el cobro y NO escribe nada (no negociable)

**Pedido de Daniel (2026-10-29, FASE 2 del costeo):** «estamos en el paso de la depuración de datos: necesito que los costos sean estructurados… SIEMPRE darle la prioridad al DOCUMENTO que lleva el cobro… dos facturas con servicio se pueden sumar… identificar si hay cobros en otras OT para revisar el saldo» y «construyamos el dashboard también para ir depurando todos los valores».

- **Dónde:** 3.ª pestaña de «Costeo de OT» (`/ot/gerencial`: Resumen | Explorar | **Calidad de datos**), `static/ot_costeo_calidad.js/.css`, datos de `GET /ot/api/costeo/calidad` (mismos parámetros de período que Explorar; sin `codigo` = tarjetas por regla con cuántas OT, cuánta plata toca y dónde se concentra por técnico, centro y cliente; con `?codigo=&page=&por=&q=` = la lista paginada como Etiquetas). Solo superadmin (`@_mant_required @_no_tecnico @_calidad_solo_superadmin`); un técnico jamás (REGLAS #19/#26).
- **Una sola fuente de las reglas:** `ot_calidad.py` (puro, con pruebas), sobre la MISMA fila del tablero Explorar (`ot_costeo.fila`, la cuenta `_ot_finanzas` sin tocarla) y reutilizando `_ot_fin_dudosas_motivos`, `_ot_aut_cubre_cero`, `ot_anexo_costo` y la clasificación de documentos de `ot_costeo.tipo_doc`. Nadie copia una regla de calidad en otro lado: la 3.ª pestaña de «Finanzas por reparar» usa `ot_calidad.cobro_sin_origen`.
- **El documento manda:** el cobro «oficial» es la SUMA de lo que aportan los documentos de cobro VIVOS (`mant_visita_documentos.zz_serv_monto/zz_envio_monto`: factura, boleta o la nota de venta mientras no se facture). Una factura puede traer solo productos y otra solo el servicio; varias con servicio se suman; un documento de otra empresa (`rut_ok=0`) no cuenta; la nota dada de baja y el espejo de la madre tampoco. Un aporte en NULL es «sin verificar»: nunca se inventa una diferencia (solo se acusa si TODO está registrado, o si lo declarado es MENOR que lo registrado). Quién más usa cada documento sale del libro de saldo (`ot_fac_libro`/`ot_fac_doc`/`ot_fac_alerta`, REGLAS #25/#28), solo para la página que se mira.
- **Códigos** (rojo SOLO para plata realmente perdida; lo demás ámbar = revisar, azul = informativo): `PERDIDA_SIN_AUTORIZACION` (rojo) · `COBRO_DOC_SIN_DECLARAR` · `COBRO_DISTINTO_DE_DOCUMENTOS` · `COBRO_SIN_ORIGEN` · `SOLO_NOTA_VENTA` (solo OT terminadas que se cobran) · `NV_YA_FACTURADA` · `FACTURA_SIN_SALDO` · `DOC_OTRO_RUT` · `ANEXO_DISTINTO` · `TECNICO_SIN_COSTO` · `MARGEN_100` · `CERO_SIN_AUTORIZACION` · `COSTO_MAYOR_VALOR` (la misma regla del Resumen) · `PRECIO_ANOTADO` / `SIN_SEPARAR` / `ZZ_NO_SERVICIO` (de Finanzas dudosas) · `SIN_CENTRO_COSTO` · `SIN_VALORIZAR` · `SIN_TECNICO` · `SIN_INTENCION` (solo se cuenta: las antiguas no se rellenan, REGLA #47). Las de la fase 5 (memoria de cierre) no van aquí.
- **NO escribe nada:** «Arreglar» es un ENLACE al camino que ya existe (Finanzas por reparar con `?tab=cobro|anexo|origen&q=`, Regularizar con `?falta=&cliente_q=`, Saldo de facturas o la OT `#otdCardFinanzas`). Solo SELECT por lotes de 800, caché de 90 s (la misma lectura del tablero), sin DDL, sin Random ni Check (REGLAS #4.1, #4.4 y #18), sin correos ni avisos.
- **3.ª pestaña de «Finanzas por reparar» — «Cobro sin respaldo»** (`cobro_sin_origen` en `GET /ot/api/finanzas/por-reparar`): «Revisar» solo lee (`/correcciones`) y separa (a) los documentos dicen EXACTAMENTE lo mismo → «Fijar el origen» (entra a «Aplicar las listas»), (b) dicen otro monto → «Usar los documentos» (de a una, `ilusConfirm` con antes → después), (c) sin documento o sin poder leerlo → «Dejar a mano con mi motivo» (`ilusPrompt`, ≥ 15 caracteres). Todo por el MISMO `POST /ot/api/finanzas/<id>/corregir`: `confirmar_origen: 'documento' | 'manual'` vale SOLO si los montos no cambian (el servidor recalcula con `_ot_fin_docs_cobro` + `_ot_fin_corr_desde_docs`; 'manual' copia el motivo a `zz_motivo_manual`), con el mismo registro inmutable y sin tocar el costo (candado del anexo, REGLA #56).
- **Siguiente paso anotado:** ofrecer los códigos como dimensión multi-valor de «Explorar» (hoy el filtro de Explorar es de un valor por OT y su lectura no trae los documentos con aporte); mientras, la concentración por técnico/centro/cliente vive en la lista de cada código.
- Pruebas: `tests/test_ot_calidad.py` (reglas, ruta con base de mentira, plantilla y pantalla en Node) y `tests/test_ot_fin_cobro_sin_origen.py` (lista, `confirmar_origen` y la pestaña en Node).

---

_Última actualización: 2026-10-29_
_Mantenedor: Daniel Aguilar (daniel.aguilar@sphs.cl)_
