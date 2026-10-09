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
- **Check ya preparó Y despachó pero el retiro sigue en «Cita confirmada»:** no se mueve solo (pudo entregarse antes; «estamos preparando» llegaría con el pedido ya entregado). Se avisa al
  equipo UNA vez al día (`check_desfase`, campana + bitácora), la guía dice «preparado según Check» y ofrece «Marcar como RETIRADO» (el modal de retirado existe también con la cita
  confirmada), y «Enviar a preparación» advierte que no corresponde. Nunca se envía a preparación un pedido ya despachado sin ese aviso.
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

- **Guía:** lo único que toca es «Me hago cargo»; las demás acciones quedan bloqueadas con el motivo a la vista y los botones nativos (proponer fecha, enviar a
  preparación, marcar retirado) abren un aviso que lleva al paso 2. Enter va solo a ese pendiente. `retiros_guia.evaluar` → `sin_responsable`.
- **Servidor (`pickups_module.py`, `_sin_responsable`):** exige responsable `confirmar-docs`, `confirmar-productos`, `/proposal` (agendar), `/aceptar-contrapropuesta`,
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
- **El reloj del SLA («Sin responder») corre en el MISMO horario de cobertura** (`_cc_ventanas()` → `mon.ventanas`; servidor y navegador iguales). Nunca un contador «00:00:00»: fuera de horario dice cuándo parte el reloj.

---

## ⏱️ REGLA #23 — Retiros: tiempo medido y controlado (Check = evidencia), y lo que NO se activa sin Daniel

**Pedido de Daniel (2026-10-06): "hay que medirlo… prometer automatización, tiempo controlado, una gestión de retiro a nivel de gerencia operacional logística"** y "calcular los minutos que se prepara el retiro según el WMS y tener toda la evidencia… registro de cuánto se tarda por producto en promedio".

- **Una sola lógica de tiempos:** `retiros_tiempos.py` (puro, con pruebas) y su espejo en `static/retiros_guia.js`. Trabajo = suma de cada OT; preparación efectiva = tiempo con al menos una OT abierta; pausas = principio a fin − efectiva; un tramo que pasa de un día a otro solo cuenta la jornada de bodega (07:30–20:00 L–V, `RETIROS_JORNADA_BODEGA`). Si cambias una, cambia la otra y sus pruebas.
- Se guarda como evidencia en `pickup_prep_tiempos` (con el criterio escrito) y `pickup_prep_productos` (minutos de picking por SKU) al guardar el registro de Check. Alimenta los KPIs «Gestión operacional» y `/retiros/api/tiempos-preparacion`.
- **Expedición en Check = RETIRADO está ACTIVA desde el 2026-10-08** (Daniel lo autorizó explícitamente; `RETIROS_RETIRO_AUTO=activo` en `deploy.yml`): cuando bodega expide en Check un retiro que YA está «En preparación», pasa solo a RETIRADO y el cliente recibe «Retiro completado». Con la cita solo confirmada no se cierra: se avisa al equipo. Bodega debe expedir solo cuando el cliente está retirando. Volver a `sombra` o apagarlo, solo con su permiso.
- **La encuesta de satisfacción está CREADA y NO lanzada** (`RETIROS_ENCUESTA_ACTIVA` apagada: el cliente ve 404, no se envía nada). Lanzarla, y cualquier firma digital del cliente, **solo con el «sí» explícito de Daniel**. Los datos de la encuesta siguen la Ley 21.719 (aviso, consentimiento, sin datos personales, 24 meses).
- **Daniel 2026-10-07: «no generes ninguna notificación a ningún cliente hasta que te autorice, sobre todo con la encuesta».** El enlace de la encuesta en el correo «retiro completado» y en el seguimiento ya está programado, pero solo aparece con `RETIROS_ENCUESTA_ACTIVA=1`; el comprobante de firma por correo solo sale con `RETIROS_FIRMA_CORREO=1`. Ninguna de esas variables se enciende (ni en `deploy.yml`) sin su autorización explícita en ese mensaje (`RETIROS_RETIRO_AUTO=activo` sí: autorizado el 2026-10-08).
- El tiempo estimado de preparación **nunca** se le muestra al cliente (hay pruebas que lo vigilan).
- **Retiro terminado = ficha en SOLO LECTURA** (Daniel 2026-10-06: «una vez que se cierra… que no se pueda gestionar nada más, que no pueda agregar factura»). Con estado retirada, cerrada, rechazada o fallida, toda ruta de gestión responde 409 (`_rechazo_si_cerrado`) y la ficha muestra la franja «Retiro cerrado…». Siguen abiertos: reabrir desde Cambiar estado (`/status`), el chat con el cliente, las lecturas y los procesos automáticos. Toda ruta NUEVA que modifique un retiro debe pasar por `_rechazo_si_cerrado`.

---

## 🧾 REGLA #24 — El documento de Random manda en OT, Tickets y Cotizaciones; sin documento solo con autorización de Daniel (no negociable)

**Pedido explícito de Daniel (2026-10-07), tras revisar con el gerente general OT de junio sin documento (OT 43 La Dehesa) y OT in situ abiertas sin documento:** "Los servicios deben cobrarse… Todo con documento tiene que ser absoluto y solamente pidiendo autorización remota con un argumento… esto tiene que ser inviolable." Y: "tanto tickets y OT y cotización deberán siempre predominar con el documento de Random a menos que yo lo autorice, ahí predomina el argumento; que dejemos con la trazabilidad de quién autorizó."

- **OT de cliente:** no se crea ni se cierra sin documento de Random (factura, boleta o nota de venta, validado contra el ERP y el RUT). La única salida es «Pedir autorización a Daniel» con argumento; un superadmin aprueba o rechaza a distancia. **Todo $0** (garantía, regalía, arriendo/leasing) también pasa por esa autorización. Exentas solo: OT interna sin cliente y mantención preventiva de un contrato REAL. **Para CREAR basta nota de venta o cotización; para CERRAR siempre factura/boleta** (la nota de venta queda como documento anterior, dada de baja por la factura; deja sin efecto la regla del 19-08 «la NV cierra»). En el modal de cierre el autorizador (Aarón, Juan Pablo, Víctor) puede retractar el cobro y pedir garantía con argumento (va a Daniel). **Centro de costo obligatorio siempre.** Ningún candado sin salida: cada rechazo de cierre dice qué hacer y se resuelve desde el mismo modal (Daniel: «que no puedan entramparse en un ciclo que no tiene solución»).
- **Tickets y Cotizaciones:** mismo principio (documento de Random primero; sin él, argumento + autorización). Usan el MISMO mecanismo de autorizaciones (tabla genérica con `entidad`), no uno propio.
- **Trazabilidad siempre:** quién pidió, quién autorizó o rechazó, cuándo (hora Chile) y el argumento completo, visible donde se vea el documento. Nada se borra; se registra.
- **Gestión documental transparente:** todos los documentos de una OT viven en `mant_visita_documentos` (multidocumento); ninguno se sobrescribe; aparecen en la ficha, el recorrido, las bandejas, el Excel y el informe/PDF (al cliente solo tipo y número, nunca montos internos ni proveedores).
- Una puerta única (`_ot_puerta_documento`) cubre todos los caminos que crean o cierran OT. Todo camino NUEVO pasa por ella. El interruptor del candado solo lo cambia superadmin.

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

_Última actualización: 2026-10-08_
_Mantenedor: Daniel Aguilar (daniel.aguilar@sphs.cl)_
