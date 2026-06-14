# ILUS Sport & Health — Reglas para Claude

Este archivo establece las **reglas no negociables** del proyecto que TODO
agente que toque código debe respetar. Está pensado para Claude pero
sirve a cualquier desarrollador.

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
fuente de verdad de la empresa. ILUS Sport & Health JAMÁS modifica
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

Todas son **opcionales** — si no se setean, hay defaults sensatos:

| Variable                  | Default                                  | Para qué sirve                          |
|---------------------------|------------------------------------------|------------------------------------------|
| `ILUS_BRAND_NAME`         | `ILUS Sport & Health`                    | Nombre legal completo (footer email)     |
| `ILUS_BRAND_FROM_NAME`    | `ILUS`                                   | Visible en cabecera "De:" del email      |
| `ILUS_BRAND_FROM_EMAIL`   | `no-reply@ilusfitness.com`               | Dirección remitente (no-reply genérico)  |
| `ILUS_BRAND_REPLY_TO`     | `servicio.tecnico@ilusfitness.com`       | Buzón donde caen respuestas reales       |
| `ILUS_BRAND_WA_NAME`      | `ILUS`                                   | Prefijo de WhatsApp/SMS (`🔧 ILUS · …`)  |
| `ILUS_BRAND_SUPPORT_EMAIL`| `servicio.tecnico@ilusfitness.com`       | Email en footer "Para soporte: …"        |
| `ILUS_BRAND_SUPPORT_URL`  | `https://ilusfitness.com/soporte`        | URL portal soporte (footer)              |

**Cómo aparece para el destinatario:**

- **Email:** `De: ILUS <no-reply@ilusfitness.com>` · `Reply-To: servicio.tecnico@ilusfitness.com`
  Asunto: `ILUS · Cambio seguro de contraseña`
- **WhatsApp/SMS:** comienza con `🔧 ILUS · {tema}` y termina con `— ILUS Sport & Health`

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
- **Pide OK a Daniel antes de cada merge a main** (= deploy a producción).

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

_Última actualización: 2026-05-17_
_Mantenedor: Daniel Aguilar (daniel.aguilar@sphs.cl)_
