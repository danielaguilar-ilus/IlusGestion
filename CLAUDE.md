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

## 🤖 REGLA #10 — Cuando llega un agente nuevo

Cualquier agente nuevo debe leer **este archivo primero** antes de
escribir código. Si una regla no encaja con la tarea, ESCALARLO al
usuario (no improvisar).

Si necesitas agregar reglas nuevas, hazlo aquí — no en comentarios
sueltos del código.

---

_Última actualización: 2026-05-16_
_Mantenedor: Daniel Aguilar (daniel.aguilar@sphs.cl)_
