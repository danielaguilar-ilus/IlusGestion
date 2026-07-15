# BLUEPRINT — Módulo de Cotizaciones (ILUS)

> Escrito para que Sonnet 5 lo construya por fases sin tener que releer
> Triple A ni redescubrir el estado actual. Cada fase es un commit y un
> deploy independiente, verificable por separado. No requiere sesiones
> caras de razonamiento — es un plano de ejecución.

**Contexto y urgencia (2026-07-15):** el ambiente con Triple A se está
poniendo tenso y puede haber que migrar de forma abrupta. Cotizaciones es
uno de los módulos más atrasados y más significativos del negocio — hay
que ponerlo operativo pronto, con esencia propia (no un clon 1:1 de
Triple A), corrigiendo sus bugs conocidos y aprovechando piezas que ya
tenemos construidas y probadas (modal ERP, motor de correo, Playwright,
numeración atómica, y el `clase_producto` del Catálogo recién
desplegado hoy).

---

## 0. Decisión de arquitectura — cuál sistema se queda

Hoy conviven DOS esquemas de cotizaciones sin fusionar (decisión
deliberada anterior, Regla #4.2):
- `mant_cotizaciones` / `mant_cotizacion_items` (app.py) — **completo**:
  motor de cálculo real (descuento por contrato, recargo por urgencia,
  IVA), máquina de estados con transiciones válidas, conversión a OT,
  ficha visual terminada. Le falta: envío de correo real y PDF.
- `tk_cotizaciones` / `tk_cotizacion_items` (tickets_module.py) —
  **esqueleto**: solo crea un borrador con todo en $0, sin ficha, sin
  cálculo, sin nada más.

**Decisión para este blueprint: `tk_cotizaciones` es la base que se
construye hacia adelante** (vive en el módulo central de Tickets, se
liga a `ticket_id`, es donde Daniel quiere trabajar desde ahora — así lo
pidió explícitamente: "quiero tener acceso a las cotizaciones" desde el
flujo de tickets/ERP). `mant_cotizaciones` queda **congelado tal cual
está** (Regla #4.2, nadie la toca ni la borra) como sistema legado
visible en el hub `/cotizaciones` mientras haya cotizaciones viejas
vivas. No se migran datos de una a otra — son historiales separados,
igual que hoy.

**Lo que SÍ se porta de `mant_cotizaciones` hacia `tk_cotizaciones`** (la
esencia que Daniel no quiere perder), en fases posteriores: el motor de
cálculo con descuento/recargo/IVA, la máquina de estados con
transiciones válidas, y el patrón de numeración — **usando el patrón
race-free A (`_next_ot_number_atomic`) o B (derivado del id en el mismo
cursor), NUNCA el anti-patrón de `_next_cotizacion_number()`** (MAX+1 con
race condition conocida).

---

## 1. Qué se copia de Triple A, qué se corrige, qué se mejora

### Se copia (la esencia que funciona)
- **Fórmula de cálculo** (orden exacto): `itemsSubtotal = Σ item.total`
  → `+ costo_ruta` → `subtotal` → descuento global (% pisa a monto fijo)
  → `subtotalAfterDiscount` → IVA sobre eso → `total`. Por ítem:
  `subtotal = cantidad×precio_unitario`, `total = subtotal - descuento_item`.
- **Backend SIEMPRE recalcula** al guardar — nunca confía en totales que
  mande el frontend (Triple A lo hace bien, cópialo tal cual).
- **Tipos de ítem**: producto / servicio / ruta(informativo en cabecera,
  no como línea) / otro — ya está en el ENUM de `tk_cotizacion_items`.
- **Llamado a documento ERP** como punto de partida (modal compartido).
- **PDF con plantilla formal** + envío por correo con threading al hilo
  del ticket — adaptado a nuestra plantilla maestra, no a la de Triple A.

### Se corrige (bugs confirmados en el levantamiento, NO reproducir)
1. **Numeración COUNT+1**: colisiona si se borra una cotización. Usar
   patrón A o B (ver §0), nunca MAX+1/COUNT+1.
2. **`QUOTATION_STATUS_MAP` desalineado** con el enum real (afectaba el
   CSV en Triple A) — al construir el mapeo estado→label, un solo
   diccionario, una sola fuente de verdad.
3. **Envío de confirmación al ejecutivo en vez del cliente** (bug real
   de Triple A en `handleUpdateQuotationStatus`) — verificar SIEMPRE que
   el destinatario del correo de cotización sea el email del CLIENTE
   (`tk_cotizaciones.email` o el del ticket asociado), nunca el usuario
   que la creó.
4. **Botón "eliminar" que no borra nada** (stub roto en el front de
   Triple A) — si se construye soft/hard delete, que el botón
   efectivamente llame al endpoint.
5. **`routeCostId` guardado con un monto en vez de un id** (bug de
   Triple A en el diálogo de edición) — al persistir la ruta elegida,
   guardar el `id` de `tk_cotiz_rutas`, nunca el precio.
6. **Prorrateo invisible del costo de ruta dentro de los precios del
   PDF** (Triple A esconde el traslado repartiéndolo entre los ítems) —
   **NO copiar esto**: mostrar el costo de ruta como su propia línea
   visible en el PDF y en la ficha. Transparencia > truco visual.

### Se mejora (evolución nuestra, no existe en Triple A)
1. **Precios reales del documento ERP**: Triple A trae del documento
   solo SKU+cantidad y vuelve a cotizar todo con su tarifario local
   (nunca usa el valor real de la línea, `VANELI`). Nosotros SÍ podemos
   traer `vaneli` (valor neto de línea) desde `/api/erp/documento` y
   usarlo como precio de partida sugerido — editable, no forzado.
2. **Clasificación de producto integrada con Catálogo** (pedido
   explícito de Daniel hoy, no existe en Triple A en absoluto — ver §3).
3. **Portal de cliente para aceptar/rechazar** (en Triple A es un mockup
   muerto) — reusar el mismo patrón de token HMAC + página pública que
   ya existe para firma remota de OT (`/firmar-ot/<token>`).
4. **Costo de ruta desde datos reales**: ya tenemos 253 rutas reales
   importadas (`tk_cotiz_rutas`, origen Quilicura) esperando conectarse
   — Triple A calcula la ruta con una fórmula genérica
   (combustible+margen+depreciación), nosotros ya tenemos tarifas
   negociadas reales por comuna.
5. **PDF server-side con Playwright** (ya construido y probado en el
   proyecto para OTs) en vez de pdfmake en el navegador — más
   consistente, no depende del cliente del usuario.

---

## 2. Modelo de datos — cambios necesarios sobre lo que YA existe

`tk_cotizaciones` y `tk_cotizacion_items` (tickets_module.py:861-910) NO
se recrean — se **amplían** de forma idempotente (mismo patrón
`information_schema` + `ALTER TABLE ADD COLUMN` ya usado en todo el
proyecto):

```
tk_cotizaciones — agregar:
  cliente_id_erp     VARCHAR(50) NULL   -- koen del cliente (Triple A lo pierde; nosotros no)
  origen_tido        VARCHAR(10) NULL   -- COV/NVV/FCV/BLV/... del documento origen
  origen_nudo        VARCHAR(40) NULL
  origen_subtipo     VARCHAR(10) NULL   -- 'interna'|'web'|'directa' solo si origen_tido='NVV' (ver §5)
  email              VARCHAR(190) NULL  -- email del cliente, destinatario real del correo
  telefono           VARCHAR(50) NULL
  ruta_id            INT NULL           -- FK tk_cotiz_rutas.id (el id, NUNCA el monto -- bug #5 de Triple A)
  enviada_at         DATETIME NULL
  aprobada_at        DATETIME NULL
  rechazada_at       DATETIME NULL
  vista_por_cliente_at DATETIME NULL    -- para el portal (fase 4)

tk_cotizacion_items — agregar:
  clase_producto     VARCHAR(30) NULL   -- copia denormalizada del cat_productos.clase_producto
                                         -- al momento de agregar el item (igual que Triple A
                                         -- copia el precio del documento -- snapshot, no referencia viva)
  vaneli_original    INT NULL           -- valor de línea que trajo el ERP, para trazabilidad/auditoría
                                         -- (precio_unitario puede editarse después; este campo no)
```

**Nueva tabla, solo si Fase 3 (máquina de estados) se construye:**
```sql
CREATE TABLE IF NOT EXISTS tk_cotiz_secuencia (
  id INT PRIMARY KEY DEFAULT 1,
  anio INT NOT NULL,
  n INT NOT NULL DEFAULT 0,
  UNIQUE KEY uq_anio (anio)
) ENGINE=InnoDB;
-- mismo patrón que _next_ot_number_atomic, adaptado a reiniciar el
-- correlativo cada año (COT-2026-00001, COT-2027-00001, ...)
```

No se toca `mant_cotizaciones` en ningún punto de este blueprint.

---

## 3. FASE 1 — El disparador: llamar documento → clasificar → borrador limpio

**Esto es lo que Daniel pidió construir YA, con sus palabras exactas:**
*"acceder a las cotizaciones... llamar a un documento con el modal que
tenemos, y llamando a ese documento me deberá traer los productos...
los productos, si no tienen clasificación, tendrán que clasificarse, y
eso también va a estar en los catálogos... necesito distinguir qué es
Rack, qué es una bicicleta, qué es una trotadora, qué es un selector de
peso... construyendo el llamado donde me traiga los datos del cliente y
los productos, obviando los servicios, que son los ZZ."*

### 3.1 Lo que YA existe y se reutiliza sin tocar
- Modal `_tka_modal.html`, `tkaOpen({mode:'seleccionar', tabs:[...]})`
  — pestaña "Por documento" ya filtra ZZ (`!l.es_zz`) antes de entregar
  la selección. **No hay que reconstruir el filtro de ZZ, ya está.**
- `cat_productos.clase_producto` (ENUM de 10 valores: selector_peso,
  rack, rack_avanzado, carga_disco, trotadora, escaladora, eliptica,
  bicicleta, banco, otro) + `GET /catalogo/api/clases` — **construido y
  desplegado HOY, úsalo tal cual, no inventes una taxonomía nueva.**
- `POST /catalogo/api/productos/desde-erp` — crea/reusa un producto en
  Catálogo por SKU, idempotente. **Reutilizar esta función interna**
  (o llamarla) en vez de duplicar lógica de creación de producto.
- `POST /tickets/api/cotizaciones/desde-erp` (tickets_module.py:1553) —
  YA crea el header + ítems en `draft`. Se **extiende**, no se reescribe
  desde cero.

### 3.2 Cambios concretos a hacer

**A. Agregar `COV` al selector de la pestaña "Por documento"** del modal
(`_tka_modal.html:498-504`) — hoy solo ofrece FCV/BLV/VD/WEB/NVI/GDV;
Cotización de venta (COV) del ERP falta en esa lista pese a estar en la
lista canónica de TIDOs válidos (`app.py:121-126`).

**B. El header del documento (cliente) debe viajar hasta el payload**
de `/cotizaciones/desde-erp`. Hoy el contrato de items del modal
(`onSeleccionar`) solo entrega `{tido,nudo,sku,nombre,qty,...}` por
línea — **no** trae datos de cliente. El fetch del documento (que ya
resuelve `cliente_nombre/cliente_rut/email/telefono` en
`/api/erp/documento`, ver hdr en app.py:13633-13658) ocurre ANTES,
cuando se carga el documento en el modal. Al llamar
`cotizaciones.html`/donde sea que se dispare `onSeleccionar`, captura
también ese header (ya está en memoria del JS del modal en el momento
del fetch — expón `_TKA.loaded.docHeader` o equivalente al callback, o
pasa el header como segundo argumento de `onSeleccionar(items, header)`)
y mándalo en el body del POST junto con `items`.
**Corrige aquí el bug ya confirmado**: `empresa`/`rut` en
`tk_cotizaciones` quedan siempre NULL hoy porque el template no los
manda — con el header real disponible, esto se arregla de raíz.

**C. Backend `/tickets/api/cotizaciones/desde-erp` — clasificación
automática por SKU:**
Por cada item (ya sin ZZ):
1. Busca el SKU en `cat_productos`. Si no existe, créalo
   (mismo camino que `cat_api_producto_desde_erp`, sin duplicar código
   — factoriza esa lógica a una función interna reusable si hoy vive
   solo dentro del endpoint de Catálogo).
2. Si el producto (nuevo o existente) tiene `clase_producto` NULL,
   márcalo en la respuesta: agrega el sku a un array
   `sin_clasificar: ["SKU1", "SKU2"]` en el JSON de retorno.
3. Guarda en `tk_cotizacion_items.clase_producto` el valor actual (o
   NULL si aún no está clasificado — se completa después).
4. Guarda `vaneli_original` con el valor de línea si el item lo trae
   (ver §1.mejora-1 — puede venir vacío si el modal aún no lo expone,
   entonces NULL, sin bloquear nada).

**D. Frontend — clasificación inline post-creación:**
Si la respuesta trae `sin_clasificar` con al menos un SKU, abre
inmediatamente (sin salir del flujo) un panel/modal pequeño: una fila
por SKU sin clasificar, con el mismo `<select>` de 10 clases que ya
existe en el wizard de Catálogo (fetch a `/catalogo/api/clases`,
reutilizar el JS si es sencillo). Al confirmar, `PATCH
/catalogo/api/productos/<id>` con `clase_producto` (endpoint que YA
existe, construido hoy). Si Daniel cierra el panel sin clasificar
alguno, no bloquea nada — la cotización ya quedó creada en `draft`, la
clasificación pendiente se puede completar después desde el propio
Catálogo.

### 3.3 Resultado de la Fase 1
Daniel puede: abrir el modal ERP desde Cotizaciones → elegir tipo de
documento (incluyendo COV) → buscar por número → seleccionar líneas
(los ZZ ya vienen filtrados) → confirmar → se crea una cotización
`draft` con: cliente real (empresa/rut/email), ítems con su SKU/nombre/
cantidad, y cada producto queda clasificado (si no lo estaba, se pide
en el momento) — y esa clasificación quedó también en el Catálogo,
formalizando el maestro de productos para todo el sistema, no solo para
esta cotización. **Todavía sin precios ni PDF ni envío — eso es Fase 2
en adelante.**

---

## 4. FASE 2 — Motor de cálculo (portar la esencia de Triple A, corregida)

- Función `_tk_cotiz_calcular_totales(cid)`, mismo patrón que
  `_cotiz_calcular_totales` de `mant_` (app.py:73139-73218) pero
  adaptado al esquema `tk_` (montos INT, no DECIMAL) y **sin** el
  prorrateo invisible de ruta (§1, corrección 6).
- Precio unitario de partida por ítem: si `vaneli_original` no es NULL,
  precárgalo como sugerencia editable; si es NULL, precio manual (como
  hace Triple A con ítems `service`/`other`).
- Costo de ruta: `JOIN tk_cotiz_rutas` por comuna del cliente (ya
  resuelta en el header del documento, §3.2.B) — este es el momento de
  **conectar por fin** la tabla de 253 rutas que hoy está poblada y
  huérfana.
- Recalcular SIEMPRE en el backend al crear/editar ítems — nunca
  confiar en totales que mande el frontend (regla de Triple A que sí se
  copia).
- UI: reusar el bloque de totales visual de `mant_cotizacion_ficha.html`
  (subtotal/descuento/recargo/neto/IVA/final) como referencia de
  diseño, con los tokens 2027 ya establecidos en `ficha.html`.

## 5. FASE 3 — Ficha propia + máquina de estados

- Ruta nueva `GET /tickets/cotizaciones/<cid>` (hoy NO existe — el hub
  enlaza al listado, no a una ficha, porque no hay ficha).
- Estados y transiciones válidas: portar `_COTIZ_TRANS` de `mant_`
  (app.py:73570-73578) al vocabulario en inglés que ya usa el ENUM `tk_`
  (draft→sent→approved/rejected/expired).
- Numeración: patrón A o B de §0 — nunca MAX+1.
- Botonera por estado (mismo patrón visual que `mant_cotizacion_ficha`,
  líneas 317-351, pero sin `confirm()`/`alert()` nativos — usar
  `ilusConfirm`/`ilusToast`, Regla #1).
- **`origen_subtipo`** (interna/web/directa): solo aplica si
  `origen_tido='NVV'`. El único mecanismo real que existe hoy para esa
  distinción es el prefijo del NUDO (`VD` / `WEB` / ninguno = interna) —
  mismo patrón ya usado 5 veces en el código (`app.py:60002-60011`,
  `tickets_module.py:4687-4692`). No hay campo de MAEEDO (SULIDO/KOFU/
  LUVTLI) que hoy se lea para esto — si en el futuro se quiere un
  criterio más fino, validar esos campos contra los diccionarios reales
  en `C:\Users\DANIE\ilus-migracion\erp-diccionarios\` antes de usarlos,
  siempre vía `_random_sql_query` (Regla #4.1, jamás conexión directa).

## 6. FASE 4 — PDF + envío + portal cliente

- PDF: `_pw_pdf(html, ...)` (app.py:524-574, Playwright ya probado) con
  una plantilla propia (no la de Triple A) que incluya el costo de ruta
  como línea visible (corrección §1.6). Reusar el patrón de
  `_ot_pdf_context` / `mant_visita_pdf` como referencia de estructura,
  con fallback a vista imprimible si Chromium no está disponible (mismo
  patrón defensivo ya usado en el proyecto).
- Correo: `_ilus_email_master(ctx)` + `_send_ilus_email(...)` (patrón
  único del proyecto, nunca HTML de correo suelto). **Destinatario:
  SIEMPRE `tk_cotizaciones.email` (el cliente)** — corrección directa
  del bug #3 de Triple A (que mandaba al ejecutivo).
- Threading a Gmail: si la cotización tiene `ticket_id`, responder en el
  mismo hilo del ticket (mismo patrón que ya usa el módulo de
  Tickets para las respuestas).
- Portal cliente: página pública `/cotizacion/<token>` con token HMAC
  (mismo patrón que `/firmar-ot/<token>`), donde el cliente ve el PDF y
  puede Aceptar/Rechazar — mejora real sobre Triple A, que no tiene esto
  operativo.

---

## 7. Piezas reusables — resumen para no reinventar nada

| Necesito... | Ya existe en... |
|---|---|
| Modal de búsqueda ERP | `templates/tickets/_tka_modal.html`, `tkaOpen()` |
| Traer un documento con cliente+líneas+totales | `app.py:13513` `/api/erp/documento` (o `erp_engine.fetch_document`) |
| Filtrar servicios ZZ | ya aplicado en el modal (`!l.es_zz`), y `sku.startswith("ZZ")` en el backend |
| Clasificación de producto | `cat_productos.clase_producto` + `GET /catalogo/api/clases` (HOY) |
| Crear producto en catálogo por SKU (idempotente) | `POST /catalogo/api/productos/desde-erp` |
| Tarifas de ruta reales | `tk_cotiz_rutas` (253 filas, Quilicura, esperando join) |
| Correo con marca | `_ilus_email_master` + `_send_ilus_email` (app.py:6534, 7192) |
| PDF | `_pw_pdf` (app.py:524), patrón `_ot_pdf_context`/`mant_visita_pdf` |
| Numeración sin colisión | `_next_ot_number_atomic` (patrón A) o `CONCAT(...LPAD(id,...))` en el mismo cursor (patrón B) |
| Token público tipo portal | patrón `/firmar-ot/<token>` (HMAC) |
| Toasts/confirms sin nativos | `ilusToast`/`ilusConfirm`/`ilusAlert` (Regla #1) |
| Tokens visuales 2027 | `:root` en `ficha.html`/`_tka_modal.html`/`soporte_publico.html` |

---

## 8. Orden de construcción recomendado

1. **Fase 1** (este momento) — disparador: documento → cliente+productos
   sin ZZ → clasificación integrada con Catálogo → borrador limpio.
2. **Fase 2** — motor de cálculo + conectar `tk_cotiz_rutas`.
3. **Fase 3** — ficha propia + estados.
4. **Fase 4** — PDF + correo + portal cliente.

Cada fase se prueba y se despliega por separado (mismo estándar de
verificación de todo el proyecto: sintaxis Python/Jinja/JS, revisión de
diff, confirmación explícita de Daniel antes de cada push a `main`).

---

*Blueprint escrito 2026-07-15 a partir de un levantamiento exhaustivo
del código real de Triple A (`C:\Users\DANIE\ilus-dev`) y del estado
real de este proyecto — cada afirmación de ambos levantamientos fue
verificada archivo:línea, no es memoria ni suposición.*
