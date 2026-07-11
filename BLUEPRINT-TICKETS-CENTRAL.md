# Blueprint — Módulo TICKETS CENTRAL (ILUS Sport & Health)

> Documento de arquitectura para replicar en **nuestro código (Flask + MySQL, Google Cloud Run)** el sistema de tickets de `ilus-back`/`ilus-front` (NestJS + Next.js), como un módulo **dedicado y central** al nivel de Retiros/Transporte. No se toca ni se porta el código de terceros: solo se usa como modelo. Se centraliza aquí todo el soporte, incluyendo los tickets que hoy viven en Mantenciones (`mant_tickets*`), **sin romperlos** (Regla #4.2).
>
> Todos los nombres de tablas/campos/funciones citados son reales (mapas de código adjuntos). Los identificadores nuevos que se proponen usan el prefijo **`tk_`**.

---

## 1. Objetivo y alcance

### 1.1 Objetivo
Construir un módulo **TICKETS CENTRAL** en la plataforma Flask que reproduzca la experiencia y velocidad del módulo de tickets de terceros, con:

- **Formulario público** que detona tickets (RUT chileno, teléfono chileno, email, dirección con Google Places, equipo/producto conectado al ERP Random, N° de boleta/factura opcional, descripción máx. 260 caracteres, subida de fotos/videos/documentos a GCS).
- **Creación interna** de tickets (backoffice), incluyendo detonar un ticket **desde una o varias facturas/documentos del ERP** (`nudo`+`tido`) y **desde cotizaciones internas**.
- **8 tipos de solicitud** expuestos al público (13 internos, ver §2.4), **cambio de estado** del ticket, y **ficha** con la información del cliente bien separada y autocompletada desde el ERP.
- Pestaña de **Conversación** (correo real al cliente + comentarios internos, con contador de no-leídos).
- Pestaña de **Acciones** (ej. *agendar hotel del técnico*) — **no existe hoy en ninguna parte**.
- Pestaña de **Cotizaciones** (módulo nuevo, copiando el de `ilus-front`).
- Conexión de solo-lectura a **clientes, productos, cotizaciones y documentos del ERP Random**.
- **Rápido y escalable a 10.000+ tickets con fotos**.

### 1.2 Alcance funcional
Dentro: CRUD de tickets, conversación por correo + comentarios, adjuntos GCS, acciones, cotizaciones, integración ERP read-only, migración de los tickets de Mantenciones, listado con filtros/KPIs/paginación, export CSV, métricas SLA.

Fuera (por ahora): reescribir OTs (se **reutiliza** `mant_visitas`), WhatsApp/SMS (stack ILUS hoy solo email), y cualquier escritura al ERP (prohibida, Regla #4.1).

### 1.3 Principios y reglas de plataforma que este módulo respeta
- **Regla #1**: nada de `alert/confirm/prompt` nativos → `ilusAlert/ilusConfirm/ilusPrompt/ilusToast/ilusActionSheet/ilusLoader` (`static/ilus_ui.js`).
- **Regla #2**: paleta rojo `#dc2626` / negro `#0a0a0a` / blanco. Reusar tokens y clases `.btn-ilus`, `.ilus-card` (`static/style.css`).
- **Regla #3**: mobile-first (`static/mobile.css` ya global).
- **Regla #4/#4.1**: SQL parametrizado con `%s`; ERP Random **READ-ONLY absoluto** vía `_random_sql_query`/`_random_sql_one`/`erp_engine.fetch_*`.
- **Regla #4.2**: nada de borrar features. `/mantenciones/tickets` sigue vivo hasta que Daniel apruebe el cutover.
- **Regla #5/#6/#7**: soft-delete/audit en acciones destructivas; `chile_fmt` para todo datetime; `rut_fmt` para RUT.
- **Migraciones idempotentes** `_ensure_*` (prod corre con `ILUS_SKIP_MIGRATIONS=1`).
- **Auth** por `g.permissions` (dict plano de bools).
- **Deploy** solo por merge a `main` → GitHub Actions → Cloud Run (Regla #12), con OK explícito de Daniel.

### 1.4 Diferencias clave respecto al origen NestJS (adaptaciones obligatorias)
| Origen (ilus-back) | Destino (ILUS Flask) |
|---|---|
| Postgres + TypeORM, PK `ticketId` camelCase, entidades ORM | MySQL Clever Cloud + helpers `mysql_fetchone/fetchall/execute`, snake_case |
| **Cache local del ERP** (`random_entities`, `random_products`, `random_documents`) con FK `entity_id` | **NO hay mirror del ERP**: se guardan **snapshots planos** (`erp_idmaeen`, `erp_koen`, `rut`, `empresa`, …) y se resuelve **en vivo** con `_random_sql_query`/`erp_engine` |
| Adjuntos en GCS con **URL firmada que expira** (regenerar desde `file_path`) | Adjuntos servidos por el **proxy `/f/<key>`** (bucket privado, ETag, cache 30d) → se guarda la URL `/f/…` tal cual, **sin regenerar firmas** |
| Correo por **Gmail API** + cron `GmailThreadMonitor` (lee respuestas del hilo) | Correo saliente por `_send_ilus_email` (Resend/SMTP, branding ILUS). **Ingesta de respuestas = decisión abierta** (§9) |
| Numerador = `ticketId` entero autoincrement | `numero_ticket` derivado del `id` autoincrement (atómico, §2.11) |

---

## 2. Modelo de datos MySQL propuesto

Todas las tablas se crean dentro de una función idempotente **`_ensure_tickets_tables()`** (patrón de `_ensure_transporte_labels_table` / `_ensure_transporte_columns`), cableada al boot dentro de `with app.app_context():` (bloque incondicional del final de `app.py`, junto a los otros `_ensure_*`). Motor `InnoDB`, charset `utf8mb4`. Fechas en UTC con `CURRENT_TIMESTAMP`/`NOW()` (mostradas con `chile_fmt`, Regla #6).

### 2.1 Listas literales tomadas del modelo (usar tal cual)

**`origen`** (3 valores, de `ticket_details_origin_enum`):
```
'form' | 'backoffice' | 'erp'
```
Etiquetas ES: form=Formulario, backoffice=Back Office, erp=ERP.

**`estado`** (8 valores de `ticket_details_status_enum` + `cancelado` añadido por ILUS para conservar datos de `mant_tickets`, §7):
```
'open' | 'in_progress' | 'pending' | 'resolved' | 'closed'
| 'ot_pending_approval' | 'ot_generated' | 'ot_in_progress'
| 'cancelado'   -- solo para migración desde mant_tickets
```
Etiquetas ES: open=Abierto, in_progress=En Curso, pending=Pendiente, resolved=Resuelto, closed=Cerrado, ot_pending_approval=OT Pendiente de Aprobación, ot_generated=OT Generada, ot_in_progress=OT En Curso.

**`tipo`** (13 valores literales de `ticket_details_type_enum`):
```
'install' | 'tech_support' | 'shipping' | 'quotation' | 'return'
| 'tech_evaluation' | 'maintenance' | 'spare_parts' | 'equipment_transfer'
| 'warranty' | 'repair' | 'spare_parts_store' | 'spare_parts_import'
```
Etiquetas ES: install=Instalación, tech_support=Soporte Técnico, shipping=Envío/Despacho, quotation=Cotización, return=Devolución, tech_evaluation=Evaluación Técnica, maintenance=Mantenimiento, spare_parts=Repuestos, equipment_transfer=Movimiento de Equipos, warranty=Garantía, repair=Reparación, spare_parts_store=Repuestos bodega, spare_parts_import=Repuestos importación.

**Los 8 tipos expuestos en el formulario público** (subconjunto pedido por Daniel):
```
install, tech_support, maintenance, warranty, spare_parts, quotation, shipping, return
```

**`prioridad`** (de `mant_tickets`, útil y ya presente en destino):
```
'baja' | 'media' | 'alta' | 'urgente'   -- default 'media'
```

### 2.2 `tk_tickets` — tabla maestra
```sql
CREATE TABLE IF NOT EXISTS tk_tickets (
  id                INT AUTO_INCREMENT PRIMARY KEY,
  numero_ticket     VARCHAR(30) NOT NULL UNIQUE,          -- TK-2026-00001 (§2.11)
  origen            ENUM('form','backoffice','erp') NOT NULL DEFAULT 'backoffice',
  estado            ENUM('open','in_progress','pending','resolved','closed',
                         'ot_pending_approval','ot_generated','ot_in_progress',
                         'cancelado') NOT NULL DEFAULT 'open',
  tipo              ENUM('install','tech_support','shipping','quotation','return',
                         'tech_evaluation','maintenance','spare_parts','equipment_transfer',
                         'warranty','repair','spare_parts_store','spare_parts_import') NULL,
  prioridad         ENUM('baja','media','alta','urgente') NOT NULL DEFAULT 'media',
  titulo            VARCHAR(300) NULL,                     -- opcional; en el form se deriva del tipo
  descripcion       TEXT NULL,                             -- problema declarado (público máx 260)

  -- Snapshot de cliente/contacto (denormalizado, como ticket_details)
  rut               VARCHAR(12)  NULL,
  empresa           VARCHAR(150) NULL,
  sucursal          VARCHAR(100) NULL,
  nombre_contacto   VARCHAR(150) NULL,
  email             VARCHAR(150) NULL,
  phone             VARCHAR(20)  NULL,

  -- Dirección + Google Places (patrón skill direcciones-google-places)
  direccion         VARCHAR(255) NULL,
  direccion_lat     DECIMAL(10,7) NULL,
  direccion_lng     DECIMAL(10,7) NULL,
  direccion_place_id VARCHAR(200) NULL,
  region_nombre     VARCHAR(120) NULL,
  comuna_nombre     VARCHAR(120) NULL,
  comuna_kocm       VARCHAR(20)  NULL,   -- TABCM.KOCM si se resolvió contra ERP

  -- Snapshot de equipo (denormalizado, como ticket_details)
  producto          TEXT NULL,           -- códigos/nombres kopr concatenados
  marca             VARCHAR(100) NULL,
  sku               VARCHAR(100) NULL,
  numero_documento  TEXT NULL,           -- nudos concatenados

  -- Anclaje ERP en vivo (NO hay FK; snapshot de identificadores)
  erp_idmaeen       INT NULL,            -- IDMAEEN de MAEEN (cliente)
  erp_koen          VARCHAR(50) NULL,    -- KOEN

  -- Asignación
  asignado_a        VARCHAR(190) NULL,   -- username ejecutivo (g.user)
  tecnico_id        INT NULL,            -- FK -> mant_tecnicos.id ON DELETE SET NULL

  -- Vínculos
  visita_id         INT NULL,            -- FK -> mant_visitas.id (la OT) ON DELETE SET NULL
  mant_ticket_id    INT NULL,            -- origen del registro migrado desde mant_tickets

  -- Lectura compartida (burbuja de no-leídos)
  staff_last_read_at DATETIME NULL,

  -- SLA / auditoría
  fecha_limite      DATE NULL,
  notas_internas    TEXT NULL,
  created_by        VARCHAR(190) NULL,
  created_at        DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at        DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  cerrado_at        DATETIME NULL,
  cerrado_por       VARCHAR(190) NULL,

  KEY idx_estado          (estado),
  KEY idx_tipo            (tipo),
  KEY idx_origen          (origen),
  KEY idx_prioridad       (prioridad),
  KEY idx_asignado        (asignado_a),
  KEY idx_created         (created_at),
  KEY idx_estado_updated  (estado, updated_at),   -- orden de bandeja
  KEY idx_erp_idmaeen     (erp_idmaeen),
  KEY idx_rut             (rut),
  KEY idx_mant_ticket     (mant_ticket_id),
  FULLTEXT KEY ft_busqueda (empresa, nombre_contacto, producto, descripcion)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

**Notas de escala (10k+):** `idx_estado_updated` sostiene el listado tipo bandeja (orden `updated_at DESC`); `FULLTEXT` reemplaza los `LIKE %…%` del search por búsqueda indexada; los snapshots evitan JOINs al ERP en el listado. No FK a `random_entities` porque **no existe mirror local del ERP** en ILUS.

### 2.3 `tk_ticket_equipos` — productos ERP asociados (analog. `ticket_products`)
```sql
CREATE TABLE IF NOT EXISTS tk_ticket_equipos (
  id           INT AUTO_INCREMENT PRIMARY KEY,
  ticket_id    INT NOT NULL,                 -- FK -> tk_tickets.id ON DELETE CASCADE
  erp_kopr     VARCHAR(100) NULL,            -- código producto (MAEPR.KOPR)
  nombre       VARCHAR(300) NULL,            -- NOKOPR
  tipo         VARCHAR(100) NULL,            -- TIPR
  sku          VARCHAR(100) NULL,
  serie        VARCHAR(120) NULL,
  cantidad     INT NOT NULL DEFAULT 1,
  maquina_id   INT NULL,                     -- FK opcional -> mant_maquinas.id (puente con Mantenciones)
  notas        VARCHAR(500) NULL,
  created_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_ticket_kopr (ticket_id, erp_kopr),
  KEY idx_ticket (ticket_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```
Espeja el M2M `ticket_products` + la relación N:N `mant_ticket_equipos` (que sí ancla a `mant_maquinas`). `productData` (cantidades por producto) del origen se colapsa aquí en la columna `cantidad`.

### 2.4 `tk_ticket_documentos` — documentos ERP asociados (analog. `ticket_documents`)
```sql
CREATE TABLE IF NOT EXISTS tk_ticket_documentos (
  id            INT AUTO_INCREMENT PRIMARY KEY,
  ticket_id     INT NOT NULL,                -- FK -> tk_tickets.id ON DELETE CASCADE
  erp_tido      VARCHAR(10) NULL,            -- tipo doc (BLV, FCV, NVV, COV…)
  erp_nudo      VARCHAR(40) NULL,            -- número de documento
  erp_idmaeedo  INT NULL,                    -- MAEEDO.IDMAEEDO
  fecha         DATE NULL,                   -- FEEMDO
  monto         INT NULL,                    -- VANEDO
  created_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_ticket_doc (ticket_id, erp_tido, erp_nudo),
  KEY idx_ticket (ticket_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

### 2.5 `tk_mensajes` — conversación + timeline (fusión de `ticket_mail_messages` y `mant_ticket_bitacora`)
Una sola tabla cubre correo saliente/entrante **y** eventos internos, para no perder la trazabilidad de la bitácora.
```sql
CREATE TABLE IF NOT EXISTS tk_mensajes (
  id             INT AUTO_INCREMENT PRIMARY KEY,
  ticket_id      INT NOT NULL,               -- FK -> tk_tickets.id ON DELETE CASCADE
  tipo           ENUM('comentario',          -- nota interna (no sale al cliente)
                      'mensaje',              -- correo saliente al cliente  (= 'message')
                      'client_message',       -- respuesta entrante del cliente
                      'cambio_estado','asignacion','creacion','cierre',
                      'reapertura','archivo','otro') NOT NULL DEFAULT 'comentario',
  contenido      MEDIUMTEXT NULL,            -- HTML en correos
  metadata       TEXT NULL,                  -- JSON: {campo, antes, nuevo, cc[]...}
  mail_message_id VARCHAR(150) NULL,         -- id del mensaje en el proveedor (hilo)
  es_interno     TINYINT(1) NOT NULL DEFAULT 1,  -- 0 = visible al cliente
  usuario        VARCHAR(190) NULL,          -- autor (NULL = cliente/sistema)
  message_date   DATETIME NULL,             -- fecha real del correo
  created_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
  KEY idx_ticket   (ticket_id, created_at),
  KEY idx_unread   (ticket_id, tipo, created_at)   -- soporta el conteo de no-leídos
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```
Semántica de `tipo` heredada del origen: `mensaje`=correo saliente de soporte, `client_message`=respuesta entrante del cliente, `comentario`=nota interna. Los eventos (`cambio_estado`, `asignacion`, `creacion`, `cierre`, `reapertura`) provienen de `mant_ticket_bitacora`.

### 2.6 `tk_adjuntos` — adjuntos en GCS (analog. `ticket_attachments` + `mant_ticket_adjuntos`)
```sql
CREATE TABLE IF NOT EXISTS tk_adjuntos (
  id             INT AUTO_INCREMENT PRIMARY KEY,
  ticket_id      INT NOT NULL,               -- FK -> tk_tickets.id ON DELETE CASCADE
  mensaje_id     INT NULL,                   -- FK opcional -> tk_mensajes.id (adjunto de un correo)
  archivo_url    VARCHAR(500) NOT NULL,      -- '/f/<key>' (proxy GCS)
  archivo_path   VARCHAR(500) NULL,          -- object key / public_id en el bucket
  archivo_nombre VARCHAR(300) NULL,
  mime_type      VARCHAR(150) NULL,
  file_size_kb   INT NULL,
  origen         ENUM('form','backoffice','cliente') NOT NULL DEFAULT 'backoffice',
  subido_por     VARCHAR(190) NULL,
  created_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
  KEY idx_ticket (ticket_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```
Diferencia con el origen: se guarda la URL **`/f/<key>`** (estable) en `archivo_url`; **no** hay que regenerar URLs firmadas al listar (lo resuelve el proxy `/f/<path:key>`). La tabla `mant_ticket_adjuntos` (definida pero **nunca usada**) queda deprecada — no se borra (Regla #4.2).

### 2.7 `tk_acciones` — pestaña Acciones (NUEVA, no existe hoy)
Tabla genérica de items accionables (agendar hotel del técnico, logística de acceso, tareas por equipo, agendamiento de visita, transporte…). Se modela flexible con `tipo` + `datos JSON`.
```sql
CREATE TABLE IF NOT EXISTS tk_acciones (
  id               INT AUTO_INCREMENT PRIMARY KEY,
  ticket_id        INT NOT NULL,             -- FK -> tk_tickets.id ON DELETE CASCADE
  tipo             ENUM('agendar_visita','hotel_tecnico','logistica_acceso',
                        'tarea_equipo','transporte','compra','otro') NOT NULL DEFAULT 'otro',
  titulo           VARCHAR(300) NOT NULL,
  descripcion      TEXT NULL,
  estado           ENUM('pendiente','en_progreso','completada','cancelada')
                        NOT NULL DEFAULT 'pendiente',
  datos            JSON NULL,                -- {hotel, checkin, checkout, tecnico, tareas[], accesos{}...}
  tecnico_id       INT NULL,                 -- FK opcional -> mant_tecnicos.id
  responsable      VARCHAR(190) NULL,
  fecha_programada DATE NULL,
  hora_programada  TIME NULL,
  costo            INT NULL,
  created_by       VARCHAR(190) NULL,
  created_at       DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at       DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_ticket (ticket_id),
  KEY idx_estado (estado)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

### 2.8 `tk_cotizaciones` — cotización (analog. `quotations`)
```sql
CREATE TABLE IF NOT EXISTS tk_cotizaciones (
  id                INT AUTO_INCREMENT PRIMARY KEY,
  numero_cotizacion VARCHAR(30) NOT NULL UNIQUE,   -- COT-2026-00001
  ticket_id         INT NULL,                       -- FK -> tk_tickets.id ON DELETE SET NULL
  estado            ENUM('draft','sent','approved','rejected','expired')
                        NOT NULL DEFAULT 'draft',
  erp_idmaeen       INT NULL,
  erp_koen          VARCHAR(50) NULL,
  rut               VARCHAR(12) NULL,
  empresa           VARCHAR(150) NULL,
  costo_tecnico     INT NOT NULL DEFAULT 0,
  costo_ruta        INT NOT NULL DEFAULT 0,
  subtotal_items    INT NOT NULL DEFAULT 0,
  subtotal          INT NOT NULL DEFAULT 0,
  descuento_pct     DECIMAL(5,2) NOT NULL DEFAULT 0,
  descuento_monto   INT NOT NULL DEFAULT 0,
  iva_pct           DECIMAL(5,2) NOT NULL DEFAULT 19,
  iva_monto         INT NOT NULL DEFAULT 0,
  total             INT NOT NULL DEFAULT 0,
  valida_hasta      DATE NULL,
  notas             TEXT NULL,
  created_by        VARCHAR(190) NULL,
  created_at        DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at        DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_ticket (ticket_id),
  KEY idx_estado (estado)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```
Estados literales del origen (`quotations.status`): `draft | sent | approved | rejected | expired`. Regla del origen conservada: una cotización requiere al menos `ticket_id` o (`erp_idmaeen`/`rut`). Total = `subtotal_items + costo_ruta` − descuento + IVA(19%).

### 2.9 `tk_cotizacion_items` — ítems de cotización (analog. `quotation_items`)
```sql
CREATE TABLE IF NOT EXISTS tk_cotizacion_items (
  id             INT AUTO_INCREMENT PRIMARY KEY,
  cotizacion_id  INT NOT NULL,              -- FK -> tk_cotizaciones.id ON DELETE CASCADE
  item_tipo      ENUM('producto','servicio','ruta','otro') NOT NULL DEFAULT 'producto',
  erp_kopr       VARCHAR(100) NULL,
  descripcion    VARCHAR(300) NULL,
  cantidad       INT NOT NULL DEFAULT 1,
  precio_unitario INT NOT NULL DEFAULT 0,
  subtotal       INT NOT NULL DEFAULT 0,
  descuento_pct  DECIMAL(5,2) NOT NULL DEFAULT 0,
  total          INT NOT NULL DEFAULT 0,
  desde_ticket   TINYINT(1) NOT NULL DEFAULT 0,  -- vino del ticket/OT vs manual
  notas          TEXT NULL,
  KEY idx_cotizacion (cotizacion_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```
Estados literales del ítem (`quotation_items.itemType`): `producto | servicio | ruta | otro`.

### 2.10 Relaciones (resumen)
```
tk_tickets 1──N tk_ticket_equipos      (equipos ERP; opcional maquina_id → mant_maquinas)
tk_tickets 1──N tk_ticket_documentos   (facturas/boletas ERP)
tk_tickets 1──N tk_mensajes            (conversación + eventos)  1──N tk_adjuntos.mensaje_id
tk_tickets 1──N tk_adjuntos            (GCS /f/<key>)
tk_tickets 1──N tk_acciones            (hotel técnico, agenda, accesos…)
tk_tickets 1──N tk_cotizaciones        1──N tk_cotizacion_items
tk_tickets N──1 mant_visitas (visita_id)   -- la OT (1:1 lógico, como mant_tickets)
tk_tickets N──1 mant_tecnicos (tecnico_id)
tk_tickets snapshot ERP: erp_idmaeen/erp_koen/rut  (resuelto en vivo, sin FK)
```
Todas las FK internas con `ON DELETE CASCADE` salvo `visita_id`/`tecnico_id` (`SET NULL`) y `tk_cotizaciones.ticket_id` (`SET NULL`). Las FK a `mant_*` se declaran solo si el motor lo permite sin bloquear (si hay riesgo, se valida por lookup como hace `spare_part_requests`).

### 2.11 Numeración atómica (mejora sobre `_next_ticket_number`)
`_next_ticket_number` (mant) **no es atómico** (lee último + 1, sin lock → colisión posible). Propuesta race-free: insertar la fila primero y derivar el número del `id` autoincrement:
```sql
-- 1) INSERT sin numero
-- 2) UPDATE tk_tickets SET numero_ticket = CONCAT('TK-', YEAR(created_at), '-', LPAD(id,5,'0'))
--    WHERE id = <lastrowid>
```
Único y sin condición de carrera. Si Daniel exige secuencia **reiniciable por año** (00001 cada enero), usar una tabla contador `tk_counters(anio, valor)` con `SELECT … FOR UPDATE` dentro de una transacción `get_db()`. Cotizaciones (`COT-YYYY-NNNNN`) siguen el mismo patrón.

### 2.12 Contador de no-leídos (MySQL)
```sql
-- Resumen para la burbuja del menú:
SELECT COUNT(DISTINCT t.id)
FROM tk_tickets t
JOIN tk_mensajes m ON m.ticket_id = t.id AND m.tipo = 'client_message'
WHERE t.estado NOT IN ('resolved','closed','cancelado')
  AND m.created_at > COALESCE(t.staff_last_read_at, '1970-01-01');
```
**Marcar leído sin subir el ticket en la bandeja** (clave en MySQL): como `updated_at` tiene `ON UPDATE CURRENT_TIMESTAMP`, hay que fijarlo explícitamente para que no se dispare:
```sql
UPDATE tk_tickets SET staff_last_read_at = NOW(), updated_at = updated_at WHERE id = %s;
```

---

## 3. Rutas Flask propuestas

Estilo idéntico al de Mantenciones (`mant_tickets_list/nuevo/ficha` + APIs `/mantenciones/api/tickets/*`). Se puede implementar **inline en `app.py`** (como mantenciones/transporte) o en archivo aparte **`tickets_module.py`** con `register_tickets_routes(app, ctx)` (patrón `pickups_module.py`). Recomendado: `tickets_module.py` por tamaño y aislamiento.

**Protección:** decorador `@_tickets_required` (clon de `@_mant_required`) que exige `g.permissions['tickets'] or g.permissions['superadmin']`, o reutilizar `@require_permission('tickets')`. Crear una **permission key nueva `tickets`** en `PERMS_KEYS` (§9). Respuestas: HTML (`render_template`) o JSON (`jsonify`), CSRF automático por el wrapper global de `base.html`.

### 3.1 Páginas internas (HTML)
| Método | Path | Endpoint | Propósito |
|---|---|---|---|
| GET | `/tickets` | `tk_list` | Listado global (bandeja) con filtros/KPIs |
| GET | `/tickets/nuevo` | `tk_nuevo` | Formulario de creación interna (backoffice) |
| GET | `/tickets/<int:tid>` | `tk_ficha` | Ficha con pestañas Información/Conversación/Acciones/Cotizaciones |

### 3.2 API interna (JSON) — `@_tickets_required`
| Método | Path | Propósito (origen espejado) |
|---|---|---|
| GET | `/tickets/api/tickets` | Listado paginado + filtros + KPIs + `unread_count` por fila (`GET /tickets/backoffice`) |
| POST | `/tickets/api/tickets` | Crear ticket backoffice, `origen='backoffice'` (`POST /tickets/backoffice`) |
| GET | `/tickets/api/tickets/<tid>` | Detalle + equipos + documentos + mensajes (`GET /:id`) |
| PATCH | `/tickets/api/tickets/<tid>` | Actualizar (estado, prioridad, asignado_a, tipo, campos…) (`PUT /:id`) |
| DELETE | `/tickets/api/tickets/<tid>` | Eliminar (superadmin o `created_by`; audit previo, §3.6) |
| PATCH | `/tickets/api/tickets/<tid>/marcar-leido` | Poner en cero no-leídos, sin tocar `updated_at` (`PATCH /:id/mark-read`) |
| GET | `/tickets/api/unread-summary` | Resumen burbuja del menú (`GET /unread-summary`) |
| GET | `/tickets/api/tickets/<tid>/mensajes` | Conversación combinada (correos + comentarios + adjuntos) |
| POST | `/tickets/api/tickets/<tid>/comentario` | Comentario interno (`type='comment'`) (`POST /:ticketId/comments`) |
| POST | `/tickets/api/tickets/<tid>/responder-cliente` | Enviar correo real al cliente + CC (`POST /send-response-email`) |
| POST | `/tickets/api/tickets/<tid>/iniciar-correo` | Primer correo / semilla del hilo (`POST /send-initial-email`) |
| GET/POST | `/tickets/api/tickets/<tid>/adjuntos` | Listar / subir adjuntos backoffice a GCS (`/:ticketId/attachments`) |
| POST | `/tickets/api/tickets/<tid>/equipos` | Agregar producto ERP |
| DELETE | `/tickets/api/tickets/<tid>/equipos/<eid>` | Quitar producto |
| POST | `/tickets/api/tickets/<tid>/documentos` | Asociar documento ERP (`nudo`+`tido`) |
| POST | `/tickets/api/tickets/desde-documento` | **Crear ticket desde una o varias facturas** (`documentNumbers[]`) |
| POST | `/tickets/api/tickets/desde-cotizacion` | **Crear ticket desde cotización interna** |
| GET | `/tickets/api/tickets/<tid>/lifecycle` | Ciclo consolidado cliente→cotización→OT→cierre (`GET /:id/lifecycle`) |
| POST | `/tickets/api/tickets/<tid>/convertir-en-ot` | Generar OT (`mant_visitas`) reutilizando el flujo de `mant_ticket_convertir_ot` |
| GET | `/tickets/api/export/csv` | Export CSV (mismas columnas ES que `export/csv`) |
| GET | `/tickets/api/sla/export-csv` | Métricas SLA desde audit log (`sla/export-csv`) |

### 3.3 API Acciones — `@_tickets_required`
| Método | Path | Propósito |
|---|---|---|
| GET | `/tickets/api/tickets/<tid>/acciones` | Listar acciones del ticket |
| POST | `/tickets/api/tickets/<tid>/acciones` | Crear acción (ej. hotel técnico) |
| PATCH | `/tickets/api/acciones/<aid>` | Cambiar estado/datos |
| DELETE | `/tickets/api/acciones/<aid>` | Quitar acción |

### 3.4 API Cotizaciones — `@_tickets_required`
| Método | Path | Propósito (origen `quotation.service`) |
|---|---|---|
| GET | `/tickets/api/tickets/<tid>/cotizaciones` | Cotizaciones del ticket |
| GET | `/tickets/api/cotizaciones/preview/<tid>` | Preview desde ticket (ítem por equipo) |
| GET | `/tickets/api/cotizaciones/preview-doc/<tido>/<nudo>` | Preview desde documento ERP (filtra SKUs `ZZ*`) |
| POST | `/tickets/api/cotizaciones` | Crear/persistir cotización + ítems |
| PATCH | `/tickets/api/cotizaciones/<cid>` | Cambiar estado (approved/rejected/…) |
| POST | `/tickets/api/cotizaciones/<cid>/enviar` | Generar PDF y responder el hilo de correo |

### 3.5 API ERP (read-only) — proxys — `@_tickets_required`
| Método | Path | Backend |
|---|---|---|
| GET | `/tickets/api/erp/cliente?rut=` | `_random_sql_one` sobre `MAEEN` / `erp_engine.fetch_entity` (§6) |
| GET | `/tickets/api/erp/productos?q=` | `MAEPR` search |
| GET | `/tickets/api/erp/documento/<tido>/<nudo>` | `erp_engine.fetch_document` (MAEEDO+MAEDDO, filtra `ZZ*`) |

### 3.6 Rutas públicas (sin `@login_required`)
Sin gate global de login: una ruta sin decorador es pública. **Añadir el prefijo `/soporte/` a `_CSRF_EXEMPT_PREFIXES`** y aplicar rate-limit (patrón `_public_search_rate_ok` 12/min por IP, `_track_rate_ok` 60/min por token).

| Método | Path | Propósito |
|---|---|---|
| GET | `/soporte` | Formulario público (5 pasos, embebible por iframe) |
| POST | `/soporte/api/crear` | Crear ticket `origen='form'`, `estado='open'`; devuelve `{ticket_id, numero_ticket, upload_token}` |
| POST | `/soporte/api/adjuntos/<tid>` | Subir fotos/videos/PDF a GCS; **gated por `upload_token` HMAC** (patrón `_ot_firma_token`) + rate-limit |
| GET | `/soporte/api/erp/productos?q=` | Búsqueda pública de productos (proxy read-only) |
| GET | `/soporte/api/erp/regiones-comunas` | Territorios (o resolver todo por Google Places) |

**Auditoría/borrado:** `DELETE` registra `_audit('tk_ticket_delete', target_id=tid, details=…)` **antes** de borrar (Regla #5); autorización superadmin o `created_by`. Toda transición de estado se registra en `tk_mensajes` (evento) y `_audit` (base del SLA).

---

## 4. El formulario público

Página `/soporte` (plantilla `templates/tickets/soporte_publico.html`), estética control-room negra+roja (tokens ILUS), mobile-first. Replica los **5 pasos** del origen (`formulario.html`), pero servido por Flask y apuntando a nuestras rutas `/soporte/api/*` (no a la API de terceros).

### 4.1 Campos y validaciones
| Paso | Campo | Validación |
|---|---|---|
| 1 | **Tipo de solicitud** (pills con icono) | requerido; 1 de los **8 públicos** (§2.1) |
| 2 | **RUT de compra** | requerido; `validar_rut()` en backend (módulo-11) + `ilusRutsMatch`/`formatRut` en front; se muestra con `rut_fmt` |
| 2 | Empresa / persona | opcional |
| 2 | Nombre de contacto | requerido |
| 2 | Teléfono | requerido; `isValidChileanPhone` `/^[2-9]\d{8}$/`, formateo `+56 9 XXXX XXXX` |
| 2 | Correo | requerido; regex email; canal de avisos |
| 3 | **Dirección** (Google Places) | requerida; `ilusPlacesAutocomplete({country:'cl',types:['address']})`; guarda `direccion`, `direccion_lat/lng/place_id`; resuelve `region_nombre`/`comuna_nombre` en 2° plano (campos ocultos, no bloquean el envío) |
| 3 | Sucursal | opcional |
| 4 | **Productos** (chips, ERP) | requerido ≥1; búsqueda con debounce 300ms a `/soporte/api/erp/productos`; envía `productCodes[]` (kopr) |
| 4 | **N° boleta/factura** | **opcional**; varios separados por coma → `documentNumbers[]` (nudo) |
| 5 | **Descripción del problema** | requerida; `maxlength=260` con contador en vivo (rojo ≤20 restantes) |
| 5 | **Adjuntos** | hasta **10** archivos (fotos/videos/PDF); preview con miniatura; sin restricción MIME |

### 4.2 Conexión al ERP (read-only)
- `/soporte/api/erp/productos?q=` → `_random_sql_query` sobre `MAEPR` (`KOPR`, `NOKOPR`, `TIPR`), o `erp_engine`, con `q` parametrizado (`%s`), sin escritura.
- Al crear, el backend resuelve **cliente por RUT** (cuerpo sin DV vía `_rut_cuerpo`, porque `MAEEN.RTEN` va sin DV) y **documentos por `nudo`+`tido`** para poblar `erp_idmaeen`, `erp_koen`, `empresa`, snapshots de equipo/documento y las tablas `tk_ticket_equipos`/`tk_ticket_documentos`. Si el RUT no matchea el ERP, el ticket igual se crea con los snapshots del formulario (como hace `ticket_details` con sus columnas de texto libre).

### 4.3 Envío en 2 fases + subida de archivos (GCS `/f/`)
1. `POST /soporte/api/crear` (JSON): normaliza strings (mayúsculas + sin acentos, como el origen), valida, inserta el ticket (`origen='form'`, `estado='open'`, `asignado_a=NULL`), numera (§2.11), dispara **correo de confirmación** al cliente (`_send_ilus_email`, semilla del hilo, asunto estándar §5) y devuelve `{ticket_id, numero_ticket, upload_token}`.
2. Si hay archivos → `POST /soporte/api/adjuntos/<tid>` (multipart, header con `upload_token`): compresión **en el navegador** (canvas 1600px, JPEG 0.85; imágenes) y subida con progreso; en backend `_uploader_upload(src, public_id, folder='tickets', resource_type=…)` → GCS bucket `ilus-app-fotos`, guarda `/f/<key>` en `tk_adjuntos` (`origen='form'`). Cleanup del blob huérfano si el INSERT falla (patrón `upload_photo`).
3. Pantalla de éxito con folio `#numero_ticket`. Si falla la subida, se avisa que un ejecutivo pedirá los archivos.

Toda alerta/confirmación del form usa `ilusToast`/`ilusAlert` (Regla #1); loader `ilusLoader` mientras sube.

---

## 5. La ficha interna con pestañas

Página `/tickets/<tid>` (plantilla `templates/tickets/ticket_ficha.html`, `{% extends "base.html" %}`), navegación por tabs. Al montar llama `marcar-leido`. Pestañas dependientes de permiso (un rol tipo "seller" ve solo Información + Conversación; el resto ve todas).

### 5.1 Información (analog. *Origen*)
- **Identificación del cliente** (bien separada y **autocompletada desde el ERP**): RUT (`rut_fmt`), empresa, nombre contacto, correo, teléfono, dirección (+ Región/Comuna encadenadas), sucursal, N° documento, tipo de solicitud, estado, prioridad, ejecutivo asignado. Campos editables → `PATCH /tickets/api/tickets/<tid>`.
- **Problema declarado** (`descripcion`).
- **Equipos**: lista desde `tk_ticket_equipos`; buscar producto ERP y agregar; **"Asignar desde documento"** (modal con `tido`+`nudo` → `/tickets/api/erp/documento/<tido>/<nudo>`, filtra `ZZ*`, selección múltiple).
- **Documentos ERP** asociados (`tk_ticket_documentos`).
- **Ciclo de vida** (opcional, desde `/lifecycle`): cliente → cotización → OT → cierre.
- Fechas mostradas con `chile_fmt`.

### 5.2 Conversación (analog. *Respuestas*)
- Combina en `/tickets/api/tickets/<tid>/mensajes`: correos salientes (`tipo='mensaje'`), respuestas del cliente (`tipo='client_message'`), comentarios internos (`tipo='comentario'`) y adjuntos GCS; ordenados por fecha.
- Burbujas estilo chat: entrantes a la izquierda (fondo claro), salientes a la derecha (degradado rojo `#dc2626→#991b1b`), comentarios internos en ámbar con candado.
- Composición: editor de texto rico, **CC**, botón **"Enviar al cliente"** (`/responder-cliente`, correo real), **"Comentario interno"** (`/comentario`, no sale), **"Iniciar conversación"** (`/iniciar-correo`) si aún no hay hilo. Adjuntos por correo y "subir imagen al ticket" (GCS, se ve inline pero no sale por correo).
- **No-leídos**: burbuja roja por ticket (listado) y en el ítem del menú (`/unread-summary` cada 60s).

### 5.3 Acciones (NUEVA)
- Lista de `tk_acciones` con estado (pendiente/en_progreso/completada/cancelada).
- Crear acción por `tipo`: **hotel del técnico** (`datos`: hotel, check-in/out, técnico, costo), **agendar visita** (técnico(s), fecha/hora, logística de acceso: estacionamiento/ascensor/montacargas), **tareas por equipo**, **transporte**, **compra**, etc.
- Botón **"Crear/Actualizar OT"** → `/convertir-en-ot` (genera `mant_visitas`, hereda equipos como `mant_visita_tareas`, vínculo bidireccional `visita_id`). Sección "Órdenes de Trabajo" con las OT ligadas.

### 5.4 Cotizaciones
- Lista de `tk_cotizaciones` del ticket con acciones Ver/Enviar/Aprobar/Rechazar (`ilusConfirm`).
- Preview desde ticket (`/cotizaciones/preview/<tid>`) o desde documento (`/cotizaciones/preview-doc/<tido>/<nudo>`), armado de ítems (`tk_cotizacion_items`), totales con IVA 19% (`formatCurrency` CLP).
- **Enviar** genera PDF y responde el hilo de correo. Nota: la generación PDF por Chromium/Playwright puede estar caída en prod (memoria) → considerar HTML imprimible como fallback.

---

## 6. Integración con el ERP Random (read-only)

**Regla de oro:** toda lectura del ERP pasa por `_random_sql_query`/`_random_sql_one` (whitelist SELECT/WITH, blacklist de tokens, `%s`, autocommit OFF) o por `erp_engine.fetch_*` (solo GET). **Nunca escribir** (Regla #4.1). No hay mirror local del ERP en ILUS → siempre en vivo, con caché en proceso y debounce en el front.

### 6.1 Autocompletar cliente desde RUT
- Normalizar: `_rut_cuerpo(rut)` (cuerpo sin DV) porque `MAEEN.RTEN` va **sin DV ni puntos**.
- Query: `SELECT KOEN, NOKOEN, GIEN, DIEN, CMEN, FOEN, EMAIL, IDMAEEN FROM MAEEN WHERE RTEN = %s` (mapea a `erp_koen`, `empresa=NOKOEN`, `direccion=DIEN`, `email`, `phone=FOEN`, `erp_idmaeen=IDMAEEN`). Fallback por nombre (`NOKOEN`, `COLLATE Modern_Spanish_CI_AI`).
- Contrato de salida uniforme para el combobox: `{koen, nokoen, rten, dien, foen, email, idmaeen, comuna}`.

### 6.2 Autocompletar equipo/producto
- `SELECT KOPR, NOKOPR, TIPR, STFI1 FROM MAEPR WHERE KOPR = %s` (o search por `NOKOPR`). Stock por `MAEST` si se requiere.

### 6.3 Crear ticket desde factura(s) — `nudo`+`tido`
- `erp_engine.fetch_document(tido, nudo)` → encabezado `MAEEDO` (`IDMAEEDO`, `FEEMDO`, `VANEDO`) + líneas `MAEDDO` (`KOPRCT`, `NOKOPR`, `TIPR`, `CAPRCO1`). **Filtrar SKUs que empiezan con `ZZ`** (servicios/conceptos), como hace el origen.
- `POST /tickets/api/tickets/desde-documento` (uno o varios documentos): crea el ticket, puebla `tk_ticket_documentos` y `tk_ticket_equipos` con las líneas reales, resuelve el cliente por `IDMAEEN`/RUT del documento.
- **Patrón "documento → ticket" del origen** (`ZZINSTALACION`): opcional en fase avanzada, un job read-only que detecta líneas `ZZINSTALACION` en documentos recientes y crea tickets `origen='erp'`, `tipo='install'` (notificando a admins). No escribe al ERP.

### 6.4 Cotización desde documento
- `preview-from-document/<tido>/<nudo>`: usa `fetch_document`, filtra `ZZ*`, arma un ítem por producto real (`precio`, `CAPRCO1`), resuelve costo de ruta por comuna.

---

## 7. Plan de migración / centralización desde `mant_tickets`

**Filosofía:** aditivo primero, cutover después con OK explícito de Daniel. **Nunca** se rompe `/mantenciones/tickets` (Regla #4.2).

### 7.1 Qué se conserva intacto
- Las 4 tablas `mant_tickets`, `mant_ticket_equipos`, `mant_ticket_bitacora`, `mant_ticket_adjuntos` y sus rutas (`mant_tickets_list/nuevo/ficha` + APIs, `@_mant_required`) **siguen funcionando** durante toda la migración.
- Helpers `_next_ticket_number` y `_ticket_log` permanecen (los usa el flujo de mant).

### 7.2 Estrategia aditiva (fases 1–6)
1. Se construye `tk_*` **en paralelo**, sin tocar `mant_*`.
2. Un **importador idempotente** `_tk_import_desde_mant()` (ejecutable a demanda, no en boot) copia `mant_tickets` → `tk_tickets` guardando `mant_ticket_id` (dedup: si ya existe un `tk_tickets` con ese `mant_ticket_id`, actualiza en vez de duplicar). Mapea:

**Estados** `mant_tickets.estado` → `tk_tickets.estado`:
| mant | central |
|---|---|
| abierto | open |
| en_proceso | in_progress |
| esperando_cliente | pending |
| esperando_repuesto | pending |
| resuelto | resolved |
| cerrado | closed |
| cancelado | cancelado |

**Tipos** `mant_tickets.tipo` → `tk_tickets.tipo` (propuesta, a confirmar por Daniel):
| mant | central |
|---|---|
| cambio | equipment_transfer |
| garantia | warranty |
| falla | repair |
| consulta | tech_support |
| cotizacion | quotation |
| presupuesto | quotation |
| seguimiento | tech_support |
| otro | tech_support |

**Bitácora** `mant_ticket_bitacora` → `tk_mensajes` (tipos: comentario/cambio_estado/asignacion/creacion/cierre/reapertura/otro se conservan; `es_interno` respetado). **Equipos** `mant_ticket_equipos` → `tk_ticket_equipos` (conservando `maquina_id`). **Adjuntos**: `mant_ticket_adjuntos` está vacía (nunca se usó), no hay nada que migrar.

3. Numeración: los tickets migrados **conservan su `TKT-YYYY-NNNNN`** en `numero_ticket`; los nuevos centrales usan `TK-YYYY-NNNNN`. Ambos conviven en la columna UNIQUE.

### 7.3 Cutover (fase final, con OK de Daniel)
- El **sidebar** apunta el link "Tickets" al módulo central (`/tickets`). El link de tickets de mantención **se mantiene** (Regla #4.2) o `/mantenciones/tickets` pasa a **redirección 302** a `/tickets?cliente=…` — decisión de Daniel.
- Los tickets nuevos de mantención se crean en el central; el flujo `mant_ticket_convertir_ot` se replica en `/tickets/api/tickets/<tid>/convertir-en-ot` reutilizando `mant_visitas` y `_next_ot_number`, para no duplicar OTs.
- Se documenta que `tk_*` es la fuente única; `mant_tickets` queda **read-only/congelado** (no se borra).

### 7.4 Qué NO hacer
- No borrar `mant_ticket_adjuntos` ni ninguna ruta de mant.
- No cambiar `_ticket_log`/`_next_ticket_number` (los usa mant); el central usa **sus propios** helpers (`_tk_log`, numeración §2.11).

---

## 8. Fases de implementación (incrementales y verificables)

### Fase 1 — CRUD interno mínimo útil ✅ entregable
- `_ensure_tickets_tables()` (tk_tickets, tk_mensajes, tk_adjuntos) + wiring al boot.
- Permission key `tickets` + `@_tickets_required` + link en sidebar.
- Páginas `/tickets` (listado con filtros + KPIs + paginación 2-fases), `/tickets/nuevo`, `/tickets/<tid>`.
- API: listar/crear/detalle/`PATCH`/`DELETE`/marcar-leido/unread-summary.
- Conversación **solo con comentarios internos** (`tk_mensajes` tipo `comentario`) + adjuntos GCS `/f/`.
- Autocomplete de cliente por RUT desde ERP (§6.1).
- **Verificable:** crear, listar, filtrar, comentar, adjuntar foto, cambiar estado, no-leídos.

### Fase 2 — Formulario público
- `/soporte` (5 pasos), `/soporte/api/crear` + adjuntos con `upload_token` HMAC + rate-limit + prefijo CSRF-exento.
- Proxys ERP públicos (productos), Google Places, correo de confirmación.
- **Verificable:** ticket entra desde la web, con RUT/teléfono/dirección/producto/archivos válidos.

### Fase 3 — Correo real bidireccional
- Saliente: `/iniciar-correo`, `/responder-cliente` con plantilla marca ILUS (`_send_ilus_email`, asunto estándar `ILUS · Ticket #…`).
- Entrante: implementar la ingesta de respuestas (decisión §9) → `tk_mensajes` tipo `client_message` → alimenta no-leídos.
- **Verificable:** correo llega al cliente; su respuesta aparece en la conversación.

### Fase 4 — Pestaña Acciones
- `tk_acciones` + API + UI (hotel técnico, agenda de visita, logística de acceso).
- Botón "Crear OT" → `mant_visitas` (reutiliza flujo de convertir-en-ot).

### Fase 5 — Cotizaciones
- `tk_cotizaciones` + `tk_cotizacion_items` + API (preview desde ticket / desde documento, crear, estados, enviar PDF).
- **Verificable:** cotizar desde un ticket y desde una factura ERP.

### Fase 6 — Documentos ERP → ticket + lifecycle + migración
- `/desde-documento`, `/desde-cotizacion`, `/lifecycle`.
- `_tk_import_desde_mant()` (aditivo). Detección `ZZINSTALACION` (opcional).

### Fase 7 — SLA, export y endurecimiento a escala
- Export CSV + SLA desde audit log.
- Índices FULLTEXT afinados, thumbnails/lifecycle GCS, caché de proxys ERP, revisión de N+1 en el listado, verificación de rendimiento con 10k+ filas.
- **Cutover** con OK de Daniel (§7.3).

---

## 9. Riesgos y decisiones abiertas para consultar a Daniel

1. **¿Reemplaza o convive con `mant_tickets`?** El plan asume convivencia + cutover suave. Confirmar si `/mantenciones/tickets` se redirige o se mantiene como acceso paralelo (Regla #4.2).
2. **Ingesta de respuestas del cliente (crítico):** ILUS hoy envía por Resend/SMTP y **no tiene** el cron Gmail del origen. Opciones: (a) buzón monitoreado (`soportetec@sphs.cl`) + poller que inserte `client_message`; (b) **link público con token HMAC** ("responder mi ticket") reusando el patrón `_ot_firma_token` (más ILUS-native, sin infra de correo entrante); (c) Inbound Parse. Elegir una.
3. **Tipos: 8 públicos vs 13 internos.** El modelo tiene 13; Daniel pidió 8. Propuesta: enum interno con los 13, formulario público con 8. Confirmar el subconjunto y las etiquetas ES.
4. **Mapeo `mant.tipo` → `tk.tipo`** (§7.2) es una propuesta con casos difusos (`cambio`, `falla`, `seguimiento`). Validar.
5. **Numeración:** ¿`TK-YYYY-NNNNN` derivado del `id` (atómico, no reinicia por año) o secuencia reiniciable por año con tabla contador? (§2.11).
6. **Permiso nuevo `tickets`:** agregar clave a `PERMS_KEYS`/matriz `rol_permisos`, o reutilizar `mantenciones`. Definir qué roles ven qué (equivalente a "seller" del origen).
7. **Formulario público embebido en Shopify (iframe):** ¿se requiere API Key/CORS como el origen, o solo se usa desde dominio ILUS? Afecta el gating de `/soporte/api/*`.
8. **Escala de adjuntos (10k tickets con fotos/videos):** política de retención/lifecycle en GCS, límites de tamaño (público 50MB, backoffice 25MB en el origen), y generación de thumbnails. Confirmar límites para ILUS.
9. **Carga al ERP por autocompletar:** búsquedas en vivo (`MAEEN`/`MAEPR`) bajo alta concurrencia. Definir debounce/caché y umbrales de rate-limit.
10. **PDF de cotización:** Chromium/Playwright podría estar caído en prod (memoria). ¿PDF real o HTML imprimible como fallback?
11. **Región/Comuna sin `RandomTerritory`:** se guardan como texto (`region_nombre`/`comuna_nombre`) + `comuna_kocm` (TABCM). Confirmar si se necesita normalización estricta o basta con Google Places.
12. **Borrado:** el origen y `mant` hacen **hard delete**; Regla #5 sugiere soft-delete + `confirm_text` para tablas críticas. Definir si `tk_tickets` usa soft-delete (recomendado) y quién puede borrar.
13. **Vínculo ticket↔OT:** hoy es 1:1 (`visita_id`), pero el negocio a veces necesita 1↔N OTs. ¿Mantener 1:1 (como el esquema real) o modelar 1:N con tabla puente?