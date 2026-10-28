# Tickets 2.0 — Levantamiento (27-oct-2026)

Solo lectura: no se cambió código. Meta de Daniel: tickets que nacen por el formulario, sin la carga heredada de Triple A, y que la bandeja siga rápida con 50.000 a 100.000 tickets (hoy hay consultas de 5-10 minutos, una duró un día).

## 1. Por qué se vuelve lenta (causas, en orden de peso)

### 1.1 El orden por defecto de la bandeja recorre todo (`tickets_module.py` ~374, `_TK_ORDER_DEFAULT`)
`ORDER BY (SELECT COUNT(*) FROM tk_mensajes ... ) > 0 DESC, FIELD(estado...), FIELD(prioridad...), updated_at DESC`.
- Es una subconsulta correlacionada **por cada ticket**, evaluada para **todos** los tickets que calzan el filtro **antes** del `LIMIT`. Más `FIELD()`, que no puede usar índice.
- MySQL ordena la tabla completa en disco cada vez que alguien abre la bandeja. Crece con tickets × mensajes: es la causa más probable de los minutos.

### 1.2 Cada carga de la bandeja hace 3 consultas pesadas más (`tk_api_list`, ~6381)
- `COUNT(*)` del total + un segundo barrido para los KPI (activos, urgentes, vencidos) sobre el mismo filtro.
- `unread_count` como subconsulta por fila (esta sí es solo de la página, ok).
- Y el navegador lo repite **cada 10 s** por cada persona con la bandeja abierta (`tickets_list.js` ~384), más `/unread-summary` en cada recarga.

### 1.3 `/tickets/api/unread-summary` (~8055)
`COUNT(DISTINCT t.id)` con JOIN de tickets × mensajes sobre **toda** la historia, sin filtrar por estado ni fecha. Se ejecuta cada 10 s por usuario.

### 1.4 Filtros que no usan índice (`_tk_list_where`, ~414)
- Búsqueda `q`: `LIKE '%texto%'` en 6 columnas, incluida `descripcion` (TEXT). Recorre todo.
- Fechas: `DATE(CONVERT_TZ(created_at, ...))`: envuelve la columna en funciones y anula `idx_created`.
- `rut LIKE '%...%'` anula `idx_rut`.

### 1.5 La ficha (`tk_api_get`, ~6757)
- `SELECT t.*` más **todo el hilo** de mensajes (`contenido` MEDIUMTEXT, con HTML) en cada apertura; sin paginar el hilo.
- **Escribe** en `tk_vistas` en cada lectura.
- Dispara el barrido de correo (ver 1.6).

### 1.6 Barrido IMAP del buzón (herencia de Triple A)
- `tk_api_list` **y** `tk_api_get` llaman `_tk_autopoll_correo()`: con la app abierta, cada 8 s se puede abrir un login IMAP a Gmail, buscar `SINCE 60 días` con `OR SUBJECT "TK-" "TAA-" "Ticket - ID:"`, y bajar las cabeceras de hasta N correos, una por una.
- Es trabajo de red lento y sin límite real ligado al tamaño de la bandeja; además corre en hilos del mismo proceso que atiende a la gente.
- Incluye código solo para Triple A: `TAA-`, `Ticket - ID:`, auto-creación de tickets `sin_ticket_taa`, importación CSV (`tk_import_taa`), recuperación histórica, restauración de correos de prueba.

### 1.7 Esquema
- `tk_tickets` tiene ~45 columnas anchas (TEXT: `descripcion`, `producto`, `numero_documento`, `notas_internas`) y 10 índices simples. Faltan índices **compuestos** para los patrones reales: `(estado, prioridad, updated_at)`, `(asignado_a, estado)`, `(origen, created_at)`.
- No hay una columna "tiene mensajes sin leer" en el ticket: por eso hay que contar mensajes en cada consulta.
- `tk_mensajes.contenido` es MEDIUMTEXT con HTML completo (incluye citas anteriores de las respuestas): es el candidato principal a "pesar" en la base.

## 2. Propuesta Tickets 2.0

**A. Entrada formal por el formulario**
- El formulario público / el de backoffice son la única vía de creación. Se retira la auto-creación desde correos de Triple A y el barrido `Ticket - ID:`/`TAA-`.
- Las respuestas de clientes: decisión pendiente (ver preguntas).

**B. Consultas que escalan (sin cambiar lo que ve el usuario)**
1. Columna denormalizada `tk_tickets.sin_leer` (contador) mantenida al insertar/leer mensajes; así el orden "con mensajes sin leer arriba" usa índice y no cuenta nada.
2. Orden por defecto por columna de rango calculada (`orden_estado`, `orden_prioridad`) con índice compuesto, en vez de `FIELD()`.
3. Paginación por cursor (`updated_at, id`) en vez de `OFFSET` profundo.
4. KPI y total de la bandeja desde una tabla de contadores o con `COUNT` indexado, **cacheados 30-60 s**; no por cada refresco.
5. Filtros de fecha como rango sobre `created_at` (convertido a UTC una vez, en Python), no `CONVERT_TZ` por fila.
6. Búsqueda: `FULLTEXT` en `numero_ticket, empresa, nombre_contacto, titulo` (+ `descripcion`), y prefijo para RUT.
7. Ficha: mensajes paginados (últimos N + "ver anteriores"), sin `t.*`, y la vista se registra sin bloquear la lectura.

**C. Mensajes nuevos sin ir a buscarlos**
- En vez de recargar toda la bandeja cada 10 s: un endpoint liviano `/tickets/api/novedades?desde=<marca>` (una consulta indexada, devuelve solo cambios) cada 5 s con la pestaña visible, y en la ficha solo los mensajes nuevos desde el último id. Sin barrer correo en cada petición.

**D. Base de datos más liviana**
- Medir primero (solo lectura): tamaño por tabla, mensajes más pesados, citas repetidas, adjuntos en BD.
- Archivar tickets cerrados con más de X meses a tablas de histórico (siguen consultables) y recortar citas repetidas en `contenido`.
- Todo con el patrón de la REGLA #18: migraciones baratas, una cláusula por sentencia, nada que bloquee el arranque. Cloud SQL `ilus-db` es intocable a nivel de instancia (REGLA #17): solo `ALTER`/índices por la app, sin tocar la instancia.

**E. Prueba de carga**
- Base local o de mentira con 100.000 tickets y 1 millón de mensajes; medir tiempos de lista, filtros, ficha y novedades antes y después. Nada contra producción (REGLA #22).

## 3. Orden de trabajo sugerido
1. Medir (solo lectura) lo que pesa hoy en producción.
2. Índices + contadores (B1-B2) y cache de KPI: el mayor alivio con menor riesgo.
3. Sacar el barrido IMAP de la bandeja y la ficha (queda como cron aparte o se retira, según decisión).
4. Novedades en vivo (C) y paginación de ficha.
5. Archivo/limpieza de la base (D) con el respaldo ya activo.

---

## 4. Aclaración de Daniel (27-oct) — la ticketera y el MOTOR ÚNICO

**Visión:** Tickets es el gestor central de la empresa (formulario, OT, solicitud de repuestos, etc.) que se pueda ver y editar a gusto. Se mantiene lo que ya gusta: bandeja tipo WhatsApp (el ticket con actividad sube arriba) y conversación con burbujas.

**Se quitan** (decisión de Daniel): el lector de correo de Triple A (`TAA-`, `Ticket - ID:`, auto-creación `sin_ticket_taa`, importación CSV, recuperación histórica), el barrido del buzón disparado al abrir la bandeja/ficha (`_tk_autopoll_correo`) y los lectores separados de Entrantes / Enviados.

**Entra un motor único de ingreso** (un solo punto de escritura, `ingresar_evento_ticket`) con 2 canales de entrada:
1. **Formulario** (`/soporte/api/crear` y el de backoffice): crea el ticket.
2. **Correo con el código del ticket en el asunto** (`TK-AAAA-NNNNN`): se agrega a la conversación de ese ticket como burbuja del cliente. Sin código reconocible no crea ticket: se ignora o va a una bandeja "sin clasificar" (a decidir).

El mismo motor es el que usan hoy los otros módulos para dejar mensajes (hay **30 `INSERT INTO tk_mensajes` en `app.py`** + 4 en `tickets_module.py`): pasar por una sola función permite mantener el contador de "sin leer", `ultima_actividad_at` y los avisos en un único lugar, y que todo quede en tiempo real.

**Diseño del correo "casi en tiempo real" y barato:**
- Lectura **incremental por UID** (solo correos nuevos desde el último UID guardado), nunca `SINCE 60 días` ni bajar cabeceras de todo.
- La dispara **Cloud Scheduler cada 1 minuto** (mismo patrón que `simpliroute-poll`), no la navegación de las personas; botón "Actualizar ahora" para quien no quiera esperar. Dedup por Message-ID (ya existe `tk_mail_ingeridos`).
- Latencia del correo: hasta ~1 min. Si se quiere de segundos: Gmail API con push (Pub/Sub), requiere configuración en Google Cloud (fase aparte).

**Tiempo real en pantalla:** endpoint `novedades?desde=<marca>` (una consulta indexada) cada 5 s con la pestaña visible; la bandeja reordena y marca burbujas, la ficha agrega solo los mensajes nuevos.

**Fases:**
1. Medir (solo lectura) + índices compuestos + `ultima_actividad_at` y `sin_leer` en `tk_tickets` + orden de bandeja por esas columnas. KPI en caché.
2. Motor único de ingreso (función central) y migrar los 34 puntos de escritura a ella.
3. Correo incremental por UID + Cloud Scheduler; retirar autopoll y lectores de Triple A (con respaldo y sin borrar datos históricos).
4. Novedades cada 5 s, paginación de la ficha.
5. Editor de la ticketera (campos, tipos, estados, formulario editable) — a diseñar con Daniel.
6. Prueba de tráfico: 100.000 tickets / 1 millón de mensajes en base de mentira, con tiempos antes y después.

**Cuidado (REGLAS #17, #18, #22):** nada se borra de la base ni se toca la instancia; los `ALTER` van de a una cláusula y baratos; las pruebas con correo y datos de mentira, nunca contra clientes reales. Retirar los lectores pasa por el "sí" ya dado por Daniel en este mensaje (REGLA #4.2).

---

## 5. Auditoría Impeccable (solo lectura, REGLA #27) — bandeja y ficha de Tickets

Alcance: `templates/tickets/list.html`, `ficha.html`, `static/tickets_list.{css,js}`, `static/tickets_ficha.{css,js}`. No se modificó nada. Detector mecánico + conteos de código; **no** se revisó en navegador real (falta captura a 390 px).

| # | Dimensión | Nota | Hallazgo principal |
|---|---|---|---|
| 1 | Accesibilidad | 2/4 | `tickets_list.js` no tiene un solo `aria-*` y arma 29 `innerHTML` + 9 `onclick=` inline; ~144 tamaños de fuente < 12 px; `:focus-visible` solo 2 veces en la bandeja |
| 2 | Rendimiento | 2/4 | La bandeja se repinta entera (`innerHTML`) cada 10 s, pierde foco/selección y hace trabajo de DOM aunque no cambie nada; la ficha tiene 120 `innerHTML` y carga el hilo completo |
| 3 | Tema / tokens | 2/4 | 792 colores `#hex` fijos contra 92 `var(--…)`; sin modo oscuro (`prefers-color-scheme` = 0) — aceptable hoy, la paleta es la de ILUS (REGLA #2) |
| 4 | Responsive | 3/4 | Hay 27 breakpoints ≤ 768 px y 18 objetivos de 44 px; falta verificar en 390 px real |
| 5 | Integridad | 3/4 | Coherente con el lenguaje de Retiros; detector: 5 `side-tab` (borde lateral de 3-4 px), 1 `border-accent-on-rounded`, 5 easings con rebote (`--ease-spring`) |

Problemas en orden de prioridad (ninguno se corrige sin tu "sí", REGLA #4.2):
1. **Repintado total cada 10 s** en la bandeja → se resuelve con las "novedades" (fase 4): repintar solo filas que cambiaron.
2. **Sin semántica ni etiquetas accesibles** en la bandeja (botones de icono sin `aria-label`, orden de columnas sin `aria-sort`, burbujas sin rol).
3. **Texto muy chico** (< 12 px, 144 usos): la REGLA #15 pide datos legibles.
4. **`prefers-reduced-motion` ausente en la bandeja** (la ficha lo cubre en 3 lugares).
5. **Colores fijos** repetidos: extraer tokens sin cambiar la paleta.
6. **Detector (estético, bajo):** bordes laterales y easing de rebote en la ficha (`tickets_ficha.css` líneas 9, 63, 81, 138, 348, 481, 623, 637, 794, 841, 859).
7. Nota: hay una versión nueva de Impeccable (4.8.0, hoy 4.5.1); no se actualizó.
