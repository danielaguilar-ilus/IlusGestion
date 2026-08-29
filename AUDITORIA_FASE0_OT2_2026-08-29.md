# AUDITORÍA FASE 0 — OT 2.0 ILUS Fitness
## Informe consolidado (8 dominios + verificación adversarial) — Master Task Register y decisiones para Daniel

**Alcance:** solo lectura de código (`app.py` 111.786 líneas / 5,68 MB, `tickets_module.py`, `templates/ot2/*`, `templates/mantenciones/*`, `static/*.js`). Sin escritura, sin migraciones, sin llamadas a endpoints de escritura, sin pruebas de carga ejecutadas contra producción — conforme al mandato de Fase 0. Cada hallazgo P0/P1 de los 8 dominios pasó por una segunda lectura adversarial independiente antes de entrar a este documento; solo lo que sobrevivió esa verificación aparece como hallazgo activo abajo.

**Nota de calidad de evidencia:** las citas de línea de los 8 informes de dominio tienen un desfase sistemático de 5 a 150 líneas respecto al `app.py` actual (el archivo sigue creciendo el mismo día de la auditoría). Ningún desfase cambió una función o comportamiento citado — se verificó por nombre de símbolo, no por número. Los números de línea en este documento son los reconfirmados en la verificación, no los del primer borrador.

---

## 1. RESUMEN EJECUTIVO (máximo 10 conclusiones, P0/P1 primero)

1. **No existe un servicio único de creación de OT.** Hay 3 motores paralelos (`ot2_api_crear`, `_mant_lev_crear_ot_core`, `_mant_visita_crear_core`) más 9 `INSERT INTO mant_visitas` sueltos en jobs/crons. Cualquier regla de negocio nueva debe replicarse 3+ veces o diverge. *(OT2-P0-01)*

2. **Tres familias de cierre coexisten y al menos dos evaden los candados financieros que Daniel pidió endurecer el 26/27-ago-2026.** La legacy `/firmar`+`/cerrar` y la de "OT espejo" de levantamiento (`mant_lev_cerrar`) pueden llevar una OT a `estado='cerrada'` sin factura, sin centro de costo y sin las 3 firmas en el orden correcto — mientras la canónica (`aprobar-cierre`) sí lo exige. Ambas rutas legacy están **vivas y alcanzables** desde un botón real, no son código muerto. *(OT2-P0-02)*

3. **Un técnico puede crear una OT completa de cliente, con datos financieros propios, sin ningún chequeo de rol** — pese a que la matriz de permisos documenta "crear OT: técnico ✗". La función que debería impedirlo (`_puede_crear_ot()`) ni siquiera existe en el código; solo se referencia en un comentario. *(OT2-P0-03)*

4. **`/f/<key>` sirve cualquier foto de evidencia o firma legal (cliente/técnico/supervisor) sin sesión ni verificación de pertenencia**, con claves construidas de forma predecible (`vid` secuencial + timestamp en segundos), no aleatorias. Es la ruta más sensible del sistema y no tiene ningún control de acceso. *(OT2-P0-04)*

5. **Bypass financiero real, no hipotético:** `ot2_api_finanzas` y `ot2_api_crear` graban `factura_tido`/`factura_nudo` directo del body sin consultar el ERP, mientras existe un tercer endpoint (`asociar-factura`) que sí verifica RUT contra Random. El gate de cierre `SIN_FACTURA` solo exige que el campo no esté vacío — acepta un documento inventado. *(OT2-P0-05)* — el vector "cualquier rol puede hacerlo" del informe externo queda **REFUTADO** (el gate de rol ya se corrigió el 26-ago), pero el vector "sin verificación ERP" se **CONFIRMA**.

6. **`PUT /mantenciones/api/visitas/<vid>` acepta `estado` como campo libre**, incluido `'cerrada'`, sin ejecutar ninguna de las reglas de cierre (checklist, firmas, factura). Cualquier admin/supervisor/ejecutivo puede cerrar una OT saltándose todo el flujo. *(OT2-P0-06)*

7. **Sin idempotencia real contra doble-creación:** el único candado (`_next_ot_number_atomic`) evita colisión de número de OT, no la duplicación de la fila completa. Un doble-click, reintento de red o dos pestañas generan dos OT válidas — una queda huérfana de ticket porque el vínculo se intenta **después** del commit y falla en silencio. *(OT2-P0-07)*

8. **El choque de horario de técnico nunca se valida en el backend de creación ni de reasignación** — la función que lo detecta existe pero solo la usa un endpoint GET informativo; el flujo "confirmar de todos modos" en el formulario es código muerto que nunca llega al servidor. Dos administradores pueden agendar al mismo técnico dos veces sin ningún aviso server-side. *(OT2-P0-08)*

9. **Guardar una plantilla de mantención dispara una resincronización masiva (hasta 500 grupos equipo×plantilla) cuyo candado "sin respuestas" es una consulta separada de la escritura** — un técnico que complete una tarea en esa ventana pierde su respuesta en silencio, sin log de la pérdida. Es una acción administrativa común (editar una plantilla compartida), no un caso exótico. *(OT2-P0-09)*

10. **`fecha_fin` y contraparte se pierden al crear una OT desde el flujo vivo hoy** (`_modal_crear.html`/`ot2_api_crear`), confirmando el síntoma reportado externamente — pero la causa real no es el archivo que citaba el informe externo (`ot2_form.js`, hoy código muerto), sino que el formulario realmente en uso nunca pide esos campos, aunque las columnas existen y el motor legado sí las escribe. *(OT2-P0-10)*

**Hallazgos P1 más relevantes que no llegan al Top 10 pero exigen acción antes de escalar:** `ot_sellada` no cubre `cancelada`/`anulada` (una OT cancelada sigue firmable/ejecutable); `firmar-revision` no valida estado de origen (se puede "completar sin iniciar"); no existe columna de estado de pago (factura ≠ pago, la Dimensión C del modelo financiero no existe); 5 criterios distintos de "atrasada"/SLA en 5 pantallas; cobertura de pruebas automatizadas del dominio crítico (firma/cierre) es prácticamente nula.

---

## 2. MAPA ACTUAL — productores, servicios, tablas, consumidores, flujos

### Productores de OT (`INSERT INTO mant_visitas`)

| Motor | Función | Invocado por | Reglas de rol en creación |
|---|---|---|---|
| Canónico "nuevo" | `ot2_api_crear` (`app.py`, ruta `POST /ot/api/crear`) | `templates/ot2/_modal_crear.html` (`OT2C.crear()`) — único flujo vivo del panel OT2.0 | Solo valida rol para "trabajo interno" (`_puede_crear_ot_interna()`); OT de cliente sin gate de rol |
| Legado de levantamiento | `_mant_lev_crear_ot_core` → `_ot_crear_visita_espejo` | `POST /mantenciones/api/clientes/<cid>/levantamientos` (usado en vivo por `static/tickets_ficha.js`) y `tk_api_generar_ot` (botón "Generar OT" de Tickets) | Mismo patrón débil, pero sí valida `visita_id IS NULL` antes de crear (409 si el ticket ya tiene OT) |
| Legado de ficha de cliente | `_mant_visita_crear_core` | `POST /mantenciones/api/visitas`, usado en vivo por `static/mant_ficha.js` (modal "Nueva OT" clásico) | Igual — solo gate para trabajo interno |
| Escrituras sueltas (9 sitios) | crons y acciones puntuales: `_mantenciones_cron_run_once`, `mant_generar_calendario`, `mant_visita_multi`, `mant_maquina_solicitar_cambio`, `mant_visita_historica`, `mant_visita_retroactiva`, `mant_contrato_auto_calendar`, `mant_intel_accion`, `mant_planificador_generar_ots` | Jobs de fondo / acciones administrativas específicas | Sin pasar por ninguno de los 3 núcleos |

### Servicios de ejecución/firma/cierre (múltiples familias, ver §6)

- **Canónica:** `firmar-revision` → `firmar-cliente` → `aprobar-cierre` / `rechazar-cierre`, más `liberar-firma-tecnico`.
- **Legacy genérica:** `mant_visita_firmar` (`/firmar`, 3 tipos) + `mant_visita_cerrar` (`/cerrar`) — vivas, alcanzables desde `templates/mantenciones/ot_ficha.html?legacy=1`, accesible desde un botón real de `ot_ejecutar.html`.
- **Tercera vía — "OT espejo" de levantamiento:** `mant_lev_cerrar` (`/mantenciones/api/levantamientos/<lid>/cerrar`) — sin consumidor de frontend encontrado, pero endpoint montado y sin ningún gate financiero.
- **Firma pública remota:** `/firmar-ot/<token>` (HMAC atado a `vid`, TTL 120h, sin estado en BD) → `/ot-firmada/<token>` (PDF público).

### Tablas centrales

`mant_visitas` (tabla "OT", ENUM `estado` con 14 valores, varios nunca escritos por el flujo vivo), `mant_visita_tareas` (checklist), `mant_levantamientos`, `mant_logs` (auditoría en texto libre, no estructurada), `mant_visita_tecnicos`, `mant_visita_fotos`, `mant_ot_signatures`, `tk_tickets` (vínculo `visita_id`), `mant_tecnicos` (tabla "zombie", desconectada de la asignación real vía `app_users`), `mant_plantillas`.

### Consumidores externos de la OT

Tickets (`tk_api_generar_ot`), ficha del cliente (`mant_ficha.js`), Monitor TV (`/tv/<token>`, solo lectura vía `_OT_TV_SELECT` — sin fuga financiera confirmada), firma pública, PDF público, Analytics/dashboard de mantenciones.

### Flujo real hoy (creación → ejecución → cierre)

`_modal_crear.html` → `ot2_api_crear` → redirige a `mant_ot_ficha` → **redirige internamente a `mant_ot_ejecutar`** (vista de ejecución legacy, no a `ot2_detalle`, que es la vista de mando que usa el propio panel para listar) → ejecución de tareas vía `mantenciones_ot_ejecutar.js` → firma técnico/cliente vía endpoints canónicos (mayoritariamente) → `aprobar-cierre`. Puntos de fuga: cualquier paso puede desviarse a la familia legacy vía `?legacy=1`.

---

## 3. MATRIZ OT NORMAL vs OT 2.0 — conservar / corregir / migrar / retirar

| Componente | Estado hoy | Decisión propuesta |
|---|---|---|
| `ot2_api_crear` (creación) | Vivo, con brechas de rol/idempotencia/campos | **CORREGIR** — es el candidato correcto a motor único, pero necesita los fixes P0-03/07/08/10 |
| `_mant_lev_crear_ot_core` / Tickets→OT | Vivo, con chequeo de idempotencia mejor que el "nuevo" | **MIGRAR** su candado de idempotencia hacia `ot2_api_crear`; luego **RETIRAR** como motor independiente |
| `_mant_visita_crear_core` / ficha cliente | Vivo | **MIGRAR** a wrapper del motor único; **RETIRAR** el INSERT propio |
| `/firmar` + `/cerrar` (legacy genérico) | Vivo, alcanzable, evade candados financieros | **RETIRAR** — pero antes hay que mover "Configurar OT (plantilla+equipos)" fuera de `ot_ficha.html` a un modal ligero (es la única función legítima que hoy vive en esa pantalla) |
| `mant_lev_cerrar` (OT espejo) | Vivo, sin consumidor de UI encontrado | **CONFIRMAR con Daniel si tiene dueño** (integración/script externo); si no, **RETIRAR** de inmediato (bajo riesgo) |
| Firma canónica (`firmar-revision`/`firmar-cliente`/`aprobar-cierre`) | Vivo, mejor blindado (fixes 26/27-ago) | **CONSERVAR y CORREGIR** los huecos residuales (§6) — es la base correcta |
| `ot2_form.js` + `_modal_ot_form.html` | **Código muerto** (confirmado por grep: ninguna llamada real, solo comentarios) | **RETIRAR** su inclusión de `panel.html` — hoy es peso muerto y trampa de auditoría (ya indujo un error en el informe externo) |
| `ot_ejecutar.html` (vista de ejecución) | Vivo, en uso real, con regresión de peso (+65% en 1 mes) | **CONSERVAR** (es intencional según su propio docstring, no "legacy abandonado") — **CORREGIR** performance |
| Monitor TV | Vivo, bien construido, sin fuga financiera | **CONSERVAR** |
| `mant_tecnicos` (tabla) | "Zombie" — 8 lecturas, 0 escrituras según el propio código | **RETIRAR** de los cálculos de capacidad; reemplazar por `app_users.role` |
| Tests `tests/test_permisos_equipos_ot.py` | Único test del dominio, no cubre firma/cierre | **CORREGIR/AMPLIAR** — es la base de la red de seguridad que falta |

---

## 4. HALLAZGOS PRIORIZADOS (Master Task Register — ver también §14)

Formato exigido: `ID | Área | Severidad | Problema | Evidencia | Escenario | Impacto operacional | Impacto técnico | Clasificación | Solución | Complejidad`.

### P0

**OT2-P0-01 | Arquitectura | P0**
Problema: 3 motores de creación de OT paralelos sin servicio canónico único.
Evidencia: `app.py:ot2_api_crear`, `app.py:_mant_lev_crear_ot_core`→`_ot_crear_visita_espejo`, `app.py:_mant_visita_crear_core`; llamadores en `app.py:mant_lev_crear_o_listar`, `app.py:mant_visita_crear`, `static/mant_ficha.js`, `static/tickets_ficha.js`.
Escenario: un supervisor crea desde la ficha del cliente, otro desde "Generar OT" de un Ticket, un tercero desde el panel OT2.0 — tres caminos de código distintos escriben en la misma tabla.
Impacto operacional: una regla nueva debe replicarse 3+ veces o queda inconsistente según el origen.
Impacto técnico: alto riesgo de divergencia; viola el criterio 1-2 de independencia del proyecto.
Clasificación: CONFIRMADO ACTUAL.
Solución: consolidar todo `INSERT` de `mant_visitas` detrás de un único núcleo; los otros 2 pasan a ser wrappers de compatibilidad.
Complejidad: Alta.

**OT2-P0-02 | Firmas/Cierre | P0**
Problema: tres familias de cierre coexisten; dos evaden los candados financieros `SIN_FACTURA`/`SIN_CENTRO_COSTO` endurecidos el 27-ago.
Evidencia: `app.py:_ot_validar_cierre` (R1-R4, nunca revisa factura/centro de costo) usada por `app.py:mant_visita_cerrar` (`/cerrar`); `app.py:mant_visita_firmar` (`/firmar`, 3 tipos, sin condición de estado); `app.py:mant_lev_cerrar` (`UPDATE ... SET estado='cerrada' WHERE id=%s`, sin ninguna condición financiera); contraste con `app.py:mant_ot_aprobar_cierre` (gates `SIN_FACTURA`/`SIN_CENTRO_COSTO`, fechado 27-ago). Alcance UI confirmado: `templates/mantenciones/ot_ficha.html` (botones "Firmar como supervisor" y "Cerrar OT"), accesible desde `templates/mantenciones/ot_ejecutar.html` (botón "Configurar OT") vía `?legacy=1`.
Escenario: técnico captura ambas firmas vía `/firmar` legacy; el creador firma como supervisor; se llama `/cerrar` — la OT queda `cerrada` sin factura ni centro de costo, sin revisión de Aarón.
Impacto operacional: OT cobrable cerrada sin documento tributario ni imputación contable — justo lo que la regla del 27-ago quiso impedir.
Impacto técnico: 3 máquinas de estado independientes escriben las mismas columnas con invariantes distintas.
Clasificación: CONFIRMADO ACTUAL.
Solución: mover los gates financieros a `_ot_validar_cierre` (fuente única) o forzar todo cierre por `aprobar-cierre`; confirmar con Daniel el destino de `mant_lev_cerrar`.
Complejidad: Media.

**OT2-P0-03 | Permisos/Creación | P0**
Problema: un técnico puede crear una OT de cliente completa, con garantía/costo declarados, sin ningún chequeo de rol.
Evidencia: `app.py:_mant_visita_crear_core` y `app.py:ot2_api_crear` — el único gate de rol (`_puede_crear_ot_interna()`) solo corre para "trabajo interno"; `_puede_crear_ot()` (referenciada en el comentario de `_puede_ot_accion`, rama "crear") **no existe** en el repo; `_legacy_permission_set('tecnico')` da `mantenciones: True` sin restricción adicional.
Escenario: técnico llama `POST /mantenciones/api/visitas` o `POST /ot/api/crear` con `cliente_id`, `garantia_aplica:true`, `tecnico_user_id` propio.
Impacto operacional: técnico fabrica OT para clientes ajenos, se autoasigna trabajo, declara garantía sin revisión de gestión.
Impacto técnico: rompe la segregación de funciones documentada en la propia matriz de permisos.
Clasificación: CONFIRMADO ACTUAL.
Solución: agregar `if _es_rol_tecnico() and not <permiso "crear OT normal">: return 403` en ambos endpoints.
Complejidad: Baja.

**OT2-P0-04 | Seguridad/Evidencia | P0**
Problema: `/f/<key>` sirve cualquier foto/firma de GCS sin login ni verificación de pertenencia; claves predecibles (`vid` secuencial + timestamp en segundos).
Evidencia: `app.py:serve_archivo` (`@app.route("/f/<path:key>")`, sin decorador); claves en `app.py:_subir_firma_storage` (`firma_{tipo}_{vid}_{timestamp}`) y `_mant_visita_fotos_subir_impl`.
Escenario: un actor externo sin sesión itera `vid` + rango de timestamp del día laboral y descarga firmas/fotos de OT ajenas.
Impacto operacional: filtración de firmas con valor legal y evidencia fotográfica de domicilios de clientes.
Impacto técnico: cero control de acceso en la ruta más sensible del sistema.
Clasificación: CONFIRMADO ACTUAL.
Solución: exigir sesión + verificación de pertenencia, o URLs firmadas de GCS con expiración corta; migrar generación de `public_id` a alta entropía.
Complejidad: Media.

**OT2-P0-05 | Finanzas | P0**
Problema: `factura_tido`/`factura_nudo` se graban sin verificación ERP en 2 endpoints, mientras un tercero sí verifica RUT.
Evidencia: `app.py:ot2_api_finanzas` y `app.py:ot2_api_crear` (toman el valor directo del body); contraste `app.py:mant_ot_asociar_factura` (sí llama `_erp_doc_lookup` + compara RUT); gate `SIN_FACTURA` en `mant_ot_aprobar_cierre` solo exige campo no vacío.
Escenario: admin/supervisor/ejecutivo (rol autorizado para "cobertura" — el vector "cualquier técnico" ya está cerrado desde el 26-ago) declara un documento inventado o de otro cliente.
Impacto operacional: OT cerrada "facturada" sin factura real, saltando el control anti-fraude que el propio proyecto construyó.
Impacto técnico: 2 escritores de las mismas columnas "selladas" con niveles de verificación opuestos.
Clasificación: CONFIRMADO ACTUAL (vector ERP) / REFUTADO (vector "cualquier técnico", ya corregido 26-ago).
Solución: exigir `_erp_doc_lookup` + comparación RUT en ambos endpoints, o remover el campo libre y forzar siempre `asociar-factura`.
Complejidad: Baja-Media.

**OT2-P0-06 | Máquina de estados | P0**
Problema: el `PUT` genérico de metadata permite escribir `estado` (incluido `'cerrada'`) sin ejecutar ninguna regla de cierre.
Evidencia: `app.py:mant_visita_update` — `"estado"` está en la whitelist `allowed`, el `UPDATE` dinámico lo escribe tal cual; el único gate es `_puede_ot_accion(vid,'metadata')` (rol + no-sellada), que no mira el valor destino.
Escenario: admin/supervisor/ejecutivo hace `PUT {"estado":"cerrada"}` sobre una OT `programada`/`en_curso`.
Impacto operacional: cierre sin checklist, sin firmas, sin factura, sin centro de costo.
Impacto técnico: rompe la invariante central del proyecto.
Clasificación: CONFIRMADO ACTUAL.
Solución: sacar `"estado"` del whitelist (o interceptarlo y redirigir a los endpoints canónicos para valores sensibles).
Complejidad: Baja.

**OT2-P0-07 | Concurrencia/Creación | P0**
Problema: sin idempotencia server-side contra doble-click, reintento de red o doble pestaña.
Evidencia: `app.py:ot2_api_crear` — el único candado (`_next_ot_number_atomic`) protege el número, no la fila; el vínculo con el ticket (`UPDATE tk_tickets ... WHERE visita_id IS NULL`) corre **después** del commit y su fallo no revierte nada. Única protección: `disabled=true` del botón en el frontend.
Escenario: reintento automático del navegador por timeout, o dos administradores creando la OT del mismo ticket desde dos pestañas.
Impacto operacional: dos OT completas para el mismo trabajo, doble notificación al técnico, una queda huérfana de ticket.
Impacto técnico: duplicación real de fila con dos correlativos válidos.
Clasificación: CONFIRMADO ACTUAL.
Solución: `client_request_id` con `UNIQUE KEY`, devolver la OT existente si se repite en ventana corta.
Complejidad: Media.

**OT2-P0-08 | Calendario/Asignación | P0**
Problema: el choque de horario de técnico nunca se valida en backend en ningún punto del ciclo de vida (crear, reagendar, reasignar).
Evidencia: `app.py:_validar_disponibilidad_visita` (docstring: "Nunca bloquea, solo detecta"); ni `ot2_api_crear` ni `mant_visita_update` la invocan; único consumidor real es el GET informativo `mant_calendario_choque`; el flujo "confirmar de todos modos" en `ot2_form.js` es código muerto (payload real `_o2fPayload` nunca incluye `forzar_choque`).
Escenario: dos administradores agendan al mismo técnico en el mismo horario.
Impacto operacional: doble-agenda real sin ningún candado — se descubre en terreno.
Impacto técnico: función bien escrita pero no cableada donde importa.
Clasificación: CONFIRMADO ACTUAL.
Solución: invocar la validación dentro de la transacción de creación/reasignación, con `forzar_choque` auditado y motivo obligatorio.
Complejidad: Media.

**OT2-P0-09 | Concurrencia/Plantillas | P0**
Problema: guardar una plantilla dispara una resincronización masiva (hasta 500 grupos, de cualquier OT/técnico) cuyo candado "sin respuestas" es una consulta separada de la escritura.
Evidencia: `app.py:mant_plantilla_actualizar` (bucle) → `app.py:_grupo_ot_sin_editar` (SELECT aislado) → `app.py:_sincronizar_grupo_desde_plantilla` (DELETE+INSERT); mismo patrón en `mant_ot_equipo_cambiar_plantilla` para un solo equipo.
Escenario: Daniel edita y guarda una plantilla usada en decenas de OT activas justo cuando un técnico en terreno completa una tarea de esa plantilla.
Impacto operacional: la respuesta/evidencia recién guardada desaparece sin error visible, puede volver a bloquear el cierre por "checklist incompleto".
Impacto técnico: `DELETE` real de filas con `completada=1` sin ningún log de la pérdida.
Clasificación: CONFIRMADO ACTUAL (mecanismo verificado; requiere colisión de timing para manifestarse, pero es un evento administrativo común y sin advertencia).
Solución: unificar check-and-delete en una sola transacción o adquirir `SELECT ... FOR UPDATE` sobre el grupo antes de decidir.
Complejidad: Media-Alta.

**OT2-P0-10 | Creación/Datos | P0**
Problema: `fecha_fin` y contraparte (`contacto_*`) se pierden al crear desde el flujo vivo hoy.
Evidencia: `app.py:ot2_api_crear` (INSERT sin esas columnas) vs columnas existentes en el esquema (`fecha_fin`, `contacto_nombre/cargo/tel/email/origen`) vs `app.py:_ot_crear_visita_espejo` (motor legado, sí las escribe); el propio comentario de `templates/ot2/_modal_crear.html` documenta que el formulario no ofrece el rango multi-día "hasta que OT2 soporte visitas multi-día".
Escenario: instalación multi-día o cualquier OT que necesite dejar contraparte registrada, creada desde el panel OT2.0.
Impacto operacional: instalaciones multi-día nacen como de un día; WhatsApp/correo/firma remota quedan sin destinatario declarado.
Impacto técnico: regresión de capacidad del motor "nuevo" respecto al "viejo".
Clasificación: CONFIRMADO ACTUAL (corrige la evidencia del informe externo, que citaba un archivo muerto).
Solución: agregar los campos al formulario y al INSERT — columnas ya existen.
Complejidad: Media.

### P1 (resumen — detalle completo replicado en §14)

| ID | Problema | Evidencia clave | Clasificación |
|---|---|---|---|
| OT2-P1-01 | `ot_sellada` no cubre `cancelada`/`anulada` | `app.py:_puede_ot_accion`, admitido en 2 comentarios del propio código | CONFIRMADO |
| OT2-P1-02 | `firmar-revision` no valida estado origen — "completar sin iniciar" posible | `app.py:mant_ot_firmar_revision` (`WHERE ... firma_tecnico_url IS NULL`, sin `estado`) | CONFIRMADO |
| OT2-P1-03 | `/iniciar` y `/liberar-firma-tecnico`: check-then-act sin repetir condición en el UPDATE | `app.py:mant_visita_iniciar`, `mant_ot_liberar_firma_tecnico` | CONFIRMADO (riesgo de colisión INFERIDO) |
| OT2-P1-04 | `mant_ot_declarar_cobertura` usa patrón de escritura inseguro (sin rowcount/condición) | `app.py:mant_ot_declarar_cobertura` vs `ot2_api_finanzas` (ya blindado) | CONFIRMADO |
| OT2-P1-05 | `mant_visita_update` (reagendar/reasignar) sin candado de concurrencia sobre estado | `app.py:mant_visita_update` | CONFIRMADO |
| OT2-P1-06 | `modalidad_cobro='pagado'` mal etiquetado — se ve como "pago recibido" y es solo "facturable" | `app.py:ot2_api_crear`/`ot2_api_finanzas`, `templates/ot2/detalle.html` | CONFIRMADO |
| OT2-P1-07 | No existe columna/tabla de estado de pago — Dimensión "Pago" ausente del modelo | `CREATE TABLE mant_visitas`, grep global sin `monto_pagado`/`fecha_pago` | BRECHA |
| OT2-P1-08 | OT puede crearse/quedar sin técnico asignado, sin bloqueo ni KPI dedicado | `app.py:ot2_api_crear`, `mant_visita_update` | CONFIRMADO |
| OT2-P1-09 | Iniciar tarea no valida fecha/ventana programada — viola regla no negociable #5 | `app.py:mant_tarea_cronometro` | CONFIRMADO |
| OT2-P1-10 | KPI de capacidad/utilización roto — `duracion_planificada_min` nunca se escribe | `app.py:mant_analytics_data` | CONFIRMADO |
| OT2-P1-11 | 5 criterios distintos de "atrasada"/SLA en 5 lugares | `_ot2_calcular_salud`, `mant_ots_list`, Monitor TV (2 vistas), `mant_analytics_data` | CONFIRMADO |
| OT2-P1-12 | Notificación de asignación solo interna (campanita); sin correo real pese al docstring | `app.py:_notificar_ot_asignada`, `_notificar_ot_asignada_interna` | CONFIRMADO |
| OT2-P1-13 | `/ot-firmada/<token>` (pública) trae `SELECT v.*` con columnas financieras hacia el contexto de render | `app.py:_ot_pdf_context`, `ot_publica_firmada` | BRECHA de defensa en profundidad (hoy sin fuga activa: el template no las imprime) |
| OT2-P1-14 | Cobertura de pruebas automatizadas del dominio crítico (firma/cierre) es prácticamente nula | único test real: `tests/test_permisos_equipos_ot.py`, no cubre firma/cierre | CONFIRMADO |
| OT2-P1-15 | `ot2_form.js`+`_modal_ot_form.html` código muerto servido en cada carga (~3.500 líneas) | grep de `O2F.iniciar(` — 0 llamadas reales | CONFIRMADO |
| OT2-P1-16 | "Garantía"/"emergencia" no seleccionables en el formulario vivo; `prioridad` hardcodeada | `templates/ot2/_modal_crear.html`, `_OT_TIPOS_VALIDOS` | PARCIAL |

### P2 (mencionados, no bloqueantes para Fase 1)

`en_curso` vs `en_ejecucion` sin función única de escritura · KPI "próxima semana" subcuenta estados · % de avance con 3 fórmulas distintas (una con bug ya corregido en Monitor TV, no en panel) · firma remota pública sin patrón atómico · excepción de superadmin al anexo sin motivo auditado · garantía sin segregación propuesta/revisión · regresión de peso +65% en `ot_ejecutar.html` en 1 mes · monolito de 5,68 MB en un solo archivo.

---

## 5. MATRIZ POR TIPO DE OT

| Tipo | ¿Reglas propias en `ot2_api_crear`? | ¿Solo levantamiento activa descubrimiento? | ¿Seleccionable en UI viva? | Estado |
|---|---|---|---|---|
| Levantamiento | Sí — único que crea `mant_levantamientos` | Sí, confirmado (`if tipo_ot == "levantamiento"`, sin fallback a otro tipo) | Sí | OK |
| Instalación | Sí (permiso de trabajo interno, plantilla) | No contamina levantamiento | Sí | OK |
| Mantención/Visita | Sí | No contamina levantamiento | Sí | OK |
| Trabajo interno | Sí (`_cliente_opcional`, único con gate de rol real) | N/A | Sí | OK |
| Garantía | Reconvertido a atributo de cobertura, no tipo propio | N/A | Como cobertura, no como tipo | Diseño razonable, pero inconsistente con `_OT_TIPOS_VALIDOS` que sí lo trata como tipo |
| Emergencia/Urgente | Sin reflejo — `prioridad` queda fija en `'media'` en el motor legado y ni se toca en `ot2_api_crear` | N/A | **No** | BRECHA — exigido como tipo mínimo por el encargo original |
| Movimiento de equipos | Existe en `_OT_TIPOS_VALIDOS` de `app.py` pero no en la tupla local de `tickets_module.py` | N/A | Parcial | Whitelist duplicada y desincronizada entre 2 archivos |

---

## 6. MÁQUINA DE ESTADOS — actual y objetivo

**ENUM real de `mant_visitas.estado`:** `creada, programada, asignada, en_curso, en_ejecucion, firmada_tecnico, pendiente_info, pendiente_repuesto, pendiente_aprobacion, completada, cerrada, cancelada, anulada, reagendada`.

- `creada`, `asignada`, `pendiente_info`, `pendiente_repuesto`, `reagendada` existen en el ENUM pero **ningún UPDATE del flujo vivo los escribe** — reprogramar deja solo un texto libre en `mant_logs`, no hay historial estructurado.
- `en_curso` y `en_ejecucion` son sinónimos **solo donde alguien se acordó**: `/iniciar` escribe siempre `en_curso`; `/liberar-firma-tecnico` escribe siempre `en_ejecucion`. Existe una constante que los trata como sinónimos, pero solo se usa en 2 de ~15 lugares que filtran por uno u otro.

### Transiciones reales vs objetivo

| Transición | Candado real hoy | Objetivo |
|---|---|---|
| programada → en_curso/en_ejecucion | Check-then-act, sin repetir condición (OT2-P1-03) | `WHERE id=%s AND estado='programada'` + rowcount |
| en_ejecucion → firmada_tecnico | Sin validar estado de origen (OT2-P1-02) | `WHERE id=%s AND estado IN ('en_curso','en_ejecucion')` + rowcount |
| firmada_tecnico → pendiente_aprobacion (firma cliente) | **Ya blindado** (27-ago): `WHERE ... AND estado='firmada_tecnico' AND firma_cliente_url IS NULL` + rowcount | Mantener — es el patrón de referencia |
| pendiente_aprobacion → cerrada | **Ya blindado** con gates `SIN_FACTURA`/`SIN_CENTRO_COSTO` (27-ago) | Mantener; extender el mismo gate a las 2 rutas de cierre alternas (OT2-P0-02) |
| cualquier estado → cerrada (vía `PUT` genérico) | **Sin ningún candado** (OT2-P0-06) | Eliminar esta vía de escritura |
| sellada (`pendiente_aprobacion/completada/cerrada`) | No incluye `cancelada`/`anulada` (OT2-P1-01) | Ampliar `ot_sellada` a las 5 |

**Objetivo de diseño:** una sola función de transición por par (origen, destino), con `WHERE` que repita el estado de origen + `rowcount` + 409 en colisión — el patrón ya probado en `firmar-cliente`/`aprobar-cierre` debe generalizarse, no reinventarse por endpoint.

---

## 7. MATRIZ DE PERMISOS POR ACCIÓN Y ROL

| Acción | superadmin | admin | supervisor | ejecutivo | técnico (int./ext.) | Hueco detectado |
|---|---|---|---|---|---|---|
| Ver | ✓ | ✓ | ✓ | ✓ (todas) | ✓ solo si asignado/colaborador/creador | Ninguno — verificado sin IDOR horizontal |
| Ejecutar | ✓ | ✗ | ✗ | ✗ | ✓ si asignado y OT no sellada | `ot_sellada` no cubre cancelada/anulada (OT2-P1-01) |
| Configurar (plantilla/equipos) | ✓ | ✓ | ✓ | ✓ si creador | ✓ si asignado | — |
| Metadata/editar (incluye `PUT` genérico) | ✓ | ✓ | ✓ | ✓ | ✗ | Permite escribir `estado` sin reglas (OT2-P0-06) |
| Eliminar | ✓ siempre | según flag `/admin/roles` | ídem | ídem | ídem | — |
| **Crear OT (real, en el código)** | ✓ | ✓ (sin verificar) | ✓ (sin verificar) | ✓ (sin verificar) | **✓ — sin gate, contradice la matriz documentada** | OT2-P0-03 |
| Aprobar/firmar como creador | ✓ | ✓ | ✓ | ✓ | ✗ | — |
| Firmar cliente | ✓ | ✓ | ✓ | ✓ | ✓ si asignado | Solo en la ruta canónica; la legacy no verifica orden (OT2-P0-02) |
| Cobertura (garantía/factura) | ✓ | ✓ | ✓ | ✓ | ✗ nunca | Falta verificación ERP, no falta gate de rol (OT2-P0-05) |
| Leer archivo de evidencia/firma (`/f/<key>`) | — | — | — | — | — | **Sin ningún gate de sesión/rol** (OT2-P0-04) |

**Técnico externo vs interno:** `_rol_familia()` los colapsa a la misma familia sin diferenciación. Existe un incidente real ya corregido (Isabel Milling, técnica externa, vio datos financieros el 27-ago) — el fix (`{% if not es_tecnico %}`) es server-side (Jinja), no solo CSS, y sí funciona hoy en las 2 pantallas revisadas.

**Firma remota por token:** atada server-side al `vid` exacto vía HMAC — no se puede reutilizar el link de una OT para otra (hipótesis del stress-test **REFUTADA** para este vector específico).

---

## 8. MODELO FINANCIERO — cobertura, facturación y pago

| Dimensión | Existe hoy | Estado |
|---|---|---|
| **Cobertura** (`modalidad_cobro`, `cubierto_por`, `garantia_motivo`) | Sí, con endpoint dedicado (`mant_ot_declarar_cobertura`) que exige motivo ≥10 caracteres y monto >0, con log de auditoría | Existe pero **de un solo actor** — quien propone puede ser quien "aprueba" (sin segregación propuesta/revisión) |
| **Facturación** (`factura_tido`, `factura_nudo`, `estado_facturacion`) | Sí, con un camino verificado (`asociar-factura`, ERP+RUT) y dos sin verificar (`ot2_api_finanzas`, `ot2_api_crear`) | **BRECHA de verificación** — ver OT2-P0-05 |
| **Pago** (¿se cobró de verdad?) | **No existe ninguna columna** (`monto_pagado`, `fecha_pago`, `saldo`, `estado_pago` persistido) | **BRECHA TOTAL** — el único `estado_pago` del repo lee `ESPGDO` del ERP en vivo, para un buscador, y no se persiste ni alimenta `aprobar-cierre` |

**Confusión detectada (confirma Hipótesis 9 del prompt maestro):** `modalidad_cobro='pagado'` se asigna automáticamente a toda OT no-garantía/no-interna **en el momento de crear**, antes de que exista cotización, factura o cobro real. El propio comentario del código admite: *"`pagado` significa 'facturable', no 'ya pagado'"* — pero la UI lo muestra literalmente como "Modalidad: Pagado".

**Confirmaciones positivas:** exclusión garantía↔documento (activar garantía anula `factura_tido`/`factura_nudo` server-side, no solo en frontend) — correcta en ambos endpoints que la implementan. Centro de costo en creación — **ya no es un bug abierto**, corregido el 26-ago (el informe externo lo daba como pendiente; queda REFUTADO como hallazgo activo).

---

## 9. MODELO DE CONTRATO Y ANEXO

El Anexo de Servicios (proveedor) tiene ciclo completo y real, no solo de plantilla: `ot2_api_anexo_crear` (borrador) → `ot2_api_anexo_enviar` (token + correo + deep-link WhatsApp) → `ot2_anexo_firma_publica`/`ot2_anexo_firma_submit` (firma con hash servidor+cliente, IP, user-agent) → `_anexo_bloquea_ot`, que **sí bloquea en backend** (no solo en pantalla) tanto `_puede_ot_accion` (toda acción de ejecución) como `mant_visita_iniciar` (el inicio del trabajo). Es la pieza mejor construida del sistema financiero/contractual.

**Único hueco:** la excepción de superadmin al candado del anexo no exige motivo ni queda auditada como excepción explícita (`if _anexo_pend and rol != 'superadmin'` — el superadmin pasa igual, sin registro de por qué). P2, baja complejidad de corregir.

**No verificado en esta fase (fuera de alcance de los 8 dominios):** el modelo de contrato principal del cliente (los "3 Contratos ILUS" citados en memoria) y su relación estructural con el Anexo — se recomienda auditoría dedicada antes de Fase 1 si el anexo va a derivar reglas de negocio del contrato.

---

## 10. FLUJO DE TÉCNICO, CLIENTE, AARÓN Y ADMINISTRADOR

| Rol | Flujo hoy | Punto de fricción/riesgo |
|---|---|---|
| **Técnico** | Ve solo sus OT asignadas (sin IDOR horizontal verificado); inicia, ejecuta checklist, sube evidencia, firma | Puede iniciar tarea fuera de la fecha/ventana programada (OT2-P1-09); puede crear OT de cliente sin permiso (OT2-P0-03); notificación de asignación es solo campanita, sin correo (OT2-P1-12) |
| **Cliente** | Firma en pantalla presencial o remota por token/WhatsApp | Token seguro (HMAC atado a `vid`); pero la firma remota pública no usa patrón atómico (P2); puede firmar antes que el técnico si alguien usa la ruta legacy `/firmar` (OT2-P0-02) |
| **Aarón (revisor financiero)** | Vía `cobertura`/`aprobar-cierre` — rol reservado a admin/supervisor/ejecutivo/superadmin | No hay estado intermedio "garantía propuesta pendiente de su revisión" — quien declara puede ser quien aprueba (P2); puede aprobar cierre confiando en un `factura_nudo` no verificado (OT2-P0-05) |
| **Administrador/supervisor/ejecutivo** | Crea, reagenda, reasigna, cierra vía canónico o vía `PUT` genérico | `PUT` genérico permite forzar `estado='cerrada'` sin reglas (OT2-P0-06); reagendar/reasignar sin candado de concurrencia (OT2-P1-05) |

---

## 11. MODELO DE DATOS OBJETIVO Y MIGRACIONES PROPUESTAS

**Cambios de esquema necesarios (aditivos, sin romper lo existente — REGLA #4.2):**

1. Columnas de pago en `mant_visitas` o tabla nueva `mant_visita_pagos`: `estado_pago`, `monto_pagado`, `fecha_pago`, `saldo` — alimentadas manualmente con evidencia, nunca inferidas de `factura_nudo` ni `modalidad_cobro` (§8).
2. Columna `client_request_id` con `UNIQUE KEY` en `mant_visitas` para idempotencia de creación (§4, OT2-P0-07).
3. Ampliar `_OT_TIPOS_VALIDOS` para sincronizar con la tupla local de `tickets_module.py` (hoy duplicadas y desincronizadas).
4. Campo relacional para reincidencia (First Time Fix Rate) — hoy no existe ningún vínculo entre "visita que resuelve" y "visita que la originó".
5. Persistir `duracion_planificada_min` desde `ot2_api_crear` (hoy nunca se escribe, rompe el KPI de capacidad).
6. Unificar el literal de "en ejecución" a uno solo (`en_ejecucion` recomendado, es el que documenta el flujo "oficial") con migración de datos de las filas existentes en `en_curso`.

**Migraciones NO recomendadas sin decisión de negocio previa (REGLA de "preguntar, no inventar"):** cualquier cambio a `ot_sellada`, a la definición única de "atrasada"/SLA, o al criterio de qué campos quedan congelados tras el cierre — requieren que Daniel confirme la regla exacta antes de tocar código (ver §15).

---

## 12. PLAN DE MIGRACIÓN, CONCILIACIÓN Y REVERSA

**Estado actual: no hay nada que migrar todavía en Fase 0** — OT normal y OT 2.0 siguen siendo el mismo objeto (`mant_visitas`), no dos sistemas de datos separados. El "plan de migración" real es de **consolidación de código**, no de datos:

1. **Fase de congelamiento de comportamiento:** escribir pruebas de caracterización sobre las 3 familias de cierre ANTES de tocar nada (ya recomendado por el dominio de legacy/tests).
2. **Fase de unificación de escritura:** mover los 2 motores de creación secundarios a wrappers del canónico, sin quitar sus endpoints HTTP todavía (compatibilidad).
3. **Fase de retiro de rutas legacy de firma/cierre:** solo después de mover "Configurar OT" fuera de `ot_ficha.html` — es la única función legítima que hoy obliga a mantener esa pantalla viva.
4. **Reversa:** no existe hoy ningún mecanismo de reversa/reapertura de una OT cerrada (`grep` de "reabrir": cero resultados) ni un "modo solo lectura" a nivel de sistema para congelar el camino legacy sin quitar código. **Antes de retirar cualquier ruta hay que construir esto primero** — es un prerrequisito de seguridad para el propio plan de migración, no un nice-to-have.
5. **Conciliación:** no se puede cuantificar hoy cuántas OT históricas pasaron por cada una de las 3 familias de cierre sin acceso de lectura a datos de producción — **BLOQUEADO**, es el primer paso analítico antes de decidir un retiro (marcado también por el dominio de legacy).

---

## 13. PLAN DE PRUEBAS CON CRITERIOS DE ACEPTACIÓN

**Estado real de cobertura hoy:** de ~79 archivos en `tests/`, solo `tests/test_permisos_equipos_ot.py` toca el dominio OT, y no cubre creación, máquina de estados, ni ninguna de las 3 familias de firma/cierre. Es un script de análisis estático (regex + `ast.parse` parcial), no levanta Flask test client ni toca BD real — confirma presencia de decoradores, no comportamiento en ejecución. `python -m pytest tests/ -k ot` no converge en tiempo razonable (reproducido, >45s solo en colección); la causa exacta queda sin diagnosticar (la hipótesis original — "no usan `def test_*`" — es incorrecta para el 87% de la suite).

**Pruebas mínimas antes de tocar cualquier endpoint de firma/cierre (criterios de aceptación):**

| Caso | Criterio de aceptación |
|---|---|
| Doble-click en "Crear OT" | 1 sola fila en `mant_visitas`, 1 solo correlativo |
| Reintento de red tras timeout | Idéntico — 0 duplicados |
| Cliente firma antes que el técnico | Rechazado con error explícito, no aceptado en silencio |
| Cierre solicitado dos veces | Segunda llamada devuelve 409, no reejecuta notificaciones/promoción de levantamiento |
| Declarar garantía + asociar factura simultáneamente | Ambas se resuelven sin pisarse; si compiten, la segunda ve 409 con estado fresco |
| Reagendar y reasignar en paralelo (campos distintos) | Ambos cambios sobreviven, sin lost-update sobre campos disjuntos |
| Reasignar el mismo campo en paralelo (2 técnicos distintos) | El perdedor recibe 409, no un commit silencioso |
| Guardar plantilla mientras un técnico completa una tarea del mismo grupo | La respuesta del técnico sobrevive o el guardado de plantilla se detiene con error — nunca borrado silencioso |
| Técnico inicia tarea fuera de la fecha/ventana | Rechazado en backend, con excepción auditada si es autorizada |
| `PUT` con `estado=cerrada` directo | Rechazado — el campo no debe ser escribible por esa vía |

**Plan de carga:** diseñar (no ejecutar sin autorización explícita y entorno aislado) con k6/Locust sobre: ráfaga de 200 creaciones en 10 min, 30 técnicos actualizando checklist simultáneamente, 50 firmas de cliente concurrentes. Medir p50/p95/p99, tasa de error, duplicados creados (contra `mant_visitas`), correlativos únicos, firmas asociadas a la OT correcta. **No ejecutado en esta fase — BLOQUEADO por mandato de solo lectura.**

---

## 14. MASTER TASK REGISTER INICIAL

`ID | Área | Escenario | Severidad | Evidencia | Impacto | Solución | Estado | Dependencias | Archivos | Prueba | Resultado`

| ID | Área | Escenario | Sev. | Evidencia (símbolo) | Impacto | Solución | Estado | Dependencias | Archivos | Prueba | Resultado |
|---|---|---|---|---|---|---|---|---|---|---|---|
| OT2-P0-01 | Arquitectura | 3 orígenes crean OT en paralelo | P0 | `ot2_api_crear`, `_mant_lev_crear_ot_core`, `_mant_visita_crear_core` | Reglas divergen por origen | Consolidar en 1 servicio | Pendiente | Ninguna | app.py, tickets_module.py | Test de caracterización de los 3 caminos | — |
| OT2-P0-02 | Firmas/Cierre | Cierre vía legacy o vía OT-espejo sin factura/centro de costo | P0 | `mant_visita_cerrar`, `mant_visita_firmar`, `mant_lev_cerrar` vs `mant_ot_aprobar_cierre` | OT cobrable cerrada sin documento | Mover gates a `_ot_validar_cierre` o forzar `aprobar-cierre` único | Pendiente | Confirmar dueño de `mant_lev_cerrar` con Daniel | app.py, ot_ficha.html, ot_ejecutar.html | Cierre por las 3 vías con OT sin factura → debe fallar en las 3 | — |
| OT2-P0-03 | Permisos | Técnico crea OT de cliente | P0 | `_mant_visita_crear_core`, `ot2_api_crear` sin gate de rol | Fabricación de OT/garantía por técnico | Agregar chequeo de rol explícito | Pendiente | Ninguna | app.py | Request de técnico → 403 | — |
| OT2-P0-04 | Seguridad | `/f/<key>` sin auth | P0 | `serve_archivo`, `_subir_firma_storage` | Filtración de firmas/fotos | Auth + URLs firmadas GCS | Pendiente | Ninguna | app.py | Descarga sin sesión → 401/403 | — |
| OT2-P0-05 | Finanzas | Factura sin verificar ERP | P0 | `ot2_api_finanzas`, `ot2_api_crear` vs `mant_ot_asociar_factura` | Cierre "facturado" sin factura real | Exigir `_erp_doc_lookup`+RUT en ambos | Pendiente | Ninguna | app.py | Documento inventado → rechazado | — |
| OT2-P0-06 | Estados | `PUT` fuerza `estado=cerrada` | P0 | `mant_visita_update`, campo `estado` en whitelist | Cierre sin ninguna regla | Quitar `estado` del whitelist | Pendiente | Ninguna | app.py | `PUT {"estado":"cerrada"}` → rechazado | — |
| OT2-P0-07 | Concurrencia | Doble-click/reintento duplica OT | P0 | `ot2_api_crear`, vínculo post-commit | 2 OT por 1 trabajo | `client_request_id` UNIQUE | Pendiente | Migración de columna | app.py | 2 POST idénticos → 1 sola OT | — |
| OT2-P0-08 | Calendario | Choque de horario no bloquea | P0 | `_validar_disponibilidad_visita` nunca invocada en escritura | Doble-agenda de técnico | Cablear validación en creación/reasignación | Pendiente | Decisión: ¿bloquear o solo advertir con motivo? | app.py, ot2_form.js | 2 OT mismo técnico/horario → advertencia auditada | — |
| OT2-P0-09 | Concurrencia | Guardar plantilla borra checklist en curso | P0 | `mant_plantilla_actualizar`→`_grupo_ot_sin_editar`→`_sincronizar_grupo_desde_plantilla` | Pérdida silenciosa de evidencia | Lock o transacción única check+delete | Pendiente | Ninguna | app.py | Guardar plantilla mientras técnico completa tarea → dato sobrevive | — |
| OT2-P0-10 | Creación/Datos | `fecha_fin`/contraparte se pierden | P0 | `ot2_api_crear` INSERT incompleto | Instalaciones multi-día nacen de 1 día | Agregar campos a formulario+INSERT | Pendiente | Ninguna | app.py, _modal_crear.html | Crear OT multi-día → `fecha_fin` persiste | — |
| OT2-P1-01 | Estados | `ot_sellada` no cubre cancelada/anulada | P1 | `_puede_ot_accion` | OT cancelada sigue ejecutable/firmable | Ampliar tupla de estados sellados | Pendiente | Revisar callers | app.py | OT cancelada → `ejecutar` debe fallar | — |
| OT2-P1-02 | Firmas | Completar sin iniciar | P1 | `mant_ot_firmar_revision` sin chequeo de estado | Pierde dato de duración real | `AND estado IN ('en_curso','en_ejecucion')` | Pendiente | Ninguna | app.py | Firmar OT `programada` → rechazado | — |
| OT2-P1-03 | Concurrencia | Race en `/iniciar`/`liberar-firma-tecnico` | P1 | Check-then-act sin repetir condición | Liberar firma técnico puede desandar firma de cliente | Mover condición al WHERE + rowcount | Pendiente | Ninguna | app.py | Concurrencia simulada | — |
| OT2-P1-04 | Finanzas | Cobertura sin candado de concurrencia | P1 | `mant_ot_declarar_cobertura` | Última decisión financiera pisada sin aviso | Igual patrón que `ot2_api_finanzas` | Pendiente | Ninguna | app.py | 2 declaraciones simultáneas → 409 en la segunda | — |
| OT2-P1-05 | Concurrencia | `mant_visita_update` sin candado de estado | P1 | UPDATE sin condición de estado | Reasignación sobre OT sellada | `AND estado NOT IN (...)` + rowcount | Pendiente | Definir con Daniel qué campos quedan congelados | app.py | Reasignar OT sellada → rechazado | — |
| OT2-P1-06 | UX financiero | "Modalidad: Pagado" mal interpretado | P1 | `ot2_api_crear`, `detalle.html` | Cobranza confía en campo equivocado | Relabelizar a "Cliente paga" | Pendiente | Ninguna | app.py, detalle.html | Revisión visual | — |
| OT2-P1-07 | Finanzas | Sin Dimensión Pago | P1 | Sin columnas de pago en esquema | No hay KPI de cobro real | Nueva tabla/columnas de pago | Pendiente | Decisión de negocio | app.py | — | — |
| OT2-P1-08 | Asignación | OT sin técnico permitido | P1 | `ot2_api_crear`, `mant_visita_update` | OT "fantasma" sin dueño operativo | Exigir ≥1 técnico salvo interna | Pendiente | Ninguna | app.py | Crear sin técnico → rechazado (no interna) | — |
| OT2-P1-09 | Jornada | Iniciar tarea sin validar fecha | P1 | `mant_tarea_cronometro` | Viola regla no negociable #5 | Chequeo de fecha±tolerancia con excepción auditada | Pendiente | Definir tolerancia con Daniel | app.py | Iniciar tarea de OT futura/pasada → rechazado sin autorización | — |
| OT2-P1-10 | KPI | Capacidad/utilización roto | P1 | `mant_analytics_data`, `duracion_planificada_min` nunca escrita | Decisión de dotación con dato falso | Escribir la columna en creación | Pendiente | Ninguna | app.py | — | — |
| OT2-P1-11 | SLA | 5 criterios de "atrasada" | P1 | 5 funciones distintas | Números de atraso no confiables | Función única parametrizable | Pendiente | Decisión de negocio (tolerancia) | app.py | — | — |
| OT2-P1-12 | Notificación | Solo campanita, sin correo | P1 | `_notificar_ot_asignada` delega en interna | Técnico sin app abierta no se entera | Agregar `_send_ilus_email` | Pendiente | Ninguna | app.py | — | — |
| OT2-P1-13 | Seguridad | PDF público con `SELECT v.*` financiero | P1 | `_ot_pdf_context`, `ot_publica_firmada` | Riesgo prospectivo de fuga de margen | SELECT explícito de columnas seguras | Pendiente | Ninguna | app.py | — | — |
| OT2-P1-14 | Tests | Cobertura casi nula en firma/cierre | P1 | `tests/test_permisos_equipos_ot.py` único, no cubre cierre | Sin red de seguridad para cambios | Tests de integración con Flask test client + BD de prueba | Pendiente | Entorno de test con BD | tests/ | — | — |
| OT2-P1-15 | Deuda técnica | Código muerto servido (`ot2_form.js`) | P1 | grep de `O2F.iniciar(` sin llamadas reales | ~3.500 líneas sin uso, trampa de auditoría | Retirar inclusión de panel.html | Pendiente | Ninguna | panel.html | — | — |
| OT2-P1-16 | Producto | Garantía/emergencia no seleccionables | P1 | `_modal_crear.html` TIPOS | Sin urgencia visible en agenda/TV | Decidir con Daniel: tipo propio o nivel de prioridad | Pendiente | Decisión de negocio | _modal_crear.html | — | — |

*(P2/P3 completos en §4; se omiten de esta tabla por brevedad — quedan registrados igual para Fase 1.)*

---

## 15. DECISIONES ABIERTAS QUE REALMENTE REQUIEREN APROBACIÓN DE DANIEL

Estas **no se pueden resolver solo con código** — son reglas de negocio o compromisos de producto que el equipo técnico no debe inventar (consistente con la memoria: "en OT 2.0: preguntar cada regla de negocio, no inventar"):

1. **¿"Emergencia/urgente" es un tipo de OT propio o un nivel de `prioridad` seleccionable?** Hoy no existe ninguna de las dos cosas en la UI viva (§5, OT2-P1-16).
2. **¿Qué campos quedan realmente "congelados" tras el cierre/sellado de una OT, y quién puede editarlos igual (con qué motivo auditado)?** Hoy `ot_sellada` es incompleto (no cubre cancelada/anulada) y el `PUT` genérico ignora el concepto por completo — se necesita la regla exacta antes de escribir el candado (OT2-P0-06, OT2-P1-01, OT2-P1-05).
3. **¿Qué tolerancia de fecha/hora se permite para iniciar una tarea fuera de la ventana programada, y quién puede autorizar la excepción?** (OT2-P1-09, regla no negociable #5 del stress test).
4. **¿`mant_lev_cerrar` (la tercera vía de cierre, "OT espejo") tiene algún consumidor real fuera del código (script, integración retirada) o se puede retirar de inmediato?** No se encontró ningún frontend que lo llame — pero antes de borrar código que funciona, se pregunta (REGLA #4.2).
5. **¿Cuál es el criterio único de "atrasada"/SLA que debe reportarse?** Hoy hay 5 definiciones distintas (tolerancia, granularidad de fecha/hora, subconjunto de estados) — es una decisión de negocio, no técnica (OT2-P1-11).
6. **¿Se retiran las rutas legacy `/firmar` y `/cerrar`, y en qué plazo?** Antes hay que decidir dónde vive "Configurar OT (plantilla+equipos)" — hoy esa función legítima obliga a mantener viva toda la pantalla legacy (§12).
7. **¿Qué campos de pago se necesitan realmente (`monto_pagado`, `saldo`, `fecha_pago`, ¿reconciliación con `ESPGDO` del ERP?) y quién los alimenta?** Es un modelo de datos nuevo, no un fix (§8, OT2-P1-07).
8. **¿Garantía necesita un flujo de doble aprobación (propuesta → revisión de Aarón) o el registro actual (motivo+monto+auditoría de un solo actor) es suficiente para el volumen actual?** (P2-06, mencionado por completitud).
9. **¿Se prioriza ahora cerrar el hueco de `/f/<key>` (OT2-P0-04) con URLs firmadas de GCS, aceptando el trabajo de migrar URLs ya guardadas en BD, o se acepta el riesgo mientras se decide un rediseño mayor de almacenamiento?** Es una decisión de costo/riesgo, no solo técnica.
10. **¿Cuál es el plan de conciliación de datos históricos antes de cualquier retiro de ruta legacy?** No hay acceso de lectura a producción en esta fase para cuantificar cuántas OT pasaron por cada camino de cierre — se necesita autorización explícita para esa consulta de solo lectura contra producción (§12).

### Lo que SÍ se puede corregir sin preguntar (bajo REGLA de push/merge autoautorizado, pero revisando diff antes)

- OT2-P0-06 (quitar `estado` del whitelist de `PUT`) — no cambia ninguna regla de negocio, solo cierra una vía de escritura no intencional.
- OT2-P0-03 (agregar gate de rol a creación) — la matriz de permisos ya documenta la regla; falta implementarla.
- OT2-P1-15 (retirar `ot2_form.js` muerto de `panel.html`) — cero impacto funcional, ya confirmado sin llamadas reales.
- OT2-P0-07 (idempotencia de creación vía `client_request_id`) — mecánica pura, no cambia comportamiento visible.
- OT2-P1-02, OT2-P1-03, OT2-P1-04, OT2-P1-05 (aplicar el patrón `WHERE <estado> + rowcount` ya usado en 6 endpoints hermanos) — es replicar un patrón ya aprobado y probado el 26/27-ago, no inventar uno nuevo.
- OT2-P1-06 (relabelizar "Pagado" → "Cliente paga" en la UI) — cosmético, sin tocar la columna interna.

---

## Hallazgos descartados tras verificación (no aparecen en el resumen ni en el Top 10)

- **"Cualquier técnico puede declarar factura sin rol"** en `/ot/api/finanzas/<vid>` — **REFUTADO**: el gate `@_ot_can_cobertura` ya restringe a admin/supervisor/ejecutivo/superadmin desde el 26-ago-2026. Sobrevive el hallazgo distinto de falta de verificación ERP (OT2-P0-05).
- **"El centro de costo se pierde al crear desde el wizard"** — **REFUTADO**: `ot2_api_crear` ya lo persiste desde el 26-ago-2026; el informe externo lo daba como pendiente sin evidencia actualizada.
- **Evidencia de `static/ot2_form.js` para la pérdida de `fecha_fin`/contraparte** — el archivo citado es código muerto (confirmado por grep, sin llamadas reales). El síntoma es real pero la causa raíz está en `_modal_crear.html`/`ot2_api_crear` (OT2-P0-10 reescribe la evidencia correcta).
- **"No existe ningún job de fondo que calcule OT atrasadas"** — **REFUTADO**: existe `_mantenciones_cron_run_once`, activo, corre diariamente a las 06:00 hora Chile y calcula atrasos — solo que notifica al técnico individual, no alimenta una bandeja de supervisión (la brecha real que sí sobrevive es la ausencia de esa bandeja consolidada).
- **"Los tests fallan/cuelgan porque no usan `def test_*`"** — **REFUTADO** como causa: 69 de 79 archivos de `tests/` sí usan `unittest.TestCase`/`def test_*`. El cuelgue real de `pytest -k ot` se reprodujo y es cierto, pero su causa estructural queda sin diagnosticar correctamente por el informe original.
- **Lapsus de referencia cruzada ("combinado con H4" debía decir H5)** en el dominio de máquina de estados — error de redacción interno, no un hallazgo técnico.