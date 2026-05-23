# Plan — Módulo de Retiros (potenciar asignador + tolerancia 2 facturas paralelas)

**Fecha:** 2026-05-22 (Daniel Aguilar)
**Status:** Pendiente de aprobación

---

## Lo que ya existe en producción

| Tabla | Filas | Rol |
|---|---|---|
| `pickup_requests` | 7 | Solicitudes de retiro (ya integrado con ERP: `document_type`, `document_number`, `customer_*`, `pickup_person_*`) |
| `pickup_proposals` | 2 | Propuestas de fecha/hora cuando se reagenda |
| `pickup_packages` | 7 | Bultos/paquetes asociados al retiro |
| `pickup_signatures` | 7 | Firmas digitales |
| `pickup_blocks` | 0 | Bloqueos de horas |
| `pickup_settings` | 1 | Config: `max_picks_per_slot`, `max_picks_per_day`, `slot_minutes`, `slot_step_minutes`, `lunch_start/end`, kg/m3 por slot |
| `pickup_templates` | 5 | Emails/SMS pre-armados |
| `pickup_logs` | 36 | Auditoría |

**Templates:** `calendario.html`, `internal_dashboard.html`, `internal_detail.html`, `public_request.html`, `public_tracking.html`

**Lo que detecté que falta o se queda corto:**
- El monitor cuando entra a confirmar **solo ve un documento** (tú lo reportaste). El asignador es rudimentario.
- `pickup_settings.max_picks_per_day` existe pero NO se aplica como tolerancia visual en el calendario para "2 facturas paralelas".
- No hay tabla de **asignación interna** (quién ASIGNÓ el retiro, qué nombre se le puso, quién es el responsable de recibir al cliente cuando venga).
- No hay **recalculo de tiempos** al momento de la llegada real del cliente vs. lo agendado.

---

## Las 3 mejoras concretas que pediste

### Mejora 1 — Asignador de documento POTENCIADO

**Problema actual:** El monitor al entrar ve solo 1 documento sugerido, sin contexto para elegir si hay varios pendientes.

**Solución propuesta:**

- **Buscador con autocomplete** del ERP Random:
  - Escribes 3+ chars del RUT, número de factura o razón social → lista hasta 10 candidatos
  - Cada candidato muestra: tipo doc (FCV/BLV/VD), número, cliente, fecha, monto, bultos
  - Click → asigna el documento al retiro
- **Lista lateral "Pendientes de retiro"** (documentos ERP recientes no retirados todavía):
  - Filtros: solo hoy / esta semana / este mes
  - Cada fila tiene botón "Asignar al retiro"
- **Validaciones server-side:**
  - Documento no debe estar ya asignado a otro retiro activo
  - Si el RUT del cliente no coincide con el del documento, pedir motivo + audit log
- **Multi-documento:** un retiro puede tener N facturas/boletas (ej. cliente que viene a buscar 3 BLV del mismo día). Tabla nueva `pickup_request_docs` (request_id, document_type, document_number, ts, asignado_por) — relación 1:N.

### Mejora 2 — Tracking interno del nombre + recálculo al llegar

**Problema actual:** Cuando el cliente llega, no hay forma de marcar "ya llegó" y recalcular si vino a la hora o tarde.

**Solución propuesta:**

- **Campos nuevos en `pickup_requests`:**
  - `assigned_internal_name` (varchar 200) — el nombre con el que internamente identificamos el retiro (ej. "Lunes 9am - Aaron - 3 cajas Vitacura"). Lo asigna el monitor al confirmar.
  - `assigned_responsible_user_id` (int FK app_users) — quién va a recibir al cliente
  - `arrived_at` (datetime) — timestamp real cuando se marca "cliente llegó"
  - `served_at` (datetime) — timestamp cuando se entregó
  - `lateness_minutes` (int computed) — diferencia entre `confirmed_time_from` y `arrived_at` (puede ser negativo si llegó antes)
  - `service_minutes` (int) — duración real del servicio (`served_at - arrived_at`)
- **Botón "Marcar llegada"** en `internal_detail.html` (rojo grande, mobile-first):
  - Al click captura `arrived_at = NOW()`
  - Si llegó >15 min tarde → toast amarillo "Cliente llegó 27 min tarde"
  - Si llegó >60 min tarde → notifica al responsable
- **Botón "Cerrar atención"** después de la entrega:
  - Captura `served_at = NOW()`
  - Recalcula `service_minutes`
  - Si excedió el slot reservado → toast informativo "Atención duró 1h 25min (slot era 1h)"
- **Dashboard interno** con KPIs:
  - Retiros completados hoy / esta semana
  - Tasa de puntualidad (% que llegaron dentro de su slot)
  - Tiempo promedio de atención
  - Top clientes con retrasos recurrentes

### Mejora 3 — Tolerancia "2 facturas paralelas" en el calendario

**Problema actual:** El calendario muestra 1 slot por hora sin considerar que la bodega puede atender 2 retiros en paralelo.

**Solución propuesta:**

- **Setting nuevo:** `pickup_settings.parallel_capacity` (int, default 2)
  - Esto significa "la bodega puede atender N retiros en simultáneo"
  - Si un retiro es **GRANDE** (toma el día) cuenta como 1 capacidad
  - Si son **CHICOS** (toma 30 min) pueden caber muchos pero limitados por `max_picks_per_slot`
- **Campo nuevo en `pickup_requests`:** `weight_load` ENUM('light','normal','heavy','full_day')
  - `light` = 0.25 capacidad
  - `normal` = 0.5 capacidad
  - `heavy` = 1.0 capacidad
  - `full_day` = 2.0 capacidad (ocupa toda la bodega — bloquea el día)
- **Calendario UI mejorado:**
  - Por día se ve un **bar de capacidad** (0-100% en función de los retiros agendados)
  - Color: verde <60%, amarillo 60-90%, rojo >90%
  - Si Daniel intenta agendar un nuevo retiro que excede 100% → ilusConfirm "Este día ya tiene capacidad ocupada al 95%. ¿Confirmar igual?" con opción Sí / No
- **Visualización de paralelismo:**
  - En el detalle del día, ver lado a lado los retiros simultáneos (columnas verticales en el horario)
  - Si Pedro y Juan están en 10:00 ambos, se ven 2 cards al lado para que el monitor sepa que tiene 2 clientes al mismo tiempo
- **Bloqueo inteligente:**
  - Si ya hay 2 retiros `heavy` agendados al mismo día → el slot picker bloquea más adds salvo override admin
  - Notificación al responsable cuando un día se acerca a 100% capacidad (T-24h)

---

## Tablas/columnas nuevas (migraciones idempotentes)

```sql
-- A. Multi-documento por retiro (Mejora 1)
CREATE TABLE IF NOT EXISTS pickup_request_docs (
  id INT AUTO_INCREMENT PRIMARY KEY,
  request_id INT NOT NULL,
  document_type VARCHAR(30) NOT NULL,
  document_number VARCHAR(60) NOT NULL,
  asignado_por VARCHAR(190),
  asignado_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  notas TEXT,
  UNIQUE KEY uq_request_doc (request_id, document_type, document_number),
  INDEX idx_request (request_id),
  FOREIGN KEY (request_id) REFERENCES pickup_requests(id) ON DELETE CASCADE
);

-- B. Tracking interno + tiempos reales (Mejora 2)
ALTER TABLE pickup_requests
  ADD COLUMN assigned_internal_name VARCHAR(200) NULL,
  ADD COLUMN assigned_responsible_user_id INT NULL,
  ADD COLUMN arrived_at DATETIME NULL,
  ADD COLUMN served_at DATETIME NULL,
  ADD COLUMN lateness_minutes INT NULL,
  ADD COLUMN service_minutes INT NULL,
  ADD INDEX idx_arrived_at (arrived_at),
  ADD INDEX idx_responsible (assigned_responsible_user_id);

-- C. Capacidad paralela (Mejora 3)
ALTER TABLE pickup_settings
  ADD COLUMN parallel_capacity INT NOT NULL DEFAULT 2
  COMMENT 'Cuántos retiros simultáneos puede atender la bodega';

ALTER TABLE pickup_requests
  ADD COLUMN weight_load ENUM('light','normal','heavy','full_day') NOT NULL DEFAULT 'normal'
  COMMENT 'Peso operativo: light=0.25, normal=0.5, heavy=1.0, full_day=2.0',
  ADD INDEX idx_weight (weight_load, confirmed_date);
```

---

## Endpoints nuevos a implementar

| Método | Ruta | Función |
|---|---|---|
| GET | `/retiros/api/erp-documentos/buscar?q=<query>&limit=10` | Autocomplete documentos ERP |
| GET | `/retiros/api/erp-documentos/pendientes?dias=7` | Lista documentos ERP recientes sin retiro asignado |
| POST | `/retiros/api/requests/<id>/docs/agregar` | Body: `{document_type, document_number}` — agrega doc al retiro |
| DELETE | `/retiros/api/requests/<id>/docs/<doc_id>` | Quita doc del retiro |
| POST | `/retiros/api/requests/<id>/asignar-interno` | Body: `{internal_name, responsible_user_id}` |
| POST | `/retiros/api/requests/<id>/llegada` | Marca `arrived_at=NOW()`, calcula `lateness_minutes` |
| POST | `/retiros/api/requests/<id>/atencion-cerrada` | Marca `served_at=NOW()`, calcula `service_minutes` |
| GET | `/retiros/api/calendario/capacidad-dia?fecha=YYYY-MM-DD` | Devuelve `{pct_ocupado, retiros: [...], capacidad_max}` |
| GET | `/retiros/api/dashboard/kpis?desde=&hasta=` | Tasa puntualidad, tiempo promedio, top retrasos |

---

## Frontend a tocar

| Archivo | Cambio |
|---|---|
| `templates/retiros/internal_detail.html` | Asignador de documento potenciado + botones "Marcar llegada" / "Cerrar atención" + tracking interno (nombre + responsable) |
| `templates/retiros/calendario.html` | Bar de capacidad por día + visualización paralela + bloqueo cuando >100% |
| `templates/retiros/internal_dashboard.html` | Nuevos KPIs (puntualidad, tiempo promedio, top retrasos) |
| `app.py` | 9 endpoints nuevos + 3 migraciones idempotentes en `init_pickup_tables` |

---

## Orden sugerido de implementación

1. **Migraciones SQL** (3 tablas/columnas, idempotentes — riesgo cero)
2. **Backend Capa 1** — Multi-documento + asignador potenciado (Mejora 1)
3. **Backend Capa 2** — Tracking interno + recálculo tiempos (Mejora 2)
4. **Backend Capa 3** — Capacidad paralela (Mejora 3)
5. **Frontend Capa 1** — `internal_detail.html` con todo lo nuevo
6. **Frontend Capa 2** — Calendario con capacidad
7. **Frontend Capa 3** — Dashboard interno con KPIs
8. **Verificación end-to-end** con un retiro de prueba

---

## Lo que NO entra en esta tanda (mencionado pero queda fuera)

- Notificaciones automáticas al cliente sobre llegada/atención (depende del DNS de email y Twilio aprobado)
- Conversión automática "documento ERP → retiro" cuando se genera la factura (sería ideal pero requiere webhook ERP, fuera de scope)
- Geolocalización del cliente cuando viene al retiro (sería muy invasivo)
- Pago en bodega (no es parte del flujo)

---

## Preguntas para Daniel antes de implementar

1. **`parallel_capacity` default 2** — ¿es correcto o son 3? ¿La capacidad cambia por día de la semana (ej. lunes 3, sábado 1)?
2. **`weight_load`** — ¿prefieres que sea automático según `pickup_packages.kg` total o que el monitor lo elija manualmente?
3. **Multi-documento** — ¿hasta cuántos docs máximo por retiro? ¿O sin límite?
4. **`assigned_responsible_user_id`** — ¿quiénes son los responsables válidos? Solo Aarón? Aarón + otro? Cualquier usuario con permiso de retiros?
5. **Notificaciones internas** — ¿quieres una campana de notificaciones para retiros (similar a la de mantenciones), o por ahora solo dashboard?

---

## Riesgos / consideraciones

- Cambiar la lógica del calendario puede afectar la UI pública (`public_tracking.html`). Hay que probar que el cliente sigue viendo su slot bien.
- La columna `weight_load` con default 'normal' aplica a los 7 retiros históricos — no afecta nada pero queda con ese label en el dashboard.
- El recálculo de tiempos requiere reloj sincronizado (Railway está en UTC, pasamos por `chile_fmt` filter — debería ser OK).
- Si Daniel tiene 2 ejecutivos diferentes atendiendo en paralelo, el sistema debe saber distinguirlos. Por eso el `assigned_responsible_user_id`.

---

**Cuando apruebes este plan, atacamos por capa. Si quieres ajustar algo (ej. más documentos por retiro, capacidad diferente, etc.), te leo y ajustamos antes de codear.**
