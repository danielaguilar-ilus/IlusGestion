# Notas de verificación — "el levantamiento es un tipo más"

Plan implementado el 2026-08-12 en el worktree `sesion-ot`, branch
`fix/ot-jinja-leak-y-backfill-categorias`, commits del Paso 1 al Paso 7
(Paso 8 quedó pendiente, es cosmético/opcional — ver reporte final del
agente).

**Estas consultas NO se ejecutaron.** El agente que implementó este plan
no tiene acceso a la base de datos de producción. Quedan documentadas
aquí para que el coordinador (o Daniel) las corra manualmente antes y
después de cada despliegue.

Todas las consultas son `SELECT` puro — ninguna modifica datos.

---

## 1. ANTES de desplegar

### 1.1 — Tamaño del problema: OT con vínculo pero sin ser tipo levantamiento

Mide cuántas OT hoy tienen `levantamiento_id` poblado siendo de OTRO
tipo — es el universo que hoy se comporta "como levantamiento" para
cosas que no debería, y es exactamente lo que el Paso 4/5 corta hacia
adelante.

```sql
SELECT tipo, estado, COUNT(*) AS n
  FROM mant_visitas
 WHERE levantamiento_id IS NOT NULL
   AND tipo <> 'levantamiento'
 GROUP BY tipo, estado
 ORDER BY n DESC;
```
**Esperado:** un número documentado (probablemente no-cero). Sirve de
línea base — no hay un "correcto" a priori, es la magnitud real del
cambio.

### 1.2 — ENUM real de `mant_visitas.tipo` en producción

Confirma que el ENUM real de la base coincide con `tipos_ok` usado en
`_mant_lev_crear_ot_core` (app.py) y en `tickets_module.py`
(`tipos_ot_ok`): `levantamiento, instalacion, preventiva, correctiva,
visita_tecnica, inspeccion, garantia, cambio_equipo, desinstalacion,
capacitacion, repuesto, revision_interna, visita_correctiva,
control_calidad`.

```sql
SELECT COLUMN_TYPE
  FROM information_schema.COLUMNS
 WHERE TABLE_SCHEMA = DATABASE()
   AND TABLE_NAME = 'mant_visitas'
   AND COLUMN_NAME = 'tipo';
```
**Esperado:** el ENUM de producción contiene AL MENOS los 14 valores de
`tipos_ok`. Si falta alguno (ej. `control_calidad` si el ALTER no llegó
a correr todavía), el INSERT de una OT de ese tipo falla directo en la
base — no relacionado con este plan, pero conviene confirmarlo antes.

### 1.3 — Impacto de la regla R1/R1b (Paso 1): tareas huérfanas por tipo de OT

Mide cuántas OT ABIERTAS (no cerradas) tienen tareas con `maquina_id
NULL` — el universo que el Paso 1 deja de bloquear en el cierre,
agrupado por tipo para ver si el bug reportado (OT-2026-00056) es un
caso aislado o un patrón.

```sql
SELECT v.tipo, v.estado, COUNT(DISTINCT v.id) AS ots_afectadas,
       COUNT(t.id) AS tareas_huerfanas
  FROM mant_visitas v
  JOIN mant_visita_tareas t ON t.visita_id = v.id
 WHERE t.maquina_id IS NULL
   AND v.estado NOT IN ('cerrada', 'cancelada', 'anulada')
 GROUP BY v.tipo, v.estado
 ORDER BY tareas_huerfanas DESC;
```
**Esperado:** documentar cuántas OT (y de qué tipo) tenían tareas
huérfanas bloqueándolas antes del Paso 1 — sirve para medir el impacto
real del fix reportado por Daniel.

### 1.4 — Fotos candidatas al backfill que el Paso 2 deja de tocar

Mide cuántas filas de `mant_visita_fotos` (con `maquina_id`) pertenecen
a visitas NO-levantamiento pero con vínculo — es el universo que el
backfill (`_reparar_fotos_levantamiento_a_galeria`) dejó de re-empujar
a la ficha en cada arranque desde el Paso 2.

```sql
SELECT v.tipo, COUNT(*) AS fotos_candidatas
  FROM mant_visita_fotos f
  JOIN mant_visitas v ON v.id = f.visita_id
 WHERE f.maquina_id IS NOT NULL
   AND v.levantamiento_id IS NOT NULL
   AND v.tipo <> 'levantamiento'
   AND (f.cloudinary_url IS NOT NULL
        OR (f.archivo_path IS NOT NULL AND f.archivo_path <> ''))
 GROUP BY v.tipo
 ORDER BY fotos_candidatas DESC;
```
**Esperado:** documentar el número. Estas fotos YA copiadas a
`mant_maquina_fotos` en arranques anteriores NO se revierten (Paso 2 es
seguro en caliente) — esta consulta solo mide cuántas quedan "congeladas"
sin re-copiarse en el futuro.

### 1.5 — Universo de riesgo real del Paso 5 (OT EN CURSO ahora mismo)

**LA MÁS IMPORTANTE ANTES DE DESPLEGAR EL PASO 5.** Mide cuántas OT
están ABIERTAS ahora mismo (con un técnico potencialmente trabajando)
que tienen vínculo `levantamiento_id` pero NO son tipo `levantamiento`.
Estas son las OT que, en el momento exacto en que el Paso 5 se
despliegue, dejarán de escribir la ficha permanente del equipo y de
marcar fotos como "principal" — sin aviso, a mitad de captura.

```sql
SELECT v.id AS visita_id, v.numero_ot, v.tipo, v.estado,
       v.tecnico_user_id, v.cliente_id, v.levantamiento_id
  FROM mant_visitas v
 WHERE v.levantamiento_id IS NOT NULL
   AND v.tipo <> 'levantamiento'
   AND v.estado NOT IN ('cerrada', 'cancelada', 'anulada')
 ORDER BY v.id DESC;
```
**Esperado:** lista concreta de OT (idealmente corta o vacía). Si hay
filas, coordinar con Daniel/el técnico asignado ANTES de desplegar el
Paso 5 — o esperar a que esas OT específicas se cierren.

### 1.6 — Confirmar que `modalidad_captura` existe antes de desplegar el Paso 7

El candado del Paso 7 depende de esta columna (Paso 3). Producción corre
con `ILUS_SKIP_MIGRATIONS=1`, pero `_ensure_lev_modalidad_captura_col()`
corre SIEMPRE en boot — de todos modos, confirmar antes de asumir que el
Paso 7 ya está protegiendo de verdad (y no cayendo al caso "1054 → se
permite" del Paso 7c en cada llamada).

```sql
SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE
  FROM information_schema.COLUMNS
 WHERE TABLE_SCHEMA = DATABASE()
   AND TABLE_NAME = 'mant_levantamientos'
   AND COLUMN_NAME = 'modalidad_captura';
```
**Esperado:** 1 fila, `COLUMN_TYPE = enum('descubrimiento','ficha')`,
`IS_NULLABLE = YES`. Si devuelve 0 filas, el Paso 3 no corrió en ese
entorno todavía — revisar logs de boot (`[ensure_lev_modalidad]`).

---

## 2. DESPUÉS de cada paso desplegado

### Después del Paso 1 (tareas huérfanas ya no bloquean el cierre)

```sql
-- Repetir 1.3. Esperado: el conteo de "tareas_huerfanas" no cambia (el
-- Paso 1 no borra tareas, solo deja de usarlas como candado), pero las
-- OT que antes reportaban "no tiene tareas asignadas" o quedaban
-- trabadas por esto ya deben poder cerrarse. Confirmar puntualmente con
-- el caso real OT-2026-00056 si sigue accesible.
```

### Después del Paso 2 (backfill de fotos acotado)

```sql
-- Conteo de mant_maquina_fotos por visita_origen agrupado por tipo de
-- la visita de origen, ANTES vs DESPUÉS de un reinicio de instancia.
SELECT v.tipo, COUNT(*) AS fotos_en_galeria
  FROM mant_maquina_fotos mf
  JOIN mant_visitas v ON v.id = mf.visita_origen
 GROUP BY v.tipo
 ORDER BY fotos_en_galeria DESC;
```
**Esperado:** tras un reinicio de instancia, el conteo para tipos
DISTINTOS de `levantamiento` no debe crecer (ya no se re-empujan fotos
nuevas hacia esos tipos). El conteo para `tipo='levantamiento'` sí puede
seguir creciendo normalmente.

### Después del Paso 3 (columna `modalidad_captura`)

```sql
-- Repetir 1.6 — debe devolver 1 fila ahora en TODOS los entornos.
-- Además, confirmar que los levantamientos NUEVOS creados después del
-- deploy la traen poblada (no NULL):
SELECT modalidad_captura, COUNT(*) AS n
  FROM mant_levantamientos
 WHERE fecha_inicio >= '2026-08-12'
 GROUP BY modalidad_captura;
```
**Esperado:** filas con `modalidad_captura` IN ('descubrimiento',
'ficha') para los levantamientos creados después del deploy — NULL debe
ir desapareciendo a medida que se crean levantamientos nuevos (los
viejos se quedan NULL para siempre, es intencional).

### Después del Paso 4 (el levantamiento solo nace si tipo='levantamiento')

**La verificación más importante de todo el plan.**

```sql
-- Ninguna OT NUEVA no-levantamiento debe nacer con levantamiento_id
-- poblado. Filtrar por fecha del deploy hacia adelante.
SELECT id, numero_ot, tipo, levantamiento_id, created_by, fecha_programada
  FROM mant_visitas
 WHERE tipo <> 'levantamiento'
   AND levantamiento_id IS NOT NULL
   AND fecha_programada >= '2026-08-12'  -- fecha real del deploy
 ORDER BY id DESC;
```
**Esperado: 0 filas.** Si aparece alguna, algo no está cortando la
creación como debería (revisar si además tiene `visita_id` reverse en
`mant_levantamientos` sin pasar por `_mant_lev_crear_ot_core` — ej. la
"segunda puerta" del Paso 5 si ese paso aún no se desplegó).

```sql
-- Confirmar que la auditoría no se pierde para OT no-levantamiento
-- nuevas (Paso 4d): deben aparecer como entidad='visita', no
-- entidad='levantamiento'. Columna de fecha real (verificada contra el
-- CREATE TABLE, app.py ~48261): created_at. `entidad` es VARCHAR(40)
-- desde la migración "ENUM entidad → VARCHAR" (app.py ~48508) -- ya NO
-- es el ENUM original ('cliente','maquina','contrato','visita'), admite
-- valores libres como 'levantamiento'/'sistema' (ya en uso hoy).
SELECT entidad, accion, COUNT(*) AS n
  FROM mant_logs
 WHERE accion IN ('creado', 'creada')
   AND created_at >= '2026-08-12'
 GROUP BY entidad, accion;
```
**Esperado:** aparecen filas `entidad='visita', accion='creada'` (antes
solo existía `entidad='levantamiento', accion='creado'` para TODAS las
OT).

### Después del Paso 5 (segunda puerta cerrada)

```sql
-- Repetir 1.5 — el universo de OT en riesgo debería reducirse con el
-- tiempo a medida que esas OT específicas se cierran (no desaparece de
-- golpe: las que ya tenían vínculo lo conservan, solo dejan de escribir
-- ficha en ediciones NUEVAS desde el deploy en adelante).
```

```sql
-- Confirmar que _ensure_levantamiento_para_visita ya NO crea
-- levantamientos nuevos para preventiva/instalacion/inspeccion.
SELECT v.tipo, COUNT(*) AS n
  FROM mant_levantamientos l
  JOIN mant_visitas v ON v.id = l.visita_id
 WHERE l.fecha_inicio >= '2026-08-12'  -- fecha real del deploy del Paso 5
   AND v.tipo <> 'levantamiento'
 GROUP BY v.tipo;
```
**Esperado: 0 filas** para levantamientos creados DESPUÉS del deploy del
Paso 5 con tipo distinto de 'levantamiento'.

### Después del Paso 6 (universo acotado al cierre)

Prueba puntual, no una query genérica: tomar una OT de levantamiento de
prueba (datos de Daniel) para un cliente con varias máquinas, dejar
equipos SIN revisar fuera del alcance de la OT, e intentar cerrar.

```sql
-- Antes de cerrar, comparar cuántas máquinas activas con
-- aplica_mantencion=1 tiene el cliente en total...
SELECT COUNT(*) AS total_cliente
  FROM mant_maquinas
 WHERE cliente_id = <CLIENTE_ID_PRUEBA>
   AND aplica_mantencion = 1 AND estado <> 'baja';

-- ...contra cuántas están realmente vinculadas a ESA OT (universo nuevo
-- del Paso 6):
SELECT COUNT(DISTINCT maquina_id) AS total_ot
  FROM (
    SELECT maquina_id FROM mant_visita_tareas
     WHERE visita_id = <VISITA_ID_PRUEBA> AND maquina_id IS NOT NULL
    UNION
    SELECT maquina_id FROM mant_levantamiento_items
     WHERE levantamiento_id = <LEV_ID_PRUEBA> AND maquina_id IS NOT NULL
  ) x;
```
**Esperado:** `total_ot` < `total_cliente` (si el cliente tiene más
máquinas que las de esta OT específica), y el mensaje "Levantamiento
incompleto: N equipos" al intentar cerrar debe usar `total_ot`, NO
`total_cliente`.

### Después del Paso 7 (candado real de agregar equipos)

Prueba puntual con datos de Daniel, no una query — ver sección 3 más
abajo (plan de pruebas manuales). Solo como apoyo:

```sql
-- Confirma que el override de superadmin (si se usó) quedó auditado.
SELECT * FROM mant_logs
 WHERE accion = 'equipo_agregado_override_superadmin'
 ORDER BY id DESC LIMIT 20;
```

---

## 3. Plan de pruebas manuales

**SIEMPRE con los datos de prueba de Daniel — NUNCA con un cliente
real** (le llegan correos, se generan documentos reales).

1. **Una OT de instalación no se comporta como levantamiento.**
   Crear una OT tipo `instalacion` (o `preventiva`/`correctiva`) con 1–2
   equipos de la ficha del cliente de prueba. Verificar:
   - No adquiere `levantamiento_id` al crearse (`SELECT levantamiento_id
     FROM mant_visitas WHERE id=<vid>` → NULL).
   - Al subir una foto de un equipo, NO se marca como "principal" ni
     pisa `mant_maquinas.foto_url` — se guarda en `mant_visita_fotos`
     igual (evidencia de la OT).
   - Al editar un dato de ficha del equipo (serie/marca/modelo) desde
     el modal de captura, la respuesta sigue siendo `ok:true` (no un
     error), pero `mant_maquinas` NO cambia — en su lugar debe aparecer
     una fila nueva en `mant_sugerencias_evidencia` con
     `tipo_sugerencia='dato_ficha_equipo_ot'`.

2. **Un levantamiento por descubrimiento sí deja agregar equipos y los
   materializa a la ficha al cerrar.**
   Crear una OT tipo `levantamiento` con "descubrimiento" activado
   (0 equipos preseleccionados). Verificar:
   - `mant_levantamientos.modalidad_captura = 'descubrimiento'`.
   - El técnico puede agregar equipos nuevos desde el modal en terreno
     (POST a `/mantenciones/api/levantamientos/<lid>/items` responde
     `ok:true`).
   - Al cerrar la OT, los equipos se materializan en `mant_maquinas`
     (flujo FROZEN de promoción, no tocado).

3. **Un levantamiento "con equipos de la ficha" (modalidad `ficha`) NO
   deja agregar equipos pero sí escribe la ficha permanente.**
   Crear una OT tipo `levantamiento` seleccionando equipos YA
   existentes en la ficha (sin descubrimiento). Verificar:
   - `mant_levantamientos.modalidad_captura = 'ficha'`.
   - Un intento de agregar un equipo nuevo devuelve 403
     `TECNICO_NO_AGREGA_EQUIPOS` (para cualquier rol, incluido
     supervisor/admin — solo superadmin con `override_superadmin` +
     motivo ≥10 caracteres puede forzarlo).
   - Editar datos de ficha de los equipos SÍ escribe `mant_maquinas`
     (a diferencia del caso 1).

4. **Regresión del caso ya reportado (OT-2026-00056): una OT de
   instalación con tareas sin `maquina_id` no bloquea al técnico al
   firmar.**
   Crear una OT con al menos 1 tarea huérfana (`maquina_id NULL` —
   puede simularse con un equipo declarado desde un Ticket sin ficha
   previa). Verificar que el técnico puede completar el checklist de
   las demás tareas y FIRMAR sin que el sistema exija completar la
   tarea huérfana (no dibuja tarjeta, es imposible de trabajar).

5. **El PDF de una OT vieja con `levantamiento_id` colgado no pierde su
   anexo fotográfico.**
   Abrir el PDF/informe de una OT HISTÓRICA (anterior a este deploy)
   que sea de tipo distinto a `levantamiento` pero tenga
   `levantamiento_id` poblado y equipos capturados. Verificar que el
   informe sigue mostrando el anexo de equipos/fotos completo (la
   selección de plantilla del PDF sigue usando criterio de EVIDENCIA,
   `_ot_es_levantamiento(...) and ctx.get('lev_items')` — no se tocó).

6. **Tras un reinicio de instancia, el backfill de fotos no vuelve a
   empujar fotos de OT no-levantamiento a la ficha.**
   Forzar (o esperar) un reinicio de la instancia de Cloud Run.
   Verificar en los logs `[reparar-fotos-galeria]` que el criterio
   aplicado es solo `tipo='levantamiento'` (buscar el log de
   `_reparar_fotos_levantamiento_a_galeria`), y repetir la consulta de
   la sección "Después del Paso 2" para confirmar que el conteo de
   `mant_maquina_fotos` para tipos no-levantamiento no creció.

7. **(Nueva, Paso 4) Equipo sin ficha en una OT no-levantamiento queda
   excluido, no crea tarea huérfana.**
   Simular la generación de una OT desde un Ticket con un producto que
   NO tiene ficha en `mant_maquinas`, con `tipo_ot` distinto de
   `levantamiento`. Verificar que la respuesta incluye
   `equipos_excluidos` (o, si era el único equipo, `error_codigo:
   EQUIPO_SIN_FICHA`), y que NO se crea una tarea con `maquina_id NULL`
   para ese producto. Repetir el mismo caso con `tipo_ot='levantamiento'`
   y confirmar que ahí SÍ se acepta tal cual (comportamiento sin
   cambios).

8. **(Nueva, Paso 4) — ALTO IMPACTO A VERIFICAR: "Generar OT" desde un
   Ticket de cliente 100% nuevo.**
   En `tickets_module.py` (rama `cliente_recien_creado`), TODOS los
   equipos del ticket se declaran hoy sin `maquina_id` (no hay ficha
   previa posible — el cliente acaba de crearse). Si el wizard no manda
   `tipo_ot='levantamiento'` explícito, después de este cambio esos
   equipos quedan excluidos y la OT puede fallar con
   `EQUIPO_SIN_FICHA`. Probar este flujo específico con un ticket de
   prueba de Daniel y confirmar que el mensaje de error es claro y que
   el camino correcto (elegir tipo 'Levantamiento' en el wizard) sigue
   funcionando.

---

## 4. Pendiente explícito (no se hizo en esta sesión)

- **Paso 8 (opcional/cosmético):** renombrar `_ot_es_levantamiento` a
  `_ot_tiene_levantamiento_vinculado` con alias de compatibilidad. No se
  hizo por priorizar los pasos con impacto funcional real — el plan lo
  autoriza explícitamente a saltarse.
- **Superficie de UI para el override de superadmin (Paso 7):** el
  override existe a nivel de API (`override_superadmin` +
  `override_motivo` en el body del POST), pero NO se construyó un
  control visual (botón/checkbox/prompt) en `ot_ejecutar.html` para que
  un superadmin lo dispare desde la pantalla — queda como TODO explícito,
  tal como pidió el plan, para no inventar una superficie de UI sin
  poder verificarla en vivo.
