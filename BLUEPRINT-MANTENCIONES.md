# Blueprint — Módulo de Mantenciones ILUS: "Centro de Control"

> **Visión.** El **Resumen** de la ficha del cliente es el tablero único donde todo se consolida (operación, dinero, riesgo, oportunidad). **Cada dato se edita en su pestaña** (Finanzas, Visitas, Equipos, Contrato) y se refleja en el Resumen. **El Agente** es el único punto que pide, analiza y propone de forma inteligente. **Meta de la próxima semana:** cálculo automático de costos sin intervención humana.
>
> **Regla de oro (CLAUDE.md #4.2):** todo es **aditivo**. No se quita ningún card, columna ni endpoint existente. Cuando un dato hoy no existe se *agrega*; cuando existe pero está escondido, se *expone*.

---

## 1) RESUMEN = CENTRO DE CONTROL

El Resumen (`templates/mantenciones/ficha.html:859-1557`) hoy es **HTML estático** con 7 KPIs operativos pero **sin nada financiero, sin brecha contractual y sin oportunidades**. El objetivo es que la primera pantalla responda, sin abrir otra pestaña: *¿está al día?, ¿cuánto vale?, ¿qué se le debe cobrar?, ¿qué visita falta?*.

### Cómo se hidrata (sin reescribir el Resumen)
El Resumen sigue siendo estático para la carga inicial, pero al abrir la ficha se dispara **una llamada ligera** a los endpoints que **ya existen** y se rellenan los nuevos chips por JS (igual patrón que `cargarInteligencia()` en `ficha.html:830`):
- `GET /mantenciones/api/clientes/<cid>/inteligencia` → `_cliente_inteligencia()` (`app.py:57765`): brecha, exposición, vencidas, score, finanzas.
- `GET /mantenciones/api/clientes/<cid>/finanzas-servicios` → `mant_finanzas_servicios` (`app.py:48533`): pendiente de facturar, totales del año/mes.

### Tarjetas que debe consolidar el Resumen

| Tarjeta / chip | Qué muestra | Origen del dato (archivo:línea / campo) | ¿Existe hoy? |
|---|---|---|---|
| **KPI Contrato** (ampliar) | Estado + vencimiento **+ monto mensual + frecuencia real** | `mant_contratos.monto_mensual`, `frecuencia_meses` (`app.py:32582`); render `ficha.html:939-962` | Estado sí; monto/frecuencia **falta** |
| **Salud financiera** (nueva) | Monto mensual, **pendiente de facturar ($)**, margen acumulado | totales de `mant_finanzas_servicios` (`fin_t_pendiente`, `app.py:48694`; `ficha.html:4732-4736`) | Solo en tab Finanzas → **exponer** |
| **Próxima visita** (mantener) | Programada + días | `stats.pv_dias` (`ficha.html:994-1011`) | Sí |
| **Mantenciones vencidas (proyectadas)** (nueva) | N vencidas según contrato + botón "Generar OT" | `mant.vencidas` (`app.py:58004-58022`); acción ya existe `app.py:58229-58239` | Calculado pero **escondido en Intel** |
| **Brechas y oportunidades** (panel nuevo) | Brecha gratuitas + **$ exposición** + sin facturar + sin frecuencia | `pendientes_gratis`, `exposicion_clp` (`app.py:58028-58047`); `freq_origen` (`app.py:57943`) | Solo panel Agente → **promover** |
| **Equipos** (consolidar) | total / activos / advertencia / garantía por vencer | donut sidebar `ficha.html:1434-1483`; `equipos_resumen` (`app.py:57922`) | Disperso → **un solo card** |
| **Estado de contrato** (enriquecer) | Score AI + nivel de riesgo | `mant_contratos.ai_score`, `nivel_riesgo` (`app.py:45499-45510`) | Solo en tab Contrato → **chip en Resumen** |

### Panel "Brechas y oportunidades" (el corazón del centro de control)
Bloque de chips de color, cada uno con acción directa. Todos los datos **ya se calculan** en `_cliente_inteligencia()`; el trabajo es **mostrarlos en la primera pantalla**:
- 🔴 **Brecha gratuitas** `pendientes_gratis>0` → chip danger con `$exposicion_clp` (`app.py:58028-58047`) → botón *Agendar*.
- 🟠 **Sin facturar** N visitas completadas y no garantía (`app.py:48694`, `48705-48764`) → *Cotizar/Facturar*.
- 🔴 **Vencidas proyectadas** (`app.py:58019`) → *Generar OT*.
- 🟡 **Contrato sin frecuencia** (`freq_origen='regla_default'`, `app.py:57943`) → input inline (acción `set_frecuencia` ya existe, `app.py:58204-58211`).
- 🟡 **Contrato/garantía por vencer** (`app.py:39446-39463`) → *Renovar*.

---

## 2) RELLENO POR PESTAÑA (cada dato editable donde vive, reflejado en el Resumen)

El patrón canónico **ya implementado** es el de **frecuencia inline** en el panel Agente: `POST /inteligencia/accion` con `accion='set_frecuencia'` que ejecuta `UPDATE mant_contratos SET frecuencia_meses=%s` (`app.py:58648-58654`). Hay que **extender ese mismo patrón** a las demás pestañas. Regla: *editar en la pestaña → persistir en su columna → el Resumen lo lee del endpoint, no de HTML duro*.

### Contrato — datos comerciales (hoy NO editables)
**Problema raíz** (`app.py:43443-43444`): el `PUT /mantenciones/api/contratos/<ctid>` solo admite 9 campos; `costo_por_mant`, `costo_total`, `sla_horas`, `incluye_repuestos`, `incluye_mant_gratis`, `nivel_riesgo` **no se pueden tocar** → los KPIs de `ficha.html:3232-3281` son UI fantasma y "Mantenciones gratis / Repuestos" mienten "No incluidas / Excluidos" por su DEFAULT 0.

**Acción:**
1. Ampliar el whitelist `allowed` (`app.py:43443`) con: `costo_por_mant`, `costo_total`, `sla_horas`, `incluye_repuestos`, `incluye_mant_gratis`, `nivel_riesgo` (con casteo/validación numérica y de ENUM).
2. Mini-form "Editar datos comerciales del contrato" en la pestaña Contrato que haga ese PUT.
3. Al guardar → el chip de contrato del Resumen refleja monto/frecuencia/SLA.

### Finanzas — margen visible
**Problema** (`app.py:48564-48570`): `mant_finanzas_servicios` no selecciona `costo_proveedor`, así que el margen capturado en `ficha.html:9535-9548` no se muestra.

**Acción:** agregar `v.costo_proveedor` al SELECT, calcular `margen = monto_total - costo_proveedor` por fila, sumar **"Margen acumulado"** en `totales` (`app.py:48689-48695`) y renderizar columna Margen en la tabla (`ficha.html:4699`). El Resumen lee ese total para la tarjeta Salud financiera.

### Visitas — recálculo de costo al editar
**Problema** (`app.py:40960-40974` vs `49575`): el costo se calcula **solo al crear**. Si cambias técnico o repuestos en el `PUT`, el costo queda viejo.

**Acción:** extraer un helper `_mant_costo_auto_visita(tecnicos, repuestos, zona)` reutilizado por creación **y** edición. En `mant_visita_update`, si cambian técnicos/repuestos y el usuario **no** envió `costo` explícito, recalcular y **ofrecer** ("sugerido $X, ¿aplicar?") — nunca pisar un costo manual.

### Equipos — garantía como fuente única
**Problema** (`app.py:19737` cron vs `39432` ficha): dos fuentes de garantía desalineadas (`fecha_fin_garantia` vs `doc_fecha`).

**Acción:** unificar a `mant_maquinas.fecha_fin_garantia` en ambos; si está NULL, derivar de `doc_fecha + meses` como fallback único. Editar la fecha en la pestaña Equipos alimenta el chip de garantía del Resumen.

---

## 3) EL AGENTE INTELIGENTE (único punto que pide / analiza)

El Agente (`_cliente_inteligencia()`, `app.py:57765`) ya es un **motor determinista** de 13 dimensiones con 17 consultas accionables (`app.py:58177-58292`) y KPIs contextuales (`app.py:58159-58176`). **Principio:** ningún otro lugar "piensa"; las pestañas solo **muestran y editan**. El Agente:
- **Pide** lo que falta (datos del cliente, frecuencia, gratuitas incluidas, día preferido, costos de proveedor).
- **Analiza** (score salud, riesgo, brecha, exposición, proyección de fechas).
- **Propone** acciones ejecutables (Generar OT, Agendar, Cotizar, Renovar).

### Pulir el análisis del contrato

**A. Persistir lo que el analizador ya extrae pero descarta** (`app.py:45482-45483` → `45495`). El motor produce `frecuencia_sugerida_meses` y `sla_horas` pero el UPDATE final (verificado en `app.py:45495-45511`) **no los escribe** → quedan NULL para siempre.
- Ampliar el `SET` con: `ai_frecuencia_sug=%s`, `sla_horas=COALESCE(sla_horas,%s)`, `costo_por_mant=COALESCE(costo_por_mant,%s)`. **`COALESCE`** para no pisar lo que un humano editó (sección 2). Costo cero: el dato ya está calculado.

**B. Unificar la precedencia de frecuencia.** Hoy es inconsistente: casi todo usa `frecuencia_meses` primero (`app.py:19490, 47377, 57942`), pero el calendario invierte (`app.py:38667`; `ficha.html:3382, 3451`). Al rellenar `ai_frecuencia_sug` (punto A) esto **diverge**.
- Helper único `_freq_contrato(ct)` con precedencia **canónica: `frecuencia_meses` (humano) manda sobre `ai_frecuencia_sug`**. Corregir `app.py:38667` y `ficha.html:3382/3451`.

**C. Nuevas detecciones del cron** (mismo motor, sin datos nuevos): garantía recién vencida, contrato indefinido antiguo, sobre-mantención. Detalle en §4.

---

## 4) BRECHAS Y OPORTUNIDADES PRIORIZADAS

### 🔴 Impacto ALTO
| # | Tipo | Hallazgo | Evidencia | Propuesta |
|---|---|---|---|---|
| 1 | brecha | Analizador extrae `frecuencia_sugerida_meses` y `sla_horas` pero el UPDATE los **descarta** → NULL para siempre | `app.py:45482` vs `45495-45511` (verificado) | Ampliar UPDATE con `COALESCE` (§3-A) |
| 2 | brecha | No hay forma de **editar campos financieros** del contrato (whitelist de 9) | `app.py:43443-43444` (verificado) | Ampliar whitelist + mini-form (§2) |
| 3 | brecha | KPIs de contrato (costo/mant, SLA, frecuencia) son **UI fantasma**; "Repuestos/Gratis" mienten "Excluidos" por DEFAULT 0 | `ficha.html:3232-3281` | Cobran vida tras #1+#2 |
| 4 | oport. cobro | **Garantía YA vencida** no se detecta (cron solo mira ventana futura +30d) | `app.py:19740` | Paso cron 2d-bis: `fecha_fin_garantia BETWEEN -60d AND hoy` → sugerencia alta |
| 5 | oport. cobro | **Contratos indefinidos** nunca disparan revisión de reajuste | `app.py:19772` (`es_indefinido=0`) | Sugerencia `revisar_contrato_indefinido` si `fecha_inicio < hoy-12m` |
| 6 | oport. visita | **Vencidas proyectadas** se calculan pero no están en Resumen | `app.py:58004-58022`, `58229-58239` | Chip Resumen + botón Generar OT |
| 7 | oport. cobro | **Brecha gratuitas + exposición CLP** solo en tab Intel | `app.py:58028-58047` | Chip danger en Resumen |
| 8 | oport. cobro | **Visitas sin facturar** (no garantía) sin visibilidad en Resumen | `app.py:19700-19729`, `48705-48764` | Chip "Sin facturar N ($X)" |

### 🟠 Impacto MEDIO
| # | Tipo | Hallazgo | Evidencia | Propuesta |
|---|---|---|---|---|
| 9 | brecha | Precedencia de frecuencia **inconsistente** (calendario invierte) | `app.py:38667`; `ficha.html:3382/3451` | Helper `_freq_contrato` canónico (§3-B) |
| 10 | brecha | **Margen** (`costo_proveedor`) capturado pero no mostrado en Finanzas | `app.py:48564-48570` | Columna + total Margen (§2) |
| 11 | automatización | Costo de OT solo se calcula al **crear**, no al editar | `app.py:40960-40974` vs `49575` | Helper compartido + "sugerido, ¿aplicar?" (§2) |
| 12 | oport. cobro | **Sin tarifa por tipo** ni **recargo por zona** (transporte sí tiene `ZONA_MULT`) | `app.py:40972` vs `2769` | Reusar `ZONA_MULT` (§5) |
| 13 | brecha | Garantía con **dos fuentes desalineadas** (cron vs ficha) | `app.py:19737` vs `39432` | Unificar a `fecha_fin_garantia` (§2) |
| 14 | oport. cobro | **Sobre-mantención** (más visitas que lo pactado) no se mide | `app.py:58028-58036` | `exceso_cobertura = max(0, n_por_contrato - esperadas_a_hoy)` → consulta renegociar |
| 15 | oport. visita | Cliente con **equipos y sin contrato**: motor lo analiza, falta acción | `app.py:57935-57937`, `58286-58292` | Convertir `proponer_plan` en sugerencia del cron |

### 🟡 Impacto BAJO
| # | Tipo | Hallazgo | Evidencia | Propuesta |
|---|---|---|---|---|
| 16 | automatización | Aceptar sugerencia "agendar" **no crea la OT** (doble paso) | `app.py:19449-19597` | Al aceptar → crear OT con fecha sugerida + marcar aceptada |
| 17 | oport. cobro | **Cobro retrasado** (factura emitida e impaga) no se detecta | `app.py:48630-48654` | **Requiere confirmar con Daniel** si el ERP expone saldo (REGLA #4.1 read-only) — no inventar el dato |

---

## 5) ARQUITECTURA DE CÁLCULO AUTOMÁTICO DE COSTOS (próxima semana)

**Objetivo:** un endpoint que devuelva el costo de una mantención **sin intervención humana**, reusando 3 motores ya escritos (`_cliente_inteligencia`, `_cotiz_calcular_totales`, las reglas de `mant_reglas_negocio`).

### Paso 0 — Prerrequisitos (de §3-A): que el sistema lea frecuencia y SLA reales
Sin persistir `ai_frecuencia_sug`/`sla_horas` el costeo anual usaría fallbacks. Es el desbloqueante.

### Paso 1 — Knobs nuevos en `mant_reglas_negocio` (aditivo, editable sin deploy)
A `_REGLAS_DEFAULTS` (`app.py:57644-57668`):
- `tarifa_traslado_base` (CLP fijo fuera de RM)
- `tarifa_km` (para Haversine)
- `pct_repuestos_estimado` (% sobre mano de obra cuando no hay historial)
- `margen_objetivo_pct`

Editables desde `/mantenciones/configuracion` (`mant_reglas_guardar`, `app.py:58781`), invalidan caché (`app.py:57699`).

### Paso 2 — Multiplicador por zona (reusar transporte)
Extraer `ZONA_MULT` (`app.py:2769`) a constante de módulo compartida (sin tocar transporte). Helper `_mant_zona_mult(comuna_o_region)` sembrado desde la tabla `COMUNAS` ya en RAM (`app.py:2760-2766`, resolver TABCM `app.py:1726-1737`). Sin comuna → factor 1.0 (comportamiento actual intacto). Usa `mant_clientes.comuna/region` (`app.py:32511/32526`).

### Paso 3 — Traslado preciso por Haversine (degradación limpia)
Helper `_mant_distancia_km(lat1,lng1,lat2,lng2)` (sin API externa) usando lat/lng **ya persistidos**: `mant_clientes.direccion_lat/lng` (`app.py:33647`), `mant_tecnicos_externos.direccion_lat/lng` (`app.py:33398`). `costo_traslado = km × tarifa_km`; si falta lat/lng → fallback a `ZONA_MULT`.

### Paso 4 — Repuestos estimados por familia
`_mant_repuesto_promedio(familia_equipo)` = `AVG(costo_total)` histórico de esa familia (`mant_maquinas.familia_equipo`, repuestos en `app.py:40992-41002`). Sin historial → `pct_repuestos_estimado`.

### Paso 5 — Función orquestadora `_mant_estimar_costo_anual(cid)`
Determinista, reusa `_cliente_inteligencia`:
```
visitas_por_ano = 12 / frecuencia_meses           # ya proyectado, app.py:57995
mano_obra       = Σ tarifa_visita(tecnicos) × zona_mult
traslado        = _mant_distancia_km × tarifa_km  (o ZONA_MULT fallback)
repuestos_estim = _mant_repuesto_promedio(familia) × equipos_mantenibles
total_anual     = (mano_obra + traslado + repuestos_estim) × visitas_por_ano
                  + IVA  (iva_pct de mant_reglas_negocio, app.py:57650)
```
Devuelve **desglose por línea**.

### Paso 6 — Endpoint público de cotización
`GET /mantenciones/api/clientes/<cid>/costo-estimado`:
1. Llama `_mant_estimar_costo_anual(cid)`.
2. Arma items `[mano_obra, traslado, repuestos_estim]`.
3. Los pasa por `_cotiz_calcular_totales` (`app.py:68406-68474`) para IVA + descuento del contrato vigente.
4. Devuelve `{costo_por_visita, visitas_ano, costo_anual, desglose, margen_vs_proveedor}`.

Lo consume el Resumen (tarjeta Salud financiera) y la pestaña Contrato → **cotizar con un clic**.

---

## 6) ROADMAP EN 3 OLAS

### 🟢 Ola A — AHORA / esta sesión (bajo riesgo, 100% aditivo)
| Item | Riesgo | Por qué seguro |
|---|---|---|
| **A1.** Persistir `ai_frecuencia_sug`/`sla_horas`/`costo_por_mant` con `COALESCE` en el UPDATE de análisis (`app.py:45495`) | **Bajo** | Solo agrega columnas al SET; COALESCE no pisa valores humanos. Desbloquea KPIs fantasma (#1, #3) |
| **A2.** Ampliar whitelist del `PUT` de contrato + mini-form datos comerciales (`app.py:43443`) | **Bajo** | Aditivo; el flujo de subida sigue intacto (#2) |
| **A3.** Helper `_freq_contrato(ct)` canónico y corregir calendario/template (`app.py:38667`, `ficha.html:3382/3451`) | **Bajo-medio** | Toca render del calendario → verificar proyección tras el cambio (#9) |
| **A4.** Margen en Finanzas: `costo_proveedor` al SELECT + columna/total (`app.py:48564`) | **Bajo** | Solo lectura adicional (#10) |
| **A5.** Chips nuevos en Resumen (vencidas, brecha+exposición, sin facturar, sin frecuencia) hidratados desde endpoints existentes | **Bajo** | No toca Finanzas ni Intel; solo expone datos ya calculados (#6, #7, #8) |

### 🟡 Ola B — esta semana
| Item | Riesgo |
|---|---|
| **B1.** Tarjeta "Salud financiera" + KPI Contrato ampliado en Resumen | Bajo |
| **B2.** Recálculo de costo de OT al editar (helper `_mant_costo_auto_visita`, "sugerido ¿aplicar?") | **Medio** (toca creación + edición; mantener override manual) |
| **B3.** Unificar fuente de garantía a `fecha_fin_garantia` (cron + ficha) (#13) | Medio |
| **B4.** Nuevas detecciones del cron: garantía recién vencida (#4), indefinido antiguo (#5), sobre-mantención (#14) | Bajo (mismo motor, sin datos nuevos) |
| **B5.** Aceptar "agendar" → crear OT en un paso (#16) | Medio (reusa endpoint de creación; con confirmación) |

### 🔵 Ola C — próxima semana (la meta grande: costeo automático)
| Item | Riesgo |
|---|---|
| **C1.** Knobs de costeo en `mant_reglas_negocio` (Paso 1) | Bajo (aditivo, editable sin deploy) |
| **C2.** `ZONA_MULT` compartido + `_mant_zona_mult` (Paso 2) | Medio (extraer constante sin tocar transporte) |
| **C3.** Haversine `_mant_distancia_km` + repuestos por familia (Pasos 3-4) | Medio (degradación limpia con fallback) |
| **C4.** `_mant_estimar_costo_anual(cid)` (Paso 5) | **Medio-alto** (núcleo del costeo; validar contra OTs reales antes de exponer) |
| **C5.** Endpoint `GET /costo-estimado` cableado a Resumen y Contrato (Paso 6) | Medio (reusa `_cotiz_calcular_totales`) |

> **Dependencia crítica de orden:** **A1 → A3 → C4**. Sin persistir la frecuencia real (A1) y unificar su precedencia (A3), el costeo automático (C4) operaría sobre fallbacks y daría cifras divergentes entre calendario y cotización.
>
> **Pendiente de decisión de Daniel (no inventar dato):** detección de cobro retrasado (#17) requiere confirmar si el ERP Random expone saldo/pagado vía `fetch` **read-only** (REGLA #4.1). Si no lo expone, queda como gap conocido.

---

**Archivos clave para implementación:**
- `C:\Users\DANIE\OneDrive\Escritorio\Claude\Etiquetas\app.py` — backend (líneas citadas)
- `C:\Users\DANIE\OneDrive\Escritorio\Claude\Etiquetas\templates\mantenciones\ficha.html` — Resumen y pestañas
- `C:\Users\DANIE\OneDrive\Escritorio\Claude\Etiquetas\static\mant_ficha.js` — render del panel Agente (`7727-8050`)