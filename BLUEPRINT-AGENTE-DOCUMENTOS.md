Tengo toda la evidencia que necesito. Escribo la propuesta.

# Propuesta Estratégica — Agente de Documentos Inteligente + Pipeline Documental con Orden de Compra
**Para:** Daniel (Jefe de Servicio Técnico ILUS) · **De:** tu aliado estratégico · **Fecha:** 21/06/2026

---

## 1) DIAGNÓSTICO — por qué un PEDIDO se trata como CONTRATO

El caso real (Pedido N° 4500865147 de Clínica Alemana clasificado como contrato) **no es mala suerte: es una brecha estructural** de tres causas que se acumulan.

### Causa raíz A — La palabra "PEDIDO" no penaliza el gate
El detector tiene una lista de "esto NO es contrato" (`_CONTRACT_NEG`) que incluye factura, guía, cotización, boleta y **"orden de compra" (-35)**, pero **NO incluye "pedido"** ni "nro de pedido".

> Evidencia: `contrato_reglas.py:680-698`. La línea `("orden de compra", "una ORDEN DE COMPRA", 35)` existe (685), pero no hay entrada para `pedido`.

Un documento que se titula **"PEDIDO"** (que es como las clínicas grandes y el retail nombran sus órdenes de compra) entra **sin restar un solo punto**. Si además trae cláusulas, RUTs y montos —cosa típica de un pedido corporativo pesado— su score de contractualidad sube por encima de 55 y el gate lo bautiza "contrato".

### Causa raíz B — El tipo de documento se decide DESPUÉS de admitirlo
El gate de subida (`app.py:43048`) solo llama a `evaluar_contractualidad()`, que devuelve contrato / dudoso / no_contrato. La clasificación fina (mantención vs arriendo vs compraventa vs pedido) ocurre **mucho después**, en el botón Analizar (`mant_contrato_analizar`). Para entonces el documento **ya está guardado en la sección Contratos y ya se le extrajeron datos financieros**.

> Evidencia: `app.py:43048` (gate, solo contractualidad) vs el análisis fino post-insert (mapa: `app.py:45636`, `_tipo_doc_motor = _an.get('tipo_documento')`).

### Causa raíz C — "Mantención gana los empates" + no hay categoría PEDIDO
La clasificación de tipo no tiene keywords de pedido/orden de compra, y ante duda **devuelve "mantención" por defecto**.

> Evidencia: `contrato_reglas.py:269-281` (`_TIPO_KEYWORDS` sin entrada `pedido`/`orden_compra`); `contrato_reglas.py:299-300`: `if mant > 0 and mant >= mejor: return "mantencion"`.

Resultado: el pedido no solo entra como contrato, sino que **se etiqueta como contrato de mantención** y se le leen "monto mensual", "frecuencia" y "vigencia" que en un pedido significan otra cosa (precio total, plazo de pago, vencimiento del crédito).

### El riesgo concreto para ti
- **Operativo:** la ficha del cliente muestra un "contrato de mantención" que no existe → alguien agenda visitas, calcula costeo o promete cobertura sobre un dato falso. Justo lo contrario de "como reloj".
- **Financiero:** un precio total de pedido leído como "monto mensual" infla 12× el ingreso esperado en cualquier dashboard de costeo (tu meta de costeo automático).
- **Confianza:** si Gerencia ve un módulo que confunde un pedido con un contrato, pierde credibilidad lo que estás construyendo.
- **El override empeora el silencio:** si el gate sí lo rechaza, `force_contrato=1` lo deja entrar igual y **no re-valida la extracción** (`app.py:43063-43065`). Queda anotado en el log, pero el dato malo ya está en la ficha.

---

## 2) AGENTE INTELIGENTE — Clasificador de Documentos determinista

**Principio:** el agente NO debe asumir que todo lo subido es contrato. Debe **leer el documento entero, decir QUÉ es, si está FIRMADO, qué CONDICIONES trae, y dar un diagnóstico ligado a lo que ya existe en la base.** Todo determinista (cero IA, cero tokens, cero costo), igual que el motor actual.

La buena noticia: **el 80% ya está construido.** Lo que falta es ordenarlo en un clasificador de 6 tipos y moverlo ANTES del gate.

### 2.1 Tipos de documento a distinguir (las 6 categorías que pediste)

| Tipo | Señales fuertes (keywords/estructura, normalizado sin tildes) |
|---|---|
| **CONTRATO** | "contrato", "las partes", "comparecen", "convienen", ≥3 cláusulas numeradas (PRIMERO/CLÁUSULA X), 2+ RUTs, "vigencia", "plazo de" |
| **PEDIDO / ORDEN DE COMPRA** | **"pedido"**, "nro de pedido", "n° de pedido", "orden de compra", "purchase order", "OC N°", "solicitamos despachar", "condiciones de pago", "centro de costo", "código SAP", número de 8-10 dígitos como folio |
| **COTIZACIÓN** | "cotizacion", "presupuesto", "validez de la oferta", "precio unitario", "no constituye venta" |
| **FACTURA / BOLETA** | "factura electronica", "boleta", "nota de credito", "SII", "timbre electronico", "monto total a pagar", "IVA" |
| **LEVANTAMIENTO** | "levantamiento", "estado del equipo", "anomalias", "serie", "diagnostico de equipos", "informe de visita técnica" |
| **OTRO** | manual, currículum, certificado, acta — ya cubiertos en `_CONTRACT_NEG` |

### 2.2 El cambio de arquitectura clave: clasificar PRE-GATE
Hoy: `evaluar_contractualidad()` → guarda → (después) detecta tipo.
**Propuesta:** una función nueva `clasificar_documento(texto)` que **primero nombra el tipo** y, si NO es contrato, el gate lo desvía a Documentos con su etiqueta correcta — sin extraerle datos de contrato.

```
clasificar_documento(texto) → {
  tipo: 'contrato'|'pedido'|'cotizacion'|'factura'|'levantamiento'|'otro',
  confianza: 0-100,
  por_que: ["titulo dice PEDIDO", "trae N° de pedido 4500865147", ...],
  firma: {...}, condiciones: {...}
}
```

**Esto es ADITIVO y de bajo riesgo (REGLA #4.2):** no borra el gate actual, lo envuelve. `evaluar_contractualidad()` se mantiene; `clasificar_documento()` la llama internamente y le suma la decisión de tipo PRE-GATE.

### 2.3 Detección de FIRMADO
**Ya existe y es honesta** — reusar `detectar_firmas()` tal cual (`contrato_reglas.py:787-831`):
- "firmado electronicamente", "docusign", "firma digital" → **firmado** (`_FIRMA_FUERTE`, 774-777)
- bloque de cierre ("en senal de conformidad", "ante mi", "notario") + 2 RUTs al final → **indeterminado** (firma manuscrita no se ve en OCR; confirmación humana)
- "borrador"/"draft" → **sin_firma**

**Aditivo:** cruzar con los campos de firma que ya guarda la BD (`firma_cliente_url`, `firma_tecnico_url`, `firma_supervisor_url` en `mant_visitas`) para que el diagnóstico diga "documento firmado electrónicamente / pendiente de firma del cliente / firmado pero requiere verificación visual".

### 2.4 Extracción de CONDICIONES (pago, multas, confidencialidad, cumplimiento)
**Parcialmente hecho, completar con una dimensión nueva.** Hoy el motor ya detecta pago (monto/moneda), multas (las excluye del costo real) y términos sensibles ad-hoc en `_TERMINOS_SENSIBLES` (`contrato_reglas.py:574-599`). Lo que falta sistematizar:

| Condición | Patrones de presencia (aditivo a `contrato_reglas.json`) |
|---|---|
| **Pago** | "plazo de pago", "30 dias", "60 dias", "condiciones de pago", "pago contra factura" |
| **Confidencialidad** | "confidencialidad", "informacion confidencial", "secreto comercial", "no divulgar" |
| **Multas / penalidades** | "multa", "penalidad", "interes por mora", "descuento por atraso" |
| **Cumplimiento normativo** | "norma", "ISO", "reglamento", "ley 19.886", "compliance", "código de conducta proveedor" |

**Clave de seguridad:** estas condiciones se extraen para CUALQUIER tipo, pero **monto_mensual / frecuencia / vigencia SOLO se extraen si tipo == 'contrato'**. Hoy `_contrato_extraer_determinista` extrae siempre, sin importar el tipo (`app.py:38386`). Agregar un guard: `if tipo != 'contrato': no asignar monto_mensual/frecuencia_meses` — corrige la causa raíz del bug del pedido.

### 2.5 DIAGNÓSTICO ligado a lo que ya existe en la base
El agente debe cerrar con un párrafo ejecutivo que cruce el documento con las tablas reales:

- **Cliente:** ¿el RUT extraído existe en el ERP / en `mant_clientes`? Si no → "cliente desconocido, verificar".
- **Si es PEDIDO:** ¿hay una cotización ILUS (`mant_cotizaciones`) o una OT (`mant_visitas`) a la que este pedido corresponda? → "Este pedido N° 4500865147 podría cerrar la cotización COT-2026-000XX; ligar para abrir la OT."
- **Si es CONTRATO:** ¿ya hay un contrato vigente del mismo cliente en `mant_contratos`? → evita duplicados; define frecuencia de visitas.
- **Si es LEVANTAMIENTO:** ¿existe `mant_levantamientos` abierto? → sugiere generar cotización.

**Reusa:** `_contrato_extraer_determinista` (`app.py:38386`), `evaluar_contractualidad` / `detectar_firmas` / `leer_clausulas` (`contrato_reglas.py`), `doc_check`/`doc_check_detalle` ya persistidos.
**Se agrega (aditivo):** `clasificar_documento()` (envoltorio PRE-GATE), categoría `pedido`/`orden_compra` en keywords, guard de extracción por tipo, dimensión "condiciones especiales" en el JSON de reglas, y el cruce con ERP/tablas para el diagnóstico.

---

## 3) PIPELINE DOCUMENTAL con ORDEN DE COMPRA

Tu flujo es: **levantamiento → cotización → ORDEN DE COMPRA → N° OT → factura → firmas**, flexible (no todos los pasos siempre; garantía no factura; el contrato va aparte y define frecuencia).

### Lo mejor: casi todo ya está en la base
No hay que inventar el pipeline — **ya existe denormalizado en `mant_visitas`**:

| Paso | Dónde vive hoy | Estado |
|---|---|---|
| Levantamiento | `mant_levantamientos` + `mant_visitas.levantamiento_id` | ✅ existe |
| Cotización | `mant_cotizaciones` (COT-2026-XXXXX) + `mant_visitas.cotizacion_tido/nudo` | ✅ existe |
| **Orden de Compra** | `mant_visitas.oc_numero` / `oc_fecha` / `oc_archivo_url` | ✅ existe (texto libre) |
| N° OT | `mant_visitas.numero_ot` (único) | ✅ existe |
| Factura | `mant_visitas.factura_tido/nudo` (FCV/BLV en ERP) | ✅ existe |
| Firmas | `mant_visitas.firma_cliente/tecnico/supervisor_url` + `mant_ot_signatures` | ✅ existe |
| Tracking | `estado_facturacion` ENUM: sin_cotizar→cotizado→con_oc→facturado→no_aplica | ✅ existe |

**Dónde encaja la OC:** exactamente entre cotización aceptada y factura (`estado_facturacion='con_oc'`). El endpoint `POST /mantenciones/api/visitas/<vid>/oc` ya la liga. **No hay que crear estructura nueva para el caso normal.**

### Mi opinión honesta de tu flujo

**Lo que está bien (no lo toques):**
- El orden conceptual es correcto y es el estándar de la industria. La OC entre cotización y factura es exactamente donde debe ir.
- Que sea flexible (garantía sin factura) ya está soportado: `estado_facturacion='no_aplica'` + `cubierto_por='garantia'`.
- El contrato aparte que define frecuencia: correcto, vive en `mant_contratos`, no en el flujo de la OT.

**Lo que refinaría:**
1. **La OC hoy es texto libre sin metadata** (`oc_numero` VARCHAR + PDF suelto). Para garantías y auditoría te conviene **subir el PDF de la OC al mismo Agente de Documentos** → que lo clasifique como "pedido/OC", extraiga número, monto y condiciones de pago, y **lo ligue automáticamente** a la OT. Así el documento de Clínica Alemana, en vez de romper la sección Contratos, **alimenta el paso OC del pipeline** — que es donde pertenece.
2. **El paso levantamiento→cotización es manual.** Existe la conversión COT→OT (`mant_cotizacion_convertir_ot`) pero no levantamiento→COT. Es la grieta más grande del "como reloj". (Ola 3.)
3. **La garantía no tiene tabla propia.** Hoy es solo `cubierto_por='garantia'`. Para tu caso "OC en el medio sobre todo para garantías", a futuro conviene `mant_garantias` que ligue equipo → cobertura → OC específica. (Ola 3, no urgente.)

**Lo que contradigo (con respeto):**
- **"OC siempre en el medio" — no.** En mantención por contrato muchas visitas NO llevan OC (el contrato ya las cubre). Forzar OC obligatoria rompería esos flujos. El pipeline debe tratar la OC como **opcional según el origen**: contrato → sin OC; trabajo puntual/garantía/cliente nuevo → con OC. El ENUM `estado_facturacion` ya permite esto; solo hay que no volverlo obligatorio.
- **El pedido NO es un "documento del flujo de OT" cualquiera — es la OC del cliente.** Conceptualmente, "pedido de Clínica Alemana" = `oc_numero`. Esto cierra el círculo con la sección 2: el clasificador que detecta "pedido" debe **ofrecer ligarlo como OC de una OT**, no guardarlo como contrato.

---

## 4) ROADMAP en olas — y qué pruebas tú en cada paso

### 🟢 OLA 1 — AHORA, bajo riesgo (1 línea + 1 guard, aditivo)
**Qué se hace:**
1. Agregar `("pedido", "un PEDIDO / ORDEN DE COMPRA", 40)` y `("nro de pedido", ...)` a `_CONTRACT_NEG` (`contrato_reglas.py:680-698`). Una línea, tapa la causa raíz A.
2. Agregar categoría `pedido` a `_TIPO_KEYWORDS` (`contrato_reglas.py:269-281`).
3. Guard en `_contrato_extraer_determinista`: si el tipo no es contrato, no asignar monto_mensual/frecuencia.

**Prueba concreta (tú, Daniel):**
- Sube el PDF del Pedido N° 4500865147 de Clínica Alemana a la sección **Contratos**.
- **Qué debe pasar:** el sistema lo **rechaza** con el mensaje *"Este documento parece un PEDIDO / ORDEN DE COMPRA, no un contrato"* y te ofrece guardarlo en Documentos o ligarlo como OC.
- **Dónde mirar:** el modal de subida (mensaje rojo) y, si lo fuerzas, que **NO aparezca** monto mensual ni frecuencia inventados en la ficha.

### 🟡 OLA 2 — ESTA SEMANA (clasificador PRE-GATE + diagnóstico)
**Qué se hace:**
1. `clasificar_documento(texto)` envolviendo el gate (devuelve tipo + por_qué + confianza) — aditivo, no borra nada.
2. Dimensión "condiciones especiales" en `contrato_reglas.json` (pago/multas/confidencialidad/cumplimiento).
3. Párrafo de **diagnóstico ejecutivo** que cruce el RUT con ERP/`mant_clientes` y diga qué es el documento y qué hacer con él.
4. Disparar `leer_clausulas()` automáticamente (hoy es fallback) para que el contrato real se lea cláusula por cláusula siempre.

**Prueba concreta:**
- Sube 4 documentos distintos: un **contrato** de mantención, el **pedido** de Clínica Alemana, una **cotización** y una **factura**.
- **Qué debe pasar:** cada uno muestra su etiqueta correcta + un párrafo *"Esto es un PEDIDO de [cliente]. Trae condiciones de pago a 30 días y cláusula de confidencialidad. El cliente [existe / NO existe] en el ERP. Acción sugerida: ligarlo como OC de una OT."*
- **Dónde mirar:** el panel del Agente tras "Analizar" — la etiqueta de tipo, las condiciones detectadas y el diagnóstico final.

### 🔵 OLA 3 — MÁS GRANDE (pipeline cerrado y automatizado)
**Qué se hace:**
1. Que el Agente, al detectar un **pedido/OC**, ofrezca **"Ligar como Orden de Compra a la OT N°…"** → escribe `oc_numero`/`oc_fecha`/`oc_archivo_url` y mueve `estado_facturacion` a `con_oc`. Cierra el círculo: el documento que rompía Contratos ahora **alimenta el pipeline correcto**.
2. Endpoint **levantamiento → cotización** automático (rellena items desde los equipos capturados).
3. Tabla `mant_garantias` (equipo → cobertura → OC específica) para tu caso de garantías.

**Prueba concreta:**
- Desde una OT, sube el PDF del pedido del cliente.
- **Qué debe pasar:** el sistema lo reconoce como OC, te muestra el número y la fecha extraídos, y con un clic la liga a la OT; el chip OC se pone naranja con "OC-XXXX" y el estado pasa a "con OC".
- **Dónde mirar:** la pestaña **Finanzas** de la ficha del cliente — la columna OC ahora poblada, sin haber tipeado nada a mano.

---

### Cierre
El bug de Clínica Alemana no es un fallo aislado: es la **misma pieza vista desde dos lados**. El documento que ensucia la sección Contratos es exactamente el que le falta al paso "Orden de Compra" del pipeline. Arreglar el clasificador (Ola 1, hoy, una línea) y luego conectarlo al pipeline (Ola 3) resuelve las dos cosas con una sola idea: **enseñarle al agente a nombrar lo que lee antes de asumir qué es.** Todo aditivo, todo determinista, cero costo de tokens, respetando REGLA #4.2 (nada se borra) y REGLA #4.1 (ERP solo lectura).

> Archivos clave: `contrato_reglas.py:680-698` (gate), `:269-301` (tipo), `:787-831` (firmas), `:574-599` (condiciones) · `app.py:43024-43075` (gate de subida + override), `:38386` (extracción), `:45412-45733` (endpoint Analizar) · pipeline en `mant_visitas` (`oc_numero`/`cotizacion_*`/`factura_*`/`estado_facturacion`/firmas).