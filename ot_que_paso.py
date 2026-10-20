# -*- coding: utf-8 -*-
"""ILUS Fitness · OT — «¿QUÉ PASÓ?»: desviaciones de una orden de trabajo (funciones puras, sin BD ni Flask).

Daniel 2026-10-20, con el caso real de Aurum Fit Girls Spa (OT-2026-00171): quedaron 2 accesorios sin instalar porque no llegaron
por falta de stock y la OT no se podía completar. «Así como planteamos en Retiros… ¿pasó algo?» (retiros_desviaciones.py).
Decisiones de Daniel (mismo día):
  · Falta de stock → se CIERRA lo que sí se hizo y se crea una OT HIJA «pendiente», unida a la madre, con la MISMA factura (no se
    cobra dos veces: el documento queda ESPEJADO, no consume saldo, REGLAS #25 y #28) y SIN fecha hasta que llegue el stock.
  · Lo usa el técnico asignado (también el externo) y la gestión; quedan quién, cuándo, motivo y detalle (REGLA #24).
  · Avisos: gestión por la campana y bodega por correo INTERNO. Al cliente jamás se le escribe (REGLA #22).
  · Motivos de una lista; con «Otro motivo» el detalle es obligatorio.

Aquí viven el catálogo, la validación de lo que manda la pantalla, qué equipos se pueden dejar pendientes y los textos. Las rutas, la base,
la hora Chile y los avisos están en app.py (`ot_api_que_paso_*`).

«Sin fecha»: mant_visitas.fecha_programada es NOT NULL y miles de consultas la leen, así que la OT pendiente nace con una fecha CENTINELA
(2099-12-31) y la relación `pendiente_stock`. Cuando alguien la reagenda (el Reagendar de siempre) la fecha cambia y deja de estar «sin
fecha» sola: no hay una columna que sincronizar.
"""
import datetime as _dt

REL_PENDIENTE_STOCK = "pendiente_stock"
FECHA_SIN_FECHA = _dt.date(2099, 12, 31)
ANIO_SIN_FECHA = 2099

ESTADOS_TERMINADOS = ("cerrada", "completada", "cancelada", "anulada")      # igual que _OTPROD_ESTADOS_FIN de app.py
# Lo que la ficha ya trata como SOLO LECTURA (ot_solo_lectura de ot2_detalle): lo terminado, más «pendiente_aprobacion» y la firma del cliente.
ESTADOS_SOLO_LECTURA = ESTADOS_TERMINADOS + ("pendiente_aprobacion",)
ESTADOS_FUERA = ("cancelada", "anulada")
ESTADOS_SIN_EMPEZAR = ("programada", "creada", "asignada", "reagendada")
DETALLE_MAX = 800
DETALLE_MIN_OTRO = 5

# ── Catálogo ──────────────────────────────────────────────────────────────────────────────────────────────────────────────
TIPOS = {
    "falta_stock": {"titulo": "Faltan productos por falta de stock", "icono": "bi-box-seam", "color": "ambar",
                    "ayuda": "Lo que no llegó queda en una OT pendiente, sin fecha, hasta que llegue el stock."},
    "no_se_pudo": {"titulo": "No se pudo hacer el trabajo", "icono": "bi-slash-circle", "color": "rojo",
                   "ayuda": "El cliente no estaba, el equipo llegó dañado, faltó tiempo o el clima lo impidió."},
}

MOTIVOS = {
    "falta_stock": [
        ("falta_stock", "Falta de stock: el producto no llegó a terreno"),
        ("falta_repuesto", "Falta un repuesto para terminar"),
        ("otro", "Otro motivo"),
    ],
    "no_se_pudo": [
        ("cliente_no_estaba", "El cliente no estaba o no hubo acceso"),
        ("equipo_danado", "El equipo llegó dañado"),
        ("falta_tiempo", "Faltó tiempo"),
        ("clima", "Clima o fuerza mayor"),
        ("otro", "Otro motivo"),
    ],
}

ACCIONES = {
    "cerrar_y_pendiente": {"titulo": "Cerrar lo hecho y dejar lo que falta en una OT pendiente",
                           "ayuda": "Los equipos que eliges quedan «falta stock» (ya no frenan la firma). Se crea una OT hija sin fecha, con el "
                                    "mismo documento y sin cobrar de nuevo. La firma y el cierre de esta OT los haces tú después.",
                           "crea_pendiente": True},
    "reagendar": {"titulo": "Registrar y reagendar esta OT",
                  "ayuda": "Queda escrito qué pasó y te llevo al Reagendar de siempre para elegir la fecha nueva.",
                  "crea_pendiente": False},
    "solo_registrar": {"titulo": "Solo registrar lo que pasó",
                       "ayuda": "Queda en la bitácora como evidencia; la OT sigue igual.",
                       "crea_pendiente": False},
}

ACCIONES_POR_TIPO = {
    "falta_stock": ("cerrar_y_pendiente", "solo_registrar"),
    "no_se_pudo": ("reagendar", "solo_registrar"),
}


def catalogo():
    """Lo que necesita la pantalla para armar el formulario (orden incluido). Sin plata ni proveedores."""
    return {
        "tipos": [dict(clave=k, **v) for k, v in TIPOS.items()],
        "motivos": {t: [{"clave": c, "texto": x} for c, x in lista] for t, lista in MOTIVOS.items()},
        "acciones": {k: dict(clave=k, **v) for k, v in ACCIONES.items()},
        "acciones_por_tipo": {k: list(v) for k, v in ACCIONES_POR_TIPO.items()},
        "detalle_max": DETALLE_MAX, "detalle_min_otro": DETALLE_MIN_OTRO,
    }


def texto_motivo(tipo, motivo):
    for c, x in MOTIVOS.get(tipo, []):
        if c == motivo:
            return x
    return ""


def _limpio(v, maximo):
    return " ".join(str(v or "").split())[:maximo]


# ── «Sin fecha» ───────────────────────────────────────────────────────────────────────────────────────────────────────────
def _a_fecha(v):
    if isinstance(v, _dt.datetime):
        return v.date()
    if isinstance(v, _dt.date):
        return v
    try:
        return _dt.datetime.strptime(str(v or "")[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def es_sin_fecha(fecha):
    """¿Esta fecha es la CENTINELA de «todavía sin fecha» (la OT pendiente que espera el stock)?"""
    f = _a_fecha(fecha)
    return bool(f and f.year >= ANIO_SIN_FECHA)


def estado_vigente(estado):
    return str(estado or "").strip().lower() not in ESTADOS_FUERA


def esta_terminada(estado, firma_cliente=False):
    """¿La OT quedó en solo lectura (terminada, esperando la aprobación del cierre o ya firmada por el cliente)?"""
    return bool(firma_cliente) or str(estado or "").strip().lower() in ESTADOS_SOLO_LECTURA


def es_pendiente(fila):
    """¿Esta fila de mant_visitas es una OT pendiente por falta de stock (hija de otra)?"""
    f = fila or {}
    return bool(f.get("ot_madre_id")) and str(f.get("ot_relacion") or "").strip().lower() == REL_PENDIENTE_STOCK


def hija_reutilizable(hijas):
    """De las hijas pendientes de una madre, la que todavía admite más equipos: vigente, sin empezar y todavía sin fecha. Si no hay,
    None (nace una nueva). Una hija ya reagendada o empezada es trabajo en curso: no se le agrega nada por detrás."""
    for h in (hijas or []):
        if not estado_vigente(h.get("estado")):
            continue
        if str(h.get("estado") or "").strip().lower() not in ESTADOS_SIN_EMPEZAR:
            continue
        if any(h.get(k) for k in ("firma_tecnico_at", "hora_real_inicio", "fecha_realizada")):
            continue
        if es_sin_fecha(h.get("fecha_programada")):
            return h
    return None


# ── Qué equipos se pueden dejar pendientes ────────────────────────────────────────────────────────────────────────────────
def candidatos_equipos(filas):
    """Los equipos de la OT que se ofrecen en el formulario.

    `filas`: [{maquina_id, nombre, sku, serie, total, pendientes, estado_pe, baja, fuera_servicio, revision, razon_saltado, en_pendiente}]
      · total / pendientes: tareas del equipo y cuántas siguen sin hacer.
      · estado_pe: la acción vigente del equipo ('falta_stock', 'no_esta', 'no_revisado' o None).
      · en_pendiente: el N° de la OT pendiente que ya lo tiene (o None).
    Devuelve [{maquina_id, nombre, sku, serie, total, pendientes, estado_pe, preseleccionado, elegible, motivo_no}]:
      · un equipo dado de baja, fuera de servicio o con falla detectada NO se ofrece (su salida es otra);
      · uno sin tareas pendientes ya está completo: no hay nada que dejar pendiente;
      · uno que ya está en una OT pendiente se muestra, pero no se puede elegir otra vez;
      · el sistema SUGIERE (preselecciona los que ya están «falta stock») y la persona DECIDE (REGLA #32)."""
    out = []
    for f in (filas or []):
        try:
            mid = int(f.get("maquina_id"))
        except (TypeError, ValueError):
            continue
        razon = str(f.get("razon_saltado") or "")
        if (f.get("baja") or f.get("fuera_servicio") or str(f.get("revision") or "") == "falla_detectada"
                or razon in ("dado_de_baja", "fuera_de_servicio")):
            continue
        total = int(f.get("total") or 0)
        pend = int(f.get("pendientes") or 0)
        ya_en = f.get("en_pendiente")
        elegible, motivo_no = True, ""
        if ya_en:
            elegible, motivo_no = False, "Ya está en la OT pendiente %s." % ya_en
        elif pend <= 0:
            elegible, motivo_no = False, "Ya terminó todas sus tareas: no hay nada pendiente."
        out.append({
            "maquina_id": mid, "nombre": str(f.get("nombre") or "").strip() or "Equipo #%s" % mid,
            "sku": str(f.get("sku") or "").strip(), "serie": str(f.get("serie") or "").strip(),
            "total": total, "pendientes": pend, "estado_pe": f.get("estado_pe") or None,
            "preseleccionado": bool(elegible and f.get("estado_pe") == "falta_stock"),
            "elegible": elegible, "motivo_no": motivo_no,
        })
    return out


# ── Validación ────────────────────────────────────────────────────────────────────────────────────────────────────────────
def validar(datos, estado_ot, candidatos=None, tiene_cliente=True, firma_cliente=False):
    """Revisa lo que manda la pantalla. Devuelve (normalizado, None) o (None, «mensaje para la persona»).
    normalizado = {tipo, motivo, motivo_texto, detalle, accion, crea_pendiente, equipos, avisar_bodega}."""
    d = datos if isinstance(datos, dict) else {}
    if esta_terminada(estado_ot, firma_cliente):
        return None, "La orden ya está terminada: queda en solo lectura."
    tipo = str(d.get("tipo") or "").strip()
    if tipo not in TIPOS:
        return None, "Elige qué pasó."
    motivo = str(d.get("motivo") or "").strip()
    texto = texto_motivo(tipo, motivo)
    if not texto:
        return None, "Elige el motivo de la lista."
    detalle = _limpio(d.get("detalle"), DETALLE_MAX)
    if motivo == "otro" and len(detalle) < DETALLE_MIN_OTRO:
        return None, "Con «Otro motivo», escribe en el detalle qué pasó."
    accion = str(d.get("accion") or "").strip()
    if accion not in ACCIONES_POR_TIPO[tipo]:
        return None, "Elige qué hacer con la orden."
    crea = bool(ACCIONES[accion]["crea_pendiente"])
    equipos = []
    if crea:
        if not tiene_cliente:
            return None, "Esta orden no tiene cliente: no hay equipos que dejar pendientes."
        validos = {c["maquina_id"] for c in (candidatos or []) if c.get("elegible")}
        crudos = d.get("equipos")
        if not isinstance(crudos, list):
            crudos = []
        for x in crudos:
            try:
                mid = int(x)
            except (TypeError, ValueError):
                return None, "Uno de los equipos elegidos no es válido."
            if mid not in validos:
                return None, "Uno de los equipos elegidos ya no se puede dejar pendiente. Recarga y vuelve a elegir."
            if mid not in equipos:
                equipos.append(mid)
        if not equipos:
            return None, "Elige al menos un equipo que quedó pendiente."
    return {
        "tipo": tipo, "motivo": motivo, "motivo_texto": texto, "detalle": detalle, "accion": accion,
        "crea_pendiente": crea, "equipos": equipos,
        "avisar_bodega": bool(d.get("avisar_bodega")) and crea,
    }, None


# ── Textos ────────────────────────────────────────────────────────────────────────────────────────────────────────────────
def resumen_equipos(equipos):
    """«Pulsera (SKU ABC123) · S/N 55; Accesorio (SKU XYZ)» — para la bitácora, la campana y el correo de bodega."""
    partes = []
    for e in (equipos or []):
        t = str(e.get("nombre") or "Equipo").strip()
        if e.get("sku"):
            t += " (SKU %s)" % e["sku"]
        if e.get("serie"):
            t += " · S/N %s" % e["serie"]
        partes.append(t)
    return "; ".join(partes)


def texto_bitacora(n, quien="", equipos=None, numero_pendiente=None):
    """«Faltan productos por falta de stock · Falta de stock… · Acción: … · Equipos: … · OT pendiente: OT-… · Decidió: …»"""
    partes = [TIPOS[n["tipo"]]["titulo"], n["motivo_texto"], "Acción: " + ACCIONES[n["accion"]]["titulo"]]
    if n.get("detalle"):
        partes.append("Detalle: " + n["detalle"])
    eq = resumen_equipos(equipos)
    if eq:
        partes.append("Equipos: " + eq)
    if numero_pendiente:
        partes.append("OT pendiente: " + numero_pendiente)
    if quien:
        partes.append("Decidió: " + quien)
    return " · ".join(partes)[:900]


def titulo_pendiente(numero_hija, numero_madre, cliente=None):
    return ("%s · Pendiente de %s" % (numero_hija, numero_madre) + (" · %s" % cliente if cliente else ""))[:200]


def descripcion_pendiente(numero_madre, motivo_texto, equipos, detalle=None):
    base = "Pendiente por falta de stock de %s: %s." % (numero_madre, resumen_equipos(equipos) or "equipos por definir")
    base += " Motivo: %s." % motivo_texto
    if detalle:
        base += " %s" % detalle
    base += " Sin fecha hasta que llegue el stock."
    return base[:2000]


def chip_pendiente(numero_madre):
    return "Pendiente de %s · falta stock" % (numero_madre or "su OT")


def chip_madre(numero_hija):
    return "Pendiente: %s · falta stock" % (numero_hija or "OT")


def titulo_campana(numero_ot, n_equipos):
    return ("%s: quedó trabajo pendiente por falta de stock" % numero_ot)[:200] if n_equipos else ("%s: ¿qué pasó?" % numero_ot)[:200]


def cuerpo_campana(cliente, equipos, motivo_texto, quien, numero_pendiente=None):
    """Sin plata, sin documentos, sin proveedores (REGLAS #19 y #26)."""
    t = "%s — %s." % (cliente or "Cliente", resumen_equipos(equipos) or "sin equipos")
    t += " Motivo: %s." % motivo_texto
    if numero_pendiente:
        t += " Quedó la OT pendiente %s, sin fecha hasta que llegue el stock." % numero_pendiente
    if quien:
        t += " Lo registró %s." % quien
    return t[:1500]


def json_clasificacion(numero_madre, madre_id, motivo_texto, quien, cuando_txt, detalle=None):
    """Clasificación persistente de la OT pendiente (solo la ve el superadministrador): cómo nació, con quién, cuándo y por qué."""
    return {
        "codigo": "pendiente_stock", "codigos": ["pendiente_stock"],
        "texto": "Pendiente por falta de stock de %s · no cobra aparte" % numero_madre,
        "otros": ["El documento y el saldo son los de %s (no se cobra dos veces)" % numero_madre], "nivel": "ambar",
        "madre_id": madre_id, "madre_numero": numero_madre, "motivo": motivo_texto, "argumento": (detalle or None),
        "derivada": False, "registrado_por": quien, "registrado_at": cuando_txt,
    }
