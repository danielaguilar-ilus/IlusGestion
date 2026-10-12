"""SALDO por línea de servicio de cada documento de Random (Daniel, 08-oct-2026).

Daniel: «algo bien inteligente para evitar que dos instalaciones se paguen con el mismo saldo». Una línea de servicio
(ZZINSTALACION, ZZMANTENCION…) o de despacho (ZZENVIO) de una factura NO puede contarse como cobro, sumando todas las
OT (no canceladas ni anuladas), por más que su monto. Este módulo es PURO (sin base de datos, sin Flask, sin ERP):
recibe las líneas del documento y lo que ya usan las OT, y devuelve el saldo, el veredicto de un pedido y las acciones
que se ofrecen cuando se excede. Las lecturas (base y ERP en solo lectura, REGLA #4.1) viven en app.py
(_ot_saldo_*), que lo llama.

Reglas del modelo:
  · Dos categorías: «servicio» (todo ZZ que no sea ZZENVIO, igual que _ot_zz_topes_reales) y «despacho» (ZZENVIO).
  · El saldo de una categoría = capacidad (suma de las líneas del documento) − lo usado en OTRAS OT.
  · Una nota de venta dada de baja por su factura NO cuenta doble: la factura hereda lo que usó la nota. Se resuelve
    con la «familia» del documento (la nota y su factura): de una misma OT se toma el MAYOR aporte entre los miembros
    de la familia, nunca la suma.
  · Sin líneas en una categoría (capacidad 0) no hay nada que consumir: ahí el candado de saldo no aplica.
"""

CATEGORIAS = ("servicio", "despacho")
ESTADOS_OT_FUERA = ("cancelada", "anulada")
NOTAS_VENTA = ("NVV", "NVI", "VD", "WEB")
DOCS_COBRO = ("FCV", "FCE", "BLV", "BLE")
ARGUMENTO_MIN = 30   # igual que _OT_AUT_ARGUMENTO_MIN: lo que lee Daniel para autorizar
CATEGORIA_TXT = {"servicio": "instalación o servicio", "despacho": "despacho"}
NO_SERVICIO = ("ZZRETIRO",)   # líneas ZZ que no son un servicio cobrable (igual que _OT_FIN_ZZ_NO_SERVICIO)


def _entero(x):
    try:
        return int(round(float(x or 0)))
    except (TypeError, ValueError):
        return 0


def clp(n):
    """$ 200.000 (sin decimales, punto de miles)."""
    return "$" + "{:,.0f}".format(float(n or 0)).replace(",", ".")


def clave_doc(tido, nudo):
    """Clave canónica de un documento: ('FCV', '11439'); la nota de venta 'NVV'+'VD00010667' y la escrita como
    'VD'+'10667' son la misma: ('VD', '10667'). Vacía → ('', '')."""
    t = (str(tido or "")).strip().upper()
    n = (str(nudo or "")).strip().upper()
    if t == "NVV" and n.startswith("VD"):
        t, n = "VD", n[2:]
    elif t == "NVV" and n.startswith("WEB"):
        t, n = "WEB", n[3:]
    if not n:
        return (t, "")
    return (t, n.lstrip("0") or "0")


def variantes_sql(clave):
    """Las formas en que la base puede guardar ese documento (erp_tido, erp_nudo): la del ERP (con ceros), la de la
    app y la plana. Sirve para armar el WHERE sin traerse toda la tabla."""
    t, n = clave
    if not t or not n:
        return []
    out = []

    def _add(a, b):
        if (a, b) not in out:
            out.append((a, b))
    _add(t, n)
    _add(t, n.zfill(10))
    if t in ("VD", "WEB"):
        _add("NVV", t + n.zfill(8))
        _add("NVV", t + n.zfill(10 - len(t)))
        _add("NVV", t + n)
    return out


def clasificar_lineas(zz_lineas):
    """Líneas ZZ del documento ({sku, descripcion, monto}) → (capacidad, lineas) por categoría."""
    cap = {c: 0 for c in CATEGORIAS}
    lin = {c: [] for c in CATEGORIAS}
    for l in (zz_lineas or []):
        sku = (str(l.get("sku") or "")).strip().upper()
        if not sku.startswith("ZZ") or sku in NO_SERVICIO:
            continue
        cat = "despacho" if sku == "ZZENVIO" else "servicio"
        m = _entero(l.get("monto"))
        cap[cat] += m
        lin[cat].append({"sku": sku, "descripcion": str(l.get("descripcion") or sku)[:180], "monto": m})
    return cap, lin


def consolidar_usos(filas):
    """Une lo que usan las OT sobre los miembros de la familia del documento. `filas`: [{vid, numero_ot, cliente,
    estado, servicio, despacho, clave?}]. Por OT se toma el MAYOR aporte entre los miembros (la nota de venta y su
    factura son el mismo cobro: nunca se suman). Devuelve una lista por OT, ordenada por vid, con los documentos
    por los que esa OT usa el saldo."""
    por_vid = {}
    for f in (filas or []):
        try:
            vid = int(f.get("vid"))
        except (TypeError, ValueError):
            continue
        if str(f.get("estado") or "").lower() in ESTADOS_OT_FUERA:
            continue
        u = por_vid.setdefault(vid, {"vid": vid, "numero_ot": f.get("numero_ot") or "", "cliente": f.get("cliente") or "",
                                     "estado": f.get("estado") or "", "servicio": 0, "despacho": 0, "documentos": []})
        for c in CATEGORIAS:
            u[c] = max(u[c], _entero(f.get(c)))
        k = f.get("clave")
        if k and list(k) not in u["documentos"]:
            u["documentos"].append(list(k))
    return [por_vid[v] for v in sorted(por_vid) if por_vid[v]["servicio"] or por_vid[v]["despacho"]]


def asignar_a_lineas(lineas, usado):
    """Reparte lo usado sobre las líneas en orden (la primera se consume primero) para mostrar el saldo POR LÍNEA."""
    resto = max(0, _entero(usado))
    out = []
    for l in (lineas or []):
        m = max(0, _entero(l.get("monto")))
        u = min(m, resto)
        resto -= u
        out.append(dict(l, usado=u, saldo=m - u))
    return out


def calcular(zz_lineas, usos):
    """Saldo de UN documento. `usos`: salida de consolidar_usos (otras OT). Devuelve
    {servicio: {total, usado, saldo, hay_lineas, excedido, lineas}, despacho: {...}, usos: [...]}."""
    cap, lin = clasificar_lineas(zz_lineas)
    out = {"usos": list(usos or [])}
    for c in CATEGORIAS:
        usado = sum(_entero(u.get(c)) for u in out["usos"])
        total = cap[c]
        out[c] = {"total": total, "usado": usado, "saldo": max(0, total - usado), "hay_lineas": total > 0,
                  "excedido": bool(total > 0 and usado > total), "lineas": asignar_a_lineas(lin[c], usado)}
    return out


def marcar_omitidas(infos):
    """Una nota de venta cuya factura TAMBIÉN está en la misma OT es el mismo cobro: cuenta la factura. Marca la nota
    con `omitido` (texto) y devuelve la lista de los documentos que sí cuentan. `infos`: [{clave, familia, ...}]."""
    claves_cobro = {tuple(i["clave"]) for i in infos if i["clave"][0] in DOCS_COBRO}
    for i in infos:
        if i["clave"][0] in NOTAS_VENTA and any(tuple(f) in claves_cobro for f in i.get("familia") or []):
            i["omitido"] = "Su factura ya está en la OT: el cobro cuenta en la factura."
    return [i for i in infos if i.get("leido", True) and not i.get("otro_cliente") and not i.get("omitido")]


def combinar(saldos):
    """Saldo del conjunto de documentos de UNA OT (la suma de lo que cada uno aún permite). Una categoría sin
    ninguna línea en ningún documento queda con hay_lineas=False (el candado no aplica)."""
    out = {}
    for c in CATEGORIAS:
        hay = any(s[c]["hay_lineas"] for s in saldos)
        out[c] = {"hay_lineas": hay, "total": sum(s[c]["total"] for s in saldos),
                  "usado": sum(s[c]["usado"] for s in saldos), "saldo": sum(s[c]["saldo"] for s in saldos)}
    return out


def evaluar_pedido(saldo, pedido):
    """¿Cabe `pedido` ({servicio: n|None, despacho: n|None}) en el saldo ({servicio: {hay_lineas, saldo}, ...})?
    Devuelve {ok, excesos: [{categoria, pedido, saldo, exceso}], permitido: {categoria: lo máximo que cabe}}."""
    excesos, permitido = [], {}
    for c in CATEGORIAS:
        p = pedido.get(c) if isinstance(pedido, dict) else None
        if p is None:
            continue
        p = _entero(p)
        s = saldo.get(c) or {}
        if not s.get("hay_lineas"):
            permitido[c] = p
            continue
        sal = _entero(s.get("saldo"))
        permitido[c] = min(p, sal)
        if p > sal:
            excesos.append({"categoria": c, "pedido": p, "saldo": sal, "exceso": p - sal})
    return {"ok": not excesos, "excesos": excesos, "permitido": permitido}


def acciones_exceso(excesos, puede_garantia=True):
    """Las salidas que se ofrecen SIEMPRE que un cobro excede el saldo (nunca un callejón sin salida):
    tomar solo el saldo (si hay algo), agregar otra factura, pasar a garantía, pedir autorización con argumento."""
    total_saldo = sum(_entero(e.get("saldo")) for e in (excesos or []))
    acc = []
    if total_saldo > 0:
        acc.append({"tipo": "tomar_saldo", "label": "Tomar solo el saldo (" + clp(total_saldo) + ")",
                    "monto": total_saldo})
    acc.append({"tipo": "ligar_factura", "label": "Agregar otra factura que tenga saldo"})
    if puede_garantia:
        acc.append({"tipo": "pasar_garantia", "label": "Pasar a garantía (no se le cobra al cliente)"})
    acc.append({"tipo": "pedir_autorizacion",
                "label": "Pedir autorización a Daniel (con un argumento de al menos %d caracteres)" % ARGUMENTO_MIN})
    return acc


def texto_exceso(excesos, detalle_docs=None):
    """El mensaje para la persona: qué documento, cuánto tenía, quién lo usó y cuánto saldo queda."""
    partes = []
    for e in (excesos or []):
        partes.append("%s: se quiere cobrar %s y el saldo disponible es %s"
                      % (CATEGORIA_TXT.get(e["categoria"], e["categoria"]), clp(e["pedido"]), clp(e["saldo"])))
    msg = "Ese cobro supera el saldo del documento (" + "; ".join(partes) + ")."
    quienes = []
    for d in (detalle_docs or []):
        for u in (d.get("usos") or []):
            quienes.append("%s%s" % (u.get("numero_ot") or ("OT #%s" % u.get("vid")),
                                     (" (" + u["cliente"] + ")") if u.get("cliente") else ""))
    if quienes:
        msg += " Ya lo usan: " + ", ".join(dict.fromkeys(quienes)) + "."
    msg += " Un mismo servicio de una factura no se puede cobrar en dos OT."
    return msg


def repartir_aportes(docs_caps, servicio, despacho):
    """Reparte lo cobrado por la OT ({servicio, despacho} totales) entre sus documentos EN ORDEN (el principal
    primero), sin pasar la capacidad de cada uno. `docs_caps`: [{clave, servicio, despacho}] con la capacidad de
    cada documento (suma de sus líneas). Lo que ningún documento respalda (un cobro a mano) no se reparte: no es
    plata de ninguna línea. Devuelve [{servicio, despacho}] alineado con `docs_caps`."""
    resto = {"servicio": max(0, _entero(servicio)), "despacho": max(0, _entero(despacho))}
    out = []
    for d in (docs_caps or []):
        fila = {}
        for c in CATEGORIAS:
            tomar = min(resto[c], max(0, _entero(d.get(c))))
            resto[c] -= tomar
            fila[c] = tomar
        out.append(fila)
    return out


def aviso_compartido(titulo_doc, otras):
    """Texto corto de aviso para Regularizar y Facturación de proveedor: esta OT usa un documento que otras OT
    también usan. `otras`: [{numero_ot, cliente}]."""
    if not otras:
        return ""
    nombres = ", ".join(str(o.get("numero_ot") or ("OT #%s" % o.get("vid"))) for o in otras[:4])
    mas = (" y %d más" % (len(otras) - 4)) if len(otras) > 4 else ""
    return ("Usa saldo de %s, que también usan %s%s: revisa que el servicio no se cobre dos veces."
            % (titulo_doc, nombres, mas))


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  📒 LIBRO DE SALDO DE FACTURAS (Daniel, 11-oct-2026, antes de la auditoría del martes)
#  «Si ya utilicé una factura y se ocupó el saldo en otra orden de trabajo, ese saldo tiene que quedar bloqueado…
#  necesito decirle [al auditor] que mi sistema se encarga de eso». Solo la OT consume el saldo (el ticket y la
#  cotización heredan la factura). Cada vez que lo que una OT toma de una factura cambia, queda un MOVIMIENTO que no
#  se borra (consumo o liberación), con quién, cuándo y por qué. Funciones PURAS: las lecturas y escrituras viven en
#  app.py (_fac_libro_*).
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════

ESTADOS_FACTURA = {
    "libre":      {"txt": "Sin usar", "clase": "info",
                   "frase": "Nadie la ha usado todavía: todo su saldo está disponible."},
    "con_saldo":  {"txt": "Con saldo", "clase": "ok",
                   "frase": "Ya se usó una parte: lo que queda se puede usar en otra OT."},
    "agotada":    {"txt": "Saldo agotado", "clase": "agotada",
                   "frase": "Ya se usó completa: una OT nueva con esta factura necesita la autorización de Daniel."},
    "excedida":   {"txt": "Doble cobro", "clase": "rojo",
                   "frase": "Las OT suman más de lo que trae la factura: revisa cuál cobra de más."},
    "sin_lineas": {"txt": "Sin líneas de servicio", "clase": "gris",
                   "frase": "No trae instalación, mantención ni despacho (líneas ZZ): no hay saldo de servicio que controlar."},
    "sin_leer":   {"txt": "Sin leer en Random", "clase": "gris",
                   "frase": "Random no respondió al leerla: se vuelve a revisar sola."},
}
ORDEN_ESTADOS = ("excedida", "agotada", "con_saldo", "libre", "sin_lineas", "sin_leer")


def clave_libro(tido, nudo, categoria):
    """La llave de un saldo en el libro: ('FCV', '11439', 'servicio'). Mismo canon que clave_doc."""
    t, n = clave_doc(tido, nudo)
    return (t, n, categoria)


def aportes_de_ot(filas_usos, vid):
    """Lo que UNA OT toma de cada documento, desde las filas crudas de usos (las mismas que alimentan el saldo):
    {('FCV','11439','servicio'): 50000, ...}. Si por algún motivo dos fuentes traen el mismo documento, se toma el
    mayor (igual que consolidar_usos: nunca se suma dos veces el mismo aporte). Solo montos > 0."""
    try:
        vid = int(vid)
    except (TypeError, ValueError):
        return {}
    out = {}
    for f in (filas_usos or []):
        try:
            if int(f.get("vid")) != vid:
                continue
        except (TypeError, ValueError):
            continue
        if str(f.get("estado") or "").lower() in ESTADOS_OT_FUERA:
            continue
        k = f.get("clave") or ("", "")
        t, n = (k[0], k[1]) if len(k) >= 2 else ("", "")
        if not t or not n:
            continue
        for c in CATEGORIAS:
            m = _entero(f.get(c))
            if m > 0:
                kk = (t, n, c)
                out[kk] = max(out.get(kk, 0), m)
    return out


def movimientos_libro(anterior, actual):
    """Compara lo que una OT tomaba de cada documento (`anterior`, el último acumulado del libro) con lo que toma
    ahora (`actual`) y devuelve los movimientos a registrar, en orden estable:
    [{tido, nudo, categoria, movimiento: 'consumo'|'liberacion', monto (con signo), acumulado}].
    Sube → consumo (+). Baja o desaparece → liberación (−). Sin cambio → nada."""
    anterior = anterior or {}
    actual = actual or {}
    movs = []
    for k in sorted(set(anterior) | set(actual)):
        a = _entero(anterior.get(k))
        b = _entero(actual.get(k))
        if a == b:
            continue
        movs.append({"tido": k[0], "nudo": k[1], "categoria": k[2],
                     "movimiento": "consumo" if b > a else "liberacion", "monto": b - a, "acumulado": b})
    return movs


def estado_factura(servicio, despacho, leido=True):
    """Semáforo de UNA factura a partir de su saldo por categoría ({total, usado} cada una, como lo entrega
    calcular()). Devuelve {estado, txt, clase, frase, total, usado, saldo, pct_usado, exceso}."""
    s = servicio or {}
    d = despacho or {}
    tot = _entero(s.get("total")) + _entero(d.get("total"))
    usado = _entero(s.get("usado")) + _entero(d.get("usado"))
    exceso = sum(max(0, _entero(x.get("usado")) - _entero(x.get("total"))) for x in (s, d) if _entero(x.get("total")) > 0)
    saldo = sum(max(0, _entero(x.get("total")) - _entero(x.get("usado"))) for x in (s, d))
    if not leido:
        est = "sin_leer"
    elif tot <= 0:
        est = "sin_lineas"
    elif exceso > 0:
        est = "excedida"
    elif usado <= 0:
        est = "libre"
    elif saldo <= 0:
        est = "agotada"
    else:
        est = "con_saldo"
    info = ESTADOS_FACTURA[est]
    pct = round(usado * 100.0 / tot, 1) if tot > 0 else 0.0
    return {"estado": est, "txt": info["txt"], "clase": info["clase"], "frase": info["frase"], "total": tot,
            "usado": usado, "saldo": saldo, "pct_usado": pct, "exceso": exceso}


def puede_usar(servicio, despacho, pedido=None):
    """¿Se puede crear una OT con esta factura? Veredicto en palabras para la consulta «¿Puedo usar esta factura?».
    `pedido` opcional ({servicio, despacho}). Devuelve {ok, clase, titulo, texto, disponible: {servicio, despacho}}."""
    s = servicio or {}
    d = despacho or {}
    disp = {"servicio": max(0, _entero(s.get("total")) - _entero(s.get("usado"))),
            "despacho": max(0, _entero(d.get("total")) - _entero(d.get("usado")))}
    hay = _entero(s.get("total")) > 0 or _entero(d.get("total")) > 0
    if not hay:
        return {"ok": True, "clase": "gris", "titulo": "Sin líneas de servicio ni despacho",
                "texto": "Esta factura no trae instalación, mantención ni despacho: no tiene saldo de servicio que "
                         "controlar. Si la OT cobra un servicio, debe salir de otra factura o declararse con su motivo.",
                "disponible": disp}
    if pedido:
        ev = evaluar_pedido({"servicio": dict(s, saldo=disp["servicio"], hay_lineas=_entero(s.get("total")) > 0),
                             "despacho": dict(d, saldo=disp["despacho"], hay_lineas=_entero(d.get("total")) > 0)}, pedido)
        if not ev["ok"]:
            return {"ok": False, "clase": "rojo", "titulo": "No alcanza el saldo",
                    "texto": "; ".join("%s: pides %s y quedan %s" % (CATEGORIA_TXT[e["categoria"]], clp(e["pedido"]), clp(e["saldo"]))
                                       for e in ev["excesos"]) + ". Para cobrar más se necesita la autorización de Daniel.",
                    "disponible": disp}
    if disp["servicio"] <= 0 and disp["despacho"] <= 0:
        return {"ok": False, "clase": "rojo", "titulo": "Saldo agotado",
                "texto": "Esta factura ya se usó completa en otras OT. Una OT nueva con ella necesita la "
                         "autorización de Daniel, con argumento.", "disponible": disp}
    partes = []
    if _entero(s.get("total")) > 0:
        partes.append("%s de instalación o servicio" % clp(disp["servicio"]))
    if _entero(d.get("total")) > 0:
        partes.append("%s de despacho" % clp(disp["despacho"]))
    return {"ok": True, "clase": "ok", "titulo": "Tiene saldo",
            "texto": "Puedes crear una OT que cobre hasta " + " y ".join(partes) + ".", "disponible": disp}


def perdida_cierre(cobre_total, me_cobraron_total, autorizada=0):
    """¿La OT cierra a pérdida? (Daniel: «te va a indicar si la factura va a pérdida y eso iría autorizado por mí»).
    Pérdida = lo que nos cobraron − lo que cobramos, si es positivo. Pasa si no hay pérdida o si la autorización
    aprobada cubre esa pérdida (1 peso de tolerancia por redondeo). Devuelve {perdida, ok, falta_autorizar}."""
    cob = _entero(cobre_total)
    me = _entero(me_cobraron_total)
    per = max(0, me - cob)
    aut = max(0, _entero(autorizada))
    ok = per <= 0 or aut + 1 >= per
    return {"perdida": per, "ok": ok, "falta_autorizar": 0 if ok else per - aut}


class LecturaIncompleta(Exception):
    """El libro no puede afirmar lo que toma una OT (Random no respondió para un documento que lo necesita). Quien
    llama NO anota nada ni guarda la huella: el siguiente barrido lo vuelve a intentar."""


def aportes_reales_ot(ot, docs, caps):
    """📒 2026-10-11 (revisión adversarial): lo que UNA OT toma de verdad de cada documento, para el libro.
    Mismas reglas que las tres fuentes del candado, pero sin contar dos veces lo antiguo:
      · La OT no consume si está cancelada/anulada, en garantía/sin costo o con $0 autorizado.
      · Fuente 1: lo anotado por documento (zz_serv_monto / zz_envio_monto) en filas vigentes (no reemplazadas).
      · Fuente 2 (OT anteriores a esa anotación): el residuo de la OT (zz_monto − lo anotado) se REPARTE entre sus
        documentos de cobro sin anotar, en orden (el principal primero) y sin pasar la capacidad de cada uno
        (repartir_aportes). Un documento sin líneas de una categoría no consume esa categoría.
      · Fuente 3 (OT sin tabla puente): el cobro sale de factura_* hasta su capacidad.
      · Un valor estimado/manual/contrato no es plata de ninguna línea (no consume).
    `ot`: {estado, modalidad_cobro, cobro_cero_motivo, valor_origen, zz_monto, zz_envio_monto, factura_tido,
    factura_nudo}. `docs`: [{tido, nudo, es_cobro, reemplazado, serv, env}] en orden (principal primero; serv/env
    None = sin anotar). `caps`: {clave_doc: {servicio, despacho}} o None si Random no respondió.
    Devuelve {(tido, nudo, categoria): monto}. Lanza LecturaIncompleta si falta una capacidad que se necesita."""
    ot = ot or {}
    if str(ot.get("estado") or "").lower() in ESTADOS_OT_FUERA:
        return {}
    if str(ot.get("modalidad_cobro") or "").lower() in ("garantia", "sin_costo") or ot.get("cobro_cero_motivo"):
        return {}
    out = {}

    def _sumar(clave, cat, m):
        m = _entero(m)
        if m > 0 and clave[0] and clave[1]:
            kk = (clave[0], clave[1], cat)
            out[kk] = max(out.get(kk, 0), m)

    vigentes = [d for d in (docs or []) if not d.get("reemplazado")]
    anotado = {c: 0 for c in CATEGORIAS}
    sin_anotar = []
    vistos = set()
    for d in vigentes:
        k = clave_doc(d.get("tido"), d.get("nudo"))
        if not k[0] or not k[1]:
            continue
        if d.get("serv") is not None or d.get("env") is not None:
            _sumar(k, "servicio", d.get("serv"))
            _sumar(k, "despacho", d.get("env"))
            anotado["servicio"] += _entero(d.get("serv"))
            anotado["despacho"] += _entero(d.get("env"))
        elif d.get("es_cobro") and k not in vistos:
            sin_anotar.append(k)
        vistos.add(k)
    origen = str(ot.get("valor_origen") or "").lower()
    if origen in ("manual", "supuesto", "estimado", "contrato"):
        return out
    resto = {"servicio": max(0, _entero(ot.get("zz_monto")) - anotado["servicio"]),
             "despacho": max(0, _entero(ot.get("zz_envio_monto")) - anotado["despacho"])}
    if not (resto["servicio"] or resto["despacho"]):
        return out
    if not vigentes:
        k = clave_doc(ot.get("factura_tido"), ot.get("factura_nudo"))
        sin_anotar = [k] if (k[0] and k[1]) else []
    if not sin_anotar:
        return out
    filas = []
    for k in sin_anotar:
        cap = (caps or {}).get(k)
        if cap is None:
            raise LecturaIncompleta("%s %s" % k)
        filas.append({"clave": k, "servicio": _entero(cap.get("servicio")), "despacho": _entero(cap.get("despacho"))})
    for f, ap in zip(filas, repartir_aportes(filas, resto["servicio"], resto["despacho"])):
        _sumar(f["clave"], "servicio", ap["servicio"])
        _sumar(f["clave"], "despacho", ap["despacho"])
    return out


def perdida_de_cuenta(cobre_total, me_tecnico, me_despacho, autorizada=0):
    """Pérdida que exige autorización al cerrar (revisión 2026-10-11): lo que nos cobraron por el SERVICIO (técnico +
    despacho) − lo que cobramos. Los repuestos instalados se muestran aparte y no cuentan acá: la factura los cobra
    como producto, no como línea de servicio, y contarlos inventaba pérdidas que solo Daniel podía destrabar.
    Un costo de despacho vacío cuenta como $0: la pérdida calculada es un mínimo (si ya es > 0, existe)."""
    return perdida_cierre(cobre_total, _entero(me_tecnico) + _entero(me_despacho), autorizada)


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  ⚖️ PUNTO DE EQUILIBRIO AL PAGARLE AL PROVEEDOR (Daniel, 12-oct-2026)
#  «Primero evaluamos la factura. Si está repetida, evaluamos cuánto tiene de saldo. Y el límite es el punto de
#  equilibrio, donde no ganemos ni perdamos por el servicio. Si se pasa de allí, no puedes asignarla sin autorización»
#  y «el sistema por ninguna razón debe permitir que paguemos dos veces por un servicio».
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════

def reduccion_por_saldo(aporte_ot, capacidad, usado_antes):
    """Cuánto de lo que una OT dice cobrar de una factura NO tiene respaldo porque otras OT con prioridad (las que ya
    se le pagaron al proveedor y las anteriores) ya usaron ese saldo. 0 si alcanza. Por categoría."""
    disponible = max(0, _entero(capacidad) - _entero(usado_antes))
    return max(0, _entero(aporte_ot) - disponible)


def clasificar_sin_saldo(aporte_ot, capacidad, usado_otras, reduccion, hay_otras):
    """🚩 2026-10-12 (Daniel: «si ingresamos una OT con una factura ya consumida… si no tiene saldo igual es
    sospechoso; hay que avisar hasta en la tabla, persistente»). ¿Esta OT tiene una factura SIN saldo para ella?
    `aporte_ot`: lo que la OT toma de la factura (servicio + despacho); `capacidad` / `usado_otras`: {servicio,
    despacho} de la factura y lo que ya usan las demás OT; `reduccion`: lo que la OT dice cobrar de ella sin saldo
    que lo respalde (punto de equilibrio); `hay_otras`: otras OT la usan.
    Devuelve None (todo bien), 'sin_saldo' (la factura ya estaba consumida: esta OT no recibe nada de ella) o
    'saldo_insuficiente' (le alcanza solo una parte de lo que cobra). Una factura sin líneas de servicio ni despacho
    no tiene saldo que controlar: None."""
    cap = {c: _entero((capacidad or {}).get(c)) for c in CATEGORIAS}
    if not any(cap.values()):
        return None
    ap, red = _entero(aporte_ot), _entero(reduccion)
    if ap > 0 and red > 0:
        return "sin_saldo" if red >= ap else "saldo_insuficiente"
    if ap <= 0 and hay_otras:
        libre = [cap[c] - _entero((usado_otras or {}).get(c)) for c in CATEGORIAS if cap[c] > 0]
        if libre and all(x <= 0 for x in libre):
            return "sin_saldo"
    return None


def equilibrio_pago(pago, cobre, reduccion=0, autorizada=0, exento=False):
    """¿Se le puede pagar `pago` al proveedor por una OT que cobró `cobre`? El cobro que vale es el que la factura
    respalda: `cobre` − `reduccion` (el saldo que otras OT ya usaron). El límite es el punto de equilibrio: pagar
    hasta lo cobrado. Si se pasa, el exceso necesita la autorización de Daniel (la MISMA autorización de pérdida que
    se pide al cerrar la OT, válida hasta el monto que él vio; 1 peso de tolerancia). `exento`: $0 ya autorizado
    (garantía, contrato, interna) — se paga sin cobrar, con permiso previo.
    Devuelve {pago, cobre, cobro_efectivo, reduccion, exceso, ok, falta_autorizar}."""
    p, c, red = _entero(pago), _entero(cobre), _entero(reduccion)
    efectivo = max(0, c - red)
    if exento:
        return {"pago": p, "cobre": c, "cobro_efectivo": efectivo, "reduccion": red, "exceso": 0, "ok": True,
                "falta_autorizar": 0}
    r = perdida_cierre(efectivo, p, autorizada)
    return {"pago": p, "cobre": c, "cobro_efectivo": efectivo, "reduccion": red, "exceso": r["perdida"],
            "ok": r["ok"], "falta_autorizar": r["falta_autorizar"]}


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  🚦 AVISO AL ELEGIR UN DOCUMENTO (Daniel, 12-oct-2026)
#  «La OT le debe avisar al usuario, ya que si no sabe puede duplicar el servicio con un documento con el saldo ya
#  consumido» · «que sea inteligente de reconocer cuando una nota de venta llega a esta [factura] y cuando esa nota de
#  venta hereda el saldo a la factura o a la boleta». Texto en palabras + semáforo, con el MISMO saldo del candado.
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════

def _lista_humana(xs):
    xs = [x for x in xs if x]
    if not xs:
        return ""
    if len(xs) == 1:
        return xs[0]
    return ", ".join(xs[:-1]) + " y " + xs[-1]


def _ot_txt(u):
    return "%s%s" % (u.get("numero_ot") or ("OT #%s" % u.get("vid")), (" (" + u["cliente"] + ")") if u.get("cliente") else "")


def desglose_documento(productos, servicio, despacho):
    """🧾 2026-10-12 (Daniel: «diferenciando la parte de los productos y la otra de los costos, algo bien entendible»).
    Qué trae el documento, separado: productos (equipos, líneas que no son ZZ: no se cobran en la OT), instalación o
    servicio (ZZ) y despacho (ZZENVIO). `productos`: {monto, lineas}; `servicio`/`despacho`: {total, hay_lineas}.
    Devuelve {productos, n_productos, servicio, despacho, texto}."""
    p = productos or {}
    pm, pn = _entero(p.get("monto")), _entero(p.get("lineas"))
    sv, dp = _entero((servicio or {}).get("total")), _entero((despacho or {}).get("total"))
    partes = []
    if pn:
        partes.append("productos %s (%d %s)" % (clp(pm), pn, "línea" if pn == 1 else "líneas"))
    partes.append(("instalación o servicio %s" % clp(sv)) if sv else "sin instalación ni servicio")
    partes.append(("despacho %s" % clp(dp)) if dp else "sin despacho")
    texto = "Este documento trae " + _lista_humana(partes) + "."
    if pn:
        texto += " Los productos son venta de equipos: no se cobran en la OT. La OT cobra solo el servicio y el despacho."
    return {"productos": pm, "n_productos": pn, "servicio": sv, "despacho": dp, "texto": texto}


def aviso_documento(clave, titulo, servicio, despacho, usos, familia, leido=True):
    """El aviso que ve la persona al elegir un documento para una OT.
    `clave`: (tido, nudo) canónica del documento elegido; `titulo`: «FCV 11439»; `servicio`/`despacho`: {total, usado,
    saldo, hay_lineas} (calcular()); `usos`: OT que ya lo usan (consolidar_usos: incluye `documentos`, las claves por
    las que cada OT lo usa, p. ej. la nota de venta); `familia`: [(clave, titulo)] de la nota de venta de origen o de la
    factura/boleta que la reemplaza (Random: MAEDDO.TIDOPA/NUDOPA).
    Devuelve {semaforo: {estado, nivel, titulo, texto}, avisos: [{nivel, texto}], alerta: bool}. `alerta`=True cuando
    hay que interrumpir a la persona (consumido, doble cobro o una nota de venta que ya se facturó)."""
    s = servicio or {}
    d = despacho or {}
    usos = list(usos or [])
    tipo = (clave or ("", ""))[0]
    es_nv = tipo in NOTAS_VENTA
    nombre = "nota de venta" if es_nv else ("boleta" if tipo in ("BLV", "BLE") else "factura")
    cobros = [t for k, t in (familia or []) if k and k[0] in DOCS_COBRO]
    notas = [t for k, t in (familia or []) if k and k[0] in NOTAS_VENTA]
    claves_nv = {tuple(k) for k, _t in (familia or []) if k and k[0] in NOTAS_VENTA}
    quienes = _lista_humana([_ot_txt(u) for u in usos])
    hay = bool(s.get("hay_lineas")) or bool(d.get("hay_lineas"))
    saldo_s, saldo_d = _entero(s.get("saldo")), _entero(d.get("saldo"))
    partes_saldo = []
    if s.get("hay_lineas"):
        partes_saldo.append("%s de instalación o servicio" % clp(saldo_s))
    if d.get("hay_lineas"):
        partes_saldo.append("%s de despacho" % clp(saldo_d))
    excedido = bool(s.get("excedido")) or bool(d.get("excedido"))
    agotado = hay and all(_entero(x.get("saldo")) <= 0 for x in (s, d) if x.get("hay_lineas"))

    if not leido:
        sem = {"estado": "sin_leer", "nivel": "gris", "titulo": "Random no respondió",
               "texto": "No pudimos leer %s en Random: no se puede saber si tiene saldo. Reintenta en unos minutos." % titulo}
    elif not hay:
        sem = {"estado": "sin_lineas", "nivel": "gris", "titulo": "Sin servicio que controlar",
               "texto": "%s no trae instalación, mantención ni despacho (líneas ZZ): no respalda el cobro de un servicio."
                        % titulo}
    elif excedido:
        sem = {"estado": "doble", "nivel": "rojo", "titulo": "Doble cobro",
               "texto": "Las OT que ya usan %s suman más de lo que trae (%s). No lo uses para cobrar otro servicio."
                        % (titulo, quienes or "otras OT")}
    elif agotado:
        sem = {"estado": "consumido", "nivel": "rojo", "titulo": "Saldo consumido",
               "texto": "%s ya se usó completo en %s. Si creas otra OT con este documento, estarías cobrando dos veces el "
                        "mismo servicio: agrega otra factura o pide la autorización de Daniel." % (titulo, quienes or "otra OT")}
    elif usos:
        sem = {"estado": "parcial", "nivel": "ambar", "titulo": "Ya se usó una parte",
               "texto": "%s ya lo usa %s. Quedan %s." % (titulo, quienes, _lista_humana(partes_saldo))}
    else:
        sem = {"estado": "disponible", "nivel": "verde", "titulo": "Disponible",
               "texto": "Nadie ha usado %s. Tiene %s." % (titulo, _lista_humana(partes_saldo))}

    avisos = []
    if es_nv and cobros:
        avisos.append({"nivel": "ambar",
                       "texto": "Esta nota de venta ya se facturó en %s. Usa la %s: hereda lo que ya se usó con esta nota "
                                "(no es un saldo nuevo)." % (_lista_humana(cobros), "factura o boleta" if len(cobros) > 1 else
                                                              ("boleta" if cobros[0].startswith("BL") else "factura"))})
    if not es_nv and notas:
        via_nv = [u for u in usos if any(tuple(k) in claves_nv for k in (u.get("documentos") or []))]
        txt = "Esta %s viene de la nota de venta %s." % (nombre, _lista_humana(notas))
        if via_nv:
            txt += " %s ya la usó con la nota de venta: ese saldo ya está tomado." % _lista_humana([_ot_txt(u) for u in via_nv])
        avisos.append({"nivel": "ambar" if via_nv else "info", "texto": txt})
    alerta = sem["nivel"] == "rojo" or (es_nv and bool(cobros))
    return {"semaforo": sem, "avisos": avisos, "alerta": alerta}
