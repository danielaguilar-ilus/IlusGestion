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
    acc.append({"tipo": "ligar_factura", "label": "Ligar otra factura que tenga saldo"})
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
