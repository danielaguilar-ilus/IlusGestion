"""TRAZABILIDAD EN RANDOM de los documentos de una OT (Daniel, 25-oct-2026).

Daniel: «es para agregar inteligencia a los cobros, ya que se les olvida que los servicios se facturan… sugieras el
documento en caso que lo tenga: FCV o BLV es lo que cierra una OT, a menos que pase por la autorización»; «si la ocupé
en otra orden de trabajo, me debe arrojar el saldo y el margen de esa OT… que sepas relacionar los documentos en
Random para diferenciar los saldos y no te metan gato por liebre… nació una nota de venta, se volvió factura o boleta
y después salió el stock con una guía de despacho».

Este módulo es PURO (sin base de datos, sin Flask, sin ERP). app.py (_ot_doc_cadena) hace las lecturas —la base y
Random en SOLO LECTURA (REGLA #4.1)— y le entrega:
  · los documentos de la OT (clave, si es de cobro, si quedó dada de baja y por cuál, si es copia de la OT madre),
  · la familia de cada documento según Random (la nota de venta y la factura o boleta que nació de ella) y si se leyó,
  · lo que se sabe de cada documento (fecha, RUT, razón social, saldo, qué otras OT lo usan),
  · las guías de despacho que salieron de cada uno y el margen de esas otras OT.
Devuelve la cadena por venta, las sugerencias y las alertas, con textos listos para mostrar. Nunca decide nada:
sugiere y alerta (REGLA #32: la persona decide; «alertar, no trabar»).
"""

import datetime as _dt

NOTAS_VENTA = ("NVV", "NVI", "VD", "WEB", "BD")
DOCS_COBRO = ("FCV", "FCE", "BLV", "BLE")
GUIAS = ("GDV",)
CATEGORIAS = ("servicio", "despacho")

_TIPO_TXT = {"FCV": "Factura", "FCE": "Factura exenta", "BLV": "Boleta", "BLE": "Boleta exenta", "GDV": "Guía de despacho"}
MARGEN_SANO = 10.0   # % desde el que el margen se pinta verde (igual que «Margen bajo (menos de 10 %)» del motor)


def _entero(x):
    try:
        return int(round(float(x or 0)))
    except (TypeError, ValueError):
        return 0


def clp(n):
    """$1.411.640 (sin decimales, punto de miles)."""
    return "$" + "{:,.0f}".format(float(n or 0)).replace(",", ".")


def fecha_txt(f):
    """dd/mm/aaaa desde date, datetime o 'AAAA-MM-DD…' (REGLA #6). Vacío si no hay fecha."""
    if not f:
        return ""
    if isinstance(f, (_dt.date, _dt.datetime)):
        return f.strftime("%d/%m/%Y")
    s = str(f).strip()
    try:
        return _dt.date(int(s[0:4]), int(s[5:7]), int(s[8:10])).strftime("%d/%m/%Y")
    except (ValueError, IndexError):
        return s[:10]


def es_nota(clave):
    return bool(clave) and clave[0] in NOTAS_VENTA


def es_cobro(clave):
    return bool(clave) and clave[0] in DOCS_COBRO


def tipo_txt(clave):
    if es_nota(clave):
        return "Nota de venta"
    return _TIPO_TXT.get(clave[0], clave[0])


def titulo(clave):
    """'VD 10414', 'FCV 11745': como se escribe en la app."""
    return f"{clave[0]} {clave[1]}".strip()


def _orden(clave):
    return (0 if es_nota(clave) else 1 if es_cobro(clave) else 2, clave[0], clave[1].zfill(12))


def agrupar(docs_ot, familias):
    """Junta en una misma VENTA los documentos que Random relaciona (una nota de venta y su factura o boleta). Una
    unión simple: si dos documentos comparten algún miembro de familia, son la misma venta. Devuelve una lista de
    conjuntos de claves, cada uno ordenado al armar la cadena. Los documentos de la OT siempre aparecen."""
    grupos = []
    for d in docs_ot:
        k = tuple(d["clave"])
        miembros = {k} | {tuple(x) for x in (familias.get(k) or {}).get("miembros", ())}
        unidos = [g for g in grupos if g & miembros]
        for g in unidos:
            grupos.remove(g)
            miembros |= g
        grupos.append(miembros)
    return grupos


def sugerencias(docs_ot, familias, info=None):
    """Para cada nota de venta de la OT que sigue VIVA (no dada de baja, no copiada de la madre) y cuya familia se
    leyó en Random: las facturas o boletas de esa familia que todavía no están en la OT. Si la OT ya tiene alguna
    factura o boleta de esa misma venta, no se sugiere otra (esa ya es la que cierra)."""
    info = info or {}
    en_ot = {tuple(d["clave"]) for d in docs_ot}
    out = []
    for d in docs_ot:
        k = tuple(d["clave"])
        if not es_nota(k) or d.get("reemplazado_por") or d.get("heredado"):
            continue
        fam = familias.get(k) or {}
        if not fam.get("leida"):
            continue
        cobros = sorted((tuple(x) for x in fam.get("miembros", ()) if es_cobro(tuple(x))), key=_orden)
        if any(c in en_ot for c in cobros):
            continue
        for c in cobros:
            i = info.get(c) or {}
            serv = (i.get("saldo") or {}).get("servicio") or {}
            desp = (i.get("saldo") or {}).get("despacho") or {}
            libre = _entero(serv.get("saldo")) + _entero(desp.get("saldo"))
            total = _entero(serv.get("total")) + _entero(desp.get("total"))
            otra = i.get("mismo_cliente") is False
            partes = [f"La nota de venta {titulo(k)} ya se facturó en Random: {tipo_txt(c).lower()} {titulo(c)}"]
            if i.get("fecha"):
                partes[0] += f" del {fecha_txt(i['fecha'])}"
            if total:
                det = []
                if _entero(serv.get("total")):
                    det.append(f"servicio {clp(serv.get('total'))}")
                if _entero(desp.get("total")):
                    det.append(f"despacho {clp(desp.get('total'))}")
                partes.append(" + ".join(det))
                partes.append(f"saldo libre {clp(libre)}" if libre < total else "saldo completo")
            s = {"nota": titulo(k), "tipo": c[0], "numero": c[1], "titulo": titulo(c), "tipo_txt": tipo_txt(c),
                 "fecha": fecha_txt(i.get("fecha")), "servicio": _entero(serv.get("total")),
                 "despacho": _entero(desp.get("total")), "saldo_libre": libre, "total_servicios": total,
                 "mismo_cliente": i.get("mismo_cliente"), "rut": i.get("rut") or "",
                 "razon_social": i.get("razon_social") or "", "texto": " · ".join(partes),
                 "consumido": bool(total) and libre <= 0,
                 "etiqueta": f"{tipo_txt(c)} de la nota de venta {titulo(k)}"}
            if otra:
                s["aviso_otra_empresa"] = (f"Es de otra empresa (RUT {i.get('rut') or 'sin RUT'}"
                                           + (f", {i['razon_social']}" if i.get("razon_social") else "")
                                           + "): al agregarla tendrás que justificarlo.")
            if s["consumido"]:
                s["aviso_consumido"] = "El saldo de servicio y despacho de este documento ya lo usan otras OT."
            out.append(s)
    return out


def alertas(docs_ot, familias, info=None):
    """Lo que no cuadra con Random, en ámbar y sin bloquear nada:
      · una nota de venta de la OT dada de baja por una factura que NO viene de esa nota en Random (gato por liebre),
      · un documento de cobro de la OT emitido a otra empresa (RUT distinto al del cliente)."""
    info = info or {}
    por_clave = {tuple(d["clave"]): d for d in docs_ot}
    out = []
    for d in docs_ot:
        k = tuple(d["clave"])
        rep = d.get("reemplazado_por")
        if es_nota(k) and rep and not d.get("heredado"):
            rep = tuple(rep)
            fam = familias.get(k) or {}
            if fam.get("leida"):
                cobros = sorted((tuple(x) for x in fam.get("miembros", ()) if es_cobro(tuple(x))), key=_orden)
                if rep not in cobros:
                    if cobros:
                        txt = (f"La {tipo_txt(rep).lower()} {titulo(rep)} dio de baja la nota de venta {titulo(k)}, pero en "
                               f"Random esa nota de venta se facturó con {', '.join(titulo(c) for c in cobros)}. "
                               f"Revisa que la {titulo(rep)} sea la correcta.")
                    else:
                        txt = (f"La {tipo_txt(rep).lower()} {titulo(rep)} dio de baja la nota de venta {titulo(k)}, pero en "
                               f"Random esa nota de venta todavía no tiene factura ni boleta. Revisa que sea la correcta.")
                    out.append({"tipo": "no_viene_de_la_nota", "documento": titulo(rep), "nota": titulo(k), "texto": txt})
        if es_cobro(k) and not d.get("heredado"):
            i = info.get(k) or {}
            if i.get("mismo_cliente") is False:
                out.append({"tipo": "otra_empresa", "documento": titulo(k),
                            "texto": (f"La {tipo_txt(k).lower()} {titulo(k)} está emitida a otra empresa (RUT "
                                      f"{i.get('rut') or 'sin RUT'}" + (f", {i['razon_social']}" if i.get("razon_social") else "")
                                      + ") y no al cliente de esta OT.")})
    # Dos alertas iguales no suman nada.
    vistos, unicas = set(), []
    for a in out:
        if a["texto"] not in vistos:
            vistos.add(a["texto"])
            unicas.append(a)
    return unicas


def margen_nivel(pct):
    """verde (sano) · ambar (bajo, entre 0 y 10 %) · rojo (pérdida real). None si no se puede medir."""
    if pct is None:
        return None
    try:
        p = float(pct)
    except (TypeError, ValueError):
        return None
    if p < 0:
        return "rojo"
    return "verde" if p >= MARGEN_SANO else "ambar"


def texto_uso(u, fin=None):
    """«OT-2026-00249 · Gimnasio X tomó $252.101 de servicio + $50.000 de despacho · quedó con margen 12 %»."""
    fin = fin or {}
    tomo = []
    if _entero(u.get("servicio")):
        tomo.append(f"{clp(u.get('servicio'))} de servicio")
    if _entero(u.get("despacho")):
        tomo.append(f"{clp(u.get('despacho'))} de despacho")
    txt = (u.get("numero_ot") or f"OT {u.get('vid')}") + (f" · {u['cliente']}" if u.get("cliente") else "")
    if tomo:
        txt += " tomó " + " + ".join(tomo)
    if fin.get("mostrar") and fin.get("pct") is not None:
        txt += f" · quedó con margen {float(fin['pct']):.0f} %".replace(".", ",")
    elif fin:
        txt += " · su margen no se puede medir todavía"
    return txt


def _saldo_nodo(i):
    sal = i.get("saldo") or {}
    out = {}
    for c in CATEGORIAS:
        x = sal.get(c) or {}
        if _entero(x.get("total")) or x.get("hay_lineas"):
            out[c] = {"total": _entero(x.get("total")), "usado": _entero(x.get("usado")), "saldo": _entero(x.get("saldo"))}
    if not out:
        return None, "sin_servicios"
    tot = sum(v["total"] for v in out.values())
    lib = sum(max(0, v["saldo"]) for v in out.values())
    estado = "libre" if lib >= tot else ("consumido" if lib <= 0 else "parcial")
    return out, estado


def armar(docs_ot, familias, info=None, guias=None, fins=None):
    """La respuesta completa para la tarjeta: {cadenas, sugerencias, alertas, leido}. Una cadena por venta, con sus
    nodos en orden (nota de venta → factura o boleta → guías de despacho)."""
    info = info or {}
    guias = guias or {}
    fins = fins or {}
    por_clave = {tuple(d["clave"]): d for d in docs_ot}
    cadenas = []
    for g in agrupar(docs_ot, familias):
        nodos = []
        for k in sorted(g, key=_orden):
            d = por_clave.get(k) or {}
            i = info.get(k) or {}
            n = {"clave": list(k), "tipo": k[0], "numero": k[1], "titulo": titulo(k), "tipo_txt": tipo_txt(k),
                 "fecha": fecha_txt(i.get("fecha")), "rut": i.get("rut") or "", "razon_social": i.get("razon_social") or "",
                 "mismo_cliente": i.get("mismo_cliente"), "en_ot": k in por_clave,
                 "dada_de_baja": bool(d.get("reemplazado_por")), "heredado": bool(d.get("heredado")),
                 "leido": i.get("leido", k in info)}
            if d.get("reemplazado_por"):
                n["reemplazado_por"] = titulo(tuple(d["reemplazado_por"]))
            sal, estado = _saldo_nodo(i)
            n["saldo"], n["estado_saldo"] = sal, estado
            usos = []
            for u in (i.get("usos") or []):
                f = fins.get(u.get("vid")) or {}
                usos.append({"vid": u.get("vid"), "numero_ot": u.get("numero_ot") or "", "cliente": u.get("cliente") or "",
                             "url": u.get("url") or "", "servicio": _entero(u.get("servicio")),
                             "despacho": _entero(u.get("despacho")),
                             "margen_pct": (round(float(f["pct"]), 1) if f.get("mostrar") and f.get("pct") is not None else None),
                             "queda": (_entero(f.get("queda")) if f.get("mostrar") else None),
                             "medible": bool(f.get("mostrar")),
                             "nivel": margen_nivel(f.get("pct")) if f.get("mostrar") else None,
                             "texto": texto_uso(u, f)})
            n["usos"] = usos
            nodos.append(n)
        # Las guías van al final de la venta, una vez cada una aunque salgan de la nota y de la factura.
        vistas = set()
        for k in sorted(g, key=_orden):
            for gu in (guias.get(k) or []):
                num = str(gu.get("numero") or "").lstrip("0") or "0"
                if num in vistas:
                    continue
                vistas.add(num)
                prods = gu.get("productos") or []
                nodos.append({"clave": ["GDV", num], "tipo": "GDV", "numero": num, "titulo": f"GDV {num}",
                              "tipo_txt": "Guía de despacho", "fecha": fecha_txt(gu.get("fecha")), "en_ot": False,
                              "desde": titulo(k), "lineas": _entero(gu.get("lineas")) or len(prods),
                              "productos": prods[:12],
                              "texto": (f"Salió el stock el {fecha_txt(gu.get('fecha'))}" if gu.get("fecha") else "Salió el stock")
                                       + (f" ({len(prods)} producto{'s' if len(prods) != 1 else ''})" if prods else "")})
        leida = all((familias.get(k) or {}).get("leida") for k in g if k in por_clave)
        cadenas.append({"nodos": nodos, "leida": leida})
    cadenas.sort(key=lambda c: (0 if any(n.get("en_ot") and not n.get("dada_de_baja") for n in c["nodos"]) else 1))
    return {"cadenas": cadenas, "sugerencias": sugerencias(docs_ot, familias, info),
            "alertas": alertas(docs_ot, familias, info),
            "leido": all(c["leida"] for c in cadenas) if cadenas else True}
