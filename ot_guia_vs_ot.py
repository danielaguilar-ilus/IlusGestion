# -*- coding: utf-8 -*-
"""LOGÍSTICA (WMS) DE LAS GUÍAS DE DESPACHO DE UNA OT: «qué se fue» y «lo que salió» vs «lo que gestiona la OT».

Daniel (29-oct-2026), sobre la tarjeta «Trazabilidad en Random»: «En la guía de despacho, presiono y no hicimos la
conexión con el WMS, que es la parte logística. Me gustaría verlo, para saber QUÉ SE FUE. Y hacer una comparación de qué
se fue en la guía con los productos que se gestionaron en la OT. Eso es inteligencia y percepción. Y levantar una alerta,
obviamente.» Ejemplos suyos: «la documentación no sacó un producto» o «salió algo que la OT no gestionó».

Este módulo es PURO (sin base de datos, sin Flask, sin ERP, sin red). app.py (_ot_logistica y compañía) hace las lecturas
—Random y CheckWMS en SOLO LECTURA (REGLAS #4.1 y #4.4)— y le entrega:
  · las guías de despacho (GDV) de la venta con TODAS sus líneas (SKU, nombre, cantidad) y si están anuladas,
  · los productos que la OT gestiona (equipos y accesorios, y sus repuestos con SKU),
  · los productos que gestionan OTRAS OT de la misma venta (una instalación partida en dos OT, la OT hija pendiente),
  · lo que Check (bodega) informa de un documento (GetSeguimientoDespacho) y sus movimientos (volcado ya cacheado).
Devuelve la comparación por producto, el veredicto en palabras, los hallazgos que se registran y los textos de la campana.

Reglas (decididas aquí, ajustables):
  · Se empareja por SKU normalizado (mayúsculas, sin espacios ni ceros a la izquierda) y se suman las cantidades.
  · Los servicios (ZZ…, «DE» descuento) no son productos físicos: no se comparan (app.py pasa `_sku_es_servicio`).
  · Sin guía o sin productos gestionados NO hay discrepancia: es «sin datos para comparar». Nunca se inventa.
  · Lo que FALTA por salir y la OT ya declaró que no llegó (falta de stock, REGLA #35, o «No llegó») no es diferencia.
  · Lo que SOBRA y lo gestiona otra OT de la misma venta no es diferencia: se dice cuál OT lo gestiona.
  · Los equipos de una OT solo se esperan en una guía si la OT ENTREGA productos (instalación, cambio de equipo); en una
    mantención los equipos son del cliente y solo cuentan sus repuestos. Ver TIPOS_EQUIPOS_SALEN.
Nada de montos (ni el «valor» que trae Check): esto lo ve la gestión, pero no es plata (REGLAS #19/#26).

2026-10-29 (tarde) — CHECK TIENE LA ÚLTIMA PALABRA. Daniel: «es Check quien nos tiene que dar la ÚLTIMA PALABRA y avisar de
posibles descuadres en caso de que se haya hecho guía y en el WMS no haya salido, pero algo POCO: no quiero ser un analista de
inventario, solamente ver si hay diferencias con la orden de trabajo. Ejemplo: hice una OT por 10 productos, pero en realidad
salieron 8: tiene que haber una alerta.» Y «si hay diferencia, dame una ALERTA, así sea POR CORREO».
Comparación a TRES BANDAS (tres_bandas): la OT ↔ la guía (Random) ↔ Check (lo que bodega expidió). Tres reglas, pocas y sin ruido:
  A  la guía no cuadra con la OT (lo de siempre: códigos de ESTADOS);
  B  Check expidió MENOS de lo que la OT gestiona y la guía dice que salió («la OT gestiona 10; Check informa que salieron 8»);
  C  hay guía y Check no registra que haya salido, pasado el plazo de gracia (GRACIA_DIAS_HABILES desde la fecha de la guía).
Sin información de Check (no respondió, no tiene el documento, no informa ese producto) NO se alerta: no se puede saber. Mientras
bodega prepara (dentro del plazo, con Check todavía esperando expedir) se ve en ámbar pero no se guarda ni suena.
"""
import datetime as _dt
import hashlib
import math
import re

import cl_feriados as _clf            # REGLA #31: la ÚNICA fuente de feriados de Chile (días hábiles del plazo de gracia)
import retiros_check as _rck          # MISMA lectura de las cantidades de Check que Retiros (cubetas / acumulado)
import retiros_check_ot as _rco       # movimientos (OT de Check): quién, cuándo, en qué estado

TIPO_AVISO = "ot_guia_diferencia"     # tipo de la campana (va AL FINAL del ENUM de mant_notificaciones)
CORREO_CLAVE = "ot_guia_diferencia"   # clave del correo interno en comm_correos_registro.py (REGLA #33)
COD_A = ("salio_y_no_esta_en_la_OT", "esta_en_la_OT_y_no_salio", "cantidad_distinta")   # regla A: guía vs OT
COD_B = "check_expidio_menos"         # regla B: Check expidió menos de lo que la OT gestiona (y la guía dice que salió)
COD_C = "guia_sin_salida_check"       # regla C: hay guía y Check no registra la salida, pasado el plazo de gracia
GRACIA_DIAS_HABILES = 3               # días hábiles (L-V sin feriados) desde la fecha de la guía antes de acusar algo a Check
DETALLE_TOTAL = "TOTAL"               # regla B cuando Check no informa por producto: se comparan los totales
TODOS = "*"                           # en `evaluados`: todo lo de ese código quedó revisado en esta lectura
TIPOS_EQUIPOS_SALEN = ("instalacion", "cambio_equipo")
ESTADOS = ("coincide", "salio_y_no_esta_en_la_OT", "esta_en_la_OT_y_no_salio", "cantidad_distinta")
ESTADO_TXT = {
    "coincide": "Coincide",
    "salio_y_no_esta_en_la_OT": "Salió y no está en la OT",
    "esta_en_la_OT_y_no_salio": "Está en la OT y no salió",
    "cantidad_distinta": "Cantidad distinta",
}
DECLARADO_TXT = {"falta_stock": "falta de stock", "no_llego": "«No llegó»"}
MAX_EN_TEXTO = 4                      # productos que se nombran en una frase antes de «y N más»
_EPS = 0.001


# ───────────────────────────── utilidades ─────────────────────────────
def norm_sku(s):
    """' 00123ab ' → '123AB'. Vacío → ''. Un SKU de puros ceros queda '0'."""
    t = "".join(str(s or "").split()).upper()
    if not t:
        return ""
    return t.lstrip("0") or "0"


def cantidad(x):
    """Número ≥ 0 a partir de lo que venga (texto con coma, None, basura → 0)."""
    try:
        f = float(str(x).strip().replace(",", ".")) if x not in (None, "") else 0.0
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(f) or f < 0:
        return 0.0
    return f


def cant_txt(x):
    """2.0 → '2' · 2.5 → '2,5'."""
    f = cantidad(x)
    if abs(f - round(f)) < _EPS:
        return str(int(round(f)))
    return ("%.2f" % f).rstrip("0").rstrip(".").replace(".", ",")


def es_servicio_basico(sku, nombre=""):
    """Respaldo de `_sku_es_servicio` (app.py) para cuando no se pasa: ZZ… y «DE» (descuento) no son productos."""
    s = str(sku or "").strip().upper()
    return s.startswith("ZZ") or s == "DE"


def _plural(n, uno, varios):
    return uno if abs(cantidad(n) - 1) < _EPS else varios


def _lista_nombres(items, con_cantidad=True):
    """«2 × Banco plano, 1 × Mancuerna 10 kg y 3 más»."""
    partes = []
    for it in items[:MAX_EN_TEXTO]:
        q = it.get("q")
        partes.append((cant_txt(q) + " × " if con_cantidad and q is not None else "") + (it.get("nombre") or it.get("sku") or "producto"))
    resto = len(items) - MAX_EN_TEXTO
    if resto > 0:
        partes.append(f"{resto} más")
    if len(partes) <= 1:
        return "".join(partes)
    return ", ".join(partes[:-1]) + " y " + partes[-1]


def titulo_guias(numeros):
    """['33193'] → 'La guía GDV 33193' · ['1', '2'] → 'Las guías GDV 1 y GDV 2'."""
    ns = [f"GDV {n}" for n in numeros]
    if not ns:
        return "La guía"
    if len(ns) == 1:
        return "La guía " + ns[0]
    return "Las guías " + ", ".join(ns[:-1]) + " y " + ns[-1]


# ───────────────────────────── qué gestiona la OT ─────────────────────────────
def armar_gestionados(tipo_ot, equipos, repuestos, es_servicio=None):
    """Lo que la OT GESTIONA y se espera ver salir en una guía → (gestionados, no_comparados).

    `equipos`: [{sku, nombre, cantidad, declarado}] (tareas ∪ equipos de la OT, sin bajas; `declarado` = 'falta_stock' o
    'no_llego' si la OT ya dijo que no llegó). `repuestos`: [{sku, nombre, cantidad}] (mant_visita_repuestos).
    Los equipos solo cuentan si la OT entrega productos (TIPOS_EQUIPOS_SALEN); en otro tipo de OT son equipos del cliente
    y van a `no_comparados` con su razón (se muestran, no se acusan)."""
    es_serv = es_servicio or es_servicio_basico
    tipo = (tipo_ot or "").strip().lower()
    gest, fuera = [], []
    for e in equipos or []:
        it = {"sku": str(e.get("sku") or "").strip(), "nombre": str(e.get("nombre") or "").strip(),
              "cantidad": cantidad(e.get("cantidad") if e.get("cantidad") is not None else 1), "origen": "equipo",
              "declarado": e.get("declarado") or None}
        if es_serv(it["sku"], it["nombre"]):
            continue
        if tipo not in TIPOS_EQUIPOS_SALEN:
            fuera.append(dict(it, razon="equipo_del_cliente"))
        elif not norm_sku(it["sku"]):
            fuera.append(dict(it, razon="sin_sku"))
        else:
            gest.append(it)
    for r in repuestos or []:
        it = {"sku": str(r.get("sku") or "").strip(), "nombre": str(r.get("nombre") or "").strip(),
              "cantidad": cantidad(r.get("cantidad") if r.get("cantidad") is not None else 1), "origen": "repuesto",
              "declarado": None}
        if es_serv(it["sku"], it["nombre"]):
            continue
        if not norm_sku(it["sku"]):
            fuera.append(dict(it, razon="sin_sku"))
        else:
            gest.append(it)
    return gest, fuera


# ───────────────────────────── la comparación ─────────────────────────────
def comparar(guias, gestionados, otras_ot=None, es_servicio=None, no_comparados=None):
    """«Lo que salió» (las guías de despacho de la venta) vs «lo que gestiona la OT».

    `guias`: [{numero, fecha, desde, anulada, lineas: [{sku, nombre, cantidad}]}] (TODAS las líneas, de Random).
    `gestionados`: salida de armar_gestionados. `otras_ot`: [{vid, numero_ot, productos: [{sku, nombre, cantidad}]}] (lo
    que gestionan otras OT de la MISMA venta). Devuelve un dict listo para mostrar (ver claves al final)."""
    es_serv = es_servicio or es_servicio_basico
    guias = [g for g in (guias or []) if isinstance(g, dict)]
    vivas = [g for g in guias if not g.get("anulada")]
    anuladas = [str(g.get("numero") or "") for g in guias if g.get("anulada")]

    salio, servicios, sin_sku_guia = {}, 0, []
    for g in vivas:
        num = str(g.get("numero") or "")
        for l in g.get("lineas") or []:
            if es_serv(l.get("sku"), l.get("nombre")):
                servicios += 1
                continue
            k = norm_sku(l.get("sku"))
            if not k:
                sin_sku_guia.append({"nombre": str(l.get("nombre") or "").strip(), "cantidad": cantidad(l.get("cantidad")), "guia": num})
                continue
            s = salio.setdefault(k, {"q": 0.0, "nombre": "", "sku": str(l.get("sku") or "").strip(), "guias": [], "por_guia": {}})
            q = cantidad(l.get("cantidad"))
            s["q"] += q
            s["por_guia"][num] = s["por_guia"].get(num, 0.0) + q
            if not s["nombre"]:
                s["nombre"] = str(l.get("nombre") or "").strip()
            if num not in s["guias"]:
                s["guias"].append(num)

    gest = {}
    for it in gestionados or []:
        k = norm_sku(it.get("sku"))
        if not k:
            continue
        x = gest.setdefault(k, {"q": 0.0, "declarada": 0.0, "nombre": "", "sku": str(it.get("sku") or "").strip(),
                                "origenes": [], "declarado": None})
        q = cantidad(it.get("cantidad"))
        x["q"] += q
        if it.get("declarado"):
            x["declarada"] += q
            x["declarado"] = x["declarado"] or it.get("declarado")
        if not x["nombre"]:
            x["nombre"] = str(it.get("nombre") or "").strip()
        if it.get("origen") and it["origen"] not in x["origenes"]:
            x["origenes"].append(it["origen"])

    otras = {}
    for o in otras_ot or []:
        for p in o.get("productos") or []:
            k = norm_sku(p.get("sku"))
            if not k:
                continue
            otras.setdefault(k, []).append({"vid": o.get("vid"), "numero_ot": o.get("numero_ot") or f"OT {o.get('vid')}",
                                            "q": cantidad(p.get("cantidad"))})

    base = {"guias": [str(g.get("numero") or "") for g in vivas], "anuladas": anuladas, "servicios_omitidos": servicios,
            "sin_sku": [dict(x, origen=x.get("origen")) for x in (no_comparados or []) if x.get("razon") == "sin_sku"]
                       + [dict(x, origen="guia") for x in sin_sku_guia],
            "equipos_del_cliente": [x for x in (no_comparados or []) if x.get("razon") == "equipo_del_cliente"],
            "productos": [], "no_salio": [], "por_guia": {}, "n_diferencias": 0, "n_coinciden": 0,
            "unidades_salieron": _red(sum(v["q"] for v in salio.values())),
            "unidades_gestionadas": _red(sum(v["q"] for v in gest.values()))}

    if not vivas:
        motivo = "solo_anuladas" if anuladas else "sin_guias"
        txt = ("Sin datos para comparar: las guías de despacho de esta venta están anuladas en Random." if anuladas
               else "Sin datos para comparar: la venta todavía no tiene guía de despacho en Random.")
        return dict(base, estado="sin_datos", motivo=motivo, veredicto=txt, color="gris")
    if not gest:
        txt = "Sin datos para comparar: la OT todavía no gestiona productos con SKU (equipos, accesorios o repuestos)."
        if base["equipos_del_cliente"]:
            txt = ("Sin datos para comparar: los equipos de esta OT son del cliente (no se espera que salgan en una guía) "
                   "y la OT no tiene repuestos con SKU.")
        out = dict(base, estado="sin_datos", motivo="sin_gestionados", veredicto=txt, color="gris")
        out["por_guia"] = {n: {"numero": n, "color": "gris", "n_dif": 0, "texto": txt,
                               "lineas": _lineas_guia_sin_comparar(g, es_serv)} for n, g in
                           ((str(g.get("numero") or ""), g) for g in vivas)}
        return out

    productos = []
    for k in sorted(set(salio) | set(gest), key=lambda kk: ((salio.get(kk) or gest.get(kk))["nombre"].lower(), kk)):
        s = (salio.get(k) or {}).get("q", 0.0)
        gx = gest.get(k) or {}
        gq = gx.get("q", 0.0)
        decl = gx.get("declarada", 0.0)
        nombre = (gx.get("nombre") or (salio.get(k) or {}).get("nombre") or "").strip()
        sku = (gx.get("sku") or (salio.get(k) or {}).get("sku") or k)
        if s > _EPS and gq > _EPS and abs(s - gq) < _EPS:
            estado = "coincide"
        elif s > _EPS and gq <= _EPS:
            estado = "salio_y_no_esta_en_la_OT"
        elif s <= _EPS and gq > _EPS:
            estado = "esta_en_la_OT_y_no_salio"
        else:
            estado = "cantidad_distinta"
        falta = max(gq - s, 0.0)
        sobra = max(s - gq, 0.0)
        falta_expl = min(falta, decl)
        otras_k = otras.get(k) or []
        sobra_expl = min(sobra, sum(o["q"] for o in otras_k))
        es_dif = estado != "coincide" and ((falta - falta_expl) > _EPS or (sobra - sobra_expl) > _EPS)
        guias_k = list((salio.get(k) or {}).get("guias") or [])
        p = {"sku": sku, "sku_norm": k, "nombre": nombre or sku, "salio": _red(s), "gestiona": _red(gq),
             "declarada": _red(decl),     # 2026-10-29: lo que la OT ya dijo que no llegó (no se le pide a Check)
             "falta": _red(falta), "sobra": _red(sobra), "estado": estado, "estado_txt": ESTADO_TXT[estado],
             "es_diferencia": bool(es_dif), "guias": guias_k,
             "por_guia": {n: _red(q) for n, q in ((salio.get(k) or {}).get("por_guia") or {}).items()},
             "origenes": gx.get("origenes") or [],
             "declarado": (gx.get("declarado") if decl > _EPS else None),
             "explicado_por": ([o["numero_ot"] for o in otras_k] if sobra_expl > _EPS else [])}
        p["texto"] = _texto_producto(p, falta_expl, sobra_expl)
        productos.append(p)

    difs = [p for p in productos if p["es_diferencia"]]
    no_salio = [p for p in productos if p["estado"] == "esta_en_la_OT_y_no_salio"]
    falta_global = any(p["es_diferencia"] and p["falta"] > _EPS and p["estado"] == "esta_en_la_OT_y_no_salio" for p in productos)

    por_guia = {}
    for g in vivas:
        num = str(g.get("numero") or "")
        lineas, n_dif = [], 0
        for l in g.get("lineas") or []:
            if es_serv(l.get("sku"), l.get("nombre")):
                continue
            k = norm_sku(l.get("sku"))
            p = next((x for x in productos if x["sku_norm"] == k), None) if k else None
            ln = {"sku": str(l.get("sku") or "").strip(), "nombre": str(l.get("nombre") or "").strip() or (p or {}).get("nombre") or "",
                  "cantidad": _red(cantidad(l.get("cantidad"))),
                  "estado": (p or {}).get("estado") or "sin_sku", "estado_txt": (p or {}).get("estado_txt") or "Sin SKU: no se compara",
                  "es_diferencia": bool((p or {}).get("es_diferencia")), "texto": (p or {}).get("texto") or ""}
            if ln["es_diferencia"]:
                n_dif += 1
            lineas.append(ln)
        color = "ambar" if (n_dif or falta_global) else "verde"
        if n_dif:
            texto = f"{n_dif} {_plural(n_dif, 'diferencia', 'diferencias')} con la OT en esta guía."
            if falta_global:
                texto += " Además, la OT gestiona productos que no salieron en ninguna guía."
        elif falta_global:
            texto = "Lo de esta guía cuadra, pero la OT gestiona productos que no salieron en ninguna guía."
        else:
            texto = "Coincide con la OT: lo que salió en esta guía es lo que la OT gestiona."
        por_guia[num] = {"numero": num, "color": color, "n_dif": n_dif, "texto": texto, "lineas": lineas}

    out = dict(base, productos=productos, no_salio=no_salio, por_guia=por_guia, n_diferencias=len(difs),
               n_coinciden=len([p for p in productos if not p["es_diferencia"]]))
    if difs:
        out.update(estado="diferencias", motivo=None, color="ambar", veredicto=_veredicto(out, salio, gest))
    else:
        out.update(estado="coincide", motivo=None, color="verde", veredicto=_veredicto(out, salio, gest))
    return out


def _red(x):
    """Cantidad redonda para mostrar (2.0 → 2; 2.5 → 2.5)."""
    f = cantidad(x)
    return int(round(f)) if abs(f - round(f)) < _EPS else round(f, 2)


redondo = _red


def _lineas_guia_sin_comparar(g, es_serv):
    return [{"sku": str(l.get("sku") or "").strip(), "nombre": str(l.get("nombre") or "").strip(),
             "cantidad": _red(cantidad(l.get("cantidad"))), "estado": "sin_datos", "estado_txt": "Sin datos para comparar",
             "es_diferencia": False, "texto": ""}
            for l in (g.get("lineas") or []) if not es_serv(l.get("sku"), l.get("nombre"))]


def _texto_producto(p, falta_expl, sobra_expl):
    s, g = cant_txt(p["salio"]), cant_txt(p["gestiona"])
    guias = ", ".join(f"GDV {n}" for n in p["guias"])
    e = p["estado"]
    if e == "coincide":
        t = f"Salió {s} y la OT gestiona {g}."
        if p.get("declarado"):
            t += f" La OT lo había marcado {DECLARADO_TXT.get(p['declarado'], p['declarado'])}, pero la guía lo incluye."
        return t
    if e == "salio_y_no_esta_en_la_OT":
        t = f"Salió {s} en {guias} y la OT no lo gestiona."
    elif e == "esta_en_la_OT_y_no_salio":
        t = f"La OT gestiona {g} y no salió en ninguna guía."
    else:
        t = f"Salió {s} y la OT gestiona {g}: "
        if p["falta"] > _EPS:
            t += f"{_plural(p['falta'], 'falta', 'faltan')} por salir {cant_txt(p['falta'])}."
        else:
            t += f"{_plural(p['sobra'], 'salió', 'salieron')} {cant_txt(p['sobra'])} de más."
    if falta_expl > _EPS:
        t += (f" La OT ya declaró {DECLARADO_TXT.get(p.get('declarado'), 'que no llegó')}"
              + (f" para {cant_txt(falta_expl)}" if falta_expl + _EPS < p["falta"] else "") + ".")
    if sobra_expl > _EPS:
        quien = ", ".join(p["explicado_por"][:3])
        t += (f" Lo gestiona {quien} (misma venta)"
              + (f" ({cant_txt(sobra_expl)} de {cant_txt(p['sobra'])})" if sobra_expl + _EPS < p["sobra"] else "") + ".")
    return t


def _palabra_gestion(gest):
    origenes = {o for v in gest.values() for o in (v.get("origenes") or [])}
    if origenes == {"equipo"}:
        return ("equipo", "equipos")
    if origenes == {"repuesto"}:
        return ("repuesto", "repuestos")
    return ("producto", "productos")


def _veredicto(out, salio, gest):
    """La frase de una línea, como la diría Daniel: «La guía GDV 33193 sacó 1 × Booty Builder Cushioned Belt; la OT
    gestiona 3 equipos: faltan por salir 2 (X, Y)»."""
    salidos = sorted(({"q": v["q"], "nombre": v["nombre"] or v["sku"]} for v in salio.values()), key=lambda x: x["nombre"].lower())
    uno, varios = _palabra_gestion(gest)
    ug = out["unidades_gestionadas"]
    cab = (f"{titulo_guias(out['guias'])} {'sacó' if len(out['guias']) == 1 else 'sacaron'} "
           f"{_lista_nombres(salidos) if salidos else 'solo servicios'}; la OT gestiona {cant_txt(ug)} {_plural(ug, uno, varios)}")
    difs = [p for p in out["productos"] if p["es_diferencia"]]
    if not difs:
        txt = (f"Lo que salió en {titulo_guias(out['guias'])[0].lower()}{titulo_guias(out['guias'])[1:]} coincide con lo que "
               f"gestiona la OT ({cant_txt(ug)} {_plural(ug, uno, varios)}).")
        expl = [p for p in out["productos"] if p["estado"] != "coincide"]
        if expl:
            txt += " Hay diferencias explicadas: " + " ".join(f"{p['nombre']}: {p['texto']}" for p in expl[:MAX_EN_TEXTO])
        return txt
    partes = []
    faltan = [p for p in difs if p["falta"] > _EPS]
    if faltan:
        uf = sum(p["falta"] for p in faltan)
        partes.append(f"{_plural(uf, 'falta', 'faltan')} por salir {cant_txt(uf)} ("
                      + _lista_nombres([{"q": p["falta"], "nombre": p["nombre"]} for p in faltan],
                                       con_cantidad=any(p["falta"] > 1 + _EPS for p in faltan)) + ")")
    no_esta = [p for p in difs if p["estado"] == "salio_y_no_esta_en_la_OT"]
    if no_esta:
        partes.append("salió sin estar en la OT: " + _lista_nombres([{"q": p["salio"], "nombre": p["nombre"]} for p in no_esta]))
    de_mas = [p for p in difs if p["estado"] == "cantidad_distinta" and p["sobra"] > _EPS]
    if de_mas:
        partes.append("salió de más: " + _lista_nombres([{"q": p["sobra"], "nombre": p["nombre"]} for p in de_mas]))
    return cab + ": " + "; ".join(partes) + "."


# ───────────────────────────── hallazgos persistentes y campana ─────────────────────────────
def hallazgos(comp, tb=None):
    """Las diferencias que quedan registradas: una por producto (clave OT + código + SKU) y, con la lectura de Check
    (`tb` = tres_bandas), las de las reglas B (por SKU o «TOTAL») y C (por guía). Sin diferencias → []."""
    out = []
    for p in (comp or {}).get("productos") or []:
        if not p.get("es_diferencia"):
            continue
        t = p.get("texto") or ""
        t = t[:1].lower() + t[1:]                     # después de los dos puntos va en minúscula
        out.append({"codigo": p["estado"], "detalle": (p.get("sku_norm") or norm_sku(p.get("sku")))[:80],
                    "texto": (f"{p['nombre']} ({p['sku']}): {t}" if p.get("sku") and p["sku"] != p["nombre"]
                              else f"{p['nombre']}: {t}")[:500],
                    "guias": ", ".join(f"GDV {n}" for n in p.get("guias") or [])[:200]})
    out.extend(dict(h) for h in ((tb or {}).get("hallazgos") or []))
    return out


def alcance(comp, tb=None):
    """Qué hallazgos guardados se pueden dar por RESUELTOS con esta lectura → (evaluados, todo).
    Los de la regla A (guía vs OT) se revisan siempre que Random respondió. Los de B y C solo si Check se leyó para eso
    (`tb["evaluados"]`): una lectura rápida sin Check, o con Check caído, NUNCA resuelve un descuadre a ciegas.
    `todo` = la OT ya no tiene nada que comparar (sin guía viva o sin productos gestionados): no puede quedar ningún hallazgo."""
    comp = comp or {}
    if comp.get("estado") == "sin_datos":
        return [], True
    return [tuple(e) for e in ((tb or {}).get("evaluados") or [])], False


def plan_hallazgos(actuales, previas, evaluados=None, todo=False):
    """Qué hacer con la tabla de hallazgos (que NUNCA se borra). `actuales`: {(codigo, detalle): hallazgo} de esta lectura;
    `previas`: filas guardadas [{id, codigo, detalle, estado, texto, notificada_el}].
    `evaluados` (salida de alcance): los hallazgos de Check (B y C) que esta lectura SÍ revisó; un (código, "*") = todos los
    de ese código. Lo que no se revisó no se resuelve (Check pudo no responder). Los de la regla A se revisan siempre.
    → {insertar, reabrir, actualizar, resolver, avisar}. Se avisa UNA sola vez por hallazgo (si el aviso anterior no
    alcanzó a salir —notificada_el vacío— se reintenta)."""
    ev = {tuple(e) for e in (evaluados or [])}

    def revisado(k):
        return todo or k[0] in COD_A or k in ev or (k[0], TODOS) in ev

    por_clave = {(r.get("codigo"), r.get("detalle")): r for r in (previas or [])}
    plan = {"insertar": [], "reabrir": [], "actualizar": [], "resolver": [], "avisar": []}
    for k, h in (actuales or {}).items():
        r = por_clave.get(k)
        if r is None:
            plan["insertar"].append(h)
            plan["avisar"].append(h)
            continue
        if r.get("estado") == "resuelta":
            plan["reabrir"].append((r, h))
        elif (r.get("texto") or "") != h.get("texto") or (r.get("guias") or "") != (h.get("guias") or ""):
            plan["actualizar"].append((r, h))
        if not r.get("notificada_el"):
            plan["avisar"].append(h)
    for k, r in por_clave.items():
        if k not in (actuales or {}) and r.get("estado") == "vigente" and revisado(k):
            plan["resolver"].append(r)
    return plan


def _hay_check(lista):
    return any((h.get("codigo") in (COD_B, COD_C)) for h in (lista or []))


def texto_campana(numero_ot, cliente, lista):
    """(título, cuerpo) de la campana a la gestión: tono amable, sin montos y sin culpar a nadie."""
    n = len(lista or [])
    con_check = _hay_check(lista)
    titulo = (f"{numero_ot}: {n} {_plural(n, 'diferencia', 'diferencias')} entre la OT y lo que salió (guía y Check)" if con_check
              else f"{numero_ot}: {n} {_plural(n, 'diferencia', 'diferencias')} entre la guía de despacho y la OT")
    textos = [(h.get("texto") or "").rstrip(" .") for h in (lista or [])][:MAX_EN_TEXTO]
    cuerpo = (f"{cliente + ' · ' if cliente else ''}Al revisar la trazabilidad, lo que salió "
              + ("(según la guía y lo que informa Check) " if con_check else "en la guía ")
              + "no calza del todo con lo que gestiona la OT: " + " · ".join(textos))
    if n > MAX_EN_TEXTO:
        cuerpo += f" · y {n - MAX_EN_TEXTO} más"
    cuerpo += ". Puedes verlo en «Trazabilidad en Random» de la OT: toca la guía de despacho para ver todos los movimientos."
    return titulo[:200], cuerpo[:1500]


def huella(texto):
    """Huella corta de un texto de hallazgo: el correo sale UNA vez por hallazgo y otra vez solo si la diferencia cambia."""
    return hashlib.sha1(" ".join(str(texto or "").split()).encode("utf-8")).hexdigest()[:40]


def texto_correo(numero_ot, cliente, lista):
    """(título, cuerpo) del CORREO INTERNO a la gestión de la OT (Daniel: «si hay diferencia, dame una ALERTA, así sea por
    correo»). Corto y con números («la OT gestiona 10; Check informa que salieron 8»), sin montos ni proveedores, y nunca al
    cliente ni a un técnico (eso lo asegura app.py: solo _hall_destinos, sin técnicos). El cuerpo es texto: la puerta lo escapa."""
    lista = [h for h in (lista or []) if (h.get("texto") or "").strip()]
    n = len(lista)
    titulo = (f"{numero_ot}: {n} {_plural(n, 'diferencia', 'diferencias')} entre la OT y lo que salió de bodega" if _hay_check(lista)
              else f"{numero_ot}: {n} {_plural(n, 'diferencia', 'diferencias')} entre la guía de despacho y la OT")
    lineas = ["• " + (h.get("texto") or "").strip() for h in lista[:12]]
    if n > 12:
        lineas.append(f"• y {n - 12} más (están en la OT)")
    cuerpo = (f"{numero_ot}{' · ' + cliente if cliente else ''}\n\n"
              "Al revisar lo que salió de bodega (la guía de despacho en Random y lo que informa Check), esto no cuadra con lo "
              "que gestiona la orden de trabajo:\n" + "\n".join(lineas) + "\n\n"
              "Míralo en la OT, tarjeta «Trazabilidad en Random»: toca la guía de despacho para ver qué salió y qué no. "
              "Si la diferencia se corrige, el aviso se cierra solo.")
    return titulo[:200], cuerpo[:3000]


# ───────────────────────────── CheckWMS (bodega): qué se fue físicamente ─────────────────────────────
_CAMPOS_SKU = ("codigoMaterial", "codigo", "sku", "material")
_CAMPOS_DESC = ("descripcionMaterial", "descripcion", "nombreMaterial", "material")


def _primero(f, campos, distinto_de=None):
    for c in campos:
        v = f.get(c)
        if v not in (None, "") and str(v).strip() and str(v).strip() != (distinto_de or ""):
            return str(v).strip()
    return ""


def lineas_check(filas, tipo=None, num=None):
    """Las líneas de UN documento según GetSeguimientoDespacho → [{sku, descripcion, pedidas, pickeadas, revisadas,
    despachadas, malo, crudo, emision, entrega}] o [] si Check no lo tiene. Descarta las filas de OTRO documento (igual que
    retiros_check.resumir_filas) y lee las cantidades con la MISMA regla de Retiros (retiros_check._linea). Nunca guarda el
    «valor» que trae Check."""
    out = []
    for f in (filas or []):
        if not isinstance(f, dict):
            continue
        if tipo and f.get("tipoDocumento") not in (None, "") and str(f.get("tipoDocumento")).strip().upper() != str(tipo).strip().upper():
            continue
        if num and f.get("numeroDocumento") not in (None, "") and _rck._digitos(f.get("numeroDocumento")) != _rck._digitos(num):
            continue
        crudo, malo = {}, False
        for c in _rck._CAMPOS:
            v = _rck._num(f.get(c))
            if v is None:
                malo, v = True, 0.0
            crudo[c] = v
        crudo["_malo"] = malo
        pend, adel, asig, rev, desp = _rck._linea(crudo)
        sku = _primero(f, _CAMPOS_SKU)
        out.append({"sku": sku, "descripcion": _primero(f, _CAMPOS_DESC, distinto_de=sku),
                    "pedidas": _red(pend), "pickeadas": _red(adel), "revisadas": _red(rev), "despachadas": _red(desp),
                    "malo": malo, "crudo": crudo,
                    "emision": _rco.formatear_momento(f.get("fechaEmision")) or "",
                    "entrega": _rco.formatear_momento(f.get("fechaEntrega")) or ""})
    return out


def bodega(lineas, fuente="", consultados=None, respondio=True, configurado=True):
    """Lo que Check (bodega) dice de la guía → {estado, color, texto, fuente, consultados, pedidas, pickeadas,
    despachadas, etapas, lineas, emision, entrega}. Sin datos nunca se inventa: «Sin información de bodega (Check)».
    estado: sin_credenciales · sin_conexion · sin_datos · ok."""
    consultados = list(consultados or [])
    base = {"fuente": fuente or "", "consultados": consultados, "pedidas": 0, "pickeadas": 0, "despachadas": 0,
            "etapas": [], "lineas": [], "emision": "", "entrega": ""}
    if not configurado:
        return dict(base, estado="sin_credenciales", color="gris",
                    texto="La conexión con Check (bodega) no está configurada en esta instalación.")
    if not respondio:
        return dict(base, estado="sin_conexion", color="gris",
                    texto="Check no respondió ahora. Intenta de nuevo en un minuto: la ficha sigue igual.")
    if not lineas:
        donde = " ni para ".join(consultados) if consultados else "este documento"
        return dict(base, estado="sin_datos", color="gris",
                    texto=f"Sin información de bodega (Check) para {donde}.")
    ev = _rck.evaluar([{"lineas": [l["crudo"] for l in lineas]}])
    p, k, d = ev.get("pedidas") or 0, ev.get("pickeadas") or 0, ev.get("despachadas") or 0
    malo = any(l.get("malo") for l in lineas)
    if malo:
        color, texto = "gris", "Check devolvió números que no se entienden: no se puede decir qué salió."
    elif p <= 0:
        color, texto = "gris", "Check tiene el documento, pero sin unidades pedidas (o cancelado)."
    elif d + _EPS >= p:
        color, texto = "verde", f"Bodega expidió todo: {cant_txt(d)} de {cant_txt(p)} {_plural(p, 'unidad', 'unidades')}."
    elif d > _EPS:
        color, texto = "ambar", f"Bodega expidió {cant_txt(d)} de {cant_txt(p)} {_plural(p, 'unidad', 'unidades')}."
    elif k > _EPS:
        color, texto = "ambar", f"Bodega juntó {cant_txt(k)} de {cant_txt(p)} {_plural(p, 'unidad', 'unidades')} y todavía no las expide."
    else:
        color, texto = "gris", "Check tiene el documento, pero bodega todavía no lo prepara."
    etapas = [{"clave": e["clave"], "titulo": e["titulo"], "hechas": e["hechas"], "de": e["de"], "completa": e["completa"]}
              for e in ev.get("etapas") or [] if e.get("clave") in ("pedido", "pickeado", "despachado")]
    return dict(base, estado="ok", color=color, texto=texto, pedidas=p, pickeadas=k, despachadas=d, etapas=etapas, malo=malo,
                lineas=[{c: l[c] for c in ("sku", "descripcion", "pedidas", "pickeadas", "despachadas")} for l in lineas],
                emision=next((l["emision"] for l in lineas if l.get("emision")), ""),
                entrega=next((l["entrega"] for l in lineas if l.get("entrega")), ""))


def cruzar_bodega(lineas_guia, bod):
    """A cada línea de la guía le agrega lo que bodega expidió de ese SKU (si Check lo informa): {pedidas, despachadas}."""
    por_sku = {}
    for l in (bod or {}).get("lineas") or []:
        k = norm_sku(l.get("sku"))
        if not k:
            continue
        x = por_sku.setdefault(k, {"pedidas": 0.0, "pickeadas": 0.0, "despachadas": 0.0})
        for c in x:
            x[c] += cantidad(l.get(c))
    out = []
    for l in lineas_guia or []:
        b = por_sku.get(norm_sku(l.get("sku")))
        out.append(dict(l, bodega=({c: _red(v) for c, v in b.items()} if b else None)))
    return out


def semaforo_guia(comp, num, bod=None, pendiente=False, tb=None):
    """La LÍNEA RESUMEN de una guía dentro de «Trazabilidad en Random» (Daniel, 29-oct: «quiero a Check en este reporte…
    todo bien resumido»): un semáforo chico y una frase que junta la guía frente a la OT y lo que bodega (Check) expidió.
      verde = salió todo lo gestionado (la guía cuadra con la OT y bodega lo expidió todo);
      ámbar = falta o sobra algo (en la guía frente a la OT, o bodega no ha expedido todo lo de la guía);
      gris  = sin información de bodega (Check) o sin datos para comparar.
    `bod`: salida de bodega() (None = todavía no se le preguntó a Check; `pendiente` = se está preguntando).
    `tb` (2026-10-29, Check tiene la última palabra): salida de tres_bandas(); si trae esta guía, su parte de bodega manda
    (plazo de gracia, «Check no registra la salida», «Check informa que salieron 8»).
    → {color, texto, comparacion, bodega, pendiente}."""
    tbg = ((tb or {}).get("por_guia") or {}).get(str(num))
    if tbg and bod is not None and bod.get("estado") == "ok":
        base = semaforo_guia(comp, num, None)
        ccol, ctxt = base["comparacion"], base["texto"]
        bcol, btxt = tbg.get("color") or "gris", tbg.get("texto") or ""
        if ccol == "ambar" or bcol == "ambar":
            color = "ambar"
        elif ccol == "verde" and bcol == "verde":
            color, ctxt = "verde", "Salió todo lo gestionado"
        else:
            color = "gris"
        return {"color": color, "texto": ctxt + (" · " + btxt if btxt else ""), "comparacion": ccol, "bodega": bcol,
                "pendiente": False, "check": tbg.get("estado") or ""}
    comp = comp or {}
    pg = (comp.get("por_guia") or {}).get(str(num)) or {}
    ccol = pg.get("color") or ("gris" if comp.get("estado") != "diferencias" else "ambar")
    if comp.get("estado") == "sin_lectura":
        ccol, ctxt = "gris", "Random no respondió"
    elif ccol == "verde":
        # 2026-10-29 (auditoría): «Salió lo que gestiona la OT» contradecía a Check cuando bodega expidió menos; lo que esta
        # mitad afirma es solo el PAPEL (la guía de Random). Lo que salió de verdad lo dice la mitad de Check.
        ctxt = "La guía cuadra con la OT"
    elif ccol == "ambar":
        n = int(pg.get("n_dif") or 0)
        if n:
            ctxt = f"{n} {_plural(n, 'diferencia', 'diferencias')} con la OT"
        else:
            uf = sum(cantidad(p.get("falta")) for p in comp.get("productos") or []
                     if p.get("es_diferencia") and p.get("estado") == "esta_en_la_OT_y_no_salio")
            ctxt = (f"{_plural(uf, 'falta', 'faltan')} por salir {cant_txt(uf)} de lo que gestiona la OT" if uf > _EPS
                    else "La OT gestiona algo que no salió")
    else:
        ctxt = "Sin datos para comparar con la OT"
    # Bodega, medida SOLO con los productos de esta guía (si Check informó la factura completa, se acota a esta guía).
    bcol, btxt = "gris", ""
    if bod is None:
        btxt = "consultando la bodega (Check)…" if pendiente else ""
    elif bod.get("estado") != "ok":
        btxt = {"sin_conexion": "Check no respondió ahora", "sin_credenciales": "sin conexión con Check"}.get(
            bod.get("estado"), "sin información de bodega (Check)")
    else:
        fisicas = [l for l in (pg.get("lineas") or []) if norm_sku(l.get("sku"))]
        if fisicas:
            cruz = cruzar_bodega(fisicas, bod)
            p = sum(cantidad(l.get("cantidad")) for l in cruz)
            d = sum(min(cantidad((l.get("bodega") or {}).get("despachadas")), cantidad(l.get("cantidad"))) for l in cruz)
        else:
            p, d = cantidad(bod.get("pedidas")), cantidad(bod.get("despachadas"))
        if bod.get("malo") or cantidad(bod.get("pedidas")) <= _EPS or p <= _EPS:
            btxt = (bod.get("texto") or "sin información de bodega").rstrip(".")
            btxt = btxt[:1].lower() + btxt[1:]
        elif d + _EPS >= p:
            bcol, btxt = "verde", f"bodega expidió {cant_txt(d)} de {cant_txt(p)}"
        else:
            bcol, btxt = "ambar", f"bodega expidió {cant_txt(d)} de {cant_txt(p)}"
    if ccol == "ambar" or bcol == "ambar":
        color = "ambar"
    elif ccol == "verde" and bcol == "verde":
        color, ctxt = "verde", "Salió todo lo gestionado"
    else:
        color = "gris"
    ctxt = ctxt[:1].upper() + ctxt[1:]
    return {"color": color, "texto": ctxt + (" · " + btxt if btxt else ""), "comparacion": ccol, "bodega": bcol,
            "pendiente": bool(pendiente and bod is None)}


def movimientos(filas_todas, docs, edad_min=None, max_ot=8):
    """Movimientos (OT de Check) de estos documentos, desde el volcado YA leído de GetStockTrazabilidad: quién, cuándo,
    en qué estado y qué UA. `docs`: [(tipo, numero)]. → {estado, ots, edad_min}. estado: ok · sin_datos."""
    ots = []
    for tipo, num in docs or []:
        filas = _rco.filas_del_documento(filas_todas or [], tipo, num)
        for o in _rco.agrupar_por_ot(filas):
            uas = []
            for l in o.get("lineas") or []:
                if l.get("ua") and l["ua"] not in uas:
                    uas.append(l["ua"])
            ots.append({"doc": f"{tipo} {num}", "ot": o.get("ot") or "", "tipo": o.get("tipo") or "",
                        "estado": o.get("estado") or "", "inicio": o.get("inicio") or "", "fin": o.get("fin") or "",
                        "personas": [p["valor"] for p in (o.get("personas") or [])][:4], "uas": uas[:6],
                        "unidades": _red(o.get("unidades") or 0)})
    if not ots:
        return {"estado": "sin_datos", "ots": [], "edad_min": edad_min,
                "texto": "Check no registra movimientos de bodega para este documento (o todavía no llegan al reporte)."}
    vistos, unicas = set(), []
    for o in ots:
        k = (o["doc"], o["ot"])
        if k in vistos:
            continue
        vistos.add(k)
        unicas.append(o)
    return {"estado": "ok", "ots": unicas[:max_ot], "mas": max(len(unicas) - max_ot, 0), "edad_min": edad_min, "texto": ""}


# ═════════════════ 2026-10-29 (tarde): TRES BANDAS — la OT ↔ la guía (Random) ↔ Check (lo que bodega expidió) ═════════════════
# Daniel: «es Check quien nos tiene que dar la ÚLTIMA PALABRA… algo POCO: no quiero ser un analista de inventario, solamente ver
# si hay diferencias con la orden de trabajo. Ejemplo: hice una OT por 10 productos, pero en realidad salieron 8».
def fecha_de(x):
    """'08/10/2026', '2026-10-08', date o datetime → date. None si no se entiende (nunca se adivina)."""
    if isinstance(x, _dt.datetime):
        return x.date()
    if isinstance(x, _dt.date):
        return x
    s = str(x or "").strip()
    try:
        m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})", s)
        if m:
            return _dt.date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        m = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})", s)
        if m:
            return _dt.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None
    return None


def fecha_txt(d):
    return d.strftime("%d/%m/%Y") if d else ""


def vence_el(fecha, dias=GRACIA_DIAS_HABILES):
    """El día hábil número `dias` DESPUÉS de la fecha de la guía (lunes a viernes sin feriados de Chile: cl_feriados, REGLA
    #31). Desde ese día, si Check sigue sin expedir, se alerta. Guía del jueves 08/10/2026 → miércoles 14/10/2026 (el lunes 12
    es feriado). None si la fecha no se entiende."""
    f = fecha_de(fecha)
    if not f:
        return None
    d, n = f, 0
    for _ in range(60):
        d += _dt.timedelta(days=1)
        if _clf.es_dia_habil(d):
            n += 1
            if n >= dias:
                return d
    return d


def en_plazo(fecha, hoy, dias=GRACIA_DIAS_HABILES):
    """¿La guía sigue dentro del plazo de gracia (bodega puede estar preparando)? Sin fecha o sin «hoy» → True: no se acusa
    a ciegas."""
    v, h = vence_el(fecha, dias), fecha_de(hoy)
    if not v or not h:
        return True
    return h < v


def check_por_sku(bodegas):
    """Lo que Check dice que salió, por SKU, juntando las guías SIN contar dos veces un mismo documento de Check (dos guías
    que salieron de la misma factura y Check solo tiene la factura: se cuenta una vez). Si Check informa por la guía Y por la
    factura, por SKU se toma el MAYOR de los dos (nunca la suma: sería el mismo producto contado dos veces).
    → {por_sku: {sku_norm: {pedidas, despachadas}}, sin_sku: bool (alguna línea de Check sin código → se compara el total),
       pedidas, despachadas (totales), fuentes} o None si Check no informó nada usable."""
    fuentes = {}
    for num, b in (bodegas or {}).items():
        if not b or b.get("estado") != "ok" or b.get("malo"):
            continue
        fuentes.setdefault(b.get("fuente") or f"GDV {num}", b)
    if not fuentes:
        return None
    grupos, tot, sin_sku = {"guia": {}, "otro": {}}, {"guia": [0.0, 0.0], "otro": [0.0, 0.0]}, False
    for f, b in fuentes.items():
        g = "guia" if str(f).upper().startswith("GDV ") else "otro"
        for l in b.get("lineas") or []:
            p, d = cantidad(l.get("pedidas")), cantidad(l.get("despachadas"))
            tot[g][0] += p
            tot[g][1] += d
            k = norm_sku(l.get("sku"))
            if not k:
                sin_sku = True
                continue
            x = grupos[g].setdefault(k, {"pedidas": 0.0, "despachadas": 0.0})
            x["pedidas"] += p
            x["despachadas"] += d
    por = {}
    for k in set(grupos["guia"]) | set(grupos["otro"]):
        a, b = grupos["guia"].get(k), grupos["otro"].get(k)
        por[k] = dict(a if (a and (not b or a["despachadas"] >= b["despachadas"])) else b)
    tp = tot["guia"] if tot["guia"][1] >= tot["otro"][1] else tot["otro"]
    return {"por_sku": por, "sin_sku": sin_sku, "pedidas": tp[0], "despachadas": tp[1], "fuentes": sorted(fuentes)}


def _txt_bodega_no_ok(b):
    est = (b or {}).get("estado")
    if est == "sin_conexion":
        return "Check no respondió ahora"
    if est == "sin_credenciales":
        return "sin conexión con Check"
    return "sin información de bodega (Check)"


def _quien(p):
    return f"{p['nombre']} ({p['sku']})" if p.get("sku") and p["sku"] != p.get("nombre") else (p.get("nombre") or p.get("sku") or "Producto")


def tres_bandas(comp, bodegas, fechas=None, hoy=None, dias=GRACIA_DIAS_HABILES):
    """La OT ↔ la guía (Random) ↔ Check. Check tiene la última palabra sobre lo que SALIÓ de bodega.
    `comp`: salida de comparar(). `bodegas`: {número de guía: salida de bodega()} — solo las guías a las que se le preguntó a
    Check. `fechas`: {número: 'dd/mm/aaaa'} de las guías (para el plazo de gracia). `hoy`: date en hora Chile.
    → {estado, modo, por_guia: {num: {estado, color, texto, vence, en_plazo, pedidas, salieron, sin_info}},
       productos: {sku_norm: {ot, guia, check, estado}}, hallazgos: [reglas B y C], evaluados: [(código, detalle)]}.
    Estados de una guía frente a Check: sin_info (gris: no se puede saber, no se alerta) · completo (verde) · parcial (ámbar)
    · preparando (ámbar, dentro del plazo: no se guarda ni suena) · sin_salida (ámbar + regla C)."""
    comp = comp or {}
    fechas = {str(k): v for k, v in (fechas or {}).items()}
    out = {"estado": "sin_datos", "modo": "", "por_guia": {}, "productos": {}, "hallazgos": [], "evaluados": []}
    if comp.get("estado") not in ("coincide", "diferencias"):
        return out                         # sin guía, anuladas o sin productos gestionados: nada que pedirle a Check
    out["estado"] = "ok"
    bodegas = {str(k): v for k, v in (bodegas or {}).items() if v}
    vivas = [str(n) for n in comp.get("guias") or []]
    por_guia_comp = comp.get("por_guia") or {}
    out["evaluados"] += [(COD_C, str(n)) for n in comp.get("anuladas") or []]   # una guía anulada ya no espera nada de Check

    # ── Regla C y el semáforo de bodega, guía por guía ──
    con_fisicas = []
    for num in vivas:
        fisicas = [l for l in (por_guia_comp.get(num) or {}).get("lineas") or [] if norm_sku(l.get("sku"))]
        if fisicas:
            con_fisicas.append(num)
        b = bodegas.get(num)
        if b is None:
            continue                       # no se le preguntó a Check por esta guía: nada se concluye
        vence = vence_el(fechas.get(num), dias)
        plazo = en_plazo(fechas.get(num), hoy, dias)
        info = {"estado": "sin_info", "color": "gris", "texto": "", "vence": fecha_txt(vence), "en_plazo": plazo,
                "pedidas": 0, "salieron": 0, "sin_info": 0}
        if b.get("estado") != "ok":
            info["texto"] = _txt_bodega_no_ok(b)
        elif b.get("malo") or cantidad(b.get("pedidas")) <= _EPS:
            t = (b.get("texto") or "sin información de bodega").rstrip(".")
            info["texto"] = t[:1].lower() + t[1:]
        elif not fisicas:
            info["texto"] = "la guía no trae productos con código para comparar con Check"
        else:
            cruz = cruzar_bodega(fisicas, b)
            con = [l for l in cruz if l.get("bodega")]
            sin_sku_check = any(not norm_sku(l.get("sku")) for l in b.get("lineas") or [])
            if con:
                p = sum(cantidad(l.get("cantidad")) for l in con)
                d = sum(min(cantidad(l["bodega"].get("despachadas")), cantidad(l.get("cantidad"))) for l in con)
                faltan = len(cruz) - len(con)
            elif sin_sku_check:            # Check no informa por producto: se mira el total de su documento
                p = sum(cantidad(l.get("cantidad")) for l in fisicas)
                d = min(cantidad(b.get("despachadas")), p)
                faltan = 0
            else:
                p = d = 0.0
                faltan = len(cruz)
            if p <= _EPS:
                info["texto"] = "Check no informa los productos de esta guía"
            else:
                nota = f" (Check no informa {faltan} {_plural(faltan, 'producto', 'productos')})" if faltan else ""
                info.update(pedidas=_red(p), salieron=_red(d), sin_info=faltan)
                if d + _EPS >= p:
                    info.update(estado="completo", color=("gris" if faltan else "verde"),
                                texto=f"bodega expidió {cant_txt(d)} de {cant_txt(p)}" + nota)
                elif d > _EPS:
                    info.update(estado="parcial", color="ambar", texto=f"bodega expidió {cant_txt(d)} de {cant_txt(p)}" + nota)
                elif plazo:
                    info.update(estado="preparando", color="ambar",
                                texto=f"bodega expidió 0 de {cant_txt(p)}, todavía en plazo"
                                      + (f" (hasta el {fecha_txt(vence)})" if vence else ""))
                else:
                    info.update(estado="sin_salida", color="ambar",
                                texto="Check no registra la salida" + (f" (plazo vencido el {fecha_txt(vence)})" if vence else ""))
                    f_txt = fecha_txt(fecha_de(fechas.get(num)))
                    out["hallazgos"].append({
                        "codigo": COD_C, "detalle": num[:80], "guias": f"GDV {num}",
                        "texto": (f"Hay guía GDV {num}" + (f" del {f_txt}" if f_txt else "")
                                  + f" y Check no registra que haya salido ({cant_txt(p)} {_plural(p, 'unidad', 'unidades')}"
                                  + (f"; el plazo de {dias} días hábiles venció el {fecha_txt(vence)}" if vence else "")
                                  + ").")[:500]})
                out["evaluados"].append((COD_C, num))
        out["por_guia"][num] = info
    if con_fisicas and all((COD_C, n) in out["evaluados"] for n in con_fisicas):
        out["evaluados"].append((COD_C, TODOS))

    # ── Regla B: Check expidió MENOS de lo que la OT gestiona (y la guía dice que salió) ──
    cs = check_por_sku({n: bodegas[n] for n in vivas if n in bodegas})
    out["modo"] = ("por_total" if cs and cs["sin_sku"] else "por_producto") if cs else ""

    def guias_ok(gs):
        return bool(gs) and all((bodegas.get(str(g)) or {}).get("estado") == "ok" for g in gs)

    def todas_sin_salida(gs):
        return bool(gs) and all((out["por_guia"].get(str(g)) or {}).get("estado") == "sin_salida" for g in gs)

    def alguna_en_plazo(gs):
        return any((out["por_guia"].get(str(g)) or {}).get("en_plazo", True) for g in gs)

    esperados, b_todo = [], True
    for p in comp.get("productos") or []:
        k = p.get("sku_norm") or norm_sku(p.get("sku"))
        ot = max(cantidad(p.get("gestiona")) - cantidad(p.get("declarada")), 0.0)
        s = cantidad(p.get("salio"))
        esperado = min(ot, s)
        fila = {"ot": _red(ot), "guia": _red(s), "check": None, "estado": "no_aplica"}
        out["productos"][k] = fila
        if esperado <= _EPS:
            continue                       # nada que Check deba expedir (lo que falta en la guía es la regla A)
        gs = [str(g) for g in p.get("guias") or []]
        if not cs or not guias_ok(gs):
            fila["estado"] = "sin_info"
            b_todo = False
            continue
        esperados.append((p, k, ot, s, esperado, gs))
        if cs["sin_sku"]:
            fila["estado"] = "por_total"
            continue
        c = cs["por_sku"].get(k)
        if c is None:
            fila["estado"] = "sin_info"    # Check no informa este producto: no se puede saber
            b_todo = False
            continue
        chk = cantidad(c["despachadas"])
        fila["check"] = _red(chk)
        out["evaluados"].append((COD_B, k[:80]))
        if chk + _EPS >= esperado:
            fila["estado"] = "salio"
        elif todas_sin_salida(gs):
            fila["estado"] = "sin_salida"  # ya lo dice la regla C de su guía: no se acusa dos veces
        elif chk + _EPS >= cantidad(c["pedidas"]) or not alguna_en_plazo(gs):
            fila["estado"] = "menos"
            dec = cantidad(p.get("declarada"))
            t = (f"la OT gestiona {cant_txt(ot)}" + (f" y la guía dice {cant_txt(s)}" if abs(s - ot) > _EPS else "")
                 + f"; Check informa que {_plural(chk, 'salió', 'salieron')} {cant_txt(chk)}"
                 + (f" (la OT ya declaró que no llegó {cant_txt(dec)})" if dec > _EPS else "") + ".")
            out["hallazgos"].append({"codigo": COD_B, "detalle": k[:80], "texto": f"{_quien(p)}: {t}"[:500],
                                     "guias": ", ".join(f"GDV {g}" for g in gs)[:200]})
        else:
            fila["estado"] = "preparando"  # Check todavía espera expedir y la guía está dentro del plazo
    if cs and cs["sin_sku"] and esperados:
        # Check no informa por producto: se comparan los TOTALES (lo dice el texto, para no confundir).
        gs_all = sorted({g for e in esperados for g in e[5]})
        esp_t = sum(e[4] for e in esperados)
        chk_t = cantidad(cs["despachadas"])
        out["evaluados"].append((COD_B, DETALLE_TOTAL))
        if chk_t + _EPS < esp_t and not todas_sin_salida(gs_all) and (chk_t + _EPS >= cantidad(cs["pedidas"])
                                                                      or not alguna_en_plazo(gs_all)):
            out["hallazgos"].append({
                "codigo": COD_B, "detalle": DETALLE_TOTAL, "guias": ", ".join(f"GDV {g}" for g in gs_all)[:200],
                "texto": (f"La OT gestiona {cant_txt(esp_t)} {_plural(esp_t, 'producto', 'productos')}; Check informa que "
                          f"{_plural(chk_t, 'salió', 'salieron')} {cant_txt(chk_t)} (Check no informa por producto: se "
                          f"comparan los totales).")[:500]})
    if cs and b_todo:
        out["evaluados"].append((COD_B, TODOS))
    return out


def detalle_guia(comp, num, tb=None, bod=None):
    """Lo que se ve al tocar la guía (Daniel: «quiero que me diga TODOS LOS MOVIMIENTOS: qué se entregó y qué no se entregó,
    y si hay DIFERENCIA con la orden de trabajo, que es lo que me interesa»):
      diferencias: [{codigo, regla, texto}] — lo que no cuadra con la OT, para mostrar ARRIBA ([] = coincide);
      productos: una fila por producto de esta guía y por lo que la OT gestiona y no salió en NINGUNA guía, con lo que dice
                 cada banda (OT · guía · Check) y qué pasó en bodega, en palabras;
      check: el semáforo de bodega de esta guía (tres_bandas). Sin montos."""
    comp, num = comp or {}, str(num)
    pg = (comp.get("por_guia") or {}).get(num) or {}
    tbg = ((tb or {}).get("por_guia") or {}).get(num) or {}
    prods = {p.get("sku_norm"): p for p in comp.get("productos") or []}
    cruz = {}
    if bod and bod.get("estado") == "ok":
        for l in cruzar_bodega(pg.get("lineas") or [], bod):
            k = norm_sku(l.get("sku"))
            if k and l.get("bodega"):
                cruz[k] = l["bodega"]
    filas = []
    for l in pg.get("lineas") or []:
        k = norm_sku(l.get("sku"))
        p = prods.get(k) or {}
        q = cantidad(l.get("cantidad"))
        b = cruz.get(k) if k else None
        chk = min(cantidad(b.get("despachadas")), q) if b else None
        if not k:
            ctxt, ccol = "Sin código: no se compara con Check", "gris"
        elif b is None:
            ctxt, ccol = "Check no informa este producto", "gris"
        elif chk + _EPS >= q:
            ctxt, ccol = f"Salió de bodega ({cant_txt(chk)} de {cant_txt(q)}, según Check)", "verde"
        elif chk > _EPS:
            ctxt, ccol = f"Según Check {_plural(chk, 'salió', 'salieron')} {cant_txt(chk)} de {cant_txt(q)}", "ambar"
        elif tbg.get("en_plazo", True):
            ctxt, ccol = "Bodega todavía no lo expide" + (f" (en plazo hasta el {tbg['vence']})" if tbg.get("vence") else ""), "ambar"
        else:
            ctxt, ccol = "Check no registra que haya salido", "ambar"
        dif = bool(l.get("es_diferencia"))
        color = "ambar" if (dif or ccol == "ambar") else ("verde" if (ccol == "verde" and l.get("estado") == "coincide") else "gris")
        filas.append({"sku": str(l.get("sku") or "").strip(), "nombre": l.get("nombre") or p.get("nombre") or "",
                      "ot": p.get("gestiona") if p else None, "guia": _red(q), "check": (_red(chk) if chk is not None else None),
                      "estado": l.get("estado") or "", "estado_txt": l.get("estado_txt") or "", "es_diferencia": dif,
                      "texto": l.get("texto") or "", "check_txt": ctxt, "check_color": ccol, "color": color, "en_guia": True})
    for p in comp.get("no_salio") or []:
        filas.append({"sku": p.get("sku") or "", "nombre": p.get("nombre") or "", "ot": p.get("gestiona"), "guia": 0,
                      "check": None, "estado": p.get("estado") or "", "estado_txt": p.get("estado_txt") or "",
                      "es_diferencia": bool(p.get("es_diferencia")), "texto": p.get("texto") or "",
                      "check_txt": "No salió en ninguna guía", "check_color": "gris",
                      "color": "ambar" if p.get("es_diferencia") else "gris", "en_guia": False})
    skus = {norm_sku(l.get("sku")) for l in pg.get("lineas") or []} - {""}
    diferencias = []
    for h in hallazgos(comp, tb):
        c, d = h.get("codigo"), h.get("detalle")
        if ((c == COD_C and d == num) or (c == COD_B and (d in skus or d == DETALLE_TOTAL))
                or (c in COD_A and (d in skus or c == "esta_en_la_OT_y_no_salio"))):
            diferencias.append({"codigo": c, "texto": h.get("texto") or "",
                                "regla": "C" if c == COD_C else ("B" if c == COD_B else "A")})
    orden = {"C": 0, "B": 1, "A": 2}
    diferencias.sort(key=lambda x: orden.get(x["regla"], 3))
    return {"guia": num, "diferencias": diferencias, "productos": filas,
            "check": ({k: tbg.get(k) for k in ("estado", "color", "texto", "vence", "en_plazo")} if tbg else None)}
