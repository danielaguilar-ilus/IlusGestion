# -*- coding: utf-8 -*-
"""🧹 «Costeo de OT · Calidad de datos» — lo que hay que DEPURAR para que los costos sean confiables (FASE 2, Daniel 2026-10-29).

Pedido de Daniel: «estamos en el paso de la depuración de datos: necesito que los costos sean estructurados. La calidad de datos,
perfecto, pero SIEMPRE darle la prioridad al DOCUMENTO que lleva el cobro. Es muy probable que en una factura quepan los productos y
en otra quepa solamente el servicio: adecuémonos. También puede ser que DOS FACTURAS me tengan servicio y se puedan SUMAR. Y es muy
importante identificar si hay COBROS EN OTRAS OT para revisar el SALDO». Y: «construyamos el dashboard también para ir depurando».

Este módulo es la ÚNICA fuente de las reglas de calidad (REGLA #59). Trabaja sobre la MISMA fila plana del tablero «Explorar»
(ot_costeo.fila) más lo que las lecturas por lote de app.py traen aparte: los documentos de la OT con lo que aportó cada uno
(mant_visita_documentos.zz_serv_monto / zz_envio_monto), la foto del libro de saldo (ot_fac_doc / ot_fac_alerta / ot_fac_libro),
el anexo vivo, las autorizaciones aprobadas y los motivos de «Finanzas dudosas» (_ot_fin_dudosas_motivos, que se REUTILIZA).

MÓDULO PURO (REGLAS #9 y #18): sin Flask, sin base, sin Random ni Check. No escribe nada ni decide nada sobre la plata: solo dice
qué está incompleto o no cuadra, cuánto dinero toca y por qué camino (que YA existe) se arregla. Rojo SOLO para plata realmente
perdida; lo demás va en ámbar (revisar) o azul (informativo).

La prioridad del documento (Daniel): el cobro «oficial» de una OT es la SUMA de lo que aportan sus documentos de cobro VIVOS
(factura / boleta; la nota de venta mientras no se facture). Un documento puede traer solo productos (no aporta) y otro solo el
servicio; varias facturas con servicio se SUMAN; un documento emitido a otra empresa (otro RUT) no cuenta. Un aporte en NULL es
«sin verificar» (anterior al 25-09-2026, o un documento que no aportó): nunca se inventa un cero para acusar una diferencia.

Confidencial · solo gerencia (REGLAS #19 y #26): todo esto es plata y proveedores; la ruta exige superadmin.
"""
from __future__ import annotations

import unicodedata
from urllib.parse import urlencode

import ot_anexo_costo as OAC
import ot_costeo as C
import ot_gerencial as G
import ot_saldo_servicio as SALDO

NOMBRE = "Calidad de datos"
SEVERIDADES = ("rojo", "ambar", "azul")
SEVERIDAD_TXT = {"rojo": "Plata perdida", "ambar": "Revisar", "azul": "Informativo"}
TERMINADAS = ("cerrada", "completada")              # estados en que la OT ya debió quedar con su factura o boleta
CEROS = G.COBERTURAS_REGALADAS                      # $0 que exige la autorización de Daniel (REGLA #24)
ORIGENES_SIN_MOTIVO = ("manual", "supuesto")        # «escrito a mano»: solo vale con su motivo (REGLA #56)
TIPOS_COBRO = ("factura", "boleta", "nota_venta")    # documentos que llevan el cobro (la nota de venta mientras no se facture)
TOLERANCIA = 1.0                                     # pesos: la misma holgura del resto del proyecto
POR_PAGINA = (10, 25, 50, 100, 200)
POR_PAGINA_DEF = 25
RUTAS_DEF = {"ot": "/ot/0", "reparar": "/ot/finanzas-reparar", "regularizar": "/ot/regularizar",
             "saldo": "/ot/saldo-facturas"}
TIPO_DOC_TXT = {"factura": "Factura", "boleta": "Boleta", "nota_venta": "Nota de venta", "cotizacion": "Cotización",
                "otro_doc": "Otro documento", None: "Documento"}

# Motivos de _ot_fin_dudosas_motivos (app.py) que esta fase REUTILIZA tal cual (no se copian sus reglas): el texto es la llave.
# tests/test_ot_calidad.py verifica que esos textos sigan existiendo en app.py.
MOTIVOS_DUDOSAS = {
    "Precio anotado sin documento de cobro (no cuenta como cobro)": "PRECIO_ANOTADO",
    "Sin separar servicio y despacho": "SIN_SEPARAR",
    "Línea ZZ que no es de servicio": "ZZ_NO_SERVICIO",
}


def _cod(codigo, titulo, regla, severidad, monto_txt, arreglo, como, solo_cuenta=False):
    return {"codigo": codigo, "titulo": titulo, "regla": regla, "severidad": severidad, "monto_txt": monto_txt,
            "arreglo": arreglo, "como": como, "solo_cuenta": solo_cuenta}


# El orden es el de la pantalla: primero la plata perdida, luego lo que mueve el cobro y el costo, al final lo informativo.
CODIGOS = (
    _cod("PERDIDA_SIN_AUTORIZACION", "Cerrada a pérdida sin autorización",
         "La OT está cerrada, se cobra y lo que nos costó supera lo cobrado (queda menos de $0), y no tiene aprobada la "
         "autorización «cerrar con pérdida». Son casos antiguos: hoy el cierre la exige (REGLA #28).",
         "rojo", "perdido", "ot_autorizacion",
         "Informativo: la OT ya está cerrada y es evidencia. Ábrela para ver su cuenta; la autorización se gestiona desde la "
         "bandeja de autorizaciones."),
    _cod("COBRO_DOC_SIN_DECLARAR", "Documento declarado, cobro en blanco",
         "La OT se cobra, tiene una factura, boleta o nota de venta de cobro vigente y «Cobré» está vacío.",
         "ambar", "registrado en sus documentos", "reparar_cobro",
         "En «Finanzas por reparar», pestaña «Documento declarado, cobro sin declarar»: Revisar y tomar el cobro de los documentos."),
    _cod("COBRO_DISTINTO_DE_DOCUMENTOS", "Cobro distinto de sus documentos",
         "Lo que la OT declara como cobrado no es igual a la SUMA de lo que aportan sus documentos de cobro vivos (varias "
         "facturas con servicio se suman; un documento solo de productos no aporta; otra empresa no cuenta). Solo se acusa "
         "cuando se puede verificar: con aportes sin registrar (antes del 25-09-2026) queda «sin verificar».",
         "ambar", "de diferencia", "ot_finanzas",
         "Abre la OT y usa «Corregir finanzas»: trae lo que aportan los documentos ya declarados (con su saldo)."),
    _cod("COBRO_SIN_ORIGEN", "Cobro sin respaldo",
         "La OT se cobra y tiene un cobro de servicio sin origen («declarado») o «escrito a mano» sin su motivo (REGLA #56).",
         "ambar", "cobrado sin respaldo", "reparar_origen",
         "En «Finanzas por reparar», pestaña «Cobro sin respaldo»: fija el origen en la línea del documento, usa los documentos "
         "o déjalo a mano con tu motivo."),
    _cod("SOLO_NOTA_VENTA", "Cerrada solo con nota de venta",
         "La OT terminada se cobra y su único documento de cobro vigente es una nota de venta: falta la factura o boleta.",
         "ambar", "cobrado sin factura", "regularizar_factura",
         "En «Regularizar», filtro «Falta factura»: agrega la factura o boleta (la nota queda dada de baja)."),
    _cod("NV_YA_FACTURADA", "Nota de venta ya facturada",
         "La nota de venta de la OT ya tiene su factura o boleta en Random (según la foto del libro de saldo) y esa factura "
         "no está en la OT.",
         "ambar", "cobrado con la nota de venta", "ot_documento",
         "Abre la OT y agrega esa factura: la nota de venta queda dada de baja por ella y la factura pasa a ser el principal."),
    _cod("FACTURA_SIN_SALDO", "Factura sin saldo",
         "El libro de saldo tiene una alerta vigente: la OT usa una factura cuyo servicio o despacho ya lo usan otras OT "
         "(REGLAS #25 y #28).",
         "ambar", "cobrado en la OT", "saldo",
         "En «Saldo de facturas» revisa qué OT usan esa factura y corrige la que sobra."),
    _cod("DOC_OTRO_RUT", "Documento de otra empresa",
         "La OT tiene un documento emitido a otro RUT y nadie lo justificó. No cuenta como cobro de esta OT.",
         "ambar", "aportado por ese documento", "ot_documento",
         "Abre la OT: justifica el documento o reemplázalo por el del cliente."),
    _cod("ANEXO_DISTINTO", "Costo distinto del anexo",
         "Lo que la OT le paga al proveedor no es igual al total de su anexo vivo (un solo valor, REGLAS #34 y #56).",
         "ambar", "de diferencia con el anexo", "reparar_anexo",
         "En «Finanzas por reparar», pestaña «Costo de la OT distinto al anexo»: alinear con el anexo."),
    _cod("TECNICO_SIN_COSTO", "Técnico externo sin costo",
         "La OT la hizo un técnico externo y no declara lo que cobró: el margen sale inflado.",
         "ambar", "cobrado sin costo declarado", "ot_finanzas",
         "Abre la OT: declara el costo con «Editar cobro y costos» o con su anexo."),
    _cod("MARGEN_100", "Margen de 100 %",
         "La OT se cobra, cobró más de $0 y lo que nos costó es $0. Revísalo: el costo de los técnicos propios llega en otra fase.",
         "ambar", "cobrado sin costo", "ot_finanzas",
         "Abre la OT y revisa si falta el costo del técnico o del despacho."),
    _cod("CERO_SIN_AUTORIZACION", "$0 sin autorización",
         "La OT no se cobra (garantía, regalía, arriendo o leasing, cortesía) y no tiene la autorización de Daniel aprobada.",
         "ambar", "nos costó", "regularizar_cero",
         "En «Regularizar», filtro «$0 sin autorización de Daniel»: pide o registra la autorización."),
    _cod("COSTO_MAYOR_VALOR", "Cuesta más de lo que vale",
         "Una garantía, regalía, arriendo o cortesía costó más que su valorizado (la misma regla del Resumen).",
         "ambar", "costó por sobre su valor", "ot_finanzas",
         "Abre la OT y revisa su valorizado o su costo."),
    _cod("PRECIO_ANOTADO", "Precio anotado sin documento",
         "Hay un «Precio al cliente» anotado sin documento de cobro: no cuenta como cobro (Finanzas dudosas).",
         "ambar", "anotado", "ot_finanzas",
         "Abre la OT y usa «Corregir finanzas»: declara el cobro desde su documento."),
    _cod("SIN_SEPARAR", "Cobro sin separar",
         "El cobro sale del «Precio al cliente» sin separar servicio y despacho (Finanzas dudosas).",
         "ambar", "cobrado", "ot_finanzas",
         "Abre la OT y usa «Corregir finanzas» para separar servicio y despacho."),
    _cod("ZZ_NO_SERVICIO", "Línea que no es servicio",
         "La línea ZZ guardada no es de servicio (por ejemplo ZZRETIRO): su monto no cuenta como cobro (Finanzas dudosas).",
         "ambar", "en esa línea", "ot_finanzas",
         "Abre la OT y usa «Corregir finanzas» para declarar la línea de servicio correcta."),
    _cod("SIN_CENTRO_COSTO", "Sin centro de costo",
         "La OT no tiene centro de costo: no se sabe a qué área cargarla (REGLA #24).",
         "azul", "sin centro", "ot_finanzas",
         "Abre la OT y declara el centro de costo (o «Corregir finanzas» si ya está cerrada)."),
    _cod("SIN_VALORIZAR", "Sin valorizar",
         "La OT no se cobra (garantía, cortesía, interno, contrato…) y no tiene su valorizado de referencia.",
         "azul", "nos costó", "ot",
         "Por ahora solo se cuenta y se enlaza a la OT: la valorización sugerida llega en la fase 3."),
    _cod("SIN_TECNICO", "Cerrada sin técnico",
         "La OT está cerrada y no tiene un técnico asignado.",
         "azul", "", "ot",
         "Informativo: abre la OT para ver quién la hizo."),
    _cod("SIN_INTENCION", "$0 sin «¿Por qué?»",
         "Un $0 (garantía, regalía, arriendo o cortesía) sin su clasificación de negocio. Las OT antiguas no se rellenan "
         "(REGLA #47): solo se cuentan.",
         "azul", "nos costó", "ninguno",
         "Solo se cuenta: las nuevas ya nacen clasificadas.", solo_cuenta=True),
)
POR_CODIGO = {c["codigo"]: c for c in CODIGOS}
ORDEN = tuple(c["codigo"] for c in CODIGOS)


# ════════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  Ayudas
# ════════════════════════════════════════════════════════════════════════════════════════════════════════════════
def _num(x):
    if x is None or (isinstance(x, str) and not x.strip()):
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _txt(x):
    return "" if x is None else str(x).strip()


def _r(x):
    return None if x is None else round(float(x), 2)


def clp(n):
    return G.clp(n)


def sin_tildes(s):
    t = unicodedata.normalize("NFD", str(s or ""))
    return "".join(c for c in t if unicodedata.category(c) != "Mn").lower().strip()


def _es_cero(x):
    """rut_ok guardado como 0 / False / «0» (Random dijo que el RUT no es el del cliente). None = no se revisó: no cuenta."""
    if x is None:
        return False
    if x is False:
        return True
    return str(x).strip().lower() in ("0", "false")


def titulo_doc(tido, nudo):
    """«FCV 11348» (sin ceros a la izquierda), igual que «Finanzas por reparar»."""
    n = _txt(nudo)
    return (f"{_txt(tido).upper()} {n.lstrip('0') or n}").strip()


# ════════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  Documentos de la OT (la prioridad del documento)
# ════════════════════════════════════════════════════════════════════════════════════════════════════════════════
def normalizar_docs(docs, v=None):
    """Los documentos de la OT listos para las reglas. `docs`: filas de mant_visita_documentos (visita_id, origen, erp_tido,
    erp_nudo, cotizacion_id, es_cobro, reemplazado_por_id, heredado_de_visita_id, zz_serv_monto, zz_envio_monto, rut_ok,
    justificado). `v`: la OT (su documento principal antiguo, factura_*, cuenta solo si se validó contra Random, igual que
    ot_costeo.docs_con_principal). Cada uno:
      · vivo: no fue dado de baja por su factura (reemplazado_por_id) ni es la copia espejada de la madre (heredado);
      · cobro: documento de Random que lleva cobro (es_cobro), con tipo y número;
      · otra_empresa: Random dijo que el RUT no es el del cliente (rut_ok = 0). NO cuenta como cobro (mismo criterio del motor
        de saldo, REGLA #56); `justificado` dice si alguien lo explicó;
      · registrado: tiene anotado lo que aportó (zz_serv_monto o zz_envio_monto); NULL = no aportó o es anterior al 25-09-2026."""
    out, vistos = [], set()
    for d in docs or ():
        origen = _txt(d.get("origen")) or "erp"
        t = _txt(d.get("erp_tido") if "erp_tido" in d else d.get("tido")).upper()
        n = _txt(d.get("erp_nudo") if "erp_nudo" in d else d.get("nudo"))
        if origen == "erp":
            clave = SALDO.clave_doc(t, n)
            titulo = titulo_doc(t, n)
        else:
            clave = ("COT", _txt(d.get("cotizacion_id")))
            titulo = "Cotización " + _txt(d.get("etiqueta") or d.get("cotizacion_id"))
        tipo = C.tipo_doc({"origen": origen, "tido": t})
        serv, desp = _num(d.get("zz_serv_monto")), _num(d.get("zz_envio_monto"))
        rut_ok = d.get("rut_ok")
        x = {"clave": clave, "tido": t, "nudo": n, "titulo": titulo, "tipo": tipo, "tipo_txt": TIPO_DOC_TXT.get(tipo, "Documento"),
             "origen": origen,
             "vivo": not d.get("reemplazado_por_id") and not d.get("heredado_de_visita_id"),
             "heredado": bool(d.get("heredado_de_visita_id")), "reemplazado": bool(d.get("reemplazado_por_id")),
             "cobro": origen == "erp" and bool(t and n) and d.get("es_cobro", 1) not in (False, 0, "0"),
             "otra_empresa": _es_cero(rut_ok),
             "justificado": bool(d.get("justificado")) or bool(_txt(d.get("rut_justif"))),
             "serv": serv, "desp": desp, "registrado": serv is not None or desp is not None, "principal_antiguo": False}
        vistos.add(clave)
        out.append(x)
    v = v or {}
    t, n = _txt(v.get("factura_tido")).upper(), _txt(v.get("factura_nudo"))
    validado = bool(_txt(v.get("factura_asociada_por"))) or v.get("factura_rut_ok") is not None
    if t and n and validado and SALDO.clave_doc(t, n) not in vistos:
        tipo = C.tipo_doc({"origen": "erp", "tido": t})
        rk = v.get("factura_rut_ok")
        out.append({"clave": SALDO.clave_doc(t, n), "tido": t, "nudo": n, "titulo": titulo_doc(t, n), "tipo": tipo,
                    "tipo_txt": TIPO_DOC_TXT.get(tipo, "Documento"), "origen": "erp", "vivo": True, "heredado": False,
                    "reemplazado": False, "cobro": True, "otra_empresa": _es_cero(rk),
                    "justificado": False, "serv": None, "desp": None, "registrado": False, "principal_antiguo": True})
    return out


def de_cobro(nd):
    """Los documentos que llevan el cobro HOY: vivos, de cobro, factura / boleta / nota de venta, y NO de otra empresa."""
    return [d for d in nd if d["vivo"] and d["cobro"] and d["tipo"] in TIPOS_COBRO and not d["otra_empresa"]]


def claves_nota_venta(docs):
    """Las claves canónicas de las notas de venta VIVAS de una OT (para leer su familia en ot_fac_doc)."""
    return [d["clave"] for d in de_cobro(normalizar_docs(docs)) if d["tipo"] == "nota_venta"]


def suma_aportes(docs):
    """Lo que aportan los documentos al cobro: la SUMA (dos facturas con servicio se suman; una solo de productos no aporta)."""
    s = e = 0.0
    reg = sin = 0
    for d in docs:
        if d["registrado"]:
            reg += 1
            s += d["serv"] or 0.0
            e += d["desp"] or 0.0
        else:
            sin += 1
    return {"servicio": round(s, 2), "despacho": round(e, 2), "total": round(s + e, 2), "registrados": reg, "sin_registrar": sin}


def comparar_con_documentos(declarado, docs):
    """¿Lo declarado como cobrado es igual a lo que aportan los documentos de cobro?
    'sin_documentos' · 'sin_verificar' (ningún aporte registrado, o lo declarado supera lo registrado y hay documentos sin
    registrar: la diferencia puede estar en ellos) · 'igual' · 'distinto' (con `diferencia` = declarado − suma)."""
    if not docs:
        return {"estado": "sin_documentos", "diferencia": None, "suma": None}
    s = suma_aportes(docs)
    if not s["registrados"]:
        return {"estado": "sin_verificar", "diferencia": None, "suma": s}
    dif = round(float(declarado or 0) - s["total"], 2)
    if abs(dif) < TOLERANCIA:
        return {"estado": "igual", "diferencia": 0.0, "suma": s}
    if dif < 0 or s["sin_registrar"] == 0:
        return {"estado": "distinto", "diferencia": dif, "suma": s}
    return {"estado": "sin_verificar", "diferencia": dif, "suma": s}


def cobro_sin_origen(v):
    """REGLA ÚNICA de «cobro sin respaldo» (la usan la Calidad de datos y la 3.ª pestaña de «Finanzas por reparar»): un cobro de
    servicio (> $0, en una línea que sí es de servicio) sin origen («declarado») o «escrito a mano» sin su motivo."""
    v = v or {}
    zz = _num(v.get("zz_monto"))
    if zz is None or zz <= 0:
        return False
    if _txt(v.get("zz_codigo")).upper() in SALDO.NO_SERVICIO:
        return False
    origen = _txt(v.get("valor_origen")).lower()
    if not origen:
        return True
    return origen in ORIGENES_SIN_MOTIVO and not _txt(v.get("zz_motivo_manual"))


def fuente_cobro_txt(v):
    """«declarado» (sin origen) o «escrito a mano» (sin motivo): cómo quedó el cobro que no tiene respaldo."""
    return "declarado" if not _txt((v or {}).get("valor_origen")) else "escrito a mano"


# ════════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  Las reglas
# ════════════════════════════════════════════════════════════════════════════════════════════════════════════════
def _h(codigo, monto, detalle):
    return {"codigo": codigo, "monto": _r(monto), "detalle": detalle}


def _lista(docs):
    return ", ".join(d["titulo"] for d in docs)


def evaluar(fila, ctx):
    """Los hallazgos de UNA OT, en el orden de la pantalla. `fila`: ot_costeo.fila (la del tablero Explorar). `ctx`:
      v (la fila de mant_visitas), fin (_ot_finanzas), docs (mant_visita_documentos), auts (autorizaciones APROBADAS),
      cubre_cero (bool: _ot_aut_cubre_cero sobre esas autorizaciones), anexo ({numero, estado, items, rol} o None),
      dudosas (los textos de _ot_fin_dudosas_motivos), alertas (ot_fac_alerta vigentes), familias ({clave NV: «FCV 11348»})."""
    v = ctx.get("v") or {}
    fin = ctx.get("fin") or {}
    cob_d = fin.get("cobre") or {}
    me_d = fin.get("me_cobraron") or {}
    nd = normalizar_docs(ctx.get("docs"), v)
    cobro = de_cobro(nd)
    cobra = bool(fila.get("cobra"))
    cob = fila.get("cob") or ""
    estado = _txt(fila.get("estado")).lower()
    cobre = fila.get("cobre") or 0.0
    me_total = fila.get("me_total") or 0.0
    out = {}

    # 1 · Cerrada a pérdida sin autorización (rojo: plata realmente perdida)
    if estado == "cerrada" and fila.get("clase") == "perdida" and not any(
            _txt(a.get("tipo")) == "cerrar_con_perdida" and _txt(a.get("estado") or "aprobada").lower() == "aprobada"
            for a in (ctx.get("auts") or ())):
        q = fila.get("queda") or 0.0
        out["PERDIDA_SIN_AUTORIZACION"] = _h("PERDIDA_SIN_AUTORIZACION", -q,
                                             f"Cobró {clp(cobre)} y nos costó {clp(me_total)}: se perdieron {clp(-q)}.")

    # 2 · Documento declarado y cobro en blanco (la misma condición de «Finanzas por reparar», pestaña 1)
    zz, env = _num(v.get("zz_monto")), _num(v.get("zz_envio_monto"))
    if cobra and cobro and not cob_d.get("hay") and not (zz or 0) and not (env or 0):
        s = suma_aportes(cobro)
        out["COBRO_DOC_SIN_DECLARAR"] = _h("COBRO_DOC_SIN_DECLARAR", s["total"] if s["registrados"] else None,
                                           f"Documentos de cobro: {_lista(cobro)}. «Cobré» está vacío.")

    # 3 · Cobro distinto de la SUMA de lo que aportan sus documentos (solo cuando se puede verificar)
    origen = _txt(v.get("valor_origen")).lower()
    decision_persona = origen in ORIGENES_SIN_MOTIVO and bool(_txt(v.get("zz_motivo_manual")))
    if cobra and cob_d.get("hay") and cobro and not decision_persona:
        cmp_ = comparar_con_documentos(cob_d.get("total") or 0.0, cobro)
        if cmp_["estado"] == "distinto":
            s = cmp_["suma"]
            partes = "; ".join(
                f"{d['titulo']}: " + ("servicio " + clp(d["serv"] or 0) + (" + despacho " + clp(d["desp"]) if d["desp"] else "")
                                      if d["registrado"] else "sin aporte registrado") for d in cobro)
            out["COBRO_DISTINTO_DE_DOCUMENTOS"] = _h(
                "COBRO_DISTINTO_DE_DOCUMENTOS", abs(cmp_["diferencia"]),
                f"La OT declara {clp(cob_d.get('total'))}; sus documentos aportan {clp(s['total'])} ({partes}).")

    # 4 · Cobro sin respaldo (declarado sin origen o «a mano» sin motivo)
    if cobra and cobro_sin_origen(v):
        out["COBRO_SIN_ORIGEN"] = _h("COBRO_SIN_ORIGEN", cobre,
                                     f"Cobro {fuente_cobro_txt(v)} de {clp(zz)} sin "
                                     + ("origen." if not origen else "su motivo."))

    # 5 · Terminada solo con nota de venta (las que no se cobran no piden factura: REGLA #56)
    if cobra and estado in TERMINADAS and cobro and all(d["tipo"] == "nota_venta" for d in cobro):
        out["SOLO_NOTA_VENTA"] = _h("SOLO_NOTA_VENTA", cobre, f"Solo {_lista(cobro)}: falta la factura o boleta.")

    # 6 · Nota de venta que Random ya facturó y la factura no está en la OT
    if cobra:
        familias = ctx.get("familias") or {}
        tiene = {d["clave"] for d in nd}
        faltan = []
        for d in cobro:
            if d["tipo"] != "nota_venta":
                continue
            fam = _txt(familias.get(d["clave"]))
            if " " not in fam:
                continue
            t2, n2 = fam.split(" ", 1)
            if SALDO.clave_doc(t2, n2) not in tiene:
                faltan.append(f"{d['titulo']} → {fam}")
        if faltan:
            out["NV_YA_FACTURADA"] = _h("NV_YA_FACTURADA", cobre,
                                        "Ya facturada en Random: " + "; ".join(faltan) + ". Esa factura no está en la OT.")

    # 7 · Factura sin saldo (alerta vigente del libro)
    al = [a for a in (ctx.get("alertas") or ()) if _txt(a.get("estado") or "vigente").lower() == "vigente"]
    if al:
        txt = "; ".join((_txt(a.get("titulo")) or titulo_doc(a.get("doc_tido"), a.get("doc_nudo")))
                        + (f" (también la usan {_txt(a.get('otras_ots'))})" if _txt(a.get("otras_ots")) else "") for a in al)
        out["FACTURA_SIN_SALDO"] = _h("FACTURA_SIN_SALDO", cobre if cobra else None, "Sin saldo: " + txt + ".")

    # 8 · Documento de otra empresa sin justificar
    otros = [d for d in nd if d["vivo"] and d["otra_empresa"] and not d["justificado"]]
    if otros:
        m = sum((d["serv"] or 0) + (d["desp"] or 0) for d in otros if d["registrado"])
        out["DOC_OTRO_RUT"] = _h("DOC_OTRO_RUT", m if any(d["registrado"] for d in otros) else None,
                                 f"{_lista(otros)}: emitido a otro RUT, sin justificación. No cuenta como cobro.")

    # 9 · Costo distinto del anexo vivo
    ax = ctx.get("anexo")
    if ax and OAC.estado(v.get("costo_proveedor"), v.get("costo_despacho"), ax.get("items"), ax.get("rol")) != "alineado":
        tot = OAC.total(ax.get("items"), ax.get("rol"))
        p_, d_ = _num(v.get("costo_proveedor")), _num(v.get("costo_despacho"))
        costo = None if p_ is None else p_ + (d_ or 0)
        parte = " (su parte del anexo compartido)" if ax.get("rol") == OAC.ROL_SALIDA else ""
        out["ANEXO_DISTINTO"] = _h("ANEXO_DISTINTO", abs(tot - (costo or 0)),
                                   f"La OT paga {clp(costo) if costo is not None else 'sin declarar'}; el Anexo N° "
                                   f"{ax.get('numero')}{parte} dice {clp(tot)} ({_txt(ax.get('estado')) or 'vigente'}).")

    # 10 · Técnico externo sin costo declarado
    externo = fila.get("ttipo") == "externo" or _txt(v.get("proveedor_tipo")).lower() == "externo"
    tec_sin = bool(me_d.get("falta_tecnico")) and externo
    if tec_sin:
        out["TECNICO_SIN_COSTO"] = _h("TECNICO_SIN_COSTO", cobre if cobra else None,
                                      "Lo hizo un técnico externo y la OT no declara lo que cobró.")

    # 11 · Margen de 100 % (cobra y no costó nada; si la causa es un externo sin costo, ya lo dice el punto 10)
    if cobra and cobre > 0 and me_total <= 0 and not tec_sin:
        out["MARGEN_100"] = _h("MARGEN_100", cobre, f"Cobró {clp(cobre)} y lo que nos costó es $0.")

    # 12 · $0 sin autorización (garantía, regalía, arriendo/leasing, cortesía)
    if cob in CEROS and not ctx.get("cubre_cero"):
        out["CERO_SIN_AUTORIZACION"] = _h("CERO_SIN_AUTORIZACION", me_total,
                                          f"{G.COBERTURA_TXT.get(cob, cob)} sin la autorización de Daniel registrada.")

    # 13 · Cuesta más de lo que vale (la misma regla de «Cuestan más de lo que valen» del Resumen)
    val = fila.get("val")
    if cob in CEROS and not fila.get("falta_tec") and val and me_total > val:
        out["COSTO_MAYOR_VALOR"] = _h("COSTO_MAYOR_VALOR", me_total - val,
                                      f"Nos costó {clp(me_total)} y está valorizada en {clp(val)}.")

    # 14-16 · Motivos de «Finanzas dudosas» que se reutilizan tal cual
    for texto in ctx.get("dudosas") or ():
        cod = MOTIVOS_DUDOSAS.get(texto)
        if not cod or cod in out:
            continue
        if cod == "PRECIO_ANOTADO":
            m = fin.get("precio_anotado")
        elif cod == "ZZ_NO_SERVICIO":
            m = zz
        else:
            m = cobre
        out[cod] = _h(cod, m, texto + ".")

    # 17 · Sin centro de costo
    if not _txt(v.get("centro_costo")):
        out["SIN_CENTRO_COSTO"] = _h("SIN_CENTRO_COSTO", cobre if cobra else me_total, "La OT no tiene centro de costo.")

    # 18 · Sin valorizar (no se cobra; la desinstalación cubierta la valoriza su instalación)
    if not cobra and cob != "cubierta" and _num(v.get("valorizado_clp")) is None:
        out["SIN_VALORIZAR"] = _h("SIN_VALORIZAR", me_total, f"{G.COBERTURA_TXT.get(cob, cob)} sin valorizado de referencia.")

    # 19 · Cerrada sin técnico
    if estado == "cerrada" and fila.get("ttipo") == "sin_asignar":
        out["SIN_TECNICO"] = _h("SIN_TECNICO", None, "Cerrada sin técnico asignado.")

    # 20 · $0 sin «¿Por qué?» (solo se cuenta)
    if cob in CEROS and (fila.get("int") or C.SIN["int"]) == C.SIN["int"]:
        out["SIN_INTENCION"] = _h("SIN_INTENCION", me_total, f"{G.COBERTURA_TXT.get(cob, cob)} sin clasificar.")

    return [out[c] for c in ORDEN if c in out]


def verificacion(fila, ctx):
    """'igual' | 'distinto' | 'sin_verificar' | 'sin_documentos' | None (no se cobra o no declara cobro): cómo quedó la
    comparación del cobro con sus documentos (para contar las OT que todavía no se pueden verificar)."""
    fin = ctx.get("fin") or {}
    if not fila.get("cobra") or not (fin.get("cobre") or {}).get("hay"):
        return None
    cobro = de_cobro(normalizar_docs(ctx.get("docs"), ctx.get("v")))
    return comparar_con_documentos((fin.get("cobre") or {}).get("total") or 0.0, cobro)["estado"]


def item(fila, ctx):
    """Lo que la pantalla necesita de UNA OT (se guarda en la caché de 90 s): sus datos, sus documentos y sus hallazgos."""
    lab = fila.get("_lab") or {}
    nd = normalizar_docs(ctx.get("docs"), ctx.get("v"))
    return {
        "id": int(fila["id"]), "numero": fila.get("numero") or "", "titulo": fila.get("titulo") or "",
        "estado": fila.get("estado") or "", "estado_txt": lab.get("estado") or fila.get("estado") or "",
        "fecha": fila.get("dia") or "", "cli": fila.get("cli") or "", "cli_n": lab.get("cli") or "",
        "tec": fila.get("tec") or "", "tec_n": lab.get("tec") or "", "cc": fila.get("cc") or "", "cc_n": lab.get("cc") or "",
        "cob_n": lab.get("cob") or "", "cobra": bool(fila.get("cobra")), "cobre": fila.get("cobre"),
        "me_total": fila.get("me_total"), "queda": fila.get("queda"),
        "docs": [{"clave": list(d["clave"]), "titulo": d["titulo"], "tipo_txt": d["tipo_txt"], "vivo": d["vivo"],
                  "cobro": d["cobro"], "otra_empresa": d["otra_empresa"], "justificado": d["justificado"],
                  "serv": _r(d["serv"]), "desp": _r(d["desp"]), "registrado": d["registrado"],
                  "reemplazado": d["reemplazado"], "heredado": d["heredado"]} for d in nd],
        "hallazgos": evaluar(fila, ctx),
        "verif": verificacion(fila, ctx),
    }


# ════════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  Resumen, concentración, lista y paginación
# ════════════════════════════════════════════════════════════════════════════════════════════════════════════════
def ficha_codigo(codigo):
    c = POR_CODIGO[codigo]
    return dict(c, severidad_txt=SEVERIDAD_TXT[c["severidad"]])


def concentracion(items, dim, tope=5):
    """Dónde se concentra un hallazgo: [{clave, nombre, n, monto}] por técnico (`tec`), centro de costo (`cc`) o cliente
    (`cli`), de mayor a menor (cantidad y luego monto). Lo que sobra del tope va en «Otros N»."""
    g = {}
    for it in items:
        k = it.get(dim) or ""
        e = g.setdefault(k, {"clave": k, "nombre": it.get(dim + "_n") or k or "Sin dato", "n": 0, "monto": 0.0})
        e["n"] += 1
        e["monto"] = round(e["monto"] + (it.get("_monto") or 0.0), 2)
    lista = sorted(g.values(), key=lambda e: (-e["n"], -e["monto"], sin_tildes(e["nombre"])))
    if len(lista) > tope:
        resto = lista[tope:]
        lista = lista[:tope] + [{"clave": "", "nombre": f"Otros {len(resto)}", "n": sum(e["n"] for e in resto),
                                 "monto": round(sum(e["monto"] for e in resto), 2), "otros": True}]
    return lista


def con_codigo(items, codigo):
    """Las OT que tienen ese hallazgo, cada una con su `_monto` y su `_detalle` (copia: la caché no se toca)."""
    out = []
    for it in items:
        h = next((x for x in it.get("hallazgos") or () if x["codigo"] == codigo), None)
        if h:
            out.append(dict(it, _monto=h["monto"], _detalle=h["detalle"]))
    return out


def resumen(items):
    """Lo que muestran las tarjetas: por código, cuántas OT, cuánta plata toca (la que se puede medir) y dónde se concentra;
    y en total, cuántas OT se revisaron, cuántas tienen algo, cuántas no y cuántas aún no se pueden verificar."""
    cods = []
    for c in CODIGOS:
        ots = con_codigo(items, c["codigo"])
        con_monto = [x["_monto"] for x in ots if x["_monto"] is not None]
        cods.append(dict(ficha_codigo(c["codigo"]), n=len(ots), monto=round(sum(con_monto), 2),
                         n_sin_monto=len(ots) - len(con_monto),
                         concentracion={d: concentracion(ots, d) for d in ("tec", "cc", "cli")} if ots else {}))
    n_con = sum(1 for it in items if it.get("hallazgos"))
    por_sev = {s: sum(1 for it in items if any(POR_CODIGO[h["codigo"]]["severidad"] == s for h in it.get("hallazgos") or ()))
               for s in SEVERIDADES}
    return {"codigos": cods, "n_ot": len(items), "n_con": n_con, "n_sin": len(items) - n_con,
            "n_sin_verificar": sum(1 for it in items if it.get("verif") == "sin_verificar"),
            "por_severidad": por_sev, "severidades_txt": dict(SEVERIDAD_TXT)}


def filtrar(items, codigo, q=None):
    """Las OT con ese hallazgo que además calzan con la búsqueda (N° de OT, cliente, técnico o título; sin tildes ni
    mayúsculas; varias palabras = todas). De mayor a menor plata en juego (lo que no se puede medir, al final)."""
    lista = con_codigo(items, codigo)
    palabras = [p for p in sin_tildes(q).split() if p][:8]
    if palabras:
        def _hay(it):
            h = sin_tildes(" ".join(str(it.get(k) or "") for k in ("numero", "cli_n", "tec_n", "titulo", "cc_n")))
            return all(p in h for p in palabras)
        lista = [it for it in lista if _hay(it)]
    return sorted(lista, key=lambda it: (it["_monto"] is None, -(it["_monto"] or 0.0), -it["id"]))


def leer_pagina(args):
    """?page=&por= sin confiar en nada: página ≥ 1 y N por página de la lista permitida."""
    try:
        page = max(1, int(args.get("page") or 1))
    except (TypeError, ValueError):
        page = 1
    try:
        por = int(args.get("por") or POR_PAGINA_DEF)
    except (TypeError, ValueError):
        por = POR_PAGINA_DEF
    return page, (por if por in POR_PAGINA else POR_PAGINA_DEF)


def paginar(lista, page, por):
    """Como Etiquetas (REGLA #4.3): {filas, total, page, pages, por, desde, hasta}. Una página de más vuelve a la última."""
    total = len(lista)
    pages = max(1, (total + por - 1) // por)
    page = min(max(1, page), pages)
    ini = (page - 1) * por
    vista = lista[ini:ini + por]
    return {"filas": vista, "total": total, "page": page, "pages": pages, "por": por,
            "desde": (ini + 1) if total else 0, "hasta": ini + len(vista)}


def claves_de(items):
    """Las claves canónicas de los documentos de Random de esas OT (para leer quién más los usa)."""
    out = set()
    for it in items:
        for d in it.get("docs") or ():
            if d.get("cobro"):
                out.add(tuple(d["clave"]))
    return out


def ultimos_usos(movs):
    """Quién toma HOY cada documento según el libro (ot_fac_libro): el último movimiento de cada OT y categoría manda (el
    libro nunca se edita: consumo + / liberación −, con el acumulado). {clave: [{vid, numero, cliente, categoria, monto}]}."""
    ult = {}
    for m in sorted(movs or (), key=lambda m: int(m.get("id") or 0)):
        try:
            vid = int(m.get("visita_id"))
        except (TypeError, ValueError):
            continue
        ult[(SALDO.clave_doc(m.get("doc_tido"), m.get("doc_nudo")), _txt(m.get("categoria")), vid)] = m
    out = {}
    for (k, cat, vid), m in ult.items():
        a = SALDO._entero(m.get("acumulado_ot"))
        if a > 0:
            out.setdefault(k, []).append({"vid": vid, "numero": _txt(m.get("numero_ot")) or f"OT #{vid}",
                                          "cliente": _txt(m.get("cliente")), "categoria": cat, "monto": a})
    for k in out:
        out[k].sort(key=lambda u: (u["numero"], u["categoria"]))
    return out


def url_ot(rutas, vid, ancla=""):
    base = str((rutas or RUTAS_DEF).get("ot") or RUTAS_DEF["ot"])
    base = base[:-2] if base.endswith("/0") else base.rstrip("/")
    return f"{base}/{int(vid)}" + (("#" + ancla) if ancla else "")


def arreglo(codigo, it, rutas=None):
    """El camino que YA existe para arreglarlo: {url, texto}; None si solo se cuenta. Esta pantalla NUNCA escribe: solo lleva."""
    r = dict(RUTAS_DEF, **(rutas or {}))
    k = POR_CODIGO[codigo]["arreglo"]
    if k == "ninguno":
        return None
    if k.startswith("reparar_"):
        tab = k.split("_", 1)[1]
        txt = {"cobro": "Tomar el cobro", "origen": "Respaldar el cobro", "anexo": "Alinear con el anexo"}[tab]
        return {"url": r["reparar"] + "?" + urlencode({"tab": tab, "q": it.get("numero") or ""}), "texto": txt,
                "donde": "Finanzas por reparar"}
    if k.startswith("regularizar_"):
        falta = {"regularizar_factura": "factura", "regularizar_cero": "autorizacion_cobro_cero"}[k]
        q = {"falta": falta}
        if it.get("cli_n") and it.get("cli") and it.get("cli") != C.SIN["cli"]:
            q["cliente_q"] = it["cli_n"]
        return {"url": r["regularizar"] + "?" + urlencode(q),
                "texto": "Agregar la factura" if falta == "factura" else "Pedir la autorización", "donde": "Regularizar"}
    if k == "saldo":
        return {"url": r["saldo"], "texto": "Revisar el saldo", "donde": "Saldo de facturas"}
    if k == "ot_documento":
        return {"url": url_ot(r, it["id"], "otdCardFinanzas"), "texto": "Abrir la OT · documentos", "donde": "la OT"}
    if k == "ot_finanzas":
        return {"url": url_ot(r, it["id"], "otdCardFinanzas"), "texto": "Abrir la OT · costos", "donde": "la OT"}
    if k == "ot_autorizacion":
        return {"url": url_ot(r, it["id"], "otdCardFinanzas"), "texto": "Ver la OT", "donde": "la OT"}
    return {"url": url_ot(r, it["id"]), "texto": "Ver la OT", "donde": "la OT"}


def fila_lista(it, codigo, rutas=None, usos=None, fotos=None):
    """Una fila de la lista de un código: la OT con el monto y el detalle de ESE hallazgo, sus documentos (con lo que aportó
    cada uno, quién más los usa según el libro y su saldo según la foto) y el botón «Arreglar» (solo un enlace)."""
    usos, fotos = usos or {}, fotos or {}
    docs = []
    for d in it.get("docs") or ():
        k = tuple(d.get("clave") or ())
        x = dict(d)
        x["otras_ot"] = [u for u in usos.get(k, []) if u["vid"] != it["id"]] if d.get("cobro") else []
        f = fotos.get(k)
        if f and d.get("cobro"):
            x["saldo"] = {"servicio": max(0, int(f.get("cap_servicio") or 0) - int(f.get("usado_servicio") or 0)),
                          "despacho": max(0, int(f.get("cap_despacho") or 0) - int(f.get("usado_despacho") or 0)),
                          "cap_servicio": int(f.get("cap_servicio") or 0), "cap_despacho": int(f.get("cap_despacho") or 0),
                          "n_ot": int(f.get("n_ot") or 0), "leido": bool(int(f.get("erp_leido") or 0))}
        docs.append(x)
    otros = [h["codigo"] for h in it.get("hallazgos") or () if h["codigo"] != codigo]
    return {"id": it["id"], "numero": it["numero"], "titulo": it["titulo"], "estado_txt": it["estado_txt"],
            "fecha": it["fecha"], "cliente": it["cli_n"], "tecnico": it["tec_n"], "centro": it["cc_n"], "cobertura": it["cob_n"],
            "cobre": it["cobre"], "me_total": it["me_total"], "monto": it.get("_monto"), "detalle": it.get("_detalle") or "",
            "docs": docs, "otros": [{"codigo": c, "titulo": POR_CODIGO[c]["titulo"]} for c in otros],
            "arreglo": arreglo(codigo, it, rutas), "url": url_ot(rutas, it["id"])}
