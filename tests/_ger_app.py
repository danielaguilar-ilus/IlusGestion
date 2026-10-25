# -*- coding: utf-8 -*-
"""Ayudas de las pruebas del Costeo de órdenes de trabajo (ot_gerencial) — SIN Flask, SIN base de datos.

Saca de app.py (por TEXTO, sin parsear los 9 MB con ast: eso tarda minutos) las piezas reales de la cuenta de la OT
(`_ot_finanzas`, `_ot_cobertura`, `_ot_fin_agregado`…) para que las pruebas usen la regla verdadera del proyecto, y el bloque
del costeo (`_ger_*` y sus rutas) para ejercitarlo con dobles de la base."""
import os
import re
import types
from datetime import datetime, timedelta

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_CACHE = {}


def leer(*partes):
    with open(os.path.join(RAIZ, *partes), encoding="utf-8") as fh:
        return fh.read().replace("\r\n", "\n")


def app_src():
    if "app" not in _CACHE:
        _CACHE["app"] = leer("app.py")
    return _CACHE["app"]


def bloque(src, nombre):
    """El texto de una función (`def nombre(`) o constante (`nombre =`) de nivel superior: hasta la primera línea en columna
    0 que no sea blanca ni cierre de paréntesis."""
    m = re.search(r"^(def %s\(|%s\s*=)" % (re.escape(nombre), re.escape(nombre)), src, re.M)
    if not m:
        raise AssertionError("no está en app.py: " + nombre)
    lineas = src[m.start():].split("\n")
    out = [lineas[0]]
    for ln in lineas[1:]:
        if ln == "" or ln[0] in " \t)]}":
            out.append(ln)
            continue
        break
    return "\n".join(out).rstrip() + "\n"


CUENTA_CONSTANTES = ("_OT_FIN_ZZ_NO_SERVICIO", "_OT_FIN_ORIGENES_NO_COBRO", "_OT_FIN_FUENTE_COBRO",
                     "_OT_FIN_ORIGENES_COBRO_RESPALDO", "_OT_FIN_COBERTURA_TXT", "_OT_FIN_UMBRAL_BAJO",
                     "_OT_FIN_AGG_APARTE", "_OT_FIN_COBERTURA_CORTA")
CUENTA_FUNCIONES = ("_ot_fin_num", "_ot_fin_clp", "_ot_es_interna", "_ot_cobertura", "_ot_finanzas", "_vida_margen_clase",
                    "_ot_fin_agregado")


def cuenta_real():
    """Namespace con la cuenta REAL de la OT y la suma única del proyecto, tal como están en app.py."""
    if "cuenta" in _CACHE:
        return _CACHE["cuenta"]
    src = app_src()
    amb = {"re": re}
    for n in CUENTA_CONSTANTES + CUENTA_FUNCIONES:
        exec(compile(bloque(src, n), "<app:" + n + ">", "exec"), amb)
    _CACHE["cuenta"] = amb
    return amb


def bloque_gerencial():
    """El bloque contiguo del costeo en app.py: desde su encabezado hasta el comentario del HUB que lo sigue."""
    src = app_src()
    ini = src.index("#  📊 COSTEO DE ÓRDENES DE TRABAJO — el tablero gerencial")
    ini = src.rfind("# ═", 0, ini)
    fin = src.index("#  HUB TOP-LEVEL — Dashboard / Órdenes de Trabajo / Plantillas")
    fin = src.rfind("# ═", 0, fin)
    return src[ini:fin]


class Fila(dict):
    """Una fila de mant_visitas con valores por defecto de una OT cerrada normal."""


def visita(id, **kw):
    """Fila tipo mant_visitas + joins, como la devuelve el SELECT del costeo."""
    v = {"id": id, "numero_ot": f"OT-2026-{id:05d}", "titulo": f"Trabajo {id}", "estado": "cerrada", "centro_costo": "sstt",
         "cerrada_at": datetime(2026, 10, 21, 15, 0), "created_by": "ejecutivo1", "created_at": datetime(2026, 10, 14, 13, 0),
         "tecnico_user_id": 10, "proveedor_nombre": None, "responsable_nombre": "Aarón Urbina", "responsable_at": datetime(2026, 10, 15, 14, 0),
         "intencion": None, "asume_area": None,
         "modalidad_cobro": "pagado", "cubierto_por": None, "tipo": "instalacion", "cliente_id": 100 + id % 7, "contrato_id": None,
         "costo": None, "zz_monto": None, "cobro_cero_motivo": None, "zz_codigo": "ZZINSTALACION", "zz_envio_monto": None,
         "valor_origen": "zz", "costo_proveedor": None, "costo_despacho": None, "proveedor_tipo": "externo",
         "valorizado_clp": None, "valorizado_fuente": None, "zz_motivo_manual": None, "ot_madre_id": None, "ot_relacion": None,
         "contrato_real": 0, "madre_valida": 0, "cliente": f"Cliente {100 + id % 7} SpA",
         "tecnico_nombre": "Técnico Externo Uno", "tecnico_role": "tecnico_externo", "te_id": 5, "te_empresa": "Servicios Norte Ltda.",
         "tk_id": None, "numero_ticket": None, "tk_origen": None, "tk_creado_por": None, "tk_creado_at": None}
    v.update(kw)
    return v


def fin_de(v, rep=None):
    return cuenta_real()["_ot_finanzas"](v, rep)


def fila(id, ctx=None, rep=None, **kw):
    """Fila del tablero armada con la cuenta REAL."""
    import ot_gerencial as G
    v = visita(id, **kw)
    return G.armar_fila(v, fin_de(v, rep), ctx or {})


def ot_cobrada(id, cobre, tec, desp=0, **kw):
    return visita(id, zz_monto=cobre, costo_proveedor=tec, costo_despacho=desp, **kw)


def ot_garantia(id, tec, val=None, **kw):
    return visita(id, modalidad_cobro="garantia", cobro_cero_motivo="garantia", costo_proveedor=tec,
                  valorizado_clp=val, **kw)


# ── Doble de la base ─────────────────────────────────────────────────────────────────────────────────────────
class BaseFalsa:
    """Registra cada SQL y devuelve filas según una función. Falla si alguna sentencia no es una lectura."""

    def __init__(self, responder=None):
        self.sqls = []
        self.responder = responder or (lambda sql, params: [])

    def _vigilar(self, sql):
        s = " ".join(str(sql).split())
        self.sqls.append(s)
        primero = s.split(" ", 1)[0].upper()
        if primero not in ("SELECT", "WITH"):
            raise AssertionError("el costeo ejecutó algo que NO es una lectura: " + s[:120])
        palabras = set(re.findall(r"\b[A-Z_]+\b", s))
        malas = palabras & {"INSERT", "UPDATE", "DELETE", "ALTER", "CREATE", "DROP", "TRUNCATE", "REPLACE", "GRANT", "MERGE"}
        if malas:
            raise AssertionError("el costeo ejecutó una escritura: " + ", ".join(sorted(malas)) + " en " + s[:120])

    def fetchall(self, sql, params=None):
        self._vigilar(sql)
        return self.responder(sql, params or ())

    def fetchone(self, sql, params=None):
        self._vigilar(sql)
        r = self.responder(sql, params or ())
        return r[0] if r else None
