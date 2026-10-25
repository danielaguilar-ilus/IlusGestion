# -*- coding: utf-8 -*-
"""Datos de MENTIRA para probar y ver el Costeo de órdenes de trabajo (REGLA #22: nunca datos reales de clientes).
Nombres, personas y montos inventados. Usa la cuenta REAL de la OT (`_ot_finanzas`) sacada de app.py por texto."""
from datetime import date, datetime, timedelta

import ot_gerencial as G
from tests import _ger_app as A

HOY = date(2026, 10, 23)          # viernes: la semana 19–25 de octubre está en curso
CLIENTES = ["Gimnasio Alfa SpA", "Club Deportivo Beta Ltda.", "Fitness Gamma SpA", "Centro Deportivo Delta SpA",
            "Hotel Épsilon Spa & Gym", "Inversiones Zeta Fitness SpA", "Condominio Eta Club House"]
EXTERNOS = [("Servicios Norte Ltda.", "Pedro Ejemplo", 21, 5), ("Técnica Sur SpA", "Marta Demo", 22, 6)]
GERENTE = "Gerente Demo"
USUARIOS = {"ejecutivo1": "Ejecutiva Demo Uno", "ejecutivo2": "Ejecutivo Demo Dos", "admin1": "Administrador Demo"}


def _utc(d, h=15, m=0):
    """Un día/hora de Chile (UTC-3 aprox.) como datetime naive en UTC, para guardar en cerrada_at."""
    return datetime(d.year, d.month, d.day, h + 3, m)


def lunes(offset_semanas=0):
    return G.lunes_de(HOY) - timedelta(days=7 * offset_semanas)


def _ejecutor(i):
    k = i % 5
    if k in (0, 1, 2):
        emp, per, uid, teid = EXTERNOS[k % 2]
        return {"tecnico_user_id": uid, "tecnico_nombre": per, "tecnico_role": "tecnico_externo", "te_id": teid, "te_empresa": emp,
                "proveedor_tipo": "externo", "proveedor_nombre": emp}
    if k == 3:
        return {"tecnico_user_id": 31, "tecnico_nombre": "Jaizer Demo", "tecnico_role": "tecnico_ejecutivo", "te_id": None,
                "te_empresa": None, "proveedor_tipo": "interno", "proveedor_nombre": None}
    return {"tecnico_user_id": 32, "tecnico_nombre": "Técnico Propio Demo", "tecnico_role": "tecnico", "te_id": None,
            "te_empresa": None, "proveedor_tipo": "interno", "proveedor_nombre": None}


def dataset(hoy=HOY):
    """([visitas], {id: ctx}) — 12 semanas de OT cerradas con de todo: ganadas, perdidas, garantías, regalías, arriendo,
    cortesías, desinstalación cubierta, interna, contrato y una sin medir."""
    vs, ctxs = [], {}
    n = 0

    def nueva(sem, dia, **kw):
        nonlocal n
        n += 1
        fecha = lunes(sem) + timedelta(days=dia)
        base = dict(cerrada_at=_utc(fecha), created_at=_utc(fecha - timedelta(days=9)), cliente_id=100 + n % 7,
                    cliente=CLIENTES[n % 7], created_by=("ejecutivo1", "ejecutivo2", "admin1")[n % 3], **_ejecutor(n))
        base.update(kw)
        v = A.visita(n, **base)
        if v["proveedor_tipo"] == "interno" and v.get("costo_proveedor") is not None:
            v["costo_proveedor"] = 0          # un técnico propio no cobra por OT
        vs.append(v)
        return v

    for sem in range(0, 12):
        # 3 OT sanas, una con margen justo
        for dia, (cob, tec) in enumerate([(420000, 330000), (310000, 240000), (240000, 190000)]):
            v = nueva(sem, dia, zz_monto=cob + sem * 3000, costo_proveedor=tec, costo_despacho=15000 if dia == 1 else 0)
            ctxs[v["id"]] = {"clasif": {"codigo": "factura", "codigos": ["factura"], "texto": "Factura con saldo"}}
        if sem % 3 == 0:         # una OT a pérdida (cobra menos de lo que pagó)
            v = nueva(sem, 3, zz_monto=180000, costo_proveedor=240000, **_ejecutor(1))
            ctxs[v["id"]] = {"clasif": {"codigo": "factura", "codigos": ["factura"], "texto": "Factura con saldo"},
                             "autorizaciones": [{"id": 900 + v["id"], "tipo": "cerrar_con_perdida", "estado": "aprobada",
                                                 "argumento": "El cliente renovó el contrato anual; el servicio se hizo bajo precio para no perderlo.",
                                                 "solicitado_por_nombre": USUARIOS["ejecutivo1"], "solicitado_at": _utc(lunes(sem), 10),
                                                 "resuelto_por_nombre": GERENTE, "resuelto_at": _utc(lunes(sem) + timedelta(days=3), 18, 20)}]}
        if sem % 2 == 0:         # una garantía, a veces con costo mayor que su valor
            val = 150000 if sem % 4 else 60000
            v = nueva(sem, 2, **{"modalidad_cobro": "garantia", "cobro_cero_motivo": "garantia", "costo_proveedor": 95000,
                                  "valorizado_clp": val, "tipo": "correctiva", "zz_codigo": None, "zz_monto": None})
            ctxs[v["id"]] = {"clasif": {"codigo": "garantia", "codigos": ["garantia"], "texto": "$0 · garantía"},
                             "autorizaciones": [{"id": 700 + v["id"], "tipo": "cobro_cero", "estado": "aprobada", "motivo": "garantia",
                                                 "argumento": "Garantía de fábrica: falló la tarjeta del equipo a los cuatro meses de la compra; documento de venta adjunto.",
                                                 "solicitado_por_nombre": USUARIOS["ejecutivo2"], "solicitado_at": _utc(lunes(sem), 9),
                                                 "resuelto_por_nombre": GERENTE, "resuelto_at": _utc(lunes(sem) + timedelta(days=1), 12, 5)}]}
        if sem == 0:
            v = nueva(sem, 1, **{"modalidad_cobro": "sin_costo", "cobro_cero_motivo": "regalia", "costo_proveedor": 70000,
                                  "valorizado_clp": 120000, "centro_costo": "marketing", "zz_codigo": None, "zz_monto": None})
            ctxs[v["id"]] = {"clasif": {"codigo": "regalia", "codigos": ["regalia"], "texto": "$0 · regalía"},
                             "autorizaciones": [{"id": 800 + v["id"], "tipo": "cobro_cero", "estado": "aprobada", "motivo": "regalia",
                                                 "argumento": "Visita de cortesía acordada por el área comercial para cerrar la venta del gimnasio nuevo.",
                                                 "solicitado_por_nombre": USUARIOS["ejecutivo1"], "solicitado_at": _utc(lunes(sem), 9),
                                                 "resuelto_por_nombre": GERENTE, "resuelto_at": _utc(lunes(sem) + timedelta(days=1), 11)}]}
            v = nueva(sem, 2, **{"modalidad_cobro": "sin_costo", "cobro_cero_motivo": "arriendo_leasing", "costo_proveedor": 55000,
                                  "valorizado_clp": 90000, "centro_costo": "comercial", "zz_codigo": None, "zz_monto": None})
            ctxs[v["id"]] = {"clasif": {"codigo": "arriendo_leasing", "codigos": ["arriendo_leasing"], "texto": "$0 · arriendo o leasing"}}
            v = nueva(sem, 0, **{"modalidad_cobro": "garantia", "cobro_cero_motivo": "garantia", "costo_proveedor": None,
                                  "valorizado_clp": 80000, "tipo": "correctiva", "zz_codigo": None, "zz_monto": None})
            ctxs[v["id"]] = {"clasif": {"codigo": "garantia", "codigos": ["garantia"], "texto": "$0 · garantía"}}
            v = nueva(sem, 3, zz_monto=300000, costo_proveedor=None)                      # sin medir: falta el costo
            ctxs[v["id"]] = {"clasif": {"codigo": "factura", "codigos": ["factura"], "texto": "Factura con saldo"}}
            v = nueva(sem, 4, cliente_id=None, cliente=None, tipo="revision_interna", modalidad_cobro="interno", zz_codigo=None,
                      zz_monto=None, costo_proveedor=0, proveedor_tipo="interno", tecnico_user_id=32, tecnico_nombre="Técnico Propio Demo",
                      tecnico_role="tecnico", te_id=None, te_empresa=None, proveedor_nombre=None)
            v = nueva(sem, 4, zz_monto=210000, costo_proveedor=190000, tk_id=40 + n, numero_ticket=f"TK-2026-{900 + n}",
                      tk_origen="form", tk_creado_por=None, tk_creado_at=_utc(lunes(sem) - timedelta(days=5), 16))
            ctxs[v["id"]] = {"clasif": {"codigo": "nota_venta", "codigos": ["nota_venta"], "texto": "Nota de venta · falta factura"}}
    return vs, ctxs


def filas_demo():
    vs, ctxs = dataset()
    out = []
    for v in vs:
        ctx = dict(ctxs.get(v["id"], {}))
        ctx.setdefault("nombres", USUARIOS)
        ctx["tipo_txt"] = {"instalacion": "Instalación", "correctiva": "Mantención correctiva", "revision_interna": "Trabajo de bodega"}.get(v["tipo"], v["tipo"])
        ctx["centro_txt"] = {"sstt": "Servicio Técnico", "comercial": "Comercial", "marketing": "Marketing", "logistica": "Logística"}.get(v["centro_costo"], "Sin centro de costo")
        out.append(G.armar_fila(v, A.fin_de(v), ctx))
    return out


def reinc_demo():
    """Cerradas por externos y OT posteriores: Servicios Norte vuelve a tener 2 reincidencias, Técnica Sur ninguna."""
    ah = datetime(2026, 10, 23, 18, 0)
    ej_n = G.armar_ejecutor({"tecnico_role": "tecnico_externo", "te_id": 5, "te_empresa": "Servicios Norte Ltda.", "tecnico_nombre": "Pedro Ejemplo", "tecnico_user_id": 21})
    ej_s = G.armar_ejecutor({"tecnico_role": "tecnico_externo", "te_id": 6, "te_empresa": "Técnica Sur SpA", "tecnico_nombre": "Marta Demo", "tecnico_user_id": 22})
    c = lambda i, cli, dias_atras, ej, maq: {"id": i, "numero": f"OT-2026-{i:05d}", "cliente_id": cli, "cliente": CLIENTES[cli % 7],
                                             "cerrada_at": ah - timedelta(days=dias_atras), "ejecutor": ej, "maquinas": set(maq)}
    cerradas = [c(1, 100, 120, ej_n, [1, 2]), c(2, 101, 100, ej_n, [3]), c(3, 102, 90, ej_n, [4]), c(4, 103, 80, ej_s, [5]),
                c(5, 104, 70, ej_s, [6]), c(6, 105, 20, ej_n, [7])]
    cand = [{"id": 501, "numero": "OT-2026-00501", "cliente_id": 100, "tipo": "correctiva", "es_garantia": False, "estado": "cerrada",
             "created_at": ah - timedelta(days=95), "ot_madre_id": None, "ot_relacion": None, "maquinas": {2}, "ejecutor": ej_n},
            {"id": 502, "numero": "OT-2026-00502", "cliente_id": 101, "tipo": "garantia", "es_garantia": True, "estado": "programada",
             "created_at": ah - timedelta(days=60), "ot_madre_id": None, "ot_relacion": None, "maquinas": {3}, "ejecutor": ej_s}]
    return {"cerradas": cerradas, "candidatas": cand, "dias": 60, "hoy_utc": ah}


def agendadas_demo(periodo):
    ej_n = G.armar_ejecutor({"tecnico_role": "tecnico_externo", "te_id": 5, "te_empresa": "Servicios Norte Ltda.", "tecnico_nombre": "Pedro Ejemplo", "tecnico_user_id": 21})
    d = periodo["desde"]
    return [{"id": 9001, "numero": "OT-2026-09001", "estado": "cerrada", "fecha_programada": d, "ejecutor": ej_n},
            {"id": 9002, "numero": "OT-2026-09002", "estado": "programada", "fecha_programada": d + timedelta(days=1), "ejecutor": ej_n},
            {"id": 9003, "numero": "OT-2026-09003", "estado": "programada", "fecha_programada": d + timedelta(days=5), "ejecutor": ej_n}]


def tablero_demo(modo="semana", ancla=None):
    per = G.rango_periodo(HOY, modo, ancla=ancla)
    cuenta = A.cuenta_real()
    abiertas = [{"cobra": True, "mide": True, "cobre": 450000.0, "a_pagar": 260000.0},
                {"cobra": True, "mide": False, "cobre": 0.0, "a_pagar": 0.0}]
    return G.armar_tablero(
        per, filas_demo(), cuenta["_ot_fin_agregado"], HOY, agendadas=agendadas_demo(per), reinc=reinc_demo(), abiertas=abiertas,
        cxp={"pendiente_n": 3, "pendiente_monto": 1250000.0, "pagadas_n": 2, "pagadas_monto": 640000.0},
        meta={"truncado": False, "limite": 6000, "fallos": [], "generado": "23/10/2026 18:00", "dias_reinc": 60,
              "col_intencion": None, "col_asume": None})
