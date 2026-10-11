"""📒 LIBRO DE SALDO DE FACTURAS + OT a pérdida con autorización (Daniel, 11-oct-2026, antes de la auditoría del martes).

Daniel: «Si ya utilicé una factura y se ocupó el saldo en otra orden de trabajo, ese saldo tiene que quedar bloqueado, a
menos que utilicemos un porcentaje nada más… si tengo 100 mil y uso 10 por un lado y 80 por otro, me quedan 10 mil».
«Solo consume el saldo la OT, que es la realización del servicio; [el ticket y la cotización] heredan». «Te va a indicar
si la factura va a pérdida y eso iría autorizado por mí».

Qué se vigila:
  · La lógica pura (ot_saldo_servicio.py): el ejemplo de los $100.000, los semáforos, los movimientos del libro y la
    pérdida autorizada HASTA un monto.
  · El libro en app.py con un mundo en memoria: consumo, sin cambios (la huella no escribe), liberación al cancelar,
    «carga inicial» para las OT que ya existían, y que el libro NUNCA edita ni borra un movimiento.
  · El candado de pérdida en el cierre con firma (después del costo del proveedor), su autorización nueva en la MISMA
    tabla y con la MISMA columna (REGLA #18: una sola sentencia define el ENUM), y el paso 6 del motor que la ofrece.
  · La herencia: la OT que nace de un ticket trae sus documentos (asistente), y la pantalla muestra dónde viaja la factura.
  · Confidencialidad (REGLA #26): toda ruta nueva con plata es @_no_tecnico.
Sin BD, sin Flask, sin ERP (REGLA #4.1).
Correr con:  py -m unittest tests.test_fac_libro_saldo
"""
import ast
import datetime as _dt
import hashlib
import json
import os
import re
import unittest

import ot_saldo_servicio as S
from tests.test_ot_saldo_servicio import _arbol, _fuente, _decoradores, _leer


def _exec(nombre, amb):
    _codigo, arbol = _arbol()
    for n in arbol.body:
        if isinstance(n, ast.FunctionDef) and n.name == nombre:
            n2 = ast.parse(ast.unparse(n)).body[0]
            n2.decorator_list = []
            exec(compile(ast.Module(body=[n2], type_ignores=[]), "<app>", "exec"), amb)
            return amb[nombre]
    raise AssertionError("no está " + nombre)


# ───────────────────────── lógica pura ─────────────────────────
class TestEjemploDeDaniel(unittest.TestCase):
    def test_100_mil_usa_10_y_80_quedan_10(self):
        e = S.estado_factura({"total": 100000, "usado": 10000 + 80000}, {"total": 0, "usado": 0})
        self.assertEqual(e["estado"], "con_saldo")
        self.assertEqual(e["saldo"], 10000)
        self.assertEqual(e["pct_usado"], 90.0)
        v = S.puede_usar({"total": 100000, "usado": 90000}, {"total": 0, "usado": 0})
        self.assertTrue(v["ok"])
        self.assertIn("$10.000", v["texto"])

    def test_usada_completa_queda_bloqueada(self):
        e = S.estado_factura({"total": 100000, "usado": 100000}, {"total": 30000, "usado": 30000})
        self.assertEqual(e["estado"], "agotada")
        v = S.puede_usar({"total": 100000, "usado": 100000}, {"total": 30000, "usado": 30000})
        self.assertFalse(v["ok"])
        self.assertIn("autorización de Daniel", v["texto"])

    def test_pedido_mayor_al_saldo_no_alcanza(self):
        v = S.puede_usar({"total": 100000, "usado": 90000}, {"total": 0, "usado": 0}, {"servicio": 20000})
        self.assertFalse(v["ok"])
        self.assertEqual(v["titulo"], "No alcanza el saldo")

    def test_doble_cobro_se_ve_en_rojo(self):
        e = S.estado_factura({"total": 100000, "usado": 150000}, {"total": 0, "usado": 0})
        self.assertEqual(e["estado"], "excedida")
        self.assertEqual(e["exceso"], 50000)
        self.assertEqual(e["clase"], "rojo")

    def test_otros_estados(self):
        self.assertEqual(S.estado_factura({"total": 100, "usado": 0}, {})["estado"], "libre")
        self.assertEqual(S.estado_factura({"total": 0, "usado": 0}, {"total": 0, "usado": 0})["estado"], "sin_lineas")
        self.assertEqual(S.estado_factura({"total": 100, "usado": 0}, {}, leido=False)["estado"], "sin_leer")
        for k in S.ORDEN_ESTADOS:
            self.assertIn(k, S.ESTADOS_FACTURA)


class TestMovimientosDelLibro(unittest.TestCase):
    def test_consumo_liberacion_y_sin_cambio(self):
        a = {("FCV", "1", "servicio"): 50000, ("FCV", "1", "despacho"): 10000, ("FCV", "2", "servicio"): 5000}
        b = {("FCV", "1", "servicio"): 80000, ("FCV", "1", "despacho"): 10000}
        movs = S.movimientos_libro(a, b)
        self.assertEqual(len(movs), 2)
        m1 = next(m for m in movs if m["nudo"] == "1")
        self.assertEqual((m1["movimiento"], m1["monto"], m1["acumulado"]), ("consumo", 30000, 80000))
        m2 = next(m for m in movs if m["nudo"] == "2")
        self.assertEqual((m2["movimiento"], m2["monto"], m2["acumulado"]), ("liberacion", -5000, 0))
        self.assertEqual(S.movimientos_libro(b, dict(b)), [])

    def test_aportes_de_una_ot_solo_suyos_y_sin_canceladas(self):
        filas = [{"vid": 7, "estado": "programada", "clave": ("FCV", "1"), "servicio": 50000, "despacho": 0},
                 {"vid": 7, "estado": "programada", "clave": ("FCV", "1"), "servicio": 40000, "despacho": 9000},
                 {"vid": 8, "estado": "programada", "clave": ("FCV", "1"), "servicio": 999, "despacho": 0},
                 {"vid": 9, "estado": "cancelada", "clave": ("FCV", "1"), "servicio": 1, "despacho": 0}]
        self.assertEqual(S.aportes_de_ot(filas, 7), {("FCV", "1", "servicio"): 50000, ("FCV", "1", "despacho"): 9000})
        self.assertEqual(S.aportes_de_ot(filas, 9), {})


class TestPerdidaPura(unittest.TestCase):
    def test_sin_perdida_pasa(self):
        self.assertTrue(S.perdida_cierre(100000, 60000)["ok"])

    def test_perdida_sin_autorizar_no_pasa(self):
        r = S.perdida_cierre(100000, 125000)
        self.assertEqual((r["perdida"], r["ok"], r["falta_autorizar"]), (25000, False, 25000))

    def test_autorizada_hasta_un_monto(self):
        self.assertTrue(S.perdida_cierre(100000, 125000, autorizada=25000)["ok"])
        r = S.perdida_cierre(100000, 140000, autorizada=25000)   # creció después de autorizar: se vuelve a pedir
        self.assertFalse(r["ok"])
        self.assertEqual(r["falta_autorizar"], 15000)


# ───────────────────────── el libro en app.py (mundo en memoria) ─────────────────────────
class MundoLibro:
    def __init__(self):
        self.ots = {}        # vid → fila de mant_visitas (para la huella)
        self.aportes = {}    # vid → {(t, n, c): monto}
        self.libro = []      # filas de ot_fac_libro
        self.huellas = {}
        self.sql = []
        amb = {"print": lambda *a, **k: None, "_saldo": S, "datetime": _dt.datetime, "get_mysql": self._conn,
               "mysql_fetchone": self.fetchone, "mysql_fetchall": self.fetchall, "mysql_execute": self.execute,
               "_fac_aportes_ot": lambda vid: dict(self.aportes.get(int(vid), {})),
               "_ot_aut_usuario": lambda: (5, "Ejecutivo de SSTT"), "current_username": lambda: "ejecutivo",
               "chile_fmt_filter": lambda v, f="%d/%m/%Y %H:%M": v.strftime(f), "_FAC_LIBRO_DESDE": _dt.datetime(2026, 10, 11)}
        _exec("_fac_huella_ot", amb)
        _exec("_fac_libro_ultimo", amb)
        self.conciliar = _exec("_fac_libro_conciliar_ot", amb)

    def _conn(self):
        class C:
            def cursor(s):
                class K:
                    def __enter__(k): return k
                    def __exit__(k, *a): return False
                    def execute(k, *a): return None
                return K()
            def close(s): pass
        return C()

    def fetchone(self, sql, params=()):
        self.sql.append(sql)
        if "FROM mant_visitas v" in sql:
            v = self.ots.get(int(params[0]))
            if not v:
                return None
            return dict(v, h_prev=self.huellas.get(int(params[0])))
        if "FROM ot_fac_huella" in sql:
            h = self.huellas.get(int(params[0]))
            return {"huella": h} if h else None
        return None

    def fetchall(self, sql, params=()):
        self.sql.append(sql)
        if "FROM ot_fac_libro" in sql:
            vid = int(params[0])
            ult = {}
            for r in self.libro:
                if r["visita_id"] == vid:
                    ult[(r["doc_tido"], r["doc_nudo"], r["categoria"])] = r
            return list(ult.values())
        return []

    def execute(self, sql, params=()):
        self.sql.append(sql)
        if sql.startswith("INSERT INTO ot_fac_libro"):
            cols = ("doc_tido", "doc_nudo", "categoria", "movimiento", "monto", "acumulado_ot", "visita_id", "numero_ot",
                    "cliente", "origen", "usuario", "user_id", "detalle")
            self.libro.append(dict(zip(cols, params), id=len(self.libro) + 1))
        elif sql.startswith("INSERT INTO ot_fac_huella"):
            self.huellas[int(params[0])] = params[1]
        elif sql.startswith("DELETE FROM ot_fac_huella"):
            self.huellas.pop(int(params[0]), None)

    def ot(self, vid, creada=_dt.datetime(2026, 10, 12), **k):
        base = {"id": vid, "numero_ot": f"OT-2026-{vid:05d}", "created_at": creada, "estado": "programada",
                "modalidad_cobro": None, "cobro_cero_motivo": None, "valor_origen": "zz", "zz_monto": 0,
                "zz_envio_monto": 0, "factura_tido": "FCV", "factura_nudo": "0000011439", "cliente": "Gimnasio de ejemplo SpA",
                "docs": ""}
        base.update(k)
        self.ots[vid] = base


class TestLibroEnApp(unittest.TestCase):
    def test_consumo_y_sin_cambio_no_escribe(self):
        m = MundoLibro()
        m.ot(1, zz_monto=10000, docs="FCV:0000011439:1:-:10000:-")
        m.aportes[1] = {("FCV", "11439", "servicio"): 10000}
        r = m.conciliar(1, "crear")
        self.assertEqual(r["movimientos"], 1)
        self.assertEqual(m.libro[0]["movimiento"], "consumo")
        self.assertEqual(m.libro[0]["monto"], 10000)
        self.assertEqual(m.libro[0]["origen"], "crear")
        self.assertEqual(m.libro[0]["usuario"], "Ejecutivo de SSTT")
        # misma plata: la huella no cambió → ni una escritura más
        n = len(m.libro)
        self.assertEqual(m.conciliar(1, "edicion")["movimientos"], 0)
        self.assertEqual(len(m.libro), n)

    def test_dos_ot_10_y_80_y_al_cancelar_se_libera(self):
        m = MundoLibro()
        m.ot(1, zz_monto=10000, docs="a")
        m.aportes[1] = {("FCV", "11439", "servicio"): 10000}
        m.ot(2, zz_monto=80000, docs="b")
        m.aportes[2] = {("FCV", "11439", "servicio"): 80000}
        m.conciliar(1, "crear")
        m.conciliar(2, "crear")
        usado = sum(r["monto"] for r in m.libro)
        self.assertEqual(usado, 90000)       # de $100.000 quedan $10.000
        m.ots[2]["estado"] = "cancelada"     # la cancelada deja de consumir: se libera
        m.aportes[2] = {}
        r = m.conciliar(2, "estado")
        self.assertEqual(r["movimientos"], 1)
        self.assertEqual(m.libro[-1]["movimiento"], "liberacion")
        self.assertEqual(m.libro[-1]["monto"], -80000)
        self.assertEqual(sum(x["monto"] for x in m.libro), 10000)

    def test_ot_anterior_al_libro_es_carga_inicial(self):
        m = MundoLibro()
        m.ot(3, creada=_dt.datetime(2026, 9, 1), zz_monto=50000, docs="c")
        m.aportes[3] = {("FCV", "11439", "servicio"): 50000}
        m.conciliar(3, "revision")
        self.assertEqual(m.libro[0]["origen"], "inicial")
        self.assertEqual(m.libro[0]["usuario"], "Sistema")
        self.assertIn("11/10/2026", m.libro[0]["detalle"])

    def test_ot_eliminada_libera_todo(self):
        m = MundoLibro()
        m.ot(4, zz_monto=30000, docs="d")
        m.aportes[4] = {("FCV", "11439", "servicio"): 30000}
        m.conciliar(4, "crear")
        del m.ots[4]
        m.aportes[4] = {}
        r = m.conciliar(4, "eliminada")
        self.assertEqual(r["movimientos"], 1)
        self.assertEqual(m.libro[-1]["origen"], "eliminada")
        self.assertNotIn(4, m.huellas)

    def test_el_libro_nunca_se_edita_ni_se_borra(self):
        app = _leer("app.py")
        self.assertNotRegex(app, r"UPDATE\s+ot_fac_libro")
        self.assertNotRegex(app, r"DELETE\s+FROM\s+ot_fac_libro")


class TestCableado(unittest.TestCase):
    def test_tablas_al_arrancar_y_siempre(self):
        app = _leer("app.py")
        self.assertIn("_ensure_fac_libro()", app)
        f = _fuente("_ensure_fac_libro")
        for t in ("ot_fac_libro", "ot_fac_doc", "ot_fac_huella", "ot_fac_cursor"):
            self.assertIn("CREATE TABLE IF NOT EXISTS " + t, f)

    def test_registro_tras_cada_escritura(self):
        f = _fuente("_fac_libro_tras_escritura")
        self.assertIn('"/ot/"', f)
        self.assertIn('"/mantenciones/"', f)
        self.assertIn('"/tickets/"', f)
        self.assertIn("_fac_libro_conciliar_ot(", f)
        self.assertIn("_fac_doc_refrescar(", f)
        self.assertIn("app.after_request", _decoradores("_fac_libro_tras_escritura"))
        self.assertIn("_fac_libro_marcar(vid)", _fuente("_ot_saldo_reanotar_aportes"))

    def test_mismo_motor_que_el_candado(self):
        self.assertIn("_ot_saldo_usos({k}, None, cap, solo_vid=vid)", _fuente("_fac_aportes_ot"))
        self.assertIn("_ot_saldo_doc(", _fuente("_fac_doc_refrescar"))
        u = _fuente("_ot_saldo_usos")
        self.assertEqual(u.count("if solo:"), 3)

    def test_barrido_programado_cada_10_minutos(self):
        self.assertIn("_fac_libro_barrido(", _fuente("tr_cron_simpliroute_poll"))

    def test_rutas_nuevas_no_son_para_tecnicos(self):
        for r in ("ot_saldo_facturas_pagina", "ot_api_saldo_facturas", "ot_api_saldo_facturas_doc",
                  "ot_api_saldo_facturas_revisar", "ot_saldo_facturas_excel"):
            self.assertIn("_no_tecnico", _decoradores(r), r)

    def test_menu(self):
        b = _leer("templates/base.html")
        i = b.index("ot_saldo_facturas_pagina")
        self.assertLess(b.rfind("{% if not es_tecnico %}", 0, i), i)


class TestPerdidaEnElCierre(unittest.TestCase):
    def test_tipo_nuevo_en_la_misma_tabla_y_una_sola_sentencia(self):
        app = _leer("app.py")
        self.assertIn('"cerrar_con_perdida")', app.split("_OT_AUT_TIPOS = ", 1)[1].split("\n", 1)[0])
        self.assertIn("'exceder_saldo','cerrar_con_perdida') NOT NULL", app)
        self.assertNotIn("MODIFY tipo ENUM('crear_sin_documento','cobro_cero','cerrar_sin_documento','exceder_saldo') NOT NULL", app)

    def test_candado_despues_del_costo_del_proveedor_y_antes_del_anexo(self):
        f = _fuente("mant_ot_aprobar_cierre")
        i_prov = f.index('"error_codigo": "SIN_COSTO_PROVEEDOR"')
        i_per = f.index('"CIERRE_CON_PERDIDA"')
        self.assertLess(i_prov, i_per)
        self.assertIn("_fac_perdida_de_ot(vid)", f)
        self.assertIn('_cob_cierre == "cobra"', f[i_prov:i_per + 200])
        self.assertIn('"accion": _ot_cierre_accion("CIERRE_CON_PERDIDA", vid)', f)

    def test_perdida_info_lee_la_autorizacion_mayor(self):
        filas = [{"id": 1, "estado": "aprobada", "payload_json": json.dumps({"perdida": 10000}), "resuelto_por_nombre": "Daniel"},
                 {"id": 2, "estado": "aprobada", "payload_json": json.dumps({"perdida": 25000}), "resuelto_por_nombre": "Daniel Aguilar"},
                 {"id": 3, "estado": "pendiente", "payload_json": "{}", "resuelto_por_nombre": None}]
        amb = {"mysql_fetchall": lambda *a, **k: filas, "json": json, "_saldo": S, "print": lambda *a, **k: None}
        f = _exec("_fac_perdida_info", amb)
        fin = {"cobra": True, "cobre": {"total": 100000, "hay": True}, "me_cobraron": {"total": 125000}, "queda": {"mostrar": True}}
        r = f(9, fin)
        self.assertTrue(r["ok"])
        self.assertEqual(r["autorizada"], 25000)
        self.assertEqual(r["autorizada_por"], "Daniel Aguilar")
        self.assertEqual(r["pendiente_id"], 3)
        fin["me_cobraron"]["total"] = 150000
        self.assertFalse(f(9, fin)["ok"])

    def test_solicitud_guarda_la_cuenta_y_aprobar_deja_constancia(self):
        c = _fuente("ot_aut_api_crear")
        self.assertIn('if tipo == "cerrar_con_perdida":', c)
        self.assertIn('"perdida": _fp["perdida"]', c)
        a = _fuente("ot_aut_api_aprobar")
        self.assertIn('elif tipo == "cerrar_con_perdida":', a)
        self.assertIn("cierre_con_perdida_autorizado", a)

    def test_paso_6_y_motor_ofrecen_pedir_la_autorizacion(self):
        r = _fuente("ot_api_recorrido")
        self.assertIn('"accion": "pedir_aut_perdida"', r)
        self.assertIn('"perdida": perdida', r)
        js = _leer("static/ot_fin_motor.js")
        self.assertIn("p.accion === 'pedir_aut_perdida'", js)
        self.assertIn("pedir_aut_perdida: 'pedirPerdida'", js)
        self.assertIn("tipo: 'cerrar_con_perdida'", js)
        self.assertIn("a.tipo === 'cerrar_con_perdida'", _leer("static/ot_autorizaciones.js"))


class TestExcelAuditoria(unittest.TestCase):
    def test_el_excel_se_arma_y_se_abre(self):
        import io as _io
        from openpyxl import load_workbook
        hoy = _dt.datetime(2026, 10, 11, 21, 0)
        docs = [{"doc_tido": "FCV", "doc_nudo": "11610", "titulo": "FCV 11610", "cliente": "Centro de ejemplo", "cliente_rut": "76.555.444-3",
                 "fecha_doc": "07/10/2026", "total_doc": 1850000, "cap_servicio": 100000, "cap_despacho": 30000,
                 "usado_servicio": 90000, "usado_despacho": 0, "n_ot": 2, "ots_txt": "OT-2026-00320, OT-2026-00322",
                 "estado": "con_saldo", "familia_de": None, "erp_leido": 1, "actualizado_el": hoy},
                {"doc_tido": "FCV", "doc_nudo": "11502", "titulo": "FCV 11502", "cliente": "Club de ejemplo", "cliente_rut": "",
                 "fecha_doc": "02/10/2026", "total_doc": 0, "cap_servicio": 300000, "cap_despacho": 0,
                 "usado_servicio": 360000, "usado_despacho": 0, "n_ot": 2, "ots_txt": "OT-2026-00311, OT-2026-00317",
                 "estado": "excedida", "familia_de": None, "erp_leido": 1, "actualizado_el": hoy}]
        libro = [{"id": 1, "creado_el": hoy, "doc_tido": "FCV", "doc_nudo": "11610", "categoria": "servicio", "movimiento": "consumo",
                  "monto": 80000, "acumulado_ot": 80000, "numero_ot": "OT-2026-00322", "cliente": "Centro de ejemplo",
                  "origen": "crear", "usuario": "Ejecutivo de SSTT", "detalle": ""}]
        salida = {}

        def fetchall(sql, params=()):
            if "FROM ot_fac_doc" in sql:
                return docs
            if "FROM ot_fac_libro ORDER BY id" in sql:
                return libro
            if "SELECT DISTINCT visita_id FROM ot_fac_libro" in sql:
                return [{"visita_id": 322}]
            return []

        def fetchone(sql, params=()):
            if "FROM ot_fac_huella" in sql:
                return {"r": hoy, "n": 860}
            if "numero_ot FROM mant_visitas" in sql:
                return {"numero_ot": "OT-2026-00322"}
            return {}

        def send_file(buf, **k):
            salida["buf"], salida["k"] = buf, k
            return "ok"
        amb = {"mysql_fetchall": fetchall, "mysql_fetchone": fetchone, "send_file": send_file, "io": _io, "datetime": _dt.datetime,
               "jsonify": lambda d: d, "print": lambda *a, **k: None, "_saldo": S, "current_username": lambda: "daniel",
               "chile_fmt_filter": lambda v, f="%d/%m/%Y %H:%M": v.strftime(f),
               "_ot_doc_real_a_usuario": lambda t, n: (t, n.lstrip("0")), "_ot_saldo_real": lambda k: k,
               "_FAC_ORIGEN_TXT": {"crear": "Creación de la OT"},
               "_fac_finanzas_de_ots": lambda vids: {322: {"cobra": True, "cobre": 80000, "me_cobraron": 95000, "queda": -15000,
                                                          "frase": "Cobré $80.000 − me cobraron $95.000", "estado": "pendiente_aprobacion",
                                                          "estado_txt": "Pendiente de aprobación"}}}
        _exec("_fac_fila_lista", amb)
        f = _exec("ot_saldo_facturas_excel", amb)
        self.assertEqual(f(), "ok")
        self.assertTrue(salida["k"]["download_name"].startswith("Saldo_de_facturas_"))
        wb = load_workbook(salida["buf"])
        self.assertEqual(wb.sheetnames, ["Cómo funciona", "Resumen", "Facturas", "Alertas", "Movimientos"])
        alertas = [[c.value for c in r] for r in wb["Alertas"].iter_rows(min_row=2)]
        self.assertTrue(any(a[0] == "Doble cobro" and a[1] == "FCV 11502" for a in alertas))
        self.assertTrue(any(a[0] == "OT a pérdida" and "Sin autorización" in (a[5] or "") for a in alertas))
        self.assertEqual(wb["Movimientos"].cell(row=2, column=6).value, 80000)
        texto = " ".join(str(c.value or "") for c in wb["Cómo funciona"]["A"])
        self.assertIn("Solo la OT consume el saldo", texto)
        self.assertIn("ILUS Fitness", texto)


class TestHerenciaYPantalla(unittest.TestCase):
    def test_la_ot_del_ticket_hereda_sus_documentos(self):
        h = _leer("templates/ot2/_modal_crear.html")
        self.assertIn("function _o2mHeredarDocsTicket(docs, numeroTicket)", h)
        self.assertIn("_o2mHeredarDocsTicket(d.documentos, t.numero_ticket);", h)
        self.assertIn("_o2mHeredarDocsTicket(j && j.documentos, t.numero_ticket);", h)
        # el documento heredado entra por el MISMO camino que si la persona lo buscara
        i = h.index("function _o2mHeredarDocsTicket")
        self.assertIn("_o2mFinDocumentoSeleccionado([], {", h[i:i + 2500])

    def test_pantalla_sin_alert_nativo_y_paginada(self):
        js = _leer("static/ot_saldo_facturas.js")
        self.assertNotRegex(js, r"\b(alert|confirm|prompt)\(")
        self.assertIn("Mostrando ", js)
        self.assertIn("Anterior", js)
        self.assertIn("Siguiente", js)
        html = _leer("templates/ot2/saldo_facturas.html")
        self.assertIn("ot_saldo_facturas_excel", html)

    def test_marca_ilus_fitness(self):
        self.assertIn("ILUS Fitness", _leer("templates/ot2/saldo_facturas.html"))
        self.assertNotIn("Sport & Health", _leer("templates/ot2/saldo_facturas.html"))


if __name__ == "__main__":
    unittest.main()
