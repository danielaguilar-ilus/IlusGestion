"""🚩 Marca PERSISTENTE de «factura sin saldo» (Daniel, 12-oct-2026).

Daniel: «Quiero saber qué pasa si ingresamos una OT con una factura ya consumida… si no tiene saldo igual es
sospechoso, por eso tenemos que avisar hasta en la tabla… para que no se omita el conocimiento de parte del usuario»
y «que sea persistente por favor, eso es muy importante». Y: «los técnicos… jamás las finanzas o costos».

Qué se vigila:
  · La regla pura: factura consumida por otras y esta OT no recibe nada → 'sin_saldo'; le alcanza una parte →
    'saldo_insuficiente'; factura sin líneas de servicio → nada que controlar.
  · El motor (mismo del candado de pago) entrega la alerta por factura.
  · La marca es PERSISTENTE: nueva → vigente + bitácora; corregida → 'resuelta' con fecha (nunca se borra); vuelve a
    pasar → vigente otra vez (veces + 1); Random caído → no cambia nada (no se resuelve a ciegas).
  · Se ve en la tabla de OT, la ficha, Facturación de proveedor (OT por facturar y lote) y Regularizar.
  · Un técnico nunca la recibe: se quita en el servidor (REGLA #26).
Sin BD, sin Flask, sin ERP.
Correr con:  py -m unittest tests.test_fac_alerta_sin_saldo
"""
import datetime
import re
import unittest

import ot_saldo_servicio as S
from tests.test_fac_libro_saldo import _exec
from tests.test_facprov_equilibrio import _motor, OTRA
from tests.test_ot_saldo_servicio import _fuente, _leer

CAP = {"servicio": 100000, "despacho": 0}


class TestReglaPura(unittest.TestCase):
    def test_factura_consumida_y_esta_ot_no_recibe_nada(self):
        self.assertEqual(S.clasificar_sin_saldo(0, CAP, {"servicio": 100000, "despacho": 0}, 0, True), "sin_saldo")

    def test_otra_ot_con_prioridad_ya_uso_todo(self):
        self.assertEqual(S.clasificar_sin_saldo(100000, CAP, {"servicio": 100000}, 100000, True), "sin_saldo")

    def test_le_alcanza_solo_una_parte(self):
        self.assertEqual(S.clasificar_sin_saldo(100000, CAP, {"servicio": 60000}, 60000, True), "saldo_insuficiente")

    def test_con_saldo_no_marca(self):
        self.assertIsNone(S.clasificar_sin_saldo(40000, CAP, {"servicio": 60000}, 0, True))
        self.assertIsNone(S.clasificar_sin_saldo(0, CAP, {"servicio": 60000}, 0, True))      # queda saldo: no es sospechoso
        self.assertIsNone(S.clasificar_sin_saldo(0, CAP, {}, 0, False))                      # nadie más la usa

    def test_factura_sin_lineas_de_servicio_no_se_controla(self):
        self.assertIsNone(S.clasificar_sin_saldo(0, {"servicio": 0, "despacho": 0}, {"servicio": 0}, 0, True))


class TestMotor(unittest.TestCase):
    def test_factura_repetida_ya_pagada_por_otra_ot_queda_marcada(self):
        r = _motor(usos=[OTRA], pagados={60})(50)
        self.assertEqual([a["tipo"] for a in r["alertas_doc"]], ["sin_saldo"])
        self.assertEqual(r["alertas_doc"][0]["otras"], ["OT-2026-00360"])
        self.assertEqual(r["alertas_doc"][0]["clave"], ["FCV", "11439"])

    def test_ot_que_asocio_una_factura_consumida_sin_tomar_nada(self):
        r = _motor(aportes={}, usos=[OTRA])(50)
        self.assertEqual([a["tipo"] for a in r["alertas_doc"]], ["sin_saldo"])

    def test_con_saldo_no_hay_alerta(self):
        self.assertFalse(_motor()(50).get("alertas_doc"))


class MundoAlerta(object):
    """Base de mentira para _fac_alerta_revisar: ot_fac_alerta + bitácora."""

    def __init__(self, eq, estado_ot="cerrada"):
        self.eq, self.estado_ot = eq, estado_ot
        self.filas, self.logs, self.sql = [], [], []

    def fetchone(self, sql, p=None):
        return {"estado": self.estado_ot} if "FROM mant_visitas" in sql else None

    def fetchall(self, sql, p=None):
        return [dict(f) for f in self.filas if f["visita_id"] == p[0]]

    def execute(self, sql, p=None):
        self.sql.append(sql)
        if sql.startswith("INSERT INTO ot_fac_alerta"):
            self.filas.append({"id": len(self.filas) + 1, "visita_id": p[0], "doc_tido": p[1], "doc_nudo": p[2],
                               "titulo": p[3], "tipo": p[4], "estado": "vigente", "veces": 1})
        elif "SET estado='resuelta'" in sql:
            next(f for f in self.filas if f["id"] == p[2])["estado"] = "resuelta"
        elif "SET estado='vigente'" in sql:
            f = next(f for f in self.filas if f["id"] == p[4])
            f["estado"], f["veces"] = "vigente", f["veces"] + 1

    def revisar(self):
        amb = {"mysql_fetchone": self.fetchone, "mysql_fetchall": self.fetchall, "mysql_execute": self.execute,
               "_fac_equilibrio_ot": lambda vid: self.eq, "_mant_log": lambda *a: self.logs.append(a),
               "_fac_alerta_texto": _exec("_fac_alerta_texto", {}), "_FAC_ALERTA_TIPO_TXT": {"sin_saldo": "Factura sin saldo"},
               "datetime": datetime.datetime, "print": lambda *a, **k: None}
        return _exec("_fac_alerta_revisar", amb)([50], quien="Daniel Aguilar")


ALERTA = {"clave": ["FCV", "11439"], "titulo": "FCV 11439", "tipo": "sin_saldo", "otras": ["OT-2026-00360"], "otras_vid": [60]}


class TestPersistente(unittest.TestCase):
    def test_nueva_queda_vigente_y_en_la_bitacora(self):
        m = MundoAlerta({"alertas_doc": [ALERTA]})
        m.revisar()
        self.assertEqual(m.filas[0]["estado"], "vigente")
        self.assertEqual(m.logs[0][2], "factura_sin_saldo")
        self.assertIn("ya estaba consumida por OT-2026-00360", m.logs[0][3])
        self.assertIn("Daniel Aguilar", m.logs[0][3])

    def test_corregida_queda_resuelta_y_nunca_se_borra(self):
        m = MundoAlerta({"alertas_doc": [ALERTA]})
        m.revisar()
        m.eq = {"alertas_doc": []}
        m.revisar()
        self.assertEqual(len(m.filas), 1)
        self.assertEqual(m.filas[0]["estado"], "resuelta")
        self.assertEqual(m.logs[-1][2], "factura_sin_saldo_resuelta")
        self.assertFalse(any(s.lstrip().upper().startswith("DELETE") for s in m.sql))

    def test_si_vuelve_a_pasar_vuelve_a_vigente(self):
        m = MundoAlerta({"alertas_doc": [ALERTA]})
        m.revisar()
        m.eq = {"alertas_doc": []}
        m.revisar()
        m.eq = {"alertas_doc": [ALERTA]}
        m.revisar()
        self.assertEqual((m.filas[0]["estado"], m.filas[0]["veces"]), ("vigente", 2))

    def test_random_caido_no_resuelve_a_ciegas(self):
        m = MundoAlerta({"alertas_doc": [ALERTA]})
        m.revisar()
        m.eq = {"code": "LECTURA_INCOMPLETA", "estado": "sin_verificar"}
        m.revisar()
        self.assertEqual(m.filas[0]["estado"], "vigente")

    def test_ot_cancelada_resuelve(self):
        m = MundoAlerta({"alertas_doc": [ALERTA]})
        m.revisar()
        m.estado_ot = "cancelada"
        m.revisar()
        self.assertEqual(m.filas[0]["estado"], "resuelta")

    def test_ningun_delete_sobre_la_tabla(self):
        src = _leer("app.py")
        self.assertIsNone(re.search(r"DELETE\s+FROM\s+ot_fac_alerta", src, re.I))
        self.assertIsNone(re.search(r"TRUNCATE\s+(TABLE\s+)?ot_fac_alerta", src, re.I))


class TestDondeSeVe(unittest.TestCase):
    def test_se_revisa_al_escribir_y_en_el_barrido(self):
        self.assertIn("_fac_alerta_revisar(sorted(vids)[:3], quien=current_username(), vecinos=4)",
                      _fuente("_fac_libro_tras_escritura"))
        b = _fuente("_fac_libro_barrido")
        self.assertIn("estado='vigente'", b)
        self.assertIn("ot_fac_cursor (id, desde, vuelta, actualizado) VALUES (2", b)

    def test_tabla_de_ot_ficha_facturacion_y_regularizar(self):
        self.assertIn("_fac_alertas_de(ids)", _fuente("_ot2_panel_extras"))
        self.assertIn('class="ot2-sinsaldo {{ _fa.estado }}"', _leer("templates/ot2/panel.html"))
        self.assertIn("fac_alerta=fac_alerta", _fuente("ot2_detalle"))
        self.assertIn('class="otd-sinsaldo {{ fac_alerta.estado }}"', _leer("templates/ot2/detalle.html"))
        self.assertIn("return _mfp_agregar_alertas(filas)", _fuente("_mfp_por_facturar"))
        self.assertIn("return _mfp_agregar_alertas(out)", _fuente("_mfp_items"))
        self.assertIn("_fac_alertas_de([it[\"id\"] for it in pagina])", _leer("app.py"))

    def test_un_tecnico_nunca_la_recibe(self):
        x = _fuente("_ot2_panel_extras")
        self.assertLess(x.index("if not _es_rol_tecnico():"), x.index("_fac_alertas_de(ids)"))
        self.assertIn("fac_alerta = None if _es_rol_tecnico() else _fac_alertas_de([vid])", _fuente("ot2_detalle"))
        self.assertIn("if not filas or _es_rol_tecnico():", _fuente("_mfp_agregar_alertas"))
        self.assertIn("{% if fac_alerta and not es_tecnico %}", _leer("templates/ot2/detalle.html"))


if __name__ == "__main__":
    unittest.main()
