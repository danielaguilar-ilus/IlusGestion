"""Solicitudes de repuesto: 5 etapas y plazos en horas hábiles (2026-10-04,
Daniel: "hay mucho estado" + tabla como el Monitor de Retiros "con tiempo
para responder en plazos" y que moleste).

Prueba _otrep_horas_habiles, _otrep_sumar_horas_habiles y _otrep_etapa_y_plazo
extrayéndolas de app.py con ast (importar app.py levanta Flask y la base).

Correr con:  py -m unittest tests.test_repuestos_etapas_plazos
"""
import ast
import os
import unittest
from datetime import datetime, timedelta

APP_PY = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "app.py")
_FUN = ("_otrep_feriados", "_otrep_a_chile", "_otrep_dia_habil", "_otrep_horas_habiles",
        "_otrep_sumar_horas_habiles", "_otrep_fmt_horas", "_otrep_etapa_y_plazo")
_CONST = ("_OTREP_ETAPA_LABEL", "_OTREP_PLAZO_H", "_OTREP_JORNADA", "_OTREP_COMPRA_DIAS_DEF", "_OTREP_FERIADOS")
_AMB = {}


def _cargar():
    if _AMB:
        return _AMB
    with open(APP_PY, encoding="utf-8") as fh:
        arbol = ast.parse(fh.read())
    nodos = [n for n in arbol.body
             if (isinstance(n, ast.FunctionDef) and n.name in _FUN)
             or (isinstance(n, ast.Assign) and any(getattr(t, "id", "") in _CONST for t in n.targets))]
    _AMB.update({"datetime": datetime, "timedelta": timedelta})
    exec(compile(ast.Module(body=nodos, type_ignores=[]), "<app>", "exec"), _AMB)
    return _AMB


class HorasHabiles(unittest.TestCase):
    def setUp(self):
        self.a = _cargar()

    def test_fin_de_semana_no_cuenta(self):
        # viernes 2026-10-02 16:00 -> lunes 2026-10-05 09:00 = 1 h (vie) + 1 h (lun)
        h = self.a["_otrep_horas_habiles"](datetime(2026, 10, 2, 16), datetime(2026, 10, 5, 9))
        self.assertAlmostEqual(h, 2.0)

    def test_feriado_no_cuenta(self):
        # 2026-09-18 y 19 son feriados (Fiestas Patrias)
        h = self.a["_otrep_horas_habiles"](datetime(2026, 9, 17, 16), datetime(2026, 9, 21, 9))
        self.assertAlmostEqual(h, 2.0)

    def test_sumar_horas_salta_noche_y_fin_de_semana(self):
        v = self.a["_otrep_sumar_horas_habiles"](datetime(2026, 10, 2, 16), 4)
        self.assertEqual(v, datetime(2026, 10, 5, 11))


class EtapasYPlazos(unittest.TestCase):
    def setUp(self):
        self.f = _cargar()["_otrep_etapa_y_plazo"]

    def _base(self, **kw):
        s = {"abierta": True, "estado": "solicitado", "created_at": datetime.utcnow() - timedelta(minutes=5)}
        s.update(kw)
        return s

    def test_por_gestionar_con_plazo(self):
        out = self.f(self._base())
        self.assertEqual(out["etapa"], "gestionar")
        self.assertIsNotNone(out["plazo"])

    def test_fuera_de_servicio_plazo_mas_corto(self):
        normal = self.f(self._base())
        fs = self.f(self._base(prioridad_fs=True))
        self.assertIn("4 h", fs["plazo"]["objetivo"])
        self.assertNotEqual(normal["plazo"]["objetivo"], fs["plazo"]["objetivo"])

    def test_validado_con_stock_es_lista_para_ot(self):
        out = self.f(self._base(estado="validado", validado_at=datetime.utcnow(), stock_disponible=2))
        self.assertEqual(out["etapa"], "ot")

    def test_validado_sin_stock_es_lista_para_lote(self):
        out = self.f(self._base(estado="validado", validado_at=datetime.utcnow(), stock_disponible=-1))
        self.assertEqual(out["etapa"], "lote")

    def test_pedido_es_en_compra_y_usa_la_fecha_del_proveedor(self):
        eta = (datetime.utcnow() - timedelta(days=3)).date()
        out = self.f(self._base(estado="pedido", pedido_at=datetime.utcnow() - timedelta(days=20), compra_eta=eta))
        self.assertEqual(out["etapa"], "compra")
        self.assertEqual(out["plazo"]["color"], "rojo")
        self.assertIn("Atrasada", out["plazo"]["texto"])

    def test_con_ot_generada_el_plazo_lo_lleva_la_ot(self):
        out = self.f(self._base(estado="recibido", recibido_at=datetime.utcnow(), ot_generada_id=9, ot_generada_numero="OT-1"))
        self.assertEqual(out["etapa"], "ot")
        self.assertIsNone(out["plazo"])

    def test_cerradas(self):
        for est in ("instalado", "rechazado"):
            out = self.f(self._base(estado=est, abierta=False))
            self.assertEqual(out["etapa"], "cerrada")


if __name__ == "__main__":
    unittest.main()
