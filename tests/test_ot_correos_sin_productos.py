# -*- coding: utf-8 -*-
"""LOS CORREOS DE LA OT NO LISTAN PRODUCTOS (SOLO LA CANTIDAD) Y LA VISTA PREVIA MUESTRA EL PASO REAL (Daniel, 6-nov-2026).

Daniel: «excluye los productos de los correos, porque si son muchos se agrandarían y se pondrían feos; por último le adjuntamos el detalle».
Y con la captura de la prueba de «OT terminada»: «dice que sigue nuestro técnico llega en el horario indicado… pero si ya está terminada… es contradictorio».
(La vista previa usaba un recorrido de ejemplo FIJO, «2 · Recordatorio», para todos los correos; el correo real de OT terminada siempre llevó el recorrido completo.)

Qué se vigila:
  · `equipos_txt` dice «3 equipos» (nunca nombres) y `contar_equipos_txt` lo lee de vuelta; con 24 equipos el correo no crece.
  · El «Qué va a pasar» del correo de OT creada cuenta bien («24 equipos», no «1»), y la invitación de calendario también.
  · Ningún correo de la OT (cliente ni técnico) trae el nombre de un producto, con 1 o con 24 equipos.
  · La vista previa y «Enviarme una prueba» arman el recorrido con la función REAL y el paso de cada correo.
Todo con datos de mentira (REGLA #22). Correr:  python -m pytest tests/test_ot_correos_sin_productos.py -q -p no:warnings
"""
import re
import unittest

import comm_correos_registro as CCR
import ot_comunicacion as C
import ot_correos_tecnicos as T
import ot_recordatorio_24h as O
from tests import _fuente_codigo as FC
from tests.test_ot_recordatorio_24h import _t

_BARRA = "line-height:4px;font-size:0;background:"       # 6-nov (REGLA #67): barra de 4 px bajo los círculos numerados; el gris pendiente es #d1d5db
_NOMBRES = [f"Trotadora ILUS Modelo {i}" for i in range(24)]


class TestSoloLaCantidad(unittest.TestCase):
    def test_equipos_txt_dice_solo_cuantos(self):
        self.assertEqual(T.equipos_txt(["Trotadora"]), "1 equipo")
        self.assertEqual(T.equipos_txt(["A", "B", "C"]), "3 equipos")
        self.assertEqual(T.equipos_txt(_NOMBRES), "24 equipos")
        self.assertEqual(T.equipos_txt([]), "—")
        self.assertEqual(T.equipos_txt(None), "—")
        self.assertEqual(T.equipos_txt(["", "  ", None]), "—")                        # vacíos no cuentan
        self.assertEqual(T.equipos_txt(["A", "B"], tope=1), "2 equipos")              # `tope` se acepta por compatibilidad y no se usa

    def test_contar_equipos_lee_de_vuelta_lo_que_dice_el_correo(self):
        for n in (1, 2, 24, 300):
            self.assertEqual(T.contar_equipos_txt(T.equipos_txt(["x"] * n)), n)
        self.assertEqual(T.contar_equipos_txt("—"), 0)
        self.assertEqual(T.contar_equipos_txt(""), 0)
        self.assertEqual(T.contar_equipos_txt(None), 0)
        self.assertEqual(T.contar_equipos_txt("Trotadora, Bicicleta"), 2)             # compatibilidad con una lista de nombres separada por comas

    def test_las_variables_de_los_correos_no_llevan_nombres_de_productos(self):
        ot = {"id": 1, "numero_ot": "OT-2026-00077", "cliente": "Gimnasio Demo", "equipos": _NOMBRES, "tecnicos": ["Lenin"]}
        for v in (C.variables_ot(ot),
                  O.variables_recordatorio(dict(ot, hora_inicio=None, hora_fin=None, tipo="instalacion"), __import__("datetime").datetime(2026, 11, 10, 10, 0),
                                           __import__("datetime").date(2026, 11, 9), para="cliente"),
                  O.variables_recordatorio(dict(ot, hora_inicio=None, hora_fin=None, tipo="instalacion"), __import__("datetime").datetime(2026, 11, 10, 10, 0),
                                           __import__("datetime").date(2026, 11, 9), para="tecnico"),
                  T.construir_variables(ot)):
            blob = " ".join(str(x) for x in v.values())
            self.assertNotIn("Trotadora", blob)
            self.assertEqual(v["maquina"], "24 equipos")

    def test_con_24_equipos_los_bloques_no_crecen(self):
        qp = _t(O.que_esperar_html(_NOMBRES, ["Lenin"]))
        self.assertNotIn("Trotadora", qp)                                                      # (6-nov, auditoría: la caja ya no repite cuántos equipos; lo dice la tabla)
        self.assertNotIn("24", qp)
        self.assertEqual(O.equipos_lista_html(_NOMBRES), "")
        rt = _t(C.resumen_trabajo_html({"equipos": _NOMBRES, "tecnicos": ["Lenin"]}))
        self.assertIn("Documento firmado", rt)                                                 # (6-nov, auditoría: ni nombres ni cantidad; ya están en la tabla del correo)
        self.assertNotIn("Trotadora", rt)
        self.assertNotIn("24", rt)
        self.assertLess(len(O.que_esperar_html(_NOMBRES, ["Lenin"])), len(O.que_esperar_html(["A"], ["Lenin"])) + 40)     # no crece con la cantidad

    def test_el_resumen_y_la_invitacion_cuentan_bien_el_texto_de_la_cantidad(self):
        """`maquina` ya trae «24 equipos» (no la lista): el correo de OT creada y su invitación deben contar 24, no 1."""
        app = FC.texto("app.py")
        self.assertIn('equipos = ["equipo"] * min(_otc.contar_equipos_txt(eq_txt), 500)', app)
        self.assertIn("_n_eq = _otc.contar_equipos_txt(eq)", app)
        amb = {"_ot_com_promesas": lambda: {"recordatorio": True, "documento": True}, "_orec": O, "_ocom": C, "_otc": T}
        FC.extraer(amb, "app.py", ("_ot_com_bloques_creacion",), ())
        b = amb["_ot_com_bloques_creacion"]({"tipo": "instalacion"}, {"tecnico": "Lenin Urbina", "maquina": "24 equipos"}, con_invitacion=True)
        self.assertNotIn("Trotadora", _t(b["que_esperar_html"]))
        self.assertIn("confirmar la recepción del trabajo con tu firma", _t(b["que_esperar_html"]))
        b1 = amb["_ot_com_bloques_creacion"]({"tipo": "instalacion"}, {"tecnico": "Lenin Urbina", "maquina": "1 equipo"}, con_invitacion=False)
        self.assertEqual(b1["que_esperar_html"], b["que_esperar_html"])                       # igual con 1 o con 24 equipos
        b0 = amb["_ot_com_bloques_creacion"]({"tipo": "instalacion"}, {"tecnico": "Lenin Urbina", "maquina": "—"}, con_invitacion=False)
        self.assertEqual(b0["que_esperar_html"], b["que_esperar_html"])                       # y sin equipos

    def test_la_invitacion_de_calendario_dice_la_cantidad_y_no_los_nombres(self):
        import datetime as dt
        ot = {"id": 5, "numero_ot": "OT-2026-00005", "cliente": "Demo", "tipo_label": "Instalación", "fecha": dt.date(2026, 11, 10),
              "hora_inicio": dt.timedelta(hours=10), "hora_fin": dt.timedelta(hours=12), "equipos": ["equipo"] * 24, "tecnicos": ["Lenin"]}
        ics = T.ics_evento(ot, "asignada", correo="a@b.cl", nombre="", ts=0, base_url="", organizador="soportetec@sphs.cl", rsvp=True, sin_link=True)
        txt = ics.decode("utf-8", "replace") if isinstance(ics, (bytes, bytearray)) else str(ics)
        txt = txt.replace("\r\n ", "")                                                  # líneas plegadas
        self.assertIn("Equipos: 24 equipos.", txt)

    def test_ningun_correo_armado_de_punta_a_punta_trae_un_producto(self):
        from tests.test_ot_recordatorio_24h import MundoRec, LUN
        w = MundoRec()
        self.addCleanup(w.cerrar)
        w.amb["_ot_com_promesas"] = lambda: {"recordatorio": True, "documento": True}
        w.barrer(LUN(10, 5))
        for destino in ("contacto@gimnasio.cl", "lenin@sphs.cl"):
            h = w.para(destino)[0]["html"]
            self.assertNotIn("Trotadora ILUS Pro 9000", h, destino)
            self.assertIn("1 equipo", _t(h), destino)


class TestElRecorridoDeLaVistaPrevia(unittest.TestCase):
    """Daniel (6-nov): la prueba de «OT terminada» mostraba «2 · Recordatorio» y «Qué sigue: el técnico llega…». Ahora cada correo muestra SU paso."""

    def _demo(self, clave, cuerpo="{{recorrido_html}}"):
        amb = {"mysql_fetchone": lambda *a, **k: {"asunto": "x", "cuerpo": cuerpo, "activo": 1},
               "_comm_demo_replacements": lambda: {}, "_ccr": CCR, "_ocom": C, "_orec": O, "re": re, "print": lambda *a, **k: None,
               "_public_base_url": lambda: "https://ilus.test"}
        FC.extraer(amb, "app.py", ("_comm_correo_demo",), ())
        _a, cuerpo, _i = amb["_comm_correo_demo"]({"clave": clave, "plantilla": ("mantenciones", "x"), "nombre": clave, "demo": {}})
        return cuerpo

    def _barras(self, h):
        return (h.count(_BARRA + "#16a34a"), h.count(_BARRA + "#dc2626"), h.count(_BARRA + "#d1d5db"))

    def test_la_ot_terminada_muestra_los_cuatro_pasos_hechos_y_nada_que_sigue(self):
        for clave in ("ot_terminada_cliente", "ot_envio_manual"):
            h = self._demo(clave)
            self.assertEqual(self._barras(h), (4, 0, 0), clave)
            self.assertNotIn("Qué sigue", _t(h), clave)
            self.assertNotIn("técnico llega", _t(h), clave)

    def test_cada_correo_muestra_su_paso(self):
        self.assertEqual(self._barras(self._demo("ot_creada_cliente")), (0, 1, 3))
        self.assertIn("te enviaremos un recordatorio", _t(self._demo("ot_creada_cliente")))
        self.assertEqual(self._barras(self._demo("ot_recordatorio_cliente")), (1, 1, 2))
        self.assertEqual(self._barras(self._demo("ot_firma_remota")), (2, 1, 1))

    def test_lo_que_se_ve_es_lo_que_manda_el_correo_real(self):
        # la vista previa lleva el botón «Ver seguimiento en vivo» a un enlace de EJEMPLO (nunca a una OT real); el resto es idéntico al correo real
        ejemplo = "https://ilus.test/ot-seguimiento/ejemplo"
        self.assertEqual(self._demo("ot_terminada_cliente"), C.recorrido_html("documento", "", url_seguimiento=ejemplo))
        self.assertEqual(self._demo("ot_recordatorio_cliente"), C.recorrido_html("recordatorio", "", url_seguimiento=ejemplo))        # sin «Qué sigue» (6-nov, auditoría)
        # y el correo real de «OT terminada» sigue pasando el paso «documento» (cerrado), con el enlace de seguimiento de ESA OT
        self.assertIn('recorrido=_ocom.recorrido_html("documento", url_seguimiento=_ot_seg_url(v_id))', FC.texto("app.py"))

    def test_los_bloques_de_valor_de_la_vista_previa_salen_de_las_funciones_reales(self):
        """Antes eran textos de ejemplo fijos: el 6-nov la vista previa seguía mostrando «Llegará…» y «Qué sigue» cuando el correo real ya los había quitado."""
        h = self._demo("ot_recordatorio_cliente", "{{que_esperar_html}}|{{preparacion_html}}|{{resumen_trabajo_html}}")
        qe, prep, res = h.split("|")
        self.assertEqual(qe, O.que_esperar_html([], []))
        self.assertEqual(prep, O.preparacion_html("mantencion"))
        self.assertEqual(res, C.resumen_trabajo_html({}))
        for viejo in ("Llegará", "Trabajará en", "Se atendieron", "Lo realizó"):
            self.assertNotIn(viejo, _t(h), viejo)
        self.assertNotIn("Qué sigue", _t(self._demo("ot_recordatorio_cliente")))

    def test_la_demo_de_equipos_del_registro_ya_es_solo_la_cantidad(self):
        self.assertEqual(CCR.DEMO_EXTRA["maquina"], "2 equipos")
        self.assertNotIn("Trotadora", CCR.DEMO_EXTRA["resumen_trabajo_html"])


if __name__ == "__main__":
    unittest.main()
