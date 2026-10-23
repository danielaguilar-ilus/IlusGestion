# -*- coding: utf-8 -*-
"""2026-10-23 · OT 353 (D Vitacura), Daniel: «mientras no haya firmado el anexo se pueden eliminar los productos, con registro de
actividad; si firmo el anexo esa edición queda bloqueada». Dos caminos:
  · quitar un PRODUCTO (equipo + checklist) de la OT: POST /ot/api/<vid>/equipos/<mid>/quitar
  · quitar productos DEL ANEXO (PATCH del anexo, que ya se bloquea al firmar): ahora queda anotado quién y cuáles.
"""
import os
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(*p):
    with open(os.path.join(RAIZ, *p), encoding="utf-8") as fh:
        return fh.read()


APP = _leer("app.py")
DET = _leer("templates", "ot2", "detalle.html")
PARCIAL = _leer("templates", "ot2", "_quitar_equipo.html")


def _ruta():
    i = APP.index('@app.route("/ot/api/<int:vid>/equipos/<int:mid>/quitar", methods=["POST"])')
    fin = APP.index('@app.route("/ot/api/<int:vid>/equipos-masivo"', i)
    return APP[i:fin]


class TestRutaQuitar(unittest.TestCase):
    def setUp(self):
        self.f = _ruta()

    def test_solo_gestion_y_ot_abierta(self):
        self.assertIn("@_mant_required\n@_ot_can_configurar", self.f)
        self.assertIn("if _es_rol_tecnico():", self.f)
        self.assertIn('("cerrada", "completada", "cancelada", "anulada")', self.f)

    def test_anexo_firmado_bloquea_antes_de_borrar(self):
        self.assertLess(self.f.index("_ot_anexo_firmado_bloqueo(vid)"), self.f.index("DELETE FROM mant_visita_tareas"))
        self.assertIn('"ANEXO_FIRMADO", http=409', self.f)

    def test_una_ot_es_evidencia_no_borra_trabajo_hecho(self):
        self.assertIn('"CON_ACTIVIDAD", http=409', self.f)
        for pista in ("completada=1", "estado_trabajo", "revisado_at IS NOT NULL", "mant_ot_repuesto_solicitudes"):
            self.assertIn(pista, self.f)

    def test_queda_en_la_bitacora_antes_de_borrar(self):
        self.assertLess(self.f.index('"equipo_quitado"'), self.f.index("DELETE FROM mant_visita_tareas"))

    def test_el_bloqueo_mira_solo_anexos_firmados(self):
        i = APP.index("def _ot_anexo_firmado_bloqueo(vid):")
        h = APP[i:i + 1200]
        self.assertIn("estado='firmado'", h)
        self.assertIn("quedan bloqueados", h)


class TestAnexoQuedaRegistro(unittest.TestCase):
    def test_productos_quitados_del_anexo_se_anotan(self):
        i = APP.index("def ot2_api_anexo_editar(aid):")
        f = APP[i:i + 12000]
        self.assertIn('"productos_quitados"', f)
        self.assertIn('"anexo_productos_quitados"', f)
        self.assertIn("(aún sin firmar)", f)
        self.assertIn("if a[\"estado\"] not in _ANEXO_EDITABLES:", f, "el anexo firmado sigue sin poder editarse")


class TestFront(unittest.TestCase):
    def test_el_contexto_trae_el_candado(self):
        self.assertIn("eq_quitar=eq_quitar,", APP)
        self.assertIn('"bloqueo": (_ot_anexo_firmado_bloqueo(vid) or "")', APP)

    def test_el_boton_esta_en_las_dos_tarjetas_y_usa_los_modales_ilus(self):
        self.assertEqual(DET.count("{% include 'ot2/_quitar_equipo.html' %}"), 2)
        i = DET.index("async function otdEquipoQuitar(mid, btn)")
        f = DET[i:i + 1800]
        self.assertIn("ilusConfirm", f)
        self.assertIn("/quitar`", f)
        self.assertNotIn("confirm(", f.replace("ilusConfirm(", ""))

    def test_el_parcial_muestra_el_motivo_si_el_anexo_esta_firmado(self):
        from jinja2 import Environment
        t = Environment(autoescape=True).from_string(PARCIAL)      # como Flask: las plantillas .html escapan
        e = {"id": 7, "nombre": "Trotadora <X>", "_interno": False}
        libre = t.render(eq_quitar={"puede": True, "bloqueo": ""}, e=e)
        self.assertIn("otdEquipoQuitar(7, this)", libre)
        self.assertNotIn("<X>", libre.replace("&lt;X&gt;", ""))     # el nombre va escapado
        firmado = t.render(eq_quitar={"puede": True, "bloqueo": "El Anexo N° 9 ya está firmado"}, e=e)
        self.assertNotIn("otdEquipoQuitar", firmado)
        self.assertIn("ya está firmado", firmado)
        self.assertEqual(t.render(eq_quitar={"puede": False, "bloqueo": ""}, e=e).strip(), "")
        self.assertEqual(t.render(eq_quitar={"puede": True, "bloqueo": ""}, e={"id": 7, "_interno": True}).strip(), "")


if __name__ == "__main__":
    unittest.main()
