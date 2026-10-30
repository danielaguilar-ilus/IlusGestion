"""30-oct-2026 (Daniel): «en los hallazgos de Costanera no tenemos fotos… hazlo obligatorio» y «generamos una OT desde los hallazgos pero sigue
como si no lo hubiéramos gestionado».

1) Un hallazgo (Falla/Alerta del checklist o un resultado distinto de «Apto») sin NINGUNA foto en su tarea pone una razón en la firma del técnico
   (`_ot_hallazgos_sin_foto_razones`, dentro de `_ot_validar_diagnostico_y_fotos`). Una tarea «No aplica» o de un equipo excluido no se exige.
2) «Ya generé la OT: ligarla» (`POST /ot/api/<vid>/hallazgos/<hid>/ligar-ot`): la ruta existe con sus candados y el botón está en la tarjeta.
Se prueba con una base de mentira: sin clientes ni correos reales (REGLA #22). Correr con: python -m unittest tests.test_ot_30oct_hallazgos_foto_y_ligar
"""
import json
import os
import unittest

import ot_hallazgos as OH
from tests import _fuente_app as FA

RAIZ = FA.RAIZ


def _tarea(tid, titulo, tipo, valor, maquina=None, opciones=None):
    return {"id": tid, "titulo": titulo, "maquina_id": maquina, "tipo_respuesta": tipo, "valor_json": json.dumps(valor),
            "opciones_lista_json": json.dumps(opciones) if opciones is not None else None}


def _razones(filas, excluir=None):
    amb = FA.cargar(["_ot_hallazgos_sin_foto_razones"], extra={
        "mysql_fetchall": lambda *a, **k: filas, "_oh": OH, "_ot_tarea_no_trabajable_sql": lambda p: ""})
    return amb["_ot_hallazgos_sin_foto_razones"](7, excluir)


class TestHallazgoSinFoto(unittest.TestCase):
    def test_falla_y_alerta_sin_foto_piden_foto(self):
        r = _razones([_tarea(1, "Estado del cable", "verificacion", {"valor": "falla"}),
                      _tarea(2, "Poleas", "verificacion", {"valor": "alerta"})])
        self.assertEqual(len(r), 1)
        self.assertIn("2 hallazgo(s)", r[0])
        self.assertIn("Estado del cable", r[0])
        self.assertIn("Poleas", r[0])

    def test_aprobado_y_no_aplica_no_son_hallazgo(self):
        self.assertEqual(_razones([_tarea(1, "Cable", "verificacion", {"valor": "aprobado"}),
                                   _tarea(2, "Poleas", "verificacion", {"valor": "falla", "na": True})]), [])

    def test_un_equipo_excluido_del_cierre_no_se_exige(self):
        filas = [_tarea(1, "Cable", "verificacion", {"valor": "falla"}, maquina=9)]
        self.assertEqual(_razones(filas, excluir={9}), [])
        self.assertEqual(len(_razones(filas, excluir={10})), 1)

    def test_texto_y_numero_no_son_hallazgo(self):
        self.assertEqual(_razones([_tarea(1, "Medición", "numero", {"valor": 3})]), [])

    def test_una_lista_resultado_no_apto_pide_foto(self):
        ops = ["Apto", "Apto con observaciones", "Reparar", "Dar de baja"]
        r = _razones([_tarea(3, "Resultado del control", "lista", {"opcion": "Reparar"}, opciones=ops)])
        self.assertEqual(len(r), 1)

    def test_sin_hallazgos_no_hay_razones(self):
        self.assertEqual(_razones([]), [])

    def test_la_firma_del_tecnico_lo_usa_y_falla_abierta(self):
        src = FA.fuente_funcion("_ot_validar_diagnostico_y_fotos")
        self.assertIn("_ot_hallazgos_sin_foto_razones(vid, excluir_maquinas)", src)
        self.assertIn("except Exception as _e_hf", src, "fail-open: un problema de infraestructura no traba al técnico en terreno")


class TestLigarOt(unittest.TestCase):
    def test_la_ruta_existe_con_sus_candados(self):
        deco = " ".join(FA.decoradores("ot_api_hallazgo_ligar_ot"))
        self.assertIn("/ot/api/<int:vid>/hallazgos/<int:hid>/ligar-ot", deco)
        for d in ("_mant_required", "_no_tecnico", "_ot_can_view"):
            self.assertIn(d, deco)

    def test_valida_misma_ot_cliente_y_ot_caida(self):
        src = FA.fuente_funcion("ot_api_hallazgo_ligar_ot")
        for codigo in ("OT_REQUERIDA", "OT_NO_EXISTE", "OT_ES_LA_MISMA", "OT_CAIDA", "OT_OTRO_CLIENTE", "HALLAZGO_CAMBIO"):
            self.assertIn(codigo, src)
        self.assertIn("estado=%s", src, "el UPDATE es atómico: no pisa a quien lo cambió mientras tanto")
        self.assertIn("ot_madre_id IS NULL", src, "no le quita su madre a una OT que ya es hija de otra (desinstalación)")

    def test_el_boton_esta_en_la_tarjeta_y_el_js_lo_atiende(self):
        html = open(os.path.join(RAIZ, "templates", "ot2", "_hallazgos.html"), encoding="utf-8").read()
        js = open(os.path.join(RAIZ, "static", "ot_hallazgos.js"), encoding="utf-8").read()
        self.assertIn('data-hz-ligar="{{ i.id }}"', html)
        self.assertIn("Ya generé la OT: ligarla", html)
        self.assertIn("'/ligar-ot'", js)
        self.assertIn("[data-hz-ligar]", js)
        self.assertNotIn("alert(", js.replace("ilusAlert(", ""), "REGLA #1: nada de alert nativo")


class TestSinOrdenImpuesto(unittest.TestCase):
    """30-oct (Daniel): «no le impongamos un orden de ejecución a los técnicos; el técnico dijo que no le dejaba presionar siguiente»."""

    def test_siguiente_pendiente_nunca_se_apaga(self):
        with open(os.path.join(RAIZ, "templates", "ot2", "detalle.html"), encoding="utf-8") as fh:
            t = fh.read()
        self.assertNotIn("btn.disabled = (listos === items.length)", t)
        self.assertIn("if (btn) btn.disabled = false;", t)
        self.assertIn("Puedes empezar por el equipo que quieras", t)
        self.assertIn("window.otdWkSiguienteDesde(abierto0.dataset.mid)", t, "con todo listo sigue con el equipo de al lado")


if __name__ == "__main__":
    unittest.main()
