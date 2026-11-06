# -*- coding: utf-8 -*-
"""🚚 FLOTA · La lista de solicitudes se mantiene al día (Daniel, 06-nov-2026):
«Alex la ve bien en el calendario pero en la lista no ve la solicitud 9, y debe verla para entregar las llaves».

Causa: la lista se pedía UNA sola vez, al abrir la página. El calendario sí se vuelve a pedir al abrirlo y cada 2 minutos. Quien dejaba /flota
abierta veía las reservas nuevas en el calendario y no en la lista (el servidor y los permisos estaban bien: la lista no filtra por persona).

Decisiones que se vigilan:
  1. La lista se refresca al volver a ella, cada minuto mientras se mira y al volver a la pantalla tras un rato (celular desbloqueado, pestaña al frente).
  2. Nunca con un formulario o detalle abierto, ni con la pantalla oculta, ni en otra pestaña o vista, ni si ya hay una carga en camino o acaba de cargar.
  3. Si la carga automática falla no avisa ni vacía la lista; y una respuesta vieja nunca pisa a una más nueva.
"""
import json
import os
import re
import subprocess
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from test_flota_solicitudes import _leer  # noqa: E402

JS = _leer("static", "flota_uso.js")


def _trozo(desde, hasta):
    i = JS.index(desde)
    j = JS.index(hasta, i)
    return JS[i:j]


class TestComportamiento(unittest.TestCase):
    """Ejecuta las funciones reales con Node y una pantalla de mentira."""

    def correr(self, guion):
        fns = _trozo("function refrescarListaViva(", "function recargarSol(")
        base = r"""
var S = { tab: 'solicitudes', vista: 'lista', cargandoSol: 0, ultimaCargaSol: 0, latidos: 0 };
var llamadas = { sol: 0, cal: 0, prox: 0, silencio: [], calForzar: [] };
var modal = false;
var document = { hidden: false, querySelector: function (s) { return s === '.modal.show' && modal ? {} : null; } };
function cargarSol(silencio) { llamadas.sol++; llamadas.silencio.push(silencio); }
function cargarCal(f) { llamadas.cal++; llamadas.calForzar.push(!!f); }
function pintarProximo() { llamadas.prox++; }
""" + fns + guion
        r = subprocess.run(["node", "-e", base], capture_output=True, text=True, timeout=60)
        self.assertEqual(r.returncode, 0, r.stderr)
        return json.loads(r.stdout.strip())

    def test_se_refresca_la_lista_cuando_corresponde_y_de_forma_silenciosa(self):
        out = self.correr("var r = refrescarListaViva(0); console.log(JSON.stringify([r, llamadas.sol, llamadas.silencio]));")
        self.assertEqual(out, [True, 1, [True]])

    def test_no_se_refresca_en_los_casos_que_estorban(self):
        casos = {
            "oculta": "document.hidden = true;",
            "otra pestaña": "S.tab = 'vehiculos';",
            "calendario": "S.vista = 'calendario';",
            "formulario o detalle abierto": "modal = true;",
            "carga en camino": "S.cargandoSol = Date.now() - 5000;",
            "acaba de cargar": "S.ultimaCargaSol = Date.now() - 3000;",
        }
        for nombre, prep in casos.items():
            out = self.correr(prep + "var r = refrescarListaViva(20); console.log(JSON.stringify([r, llamadas.sol]));")
            self.assertEqual(out, [False, 0], nombre)

    def test_una_carga_trabada_hace_mas_de_15_segundos_no_bloquea_para_siempre(self):
        out = self.correr("S.cargandoSol = Date.now() - 20000; var r = refrescarListaViva(0); console.log(JSON.stringify([r, llamadas.sol]));")
        self.assertEqual(out, [True, 1])

    def test_ya_pasaron_mas_de_20_segundos_desde_la_ultima_carga_si_se_refresca(self):
        out = self.correr("S.ultimaCargaSol = Date.now() - 30000; var r = refrescarListaViva(20); console.log(JSON.stringify([r, llamadas.sol]));")
        self.assertEqual(out, [True, 1])

    def test_el_latido_en_la_lista_refresca_cada_minuto(self):
        out = self.correr("for (var i = 0; i < 4; i++) latidoFlota(); console.log(JSON.stringify([llamadas.sol, llamadas.cal, llamadas.prox]));")
        self.assertEqual(out, [2, 0, 0])                         # 4 latidos de 30 s = 2 minutos = 2 cargas

    def test_el_latido_en_el_calendario_sigue_como_antes(self):
        out = self.correr("S.vista = 'calendario'; for (var i = 0; i < 4; i++) latidoFlota(); console.log(JSON.stringify([llamadas.sol, llamadas.cal, llamadas.prox, llamadas.calForzar]));")
        self.assertEqual(out, [0, 1, 3, [True]])                 # la agenda cada 2 min (forzada) y la cuenta regresiva en los otros 3

    def test_el_latido_no_hace_nada_con_la_pantalla_oculta_o_en_otra_pestana(self):
        for prep in ("document.hidden = true;", "S.tab = 'choferes';", "document.hidden = true; S.vista = 'calendario';",
                     "S.tab = 'vehiculos'; S.vista = 'calendario';"):
            out = self.correr(prep + "for (var i = 0; i < 6; i++) latidoFlota(); console.log(JSON.stringify([llamadas.sol, llamadas.cal, llamadas.prox]));")
            self.assertEqual(out, [0, 0, 0], prep)

    def test_al_volver_a_la_pantalla_refresca_lo_que_se_estaba_mirando(self):
        out = self.correr("alVolverALaPantalla(); console.log(JSON.stringify([llamadas.sol, llamadas.cal]));")
        self.assertEqual(out, [1, 0])
        out = self.correr("S.vista = 'calendario'; alVolverALaPantalla(); console.log(JSON.stringify([llamadas.sol, llamadas.cal, llamadas.calForzar]));")
        self.assertEqual(out, [0, 1, [True]])
        out = self.correr("document.hidden = true; alVolverALaPantalla(); console.log(JSON.stringify([llamadas.sol, llamadas.cal]));")
        self.assertEqual(out, [0, 0])


    def test_la_visibilidad_anota_cuando_se_oculta_y_actualiza_si_pasaron_mas_de_30_segundos(self):
        out = self.correr("document.hidden = true; alCambiarVisibilidad(); var anotado = S.ocultoDesde > 0; "
                          "document.hidden = false; S.ocultoDesde = Date.now() - 40000; alCambiarVisibilidad(); "
                          "console.log(JSON.stringify([anotado, llamadas.sol]));")
        self.assertEqual(out, [True, 1])
        out = self.correr("document.hidden = false; S.ocultoDesde = Date.now() - 5000; alCambiarVisibilidad(); console.log(JSON.stringify([llamadas.sol, llamadas.cal]));")
        self.assertEqual(out, [0, 0])                            # volvió enseguida: no hace falta
        out = self.correr("S.vista = 'calendario'; document.hidden = false; S.ocultoDesde = Date.now() - 60000; alCambiarVisibilidad(); "
                          "console.log(JSON.stringify([llamadas.sol, llamadas.cal]));")
        self.assertEqual(out, [0, 1])


class TestCableado(unittest.TestCase):
    def test_volver_a_la_lista_o_a_la_pestana_la_refresca(self):
        self.assertIn("if (v === 'calendario') cargarCal(true); else refrescarListaViva(5);", JS)
        self.assertIn("else if (t === 'solicitudes') refrescarListaViva(5);", JS)

    def test_el_latido_y_la_vuelta_a_la_pantalla_estan_conectados(self):
        self.assertIn("setInterval(latidoFlota, 30000);", JS)
        self.assertIn("document.addEventListener('visibilitychange', alCambiarVisibilidad);", JS)
        self.assertIn("window.addEventListener('pageshow', function (e) { if (e.persisted) alVolverALaPantalla(); });", JS)

    def test_la_carga_automatica_es_silenciosa_y_las_respuestas_viejas_no_pisan(self):
        i = JS.index("function cargarSol(silencio)")
        t = JS[i:i + 900]
        self.assertIn("if (mia !== S.solSeq) return;", t)
        self.assertIn("if (!d.ok && silencio) return;", t)
        self.assertLess(t.index("if (!d.ok && silencio) return;"), t.index("toast("))
        self.assertIn("cargarSol(true);", _trozo("function refrescarListaViva(", "function latidoFlota("))

    def test_las_acciones_siguen_recargando_con_aviso_normal(self):
        # recargarSol (después de confirmar, entregar la llave, cancelar…) NO es silenciosa: si falla, se avisa
        self.assertIn("var p = [cargarSol()];", JS)

    def test_el_servidor_no_filtra_la_lista_por_persona(self):
        mod = _leer("flota_module.py")
        i = mod.index("def flota_api_solicitudes():")
        t = mod[i:i + 1400]
        self.assertNotIn("solicitante_id", t)
        self.assertNotIn("_uid()", t)


if __name__ == "__main__":
    unittest.main()
