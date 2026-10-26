# -*- coding: utf-8 -*-
"""🧩 Un SOLO «Generar OT» (Daniel, 2026-10-26): «el botón de generar OT de la ficha de clientes no está tan actualizado; que sea uno solo, de
manera que si hacemos un cambio en el modal de gestionar OT se aplique a todos».

La ficha del cliente (templates/mantenciones/ficha.html) abría su propia copia del formulario (#modalGenerarOT, tickets_ficha.js en «modo
cliente»). Ahora TODOS sus botones —«Generar OT», «Nueva visita», «Programar mantención», la agenda del plan anual y «Generar OT con los
seleccionados»— abren el asistente «Nueva OT» (OT2C, templates/ot2/_modal_crear.html) con el cliente ya puesto. Datos de mentira."""
import json
import os
import re
import shutil
import subprocess
import tempfile
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(*partes):
    with open(os.path.join(RAIZ, *partes), encoding="utf-8", newline="") as f:
        return f.read().replace("\r\n", "\n")


FICHA = _leer("templates", "mantenciones", "ficha.html")
MANT = _leer("static", "mant_ficha.js")
MULTI = _leer("static", "mantenciones_ficha.js")


def _js_funcion(texto, nombre):
    m = re.search(r"(?:async\s+)?function %s\(" % re.escape(nombre), texto)
    i = m.start()
    prof, j = 0, texto.index("{", i)
    while True:
        if texto[j] == "{":
            prof += 1
        elif texto[j] == "}":
            prof -= 1
            if prof == 0:
                return texto[i:j + 1]
        j += 1


def _node(programa):
    f = os.path.join(tempfile.mkdtemp(), "prog.js")
    with open(f, "w", encoding="utf-8") as fh:
        fh.write(programa)
    r = subprocess.run(["node", f], capture_output=True, text=True, encoding="utf-8", timeout=60)
    assert r.returncode == 0, r.stderr
    return json.loads(r.stdout)


class TestLaFichaIncluyeElAsistenteUnico(unittest.TestCase):
    def test_trae_el_mismo_cargador_del_asistente_que_el_panel_de_ot_los_tickets_y_los_repuestos(self):
        # desde el 23-oct el asistente (~765 KB) no viaja dentro de las páginas: un cargador de ~3 KB lo descarga bajo demanda de /ot/modal-crear
        self.assertIn("ot_modal_lazy.js", FICHA)
        self.assertNotIn('{% include "ot2/_modal_crear.html" %}', FICHA)
        for rel in (("templates", "tickets", "ficha.html"), ("templates", "ot2", "panel.html"), ("templates", "clientes_hub", "repuestos.html")):
            self.assertIn("ot_modal_lazy.js", _leer(*rel), rel)

    def test_el_cargador_va_despues_del_modal_viejo_y_antes_de_cerrar_el_bloque(self):
        self.assertLess(FICHA.index('{% include "tickets/_modal_generar_ot.html" %}'), FICHA.index("ot_modal_lazy.js') }}"))
        self.assertLess(FICHA.index("ot_modal_lazy.js') }}"), FICHA.rindex("{% endblock %}"))

    def test_el_modal_viejo_sigue_en_la_pagina_porque_tickets_ficha_js_lo_necesita(self):
        # REGLA #4.2: nada se borra; ningún botón lo abre
        self.assertIn('{% include "tickets/_modal_generar_ot.html" %}', FICHA)


    def test_la_plantilla_sigue_siendo_jinja_valido(self):
        from jinja2 import Environment
        Environment().parse(FICHA)

    def test_el_asistente_se_dibuja_con_los_permisos_de_la_ficha(self):
        from jinja2 import Environment, FileSystemLoader
        env = Environment(loader=FileSystemLoader(os.path.join(RAIZ, "templates")))
        env.globals["url_for"] = lambda endpoint, **kw: "/static/" + kw.get("filename", "")
        html = env.get_template("ot2/_modal_crear.html").render(permissions={"superadmin": False}, es_tecnico=False)
        self.assertIn('id="ot2ModalCrear"', html)
        self.assertIn("window.OT2C", html)


@unittest.skipUnless(shutil.which("node"), "sin Node")
class TestTodosLosBotonesAbrenElMismoFormulario(unittest.TestCase):
    def _abrir(self, args, ot2c=True, cid=26, razon="Gimnasio Demo SpA", rut="76.111.222-3"):
        prog = "\n".join([
            "var CID = %s, DATA = {cliente_razon_social: %s, cliente_rut: %s};" % (cid, json.dumps(razon), json.dumps(rut)),
            "var llamadas = [], toasts = [], recargas = [], cierres = 0;",
            "var ilusToast = function(m, o){ toasts.push([m, (o||{}).type]); };",
            "var location = {reload: function(){ recargas.push(1); }};",
            "var _timers = []; var setTimeout = function(f, ms){ _timers.push([f, ms]); };",
            ("var window = {OT2C: {abrir: function(o){ llamadas.push(o); }, cerrar: function(){ cierres++; }}};\nvar OT2C = window.OT2C;"
             if ot2c else "var window = {};"),
            _js_funcion(MANT, "abrirLevantamientoSelector"),
            "(async function(){ await abrirLevantamientoSelector(%s); "
            "var o = llamadas[0]; var r = {n: llamadas.length, toasts: toasts};"
            " if (o){ r.origen = o.origen; r.cliente = o.cliente; r.tipo = o.tipo; r.fecha = o.fecha; r.ids = o.equipo_preset_maquina_ids;"
            "          o.onCreada(); r.cierres = cierres; r.timers = _timers.length; _timers.forEach(function(t){ t[0](); }); r.recargas = recargas.length; }"
            " console.log(JSON.stringify(r)); })();" % args])
        return _node(prog)

    def test_generar_ot_abre_el_asistente_con_el_cliente_de_la_ficha(self):
        r = self._abrir("undefined")
        self.assertEqual(r["n"], 1)
        self.assertEqual(r["origen"], "cliente")
        self.assertEqual(r["cliente"], {"id": 26, "nombre": "Gimnasio Demo SpA", "rut": "76.111.222-3"})
        self.assertIsNone(r.get("tipo"))                    # sin preset: el asistente pregunta el tipo
        self.assertIsNone(r.get("fecha"))
        self.assertIsNone(r.get("ids"))

    def test_programar_mantencion_y_nueva_visita_llegan_con_el_tipo_preventiva(self):
        self.assertEqual(self._abrir("'preventiva'")["tipo"], "preventiva")

    def test_la_agenda_del_plan_anual_lleva_su_fecha_y_una_fecha_rara_se_ignora(self):
        self.assertEqual(self._abrir("'preventiva', '2026-11-12'")["fecha"], "2026-11-12")
        self.assertIsNone(self._abrir("'preventiva', '12/11/2026'").get("fecha"))
        self.assertIsNone(self._abrir("'preventiva', '2026-11-12; DROP'").get("fecha"))

    def test_los_equipos_marcados_viajan_como_presets_y_lo_raro_se_descarta(self):
        self.assertEqual(self._abrir("null, null, ['7', 9, 'x', 0, -3]")["ids"], [7, 9])
        self.assertIsNone(self._abrir("null, null, []").get("ids"))

    def test_al_crearse_la_persona_sigue_en_la_ficha_y_se_refresca(self):
        r = self._abrir("undefined")
        self.assertEqual((r["cierres"], r["timers"], r["recargas"]), (1, 1, 1))

    def test_ya_no_se_exige_tener_equipos_registrados(self):
        # la copia vieja frenaba con «Sin equipos»; el asistente sabe crear una instalación o un levantamiento sin ellos
        f = _js_funcion(MANT, "abrirLevantamientoSelector")
        self.assertNotIn("Sin equipos", f)
        self.assertNotIn("data-maquina-id", f)

    def test_si_el_asistente_no_cargo_se_dice_y_no_se_rompe(self):
        r = self._abrir("undefined", ot2c=False)
        self.assertEqual(r["n"], 0)
        self.assertEqual(r["toasts"][0][1], "error")
        self.assertIn("recarga la página", r["toasts"][0][0])

    def test_no_queda_ningun_camino_al_formulario_viejo_desde_la_ficha(self):
        f = _js_funcion(MANT, "abrirLevantamientoSelector")
        codigo = "\n".join(l for l in f.split("\n") if not l.strip().startswith("//"))      # los comentarios pueden nombrarlo
        for viejo in ("modalGenerarOT", "bootstrap.Modal", "_TKOT"):
            self.assertNotIn(viejo, codigo, viejo)
        self.assertIn("OT2C.abrir(opts)", codigo)


@unittest.skipUnless(shutil.which("node"), "sin Node")
class TestLaBarraDeSeleccionTambienLoUsa(unittest.TestCase):
    def _multi(self, marcados):
        chks = [{"id": i} for i in marcados]
        prog = "\n".join([
            "var toasts = [], abiertas = [];",
            "var ilusToast = function(m, o){ toasts.push([m, (o||{}).type]); };",
            "var document = {querySelectorAll: function(sel){ return %s.map(function(c){ return {dataset: c}; }); }};" % json.dumps(chks),
            "var abrirLevantamientoSelector = function(){ abiertas.push([].slice.call(arguments)); };",
            _js_funcion(MULTI, "abrirVisitaMulti"),
            "abrirVisitaMulti(); console.log(JSON.stringify({toasts: toasts, abiertas: abiertas}));"])
        return _node(prog)

    def test_con_equipos_marcados_abre_el_asistente_con_esos_equipos(self):
        r = self._multi(["4", "9", "11"])
        self.assertEqual(r["abiertas"], [[None, None, [4, 9, 11]]])
        self.assertEqual(r["toasts"], [])

    def test_sin_equipos_marcados_pide_marcar_al_menos_uno(self):
        r = self._multi([])
        self.assertEqual(r["abiertas"], [])
        self.assertEqual(r["toasts"][0][0], "Selecciona al menos un equipo")

    def test_el_formulario_propio_de_la_barra_queda_intacto_pero_sin_botones(self):
        self.assertIn("function abrirVisitaMultiAntiguo()", MULTI)
        self.assertIn("vm_lista", _js_funcion(MULTI, "abrirVisitaMultiAntiguo"))
        self.assertEqual(len(re.findall(r"onclick=\"abrirVisitaMultiAntiguo\(", FICHA)), 0)


class TestTodosLosBotonesDeLaFichaPasanPorUnSoloPunto(unittest.TestCase):
    def test_los_botones_de_generar_ot_llaman_a_abrirGenerarOT(self):
        self.assertEqual(len(re.findall(r'onclick="abrirGenerarOT\(\)"', FICHA)), 2)       # acciones rápidas y pestaña Equipos
        self.assertIn('onclick="abrirVisitaMulti()"', FICHA)                               # la barra de selección

    def test_nueva_visita_y_programar_mantencion_pasan_por_el_mismo_punto(self):
        self.assertIn("abrirGenerarOT(tipoPreset || 'preventiva');", _js_funcion(MANT, "abrirNuevaVisita"))
        self.assertIn("abrirLevantamientoSelector(tipoPreset);", _js_funcion(MANT, "abrirGenerarOT"))

    def test_la_agenda_del_plan_anual_pasa_por_el_mismo_punto(self):
        self.assertIn("onclick=\"abrirLevantamientoSelector('preventiva','${fechaParam}')\"", MANT)

    def test_abrir_con_la_url_tambien_usa_el_asistente_unico(self):
        self.assertIn("abrir_generar_ot", MANT)
        i = MANT.index("qs.get('abrir_generar_ot') === '1'")
        self.assertIn("abrirGenerarOT();", MANT[i:i + 200])


if __name__ == "__main__":
    unittest.main()
