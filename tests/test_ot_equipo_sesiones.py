# -*- coding: utf-8 -*-
"""Sesiones de trabajo por equipo en la OT (Daniel, 2026-11-04):
«si el técnico selecciona otro equipo debe contraer el equipo seleccionado y pausar sus tiempos… reconociendo si hay otro técnico
trabajando un equipo». Cubre: las reglas puras (ot_equipo_sesiones.py), el cableado del servidor, la plantilla y el JS ejecutado con
Node sobre un DOM de mentira (acordeón, pausa, latido, aviso de otro técnico y que la gestión no registre nada)."""
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import ot_equipo_sesiones as oes  # noqa: E402
from tests._fuente_app import RAIZ, decoradores, fuente_funcion, texto  # noqa: E402

HAY_NODE = shutil.which("node") is not None
T0 = datetime(2026, 11, 4, 10, 0, 0)


def _ses(inicio_min=0, fin_min=None, latido_min=None, clave="e1", user=1, nombre="Ana"):
    return {"clave": clave, "user_id": user, "user_nombre": nombre,
            "inicio": T0 + timedelta(minutes=inicio_min),
            "fin": None if fin_min is None else T0 + timedelta(minutes=fin_min),
            "ultimo_latido": T0 + timedelta(minutes=inicio_min if latido_min is None else latido_min)}


def _leer(rel):
    with open(os.path.join(RAIZ, rel.replace("/", os.sep)), encoding="utf-8", newline="") as fh:
        return fh.read().replace("\r\n", "\n")


# ══════════════════════════════════ A) reglas puras ══════════════════════════════════
class TestClaves(unittest.TestCase):
    def test_claves_validas_e_invalidas(self):
        for ok in ("e1", "e12", "i3", "i999999999"):
            self.assertTrue(oes.clave_valida(ok), ok)
        for mal in ("", "1", "x1", "e", "e-1", "e1;DROP", "e1234567890", "E1", None, 5, "e 1"):
            self.assertFalse(oes.clave_valida(mal), repr(mal))

    def test_clave_de(self):
        self.assertEqual(oes.clave_de(12), "e12")
        self.assertEqual(oes.clave_de("7", interno=True), "i7")


class TestTiempo(unittest.TestCase):
    def test_sesion_cerrada_cuenta_de_inicio_a_fin(self):
        s = _ses(0, fin_min=10, latido_min=10)
        self.assertEqual(oes.segundos(s, T0 + timedelta(hours=5)), 600)

    def test_sesion_abierta_y_viva_cuenta_hasta_ahora(self):
        s = _ses(0, latido_min=4)
        self.assertEqual(oes.segundos(s, T0 + timedelta(minutes=5)), 300)

    def test_telefono_dormido_no_cuenta_la_pausa(self):
        """Último latido a los 5 min y nadie volvió a tocar: a las 2 horas SOLO cuentan 5 min + la gracia, no 2 horas."""
        s = _ses(0, latido_min=5)
        self.assertEqual(oes.segundos(s, T0 + timedelta(hours=2)), 5 * 60 + oes.GRACIA_SEG)

    def test_un_cierre_tardio_tampoco_cuenta_el_hueco(self):
        s = _ses(0, fin_min=120, latido_min=5)
        self.assertEqual(oes.segundos(s, T0 + timedelta(hours=3)), 5 * 60 + oes.GRACIA_SEG)

    def test_nunca_negativo_ni_falla_con_datos_vacios(self):
        self.assertEqual(oes.segundos({}, T0), 0)
        self.assertEqual(oes.segundos({"inicio": T0, "fin": T0 - timedelta(minutes=1), "ultimo_latido": T0}, T0), 0)

    def test_total_suma_todos_los_tecnicos_de_cada_equipo(self):
        ses = [_ses(0, 10, 10, "e1", 1), _ses(0, 20, 20, "e1", 2, "Beto"), _ses(10, 15, 15, "e2", 1)]
        self.assertEqual(oes.total_por_clave(ses, T0 + timedelta(hours=1)), {"e1": 30 * 60, "e2": 5 * 60})

    def test_texto_duracion(self):
        self.assertEqual(oes.texto_duracion(0), "")
        self.assertEqual(oes.texto_duracion(None), "")
        self.assertEqual(oes.texto_duracion(40), "40s")
        self.assertEqual(oes.texto_duracion(12 * 60), "12m")
        self.assertEqual(oes.texto_duracion(3600 + 5 * 60), "1h 5m")


class TestReanudacion(unittest.TestCase):
    def test_sin_sesion_es_nueva(self):
        self.assertEqual(oes.decidir_reanudacion(None, T0), "nueva")

    def test_con_latido_reciente_se_refresca(self):
        s = _ses(0, latido_min=0)
        self.assertEqual(oes.decidir_reanudacion(s, T0 + timedelta(seconds=oes.GRACIA_SEG)), "refrescar")

    def test_pasada_la_gracia_se_reabre_para_no_contar_el_hueco(self):
        s = _ses(0, latido_min=0)
        self.assertEqual(oes.decidir_reanudacion(s, T0 + timedelta(seconds=oes.GRACIA_SEG + 1)), "reabrir")


class TestOtrosTrabajando(unittest.TestCase):
    AHORA = T0 + timedelta(minutes=1)

    def test_no_incluye_a_quien_pregunta_ni_sesiones_cerradas_ni_viejas(self):
        ses = [_ses(0, None, 1, "e1", 1, "Yo"), _ses(0, None, 1, "e2", 2, "Beto"),
               _ses(0, 0, 0, "e3", 3, "Cerrada"), _ses(-30, None, -30, "e4", 4, "Dormido")]
        self.assertEqual(oes.otros_trabajando(ses, self.AHORA, yo_id=1), {"e2": ["Beto"]})

    def test_sin_repetir_nombres_y_en_orden_de_llegada(self):
        ses = [_ses(0, None, 1, "e1", 2, "Beto"), _ses(0.5, None, 1, "e1", 3, "Carla"), _ses(0.6, None, 1, "e1", 2, "Beto")]
        self.assertEqual(oes.otros_trabajando(ses, self.AHORA, yo_id=1), {"e1": ["Beto", "Carla"]})

    def test_nombre_vacio_no_deja_el_aviso_sin_texto(self):
        ses = [_ses(0, None, 1, "e1", 2, "  ")]
        self.assertEqual(oes.otros_trabajando(ses, self.AHORA, yo_id=1), {"e1": ["Otro técnico"]})

    def test_texto_del_aviso(self):
        self.assertEqual(oes.texto_aviso([]), "")
        self.assertEqual(oes.texto_aviso(["Ana"]), "Ana está trabajando aquí")
        self.assertEqual(oes.texto_aviso(["Ana", "Beto"]), "Ana y Beto están trabajando aquí")
        self.assertEqual(oes.texto_aviso(["Ana", "Beto", "Carla"]), "Ana, Beto y Carla están trabajando aquí")


# ══════════════════════════════════ B) el servidor ══════════════════════════════════
class TestServidor(unittest.TestCase):
    APP = texto("app.py")

    def test_rutas_y_permisos(self):
        post = decoradores("ot_api_equipo_trabajo_registrar")
        self.assertIn('@app.route("/ot/api/<int:vid>/equipo-trabajo", methods=["POST"])', post)
        self.assertIn("@_mant_required", post)
        self.assertIn("@_tecnico_owns_visita", post, "solo el técnico asignado (o superadmin) ejecuta la OT")
        leer = decoradores("ot_api_equipo_trabajo_leer")
        self.assertIn("@_mant_required", leer)
        self.assertIn("@_ot_can_view", leer)

    def test_solo_registra_la_familia_tecnico_y_nunca_falla_hacia_el_tecnico(self):
        f = fuente_funcion("ot_api_equipo_trabajo_registrar")
        self.assertRegex(f, r"registra\s*=\s*bool\(uid and _es_rol_tecnico\(\)\)")
        self.assertIn("sin_registro", f)
        self.assertNotIn("500", f, "un fallo de la base no puede cortar el trabajo del técnico")

    def test_el_tiempo_solo_va_a_la_gestion(self):
        f = fuente_funcion("ot_api_equipo_trabajo_leer")
        self.assertRegex(f, r"if not _es_rol_tecnico\(\):\s*\n\s*totales =")
        # y en la ficha, el tiempo trabajado solo se calcula si quien mira NO es de la familia técnico
        self.assertIn("if _eqs_ok() and not _es_rol_tecnico():\n                _eqs_tot =", fuente_funcion("ot2_detalle"))

    def test_nada_de_plata_ni_proveedores_en_las_funciones_nuevas(self):
        plata = re.compile(r"\$\s?\d|\bproveedor|\bcosto|precio|margen|\bfactura|\bsaldo|valorizad|\bOC\b|\bmonto|ganancia", re.I)
        for n in ("_eqs_aplicar", "_eqs_otros_json", "ot_api_equipo_trabajo_registrar", "ot_api_equipo_trabajo_leer", "_eqs_sesiones"):
            self.assertIsNone(plata.search(fuente_funcion(n)), n)

    def test_la_tabla_es_una_sola_sentencia_barata_y_se_crea_al_arrancar(self):
        f = fuente_funcion("_ensure_ot_equipo_sesiones")
        self.assertEqual(len(re.findall(r"mysql_execute\(", f)), 1)
        self.assertIn("CREATE TABLE IF NOT EXISTS mant_ot_equipo_sesiones", f)
        self.assertNotIn("ALTER", f.upper().replace("ALTERNA", ""))
        self.assertIn("_ensure_ot_equipo_sesiones()", self.APP.split("# ⏱️ 2026-11-04: sesiones de trabajo por equipo en la OT")[1][:400])

    def test_nada_se_borra_y_el_cierre_no_cuenta_el_hueco(self):
        self.assertNotIn("DELETE FROM mant_ot_equipo_sesiones", self.APP)
        self.assertRegex(fuente_funcion("_eqs_cerrar"), r"LEAST\(NOW\(\), ultimo_latido \+ INTERVAL %s SECOND\)")

    def test_abrir_un_equipo_pausa_el_otro(self):
        f = fuente_funcion("_eqs_aplicar")
        self.assertIn('_eqs_cerrar(s["id"], "cambio")', f)
        self.assertIn('if decision == "nueva" and accion == "latido":', f)

    def test_las_pruebas_de_seguridad_no_quedan_con_una_ruta_suelta(self):
        """Ningún nombre de ruta nuevo cae en el patrón financiero que vigila test_confidencialidad_financiera_tecnicos."""
        patron = re.compile(r"finanz|costo|factura|autoriz|regulariz|dashboard|facprov|margen|valoriz", re.I)
        for ruta in ("/ot/api/<int:vid>/equipo-trabajo",):
            self.assertIsNone(patron.search(ruta))


# ══════════════════════════════════ C) la plantilla ══════════════════════════════════
class TestPlantilla(unittest.TestCase):
    T = _leer("templates/ot2/detalle.html")

    def test_cada_tarjeta_lleva_su_clave_y_su_aviso(self):
        self.assertIn("{% set _clave_eq = ('i' ~ e.id) if e._interno else ('e' ~ e.id) %}", self.T)
        self.assertIn('data-clave="{{ _clave_eq }}"', self.T)
        self.assertIn('<span class="otd-wk-pres" data-pres="{{ _clave_eq }}" role="status" hidden></span>', self.T)

    def test_solo_el_tecnico_con_la_ot_en_ejecucion_registra(self):
        self.assertIn("puede: {{ 'true' if (es_tecnico and puede_ejecutar and not ot_solo_lectura) else 'false' }}", self.T)
        self.assertIn("url_for('static', filename='ot_equipo_sesiones.js')", self.T)
        self.assertLess(self.T.index("window.OTD_EQS_CFG"), self.T.index("filename='ot_equipo_sesiones.js'"))

    def test_el_tiempo_trabajado_solo_sale_en_la_tarjeta_de_la_gestion(self):
        gate = self.T.index("{% if not es_tecnico %}\n      <div class=\"otd-crono\">")
        self.assertGreater(self.T.index("m.trabajado"), gate)

    def test_siguiente_equipo_marca_su_apertura_para_no_desplazar_dos_veces(self):
        self.assertIn("if (!el.open) el.dataset.prog = '1';", self.T)

    def test_el_aviso_es_ambar_y_no_bloquea(self):
        css = self.T[self.T.index(".otd-wk-item .nm .otd-wk-pres{"):][:420]
        self.assertIn("#fff8e1", css)
        self.assertNotIn("pointer-events", css)


# ══════════════════════════════════ D) el JS, ejecutado con Node ══════════════════════════════════
_HARNESS = r"""
const fs = require('fs'), vm = require('vm');
const src = fs.readFileSync(process.argv[2], 'utf8');
const espera = () => new Promise(r => setImmediate(r));

function montar(cfg, respuesta) {
  const log = { fetches: [], scrolls: {} };
  const oyentes = { doc: {}, win: {} };
  const disparar = (donde, tipo, ev) => (oyentes[donde][tipo] || []).forEach(fn => fn(ev || {}));
  class Item {
    constructor(clave) { this.clave = clave; this._open = false; this.dataset = {}; }
    get open() { return this._open; }
    set open(v) {
      v = !!v; if (v === this._open) return; this._open = v;
      Promise.resolve().then(() => disparar('doc', 'toggle', { target: this }));   // el evento llega DESPUÉS, como en el navegador
    }
    getAttribute(n) { return n === 'data-clave' ? this.clave : null; }
    matches(sel) { return sel === '#otdWkCard .otd-wk-item'; }
    querySelector() { const s = this; return { scrollIntoView() { log.scrolls[s.clave] = (log.scrolls[s.clave] || 0) + 1; } }; }
  }
  const items = ['e1', 'e2', 'i3'].map(c => new Item(c));
  const pres = items.map(it => ({ clave: it.clave, hidden: true, textContent: '', getAttribute(n) { return n === 'data-pres' ? this.clave : null; } }));
  const intervalos = [];
  const document = {
    hidden: false, readyState: 'complete',
    addEventListener(t, fn) { (oyentes.doc[t] = oyentes.doc[t] || []).push(fn); },
    querySelectorAll(sel) { return sel === '#otdWkCard .otd-wk-item' ? items : (sel === '[data-pres]' ? pres : []); },
  };
  const win = { OTD_EQS_CFG: cfg, addEventListener(t, fn) { (oyentes.win[t] = oyentes.win[t] || []).push(fn); } };
  const fetch = (url, opts) => {
    log.fetches.push([url.replace(/^.*equipo-trabajo/, 'equipo-trabajo'), (opts && opts.method) || 'GET',
      opts && opts.body ? JSON.parse(opts.body) : null]);
    return Promise.resolve({ ok: true, json: () => Promise.resolve(respuesta || { ok: true, otros: {} }) });
  };
  vm.runInNewContext(src, { window: win, document, fetch, JSON, Promise, setInterval: (fn) => { intervalos.push(fn); return intervalos.length; } });
  return { items, pres, document, win, log, intervalos, disparar };
}

(async () => {
  const R = {};
  const cfg = (puede) => ({ vid: 171, puede, latidoSeg: 30 });
  const acciones = (m) => m.log.fetches.filter(f => f[1] === 'POST').map(f => f[2].accion + ':' + f[2].clave + (f[2].motivo ? ':' + f[2].motivo : ''));

  // 1) abrir A y luego B: B contrae a A; solo se inicia B y NO se pausa A por su cuenta (el servidor lo pausa al iniciar B)
  let m = montar(cfg(true)); await espera();
  R.entrada = m.log.fetches.slice();
  m.items[0].open = true; await espera();
  m.items[1].open = true; await espera(); await espera();
  R.cambio = { acciones: acciones(m), abiertos: m.items.map(i => i.open), abierta: m.win.OTD_EQS.abierta(), scrolls: m.log.scrolls };

  // 2) contraer B: se pausa
  m.items[1].open = false; await espera();
  R.contraer = { acciones: acciones(m), abierta: m.win.OTD_EQS.abierta() };

  // 3) la gestión (puede:false) abre equipos: ni sesión ni latido ni lectura, pero el acordeón sí funciona
  m = montar(cfg(false)); await espera();
  m.items[0].open = true; await espera(); m.items[1].open = true; await espera(); await espera();
  R.gestion = { fetches: m.log.fetches.length, abiertos: m.items.map(i => i.open), intervalos: m.intervalos.length };

  // 4) ocultar la pantalla pausa; volver la reanuda
  m = montar(cfg(true)); await espera();
  m.items[2].open = true; await espera();
  m.document.hidden = true; m.disparar('doc', 'visibilitychange'); await espera();
  m.document.hidden = false; m.disparar('doc', 'visibilitychange'); await espera();
  m.disparar('win', 'pagehide'); await espera();
  R.oculta = acciones(m);

  // 5) latido mientras hay uno abierto; sin ninguno, solo lee quién más está trabajando; en segundo plano no manda nada
  m = montar(cfg(true)); await espera();
  m.items[0].open = true; await espera();
  const antes = m.log.fetches.length;
  m.intervalos[0](); await espera();
  m.document.hidden = true; const oculto = m.log.fetches.length; m.intervalos[0](); await espera();
  R.latido = { nuevo: m.log.fetches[antes], oculto_no_manda: m.log.fetches.length === oculto };
  m.document.hidden = false; m.items[0].open = false; await espera();
  const n = m.log.fetches.length; m.intervalos[0](); await espera();
  R.sin_abierto = m.log.fetches[n];

  // 6) lo abrió «Siguiente equipo» (marca prog): no vuelve a desplazar la pantalla y la marca se limpia
  m = montar(cfg(true)); await espera();
  m.items[1].dataset.prog = '1'; m.items[1].open = true; await espera();
  m.items[2].open = true; await espera();
  R.prog = { scrolls: m.log.scrolls, marca: m.items[1].dataset.prog === undefined };

  // 7) el aviso de otro técnico se pinta y se borra
  m = montar(cfg(true), { ok: true, otros: { e2: { nombres: ['Beto'], texto: 'Beto está trabajando aquí' } } }); await espera(); await espera();
  R.aviso = m.pres.map(p => [p.clave, p.hidden, p.textContent]);
  m.win.OTD_EQS.pintar({});
  R.aviso_limpio = m.pres.map(p => [p.hidden, p.textContent]);

  console.log(JSON.stringify(R));
})();
"""


@unittest.skipUnless(HAY_NODE, "Node no está instalado")
class TestJs(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with tempfile.TemporaryDirectory() as d:
            ruta = os.path.join(d, "arnes.js")
            with open(ruta, "w", encoding="utf-8") as fh:
                fh.write(_HARNESS)
            r = subprocess.run(["node", ruta, os.path.join(RAIZ, "static", "ot_equipo_sesiones.js")],
                               capture_output=True, text=True, encoding="utf-8", timeout=90)
        if r.returncode != 0:
            raise AssertionError("node falló:\n" + r.stderr[-2500:])
        cls.R = json.loads(r.stdout.strip().splitlines()[-1])

    def test_sintaxis_y_sin_popups_nativos(self):
        ruta = os.path.join(RAIZ, "static", "ot_equipo_sesiones.js")
        self.assertEqual(subprocess.run(["node", "--check", ruta], capture_output=True, text=True, timeout=60).returncode, 0)
        js = _leer("static/ot_equipo_sesiones.js")
        self.assertIsNone(re.search(r"(?<![\w.])(alert|confirm|prompt)\(", js), "REGLA #1")
        self.assertNotIn("innerHTML", js, "los nombres de otros técnicos se pintan con textContent")

    def test_al_entrar_lee_quien_mas_trabaja_sin_registrar_nada(self):
        self.assertEqual(self.R["entrada"], [["equipo-trabajo", "GET", None]])

    def test_abrir_otro_equipo_contrae_el_anterior_y_solo_inicia_el_nuevo(self):
        r = self.R["cambio"]
        self.assertEqual(r["abiertos"], [False, True, False], "el acordeón deja uno solo abierto")
        self.assertEqual(r["acciones"], ["iniciar:e1", "iniciar:e2"], "el servidor pausa a e1 al iniciar e2")
        self.assertEqual(r["abierta"], "e2")
        self.assertEqual(r["scrolls"], {"e1": 1, "e2": 1}, "al abrir con el dedo la pantalla llega a la cabecera")

    def test_contraer_pausa_el_equipo(self):
        r = self.R["contraer"]
        self.assertEqual(r["acciones"], ["iniciar:e1", "iniciar:e2", "pausar:e2:contrajo"])
        self.assertIsNone(r["abierta"])

    def test_la_gestion_no_registra_nada_pero_el_acordeon_funciona(self):
        r = self.R["gestion"]
        self.assertEqual(r["fetches"], 0)
        self.assertEqual(r["intervalos"], 0)
        self.assertEqual(r["abiertos"], [False, True, False])

    def test_ocultar_la_pantalla_pausa_y_volver_reanuda(self):
        self.assertEqual(self.R["oculta"], ["iniciar:i3", "pausar:i3:salio", "iniciar:i3", "pausar:i3:salio"])

    def test_latido_cada_30s_solo_con_equipo_abierto_y_pantalla_visible(self):
        r = self.R["latido"]
        self.assertEqual(r["nuevo"], ["equipo-trabajo", "POST", {"accion": "latido", "clave": "e1", "motivo": ""}])
        self.assertTrue(r["oculto_no_manda"])
        self.assertEqual(self.R["sin_abierto"], ["equipo-trabajo", "GET", None])

    def test_siguiente_equipo_no_desplaza_dos_veces(self):
        r = self.R["prog"]
        self.assertEqual(r["scrolls"], {"i3": 1}, "e2 lo abrió el programa (ya llevó la pantalla); solo i3 lo abrió el dedo")
        self.assertTrue(r["marca"])

    def test_aviso_de_otro_tecnico(self):
        self.assertEqual(self.R["aviso"], [["e1", True, ""], ["e2", False, "Beto está trabajando aquí"], ["i3", True, ""]])
        self.assertEqual(self.R["aviso_limpio"], [[True, ""], [True, ""], [True, ""]])


if __name__ == "__main__":
    unittest.main()
