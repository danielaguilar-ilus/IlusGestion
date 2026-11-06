# -*- coding: utf-8 -*-
"""🕒 La HORA DEL VEHÍCULO en el asistente «Nueva/Generar OT» se elige en BLOQUES, viene PRELLENADA con la de la OT y es REGLA DEL SISTEMA
(Daniel, 05-nov-2026).

Daniel, viendo el paso «Vehículo para esta OT» con «Sale a las 14:00 / Vuelve a las 18:00» y el aviso «la devolución queda fuera del horario de
llaves»: «¿qué pasó con el horario? Déjalo en bloques y déjalo como regla del sistema… obvio debe prellenar la hora».

Decisiones que se vigilan:
  1. UNA sola fuente del horario de llaves: `flota_reglas.HORARIO_LLAVES` + `PASO_BLOQUE_MIN`. El servidor lo manda (`horario`) y el JS del asistente
     solo trae un valor de partida que estas pruebas comparan con flota_reglas (y con el de Solicitudes de vehículo).
  2. La salida es el COMIENZO de un bloque y la vuelta el FINAL de uno: los mismos extremos de `flota_reglas.en_horario`, así lo prellenado jamás dispara
     el aviso de «fuera del horario».
  3. Se prellena con la hora de la OT llevada a los bloques; si la OT empieza antes de abrir, cae en la colación o termina después del cierre se
     prellena lo más cercano DENTRO del horario y una nota lo dice (nada cambia en silencio). Python (`ajustar_a_bloques`) y JS (`_o2mVehPrellenado`)
     dan LO MISMO (paridad en 1.681 combinaciones).
  4. La hora a mano es la excepción: plegada, y solo alerta. Lo que viaja al servidor no cambia (`desde`, `hasta`, `manualHora`, `hora_desde/hasta`).
Correr:  python -m pytest tests/test_ot_vehiculo_horario_bloques.py -q -p no:warnings
"""
import datetime as dt
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RAIZ)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import jinja2  # noqa: E402

import flota_reglas as fr  # noqa: E402
from test_flota_desde_ot import BaseOT  # noqa: E402


def _leer(*ruta):
    with open(os.path.join(RAIZ, *ruta), encoding="utf-8") as fh:
        return fh.read().replace("\r\n", "\n")


PLANTILLA = _leer("templates", "ot2", "_modal_crear.html")
FLOTA_JS = _leer("static", "flota_uso.js")
INI_JS = PLANTILLA.index("  var _o2mVehToken = 0")
FIN_JS = PLANTILLA.index("  function pintarTecnicos(b){")
BLOQUE_JS = PLANTILLA[INI_JS:FIN_JS]


def mm(t):
    return None if not t else int(t[:2]) * 60 + int(t[3:5])


def hm(m):
    return "" if m is None else "%02d:%02d" % (m // 60, m % 60)


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
#  1. La referencia en Python (flota_reglas): la especificación ejecutable
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestReferenciaPython(unittest.TestCase):
    def aj(self, a, b):
        r = fr.ajustar_a_bloques(mm(a), mm(b))
        return hm(r["desde"]), hm(r["hasta"]), r["motivo_desde"], r["motivo_hasta"]

    def test_el_caso_de_la_captura_14_a_18_sale_14_y_vuelve_17_con_su_motivo(self):
        self.assertEqual(self.aj("14:00", "18:00"), ("14:00", "17:00", "", "despues"))

    def test_una_hora_que_ya_es_un_bloque_no_se_toca(self):
        for a, b in (("08:00", "12:00"), ("09:30", "13:00"), ("14:00", "17:00"), ("12:30", "14:30"), ("16:30", "17:00")):
            self.assertEqual(self.aj(a, b), (a, b, "", ""), (a, b))

    def test_una_hora_dentro_de_un_bloque_se_lleva_a_sus_extremos(self):
        self.assertEqual(self.aj("09:10", "11:50"), ("09:00", "12:00", "bloque", "bloque"))
        self.assertEqual(self.aj("16:40", "16:50"), ("16:30", "17:00", "bloque", "bloque"))

    def test_antes_de_abrir(self):
        self.assertEqual(self.aj("07:30", "09:00"), ("08:00", "09:00", "antes", ""))
        self.assertEqual(self.aj("08:00", "08:00"), ("08:00", "08:30", "", "antes"))       # terminar justo a la apertura: la primera vuelta posible es 08:30

    def test_en_la_colacion_no_se_entregan_llaves(self):
        self.assertEqual(self.aj("13:20", "13:40"), ("14:00", "14:30", "colacion", "colacion"))
        self.assertEqual(self.aj("12:50", "13:10"), ("12:30", "14:30", "bloque", "colacion"))
        self.assertEqual(self.aj("13:00", "13:30"), ("14:00", "14:30", "colacion", "colacion"))

    def test_la_vuelta_puede_caer_al_cierre_de_un_tramo_pero_no_a_su_apertura(self):
        self.assertEqual(fr.ajustar_regreso(13 * 60), (13 * 60, ""))              # 13:00 = cierre de la mañana: se recibe
        self.assertEqual(fr.ajustar_regreso(17 * 60), (17 * 60, ""))              # 17:00 = cierre del día: se recibe
        self.assertEqual(fr.ajustar_regreso(14 * 60), (14 * 60 + 30, "colacion"))  # 14:00 ya es del tramo siguiente: primera vuelta 14:30

    def test_despues_del_cierre_se_prellena_el_ultimo_bloque(self):
        self.assertEqual(self.aj("17:00", "18:00"), ("16:30", "17:00", "despues", "despues"))
        self.assertEqual(self.aj("18:00", "19:00"), ("16:30", "17:00", "despues", "despues"))

    def test_sin_hora_no_se_inventa_nada(self):
        self.assertEqual(self.aj("", "10:00"), ("", "10:00", "", ""))
        self.assertEqual(self.aj("10:00", ""), ("10:00", "", "", ""))
        self.assertEqual(self.aj("", ""), ("", "", "", ""))

    def test_lo_prellenado_siempre_es_un_extremo_de_bloque_y_nunca_dispara_el_aviso_de_fuera_de_horario(self):
        """Lunes 9-nov-2026 (día hábil): toda salida prellenada cae en un horario de entrega y toda vuelta en uno de recepción (`en_horario`)."""
        inicios, finales = fr._inicios_y_finales()
        lun = dt.date(2026, 11, 9)
        for m in range(0, 1440):
            s, _ = fr.ajustar_salida(m)
            e, _ = fr.ajustar_regreso(m)
            self.assertIn(s, inicios, m)
            self.assertIn(e, finales, m)
            self.assertTrue(fr.en_horario(dt.datetime.combine(lun, dt.time(s // 60, s % 60))), ("salida", m, s))
            self.assertTrue(fr.en_horario(dt.datetime.combine(lun, dt.time(e // 60, e % 60)), es_termino=True), ("vuelta", m, e))

    def test_si_la_ot_es_valida_la_vuelta_siempre_queda_despues_de_la_salida(self):
        for a in range(0, 1440, 7):
            for b in range(a + 1, 1440, 11):
                r = fr.ajustar_a_bloques(a, b)
                self.assertGreater(r["hasta"], r["desde"], (a, b, r))

    def test_el_horario_para_pantalla_es_la_unica_fuente(self):
        h = fr.horario_para_pantalla()
        self.assertEqual(h["tramos"], [[480, 780], [840, 1020]])
        self.assertEqual((h["paso_min"], h["texto"]), (30, fr.HORARIO_TEXTO))
        self.assertEqual([tuple(t) for t in h["tramos"]], list(fr.HORARIO_LLAVES))

    def test_funciona_con_otro_horario_sin_tocar_nada_mas(self):
        tramos, paso = ((540, 720), (780, 960)), 60                                 # 09–12 y 13–16, bloques de 1 h
        self.assertEqual(fr.ajustar_salida(9 * 60 + 20, tramos, paso), (540, "bloque"))
        self.assertEqual(fr.ajustar_regreso(16 * 60 + 30, tramos, paso), (960, "despues"))
        self.assertEqual(fr.ajustar_regreso(12 * 60 + 30, tramos, paso), (840, "colacion"))


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
#  2. El servidor manda el horario
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestServidorMandaElHorario(BaseOT):
    def test_la_disponibilidad_manda_los_bloques_y_conserva_el_texto_de_siempre(self):
        j = self.disponibilidad()
        self.assertEqual(j["horario"], fr.horario_para_pantalla())
        self.assertEqual(j["horario_llaves"], fr.HORARIO_TEXTO)

    def test_tambien_lo_manda_sin_hora_porque_el_asistente_lo_necesita_para_prellenar(self):
        j = self.disponibilidad(desde="", hasta="")
        self.assertFalse(j["horario_ok"])
        self.assertEqual(j["horario"]["paso_min"], fr.PASO_BLOQUE_MIN)

    def test_la_lista_de_solicitudes_de_flota_usa_la_misma_fuente(self):
        r = self.c.get("/flota/api/solicitudes", headers={"X-Rol": "superadmin"})
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        h = r.get_json()["horario"]
        self.assertEqual({k: h[k] for k in ("tramos", "texto", "paso_min")}, fr.horario_para_pantalla())
        self.assertEqual((h["tolerancia_min"], h["critico_min"]), (fr.SLA_TOLERANCIA_MIN, fr.SLA_CRITICO_MIN))      # lo de siempre sigue


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
#  3. Una sola fuente: los valores de partida del JS se comparan con flota_reglas
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestUnaSolaFuente(unittest.TestCase):
    def test_el_valor_de_partida_del_asistente_es_el_de_flota_reglas(self):
        m = re.search(r"var _O2M_VEH_TRAMOS_DEF = (\[\[.*?\]\]);", PLANTILLA)
        self.assertIsNotNone(m)
        self.assertEqual([tuple(t) for t in json.loads(m.group(1))], list(fr.HORARIO_LLAVES))
        self.assertEqual(int(re.search(r"var _O2M_VEH_PASO_DEF = (\d+);", PLANTILLA).group(1)), fr.PASO_BLOQUE_MIN)

    def test_el_de_solicitudes_de_vehiculo_tambien(self):
        self.assertEqual(int(re.search(r"var PK_PASO = (\d+);", FLOTA_JS).group(1)), fr.PASO_BLOQUE_MIN)
        for m in re.finditer(r"S\.horario\.tramos \|\| (\[\[.*?\]\])", FLOTA_JS):
            self.assertEqual([tuple(t) for t in json.loads(m.group(1))], list(fr.HORARIO_LLAVES))
        self.assertGreaterEqual(len(re.findall(r"S\.horario\.tramos \|\|", FLOTA_JS)), 3)
        m = re.search(r"horario: \{ tramos: (\[\[.*?\]\])", FLOTA_JS)
        self.assertEqual([tuple(t) for t in json.loads(m.group(1))], list(fr.HORARIO_LLAVES))

    def test_el_asistente_no_tiene_otras_horas_de_llaves_escritas(self):
        """Los minutos 480/780/840/1020 (08:00, 13:00, 14:00, 17:00) aparecen UNA sola vez en el JS del paso: en el valor de partida."""
        lineas = [ln for ln in BLOQUE_JS.splitlines() if re.search(r"\b(480|780|840|1020)\b", ln)]
        self.assertEqual(len(lineas), 1, lineas)
        self.assertIn("_O2M_VEH_TRAMOS_DEF", lineas[0])
        self.assertNotRegex(BLOQUE_JS, r"'(08|13|14|17):00'")

    def test_lo_que_se_agrego_no_trae_sintaxis_de_jinja_que_la_pagina_interpretaria(self):
        i = PLANTILLA.index("/* 🕒 2026-11-05 (Daniel: «¿qué pasó con el horario?")
        css = PLANTILLA[i:PLANTILLA.index("/* ✏️ 2026-10-18", i)]
        j = BLOQUE_JS.index("/* ── HORARIO EN BLOQUES (2026-11-05)")
        for trozo in (css, BLOQUE_JS[j:]):
            for prohibido in ("{{", "{%", "{#"):
                self.assertNotIn(prohibido, trozo, prohibido)

    def test_la_plantilla_sigue_siendo_jinja_valido_y_conserva_los_finales_crlf(self):
        with open(os.path.join(RAIZ, "templates", "ot2", "_modal_crear.html"), "rb") as fh:
            crudo = fh.read()
        self.assertEqual(crudo.count(b"\n"), crudo.count(b"\r\n"))
        jinja2.Environment(extensions=["jinja2.ext.do"]).parse(crudo.decode("utf-8"))

    def test_lo_que_viaja_al_servidor_no_cambio(self):
        self.assertIn("body.vehiculo = {vehiculo_id: _vh.vehiculo_id, chofer_id: _vh.chofer_id, hora_desde: _vh.desde, hora_hasta: _vh.hasta};", PLANTILLA)
        self.assertIn("vehSel:vehSel, vehChofer:vehChofer, vehHora:vehHora", PLANTILLA)
        for nuevo in ("vehBloque:vehBloque", "vehAjuste:vehAjuste", "vehRestaurar:vehRestaurar"):
            self.assertIn(nuevo, PLANTILLA)

    def test_sin_alert_ni_confirm_nativos(self):
        self.assertNotRegex(BLOQUE_JS, r"(?<![\w.])(alert|confirm|prompt)\(")


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
#  4. La lógica y lo que se pinta, EJECUTADO en Node (JS real del asistente en un contexto aislado)
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
HAY_NODE = shutil.which("node") is not None

_PRUEBA_JS = r"""
const vm = require('vm'), fs = require('fs');
const src = fs.readFileSync(process.argv[2], 'utf8');
const bloque = src.slice(src.indexOf('  var _o2mVehToken = 0'), src.indexOf('  function pintarTecnicos(b){'));
const resultados = [];
const ok = (nombre, cond, extra) => resultados.push({nombre, ok: !!cond, extra: cond ? '' : String(extra || '')});

function montar(opts) {
  opts = opts || {};
  const els = {o2mVehCuerpo: {innerHTML: '', contains: () => false}, o2mVehLive: {textContent: ''}};
  const toasts = [], pendientes = [];
  const veh = {vehiculo_id: null, chofer_id: null, usuario_id: null, desde: '', hasta: '', manualHora: false, manualChofer: false, data: null, cargando: false, error: false, aviso: ''};
  if (!opts.vehViejo) { veh.notas = []; veh.ajusteAbierto = null; }
  const ctx = {
    S: {fecha: '2026-11-06', hini: 'hini' in opts ? opts.hini : '14:00', hfin: 'hfin' in opts ? opts.hfin : '18:00', tecnicos: [1, 2], lider: 1, desinst: {lleva: ''}, veh},
    cacheTec: [{id: 1, nombre: 'Ana', empresa_id: null, es_externo: false}, {id: 2, nombre: 'Beto', empresa_id: null, es_externo: false}],
    O2M_ES_TECNICO: false, window: {}, console,
    document: {activeElement: null, getElementById: id => els[id] || null},
    esc: s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'}[c])),
    refrescarAnclas() {}, pintarFormulario() {}, _o2mDiAplica: () => false,
    ilusToast: (m, o) => toasts.push({m, tipo: o && o.type}), encodeURIComponent, setTimeout, clearTimeout,
    fetch: url => new Promise(res => pendientes.push({url, responder: d => res({json: () => Promise.resolve(d)})})),
  };
  ctx.window.ilusToast = ctx.ilusToast;           // el código pregunta `if (window.ilusToast) ilusToast(...)`
  vm.createContext(ctx);
  vm.runInContext(bloque, ctx);
  return {ctx, els, toasts, pendientes};
}
const datos = (extra) => Object.assign({ok: true, horario_ok: true, vehiculos: [{id: 10, titulo: 'Furgón TVDZ-88', marca_modelo: 'Changan', licencia_exigida: ['B'], libre: true, motivo: null, choque: null}],
  choferes: [{usuario_id: 1, nombre: 'Lenin', chofer_id: 101, enrolado: true, motivo: null, por_vehiculo: {10: {ok: true}}}], avisos: [], horario_llaves: 'lunes a viernes', sla_respuesta_min: 120}, extra || {});
const conVehiculo = (m, extra) => { const V = m.ctx.S.veh; V.data = datos(extra); V.vehiculo_id = 10; V.chofer_id = 101; m.ctx._o2mVehSync(); m.ctx._o2mVehPintar(); return m.els.o2mVehCuerpo.innerHTML; };
const cuenta = (h, re) => (h.match(re) || []).length;
const tick = () => new Promise(r => setTimeout(r, 0));

(async () => {
  // A. El caso de la captura: OT 14:00–18:00 → sale 14:00, vuelve 17:00 y una nota ámbar lo dice
  { const m = montar(), {ctx} = m, V = ctx.S.veh;
    ctx._o2mVehSync();
    ok('captura: sale 14:00 y vuelve 17:00', V.desde === '14:00' && V.hasta === '17:00', V.desde + '-' + V.hasta);
    const warn = (V.notas || []).filter(n => n.tono === 'warn').map(n => n.t);
    ok('captura: la nota dice exactamente qué pasó', warn.length === 1 && warn[0] === 'La OT termina a las 18:00, pero las llaves se reciben hasta las 17:00: la devolución quedó a las 17:00. Si el vehículo vuelve más tarde, ajusta la hora a mano.', warn.join(' | ')); }

  // B. Lo que se pinta: bloques ya elegidos, colación, ámbar, «Ajustar a mano» plegado
  { const m = montar(), h = conVehiculo(m);
    ok('16 comienzos de bloque para la salida', cuenta(h, /id="o2mVehBdesde\d+"/g) === 16, cuenta(h, /id="o2mVehBdesde\d+"/g));
    ok('16 finales de bloque para la vuelta', cuenta(h, /id="o2mVehBhasta\d+"/g) === 16, cuenta(h, /id="o2mVehBhasta\d+"/g));
    ok('la salida 14:00 sale ya elegida', /id="o2mVehBdesde840" class="o2m-hb-b sel" aria-pressed="true"/.test(h));
    ok('la vuelta 17:00 sale ya elegida', /id="o2mVehBhasta1020" class="o2m-hb-b sel" aria-pressed="true"/.test(h));
    ok('solo hay UN bloque elegido por fila', cuenta(h, /aria-pressed="true"/g) === 2, cuenta(h, /aria-pressed="true"/g));
    ok('las vueltas anteriores o iguales a la salida están deshabilitadas (gris, sin opacidad)', /id="o2mVehBhasta510" class="o2m-hb-b off" aria-pressed="false" aria-disabled="true"/.test(h) && /id="o2mVehBhasta870" class="o2m-hb-b"/.test(h));
    ok('se ve la colación', /Colación 13:00–14:00 · no se entregan llaves/.test(h));
    ok('se ven Mañana y Tarde con su rango', /Mañana · 08:00–13:00/.test(h) && /Tarde · 14:00–17:00/.test(h));
    ok('la nota ámbar está a la vista', /La OT termina a las 18:00, pero las llaves se reciben hasta las 17:00/.test(h) && /color:#92400e/.test(h));
    ok('resumen: sale, vuelve y cuánto dura', /Sale <b>14:00<\/b> · Vuelve <b>17:00<\/b> <span>\(3 h\)<\/span>/.test(h), h.slice(h.indexOf('o2m-hb-res'), h.indexOf('o2m-hb-res') + 200));
    ok('«Ajustar a mano» sale plegado', /<details class="o2m-hb-mano" id="o2mVehAjuste" ontoggle/.test(h) && !/id="o2mVehAjuste" open/.test(h));
    ok('los dos <input type=time> siguen ahí con step=60 y sus ids de siempre', /type="time" step="60" class="form-control" id="o2mVehDesde" value="14:00"/.test(h) && /id="o2mVehHasta" value="17:00"/.test(h));
    ok('grupo accesible y SOLO dos radiogroup (vehículo y quién maneja)', /role="group" aria-label="Horario del vehículo, en bloques de 30 minutos"/.test(h) && cuenta(h, /role="radiogroup"/g) === 2);
    ok('nada de opacidad ni de popups nativos', !/opacity/.test(h) && !/(alert|confirm|prompt)\(/.test(h)); }

  // C. Prellenado según la OT (casos borde)
  { const casos = [
      ['08:00', '12:00', '08:00', '12:00', 0], ['09:10', '11:50', '09:00', '12:00', 0], ['07:30', '09:00', '08:00', '09:00', 1],
      ['13:20', '13:40', '14:00', '14:30', 2], ['17:30', '18:30', '16:30', '17:00', 2], ['10:00', '', '10:00', '', 0], ['', '', '', '', 0]];
    for (const [i, f, d, h, nWarn] of casos) {
      const m = montar({hini: i, hfin: f}), V = m.ctx.S.veh; m.ctx._o2mVehSync();
      const w = (V.notas || []).filter(n => n.tono === 'warn').length;
      ok('OT ' + (i || '—') + '–' + (f || '—') + ' → ' + (d || '—') + '–' + (h || '—') + ' (' + nWarn + ' aviso(s) ámbar)', V.desde === d && V.hasta === h && w === nWarn, V.desde + '-' + V.hasta + ' warn=' + w); }
    const m = montar({hini: '09:10', hfin: '11:50'}); m.ctx._o2mVehSync();
    ok('si solo se llevó al bloque, la nota es informativa (gris), no ámbar', (m.ctx.S.veh.notas || []).length === 1 && m.ctx.S.veh.notas[0].tono === 'info' && /bloques de 30 min: sale a las 09:00 y vuelve a las 12:00/.test(m.ctx.S.veh.notas[0].t)); }

  // D. Lo que la persona elige a mano ya no lo pisa la OT; «Volver a la hora de la OT» lo deshace
  { const m = montar(), {ctx, toasts} = m, V = ctx.S.veh; conVehiculo(m);
    ctx.vehBloque('desde', 600);
    ok('tocar un bloque cambia la salida, marca manual y limpia las notas', V.desde === '10:00' && V.hasta === '17:00' && V.manualHora === true && V.notas.length === 0, V.desde + '-' + V.hasta);
    ctx.S.hini = '09:00'; ctx._o2mVehSync(); ctx._o2mVehPintar();
    ok('la hora de la OT ya no pisa lo elegido', V.desde === '10:00', V.desde);
    let h = m.els.o2mVehCuerpo.innerHTML;
    ok('con lo elegido a mano aparece «Volver a la hora de la OT»', /Volver a la hora de la OT \(09:00–18:00\)/.test(h) && !/La OT termina a las 18:00, pero/.test(h), h.slice(0, 120));
    ctx.vehRestaurar();
    ok('«Volver a la hora de la OT» la prellena de nuevo (09:00 → 17:00 por el cierre)', V.desde === '09:00' && V.hasta === '17:00' && V.manualHora === false && V.notas.length === 1, V.desde + '-' + V.hasta); }

  // E. La vuelta nunca queda antes de la salida, y si hay que moverla SE DICE
  { const m = montar({hini: '08:00', hfin: '08:30'}), {ctx, toasts} = m, V = ctx.S.veh; conVehiculo(m);
    ctx.vehBloque('desde', 570);
    ok('salida 09:30 con vuelta 08:30: la vuelta pasa a 10:00', V.desde === '09:30' && V.hasta === '10:00', V.desde + '-' + V.hasta);
    ok('y se avisa (toast y lector de pantalla)', toasts.some(t => /La vuelta pasó a las 10:00/.test(t.m)) && /La vuelta pasó a las 10:00/.test(m.els.o2mVehLive.textContent), JSON.stringify(toasts));
    ctx.vehBloque('hasta', 570);
    ok('elegir una vuelta anterior o igual a la salida se rechaza con un aviso', V.hasta === '10:00' && toasts.some(t => /tiene que ser después de la salida/.test(t.m)), V.hasta);
    ctx.vehBloque('desde', 605); ctx.vehBloque('hasta', 1050); ctx.vehBloque('desde', 'x'); ctx.vehBloque('otro', 600);
    ok('solo se aceptan extremos reales de bloque (nada inventado)', V.desde === '09:30' && V.hasta === '10:00', V.desde + '-' + V.hasta);
    ctx.vehBloque('hasta', 780);
    ok('la vuelta puede ser el cierre de la mañana (13:00)', V.hasta === '13:00', V.hasta); }

  // F. El servidor manda el horario: manda sobre el de partida
  { const m = montar({hini: '09:00', hfin: '16:00'}), {ctx} = m;
    const h = conVehiculo(m, {horario: {tramos: [[540, 720], [780, 960]], paso_min: 60, texto: 'x'}});
    ok('con el horario del servidor: bloques de 1 hora (3 + 3)', cuenta(h, /id="o2mVehBdesde\d+"/g) === 6 && /Colación 12:00–13:00/.test(h), cuenta(h, /id="o2mVehBdesde\d+"/g));
    ok('y el prellenado lo usa (09:00 → 16:00)', ctx.S.veh.desde === '09:00' && ctx.S.veh.hasta === '16:00', ctx.S.veh.desde + '-' + ctx.S.veh.hasta); }
  { const m = montar({hini: '08:00', hfin: '12:00'}), {ctx, pendientes} = m;
    ctx.pintarVehiculo({innerHTML: ''});
    ok('la primera consulta usa la hora ya llevada a los bloques', /desde=08%3A00&hasta=12%3A00/.test(pendientes[0].url), pendientes[0].url);
    pendientes[0].responder(datos({horario: {tramos: [[540, 720], [780, 960]], paso_min: 60, texto: 'x'}})); await tick(); await tick();
    ok('si el servidor manda OTRO horario la hora se prellena de nuevo y se vuelve a consultar', ctx.S.veh.desde === '09:00' && ctx.S.veh.hasta === '12:00', ctx.S.veh.desde + '-' + ctx.S.veh.hasta); }

  // G. «Ajustar a mano»: se abre solo si la hora no cae en un bloque (una vez); la persona manda
  { const m = montar(), {ctx} = m, V = ctx.S.veh; conVehiculo(m);
    ctx.vehHora('desde', '12:10'); ctx.vehHora('hasta', '15:00');
    let h = m.els.o2mVehCuerpo.innerHTML;
    ok('una hora a mano fuera de los bloques abre «Ajustar a mano»', /id="o2mVehAjuste" open/.test(h) && !/aria-pressed="true"[^>]*>\s*<i class="bi bi-check-lg me-1"><\/i>12:10/.test(h), h.slice(h.indexOf('o2mVehAjuste') - 40, h.indexOf('o2mVehAjuste') + 80));
    ok('y los bloques no marcan nada falso', cuenta(h, /aria-pressed="true"/g) === 1, cuenta(h, /aria-pressed="true"/g));
    ctx.vehAjuste(false); ctx._o2mVehPintar(); h = m.els.o2mVehCuerpo.innerHTML;
    ok('si la persona lo pliega, queda plegado', !/id="o2mVehAjuste" open/.test(h)); }

  // H. Foco: repintar tras tocar un bloque no lo pierde
  { const m = montar(), {ctx, els} = m; conVehiculo(m); let enfocado = null;
    const boton = {id: 'o2mVehBdesde840', focus() { enfocado = 'o2mVehBdesde840'; }};
    els.o2mVehBdesde840 = boton; ctx.document.activeElement = boton; els.o2mVehCuerpo.contains = el => el === boton;
    ctx._o2mVehPintar();
    ok('el foco vuelve al mismo bloque tras repintar', enfocado === 'o2mVehBdesde840', enfocado); }

  // I. Compatibilidad: un estado sin los campos nuevos (S.veh viejo) también funciona
  { const m = montar({vehViejo: true}), {ctx} = m, V = ctx.S.veh;
    ctx._o2mVehSync(); ok('S.veh sin `notas` ni `ajusteAbierto` no rompe', V.desde === '14:00' && V.hasta === '17:00');
    const h = conVehiculo(m); ok('y se pinta igual', /id="o2mVehBdesde840"/.test(h)); }

  // J. El texto del servidor va escapado en las notas (la hora de la OT es texto de una persona)
  { const m = montar({hini: '<b>7:30</b>', hfin: '09:00'}), {ctx} = m; ctx._o2mVehSync();
    ok('una hora inválida no inventa nada', ctx.S.veh.desde === '' && ctx.S.veh.hasta === '09:00', ctx.S.veh.desde + '-' + ctx.S.veh.hasta); }

  // K. Paridad con Python: la misma cuenta en 41 × 41 combinaciones (incluye «sin hora»)
  const M = [0, 59, 360, 420, 479, 480, 481, 490, 509, 510, 539, 540, 570, 600, 720, 749, 750, 779, 780, 781, 799, 800, 810, 839, 840, 841, 869, 870, 900, 960, 989, 990, 1019, 1020, 1021, 1050, 1080, 1110, 1200, 1439];
  const fmt = m => String(Math.floor(m / 60)).padStart(2, '0') + ':' + String(m % 60).padStart(2, '0');
  const lista = [''].concat(M.map(fmt)), filas = [];
  { const {ctx} = montar();
    for (const a of lista) for (const b of lista) { const r = ctx._o2mVehPrellenado(a, b); filas.push([a, b, r.desde, r.hasta, r.kd, r.kh]); } }
  console.log(JSON.stringify({resultados, filas}));
})();
"""


@unittest.skipUnless(HAY_NODE, "Node no está instalado")
class TestAsistenteEjecutado(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with tempfile.TemporaryDirectory() as d:
            prueba = os.path.join(d, "prueba.js")
            with open(prueba, "w", encoding="utf-8") as f:
                f.write(_PRUEBA_JS)
            r = subprocess.run(["node", prueba, os.path.join(RAIZ, "templates", "ot2", "_modal_crear.html")],
                               capture_output=True, text=True, timeout=120, encoding="utf-8")
        cls.proc = r
        cls.salida = json.loads(r.stdout.strip().splitlines()[-1]) if r.returncode == 0 and r.stdout.strip() else None

    def test_el_script_corre_sin_errores(self):
        self.assertEqual(self.proc.returncode, 0, self.proc.stderr[-1500:])
        self.assertIsNotNone(self.salida)

    def test_toda_la_logica_del_paso_pasa(self):
        self.assertIsNotNone(self.salida, self.proc.stderr[-1500:])
        res = self.salida["resultados"]
        self.assertGreaterEqual(len(res), 40)
        fallas = [x for x in res if not x["ok"]]
        self.assertEqual(fallas, [], json.dumps(fallas, ensure_ascii=False, indent=1))

    def test_paridad_con_python_en_1681_combinaciones(self):
        self.assertIsNotNone(self.salida, self.proc.stderr[-1500:])
        filas = self.salida["filas"]
        self.assertEqual(len(filas), 41 * 41)
        distintas = []
        for a, b, d, h, kd, kh in filas:
            r = fr.ajustar_a_bloques(mm(a), mm(b))
            esperado = [hm(r["desde"]), hm(r["hasta"]), r["motivo_desde"], r["motivo_hasta"]]
            if [d, h, kd, kh] != esperado:
                distintas.append(((a, b), [d, h, kd, kh], esperado))
        self.assertEqual(distintas, [], distintas[:5])


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
#  5. La regla del sistema está escrita
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestLaReglaEstaEscrita(unittest.TestCase):
    def test_claude_md_tiene_la_regla_del_horario_en_bloques(self):
        md = _leer("CLAUDE.md")
        i = md.index("## 🕒 REGLA #63")
        regla = md[i:md.index("\n---\n", i)]
        for pieza in ("BLOQUES", "PRELLENADA", "flota_reglas.HORARIO_LLAVES", "PASO_BLOQUE_MIN", "horario_para_pantalla", "ajustar_a_bloques",
                      "_o2mVehPrellenado", "Ajustar a mano", "solo alerta", "Nada se mueve en silencio", "2026-11-05", "regla del sistema",
                      "tests/test_ot_vehiculo_horario_bloques.py"):
            self.assertIn(pieza, regla, pieza)


if __name__ == "__main__":
    unittest.main()
