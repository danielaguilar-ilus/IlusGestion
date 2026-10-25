# -*- coding: utf-8 -*-
"""2026-10-25 · Revisión independiente del calendario único «Entrada | Salida» (REGLA #43): lo que encontró se corrigió y SE EJECUTA acá.

El revisor (un agente aparte, solo lectura) probó el paso Agenda en Chromium a 1024, 700, 375 y 320 px, con teclas reales, y encontró 12 cosas.
Estas pruebas EJECUTAN el JavaScript real de la plantilla en Node (con un documento de mentira) —no solo buscan texto— para que no vuelvan:

  1. (ALTA)  En celular la caja roja de choque de la salida quedaba ilegible (el botón aplastaba el texto).        → CSS
  2. (MEDIA) Cambiar de orden durante una espera de red hacía que una orden escribiera sobre la otra.              → _o2mDayIrADia, Sugerir, _agAplicarOrden, setFecha
  3. (MEDIA) Escribir a mano la fecha de la salida la borraba en cada dígito.                                      → setFecha
  4. (MEDIA) Las fichas, la guía y el check de «Agenda» decían «lista» mientras el servidor bloqueaba.             → _diSalidaBloqueada y compañía
  5. (MEDIA) «Sugerir horario» ignoraba la otra orden.                                                              → _agLimiteOtraOrden, _o2mDayCalcularHueco
  6. (BAJA)  El choque de la entrada reaparecía en la salida.                                                       → o2mChequearChoque
  7. (BAJA)  El clic en un hueco cambió de comportamiento en el camino SIN «con desinstalación».                    → o2mDaySlotClick
  8. (BAJA)  El choque de un técnico que no es líder no se explicaba con texto en la salida.                        → _diAvisosHtml
  9. (BAJA)  Accesibilidad: aria-live repetía los atajos, el foco caía al <body>, la barra pegajosa tapaba el foco. → _agPintarConFoco, CSS
 10. (BAJA)  Botones táctiles bajo 44 px en celular.                                                                → CSS
 12. (NIT)   backdrop-filter en la barra pegajosa.                                                                  → CSS
"""
import re
import unittest

from tests.test_ot_desinst_calendario import CSS, JS, NODE, _funcion, _node

BASE_S = r"""
const S = { agOrden: 'sal', tipo: 'instalacion', conDesinst: true, cliente: {id: 1}, fecha: '2026-10-27', hini: '09:00', hfin: '13:00', tecnicos: [11], lider: 11,
  desinst: { fecha: '2026-12-01', hini: '09:00', hfin: '13:00', tec_modo: 'mismos', tecnicos: [], lider: null, previo: null, previoPend: false, forzar: false } };
const O2M_ES_TECNICO = false, O2M_DAY_END = 22 * 60, O2M_DAY_START = 8 * 60, O2M_DAY_MINDUR = 30;
const esInterno = function () { return false; };
function _o2mDiAplica() { return S.tipo === 'instalacion' && !!S.conDesinst && !esInterno() && !O2M_ES_TECNICO && !!S.cliente; }
const _o2mHoyStr = function () { return '2026-10-24'; }, _o2mDayHoyStr = _o2mHoyStr;
const _o2mFechaBonita = function (f) { return f; };
const _o2mEsDomingo = function (f) { return new Date(f + 'T12:00:00').getDay() === 0; };
"""


def _piezas(*nombres):
    return "\n".join(_funcion(n) for n in nombres)


ACCESORES = ("_agSal", "agFecha", "agHini", "agHfin", "agTecnicos", "agLider", "_agDueno", "agMinFecha", "_diTec", "_diLider", "_o2mDayToMin", "_o2mDayHHMM")


@unittest.skipIf(not NODE, "Node no está instalado")
class TestCambiarDeOrdenMientrasEspera(unittest.TestCase):
    """Hallazgo 2: una respuesta de red que llega después de cambiar de orden NUNCA escribe sobre la otra orden."""

    @classmethod
    def setUpClass(cls):
        piezas = _piezas(*ACCESORES, "_agLimiteOtraOrden", "_o2mDayIrADia", "o2mDaySugerirHueco", "_agAplicarOrden")
        cls.o = _node(BASE_S + r"""
let trace = [], gate = null, busqueda = null, capturado = [];
const marca = function (n) { return function () { trace.push(n); }; };
const o2mCal = { anio: 2026, mes: 10, diaSel: null };
const o2mDay = { fecha: null };
const o2mCalCargarMes = function () { trace.push('cargarMes'); return gate || Promise.resolve(); };
const o2mCalSelDia = marca('selDia'), refrescarAnclas = marca('refrescarAnclas'), _o2mAnexoSincronizarFechas = marca('anexo'),
  _diPintarRegiones = marca('diRegiones'), _diProgramarPrevio = marca('diPrevio'), o2mCalRenderGrid = marca('grid'), _o2mAgResumenPintar = marca('resumen'),
  avisosAgenda = marca('avisos'), _o2mAgAvisosSalida = marca('avisosSalida'), o2mChequearChoque = marca('choque');
const o2mDayRender = function (f) { trace.push('dayRender:' + f); };
const horas = []; const setHora = function (c, v) { horas.push(c + '=' + v + '@' + S.agOrden); };
const toasts = []; global.window = { ilusToast: function (m) { toasts.push(m); } }; const ilusToast = global.window.ilusToast;
const _o2mDayBuscarHuecoRango = function () { capturado.push(Array.prototype.slice.call(arguments)); return busqueda || Promise.resolve(null); };
const el = {}; global.document = { getElementById: function (id) { return (id === 'o2mFecha' || id === 'o2mHoraIni' || id === 'o2mHoraFin') ? (el[id] = el[id] || { value: '' }) : null; } };
""" + piezas + r"""
const out = {};
(async function () {
  // ── _o2mDayIrADia ───────────────────────────────────────────────────────────────────────────────────────────────
  trace = []; gate = null; o2mCal.anio = 2026; o2mCal.mes = 10; S.agOrden = 'sal'; S.desinst.fecha = '2026-12-01';
  out.ir_ok = await _o2mDayIrADia('2026-11-12', 'sal');
  out.ir_ok_salida = S.desinst.fecha; out.ir_ok_entrada = S.fecha; out.ir_ok_trace = trace.slice();

  trace = []; o2mCal.anio = 2026; o2mCal.mes = 10; S.agOrden = 'sal'; S.desinst.fecha = '2026-12-01';
  let soltar; gate = new Promise(function (r) { soltar = r; });
  let p = _o2mDayIrADia('2026-11-20', 'sal');
  S.agOrden = 'ent';                                   // se cambia de orden MIENTRAS llega el mes
  soltar(); gate = null;
  out.ir_cruzada = await p; out.ir_cruzada_salida = S.desinst.fecha; out.ir_cruzada_entrada = S.fecha; out.ir_cruzada_trace = trace.slice();

  // la entrada (sin cambio de mes) escribe S.fecha y sincroniza el anexo
  trace = []; S.agOrden = 'ent'; o2mCal.anio = 2026; o2mCal.mes = 10;
  out.ir_entrada = await _o2mDayIrADia('2026-10-29', 'ent'); out.ir_entrada_fecha = S.fecha; out.ir_entrada_trace = trace.slice();
  S.fecha = '2026-10-27';

  // llamada para una orden que YA no es la activa (sin esperar nada): no escribe ni el casillero de fecha
  trace = []; S.agOrden = 'ent'; el.o2mFecha = { value: '2026-10-27' };
  out.ir_otra = await _o2mDayIrADia('2026-11-20', 'sal'); out.ir_otra_input = el.o2mFecha.value; out.ir_otra_salida = S.desinst.fecha; out.ir_otra_trace = trace.slice();

  // ── «Sugerir horario» ───────────────────────────────────────────────────────────────────────────────────────────
  S.agOrden = 'sal'; S.desinst.fecha = '2026-12-01'; capturado = []; horas.length = 0; toasts.length = 0; trace = [];
  busqueda = Promise.resolve({ fecha: '2026-10-27', hora: '13:00' });
  await o2mDaySugerirHueco();
  out.sug_sal = { args: capturado[0].slice(3), horas: horas.slice(), salida: S.desinst.fecha };

  S.agOrden = 'ent'; S.desinst.fecha = '2026-11-05'; S.desinst.hini = '09:00'; capturado = []; horas.length = 0; busqueda = Promise.resolve({ fecha: '2026-10-28', hora: '10:00' });
  await o2mDaySugerirHueco();
  out.sug_ent = { args: capturado[0].slice(3), horas: horas.slice(), fecha: S.fecha };
  S.fecha = '2026-10-27';

  S.conDesinst = false; capturado = []; horas.length = 0; busqueda = Promise.resolve({ fecha: '2026-10-28', hora: '10:00' });
  await o2mDaySugerirHueco();
  out.sug_simple = { args: capturado[0].slice(3) };                                // sin la tarjeta nueva: sin límites de la otra orden
  S.conDesinst = true; S.fecha = '2026-10-27';

  // el resultado llega DESPUÉS de cambiar de orden: no se escribe nada
  S.agOrden = 'sal'; S.desinst.fecha = '2026-12-01'; horas.length = 0; trace = []; o2mCal.anio = 2026; o2mCal.mes = 10;
  let llega; busqueda = new Promise(function (r) { llega = function () { r({ fecha: '2026-11-12', hora: '10:00' }); }; });
  el.o2mFecha = { value: '2026-10-27' }; toasts.length = 0;
  p = o2mDaySugerirHueco();
  S.agOrden = 'ent';
  llega(); await p;
  out.sug_cruzada = { horas: horas.slice(), salida: S.desinst.fecha, entrada: S.fecha, trace: trace.slice(), input: el.o2mFecha.value, toasts: toasts.slice() };

  // lo mismo cuando la búsqueda NO encuentra nada: tampoco se avisa «sin huecos» de la orden que ya no está a la vista
  S.agOrden = 'sal'; toasts.length = 0; let llega2; busqueda = new Promise(function (r) { llega2 = function () { r(null); }; });
  p = o2mDaySugerirHueco();
  S.agOrden = 'ent';
  llega2(); await p;
  out.sug_cruzada_sin_huecos = toasts.slice();

  // sin huecos antes de la salida: el mensaje lo dice
  S.agOrden = 'ent'; S.desinst.fecha = '2026-11-05'; toasts.length = 0; busqueda = Promise.resolve(null);
  await o2mDaySugerirHueco(); out.sug_sin_huecos_entrada = toasts.slice();
  S.agOrden = 'sal'; S.desinst.fecha = '2026-12-01'; toasts.length = 0; busqueda = Promise.resolve(null);
  await o2mDaySugerirHueco(); out.sug_sin_huecos_salida = toasts.slice();

  // la salida sin técnico no busca
  S.tecnicos = []; capturado = []; toasts.length = 0;
  await o2mDaySugerirHueco(); out.sug_sin_tecnico = { busquedas: capturado.length, toasts: toasts.slice() }; S.tecnicos = [11];

  // ── _agAplicarOrden: el pintado atrasado de la orden anterior se descarta ─────────────────────────────────────────
  S.agOrden = 'ent'; S.desinst.fecha = '2026-11-12'; trace = []; o2mCal.anio = 2026; o2mCal.mes = 10;
  gate = new Promise(function (r) { soltar = r; });
  S.agOrden = 'sal'; _agAplicarOrden();                // salida: noviembre aún sin cargar
  S.agOrden = 'ent';                                  // vuelve a la entrada antes de que llegue
  soltar(); gate = null; await new Promise(function (r) { setTimeout(r, 10); });
  out.aplicar_descartado = trace.filter(function (t) { return t.indexOf('dayRender:2026-11') === 0; });
  console.log(JSON.stringify(out));
})();
""")

    def test_ir_a_un_dia_escribe_la_fecha_de_la_orden_para_la_que_se_busca(self):
        self.assertTrue(self.o["ir_ok"])
        self.assertEqual((self.o["ir_ok_salida"], self.o["ir_ok_entrada"]), ("2026-11-12", "2026-10-27"))
        self.assertIn("diPrevio", self.o["ir_ok_trace"])
        self.assertNotIn("anexo", self.o["ir_ok_trace"])                       # el anexo toma la fecha de la ENTRADA
        self.assertTrue(self.o["ir_entrada"])
        self.assertEqual(self.o["ir_entrada_fecha"], "2026-10-29")
        self.assertIn("anexo", self.o["ir_entrada_trace"])

    def test_si_se_cambia_de_orden_mientras_llega_el_mes_no_se_escribe_nada(self):
        self.assertFalse(self.o["ir_cruzada"])
        self.assertEqual(self.o["ir_cruzada_salida"], "2026-12-01")           # la salida quedó como estaba…
        self.assertEqual(self.o["ir_cruzada_entrada"], "2026-10-27")          # …y la entrada no recibió la fecha de la salida
        self.assertNotIn("selDia", self.o["ir_cruzada_trace"])

    def test_una_fecha_para_una_orden_que_ya_no_es_la_activa_ni_toca_el_casillero(self):
        self.assertFalse(self.o["ir_otra"])
        self.assertEqual(self.o["ir_otra_input"], "2026-10-27")               # el casillero de fecha de la orden activa no se pisó
        self.assertEqual(self.o["ir_otra_salida"], "2026-12-01")
        self.assertEqual(self.o["ir_otra_trace"], [])                         # y no se pidió ningún mes ni se repintó nada

    def test_sugerir_para_la_salida_el_mismo_dia_de_la_entrada_parte_cuando_ella_termina(self):
        a = self.o["sug_sal"]["args"]                                          # [maxDias, lim, tope]
        self.assertEqual(a[0], 30)
        self.assertEqual(a[1], {"fecha": "2026-10-27", "antes": 13 * 60})
        self.assertIsNone(a[2])
        self.assertEqual(self.o["sug_sal"]["horas"], ["hini=13:00@sal", "hfin=17:00@sal"])
        self.assertEqual(self.o["sug_sal"]["salida"], "2026-10-27")

    def test_sugerir_para_la_entrada_no_pasa_de_la_fecha_de_la_salida_ni_termina_despues_de_que_empiece(self):
        a = self.o["sug_ent"]["args"]
        self.assertEqual(a[0], 14)
        self.assertEqual(a[1], {"fecha": "2026-11-05", "despues": 9 * 60})
        self.assertEqual(a[2], "2026-11-05")
        self.assertEqual(self.o["sug_ent"]["fecha"], "2026-10-28")

    def test_sin_la_tarjeta_nueva_sugerir_no_trae_limites_de_otra_orden(self):
        a = self.o["sug_simple"]["args"]
        self.assertEqual((a[1], a[2]), (None, None))

    def test_si_se_cambia_de_orden_mientras_busca_el_resultado_se_descarta(self):
        c = self.o["sug_cruzada"]
        self.assertEqual(c["horas"], [])                                       # no se escribió ninguna hora
        self.assertEqual((c["salida"], c["entrada"]), ("2026-12-01", "2026-10-27"))
        self.assertNotIn("selDia", c["trace"])
        self.assertEqual(c["input"], "2026-10-27")                             # ni se pisó el casillero de fecha de la orden que sí está a la vista
        self.assertNotIn("cargarMes", c["trace"])
        self.assertEqual(self.o["sug_cruzada_sin_huecos"], [])                 # tampoco un aviso de «sin huecos» de la orden que ya no se ve

    def test_los_mensajes_de_sin_huecos_dicen_contra_que_chocaron(self):
        self.assertTrue(any("antes de la salida" in t for t in self.o["sug_sin_huecos_entrada"]))
        self.assertTrue(any("próximos 30 días" in t for t in self.o["sug_sin_huecos_salida"]))

    def test_la_salida_sin_tecnico_no_busca(self):
        self.assertEqual(self.o["sug_sin_tecnico"]["busquedas"], 0)
        self.assertTrue(any("técnico de la desinstalación" in t for t in self.o["sug_sin_tecnico"]["toasts"]))

    def test_alternar_rapido_no_deja_la_linea_de_tiempo_en_el_dia_de_la_otra_orden(self):
        self.assertEqual(self.o["aplicar_descartado"], [])


@unittest.skipIf(not NODE, "Node no está instalado")
class TestHuecosConLaOtraOrden(unittest.TestCase):
    """Hallazgo 5: el hueco que ocupa la otra orden cuenta como ocupado."""

    @classmethod
    def setUpClass(cls):
        piezas = _piezas("_o2mCalKey", "_o2mDayToMin", "_o2mDayHHMM", "_o2mDayCalcularHueco", "_o2mDayBuscarHuecoRango")
        cls.o = _node(r"""
const O2M_DAY_MINDUR = 30;
const _o2mDayHoyStr = function () { return '2026-10-24'; };
const o2mCal = { cache: { '2026-10': { '2026-10-27': [{ tecnico_id: 11, hora_inicio: '13:00', hora_fin: '14:00', estado: 'programada' }] } } };
const _o2mDayAsegurarMesCache = async function () {};
""" + piezas + r"""
(async function () {
  const out = {};
  out.sin_lim = _o2mDayCalcularHueco('2026-10-27', 120, [11]);                                   // 09:00–11:00 libre
  out.salida_mismo_dia = _o2mDayCalcularHueco('2026-10-27', 120, [11], { fecha: '2026-10-27', antes: 13 * 60 });   // la entrada termina 13:00 y a esa hora ya hay otra visita (13:00–14:00)
  out.salida_otro_dia = _o2mDayCalcularHueco('2026-10-27', 120, [11], { fecha: '2026-10-28', antes: 13 * 60 });    // el límite es de OTRO día: no cuenta hoy
  out.entrada_corta = _o2mDayCalcularHueco('2026-10-27', 60, [11], { fecha: '2026-10-27', despues: 10 * 60 });     // debe terminar antes de las 10:00
  out.entrada_larga = _o2mDayCalcularHueco('2026-10-27', 120, [11], { fecha: '2026-10-27', despues: 10 * 60 });    // 2 h no caben antes de las 10:00 si parte 09:00
  out.rango_con_lim = await _o2mDayBuscarHuecoRango('2026-10-27', 240, [11], 5, { fecha: '2026-10-27', antes: 12 * 60 }, null);
  out.rango_con_tope = await _o2mDayBuscarHuecoRango('2026-10-27', 600, [11], 5, null, '2026-10-27');           // 10 h no caben ningún día; el tope corta en el 27
  out.rango_pasa_tope = await _o2mDayBuscarHuecoRango('2026-10-28', 60, [11], 5, null, '2026-10-27');           // arrancar después del tope no devuelve nada
  console.log(JSON.stringify(out));
})();
""")

    def test_la_salida_del_mismo_dia_no_parte_antes_de_que_termine_la_entrada(self):
        self.assertEqual(self.o["sin_lim"], "09:00")
        self.assertEqual(self.o["salida_mismo_dia"], "14:00")                  # 12:00 ya no (la entrada termina a las 13:00); 13:00–14:00 está ocupado
        self.assertEqual(self.o["salida_otro_dia"], "09:00")

    def test_la_entrada_del_dia_de_la_salida_termina_antes_de_que_ella_empiece(self):
        self.assertEqual(self.o["entrada_corta"], "09:00")
        self.assertIsNone(self.o["entrada_larga"])

    def test_el_rango_aplica_el_limite_y_el_tope(self):
        self.assertEqual(self.o["rango_con_lim"]["fecha"], "2026-10-27")
        self.assertEqual(self.o["rango_con_lim"]["hora"], "14:00")             # 4 h desde las 12:00 chocan con la visita de las 13:00; la primera libre es 14:00
        self.assertIsNone(self.o["rango_con_tope"])
        self.assertIsNone(self.o["rango_pasa_tope"])


@unittest.skipIf(not NODE, "Node no está instalado")
class TestChoqueDeTecnicos(unittest.TestCase):
    """Hallazgos 6 y 8: el choque de la entrada no reaparece en la salida, y el de un técnico que no es líder se explica en la caja única."""

    @classmethod
    def setUpClass(cls):
        piezas = _piezas(*ACCESORES, "o2mChequearChoque", "_diAvisosHtml", "_diEsc")
        cls.o = _node(BASE_S + r"""
const DI_COD_FECHA = {FECHA_REQUERIDA:1, DESINST_FECHA_REQUERIDA:1, FECHA_INVALIDA:1, HORA_INVALIDA:1, DOMINGO_BLOQUEADO:1, DESINST_ANTES_DE_INSTALACION:1};
let _o2mChoqueToken = 0, trace = [], respuestas = [];
const esc = function (s) { return String(s).replace(/[&<>"']/g, function (c) { return {'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'}[c]; }); };
const cacheTec = [{ id: 11, nombre: 'Lenin Urbina' }, { id: 12, nombre: 'Jaizer Mora' }];
const o2mDay = { fecha: null, choque: false, choqueFecha: null, choqueKeys: null, choqueMsgs: null };
const warn = { style: { display: 'none' }, innerHTML: '' };
global.document = { getElementById: function (id) { return id === 'o2mChoqueWarn' ? warn : null; } };
const o2mDayAplicarChoque = function () { trace.push('aplicarChoque'); };
const _o2mAgAvisosSalida = function () { trace.push('avisosSalida'); };
const _o2mDayVisitaKey = function (v) { return 'v' + v.visita_id; };
const _o2mDayHoy = null;
global.URLSearchParams = URLSearchParams;
const fetch = function (url) {
  return new Promise(function (resolve) {
    respuestas.push(function () {
      const tid = Number(/tecnico_id=(\d+)/.exec(url)[1]);
      resolve({ ok: true, json: function () { return Promise.resolve({ tecnicos: [{ tecnico_id: tid, tecnico_nombre: tid === 11 ? 'Lenin Urbina' : 'Jaizer Mora', choque: true,
        visitas_choque: [{ visita_id: 5, numero_ot: 'OT-2026-00100', hora_inicio: '09:00', hora_fin: '10:00' }] }] }); } });
    });
  });
};
""" + piezas + r"""
const tick = function () { return new Promise(function (r) { setTimeout(r, 5); }); };
const out = {};
(async function () {
  // 6) la petición de la ENTRADA va en vuelo y se cambia a una SALIDA sin fecha: la respuesta vieja no pinta nada
  S.agOrden = 'ent'; S.fecha = '2026-10-27'; respuestas = []; trace = [];
  o2mChequearChoque();
  S.agOrden = 'sal'; S.desinst.fecha = '';
  o2mChequearChoque();                                              // rama temprana: sin fecha de salida
  respuestas.forEach(function (f) { f(); }); await tick(); await tick();
  out.vieja = { warn: warn.style.display, choque: o2mDay.choque, msgs: o2mDay.choqueMsgs, token: _o2mChoqueToken };

  // 8) la salida con un técnico que NO es el líder: el servidor no lo ve; el calendario sí → mensajes guardados y la caja única los muestra
  S.agOrden = 'sal'; S.desinst.fecha = '2026-11-12'; S.desinst.tec_modo = 'otros'; S.desinst.tecnicos = [12]; S.desinst.lider = 11; respuestas = []; trace = [];
  o2mChequearChoque();
  respuestas.forEach(function (f) { f(); }); await tick(); await tick();
  out.salida = { warn: warn.style.display, choque: o2mDay.choque, msgs: (o2mDay.choqueMsgs || []).length, fecha: o2mDay.choqueFecha, trace: trace.slice() };
  S.desinst.previo = { ok: true, avisos: [], choque: null, bloqueo: null };
  out.caja_sin_choque_del_servidor = _diAvisosHtml('fecha');
  S.desinst.previo = { ok: true, avisos: [], choque: { tecnico_nombre: 'Lenin Urbina', numero_ot: 'OT-9', puede_forzar: false }, bloqueo: null };
  out.caja_con_choque_del_servidor = _diAvisosHtml('fecha');
  S.desinst.previo = null; o2mDay.choqueFecha = '2026-11-30';
  out.caja_de_otra_fecha = _diAvisosHtml('fecha');                  // mensajes de OTRA fecha ya no valen

  // la entrada sigue mostrando su aviso propio en #o2mChoqueWarn
  S.agOrden = 'ent'; S.fecha = '2026-10-27'; S.tecnicos = [11]; respuestas = []; trace = [];
  o2mChequearChoque();
  respuestas.forEach(function (f) { f(); }); await tick(); await tick();
  out.entrada = { warn: warn.style.display, html: warn.innerHTML, avisosSalida: trace.indexOf('avisosSalida') >= 0 };
  console.log(JSON.stringify(out));
})();
""")

    def test_la_respuesta_vieja_de_la_entrada_no_pinta_nada_en_la_salida(self):
        v = self.o["vieja"]
        self.assertEqual(v["warn"], "none")
        self.assertFalse(v["choque"])
        self.assertIsNone(v["msgs"])

    def test_el_choque_de_otro_tecnico_de_la_salida_se_explica_en_la_caja_unica(self):
        s = self.o["salida"]
        self.assertTrue(s["choque"])
        self.assertEqual((s["msgs"], s["fecha"], s["warn"]), (1, "2026-11-12", "none"))   # el aviso del calendario no se repite en la salida…
        self.assertIn("avisosSalida", s["trace"])                                           # …sino que repinta la caja única
        h = self.o["caja_sin_choque_del_servidor"]
        self.assertIn("Jaizer Mora", h)
        self.assertIn("ya tiene una visita agendada", h)
        self.assertIn("Buscar hueco libre", h)
        self.assertIn("o2m-di-av ambar", h)                                                 # aviso, no bloqueo

    def test_si_el_servidor_ya_explica_el_choque_no_se_duplica(self):
        h = self.o["caja_con_choque_del_servidor"]
        self.assertIn("ya tiene trabajo agendado ese día en ese horario", h)
        self.assertNotIn("ya tiene una visita agendada", h)

    def test_los_mensajes_de_otra_fecha_no_se_muestran(self):
        self.assertNotIn("ya tiene una visita agendada", self.o["caja_de_otra_fecha"])

    def test_la_entrada_conserva_su_aviso_propio(self):
        e = self.o["entrada"]
        self.assertEqual(e["warn"], "")
        self.assertIn("Lenin Urbina", e["html"])
        self.assertIn("Buscar hueco libre", e["html"])
        self.assertFalse(e["avisosSalida"])


@unittest.skipIf(not NODE, "Node no está instalado")
class TestLaSalidaNoDiceListaSiElServidorLaBloquea(unittest.TestCase):
    """Hallazgo 4: fichas, guía y check del paso dicen lo mismo que el servidor."""

    @classmethod
    def setUpClass(cls):
        piezas = _piezas(*ACCESORES, "_diFechaSalidaOk", "_diSalidaChoca", "_diSalidaBloqueada", "_diSalidaListaParaAgendar", "_diSalidaMotivo", "_o2mAgResumenHtml",
                         "_o2mAgGuiaHtml", "_agAtajosHtml", "_diEsc", "_diFechaLarga", "_diDOW", "_diFechaCorta", "_diDM", "_diDiasEntre", "_diSumarDias", "_diSumarMeses", "_diHabil")
        cls.o = _node(BASE_S + r"""
const DI_COD_FECHA = {FECHA_REQUERIDA:1, DESINST_FECHA_REQUERIDA:1, FECHA_INVALIDA:1, HORA_INVALIDA:1, DOMINGO_BLOQUEADO:1, DESINST_ANTES_DE_INSTALACION:1};
const O2M_CAL_MES_ES = ['enero','febrero','marzo','abril','mayo','junio','julio','agosto','septiembre','octubre','noviembre','diciembre'];
const esc = function (s) { return String(s).replace(/[&<>"']/g, function (c) { return {'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'}[c]; }); };
const plano = function (h) { return h.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim(); };
""" + piezas + r"""
function estado() {
  const d = S.desinst, r = { lista: _diSalidaListaParaAgendar(), bloqueada: !!_diSalidaBloqueada(), motivo: _diSalidaMotivo() };
  const h = _o2mAgResumenHtml();
  const salida = h.split('class="o2m-ag-chip sal')[1] || '';
  r.ficha_salida = /^[^"]*\bfalta\b/.test(salida) ? 'falta' : (/^[^"]*\bok\b/.test(salida) ? 'ok' : '?');
  r.ficha_texto = plano(salida);
  S.agOrden = 'ent'; r.guia_entrada = plano(_o2mAgGuiaHtml());
  S.agOrden = 'sal'; r.guia_salida = plano(_o2mAgGuiaHtml());
  return r;
}
const BLOQ = { ok: false, valido: false, bloqueo: { codigo: 'DESINST_ANTES_DE_INSTALACION', mensaje: 'El mismo día, la desinstalación tiene que empezar después de que termine la instalación (13:00).' }, avisos: [], checklists_sugeridos: [] };
const out = {};
S.desinst.fecha = '2026-10-29'; S.fecha = '2026-10-29';
out.ok = estado();                                                                          // sin respuesta del servidor todavía: no se inventa un bloqueo
S.desinst.previo = BLOQ; out.bloqueada = estado();                                          // el servidor lo rechaza: ya no es «lista»
S.desinst.previoPend = true; out.revisando = estado(); S.desinst.previoPend = false;       // mientras revisa, es neutral
S.desinst.previo = { ok: false, valido: false, bloqueo: { codigo: 'PREVIO', mensaje: 'No se pudo revisar.' }, avisos: [] }; out.red = estado();   // un error del servidor NO es un bloqueo de fecha
S.desinst.previo = { ok: true, valido: true, bloqueo: null, avisos: [], choque: { tecnico_nombre: 'Lenin', numero_ot: 'OT-1' } }; out.choque = estado();
S.desinst.forzar = true; out.choque_forzado = estado(); S.desinst.forzar = false;
S.desinst.previo = null; S.desinst.fecha = ''; out.sin_fecha = estado();
S.desinst.fecha = '2026-10-26'; S.fecha = '2026-10-29'; out.antes = estado();
S.desinst.fecha = '2026-11-01'; S.fecha = '2026-10-29'; out.domingo = estado();           // 1/11/2026 es domingo
console.log(JSON.stringify(out));
""")

    def test_sin_respuesta_del_servidor_no_se_inventa_un_bloqueo(self):
        o = self.o["ok"]
        self.assertTrue(o["lista"])
        self.assertEqual(o["ficha_salida"], "ok")
        self.assertIn("Salida lista", o["guia_salida"])

    def test_si_el_servidor_la_bloquea_la_ficha_la_guia_y_el_check_lo_dicen(self):
        b = self.o["bloqueada"]
        self.assertFalse(b["lista"])
        self.assertTrue(b["bloqueada"])
        self.assertEqual(b["ficha_salida"], "falta")
        self.assertIn("tiene que empezar después de que termine la instalación", b["ficha_texto"])   # el motivo COMPLETO, sin cortar
        self.assertIn("revisa", b["ficha_texto"].lower() + " revisa")
        self.assertNotIn("Salida lista", b["guia_salida"])
        self.assertIn("tiene que empezar después de que termine la instalación", b["motivo"])

    def test_con_la_entrada_activa_la_guia_no_dice_agenda_la_salida_si_ya_existe(self):
        g = self.o["bloqueada"]["guia_entrada"]
        self.assertNotIn("Ahora agenda la salida", g)
        self.assertIn("necesita un cambio", g)
        self.assertIn("Revisar la salida", g)

    def test_mientras_el_servidor_revisa_es_neutral(self):
        self.assertTrue(self.o["revisando"]["lista"])

    def test_un_error_del_servidor_no_es_un_bloqueo_de_fecha(self):
        self.assertTrue(self.o["red"]["lista"])
        self.assertFalse(self.o["red"]["bloqueada"])

    def test_el_choque_del_lider_tampoco_es_lista_salvo_agendar_igual(self):
        self.assertFalse(self.o["choque"]["lista"])
        self.assertEqual(self.o["choque"]["ficha_salida"], "falta")
        self.assertIn("el técnico ya tiene trabajo", self.o["choque"]["ficha_texto"])
        self.assertIn("necesita un cambio", self.o["choque"]["guia_entrada"])
        self.assertTrue(self.o["choque_forzado"]["lista"])

    def test_sin_fecha_antes_de_la_entrada_y_domingo_se_dice_en_palabras(self):
        self.assertEqual(self.o["sin_fecha"]["motivo"], "Falta la fecha de salida")
        self.assertEqual(self.o["antes"]["motivo"], "Va antes de la instalación")
        self.assertEqual(self.o["domingo"]["motivo"], "Domingo: elige otro día")
        self.assertIn("Entrada lista", self.o["sin_fecha"]["guia_entrada"])                # sin salida todavía sí corresponde «ahora agenda la salida»

    def test_el_check_del_paso_agenda_usa_la_misma_regla(self):
        self.assertIn("_diSalidaListaParaAgendar()", _funcion("completo"))


@unittest.skipIf(not NODE, "Node no está instalado")
class TestClicEnUnHuecoYFoco(unittest.TestCase):
    """Hallazgo 7 (el camino sin «con desinstalación» queda idéntico) y 9 (el foco no se pierde al repintar las fichas)."""

    @classmethod
    def setUpClass(cls):
        piezas = _piezas(*ACCESORES, "o2mDaySlotClick", "_agPintarConFoco")
        cls.o = _node(BASE_S + r"""
let horas = [], clics = 0, _o2mFlashToken = 0;
const O2M_DAY_PXMIN = 0.8;
const o2mDay = { fecha: '2026-10-22' };                                  // la línea de tiempo mira un día que NO se puede agendar (pasado)
const o2mCalClicDia = function () { clics++; };                          // lo rechaza: no cambia la fecha de la orden
const setHora = function (c, v) { horas.push(c + '=' + v); };
const canvas = { getBoundingClientRect: function () { return { top: 0 }; } };
global.document = { getElementById: function (id) { return id === 'o2mDayCanvas' ? canvas : null; } };
const ev = { clientY: 240, target: { closest: function () { return null; } } };
""" + piezas + r"""
const out = {};
S.agOrden = 'ent';
o2mDaySlotClick(ev); out.con_tarjeta = { horas: horas.slice(), clics: clics };
horas = []; clics = 0; S.conDesinst = false;
o2mDaySlotClick(ev); out.sin_tarjeta = { horas: horas.slice(), clics: clics };

// _agPintarConFoco: el foco vuelve al botón que ocupa el mismo lugar
function caja(n) {
  const c = { botones: [], contains: function (x) { return c.botones.indexOf(x) >= 0; }, querySelectorAll: function () { return c.botones; },
    set innerHTML(h) { c.html = h; c.botones = []; for (let i = 0; i < n; i++) c.botones.push({ i: i, focus: function () { global.document.activeElement = this; } }); }, get innerHTML() { return c.html; } };
  c.innerHTML = 'x'; return c;
}
const c1 = caja(3); global.document.activeElement = c1.botones[1];
_agPintarConFoco(c1, 'nuevo'); out.foco = { indice: global.document.activeElement.i, es_nuevo: c1.botones.indexOf(global.document.activeElement) >= 0 };
const c2 = caja(3); global.document.activeElement = { i: 99 };               // el foco estaba en OTRO lado: no se roba
_agPintarConFoco(c2, 'nuevo'); out.sin_foco = global.document.activeElement.i;
console.log(JSON.stringify(out));
""")

    def test_con_la_tarjeta_nueva_un_clic_en_un_dia_no_agendable_no_toca_las_horas(self):
        self.assertEqual(self.o["con_tarjeta"]["horas"], [])
        self.assertEqual(self.o["con_tarjeta"]["clics"], 1)

    def test_sin_la_tarjeta_nueva_el_clic_aplica_las_horas_como_siempre(self):
        self.assertEqual(self.o["sin_tarjeta"]["horas"], ["hini=13:00", "hfin=17:00"])       # el hueco tocado (y=240 px → 13:00) con la duración de la orden (4 h)
        self.assertEqual(self.o["sin_tarjeta"]["clics"], 1)

    def test_el_foco_vuelve_al_boton_del_mismo_lugar(self):
        self.assertEqual(self.o["foco"], {"indice": 1, "es_nuevo": True})
        self.assertEqual(self.o["sin_foco"], 99)


class TestEstilosYAccesibilidad(unittest.TestCase):
    """Hallazgos 1, 9, 10 y 12 (CSS y marcado)."""

    def test_la_caja_de_aviso_no_deja_que_el_boton_aplaste_el_texto(self):
        self.assertRegex(CSS, r"\.o2m-di-av\{display:flex;flex-wrap:wrap;gap:9px;")
        self.assertIn(".o2m-di-av>span{flex:1 1 220px;min-width:0}", CSS)
        self.assertRegex(CSS, r"@media \(max-width:640px\)\{\.o2m-di-av button\{flex:1 1 140px;margin-left:0;")
        self.assertIn("overflow-wrap:anywhere", CSS.split(".o2m-di-av{")[1].split("}")[0])      # REGLA #15: nada se corta

    def test_el_choque_de_la_salida_envuelve_los_botones_dentro_de_la_caja(self):
        h = _funcion("_diAvisosHtml")
        self.assertIn("Buscar hueco libre", h)
        self.assertIn("<span><b>'+_diEsc(c.tecnico_nombre", h)                                   # el texto vive en un <span> (flex:1 1 220px), los botones aparte

    def test_la_barra_pegajosa_es_solida_y_no_tapa_el_foco(self):
        regla = CSS.split(".o2m-ag-resumen{position:sticky;")[1].split("}")[0]
        self.assertIn("background:#fff", regla)
        self.assertNotIn("backdrop-filter", CSS)
        self.assertIn(".o2m-body:has(.o2m-ag-resumen){scroll-padding-top:116px}", CSS)

    def test_en_celular_todo_lo_que_se_toca_mide_44_con_las_dos_ordenes(self):
        m = CSS.split(".o2m-ag-resumen{top:-16px}")[1].split("@media (max-width:720px)")[0]
        for regla in (".o2m-ag-di .o2m-cal-navbtn{width:44px;height:44px}", ".o2m-ag-di .o2m-cal-hoy{min-height:44px;",
                      ".o2m-ag-di .o2m-day-navd button{width:44px;height:44px}", ".o2m-ag-di .o2m-day-sugerir-btn{min-height:44px;",
                      ".o2m-ag-di input.form-control{min-height:44px}"):
            self.assertIn(regla, m)
        f = _funcion("pintarAgenda")
        self.assertIn("b.classList.toggle('o2m-ag-di', !!di);", f)                               # sin la tarjeta nueva la clase no existe: el paso queda como siempre

    def test_la_guia_se_anuncia_por_una_region_fija_y_no_por_los_atajos(self):
        f = _funcion("pintarAgenda")
        self.assertNotIn('id="o2mAgGuia" aria-live', f)
        self.assertRegex(f, r'id="o2mAgLive" role="status" aria-live="polite" aria-atomic="true"')
        self.assertIn("lv.textContent", _funcion("_o2mAgResumenPintar"))


@unittest.skipIf(not NODE, "Node no está instalado")
class TestScriptValido(unittest.TestCase):
    def test_script_valido(self):
        import os
        import subprocess
        import tempfile
        bloques = re.findall(r"<script(?![^>]*src)[^>]*>(.*?)</script>", JS, re.S)
        b = re.sub(r"\{#.*?#\}", "", bloques[0], flags=re.S)
        b = re.sub(r"\{\{.*?\}\}", "0", b, flags=re.S)
        b = re.sub(r"\{%.*?%\}", "", b, flags=re.S)
        with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as f:
            f.write(b)
        r = subprocess.run([NODE, "--check", f.name], capture_output=True, text=True)
        os.unlink(f.name)
        self.assertEqual(r.returncode, 0, r.stderr[:300])


if __name__ == "__main__":
    unittest.main()
