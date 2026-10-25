# -*- coding: utf-8 -*-
"""2026-10-25 · Paso «Agenda» del asistente «Nueva OT» con «Instalación con desinstalación» (Daniel, probando en producción: «reciclar el mismo
calendario… para ahorrar código y texto lo usamos el mismo calendario… hazlo potente, amigable, con pocos pasos, muy poco scroll… y que este
comportamiento solamente ocurra si escoges instalación con desinstalación»).

La SALIDA ya no tiene calendario propio (el calendario compacto oscuro de la noche del 23-oct desapareció): se agenda en el MISMO calendario de la
entrada (mes + línea de tiempo del día) con un interruptor «Entrada | Salida» (S.agOrden). Estas pruebas EJECUTAN el JavaScript real de la
plantilla en Node (con un documento de mentira) para ver que:
  · la entrada y la salida escriben cada una SU estado (S.* y S.desinst.*) y nunca el de la otra;
  · el calendario marca las dos fechas y los días de en medio, y la salida no admite días anteriores a la entrada;
  · la instalación a secas (sin la tarjeta nueva) queda exactamente igual que siempre.
"""
import json
import os
import re
import shutil
import subprocess
import tempfile
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NODE = shutil.which("node")


def _leer(*partes):
    with open(os.path.join(RAIZ, *partes), encoding="utf-8") as fh:
        return fh.read().replace("\r\n", "\n")


JS = _leer("templates", "ot2", "_modal_crear.html")
CSS = _leer("static", "ot_evento.css")


def _funcion(nombre):
    """Texto completo de `function nombre(...) {...}` (emparejando llaves) de la plantilla; con su `async` si lo lleva."""
    i = JS.index("function " + nombre + "(")
    if JS[max(0, i - 6):i] == "async ":
        i -= 6
    nivel, pos = 0, JS.index("{", i)
    for n in range(pos, len(JS)):
        if JS[n] == "{":
            nivel += 1
        elif JS[n] == "}":
            nivel -= 1
            if nivel == 0:
                return JS[i:n + 1]
    raise AssertionError("no cierra: " + nombre)


def _node(guion):
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as f:
        f.write(guion)
        ruta = f.name
    try:
        r = subprocess.run([NODE, ruta], capture_output=True, text=True, encoding="utf-8", timeout=60)
    finally:
        os.unlink(ruta)
    assert r.returncode == 0, r.stderr[-1500:]
    return json.loads(r.stdout.strip().splitlines()[-1])


class TestYaNoHayCalendarioCompactoDeLaSalida(unittest.TestCase):
    """Se ahorra código: el calendario compacto de la salida y todo lo que solo él usaba salieron (REGLA #4.2: Daniel pidió expresamente reciclar el
    mismo calendario; no se pierde ninguna función: fecha, horas, atajos, hueco libre, feriados y choque viven ahora en el calendario único)."""

    def test_las_funciones_del_calendario_compacto_ya_no_existen(self):
        for resto in ("_diRegFecha", "_diCalGrid", "_diCalCargar", "_diCalInicio", "_diAtajos(", "_diDurTxt", "_o2mAgSalidaHtml", "_o2mAgSalidaPintar",
                      "function diCalMes", "function diBuscarHueco", "function diFecha", "o2mDiFecha", "o2mDiHini", "o2mDiHfin", "o2mDiHuecoBtn", "agIr",
                      "diFecha:diFecha", "diCalMes:diCalMes", "diBuscarHueco:diBuscarHueco"):
            self.assertNotIn(resto, JS, resto)

    def test_los_estilos_del_calendario_compacto_tampoco(self):
        for clase in (".o2m-dc", ".dc-d", ".dc-side", ".dc-big", ".dc-hrs", ".dc-at", ".dc-ley", "o2m-ag-salida", "o2m-ag-union", "o2m-ag-lbl"):
            self.assertNotIn(clase, CSS, clase)

    def test_hay_un_solo_calendario_en_el_paso_agenda(self):
        self.assertEqual(JS.count('id="o2mCalGrid"'), 1)
        self.assertEqual(JS.count('id="o2mDayCanvas"'), 1)
        self.assertEqual(JS.count('id="o2mFecha"'), 1)

    def test_el_paso_desinstalacion_ya_no_tiene_contenedor_de_fecha(self):
        f = _funcion("pintarDesinst")
        self.assertNotIn("o2mDiFecha", f)
        self.assertIn('<div id="o2mDiAvisosDes"></div>', f)
        self.assertEqual(JS.count('id="o2mDiAvisosDes"'), 1)
        # y el único contenedor de avisos de la fecha de la salida vive en la Agenda (solo con la tarjeta nueva)
        self.assertEqual(JS.count("'<div id=\"o2mDiAvisos\"></div>'"), 1)
        self.assertIn("(di ? '<div id=\"o2mDiAvisos\"></div>' : '')", _funcion("pintarAgenda"))


class TestAccesoresDeLaOrdenQueSeAgenda(unittest.TestCase):
    """El calendario lee y escribe la orden que dice el interruptor: la ENTRADA en S.*, la SALIDA en S.desinst.* (lo que viaja en body.desinstalacion)."""

    @classmethod
    @unittest.skipIf(not NODE, "Node no está instalado")
    def setUpClass(cls):
        piezas = "\n".join(_funcion(n) for n in ("_agSal", "agFecha", "agHini", "agHfin", "agTecnicos", "agLider", "_agDueno", "agMinFecha", "_diTec", "_diLider"))
        cls.o = _node(r"""
const S = { agOrden: 'ent', tipo: 'instalacion', conDesinst: true, cliente: {id: 1}, fecha: '2026-10-27', hini: '09:00', hfin: '12:00', tecnicos: [11, 12], lider: 11,
  desinst: { fecha: '2026-11-12', hini: '14:00', hfin: '16:00', tec_modo: 'mismos', tecnicos: [], lider: null } };
const O2M_ES_TECNICO = false;
const esInterno = function () { return false; };
const _o2mHoyStr = function () { return '2026-10-24'; };
function _o2mDiAplica() { return S.tipo === 'instalacion' && !!S.conDesinst && !esInterno() && !O2M_ES_TECNICO && !!S.cliente; }
""" + piezas + r"""
function ver() { return { sal: _agSal(), fecha: agFecha(), hini: agHini(), hfin: agHfin(), tec: agTecnicos(), lider: agLider(), min: agMinFecha(), dueno: _agDueno() === S ? 'S' : 'S.desinst' }; }
const out = {};
out.entrada = ver();
S.agOrden = 'sal'; out.salida_mismos = ver();
S.desinst.tec_modo = 'otros'; S.desinst.tecnicos = [21, 22]; S.desinst.lider = 22; out.salida_otros = ver();
S.fecha = '2026-10-20'; out.entrada_ya_paso = ver();          // la salida nunca admite antes de HOY, aunque la entrada sea vieja
S.fecha = '2026-10-27';
S.conDesinst = false; out.sin_tarjeta_nueva = ver();          // 'sal' sin la tarjeta nueva = la instalación a secas
S.conDesinst = true; S.cliente = null; out.sin_cliente = ver();
S.cliente = {id: 1};
// agTecnicos devuelve una COPIA: tocarla no cambia a los técnicos de la OT
S.agOrden = 'ent'; const t = agTecnicos(); t.push(99); out.copia = S.tecnicos.length;
console.log(JSON.stringify(out));
""")

    def test_la_entrada_lee_su_propio_estado(self):
        e = self.o["entrada"]
        self.assertEqual((e["sal"], e["fecha"], e["hini"], e["hfin"], e["tec"], e["lider"], e["dueno"]),
                         (False, "2026-10-27", "09:00", "12:00", [11, 12], 11, "S"))
        self.assertEqual(e["min"], "2026-10-24")                    # hoy

    def test_la_salida_lee_el_estado_de_la_desinstalacion(self):
        s = self.o["salida_mismos"]
        self.assertEqual((s["sal"], s["fecha"], s["hini"], s["hfin"], s["dueno"]), (True, "2026-11-12", "14:00", "16:00", "S.desinst"))
        self.assertEqual((s["tec"], s["lider"]), ([11, 12], 11))      # «los mismos de la instalación»
        self.assertEqual(s["min"], "2026-10-27")                      # no puede ir antes de la entrada

    def test_la_salida_con_otros_tecnicos_usa_los_suyos(self):
        s = self.o["salida_otros"]
        self.assertEqual((s["tec"], s["lider"]), ([21, 22], 22))

    def test_la_salida_nunca_admite_antes_de_hoy(self):
        self.assertEqual(self.o["entrada_ya_paso"]["min"], "2026-10-24")

    def test_sin_la_tarjeta_nueva_el_interruptor_no_hace_nada(self):
        s = self.o["sin_tarjeta_nueva"]
        self.assertEqual((s["sal"], s["fecha"], s["dueno"]), (False, "2026-10-27", "S"))
        self.assertFalse(self.o["sin_cliente"]["sal"])

    def test_los_tecnicos_se_entregan_en_copia(self):
        self.assertEqual(self.o["copia"], 2)


class TestElCalendarioSigueALaOrdenQueSeAgenda(unittest.TestCase):
    """Nada del calendario lee ya la entrada «a mano»: todo pasa por los accesores (si no, la salida editaría la fecha de la entrada)."""

    def test_las_funciones_del_calendario_no_leen_el_estado_de_la_entrada_directo(self):
        def codigo(nombre):                      # sin los comentarios (que sí mencionan S.lider para explicar el porqué)
            return re.sub(r"//[^\n]*", "", _funcion(nombre))
        for nombre in ("o2mCalInit", "o2mDaySlotClick", "o2mDayRenderMine", "_o2mDayMineDragStart", "o2mDayPaso", "o2mChequearChoque",
                       "_o2mDayCarriles", "_o2mDayPintarLeyendaCarriles", "o2mDaySugerirHueco", "o2mDayRender"):
            self.assertNotRegex(codigo(nombre), r"\bS\.(hini|hfin|tecnicos|lider)\b", nombre)
        # La fecha: las que quedan son a propósito (la salida no puede ir antes de la entrada, o el fantasma de la otra orden)
        self.assertNotIn("S.fecha", codigo("o2mDayRenderMine"))
        self.assertNotIn("S.fecha ||", codigo("o2mChequearChoque"))
        self.assertIn("agFecha()", _funcion("o2mChequearChoque"))
        self.assertIn("agTecnicos()", _funcion("o2mChequearChoque"))

    def test_el_calendario_de_entrada_conserva_su_choque_pero_la_salida_no_lo_repite(self):
        f = _funcion("o2mChequearChoque")
        self.assertIn("if (mensajes.length && !sal){", f)             # el aviso del calendario solo en la entrada
        self.assertIn("o2mDayAplicarChoque();", f)                    # los bloques en choque se pintan siempre
        self.assertIn("OT2C.o2mDaySugerirHueco()", _funcion("_diAvisosHtml"))   # la salida ofrece «Buscar hueco libre» en SU aviso

    def test_buscar_hueco_para_la_salida(self):
        f = _funcion("o2mDaySugerirHueco")
        self.assertIn("var tecnicoIds = agTecnicos();", f)
        self.assertIn("var maxDias = sal ? 30 : 14;", f)               # la desinstalación suele ir más lejos que 14 días
        self.assertIn("fechaBase < S.fecha", f)                        # nunca antes de la entrada
        self.assertIn("Elige primero el técnico de la desinstalación", f)

    def test_ir_a_un_dia_escribe_la_fecha_de_la_orden_que_se_agenda(self):
        # (el comportamiento se EJECUTA en tests/test_ot_agenda_unica_revision.py; acá solo la forma)
        f = _funcion("_o2mDayIrADia")
        self.assertIn("S.desinst.fecha = fecha;", f)
        self.assertIn("if (orden !== 'sal') _o2mAnexoSincronizarFechas();", f)   # el anexo toma la fecha de la ENTRADA

    def test_si_el_dia_que_se_mira_no_se_puede_agendar_no_se_tocan_las_horas(self):
        f = _funcion("o2mDaySlotClick")
        # solo con la tarjeta nueva: sin ella el clic en un hueco sigue como siempre (revisión independiente 25-oct, hallazgo 7)
        self.assertIn("if (_o2mDiAplica() && o2mDay.fecha !== agFecha()) return;", f)

    def test_cambiar_los_tecnicos_de_la_salida_repinta_el_calendario(self):
        self.assertIn("_agRefrescarTecnicosSalida();", _funcion("diTecTog"))
        self.assertIn("_agRefrescarTecnicosSalida();", _funcion("diTecModo"))
        self.assertIn("if (!_agSal() || !document.getElementById('o2mDayCanvas')) return;", _funcion("_agRefrescarTecnicosSalida"))


@unittest.skipIf(not NODE, "Node no está instalado")
class TestSetFechaYSetHoraEscribenLaOrdenCorrecta(unittest.TestCase):
    """Se ejecutan setFecha() y setHora() de verdad, con todo lo demás de mentira, para ver QUÉ estado escriben y qué llaman."""

    @classmethod
    def setUpClass(cls):
        piezas = "\n".join(_funcion(n) for n in ("_agSal", "agFecha", "agHini", "agHfin", "_agDueno", "setFecha", "setHora", "_o2mDayToMin", "_o2mDayHHMM"))
        cls.o = _node(r"""
const S = { agOrden: 'ent', tipo: 'instalacion', conDesinst: true, cliente: {id: 1}, fecha: '2026-10-27', hini: '09:00', hfin: '12:00', tecnicos: [11], lider: 11,
  desinst: { fecha: '2026-11-12', hini: '14:00', hfin: '16:00', tec_modo: 'mismos', tecnicos: [], lider: null } };
const O2M_ES_TECNICO = false, O2M_SCHED_LIMITE_ABS = '22:00', O2M_DAY_END = 22 * 60;
const esInterno = function () { return false; };
let llamadas = [];
const marca = function (n) { return function () { llamadas.push(n); }; };
const avisosAgenda = marca('avisosAgenda'), refrescarAnclas = marca('refrescarAnclas'), _o2mAnexoSincronizarFechas = marca('anexo'),
  _diPintarRegiones = marca('diRegiones'), _diProgramarPrevio = marca('diPrevio'), o2mChequearChoqueDebounced = marca('choque'),
  o2mDayRenderMine = marca('mine'), _o2mDayAutoScroll = marca('autoscroll'), _o2mCostoInternoAutoCalc = marca('costoInterno'),
  o2mCalRenderGrid = marca('grid'), o2mDayRender = marca('dayRender'), o2mCalSelDia = marca('selDia');
const o2mCal = { anio: 2026, mes: 10, diaSel: null };
const o2mCalCargarMes = function () { llamadas.push('cargarMes'); return Promise.resolve(); };
const _o2mHoyStr = function () { return '2026-10-24'; };
const _o2mFechaBonita = function (f) { return f; };
const _o2mEsDomingo = function (f) { return new Date(f + 'T12:00:00').getDay() === 0; };
const toasts = []; const ilusToast = function (m) { toasts.push(m); }; global.window = { ilusToast: ilusToast };
const inputs = { o2mFecha: { value: '' }, o2mHoraIni: { value: '' }, o2mHoraFin: { value: '' } };
global.document = { getElementById: function (id) { return inputs[id] || null; } };
function _o2mDiAplica() { return S.tipo === 'instalacion' && !!S.conDesinst && !esInterno() && !O2M_ES_TECNICO && !!S.cliente; }
""" + piezas + r"""
const out = {};
function caso(nombre, f) { llamadas = []; toasts.length = 0; f(); out[nombre] = { llamadas: llamadas.slice(), toasts: toasts.slice(), fecha: S.fecha, salida: S.desinst.fecha,
  hini: S.hini, hfin: S.hfin, shini: S.desinst.hini, shfin: S.desinst.hfin, topeE: !!S._hfinTopado, topeS: !!S.desinst._hfinTopado, input: inputs.o2mFecha.value,
  ihini: inputs.o2mHoraIni.value, ihfin: inputs.o2mHoraFin.value }; }
caso('entrada_fecha', function () { setFecha('2026-10-29'); });
S.agOrden = 'sal';
caso('salida_fecha', function () { setFecha('2026-11-20'); });
caso('salida_domingo', function () { setFecha('2026-11-29'); });
caso('salida_antes_de_la_entrada', function () { setFecha('2026-10-26'); });
// escrita a mano, año por año (el navegador avisa cada año parcial): nunca se borra lo que se tipea ni se pide un mes absurdo al servidor
inputs.o2mFecha.value = '0002-11-12';
caso('salida_año_parcial', function () { setFecha('0002-11-12'); });
// el mismo día que la entrada (termina 12:00): la salida con 14:00–16:00 ya sirve y no se toca; con 09:00–13:00 se sugiere el horario que sí sirve
S.desinst.hini = '14:00'; S.desinst.hfin = '16:00';
caso('salida_mismo_dia_que_ya_sirve', function () { setFecha('2026-10-29'); });
S.desinst.hini = '09:00'; S.desinst.hfin = '13:00';
caso('salida_mismo_dia_choca', function () { setFecha('2026-10-29'); });
S.desinst.fecha = '2026-11-20'; S.desinst.hini = '14:00'; S.desinst.hfin = '16:00';
caso('salida_vaciar', function () { setFecha(''); });
caso('salida_hora', function () { setHora('hini', '10:30'); });
caso('salida_tope_22', function () { setHora('hfin', '23:30'); });
S.agOrden = 'ent';
caso('entrada_hora', function () { setHora('hfin', '15:00'); });
caso('entrada_tope_22', function () { setHora('hfin', '23:30'); });
console.log(JSON.stringify(out));
""")

    def test_la_fecha_de_la_entrada_va_a_S_fecha_y_sincroniza_el_anexo(self):
        e = self.o["entrada_fecha"]
        self.assertEqual((e["fecha"], e["salida"]), ("2026-10-29", "2026-11-12"))
        self.assertIn("anexo", e["llamadas"])
        self.assertNotIn("diPrevio", e["llamadas"])

    def test_la_fecha_de_la_salida_va_solo_a_la_desinstalacion_y_se_revisa_con_el_servidor(self):
        s = self.o["salida_fecha"]
        self.assertEqual((s["fecha"], s["salida"]), ("2026-10-29", "2026-11-20"))   # la entrada NO se movió
        self.assertIn("diPrevio", s["llamadas"])
        self.assertIn("diRegiones", s["llamadas"])
        self.assertNotIn("anexo", s["llamadas"])                                     # el anexo toma la fecha de la entrada
        self.assertIn("cargarMes", s["llamadas"])                                    # el calendario salta al mes de la salida (noviembre)

    def test_la_salida_no_acepta_domingo_y_un_dia_antes_de_la_entrada_se_avisa_sin_borrar(self):
        d = self.o["salida_domingo"]
        self.assertEqual(d["salida"], "2026-11-20")                                  # quedó la de antes
        self.assertTrue(any("domingos" in t for t in d["toasts"]))
        # Revisión independiente 25-oct, hallazgo 3: escrita a mano la fecha NO se rechaza ni se borra el campo (la ficha «Salida» queda en rojo y el
        # servidor la rechaza igual); un clic o un atajo ni llegan acá (o2mCalClicDia ya los rechazó). Se avisa con un toast porque ya es una fecha completa.
        a = self.o["salida_antes_de_la_entrada"]
        self.assertEqual(a["salida"], "2026-10-26")
        self.assertTrue(any("no puede ir antes de la instalación" in t for t in a["toasts"]))
        self.assertIn("diPrevio", a["llamadas"])                                     # el servidor la revisa y dice el motivo

    def test_tipear_la_fecha_de_la_salida_no_la_borra_en_cada_digito(self):
        p = self.o["salida_año_parcial"]
        self.assertEqual(p["salida"], "0002-11-12")                                  # el valor se acepta mientras se escribe el año…
        self.assertEqual(p["input"], "0002-11-12")                                   # …y el campo NO se reasigna (antes se reseteaba y no dejaba escribir)
        self.assertEqual(p["toasts"], [])                                            # sin toast mientras la fecha es un año parcial
        self.assertNotIn("cargarMes", p["llamadas"])                                 # ni se le pide al servidor el calendario del año 2
        self.assertNotIn("selDia", p["llamadas"])

    def test_el_mismo_dia_de_la_entrada_la_salida_empieza_cuando_ella_termina(self):
        ok = self.o["salida_mismo_dia_que_ya_sirve"]
        self.assertEqual((ok["shini"], ok["shfin"]), ("14:00", "16:00"))             # ya empezaba después del término (12:00): no se toca
        self.assertEqual(ok["toasts"], [])
        c = self.o["salida_mismo_dia_choca"]
        self.assertEqual(c["salida"], "2026-10-29")
        self.assertEqual((c["shini"], c["shfin"]), ("12:00", "16:00"))               # empieza cuando termina la entrada y conserva su duración (4 h)
        self.assertEqual((c["ihini"], c["ihfin"]), ("12:00", "16:00"))               # y los casilleros lo muestran
        self.assertEqual((c["hini"], c["hfin"]), ("09:00", "12:00"))                 # la ENTRADA no se tocó
        self.assertTrue(any("empieza a las 12:00" in t for t in c["toasts"]))        # se dice: el sistema sugiere, la persona decide

    def test_vaciar_la_fecha_de_la_salida_la_deja_vacia_sin_tocar_la_entrada(self):
        v = self.o["salida_vaciar"]
        self.assertEqual((v["fecha"], v["salida"]), ("2026-10-29", ""))

    def test_las_horas_de_la_salida_van_a_la_desinstalacion(self):
        h = self.o["salida_hora"]
        self.assertEqual((h["hini"], h["shini"]), ("09:00", "10:30"))                # la entrada NO cambió
        self.assertIn("diPrevio", h["llamadas"])
        self.assertIn("mine", h["llamadas"])                                         # el bloque de la orden se mueve en vivo

    def test_el_tope_de_las_22_es_de_cada_orden(self):
        s = self.o["salida_tope_22"]
        self.assertEqual((s["shfin"], s["topeS"], s["topeE"]), ("22:00", True, False))
        e = self.o["entrada_tope_22"]
        self.assertEqual((e["hfin"], e["topeE"]), ("22:00", True))
        self.assertTrue(e["topeS"])                                                   # el de la salida sigue siendo suyo, no se pisó

    def test_las_horas_de_la_entrada_siguen_escribiendo_S(self):
        e = self.o["entrada_hora"]
        self.assertEqual((e["hfin"], e["shfin"]), ("15:00", "22:00"))
        # Revisión independiente 25-oct, hallazgo 4: la regla del servidor para «el mismo día» depende de cuándo TERMINA la entrada, así que
        # cambiar sus horas también vuelve a revisar la salida (sin repintar las regiones de la salida: su caja de avisos no está a la vista).
        self.assertIn("diPrevio", e["llamadas"])
        self.assertNotIn("diRegiones", e["llamadas"])


@unittest.skipIf(not NODE, "Node no está instalado")
class TestElCalendarioMarcaLasDosFechas(unittest.TestCase):
    """o2mCalRenderGrid() de verdad: entrada y salida marcadas, franja «equipos instalados» de en medio, y la salida sin días anteriores a la entrada."""

    @classmethod
    def setUpClass(cls):
        piezas = "\n".join(_funcion(n) for n in ("_agSal", "agFecha", "agMinFecha", "_o2mCalKey", "o2mCalRenderGrid"))
        cls.o = _node(r"""
const S = { agOrden: 'ent', tipo: 'instalacion', conDesinst: true, cliente: {id: 1}, fecha: '2026-10-27', hini: '09:00', hfin: '12:00',
  desinst: { fecha: '2026-11-12', hini: '09:00', hfin: '13:00' } };
const O2M_ES_TECNICO = false, O2M_CAL_UMBRAL_AMBAR = 3, O2M_CAL_UMBRAL_ROJO = 5;
const esInterno = function () { return false; };
const esc = function (s) { return String(s).replace(/[&<>"']/g, function (c) { return {'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'}[c]; }); };
const _o2mHoyStr = function () { return '2026-10-24'; };
const _o2mFaseColor = function () { return '#3b82f6'; };
function _o2mDiAplica() { return S.tipo === 'instalacion' && !!S.conDesinst && !esInterno() && !O2M_ES_TECNICO && !!S.cliente; }
const o2mCal = { anio: 2026, mes: 11, cache: { '2026-10': {}, '2026-11': {} }, fer: { '2026-11': { '2026-11-01': 'Día de Todos los Santos' } }, diaSel: null };
const grid = { innerHTML: '' };
global.document = { getElementById: function (id) { return id === 'o2mCalGrid' ? grid : null; } };
""" + piezas + r"""
// OJO: el reloj de «hoy» dentro de o2mCalRenderGrid es new Date(); se fija para que 2026-10-24 sea hoy
const RealDate = Date; global.Date = class extends RealDate { constructor(...a) { if (a.length) super(...a); else super(2026, 9, 24, 10, 0, 0); } };
function dias(mes) {
  o2mCal.mes = mes; o2mCalRenderGrid();
  const mapa = {};
  grid.innerHTML.split('<div class="o2m-cal-day').slice(1).forEach(function (h) {
    const cl = /^([^"]*)"/.exec(h)[1].trim(); const n = /<span class="num">(\d+)<\/span>/.exec(h);
    if (!n) return;
    mapa[+n[1]] = { cl: cl, desactivado: /aria-disabled="true"/.test(h), onclick: /onclick=/.test(h), marcas: (h.match(/mk-o/g) || []).length, ent: /bi-box-arrow-in-down/.test(h), sal: /bi-box-arrow-up/.test(h), aria: (/aria-label="([^"]*)"/.exec(h) || [])[1] };
  });
  return mapa;
}
const out = {};
out.ent_oct = dias(10); out.ent_nov = dias(11);
S.agOrden = 'sal'; out.sal_oct = dias(10); out.sal_nov = dias(11);
S.desinst.fecha = ''; out.sin_salida_nov = dias(11);
S.conDesinst = false; S.agOrden = 'ent'; S.desinst.fecha = '2026-11-12'; out.simple_oct = dias(10); out.simple_nov = dias(11);
console.log(JSON.stringify(out));
""")

    def test_el_dia_de_la_entrada_y_el_de_la_salida_llevan_su_marca(self):
        self.assertIn("m-ent", self.o["ent_oct"]["27"]["cl"])
        self.assertTrue(self.o["ent_oct"]["27"]["ent"])
        self.assertIn("día de la entrada", self.o["ent_oct"]["27"]["aria"])
        self.assertIn("m-sal", self.o["ent_nov"]["12"]["cl"])
        self.assertTrue(self.o["ent_nov"]["12"]["sal"])
        self.assertIn("día de la salida", self.o["ent_nov"]["12"]["aria"])

    def test_el_dia_que_se_agenda_es_el_seleccionado(self):
        self.assertIn(" sel", " " + self.o["ent_oct"]["27"]["cl"])          # entrada activa: el 27/10 es el negro de siempre
        self.assertNotIn(" sel", " " + self.o["ent_nov"]["12"]["cl"])
        self.assertIn(" sel", " " + self.o["sal_nov"]["12"]["cl"])          # salida activa: el 12/11
        self.assertNotIn(" sel", " " + self.o["sal_oct"]["27"]["cl"])

    def test_los_dias_de_en_medio_llevan_la_franja_de_equipos_instalados(self):
        for d in ("28", "29", "30", "31"):
            self.assertIn("rango", self.o["ent_oct"][d]["cl"].split(), d)
        for d in ("2", "3", "11"):
            self.assertIn("rango", self.o["ent_nov"][d]["cl"].split(), d)
        for d in ("27",):
            self.assertNotIn("rango", self.o["ent_oct"][d]["cl"].split())        # los extremos tienen su propia marca
        self.assertNotIn("rango", self.o["ent_nov"]["12"]["cl"].split())
        self.assertNotIn("rango", self.o["ent_nov"]["13"]["cl"].split())          # después de la salida ya no hay equipos

    def test_la_salida_no_admite_dias_anteriores_a_la_entrada(self):
        for d in ("24", "25", "26"):                                           # hoy (24/10) y los días hasta la entrada (27/10)
            c = self.o["sal_oct"][d]
            self.assertTrue(c["desactivado"], d)
            self.assertFalse(c["onclick"], d)
            self.assertIn("antes", c["cl"].split(), d)
        self.assertIn("antes de la instalación", self.o["sal_oct"]["26"]["aria"])
        self.assertIn("pasado", self.o["sal_oct"]["23"]["cl"].split())         # ayer: pasado a secas (no «antes de la entrada»)
        self.assertNotIn("antes", self.o["sal_oct"]["23"]["cl"].split())
        self.assertTrue(self.o["sal_oct"]["27"]["onclick"])                    # el mismo día de la entrada sí se puede
        self.assertTrue(self.o["sal_oct"]["28"]["onclick"])

    def test_la_entrada_si_admite_cualquier_dia_desde_hoy(self):
        for d in ("24", "26"):
            self.assertTrue(self.o["ent_oct"][d]["onclick"], d)
            self.assertNotIn("antes", self.o["ent_oct"][d]["cl"].split(), d)
        self.assertTrue(self.o["ent_oct"]["25"]["desactivado"])               # 25/10/2026 es domingo: no se agenda
        self.assertFalse(self.o["ent_oct"]["23"]["onclick"])                  # ayer ya pasó

    def test_el_domingo_sigue_sin_poderse_agendar(self):
        self.assertTrue(self.o["sal_nov"]["29"]["desactivado"])                # 29/11/2026 es domingo
        self.assertIn("domingo", self.o["sal_nov"]["29"]["cl"].split())

    def test_sin_fecha_de_salida_no_hay_marca_ni_franja(self):
        n = self.o["sin_salida_nov"]
        self.assertTrue(all("m-sal" not in c["cl"] and "rango" not in c["cl"].split() for c in n.values()))

    def test_la_instalacion_a_secas_queda_identica_sin_marcas(self):
        for mes in ("simple_oct", "simple_nov"):
            for d, c in self.o[mes].items():
                self.assertEqual(c["marcas"], 0, (mes, d))
                self.assertNotRegex(c["cl"], r"m-ent|m-sal|rango|antes")

    def test_el_feriado_se_sigue_marcando(self):
        self.assertIn("feriado", self.o["ent_nov"]["1"]["cl"].split())


class TestEstilosDelCalendarioUnico(unittest.TestCase):
    def test_marcas_y_franja_del_calendario(self):
        for pista in (".o2m-cal-day.m-ent:not(.sel){", ".o2m-cal-day.m-sal:not(.sel){", ".o2m-cal-day .mk-o{", ".o2m-cal-day.rango::after{",
                      ".o2m-day-blk.otra{", ".o2m-day-blk.otra .ot-tag{", ".o2m-day-legend i.k.otra{"):
            self.assertIn(pista, JS, pista)
        self.assertIn("pointer-events:none", JS[JS.index(".o2m-day-blk.otra{"):][:260])        # el fantasma nunca roba el clic

    def test_el_interruptor_es_pegajoso_y_no_corta_nada(self):
        self.assertRegex(CSS, r"\.o2m-ag-resumen\{position:sticky;top:-22px;z-index:30;")
        self.assertIn("overflow-wrap:anywhere", CSS.split(".o2m-ag-chip .tx em{")[1].split("}")[0])      # REGLA #15: ningún dato se corta
        self.assertIn(".o2m-ag-chip.on.ok,.o2m-ag-chip.on.falta{border-color:#0a0a0a;", CSS)         # la orden que se agenda: borde negro

    def test_en_celular_es_una_fila_de_dos_fichas_cortas(self):
        self.assertIn(".o2m-ag-resumen{top:-16px}", CSS)                     # el relleno de .o2m-body en celular
        m = CSS[CSS.index(".o2m-ag-resumen{grid-template-columns:1fr 1fr;") - 40:][:900]
        self.assertIn(".o2m-ag-resumen{grid-template-columns:1fr 1fr;", m)
        self.assertIn(".o2m-ag-chip .lg{display:none}", m)
        self.assertIn(".o2m-ag-chip .sh{display:inline}", m)

    def test_los_botones_miden_44_en_celular_y_menos_en_escritorio(self):
        self.assertRegex(CSS, r"\.o2m-ag-go\{min-height:44px")
        self.assertRegex(CSS, r"\.o2m-ag-at\{min-height:44px")
        self.assertRegex(CSS, r"@media \(min-width:641px\)\{\.o2m-ag-at\{min-height:32px\}\}")

    def test_los_atajos_se_deslizan_dentro_de_su_caja_en_celular(self):
        m = CSS[CSS.index(".o2m-ag-resumen{top:-16px}") - 40:][:700]
        self.assertIn(".o2m-ag-atajos{flex-wrap:nowrap;overflow-x:auto;", m)

    def test_las_horas_van_lado_a_lado_en_celular_solo_con_las_dos_ordenes(self):
        f = _funcion("pintarAgenda")
        self.assertEqual(f.count("(di ? 'col-6 ' : '')+'col-md-4\"><label"), 2)
        self.assertIn("'<div class=\"row '+(di ? 'g-2' : 'g-3')+'\">'", f)


@unittest.skipIf(not NODE, "Node no está instalado")
class TestScriptValido(unittest.TestCase):
    def test_script_valido(self):
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
