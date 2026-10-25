# -*- coding: utf-8 -*-
"""2026-10-24 · Daniel: «una tarjeta que diga instalación con desinstalación… el tipo de OT decide el comportamiento»; «si el usuario la usa es
porque es un evento: abrir una OT de entrada (instalar) y una de salida (desinstalar)… sí o sí un técnico»; «las pruebas pasan de rojo a verde…
en el proyecto entero, cuando la tarjeta apruebe los caracteres, tiene que pasar de rojo a verde».
"""
import json
import os
import re
import shutil
import subprocess
import tempfile
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
with open(os.path.join(RAIZ, "templates", "ot2", "_modal_crear.html"), encoding="utf-8") as _fh:
    M = _fh.read()


class TestTarjetaInstalacionConDesinstalacion(unittest.TestCase):
    def test_la_tarjeta_existe_y_es_la_misma_instalacion_en_la_base(self):
        self.assertIn("n:'Instalación con desinstalación'", M)
        self.assertIn("conDesinst:true}", M)
        i = M.index("n:'Instalación con desinstalación'")
        self.assertIn("{v:'instalacion',", M[i - 40:i])           # el servidor sigue recibiendo tipo_ot='instalacion'
        self.assertLess(M.index("n:'Instalación',            i:'bi-tools'"), i, "la instalación a secas va primero (abrir() la encuentra primero)")

    def test_el_estado_y_la_seleccion_distinguen_las_dos_tarjetas(self):
        self.assertIn("conDesinst:false, cliente:null", M)
        self.assertIn("!!S.conDesinst === !!t.conDesinst);", M)
        self.assertIn("S.conDesinst = !!t.conDesinst;", M)
        self.assertIn("if (S.conDesinst && S.desinst){ S.desinst.lleva = 'si'; _diAutoCosto(); }", M)

    def test_el_paso_de_desinstalacion_solo_existe_con_la_tarjeta_nueva(self):
        self.assertIn("function _o2mDiAplica(){ return S.tipo === 'instalacion' && !!S.conDesinst && !esInterno() && !O2M_ES_TECNICO && !!S.cliente; }", M)

    def test_el_origen_cliente_sigue_sin_ofrecer_instalacion(self):
        self.assertIn("if (origen === 'cliente' && t.v === 'instalacion') return false;", M)

    def test_el_paso_dice_entrada_y_salida_y_pide_tecnico(self):
        self.assertIn("evento o arriendo", M)
        self.assertIn("<b>de entrada</b> (instalación)", M)
        self.assertIn("<b>de salida</b> (desinstalación)", M)
        self.assertIn("sí o sí, un técnico asignado", M)

    def test_con_la_tarjeta_ya_no_se_pregunta_si_se_desinstala(self):
        i = M.index("function _diRegPreg(el){")
        f = M[i:i + 3200]
        self.assertIn("if (S.conDesinst){", f)
        self.assertLess(f.index("if (S.conDesinst){"), f.index("o2m-di-pregunta"))
        self.assertIn("return;", f[f.index("if (S.conDesinst){"):f.index("o2m-di-pregunta")])

    def test_el_tecnico_sigue_siendo_obligatorio(self):
        i = M.index("function _diCompleto(){")
        self.assertIn("if (!_diLider() || !_diTec().length) return false;", M[i:i + 900])

    def test_abrir_desde_otros_modulos_respeta_la_variante(self):
        self.assertIn("!!x.conDesinst === !!opts.conDesinst", M)
        self.assertIn("S.conDesinst = !!t.conDesinst;\n        if (S.conDesinst && S.desinst) S.desinst.lleva = 'si';", M)


with open(os.path.join(RAIZ, "static", "ot_evento.css"), encoding="utf-8") as _fh:
    CSS_EV = _fh.read()


def _fn(nombre, largo=3000):
    i = M.index("function " + nombre)
    return M[i:i + largo]


class TestAgendaEntradaYSalida(unittest.TestCase):
    """Daniel (24-oct, de noche): «que el modal se comporte en términos de calendario para crear dos órdenes de trabajo». 25-oct, tras probarlo en
    producción: «reciclar el mismo calendario… para ahorrar código y texto lo usamos el mismo calendario». El paso Agenda agenda LAS DOS órdenes
    con UN solo calendario y un interruptor «Entrada | Salida» (la lógica a fondo se ejecuta en tests/test_ot_desinst_calendario.py)."""

    def test_el_paso_agenda_agrega_el_interruptor_solo_con_la_tarjeta_nueva(self):
        f = _fn("pintarAgenda(b){", 12000)
        self.assertIn("var di = _o2mDiAplica();", f)
        self.assertIn("if (!di) S.agOrden = 'ent';", f)
        self.assertIn("(di ? ' · entrada y salida' : '')", f)
        self.assertIn('<div class="o2m-ag-resumen" id="o2mAgResumen" role="group"', f)
        self.assertIn('id="o2mAgGuia"', f)
        self.assertIn("if (di){ _agResUlt = ''; _agGuiaUlt = ''; _o2mAgResumenPintar(); _o2mAgAvisosSalida(); _diProgramarPrevio(true); }", f)
        # La instalación a secas queda IGUAL: mismo párrafo, mismo pliegue, mismo rótulo y sin bloque de la otra orden.
        self.assertIn("Toca un espacio libre o arrastra el bloque «Tu OT» (mouse o dedo) para fijar el horario", f)
        self.assertIn("<summary>Puedes salirte de la jornada</summary>", f)
        self.assertIn("(di ? 'Fecha de la entrada *' : 'Fecha programada *')", f)
        self.assertIn("(di ? '<div class=\"o2m-day-blk otra\" id=\"o2mDayOtra\" style=\"display:none\"></div>' : '')", f)

    def test_las_dos_ordenes_se_rotulan_entrada_y_salida_en_el_interruptor(self):
        f = _fn("_o2mAgResumenHtml(){", 3600)
        self.assertIn("'Entrada · instalación', 'Entrada'", f)
        self.assertIn("'Salida · desinstalación', 'Salida'", f)
        self.assertIn("onclick=\"OT2C.agOrden(\\''+cual+'\\')\"", f)
        # el técnico y el checklist de la salida siguen en su paso: la guía lo dice
        self.assertIn("Su técnico y checklist van en el paso «Desinstalación».", _fn("_o2mAgGuiaHtml(){", 3600))

    def test_la_fecha_de_la_salida_ya_no_tiene_contenedor_propio(self):
        f = _fn("pintarAgenda(b){", 12000)
        self.assertNotIn("o2mDiFecha", f)
        self.assertNotIn("_o2mAgSalidaHtml", f)
        # En el paso «Desinstalación» solo queda la región donde se explica lo que NO es de la fecha (técnico, costo, cobro, saldo).
        self.assertIn('<div id="o2mDiAvisosDes"></div><div id="o2mDiTec"></div>', M)
        self.assertEqual(M.count('id="o2mDiAvisosDes"'), 1)
        self.assertEqual(M.count('id="o2mDiAvisos"'), 1, "el de la fecha y el choque de la salida: uno, en la Agenda, solo con la tarjeta nueva")

    def test_pintar_los_avisos_de_la_salida_no_depende_de_que_exista_el_paso_desinstalacion(self):
        # Al pintar el formulario completo la Agenda va ANTES: _diPintarRegiones se sale si no existe o2mDiPreg, así que se pinta directo.
        f = _fn("_o2mAgAvisosSalida(){", 400)
        self.assertIn("_diRegAvisos(a)", f)
        self.assertNotIn("_diPintarRegiones", f)

    def test_la_agenda_exige_tambien_la_fecha_de_la_salida(self):
        # (25-oct, revisión independiente hallazgo 4) «lista» = fecha válida + sin bloqueo del servidor + sin choque; los casos se EJECUTAN en
        # tests/test_ot_agenda_unica_revision.py
        self.assertIn("case 'agenda':   return !!S.fecha && S.fecha >= _o2mHoyStr() && (!_o2mDiAplica() || _diSalidaListaParaAgendar());", M)
        self.assertIn("function _diSalidaListaParaAgendar(){ return _diFechaSalidaOk() && !_diSalidaBloqueada() && !_diSalidaChoca(); }", M)
        f = _fn("_diFechaSalidaOk(){", 300)
        self.assertIn("d.fecha >= (S.fecha || '')", f)
        self.assertIn("!_o2mEsDomingo(d.fecha)", f)

    def test_el_paso_desinstalacion_apunta_a_la_agenda_y_se_mantiene_al_dia(self):
        i = M.index("function _diRegPreg(el){")
        f = M[i:i + 3600]
        self.assertIn("que agendas en el paso <b>Agenda</b>", f)
        # «Ir a Agenda» deja elegida la SALIDA (es lo que falta) y baja al paso
        self.assertIn("OT2C.agOrden(\\'sal\\');OT2C.irAncla(\\'agenda\\')", f)
        # El aviso «necesita un técnico» y las fechas de las dos órdenes se repintan al cambiar técnicos o la fecha de la instalación.
        self.assertIn("var regs = ['preg', 'tec', 'res'];", M)
        self.assertIn("_diPintarRegiones(['preg', 'avisos', 'res']);", M)          # una fecha de salida nueva (setFecha / _o2mDayIrADia)
        self.assertEqual(M.count("_diPintarRegiones(['preg', 'tec', 'costo', 'res']);"), 2)       # diTecModo y diTecTog
        self.assertNotIn("'fecha', 'avisos'", M)                                                  # la región «fecha» ya no existe

    def test_estilos_del_interruptor_la_guia_y_los_atajos(self):
        for pista in (".o2m-ag-mas{", ".o2m-ag-resumen{", ".o2m-ag-chip{", ".o2m-ag-chip.on.ok,.o2m-ag-chip.on.falta{", ".o2m-ag-guia{", ".o2m-ag-go{",
                      ".o2m-ag-atajos{", ".o2m-ag-at{", ".o2m-ag-at.sug{"):
            self.assertIn(pista, CSS_EV)
        for viejo in (".o2m-ag-lbl{", ".o2m-ag-salida{", ".o2m-ag-union{"):
            self.assertNotIn(viejo, CSS_EV)

    def test_la_salida_no_se_pide_a_un_tecnico_ni_en_trabajo_interno(self):
        self.assertIn("!esInterno() && !O2M_ES_TECNICO && !!S.cliente", M[M.index("function _o2mDiAplica(){"):][:300])


NODE = shutil.which("node")


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


@unittest.skipIf(not NODE, "Node no está instalado")
class TestUnTecnicoNoTieneLaTarjeta(unittest.TestCase):
    """Revisión independiente (24-oct): la tarjeta se mostraba también a un técnico, pero la desinstalación (costos, cobro, saldo) no existe para
    él (REGLA #26) y el servidor le exige no ver esa información: elegirla creaba UNA sola orden aunque creyera estar creando dos (REGLA #32)."""

    @classmethod
    def setUpClass(cls):
        i = M.index("  var TIPOS = [")
        tipos = M[i:M.index("  var ORIGENES", i)]
        j = M.index("  function tipoValidoParaOrigen(t, origen){")
        valida = M[j:M.index("  function tiposParaOrigen(){", j)]
        cls.o = {}
        for etiqueta, flag in (("tecnico", "true"), ("gestion", "false")):
            cls.o[etiqueta] = _node("const O2M_ES_TECNICO = %s;\n%s\n%s\n" % (flag, tipos, valida) + """
const out = {};
['ticket', 'cotizacion', 'documento', 'cliente', 'interno'].forEach(function (o) {
  out[o] = TIPOS.filter(function (t) { return tipoValidoParaOrigen(t, o); }).map(function (t) { return t.n; });
});
console.log(JSON.stringify(out));
""")

    def test_gestion_la_ve_en_los_origenes_con_instalacion(self):
        for origen in ("ticket", "cotizacion", "documento"):
            self.assertIn("Instalación con desinstalación", self.o["gestion"][origen], origen)
            self.assertIn("Instalación", self.o["gestion"][origen], origen)
        self.assertNotIn("Instalación con desinstalación", self.o["gestion"]["cliente"])         # como siempre: sin instalaciones desde un cliente
        self.assertNotIn("Instalación con desinstalación", self.o["gestion"]["interno"])

    def test_el_tecnico_no_la_ve_en_ningun_origen_y_conserva_todo_lo_demas(self):
        for origen, nombres in self.o["tecnico"].items():
            self.assertNotIn("Instalación con desinstalación", nombres, origen)
        for origen in ("ticket", "cotizacion", "documento"):
            self.assertEqual([n for n in self.o["gestion"][origen] if n != "Instalación con desinstalación"], self.o["tecnico"][origen], origen)

    def test_abrir_desde_otro_modulo_tampoco_se_la_da(self):
        i = M.index("function abrir(opts){")
        f = M[i:i + 5200]
        self.assertIn("if (t && t.conDesinst && O2M_ES_TECNICO) t = null;", f)
        self.assertLess(f.index("var t = TIPOS.find("), f.index("if (t && t.conDesinst && O2M_ES_TECNICO) t = null;"))
        self.assertLess(f.index("if (t && t.conDesinst && O2M_ES_TECNICO) t = null;"), f.index("S.tipo = t.v;"))


@unittest.skipIf(not NODE, "Node no está instalado")
class TestAvisosRepartidosEntreAgendaYDesinstalacion(unittest.TestCase):
    """Revisión independiente (24-oct): con la tarjeta nueva el paso Agenda mostraba TODOS los avisos del servidor, incluidos los de pasos que
    todavía no se completan («Indica cuánto le pagamos…» en rojo, sin ningún campo para corregirlo ahí). Ahora la fecha y el choque del técnico
    se explican en Agenda, y lo demás en el paso Desinstalación, junto al dato que lo resuelve."""

    @classmethod
    def setUpClass(cls):
        i = M.index("  var DI_COD_FECHA = ")
        codigo = M[i:M.index("  function _diRegTec(el){", i)]
        cls.o = _node(r"""
const contenedores = {};
global.document = { getElementById: function (id) { return contenedores[id] || null; } };
const S = { agOrden: 'sal', fecha: '2026-10-27', desinst: { lleva: 'si', fecha: '2026-11-12', previo: null, previoPend: false, forzar: false } };
const _diEsc = function (s) { return String(s); };
const _o2mFechaBonita = function (s) { return s; };
const _o2mEsDomingo = function (f) { return new Date(f + 'T12:00:00').getDay() === 0; };
const _agSal = function () { return S.agOrden === 'sal'; };      // (la real también exige la tarjeta nueva: aquí siempre la hay)
const o2mDay = { choque: false, choqueFecha: null, choqueMsgs: null };      // el aviso de choque del calendario (lo llena o2mChequearChoque)
""" + codigo + r"""
function txt(h) { return h.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim(); }
const out = {};
// 1) El servidor pide el costo (un dato del paso Desinstalación) y avisa un sábado y un choque (datos de la fecha)
S.desinst.previo = { bloqueo: { codigo: 'FINANZAS_SIN_COSTO_PROVEEDOR', mensaje: 'Indica cuánto le pagamos al técnico o proveedor por la desinstalación.' },
  avisos: ['Cae en sábado, cuando no hay operación.'], choque: { tecnico_nombre: 'Pedro', numero_ot: 'OT-2026-00123', puede_forzar: false },
  sin_documento_madre: true };
out.fecha = txt(_diAvisosHtml('fecha')); out.resto = txt(_diAvisosHtml('resto')); out.todo = txt(_diAvisosHtml('todo'));
// 2) Un bloqueo de la fecha va a Agenda, no a Desinstalación
S.desinst.previo = { bloqueo: { codigo: 'DESINST_ANTES_DE_INSTALACION', mensaje: 'La desinstalación no puede ir antes de la instalación.' }, avisos: [], choque: null };
S.desinst.fecha = '2026-11-12';
out.bloqueoFecha = { fecha: txt(_diAvisosHtml('fecha')), resto: txt(_diAvisosHtml('resto')) };
// 3) Revisando: se ve en las dos (el servidor está mirando todo)
S.desinst.previo = null; S.desinst.previoPend = true;
out.revisando = { fecha: txt(_diAvisosHtml('fecha')), resto: txt(_diAvisosHtml('resto')) };
S.desinst.previoPend = false;
// 4) _diRegAvisos pinta la región de Agenda con lo de la fecha (mientras se agenda la SALIDA) y la de Desinstalación con el resto (cuando existe)
S.desinst.previo = { bloqueo: { codigo: 'LECTURA_INCOMPLETA', mensaje: 'No pudimos leer en Random el saldo.' }, avisos: ['Cae en feriado.'], choque: null };
const ag = { innerHTML: '' }, de = { innerHTML: '' };
contenedores.o2mDiAvisosDes = de; _diRegAvisos(ag);
out.regiones = { agenda: txt(ag.innerHTML), desinstalacion: txt(de.innerHTML) };
// 4b) Con la ENTRADA en el calendario (25-oct: un calendario, una orden a la vez) la región de la fecha de la salida va vacía; el resto sigue en su paso
S.agOrden = 'ent'; _diRegAvisos(ag);
out.con_la_entrada = { agenda: ag.innerHTML, desinstalacion: txt(de.innerHTML) };
S.agOrden = 'sal';
// 5) Sin la región (la desinstalación clásica): todo junto, como siempre
delete contenedores.o2mDiAvisosDes; const solo = { innerHTML: '' }; _diRegAvisos(solo);
out.clasica = txt(solo.innerHTML);
// 6) «No se desinstala»: ambas regiones se vacían
S.desinst.lleva = 'no'; contenedores.o2mDiAvisosDes = de; _diRegAvisos(ag);
out.vacias = { agenda: ag.innerHTML, desinstalacion: de.innerHTML };
// 7) El choque de la salida ofrece «Buscar hueco libre» y, solo al superadministrador, «Agendar igual»
S.desinst.lleva = 'si'; S.desinst.previo = { avisos: [], choque: { tecnico_nombre: 'Pedro', numero_ot: 'OT-1', puede_forzar: false } };
out.choque_comun = txt(_diAvisosHtml('fecha')); out.choque_comun_botones = (_diAvisosHtml('fecha').match(/<button/g) || []).length;
S.desinst.previo.choque.puede_forzar = true; out.choque_super_botones = (_diAvisosHtml('fecha').match(/<button/g) || []).length;
console.log(JSON.stringify(out));
""")

    def test_agenda_solo_explica_la_fecha_y_el_choque_del_tecnico(self):
        f = self.o["fecha"]
        self.assertIn("Cae en sábado", f)
        self.assertIn("Pedro", f)
        self.assertNotIn("cuánto le pagamos", f)
        self.assertNotIn("sin documento de Random", f)

    def test_desinstalacion_explica_lo_que_se_resuelve_ahi(self):
        r = self.o["resto"]
        self.assertIn("Indica cuánto le pagamos", r)
        self.assertIn("sin documento de Random", r)
        self.assertNotIn("Cae en sábado", r)
        self.assertNotIn("Pedro", r)

    def test_la_desinstalacion_clasica_sigue_mostrando_todo_junto(self):
        t = self.o["todo"]
        for pista in ("Cae en sábado", "Pedro", "Indica cuánto le pagamos", "sin documento de Random"):
            self.assertIn(pista, t)

    def test_un_bloqueo_de_fecha_va_a_agenda(self):
        self.assertIn("no puede ir antes de la instalación", self.o["bloqueoFecha"]["fecha"])
        self.assertNotIn("no puede ir antes de la instalación", self.o["bloqueoFecha"]["resto"])

    def test_revisando_se_ve_en_las_dos(self):
        self.assertIn("Revisando fecha", self.o["revisando"]["fecha"])
        self.assertIn("Revisando fecha", self.o["revisando"]["resto"])

    def test_las_dos_regiones_se_pintan_juntas(self):
        r = self.o["regiones"]
        self.assertIn("Cae en feriado", r["agenda"])
        self.assertNotIn("No pudimos leer en Random", r["agenda"])
        self.assertIn("No pudimos leer en Random", r["desinstalacion"])
        self.assertNotIn("Cae en feriado", r["desinstalacion"])
        self.assertIn("Cae en feriado", self.o["clasica"])
        self.assertIn("No pudimos leer en Random", self.o["clasica"])

    def test_si_no_se_desinstala_se_vacian_las_dos(self):
        self.assertEqual(self.o["vacias"], {"agenda": "", "desinstalacion": ""})

    def test_con_la_entrada_en_el_calendario_no_se_mezclan_los_avisos_de_la_salida(self):
        c = self.o["con_la_entrada"]
        self.assertEqual(c["agenda"], "")
        self.assertIn("No pudimos leer en Random", c["desinstalacion"])        # lo demás sigue explicándose en su paso

    def test_el_choque_de_la_salida_ofrece_buscar_hueco_y_solo_al_superadmin_agendar_igual(self):
        self.assertIn("Pedro", self.o["choque_comun"])
        self.assertEqual(self.o["choque_comun_botones"], 1)                    # «Buscar hueco libre»
        self.assertEqual(self.o["choque_super_botones"], 2)                    # + «Agendar igual»

    def test_los_codigos_de_fecha_son_los_que_el_servidor_devuelve(self):
        with open(os.path.join(RAIZ, "ot_eventos.py"), encoding="utf-8") as fh:
            ev = fh.read()
        for codigo in ("FECHA_REQUERIDA", "FECHA_INVALIDA", "HORA_INVALIDA", "DOMINGO_BLOQUEADO", "DESINST_ANTES_DE_INSTALACION"):
            self.assertIn('"%s"' % codigo, ev)
            self.assertIn(codigo + ":1", M)

    def test_el_rellenado_directo_no_depende_de_que_exista_agenda(self):
        f = _fn("_diPintarRegiones(lista){", 1500)
        self.assertIn("document.getElementById('o2mDiAvisosDes')", f)
        self.assertIn("_diAvisosHtml('resto')", f)


def _funcion(nombre):
    """Texto completo de `function nombre(...) {...}` (emparejando llaves) de la plantilla."""
    i = M.index("function " + nombre + "(")
    nivel, pos = 0, M.index("{", i)
    for n in range(pos, len(M)):
        if M[n] == "{":
            nivel += 1
        elif M[n] == "}":
            nivel -= 1
            if nivel == 0:
                return M[i:n + 1]
    raise AssertionError("no cierra: " + nombre)


@unittest.skipIf(not NODE, "Node no está instalado")
class TestInterruptorDeLasDosOrdenes(unittest.TestCase):
    """Daniel (24-oct): «que el modal se comporte en términos de calendario para crear dos órdenes de trabajo». 25-oct: UN solo calendario. Arriba del
    paso Agenda, el interruptor «Entrada | Salida»: las fechas de las dos órdenes de un vistazo (verde si sirve, rojo con el motivo si no), cuántos
    días hay entre ellas, cuál se está agendando, la guía de qué sigue y los atajos de fecha de la salida."""

    @classmethod
    def setUpClass(cls):
        piezas = "\n".join(_funcion(n) for n in ("_o2mFechaBonita", "_diDOW", "_diFechaLarga", "_diDiasEntre", "_o2mEsDomingo", "_diFechaSalidaOk", "_diSalidaChoca",
                                                 "_diSalidaBloqueada", "_diSalidaListaParaAgendar", "_diSalidaMotivo",
                                                 "_agSal", "_diDM", "_diFechaCorta", "_diSumarDias", "_diSumarMeses", "_diHabil", "_agAtajosHtml",
                                                 "_o2mAgResumenHtml", "_o2mAgGuiaHtml"))
        cls.o = _node(r"""
const S = { agOrden: 'ent', tipo: 'instalacion', conDesinst: true, cliente: {id: 1}, fecha: '2026-10-27', hini: '09:00', hfin: '12:00',
  desinst: { fecha: '2026-11-12', hini: '09:00', hfin: '11:00', previo: null, previoPend: false, forzar: false } };
const DI_COD_FECHA = {FECHA_REQUERIDA:1, DESINST_FECHA_REQUERIDA:1, FECHA_INVALIDA:1, HORA_INVALIDA:1, DOMINGO_BLOQUEADO:1, DESINST_ANTES_DE_INSTALACION:1};
const O2M_ES_TECNICO = false, O2M_CAL_MES_ES = ['enero','febrero','marzo','abril','mayo','junio','julio','agosto','septiembre','octubre','noviembre','diciembre'];
const esInterno = function () { return false; };
const _o2mHoyStr = function () { return '2026-10-24'; };
const _diEsc = function (s) { return String(s).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; }); };
function _o2mDiAplica() { return S.tipo === 'instalacion' && !!S.conDesinst && !esInterno() && !O2M_ES_TECNICO && !!S.cliente; }
""" + piezas + r"""
function txt(h) { return h.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim(); }
function quitar(h) { return h.replace(/<[^>]+>/g, '').replace(/\s+/g, ' ').trim(); }      // sin tags y sin espacios de más (la puntuación queda pegada)
function ver(h) {
  const chips = h.split('<button').slice(1).map(function (b) {
    return { cl: /class="o2m-ag-chip ([^"]+)"/.exec(b)[1], largo: txt(/<em><span class="lg">(.*?)<\/span>/.exec(b)[1]), corto: txt(/<em><span class="lg">.*?<\/span><span class="sh">(.*?)<\/span><\/em>/.exec(b)[1]),
             titulo: txt(/<b><span class="lg">(.*?)<\/span>/.exec(b)[1]), tituloCorto: txt(/<span class="sh">(.*?)<\/span>(?:<span class="ed">|<\/b>)/.exec(b)[1]),
             aria: /aria-label="([^"]*)"/.exec(b)[1], ir: /agOrden\('(\w+)'\)/.exec(b)[1], pressed: /aria-pressed="(\w+)"/.exec(b)[1], editando: /class="ed"/.test(b) };
  });
  const gap = /<span class="o2m-ag-gap"[^>]*>(.*?)<\/span>/.exec(h)[1].replace(/<[^>]+>/g, '');
  return { chips: chips, gap: gap };
}
const out = {};
out.ambas = ver(_o2mAgResumenHtml());
S.agOrden = 'sal'; out.activaSalida = ver(_o2mAgResumenHtml()); S.agOrden = 'ent';
S.desinst.fecha = ''; out.sinSalida = ver(_o2mAgResumenHtml());
S.desinst.fecha = '2026-10-20'; out.antes = ver(_o2mAgResumenHtml());
S.desinst.fecha = '2026-11-15'; out.domingo = ver(_o2mAgResumenHtml());
S.desinst.fecha = '2026-10-27'; out.mismoDia = ver(_o2mAgResumenHtml());
S.desinst.fecha = '2026-10-28'; out.unDia = ver(_o2mAgResumenHtml());
S.fecha = '2026-10-20'; S.desinst.fecha = '2026-11-12'; out.entradaPasada = ver(_o2mAgResumenHtml());
S.fecha = ''; out.sinEntrada = ver(_o2mAgResumenHtml());
S.fecha = '2026-10-27';
// el servidor avisa que el técnico de la salida ya tiene trabajo (y nadie autorizó agendar igual): la ficha de la salida lo dice
S.desinst.previo = { choque: { tecnico_nombre: 'Pedro', puede_forzar: false } }; out.choque = ver(_o2mAgResumenHtml());
S.desinst.forzar = true; out.choqueForzado = ver(_o2mAgResumenHtml()); S.desinst.forzar = false; S.desinst.previo = null;
// guía
const g = {};
S.fecha = ''; S.desinst.fecha = ''; g.sinEntrada = quitar(_o2mAgGuiaHtml());
S.fecha = '2026-10-27'; g.entradaLista = _o2mAgGuiaHtml();
S.desinst.fecha = '2026-11-12'; g.ambasEnEntrada = _o2mAgGuiaHtml();
S.agOrden = 'sal'; g.salidaLista = _o2mAgGuiaHtml();
S.desinst.fecha = ''; g.salidaVacia = _o2mAgGuiaHtml();
S.fecha = ''; g.salidaSinEntrada = _o2mAgGuiaHtml();
S.fecha = '2026-10-27'; S.desinst.fecha = '2026-10-20'; g.salidaMala = quitar(_o2mAgGuiaHtml());
S.desinst.fecha = '2026-10-27'; g.mismoDia = quitar(_o2mAgGuiaHtml());
out.guia = g;
// atajos de la salida, contados desde la entrada (el domingo se corre al lunes)
S.desinst.fecha = '2026-11-03'; S.desinst.previo = { fecha_sugerida: '2026-11-20', fecha_sugerida_motivo: 'Propuesta: a los 24 días' };
out.atajos = _agAtajosHtml();
console.log(JSON.stringify(out));
""")

    def test_las_dos_fechas_listas_se_ven_verdes_con_los_dias_que_hay_entre_ellas(self):
        a = self.o["ambas"]
        self.assertEqual([c["cl"] for c in a["chips"]], ["ent on ok", "sal ok"])
        self.assertEqual([c["ir"] for c in a["chips"]], ["ent", "sal"])
        self.assertEqual(a["chips"][0]["largo"], "Martes 27/10/2026 · 09:00–12:00")
        self.assertEqual(a["chips"][1]["largo"], "Jueves 12/11/2026 · 09:00–11:00")
        self.assertEqual(a["gap"], "16 días")
        self.assertIn("Entrada · instalación: Martes 27/10/2026", a["chips"][0]["aria"])       # un lector de pantalla oye el estado

    def test_el_interruptor_marca_la_orden_que_se_agenda(self):
        a = self.o["ambas"]["chips"]
        self.assertEqual([c["pressed"] for c in a], ["true", "false"])
        self.assertEqual([c["editando"] for c in a], [True, False])
        self.assertIn("(la que estás agendando)", a[0]["aria"])
        self.assertNotIn("(la que estás agendando)", a[1]["aria"])
        s = self.o["activaSalida"]["chips"]
        self.assertEqual([c["cl"] for c in s], ["ent ok", "sal on ok"])
        self.assertEqual([c["pressed"] for c in s], ["false", "true"])

    def test_cada_ficha_trae_su_version_corta_para_el_celular(self):
        e, s = self.o["ambas"]["chips"]
        self.assertEqual((e["titulo"], e["tituloCorto"], e["corto"]), ("Entrada · instalación", "Entrada", "Mar 27 oct"))
        self.assertEqual((s["titulo"], s["tituloCorto"], s["corto"]), ("Salida · desinstalación", "Salida", "Jue 12 nov"))
        # lo que no es una fecha (un aviso) se ve completo también en celular: nada se corta
        self.assertEqual(self.o["sinSalida"]["chips"][1]["corto"], "Falta la fecha de salida")

    def test_lo_que_falta_se_ve_rojo_y_dice_por_que(self):
        self.assertEqual((self.o["sinSalida"]["chips"][1]["cl"], self.o["sinSalida"]["chips"][1]["largo"]), ("sal falta", "Falta la fecha de salida"))
        self.assertEqual(self.o["sinSalida"]["gap"], "")                                         # sin las dos fechas no hay «días entre»
        self.assertEqual(self.o["antes"]["chips"][1]["largo"], "Va antes de la instalación")
        self.assertEqual(self.o["domingo"]["chips"][1]["largo"], "Domingo: elige otro día")
        self.assertEqual(self.o["entradaPasada"]["chips"][0]["largo"], "La fecha ya pasó: elige hoy o una futura")
        self.assertEqual(self.o["entradaPasada"]["chips"][0]["cl"], "ent on falta")
        self.assertEqual((self.o["sinEntrada"]["chips"][0]["cl"], self.o["sinEntrada"]["chips"][0]["largo"]), ("ent on falta", "Falta la fecha"))

    def test_si_el_tecnico_de_la_salida_ya_tiene_trabajo_la_ficha_lo_dice(self):
        c = self.o["choque"]["chips"][1]
        self.assertEqual(c["cl"], "sal falta")
        self.assertIn("el técnico ya tiene trabajo", c["largo"])
        self.assertTrue(c["corto"].endswith("· choca"))
        self.assertEqual(self.o["choque"]["gap"], "16 días")                                      # la fecha en sí sirve: los días entre ellas se muestran
        self.assertEqual(self.o["choqueForzado"]["chips"][1]["cl"], "sal ok")                      # «Agendar igual» autorizado: ya no se marca

    def test_los_dias_entre_las_dos_fechas(self):
        self.assertEqual(self.o["mismoDia"]["gap"], "mismo día")
        self.assertEqual(self.o["unDia"]["gap"], "1 día")
        self.assertEqual(self.o["entradaPasada"]["gap"], "")                                      # con una fecha inválida no se afirma nada

    def test_la_guia_dice_que_sigue(self):
        g = self.o["guia"]
        self.assertIn("Toca en el calendario el día en que se instala", g["sinEntrada"])
        self.assertIn("Entrada lista. Ahora agenda la salida", txt_sin_html(g["entradaLista"]))
        self.assertIn("OT2C.agOrden('sal')", g["entradaLista"])                                    # un toque pasa a la salida
        self.assertIn("Ajustando la entrada. La salida ya está agendada (16 días después).", re.sub(r"<[^>]+>", "", g["ambasEnEntrada"]))
        self.assertIn("Salida lista. Los equipos quedan instalados 16 días.", txt_sin_html(g["salidaLista"]))
        self.assertIn("Su técnico y checklist van en el paso «Desinstalación».", txt_sin_html(g["salidaLista"]))
        self.assertIn("Toca el día en que se retiran los equipos, o elige un atajo:", txt_sin_html(g["salidaVacia"]))
        self.assertIn("Primero elige la fecha de la entrada", txt_sin_html(g["salidaSinEntrada"]))
        self.assertIn("OT2C.agOrden('ent')", g["salidaSinEntrada"])                                # y un botón para volver
        self.assertIn("Revisa la fecha de la salida", g["salidaMala"])
        self.assertIn("el mismo día de la instalación", g["mismoDia"])

    def test_los_atajos_de_la_salida_solo_aparecen_con_la_salida_activa(self):
        self.assertNotIn("o2m-ag-atajos", self.o["guia"]["ambasEnEntrada"])
        self.assertIn("o2m-ag-atajos", self.o["guia"]["salidaLista"])
        self.assertIn("o2m-ag-atajos", self.o["guia"]["salidaVacia"])
        self.assertNotIn("o2m-ag-atajos", self.o["guia"]["salidaSinEntrada"])                      # sin entrada no hay desde dónde contar

    def test_los_atajos_cuentan_desde_la_entrada_y_no_caen_en_domingo(self):
        h = self.o["atajos"]
        fechas = re.findall(r"agFechaRapida\('([\d-]+)'\)", h)
        self.assertEqual(fechas, ["2026-11-20", "2026-11-03", "2026-11-10", "2026-11-27", "2026-12-28"])   # sugerida, +1 sem, +2 sem, +1 mes, +2 meses (27/12 es domingo)
        self.assertIn("+1 sem · 3 nov", h)
        self.assertIn("+2 meses · 28 dic", h)
        self.assertIn("Sugerida · 20 nov", h)
        self.assertEqual(len(re.findall(r'class="o2m-ag-at on"', h)), 1)                           # el que coincide con la fecha elegida (3/11)
        self.assertIn("a los 24 días", h)                                                          # el motivo de la sugerida, sin el «Propuesta:»
        self.assertNotIn("Propuesta:", h)

    def test_esta_cableado_a_todo_cambio_de_fecha_y_solo_existe_con_la_tarjeta_nueva(self):
        self.assertIn('<div class="o2m-ag-resumen" id="o2mAgResumen" role="group"', _fn("pintarAgenda(b){", 12000))
        self.assertIn("_o2mAgResumenPintar();", _funcion("avisosAgenda"))                    # fecha u hora de la orden que se agenda
        self.assertTrue(_funcion("_diPintarRegiones").lstrip().split("\n")[1].strip().startswith("_o2mAgResumenPintar();"))   # cualquier cambio de la salida
        self.assertIn("agOrden:agOrden, agFechaRapida:agFechaRapida", M)
        self.assertEqual(M.count('id="o2mAgResumen"'), 1)
        self.assertIn("if (!el) return;", _funcion("_o2mAgResumenPintar").replace("var el = document.getElementById('o2mAgResumen'); ", ""))   # sin resumen no hace nada

    def test_no_se_reescribe_si_no_cambio(self):
        f = _funcion("_o2mAgResumenPintar")
        self.assertIn("if (h !== _agResUlt || !el.innerHTML)", f)
        self.assertIn("if (gu && (g !== _agGuiaUlt || !gu.innerHTML))", f)

    def test_cambiar_de_orden_no_reconstruye_el_paso_y_le_devuelve_el_foco_al_interruptor(self):
        f = _funcion("agOrden")
        self.assertIn("if (w === 'sal' && !_o2mDiAplica()) return;", f)                          # sin la tarjeta nueva no hace nada
        self.assertIn("if (S.agOrden === w) return;", f)
        self.assertIn("_agAplicarOrden();", f)
        self.assertIn("btn.focus(", f)                                                             # los botones se repintan: el foco vuelve
        g = _funcion("_agAplicarOrden")
        for pista in ("fp.value = f; fp.min = agMinFecha();", "hi.value = agHini();", "hf.value = agHfin();", "o2mCalCargarMes().then(pintar)",
                      "o2mDay.choque = false;", "avisosAgenda();", "_o2mAgAvisosSalida();", "o2mChequearChoque();"):
            self.assertIn(pista, g, pista)
        self.assertNotIn("pintarAgenda(", g)

    def test_estilos(self):
        for pista in (".o2m-ag-resumen{", ".o2m-ag-chip{", ".o2m-ag-chip.ok{", ".o2m-ag-chip.falta{", ".o2m-ag-gap{"):
            self.assertIn(pista, CSS_EV)
        self.assertIn("overflow-wrap:anywhere", CSS_EV.split(".o2m-ag-chip .tx em{")[1].split("}")[0])      # REGLA #15: nada se corta
        self.assertIn("grid-template-columns:1fr 1fr", CSS_EV.split("@media (max-width:720px){\n  .o2m-ag-resumen")[1][:80])   # en celular, dos fichas en UNA fila


def txt_sin_html(h):
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", h)).strip()


class TestFeriadosEnLosCalendariosDelAsistente(unittest.TestCase):
    """REGLA #31: todo calendario marca los días no laborales. El calendario del asistente (único para la entrada y la salida desde el 25-oct) lee
    `feriados` del mes."""

    def test_el_servidor_manda_los_feriados_del_mes(self):
        with open(os.path.join(RAIZ, "app.py"), encoding="utf-8", newline="") as fh:
            app = fh.read().replace("\r\n", "\n")
        i = app.index('_pref_mes = "%04d-%02d-" % (anio, mes)')
        self.assertIn("_feriados_mes = {k: v for k, v in _feriados_anio.items() if str(k).startswith(_pref_mes)}", app[i:i + 200])
        self.assertIn('"feriados": _feriados_mes,', app[i:i + 3200])

    def test_el_calendario_de_entrada_los_marca(self):
        f = _fn("o2mCalRenderGrid", 7500)
        for pista in ("nomFer", "clases.push('feriado')", "fer-mk", "Feriado: "):
            self.assertIn(pista, f)
        self.assertIn("o2mCal.fer[key] = (d && d.feriados) || {};", M)

    def test_la_salida_usa_el_mismo_calendario_y_por_lo_tanto_los_mismos_feriados(self):
        # (el calendario compacto propio de la salida, con su propia lectura de feriados, ya no existe)
        self.assertNotIn("_diCalGrid", M)
        self.assertNotIn("_diCalCargar", M)
        self.assertEqual(M.count("o2mCal.fer[key] = (d && d.feriados) || {};"), 1)
        self.assertEqual(M.count("/servicio-tecnico/api/calendario/mes/'+"), 2)       # el mes del calendario y la búsqueda de huecos (misma caché)
        self.assertIn('<i class="k" style="background:#fef2f2;border:1px solid #fecaca"></i>Feriado', M)
        self.assertNotIn(".dc-d.fer{", CSS_EV)

    def test_un_feriado_no_traba_solo_avisa_en_la_agenda(self):
        f = _fn("avisosAgenda(){", 4000)
        self.assertIn("var nomFerEntrada = (fecha && !sal) ? _o2mFeriadoDe(fecha) : '';", f)
        self.assertIn("a.push('Cae en feriado ('+esc(nomFerEntrada)+'), cuando no hay operación.');", f)
        self.assertNotIn("return false", f[f.index("nomFerEntrada"):f.index("nomFerEntrada") + 300])
        # en la SALIDA el feriado y el fin de semana los explica el servidor (_diAvisosHtml): no se duplican acá
        self.assertIn("if (fecha && !sal){ var d = new Date(fecha+'T12:00:00').getDay();", f)
        g = _fn("_o2mFeriadoDe(fecha){", 600)
        self.assertIn("o2mCal.fer[k]", g)

    def test_el_aviso_de_feriado_se_repinta_cuando_llega_el_mes(self):
        # 2.ª revisión independiente: con una fecha preasignada o tecleada en un mes aún no cargado, el aviso se calculaba antes de saber los feriados
        # y no volvía a calcularse; la grilla sí pintaba la bandera. Al llegar el mes se vuelve a avisar.
        f = _fn("o2mCalCargarMes(){", 2200)
        i = f.index("o2mCal.fer[key] = (d && d.feriados) || {};")
        self.assertIn("avisosAgenda();", f[i:i + 400])
        self.assertLess(f.index("o2mCalRenderGrid();", i), f.index("avisosAgenda();", i))


class TestRojoAVerde(unittest.TestCase):
    def test_los_campos_con_minimo_de_caracteres_pasan_de_rojo_a_verde(self):
        for pista in ("function _o2mVcUno(el){", "function _o2mVcBarrido(){", "if (raiz) raiz.addEventListener('input'",
                      "el.classList.toggle('is-valid', ok);", "el.classList.toggle('is-invalid', len > 0 && !ok);"):
            self.assertIn(pista, M)

    def test_el_motivo_y_los_cuatro_argumentos_llevan_data_min(self):
        self.assertIn('data-min="\'+O2M_DESC_MIN+\'"', M)
        self.assertEqual(len(re.findall(r'data-min="30"', M)), 4)

    def test_la_tarjeta_del_motivo_se_pone_verde(self):
        self.assertIn(".o2m-motivo.ok{border-color:#86efac;border-left-color:#16a34a;background:#f4fdf7}", M)
        self.assertIn(".o2m-motivo.ok .imp{color:#16a34a}", M)
        self.assertIn("'<div class=\"o2m-motivo'+(descOk?' ok':'')+'\">'", M)
        self.assertIn("var _cm = ta.closest('.o2m-motivo'); if (_cm) _cm.classList.toggle('ok', ok);", M)

    def test_tambien_tras_cada_repintado(self):
        i = M.index("function pintarAnclas(){")
        self.assertIn("_o2mVcBarrido()", M[i:i + 300])


if __name__ == "__main__":
    unittest.main()
