# -*- coding: utf-8 -*-
"""2026-10-24 · Daniel: «una tarjeta que diga instalación con desinstalación… el tipo de OT decide el comportamiento»; «si el usuario la usa es
porque es un evento: abrir una OT de entrada (instalar) y una de salida (desinstalar)… sí o sí un técnico»; «las pruebas pasan de rojo a verde…
en el proyecto entero, cuando la tarjeta apruebe los caracteres, tiene que pasar de rojo a verde».
"""
import os
import re
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
    """Daniel (24-oct, de noche): «que el modal se comporte en términos de calendario para crear dos órdenes de trabajo, una por instalación y otra
    por desinstalación». El paso Agenda agenda LAS DOS: arriba la entrada (el calendario de siempre) y abajo la salida (calendario compacto)."""

    def test_el_paso_agenda_agrega_la_salida_solo_con_la_tarjeta_nueva(self):
        f = _fn("pintarAgenda(b){", 9500)
        self.assertIn("var di = _o2mDiAplica();", f)
        self.assertIn("(di ? ' · entrada y salida' : '')", f)
        self.assertIn("+(di ? _o2mAgSalidaHtml() : '');", f)
        self.assertIn("if (di) _o2mAgSalidaPintar();", f)
        # La instalación a secas queda IGUAL (sin bloque de salida ni textos nuevos).
        self.assertIn("(di ? '<p class=\"d\"><b>Aquí agendas las dos órdenes.</b>", f)

    def test_los_dos_calendarios_se_rotulan_entrada_y_salida(self):
        self.assertIn("<b>Entrada · instalación</b>", M)
        i = M.index("function _o2mAgSalidaHtml(){")
        h = M[i:i + 1400]
        self.assertIn("<b>Salida · desinstalación</b>", h)
        self.assertIn("sí o sí", h)                                    # el técnico de la salida es obligatorio
        self.assertIn("paso «Desinstalación»", h)                       # técnico, costo y checklist siguen en su paso

    def test_la_salida_usa_los_mismos_contenedores_que_ya_repinta_la_desinstalacion(self):
        h = M[M.index("function _o2mAgSalidaHtml(){"):][:1500]
        self.assertIn('<div id="o2mDiFecha"></div><div id="o2mDiAvisos"></div>', h)
        # En el paso «Desinstalación» ya NO existen (ids únicos) cuando se usa la tarjeta nueva.
        self.assertIn("(S.conDesinst ? '' : '<div id=\"o2mDiFecha\"></div><div id=\"o2mDiAvisos\"></div>')", M)
        self.assertEqual(M.count('<div id="o2mDiFecha"></div>'), 2, "uno en la salida de la agenda y el clásico (solo sin la tarjeta nueva)")

    def test_pintar_la_salida_no_depende_de_que_exista_el_paso_desinstalacion(self):
        # Al pintar el formulario completo la Agenda va ANTES: _diPintarRegiones se sale si no existe o2mDiPreg, así que se pinta directo.
        f = _fn("_o2mAgSalidaPintar(){", 700)
        self.assertIn("_diRegFecha(f)", f)
        self.assertIn("_diRegAvisos(a)", f)
        self.assertNotIn("_diPintarRegiones", f)
        self.assertIn("_diProgramarPrevio(true)", f)

    def test_la_agenda_exige_tambien_la_fecha_de_la_salida(self):
        self.assertIn("case 'agenda':   return !!S.fecha && S.fecha >= _o2mHoyStr() && (!_o2mDiAplica() || _diFechaSalidaOk());", M)
        f = _fn("_diFechaSalidaOk(){", 300)
        self.assertIn("d.fecha >= (S.fecha || '')", f)
        self.assertIn("!_o2mEsDomingo(d.fecha)", f)

    def test_el_paso_desinstalacion_apunta_a_la_agenda_y_se_mantiene_al_dia(self):
        i = M.index("function _diRegPreg(el){")
        f = M[i:i + 3600]
        self.assertIn("que agendas en el paso <b>Agenda</b>", f)
        self.assertIn("OT2C.irAncla(\\'agenda\\')", f)
        # El aviso «necesita un técnico» y las fechas de las dos órdenes se repintan al cambiar fecha, técnicos o la fecha de la instalación.
        self.assertIn("var regs = ['preg', 'tec', 'res'];", M)
        self.assertIn("_diPintarRegiones(['preg', 'fecha', 'avisos', 'res']);", M)
        self.assertEqual(M.count("_diPintarRegiones(['preg', 'tec', 'costo', 'res']);"), 2)       # diTecModo y diTecTog

    def test_estilos_de_los_dos_calendarios(self):
        for pista in (".o2m-ag-lbl{", ".o2m-ag-lbl.ent i{", ".o2m-ag-lbl.sal i{", ".o2m-ag-salida{", ".o2m-ag-union{"):
            self.assertIn(pista, CSS_EV)

    def test_la_salida_no_se_pide_a_un_tecnico_ni_en_trabajo_interno(self):
        self.assertIn("!esInterno() && !O2M_ES_TECNICO && !!S.cliente", M[M.index("function _o2mDiAplica(){"):][:300])


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
