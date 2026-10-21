"""DESINSTALACIÓN — el FRENTE (Daniel, 19/20-oct-2026). Sin base de datos ni navegador: vigila por texto y con Jinja/node.

2026-10-21: Daniel pidió eliminar la función «Eventos» («el de eventos no me sirve, elimínalo y el código»): se quitaron de aquí las
pruebas del cuadro de evento del asistente, del filtro `evento` del panel y de las dos pantallas de eventos.

Cubre: el asistente de crear OT (paso «Desinstalación» después de Costos y antes de Anexo, nunca para un
técnico; el bloque que viaja es EXACTAMENTE lo que el servidor lee), la ficha (cinta/chips, filas, botón y modal «Programar desinstalación»,
nada para un técnico), el panel (chips de relación), Regularizar y Saldo de facturas, y la REGLA #1 (nada de alert/confirm/prompt nativos).
La prueba de verdad en navegador (Playwright, 1366 y 390 px) se corre aparte con el arnés local; esto evita que se rompa en silencio.

Correr (por archivo, la suite completa no sirve en esta máquina):  py -m pytest tests/test_ot_eventos_front.py -q -p no:warnings
"""
import os
import re
import shutil
import subprocess
import tempfile
import unittest

import jinja2

from tests import _fuente_app as F

RAIZ = F.RAIZ


def leer(*partes):
    with open(os.path.join(RAIZ, *partes), encoding="utf-8", newline="") as fh:
        return fh.read().replace("\r\n", "\n")


MODAL = leer("templates", "ot2", "_modal_crear.html")
FICHA = leer("templates", "ot2", "detalle.html")
PANEL = leer("templates", "ot2", "panel.html")
BASE = leer("templates", "base.html")
JS_FICHA = leer("static", "ot_desinst.js")
CSS = leer("static", "ot_evento.css")
JS_REGULARIZAR = leer("static", "ot_regularizar.js")
JS_SALDO = leer("static", "ot_saldo_facturas.js")
JS_AUT = leer("static", "ot_autorizaciones.js")

NATIVOS = re.compile(r"(?<![\w.$])(alert|confirm|prompt)\(")


def bloque_wizard():
    """El bloque «DESINSTALACIÓN UNIDA A SU INSTALACIÓN» del asistente (de su comentario de cabecera hasta la API pública)."""
    i = MODAL.index("DESINSTALACIÓN UNIDA A SU INSTALACIÓN (Daniel, 19-oct)")
    j = MODAL.index("API PÚBLICA — un solo modal")
    return MODAL[i:j]


class TestPlantillas(unittest.TestCase):
    def test_jinja_de_las_plantillas_tocadas(self):
        env = jinja2.Environment(extensions=["jinja2.ext.do"])
        for nombre, src in (("_modal_crear", MODAL), ("detalle", FICHA), ("panel", PANEL), ("base", BASE)):
            env.parse(src)   # lanza TemplateSyntaxError si algo quedó mal

    @unittest.skipUnless(shutil.which("node"), "node no está disponible")
    def test_js_sin_errores_de_sintaxis(self):
        for ruta in ("ot_desinst.js", "ot_regularizar.js", "ot_saldo_facturas.js", "ot_autorizaciones.js"):
            r = subprocess.run(["node", "--check", os.path.join(RAIZ, "static", ruta)], capture_output=True, text=True)
            self.assertEqual(r.returncode, 0, ruta + "\n" + r.stderr[:800])
        # El script del asistente (Jinja neutralizado): es el más grande y el que más cambió.
        bloques = re.findall(r"<script(?![^>]*\bsrc=)[^>]*>(.*?)</script>", MODAL, re.S)
        grande = max(bloques, key=len)
        grande = re.sub(r"\{#.*?#\}", "", grande, flags=re.S)
        grande = re.sub(r"\{\{.*?\}\}", "false", grande, flags=re.S)
        with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as fh:
            fh.write(grande)
            nombre = fh.name
        try:
            r = subprocess.run(["node", "--check", nombre], capture_output=True, text=True)
            self.assertEqual(r.returncode, 0, r.stderr[:800])
        finally:
            os.unlink(nombre)

    def test_regla_1_nada_de_popups_nativos_en_lo_nuevo(self):
        for nombre, src in (("ot_desinst.js", JS_FICHA), ("bloque del asistente", bloque_wizard())):
            self.assertIsNone(NATIVOS.search(src), nombre)

    def test_base_ya_no_nombra_las_pantallas_de_eventos(self):
        self.assertNotIn("ot_pagina_evento", BASE)
        self.assertIn("['ot2_panel','ot2_diagnostico']", BASE)


class TestAsistente(unittest.TestCase):
    def test_paso_desinstalacion_va_despues_de_costos_y_antes_de_anexo(self):
        i_fin = MODAL.index("p.push({k:'finanzas', n:'Costos'});")
        i_des = MODAL.index("p.push({k:'desinst', n:'Desinstalación'});")
        i_anx = MODAL.index("p.push({k:'anexo', n:'Anexo'});")
        self.assertLess(i_fin, i_des)
        self.assertLess(i_des, i_anx)
        self.assertIn("case 'desinst':  return _diCompleto();", MODAL)
        self.assertIn("if (p.k==='desinst')  pintarDesinst(b);", MODAL)
        self.assertIn("if (k==='desinst')  pintarDesinst(b);", MODAL)

    def test_el_paso_no_existe_para_un_tecnico_ni_para_otros_tipos(self):
        self.assertIn("var O2M_ES_TECNICO = {{ 'true' if es_tecnico else 'false' }};", MODAL)
        m = re.search(r"function _o2mDiAplica\(\)\{ return ([^}]*) \}", MODAL)
        self.assertIsNotNone(m)
        cond = m.group(1)
        for k in ("S.tipo === 'instalacion'", "!esInterno()", "!O2M_ES_TECNICO", "!!S.cliente"):
            self.assertIn(k, cond)

    def test_el_cuerpo_lleva_la_desinstalacion_antes_de_pedir_autorizacion(self):
        i_cuerpo = MODAL.index("if (_o2mDiAplica() && S.desinst.lleva) body.desinstalacion = _diPayload();")
        i_aut = MODAL.index("return pedirAutorizacionCrear(body, _esSA, sig, _origSig, {tipo: 'exceder_saldo'")
        self.assertLess(i_cuerpo, i_aut)   # el payload de «Pedir autorización a Daniel» copia este mismo body
        self.assertNotIn("body.evento_id", MODAL)
        self.assertIn("var payload = JSON.parse(JSON.stringify(body));", MODAL)

    def test_lo_que_viaja_es_lo_que_el_servidor_lee(self):
        """Cada clave que el frente manda en `desinstalacion` existe en _ot_desinst_normalizar (contrato del backend)."""
        f = F.fuente_funcion("_ot_desinst_normalizar")
        i = bloque_wizard().index("function _diPayload()")
        cuerpo = bloque_wizard()[i:i + 3200]
        claves = set(re.findall(r"\bo\.([a-z_]+)\s*=", cuerpo)) | {"lleva"}
        self.assertGreaterEqual(len(claves), 14, claves)
        for k in sorted(claves):
            self.assertIn('"%s"' % k, f, "el servidor no lee la clave «%s»" % k)
        self.assertNotIn("confirmo_no", F.texto())   # 2026-10-21: la confirmación del «No» dentro de un evento se eliminó con Eventos
        # y el mismo formato en la ficha (POST /ot/api/<vid>/desinstalacion)
        j = JS_FICHA.index("function cuerpo()")
        cuerpo2 = JS_FICHA[j:j + 2200]
        for k in set(re.findall(r"\bo\.([a-z_]+)\s*=", cuerpo2)):
            if k == "argumento":
                continue   # lo lee la ruta (autorización), no el normalizador
            self.assertIn('"%s"' % k, f, "la ficha manda «%s» y el servidor no la lee" % k)

    def test_el_sistema_sugiere_y_la_persona_decide(self):
        b = bloque_wizard()
        # lo sugerido queda visible y editable: nada se agrega por detrás del cuerpo
        self.assertIn("o.equipos = _diEqIncluidos().map(", b)           # solo los equipos marcados en pantalla, con el checklist elegido
        self.assertIn("Seleccionar/deseleccionar todo", b)              # REGLA #14
        self.assertIn("No puedes mezclar técnicos externos de dos empresas distintas", b)
        self.assertIn("Los domingos no se agendan órdenes de trabajo", b)   # REGLA #31

    def test_pie_y_mensajes(self):
        self.assertIn("txt = 'Crear 2 órdenes'", MODAL)
        self.assertIn("' + desinstalación '", MODAL)
        self.assertIn("La instalación quedó creada", MODAL)       # si falla solo la hija, ámbar y con salida
        self.assertIn("Prográmala desde la ficha de la instalación", MODAL)
        self.assertNotIn("DESINST_CONFIRMAR_NO", MODAL)

    def test_ya_no_hay_cuadro_de_evento_en_el_paso_tipo(self):
        for k in ("o2mEventoBox", "/ot/api/eventos", "_pintarEventoDom", "OT2C.crearEvento", "OTEventoForm", "ot_evento_form.js",
                  "_evPreset", "_qs.get('evento')", "usarDireccionEvento", "o2mCpEvento"):
            self.assertNotIn(k, MODAL, k)
        # el estilo de la desinstalación sí se carga
        self.assertIn("ot_evento.css", MODAL)

    def test_margen_de_la_familia_en_costos(self):
        self.assertIn("html += _o2mDiFamiliaHtml();", MODAL)
        self.assertIn("Cuenta de la familia (instalación + desinstalación)", bloque_wizard())

    def test_revision_previa_sin_escribir(self):
        b = bloque_wizard()
        self.assertIn("/ot/api/desinstalacion/previo", b)
        self.assertIn("pedido_madre", b)
        self.assertIn("setTimeout(function(){ _diPrevio(body, key); }, rapido ? 0 : 450)", b)   # debounced
        self.assertIn("if (mi !== _diSeq", b)                                                   # una respuesta vieja no pisa a la nueva


class TestFicha(unittest.TestCase):
    def test_cinta_chips_y_filas(self):
        for k in ('class="ev-rel" id="evRel"', "desinst.es_hija and desinst.madre", "desinst.chip",
                  '<span class="lbl">Desinstalación</span>', '<span class="lbl">Instalación</span>',
                  "desinst.aviso_cierre.texto", "desinst.madre.ticket.url"):
            self.assertIn(k, FICHA, k)

    def test_botones_de_gestion_nunca_para_un_tecnico(self):
        for m in re.finditer(r"OTDesinst\.(abrirProgramar|abrirDeclarar|abrirQuitar)\(\)", FICHA):
            previo = FICHA[max(0, m.start() - 900):m.start()]
            self.assertIn("not es_tecnico", previo, m.group(0))
        # 2026-10-21: la fila «Evento» y su botón se eliminaron con Eventos
        self.assertNotIn("cambiarEvento", FICHA)
        self.assertNotIn("desinst.evento", FICHA)

    def test_config_y_script(self):
        self.assertIn("window.OTD_DESINST_CFG = {", FICHA)
        self.assertIn("ot_desinst.js", FICHA)
        self.assertIn("ot_evento.css", FICHA)

    def test_modal_programar_y_demas_acciones(self):
        for k in ("/ot/api/' + VID + '/desinstalacion'", "/ot/api/' + VID + '/desinstalacion-prevista'",
                  "otdDesinstModal", "Seleccionar/deseleccionar todo", "Quitar la desinstalación prevista", "Agendar igual", "pendiente_autorizacion",
                  "Los domingos no se agendan órdenes de trabajo"):
            self.assertIn(k, JS_FICHA, k)


class TestPanel(unittest.TestCase):
    def test_el_panel_ya_no_tiene_filtro_de_evento_y_conserva_los_filtros_de_siempre(self):
        n = PANEL.count("pago_prov=f_pago or None")
        self.assertEqual(n, 12)
        self.assertEqual(PANEL.count("pago_prov=f_pago or None, resp=f_resp or None"), n)
        for k in ("f_evento", "eventos_opts", 'name="evento"', "evento=f_evento"):
            self.assertNotIn(k, PANEL, k)
        self.assertNotIn("evento=", PANEL.replace("evento_", ""))

    def test_chips_de_la_desinstalacion_en_el_panel(self):
        # 2026-10-21 (Daniel: «el de eventos no me sirve, elimínalo y el código»): sin botón «Eventos», sin filtro y sin chip de evento.
        self.assertNotIn("url_for('ot_pagina_eventos')", PANEL)
        for k in ("f.rel.chip", "ev-pchip", "d.rel_nivel"):
            self.assertIn(k, PANEL, k)
        for k in ("f.evento_nombre", "ot_pagina_evento_detalle", "d.evento"):
            self.assertNotIn(k, PANEL, k)
        self.assertNotIn("bi-flag-fill", bloque_wizard())


class TestBandejas(unittest.TestCase):
    def test_regularizar_dice_a_quien_cubre_y_no_invita_a_agregar_documento(self):
        self.assertIn("it.falta === 'madre_factura'", JS_REGULARIZAR)
        self.assertIn("Abrir la instalación", JS_REGULARIZAR)
        self.assertIn("Cubierta por", JS_REGULARIZAR)

    def test_saldo_de_facturas_muestra_la_desinstalacion_bajo_la_ot(self):
        self.assertIn("u.desinstalaciones", JS_SALDO)
        self.assertIn("sf-desinst", JS_SALDO)

    def test_autorizaciones_muestra_que_incluye_la_desinstalacion(self):
        self.assertIn("a.incluye_desinstalacion", JS_AUT)


class TestCssYCelular(unittest.TestCase):
    def test_botones_e_inputs_de_celular(self):
        self.assertRegex(CSS, r"\.ev-btn\{min-height:44px")
        self.assertRegex(CSS, r"\.ev-in\{[^}]*min-height:44px;font-size:16px")
        self.assertRegex(CSS, r"\.o2m-di-atajo\{min-height:44px")
        self.assertIn(".o2m-di-fila input,.o2m-di-sub textarea,.o2m-di-sub select{width:100%;min-height:44px;font-size:16px", CSS)

    def test_nombres_completos_nunca_cortados(self):
        self.assertNotIn("text-overflow:ellipsis", CSS)
        self.assertNotIn("line-clamp", CSS)


if __name__ == "__main__":
    unittest.main()
