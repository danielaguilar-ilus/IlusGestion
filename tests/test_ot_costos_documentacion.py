"""«Costos y documentación de la OT» — el bloque único se renombra, la tarjeta «Editar finanzas» sale de la página y se vuelve el modal
«Editar cobro y costos», y el resumen de arriba habla en palabras de Daniel (2026-10-08, con la captura de la OT-2026-00245).

Daniel: «hay un botón que dice Editar finanzas que no lo veo muy necesario; acá podemos ahorrar espacio porque arriba dice toda esta
información… el Editar finanzas está de más y ocupa muchísimo espacio, es casi la misma información que está arriba. Resumamos arriba:
dejemos cuánto ganamos, cuánto cobramos, cuánto nos cobraron, cuánto perdimos… todo bien detalladito.» Y antes: «ahí me gustaría colocarle
COSTOS Y DOCUMENTACIÓN de la orden de trabajo».

Lo que se fija acá (REGLA #4.2: no se pierde ninguna función):
  1. Nombres: «Costos y documentación de la OT» (ficha) y «… para cerrar» (modal de cierre); toast «✓ Costos de la OT guardados».
  2. La tarjeta #otdCardFinanzas ya no está en el flujo de la página: vive dentro del modal #otdFinModal, a NIVEL RAÍZ (antes de los
     scripts, fuera de la grilla y de las pestañas) y con TODOS sus ids de edición.
  3. El botón «Editar cobro y costos» solo lo dibuja el servidor para quien puede editar (`fm_edita` = puede_metadata); nunca en el modal
     de cierre ni para técnicos (REGLA #19).
  4. El resumen: «Cobramos $X», «Nos cobraron $Y» y el resultado en grande, «Ganamos $Z (n %)» o «Perdimos $Z» (o «No se cobra · nos costó
     $Y»), la mini-tabla Cobramos / Nos cobraron / Queda con su Total, y una línea por dato (de dónde sale lo cobrado, valorizado, quién
     lo declaró, autorización).
  5. Guardar sigue actualizando el bloque único (OTFinMotor.refrescar) y cierra el modal; el chip «Documento» del encabezado lo abre.

Sin BD ni Flask: texto de las plantillas/JS/CSS, la plantilla del motor con jinja2 y el motor JS en node con un DOM mínimo.
Correr con:  py -m unittest tests.test_ot_costos_documentacion
"""
import json
import os
import re
import shutil
import subprocess
import tempfile
import unittest

import jinja2

from tests.test_ot_finanzas_compacto import RAIZ, TXT_INTERNA, _caso, _leer

IDS_DE_EDICION = (
    # paso 1 · lo que se le cobra al cliente
    "otdFinPasoCobro", "otdFinAddToggle", "otdFinAddBox", "otdErpDocQ", "otdErpDocResultados", "otdDocTipo", "otdDocNum", "otdDocsLista",
    "otdFinDocErpTido", "otdFinDocErpNudo", "otdBtnLeerZz", "otdZzBox", "otdZzUsar", "otdFinCobroMontos", "otdFinZzServ", "otdFinZzEnvio",
    "otdFinCosto", "otdFinZzMotivo", "otdFinValBox", "otdFinValAbrir", "otdFinValorizado", "otdFinNoCobra",
    # paso 2 · lo que nos cobraron
    "otdFinPasoCosto", "otdFinProveedorTipo", "otdTecChips", "otdFinProveedorNombre", "otdFinCostoProveedor", "otdFinCostoDespacho",
    "otdFinCostoTotal", "otdFinCotiz", "otdFinCotizCuerpo",
    # paso 3 · centro de costo y la barra con la cuenta en vivo
    "otdFinPasoArea", "otdFinCentroCosto", "otdCcGrid", "otdFinBar", "otdFinBarCobro", "otdFinBarCosto", "otdFinMargen", "otdFinMargenPct",
    "otdFinBtnGuardar", "otdFinBtnCorregirBar", "otdFinBloqueo", "otdFinResultado",
)


def _sin_ruido(txt):
    """Quita comentarios Jinja y el contenido de <script> (que trae '<div' en cadenas de JS) sin mover las posiciones."""
    blanco = lambda m: re.sub(r"[^\n]", " ", m.group(0))
    txt = re.sub(r"\{#.*?#\}", blanco, txt, flags=re.S)
    return re.sub(r"<script\b.*?</script>", blanco, txt, flags=re.S)


def _profundidad(txt, pos):
    """Cuántos <div> hay abiertos justo antes de `pos` (el texto se limpia antes con _sin_ruido)."""
    antes = txt[:pos]
    return len(re.findall(r"<div\b", antes)) - len(re.findall(r"</div>", antes))


class _HarnessBase(unittest.TestCase):
    """El motor JS en node con un DOM mínimo (como test_ot_finanzas_compacto) y los dos <template> del servidor."""

    @classmethod
    def setUpClass(cls):
        cls.node = shutil.which("node")
        if not cls.node:
            raise unittest.SkipTest("node no está instalado")
        cls.tmp = tempfile.mkdtemp(prefix="costos_doc_")
        cls.harness = os.path.join(cls.tmp, "harness.js")
        with open(cls.harness, "w", encoding="utf-8") as f:
            f.write(r"""
const fs = require('fs');
const datos = JSON.parse(fs.readFileSync(process.argv[3], 'utf8'));
global.window = global;
const attrs = { 'data-vid': '245', 'data-modo': process.argv[4] || 'ficha' };
if (datos.declaro) attrs['data-declaro'] = datos.declaro;
const el = {
  innerHTML: '', classList: { add() {}, remove() {} },
  getAttribute(k) { return attrs[k] || null; },
  addEventListener() {}, closest() { return null; }, contains() { return true; },
  querySelector(sel) {
    if (sel.indexOf('data-fm-extra') >= 0 && datos.extra) return { innerHTML: datos.extra };
    if (sel.indexOf('data-fm-edit') >= 0 && datos.editar) return { innerHTML: datos.editar };
    return null;
  }
};
global.document = { readyState: 'complete', querySelectorAll() { return [el]; }, getElementById() { return null; },
                    createElement() { return { style: {}, setAttribute() {}, appendChild() {} }; }, addEventListener() {} };
global.ilusToast = function () {};
global.fetch = function (url) {
  const j = url.indexOf('/recorrido') >= 0 ? datos.recorrido : datos.panorama;
  return Promise.resolve({ ok: true, status: 200, json() { return Promise.resolve(j); } });
};
require(process.argv[2]);
setTimeout(function () { process.stdout.write(el.innerHTML); }, 80);
""")

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def _pintar(self, caso, modo="ficha", editar="", extra="", declaro="", mutar=None):
        rec, pan = _caso(caso)
        if mutar:
            mutar(rec, pan)
        datos = os.path.join(self.tmp, "datos.json")
        with open(datos, "w", encoding="utf-8") as f:
            json.dump({"recorrido": rec, "panorama": pan, "extra": extra, "editar": editar, "declaro": declaro}, f)
        r = subprocess.run([self.node, self.harness, os.path.join(RAIZ, "static", "ot_fin_motor.js"), datos, modo],
                           capture_output=True, text=True, encoding="utf-8", timeout=60)
        self.assertEqual(r.returncode, 0, r.stderr)
        return r.stdout


BOTON_EDITAR = ('<button type="button" class="fm-btn fm-btn-pri" onclick="otdFinModalAbrir()">'
                '<i class="bi bi-pencil-square"></i> Editar cobro y costos</button>')


class TestNombres(_HarnessBase):
    def test_el_bloque_se_llama_costos_y_documentacion_en_la_ficha_y_en_el_modal_de_cierre(self):
        ficha = self._pintar("cobro_2docs")
        self.assertIn("Costos y documentación de la OT", ficha.split('<div class="fm-head">')[1].split("</h3>")[0])
        modal = self._pintar("cobro_2docs", modo="modal")
        self.assertIn("Costos y documentación para cerrar", modal.split('<div class="fm-head">')[1].split("</h3>")[0])
        for html in (ficha, modal):
            self.assertNotIn("Finanzas y documentos", html)

    def test_ningun_texto_viejo_queda_en_el_codigo_que_se_ve(self):
        js = _leer("static/ot_fin_motor.js")
        self.assertNotIn("> Finanzas y documentos", js)
        tpl = _leer("templates/ot2/_fin_motor.html")
        self.assertNotIn("Cargando finanzas y documentos", tpl)
        self.assertIn("Cargando costos y documentación", tpl)
        det = _leer("templates/ot2/detalle.html")
        self.assertNotIn("'Editar finanzas'", det)
        self.assertNotIn(">Editar finanzas<", det)
        self.assertNotIn("✓ Finanzas de la OT guardadas", det)
        self.assertIn("✓ Costos de la OT guardados", det)
        self.assertIn("✓ Costos guardados con el saldo disponible", det)

    def test_la_tarjeta_se_llama_editar_cobro_y_costos_para_quien_edita_y_costos_y_documentacion_para_el_resto(self):
        det = _leer("templates/ot2/detalle.html")
        self.assertIn("{{ 'Editar cobro y costos' if puede_metadata else 'Costos y documentación de la OT' }}", det)


class TestLaTarjetaSaleDeLaPaginaYVaAUnModal(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.det = _leer("templates/ot2/detalle.html")
        cls.limpio = _sin_ruido(cls.det)

    def _modal(self):
        i = self.det.index('id="otdFinModal"')
        j = self.det.index("<script>", i)
        return self.det[i:j]

    def test_hay_un_solo_modal_y_una_sola_tarjeta(self):
        self.assertEqual(self.det.count('id="otdFinModal"'), 1)
        self.assertEqual(self.det.count('id="otdCardFinanzas"'), 1)
        self.assertEqual(self.det.count('id="otdCardMotor"'), 1)

    def test_la_tarjeta_ya_no_esta_en_la_grilla_ni_en_las_pestanas_sino_en_el_modal_a_nivel_raiz(self):
        det, limpio = self.det, self.limpio
        i_modal, i_card = limpio.index('id="otdFinModal"'), limpio.index('id="otdCardFinanzas"')
        i_motor, i_ruta = limpio.index('id="otdCardMotor"'), limpio.index('id="otdModalRuta"')
        # la tarjeta viene DESPUÉS del bloque único, de la barra inferior y de toda la grilla, y antes del primer <script> que usa sus ids
        self.assertGreater(i_modal, i_motor)
        self.assertGreater(i_modal, limpio.index('class="otd-bottom"'))
        self.assertLess(i_modal, i_ruta)
        self.assertLess(i_card, det.index("/* 🔧 OBSERVACIÓN Y SALTO POR EQUIPO"))
        # profundidad: el modal cuelga del mismo nivel que otros modales a nivel raíz (otdModalRuta); la tarjeta va 4 niveles más adentro
        # (modal > dialog > content > body), nunca dentro de .otd-hero ni de .otd-tabpanel
        d_modal = _profundidad(limpio, limpio.rindex("<div", 0, i_modal))
        d_ruta = _profundidad(limpio, limpio.rindex("<div", 0, i_ruta))
        self.assertEqual(d_modal, d_ruta, "el modal está al mismo nivel que los otros modales de la ficha (raíz)")
        self.assertEqual(_profundidad(limpio, limpio.rindex("<div", 0, i_card)), d_modal + 4)
        self.assertGreater(_profundidad(limpio, limpio.rindex("<div", 0, i_motor)), d_modal, "el bloque único sigue dentro de la grilla")

    def test_el_lugar_viejo_quedo_solo_con_el_comentario(self):
        det = self.det
        i = det.index("La tarjeta de edición (#otdCardFinanzas, con TODOS sus ids y funciones: REGLA #4.2)")
        resto = det[i:i + 900]
        self.assertIn("#otdFinModal", resto)
        self.assertNotIn('class="otd-card{{', resto)

    def test_el_modal_solo_existe_para_gestion_que_puede_editar_y_nunca_para_tecnicos(self):
        det = self.det
        i = det.index('<div class="modal fade otd-finm" id="otdFinModal"')
        previo = det[det.rindex("{# ═", 0, i):i]
        self.assertIn("{% if not es_tecnico %}", previo)
        self.assertIn("{% if puede_metadata %}", previo, "sin permiso de edición no hay modal (la OT cerrada queda como hoy)")
        self.assertLess(previo.index("{% if not es_tecnico %}"), previo.index("{% if puede_metadata %}"))
        # y cierra con los dos {% endif %} al final de la tarjeta
        pie = det[det.index('class="modal-footer otd-finm-f"'):][:900]
        self.assertEqual(pie.count("{% endif %}"), 2)

    def test_el_modal_conserva_todos_los_ids_de_edicion(self):
        modal = self._modal()
        for ident in IDS_DE_EDICION:
            self.assertIn(f'id="{ident}"', modal, ident)
        # los modales y paneles que usa la tarjeta siguen en la página
        det = self.det
        self.assertIn('id="otdModalFinCorr"', det)
        self.assertIn("async function otdFinCorrAbrir(){", det)
        for fn in ("window.otdFinCuenta = function", "window.otdFinPintar = function", "window.otdFinVivo = vivo",
                   "window.otdGuardarFinanzas = async function", "window.otdCorregirFinanzas = async function",
                   "window.otdBloquearFinanzas = function", "window.otdFinAbrirSumar = function", "window.otdFinValAbrir = function"):
            self.assertIn(fn, det, fn)
        self.assertIn("window.ilusOtFinanzas", det + _leer("static/ot_finanzas.js"))
        self.assertIn("ot_finanzas.js", det)

    def test_el_modal_es_de_pantalla_completa_en_el_celular_con_botones_de_44px(self):
        det = self.det
        self.assertIn('class="modal-dialog modal-xl modal-dialog-scrollable modal-fullscreen-md-down"', det)
        self.assertIn('data-bs-backdrop="static"', self._modal().split(">")[0] + ">" + self._modal()[:600])
        css = det[det.index("«Editar cobro y costos»: la tarjeta de edición vive en un MODAL"):]
        css = css[:css.index("/* ── 0 · Barra de resultado ── */")]
        self.assertIn("height:100dvh !important", css, "mobile.css fija height:auto !important: se pide la pantalla completa a propósito")
        self.assertIn("min-height:44px", css)
        self.assertIn("@media (max-width:767.98px)", css)
        self.assertIn("#otdFinModal #otdCardFinanzas > h3{display:none}", css, "el título vive en el encabezado del modal, no se repite")

    def test_la_vista_de_solo_lectura_sigue_para_la_ot_cerrada_y_sale_junto_al_bloque_si_el_motor_falla(self):
        det, js = self.det, _leer("static/ot_fin_motor.js")
        self.assertIn("class=\"otd-card{{ '' if puede_metadata else ' fin-ro' }}\" id=\"otdCardFinanzas\"", det)
        self.assertIn("#otdCardFinanzas.fin-ro:not(.fin-motor-fallo){display:none}", det)
        self.assertIn("bloque.parentNode.insertBefore(viejaFin, bloque.nextSibling)", js)
        self.assertIn("!viejaFin.closest('.modal')", js, "la tarjeta del modal de edición nunca se mueve")


class TestElBotonEditarCobroYCostos(_HarnessBase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.env = jinja2.Environment(loader=jinja2.FileSystemLoader(os.path.join(RAIZ, "templates")), autoescape=True)
        cls.env.globals["url_for"] = lambda ep, **kw: "/static/" + kw.get("filename", "")
        cls.env.filters["chile_fmt"] = lambda d, f="%d/%m/%Y %H:%M": d.strftime(f) if hasattr(d, "strftime") else ""
        cls.tpl = cls.env.get_template("ot2/_fin_motor.html")

    def test_el_servidor_dibuja_el_boton_solo_si_puede_editar_y_solo_en_la_ficha(self):
        con = self.tpl.render(fm_vid=7, fm_modo="ficha", fm_assets=False, fm_edita=True)
        self.assertIn("<template data-fm-edit>", con)
        self.assertIn("Editar cobro y costos", con)
        self.assertIn("otdFinModalAbrir()", con)
        self.assertNotIn("<template data-fm-edit>", self.tpl.render(fm_vid=7, fm_modo="ficha", fm_assets=False, fm_edita=False))
        self.assertNotIn("<template data-fm-edit>", self.tpl.render(fm_vid=7, fm_modo="ficha", fm_assets=False), "sin permiso no hay botón")
        self.assertNotIn("<template data-fm-edit>", self.tpl.render(fm_vid=7, fm_modo="modal", fm_assets=False, fm_edita=True),
                         "nunca en el modal de cierre")

    def test_la_ficha_le_pasa_el_permiso_de_edicion_y_el_modal_de_cierre_no(self):
        det = _leer("templates/ot2/detalle.html")
        self.assertIn("{% with fm_vid=v.id, fm_modo='ficha', fm_assets=True, fm_edita=puede_metadata %}{% include 'ot2/_fin_motor.html' %}", det)
        self.assertIn("{% with fm_vid=v.id, fm_modo='modal', fm_assets=False %}{% include 'ot2/_fin_motor.html' %}", det)

    def test_el_motor_pone_el_boton_en_el_encabezado_junto_a_actualizar_y_antes_de_los_de_superadmin(self):
        html = self._pintar("cobro_2docs", editar=BOTON_EDITAR, extra='<button class="fm-btn">Corregir finanzas</button>')
        botones = html.split('<div class="fm-head-a">')[1].split("</div>")[0]
        i_act, i_edit, i_cor = botones.index("Actualizar"), botones.index("Editar cobro y costos"), botones.index("Corregir finanzas")
        self.assertTrue(i_act < i_edit < i_cor)
        self.assertIn("otdFinModalAbrir()", botones)

    def test_sin_el_template_el_motor_no_inventa_el_boton(self):
        self.assertNotIn("Editar cobro y costos", self._pintar("cobro_2docs"))
        self.assertNotIn("Editar cobro y costos", self._pintar("cobro_2docs", modo="modal"))

    def test_si_el_motor_no_puede_leer_el_boton_de_editar_sigue_disponible(self):
        js = _leer("static/ot_fin_motor.js")
        self.assertIn("""Reintentar</button>' + (inst.editar || '') + '</div>'""", js)
        self.assertIn("inst.editar = ed ? ed.innerHTML : ''", js)
        # y el botón no se cuela en ninguna otra pantalla que monte el motor
        self.assertNotIn("data-fm-edit", _leer("templates/ot2/panel.html"))


class TestElResumenDeArriba(_HarnessBase):
    def _cta(self, html):
        return html.split('<section class="fm-cta ')[1].split("</section>")[0]

    def test_ganamos_en_grande_con_cobramos_y_nos_cobraron(self):
        cta = self._cta(self._pintar("cobro_2docs"))
        self.assertIn("<small>Cobramos</small><b>$302.101</b>", cta)
        self.assertIn("<small>Nos cobraron</small><b>$250.000</b>", cta)
        self.assertIn('<div class="fm-res-i q"><small>Ganamos</small><b>$52.101<em>(17,2 %)</em></b></div>', cta)
        self.assertTrue(cta.index("Cobramos</small>") < cta.index("Nos cobraron</small>") < cta.index("Ganamos</small>"))
        self.assertIn("Margen sano", cta)
        self.assertTrue(cta.startswith("c-ok"), "el resultado va en verde")

    def test_perdimos_en_rojo_con_el_monto_sin_signo(self):
        def perdida(rec, pan):
            f = rec["fin"]
            f.update(clase="ambar", label="Pérdida")
            f["me_cobraron"].update(tecnico=330000, total=380000)
            f["queda"].update(total=-78000, servicio=-78000, despacho=0, pct=-25.8)
        cta = self._cta(self._pintar("cobro_2docs", mutar=perdida))
        self.assertIn('<div class="fm-res-i q"><small>Perdimos</small><b>$78.000<em>(−25,8 %)</em></b></div>', cta)
        self.assertTrue(cta.startswith("c-rojo"), "una pérdida siempre va en rojo, aunque el semáforo del servidor diga otra cosa")
        self.assertNotIn("Ganamos", cta)

    def test_sin_dato_para_calcular_no_inventa_ganancia_ni_perdida(self):
        def falta(rec, pan):
            rec["fin"]["queda"].update(mostrar=False, total=0, pct=None)
            rec["fin"]["me_cobraron"].update(falta_tecnico=True, tecnico=None)
        cta = self._cta(self._pintar("cobro_2docs", mutar=falta))
        self.assertIn("<small>Resultado</small><b>—<em>falta un dato para calcularlo</em></b>", cta)
        self.assertNotIn("Ganamos", cta)
        self.assertNotIn("Perdimos", cta)

    def test_si_no_se_cobra_dice_no_se_cobra_nos_costo(self):
        cta = self._cta(self._pintar("garantia_nv"))
        self.assertIn('<div class="fm-res-i q"><small>No se cobra</small><b>nos costó $200.000</b></div>', cta)
        self.assertIn('<span class="fm-motivo">Garantía</span>', cta)
        self.assertNotIn("Ganamos", cta)
        self.assertNotIn("Perdimos", cta)
        self.assertIn("Autorizado por Daniel el 07/10/2026 14:00: falla de fábrica", cta, "la autorización con su argumento completo")

    def test_la_mini_tabla_trae_cobramos_nos_cobraron_queda_y_el_total(self):
        cta = self._cta(self._pintar("cobro_2docs"))
        tabla = cta.split('<table class="fm-neg">')[1].split("</table>")[0]
        self.assertIn("<th>Por negocio</th><th>Cobramos</th><th>Nos cobraron</th><th>Queda</th>", tabla)
        self.assertEqual(tabla.count("<tr>") + tabla.count('<tr class="tot">'), 4, "encabezado + instalación + despacho + total")
        self.assertIn('<tr class="tot"><th scope="row">Total</th><td>$302.101</td><td>$250.000</td><td class="pos">+$52.101</td></tr>', tabla)
        self.assertIn("Instalación o servicio", tabla)
        self.assertIn("Despacho", tabla)

    def test_con_repuestos_instalados_la_tabla_los_suma_antes_del_total(self):
        def con_rep(rec, pan):
            f = rec["fin"]
            f["me_cobraron"].update(repuestos=40000, total=290000, repuestos_desglose={"bodega": 40000})
            f["queda"].update(repuestos=-40000, total=12101, pct=4.0)
        tabla = self._cta(self._pintar("cobro_2docs", mutar=con_rep)).split('<table class="fm-neg">')[1].split("</table>")[0]
        self.assertIn("Repuestos instalados", tabla)
        self.assertLess(tabla.index("Repuestos instalados"), tabla.index("Total"))
        self.assertIn("bodega $40.000", tabla)

    def test_de_donde_sale_lo_cobrado_dice_el_documento_el_servicio_y_el_despacho(self):
        cta = self._cta(self._pintar("cobro_2docs"))
        self.assertIn('<li class="o">Lo cobrado sale de la FCV 11439: servicio $252.101 · la BLV 23375: despacho $50.000</li>', cta)

    def test_el_valorizado_quien_lo_declaro_y_la_autorizacion_van_en_una_linea_cada_uno(self):
        def con_aut(rec, pan):
            rec["fin"]["valorizado"] = {"monto": 959000}
            rec["autorizaciones"] = [{"estado": "pendiente", "tipo_txt": "Otra", "resuelto_por_nombre": None},
                                     {"estado": "aprobada", "tipo_txt": "Cerrar sin documento", "motivo_txt": "", "resuelto_por_nombre": "Daniel Aguilar",
                                      "resuelto_at": "08/10/2026 11:00"}]
        html = self._pintar("cobro_2docs", declaro="Declaradas por Aarón Urbina el 08/10 12:30", mutar=con_aut)
        det = self._cta(html).split('<ul class="fm-det">')[1].split("</ul>")[0]
        self.assertEqual(det.count("<li"), 4)
        self.assertIn("Valorizado (referencia): <b>$959.000</b> · no se suma a lo cobrado", det)
        self.assertIn("Costos declarados por Aarón Urbina el 08/10 12:30", det)
        self.assertIn("Autorización: Cerrar sin documento, aprobada por Daniel Aguilar el 08/10/2026 11:00", det)
        self.assertNotIn("Otra", det, "solo cuenta la autorización aprobada")

    def test_sin_declaracion_ni_valorizado_no_hay_lineas_vacias(self):
        det = self._cta(self._pintar("cobro_2docs")).split('<ul class="fm-det">')[1].split("</ul>")[0]
        self.assertEqual(det.count("<li"), 1, "solo de dónde sale lo cobrado")

    def test_el_cobro_escrito_a_mano_dice_que_no_viene_de_un_documento(self):
        def a_mano(rec, pan):
            rec["fin"]["cobre"]["fuente"] = "escrito a mano"
            pan["documentos"] = []
        cta = self._cta(self._pintar("sin_docs", mutar=a_mano))
        self.assertIn("Lo cobrado: escrito a mano (no viene de un documento de Random)", cta)

    def test_la_interna_no_muestra_ni_ganamos_ni_autorizacion(self):
        cta = self._cta(self._pintar("interna_cerrada"))
        self.assertIn("<small>No se cobra</small>", cta)
        self.assertIn("nos costó $0", cta)
        self.assertNotIn("Sin autorización", cta)
        self.assertNotIn("Ganamos", cta)

    def test_el_modal_de_cierre_usa_el_mismo_resumen(self):
        cta = self._cta(self._pintar("cobro_2docs", modo="modal"))
        self.assertIn("<small>Ganamos</small>", cta)
        self.assertIn("<small>Cobramos</small>", cta)

    def test_la_frase_del_modelo_queda_en_el_tooltip_y_los_numeros_no_se_cortan(self):
        html = self._pintar("cobro_2docs")
        self.assertIn('title="Cobré $302.101 − me cobraron $250.000 = quedan $52.101 (17,2 %)."', html)
        css = _leer("static/ot_fin_motor.css")
        self.assertIn(".fm-res-i.q{flex:1 1 230px;background:var(--c);color:#fff}", css)
        self.assertIn(".fm-det li.o{grid-column:1/-1}", css)
        self.assertNotIn("text-overflow", css.replace(" ", ""))
        self.assertNotIn("line-clamp", css)


class TestGuardarYAbrirElModal(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.det = _leer("templates/ot2/detalle.html")

    def test_guardar_refresca_el_bloque_unico_y_cierra_el_modal(self):
        det = self.det
        i = det.index("window.otdGuardarFinanzas = async function(){")
        cuerpo = det[i:det.index("window.otdActToggleTodo = function", i)]
        self.assertIn("if (window.OTFinMotor && OTFinMotor.refrescar) OTFinMotor.refrescar();", cuerpo)
        self.assertIn("setTimeout(window.otdFinModalCerrar, 700)", cuerpo)
        self.assertIn("window.otdFinModalCerrar = function(){", det)
        self.assertIn("window.otdFinModalAbrir = function(){", det)

    def test_quien_declaro_se_mantiene_al_dia_en_el_bloque_unico(self):
        det = self.det
        self.assertIn("document.querySelectorAll('[data-fin-motor][data-modo=\"ficha\"]').forEach(function(m){ m.setAttribute('data-declaro', txt); })", det)
        tpl = _leer("templates/ot2/_fin_motor.html")
        self.assertIn('data-declaro="Declaradas por {{ v.finanzas_por or \'—\' }} el {{ v.finanzas_at | chile_fmt(\'%d/%m %H:%M\') }}"', tpl)

    def test_ir_a_finanzas_y_el_chip_documento_abren_el_modal_con_el_buscador_listo(self):
        det = self.det
        i = det.index("window.otdIrAFinanzas = function(){")
        cuerpo = det[i:det.index("window.otdFinAbrirSumar = function", i)]
        self.assertIn("document.getElementById('otdFinModal')", cuerpo)
        self.assertIn("shown.bs.modal", cuerpo)
        self.assertIn("window.otdFinAbrirSumar(true)", cuerpo)
        self.assertIn("document.getElementById('otdCardMotor') || card", cuerpo, "sin modal (OT cerrada) cae al bloque único como antes")
        self.assertEqual(det.count('onclick="otdIrAFinanzas()"'), 3, "los tres chips del encabezado siguen yendo a gestionar el documento")

    def test_tras_recargar_por_el_primer_documento_se_vuelve_al_modal(self):
        det = self.det
        i = det.index("_volver && _volver.vid === OTD_VID")
        cuerpo = det[i:i + 1500]
        self.assertIn("window.otdFinModalAbrir()", cuerpo)

    def test_un_boton_de_la_ficha_ya_no_depende_de_que_la_tarjeta_este_visible(self):
        # ningún código mide la tarjeta como si estuviera en la página sin tener la salida al bloque único
        det = self.det
        self.assertEqual(det.count("if (!card || !card.offsetParent) card = document.getElementById('otdCardMotor') || card;"), 2)


class TestTrabajoInternoYSinPermisos(_HarnessBase):
    def test_la_interna_no_ofrece_documento_ni_autorizacion_y_el_boton_de_editar_depende_solo_del_permiso(self):
        html = self._pintar("interna_cerrada", editar=BOTON_EDITAR, mutar=lambda rec, pan: pan.update(puede_editar=True))
        self.assertIn("Editar cobro y costos", html)
        for malo in ("pedirCero", "pedirCierre", "ligarDoc", "declararCobro"):
            self.assertNotIn(f'data-fm-act="{malo}"', html)
        visible = re.sub(r'title="[^"]*"', "", html)
        self.assertEqual(visible.count(TXT_INTERNA), 1, "a la vista una sola vez (la línea del cuerpo); el resto va en los tooltips")


if __name__ == "__main__":
    unittest.main()
