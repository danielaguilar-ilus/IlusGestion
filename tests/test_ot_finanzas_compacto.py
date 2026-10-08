"""«Finanzas y documentos de la OT» en UN SOLO bloque compacto (Daniel, 2026-10-08, con dos capturas de la OT-2026-00284, una OT
interna cerrada): «la finanza y finanzas y documentos de la OT te quedó mucho espacio; ahora es el momento de ahorrar espacio,
compactemos todo en un mismo lugar».

Antes había DOS bloques apilados que repetían la información: el motor nuevo (tarjetas altas, una caja punteada grande en las OT
internas, la cuenta en tres cajas) y la tarjeta vieja «Finanzas de la OT» (barra Cobré/Me cobraron/Queda, «Resultado de la OT» con tres
cajas, una lista de solo lectura). Ahora:

  1. Encabezado en UNA fila: título + Actualizar · Corregir finanzas (solo superadmin) · OT con finanzas dudosas.
  2. El recorrido de 6 pasos es UNA franja de 6 chips (círculo de estado + título corto + una línea de estado).
  3. Los contadores son una fila de chips chicos.
  4. La cuenta va en una línea «Cobré − Me cobraron = Queda», con el centro de costo (4 botones compactos) al lado.
  5. Cuerpo en dos columnas: «Documentos» (sin productos) | «Lo que nos costó» + «Modificar». Sin documentos NO hay caja punteada
     grande: una sola línea, y «Lo que nos costó» ocupa el ancho completo.
  6. La tarjeta vieja no se borra (REGLA #4.2): su vista de solo lectura y las tres cajas «Resultado» dejan de mostrarse, y la tarjeta
     queda como la sección «Editar finanzas» de quien puede editar.

Sin BD ni Flask: texto de las plantillas/JS/CSS, la plantilla del motor renderizada con jinja2 y el motor JS ejecutado en node con un
DOM mínimo.  Correr con:  py -m unittest tests.test_ot_finanzas_compacto
"""
import json
import os
import re
import shutil
import subprocess
import tempfile
import unittest

import jinja2

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TXT_INTERNA = "Trabajo interno: no necesita documento ni autorización."


def _leer(rel):
    with open(os.path.join(RAIZ, rel.replace("/", os.sep)), encoding="utf-8", newline="") as f:
        return f.read().replace("\r\n", "\n")


EXTRA_SUPERADMIN = ('<button type="button" class="fm-btn" onclick="otdFinCorrAbrir()"><i class="bi bi-pencil-square"></i> Corregir finanzas '
                    '<small>solo superadmin</small></button><a class="fm-btn" href="/ot/finanzas-dudosas">OT con finanzas dudosas</a>')


def _linea(desc, sku, monto):
    return {"descripcion": desc, "sku": sku, "monto": float(monto), "cantidad": 1}


def _saldo(lineas_s, lineas_d):
    def blk(arr):
        return {"hay_lineas": bool(arr), "saldo": sum(x["saldo"] for x in arr), "usado": 0.0, "lineas": arr}
    return {"servicio": blk([dict(sku=x["sku"], descripcion=x["descripcion"], monto=x["monto"], usado=0.0, saldo=x["monto"]) for x in lineas_s]),
            "despacho": blk([dict(sku=x["sku"], descripcion=x["descripcion"], monto=x["monto"], usado=0.0, saldo=x["monto"]) for x in lineas_d]),
            "usos": [], "omitido": ""}


def _doc(i, tipo_txt, tido, nudo, ser, des, cuenta, es_cobro=True, cat="cobro", saldo=True):
    d = {"id": i, "es_principal": i == 1, "es_cobro": es_cobro, "etiqueta": "", "asociado_por": "Víctor Betancur",
         "asociado_at": "14/09/2026 10:21", "fecha": "12/09/2026", "reemplazado_por_id": None, "cuenta": cuenta, "origen": "erp",
         "categoria": cat, "tipo_txt": tipo_txt, "titulo": f"{tido} {nudo}", "tido": tido, "nudo": nudo, "monto": 959000.0,
         "rut": "76.123.456-7", "rut_estado": "ok", "rut_justif": "",
         "zz_serv": float(sum(x["monto"] for x in ser)) or None, "zz_envio": float(sum(x["monto"] for x in des)) or None,
         "lineas": {"servicio": ser, "despacho": des, "productos": [_linea("Trotadora comercial T9", "TRO-T9", 450000)], "total": 959000.0}}
    if saldo and es_cobro and (ser or des):
        d["saldo"] = _saldo(ser, des)
    return d


SER = [_linea("Instalación de equipos de gimnasio", "ZZINSTALACION", 252101)]
DES = [_linea("Despacho a regiones", "ZZENVIO", 50000)]


def _fin(cobra=True, interna=False):
    if interna:
        return {"cobra": False, "cobertura": "interno", "cobertura_txt": "Trabajo interno: no se le cobra", "clase": "info",
                "frase": "Trabajo interno: no se le cobra. Nos costó $0.", "label": "Trabajo interno · nos costó $0", "avisos": [],
                "cobre": {"servicio": 0, "despacho": 0, "total": 0, "hay": True},
                "me_cobraron": {"tecnico": 0, "despacho": 0, "repuestos": 0, "total": 0, "falta_tecnico": False, "falta_despacho": False},
                "queda": {"servicio": 0, "despacho": 0, "repuestos": 0, "total": 0, "pct": None, "mostrar": True}, "valorizado": {"monto": None}}
    if not cobra:
        return {"cobra": False, "cobertura": "garantia", "cobertura_txt": "Garantía: no se le cobra", "clase": "info",
                "frase": "Garantía: no se le cobra. Nos costó $200.000.", "label": "Garantía · nos costó $200.000", "avisos": [],
                "cobre": {"servicio": 0, "despacho": 0, "total": 0, "hay": True},
                "me_cobraron": {"tecnico": 130000, "despacho": 70000, "repuestos": 0, "total": 200000, "falta_tecnico": False, "falta_despacho": False},
                "queda": {"servicio": -130000, "despacho": -70000, "repuestos": 0, "total": -200000, "pct": None, "mostrar": True}, "valorizado": {"monto": 959000}}
    return {"cobra": True, "cobertura": "cobra", "cobertura_txt": "Se le cobra al cliente", "clase": "ok",
            "frase": "Cobré $302.101 − me cobraron $250.000 = quedan $52.101 (17,2 %).", "label": "Margen sano", "avisos": [],
            "cobre": {"servicio": 252101, "despacho": 50000, "total": 302101, "hay": True},
            "me_cobraron": {"tecnico": 200000, "despacho": 50000, "repuestos": 0, "total": 250000, "falta_tecnico": False, "falta_despacho": False},
            "queda": {"servicio": 52101, "despacho": 0, "repuestos": 0, "total": 52101, "pct": 17.2, "mostrar": True}, "valorizado": {"monto": None}}


PASOS_OK = [
    {"n": 1, "titulo": "¿Se cobra?", "estado": "hecho", "texto": "Se le cobra al cliente"},
    {"n": 2, "titulo": "Documentos", "estado": "hecho", "texto": "FCV 11439, BLV 23375"},
    {"n": 3, "titulo": "Cobré", "estado": "hecho", "texto": "Servicio $252.101 + despacho $50.000 = $302.101"},
    {"n": 4, "titulo": "Me cobró el proveedor", "estado": "hecho", "texto": "Instalación $200.000 + despacho $50.000 = $250.000"},
    {"n": 5, "titulo": "Margen", "estado": "hecho", "texto": "Cobré $302.101 − me cobraron $250.000 = quedan $52.101 (17,2 %).", "clase": "ok"},
    {"n": 6, "titulo": "Revisión al cerrar", "estado": "pendiente", "texto": "Puede cerrar: documento de cobro validado"}]
PASOS_INT = [
    {"n": 1, "titulo": "¿Se cobra?", "estado": "no_aplica", "texto": "Trabajo interno: no se le cobra a nadie"},
    {"n": 2, "titulo": "Documentos", "estado": "no_aplica", "texto": TXT_INTERNA},
    {"n": 3, "titulo": "Cobré", "estado": "no_aplica", "texto": "Trabajo interno: no hay cobro que declarar"},
    {"n": 4, "titulo": "Me cobró el proveedor", "estado": "hecho", "texto": "Instalación $0 + despacho $0 = $0"},
    {"n": 5, "titulo": "Margen", "estado": "hecho", "texto": "Trabajo interno: no se le cobra. Nos costó $0. → centro sstt", "clase": "info"},
    {"n": 6, "titulo": "Revisión al cerrar", "estado": "hecho", "texto": "Cerrada el 07/10/2026 14:22 · " + TXT_INTERNA}]
PASOS_SIN_DOC = [
    {"n": 1, "titulo": "¿Se cobra?", "estado": "hecho", "texto": "Se le cobra al cliente"},
    {"n": 2, "titulo": "Documentos", "estado": "falta", "texto": "Sin documento validado del ERP"},
    {"n": 3, "titulo": "Cobré", "estado": "falta", "texto": "Falta lo que cobraste (servicio y despacho)"},
    {"n": 4, "titulo": "Me cobró el proveedor", "estado": "hecho", "texto": "Instalación $200.000 + despacho $0 = $200.000"},
    {"n": 5, "titulo": "Margen", "estado": "falta", "texto": "Falta el documento de cobro."},
    {"n": 6, "titulo": "Revisión al cerrar", "estado": "falta",
     "texto": "Se necesita al menos un documento de cobro de Random (factura, boleta o nota de venta) o la autorización de Daniel."}]
CENTROS = [{"v": "sstt", "n": "Servicio Técnico"}, {"v": "logistica", "n": "Logística"}, {"v": "comercial", "n": "Comercial"},
           {"v": "marketing", "n": "Marketing"}]


def _caso(nombre):
    """(recorrido, panorama) de los cuatro casos del pedido de Daniel."""
    if nombre == "interna_cerrada":
        docs, interna, fin, pasos, cerrada = [], True, _fin(interna=True), PASOS_INT, True
    elif nombre == "garantia_nv":
        docs = [_doc(1, "Nota de venta", "NVV", "4521", SER, DES, "referencia_garantia", es_cobro=False, cat="nota_venta", saldo=False)]
        interna, fin, cerrada = False, _fin(cobra=False), False
        pasos = [dict(p) for p in PASOS_OK]
        pasos[0] = {"n": 1, "titulo": "¿Se cobra?", "estado": "hecho",
                    "texto": "$0 · Garantía: no se le cobra · Autorizado por Daniel el 07/10/2026 14:00: falla de fábrica cubierta por la garantía"}
    elif nombre == "cobro_2docs":
        docs = [_doc(1, "Factura", "FCV", "11439", SER, [], "servicio"), _doc(2, "Boleta", "BLV", "23375", [], DES, "despacho")]
        interna, fin, pasos, cerrada = False, _fin(), PASOS_OK, False
    else:   # sin_docs
        docs, interna, fin, pasos, cerrada = [], False, _fin(), PASOS_SIN_DOC, False
    rec = {"ok": True, "numero_ot": "OT-2026-00284", "cliente": "" if interna else "Gimnasio de prueba", "interna": interna,
           "puede_pedir_autorizacion": not interna, "estado": "cerrada" if cerrada else "en_curso", "cerrada": cerrada,
           "pasos": pasos, "fin": fin, "solicitud_pendiente": None, "autorizaciones": [],
           "cobro_cero": {"motivo": "garantia", "motivo_txt": "Garantía", "constancia": "Autorizado por Daniel el 07/10/2026 14:00: falla de fábrica"}
           if nombre == "garantia_nv" else {}, "superadmin": True}
    cont = {"total": len(docs), "facturas": sum(d["categoria"] == "cobro" for d in docs), "notas_venta": sum(d["categoria"] == "nota_venta" for d in docs),
            "cotizaciones": 0, "servicios": sum(len(d["lineas"]["servicio"]) for d in docs), "despachos": sum(len(d["lineas"]["despacho"]) for d in docs),
            "otros": 0, "docs_con_servicio": sum(bool(d.get("saldo") and d["saldo"]["servicio"]["hay_lineas"]) for d in docs),
            "docs_con_despacho": sum(bool(d.get("saldo") and d["saldo"]["despacho"]["hay_lineas"]) for d in docs),
            "docs_usados_en_otras": 0, "docs_saldo_agotado": 0}
    regs = [{"clave": "tecnico", "rotulo": "Instalación o servicio del técnico / proveedor", "detalle": "Técnico Demo · proveedor externo",
             "monto": float(fin["me_cobraron"]["tecnico"]), "falta": False}]
    if fin["me_cobraron"]["despacho"]:
        regs.append({"clave": "despacho", "rotulo": "Despacho del proveedor", "detalle": "Flete o courier que nos cobraron",
                     "monto": float(fin["me_cobraron"]["despacho"]), "falta": False})
    pan = {"ok": True, "numero_ot": "OT-2026-00284", "cliente": rec["cliente"], "interna": interna, "contadores": cont, "documentos": docs,
           "saldo": None, "costos": {"registros": regs, "total": fin["me_cobraron"]["total"], "sin_costo": 0},
           "centro": {"valor": "sstt", "nombre": "Servicio Técnico", "opciones": CENTROS},
           "puede_editar": not cerrada, "puede_regularizar": True, "superadmin": True, "cerrada": cerrada}
    return rec, pan


class _Base(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.node = shutil.which("node")
        if not cls.node:
            raise unittest.SkipTest("node no está instalado")
        cls.tmp = tempfile.mkdtemp(prefix="motor_compacto_")
        cls.harness = os.path.join(cls.tmp, "harness.js")
        with open(cls.harness, "w", encoding="utf-8") as f:
            f.write(r"""
const fs = require('fs');
const datos = JSON.parse(fs.readFileSync(process.argv[3], 'utf8'));
global.window = global;
const el = {
  innerHTML: '', classList: { add() {}, remove() {} },
  getAttribute(k) { return { 'data-vid': '461', 'data-modo': process.argv[4] || 'ficha' }[k] || null; },
  addEventListener() {}, closest() { return null; }, contains() { return true; },
  querySelector(sel) { return (sel.indexOf('data-fm-extra') >= 0 && datos.extra) ? { innerHTML: datos.extra } : null; }
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

    def _pintar(self, caso, modo="ficha", extra="", mutar=None):
        rec, pan = _caso(caso)
        if mutar:
            mutar(rec, pan)
        datos = os.path.join(self.tmp, "datos.json")
        with open(datos, "w", encoding="utf-8") as f:
            json.dump({"recorrido": rec, "panorama": pan, "extra": extra}, f)
        r = subprocess.run([self.node, self.harness, os.path.join(RAIZ, "static", "ot_fin_motor.js"), datos, modo],
                           capture_output=True, text=True, encoding="utf-8", timeout=60)
        self.assertEqual(r.returncode, 0, r.stderr)
        return r.stdout


class TestEncabezadoUnico(_Base):
    def test_el_encabezado_es_una_fila_con_los_tres_botones(self):
        html = self._pintar("cobro_2docs", extra=EXTRA_SUPERADMIN)
        cab = html.split('<div class="fm-head">')[1].split('<section class="fm-s1">')[0]
        self.assertIn('<div class="fm-head-a">', cab)
        botones = cab.split('<div class="fm-head-a">')[1]
        i_act, i_cor, i_dud = botones.index("Actualizar"), botones.index("Corregir finanzas"), botones.index("OT con finanzas dudosas")
        self.assertLess(i_act, i_cor)
        self.assertLess(i_cor, i_dud)
        self.assertEqual(botones.count("fm-btn"), 3, "los tres botones, todos .fm-btn (misma altura)")
        self.assertIn('data-fm-act="recargar"', botones)
        self.assertIn("otdFinCorrAbrir()", botones)
        self.assertIn('href="/ot/finanzas-dudosas"', botones)

    def test_sin_extra_solo_actualizar_y_nunca_en_el_modal(self):
        self.assertNotIn("Corregir finanzas", self._pintar("cobro_2docs"))
        self.assertNotIn("Corregir finanzas", self._pintar("cobro_2docs", modo="modal", extra=""))
        modal = self._pintar("cobro_2docs", modo="modal")
        self.assertIn("Finanzas y documentos para cerrar", modal)
        self.assertIn('data-fm-act="recargar"', modal)

    def test_la_plantilla_dibuja_los_botones_solo_para_superadmin_y_solo_en_la_ficha(self):
        env = jinja2.Environment(loader=jinja2.FileSystemLoader(os.path.join(RAIZ, "templates")), autoescape=True)
        env.globals["url_for"] = lambda ep, **kw: "/static/" + kw.get("filename", "")
        tpl = env.get_template("ot2/_fin_motor.html")
        sa = tpl.render(fm_vid=7, fm_modo="ficha", fm_assets=False, permissions={"superadmin": True})
        self.assertIn("<template data-fm-extra>", sa)
        self.assertIn("Corregir finanzas", sa)
        self.assertIn("/ot/finanzas-dudosas", sa)
        self.assertIn("otdFinCorrAbrir()", sa)
        self.assertNotIn("<template data-fm-extra>", tpl.render(fm_vid=7, fm_modo="ficha", fm_assets=False, permissions={"superadmin": False}))
        self.assertNotIn("<template data-fm-extra>", tpl.render(fm_vid=7, fm_modo="ficha", fm_assets=False))
        self.assertNotIn("<template data-fm-extra>", tpl.render(fm_vid=7, fm_modo="modal", fm_assets=False, permissions={"superadmin": True}))

    def test_las_funciones_de_los_botones_viven_en_la_ficha_y_no_se_duplican_en_la_tarjeta_vieja(self):
        det = _leer("templates/ot2/detalle.html")
        self.assertIn("async function otdFinCorrAbrir(){", det)
        self.assertIn('id="otdModalFinCorr"', det)
        self.assertNotIn('class="otd-fincorr-bar"', det.replace("#otdCardFinanzas .otd-fincorr-bar", ""),
                         "los botones se mudaron al encabezado del bloque único")
        self.assertNotIn('onclick="otdFinCorrAbrir()"', det)
        self.assertIn('onclick="otdFinCorrAbrir()"', _leer("templates/ot2/_fin_motor.html"))


class TestFranjaDeSeisChips(_Base):
    def test_seis_chips_con_circulo_titulo_corto_y_una_linea_de_estado(self):
        html = self._pintar("cobro_2docs")
        franja = html.split('<ol class="fm-pasos">')[1].split("</ol>")[0]
        chips = franja.split('<li class="fm-paso ')[1:]
        self.assertEqual(len(chips), 6)
        for n, chip in enumerate(chips, 1):
            self.assertIn('<span class="fm-circ">', chip)
            self.assertIn(f"<b>{n}. ", chip)
            self.assertEqual(chip.count('class="fm-paso-e"'), 1, "UNA línea de estado por chip")
        # lo resuelto se resume: la cifra, no la frase entera (la frase entera queda en el tooltip)
        self.assertIn('<span class="fm-paso-e">$302.101</span>', franja)
        self.assertIn('<span class="fm-paso-e">$250.000</span>', franja)
        self.assertIn('<span class="fm-paso-e">Queda $52.101 · 17,2 %</span>', franja)
        self.assertIn("Servicio $252.101 + despacho $50.000 = $302.101", franja, "el texto completo va en el tooltip")

    def test_la_accion_del_paso_es_un_boton_chico_dentro_del_chip_solo_si_falta(self):
        ok = self._pintar("cobro_2docs")
        self.assertNotIn("fm-btn-chip", ok.split('<ol class="fm-pasos">')[1].split("</ol>")[0])
        falta = self._pintar("sin_docs")
        franja = falta.split('<ol class="fm-pasos">')[1].split("</ol>")[0]
        for accion in ("ligarDoc", "pedirCierre", "declararCobro"):
            self.assertIn(f'class="fm-btn fm-btn-chip', franja)
            self.assertIn(f'data-fm-act="{accion}"', franja)
        chip2 = franja.split('<li class="fm-paso ')[2]
        self.assertIn("fm-paso-a", chip2)
        self.assertIn("Agregar factura", chip2)
        # un mensaje largo se lee en su primera cláusula; el entero queda en el tooltip
        chip6 = franja.split('<li class="fm-paso ')[6]
        self.assertIn('<span class="fm-paso-e">Se necesita al menos un documento de cobro de Random</span>', chip6)
        self.assertIn("o la autorización de Daniel.", chip6)

    def test_css_de_la_franja_seis_columnas_en_escritorio_y_dos_en_celular(self):
        css = _leer("static/ot_fin_motor.css")
        self.assertIn("grid-template-columns:repeat(2,minmax(0,1fr))", css.split(".fm-pasos{")[1].split("}")[0])
        self.assertRegex(css, r"@container \(min-width:900px\)\{\.fm-pasos\{grid-template-columns:repeat\(6,minmax\(0,1fr\)\)\}\}")
        self.assertIn("min-height:48px", css.split(".fm-paso{")[1].split("}")[0], "chip compacto (~56 px con su texto)")
        self.assertIn("@media (max-width:560px)", css)
        self.assertIn("min-height:44px", css.split("@media (max-width:560px)")[-1], "botones táctiles de 44 px en el celular")

    def test_la_interna_resume_sus_chips_sin_ofrecer_nada(self):
        html = self._pintar("interna_cerrada")
        franja = html.split('<ol class="fm-pasos">')[1].split("</ol>")[0]
        self.assertNotIn("fm-btn", franja)
        self.assertIn('<span class="fm-paso-e">Trabajo interno</span>', franja)
        self.assertIn('<span class="fm-paso-e">No necesita documento</span>', franja)
        self.assertIn('<span class="fm-paso-e">Sin cobro</span>', franja)
        self.assertIn(TXT_INTERNA, franja, "el texto completo, en el tooltip")


class TestContadoresYCuenta(_Base):
    def test_los_contadores_son_una_fila_de_chips_chicos(self):
        html = self._pintar("cobro_2docs")
        fila = html.split('<div class="fm-tiles">')[1].split("</div>")[0]
        self.assertEqual(fila.count('<span class="fm-tile '), 11)
        for texto in ("Documentos involucrados", "Con instalación o servicio", "Con despacho", "Usados en otras OT", "Saldo agotado",
                      "Facturas y boletas", "Notas de venta", "Cotizaciones", "Servicios", "Despachos", "Otros documentos"):
            self.assertIn(texto, fila)
        css = _leer("static/ot_fin_motor.css")
        self.assertIn("display:flex;flex-wrap:wrap", css.split(".fm-tiles{")[1].split("}")[0])
        self.assertNotIn("font-size:2rem", css, "sin tarjetas grandes de contadores")

    def test_la_cuenta_va_en_una_linea_y_el_desglose_en_una_mini_tabla_de_dos_filas(self):
        html = self._pintar("cobro_2docs")
        cta = html.split('<section class="fm-cta ')[1].split("</section>")[0]
        i1, i2, i3 = cta.index("Cobré</small><b>$302.101"), cta.index("Me cobraron</small><b>$250.000"), cta.index("Queda</small><b>$52.101")
        self.assertTrue(i1 < i2 < i3)
        self.assertIn("<em>17,2 %</em>", cta)
        self.assertIn("Margen sano", cta)
        mini = cta.split('<table class="fm-neg">')[1].split("</table>")[0]
        self.assertEqual(mini.count("<tr>"), 3, "encabezado + 2 filas (servicio y despacho)")
        self.assertIn("Instalación o servicio", mini)
        self.assertIn("Despacho", mini)
        self.assertIn("+$52.101", mini)
        # ya no hay tres cajas «Resultado de la OT», ni la cuenta en tarjetas grandes, ni el total repetido
        self.assertNotIn("fm-cuenta-g", html)
        self.assertNotIn("Total que nos costó", html)
        self.assertNotIn("Resultado de la OT", html)

    def test_en_una_ot_que_no_se_cobra_dice_nos_costo_y_la_constancia_completa(self):
        html = self._pintar("garantia_nv")
        cta = html.split('<section class="fm-cta ')[1].split("</section>")[0]
        self.assertIn("Nos costó</small><b>$200.000", cta)
        self.assertIn("Garantía: no se cobra", cta)
        self.assertIn("Autorizado por Daniel el 07/10/2026 14:00: falla de fábrica", cta)
        self.assertNotIn("fm-neg", cta, "sin cobro no hay desglose Cobré/Queda por negocio")
        self.assertIn("Valorizado (referencia): <b>$959.000</b>", cta)

    def test_centro_de_costo_compacto_al_lado_de_la_cuenta(self):
        html = self._pintar("cobro_2docs")
        fila = html.split('<div class="fm-fila">')[1].split('<div class="fm-main">')[0]
        self.assertLess(fila.index('class="fm-cta '), fila.index('class="fm-cen"'))
        self.assertEqual(fila.count("data-fm-cc="), 4)
        self.assertNotIn("<small>El costo es nuestro</small>", fila, "icono + nombre, sin subtítulo (queda en el tooltip)")
        css = _leer("static/ot_fin_motor.css")
        cc = css.split(".fm-cc{")[1].split("}")[0]
        self.assertIn("min-height:44px", cc)
        self.assertIn("flex-direction:row", cc)
        self.assertRegex(css, r"@container \(min-width:900px\)\{\.fm-fila\{grid-template-columns:minmax\(0,1\.5fr\) minmax\(0,1fr\)\}\}")


class TestCuerpoYSinDocumentos(_Base):
    def test_con_documentos_dos_columnas_documentos_y_lo_que_nos_costo_con_modificar(self):
        html = self._pintar("cobro_2docs")
        cuerpo = html.split('<div class="fm-main">')[1]
        izq, der = cuerpo.split('<div class="fm-col">')
        self.assertIn("<h3>Documentos <small>2</small></h3>", izq)
        self.assertIn("FCV 11439", izq)
        self.assertIn("BLV 23375", izq)
        self.assertIn("<h3>Lo que nos costó</h3>", der)
        self.assertIn("<span>Modificar</span>", der)
        self.assertLess(der.index("Lo que nos costó"), der.index("Modificar"))
        css = _leer("static/ot_fin_motor.css")
        self.assertIn("@container (min-width:900px){.fm-main{grid-template-columns:minmax(0,1.5fr) minmax(0,1fr)}", css)

    def test_los_productos_ya_no_se_muestran_y_el_saldo_no_repite_las_lineas(self):
        js = _leer("static/ot_fin_motor.js")
        self.assertNotIn("grupoLineas('Productos'", js)
        html = self._pintar("cobro_2docs")
        self.assertNotIn("Trotadora", html)
        self.assertNotIn("TRO-T9", html)
        fcv = html.split('<article class="fm-doc')[1]
        self.assertEqual(fcv.count("$252.101") - fcv.count("17,2"), fcv.count("$252.101"))
        self.assertIn("Servicio y despacho del documento", fcv)
        self.assertNotIn('<table class="fm-lt fm-lineas">', fcv, "con tabla de saldo, las líneas no se repiten arriba")

    def test_un_documento_sin_saldo_igual_muestra_sus_lineas_de_servicio_y_despacho(self):
        html = self._pintar("garantia_nv")
        self.assertIn('<table class="fm-lt fm-lineas">', html)
        self.assertIn(">Servicio</th>", html)
        self.assertIn(">Despacho</th>", html)
        self.assertIn("Instalación de equipos de gimnasio", html)
        self.assertIn("Despacho a regiones", html)
        self.assertIn("Referencia de la garantía: no se cobra", html)

    def test_la_interna_no_dibuja_la_caja_punteada_sino_una_linea_y_lo_que_nos_costo_a_todo_el_ancho(self):
        html = self._pintar("interna_cerrada")
        self.assertIn('<div class="fm-sindoc interna">', html)
        self.assertIn(TXT_INTERNA, html.split('<div class="fm-sindoc interna">')[1].split("</div>")[0])
        self.assertNotIn("fm-vacio", html)
        self.assertNotIn("Documentos <small>", html, "no hay columna de documentos")
        self.assertIn('<div class="fm-main fm-main-uno">', html)
        self.assertIn("<h3>Lo que nos costó</h3>", html.split('<div class="fm-main fm-main-uno">')[1])
        css, js = _leer("static/ot_fin_motor.css"), _leer("static/ot_fin_motor.js")
        self.assertNotIn(".fm-vacio", css)
        self.assertNotIn("fm-vacio", js)
        self.assertNotIn("2px dashed", css, "ya no hay caja punteada grande")
        self.assertIn(".fm-main.fm-main-uno{grid-template-columns:minmax(0,1fr)}", css)
        self.assertIn(".fm-main-uno .fm-cs{grid-template-columns:repeat(2,minmax(0,1fr))}", css)

    def test_el_trabajo_interno_no_repite_su_texto_en_la_cuenta(self):
        html = self._pintar("interna_cerrada")
        visible = re.sub(r'title="[^"]*"', "", html)
        self.assertEqual(visible.count(TXT_INTERNA), 1, "una sola vez a la vista (la línea del cuerpo)")

    def test_un_cliente_sin_documentos_ve_una_linea_con_el_boton_de_ligar_y_sin_duplicarlo(self):
        html = self._pintar("sin_docs")
        linea = html.split('<div class="fm-sindoc">')[1].split("</div>")[0]
        self.assertIn("Sin documentos aún", linea)
        self.assertIn('data-fm-act="ligarDoc"', linea)
        self.assertNotIn("fm-vacio", html)
        mod = html.split('<div class="fm-acciones">')[1].split("</div>")[0]
        self.assertNotIn('data-fm-act="ligarDoc"', mod, "el botón de ligar ya está en la línea: no se repite en «Modificar»")
        self.assertIn('data-fm-act="pedirCero"', mod)
        self.assertIn('<div class="fm-main fm-main-uno">', html)

    def test_la_interna_solo_ofrece_corregir_el_costo_del_proveedor(self):
        html = self._pintar("interna_cerrada", mutar=lambda rec, pan: pan.update(puede_editar=True))
        mod = html.split('<div class="fm-acciones">')[1].split("</div>")[0]
        self.assertIn('data-fm-act="corregirProv"', mod)
        for malo in ("pedirCero", "pedirCierre", "ligarDoc", "declararCobro"):
            self.assertNotIn(f'data-fm-act="{malo}"', html)


class TestLaTarjetaViejaNoSeMuestraDuplicada(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.det = _leer("templates/ot2/detalle.html")
        cls.js = _leer("static/ot_fin_motor.js")

    def test_un_solo_bloque_y_la_tarjeta_vieja_va_debajo_y_solo_para_gestion(self):
        det = self.det
        i_motor, i_vieja = det.index('id="otdCardMotor"'), det.index('id="otdCardFinanzas"')
        self.assertLess(i_motor, i_vieja)
        i_if = det.rindex("{% if not es_tecnico %}", 0, i_motor)
        self.assertLess(i_motor - i_if, 6000, "REGLA #19: el bloque está dentro del {% if not es_tecnico %} de la ficha")
        self.assertEqual(det.count('id="otdCardMotor"'), 1)
        self.assertEqual(det.count('id="otdCardFinanzas"'), 1)
        # lo que repetía al motor sale del flujo normal; los ids siguen (REGLA #4.2)
        for ident in ("otdFinResultado", "otdFinResInst", "otdFinResDesp", "otdFinResTot", "otdFinFrase", "otdFinBar", "otdFinBarCobro",
                      "otdFinBarCosto", "otdFinMargen", "otdFinBtnGuardar", "otdFinBtnCorregirBar", "otdCcGrid", "otdFinCentroCosto",
                      "otdFinPasoCobro", "otdFinFaltan", "otdFinFaltanTxt"):
            self.assertIn(f'id="{ident}"', det, ident)
        for fn in ("window.otdFinCuenta = function", "window.otdFinPintar = function", "window.otdFinVivo = vivo",
                   "window.otdIrAFinanzas = function", "window.otdCorregirFinanzas = async function"):
            self.assertIn(fn, det, fn)

    def test_la_vista_de_solo_lectura_vieja_no_se_muestra_con_el_motor_presente(self):
        det = self.det
        self.assertIn("class=\"otd-card{{ '' if puede_metadata else ' fin-ro' }}\" id=\"otdCardFinanzas\"", det)
        self.assertIn("#otdCardFinanzas.fin-ro:not(.fin-motor-fallo){display:none}", det)
        # sigue en la página (REGLA #4.2): su contenido y sus ids están, solo no se muestra
        self.assertIn('<div class="row"><span>¿Se le cobra?</span>', det)
        self.assertIn("<span>Centro de costo</span>", det)
        # si el motor no pudo leer, la vista vieja vuelve a verse
        self.assertIn("classList.add('fin-motor-fallo')", self.js)
        self.assertIn("classList.remove('fin-motor-fallo')", self.js)

    def test_las_tres_cajas_resultado_se_ocultan_y_la_cuenta_las_reemplaza(self):
        det = self.det
        self.assertIn("#otdCardFinanzas:not(.fin-motor-fallo) #otdFinResultado{display:none}", det)
        self.assertIn('<div class="otd-fres" id="otdFinResultado">', det, "el cuadro sigue en la página y sigue pintándose")
        self.assertIn("window.otdFinPintar(F.fin)", det)

    def test_la_barra_duplicada_solo_aparece_mientras_se_escribe_tambien_en_el_celular(self):
        det = self.det
        regla = "#otdCardFinanzas:not(.fin-editando) .otd-fbar-cuenta,"
        self.assertIn(regla, det)
        i = det.index(regla)
        antes = det[:i]
        self.assertGreater(i, antes.rindex("#otdCardFinanzas.fin-ro:not(.fin-motor-fallo)"))
        self.assertNotIn("@media (min-width:769px){\n  #otdCardFinanzas:not(.fin-editando)", det, "ya no solo en escritorio")
        self.assertIn("#otdCardFinanzas.fin-editando .otd-fbar-cuenta::before", det)
        self.assertIn("Con lo que estás escribiendo", det)

    def test_la_tarjeta_de_edicion_conserva_su_formulario_y_se_llama_editar_finanzas(self):
        det = self.det
        self.assertIn("{{ 'Editar finanzas' if puede_metadata else 'Finanzas de la OT' }}", det)
        for ident in ("otdFinZzServ", "otdFinZzEnvio", "otdFinCosto", "otdFinCostoProveedor", "otdFinCostoDespacho", "otdFinValorizado",
                      "otdFinProveedorNombre", "otdErpDocQ", "otdBtnLeerZz"):
            self.assertIn(f'id="{ident}"', det, ident)

    def test_el_aviso_de_lo_que_falta_para_cerrar_y_la_factura_del_proveedor_suben_al_bloque_unico(self):
        det = self.det
        i_motor, i_vieja = det.index('id="otdCardMotor"'), det.index('id="otdCardFinanzas"')
        self.assertTrue(i_motor < det.index('id="otdFinFaltan"') < i_vieja)
        self.assertTrue(i_motor < det.index("{{ facprov_bloque(") < i_vieja, "la factura del proveedor se ve en el bloque único (OT cerrada)")
        self.assertLess(det.index("{% macro facprov_bloque"), i_motor, "el macro se define antes de usarse")
        self.assertIn("#otdCardMotor [hidden]{display:none !important}", _leer("static/ot_fin_motor.css"))

    def test_ir_a_finanzas_cae_en_el_bloque_unico_cuando_la_tarjeta_de_edicion_no_se_ve(self):
        det = self.det
        self.assertEqual(det.count("document.getElementById('otdCardMotor') || card"), 2)
        self.assertIn("document.getElementById('otdCardMotor') || c", self.js)

    def test_el_motor_esta_en_la_ficha_y_en_el_modal_de_cierre_con_el_mismo_componente(self):
        self.assertEqual(self.det.count("ot2/_fin_motor.html"), 2)
        css = _leer("static/ot_fin_motor.css")
        self.assertIn("container-type:inline-size", css, "el diseño responde al ancho del bloque, no al de la pantalla")
        self.assertIn("@container (min-width:900px)", css)
        self.assertNotIn("line-clamp", css)
        self.assertNotIn("text-overflow:ellipsis", css.replace(" ", ""))

    def test_nada_desplegable_para_la_informacion_principal(self):
        self.assertNotIn("<details", self.js)
        self.assertNotIn("<details", _leer("templates/ot2/_fin_motor.html"))


if __name__ == "__main__":
    unittest.main()
