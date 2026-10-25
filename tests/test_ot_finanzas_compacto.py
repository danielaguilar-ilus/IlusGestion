"""«Finanzas y documentos de la OT» en UN SOLO bloque compacto (Daniel, 2026-10-08, con dos capturas de la OT-2026-00284, una OT
interna cerrada): «la finanza y finanzas y documentos de la OT te quedó mucho espacio; ahora es el momento de ahorrar espacio,
compactemos todo en un mismo lugar».

Antes había DOS bloques apilados que repetían la información: el motor nuevo (tarjetas altas, una caja punteada grande en las OT
internas, la cuenta en tres cajas) y la tarjeta vieja «Finanzas de la OT» (barra Cobré/Me cobraron/Queda, «Resultado de la OT» con tres
cajas, una lista de solo lectura). Ahora:

  1. Encabezado en UNA fila: título + Actualizar · Corregir finanzas (solo superadmin) · OT con finanzas dudosas.
  2. El recorrido de 6 pasos es UNA franja de 6 chips (círculo de estado + título corto + una línea de estado).
  3. Los contadores son una fila de chips chicos.
  4. La cuenta va en una línea, con el centro de costo (4 botones compactos) al lado. (2026-10-08, segunda vuelta: la línea ahora dice
     «Cobramos $X · Nos cobraron $Y · Ganamos $Z (n %)» y el bloque se llama «Costos y documentación de la OT»: ver
     tests/test_ot_costos_documentacion.py.)
  5. Cuerpo en dos columnas: «Documentos» (sin productos) | «Lo que nos costó» + «Modificar». Sin documentos NO hay caja punteada
     grande: una sola línea, y «Lo que nos costó» ocupa el ancho completo.
  6. La tarjeta vieja no se borra (REGLA #4.2): su vista de solo lectura y las tres cajas «Resultado» dejan de mostrarse, y la tarjeta
     queda como la sección de edición de quien puede editar. (2026-10-08, segunda vuelta: esa sección salió de la página y es el modal
     «Editar cobro y costos»; ver tests/test_ot_costos_documentacion.py.)

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
if (process.argv[5] === 'anexo') global.otdCostoDesdeAnexo = function () {};       // la ficha de la OT define esta función (templates/ot2/detalle.html)
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

    def _pintar(self, caso, modo="ficha", extra="", mutar=None, con_funcion_anexo=False):
        rec, pan = _caso(caso)
        if mutar:
            mutar(rec, pan)
        datos = os.path.join(self.tmp, "datos.json")
        with open(datos, "w", encoding="utf-8") as f:
            json.dump({"recorrido": rec, "panorama": pan, "extra": extra}, f)
        r = subprocess.run([self.node, self.harness, os.path.join(RAIZ, "static", "ot_fin_motor.js"), datos, modo, "anexo" if con_funcion_anexo else ""],
                           capture_output=True, text=True, encoding="utf-8", timeout=60)
        self.assertEqual(r.returncode, 0, r.stderr)
        return r.stdout


class TestEncabezadoUnico(_Base):
    def test_el_encabezado_toca_la_plata_por_un_solo_camino_y_lo_avanzado_va_en_el_pie(self):
        """2026-10-25 (Daniel: «Editar costos, Corregir finanzas, OT con finanzas dudosas… ¿no hacen lo mismo?»): con la OT ABIERTA el encabezado trae
        Actualizar (y «Editar cobro y costos» si se puede); «Corregir finanzas» y «OT con finanzas dudosas» van en un pie «Solo superadmin», cada uno con
        una línea que dice qué hace."""
        html = self._pintar("cobro_2docs", extra=EXTRA_SUPERADMIN)
        cab = html.split('<div class="fm-head">')[1].split('<div class="fm-top">')[0]
        botones = cab.split('<div class="fm-head-a">')[1]
        self.assertIn("Actualizar", botones)
        self.assertEqual(botones.count("fm-btn"), 1, "solo Actualizar (ningún botón de superadmin arriba)")
        self.assertNotIn("Corregir finanzas", botones)
        self.assertIn('data-fm-act="recargar"', botones)
        pie = html.split('<div class="fm-pie">')[1].split("</div>")[0] + "</div>"
        self.assertIn("Solo superadmin", pie)
        i_cor, i_dud = pie.index("Corregir finanzas"), pie.index("OT con finanzas dudosas")
        self.assertLess(i_cor, i_dud)
        self.assertIn("otdFinCorrAbrir()", pie)
        self.assertIn('href="/ot/finanzas-dudosas"', pie)
        # cada uno explica qué hace (así no se confunden con «Editar cobro y costos»)
        self.assertIn("reabre lo ya declarado y deja registrado quién lo cambió y por qué", pie)
        self.assertIn("lista todas las OT con la plata por revisar", pie)

    def test_con_la_ot_cerrada_corregir_finanzas_sube_al_encabezado_y_el_pie_solo_trae_las_dudosas(self):
        html = self._pintar("cobro_2docs", extra=EXTRA_SUPERADMIN, mutar=lambda rec, pan: pan.update(cerrada=True, puede_editar=False))
        botones = html.split('<div class="fm-head">')[1].split('<div class="fm-top">')[0].split('<div class="fm-head-a">')[1]
        self.assertLess(botones.index("Actualizar"), botones.index("Corregir finanzas"))
        pie = html.split('<div class="fm-pie">')[1].split("</div>")[0]
        self.assertNotIn("Corregir finanzas", pie)
        self.assertIn("OT con finanzas dudosas", pie)

    def test_el_pie_de_superadmin_no_existe_sin_el_template_ni_en_el_modal(self):
        self.assertNotIn('class="fm-pie"', self._pintar("cobro_2docs"))
        self.assertNotIn('class="fm-pie"', self._pintar("cobro_2docs", modo="modal", extra=EXTRA_SUPERADMIN))

    def test_sin_extra_solo_actualizar_y_nunca_en_el_modal(self):
        self.assertNotIn("Corregir finanzas", self._pintar("cobro_2docs"))
        self.assertNotIn("Corregir finanzas", self._pintar("cobro_2docs", modo="modal", extra=""))
        modal = self._pintar("cobro_2docs", modo="modal")
        # 2026-10-08: el bloque pasó a llamarse «Costos y documentación» (Daniel: «COSTOS Y DOCUMENTACIÓN de la orden de trabajo»)
        self.assertIn("Costos y documentación para cerrar", modal)
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


class TestElRecorridoEnSeisFilas(_Base):
    """2026-10-25 (Daniel, con la OT-2026-00354: «contradicciones y muy poco explicativo… entiendo que es un paso a paso, pero lo necesito un poco más
    específico»): la franja de seis chips con texto cortado pasó a SEIS FILAS con el texto específico y COMPLETO de cada paso."""

    def test_seis_filas_con_circulo_titulo_y_el_texto_completo(self):
        html = self._pintar("cobro_2docs")
        franja = html.split('<ol class="fm-pasos">')[1].split("</ol>")[0]
        filas = franja.split('<li class="fm-paso ')[1:]
        self.assertEqual(len(filas), 6)
        for n, fila in enumerate(filas, 1):
            self.assertIn('<span class="fm-circ">', fila)
            self.assertIn(f"<b>{n}. ", fila)
            self.assertEqual(fila.count('class="fm-paso-e"'), 1, "UN texto por paso")
        # el texto completo del servidor, sin recortar ni resumir a una cifra (REGLA #15: nada se corta)
        self.assertIn('<span class="fm-paso-e">Servicio $252.101 + despacho $50.000 = $302.101</span>', franja)
        self.assertIn('<span class="fm-paso-e">Instalación $200.000 + despacho $50.000 = $250.000</span>', franja)
        self.assertIn('<span class="fm-paso-e">Cobré $302.101 − me cobraron $250.000 = quedan $52.101 (17,2 %).</span>', franja)
        self.assertNotIn("title=", franja, "ya no hace falta un tooltip con lo que no cabía: todo está a la vista")

    def test_el_paso_dice_su_estado_con_el_color_de_guiar_sin_gritar(self):
        franja = self._pintar("sin_docs").split('<ol class="fm-pasos">')[1].split("</ol>")[0]
        self.assertIn('<li class="fm-paso hecho" data-paso="1">', franja)
        self.assertIn('<li class="fm-paso falta" data-paso="2">', franja)
        css = _leer("static/ot_fin_motor.css")
        self.assertRegex(css, r"\.fm-paso\.falta\{border-color:#fde68a;background:var\(--fm-ambar-f\)\}")        # lo que TOCA: ámbar (no rojo)
        self.assertRegex(css, r"\.fm-paso\.pendiente\{border-color:#bfdbfe;background:#eff6ff\}")                  # lo que ESPERA: azul
        self.assertRegex(css, r"\.fm-paso\.hecho\{border-color:#bbf7d0;")                                          # lo hecho: verde
        self.assertIn(".fm-paso-a .fm-btn-pri{background:#2563eb;", css)                                            # los botones de «te toca»: azules

    def test_la_accion_del_paso_es_un_boton_chico_dentro_de_su_fila_solo_si_falta(self):
        ok = self._pintar("cobro_2docs")
        self.assertNotIn("fm-btn-chip", ok.split('<ol class="fm-pasos">')[1].split("</ol>")[0])
        falta = self._pintar("sin_docs")
        franja = falta.split('<ol class="fm-pasos">')[1].split("</ol>")[0]
        for accion in ("ligarDoc", "pedirCierre", "declararCobro"):
            self.assertIn(f'class="fm-btn fm-btn-chip', franja)
            self.assertIn(f'data-fm-act="{accion}"', franja)
        fila2 = franja.split('<li class="fm-paso ')[2]
        self.assertIn("fm-paso-a", fila2)
        self.assertIn("Agregar factura", fila2)
        # el mensaje largo se lee ENTERO en la fila (antes se cortaba en su primera cláusula)
        fila6 = franja.split('<li class="fm-paso ')[6]
        self.assertIn('<span class="fm-paso-e">Se necesita al menos un documento de cobro de Random (factura, boleta o nota de venta) o la autorización de Daniel.</span>', fila6)

    def test_css_el_recorrido_es_una_lista_y_la_cuenta_pasa_primero_en_una_columna(self):
        css = _leer("static/ot_fin_motor.css")
        lista = css.split(".fm-pasos{")[1].split("}")[0]
        self.assertIn("display:flex;flex-direction:column", lista)
        self.assertIn("min-height:48px", css.split(".fm-paso{")[1].split("}")[0])
        # dos columnas iguales en un bloque ancho (REGLA #16) y, en una sola columna, lo primero es la plata
        self.assertRegex(css, r"@container \(min-width:900px\)\{\.fm-top\{grid-template-columns:minmax\(0,1fr\) minmax\(0,1fr\)\}\}")
        self.assertIn("@container (max-width:899px){.fm-top .fm-cta{order:-1}}", css)
        self.assertIn("@media (max-width:560px)", css)
        self.assertIn("min-height:44px", css.split("@media (max-width:560px)")[-1], "botones táctiles de 44 px en el celular")
        self.assertIn(".fm-btn,.fm-btn-chip,.fm-pie .fm-btn{min-height:44px}", css)        # también los del pie de superadmin

    def test_la_interna_muestra_sus_pasos_completos_sin_ofrecer_nada(self):
        html = self._pintar("interna_cerrada")
        franja = html.split('<ol class="fm-pasos">')[1].split("</ol>")[0]
        self.assertNotIn("fm-btn", franja)
        self.assertIn("Trabajo interno: no se le cobra a nadie", franja)
        self.assertIn(TXT_INTERNA.rstrip("."), franja)
        self.assertIn("Trabajo interno: no hay cobro que declarar", franja)


class TestLaFraseDeArribaYElPaso2(_Base):
    """La frase dinámica (la dice el servidor, rec.resumen) y el paso «Factura»."""

    def test_la_frase_de_arriba_dice_si_esta_lista_o_que_falta(self):
        def con(estado, titulo):
            return lambda rec, pan: rec.update(resumen={"estado": estado, "titulo": titulo, "faltan": []})
        h = self._pintar("sin_docs", mutar=con("falta", "Falta la factura · lo que se cobró"))
        self.assertIn('<div class="fm-resumen falta" role="status"><i class="bi bi-exclamation-circle-fill"></i><b>Falta la factura · lo que se cobró</b></div>', h)
        h = self._pintar("cobro_2docs", mutar=con("listo", "Lista para cerrar"))
        self.assertIn('<div class="fm-resumen listo" role="status"><i class="bi bi-check-circle-fill"></i><b>Lista para cerrar</b></div>', h)
        h = self._pintar("cobro_2docs", mutar=con("espera", "Esperando a Daniel (solicitud N° 12)"))
        self.assertIn('fm-resumen espera', h)
        h = self._pintar("interna_cerrada", mutar=con("cerrada", "Cerrada"))
        self.assertIn('fm-resumen cerrada', h)
        # va justo debajo del encabezado y antes del recorrido
        h = self._pintar("sin_docs", mutar=con("falta", "x"))
        self.assertLess(h.index('class="fm-head"'), h.index('class="fm-resumen'))
        self.assertLess(h.index('class="fm-resumen'), h.index('class="fm-top"'))

    def test_si_el_servidor_no_manda_la_frase_no_se_inventa_una(self):
        self.assertNotIn('class="fm-resumen', self._pintar("cobro_2docs"))

    def test_si_lo_cobrado_pasa_del_saldo_de_la_factura_la_frase_no_dice_lista_para_cerrar(self):
        """El saldo lo mide el panorama (el recorrido no lo lee): el cierre se rechaza con ZZ_SALDO_CONSUMIDO aunque los seis pasos estén en verde."""
        def listo(extra_rec=None, excedido=True, **kw):
            def f(rec, pan):
                rec.update(resumen={"estado": "listo", "titulo": "Lista para cerrar", "faltan": []})
                rec.update(extra_rec or {})
                pan["saldo"] = {"excedido": excedido, "excesos": [], "acciones": [], "texto": "x"}
            return f
        h = self._pintar("cobro_2docs", mutar=listo())
        self.assertIn('<div class="fm-resumen falta" role="status"><i class="bi bi-exclamation-circle-fill"></i>'
                      '<b>Falta resolver el saldo de la factura: lo que se cobra pasa de lo que le queda</b></div>', h)
        self.assertNotIn("Lista para cerrar", h.split('class="fm-resumen')[1].split("</div>")[0])
        # sin exceso, todo como siempre
        self.assertIn("<b>Lista para cerrar</b>", self._pintar("cobro_2docs", mutar=listo(excedido=False)))
        # el cierre autorizado sin factura y la pendiente por falta de stock no corren ese candado en el servidor
        for via in ("autorizacion_cerrar", "pendiente_madre"):
            self.assertIn("<b>Lista para cerrar</b>", self._pintar("cobro_2docs", mutar=listo({"puerta": {"ok": True, "via": via}})), via)
        # una OT cerrada no se vuelve a juzgar
        cerrada = self._pintar("cobro_2docs", mutar=lambda rec, pan: (rec.update(cerrada=True, resumen={"estado": "cerrada", "titulo": "Cerrada", "faltan": []}),
                                                                    pan.update(saldo={"excedido": True, "excesos": [], "acciones": [], "texto": "x"})))
        self.assertIn("<b>Cerrada</b>", cerrada)

    def test_el_paso_2_se_llama_factura_en_el_servidor(self):
        app = _leer("app.py")
        self.assertIn('dict(n=2, titulo="Factura", **p2)', app)


class TestContadoresYCuenta(_Base):
    def test_los_contadores_viven_en_documentos_y_solo_se_muestran_los_que_dicen_algo(self):
        """2026-10-25 (Daniel: «no tantas tarjetas»): antes eran once chips fijos (con ceros y datos repetidos) arriba de todo; ahora van dentro de
        «Documentos» y solo los que tienen algo que decir."""
        html = self._pintar("cobro_2docs")
        self.assertNotIn("Documentos involucrados", html)
        docs = html.split("<h3>Documentos <small>2</small></h3>")[1]
        fila = docs.split('<div class="fm-tiles">')[1].split("</div>")[0]
        self.assertEqual(fila.count('<span class="fm-tile '), 3)
        for texto in ("Facturas y boletas", "Con instalación o servicio", "Con despacho"):
            self.assertIn(texto, fila)
        for cero in ("Notas de venta", "Cotizaciones", "Otros documentos", "Usados en otras OT", "Saldo agotado", "Servicios", "Despachos"):
            self.assertNotIn(cero, fila, "lo que está en cero no se dibuja")
        self.assertIn('<span class="fm-tile ok" title="Facturas y boletas: Documentos que cobran"><b>2</b>', fila)
        # el contador queda ANTES de los documentos y la fila no existe fuera de «Documentos»
        self.assertLess(docs.index('class="fm-tiles"'), docs.index("<article"))
        self.assertEqual(html.count('class="fm-tiles"'), 1)
        css = _leer("static/ot_fin_motor.css")
        self.assertIn("display:flex;flex-wrap:wrap", css.split(".fm-tiles{")[1].split("}")[0])
        self.assertNotIn("font-size:2rem", css, "sin tarjetas grandes de contadores")

    def test_una_nota_de_venta_y_el_aviso_de_saldo_si_aparecen_cuando_pasan(self):
        h = self._pintar("garantia_nv").split("<h3>Documentos <small>1</small></h3>")[1].split('<div class="fm-tiles">')[1].split("</div>")[0]
        self.assertIn("Notas de venta", h)
        self.assertIn("Falta agregar la factura", h)
        def usadas(rec, pan):
            pan["contadores"].update(docs_usados_en_otras=1, docs_saldo_agotado=1)
        h = self._pintar("cobro_2docs", mutar=usadas).split('<div class="fm-tiles">')[1].split("</div>")[0]
        self.assertIn("Usados en otras OT", h)
        self.assertIn("Saldo agotado", h)

    def test_la_cuenta_va_en_una_linea_y_el_desglose_en_una_mini_tabla_de_dos_filas(self):
        html = self._pintar("cobro_2docs")
        cta = html.split('<section class="fm-cta ')[1].split("</section>")[0]
        # 2026-10-08: con las palabras de Daniel: «Cobramos $X», «Nos cobraron $Y» y el resultado en grande, «Ganamos $Z (n %)»
        i1, i2, i3 = cta.index("Cobramos</small><b>$302.101"), cta.index("Nos cobraron</small><b>$250.000"), cta.index("Ganamos</small><b>$52.101")
        self.assertTrue(i1 < i2 < i3)
        self.assertIn("<em>(17,2 %)</em>", cta)
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

    def test_en_una_ot_que_no_se_cobra_dice_nos_costo_y_la_constancia_vive_en_el_paso_1(self):
        html = self._pintar("garantia_nv")
        cta = html.split('<section class="fm-cta ')[1].split("</section>")[0]
        # 2026-10-08: «No se cobra · nos costó $Y» (la cuenta ya no repite «Cobré $0 − …»); el motivo va aparte
        self.assertIn("No se cobra</small><b>nos costó $200.000", cta)
        self.assertIn('<span class="fm-motivo">Garantía</span>', cta)
        self.assertNotIn("fm-neg", cta, "sin cobro no hay desglose Cobré/Queda por negocio")
        self.assertIn("Valorizado (referencia): <b>$959.000</b>", cta)
        # 2026-10-25 (Daniel: «¿la pidió o no la pidió? tiene que ser dinámico»): quién autorizó el $0 se cuenta UNA sola vez, en el paso ①, con las
        # palabras del servidor; la tarjeta ya no repite la constancia con otras palabras (así no se pueden contradecir)
        self.assertNotIn("fm-const", html)
        self.assertNotIn("Sin autorización", html)
        paso1 = html.split('<ol class="fm-pasos">')[1].split("</ol>")[0].split('<li class="fm-paso ')[1]
        self.assertIn("Autorizado por Daniel el 07/10/2026 14:00: falla de fábrica cubierta por la garantía", paso1)
        self.assertEqual(html.count("Autorizado por Daniel el 07/10/2026 14:00"), 1)

    def test_el_centro_de_costo_vive_dentro_de_la_tarjeta_de_la_cuenta(self):
        html = self._pintar("cobro_2docs")
        cta = html.split('<section class="fm-cta ')[1].split('<div class="fm-main">')[0]
        self.assertIn('<section class="fm-cen">', cta)
        self.assertEqual(cta.count("data-fm-cc="), 4)
        self.assertEqual(html.count("data-fm-cc="), 4, "los cuatro botones, una sola vez")
        self.assertNotIn('class="fm-fila"', html)
        self.assertNotIn("<small>El costo es nuestro</small>", cta, "icono + nombre, sin subtítulo (queda en el tooltip)")
        css = _leer("static/ot_fin_motor.css")
        cc = css.split(".fm-cc{")[1].split("}")[0]
        self.assertIn("min-height:44px", cc)
        self.assertIn("flex-direction:row", cc)
        self.assertNotIn(".fm-fila", css)
        self.assertIn(".fm-cta .fm-cen{border:0;border-top:1px dashed", css)

    def test_una_cobertura_sin_cobro_se_carga_a_un_centro_de_costo_y_lo_dice(self):
        """«Garantías o coberturas asociadas a centro de costo y poder reportar esas situaciones» (Daniel, 25-oct)."""
        cen = self._pintar("garantia_nv").split('<section class="fm-cen">')[1].split("</section>")[0]
        self.assertIn("Se carga a este centro de costo", cen)
        self.assertIn("siempre se carga a un centro de costo: así se puede reportar cuánto cuesta cada cobertura", cen)
        normal = self._pintar("cobro_2docs").split('<section class="fm-cen">')[1].split("</section>")[0]
        self.assertIn("Centro de costo", normal)
        self.assertNotIn("Se carga a este centro de costo", normal)


class TestElAnexoYElCandado(_Base):
    """REGLA #34 dentro de la tarjeta: el costo del proveedor ES el del anexo; se ve el candado y no se ofrece escribirlo a mano."""

    ANEXO = {"id": 9, "numero": 221, "estado": "firmado", "firmado": True, "total": 250000, "total_txt": "$250.000", "vinculo": "alineado",
             "costo_txt": "$250.000", "abierta": True}

    def _con_anexo(self, **kw):
        a = dict(self.ANEXO, **kw)
        return lambda rec, pan: rec.update(anexo=a)

    def test_con_un_anexo_vivo_las_filas_del_proveedor_llevan_candado(self):
        html = self._pintar("cobro_2docs", mutar=self._con_anexo())
        costos = html.split("<h3>Lo que nos costó</h3>")[1]
        self.assertIn('<i class="bi bi-lock-fill"></i> Anexo N° 221 · firmado</span>', costos)
        self.assertEqual(costos.count('class="fm-lock"'), 2, "técnico y despacho")
        self.assertNotIn('class="fm-lock"', self._pintar("cobro_2docs").split("<h3>Lo que nos costó</h3>")[1])

    def test_el_costo_no_se_escribe_a_mano_cuando_hay_anexo(self):
        html = self._pintar("cobro_2docs", mutar=lambda rec, pan: (self._con_anexo()(rec, pan), pan.update(puede_editar=True)))
        mod = html.split('<div class="fm-acciones">')[1].split("</div>")[0]
        self.assertNotIn('data-fm-act="corregirProv"', mod, "con un anexo vivo el servidor rechazaría escribirlo (COSTO_ES_DEL_ANEXO)")
        self.assertIn("El costo del proveedor lo fija el Anexo N° 221 (firmado): para cambiarlo, emite un anexo nuevo y la OT lo sigue sola.", mod)
        vigente = self._pintar("cobro_2docs", mutar=lambda rec, pan: (self._con_anexo(estado="vigente", firmado=False)(rec, pan), pan.update(puede_editar=True)))
        self.assertIn("para cambiarlo, corrige el monto en el anexo", vigente.split('<div class="fm-acciones">')[1].split("</div>")[0])
        # sin anexo, todo como siempre
        libre = self._pintar("cobro_2docs", mutar=lambda rec, pan: pan.update(puede_editar=True))
        self.assertIn('data-fm-act="corregirProv"', libre.split('<div class="fm-acciones">')[1].split("</div>")[0])

    def test_si_no_coincide_con_el_anexo_el_paso_4_ofrece_usar_el_valor_del_anexo(self):
        def difiere(rec, pan):
            rec["anexo"] = dict(self.ANEXO, vinculo="difiere", costo_txt="$200.000")
            rec["pasos"][3] = {"n": 4, "titulo": "Me cobró el proveedor", "estado": "falta", "texto": "No coincide con el Anexo N° 221: la OT dice $200.000 y el anexo $250.000. Es un solo valor."}
            pan.update(puede_editar=True)
        # el botón solo se ofrece si la ficha trae la función (otdCostoDesdeAnexo vive en detalle.html)
        html = self._pintar("cobro_2docs", mutar=difiere, con_funcion_anexo=True)
        paso4 = html.split('<ol class="fm-pasos">')[1].split("</ol>")[0].split('<li class="fm-paso ')[4]
        self.assertIn('data-fm-act="usarAnexo"', paso4)
        self.assertIn("Usar el valor del anexo", paso4)
        self.assertNotIn('data-fm-act="corregirProv"', paso4)
        sin_funcion = self._pintar("cobro_2docs", mutar=difiere)
        self.assertNotIn('data-fm-act="usarAnexo"', sin_funcion)

    def test_en_el_modal_de_cierre_se_dice_de_cual_anexo_sale_lo_que_nos_cobraron(self):
        html = self._pintar("cobro_2docs", modo="modal", mutar=self._con_anexo())
        self.assertIn('<p class="fm-anx-l"><i class="bi bi-lock-fill"></i> Lo que nos cobraron es el del <b>Anexo N° 221</b> (firmado, $250.000)', html)
        self.assertNotIn("data-fm-anx", html)
        sd = self._pintar("cobro_2docs", modo="modal", mutar=self._con_anexo(vinculo="sin_declarar", costo_txt=""))
        self.assertIn("y la OT aún no lo declara: alinéalo desde la ficha de la OT.", sd)
        df = self._pintar("cobro_2docs", modo="modal", mutar=self._con_anexo(vinculo="difiere", costo_txt="$200.000"))
        self.assertIn("y la OT dice $200.000: alinéalo desde la ficha de la OT.", df)
        ficha = self._pintar("cobro_2docs", mutar=self._con_anexo())
        self.assertIn('<div class="fm-anx" data-fm-anx hidden></div>', ficha)        # en la ficha el servidor dibuja la franja y el motor la adopta

    def test_en_el_modal_de_cierre_con_la_ot_abierta_el_boton_alinea_ahi_mismo(self):
        """Antes decía «alinéalo desde la ficha de la OT» y obligaba a salir del cierre: ahora el botón está ahí (si la ficha trae la función)."""
        df = self._pintar("cobro_2docs", modo="modal", mutar=self._con_anexo(vinculo="difiere", costo_txt="$200.000"), con_funcion_anexo=True)
        self.assertIn('data-fm-act="usarAnexo"', df)
        self.assertIn('y la OT dice $200.000: <span class="fm-anx-b"><button', df)
        self.assertEqual(df.count('class="fm-anx-l"'), 1, "el botón va dentro de la misma línea del anexo, no en otra caja")
        self.assertIn(".fm-anx-l .fm-btn-pri{background:#2563eb", _leer("static/ot_fin_motor.css"))   # azul, no rojo: «guiar sin gritar»
        self.assertNotIn("alinéalo desde la ficha de la OT", df)
        # cerrada, ya no se alinea desde acá; alineado, no hay nada que alinear
        cer = self._pintar("cobro_2docs", modo="modal", mutar=self._con_anexo(vinculo="difiere", costo_txt="$200.000", abierta=False), con_funcion_anexo=True)
        self.assertNotIn('data-fm-act="usarAnexo"', cer)
        self.assertIn("se corrige desde la ficha de la OT.", cer)
        self.assertNotIn('data-fm-act="usarAnexo"', self._pintar("cobro_2docs", modo="modal", mutar=self._con_anexo(), con_funcion_anexo=True))

    def test_usar_el_valor_del_anexo_en_el_modal_deja_el_aviso_para_reabrir_el_cierre_y_lo_retira_si_se_cancela(self):
        js = _leer("static/ot_fin_motor.js")
        i = js.index("usarAnexo: function (inst) {")
        f = js[i:js.index("declararCobro: function", i)]
        self.assertIn("inst.modo === 'modal'", f)
        self.assertIn("sessionStorage.setItem(k, '1')", f)
        self.assertIn("sessionStorage.removeItem(k)", f)       # si la página no se recargó (canceló), el aviso se retira
        self.assertIn("'otfReabrirCierre_' + inst.vid", f)     # la misma clave que lee detalle.html

    def test_si_el_bloque_no_se_puede_leer_la_franja_del_anexo_no_se_pierde(self):
        js = _leer("static/ot_fin_motor.js")
        i = js.index("function cargar(inst) {")
        f = js[i:js.index("/* ── Pintado", i)]
        self.assertLess(f.index("var franjaAnx = inst.anexoNodo"), f.index("inst.el.innerHTML = '<div class=\"fm-err\">"))
        self.assertIn("inst.el.appendChild(franjaAnx)", f)

    def test_la_franja_del_servidor_se_adopta_dentro_de_la_tarjeta(self):
        js = _leer("static/ot_fin_motor.js")
        i = js.index("function adoptarAnexo(inst) {")
        f = js[i:js.index("function pintar(inst) {", i)]
        self.assertIn("document.getElementById('otdAnexoVinculoFicha')", f)
        self.assertIn("if (!inst.anexoNodo)", f)          # el nodo se guarda la primera vez: al repintar el bloque entero no se pierde ni se duplica
        self.assertIn("slot.appendChild(inst.anexoNodo)", f)
        self.assertIn(".fm-anx #otdAnexoVinculoFicha{margin:0 !important}", _leer("static/ot_fin_motor.css"))


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
        # (el recorrido ya dice «Trabajo interno: no necesita documento ni autorización»: la línea no lo repite con las mismas palabras)
        self.assertIn("Sin documentos: es un trabajo interno.", html.split('<div class="fm-sindoc interna">')[1].split("</div>")[0])
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

    def test_el_trabajo_interno_no_repite_su_texto_en_la_cuenta_ni_en_el_cuerpo(self):
        html = self._pintar("interna_cerrada")
        visible = re.sub(r'title="[^"]*"', "", html)
        # solo en los dos pasos del recorrido que lo dicen (②, y ⑥ «Cerrada… · …»): ni la tarjeta de la cuenta ni la línea del cuerpo lo repiten
        self.assertEqual(visible.count(TXT_INTERNA.rstrip(".")), 2)
        cta = html.split('<section class="fm-cta ')[1].split("</section>")[0]
        self.assertNotIn("no necesita documento ni autorización", cta)

    def test_un_cliente_sin_documentos_ve_una_linea_con_el_boton_de_ligar_y_sin_duplicarlo(self):
        html = self._pintar("sin_docs")
        linea = html.split('<div class="fm-sindoc">')[1].split("</div>")[0]
        self.assertIn("Sin documentos aún", linea)
        self.assertIn('data-fm-act="ligarDoc"', linea)
        self.assertNotIn("fm-vacio", html)
        mod = html.split('<div class="fm-acciones">')[1].split("</div>")[0]
        self.assertNotIn('data-fm-act="ligarDoc"', mod, "el botón de asociar ya está en la línea: no se repite en «Modificar»")
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
        # 2026-10-08 (Daniel: «el Editar finanzas está de más y ocupa muchísimo espacio»): ya no se llama «Editar finanzas» ni está en la
        # página; es el modal «Editar cobro y costos» (tests/test_ot_costos_documentacion.py). Conserva su formulario completo.
        self.assertIn("{{ 'Editar cobro y costos' if puede_metadata else 'Costos y documentación de la OT' }}", det)
        self.assertNotIn("'Editar finanzas'", det)
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
