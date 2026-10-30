"""«Costos y documentación de la OT», v4 — 2026-10-29.

Daniel (casi textual): «Trata de evitar duplicidad de botones: ahorita los costos tienen para agregar factura por todos lados; debería ser más
estructurado. También debería indicar quién se está adjudicando ese dinero, aunque lo diga el header. Arriba: Daniel Pulgar nos cobró tanto; nosotros
cobramos a la empresa tanto; el cliente nos pagó 1 millón en el servicio de instalación y 500 mil en despacho; y de ahí para abajo indicas cuánto cobró
ILUS o cuánto cobró el interno. Dentro de eso también tenemos cobros internos, como ILUS. Pero eso muéstralo solamente por ahí: NO me vayas a crear un
agente ILUS ni un técnico ILUS.» Y antes: «las tarjetas deben ser pequeñas, indicando los documentos asociados y su trazabilidad y el total; no muestres
tanta información a menos que le den clic» y «la nota de venta pasa a ser la referencia de la venta; la factura o boleta, ok».

Lo que se fija acá:
  1. «¿Quién se adjudica el dinero?» arriba de la tarjeta: quién nos cobró (persona + empresa + anexo), a quién le cobramos (servicio y despacho con el
     documento que los lleva), la línea «ILUS · interno» solo cuando corresponde (un rótulo, nunca un usuario) y el margen con la política de la REGLA #57.
  2. UN botón por acción: «Agregar factura, boleta o nota de venta» existe UNA vez (encabezado de «Documentos») en cada escenario; el recorrido y el rechazo
     del cierre enlazan a él (irA). Ninguna acción se dibuja dos veces como botón.
  3. Cada documento es una tarjeta chica; el detalle se abre con «Ver detalle» (y con el texto completo de siempre).
  4. El servidor: el panorama trae quién nos cobró (persona, si es externo, empresa) DESPUÉS de cortar a un técnico (403), en la misma consulta; el
     vínculo con el anexo trae la empresa que lo firma.

v6 — 2026-10-29. Daniel, viendo la v5: «Me gustaba cómo salía antes, me había acostumbrado: cuánto cobré, cuánto nos cobraron y el margen en verde.
Siento que lo nuevo ocupa mucho espacio y le falta calidad de información… más colorido: antes estaba un poco más espectacular. La cuenta está perfecta.
El costo está perfecto.» Desde la v6:
  1'. Las TRES cifras grandes vuelven a «La cuenta» (Cobramos · Nos cobraron · GANAMOS con su %), con el color y el chip de su banda de la REGLA #57
      («Margen sano» desde la tolerancia, «Margen bajo (menos de 20 %)», «Pérdida»); el margen vive SOLO ahí.
  1''. «¿Quién se adjudica el dinero?» es una FRANJA FINA (una o dos líneas, sin cifras grandes ni tarjetas): «Nos cobró: persona · empresa $X [Anexo]» ·
      «Cobramos a: cliente $Y [Factura N°]» · «ILUS · interno: …» (solo cuando aplica). El desglose sigue detrás de «Ver el desglose».

Sin BD ni Flask: el motor JS en node con un DOM mínimo y las funciones de app.py recortadas con tests/_fuente_app.py (datos de mentira).
Correr con:  py -m unittest tests.test_ot_motor_quien_y_botones
"""
import copy
import json
import os
import re
import shutil
import subprocess
import tempfile
import unittest

from tests import _fuente_app as FA
from tests.test_ot_finanzas_compacto import RAIZ, _caso, _doc, _leer, _linea

MOTOR = os.path.join(RAIZ, "static", "ot_fin_motor.js")
# Las acciones que tienen (o tenían) botón en más de un lugar: cada una puede dibujarse como BOTÓN una sola vez.
ACCIONES_UNICAS = ("ligarDoc", "pedirCero", "quitarCero", "pedirCierre", "corregirProv", "declararCobro", "pedirPerdida", "usarAnexo")


def _ejemplo_de_daniel(cobra=True):
    """Daniel Pulgar (DAP Servicio SpA) nos cobró $1.000.000 por la instalación y $500.000 por el despacho; al cliente le cobramos $1.400.000 de
    instalación y $600.000 de despacho con la factura FCV 11567."""
    rec, pan = _caso("cobro_2docs" if cobra else "garantia_nv")
    f = rec["fin"]
    f["me_cobraron"].update(tecnico=1000000, despacho=500000, repuestos=0, total=1500000, falta_tecnico=False, falta_despacho=False)
    if cobra:
        f["cobre"].update(servicio=1400000, despacho=600000, total=2000000, hay=True)
        f["queda"].update(servicio=400000, despacho=100000, total=500000, pct=25.0, mostrar=True)
        pan["documentos"] = [_doc(1, "Factura", "FCV", "11567", [_linea("Instalación de equipos", "ZZINSTALACION", 1400000)],
                                  [_linea("Despacho a regiones", "ZZENVIO", 600000)], "servicio")]
    else:
        f["queda"].update(total=-1500000)
    pan["costos"]["registros"] = [
        {"clave": "tecnico", "rotulo": "Instalación o servicio del técnico / proveedor", "detalle": "Daniel Pulgar · proveedor externo",
         "monto": 1000000.0, "falta": False},
        {"clave": "despacho", "rotulo": "Despacho del proveedor", "detalle": "Flete o courier que nos cobraron", "monto": 500000.0, "falta": False}]
    pan["costos"]["proveedor"] = {"nombre": "Daniel Pulgar", "externo": True, "empresa": "DAP Servicio SpA"}
    pan["cliente"] = rec["cliente"] = "Gimnasio Las Condes SpA"
    rec["anexo"] = {"id": 9, "numero": 221, "estado": "firmado", "firmado": True, "total": 1500000, "total_txt": "$1.500.000",
                    "vinculo": "alineado", "costo_txt": "$1.500.000", "abierta": True, "proveedor": "DAP Servicio SpA"}
    return rec, pan


class _Motor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.node = shutil.which("node")
        if not cls.node:
            raise unittest.SkipTest("node no está instalado")
        cls.tmp = tempfile.mkdtemp(prefix="motor_quien_")
        cls.harness = os.path.join(cls.tmp, "harness.js")
        with open(cls.harness, "w", encoding="utf-8") as f:
            f.write(r"""
const fs = require('fs');
const datos = JSON.parse(fs.readFileSync(process.argv[3], 'utf8'));
global.window = global;
const oyentes = [];
const el = {
  innerHTML: '', classList: { add() {}, remove() {} },
  getAttribute(k) { return { 'data-vid': '461', 'data-modo': process.argv[4] || 'ficha' }[k] || null; },
  addEventListener(t, f) { if (t === 'click') oyentes.push(f); }, closest() { return null; }, contains() { return true; },
  querySelector() { return null; }, querySelectorAll() { return []; }
};
global.document = { readyState: 'complete', querySelectorAll() { return [el]; }, getElementById() { return null; },
                    createElement() { return { style: {}, setAttribute() {}, appendChild() {} }; }, addEventListener() {} };
global.ilusToast = function () {};
if (datos.anexoFn) global.otdCostoDesdeAnexo = function () {};
global.fetch = function (url) {
  const j = url.indexOf('/recorrido') >= 0 ? datos.recorrido : datos.panorama;
  return Promise.resolve({ ok: true, status: 200, json() { return Promise.resolve(j); } });
};
require(process.argv[2]);
setTimeout(function () {
  if (datos.rechazo) { global.OTFinMotor.alCerrarRechazado(datos.rechazo); }
  // clics: "accion" o "accion:argumento" (el argumento va en data-fm-doc / data-fm-ir, como en la página)
  (datos.clics || []).forEach(function (c) {
    const p = c.split(':'), act = p[0], arg = p.slice(1).join(':');
    const b = { getAttribute(k) { return k === 'data-fm-act' ? act : ((k === 'data-fm-doc' || k === 'data-fm-ir') ? arg : null); } };
    const ev = { target: { closest(sel) { return sel === '[data-fm-act]' ? b : null; } }, preventDefault() {} };
    oyentes.forEach(function (f) { f(ev); });
  });
  setTimeout(function () { process.stdout.write(el.innerHTML); }, 30);
}, 80);
""")

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def pintar(self, rec, pan, modo="ficha", clics=(), anexo_fn=False, rechazo=None):
        datos = os.path.join(self.tmp, "datos.json")
        with open(datos, "w", encoding="utf-8") as f:
            json.dump({"recorrido": rec, "panorama": pan, "clics": list(clics), "anexoFn": anexo_fn, "rechazo": rechazo}, f)
        r = subprocess.run([self.node, self.harness, MOTOR, datos, modo], capture_output=True, text=True, encoding="utf-8", timeout=60)
        self.assertEqual(r.returncode, 0, r.stderr)
        return r.stdout

    @staticmethod
    def quien(html):
        return html.split('<section class="fm-adj"')[1].split("</section>")[0]

    @staticmethod
    def plano(html):
        return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html)).strip()

    # 2026-10-29 (v6): la franja fina tiene UNA línea de ítems (`.fm-adj-l`) y, debajo, el desglose (`.fm-adj-det`, oculto hasta el clic)
    @staticmethod
    def linea(q):
        return q.split('<div class="fm-adj-det"')[0]

    @staticmethod
    def desglose(q):
        return q.split('<div class="fm-adj-det"')[1] if '<div class="fm-adj-det"' in q else ""

    @staticmethod
    def cuenta(html):
        """La cuenta, desde su clase (c-ok, c-tol…) hasta antes del centro de costo (que es otra <section> adentro)."""
        return html.split('<section class="fm-cta ')[1].split("</section>")[0]

    @staticmethod
    def css_franja():
        css = _leer("static/ot_fin_motor.css")
        return css.split("/* \u2550\u2550\u2550 v6 \u00b7 2026-10-29 \u00b7 \u00ab\u00bfQui\u00e9n se adjudica el dinero?\u00bb en UNA FRANJA FINA")[1] \
            .split("/* \u2550\u2550\u2550 v6 \u00b7 2026-10-29 \u00b7 \u00abLa cuenta\u00bb vuelve")[0]


class TestQuienSeAdjudicaElDinero(_Motor):
    """2026-10-29 (v6): «¿Quién se adjudica el dinero?» es UNA FRANJA FINA (Daniel, viendo la v5: «ocupa mucho espacio… me gustaba cómo salía antes»):
    «Nos cobró: persona · empresa $X [Anexo]» · «Cobramos a: cliente $Y [Factura N°]» · «ILUS · interno: …» (solo cuando aplica). El margen ya no va acá:
    vive en el GANAMOS de «La cuenta». Servicio y despacho de cada lado, con su documento, en «Ver el desglose»."""

    def test_el_ejemplo_de_daniel_nos_cobro_y_cobramos_en_una_franja(self):
        rec, pan = _ejemplo_de_daniel()
        html = self.pintar(rec, pan)
        q = self.quien(html)
        linea, det = self.linea(q), self.desglose(q)
        # va arriba de todo: después de la frase y antes de La cuenta | El recorrido
        self.assertLess(html.index('<section class="fm-adj"'), html.index('<div class="fm-top'))
        self.assertIn("¿Quién se adjudica el dinero?", linea)
        # 1) Nos cobró: la persona, su empresa (la del anexo), la cifra y el anexo, en UNA línea
        self.assertIn('<span class="fm-q prov"><small class="fm-q-t">Nos cobró:</small><b class="fm-q-n">Daniel Pulgar</b><span class="fm-q-e">· DAP Servicio SpA</span>'
                      '<b class="fm-q-v">$1.500.000</b><span class="fm-q-ds"><span class="fm-lock"><i class="bi bi-lock-fill"></i> Anexo N° 221 · firmado</span></span></span>', linea)
        # 2) Cobramos a: el cliente, el total y, JUNTO al total, el chip chico de la factura que lo lleva (una vez, aunque cobre servicio y despacho)
        fac = '<em class="fm-q-doc" title="FCV 11567: documento de cobro">Factura N° 11567</em>'
        self.assertIn('<span class="fm-q cli" title="Lo cobrado sale de la FCV 11567: servicio $1.400.000 + despacho $600.000"><small class="fm-q-t">Cobramos a:</small>'
                      '<b class="fm-q-n">Gimnasio Las Condes SpA</b><b class="fm-q-v">$2.000.000</b><span class="fm-q-ds">' + fac + '</span></span>', linea)
        # el desglose (oculto hasta el clic): servicio y despacho de cada lado, cada línea con su documento; y el dato completo del proveedor
        self.assertIn('<div class="fm-adj-det" id="fmQuien1" hidden>', q)
        self.assertIn('<b class="fm-q-dt">Nos cobró · Daniel Pulgar <small>(DAP Servicio SpA · proveedor externo)</small></b>', det)
        self.assertIn('<li><span class="fm-q-r">Instalación o servicio</span><b>$1.000.000</b></li>', det)
        self.assertIn('<li><span class="fm-q-r">Despacho</span><b>$500.000</b></li>', det)
        self.assertIn('<span class="fm-q-r">Instalación o servicio</span><b>$1.400.000</b><span class="fm-q-ds">' + fac + '</span>', det)
        self.assertIn('<span class="fm-q-r">Despacho</span><b>$600.000</b><span class="fm-q-ds">' + fac + '</span>', det)
        # 3) se cobra: no hay línea «ILUS · interno»
        self.assertNotIn("ILUS · interno", q)
        # el margen NO se repite en la franja: vive en el GANAMOS de La cuenta (25 % = tolerancia de la REGLA #57: verde, «Margen sano»)
        for nada in ("Ganamos", "Perdimos", "%", "fm-q-m", "fm-res"):
            self.assertNotIn(nada, q, nada)
        cta = self.cuenta(html)
        self.assertTrue(cta.startswith("c-tol"), "La cuenta va en el verde de la tolerancia")
        self.assertIn('<div class="fm-res-i q"><small>Ganamos</small><b>$500.000<em>(25 %)</em></b></div>'
                      '<span class="fm-sem" title="En tolerancia: entre 20 y 30 %">Margen sano</span>', cta)

    def test_en_garantia_no_se_cobra_y_lo_asume_ilus_como_una_linea(self):
        rec, pan = _ejemplo_de_daniel(cobra=False)
        html = self.pintar(rec, pan)
        q = self.quien(html)
        linea, det = self.linea(q), self.desglose(q)
        self.assertIn('<span class="fm-q cli nocobra"><small class="fm-q-t">Cobramos a:</small><b class="fm-q-n">Gimnasio Las Condes SpA</b>'
                      '<b class="fm-q-v">No se cobra</b><span class="fm-q-ds"><span class="fm-q-e">· <span class="fm-motivo">Garantía</span></span></span></span>', linea)
        # lo que costó lo dice la línea «ILUS · interno» (la empresa lo asume): no se repite junto al cliente
        self.assertIn('<span class="fm-q ilus"><small class="fm-q-t">ILUS · interno:</small><b class="fm-q-n">Lo asume ILUS</b><b class="fm-q-v">$1.500.000</b></span>', linea)
        self.assertNotIn("nos costó", linea)
        self.assertEqual(q.count('class="fm-q ilus"'), 1, "UNA línea de cobro interno")
        # el centro de costo elegido y la venta de referencia, en el desglose
        self.assertIn("Se carga al centro de costo: <b>Servicio Técnico</b>", det)
        self.assertIn('<em class="fm-q-doc ref" title="NVV 4521: referencia de la garantía, no es el documento de cobro">', det)
        self.assertNotIn("%", q, "sin cobro no hay margen que medir (nunca se inventa un cobro ni un $0)")
        # La cuenta: «No se cobra · nos costó $X» y el motivo, como antes; ningún % ni chip de margen
        cta = self.cuenta(html)
        self.assertIn('<div class="fm-res"><div class="fm-res-i q"><small>No se cobra</small><b>nos costó $1.500.000</b></div><span class="fm-motivo">Garantía</span></div>', cta)
        self.assertNotIn("<em>(", cta)
        self.assertNotIn('class="fm-sem"', cta)

    def test_sin_centro_de_costo_la_franja_lo_pide_en_ambar(self):
        rec, pan = _ejemplo_de_daniel(cobra=False)
        pan["centro"]["nombre"] = ""
        pan["centro"]["valor"] = ""
        linea = self.linea(self.quien(self.pintar(rec, pan)))
        self.assertIn('<b class="fm-q-v">$1.500.000</b><span class="fm-q-ds"><span class="fm-pend">Falta elegir el centro de costo</span></span>', linea)

    def test_trabajo_interno_solo_quien_lo_hizo_y_la_linea_ilus_sin_cliente(self):
        rec, pan = _caso("interna_cerrada")
        pan["costos"]["registros"][0]["detalle"] = "Jaizer Rodríguez · técnico propio"
        pan["costos"]["proveedor"] = {"nombre": "Jaizer Rodríguez", "externo": False, "empresa": ""}
        html = self.pintar(rec, pan)
        q = self.quien(html)
        self.assertIn('<small class="fm-q-t">Lo hizo un técnico propio:</small><b class="fm-q-n">Jaizer Rodríguez</b><b class="fm-q-v">$0</b>'
                      '<span class="fm-q-ds"><span class="fm-q-ok">No se le paga aparte</span></span>', q)
        self.assertIn('<small class="fm-q-t">ILUS · interno:</small><b class="fm-q-n">Trabajo interno de ILUS</b>', q)
        self.assertNotIn("Cobramos a", q, "un trabajo interno no tiene cliente")
        self.assertNotIn("%", q)
        cta = self.cuenta(html)
        self.assertIn('<div class="fm-res-i q"><small>No se cobra</small><b>nos costó $0</b></div><span class="fm-motivo">Trabajo interno</span>', cta)
        self.assertNotIn("<em>(", cta, "un trabajo interno no tiene un % inventado")
        self.assertNotIn("Ganamos", cta)

    def test_nunca_se_crea_un_tecnico_ni_un_agente_ilus(self):
        """«ILUS · interno» es SOLO el rótulo de una línea: el motor no inventa una persona, un técnico ni un agente llamado ILUS."""
        js = re.sub(r"/\*.*?\*/", "", _leer("static/ot_fin_motor.js"), flags=re.S)       # el código, sin los comentarios (que citan a Daniel)
        for malo in ("Técnico ILUS", "técnico ILUS", "Agente ILUS", "agente ILUS", "'ILUS'", '"ILUS"'):
            self.assertNotIn(malo, js)
        for caso in ("cobro_2docs", "garantia_nv", "interna_cerrada"):
            rec, pan = _caso(caso)
            html = self.pintar(rec, pan)
            self.assertNotIn("Técnico ILUS", html)
            self.assertNotIn("agente", html.lower())
            prov = self.linea(self.quien(html)).split('<span class="fm-q prov')[1].split('<span class="fm-q ')[0]
            self.assertNotIn("ILUS", prov, "quien nos cobró es la persona de la OT, nunca «ILUS»")

    def test_sin_cobro_declarado_falta_declararlo_en_ambar_y_la_nota_de_venta_es_la_referencia(self):
        rec, pan = _caso("cobro_2docs")
        rec["fin"]["cobre"].update(servicio=0, despacho=0, total=0, hay=False)
        rec["fin"]["queda"].update(mostrar=False, pct=None)
        rec["fin"].update(clase="gris", label="Falta el documento de cobro")
        pan["documentos"] = [_doc(1, "Nota de venta", "VD", "10618", [], [], "nota_venta", cat="nota_venta", saldo=False)]
        html = self.pintar(rec, pan)
        linea = self.linea(self.quien(html))
        cli = linea.split('<span class="fm-q cli falta"')[1]
        self.assertIn('<b class="fm-q-v"><span class="fm-pend">Falta declararlo</span></b>', cli)
        # la nota de venta en su chip AZUL (referencia de la venta) y el ÁMBAR de lo que falta, a la vista junto a lo cobrado
        self.assertIn('<span class="fm-q-ds"><em class="fm-q-doc ref" title="VD 10618: referencia de la venta, no es el documento de cobro">'
                      'Nota de venta N° 10618 · referencia de la venta</em><em class="fm-q-doc falta">Falta la factura o boleta</em></span>', cli)
        # La cuenta: «Resultado —, falta un dato para calcularlo» (nunca un margen inventado) y lo que falta, en el chip del semáforo
        cta = self.cuenta(html)
        self.assertTrue(cta.startswith("c-gris"))
        self.assertIn('<div class="fm-res-i"><small>Cobramos</small><b>—</b></div>', cta)
        self.assertIn('<div class="fm-res-i q"><small>Resultado</small><b>—<em>falta un dato para calcularlo</em></b></div>'
                      '<span class="fm-sem">Falta el documento de cobro</span>', cta)
        self.assertNotIn("Ganamos", html)

    def test_la_politica_de_margen_de_la_regla_57_tambien_la_manda_el_servidor(self):
        # si el servidor manda su política (ot_margen_politica.py), manda la del servidor
        rec, pan = _ejemplo_de_daniel()
        rec["fin"]["queda"].update(pct=35.0, total=700000)
        rec["fin"]["politica_margen"] = {"objetivo": 40, "tolerancia": 25}
        cta = self.cuenta(self.pintar(rec, pan))
        self.assertTrue(cta.startswith("c-tol"))
        self.assertIn('<span class="fm-sem" title="En tolerancia: entre 25 y 40 %">Margen sano</span>', cta)
        rec["fin"]["queda"].update(pct=22.0, total=440000)
        cta = self.cuenta(self.pintar(rec, pan))
        self.assertIn('<span class="fm-sem" title="Margen bajo: menos de 25 % (el objetivo es 40 %)">Margen bajo (menos de 25 %)</span>', cta)
        js = _leer("static/ot_fin_motor.js")
        self.assertIn("var POLITICA_MARGEN = { objetivo: 30, tolerancia: 20 };", js, "UNA constante, fácil de reemplazar")

    def test_el_anexo_que_no_coincide_se_dice_en_la_franja_y_el_desglose_da_las_dos_cifras(self):
        rec, pan = _ejemplo_de_daniel()
        rec["anexo"].update(vinculo="difiere", costo_txt="$1.200.000")
        q = self.quien(self.pintar(rec, pan))
        prov = self.linea(q).split('<span class="fm-q prov')[1].split('<span class="fm-q ')[0]
        self.assertIn('<span class="fm-lock no"><i class="bi bi-lock-fill"></i> Anexo N° 221 · firmado · no coincide con la OT</span>', prov)
        self.assertIn('<span class="fm-q-av">El Anexo N° 221 dice $1.500.000 y la OT dice $1.200.000</span>', self.desglose(q))
        # el semáforo del anexo: «no coincide» en rojo suave; vigente o «la OT aún no lo declara», en ámbar
        rec, pan = _ejemplo_de_daniel()
        rec["anexo"].update(vinculo="sin_declarar", costo_txt="")
        q = self.quien(self.pintar(rec, pan))
        self.assertIn('<span class="fm-lock no sd"><i class="bi bi-lock-fill"></i> Anexo N° 221 · firmado · la OT aún no lo declara</span>', q)
        self.assertIn("El Anexo N° 221 dice $1.500.000 y la OT aún no lo declara", q)
        rec, pan = _ejemplo_de_daniel()
        rec["anexo"].update(firmado=False, estado="vigente")
        html = self.pintar(rec, pan)
        self.assertIn('<span class="fm-lock vig"><i class="bi bi-lock-fill"></i> Anexo N° 221 · vigente</span>', self.quien(html))
        self.assertIn('<span class="fm-lock vig" title=', html.split('<section class="fm-cta ')[1], "La cuenta dice lo mismo, con el mismo color")
        css = _leer("static/ot_fin_motor.css")
        self.assertIn(".fm-lock.vig,.fm-lock.no.sd{background:var(--fm-ambar-f);color:#78350f}", css)
        self.assertIn(".fm-lock.no{background:var(--fm-rojo-f);color:#991b1b}", css)

    def test_nada_se_corta_en_la_franja(self):
        bloque = self.css_franja()
        self.assertNotIn("text-overflow", bloque)
        self.assertNotIn("line-clamp", bloque)
        for regla in ("\n.fm-q-n{", "\n.fm-q-e{", "\n.fm-q-r{", "\n.fm-q-dt{"):
            self.assertIn("overflow-wrap:anywhere", bloque.split(regla)[1].split("}")[0], regla)
        self.assertIn("grid-template-columns:repeat(auto-fit,minmax(min(100%,240px),1fr))", bloque.split("\n.fm-adj-det{")[1].split("}")[0])
        # nombres largos: completos, en la franja y en el desglose
        rec, pan = _ejemplo_de_daniel()
        largo_cli = "Corporación de Gimnasios y Centros Deportivos Integrales del Sur de Chile SpA"
        largo_emp = "Servicios Técnicos Integrales de Mantención y Montaje Valenzuela Contreras Limitada"
        pan["cliente"] = rec["cliente"] = largo_cli
        pan["costos"]["proveedor"] = {"nombre": "María Fernanda de los Ángeles Valenzuela Contreras", "externo": True, "empresa": largo_emp}
        rec["anexo"]["proveedor"] = largo_emp
        linea = self.linea(self.quien(self.pintar(rec, pan)))
        for texto in (largo_cli, largo_emp, "María Fernanda de los Ángeles Valenzuela Contreras"):
            self.assertIn(texto, linea)


class TestLaCuentaYLaFranjaFina(_Motor):
    """v6 — 2026-10-29. Daniel, viendo la v5: «Me gustaba cómo salía antes, me había acostumbrado: cuánto cobré, cuánto nos cobraron y el margen en verde.
    Siento que lo nuevo ocupa mucho espacio y le falta calidad de información… Pienso que más colorido: antes estaba un poco más espectacular. La cuenta está
    perfecta. El costo está perfecto.» Lo que se fija: las TRES cifras grandes vuelven a «La cuenta» (Cobramos · Nos cobraron · GANAMOS con su %, lleno con el
    color de su banda de la REGLA #57, y el chip «Margen sano» / «Margen bajo (menos de 20 %)» / «Pérdida»); la franja de arriba es fina (sin cifras
    grandes) y no existe un segundo grupo de cifras grandes; nunca un % inventado."""

    BANDAS = (  # (pct, queda, tecnico+despacho, clase de La cuenta, rótulo, cifra, chip, tooltip)
        (35.0, 700000, (900000, 400000), "c-ok", "Ganamos", "$700.000<em>(35 %)</em>", "Margen sano", "Margen objetivo: 30 % o más"),
        (25.0, 500000, (1000000, 500000), "c-tol", "Ganamos", "$500.000<em>(25 %)</em>", "Margen sano", "En tolerancia: entre 20 y 30 %"),
        (15.0, 300000, (1200000, 500000), "c-bajo", "Ganamos", "$300.000<em>(15 %)</em>", "Margen bajo (menos de 20 %)",
         "Margen bajo: menos de 20 % (el objetivo es 30 %)"),
        (-5.0, -100000, (1600000, 500000), "c-rojo", "Perdimos", "$100.000<em>(−5 %)</em>", "Pérdida", "Pérdida: lo cobrado no alcanza a cubrir lo que nos costó"),
    )

    def test_las_tres_cifras_grandes_con_el_semaforo_de_cada_banda(self):
        for pct, queda, (tec, des), clase, rot, cifra, chip, tip in self.BANDAS:
            rec, pan = _ejemplo_de_daniel()
            rec["fin"]["me_cobraron"].update(tecnico=tec, despacho=des, total=tec + des)
            rec["fin"]["queda"].update(pct=pct, total=queda)
            html = self.pintar(rec, pan)
            cta = self.cuenta(html)
            self.assertTrue(cta.startswith(clase), (pct, cta[:20]))
            res = cta.split('<div class="fm-res">')[1].split('<table')[0]
            self.assertIn('<div class="fm-res-i co"><small>Cobramos</small><b>$2.000.000</b></div>', res, pct)
            self.assertIn('<div class="fm-res-i"><small>Nos cobraron</small><b>' + self._clp(tec + des) + '</b></div>', res, pct)
            self.assertIn(f'<div class="fm-res-i q"><small>{rot}</small><b>{cifra}</b></div><span class="fm-sem" title="{tip}">{chip}</span>', res, pct)
            # orden de lectura: cuánto cobré → cuánto nos cobraron → el resultado
            self.assertTrue(res.index("Cobramos") < res.index("Nos cobraron") < res.index(rot), pct)
            # el margen vive SOLO en La cuenta
            self.assertEqual(html.count(f"<small>{rot}</small>"), 1, pct)
            self.assertNotIn(rot, self.quien(html), pct)

    def test_sin_medir_no_inventa_ni_porcentaje_ni_banda(self):
        rec, pan = _ejemplo_de_daniel()
        rec["fin"]["queda"].update(mostrar=False, pct=None)
        rec["fin"]["me_cobraron"].update(falta_tecnico=True, tecnico=None)
        rec["fin"].update(clase="ambar", label="Falta lo que te cobró el técnico")
        html = self.pintar(rec, pan)
        cta = self.cuenta(html)
        self.assertTrue(cta.startswith("c-ambar"))
        self.assertIn('<div class="fm-res-i"><small>Nos cobraron</small><b>—</b></div>', cta)
        self.assertIn('<div class="fm-res-i q"><small>Resultado</small><b>—<em>falta un dato para calcularlo</em></b></div>'
                      '<span class="fm-sem">Falta lo que te cobró el técnico</span>', cta)
        self.assertNotIn("Ganamos", html)
        self.assertNotIn("Perdimos", html)
        self.assertNotIn("Margen sano", html)
        self.assertIn('<b class="fm-q-v"><span class="fm-pend">Falta declararlo</span></b>', self.linea(self.quien(html)), "lo que nos cobraron falta: en ámbar")
        # ni ganamos ni perdimos ($0 exacto): «Resultado $0», sin paréntesis; 0 % es margen bajo (ámbar), nunca verde
        rec, pan = _ejemplo_de_daniel()
        rec["fin"]["me_cobraron"].update(tecnico=1500000, despacho=500000, total=2000000)
        rec["fin"]["queda"].update(pct=0.0, total=0)
        cta = self.cuenta(self.pintar(rec, pan))
        self.assertTrue(cta.startswith("c-bajo"))
        self.assertIn('<small>Resultado</small><b>$0<em>ni ganamos ni perdimos</em></b>', cta)

    def test_garantia_e_interno_sin_porcentaje_inventado(self):
        for caso in ("garantia_nv", "interna_cerrada"):
            html = self.pintar(*_caso(caso))
            cta = self.cuenta(html)
            self.assertTrue(cta.startswith("c-info"), caso)
            self.assertIn('<div class="fm-res-i q"><small>No se cobra</small><b>nos costó ', cta, caso)
            self.assertNotIn("%", cta.split("<table")[0], caso)
            self.assertNotIn("Ganamos", html, caso)
            self.assertNotIn("Margen sano", html, caso)
            self.assertNotIn("Margen bajo", html, caso)

    def test_la_politica_del_motor_es_la_de_ot_margen_politica(self):
        import ot_margen_politica as POL
        js = _leer("static/ot_fin_motor.js")
        m = re.search(r"var POLITICA_MARGEN = \{ objetivo: (\d+(?:\.\d+)?), tolerancia: (\d+(?:\.\d+)?) \};", js)
        self.assertTrue(m)
        self.assertEqual((float(m.group(1)), float(m.group(2))), (POL.OBJETIVO_PCT, POL.TOLERANCIA_PCT))
        self.assertEqual(POL.banda(25), "tolerancia")      # (el motor la llama «tol»: misma banda)
        self.assertEqual(POL.banda(15), "amarillo")        # (el motor: «bajo»)

    def test_el_ganamos_es_el_protagonista_y_su_verde_cumple_contraste(self):
        css = _leer("static/ot_fin_motor.css")
        self.assertIn(".fm-res-i.q{flex:1 1 230px;background:var(--c);color:#fff}", css)
        self.assertIn(".fm-res-i.q b{font-size:1.45rem;", css, "la cifra grande de antes")
        self.assertIn(".fm-cta.c-ok .fm-res-i.q,.fm-cta.c-tol .fm-res-i.q{background:#15803d}", css)
        self.assertIn(".fm-cta.c-ok .fm-sem,.fm-cta.c-tol .fm-sem{background:var(--fm-verde-f);color:#14532d}", css, "Margen sano en verde")
        self.assertIn(".fm-cta.c-gris .fm-res-i.q{background:var(--fm-gris-f);color:#374151", css, "sin medir: nunca blanco sobre gris")
        self.assertIn(".fm-res-i.co b{color:#15803d}", css, "lo cobrado en verde")

        def lum(h):
            c = [int(h[i:i + 2], 16) / 255 for i in (1, 3, 5)]
            c = [x / 12.92 if x <= 0.03928 else ((x + 0.055) / 1.055) ** 2.4 for x in c]
            return 0.2126 * c[0] + 0.7152 * c[1] + 0.0722 * c[2]
        for fondo in ("#15803d", "#2563eb", "#dc2626"):        # verde (objetivo/tolerancia), azul (no se cobra), rojo (pérdida) con texto blanco
            self.assertGreaterEqual((1.05) / (lum(fondo) + 0.05), 4.5, fondo)

    def test_la_franja_es_fina_y_no_hay_un_segundo_grupo_de_cifras_grandes(self):
        rec, pan = _ejemplo_de_daniel()
        html = self.pintar(rec, pan)
        q = self.quien(html)
        self.assertEqual(html.count('class="fm-res"'), 1, "las cifras grandes, UNA vez: en La cuenta")
        self.assertEqual(html.count('class="fm-res-i q"'), 1)
        self.assertNotIn("fm-res", q)
        self.assertNotIn("fm-q-m", html, "el bloque del margen de la v5 ya no existe")
        self.assertNotIn("fm-q-grid", html)
        # una sola línea de ítems (título + quién nos cobró + a quién le cobramos + el botón), sin tarjetas
        self.assertEqual(q.count('<div class="fm-adj-l">'), 1)
        self.assertEqual(q.count('<span class="fm-q '), 2)
        self.assertNotIn('<div class="fm-q ', q)
        bloque = self.css_franja()
        self.assertNotIn("border-left:", _leer("static/ot_fin_motor.css").replace("border-left:0", ""), "ningún borde lateral de acento")
        adj = bloque.split("\n.fm-adj{")[1].split("}")[0]
        self.assertIn("padding:6px 12px", adj, "franja fina")
        for regla in ("\n.fm-q-v{", "\n.fm-q-n{"):
            tam = float(re.search(r"font-size:([\d.]+)rem", bloque.split(regla)[1].split("}")[0]).group(1))
            self.assertLessEqual(tam, 0.9, regla + ": sin cifras grandes en la franja")
        self.assertNotIn(".fm-q-m", _leer("static/ot_fin_motor.css"))
        # la barra separadora va en el hueco y la recorta la franja (nunca una barra suelta al empezar una línea)
        self.assertIn("overflow:hidden", bloque.split("\n.fm-adj-l{")[1].split("}")[0])
        self.assertIn("left:-8px", bloque.split("\n.fm-q::before{")[1].split("}")[0])
        # en un bloque angosto (celular, modal chico): un ítem por línea
        angosto = _leer("static/ot_fin_motor.css").split("@container (max-width:560px){")[1].split("\n}")[0]
        self.assertIn(".fm-q,.fm-adj-t{flex:1 1 100%}", angosto)
        self.assertIn(".fm-q::before{display:none}", angosto)

    def test_el_chip_del_documento_va_junto_a_lo_cobrado(self):
        # factura + boleta: los dos chips verdes junto al total
        linea = self.linea(self.quien(self.pintar(*_caso("cobro_2docs"))))
        self.assertIn('<b class="fm-q-v">$302.101</b><span class="fm-q-ds"><em class="fm-q-doc" title="FCV 11439: documento de cobro">Factura N° 11439</em>'
                      '<em class="fm-q-doc" title="BLV 23375: documento de cobro">Boleta N° 23375</em></span>', linea)
        # cobra solo con la nota de venta (sus líneas aportan): chip AZUL de referencia + ÁMBAR «Falta la factura o boleta», junto al total y en cada línea
        rec, pan = _caso("cobro_2docs")
        pan["documentos"] = [_doc(1, "Nota de venta", "VD", "10618", [_linea("Instalación", "ZZINSTALACION", 252101)], [_linea("Despacho", "ZZENVIO", 50000)],
                                  "nota_venta", cat="nota_venta")]
        q = self.quien(self.pintar(rec, pan))
        nv = ('<em class="fm-q-doc ref" title="VD 10618: referencia de la venta, no es el documento de cobro">Nota de venta N° 10618 · referencia de la venta</em>'
              '<em class="fm-q-doc falta">Falta la factura o boleta</em>')
        self.assertEqual(self.linea(q).count(nv), 1, "junto al total")
        self.assertEqual(self.desglose(q).count(nv), 2, "en servicio y despacho del desglose")
        self.assertNotIn('<em class="fm-q-doc" title=', q, "ningún chip verde: la nota de venta no es el documento de cobro")
        # garantía: la venta de referencia en azul, dentro del desglose
        q = self.quien(self.pintar(*_ejemplo_de_daniel(cobra=False)))
        self.assertIn('<div class="fm-q-det cli"><b class="fm-q-dt">Cobramos a · Gimnasio Las Condes SpA</b><span class="fm-q-ds"><em class="fm-q-doc ref" '
                      'title="NVV 4521: referencia de la garantía, no es el documento de cobro">Nota de venta N° 4521 · referencia de la garantía</em></span></div>', q)
        css = _leer("static/ot_fin_motor.css")
        doc = css.split(".fm-q-doc{")[1].split("}")[0]
        self.assertIn("background:var(--fm-verde-f);color:#14532d", doc)
        self.assertIn(".fm-q-doc.ref{background:var(--fm-azul-f)", css)
        self.assertIn(".fm-q-doc.falta{background:var(--fm-ambar-f)", css)

    def test_el_mismo_verde_en_la_tarjeta_chica_del_documento_y_en_el_chip_5_del_recorrido(self):
        rec, pan = _caso("cobro_2docs")
        pan["documentos"].append(_doc(3, "Nota de venta", "VD", "10618", [], [], "nota_venta", cat="nota_venta", saldo=False))
        rec["resumen"] = {"estado": "listo", "titulo": "Lista para cerrar", "faltan": []}
        html = self.pintar(rec, pan)
        self.assertIn('<span class="fm-chip k-cobro">Factura</span><h4>FCV 11439</h4>', html)
        self.assertIn('<span class="fm-chip k-ref">Nota de venta</span><h4>VD 10618</h4>', html)
        css = _leer("static/ot_fin_motor.css")
        self.assertIn(".fm-chip.k-cobro{background:var(--fm-verde-f);color:#14532d", css)
        # el chip ⑤ (Margen) del recorrido plegado lleva el semáforo de su banda (17,2 % → bajo), igual que La cuenta
        chips = html.split('<span class="fm-rec-chips">')[1]
        self.assertIn('<span class="fm-pchip hecho m-bajo" title="5. Margen:', chips)
        self.assertIn(".fm-pchip.m-bajo{background:var(--fm-ambar-f)", css)
        self.assertTrue(self.cuenta(html).startswith("c-bajo"))

    def test_el_desglose_se_abre_y_se_cierra_con_un_clic(self):
        rec, pan = _ejemplo_de_daniel()
        cerrado = self.quien(self.pintar(rec, pan))
        self.assertIn('<button type="button" class="fm-adj-ver" data-fm-act="quienTog" aria-expanded="false" aria-controls="fmQuien1">Ver el desglose', cerrado)
        self.assertIn('<div class="fm-adj-det" id="fmQuien1" hidden>', cerrado)
        self.assertEqual(cerrado.count('<div class="fm-q-det '), 2, "Nos cobró y Cobramos a")
        abierto = self.quien(self.pintar(rec, pan, clics=("quienTog",)))
        self.assertIn('data-fm-act="quienTog" aria-expanded="true" aria-controls="fmQuien1">Ocultar el desglose', abierto)
        self.assertIn('<div class="fm-adj-det" id="fmQuien1">', abierto)
        otra = self.quien(self.pintar(rec, pan, clics=("quienTog", "quienTog")))
        self.assertIn('<div class="fm-adj-det" id="fmQuien1" hidden>', otra)
        css = _leer("static/ot_fin_motor.css")
        self.assertIn(".fm-adj-det[hidden]{display:none !important}", css)            # también en el modal de cierre (fuera de #otdCardMotor)
        self.assertIn(".fm-adj-ver{min-height:44px", css.split("@media (max-width:560px)")[-1], "táctil en el celular")
        self.assertIn("quienTog: function (inst) { return alternar(inst, 'quienAbierto', 'quienTog'); }", _leer("static/ot_fin_motor.js"))
        # nada se pierde: con el desglose cerrado, servicio y despacho de cada lado siguen en la página
        for texto in ("Instalación o servicio", "$1.000.000", "$1.400.000", "$600.000", "$500.000"):
            self.assertIn(texto, cerrado, texto)

    def test_los_repuestos_se_explican_en_el_desglose(self):
        """«Nos cobró» (la franja) es lo del proveedor; los repuestos suman aparte en «Nos cobraron» de La cuenta: el desglose lo dice."""
        rec, pan = _ejemplo_de_daniel()
        rec["fin"]["me_cobraron"].update(repuestos=40000, total=1540000)
        rec["fin"]["queda"].update(total=460000, pct=23.0)
        html = self.pintar(rec, pan)
        q = self.quien(html)
        self.assertIn('<b class="fm-q-v">$1.500.000</b>', self.linea(q))
        self.assertIn("Además, repuestos instalados $40.000: suman en «Nos cobraron» de La cuenta", self.desglose(q))
        self.assertIn('<div class="fm-res-i"><small>Nos cobraron</small><b>$1.540.000</b></div>', self.cuenta(html))

    @staticmethod
    def _clp(n):
        return "$" + f"{n:,}".replace(",", ".")


class TestUnBotonPorAccion(_Motor):
    """Cada escenario: «Agregar factura, boleta o nota de venta» existe UNA vez como botón (o ninguna si no se puede agregar), y ninguna acción se
    dibuja dos veces como botón: el recorrido y el rechazo enlazan al botón único."""

    def _escenarios(self):
        def edita(rec, pan):
            pan.update(puede_editar=True)
            return rec, pan
        sin_docs = edita(*_caso("sin_docs"))
        solo_nv = edita(*_caso("cobro_2docs"))
        solo_nv[1]["documentos"] = [_doc(1, "Nota de venta", "VD", "10618", [], [], "nota_venta", cat="nota_venta", saldo=False)]
        solo_nv[0]["pasos"][1] = {"n": 2, "titulo": "Factura", "estado": "falta", "texto": "Solo hay nota de venta (VD 10618): es provisoria, falta la factura para poder cerrar."}
        con_factura = edita(*_caso("cobro_2docs"))
        cerrada = _caso("cobro_2docs")
        cerrada[0].update(cerrada=True)
        cerrada[1].update(cerrada=True, puede_editar=False, puede_regularizar=True)
        garantia = edita(*_caso("garantia_nv"))
        interna = edita(*_caso("interna_cerrada"))
        # todo lo que falta a la vez: $0 sin autorización, factura, cobro, costo y cierre
        todo = edita(*_caso("sin_docs"))
        todo[0]["pasos"][0] = {"n": 1, "titulo": "¿Se cobra?", "estado": "falta", "texto": "Garantía: no se le cobra. Falta la autorización de Daniel."}
        todo[0]["pasos"][3] = {"n": 4, "titulo": "Me cobró el proveedor", "estado": "falta", "texto": "Falta lo que te cobró el técnico/proveedor"}
        return {"sin documentos": (sin_docs, 1), "solo nota de venta": (solo_nv, 1), "con factura": (con_factura, 1), "OT cerrada": (cerrada, 1),
                "garantía": (garantia, 1), "interna sin cliente": (interna, 0), "todo falta": (todo, 1)}

    def test_agregar_factura_existe_una_sola_vez_en_cada_escenario(self):
        for nombre, ((rec, pan), esperado) in self._escenarios().items():
            for modo in ("ficha", "modal"):
                html = self.pintar(copy.deepcopy(rec), copy.deepcopy(pan), modo=modo)
                self.assertEqual(html.count('data-fm-act="ligarDoc"'), esperado, f"{nombre} ({modo})")
                botones = re.findall(r"<button[^>]*>(?:<i[^>]*></i>)?\s*Agregar factura[^<]*</button>", html)
                self.assertEqual(len(botones), esperado, f"{nombre} ({modo}): {botones}")
                if esperado:
                    cab = html.split("<h3>Documentos <small>")[1].split("</div>")[0]
                    self.assertIn('data-fm-act="ligarDoc"', cab, f"{nombre}: el botón único vive en el encabezado de «Documentos»")

    def test_ninguna_accion_se_dibuja_dos_veces_como_boton(self):
        for nombre, ((rec, pan), _n) in self._escenarios().items():
            for modo in ("ficha", "modal"):
                html = self.pintar(copy.deepcopy(rec), copy.deepcopy(pan), modo=modo)
                for act in ACCIONES_UNICAS:
                    self.assertLessEqual(html.count(f'data-fm-act="{act}"'), 1, f"{nombre} ({modo}): «{act}» repetido")

    def test_el_recorrido_enlaza_al_boton_unico_en_vez_de_repetirlo(self):
        (rec, pan), _n = self._escenarios()["todo falta"]
        html = self.pintar(rec, pan)
        pasos = html.split('<ol class="fm-pasos">')[1].split("</ol>")[0]
        for act, texto in (("pedirCero", "Pedir la autorización del $0: en «Modificar»"), ("ligarDoc", "Agregar la factura o boleta: en «Documentos»"),
                           ("corregirProv", "Declarar lo que cobró el proveedor: en «Modificar»"), ("pedirCierre", "Pedir autorización a Daniel: en «Modificar»")):
            self.assertIn(f'data-fm-act="irA" data-fm-ir="{act}"', pasos, act)
            self.assertIn(texto, pasos, act)
            self.assertNotIn(f'data-fm-act="{act}"', pasos, act)
        # lo que solo vive en el paso (declarar el cobro) sigue siendo su botón
        self.assertIn('data-fm-act="declararCobro"', pasos)

    def test_sin_permiso_de_modificar_el_paso_conserva_su_boton(self):
        """REGLA #4.2: si el botón único no existe, el paso no queda sin salida (aquí: OT cerrada con permiso de regularizar y paso ① en falta)."""
        rec, pan = _caso("garantia_nv")
        rec["pasos"][0] = {"n": 1, "titulo": "¿Se cobra?", "estado": "falta", "texto": "Garantía: falta la autorización de Daniel."}
        rec.update(cerrada=True)
        pan.update(cerrada=True, puede_editar=False, puede_regularizar=True)
        pasos = self.pintar(rec, pan).split('<ol class="fm-pasos">')[1].split("</ol>")[0]
        self.assertIn('class="fm-btn fm-btn-chip" data-fm-act="pedirCero"', pasos, "«Modificar» de una OT cerrada no ofrece el $0: el paso conserva su botón")

    def test_el_rechazo_del_cierre_enlaza_al_boton_unico(self):
        (rec, pan), _n = self._escenarios()["solo nota de venta"]
        html = self.pintar(rec, pan, modo="modal",
                           rechazo={"error": "Falta la factura o boleta.", "accion": {"tipo": "ligar_factura", "label": "Agregar la factura o boleta"}})
        caja = html.split('<div class="fm-rechazo" id="fmRechazo">')[1].split("</div></div>")[0] if "fmRechazo" in html else ""
        self.assertIn("No se pudo cerrar la OT", caja)
        self.assertIn('data-fm-act="irA" data-fm-ir="ligarDoc"', caja)
        self.assertIn("Agregar la factura o boleta: en «Documentos»", caja)
        self.assertEqual(html.count('data-fm-act="ligarDoc"'), 1)
        # declarar el cobro: el rechazo enlaza al botón del paso ③ (que es el único)
        rec2, pan2 = _caso("sin_docs")
        html2 = self.pintar(rec2, pan2, modo="modal", rechazo={"error": "Falta lo cobrado.", "accion": {"tipo": "declarar_cobro", "label": "Declarar lo que cobré"}})
        self.assertEqual(html2.count('data-fm-act="declararCobro"'), 1)
        self.assertIn('data-fm-ir="declararCobro"', html2.split('id="fmRechazo"')[1].split('<ol class="fm-pasos">')[0])

    def test_en_el_modal_usar_el_valor_del_anexo_no_se_repite(self):
        rec, pan = _ejemplo_de_daniel()
        pan.update(puede_editar=True)
        rec["anexo"].update(vinculo="difiere", costo_txt="$1.200.000")
        rec["pasos"][3] = {"n": 4, "titulo": "Me cobró el proveedor", "estado": "falta", "texto": "No coincide con el Anexo N° 221."}
        html = self.pintar(rec, pan, modo="modal", anexo_fn=True)
        self.assertEqual(html.count('data-fm-act="usarAnexo"'), 1, "el botón vive en la línea del anexo")
        self.assertIn('data-fm-ir="usarAnexo"', html.split('<ol class="fm-pasos">')[1].split("</ol>")[0])

    def test_el_enlace_lleva_al_boton_unico_y_lo_resalta(self):
        js = _leer("static/ot_fin_motor.js")
        f = js.split("function irA(inst, act) {")[1].split("\n  }")[0]
        self.assertIn("inst.masAbierto = true", f, "si el botón está detrás de «Más opciones», se abre")
        self.assertIn("scrollIntoView", f)
        self.assertIn("fm-resalta", f)
        self.assertIn(".focus(", f)
        self.assertIn("return fn(inst)", f, "si el botón único no está, el enlace hace la acción: nunca queda sin salida")
        self.assertIn("closest('.fm-rechazo')", js.split("function botonUnico(inst, act) {")[1].split("\n  }")[0])
        css = _leer("static/ot_fin_motor.css")
        self.assertIn(".fm-resalta{", css)
        self.assertIn("@media (prefers-reduced-motion:reduce){.fm-resalta{animation:none}}", css)

    def test_cada_punto_de_entrada_sigue_usando_el_flujo_de_siempre(self):
        """El botón único, la sugerencia de «Trazabilidad en Random», la bandeja Regularizar y «Agregar otro documento» de Corregir finanzas llaman al
        MISMO diálogo (ligarDoc / OTFinMotor.ligarDocumento)."""
        js = _leer("static/ot_fin_motor.js")
        self.assertIn("ligarDoc: function (inst) { return ligarDoc(inst); }", js)
        self.assertIn("ligarDocumento: function (vid, o) {", js)
        self.assertIn("global.OTFinMotor.ligarDocumento(vid, {", _leer("static/ot_doc_cadena.js"))
        self.assertIn("window.OTFinMotor.ligarDocumento(parseInt(t.getAttribute('data-lig'), 10)", _leer("static/ot_regularizar.js"))
        self.assertIn("window.OTFinMotor.ligarDocumento(OTD_VID", _leer("templates/ot2/detalle.html"))


class TestDocumentosChicos(_Motor):
    def test_tarjeta_chica_con_rol_fecha_total_y_el_detalle_detras_de_un_toque(self):
        rec, pan = _caso("cobro_2docs")
        pan["documentos"].append(_doc(3, "Nota de venta", "VD", "10618", [], [], "nota_venta", cat="nota_venta", saldo=False))
        pan["documentos"][2]["dada_de_baja_por"] = {"id": 1, "titulo": "FCV 11439"}
        html = self.pintar(rec, pan)
        fcv = html.split('<article class="fm-doc cat-cobro" data-doc="1">')[1].split("</article>")[0]
        resumen = fcv.split('<div class="fm-doc-det"')[0]
        self.assertIn("<h4>FCV 11439</h4>", resumen)
        self.assertIn('<span class="fm-rol r-cobro">Documento de cobro · servicio</span>', resumen)
        self.assertIn('<span class="fm-doc-f">12/09/2026</span>', resumen)
        self.assertIn('<span class="fm-doc-m">Total $959.000</span>', resumen)
        self.assertIn("Aporta al cobro: servicio $252.101", resumen)
        self.assertIn('data-fm-act="docTog" data-fm-doc="1" aria-expanded="false" aria-controls="fmDoc1_1"', resumen)
        # el detalle está en la página (completo), oculto hasta el toque
        self.assertIn('<div class="fm-doc-det" id="fmDoc1_1" hidden>', fcv)
        det = fcv.split('<div class="fm-doc-det" id="fmDoc1_1" hidden>')[1]
        for texto in ("Fecha de emisión", "Lo agregó", "Víctor Betancur", "Servicio y despacho del documento"):
            self.assertIn(texto, det)
        # la nota de venta: «Referencia de la venta» y su trazabilidad a la vista (dada de baja por la factura)
        nv = html.split('<article class="fm-doc cat-nota_venta baja" data-doc="3">')[1].split('<div class="fm-doc-det"')[0]
        self.assertIn('<span class="fm-rol r-ref">Referencia de la venta</span>', nv)
        self.assertIn("Dada de baja por FCV 11439", nv)

    def test_un_toque_abre_el_detalle_y_otro_lo_cierra(self):
        rec, pan = _caso("cobro_2docs")
        abierto = self.pintar(rec, pan, clics=("docTog:1",))
        self.assertIn('<div class="fm-doc-det" id="fmDoc1_1">', abierto)
        self.assertIn('data-fm-doc="1" aria-expanded="true"', abierto)
        self.assertIn("Ocultar detalle", abierto)
        self.assertIn('<div class="fm-doc-det" id="fmDoc1_2" hidden>', abierto, "los demás siguen cerrados")
        cerrado = self.pintar(rec, pan, clics=("docTog:1", "docTog:1"))
        self.assertIn('<div class="fm-doc-det" id="fmDoc1_1" hidden>', cerrado)

    def test_sin_details_ni_acordeon_ni_texto_cortado(self):
        js, css = _leer("static/ot_fin_motor.js"), _leer("static/ot_fin_motor.css")
        self.assertNotIn("<details", js)
        self.assertNotIn("collapse", js)
        self.assertIn(".fm-doc-det[hidden]{display:none !important}", css)
        self.assertIn("min-height:44px;flex:1 1 100%;justify-content:center;margin-left:0", css.split("@media (max-width:560px)")[-1],
                      "«Ver detalle» es táctil en el celular")


class TestServidor(unittest.TestCase):
    """El panorama trae quién nos cobró en la MISMA consulta y solo después de cortar a un técnico (REGLAS #19/#26)."""

    def test_el_panorama_corta_al_tecnico_antes_de_leer_y_trae_la_empresa_en_la_misma_consulta(self):
        src = FA.fuente_funcion("ot_api_panorama")
        self.assertLess(src.index("if _es_rol_tecnico():"), src.index("mysql_fetchone("))
        self.assertEqual(src.count("mysql_fetchone("), 1, "ninguna consulta nueva por OT")
        self.assertIn("JOIN mant_tecnicos_externos tee ON tee.id = teu.tecnico_externo_id", src)
        self.assertIn("AS tecnico_empresa", src)
        self.assertIn('"proveedor": {"nombre": (v.get("tecnico") or "").strip(), "externo": ext,', src)

    def _panorama(self, rol_tecnico, fila):
        import ot_saldo_servicio
        llamadas = []

        def fetchone(sql, p=()):
            llamadas.append(sql)
            return dict(fila)
        amb = FA.cargar(funciones=("_ot_interna_sin_cliente", "ot_api_panorama"), extra={
            "_es_rol_tecnico": lambda: rol_tecnico, "jsonify": lambda d: d, "mysql_fetchone": fetchone, "_ot_fin_cols_sql": lambda a="v": "v.cliente_id",
            "_ot_panorama_docs": lambda vid, v: [], "_saldo": ot_saldo_servicio, "_ot_cobertura": lambda v: "cobra",
            "_ot_repuestos_desglose": lambda ids: {}, "_ot_puede_finanzas_cierre": lambda *a, **k: True, "_ot_puede_regularizar": lambda: True,
            "_ot_aut_es_superadmin": lambda: False, "_OT2_CENTROS_COSTO": [("sstt", "Servicio Técnico")], "_OT_PANORAMA_MAX_LECTURAS_ERP": 12,
            "g": type("G", (), {"user": {}})(), "print": lambda *a, **k: None})
        return amb["ot_api_panorama"](461), llamadas

    def test_el_tecnico_recibe_403_y_no_se_lee_nada(self):
        r, llamadas = self._panorama(True, {})
        self.assertEqual(r[1], 403)
        self.assertEqual(llamadas, [])

    def test_la_gestion_recibe_quien_nos_cobro(self):
        fila = {"id": 461, "numero_ot": "OT-2026-00461", "estado": "en_ejecucion", "centro_costo": "sstt", "cliente_id": 7, "cliente": "Gimnasio",
                "proveedor_tipo": "externo", "costo_proveedor": 1000000, "costo_despacho": 500000, "zz_monto": None, "zz_envio_monto": None,
                "factura_asociada_por": None, "estado_facturacion": None, "tecnico": "Daniel Pulgar ", "tecnico_empresa": "DAP Servicio SpA",
                "proveedor_nombre": "Otro nombre"}
        r, _ = self._panorama(False, fila)
        self.assertEqual(r["costos"]["proveedor"], {"nombre": "Daniel Pulgar", "externo": True, "empresa": "DAP Servicio SpA"})
        # sin ficha de empresa, el proveedor que anotó la OT; y un técnico propio no lleva empresa
        r, _ = self._panorama(False, dict(fila, tecnico_empresa=None))
        self.assertEqual(r["costos"]["proveedor"]["empresa"], "Otro nombre")
        r, _ = self._panorama(False, dict(fila, proveedor_tipo="interno"))
        self.assertEqual(r["costos"]["proveedor"], {"nombre": "Daniel Pulgar", "externo": False, "empresa": ""})

    def test_el_vinculo_con_el_anexo_trae_la_empresa_que_lo_firma(self):
        import ot_anexo_costo as O
        ax = {"id": 9, "numero": 221, "estado": "firmado", "items": [{"concepto": "Servicio", "monto": 1500000}], "total": 1500000,
              "compartido": False, "rol": None, "ot_id": 461, "items_general": [], "total_general": 1500000, "proveedor": "DAP Servicio SpA"}
        amb = FA.cargar(funciones=("_ot_anexo_vinculo_vista",), extra={"_oac": O, "_ot_anexo_vivo": lambda vid: ax, "print": lambda *a, **k: None})
        v = amb["_ot_anexo_vinculo_vista"]({"id": 461, "costo_proveedor": 1500000, "costo_despacho": None, "estado": "en_ejecucion"})
        self.assertEqual(v["proveedor"], "DAP Servicio SpA")
        self.assertEqual(v["vinculo"], "alineado")

    def test_el_recorrido_sigue_cerrado_para_tecnicos(self):
        self.assertIn("@_no_tecnico", FA.decoradores("ot_api_recorrido"))


if __name__ == "__main__":
    unittest.main()
