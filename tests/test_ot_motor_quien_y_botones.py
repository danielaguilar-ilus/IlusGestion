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


class TestQuienSeAdjudicaElDinero(_Motor):
    def test_el_ejemplo_de_daniel_nos_cobro_cobramos_y_el_margen(self):
        rec, pan = _ejemplo_de_daniel()
        html = self.pintar(rec, pan)
        q = self.quien(html)
        # va arriba de todo: después de la frase y antes de La cuenta | El recorrido
        self.assertLess(html.index('<section class="fm-adj"'), html.index('<div class="fm-top'))
        self.assertIn("¿Quién se adjudica el dinero?", q)
        # 1) Nos cobró: la persona, su empresa (la del anexo) y el anexo; total y debajo servicio y despacho
        prov = q.split('<div class="fm-q prov')[1].split('<div class="fm-q ')[0]
        self.assertIn('<small class="fm-q-t">Nos cobró</small><b class="fm-q-n">Daniel Pulgar</b><span class="fm-q-e">DAP Servicio SpA · proveedor externo</span>', prov)
        self.assertIn('<b class="fm-q-v">$1.500.000</b>', prov)
        # 2026-10-29 (v5, Daniel: «todo bien resumido… la información extra se abre con clic»): servicio y despacho van en el desglose (oculto hasta el clic)
        self.assertIn('<div class="fm-q-det" hidden><ul class="fm-q-l"><li><span class="fm-q-r">Instalación o servicio</span><b>$1.000.000</b></li>', prov)
        self.assertIn('<li><span class="fm-q-r">Despacho</span><b>$500.000</b></li>', prov)
        # …y el anexo queda a la vista, junto a la cifra (chip azul: firmado y alineado)
        self.assertIn('<b class="fm-q-v">$1.500.000</b><span class="fm-q-ds"><span class="fm-lock"><i class="bi bi-lock-fill"></i> Anexo N° 221 · firmado</span></span>', prov)
        # 2) Cobramos a: el cliente, el total y, JUNTO al total, el chip chico de la factura que lo lleva (una vez, aunque cobre servicio y despacho)
        cli = q.split('<div class="fm-q cli')[1].split("</div>")[0]
        fac = '<em class="fm-q-doc" title="FCV 11567: documento de cobro">Factura N° 11567</em>'
        self.assertIn('<small class="fm-q-t">Cobramos a</small><b class="fm-q-n">Gimnasio Las Condes SpA</b><b class="fm-q-v">$2.000.000</b>'
                      '<span class="fm-q-ds">' + fac + '</span>', cli)
        # en el desglose, cada línea con el documento que la cobra
        self.assertIn('<span class="fm-q-r">Instalación o servicio</span><b>$1.400.000</b><span class="fm-q-ds">' + fac + '</span>', cli)
        self.assertIn('<span class="fm-q-r">Despacho</span><b>$600.000</b><span class="fm-q-ds">' + fac + '</span>', cli)
        # 3) se cobra: no hay línea «ILUS · interno»
        self.assertNotIn("ILUS · interno", q)
        # el margen: 25 % = tolerancia (REGLA #57), con su semáforo chico («Tolerancia»: verde) y la frase completa en el tooltip y el desglose
        self.assertIn('<div class="fm-q-m m-tol" role="status"><span class="fm-q-mt">Margen</span><span class="fm-q-mn"><b class="fm-q-mv">Ganamos $500.000</b>'
                      '<em class="fm-q-mp">25 %</em></span><span class="fm-q-pol" title="En tolerancia: entre 20 y 30 %">Tolerancia</span>', q)
        self.assertIn('<span class="fm-q-cu">Cobramos $2.000.000 − nos costó $1.500.000</span><span class="fm-q-frase">En tolerancia: entre 20 y 30 %</span>', q)
        self.assertTrue(html.split('<section class="fm-cta ')[1].startswith("c-tol"), "La cuenta lleva el mismo color que el margen")

    def test_en_garantia_no_se_cobra_nos_costo_y_lo_asume_ilus_como_una_linea(self):
        rec, pan = _ejemplo_de_daniel(cobra=False)
        q = self.quien(self.pintar(rec, pan))
        cli = q.split('<div class="fm-q cli nocobra"')[1].split('<div class="fm-q ')[0]
        self.assertIn('<b class="fm-q-v">No se cobra</b>', cli)
        self.assertIn('<span class="fm-q-cost">nos costó $1.500.000</span>', cli)
        self.assertIn('<span class="fm-motivo">Garantía</span>', cli)
        ilus = q.split('<div class="fm-q ilus"')[1]
        self.assertIn('<small class="fm-q-t">ILUS · interno</small><b class="fm-q-n">Lo asume ILUS</b>', ilus)
        self.assertIn("Centro de costo: Servicio Técnico", ilus)
        self.assertIn('<b class="fm-q-v">$1.500.000</b>', ilus)
        self.assertEqual(q.count('class="fm-q ilus"'), 1, "UNA línea de cobro interno")
        self.assertNotIn('class="fm-q-m', q, "sin cobro no hay margen que medir (nunca se inventa un cobro ni un $0)")

    def test_trabajo_interno_solo_quien_lo_hizo_y_la_linea_ilus_sin_cliente(self):
        rec, pan = _caso("interna_cerrada")
        pan["costos"]["registros"][0]["detalle"] = "Jaizer Rodríguez · técnico propio"
        pan["costos"]["proveedor"] = {"nombre": "Jaizer Rodríguez", "externo": False, "empresa": ""}
        q = self.quien(self.pintar(rec, pan))
        self.assertIn('<small class="fm-q-t">Lo hizo un técnico propio</small><b class="fm-q-n">Jaizer Rodríguez</b>', q)
        self.assertIn("No se le paga aparte", q)
        self.assertIn('<small class="fm-q-t">ILUS · interno</small><b class="fm-q-n">Trabajo interno de ILUS</b>', q)
        self.assertNotIn("Cobramos a", q, "un trabajo interno no tiene cliente")
        self.assertNotIn('class="fm-q-m', q)

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
            prov = self.quien(html).split('<div class="fm-q prov')[1].split('<div class="fm-q ')[0]
            self.assertNotIn("ILUS", prov, "quien nos cobró es la persona de la OT, nunca «ILUS»")

    def test_sin_cobro_declarado_falta_declararlo_en_ambar_y_la_nota_de_venta_es_la_referencia(self):
        rec, pan = _caso("cobro_2docs")
        rec["fin"]["cobre"].update(servicio=0, despacho=0, total=0, hay=False)
        rec["fin"]["queda"].update(mostrar=False)
        pan["documentos"] = [_doc(1, "Nota de venta", "VD", "10618", [], [], "nota_venta", cat="nota_venta", saldo=False)]
        q = self.quien(self.pintar(rec, pan))
        cli = q.split('<div class="fm-q cli falta"')[1].split('<div class="fm-q-m')[0]
        self.assertIn('<b class="fm-q-v"><span class="fm-pend">Falta declararlo</span></b>', cli)
        # 2026-10-29 (v5): la nota de venta en su chip AZUL (referencia de la venta) y el ÁMBAR de lo que falta, a la vista junto a lo cobrado
        self.assertIn('<span class="fm-q-ds"><em class="fm-q-doc ref" title="VD 10618: referencia de la venta, no es el documento de cobro">'
                      'Nota de venta N° 10618 · referencia de la venta</em><em class="fm-q-doc falta">Falta la factura o boleta</em></span>', cli)
        self.assertIn('<b class="fm-q-mv">Falta un dato para calcularlo</b>', q)
        self.assertIn(">Sin medir</span>", q)
        self.assertNotIn("Ganamos", q)

    def test_la_politica_de_margen_de_la_regla_57(self):
        casos = ((35.0, 700000, "m-ok", "Margen objetivo: 30 % o más"), (25.0, 500000, "m-tol", "En tolerancia: entre 20 y 30 %"),
                 (15.0, 300000, "m-bajo", "Margen bajo: menos de 20 % (el objetivo es 30 %)"), (-5.0, -100000, "m-rojo", "Pérdida"),
                 (0.0, 0, "m-bajo", "Margen bajo"))
        for pct, total, clase, txt in casos:
            rec, pan = _ejemplo_de_daniel()
            rec["fin"]["queda"].update(pct=pct, total=total)
            q = self.quien(self.pintar(rec, pan))
            self.assertIn(f'<div class="fm-q-m {clase}" role="status">', q, pct)
            self.assertIn(txt, q, pct)
        # si el servidor manda su política (ot_margen_politica.py), manda la del servidor
        rec, pan = _ejemplo_de_daniel()
        rec["fin"]["queda"].update(pct=35.0, total=700000)
        rec["fin"]["politica_margen"] = {"objetivo": 40, "tolerancia": 25}
        q = self.quien(self.pintar(rec, pan))
        self.assertIn('<div class="fm-q-m m-tol" role="status">', q)
        self.assertIn("En tolerancia: entre 25 y 40 %", q)
        js = _leer("static/ot_fin_motor.js")
        self.assertIn("var POLITICA_MARGEN = { objetivo: 30, tolerancia: 20 };", js, "UNA constante, fácil de reemplazar")

    def test_el_anexo_que_no_coincide_se_dice_en_ambar(self):
        rec, pan = _ejemplo_de_daniel()
        rec["anexo"].update(vinculo="difiere", costo_txt="$1.200.000")
        prov = self.quien(self.pintar(rec, pan)).split('<div class="fm-q prov')[1].split('<div class="fm-q ')[0]
        self.assertIn('<span class="fm-lock no"><i class="bi bi-lock-fill"></i> Anexo N° 221 · firmado · no coincide con la OT</span>', prov)
        # 2026-10-29 (v5): el semáforo del anexo. «no coincide» en rojo suave; vigente o «la OT aún no lo declara», en ámbar
        rec, pan = _ejemplo_de_daniel()
        rec["anexo"].update(vinculo="sin_declarar", costo_txt="")
        self.assertIn('<span class="fm-lock no sd"><i class="bi bi-lock-fill"></i> Anexo N° 221 · firmado · la OT aún no lo declara</span>', self.quien(self.pintar(rec, pan)))
        rec, pan = _ejemplo_de_daniel()
        rec["anexo"].update(firmado=False, estado="vigente")
        html = self.pintar(rec, pan)
        self.assertIn('<span class="fm-lock vig"><i class="bi bi-lock-fill"></i> Anexo N° 221 · vigente</span>', self.quien(html))
        self.assertIn('<span class="fm-lock vig" title=', html.split('<section class="fm-cta ')[1], "La cuenta dice lo mismo, con el mismo color")
        css = _leer("static/ot_fin_motor.css")
        self.assertIn(".fm-lock.vig,.fm-lock.no.sd{background:var(--fm-ambar-f);color:#78350f}", css)
        self.assertIn(".fm-lock.no{background:var(--fm-rojo-f);color:#991b1b}", css)

    def test_nada_se_corta_en_el_encabezado(self):
        css = _leer("static/ot_fin_motor.css")
        bloque = css.split("/* ═══ v4 · 2026-10-29")[1].split("/* Bloque angosto")[0]
        self.assertNotIn("text-overflow", bloque)
        self.assertNotIn("line-clamp", bloque)
        for regla in (".fm-q-n{", ".fm-q-e{", ".fm-q-r{"):
            self.assertIn("overflow-wrap:anywhere", bloque.split(regla)[1].split("}")[0], regla)
        self.assertIn("grid-template-columns:repeat(auto-fit,minmax(min(100%,220px),1fr))", bloque.split(".fm-q-grid{")[1].split("}")[0])


class TestResumidoConSemaforo(_Motor):
    """v5 — 2026-10-29. Daniel, viendo la v4: «me hubiese gustado… un poco más de COLOR y se reconozca a simple vista cuánto cobré, en qué factura, el
    margen»; y al ver un primer intento muy vistoso: «no tan colorido; ya habíamos puesto hasta un semáforo… no tarjetas tan grandes; todo bien resumido».
    Lo que se fija: cada bloque en dos líneas sobre fondo gris suave; el color SOLO en chips chicos (el documento junto a lo cobrado, el anexo, el semáforo
    del margen con la banda de la REGLA #57); el resto en «Ver el desglose», en la página y oculto hasta el clic (nada se pierde)."""

    def test_el_semaforo_del_margen_sigue_la_politica_de_la_regla_57(self):
        # verde = 20 % o más (objetivo o tolerancia) · ámbar = bajo · rojo = pérdida
        for pct, total, clase, palabra in ((35.0, 700000, "m-ok", "Objetivo"), (25.0, 500000, "m-tol", "Tolerancia"),
                                           (15.0, 300000, "m-bajo", "Margen bajo"), (-5.0, -100000, "m-rojo", "Pérdida")):
            rec, pan = _ejemplo_de_daniel()
            rec["fin"]["queda"].update(pct=pct, total=total)
            q = self.quien(self.pintar(rec, pan))
            m = q.split(f'<div class="fm-q-m {clase}" role="status">')[1]
            self.assertIn(f'">{palabra}</span>', m.split('<div class="fm-q-det"')[0], pct)
            self.assertIn('<em class="fm-q-mp">' + (str(pct).replace(".0", "").replace("-", "−")) + ' %</em>', m, pct)
        # gris = sin medir: ni % ni banda inventados
        rec, pan = _ejemplo_de_daniel()
        rec["fin"]["queda"].update(mostrar=False, pct=None)
        q = self.quien(self.pintar(rec, pan))
        self.assertIn('<div class="fm-q-m m-falta" role="status">', q)
        self.assertIn(">Sin medir</span>", q)
        self.assertNotIn('class="fm-q-mp"', q)
        css = _leer("static/ot_fin_motor.css")
        self.assertIn(".fm-q-m.m-ok,.fm-q-m.m-tol{--sem:#16a34a;", css)
        self.assertIn(".fm-q-m.m-bajo{--sem:#f59e0b;", css)
        self.assertIn(".fm-q-m.m-rojo{--sem:#dc2626;", css)
        self.assertIn("background:var(--sem,#9ca3af)", css.split(".fm-q-pol::before{")[1].split("}")[0], "sin banda, el punto es gris")

    def test_la_politica_del_motor_es_la_de_ot_margen_politica(self):
        import ot_margen_politica as POL
        js = _leer("static/ot_fin_motor.js")
        m = re.search(r"var POLITICA_MARGEN = \{ objetivo: (\d+(?:\.\d+)?), tolerancia: (\d+(?:\.\d+)?) \};", js)
        self.assertTrue(m)
        self.assertEqual((float(m.group(1)), float(m.group(2))), (POL.OBJETIVO_PCT, POL.TOLERANCIA_PCT))
        self.assertEqual(POL.banda(25), "tolerancia")      # (el motor la llama «tol»: misma banda)

    def test_el_chip_del_documento_va_junto_a_lo_cobrado(self):
        # factura + boleta: los dos chips verdes junto al total
        q = self.quien(self.pintar(*_caso("cobro_2docs")))
        resumen = q.split('<div class="fm-q cli"')[1].split('<div class="fm-q-det"')[0]
        self.assertIn('<b class="fm-q-v">$302.101</b><span class="fm-q-ds"><em class="fm-q-doc" title="FCV 11439: documento de cobro">Factura N° 11439</em>'
                      '<em class="fm-q-doc" title="BLV 23375: documento de cobro">Boleta N° 23375</em></span>', resumen)
        # cobra solo con la nota de venta (sus líneas aportan): chip AZUL de referencia + ÁMBAR «Falta la factura o boleta», junto al total y en cada línea
        rec, pan = _caso("cobro_2docs")
        pan["documentos"] = [_doc(1, "Nota de venta", "VD", "10618", [_linea("Instalación", "ZZINSTALACION", 252101)], [_linea("Despacho", "ZZENVIO", 50000)],
                                  "nota_venta", cat="nota_venta")]
        q = self.quien(self.pintar(rec, pan))
        nv = ('<em class="fm-q-doc ref" title="VD 10618: referencia de la venta, no es el documento de cobro">Nota de venta N° 10618 · referencia de la venta</em>'
              '<em class="fm-q-doc falta">Falta la factura o boleta</em>')
        self.assertEqual(q.count(nv), 3, "junto al total, y en servicio y despacho del desglose")
        self.assertNotIn('<em class="fm-q-doc" title=', q, "ningún chip verde: la nota de venta no es el documento de cobro")
        # garantía: la venta de referencia en azul, dentro del desglose
        q = self.quien(self.pintar(*_ejemplo_de_daniel(cobra=False)))
        self.assertIn('<div class="fm-q-det" hidden><span class="fm-q-ds"><em class="fm-q-doc ref" title="NVV 4521: referencia de la garantía, no es el documento de cobro">'
                      'Nota de venta N° 4521 · referencia de la garantía</em></span></div>', q)
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
        # el chip ⑤ (Margen) del recorrido plegado lleva el semáforo de su banda (17,2 % → bajo)
        chips = html.split('<span class="fm-rec-chips">')[1]
        self.assertIn('<span class="fm-pchip hecho m-bajo" title="5. Margen:', chips)
        self.assertIn(".fm-pchip.m-bajo{background:var(--fm-ambar-f)", css)

    def test_el_desglose_se_abre_y_se_cierra_con_un_clic(self):
        rec, pan = _ejemplo_de_daniel()
        cerrado = self.quien(self.pintar(rec, pan))
        self.assertIn('<button type="button" class="fm-adj-ver" data-fm-act="quienTog" aria-expanded="false" aria-controls="fmQuien1">Ver el desglose', cerrado)
        self.assertEqual(cerrado.count('<div class="fm-q-det" hidden>'), 3, "Nos cobró, Cobramos a y el margen")
        abierto = self.quien(self.pintar(rec, pan, clics=("quienTog",)))
        self.assertIn('data-fm-act="quienTog" aria-expanded="true" aria-controls="fmQuien1">Ocultar el desglose', abierto)
        self.assertNotIn('<div class="fm-q-det" hidden>', abierto)
        self.assertEqual(abierto.count('<div class="fm-q-det">'), 3)
        otra = self.quien(self.pintar(rec, pan, clics=("quienTog", "quienTog")))
        self.assertEqual(otra.count('<div class="fm-q-det" hidden>'), 3)
        css = _leer("static/ot_fin_motor.css")
        self.assertIn(".fm-q-det[hidden]{display:none !important}", css)            # también en el modal de cierre (fuera de #otdCardMotor)
        self.assertIn(".fm-adj-ver{min-height:44px", css.split("@media (max-width:560px)")[-1], "táctil en el celular")
        self.assertIn("quienTog: function (inst) { return alternar(inst, 'quienAbierto', 'quienTog'); }", _leer("static/ot_fin_motor.js"))

    def test_resumido_bloques_de_dos_lineas_sin_tarjetas_teñidas_ni_numeros_gigantes(self):
        css = _leer("static/ot_fin_motor.css")
        bloque = css.split("/* ═══ v4 · 2026-10-29")[1].split("/* Bloque angosto")[0]
        self.assertNotIn("border-left:", css.replace("border-left:0", ""), "ningún borde lateral de acento")
        self.assertNotIn("border-top:3px", bloque)
        caja = css.split(".fm-q,.fm-q-m{")[1].split("}")[0]
        self.assertIn("background:#fafafa", caja, "fondo gris suave: el color vive en los chips")
        self.assertIn("flex-wrap:wrap", caja)
        self.assertIn("order:4;flex-basis:100%", css.split(".fm-q::before,.fm-q-m::before{")[1].split("}")[0], "dos líneas: el ::before parte la fila")
        for regla in ("\n.fm-q-v{", "\n.fm-q-mv{", "\n.fm-q-mp{"):
            tam = float(re.search(r"font-size:([\d.]+)rem", css.split(regla)[1].split("}")[0]).group(1))
            self.assertLessEqual(tam, 1.1, regla + " sin números gigantes")
        self.assertIn("background:#fff", css.split("\n.fm-cta{")[1].split("}")[0], "La cuenta: blanca")
        self.assertIn("background:#fff", css.split("\n.fm-doc{")[1].split("}")[0], "la tarjeta chica del documento: blanca")
        # nada se pierde: con el desglose cerrado, servicio, despacho, la cuenta y la frase de la política siguen en la página
        rec, pan = _ejemplo_de_daniel()
        q = self.quien(self.pintar(rec, pan))
        for texto in ("Instalación o servicio", "$1.000.000", "$1.400.000", "$600.000", "Cobramos $2.000.000 − nos costó $1.500.000", "En tolerancia: entre 20 y 30 %"):
            self.assertIn(texto, q, texto)


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
