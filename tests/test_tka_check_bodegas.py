# -*- coding: utf-8 -*-
"""2026-10-24 · Daniel: «todos los formularios con este modal [Búsqueda avanzada de productos] deberán notificar en qué bodega se encuentra el
producto… con Check podemos identificar las bodegas… que este motor sea un motor único».

Un solo motor: `retiros_check.respuesta_bodegas` arma la respuesta de las DOS rutas (Retiros y la general de todos los formularios) y el modal
compartido (templates/tickets/_tka_modal.html) pinta la bodega de Check de cada producto con UNA función. Check sigue SOLO LECTURA (REGLA #4.4):
se usa el reporte de stock que ya estaba en caché; aquí, filas de mentira.
"""
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RAIZ)
NODE = shutil.which("node")

import retiros_check as RC  # noqa: E402


def _leer(*partes):
    with open(os.path.join(RAIZ, *partes), encoding="utf-8", newline="") as fh:
        return fh.read().replace("\r\n", "\n")


TKA = _leer("templates", "tickets", "_tka_modal.html")
APP = _leer("app.py")
PICKUPS = _leer("pickups_module.py")

FILAS = [
    {"codigo": "A", "bodega": "BODEGA 02", "ubicacion": "PISOD-12", "ua": "UA1", "stFisico": "3", "disponible": "3"},
    {"codigo": "A", "bodega": "BODEGA EXTERNA LAMPA", "ubicacion": "RACK-3", "ua": "UA2", "stFisico": 2, "disponible": 2},
    {"codigo": "B", "bodega": "BODEGA 02", "ubicacion": "PISOD-01", "ua": "UA3", "stFisico": 1, "disponible": 0},
]


class TestMotorUnico(unittest.TestCase):
    def test_sin_skus_no_espera_a_check(self):
        for txt in ("", None, " , ,"):
            self.assertEqual(RC.respuesta_bodegas(txt, None), ({"ok": True, "bodegas": {}, "edad_min": None}, 200))
        self.assertEqual(RC.skus_de(" a, b ,,C"), ["A", "B", "C"])
        self.assertEqual(len(RC.skus_de(",".join("S%d" % i for i in range(200)))), 80)

    def test_check_caido_lo_dice_sin_inventar(self):
        p, st = RC.respuesta_bodegas("A", None)
        self.assertEqual(st, 503)
        self.assertFalse(p["ok"])
        self.assertIn("Check no respondió", p["error"])

    def test_respuesta_con_bodegas_y_antiguedad(self):
        p, st = RC.respuesta_bodegas("a,b,zzz", FILAS, ts_cache=1000.0, ahora=1000.0 + 7 * 60 + 5)
        self.assertEqual(st, 200)
        self.assertTrue(p["ok"])
        self.assertEqual(p["edad_min"], 7)
        self.assertEqual([b["bodega"] for b in p["bodegas"]["A"]], ["BODEGA 02", "BODEGA EXTERNA LAMPA"])
        self.assertEqual(p["bodegas"]["B"][0]["fisico"], 1)
        self.assertEqual(p["bodegas"]["ZZZ"], [])                      # Check no lo tiene: lista vacía (el modal dice «sin stock en Check»)

    def test_antiguedad_rara_nunca_rompe(self):
        self.assertIsNone(RC.respuesta_bodegas("A", FILAS, ts_cache="x", ahora=5)[0]["edad_min"])
        self.assertIsNone(RC.respuesta_bodegas("A", FILAS, ts_cache=None, ahora=5)[0]["edad_min"])

    def test_las_dos_rutas_usan_la_misma_respuesta(self):
        self.assertIn("respuesta_bodegas(txt, filas, _CHECKWMS_STOCK.get(\"ts\"), time.time())", APP)
        self.assertIn("respuesta_bodegas(txt, filas, ts, _t_cb.time())", PICKUPS)
        for src in (APP, PICKUPS):
            self.assertIn("skus_de(txt)", src)


class TestRutaGeneral(unittest.TestCase):
    def setUp(self):
        i = APP.index('@app.route("/api/erp/check-bodegas"')
        self.ruta = APP[i:APP.index("def get_full(product_id):", i)]

    def test_la_ruta_es_de_solo_lectura_y_cualquier_usuario_con_sesion(self):
        self.assertIn('methods=["GET"])\n@login_required\ndef api_check_bodegas():', self.ruta)
        self.assertIn("_checkwms_stock_rows()", self.ruta)             # el reporte YA cacheado
        self.assertNotIn("_checkwms_get(", self.ruta)                   # nunca le habla a Check por su cuenta
        self.assertNotIn("requests.", self.ruta)
        self.assertNotIn("request.method", self.ruta)

    def test_no_expone_plata_ni_proveedores(self):
        # REGLAS #19 y #26: solo DÓNDE está el stock (el código de la función, sin su docstring ni sus comentarios).
        cuerpo = re.sub(r'""".*?"""', "", self.ruta, flags=re.S)
        cuerpo = re.sub(r"#.*", "", cuerpo)
        cuerpo = cuerpo.replace("_oculta_proveedores", "")            # el candado que NIEGA el acceso a los técnicos, no un dato que se entregue
        for palabra in ("mysql", "costo", "precio", "proveedor", "margen", "valor"):
            self.assertNotIn(palabra, cuerpo.lower())

    def test_ningun_tecnico_ve_las_bodegas_de_check(self):
        # REGLA #19 (ni siquiera pueden ver mis proveedores) + Daniel 2026-09-08 (el externo no ve stock): las bodegas de Check son las de un
        # operador externo. TODA la familia técnico queda fuera con la fuente única `_oculta_proveedores()` (revisión independiente 24-oct: antes
        # solo se bloqueaba al externo). El decorador se define MÁS ABAJO que esta ruta (NameError al importar): la revisión va dentro de la función.
        self.assertIn("if _oculta_proveedores():", self.ruta)
        self.assertNotIn("_es_tecnico_externo()", self.ruta)
        self.assertIn('"No tienes acceso a esta información."}), 403', self.ruta)
        self.assertLess(self.ruta.index("if _oculta_proveedores():"), self.ruta.index("_checkwms_stock_rows()"))
        self.assertGreater(APP.index("def _no_tecnico_externo("), APP.index("def api_check_bodegas("))        # por eso no es decorador

    def test_sin_skus_responde_sin_leer_el_reporte(self):
        self.assertLess(self.ruta.index("if not _rck.skus_de(txt):"), self.ruta.index("_checkwms_stock_rows()"))

    def test_la_cache_fria_no_deja_a_nadie_esperando_45_segundos(self):
        # La primera lectura del reporte de Check (instancia recién levantada) tarda 4-45 s: se pide en segundo plano y se responde «calentando».
        self.assertIn('if _CHECKWMS_STOCK.get("rows") is None:', self.ruta)
        self.assertIn('_checkwms_swr("stock", _checkwms_stock_rows)', self.ruta)
        self.assertIn('"calentando": True', self.ruta)
        self.assertIn("}), 503", self.ruta)
        self.assertLess(self.ruta.index('if _CHECKWMS_STOCK.get("rows") is None:'), self.ruta.index("filas = _checkwms_stock_rows()"))
        self.assertLess(self.ruta.index("if not _rck.skus_de(txt):"), self.ruta.index('if _CHECKWMS_STOCK.get("rows") is None:'))
        # sin credenciales no se queda «calentando» para siempre: lo dice
        self.assertIn("_checkwms_configurado()", self.ruta)

    def test_un_reporte_vacio_no_se_guarda_en_la_cache(self):
        i = APP.index("def _checkwms_stock_rows(")
        f = APP[i:APP.index("def _checkwms_precalentar(", i)]
        self.assertIn("if not rows:", f)
        self.assertLess(f.index("if not rows:"), f.index('_CHECKWMS_STOCK.update({"ts": time.time(), "rows": rows})'))

    def test_un_reporte_vacio_se_responde_como_check_no_respondio(self):
        # `[]` ya no es «ningún producto tiene stock»: la bodega real tiene miles de filas, así que es una lectura fallida.
        p, st = RC.respuesta_bodegas("A", [])
        self.assertEqual(st, 503)
        self.assertFalse(p["ok"])
        self.assertIn("Check no respondió", p["error"])
        self.assertEqual(RC.respuesta_bodegas("A", FILAS)[1], 200)        # con filas, igual que siempre


class TestModalCompartido(unittest.TestCase):
    def test_cada_lugar_donde_hay_productos_tiene_su_caja_de_bodega(self):
        # Por documento + Por RUT (líneas del documento), Productos y Catálogo (tarjetas) y Cotización interna.
        self.assertEqual(TKA.count('<div class="ln-bod" data-bod-sku='), 5)
        self.assertEqual(TKA.count("_tkaCargarBodegas("), 6)            # la definición + las 5 pestañas
        for pista in ("_tkaCargarBodegas(cont);", "_tkaCargarBodegas(body);", "_tkaCargarBodegas(itemsRes)",
                      "_tkaCargarBodegas(cont, { resumen: false });"):
            self.assertIn(pista, TKA)
        self.assertEqual(TKA.count("_tkaCargarBodegas(cont, { resumen: false });"), 2)   # Productos y Catálogo: no son un documento

    def test_usa_la_ruta_general_y_no_la_de_retiros(self):
        self.assertTrue("'/api/erp/check-bodegas?skus='" in TKA)
        # el modal lo usan Tickets, OT, Transporte… sin el permiso de Retiros: nunca se le pide a la ruta de Retiros
        self.assertFalse("/retiros/api/check-bodegas?" in TKA, "el modal compartido no debe llamar a la ruta de Retiros")
        self.assertFalse("fetch('/retiros/" in TKA)

    def test_dice_lo_mismo_que_retiros(self):
        for texto in ("¿Dónde está este pedido según Check?", "Está repartido en ", "sin stock en Check", "Sin stock en Check",
                      "Check no respondió: bodega sin verificar", "u. en total", "Buscando la bodega en Check…"):
            self.assertIn(texto, TKA)

    def test_solo_informa_no_toca_la_seleccion(self):
        i = TKA.index("async function _tkaCargarBodegas(")
        f = TKA[i:i + 5200]
        for prohibido in ("selDoc", "selCli", "selBodega", "selCatalogo", "selCotizacion", ".checked", "tkaUpdateCounter"):
            self.assertNotIn(prohibido, f)

    def test_los_nombres_completos_no_se_cortan(self):
        # REGLA #15: el nombre del producto y la razón social, sin «…».
        self.assertNotIn("-webkit-line-clamp:2", TKA.split(".tka-bod-nombre{")[1].split("}")[0])
        self.assertIn("overflow-wrap:anywhere", TKA.split(".tka-bod-nombre{")[1].split("}")[0])
        self.assertNotIn("text-overflow:ellipsis", TKA.split(".tka-lt-cli{")[1].split("}")[0])

    def test_en_celular_la_descripcion_usa_todo_el_ancho(self):
        self.assertIn("@media(max-width:600px){.tka-line .ln-desc{grid-column:2;min-width:0}}", TKA)

    def test_estilos_de_los_chips(self):
        for pista in (".tka-modal .ln-bod{", ".tka-modal .ln-bod-chip{", ".tka-bod-resumen{", ".tka-bod-resumen.is-varias{"):
            self.assertIn(pista, TKA)


@unittest.skipIf(not NODE, "Node no está instalado")
class TestFuncionEjecutada(unittest.TestCase):
    """_tkaCargarBodegas corre de verdad (DOM mínimo de mentira, fetch de mentira)."""

    @classmethod
    def setUpClass(cls):
        i = TKA.index("const _TKA_BOD = {")
        j = TKA.index("// 2026-10-23 (Daniel, OT-2026-00348:")
        cuerpo = TKA[i:j]
        k = TKA.index("function _tkaEsc(s){")
        esc = TKA[k:TKA.index("}", TKA.index("replace(", k) + 600) + 1]
        guion = r"""
%s
%s
function caja(sku){ return { dataset:{ bodSku: sku }, innerHTML:'' }; }
function raiz(cajas){ const r = { cajas, resumen:null, firstChild:{ id:'primero' },
  querySelectorAll: function(){ return r.cajas; }, querySelector: function(){ return r.resumen; },
  insertBefore: function(el){ r.resumen = el; el.remove = function(){ r.resumen = null; }; } }; return r; }
global.document = { createElement: function(){ return { className:'', innerHTML:'' }; } };
const llamadas = []; let modo = 'ok'; const pend = [];
const BODS = {
  A:[{bodega:'BODEGA 02', fisico:3, disponible:3, ubicaciones:['PISOD-12'], mas_ubicaciones:0}, {bodega:'BODEGA EXTERNA LAMPA', fisico:2, disponible:2, ubicaciones:['RACK-3','RACK-4'], mas_ubicaciones:2}],
  B:[{bodega:'BODEGA 02', fisico:1, disponible:0, ubicaciones:[], mas_ubicaciones:0}],
  RA:[{bodega:'BODEGA DEL PEDIDO VIEJO', fisico:1, disponible:1, ubicaciones:[], mas_ubicaciones:0}],
  RB:[{bodega:'BODEGA DEL PEDIDO NUEVO', fisico:1, disponible:1, ubicaciones:[], mas_ubicaciones:0}],
  X:[{bodega:'<img src=x onerror=alert(1)>', fisico:1, disponible:1, ubicaciones:[], mas_ubicaciones:0}] };
const respuesta = function(skus){ const b = {}; skus.forEach(s => { b[s] = BODS[s] || []; }); return { ok:true, bodegas:b, edad_min:7 }; };
global.setTimeout = function(fn){ fn(); return 0; };          // las esperas entre reintentos no se esperan de verdad
let calentando = 0;
global.fetch = async function(u){
  llamadas.push(decodeURIComponent(u.split('skus=')[1]).split(',').length);
  const skus = decodeURIComponent(u.split('skus=')[1]).split(',');
  if (modo === 'caido') return { status: 503, json: async () => ({ ok:false, error:'Check no respondió' }) };
  if (modo === 'prohibido') return { status: 403, json: async () => ({ ok:false, error:'No tienes acceso a esta información.' }) };
  if (modo === 'calentando' && calentando-- > 0) return { status: 503, json: async () => ({ ok:false, calentando:true, error:'Conectando con Check…' }) };
  if (modo === 'manual') return new Promise(function(res){ pend.push(function(){ res({ status: 200, json: async () => respuesta(skus) }); }); });
  return { status: 200, json: async () => respuesta(skus) };
};
(async function(){
  const out = {};
  // 1) Normal: A en 2 bodegas, B en 1, C no está en Check
  let r = raiz([caja('A'), caja('B'), caja('C')]);
  await _tkaCargarBodegas(r);
  out.normal = { llamadas: llamadas.slice(), a: r.cajas[0].innerHTML, b: r.cajas[1].innerHTML, c: r.cajas[2].innerHTML, resumen: r.resumen && r.resumen.innerHTML, claseResumen: r.resumen && r.resumen.className };
  // 2) Mismo pedido otra vez: sale de la memoria, no vuelve a preguntar
  const antes = llamadas.length; r = raiz([caja('A'), caja('B')]); await _tkaCargarBodegas(r);
  out.memoria = { nuevasLlamadas: llamadas.length - antes, a: r.cajas[0].innerHTML.indexOf('En 2 bodegas') >= 0 };
  // 3) Sin resumen (listas de productos sueltos)
  r = raiz([caja('A')]); await _tkaCargarBodegas(r, { resumen:false }); out.sinResumen = { resumen: r.resumen };
  // 4) Check caído: cada caja lo dice y no hay resumen
  modo = 'caido'; r = raiz([caja('N1'), caja('N2')]); await _tkaCargarBodegas(r);
  out.caido = { c: r.cajas[0].innerHTML, resumen: r.resumen };
  // 4b) Tras un fallo no se golpea a Check en cada búsqueda: durante 30 s se contesta «sin verificar» sin preguntar
  const a4 = llamadas.length; modo = 'ok'; r = raiz([caja('N3')]); await _tkaCargarBodegas(r);
  out.tregua = { nuevasLlamadas: llamadas.length - a4, c: r.cajas[0].innerHTML };
  _TKA_BOD.falloHasta = 0;                                    // (el reloj de la prueba: pasaron los 30 s)
  // 5) Más de 80 productos: se piden de a 80
  const muchas = []; for (let i = 0; i < 100; i++) muchas.push(caja('M' + i)); const a2 = llamadas.length; r = raiz(muchas); await _tkaCargarBodegas(r);
  out.lotes = llamadas.slice(a2);
  // 6) Lo que viene de Check se escapa (nunca se interpreta como HTML)
  r = raiz([caja('X')]); await _tkaCargarBodegas(r); out.escape = r.cajas[0].innerHTML;
  // 7) Cajas sin SKU: se limpian y no se pregunta nada
  const a3 = llamadas.length; r = raiz([caja('')]); await _tkaCargarBodegas(r); out.sinSku = { nuevasLlamadas: llamadas.length - a3 };
  // 8) Caché fría del servidor: responde «calentando» dos veces y a la tercera hay datos → el modal reintenta solo
  _TKA_BOD.cache.clear(); modo = 'calentando'; calentando = 2; const a5 = llamadas.length;
  r = raiz([caja('A')]); await _tkaCargarBodegas(r);
  out.calentando = { llamadas: llamadas.length - a5, a: r.cajas[0].innerHTML.indexOf('En 2 bodegas') >= 0 };
  modo = 'ok';
  // 9) Una búsqueda NUEVA sobre el mismo contenedor deja obsoleta a la anterior: la respuesta lenta de A no puede pintar encima de B
  _TKA_BOD.cache.clear(); modo = 'manual'; pend.length = 0;
  const rr = raiz([caja('RA')]);
  const pA = _tkaCargarBodegas(rr);                           // A pregunta por RA y queda esperando
  await Promise.resolve();
  const cajasA = rr.cajas; rr.cajas = [caja('RB')];           // el contenedor se vuelve a dibujar con OTRO pedido
  const pB = _tkaCargarBodegas(rr);                           // B pregunta por RB
  await Promise.resolve();
  pend[1]();                                                  // B responde primero
  await pB;
  const resumenB = rr.resumen && rr.resumen.innerHTML;
  pend[0]();                                                  // y recién después responde A (la lenta)
  await pA;
  out.carrera = { resumenB: resumenB, resumenFinal: rr.resumen && rr.resumen.innerHTML, cajaB: rr.cajas[0].innerHTML, cajaA: cajasA[0].innerHTML };
  modo = 'ok';
  // 10) Sin acceso (403: técnicos): el cuadro NO existe (sin falso «sin verificar»), y no se vuelve a preguntar
  _TKA_BOD.cache.clear(); modo = 'prohibido'; const a6 = llamadas.length;
  r = raiz([caja('A'), caja('B')]); await _tkaCargarBodegas(r);
  const trasPrimera = llamadas.length - a6;
  r = raiz([caja('M1')]); await _tkaCargarBodegas(r);
  out.prohibido = { llamadasPrimera: trasPrimera, llamadasTotales: llamadas.length - a6, cajas: [r.cajas[0].innerHTML], resumen: r.resumen, bandera: _TKA_BOD.sinAcceso };
  console.log(JSON.stringify(out));
})();
""" % (esc, cuerpo)
        with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as f:
            f.write(guion)
            ruta = f.name
        try:
            r = subprocess.run([NODE, ruta], capture_output=True, text=True, encoding="utf-8", timeout=60)
        finally:
            os.unlink(ruta)
        assert r.returncode == 0, r.stderr[-1500:]
        cls.o = json.loads(r.stdout.strip().splitlines()[-1])

    def test_un_producto_en_varias_bodegas_las_muestra_todas_con_el_total(self):
        a = self.o["normal"]["a"]
        self.assertIn("En 2 bodegas · 5 u. en total", a)
        self.assertIn("BODEGA 02</b> · 3 u.", a)
        self.assertIn("BODEGA EXTERNA LAMPA</b> · 2 u.", a)
        self.assertIn("RACK-3, RACK-4 +2", a)                        # ubicaciones, y cuántas más hay

    def test_un_producto_en_una_bodega_y_otro_que_check_no_tiene(self):
        n = self.o["normal"]
        self.assertIn("BODEGA 02</b> · 1 u.", n["b"])
        self.assertNotIn("En 1 bodegas", n["b"])
        self.assertIn("Sin stock en Check", n["c"])

    def test_el_resumen_del_pedido(self):
        n = self.o["normal"]
        self.assertEqual(n["llamadas"], [3])                        # una sola consulta por los 3 productos
        self.assertEqual(n["claseResumen"], "tka-bod-resumen is-varias")
        self.assertIn("¿Dónde está este pedido según Check?", n["resumen"])
        self.assertIn("Está repartido en 2 bodegas", n["resumen"])
        self.assertIn("BODEGA 02</b> · 2 productos · 4 u.", n["resumen"])
        self.assertIn("BODEGA EXTERNA LAMPA</b> · 1 producto · 2 u.", n["resumen"])
        self.assertIn("1 producto sin stock en Check", n["resumen"])
        self.assertIn("Check · hace 7 min", n["resumen"])

    def test_el_mismo_pedido_no_se_vuelve_a_consultar(self):
        self.assertEqual(self.o["memoria"], {"nuevasLlamadas": 0, "a": True})

    def test_sin_resumen_en_las_listas_de_productos_sueltos(self):
        self.assertIsNone(self.o["sinResumen"]["resumen"])

    def test_check_caido_no_inventa_nada(self):
        c = self.o["caido"]
        self.assertIn("Check no respondió: bodega sin verificar", c["c"])
        self.assertIsNone(c["resumen"])

    def test_mas_de_ochenta_productos_se_piden_de_a_ochenta(self):
        self.assertEqual(self.o["lotes"], [80, 20])

    def test_lo_que_viene_de_check_se_escapa(self):
        self.assertNotIn("<img", self.o["escape"])
        self.assertIn("&lt;img", self.o["escape"])

    def test_sin_sku_no_pregunta(self):
        self.assertEqual(self.o["sinSku"], {"nuevasLlamadas": 0})

    def test_tras_un_fallo_no_se_golpea_a_check_en_cada_busqueda(self):
        t = self.o["tregua"]
        self.assertEqual(t["nuevasLlamadas"], 0)
        self.assertIn("Check no respondió: bodega sin verificar", t["c"])

    def test_la_primera_consulta_de_una_instancia_fria_se_reintenta_sola(self):
        c = self.o["calentando"]
        self.assertEqual(c["llamadas"], 3)           # «calentando», «calentando» y por fin los datos
        self.assertTrue(c["a"])

    def test_una_respuesta_lenta_no_pisa_a_la_de_la_busqueda_nueva(self):
        # Revisión independiente (24-oct): si la respuesta de A (lenta) llegaba después que la de B, borraba el resumen correcto de B e
        # insertaba el de A sobre el documento nuevo («¿Dónde está este pedido?» con productos de otro documento).
        c = self.o["carrera"]
        self.assertIn("BODEGA DEL PEDIDO NUEVO", c["resumenB"])
        self.assertEqual(c["resumenFinal"], c["resumenB"])
        self.assertNotIn("PEDIDO VIEJO", c["resumenFinal"])
        self.assertIn("BODEGA DEL PEDIDO NUEVO", c["cajaB"])
        self.assertNotIn("PEDIDO VIEJO", c["cajaA"])

    def test_sin_acceso_el_cuadro_no_existe_y_no_se_vuelve_a_preguntar(self):
        # Un técnico recibe 403: antes cada línea decía «Check no respondió: bodega sin verificar» (engañoso) y se preguntaba en cada búsqueda.
        p = self.o["prohibido"]
        self.assertEqual(p["llamadasPrimera"], 1)
        self.assertEqual(p["llamadasTotales"], 1)
        self.assertEqual(p["cajas"], [""])
        self.assertIsNone(p["resumen"])
        self.assertTrue(p["bandera"])


if __name__ == "__main__":
    unittest.main()
