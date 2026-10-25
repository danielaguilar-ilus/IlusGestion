# -*- coding: utf-8 -*-
"""2026-10-24 · Revisión independiente de «Editar cliente» (static/cliente_form.js): la puerta «la ubicación se debe verificar para poder
avanzar» (Daniel) tenía tres grietas, todas reproducidas aquí con el JS corriendo de verdad (Node + DOM mínimo de mentira):

1. Se saltaba cerrando y reabriendo el modal: `show.bs.modal` recalculaba «lo verificado» desde el texto que hubiera en el campo, así que una
   dirección CAMBIADA (con el place_id viejo todavía en el campo oculto) pasaba a contar como verificada y se guardaba con las coordenadas de
   otro lugar. Ahora lo verificado vive en memoria y el place_id/coordenadas ocultos SIEMPRE calzan con el texto.
2. Callejón sin salida: un cliente anterior a la regla (dirección sin place_id) o sin dirección no se podía editar en NADA (ni estado, ni
   ejecutivo, ni tipo). Ahora, si la dirección no se tocó y nunca estuvo verificada, se guarda con un aviso ámbar («alertar, no trabar»).
3. Accesibilidad: las regiones `aria-live` se reescribían en cada evaluación (cada tecla y cada 500 ms mientras carga Google).
"""
import json
import os
import shutil
import subprocess
import tempfile
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NODE = shutil.which("node")

GUION = r"""
const fs = require('fs'), vm = require('vm');
const cfg = JSON.parse(process.argv[3]);
const els = {}, escr = {};
function El(id) {
  this.id = id; this.value = ''; this.className = ''; this.title = ''; this.dataset = {}; this.style = {}; this.hidden = false; this.disabled = false;
  this._c = new Set(); this._h = {};
  const self = this; let ih = '', tc = '';
  const cuenta = function (k) { (escr[id] = escr[id] || { innerHTML: 0, textContent: 0 })[k]++; };
  Object.defineProperty(this, 'innerHTML', { get: function () { return ih; }, set: function (v) { ih = v; cuenta('innerHTML'); } });
  Object.defineProperty(this, 'textContent', { get: function () { return tc; }, set: function (v) { tc = v; cuenta('textContent'); } });
  this.classList = {
    add: function () { for (const c of arguments) self._c.add(c); }, remove: function () { for (const c of arguments) self._c.delete(c); },
    contains: function (c) { return self._c.has(c); },
    toggle: function (c, f) { if (f === undefined) f = !self._c.has(c); if (f) self._c.add(c); else self._c.delete(c); return f; },
  };
  this.addEventListener = function (t, fn) { (self._h[t] = self._h[t] || []).push(fn); };
}
const msgs = {};
global.window = global;
global.document = {
  readyState: 'complete',
  getElementById: function (id) { return els[id] || (els[id] = new El(id)); },
  querySelector: function (sel) {
    const m = /data-ecf-msg="([^"]+)"/.exec(sel); if (m) return msgs[m[1]] || (msgs[m[1]] = new El('msg_' + m[1]));
    return els[sel] || (els[sel] = new El(sel));
  },
  addEventListener: function () {},
};
// Lo que el servidor dejó escrito en el HTML ANTES de que la página cargara (y de que alguien editara).
for (const k of Object.keys(cfg.inicial)) document.getElementById(k).value = cfg.inicial[k];
let ocultado = 0, hayAviso = false, hayLista = false;
global.bootstrap = { Modal: { getInstance: function () { return { hide: function () { ocultado++; } }; } } };
vm.runInThisContext(fs.readFileSync(process.argv[2], 'utf8'));
const g = function (id) { return document.getElementById(id); };
// el modal y el documento de mentira para el Esc: ¿hay un aviso (ilusConfirm…) encima? ¿está abierta la lista de sugerencias de Google?
g('modalEditarCliente').querySelector = function (sel) { return (sel === '.ilus-overlay' && hayAviso) ? {} : null; };
const qsPrevio = document.querySelector;
document.querySelector = function (sel) { return sel === '.pac-container' ? (hayLista ? { offsetParent: {}, children: [1] } : null) : qsPrevio(sel); };
const snap = function () {
  const p = g('ecfFootEstado'), b = g('btnGuardarCliente');
  return {
    pend: window.ecfDireccionPendiente(),
    caja: g('ecfVerif').className,
    cajaTxt: g('ecfVerif').innerHTML.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim(),
    cajaHtml: g('ecfVerif').innerHTML,
    pie: { clase: p.className, texto: p.innerHTML.replace(/<[^>]+>/g, ''), apagado: b.disabled },
    dirCls: ['ecf-ok', 'ecf-bad', 'ecf-warn'].filter(function (c) { return g('ec_direccion').classList.contains(c); })[0] || '',
    oculto: { pid: g('ec_direccion_place_id').value, lat: g('ec_direccion_lat').value, lng: g('ec_direccion_lng').value },
    geo: { region: g('ec_region').value, comuna: g('ec_comuna').value, ciudad: g('ec_ciudad').value },
    dir: g('ec_direccion').value,
    cerrado: ocultado,
  };
};
const out = [];
for (const paso of cfg.pasos) {
  if (paso.set) for (const k of Object.keys(paso.set)) g(k).value = paso.set[k];
  if (paso.google !== undefined) g('ec_direccion').dataset.placesInit = paso.google ? '1' : '';
  if (paso.verificar) window.ecfDireccionVerificada(paso.verificar, paso.geo);
  if (paso.authFallo) window.gm_authFailure();
  if (paso.restaurar) window.ecfRestaurarDireccion();
  if (paso.abrir) (g('modalEditarCliente')._h['show.bs.modal'] || []).forEach(function (fn) { fn({}); });
  if (paso.esc) { hayAviso = !!paso.aviso; hayLista = !!paso.lista; (g('modalEditarCliente')._h.keydown || []).forEach(function (fn) { fn({ key: 'Escape', target: g('ec_direccion'), defaultPrevented: false, preventDefault: function () {} }); }); }
  out.push(snap());
}
// Cuántas veces se reescribieron las regiones aria-live (al terminar todos los pasos).
console.log(JSON.stringify({ pasos: out, escrituras: { verif: escr.ecfVerif, pie: escr.ecfFootEstado, prog: escr.ecfProgTxt } }));
"""


def _correr(inicial, pasos):
    cfg = json.dumps({"inicial": inicial, "pasos": pasos})
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as f:
        f.write(GUION)
        ruta = f.name
    try:
        r = subprocess.run([NODE, ruta, os.path.join(RAIZ, "static", "cliente_form.js"), cfg], capture_output=True, text=True,
                           encoding="utf-8", timeout=60)
    finally:
        os.unlink(ruta)
    assert r.returncode == 0, r.stderr[-1500:]
    return json.loads(r.stdout.strip().splitlines()[-1])


def _ini(direccion="", pid="", lat="", lng="", razon="Gimnasio Aurum SpA", region="", comuna="", ciudad=""):
    return {"ec_razon": razon, "ec_direccion": direccion, "ec_direccion_place_id": pid, "ec_direccion_lat": lat, "ec_direccion_lng": lng,
            "ec_region": region, "ec_comuna": comuna, "ec_ciudad": ciudad}


@unittest.skipIf(not NODE, "Node no está instalado")
class TestCerrarYReabrirNoVerificaUnTextoCambiado(unittest.TestCase):
    """Grieta 1 de la revisión: el escenario exacto reportado, paso a paso."""

    @classmethod
    def setUpClass(cls):
        cls.r = _correr(_ini(), [
            {"google": True},                                                                                   # 0 cliente sin dirección, Google listo
            {"set": {"ec_direccion": "Av. Apoquindo 4501, Las Condes", "ec_direccion_place_id": "X",
                     "ec_direccion_lat": "-33.40", "ec_direccion_lng": "-70.57"}, "verificar": "Av. Apoquindo 4501, Las Condes"},   # 1 elige la sugerencia
            {"set": {"ec_direccion": "Parcela 12, Km 40"}},                                                     # 2 la cambia por otra cosa
            {"abrir": True},                                                                                    # 3 Cancelar y reabrir el modal
            {"set": {"ec_direccion": "Av. Apoquindo 4501, Las Condes, oficina 3"}},                             # 4 vuelve a lo verificado + detalle
        ])["pasos"]

    def test_una_direccion_cambiada_apaga_guardar(self):
        p = self.r[2]
        self.assertEqual(p["pend"], {"ok": False, "motivo": "cambiada"})
        self.assertTrue(p["pie"]["apagado"])
        self.assertEqual(p["dirCls"], "ecf-bad")

    def test_cerrar_y_reabrir_no_la_vuelve_verificada(self):
        p = self.r[3]
        self.assertEqual(p["pend"], {"ok": False, "motivo": "cambiada"})       # antes: ok:true (recalculaba «verificada» desde el campo)
        self.assertTrue(p["pie"]["apagado"])
        self.assertIn("bad", p["caja"])

    def test_el_place_id_y_las_coordenadas_no_se_guardan_con_un_texto_que_no_les_corresponde(self):
        self.assertEqual(self.r[2]["oculto"], {"pid": "", "lat": "", "lng": ""})
        self.assertEqual(self.r[3]["oculto"], {"pid": "", "lat": "", "lng": ""})

    def test_volver_a_lo_verificado_recupera_la_verificacion_y_sus_coordenadas(self):
        p = self.r[4]
        self.assertEqual(p["pend"], {"ok": True})
        self.assertFalse(p["pie"]["apagado"])
        self.assertEqual(p["oculto"], {"pid": "X", "lat": "-33.40", "lng": "-70.57"})
        self.assertIn("Detalle agregado: oficina 3", p["cajaTxt"])

    def test_elegir_la_sugerencia_la_verifica_con_sus_coordenadas(self):
        p = self.r[1]
        self.assertEqual(p["pend"], {"ok": True})
        self.assertEqual(p["oculto"], {"pid": "X", "lat": "-33.40", "lng": "-70.57"})


@unittest.skipIf(not NODE, "Node no está instalado")
class TestClienteAnteriorSinVerificarNoQuedaAtrapado(unittest.TestCase):
    """Grieta 2: sin callejón sin salida («alertar, no trabar»). Se exige verificar lo que se CAMBIA, no el pasado."""

    @classmethod
    def setUpClass(cls):
        cls.legado = _correr(_ini("Av. Vieja 100, Ñuñoa"), [
            {"google": True},                                          # 0 abre el modal solo para cambiar el ejecutivo
            {"set": {"ec_direccion": "Av. Nueva 200"}},                # 1 toca la dirección sin elegir sugerencia
            {"set": {"ec_direccion": "Av. Vieja 100, Ñuñoa"}},         # 2 la deja como estaba
            {"google": False, "set": {"ec_direccion": "Av. Nueva 200"}},    # 3 Google no responde
        ])["pasos"]
        cls.sin_dir = _correr(_ini(""), [{"google": True}, {"google": False}])["pasos"]

    def test_no_se_toca_la_direccion_se_puede_guardar_con_aviso_ambar(self):
        p = self.legado[0]
        self.assertEqual(p["pend"], {"ok": True, "aviso": "heredada"})
        self.assertFalse(p["pie"]["apagado"])
        self.assertIn("warn", p["pie"]["clase"])
        self.assertIn("nunca se verificó", p["pie"]["texto"])
        self.assertEqual(p["dirCls"], "ecf-warn")
        self.assertIn("warn", p["caja"])
        self.assertIn("Puedes guardar sin tocarla", p["cajaTxt"])

    def test_si_la_cambia_sin_verificar_si_traba(self):
        p = self.legado[1]
        self.assertEqual(p["pend"], {"ok": False, "motivo": "sin_verificar"})
        self.assertTrue(p["pie"]["apagado"])
        self.assertEqual(p["dirCls"], "ecf-bad")

    def test_volver_al_texto_original_destraba(self):
        self.assertEqual(self.legado[2]["pend"], {"ok": True, "aviso": "heredada"})
        self.assertFalse(self.legado[2]["pie"]["apagado"])

    def test_sin_google_sigue_siendo_avisar_y_dejar_decidir(self):
        p = self.legado[3]
        self.assertEqual(p["pend"], {"ok": False, "motivo": "google_no_disponible"})
        self.assertFalse(p["pie"]["apagado"])

    def test_un_cliente_sin_direccion_tampoco_queda_atrapado(self):
        for p in self.sin_dir:       # con y sin Google
            self.assertEqual(p["pend"], {"ok": True, "aviso": "heredada"})
            self.assertFalse(p["pie"]["apagado"])
        self.assertIn("no tiene dirección", self.sin_dir[0]["pie"]["texto"])

    def test_los_nombres_cortos_reales_no_traban_la_razon_social(self):
        r = _correr(_ini("Av. Vieja 100", razon="3M"), [{"google": True}])["pasos"][0]
        self.assertFalse(r["pie"]["apagado"])
        v = _correr(_ini("Av. Vieja 100", razon="A"), [{"google": True}])["pasos"][0]
        self.assertTrue(v["pie"]["apagado"])
        self.assertIn("la razón social", v["pie"]["texto"])


@unittest.skipIf(not NODE, "Node no está instalado")
class TestVerificadaDeSiempre(unittest.TestCase):
    """Un cliente con dirección ya verificada: detalle al final sí, cambiar o borrar no; y las coordenadas siguen al texto."""

    @classmethod
    def setUpClass(cls):
        cls.r = _correr(_ini("Av. Nueva 1, Santiago", "P1", "-33.40", "-70.60"), [
            {"google": True},                                                                  # 0 recién abierto
            {"set": {"ec_direccion": "Av. Nueva 1, Santiago, oficina 5"}},                     # 1 detalle al final
            {"set": {"ec_direccion": "Otra calle 5"}},                                         # 2 otra dirección sin sugerencia
            {"set": {"ec_direccion": ""}},                                                     # 3 la borra
            {"set": {"ec_direccion": "Av. Nueva 1, Santiago"}},                                # 4 vuelve a la original
            {"set": {"ec_direccion": "Calle Dos 2, Providencia", "ec_direccion_place_id": "P2",
                     "ec_direccion_lat": "-33.50", "ec_direccion_lng": "-70.70"}, "verificar": "Calle Dos 2, Providencia"},   # 5 elige otra sugerencia
            {"set": {"ec_direccion": "Av. Nueva 1, Santiago"}},                                # 6 y se arrepiente: vuelve a la primera
            {"set": {"ec_direccion": "Av. Nueva 1, Santiago4"}},                               # 7 pega un dígito al número
            {"set": {"ec_direccion": "Av. Nueva 1, Santiago 4"}},                              # 8 detalle separado
        ])["pasos"]

    def test_recien_abierto_esta_verificada(self):
        self.assertEqual(self.r[0]["pend"], {"ok": True})
        self.assertEqual(self.r[0]["oculto"], {"pid": "P1", "lat": "-33.40", "lng": "-70.60"})

    def test_detalle_al_final_conserva_la_verificacion(self):
        self.assertEqual(self.r[1]["pend"], {"ok": True})
        self.assertEqual(self.r[1]["oculto"]["pid"], "P1")

    def test_cambiarla_la_traba_y_vacia_las_coordenadas(self):
        self.assertEqual(self.r[2]["pend"], {"ok": False, "motivo": "cambiada"})
        self.assertEqual(self.r[2]["oculto"], {"pid": "", "lat": "", "lng": ""})

    def test_borrar_una_direccion_verificada_si_traba(self):
        self.assertEqual(self.r[3]["pend"], {"ok": False, "motivo": "vacia"})
        self.assertTrue(self.r[3]["pie"]["apagado"])

    def test_volver_a_la_original_restituye_todo(self):
        self.assertEqual(self.r[4]["pend"], {"ok": True})
        self.assertEqual(self.r[4]["oculto"], {"pid": "P1", "lat": "-33.40", "lng": "-70.60"})

    def test_probar_otra_sugerencia_y_volver_a_la_primera_sigue_verificada(self):
        self.assertEqual(self.r[5]["oculto"]["pid"], "P2")
        self.assertEqual(self.r[6]["pend"], {"ok": True})
        self.assertEqual(self.r[6]["oculto"], {"pid": "P1", "lat": "-33.40", "lng": "-70.60"})     # las coordenadas siguen al texto

    def test_el_detalle_va_separado_pegar_un_digito_cambia_el_numero(self):
        self.assertEqual(self.r[7]["pend"], {"ok": False, "motivo": "cambiada"})     # «…Santiago4» ya no es la dirección verificada
        self.assertEqual(self.r[8]["pend"], {"ok": True})


@unittest.skipIf(not NODE, "Node no está instalado")
class TestCoordenadasSinPlaceIdNoSePierden(unittest.TestCase):
    """Un cliente con lat/lng pero sin place_id (geocodificado por otro medio): abrir y guardar sin tocar la dirección no borra sus coordenadas."""

    def test_se_conservan_mientras_el_texto_no_cambia_y_se_vacian_si_cambia(self):
        pasos = _correr(_ini("Calle Z 5", "", "-33.10", "-70.10"), [
            {"google": True},
            {"set": {"ec_direccion": "Calle Z 6"}},
            {"set": {"ec_direccion": "Calle Z 5"}},
        ])["pasos"]
        self.assertEqual(pasos[0]["oculto"], {"pid": "", "lat": "-33.10", "lng": "-70.10"})
        self.assertEqual(pasos[0]["pend"], {"ok": True, "aviso": "heredada"})
        self.assertEqual(pasos[1]["oculto"], {"pid": "", "lat": "", "lng": ""})
        self.assertEqual(pasos[2]["oculto"], {"pid": "", "lat": "-33.10", "lng": "-70.10"})


@unittest.skipIf(not NODE, "Node no está instalado")
class TestRegionesAriaLiveSoloSiCambian(unittest.TestCase):
    """Grieta 3: un lector de pantalla relee una región `aria-live` cada vez que se reescribe su contenido."""

    def test_evaluar_sin_cambios_no_reescribe_las_regiones(self):
        pasos = [{"google": True}] + [{"abrir": True}] * 6 + [{"set": {"ec_razon": "Gimnasio Aurum SpA"}}] * 3
        r = _correr(_ini("Av. Nueva 1, Santiago", "P1", "-33.4", "-70.6"), pasos)
        # `snap()` evalúa en cada paso: son 10 evaluaciones sin que cambie nada visible.
        self.assertEqual(r["escrituras"]["verif"]["innerHTML"], 1)
        self.assertEqual(r["escrituras"]["pie"]["innerHTML"], 1)
        self.assertEqual(r["escrituras"]["prog"]["textContent"], 1)

    def test_si_cambia_el_estado_si_se_reescribe(self):
        r = _correr(_ini("Av. Nueva 1, Santiago", "P1", "-33.4", "-70.6"), [{"google": True}, {"set": {"ec_direccion": "Otra"}}])
        self.assertEqual(r["escrituras"]["verif"]["innerHTML"], 2)
        self.assertEqual(r["escrituras"]["pie"]["innerHTML"], 2)


LC = dict(direccion="Av. Apoquindo 4501, Las Condes, Chile", pid="P_LC", lat="-33.41", lng="-70.58", region="Metropolitana", comuna="Las Condes", ciudad="Santiago")
PROVIDENCIA = {"ec_direccion": "Calle Dos 2, Providencia, Chile", "ec_direccion_place_id": "P_PV", "ec_direccion_lat": "-33.43", "ec_direccion_lng": "-70.61",
               "ec_region": "Metropolitana", "ec_comuna": "Providencia", "ec_ciudad": "Providencia"}
GEO_PV = {"region": "Metropolitana", "comuna": "Providencia", "ciudad": "Providencia"}
GEO_LC = {"region": "Metropolitana", "comuna": "Las Condes", "ciudad": "Santiago"}


@unittest.skipIf(not NODE, "Node no está instalado")
class TestRegionComunaYCiudadSiguenALaDireccion(unittest.TestCase):
    """2.ª revisión independiente (25-oct): probar otra sugerencia y volver a la original dejaba el modal diciendo «verificada» con el place_id y
    las coordenadas de Las Condes, pero comuna y ciudad de Providencia (y eso es lo que se guardaba)."""

    def test_volver_a_la_direccion_original_repone_region_comuna_y_ciudad(self):
        r = _correr(_ini(**LC), [
            {"google": True},
            {"set": PROVIDENCIA, "verificar": PROVIDENCIA["ec_direccion"], "geo": GEO_PV},        # elige una sugerencia de Providencia
            {"set": {"ec_direccion": LC["direccion"]}},                                           # se arrepiente
        ])["pasos"]
        self.assertEqual(r[1]["geo"], GEO_PV)
        self.assertEqual(r[1]["oculto"]["pid"], "P_PV")
        self.assertEqual(r[2]["pend"], {"ok": True})
        self.assertEqual(r[2]["oculto"], {"pid": "P_LC", "lat": "-33.41", "lng": "-70.58"})
        self.assertEqual(r[2]["geo"], GEO_LC)                                                     # antes: seguían siendo las de Providencia

    def test_lo_que_la_persona_escribio_a_mano_no_se_pisa(self):
        r = _correr(_ini(**LC), [
            {"google": True},
            {"set": PROVIDENCIA, "verificar": PROVIDENCIA["ec_direccion"], "geo": GEO_PV},
            {"set": {"ec_comuna": "Providencia Norte"}},                                          # corrige la comuna a mano
            {"set": {"ec_direccion": LC["direccion"]}},
        ])["pasos"]
        self.assertEqual(r[3]["geo"], {"region": "Metropolitana", "comuna": "Providencia Norte", "ciudad": "Santiago"})   # la ciudad sin tocar sí vuelve

    def test_lo_que_google_no_trajo_no_se_queda_con_el_valor_de_otra_direccion(self):
        r = _correr(_ini(**LC), [
            {"google": True},
            # sugerencia rural sin comuna ni ciudad: mant_ficha.js solo escribe lo que Google trae, así que los campos quedan con lo de Las Condes
            {"set": {"ec_direccion": "Sector Rural s/n, Chile", "ec_direccion_place_id": "P_R", "ec_direccion_lat": "-38.7", "ec_direccion_lng": "-72.6",
                     "ec_region": "La Araucanía"}, "verificar": "Sector Rural s/n, Chile", "geo": {"region": "La Araucanía", "comuna": "", "ciudad": ""}},
        ])["pasos"]
        self.assertEqual(r[1]["geo"], {"region": "La Araucanía", "comuna": "", "ciudad": ""})

    def test_si_la_persona_ya_habia_escrito_la_comuna_se_respeta_aunque_google_no_la_traiga(self):
        r = _correr(_ini(**LC), [
            {"google": True},
            {"set": {"ec_comuna": "Mi comuna"}},                                                  # a mano, distinta de lo guardado
            {"set": {"ec_direccion": "Sector Rural s/n, Chile", "ec_direccion_place_id": "P_R", "ec_direccion_lat": "-38.7", "ec_direccion_lng": "-72.6",
                     "ec_region": "La Araucanía"}, "verificar": "Sector Rural s/n, Chile", "geo": {"region": "La Araucanía", "comuna": "", "ciudad": ""}},
        ])["pasos"]
        self.assertEqual(r[2]["geo"]["comuna"], "Mi comuna")
        self.assertEqual(r[2]["geo"]["ciudad"], "")                                               # esa sí era la de la dirección anterior

    def test_un_cliente_anterior_sin_verificar_recupera_sus_datos_al_volver_a_su_texto(self):
        r = _correr(_ini("Calle Vieja 100", region="Valparaíso", comuna="Viña del Mar", ciudad="Viña del Mar"), [
            {"google": True},
            {"set": PROVIDENCIA, "verificar": PROVIDENCIA["ec_direccion"], "geo": GEO_PV},
            {"set": {"ec_direccion": "Calle Vieja 100"}},
        ])["pasos"]
        self.assertEqual(r[2]["geo"], {"region": "Valparaíso", "comuna": "Viña del Mar", "ciudad": "Viña del Mar"})
        self.assertEqual(r[2]["pend"], {"ok": True, "aviso": "heredada"})
        self.assertEqual(r[2]["oculto"], {"pid": "", "lat": "", "lng": ""})


@unittest.skipIf(not NODE, "Node no está instalado")
class TestDetalleYaGuardadoSePuedeEditar(unittest.TestCase):
    """2.ª revisión: «…, Chile, oficina 5» ya guardado trataba TODO como base verificada: cambiar a «oficina 6» o quitar el detalle dejaba Guardar apagado."""

    @classmethod
    def setUpClass(cls):
        base = "Av. Apoquindo 4501, Las Condes, Chile"
        cls.r = _correr(_ini(base + ", oficina 5", "P1", "-33.4", "-70.6"), [
            {"google": True},                                          # 0 recién abierto
            {"set": {"ec_direccion": base + ", oficina 6"}},           # 1 cambia el detalle
            {"set": {"ec_direccion": base}},                           # 2 lo quita
            {"set": {"ec_direccion": base + ", piso 3, oficina 5"}},   # 3 detalle nuevo
            {"set": {"ec_direccion": "Av. Apoquindo 4502, Las Condes, Chile, oficina 5"}},   # 4 cambia la calle de verdad
        ])["pasos"]

    def test_se_puede_editar_o_quitar_el_detalle(self):
        for i in (0, 1, 2, 3):
            self.assertEqual(self.r[i]["pend"], {"ok": True}, i)
            self.assertFalse(self.r[i]["pie"]["apagado"], i)
            self.assertEqual(self.r[i]["oculto"]["pid"], "P1", i)
        self.assertIn("Detalle agregado: oficina 6", self.r[1]["cajaTxt"])

    def test_cambiar_la_calle_si_la_traba(self):
        self.assertEqual(self.r[4]["pend"], {"ok": False, "motivo": "cambiada"})

    def test_sin_chile_en_el_texto_la_base_es_todo_lo_guardado_como_antes(self):
        r = _correr(_ini("Av. Vieja 100, Santiago", "P1", "-33.4", "-70.6"), [
            {"google": True}, {"set": {"ec_direccion": "Av. Vieja 100, Santiago, oficina 2"}}, {"set": {"ec_direccion": "Av. Vieja 100"}}])["pasos"]
        self.assertEqual(r[1]["pend"], {"ok": True})                    # detalle al final: sigue verificada
        self.assertEqual(r[2]["pend"], {"ok": False, "motivo": "cambiada"})   # quitar parte de lo guardado: no hay base conocida


@unittest.skipIf(not NODE, "Node no está instalado")
class TestVolverALaDireccionGuardada(unittest.TestCase):
    def test_un_retoque_accidental_tiene_salida_directa(self):
        r = _correr(_ini(**LC), [
            {"google": True},
            {"set": {"ec_direccion": "Parcela 12, Km 40"}},            # retoque
            {"restaurar": True},
        ])["pasos"]
        self.assertEqual(r[1]["pend"], {"ok": False, "motivo": "cambiada"})
        self.assertIn("ecfRestaurarDireccion()", r[1]["cajaHtml"])                               # el botón está en el aviso
        self.assertIn("Volver a la dirección guardada", r[1]["cajaTxt"])
        self.assertEqual(r[2]["pend"], {"ok": True})
        self.assertEqual(r[2]["dir"], LC["direccion"])
        self.assertEqual(r[2]["oculto"], {"pid": "P_LC", "lat": "-33.41", "lng": "-70.58"})
        self.assertFalse(r[2]["pie"]["apagado"])
        self.assertNotIn("ecfRestaurarDireccion", r[2]["cajaHtml"])

    def test_tambien_repone_region_comuna_y_ciudad(self):
        r = _correr(_ini(**LC), [
            {"google": True},
            {"set": PROVIDENCIA, "verificar": PROVIDENCIA["ec_direccion"], "geo": GEO_PV},
            {"set": {"ec_direccion": "Otra calle 5"}},
            {"restaurar": True},
        ])["pasos"]
        self.assertEqual(r[3]["geo"], GEO_LC)
        self.assertEqual(r[3]["pend"], {"ok": True})

    def test_sin_direccion_guardada_no_hay_a_donde_volver(self):
        r = _correr(_ini(""), [{"google": True}, {"set": {"ec_direccion": "Algo 1"}}])["pasos"]
        self.assertEqual(r[1]["pend"], {"ok": False, "motivo": "sin_verificar"})
        self.assertNotIn("ecfRestaurarDireccion", r[1]["cajaHtml"])

    def test_si_la_direccion_no_se_toco_no_se_ofrece(self):
        r = _correr(_ini("Calle Vieja 100"), [{"google": True}])["pasos"]
        self.assertNotIn("ecfRestaurarDireccion", r[0]["cajaHtml"])


@unittest.skipIf(not NODE, "Node no está instalado")
class TestEscCierraSoloLoQueCorresponde(unittest.TestCase):
    """data-bs-keyboard="false" + este manejador: el Esc de un aviso (ilusConfirm…) o de la lista de Google no cierra el formulario entero, pero el teclado sigue sirviendo."""

    def test_esc(self):
        r = _correr(_ini(**LC), [
            {"esc": True, "aviso": True},          # un aviso encima: el Esc es suyo
            {"esc": True, "lista": True},          # la lista de sugerencias de Google abierta: el Esc es suyo
            {"esc": True},                         # nada encima: cierra el formulario
        ])["pasos"]
        self.assertEqual([p["cerrado"] for p in r], [0, 0, 1])


@unittest.skipIf(not NODE, "Node no está instalado")
class TestGoogleCargadoPeroInutil(unittest.TestCase):
    """Google rechaza la clave (gm_authFailure): el campo queda «enganchado» pero nunca da sugerencias. Eso es «Google no disponible», no un callejón."""

    def test_auth_failure_deja_guardar_con_aviso(self):
        r = _correr(_ini("Calle Vieja 100"), [
            {"google": True},
            {"set": {"ec_direccion": "Calle Nueva 5"}},
            {"authFallo": True},
        ])["pasos"]
        self.assertEqual(r[1]["pend"], {"ok": False, "motivo": "sin_verificar"})
        self.assertTrue(r[1]["pie"]["apagado"])
        self.assertEqual(r[2]["pend"], {"ok": False, "motivo": "google_no_disponible"})          # guardarCliente avisa y deja decidir
        self.assertFalse(r[2]["pie"]["apagado"])

    def test_gm_authfailure_encadena_al_anterior(self):
        with open(os.path.join(RAIZ, "static", "cliente_form.js"), encoding="utf-8") as fh:
            js = fh.read()
        self.assertIn("var _gmPrevio = window.gm_authFailure;", js)
        self.assertIn("_gmPrevio.apply(this, arguments)", js)


@unittest.skipIf(not NODE, "Node no está instalado")
class TestGoogleTodaviaCargando(unittest.TestCase):
    """2.ª revisión: en los primeros segundos «Guardar igual» decía «no hay conexión con Google» cuando Google aún estaba cargando."""

    def test_cargando_no_es_sin_conexion(self):
        r = _correr(_ini("Calle Vieja 100"), [
            {"abrir": True, "google": False, "set": {"ec_direccion": "Calle Nueva 5"}},
        ])["pasos"]
        self.assertEqual(r[0]["pend"], {"ok": False, "motivo": "google_conectando"})
        self.assertIn("Conectando con Google", r[0]["pie"]["texto"])

    def test_un_cliente_sin_tocar_dice_la_verdad_sobre_google(self):
        r = _correr(_ini("Calle Vieja 100"), [
            {"google": True},                                          # 0 con Google
            {"google": False},                                         # 1 sin Google (nunca se abrió: «no responde»)
            {"abrir": True, "google": False},                          # 2 recién abierto, Google cargando
        ])["pasos"]
        self.assertIn("elige una sugerencia del menú", r[0]["cajaTxt"])
        self.assertNotIn("elige una sugerencia del menú", r[1]["cajaTxt"])
        self.assertIn("no hay conexión con Google", r[1]["cajaTxt"])
        self.assertIn("Google está cargando", r[2]["cajaTxt"])
        for p in r:
            self.assertEqual(p["pend"], {"ok": True, "aviso": "heredada"})                          # nada de esto traba


class TestCodigoDeLaFicha(unittest.TestCase):
    """Lo que el JS de la ficha tiene que seguir haciendo para que lo anterior valga."""

    def test_google_deja_el_place_id_y_las_coordenadas_antes_de_avisar_al_semaforo(self):
        with open(os.path.join(RAIZ, "static", "mant_ficha.js"), encoding="utf-8") as fh:
            js = fh.read()
        i = js.index("function _ilusInitDireccionCliente()")
        f = js[i:i + 5200]
        self.assertLess(f.index("set('ec_direccion_place_id'"), f.index("window.ecfDireccionVerificada(place.direccion"))
        self.assertLess(f.index("set('ec_direccion_lat'"), f.index("window.ecfDireccionVerificada(place.direccion"))
        self.assertLess(f.index("setIf('ec_ciudad'"), f.index("window.ecfDireccionVerificada(place.direccion"))
        # y le cuenta qué región/comuna/ciudad trajo Google (vacío si no trajo alguna)
        self.assertIn("{ region: _ilusLimpiaRegion(region), comuna: comuna, ciudad: ciudad }", f)

    def test_guardar_deja_el_semaforo_al_dia_antes_de_leer_los_campos(self):
        with open(os.path.join(RAIZ, "static", "mant_ficha.js"), encoding="utf-8") as fh:
            js = fh.read()
        i = js.index("async function guardarCliente()")
        f = js[i:i + 3600]
        self.assertLess(f.index("window.ecfEvaluar()"), f.index("const data = {"))
        # Google todavía cargando: se pide esperar, no se dice «no hay conexión»
        self.assertIn("dp.motivo === 'google_conectando'", f)
        self.assertLess(f.index("dp.motivo === 'google_conectando'"), f.index("dp.motivo === 'google_no_disponible'"))

    def test_el_modal_no_deja_que_bootstrap_cierre_con_esc_y_el_manejador_propio_lo_hace(self):
        with open(os.path.join(RAIZ, "templates", "mantenciones", "ficha.html"), encoding="utf-8") as fh:
            ficha = fh.read()
        self.assertIn('id="modalEditarCliente" tabindex="-1" aria-labelledby="ecfTitulo" data-bs-keyboard="false"', ficha)
        with open(os.path.join(RAIZ, "static", "cliente_form.js"), encoding="utf-8") as fh:
            js = fh.read()
        self.assertIn("m.querySelector('.ilus-overlay')", js)
        self.assertIn("document.querySelector('.pac-container')", js)

    def test_show_ya_no_recalcula_lo_verificado_desde_el_campo(self):
        with open(os.path.join(RAIZ, "static", "cliente_form.js"), encoding="utf-8") as fh:
            js = fh.read()
        bloque = js[js.index("m.addEventListener('show.bs.modal'"):js.index("m.addEventListener('shown.bs.modal'")]
        self.assertNotIn("verificada =", bloque)
        self.assertNotIn("place_id", bloque)
        self.assertIn("capturarInicial()", js)


if __name__ == "__main__":
    unittest.main()
