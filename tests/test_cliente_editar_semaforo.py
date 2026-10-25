# -*- coding: utf-8 -*-
"""2026-10-24 · Daniel: «necesito que el modal de editar clientes se comporte como el formulario: que tenga semáforo, tarjetas con paso a
paso, la validación de Google, el cargo con más espacio». Y después: «la ubicación se debe verificar para poder avanzar, y los espacios de
región, comuna y ciudad deben ser grandes… se va a entrecortar cualquier dato que escriba en la parte responsiva».

Solo cambia cómo se PRESENTA el modal «Editar cliente» de la ficha: los mismos ids `ec_*`, el mismo `guardarCliente()` y lo mismo que se
guarda. El semáforo (static/cliente_form.js) marca; lo ÚNICO que traba el guardado es la dirección sin verificar con Google (y la razón social
vacía, que ya trababa). Lo demás mal escrito se avisa («alertar, no trabar»).
"""
import json
import os
import re
import shutil
import subprocess
import tempfile
import unittest

from jinja2 import Environment

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NODE = shutil.which("node")


def _leer(*partes):
    with open(os.path.join(RAIZ, *partes), encoding="utf-8", newline="") as fh:
        return fh.read().replace("\r\n", "\n")


FICHA = _leer("templates", "mantenciones", "ficha.html")
MANT_JS = _leer("static", "mant_ficha.js")
ECF_JS = _leer("static", "cliente_form.js")
ECF_CSS = _leer("static", "cliente_form.css")

# Los 26 campos que ya existían: ninguno puede perderse (REGLA #4.2).
IDS_EC = [
    "ec_razon", "ec_rut", "ec_estado", "ec_tipo_cliente", "ec_giro", "ec_dia_pref", "ec_email_empresa", "ec_tel_empresa",
    "ec_direccion", "ec_direccion_lat", "ec_direccion_lng", "ec_direccion_place_id", "ec_direccion_hint", "ec_region", "ec_comuna",
    "ec_ciudad", "ec_contacto_nombre", "ec_contacto_cargo", "ec_contacto_tel", "ec_contacto_email", "ec_ejecutivo_user_id",
    "ec_contacto2_nombre", "ec_contacto2_cargo", "ec_contacto2_tel", "ec_contacto2_email", "ec_notas",
]
# Los textos que pueden ser largos van en cajas que CRECEN con lo escrito (nunca se cortan).
IDS_QUE_CRECEN = [
    "ec_razon", "ec_giro", "ec_region", "ec_comuna", "ec_ciudad", "ec_email_empresa", "ec_contacto_nombre", "ec_contacto_cargo",
    "ec_contacto_email", "ec_contacto2_nombre", "ec_contacto2_cargo", "ec_contacto2_email",
]


def _modal():
    i = FICHA.index("{# ── Modal: Editar cliente ──")
    j = FICHA.index("{# ── Modal: Buscar en ERP ── #}")
    return FICHA[i:j]


def _fn(src, inicio):
    """Texto de una función (hasta su llave de cierre) a partir de `inicio`."""
    k = src.index(inicio)
    nivel, pos = 0, src.index("{", k)
    for n in range(pos, len(src)):
        if src[n] == "{":
            nivel += 1
        elif src[n] == "}":
            nivel -= 1
            if nivel == 0:
                return src[k:n + 1]
    raise AssertionError("no cierra: " + inicio)


class TestMarcado(unittest.TestCase):
    def setUp(self):
        self.m = _modal()

    def test_los_26_campos_de_siempre_siguen_con_su_id(self):
        for i in IDS_EC:
            self.assertIn('id="%s"' % i, self.m, "se perdió el campo %s" % i)

    def test_seis_tarjetas_de_paso_con_semaforo(self):
        for n in range(1, 7):
            self.assertIn('id="ecfSec%d"' % n, self.m)
        self.assertEqual(self.m.count('class="ecf-circ"'), 6)
        # Los dos últimos son opcionales: nacen ámbar, no rojos.
        self.assertIn('class="ecf-sec is-neutral" id="ecfSec5"', self.m)
        self.assertIn('class="ecf-sec is-neutral" id="ecfSec6"', self.m)

    def test_esc_de_un_aviso_no_cierra_tambien_el_formulario(self):
        # Revisión independiente (24-oct): «Hay datos por revisar» y «Falta verificar la ubicación» se montan dentro del modal y cierran con Esc;
        # Bootstrap cerraba además el formulario entero. Se cierra con Cancelar, ✕ o el fondo.
        self.assertIn('id="modalEditarCliente" tabindex="-1" aria-labelledby="ecfTitulo" data-bs-keyboard="false"', self.m)

    def test_el_guardado_sigue_siendo_el_mismo(self):
        self.assertIn('id="btnGuardarCliente" onclick="guardarCliente()"', self.m)
        self.assertIn('data-current="{{ cliente.ejecutivo_user_id or \'\' }}"', self.m)   # lo llena _ilusCargarEjecutivos()

    def test_tipo_de_cliente_conserva_sus_cinco_opciones(self):
        for v in ("mantencion", "instalacion", "arriendo", "leasing", "prospecto"):
            self.assertIn('<option value="%s"' % v, self.m)

    def test_el_cargo_tiene_mas_espacio_que_el_nombre(self):
        for cargo, nombre in (("ec_contacto_cargo", "ec_contacto_nombre"), ("ec_contacto2_cargo", "ec_contacto2_nombre")):
            ic = self.m.index('for="%s"' % cargo)
            inom = self.m.index('for="%s"' % nombre)
            self.assertIn("col-md-7", self.m[ic - 60:ic], "el cargo debe ocupar 7/12 de la fila")
            self.assertIn("col-md-5", self.m[inom - 60:inom])

    def test_los_textos_largos_van_en_cajas_que_crecen_y_nunca_se_cortan(self):
        # Daniel: «se va a entrecortar cualquier dato que escriba en la parte responsiva».
        for i in IDS_QUE_CRECEN:
            self.assertRegex(self.m, r'<textarea id="%s" class="form-control ecf-auto[^"]*" rows="1"' % i, i)
            self.assertIn("'%s'" % i, ECF_JS, "cliente_form.js debe hacerla crecer: %s" % i)
        self.assertIn("textarea.ecf-auto{", ECF_CSS)
        self.assertIn("overflow:hidden", ECF_CSS.split("textarea.ecf-auto{")[1].split("}")[0])      # sin barra de scroll: crece
        # Los que no pueden ser textarea: la dirección (Google Places exige un <input>) y los de formato fijo.
        self.assertIn('<input type="text" id="ec_direccion"', self.m)

    def test_region_comuna_y_ciudad_son_grandes(self):
        # Daniel (2.º mensaje): «los espacios de región, comuna y ciudad deben ser grandes».
        for i in ("ec_region", "ec_comuna", "ec_ciudad"):
            self.assertRegex(self.m, r'<textarea id="%s" class="form-control ecf-auto ecf-grande"' % i)
        ir = self.m.index('for="ec_region"')
        self.assertIn('class="col-12"', self.m[ir - 60:ir], "la región usa todo el ancho")
        for i in ("ec_comuna", "ec_ciudad"):
            ic = self.m.index('for="%s"' % i)
            self.assertIn("col-12 col-md-6", self.m[ic - 70:ic])
        grande = ECF_CSS.split(".ecf .ecf-grande{")[1].split("}")[0]
        self.assertIn("min-height:56px", grande)
        self.assertIn("font-size:1.08rem", grande)

    def test_el_nombre_del_cliente_de_la_cabecera_no_se_corta_en_celular(self):
        # Revisión independiente (24-oct): un `line-clamp:2` cortaba razones sociales largas (REGLA #15: ningún dato se trunca).
        self.assertNotIn("line-clamp", ECF_CSS)
        self.assertNotIn("text-overflow:ellipsis", ECF_CSS)
        self.assertIn(".ecf-head .sub{overflow-wrap:anywhere}", ECF_CSS)

    def test_el_modal_se_ensancha_en_tablet(self):
        # A 768–991 px el modal de Bootstrap mide 500 px y las columnas quedaban angostas.
        self.assertIn("@media (min-width:576px){.ecf .modal-dialog{max-width:min(1100px,calc(100vw - 1.5rem))}}", ECF_CSS)

    def test_la_ubicacion_verificada_es_requisito_y_el_pie_dice_por_que(self):
        self.assertIn("La dirección debe quedar verificada para poder guardar.", self.m)
        self.assertIn('id="ecfFootEstado"', self.m)
        self.assertIn("Para guardar falta ", ECF_JS)
        self.assertIn(".ecf-foot .btn-ilus:disabled", ECF_CSS)

    def test_el_mensaje_de_google_reemplaza_al_texto_viejo(self):
        self.assertIn('id="ecfVerif"', self.m)
        self.assertIn(".ecf #ec_direccion_hint{display:none!important}", ECF_CSS)   # mant_ficha.js lo sigue escribiendo, pero no se ve
        self.assertNotIn('id="ec_direccion_hint" class="d-block', self.m)

    def test_se_incluyen_los_estilos_y_el_script(self):
        self.assertIn("filename='cliente_form.css'", FICHA)
        self.assertIn("filename='cliente_form.js'", FICHA)

    def test_nada_de_popups_nativos(self):
        for src in (ECF_JS, _fn(MANT_JS, "async function guardarCliente()")):
            self.assertIsNone(re.search(r"(?<![\w.])(alert|confirm|prompt)\(", src), "REGLA #1: usa ilusAlert / ilusConfirm")

    def test_mobile_primero(self):
        self.assertIn("@media (max-width:575.98px)", ECF_CSS)
        self.assertIn("min-height:46px", ECF_CSS)          # botones y campos tocables
        self.assertIn("font-size:16px", ECF_CSS)           # iOS no hace zoom
        self.assertIn(".ecf-foot-estado{order:3;width:100%", ECF_CSS)   # el pie usa column-reverse en celular: el aviso va arriba


class TestRenderizado(unittest.TestCase):
    """El bloque se renderiza de verdad con datos de mentira y cada valor cae en su campo."""

    def test_los_valores_del_cliente_caen_en_sus_campos(self):
        env = Environment(autoescape=True)
        env.filters["rut_fmt"] = lambda r: "76.753.123-0" if r else ""
        env.filters["tel_chile_fmt"] = lambda t: t or ""
        cli = dict(razon_social="Aurum Fit Girls SpA", rut="767531230", estado="activo", tipo_cliente="arriendo", giro="Gimnasio",
                   dia_mantencion_pref=12, email_empresa="a@aurum.cl", tel_empresa="+56 2 2345 6789", direccion="Apoquindo 4500",
                   direccion_lat=-33.4, direccion_lng=-70.5, direccion_place_id="ChIJabc", region="Metropolitana", comuna="Las Condes",
                   ciudad="Santiago", contacto_nombre="María González", contacto_cargo="Administradora", contacto_tel="+56 9 8765 4321",
                   contacto_email="m@aurum.cl", ejecutivo_user_id=3, contacto2_nombre="Ana", contacto2_cargo="Jefa",
                   contacto2_tel="", contacto2_email="", notas="Llamar antes")
        html = env.from_string(_modal()).render(cliente=cli)
        for fragmento in ('spellcheck="false">Aurum Fit Girls SpA</textarea>', 'value="76.753.123-0"',
                          'id="ec_dia_pref" class="form-control" min="1" max="28"', 'value="ChIJabc"', 'data-current="3"',
                          '>María González</textarea>', '>Administradora</textarea>', '>Metropolitana</textarea>', '>Las Condes</textarea>',
                          '>Santiago</textarea>', '>a@aurum.cl</textarea>', '>m@aurum.cl</textarea>', '>Llamar antes</textarea>'):
            self.assertIn(fragmento, html, fragmento)
        self.assertRegex(html, r'<option value="arriendo"\s+selected>')       # el tipo guardado queda elegido
        self.assertNotRegex(html, r'<option value="mantencion"\s+selected>')


@unittest.skipIf(not NODE, "Node no está instalado")
class TestSemaforo(unittest.TestCase):
    """cliente_form.js corre de verdad (con un DOM mínimo de mentira) y marca lo que debe."""

    GUION = r"""
const fs = require('fs'), vm = require('vm');
const els = {};
function El(id) {
  this.id = id; this.value = ''; this.className = ''; this.textContent = ''; this.innerHTML = ''; this.title = ''; this.dataset = {}; this.style = {};
  this.hidden = false; this.disabled = false; this._c = new Set(); this._h = {};
  const self = this;
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
vm.runInThisContext(fs.readFileSync(process.argv[2], 'utf8'));
const set = function (o) { for (const k of Object.keys(o)) document.getElementById(k).value = o[k]; };
const cls = function (id) { const e = document.getElementById(id); return ['ecf-ok', 'ecf-bad', 'ecf-warn'].filter(function (c) { return e.classList.contains(c); })[0] || ''; };
const paso = function (n) { const e = document.getElementById('ecfSec' + n); return e.classList.contains('is-complete') ? 'verde' : (e.classList.contains('is-neutral') ? 'ambar' : 'rojo'); };
const pie = function () { const p = document.getElementById('ecfFootEstado'), b = document.getElementById('btnGuardarCliente'); return { texto: p.textContent || p.innerHTML.replace(/<[^>]+>/g, ''), clase: p.className, guardarApagado: b.disabled, ir: p.dataset.ir }; };
const google = function (si) { document.getElementById('ec_direccion').dataset.placesInit = si ? '1' : ''; };
const out = {};
// 1) Todo vacío (Google todavía sin cargar)
window.ecfEvaluar();
out.vacio = { razon: cls('ec_razon'), rut: cls('ec_rut'), dir: cls('ec_direccion'), pasos: [1, 2, 3, 4, 5, 6].map(paso).join(','), prog: document.getElementById('ecfProgTxt').textContent, pie: pie() };
// 2) Escritura mala: sale en rojo y en la lista de problemas
set({ ec_razon: 'Fit Paradise SpA', ec_rut: '12.345.678-9', ec_dia_pref: '40', ec_email_empresa: 'sin-arroba.cl', ec_tel_empresa: '12345',
      ec_contacto_nombre: 'Pedro', ec_contacto_tel: '9999', ec_contacto_email: 'pedro@' });
window.ecfEvaluar();
out.malos = { rut: cls('ec_rut'), dia: cls('ec_dia_pref'), email: cls('ec_email_empresa'), tel: cls('ec_tel_empresa'), p1: paso(1), p3: paso(3),
              problemas: window.ecfProblemas().map(function (p) { return p.id; }) };
// 3) Todo bien (RUT válido, correo y teléfonos válidos) → paso 1 y 3 en verde
set({ ec_rut: '12.345.678-5', ec_dia_pref: '12', ec_email_empresa: 'contacto@fit.cl', ec_tel_empresa: '+56 2 2345 6789',
      ec_contacto_tel: '+56 9 8765 4321', ec_contacto_email: 'pedro@fit.cl' });
window.ecfEvaluar();
out.bien = { rut: cls('ec_rut'), p1: paso(1), p3: paso(3), problemas: window.ecfProblemas().length };
// 4) Dirección SIN Google disponible: no hay cómo verificar → ámbar, no traba (guardarCliente avisa y deja decidir)
set({ ec_direccion: 'Alameda 123, Santiago, Chile', ec_region: 'Metropolitana', ec_comuna: 'Santiago', ec_ciudad: 'Santiago' });
google(false);
window.ecfEvaluar();
out.sinGoogle = { dir: cls('ec_direccion'), p2: paso(2), pie: pie(), pendiente: window.ecfDireccionPendiente() };
// 5) Con Google disponible pero sin elegir sugerencia → ROJO y no se puede guardar
google(true);
window.ecfEvaluar();
out.sinVerificar = { dir: cls('ec_direccion'), p2: paso(2), pie: pie(), pendiente: window.ecfDireccionPendiente(), caja: document.getElementById('ecfVerif').className };
// 6) Se elige una sugerencia de Google → verificada
set({ ec_direccion_place_id: 'ChIJx' });
window.ecfDireccionVerificada('Alameda 123, Santiago, Chile');
out.verificada = { dir: cls('ec_direccion'), p2: paso(2), pie: pie(), pendiente: window.ecfDireccionPendiente(), caja: document.getElementById('ecfVerif').className };
// 7) Agregar oficina o depto AL FINAL no pierde la verificación
set({ ec_direccion: 'Alameda 123, Santiago, Chile, oficina 301' });
window.ecfEvaluar();
out.conDetalle = { dir: cls('ec_direccion'), p2: paso(2), pie: pie(), caja: document.getElementById('ecfVerif').innerHTML.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim() };
// 8) Tocar lo verificado SÍ la pierde
set({ ec_direccion: 'Alameda 1234, Santiago, Chile' });
window.ecfEvaluar();
out.retocada = { dir: cls('ec_direccion'), p2: paso(2), pie: pie(), pendiente: window.ecfDireccionPendiente() };
// 9) Dirección vacía con Google disponible
set({ ec_direccion: '' });
window.ecfEvaluar();
out.vaciaConGoogle = { pie: pie(), pendiente: window.ecfDireccionPendiente() };
// 10) Razón social vacía también apaga Guardar
set({ ec_direccion: 'Alameda 123, Santiago, Chile', ec_razon: '' });
window.ecfEvaluar();
out.sinRazon = { pie: pie() };
set({ ec_razon: 'Fit Paradise SpA' });
// 11) Ejecutivo y progreso
set({ ec_ejecutivo_user_id: '3' });
window.ecfEvaluar();
out.ejecutivo = { p4: paso(4), prog: document.getElementById('ecfProgTxt').textContent, ancho: document.getElementById('ecfProgBar').style.width, pie: pie() };
// 12) Contacto secundario: opcional, ámbar si está vacío
out.sec = { p5: paso(5), p6: paso(6) };
set({ ec_contacto2_nombre: 'Ana López', ec_contacto2_email: 'ana@fit.cl' });
window.ecfEvaluar();
out.sec2 = { p5: paso(5) };
// 13) Las cajas de una sola línea: Enter no agrega saltos y lo pegado con saltos se vuelve espacios
const modal = document.getElementById('modalEditarCliente');
const ta = document.getElementById('ec_giro'); ta.classList.add('ecf-auto');
let evitado = 0; const tecla = function (t, key) { let p = 0; (modal._h.keydown || []).forEach(function (fn) { fn({ key: key, target: t, preventDefault: function () { p++; } }); }); return p; };
out.enter = { enCaja: tecla(ta, 'Enter'), enOtro: tecla(document.getElementById('ec_rut'), 'Enter'), otraTecla: tecla(ta, 'a') };
ta.value = 'Gimnasios\r\ny spa\nen general';
(modal._h.input || []).forEach(function (fn) { fn({ target: ta }); });
out.pegado = ta.value;
console.log(JSON.stringify(out));
"""

    @classmethod
    def setUpClass(cls):
        with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as f:
            f.write(cls.GUION)
            ruta = f.name
        try:
            r = subprocess.run([NODE, ruta, os.path.join(RAIZ, "static", "cliente_form.js")], capture_output=True, text=True, encoding="utf-8", timeout=60)
        finally:
            os.unlink(ruta)
        assert r.returncode == 0, r.stderr[-1500:]
        cls.o = json.loads(r.stdout.strip().splitlines()[-1])

    def test_vacio_todo_en_rojo_menos_lo_opcional(self):
        v = self.o["vacio"]
        self.assertEqual(v["razon"], "ecf-bad")
        self.assertEqual(v["rut"], "ecf-warn")           # el RUT se recomienda pero no se exige
        # Un cliente SIN dirección que no se toca no se traba (se avisa en ámbar): se exige verificar lo que se escribe o cambia.
        self.assertEqual(v["dir"], "ecf-warn")
        self.assertEqual(v["pasos"], "rojo,rojo,rojo,rojo,ambar,ambar")
        self.assertEqual(v["prog"], "0 de 4 pasos listos")
        self.assertTrue(v["pie"]["guardarApagado"])          # sin razón social no se guarda (como siempre)
        self.assertIn("la razón social", v["pie"]["texto"])

    def test_lo_mal_escrito_se_ve_rojo_y_se_lista_para_avisar_antes_de_guardar(self):
        m = self.o["malos"]
        self.assertEqual((m["rut"], m["dia"], m["email"], m["tel"]), ("ecf-bad",) * 4)
        self.assertEqual((m["p1"], m["p3"]), ("rojo", "rojo"))
        self.assertEqual(m["problemas"], ["ec_rut", "ec_dia_pref", "ec_email_empresa", "ec_tel_empresa", "ec_contacto_tel", "ec_contacto_email"])

    def test_lo_bien_escrito_pasa_a_verde(self):
        b = self.o["bien"]
        self.assertEqual(b["rut"], "ecf-ok")
        self.assertEqual((b["p1"], b["p3"]), ("verde", "verde"))
        self.assertEqual(b["problemas"], 0)

    def test_sin_google_no_hay_como_verificar_y_no_se_traba(self):
        s = self.o["sinGoogle"]
        self.assertEqual(s["dir"], "ecf-warn")
        self.assertEqual(s["p2"], "rojo")
        self.assertFalse(s["pie"]["guardarApagado"])
        self.assertEqual(s["pendiente"], {"ok": False, "motivo": "google_no_disponible"})   # guardarCliente avisa y deja decidir

    def test_con_google_la_direccion_sin_verificar_traba_el_guardado(self):
        # Daniel: «la ubicación se debe verificar para poder avanzar».
        s = self.o["sinVerificar"]
        self.assertEqual(s["dir"], "ecf-bad")
        self.assertEqual(s["p2"], "rojo")
        self.assertTrue(s["pie"]["guardarApagado"])
        self.assertIn("la dirección verificada con Google", s["pie"]["texto"])
        self.assertEqual(s["pie"]["ir"], "ec_direccion")                                      # tocar el aviso lleva a la dirección
        self.assertEqual(s["pendiente"], {"ok": False, "motivo": "sin_verificar"})
        self.assertIn("bad", s["caja"])

    def test_elegir_una_sugerencia_de_google_la_verifica(self):
        v = self.o["verificada"]
        self.assertEqual((v["dir"], v["p2"]), ("ecf-ok", "verde"))
        self.assertFalse(v["pie"]["guardarApagado"])
        self.assertIn("Listo para guardar", v["pie"]["texto"])
        self.assertEqual(v["pendiente"], {"ok": True})
        self.assertIn("ok", v["caja"])

    def test_se_puede_agregar_oficina_o_depto_al_final_sin_perder_la_verificacion(self):
        d = self.o["conDetalle"]
        self.assertEqual((d["dir"], d["p2"]), ("ecf-ok", "verde"))
        self.assertFalse(d["pie"]["guardarApagado"])
        self.assertIn("Detalle agregado: oficina 301", d["caja"])
        self.assertIn("Alameda 123, Santiago, Chile", d["caja"])                              # la dirección verificada completa, sin cortar

    def test_tocar_lo_verificado_la_deja_sin_verificar(self):
        r = self.o["retocada"]
        self.assertEqual((r["dir"], r["p2"]), ("ecf-bad", "rojo"))
        self.assertTrue(r["pie"]["guardarApagado"])
        self.assertEqual(r["pendiente"], {"ok": False, "motivo": "cambiada"})

    def test_dejar_la_direccion_como_vino_y_sin_verificar_se_guarda_con_aviso(self):
        # Volver al estado inicial (sin dirección y nunca verificada) no es un cambio: no hay nada que verificar ni que trabar.
        # (Borrar una dirección que SÍ estaba verificada se traba: lo cubre test_cliente_editar_ubicacion.py.)
        v = self.o["vaciaConGoogle"]
        self.assertFalse(v["pie"]["guardarApagado"])
        self.assertEqual(v["pendiente"], {"ok": True, "aviso": "heredada"})
        self.assertIn("warn", v["pie"]["clase"])

    def test_sin_razon_social_tampoco_se_guarda(self):
        s = self.o["sinRazon"]["pie"]
        self.assertTrue(s["guardarApagado"])
        self.assertIn("la razón social", s["texto"])
        self.assertEqual(s["ir"], "ec_razon")

    def test_progreso_de_la_cabecera(self):
        e = self.o["ejecutivo"]
        self.assertEqual(e["p4"], "verde")
        self.assertEqual(e["prog"], "4 de 4 pasos listos")     # la dirección volvió al texto verificado: los 4 pasos están listos
        self.assertEqual(e["ancho"], "100%")
        self.assertIn("Listo para guardar", e["pie"]["texto"])

    def test_los_pasos_opcionales_son_ambar_hasta_que_se_usan(self):
        self.assertEqual(self.o["sec"], {"p5": "ambar", "p6": "ambar"})
        self.assertEqual(self.o["sec2"]["p5"], "verde")

    def test_las_cajas_que_crecen_son_de_una_linea(self):
        self.assertEqual(self.o["enter"], {"enCaja": 1, "enOtro": 0, "otraTecla": 0})        # Enter se bloquea solo en esas cajas
        self.assertEqual(self.o["pegado"], "Gimnasios y spa en general")                      # los saltos pegados se vuelven espacios

    def test_validadores_de_rut(self):
        guion = r"""
const fs = require('fs');
const src = fs.readFileSync(process.argv[2], 'utf8');
const i = src.indexOf('function rutOk'), j = src.indexOf('function emailOk'), k = src.indexOf('// ── Dirección: qué está verificado');
const f = new Function(src.slice(i, k) + '; return {rutOk, emailOk, telOk};')();
console.log(JSON.stringify({
  ok: ['76.753.123-0', '12.345.678-5', '6-K', '11.111.111-1'].map(f.rutOk),
  mal: ['76.753.123-1', '12.345.678-9', '', '5'].map(f.rutOk),
  mail: [f.emailOk('a@b.cl'), f.emailOk('a@b'), f.emailOk('a b@c.cl'), f.emailOk('pedro@')],
  tel: [f.telOk('+56 9 8765 4321'), f.telOk('+56 2 2345 6789'), f.telOk('2345 6789'), f.telOk('9999'), f.telOk('')],
}));
"""
        with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as fh:
            fh.write(guion)
            ruta = fh.name
        try:
            r = subprocess.run([NODE, ruta, os.path.join(RAIZ, "static", "cliente_form.js")], capture_output=True, text=True, encoding="utf-8", timeout=60)
        finally:
            os.unlink(ruta)
        self.assertEqual(r.returncode, 0, r.stderr[-800:])
        o = json.loads(r.stdout.strip().splitlines()[-1])
        self.assertEqual(o["ok"], [True] * 4)
        self.assertEqual(o["mal"], [False] * 4)
        self.assertEqual(o["mail"], [True, False, False, False])
        self.assertEqual(o["tel"], [True, True, True, False, False])


class TestGuardado(unittest.TestCase):
    """mant_ficha.js: Google Places avisa al semáforo, la dirección sin verificar no se guarda y lo demás mal escrito se avisa."""

    def test_google_places_avisa_al_semaforo(self):
        f = _fn(MANT_JS, "function _ilusInitDireccionCliente()")
        self.assertIn("window.ecfDireccionVerificada(place.direccion)", f)
        self.assertIn("window.ecfEvaluar()", f)
        # Lo que ya hacía sigue igual: coordenadas, place_id y región/comuna/ciudad.
        for pista in ("ec_direccion_lat", "ec_direccion_lng", "ec_direccion_place_id", "setIf('ec_region'", "setIf('ec_comuna'", "setIf('ec_ciudad'"):
            self.assertIn(pista, f)

    def test_la_direccion_sin_verificar_no_se_guarda(self):
        f = _fn(MANT_JS, "async function guardarCliente()")
        self.assertIn("window.ecfDireccionPendiente()", f)
        self.assertIn("title: 'Falta verificar la ubicación'", f)
        i = f.index("title: 'Falta verificar la ubicación'")
        # El bloqueo termina en `return` ANTES de pedir el PUT (no es solo un aviso).
        bloque = f[i:f.index("fetch(`/mantenciones/api/clientes/${CID}`")]
        self.assertIn("return;", bloque)
        self.assertLess(f.index("window.ecfDireccionPendiente()"), f.index("window.ecfProblemas()"))
        self.assertLess(f.index("window.ecfDireccionPendiente()"), f.index("fetch(`/mantenciones/api/clientes/${CID}`"))

    def test_sin_google_no_hay_callejon_sin_salida(self):
        f = _fn(MANT_JS, "async function guardarCliente()")
        self.assertIn("dp.motivo === 'google_no_disponible'", f)
        self.assertIn("title: 'No se pudo verificar la dirección'", f)
        self.assertIn("okLabel: 'Guardar igual'", f)

    def test_guardar_avisa_lo_mal_escrito_pero_deja_guardar_igual(self):
        f = _fn(MANT_JS, "async function guardarCliente()")
        self.assertIn("window.ecfProblemas()", f)
        self.assertIn("await ilusConfirm(", f)
        self.assertIn("cancelLabel: 'Revisar'", f)
        # El aviso va ANTES de pedir el PUT, y «Revisar» devuelve a la persona al primer dato en rojo sin guardar.
        self.assertLess(f.index("window.ecfProblemas()"), f.index("fetch(`/mantenciones/api/clientes/${CID}`"))
        self.assertIn("if (!seguir)", f)
        self.assertIn("e.focus()", f)

    def test_al_terminar_se_reevalua_para_no_dejar_guardar_encendido_de_mas(self):
        f = _fn(MANT_JS, "async function guardarCliente()")
        self.assertIn("window.ecfEvaluar()", f[f.index("finally"):])

    def test_lo_que_se_guarda_no_cambio(self):
        f = _fn(MANT_JS, "async function guardarCliente()")
        for campo in ("razon_social", "rut", "estado", "tipo_cliente", "giro", "email_empresa", "tel_empresa", "dia_mantencion_pref", "direccion",
                      "direccion_lat", "direccion_lng", "direccion_place_id", "region", "comuna", "ciudad", "contacto_nombre", "contacto_cargo",
                      "contacto_tel", "contacto_email", "contacto2_nombre", "contacto2_cargo", "contacto2_tel", "contacto2_email",
                      "ejecutivo_user_id", "notas"):
            self.assertIn(campo + ":", f, campo)
        self.assertIn("if (!data.razon_social)", f)       # la regla de siempre: sin razón social no se guarda
        self.assertIn("method: 'PUT'", f)


if __name__ == "__main__":
    unittest.main()
