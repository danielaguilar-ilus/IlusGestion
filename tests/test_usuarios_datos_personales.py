"""Datos personales del formulario Nuevo/Editar usuario (Daniel, 2026-09-29):
RUT con formato chileno, fecha de nacimiento con semáforo y dirección validada
con Google.

Corre con:
    py -m unittest tests.test_usuarios_datos_personales -v

Sigue el patrón del proyecto: app.py no se importa (arrastra pools de MySQL y del
ERP); sus funciones puras se extraen del código fuente y el cableado se revisa
sobre el texto. La lógica de navegador (static/ilus_persona_fields.js) se ejecuta
de verdad con Node y se compara contra la implementación de Python.
"""
import json
import os
import random
import re
import shutil
import subprocess
import unittest
from datetime import date
from decimal import Decimal

import usuarios_perfil as up

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HAY_NODE = shutil.which("node") is not None


def _leer(*partes):
    with open(os.path.join(RAIZ, *partes), encoding="utf-8") as f:
        return f.read()


def _extraer_funcion(src, nombre):
    m = re.search(rf"^def {re.escape(nombre)}\(.*?(?=^\S)", src, re.M | re.S)
    assert m, f"no se encontró la función {nombre} en app.py"
    return m.group(0)


def _cargar_funciones_rut():
    src = _leer("app.py")
    ns = {"re": re}
    for nombre in ("normalizar_rut", "formatear_rut", "_calcular_dv_rut", "validar_rut"):
        exec(_extraer_funcion(src, nombre), ns)
    return ns


def _correr_node(entrada):
    """Ejecuta static/ilus_persona_fields.js en Node y devuelve lo que pida `entrada`."""
    script = r"""
const fs = require('fs'), vm = require('vm');
const ctx = {}; ctx.window = ctx; vm.createContext(ctx);
vm.runInContext(fs.readFileSync(process.argv[1], 'utf8'), ctx);
const P = ctx.ilusPersona;
const inp = JSON.parse(fs.readFileSync(0, 'utf8'));
const out = {};
if (inp.dv) out.dv = inp.dv.map(b => P.rutDV(b));
if (inp.limpiar) out.limpiar = inp.limpiar.map(s => P.rutLimpiar(s));
if (inp.formatear) out.formatear = inp.formatear.map(s => P.rutFormatear(s));
if (inp.estado) out.estado = inp.estado.map(s => P.rutEstado(s));
if (inp.fechas) {
  const h = new Date(inp.hoy[0], inp.hoy[1] - 1, inp.hoy[2]);
  out.fechas = inp.fechas.map(s => P.fechaNacEstado(s, h));
}
console.log(JSON.stringify(out));
"""
    r = subprocess.run(
        ["node", "-e", script, os.path.join(RAIZ, "static", "ilus_persona_fields.js")],
        input=json.dumps(entrada), capture_output=True, text=True, encoding="utf-8", timeout=60)
    assert r.returncode == 0, r.stderr
    return json.loads(r.stdout)


class TestFechaNacimientoPython(unittest.TestCase):
    HOY = date(2026, 9, 29)

    def test_vacio_es_valido_y_devuelve_none(self):
        self.assertEqual(up.validar_fecha_nacimiento("", self.HOY), (True, None))
        self.assertEqual(up.validar_fecha_nacimiento("   ", self.HOY), (True, None))
        self.assertEqual(up.validar_fecha_nacimiento(None, self.HOY), (True, None))

    def test_fecha_normal(self):
        self.assertEqual(up.validar_fecha_nacimiento("1990-05-17", self.HOY), (True, date(1990, 5, 17)))

    def test_29_de_febrero_solo_en_bisiesto(self):
        self.assertTrue(up.validar_fecha_nacimiento("2000-02-29", self.HOY)[0])
        self.assertFalse(up.validar_fecha_nacimiento("1990-02-29", self.HOY)[0])

    def test_rechaza_fecha_que_no_existe_o_mal_escrita(self):
        for malo in ("1990-02-30", "1990-13-01", "17/05/1990", "17-05-1990", "1990-5-17", "20260101", "hola"):
            ok, msg = up.validar_fecha_nacimiento(malo, self.HOY)
            self.assertFalse(ok, malo)
            self.assertIsInstance(msg, str)

    def test_rechaza_futura(self):
        ok, msg = up.validar_fecha_nacimiento("2026-09-30", self.HOY)
        self.assertFalse(ok)
        self.assertIn("futura", msg)

    def test_limites_de_edad_minima(self):
        self.assertTrue(up.validar_fecha_nacimiento("2011-09-29", self.HOY)[0])   # cumple 15 hoy
        self.assertFalse(up.validar_fecha_nacimiento("2011-09-30", self.HOY)[0])  # 14 años y 364 días

    def test_limites_de_edad_maxima(self):
        self.assertTrue(up.validar_fecha_nacimiento("1926-09-29", self.HOY)[0])   # 100 años justos
        self.assertTrue(up.validar_fecha_nacimiento("1925-09-30", self.HOY)[0])   # 100 años, cumple 101 mañana
        self.assertFalse(up.validar_fecha_nacimiento("1925-09-29", self.HOY)[0])  # 101 años cumplidos hoy

    def test_edad_en_anios(self):
        self.assertEqual(up.edad_en_anios(date(1990, 5, 17), self.HOY), 36)
        self.assertEqual(up.edad_en_anios(date(1990, 10, 1), self.HOY), 35)


class TestDireccionValidada(unittest.TestCase):
    OK = dict(lat="-33.4123456", lng="-70.5678901", place_id="ChIJN1t_tDeuEmsRUsoyG83frY4")

    def test_vacia_se_limpia(self):
        ok, d = up.resolver_direccion("  ", "", "", "")
        self.assertTrue(ok)
        self.assertEqual(d["accion"], "limpiar")
        self.assertIsNone(d["direccion"])

    def test_vacia_ignora_coordenadas_sueltas(self):
        ok, d = up.resolver_direccion("", **self.OK)
        self.assertTrue(ok)
        self.assertEqual(d["accion"], "limpiar")
        self.assertIsNone(d["lat"])

    def test_nueva_validada_se_guarda(self):
        ok, d = up.resolver_direccion("Av. Apoquindo 4500,  Las Condes", **self.OK)
        self.assertTrue(ok)
        self.assertEqual(d["accion"], "guardar")
        self.assertEqual(d["direccion"], "Av. Apoquindo 4500, Las Condes")
        self.assertEqual((d["lat"], d["lng"]), (-33.4123456, -70.5678901))
        self.assertEqual(d["place_id"], self.OK["place_id"])

    def test_nueva_sin_validar_se_rechaza(self):
        ok, msg = up.resolver_direccion("Av. Apoquindo 4500", "", "", "")
        self.assertFalse(ok)
        self.assertEqual(msg, up.MSG_DIRECCION_SIN_VALIDAR)

    def test_coordenadas_a_medias_se_rechazan(self):
        for lat, lng, pid in (("-33.4", "", "ChIJN1t_tDeuEmsRUsoyG83frY4"),
                              ("", "-70.5", "ChIJN1t_tDeuEmsRUsoyG83frY4"),
                              ("-33.4", "-70.5", "")):
            self.assertFalse(up.resolver_direccion("Av. X 1", lat, lng, pid)[0], (lat, lng, pid))

    def test_ubicacion_fuera_de_chile_se_rechaza(self):
        for lat, lng in (("0", "0"), ("40.4", "-3.7"), ("-70.5", "-33.4"), ("nan", "-70.5"), ("abc", "def")):
            ok, msg = up.resolver_direccion("Av. X 1", lat, lng, "ChIJN1t_tDeuEmsRUsoyG83frY4")
            self.assertFalse(ok, (lat, lng))

    def test_isla_de_pascua_y_antartica_chilena_son_chile(self):
        self.assertTrue(up.resolver_direccion("Hanga Roa", "-27.1127", "-109.3497", "ChIJN1t_tDeuEmsRUsoyG83frY4")[0])
        self.assertTrue(up.resolver_direccion("Base Frei", "-62.2", "-58.9", "ChIJN1t_tDeuEmsRUsoyG83frY4")[0])

    def test_place_id_con_caracteres_raros_se_rechaza(self):
        for pid in ("corto", "ChIJ<script>alert(1)</script>", "con espacios y mas de diez"):
            self.assertFalse(up.resolver_direccion("Av. X 1", "-33.4", "-70.5", pid)[0], pid)

    def test_direccion_vieja_sin_cambios_se_mantiene(self):
        ok, d = up.resolver_direccion("Calle Vieja 123", "", "", "", direccion_actual="  Calle   Vieja 123 ")
        self.assertTrue(ok)
        self.assertEqual(d["accion"], "mantener")

    def test_direccion_vieja_editada_sin_validar_se_rechaza(self):
        self.assertFalse(up.resolver_direccion("Calle Vieja 124", "", "", "", direccion_actual="Calle Vieja 123")[0])

    def test_al_crear_no_hay_direccion_previa_que_mantener(self):
        self.assertFalse(up.resolver_direccion("Calle Vieja 123", "", "", "", direccion_actual=None)[0])
        # editar un usuario que no tenía dirección: escribir una sin validar tampoco pasa
        self.assertFalse(up.resolver_direccion("Calle Vieja 123", "", "", "", direccion_actual="")[0])


@unittest.skipUnless(HAY_NODE, "Node no está instalado: se omiten las pruebas del navegador")
class TestRutYFechaEnElNavegador(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.py = _cargar_funciones_rut()

    def test_digito_verificador_identico_a_python_en_500_ruts(self):
        rng = random.Random(20260929)
        cuerpos = [str(rng.randint(1_000_000, 99_999_999)) for _ in range(500)]
        # asegurar casos con K y con 0
        cuerpos += [str(b) for b in range(10_000_000, 10_000_400) if self.py["_calcular_dv_rut"](str(b)) in ("K", "0")][:20]
        js = _correr_node({"dv": cuerpos})["dv"]
        esperado = [self.py["_calcular_dv_rut"](c) for c in cuerpos]
        self.assertEqual(js, esperado)
        self.assertIn("K", esperado)
        self.assertIn("0", esperado)

    def test_rut_de_daniel(self):
        r = _correr_node({"estado": ["25.547.065-5", "255470655", "25547065-5", "25.547.065-6"]})["estado"]
        self.assertEqual([x["estado"] for x in r], ["valido", "valido", "valido", "invalido"])
        self.assertEqual(r[0]["formateado"], "25.547.065-5")
        self.assertEqual(r[3]["dvEsperado"], "5")

    def test_formato_igual_al_de_python(self):
        entradas = ["255470655", "12.345.678-5", "1234567-4", "76996964-0", "5126663-K"]
        js = _correr_node({"formatear": entradas})["formatear"]
        for entrada, salida in zip(entradas, js):
            self.assertEqual(salida, self.py["formatear_rut"](self.py["normalizar_rut"](entrada)), entrada)

    def test_limpieza_al_escribir(self):
        entradas = ["abc12.345.678-k", "12K3", "kkk", "  1 2 3  ", "1234567890123", "12345678K", ""]
        esperado = ["12345678K", "123", "", "123", "123456789", "12345678K", ""]
        self.assertEqual(_correr_node({"limpiar": entradas})["limpiar"], esperado)

    def test_estados_mientras_se_escribe(self):
        r = _correr_node({"estado": ["", "1", "1234567", "12345678"]})["estado"]
        self.assertEqual([x["estado"] for x in r][:3], ["vacio", "incompleto", "incompleto"])

    def test_rut_valido_de_python_es_valido_en_js_y_alterado_es_invalido(self):
        rng = random.Random(7)
        cuerpos = [str(rng.randint(5_000_000, 30_000_000)) for _ in range(60)]
        buenos = [c + self.py["_calcular_dv_rut"](c) for c in cuerpos]
        malos = [c + ("0" if self.py["_calcular_dv_rut"](c) != "0" else "1") for c in cuerpos]
        r = _correr_node({"estado": buenos + malos})["estado"]
        self.assertTrue(all(x["estado"] == "valido" for x in r[:60]))
        self.assertTrue(all(x["estado"] == "invalido" for x in r[60:]))
        for b in buenos:  # y Python está de acuerdo
            self.assertTrue(self.py["validar_rut"](b, auto_completar_dv=False)[0])

    def test_fecha_de_nacimiento_igual_que_python(self):
        hoy = (2026, 9, 29)
        fechas = ["", "1990-05-17", "2000-02-29", "1990-02-29", "1990-02-30", "1990-13-01", "17/05/1990",
                  "2026-09-30", "2011-09-29", "2011-09-30", "1926-09-29", "1926-09-28", "2030-01-01", "1899-01-01"]
        js = _correr_node({"fechas": fechas, "hoy": list(hoy)})["fechas"]
        for f, r in zip(fechas, js):
            ok_py, _ = up.validar_fecha_nacimiento(f, date(*hoy))
            self.assertEqual(r["estado"] in ("valido", "vacio"), ok_py, f)
        edades = _correr_node({"fechas": ["1990-05-17", "1990-10-01"], "hoy": list(hoy)})["fechas"]
        self.assertEqual([e["edad"] for e in edades], [36, 35])


class TestCableadoDelServidor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.src = _leer("app.py")
        m = re.search(r"^def edit_user\(.*?(?=^@app\.route\(\"/admin/users/<int:user_id>/delete\")", cls.src, re.M | re.S)
        cls.edit_user = m.group(0)
        m = re.search(r"^def new_user\(.*?(?=^@app\.route\(\"/admin/users/<int:user_id>/edit\")", cls.src, re.M | re.S)
        cls.new_user = m.group(0)

    def test_modulo_importado(self):
        self.assertIn("import usuarios_perfil as _uperf", self.src)

    def test_columnas_geo_se_crean_siempre_en_el_arranque(self):
        self.assertIn("def _ensure_app_users_geo_columns():", self.src)
        self.assertRegex(self.src, r"(?m)^    _ensure_app_users_geo_columns\(\)\s*$")

    def test_migracion_es_una_sentencia_por_columna(self):
        fn = _extraer_funcion(self.src, "_ensure_app_users_geo_columns")
        # REGLA #18: una cláusula por ALTER para que _ddl_ya_aplicado pueda saltarla sin lock
        self.assertEqual(len(re.findall(r"ADD COLUMN", fn)), 3)
        self.assertNotRegex(fn, r"ADD COLUMN[^\"]*,\s*ADD COLUMN")

    def test_login_no_depende_de_las_columnas_nuevas(self):
        for nombre in ("get_auth_user_by_id", "get_auth_user_by_username"):
            fn = _extraer_funcion(self.src, nombre)
            self.assertNotIn("direccion_lat", fn, nombre)
            self.assertNotIn("direccion_lng", fn, nombre)
            self.assertNotIn("direccion_place_id", fn, nombre)

    def test_alta_y_edicion_validan_y_guardan(self):
        for nombre, fn in (("new_user", self.new_user), ("edit_user", self.edit_user)):
            self.assertIn("_validar_rut_usuario(", fn, nombre)
            self.assertIn("_uperf.validar_fecha_nacimiento(", fn, nombre)
            self.assertIn("_uperf.resolver_direccion(", fn, nombre)
            self.assertIn("_guardar_perfil_usuario(", fn, nombre)

    def test_edicion_pasa_las_coordenadas_a_la_plantilla_en_los_dos_render(self):
        self.assertEqual(self.edit_user.count("geo=_usuario_geo(user_id)"), 2)

    def test_where_del_guardado_es_siempre_una_constante(self):
        usos = re.findall(r"_guardar_perfil_usuario\(cur, (\"?\w+\"?),", self.src)
        usos = [u for u in usos if u != "where_col"]  # la línea `def` de la propia función
        self.assertEqual(sorted(usos), ['"id"', '"username"'])

    def test_rut_se_valida_estricto_y_sin_duplicados(self):
        fn = _extraer_funcion(self.src, "_validar_rut_usuario")
        self.assertIn("validar_rut(txt, auto_completar_dv=False)", fn)
        self.assertIn("Ese RUT ya está registrado", fn)
        self.assertIn("escape", fn)  # los nombres se escapan: la plantilla imprime los errores con |safe

    def test_mi_cuenta_borra_la_ubicacion_si_cambia_el_texto(self):
        m = re.search(r"^def mi_cuenta_datos\(.*?(?=^@app\.route)", self.src, re.M | re.S)
        self.assertIn("direccion_lat=NULL", m.group(0))


class _Multi(dict):
    """Imita request.form (get + `in`) sin importar werkzeug."""


class TestPlantillaUsuario(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            import jinja2
        except ImportError:
            raise unittest.SkipTest("jinja2 no está instalado")
        cls.py = _cargar_funciones_rut()
        base = ("{% block title %}{% endblock %}{% block head_extra %}{% endblock %}"
                "{% block content %}{% endblock %}{% block scripts %}{% endblock %}")
        cls.env = jinja2.Environment(
            loader=jinja2.DictLoader({"base.html": base, "user_form.html": _leer("templates", "user_form.html")}),
            autoescape=True)
        cls.env.filters["rut_fmt"] = lambda v: cls.py["formatear_rut"](v) if v else ""
        cls.env.globals.update(
            csrf_token=lambda: "tok", url_for=lambda ep, **kw: "/" + ep + (("/" + kw["filename"]) if "filename" in kw else ""),
            permissions={"superadmin": True})

    def _render(self, **ctx):
        base = dict(errors=[], user=None, fd={}, roles=[])
        base.update(ctx)
        return self.env.get_template("user_form.html").render(**base)

    def _valor(self, html, name):
        m = re.search(rf'<input[^>]*name="{name}"[^>]*value="([^"]*)"', html)
        self.assertIsNotNone(m, f"no se encontró el campo {name}")
        return m.group(1)

    def test_usuario_nuevo_trae_los_tres_pasos_vacios(self):
        html = self._render()
        for paso in ("ufCircle-rut", "ufCircle-fecha", "ufCircle-dir"):
            self.assertIn(paso, html)
        for n in ("5", "6", "7"):
            self.assertIn(f'<span class="uf-step-num">{n}</span>', html)
        self.assertIn("ilus_persona_fields.js", html)
        for name in ("rut", "fecha_nac", "direccion", "direccion_lat", "direccion_lng", "direccion_place_id", "comuna", "ciudad"):
            self.assertEqual(self._valor(html, name), "", name)

    def test_reintento_tras_error_conserva_lo_escrito(self):
        fd = _Multi(rut="12.345.678-9", fecha_nac="1990-05-17", direccion="Av. X 1",
                    direccion_lat="-33.41", direccion_lng="-70.56", direccion_place_id="ChIJN1t_tDeuEmsRUsoyG83frY4",
                    comuna="Las Condes", ciudad="Santiago")
        html = self._render(errors=["algo falló"], fd=fd)
        self.assertEqual(self._valor(html, "rut"), "12.345.678-9")
        self.assertEqual(self._valor(html, "fecha_nac"), "1990-05-17")
        self.assertEqual(self._valor(html, "direccion_lat"), "-33.41")
        self.assertEqual(self._valor(html, "direccion_place_id"), "ChIJN1t_tDeuEmsRUsoyG83frY4")
        self.assertEqual(self._valor(html, "comuna"), "Las Condes")

    def test_reintento_respeta_un_campo_borrado_a_proposito(self):
        user = dict(id=7, username="f@x.cl", nombre="Felipe", role="editor", active=1, phone=None,
                    rut="255470655", fecha_nac=date(1990, 5, 17), direccion="Vieja 1", comuna="X", ciudad="Y")
        html = self._render(errors=["algo falló"], user=user, fd=_Multi(username="f@x.cl", nombre="Felipe", rut="", fecha_nac="", direccion=""))
        self.assertEqual(self._valor(html, "rut"), "")
        self.assertEqual(self._valor(html, "fecha_nac"), "")
        self.assertEqual(self._valor(html, "direccion"), "")

    def test_editar_muestra_lo_guardado_con_formato(self):
        user = dict(id=7, username="f@x.cl", nombre="Felipe", role="editor", active=1, phone=None,
                    rut="255470655", fecha_nac=date(1990, 5, 17), direccion="Av. Apoquindo 4500, Las Condes",
                    comuna="Las Condes", ciudad="Santiago")
        geo = dict(direccion_lat=Decimal("-33.4123456"), direccion_lng=Decimal("-70.5678901"),
                   direccion_place_id="ChIJN1t_tDeuEmsRUsoyG83frY4")
        html = self._render(user=user, geo=geo)
        self.assertEqual(self._valor(html, "rut"), "25.547.065-5")
        self.assertEqual(self._valor(html, "fecha_nac"), "1990-05-17")
        self.assertEqual(self._valor(html, "direccion"), "Av. Apoquindo 4500, Las Condes")
        self.assertEqual(self._valor(html, "direccion_lat"), "-33.4123456")
        self.assertEqual(self._valor(html, "direccion_place_id"), "ChIJN1t_tDeuEmsRUsoyG83frY4")
        self.assertIn('data-original="Av. Apoquindo 4500, Las Condes"', html)

    def test_editar_direccion_vieja_sin_ubicacion(self):
        user = dict(id=7, username="f@x.cl", nombre="Felipe", role="editor", active=1, phone=None,
                    rut=None, fecha_nac=None, direccion="Calle Vieja 123", comuna=None, ciudad=None)
        html = self._render(user=user, geo={})
        self.assertEqual(self._valor(html, "direccion"), "Calle Vieja 123")
        self.assertEqual(self._valor(html, "direccion_lat"), "")
        self.assertIn('data-original="Calle Vieja 123"', html)
        self.assertEqual(self._valor(html, "rut"), "")

    def test_editar_sin_variable_geo_no_revienta(self):
        user = dict(id=7, username="f@x.cl", nombre="Felipe", role="editor", active=1, phone=None,
                    rut=None, fecha_nac=None, direccion=None, comuna=None, ciudad=None)
        self.assertIn("Datos personales", self._render(user=user))

    def test_el_script_inline_de_datos_personales_es_javascript_valido(self):
        if not HAY_NODE:
            self.skipTest("Node no está instalado")
        html = self._render()
        bloque = [b for b in re.findall(r"<script>(.*?)</script>", html, re.S) if "DATOS PERSONALES" in b]
        self.assertEqual(len(bloque), 1)
        import tempfile
        with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as f:
            f.write(bloque[0])
        try:
            r = subprocess.run(["node", "--check", f.name], capture_output=True, text=True)
            self.assertEqual(r.returncode, 0, r.stderr)
        finally:
            os.unlink(f.name)


if __name__ == "__main__":
    unittest.main()
