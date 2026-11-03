# -*- coding: utf-8 -*-
"""3-nov-2026 · Anexo de Servicios: «Lugar del Servicio» y nueva redacción de Hitos de Pago y Alcance (pedido de Félix, vía Daniel).

Daniel: «en el anexo de servicios necesitamos que indique dónde se realizará el servicio. Es importante dejar la dirección del lugar en el anexo»,
y los textos nuevos de «Hitos de Pago» y «Alcance del Servicio». Se vigila:
  · el módulo puro (anexo_lugar): arma «Dirección, Comuna», no inventa un lugar y un anexo FIRMADO sin lugar guardado no muestra uno nuevo;
  · el texto canónico que se hashea al firmar queda IDÉNTICO cuando el anexo no tiene lugar (nada de lo ya emitido cambia);
  · los textos nuevos solo rigen para anexos nuevos, y un borrador viejo en garantía sigue recibiendo el texto de garantía;
  · el lugar se guarda aparte del INSERT (sin la columna el anexo se crea igual), por los tres caminos que crean un anexo, y se congela al firmar;
  · el PDF y la pantalla de firma lo dibujan (la huella del navegador lo cubre), y el modal lo exige.
Correr con:  py -m unittest tests.test_anexo_lugar_servicio
"""
import os
import re
import unittest

from jinja2 import Environment, FileSystemLoader

import anexo_lugar as axl

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(rel):
    with open(os.path.join(RAIZ, rel), encoding="utf-8", newline="") as f:
        return f.read().replace("\r\n", "\n")


APP = _leer("app.py")


def _tramo(desde, hasta):
    i = APP.index(desde)
    return APP[i:APP.index(hasta, i)]


def _ejecutar(*tramos):
    ns = {}
    exec(compile("\n".join(tramos), "anexo_aislado", "exec"), ns)
    return ns


class TestComponerLugar(unittest.TestCase):
    def test_agrega_la_comuna_solo_si_la_direccion_no_la_trae(self):
        self.assertEqual(axl.componer_lugar("1 Norte 4745, Talca, Maule", "Talca"), "1 Norte 4745, Talca, Maule")
        self.assertEqual(axl.componer_lugar("Av. Providencia 1234", "Providencia"), "Av. Providencia 1234")
        self.assertEqual(axl.componer_lugar("Los Leones 55", "Ñuñoa"), "Los Leones 55, Ñuñoa")

    def test_sin_direccion_no_hay_lugar_ni_se_inventa_con_la_comuna(self):
        self.assertEqual(axl.componer_lugar("", "Talca"), "")
        self.assertEqual(axl.componer_lugar(None, None), "")
        self.assertEqual(axl.componer_lugar("   ", "Talca"), "")

    def test_detalle_entre_parentesis_y_sin_repetirse(self):
        self.assertEqual(axl.componer_lugar("Los Leones 55", "Ñuñoa", "Piso 3, local 2"), "Los Leones 55, Ñuñoa (Piso 3, local 2)")
        self.assertEqual(axl.componer_lugar("Los Leones 55 piso 3", "", "piso 3"), "Los Leones 55 piso 3")

    def test_limpia_espacios_y_pone_tope(self):
        self.assertEqual(axl.componer_lugar("  Calle   Uno   10 ", " Talca "), "Calle Uno 10, Talca")
        self.assertEqual(len(axl.componer_lugar("x" * 900)), axl.MAX_LARGO)


class TestLugarDeLaOT(unittest.TestCase):
    def test_manda_la_direccion_propia_de_la_ot_con_su_comuna(self):
        f = {"direccion_visita": "Sede Norte 100", "direccion_comuna": "Colina", "cliente_direccion": "Casa Matriz 5", "cliente_comuna": "Colchane"}
        self.assertEqual(axl.lugar_de_ot(f), "Sede Norte 100, Colina")

    def test_la_comuna_del_cliente_nunca_acompana_a_una_direccion_propia(self):
        """Caso OT-2026-00171: visita en Colina, ficha del cliente en Colchane, a 1.900 km."""
        f = {"direccion_visita": "Sede Norte 100", "direccion_comuna": "", "cliente_direccion": "Casa Matriz 5", "cliente_comuna": "Colchane"}
        self.assertEqual(axl.lugar_de_ot(f), "Sede Norte 100")

    def test_sin_direccion_propia_usa_la_del_cliente(self):
        f = {"direccion_visita": "", "cliente_direccion": "1 Norte 4745", "cliente_comuna": "Talca"}
        self.assertEqual(axl.lugar_de_ot(f), "1 Norte 4745, Talca")

    def test_sin_nada_o_sin_fila(self):
        self.assertEqual(axl.lugar_de_ot({}), "")
        self.assertEqual(axl.lugar_de_ot(None), "")


class TestLugarDelAnexo(unittest.TestCase):
    def test_el_guardado_manda_siempre(self):
        self.assertEqual(axl.lugar_del_anexo("Dirección guardada 1", "firmado", "2026-11-01", "Otra 2"), "Dirección guardada 1")
        self.assertEqual(axl.lugar_del_anexo("Dirección guardada 1", "enviado", None, "Otra 2"), "Dirección guardada 1")

    def test_un_borrador_sin_lugar_toma_el_de_la_ot(self):
        self.assertEqual(axl.lugar_del_anexo("", "borrador", None, "Dirección OT 3"), "Dirección OT 3")
        self.assertEqual(axl.lugar_del_anexo(None, "enviado", None, "Dirección OT 3"), "Dirección OT 3")

    def test_un_anexo_firmado_sin_lugar_no_muestra_uno_inventado(self):
        """Un documento firmado tiene que decir lo que decía cuando se firmó."""
        self.assertEqual(axl.lugar_del_anexo("", "firmado", "2026-11-01 10:00", "Dirección OT 3"), "")
        self.assertEqual(axl.lugar_del_anexo(None, "visto", "2026-11-01 10:00", "Dirección OT 3"), "")


class TestTextoCanonico(unittest.TestCase):
    """`_anexo_render_canonico` es la base del hash al firmar: lo ya emitido no puede cambiar."""

    @classmethod
    def setUpClass(cls):
        cls.f = staticmethod(_ejecutar(_tramo("def _anexo_render_canonico(a):", "def _anexo_hash(a):"))["_anexo_render_canonico"])

    def _anexo(self, **extra):
        a = {"numero": 245, "proveedor_nombre": "Prov SPA", "proveedor_rut": "78065371K", "cliente_nombre": "Cliente",
             "objetivo_servicio": "Ejecutar servicio.", "precio_items": [{"concepto": "Instalación", "monto": 300000}],
             "fecha_inicio": "2026-11-05", "fecha_termino": "2026-11-05", "niveles_servicio": "N", "hitos_pago": "H", "alcance_servicio": "A"}
        a.update(extra)
        return a

    def test_sin_lugar_queda_identico_al_formato_de_siempre(self):
        esperado = ("ANEXO DE SERVICIOS N° 245\nProveedor: Prov SPA (RUT 78065371K)\nCliente: Cliente\nObjetivo del Servicio: Ejecutar servicio.\n"
                    "Precio: $300.000 (Instalación) Neto.\nDuración del Servicio: Inicio 2026-11-05 / Término 2026-11-05\nNiveles de Servicio: N\n"
                    "Hitos de Pago: H\nAlcance del Servicio: A\n")
        self.assertEqual(self.f(self._anexo()), esperado)
        self.assertEqual(self.f(self._anexo(lugar_servicio="")), esperado)
        self.assertEqual(self.f(self._anexo(lugar_servicio="   ")), esperado)

    def test_con_lugar_va_justo_despues_del_objetivo(self):
        t = self.f(self._anexo(lugar_servicio="1 Norte 4745, Talca, Maule"))
        self.assertIn("Objetivo del Servicio: Ejecutar servicio.\nLugar del Servicio: 1 Norte 4745, Talca, Maule\nPrecio:", t)


class TestTextosNuevos(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ns = _ejecutar(_tramo("def _anexo_clausulas_defecto():", "# 2026-10-02 (Daniel, Anexo N°203"),
                       _tramo("_ANEXO_HITOS_GARANTIA_SIN_PAGO = (", "def _anexo_ot_en_garantia(vid):"),
                       _tramo("def _anexo_hitos_para(items, hitos_in, es_garantia):", "def _anexo_clausulas_pendientes():"))
        cls.ns = ns
        cls.d = ns["_anexo_clausulas_defecto"]()

    def test_hitos_de_pago_nuevo(self):
        h = self.d["hitos_pago"]
        self.assertIn("firmada por el cliente", h)
        self.assertIn("período de pago del mes", h)
        self.assertIn("pendientes", h)
        self.assertNotIn("se emitirá documentación", h, "el texto de antes ya no rige para anexos nuevos")

    def test_alcance_nuevo_con_garantia_de_ejecucion_de_6_meses(self):
        a = self.d["alcance_servicio"]
        self.assertIn("todos los equipos declarados", a)
        self.assertIn("garantizando un servicio de calidad", a)
        self.assertIn("garantía de ejecución de 6 meses", a)
        self.assertNotIn("penalizaciones", a)

    def test_no_se_agregan_plazos_ni_montos_que_nadie_pidio(self):
        for campo in ("hitos_pago", "alcance_servicio"):
            self.assertNotRegex(self.d[campo], r"\$|\d+ días|penalidad", campo)

    def test_un_borrador_viejo_en_garantia_sin_pago_sigue_recibiendo_el_texto_de_garantia(self):
        para = self.ns["_anexo_hitos_para"]
        sin_pago = [{"concepto": "Servicio en garantía — sin pago al proveedor", "monto": 0}]
        garantia = self.ns["_ANEXO_HITOS_GARANTIA_SIN_PAGO"]
        viejo = "Una vez realizados los servicios, se debe llenar la OT con la respectiva firma del cliente, posteriormente, se emitirá documentación para el pago del servicio."
        self.assertEqual(para(sin_pago, viejo, True), garantia)
        self.assertEqual(para(sin_pago, self.d["hitos_pago"], True), garantia)
        self.assertEqual(para(sin_pago, "", True), garantia)

    def test_un_texto_escrito_a_mano_se_respeta_y_sin_garantia_no_cambia(self):
        para = self.ns["_anexo_hitos_para"]
        con_pago = [{"concepto": "Instalación", "monto": 90000}]
        self.assertEqual(para(con_pago, "Pago contra entrega", False), "Pago contra entrega")
        self.assertEqual(para([{"concepto": "x", "monto": 0}], "Texto propio", True), "Texto propio")
        self.assertEqual(para(con_pago, self.d["hitos_pago"], False), self.d["hitos_pago"])


class TestPersistencia(unittest.TestCase):
    def test_la_columna_se_crea_en_el_arranque_con_su_ensure(self):
        ens = _tramo("def _ensure_mant_anexos():", "def _next_anexo_numero_atomic")
        self.assertIn('("lugar_servicio", "ALTER TABLE mant_anexos ADD COLUMN lugar_servicio VARCHAR(500) NULL', ens)

    def test_el_insert_del_anexo_no_depende_de_la_columna_nueva(self):
        """Si el ALTER fallara, crear anexos tiene que seguir funcionando: el lugar se guarda en un UPDATE aparte, best-effort."""
        crear = _tramo('@app.route("/ot/api/anexos", methods=["POST"])', '@app.route("/ot/api/anexos/<int:aid>", methods=["GET"])')
        i = crear.index("INSERT INTO mant_anexos")
        self.assertNotIn("lugar_servicio", crear[i:crear.index("aid = cur.lastrowid", i)])
        self.assertIn("_anexo_guardar_lugar(aid, _axl.componer_lugar(d.get(\"lugar_servicio\")) or _anexo_lugar_ot(vid))", crear)
        helper = _tramo("def _anexo_guardar_lugar(", "def _anexo_dict(row):")
        self.assertIn("except Exception", helper)

    def test_el_anexo_automatico_del_asistente_tambien_guarda_el_lugar(self):
        self.assertIn("_anexo_guardar_lugar(_aid_auto, _axl.componer_lugar(_cp_dir, _cp_comuna, _cp_detalle) or _anexo_lugar_ot(vid))", APP)

    def test_corregir_un_borrador_actualiza_el_lugar_solo_si_el_formulario_lo_trae(self):
        edit = _tramo("def ot2_api_anexo_editar(aid):", "_ANEXO_ERR_MSG = {")
        self.assertIn('if "lugar_servicio" in d:', edit)
        self.assertIn("_anexo_guardar_lugar(aid, _axl.componer_lugar(d.get(\"lugar_servicio\")))", edit)

    def test_al_firmar_el_lugar_visto_queda_congelado_antes_de_archivar(self):
        sub = _tramo("def ot2_anexo_firma_submit(token):", "# 📧 2026-08-31 (Daniel, urgente")
        i_congela = sub.index('_anexo_guardar_lugar(a["id"], a_full.get("lugar_servicio"), solo_si_vacio=True)')
        self.assertGreater(i_congela, sub.index("mysql_execute_returning_rowcount("), "después de que la firma se guardó")
        self.assertLess(i_congela, sub.index("_anexo_archivar_en_ot("), "antes de archivar el PDF firmado")

    def test_los_dos_lectores_ven_lo_mismo(self):
        """El payload (PDF y pantalla de firma), el hash y el formulario de corrección salen de `_anexo_dict`."""
        d = _tramo("def _anexo_dict(row):", "@app.route(\"/ot/<int:vid>/anexos\", methods=[\"GET\"])")
        self.assertIn("_axl.lugar_del_anexo(", d)
        pay = _tramo("def _anexo_publico_payload(a):", "def _anexo_precio_texto(items):")
        self.assertIn('"lugar_servicio": a.get("lugar_servicio") or ""', pay)
        det = _tramo("def ot2_api_anexo_detalle(aid):", '@app.route("/ot/api/anexos/<int:aid>", methods=["PATCH", "POST"])')
        self.assertIn('"lugar_servicio": a.get("lugar_servicio") or ""', det)

    def test_la_consulta_a_la_ot_usa_columnas_que_existen(self):
        """Lección del proyecto: toda columna pedida existe en su CREATE TABLE."""
        q = _tramo("def _anexo_lugar_ot(vid):", "def _anexo_guardar_lugar(")
        for col in ("direccion_visita", "direccion_comuna", "direccion_detalle"):
            self.assertIn(col, q)
            self.assertRegex(APP, r"%s\s+(VARCHAR|TEXT)" % col, f"{col} tiene su ALTER/CREATE en app.py")
        i = APP.index("CREATE TABLE IF NOT EXISTS mant_clientes (")
        tabla = APP[i:APP.index(") ENGINE", i)]
        for col in ("direccion", "comuna"):
            self.assertRegex(tabla, r"\n\s+%s\s" % col)


class TestDocumentos(unittest.TestCase):
    def _env(self):
        env = Environment(loader=FileSystemLoader(os.path.join(RAIZ, "templates")), autoescape=True)
        env.filters["rut_fmt"] = lambda v: v   # el filtro real vive en app.py (REGLA #7); aquí no se importa la aplicación
        return env

    def _anexo(self, **extra):
        a = dict(numero=999, fecha="03/11/2026", proveedor_nombre="Proveedor QA", proveedor_rut="11.111.111-1",
                 objetivo_servicio="Ejecutar servicio de instalación al cliente QA.", cliente_nombre="Cliente QA",
                 precio_items=[dict(concepto="Instalación", monto=90000)], fecha_inicio="05/11/2026", fecha_termino="05/11/2026",
                 niveles_servicio="Niveles QA", hitos_pago="Hitos QA", alcance_servicio="Alcance QA", clausulas_adicionales="")
        a.update(extra)
        return a

    def test_el_pdf_dice_el_lugar_entre_el_objetivo_y_la_duracion(self):
        env = self._env()
        html = env.get_template("ot2/anexo_documento.html").render(
            anexo=self._anexo(lugar_servicio="1 Norte 4745, Talca, Maule"), firma=None, productos=[], es_vista_previa=False)
        i_obj, i_lug, i_dur = html.index("Objetivo del Servicio:"), html.index("Lugar del Servicio:"), html.index("Duración del Servicio:")
        self.assertTrue(i_obj < i_lug < i_dur)
        self.assertIn("1 Norte 4745, Talca, Maule", html)

    def test_sin_lugar_el_pdf_no_dibuja_la_linea(self):
        html = self._env().get_template("ot2/anexo_documento.html").render(
            anexo=self._anexo(lugar_servicio=""), firma=None, productos=[], es_vista_previa=False)
        self.assertNotIn("Lugar del Servicio", html)

    def test_el_lugar_se_escapa(self):
        html = self._env().get_template("ot2/anexo_documento.html").render(
            anexo=self._anexo(lugar_servicio="Calle <b>1</b>"), firma=None, productos=[], es_vista_previa=False)
        self.assertIn("Calle &lt;b&gt;1&lt;/b&gt;", html)

    def test_la_pantalla_de_firma_lo_dibuja_dentro_del_texto_que_se_hashea(self):
        firma = _leer("templates/ot2/anexo_firma.html")
        ini = firma.index('id="docLegal"')
        i_lug = firma.index('class="lugar-servicio"')
        self.assertTrue(firma.index("Primero", ini) < i_lug < firma.index("Segundo", ini), "dentro de #docLegal, en la cláusula del objetivo")
        self.assertIn("{% if anexo.lugar_servicio %}", firma[i_lug - 200:i_lug])

    def test_el_modal_lo_pide_lo_propone_desde_la_ot_y_lo_manda(self):
        m = _leer("templates/ot2/_modal_anexo.html")
        self.assertIn('id="oaxLugar"', m)
        self.assertIn("lugar_servicio: (DEF_OT && DEF_OT.lugar) || ''", m)
        self.assertIn("S.lugar_servicio = a.lugar_servicio || (DEF_OT && DEF_OT.lugar) || ''", m)
        self.assertIn("lugar_servicio: (S.lugar_servicio || '').trim()", m)
        self.assertIn("Falta el lugar del servicio", m)
        self.assertIn("if (DEF_OT && DEF_OT.lugar) t += ', en ' + DEF_OT.lugar;", m, "el objetivo ya no repite la comuna")
        self.assertNotIn("DEF_OT.comuna ? ', ' + DEF_OT.comuna", m)
        self.assertRegex(m, r"setObjetivo, setLugar, setFecha")
        self.assertIn('"lugar": _axl.lugar_de_ot(v),', APP)

    def test_la_vista_previa_del_asistente_manda_el_lugar_y_el_servidor_lo_imprime(self):
        crear = _leer("templates/ot2/_modal_crear.html")
        i = crear.index("fetch('/ot/api/anexos/preview-pdf'")
        self.assertIn("lugar_servicio:", crear[i:i + 1800])
        prev = _tramo("def ot2_api_anexo_preview_pdf():", "_ANEXO_ERR_MSG = {")
        self.assertIn('"lugar_servicio": _axl.componer_lugar(d.get("lugar_servicio"))', prev)


if __name__ == "__main__":
    unittest.main()
