# -*- coding: utf-8 -*-
"""📧 Enviar lotes de Facturación de proveedor por correo — las RUTAS de app.py (Daniel, 28-oct-2026): «un envío de lotes seleccionados, con
un mensaje y los Excel y los montos en el mail, y un cuerpo bien automático».

Las funciones y rutas se extraen de app.py por nombre (tests/_fuente_app.py) y corren sobre una base y una puerta de correo de MENTIRA
(REGLA #22: jamás sale un correo de verdad). Lo que fijan:
  a) El cuerpo sale armado solo (nombre corto, monto, total, firma) y lo que la persona edita manda.
  b) Un solo correo: Para el primero, con copia el resto, un Excel adjunto por lote, reply_to del que envía, por la puerta única con su clave.
  c) Solo correos internos y nunca un técnico (REGLAS #19 y #26); un lote anulado o inexistente no se envía.
  d) Si el correo está apagado o falla, se dice la verdad y NO se anota ni se recuerda nada como enviado.
  e) El borrador `.eml` no envía nada y trae los Excel.
  f) Las tres rutas exigen sesión, no-técnico y el permiso de Facturación de proveedor.
Correr con:  py -m unittest tests.test_facprov_envio_ruta
"""
import ast
import datetime
import email
import email.policy
import io
import json
import os
import re
import unittest

from tests._fuente_app import decoradores, fuente_constante, fuente_funcion, texto

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
AHORA = datetime.datetime(2026, 10, 28, 11, 30)

FUNCS = ("_ensure_facprov_envio_reglas", "_fpe_regla", "_fpe_regla_json", "_fpe_regla_guardar", "_fpe_correos_validos",
         "_fpe_sugeridos", "_fpe_nombre_de_correo", "_fpe_lote", "_fpe_estado_envio", "_fpe_armar", "_fpe_recordar",
         "_fpe_adjuntos", "_fpe_respuesta_error", "mant_facprov_envio_previa", "mant_facprov_envio_enviar",
         "mant_facprov_envio_borrador", "_mfp_tiene_factura_real", "_mfp_excel_nombre")
CONSTS = ("_FPE_R_PARA", "_FPE_R_CC", "_FPE_R_SALUDOS", "_FPE_R_ETIQ", "_FPE_CORREO", "_FPE_TOPE_ADJUNTOS")


class _Req:
    def __init__(self):
        self.body = {}

    def get_json(self, silent=False):
        return self.body


class _G:
    user = {"id": 1, "username": "daniel.aguilar@sphs.cl", "nombre": "Daniel Aguilar", "cargo": "Gerente", "phone": "", "role": "superadmin"}


class Mundo:
    """Base, reglas y puerta de correo de mentira."""

    def __init__(self):
        self.lotes = {
            21: {"id": 21, "proveedor_nombre": "Transportes felcarm SPA", "monto_total": 4750000, "estado_pago": "pendiente",
                 "numero_documento": "F-100", "prov": "TRANSPORTES FELCARM SPA"},
            22: {"id": 22, "proveedor_nombre": "Milling", "monto_total": 2230000, "estado_pago": "pendiente",
                 "numero_documento": None, "prov": "LOGISTICA Y TRANSPORTES MILLING SPA"},
            23: {"id": 23, "proveedor_nombre": "DAP Servicio", "monto_total": 750000, "estado_pago": "pagada",
                 "numero_documento": "F-7", "prov": "DAP Servicio"},
            24: {"id": 24, "proveedor_nombre": "Anulado SPA", "monto_total": 1, "estado_pago": "anulada",
                 "numero_documento": "F-1", "prov": "Anulado SPA"},
        }
        self.users = [
            {"username": "nicolas.bastias@sphs.cl", "nombre": "Nicolás Bastías", "role": "admin", "active": 1},
            {"username": "daniel.aguilar@sphs.cl", "nombre": "Daniel Aguilar", "role": "superadmin", "active": 1},
            {"username": "jaizer@sphs.cl", "nombre": "Jaizer", "role": "tecnico_ejecutivo", "active": 1},
            {"username": "externo@gmail.com", "nombre": "Externo", "role": "admin", "active": 1},
        ]
        self.reglas = {}
        self.sql = []
        self.enviados = []
        self.logs = []
        self.envio_ok = True
        self.correo_on = True
        self.permitido = True
        self.req = _Req()
        self.excel_falla = set()

    # ── dobles de la capa de datos ──
    def fetchone(self, sql, params=None):
        params = params or ()
        if "SELECT nombre FROM app_users" in sql:
            for u in self.users:
                if u["username"].lower() == params[0]:
                    return {"nombre": u["nombre"]}
        return None

    def fetchall(self, sql, params=None):
        params = params or ()
        if "FROM app_users" in sql and " IN (" in sql:
            return [dict(u) for u in self.users if u["username"].lower() in params]
        if "FROM app_users" in sql and "LIKE" in sql:
            return [dict(u) for u in self.users if u["username"].endswith(("@sphs.cl", "@ilusfitness.com"))]
        return []

    def execute(self, sql, params=None):
        self.sql.append((sql, params))
        if "UPDATE mant_reglas_negocio" in sql:
            self.reglas[params[2]] = params[0]
        if "INSERT IGNORE INTO mant_reglas_negocio" in sql:
            self.reglas.setdefault(params[0], params[1])

    def cargar_lote(self, fid):
        return dict(self.lotes[fid]) if fid in self.lotes else None

    def enviar(self, to, asunto, html, **kw):
        self.enviados.append({"to": to, "asunto": asunto, "html": html, **kw})
        return self.envio_ok

    def excel(self, fid):
        if fid in self.excel_falla or fid not in self.lotes:
            return None
        return {"buf": io.BytesIO(b"XLSX-" + str(fid).encode() * 40), "nombre": f"lote_{fid}_x_28-10-2026.xlsx",
                "prov_nombre": self.lotes[fid]["prov"], "prov_rut": "", "factura": self.lotes[fid]}


def _amb(m):
    class _Cfg:
        pass

    def _items(fid):
        return [{"prov_ficha_id": None}] if fid != 22 else []

    ambito = {
        "re": re, "json": json, "g": _G, "request": m.req, "jsonify": lambda d: d,
        "send_file": lambda f, **kw: {"archivo": f, **kw},
        "mysql_fetchone": m.fetchone, "mysql_fetchall": m.fetchall, "mysql_execute": m.execute,
        "_reglas_cargar": lambda: dict(m.reglas), "_reglas_invalidar": lambda: None,
        "_es_rol_tecnico": lambda u=None: str((u or {}).get("role") or "").startswith("tecnico"),
        "_facprov_puede": lambda: True, "_mfp_403": lambda: ({"ok": False, "error_codigo": "SIN_PERMISO"}, 403),
        "_mfp_cargar": m.cargar_lote, "_mfp_items": _items,
        "_mfp_proveedor_canon": lambda f, items, fid: (m.lotes[fid]["prov"], "", None),
        "_mfp_excel_de_lote": m.excel, "_now_chile": lambda: AHORA,
        "comm_is_enabled": lambda canal: m.correo_on, "_modulo_canal_bloqueado": lambda mod, canal: False,
        "_correo_permitido": lambda clave: m.permitido, "_send_ilus_email": m.enviar,
        "_email_resultado": lambda ok: ({"salio": True, "mensaje": ""} if ok else
                                        {"salio": False, "mensaje": "El correo no salió: el proveedor de correo no lo aceptó"}),
        "_mant_log": lambda ent, eid, acc, det="": m.logs.append((ent, eid, acc, det)),
        "current_username": lambda: "Daniel Aguilar",
    }
    amb = dict(ambito)
    for codigo in _codigos():                 # el código de app.py se compila UNA vez; cada prueba lo ejecuta sobre sus propios dobles
        exec(codigo, amb)
    return amb


_COMPILADO = []


def _codigos():
    """Las constantes y funciones de app.py ya compiladas (sin decoradores), en el orden en que hay que ejecutarlas."""
    if not _COMPILADO:
        for c in CONSTS:
            _COMPILADO.append(compile(ast.parse(fuente_constante(c)), "<app.py:%s>" % c, "exec"))
        for f in FUNCS:
            arbol = ast.parse(fuente_funcion(f))
            for nodo in arbol.body:
                if isinstance(nodo, ast.FunctionDef):
                    nodo.decorator_list = []
            _COMPILADO.append(compile(arbol, "<app.py:%s>" % f, "exec"))
    return _COMPILADO


def _correr(m, nombre, cuerpo=None):
    e = _amb(m)
    m.req.body = cuerpo if cuerpo is not None else {}
    return e[nombre]()


class TestArmadoAutomatico(unittest.TestCase):
    def test_el_cuerpo_sale_solo_y_lo_editado_manda(self):
        m = Mundo()
        r, = [_correr(m, "mant_facprov_envio_previa", {"ids": [21, 22, 23], "para": "nicolas.bastias@sphs.cl"})]
        self.assertTrue(r["ok"])
        self.assertEqual(r["asunto"], "Cierre de proveedores — 3 lotes (28/10/2026)")
        self.assertEqual([ln["etiqueta"] for ln in r["lineas"]], ["Felcarm", "Milling", "DAP"])
        self.assertEqual(r["saludo"], "Hola Nicolás,")
        self.assertIn("1. Felcarm $4.750.000", r["cuerpo_texto"])
        self.assertIn("2. Milling $2.230.000", r["cuerpo_texto"])
        self.assertIn("Total $7.730.000", r["cuerpo_texto"])
        self.assertIn("Daniel Aguilar", r["cuerpo_texto"])
        # lo que la persona edita manda; el HTML siempre lo arma el servidor y lo escribe escapado
        r2 = _correr(m, "mant_facprov_envio_previa", {"ids": [21, 22], "para": "nicolas.bastias@sphs.cl", "saludo": "Nico,",
                                                       "intro": "Va <b>esto</b>", "lineas": [{"id": 22, "etiqueta": "Pulgar"}],
                                                       "con_total": False})
        self.assertEqual(r2["saludo"], "Nico,")
        self.assertIn("2. Pulgar $2.230.000", r2["cuerpo_texto"])
        self.assertNotIn("Total", r2["cuerpo_texto"])
        self.assertIn("Va &lt;b&gt;esto&lt;/b&gt;", r2["cuerpo_html"])
        self.assertNotIn("<b>esto</b>", r2["cuerpo_html"])

    def test_avisos_y_estado_del_envio(self):
        m = Mundo()
        m.correo_on = False
        r = _correr(m, "mant_facprov_envio_previa", {"ids": [22]})
        self.assertFalse(r["envio"]["puede"])
        self.assertIn("apagado", r["envio"]["motivo"])
        self.assertTrue(any("todavía no tiene la factura" in a for a in r["avisos"]))      # el lote 22 no tiene factura
        self.assertTrue(any("no tiene OT asignadas" in a for a in r["avisos"]))

    def test_sugeridos_sin_tecnicos_ni_externos(self):
        r = _correr(Mundo(), "mant_facprov_envio_previa", {"ids": [21]})
        correos = [s["correo"] for s in r["sugeridos"]]
        self.assertIn("nicolas.bastias@sphs.cl", correos)
        self.assertNotIn("jaizer@sphs.cl", correos)          # técnico: no recibe montos de proveedores
        self.assertNotIn("externo@gmail.com", correos)

    def test_lo_recordado_se_ofrece_la_proxima_vez(self):
        m = Mundo()
        m.reglas.update({"facprov_envio_para": "nicolas.bastias@sphs.cl", "facprov_envio_cc": "daniel.aguilar@sphs.cl",
                         "facprov_envio_saludos": json.dumps({"nicolas.bastias@sphs.cl": "Nico,"}),
                         "facprov_envio_etiquetas": json.dumps({"logistica y transportes milling spa": "Pulgar"})})
        r = _correr(m, "mant_facprov_envio_previa", {"ids": [21, 22]})
        self.assertEqual((r["para"], r["cc"], r["saludo"]), (["nicolas.bastias@sphs.cl"], ["daniel.aguilar@sphs.cl"], "Nico,"))
        self.assertEqual([ln["etiqueta"] for ln in r["lineas"]], ["Felcarm", "Pulgar"])


class TestElEnvio(unittest.TestCase):
    PEDIDO = {"ids": [21, 22, 23], "para": "nicolas.bastias@sphs.cl", "cc": "daniel.aguilar@sphs.cl",
              "saludo": "Nico,", "lineas": [{"id": 22, "etiqueta": "Pulgar"}]}

    def test_un_solo_correo_con_los_excel_y_los_montos(self):
        m = Mundo()
        r = _correr(m, "mant_facprov_envio_enviar", dict(self.PEDIDO))
        self.assertTrue(r["ok"], r)
        self.assertEqual(len(m.enviados), 1)                      # UN correo, no uno por lote
        c = m.enviados[0]
        self.assertEqual(c["to"], "nicolas.bastias@sphs.cl")
        self.assertEqual(c["cc"], ["daniel.aguilar@sphs.cl"])
        self.assertEqual(c["modulo"], "comunicacion_interna")
        self.assertEqual(c["correo"], "facprov_cierre_lotes")
        self.assertEqual(c["reply_to"], "daniel.aguilar@sphs.cl")
        self.assertEqual([a[0] for a in c["attachments"]], ["lote_21_x_28-10-2026.xlsx", "lote_22_x_28-10-2026.xlsx", "lote_23_x_28-10-2026.xlsx"])
        self.assertTrue(all(a[2].endswith("spreadsheetml.sheet") and a[1] for a in c["attachments"]))
        self.assertIn("Nico,", c["html"])
        self.assertIn("Pulgar", c["html"])
        self.assertIn("$7.730.000", c["html"])

    def test_deja_constancia_en_cada_lote_y_recuerda_para_la_proxima(self):
        m = Mundo()
        _correr(m, "mant_facprov_envio_enviar", dict(self.PEDIDO))
        self.assertEqual([lg[1] for lg in m.logs], [21, 22, 23])
        self.assertTrue(all(lg[0] == "factura_proveedor" and lg[2] == "enviado_por_correo" for lg in m.logs))
        self.assertIn("nicolas.bastias@sphs.cl", m.logs[0][3])
        self.assertEqual(m.reglas["facprov_envio_para"], "nicolas.bastias@sphs.cl")
        self.assertEqual(m.reglas["facprov_envio_cc"], "daniel.aguilar@sphs.cl")
        self.assertEqual(json.loads(m.reglas["facprov_envio_saludos"]), {"nicolas.bastias@sphs.cl": "Nico,"})
        self.assertEqual(json.loads(m.reglas["facprov_envio_etiquetas"]), {"logistica y transportes milling spa": "Pulgar"})

    def test_varios_para_el_resto_va_con_copia(self):
        m = Mundo()
        r = _correr(m, "mant_facprov_envio_enviar", {"ids": [21], "para": "nicolas.bastias@sphs.cl, daniel.aguilar@sphs.cl"})
        self.assertTrue(r["ok"])
        self.assertEqual(m.enviados[0]["to"], "nicolas.bastias@sphs.cl")
        self.assertEqual(m.enviados[0]["cc"], ["daniel.aguilar@sphs.cl"])

    def test_solo_internos_y_nunca_un_tecnico(self):
        for malo in ("externo@gmail.com", "jaizer@sphs.cl", "nicolas@sphs.cl.evil.com"):
            m = Mundo()
            r, cod = _correr(m, "mant_facprov_envio_enviar", {"ids": [21], "para": "nicolas.bastias@sphs.cl", "cc": malo})
            self.assertEqual((r["error_codigo"], cod), ("CORREO_NO_VALIDO", 400), malo)
            self.assertEqual(m.enviados, [], malo)
            self.assertEqual(m.logs, [], malo)

    def test_sin_destinatario_o_con_demasiados(self):
        m = Mundo()
        r, cod = _correr(m, "mant_facprov_envio_enviar", {"ids": [21], "para": ""})
        self.assertEqual((r["error_codigo"], cod), ("SIN_DESTINATARIO", 400))
        muchos = ", ".join(f"p{i}@sphs.cl" for i in range(11))
        r, cod = _correr(m, "mant_facprov_envio_enviar", {"ids": [21], "para": muchos})
        self.assertEqual((r["error_codigo"], cod), ("DEMASIADOS_DESTINATARIOS", 400))
        self.assertEqual(m.enviados, [])

    def test_lote_anulado_o_inexistente_no_se_envia(self):
        for ids, esperado in (([24], "anulado"), ([999], "no existe")):
            m = Mundo()
            r, cod = _correr(m, "mant_facprov_envio_enviar", {"ids": ids, "para": "nicolas.bastias@sphs.cl"})
            self.assertEqual((r["error_codigo"], cod), ("LOTE_NO_VALIDO", 404))
            self.assertIn(esperado, r["error"])
            self.assertEqual(m.enviados, [])
        r, cod = _correr(Mundo(), "mant_facprov_envio_enviar", {"ids": [], "para": "nicolas.bastias@sphs.cl"})
        self.assertEqual((r["error_codigo"], cod), ("SIN_LOTES", 400))

    def test_correo_apagado_dice_la_verdad_y_no_anota_nada(self):
        m = Mundo()
        m.correo_on = False
        r, cod = _correr(m, "mant_facprov_envio_enviar", dict(self.PEDIDO))
        self.assertEqual((r["error_codigo"], cod), ("CORREO_APAGADO", 409))
        self.assertIn("El correo no salió", r["error"])
        self.assertIn("borrador", r["error"])
        self.assertEqual((m.enviados, m.logs), ([], []))
        self.assertNotIn("facprov_envio_para", m.reglas)

    def test_si_el_proveedor_de_correo_falla_no_se_anota_como_enviado(self):
        m = Mundo()
        m.envio_ok = False
        r, cod = _correr(m, "mant_facprov_envio_enviar", dict(self.PEDIDO))
        self.assertEqual((r["error_codigo"], cod), ("NO_SALIO", 502))
        self.assertIn("El correo no salió", r["error"])
        self.assertEqual(m.logs, [])
        self.assertNotIn("facprov_envio_para", m.reglas)

    def test_un_excel_que_no_se_pudo_armar_frena_todo_antes_de_enviar(self):
        m = Mundo()
        m.excel_falla = {22}
        r, cod = _correr(m, "mant_facprov_envio_enviar", dict(self.PEDIDO))
        self.assertEqual((r["error_codigo"], cod), ("LOTE_NO_VALIDO", 404))
        self.assertEqual(m.enviados, [])

    def test_adjuntos_demasiado_pesados(self):
        m = Mundo()
        m.excel = lambda fid: {"buf": io.BytesIO(b"x" * (9 * 1024 * 1024)), "nombre": f"l{fid}.xlsx", "prov_nombre": "", "prov_rut": "", "factura": {}}
        r, cod = _correr(m, "mant_facprov_envio_enviar", {"ids": [21, 22], "para": "nicolas.bastias@sphs.cl"})
        self.assertEqual((r["error_codigo"], cod), ("ADJUNTOS_MUY_GRANDES", 400))
        self.assertEqual(m.enviados, [])


class TestElBorrador(unittest.TestCase):
    def test_trae_los_excel_y_no_envia_nada(self):
        m = Mundo()
        m.correo_on = False                                  # aunque el correo esté apagado, el borrador se puede bajar
        r = _correr(m, "mant_facprov_envio_borrador", {"ids": [21, 22], "para": "nicolas.bastias@sphs.cl", "saludo": "Nico,"})
        self.assertEqual(r["mimetype"], "message/rfc822")
        self.assertEqual(r["download_name"], "cierre_proveedores_28-10-2026.eml")
        msg = email.message_from_bytes(r["archivo"].getvalue(), policy=email.policy.default)
        self.assertEqual(msg["X-Unsent"], "1")
        self.assertEqual(msg["To"], "nicolas.bastias@sphs.cl")
        self.assertEqual(len([p for p in msg.iter_attachments()]), 2)
        self.assertIn("Nico,", msg.get_body(("plain",)).get_content())
        self.assertEqual((m.enviados, m.logs), ([], []))

    def test_tampoco_con_correos_que_no_corresponden(self):
        r, cod = _correr(Mundo(), "mant_facprov_envio_borrador", {"ids": [21], "para": "externo@gmail.com"})
        self.assertEqual((r["error_codigo"], cod), ("CORREO_NO_VALIDO", 400))


class TestSemilla(unittest.TestCase):
    def test_las_cuatro_memorias_nacen_vacias_y_no_pisan_lo_guardado(self):
        m = Mundo()
        m.reglas["facprov_envio_para"] = "nicolas.bastias@sphs.cl"
        self.assertTrue(_amb(m)["_ensure_facprov_envio_reglas"]())
        self.assertEqual(set(m.reglas), {"facprov_envio_para", "facprov_envio_cc", "facprov_envio_saludos", "facprov_envio_etiquetas"})
        self.assertEqual(m.reglas["facprov_envio_para"], "nicolas.bastias@sphs.cl")        # INSERT IGNORE: no pisa
        self.assertTrue(all("INSERT IGNORE" in s for s, _ in m.sql))


class TestCableado(unittest.TestCase):
    RUTAS = ("mant_facprov_envio_previa", "mant_facprov_envio_enviar", "mant_facprov_envio_borrador")

    def test_las_tres_rutas_cierran_a_los_tecnicos_y_piden_el_permiso(self):
        for f in self.RUTAS:
            d = decoradores(f)
            self.assertTrue(any("_mant_required" in x for x in d), f)
            self.assertTrue(any("_no_tecnico" in x for x in d), f)
            self.assertTrue(any("methods=[\"POST\"]" in x for x in d), f)
            src = fuente_funcion(f)
            self.assertIn("if not _facprov_puede():", src, f)
            self.assertLess(src.index("_facprov_puede()"), src.index("_fpe_armar("), f)

    def test_el_correo_sale_por_la_puerta_unica_con_su_clave_registrada(self):
        src = fuente_funcion("mant_facprov_envio_enviar")
        self.assertIn("_send_ilus_email(", src)
        self.assertIn('correo="facprov_cierre_lotes"', src)          # literal: la prueba de cobertura de correos lo exige
        self.assertIn('modulo="comunicacion_interna"', src)
        import comm_correos_registro as ccr
        c = ccr.por_clave("facprov_cierre_lotes")
        self.assertIsNotNone(c)
        self.assertEqual((c["llave"], c["destinatario"], c["modo"], c["tipo"]), ("comunicacion_interna", "interno", "manual", "ilus"))
        self.assertFalse(c["protegido"])
        self.assertIn('_FPE_CORREO = "facprov_cierre_lotes"', texto("app.py"))

    def test_el_historial_del_lote_entiende_la_accion_nueva(self):
        self.assertIn('"enviado_por_correo": "Enviado por correo"', texto("app.py"))

    def test_la_semilla_corre_al_arrancar(self):
        self.assertIn("_ensure_facprov_envio_reglas()", texto("app.py").split("def _ensure_acta_recepcion_regla")[1])

    def test_la_pantalla_trae_casillas_barra_modal_y_scripts(self):
        with open(os.path.join(RAIZ, "templates", "mantenciones", "facturas_proveedor.html"), encoding="utf-8") as fh:
            t = fh.read()
        for ancla in ('class="fpv-env-chk"', 'id="fpvEnvTodo"', 'id="fpvEnvBar"', 'id="fpvEnvAbrir"', 'id="fpeModal"',
                      "url_for('mant_facprov_envio_previa')", "url_for('mant_facprov_envio_enviar')",
                      "url_for('mant_facprov_envio_borrador')", "facprov_envio.js", "facprov_envio.css"):
            self.assertIn(ancla, t, ancla)
        # un lote anulado no se puede marcar
        self.assertRegex(t, r"\{% if f\.estado_pago != 'anulada' %\}\s*<label class=\"fpv-env\"")
        # REGLA #1: nada de alert/confirm/prompt nativos en lo nuevo
        with open(os.path.join(RAIZ, "static", "facprov_envio.js"), encoding="utf-8") as fh:
            js = fh.read()
        self.assertIsNone(re.search(r"(?<![A-Za-z.])(alert|confirm|prompt)\(", js))


if __name__ == "__main__":
    unittest.main()
