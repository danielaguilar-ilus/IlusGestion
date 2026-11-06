# -*- coding: utf-8 -*-
"""Pestaña «Comunicaciones» de la ficha del cliente (5-nov-2026, Daniel: «todo se centraliza en la ficha del cliente»).

Dos capas, ambas sin tocar la base ni mandar correos (REGLA #22):

  · BACKEND: `GET /mantenciones/api/clientes/<id>/comunicaciones` se ejecuta de verdad (las funciones reales de app.py, extraídas con
    `_fuente_codigo` para no parsear los 9 MB del archivo) contra una base de mentira que solo anota el SQL y los parámetros. Se verifica
    el SQL (solo SELECT, un `%s` por parámetro, nada concatenado), el filtro por OT/estado, el paginado, los destinatarios, el estado de
    los avisos y que un técnico no entra.
  · FRONTEND: las funciones de render de `static/mantenciones_ficha.js` se ejecutan con Node en un DOM mínimo.

Se corre SOLA (`python -m pytest tests/test_clientes_comunicaciones.py`): nunca el pytest completo del proyecto.
"""
import os
import re
import shutil
import subprocess
import unittest

from flask import Flask, g

import comm_correos_registro as CCR
import ot_comunicacion as OCOM
from tests import _fuente_codigo as FC

FUNCS = ("_cli_com_ots", "_cli_com_destinatarios", "_cli_com_estado_avisos", "_cli_com_ramas", "mant_cliente_comunicaciones")
CONSTS = ("_CLI_COM_PER_PAGE", "_CLI_COM_ESTADOS", "_CLI_COM_AVISOS")

OTS = [
    {"id": 30, "numero_ot": "OT-2026-00030", "titulo": "Mantención trotadoras", "estado": "pendiente", "fecha_programada": None,
     "contacto_nombre": "Carla Pérez", "contacto_email": "Carla@Gimnasio.cl"},
    {"id": 20, "numero_ot": "OT-2026-00020", "titulo": "Instalación", "estado": "cerrada", "fecha_programada": None,
     "contacto_nombre": "Luis", "contacto_email": "luis@gimnasio.cl"},
    {"id": 10, "numero_ot": None, "titulo": "Revisión", "estado": "en_curso", "fecha_programada": None,
     "contacto_nombre": "", "contacto_email": "esto-no-es-un-correo"},
]
FICHA = {"contacto_nombre": "Ana Soto", "contacto_email": "ana@gimnasio.cl", "contacto2_nombre": "Beto",
         "contacto2_email": "ANA@gimnasio.cl", "email_empresa": "contacto@gimnasio.cl"}


class Mundo:
    """La base de mentira: anota cada consulta y contesta con lo mínimo."""

    def __init__(self, *, conteos=None, filas=None, falla=False, superadmin=False, bloqueos=None, ots=None, ficha=None):
        self.sql = []
        self.conteos = conteos if conteos is not None else {"enviado": 5, "bloqueado": 2, "fallido": 1}
        self.filas = filas if filas is not None else []
        self.falla, self.superadmin = falla, superadmin
        self.bloqueos = bloqueos or {}
        self.ots = OTS if ots is None else ots
        self.ficha = FICHA if ficha is None else ficha
        self.app = Flask(__name__)
        a = self.amb = {
            "_ocom": OCOM, "_ccr": CCR, "request": None, "jsonify": None, "g": g, "print": lambda *x, **k: None,
            "mysql_fetchone": self._uno, "mysql_fetchall": self._varios,
            "_comm_hist_proyecciones": lambda: {"email_log": ("e", "SELECT 'email_log' AS src, e.id AS id, e.estado AS estado FROM email_log e")},
            "_comm_hist_serializar": self._serializar,
            "_correo_bloqueo_previo": lambda llave, clave=None: self.bloqueos.get(clave),
            "_email_motivo_bloqueo_txt": lambda tipo, llave, clave=None: f"frenado por {tipo}",
        }
        import flask
        a["request"], a["jsonify"] = flask.request, flask.jsonify
        FC.extraer(a, "app.py", FUNCS, CONSTS)
        self.app.add_url_rule("/c/<int:cid>", "com", a["mant_cliente_comunicaciones"])

    def _uno(self, sql, params=()):
        self.sql.append((sql, tuple(params)))
        if sql.startswith("SELECT id FROM mant_clientes"):
            return {"id": params[0]} if int(params[0]) == 5 else None
        if "FROM mant_clientes" in sql:
            return dict(self.ficha)
        return None

    def _varios(self, sql, params=()):
        self.sql.append((sql, tuple(params)))
        if self.falla and "email_log" in sql:
            raise RuntimeError("secreto interno: password=XYZ")
        if "GROUP BY t.estado" in sql:
            return [{"estado": k, "n": v} for k, v in self.conteos.items()]
        if sql.startswith("SELECT * FROM ("):
            return [dict(f) for f in self.filas]
        if sql.startswith("SELECT id, numero_ot"):
            return [dict(o) for o in self.ots]
        return []

    @staticmethod
    def _serializar(filas):
        out = []
        for r in filas:
            out.append({"id": r["id"], "fecha": "05/11/2026", "hora": "21:37:10", "estado": r.get("estado", "enviado"), "motivo": r.get("motivo", ""),
                        "correo_nombre": r.get("correo_nombre", ""), "asunto": r.get("asunto", "Asunto"), "destinatario": r.get("destinatario", "x@y.cl"),
                        "origen": "automatico", "enviado_por": r.get("enviado_por", ""), "via": "", "ref": r.get("ref"),
                        "adjuntos": r.get("adjuntos", []), "cuerpo": r.get("cuerpo", "sin_copia")})
        return out

    def get(self, cid=5, qs=""):
        with self.app.test_request_context(f"/c/{cid}{qs}"):
            g.permissions = {"superadmin": True} if self.superadmin else {}
            resp = self.amb["mant_cliente_comunicaciones"](cid)
            if isinstance(resp, tuple):
                return resp[1], resp[0].get_json()
            return resp.status_code, resp.get_json()

    def consultas_email_log(self):
        return [(s, p) for s, p in self.sql if "email_log" in s]


class TestEndpoint(unittest.TestCase):
    def test_cliente_que_no_existe_da_404(self):
        st, d = Mundo().get(cid=999)
        self.assertEqual(st, 404)
        self.assertFalse(d["ok"])

    def test_solo_lee_y_cada_porcentaje_s_tiene_su_parametro(self):
        m = Mundo(filas=[{"id": 1}])
        st, _ = m.get(qs="?estado=enviado&ot=30&page=1&per_page=10")
        self.assertEqual(st, 200)
        self.assertTrue(m.sql)
        for sql, params in m.sql:
            self.assertTrue(sql.lstrip().upper().startswith(("SELECT", "(SELECT")), sql[:60])
            self.assertEqual(len(re.findall(r"%s", sql)), len(params), f"parámetros descuadrados en: {sql[:140]}")
            for prohibido in ("INSERT ", "UPDATE ", "DELETE ", "ALTER ", "DROP ", "CREATE ", "TRUNCATE "):
                self.assertNotIn(prohibido, sql.upper())
        # y el código de la ruta no sabe escribir
        fuente = FC.funcion_top("app.py", "mant_cliente_comunicaciones")
        for prohibido in ("mysql_execute", "get_mysql", "commit(", ".execute("):
            self.assertNotIn(prohibido, fuente)

    def test_lo_que_viene_en_la_url_nunca_se_concatena_al_sql(self):
        m = Mundo()
        m.get(qs="?estado=enviado'%20OR%201=1--&ot=30;DROP%20TABLE%20x&page=abc&per_page=9999")
        for sql, _ in m.consultas_email_log():
            self.assertNotIn("DROP", sql.upper())
            self.assertNotIn("OR 1=1", sql.upper())
            self.assertNotIn("9999", sql)

    def test_filtros_invalidos_vuelven_a_sus_valores_por_omision(self):
        m = Mundo()
        _, d = m.get(qs="?estado=cualquiera&per_page=7&page=-3&ot=basura")
        self.assertEqual((d["filtros"]["estado"], d["filtros"]["ot"], d["per_page"], d["page"]), ("", 0, 20, 1))

    def test_una_ot_de_otro_cliente_no_filtra_nada_ajeno(self):
        m = Mundo()
        _, d = m.get(qs="?ot=777")
        self.assertEqual(d["filtros"]["ot"], 0)
        for sql, params in m.consultas_email_log():
            self.assertNotIn(777, params)

    def test_filtrar_por_una_ot_propia_mira_solo_esa_ot(self):
        m = Mundo()
        _, d = m.get(qs="?ot=30")
        self.assertEqual(d["filtros"]["ot"], 30)
        con_ot = [(s, p) for s, p in m.consultas_email_log() if "e.ref_id=%s" in s]
        self.assertTrue(con_ot)
        for s, p in m.consultas_email_log():
            self.assertNotIn("e.destinatario IN", s, "filtrando por OT no debe mezclar lo enviado a los correos del cliente")
            self.assertIn(30, p)

    def test_sin_filtro_mira_las_dos_ramas_la_de_las_ot_y_la_de_los_correos(self):
        m = Mundo()
        m.get()
        sqls = " ".join(s for s, _ in m.consultas_email_log())
        self.assertIn("e.ref_tipo='OT'", sqls)
        self.assertIn("SELECT id FROM mant_visitas WHERE cliente_id=%s", sqls)
        self.assertIn("e.destinatario IN (", sqls)
        # los correos de prueba no cuentan como comunicación
        self.assertIn("NOT LIKE %s", sqls)
        self.assertTrue(any("prueba\\_%" in p for _, ps in m.consultas_email_log() for p in ps if isinstance(p, str)))
        # y los correos que se buscan son los del cliente y los de sus OT (sin repetir, en minúsculas, solo válidos)
        for s, ps in m.consultas_email_log():
            if "e.destinatario IN" not in s:
                continue
            en = [p for p in ps if isinstance(p, str) and "@" in p]
            self.assertIn("carla@gimnasio.cl", en)
            self.assertIn("ana@gimnasio.cl", en)
            self.assertNotIn("esto-no-es-un-correo", en)
            self.assertEqual(len(en), len(set(en)), "un correo repetido en el IN")

    def test_conteos_total_y_paginas(self):
        _, d = Mundo(conteos={"enviado": 25, "bloqueado": 3, "fallido": 2}).get(qs="?per_page=10")
        self.assertEqual(d["conteos"], {"enviado": 25, "fallido": 2, "bloqueado": 3})
        self.assertEqual((d["total"], d["pages"], d["per_page"]), (30, 3, 10))

    def test_con_filtro_de_estado_el_total_es_el_de_ese_estado(self):
        _, d = Mundo(conteos={"enviado": 25, "bloqueado": 3, "fallido": 2}).get(qs="?estado=bloqueado")
        self.assertEqual((d["total"], d["pages"]), (3, 1))
        self.assertEqual(d["conteos"]["enviado"], 25, "los botones de arriba no cambian al filtrar")

    def test_una_pagina_que_no_existe_se_corrige_a_la_ultima(self):
        m = Mundo(conteos={"enviado": 25, "bloqueado": 0, "fallido": 0}, filas=[{"id": 1}])
        _, d = m.get(qs="?page=99&per_page=10")
        self.assertEqual(d["page"], 3)
        self.assertEqual((d["desde_n"], d["hasta_n"]), (21, 21))
        # la página 3 de 10 en 10 salta 20 filas
        pag = [p for s, p in m.consultas_email_log() if "SELECT * FROM (" in s][0]
        self.assertEqual(pag[-2:], (10, 20))

    def test_sin_correos_no_hay_filas_ni_consulta_de_pagina(self):
        m = Mundo(conteos={})
        _, d = m.get()
        self.assertEqual((d["total"], d["filas"], d["desde_n"], d["hasta_n"], d["pages"]), (0, [], 0, 0, 1))
        self.assertFalse([1 for s, _ in m.sql if "SELECT * FROM (" in s])

    def test_cada_fila_trae_lo_que_la_pantalla_necesita(self):
        fila = {"id": 7, "estado": "bloqueado", "motivo": "Llave de paso de Servicio Técnico", "correo_nombre": "OT creada: aviso de la visita",
                "destinatario": "ana@gimnasio.cl", "enviado_por": "Víctor Soto", "adjuntos": [{"nombre": "visita.ics"}, "otro.pdf", None],
                "ref": {"tipo": "OT", "id": 30, "codigo": "OT-2026-00030"}, "cuerpo": "disponible"}
        _, d = Mundo(filas=[fila]).get()
        f = d["filas"][0]
        self.assertEqual((f["estado"], f["motivo"], f["destinatario"], f["enviado_por"]),
                         ("bloqueado", "Llave de paso de Servicio Técnico", "ana@gimnasio.cl", "Víctor Soto"))
        self.assertEqual(f["ot"], {"id": 30, "numero": "OT-2026-00030", "url": "/ot/30"})
        self.assertEqual(f["adjuntos"], ["visita.ics", "otro.pdf", ""])

    def test_el_enlace_a_la_ot_usa_el_numero_de_la_ficha_y_si_no_tiene_uno_de_respaldo(self):
        fila = {"id": 8, "ref": {"tipo": "OT", "id": 10, "codigo": None}}
        _, d = Mundo(filas=[fila]).get()
        self.assertEqual(d["filas"][0]["ot"]["numero"], "V-00010")

    def test_un_correo_que_no_es_de_una_ot_no_lleva_enlace(self):
        _, d = Mundo(filas=[{"id": 9, "ref": {"tipo": "COT", "id": 3, "codigo": "COT-3"}}]).get()
        self.assertIsNone(d["filas"][0]["ot"])

    def test_ver_correo_solo_para_el_superadmin_y_si_hay_copia(self):
        fila = {"id": 7, "cuerpo": "disponible"}
        self.assertFalse(Mundo(filas=[fila], superadmin=False).get()[1]["filas"][0]["puede_ver"])
        self.assertTrue(Mundo(filas=[fila], superadmin=True).get()[1]["filas"][0]["puede_ver"])
        self.assertFalse(Mundo(filas=[{"id": 7, "cuerpo": "privado"}], superadmin=True).get()[1]["filas"][0]["puede_ver"])

    def test_destinatarios_sin_repetidos_ni_correos_invalidos_ni_ot_terminadas(self):
        _, d = Mundo().get()
        fich = d["destinatarios"]["ficha"]
        self.assertEqual([f["correo"] for f in fich], ["ana@gimnasio.cl", "contacto@gimnasio.cl"],
                         "ANA@ y ana@ son el mismo buzón: sale una vez, y en minúsculas")
        self.assertEqual(fich[0]["rol"], "Contacto principal")
        cont = d["destinatarios"]["contrapartes"]
        self.assertEqual([c["correo"] for c in cont], ["carla@gimnasio.cl"], "la OT cerrada y la del correo inválido no son contraparte")
        self.assertEqual(cont[0]["ot"], "OT-2026-00030")
        self.assertFalse(cont[0]["tambien_en_ficha"])

    def test_cliente_sin_ningun_correo_lo_dice_con_listas_vacias(self):
        _, d = Mundo(ots=[], ficha={"contacto_email": "", "email_empresa": None}).get()
        self.assertEqual((d["destinatarios"]["ficha"], d["destinatarios"]["contrapartes"]), ([], []))

    def test_los_avisos_dicen_si_salen_y_por_que_no(self):
        m = Mundo(bloqueos={"ot_creada_cliente": "llave"})
        _, d = m.get()
        por_clave = {a["clave"]: a for a in d["avisos"]}
        self.assertEqual(set(por_clave), {"ot_creada_cliente", "ot_recordatorio_cliente", "ot_terminada_cliente", "ot_firma_remota"})
        self.assertFalse(por_clave["ot_creada_cliente"]["sale"])
        self.assertIn("frenado", por_clave["ot_creada_cliente"]["motivo"])
        self.assertTrue(por_clave["ot_firma_remota"]["sale"])
        self.assertEqual(por_clave["ot_firma_remota"]["motivo"], "")
        for a in d["avisos"]:
            self.assertTrue(a["nombre"])

    def test_el_freno_de_emergencia_del_servidor_se_ve_como_frenado(self):
        antes = os.environ.get(OCOM.ENV_CLIENTE_ACTIVA)
        os.environ[OCOM.ENV_CLIENTE_ACTIVA] = "0"
        try:
            _, d = Mundo().get()
        finally:
            if antes is None:
                os.environ.pop(OCOM.ENV_CLIENTE_ACTIVA, None)
            else:
                os.environ[OCOM.ENV_CLIENTE_ACTIVA] = antes
        por_clave = {a["clave"]: a for a in d["avisos"]}
        self.assertFalse(por_clave["ot_terminada_cliente"]["sale"])
        self.assertIn("OT_TERMINADA_CLIENTE_ACTIVA", por_clave["ot_terminada_cliente"]["motivo"])
        self.assertTrue(por_clave["ot_firma_remota"]["sale"])

    def test_si_la_base_falla_el_cliente_ve_un_mensaje_amable_sin_detalles_internos(self):
        st, d = Mundo(falla=True).get()
        self.assertEqual(st, 500)
        self.assertFalse(d["ok"])
        self.assertNotIn("password", d["error"].lower())
        self.assertNotIn("XYZ", d["error"])

    def test_la_respuesta_no_se_guarda_en_cache(self):
        m = Mundo()
        with m.app.test_request_context("/c/5"):
            g.permissions = {}
            resp = m.amb["mant_cliente_comunicaciones"](5)
            self.assertIn("no-store", resp.headers.get("Cache-Control", ""))

    def test_la_ruta_pide_sesion_y_nunca_deja_pasar_a_un_tecnico(self):
        t = FC.texto("app.py")
        i = t.index('@app.route("/mantenciones/api/clientes/<int:cid>/comunicaciones"')
        cabecera = t[i:i + 320]
        self.assertIn("@_mant_required", cabecera)
        self.assertIn("@_no_tecnico", cabecera)
        self.assertIn('methods=["GET"]', cabecera)

    def test_los_avisos_del_panel_son_correos_registrados(self):
        for clave in FC.valor_literal("app.py", "_CLI_COM_AVISOS"):
            c = CCR.por_clave(clave)
            self.assertIsNotNone(c, clave)
            self.assertEqual(c["destinatario"], "cliente", clave)


# ──────────────────────────────────────────────────────────────────────────────
#  El frente (Node): las funciones de dibujo se ejecutan de verdad en un DOM mínimo.
# ──────────────────────────────────────────────────────────────────────────────
JS = FC.texto("static/mantenciones_ficha.js")


def _trozo_js():
    i = JS.index("const _COM = {")
    j = JS.index("async function cargarCampanaCliente()")
    return JS[i:j]


HARNESS = r"""
const els = {};
const mk = id => (els[id] = { id, innerHTML: '' });
['comAvisos', 'comFiltros', 'comListado', 'comPaginador', 'comCampana'].forEach(mk);
global.document = { getElementById: id => els[id] || null };
global.window = { __FICHA_DATA: { is_superadmin: __SUPER__ } };
global.CID = 5;
global.URLSearchParams = URLSearchParams;
function _evEsc(s) { return String(s == null ? '' : s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }
const llamadas = [];
let campanas = 0;
async function cargarCampanaCliente() { campanas++; }
global.fetch = async (url) => { llamadas.push(url); const d = RESPUESTAS.shift();
  return { ok: d.__ok !== false, json: async () => d }; };
__TROZO__
(async () => {
  const salida = {};
  __PRUEBA__
  console.log(JSON.stringify(salida));
})().catch(e => { console.log(JSON.stringify({ error: String(e && e.stack || e) })); });
"""

BASE = {
    "ok": True, "page": 1, "pages": 1, "per_page": 20, "total": 2, "desde_n": 1, "hasta_n": 2,
    "conteos": {"enviado": 1, "fallido": 0, "bloqueado": 1}, "filtros": {"estado": "", "ot": 0},
    "ots": [{"id": 30, "numero": "OT-2026-00030", "titulo": "Mantención <b>trotadoras</b>"}],
    "destinatarios": {"ficha": [{"correo": "ana@gimnasio.cl", "rol": "Contacto principal", "nombre": "Ana"}],
                      "contrapartes": [{"correo": "carla@gimnasio.cl", "nombre": "Carla", "ot_id": 30, "ot": "OT-2026-00030"}]},
    "avisos": [{"clave": "ot_creada_cliente", "nombre": "OT creada", "sale": True, "motivo": ""},
               {"clave": "ot_terminada_cliente", "nombre": "OT terminada", "sale": False, "motivo": "llave cerrada"}],
    "filas": [
        {"id": 1, "fecha": "05/11/2026", "hora": "21:37:10", "estado": "enviado", "motivo": "", "correo_nombre": "OT creada: aviso",
         "asunto": "Tu visita", "destinatario": "ana@gimnasio.cl", "origen": "automatico", "enviado_por": "Víctor",
         "ot": {"id": 30, "numero": "OT-2026-00030", "url": "/ot/30"}, "adjuntos": ["visita.ics"], "puede_ver": True},
        {"id": 2, "fecha": "05/11/2026", "hora": "21:40:00", "estado": "bloqueado", "motivo": "Llave de paso de Servicio Técnico",
         "correo_nombre": "", "asunto": "<img src=x onerror=alert(1)>", "destinatario": "ana@gimnasio.cl", "origen": "manual",
         "enviado_por": "", "ot": None, "adjuntos": [], "puede_ver": False},
    ],
}


def _node(respuestas, prueba, superadmin=False):
    if not shutil.which("node"):
        raise unittest.SkipTest("Node no está instalado")
    import json
    codigo = (HARNESS.replace("__SUPER__", "true" if superadmin else "false").replace("__TROZO__", _trozo_js())
              .replace("__PRUEBA__", prueba))
    codigo = "const RESPUESTAS = " + json.dumps(respuestas) + ";\n" + codigo
    r = subprocess.run(["node", "-e", codigo], capture_output=True, text=True, timeout=60, encoding="utf-8")
    sal = (r.stdout or "").strip().splitlines()
    if not sal:
        raise AssertionError(f"Node no devolvió nada: {r.stderr[:400]}")
    d = json.loads(sal[-1])
    if "error" in d:
        raise AssertionError(d["error"])
    return d


class TestFrente(unittest.TestCase):
    def test_pinta_avisos_filtros_lista_y_paginador(self):
        d = _node([BASE], "await cargarComunicaciones(); salida.av = els.comAvisos.innerHTML; salida.fi = els.comFiltros.innerHTML;"
                          " salida.li = els.comListado.innerHTML; salida.pa = els.comPaginador.innerHTML; salida.camp = campanas;"
                          " salida.url = llamadas[0];")
        self.assertIn("ana@gimnasio.cl", d["av"])
        self.assertIn("carla@gimnasio.cl", d["av"])
        self.assertIn("frenado: llave cerrada", d["av"])
        self.assertIn("Editar contactos", d["av"])
        self.assertIn("Enviados", d["fi"])
        self.assertIn("Frenados", d["fi"])
        self.assertIn("No salieron", d["fi"])
        self.assertIn("OT creada: aviso", d["li"])
        self.assertIn("Invitación de calendario", d["li"], "el .ics se muestra como lo que es")
        self.assertIn("OT-2026-00030", d["li"])
        self.assertIn("Llave de paso de Servicio Técnico", d["li"], "el motivo del frenado queda a la vista")
        self.assertIn("Mostrando 1–2 de 2", d["pa"])
        self.assertEqual(d["camp"], 1, "la campana de siempre se sigue cargando")
        self.assertIn("/mantenciones/api/clientes/5/comunicaciones?", d["url"])
        self.assertIn("per_page=20", d["url"])

    def test_lo_que_viene_del_servidor_se_escapa(self):
        d = _node([BASE], "await cargarComunicaciones(); salida.li = els.comListado.innerHTML; salida.fi = els.comFiltros.innerHTML;")
        self.assertNotIn("<img src=x", d["li"])
        self.assertIn("&lt;img", d["li"])
        self.assertNotIn("<b>trotadoras</b>", d["fi"])

    def test_ver_correo_solo_aparece_para_el_superadmin(self):
        prueba = "await cargarComunicaciones(); salida.li = els.comListado.innerHTML;"
        self.assertNotIn("/comunicaciones/log/1/cuerpo", _node([BASE], prueba, superadmin=False)["li"])
        self.assertIn("/comunicaciones/log/1/cuerpo", _node([BASE], prueba, superadmin=True)["li"])
        self.assertNotIn("/comunicaciones/log/2/cuerpo", _node([BASE], prueba, superadmin=True)["li"], "sin copia no hay botón")

    def test_filtrar_pide_la_pagina_1_con_el_filtro_y_no_recarga_la_campana(self):
        d = _node([BASE, BASE], "await cargarComunicaciones(); comFiltrarEstado('bloqueado'); await new Promise(r => setTimeout(r, 30));"
                               " salida.urls = llamadas; salida.camp = campanas;")
        self.assertIn("estado=bloqueado", d["urls"][1])
        self.assertIn("page=1", d["urls"][1])
        self.assertEqual(d["camp"], 1)

    def test_filtrar_por_ot_y_cambiar_de_pagina(self):
        d = _node([BASE, BASE, BASE], "await cargarComunicaciones(); comFiltrarOt('30'); await new Promise(r => setTimeout(r, 30));"
                                      " comPagina(1); await new Promise(r => setTimeout(r, 30)); salida.urls = llamadas;")
        self.assertIn("ot=30", d["urls"][1])
        self.assertIn("page=2", d["urls"][2])
        self.assertIn("ot=30", d["urls"][2], "el filtro sobrevive al cambio de página")

    def test_un_clic_mientras_carga_no_se_pierde(self):
        d = _node([BASE, BASE], "cargarComunicaciones(); comFiltrarEstado('fallido'); await new Promise(r => setTimeout(r, 60));"
                               " salida.urls = llamadas;")
        self.assertEqual(len(d["urls"]), 2)
        self.assertIn("estado=fallido", d["urls"][1])

    def test_sin_correos_dice_que_todavia_no_hay_y_con_filtro_dice_que_ninguno_coincide(self):
        vacio = dict(BASE, filas=[], total=0, desde_n=0, hasta_n=0)
        d = _node([vacio], "await cargarComunicaciones(); salida.li = els.comListado.innerHTML; salida.pa = els.comPaginador.innerHTML;")
        self.assertIn("Todavía no se le ha enviado ningún correo", d["li"])
        self.assertEqual(d["pa"], "")
        d2 = _node([vacio], "await cargarComunicaciones({ estado: 'fallido' }); salida.li = els.comListado.innerHTML;")
        self.assertIn("Ningún correo con ese filtro", d2["li"])

    def test_cliente_sin_correo_registrado_avisa_con_claridad(self):
        sin = dict(BASE, destinatarios={"ficha": [], "contrapartes": []})
        d = _node([sin], "await cargarComunicaciones(); salida.av = els.comAvisos.innerHTML;")
        self.assertIn("no tiene ningún correo registrado", d["av"])

    def test_si_el_servidor_falla_se_ve_el_error_y_no_se_rompe(self):
        d = _node([{"ok": False, "error": "No se pudieron cargar las comunicaciones.", "__ok": False}],
                  "await cargarComunicaciones(); salida.li = els.comListado.innerHTML;")
        self.assertIn("No se pudieron cargar las comunicaciones.", d["li"])

    def test_sin_popups_nativos_en_el_trozo(self):
        t = _trozo_js()
        for nativo in ("alert(", "confirm(", "prompt("):
            self.assertNotIn(nativo, t)


class TestPlantilla(unittest.TestCase):
    HTML = FC.texto("templates/mantenciones/ficha.html")

    def test_la_pestana_tiene_los_contenedores_que_el_js_necesita(self):
        i = self.HTML.index('id="tab-comunicaciones"')
        trozo = self.HTML[i:i + 4000]
        for ident in ("comAvisos", "comFiltros", "comListado", "comPaginador", "comCampana"):
            self.assertIn(f'id="{ident}"', trozo)
        self.assertIn("<details", trozo, "la campana interna sigue ahí, plegada (REGLA #4.2: no se quita)")

    def test_el_boton_de_la_pestana_carga_las_comunicaciones(self):
        self.assertRegex(self.HTML, r'data-tab="comunicaciones"[^>]*onclick="switchTab\(\'comunicaciones\'\);cargarComunicaciones\(\)"')

    def test_el_js_conserva_la_campana_y_las_funciones_de_siempre(self):
        for f in ("cargarCampanaCliente", "abrirEmailManual", "cargarComunicaciones"):
            self.assertRegex(JS, r"function " + f + r"\b")


if __name__ == "__main__":
    unittest.main()
