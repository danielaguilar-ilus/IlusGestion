"""📜 Anexo FIRMADO + OT eliminada (REGLA #52). Daniel, 2026-10-28 («es OT, no UTS»): «cuando eliminemos una OT y el anexo está firmado, que se le envíe
al correo que el anexo queda inválido por la anulación del servicio. Por ambas partes».

Qué fijan estas pruebas (SIN base ni correo reales: REGLA #22; las funciones de app.py se extraen por texto con tests/_fuente_app.py y corren sobre
una base y un correo de MENTIRA, sin Flask ni red):
  a) El anexo firmado de la OT pasa a 'anulado' DENTRO de la transacción del borrado y ANTES del DELETE (con quién, cuándo, por qué y de qué OT).
  b) Se avisa a las DOS partes por la puerta única, con la clave del registro: proveedor (llave General) e ILUS (Comunicación interna); el
     proveedor recibe SIN montos ni datos internos; ILUS recibe quién eliminó, el lote de proveedor y si el aviso al proveedor salió.
  c) Un anexo sin firmar no cambia ("se maneja como hoy"); un correo que no sale o que revienta no deshace el borrado y se dice con la verdad.
  d) Anexo único (REGLA #46): eliminar la desinstalación que COMPARTE el anexo firmado de su instalación NO invalida el anexo.
  e) Fail-CLOSED si no se puede revisar el anexo; el aviso previo usa el mismo plan que el borrado; un técnico nunca lo ve.
Correr con:  py -m unittest tests.test_anexo_anulacion_ot_eliminada
"""
import datetime
import json
import os
import shutil
import subprocess
import unittest

import anexo_anulacion as AX
import comm_correos_registro as R
import ot_anexo_costo as OAC
import ot_eventos
from tests import _fuente_app as F

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
AHORA = datetime.datetime(2026, 10, 28, 10, 30)
FIRMADO = datetime.datetime(2026, 10, 20, 15, 0)        # naive = UTC, como lo guarda MySQL (en pantalla se muestra en hora Chile)

FUNCIONES = ("_mant_visita_eliminar_core", "_anexo_anul_destinos_proveedor", "_anexo_anul_interno", "_anexo_anul_item",
             "_anexo_anul_plan", "_anexo_anul_canal_apagado", "_anexo_anul_marcar", "_anexo_anul_html", "_anexo_anul_enviar",
             "_anexo_anul_avisar", "ot2_api_anexo_anulacion_previa")


def _items(*partidas):
    """precio_items_json de un anexo; cada partida (concepto, monto[, rol])."""
    out = []
    for p in partidas:
        fila = {"concepto": p[0], "monto": p[1]}
        if len(p) > 2:
            fila["ot"] = p[2]
        out.append(fila)
    return json.dumps(out)


class _Cur:
    def __init__(self, base):
        self.b = base

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        b = self.b
        b.sqls.append((sql, params))
        if sql.startswith("UPDATE mant_anexos SET estado='anulado', anulado_at"):
            if b.sin_columnas:
                raise Exception("(1054, \"Unknown column 'anulado_at' in 'field list'\")")
            ax = b.anexos.get(params[3])
            if ax and ax["estado"] == "firmado":
                ax.update(estado="anulado", anulado_por=params[0], anulado_motivo=params[1], anulado_ot_numero=params[2])
                return 1
            return 0
        if sql.startswith("UPDATE mant_anexos SET estado='anulado' WHERE id=%s"):
            ax = b.anexos.get(params[0])
            if ax and ax["estado"] == "firmado":
                ax["estado"] = "anulado"
                return 1
            return 0
        if sql.startswith("DELETE FROM mant_visitas"):
            b.visitas.pop(params[0], None)
            for ax in b.anexos.values():          # el FK ON DELETE SET NULL de mant_anexos.ot_id
                if ax.get("ot_id") == params[0]:
                    ax["ot_id"] = None
            return 1
        return 0


class _Conn:
    def __init__(self, base):
        self.b = base

    def cursor(self):
        return _Cur(self.b)

    def commit(self):
        self.b.commits += 1

    def close(self):
        pass


class Base:
    """La base y el correo de mentira de una eliminación."""

    def __init__(self):
        self.visitas = {}
        self.anexos = {}
        self.ficha = {}
        self.roles = {}
        self.gestion = []
        self.sqls, self.mails, self.logs = [], [], []
        self.commits = 0
        self.fac = None
        self.send_ok = True
        self.send_raises = False
        self.sin_columnas = False
        self.plan_falla = False

    # ── datos ────────────────────────────────────────────────────────────────
    def visita(self, vid=300, numero="OT-2026-00300", **kw):
        fila = {"id": vid, "cliente_id": 5, "numero_ot": numero, "titulo": "Instalación de trotadoras", "fecha_programada": None,
                "levantamiento_id": None, "estado": "programada", "fecha_fin": None, "ot_madre_id": None, "ot_relacion": None}
        fila.update(kw)
        self.visitas[vid] = fila
        return fila

    def anexo(self, aid=221, ot_id=300, estado="firmado", numero=221, **kw):
        fila = {"id": aid, "numero": numero, "ot_id": ot_id, "estado": estado, "proveedor_nombre": "Milling Logística SpA",
                "proveedor_rut": "77.000.000-1", "tecnico_externo_id": 7, "enviado_email": "contacto@milling.cl",
                "firmado_at": FIRMADO, "firmante_nombre": "Isabel Milling",
                "precio_items_json": _items(("Instalación", 370000))}
        fila.update(kw)
        self.anexos[aid] = fila
        return fila

    # ── dobles de la capa de datos ───────────────────────────────────────────
    def fetchone(self, sql, params=None):
        p = params or ()
        if sql.startswith("SELECT * FROM mant_visitas"):
            return dict(self.visitas[p[0]]) if p[0] in self.visitas else None
        if "FROM mant_visitas WHERE id=%s" in sql:
            v = self.visitas.get(p[0])
            return {k: v[k] for k in ("cliente_id", "numero_ot", "titulo", "fecha_programada", "levantamiento_id", "estado",
                                      "fecha_fin")} if v else None
        if "contacto_email FROM mant_tecnicos_externos" in sql:
            return {"contacto_email": self.ficha[p[0]]} if p[0] in self.ficha else None
        if "FROM mant_anexos WHERE id=%s" in sql:
            return dict(self.anexos[p[0]]) if p[0] in self.anexos else None
        return None

    def fetchall(self, sql, params=None):
        p = params or ()
        if "FROM mant_anexos WHERE ot_id=%s AND estado='firmado'" in sql:
            if self.plan_falla:
                raise Exception("MySQL server has gone away")
            return [dict(a) for a in self.anexos.values() if a.get("ot_id") == p[0] and a["estado"] == "firmado"]
        if "FROM app_users WHERE id IN" in sql:
            return [{"id": i, "role": self.roles.get(i)} for i in p]
        return []

    def madre(self, vid, solo_firmado=False):
        """_ot_anexo_de_la_madre de mentira: el anexo FIRMADO de la instalación si trae partidas de la desinstalación."""
        v = self.visitas[vid]
        for a in self.anexos.values():
            if a.get("ot_id") == v.get("ot_madre_id") and a["estado"] == "firmado" and OAC.tiene_salida(a["precio_items_json"]):
                return dict(a)
        return None

    def enviar(self, to, subject, html, **kw):
        if self.send_raises:
            raise RuntimeError("SMTP caído")
        self.mails.append({"to": to, "subject": subject, "html": html, **kw})
        return self.send_ok

    def entorno(self):
        b = self
        return {
            "print": lambda *a, **k: None, "json": json, "_oac": OAC, "_axa": AX, "_evt": ot_eventos,
            "_FACTURA_CHECK_ERROR": object(),
            "mysql_fetchone": b.fetchone, "mysql_fetchall": b.fetchall, "get_mysql": lambda: _Conn(b),
            "current_username": lambda: "Daniel Aguilar", "_mant_log": lambda *a, **k: b.logs.append(a),
            "_mant_visita_factura_proveedor": lambda vid: b.fac, "_ot_levantamiento_de": lambda vid: None,
            "_ot_tec_snapshot_borrado": lambda vid, v_info: None, "_ot_tec_correo_borrada": lambda vid, snap: None,
            "_ot_hijas_de": lambda vid, cur=None, estricto=False: [], "_fac_libro_marcar": lambda vid: None,
            "_ot_anexo_de_la_madre": b.madre,
            "_ot_gestion_destinatarios": lambda vid, fila=None: [dict(g) for g in b.gestion],
            "_rol_familia": lambda r: "tecnico" if (r or "").startswith("tecnico") else (r or ""),
            "chile_fmt_filter": lambda v, fmt="%d/%m/%Y %H:%M": v.strftime(fmt), "_now_chile": lambda: AHORA,
            "_get_brand_cfg": lambda: {"support_email": "soportetec@sphs.cl"}, "ILUS_SOPORTE_EMAIL": "soportetec@sphs.cl",
            "_brand_subject": lambda t: "ILUS · " + t, "_send_ilus_email": b.enviar,
            "_email_resultado": lambda ok: {"motivo": "el envío de correos de General está apagado"},
            "_ilus_email_rows": lambda pares: ";".join(f"{a}: {c}" for a, c in pares),
            "_ilus_email_master": lambda ctx: "|".join([ctx["title"], ctx["message"], ctx["detail_rows_html"], ctx["closing_message"]]),
            "_correo_bloqueo_previo": lambda modulo, clave=None: None,
            "_email_motivo_bloqueo_txt": lambda tipo, modulo, correo=None: f"bloqueo {tipo}",
            "jsonify": lambda d: d, "_no_tecnico": lambda f: f,
        }

    def nucleo(self):
        amb = F.cargar(funciones=FUNCIONES, extra=self.entorno())
        return amb


def _escenario_basico(**kw):
    b = Base()
    b.visita()
    b.anexo(**kw)
    b.ficha[7] = "facturacion@milling.cl"
    b.gestion = [
        {"id": 1, "nombre": "Aarón Rojas", "correo": "aaron.rojas@sphs.cl", "por": "responsable"},
        {"id": 2, "nombre": "Daniel Aguilar", "correo": "daniel.aguilar@sphs.cl", "por": "creador"},
    ]
    b.roles = {1: "supervisor", 2: "superadmin"}
    return b


# ════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  1) El módulo puro
# ════════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestModuloPuro(unittest.TestCase):
    def test_correos_de_valida_deduplica_y_limita(self):
        self.assertEqual(AX.correos_de("A@Milling.cl; a@milling.cl, basura, sin@punto", "otro@milling.cl"),
                         ["a@milling.cl", "otro@milling.cl"])
        self.assertEqual(AX.correos_de(None, "", "  "), [])
        self.assertEqual(len(AX.correos_de("a@x.cl b@x.cl c@x.cl d@x.cl")), AX.MAX_CORREOS_PROVEEDOR)

    def test_partidas_de_la_desinstalacion_sin_montos(self):
        its = _items(("Instalación", 370000, "entrada"), ("Despacho (instalación)", 20000, "entrada"),
                     ("Desinstalación", 150000, "salida"), ("Despacho (desinstalación)", 15000, "salida"))
        self.assertEqual(AX.partidas_salida(its), ["Desinstalación", "Despacho (desinstalación)"])

    def test_el_aviso_previo_dice_que_anexo_queda_invalido_y_a_quien_se_le_avisa(self):
        plan = {"anexos": [{"numero": 221, "proveedor": "Milling", "firmado_el": "20/10/2026", "parcial": False,
                            "destinos": ["contacto@milling.cl"]}],
                "interno": [{"nombre": "Aarón Rojas", "correo": "aaron.rojas@sphs.cl"}]}
        p = AX.para_pantalla(plan)
        self.assertTrue(p["aplica"])
        txt = " ".join(p["lineas"])
        self.assertIn("Anexo N° 221", txt)
        self.assertIn("quedará inválido por la anulación del servicio", txt)
        self.assertIn("contacto@milling.cl", txt)
        self.assertIn("Aarón Rojas (aaron.rojas@sphs.cl)", txt)
        self.assertIn("anulación del servicio", p["titulo"])

    def test_sin_anexo_firmado_no_hay_aviso(self):
        self.assertEqual(AX.para_pantalla({"anexos": [], "interno": []})["aplica"], False)

    def test_el_aviso_previo_no_oculta_un_proveedor_sin_correo_ni_un_correo_apagado(self):
        plan = {"anexos": [{"numero": 9, "proveedor": "X", "firmado_el": "01/10/2026", "destinos": []}],
                "interno": [{"nombre": "A", "correo": "a@sphs.cl"}]}
        txt = " ".join(AX.lineas_confirmacion(plan, apagado_interno="el envío de correos está apagado"))
        self.assertIn("no tiene un correo registrado", txt)
        self.assertIn("Hoy ese correo no saldrá: el envío de correos está apagado", txt)

    def test_el_anexo_compartido_se_dice_parcial(self):
        plan = {"anexos": [{"numero": 5, "proveedor": "X", "firmado_el": "01/10/2026", "parcial": True,
                            "partidas": ["Desinstalación", "Despacho (desinstalación)"], "destinos": ["x@x.cl"]}], "interno": []}
        txt = " ".join(AX.lineas_confirmacion(plan))
        self.assertIn("sigue vigente para la OT de la instalación", txt)
        self.assertIn("Desinstalación y Despacho (desinstalación)", txt)
        self.assertNotIn("quedará inválido", txt)

    def test_el_correo_al_proveedor_no_lleva_montos_ni_datos_internos(self):
        a = {"numero": 221, "proveedor": "Milling", "firmado_el": "20/10/2026", "firmante": "Isabel Milling"}
        c = AX.correo_proveedor(a, "OT-2026-00300", "soportetec@sphs.cl")
        todo = json.dumps(c, ensure_ascii=False)
        self.assertIn("Anexo de Servicios N° 221", c["asunto"])
        self.assertIn("quedó inválido por la anulación del servicio", c["mensaje"])
        self.assertIn("OT-2026-00300", todo)
        self.assertIn("soportetec@sphs.cl", c["cierre"])
        for prohibido in ("$", "monto", "margen", "costo", "lote", "cliente"):
            self.assertNotIn(prohibido, todo.lower(), f"el proveedor no debe ver «{prohibido}»")

    def test_el_correo_parcial_al_proveedor_dice_que_el_resto_sigue_vigente(self):
        a = {"numero": 5, "proveedor": "X", "firmado_el": "01/10/2026", "parcial": True, "partidas": ["Desinstalación"]}
        c = AX.correo_proveedor(a, "OT-2026-00301", "soportetec@sphs.cl")
        self.assertIn("partidas", c["asunto"])
        self.assertIn("El resto del anexo sigue vigente", c["mensaje"])
        self.assertIn("(Desinstalación)", c["mensaje"])

    def test_el_correo_interno_trae_quien_cuando_lote_y_si_el_proveedor_fue_avisado(self):
        a = {"numero": 221, "proveedor": "Milling", "firmado_el": "20/10/2026", "firmante": "Isabel Milling"}
        c = AX.correo_interno(a, "OT-2026-00300", "Instalación de trotadoras", "Daniel Aguilar", "28/10/2026 10:30",
                              "estaba en el lote #8 (pagada) y salió de él", "NO salió (el envío está apagado) — destinatarios: x@x.cl")
        filas = dict(c["filas"])
        self.assertEqual(filas["Eliminada por"], "Daniel Aguilar")
        self.assertEqual(filas["Lote de proveedor"], "estaba en el lote #8 (pagada) y salió de él")
        self.assertIn("NO salió", filas["Aviso al proveedor"])
        self.assertEqual(filas["Firmado el"], "20/10/2026 por Isabel Milling")
        self.assertEqual(filas["Motivo"], AX.MOTIVO)
        self.assertNotIn("$", json.dumps(c, ensure_ascii=False))

    def test_texto_aviso_dice_la_verdad(self):
        self.assertIn("sin correo registrado", AX.texto_aviso([], False))
        self.assertEqual(AX.texto_aviso(["a@x.cl", "b@x.cl"], True), "enviado a a@x.cl, b@x.cl")
        self.assertIn("NO salió (apagado)", AX.texto_aviso(["a@x.cl"], False, "apagado"))


# ════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  2) El plan (solo lectura)
# ════════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestPlan(unittest.TestCase):
    def test_un_anexo_firmado_de_la_ot_se_anula_completo_con_los_dos_correos_del_proveedor(self):
        b = _escenario_basico()
        plan = b.nucleo()["_anexo_anul_plan"](300)
        self.assertEqual(plan["numero_ot"], "OT-2026-00300")
        (ax,) = plan["anexos"]
        self.assertFalse(ax["parcial"])
        self.assertEqual((ax["numero"], ax["firmado_el"], ax["firmante"]), (221, "20/10/2026", "Isabel Milling"))
        self.assertEqual(ax["destinos"], ["contacto@milling.cl", "facturacion@milling.cl"])     # donde se mandó el enlace + la ficha
        self.assertEqual([p["correo"] for p in plan["interno"]], ["aaron.rojas@sphs.cl", "daniel.aguilar@sphs.cl"])

    def test_un_anexo_sin_firmar_no_entra(self):
        for estado in ("borrador", "enviado", "visto", "vencido", "rechazado"):
            b = _escenario_basico(estado=estado)
            self.assertEqual(b.nucleo()["_anexo_anul_plan"](300)["anexos"], [], estado)

    def test_el_plan_no_arma_la_lista_interna_si_no_hay_anexo(self):
        b = _escenario_basico(estado="enviado")
        self.assertEqual(b.nucleo()["_anexo_anul_plan"](300)["interno"], [])

    def test_la_desinstalacion_que_comparte_el_anexo_firmado_es_parcial(self):
        b = _escenario_basico(precio_items_json=_items(("Instalación", 370000, "entrada"), ("Desinstalación", 150000, "salida"),
                                                      ("Despacho (desinstalación)", 15000, "salida")))
        b.visita(301, "OT-2026-00301", ot_madre_id=300, ot_relacion="desinstalacion")
        (ax,) = b.nucleo()["_anexo_anul_plan"](301)["anexos"]
        self.assertTrue(ax["parcial"])
        self.assertEqual(ax["partidas"], ["Desinstalación", "Despacho (desinstalación)"])
        self.assertEqual(ax["numero"], 221)

    def test_una_desinstalacion_cuyo_anexo_comun_no_esta_firmado_no_cambia_nada(self):
        b = _escenario_basico(estado="enviado", precio_items_json=_items(("Instalación", 1, "entrada"), ("Desinstalación", 1, "salida")))
        b.visita(301, "OT-2026-00301", ot_madre_id=300, ot_relacion="desinstalacion")
        self.assertEqual(b.nucleo()["_anexo_anul_plan"](301)["anexos"], [])

    def test_la_lista_interna_excluye_tecnicos_y_repetidos(self):
        b = _escenario_basico()
        b.gestion = [
            {"id": 1, "nombre": "Aarón", "correo": "aaron.rojas@sphs.cl", "por": "responsable"},
            {"id": 3, "nombre": "Técnico Interno", "correo": "tecnico@sphs.cl", "por": "creador"},
            {"id": 4, "nombre": "Aarón (otra cuenta)", "correo": "AARON.ROJAS@sphs.cl", "por": "ejecutivo"},
            {"id": 5, "nombre": "Sin correo", "correo": "", "por": "respaldo"},
        ]
        b.roles = {1: "supervisor", 3: "tecnico_ejecutivo", 4: "admin", 5: "admin"}
        interno = b.nucleo()["_anexo_anul_interno"](300, {})
        self.assertEqual([p["correo"] for p in interno], ["aaron.rojas@sphs.cl"])

    def test_si_la_ficha_del_proveedor_falla_igual_se_usa_el_correo_del_envio(self):
        b = _escenario_basico()
        amb = b.nucleo()
        amb["mysql_fetchone"] = lambda sql, p=None: (_ for _ in ()).throw(RuntimeError("caído")) if "tecnicos_externos" in sql else b.fetchone(sql, p)
        self.assertEqual(amb["_anexo_anul_destinos_proveedor"]({"id": 221, "tecnico_externo_id": 7, "enviado_email": "contacto@milling.cl"}),
                         ["contacto@milling.cl"])


# ════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  3) La eliminación completa
# ════════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestEliminacion(unittest.TestCase):
    def _eliminar(self, b, vid=300, quitar=True):
        res = b.nucleo()["_mant_visita_eliminar_core"](vid, quitar_de_factura=quitar)
        return res

    def test_el_anexo_firmado_queda_anulado_en_la_misma_transaccion_y_antes_del_delete(self):
        b = _escenario_basico()
        res = self._eliminar(b)
        self.assertTrue(res["ok"])
        ax = b.anexos[221]
        self.assertEqual(ax["estado"], "anulado")
        self.assertEqual(ax["anulado_por"], "Daniel Aguilar")
        self.assertEqual(ax["anulado_motivo"], "anulación del servicio (OT eliminada)")
        self.assertEqual(ax["anulado_ot_numero"], "OT-2026-00300")
        self.assertIsNone(ax["ot_id"], "el FK dejó el ot_id en NULL: el número de la OT quedó guardado en el propio anexo")
        sqls = [s for s, _ in b.sqls]
        i_upd = [i for i, s in enumerate(sqls) if s.startswith("UPDATE mant_anexos SET estado='anulado'")][0]
        i_del = [i for i, s in enumerate(sqls) if s.startswith("DELETE FROM mant_visitas")][0]
        self.assertLess(i_upd, i_del, "primero se anula el anexo, después se borra la OT (todo antes del commit)")
        self.assertEqual(b.commits, 1, "una sola transacción")

    def test_se_avisa_a_las_dos_partes_por_la_puerta_unica_con_su_clave(self):
        b = _escenario_basico()
        res = self._eliminar(b)
        prov, interno = b.mails
        # proveedor: «Para» el correo del envío, con copia la ficha; llave General; clave del registro
        self.assertEqual(prov["to"], "contacto@milling.cl")
        self.assertEqual(prov["cc"], ["facturacion@milling.cl"])
        self.assertEqual((prov["correo"], prov["modulo"]), ("anexo_anulado_proveedor", "general"))
        self.assertIn("Anexo de Servicios N° 221", prov["subject"])
        # ILUS: responsable primero, el resto con copia; Comunicación interna
        self.assertEqual(interno["to"], "aaron.rojas@sphs.cl")
        self.assertEqual(interno["cc"], ["daniel.aguilar@sphs.cl"])
        self.assertEqual((interno["correo"], interno["modulo"]), ("anexo_anulado_interno", "comunicacion_interna"))
        self.assertEqual(prov["ref"], {"tipo": "OT", "codigo": "OT-2026-00300"})
        # el proveedor NO ve montos ni datos internos; ILUS ve quién la eliminó y que el proveedor fue avisado
        self.assertNotIn("$", prov["html"])
        self.assertNotIn("370", prov["html"])
        self.assertIn("Eliminada por: Daniel Aguilar", interno["html"])
        self.assertIn("Aviso al proveedor: enviado a contacto@milling.cl, facturacion@milling.cl", interno["html"])
        # la respuesta (para la pantalla) lo cuenta
        (a,) = res["anexo_anulado"]["anexos"]
        self.assertTrue(a["proveedor"]["salio"] and a["interno"]["salio"])
        txt = " ".join(res["anexo_anulado"]["lineas"])
        self.assertIn("Anexo N° 221: quedó inválido por la anulación del servicio.", txt)
        self.assertIn("Aviso al proveedor: enviado a contacto@milling.cl", txt)
        self.assertIn("Aviso al equipo ILUS: enviado a aaron.rojas@sphs.cl, daniel.aguilar@sphs.cl", txt)

    def test_queda_la_constancia_en_la_bitacora_del_anexo_de_la_ot_y_del_cliente(self):
        b = _escenario_basico()
        self._eliminar(b)
        acciones = {(e, i, a) for e, i, a, *_ in b.logs}
        self.assertIn(("anexo", 221, "anulado_ot_eliminada"), acciones)
        self.assertIn(("visita", 300, "anexo_anulado"), acciones)
        self.assertIn(("anexo", 221, "anulado_aviso_proveedor"), acciones)
        self.assertIn(("anexo", 221, "anulado_aviso_interno"), acciones)
        detalle = [d for e, i, a, d in b.logs if (e, a) == ("anexo", "anulado_ot_eliminada")][0]
        self.assertIn("quedó inválido por la anulación del servicio (OT eliminada)", detalle)
        self.assertIn("OT-2026-00300 eliminada por Daniel Aguilar el 28/10/2026 10:30", detalle)

    def test_si_la_ot_estaba_en_un_lote_de_proveedor_se_avisa_y_queda_en_el_historial_del_lote(self):
        b = _escenario_basico()
        b.fac = {"id": 8, "numero_documento": "F-123", "proveedor_nombre": "Milling", "estado_pago": "pagada"}
        self._eliminar(b)
        interno = b.mails[1]
        self.assertIn("Lote de proveedor: estaba en el lote #8 (pagada) y salió de él", interno["html"])
        self.assertTrue(any((e, i, a) == ("factura_proveedor", 8, "anexo_anulado_ot_eliminada") for e, i, a, *_ in b.logs))

    def test_un_proveedor_sin_correo_no_rompe_y_ilus_queda_avisada_de_que_hay_que_llamarlo(self):
        b = _escenario_basico(enviado_email=None)
        b.ficha.clear()
        res = self._eliminar(b)
        self.assertTrue(res["ok"])
        self.assertEqual(len(b.mails), 1, "solo el aviso interno")
        self.assertEqual(b.mails[0]["correo"], "anexo_anulado_interno")
        self.assertIn("Aviso al proveedor: sin correo registrado: hay que avisarle por otro medio", b.mails[0]["html"])
        self.assertIn("sin correo registrado", res["anexo_anulado"]["anexos"][0]["proveedor"]["texto"])
        self.assertEqual(b.anexos[221]["estado"], "anulado")

    def test_un_correo_que_no_sale_se_dice_con_el_motivo_y_no_deshace_el_borrado(self):
        b = _escenario_basico()
        b.send_ok = False
        res = self._eliminar(b)
        self.assertTrue(res["ok"])
        self.assertEqual(b.anexos[221]["estado"], "anulado")
        self.assertNotIn(300, b.visitas)
        p = res["anexo_anulado"]["anexos"][0]["proveedor"]
        self.assertFalse(p["salio"])
        self.assertIn("NO salió (el envío de correos de General está apagado)", p["texto"])

    def test_un_correo_que_revienta_no_deshace_el_borrado(self):
        b = _escenario_basico()
        b.send_raises = True
        res = self._eliminar(b)
        self.assertTrue(res["ok"])
        self.assertEqual(b.anexos[221]["estado"], "anulado")
        self.assertFalse(res["anexo_anulado"]["anexos"][0]["proveedor"]["salio"])

    def test_un_anexo_sin_firmar_se_maneja_como_hoy(self):
        b = _escenario_basico(estado="enviado")
        res = self._eliminar(b)
        self.assertTrue(res["ok"])
        self.assertEqual(b.anexos[221]["estado"], "enviado", "no se toca")
        self.assertEqual(b.mails, [])
        self.assertIsNone(res["anexo_anulado"])
        self.assertFalse(any(s.startswith("UPDATE mant_anexos") for s, _ in b.sqls))

    def test_una_ot_sin_anexo_tampoco_envia_nada(self):
        b = Base()
        b.visita()
        res = self._eliminar(b)
        self.assertTrue(res["ok"])
        self.assertEqual((b.mails, res["anexo_anulado"]), ([], None))

    def test_anexo_unico_eliminar_la_desinstalacion_no_invalida_el_anexo_de_la_instalacion(self):
        b = _escenario_basico(precio_items_json=_items(("Instalación", 370000, "entrada"), ("Desinstalación", 150000, "salida"),
                                                      ("Despacho (desinstalación)", 15000, "salida")))
        b.visita(301, "OT-2026-00301", ot_madre_id=300, ot_relacion="desinstalacion")
        res = self._eliminar(b, vid=301)
        self.assertTrue(res["ok"])
        ax = b.anexos[221]
        self.assertEqual((ax["estado"], ax["ot_id"]), ("firmado", 300), "el anexo sigue firmado y ligado a la instalación")
        self.assertFalse(any(s.startswith("UPDATE mant_anexos") for s, _ in b.sqls))
        prov = b.mails[0]
        self.assertIn("partidas", prov["subject"])
        self.assertIn("Desinstalación y Despacho (desinstalación)", prov["html"])
        self.assertIn("El resto del anexo sigue vigente", prov["html"])
        self.assertTrue(any((e, i, a) == ("anexo", 221, "anexo_partidas_sin_efecto") for e, i, a, *_ in b.logs))
        self.assertTrue(res["anexo_anulado"]["anexos"][0]["parcial"])

    def test_anexo_unico_eliminar_la_instalacion_duena_lo_invalida_completo(self):
        b = _escenario_basico(precio_items_json=_items(("Instalación", 370000, "entrada"), ("Desinstalación", 150000, "salida")))
        res = self._eliminar(b, vid=300)       # la desinstalación ya estaba cancelada: nada vigente que lo impida
        self.assertTrue(res["ok"])
        self.assertEqual(b.anexos[221]["estado"], "anulado")
        self.assertFalse(res["anexo_anulado"]["anexos"][0]["parcial"])

    def test_si_alguien_ya_lo_cambio_justo_antes_no_se_avisa_nada_falso(self):
        b = _escenario_basico()
        amb = b.nucleo()
        plan_real = amb["_anexo_anul_plan"]

        def plan_y_carrera(vid):
            p = plan_real(vid)
            b.anexos[221]["estado"] = "anulado"       # otra persona lo anuló entre la lectura y el UPDATE
            return p
        amb["_anexo_anul_plan"] = plan_y_carrera
        res = amb["_mant_visita_eliminar_core"](300, quitar_de_factura=True)
        self.assertTrue(res["ok"])
        self.assertEqual(b.mails, [])
        self.assertEqual(res["anexo_anulado"]["anexos"], [])

    def test_si_las_columnas_nuevas_aun_no_existen_se_anula_igual(self):
        b = _escenario_basico()
        b.sin_columnas = True
        res = self._eliminar(b)
        self.assertTrue(res["ok"])
        self.assertEqual(b.anexos[221]["estado"], "anulado")

    def test_fail_closed_si_no_se_puede_revisar_el_anexo_no_se_borra_nada(self):
        b = _escenario_basico()
        b.plan_falla = True
        res = self._eliminar(b)
        self.assertFalse(res["ok"])
        self.assertEqual(res["error_codigo"], "ANEXO_NO_VERIFICABLE")
        self.assertIn("no se eliminó", res["error"])
        self.assertIn(300, b.visitas)
        self.assertEqual(b.sqls, [])
        self.assertEqual(b.anexos[221]["estado"], "firmado")

    def test_la_eliminacion_no_toca_el_libro_de_saldo_ni_escribe_en_ot_fac_libro(self):
        b = _escenario_basico()
        self._eliminar(b)
        for s, _ in b.sqls:
            self.assertNotIn("ot_fac_libro", s)


# ════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  4) El aviso previo y el cableado
# ════════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestAvisoPrevio(unittest.TestCase):
    def test_el_aviso_previo_usa_el_mismo_plan_que_el_borrado(self):
        b = _escenario_basico()
        d = b.nucleo()["ot2_api_anexo_anulacion_previa"](300)
        self.assertTrue(d["ok"] and d["aplica"])
        txt = " ".join(d["lineas"])
        self.assertIn("Anexo N° 221", txt)
        self.assertIn("contacto@milling.cl, facturacion@milling.cl", txt)
        self.assertIn("Aarón Rojas (aaron.rojas@sphs.cl)", txt)

    def test_el_aviso_previo_dice_si_el_correo_hoy_no_saldria(self):
        b = _escenario_basico()
        amb = b.nucleo()
        amb["_correo_bloqueo_previo"] = lambda modulo, clave=None: "correo" if clave == "anexo_anulado_interno" else None
        d = amb["ot2_api_anexo_anulacion_previa"](300)
        self.assertIn("Hoy ese correo no saldrá: bloqueo correo", " ".join(d["lineas"]))

    def test_sin_anexo_firmado_el_aviso_previo_no_aplica(self):
        b = _escenario_basico(estado="enviado")
        self.assertFalse(b.nucleo()["ot2_api_anexo_anulacion_previa"](300)["aplica"])

    def test_si_no_se_puede_revisar_el_aviso_previo_lo_dice(self):
        b = _escenario_basico()
        b.plan_falla = True
        amb = b.nucleo()
        amb["jsonify"] = lambda d: d
        # jsonify real devuelve una Response; aquí el doble devuelve el dict y el código HTTP no se ve: se prueba que el error es honesto
        try:
            r = amb["ot2_api_anexo_anulacion_previa"](300)
        except Exception:
            self.fail("el aviso previo no debe reventar")
        self.assertIn("No se pudo revisar", json.dumps(r, ensure_ascii=False))

    def test_la_ruta_del_aviso_previo_es_solo_para_quien_puede_eliminar_y_nunca_un_tecnico(self):
        d = F.decoradores("ot2_api_anexo_anulacion_previa")
        self.assertIn("@_no_tecnico", d)
        self.assertIn("@_ot_can_eliminar", d)
        self.assertIn("@_mant_required", d)
        self.assertTrue(any('"/ot/api/<int:vid>/anexo-anulacion"' in x and '"GET"' in x for x in d))


class TestCableado(unittest.TestCase):
    def test_el_nucleo_lee_antes_anula_dentro_de_la_transaccion_y_avisa_despues_del_commit(self):
        s = F.fuente_funcion("_mant_visita_eliminar_core")
        i_plan, i_marcar = s.index("_anexo_anul_plan(vid)"), s.index("_anexo_anul_marcar(cur,")
        i_del, i_commit, i_avisar = s.index('cur.execute("DELETE FROM mant_visitas WHERE id=%s", (vid,))'), s.index("conn.commit()"), s.index("_anexo_anul_avisar(")
        self.assertLess(i_plan, s.index("conn = get_mysql()"), "el plan se lee ANTES de abrir la transacción")
        self.assertLess(i_marcar, i_del, "se anula dentro de la transacción, antes del DELETE")
        self.assertLess(i_del, i_commit)
        self.assertLess(i_commit, i_avisar, "los correos salen DESPUÉS del commit: un correo nunca deshace el borrado")
        self.assertIn('"error_codigo": "ANEXO_NO_VERIFICABLE"', s)

    def test_el_lote_cuenta_los_anexos_firmados_y_los_informa(self):
        p = F.fuente_funcion("ot2_api_eliminar_lote_preview")
        self.assertIn("n_anexos_firmados", p)
        self.assertIn('"anexos_firmados"', p)
        l = F.fuente_funcion("ot2_api_eliminar_lote")
        self.assertIn('"anexo_anulado"', l)

    def test_la_eliminacion_individual_devuelve_lo_que_paso_con_el_anexo(self):
        s = F.fuente_funcion("mant_visita_del")
        self.assertIn('"anexo_anulado": res.get("anexo_anulado")', s)

    def test_las_claves_estan_en_el_registro_y_salen_por_su_llave(self):
        prov, interno = R.por_clave(AX.CLAVE_PROVEEDOR), R.por_clave(AX.CLAVE_INTERNO)
        self.assertEqual((prov["llave"], prov["destinatario"], prov["modo"]), ("general", "proveedor_courier", "automatico"))
        self.assertEqual((interno["llave"], interno["destinatario"]), ("comunicacion_interna", "responsable_ot"))
        for c in (prov, interno):
            self.assertEqual(c["modulo_ui"], "ot")
            self.assertFalse(c["protegido"])
            self.assertEqual(c["tipo"], "ilus")
        # la puerta única recibe la clave del registro por parámetro (nunca dentro de **kwargs)
        s = F.fuente_funcion("_anexo_anul_enviar")
        self.assertIn("correo=clave", s)
        self.assertIn("_send_ilus_email(", s)

    def test_los_destinatarios_internos_salen_de_la_unica_funcion_de_la_gestion(self):
        s = F.fuente_funcion("_anexo_anul_interno")
        self.assertIn("_ot_gestion_destinatarios(vid, fila)", s)
        self.assertIn('== "tecnico"', s)        # REGLAS #19 y #26: un técnico nunca lo recibe

    def test_la_pagina_publica_de_un_anexo_anulado_no_ofrece_firmar(self):
        s = F.fuente_funcion("ot2_anexo_firma_publica")
        self.assertIn('a["estado"] == "anulado"', s)
        self.assertLess(s.index('a["estado"] == "anulado"'), s.index("vencido ="))
        self.assertIn("anulado=True", s)
        t = F.texto("templates/ot2/anexo_firma.html")
        self.assertIn("{% if anulado is defined and anulado %}", t)
        self.assertIn("quedó inválido por la anulación del servicio", t)
        from jinja2 import Environment
        Environment().parse(t)               # REGLA #9: la plantilla compila

    def test_las_columnas_de_la_anulacion_se_agregan_una_por_sentencia(self):
        s = F.fuente_funcion("_ensure_mant_anexos")
        for col in ("anulado_at", "anulado_por", "anulado_motivo", "anulado_ot_numero"):
            self.assertIn(f'("{col}", "ALTER TABLE mant_anexos ADD COLUMN {col} ', s)
        self.assertNotIn("ADD COLUMN anulado_at DATETIME NULL, ADD COLUMN", s)      # REGLA #18: una cláusula por sentencia

    def test_el_historial_del_lote_entiende_la_nueva_accion(self):
        self.assertIn('"anexo_anulado_ot_eliminada": "Anexo firmado anulado (OT eliminada)"', F.fuente_constante("_MFP_ACCION_TXT"))

    def test_las_cuatro_puertas_de_eliminar_avisan_antes_y_dicen_despues(self):
        for ruta in ("templates/ot2/detalle.html", "templates/mantenciones/calendario.html", "static/mant_ficha.js"):
            t = F.texto(ruta)
            self.assertIn("otAnexoAnulacion.avisoHtml(", t, ruta)
            self.assertIn("_axAviso +", t, ruta)
        self.assertIn("otAnexoAnulacion.resultadoTxt(d)", F.texto("templates/ot2/detalle.html"))
        self.assertIn("anexos_firmados", F.texto("templates/ot2/panel.html"))
        self.assertIn("ot_anexo_anulacion.js", F.texto("templates/base.html"))

    def test_no_se_usan_dialogos_nativos(self):
        js = F.texto("static/ot_anexo_anulacion.js")
        for prohibido in ("alert(", "confirm(", "prompt("):
            self.assertNotIn(" " + prohibido, js)


# ════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  5) El ayudante del navegador (se ejecuta con Node si está disponible)
# ════════════════════════════════════════════════════════════════════════════════════════════════════════════
@unittest.skipUnless(shutil.which("node"), "Node no está instalado")
class TestAyudanteNavegador(unittest.TestCase):
    def _correr(self, respuesta):
        script = r"""
        const fs = require('fs');
        const respuesta = %s;
        global.window = {};
        global.fetch = async () => ({ ok: respuesta.ok, json: async () => respuesta.cuerpo });
        eval(fs.readFileSync(process.argv[1], 'utf8'));
        (async () => {
          const html = await window.otAnexoAnulacion.avisoHtml(300);
          const txt = window.otAnexoAnulacion.resultadoTxt({ anexo_anulado: { lineas: ['Anexo N° 221: quedó inválido.', 'Aviso al proveedor: enviado.'] } });
          const vacio = window.otAnexoAnulacion.resultadoTxt({});
          console.log(JSON.stringify({ html, txt, vacio }));
        })();
        """ % json.dumps(respuesta)
        out = subprocess.run(["node", "-e", script, os.path.join(RAIZ, "static", "ot_anexo_anulacion.js")],
                             capture_output=True, text=True, encoding="utf-8", timeout=60)
        self.assertEqual(out.returncode, 0, out.stderr)
        return json.loads(out.stdout.strip().splitlines()[-1])

    def test_pinta_el_aviso_escapado_cuando_aplica(self):
        r = self._correr({"ok": True, "cuerpo": {"ok": True, "aplica": True, "titulo": "Esta OT tiene un anexo firmado",
                                                 "lineas": ["Anexo N° 9 (<b>X</b>): quedará inválido."]}})
        self.assertIn("Esta OT tiene un anexo firmado", r["html"])
        self.assertIn("&lt;b&gt;X&lt;/b&gt;", r["html"])
        self.assertNotIn("<b>X</b>", r["html"])
        self.assertEqual(r["txt"], " Anexo N° 221: quedó inválido. Aviso al proveedor: enviado.")
        self.assertEqual(r["vacio"], "")

    def test_no_pinta_nada_si_no_aplica_o_si_falla_la_consulta(self):
        self.assertEqual(self._correr({"ok": True, "cuerpo": {"ok": True, "aplica": False}})["html"], "")
        self.assertEqual(self._correr({"ok": False, "cuerpo": {"ok": False}})["html"], "")


if __name__ == "__main__":
    unittest.main()
