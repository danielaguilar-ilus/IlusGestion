"""📜 Anexo anulado por la anulación del servicio — SEGUNDA PARTE (REGLA #52). Daniel, 2026-10-28:
  1. el aviso es SOLO TEXTO: jamás lleva el anexo (ni un PDF) adjunto;
  2. «Reenviar aviso del anexo anulado» (muestra antes a quién llega, elige proveedor y/o ILUS, agrega un correo, dice la verdad y deja constancia);
  3. un anexo ENVIADO o VISTO (sin firmar) de una OT que se elimina también queda sin efecto (su enlace deja de permitir firmar) y se avisa;
  4. CANCELAR o ANULAR una OT (sin eliminarla) dispara el mismo flujo; reactivar la OT NO revive el anexo en silencio.
Sin base ni correo reales (REGLA #22): las funciones de app.py se extraen por texto (tests/_fuente_app.py) y corren sobre dobles.
Correr con:  py -m unittest tests.test_anexo_anulacion_2
"""
import json
import os
import shutil
import subprocess
import types
import unittest

import anexo_anulacion as AX
import ot_anexo_costo as OAC
from tests import _fuente_app as F
from tests.test_anexo_anulacion_ot_eliminada import FIRMADO, FUNCIONES, Base, _Cur, _escenario_basico, _items

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FUNC2 = ("_anexo_anul_reenvio_ctx", "ot2_api_anexo_aviso_anulado", "ot2_api_anexo_reenviar_aviso", "ot2_anexos_anulados_pagina",
         "_anexo_anul_puerta_previa", "_anexo_anul_reactivada")
ITEMS_COMPARTIDO = _items(("Instalación", 370000, "entrada"), ("Desinstalación", 150000, "salida"), ("Despacho (desinstalación)", 15000, "salida"))


def _cargar(b, body=None, args=None, extra=None):
    """Las funciones de la 1.ª parte + las nuevas, sobre la base de mentira `b`."""
    env = b.entorno()
    env.update({"request": types.SimpleNamespace(get_json=lambda silent=True: body or {}, args=args or {}),
                "render_template": lambda nombre, **kw: (nombre, kw), "url_for": lambda *a, **k: "/x"})
    env.update(extra or {})
    return F.cargar(funciones=FUNCIONES + FUNC2, constantes=("_ANEXO_ANUL_SEL",), extra=env)


def _eliminar(b, vid=300):
    return _cargar(b)["_mant_visita_eliminar_core"](vid, quitar_de_factura=True)


# ════════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestPuro(unittest.TestCase):
    def test_acciones_y_motivos(self):
        self.assertEqual(AX.accion_de_estado("cancelada"), "cancelar")
        self.assertEqual(AX.accion_de_estado("anulada"), "anular")
        self.assertIsNone(AX.accion_de_estado("programada"))
        self.assertEqual(AX.motivo("cancelar"), "anulación del servicio (OT cancelada)")
        self.assertEqual(AX.motivo("anular"), "anulación del servicio (OT anulada)")
        self.assertEqual(AX.motivo("cualquier cosa"), AX.MOTIVO)
        self.assertEqual([AX.accion_de_motivo(AX.motivo(a)) for a in AX.ACCIONES], list(AX.ACCIONES))
        self.assertEqual(AX.accion_de_motivo(None), "eliminar")

    def test_el_aviso_previo_de_cancelar_advierte_la_reactivacion_y_el_de_eliminar_no(self):
        plan = {"anexos": [{"numero": 7, "proveedor": "X", "firmado_el": "01/10/2026", "destinos": ["x@x.cl"]}], "interno": []}
        canc = " ".join(AX.lineas_confirmacion(plan, accion="cancelar"))
        self.assertIn(AX.LINEA_REACTIVAR, canc)
        self.assertNotIn(AX.LINEA_REACTIVAR, " ".join(AX.lineas_confirmacion(plan, accion="eliminar")))
        for a in AX.ACCIONES:
            self.assertIn("no lleva el anexo adjunto", " ".join(AX.lineas_confirmacion(plan, accion=a)))

    def test_el_aviso_previo_de_un_anexo_enviado_dice_que_su_enlace_deja_de_funcionar(self):
        plan = {"anexos": [{"numero": 8, "proveedor": "X", "enviado_el": "19/10/2026", "sin_firma": True, "destinos": []}], "interno": []}
        txt = " ".join(AX.lineas_confirmacion(plan, accion="cancelar"))
        self.assertIn("enviado el 19/10/2026 y sin firmar", txt)
        self.assertIn("su enlace de firma dejará de funcionar", txt)

    def test_un_borrador_no_genera_aviso_previo(self):
        plan = {"anexos": [{"numero": 9, "parcial": True, "sin_aviso": True}], "interno": []}
        self.assertFalse(AX.para_pantalla(plan)["aplica"])

    def test_correo_al_proveedor_de_un_anexo_enviado_sin_firmar(self):
        a = {"numero": 8, "proveedor": "X", "enviado_el": "19/10/2026", "sin_firma": True}
        c = AX.correo_proveedor(a, "OT-2026-00310", "soportetec@sphs.cl", "cancelar")
        self.assertIn("quedó sin efecto", c["asunto"])
        self.assertIn("que te enviamos para firmar", c["mensaje"])
        self.assertIn("Ya no es posible firmarlo con el enlace que recibiste", c["mensaje"])
        self.assertEqual(dict(c["filas"])["Enviado el"], "19/10/2026")
        todo = json.dumps(c, ensure_ascii=False).lower()
        for prohibido in ("$", "monto", "margen", "costo", "adjunto"):
            self.assertNotIn(prohibido, todo)

    def test_parcial_sin_firma_dice_que_las_partidas_se_retiraron(self):
        a = {"numero": 8, "proveedor": "X", "sin_firma": True, "parcial": True, "partidas": ["Desinstalación"]}
        self.assertIn("se retiraron", AX.correo_proveedor(a, "OT-2026-00311", "s@sphs.cl")["mensaje"])
        self.assertIn("se retiraron del anexo", AX.correo_interno(a, "OT-2026-00311", "", "Daniel", "28/10/2026 10:00")["mensaje"])

    def test_el_correo_interno_de_cancelar_dice_cancelada_y_el_reenvio_dice_quien(self):
        a = {"numero": 5, "proveedor": "X", "firmado_el": "01/10/2026"}
        c = AX.correo_interno(a, "OT-2026-00300", "Instalación", "Daniel", "28/10/2026 10:00", accion="cancelar", reenvio_por="Aarón")
        f = dict(c["filas"])
        self.assertEqual((f["Cancelada por"], f["OT cancelada"], f["Motivo"]), ("Daniel", "OT-2026-00300", "anulación del servicio (OT cancelada)"))
        self.assertEqual(f["Aviso reenviado por"], "Aarón")
        self.assertIn("quedó cancelada", c["asunto"])
        p = AX.correo_proveedor(a, "OT-2026-00300", "s@sphs.cl", reenvio_por="Aarón")
        self.assertEqual(dict(p["filas"])["Aviso"], "Reenvío del aviso")

    def test_utilidades(self):
        self.assertEqual([i["concepto"] for i in AX.sin_partidas_salida(ITEMS_COMPARTIDO)], ["Instalación"])
        self.assertEqual(AX.correos_invalidos("a@x.cl, basura, b@y"), ["basura", "b@y"])
        self.assertEqual(AX.correos_invalidos(""), [])
        self.assertIn("N° 5", AX.aviso_reactivada(["5"]))
        self.assertIn("un anexo nuevo", AX.aviso_reactivada(["5"]))
        self.assertIn("anexos nuevos", AX.aviso_reactivada(["5", "6"]))
        self.assertEqual(AX.lineas_resultado([{"numero": 1, "sin_firma": True, "proveedor": {"texto": "x"}, "interno": {"texto": "y"}}])[0],
                         "Anexo N° 1: quedó sin efecto por la anulación del servicio (su enlace de firma ya no sirve).")


# ════════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestAnexoEnviadoOVisto(unittest.TestCase):
    """Punto 3: el anexo que se le mandó al proveedor para firmar también queda sin efecto al eliminar la OT."""

    def test_eliminar_con_anexo_enviado_o_visto_lo_anula_y_avisa_sin_adjuntos(self):
        for est in ("enviado", "visto"):
            b = _escenario_basico(estado=est, firmado_at=None, firmante_nombre=None)
            res = _eliminar(b)
            self.assertTrue(res["ok"], est)
            self.assertEqual(b.anexos[221]["estado"], "anulado", est)
            self.assertIsNone(b.anexos[221]["ot_id"])
            prov, interno = b.mails
            self.assertIn("el anexo que te enviamos para firmar quedó sin efecto", prov["subject"])
            self.assertIn("Ya no es posible firmarlo", prov["html"])
            self.assertIn("sin efecto", interno["subject"])
            self.assertTrue(res["anexo_anulado"]["anexos"][0]["sin_firma"])
            for m in b.mails:
                self.assertNotIn("attachments", m, "el aviso es solo texto: nunca lleva el anexo adjunto")
            self.assertIn("quedó sin efecto por la anulación del servicio (su enlace de firma ya no sirve)", " ".join(res["anexo_anulado"]["lineas"]))

    def test_el_enlace_de_firma_publico_de_un_anexo_anulado_no_ofrece_firmar(self):
        s = F.fuente_funcion("ot2_anexo_firma_publica")
        self.assertIn('a["estado"] == "anulado"', s)
        post = F.fuente_funcion("ot2_anexo_firma_submit")
        self.assertIn("estado IN ('enviado','visto')", post, "el UPDATE atómico de la firma solo firma enviado/visto")

    def test_carrera_el_proveedor_firma_justo_antes_de_que_se_tome_el_candado(self):
        b = _escenario_basico(estado="enviado")
        b.carrera_firma = True              # entre la lectura del plan y el SELECT … FOR UPDATE el proveedor firma
        res = _eliminar(b)
        self.assertTrue(res["ok"])
        self.assertEqual(b.anexos[221]["estado"], "anulado", "no se pierde un anexo recién firmado: se anula como firmado")
        a = res["anexo_anulado"]["anexos"][0]
        self.assertFalse(a["sin_firma"])
        self.assertIn("quedó inválido", b.mails[0]["subject"])

    def test_el_candado_se_toma_dentro_de_la_transaccion_y_el_update_lleva_el_estado(self):
        s = F.fuente_funcion("_anexo_anul_marcar")
        self.assertIn("FOR UPDATE", s)
        self.assertIn("estado IN", s)
        b = _escenario_basico(estado="enviado")
        _eliminar(b)
        sqls = [x for x, _ in b.sqls]
        i_lock = [i for i, x in enumerate(sqls) if "FOR UPDATE" in x][0]
        i_del = [i for i, x in enumerate(sqls) if x.startswith("DELETE FROM mant_visitas")][0]
        self.assertLess(i_lock, i_del)

    def test_una_desinstalacion_con_anexo_comun_sin_firmar_pierde_sus_partidas_y_se_avisa(self):
        b = _escenario_basico(estado="enviado", firmado_at=None, precio_items_json=ITEMS_COMPARTIDO)
        b.visita(301, "OT-2026-00301", ot_madre_id=300, ot_relacion="desinstalacion")
        res = _eliminar(b, vid=301)
        self.assertTrue(res["ok"])
        ax = b.anexos[221]
        self.assertEqual((ax["estado"], ax["ot_id"]), ("enviado", 300))
        self.assertEqual([i["concepto"] for i in OAC.items_de(ax["precio_items_json"])], ["Instalación"])
        self.assertIn("se retiraron", b.mails[0]["html"])
        self.assertTrue(res["anexo_anulado"]["anexos"][0]["parcial"])

    def test_una_desinstalacion_con_anexo_comun_en_borrador_pierde_las_partidas_en_silencio(self):
        b = _escenario_basico(estado="borrador", firmado_at=None, precio_items_json=ITEMS_COMPARTIDO)
        b.visita(301, "OT-2026-00301", ot_madre_id=300, ot_relacion="desinstalacion")
        res = _eliminar(b, vid=301)
        self.assertTrue(res["ok"])
        self.assertEqual([i["concepto"] for i in OAC.items_de(b.anexos[221]["precio_items_json"])], ["Instalación"])
        self.assertEqual(b.mails, [], "un borrador nunca enviado no se avisa")


# ════════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestCancelarYAnular(unittest.TestCase):
    """Punto 4: cancelar o anular una OT (sin eliminarla) dispara el mismo flujo."""

    def test_sin_confirmar_el_servidor_devuelve_el_aviso_previo_y_no_cambia_nada(self):
        b = _escenario_basico()
        plan, lote, resp = _cargar(b)["_anexo_anul_puerta_previa"](300, {"estado": "cancelada"}, "cancelar")
        cuerpo, http = resp
        self.assertEqual(http, 409)
        self.assertEqual(cuerpo["error_codigo"], "ANEXO_REQUIERE_CONFIRMACION")
        self.assertTrue(cuerpo["puede_confirmar_anexo"])
        lineas = " ".join(cuerpo["anexo_aviso"]["lineas"])
        self.assertIn("quedará inválido por la anulación del servicio", lineas)
        self.assertIn(AX.LINEA_REACTIVAR, lineas, "si la OT puede reactivarse, el aviso previo lo dice")
        self.assertIn("Anexo N° 221", cuerpo["error"], "una pantalla que no sabe de esto igual muestra qué pasa")
        self.assertEqual(b.sqls, [], "nada se tocó")

    def test_confirmado_deja_pasar_y_trae_el_lote(self):
        b = _escenario_basico()
        b.fac = {"id": 8, "numero_documento": None, "proveedor_nombre": "Milling", "estado_pago": "pagada"}
        plan, lote, resp = _cargar(b)["_anexo_anul_puerta_previa"](300, {"estado": "anulada", "confirmar_anexo": 1}, "anular")
        self.assertIsNone(resp)
        self.assertEqual(lote["id"], 8)
        self.assertEqual(len(plan["anexos"]), 1)

    def test_sin_anexo_vivo_no_pide_nada(self):
        b = _escenario_basico(estado="borrador")
        plan, lote, resp = _cargar(b)["_anexo_anul_puerta_previa"](300, {"estado": "cancelada"}, "cancelar")
        self.assertIsNone(resp)
        self.assertEqual(plan["anexos"], [])

    def test_falla_cerrado_si_no_se_puede_revisar(self):
        b = _escenario_basico()
        b.plan_falla = True
        plan, lote, resp = _cargar(b)["_anexo_anul_puerta_previa"](300, {"estado": "cancelada", "confirmar_anexo": 1}, "cancelar")
        self.assertEqual(resp[1], 409)
        self.assertEqual(resp[0]["error_codigo"], "ANEXO_NO_VERIFICABLE")

    def test_cancelar_anula_el_anexo_dentro_de_la_transaccion_sin_perder_la_ot_y_avisa(self):
        for est, accion, verbo in (("firmado", "cancelar", "cancelada"), ("enviado", "anular", "anulada")):
            b = _escenario_basico(estado=est)
            b.fac = {"id": 8, "numero_documento": None, "proveedor_nombre": "Milling", "estado_pago": "pendiente"}
            amb = _cargar(b)
            plan = amb["_anexo_anul_plan"](300)
            hechos = amb["_anexo_anul_marcar"](_Cur(b), plan, plan["numero_ot"], "Daniel Aguilar", 300, accion)
            self.assertEqual(hechos["anuladas"], {221: est})
            ax = b.anexos[221]
            self.assertEqual((ax["estado"], ax["ot_id"]), ("anulado", 300), "la OT sigue existiendo: el anexo queda ligado a ella")
            self.assertEqual(ax["anulado_motivo"], f"anulación del servicio (OT {verbo})")
            av = amb["_anexo_anul_avisar"](300, plan, hechos, plan["numero_ot"], "Daniel Aguilar", b.fac, accion)
            self.assertEqual(len(b.mails), 2)
            self.assertIn(f"OT {verbo}", b.mails[1]["html"])
            self.assertIn("sigue en el lote #8 (pendiente)", b.mails[1]["html"])
            self.assertTrue(any(a == f"anulado_ot_{verbo}" for e, i, a, *_ in b.logs))
            self.assertTrue(any((e, i, a) == ("factura_proveedor", 8, f"anexo_anulado_ot_{verbo}") for e, i, a, *_ in b.logs))
            self.assertEqual(len(av["anexos"]), 1)
            # volver a cancelar la OT ya no encuentra nada vivo: no se avisa dos veces
            self.assertEqual(amb["_anexo_anul_plan"](300)["anexos"], [])

    def test_la_hija_cancelada_en_cascada_solo_anula_sus_anexos_propios(self):
        b = _escenario_basico(precio_items_json=ITEMS_COMPARTIDO)
        b.visita(301, "OT-2026-00301", ot_madre_id=300, ot_relacion="desinstalacion")
        amb = _cargar(b)
        self.assertEqual(amb["_anexo_anul_plan"](301, solo_propios=True)["anexos"], [])
        self.assertEqual([a["parcial"] for a in amb["_anexo_anul_plan"](301)["anexos"]], [True])

    def test_cancelar_la_desinstalacion_que_comparte_el_anexo_firmado_no_lo_invalida(self):
        b = _escenario_basico(precio_items_json=ITEMS_COMPARTIDO)
        b.visita(301, "OT-2026-00301", ot_madre_id=300, ot_relacion="desinstalacion")
        amb = _cargar(b)
        plan = amb["_anexo_anul_plan"](301)
        hechos = amb["_anexo_anul_marcar"](_Cur(b), plan, "OT-2026-00301", "Daniel", 301, "cancelar")
        self.assertEqual((hechos["anuladas"], hechos["parciales"]), ({}, {221: "firmado"}))
        self.assertEqual(b.anexos[221]["estado"], "firmado")
        amb["_anexo_anul_avisar"](301, plan, hechos, "OT-2026-00301", "Daniel", None, "cancelar")
        self.assertIn("quedan sin efecto", b.mails[0]["html"])

    def test_reactivar_la_ot_no_revive_el_anexo_y_lo_dice(self):
        b = _escenario_basico(estado="anulado", anulado_at=FIRMADO)
        amb = _cargar(b, extra={"mysql_fetchall": lambda sql, p=None: [{"numero": 221}] if "estado='anulado' AND anulado_at IS NOT NULL" in sql else []})
        txt = amb["_anexo_anul_reactivada"](300, "cancelada", "programada")
        self.assertIn("N° 221", txt)
        self.assertIn("hay que emitir un anexo nuevo", txt)
        self.assertTrue(any((e, i, a) == ("visita", 300, "anexo_sigue_anulado") for e, i, a, *_ in b.logs))
        self.assertEqual(b.anexos[221]["estado"], "anulado", "sigue anulado")
        # no corresponde: no sale de cancelada/anulada, o sigue cancelada
        self.assertIsNone(amb["_anexo_anul_reactivada"](300, "programada", "reagendada"))
        self.assertIsNone(amb["_anexo_anul_reactivada"](300, "cancelada", "anulada"))
        self.assertIsNone(amb["_anexo_anul_reactivada"](300, "cancelada", ""))

    def test_el_put_de_la_ot_es_la_unica_puerta_y_el_cableado_esta_en_orden(self):
        s = F.fuente_funcion("mant_visita_update")
        self.assertLess(s.index("_anexo_anul_puerta_previa(vid, d, _accion_ax)"), s.index("conn = get_mysql()"),
                        "el aviso previo se exige ANTES de abrir la transacción")
        i_upd = s.index("cur.execute(f\"UPDATE mant_visitas SET {','.join(sets)} WHERE id=%s\"")
        i_marcar, i_commit = s.index("_anexo_anul_marcar(cur, _ax_plan_c"), s.index("conn.commit()")
        self.assertLess(i_upd, i_marcar, "los anexos se anulan en la MISMA transacción que el estado de la OT")
        self.assertLess(i_marcar, i_commit)
        self.assertLess(i_commit, s.index("_anexo_anul_avisar(vid, _ax_plan_c"), "los correos salen DESPUÉS del commit")
        self.assertIn("_anexo_anul_reactivada(vid, _estado_prev_audit, _estado_nuevo_dm)", s)
        self.assertIn('resp["anexo_anulado"]', s)
        # ningún otro camino escribe esos estados
        self.assertIn("_anexo_anul_plan(int(_cascada_hija[\"id\"]), solo_propios=True)", s)


# ════════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestReenviarAviso(unittest.TestCase):
    """Punto 2: «Reenviar aviso del anexo anulado»."""

    def _anulado(self, ot_viva=True, **kw):
        datos = dict(anulado_at=FIRMADO, anulado_por="Aarón Rojas", anulado_motivo="anulación del servicio (OT cancelada)",
                     anulado_ot_numero="OT-2026-00300")
        datos.update(kw)
        b = _escenario_basico(estado="anulado", ot_id=300 if ot_viva else None, **datos)
        if not ot_viva:
            b.visitas.pop(300, None)
        return b

    def test_el_aviso_previo_muestra_a_quien_llegaria_y_que_no_lleva_adjunto(self):
        b = self._anulado()
        d = _cargar(b)["ot2_api_anexo_aviso_anulado"](221)
        self.assertTrue(d["ok"])
        self.assertEqual(d["proveedor"]["destinos"], ["contacto@milling.cl", "facturacion@milling.cl"])
        self.assertEqual([p["correo"] for p in d["interno"]["personas"]], ["aaron.rojas@sphs.cl", "daniel.aguilar@sphs.cl"])
        self.assertIn("no lleva el anexo adjunto", d["nota"])
        self.assertEqual((d["numero"], d["anulado_por"], d["ot"]["existe"]), (221, "Aarón Rojas", True))

    def test_el_aviso_previo_dice_si_el_correo_esta_apagado_o_no_hay_correo(self):
        b = self._anulado(enviado_email=None)
        b.ficha.clear()
        amb = _cargar(b, extra={"_correo_bloqueo_previo": lambda m, c=None: "correo" if c == "anexo_anulado_interno" else None})
        d = amb["ot2_api_anexo_aviso_anulado"](221)
        self.assertEqual(d["proveedor"]["destinos"], [])
        self.assertIn("bloqueo correo", d["interno"]["apagado"])

    def test_un_anexo_que_no_esta_anulado_no_se_reenvia(self):
        b = _escenario_basico()
        for fn, args in (("ot2_api_anexo_aviso_anulado", (221,)), ("ot2_api_anexo_reenviar_aviso", (221,))):
            cuerpo, http = _cargar(b, body={"proveedor": True})[fn](*args)
            self.assertEqual((http, cuerpo["error_codigo"]), (409, "ANEXO_NO_ANULADO"))
        self.assertEqual(b.mails, [])

    def test_reenviar_a_las_dos_partes_dice_la_verdad_deja_constancia_y_no_adjunta(self):
        b = self._anulado()
        cuerpo, http = _cargar(b, body={"proveedor": True, "interno": True, "extra": "otro@milling.cl"})["ot2_api_anexo_reenviar_aviso"](221)
        self.assertEqual(http, 200)
        self.assertTrue(cuerpo["ok"])
        prov, interno = b.mails
        self.assertEqual((prov["to"], prov["cc"]), ("contacto@milling.cl", ["facturacion@milling.cl", "otro@milling.cl"]))
        self.assertEqual((prov["correo"], prov["modulo"]), ("anexo_anulado_proveedor", "general"))
        self.assertEqual((interno["correo"], interno["modulo"]), ("anexo_anulado_interno", "comunicacion_interna"))
        self.assertIn("Aviso: Reenvío del aviso", prov["html"])
        self.assertIn("Aviso reenviado por: Daniel Aguilar", interno["html"])
        self.assertIn("quedó cancelada", interno["subject"], "el texto sigue la acción con que se anuló")
        for m in b.mails:
            self.assertNotIn("attachments", m)
        self.assertIn("enviado a contacto@milling.cl", cuerpo["lineas"][0])
        reg = [d for e, i, a, d in b.logs if (e, i, a) == ("anexo", 221, "aviso_anulado_reenviado")][0]
        self.assertIn("Daniel Aguilar reenvió el aviso del Anexo N° 221 el 28/10/2026 10:30", reg)
        self.assertIn("otro@milling.cl", reg)
        self.assertTrue(any((e, i, a) == ("visita", 300, "anexo_aviso_reenviado") for e, i, a, *_ in b.logs))

    def test_solo_a_ilus_y_solo_a_un_correo_extra(self):
        b = self._anulado()
        cuerpo, http = _cargar(b, body={"proveedor": False, "interno": True})["ot2_api_anexo_reenviar_aviso"](221)
        self.assertEqual((http, [m["correo"] for m in b.mails]), (200, ["anexo_anulado_interno"]))
        self.assertIn("no se reenvió al proveedor", cuerpo["proveedor"]["texto"])
        b2 = self._anulado()
        cuerpo, http = _cargar(b2, body={"extra": "nuevo@milling.cl"})["ot2_api_anexo_reenviar_aviso"](221)
        self.assertEqual([(m["to"], m["correo"]) for m in b2.mails], [("nuevo@milling.cl", "anexo_anulado_proveedor")])

    def test_validaciones_correo_invalido_y_sin_destinatarios(self):
        b = self._anulado()
        cuerpo, http = _cargar(b, body={"proveedor": True, "extra": "esto-no-es-correo"})["ot2_api_anexo_reenviar_aviso"](221)
        self.assertEqual((http, cuerpo["error_codigo"]), (400, "CORREO_INVALIDO"))
        cuerpo, http = _cargar(b, body={})["ot2_api_anexo_reenviar_aviso"](221)
        self.assertEqual((http, cuerpo["error_codigo"]), (400, "SIN_DESTINATARIOS"))
        self.assertEqual(b.mails, [])

    def test_si_ningun_correo_sale_responde_502_con_la_verdad(self):
        b = self._anulado()
        b.send_ok = False
        cuerpo, http = _cargar(b, body={"proveedor": True, "interno": True})["ot2_api_anexo_reenviar_aviso"](221)
        self.assertEqual(http, 502)
        self.assertFalse(cuerpo["ok"])
        self.assertIn("NO salió (el envío de correos de General está apagado)", cuerpo["lineas"][0])

    def test_un_anexo_huerfano_de_una_ot_eliminada_se_reenvia_sin_ot(self):
        b = self._anulado(ot_viva=False, anulado_motivo="anulación del servicio (OT eliminada)")
        amb = _cargar(b, body={"proveedor": True, "interno": True})
        previo = amb["ot2_api_anexo_aviso_anulado"](221)
        self.assertEqual((previo["ot"]["existe"], previo["ot"]["numero"]), (False, "OT-2026-00300"))
        cuerpo, http = amb["ot2_api_anexo_reenviar_aviso"](221)
        self.assertEqual(http, 200)
        self.assertIn("OT-2026-00300", b.mails[0]["html"])
        self.assertFalse(any(e == "visita" for e, *_ in b.logs), "no hay OT donde anotar: queda en la bitácora del anexo")

    def test_las_rutas_son_solo_para_gestion_nunca_un_tecnico(self):
        for fn in ("ot2_api_anexo_aviso_anulado", "ot2_api_anexo_reenviar_aviso", "ot2_anexos_anulados_pagina"):
            d = F.decoradores(fn)
            self.assertIn("@_no_tecnico", d, fn)
            self.assertIn("@_mant_required", d, fn)
        self.assertTrue(any('"/ot/api/anexos/<int:aid>/reenviar-aviso"' in x and '"POST"' in x for x in F.decoradores("ot2_api_anexo_reenviar_aviso")))

    def test_ninguna_funcion_del_aviso_adjunta_nada(self):
        for fn in ("_anexo_anul_enviar", "_anexo_anul_avisar", "ot2_api_anexo_reenviar_aviso", "_anexo_anul_html"):
            self.assertNotIn("attachments", F.fuente_funcion(fn).replace("SIN attachments", ""), fn)
            self.assertNotIn("_anexo_pdf_bytes", F.fuente_funcion(fn), fn)


class TestListado(unittest.TestCase):
    def test_el_listado_trae_tambien_los_huerfanos_y_pagina(self):
        b = Base()
        filas = [{"id": 221, "numero": 221, "proveedor_nombre": "Milling", "ot_id": None, "firmado_at": FIRMADO, "enviado_at": None,
                  "updated_at": None, "anulado_at": FIRMADO, "anulado_por": "Daniel", "anulado_motivo": "anulación del servicio (OT eliminada)",
                  "anulado_ot_numero": "OT-2026-00300", "ot_numero_actual": None, "ot_estado": None}]
        amb = _cargar(b, args={"page": "1", "per_page": "25"}, extra={
            "mysql_fetchone": lambda sql, p=None: {"n": 1}, "mysql_fetchall": lambda sql, p=None: [dict(f) for f in filas]})
        nombre, kw = amb["ot2_anexos_anulados_pagina"]()
        self.assertEqual(nombre, "ot2/anexos_anulados.html")
        f = kw["filas"][0]
        self.assertEqual((f["ot_numero"], f["ot_existe"], f["tenia_firma"]), ("OT-2026-00300", False, True))
        self.assertEqual((kw["total"], kw["page"], kw["paginas"], kw["per_page"]), (1, 1, 1, 25))
        s = F.fuente_funcion("ot2_anexos_anulados_pagina")
        self.assertIn("LEFT JOIN mant_visitas", s, "los huérfanos (ot_id NULL) no desaparecen")
        self.assertIn("LIMIT %s OFFSET %s", s)

    def test_la_plantilla_compila_y_tiene_el_pie_de_paginacion_y_el_boton(self):
        t = F.texto("templates/ot2/anexos_anulados.html")
        from jinja2 import Environment
        Environment().parse(t)
        for pieza in ("Mostrando", "por página", "Anterior", "Siguiente", "otAnexoAviso.abrir(", "Reenviar aviso del anexo anulado"):
            self.assertIn(pieza, t)


class TestFrente(unittest.TestCase):
    def test_la_ficha_de_la_ot_muestra_el_anexo_anulado_y_no_lo_revive(self):
        t = F.texto("templates/ot2/detalle.html")
        self.assertIn("{% elif anexo.estado == 'anulado' %}", t)
        self.assertIn("otAnexoAviso.abrir({{ anexo.id }})", t)
        self.assertIn("no se reactiva solo", t)
        self.assertIn("Si reactivas esta OT, el anexo seguirá anulado", t)
        from jinja2 import Environment
        Environment().parse(t)
        self.assertIn("ot_anexo_aviso.js", F.texto("templates/base.html"))

    def test_cancelar_pasa_por_ilusConfirm_y_reenvia_con_confirmar_anexo(self):
        js = F.texto("static/ot_cancelar.js")
        self.assertIn("ANEXO_REQUIERE_CONFIRMACION", js)
        self.assertIn("window.ilusConfirm", js)
        self.assertIn("confirmar_anexo: 1", js)
        self.assertIn("d.anexo_reactivada", js)

    def test_el_modal_del_reenvio_cumple_las_reglas_de_la_interfaz(self):
        js = F.texto("static/ot_anexo_aviso.js")
        self.assertIn("ilusConfirm", js)                               # REGLA #1: la confirmación no es nativa
        self.assertIn("Marcar / desmarcar todo", js)                   # REGLA #14
        self.assertIn("min-height:44px", js)                           # REGLA #3
        self.assertIn("font-size:16px", js)
        self.assertIn("no lleva el anexo adjunto", js)
        for prohibido in (" alert(", " confirm(", " prompt("):
            self.assertNotIn(prohibido, js)

    def test_el_panel_enlaza_la_pantalla_de_anexos_anulados(self):
        self.assertIn("ot2_anexos_anulados_pagina", F.texto("templates/ot2/panel.html"))

    @unittest.skipUnless(shutil.which("node"), "Node no está instalado")
    def test_la_sintaxis_de_los_js(self):
        for f in ("static/ot_anexo_aviso.js", "static/ot_cancelar.js", "static/ot_anexo_anulacion.js"):
            out = subprocess.run(["node", "--check", os.path.join(RAIZ, *f.split("/"))], capture_output=True, text=True, encoding="utf-8")
            self.assertEqual(out.returncode, 0, out.stderr)


if __name__ == "__main__":
    unittest.main()
