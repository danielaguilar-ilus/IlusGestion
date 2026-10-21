# -*- coding: utf-8 -*-
"""COMUNICACIÓN DE LA ORDEN DE TRABAJO (Daniel, 20-oct-2026) — «OT terminada» automática, «Enviar OT por correo» manual y destinatarios a
la vista (REGLA #37).

Daniel: «necesito que esto sea CONTROLABLE, que avises cuándo lo tienes que enviar y además habilita, así como las cotizaciones, un envío por
mail directo de las órdenes de trabajo… y quiero que avise a quién se le está enviando: al momento de crearse y enviarse los correos se vaya
educando el personal de cuándo genera comunicación tanto con los técnicos como con los clientes».

Decisiones que se vigilan:
  · OT terminada, AUTOMÁTICA, al APROBAR el cierre (la OT ya es definitiva), una sola vez por aprobación: al cliente, a quien creó la OT / el
    ejecutivo / el responsable (los decide `_ot_gestion_destinatarios`, la ÚNICA función que sabe a quién avisa la gestión) y al técnico que la
    hizo (copia SIN montos ni proveedores: REGLAS #19 y #26).
  · El correo al CLIENTE NACE APAGADO y solo Daniel lo enciende (variable OT_TERMINADA_CLIENTE_ACTIVA; REGLA #22): nada retroactivo, y la
    variable NO se enciende en deploy.yml ni en ningún archivo del repositorio.
  · Alerta previa (técnico al firmar, gestión al aprobar) y al crear la OT: «Al … se enviará esta OT a: …», marcando lo apagado.
  · «Enviar OT por correo» (solo gestión, como Cotizaciones): destinatarios prellenados y editables, la verdad por destinatario («Se envió a…» /
    «No salió porque…»), un técnico SIEMPRE por su llave 'ot_tecnicos' y sin plata. Queda en la bitácora y en mant_ot_comunicaciones.
  · Recordatorio ámbar en la ficha de una OT terminada cuyo aviso al cliente NO salió (nunca bloquea; sin antecedente no se inventa).

Se prueba:
  1. El módulo puro ot_comunicacion.py (plan de destinatarios, estados, franja, manual, textos, plantillas sin plata).
  2. El registro de correos (5 claves nuevas, el del cliente nace apagado) y el editor de plantillas.
  3. Las funciones REALES de app.py en un mundo de mentira (base y correo en memoria, la puerta única de envío REAL): el aviso automático,
     el envío manual, el aviso previo, el contexto de la ficha y los avisos de la creación.
  4. Cableado: puntos de llamada, candados de las rutas, arranque barato (REGLA #18), variable apagada en el repositorio, front sin popups.
  5. La ficha SE DIBUJA (jinja2 de verdad) también en una OT CERRADA.

Sin BD, sin Flask, sin correos reales (REGLA #22). Correr:  python -m pytest tests/test_ot_comunicacion_envio.py -q -p no:warnings
"""
import datetime as dt
import json
import os
import re
import types
import unittest
from unittest import mock

import jinja2

import comm_correos_registro as R
import ot_comunicacion as C
import ot_correos_tecnicos as OTC
import ot_eventos as EVT
from tests import _fuente_codigo as FC
from tests import _mundo_correos as MC

RAIZ = FC.RAIZ


def _leer(rel):
    return FC.texto(rel)


def _txt(html):
    """El texto visible de un HTML (sin etiquetas, sin entidades)."""
    import html as _h
    return re.sub(r"\s+", " ", _h.unescape(re.sub(r"<[^>]+>", " ", html or ""))).strip()


PLATA = re.compile(r"\$\s?\d|\bcosto|\bmonto|\bprecio|\bmargen|\bproveedor|\bfactur|\bsaldo|\bcobr|\bvalorizad|\bOC\b|n[°º] de oc", re.I)


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
#  1) EL MÓDULO PURO
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
def _plan(**kw):
    base = dict(cliente_nombre="Gimnasio Demo SpA",
                cliente_correos=[{"correo": "Contacto@Gimnasio.cl", "nombre": "María", "origen": "contacto_ot"},
                                 {"correo": "admin@gimnasio.cl", "nombre": "", "origen": "ficha"}],
                aplica_cliente=True, cliente_encendido=True, documento_ok=True,
                gestion=[{"id": 2, "nombre": "Aarón Rojas", "correo": "aaron@sphs.cl", "por": "ejecutivo"},
                         {"id": 3, "nombre": "Víctor Soto", "correo": "victor@sphs.cl", "por": "creador"}],
                tecnicos={"lenin@sphs.cl": "Lenin Urbina"}, fichas=(), bloqueos={}, llaves_txt={"mantenciones": "Mantenciones",
                                                                                             "ot_tecnicos": "Correos a técnicos (OT)"},
                nombres_correo={C.CORREO_TERMINADA_CLIENTE: R.nombre_de(C.CORREO_TERMINADA_CLIENTE),
                                C.CORREO_TERMINADA_RESPONSABLE: R.nombre_de(C.CORREO_TERMINADA_RESPONSABLE),
                                C.CORREO_TERMINADA_TECNICO: R.nombre_de(C.CORREO_TERMINADA_TECNICO)})
    base.update(kw)
    return C.armar_plan_cierre(**base)


def _por(items, correo):
    return next(i for i in items if i["correo"] == correo)


class TestVariableDelCliente(unittest.TestCase):
    """El aviso al cliente NACE APAGADO (REGLA #22): solo Daniel lo enciende, con una variable de entorno."""

    def test_apagado_por_defecto_y_con_cualquier_valor_que_no_sea_un_si(self):
        self.assertFalse(C.cliente_activo({}))
        for v in ("", "0", "false", "no", "off", "  ", "talvez", "2"):
            self.assertFalse(C.cliente_activo({C.ENV_CLIENTE_ACTIVA: v}), repr(v))

    def test_se_enciende_con_los_si_de_siempre(self):
        for v in ("1", "true", "TRUE", "yes", "on", "si", "sí", " 1 "):
            self.assertTrue(C.cliente_activo({C.ENV_CLIENTE_ACTIVA: v}), repr(v))

    def test_el_codigo_y_la_pantalla_leen_la_variable_con_la_misma_regla(self):
        """Control de correos muestra «Apagado por configuración» con la MISMA condición que usa el código: nunca se contradicen."""
        c = R.por_clave(C.CORREO_TERMINADA_CLIENTE)
        for v in ("", "0", "1", "true", "si", "off", "yes"):
            esperado = R.estado_efectivo(c, environ={C.ENV_CLIENTE_ACTIVA: v})["efectivo"] == "sale"
            self.assertEqual(C.cliente_activo({C.ENV_CLIENTE_ACTIVA: v}), esperado, repr(v))

    def test_la_pantalla_no_deja_encenderlo_y_lo_dice_con_el_candado(self):
        e = R.estado_efectivo(R.por_clave(C.CORREO_TERMINADA_CLIENTE), environ={})
        self.assertEqual(e["efectivo"], "config")
        self.assertFalse(e["puede_encender"])
        self.assertTrue(e["bloquea_config"])
        self.assertTrue(any(x["variable"] == C.ENV_CLIENTE_ACTIVA and not x["activa"] for x in e["condiciones"]))

    def test_los_otros_cuatro_correos_no_dependen_de_ninguna_variable(self):
        for k in (C.CORREO_TERMINADA_RESPONSABLE, C.CORREO_TERMINADA_TECNICO, C.CORREO_MANUAL, C.CORREO_MANUAL_TECNICO):
            self.assertEqual(R.por_clave(k)["condiciones"], (), k)
            self.assertEqual(R.estado_efectivo(R.por_clave(k), environ={})["efectivo"], "sale", k)


class TestPlanDeCierre(unittest.TestCase):
    def test_el_cliente_apagado_dice_que_no_se_enviara_hasta_que_se_active(self):
        p = _plan(cliente_encendido=False)
        cli = [i for i in p if i["grupo"] == "cliente"]
        self.assertEqual({i["estado"] for i in cli}, {C.EST_APAGADO_CONFIG})
        self.assertFalse(any(i["intentar"] or i["saldra"] for i in cli))
        self.assertEqual(cli[0]["motivo"], "El correo al cliente está apagado: no se enviará hasta que se active.")
        self.assertEqual([i["correo"] for i in cli], ["contacto@gimnasio.cl", "admin@gimnasio.cl"])      # el contacto de la OT primero, en minúsculas

    def test_encendido_el_cliente_sale_y_el_resto_tambien(self):
        p = _plan()
        self.assertTrue(all(i["saldra"] and i["intentar"] for i in p))
        self.assertEqual([i["rol_txt"] for i in p], ["Cliente", "Cliente", "Ejecutivo", "Quien creó la OT", "Técnico"])

    def test_cada_grupo_se_frena_por_su_propia_puerta(self):
        p = _plan(bloqueos={"cliente": "global", "responsable": "modulo", "tecnico": "correo"})
        self.assertEqual(_por(p, "contacto@gimnasio.cl")["estado"], C.EST_GENERAL)
        self.assertEqual(_por(p, "aaron@sphs.cl")["estado"], C.EST_LLAVE)
        self.assertEqual(_por(p, "lenin@sphs.cl")["estado"], C.EST_CORREO)
        # lo frenado SÍ se intenta (la puerta lo frena y deja el rastro en el Historial) pero no saldrá
        for c in ("contacto@gimnasio.cl", "aaron@sphs.cl", "lenin@sphs.cl"):
            self.assertTrue(_por(p, c)["intentar"], c)
            self.assertFalse(_por(p, c)["saldra"], c)
        self.assertIn("interruptor general", _por(p, "contacto@gimnasio.cl")["motivo"])
        self.assertIn("Mantenciones", _por(p, "aaron@sphs.cl")["motivo"])
        self.assertIn("«OT terminada: copia para el técnico que la hizo (sin montos)»", _por(p, "lenin@sphs.cl")["motivo"])

    def test_un_correo_recibe_un_solo_aviso_cliente_gana_a_gestion_y_gestion_al_tecnico(self):
        p = _plan(gestion=[{"id": 2, "nombre": "Aarón", "correo": "contacto@gimnasio.cl", "por": "ejecutivo"},
                           {"id": 3, "nombre": "Lenin", "correo": "lenin@sphs.cl", "por": "creador"}])
        self.assertEqual([i["correo"] for i in p].count("contacto@gimnasio.cl"), 1)
        self.assertEqual(_por(p, "contacto@gimnasio.cl")["grupo"], "cliente")
        self.assertEqual(_por(p, "lenin@sphs.cl")["grupo"], "responsable")      # ya recibe el aviso de gestión: no se le repite como técnico

    def test_el_rol_de_gestion_sale_de_por_y_lo_desconocido_es_gestion(self):
        p = _plan(gestion=[{"id": 1, "nombre": "A", "correo": "a@sphs.cl", "por": "responsable"},
                           {"id": 2, "nombre": "B", "correo": "b@sphs.cl", "por": "respaldo"},
                           {"id": 3, "nombre": "C", "correo": "c@sphs.cl", "por": "inventado"}])
        self.assertEqual([i["rol"] for i in p if i["grupo"] == "responsable"], ["responsable", "respaldo", "respaldo"])

    def test_alguien_de_gestion_sin_correo_se_muestra_sin_correo_y_no_se_intenta(self):
        p = _plan(gestion=[{"id": 2, "nombre": "Aarón Rojas", "correo": "", "por": "ejecutivo"}])
        i = next(x for x in p if x["grupo"] == "responsable")
        self.assertEqual((i["estado"], i["intentar"], i["correo"]), (C.EST_SIN_CORREO, False, ""))
        self.assertEqual(i["motivo"], "No tiene un correo registrado.")

    def test_cliente_sin_correo_valido(self):
        p = _plan(cliente_correos=[{"correo": "no-es-un-correo", "nombre": "x"}, {"correo": "", "nombre": ""}])
        cli = [i for i in p if i["grupo"] == "cliente"]
        self.assertEqual(len(cli), 1)
        self.assertEqual((cli[0]["estado"], cli[0]["intentar"]), (C.EST_SIN_CORREO, False))

    def test_un_trabajo_interno_no_tiene_cliente_y_una_desinstalacion_dice_por_que(self):
        interna = _plan(aplica_cliente=False)
        self.assertFalse([i for i in interna if i["grupo"] == "cliente"])
        hija = _plan(aplica_cliente=False, motivo_no_cliente="La desinstalación no le escribe al cliente.")
        cli = [i for i in hija if i["grupo"] == "cliente"]
        self.assertEqual([(i["estado"], i["intentar"], i["correo"]) for i in cli], [(C.EST_NO_APLICA, False, "")])

    def test_documento_incompleto_no_se_envia_al_cliente_pero_el_resto_si(self):
        p = _plan(documento_ok=False)
        self.assertEqual({i["estado"] for i in p if i["grupo"] == "cliente"}, {C.EST_SIN_DOCUMENTO})
        self.assertTrue(all(i["saldra"] for i in p if i["grupo"] != "cliente"))

    def test_plantilla_del_tecnico_apagada_no_sale(self):
        p = _plan(plantilla_tecnico_apagada=True)
        t = _por(p, "lenin@sphs.cl")
        self.assertEqual((t["estado"], t["intentar"]), (C.EST_PLANTILLA, False))
        self.assertIn("plantilla", t["motivo"])
        self.assertTrue(_por(p, "aaron@sphs.cl")["saldra"])

    def test_la_empresa_externa_se_marca_proveedor_y_un_tecnico_no_la_ve(self):
        p = _plan(tecnicos={"lenin@sphs.cl": "Lenin Urbina", "contacto@milling.cl": "Milling SpA"}, fichas={"contacto@milling.cl"})
        self.assertTrue(_por(p, "contacto@milling.cl")["proveedor"])
        para_tec = C.para_tecnico(p)
        self.assertNotIn("contacto@milling.cl", [i["nombre"] for i in para_tec] + [i["correo"] for i in para_tec])
        self.assertNotIn("Milling SpA", json.dumps(para_tec, ensure_ascii=False))
        self.assertEqual({i["correo"] for i in para_tec}, {""})                                  # y a nadie se le ve el correo

    def test_a_intentar_filtra_por_grupo(self):
        p = _plan(cliente_encendido=False)
        self.assertEqual({i["grupo"] for i in C.a_intentar(p)}, {"responsable", "tecnico"})
        self.assertEqual([i["correo"] for i in C.a_intentar(p, "tecnico")], ["lenin@sphs.cl"])

    def test_notas_y_claves_de_creacion(self):
        p = _plan(gestion=(), claves={"cliente": "ot_creada_cliente", "tecnico": "ot_tec_asignada"},
                  nombres_correo={"ot_creada_cliente": "OT creada: aviso de la visita agendada al cliente",
                                  "ot_tec_asignada": "OT asignada: aviso al técnico"},
                  bloqueos={"tecnico": "correo"}, notas={"cliente": "recibe la visita agendada", "tecnico": "recibe el calendario"})
        self.assertIn("«OT asignada: aviso al técnico»", _por(p, "lenin@sphs.cl")["motivo"])
        self.assertEqual(_por(p, "contacto@gimnasio.cl")["nota"], "recibe la visita agendada")


class TestTextoDelAviso(unittest.TestCase):
    def test_lista_a_quien_se_enviara_con_su_rol(self):
        t = C.resumen_se_enviara(_plan())
        self.assertTrue(t.startswith("Al aprobar el cierre se enviará esta OT a: María (Cliente) · "), t)
        for frag in ("Aarón Rojas (Ejecutivo)", "Víctor Soto (Quien creó la OT)", "Lenin Urbina (Técnico)"):
            self.assertIn(frag, t)

    def test_marca_lo_apagado_y_no_lo_lista_como_destinatario(self):
        t = C.resumen_se_enviara(_plan(cliente_encendido=False))
        self.assertNotIn("(Cliente)", t.split(". Cliente:")[0])
        self.assertIn("Cliente: El correo al cliente está apagado: no se enviará hasta que se active.", t)

    def test_sin_nadie_lo_dice(self):
        t = C.resumen_se_enviara(_plan(cliente_encendido=False, gestion=(), tecnicos={}))
        self.assertTrue(t.startswith("Al aprobar el cierre no se enviará ningún correo."), t)

    def test_un_motivo_se_dice_una_vez_por_grupo_y_el_sin_correo_por_persona(self):
        p = _plan(bloqueos={"responsable": "correo"},
                  gestion=[{"id": 1, "nombre": "A", "correo": "a@sphs.cl", "por": "ejecutivo"},
                           {"id": 2, "nombre": "B", "correo": "b@sphs.cl", "por": "creador"},
                           {"id": 3, "nombre": "C", "correo": "", "por": "respaldo"}, {"id": 4, "nombre": "D", "correo": "", "por": "respaldo"}])
        t = C.resumen_se_enviara(p)
        self.assertEqual(t.count("Equipo de gestión: El correo «"), 1)
        self.assertIn("C (Gestión): No tiene un correo registrado.", t)
        self.assertIn("D (Gestión): No tiene un correo registrado.", t)

    def test_titulo_de_la_creacion(self):
        self.assertTrue(C.resumen_se_enviara(_plan(), titulo="Al crear esta OT se enviará").startswith("Al crear esta OT se enviará: "))


class TestEstadoDelAvisoAlCliente(unittest.TestCase):
    def _con(self, plan, resultado=None):
        for i in plan:
            if resultado and i["intentar"] and i["grupo"] == "cliente":
                i["resultado"] = resultado
        return C.estado_cliente(plan)

    def test_todas_las_salidas(self):
        self.assertEqual(self._con(_plan(), C.RES_ENVIADO), C.CLI_ENVIADO)
        self.assertEqual(self._con(_plan(cliente_encendido=False)), C.CLI_APAGADO)
        self.assertEqual(self._con(_plan(documento_ok=False)), C.CLI_SIN_DOCUMENTO)
        self.assertEqual(self._con(_plan(cliente_correos=[])), C.CLI_SIN_CORREO)
        self.assertEqual(self._con(_plan(bloqueos={"cliente": "modulo"}), C.RES_FRENADO), C.CLI_FRENADO)
        self.assertEqual(self._con(_plan(), C.RES_FALLO), C.CLI_FALLO)
        self.assertEqual(self._con(_plan(aplica_cliente=False)), C.CLI_NO_APLICA)
        self.assertEqual(self._con(_plan(aplica_cliente=False, motivo_no_cliente="x")), C.CLI_NO_APLICA)
        self.assertEqual(self._con(_plan()), C.CLI_PENDIENTE)           # reservado pero el trabajo se cortó antes de terminar

    def test_basta_que_una_direccion_la_reciba(self):
        p = _plan()
        cli = [i for i in p if i["grupo"] == "cliente"]
        cli[0]["resultado"], cli[1]["resultado"] = C.RES_FRENADO, C.RES_ENVIADO
        self.assertEqual(C.estado_cliente(p), C.CLI_ENVIADO)


class TestFranjaPendiente(unittest.TestCase):
    CLAVE = "2026-10-21 14:30:00"

    def _fila(self, id_, tipo, estado, clave=None, detalle=None):
        return {"id": id_, "tipo": tipo, "cierre_clave": clave, "cliente_estado": estado, "detalle_json": detalle or "[]"}

    def test_sin_antecedente_no_se_inventa_nada(self):
        self.assertIsNone(C.franja_pendiente([], self.CLAVE))
        self.assertIsNone(C.franja_pendiente([self._fila(1, "manual", "enviado")], self.CLAVE))
        self.assertIsNone(C.franja_pendiente([self._fila(1, "terminada", "apagado", "OTRA CLAVE")], self.CLAVE))      # otra aprobación del cierre

    def test_apagado_recuerda_con_texto_y_titulo(self):
        f = C.franja_pendiente([self._fila(5, "terminada", "apagado", self.CLAVE)], self.CLAVE)
        self.assertEqual(f["estado"], "apagado")
        self.assertEqual(f["titulo"], "La OT terminada todavía no se le envió al cliente")
        self.assertEqual(f["detalle"], "El envío automático al cliente está apagado.")

    def test_cada_estado_que_falta_enviar_recuerda(self):
        for est in (C.CLI_APAGADO, C.CLI_FRENADO, C.CLI_SIN_CORREO, C.CLI_SIN_DOCUMENTO, C.CLI_FALLO, C.CLI_PENDIENTE):
            self.assertIsNotNone(C.franja_pendiente([self._fila(5, "terminada", est, self.CLAVE)], self.CLAVE), est)

    def test_si_ya_se_envio_o_no_aplica_no_recuerda(self):
        for est in (C.CLI_ENVIADO, C.CLI_NO_APLICA):
            self.assertIsNone(C.franja_pendiente([self._fila(5, "terminada", est, self.CLAVE)], self.CLAVE), est)

    def test_un_envio_manual_posterior_al_cliente_la_apaga_pero_uno_anterior_o_a_otro_no(self):
        auto = self._fila(5, "terminada", "apagado", self.CLAVE)
        self.assertIsNone(C.franja_pendiente([self._fila(9, "manual", "enviado"), auto], self.CLAVE))
        self.assertIsNotNone(C.franja_pendiente([self._fila(9, "manual", "frenado"), auto], self.CLAVE))
        self.assertIsNotNone(C.franja_pendiente([self._fila(9, "manual", ""), auto], self.CLAVE))       # se envió, pero no iba el cliente
        self.assertIsNotNone(C.franja_pendiente([auto, self._fila(2, "manual", "enviado")], self.CLAVE))  # un manual ANTERIOR al aviso no cuenta

    def test_si_quedo_frenado_dice_por_que(self):
        det = json.dumps([{"grupo": "cliente", "resultado": "frenado", "motivo": "El envío de correos de Mantenciones está apagado."}])
        f = C.franja_pendiente([self._fila(5, "terminada", "frenado", self.CLAVE, det)], self.CLAVE)
        self.assertEqual(f["detalle"], "El envío al cliente quedó frenado: El envío de correos de Mantenciones está apagado.")

    def test_acepta_el_detalle_como_texto_roto(self):
        f = C.franja_pendiente([self._fila(5, "terminada", "frenado", self.CLAVE, "{no es json")], self.CLAVE)
        self.assertEqual(f["detalle"], "El envío al cliente quedó frenado.")


class TestDocumentoYClave(unittest.TestCase):
    def test_cierre_clave(self):
        self.assertEqual(C.cierre_clave(dt.datetime(2026, 10, 21, 14, 30, 5, 999)), "2026-10-21 14:30:05")
        self.assertEqual(C.cierre_clave("2026-10-21 14:30:05.123456"), "2026-10-21 14:30:05")
        self.assertEqual(C.cierre_clave(None), "0")

    def test_documento_listo_exige_cerrada_y_las_tres_firmas(self):
        f = {"estado": "cerrada", "firma_cliente_url": "c", "firma_tecnico_url": "t", "firma_supervisor_url": "s"}
        self.assertEqual(C.documento_listo(f), (True, ""))
        for k, frag in (("firma_cliente_url", "cliente"), ("firma_tecnico_url", "técnico"), ("firma_supervisor_url", "supervisor")):
            g = dict(f, **{k: None})
            ok, motivo = C.documento_listo(g)
            self.assertFalse(ok)
            self.assertIn(frag, motivo)
        self.assertFalse(C.documento_listo(dict(f, estado="pendiente_aprobacion"))[0])

    def test_un_trabajo_interno_no_necesita_la_firma_del_cliente(self):
        f = {"estado": "cerrada", "firma_cliente_url": None, "firma_tecnico_url": "t", "firma_supervisor_url": "s"}
        self.assertFalse(C.documento_listo(f)[0])
        self.assertTrue(C.documento_listo(f, es_interna=True)[0])

    def test_es_el_mismo_criterio_que_la_pagina_publica(self):
        """Espejo barato de `_ot_pdf_context` (la fuente de verdad de /ot-firmada/<token>): si ese criterio cambia, esta prueba avisa."""
        src = FC.funcion_top("app.py", "_ot_pdf_context")
        for frag in ("estado != \"cerrada\"", "firma_cliente_url", "_ot_es_interna(visita)", "firma_tecnico_url", "firma_supervisor_url"):
            self.assertIn(frag, src, frag)


class TestEnvioManualPuro(unittest.TestCase):
    def test_normalizar_lista(self):
        buenos, malos, rep, exceso = C.normalizar_lista(["A@x.cl", " a@x.cl ", "b@x.cl", "mal", "c@@x.cl", "", None, "d@x.cl"])
        self.assertEqual((buenos, rep, exceso), (["a@x.cl", "b@x.cl", "d@x.cl"], 1, 0))
        self.assertEqual(malos, ["mal", "c@@x.cl"])

    def test_el_maximo_son_15_como_las_cotizaciones(self):
        self.assertEqual(C.MAX_DESTINATARIOS, 15)
        buenos, _m, _r, exceso = C.normalizar_lista([f"p{i}@x.cl" for i in range(20)])
        self.assertEqual((len(buenos), exceso), (15, 5))

    def test_correo_valido(self):
        for ok in ("a@x.cl", "nombre.apellido+oc@sphs.cl", "A_B-c@sub.dominio.com"):
            self.assertTrue(C.correo_valido(ok), ok)
        for mal in ("", "a@x", "a@@x.cl", "a b@x.cl", "a@x..cl", "@x.cl", "a@x.c", "x" * 200 + "@x.cl"):
            self.assertFalse(C.correo_valido(mal), mal)

    def test_clasificar_conoce_a_los_de_la_ot_y_trata_como_tecnico_a_cualquier_tecnico(self):
        conocidos = {"cli@x.cl": {"nombre": "María", "rol": "cliente"}, "tec@x.cl": {"nombre": "Lenin", "rol": "tecnico"},
                     "aaron@sphs.cl": {"nombre": "Aarón", "rol": "ejecutivo"}}
        r = C.clasificar_pedidos(["cli@x.cl", "tec@x.cl", "aaron@sphs.cl", "otro@x.cl", "tecnico.de.otra.ot@sphs.cl"], conocidos=conocidos,
                                 es_correo_tecnico=lambda c: c == "tecnico.de.otra.ot@sphs.cl")
        self.assertEqual([(x["rol"], x["tecnico"]) for x in r],
                         [("cliente", False), ("tecnico", True), ("ejecutivo", False), ("otro", False), ("tecnico", True)])
        self.assertEqual(r[0]["nombre"], "María")

    def test_si_no_se_puede_saber_si_es_tecnico_se_trata_como_tecnico(self):
        def roto(c):
            raise RuntimeError("bd caída")
        r = C.clasificar_pedidos(["x@y.cl"], conocidos={}, es_correo_tecnico=roto)
        self.assertEqual((r[0]["rol"], r[0]["tecnico"]), ("tecnico", True))          # callar plata es barato

    def test_asunto_vacio_es_el_sugerido_y_a_un_tecnico_no_le_llega_un_asunto_con_plata(self):
        self.assertEqual(C.asunto_para("", "Orden de trabajo OT-1"), "Orden de trabajo OT-1")
        self.assertEqual(C.asunto_para("  Te comparto   la OT  ", "x"), "Te comparto la OT")
        self.assertEqual(C.asunto_para("OT-1 por $120.000", "def", para_tecnico=False), "OT-1 por $120.000")
        self.assertEqual(C.asunto_para("OT-1 por $120.000", "def", para_tecnico=True), "def")
        self.assertEqual(len(C.asunto_para("x" * 400, "def")), C.MAX_ASUNTO)

    def test_a_un_tecnico_se_le_quitan_las_lineas_con_plata_del_mensaje(self):
        msg = "Gracias por el trabajo.\nEl cobro fue de $50.000.\nLa factura de proveedor está pendiente.\nQuedó todo en orden."
        self.assertIn("$50.000", C.mensaje_para(msg))
        tec = C.mensaje_para(msg, para_tecnico=True)
        self.assertIn("Gracias por el trabajo", tec)
        self.assertIn("Quedó todo en orden", tec)
        for prohibido in ("50.000", "factura", "proveedor", "cobro"):
            self.assertNotIn(prohibido, tec)
        self.assertEqual(C.mensaje_para("", True), "")

    def test_el_mensaje_se_escapa_porque_lo_escribe_una_persona(self):
        h = C.mensaje_html('Hola <script>alert(1)</script> & "gracias"\nsegunda línea')
        self.assertNotIn("<script", h)
        self.assertIn("&lt;script&gt;", h)
        self.assertIn("<br>", h)
        self.assertEqual(C.mensaje_html("  "), "")

    def test_resumenes_explicitos(self):
        items = [{"nombre": "María", "rol": "cliente", "correo": "m@x.cl"}, {"nombre": "Lenin", "rol": "tecnico", "correo": "l@x.cl"}]
        self.assertEqual(C.resumen_destinatarios(items), "Se enviará a: María (Cliente) · Lenin (Técnico).")
        self.assertEqual(C.resumen_destinatarios([]), "No hay a quién enviarla.")

    def test_la_verdad_por_destinatario(self):
        res = [{"nombre": "María", "rol": "cliente", "correo": "m@x.cl", "resultado": C.RES_ENVIADO},
               {"nombre": "Lenin", "rol": "tecnico", "correo": "l@x.cl", "resultado": C.RES_FRENADO,
                "motivo": "El envío de correos de Correos a técnicos (OT) está apagado."},
               {"nombre": "", "rol": "otro", "correo": "z@x.cl", "resultado": C.RES_FALLO, "motivo": "El proveedor de correo no lo aceptó."}]
        t = C.resumen_resultados(res)
        self.assertEqual(t[0], ("Se envió a María (Cliente) (m@x.cl).", True))
        self.assertEqual(t[1], ("No salió a Lenin (Técnico) (l@x.cl) porque el envío de correos de Correos a técnicos (OT) está apagado.", False))
        self.assertEqual(t[2][1], False)
        self.assertIn("porque el proveedor de correo no lo aceptó", t[2][0])

    def test_estado_del_cliente_en_un_envio_manual(self):
        c = lambda r: {"rol": "cliente", "resultado": r}
        t = {"rol": "tecnico", "resultado": C.RES_ENVIADO}
        self.assertEqual(C.estado_cliente_manual([t]), "")                                   # no iba ningún cliente
        self.assertEqual(C.estado_cliente_manual([c(C.RES_FRENADO), c(C.RES_ENVIADO), t]), C.CLI_ENVIADO)
        self.assertEqual(C.estado_cliente_manual([c(C.RES_FRENADO)]), C.CLI_FRENADO)
        self.assertEqual(C.estado_cliente_manual([c(C.RES_FALLO)]), C.CLI_FALLO)


class TestConstanciaYTextos(unittest.TestCase):
    def test_bitacora_dice_a_quien_con_que_rol_y_que_paso(self):
        p = _plan(cliente_encendido=False)
        for i in p:
            i["resultado"] = C.RES_ENVIADO if i["intentar"] else C.RES_NO_ENVIADO
        p[-1]["resultado"], p[-1]["motivo"] = C.RES_FRENADO, "El envío de correos de Correos a técnicos (OT) está apagado."
        t = C.bitacora_envio(p, "OT terminada · avisos al aprobar el cierre")
        self.assertTrue(t.startswith("OT terminada · avisos al aprobar el cierre: "))
        self.assertIn("María (Cliente) <contacto@gimnasio.cl> — no se envió: El correo al cliente está apagado", t)
        self.assertIn("Aarón Rojas (Ejecutivo) <aaron@sphs.cl> — enviado", t)
        self.assertIn("Lenin Urbina (Técnico) <lenin@sphs.cl> — frenado: El envío de correos de Correos a técnicos (OT) está apagado", t)
        self.assertEqual(C.bitacora_envio([], "X"), "X: sin destinatarios")

    def test_detalle_json_guarda_lo_necesario_y_se_lee_de_vuelta(self):
        p = _plan()
        for i in p:
            i["resultado"] = C.RES_ENVIADO
        raw = C.detalle_json(p)
        filas = json.loads(raw)
        self.assertEqual(len(filas), len(p))
        self.assertEqual(set(filas[0]), {"grupo", "rol", "nombre", "correo", "estado", "resultado", "motivo"})
        lineas = C.lineas_resultado(raw)
        self.assertEqual(len(lineas), len(p))
        self.assertIn("Lenin Urbina (Técnico) <lenin@sphs.cl> — enviado", lineas)
        self.assertEqual(C.lineas_resultado("{roto"), [])

    def test_capitalizar(self):
        self.assertEqual(C.capitalizar("el correo está apagado"), "El correo está apagado.")
        self.assertEqual(C.capitalizar("Ya viene con punto."), "Ya viene con punto.")
        self.assertEqual(C.capitalizar(""), "")


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
#  1b) LOS TEXTOS DE LOS CORREOS: variables escapadas, plantillas sin plata
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
OT_DEMO = {"id": 77, "numero_ot": "OT-2026-00077", "cliente": "Gimnasio <b>Demo</b> & Cía", "tipo_label": "Instalación",
           "fecha_realizada": dt.date(2026, 10, 21), "fecha": dt.date(2026, 10, 20), "direccion": "Av. Demo 123 <Las Condes>",
           "equipos": ["Trotadora ILUS Pro 9000", "Bicicleta ILUS"], "tecnicos": ["Lenin Urbina"]}


class TestVariablesYPlantillas(unittest.TestCase):
    def test_todo_va_escapado_porque_lo_escribe_una_persona(self):
        v = C.variables_ot(OT_DEMO, nombre_destinatario='Ana <script>', link_ot_firmada="https://x/ot-firmada/T?a=1&b=2", enviado_por="<img>")
        for k in ("cliente", "direccion", "nombre_destinatario", "saludo", "enviado_por", "link_ot_firmada"):
            self.assertNotIn("<", v[k], k)
        self.assertEqual(v["cliente"], "Gimnasio &lt;b&gt;Demo&lt;/b&gt; &amp; Cía")
        self.assertIn("&amp;b=2", v["link_ot_firmada"])

    def test_formatos_chilenos_y_valores_por_defecto(self):
        v = C.variables_ot(dict(OT_DEMO, cliente="", fecha_realizada=None))
        self.assertEqual(v["fecha_realizada"], "20/10/2026")                 # sin fecha de realización cae en la programada, día/mes/año
        self.assertEqual(v["cliente"], "Trabajo interno")
        self.assertEqual(v["saludo"], "Hola,")
        self.assertEqual(C.variables_ot(OT_DEMO, nombre_destinatario="María")["saludo"], "Hola María,")
        self.assertEqual(v["maquina"], "Trotadora ILUS Pro 9000, Bicicleta ILUS")
        self.assertEqual(v["dias_vigencia"], "30")

    def test_no_hay_ninguna_variable_con_plata(self):
        v = C.variables_ot(dict(OT_DEMO, costo=1500000, costo_proveedor=900000, margen=600000, factura="FCV 123", oc="OC-15"))
        self.assertFalse({"costo", "costo_proveedor", "margen", "factura", "oc"} & set(v))
        self.assertIsNone(PLATA.search(_txt(" ".join(v.values()))))

    def test_las_cuatro_plantillas_existen_y_ninguna_lleva_plata(self):
        self.assertEqual(set(C.SEMILLAS), set(C.PLANTILLAS))
        v = C.variables_ot(OT_DEMO, nombre_destinatario="María", link_ot_firmada="https://x/ot-firmada/T", link_ot="https://x/ot/77",
                           aprobado_por="Aarón Rojas", cerrada_en="21/10/2026 11:30", enviado_por="Aarón Rojas", mensaje="Gracias.")
        for slug in C.PLANTILLAS:
            asunto, cuerpo = C.armar_correo(slug, v)
            visible = _txt(asunto + " " + cuerpo)
            self.assertNotIn("{{", asunto + cuerpo, slug)                       # ningún token sin reemplazar
            self.assertIsNone(PLATA.search(visible), (slug, visible))
            self.assertIn("OT-2026-00077", asunto + cuerpo, slug)

    def test_la_del_tecnico_no_lleva_ni_el_enlace_publico(self):
        v = C.variables_ot(OT_DEMO, nombre_destinatario="Lenin", link_ot_firmada="https://x/ot-firmada/T", link_ot="https://x/ot/77")
        _a, cuerpo = C.armar_correo(C.PLANTILLA_TERMINADA_TECNICO, v)
        self.assertIn("https://x/ot/77", cuerpo)
        self.assertNotIn("ot-firmada", cuerpo)

    def test_la_del_cliente_enlaza_el_documento_firmado_y_dice_cuanto_dura(self):
        v = C.variables_ot(OT_DEMO, nombre_destinatario="María", link_ot_firmada="https://x/ot-firmada/T", link_ot="https://x/ot/77")
        asunto, cuerpo = C.armar_correo(C.PLANTILLA_TERMINADA, v)
        self.assertEqual(asunto, "Tu orden de trabajo OT-2026-00077 quedó terminada")
        self.assertIn('href="https://x/ot-firmada/T"', cuerpo)
        self.assertNotIn("https://x/ot/77", cuerpo)                              # el cliente no tiene cuenta: nunca el enlace interno
        self.assertIn("30 días", _txt(cuerpo))

    def test_la_del_equipo_cuenta_lo_que_se_envio(self):
        p = _plan()
        for i in p:
            i["resultado"] = C.RES_ENVIADO
            if i["grupo"] == "cliente":
                i["resultado"], i["motivo"] = C.RES_FRENADO, "El correo «x» está apagado en Comunicaciones → Control de correos."
        v = C.variables_ot(OT_DEMO, nombre_destinatario="Aarón", resumen_envio_html=C.resumen_envio_html(p), link_ot="https://x/ot/77",
                           aprobado_por="Aarón Rojas", cerrada_en="21/10/2026 11:30")
        _a, cuerpo = C.armar_correo(C.PLANTILLA_TERMINADA_INTERNO, v)
        t = _txt(cuerpo)
        self.assertIn("QUÉ SE ENVIÓ", t)
        self.assertIn("Cliente: todavía no se le envió", t)
        self.assertIn("Enviar OT por correo", t)
        self.assertIn("Técnico: recibió su copia (sin montos): Lenin Urbina", t)

    def test_la_del_equipo_tambien_dice_cuando_el_cliente_si_la_recibio(self):
        p = _plan()
        for i in p:
            i["resultado"] = C.RES_ENVIADO
        t = _txt(C.resumen_envio_html(p))
        self.assertIn("Cliente: se le envió el documento firmado a contacto@gimnasio.cl, admin@gimnasio.cl", t)

    def test_resumen_envio_vacio_si_no_hay_cliente_ni_tecnico(self):
        p = _plan(aplica_cliente=False, tecnicos={})
        self.assertEqual(C.resumen_envio_html(p), "")

    def test_el_editor_puede_cambiar_el_texto_y_se_usa_su_asunto(self):
        v = C.variables_ot(OT_DEMO)
        a, c = C.armar_correo(C.PLANTILLA_MANUAL, v, plantilla=("Asunto de Daniel &amp; Cía {{ot}}", "<p>Texto de Daniel</p>"))
        self.assertEqual((a, c), ("Asunto de Daniel & Cía {{ot}}", "<p>Texto de Daniel</p>"))       # el asunto es texto plano: se deshace el escape
        a2, c2 = C.armar_correo(C.PLANTILLA_MANUAL, v, plantilla=None)
        self.assertEqual(a2, "Orden de trabajo OT-2026-00077 · Gimnasio &lt;b&gt;Demo&lt;/b&gt; &amp; Cía".replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&"))
        a3, _c3 = C.armar_correo(C.PLANTILLA_MANUAL, v, plantilla=("", ""))
        self.assertTrue(a3.startswith("Orden de trabajo"))                               # vacía → la del código


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
#  2) EL REGISTRO DE CORREOS Y EL EDITOR DE PLANTILLAS
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestRegistro(unittest.TestCase):
    def test_las_cinco_claves_estan_con_su_llave_su_destinatario_y_su_modo(self):
        esperado = {
            C.CORREO_TERMINADA_CLIENTE: ("mantenciones", "cliente", "automatico"),
            C.CORREO_TERMINADA_RESPONSABLE: ("mantenciones", "responsable_ot", "automatico"),
            C.CORREO_TERMINADA_TECNICO: ("ot_tecnicos", "tecnico", "automatico"),
            C.CORREO_MANUAL: ("mantenciones", "cliente", "manual"),
            C.CORREO_MANUAL_TECNICO: ("ot_tecnicos", "tecnico", "manual"),
        }
        for clave, (llave, dest, modo) in esperado.items():
            c = R.por_clave(clave)
            self.assertIsNotNone(c, clave)
            self.assertEqual((c["llave"], c["destinatario"], c["modo"], c["modulo_ui"], c["tipo"]), (llave, dest, modo, "ot", "ilus"), clave)
        self.assertEqual(R.validar(MC.llaves_validas()), [])

    def test_los_de_tecnicos_salen_por_su_llave_propia(self):
        for clave in (C.CORREO_TERMINADA_TECNICO, C.CORREO_MANUAL_TECNICO):
            self.assertTrue(clave.startswith("ot_tec_"))
            self.assertEqual(R.por_clave(clave)["llave"], OTC.MODULO_LLAVE)
        # y la copia automática al técnico se comporta como los demás avisos a técnicos: plantilla apagada ⇒ no sale
        self.assertEqual(R.por_clave(C.CORREO_TERMINADA_TECNICO)["plantilla_apagada"], "no_sale")

    def test_las_plantillas_del_registro_son_las_que_siembra_el_codigo(self):
        usadas = {R.por_clave(k)["plantilla"][1] for k in C.CORREOS}
        self.assertEqual(usadas, set(C.PLANTILLAS))
        for k in C.CORREOS:
            self.assertEqual(R.por_clave(k)["plantilla"][0], C.MODULO_PLANTILLAS)

    def test_el_editor_de_comunicaciones_las_ofrece_con_sus_variables(self):
        js = _leer("static/comunicaciones_index.js")
        i = js.index("const TPL_ESTADOS_MANTENCIONES = [")
        bloque = js[i:js.index("\n];", i)]
        for slug in C.PLANTILLAS:
            self.assertRegex(bloque, r"key:\s*'" + slug + r"'[^}]*grupo:'ot_terminada'", slug)
        self.assertIn("ot_terminada: [", js)
        self.assertIn("ot_terminada: { label:'OT terminada y envío de la OT'", js)
        # cada variable que ofrece el editor la arma el código (si no, Daniel inserta un campo que sale vacío)
        j = js.index("  ot_terminada: [")
        ofrecidas = set(re.findall(r"\{\{(\w+)\}\}", js[j:js.index("  ],", j)]))
        armadas = set(C.variables_ot(OT_DEMO))
        self.assertLessEqual(ofrecidas, armadas, ofrecidas - armadas)
        # y las semillas usan solo variables que el código arma
        for slug, (a, c) in C.SEMILLAS.items():
            self.assertLessEqual(set(re.findall(r"\{\{(\w+)\}\}", a + c)), armadas, slug)

    def test_la_vista_previa_de_control_de_correos_conoce_las_variables_nuevas(self):
        mapa = dict(R.DEMO_EXTRA)
        for c in C.CORREOS:
            mapa.update(R.por_clave(c).get("demo") or {})
        for slug, (a, c) in C.SEMILLAS.items():
            for tok in set(re.findall(r"\{\{(\w+)\}\}", a + c)):
                self.assertTrue(tok in mapa or tok in ("ot", "cliente", "tipo_mantencion", "tecnicos", "maquina", "direccion", "link_ot"),
                                (slug, tok))
        self.assertIn("QUÉ SE ENVIÓ", R.por_clave(C.CORREO_TERMINADA_RESPONSABLE)["demo"]["resumen_envio_html"])


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
#  3) LAS FUNCIONES REALES DE app.py EN UN MUNDO DE MENTIRA (la puerta única de envío también es la REAL)
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
FUNCS_OT = (
    "_ot2_err", "_rol_familia", "_ot_es_interna", "_ot_tec_ids", "_ot_tec_enviar",
    "_ot_com_cliente_correos", "_ot_com_datos", "_ot_com_bloqueos", "_ot_com_plan", "_ot_com_documento_ok", "_ot_com_enlace_firmada",
    "_ot_com_armar", "_ot_com_anotar", "_ot_com_tec_es_correo", "_ot_terminada_notificar", "_ot_com_items_json",
    "ot2_api_com_destinatarios", "_ot_com_puede_enviar", "_ot_com_candidatos_manual", "_ot_com_anteriores", "ot2_api_com_enviar_datos",
    "ot2_api_com_vista_previa", "ot2_api_com_enviar", "_ot_com_contexto_ficha", "_ot_com_tecnicos_de_usuarios", "_ot_com_plan_creacion",
    "ot2_api_com_creacion_previa", "_ot_com_creacion_resultado", "_ensure_ot_comunicacion_tablas", "_ensure_comm_template_ot_comunicacion",
)
CONSTS_OT = ("_TIPO_OT_LABEL",)


class MundoOT:
    """Lo mínimo de la base y del resto de app.py para ejecutar las funciones REALES de la comunicación de la OT. La puerta de envío
    (`_send_ilus_email`: general > llave > interruptor de ese correo > cola) y `_email_resultado` también son las reales; el envío de verdad
    (SMTP) es de mentira y solo anota lo que le pidieron: jamás sale un correo (REGLA #22)."""

    def __init__(self, *, estado="cerrada", tecnico=False, puede=True, env_cliente=None, doc="ok"):
        self.m = MC.MundoPantalla()
        self.bd = self.m.bd
        self.amb = amb = self.m.amb
        self.tecnico, self.puede, self.doc = tecnico, puede, doc
        self.sql, self.logs, self.tabla = [], [], []             # tabla = filas de mant_ot_comunicaciones (la más nueva al final)
        self.avisos_tec = []                                      # correos con «asignada» reservada (mant_ot_tec_avisos)
        self.email_log_cli = []                                   # filas de email_log de la visita agendada al cliente
        self.rol_de = {}                                          # correo → rol (app_users)
        self.tec_correos = set()                                  # correos de mant_tecnicos / fichas de proveedores
        self.plantilla_apagada = {}
        self.plantilla_editor = None                              # (asunto, cuerpo) que devolvería el editor, o None
        self.gestion = [{"id": 2, "nombre": "Aarón Rojas", "correo": "aaron@sphs.cl", "por": "ejecutivo"},
                        {"id": 3, "nombre": "Víctor Soto", "correo": "victor@sphs.cl", "por": "creador"}]
        self.cliente_emails = ["admin@gimnasio.cl"]
        self.tecnicos = {"lenin@sphs.cl": "Lenin Urbina"}
        self.fichas = set()
        self.fila = {
            "id": 77, "numero_ot": "OT-2026-00077", "estado": estado, "cliente_id": 5, "tipo": "instalacion",
            "cerrada_at": dt.datetime(2026, 10, 21, 14, 30, 5), "cerrada_por": "Aarón Rojas", "firma_supervisor_nombre": "Aarón Rojas",
            "firma_cliente_url": "c.png", "firma_tecnico_url": "t.png", "firma_supervisor_url": "s.png",
            "contacto_email": "contacto@gimnasio.cl", "contacto_nombre": "María Pérez", "cli_razon": "Gimnasio Demo SpA",
            "cli_contacto_nombre": "Juan", "fecha_realizada": dt.date(2026, 10, 21), "fecha_programada": dt.date(2026, 10, 20),
            "ot_madre_id": None, "ot_relacion": None, "created_by": "Aarón Rojas",
            # plata que NUNCA debe llegar a un correo
            "costo": 1500000, "costo_proveedor": 900000, "costo_despacho": 50000, "zz_monto": 1200000, "margen": 550000}
        self.doc_razones = []
        self._id = 0
        env = {k: v for k, v in os.environ.items() if k != C.ENV_CLIENTE_ACTIVA}
        if env_cliente is not None:
            env[C.ENV_CLIENTE_ACTIVA] = env_cliente
        self._env = mock.patch.dict(os.environ, env, clear=True)
        self._env.start()
        self._armar()

    def cerrar(self):
        self._env.stop()

    # ── la base de mentira ──
    def _uno(self, sql, params=()):
        s = " ".join(sql.split())
        if "FROM mant_visitas v LEFT JOIN mant_clientes c" in s:
            return dict(self.fila) if self.fila else None
        if "FROM mant_clientes WHERE id" in s:
            return {"razon_social": "Gimnasio Demo SpA"}
        if "FROM app_users WHERE LOWER(username)" in s:
            r = self.rol_de.get(params[0])
            return {"role": r} if r else None
        if "FROM mant_tecnicos WHERE LOWER(email)" in s or "FROM mant_tecnicos_externos WHERE LOWER(contacto_email)" in s:
            return {"x": 1} if params[0] in self.tec_correos else None
        if "SELECT id, detalle_json, creado_por, creado_at FROM mant_ot_comunicaciones" in s:
            filas = [f for f in self.tabla if f["tipo"] == C.TIPO_CREACION]
            return dict(filas[-1], creado_at=dt.datetime.now()) if filas else None
        return self._orig_uno(sql, params)

    def _varios(self, sql, params=()):
        s = " ".join(sql.split())
        if "FROM mant_ot_comunicaciones WHERE visita_id=%s ORDER BY id DESC LIMIT 30" in s:
            return [dict(f) for f in reversed(self.tabla)]
        if "FROM mant_ot_comunicaciones WHERE visita_id=%s AND tipo IN" in s:
            return [dict(f, creado_at=dt.datetime(2026, 10, 21, 18, 0)) for f in reversed(self.tabla)
                    if f["tipo"] in (C.TIPO_TERMINADA, C.TIPO_MANUAL)][:params[-1]]
        if "FROM mant_ot_tec_avisos" in s:
            return [{"email": e} for e in self.avisos_tec]
        if "FROM email_log WHERE ref_codigo" in s:
            return list(self.email_log_cli)
        if "FROM app_users WHERE u.id IN" in s or "FROM `app_users` u" in s or "FROM `{AUTH_TABLE}`" in s:
            return []
        return self._orig_varios(sql, params)

    def _ejecutar(self, sql, params=()):
        s = " ".join(sql.split())
        self.sql.append((s, params))
        if s.startswith("UPDATE mant_ot_comunicaciones SET cliente_estado=%s, detalle_json=%s"):
            estado, detalle, vid, tipo, clave = params
            for f in self.tabla:
                if (f["visita_id"], f["tipo"], f["cierre_clave"]) == (vid, tipo, clave):
                    f.update(cliente_estado=estado, detalle_json=detalle)
        elif s.startswith("INSERT INTO mant_ot_comunicaciones"):
            self._id += 1
            if "VALUES (%s,%s,NULL,%s,%s,%s)" in s:                 # manual
                vid, tipo, estado, detalle, quien = params
            else:                                                     # creación: cliente_estado ''
                vid, tipo, detalle, quien = params
                estado = ""
            self.tabla.append({"id": self._id, "visita_id": vid, "tipo": tipo, "cierre_clave": None, "cliente_estado": estado,
                               "detalle_json": detalle, "creado_por": quien})

    def _reservar(self, sql, params=()):
        s = " ".join(sql.split())
        self.sql.append((s, params))
        if getattr(self, "romper_reserva", False):
            raise RuntimeError("la tabla no existe")
        vid, tipo, clave, estado, quien = params
        if any((f["visita_id"], f["tipo"], f["cierre_clave"]) == (vid, tipo, clave) for f in self.tabla):
            return 0
        self._id += 1
        self.tabla.append({"id": self._id, "visita_id": vid, "tipo": tipo, "cierre_clave": clave, "cliente_estado": estado,
                           "detalle_json": None, "creado_por": quien})
        return 1

    def _armar(self):
        a = self.amb
        self._orig_uno, self._orig_varios = a["mysql_fetchone"], a["mysql_fetchall"]
        FC.extraer(a, "app.py", FUNCS_OT, CONSTS_OT)
        a.update({
            "_ocom": C, "_evt": EVT, "AUTH_TABLE": "app_users", "os": os,
            "mysql_fetchone": self._uno, "mysql_fetchall": self._varios, "mysql_execute": self._ejecutar,
            "mysql_execute_returning_rowcount": self._reservar,
            "_ot_tec_filas": lambda vids: {int(v): {"id": int(v), "numero_ot": self.fila.get("numero_ot"), "cliente": self.fila.get("cli_razon"),
                                                    "tipo_label": "Instalación", "direccion": "Av. Demo 123, Las Condes", "descripcion": ""}
                                           for v in vids},
            "_ot_tec_equipos_bulk": lambda vids: {int(v): ["Trotadora ILUS Pro 9000"] for v in vids},
            "_ot_tec_destinatarios_bulk": lambda vids, con_fichas=False: (
                ({int(v): dict(self.tecnicos) for v in vids}, {int(v): set(self.fichas) for v in vids}) if con_fichas
                else {int(v): dict(self.tecnicos) for v in vids}),
            "_ot_tec_plantilla_apagada": lambda slug: bool(self.plantilla_apagada.get(slug)),
            "_ot_tec_oculta_detalle": lambda vid: False,
            "_ot_gestion_destinatarios": lambda vid, fila=None: list(self.gestion),
            "_mant_get_cliente_emails": lambda cid: list(self.cliente_emails),
            "_ot_pdf_context": lambda vid, **kw: (None, self.doc, list(self.doc_razones)),
            "_ot_firma_token": lambda vid, ttl_horas=120: f"TOKEN{vid}x{ttl_horas}",
            "_public_base_url": lambda: "https://ilus.test",
            "_mant_log": lambda ent, eid, accion, detalle="": self.logs.append((ent, eid, accion, detalle)),
            "_render_comm_template": lambda slug, canal, variables, modulo=None: self._editor(slug, variables),
            "_es_rol_tecnico": lambda user=None: self.tecnico,
            "_puede_ot_accion": lambda vid, accion, user=None: self.puede,
            "chile_fmt_filter": MC.chile_fmt,
        })
        a["_ot_com_ahora"] = None

    def _editor(self, slug, variables):
        if not self.plantilla_editor:
            return None
        asunto, cuerpo = self.plantilla_editor
        for k, v in variables.items():
            asunto, cuerpo = asunto.replace("{{" + k + "}}", str(v)), cuerpo.replace("{{" + k + "}}", str(v))
        return asunto, cuerpo

    # ── atajos ──
    def f(self, nombre):
        return self.amb[nombre]

    def llamar(self, nombre, *args, cuerpo=None):
        self.m.peticion = {"json": cuerpo if cuerpo is not None else {}, "args": {}}
        r = self.amb[nombre](*args)
        resp, status = (r[0], r[1]) if isinstance(r, tuple) else (r, getattr(r, "status", 200))
        return status, resp.data

    def destinos(self, claves=None):
        """[(destinatario, correo_clave, modulo)] de lo que SALIÓ de verdad (el envío real de mentira)."""
        out = []
        for e in self.m.enviados_reales:
            out.append(e["to"])
        return out

    def filas_log(self, estado=None):
        return [f for f in self.bd.log if estado is None or f["estado"] == estado]

    def apagar_correo(self, clave):
        self.bd.ctrl[clave] = {"clave": clave, "activo": 0, "motivo": "", "actualizado_por": "Daniel", "actualizado_en": None}
        self.amb["_correo_ctrl_invalidar"]()

    def cerrar_llave(self, modulo):
        self.bd.llaves[modulo] = {"email_bloqueado": 1, "motivo": "por pruebas"}


class _Base(unittest.TestCase):
    def mundo(self, **kw):
        m = MundoOT(**kw)
        self.addCleanup(m.cerrar)
        return m


def _html_para(m, destino):
    return next(e["html"] for e in m.m.enviados_reales if e["to"] == destino)


def _asunto_para(m, destino):
    return next(e["asunto"] for e in m.m.enviados_reales if e["to"] == destino)


class TestOtTerminadaAutomatica(_Base):
    def test_con_la_variable_apagada_nada_sale_al_cliente_pero_si_al_equipo_y_al_tecnico(self):
        m = self.mundo()
        r = m.f("_ot_terminada_notificar")(77)
        self.assertTrue(r["ok"], r)
        self.assertEqual(sorted(m.destinos()), ["aaron@sphs.cl", "lenin@sphs.cl", "victor@sphs.cl"])
        for cli in ("contacto@gimnasio.cl", "admin@gimnasio.cl"):
            self.assertNotIn(cli, m.destinos())
        self.assertEqual(r["cliente_estado"], C.CLI_APAGADO)
        fila = m.tabla[0]
        self.assertEqual((fila["tipo"], fila["cierre_clave"], fila["cliente_estado"]), (C.TIPO_TERMINADA, "2026-10-21 14:30:05", C.CLI_APAGADO))
        det = json.loads(fila["detalle_json"])
        self.assertEqual({d["correo"]: d["resultado"] for d in det if d["grupo"] == "cliente"},
                         {"contacto@gimnasio.cl": C.RES_NO_ENVIADO, "admin@gimnasio.cl": C.RES_NO_ENVIADO})
        (ent, eid, accion, texto), = m.logs
        self.assertEqual((ent, eid, accion), ("visita", 77, "ot_terminada_aviso"))
        self.assertIn("María Pérez (Cliente) <contacto@gimnasio.cl> — no se envió: El correo al cliente está apagado", texto)

    def test_cada_correo_sale_con_su_clave_y_su_llave(self):
        m = self.mundo(env_cliente="1")
        m.f("_ot_terminada_notificar")(77)
        por_dest = {f["destinatario"]: (f["correo_clave"], f["modulo"], f["estado"]) for f in m.filas_log()}
        self.assertEqual(por_dest["contacto@gimnasio.cl"], (C.CORREO_TERMINADA_CLIENTE, "mantenciones", "enviado"))
        self.assertEqual(por_dest["admin@gimnasio.cl"], (C.CORREO_TERMINADA_CLIENTE, "mantenciones", "enviado"))
        self.assertEqual(por_dest["aaron@sphs.cl"], (C.CORREO_TERMINADA_RESPONSABLE, "mantenciones", "enviado"))
        self.assertEqual(por_dest["victor@sphs.cl"], (C.CORREO_TERMINADA_RESPONSABLE, "mantenciones", "enviado"))
        self.assertEqual(por_dest["lenin@sphs.cl"], (C.CORREO_TERMINADA_TECNICO, "ot_tecnicos", "enviado"))
        self.assertTrue(all(f["evento"].startswith("ot_terminada_") for f in m.filas_log()))

    def test_encendida_le_llega_al_cliente_su_enlace_publico_y_a_la_ficha_interna_al_equipo(self):
        m = self.mundo(env_cliente="si")
        r = m.f("_ot_terminada_notificar")(77)
        self.assertEqual(r["cliente_estado"], C.CLI_ENVIADO)
        self.assertEqual(sorted(m.destinos()), ["aaron@sphs.cl", "admin@gimnasio.cl", "contacto@gimnasio.cl", "lenin@sphs.cl", "victor@sphs.cl"])
        cli = _html_para(m, "contacto@gimnasio.cl")
        self.assertIn("https://ilus.test/ot-firmada/TOKEN77x720", cli)                 # 30 días = 720 horas
        self.assertNotIn("https://ilus.test/ot/77", cli)
        self.assertIn("Hola María Pérez,", _txt(cli))
        self.assertIn("Hola,", _txt(_html_para(m, "admin@gimnasio.cl")))             # la otra dirección no es la persona del contacto
        self.assertEqual(_asunto_para(m, "contacto@gimnasio.cl"), "ILUS · Tu orden de trabajo OT-2026-00077 quedó terminada")
        self.assertNotIn("ot-firmada", _html_para(m, "lenin@sphs.cl"))
        self.assertIn("https://ilus.test/ot/77", _html_para(m, "lenin@sphs.cl"))

    def test_el_tecnico_nunca_recibe_plata_ni_proveedores_aunque_la_ot_los_tenga(self):
        m = self.mundo(env_cliente="1")
        m.f("_ot_terminada_notificar")(77)
        for destino in ("lenin@sphs.cl", "contacto@gimnasio.cl", "aaron@sphs.cl"):
            visible = _txt(_html_para(m, destino) + " " + _asunto_para(m, destino))
            # el aviso al equipo dice que la copia del técnico va «sin montos»: esa frase no es plata
            self.assertIsNone(PLATA.search(visible.replace("(sin montos)", "")), (destino, visible))
            for numero in ("1500000", "1.500.000", "900000", "900.000", "550.000", "1200000"):
                self.assertNotIn(numero, visible, destino)

    def test_una_sola_vez_por_aprobacion(self):
        m = self.mundo(env_cliente="1")
        f = m.f("_ot_terminada_notificar")
        self.assertTrue(f(77)["ok"])
        enviados = len(m.m.enviados_reales)
        r2 = f(77)
        self.assertFalse(r2["ok"])
        self.assertIn("ya se avisó", r2["omitido"])
        self.assertEqual(len(m.m.enviados_reales), enviados)              # ni un correo más
        self.assertEqual(len([t for t in m.tabla if t["tipo"] == C.TIPO_TERMINADA]), 1)
        # una OT que se reabriera y se aprobara de nuevo es OTRA aprobación
        m.fila["cerrada_at"] = dt.datetime(2026, 10, 22, 9, 0, 0)
        self.assertTrue(f(77)["ok"])
        self.assertEqual(len([t for t in m.tabla if t["tipo"] == C.TIPO_TERMINADA]), 2)

    def test_una_llave_cerrada_deja_lo_del_cliente_frenado_con_su_motivo_y_el_tecnico_sale_por_su_llave(self):
        m = self.mundo(env_cliente="1")
        m.cerrar_llave("mantenciones")
        r = m.f("_ot_terminada_notificar")(77)
        self.assertEqual(r["cliente_estado"], C.CLI_FRENADO)
        self.assertEqual(m.destinos(), ["lenin@sphs.cl"])                       # el técnico tiene SU llave
        bloq = m.filas_log("bloqueado")
        self.assertEqual({f["destinatario"] for f in bloq}, {"contacto@gimnasio.cl", "admin@gimnasio.cl", "aaron@sphs.cl", "victor@sphs.cl"})
        self.assertTrue(all(f["correo_clave"] for f in bloq))                    # el Historial lo cuenta como frenado de ESE correo
        det = {d["correo"]: d for d in json.loads(m.tabla[0]["detalle_json"])}
        self.assertEqual(det["contacto@gimnasio.cl"]["resultado"], C.RES_FRENADO)
        self.assertIn("El envío de correos de Mantenciones está apagado", det["contacto@gimnasio.cl"]["motivo"])
        self.assertEqual(det["lenin@sphs.cl"]["resultado"], C.RES_ENVIADO)

    def test_el_interruptor_de_un_correo_lo_frena_y_queda_el_rastro(self):
        m = self.mundo(env_cliente="1")
        m.apagar_correo(C.CORREO_TERMINADA_TECNICO)
        r = m.f("_ot_terminada_notificar")(77)
        self.assertNotIn("lenin@sphs.cl", m.destinos())
        det = {d["correo"]: d for d in json.loads(m.tabla[0]["detalle_json"])}
        self.assertEqual(det["lenin@sphs.cl"]["resultado"], C.RES_FRENADO)
        self.assertIn("Control de correos", det["lenin@sphs.cl"]["motivo"])
        self.assertEqual(r["cliente_estado"], C.CLI_ENVIADO)                     # el del cliente no se tocó

    def test_el_interruptor_general_frena_todo(self):
        m = self.mundo(env_cliente="1")
        m.bd.global_on = False
        m.f("_ot_terminada_notificar")(77)
        self.assertEqual(m.destinos(), [])
        self.assertEqual(m.tabla[0]["cliente_estado"], C.CLI_FRENADO)
        self.assertEqual(len(m.filas_log("bloqueado")), 5)

    def test_documento_incompleto_el_cliente_no_recibe_un_enlace_muerto(self):
        m = self.mundo(env_cliente="1", doc="incompleto")
        m.doc_razones = ["Falta firma del responsable/supervisor."]
        r = m.f("_ot_terminada_notificar")(77)
        self.assertEqual(r["cliente_estado"], C.CLI_SIN_DOCUMENTO)
        self.assertNotIn("contacto@gimnasio.cl", m.destinos())
        self.assertEqual(sorted(m.destinos()), ["aaron@sphs.cl", "lenin@sphs.cl", "victor@sphs.cl"])
        cli = [d for d in json.loads(m.tabla[0]["detalle_json"]) if d["grupo"] == "cliente"]
        self.assertIn("Falta firma del responsable/supervisor", cli[0]["motivo"])

    def test_si_el_aviso_al_cliente_esta_apagado_no_se_revisa_el_documento(self):
        m = self.mundo()                                                       # variable apagada
        llamadas = []
        m.amb["_ot_pdf_context"] = lambda vid, **kw: llamadas.append(vid) or (None, "ok", [])
        m.f("_ot_terminada_notificar")(77)
        self.assertEqual(llamadas, [])                                         # _ot_pdf_context es pesado: solo si el cliente va a recibir algo

    def test_un_trabajo_interno_no_tiene_cliente_pero_el_equipo_y_el_tecnico_si_reciben(self):
        m = self.mundo(env_cliente="1")
        m.fila.update(cliente_id=None, cli_razon=None, contacto_email=None, contacto_nombre=None)
        r = m.f("_ot_terminada_notificar")(77)
        self.assertEqual(r["cliente_estado"], C.CLI_NO_APLICA)
        self.assertEqual(sorted(m.destinos()), ["aaron@sphs.cl", "lenin@sphs.cl", "victor@sphs.cl"])
        self.assertNotIn("Cliente:", _txt(_html_para(m, "aaron@sphs.cl")).split("QUÉ SE ENVIÓ")[-1].split("Ver la OT")[0].replace("Cliente: Trabajo interno", ""))

    def test_la_desinstalacion_no_le_escribe_al_cliente_regla_22(self):
        m = self.mundo(env_cliente="1")
        m.fila.update(ot_madre_id=70, ot_relacion="desinstalacion")
        r = m.f("_ot_terminada_notificar")(77)
        self.assertEqual(r["cliente_estado"], C.CLI_NO_APLICA)
        for cli in ("contacto@gimnasio.cl", "admin@gimnasio.cl"):
            self.assertNotIn(cli, m.destinos())
        self.assertIn("lenin@sphs.cl", m.destinos())

    def test_una_pendiente_por_falta_de_stock_si_avisa_al_cliente(self):
        m = self.mundo(env_cliente="1")
        m.fila.update(ot_madre_id=70, ot_relacion="pendiente_stock")
        self.assertEqual(m.f("_ot_terminada_notificar")(77)["cliente_estado"], C.CLI_ENVIADO)

    def test_una_ot_que_no_esta_cerrada_no_avisa_nada(self):
        for estado in ("pendiente_aprobacion", "programada", "en_curso", "cancelada"):
            m = self.mundo(estado=estado, env_cliente="1")
            r = m.f("_ot_terminada_notificar")(77)
            self.assertFalse(r["ok"], estado)
            self.assertIn("no está cerrada", r["omitido"])
            self.assertEqual((m.m.enviados_reales, m.tabla), ([], []))

    def test_una_ot_que_no_existe_no_rompe(self):
        m = self.mundo()
        m.fila = None
        r = m.f("_ot_terminada_notificar")(999)
        self.assertEqual((r["ok"], r["omitido"]), (False, "la OT no existe"))

    def test_jamas_levanta_aunque_la_base_se_caiga_y_dice_el_tipo_del_error(self):
        m = self.mundo(env_cliente="1")

        def roto(*a, **k):
            raise RuntimeError("la base no responde")
        m.amb["mysql_fetchone"] = roto
        r = m.f("_ot_terminada_notificar")(77)
        self.assertFalse(r["ok"])
        self.assertEqual(r["error"], "RuntimeError")
        self.assertEqual(m.m.enviados_reales, [])
        self.assertTrue(any("[ot-terminada]" in p for p in m.m.prints))

    def test_si_la_tabla_de_constancia_no_existe_se_avisa_igual_mejor_un_aviso_de_mas(self):
        m = self.mundo(env_cliente="1")
        m.romper_reserva = True
        r = m.f("_ot_terminada_notificar")(77)
        self.assertTrue(r["ok"])
        self.assertEqual(len(m.m.enviados_reales), 5)
        self.assertEqual(m.tabla, [])                                          # no hay dónde guardar, pero los correos salieron

    def test_si_un_correo_falla_al_armarse_los_demas_salen_y_el_cierre_no_se_entera(self):
        m = self.mundo(env_cliente="1")
        original = m.amb["_ot_com_armar"]

        def armar(slug, variables):
            if slug == C.PLANTILLA_TERMINADA:
                raise RuntimeError("plantilla rota")
            return original(slug, variables)
        m.amb["_ot_com_armar"] = armar
        r = m.f("_ot_terminada_notificar")(77)
        self.assertTrue(r["ok"])
        self.assertEqual(r["cliente_estado"], C.CLI_FALLO)
        self.assertEqual(sorted(m.destinos()), ["aaron@sphs.cl", "lenin@sphs.cl", "victor@sphs.cl"])

    def test_la_plantilla_apagada_del_tecnico_no_sale_y_lo_dice(self):
        m = self.mundo(env_cliente="1")
        m.plantilla_apagada[C.PLANTILLA_TERMINADA_TECNICO] = True
        m.f("_ot_terminada_notificar")(77)
        self.assertNotIn("lenin@sphs.cl", m.destinos())
        det = {d["correo"]: d for d in json.loads(m.tabla[0]["detalle_json"])}
        self.assertEqual(det["lenin@sphs.cl"]["resultado"], C.RES_NO_ENVIADO)
        self.assertIn("plantilla", det["lenin@sphs.cl"]["motivo"])

    def test_el_aviso_al_equipo_cuenta_lo_que_paso_con_el_cliente_y_el_tecnico(self):
        m = self.mundo()                                                       # cliente apagado
        m.f("_ot_terminada_notificar")(77)
        t = _txt(_html_para(m, "aaron@sphs.cl"))
        self.assertIn("QUÉ SE ENVIÓ", t)
        self.assertIn("Cliente: todavía no se le envió. El correo al cliente está apagado", t)
        self.assertIn("Técnico: recibió su copia (sin montos): Lenin Urbina", t)
        self.assertIn("El cierre lo aprobó Aarón Rojas el 21/10/2026", t)       # hora Chile, día/mes/año (UTC naive 14:30 → 11:30 en octubre)
        self.assertIn("11:30", t)

    def test_la_plantilla_del_editor_manda_sobre_el_texto_del_codigo(self):
        m = self.mundo(env_cliente="1")
        m.plantilla_editor = ("Asunto de Daniel {{ot}}", "<p>Texto de Daniel para {{nombre_destinatario}}</p>")
        m.f("_ot_terminada_notificar")(77)
        self.assertEqual(_asunto_para(m, "aaron@sphs.cl"), "ILUS · Asunto de Daniel OT-2026-00077")
        self.assertIn("Texto de Daniel para Aarón Rojas", _html_para(m, "aaron@sphs.cl"))

    def test_los_destinatarios_de_gestion_salen_de_la_unica_funcion(self):
        m = self.mundo(env_cliente="1")
        pedidos = []
        m.amb["_ot_gestion_destinatarios"] = lambda vid, fila=None: pedidos.append(vid) or [
            {"id": 9, "nombre": "Responsable Nuevo", "correo": "resp@sphs.cl", "por": "responsable"}]
        m.f("_ot_terminada_notificar")(77)
        self.assertEqual(pedidos, [77])
        self.assertIn("resp@sphs.cl", m.destinos())
        self.assertNotIn("aaron@sphs.cl", m.destinos())                          # nadie fija a mano a quién avisar


class TestAvisoPrevioAlCierre(_Base):
    def test_la_gestion_ve_a_cada_destinatario_con_su_rol_su_correo_y_lo_apagado(self):
        m = self.mundo(estado="pendiente_aprobacion")
        st, d = m.llamar("ot2_api_com_destinatarios", 77)
        self.assertEqual(st, 200)
        self.assertTrue(d["ok"] and d["aplica"])
        por = {i["correo"]: i for i in d["items"]}
        self.assertEqual(por["contacto@gimnasio.cl"]["estado"], C.EST_APAGADO_CONFIG)
        self.assertEqual(por["contacto@gimnasio.cl"]["motivo"], "El correo al cliente está apagado: no se enviará hasta que se active.")
        self.assertEqual((por["aaron@sphs.cl"]["rol_txt"], por["aaron@sphs.cl"]["saldra"]), ("Ejecutivo", True))
        self.assertEqual((por["lenin@sphs.cl"]["rol_txt"], por["lenin@sphs.cl"]["saldra"]), ("Técnico", True))
        self.assertTrue(d["texto"].startswith("Al aprobar el cierre se enviará esta OT a: Aarón Rojas (Ejecutivo)"), d["texto"])
        self.assertIn("Cliente: El correo al cliente está apagado: no se enviará hasta que se active.", d["texto"])
        self.assertFalse(d["cliente_encendido"])

    def test_encendido_el_cliente_aparece_como_destinatario_normal(self):
        m = self.mundo(estado="pendiente_aprobacion", env_cliente="1")
        _st, d = m.llamar("ot2_api_com_destinatarios", 77)
        self.assertTrue(d["cliente_encendido"])
        self.assertIn("María Pérez (Cliente)", d["texto"])

    def test_una_llave_cerrada_se_marca_antes_de_cerrar(self):
        m = self.mundo(estado="firmada_tecnico", env_cliente="1")
        m.cerrar_llave("ot_tecnicos")
        _st, d = m.llamar("ot2_api_com_destinatarios", 77)
        t = {i["correo"]: i for i in d["items"]}["lenin@sphs.cl"]
        self.assertFalse(t["saldra"])
        self.assertIn("Correos a técnicos (OT)", t["motivo"])

    def test_el_tecnico_ve_nombres_y_roles_pero_ningun_correo_ni_proveedores(self):
        m = self.mundo(estado="firmada_tecnico", tecnico=True, env_cliente="1")
        m.tecnicos = {"lenin@sphs.cl": "Lenin Urbina", "contacto@milling.cl": "Milling SpA"}
        m.fichas = {"contacto@milling.cl"}
        _st, d = m.llamar("ot2_api_com_destinatarios", 77)
        crudo = json.dumps(d, ensure_ascii=False)
        for oculto in ("@", "Milling"):
            self.assertNotIn(oculto, crudo)
        self.assertIn("Aarón Rojas (Ejecutivo)", d["texto"])
        self.assertIn("Lenin Urbina (Técnico)", d["texto"])
        self.assertEqual({i["correo"] for i in d["items"]}, {""})

    def test_una_ot_ya_cerrada_o_cancelada_no_necesita_el_aviso(self):
        for estado in ("cerrada", "cancelada", "anulada"):
            m = self.mundo(estado=estado)
            _st, d = m.llamar("ot2_api_com_destinatarios", 77)
            self.assertEqual((d["ok"], d["aplica"], d["items"]), (True, False, []), estado)

    def test_una_ot_que_no_existe_responde_404(self):
        m = self.mundo()
        m.fila = None
        st, d = m.llamar("ot2_api_com_destinatarios", 999)
        self.assertEqual((st, d["codigo"]), (404, "OT_NO_EXISTE"))

    def test_no_viaja_plata(self):
        m = self.mundo(estado="pendiente_aprobacion", env_cliente="1")
        _st, d = m.llamar("ot2_api_com_destinatarios", 77)
        crudo = json.dumps(d, ensure_ascii=False).replace("sin montos", "")          # «sin montos» es una promesa, no un monto
        self.assertIsNone(PLATA.search(crudo))
        for numero in ("1500000", "900000", "550000", "1200000"):
            self.assertNotIn(numero, crudo)


class TestEnviarOtPorCorreo(_Base):
    """«Enviar OT por correo»: como «Enviar cotización», con la verdad por destinatario."""

    def _enviar(self, m, destinatarios, **extra):
        return m.llamar("ot2_api_com_enviar", 77, cuerpo=dict({"destinatarios": destinatarios}, **extra))

    def test_solo_la_gestion_un_tecnico_no_envia_a_clientes(self):
        m = self.mundo(tecnico=True, puede=True)
        st, d = self._enviar(m, ["contacto@gimnasio.cl"])
        self.assertEqual((st, d["codigo"]), (403, "OT_SIN_PERMISO"))
        m2 = self.mundo(puede=False)
        self.assertEqual(self._enviar(m2, ["contacto@gimnasio.cl"])[0], 403)
        self.assertEqual(m.m.enviados_reales + m2.m.enviados_reales, [])
        for ruta in ("ot2_api_com_enviar_datos", "ot2_api_com_vista_previa"):
            self.assertEqual(m.llamar(ruta, 77)[0], 403, ruta)

    def test_validaciones_sin_enviar_nada(self):
        m = self.mundo()
        casos = [([], "SIN_DESTINATARIOS"), (None, "SIN_DESTINATARIOS"), (["mal"], "CORREO_INVALIDO"), (["a@b.cl", "c@@d.cl"], "CORREO_INVALIDO"),
                 ([f"p{i}@x.cl" for i in range(16)], "DEMASIADOS_DESTINATARIOS"), ([""], "SIN_DESTINATARIOS")]
        for dest, codigo in casos:
            st, d = self._enviar(m, dest)
            self.assertEqual((st, d["codigo"]), (400, codigo), dest)
        self.assertEqual(m.m.enviados_reales, [])
        self.assertEqual(m.tabla, [])

    def test_una_ot_sin_cerrar_o_sin_documento_completo_no_se_envia(self):
        m = self.mundo(estado="pendiente_aprobacion")
        st, d = self._enviar(m, ["contacto@gimnasio.cl"])
        self.assertEqual((st, d["codigo"]), (409, "OT_NO_CERRADA"))
        m2 = self.mundo(doc="incompleto")
        m2.doc_razones = ["Falta firma del responsable/supervisor."]
        st2, d2 = self._enviar(m2, ["contacto@gimnasio.cl"])
        self.assertEqual((st2, d2["codigo"]), (409, "DOCUMENTO_INCOMPLETO"))
        self.assertIn("el enlace no abriría", d2["error"])
        self.assertEqual(m.m.enviados_reales + m2.m.enviados_reales, [])

    def test_camino_feliz_cada_uno_por_su_llave_y_la_verdad_por_destinatario(self):
        m = self.mundo()
        st, d = self._enviar(m, ["contacto@gimnasio.cl", "aaron@sphs.cl", "lenin@sphs.cl", "nuevo@otra.cl", "CONTACTO@gimnasio.cl"],
                             mensaje="Te comparto la OT. Gracias.")
        self.assertEqual(st, 200)
        self.assertTrue(d["ok"])
        self.assertEqual((d["enviados"], d["frenados"], d["fallidos"]), (4, 0, 0))                # el repetido no cuenta
        por = {r["correo"]: r for r in d["resultados"]}
        self.assertEqual({k: (v["rol"], v["resultado"]) for k, v in por.items()},
                         {"contacto@gimnasio.cl": ("cliente", "enviado"), "aaron@sphs.cl": ("ejecutivo", "enviado"),
                          "lenin@sphs.cl": ("tecnico", "enviado"), "nuevo@otra.cl": ("otro", "enviado")})
        self.assertIn("Se envió a María Pérez (Cliente) (contacto@gimnasio.cl).", [t["texto"] for t in d["textos"]])
        self.assertTrue(all(t["ok"] for t in d["textos"]))
        # cada uno por su clave y su llave (la puerta única los dejó en el Historial con la referencia a la OT)
        log = {f["destinatario"]: (f["correo_clave"], f["modulo"], f["estado"]) for f in m.filas_log()}
        self.assertEqual(log["contacto@gimnasio.cl"], (C.CORREO_MANUAL, "mantenciones", "enviado"))
        self.assertEqual(log["aaron@sphs.cl"], (C.CORREO_MANUAL, "mantenciones", "enviado"))
        self.assertEqual(log["nuevo@otra.cl"], (C.CORREO_MANUAL, "mantenciones", "enviado"))
        self.assertEqual(log["lenin@sphs.cl"], (C.CORREO_MANUAL_TECNICO, "ot_tecnicos", "enviado"))
        # constancia: la tabla, la bitácora y lo que ve la ficha
        fila = m.tabla[-1]
        self.assertEqual((fila["tipo"], fila["cierre_clave"], fila["cliente_estado"], fila["creado_por"]),
                         (C.TIPO_MANUAL, None, C.CLI_ENVIADO, "Daniel Aguilar"))
        (ent, eid, accion, texto), = m.logs
        self.assertEqual((ent, eid, accion), ("visita", 77, "ot_enviada_correo"))
        self.assertIn("María Pérez (Cliente) <contacto@gimnasio.cl> — enviado", texto)
        self.assertIn("Lenin Urbina (Técnico) <lenin@sphs.cl> — enviado", texto)

    def test_el_mensaje_llega_entero_al_cliente_y_al_tecnico_se_le_quita_lo_que_hable_de_plata(self):
        m = self.mundo()
        msg = "Gracias por tu paciencia.\nEl cobro final fue de $120.000.\nQuedamos atentos."
        self._enviar(m, ["contacto@gimnasio.cl", "lenin@sphs.cl"], mensaje=msg)
        cli = _txt(_html_para(m, "contacto@gimnasio.cl"))
        tec = _txt(_html_para(m, "lenin@sphs.cl"))
        self.assertIn("$120.000", cli)
        self.assertIn("Gracias por tu paciencia", tec)
        self.assertIn("Quedamos atentos", tec)
        self.assertNotIn("120.000", tec)
        self.assertNotIn("cobro", tec)
        self.assertIn("ot-firmada", _html_para(m, "lenin@sphs.cl"))                 # el mismo documento firmado, que no lleva costos

    def test_cualquier_tecnico_aunque_no_sea_de_esta_ot_recibe_la_version_sin_plata_y_por_su_llave(self):
        m = self.mundo()
        m.rol_de["jaizer@sphs.cl"] = "tecnico_ejecutivo"
        m.tec_correos.add("proveedor@milling.cl")
        self._enviar(m, ["jaizer@sphs.cl", "proveedor@milling.cl", "cliente2@otro.cl"], mensaje="El valor fue $50.000. Gracias.",
                     asunto="OT por $50.000")
        log = {f["destinatario"]: (f["correo_clave"], f["modulo"]) for f in m.filas_log()}
        self.assertEqual(log["jaizer@sphs.cl"], (C.CORREO_MANUAL_TECNICO, "ot_tecnicos"))
        self.assertEqual(log["proveedor@milling.cl"], (C.CORREO_MANUAL_TECNICO, "ot_tecnicos"))
        self.assertEqual(log["cliente2@otro.cl"], (C.CORREO_MANUAL, "mantenciones"))
        for tec in ("jaizer@sphs.cl", "proveedor@milling.cl"):
            self.assertNotIn("50.000", _txt(_html_para(m, tec)) + _asunto_para(m, tec), tec)
            self.assertTrue(_asunto_para(m, tec).startswith("ILUS · Orden de trabajo OT-2026-00077"), tec)       # el asunto con plata se descarta
        self.assertEqual(_asunto_para(m, "cliente2@otro.cl"), "ILUS · OT por $50.000")

    def test_si_no_se_puede_saber_si_es_tecnico_se_trata_como_tecnico(self):
        m = self.mundo()
        original = m.amb["_ot_com_tec_es_correo"]

        def roto(c):
            raise RuntimeError("sin base")
        m.amb["_ot_com_tec_es_correo"] = roto
        _st, d = self._enviar(m, ["desconocido@otra.cl"], mensaje="Cobro: $1.000")
        self.assertTrue(d["resultados"][0]["tecnico"])
        self.assertNotIn("1.000", _txt(_html_para(m, "desconocido@otra.cl")))
        self.assertTrue(callable(original))

    def test_con_una_llave_cerrada_dice_exactamente_quien_no_recibio_y_por_que(self):
        m = self.mundo()
        m.cerrar_llave("mantenciones")
        st, d = self._enviar(m, ["contacto@gimnasio.cl", "aaron@sphs.cl", "lenin@sphs.cl"])
        self.assertEqual(st, 200)                                                        # salió el del técnico (su llave es otra)
        self.assertEqual((d["enviados"], d["frenados"], d["fallidos"]), (1, 2, 0))
        textos = {t["texto"]: t["ok"] for t in d["textos"]}
        self.assertIn("No salió a María Pérez (Cliente) (contacto@gimnasio.cl) porque el envío de correos de Mantenciones está apagado.", textos)
        self.assertFalse(textos["No salió a María Pérez (Cliente) (contacto@gimnasio.cl) porque el envío de correos de Mantenciones está apagado."])
        self.assertEqual(m.destinos(), ["lenin@sphs.cl"])
        self.assertEqual(m.tabla[-1]["cliente_estado"], C.CLI_FRENADO)                    # y la ficha seguirá recordando que al cliente no le llegó
        self.assertEqual({f["destinatario"] for f in m.filas_log("bloqueado")}, {"contacto@gimnasio.cl", "aaron@sphs.cl"})

    def test_si_todo_quedo_frenado_responde_409_con_el_detalle(self):
        m = self.mundo()
        m.cerrar_llave("mantenciones")
        st, d = self._enviar(m, ["contacto@gimnasio.cl", "aaron@sphs.cl"])
        self.assertEqual((st, d["error_codigo"], d["ok"]), (409, "NADA_SALIO_FRENADO", False))
        self.assertEqual(len(d["resultados"]), 2)
        self.assertEqual({r["resultado"] for r in d["resultados"]}, {"frenado"})
        self.assertEqual(m.m.enviados_reales, [])

    def test_el_interruptor_de_ese_correo_se_respeta_y_se_nombra(self):
        m = self.mundo()
        m.apagar_correo(C.CORREO_MANUAL_TECNICO)
        _st, d = self._enviar(m, ["lenin@sphs.cl", "contacto@gimnasio.cl"])
        por = {r["correo"]: r for r in d["resultados"]}
        self.assertEqual(por["lenin@sphs.cl"]["resultado"], "frenado")
        self.assertIn("Enviar OT por correo: copia al técnico (sin montos)", por["lenin@sphs.cl"]["motivo"])
        self.assertIn("Control de correos", por["lenin@sphs.cl"]["motivo"])
        self.assertEqual(por["contacto@gimnasio.cl"]["resultado"], "enviado")

    def test_si_el_proveedor_de_correo_rechaza_responde_502_y_no_miente(self):
        m = self.mundo()
        m.m._real = lambda to, asunto, html, **kw: False
        m.amb["_send_ilus_email_real"] = m.m._real
        st, d = self._enviar(m, ["contacto@gimnasio.cl"])
        self.assertEqual((st, d["error_codigo"], d["fallidos"]), (502, "NO_SALIO", 1))
        self.assertEqual(d["resultados"][0]["resultado"], "fallo")
        self.assertIn("el proveedor de correo no lo aceptó", d["textos"][0]["texto"])
        self.assertEqual(m.tabla[-1]["cliente_estado"], C.CLI_FALLO)

    def test_un_error_al_armar_un_correo_no_tumba_a_los_demas(self):
        m = self.mundo()
        original = m.amb["_ot_com_armar"]
        n = {"v": 0}

        def armar(slug, variables):
            n["v"] += 1
            if n["v"] == 1:
                raise RuntimeError("plantilla rota")
            return original(slug, variables)
        m.amb["_ot_com_armar"] = armar
        _st, d = self._enviar(m, ["contacto@gimnasio.cl", "aaron@sphs.cl"])
        self.assertEqual([r["resultado"] for r in d["resultados"]], ["fallo", "enviado"])
        self.assertEqual(m.destinos(), ["aaron@sphs.cl"])

    def test_el_asunto_sugerido_y_el_pedido(self):
        m = self.mundo()
        self._enviar(m, ["contacto@gimnasio.cl", "aaron@sphs.cl"], asunto="Aquí va tu OT firmada")
        self.assertEqual(_asunto_para(m, "contacto@gimnasio.cl"), "ILUS · Aquí va tu OT firmada")
        m2 = self.mundo()
        self._enviar(m2, ["contacto@gimnasio.cl"])
        self.assertEqual(_asunto_para(m2, "contacto@gimnasio.cl"), "ILUS · Orden de trabajo OT-2026-00077 · Gimnasio Demo SpA")

    def test_el_enlace_es_el_documento_publico_con_la_vigencia_de_30_dias(self):
        m = self.mundo()
        self._enviar(m, ["contacto@gimnasio.cl"])
        self.assertIn("https://ilus.test/ot-firmada/TOKEN77x720", _html_para(m, "contacto@gimnasio.cl"))

    def test_la_vista_previa_no_envia_nada_y_muestra_el_mensaje_escapado(self):
        m = self.mundo()
        st, d = m.llamar("ot2_api_com_vista_previa", 77, cuerpo={"mensaje": "Hola <b>mundo</b>", "asunto": "Mi asunto"})
        self.assertEqual(st, 200)
        self.assertEqual(d["asunto"], "ILUS · Mi asunto")
        self.assertIn("Hola &lt;b&gt;mundo&lt;/b&gt;", d["html"])
        self.assertIn("ot-firmada", d["html"])
        self.assertEqual((m.m.enviados_reales, m.tabla, m.filas_log()), ([], [], []))

    def test_los_datos_del_modal_traen_destinatarios_con_rol_asunto_y_envios_anteriores(self):
        m = self.mundo()
        m.tabla.append({"id": 1, "visita_id": 77, "tipo": C.TIPO_TERMINADA, "cierre_clave": "2026-10-21 14:30:05", "cliente_estado": "apagado",
                        "detalle_json": json.dumps([{"grupo": "cliente", "rol": "cliente", "nombre": "María", "correo": "m@x.cl",
                                                     "resultado": "no_enviado", "motivo": "El correo al cliente está apagado."}]),
                        "creado_por": "Aarón Rojas"})
        st, d = m.llamar("ot2_api_com_enviar_datos", 77)
        self.assertEqual(st, 200)
        self.assertTrue(d["puede"])
        self.assertEqual(d["maximo"], 15)
        self.assertEqual(d["asunto"], "Orden de trabajo OT-2026-00077 · Gimnasio Demo SpA")
        self.assertEqual([(x["correo"], x["rol_txt"]) for x in d["destinatarios"]],
                         [("contacto@gimnasio.cl", "Cliente"), ("admin@gimnasio.cl", "Cliente"), ("aaron@sphs.cl", "Ejecutivo"),
                          ("victor@sphs.cl", "Quien creó la OT"), ("lenin@sphs.cl", "Técnico")])
        self.assertEqual(d["anteriores"][0]["titulo"], "Aviso automático al aprobar el cierre")
        self.assertIn("María (Cliente) <m@x.cl> — no se envió: El correo al cliente está apagado", d["anteriores"][0]["lineas"][0])
        self.assertIsNone(PLATA.search(json.dumps(d, ensure_ascii=False).replace("sin montos", "")))

    def test_el_modal_explica_por_que_no_se_puede_todavia(self):
        m = self.mundo(estado="pendiente_aprobacion")
        _st, d = m.llamar("ot2_api_com_enviar_datos", 77)
        self.assertFalse(d["puede"])
        self.assertIn("todavía no está cerrada", d["razon"])
        m2 = self.mundo(doc="incompleto")
        m2.doc_razones = ["Falta firma del técnico."]
        _st, d2 = m2.llamar("ot2_api_com_enviar_datos", 77)
        self.assertFalse(d2["puede"])
        self.assertIn("Falta firma del técnico.", d2["razon"])

    def test_la_empresa_externa_se_ofrece_como_tecnico(self):
        m = self.mundo()
        m.tecnicos = {"lenin@sphs.cl": "Lenin Urbina", "contacto@milling.cl": "Milling SpA"}
        m.fichas = {"contacto@milling.cl"}
        _st, d = m.llamar("ot2_api_com_enviar_datos", 77)
        self.assertIn(("contacto@milling.cl", "Técnico"), [(x["correo"], x["rol_txt"]) for x in d["destinatarios"]])     # gestión sí ve al proveedor

    def test_el_respaldo_de_gestion_no_se_propone_en_el_modal_pero_si_recibe_el_aviso_automatico(self):
        """`_ot_gestion_destinatarios` cae a TODOS los superadmin/admin/supervisor cuando de la OT no sale nadie (para que un aviso INTERNO no se
        pierda). Eso sirve para el aviso automático, pero llenaría el modal de envío manual de gente que no tiene que ver con esta OT."""
        m = self.mundo()
        m.gestion = [{"id": 10 + i, "nombre": f"Admin {i}", "correo": f"admin{i}@sphs.cl", "por": "respaldo"} for i in range(6)]
        _st, d = m.llamar("ot2_api_com_enviar_datos", 77)
        correos = [x["correo"] for x in d["destinatarios"]]
        self.assertFalse([c for c in correos if c.startswith("admin") and c.endswith("@sphs.cl")], correos)
        self.assertIn("contacto@gimnasio.cl", correos)            # el cliente y el técnico siguen ofrecidos
        self.assertIn("lenin@sphs.cl", correos)
        plan_auto = m.f("_ot_com_plan")(m.f("_ot_com_datos")(77))
        self.assertEqual(len([i for i in plan_auto if i["grupo"] == "responsable" and i["rol"] == "respaldo"]), 6)   # el automático SÍ los avisa

    def test_los_destinatarios_propuestos_nunca_pasan_del_maximo(self):
        m = self.mundo()
        m.gestion = [{"id": 10 + i, "nombre": f"Persona {i}", "correo": f"persona{i}@sphs.cl", "por": "ejecutivo"} for i in range(25)]
        _st, d = m.llamar("ot2_api_com_enviar_datos", 77)
        self.assertLessEqual(len(d["destinatarios"]), C.MAX_DESTINATARIOS)
        self.assertEqual(len(d["destinatarios"]), C.MAX_DESTINATARIOS)


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
#  3b) LA FICHA: recordatorio ámbar, botón y avisos de la creación
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestContextoDeLaFicha(_Base):
    def _fila_auto(self, estado="apagado", clave="2026-10-21 14:30:05", id_=1):
        return {"id": id_, "visita_id": 77, "tipo": C.TIPO_TERMINADA, "cierre_clave": clave, "cliente_estado": estado, "detalle_json": "[]",
                "creado_por": "Aarón"}

    def test_una_ot_cerrada_con_el_aviso_al_cliente_apagado_recuerda_enviarla(self):
        m = self.mundo()
        m.tabla.append(self._fila_auto())
        ctx = m.f("_ot_com_contexto_ficha")(77, m.fila)
        self.assertTrue(ctx["puede_enviar"])
        self.assertTrue(ctx["listo"])
        self.assertEqual(ctx["franja"]["titulo"], "La OT terminada todavía no se le envió al cliente")
        self.assertEqual(ctx["franja"]["estado"], "apagado")

    def test_sin_antecedente_la_franja_no_se_inventa_pero_el_boton_si_esta(self):
        m = self.mundo()
        ctx = m.f("_ot_com_contexto_ficha")(77, m.fila)
        self.assertTrue(ctx["puede_enviar"])
        self.assertIsNone(ctx["franja"])

    def test_un_envio_manual_posterior_al_cliente_apaga_el_recordatorio(self):
        m = self.mundo()
        m.tabla.append(self._fila_auto())
        m.tabla.append({"id": 2, "visita_id": 77, "tipo": C.TIPO_MANUAL, "cierre_clave": None, "cliente_estado": "enviado", "detalle_json": "[]"})
        self.assertIsNone(m.f("_ot_com_contexto_ficha")(77, m.fila)["franja"])

    def test_otra_aprobacion_del_cierre_otro_antecedente(self):
        m = self.mundo()
        m.tabla.append(self._fila_auto(clave="2026-10-01 09:00:00"))
        self.assertIsNone(m.f("_ot_com_contexto_ficha")(77, m.fila)["franja"])

    def test_el_documento_no_listo_se_avisa_y_el_boton_sigue(self):
        m = self.mundo()
        m.fila["firma_supervisor_url"] = None
        ctx = m.f("_ot_com_contexto_ficha")(77, m.fila)
        self.assertEqual((ctx["puede_enviar"], ctx["listo"]), (True, False))
        self.assertIn("supervisor", ctx["razon"])

    def test_una_ot_sin_cerrar_no_tiene_boton_ni_recordatorio(self):
        m = self.mundo(estado="pendiente_aprobacion")
        m.tabla.append(self._fila_auto())
        ctx = m.f("_ot_com_contexto_ficha")(77, m.fila)
        self.assertEqual((ctx["puede_enviar"], ctx["franja"]), (False, None))

    def test_un_tecnico_no_recibe_nada(self):
        m = self.mundo(tecnico=True)
        m.tabla.append(self._fila_auto())
        ctx = m.f("_ot_com_contexto_ficha")(77, m.fila)
        self.assertEqual(ctx, {"puede_enviar": False, "listo": False, "razon": "", "franja": None, "creacion": None})

    def test_quien_no_puede_aprobar_no_ve_el_boton(self):
        m = self.mundo(puede=False)
        m.tabla.append(self._fila_auto())
        ctx = m.f("_ot_com_contexto_ficha")(77, m.fila)
        self.assertEqual((ctx["puede_enviar"], ctx["franja"]), (False, None))

    def test_los_avisos_de_la_creacion_se_muestran_un_rato(self):
        m = self.mundo(estado="programada")
        m.tabla.append({"id": 3, "visita_id": 77, "tipo": C.TIPO_CREACION, "cierre_clave": None, "cliente_estado": "",
                        "detalle_json": json.dumps([{"grupo": "tecnico", "rol": "tecnico", "nombre": "Lenin", "correo": "l@x.cl", "resultado": "enviado"}]),
                        "creado_por": "Aarón"})
        ctx = m.f("_ot_com_contexto_ficha")(77, m.fila)
        self.assertEqual(ctx["creacion"]["lineas"], ["Lenin (Técnico) <l@x.cl> — enviado"])
        self.assertEqual(ctx["creacion"]["quien"], "Aarón")

    def test_si_la_base_falla_la_ficha_igual_abre(self):
        m = self.mundo()

        def roto(*a, **k):
            raise RuntimeError("bd")
        m.amb["mysql_fetchall"] = roto
        ctx = m.f("_ot_com_contexto_ficha")(77, m.fila)
        self.assertIsNone(ctx["franja"])


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
#  3c) AL CREAR LA OT: a quién se avisará y a quién se avisó
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestCreacionDeLaOt(_Base):
    def _con_usuarios(self, m, usuarios):
        orig = m._varios

        def varios(sql, params=()):
            s = " ".join(sql.split())
            if "FROM `app_users` u" in s:
                return [dict(email=e, nombre=n) for e, n in usuarios]
            if "FROM mant_tecnicos_externos te" in s or "FROM mant_tecnico_externo_usuarios" in s:
                return []
            return orig(sql, params)
        m.amb["mysql_fetchall"] = varios

    def test_antes_de_crear_dice_quien_recibira_cada_correo_con_calendario_y_visita(self):
        m = self.mundo()
        self._con_usuarios(m, [("lenin@sphs.cl", "Lenin Urbina")])
        st, d = m.llamar("ot2_api_com_creacion_previa", cuerpo={"cliente_id": 5, "tecnico_user_ids": [31], "contacto_email": "Contacto@Gimnasio.cl",
                                                               "contacto_nombre": "María"})
        self.assertEqual(st, 200)
        self.assertTrue(d["texto"].startswith("Al crear esta OT se enviará: María (Cliente)"), d["texto"])
        por = {i["correo"]: i for i in d["items"]}
        self.assertEqual(por["contacto@gimnasio.cl"]["nota"], "recibe el aviso de la visita agendada")
        self.assertEqual(por["lenin@sphs.cl"]["nota"], "recibe el correo con el archivo de calendario")
        self.assertEqual([i["correo"] for i in d["items"]], ["contacto@gimnasio.cl", "admin@gimnasio.cl", "lenin@sphs.cl"])
        self.assertNotIn("aaron@sphs.cl", por)                                          # al crear no se avisa a la gestión por correo

    def test_marca_lo_que_esta_apagado_con_las_claves_de_la_creacion(self):
        m = self.mundo()
        self._con_usuarios(m, [("lenin@sphs.cl", "Lenin Urbina")])
        m.apagar_correo("ot_tec_asignada")
        m.cerrar_llave("mantenciones")
        _st, d = m.llamar("ot2_api_com_creacion_previa", cuerpo={"cliente_id": 5, "tecnico_user_ids": [31], "contacto_email": "c@g.cl"})
        por = {i["correo"]: i for i in d["items"]}
        self.assertFalse(por["c@g.cl"]["saldra"])
        self.assertIn("Mantenciones", por["c@g.cl"]["motivo"])
        self.assertFalse(por["lenin@sphs.cl"]["saldra"])
        self.assertIn("«OT asignada: aviso al técnico»", por["lenin@sphs.cl"]["motivo"])

    def test_un_trabajo_interno_solo_avisa_al_tecnico(self):
        m = self.mundo()
        self._con_usuarios(m, [("lenin@sphs.cl", "Lenin Urbina")])
        _st, d = m.llamar("ot2_api_com_creacion_previa", cuerpo={"cliente_id": None, "tecnico_user_ids": [31]})
        self.assertEqual([i["grupo"] for i in d["items"]], ["tecnico"])

    def test_un_tecnico_que_crea_no_ve_correos_de_otros(self):
        m = self.mundo(tecnico=True)
        self._con_usuarios(m, [("lenin@sphs.cl", "Lenin Urbina")])
        _st, d = m.llamar("ot2_api_com_creacion_previa", cuerpo={"cliente_id": 5, "tecnico_user_ids": [31], "contacto_email": "c@g.cl"})
        self.assertNotIn("@", json.dumps(d, ensure_ascii=False))

    def test_entradas_raras_no_rompen(self):
        m = self.mundo()
        self._con_usuarios(m, [])
        for cuerpo in ({}, {"cliente_id": "x", "tecnico_user_ids": ["a", None, 5]}, {"tecnico_user_ids": list(range(100))}):
            st, d = m.llamar("ot2_api_com_creacion_previa", cuerpo=cuerpo)
            self.assertEqual((st, d["ok"]), (200, True), cuerpo)

    def test_despues_de_crear_cuenta_lo_que_paso_con_cada_correo_y_deja_constancia(self):
        m = self.mundo(estado="programada")
        m.avisos_tec = ["lenin@sphs.cl"]
        m.email_log_cli = [{"destinatario": "contacto@gimnasio.cl", "estado": "enviado"}, {"destinatario": "admin@gimnasio.cl", "estado": "bloqueado"}]
        r = m.f("_ot_com_creacion_resultado")(77)
        por = {x["correo"]: x for x in r}
        self.assertEqual(por["lenin@sphs.cl"]["resultado"], "enviado")
        self.assertEqual(por["contacto@gimnasio.cl"]["resultado"], "enviado")
        self.assertEqual(por["admin@gimnasio.cl"]["resultado"], "frenado")
        fila = m.tabla[-1]
        self.assertEqual((fila["tipo"], fila["cierre_clave"]), (C.TIPO_CREACION, None))
        (_e, _i, accion, texto), = m.logs
        self.assertEqual(accion, "ot_creada_aviso")
        self.assertIn("Lenin Urbina (Técnico) <lenin@sphs.cl> — enviado", texto)
        self.assertEqual(m.m.enviados_reales, [])                                      # SOLO informa: no envía nada

    def test_el_tecnico_sin_aviso_y_el_cliente_sin_constancia_se_dicen_sin_inventar(self):
        m = self.mundo(estado="programada")
        r = m.f("_ot_com_creacion_resultado")(77)
        por = {x["correo"]: x for x in r}
        self.assertEqual(por["lenin@sphs.cl"]["resultado"], "fallo")
        self.assertEqual(por["lenin@sphs.cl"]["motivo"], "El correo no salió.")
        self.assertEqual(por["contacto@gimnasio.cl"]["resultado"], "no_enviado")
        self.assertIn("No hay constancia", por["contacto@gimnasio.cl"]["motivo"])

    def test_lo_frenado_por_una_llave_se_cuenta_como_frenado(self):
        m = self.mundo(estado="programada")
        m.cerrar_llave("ot_tecnicos")
        m.avisos_tec = []
        r = {x["correo"]: x for x in m.f("_ot_com_creacion_resultado")(77)}
        self.assertEqual(r["lenin@sphs.cl"]["resultado"], "frenado")
        self.assertIn("Correos a técnicos (OT)", r["lenin@sphs.cl"]["motivo"])

    def test_un_trabajo_interno_no_cuenta_al_cliente(self):
        m = self.mundo(estado="programada")
        m.fila.update(cliente_id=None, cli_razon=None)
        r = m.f("_ot_com_creacion_resultado")(77, interna=True)
        self.assertEqual({x["grupo"] for x in r}, {"tecnico"})

    def test_nunca_levanta(self):
        m = self.mundo()

        def roto(*a, **k):
            raise RuntimeError("bd")
        m.amb["mysql_fetchone"] = roto
        self.assertEqual(m.f("_ot_com_creacion_resultado")(77), [])


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
#  4) CABLEADO: puntos de llamada, candados, arranque barato, variable apagada en el repositorio, front
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
from tests import _fuente_app as FA  # noqa: E402

PAT_NOMBRE_FINANCIERO = re.compile(r"finanz|costo|factura|autoriz|regulariz|dashboard|facprov|margen|valoriz", re.I)
RUTAS_NUEVAS = {
    "ot2_api_com_destinatarios": ('"/ot/api/<int:vid>/correo/destinatarios"', ["GET"]),
    "ot2_api_com_enviar_datos": ('"/ot/api/<int:vid>/correo/enviar"', ["GET"]),
    "ot2_api_com_vista_previa": ('"/ot/api/<int:vid>/correo/vista-previa"', ["POST"]),
    "ot2_api_com_enviar": ('"/ot/api/<int:vid>/correo/enviar"', ["POST"]),
    "ot2_api_com_creacion_previa": ('"/ot/api/correo/creacion"', ["POST"]),
}


class TestCableado(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = _leer("app.py")
        cls.detalle = _leer("templates/ot2/detalle.html")
        cls.crear = _leer("templates/ot2/_modal_crear.html")
        cls.js = _leer("static/ot_envio.js")
        cls.css = _leer("static/ot_envio.css")
        cls.parcial = _leer("templates/ot2/_enviar_ot.html")
        a = cls.app.index("#  COMUNICACIÓN DE LA OT — «OT terminada» automática")
        a = cls.app.rindex("# ══════", 0, a)
        cls.bloque = cls.app[a:cls.app.index("# ── PROPUESTA DE PLAN DE MANTENCIÓN POR EMAIL (2026-06-11)")]

    # ── rutas y candados ──
    def test_cada_ruta_existe_una_sola_vez_con_su_metodo(self):
        for fn, (ruta, metodos) in RUTAS_NUEVAS.items():
            decs = FA.decoradores(fn)
            rutas = [d for d in decs if d.startswith("@app.route(")]
            self.assertEqual(len(rutas), 1, fn)
            self.assertIn(ruta, rutas[0], fn)
            for met in metodos:
                self.assertIn(f'"{met}"', rutas[0], fn)
            self.assertEqual(len(re.findall(r"^def %s\(" % fn, self.app, re.M)), 1, fn)

    def test_el_aviso_previo_lo_ve_un_tecnico_y_lo_demas_es_solo_gestion(self):
        previo = FA.decoradores("ot2_api_com_destinatarios")
        self.assertIn("@_mant_required", previo)
        self.assertIn("@_ot_can_view", previo)
        self.assertNotIn("@_no_tecnico", previo)                                  # el técnico lo necesita al firmar
        for fn in ("ot2_api_com_enviar_datos", "ot2_api_com_vista_previa", "ot2_api_com_enviar"):
            decs = FA.decoradores(fn)
            self.assertEqual([d for d in decs if not d.startswith("@app.route")], ["@_mant_required", "@_no_tecnico", "@_ot_can_view"], fn)
            self.assertIn("_ot_com_puede_enviar(vid)", FC.funcion_top("app.py", fn), fn)           # y además el permiso de aprobar de esa OT
        self.assertIn("@_mant_required", FA.decoradores("ot2_api_com_creacion_previa"))

    def test_el_permiso_de_enviar_es_el_de_aprobar_y_nunca_de_un_tecnico(self):
        src = FC.funcion_top("app.py", "_ot_com_puede_enviar")
        self.assertIn("not _es_rol_tecnico()", src)
        self.assertIn('_puede_ot_accion(vid, "aprobar")', src)

    def test_nada_de_nombres_financieros_en_las_rutas_nuevas(self):
        for fn, (ruta, _m) in RUTAS_NUEVAS.items():
            self.assertIsNone(PAT_NOMBRE_FINANCIERO.search(fn), fn)
            self.assertIsNone(PAT_NOMBRE_FINANCIERO.search(ruta), ruta)

    def test_la_bitacora_de_estos_avisos_no_la_ve_un_tecnico(self):
        """Las acciones que se guardan en la bitácora no están en la lista blanca del técnico: él no ve a quién se le escribió."""
        txt = FC.constante_top("app.py", "_ACT_TEC_PERMITIDAS")
        for accion in ("ot_terminada_aviso", "ot_enviada_correo", "ot_creada_aviso"):
            self.assertNotIn(accion, txt, accion)
            self.assertIn(f'"{accion}"', self.bloque, accion)

    # ── dónde se llama ──
    def test_el_aviso_de_ot_terminada_sale_de_un_solo_punto_y_se_llama_al_aprobar_y_al_cerrar_auditable(self):
        self.assertEqual(len(re.findall(r"^def _ot_terminada_notificar\(", self.app, re.M)), 1)
        aprobar = FC.funcion_top("app.py", "mant_ot_aprobar_cierre")
        self.assertEqual(aprobar.count("_ot_terminada_notificar("), 1)
        self.assertLess(aprobar.index("estado='cerrada'"), aprobar.index("_ot_terminada_notificar("))          # después del UPDATE atómico del cierre
        self.assertLess(aprobar.index("_ot_terminada_notificar("), aprobar.index("_resp_cierre = "))
        # best-effort: dentro de un try/except que no deja pasar el error
        i = aprobar.index("_ot_terminada_notificar(")
        self.assertIn("try:", aprobar[i - 120:i])
        self.assertRegex(aprobar[i:i + 400], r"except Exception as _e_ot_term:\n\s+print\(")
        cerrar = FC.funcion_top("app.py", "mant_visita_cerrar")
        self.assertEqual(cerrar.count("_ot_terminada_notificar("), 1)
        self.assertLess(cerrar.index("_ot_terminada_notificar("), cerrar.index('"mensaje": "OT cerrada exitosamente'))

    def test_el_cierre_de_un_levantamiento_no_manda_ot_terminada_porque_no_tiene_documento_firmado(self):
        self.assertNotIn("_ot_terminada_notificar(", FC.funcion_top("app.py", "mant_lev_cerrar"))

    def test_no_se_resucita_la_funcion_borrada_ni_su_variable(self):
        self.assertNotRegex(self.app, r"(?m)^def _notificar_cierre_ot_async\(")
        self.assertNotIn("MANT_NOTIF_CLIENTE_EMAIL_ENABLED", self.bloque)
        self.assertNotIn("_notificar_cierre_ot_async", self.bloque)
        self.assertNotIn("MANT_NOTIF_CLIENTE_EMAIL_ENABLED", _leer("ot_comunicacion.py"))

    def test_los_correos_salen_siempre_por_la_puerta_unica_con_su_clave(self):
        self.assertNotIn("_send_ilus_email_real", self.bloque)
        self.assertNotIn("smtplib", self.bloque)
        for clave in C.CORREOS:
            self.assertRegex(self.bloque, r'correo="' + clave + '"', clave)
        # el técnico (automático y manual) por su llave propia; el cliente y el equipo por la de Servicio Técnico
        self.assertRegex(self.bloque, r'modulo="ot_tecnicos", ref=ref,\s+correo="ot_tec_envio_manual"')
        self.assertRegex(self.bloque, r'modulo="mantenciones", ref=ref,\s+correo="ot_envio_manual"')
        self.assertIn('correo="ot_tec_terminada"', self.bloque)
        self.assertEqual(self.bloque.count("asincrono=True"), 2)               # el aviso automático encola (el cierre no espera al SMTP)
        self.assertNotIn("asincrono=True", FC.funcion_top("app.py", "ot2_api_com_enviar"))     # el manual espera para decir la verdad

    def test_el_aviso_se_manda_solo_a_correos_validados(self):
        self.assertIn("_ocom.correo_valido", self.bloque)

    def test_la_ventana_de_15_minutos_usa_el_mismo_reloj_que_guarda_la_base(self):
        """`creado_at` es DEFAULT CURRENT_TIMESTAMP (la hora del servidor de la base, igual que `NOW()`): compararla con UTC_TIMESTAMP() corre la
        ventana por el huso del servidor y el aviso de la creación podría no verse nunca (o verse horas)."""
        self.assertNotIn("UTC_TIMESTAMP", self.bloque)
        self.assertEqual(self.bloque.count("INTERVAL 15 MINUTE"), 2)
        self.assertEqual(self.bloque.count("(NOW() - INTERVAL 15 MINUTE)"), 2)

    # ── la variable apagada ──
    def test_la_variable_del_cliente_no_se_enciende_en_ningun_archivo_del_repositorio(self):
        """REGLA #22: el aviso al cliente NACE APAGADO y solo Daniel lo enciende (en Cloud Run), nunca un commit."""
        permitidos = {".py", ".md", ".js", ".html", ".txt"}
        sospechosos = []
        for carpeta, subs, archivos in os.walk(RAIZ):
            subs[:] = [s for s in subs if s not in (".git", ".claude", "node_modules", "__pycache__", ".pytest_cache", "venv", ".venv")]
            for nombre in archivos:
                ruta = os.path.join(carpeta, nombre)
                ext = os.path.splitext(nombre)[1].lower()
                if ext in permitidos or ext in (".png", ".jpg", ".jpeg", ".gif", ".ico", ".woff", ".woff2", ".pdf", ".zip", ".docx", ".xlsx", ".svg"):
                    continue
                try:
                    with open(ruta, encoding="utf-8", errors="ignore") as fh:
                        if C.ENV_CLIENTE_ACTIVA in fh.read():
                            sospechosos.append(os.path.relpath(ruta, RAIZ))
                except OSError:
                    pass
        self.assertEqual(sospechosos, [], "la variable aparece en: %s" % sospechosos)
        deploy = os.path.join(RAIZ, ".github", "workflows", "deploy.yml")
        if os.path.exists(deploy):
            self.assertNotIn(C.ENV_CLIENTE_ACTIVA, _leer(".github/workflows/deploy.yml"))
        for fuente in ("app.py", "ot_comunicacion.py", "comm_correos_registro.py"):
            self.assertNotRegex(_leer(fuente), r"os\.environ\[\s*['\"]" + C.ENV_CLIENTE_ACTIVA, fuente)
            self.assertNotRegex(_leer(fuente), r"environ\.setdefault\(\s*['\"]" + C.ENV_CLIENTE_ACTIVA, fuente)

    def test_el_codigo_solo_lee_la_variable_por_la_funcion_unica(self):
        # el plan, el chequeo del documento (solo si va a salir) y el aviso previo: todos por `_ocom.cliente_activo()`, que lee la MISMA
        # condición que Control de correos muestra como «Apagado por configuración»
        self.assertEqual(len(re.findall(r"_ocom\.cliente_activo\(\)", self.bloque)), 3)
        self.assertNotIn("os.environ", self.bloque)

    # ── arranque barato (REGLA #18) ──
    def test_la_tabla_es_un_solo_create_barato_con_clave_unica_por_aprobacion(self):
        src = FC.funcion_top("app.py", "_ensure_ot_comunicacion_tablas")
        self.assertEqual(src.count("CREATE TABLE IF NOT EXISTS mant_ot_comunicaciones"), 1)
        self.assertNotIn("ALTER", src)
        self.assertIn("UNIQUE KEY uq_ot_com_cierre (visita_id, tipo, cierre_clave)", src)
        self.assertIn("cierre_clave   VARCHAR(40) NULL", src)                                      # NULL en manual/creación: varias filas por OT
        # la guardia de DDL del arranque la reconoce y la salta cuando ya existe (sin pedir lock): una sola cláusula
        sql = re.search(r'mysql_execute\("""(.*?)"""\)', src, re.S).group(1)
        self.assertIsNone(re.search(r",\s*(ADD|MODIFY|CHANGE|DROP|ALTER|RENAME)\s", sql, re.I))
        self.assertEqual(re.match(r"^\s*CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+`?(\w+)`?", sql, re.I).group(1), "mant_ot_comunicaciones")

    def test_la_constancia_no_se_borra_ni_se_reescribe_salvo_el_resultado_de_su_propio_aviso(self):
        self.assertNotRegex(self.bloque, r"DELETE\s+FROM\s+mant_ot_comunicaciones")
        updates = re.findall(r"UPDATE\s+mant_ot_comunicaciones[^\"]*", self.bloque)
        self.assertEqual(len(updates), 1)
        self.assertIn("WHERE visita_id=%s AND tipo=%s AND cierre_clave=%s", self.bloque)

    def test_el_arranque_la_crea_siempre_y_dentro_de_un_try(self):
        i = self.app.rindex("_ensure_ot_comunicacion_tablas()")
        trozo = self.app[i - 200:i + 400]
        self.assertIn("with app.app_context():", trozo)
        self.assertIn("_ensure_comm_template_ot_comunicacion()", trozo)
        self.assertGreater(i, self.app.rindex("_ensure_comm_template_ot_tecnicos()"))              # después de las plantillas de técnicos
        self.assertIn("except Exception as _ensure_otcom_err:", self.app[i:i + 700])

    def test_las_plantillas_se_siembran_solo_si_no_existen(self):
        m = MundoOT()
        try:
            m.f("_ensure_comm_template_ot_comunicacion")()
            ins = [s for s, _p in m.sql if s.startswith("INSERT IGNORE INTO comm_templates")]
            self.assertEqual(len(ins), 4)
            m.sql.clear()
            for slug in C.PLANTILLAS:
                m.m.poner_plantilla("mantenciones", slug, "Asunto editado por Daniel", "<p>Cuerpo editado por Daniel</p>")
            m.f("_ensure_comm_template_ot_comunicacion")()
            self.assertEqual([s for s, _p in m.sql if "comm_templates" in s], [])                 # nunca pisa lo que Daniel editó
            m.sql.clear()
            del m.bd.plantillas[("mantenciones", C.PLANTILLA_MANUAL)]
            m.f("_ensure_comm_template_ot_comunicacion")()
            self.assertEqual(len([s for s, _p in m.sql if s.startswith("INSERT IGNORE INTO comm_templates")]), 1)
        finally:
            m.cerrar()

    # ── front: reglas del proyecto ──
    def test_sin_popups_nativos(self):
        for nombre, txt in (("ot_envio.js", self.js), ("_enviar_ot.html", self.parcial)):
            sin_comentarios = re.sub(r"/\*.*?\*/", "", txt, flags=re.S)
            for prohibido in ("alert(", "confirm(", "prompt("):
                self.assertNotRegex(re.sub(r"(ilus)(Alert|Confirm|Prompt)\(", "", sin_comentarios), r"(?<![A-Za-z.])" + re.escape(prohibido), (nombre, prohibido))
        self.assertIn("ilusToast", self.js)

    def test_boton_de_44_px_input_de_16_px_y_celular(self):
        self.assertIn("min-height:44px", self.css)
        self.assertIn("font-size:16px", self.css)
        self.assertIn("modal-fullscreen-sm-down", self.js)
        self.assertIn("prefers-reduced-motion", self.css)
        self.assertIn(".oe-modal .btn-close{width:44px;height:44px", self.css)

    def test_los_destinatarios_se_marcan_y_desmarcan_todos_regla_14(self):
        self.assertIn("data-oe-todo", self.js)
        self.assertIn("Marcar todos", self.js)
        self.assertIn("Desmarcar todos", self.js)
        self.assertIn("sinMarcar()", self.js)

    def test_nombres_completos_nunca_cortados_regla_15(self):
        self.assertNotIn("text-overflow:ellipsis", self.css)
        self.assertNotIn("-webkit-line-clamp", self.css)
        self.assertIn("overflow-wrap:anywhere", self.css)

    def test_el_modal_se_crea_en_el_body_y_la_vista_previa_va_aislada(self):
        self.assertIn("document.body.appendChild(el)", self.js)                                   # nunca dentro de una pestaña ni del hero
        self.assertIn('sandbox=""', self.js)
        self.assertIn(".srcdoc =", self.js)

    def test_todo_lo_que_viene_del_servidor_o_se_escribe_pasa_por_esc(self):
        self.assertIn("function esc(", self.js)
        for fragmento in ("esc(quien)", "esc(i.correo)", "esc(i.nota)", "esc(i.motivo)",                         # el aviso previo
                          "esc(d.nombre || d.correo)", "esc(d.correo)", "esc(d.rol)", "esc(d.rol_txt || 'Otro destinatario')",     # los destinatarios
                          "esc(t.texto)", "esc(r.error)",                                                          # el resultado
                          "esc(a.titulo)", "esc(a.cuando)", "esc(a.quien)", "esc(l)",                              # lo ya enviado
                          "esc(S.asunto)", "esc(S.mensaje)", "esc(D.razon"):                                       # lo que escribió la persona
            self.assertTrue(fragmento in self.js, fragmento)
        # y el comportamiento: el escape neutraliza una etiqueta
        import subprocess
        trozo = re.search(r"function esc\(s\) \{.*?\n  \}", self.js, re.S).group(0)
        try:
            r = subprocess.run(["node", "-e", trozo + "; console.log(esc('<img src=x onerror=alert(1)> & \"comillas\" \\'simples\\''))"],
                               capture_output=True, text=True, timeout=30)
        except (OSError, subprocess.SubprocessError):
            self.skipTest("node no está disponible")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertNotIn("<img", r.stdout)
        self.assertIn("&lt;img src=x onerror=alert(1)&gt; &amp; &quot;comillas&quot; &#39;simples&#39;", r.stdout)

    def test_la_ficha_incluye_el_parcial_al_nivel_raiz_y_no_dentro_de_ninguna_condicion(self):
        i = self.detalle.index("{% include 'ot2/_enviar_ot.html' %}")
        self.assertEqual(_ifs_abiertos(self.detalle, i), [])
        self.assertGreater(i, self.detalle.index("{% include 'ot2/_que_paso.html' %}"))
        self.assertLess(i, self.detalle.index('<div class="otd-tabs" id="otdTabs">'))
        self.assertIn("{% block content %}", self.detalle[:self.detalle.index("{% include 'ot2/_enviar_ot.html' %}")])

    def test_el_boton_de_la_fila_de_acciones_no_depende_de_poder_editar_la_ot(self):
        """Lección del 20-oct: lo que importa en una OT CERRADA no puede vivir bajo `puede_metadata` (esa tarjeta no existe cerrada)."""
        i = self.detalle.index('id="otdBtnEnviarOT"')
        pila = _ifs_abiertos(self.detalle, i)
        self.assertEqual(pila, ["com_envio and com_envio.puede_enviar"])
        self.assertFalse([c for c in pila if re.search(r"puede_metadata|puede_ejecutar|puede_configurar|ot_solo_lectura", c)])

    def test_los_avisos_previos_viven_dentro_de_sus_modales(self):
        firma, aprob = self.detalle.index('id="otfModalFirma"'), self.detalle.index('id="otfModalAprobacion"')
        a_firma = self.detalle.index('id="oeAvisoFirma"')
        a_cierre = self.detalle.index('id="oeAvisoCierre"')
        self.assertTrue(firma < a_firma < aprob)
        self.assertGreater(a_cierre, aprob)
        self.assertLess(a_cierre - aprob, 6000)
        self.assertEqual(self.detalle.count("data-oe-aviso="), 2)

    def test_el_modal_de_cierre_ya_no_promete_lo_que_no_hacia(self):
        visible = re.sub(r"\{#.*?#\}", "", self.detalle, flags=re.S)                 # lo que se dibuja, sin los comentarios de Jinja
        self.assertTrue("Se enviará la confirmación al cliente" not in visible, "el modal de cierre vuelve a prometer un correo que no salía")
        self.assertTrue("Abajo ves a quién se le avisará por correo." in visible)

    def test_la_hoja_de_estilos_se_carga_en_la_ficha(self):
        self.assertIn("filename='ot_envio.css'", self.detalle)
        self.assertIn("filename='ot_envio.js'", self.parcial)

    def test_el_asistente_de_creacion_muestra_y_cuenta_los_avisos(self):
        c = self.crear
        self.assertIn('id="o2mAvisoEnvio"', c)
        self.assertIn("/ot/api/correo/creacion", c)
        self.assertIn("Al crear esta OT se enviará:", c)
        self.assertIn("_o2mAvisoEnvioRefrescar", FA_trozo(c, "function pintarAnclas()", "function refrescarAnclas()"))      # se vuelve a pedir con cada cambio
        self.assertIn("res.j.comunicacion", c)
        self.assertIn("La OT quedó creada · avisos por correo", c)
        self.assertIn("Estos avisos NO salieron:", c)
        # el aviso previo no bloquea nunca el formulario
        self.assertIn("try { _o2mAvisoEnvioRefrescar(); } catch(_)", c)
        self.assertNotIn("alert(", re.sub(r"ilusAlert\(", "", c[c.index("function _o2mAvisoEnvioPintar"):c.index("function _o2mAvisoEnvioRefrescar") + 900]))

    def test_el_js_del_front_es_valido(self):
        node = os.environ.get("NODE") or "node"
        import subprocess
        try:
            r = subprocess.run([node, "--check", os.path.join(RAIZ, "static", "ot_envio.js")], capture_output=True, text=True, timeout=60)
        except (OSError, subprocess.SubprocessError):
            self.skipTest("node no está disponible")
        self.assertEqual(r.returncode, 0, r.stderr)


def FA_trozo(texto, desde, hasta):
    i = texto.index(desde)
    return texto[i:texto.index(hasta, i)]


def _ifs_abiertos(texto, hasta):
    """Los `{% if … %}` que siguen abiertos justo antes de la posición `hasta` (desde `{% block content %}`), sin contar lo que está en comentarios
    Jinja: sirve para saber DENTRO DE QUÉ condiciones queda algo (lo que decide si se dibuja en una OT cerrada)."""
    ini = texto.index("{% block content %}")
    trozo = re.sub(r"\{#.*?#\}", "", texto[ini:hasta], flags=re.S)
    pila = []
    for m in re.finditer(r"\{%-?\s*(if|endif)\b([^%]*?)-?%\}", trozo):
        if m.group(1) == "if":
            pila.append(m.group(2).strip())
        elif pila:
            pila.pop()
    return pila


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
#  5) LA FICHA SE DIBUJA (jinja2 de verdad), también en una OT CERRADA
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
ENV = jinja2.Environment(loader=jinja2.FileSystemLoader(os.path.join(RAIZ, "templates")), extensions=["jinja2.ext.do"], autoescape=True)


def _dibujar(**ctx):
    ctx.setdefault("url_for", lambda endpoint, **kw: "/static/" + kw.get("filename", ""))
    ctx.setdefault("v", {"id": 77})
    ctx.setdefault("es_tecnico", False)
    return ENV.get_template("ot2/_enviar_ot.html").render(**ctx)


FRANJA = {"estado": "apagado", "titulo": "La OT terminada todavía no se le envió al cliente", "detalle": "El envío automático al cliente está apagado."}


class TestLaFichaSeDibuja(unittest.TestCase):
    def test_ot_cerrada_con_el_aviso_apagado_dibuja_el_recordatorio_con_su_boton(self):
        h = _dibujar(com_envio={"puede_enviar": True, "listo": True, "franja": FRANJA, "creacion": None})
        t = _txt(h)
        self.assertIn('id="oeFranja"', h)
        self.assertIn("La OT terminada todavía no se le envió al cliente", t)
        self.assertIn("El envío automático al cliente está apagado.", t)
        self.assertIn("Cuando quieras, envíasela tú", t)
        self.assertIn("data-oe-abrir", h)
        self.assertIn("Enviar OT por correo", t)
        self.assertIn('src="/static/ot_envio.js"', h)
        self.assertIn("gestion: true", h)

    def test_sin_el_permiso_de_enviar_no_se_dibuja_ni_con_franja(self):
        h = _dibujar(com_envio={"puede_enviar": False, "franja": FRANJA, "creacion": None})
        self.assertNotIn("oeFranja", h)
        self.assertNotIn("data-oe-abrir", h)

    def test_un_tecnico_no_recibe_recordatorio_ni_boton_pero_si_el_script_del_aviso_previo(self):
        h = _dibujar(es_tecnico=True, com_envio={"puede_enviar": False, "listo": False, "franja": None, "creacion": None})
        self.assertNotIn("oeFranja", h)
        self.assertNotIn("oeCreacion", h)
        self.assertIn("gestion: false", h)
        self.assertIn("ot_envio.js", h)

    def test_sin_contexto_igual_renderiza(self):
        for ctx in ({}, {"com_envio": None}, {"com_envio": {}}):
            h = _dibujar(**ctx)
            self.assertIn("OTD_OE_CFG", h)
            self.assertNotIn("oeFranja", h)

    def test_los_avisos_de_la_creacion_van_escapados(self):
        h = _dibujar(com_envio={"puede_enviar": True, "franja": None,
                                "creacion": {"id": 9, "quien": "Aarón <b>", "cuando": "21/10/2026 11:30",
                                             "lineas": ["Lenin (Técnico) <l@x.cl> — enviado", "<script>alert(1)</script>"]}})
        self.assertIn('id="oeCreacion"', h)
        self.assertIn('data-oe-id="9"', h)
        self.assertNotIn("<script>alert(1)", h)
        self.assertIn("&lt;script&gt;", h)
        self.assertIn("Lenin (Técnico) &lt;l@x.cl&gt; — enviado", h)
        self.assertIn("Aarón &lt;b&gt;", h)

    def test_el_contexto_de_verdad_dibuja_lo_mismo_de_punta_a_punta(self):
        """Del servidor a la plantilla: lo que arma `_ot_com_contexto_ficha` para una OT cerrada es lo que la plantilla dibuja."""
        m = MundoOT()
        try:
            m.tabla.append({"id": 1, "visita_id": 77, "tipo": C.TIPO_TERMINADA, "cierre_clave": "2026-10-21 14:30:05", "cliente_estado": "apagado",
                            "detalle_json": "[]", "creado_por": "Aarón"})
            ctx = m.f("_ot_com_contexto_ficha")(77, m.fila)
            h = _dibujar(com_envio=ctx)
            self.assertIn('id="oeFranja"', h)
            self.assertIn("El envío automático al cliente está apagado.", _txt(h))
        finally:
            m.cerrar()


if __name__ == "__main__":
    unittest.main()
