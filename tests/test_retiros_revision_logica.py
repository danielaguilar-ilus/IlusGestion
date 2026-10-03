# -*- coding: utf-8 -*-
"""Revisión adversarial de la Guía de 6 pasos y de la preparación por Check (Retiros, 2026-10-02).

Dos tipos de prueba en este archivo:

  · «GUARDIAS» — cosas que deben cumplirse siempre (el cliente real RET-VQJ58N no recibe nada
    nuevo, Check solo se consulta, un retiro avanzado no pide nada retroactivo, la huella de
    productos no cambia sola...).
  · «REGRESIONES» (clase `Regresion*`) — cada una reproduce una falla real encontrada en la
    revisión (los «hallazgos» 1-11) y verifica que el arreglo se mantiene. Nacieron como «defectos a
    propósito» (fallaban) y pasaron a pruebas normales cuando el producto se corrigió. El docstring
    dice qué falla era. Si una falla otra vez, el producto volvió a romperse.

El marcado automático de «preparado» exige dos lecturas «listo» separadas por
RETIROS_CHECK_CONFIRMACION_S segundos (25 por defecto): este archivo la deja en 0 mientras corre.

Todo corre sin BD, sin Check, sin ERP y sin correo (tests/_arnes_retiros.py).

    py -m pytest tests/test_retiros_revision_logica.py -q
"""
import os
import sys
import threading
import time
import unittest

_TESTS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _TESTS)
sys.path.insert(0, os.path.dirname(_TESTS))

import _arnes_retiros as A  # noqa: E402
import retiros_check as rc  # noqa: E402
import retiros_guia as rg  # noqa: E402
from _arnes_retiros import fila_check, respuesta_check  # noqa: E402
from flask import g  # noqa: E402


# ──────────────────────────────────────────────────────────────────────────────
#  Entorno: sin espera entre las dos lecturas «listo» y con el marcado automático encendido
# ──────────────────────────────────────────────────────────────────────────────
_ENV_ANTES = {}


def setUpModule():
    for k, v in (("RETIROS_CHECK_CONFIRMACION_S", "0"), ("RETIROS_CHECK_AUTO", None)):
        _ENV_ANTES[k] = os.environ.get(k)
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


def tearDownModule():
    for k, v in _ENV_ANTES.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


# ──────────────────────────────────────────────────────────────────────────────
#  Utilidades
# ──────────────────────────────────────────────────────────────────────────────
def _nuevo(status="solicitud_recibida", docs=1, **kw):
    """App de mentira con UN retiro (id 1) y `docs` documentos BLV. Por defecto YA tiene responsable (Daniel 2026-10-02: sin responsable
    no se avanza; esa regla se prueba aparte, pasando responsable_user_id=None y responsable_nombre=None)."""
    app, db, ctx, esp = A.construir_app()
    kw.setdefault("responsable_user_id", 7)
    kw.setdefault("responsable_nombre", "Sam")
    db.nueva_solicitud(1, status=status, **kw)
    for i in range(docs):
        db.agregar_doc(1, "BLV", str(23732 + i).zfill(10))
    db.reiniciar_registro()
    return app, db, ctx, esp


def _guia(app, rid=1):
    r = app.test_client().get(f"/retiros/{rid}/guia")
    assert r.status_code == 200, r.data
    return r.get_json()


def _estados(gu):
    return [p["estado"] for p in gu["pasos"]]


def _sin_mensajes(test, esp):
    """Nada le escribió a nadie: ni correo, ni WhatsApp (la campana interna se cuenta aparte)."""
    A.esperar_hilos_de_aviso()
    time.sleep(0.2)
    test.assertEqual(esp.correo.call_args_list, [], "salió un correo")
    test.assertEqual(esp.whatsapp.call_args_list, [], "salió un WhatsApp")


def _fila_f(**kw):
    base = {"solicitado": "0", "noAsignado": "0", "asignado": "0", "pickeado": "0", "revisado": "0",
            "despachado": "0", "cancelado": "0", "devuelto": "0"}
    base.update({k: str(v) for k, v in kw.items()})
    return base


def _doc_c(*filas):
    return rc.resumir_filas(list(filas))


# ══════════════════════════════════════════════════════════════════════════════
#  GUARDIAS — el cliente real y la regla «nada nuevo le escribe»
# ══════════════════════════════════════════════════════════════════════════════
class GuardiaClienteReal(unittest.TestCase):
    """Cliente real RET-VQJ58N: cita confirmada, en preparación, sin marca «Confirmo»."""

    def _real(self, **kw):
        app, db, ctx, esp = _nuevo("en_preparacion", confirmed_date="2026-10-03", proposed_date="2026-10-03",
                                   responsable_user_id=7, responsable_nombre="Milagros", **kw)
        return app, db, ctx, esp

    def test_retiro_avanzado_deja_1_y_3_hechos_y_no_pide_confirmar_nada(self):
        app, db, ctx, esp = self._real()
        gu = _guia(app)
        self.assertEqual(_estados(gu)[:4], ["hecho", "hecho", "hecho", "hecho"])
        self.assertEqual(gu["pasos"][4]["estado"], "espera")
        for p in gu["pasos"]:
            self.assertNotIn((p.get("accion") or {}).get("tipo"), ("confirmar_docs", "confirmar_productos"),
                             f"paso {p['n']} pide una confirmación retroactiva")

    def test_abrir_la_guia_no_escribe_nada_ni_manda_nada(self):
        app, db, ctx, esp = self._real()
        _guia(app)
        _guia(app)
        self.assertEqual(db.escrituras, [], "abrir la guía escribió en la BD")
        _sin_mensajes(self, esp)
        self.assertEqual(esp.check.llamadas, [], "abrir la guía le habló a Check")

    def test_confirmar_en_un_retiro_ya_avanzado_no_escribe_y_dice_que_ya_esta(self):
        app, db, ctx, esp = self._real()
        for ruta in ("confirmar-docs", "confirmar-productos"):
            r = app.test_client().post(f"/retiros/1/{ruta}")
            self.assertEqual(r.status_code, 200)
            self.assertTrue(r.get_json().get("ya"))
        self.assertEqual(db.escrituras, [])
        _sin_mensajes(self, esp)

    def test_check_solo_se_consulta_por_la_ruta_de_la_lista_blanca(self):
        app, db, ctx, esp = self._real()
        esp.check.respuestas["*"] = respuesta_check(fila_check(solicitado=1, noAsignado=1))
        r = app.test_client().get("/retiros/1/check-preparacion")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(esp.check.rutas, {"/api/ext/GetSeguimientoDespacho"})
        self.assertFalse(r.get_json()["aplicado_ahora"])
        self.assertEqual(db.escrituras, [])

    def test_check_listo_marca_preparado_sin_escribirle_al_cliente(self):
        """Lo único que cambia es la lista de bodega + un evento; ni correo ni WhatsApp ni cambio de estado."""
        app, db, ctx, esp = self._real()
        db.agregar_picking(1, "DISCO25", 1)
        db.reiniciar_registro()
        esp.check.respuestas["*"] = respuesta_check(fila_check(solicitado=1, pickeado=1))
        r = app.test_client().get("/retiros/1/check-preparacion")
        self.assertTrue(r.get_json()["aplicado_ahora"])
        _sin_mensajes(self, esp)
        self.assertEqual(db.solicitudes[1]["status"], "en_preparacion")
        sqls = [e[0].upper() for e in db.escrituras]
        self.assertFalse(any(s.startswith("UPDATE `PICKUP_REQUESTS`") for s in sqls), "tocó pickup_requests")
        self.assertEqual(len(db.logs_de(1, "picking_completo")), 1)

    def test_solo_lectura_nunca_escribe_aunque_check_diga_listo(self):
        app, db, ctx, esp = self._real()
        db.agregar_picking(1, "DISCO25", 1)
        db.reiniciar_registro()
        esp.check.respuestas["*"] = respuesta_check(fila_check(solicitado=1, pickeado=1))
        r = app.test_client().get("/retiros/1/check-preparacion?solo_lectura=1")
        self.assertFalse(r.get_json()["aplicado_ahora"])
        self.assertEqual(db.escrituras, [])

    def test_con_cita_pero_sin_preparacion_check_jamas_marca_preparado(self):
        # Cita lejana: no se mueve sola. (Con la cita cercana el retiro pasa solo a «En preparación», ver test_retiros_prep_auto.py;
        # lo que NUNCA hace Check es marcar «preparado» sobre un retiro que aún no estaba en preparación.)
        app, db, ctx, esp = _nuevo("agenda_confirmada", confirmed_date="2099-01-01", responsable_nombre="M")
        esp.check.respuestas["*"] = respuesta_check(fila_check(solicitado=1, pickeado=1))
        r = app.test_client().get("/retiros/1/check-preparacion")
        self.assertFalse(r.get_json()["aplicado_ahora"])
        self.assertEqual(db.escrituras, [])

    def test_con_cita_cercana_solo_se_mueve_el_estado_nunca_se_marca_preparado(self):
        import datetime as dt
        cerca = (dt.date.today() + dt.timedelta(days=1)).isoformat()
        app, db, ctx, esp = _nuevo("agenda_confirmada", confirmed_date=cerca, responsable_nombre="M")
        esp.check.respuestas["*"] = respuesta_check(fila_check(solicitado=1, pickeado=1))
        d = app.test_client().get("/retiros/1/check-preparacion").get_json()
        self.assertFalse(d["aplicado_ahora"])           # «preparado»: no, todavía no era un retiro en preparación
        self.assertEqual([x for x in db.logs if x["action"] == "picking_completo"], [])
        self.assertFalse(any(it["picked"] for it in db.picking))

    def test_check_sin_respuesta_no_inventa_nada(self):
        app, db, ctx, esp = self._real()
        r = app.test_client().get("/retiros/1/check-preparacion")  # esp.check devuelve None
        d = r.get_json()
        self.assertEqual(d["evaluacion"]["estado"], "sin_conexion")
        self.assertFalse(d["aplicado_ahora"])
        self.assertEqual(db.escrituras, [])


class GuardiaConfirmar(unittest.TestCase):
    def test_confirmar_solo_deja_un_registro_en_pickup_logs(self):
        app, db, ctx, esp = _nuevo("solicitud_recibida")
        r = app.test_client().post("/retiros/1/confirmar-docs")
        self.assertEqual(r.status_code, 200, r.data)
        self.assertEqual(len(db.escrituras), 1)
        lg = db.logs_de(1, "docs_confirmadas")
        self.assertEqual(len(lg), 1)
        self.assertEqual(lg[0]["new_status"], "solicitud_recibida")   # no cambia el estado
        self.assertRegex(lg[0]["notes"], r"^firma=[0-9a-f]{12} · ")
        self.assertEqual(lg[0]["actor_name"], "Samantha Blacio")      # sale de la sesión, no del navegador
        _sin_mensajes(self, esp)

    def test_confirmar_es_idempotente(self):
        app, db, ctx, esp = _nuevo("solicitud_recibida")
        c = app.test_client()
        c.post("/retiros/1/confirmar-docs")
        r = c.post("/retiros/1/confirmar-docs")
        self.assertTrue(r.get_json().get("ya"))
        self.assertEqual(len(db.logs_de(1, "docs_confirmadas")), 1)

    def test_no_se_confirman_productos_antes_que_las_facturas(self):
        app, db, ctx, esp = _nuevo("solicitud_recibida")
        r = app.test_client().post("/retiros/1/confirmar-productos")
        self.assertEqual(r.status_code, 409)
        self.assertEqual(db.escrituras, [])

    def test_sin_documentos_no_se_puede_confirmar(self):
        app, db, ctx, esp = _nuevo("solicitud_recibida", docs=0)
        r = app.test_client().post("/retiros/1/confirmar-docs")
        self.assertEqual(r.status_code, 409)
        self.assertEqual(db.escrituras, [])

    def test_retiro_terminado_no_se_confirma(self):
        for st in ("rechazada", "fallida", "cerrada", "retirada"):
            app, db, ctx, esp = _nuevo(st)
            r = app.test_client().post("/retiros/1/confirmar-docs")
            self.assertEqual(r.status_code, 409, st)
            self.assertEqual(db.escrituras, [], st)

    def test_flujo_completo_pasa_de_rojo_a_verde(self):
        app, db, ctx, esp = _nuevo("solicitud_recibida", responsable_user_id=7, responsable_nombre="Sam")
        c = app.test_client()
        self.assertEqual(_estados(_guia(app))[:3], ["actual", "hecho", "pendiente"])
        c.post("/retiros/1/confirmar-docs")
        self.assertEqual(_estados(_guia(app))[:3], ["hecho", "hecho", "actual"])
        c.post("/retiros/1/confirmar-productos")
        gu = _guia(app)
        self.assertEqual(_estados(gu)[:4], ["hecho", "hecho", "hecho", "actual"])
        self.assertEqual(gu["siguiente"], 4)
        self.assertEqual(gu["pasos"][3]["accion"]["tipo"], "proponer")
        self.assertTrue(gu["pasos"][3]["correo"], "proponer fecha le escribe al cliente: tiene que avisarlo")


class GuardiaHuella(unittest.TestCase):
    """(4) La huella de productos/documentos NO cambia sola."""

    def _confirmado(self):
        app, db, ctx, esp = _nuevo("solicitud_recibida", responsable_user_id=7, responsable_nombre="Sam")
        c = app.test_client()
        c.post("/retiros/1/confirmar-docs")
        c.post("/retiros/1/confirmar-productos")
        self.assertEqual(_estados(_guia(app))[:3], ["hecho", "hecho", "hecho"])
        return app, db

    def test_refrescar_la_guia_muchas_veces_no_invalida_nada(self):
        app, db = self._confirmado()
        for _ in range(5):
            self.assertEqual(_estados(_guia(app))[:3], ["hecho", "hecho", "hecho"])

    def test_un_refresco_del_erp_que_toca_saldo_o_snapshot_no_invalida(self):
        import json
        app, db = self._confirmado()
        d = db.docs[0]
        d["con_saldo"] = 0                                    # el saldo se vuelve a verificar contra el ERP
        snap = json.loads(d["erp_snapshot"])
        snap["hdr"] = {"neto": 12345, "fecha": "2026-10-01"}  # cambia lo que NO son líneas
        d["erp_snapshot"] = json.dumps(snap)
        self.assertEqual(_estados(_guia(app))[:3], ["hecho", "hecho", "hecho"])

    def test_cambiar_una_cantidad_si_pide_confirmar_de_nuevo(self):
        import json
        app, db = self._confirmado()
        d = db.docs[0]
        snap = json.loads(d["erp_snapshot"])
        snap["lineas"][1]["cantidad"] = 3
        d["erp_snapshot"] = json.dumps(snap)
        gu = _guia(app)
        self.assertEqual(gu["pasos"][0]["estado"], "hecho")
        self.assertEqual(gu["pasos"][2]["estado"], "actual")
        self.assertIn("cambiaron", gu["pasos"][2]["faltan"][0])

    def test_agregar_un_documento_si_pide_confirmar_de_nuevo(self):
        app, db = self._confirmado()
        db.agregar_doc(1, "FCV", "0000011225")
        gu = _guia(app)
        self.assertEqual(gu["pasos"][0]["estado"], "actual")
        self.assertIn("cambió", gu["pasos"][0]["faltan"][0])

    def test_la_seleccion_granular_con_las_mismas_lineas_no_cambia_la_huella(self):
        """Guardar «Selección de líneas» sin cambiar nada (todas incluidas, misma cantidad) no invalida."""
        app, db = self._confirmado()
        d = db.docs[0]
        d["has_seleccion_lineas"] = 1
        for sku, desc, q in (("DISCO25", "Set Discos 2,5 - 5 kg", 1), ("MANC10", "Mancuerna hexagonal 10 kg", 2)):
            db.lineas_sel.append({"doc_id": d["id"], "sku": sku, "descripcion": desc, "cantidad_seleccionada": q,
                                  "peso_unit_kg": 0, "vol_unit_m3": 0, "peso_total_kg": 0, "vol_total_m3": 0,
                                  "peso_vol_unit_kg": 0, "peso_vol_total_kg": 0, "marcada_sin_saldo": 0,
                                  "motivo_sin_saldo": None, "sin_saldo_por": None, "sin_saldo_en": None})
        self.assertEqual(_estados(_guia(app))[:3], ["hecho", "hecho", "hecho"])


class GuardiaGuiaPura(unittest.TestCase):
    """Invariantes de retiros_guia.evaluar para TODOS los estados reales de PICKUP_STATUS."""

    def test_invariantes_en_toda_la_matriz_de_estados(self):
        import itertools
        import pickups_module as pm
        for st, n_docs, cita, prop, resp, prep, cambio, evid, exige in itertools.product(
                pm.PICKUP_STATUS, (0, 1), (False, True), (False, True), ("", "Ana"), (False, True), (False, True),
                (False, True), (True, False)):
            if cambio and st in ("en_preparacion", "retirada", "cerrada", "rechazada", "fallida"):
                continue    # _guia_datos nunca marca «cambio» en esos estados (combinación inalcanzable)
            if prep and st != "en_preparacion":
                continue    # _retiro_preparado solo puede ser True en preparación
            if not evid and st != "cerrada":
                continue    # la evidencia de retiro solo cambia algo en «cerrada»
            c = {"status": st, "n_docs": n_docs, "docs": [], "docs_firma": "a", "prod_n": 2, "prod_firma": "b",
                 "correo_ok": True, "cita": cita, "propuesta": prop, "responsable": resp, "preparado": prep,
                 "cambio_pedido": cambio, "evidencia_retiro": evid, "exige_responsable": exige,
                 "adelantado": bool(prop or cita or st in ("agenda_confirmada", "reagendada", "en_preparacion",
                                                           "retirada", "cerrada"))}
            gu = rg.evaluar(c)
            ctx_txt = f"{c}"
            self.assertEqual([p["n"] for p in gu["pasos"]], [1, 2, 3, 4, 5, 6], ctx_txt)
            for p in gu["pasos"]:
                self.assertIn(p["estado"], ("hecho", "actual", "espera", "pendiente", "bloqueado"), ctx_txt)
                if p["estado"] == "hecho":
                    self.assertEqual(p["faltan"], [], ctx_txt)
                    self.assertIsNone(p["accion"] if st not in ("rechazada", "fallida") else None, ctx_txt)
                if p["estado"] == "actual":
                    self.assertTrue(p["faltan"], f"paso {p['n']} en rojo sin decir qué falta · {ctx_txt}")
            if st in ("rechazada", "fallida") or (st == "cerrada" and not evid):
                self.assertIsNone(gu["siguiente"], ctx_txt)
                self.assertTrue(gu["terminal"], ctx_txt)
                self.assertNotEqual(gu["pasos"][5]["estado"], "hecho", ctx_txt)    # nunca «retiro completado» falso
                self.assertNotEqual(gu["pasos"][5]["resumen"], "Retiro completado", ctx_txt)
            else:
                self.assertIsNone(gu["terminal"], ctx_txt)
                pend = [p for p in gu["pasos"] if p["estado"] != "hecho"]
                retirado = st == "retirada" or (st == "cerrada" and evid)
                # Responsable obligatorio (Daniel 2026-10-02): se frena SOLO un retiro en curso, sin responsable y con la regla encendida
                self.assertEqual(gu["sin_responsable"], bool(exige and not resp and not retirado), ctx_txt)
                if gu["sin_responsable"]:
                    self.assertEqual(gu["siguiente"], 2, ctx_txt)
                    self.assertEqual(gu["pasos"][1]["estado"], "actual", ctx_txt)
                    for p in gu["pasos"]:
                        a = p.get("accion")
                        if a and a["tipo"] != "tomar":
                            self.assertTrue(a.get("deshabilitada"), f"paso {p['n']} sin bloquear · {ctx_txt}")
                else:
                    # «siguiente» = primer paso pendiente que NO sea secundario (el responsable de un retiro en curso es un
                    # aviso, no lo operativo); si solo quedan secundarios, el primero de ellos
                    principales = [p for p in pend if not p.get("secundario")]
                    primero = (principales or pend)[0]["n"] if pend else None
                    self.assertEqual(gu["siguiente"], primero, ctx_txt)
                    for p in gu["pasos"]:
                        if p.get("secundario"):      # solo con la regla apagada: el responsable de un retiro ya en curso es un aviso
                            self.assertEqual((p["n"], p["estado"], c["adelantado"], exige), (2, "pendiente", True, False), ctx_txt)
            # nunca aparece «hecho» un paso posterior a uno que no está listo, salvo 1/2/3 que son independientes
            if gu["pasos"][4]["estado"] == "hecho" and st not in ("retirada", "cerrada"):
                self.assertEqual(gu["pasos"][3]["estado"], "hecho", ctx_txt)


# ══════════════════════════════════════════════════════════════════════════════
#  REGRESIONES — cada docstring es el hallazgo de la revisión, ya corregido en el producto
# ══════════════════════════════════════════════════════════════════════════════
class RegresionGuiaContradiceLaFicha(unittest.TestCase):
    def test_contrapropuesta_del_cliente_sin_cita_confirmada_le_toca_a_ILUS(self):
        """HALLAZGO 1. El cliente respondió a la PRIMERA propuesta con otra fecha (pickup_public_tracking
        rama 'counter': status -> en_revision, proposed_date queda con la fecha vieja, queda una propuesta
        'pending' proposed_by='cliente'). La ficha dice «responder la contrapropuesta del cliente»
        (internal_detail.html:246) pero la guía solo mira la contrapropuesta cuando YA hay cita
        (_guia_datos: `if cita and st not in ...`), así que dice «Esperar la respuesta del cliente» en
        ÁMBAR: le dice al operador que espere algo que ya llegó."""
        app, db, ctx, esp = _nuevo("en_revision", proposed_date="2026-10-06", responsable_user_id=7,
                                   responsable_nombre="Sam")
        db.propuestas.append({"id": 1, "request_id": 1, "status": "pending", "proposed_by": "cliente"})
        gu = _guia(app)
        p4 = gu["pasos"][3]
        self.assertEqual(p4["estado"], "actual", f"quedó en {p4['estado']}: {p4['faltan']}")
        self.assertNotIn("Esperar la respuesta", " ".join(p4["faltan"]))
        self.assertIn("cliente", " ".join(p4["faltan"]).lower())
        self.assertEqual(p4["ancla"], "#paso-esperando")
        self.assertTrue(p4["correo"])

    def test_correo_en_extra_emails_cuenta_como_destino_de_la_propuesta(self):
        """HALLAZGO 2. `correo_ok` mira solo contact_email, pero el envío real usa contact_email +
        extra_emails (_get_pickup_all_emails). Con contact_email vacío/inválido y un extra válido la guía
        pone el paso 4 en «bloqueado» y el JS lo trata como bloqueo DURO (sin «Hacerlo igual»): se le quita
        al operador una acción que sí funciona (REGLA #4.2)."""
        app, db, ctx, esp = _nuevo("solicitud_recibida", contact_email="", extra_emails="compras@cliente.cl",
                                   responsable_user_id=7, responsable_nombre="Sam")
        c = app.test_client()
        c.post("/retiros/1/confirmar-docs")
        c.post("/retiros/1/confirmar-productos")
        p4 = _guia(app)["pasos"][3]
        self.assertNotEqual(p4["estado"], "bloqueado", p4["faltan"])
        self.assertFalse(p4["bloquea"])

    def test_cerrada_sin_haber_sido_retirada_no_dice_que_el_cliente_se_llevo_el_pedido(self):
        """HALLAZGO 3. 'cerrada' también se usa para cerrar duplicados/spam que NUNCA se retiraron
        (pickup_update_status:6405 lo dice). Antes la guía marcaba los 6 pasos «hecho» con «Retiro completado»
        y el JS decía «El cliente ya se llevó su pedido». Ahora: terminal honesto y sin preparación/entrega «hechas»."""
        app, db, ctx, esp = _nuevo("cerrada")   # sin cita, sin evento de retiro
        gu = _guia(app)
        self.assertEqual(gu["terminal"], "Este retiro se cerró sin que el cliente lo retirara.")
        self.assertIsNone(gu["siguiente"])
        self.assertNotEqual(gu["pasos"][5]["resumen"], "Retiro completado")
        self.assertNotEqual(gu["pasos"][5]["estado"], "hecho")
        self.assertNotEqual(gu["pasos"][4]["estado"], "hecho")
        self.assertNotEqual(gu["pasos"][4]["resumen"], "Pedido preparado")

    def test_cerrada_sin_retiro_ni_cita_no_dice_cita_confirmada(self):
        """BUG ABIERTO (revisión, residual del hallazgo 3) — retiros_guia.py:75: `cita = bool(c.get("cita")) or st in
        ("en_preparacion", "retirada", "cerrada")` fuerza cita=True también en una «cerrada» SIN evidencia de retiro.
        Un duplicado cerrado que nunca tuvo cita muestra el paso 4 «hecho · Cita confirmada»: dato falso, mismo
        problema que el paso 6. Arreglo: excluir «cerrada» sin evidencia de esa lista (o usar `not terminal`).
        Cuando se arregle, esta prueba dará «unexpected success»: quitar @expectedFailure."""
        app, db, ctx, esp = _nuevo("cerrada")   # sin cita, sin evento de retiro
        self.assertNotEqual(_guia(app)["pasos"][3]["resumen"], "Cita confirmada")

    def test_cerrada_con_evidencia_de_retiro_si_es_retiro_completado(self):
        app, db, ctx, esp = _nuevo("cerrada", confirmed_date="2026-10-03", responsable_nombre="Sam",
                                   retirado_por_nombre="Juan Pérez")
        gu = _guia(app)
        self.assertIsNone(gu["terminal"])
        self.assertEqual(_estados(gu), ["hecho"] * 6)

    def test_retiro_en_preparacion_sin_responsable_antes_era_un_aviso_ahora_se_frena(self):
        """HALLAZGO 4 (2026-10-02, antes de la regla del responsable). En preparación la ficha decía «Siguiente: entregar al
        cliente» y la guía mandaba a asignar responsable en pleno picking. Daniel decidió después que SIN responsable no se
        avanza («para avanzar debe declarar el responsable»): ahora es lo primero; con RETIROS_EXIGE_RESPONSABLE=0 vuelve a
        ser solo un aviso que no tapa el paso real."""
        app, db, ctx, esp = _nuevo("en_preparacion", confirmed_date="2026-10-03", proposed_date="2026-10-03",
                                   responsable_user_id=None, responsable_nombre=None)
        gu = _guia(app)
        self.assertEqual(gu["siguiente"], 2)
        self.assertTrue(gu["sin_responsable"])
        os.environ["RETIROS_EXIGE_RESPONSABLE"] = "0"
        try:
            gu = _guia(app)
        finally:
            os.environ.pop("RETIROS_EXIGE_RESPONSABLE", None)
        self.assertNotEqual(gu["siguiente"], 2, "con la regla apagada el cartel principal no manda a asignar responsable en pleno picking")
        self.assertFalse(gu["sin_responsable"])


class RegresionPreparacionCheck(unittest.TestCase):
    def test_sobrepickeo_en_una_linea_no_tapa_a_otra_linea_sin_empezar(self):
        """HALLAZGO 5. Las columnas se SUMAN entre líneas antes de decidir y recién ahí se compara con lo
        pedido: una línea con pickeado > solicitado (2 de 1) compensa a otra línea sin empezar
        (noAsignado=1) y da LISTO falso. Hay que decidir línea por línea y limitar cada línea a lo suyo."""
        r = rc.evaluar([_doc_c(_fila_f(solicitado=1, pickeado=2), _fila_f(solicitado=1, noAsignado=1))])
        self.assertFalse(r["listo"], r["frase"])
        self.assertFalse(r["listo_auto"])
        self.assertEqual(r["faltan"], 1)       # cada línea se limita a lo suyo: sobra de una no tapa lo que falta de otra

    def test_documento_cancelado_entero_no_da_frase_absurda(self):
        """HALLAZGO 6. Un documento 100% cancelado en Check nunca cuenta como «completo»: el retiro no
        queda listo jamás y la frase dice «faltan 0 de 1 unidades» (contradictoria)."""
        r = rc.evaluar([_doc_c(_fila_f(solicitado=1, pickeado=1)), _doc_c(_fila_f(solicitado=2, cancelado=2))])
        self.assertNotIn("faltan 0", r["frase"])
        # un documento 100% cancelado no bloquea ni da «listo» solo: el otro documento decide
        self.assertTrue(r["listo"], r["frase"])
        solo = rc.evaluar([_doc_c(_fila_f(solicitado=2, cancelado=2))])
        self.assertFalse(solo["listo"])
        self.assertIn("cancelado", solo["frase"])

    def test_filas_de_OTRO_documento_no_cuentan(self):
        """HALLAZGO 7. resumir_filas suma TODO lo que Check devuelva, sin verificar que tipoDocumento /
        numeroDocumento sean los del documento pedido. Si el parámetro numdoc de Check filtra «por
        contiene», o devuelve vecinos, filas de otro documento ya pickeado dan LISTO falso y se marca
        preparado un pedido que bodega no tocó (el cliente lo ve en su seguimiento)."""
        app, db, ctx, esp = _nuevo("en_preparacion", confirmed_date="2026-10-03", responsable_nombre="M")
        db.agregar_picking(1, "DISCO25", 1)
        db.reiniciar_registro()
        # pedimos BLV 23732; Check contesta con una fila de la BLV 123732 (otra), ya pickeada
        esp.check.respuestas["*"] = respuesta_check(
            fila_check(tipoDocumento="BLV", numeroDocumento="123732", solicitado=1, pickeado=1))
        d = app.test_client().get("/retiros/1/check-preparacion").get_json()
        self.assertFalse(d["evaluacion"]["listo"], d["evaluacion"]["frase"])
        self.assertEqual(d["evaluacion"]["estado"], "sin_datos")     # la fila ajena se descarta: Check «no lo tiene»
        self.assertFalse(d["aplicado_ahora"])
        self.assertEqual([e for e in db.escrituras if "picking_items" in e[0].lower()], [])

    def test_con_mas_de_12_documentos_no_da_listo_sin_mirar_todos(self):
        """HALLAZGO 8. _check_prep_retiro hace docs[:12]: del 13 en adelante no se consulta y el retiro
        puede quedar LISTO con documentos sin revisar."""
        app, db, ctx, esp = _nuevo("en_preparacion", docs=13, confirmed_date="2026-10-03", responsable_nombre="M")
        esp.check.respuestas["*"] = respuesta_check(fila_check(solicitado=1, pickeado=1))
        ev = app.test_client().get("/retiros/1/check-preparacion?solo_lectura=1").get_json()["evaluacion"]
        self.assertFalse(ev["listo"] and ev["documentos"] < 13,
                         f"listo con {ev['documentos']} de 13 documentos revisados")
        # con más de 12 documentos no se revisa solo: ni listo, ni listo_auto, ni se le pregunta a Check
        self.assertFalse(ev["listo"] or ev["listo_auto"])
        self.assertEqual(ev["documentos"], 13)
        self.assertEqual(esp.check.llamadas, [])

    def test_si_el_retiro_cambia_de_estado_mientras_se_consulta_check_no_se_marca_nada(self):
        """HALLAZGO 9. _check_aplicar_listo usa el `req` leído ANTES de hablar con Check (hasta 80 s por
        documento). Si en ese lapso el retiro dejó de estar en preparación (RETIRADO, o devuelto a
        agenda_confirmada) igual escribe picking_completo y avisa «LISTO para entrega»."""
        app, db, ctx, esp = _nuevo("en_preparacion", confirmed_date="2026-10-03", responsable_nombre="M")
        db.agregar_picking(1, "DISCO25", 1)
        db.reiniciar_registro()

        class RespuestasQueCambianElEstado(dict):
            def __contains__(self_inner, k):
                db.solicitudes[1]["status"] = "retirada"     # alguien lo marcó RETIRADO durante la espera
                return super().__contains__(k)

        esp.check.respuestas = RespuestasQueCambianElEstado({"*": respuesta_check(fila_check(solicitado=1, pickeado=1))})
        d = app.test_client().get("/retiros/1/check-preparacion").get_json()
        self.assertFalse(d["aplicado_ahora"])
        self.assertEqual(db.logs_de(1, "picking_completo"), [])


class RegresionBarridoSinContexto(unittest.TestCase):
    def test_el_barrido_de_fondo_corre_con_la_BD_real_que_exige_contexto_de_app(self):
        """HALLAZGO 10 (el más grave). `_check_barrido` corre en threading.Thread SIN `app.app_context()`.
        mysql_fetchall/mysql_execute de app.py usan get_db() -> `"_db" not in g`, que fuera de un contexto
        lanza «RuntimeError: Working outside of application context»; el `except` solo lo imprime. Resultado:
        el «preparado automático aunque nadie tenga abierta la ficha» NO funciona nunca en producción.
        (El arnés normal no lo ve porque su BD de mentira no usa `g`; esta la imita igual que get_db.)"""
        class BDConContexto(A.BDFalsa):
            @staticmethod
            def _exigir():
                "_db" in g   # exactamente lo que hace app.get_db()

            def fetchall(self, sql, params=()):
                self._exigir()
                return super().fetchall(sql, params)

            def fetchone(self, sql, params=()):
                self._exigir()
                return super().fetchone(sql, params)

            def execute(self, sql, params=()):
                self._exigir()
                return super().execute(sql, params)

        original = A.BDFalsa
        A.BDFalsa = BDConContexto
        try:
            app, db, ctx, esp = A.construir_app()
        finally:
            A.BDFalsa = original
        db.nueva_solicitud(1, status="en_preparacion", confirmed_date="2026-10-03", responsable_nombre="M")
        db.agregar_doc(1, "BLV", "0000023732")
        db.agregar_picking(1, "DISCO25", 1)
        esp.check.respuestas["*"] = respuesta_check(fila_check(solicitado=1, pickeado=1))
        db.reiniciar_registro()
        try:
            app.test_client().get("/retiros")   # entrar al Monitor dispara _check_barrido_si_toca()
        except Exception:
            pass                                 # la plantilla del Monitor no se renderiza en el arnés; da igual
        t0 = time.time()
        while time.time() - t0 < 4 and not db.logs_de(1, "picking_completo"):
            time.sleep(0.1)
        A.esperar_hilos_de_aviso()
        self.assertTrue(esp.check.llamadas, "el barrido nunca llegó a consultar Check")
        self.assertEqual(len(db.logs_de(1, "picking_completo")), 1)


class RegresionRegistroDeConfirmacion(unittest.TestCase):
    def test_el_registro_de_la_confirmacion_dice_cuantos_documentos_son_de_verdad(self):
        """HALLAZGO 11. `n = len(p1['detalle'])` y `detalle` se recorta a 8 documentos (12 productos): con 10
        facturas el registro queda como «8 documentos: …». Es el rastro de auditoría de quién confirmó qué
        (la OT/retiro es evidencia): no puede contar de menos."""
        app, db, ctx, esp = _nuevo("solicitud_recibida", docs=10)
        app.test_client().post("/retiros/1/confirmar-docs")
        nota = db.logs_de(1, "docs_confirmadas")[0]["notes"]
        self.assertIn("10 documentos", nota)


class PuraCheckReglasQueYaFuncionan(unittest.TestCase):
    """Casos límite que HOY se comportan bien (guardias del contrato de retiros_check)."""

    def test_canceladas_devueltos_y_varias_lineas(self):
        self.assertTrue(rc.evaluar([_doc_c(_fila_f(solicitado=3, cancelado=1, pickeado=2))])["listo"])
        self.assertFalse(rc.evaluar([_doc_c(_fila_f(solicitado=2, cancelado=2))])["listo"])
        self.assertFalse(rc.evaluar([_doc_c(_fila_f(solicitado=2, pickeado=1), _fila_f(solicitado=2, pickeado=1))])["listo"])

    def test_acumulativas_nunca_dan_falso_listo_con_asignado_alto(self):
        self.assertFalse(rc.evaluar([_doc_c(_fila_f(solicitado=4, asignado=4, pickeado=2, revisado=2))])["listo"])
        self.assertFalse(rc.evaluar([_doc_c(_fila_f(solicitado=2, asignado=1, pickeado=1))])["listo"])

    def test_un_documento_que_check_no_devuelve_impide_el_listo(self):
        completo = _doc_c(_fila_f(solicitado=1, pickeado=1))
        self.assertFalse(rc.evaluar([completo, None])["listo"])

    def test_numeros_ilegibles_cuentan_cero(self):
        self.assertFalse(rc.evaluar([_doc_c({"solicitado": "dos", "pickeado": "dos"})])["listo"])


if __name__ == "__main__":
    unittest.main()
