# -*- coding: utf-8 -*-
"""Backend de la Guía de 6 pasos de Retiros y de la preparación detectada por Check.

Registra las rutas REALES (pickups_module.register_pickup_routes) sobre un Flask de mentira, con una
BD en memoria (tests/_arnes_retiros.py + tests/_arnes_retiros_guia.py). No toca MySQL, Check, el ERP
ni manda correos: todo se espía para poder afirmar «esto NO se llamó».

Hay UN CLIENTE REAL en producción (RET-VQJ58N). Por eso estas pruebas vigilan, en cada ruta nueva:
  · nada le escribe al cliente (ni correo ni WhatsApp) y nadie cambia el estado del retiro;
  · Check es SOLO LECTURA (REGLA #4.4): solo GET a GetSeguimientoDespacho;
  · el ERP Random no se toca (REGLA #4.1).

Los antiguos `xfail` (bugs del borrador demostrados con un caso que fallaba) ya se corrigieron en el
producto: ahora son pruebas normales que verifican el arreglo.

El marcado automático de «preparado» exige DOS lecturas «listo» separadas por RETIROS_CHECK_CONFIRMACION_S
segundos (25 por defecto): el fixture `env` lo pone en 0 para que las pruebas normales apliquen a la primera;
las pruebas de la doble lectura lo suben explícitamente.

Correr con:  python -m pytest tests/test_retiros_guia_backend.py -q
"""
import json
import os
import re
import sys
import threading
import time
from types import SimpleNamespace
from unittest import mock

import pytest

AQUI = os.path.dirname(os.path.abspath(__file__))
RAIZ = os.path.dirname(AQUI)
for _p in (AQUI, RAIZ):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import flask  # noqa: E402
import _arnes_retiros as A  # noqa: E402
import shutil  # noqa: E402
from _arnes_retiros_guia import CLIENTE_EMAIL, DIRS_TEMPORALES, EQUIPO_EMAIL, construir  # noqa: E402

RID = 1
DOC_A = "0000023732"        # BLV real de la muestra de Check: DISCO25 x1 + MANC10 x2 = 3 unidades
DOC_B = "0000023733"
TODO_PICKEADO = A.respuesta_check(A.fila_check(solicitado=1, pickeado=1), A.fila_check(solicitado=2, pickeado=2))
SIN_FILAS = {"estado": {"codigo": 200}, "body": {"response": []}}


# ══════════════════════════════════════════════════════════════════════════════
#  Utilidades
# ══════════════════════════════════════════════════════════════════════════════
class EspiaCheckConTiempo(A.EspiaCheck):
    """El espía del borrador no guardaba el timeout: aquí sí (para medir cuánto puede esperar la ficha)."""

    def __init__(self):
        super().__init__()
        self.timeouts = []
        self.reloj = 0.0        # segundos simulados que la ficha lleva esperando a Check

    def __call__(self, path, params, timeout=60):
        self.timeouts.append(timeout)
        resp = super().__call__(path, params, timeout)
        if resp is None:        # Check «colgado»: la llamada agota su timeout completo
            self.reloj += timeout
        return resp


@pytest.fixture(scope="module", autouse=True)
def _limpiar_carpetas_temporales():
    yield
    for d in DIRS_TEMPORALES:
        shutil.rmtree(d, ignore_errors=True)
    DIRS_TEMPORALES.clear()


@pytest.fixture()
def env(monkeypatch):
    monkeypatch.setenv("RETIROS_CHECK_CONFIRMACION_S", "0")      # sin espera entre las dos lecturas «listo»
    monkeypatch.delenv("RETIROS_CHECK_AUTO", raising=False)      # marcado automático encendido (por defecto)
    app, db, ctx, esp = construir()
    db.admins = [{"id": 11, "username": "admin1@sphs.cl"}, {"id": 12, "username": "admin2@sphs.cl"}]
    esp.check = EspiaCheckConTiempo()
    ctx["_checkwms_get"] = esp.check
    return SimpleNamespace(app=app, db=db, ctx=ctx, esp=esp, cli=app.test_client())


def retiro(env, rid=RID, status="solicitud_recibida", docs=(DOC_A,), **kw):
    # Por defecto el retiro YA tiene responsable (Daniel 2026-10-02: sin responsable no se avanza; esa regla se prueba aparte,
    # pasando responsable_user_id=None y responsable_nombre=None).
    kw.setdefault("responsable_user_id", 7)
    kw.setdefault("responsable_nombre", "Samantha Blacio")
    env.db.nueva_solicitud(rid, status=status, **kw)
    for n, numero in enumerate(docs):
        snap = None if n == 0 else {"lineas": [{"sku": f"EXTRA{n}", "descripcion_erp": f"Banca {n}", "cantidad": 1}]}
        env.db.agregar_doc(rid, numero=numero, snapshot=snap)
    env.db.reiniciar_registro()
    return env.db.solicitudes[rid]


def retiro_en_preparacion(env, rid=RID, docs=(DOC_A,), items=(("DISCO25", 1), ("MANC10", 2)), **kw):
    kw.setdefault("confirmed_date", "2026-10-05")
    kw.setdefault("responsable_user_id", 7)
    kw.setdefault("responsable_nombre", "Samantha Blacio")
    retiro(env, rid, status="en_preparacion", docs=docs, **kw)
    env.db.agregar_log(rid, "estado_actualizado", new_status="en_preparacion")
    for sku, cant in items:
        env.db.agregar_picking(rid, sku, cant)
    env.db.reiniciar_registro()


def guia(env, rid=RID):
    r = env.cli.get(f"/retiros/{rid}/guia")
    assert r.status_code == 200, r.get_data(as_text=True)
    return r.get_json()


def paso(g, n):
    return g["pasos"][n - 1]


def destinos_de_correo(env):
    A.esperar_hilos_de_aviso()
    return [(c.args[0] if c.args else c.kwargs.get("to")) for c in env.esp.correo.call_args_list]


def assert_nada_al_cliente(env, permitidos=()):
    """Ni WhatsApp, ni correo al cliente, ni toque al ERP. (El equipo interno sí puede recibir avisos.)"""
    A.esperar_hilos_de_aviso()
    assert env.esp.whatsapp.call_count == 0, "no debe salir ningún WhatsApp"
    for dest in destinos_de_correo(env):
        assert (dest or "").lower() != CLIENTE_EMAIL.lower(), "¡un correo salió hacia el cliente real!"
        assert (dest or "").lower() in {p.lower() for p in permitidos}, f"correo inesperado a {dest}"
    assert env.esp.erp.mock_calls == [], "el ERP Random es solo lectura y estas rutas no deben tocarlo"


def assert_sin_escrituras(env):
    assert env.db.escrituras == [], f"no debía escribir nada y escribió: {env.db.escrituras}"


# ══════════════════════════════════════════════════════════════════════════════
#  Premisas del arnés (si esto falla, las demás pruebas no son fiables)
# ══════════════════════════════════════════════════════════════════════════════
def test_premisa_las_rutas_reales_quedaron_registradas(env):
    reglas = {r.rule for r in env.app.url_map.iter_rules()}
    for ruta in ("/retiros/<int:rid>/guia", "/retiros/<int:rid>/confirmar-docs",
                 "/retiros/<int:rid>/confirmar-productos", "/retiros/<int:rid>/check-preparacion",
                 "/retiros/<int:rid>/tomar"):
        assert ruta in reglas


def test_premisa_flask_g_fuera_de_contexto_revienta():
    """En producción mysql_fetchall/mysql_execute van por get_db() → flask.g. La BD falsa imita eso:
    un hilo sin app.app_context() falla igual que en Cloud Run (Python 3.12 no hereda el contexto)."""
    with pytest.raises(RuntimeError):
        "_db" in flask.g  # noqa: B015


def test_premisa_la_bd_falsa_exige_contexto_de_app(env):
    with pytest.raises(RuntimeError):
        env.db.fetchall("SELECT 1")
    with env.app.app_context():
        env.db.fetchall("SELECT id FROM pickup_requests_inexistente")   # con contexto no revienta


# ══════════════════════════════════════════════════════════════════════════════
#  GET /retiros/<rid>/guia
# ══════════════════════════════════════════════════════════════════════════════
class TestGuiaGET:
    def test_retiro_inexistente_es_404(self, env):
        r = env.cli.get("/retiros/99/guia")
        assert r.status_code == 404 and r.get_json()["ok"] is False

    def test_retiro_nuevo_sin_documentos(self, env):
        retiro(env, docs=())
        r = env.cli.get(f"/retiros/{RID}/guia")
        g = r.get_json()
        assert r.status_code == 200 and g["ok"] is True
        assert r.headers["Cache-Control"] == "no-store"
        assert [p["n"] for p in g["pasos"]] == [1, 2, 3, 4, 5, 6]
        assert g["siguiente"] == 1 and g["terminal"] is None
        assert paso(g, 1)["estado"] == "actual" and "factura o boleta" in paso(g, 1)["faltan"][0]
        assert paso(g, 3)["estado"] == "bloqueado"
        assert paso(g, 4)["estado"] == "bloqueado"
        assert g["rid"] == RID and g["codigo"] == f"RET-T{RID:05d}"
        assert_sin_escrituras(env)
        assert_nada_al_cliente(env)

    def test_con_documento_sin_confirmar_muestra_que_se_va_a_confirmar(self, env):
        retiro(env)
        g = guia(env)
        p1, p3 = paso(g, 1), paso(g, 3)
        assert p1["estado"] == "actual" and p1["accion"]["tipo"] == "confirmar_docs"
        # Documento · cliente · RUT · saldo: COMPLETO, es lo que la persona confirma (REGLA #15)
        assert p1["detalle"] == ["BLV 0000023732 · Gerd Müller · RUT 11.111.111-1 · con saldo"]
        assert p1["total"] == 1
        assert p3["estado"] == "pendiente" and p3["accion"]["deshabilitada"] is True
        assert p3["detalle"] == ["1 × Set Discos 2,5 - 5 kg (SKU DISCO25)", "2 × Mancuerna hexagonal 10 kg (SKU MANC10)"]
        assert p3["total"] == 2
        assert paso(g, 2)["estado"] == "hecho" and "A cargo de Samantha Blacio" in paso(g, 2)["resumen"]
        assert g["siguiente"] == 1 and g["sin_responsable"] is False
        # el navegador recibe las huellas de lo que está viendo (para que «Confirmo» confirme EXACTAMENTE eso)
        assert re.fullmatch(r"[0-9a-f]{12}", g["firma_docs"]) and re.fullmatch(r"[0-9a-f]{12}", g["firma_prod"])
        assert g["status"] == "solicitud_recibida"
        assert_sin_escrituras(env)

    def test_documento_sin_rut_ni_saldo_verificado_lo_dice(self, env):
        retiro(env)
        env.db.docs[0]["cliente_rut"] = ""
        env.db.docs[0]["con_saldo"] = None
        assert paso(guia(env), 1)["detalle"] == ["BLV 0000023732 · Gerd Müller · saldo sin verificar"]

    def test_detalle_completo_sin_recortes_con_muchos_documentos_y_productos(self, env):
        retiro(env, docs=tuple(f"00000{i:05d}" for i in range(10)))
        env.db.docs[0]["erp_snapshot"] = json.dumps({"lineas": [
            {"sku": f"SKU{i:02d}", "descripcion_erp": f"Producto {i}", "cantidad": 1} for i in range(15)]})
        g = guia(env)
        assert len(paso(g, 1)["detalle"]) == 10 and paso(g, 1)["total"] == 10
        assert len(paso(g, 3)["detalle"]) >= 15 and paso(g, 3)["total"] >= 15

    def test_la_guia_no_incluye_datos_de_otro_retiro(self, env):
        """Dos retiros con documentos, clientes, RUT y productos distintos: cada guía solo trae lo suyo, y lo
        confirmado en uno no da por confirmado el otro."""
        retiro(env, rid=1, docs=(DOC_A,))
        retiro(env, rid=2, docs=("0000077777",))
        env.db.docs[1]["cliente_nombre"] = "Otra Empresa SPA"
        env.db.docs[1]["cliente_rut"] = "99.888.777-6"
        env.db.docs[1]["erp_snapshot"] = json.dumps({"lineas": [
            {"sku": "OTROSKU", "descripcion_erp": "Producto Ajeno 77", "cantidad": 9}]})
        env.db.solicitudes[2]["responsable_nombre"] = "Persona Ajena"
        env.db.reiniciar_registro()
        g1 = json.dumps(guia(env, 1), ensure_ascii=False)
        for ajeno in ("0000077777", "Otra Empresa", "99.888.777-6", "OTROSKU", "Producto Ajeno", "Persona Ajena", "RET-T00002"):
            assert ajeno not in g1, f"la guía del retiro 1 trae «{ajeno}», que es del retiro 2"
        assert "0000023732" in g1 and "Gerd Müller" in g1 and "RET-T00001" in g1
        g2 = json.dumps(guia(env, 2), ensure_ascii=False)
        for ajeno in ("0000023732", "Gerd Müller", "DISCO25", "MANC10"):
            assert ajeno not in g2, f"la guía del retiro 2 trae «{ajeno}», que es del retiro 1"
        # confirmar el 2 no da por confirmado el 1
        env.cli.post("/retiros/2/confirmar-docs")
        assert paso(guia(env, 1), 1)["estado"] == "actual" and paso(guia(env, 2), 1)["estado"] == "hecho"
        # y la preparación de uno no consulta los documentos del otro
        env.db.solicitudes[1]["status"] = "en_preparacion"
        env.esp.check.respuestas["*"] = None
        env.cli.get("/retiros/1/check-preparacion?solo_lectura=1")
        assert {p["numdoc"].lstrip("0") for _, p in env.esp.check.llamadas} == {"23732"}

    def test_cada_paso_dice_que_falta_antes_de_el(self, env):
        retiro(env, responsable_user_id=None, responsable_nombre=None)
        previos = guia(env)["previos"]
        assert previos["1"] == []
        assert [n for n, _ in previos["3"]] == [1, 2]
        assert [n for n, _ in previos["4"]] == [1, 2, 3]
        assert [n for n, _ in previos["6"]] == [1, 2, 3, 4, 5]
        assert all(textos for _, textos in previos["6"])

    def test_retiro_avanzado_da_por_confirmados_facturas_y_productos(self, env):
        retiro(env, status="agenda_confirmada", confirmed_date="2026-10-05",
               responsable_user_id=7, responsable_nombre="Samantha Blacio")
        g = guia(env)
        assert [paso(g, n)["estado"] for n in (1, 2, 3, 4)] == ["hecho"] * 4
        p5 = paso(g, 5)
        assert p5["estado"] == "actual" and p5["accion"]["tipo"] == "preparacion" and p5["correo"] is True
        assert g["siguiente"] == 5
        assert_sin_escrituras(env)

    def test_propuesta_enviada_espera_al_cliente(self, env):
        retiro(env, status="propuesta_enviada", proposed_date="2026-10-06")
        p4 = paso(guia(env), 4)
        assert p4["estado"] == "espera" and "Esperar la respuesta del cliente" in p4["faltan"][0]

    def test_cliente_pide_cambiar_la_cita_bloquea_la_preparacion(self, env):
        retiro(env, status="agenda_confirmada", confirmed_date="2026-10-05")
        env.db.propuestas.append({"id": 5, "request_id": RID, "status": "pending", "proposed_by": "cliente"})
        g = guia(env)
        assert paso(g, 4)["estado"] == "actual" and "otra fecha" in paso(g, 4)["faltan"][0]
        assert paso(g, 4)["ancla"] == "#paso-esperando"
        assert paso(g, 5)["estado"] == "bloqueado"

    def test_contrapropuesta_del_cliente_antes_de_haber_cita_le_toca_a_ILUS(self, env):
        """Cliente respondió a la PRIMERA propuesta con otra fecha: ya no dice «espera» (ámbar): le toca a ILUS."""
        retiro(env, status="en_revision", proposed_date="2026-10-06", responsable_user_id=7,
               responsable_nombre="Sam")
        env.db.propuestas.append({"id": 5, "request_id": RID, "status": "pending", "proposed_by": "cliente"})
        p4 = paso(guia(env), 4)
        assert p4["estado"] == "actual" and p4["ancla"] == "#paso-esperando"
        assert "Esperar la respuesta" not in " ".join(p4["faltan"]) and p4["correo"] is True

    def test_correo_invalido_ya_no_bloquea_solo_avisa(self, env):
        """El envío real usa contacto + extras (+ SMS/WhatsApp): un correo malo es un aviso blando, no un bloqueo."""
        retiro(env, contact_email="no-es-un-correo", responsable_user_id=7, responsable_nombre="Sam")
        env.cli.post(f"/retiros/{RID}/confirmar-docs")
        env.cli.post(f"/retiros/{RID}/confirmar-productos")
        p4 = paso(guia(env), 4)
        assert p4["estado"] == "actual" and p4["bloquea"] is False
        assert any("correo válido" in a for a in p4["avisos"])

    def test_un_correo_extra_valido_cuenta_como_destino(self, env):
        retiro(env, contact_email="", extra_emails="compras@cliente.cl", responsable_user_id=7,
               responsable_nombre="Sam")
        env.cli.post(f"/retiros/{RID}/confirmar-docs")
        env.cli.post(f"/retiros/{RID}/confirmar-productos")
        p4 = paso(guia(env), 4)
        assert p4["estado"] == "actual" and not any("correo válido" in a for a in p4["avisos"])

    def test_en_preparacion_muestra_el_avance_de_bodega(self, env):
        retiro_en_preparacion(env)
        env.db.picking[0]["picked"] = 1
        g = guia(env)
        assert paso(g, 5)["estado"] == "espera" and "Van 1 de 2" in paso(g, 5)["faltan"][0]
        assert paso(g, 6)["estado"] == "pendiente"
        assert g["siguiente"] == 5

    def test_en_preparacion_con_pedido_listo_habilita_la_entrega(self, env):
        """Preparado = evento picking_completo posterior al paso a preparación Y la lista de bodega completa."""
        retiro_en_preparacion(env)
        for it in env.db.picking:
            it["picked"] = 1
        env.db.agregar_log(RID, "picking_completo", new_status="en_preparacion")
        g = guia(env)
        assert paso(g, 5)["estado"] == "hecho"
        assert paso(g, 6)["estado"] == "actual" and paso(g, 6)["accion"]["tipo"] == "retirar"

    def test_lista_de_picking_completa_sin_evento_ya_no_cuenta_como_preparado(self, env):
        """Cambio intencional: «preparado» ya no sale solo de los ítems marcados; hace falta el evento
        picking_completo (que el marcado manual y Check dejan siempre)."""
        retiro_en_preparacion(env)
        for it in env.db.picking:
            it["picked"] = 1
        assert paso(guia(env), 5)["estado"] == "espera"

    def test_evento_de_picking_completo_pero_lista_incompleta_no_cuenta(self, env):
        """Si bodega desmarca un ítem después de terminar, deja de estar «listo»."""
        retiro_en_preparacion(env)
        env.db.agregar_log(RID, "picking_completo", new_status="en_preparacion")
        env.db.picking[0]["picked"] = 1          # el otro ítem sigue sin marcar
        g = guia(env)
        assert paso(g, 5)["estado"] == "espera" and paso(g, 6)["estado"] == "pendiente"

    def test_retiro_en_curso_sin_responsable_se_frena_y_pide_hacerse_cargo_primero(self, env):
        """Daniel 2026-10-02: «para avanzar debe declarar el responsable, y para agendar o liberar el calendario»."""
        retiro(env, status="agenda_confirmada", confirmed_date="2026-10-05", responsable_user_id=None, responsable_nombre=None)
        g = guia(env)
        p2 = paso(g, 2)
        assert p2["estado"] == "actual" and p2["secundario"] is False and p2["accion"]["tipo"] == "tomar"
        assert g["siguiente"] == 2 and g["sin_responsable"] is True
        p5 = paso(g, 5)
        assert p5["accion"]["tipo"] == "preparacion" and p5["accion"]["deshabilitada"] is True
        assert "Sin responsable no se avanza" in p5["accion"]["motivo"]

    def test_con_la_regla_apagada_vuelve_a_ser_solo_un_aviso(self, env, monkeypatch):
        monkeypatch.setenv("RETIROS_EXIGE_RESPONSABLE", "0")
        retiro(env, status="agenda_confirmada", confirmed_date="2026-10-05", responsable_user_id=None, responsable_nombre=None)
        g = guia(env)
        p2 = paso(g, 2)
        assert p2["estado"] == "pendiente" and p2["secundario"] is True and p2["accion"]["tipo"] == "tomar"
        assert g["siguiente"] == 5 and g["sin_responsable"] is False          # lo operativo, no «asignar responsable»

    def test_retiro_ya_en_curso_sin_marca_confirmo_lo_dice_con_honestidad(self, env):
        retiro(env, status="agenda_confirmada", confirmed_date="2026-10-05")
        g = guia(env)
        assert "sin marca" in paso(g, 1)["resumen"] and "sin marca" in paso(g, 3)["resumen"]

    @pytest.mark.parametrize("estado,frase", [("rechazada", "rechazó"), ("fallida", "no se concretó")])
    def test_retiro_terminal_negativo(self, env, estado, frase):
        retiro(env, status=estado)
        g = guia(env)
        assert frase in g["terminal"] and g["siguiente"] is None
        assert all(p["estado"] != "actual" for p in g["pasos"])

    @pytest.mark.parametrize("estado", ["retirada", "cerrada"])
    def test_retiro_completado(self, env, estado):
        retiro(env, status=estado, confirmed_date="2026-10-05", responsable_user_id=7, responsable_nombre="Sam")
        env.db.agregar_log(RID, "estado_actualizado", new_status="retirada")     # evidencia de que sí se retiró
        g = guia(env)
        assert g["terminal"] is None and g["siguiente"] is None
        assert [p["estado"] for p in g["pasos"]] == ["hecho"] * 6

    def test_cerrada_con_quien_retiro_tambien_es_retiro_completado(self, env):
        retiro(env, status="cerrada", confirmed_date="2026-10-05", responsable_user_id=7,
               responsable_nombre="Sam", retirado_por_nombre="Juan Pérez")
        g = guia(env)
        assert g["terminal"] is None and [p["estado"] for p in g["pasos"]] == ["hecho"] * 6

    def test_cerrada_sin_evidencia_no_dice_que_el_cliente_se_llevo_el_pedido(self, env):
        """'cerrada' también cierra duplicados/spam que nunca se retiraron: terminal honesto, sin 6 pasos «hecho»."""
        retiro(env, status="cerrada", confirmed_date="2026-10-05", responsable_user_id=7, responsable_nombre="Sam")
        g = guia(env)
        assert g["terminal"] == "Este retiro se cerró sin que el cliente lo retirara."
        assert g["siguiente"] is None
        assert paso(g, 5)["estado"] != "hecho" and paso(g, 6)["estado"] != "hecho"
        assert paso(g, 6)["resumen"] != "Retiro completado" and paso(g, 5)["resumen"] != "Pedido preparado"
        assert all(p["estado"] != "actual" for p in g["pasos"])

    def test_si_la_bd_falla_responde_500_amigable_sin_detalles_internos(self, env):
        retiro(env)
        env.db.falla_si.append((r"from pickup_request_docs", RuntimeError("Table 'ilus_prod.secreto_interno' doesn't exist")))
        r = env.cli.get(f"/retiros/{RID}/guia")
        assert r.status_code == 500
        cuerpo = r.get_data(as_text=True)
        assert "secreto_interno" not in cuerpo and "ilus_prod" not in cuerpo
        assert r.get_json()["error"] == "No se pudo armar la guía."

    def test_columna_nueva_ausente_cae_a_la_consulta_vieja(self, env):
        """Entornos sin con_saldo/motivo_otro_rut: la guía igual se arma (fallback del SELECT)."""
        retiro(env)
        for d in env.db.docs:
            d.pop("motivo_otro_rut")
        g = guia(env)
        assert paso(g, 1)["estado"] == "actual"

    def test_aviso_de_documento_sin_saldo_y_de_otro_rut(self, env):
        retiro(env)
        env.db.docs[0]["con_saldo"] = 0
        env.db.docs[0]["motivo_otro_rut"] = "Lo retira la empresa"
        avisos = paso(guia(env), 1)["avisos"]
        assert any("sin saldo" in a for a in avisos) and any("otro RUT" in a for a in avisos)


# ══════════════════════════════════════════════════════════════════════════════
#  POST confirmar-docs / confirmar-productos
# ══════════════════════════════════════════════════════════════════════════════
class TestConfirmar:
    def test_retiro_inexistente_es_404(self, env):
        for ruta in ("confirmar-docs", "confirmar-productos"):
            assert env.cli.post(f"/retiros/99/{ruta}").status_code == 404

    def test_confirmar_docs_solo_inserta_una_fila_en_pickup_logs(self, env):
        retiro(env)
        r = env.cli.post(f"/retiros/{RID}/confirmar-docs")
        d = r.get_json()
        assert r.status_code == 200 and d["ok"] is True
        assert paso(d, 1)["estado"] == "hecho" and "por Samantha Blacio" in paso(d, 1)["resumen"]
        # UNA sola escritura en toda la BD: el evento (nada de UPDATE al retiro ni a otras tablas)
        assert len(env.db.escrituras) == 1 and len(env.db.inserts_en_logs()) == 1
        ev = env.db.eventos(RID, "docs_confirmadas")
        assert len(ev) == 1
        assert re.match(r"^firma=[0-9a-f]{12} · 1 documento: BLV 0000023732", ev[0]["notes"])
        assert (ev[0]["actor_type"], ev[0]["actor_name"]) == ("interno", "Samantha Blacio")
        # no cambia el estado del retiro
        assert ev[0]["old_status"] == ev[0]["new_status"] == "solicitud_recibida"
        assert env.db.solicitudes[RID]["status"] == "solicitud_recibida"
        assert_nada_al_cliente(env)
        assert env.esp.mant_notif.call_count == 0

    def test_confirmar_productos_solo_inserta_una_fila_en_pickup_logs(self, env):
        retiro(env)
        env.cli.post(f"/retiros/{RID}/confirmar-docs")
        env.db.reiniciar_registro()
        r = env.cli.post(f"/retiros/{RID}/confirmar-productos")
        d = r.get_json()
        assert r.status_code == 200 and d["ok"] is True
        assert paso(d, 3)["estado"] == "hecho" and "2 productos confirmados" in paso(d, 3)["resumen"]
        assert len(env.db.escrituras) == 1 and len(env.db.inserts_en_logs()) == 1
        ev = env.db.eventos(RID, "productos_confirmados")
        assert len(ev) == 1 and re.match(
            r"^firma=[0-9a-f]{12} · 2 productos: 1 × Set Discos 2,5 - 5 kg \(SKU DISCO25\); 2 × Mancuerna hexagonal 10 kg",
            ev[0]["notes"])
        assert env.db.solicitudes[RID]["status"] == "solicitud_recibida"
        assert_nada_al_cliente(env)

    def test_productos_sin_confirmar_las_facturas_primero_es_409_y_no_escribe(self, env):
        retiro(env)
        r = env.cli.post(f"/retiros/{RID}/confirmar-productos")
        assert r.status_code == 409 and "confirma las facturas" in r.get_json()["error"]
        assert_sin_escrituras(env)

    def test_sin_documentos_ninguna_confirmacion_pasa(self, env):
        retiro(env, docs=())
        for ruta in ("confirmar-docs", "confirmar-productos"):
            r = env.cli.post(f"/retiros/{RID}/{ruta}")
            assert r.status_code == 409 and "agrega la factura" in r.get_json()["error"]
        assert_sin_escrituras(env)

    def test_confirmar_dos_veces_es_idempotente(self, env):
        retiro(env)
        env.cli.post(f"/retiros/{RID}/confirmar-docs")
        env.db.reiniciar_registro()
        r = env.cli.post(f"/retiros/{RID}/confirmar-docs")
        assert r.status_code == 200 and r.get_json()["ya"] is True
        assert_sin_escrituras(env)

    @pytest.mark.parametrize("estado", ["rechazada", "cerrada", "retirada", "fallida"])
    def test_retiro_terminado_no_se_puede_confirmar(self, env, estado):
        retiro(env, status=estado)
        for ruta in ("confirmar-docs", "confirmar-productos"):
            r = env.cli.post(f"/retiros/{RID}/{ruta}")
            assert r.status_code == 409 and "ya terminó" in r.get_json()["error"]
        assert_sin_escrituras(env)

    def test_retiro_ya_avanzado_no_pide_ni_escribe_confirmaciones(self, env):
        retiro(env, status="agenda_confirmada", confirmed_date="2026-10-05")
        for ruta in ("confirmar-docs", "confirmar-productos"):
            r = env.cli.post(f"/retiros/{RID}/{ruta}")
            assert r.status_code == 200 and r.get_json()["ya"] is True
        assert_sin_escrituras(env)
        assert_nada_al_cliente(env)

    def test_sin_usuario_identificado_es_400_y_no_escribe(self):
        app, db, ctx, esp = construir(usuario={"id": 7})
        e = SimpleNamespace(app=app, db=db, esp=esp, cli=app.test_client())
        retiro(e)
        r = e.cli.post(f"/retiros/{RID}/confirmar-docs")
        assert r.status_code == 400 and "identificar tu usuario" in r.get_json()["error"]
        assert_sin_escrituras(e)

    def test_la_huella_guardada_la_calcula_el_servidor_no_el_navegador(self, env):
        """El navegador manda la huella de lo que VIO; si coincide se confirma, y lo guardado es la del servidor."""
        retiro(env)
        vista = guia(env)["firma_docs"]
        r = env.cli.post(f"/retiros/{RID}/confirmar-docs", json={"firma": vista})
        assert r.status_code == 200
        ev = env.db.eventos(RID, "docs_confirmadas")[0]
        assert f"firma={vista} " in ev["notes"]
        assert paso(guia(env), 1)["estado"] == "hecho"      # la huella guardada es la vigente

    @pytest.mark.parametrize("ruta,campo", [("confirmar-docs", "firma_docs"), ("confirmar-productos", "firma_prod")])
    def test_firma_desactualizada_es_409_y_no_escribe(self, env, ruta, campo):
        """Lo confirmado es EXACTAMENTE lo que la persona vio: si la lista cambió, se pide revisar de nuevo."""
        retiro(env)
        if ruta == "confirmar-productos":
            env.cli.post(f"/retiros/{RID}/confirmar-docs")
        env.db.reiniciar_registro()
        r = env.cli.post(f"/retiros/{RID}/{ruta}", json={"firma": "aaaaaaaaaaaa"})
        assert r.status_code == 409 and "La lista cambió" in r.get_json()["error"]
        assert_sin_escrituras(env)
        assert_nada_al_cliente(env)

    def test_la_lista_cambia_mientras_la_miras_y_el_confirmo_viejo_no_vale(self, env):
        retiro(env)
        vista = guia(env)["firma_docs"]                                    # la persona miró 1 documento…
        env.db.agregar_doc(RID, numero=DOC_B)                              # …y entró otro mientras tanto
        env.db.reiniciar_registro()
        r = env.cli.post(f"/retiros/{RID}/confirmar-docs", json={"firma": vista})
        assert r.status_code == 409
        assert_sin_escrituras(env)
        assert paso(guia(env), 1)["estado"] == "actual"                    # sigue sin confirmar

    def test_confirmar_con_la_firma_vigente_funciona_para_docs_y_productos(self, env):
        retiro(env)
        g = guia(env)
        assert env.cli.post(f"/retiros/{RID}/confirmar-docs", json={"firma": g["firma_docs"]}).status_code == 200
        assert env.cli.post(f"/retiros/{RID}/confirmar-productos", json={"firma": g["firma_prod"]}).status_code == 200

    def test_texto_de_la_marca_no_se_pasa_de_900_caracteres(self, env):
        retiro(env, docs=tuple(f"00000{i:05d}" for i in range(8)))
        for d in env.db.docs:
            d["cliente_nombre"] = "N" * 200
        env.cli.post(f"/retiros/{RID}/confirmar-docs")
        assert len(env.db.eventos(RID, "docs_confirmadas")[0]["notes"]) <= 900

    # ── la huella invalida la confirmación ────────────────────────────────
    def test_si_se_agrega_un_documento_hay_que_confirmar_de_nuevo(self, env):
        retiro(env)
        env.cli.post(f"/retiros/{RID}/confirmar-docs")
        env.cli.post(f"/retiros/{RID}/confirmar-productos")
        g = guia(env)
        assert paso(g, 1)["estado"] == "hecho" and paso(g, 3)["estado"] == "hecho"

        env.db.agregar_doc(RID, numero=DOC_B, snapshot={"lineas": [
            {"sku": "BANCA1", "descripcion_erp": "Banca plana", "cantidad": 1}]})
        g = guia(env)
        assert paso(g, 1)["estado"] == "actual" and "cambió" in paso(g, 1)["faltan"][0]
        assert paso(g, 3)["estado"] == "pendiente" and paso(g, 3)["accion"]["deshabilitada"] is True

        env.cli.post(f"/retiros/{RID}/confirmar-docs")
        g = guia(env)
        assert paso(g, 1)["estado"] == "hecho"
        assert paso(g, 3)["estado"] == "actual" and "cambiaron" in paso(g, 3)["faltan"][0]

        env.cli.post(f"/retiros/{RID}/confirmar-productos")
        assert paso(guia(env), 3)["estado"] == "hecho"
        assert_nada_al_cliente(env)

    def test_si_se_quita_un_documento_tambien_se_invalida(self, env):
        retiro(env, docs=(DOC_A, DOC_B))
        env.cli.post(f"/retiros/{RID}/confirmar-docs")
        env.db.docs.pop()
        assert paso(guia(env), 1)["estado"] == "actual"

    def test_si_cambia_una_cantidad_solo_se_invalidan_los_productos(self, env):
        retiro(env)
        env.cli.post(f"/retiros/{RID}/confirmar-docs")
        env.cli.post(f"/retiros/{RID}/confirmar-productos")
        snap = json.loads(env.db.docs[0]["erp_snapshot"])
        snap["lineas"][1]["cantidad"] = 5
        env.db.docs[0]["erp_snapshot"] = json.dumps(snap)
        g = guia(env)
        assert paso(g, 1)["estado"] == "hecho"
        assert paso(g, 3)["estado"] == "actual" and "cambiaron" in paso(g, 3)["faltan"][0]

    def test_la_ultima_confirmacion_manda(self, env):
        """Dos confirmaciones del mismo tipo: vale la más reciente (la guía lee ORDER BY id DESC)."""
        retiro(env)
        env.cli.post(f"/retiros/{RID}/confirmar-docs")
        env.db.agregar_doc(RID, numero=DOC_B)
        env.cli.post(f"/retiros/{RID}/confirmar-docs")
        assert paso(guia(env), 1)["estado"] == "hecho"
        assert len(env.db.eventos(RID, "docs_confirmadas")) == 2

    def test_flujo_completo_de_los_tres_primeros_pasos_sin_un_solo_mensaje(self, env):
        retiro(env, responsable_user_id=None, responsable_nombre=None)
        for ruta in ("tomar", "confirmar-docs", "confirmar-productos"):       # primero el responsable: sin él no se avanza
            r = env.cli.post(f"/retiros/{RID}/{ruta}")
            assert r.status_code == 200 and r.get_json()["ok"] is True, ruta
        g = guia(env)
        assert [paso(g, n)["estado"] for n in (1, 2, 3)] == ["hecho"] * 3
        p4 = paso(g, 4)
        assert p4["estado"] == "actual" and p4["accion"]["tipo"] == "proponer"
        assert g["siguiente"] == 4
        # lo único escrito: 3 eventos del equipo + el UPDATE del responsable; el estado no se movió
        assert {e["action"] for e in env.db.logs} == {"docs_confirmadas", "productos_confirmados", "responsable_asignado"}
        assert env.db.solicitudes[RID]["status"] == "solicitud_recibida"
        assert_nada_al_cliente(env)
        assert env.esp.mant_notif.call_count == 0

    def test_la_bitacora_registra_la_cantidad_real_de_productos(self, env):
        """La constancia cuenta los productos reales (15), no la lista recortada de la pantalla."""
        retiro(env)
        env.db.docs[0]["erp_snapshot"] = json.dumps({"lineas": [
            {"sku": f"SKU{i:02d}", "descripcion_erp": f"Producto {i}", "cantidad": 1} for i in range(15)]})
        env.cli.post(f"/retiros/{RID}/confirmar-docs")
        env.cli.post(f"/retiros/{RID}/confirmar-productos")
        assert re.match(r"^firma=[0-9a-f]{12} · 15 productos: ", env.db.eventos(RID, "productos_confirmados")[0]["notes"])

    def test_la_bitacora_registra_la_cantidad_real_de_documentos(self, env):
        retiro(env, docs=tuple(f"00000{i:05d}" for i in range(10)))
        env.cli.post(f"/retiros/{RID}/confirmar-docs")
        assert re.match(r"^firma=[0-9a-f]{12} · 10 documentos: ", env.db.eventos(RID, "docs_confirmadas")[0]["notes"])

    def test_si_el_insert_falla_no_responde_ok(self, env):
        """log_event se traga la excepción del INSERT; la ruta relee y, si la marca no quedó, responde 500."""
        retiro(env)
        env.db.falla_escritura_si.append((r"insert into `pickup_logs`", RuntimeError("deadlock")))
        r = env.cli.post(f"/retiros/{RID}/confirmar-docs")
        d = r.get_json()
        assert r.status_code == 500 and d["ok"] is False and "No se pudo guardar" in d["error"]
        assert "deadlock" not in r.get_data(as_text=True)
        assert env.db.eventos(RID, "docs_confirmadas") == []
        assert paso(guia(env), 1)["estado"] == "actual"
        assert_nada_al_cliente(env)


# ══════════════════════════════════════════════════════════════════════════════
#  POST /retiros/<rid>/tomar  (paso 2: «Me hago cargo»)
# ══════════════════════════════════════════════════════════════════════════════
class TestTomar:
    def test_toma_el_retiro_con_el_nombre_de_la_sesion_y_no_manda_nada(self, env):
        retiro(env, responsable_user_id=None, responsable_nombre=None)
        r = env.cli.post(f"/retiros/{RID}/tomar", json={"nombre": "Otra Persona", "uid": 99})
        assert r.status_code == 200 and r.get_json() == {"ok": True, "nombre": "Samantha Blacio"}
        fila = env.db.solicitudes[RID]
        assert (fila["responsable_user_id"], fila["responsable_nombre"]) == (7, "Samantha Blacio")   # no del navegador
        assert [e["action"] for e in env.db.logs] == ["responsable_asignado"]
        assert env.db.solicitudes[RID]["status"] == "solicitud_recibida"
        assert_nada_al_cliente(env)

    def test_tomarlo_dos_veces_es_idempotente(self, env):
        retiro(env, responsable_user_id=None, responsable_nombre=None)
        env.cli.post(f"/retiros/{RID}/tomar")
        env.db.reiniciar_registro()
        r = env.cli.post(f"/retiros/{RID}/tomar")
        assert r.status_code == 200 and r.get_json()["ok"] is True
        assert_sin_escrituras(env)

    def test_si_ya_lo_tiene_otra_persona_es_409(self, env):
        retiro(env, responsable_user_id=3, responsable_nombre="Fran")
        r = env.cli.post(f"/retiros/{RID}/tomar")
        assert r.status_code == 409 and "Fran" in r.get_json()["error"]
        assert_sin_escrituras(env)

    @pytest.mark.parametrize("estado", ["rechazada", "cerrada", "retirada", "fallida"])
    def test_retiro_terminado_no_necesita_responsable(self, env, estado):
        retiro(env, status=estado)
        assert env.cli.post(f"/retiros/{RID}/tomar").status_code == 409
        assert_sin_escrituras(env)

    def test_sin_usuario_es_400(self):
        app, db, ctx, esp = construir(usuario={"nombre": "Sin id"})
        e = SimpleNamespace(app=app, db=db, esp=esp, cli=app.test_client())
        retiro(e)
        assert e.cli.post(f"/retiros/{RID}/tomar").status_code == 400
        assert_sin_escrituras(e)


# ══════════════════════════════════════════════════════════════════════════════
#  RESPONSABLE OBLIGATORIO (Daniel 2026-10-02): «para avanzar debe declarar el responsable, y para agendar o liberar el calendario»
# ══════════════════════════════════════════════════════════════════════════════
class TestResponsableObligatorio:
    MSG = "Sin responsable no se avanza ni se agenda o libera el calendario"

    def _sin_resp(self, env, status="solicitud_recibida", **kw):
        return retiro(env, status=status, responsable_user_id=None, responsable_nombre=None, **kw)

    @pytest.mark.parametrize("ruta", ["confirmar-docs", "confirmar-productos"])
    def test_confirmar_pide_antes_el_responsable(self, env, ruta):
        self._sin_resp(env)
        r = env.cli.post(f"/retiros/{RID}/{ruta}")
        assert r.status_code == 409 and self.MSG in r.get_json()["error"] and r.get_json()["code"] == "SIN_RESPONSABLE"
        assert_sin_escrituras(env)
        assert_nada_al_cliente(env)

    def test_hacerse_cargo_destraba_todo(self, env):
        self._sin_resp(env)
        assert env.cli.post(f"/retiros/{RID}/confirmar-docs").status_code == 409
        assert env.cli.post(f"/retiros/{RID}/tomar").status_code == 200
        assert env.cli.post(f"/retiros/{RID}/confirmar-docs").status_code == 200
        assert env.cli.post(f"/retiros/{RID}/confirmar-productos").status_code == 200

    def test_proponer_fecha_ocupa_el_calendario_y_exige_responsable(self, env):
        self._sin_resp(env, docs=(DOC_A,))
        r = env.cli.post(f"/retiros/{RID}/proposal", json={"date": "2026-10-08", "time_from": "10:00", "time_to": "10:30"})
        assert r.status_code == 409 and self.MSG in r.get_json()["error"]
        assert_sin_escrituras(env)
        assert_nada_al_cliente(env)

    def test_aceptar_la_fecha_del_cliente_exige_responsable(self, env):
        self._sin_resp(env, status="en_revision", proposed_date="2026-10-08")
        env.db.propuestas.append({"id": 5, "request_id": RID, "status": "pending", "proposed_by": "cliente",
                                  "date": "2026-10-09", "time_from": "10:00", "time_to": "10:30"})
        r = env.cli.post(f"/retiros/{RID}/aceptar-contrapropuesta")
        assert r.status_code == 409 and r.get_json()["code"] == "SIN_RESPONSABLE"
        assert env.db.solicitudes[RID]["status"] == "en_revision"
        assert_sin_escrituras(env)

    def test_marcar_la_cita_como_aceptada_exige_responsable(self, env):
        self._sin_resp(env, status="propuesta_enviada", proposed_date="2026-10-08")
        r = env.cli.post(f"/retiros/{RID}/marcar-aceptada-manual", json={"motivo": "llamó y confirmó"})
        assert r.status_code == 409 and r.get_json()["code"] == "SIN_RESPONSABLE"
        assert_sin_escrituras(env)

    @pytest.mark.parametrize("nuevo", ["agenda_confirmada", "en_preparacion", "retirada", "reagendada", "rechazada", "cerrada",
                                       "fallida", "en_revision"])
    def test_cambiar_de_estado_un_retiro_sin_responsable_no_hace_nada(self, env, nuevo):
        self._sin_resp(env, status="propuesta_enviada", confirmed_date="2026-10-08")
        r = env.cli.post(f"/retiros/{RID}/status", data={"status": nuevo})
        assert r.status_code == 302
        assert env.db.solicitudes[RID]["status"] == "propuesta_enviada"
        assert_sin_escrituras(env)
        assert_nada_al_cliente(env)

    @pytest.mark.parametrize("cabeceras", [{"X-Requested-With": "XMLHttpRequest"}, {"Sec-Fetch-Dest": "empty"},
                                           {"Accept": "application/json"}])
    def test_el_kanban_y_el_calendario_reciben_un_409_y_no_un_redirect(self, env, cabeceras):
        """Ambos hacen fetch con redirect:'manual' y dan por bueno cualquier redirect: con un 302 mostrarían «✓ movido» sin que nada cambiara."""
        self._sin_resp(env, status="propuesta_enviada", confirmed_date="2026-10-08")
        r = env.cli.post(f"/retiros/{RID}/status", data={"status": "agenda_confirmada"}, headers=cabeceras)
        assert r.status_code == 409 and r.get_json()["code"] == "SIN_RESPONSABLE" and self.MSG in r.get_json()["error"]
        assert env.db.solicitudes[RID]["status"] == "propuesta_enviada"
        assert_sin_escrituras(env)
        assert_nada_al_cliente(env)

    def test_el_mensaje_llega_a_la_pantalla(self, env):
        self._sin_resp(env, status="agenda_confirmada", confirmed_date="2026-10-08")
        r = env.cli.post(f"/retiros/{RID}/status", data={"status": "en_preparacion"})
        with env.cli.session_transaction() as sesion:
            avisos = [m for _, m in sesion.get("_flashes", [])]
        assert r.status_code == 302 and any(self.MSG in m for m in avisos), avisos

    def test_con_responsable_el_cambio_de_estado_sigue_funcionando(self, env):
        retiro(env, status="agenda_confirmada", confirmed_date="2026-10-08")
        r = env.cli.post(f"/retiros/{RID}/status", data={"status": "en_preparacion"})
        assert r.status_code == 302 and env.db.solicitudes[RID]["status"] == "en_preparacion"

    def test_el_mismo_estado_no_pide_responsable(self, env):
        """Guardar el estado que ya tiene no avanza nada (no es «avanzar»)."""
        self._sin_resp(env, status="agenda_confirmada", confirmed_date="2026-10-08")
        r = env.cli.post(f"/retiros/{RID}/status", data={"status": "agenda_confirmada", "notes": "nota"})
        assert r.status_code == 302 and env.db.solicitudes[RID]["status"] == "agenda_confirmada"

    @pytest.mark.parametrize("estado", ["rechazada", "cerrada", "retirada", "fallida"])
    def test_un_retiro_terminado_se_puede_reabrir_sin_responsable(self, env, estado):
        """«Tomar» rechaza los retiros terminados (409), así que exigirles responsable para reabrirlos sería un callejón sin salida."""
        self._sin_resp(env, status=estado)
        r = env.cli.post(f"/retiros/{RID}/status", data={"status": "en_revision"})
        assert r.status_code == 302 and env.db.solicitudes[RID]["status"] == "en_revision"

    def test_un_retiro_anterior_a_la_regla_no_se_frena_por_no_tener_responsable(self, env):
        """Retiro real RET-VQJ58N (2026-10-05): «esto avanzó antes de que fuera una restricción, por eso no avanzó». Los creados antes del 03-oct
        pueden seguir hasta el final; la guía solo les avisa que falta el responsable."""
        import datetime as dt
        self._sin_resp(env, status="agenda_confirmada", confirmed_date="2026-10-05", created_at=dt.datetime(2026, 9, 30, 15, 0))
        g = guia(env)
        assert g["sin_responsable"] is False and g["siguiente"] == 5
        assert paso(g, 2)["estado"] == "pendiente" and paso(g, 2)["secundario"] is True       # aviso, no bloqueo
        assert not paso(g, 5)["accion"].get("deshabilitada")
        r = env.cli.post(f"/retiros/{RID}/status", data={"status": "retirada", "retirado_por": "Gerd Müller"})
        assert r.status_code == 302 and env.db.solicitudes[RID]["status"] == "retirada"

    def test_un_retiro_creado_despues_de_la_regla_si_se_frena(self, env):
        import datetime as dt
        self._sin_resp(env, status="agenda_confirmada", confirmed_date="2026-10-05", created_at=dt.datetime(2026, 10, 4, 15, 0))
        assert guia(env)["sin_responsable"] is True
        r = env.cli.post(f"/retiros/{RID}/status", data={"status": "retirada"})
        assert r.status_code == 302 and env.db.solicitudes[RID]["status"] == "agenda_confirmada"

    def test_la_fecha_de_corte_se_puede_mover_por_entorno(self, env, monkeypatch):
        import datetime as dt
        monkeypatch.setenv("RETIROS_EXIGE_RESPONSABLE_DESDE", "2026-10-10")
        self._sin_resp(env, status="agenda_confirmada", confirmed_date="2026-10-05", created_at=dt.datetime(2026, 10, 4, 15, 0))
        assert guia(env)["sin_responsable"] is False

    def test_la_regla_se_puede_apagar_por_entorno(self, env, monkeypatch):
        monkeypatch.setenv("RETIROS_EXIGE_RESPONSABLE", "0")
        self._sin_resp(env)
        assert env.cli.post(f"/retiros/{RID}/confirmar-docs").status_code == 200

    def test_la_guia_dice_que_hay_que_hacerse_cargo_y_bloquea_el_resto(self, env):
        self._sin_resp(env)
        env.db.reiniciar_registro()
        g = guia(env)
        assert g["sin_responsable"] is True and g["siguiente"] == 2
        assert paso(g, 2)["estado"] == "actual" and paso(g, 2)["accion"]["tipo"] == "tomar"
        assert paso(g, 1)["accion"]["deshabilitada"] is True and self.MSG in paso(g, 1)["accion"]["motivo"]
        assert_sin_escrituras(env)


# ══════════════════════════════════════════════════════════════════════════════
#  GET /retiros/<rid>/check-preparacion
# ══════════════════════════════════════════════════════════════════════════════
class TestCheckPreparacion:
    def test_retiro_inexistente_es_404(self, env):
        assert env.cli.get("/retiros/99/check-preparacion").status_code == 404

    def test_solo_lectura_nunca_escribe_aunque_check_diga_listo(self, env):
        retiro_en_preparacion(env)
        env.esp.check.respuestas["23732"] = TODO_PICKEADO
        r = env.cli.get(f"/retiros/{RID}/check-preparacion?solo_lectura=1")
        d = r.get_json()
        assert r.status_code == 200 and d["ok"] is True
        assert d["evaluacion"]["listo"] is True            # lo detecta…
        assert d["aplicado_ahora"] is False and d["preparado"] is False    # …pero no lo aplica
        assert_sin_escrituras(env)
        assert not any(it["picked"] for it in env.db.picking)
        assert_nada_al_cliente(env)

    def test_en_preparacion_con_todo_pickeado_marca_preparado(self, env):
        retiro_en_preparacion(env)
        env.esp.check.respuestas["23732"] = TODO_PICKEADO
        r = env.cli.get(f"/retiros/{RID}/check-preparacion")
        d = r.get_json()
        assert d["ok"] and d["aplicado_ahora"] is True and d["preparado"] is True
        assert d["status"] == "en_preparacion"
        # exactamente 2 escrituras: ítems de la lista de bodega + el evento
        assert len(env.db.escrituras) == 2
        assert env.db.escrituras[0][0].startswith("UPDATE pickup_picking_items SET picked=1")
        assert len(env.db.inserts_en_logs()) == 1
        ev = env.db.eventos(RID, "picking_completo")
        assert len(ev) == 1
        assert (ev[0]["actor_type"], ev[0]["actor_name"]) == ("sistema", "Check WMS")
        assert ev[0]["old_status"] == ev[0]["new_status"] == "en_preparacion"
        assert "3 unidades" in ev[0]["notes"]
        assert all(it["picked"] and it["picked_by"] == "Check WMS" for it in env.db.picking)
        # NO cambia el estado del retiro
        assert env.db.solicitudes[RID]["status"] == "en_preparacion"
        # La guía y la línea de tiempo coinciden
        g = guia(env)
        assert paso(g, 5)["estado"] == "hecho" and paso(g, 6)["estado"] == "actual"

    def test_el_aviso_va_solo_al_equipo_interno_nunca_al_cliente(self, env):
        retiro_en_preparacion(env)
        env.esp.check.respuestas["23732"] = TODO_PICKEADO
        env.cli.get(f"/retiros/{RID}/check-preparacion")
        A.esperar_hilos_de_aviso()
        assert destinos_de_correo(env) == [EQUIPO_EMAIL]
        assert all(c.kwargs.get("modulo") == "comunicacion_interna" for c in env.esp.correo.call_args_list)
        assert env.esp.mant_notif.call_count == 2                       # campana: los 2 admins
        assert {c.kwargs["destino_user_id"] for c in env.esp.mant_notif.call_args_list} == {11, 12}
        assert all(c.kwargs["tipo"] == "retiro_listo" for c in env.esp.mant_notif.call_args_list)
        assert_nada_al_cliente(env, permitidos=(EQUIPO_EMAIL,))

    def test_es_idempotente_se_marca_una_sola_vez(self, env):
        retiro_en_preparacion(env)
        env.esp.check.respuestas["23732"] = TODO_PICKEADO
        env.cli.get(f"/retiros/{RID}/check-preparacion")
        A.esperar_hilos_de_aviso()
        avisos = env.esp.correo.call_count, env.esp.mant_notif.call_count
        env.db.reiniciar_registro()
        for _ in range(3):
            d = env.cli.get(f"/retiros/{RID}/check-preparacion").get_json()
            assert d["aplicado_ahora"] is False and d["preparado"] is True
        assert_sin_escrituras(env)
        A.esperar_hilos_de_aviso()
        assert (env.esp.correo.call_count, env.esp.mant_notif.call_count) == avisos
        assert len(env.db.eventos(RID, "picking_completo")) == 1

    def test_es_idempotente_aunque_la_cache_de_check_ya_venciera(self, env):
        """La idempotencia viene de la BD (evento + lista), no de que la respuesta de Check esté cacheada."""
        retiro_en_preparacion(env)
        env.esp.check.respuestas["23732"] = TODO_PICKEADO
        env.cli.get(f"/retiros/{RID}/check-preparacion")
        env.db.reiniciar_registro()
        llamadas = len(env.esp.check.llamadas)
        real = time.time
        with mock.patch("time.time", lambda: real() + 3600):         # caché (TTL 45 s) vencida
            d = env.cli.get(f"/retiros/{RID}/check-preparacion").get_json()
        assert len(env.esp.check.llamadas) > llamadas                 # sí volvió a preguntarle a Check
        assert d["aplicado_ahora"] is False and d["preparado"] is True
        assert_sin_escrituras(env)

    def test_varias_pestanas_a_la_vez_marcan_una_sola_vez(self, env):
        retiro_en_preparacion(env)
        env.esp.check.respuestas["23732"] = TODO_PICKEADO
        n = 6
        barrera = threading.Barrier(n)
        resultados = []

        def pestana():
            cli = env.app.test_client()
            barrera.wait(timeout=10)
            resultados.append(cli.get(f"/retiros/{RID}/check-preparacion").get_json())

        hilos = [threading.Thread(target=pestana) for _ in range(n)]
        [h.start() for h in hilos]
        [h.join(20) for h in hilos]
        assert len(resultados) == n
        assert sum(1 for d in resultados if d["aplicado_ahora"]) == 1
        assert len(env.db.eventos(RID, "picking_completo")) == 1
        A.esperar_hilos_de_aviso()
        assert destinos_de_correo(env) == [EQUIPO_EMAIL]              # un solo aviso interno

    @pytest.mark.parametrize("estado", ["solicitud_recibida", "propuesta_enviada", "agenda_confirmada",
                                        "reagendada", "retirada", "cerrada", "rechazada"])
    def test_fuera_de_preparacion_nunca_se_aplica(self, env, estado):
        # Cita lejana a propósito: con la cita cercana, «agenda_confirmada» + picking en Check sí pasa sola a «En preparación»
        # (envío automático, Daniel 2026-10-02; ver tests/test_retiros_prep_auto.py). Aquí se fija que Check NUNCA marca «preparado».
        retiro(env, status=estado, confirmed_date="2099-01-01")
        env.esp.check.respuestas["23732"] = TODO_PICKEADO
        d = env.cli.get(f"/retiros/{RID}/check-preparacion").get_json()      # sin solo_lectura
        assert d["ok"] and d["evaluacion"]["listo"] is True
        assert d["aplicado_ahora"] is False and d["preparado"] is False
        assert_sin_escrituras(env)
        assert_nada_al_cliente(env)

    def test_check_caido_es_sin_conexion_y_no_rompe(self, env):
        retiro_en_preparacion(env)
        env.esp.check.respuestas["*"] = None
        r = env.cli.get(f"/retiros/{RID}/check-preparacion")
        d = r.get_json()
        assert r.status_code == 200 and d["ok"] is True
        assert d["evaluacion"]["estado"] == "sin_conexion" and d["evaluacion"]["listo"] is False
        assert d["sin_respuesta"] == 1 and d["aplicado_ahora"] is False and d["preparado"] is False
        assert "No pudimos consultar Check" in d["evaluacion"]["frase"]
        assert_sin_escrituras(env)

    def test_check_que_lanza_excepcion_se_trata_como_sin_conexion_sin_detalles(self, env):
        """_check_doc_resumen atrapa la excepción de la consulta: Check «sin conexión», nunca detalles internos."""
        retiro_en_preparacion(env)
        env.esp.check.excepcion = RuntimeError("token=SECRETO-CHECK")
        r = env.cli.get(f"/retiros/{RID}/check-preparacion")
        d = r.get_json()
        assert r.status_code == 200 and d["ok"] is True
        assert d["evaluacion"]["estado"] == "sin_conexion" and d["aplicado_ahora"] is False
        assert "SECRETO" not in r.get_data(as_text=True)
        assert_sin_escrituras(env)

    def test_si_la_bd_falla_al_leer_los_documentos_da_500_amigable_sin_detalles(self, env):
        retiro_en_preparacion(env)
        env.db.falla_si.append((r"from pickup_request_docs", RuntimeError("tabla_secreta caída")))
        r = env.cli.get(f"/retiros/{RID}/check-preparacion")
        assert r.status_code == 500 and r.get_json() == {"ok": False, "error": "No se pudo consultar Check."}
        assert "tabla_secreta" not in r.get_data(as_text=True)
        assert_sin_escrituras(env)

    def test_pedido_a_medias_no_se_marca(self, env):
        retiro_en_preparacion(env)
        env.esp.check.respuestas["23732"] = A.respuesta_check(
            A.fila_check(solicitado=1, pickeado=1), A.fila_check(solicitado=2, pickeado=1, asignado=1))
        d = env.cli.get(f"/retiros/{RID}/check-preparacion").get_json()
        ev = d["evaluacion"]
        assert ev["estado"] == "en_proceso" and ev["listo"] is False
        assert (ev["pedidas"], ev["faltan"]) == (3, 1)
        assert d["aplicado_ahora"] is False
        assert_sin_escrituras(env)

    def test_check_sin_el_documento_no_se_marca(self, env):
        retiro_en_preparacion(env)
        env.esp.check.respuestas["*"] = SIN_FILAS
        d = env.cli.get(f"/retiros/{RID}/check-preparacion").get_json()
        assert d["evaluacion"]["estado"] == "sin_datos" and d["sin_respuesta"] == 0
        assert d["aplicado_ahora"] is False
        assert_sin_escrituras(env)

    def test_con_dos_documentos_hace_falta_que_esten_los_dos(self, env):
        retiro_en_preparacion(env, docs=(DOC_A, DOC_B), items=(("DISCO25", 1), ("MANC10", 2), ("EXTRA1", 1)))
        env.esp.check.respuestas["23732"] = TODO_PICKEADO
        env.esp.check.respuestas["23733"] = A.respuesta_check(A.fila_check(numeroDocumento="23733", solicitado=1, asignado=1))
        d = env.cli.get(f"/retiros/{RID}/check-preparacion").get_json()
        assert [x["estado"] for x in d["documentos"]] == ["listo", "en_proceso"]
        assert d["evaluacion"]["listo"] is False and d["aplicado_ahora"] is False
        assert_sin_escrituras(env)

        env.esp.check.respuestas["23733"] = A.respuesta_check(A.fila_check(numeroDocumento="23733", solicitado=1, pickeado=1))
        env.db.reiniciar_registro()
        real = time.time
        with mock.patch("time.time", lambda: real() + 3600):       # caché vencida
            d = env.cli.get(f"/retiros/{RID}/check-preparacion").get_json()
        assert d["evaluacion"]["listo"] is True and d["aplicado_ahora"] is True

    def test_prueba_la_variante_sin_ceros_y_luego_la_de_10_digitos(self, env):
        retiro_en_preparacion(env)
        env.esp.check.respuestas["23732"] = SIN_FILAS             # sin ceros: Check no lo tiene así
        env.esp.check.respuestas["0000023732"] = TODO_PICKEADO    # a 10 dígitos sí
        d = env.cli.get(f"/retiros/{RID}/check-preparacion?solo_lectura=1").get_json()
        assert [p["numdoc"] for _, p in env.esp.check.llamadas] == ["23732", "0000023732"]
        assert d["evaluacion"]["listo"] is True

    def test_si_la_primera_variante_responde_no_gasta_otra_consulta(self, env):
        retiro_en_preparacion(env)
        env.esp.check.respuestas["23732"] = TODO_PICKEADO
        env.cli.get(f"/retiros/{RID}/check-preparacion?solo_lectura=1")
        assert len(env.esp.check.llamadas) == 1

    def test_cache_de_45_segundos_evita_repetir_la_consulta(self, env):
        retiro_en_preparacion(env)
        env.esp.check.respuestas["23732"] = TODO_PICKEADO
        env.cli.get(f"/retiros/{RID}/check-preparacion?solo_lectura=1")
        env.cli.get(f"/retiros/{RID}/check-preparacion?solo_lectura=1")
        assert len(env.esp.check.llamadas) == 1

    def test_ya_preparado_no_vuelve_a_aplicar_ni_a_avisar(self, env):
        retiro_en_preparacion(env)
        for it in env.db.picking:
            it["picked"] = 1
        env.db.agregar_log(RID, "picking_completo", new_status="en_preparacion")
        env.db.reiniciar_registro()
        env.esp.check.respuestas["23732"] = TODO_PICKEADO
        d = env.cli.get(f"/retiros/{RID}/check-preparacion").get_json()
        assert d["aplicado_ahora"] is False and d["preparado"] is True
        assert_sin_escrituras(env)
        assert_nada_al_cliente(env)

    def test_check_solo_se_consulta_con_get_a_seguimiento_despacho(self, env):
        retiro_en_preparacion(env, docs=(DOC_A, DOC_B), items=(("A", 1),))
        env.esp.check.respuestas["*"] = None
        env.cli.get(f"/retiros/{RID}/check-preparacion")
        env.cli.get(f"/retiros/{RID}/check-preparacion?solo_lectura=1")
        assert env.esp.check.llamadas, "debía consultar a Check"
        assert env.esp.check.rutas == {"/api/ext/GetSeguimientoDespacho"}
        for _, params in env.esp.check.llamadas:
            assert set(params) == {"tipodoc", "numdoc", "diasrevisa"}
            assert params["tipodoc"] == "BLV" and params["diasrevisa"] == "90"
        permitidas = set(re.findall(r'"(/api/ext/[A-Za-z0-9_]+)"', _bloque_whitelist_check()))
        assert env.esp.check.rutas <= permitidas

    def test_con_dos_documentos_si_check_no_tiene_uno_no_se_marca(self, env):
        retiro_en_preparacion(env, docs=(DOC_A, DOC_B), items=(("DISCO25", 1), ("MANC10", 2), ("EXTRA1", 1)))
        env.esp.check.respuestas["23732"] = TODO_PICKEADO
        env.esp.check.respuestas["*"] = SIN_FILAS                  # el 2.º documento Check aún no lo tiene
        d = env.cli.get(f"/retiros/{RID}/check-preparacion").get_json()
        assert [x["estado"] for x in d["documentos"]] == ["listo", "sin_datos"]
        assert d["evaluacion"]["listo"] is False and d["aplicado_ahora"] is False
        assert "solo tiene algunos" in d["evaluacion"]["frase"]
        assert_sin_escrituras(env)

    def test_con_dos_documentos_si_check_responde_solo_por_uno_no_se_marca(self, env):
        retiro_en_preparacion(env, docs=(DOC_A, DOC_B), items=(("DISCO25", 1), ("MANC10", 2), ("EXTRA1", 1)))
        env.esp.check.respuestas["23732"] = TODO_PICKEADO          # el otro: Check no contesta (None)
        d = env.cli.get(f"/retiros/{RID}/check-preparacion").get_json()
        assert d["sin_respuesta"] == 1 and d["evaluacion"]["listo"] is False and d["aplicado_ahora"] is False
        assert_sin_escrituras(env)

    def test_retiro_sin_documentos_no_consulta_a_check(self, env):
        retiro_en_preparacion(env, docs=())
        d = env.cli.get(f"/retiros/{RID}/check-preparacion").get_json()
        assert d["evaluacion"]["estado"] == "sin_documento" and d["aplicado_ahora"] is False
        assert env.esp.check.llamadas == []
        assert_sin_escrituras(env)

    def test_documento_con_numero_ilegible_no_consulta_a_check(self, env):
        retiro_en_preparacion(env, docs=("S/N",))
        d = env.cli.get(f"/retiros/{RID}/check-preparacion").get_json()
        assert d["evaluacion"]["estado"] == "sin_datos" and d["aplicado_ahora"] is False
        assert env.esp.check.llamadas == []

    def test_con_check_colgado_la_ficha_no_espera_mas_de_un_minuto(self, env):
        """Timeout corto por consulta (10 s), UNA sola variante si la 1.ª no responde y un presupuesto de 20 s por petición."""
        retiro_en_preparacion(env, docs=(DOC_A, DOC_B, "0000023734", "0000023735", "0000023736"), items=(("A", 1),))
        env.esp.check.respuestas["*"] = None            # Check no responde: cada llamada agota su timeout
        real = time.time
        with mock.patch("time.time", lambda: real() + env.esp.check.reloj):   # reloj simulado
            r = env.cli.get(f"/retiros/{RID}/check-preparacion")
        assert r.status_code == 200
        assert max(env.esp.check.timeouts) <= 12, env.esp.check.timeouts
        assert env.esp.check.reloj <= 40, (
            f"la ficha espera {env.esp.check.reloj:.0f} s en {len(env.esp.check.timeouts)} llamadas a Check")
        # no probó la 2.ª variante del número tras un «no respondió»: a lo más una llamada por documento
        numeros = [p["numdoc"] for _, p in env.esp.check.llamadas]
        assert len(numeros) == len(set(n.lstrip("0") for n in numeros))
        assert r.get_json()["evaluacion"]["estado"] == "sin_conexion"
        assert_sin_escrituras(env)

    def test_cuando_check_no_responde_el_fallo_se_cachea_unos_segundos(self, env):
        retiro_en_preparacion(env)
        env.esp.check.respuestas["*"] = None
        env.cli.get(f"/retiros/{RID}/check-preparacion?solo_lectura=1")
        n = len(env.esp.check.llamadas)
        env.cli.get(f"/retiros/{RID}/check-preparacion?solo_lectura=1")
        assert len(env.esp.check.llamadas) == n          # no insiste en cada refresco

    @pytest.mark.parametrize("respuesta", [[{"inesperado": 1}], "texto suelto", 42])
    def test_respuesta_de_check_que_no_es_objeto_no_da_500(self, env, respuesta):
        retiro_en_preparacion(env)
        env.esp.check.respuestas["*"] = respuesta
        r = env.cli.get(f"/retiros/{RID}/check-preparacion?solo_lectura=1")
        d = r.get_json()
        assert r.status_code == 200 and d["ok"] is True
        assert d["evaluacion"]["listo"] is False and d["aplicado_ahora"] is False
        assert_sin_escrituras(env)

    def test_reingreso_a_preparacion_no_hereda_el_preparado_anterior(self, env):
        """Los ítems del ciclo anterior siguen en picked=1, pero el evento picking_completo ya es ANTERIOR al último
        paso a preparación: la guía y la línea de tiempo coinciden en «no preparado»."""
        import pickups_module as pm
        retiro_en_preparacion(env)
        for it in env.db.picking:
            it["picked"] = 1
        env.db.agregar_log(RID, "picking_completo", new_status="en_preparacion")
        env.db.agregar_log(RID, "estado_actualizado", new_status="agenda_confirmada")
        env.db.agregar_log(RID, "estado_actualizado", new_status="en_preparacion")
        logs_recientes_primero = sorted(env.db.logs, key=lambda x: -x["id"])
        hitos = pm.pickup_hitos(env.db.solicitudes[RID], [], logs_recientes_primero)
        assert hitos[3]["preparado"] is False             # la línea de tiempo dice NO preparado
        assert paso(guia(env), 5)["estado"] == "espera"   # y la guía coincide

    # ── candados de seguridad del marcado automático ──────────────────────
    def test_doble_lectura_la_primera_no_aplica_la_segunda_si(self, env, monkeypatch):
        """Un «listo» pasajero no basta: hacen falta DOS lecturas separadas por RETIROS_CHECK_CONFIRMACION_S s."""
        monkeypatch.setenv("RETIROS_CHECK_CONFIRMACION_S", "25")
        retiro_en_preparacion(env)
        env.esp.check.respuestas["23732"] = TODO_PICKEADO
        real = time.time
        d1 = env.cli.get(f"/retiros/{RID}/check-preparacion").get_json()
        assert d1["evaluacion"]["listo"] is True and d1["aplicado_ahora"] is False and d1["preparado"] is False
        assert_sin_escrituras(env)
        assert env.db.eventos(RID, "picking_completo") == []
        with mock.patch("time.time", lambda: real() + 10):                 # muy pronto: sigue sin aplicar
            d2 = env.cli.get(f"/retiros/{RID}/check-preparacion").get_json()
        assert d2["aplicado_ahora"] is False
        assert_sin_escrituras(env)
        with mock.patch("time.time", lambda: real() + 60):                 # >= 25 s después (y caché vencida)
            d3 = env.cli.get(f"/retiros/{RID}/check-preparacion").get_json()
        assert d3["aplicado_ahora"] is True and d3["preparado"] is True
        assert len(env.db.eventos(RID, "picking_completo")) == 1
        assert_nada_al_cliente(env, permitidos=(EQUIPO_EMAIL,))

    def test_una_lectura_no_listo_en_medio_reinicia_la_espera(self, env, monkeypatch):
        monkeypatch.setenv("RETIROS_CHECK_CONFIRMACION_S", "25")
        retiro_en_preparacion(env)
        real = time.time
        env.esp.check.respuestas["23732"] = TODO_PICKEADO
        env.cli.get(f"/retiros/{RID}/check-preparacion")                        # 1.ª lectura «listo» (t=0)
        env.esp.check.respuestas["23732"] = A.respuesta_check(
            A.fila_check(solicitado=1, pickeado=1), A.fila_check(solicitado=2, pickeado=1, asignado=1))
        with mock.patch("time.time", lambda: real() + 50):                      # Check ya no lo ve listo
            assert env.cli.get(f"/retiros/{RID}/check-preparacion").get_json()["evaluacion"]["listo"] is False
        env.esp.check.respuestas["23732"] = TODO_PICKEADO
        with mock.patch("time.time", lambda: real() + 100):                     # «listo» otra vez: es una 1.ª lectura
            assert env.cli.get(f"/retiros/{RID}/check-preparacion").get_json()["aplicado_ahora"] is False
        with mock.patch("time.time", lambda: real() + 150):
            assert env.cli.get(f"/retiros/{RID}/check-preparacion").get_json()["aplicado_ahora"] is True
        assert len(env.db.eventos(RID, "picking_completo")) == 1

    @pytest.mark.parametrize("valor", ["0", "false", "no", "off"])
    def test_interruptor_RETIROS_CHECK_AUTO_apagado_no_escribe_nada(self, env, monkeypatch, valor):
        monkeypatch.setenv("RETIROS_CHECK_AUTO", valor)
        retiro_en_preparacion(env)
        env.esp.check.respuestas["23732"] = TODO_PICKEADO
        for _ in range(2):
            real = time.time
            with mock.patch("time.time", lambda: real() + 100):
                d = env.cli.get(f"/retiros/{RID}/check-preparacion").get_json()
            assert d["evaluacion"]["listo"] is True            # lo muestra en pantalla…
            assert d["auto_activo"] is False                    # …avisa que el automático está apagado…
            assert d["aplicado_ahora"] is False and d["preparado"] is False      # …y no marca nada
        assert_sin_escrituras(env)
        assert not any(it["picked"] for it in env.db.picking)
        assert_nada_al_cliente(env)

    def test_interruptor_encendido_por_defecto_lo_informa(self, env):
        retiro_en_preparacion(env)
        env.esp.check.respuestas["23732"] = TODO_PICKEADO
        assert env.cli.get(f"/retiros/{RID}/check-preparacion?solo_lectura=1").get_json()["auto_activo"] is True

    def test_con_unidades_despachadas_no_se_marca_solo(self, env):
        """Check ya da unidades por despachadas (¿entregado antes?): se MUESTRA, pero decide una persona."""
        retiro_en_preparacion(env, items=(("DISCO25", 1),))
        env.esp.check.respuestas["23732"] = A.respuesta_check(A.fila_check(solicitado=1, despachado=1))
        for _ in range(2):
            real = time.time
            with mock.patch("time.time", lambda: real() + 100):
                d = env.cli.get(f"/retiros/{RID}/check-preparacion").get_json()
            assert d["evaluacion"]["listo"] is True and d["evaluacion"]["listo_auto"] is False
            assert "DESPACHADAS" in d["evaluacion"]["alerta"]
            assert d["aplicado_ahora"] is False and d["preparado"] is False
        assert_sin_escrituras(env)
        assert not any(it["picked"] for it in env.db.picking)

    def test_con_datos_raros_no_se_marca_solo(self, env):
        retiro_en_preparacion(env)
        env.esp.check.respuestas["23732"] = A.respuesta_check(A.fila_check(solicitado=1, pickeado="-1"))
        d = env.cli.get(f"/retiros/{RID}/check-preparacion").get_json()
        assert d["evaluacion"]["listo"] is False and d["evaluacion"]["listo_auto"] is False
        assert d["aplicado_ahora"] is False
        assert_sin_escrituras(env)

    def test_fila_de_otro_documento_no_marca_listo(self, env):
        retiro_en_preparacion(env)
        env.esp.check.respuestas["*"] = A.respuesta_check(
            A.fila_check(tipoDocumento="BLV", numeroDocumento="123732", solicitado=1, pickeado=1))
        d = env.cli.get(f"/retiros/{RID}/check-preparacion").get_json()
        assert d["evaluacion"]["listo"] is False and d["evaluacion"]["estado"] == "sin_datos"
        assert d["aplicado_ahora"] is False
        assert_sin_escrituras(env)

    def test_documento_100_por_ciento_cancelado_no_bloquea_ni_marca_solo(self, env):
        """Un documento todo cancelado en Check no cuenta como pendiente; si es el único, no hay nada que dar por listo."""
        retiro_en_preparacion(env, docs=(DOC_A, DOC_B), items=(("DISCO25", 1), ("MANC10", 2)))
        env.esp.check.respuestas["23732"] = TODO_PICKEADO
        env.esp.check.respuestas["23733"] = A.respuesta_check(A.fila_check(numeroDocumento="23733", solicitado=2, cancelado=2))
        d = env.cli.get(f"/retiros/{RID}/check-preparacion").get_json()
        assert d["evaluacion"]["listo"] is True and d["aplicado_ahora"] is True      # el cancelado no frena al otro
        # y si es el único documento: no hay nada pickeado que dar por listo
        retiro_en_preparacion(env, rid=2, docs=("0000023733",), items=(("X", 1),))
        d2 = env.cli.get("/retiros/2/check-preparacion").get_json()
        assert d2["evaluacion"]["listo"] is False and d2["aplicado_ahora"] is False
        assert "cancelado" in d2["evaluacion"]["frase"]

    def test_mas_de_12_documentos_no_se_revisan_solos(self, env):
        """Con más de 12 documentos no se consulta a Check ni se marca nada: se usa la lista manual."""
        retiro_en_preparacion(env, docs=tuple(f"00000{i:05d}" for i in range(13)), items=(("A", 1),))
        env.esp.check.respuestas["*"] = TODO_PICKEADO
        for _ in range(2):
            real = time.time
            with mock.patch("time.time", lambda: real() + 100):
                d = env.cli.get(f"/retiros/{RID}/check-preparacion").get_json()
            ev = d["evaluacion"]
            assert ev["listo"] is False and ev["listo_auto"] is False and ev["documentos"] == 13
            assert "13 documentos" in ev["frase"] and "lista manual" in ev["frase"]
            assert d["aplicado_ahora"] is False and d["preparado"] is False
        assert env.esp.check.llamadas == [], "con más de 12 documentos no se le pregunta nada a Check"
        assert_sin_escrituras(env)

    def test_exactamente_12_documentos_si_se_revisan(self, env):
        retiro_en_preparacion(env, docs=tuple(f"00000{i:05d}" for i in range(12)), items=(("A", 1),))
        env.esp.check.respuestas["*"] = None
        env.cli.get(f"/retiros/{RID}/check-preparacion?solo_lectura=1")
        assert env.esp.check.llamadas

    def test_si_el_retiro_cambia_de_estado_mientras_se_consulta_check_no_se_marca_nada(self, env):
        """El estado se relee DENTRO del candado: si dejó de estar en preparación (p. ej. RETIRADO), no escribe ni avisa."""
        retiro_en_preparacion(env)

        class RespuestasQueCambianElEstado(dict):
            def __contains__(self_inner, k):
                env.db.solicitudes[RID]["status"] = "retirada"     # alguien lo marcó RETIRADO durante la espera
                return super().__contains__(k)

        env.esp.check.respuestas = RespuestasQueCambianElEstado({"*": TODO_PICKEADO})
        d = env.cli.get(f"/retiros/{RID}/check-preparacion").get_json()
        assert d["aplicado_ahora"] is False
        assert env.db.eventos(RID, "picking_completo") == []
        assert not any(it["picked"] for it in env.db.picking)
        assert_nada_al_cliente(env)


# ══════════════════════════════════════════════════════════════════════════════
#  Barrido (lo dispara quien entra al Monitor /retiros)
# ══════════════════════════════════════════════════════════════════════════════
def _capturar_barrido(env, veces=1):
    """Llama al Monitor y devuelve los objetivos de hilo `_check_barrido` que se intentaron lanzar
    (sin lanzarlos). Así se prueba EXACTAMENTE la función que corre en producción."""
    import pickups_module as pm
    capturados = []

    class HiloFalso:
        def __init__(self, group=None, target=None, name=None, args=(), kwargs=None, *, daemon=None):
            self._t = target

        def start(self):
            capturados.append(self._t)

    for _ in range(veces):
        with mock.patch.object(pm.threading, "Thread", HiloFalso), \
                mock.patch.object(pm, "render_template", side_effect=RuntimeError("sin render")), \
                env.app.test_request_context("/retiros"):
            flask.g.user = {"id": 7, "nombre": "Samantha Blacio", "username": "sam@sphs.cl"}
            try:
                env.app.view_functions["pickup_dashboard"]()
            except Exception:
                pass
    env.db.reiniciar_registro()
    return [t for t in capturados if getattr(t, "__name__", "") == "_check_barrido"]


def _correr_en_hilo_suelto(objetivo):
    """Como lo hace pickups_module: threading.Thread(target=...).start(), SIN app.app_context()."""
    t = threading.Thread(target=objetivo, daemon=True)
    t.start()
    t.join(20)


class TestBarrido:
    def test_el_monitor_dispara_el_barrido_a_lo_mas_cada_dos_minutos(self, env):
        assert len(_capturar_barrido(env, veces=3)) == 1

    def test_la_logica_del_barrido_marca_solo_lo_que_check_confirma(self, env):
        retiro_en_preparacion(env, rid=1)
        retiro_en_preparacion(env, rid=2, docs=("0000099999",))
        retiro(env, rid=3, status="agenda_confirmada", confirmed_date="2099-01-01", docs=("0000055555",))   # cita lejana: no se mira
        env.esp.check.respuestas["23732"] = TODO_PICKEADO                   # retiro 1: listo
        env.esp.check.respuestas["99999"] = A.respuesta_check(A.fila_check(solicitado=2, asignado=2))   # retiro 2: no
        env.esp.check.respuestas["*"] = TODO_PICKEADO                       # (el 3 ni se consulta)
        objetivo, = _capturar_barrido(env)
        with env.app.app_context():
            objetivo()
        assert len(env.db.eventos(1, "picking_completo")) == 1
        assert env.db.eventos(2, "picking_completo") == [] and env.db.eventos(3, "picking_completo") == []
        assert env.db.solicitudes[1]["status"] == "en_preparacion"
        assert env.esp.check.rutas == {"/api/ext/GetSeguimientoDespacho"}
        assert not any(p["numdoc"].lstrip("0") == "55555" for _, p in env.esp.check.llamadas), \
            "el barrido solo revisa retiros EN PREPARACIÓN y los confirmados con la cita cercana"
        assert_nada_al_cliente(env, permitidos=(EQUIPO_EMAIL,))

    def test_el_hilo_del_barrido_marca_el_pedido_listo_sin_ficha_abierta(self, env):
        """El hilo corre SIN contexto de app (como en producción) y la BD falsa lo exige: el barrido abre el suyo."""
        retiro_en_preparacion(env)
        env.esp.check.respuestas["23732"] = TODO_PICKEADO
        objetivo, = _capturar_barrido(env)
        _correr_en_hilo_suelto(objetivo)
        assert len(env.db.eventos(RID, "picking_completo")) == 1
        A.esperar_hilos_de_aviso()
        assert destinos_de_correo(env) == [EQUIPO_EMAIL]
        assert_nada_al_cliente(env, permitidos=(EQUIPO_EMAIL,))

    def test_el_barrido_respeta_el_interruptor_apagado(self, env, monkeypatch):
        monkeypatch.setenv("RETIROS_CHECK_AUTO", "0")
        retiro_en_preparacion(env)
        env.esp.check.respuestas["23732"] = TODO_PICKEADO
        objetivo, = _capturar_barrido(env)
        _correr_en_hilo_suelto(objetivo)
        assert env.db.eventos(RID, "picking_completo") == []
        assert_sin_escrituras(env)

    def test_el_barrido_con_doble_lectura_necesita_dos_pasadas(self, env, monkeypatch):
        monkeypatch.setenv("RETIROS_CHECK_CONFIRMACION_S", "25")
        retiro_en_preparacion(env)
        env.esp.check.respuestas["23732"] = TODO_PICKEADO
        objetivo, = _capturar_barrido(env)
        _correr_en_hilo_suelto(objetivo)
        assert env.db.eventos(RID, "picking_completo") == []             # 1.ª lectura: solo toma nota
        real = time.time
        with mock.patch("time.time", lambda: real() + 60):               # 2.ª pasada del barrido, 60 s después
            _correr_en_hilo_suelto(objetivo)
        assert len(env.db.eventos(RID, "picking_completo")) == 1

    def test_un_retiro_con_respuesta_rara_no_frena_a_los_demas(self, env):
        retiro_en_preparacion(env, rid=1)
        retiro_en_preparacion(env, rid=2, docs=("0000099999",))
        env.esp.check.respuestas["23732"] = TODO_PICKEADO
        env.esp.check.respuestas["99999"] = ["no", "es", "un", "objeto"]    # el retiro 2 va primero (id DESC)
        objetivo, = _capturar_barrido(env)
        with env.app.app_context():
            objetivo()
        assert len(env.db.eventos(1, "picking_completo")) == 1
        assert env.db.eventos(2, "picking_completo") == []

    def test_un_retiro_que_revienta_por_BD_no_frena_a_los_demas(self, env):
        """try/except POR retiro: el error puntual del retiro 2 (va primero, id DESC) no aborta el barrido."""
        retiro_en_preparacion(env, rid=1)
        retiro_en_preparacion(env, rid=2, docs=("0000099999",))
        env.esp.check.respuestas["23732"] = TODO_PICKEADO
        env.db.falla_si_params.append((r"from pickup_request_docs", lambda params: params and params[0] == 2,
                                       RuntimeError("error puntual de BD")))
        objetivo, = _capturar_barrido(env)
        _correr_en_hilo_suelto(objetivo)
        assert len(env.db.eventos(1, "picking_completo")) == 1
        assert env.db.eventos(2, "picking_completo") == []


# ══════════════════════════════════════════════════════════════════════════════
#  Candados de código (REGLAS #4.1, #4.4 y «nada nuevo le escribe al cliente real»)
# ══════════════════════════════════════════════════════════════════════════════
def _leer(nombre):
    with open(os.path.join(RAIZ, nombre), encoding="utf-8") as f:
        return f.read()


def _bloque_whitelist_check():
    src = _leer("app.py")
    m = re.search(r"_CHECKWMS_GET_PERMITIDOS\s*=\s*frozenset\(\{(.*?)\}\)", src, re.S)
    assert m, "no se encontró _CHECKWMS_GET_PERMITIDOS en app.py"
    return m.group(1)


def _bloque_nuevo():
    """Desde «GUÍA DE 6 PASOS + PREPARACIÓN POR CHECK» hasta «MARCAR PROPUESTA COMO ACEPTADA MANUALMENTE»:
    guía, confirmaciones, Check, diagnóstico y «Me hago cargo»."""
    src = _leer("pickups_module.py")
    i = src.index("GUÍA DE 6 PASOS + PREPARACIÓN POR CHECK")
    j = src.index("MARCAR PROPUESTA COMO ACEPTADA MANUALMENTE", i)
    return src[i:j]


class TestCandadosDeCodigo:
    def test_el_bloque_nuevo_no_le_escribe_al_cliente(self):
        bloque = _bloque_nuevo()
        for prohibido in (r"_send_ilus_email\(", r"_send_whatsapp\(",
                          r"\bsend_email\s*=\s*True", r"_notificar_cliente"):
            assert not re.search(prohibido, bloque), f"el bloque nuevo usa {prohibido}"
        # Único aviso al cliente permitido (Daniel 2026-10-02, «envíes a preparación en automático»): el MISMO correo «preparing»
        # del botón «Enviar a preparación» (solo trae la fecha agendada), y solo desde el envío automático: en un hilo (ficha abierta)
        # o dentro de la misma petición (el cron, donde Cloud Run no da CPU a un hilo suelto).
        # Segundo (y último) aviso al cliente permitido (Daniel 2026-10-06, «cuando bodega expida en Check, el retiro se dé por retirado — eso
        # le manda al cliente el correo de retiro»): el correo «done» del cierre automático por expedición, SOLO en modo «activo» (en «sombra»
        # la función retorna antes) y SOLO dentro de _retiro_auto_aplicar. Lleva la nota sutil como custom_message.
        llamadas = re.findall(r"\bnotify(?:_async)?\((.*?)\)\s*(?:#.*)?\n", bloque)
        prep = [x for x in llamadas if x.replace(" ", "") == 'req_despues,"preparing"']
        cierre = [x for x in llamadas if x.replace(" ", "") == 'req_despues,"done",custom_message=_NOTA_CIERRE_AUTO']
        assert llamadas and len(prep) + len(cierre) == len(llamadas), llamadas
        assert len(prep) == 2, "el envío automático a preparación (hilo o cron) avisa al cliente"
        assert len(cierre) == 2, "solo el cierre automático por expedición (hilo o cron) avisa al cliente con el correo de retiro"
        i_fn = bloque.index("def _retiro_auto_aplicar(")
        i_fin = bloque.index("def _prep_auto_candidatos(", i_fn)
        assert all(bloque.index(x) > i_fn for x in ('notify(req_despues, "done"', 'notify_async(req_despues, "done"')), \
            "el correo de retiro solo sale desde _retiro_auto_aplicar"
        cuerpo_fn = bloque[i_fn:i_fin]
        i_sombra = cuerpo_fn.index("SOMBRA: solo aviso al equipo")
        assert cuerpo_fn[i_sombra:].lstrip().split("\n")[1].strip() == "return True" \
            and i_sombra < cuerpo_fn.index('notify(req_despues, "done"'), \
            "en modo sombra la función debe retornar ANTES de escribirle al cliente"
        # el único aviso permitido es al equipo interno y sin correo directo
        for llamada in re.findall(r"_notificar_equipo_retiros\((.*?)\)\s*\n", bloque, re.S):
            assert "send_email=False" in llamada, llamada

    def test_el_bloque_nuevo_solo_habla_con_check_por_la_puerta_unica(self):
        bloque = _bloque_nuevo()
        assert not re.search(r"\brequests\b|urllib|http\.client|urlopen|\.post\(|\.put\(|\.delete\(|\.patch\(", bloque)
        rutas = set(re.findall(r'"(/api/ext/[A-Za-z0-9_]+)"', bloque))
        # Retiros solo consulta reportes GET que YA estaban en la lista blanca: el seguimiento por documento (la guía) y,
        # solo en el diagnóstico de admin (2026-10-02, «quién pickeó y cuándo»), los movimientos V2 y el control de salida.
        assert rutas == {"/api/ext/GetSeguimientoDespacho", "/api/ext/GetStockTrazabilidadV2", "/api/ext/GetControlSalida"}
        assert rutas <= set(re.findall(r'"(/api/ext/[A-Za-z0-9_]+)"', _bloque_whitelist_check()))
        assert 'ctx.get("_checkwms_get")' in bloque

    def test_el_bloque_nuevo_no_toca_el_erp(self):
        bloque = _bloque_nuevo()
        for prohibido in ("_random_sql_query", "_random_sql_pool", "erp_engine", "pymssql", "MAEEDO", "MAEDDO"):
            assert prohibido not in bloque

    def test_el_sql_nuevo_va_parametrizado(self):
        """Toda consulta del bloque nuevo (mysql_fetchone/fetchall/execute) es una cadena fija con %s: lo único
        que un f-string puede interpolar son los nombres de tabla LOG/REQ/PROP. Nada de .format ni de `%`."""
        import ast
        src = _leer("pickups_module.py")
        i = src.index("GUÍA DE 6 PASOS + PREPARACIÓN POR CHECK")
        j = src.index("MARCAR PROPUESTA COMO ACEPTADA MANUALMENTE", i)
        desde, hasta = src[:i].count("\n") + 1, src[:j].count("\n") + 1
        n_consultas = 0
        for nodo in ast.walk(ast.parse(src)):
            if not (isinstance(nodo, ast.Call) and isinstance(nodo.func, ast.Name)
                    and nodo.func.id in ("mysql_fetchone", "mysql_fetchall", "mysql_execute")):
                continue
            if not (desde <= nodo.lineno <= hasta):
                continue
            n_consultas += 1
            sql = nodo.args[0]
            assert isinstance(sql, (ast.Constant, ast.JoinedStr)), f"línea {nodo.lineno}: SQL armado con algo raro"
            if isinstance(sql, ast.JoinedStr):
                for parte in sql.values:
                    if isinstance(parte, ast.FormattedValue):
                        assert isinstance(parte.value, ast.Name) and parte.value.id in ("LOG", "REQ", "PROP"), \
                            f"línea {nodo.lineno}: interpola {ast.dump(parte.value)} dentro del SQL"
        assert n_consultas >= 12, "el recorrido de ast no encontró las consultas del bloque"


# ══════════════════════════════════════════════════════════════════════════════
#  Entrada del Monitor y ficha (no deben romperse por la guía)
# ══════════════════════════════════════════════════════════════════════════════
class TestIntegracionConLaFicha:
    def _abrir_ficha(self, env):
        import pickups_module as pm
        with mock.patch.object(pm, "render_template", return_value="ok") as rt:
            with env.app.test_request_context(f"/retiros/{RID}"):
                flask.g.user = {"id": 7, "nombre": "Samantha Blacio", "username": "sam@sphs.cl"}
                resp = env.app.view_functions["pickup_detail"](RID)
        return resp, rt

    def test_la_ficha_recibe_la_guia_de_6_pasos(self, env):
        retiro(env)
        env.db.reiniciar_registro()
        resp, rt = self._abrir_ficha(env)
        assert resp == "ok" and rt.called
        g = rt.call_args.kwargs["guia"]
        assert [p["n"] for p in g["pasos"]] == [1, 2, 3, 4, 5, 6] and g["siguiente"] == 1
        json.dumps(g)                      # va a la plantilla con |tojson: tiene que ser serializable
        assert_sin_escrituras(env)         # abrir la ficha no escribe nada
        assert_nada_al_cliente(env)

    def test_si_la_guia_falla_la_ficha_se_abre_igual_sin_guia(self, env):
        retiro(env)
        env.db.falla_si.append((r"document_number FROM pickup_request_docs", RuntimeError("x")))
        resp, rt = self._abrir_ficha(env)
        assert resp == "ok" and rt.called
        assert rt.call_args.kwargs["guia"] is None


class TestDiagnosticoCheck:
    def test_pide_tipo_y_numero(self, env):
        assert env.cli.get("/retiros/admin/check-diagnostico").status_code == 400

    def test_solo_consulta_seguimiento_despacho_por_get(self, env):
        env.esp.check.respuestas["*"] = TODO_PICKEADO
        r = env.cli.get("/retiros/admin/check-diagnostico?tipo=blv&num=23732")
        assert r.status_code == 200 and r.get_json()["ok"] is True
        assert env.esp.check.rutas == {"/api/ext/GetSeguimientoDespacho"}
        assert_sin_escrituras(env)
        assert_nada_al_cliente(env)
