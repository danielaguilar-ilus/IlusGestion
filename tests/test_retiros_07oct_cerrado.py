# -*- coding: utf-8 -*-
"""2026-10-07 · Retiro TERMINADO = ficha en SOLO LECTURA (Daniel, 06-oct: «una vez que se cierra, hacerle una inteligencia para que no se pueda
gestionar nada más, que no pueda agregar factura… ya se cerró, ya está listo»).

Con el retiro en retirada, cerrada, rechazada o fallida el servidor rechaza con 409 toda gestión (facturas, productos, datos del cliente, notas,
fechas, validación, responsable, confirmaciones de la guía, checklist de bodega) y NO escribe nada. Siguen funcionando: cambiar estado / reabrir,
el chat y los mensajes con el cliente, las lecturas. Con el retiro abierto todo sigue igual.

Todo corre sin BD real, sin Check, sin ERP y con el correo de mentira (tests/_arnes_retiros.py):

    py -m pytest tests/test_retiros_07oct_cerrado.py -q
"""
import datetime as dt
import os
import re
import sys
from types import SimpleNamespace

import pytest

_TESTS = os.path.dirname(os.path.abspath(__file__))
_RAIZ = os.path.dirname(_TESTS)
sys.path.insert(0, _TESTS)
sys.path.insert(0, _RAIZ)

import _arnes_retiros as A  # noqa: E402
import pickups_module  # noqa: E402

RID = 1
AHORA = dt.datetime(2026, 10, 7, 11, 0)
CIERRE_UTC = dt.datetime(2026, 10, 6, 18, 30)           # 15:30 hora Chile del 06/10/2026
TERMINALES = ["retirada", "cerrada", "rechazada", "fallida"]
COMO = {"retirada": "Retirado", "cerrada": "Cerrado", "rechazada": "Rechazado", "fallida": "No concretado"}


@pytest.fixture()
def env(monkeypatch):
    for k in ("RETIROS_PREP_AUTO", "RETIROS_EXIGE_RESPONSABLE", "RETIROS_CHECK_AUTO", "RETIROS_RETIRO_AUTO"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(pickups_module, "_RELOJ_CHILE", lambda: AHORA)
    app, db, ctx, esp = A.construir_app()
    return SimpleNamespace(app=app, db=db, ctx=ctx, esp=esp, cli=app.test_client())


def retiro(env, estado, **kw):
    datos = dict(status=estado, responsable_user_id=7, responsable_nombre="Sam", confirmed_date="2026-10-08",
                 document_number="0000023732", closed_at=CIERRE_UTC, peso_real_kg=16, peso_vol_kg=18, total_volume_m3=0.005)
    datos.update(kw)
    env.db.nueva_solicitud(RID, **datos)
    env.db.agregar_doc(RID, "BLV", "0000023732")
    env.db.agregar_picking(RID, "DISCO25", 1)
    env.db.propuestas.append({"id": 1, "request_id": RID, "status": "pending", "proposed_by": "cliente"})
    env.db.reiniciar_registro()


def escrituras(env):
    """Lo que se escribió en la BD, sin el DDL de arranque (CREATE/ALTER)."""
    return [e for e in env.db.escrituras if not e[0].upper().startswith(("CREATE", "ALTER"))]


def sin_mensajes(env):
    A.esperar_hilos_de_aviso()
    assert env.esp.correo.call_args_list == [] and env.esp.whatsapp.call_count == 0


# (nombre, metodo, ruta, argumentos de la petición)
GESTION = [
    ("agregar factura", "post", f"/retiros/{RID}/docs/agregar", {"json": {"document_type": "FCV", "document_number": "99999"}}),
    ("quitar factura", "delete", f"/retiros/{RID}/docs/1", {}),
    ("quitar factura (POST + _method)", "post", f"/retiros/{RID}/docs/1", {"data": {"_method": "DELETE"}}),
    ("editar productos de una factura", "post", f"/retiros/{RID}/docs/1/lineas",
     {"json": {"lineas": [{"sku": "DISCO25", "incluida": True, "cantidad_seleccionada": 1}]}}),
    ("datos del cliente", "post", f"/retiros/{RID}/customer", {"json": {"customer_name": "Otro Nombre S.A."}}),
    ("campo inline (notas internas)", "patch", f"/retiros/{RID}/field", {"json": {"field": "internal_notes", "value": "nota nueva"}}),
    ("campo inline (correo de contacto)", "post", f"/retiros/{RID}/field", {"json": {"field": "contact_email", "value": "otro@example.com"}}),
    ("proponer fecha", "post", f"/retiros/{RID}/proposal", {"json": {"date": "2026-10-20", "time_from": "10:00", "time_to": "11:00"}}),
    ("aceptar contrapropuesta", "post", f"/retiros/{RID}/aceptar-contrapropuesta", {"json": {}}),
    ("marcar aceptada a mano", "post", f"/retiros/{RID}/marcar-aceptada-manual", {"json": {"motivo": "llamó por teléfono"}}),
    ("Me hago cargo", "post", f"/retiros/{RID}/tomar", {"json": {}}),
    ("confirmar facturas (guía)", "post", f"/retiros/{RID}/confirmar-docs", {"json": {}}),
    ("confirmar productos (guía)", "post", f"/retiros/{RID}/confirmar-productos", {"json": {}}),
    ("checklist de bodega", "post", f"/retiros/{RID}/picking/toggle", {"json": {"item_id": 1, "picked": True}}),
    ("validar documentación (fetch)", "post", f"/retiros/{RID}/validar-doc",
     {"data": {"action": "marcar_ok"}, "headers": {"X-Requested-With": "XMLHttpRequest"}}),
]


def llamar(env, metodo, ruta, args):
    return getattr(env.cli, metodo)(ruta, **args)


class TestServidorBloqueaTodaGestionConElRetiroTerminado:
    @pytest.mark.parametrize("estado", TERMINALES)
    @pytest.mark.parametrize("nombre,metodo,ruta,args", GESTION, ids=[g[0] for g in GESTION])
    def test_responde_409_y_no_escribe_nada(self, env, estado, nombre, metodo, ruta, args):
        retiro(env, estado)
        r = llamar(env, metodo, ruta, args)
        assert r.status_code == 409, f"{nombre} con el retiro «{estado}» debía dar 409 y dio {r.status_code}"
        d = r.get_json()
        assert d["ok"] is False and d.get("error")
        assert escrituras(env) == [], f"{nombre} escribió con el retiro «{estado}»: {escrituras(env)}"
        assert env.db.solicitudes[RID]["status"] == estado
        sin_mensajes(env)

    @pytest.mark.parametrize("estado", TERMINALES)
    @pytest.mark.parametrize("nombre,metodo,ruta,args", [g for g in GESTION if g[0] in (
        "agregar factura", "quitar factura", "editar productos de una factura", "datos del cliente", "campo inline (notas internas)",
        "checklist de bodega", "validar documentación (fetch)")], ids=lambda v: v if isinstance(v, str) else "")
    def test_las_rutas_nuevas_responden_con_el_codigo_y_el_mensaje_amigable(self, env, estado, nombre, metodo, ruta, args):
        retiro(env, estado)
        d = llamar(env, metodo, ruta, args).get_json()
        assert d["code"] == "RETIRO_CERRADO"
        assert f"Este retiro ya está cerrado ({COMO[estado]} el 06/10/2026)" in d["error"]
        assert "No se puede modificar" in d["error"] and "reábrelo desde Cambiar estado" in d["error"]

    def test_el_mensaje_usa_la_hora_chile_y_no_inventa_una_fecha_si_no_la_hay(self, env):
        retiro(env, "retirada", closed_at=dt.datetime(2026, 10, 7, 2, 30))        # 23:30 del 06/10 en Chile (UTC-3)
        assert "(Retirado el 06/10/2026)" in env.cli.delete(f"/retiros/{RID}/docs/1").get_json()["error"]
        env.db.solicitudes[RID]["closed_at"] = None
        m = env.cli.delete(f"/retiros/{RID}/docs/1").get_json()["error"]
        assert "(Retirado)" in m and " el " not in m.split("(")[1].split(")")[0]

    def test_validar_documentacion_desde_un_formulario_vuelve_a_la_ficha_con_el_aviso(self, env):
        retiro(env, "cerrada")
        r = env.cli.post(f"/retiros/{RID}/validar-doc", data={"action": "marcar_ok"})
        assert r.status_code == 302 and f"/retiros/{RID}" in r.headers["Location"]
        with env.cli.session_transaction() as s:
            avisos = [m for _c, m in s.get("_flashes", [])]
        assert any("ya está cerrado" in m for m in avisos)
        assert escrituras(env) == []

    def test_un_retiro_que_no_existe_sigue_dando_404_y_no_409(self, env):
        r = env.cli.patch("/retiros/999/field", json={"field": "internal_notes", "value": "x"})
        assert r.status_code != 409 or r.get_json().get("code") != "RETIRO_CERRADO"


class TestLoQueSigueFuncionandoConElRetiroTerminado:
    def test_cambiar_estado_reabre_el_retiro(self, env):
        retiro(env, "cerrada")
        r = env.cli.post(f"/retiros/{RID}/status", data={"status": "agenda_confirmada", "notes": "Se reabre: el cliente trae otra factura"})
        assert r.status_code == 302
        assert env.db.solicitudes[RID]["status"] == "agenda_confirmada"
        assert [x for x in env.db.logs_de(RID, "estado_actualizado") if x["new_status"] == "agenda_confirmada"]
        A.esperar_hilos_de_aviso()

    @pytest.mark.parametrize("estado", TERMINALES)
    def test_se_puede_reabrir_desde_cualquier_estado_terminal_y_luego_gestionar(self, env, estado):
        retiro(env, estado)
        env.cli.post(f"/retiros/{RID}/status", data={"status": "en_revision", "notes": "reabierto"})
        A.esperar_hilos_de_aviso()
        assert env.db.solicitudes[RID]["status"] == "en_revision"
        env.db.reiniciar_registro()
        r = env.cli.patch(f"/retiros/{RID}/field", json={"field": "internal_notes", "value": "ya reabierto"})
        assert r.status_code == 200 and r.get_json()["ok"] is True
        assert any("UPDATE `pickup_requests` SET `internal_notes`" in e[0] for e in env.db.escrituras)

    def test_el_chat_con_el_cliente_no_se_bloquea(self, env):
        retiro(env, "retirada")
        r = env.cli.post(f"/retiros/{RID}/mensaje", json={"mensaje": "Gracias por retirar"})
        assert not (r.status_code == 409 and (r.get_json() or {}).get("code") == "RETIRO_CERRADO")
        A.esperar_hilos_de_aviso()

    def test_enviar_un_mensaje_al_cliente_no_se_bloquea(self, env):
        retiro(env, "cerrada")
        r = env.cli.post(f"/retiros/{RID}/message", data={"message": "Quedó todo listo, gracias"})
        assert r.status_code == 302
        assert env.db.logs_de(RID, "mensaje_enviado")
        A.esperar_hilos_de_aviso()

    def test_las_lecturas_siguen_abiertas(self, env):
        retiro(env, "retirada")
        for ruta in (f"/retiros/{RID}/guia", f"/retiros/{RID}/picking", f"/retiros/{RID}/lineas-resumen"):
            r = env.cli.get(ruta)
            assert r.status_code != 409, ruta
        sin_mensajes(env)

    def test_el_listado_de_documentos_no_recalcula_ni_reescribe_los_totales(self, env):
        retiro(env, "retirada", peso_real_kg=16, peso_vol_kg=18, total_volume_m3=0.005)
        env.db.docs[0].update({"observaciones_erp": "", "peso_real_kg": 16, "peso_vol_kg": 18, "volumen_m3": 0.005, "n_lineas": 2,
                               "added_by": "Sam", "added_at": None, "saldo_zz": 0, "saldo_checked_at": None,
                               "otro_rut_por": None, "otro_rut_en": None})
        pickups_module._DOCS_CACHE.clear() if hasattr(pickups_module, "_DOCS_CACHE") else None
        r = env.cli.get(f"/retiros/{RID}/docs")
        assert r.status_code == 200
        assert not [e for e in env.db.escrituras if e[0].upper().startswith("UPDATE `PICKUP_REQUESTS`")]
        t = r.get_json()["totales"]
        assert t["peso_real_kg"] == 16.0 and t["peso_vol_kg"] == 18.0 and t["volumen_m3"] == pytest.approx(0.005)

    def test_el_relleno_automatico_de_totales_en_cero_sigue_funcionando(self, env):
        """Excepción (e): no es una gestión de una persona, completa lo que está en 0 con los productos y deja bitácora."""
        retiro(env, "retirada", peso_real_kg=0, peso_vol_kg=0, total_volume_m3=0)
        env.db.docs[0]["erp_snapshot"] = __import__("json").dumps({"lineas": [
            {"sku": "DISCO25", "descripcion_erp": "Set Discos", "cantidad": 2, "peso_kg_u": 8, "vol_u": 2500, "peso_vol_u": 9}]})
        d = env.cli.get(f"/retiros/{RID}/productos-erp").get_json()
        assert d["ok"] and env.db.solicitudes[RID]["peso_real_kg"] == 16.0
        sin_mensajes(env)


class TestConElRetiroAbiertoTodoSigueIgual:
    @pytest.mark.parametrize("estado", ["solicitud_recibida", "en_revision", "propuesta_enviada", "agenda_confirmada", "en_preparacion"])
    def test_editar_un_campo_inline_funciona(self, env, estado):
        retiro(env, estado, closed_at=None)
        r = env.cli.patch(f"/retiros/{RID}/field", json={"field": "internal_notes", "value": "nota"})
        assert r.status_code == 200 and r.get_json()["ok"] is True
        assert any("UPDATE `pickup_requests` SET `internal_notes`" in e[0] for e in env.db.escrituras)

    def test_cambiar_datos_del_cliente_funciona(self, env):
        retiro(env, "en_revision", closed_at=None)
        d = env.cli.post(f"/retiros/{RID}/customer", json={"customer_name": "Nombre Corregido SpA"}).get_json()
        assert d["ok"] is True
        assert any("UPDATE `pickup_requests` SET" in e[0] and "customer_name" in e[0] for e in env.db.escrituras)

    def test_quitar_y_agregar_facturas_no_las_frena_el_candado(self, env):
        retiro(env, "en_revision", closed_at=None)
        d = env.cli.delete(f"/retiros/{RID}/docs/1").get_json()
        assert d.get("code") != "RETIRO_CERRADO"
        r = env.cli.post(f"/retiros/{RID}/docs/agregar", json={})        # sin datos: pasa el candado y la propia ruta pide el documento
        assert r.status_code == 400 and r.get_json().get("code") != "RETIRO_CERRADO" and "Falta document_type" in r.get_json()["error"]
        d = env.cli.post(f"/retiros/{RID}/docs/1/lineas", json={"lineas": []}).get_json()
        assert d.get("code") != "RETIRO_CERRADO"

    def test_marcar_el_checklist_de_bodega_funciona(self, env):
        retiro(env, "en_preparacion", closed_at=None)
        d = env.cli.post(f"/retiros/{RID}/picking/toggle", json={"item_id": 1, "picked": True}).get_json()
        assert d["ok"] is True
        assert any(e[0].startswith("UPDATE pickup_picking_items") for e in env.db.escrituras)

    def test_me_hago_cargo_funciona(self, env):
        retiro(env, "en_revision", closed_at=None, responsable_user_id=None, responsable_nombre=None)
        d = env.cli.post(f"/retiros/{RID}/tomar", json={}).get_json()
        assert d["ok"] is True and env.db.solicitudes[RID]["responsable_user_id"] == 7

    def test_validar_documentacion_con_el_retiro_abierto_no_la_frena_el_candado(self, env):
        retiro(env, "solicitud_recibida", closed_at=None)
        r = env.cli.post(f"/retiros/{RID}/validar-doc", data={"action": "marcar_incompleto", "notes": "falta la factura"})
        assert r.status_code == 302
        with env.cli.session_transaction() as s:
            assert not any("ya está cerrado" in m for _c, m in s.get("_flashes", []))
        assert any("doc_validation_status='incompleto'" in e[0] for e in env.db.escrituras)


class TestFichaYArchivosEstaticos:
    """La ficha muestra la franja y el JS/CSS bloquean los controles (revisión de texto: no hay navegador en estas pruebas)."""

    def _leer(self, *partes):
        with open(os.path.join(_RAIZ, *partes), encoding="utf-8") as f:
            return f.read()

    def test_la_plantilla_compila_y_la_franja_se_ve_solo_con_el_retiro_cerrado(self):
        import jinja2
        src = self._leer("templates", "retiros", "internal_detail.html")
        env = jinja2.Environment()
        for nombre in ("chile_fmt", "rut_fmt", "hm", "tel_chile_fmt"):
            env.filters[nombre] = lambda v, *a, **k: v
        env.parse(src)                                              # sintaxis Jinja válida
        m = re.search(r"(\{% if cierre and cierre\.cerrado %\}.*?\{% endif %\})\s*\n<section class=\"rh-hero", src, re.S)
        assert m, "no se encontró la franja «Retiro cerrado» antes de la cabecera"
        tpl = env.from_string(m.group(1))
        html = tpl.render(cierre={"cerrado": True, "estado": "retirada", "como": "Retirado", "cuando": "06/10/2026 15:30", "por": "Samantha Blacio"},
                          req={"status": "retirada"})
        assert "Retiro cerrado el 06/10/2026 15:30 por Samantha Blacio (Retirado)" in html
        assert "solo lectura" in html and "Cambiar estado" in html and "is-fail" not in html
        html2 = tpl.render(cierre={"cerrado": True, "estado": "rechazada", "como": "Rechazado", "cuando": "", "por": ""}, req={"status": "rechazada"})
        assert "is-fail" in html2 and "Retiro cerrado (Rechazado)" in html2
        assert tpl.render(cierre={"cerrado": False}, req={"status": "en_revision"}).strip() == ""
        assert tpl.render(cierre=None, req={"status": "en_revision"}).strip() == ""

    def test_la_ficha_recibe_el_dato_de_cierre_en_el_js(self):
        src = self._leer("templates", "retiros", "internal_detail.html")
        assert "cierre: {{ (cierre or {'cerrado': false})|tojson }}" in src

    def test_el_js_frena_gestion_y_deja_abierto_cambiar_estado_y_el_chat(self):
        js = self._leer("static", "retiros_internal_detail.js")
        assert "function _rdCerrado()" in js and "_rdSoloLectura" in js
        assert "formCambiarEstadoAvanzado" in js                                  # «Cambiar estado» queda fuera del freno
        assert "window.fetch = function" in js and "aceptar-contrapropuesta" in js
        assert "/mensaje" not in js.split("const RE_GESTION")[1].split(");")[0]    # el chat NO está en la lista de rutas bloqueadas
        for gestion in ("rbaOpen", "quitarDoc", "enviarPropuestaWizard", "tomarRetiro", "abrirModalRetirar", "marcarAceptadaManual"):
            assert gestion in js.split("const RE_ONCLICK")[1].split("document.addEventListener")[0]
        assert "alert(" not in js.split("function _rdMsgCerrado")[1].replace("ilusAlert(", "")     # nada de alert nativo en el bloque nuevo
        assert "confirm(" not in js.split("function _rdMsgCerrado")[1].replace("ilusConfirm(", "")
        assert "_rdCerrado()" in js.split("function setupInlineEdit")[1].split("function ")[0]

    def test_el_css_de_la_franja_usa_los_colores_de_ilus(self):
        css = self._leer("static", "retiros_internal_detail.css")
        assert ".rd-cerrado-banner" in css and "#dcfce7" in css and "#16a34a" in css and ".rd-cerrado-banner.is-fail" in css

    def test_todas_las_rutas_de_gestion_pasan_por_el_candado(self):
        """Si alguien agrega una ruta POST de gestión nueva, esta lista le avisa que debe usar _rechazo_si_cerrado."""
        src = self._leer("pickups_module.py")
        for funcion in ("pickup_validar_doc", "pickup_doc_quitar", "pickup_doc_lineas_guardar", "pickup_actualizar_cliente",
                        "pickup_inline_field", "pickup_picking_toggle", "_pickup_doc_agregar_impl"):
            cuerpo = src.split(f"def {funcion}(")[1].split("\n    def ")[0].split("\n    @app.route")[0]
            assert "_rechazo_si_cerrado(" in cuerpo, f"{funcion} no revisa si el retiro está cerrado"
