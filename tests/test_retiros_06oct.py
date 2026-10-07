# -*- coding: utf-8 -*-
"""2026-10-06 (Daniel, AMBIENTE REAL: «ten mucho cuidado de enviar algún correo… no generes ningún mensaje ni cambio que moleste al cliente»).

  · El registro de Check (OT, quién, cuándo) queda guardado y se ve con el retiro completado.
  · Peso, peso volumétrico y m³ de los productos llegan al retiro (estaban en 0) sin pisar nunca una cubicación hecha por una persona.
  · Horario de cobertura (08:00–17:00, colación 13:00–14:00, días hábiles) + aviso antes de gestionar fuera de horario.
  · Monitor: kg y peso volumétrico reales y «Solicitada dd/mm/aaaa hh:mm».

Todo corre sin BD real, sin Check, sin ERP y con el correo de mentira (tests/_arnes_retiros.py):

    py -m pytest tests/test_retiros_06oct.py -q
"""
import datetime as dt
import json
import os
import sys
import threading
import time
from types import SimpleNamespace
from unittest import mock
from unittest.mock import MagicMock

import flask
import pytest

_TESTS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _TESTS)
sys.path.insert(0, os.path.dirname(_TESTS))

import _arnes_retiros as A  # noqa: E402
import pickups_module  # noqa: E402

RID = 1
AHORA = dt.datetime(2026, 10, 7, 11, 0)      # miércoles 11:00 = dentro de la cobertura


def _ot(**kw):
    base = {"ot": "481516", "tipoOT": "PICKING", "estado": "TERMINADA", "doc": "BLV-0000023732", "entidad": "CLIENTE DEMO SPA",
            "feInicioOT": "2026-10-05T08:11:10", "fechaFin": "2026-10-05T08:26:55", "ua": "UA1012965", "codigo": "MANC10",
            "descripcion": "Mancuerna hexagonal 10 kg", "sol": 2, "ejec": 2, "usuarioPicking": "Luis Felipe Bourre"}
    base.update(kw)
    return base


@pytest.fixture()
def env(monkeypatch):
    for k in ("RETIROS_PREP_AUTO", "RETIROS_COBERTURA_DESDE", "RETIROS_COBERTURA_HASTA", "RETIROS_COBERTURA_COLACION_DESDE",
              "RETIROS_COBERTURA_COLACION_HASTA", "RETIROS_EXIGE_RESPONSABLE", "RETIROS_CHECK_AUTO"):
        monkeypatch.delenv(k, raising=False)
    reloj = {"ahora": AHORA}
    monkeypatch.setattr(pickups_module, "_RELOJ_CHILE", lambda: reloj["ahora"])
    app, db, ctx, esp = A.construir_app()
    return SimpleNamespace(app=app, db=db, ctx=ctx, esp=esp, cli=app.test_client(), reloj=reloj)


def retiro(env, status="agenda_confirmada", **kw):
    kw.setdefault("responsable_user_id", 7)
    kw.setdefault("responsable_nombre", "Samantha Blacio")
    kw.setdefault("confirmed_date", AHORA.date().isoformat())
    env.db.nueva_solicitud(RID, status=status, **kw)
    env.db.agregar_doc(RID, "BLV", "0000023732")
    env.db.reiniciar_registro()


def con_cache(env, filas):
    traer = MagicMock(name="_checkwms_trazabilidad_rows")
    env.ctx["_CHECKWMS_TRAZA"] = {"ts": time.time(), "rows": filas, "lock": threading.Lock()}
    env.ctx["_checkwms_trazabilidad_rows"] = traer
    return traer


def actividad(env):
    r = env.cli.get(f"/retiros/{RID}/check-actividad")
    assert r.status_code == 200
    return r.get_json()


def inserts_snapshot(env):
    return [e for e in env.db.escrituras if e[0].lower().startswith("insert into pickup_check_snapshots")]


def sin_mensajes(env):
    assert env.esp.correo.call_args_list == [] and env.esp.whatsapp.call_args_list == []


# ══════════════════════════════════════════════════════════════════════════════
#  Registro de Check guardado
# ══════════════════════════════════════════════════════════════════════════════
class TestRegistroDeCheck:
    def test_lo_que_informa_check_queda_guardado(self, env):
        retiro(env)
        con_cache(env, [_ot(), _ot(ot="PCKM2", usuarioPicking="Samuel Gabriel Infante", doc="BLV-0000023732")])
        d = actividad(env)
        assert d["estado"] == "listo" and not d.get("desde_registro")
        guardado = json.loads(env.db.snapshots[RID]["payload"])
        ots = guardado["documentos"][0]["ots"]
        assert {o["ot"] for o in ots} == {"481516", "PCKM2"}
        nombres = json.dumps(ots, ensure_ascii=False)
        assert "Luis Felipe Bourre" in nombres and "Samuel Gabriel Infante" in nombres
        assert guardado["guardado_en"] == "07/10/2026 11:00"

    def test_no_se_vuelve_a_escribir_si_no_cambio_nada(self, env):
        retiro(env)
        con_cache(env, [_ot()])
        actividad(env)
        actividad(env)
        actividad(env)
        assert len(inserts_snapshot(env)) == 1

    def test_con_el_retiro_completado_se_muestra_lo_guardado_sin_pedirle_nada_a_check(self, env):
        retiro(env)
        con_cache(env, [_ot()])
        actividad(env)                                         # queda guardado mientras se preparaba
        env.db.solicitudes[RID]["status"] = "retirada"
        traer = con_cache(env, None)                           # Check ya no tiene el reporte en memoria
        env.esp.check.llamadas.clear()
        d = actividad(env)
        assert d["estado"] == "listo" and d["desde_registro"] is True and d["registrado"] == "07/10/2026 11:00"
        assert d["documentos"][0]["ots"][0]["ot"] == "481516"
        assert any(p["valor"] == "Luis Felipe Bourre" for p in d["documentos"][0]["ots"][0]["personas"])
        traer.assert_not_called()
        assert env.esp.check.llamadas == []

    def test_si_check_ya_no_trae_el_documento_se_ve_lo_guardado(self, env):
        retiro(env)
        con_cache(env, [_ot()])
        actividad(env)
        con_cache(env, [_ot(ot="OTRO", doc="FCV-0000099999")])       # el reporte ya no trae este documento
        d = actividad(env)
        assert d["desde_registro"] is True and d["documentos"][0]["ots"][0]["ot"] == "481516"

    def test_lo_nuevo_se_fusiona_con_lo_guardado_por_ot(self, env):
        retiro(env)
        con_cache(env, [_ot()])
        actividad(env)
        con_cache(env, [_ot(ot="PCKM2", usuarioPicking="Samuel Gabriel Infante")])      # Check ahora solo trae la segunda OT
        actividad(env)
        guardado = json.loads(env.db.snapshots[RID]["payload"])
        assert {o["ot"] for o in guardado["documentos"][0]["ots"]} == {"481516", "PCKM2"}

    def test_retiro_completado_sin_registro_lo_pide_a_check_una_vez_y_queda_guardado(self, env):
        retiro(env, status="retirada")
        traer = con_cache(env, None)
        suelta = threading.Event()
        traer.side_effect = lambda **k: suelta.wait(5)         # Check tarda 15–35 s en entregar el volcado
        d = actividad(env)
        assert d["estado"] == "cargando"
        t0 = time.time()
        while time.time() - t0 < 3 and not traer.called:
            time.sleep(0.02)
        traer.assert_called_once_with(forzar=True)
        suelta.set()
        time.sleep(0.1)
        con_cache(env, [_ot()])                                 # llegó el reporte
        d = actividad(env)
        assert d["estado"] == "listo" and RID in env.db.snapshots

    def test_cerrar_el_retiro_guarda_lo_que_check_ya_tenia_y_no_le_pide_nada(self, env):
        retiro(env)
        con_cache(env, [_ot()])
        r = env.cli.post(f"/retiros/{RID}/status", data={"status": "retirada", "retirado_por": "Gerd Müller"})
        assert r.status_code == 302 and env.db.solicitudes[RID]["status"] == "retirada"
        assert RID in env.db.snapshots
        assert env.esp.check.llamadas == []

    def test_si_el_registro_no_se_puede_guardar_no_se_rompe_nada(self, env):
        retiro(env)
        con_cache(env, [_ot()])
        env.db.falla_escritura_si.append((r"insert into pickup_check_snapshots", RuntimeError("tabla caída")))
        d = actividad(env)
        assert d["estado"] == "listo" and RID not in env.db.snapshots

    def test_check_solo_se_consulta_por_la_lista_blanca(self, env):
        retiro(env)
        con_cache(env, [_ot()])
        actividad(env)
        assert env.esp.check.rutas <= {"/api/ext/GetSeguimientoDespacho", "/api/ext/GetStockTrazabilidad", "/api/ext/GetStockTrazabilidadV2",
                                       "/api/ext/GetControlSalida", "/api/ext/GetReporteStock"}
        sin_mensajes(env)

    def test_el_registro_se_borra_con_el_retiro(self, env):
        import pickups_module as pm
        src = open(os.path.join(os.path.dirname(_TESTS), "pickups_module.py"), encoding="utf-8").read()
        assert "DELETE FROM pickup_check_snapshots WHERE request_id=%s" in src


# ══════════════════════════════════════════════════════════════════════════════
#  Peso, peso volumétrico y m³ de los productos
# ══════════════════════════════════════════════════════════════════════════════
LINEAS_CON_MEDIDAS = {"lineas": [
    {"sku": "DISCO25", "descripcion_erp": "Set Discos 2,5 - 5 kg", "cantidad": 2, "peso_kg_u": 10, "vol_u": 50000, "peso_vol_u": 12},
    {"sku": "MANC10", "descripcion_erp": "Mancuerna hexagonal 10 kg", "cantidad": 1, "peso_kg_u": 5, "vol_u": 20000, "peso_vol_u": 4}]}


def abrir_ficha(env):
    with mock.patch.object(pickups_module, "render_template", return_value="ok") as rt:
        with env.app.test_request_context(f"/retiros/{RID}"):
            flask.g.user = {"id": 7, "nombre": "Samantha Blacio", "username": "sam@sphs.cl"}
            env.app.view_functions["pickup_detail"](RID)
    return rt


def retiro_con_medidas(env, **kw):
    env.db.nueva_solicitud(RID, status="retirada", responsable_user_id=7, responsable_nombre="Sam", confirmed_date="2026-10-05", **kw)
    env.db.agregar_doc(RID, "BLV", "0000023732", snapshot=LINEAS_CON_MEDIDAS)
    env.db.reiniciar_registro()


def update_totales(env):
    return [e for e in env.db.escrituras if e[0].startswith("UPDATE `pickup_requests` SET")
            and any(c in e[0] for c in ("peso_real_kg", "peso_vol_kg", "total_volume_m3"))]


class TestTotalesDeProductos:
    def test_los_totales_en_cero_se_completan_con_los_productos(self, env):
        retiro_con_medidas(env, peso_real_kg=0, peso_vol_kg=0, total_volume_m3=0)
        rt = abrir_ficha(env)
        (sql, params), = update_totales(env)
        usado = dict(zip([c.split("=")[0].strip() for c in sql.split("SET ")[1].split(" WHERE")[0].split(",")], params))
        assert usado["peso_real_kg"] == 25.0 and usado["peso_vol_kg"] == 28.0 and usado["total_volume_m3"] == pytest.approx(0.12, abs=1e-6)
        assert rt.call_args.kwargs["req"]["peso_real_kg"] == 25.0           # la ficha ya lo muestra en esta misma carga
        (e,) = [x for x in env.db.logs if x["action"] == "totales_sincronizados"]
        assert e["actor_name"] == "ILUS (automático)" and "0 →" in e["notes"]
        sin_mensajes(env)

    def test_nunca_pisa_una_cubicacion_hecha_por_una_persona(self, env):
        retiro_con_medidas(env, peso_real_kg=50, peso_vol_kg=0, total_volume_m3=0)
        abrir_ficha(env)
        (sql, params), = update_totales(env)
        assert "peso_real_kg" not in sql.split("WHERE")[0]
        assert env.db.solicitudes[RID]["peso_real_kg"] == 50
        sin_mensajes(env)

    def test_si_ya_estan_todos_no_escribe_nada(self, env):
        retiro_con_medidas(env, peso_real_kg=30, peso_vol_kg=33, total_volume_m3=0.2)
        abrir_ficha(env)
        assert update_totales(env) == [] and not [x for x in env.db.logs if x["action"] == "totales_sincronizados"]

    def test_si_los_productos_no_traen_medidas_no_inventa_nada(self, env):
        env.db.nueva_solicitud(RID, status="retirada", responsable_user_id=7, responsable_nombre="Sam", confirmed_date="2026-10-05",
                               peso_real_kg=0, peso_vol_kg=0, total_volume_m3=0)
        env.db.agregar_doc(RID, "BLV", "0000023732")                     # líneas sin peso ni volumen
        env.db.reiniciar_registro()
        abrir_ficha(env)
        assert update_totales(env) == []

    def test_abrir_la_ficha_nunca_manda_nada_al_cliente(self, env):
        retiro_con_medidas(env, peso_real_kg=0, peso_vol_kg=0, total_volume_m3=0)
        abrir_ficha(env)
        A.esperar_hilos_de_aviso()
        sin_mensajes(env)
        assert env.db.solicitudes[RID]["status"] == "retirada"            # y no cambia el estado


# ══════════════════════════════════════════════════════════════════════════════
#  Horario de cobertura
# ══════════════════════════════════════════════════════════════════════════════
def cobertura(env, ahora):
    env.reloj["ahora"] = ahora
    r = env.cli.get("/retiros/api/cobertura")
    assert r.status_code == 200
    return r.get_json()


class TestCobertura:
    @pytest.mark.parametrize("ahora", [dt.datetime(2026, 10, 7, 8, 0), dt.datetime(2026, 10, 7, 12, 59), dt.datetime(2026, 10, 7, 14, 0),
                                       dt.datetime(2026, 10, 7, 16, 59)])
    def test_dentro_del_horario(self, env, ahora):
        d = cobertura(env, ahora)
        assert d["abierta"] is True and d["motivo"] == "" and d["horario"] == "08:00–17:00 (colación 13:00–14:00)"

    @pytest.mark.parametrize("ahora,texto,vuelve", [
        (dt.datetime(2026, 10, 7, 7, 59), "Todavía no empieza", "hoy a las 08:00"),
        (dt.datetime(2026, 10, 7, 13, 0), "colación", "hoy a las 14:00"),
        (dt.datetime(2026, 10, 7, 13, 59), "colación", "hoy a las 14:00"),
        (dt.datetime(2026, 10, 7, 17, 0), "terminó a las 17:00", "mañana a las 08:00"),
        (dt.datetime(2026, 10, 7, 20, 30), "terminó", "mañana a las 08:00"),
        (dt.datetime(2026, 10, 9, 17, 5), "terminó", "el martes 13/10 a las 08:00"),                  # viernes tarde: sáb, dom y lunes 12 (feriado) no hay
        (dt.datetime(2026, 10, 10, 11, 0), "Hoy no hay cobertura", "el martes 13/10 a las 08:00"),       # sábado; el lunes 12 es feriado
        (dt.datetime(2026, 10, 12, 11, 0), "Hoy no hay cobertura", "mañana a las 08:00"),                  # lunes feriado
    ])
    def test_fuera_del_horario(self, env, ahora, texto, vuelve):
        d = cobertura(env, ahora)
        assert d["abierta"] is False and texto in d["motivo"] and d["vuelve"] == vuelve

    def test_el_horario_se_puede_cambiar_por_entorno(self, env, monkeypatch):
        monkeypatch.setenv("RETIROS_COBERTURA_HASTA", "18:30")
        monkeypatch.setenv("RETIROS_COBERTURA_COLACION_DESDE", "12:30")
        assert cobertura(env, dt.datetime(2026, 10, 7, 17, 45))["abierta"] is True
        assert cobertura(env, dt.datetime(2026, 10, 7, 13, 0))["abierta"] is False

    def test_un_valor_malo_del_entorno_vuelve_al_horario_normal(self, env, monkeypatch):
        monkeypatch.setenv("RETIROS_COBERTURA_DESDE", "mañana")
        assert cobertura(env, dt.datetime(2026, 10, 7, 8, 30))["abierta"] is True

    def test_solo_avisa_nunca_bloquea_ni_cambia_nada(self, env):
        """El aviso es de pantalla: el cambio de estado fuera de horario sigue funcionando igual."""
        retiro(env)
        env.reloj["ahora"] = dt.datetime(2026, 10, 7, 17, 30)
        r = env.cli.post(f"/retiros/{RID}/status", data={"status": "en_preparacion"})
        assert r.status_code == 302 and env.db.solicitudes[RID]["status"] == "en_preparacion"

    def test_las_pantallas_avisan_antes_de_gestionar(self):
        raiz = os.path.dirname(_TESTS)
        def leer(*p):
            with open(os.path.join(raiz, *p), encoding="utf-8") as f:
                return f.read()
        assert "retirosAvisoCobertura" in leer("templates", "retiros", "calendario.html")
        assert "retirosAvisoCobertura" in leer("templates", "retiros", "internal_dashboard.html")
        assert "retirosAvisoCobertura" in leer("static", "retiros_internal_detail.js")
        for plantilla in ("calendario.html", "internal_dashboard.html", "internal_detail.html"):
            assert "retiros_cobertura.js" in leer("templates", "retiros", plantilla), plantilla
        js = leer("static", "retiros_cobertura.js")
        assert "/retiros/api/cobertura" in js and "return ''" in js            # sin red o con cobertura: no avisa, nunca bloquea


# ══════════════════════════════════════════════════════════════════════════════
#  Monitor
# ══════════════════════════════════════════════════════════════════════════════
class TestMonitor:
    def _fila(self, **kw):
        base = dict(id=1, code="RET-ABC123", status="retirada", customer_name="Gerd Müller", customer_rut="18.433.872-6", contact_name="Gerd",
                    contact_phone="", contact_email="", document_type="BLV", document_number="23732", pickup_person_name="", pickup_person_rut="",
                    pickup_person_phone="", pickup_person_relation="", requested_date=dt.date(2026, 10, 5), requested_time_from=dt.timedelta(hours=9),
                    requested_time_to=dt.timedelta(hours=9, minutes=30), proposed_date=None, confirmed_date=dt.date(2026, 10, 5), total_packages=1,
                    total_weight_kg=0, total_volumetric_weight=0, total_volume_m3=0.25, peso_real_kg=0, peso_vol_kg=0,
                    information_quality_score=79, request_source="web", responsable_nombre="", doc_validation_status="ok",
                    created_by_user_name=None, created_at=dt.datetime(2026, 10, 5, 12, 41))      # UTC: 09:41 hora Chile
        base.update(kw)
        return base

    def _enriquecer(self, fila):
        import retiros_monitor as rm
        rm.enriquecer_filas(
            [fila], hoy=dt.date(2026, 10, 6), ahora=dt.datetime(2026, 10, 6, 12, 41),
            horas_habiles=lambda d, h, f: (h - d).total_seconds() / 3600.0,
            utc_a_chile=lambda x: (x - dt.timedelta(hours=3)) if x else None,
            td_hhmm=lambda td: f"{int(td.total_seconds()) // 3600:02d}:{(int(td.total_seconds()) % 3600) // 60:02d}",
            estados={"retirada": "Retirada"}, grupos=[{"key": "retirada", "label": "Retirada", "icon": "x", "statuses": ["retirada"]}], relaciones={})
        return fila

    def test_el_monitor_lee_el_peso_real_y_el_volumetrico_del_retiro(self):
        f = self._enriquecer(self._fila(peso_real_kg=25.0, peso_vol_kg=28.0))
        assert f["m_csv"]["Peso kg"] == 25.0 and f["m_csv"]["Peso volumétrico"] == 28.0
        assert f["m_sin_peso"] is False

    def test_si_no_hay_peso_real_cae_al_del_formulario(self):
        f = self._enriquecer(self._fila(peso_real_kg=0, total_weight_kg=7.5))
        assert f["m_csv"]["Peso kg"] == 7.5

    def test_muestra_cuando_se_solicito_con_dia_mes_ano_hora_y_minuto(self):
        f = self._enriquecer(self._fila())
        assert f["m_creado_full"] == "05/10/2026 09:41"
        assert f["m_hace"]                                              # el «hace X» se mantiene

    def test_la_tabla_lo_dibuja(self):
        import jinja2
        raiz = os.path.dirname(_TESTS)
        with open(os.path.join(raiz, "templates", "retiros", "_monitor_tabla.html"), encoding="utf-8") as f:
            html = f.read()
        # 2026-10-07 (fila compacta): la palabra «Solicitada» va en su propia etiqueta (se oculta en pantallas angostas, queda en el title)
        assert '<span class="rm-lbl">Solicitada </span>{{ r.m_creado_full }}' in html and "r.m_hace" in html
        jinja2.Environment().parse(html)
