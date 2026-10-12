# -*- coding: utf-8 -*-
"""2026-10-12 (Daniel): formulario de la tienda multidocumento («un botón que diga agregar documento con un máximo de 5») y
disponibilidad a 48 horas («así tenemos más capacidad de reacción… ¿se podrá arreglar por el front?»).

  · documentos_extra_del_formulario: documentos 2 a 5 (pura).
  · El documento 1 sigue en el retiro (el formulario viejo sigue funcionando igual); los demás quedan «declarados».
  · Anticipación mínima editable en Horarios y alertas (min_notice_hours, 1–168 h) y expuesta en la disponibilidad pública.

    py -m pytest tests/test_retiros_12oct_multidoc_anticipacion.py -q
"""
import datetime as dt
import os
import sys
from types import SimpleNamespace

import pytest

_TESTS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _TESTS)
sys.path.insert(0, os.path.dirname(_TESTS))

import _arnes_retiros as A  # noqa: E402
import pickups_module as pm  # noqa: E402

RAIZ = os.path.dirname(_TESTS)


# ══════════════════════════════════════════════════════════════════════════════
#  Documentos 2 a 5 (pura)
# ══════════════════════════════════════════════════════════════════════════════
class TestDocumentosExtra:
    def test_el_formulario_viejo_con_un_documento_no_trae_extras(self):
        assert pm.documentos_extra_del_formulario(["factura"], ["1136"], "factura", "1136") == ([], [])

    def test_lee_los_extras_normaliza_y_descarta_vacios_y_repetidos(self):
        extras, err = pm.documentos_extra_del_formulario(
            ["factura", "boleta", "factura", "pedido", "boleta"], ["#1136", "N° 2.211", "1136", "  ", "2211"], "factura", "1136")
        assert err == []
        assert extras == [{"document_type": "boleta", "document_number": "2211", "numero_cliente": "N° 2.211"}]

    def test_hasta_cinco_documentos(self):
        tipos, nums = ["factura"] * 5, ["100", "101", "102", "103", "104"]
        extras, err = pm.documentos_extra_del_formulario(tipos, nums, "factura", "100")
        assert len(extras) == 4 and err == []
        extras, err = pm.documentos_extra_del_formulario(tipos + ["boleta"], nums + ["105"], "factura", "100")
        assert err == ["Puedes agregar hasta 5 documentos por solicitud."]

    def test_tipo_desconocido_o_numero_largo_se_rechaza(self):
        _e, err = pm.documentos_extra_del_formulario(["factura", "cheque"], ["1", "2"], "factura", "1")
        assert err == ["Elige el tipo de cada documento de la lista."]
        _e, err = pm.documentos_extra_del_formulario(["factura", "boleta"], ["1", "1 2 " * 40], "factura", "1")
        assert "demasiado largo" in err[0]

    def test_mismo_numero_con_otro_tipo_si_cuenta(self):
        extras, _ = pm.documentos_extra_del_formulario(["factura", "boleta"], ["1136", "1136"], "factura", "1136")
        assert [(x["document_type"], x["document_number"]) for x in extras] == [("boleta", "1136")]


# ══════════════════════════════════════════════════════════════════════════════
#  Rutas (arnés: sin BD real, sin correo)
# ══════════════════════════════════════════════════════════════════════════════
@pytest.fixture()
def env(monkeypatch):
    monkeypatch.setattr(pm, "_RELOJ_CHILE", lambda: dt.datetime(2026, 10, 13, 10, 0))
    app, db, ctx, esp = A.construir_app()
    return SimpleNamespace(app=app, db=db, ctx=ctx, esp=esp, cli=app.test_client())


class TestAnticipacion:
    def test_la_configuracion_guarda_la_anticipacion_entre_1_y_168(self, env):
        env.cli.post("/retiros/settings", data={"warehouse_name": "Bodega", "min_notice_hours": "48"})
        assert any("set min_notice_hours=%s" in e[0].lower() and e[1] == (48,) for e in env.db.escrituras)
        env.db.reiniciar_registro()
        for malo in ("0", "500", "abc", ""):
            env.cli.post("/retiros/settings", data={"warehouse_name": "Bodega", "min_notice_hours": malo})
        assert not any("min_notice_hours" in e[0].lower() and "update" in e[0].lower() for e in env.db.escrituras)

    def test_sin_el_campo_no_toca_la_anticipacion(self, env):
        env.cli.post("/retiros/settings", data={"warehouse_name": "Bodega"})
        assert not any("min_notice_hours" in e[0].lower() and "update" in e[0].lower() for e in env.db.escrituras)

    @pytest.mark.parametrize("horas", [24, 48])
    def test_la_disponibilidad_publica_respeta_y_cuenta_la_anticipacion(self, env, horas):
        env.db.config = {"min_notice_hours": horas, "work_days": "1,2,3,4,5", "parallel_capacity": 2}
        d = env.cli.get("/retiros/api/disponibilidad-publica").get_json()
        assert d["min_notice_hours"] == horas and d["max_documentos"] == 5
        from zoneinfo import ZoneInfo                      # la disponibilidad usa la hora real de Chile (no el reloj de las pruebas)
        ahora = dt.datetime.now(ZoneInfo("America/Santiago")).replace(tzinfo=None)
        limite = ahora + dt.timedelta(hours=horas, minutes=58)
        for iso, dia in (d.get("dias") or {}).items():
            for s in dia.get("slots") or []:
                if s.get("estado") in ("disponible", "ocupado"):
                    inicio = dt.datetime.fromisoformat(f"{iso}T{s['time_from']}")
                    assert inicio >= limite, (iso, s["time_from"])

    def test_la_pantalla_tiene_el_campo(self):
        html = open(os.path.join(RAIZ, "templates", "retiros", "internal_dashboard.html"), encoding="utf-8").read()
        assert 'name="min_notice_hours"' in html and 'min="1" max="168"' in html


class TestDeclarados:
    def test_la_ficha_y_el_correo_nombran_todos_los_documentos(self):
        src = open(os.path.join(RAIZ, "pickups_module.py"), encoding="utf-8").read()
        assert "docs_declarados=_docs_declarados_txt(req)" in src and "_docs_declarados_extra(req.get(\"id\"))" in src
        html = open(os.path.join(RAIZ, "templates", "retiros", "internal_detail.html"), encoding="utf-8").read()
        assert "docs_declarados | join(' · ')" in html

    def test_el_formulario_publico_usa_la_funcion_y_guarda_los_extras(self):
        src = open(os.path.join(RAIZ, "pickups_module.py"), encoding="utf-8").read()
        i = src.index("def pickup_public_request():")
        cuerpo = src[i:i + 40000]
        assert "documentos_extra_del_formulario(" in cuerpo and "_docs_declarados_guardar(rid, docs_extra)" in cuerpo


# ══════════════════════════════════════════════════════════════════════════════
#  Anticipación en días HÁBILES (Daniel 2026-10-12: «están solicitando reserva el sábado para el lunes: eso no tiene capacidad de
#  reacción… el viernes tiene que tener efecto inmediato en el vencimiento del lunes, no el fin de semana»)
# ══════════════════════════════════════════════════════════════════════════════
def _habil(d, feriados=()):
    return d.isoweekday() <= 5 and d not in feriados


class TestHabiles:
    @pytest.mark.parametrize("ahora, horas, esperado", [
        (dt.datetime(2026, 10, 17, 10, 0), 48, dt.datetime(2026, 10, 21, 0, 0)),     # sábado → cuenta lun+mar → miércoles 00:00
        (dt.datetime(2026, 10, 18, 22, 0), 48, dt.datetime(2026, 10, 21, 0, 0)),     # domingo → igual
        (dt.datetime(2026, 10, 16, 15, 0), 48, dt.datetime(2026, 10, 20, 15, 0)),    # viernes 15:00 → martes 15:00
        (dt.datetime(2026, 10, 16, 15, 0), 24, dt.datetime(2026, 10, 19, 15, 0)),    # viernes 15:00 con 24 h → lunes 15:00 (no el domingo)
        (dt.datetime(2026, 10, 14, 10, 0), 48, dt.datetime(2026, 10, 16, 10, 0)),    # miércoles → viernes, sin fin de semana de por medio
        (dt.datetime(2026, 10, 15, 9, 0), 48, dt.datetime(2026, 10, 19, 9, 0)),      # jueves → lunes (salta sábado y domingo)
    ])
    def test_el_fin_de_semana_no_cuenta(self, ahora, horas, esperado):
        assert pm.inicio_minimo_habil(ahora, horas, _habil) == esperado

    def test_un_feriado_tampoco_cuenta(self):
        feriado = dt.date(2026, 10, 12)                                                   # lunes feriado
        assert pm.inicio_minimo_habil(dt.datetime(2026, 10, 10, 10, 0), 48, lambda d: _habil(d, {feriado})) == \
            dt.datetime(2026, 10, 15, 0, 0)                                                # sábado → mar+mié → jueves 00:00

    def test_con_la_zona_horaria_tambien(self):
        from zoneinfo import ZoneInfo
        ahora = dt.datetime(2026, 10, 16, 15, 0, tzinfo=ZoneInfo("America/Santiago"))
        assert pm.inicio_minimo_habil(ahora, 48, _habil) == dt.datetime(2026, 10, 20, 15, 0)

    def test_el_validador_y_la_disponibilidad_lo_usan(self):
        src = open(os.path.join(RAIZ, "pickups_module.py"), encoding="utf-8").read()
        assert "if start_dt < _inicio_minimo_cliente(now, cfg):" in src
        assert "_min_dt = _inicio_minimo_cliente(_now_cl, cfg) + _td(minutes=_display_buffer_min)" in src
        assert "horas hábiles de anticipación" in src
