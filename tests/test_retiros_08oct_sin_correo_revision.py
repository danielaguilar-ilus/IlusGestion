# -*- coding: utf-8 -*-
"""2026-10-08 (Daniel: «ese correo duplicado… quítalo»): pasar un retiro a «En revisión» ya NO le escribe al cliente. Antes reusaba la
plantilla de «Solicitud recibida» y el cliente la recibía dos veces. Arnés: correo de mentira."""
import os
import sys

_TESTS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _TESTS)
sys.path.insert(0, os.path.dirname(_TESTS))

from test_retiros_06oct import RID, env, retiro, sin_mensajes  # noqa: E402,F401


def test_pasar_a_en_revision_no_manda_correo(env):
    retiro(env, status="solicitud_recibida")
    r = env.cli.post(f"/retiros/{RID}/status", data={"status": "en_revision"}, headers={"X-Requested-With": "fetch"})
    assert r.status_code in (200, 302)
    assert env.db.solicitudes[RID]["status"] == "en_revision"
    sin_mensajes(env)


def test_el_kanban_y_la_ficha_ya_no_avisan_correo_en_revision():
    raiz = os.path.dirname(_TESTS)
    for rel in ("static/retiros_cobertura.js", "static/retiros_internal_detail.js"):
        with open(os.path.join(raiz, rel), encoding="utf-8") as f:
            assert "en_revision:" not in f.read(), rel
    with open(os.path.join(raiz, "pickups_module.py"), encoding="utf-8") as f:
        assert '"en_revision":            "created"' not in f.read()
