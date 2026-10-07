# -*- coding: utf-8 -*-
"""2026-10-07 — Encuesta de satisfacción: enganche listo pero APAGADO (Daniel: «no generes ninguna notificación a ningún cliente hasta que te autorice»). El enlace va SOLO en el correo «retiro completado» y en el
seguimiento del retiro ya retirado; con la encuesta apagada no aparece nada. Arnés: correo de mentira, sin BD real.

    py -m pytest tests/test_retiros_07oct_encuesta_lanzada.py -q
"""
import os
import sys
from unittest.mock import MagicMock

import pytest

_TESTS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _TESTS)
sys.path.insert(0, os.path.dirname(_TESTS))

from test_retiros_07oct_expedicion import CLIENTE, RID, correos_al_cliente, env, retiro  # noqa: E402,F401

RUTA = "/retiros/encuesta/1-0123456789abcdef01234567"


def con_encuesta(env, activa=True):
    inv = MagicMock(name="invitar", return_value=({"ruta": RUTA, "token": "x", "vence": None} if activa else None))
    marcar = MagicMock(name="marcar_enviada")
    env.app.extensions["retiros_encuesta"] = {"invitar": inv, "marcar_enviada": marcar}
    return inv, marcar


def notificar(env, kind):
    req = dict(env.db.solicitudes[RID])
    env.ctx["_pickup_notify"](req, kind)


def test_el_correo_de_retiro_completado_lleva_la_encuesta(env):
    retiro(env, status="retirada", contact_email=CLIENTE)
    inv, marcar = con_encuesta(env)
    notificar(env, "done")
    (c,) = correos_al_cliente(env)
    cuerpo = c.args[2]
    assert "Responder la encuesta" in cuerpo and RUTA in cuerpo
    assert cuerpo.index(RUTA) < cuerpo.index("Ver mi retiro"), "va antes del botón de seguimiento"
    inv.assert_called_once_with(RID)
    marcar.assert_called_once_with(RID)


def test_otros_correos_no_llevan_la_encuesta(env):
    retiro(env, status="en_preparacion", contact_email=CLIENTE)
    inv, marcar = con_encuesta(env)
    notificar(env, "preparing")
    for c in correos_al_cliente(env):
        assert "encuesta" not in c.args[2].lower()
    inv.assert_not_called()
    marcar.assert_not_called()


def test_con_la_encuesta_apagada_no_aparece_nada(env):
    retiro(env, status="retirada", contact_email=CLIENTE)
    inv, marcar = con_encuesta(env, activa=False)
    notificar(env, "done")
    (c,) = correos_al_cliente(env)
    assert "Responder la encuesta" not in c.args[2]
    marcar.assert_not_called()


def test_si_la_encuesta_falla_el_correo_igual_sale(env):
    retiro(env, status="retirada", contact_email=CLIENTE)
    env.app.extensions["retiros_encuesta"] = {"invitar": MagicMock(side_effect=RuntimeError("bd caída")), "marcar_enviada": MagicMock()}
    notificar(env, "done")
    (c,) = correos_al_cliente(env)
    assert "Responder la encuesta" not in c.args[2]


def test_el_seguimiento_publico_ofrece_la_encuesta_solo_retirado():
    raiz = os.path.dirname(_TESTS)
    with open(os.path.join(raiz, "templates", "retiros", "public_tracking.html"), encoding="utf-8") as f:
        html = f.read()
    assert "{% if encuesta_url %}" in html and "Responder la encuesta" in html
    with open(os.path.join(raiz, "pickups_module.py"), encoding="utf-8") as f:
        pm = f.read()
    assert "encuesta_url=encuesta_url" in pm


@pytest.mark.parametrize("ruta,exenta", [
    ("/retiros/encuesta/1-0123456789abcdef01234567", True),
    ("/retiros/encuesta/preguntas/guardar", False),
    ("/retiros/encuesta/preguntas/5/activa", False),
    ("/retiros/encuesta/1-0123456789abcdef01234567/x", False),
])
def test_solo_la_pagina_publica_de_la_encuesta_queda_sin_csrf(ruta, exenta):
    import re
    raiz = os.path.dirname(_TESTS)
    with open(os.path.join(raiz, "app.py"), encoding="utf-8") as f:
        src = f.read()
    patron = re.search(r're\.fullmatch\(r"(/retiros/encuesta/[^"]+)"', src).group(1)
    assert bool(re.fullmatch(patron, ruta)) is exenta


def test_el_deploy_no_enciende_nada_sin_autorizacion_de_daniel():
    # Daniel 2026-10-07: «no generes ninguna notificación a ningún cliente hasta que te autorice, sobre todo con la encuesta».
    # La encuesta sigue apagada (RETIROS_ENCUESTA_ACTIVA) y la expedición en modo sombra (RETIROS_RETIRO_AUTO) por defecto.
    raiz = os.path.dirname(_TESTS)
    with open(os.path.join(raiz, ".github", "workflows", "deploy.yml"), encoding="utf-8") as f:
        y = f.read()
    assert "RETIROS_ENCUESTA_ACTIVA" not in y and "RETIROS_RETIRO_AUTO" not in y
