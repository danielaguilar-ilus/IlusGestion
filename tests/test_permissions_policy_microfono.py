"""El header Permissions-Policy no puede prohibir el micrófono: el dictado de
Respuestas (Tickets) mostraba «Activa tu micrófono» aunque el usuario diera el
permiso, porque `microphone=()` se lo negaba a toda la app (2026-10-28)."""
import os
import re

_APP = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "app.py")


def test_permissions_policy_permite_microfono_en_el_propio_origen():
    src = open(_APP, encoding="utf-8").read()
    politicas = re.findall(r'"(geolocation=[^"]*)"', src)
    assert politicas, "no se encontró la Permissions-Policy en app.py"
    for p in politicas:
        assert "microphone=()" not in p
        assert "microphone=(self)" in p
