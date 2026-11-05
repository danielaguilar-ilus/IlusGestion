# -*- coding: utf-8 -*-
"""CSRF · el token repetido «T, T» (05-nov-2026, Daniel en plena operación: «sigue dando error de token»).

Cuando un mismo encabezado se pone dos veces, el navegador lo junta en «token, token». La subida de fotos de Flota (JS viejo, en una pestaña
que no se había recargado) lo hacía y el servidor rechazaba CADA registro de chofer y CADA devolución de llaves, aunque T fuera el token exacto
de la sesión (en los registros: req = huella de «T, T», ses = huella de T).

El servidor ahora acepta el valor repetido SOLO si todas sus partes son idénticas y la parte es el token exacto de la sesión.
Esto ejecuta las funciones reales de app.py (no solo mira su texto).
"""
import hashlib
import os
import re
import secrets
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
with open(os.path.join(RAIZ, "app.py"), encoding="utf-8", errors="ignore") as _f:
    APP_SRC = _f.read().replace("\r\n", "\n")


def _fuente(nombre):
    """El texto de una función de primer nivel de app.py, SIN analizar todo el archivo (pesa varios MB): desde su `def` hasta la
    siguiente línea que empiece en la columna 0."""
    m = re.search(r"^def " + nombre + r"\(.*?(?=^\S)", APP_SRC + "\nX", re.S | re.M)
    if not m:
        raise AssertionError(f"no se encontró {nombre} en app.py")
    return m.group(0)


def _cargar():
    from flask import Flask, g, jsonify, request, session, redirect, flash
    app = Flask(__name__)
    app.secret_key = "prueba"
    ns = {"request": request, "session": session, "g": g, "jsonify": jsonify, "redirect": redirect, "flash": flash,
          "secrets": secrets, "hashlib": hashlib, "_csrf_is_exempt": lambda p: False, "_is_ajax_request": lambda: True,
          "url_for": lambda *a, **k: "/"}
    for f in ("_csrf_token_de_cabecera", "_csrf_check_request"):
        exec(_fuente(f), ns)
    return app, ns


class TestTokenRepetido(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app, cls.ns = _cargar()
        cls.limpiar = staticmethod(cls.ns["_csrf_token_de_cabecera"])

    T = "abcDEF123_-xyz"

    def revisar(self, cabecera, usuario=True, metodo="POST", token_sesion=None):
        from flask import g, session
        with self.app.test_request_context("/flota/api/choferes", method=metodo, headers=({"X-CSRF-Token": cabecera} if cabecera is not None else {})):
            session["csrf_token"] = self.T if token_sesion is None else token_sesion
            g.user = {"id": 1} if usuario else None
            r = self.ns["_csrf_check_request"]()
            if r is None:
                return None
            resp, status = r if isinstance(r, tuple) else (r, r.status_code)
            return status, resp.get_json()

    # ── la función pura
    def test_un_token_solo_queda_igual(self):
        self.assertEqual(self.limpiar(self.T), self.T)

    def test_el_mismo_token_repetido_queda_en_uno(self):
        for v in (f"{self.T}, {self.T}", f"{self.T},{self.T}", f"{self.T} ,  {self.T}", f"{self.T}, {self.T}, {self.T}"):
            self.assertEqual(self.limpiar(v), self.T, v)

    def test_dos_tokens_distintos_no_se_tocan(self):
        for v in (f"{self.T}, otro", f"otro, {self.T}", f"{self.T}, {self.T}x"):
            self.assertEqual(self.limpiar(v), v, v)

    def test_vacios_y_basura_no_se_vuelven_validos(self):
        self.assertEqual(self.limpiar(None), "")
        self.assertEqual(self.limpiar(""), "")
        for v in (", ", ",", f"{self.T}, ", f", {self.T}", " , "):
            self.assertNotEqual(self.limpiar(v), self.T, repr(v))
        for v in (", ", ",", " , "):
            self.assertEqual(self.limpiar(v), v, repr(v))          # nunca se «arregla» un valor vacío repetido

    # ── el chequeo completo
    def test_el_token_repetido_de_la_sesion_pasa(self):
        self.assertIsNone(self.revisar(f"{self.T}, {self.T}"))

    def test_el_token_normal_sigue_pasando(self):
        self.assertIsNone(self.revisar(self.T))

    def test_un_token_repetido_que_no_es_el_de_la_sesion_se_rechaza(self):
        r = self.revisar("ajeno, ajeno")
        self.assertEqual(r[0], 403)
        self.assertEqual(r[1]["error_codigo"], "CSRF_INVALIDO")

    def test_mezclar_el_bueno_con_otro_se_rechaza(self):
        self.assertEqual(self.revisar(f"{self.T}, ajeno")[0], 403)
        self.assertEqual(self.revisar(f"ajeno, {self.T}")[0], 403)

    def test_sin_cabecera_se_rechaza(self):
        self.assertEqual(self.revisar(None)[0], 403)

    def test_sesion_sin_token_nunca_pasa(self):
        from flask import g, session
        with self.app.test_request_context("/x", method="POST", headers={"X-CSRF-Token": ", "}):
            session.pop("csrf_token", None)
            g.user = {"id": 1}
            r = self.ns["_csrf_check_request"]()
            self.assertEqual(r[1], 403)

    def test_sesion_muerta_sigue_diciendo_sesion_expirada(self):
        r = self.revisar("ajeno, ajeno", usuario=False)
        self.assertEqual((r[0], r[1]["error_codigo"]), (401, "SESSION_EXPIRED"))

    def test_un_get_no_se_revisa(self):
        self.assertIsNone(self.revisar("lo-que-sea", metodo="GET"))


class TestCableado(unittest.TestCase):
    def test_el_chequeo_usa_la_funcion_y_sigue_comparando_en_tiempo_constante(self):
        f = _fuente("_csrf_check_request")
        self.assertIn('_csrf_token_de_cabecera(request.headers.get("X-CSRF-Token"))', f)
        self.assertIn("secrets.compare_digest", f)

    def test_la_funcion_no_registra_el_token_en_claro(self):
        self.assertNotIn("print(", _fuente("_csrf_token_de_cabecera"))


if __name__ == "__main__":
    unittest.main()
