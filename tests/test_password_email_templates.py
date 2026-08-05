"""Coherencia de los correos de contraseña (recuperar / cambiar / invitar).

POR QUÉ EXISTE ESTE ARCHIVO
Bug real reportado por Daniel el 2026-08-05: pedía "Olvidé mi contraseña",
el correo llegaba, pero -- textual -- "no manda ningún botón, solamente dice
que alguien actualizó la contraseña". Un usuario que olvidó su clave quedaba
encerrado fuera del sistema sin forma de volver a entrar.

Fueron DOS fallas encadenadas, y ninguna la habría detectado un test de HTML:

  1. El flujo self-service mandaba `email_purpose="change"`, o sea la
     plantilla del admin ('cambio_clave'), en vez de la suya
     ('olvido_contrasena').
  2. 'cambio_clave' estaba sembrada DOS VECES con contenido distinto:
     una versión de ACCIÓN (con botón) y una de AVISO ("tu contraseña fue
     actualizada", sin botón ni enlace). Como ambas siembras usan
     INSERT IGNORE, ganaba la que corriera primero -- en producción quedó la
     que no tenía botón.

Estos tests leen el CÓDIGO FUENTE de app.py con `ast` en vez de importarlo
(importar app.py exige BD y credenciales). Así corren en cualquier parte y
no dependen del estado de la base:

    py -m unittest tests.test_password_email_templates -v
"""
import ast
import pathlib
import unittest

APP = pathlib.Path("app.py")

# Plantillas que se mandan SIEMPRE junto a un token de un solo uso. Si el
# cuerpo no trae el enlace, el correo no sirve para nada: el usuario lee que
# algo pasó con su cuenta y no tiene cómo actuar.
PLANTILLAS_DE_ACCION = ("usuario_nuevo", "cambio_clave", "olvido_contrasena")


def _arbol():
    return ast.parse(APP.read_text(encoding="utf-8", errors="ignore"))


def _siembra_lista(arbol, nombre_var):
    """Extrae una lista de tuplas (estado, canal, asunto, cuerpo) asignada a
    `nombre_var` en cualquier parte del archivo."""
    for nodo in ast.walk(arbol):
        if not isinstance(nodo, ast.Assign):
            continue
        for destino in nodo.targets:
            if isinstance(destino, ast.Name) and destino.id == nombre_var:
                return ast.literal_eval(nodo.value)
    return None


def _siembra_dict(arbol, nombre_funcion):
    """Extrae el dict {estado: (asunto, cuerpo)} que devuelve una función."""
    for nodo in ast.walk(arbol):
        if isinstance(nodo, ast.FunctionDef) and nodo.name == nombre_funcion:
            for sub in ast.walk(nodo):
                if isinstance(sub, ast.Return) and isinstance(sub.value, ast.Dict):
                    return ast.literal_eval(sub.value)
    return None


class TestPlantillasLlevanSuEnlace(unittest.TestCase):
    """Toda plantilla de acción tiene que declarar {{link_acceso}}."""

    def test_siembra_de_init_comunicaciones(self):
        filas = _siembra_lista(_arbol(), "_INTERNA_TPL")
        self.assertIsNotNone(filas, "No se encontró _INTERNA_TPL en app.py")
        vistas = set()
        for estado, canal, _asunto, cuerpo in filas:
            if estado not in PLANTILLAS_DE_ACCION:
                continue
            vistas.add(estado)
            self.assertIn(
                "{{link_acceso}}", cuerpo,
                f"La plantilla '{estado}' ({canal}) se manda con un token de "
                f"un solo uso pero su cuerpo no incluye {{{{link_acceso}}}}: "
                f"el usuario recibiría un correo sin forma de actuar.",
            )
        self.assertEqual(set(PLANTILLAS_DE_ACCION), vistas)

    def test_siembra_standalone_para_produccion(self):
        # Esta es la que SÍ corre en producción: init_comunicaciones_tables()
        # se salta con ILUS_SKIP_MIGRATIONS=1.
        tpls = _siembra_dict(_arbol(), "_comunicacion_interna_tpl_seed")
        self.assertIsNotNone(tpls, "No se encontró _comunicacion_interna_tpl_seed")
        for estado in PLANTILLAS_DE_ACCION:
            self.assertIn(estado, tpls)
            _asunto, cuerpo = tpls[estado]
            self.assertIn(
                "{{link_acceso}}", cuerpo,
                f"La plantilla '{estado}' no incluye {{{{link_acceso}}}}.",
            )


class TestLasDosSiembrasNoSeContradicen(unittest.TestCase):
    """Las dos fuentes describen la MISMA plantilla. Si una dice 'hacé click
    para cambiarla' y la otra dice 'ya fue cambiada', el correo que le llega
    al usuario depende de cuál corrió primero -- que fue exactamente el bug."""

    def test_ambas_siembras_coinciden_en_ser_de_accion(self):
        arbol = _arbol()
        lista = _siembra_lista(arbol, "_INTERNA_TPL") or []
        dicc = _siembra_dict(arbol, "_comunicacion_interna_tpl_seed") or {}
        emails = {e: c for e, canal, _a, c in lista if canal == "email"}

        for estado in PLANTILLAS_DE_ACCION:
            with self.subTest(estado=estado):
                self.assertIn(estado, emails)
                self.assertIn(estado, dicc)
                # No se exige texto idéntico (una puede estar mejor redactada),
                # sí que las dos ofrezcan la acción.
                self.assertIn("{{link_acceso}}", emails[estado])
                self.assertIn("{{link_acceso}}", dicc[estado][1])

    def test_ninguna_plantilla_de_accion_habla_en_pasado(self):
        """'Tu contraseña fue actualizada' es un AVISO posterior, no una
        invitación a actuar. Mezclarlos fue lo que dejó a Daniel sin botón."""
        arbol = _arbol()
        lista = _siembra_lista(arbol, "_INTERNA_TPL") or []
        dicc = _siembra_dict(arbol, "_comunicacion_interna_tpl_seed") or {}

        cuerpos = [(e, c) for e, canal, _a, c in lista
                   if e in PLANTILLAS_DE_ACCION and canal == "email"]
        cuerpos += [(e, v[1]) for e, v in dicc.items() if e in PLANTILLAS_DE_ACCION]

        prohibidas = ("fue actualizada exitosamente", "fue actualizada el")
        for estado, cuerpo in cuerpos:
            for frase in prohibidas:
                self.assertNotIn(
                    frase, cuerpo.lower(),
                    f"'{estado}' es un correo de acción pero su texto da por "
                    f"hecho que el cambio YA ocurrió ('{frase}').",
                )


class TestCadaFlujoUsaSuPlantilla(unittest.TestCase):
    def test_forgot_password_no_usa_la_plantilla_del_admin(self):
        """El self-service tiene que mandar email_purpose='forgot'. Si vuelve
        a 'change', el usuario recibe el correo del flujo admin."""
        arbol = _arbol()
        destino = None
        for nodo in ast.walk(arbol):
            if isinstance(nodo, ast.FunctionDef) and nodo.name == "forgot_password":
                destino = nodo
                break
        self.assertIsNotNone(destino, "No se encontró forgot_password en app.py")

        propositos = [
            kw.value.value
            for sub in ast.walk(destino)
            if isinstance(sub, ast.Call)
            for kw in sub.keywords
            if kw.arg == "email_purpose" and isinstance(kw.value, ast.Constant)
        ]
        self.assertEqual(
            ["forgot"], propositos,
            "forgot_password debe llamar a _notify_user_access con "
            "email_purpose='forgot' (la plantilla 'olvido_contrasena'), no "
            "con 'change' (que es la del admin).",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
