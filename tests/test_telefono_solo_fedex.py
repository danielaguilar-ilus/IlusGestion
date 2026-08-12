"""El teléfono obligatorio para asignar a manifiesto bloqueaba a TODOS los
couriers, no solo a FedEx -- pese a que la razón original (Daniel,
2026-07-22) era específicamente que "la llamada [a FedEx] da error" sin él.

BUG REAL EN PRODUCCIÓN (2026-08-12). Captura de Daniel en /asignar: con
Felca seleccionado, "Faltan datos para el courier — Completa los
siguientes campos antes de asignar al manifiesto: • Teléfono". Daniel:
"el teléfono es elemental e indispensable para fedex... si selecciona
felca o milling se pueda avanzar, deja esa restricción solo para fedex ya
que fedex sí o sí lo solicita".

Confirmado con el código real de abajo hacia arriba:
  - simpliroute_client.build_visit_payload trata el teléfono como
    OPCIONAL ("opcional para la API, pero es lo que ve el chofer") --
    Felca/Milling nunca lo exigieron.
  - Solo la llamada a FedEx (fuera de este repo) sí lo exige, que es
    justo lo que motivó la regla original.

DOS LUGARES corregidos, igual que documenta el propio comentario del
frontend ("la validación real y definitiva vive en el backend... esta
función es la primera línea de defensa"):
  1. Frontend (static/cubicador_asignar.js, validarParaManifiesto): el
     teléfono solo se agrega a la lista de campos obligatorios si el
     courier seleccionado (_courierSel.nombre) es FedEx.
  2. Backend (app.py, tr_cubicador_enviar_manifiesto): la validación real
     -- que es la que de verdad protege la base de datos -- ahora exige
     teléfono solo cuando `courier` contiene "fedex".

Correr con:  py -m unittest tests.test_telefono_solo_fedex -v
"""
import ast
import unittest

APP_SRC = open("app.py", encoding="utf-8", errors="ignore").read()
JS_SRC = open("static/cubicador_asignar.js", encoding="utf-8", errors="ignore").read()
_ARBOL = ast.parse(APP_SRC)


def _fn(nombre):
    for n in ast.walk(_ARBOL):
        if isinstance(n, ast.FunctionDef) and n.name == nombre:
            return ast.unparse(n)
    raise AssertionError(f"no se encontro {nombre} en app.py")


class TestBackendSoloExigeTelefonoParaFedex(unittest.TestCase):

    def test_la_exigencia_esta_condicionada_a_fedex(self):
        f = _fn("tr_cubicador_enviar_manifiesto")
        self.assertIn("'fedex' in courier.lower()", f)

    def test_el_error_obligatorio_queda_dentro_de_la_rama_fedex(self):
        """Si el 'if not telefono_in: return 400' quedó FUERA del if fedex,
        el fix no sirvió de nada -- Felca seguiría bloqueado igual."""
        f = _fn("tr_cubicador_enviar_manifiesto")
        i = f.find("'fedex' in courier.lower()")
        self.assertGreater(i, 0)
        bloque = f[i:i + 600]
        self.assertIn("El teléfono de contacto es obligatorio", bloque)
        self.assertIn("if not telefono_in", bloque)

    def test_si_mandan_telefono_igual_sin_ser_fedex_se_valida_formato(self):
        """No exigir el campo no significa aceptar cualquier basura si SÍ
        lo mandan -- se sigue validando formato para no guardar teléfonos
        con formato inválido en la base."""
        f = _fn("tr_cubicador_enviar_manifiesto")
        self.assertIn("elif telefono_in:", f)

    def test_email_y_direccion_siguen_obligatorios_para_todos(self):
        """El fix es SOLO sobre teléfono -- Daniel no pidió tocar email ni
        dirección, y REGLA #4.2 exige no quitar de más."""
        f = _fn("tr_cubicador_enviar_manifiesto")
        self.assertIn("El correo de contacto es obligatorio", f)
        self.assertIn("La dirección es obligatoria", f)


class TestFrontendSoloExigeTelefonoParaFedex(unittest.TestCase):

    def test_el_campo_telefono_se_agrega_condicionalmente(self):
        i = JS_SRC.find("async function validarParaManifiesto")
        self.assertGreater(i, 0)
        bloque = JS_SRC[i:i + 2000]
        self.assertIn("/fedex/i.test(_courierSel", bloque)
        self.assertIn("campos.push({ id:'cli-tel'", bloque)
        # Ya NO debe estar en el arreglo inicial (eso era el bug real).
        i_campos = bloque.find("const campos = [")
        i_push = bloque.find("campos.push")
        bloque_inicial = bloque[i_campos:i_push]
        self.assertNotIn("cli-tel", bloque_inicial)

    def test_email_direccion_comuna_bultos_siguen_siempre_obligatorios(self):
        i = JS_SRC.find("async function validarParaManifiesto")
        bloque = JS_SRC[i:i + 2000]
        i_campos = bloque.find("const campos = [")
        i_push = bloque.find("campos.push")
        bloque_inicial = bloque[i_campos:i_push]
        for campo in ("cli-dir", "cli-bultos", "cli-comuna", "cli-email"):
            self.assertIn(campo, bloque_inicial)


if __name__ == "__main__":
    unittest.main(verbosity=2)
