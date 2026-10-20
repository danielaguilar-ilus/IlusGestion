"""Ayuda para las pruebas que necesitan EJECUTAR una función de app.py sin importar ni parsear el archivo entero
(app.py pesa ~7 MB: en esta máquina un ast.parse completo tarda varios minutos).

Se lee el texto, se recorta la función (o la constante) de nivel superior por su nombre y se compila SOLO ese pedazo,
con los dobles de base de datos / Flask que cada prueba inyecte. Es más rápido que tests/test_incidencias_bajas.py::_cargar
y permite probar funciones con una base de mentira.
"""
import ast
import os
import re

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_CACHE = {}

# Una línea que empieza una sentencia de nivel superior (fin del trozo anterior).
_FIN = re.compile(r"^(?:def |class |@[A-Za-z_]|[A-Za-z_][A-Za-z0-9_]* *(?::[^=\n]*)?= |# ═|# ──|# 🧩|# 💰|# 📒|# ⚖️)", re.M)


def texto(nombre="app.py"):
    if nombre not in _CACHE:
        with open(os.path.join(RAIZ, nombre), encoding="utf-8") as fh:
            _CACHE[nombre] = fh.read()
    return _CACHE[nombre]


def fuente_funcion(nombre, archivo="app.py", con_decoradores=False):
    """El código de la función de nivel superior `nombre`. `con_decoradores`=True antepone las líneas `@...` contiguas
    (solo decoradores de una línea; para saber QUÉ decoradores lleva una ruta usa `lineas_previas`)."""
    t = texto(archivo)
    m = re.search(r"^def %s\(" % re.escape(nombre), t, re.M)
    if not m:
        raise AssertionError("no se encontró la función %s en %s" % (nombre, archivo))
    ini = m.start()
    if con_decoradores:
        # retrocede por los decoradores contiguos
        previo = t[:ini].rstrip("\n").split("\n")
        k = len(previo)
        while k > 0 and previo[k - 1].startswith("@"):
            k -= 1
        ini = sum(len(x) + 1 for x in previo[:k]) if k < len(previo) else ini
    fin_m = _FIN.search(t, m.end())
    fin = fin_m.start() if fin_m else len(t)
    return t[ini:fin]


def decoradores(nombre, archivo="app.py"):
    """Los nombres de los decoradores que están justo ENCIMA de `def nombre(` (líneas `@...` contiguas, incluidas las rutas
    `@app.route(...)`, como texto)."""
    t = texto(archivo)
    m = re.search(r"^def %s\(" % re.escape(nombre), t, re.M)
    if not m:
        raise AssertionError("no se encontró la función %s" % nombre)
    previo = t[:m.start()].rstrip("\n").split("\n")
    out = []
    k = len(previo) - 1
    while k >= 0 and (previo[k].startswith("@") or previo[k].startswith(" ") or previo[k].startswith(")")):
        if previo[k].startswith("@"):
            out.append(previo[k].strip())
        k -= 1
    return list(reversed(out))


def fuente_constante(nombre, archivo="app.py"):
    t = texto(archivo)
    m = re.search(r"^%s *(?::[^=\n]*)?= " % re.escape(nombre), t, re.M)
    if not m:
        raise AssertionError("no se encontró la constante %s en %s" % (nombre, archivo))
    fin_m = _FIN.search(t, m.end())
    return t[m.start():(fin_m.start() if fin_m else len(t))]


def cargar(funciones=(), constantes=(), extra=None, archivo="app.py"):
    """Ejecuta (compila) solo esas funciones y constantes en un ámbito aislado y lo devuelve."""
    amb = dict(extra or {})
    for c in constantes:
        src = fuente_constante(c, archivo)
        exec(compile(ast.parse(src), "<%s:%s>" % (archivo, c), "exec"), amb)
    for f in funciones:
        src = fuente_funcion(f, archivo, con_decoradores=False)
        arbol = ast.parse(src)
        for nodo in arbol.body:
            if isinstance(nodo, ast.FunctionDef):
                nodo.decorator_list = []
        exec(compile(arbol, "<%s:%s>" % (archivo, f), "exec"), amb)
    return amb


def claves_de_diccionario(nombre_funcion, variable, archivo="app.py"):
    """Las claves literales (strings) del dict `variable = { ... }` que arma una función (p. ej. `campos`)."""
    src = fuente_funcion(nombre_funcion, archivo, con_decoradores=False)
    arbol = ast.parse(src)
    claves = None
    for nodo in ast.walk(arbol):
        if isinstance(nodo, ast.Assign) and any(isinstance(t, ast.Name) and t.id == variable for t in nodo.targets) \
                and isinstance(nodo.value, ast.Dict):
            claves = {k.value for k in nodo.value.keys if isinstance(k, ast.Constant) and isinstance(k.value, str)}
    assert claves is not None, "no se encontró %s = {...} en %s" % (variable, nombre_funcion)
    return claves
