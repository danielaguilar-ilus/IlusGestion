# -*- coding: utf-8 -*-
"""Lectura LIVIANA del código para las pruebas de Control de correos.

`app.py` pesa ~9 MB: `ast.parse` lo convierte en ~15 millones de objetos Python (varios GB) y en una máquina con poca memoria libre tarda
minutos o se queda paginando. Estas pruebas necesitan dos cosas y ninguna exige el árbol completo:

  · `funcion_top` / `constante_top`: el texto de UNA función o constante de nivel módulo (se corta por indentación y se valida con
    `ast.parse` solo ese trozo) → `extraer()` la ejecuta en un ambiente de mentira, como hacen las demás pruebas del proyecto.
  · `llamadas()`: recorre los TOKENS (flujo, memoria constante) y devuelve cada llamada a ciertas funciones con sus argumentos
    nombrados y la función que la contiene. Así se verifica que TODA llamada de envío del proyecto lleve su `correo=`.
"""
import ast
import io
import os
import re
import tokenize

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_TEXTOS = {}
_LLAMADAS = {}


def texto(rel):
    """Texto del archivo (con saltos de línea \\n), cacheado por proceso."""
    if rel not in _TEXTOS:
        with open(os.path.join(RAIZ, rel.replace("/", os.sep)), encoding="utf-8", newline="") as f:
            _TEXTOS[rel] = f.read().replace("\r\n", "\n")
    return _TEXTOS[rel]


def _trozo_valido(lineas, i, patron_fin):
    """Desde la línea i hasta la primera línea candidata a «fin» tal que el trozo se pueda parsear."""
    candidatos = [j for j in range(i + 1, len(lineas) + 1)
                  if j == len(lineas) or (lineas[j] and not lineas[j][0].isspace() and patron_fin.match(lineas[j]))]
    ultimo_error = None
    for j in candidatos[:60]:
        trozo = "\n".join(lineas[i:j])
        try:
            ast.parse(trozo)
            return trozo
        except SyntaxError as exc:
            ultimo_error = exc
    raise AssertionError(f"no se pudo aislar el bloque que empieza en la línea {i + 1}: {ultimo_error}")


_FIN_TOP = re.compile(r"^[^#)\]}\"']")


def funcion_top(rel, nombre):
    """Código de la función de nivel módulo `nombre` (sin decoradores)."""
    lineas = texto(rel).split("\n")
    pat = re.compile(r"^(?:async\s+)?def\s+" + re.escape(nombre) + r"\s*\(")
    for i, l in enumerate(lineas):
        if pat.match(l):
            return _trozo_valido(lineas, i, _FIN_TOP)
    raise AssertionError(f"no se encontró la función {nombre} en {rel}")


def constante_top(rel, nombre):
    """Código de la asignación de nivel módulo `nombre = ...`."""
    lineas = texto(rel).split("\n")
    pat = re.compile(r"^" + re.escape(nombre) + r"\s*(?::[^=\n]+)?=(?!=)")
    for i, l in enumerate(lineas):
        if pat.match(l):
            return _trozo_valido(lineas, i, _FIN_TOP)
    raise AssertionError(f"no se encontró la constante {nombre} en {rel}")


def extraer(amb, rel="app.py", funcs=(), consts=()):
    """Ejecuta en `amb` las funciones y constantes de nivel módulo indicadas."""
    for nombre in consts:
        exec(compile(constante_top(rel, nombre), f"<{rel}:{nombre}>", "exec"), amb)
    for nombre in funcs:
        exec(compile(funcion_top(rel, nombre), f"<{rel}:{nombre}>", "exec"), amb)
    return amb


def valor_literal(rel, nombre):
    """`ast.literal_eval` de una constante de nivel módulo (dicts, tuplas, strings)."""
    nodo = ast.parse(constante_top(rel, nombre)).body[0]
    return ast.literal_eval(nodo.value)


def valor_literal_anidado(rel, nombre, solo_claves=False):
    """Como `valor_literal`, pero para una constante definida DENTRO de una función (un `init_*` con closures): `    NOMBRE = {...}`.
    `solo_claves=True` devuelve las claves de un dict aunque sus valores no sean literales simples."""
    import textwrap
    lineas = texto(rel).split("\n")
    pat = re.compile(r"^(\s+)" + re.escape(nombre) + r"\s*(?::[^=\n]+)?=(?!=)")
    for i, l in enumerate(lineas):
        m = pat.match(l)
        if not m:
            continue
        sangria = len(m.group(1))
        fin = i + 1
        while fin < len(lineas):
            s = lineas[fin]
            if s.strip() and not s.lstrip().startswith("#") and (len(s) - len(s.lstrip())) <= sangria \
                    and not s.lstrip().startswith((")", "]", "}")):
                break
            fin += 1
        nodo = ast.parse(textwrap.dedent("\n".join(lineas[i:fin]))).body[0].value
        if solo_claves:
            return [k.value for k in nodo.keys]
        return ast.literal_eval(nodo)
    raise AssertionError(f"no se encontró la constante anidada {nombre} en {rel}")


def fuente_de(rel, nombre):
    """Código de una función de nivel módulo O DE CUALQUIER NIVEL (closure dentro de un init), por nombre."""
    lineas = texto(rel).split("\n")
    pat = re.compile(r"^(\s*)(?:async\s+)?def\s+" + re.escape(nombre) + r"\s*\(")
    for i, l in enumerate(lineas):
        m = pat.match(l)
        if m:
            sangria = len(m.group(1))
            fin = i + 1
            while fin < len(lineas):
                s = lineas[fin]
                if s.strip() and not s.startswith("#") and (len(s) - len(s.lstrip())) <= sangria \
                        and not s.lstrip().startswith((")", "]", "}", '"', "'")):
                    break
                fin += 1
            import textwrap
            return textwrap.dedent("\n".join(lineas[i:fin]))
    raise AssertionError(f"no se encontró {nombre} en {rel}")


# ───────────────────────────── escáner de llamadas por tokens ─────────────────────────────
def llamadas(rel, nombres):
    """Cada llamada (no definición) a una de las funciones `nombres` en el archivo `rel`.
    Devuelve [{"linea", "nombre", "funcion", "params", "kw": {argumento: texto_del_valor}}].
    `funcion` es la def más interna que contiene la llamada ('<modulo>' si ninguna); `params`, sus parámetros."""
    clave = (rel, tuple(sorted(nombres)))
    if clave in _LLAMADAS:
        return _LLAMADAS[clave]
    nombres = set(nombres)
    resultado = []
    pila_def = []          # [(nivel_de_la_def, nombre, params)]
    nivel = 0
    activas = []           # capturas en curso (una llamada puede contener otra)
    estado_def = None      # None | dict(nivel, fase, nombre, params, prof)
    prev = prev2 = None    # dos últimos tokens significativos (string)
    candidato = None       # (nombre, linea) visto, a la espera del «(»
    for tok in tokenize.generate_tokens(io.StringIO(texto(rel)).readline):
        tp, s = tok.type, tok.string
        if tp == tokenize.INDENT:
            nivel += 1
            continue
        if tp == tokenize.DEDENT:
            nivel -= 1
            while pila_def and nivel <= pila_def[-1][0]:
                pila_def.pop()
            continue
        if tp in (tokenize.NL, tokenize.NEWLINE, tokenize.COMMENT, tokenize.ENCODING, tokenize.ENDMARKER):
            continue

        # ── definición de función: nombre y parámetros ──
        if estado_def is not None:
            d = estado_def
            if d["fase"] == "nombre":
                if tp == tokenize.NAME:
                    d["nombre"], d["fase"] = s, "esperando_paren"
                prev2, prev = prev, s
                continue
            if d["fase"] == "esperando_paren":
                if s == "(":
                    d["fase"], d["prof"] = "params", 1
                prev2, prev = prev, s
                continue
            if d["fase"] == "params":
                if tp == tokenize.OP and s in ("(", "[", "{"):
                    d["prof"] += 1
                elif tp == tokenize.OP and s in (")", "]", "}"):
                    d["prof"] -= 1
                    if d["prof"] == 0:
                        pila_def.append((d["nivel"], d["nombre"], d["params"]))
                        estado_def = None
                elif tp == tokenize.NAME and d["prof"] == 1 and prev in ("(", ",", "*", "**", "/"):
                    d["params"].add(s)
                prev2, prev = prev, s
                continue

        if tp == tokenize.NAME and s == "def":
            estado_def = {"nivel": nivel, "fase": "nombre", "nombre": None, "params": set(), "prof": 0}
            prev2, prev = prev, s
            continue

        # ── capturas de llamadas en curso ──
        cerradas = []
        for cap in activas:
            if tp == tokenize.OP and s in ("(", "[", "{"):
                cap["prof"] += 1
            elif tp == tokenize.OP and s in (")", "]", "}"):
                cap["prof"] -= 1
                if cap["prof"] == 0:
                    _cerrar_arg(cap)
                    cerradas.append(cap)
                    continue
            elif tp == tokenize.OP and s == "," and cap["prof"] == 1:
                _cerrar_arg(cap)
                continue
            cap["arg"].append((tp, s))
        for cap in cerradas:
            activas.remove(cap)
            resultado.append({"linea": cap["linea"], "nombre": cap["nombre"], "funcion": cap["funcion"],
                              "params": cap["params"], "kw": cap["kw"]})

        # ── inicio de una llamada a un nombre vigilado ──
        if candidato is not None:
            if tp == tokenize.OP and s == "(" and prev2 != "def":
                nombre, linea = candidato
                f = pila_def[-1] if pila_def else (None, "<modulo>", set())
                activas.append({"nombre": nombre, "linea": linea, "funcion": f[1], "params": f[2], "prof": 1, "arg": [],
                                "kw": {}})
            candidato = None
        if tp == tokenize.NAME and s in nombres:
            candidato = (s, tok.start[0])
        prev2, prev = prev, s
    _LLAMADAS[clave] = resultado
    return resultado


def _cerrar_arg(cap):
    arg = cap["arg"]
    cap["arg"] = []
    if len(arg) >= 3 and arg[0][0] == tokenize.NAME and arg[1] == (tokenize.OP, "="):
        cap["kw"][arg[0][1]] = " ".join(s for _, s in arg[2:])
    elif len(arg) >= 2 and arg[0] == (tokenize.OP, "**"):
        cap["kw"]["**"] = " ".join(s for _, s in arg[1:])        # `**dict`: los argumentos nombrados no se ven en el sitio
