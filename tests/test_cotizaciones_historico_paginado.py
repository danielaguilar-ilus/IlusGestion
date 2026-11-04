# -*- coding: utf-8 -*-
"""📄 5-nov-2026 — Histórico de cotizaciones (Triple A): paginado (REGLA #4.3), nombres completos (#15), letra >= 12 px y sin scroll de lado (#61).

Medido EN PRODUCCIÓN antes de tocarlo: las 177 filas juntas (7.800 px de alto en escritorio, 42.000 px en celular), 205 px de scroll de lado a 1280 px,
49 nombres de cliente cortados con «…», letra de 9,9 a 11,8 px, estados en inglés («sent») y «Válida hasta» mostrada UN DÍA ANTES y a las 20:00
(el CSV la trae «06-08-2026 00:00» y la pantalla decía «05/08/2026 20:00»: se la pasaba de UTC a hora Chile siendo una fecha).

Se prueba (sin Flask ni base de datos): la ruta (se extrae de tickets_module.py y se corre con una base de mentira), la plantilla (se renderiza con
Jinja y filas de mentira) y que el CSS no vuelva a bajar de 12 px ni a cortar nombres.
Correr:  python -m pytest tests/test_cotizaciones_historico_paginado.py -q -p no:warnings
"""
import ast
import functools
import os
import re
import unittest
from datetime import date, datetime, timezone
from urllib.parse import parse_qs, urlencode, urlparse

import jinja2

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MODULO = os.path.join(RAIZ, "tickets_module.py")
PLANTILLA = os.path.join(RAIZ, "templates", "tickets", "cotizaciones_historico.html")


@functools.lru_cache(maxsize=None)
def _analizado():
    with open(MODULO, encoding="utf-8") as fh:
        fuente = fh.read()
    return fuente, ast.parse(fuente)


def _funcion(nombre):
    """Texto de una función anidada de tickets_module.py (viven dentro de register_tickets_routes), sin sangría."""
    fuente, arbol = _analizado()
    for nodo in ast.walk(arbol):
        if isinstance(nodo, ast.FunctionDef) and nodo.name == nombre:
            return re.sub(r"^ {4}", "", ast.get_source_segment(fuente, nodo), flags=re.M)
    raise AssertionError("no existe " + nombre)


class _Peticion:
    def __init__(self, args=None):
        self.args = args or {}


def _ruta(args, total, filas):
    """Corre tk_cotizaciones_historico con una base de mentira. Devuelve (contexto del render, [(sql, params)])."""
    consultas, ctx_render = [], {}

    def fetchone(sql, params=()):
        consultas.append((sql, tuple(params)))
        return {"n": total}

    def fetchall(sql, params=()):
        consultas.append((sql, tuple(params)))
        return list(filas)

    def render_template(nombre, **kw):
        ctx_render.update(kw, _plantilla=nombre)
        return "ok"

    ns = {"request": _Peticion(args), "mysql_fetchone": fetchone, "mysql_fetchall": fetchall, "render_template": render_template,
          "_fmt_row": lambda r, dt_keys=(), date_keys=(): dict(r)}
    exec(compile(_funcion("_hist_fecha_sin_zona"), "<fecha>", "exec"), ns)
    exec(compile(_funcion("tk_cotizaciones_historico"), "<historico>", "exec"), ns)
    ns["tk_cotizaciones_historico"]()
    return ctx_render, consultas


def _fila(i, **extra):
    f = {"id": i, "numero": "COT-%06d" % i, "estado": "Borrador", "cliente": "Cliente %d" % i, "rut": "76.123.456-7", "subtotal": 100000,
         "descuento": 0, "impuesto": 19000, "total": 119000, "valida_hasta": datetime(2026, 8, 6, 0, 0), "creada_csv": "07/07/2026 11:00"}
    f.update(extra)
    return f


class TestRutaPaginada(unittest.TestCase):
    def test_primera_pagina_por_defecto_25(self):
        ctx, q = _ruta({}, 177, [_fila(1)])
        self.assertEqual((ctx["page"], ctx["per_page"], ctx["total"], ctx["paginas"]), (1, 25, 177, 8))
        sql, params = q[-1]
        self.assertIn("LIMIT %s OFFSET %s", sql)
        self.assertNotIn("LIMIT 300", sql, "el tope mudo de 300 filas ya no existe")
        self.assertEqual(params[-2:], (25, 0))

    def test_la_pagina_3_de_50_salta_100_filas(self):
        ctx, q = _ruta({"page": "3", "per_page": "50"}, 177, [_fila(1)])
        self.assertEqual((ctx["page"], ctx["per_page"], ctx["paginas"]), (3, 50, 4))
        self.assertEqual(q[-1][1][-2:], (50, 100))

    def test_una_pagina_de_mas_vuelve_a_la_ultima(self):
        ctx, q = _ruta({"page": "99"}, 177, [_fila(1)])
        self.assertEqual((ctx["page"], ctx["paginas"]), (8, 8))
        self.assertEqual(q[-1][1][-2:], (25, 175))

    def test_valores_raros_vuelven_a_los_valores_buenos(self):
        for args in ({"per_page": "7"}, {"per_page": "abc"}, {"per_page": "100000"}, {"per_page": "-1"}):
            ctx, _ = _ruta(args, 177, [_fila(1)])
            self.assertEqual(ctx["per_page"], 25, args)
        ctx, _ = _ruta({"page": "xyz"}, 177, [_fila(1)])
        self.assertEqual(ctx["page"], 1)
        ctx, _ = _ruta({"page": "-4"}, 177, [_fila(1)])
        self.assertEqual(ctx["page"], 1)

    def test_las_cuatro_opciones_de_n_por_pagina_se_respetan(self):
        for n in (10, 25, 50, 100):
            ctx, q = _ruta({"per_page": str(n)}, 177, [_fila(1)])
            self.assertEqual(ctx["per_page"], n)
            self.assertEqual(q[-1][1][-2:], (n, 0))

    def test_el_filtro_va_en_el_servidor_y_en_el_conteo(self):
        ctx, q = _ruta({"q": "  fit  "}, 12, [_fila(1)])
        self.assertEqual(ctx["filtro_q"], "fit")
        (sql_n, par_n), (sql_f, par_f) = q[0], q[1]
        self.assertIn("COUNT(*)", sql_n)
        self.assertEqual(par_n, ("%fit%",) * 3, "el total se cuenta con el MISMO filtro")
        self.assertEqual(par_f, ("%fit%",) * 3 + (25, 0))
        self.assertEqual((ctx["total"], ctx["paginas"]), (12, 1))

    def test_el_texto_buscado_se_acota(self):
        ctx, _ = _ruta({"q": "x" * 500}, 0, [])
        self.assertEqual(len(ctx["filtro_q"]), 60)

    def test_sin_resultados(self):
        ctx, _ = _ruta({"q": "zzz"}, 0, [])
        self.assertEqual((ctx["total"], ctx["page"], ctx["paginas"], ctx["filas"]), (0, 1, 1, []))

    def test_valida_hasta_es_una_fecha_sin_cambio_de_zona(self):
        """El CSV trae «06-08-2026 00:00»: es el 6 de agosto. Pasarla de UTC a hora Chile daba «05/08/2026 20:00»."""
        ctx, _ = _ruta({}, 1, [_fila(1, valida_hasta=datetime(2026, 8, 6, 0, 0))])
        self.assertEqual(ctx["filas"][0]["valida_hasta"], "06/08/2026")
        ctx, _ = _ruta({}, 1, [_fila(1, valida_hasta=None)])
        self.assertIsNone(ctx["filas"][0]["valida_hasta"])
        ctx, _ = _ruta({}, 1, [_fila(1, valida_hasta=date(2026, 12, 31))])
        self.assertEqual(ctx["filas"][0]["valida_hasta"], "31/12/2026")

    def test_el_resto_de_las_columnas_sigue_igual(self):
        ctx, _ = _ruta({}, 1, [_fila(7)])
        self.assertEqual(ctx["_plantilla"], "tickets/cotizaciones_historico.html")
        self.assertEqual(ctx["filas"][0]["numero"], "COT-000007")

    def test_el_codigo_ya_no_convierte_valida_hasta_a_hora_chile(self):
        src = _funcion("tk_cotizaciones_historico")
        self.assertIn('dt_keys=("creada_csv",), date_keys=()', src, "solo la fecha de creación (un instante) pasa a hora Chile")
        self.assertNotIn('dt_keys=("valida_hasta", "creada_csv")', src)

    def test_sigue_cerrada_a_los_tecnicos(self):
        fuente, arbol = _analizado()
        for nodo in ast.walk(arbol):
            if isinstance(nodo, ast.FunctionDef) and nodo.name == "tk_cotizaciones_historico":
                deco = " ".join(ast.get_source_segment(fuente, d) for d in nodo.decorator_list)
                self.assertIn("_tickets_required", deco)
                self.assertIn("_tk_sin_cotizaciones_tecnico", deco, "REGLA #26: la familia técnico no entra (viene de #234)")
                return
        self.fail("no se encontró la ruta")


def _entorno():
    """Jinja con una base mínima, `url_for` que arma la URL como Flask (con los filtros bien codificados) y el filtro real de estados."""
    from cotiz_kpis import cotiz_estado_meta

    def url_for(endpoint, **kw):
        if endpoint == "static":
            return "/static/" + kw["filename"]
        base = {"tk_cotizaciones_list": "/tickets/cotizaciones", "tk_cotizaciones_historico": "/tickets/cotizaciones/historico"}[endpoint]
        pares = [(k, v) for k, v in kw.items() if v is not None]
        return base + ("?" + urlencode(pares) if pares else "")

    base = ("{% block title %}{% endblock %}{% block head_extra %}{% endblock %}{% block content %}{% endblock %}")
    with open(PLANTILLA, encoding="utf-8") as fh:
        cuerpo = fh.read()
    env = jinja2.Environment(loader=jinja2.DictLoader({"base.html": base, "h.html": cuerpo}), autoescape=True)
    env.globals["url_for"] = url_for
    env.filters["cotiz_estado"] = lambda e: cotiz_estado_meta(e)[0]
    return env


def _render(**kw):
    ctx = dict(filas=[], filtro_q="", page=1, per_page=25, total=0, paginas=1)
    ctx.update(kw)
    return _entorno().get_template("h.html").render(**ctx)


class TestPlantilla(unittest.TestCase):
    def test_el_pie_dice_a_b_de_n_y_pagina_x_de_y(self):
        h = _render(filas=[_fila(1)], page=2, per_page=25, total=177, paginas=8)
        self.assertIn("Mostrando <b>26–50</b> de <b>177</b>", h)
        self.assertIn("Página 2 de 8", h)
        self.assertIn(">Anterior</a>", h)
        self.assertIn(">Siguiente</a>", h)

    def test_la_ultima_pagina_muestra_hasta_el_total(self):
        h = _render(filas=[_fila(1)], page=8, per_page=25, total=177, paginas=8)
        self.assertIn("Mostrando <b>176–177</b> de <b>177</b>", h)

    def test_anterior_y_siguiente_apuntan_a_la_pagina_correcta(self):
        h = _render(filas=[_fila(1)], page=3, per_page=50, total=177, paginas=4, filtro_q="")
        ant = re.search(r'<a class="[^"]*"[^>]*href="([^"]+)">Anterior', h).group(1)
        sig = re.search(r'<a class="[^"]*"[^>]*href="([^"]+)">Siguiente', h).group(1)
        self.assertEqual(parse_qs(urlparse(ant.replace("&amp;", "&")).query), {"per_page": ["50"], "page": ["2"]})
        self.assertEqual(parse_qs(urlparse(sig.replace("&amp;", "&")).query), {"per_page": ["50"], "page": ["4"]})

    def test_los_bordes_deshabilitan_el_boton(self):
        h = _render(filas=[_fila(1)], page=1, per_page=25, total=177, paginas=8)
        self.assertRegex(h, r'<a class="dis" aria-disabled="true" tabindex="-1"\s+href="#">Anterior</a>')
        self.assertNotRegex(h, r'class="dis"[^>]*\s+href="[^#][^"]*">Siguiente')
        h = _render(filas=[_fila(1)], page=8, per_page=25, total=177, paginas=8)
        self.assertRegex(h, r'<a class="dis" aria-disabled="true" tabindex="-1"\s+href="#">Siguiente</a>')

    def test_el_filtro_con_tildes_viaja_codificado_en_los_enlaces(self):
        """REGLA #4.3: el texto libre (con acentos, espacios, &) se codifica en la URL de la paginación."""
        h = _render(filas=[_fila(1)], page=1, per_page=25, total=90, paginas=4, filtro_q="café & té")
        sig = re.search(r'href="([^"]+)">Siguiente', h).group(1).replace("&amp;", "&")
        self.assertEqual(parse_qs(urlparse(sig).query)["q"], ["café & té"])
        self.assertIn("q=caf%C3%A9+%26+t%C3%A9", sig)

    def test_el_selector_conserva_el_filtro_y_marca_el_tamano(self):
        h = _render(filas=[_fila(1)], page=1, per_page=50, total=177, paginas=4, filtro_q="fit")
        for n in (10, 25, 50, 100):
            self.assertIn('<option value="%d"' % n, h)
        self.assertRegex(h, r'<option value="50" selected>50 por página</option>')
        self.assertRegex(h, r'<input type="hidden" name="q" value="fit">')
        self.assertRegex(h, r'<input type="hidden" name="per_page" value="50">', "el buscador conserva el tamaño elegido")

    def test_quitar_el_filtro_recarga_la_tabla_completa(self):
        """REGLA #4.3 punto 4: al limpiar el filtro la tabla se recarga (el enlace va a la lista sin filtro)."""
        h = _render(filas=[_fila(1)], filtro_q="fit", total=3, paginas=1)
        self.assertRegex(h, r'<a href="/tickets/cotizaciones/historico" class="btn btn-outline-secondary"')

    def test_sin_resultados_lo_dice(self):
        h = _render(filas=[], total=0, paginas=1, filtro_q="zzz")
        cuerpo = h[h.index("</style>"):]   # el comentario del CSS también dice «Mostrando A–B de N»
        self.assertIn("Sin resultados", cuerpo)
        self.assertNotIn("Mostrando", cuerpo)

    def test_el_nombre_del_cliente_va_completo(self):
        nombre = "SERVICIOS GASTRONOMICOS ESTEBAN FELIPE SANTANDER GUTIERREZ SPA"
        h = _render(filas=[_fila(1, cliente=nombre)], total=1)
        self.assertIn(nombre, h)
        self.assertNotIn("text-overflow", h, "REGLA #15: nunca «…»")
        self.assertNotIn("max-width:280px", h)

    def test_el_estado_se_lee_en_espanol(self):
        h = _render(filas=[_fila(1, estado="sent"), _fila(2, estado="approved"), _fila(3, estado="rejected"), _fila(4, estado="Borrador"),
                           _fila(5, estado=None)], total=5)
        for texto in ("Enviada", "Aprobada", "Rechazada", "Borrador"):
            self.assertRegex(h, r"<span class=\"hist-badge [^\"]+\">\s*%s\s*</span>" % texto)
        self.assertNotRegex(h, r">\s*(sent|approved|rejected)\s*<", "nada visible va en inglés")

    def test_la_fecha_de_creacion_va_con_su_hora_aparte(self):
        h = _render(filas=[_fila(1, creada_csv="07/07/2026 11:00")], total=1)
        self.assertRegex(h, r'data-label="Creada">07/07/2026 <span class="hist-hora">11:00</span>')

    def test_la_fecha_de_validez_es_la_del_servidor(self):
        h = _render(filas=[_fila(1, valida_hasta="06/08/2026")], total=1)
        self.assertRegex(h, r'data-label="Válida hasta">06/08/2026</td>')
        self.assertNotIn("20:00", h)

    def test_los_datos_faltantes_se_ven_como_raya(self):
        h = _render(filas=[_fila(1, rut=None, valida_hasta=None, creada_csv=None, cliente=None)], total=1)
        self.assertGreaterEqual(h.count("—"), 4)


class TestEstilos(unittest.TestCase):
    @staticmethod
    def _css():
        with open(PLANTILLA, encoding="utf-8") as fh:
            t = fh.read()
        return t[t.index("<style>"):t.index("</style>")]

    def test_ninguna_letra_baja_de_12_px(self):
        malos = []
        for m in re.finditer(r"font-size:\s*([0-9]*\.?[0-9]+)\s*(rem|em|px)", self._css()):
            v, u = float(m.group(1)), m.group(2)
            if (v * 16 if u in ("rem", "em") else v) < 12:
                malos.append(m.group(0))
        self.assertEqual(malos, [], "REGLA #61: letra de menos de 12 px en el Histórico")

    def test_sin_grises_ilegibles(self):
        self.assertNotIn("#9ca3af", self._css(), "2,5:1 sobre blanco: usa #6b7280")

    def test_la_tabla_pasa_a_tarjetas_bajo_1240_px(self):
        css = self._css()
        self.assertIn("@media (max-width:1239.98px){", css)
        self.assertIn(".hist-table thead{display:none}", css)
        self.assertIn(".hist-table-wrap{border:0;background:transparent;overflow:visible}", css, "sin scroll de lado en las tarjetas")

    def test_los_datos_van_con_etiqueta_en_las_tarjetas(self):
        css = self._css()
        self.assertIn('content:attr(data-label) " "', css)
        with open(PLANTILLA, encoding="utf-8") as fh:
            t = fh.read()
        for etiqueta in ("Subtotal", "Impuesto", "Válida hasta", "Creada"):
            self.assertIn('data-label="%s"' % etiqueta, t)


if __name__ == "__main__":
    unittest.main()
