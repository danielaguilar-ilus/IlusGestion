"""Interfaz de /transporte/manifiestos: botón de buscar, chips de filtros
activos, y los 3 defectos menores que reforzaban "el filtro no funciona".

Pedido de Daniel (2026-09-01): "el filtro y el buscador no está funcionando
como corresponde... poder buscar facturas, clientes y poder filtrar
información."

Un template puede parsear perfecto y reventar recién al renderizar -- ya
pasó en este proyecto con un filtro Jinja inexistente (`|ord`). Por eso acá
se renderiza `transporte/manifiestos.html` DE VERDAD (mismo patrón que
tests/test_transporte_manifiestos_finanzas_ui.py: Environment propio de
Jinja, `app.py` no se levanta -- exige BD y credenciales que este entorno
no tiene).

Ver también tests/test_manifiestos_busqueda_backend.py para la parte de
construcción de la consulta SQL.

Correr:  py -m unittest tests.test_manifiestos_busqueda_ui -v
"""
import ast
import pathlib
import unittest
from urllib.parse import urlencode

from jinja2 import Environment, FileSystemLoader

RAIZ = pathlib.Path(__file__).resolve().parent.parent


class AttrDict(dict):
    __getattr__ = dict.get


def fake_url_for(endpoint, **values):
    if endpoint == "static":
        return f"/static/{values.get('filename', '')}?v=qa"
    if not values:
        return f"/{endpoint}"
    return f"/{endpoint}?{urlencode(values)}"


class ManifiestosBusquedaUiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.env = Environment(loader=FileSystemLoader(str(RAIZ / "templates")),
                              autoescape=True)
        cls._registrar_filtros_de_app()

    @classmethod
    def _registrar_filtros_de_app(cls):
        """Registra como identidad los filtros que app.py define con
        @app.template_filter, leidos del codigo fuente (mismo patron que
        tests/test_transporte_manifiestos_finanzas_ui.py) -- levantar
        app.py exige BD/credenciales que este entorno no tiene, y listarlos
        a mano se rompe cada vez que se agrega un filtro nuevo sin que nada
        este mal en produccion."""
        arbol = ast.parse((RAIZ / "app.py").read_text(encoding="utf-8", errors="ignore"))
        nombres = set()
        for nodo in ast.walk(arbol):
            if not isinstance(nodo, ast.FunctionDef):
                continue
            for deco in nodo.decorator_list:
                if not isinstance(deco, ast.Call):
                    continue
                attr = deco.func
                if not (isinstance(attr, ast.Attribute) and attr.attr == "template_filter"):
                    continue
                if deco.args and isinstance(deco.args[0], ast.Constant):
                    nombres.add(deco.args[0].value)
                else:
                    nombres.add(nodo.name)
        for nombre in nombres:
            cls.env.filters.setdefault(nombre, lambda value, *a, **k: str(value or ""))

    def contexto(self, manifiestos=None, filtros=None, chips=None,
                 url_limpiar="/tr_manifiestos?vista=activos&page_size=50",
                 is_superadmin=True):
        manifiesto = AttrDict(
            id=11, correlativo="MAN-2026-0010", courier="Transportes Milling",
            estado="En curso", fecha="2026-08-05", notas="Nota de prueba",
            total_items=3, costo_total=61500, cien_pct=False,
            avance=AttrDict(entregados=1, total=3, pct=33),
        )
        return {
            "current_user": AttrDict(role="superadmin", nombre="Daniel", foto_url=""),
            "embed_mode": True,
            "permissions": AttrDict(superadmin=is_superadmin, transporte=True),
            "request": AttrDict(endpoint="tr_manifiestos"),
            "session": AttrDict(user_id=1),
            "has_logo": False,
            "is_tecnico": False,
            "last_sync": "",
            "mobile_module": "",
            "role_label": "Superadmin",
            "google_maps_api_key": "",
            "csrf_token": lambda: "csrf-qa",
            "get_flashed_messages": lambda **kwargs: [],
            "url_for": fake_url_for,
            "is_superadmin": is_superadmin,
            "manifiestos": manifiestos if manifiestos is not None else [manifiesto],
            "couriers": ["Transportes Milling", "FedEx"],
            "estados_manifest": ["En preparación", "En curso", "Cerrado",
                                 "Entregado completo"],
            "filtros": AttrDict(filtros if filtros is not None else {
                "courier": "", "estado": "", "q": "", "desde": "", "hasta": "",
                "vista": "activos", "fecha_default_aplicado": False,
            }),
            "kpis": AttrDict(total=1, en_prep=0, bultos=3, costo=61500,
                             margen=18500, margen_pct=23.1),
            "tabs_count": AttrDict(activos=1, entregados=0),
            "chips": chips if chips is not None else [],
            "url_limpiar": url_limpiar,
            "paginacion": AttrDict(page=1, total_pages=1, total=1, page_size=50,
                                   has_prev=False, has_next=False),
        }

    def render(self, **kw):
        return self.env.get_template("transporte/manifiestos.html").render(
            **self.contexto(**kw))

    # ── El caso que env.parse() no detecta: que renderice de verdad ────────
    def test_renderiza_sin_reventar(self):
        html = self.render()
        self.assertIn("Manifiestos de Despacho", html)

    def test_renderiza_con_chips_vacio_sin_reventar(self):
        # chips=[] (el caso mas comun: sin ningun filtro puesto) no debe
        # dibujar la barra de chips ni reventar el {% if chips %}.
        html = self.render(chips=[])
        self.assertNotIn("man-chips-lbl", html)

    # ── El botón "Buscar" (la mitad del reclamo: "escribo y no pasa nada") ──
    def test_existe_el_boton_buscar_dentro_del_form(self):
        html = self.render()
        i_form = html.index('id="filtrosForm"')
        i_end_form = html.index("</form>", i_form)
        bloque_form = html[i_form:i_end_form]
        self.assertIn('type="submit"', bloque_form)
        self.assertIn("man-btn-buscar", bloque_form)

    def test_el_placeholder_es_honesto_sobre_lo_que_se_puede_buscar(self):
        html = self.render()
        i = html.index('name="q"')
        fragmento = html[i:i + 300]
        self.assertIn("factura", fragmento.lower())
        self.assertIn("cliente", fragmento.lower())

    def test_hay_una_pista_de_que_campos_se_pueden_buscar(self):
        html = self.render()
        self.assertIn("man-search-hint", html)
        self.assertIn("tracking", html.lower())

    # ── Chips de filtros activos ─────────────────────────────────────────
    def test_los_chips_se_dibujan_con_su_link_de_quitar(self):
        chips = [
            {"k": "q", "lbl": "Búsqueda", "val": "Cipax", "url": "/tr_manifiestos?courier=x"},
            {"k": "courier", "lbl": "Courier", "val": "FedEx", "url": "/tr_manifiestos?q=Cipax"},
        ]
        html = self.render(chips=chips)
        self.assertIn("man-chips-lbl", html)
        self.assertIn("Cipax", html)
        self.assertIn("FedEx", html)
        self.assertIn("/tr_manifiestos?courier=x", html)
        self.assertIn("man-chips-clear", html)

    def test_el_valor_del_chip_no_se_trunca(self):
        # REGLA #15: nombres/datos siempre completos.
        nombre_largo = "Cliente Con Un Nombre Bastante Largo Sociedad Anonima Ltda"
        chips = [{"k": "q", "lbl": "Búsqueda", "val": nombre_largo, "url": "/x"}]
        html = self.render(chips=chips)
        self.assertIn(nombre_largo, html)

    # ── REGLA #1: nunca alert/confirm/prompt nativos ────────────────────────
    def test_no_usa_popups_nativos(self):
        html = self.render()
        limpio = (html.replace("ilusConfirm(", "").replace("ilusAlert(", "")
                  .replace("ilusPrompt(", ""))
        for nativo in ("confirm(", "alert(", "prompt("):
            self.assertNotIn(" " + nativo, limpio, f"Se coló un {nativo} nativo (REGLA #1)")

    # ── El link "Ver todo el historial" va URL-encodeado ────────────────────
    def test_ver_todo_el_historial_urlencodea_los_filtros(self):
        html = self.render(
            manifiestos=[],
            filtros={"courier": "A&B", "estado": "", "q": "", "desde": "2026-08-01",
                     "hasta": "", "vista": "entregados", "fecha_default_aplicado": True},
        )
        i = html.index("Ver todo el historial")
        bloque = html[max(0, i - 400):i]
        self.assertIn("courier=A%26B", bloque)
        self.assertNotIn("courier=A&B&", bloque)

    # ── Estado vacío contempla el filtro de FECHA ───────────────────────────
    def test_estado_vacio_con_fecha_no_dice_todavia_no_hay_manifiestos(self):
        # Caso real: pestaña "Activos" + rango de fechas sin resultados.
        # Antes caía al mensaje generico de "nunca hubo nada", que se lee
        # como perdida de datos.
        html = self.render(
            manifiestos=[],
            filtros={"courier": "", "estado": "", "q": "", "desde": "2026-01-01",
                     "hasta": "2026-01-31", "vista": "activos",
                     "fecha_default_aplicado": False},
        )
        self.assertIn("Ningún manifiesto coincide con el filtro", html)
        self.assertNotIn("Todavía no hay manifiestos", html)

    def test_estado_vacio_sin_ningun_filtro_sigue_diciendo_todavia_no_hay(self):
        # No se toca el caso genuino de "no hay nada todavia" (REGLA #4.2).
        html = self.render(
            manifiestos=[],
            filtros={"courier": "", "estado": "", "q": "", "desde": "", "hasta": "",
                     "vista": "activos", "fecha_default_aplicado": False},
        )
        self.assertIn("Todavía no hay manifiestos", html)

    # ── "Limpiar filtros" usa la variable calculada por el backend ─────────
    def test_limpiar_filtros_usa_url_limpiar_no_url_for_pelado(self):
        with open(RAIZ / "templates" / "transporte" / "manifiestos.html",
                  encoding="utf-8") as fh:
            src = fh.read()
        # Las 2 apariciones de "Limpiar filtros" (form + estado vacío con
        # filtro) deben usar {{ url_limpiar }} -- que en el backend
        # (app.py) SÍ conserva vista y page_size -- no un url_for('tr_manifiestos')
        # pelado que los resetea (REGLA #4.3 punto 3: el tamaño de página es
        # del usuario, no del código).
        self.assertNotIn("url_for('tr_manifiestos') }}\" class=\"btn-limpiar\"", src)
        self.assertNotIn("url_for('tr_manifiestos') }}\" class=\"trx-btn-primary\"", src)
        # 3 lugares: el form, el chip "Limpiar todo", y el estado vacío
        # filtrado -- los tres deben conservar vista/page_size.
        self.assertEqual(src.count("{{ url_limpiar }}"), 3)

    def test_url_limpiar_se_calcula_con_vista_y_page_size(self):
        with open(RAIZ / "app.py", encoding="utf-8", errors="ignore") as fh:
            app_src = fh.read()
        i = app_src.index('_url_limpiar = url_for("tr_manifiestos"')
        fragmento = app_src[i:i + 150]
        self.assertIn("vista=filtros[\"vista\"]", fragmento)
        self.assertIn("page_size=page_size", fragmento)


if __name__ == "__main__":
    unittest.main(verbosity=2)
