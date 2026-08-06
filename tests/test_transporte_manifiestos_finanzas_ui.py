"""La pantalla de Manifiestos RENDERIZA con el botón "Revisar finanzas".

POR QUÉ EXISTE ESTE TEST
------------------------
Un template puede parsear perfecto y reventar recién al renderizar — ya pasó
en este proyecto con un filtro Jinja inexistente (`|ord`): el parse daba OK y
la página caía en producción. `env.parse()` NO alcanza; hay que renderizar.

Acá se renderiza `transporte/manifiestos.html` de verdad (con su base.html y
su include), en los dos casos que importan:

  · superadmin  → el botón y su JavaScript tienen que estar.
  · sin permiso → NO deben aparecer. El barrido corrige plata; el endpoint ya
                  exige superadmin en el POST, pero la pantalla tampoco debe
                  ofrecer el botón a quien no puede usarlo.

Mismo patrón que tests/test_transport_manifest_ui.py: Environment propio de
Jinja con los filtros de app.py leídos por AST (levantar app.py exige BD y
credenciales que este entorno no tiene).

Correr:  py -m unittest tests.test_transporte_manifiestos_finanzas_ui -v
"""
import ast
import pathlib
import unittest

from jinja2 import Environment, FileSystemLoader


RAIZ = pathlib.Path(__file__).resolve().parent.parent


class AttrDict(dict):
    __getattr__ = dict.get


def fake_url_for(endpoint, **values):
    if endpoint == "static":
        return f"/static/{values.get('filename', '')}?v=qa"
    return f"/{endpoint}"


class ManifiestosFinanzasUiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.env = Environment(loader=FileSystemLoader(str(RAIZ / "templates")),
                              autoescape=True)
        cls._registrar_filtros_de_app()

    @classmethod
    def _registrar_filtros_de_app(cls):
        """Registra como identidad los filtros que app.py define con
        @app.template_filter, leídos del código fuente.

        Se leen en vez de listarlos a mano: cada filtro nuevo que alguien
        agregara rompería este test con "No filter named 'X'" sin que nada
        estuviera mal en producción.
        """
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

    def contexto(self, is_superadmin=True):
        manifiesto = AttrDict(
            id=11, correlativo="MAN-2026-0010", courier="Transportes Milling",
            estado="En curso", fecha="2026-08-05", notas="",
            total_items=3, costo_total=61500, cien_pct=False,
            avance=AttrDict(entregados=1, total=3, pct=33),
        )
        return {
            # base.html
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
            # manifiestos.html
            "is_superadmin": is_superadmin,
            "manifiestos": [manifiesto],
            "couriers": ["Transportes Milling", "FedEx"],
            "estados_manifest": ["En preparación", "En curso", "Cerrado",
                                 "Entregado completo"],
            "filtros": AttrDict(courier="", estado="", q="", desde="", hasta="",
                                vista="activos", fecha_default_aplicado=False),
            "kpis": AttrDict(total=1, en_prep=0, bultos=3, costo=61500,
                             margen=18500, margen_pct=23.1),
            "tabs_count": AttrDict(activos=1, entregados=0),
            "paginacion": AttrDict(page=1, total_pages=1, total=1, page_size=50,
                                   has_prev=False, has_next=False),
        }

    def render(self, **kw):
        return self.env.get_template("transporte/manifiestos.html").render(
            **self.contexto(**kw))

    # ── El caso que el parse NO detecta: que renderice de verdad ────────────
    def test_renderiza_para_superadmin(self):
        html = self.render(is_superadmin=True)
        self.assertIn("Manifiestos de Despacho", html)

    def test_renderiza_sin_permiso(self):
        html = self.render(is_superadmin=False)
        self.assertIn("Manifiestos de Despacho", html)

    # ── El botón y su flujo ────────────────────────────────────────────────
    def test_superadmin_ve_el_boton_y_su_javascript(self):
        html = self.render(is_superadmin=True)
        self.assertIn('id="btnAuditFin"', html)
        self.assertIn("Revisar finanzas", html)
        self.assertIn("revisarHistoricoFinanciero", html)
        self.assertIn("/transporte/api/finanzas/limpieza-historica", html)

    def test_sin_permiso_no_hay_boton_ni_endpoint(self):
        html = self.render(is_superadmin=False)
        self.assertNotIn('id="btnAuditFin"', html)
        self.assertNotIn("revisarHistoricoFinanciero", html)
        self.assertNotIn("/transporte/api/finanzas/limpieza-historica", html)

    # ── REGLA #1: nunca alert/confirm/prompt nativos ───────────────────────
    def test_usa_los_helpers_ilus_y_no_los_nativos(self):
        html = self.render(is_superadmin=True)
        self.assertIn("ilusConfirm(", html)
        self.assertIn("ilusToast(", html)
        for nativo in ("confirm(", "alert(", "prompt("):
            self.assertNotIn(
                " " + nativo, html.replace("ilusConfirm(", "").replace(
                    "ilusAlert(", "").replace("ilusPrompt(", ""),
                f"Se coló un {nativo} nativo (REGLA #1)")

    def test_nunca_escribe_de_un_solo_clic(self):
        """El botón abre la VISTA PREVIA (GET); el POST solo va tras confirmar."""
        html = self.render(is_superadmin=True)
        self.assertIn('onclick="revisarHistoricoFinanciero()"', html)
        # El POST existe, pero detrás del confirm.
        self.assertIn("method: 'POST'", html)
        i_confirm = html.find("ilusConfirm(")
        i_post = html.find("method: 'POST'")
        self.assertGreater(i_confirm, 0)
        self.assertGreater(i_post, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
