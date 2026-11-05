# -*- coding: utf-8 -*-
"""🛡️ 6-nov-2026 — Cotizaciones, columna PLAN: el escudo sale SIEMPRE (también en una instalación y en un producto que vino de un documento)
y el plan se RECONOCE por SKU contra los equipos de la ficha del cliente (Daniel: «es una mantención, debería estar dentro del plan… sí lo
tiene pero no reconoce el plan; el botón debería estar igual»).
Correr:  python -m pytest tests/test_cot_plan_boton.py -q -p no:warnings"""
import os
import re
import shutil
import subprocess
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(*ruta):
    with open(os.path.join(RAIZ, *ruta), encoding="utf-8") as fh:
        return fh.read().replace("\r\n", "\n")


JS = _leer("static", "tickets_cotizaciones.js")
PY = _leer("tickets_module.py")


def _funcion(nombre):
    m = re.search(r"function %s\(.*?\n\}\n" % re.escape(nombre), JS, re.S)
    assert m, nombre
    return m.group(0)


class TestBoton(unittest.TestCase):
    def test_el_escudo_sale_siempre_no_solo_con_equipo_de_la_ficha(self):
        i = JS.index('<td class="cot-rev-plan-cell" data-label="Plan">')
        f = JS[i:i + 1300]
        self.assertNotIn("data-label=\"Plan\">' + (it.maquina_id != null", f)   # el botón ya no depende de tener equipo
        self.assertIn("</button></td>", f)                             # el botón va siempre dentro de la celda
        self.assertNotIn("'<span class=\"text-muted\">—</span>'", f)  # ya no hay guion en su lugar
        self.assertIn("cotWizTogPlan(' + i + ')", f)
        self.assertIn("solo en esta cotización", f)

    def test_marcar_todos_se_ofrece_con_cualquier_producto(self):
        self.assertIn("const _hayEquipos = _WIZ.items.length > 0;", JS)
        i = JS.index("function cotWizTogPlanTodos()")
        self.assertNotIn("if (it.maquina_id != null) candidatos.push(i)", JS[i:i + 500])

    def test_sin_equipo_en_la_ficha_se_marca_solo_en_la_cotizacion_sin_tocar_la_ficha(self):
        f = _funcion("cotWizTogPlan")
        i_local = f.index("if (it.maquina_id == null){")
        i_fetch = f.index("fetch('/mantenciones/api/maquinas/")
        self.assertLess(i_local, i_fetch)                              # el retorno local ocurre ANTES de cualquier escritura a la ficha
        self.assertIn("return;", f[i_local:i_fetch])

    def test_el_documento_dispara_el_reconocimiento(self):
        i = JS.index("async function _cotWizAsignarDesdeDocumento(")
        self.assertIn("await _cotWizReconocerPlan();", JS[i:i + 6500])

    def test_el_reconocimiento_no_pisa_el_paso_1(self):
        f = _funcion("_cotWizReconocerPlan") if "function _cotWizReconocerPlan(" in JS else JS[JS.index("async function _cotWizReconocerPlan"):][:2600]
        self.assertNotIn("_cotWizCargarFichaResumen", f)               # eso rellenaría banner y contactos
        self.assertNotIn("_WIZ.origen", f)
        self.assertIn("'/tickets/api/clientes-ficha/'", f)
        self.assertIn("_cotWizRutLimpio(x.rut) === _cotWizRutLimpio(rut)", f)   # el cliente se enlaza solo si el RUT coincide exacto

    def test_el_servidor_guarda_el_plan_marcado_aunque_no_haya_equipo(self):
        self.assertEqual(PY.count('aplica_mantencion_it = 1 if it.get("aplica_mantencion") else (0 if maquina_id_it else None)'), 2)
        self.assertNotIn('(1 if it.get("aplica_mantencion") else 0) if maquina_id_it else None', PY)

    @unittest.skipUnless(shutil.which("node"), "node no está instalado")
    def test_enlaza_por_sku_un_equipo_distinto_por_fila(self):
        codigo = _funcion("_cotWizEnlazarPlanItem") + """
globalThis._WIZ = { items: [ {sku:'tm-1'}, {sku:'TM-1'}, {sku:'TM-1'}, {sku:'XX'}, {sku:'BK-2', maquina_id: 99} ],
  fichaMaquinas: [ {id:1, sku:'TM-1', en_plan:true}, {id:2, sku:'tm-1', en_plan:false}, {id:99, sku:'BK-2', en_plan:true} ] };
const r = _WIZ.items.map(it => _cotWizEnlazarPlanItem(it));
console.log(JSON.stringify({ r, ids: _WIZ.items.map(i => i.maquina_id || null), plan: _WIZ.items.map(i => !!i.aplica_mantencion) }));"""
        out = subprocess.run(["node", "-e", codigo], capture_output=True, text=True)
        self.assertEqual(out.returncode, 0, out.stderr)
        self.assertEqual(out.stdout.strip(),
                         '{"r":[true,true,false,false,false],"ids":[1,2,null,null,99],"plan":[true,false,false,false,false]}')


if __name__ == "__main__":
    unittest.main()
