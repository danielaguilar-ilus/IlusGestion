# -*- coding: utf-8 -*-
"""🧾 2026-10-18 — Revisión de equipos v3 (ETAPA 1).

(A) Paso 5 del trabajo interno = LA MISMA TABLA que las OT de cliente. Daniel (con capturas): «cópiate de la imagen…
    donde sale el plan, donde se ven las cantidades, donde están los selectores de peso, el documento… en todas las
    órdenes de trabajo debería predominar esta tabla», «tengo dos dedos nada más… necesito que se vea con suficiente
    espacio… achica los objetos de los botones», «elegir el plan, elegir todo». La casilla = INCLUIR EN LA REVISIÓN
    (reemplaza «Llegó / No llegó», decisión de Daniel): las filas desmarcadas se ven atenuadas y NO se envían.
(C) Motivo «Producto sin saldo disponible»: «que quede persistente en los datos de la orden de trabajo, pero que no lo
    evidencie a los clientes ni a los técnicos… quién lo hizo, cuándo, por qué y el argumento».
(D) Instalaciones: «así sea un accesorio, debe haber foto para demostrar que quedó entregado».

Sin BD, sin Flask, sin ERP (REGLA #4.1). Las funciones JS se prueban de verdad con node si está instalado.
Correr:  python -m pytest tests/test_ot_revision_v3.py -q -p no:warnings
"""
import ast
import json
import re
import shutil
import subprocess
import unittest

from tests.test_fac_libro_saldo import _exec
from tests.test_ot_saldo_servicio import _arbol, _decoradores, _fuente, _leer

M = _leer("templates/ot2/_modal_crear.html")
D = _leer("templates/ot2/detalle.html")


def _js_funcion(nombre, src=M):
    """Fuente de `function nombre(...) {...}` del asistente (llaves balanceadas, sin contar las de los textos ni las
    de los comentarios)."""
    i = src.index("function " + nombre + "(")
    j = src.index("{", i)
    prof, k, comillas = 0, j, None
    while k < len(src):
        ch = src[k]
        if comillas:
            if ch == "\\":
                k += 2
                continue
            if ch == comillas:
                comillas = None
        elif src.startswith("//", k):
            k = src.index("\n", k)
            continue
        elif src.startswith("/*", k):
            k = src.index("*/", k) + 2
            continue
        elif ch in "'\"`":
            comillas = ch
        elif ch == "{":
            prof += 1
        elif ch == "}":
            prof -= 1
            if prof == 0:
                return src[i:k + 1]
        k += 1
    raise AssertionError("sin cierre: " + nombre)


def _node(js):
    """Corre `js` en node. Un script largo (los casos de la revisión pasan de 30 KB) va por archivo: Windows limita la línea de
    comandos a ~32 KB y `node -e` fallaba con «el nombre del archivo es demasiado largo»."""
    if len(js) < 8000:
        r = subprocess.run(["node", "-e", js], capture_output=True, text=True, encoding="utf-8")
    else:
        import os
        import tempfile
        with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as f:
            f.write(js)
            ruta = f.name
        try:
            r = subprocess.run(["node", ruta], capture_output=True, text=True, encoding="utf-8")
        finally:
            os.unlink(ruta)
    if r.returncode != 0:
        raise AssertionError(r.stderr)
    return json.loads(r.stdout.strip().splitlines()[-1])


HAY_NODE = shutil.which("node") is not None


# ───────────────────────── (A) la tabla del asistente ─────────────────────────
class TestTablaComoLaDeCliente(unittest.TestCase):
    def test_reusa_el_markup_y_las_clases_de_la_tabla_de_cliente(self):
        f = _js_funcion("pintarTareasManuales")
        for cls in ("o2m-eq-card", "o2m-eq-toolbar", "o2m-eq-toolbar-txt", "o2m-eq-btn-todos", "o2m-eq-modos",
                    "o2m-eq-table o2m-rv-table"):
            self.assertIn(cls, f)
        fila = _js_funcion("_o2mFilaTareaHTML")
        for cls in ("o2m-chk", "td class=\"chk\"", "td class=\"doc\"", "td class=\"prod\"", "td class=\"cant\"",
                    "td class=\"chkl\""):
            self.assertIn(cls, fila)
        self.assertIn("o2m-eq-doc", _js_funcion("_o2mTmDocHtml"))
        self.assertIn("o2m-cant-multi", _js_funcion("_o2mTmCantHtml"))

    def test_columnas(self):
        f = _js_funcion("pintarTareasManuales")
        for col in (">Documento</th>", ">Producto</th>", ">Cant.</th>", ">Clase</th>", ">Checklist</th>"):
            self.assertIn(col, f)

    def test_modos_marcar_todos_y_contador(self):   # REGLA #14 + «elegir todo»
        modos = _js_funcion("_o2mTmModosHtml")
        self.assertIn("'Todo el documento'", modos)
        self.assertIn("'Elijo a mano'", modos)
        self.assertIn("OT2C.setModoTareas(", modos)
        f = _js_funcion("pintarTareasManuales")
        self.assertIn("OT2C.toggleIncluirTodos()", f)
        self.assertIn("' de ' + lista.length + ' incluidos</b></div>'", f)
        self.assertIn("productos sin clase", f)                       # aviso ámbar
        self.assertIn("o2m-eq-modo-aviso", f)
        # «todos» pasa fila por fila por el MISMO camino que la casilla individual (REGLA #14)
        self.assertIn("_o2mTmMarcar(i, algunoFuera)", _js_funcion("toggleIncluirTodos"))
        self.assertIn("_o2mTmMarcar(i, !!incluir)", _js_funcion("setIncluirTareaManual"))
        self.assertIn("_o2mTmMarcar(i, modo === 'todo')", _js_funcion("setModoTareas"))
        self.assertIn("setIncluirTareaManual:setIncluirTareaManual, toggleIncluirTodos:toggleIncluirTodos, "
                      "setModoTareas:setModoTareas,", M)

    def test_clase_y_checklist_con_su_origen(self):
        clase = _js_funcion("_o2mTmClaseHtml")
        self.assertIn("bi-shield-check", clase)
        self.assertIn("Sin clase", clase)
        self.assertIn("Elegir su clase…", clase)
        self.assertIn("' en el catálogo\" onchange=\"OT2C.clasificarProductoChk('", clase)
        celda = _js_funcion("_o2mChkCelda")
        for t in ("Sugerida automáticamente según su clase.", "Guardada para este producto.", "Elegida a mano.",
                  "Recordar para este producto"):
            self.assertIn(t, celda)

    def test_la_casilla_llego_se_reemplazo(self):   # decisión de Daniel: el faltante va en la revisión
        for viejo in ("setLlegoTareaManual", "toggleLlegoTodos", "o2m-her-llego", "c-llego", "no_llego"):
            self.assertNotIn(viejo, M)
        self.assertIn("Si algo no llegó o no se puede usar, se declara en la ficha, arriba de ese producto.", _js_funcion("_o2mChkNota"))

    def test_las_desmarcadas_no_se_envian_ni_cuentan(self):
        self.assertIn("function _o2mTmIncluida(tm){ return !!tm && !tm.excluida; }", M)
        c = _js_funcion("crear")
        self.assertEqual(c.count("body.tareas_manuales = _o2mTareasIncluidas();"), 2)   # bodega y el resto
        self.assertNotIn("body.tareas_manuales = S.tareas_manuales.slice();", c)
        self.assertEqual(M.count("return _o2mTareasIncluidas().length > 0;"), 2)       # completo('equipos')

    def test_cantidad_igual_que_la_de_cliente(self):
        f = _js_funcion("_o2mAgregarItemsHeredando")
        self.assertIn("Math.round(_o2mQtyDeItem(it))", f)          # la del documento, no el saldo
        self.assertIn("unidad: porLinea ? null : u, unidades: porLinea ? null : it.qty", f)
        cant = _js_funcion("_o2mTmCantHtml")
        self.assertIn("tm.unidad + ' de ' +", cant)                 # «1 de 2» con badge
        self.assertIn("OT2C.setCantidadTareaManual(", cant)         # accesorios: editable

    def test_espacio_y_celular(self):   # REGLA #3: sin caja de 250px, toques de 44px, sin scroll lateral
        self.assertIn("#o2mTareasList.o2m-lista{max-height:none;overflow:visible", M)
        self.assertIn(".o2m-rv-chkbtn{width:44px;height:44px", M)
        self.assertIn("#o2mTareasList .o2m-rv-table{min-width:0 !important", M)
        self.assertIn("grid-template-columns:46px minmax(0,1fr)", M)
        self.assertIn("#o2mTareasList .o2m-rv-table td.chk{grid-column:1;grid-row:1;align-self:start}", M)
        self.assertIn("#o2mTareasList .o2m-rv-table td.x{grid-column:1;grid-row:2 / span 3", M)   # riel propio
        self.assertIn("@media (max-width:900px){\n  #o2mTareasList .o2m-eq-toolbar", M)
        self.assertIn("#o2mTareasList .o2m-rv-x{min-width:44px;min-height:44px}", M)
        # tarjetas de acción más compactas (siguen todas)
        self.assertIn("padding:8px 12px;min-height:52px", M)
        self.assertIn(".o2m-acc .ic{flex:0 0 34px;height:34px", M)
        for fn in ("abrirBusquedaDocInterno", "abrirProductoCatalogo", "abrirTareaManualModal"):
            self.assertIn("tarjeta('" + fn + "'", M)

    def test_sin_popups_nativos(self):   # REGLA #1
        i = M.index("function _o2mTmIncluida(")
        j = M.index("function setPlantillaTareaManual(")
        self.assertIsNone(re.search(r"(?<![\w.])(alert|confirm|prompt)\(", M[i:j]))


@unittest.skipUnless(HAY_NODE, "node no está instalado")
class TestTablaEnNode(unittest.TestCase):
    """Las funciones reales del asistente, ejecutadas con un S de mentira."""

    def _correr(self, script):
        fns = "\n".join(_js_funcion(n) for n in (
            "_o2mTmIncluida", "_o2mTareasIncluidas", "_o2mTmMarcar", "setIncluirTareaManual", "toggleIncluirTodos",
            "setModoTareas"))
        return _node("var pintadas=0; function pintarTareasManuales(){ pintadas++; } function refrescarAnclas(){}\n"
                     + fns + "\n" + script)

    def test_marcar_desmarcar_todos_y_lo_que_viaja(self):
        r = self._correr(
            "var S={tm_modo:'todo', tareas_manuales:[{sku:'A'},{sku:'B'},{sku:'C'}]};"
            "var out={};"
            "out.inicio=_o2mTareasIncluidas().length;"
            "setIncluirTareaManual(1,false); out.uno=_o2mTareasIncluidas().map(function(t){return t.sku;});"
            "toggleIncluirTodos(); out.todos=_o2mTareasIncluidas().length;"            # faltaba una: marca todas
            "toggleIncluirTodos(); out.ninguna=_o2mTareasIncluidas().length;"          # estaban todas: las saca
            "setModoTareas('todo'); out.modoTodo=_o2mTareasIncluidas().length;"
            "setModoTareas('mano'); out.modoMano=_o2mTareasIncluidas().length; out.filas=S.tareas_manuales.length;"
            "setModoTareas('otro'); out.modo=S.tm_modo;"
            "console.log(JSON.stringify(out));")
        self.assertEqual(r["inicio"], 3)
        self.assertEqual(r["uno"], ["A", "C"])           # la desmarcada no viaja
        self.assertEqual(r["todos"], 3)
        self.assertEqual(r["ninguna"], 0)
        self.assertEqual(r["modoTodo"], 3)
        self.assertEqual(r["modoMano"], 0)
        self.assertEqual(r["filas"], 3)                  # siguen a la vista
        self.assertEqual(r["modo"], "mano")              # un modo desconocido no cambia nada

    def test_captura_sin_saldo_sin_repetir(self):
        fns = "\n".join(_js_funcion(n) for n in (
            "_o2mCapturarSinSaldo", "_o2mSinSaldoDeSku", "_o2mSinSaldoPayload", "_o2mSkuNorm", "_o2mDocOrigenNorm",
            "_o2mQtyDeItem"))
        r = _node(fns + "\nvar S={sin_saldo:[]};"
                  "_o2mCapturarSinSaldo(["
                  " {sku:'350', nombre:'Gymleco 350', tido:'NVI', nudo:'687', qty:1, cantidad_doc:2,"
                  "  marcada_sin_saldo:true, motivo_sin_saldo:'Volvió del evento'},"
                  " {sku:'326', nombre:'Gymleco 326', tido:'NVI', nudo:'687', qty:1, marcada_sin_saldo:false}]);"
                  "_o2mCapturarSinSaldo([{sku:'350', tido:'NVI', nudo:'0687', qty:1, marcada_sin_saldo:true,"
                  " motivo_sin_saldo:'Otro motivo'}]);"
                  "console.log(JSON.stringify({n:S.sin_saldo.length, p:_o2mSinSaldoPayload(),"
                  " de350:_o2mSinSaldoDeSku('350').length, de326:_o2mSinSaldoDeSku('326').length}));")
        self.assertEqual(r["de326"], 0)                  # con saldo: no se registra
        self.assertEqual(r["p"][0]["sku"], "350")
        self.assertEqual(r["p"][0]["cantidad"], 1)
        self.assertEqual(r["p"][0]["saldo"], 0)
        self.assertEqual(r["p"][0]["motivo"], "Otro motivo")   # misma línea: se actualiza, no se duplica


class TestSinSaldoEnElAsistente(unittest.TestCase):
    def test_se_junta_en_los_cuatro_caminos_y_viaja(self):
        self.assertIn("_o2mCapturarSinSaldo(items);", _js_funcion("_o2mAgregarItemsHeredando"))   # documento/catálogo
        self.assertIn("_o2mCapturarSinSaldo(items, header);", _js_funcion("_o2mResolverClienteYEquipos"))  # Factura o boleta
        self.assertIn("_o2mCapturarSinSaldo(lineas, header);", _js_funcion("_o2mEqDocSeleccionado"))  # otro documento
        self.assertIn("body.sin_saldo = _o2mSinSaldoPayload();", _js_funcion("crear"))
        self.assertIn("sin_saldo:[],", _js_funcion("reset"))
        self.assertIn("tm_modo:'todo',", _js_funcion("reset"))
        self.assertIn("Sin saldo · motivo registrado", _js_funcion("_o2mFilaTareaHTML"))
        self.assertIn("Sin saldo · motivo registrado", _js_funcion("_o2mFilaEquipoHTML"))   # tabla de cliente


# ───────────────────────── (C) servidor: sin saldo ─────────────────────────
def _amb_sin_saldo():
    amb = {"_OT_SIN_SALDO_MAX": 100, "_OT_SIN_SALDO_ARG_MAX": 500}
    _exec("_ot_sin_saldo_num", amb)
    return amb, _exec("_ot_sin_saldo_normalizar", amb)


class TestSinSaldoNormalizar(unittest.TestCase):
    def setUp(self):
        _amb, self.f = _amb_sin_saldo()

    def test_solo_los_sku_que_quedan_en_la_ot(self):
        r = self.f([
            {"sku": "350", "nombre": "Gymleco 350", "tido": "NVI", "nudo": "687", "cantidad": 2, "saldo": 0,
             "motivo": "Volvió del evento"},
            {"sku": "999", "tido": "NVI", "nudo": "687", "motivo": "fila desmarcada"},   # ya no está en la OT
        ], {"350", "326"})
        self.assertEqual([x["sku"] for x in r], ["350"])
        self.assertEqual(r[0]["argumento"], "Volvió del evento")
        self.assertEqual((r[0]["doc_tido"], r[0]["doc_nudo"], r[0]["cantidad"], r[0]["saldo_doc"]),
                         ("NVI", "687", 2.0, 0.0))
        self.assertIn("NVI 687", r[0]["causa"])           # la causa la escribe el servidor
        self.assertIn("ya rebajó", r[0]["causa"])

    def test_topes_y_repetidos(self):
        r = self.f([{"sku": "350", "tido": "NVI", "nudo": "687", "motivo": "x" * 900}] * 3, {"350"})
        self.assertEqual(len(r), 1)                       # SKU + documento, una vez
        self.assertEqual(len(r[0]["argumento"]), 500)
        muchos = [{"sku": f"S{i}", "motivo": "m"} for i in range(150)]
        self.assertEqual(len(self.f(muchos, {f"S{i}" for i in range(150)})), 100)

    def test_basura_no_rompe(self):
        self.assertEqual(self.f(None, {"350"}), [])
        self.assertEqual(self.f([{"sku": "350"}], set()), [])
        self.assertEqual(self.f(["texto", 3, {"sku": ""}], {"350"}), [])
        r = self.f([{"sku": "350", "cantidad": "abc", "saldo": "-4"}], {"350"})
        self.assertIsNone(r[0]["cantidad"])
        self.assertEqual(r[0]["saldo_doc"], 0.0)
        self.assertEqual(r[0]["argumento"], "")


class TestSinSaldoServidor(unittest.TestCase):
    def test_tabla_barata_y_en_el_arranque(self):   # REGLA #18
        f = _fuente("_ensure_ot_sin_saldo")
        self.assertIn("CREATE TABLE IF NOT EXISTS mant_ot_sin_saldo", f)
        for col in ("visita_id INT NOT NULL", "sku VARCHAR(60) NOT NULL", "descripcion", "doc_tido", "doc_nudo",
                    "cantidad", "saldo_doc", "causa", "argumento TEXT", "declarado_por",
                    "declarado_en DATETIME DEFAULT CURRENT_TIMESTAMP", "KEY ix_ot_sin_saldo_visita (visita_id)"):
            self.assertIn(col, f)
        self.assertIn("        _ensure_ot_sin_saldo()\n", _leer("app.py"))

    def test_crear_guarda_en_la_misma_transaccion(self):
        f = _fuente("_ot2_crear_core")
        self.assertIn('_sin_saldo = _ot_sin_saldo_normalizar(d.get("sin_saldo"), _skus_ot)', f)
        self.assertIn('_skus_ot = {(_t.get("sku") or "").strip() for _t in tareas_manuales if _t.get("sku")}', f)
        self.assertIn("SELECT sku FROM mant_maquinas WHERE id IN", f)           # SKU de las máquinas de la OT
        self.assertIn('cur.execute("SAVEPOINT ot_sin_saldo")', f)
        self.assertIn("INSERT INTO mant_ot_sin_saldo", f)
        self.assertIn("'sin_saldo_declarado'", f)
        self.assertIn('cur.execute("ROLLBACK TO SAVEPOINT ot_sin_saldo")', f)
        # va dentro de la transacción de la OT: después del INSERT de la visita y antes de su commit
        i, j = f.index("INSERT INTO mant_visitas"), f.index("INSERT INTO mant_ot_sin_saldo")
        self.assertLess(i, j)
        self.assertNotIn("conn.commit()", f[i:j])
        self.assertIn("conn.commit()", f[j:])

    def test_la_bitacora_no_llega_a_un_tecnico(self):   # REGLA #26
        codigo, arbol = _arbol()
        amb = {"re": re}
        for n in arbol.body:
            if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id.startswith("_ACT_TEC_")
                                                 for t in n.targets):
                exec(compile(ast.Module(body=[n], type_ignores=[]), "<app>", "exec"), amb)
        f = _exec("_ot_actividad_para_tecnico", amb)
        self.assertEqual(f("sin_saldo_declarado", "Incluido sin saldo: 350 (NVI 687) — argumento: x"), (False, ""))
        self.assertNotIn("sin_saldo_declarado", amb["_ACT_TEC_PERMITIDAS"])

    def test_la_ficha_solo_para_el_equipo_interno(self):   # REGLA #26: el dato se quita en el servidor
        f = _fuente("ot2_detalle")
        self.assertIn("    sin_saldo = []\n    if not _es_rol_tecnico():\n        try:\n"
                      "            sin_saldo = _ot_sin_saldo_de(vid)", f)
        self.assertIn("sin_saldo=sin_saldo,", f)
        self.assertIn("{% if sin_saldo and not es_tecnico %}", D)
        i = D.index("{% if sin_saldo and not es_tecnico %}")
        tarjeta = D[i:D.index("{% endif %}", D.index("{% endfor %}", i))]
        for t in ("Productos incluidos sin saldo en el documento", "Solo equipo interno", "Por qué no tenía saldo",
                  "Argumento", "s.declarado_por", "s.declarado_en | chile_fmt", "s.sku", "s.cantidad", "s.doc_tido"):
            self.assertIn(t, tarjeta)

    def test_no_sale_al_cliente(self):   # ni PDF, ni informe, ni correo
        import glob
        import os
        from tests.test_ot_saldo_servicio import RAIZ
        for ruta in glob.glob(os.path.join(RAIZ, "templates", "**", "*.html"), recursive=True):
            if ruta.replace("\\", "/").endswith("templates/ot2/detalle.html"):
                continue
            with open(ruta, encoding="utf-8", errors="ignore") as fh:
                txt = fh.read()
            self.assertNotIn("Productos incluidos sin saldo", txt, ruta)
            self.assertNotIn("mant_ot_sin_saldo", txt, ruta)
        app = _leer("app.py")
        self.assertEqual(app.count("FROM mant_ot_sin_saldo"), 1)   # solo la lectura de la ficha
        self.assertEqual(_decoradores("ot2_detalle")[:2], ["app.route('/ot/<int:vid>')", "_mant_required"])


# ───────────────────────── (D) foto de entrega en instalaciones ─────────────────────────
def _amb_foto():
    amb = {"_FOTO_ENTREGA_TITULO": "Foto de entrega"}
    return amb, _exec("_foto_entrega_a_ajustar", amb)


PLANTILLAS = [
    {"id": 1, "nombre": "Instalación nueva", "categoria_admin": "instalacion", "activa": 1},     # ya tiene foto oblig.
    {"id": 2, "nombre": "Accesorios", "categoria_admin": "instalacion", "activa": 1},             # foto NO obligatoria
    {"id": 3, "nombre": "Trotadora vieja", "categoria_admin": "instalacion", "activa": 0},        # inactiva
    {"id": 4, "nombre": "Accesorios", "categoria_admin": "mantencion", "activa": 1},              # otra categoría
    {"id": 5, "nombre": "Banco", "categoria_admin": "instalacion", "activa": 1},                  # sin ítems
]
ITEMS = [
    {"plantilla_id": 1, "titulo": "Foto de la instalación terminada", "tipo_respuesta": "foto", "obligatoria": 1, "orden": 7},
    {"plantilla_id": 2, "titulo": "Foto del equipo por sus 4 lados", "tipo_respuesta": "foto", "obligatoria": 0, "orden": 9},
    {"plantilla_id": 2, "titulo": "Comentarios adicionales", "tipo_respuesta": "texto", "obligatoria": 0, "orden": 10},
    {"plantilla_id": 3, "titulo": "Serie", "tipo_respuesta": "texto", "obligatoria": 1, "orden": 1},
]


class TestFotoEntregaPura(unittest.TestCase):
    def setUp(self):
        _amb, self.f = _amb_foto()

    def test_solo_instalacion_activa_sin_foto_obligatoria_y_al_final(self):
        r = self.f(PLANTILLAS, ITEMS)
        self.assertEqual([(x["id"], x["orden"]) for x in r], [(2, 11), (5, 1)])

    def test_no_repite_si_ya_tiene_foto_de_entrega(self):
        items = ITEMS + [{"plantilla_id": 2, "titulo": "Foto de entrega", "tipo_respuesta": "foto",
                          "obligatoria": 0, "orden": 11}]
        self.assertEqual([x["id"] for x in self.f(PLANTILLAS, items)], [5])


class TestFotoEntregaYaNoEsAutomatica(unittest.TestCase):
    """🎛️ 2026-10-20 (REGLA #32 — Daniel: «como es regla estricta del proyecto, no debes imponer o gestionar más checklists,
    sino los que impone el usuario»): «Foto de entrega» dejó de agregarse sola al arrancar. Ahora es una PROPUESTA
    (/mantenciones/plantillas → «Propuestas del sistema»): la persona ve la plantilla actual junto a la propuesta y decide
    «Crear como plantilla nueva» (recomendado) o «Reemplazar el contenido» (gestión, con confirmación, nunca «(LU)»)."""

    def test_el_arranque_ya_no_modifica_las_plantillas_de_instalacion(self):
        app = _leer("app.py")
        for muerto in ("def _ensure_foto_entrega_instalacion", "_ensure_foto_entrega_instalacion()", "_FOTO_ENTREGA_MARCA",
                       "_FOTO_ENTREGA_LOCK"):
            self.assertNotIn(muerto, app, muerto)

    def test_la_pura_sigue_alimentando_la_propuesta(self):
        f = _fuente("_prop_construir")
        self.assertIn("_foto_entrega_a_ajustar(", f)
        self.assertIn('_prop_armar("foto_entrega"', f)
        self.assertEqual(_decoradores("mant_plantillas_propuestas")[1:], ["_mant_required", "_no_tecnico_externo"])
        self.assertNotIn("INSERT", f)           # armar la propuesta es solo lectura
        self.assertNotIn("UPDATE", f)


if __name__ == "__main__":
    unittest.main()
