"""Afinado del 07/08-oct-2026 (Daniel, mirando producción: «sigue diciendo que cobramos 200… los tamaños, los botones,
el centro de costo con botones… que no puedan entramparse en un ciclo que no tiene solución»).

Qué se vigila (sin BD ni Flask; app.py con ast, plantillas y JS con texto):
  · El «Precio al cliente» suelto (sin documento ni cobro declarado) NO es un cobro: queda como «precio anotado».
  · Sin ciclos: cada código de rechazo de «Firmar y cerrar» tiene una acción que FUNCIONA en el estado en que la OT
    llega al modal ('completada' / 'pendiente_aprobacion' / 'firmada_tecnico'), sin tocar estado ni firmas.
  · El motor y la tarjeta conviven: el motor es la vista principal; la tarjeta es la edición; una sola cuenta.
  · El centro de costo va con botones en el motor y en «Corregir finanzas»; «Corregir finanzas» separa Cobré de Me cobraron.
Correr con:  py -m unittest tests.test_ot_afinado_0710
"""
import ast
import os
import re
import unittest

from tests.test_incidencias_bajas import _codigo_y_arbol
from tests.test_ot_finanzas_modelo import F

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(rel):
    with open(os.path.join(RAIZ, rel.replace("/", os.sep)), encoding="utf-8", newline="") as f:
        return f.read().replace("\r\n", "\n")


class TestPrecioAnotado(unittest.TestCase):
    def test_precio_anotado_sin_documento_no_es_cobro(self):
        r = F(costo=200000, costo_proveedor=200000)
        self.assertFalse(r["cobre"]["hay"])
        self.assertEqual(r["cobre"]["total"], 0)
        self.assertEqual(r["label"], "Falta el documento de cobro")
        self.assertEqual(r["precio_anotado"], 200000)
        self.assertEqual(r["valorizado"], {"monto": 200000, "fuente": "precio_anotado"})
        self.assertFalse(r["queda"]["mostrar"])
        self.assertTrue(any("precio anotado" in a.lower() for a in r["avisos"]))

    def test_con_respaldo_si_cuenta(self):
        for origen in ("doc_total", "cotizacion", "contrato", "manual", "supuesto", "zz"):
            r = F(costo=200000, costo_proveedor=50000, valor_origen=origen)
            self.assertEqual(r["cobre"]["total"], 200000, origen)
            self.assertIsNone(r["precio_anotado"], origen)

    def test_zzretiro_o_estimado_con_precio_anotado_no_cuentan(self):
        r = F(zz_monto=1, zz_codigo="ZZRETIRO", valor_origen="zz", costo=200000, costo_proveedor=1000)
        self.assertEqual(r["cobre"]["total"], 0)
        self.assertEqual(r["precio_anotado"], 200000)
        r = F(zz_monto=80000, valor_origen="estimado", costo=80000, costo_proveedor=1000)
        self.assertEqual(r["cobre"]["total"], 0)

    def test_si_no_se_cobra_no_hay_precio_anotado(self):
        r = F(modalidad_cobro="garantia", costo=200000, costo_proveedor=50000)
        self.assertIsNone(r["precio_anotado"])
        self.assertEqual(r["cobre"]["total"], 0)

    def test_la_linea_del_documento_manda_sobre_el_precio_anotado(self):
        r = F(zz_monto=150000, valor_origen="zz", costo=200000, costo_proveedor=50000)
        self.assertEqual(r["cobre"]["total"], 150000)
        self.assertIsNone(r["precio_anotado"])


_ARBOL = None


def _arbol():
    global _ARBOL
    if _ARBOL is None:
        _ARBOL = _codigo_y_arbol()[1]
    return _ARBOL


def _func(nombre):
    for n in _arbol().body:
        if isinstance(n, ast.FunctionDef) and n.name == nombre:
            return n
    raise AssertionError("no está " + nombre)


def _decoradores(nombre):
    out = []
    for d in _func(nombre).decorator_list:
        out.append(ast.unparse(d))
    return out


def _const(nombre):
    for n in _arbol().body:
        if isinstance(n, ast.Assign) and len(n.targets) == 1 and isinstance(n.targets[0], ast.Name) and n.targets[0].id == nombre:
            return ast.literal_eval(n.value)
    raise AssertionError("no está " + nombre)


class _G:
    def __init__(self, user):
        self.user = user


def _puede(estado, role="ejecutivo", base=False, firma=None):
    """Ejecuta _ot_puede_finanzas_cierre con el permiso de siempre y la base simulados."""
    amb = {"print": lambda *a, **k: None,
           "_OT_FIN_ESTADOS_CIERRE": _const("_OT_FIN_ESTADOS_CIERRE"),
           "g": _G({"role": role, "id": 7}),
           "_puede_ot_accion": lambda vid, acc, u: base,
           "_rol_familia": lambda r: {"ejecutivo_sstt": "ejecutivo"}.get(r, r),
           "mysql_fetchone": lambda sql, p=(): {"estado": estado, "firma_supervisor_user_id": firma}}
    fn = _func("_ot_puede_finanzas_cierre")
    fn.decorator_list = []
    exec(compile(ast.Module(body=[fn], type_ignores=[]), "<app>", "exec"), amb)
    return amb["_ot_puede_finanzas_cierre"](5, {"role": role, "id": 7})


class TestSinCiclos(unittest.TestCase):
    """Daniel: «que no puedan entramparse en un ciclo que no tiene solución»."""

    def test_gestion_resuelve_en_los_estados_en_que_llega_al_modal(self):
        for estado in ("completada", "pendiente_aprobacion", "firmada_tecnico"):
            for rol in ("ejecutivo", "supervisor", "admin", "superadmin", "ejecutivo_sstt"):
                self.assertTrue(_puede(estado, rol), f"{rol} en {estado}")

    def test_cerrada_y_anuladas_siguen_selladas(self):
        for estado in ("cerrada", "cancelada", "anulada"):
            self.assertFalse(_puede(estado, "superadmin"), estado)

    def test_nunca_tecnico_ni_con_firma_del_responsable(self):
        self.assertFalse(_puede("completada", "tecnico"))
        self.assertFalse(_puede("completada", "tecnico_ejecutivo"))
        self.assertFalse(_puede("completada", "ejecutivo", firma=12))   # ya hay firma de cierre: evidencia

    def test_el_permiso_de_siempre_sigue_valiendo(self):
        self.assertTrue(_puede("en_ejecucion", "ejecutivo", base=True))
        self.assertFalse(_puede("en_ejecucion", "ejecutivo", base=False))

    def test_las_rutas_de_finanzas_usan_la_ventana_de_cierre(self):
        for ruta in ("ot2_api_centro_costo", "ot2_api_finanzas", "ot2_api_finanzas_buscar_erp", "ot_api_costo_proveedor",
                     "ot2_api_documentos_agregar", "ot2_api_finanzas_lineas_zz"):
            self.assertIn("_ot_can_finanzas_cierre", _decoradores(ruta), ruta)
            self.assertNotIn("_ot_can_cobertura", _decoradores(ruta), ruta)

    def test_centro_de_costo_y_cobro_no_excluyen_completada(self):
        app = _leer("app.py")
        cc = app.split("def ot2_api_centro_costo")[1].split("def ")[0]
        self.assertIn("'cerrada','cancelada','anulada'", cc)
        self.assertNotIn("'completada','cerrada'", cc)
        fin = app.split("def ot2_api_finanzas(vid)")[1].split("def ot2_api_finanzas_buscar_erp")[0]
        self.assertIn("\" AND estado NOT IN ('cerrada','cancelada','anulada')\")", fin)

    def test_panorama_y_regularizar_usan_el_mismo_permiso(self):
        app = _leer("app.py")
        self.assertIn('puede_cob = bool(_ot_puede_finanzas_cierre(vid, getattr(g, "user", None) or {}))', app)
        self.assertIn("    if _ot_puede_finanzas_cierre(vid):\n        return ot2_api_documentos_agregar(vid)", app)

    def test_pedir_autorizacion_funciona_en_cualquier_estado_no_sellado(self):
        app = _leer("app.py")
        crear = app.split("def ot_aut_api_crear")[1].split("@app.route")[0]
        self.assertIn('("cancelada", "anulada")', crear)
        self.assertNotIn("completada", crear)

    def test_cada_codigo_de_rechazo_tiene_una_accion_que_funciona(self):
        acciones = _const("_OT_CIERRE_ACCIONES")
        js = _leer("static/ot_fin_motor.js")
        mapa = re.search(r"var m = \{(.*?)\}\[a\.tipo\];", js, re.S).group(1)
        mapa = dict(re.findall(r"(\w+):\s*'(\w+)'", mapa))
        # tipo de acción → función del motor → ruta que la sirve (y su permiso)
        sirve = {"ligarDoc": "ot2_api_documentos_agregar", "corregirProv": "ot_api_costo_proveedor",
                 "declararCobro": "ot2_api_finanzas", "enfocarCentro": "ot2_api_centro_costo"}
        for codigo, (tipo, _txt) in acciones.items():
            if tipo in ("esperar_autorizacion", "actualizar_anexo", "ver_ot"):
                continue                      # solo informan o llevan a la solicitud
            if tipo == "pedir_autorizacion":
                self.assertEqual(mapa[tipo], "pedirCierre", codigo)
                continue
            fn = mapa.get(tipo)
            self.assertIsNotNone(fn, f"{codigo}: el motor no resuelve «{tipo}»")
            self.assertIn(fn, sirve, f"{codigo}: «{fn}» no tiene ruta")
            self.assertIn("_ot_can_finanzas_cierre", _decoradores(sirve[fn]), f"{codigo} → {fn}")
        # la nota de venta que falta cobrar se liga desde el mismo modal
        self.assertIn("ligar_factura", mapa)

    def test_declarar_cobro_desde_el_motor_deja_motivo_y_no_toca_firmas(self):
        js = _leer("static/ot_fin_motor.js")
        self.assertIn("function declararCobro(inst)", js)
        self.assertIn("'/ot/api/finanzas/' + inst.vid", js)
        self.assertIn("zz_motivo_manual", js)
        for prohibido in ("firma", "estado:"):
            cuerpo = js.split("function declararCobro(inst)")[1].split("function irFinanzas")[0]
            self.assertNotIn(prohibido, cuerpo)
        app = _leer("app.py")
        self.assertIn('_mant_log("visita", vid, "finanzas_declaradas"', app)
        self.assertIn('_mant_log("visita", vid, "costo_proveedor_corregido"', app)
        self.assertIn('"centro_costo"', app.split("def ot2_api_centro_costo")[1].split("def ")[0])


class TestMotorYTarjeta(unittest.TestCase):
    def test_el_centro_de_costo_es_de_botones_en_el_motor(self):
        js = _leer("static/ot_fin_motor.js")
        css = _leer("static/ot_fin_motor.css")
        self.assertIn('class="fm-cc-grid"', js)
        self.assertIn("data-fm-cc", js)
        self.assertNotIn("data-fm-centro", js)
        for centro, icono in (("sstt", "bi-wrench-adjustable-circle-fill"), ("logistica", "bi-truck"),
                              ("comercial", "bi-briefcase-fill"), ("marketing", "bi-megaphone-fill")):
            self.assertIn(centro, js)
            self.assertIn(icono, js)
        self.assertIn(".fm-cc.on", css)
        self.assertIn("fmCcPop", css)

    def test_corregir_finanzas_separa_cobre_de_me_cobraron_y_usa_botones(self):
        det = _leer("templates/ot2/detalle.html")
        self.assertIn('id="otdFcFuente"', det)
        self.assertIn('id="otdFcCcGrid"', det)
        self.assertIn('<select class="form-select" id="otdFc_centro_costo" hidden></select>', det)
        self.assertIn("function otdFinCorrFuente(f)", det)
        self.assertIn("Precio anotado sin documento", det)
        # el precio anotado NO se vuelca solo en el casillero del cobro: hay que pedirlo con el botón
        abrir = det.split("async function otdFinCorrAbrir(){")[1].split("async function otdFinCorrGuardar")[0]
        self.assertNotIn("costo", abrir.split("_OTD_FC_CAMPOS.forEach")[1].split("});")[0])

    def test_la_tarjeta_vieja_conserva_todo_y_no_repite_la_cuenta(self):
        det = _leer("templates/ot2/detalle.html")
        for ident in ("otdFinBar", "otdFinBarCobro", "otdFinBarCosto", "otdFinMargen", "otdFinBtnGuardar", "otdFinBtnCorregirBar",
                      "otdFinBarChev", "otdCcGrid", "otdFinCentroCosto", "otdFinPasoCobro", "otdCardMotor", "otdCardFinanzas"):
            self.assertIn(ident, det, ident)
        self.assertIn("#otdCardFinanzas:not(.fin-editando) .otd-fbar-cuenta", det)
        self.assertIn("fin-editando", det)
        # el motor va ANTES que la tarjeta de edición
        self.assertLess(det.index('id="otdCardMotor"'), det.index('id="otdCardFinanzas"'))

    def test_documentos_agrupados_por_factura_con_lo_que_aportan(self):
        js = _leer("static/ot_fin_motor.js")
        self.assertIn("function aporteDoc(d)", js)
        self.assertIn("Aporta al cobro: servicio", js)
        self.assertIn("fm-aporta", js)
        # orden: documentos → lo que nos costó → el resultado
        pinta = js.split("function pintar(inst)")[1]
        self.assertLess(pinta.index("htmlDocs(inst)"), pinta.index("htmlCostos(inst)"))
        self.assertLess(pinta.index("htmlCostos(inst)"), pinta.index("htmlCuenta(inst)"))

    def test_compacto_y_sin_scroll_horizontal(self):
        css = _leer("static/ot_fin_motor.css")
        self.assertIn("COMPACTO", css)
        self.assertIn(".fm-main", css)
        self.assertIn("minmax(0,1.5fr)", css)
        self.assertIn("overflow-wrap:anywhere", css)
        self.assertRegex(css, r"@media \(max-width:560px\)")
        self.assertIn("min-height:44px", css)
        self.assertNotIn("line-clamp", css)


if __name__ == "__main__":
    unittest.main()
