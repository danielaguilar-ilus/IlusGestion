"""REGLA #26 -- ningun tecnico ve cuentas, deudas ni plata de la empresa (Daniel, 2026-10-08).

Pedido textual: "Anteriormente las ordenes de trabajo no exponian las deudas, las cuentas, nada a los tecnicos
externos. Asi que tampoco a los internos... Cuidemos la imagen y la confidencialidad de la empresa, sobre todo cuando
cae en el dashboard". 'Tecnicos' = TODA la familia: interno (`tecnico`), elevado (`tecnico_ejecutivo`: Jaizer, Lenin,
Dave) y externo (`tecnico_externo`). Nada de montos cobrados, lo que cobran proveedores y tecnicos, costos, margenes,
deudas, facturas de proveedor, N de OC, valorizados ni autorizaciones de $0: ni en pantallas, APIs, bitacora ni campana.

Sin BD ni Flask: funciones de app.py extraidas con ast y ejecutadas con dobles (mismo criterio que
tests/test_privacidad_proveedores_tecnicos.py), mas una revision estatica de TODAS las rutas con nombre o ruta
financiera, que exige un decorador de gestion o una excepcion documentada en una lista blanca explicita.

Correr con:  py -m unittest tests.test_confidencialidad_financiera_tecnicos
"""
import ast
import os
import re
import sys
import types
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tests.test_incidencias_bajas import _cargar  # noqa: E402
from tests.test_incidencias_repuesto_tercera_fuente import _codigo_y_arbol, _fuente_de  # noqa: E402
from tests.test_privacidad_proveedores_tecnicos import _extraer  # noqa: E402

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FAMILIA_TECNICO = ("tecnico", "tecnico_ejecutivo", "tecnico_externo", "tecnico_externo_jr", "tecnico_jr")
GESTION = ("superadmin", "admin", "supervisor", "ejecutivo")


def _leer(*partes):
    with open(os.path.join(RAIZ, *partes), encoding="utf-8") as fh:
        return fh.read()


# ───────────────────────── escaner estatico de rutas (regex, sin ast) ─────────────────────────
_RT = re.compile(r"^(\s*)@(\w+)\.(route|get|post)\((.*)")
_DEF = re.compile(r"^\s*def (\w+)")
_CACHE_RUTAS = {}


def _rutas(archivo):
    """[{fn, linea, ruta, decs, cuerpo}] de las rutas `@app.route/get/post` de un .py (CRLF o LF)."""
    if archivo in _CACHE_RUTAS:
        return _CACHE_RUTAS[archivo]
    src = _leer(archivo).replace("\r\n", "\n").split("\n")
    out, i, n = [], 0, len(src)
    while i < n:
        m = _RT.match(src[i])
        if m and m.group(2) == "app":
            j, decs = i, []
            while j < n and not _DEF.match(src[j]):
                if src[j].strip().startswith("@"):
                    decs.append(src[j].strip())
                j += 1
            fn = _DEF.match(src[j]).group(1)
            ind = len(src[j]) - len(src[j].lstrip())
            e = j + 1
            while e < n and (src[e].strip() == "" or len(src[e]) - len(src[e].lstrip()) > ind):
                e += 1
            out.append({
                "fn": fn, "linea": j + 1,
                "ruta": " ".join(d for d in decs if _RT.match(d)),
                "decs": [d for d in decs if not _RT.match(d)],
                "cuerpo": "\n".join(src[j:e]),
            })
            i = j
        i += 1
    _CACHE_RUTAS[archivo] = out
    return out


def _todas_las_rutas():
    out = []
    for archivo in sorted(os.listdir(RAIZ)):
        if archivo.endswith(".py") and not archivo.startswith(("test_", "_")):
            for r in _rutas(archivo):
                out.append(dict(r, archivo=archivo))
    return out


# Decoradores que EXCLUYEN a la familia tecnico (cada uno verificado en su definicion por las pruebas de abajo).
DECORADORES_GESTION = re.compile(
    r"_no_tecnico\b|_require_superadmin|_ot_can_metadata|_ot_can_finanzas_cierre|_ot_can_approve|_ot_can_cobertura|"
    r"_ot_can_eliminar|_facprov_puede|_intel_es_admin|_require_gestion_monitor|_tk_sin_cotizaciones_tecnico|"
    r"_tr_required|_otrep_compra_gestion_required|_catalogo_admin_required")

# Excepciones DOCUMENTADAS: la ruta no lleva un decorador de gestion, pero su cuerpo se lo niega al tecnico (se verifica
# que el texto del cuerpo trae el chequeo que dice la razon) o la ruta no maneja plata.
EXC_GUARDA_INTERNA = {
    # nombre de la funcion -> (token que debe aparecer en su cuerpo, razon)
    "ot2_api_estimar_costo": ("_es_rol_tecnico(", "403 al inicio para la familia tecnico"),
    "ot2_api_costo_sugerido": ("_es_rol_tecnico(", "403 al inicio para la familia tecnico"),
    "ot2_api_repuestos_costo": ("_es_rol_tecnico(", "403 al inicio para la familia tecnico"),
    "ot2_api_resultado_financiero": ("_es_rol_tecnico(", "403 al inicio para la familia tecnico"),
    "ot2_api_lineas_zz": ("_es_rol_tecnico(", "403 al inicio para la familia tecnico"),
    "ot_api_saldo_servicio_doc": ("_es_rol_tecnico(", "403 al inicio para la familia tecnico"),
    "ot_api_saldo_servicio_ot": ("_es_rol_tecnico(", "403 al inicio para la familia tecnico"),
    "ot_api_panorama": ("_es_rol_tecnico(", "403 al inicio para la familia tecnico"),
    "ot2_api_documentos_listar": ("_es_rol_tecnico(", "403 al inicio para la familia tecnico"),
    "mant_cliente_finanzas": ("_es_rol_tecnico(", "403 al inicio para la familia tecnico"),
    "repstock_solicitud_ot_costo": ("_es_rol_tecnico(", "403 al inicio para la familia tecnico"),
}
EXC_SIN_PLATA = {
    "pickup_dashboard": "Retiros: pesos y volumenes, nunca montos; lo gobierna el permiso 'retiros'",
}

PAT_NOMBRE = re.compile(r"finanz|costo|factura|autoriz|regulariz|dashboard|facprov|margen|valoriz", re.I)
PAT_RUTA = re.compile(
    r"/ot/autorizaciones|/ot/api/autorizaciones|/ot/regularizar|/ot/api/regularizar|/ot/finanzas|/ot/api/finanzas|"
    r"/dashboard|/api/dashboard/|facturas-proveedor|facturacion-proveedores|/api/tarifas|tecnicos-externos|"
    r"/api/contratos|vida-cliente|/finanzas|/ot/api/saldo|agente-contrato|/api/ia/")


class TestRutasFinancierasExigenGestion(unittest.TestCase):
    """Recorre TODAS las rutas de los .py del proyecto: las de nombre o ruta financiera deben llevar un decorador de
    gestion o estar en una lista blanca explicita (con la razon verificada contra su cuerpo)."""

    @classmethod
    def setUpClass(cls):
        cls.rutas = _todas_las_rutas()

    def _sin_gestion(self, patron_nombre, patron_ruta):
        malas = []
        for r in self.rutas:
            if not (patron_nombre.search(r["fn"]) or patron_ruta.search(r["ruta"])):
                continue
            if DECORADORES_GESTION.search(",".join(r["decs"])):
                continue
            if r["fn"] in EXC_SIN_PLATA:
                continue
            if r["fn"] in EXC_GUARDA_INTERNA:
                token, razon = EXC_GUARDA_INTERNA[r["fn"]]
                if token in r["cuerpo"] or token in ",".join(r["decs"]):
                    continue
            malas.append(f"{r['archivo']}:{r['linea']} {r['fn']} {r['ruta'][:70]} [{','.join(r['decs'])[:50]}]")
        return malas

    def test_hay_muchas_rutas_escaneadas(self):
        self.assertGreater(len(self.rutas), 900, "el escaner no encontro las rutas de app.py")
        financieras = [r for r in self.rutas if PAT_NOMBRE.search(r["fn"]) or PAT_RUTA.search(r["ruta"])]
        self.assertGreater(len(financieras), 70)

    def test_toda_ruta_financiera_exige_gestion_o_esta_en_la_lista_blanca(self):
        malas = self._sin_gestion(PAT_NOMBRE, PAT_RUTA)
        self.assertEqual(malas, [], "rutas financieras sin decorador de gestion ni excepcion documentada:\n  "
                         + "\n  ".join(malas))

    def test_las_excepciones_siguen_existiendo(self):
        nombres = {r["fn"] for r in self.rutas}
        for nombre in list(EXC_GUARDA_INTERNA) + list(EXC_SIN_PLATA):
            self.assertIn(nombre, nombres, f"{nombre} ya no existe: saca la excepcion de la lista blanca")

    def test_los_decoradores_de_gestion_negaban_a_la_familia_tecnico(self):
        """Cada decorador de la lista cierra de verdad: su definicion llama a _es_rol_tecnico / _rol_familia, o es el
        gate propio de gestion (superadmin, _puede_ot_accion('metadata'/'cobertura'/'aprobar'/'eliminar'))."""
        codigo = _leer("app.py")
        tk = _leer("tickets_module.py")
        cat = _leer("catalogo_module.py")
        piezas = {
            "_no_tecnico": (codigo, "def _no_tecnico(view)", "_es_rol_tecnico()"),
            "_tr_required": (codigo, "def _tr_required(fn)", "_es_rol_tecnico()"),
            "_facprov_puede": (codigo, "def _facprov_puede()", "_es_rol_tecnico("),
            "_otrep_compra_gestion_required": (codigo, "def _otrep_compra_gestion_required(view)", "_es_rol_tecnico()"),
            "_tk_sin_cotizaciones_tecnico": (tk, "def _tk_sin_cotizaciones_tecnico(view)", "_tk_es_tecnico()"),
            "_require_superadmin": (codigo, "def _require_superadmin(", "superadmin"),
            "_ot_can_metadata": (codigo, "def _ot_can_metadata(view_func)", '"metadata"'),
            "_ot_can_cobertura": (codigo, "def _ot_can_cobertura(view_func)", '"cobertura"'),
            "_ot_can_approve": (codigo, "def _ot_can_approve(view_func)", '"aprobar"'),
            "_ot_can_eliminar": (codigo, "def _ot_can_eliminar(view_func)", '"eliminar"'),
            "_ot_can_finanzas_cierre": (codigo, "def _ot_can_finanzas_cierre(view_func)", "_ot_puede_finanzas_cierre"),
        }
        for nombre, (texto, ancla, token) in piezas.items():
            i = texto.find(ancla)
            self.assertNotEqual(i, -1, f"no se encontro {ancla}")
            self.assertIn(token, texto[i:i + 2600], f"{nombre} no deniega a la familia tecnico (falta {token})")
        # y el chequeo de gestion de la OT nunca deja pasar a un tecnico en las acciones de plata
        fuente = _fuente_de("_ot_puede_finanzas_cierre")
        self.assertIn('_fam_u == "tecnico"', fuente)
        self.assertIn("return False", fuente)
        self.assertIn("_catalogo_admin_required", cat)

    def test_las_rutas_de_la_auditoria_tienen_su_decorador(self):
        """Lista explicita de lo que se cerro el 2026-10-08 (informe de la auditoria)."""
        por_nombre = {r["fn"]: r for r in self.rutas}
        cerradas = """mant_dashboard_costos_tecnico dashboard_hub mant_visitas_sin_facturar
            ot2_api_finanzas_corregir ot2_api_finanzas_correcciones ot2_finanzas_dudosas ot2_finanzas_modelo
            ot2_api_finanzas_modelo_copiar mant_facturacion_proveedores_legacy mant_facturacion_proveedores_xlsx_legacy
            mant_tarifas_servicios_list mant_tarifas_tecnicos_list mant_plantillas_tarifa_sugerida
            mant_tecnicos_externos_index mant_tecnico_externo_wizard mant_tecnico_externo_ficha
            mant_tecnicos_externos_list_api mant_tecnico_externo_crear mant_tecnico_externo_editar
            mant_tecnico_externo_eliminar mant_tecnico_externo_usuarios_listar mant_tecnicos_externos_usuarios_disponibles
            mant_tecnico_externo_usuario_asignar mant_tecnico_externo_usuario_quitar mant_tecnico_externo_invitar
            mant_tecnico_externo_subir_contrato mant_prov_docs_list mant_prov_docs_crear mant_prov_docs_borrar
            mant_tecnico_externo_subir_foto mant_repuestos_list mant_repuesto_crear mant_repuesto_crear_desde_erp
            mant_cliente_ai_analisis mant_plan_mejora mant_documento_erp mant_documento_erp_saldos mant_buscar_erp_sql
            mant_analisis mant_radar_data mant_notif_list mant_contrato_archivo mant_contrato_check_archivo
            mant_contrato_clausulas_get mant_contrato_clausulas mant_contrato_analisis_pdf mant_informe_ficha
            mant_contrato_analizar mant_contrato_update mant_contrato_subir mant_contrato_re_subir mant_contrato_delete
            mant_contrato_ai_editar mant_contrato_auto_calendar mant_agente_contrato mant_contrato_backfill_cloudinary
            mant_adjuntos_list mant_adjunto_subir mant_ia_alertas_diarias_manual""".split()
        faltan = [n for n in cerradas if n not in por_nombre]
        self.assertEqual(faltan, [], f"rutas que ya no existen: {faltan}")
        sin = [n for n in cerradas if "@_no_tecnico" not in [d.split("  #")[0].strip() for d in por_nombre[n]["decs"]]]
        self.assertEqual(sin, [], f"sin @_no_tecnico: {sin}")

    def test_cotizaciones_de_tickets_cerradas_al_tecnico(self):
        nombres = """tk_api_cotizacion_preview_clasificacion tk_api_cotizacion_preview_precio tk_api_cotizacion_costo_ruta
            tk_cotizacion_pdf tk_cotizacion_ver tk_cotizacion_detalle_calculo tk_cotizacion_detalle_calculo_pdf
            tk_cotizaciones_historico tk_api_cotizacion_desde_erp tk_api_cotizacion_recalcular
            tk_api_cotizacion_generar_ticket tk_api_cotizacion_get tk_api_cotizacion_buscar tk_api_cotizacion_actualizar
            tk_api_cotizacion_estado tk_api_cotizacion_eliminar tk_api_cotizacion_log tk_api_cotizacion_enviar""".split()
        por_nombre = {r["fn"]: r for r in _rutas("tickets_module.py")}
        for n in nombres:
            self.assertIn(n, por_nombre)
            self.assertIn("@_tk_sin_cotizaciones_tecnico", por_nombre[n]["decs"], f"{n} sin el candado de cotizaciones")
        # y el candado responde 403 a la familia tecnico (JSON) en vez de dejarla pasar
        tk = _leer("tickets_module.py")
        i = tk.find("def _tk_sin_cotizaciones_tecnico(view)")
        bloque = tk[i:i + 1700]
        self.assertIn("COTIZACION_SIN_ACCESO", bloque)
        self.assertIn("403", bloque)

    def test_no_tecnico_responde_json_403_en_cualquier_ruta_api(self):
        fuente = _fuente_de("_no_tecnico")
        self.assertIn('"/api/" in (request.path or "")', fuente)


# ───────────────────────── funciones puras ─────────────────────────
class TestAyudasDeConfidencialidad(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.G = types.SimpleNamespace(user=None, permissions={})
        amb = _extraer(
            ["_rol_familia", "_es_rol_tecnico", "_ot_sin_finanzas", "_ot_actividad_para_tecnico", "_mant_notif_tecnico_ok",
             "_erp_doc_sin_montos", "_facprov_puede"],
            extra={"g": cls.G, "re": re},
            constantes=("_OT_COLS_FINANZAS_TECNICO", "_ACT_TEC_PERMITIDAS", "_ACT_TEC_SIN_DETALLE",
                        "_ACT_TEC_PREFIJOS_VEDADOS", "_ACT_TEC_RE_MONTO", "_NOTIF_TEC_RE_VEDADO", "_ERP_CLAVES_PLATA_RE"))
        cls.amb = amb

    def _como(self, rol):
        self.G.user = {"role": rol}
        self.G.permissions = {"mant_facturacion_proveedores": True}
        return rol

    # ── fila de la OT ──
    def test_fila_de_la_ot_sin_plata(self):
        v = {"id": 7, "numero_ot": "OT-1", "tipo": "instalacion", "razon_social": "Gimnasio X",
             "costo": 1500000, "costo_proveedor": 900000, "costo_despacho": 50000, "zz_monto": 1200000,
             "zz_envio_monto": 80000, "valorizado_clp": 700000, "oc_numero": "OC-15", "estado_facturacion": "cotizado",
             "centro_costo": "sstt", "cobro_cero_argumento": "regalo", "cotizacion_nudo": "123", "titulo": "Instalar cinta"}
        out = self.amb["_ot_sin_finanzas"](v)
        for k in ("costo", "costo_proveedor", "costo_despacho", "zz_monto", "zz_envio_monto", "valorizado_clp", "oc_numero",
                  "estado_facturacion", "centro_costo", "cobro_cero_argumento", "cotizacion_nudo"):
            self.assertIsNone(out[k], k)
        for k in ("id", "numero_ot", "tipo", "razon_social", "titulo"):
            self.assertEqual(out[k], v[k], f"{k} (lo operativo) no se toca")
        self.assertEqual(v["costo"], 1500000, "no muta la fila original")

    # ── bitacora de la OT ──
    def test_bitacora_oculta_los_eventos_de_plata(self):
        f = self.amb["_ot_actividad_para_tecnico"]
        for accion in ("finanzas_corregidas", "finanzas_declaradas", "factura_proveedor_asignada", "factura_proveedor_quitada",
                       "costo_proveedor_corregido", "costo_espejo_cobro", "cobro_cero_retractado", "autorizacion_solicitada",
                       "creada_con_autorizacion", "saldo_servicio_ajustado", "saldo_servicio_excedido", "valor_por_documento",
                       "documento_asociado", "documento_quitado", "documento_regularizado", "documento_en_ot_cerrada",
                       "factura_asociada", "factura_ligada", "cotizacion_ligada", "oc_ligada", "nota_venta_reemplazada",
                       "finanzas_tras_firma_cliente", "interno_con_cliente", "garantia_retroactiva", "monto_ajustado",
                       "accion_nueva_que_nadie_mapeo"):
            self.assertFalse(f(accion, "Cobro del servicio 1 → 0 · Queda −200.000")[0], accion)

    def test_bitacora_ejemplos_literales_de_daniel(self):
        f = self.amb["_ot_actividad_para_tecnico"]
        for accion, det in (("finanzas_corregidas", "Cobro del servicio 1 → 0 … Queda −200.000"),
                            ("finanzas_declaradas", "centro=sstt doc=—"),
                            ("factura_proveedor_asignada", "FP-12 · $450.000"),
                            ("costo_proveedor_corregido", "monto ajustado"),
                            ("autorizacion_solicitada", "autorizó $0")):
            self.assertEqual(f(accion, det), (False, ""), accion)

    def test_bitacora_conserva_lo_operativo(self):
        f = self.amb["_ot_actividad_para_tecnico"]
        for accion in ("firmada_tecnico", "firmada_cliente", "ruta_iniciada", "adjunto_subido", "foto_eliminada",
                       "repuesto_solicitado", "tecnico_agregado", "diagnostico_guardado", "plantilla_aplicada",
                       "levantamiento_completado", "aprobada_supervisor", "informe_postservicio"):
            self.assertTrue(f(accion, "detalle normal")[0], accion)
        self.assertEqual(f("ruta_iniciada", "salio 09:10"), (True, "salio 09:10"))

    def test_bitacora_actualizada_quita_cobertura_y_plata_pero_deja_el_reagendamiento(self):
        f = self.amb["_ot_actividad_para_tecnico"]
        ok, det = f("actualizada", "REAGENDADA: 01/09/2026 → 05/09/2026 (+4 días) · motivo: cliente viaja · cobertura: pagado → garantia "
                                   "· estado: programada → en_ejecucion · cobro del servicio declarado a mano (antes rotulado «zz»)")
        self.assertTrue(ok)
        self.assertIn("REAGENDADA", det)
        self.assertIn("estado: programada", det)
        self.assertNotIn("cobertura", det)
        self.assertNotIn("cobro", det)
        ok, det = f("creada", "Instalar cinta · tipo=instalacion · modalidad=pagado · garantía=aplica · ⚠ revisar")
        self.assertTrue(ok)
        self.assertIn("tipo=instalacion", det)
        self.assertNotIn("modalidad", det)
        self.assertNotIn("garant", det)

    def test_bitacora_sin_detalle_para_ruta_y_anexo_y_monto_con_signo(self):
        f = self.amb["_ot_actividad_para_tecnico"]
        self.assertEqual(f("edicion_post_firma", "[X] u modificó la OT — POST /ot/api/finanzas/12 (cobertura)"), (True, ""))
        self.assertEqual(f("anexo_enviado", "a x@y.cl")[1], "")
        ok, det = f("repuesto_solicitado", "#4 Rodamiento × 2 · ticket TK-1 · costo $25.000")
        self.assertTrue(ok)
        self.assertNotIn("25.000", det)
        self.assertIn("[oculto]", det)

    def test_toda_accion_de_visita_logueada_con_plata_queda_fuera_de_la_lista_blanca(self):
        """Estatico: las acciones que el codigo registra con `_mant_log("visita", ..., "<accion>")` y hablan de plata no
        pueden estar en la lista blanca del tecnico."""
        codigo = _leer("app.py")
        acciones = set(re.findall(r'_mant_log\(\s*"visita",[^,]+,\s*"([a-z_0-9]+)"', codigo))
        self.assertGreater(len(acciones), 30)
        plata = re.compile(r"finanz|factura|costo|cobro|valor|saldo|autoriz|oc_|cotiz|documento|nota_venta|monto|espejo")
        permitidas = self.amb["_ACT_TEC_PERMITIDAS"]
        colados = sorted(a for a in acciones if a in permitidas and plata.search(a) and a != "equipos_alta_desde_documento")
        self.assertEqual(colados, [], f"acciones de plata en la lista blanca del tecnico: {colados}")

    # ── campana ──
    def test_campana_del_tecnico_sin_plata(self):
        ok = self.amb["_mant_notif_tecnico_ok"]
        self.assertTrue(ok("ot_asignada", "Te asignaron la OT OT-2026-00300", "Cliente Gimnasio X · 10/10/2026"))
        self.assertTrue(ok("otro", "Te asignaron el ticket TK-12", "Revisa el ticket"))
        for t, c in (("OT-1: el estimado quedó lejos del valor real", "Se declaró en $200.000 y ahora vale $400.000"),
                     ("Autorización pendiente: cobro $0", "Daniel pide: regalía"),
                     ("Factura de proveedor sin pagar", "FP-12"),
                     ("Orden de compra OC-15", "al proveedor DAP"),
                     ("Margen negativo en la OT", "perdida de 200.000")):
            self.assertFalse(ok("otro", t, c), t)

    # ── facprov ──
    def test_facturacion_de_proveedores_nunca_para_un_tecnico_aunque_el_rol_tenga_el_permiso(self):
        puede = self.amb["_facprov_puede"]
        for rol in FAMILIA_TECNICO:
            self._como(rol)
            self.assertFalse(puede(), rol)
        self._como("superadmin")
        self.assertTrue(puede())
        self._como("ejecutivo")
        self.assertTrue(puede(), "gestion con el permiso encendido no pierde nada")
        self.G.permissions = {}
        self.assertFalse(puede())

    # ── ERP ──
    def test_documento_erp_sin_importes_para_el_tecnico(self):
        f = self.amb["_erp_doc_sin_montos"]
        resp = {"hdr": {"tido": "FCV", "nudo": "1234", "cliente_nombre": "Gym", "valor_neto": 100, "valor_bruto": 119,
                        "valor_iva": 19, "raw_sample": {"VANEDO": "100"}},
                "lineas": [{"sku": "A1", "nombre": "Cinta", "cantidad": 2, "saldo": 1, "precio_unitario": 50, "vaneli": 100,
                            "descuento": 5, "peso_kg_tot": 80.0, "stock": 4}]}
        out = f(resp)
        self.assertIsNone(out["hdr"]["valor_neto"])
        self.assertIsNone(out["hdr"]["valor_bruto"])
        self.assertEqual(out["hdr"]["cliente_nombre"], "Gym")
        self.assertEqual(out["hdr"]["raw_sample"], {})
        ln = out["lineas"][0]
        self.assertEqual((ln["sku"], ln["nombre"], ln["cantidad"], ln["saldo"], ln["stock"], ln["peso_kg_tot"]),
                         ("A1", "Cinta", 2, 1, 4, 80.0))
        for k in ("precio_unitario", "vaneli", "descuento"):
            self.assertNotIn(k, ln)
        self.assertEqual(resp["hdr"]["valor_neto"], 100, "no muta la respuesta cacheada")


# ───────────────────────── contratos de las rutas con doble ─────────────────────────
class _Resp:
    def __init__(self, data, status=200):
        self.data, self.status = data, status


class TestNoTecnicoDecorador(unittest.TestCase):
    """@_no_tecnico con la familia completa: 403 JSON en rutas /api/ (de cualquier prefijo); gestion pasa."""

    @classmethod
    def setUpClass(cls):
        cls.G = types.SimpleNamespace(user=None, permissions={})
        cls.req = types.SimpleNamespace(headers={}, is_json=False, path="/ot/api/finanzas/1/corregir")
        amb = _extraer(
            ["_rol_familia", "_es_rol_tecnico", "_no_tecnico"],
            extra={"g": cls.G, "request": cls.req, "wraps": __import__("functools").wraps,
                   "jsonify": lambda d: d, "flash": lambda *a, **k: None, "redirect": lambda u: ("redirect", u),
                   "url_for": lambda n, **k: "/" + n})
        cls.vista = staticmethod(amb["_no_tecnico"](lambda: "OK"))

    def _llamar(self, rol, ruta):
        self.G.user = {"role": rol}
        self.req.path = ruta
        return self.vista()

    def test_la_familia_tecnico_recibe_403_json_en_rutas_api_de_cualquier_prefijo(self):
        for rol in FAMILIA_TECNICO:
            for ruta in ("/ot/api/finanzas/1/corregir", "/mantenciones/api/dashboard/costos-tecnico",
                         "/servicio-tecnico/api/dashboard/costos-tecnico", "/ot/api/autorizaciones",
                         "/tickets/api/cotizaciones/3", "/api/erp/documento"):
                r = self._llamar(rol, ruta)
                self.assertIsInstance(r, tuple, f"{rol} {ruta}: {r!r}")
                self.assertEqual(r[1], 403, f"{rol} {ruta}")
                self.assertFalse(r[0]["ok"])

    def test_la_familia_tecnico_es_redirigida_en_las_paginas(self):
        for rol in FAMILIA_TECNICO:
            self.assertEqual(self._llamar(rol, "/ot/regularizar"), ("redirect", "/mant_ots_list"), rol)
            self.assertEqual(self._llamar(rol, "/servicio-tecnico/facturas-proveedor"), ("redirect", "/mant_ots_list"), rol)

    def test_gestion_no_pierde_nada(self):
        for rol in GESTION:
            self.assertEqual(self._llamar(rol, "/ot/api/finanzas/1/corregir"), "OK", rol)
            self.assertEqual(self._llamar(rol, "/ot/regularizar"), "OK", rol)


# ───────────────────────── revision del codigo de las pantallas ─────────────────────────
class TestFichaDeLaOtParaElTecnico(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.f = _fuente_de("ot2_detalle")

    def test_la_fila_y_el_contexto_salen_sin_plata_para_el_tecnico(self):
        f = self.f
        i = f.rfind("    if _es_rol_tecnico():\n        v = _ot_sin_finanzas(v)")
        self.assertNotEqual(i, -1, "falta el bloque que limpia el contexto del tecnico antes del render")
        bloque = f[i:i + 800]
        for pedazo in ("valor_proyecto = {", "_fin_ok, _fin_faltan = True, []", "doc_header = None", "puerta_cierre = None",
                       "factura_prov, factura_prov_aplica = None, False"):
            self.assertIn(pedazo, bloque)
        self.assertLess(i, f.index('"ot2/detalle.html"'), "el bloque va ANTES del render")

    def test_la_bitacora_del_tecnico_usa_la_lista_blanca(self):
        self.assertIn("_ot_actividad_para_tecnico(_acc, _det)", self.f)
        self.assertIn("if not _act_ok:", self.f)

    def test_fin_nunca_se_calcula_para_el_tecnico(self):
        self.assertIn("fin = fin_base = fin_rep = None\n    if not _es_rol_tecnico():", self.f)


class TestOtrasPantallasParaElTecnico(unittest.TestCase):
    def test_calendario_eventos_sin_costo_ni_estado_de_facturacion(self):
        f = _fuente_de("mant_visitas_api")
        self.assertIn('"costo":       (None if _es_rol_tecnico() else float(r.get("costo") or 0))', f)
        self.assertIn('"estado_facturacion": (None if _es_rol_tecnico() else r.get("estado_facturacion"))', f)
        self.assertIn("if rows and not _es_rol_tecnico():", f, "las cuentas del calendario solo para gestion")

    def test_calendario_dia_sin_costo(self):
        f = _fuente_de("mant_calendario_dia_drill")
        self.assertIn('v.pop("costo", None)', f)
        self.assertIn('v.pop("costo_real", None)', f)

    def test_panel_de_ot_sin_factura_de_proveedor(self):
        f = _fuente_de("_ot2_enriquecer_fila")
        self.assertIn('f["fac_numero"] = f["fac_estado"] = None', f)
        self.assertIn("_es_rol_tecnico()", f)

    def test_campana_lista_y_contador(self):
        lista = _fuente_de("mant_notif_interna_list")
        self.assertIn("_es_tec_n = _es_rol_tecnico()", lista)
        self.assertIn("_mant_notif_tecnico_ok(", lista)
        cont = _fuente_de("mant_notif_interna_contador")
        self.assertIn("_mant_notif_tecnico_ok(", cont)
        self.assertIn("destino_user_id=%s", cont)

    def test_lista_de_tecnicos_sin_tarifas_ni_datos_de_empresas(self):
        f = _fuente_de("mant_tecnicos_list_api")
        self.assertIn("_oculta_proveedores()", f)
        for k in ("tarifa_visita", "empresa_rut", "empresa_email", "empresa_tel"):
            self.assertIn(f'"{k}"', f)

    def test_verificar_documento_erp_sin_monto_para_el_tecnico(self):
        f = _fuente_de("mant_erp_doc_info")
        self.assertIn('"monto": (None if _es_rol_tecnico() else _monto)', f)

    def test_documento_unificado_erp_sin_importes_para_el_tecnico(self):
        f = _fuente_de("erp_documento_unificado")
        self.assertEqual(f.count("_erp_doc_sin_montos("), 2, "tanto la respuesta cacheada como la nueva")

    def test_transporte_no_abre_a_la_familia_tecnico(self):
        f = _fuente_de("_tr_required")
        self.assertIn("_es_rol_tecnico()", f)

    def test_dashboard_no_se_ofrece_a_tecnicos_y_su_fuente_de_plata_exige_gestion(self):
        base = _leer("templates", "base.html")
        i = base.index("bi bi-speedometer2")
        self.assertIn("{% if not es_tecnico %}", base[i - 500:i], "el link del Dashboard va dentro de not es_tecnico")
        self.assertRegex(base, r"permissions\.mant_facturacion_proveedores or permissions\.superadmin\) and not es_tecnico")
        self.assertIn("{% if not es_tecnico %}\n    <a href=\"{{ url_for('ot_regularizar_pagina') }}\"",
                      base.replace("\r\n", "\n"))
        self.assertIn("{% if not es_tecnico %}\n      <a href=\"{{ url_for('mant_tecnicos_externos_index') }}\"",
                      base.replace("\r\n", "\n"))

    def test_calendario_no_dibuja_el_costo_para_el_tecnico(self):
        cal = _leer("templates", "mantenciones", "calendario.html").replace("\r\n", "\n")
        i = cal.index('id="vi_info_costo"')
        self.assertIn("{% if not es_tecnico %}", cal[i - 400:i])
        self.assertIn("const _viInfoCosto = document.getElementById('vi_info_costo');", cal)
        self.assertIn("if (_viInfoCosto)", cal)


class TestDashboardDeServicioTecnico(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.html = _leer("templates", "mantenciones", "index.html").replace("\r\n", "\n")

    def test_plantilla_valida_para_jinja(self):
        import jinja2
        jinja2.Environment(extensions=["jinja2.ext.do", "jinja2.ext.loopcontrols"]).parse(self.html)

    def test_lo_que_lleva_plata_solo_para_gestion(self):
        h = self.html
        for ancla in ("MRR contratos", "Visitas sin facturar", 'class="t d-block">Análisis Económico'):
            i = h.index(ancla)
            previo = h[max(0, i - 2600):i]
            self.assertIn("not es_tecnico", previo, f"{ancla} debe ir dentro de un 'not es_tecnico'")
        self.assertIn("{% if facprov and not es_tecnico %}", h)
        self.assertIn("{% if permissions.superadmin and not es_tecnico %}", h)

    def test_conserva_todas_las_secciones_y_ids(self):
        h = self.html
        for pedazo in ("Clientes activos", "Contratos vigentes", "Vencen pronto", "Proveedores externos", "Costos por técnico",
                       "Visitas sin facturar", "Próximas Órdenes de Trabajo", "Alertas de contratos",
                       "Clientes sin visita reciente", "Análisis &amp; Analytics", "Analytics Operacional",
                       'id="pfList"', 'id="pfBadge"', 'id="pfDiasMin"', 'id="autoCreadasBanner"', 'id="cptTecnico"',
                       "abrirReporteExcel()", "abrirVisitaHistDashboard()", "modalResetMant", 'id="sugBadgeIndex"',
                       "modalVisitaHistDashboard", "cargarPendientesFacturar", "mant_tecnicos_externos_index",
                       "mant_ots_auto_creadas_page", "retiros_monitor.css"):
            self.assertIn(pedazo, h, f"se perdio {pedazo}")

    def test_usa_el_lenguaje_de_incidencias_y_retiros(self):
        h = self.html
        for pedazo in ("inc-hbtn", "rm-kpi", "rm-table", "rm-wrap", "data-alerta", "rm-pill"):
            self.assertIn(pedazo, h)
        self.assertNotIn("table-responsive", h)
        self.assertNotIn("mant-table", h.split("{% block content %}")[1], "las tablas ya no son .mant-table")


class TestTablasDelLote(unittest.TestCase):
    """Daniel 2026-10-08: «esa tabla del lote sin margenes, toda pegada, se ve fea»."""

    def test_detalle_del_lote_en_una_tarjeta_con_aire(self):
        h = _leer("templates", "mantenciones", "factura_proveedor_detalle.html").replace("\r\n", "\n")
        self.assertIn('<div class="fpd-tabla-card"><div class="table-responsive">', h)
        self.assertIn(".fpd-tabla-card{background:#f8fafc;border:1px solid #e5e7eb;border-radius:14px;padding:6px 16px 12px", h)
        self.assertIn(".fpd-tabla tbody td{padding:11px 10px}", h)
        self.assertIn(".fpd-tabla tbody td:first-child,.fpd-tabla thead th:first-child{padding-left:14px}", h)

    def test_lista_de_lotes_con_aire(self):
        h = _leer("templates", "mantenciones", "facturas_proveedor.html").replace("\r\n", "\n")
        self.assertIn('<div class="fpl-card"><div class="table-responsive">', h)
        self.assertIn("#fpvTabla tbody td{padding:8px 6px;", h)
        self.assertIn("#fpvTabla thead th:first-child,#fpvTabla tbody td:first-child{padding-left:16px}", h)
        self.assertIn("@media (min-width:1360px){", h)
        self.assertIn("  .fp-tabla-ot tbody td{padding:11px 9px}", h)


if __name__ == "__main__":
    unittest.main()
