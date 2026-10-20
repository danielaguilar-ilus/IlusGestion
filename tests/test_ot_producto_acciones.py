# -*- coding: utf-8 -*-
"""🧰 2026-10-20 — Barra de acciones de cada equipo y producto de la OT (Daniel, con los técnicos operando la revisión de la
NVI 687 y el caso OT-2026-00171 de Aurum).

  «antes de comenzar los checklist los técnicos tengan la posibilidad de darle de baja, de dejar fuera de servicio, de indicar
   que no llegó, de solicitar piola o cualquier repuesto» · «antes de cada checklist estaba el menú… Ese menú lo requiero para
   cada equipo y poder indicar alguna incidencia» · «algo choro, algo espectacular… esta es nuestra página de orgullo, y
   recuerda que sea responsivo» · «esas desviaciones que suceden y para las que debemos estar preparados, como que no está
   el equipo… déjalo bien entretenido».

Se prueba:
  1. El módulo puro ot_producto_estado.py: qué exige cada acción (foto, detalle, motivo de lista, riesgo), cuándo un producto
     deja de exigir su checklist, el resultado del acta, el seguimiento de 4 pasos, los sugeridos por defecto (REGLA #32) y la
     salida «No está / no llegó» de un equipo de cliente (falta_stock incluida).
  2. REGLA #32: el arranque ya no modifica plantillas; las propuestas son solo lectura, de gestión y nunca para «(LU)».
  3. Cableado de app.py: rutas, candados, tabla, «pendientes por falta de stock» para el trabajo de la OT hija, idempotencia
     del pedido de repuestos, incidencia sin candado de módulo (decisión de Daniel) y nada de plata ni proveedores (REGLA #19/#26).
  4. Espejo JS ↔ Python del dispensado del checklist (otdChkEvaluar ↔ _chk_revision_evaluar con `estados`).
  5. Front: la barra va ANTES del checklist en el <summary>, UN solo contenedor de pantalla completa y UN solo JSON (la ficha de
     19 productos × 9 tareas no puede pesar más), las mismas palabras del prototipo aprobado, pulso suave con
     prefers-reduced-motion, 44 px, sin popups nativos.

Sin BD, sin Flask, sin ERP (REGLA #4.1), sin correos reales (REGLA #22).
Correr:  python -m pytest tests/test_ot_producto_acciones.py -q -p no:warnings
"""
import json
import os
import re
import shutil
import subprocess
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import ot_producto_estado as pe

from tests import test_ot_revision_esencial as E
from tests.test_fac_libro_saldo import _exec
from tests.test_ot_revision_v3 import HAY_NODE, _js_funcion, _node
from tests.test_ot_saldo_servicio import _arbol, _decoradores, _fuente, _leer

D = _leer("templates/ot2/detalle.html")
P = _leer("templates/mantenciones/plantillas.html")
FICHA = _leer("templates/mantenciones/ficha.html")
MAQ_FICHA = _leer("templates/mantenciones/maquina_ficha.html")
ACC_JS = _leer("static/ot_acciones.js")
ACC_CSS = _leer("static/ot_acciones.css")
FS_CSS = _leer("static/eq_fuera_servicio.css")
_PLATA = re.compile(r"proveedor|costo|precio|margen|\$\s?\d|valorizado|factura de proveedor|n[°º] de oc", re.I)


def _fila(estado, **kw):
    f = {"id": 1, "estado": estado, "activo": 1, "cantidad": 1, "cantidad_total": 1}
    f.update(kw)
    return f


# ═══════════════════════════════ 1) el módulo puro ═══════════════════════════════
class TestAccionesDeUnProducto(unittest.TestCase):
    def test_fuera_de_servicio_pide_motivo_detalle_riesgo_y_foto(self):
        base = {"motivo_clave": "cable", "detalle": "Se cortó la piola del selector", "riesgo": "si"}
        ok, err = pe.normalizar("fuera_servicio", base, total=1, n_fotos=1)
        self.assertIsNone(err)
        self.assertEqual((ok["motivo"], ok["riesgo_lesion"], ok["cantidad"]), ("Cable o piola cortada", 1, 1))
        casos = [({"motivo_clave": ""}, 1, "MOTIVO_REQUERIDO"), ({"motivo_clave": "cable", "detalle": "corto"}, 1, "DETALLE_REQUERIDO"),
                 (dict(base, riesgo=""), 1, "RIESGO_REQUERIDO"), (base, 0, "FOTO_REQUERIDA")]
        for datos, fotos, codigo in casos:
            self.assertEqual(pe.normalizar("fuera_servicio", datos, 1, fotos)[1][1], codigo, codigo)
        self.assertEqual(pe.normalizar("fuera_servicio", dict(base, riesgo="no"), 1, 1)[0]["riesgo_lesion"], 0)

    def test_no_tiene_arreglo_pide_foto_diagnostico_y_motivo_de_la_lista(self):   # Daniel 20-oct
        base = {"motivo_clave": "estructura", "detalle": "El marco está fisurado en la soldadura"}
        self.assertIsNone(pe.normalizar("baja", base, 1, 1)[1])
        self.assertEqual(pe.normalizar("baja", base, 1, 0)[1][1], "FOTO_REQUERIDA")
        self.assertEqual(pe.normalizar("baja", dict(base, detalle="corto"), 1, 1)[1][1], "DIAGNOSTICO_REQUERIDO")
        self.assertEqual(pe.normalizar("baja", dict(base, motivo_clave="inventado"), 1, 1)[1][1], "MOTIVO_REQUERIDO")

    def test_no_llego_no_lleva_foto_y_siempre_dice_a_quien_se_le_pregunto(self):
        ok, err = pe.normalizar("no_llego", {"consultado_clave": "bodega", "consultado_detalle": "Pedro", "cantidad": 2}, 6, 0)
        self.assertIsNone(err)
        self.assertEqual((ok["consultado"], ok["cantidad"], ok["cantidad_total"]), ("Bodega (Pedro)", 2, 6))
        self.assertEqual(pe.normalizar("no_llego", {"consultado_clave": ""}, 1, 0)[1][1], "CONSULTADO_REQUERIDO")
        self.assertEqual(pe.normalizar("no_llego", {"consultado_clave": "nadie", "cantidad": 9}, 6, 0)[1][1], "CANTIDAD_INVALIDA")
        self.assertIsNone(pe.normalizar("no_llego", {"consultado_clave": "nadie"}, 1, 0)[1])   # «todavía no pregunté» vale

    def test_solo_una_accion_que_cubre_todo_dispensa_el_checklist(self):
        self.assertTrue(pe.dispensa_checklist(_fila("no_llego", cantidad=6, cantidad_total=6)))
        self.assertFalse(pe.dispensa_checklist(_fila("no_llego", cantidad=2, cantidad_total=6)))      # parcial
        self.assertFalse(pe.dispensa_checklist(_fila("baja", activo=0)))                              # revertida
        self.assertFalse(pe.dispensa_checklist(None))
        filas = [dict(_fila("fuera_servicio"), item_manual_id=3, id=5), dict(_fila("baja"), item_manual_id=3, id=9),
                 dict(_fila("no_llego", cantidad=1, cantidad_total=2), item_manual_id=4, id=6)]
        self.assertEqual({k: v["estado"] for k, v in pe.mapa_dispensados(filas).items()}, {3: "baja"})  # manda la más reciente

    def test_resultado_del_acta_y_nota_parcial(self):
        self.assertEqual(pe.resultado_acta(_fila("fuera_servicio")), "Fuera de servicio")
        self.assertEqual(pe.resultado_acta(_fila("baja")), "Dar de baja")
        self.assertEqual(pe.resultado_acta(_fila("no_llego")), "No llegó")
        self.assertIsNone(pe.resultado_acta(_fila("no_llego", cantidad=1, cantidad_total=3)))
        self.assertEqual(pe.nota_parcial(_fila("no_llego", cantidad=1, cantidad_total=3)), "No llegó 1 de 3")
        self.assertEqual(pe.nota_parcial(_fila("no_llego", cantidad=2, cantidad_total=6)), "No llegaron 2 de 6")
        self.assertIn("PUEDE LASTIMAR A ALGUIEN", pe.resumen_linea(_fila("fuera_servicio", riesgo_lesion=1, motivo="Cable")))

    def test_seguimiento_del_pedido_en_las_palabras_del_prototipo(self):
        self.assertEqual([t for _k, t in pe.PASOS_PEDIDO],
                         ["Tu pedido", "Revisión de bodega", "Pieza lista en bodega", "Pieza instalada"])
        # «Tu pedido» ya está hecho en cuanto se envía; lo que sigue es la revisión de bodega
        s = pe.seguimiento_pedido("solicitado")
        self.assertEqual([p["estado"] for p in s["pasos"]], ["hecho", "actual", "pendiente", "pendiente"])
        s = pe.seguimiento_pedido("validado")
        self.assertEqual([p["estado"] for p in s["pasos"]], ["hecho", "hecho", "actual", "pendiente"])
        s = pe.seguimiento_pedido("recibido")
        self.assertEqual([p["estado"] for p in s["pasos"]], ["hecho", "hecho", "hecho", "actual"])
        s = pe.seguimiento_pedido("instalado")
        self.assertTrue(all(p["estado"] == "hecho" for p in s["pasos"]))
        self.assertTrue(pe.seguimiento_pedido("rechazado")["rechazado"])

    def test_sugeridos_regla_32_la_plantilla_solo_reemplaza_si_una_persona_la_lleno(self):
        defecto, origen = pe.sugeridos_para([], "selector_peso")
        self.assertEqual(origen, "sistema")
        self.assertIn("Piola (cable de acero)", defecto)
        escrita, origen = pe.sugeridos_para([["Tambor especial", "tambor especial"]], "selector_peso")
        self.assertEqual((escrita, origen), (["Tambor especial"], "plantilla"))
        self.assertEqual(pe.sugeridos_para([[], None], "no_existe"), ([], "ninguno"))
        for lista in pe.SUGERIDOS_POR_SISTEMA.values():
            for n in lista:                                                    # solo NOMBRES de pieza: jamás un costo
                self.assertIsNone(_PLATA.search(n), n)
        self.assertEqual(pe.limpiar_lista(["Cinta $45.000", "Cinta"]), ["Cinta"])


class TestNoEstaNoLlegoDeUnEquipo(unittest.TestCase):
    """🧰 2026-10-20 (caso OT-2026-00171 de Aurum: dos accesorios no llegaron por falta de stock)."""

    def test_tres_opciones_cada_una_con_su_razon(self):
        self.assertEqual(pe.RAZON_SALTADO_DE, {"no_esta": "no_encontrado", "falta_stock": "falta_stock"})
        ok, err = pe.normalizar_equipo("no_esta", {"motivo_clave": "no_estaba"}, 0)
        self.assertIsNone(err)
        self.assertEqual((ok["razon_saltado"], ok["cantidad"], ok["estado"]), ("no_encontrado", 1, "no_esta"))
        ok, _e = pe.normalizar_equipo("falta_stock", {"motivo_clave": "sin_stock"}, 0)
        self.assertEqual((ok["razon_saltado"], ok["observacion"]), ("falta_stock", "No hay stock en bodega"))
        ok, _e = pe.normalizar_equipo("no_revisado", {"motivo_clave": "sin_acceso"}, 0)
        self.assertEqual(ok["razon_saltado"], "sin_acceso")                      # «Otra razón» = las razones de siempre
        ok, _e = pe.normalizar_equipo("no_revisado", {"motivo_clave": "otro", "detalle": "El cliente cerró la sala"}, 0)
        self.assertEqual((ok["razon_saltado"], ok["motivo"]), ("otro", "Otro motivo"))

    def test_motivo_de_lista_obligatorio_y_detalle_solo_con_otro(self):
        self.assertEqual(pe.normalizar_equipo("no_esta", {}, 0)[1][1], "MOTIVO_REQUERIDO")
        self.assertEqual(pe.normalizar_equipo("falta_stock", {"motivo_clave": "inventado"}, 0)[1][1], "MOTIVO_REQUERIDO")
        self.assertEqual(pe.normalizar_equipo("no_esta", {"motivo_clave": "otro", "detalle": "corto"}, 0)[1][1], "DETALLE_REQUERIDO")
        self.assertIsNone(pe.normalizar_equipo("no_esta", {"motivo_clave": "nadie_sabe"}, 0)[1])
        self.assertEqual(pe.normalizar_equipo("inexistente", {"motivo_clave": "x"}, 0)[1][1], "ACCION_INVALIDA")

    def test_la_foto_es_opcional_salvo_que_no_llego_no_lleva(self):
        self.assertEqual(pe.normalizar_equipo("falta_stock", {"motivo_clave": "sin_stock"}, 3)[0]["fotos_validas"], 0)
        self.assertEqual(pe.normalizar_equipo("no_esta", {"motivo_clave": "no_estaba"}, 2)[0]["fotos_validas"], 2)
        self.assertEqual(pe.normalizar_equipo("no_revisado", {"motivo_clave": "sin_acceso"}, 0)[0]["fotos_validas"], 0)

    def test_dado_de_baja_no_es_una_razon_de_no_pude_revisarlo(self):
        claves = {k for k, _t in pe.motivos_equipo("no_revisado")}
        self.assertNotIn("dado_de_baja", claves)                                # la baja de verdad es «No tiene arreglo»

    def test_chip_del_resumen_de_la_tarjeta(self):
        self.assertEqual(pe.chip_equipo({"estado": "falta_stock", "creado_por": "Isabel Rojas"}, "20/10"),
                         "No llegó · falta stock · 20/10 · Isabel")
        self.assertEqual(pe.chip_equipo({"estado": "no_esta", "creado_por": "Daniel"}, "20/10"), "No está en el lugar · 20/10 · Daniel")
        self.assertEqual(pe.chip_equipo({"estado": "fuera_servicio"}, "20/10"), "")        # de un producto: no es de equipo


# ═══════════════════════════════ 2) REGLA #32 y las propuestas ═══════════════════════════════
class TestReglaTreintaYDos(unittest.TestCase):
    def test_el_arranque_ya_no_modifica_plantillas(self):
        app = _leer("app.py")
        for muerto in ("def _ensure_bloques_revision_esencial", "def _ensure_foto_entrega_instalacion",
                       "def _ensure_repuestos_sugeridos_seed", "_CHK_REPUESTOS_SUGERIDOS_SEED", "_CHK_ESENCIAL_MARCA"):
            self.assertNotIn(muerto, app, muerto)
        # lo que SÍ corre al arrancar en la revisión es solo crear columnas y tablas propias (baratas)
        i = app.index("        _ensure_repuestos_sugeridos_col()\n        _ensure_otrep_item_manual_col()")
        bloque = app[i:i + 400]
        self.assertIn("_ensure_ot_producto_estado()", bloque)
        self.assertNotIn("UPDATE mant_tarea_plantilla", bloque)

    # Migraciones de arranque ANTERIORES a esta rama (ya estaban en main): siguen escribiendo en plantillas. NO las tocó este trabajo
    # (REGLA #4.2: no se quita nada sin permiso de Daniel); se listan para que ninguna NUEVA se cuele sin pasar por una propuesta.
    ESCRITORES_HEREDADOS = {
        "_ensure_plantillas_estandar_seed", "_ensure_checklist_herencia", "_ensure_plantillas_nombre_sin_prefijo",
        "_ensure_categoria_admin_plantillas", "_ensure_plantilla_capacitacion_rica", "_ensure_plantilla_repuesto",
        "_ensure_plantilla_aplica_plan_mantencion", "_ensure_plantillas_autorrelleno", "_ensure_plantillas_matriz_clases",
        "_ensure_levantamiento_target_field", "_ensure_tipo_respuesta_asistencia", "_ensure_marcar_revision_tecnica_28ago",
    }

    def test_ninguna_migracion_nueva_escribe_en_plantillas(self):
        """🎛️ REGLA #32: lo que el sistema quiera cambiar en una plantilla existente es una PROPUESTA (vista previa + decisión de una
        persona), nunca una sentencia de arranque. Esta prueba falla si aparece un _ensure_* NUEVO que escriba en mant_tarea_plantillas
        o mant_tarea_plantilla_items."""
        patron = re.compile(r"(UPDATE\s+mant_tarea_plantilla(?:s|_items)\b|DELETE\s+FROM\s+mant_tarea_plantilla(?:s|_items)\b|"
                            r"INSERT\s+(?:IGNORE\s+)?INTO\s+mant_tarea_plantilla(?:s|_items)\b)", re.I)
        escritores, actual, cuerpo = set(), None, []
        for linea in _leer("app.py").split("\n") + ["def _fin():"]:
            m = re.match(r"def (_ensure_\w+)\(", linea)
            if m or (actual and re.match(r"(def |@|[A-Za-z_])", linea)):
                if actual and patron.search("\n".join(cuerpo)):
                    escritores.add(actual)
                actual, cuerpo = (m.group(1) if m else None), []
                continue
            if actual:
                cuerpo.append(linea)
        nuevos = escritores - self.ESCRITORES_HEREDADOS
        self.assertFalse(nuevos, f"REGLA #32: una migración de arranque nueva escribe en plantillas: {sorted(nuevos)}. "
                                 "Conviértela en una propuesta en /mantenciones/plantillas.")
        # las tres de la revisión (esencial, foto de entrega, siembra de sugeridos) ya no están
        self.assertFalse(escritores & {"_ensure_bloques_revision_esencial", "_ensure_foto_entrega_instalacion",
                                       "_ensure_repuestos_sugeridos_seed"})

    def test_las_propuestas_son_solo_lectura_de_gestion_y_nunca_lu(self):
        from_construir = _fuente("_prop_construir")
        for escribe in ("INSERT", "UPDATE", "DELETE"):
            self.assertNotIn(escribe, from_construir)
        self.assertEqual(_decoradores("mant_plantillas_propuestas")[1:], ["_mant_required", "_no_tecnico_externo"])
        self.assertEqual(_decoradores("mant_plantillas_propuestas_aplicar")[1:], ["_mant_required", "_no_tecnico_externo"])
        f = _fuente("_prop_aplicar")
        self.assertIn("if not puede_reemplazar_usuario:", f)
        self.assertIn('if p["es_lu"]:', f)
        self.assertIn("PLANTILLA_CAMBIO", f)
        self.assertLess(f.index("reemplazada_desde_propuesta"), f.index("DELETE FROM mant_tarea_plantilla_items"))   # REGLA #5

    def test_lu_de_lenin_jamas_se_reemplaza(self):
        k = _exec_prop()
        self.assertTrue(k["_prop_es_lu"]("(LU) Selector de peso"))
        self.assertTrue(k["_prop_es_lu"]("  (lu) Accesorios"))
        self.assertFalse(k["_prop_es_lu"]("Revisión — Selector de peso"))
        it = [{"titulo": "Foto", "tipo_respuesta": "foto", "obligatoria": 1, "requiere_foto": 1}]
        p = k["_prop_armar"]("esencial", 7, "(LU) Selector de peso", it, it + it, "sin_editar", True)
        self.assertFalse(p["puede_reemplazar"])
        self.assertTrue(p["es_lu"])
        p = k["_prop_armar"]("esencial", 8, "Revisión — Carga de discos", it, it + it, "sin_editar", True)
        self.assertTrue(p["puede_reemplazar"])
        p = k["_prop_armar"]("esencial", 8, "Revisión — Carga de discos", it, it + it, "sin_editar", False)
        self.assertFalse(p["puede_reemplazar"])                                  # un técnico solo puede crear una nueva

    def test_la_pantalla_de_plantillas_deja_decidir(self):
        for pieza in ("abrirPropuestas()", 'id="modalPropuestas"', "Propuestas del sistema", "Crear como plantilla nueva",
                      "Reemplazar el contenido", "(recomendado)", "ilusConfirm(", "'/mantenciones/api/plantillas/propuestas/aplicar'",
                      "huella: p.huella", "p.puede_reemplazar ? '' : ' disabled"):
            self.assertIn(pieza, P, pieza)
        self.assertNotRegex(P.split('id="modalPropuestas"', 1)[1].split("modalEditorPlantilla", 1)[0], r"\b(alert|confirm|prompt)\(")


def _exec_prop():
    import ast
    import hashlib
    from tests.test_ot_saldo_servicio import _arbol as arbol
    amb = {"json": json, "hashlib": hashlib}
    _c, a = arbol()
    for n in a.body:
        if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id in ("_PROP_TITULO", "_PROP_EXPLICA") for t in n.targets):
            exec(compile(ast.Module(body=[n], type_ignores=[]), "<app>", "exec"), amb)
        if isinstance(n, ast.FunctionDef) and n.name in ("_prop_es_lu", "_prop_vista", "_prop_huella", "_prop_armar"):
            n2 = ast.parse(ast.unparse(n)).body[0]
            n2.decorator_list = []
            exec(compile(ast.Module(body=[n2], type_ignores=[]), "<app>", "exec"), amb)
    return amb


# ═══════════════════════════════ 3) cableado de app.py ═══════════════════════════════
class TestRutasYTablas(unittest.TestCase):
    def test_rutas_de_producto_y_de_equipo_con_sus_candados(self):
        for fn in ("ot2_api_producto_estado", "ot2_api_equipo_no_esta", "ot2_api_equipo_incidencia"):
            decs = _decoradores(fn)
            self.assertIn("_mant_required", decs, fn)
            self.assertIn("_ot_can_configurar", decs, fn)             # técnico asignado (interno, elevado o externo) o gestión
        for fn in ("ot2_api_producto_estado_revertir", "ot2_api_equipo_no_esta_revertir", "ot2_api_pendientes_falta_stock"):
            self.assertIn("_no_tecnico", _decoradores(fn), fn)        # devolver a operativo y ver pendientes: solo gestión
        codigo = _arbol()[0]
        for ruta in ('"/ot/api/<int:vid>/producto/<int:imid>/estado", methods=["POST"]',
                     '"/ot/api/<int:vid>/producto/<int:imid>/estado/revertir", methods=["POST"]',
                     '"/ot/api/<int:vid>/equipo/<int:mid>/no-esta", methods=["POST"]',
                     '"/ot/api/<int:vid>/equipo/<int:mid>/no-esta/revertir", methods=["POST"]',
                     '"/ot/api/<int:vid>/equipo/<int:mid>/incidencia", methods=["POST"]',
                     '"/ot/api/<int:vid>/pendientes-falta-stock", methods=["GET"]'):
            self.assertIn("@app.route(" + ruta, codigo, ruta)

    def test_la_tabla_guarda_productos_y_equipos_y_las_consultas_no_se_mezclan(self):
        crear = _fuente("_ensure_ot_producto_estado")
        for col in ("item_manual_id INT NULL", "maquina_id INT NULL", "creado_por VARCHAR(190)", "creado_en DATETIME",
                    "revertido_motivo", "ix_otpe_equipo", "ix_otpe_estado"):
            self.assertIn(col, crear, col)
        self.assertIn("CREATE TABLE IF NOT EXISTS mant_ot_producto_estado", crear)       # REGLA #18: barata ya aplicada
        self.assertIn("AND item_manual_id IS NOT NULL", _fuente("_otpe_filas_activas"))
        self.assertIn("AND maquina_id IS NOT NULL", _fuente("_otpe_equipos_activos"))

    def test_no_esta_usa_el_mismo_camino_de_no_pude_revisarlo(self):
        f = _fuente("ot2_api_equipo_no_esta")
        self.assertIn("INSERT INTO mant_visita_equipos", f)
        self.assertIn("estado_revision, razon_saltado", f)
        self.assertIn("'saltado'", f)                                               # el equipo deja de bloquear firma y cierre
        self.assertIn("INSERT INTO mant_ot_producto_estado", f)                     # y queda consultable
        self.assertIn("db.commit()", f)
        self.assertIn("_otrep_cargar_ot_y_equipo(vid, mid)", f)                     # el equipo es del cliente de ESA OT
        self.assertIn('"EQUIPO_YA_MARCADO"', f)                                     # no pisa fuera de servicio ni baja
        self.assertIn("if not limpio[\"fotos_validas\"]:", f)                       # «No llegó · falta stock» no lleva foto
        self.assertNotIn("DELETE", f)                                               # la fila nunca se borra
        rev = _fuente("ot2_api_equipo_no_esta_revertir")
        self.assertIn("len(motivo) < 10", rev)
        self.assertIn("activo=0, revertido_por=%s", rev)                           # se marca, no se elimina

    def test_razon_falta_stock_en_la_lista_valida_y_en_el_pdf(self):
        codigo = _arbol()[0]
        bloque = codigo[codigo.index("_RAZONES_SALTADO_VALIDAS = ("):][:900]
        self.assertIn('"falta_stock"', bloque)
        for k, _t in pe.motivos_equipo("no_revisado"):
            if k != "otro":
                self.assertIn(f'"{k}"', bloque, k)                                  # cada razón de «Otra razón» es válida
        self.assertIn('"no_encontrado"', bloque)
        pdf = codigo[codigo.index("_OT_PDF_RAZON_SALTADO = {"):][:900]
        self.assertIn('"falta_stock":        "No llegó · falta stock"', pdf)

    def test_pendientes_por_falta_de_stock_listos_para_el_trabajo_de_la_oth_hija(self):
        f = _fuente("_otpe_pendientes_falta_stock")
        self.assertIn("pe.estado = 'falta_stock' AND pe.activo = 1", f)
        for col in ("pe.visita_id", "pe.maquina_id", "pe.cantidad", "pe.creado_por", "pe.creado_en", "m.sku", "v.numero_ot"):
            self.assertIn(col, f, col)
        self.assertIn('"%d/%m/%Y %H:%M"', f)                                        # hora Chile (REGLA #6)
        # el comentario que explica qué hará OTRO trabajo, y que aquí NO se construye la OT hija
        for texto in ("OT hija", "rama de eventos", "madre/hija"):
            self.assertIn(texto, _leer("ot_producto_estado.py") + _arbol()[0], texto)
        self.assertNotIn("INSERT INTO mant_visitas", f)

    def test_el_pedido_de_repuestos_es_idempotente(self):
        fn = _exec("_otrep_lote_id_cliente", {"re": re})
        class _F(dict):
            def get(self, k, d=None):
                return dict.get(self, k, d)
        self.assertEqual(fn(_F(lote_id="A" * 32)), "a" * 32)
        self.assertIsNone(fn(_F(lote_id="no-es-un-uuid")))
        self.assertIsNone(fn(_F()))
        for ruta in ("ot2_api_producto_solicitar_repuestos", "ot2_api_equipo_solicitar_repuestos"):
            r = _fuente(ruta)
            self.assertIn("_otrep_lote_id_cliente(fd)", r, ruta)
            self.assertIn("_otrep_lote_repetido(vid, lote_id)", r, ruta)
            self.assertLess(r.index("_otrep_lote_repetido(vid, lote_id)"), r.index("_otrep_crear_fila_solicitud("), ruta)

    def test_el_equipo_de_cliente_tambien_trae_sugeridos_por_defecto(self):
        self.assertIn('"repuestos_sugeridos": _otrep_sugeridos_de_equipo(v, m),', _fuente("ot2_api_equipo_repuestos_opciones"))
        self.assertIn("_pe.sugeridos_para([], sistema)", _fuente("_otrep_sugeridos_de_equipo"))

    def test_el_acta_toma_el_motivo_el_detalle_y_el_riesgo(self):
        f = _fuente("_acta_armar")
        self.assertIn("_pe.resultado_acta(fila_estado)", f)                     # la acción manda sobre el «Resultado del control»
        self.assertIn('partes_obs.append("PUEDE LASTIMAR A ALGUIEN")', f)
        self.assertIn('"riesgo": riesgo', f)
        self.assertIn("estados=None", f)
        self.assertIn("PUEDE LASTIMAR A ALGUIEN", _leer("ot_producto_estado.py"))


class TestConfidencialidad(unittest.TestCase):
    def test_los_datos_de_la_barra_no_llevan_plata_ni_proveedores(self):   # REGLA #19 y #26
        f = _fuente("ot2_detalle")
        bloque = f[f.index("acc_puede = bool("):f.index("return render_template(")]
        self.assertIn("acc_data = {}", bloque)
        self.assertIsNone(_PLATA.search(bloque), _PLATA.search(bloque))
        codigo_js = re.sub(r"/\*.*?\*/", "", ACC_JS, flags=re.S)
        codigo_js = re.sub(r"(?m)^\s*//.*$", "", codigo_js)
        self.assertIsNone(_PLATA.search(codigo_js), _PLATA.search(codigo_js))
        for fn in ("_otpe_publico", "_otpe_equipo_publico", "_otpe_pendientes_falta_stock", "_fs_info_maquinas"):
            self.assertIsNone(_PLATA.search(_fuente(fn).split('"""', 2)[-1]), fn)
        # solo la gestión puede devolver a operativo (el botón lo trae el servidor en p.rev)
        self.assertIn('_acc_rev = bool((not _es_rol_tecnico()) and puede_configurar and not ot_solo_lectura)', bloque)
        # un externo no puede dar de baja equipos del cliente (igual que antes)
        self.assertIn('"baja": bool(acc_puede and (_interno or not _acc_externo))', bloque)
        self.assertIn('"noe": bool(acc_puede and not _interno)', bloque)           # «No está / no llegó»: equipos de cliente
        self.assertIn('"nol": bool(acc_puede and _interno)', bloque)               # «No llegó»: solo productos de la revisión

    def test_un_tecnico_solo_toca_equipos_y_productos_de_su_ot(self):
        self.assertIn("EQUIPO_NO_EN_OT", _fuente("_otrep_cargar_ot_y_equipo"))
        self.assertIn("EQUIPO_DE_OTRO_CLIENTE", _fuente("_otrep_cargar_ot_y_equipo"))
        self.assertIn("PRODUCTO_NO_EN_OT", _fuente("_otrep_cargar_ot_y_producto"))


class TestArmadoDeAccData(unittest.TestCase):
    """Corre DE VERDAD el bloque de ot2_detalle que arma el JSON de la barra (window.OTD_ACC), con equipos de mentira."""

    def _armar(self, equipos, externo=False, tecnico=False, ejecutar=True, configurar=True, solo_lectura=False, np_map=None):
        f = _fuente("ot2_detalle")
        bloque = f[f.index("    acc_puede = bool("):f.index("    return render_template(")]
        src = ("def _f(equipos, puede_ejecutar, puede_configurar, ot_solo_lectura, vid):\n" + bloque.rstrip()
               + "\n    return acc_data, acc_listas\n")
        amb = {"_pe": pe, "_otpe_equipos_activos": lambda vid: np_map or {}, "_es_tecnico_externo": lambda: externo,
               "_es_rol_tecnico": lambda: tecnico}
        exec(compile(src, "<acc_data>", "exec"), amb)
        return amb["_f"](equipos, ejecutar, configurar, solo_lectura, 900)

    EQUIPOS = [
        {"id": 0, "nombre": "Trabajo interno", "_interno": True},
        {"id": 101, "nombre": "ILUS Optimal Chest Press", "sku": "1121", "_interno": True, "cantidad_doc": 3,
         "acc_sol": [{"id": 1, "nombre": "Piola", "cant": 1, "estado": "validado", "abierta": True}],
         "estado_prod": {"estado": "baja", "dispensa": True}},
        {"id": 11, "nombre": "Trotadora", "sku": "AHT", "serie": "S1", "foto_url": "/f/x.jpg", "_interno": False,
         "estado_capturado": "fuera_servicio", "fs_info": {"motivo": "No enciende", "riesgo": True, "desde": "20/10 10:41", "por": "Isabel"},
         "dado_baja_aqui": False},
    ]

    def test_claves_y_contenido(self):
        acc, listas = self._armar(self.EQUIPOS, np_map={11: {"estado": "falta_stock", "chip": "No llegó · falta stock · 20/10 · Isabel"}})
        self.assertEqual(sorted(acc), ["e11", "i101"])                       # «Trabajo interno» (id 0) no es un producto
        i, e = acc["i101"], acc["e11"]
        self.assertEqual((i["k"], i["cant"], i["est"]["estado"]), ("i", 3, "baja"))
        self.assertEqual((e["k"], e["cant"], e["fsb"], e["fs"]["riesgo"], e["img"]), ("e", 1, True, True, "/f/x.jpg"))
        self.assertEqual(e["np"]["chip"], "No llegó · falta stock · 20/10 · Isabel")
        self.assertEqual(i["sol"][0]["nombre"], "Piola")
        for clave in ("fuera_servicio", "baja", "consultado", "no_esta", "falta_stock", "no_revisado", "min"):
            self.assertIn(clave, listas, clave)
        self.assertEqual(listas["min"], {"fuera_servicio": 10, "baja": 12, "equipo": 10})

    def test_que_ve_cada_rol(self):
        acc, _l = self._armar(self.EQUIPOS)
        self.assertEqual(acc["i101"]["p"], {"ped": True, "fs": True, "inc": True, "rev": True, "baja": True, "nol": True, "noe": False})
        self.assertEqual(acc["e11"]["p"], {"ped": True, "fs": True, "inc": True, "rev": True, "baja": True, "nol": False, "noe": True})
        # técnico (interno, elevado o externo): puede pedir, avisar e informar; NO devolver a operativo (gestión)
        acc, _l = self._armar(self.EQUIPOS, tecnico=True)
        self.assertFalse(acc["i101"]["p"]["rev"])
        self.assertTrue(acc["i101"]["p"]["inc"])                             # decisión de Daniel: cualquier técnico asignado reporta
        # el externo no da de baja equipos del cliente (sí puede en un producto de la revisión)
        acc, _l = self._armar(self.EQUIPOS, tecnico=True, externo=True)
        self.assertFalse(acc["e11"]["p"]["baja"])
        self.assertTrue(acc["i101"]["p"]["baja"])
        # sin permiso o con la OT sellada: nada
        for kw in ({"ejecutar": False, "configurar": False}, {"solo_lectura": True}):
            acc, _l = self._armar(self.EQUIPOS, **kw)
            for d in acc.values():
                self.assertFalse(any(d["p"].values()), kw)


# ═══════════════════════════════ 4) espejo JS ↔ Python del dispensado ═══════════════════════════════
@unittest.skipUnless(HAY_NODE, "node no está instalado")
class TestEspejoDelDispensado(unittest.TestCase):
    def test_otdchkevaluar_con_estados_da_lo_mismo_que_python(self):
        ev = E._k()["_chk_revision_evaluar"]
        fuente = "\n".join(_js_funcion(n, D) for n in E.TestEspejoJS.FUNCS)
        casos = {k: E.CASOS[k] for k in ("no_llego_sin_argumento", "falla_sin_foto", "dos_productos", "apto_limpio")}
        estados = {"1": "no_llego", "2": "baja"}
        js = _node(fuente + "\nconst C = " + json.dumps(casos, ensure_ascii=False) + ";\nconst Z = " + json.dumps(estados) + ";\n"
                   "const out = {}; for (const k in C){ const r = otdChkEvaluar(C[k], Z); "
                   "out[k] = {no_llego: r.no_llego.sort(), disp: r.dispensados.sort(), por_tarea: r.por_tarea}; }\n"
                   "console.log(JSON.stringify(out));")
        py_estados = {1: "no_llego", 2: "baja"}
        for caso, tareas in casos.items():
            py = ev(tareas, py_estados)
            self.assertEqual(sorted(py["no_llego"]), js[caso]["no_llego"], caso)
            self.assertEqual(sorted(py["dispensados"]), js[caso]["disp"], caso)
            self.assertEqual({str(k): v for k, v in py["por_tarea"].items()}, js[caso]["por_tarea"], caso)
        # el producto dispensado no exige nada (ni la observación): ese es el punto de la acción
        for t in js["no_llego_sin_argumento"]["por_tarea"].values():
            self.assertFalse(t["exigible"])

    def test_sin_estados_todo_sigue_igual(self):
        ev = E._k()["_chk_revision_evaluar"]
        fuente = "\n".join(_js_funcion(n, D) for n in E.TestEspejoJS.FUNCS)
        js = _node(fuente + "\nwindow = {};\nconst C = " + json.dumps(E.CASOS, ensure_ascii=False) + ";\n"
                   "const out = {}; for (const k in C){ const r = otdChkEvaluar(C[k]); out[k] = r.por_tarea; }\n"
                   "console.log(JSON.stringify(out));")
        for caso, tareas in E.CASOS.items():
            self.assertEqual({str(k): {kk: vv for kk, vv in v.items() if kk != "dispensada"} for k, v in ev(tareas)["por_tarea"].items()},
                             {k: {kk: vv for kk, vv in v.items() if kk != "dispensada"} for k, v in js[caso].items()}, caso)


# ═══════════════════════════════ 5) el front ═══════════════════════════════
class TestBarraDeAcciones(unittest.TestCase):
    def test_un_solo_json_y_un_solo_contenedor_nada_de_modales_por_producto(self):
        """RENDIMIENTO (Daniel 20-oct: la ficha de 19 productos × 9 tareas congeló Chrome): la barra NO es HTML repetido por
        tarjeta. Un <div> vacío por tarjeta, UN JSON y UNA pantalla completa creada por JS al primer uso."""
        self.assertEqual(D.count('class="otd-acc" id="otdAcc-'), 1)               # una línea de plantilla (dentro del for)
        self.assertEqual(D.count("window.OTD_ACC = "), 1)
        self.assertEqual(len(re.findall(r'id="oa[A-Z]\w*"', D)), 0)               # nada de la pantalla completa en el HTML
        self.assertEqual(ACC_JS.count("document.createElement('div')"), 1)         # el contenedor se crea UNA vez
        self.assertIn("if (root) return;", ACC_JS)
        for f in ("pedir", "fuera", "baja", "nollego", "noesta", "incidencia", "seg"):
            self.assertIn(f"var {f} = {{", ACC_JS, f)
        # el CSS y el JS son archivos estáticos (cacheables), no inline en la ficha
        self.assertNotIn(".oa-fsb", D)
        self.assertNotIn("function barHTML", D)

    def test_las_mismas_palabras_del_prototipo_aprobado(self):
        for texto in ("¿Qué pieza necesita ", "Otra pieza · pídela con una foto", "Salir a la OT", "No se puede usar",
                      "No tiene arreglo", "No llegó", "No está / no llegó", "Informar incidencia", "Pedir repuesto",
                      "Se guarda en este teléfono: puedes salir y volver.", "Bodega ya recibió tu pedido",
                      "Así va a avanzar", "Tu pedido", "Revisión de bodega", "Pieza lista en bodega", "Pieza instalada",
                      "Pedir otra pieza", "Volver a la OT", "No es ninguna · pídela con una foto", "Foto y envío",
                      "Elegir ya es agregar al pedido", "Fuera de lo normal"):
            self.assertIn(texto, ACC_JS, texto)
        for v in ("No está en el lugar", "No llegó · falta stock", "Otra razón"):
            self.assertIn(v, ACC_JS + _leer("ot_producto_estado.py"), v)

    def test_la_barra_va_antes_del_checklist_en_cada_equipo_y_producto(self):
        self.assertIn("function barHTML(key, d)", ACC_JS)
        i = ACC_JS.index("function barHTML(key, d)")
        barra = ACC_JS[i:ACC_JS.index("function pintar(key)")]
        # (b) incluye «Informar incidencia» además de pedir repuesto, no se puede usar, no tiene arreglo y no llegó
        for accion in ("'fs'", "'baja'", "'nollego'", "'noesta'", "'incidencia'", "data-f=\"pedir\""):
            self.assertIn(accion, barra, accion)
        # «No llegó» solo en productos de la revisión; «No está / no llegó» solo en equipos de cliente
        self.assertIn("if (inter && p.nol && !fs && !bj && !nl) g.push(['nollego'", barra)
        self.assertIn("if (!inter && p.noe && !fs && !bj && !np) g.push(['noesta'", barra)
        self.assertIn("if (p.inc && !bj) g.push(['incidencia'", barra)
        # un equipo ya marcado no ofrece lo mismo otra vez; un dado de baja no pide repuestos
        self.assertIn("var puedeCta = p.ped && !bj && !nlTotal && !np;", barra)

    def test_aviso_rojo_de_fuera_de_servicio_con_motivo_y_hora(self):
        self.assertIn("'Fuera de servicio', 'sign-stop-fill'", ACC_JS)
        self.assertIn("desdeTxt(", ACC_JS)
        self.assertIn("Puede lastimar a alguien", ACC_JS)
        for css in (".oa-fsb{", "linear-gradient(135deg,#dc2626,#b91c1c)", ".oa-fsb .risk{"):
            self.assertIn(css, ACC_CSS, css)
        # la ficha del cliente (lista de equipos) y la ficha del equipo: «FUERA DE SERVICIO · motivo · desde dd/mm hh:mm»
        for tpl in (FICHA, MAQ_FICHA):
            self.assertIn('class="eq-fsb"', tpl)
            self.assertIn("fs_info.motivo", tpl)
            self.assertIn("desde {{ ", tpl)
            self.assertIn("Puede lastimar a alguien", tpl)
            self.assertIn("eq_fuera_servicio.css", tpl)
        self.assertIn(".eq-fsb{", FS_CSS)
        self.assertIn('class="eq-row-fs"', FICHA)

    def test_pulso_suave_en_pedir_repuesto_con_reduced_motion(self):
        self.assertIn(".oa-primary{", ACC_CSS)
        self.assertIn("animation:oaPulse 2.4s ease-in-out infinite", ACC_CSS)
        self.assertIn("@keyframes oaPulse{", ACC_CSS)
        i = ACC_CSS.index("@media(prefers-reduced-motion:reduce)")
        self.assertIn(".oa-primary{animation:none;box-shadow:0 0 0 3px", ACC_CSS[i:])    # sin animación, pero destacado
        self.assertIn("rgba(37,99,235,", ACC_CSS)                                        # azul, nunca un texto de error

    def test_foto_obligatoria_donde_corresponde_y_ninguna_donde_no(self):
        # Fuera de servicio, baja, incidencia y pedir repuesto: foto obligatoria. No llegó (producto y equipo con falta de stock): sin foto.
        self.assertIn("var okF = s.fotos.length > 0", ACC_JS)
        self.assertIn("Falta ' + esc(txt) + ' · tócame", ACC_JS)
        self.assertIn("if (s.op !== 'falta_stock') s.fotos.forEach(", ACC_JS)
        self.assertIn("«No llegó» no lleva foto", ACC_JS)
        self.assertIn("lote_id", ACC_JS)                                                 # idempotencia del pedido
        self.assertIn("fd.append('evidencia', s.fotos[0].file", ACC_JS)
        # los motivos de lista y los mínimos salen del servidor (una sola fuente), no de la cabeza del JS
        for clave in ("fuera_servicio", "baja", "consultado", "min"):
            self.assertIn(f"LISTAS().{clave}", ACC_JS, clave)
        self.assertIn("LISTAS()[s.op]", ACC_JS)                                         # no_esta | falta_stock | no_revisado

    def test_borrador_sin_perder_la_foto_y_salir_a_la_ot_guarda(self):
        for pieza in ("indexedDB.open('ilus_ot_acciones', 1)", "localStorage.setItem(bkey(S.key)", "guardarBorrador(S).then(",
                      "Guardado en este teléfono. Lo retomas desde la tarjeta.", "BORRADOR_DIAS", "window.ilusConfirm({ title: '¿Borrar el pedido sin terminar?'"):
            self.assertIn(pieza, ACC_JS, pieza)

    def test_boton_atras_del_telefono_cierra_la_pantalla_no_la_ot(self):
        self.assertIn("history.pushState({ oaSheet: 1 }, '')", ACC_JS)
        self.assertIn("window.addEventListener('popstate'", ACC_JS)
        self.assertIn("document.documentElement.style.overflow = 'hidden'", ACC_JS)     # el fondo no se mueve

    def test_responsivo_y_accesible(self):
        for css in ("@media(min-width:760px){.oa-fs{align-items:center}", ".oa-win{position:relative;width:100%;max-width:720px;height:100vh;height:100dvh;",
                    "@media(min-width:720px){.oa-grid{grid-template-columns:repeat(4,1fr)}", "grid-template-columns:minmax(0,1fr) minmax(0,1fr)",
                    ".oa-tiles{display:grid;grid-template-columns:1fr 1fr;gap:10px}", "@media(min-width:560px){.oa-tiles{grid-template-columns:repeat(3,1fr)}}",
                    "overflow-wrap:anywhere", ":focus-visible{outline:3px solid #93c5fd"):
            self.assertIn(css, ACC_CSS, css)
        self.assertIn("setAttribute('role', 'dialog')", ACC_JS)
        self.assertIn("setAttribute('aria-modal', 'true')", ACC_JS)
        self.assertIn("aria-pressed", ACC_JS)
        self.assertIn("aria-label", ACC_JS)

    def test_tipografia_propia_sin_pedirle_nada_a_terceros(self):
        self.assertNotIn("googleapis", ACC_CSS + ACC_JS)
        for f in ("Barlow-500.woff2", "Barlow-700.woff2", "Barlow-800.woff2", "Barlow_Condensed-800.woff2"):
            self.assertIn(f, ACC_CSS, f)
            self.assertTrue(os.path.exists(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "static", "fonts", f)), f)
        self.assertIn("font-family:'OA Barlow'", ACC_CSS)                                  # nombre propio: no toca otros textos

    @unittest.skipUnless(HAY_NODE, "node no está instalado")
    def test_el_js_compila(self):
        r = subprocess.run(["node", "--check", os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "static", "ot_acciones.js")],
                           capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stderr)

    def test_los_botones_viejos_siguen_pero_apuntan_al_flujo_nuevo(self):   # REGLA #4.2
        self.assertIn("No pude revisarlo", D)                                              # se conserva el botón que ya existía
        self.assertIn("otdSaltarEquipo({{ e.id }})", D)
        f = D[D.index("window.otdFueraServicio = async function"):][:700]
        self.assertIn("window.otdAccAbrir('fs', 'e' + mid)", f)
        b = D[D.index("window.otdBajaAbrir = function"):][:600]
        self.assertIn("window.otdAccAbrir('baja', 'e' + mid)", b)
        self.assertIn("Solicitar repuesto", D)                                             # el modal clásico (piola, cinta, manual)
        self.assertIn("Cambio de piola", D)


if __name__ == "__main__":
    unittest.main()
