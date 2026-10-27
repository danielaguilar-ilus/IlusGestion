# -*- coding: utf-8 -*-
"""🩺 HALLAZGOS DE LA OT (F1, REGLA #38) — Daniel, 20-oct-2026, revisando la OT de Aurum (OT-2026-00171):
«Indícame si se está tomando fotografías como evidencia, eso es necesario, y que la OT avise para que se tome el caso y se levante una
acción que debería ser otra OT.»

Decisiones de Daniel que se vigilan:
  1. Solo AVISAR, no trabar: un hallazgo sin gestionar NO impide cerrar la OT (la franja del cierre es ámbar y dice «puedes cerrarla igual»).
  2. El aviso va a la gestión de ESA OT (el responsable primero) con UNA sola función (`_ot_gestion_destinatarios`): nunca a un técnico, nunca
     al cliente, nunca por correo. Una sola campana por OT y por persona agrupando lo que llegue en 30 minutos.
  3. La acción es OTRA OT (de seguimiento): el asistente «Nueva OT» llega PRELLENADO (cliente, equipos, tipo correctiva, título, motivo y fotos
     como referencia), queda ligada a la OT de origen (ot_madre_id + ot_relacion='seguimiento') y a sus hallazgos, y pasa por la puerta del
     documento NORMAL: no hereda cobros ni saldo (REGLAS #24, #25 y #28). Lo prellenado lo decide la persona (REGLA #32).
  4. Un técnico no ve esta gestión (REGLAS #19 y #26: el dato se quita en el SERVIDOR); su flujo de trabajo no cambia.
  5. La foto es la evidencia: «sin foto» se dice en ámbar. Nada se borra: lo que el técnico retira queda anotado.

Se prueba (sin BD, sin Flask, sin correos reales: REGLA #22):
  A. el módulo puro ot_hallazgos.py;
  B. las funciones de app.py con una base de mentira (alta idempotente, retiro, ganchos de cada ruta, campana agrupada, rutas de tomar /
     descartar / reabrir / prellenado, ficha, vínculo con la OT nueva, panel, arranque);
  C. el cableado (ENUM de la campana, arranque barato REGLA #18, candados, nombres sin plata REGLA #26, ganchos, bitácora del técnico);
  D. las plantillas RENDERIZADAS de verdad (tarjeta abierta, cerrada, sin foto, técnico, OT de seguimiento; franja del cierre; panel) y el JS.
Correr:  python -m pytest tests/test_ot_hallazgos.py -q -p no:warnings
"""
import ast
import datetime as dt
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import types
import unittest

import jinja2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import ot_eventos as EV  # noqa: E402
import ot_hallazgos as H  # noqa: E402
import gestion_circular as GC  # noqa: E402  (26-oct: gestión circular)
import hallazgos_sla as HS  # noqa: E402  (26-oct: SLA y trazabilidad)
import ot_producto_estado as PE  # noqa: E402
from tests._fuente_app import RAIZ, decoradores, fuente_constante, texto  # noqa: E402
from tests._fuente_app import fuente_funcion as _fuente_funcion  # noqa: E402

HAY_NODE = shutil.which("node") is not None
APP = texto("app.py")
# Lo que NUNCA debe viajar en un texto armado por el servidor para la campana o el prellenado (REGLAS #19 y #26).
_PLATA = re.compile(r"\$\s?\d|\bproveedor|\bcosto|precio|margen|\bfactura|\bsaldo|valorizad|\bOC\b|\bmonto|cobr|ganancia|pérdida", re.I)


def _leer(rel):
    with open(os.path.join(RAIZ, rel.replace("/", os.sep)), encoding="utf-8", newline="") as fh:
        return fh.read().replace("\r\n", "\n")


# ══════════════════════════════════ A) el módulo puro ══════════════════════════════════
class TestConstantesYCatalogo(unittest.TestCase):
    def test_constantes_y_largos_de_columna(self):
        self.assertEqual(H.TIPO_AVISO, "ot_hallazgo")
        self.assertEqual(H.TIPO_AVISO_TOMA, "ot_hallazgo_toma")
        self.assertEqual(H.REL_SEGUIMIENTO, "seguimiento")
        self.assertEqual(EV.REL_SEGUIMIENTO, H.REL_SEGUIMIENTO, "ot_eventos.py tiene que saber que NO es una desinstalación")
        self.assertLessEqual(len(H.REL_SEGUIMIENTO), 20)                         # mant_visitas.ot_relacion VARCHAR(20)
        self.assertNotEqual(H.TIPO_AVISO, H.TIPO_AVISO_TOMA)
        self.assertEqual(H.TIPO_OT_SEGUIMIENTO, "correctiva")

    def test_cada_severidad_tiene_semaforo_icono_y_orden_unico(self):
        self.assertEqual(set(H.SEVERIDADES), {"falla", "fuera_servicio", "baja", "alerta", "repuesto", "incidencia"})
        ordenes = []
        for k, s in H.SEVERIDADES.items():
            self.assertTrue(s["texto"] and s["icono"].startswith("bi-"), k)
            self.assertIn(s["color"], ("rojo", "ambar", "azul", "negro", "gris"), k)
            self.assertIn(s["prioridad"], ("media", "alta"), k)
            ordenes.append(s["orden"])
        self.assertEqual(sorted(ordenes), list(range(1, 7)))
        # lo que obliga a actuar ya va en rojo y alta; el aviso de repuesto/incidencia es informativo
        for k in ("falla", "fuera_servicio", "baja"):
            self.assertEqual(H.SEVERIDADES[k]["prioridad"], "alta", k)
        self.assertEqual(H.SEVERIDADES["alerta"]["color"], "ambar")
        self.assertEqual(H.sev_info("inventada")["texto"], "Alerta")              # una severidad desconocida nunca rompe la pantalla

    def test_estados(self):
        self.assertEqual(set(H.ESTADOS), {"abierto", "tomado", "resuelto", "descartado"})
        self.assertEqual(H.ESTADOS["abierto"]["texto"], "Sin gestionar")
        self.assertEqual(H.ESTADOS["resuelto"]["texto"], "Con OT de seguimiento")
        self.assertEqual(H.ESTADOS_GESTIONABLES, ("abierto", "tomado"))
        self.assertEqual(H.estado_info("raro")["texto"], "Sin gestionar")
        self.assertEqual(H.ESTADOS_OT_CAIDA, ("cancelada", "anulada"))


class TestUtilidades(unittest.TestCase):
    def test_norm(self):
        self.assertEqual(H.norm("  No  Llegó "), "no llego")
        self.assertEqual(H.norm(None), "")
        self.assertEqual(H.norm("Pendiente de DIAGNÓSTICO"), "pendiente de diagnostico")

    def test_limpio(self):
        self.assertEqual(H.limpio("  a \n b\t c ", 50), "a b c")
        self.assertEqual(H.limpio("x" * 20, 5), "xxxxx")
        self.assertEqual(H.limpio(None, 5), "")

    def test_entero(self):
        for bueno, esperado in ((3, 3), ("7", 7), (" 12 ", 12), (4.0, 4), ("5.0", 5)):
            self.assertEqual(H.entero(bueno), esperado, bueno)
        for malo in (None, "", "  ", 0, -1, "-3", 1.5, "x", True, False, float("nan"), float("inf"), [], {}):
            self.assertIsNone(H.entero(malo), repr(malo))

    def test_es_si(self):
        for si in ("si", "Sí", "SI", "1", "true", "yes", "on", True):
            self.assertTrue(H.es_si(si), si)
        for no in ("no", "", None, False, "0", "off", "talvez", 0):
            self.assertFalse(H.es_si(no), repr(no))

    def test_lista_enteros(self):
        self.assertEqual(H.lista_enteros("1, 2;3  4"), [1, 2, 3, 4])
        self.assertEqual(H.lista_enteros([3, "3", 4]), [3, 4], "sin repetidos y en orden")
        self.assertEqual(H.lista_enteros("x,1,-2,0,2,1.5"), [1, 2])
        self.assertEqual(H.lista_enteros(5), [5])
        self.assertEqual(H.lista_enteros(None), [])
        self.assertEqual(len(H.lista_enteros(list(range(1, 80)))), H.HALLAZGOS_MAX_POR_SEGUIMIENTO)

    def test_lista_urls(self):
        self.assertEqual(H.lista_urls(["/f/a.jpg", {"url": "/f/b.jpg"}, "/f/a.jpg", "", None, {"x": 1}]), ["/f/a.jpg", "/f/b.jpg"])
        self.assertEqual(H.lista_urls('["/f/a.jpg", "/f/b.jpg"]'), ["/f/a.jpg", "/f/b.jpg"])
        self.assertEqual(H.lista_urls("no es json"), [])
        self.assertEqual(H.lista_urls(None), [])
        self.assertEqual(len(H.lista_urls(["/f/%d.jpg" % i for i in range(40)])), H.FOTOS_MAX)
        self.assertEqual(len(H.lista_urls(["/f/" + "x" * 900])[0]), 700)


class TestQueEsUnHallazgo(unittest.TestCase):
    def test_resultado_del_control_se_reconoce_por_sus_opciones(self):
        self.assertTrue(H.es_resultado(["Apto", "Apto con observaciones", "Reparar"]))
        self.assertTrue(H.es_resultado('["Apto","Apto con observaciones","Dar de baja"]'))
        self.assertTrue(H.es_resultado(["apto", "APTO CON OBSERVACIONES"]))
        self.assertFalse(H.es_resultado(["Rojo", "Azul"]), "una lista cualquiera NO es un hallazgo")
        self.assertFalse(H.es_resultado(["Apto"]))
        self.assertFalse(H.es_resultado(None))
        self.assertFalse(H.es_resultado("basura"))

    def test_severidad_de_cada_opcion_del_resultado(self):
        casos = {
            "Apto": None, "No llegó": None, "": None, None: None, "Sin resultado": None, "No llegó el equipo": None,
            "Apto con observaciones": "alerta", "Pendiente de diagnóstico": "alerta",
            "Reparar": "falla", "Reparar antes de entregar": "falla", "Reparación": "falla",
            "Dar de baja": "baja", "Fuera de servicio": "fuera_servicio", "No apto": "fuera_servicio",
            # palabras que CONTIENEN la raíz no son un hallazgo grave: un aviso de más es mejor que uno enterrado, pero sin falsa alarma roja
            "Preparar para la venta": "alerta", "Puede seguir trabajando": "alerta",
            "Algo que nadie conoce": "alerta",
        }
        for opcion, esperado in casos.items():
            self.assertEqual(H.severidad_resultado(opcion), esperado, repr(opcion))

    def _tarea(self, tipo, valor, **kw):
        t = {"tipo_respuesta": tipo, "valor_json": json.dumps(valor, ensure_ascii=False)}
        t.update(kw)
        return t

    def test_verificacion_falla_y_alerta_son_hallazgos_y_aprobado_no(self):
        r = H.evaluar_tarea(self._tarea("verificacion", {"valor": "falla"}))
        self.assertEqual((r["severidad"], r["valor_txt"], r["clase"]), ("falla", "Falla", "verificacion"))
        r = H.evaluar_tarea(self._tarea("verificacion", {"valor": "alerta"}))
        self.assertEqual((r["severidad"], r["valor_txt"]), ("alerta", "Alerta"))
        r = H.evaluar_tarea(self._tarea("verificacion", {"valor": "Falla"}))
        self.assertEqual(r["severidad"], "falla", "mayúsculas no importan")
        self.assertIsNone(H.evaluar_tarea(self._tarea("verificacion", {"valor": "aprobado"})))
        self.assertIsNone(H.evaluar_tarea(self._tarea("verificacion", {})))

    def test_no_aplica_no_es_un_hallazgo(self):
        self.assertIsNone(H.evaluar_tarea(self._tarea("verificacion", {"valor": "falla", "na": True})))

    def test_el_resultado_con_una_opcion_grave_es_un_hallazgo(self):
        ops = ["Apto", "Apto con observaciones", "Reparar", "Dar de baja", "No llegó"]
        r = H.evaluar_tarea(self._tarea("lista", {"opcion": "Reparar"}, opciones_lista_json=json.dumps(ops)))
        self.assertEqual((r["severidad"], r["valor_txt"], r["clase"]), ("falla", "Reparar", "resultado"))
        r = H.evaluar_tarea(self._tarea("lista", {"opcion": "Dar de baja"}, opciones_lista_json=ops))
        self.assertEqual(r["severidad"], "baja")
        for nada in ("Apto", "No llegó"):
            self.assertIsNone(H.evaluar_tarea(self._tarea("lista", {"opcion": nada}, opciones_lista_json=ops)), nada)

    def test_una_lista_cualquiera_texto_o_numero_no_es_un_hallazgo(self):
        self.assertIsNone(H.evaluar_tarea(self._tarea("lista", {"opcion": "Rojo"}, opciones_lista_json=["Rojo", "Azul"])))
        self.assertIsNone(H.evaluar_tarea(self._tarea("texto", {"texto": "falla"})))
        self.assertIsNone(H.evaluar_tarea(self._tarea("numero", {"numero": 3})))
        self.assertIsNone(H.evaluar_tarea({"tipo_respuesta": "verificacion", "valor_json": "no es json"}))
        self.assertIsNone(H.evaluar_tarea({}))
        self.assertIsNone(H.evaluar_tarea(None))

    def test_camino_rapido_de_cada_respuesta(self):
        f = H.respuesta_puede_ser_hallazgo
        self.assertEqual(f({"valor": "falla"}), "evaluar")
        self.assertEqual(f({"valor": "Alerta"}), "evaluar")
        self.assertEqual(f({"opcion": "Reparar"}), "evaluar")
        self.assertEqual(f({"valor": "aprobado"}), "retirar")
        self.assertEqual(f({"na": True}), "retirar")
        for nada in ({"texto": "x"}, {"valor": "si"}, {"valor": "no"}, {"lat": 1.0, "lng": 2.0}, {"numero": 3}, {"serie": "S1"}, {}, None, "x"):
            self.assertIsNone(f(nada), repr(nada))


class TestClavesYTextos(unittest.TestCase):
    def test_referencias_unicas(self):
        self.assertEqual(H.ref_tarea(12), "t12")
        self.assertEqual(H.ref_producto(4, "fuera_servicio"), "i4:fuera_servicio")
        self.assertEqual(H.ref_producto(4, "rep", "ab12cd34"), "i4:rep:ab12cd34")
        self.assertEqual(H.ref_equipo(9, "inc", 55), "m9:inc:55")
        self.assertEqual(H.ref_equipo(9, "baja", None), "m9:baja")
        self.assertEqual(H.ref_equipo(9, "baja", ""), "m9:baja")
        self.assertLessEqual(len(H.ref_equipo(2 ** 31, "rep", "ab12cd34")), 80)    # referencia VARCHAR(80)

    def test_nombre_con_ficha_completo_nunca_cortado(self):
        self.assertEqual(H.nombre_con_ficha("Trotadora X", "ABC", "123"), "Trotadora X (SKU ABC) · S/N 123")
        self.assertEqual(H.nombre_con_ficha("Trotadora X"), "Trotadora X")
        self.assertEqual(H.nombre_con_ficha(""), "Equipo")
        largo = "Trotadora comercial de alto rendimiento con pantalla táctil de 22 pulgadas y banda ortopédica " * 2
        self.assertEqual(H.nombre_con_ficha(largo), H.limpio(largo, 300), "REGLA #15: el nombre no se corta a 2 líneas")

    def test_titulos(self):
        self.assertEqual(H.titulo_tarea("Trotadora X (SKU ABC)", "Revisar cinta", "Falla"), "Trotadora X (SKU ABC) · Revisar cinta: Falla")
        self.assertEqual(H.titulo_tarea("", "Revisar cinta", "Falla"), "Revisar cinta: Falla")
        self.assertEqual(H.titulo_tarea("", "", ""), "Tarea del checklist")
        self.assertEqual(H.titulo_accion("Banco plano X", "No se puede usar — pieza rota"), "Banco plano X: No se puede usar — pieza rota")
        self.assertEqual(H.titulo_accion("", "Dar de baja"), "Dar de baja")
        self.assertLessEqual(len(H.titulo_tarea("x" * 500, "y" * 500, "z" * 500)), H.TITULO_MAX)

    def test_resumen_de_repuestos_solo_nombres_y_cantidades(self):
        lineas = [{"nombre": "Piola (cable de acero)", "cantidad": 2.0, "proveedor": "NO DEBE SALIR", "costo": 99999},
                  {"repuesto_nombre": "Pin selector", "cantidad": "1"}, {"nombre": ""}, {"nombre": "Polea", "cantidad": None}]
        t = H.resumen_repuestos(lineas)
        self.assertEqual(t, "Piola (cable de acero) × 2, Pin selector × 1, Polea × 1")
        self.assertNotIn("NO DEBE SALIR", t)
        self.assertEqual(H.resumen_repuestos([]), "repuestos")
        seis = [{"nombre": "P%d" % i, "cantidad": 1} for i in range(6)]
        self.assertTrue(H.resumen_repuestos(seis).endswith("y 2 más"))


class TestReaparicionYEstadoEfectivo(unittest.TestCase):
    def test_reaparicion(self):
        f = H.decidir_reaparicion
        self.assertEqual(f({"estado": "tomado"}), {"estado": "tomado", "avisar": False, "reabre": False}, "sigue activo: no se avisa de nuevo")
        self.assertEqual(f(None), {"estado": "abierto", "avisar": False, "reabre": False})
        # retirado y vuelve a aparecer
        self.assertEqual(f({"estado": "descartado", "retirado_en": 1}), {"estado": "abierto", "avisar": True, "reabre": True})
        self.assertEqual(f({"estado": "abierto", "retirado_en": 1}), {"estado": "abierto", "avisar": True, "reabre": True})
        self.assertEqual(f({"estado": "tomado", "retirado_en": 1}), {"estado": "tomado", "avisar": False, "reabre": True},
                         "ya está en manos de alguien: no se le vuelve a avisar")
        self.assertEqual(f({"estado": "resuelto", "retirado_en": 1}), {"estado": "resuelto", "avisar": False, "reabre": True})

    def test_estado_efectivo(self):
        e = H.estado_efectivo
        self.assertEqual(e({"estado": "abierto"}), ("abierto", False))
        self.assertEqual(e({"estado": "resuelto", "ot_seguimiento_id": 5}, "OT-2026-00300", "programada"), ("resuelto", False))
        self.assertEqual(e({"estado": "resuelto", "ot_seguimiento_id": 5}, None, None), ("abierto", True), "la OT de seguimiento ya no existe")
        self.assertEqual(e({"estado": "tomado", "ot_seguimiento_id": 5}, "OT-2026-00300", "cancelada"), ("abierto", True))
        self.assertEqual(e({"estado": "resuelto", "ot_seguimiento_id": 5}, "OT-2026-00300", "Anulada"), ("abierto", True), "sin importar mayúsculas")
        self.assertEqual(e({"estado": "tomado"}), ("tomado", False), "sin OT de seguimiento no hay nada que caer")
        self.assertEqual(e({"estado": "descartado", "ot_seguimiento_id": 5}, None, None), ("descartado", False))

    def test_seguimiento_vigente(self):
        v = H.seguimiento_vigente
        self.assertFalse(v({}))
        self.assertTrue(v({"ot_seguimiento_id": 5}, "programada", True))
        self.assertFalse(v({"ot_seguimiento_id": 5}, "cancelada", True))
        self.assertFalse(v({"ot_seguimiento_id": 5}, "programada", False))


class TestAccionesPermitidas(unittest.TestCase):
    def test_tomar(self):
        p = H.puede_accion
        self.assertTrue(p("tomar", {"estado": "abierto"})[0])
        ok, msg = p("tomar", {"estado": "tomado", "tomado_por": "Aarón Rojas"})
        self.assertFalse(ok)
        self.assertIn("Aarón Rojas", msg)
        self.assertFalse(p("tomar", {"estado": "descartado"})[0])
        self.assertFalse(p("tomar", {"estado": "abierto", "retirado_en": 1})[0])
        self.assertFalse(p("tomar", {"estado": "resuelto", "ot_seguimiento_id": 5}, "programada")[0])
        # la OT de seguimiento se canceló: se puede volver a tomar
        self.assertTrue(p("tomar", {"estado": "abierto", "ot_seguimiento_id": 5}, "cancelada")[0])

    def test_levantar(self):
        p = H.puede_accion
        self.assertTrue(p("levantar", {"estado": "abierto"})[0])
        self.assertTrue(p("levantar", {"estado": "tomado"})[0], "quien tomó el caso puede levantar la OT")
        self.assertFalse(p("levantar", {"estado": "descartado"})[0])
        self.assertFalse(p("levantar", {"estado": "abierto", "retirado_en": 1})[0])
        self.assertFalse(p("levantar", {"estado": "resuelto", "ot_seguimiento_id": 5}, "en_curso")[0])

    def test_descartar_y_reabrir(self):
        p = H.puede_accion
        self.assertTrue(p("descartar", {"estado": "abierto"})[0])
        self.assertTrue(p("descartar", {"estado": "tomado"})[0])
        self.assertFalse(p("descartar", {"estado": "descartado"})[0])
        self.assertFalse(p("descartar", {"estado": "resuelto", "ot_seguimiento_id": 5}, "programada")[0], "con OT viva no se descarta")
        self.assertTrue(p("reabrir", {"estado": "descartado"})[0])
        self.assertFalse(p("reabrir", {"estado": "abierto"})[0])
        self.assertFalse(p("reabrir", {"estado": "descartado", "retirado_en": 1})[0], "retirado: el técnico lo devolvió, nada que reabrir")
        ok, msg = p("inventada", {})
        self.assertFalse(ok)
        self.assertTrue(msg)

    def test_todo_rechazo_trae_un_mensaje_para_la_persona(self):
        casos = [("tomar", {"estado": "descartado"}), ("tomar", {"estado": "abierto", "retirado_en": 1}), ("levantar", {"estado": "descartado"}),
                 ("descartar", {"estado": "descartado"}), ("reabrir", {"estado": "abierto"})]
        for accion, fila in casos:
            ok, msg = H.puede_accion(accion, fila)
            self.assertFalse(ok)
            self.assertGreater(len(msg), 15, (accion, fila))

    def test_descarte_exige_motivo(self):
        m, e = H.validar_descarte("  ")
        self.assertIsNone(m)
        self.assertIn("mínimo 10", e)
        self.assertIsNotNone(H.validar_descarte("corto")[1])
        m, e = H.validar_descarte("  El   equipo ya fue   reemplazado por el cliente ")
        self.assertIsNone(e)
        self.assertEqual(m, "El equipo ya fue reemplazado por el cliente")
        m, _e = H.validar_descarte("x" * 900)
        self.assertEqual(len(m), H.DESCARTE_MAX)


class TestValidarSeguimiento(unittest.TestCase):
    ORIGEN = {"id": 171, "cliente_id": 26, "estado": "en_curso", "numero_ot": "OT-2026-00171"}

    def _h(self, hid, **kw):
        f = {"id": hid, "visita_id": 171, "estado": "abierto", "retirado_en": None, "ot_seguimiento_id": None}
        f.update(kw)
        return f

    def test_camino_feliz(self):
        hs = [self._h(1), self._h(2, estado="tomado")]
        ok, err = H.validar_seguimiento(self.ORIGEN, hs, [1, 2], 26)
        self.assertIsNone(err)
        self.assertEqual([h["id"] for h in ok], [1, 2])

    def test_sin_origen_o_sin_hallazgos(self):
        self.assertEqual(H.validar_seguimiento(None, [], [1], 26)[1][1], "SEG_ORIGEN_NO_EXISTE")
        self.assertEqual(H.validar_seguimiento(self.ORIGEN, [self._h(1)], [], 26)[1][1], "SEG_SIN_HALLAZGOS")
        self.assertEqual(H.validar_seguimiento(self.ORIGEN, [self._h(1)], "x,0,-1", 26)[1][1], "SEG_SIN_HALLAZGOS")

    def test_un_hallazgo_de_otra_ot_se_rechaza(self):
        hs = [self._h(1), self._h(2, visita_id=999)]
        self.assertEqual(H.validar_seguimiento(self.ORIGEN, hs, [1, 2], 26)[1][1], "SEG_HALLAZGO_AJENO")
        self.assertEqual(H.validar_seguimiento(self.ORIGEN, hs, [1, 77], 26)[1][1], "SEG_HALLAZGO_AJENO", "ni siquiera existe")

    def test_un_hallazgo_ya_gestionado_se_rechaza_con_su_motivo(self):
        casos = {"descartado": self._h(1, estado="descartado"), "retirado": self._h(1, retirado_en=1),
                 "con otra OT": self._h(1, estado="resuelto", ot_seguimiento_id=5)}
        for nombre, h in casos.items():
            seg_info = {1: {"existe": True, "estado": "programada"}}
            _ok, err = H.validar_seguimiento(self.ORIGEN, [h], [1], 26, seg_info)
            self.assertEqual(err[1], "SEG_HALLAZGO_NO_DISPONIBLE", nombre)
            self.assertGreater(len(err[0]), 15, nombre)

    def test_una_ot_de_seguimiento_cancelada_deja_volver_a_elegir(self):
        h = self._h(1, estado="resuelto", ot_seguimiento_id=5)
        # estado 'resuelto' no es gestionable… pero la OT de seguimiento cayó: lo que manda es el estado efectivo (abierto)
        ok, err = H.validar_seguimiento(self.ORIGEN, [dict(h, estado="abierto")], [1], 26, {1: {"existe": True, "estado": "cancelada"}})
        self.assertIsNone(err)
        self.assertEqual(len(ok), 1)

    def test_tiene_que_ser_el_mismo_cliente(self):
        hs = [self._h(1)]
        self.assertEqual(H.validar_seguimiento(self.ORIGEN, hs, [1], 27)[1][1], "SEG_OTRO_CLIENTE")
        self.assertEqual(H.validar_seguimiento(self.ORIGEN, hs, [1], None)[1][1], "SEG_OTRO_CLIENTE", "de cliente → interna: no")
        interna = dict(self.ORIGEN, cliente_id=None)
        self.assertIsNone(H.validar_seguimiento(interna, hs, [1], None)[1], "interna → interna: sí")
        self.assertEqual(H.validar_seguimiento(interna, hs, [1], 26)[1][1], "SEG_OTRO_CLIENTE")


class TestPrellenado(unittest.TestCase):
    ORIGEN = {"numero_ot": "OT-2026-00171", "cliente_nombre": "Aurum Fit Girls Spa"}

    def _it(self, i, **kw):
        f = {"id": i, "severidad": "falla", "titulo": "Trotadora Pro (SKU TRO) · Revisar cinta: Falla", "detalle": "", "equipo": "Trotadora Pro (SKU TRO)",
             "maquina_id": 9, "fotos": []}
        f.update(kw)
        return f

    def test_un_hallazgo(self):
        p = H.armar_prellenado(self.ORIGEN, [self._it(1, detalle="La cinta patina", fotos=["/f/a.jpg", "/f/b.jpg"])])
        self.assertEqual(p["titulo"], "Seguimiento de OT-2026-00171 · Trotadora Pro (SKU TRO)")
        self.assertEqual(p["tipo_sugerido"], "correctiva")
        self.assertEqual(p["maquinas"], [9])
        self.assertEqual(p["fotos"], ["/f/a.jpg", "/f/b.jpg"])
        d = p["descripcion"]
        self.assertTrue(d.startswith("Seguimiento de OT-2026-00171 (Aurum Fit Girls Spa). Lo que reportó el técnico en terreno:\n"))
        self.assertIn("• Falla — Trotadora Pro (SKU TRO) · Revisar cinta: Falla. La cinta patina", d)
        self.assertIn("Fotos del hallazgo (2)", d)
        self.assertIsNone(_PLATA.search(json.dumps(p, ensure_ascii=False)), "el prellenado no lleva plata ni documentos")

    def test_varios_hallazgos_equipos_unicos_y_fotos_sin_repetir(self):
        its = [self._it(1, maquina_id=9, fotos=["/f/a.jpg"]), self._it(2, maquina_id=9, severidad="alerta", fotos=["/f/a.jpg", "/f/b.jpg"]),
               self._it(3, maquina_id=11, severidad="baja")]
        p = H.armar_prellenado(self.ORIGEN, its)
        self.assertEqual(p["titulo"], "Seguimiento de OT-2026-00171 · 3 hallazgos · Aurum Fit Girls Spa")
        self.assertEqual(p["maquinas"], [9, 11])
        self.assertEqual(p["fotos"], ["/f/a.jpg", "/f/b.jpg"])
        self.assertEqual(p["descripcion"].count("\n• "), 3)
        self.assertIn("• Dar de baja —", p["descripcion"])

    def test_las_fotos_de_referencia_tienen_tope(self):
        fotos = ["/f/%d.jpg" % i for i in range(12)]
        p = H.armar_prellenado(self.ORIGEN, [self._it(1, fotos=fotos)])
        self.assertEqual(len(p["fotos"]), H.FOTOS_REFERENCIA_MAX)
        self.assertIn("(%d)" % H.FOTOS_REFERENCIA_MAX, p["descripcion"])

    def test_largos_acotados_y_sin_cliente(self):
        p = H.armar_prellenado({"numero_ot": "OT-1"}, [self._it(i, titulo="T" * 400, detalle="D" * 600) for i in range(1, 9)])
        self.assertLessEqual(len(p["descripcion"]), 2000)
        self.assertLessEqual(len(p["titulo"]), 200)
        self.assertNotIn("()", p["descripcion"])

    def test_descripcion_de_la_foto_de_referencia(self):
        d = H.descripcion_foto_referencia("OT-2026-00171", "Trotadora Pro: Falla")
        self.assertEqual(d, "Hallazgo de OT-2026-00171 · Trotadora Pro: Falla")
        self.assertTrue(H.descripcion_foto_referencia(None, "x").startswith("Hallazgo de la OT de origen"))


class TestCampanaPura(unittest.TestCase):
    T0 = dt.datetime(2026, 10, 21, 14, 0, 0)

    def test_decidir_grupo(self):
        g = H.decidir_grupo
        self.assertEqual(g(None, self.T0), (self.T0, False))
        self.assertEqual(g(self.T0, self.T0 + dt.timedelta(minutes=29, seconds=59)), (self.T0, True))
        self.assertEqual(g(self.T0, self.T0 + dt.timedelta(minutes=30)), (self.T0 + dt.timedelta(minutes=30), False), "30 minutos exactos: grupo nuevo")
        self.assertEqual(g(self.T0 + dt.timedelta(minutes=5), self.T0), (self.T0, False), "un último aviso en el futuro no se toma como grupo")
        g2, cont = g(None, None)                                   # la base no entregó la hora: nunca un grupo vacío
        self.assertIsInstance(g2, dt.datetime)
        self.assertFalse(cont)

    def test_decidir_campana(self):
        d = H.decidir_campana
        self.assertEqual(d(None, True), "crear")
        self.assertEqual(d({"leida": False}, False), "actualizar")
        self.assertEqual(d({"leida": False}, True), "actualizar")
        self.assertEqual(d({"leida": True}, True), "actualizar", "leída y menos de 30 min: llegó información nueva")
        self.assertEqual(d({"leida": True}, False), "crear")

    def test_titulos_prioridad_y_cuerpo(self):
        self.assertEqual(H.titulo_campana("OT-2026-00171", 1), "OT-2026-00171: 1 hallazgo nuevo")
        self.assertEqual(H.titulo_campana("OT-2026-00171", 3), "OT-2026-00171: 3 hallazgos nuevos")
        self.assertEqual(H.titulo_campana(None, 0), "OT: 1 hallazgo nuevo")
        self.assertEqual(H.prioridad_campana([]), "media")
        self.assertEqual(H.prioridad_campana([{"severidad": "alerta"}, {"severidad": "repuesto"}]), "media")
        self.assertEqual(H.prioridad_campana([{"severidad": "alerta"}, {"severidad": "falla"}]), "alta")
        self.assertEqual(H.prioridad_campana([{"severidad": "alerta", "riesgo": 1}]), "urgente", "puede lastimar a alguien")
        items = [{"severidad": "falla", "titulo": "Trotadora X: Falla", "riesgo": 1}, {"severidad": "fuera_servicio", "titulo": "Banco Y: No se puede usar"},
                 {"severidad": "alerta", "titulo": "Polea Z: Alerta"}, {"severidad": "baja", "titulo": "Bici W: baja"}, {"severidad": "repuesto", "titulo": "R"}]
        c = H.cuerpo_campana("Aurum Fit Girls Spa", items, "Pedro Técnico")
        self.assertTrue(c.startswith("Aurum Fit Girls Spa: Falla — Trotadora X: Falla (PUEDE LASTIMAR A ALGUIEN); Fuera de servicio — Banco Y"))
        self.assertIn("y 2 más", c)
        self.assertIn("Lo reportó Pedro Técnico.", c)
        self.assertIn("Entra a la OT para tomar el caso o levantar una OT de seguimiento.", c)
        self.assertIsNone(_PLATA.search(c), c)
        self.assertTrue(H.cuerpo_campana("", [], None).startswith("Sin cliente:"))

    def test_aviso_suave_de_toma(self):
        self.assertEqual(H.titulo_toma("OT-2026-00171"), "OT-2026-00171: alguien tomó un hallazgo")
        c = H.cuerpo_toma("Juan Pablo Díaz", "Trotadora X: Falla")
        self.assertEqual(c, "Juan Pablo Díaz tomó el caso: Trotadora X: Falla. No hace falta que hagas nada: ya hay alguien a cargo.")
        self.assertTrue(H.cuerpo_toma(None, "x").startswith("Alguien del equipo tomó el caso"))
        fijo = H.cuerpo_toma("Alguien", "")                                  # solo la frase fija, sin el nombre del hallazgo
        self.assertNotRegex(fijo, r"(?i)tarde|culpa|error|urgente|debes|obligatori|!")   # nada que suene a castigo (el tono lo pidió Daniel, REGLA #36)


class TestConteosYSql(unittest.TestCase):
    def test_resumir(self):
        items = [{"estado_ef": "abierto"}, {"estado_ef": "abierto"}, {"estado_ef": "tomado"}, {"estado_ef": "resuelto"}, {"estado_ef": "descartado"},
                 {"estado_ef": "abierto", "retirado": True}, {"estado": "abierto"}]
        self.assertEqual(H.resumir(items), {"total": 7, "abiertos": 3, "tomados": 1, "con_ot": 1, "descartados": 1, "retirados": 1})
        self.assertEqual(H.resumir([])["total"], 0)

    def test_sql_abierto_es_una_sola_definicion(self):
        s = H.sql_abierto()
        self.assertIn("h.retirado_en IS NULL", s)
        self.assertIn("h.estado='abierto'", s)
        self.assertIn("s.estado IN ('cancelada','anulada')", s)
        self.assertIn("s.id IS NULL", s, "la OT de seguimiento ya no existe")
        self.assertTrue(H.sql_abierto("hf", "sf").startswith("(hf.retirado_en IS NULL"))
        for malo in ("h; DROP TABLE x", "1a", "", "a b", "h)--"):
            with self.assertRaises(ValueError):
                H.sql_abierto(malo)
        self.assertEqual(s.count("("), s.count(")"))


# ═══════════════════ B) las funciones de app.py con una base de mentira ═══════════════════
_INI_BLOQUE = APP.rfind("\n", 0, APP.index("HALLAZGOS DE LA OT (F1")) + 1          # desde el comienzo de la línea del banner
_FIN_BLOQUE = APP.index("\ndef _otrep_sistema_de_equipo(", _INI_BLOQUE)
BLOQUE = APP[_INI_BLOQUE:_FIN_BLOQUE]
FUNCIONES_BLOQUE = re.findall(r"^def (\w+)\(", BLOQUE, re.M)
CONSTANTES_BLOQUE = ("_HALL_ESTADO", "_HALL_RUTAS", "_HALL_SQL_SOLTAR_SEG", "_GC_ESTADO")   # 26-oct: + gestión circular
_COMPILADO = {}


def cargar(funciones=(), constantes=(), extra=None):
    """Compila cada función/constante de app.py UNA sola vez por corrida (buscar en los ~9 MB de app.py cuesta); cada prueba recibe un
    ámbito nuevo con sus propios valores mutables."""
    amb = dict(extra or {})
    for c in constantes:
        if ("c", c) not in _COMPILADO:
            _COMPILADO[("c", c)] = compile(ast.parse(fuente_constante(c)), "<app:%s>" % c, "exec")
        exec(_COMPILADO[("c", c)], amb)
    for f in funciones:
        if ("f", f) not in _COMPILADO:
            arbol = ast.parse(_fuente_funcion(f))
            for nodo in arbol.body:
                if isinstance(nodo, ast.FunctionDef):
                    nodo.decorator_list = []
            _COMPILADO[("f", f)] = compile(arbol, "<app:%s>" % f, "exec")
        exec(_COMPILADO[("f", f)], amb)
    return amb


HALL_COLS = ("id visita_id origen referencia tarea_id maquina_id item_manual_id severidad titulo detalle equipo_nombre riesgo fotos_json n_fotos "
             "estado detectado_por detectado_en tomado_por_id tomado_por tomado_en ot_seguimiento_id seguimiento_por seguimiento_en descartado_por "
             "descartado_en descarte_motivo retirado_en retirado_motivo aviso_grupo_en actualizado_en").split()
USUARIOS = {2: "supervisor", 3: "admin", 4: "supervisor", 7: "superadmin", 9: "tecnico", 10: "tecnico_externo"}
# Lo que devolvería _ot_gestion_destinatarios: el responsable primero; Lenin (técnico) creó la OT y NO debe recibir nada de esto.
GESTION = [{"id": 3, "nombre": "Aarón Rojas", "correo": "aaron@sphs.cl", "por": "responsable"},
           {"id": 4, "nombre": "Juan Pablo Díaz", "correo": "jp@sphs.cl", "por": "creador"},
           {"id": 2, "nombre": "Víctor Soto", "correo": "victor@sphs.cl", "por": "ejecutivo"},
           {"id": 9, "nombre": "Lenin Urbina", "correo": "lenin@sphs.cl", "por": "creador"}]
TEC = {"id": 10, "nombre": "Pedro Técnico", "username": "pedro@sphs.cl", "role": "tecnico_externo"}
GESTOR = {"id": 4, "nombre": "Juan Pablo Díaz", "username": "jp@sphs.cl", "role": "supervisor"}
VISITA_171 = {"id": 171, "numero_ot": "OT-2026-00171", "cliente_id": 26, "razon_social": "Aurum Fit Girls Spa", "rut": "76.123.456-7",
              "estado": "en_curso", "tipo": "revision", "ot_madre_id": None, "ot_relacion": None, "firma_cliente_url": None}


class Mundo:
    """Una base de datos de mentira (nunca la real) con SOLO lo que tocan los hallazgos. Cualquier SQL que el código haga y la prueba no
    prevea falla a gritos: así un cambio en una consulta no pasa desapercibido."""

    def __init__(self, usuario=None, tecnico=False):
        self.ahora = dt.datetime(2026, 10, 21, 14, 0, 0)
        self.hall, self._sig = [], 0
        self.visitas, self.maquinas, self.productos, self.tareas = {}, {}, {}, {}
        self.fotos_tareas, self.fotos_equipo, self.fotos_rep, self.fotos_inc = {}, {}, {}, {}
        self.estado_producto, self.equipos_visita, self.solicitudes = {}, {}, {}
        self.usuarios = dict(USUARIOS)
        self.gestion = [dict(p) for p in GESTION]
        self.notifs, self._nid = [], 0
        self.logs, self.sql, self.caches, self.avisados, self.salida = [], [], [], [], []
        self.fmts, self.commits = [], []
        self.db = types.SimpleNamespace(commit=lambda: self.commits.append("commit"))     # la conexión de la petición (get_db)
        self.tecnico = bool(tecnico)
        self.usuario = dict(usuario or GESTOR)
        self.g = types.SimpleNamespace(user=self.usuario)
        self.form, self.body, self.args = {}, {}, {}
        self.request = types.SimpleNamespace(form=self.form, args=self.args, get_json=lambda silent=True: self.body,
                                             method="POST", endpoint="", view_args={})
        self.falla = None            # fragmento de SQL que levanta un error
        self.carrera = False         # otra persona cambió la fila entre la lectura y el UPDATE (rowcount 0)
        self.oculta_primera_lectura = False
        self._uno = [
            ("SELECT id, estado, retirado_en FROM mant_ot_hallazgos WHERE visita_id=%s AND origen=%s AND referencia=%s", self._r_clave),
            ("SELECT titulo FROM mant_ot_hallazgos WHERE visita_id=%s AND origen=%s AND referencia=%s", self._r_titulo),
            ("SELECT NOW() AS t", lambda: {"t": self.ahora}),
            ("SELECT MAX(aviso_grupo_en) AS ult FROM mant_ot_hallazgos WHERE visita_id=%s", self._r_ultimo_grupo),
            ("SELECT v.*, c.razon_social FROM mant_visitas v", self._r_visita),
            ("SELECT * FROM mant_visitas WHERE id=%s", self._r_visita),
            ("SELECT v.id, v.numero_ot, v.cliente_id, v.estado, c.razon_social, c.rut FROM mant_visitas v", self._r_visita),
            ("SELECT h.*, s.numero_ot AS seg_numero, s.estado AS seg_estado FROM mant_ot_hallazgos h LEFT JOIN mant_visitas s "
             "ON s.id = h.ot_seguimiento_id WHERE h.id=%s AND h.visita_id=%s", self._r_fila_de),
            ("SELECT id, leida_at, created_at FROM mant_notificaciones WHERE tipo=%s AND visita_id=%s AND destino_user_id=%s", self._r_campana),
            ("SELECT id, titulo, tipo_respuesta, opciones_lista_json, valor_json, maquina_id, item_manual_id FROM mant_visita_tareas "
             "WHERE id=%s AND visita_id=%s", self._r_tarea),
            ("SELECT nombre, sku, serie FROM mant_maquinas WHERE id=%s", lambda mid: self._copia(self.maquinas.get(mid))),
            ("SELECT titulo, item_manual_sku, plantilla_id FROM mant_visita_tareas WHERE visita_id=%s AND maquina_id IS NULL AND item_manual_id=%s",
             lambda vid, imid: self._copia(self.productos.get((vid, imid)))),
            ("SELECT * FROM mant_ot_producto_estado WHERE visita_id=%s AND item_manual_id=%s AND activo=1",
             lambda vid, imid: self._copia(self.estado_producto.get((vid, imid)))),
            ("SELECT estado_revision, razon_saltado, observacion_tecnico, diagnostico_estado, diagnostico_texto FROM mant_visita_equipos",
             lambda vid, mid: self._copia(self.equipos_visita.get((vid, mid)))),
            ("SELECT estado_revision, razon_saltado, observacion_tecnico FROM mant_visita_equipos",
             lambda vid, mid: self._copia(self.equipos_visita.get((vid, mid)))),
            ("SELECT 1 AS x FROM mant_ot_hallazgos WHERE visita_id=%s AND origen=%s AND referencia IN (%s,%s) AND retirado_en IS NULL", self._r_especifico),
            ("SELECT repuesto_nombre, cantidad FROM mant_ot_repuesto_solicitudes WHERE id=%s", lambda sid: self._copia(self.solicitudes.get(sid))),
            ("SELECT n_fotos, n_videos FROM mant_ot_repuesto_solicitudes WHERE id=%s", lambda sid: self._copia(self.solicitudes.get(sid))),
        ]
        self._todos = [
            ("SELECT h.*, s.numero_ot AS seg_numero, s.estado AS seg_estado FROM mant_ot_hallazgos h LEFT JOIN mant_visitas s "
             "ON s.id = h.ot_seguimiento_id WHERE h.visita_id=%s ORDER BY h.id", self._r_listar),
            ("SELECT id, severidad, titulo, riesgo, detectado_por FROM mant_ot_hallazgos WHERE visita_id=%s AND aviso_grupo_en=%s", self._r_grupo),
            ("SELECT id, severidad, titulo, riesgo, detectado_por FROM mant_ot_hallazgos WHERE visita_id=%s "
             "AND aviso_grupo_en >= DATE_SUB(%s, INTERVAL 2 MINUTE)", self._r_acumulado),
            ("SELECT id, role FROM app_users WHERE id IN (", lambda *ids: [{"id": i, "role": self.usuarios.get(i)} for i in ids]),
            ("SELECT tarea_id, cloudinary_url, archivo_path FROM mant_visita_fotos WHERE visita_id=%s AND tarea_id IN (", self._r_fotos_tareas),
            ("SELECT cloudinary_url, archivo_path FROM mant_visita_fotos WHERE visita_id=%s AND maquina_id=%s AND tarea_id IS NULL "
             "AND tipo_foto='falla' AND descripcion LIKE %s", self._r_fotos_equipo),
            ("SELECT url FROM mant_ot_repuesto_evidencias WHERE solicitud_id=%s", lambda sid: [{"url": u} for u in self.fotos_rep.get(sid, [])]),
            ("SELECT gcs_key FROM mant_incidencia_fotos WHERE incidencia_id=%s", lambda iid: [{"gcs_key": k} for k in self.fotos_inc.get(iid, [])]),
        ]

    # ── armado de la escena ──
    @staticmethod
    def _copia(x):
        return dict(x) if x else None

    def agregar(self, vid, ref="t1", origen="tarea", severidad="falla", titulo="Trotadora X: Falla", estado="abierto", **kw):
        self._sig += 1
        f = {c: None for c in HALL_COLS}
        f.update({"id": self._sig, "visita_id": vid, "origen": origen, "referencia": ref, "severidad": severidad, "titulo": titulo, "riesgo": 0,
                  "n_fotos": 0, "estado": estado, "detectado_por": "Pedro Técnico", "detectado_en": self.ahora})
        f.update(kw)
        self.hall.append(f)
        return f

    def _buscar(self, vid, origen, ref):
        return next((f for f in self.hall if (f["visita_id"], f["origen"], f["referencia"]) == (vid, origen, ref)), None)

    def _por_id(self, hid, vid=None):
        return next((f for f in self.hall if f["id"] == hid and (vid is None or f["visita_id"] == vid)), None)

    def _con_seg(self, f):
        if not f:
            return None
        v = self.visitas.get(f.get("ot_seguimiento_id")) if f.get("ot_seguimiento_id") else None
        return dict(f, seg_numero=(v or {}).get("numero_ot"), seg_estado=(v or {}).get("estado"))

    # ── lecturas ──
    def _ver(self, sql, params):
        s = " ".join(sql.split())
        p = tuple(params or ())
        self.sql.append((s, p))
        if self.falla and self.falla in s:
            raise RuntimeError("falla simulada")
        return s, p

    def fetchone(self, sql, params=None):
        s, p = self._ver(sql, params)
        if self.oculta_primera_lectura and s.startswith("SELECT id, estado, retirado_en FROM mant_ot_hallazgos"):
            self.oculta_primera_lectura = False            # simula la carrera: la primera lectura no ve la fila que otro acaba de insertar
            return None
        for prefijo, fn in self._uno:
            if s.startswith(prefijo):
                return fn(*p)
        raise AssertionError("SQL de lectura (uno) no previsto en la prueba: " + s[:220])

    def fetchall(self, sql, params=None):
        s, p = self._ver(sql, params)
        for prefijo, fn in self._todos:
            if s.startswith(prefijo):
                return fn(*p)
        raise AssertionError("SQL de lectura (todos) no previsto en la prueba: " + s[:220])

    def _r_clave(self, vid, origen, ref):
        f = self._buscar(vid, origen, ref)
        return {k: f[k] for k in ("id", "estado", "retirado_en")} if f else None

    def _r_titulo(self, vid, origen, ref):
        f = self._buscar(vid, origen, ref)
        return {"titulo": f["titulo"]} if f else None

    def _r_ultimo_grupo(self, vid):
        vals = [f["aviso_grupo_en"] for f in self.hall if f["visita_id"] == vid and f["aviso_grupo_en"]]
        return {"ult": max(vals) if vals else None}

    def _r_visita(self, vid):
        return self._copia(self.visitas.get(vid))

    def _r_fila_de(self, hid, vid):
        return self._con_seg(self._por_id(hid, vid))

    def _r_campana(self, tipo, vid, uid):
        c = [n for n in self.notifs if n["tipo"] == tipo and n["visita_id"] == vid and n["destino"] == uid and not n["archivada_at"]]
        return {"id": c[-1]["id"], "leida_at": c[-1]["leida_at"], "created_at": c[-1]["created_at"]} if c else None

    def _r_tarea(self, tid, vid):
        t = self.tareas.get(tid)
        return dict(t) if t and t.get("visita_id") == vid else None

    def _r_especifico(self, vid, origen, ref1, ref2):
        return {"x": 1} if any(f["visita_id"] == vid and f["origen"] == origen and f["referencia"] in (ref1, ref2) and f["retirado_en"] is None
                               for f in self.hall) else None

    def _r_listar(self, vid):
        return [self._con_seg(f) for f in self.hall if f["visita_id"] == vid]

    def _r_grupo(self, vid, grupo):
        return [dict(f) for f in self.hall if f["visita_id"] == vid and f["aviso_grupo_en"] == grupo and f["retirado_en"] is None]

    def _r_acumulado(self, vid, creada):
        desde = creada - dt.timedelta(minutes=2)
        return [dict(f) for f in self.hall if f["visita_id"] == vid and f["aviso_grupo_en"] and f["aviso_grupo_en"] >= desde and f["retirado_en"] is None]

    def _r_fotos_tareas(self, vid, *tids):
        return [{"tarea_id": t, "cloudinary_url": u, "archivo_path": None} for t in tids for u in self.fotos_tareas.get(t, [])]

    def _r_fotos_equipo(self, vid, mid, like):
        lista = self.fotos_equipo.get((vid, mid, like.rstrip("%")), [])
        return [{"cloudinary_url": u, "archivo_path": None} for u in reversed(lista)][:3]

    # ── escrituras ──
    def escribe(self, sql, params=None):
        s, p = self._ver(sql, params)
        return self._escribir(s, p)

    def _suelta(self, s):
        return dict(ot_seguimiento_id=None, seguimiento_por=None, seguimiento_en=None) if ", ot_seguimiento_id=NULL" in s else {}

    def _condicional(self, hid, vid, previo, cambio):
        if self.carrera:
            return 0
        f = self._por_id(hid, vid)
        if f and f["retirado_en"] is None and f["estado"] == previo:
            f.update(cambio)
            return 1
        return 0

    def _escribir(self, s, p):
        if s.startswith("INSERT IGNORE INTO mant_ot_hallazgos"):
            if self._buscar(p[0], p[1], p[2]):
                return 0
            self._sig += 1
            f = {c: None for c in HALL_COLS}
            f.update({"id": self._sig, "visita_id": p[0], "origen": p[1], "referencia": p[2], "tarea_id": p[3], "maquina_id": p[4],
                      "item_manual_id": p[5], "severidad": p[6], "titulo": p[7], "detalle": p[8], "equipo_nombre": p[9], "riesgo": p[10],
                      "fotos_json": p[11], "n_fotos": p[12], "estado": "abierto", "detectado_por": p[13], "detectado_en": self.ahora})
            self.hall.append(f)
            return 1
        if s.startswith("UPDATE mant_ot_hallazgos SET severidad=%s, titulo=%s, detalle=%s, equipo_nombre=%s, riesgo=%s, fotos_json=%s, n_fotos=%s, "
                        "estado=%s, retirado_en=NULL, retirado_motivo=NULL WHERE id=%s"):
            self._por_id(p[8]).update(severidad=p[0], titulo=p[1], detalle=p[2], equipo_nombre=p[3], riesgo=p[4], fotos_json=p[5], n_fotos=p[6],
                                      estado=p[7], retirado_en=None, retirado_motivo=None)
            return 1
        if s.startswith("UPDATE mant_ot_hallazgos SET retirado_en=NOW(), retirado_motivo=%s WHERE visita_id=%s AND origen=%s AND referencia=%s "
                        "AND retirado_en IS NULL"):
            f = self._buscar(p[1], p[2], p[3])
            if f and f["retirado_en"] is None:
                f.update(retirado_en=self.ahora, retirado_motivo=p[0])
                return 1
            return 0
        if s.startswith("UPDATE mant_ot_hallazgos SET aviso_grupo_en=%s WHERE id IN ("):
            for f in self.hall:
                if f["id"] in p[1:]:
                    f["aviso_grupo_en"] = p[0]
            return len(p) - 1
        if s.startswith("UPDATE mant_ot_hallazgos SET estado='tomado'"):
            uid, quien, hid, vid, previo = p
            return self._condicional(hid, vid, previo, dict(estado="tomado", tomado_por_id=uid, tomado_por=quien, tomado_en=self.ahora,
                                                           **self._suelta(s)))
        if s.startswith("UPDATE mant_ot_hallazgos SET estado='descartado'"):
            quien, motivo, hid, vid, previo = p
            return self._condicional(hid, vid, previo, dict(estado="descartado", descartado_por=quien, descartado_en=self.ahora,
                                                           descarte_motivo=motivo, **self._suelta(s)))
        if s.startswith("UPDATE mant_ot_hallazgos SET estado='abierto', tomado_por_id=NULL"):
            hid, vid = p
            return self._condicional(hid, vid, "descartado", dict(estado="abierto", tomado_por_id=None, tomado_por=None, tomado_en=None))
        if s.startswith("UPDATE mant_notificaciones SET titulo=%s, cuerpo=%s, mensaje=%s, prioridad=%s, url_accion=%s, leida_at=NULL, estado='pendiente' "
                        "WHERE id=%s"):
            n = next(x for x in self.notifs if x["id"] == p[5])
            n.update(titulo=p[0], cuerpo=p[1], prioridad=p[3], url=p[4], leida_at=None)
            return 1
        if s.startswith("CREATE TABLE IF NOT EXISTS mant_ot_hallazgos"):
            return 0
        raise AssertionError("SQL de escritura no previsto en la prueba: " + s[:220])

    # ── lo demás que el código llama ──
    def notificar(self, destino, tipo, titulo, cuerpo="", url_accion="", prioridad="media", cliente_id=None, visita_id=None):
        self._nid += 1
        self.notifs.append({"id": self._nid, "destino": destino, "tipo": tipo, "titulo": titulo, "cuerpo": cuerpo, "url": url_accion,
                            "prioridad": prioridad, "cliente_id": cliente_id, "visita_id": visita_id, "leida_at": None, "archivada_at": None,
                            "created_at": self.ahora})
        return self._nid

    def chile(self, valor, fmt="%d/%m/%Y %H:%M"):
        self.fmts.append(fmt)
        return "21/10 11:05"

    def ambiente(self, ok=True, **extra):
        m = self
        amb = {
            "print": lambda *a, **k: m.salida.append(" ".join(str(x) for x in a)), "time": time, "json": json, "re": re, "types": types,
            "_oh": H, "_pe": PE, "g": m.g, "request": m.request, "jsonify": lambda x: x, "get_db": lambda: m.db,
            "mysql_fetchone": m.fetchone, "mysql_fetchall": m.fetchall, "mysql_execute": m.escribe, "mysql_execute_returning_rowcount": m.escribe,
            "_mant_log": lambda ent, eid, acc, det="": m.logs.append((ent, eid, acc, det)),
            "_mant_notificar": m.notificar, "_mant_notif_cache_invalidar": lambda uid=None: m.caches.append(uid),
            "current_username": lambda: m.usuario.get("nombre"), "chile_fmt_filter": m.chile,
            "_es_rol_tecnico": lambda user=None: m.tecnico,
            "_ot_gestion_destinatarios": lambda vid, fila=None: [dict(p) for p in m.gestion],
            "_ot_fila_basica": lambda vid: m._copia(m.visitas.get(vid)),
            "_foto_url_actual": lambda row: row.get("cloudinary_url") or "",
            "_otrep_nombre_producto": lambda t: t, "_otpe_ok": lambda: True,
            "_OT2_ESTADO_META": {"programada": ("Programada",), "en_curso": ("En ejecución",), "cerrada": ("Cerrada",), "cancelada": ("Cancelada",)},
        }
        amb.update(extra)
        amb.setdefault("_gc", GC)
        amb.setdefault("_aviso_interno_correo", lambda *a, **k: m.__dict__.setdefault("correos_internos", []).append((a, k)) or 0)
        amb.setdefault("_es_superadmin_actual", lambda: bool(getattr(m, "superadmin", False)))
        amb = cargar(FUNCIONES_BLOQUE + ["_ot2_err", "_rol_familia"], CONSTANTES_BLOQUE, amb)
        amb["_HALL_ESTADO"]["ok"] = bool(ok)
        amb["_HALL_ESTADO"]["revisado_en"] = time.time()
        # 26-oct: la gestión circular queda DORMIDA en estas pruebas (sus tablas no están en la base de mentira); sus pruebas
        # propias viven en tests/test_gestion_circular.py.
        amb["_GC_ESTADO"]["ok"] = bool(extra.get("gc_ok", False))
        amb["_GC_ESTADO"]["revisado_en"] = time.time()
        return amb


def _resp(r):
    """(payload, http) de lo que devuelven las rutas (el jsonify de mentira es el propio dict)."""
    return r if isinstance(r, tuple) else (r, 200)


def _hall_h(**kw):
    h = {"origen": "tarea", "referencia": "t12", "tarea_id": 12, "maquina_id": 9, "item_manual_id": None, "severidad": "falla",
         "titulo": "Trotadora Pro · Revisar cinta: Falla", "detalle": None, "equipo_nombre": "Trotadora Pro", "riesgo": 0, "fotos": ["/f/a.jpg"]}
    h.update(kw)
    return h


class TestAltaYRetiro(unittest.TestCase):
    def setUp(self):
        self.m = Mundo(usuario=TEC)
        self.amb = self.m.ambiente()
        self.reg, self.ret = self.amb["_hall_registrar"], self.amb["_hall_retirar"]

    def test_un_hallazgo_nuevo_se_guarda_con_su_evidencia_y_avisa(self):
        hid = self.reg(171, _hall_h())
        self.assertIsInstance(hid, int)
        (f,) = self.m.hall
        self.assertEqual((f["visita_id"], f["origen"], f["referencia"], f["severidad"], f["estado"]), (171, "tarea", "t12", "falla", "abierto"))
        self.assertEqual((f["tarea_id"], f["maquina_id"], f["item_manual_id"]), (12, 9, None))
        self.assertEqual((f["n_fotos"], f["fotos_json"]), (1, '["/f/a.jpg"]'))
        self.assertEqual((f["detectado_por"], f["riesgo"]), ("Pedro Técnico", 0), "quién lo reportó sale de la sesión, nunca del navegador")
        self.assertEqual(self.m.logs, [("visita", 171, "hallazgo_detectado",
                                        "Falla: Trotadora Pro · Revisar cinta: Falla · 1 foto(s) · Pedro Técnico")])

    def test_el_mismo_hallazgo_otra_vez_no_se_duplica_ni_vuelve_a_avisar(self):
        self.reg(171, _hall_h())
        self.assertIsNone(self.reg(171, _hall_h()))
        self.assertEqual(len(self.m.hall), 1)
        self.assertEqual(len(self.m.logs), 1, "ni una línea más en la bitácora")

    def test_si_cambia_el_texto_o_las_fotos_se_actualiza_la_misma_fila(self):
        self.reg(171, _hall_h())
        self.assertIsNone(self.reg(171, _hall_h(severidad="alerta", titulo="Trotadora Pro · Revisar cinta: Alerta", fotos=["/f/a.jpg", "/f/b.jpg"])))
        (f,) = self.m.hall
        self.assertEqual((f["severidad"], f["n_fotos"]), ("alerta", 2))
        self.assertIn("Alerta", f["titulo"])

    def test_si_dos_peticiones_se_pisan_gana_una_sola_y_la_otra_no_avisa(self):
        self.m.agregar(171, ref="t12")                    # otra petición acaba de insertarla…
        self.m.oculta_primera_lectura = True              # …pero ESTA no la vio al leer
        self.assertIsNone(self.reg(171, _hall_h()))
        self.assertEqual(len(self.m.hall), 1)

    def test_un_hallazgo_retirado_que_reaparece_vuelve_a_avisar(self):
        self.m.agregar(171, ref="t12", retirado_en=self.m.ahora, retirado_motivo="x")
        hid = self.reg(171, _hall_h())
        self.assertIsNotNone(hid)
        (f,) = self.m.hall
        self.assertIsNone(f["retirado_en"])
        self.assertIsNone(f["retirado_motivo"])
        self.assertEqual(self.m.logs[0][2], "hallazgo_reabierto")

    def test_si_ya_lo_tenia_alguien_el_reaparecer_no_vuelve_a_avisar(self):
        self.m.agregar(171, ref="t12", estado="tomado", retirado_en=self.m.ahora, tomado_por="Aarón Rojas")
        self.assertIsNone(self.reg(171, _hall_h()))
        (f,) = self.m.hall
        self.assertEqual((f["estado"], f["tomado_por"], f["retirado_en"]), ("tomado", "Aarón Rojas", None))

    def test_un_descartado_retirado_que_reaparece_se_reabre_y_avisa(self):
        self.m.agregar(171, ref="t12", estado="descartado", retirado_en=self.m.ahora)
        self.assertIsNotNone(self.reg(171, _hall_h()))
        self.assertEqual(self.m.hall[0]["estado"], "abierto")

    def test_retirar_queda_anotado_y_la_segunda_vez_no_hace_nada(self):
        self.reg(171, _hall_h())
        self.assertEqual(self.ret(171, "tarea", "t12", "El técnico cambió su respuesta"), 1)
        (f,) = self.m.hall
        self.assertEqual(f["retirado_motivo"], "El técnico cambió su respuesta")
        self.assertIsNotNone(f["retirado_en"])
        self.assertEqual(self.ret(171, "tarea", "t12", "otra vez"), 0)
        self.assertEqual([l[2] for l in self.m.logs], ["hallazgo_detectado", "hallazgo_retirado"])
        self.assertEqual(self.m.logs[1][3], "Trotadora Pro · Revisar cinta: Falla: El técnico cambió su respuesta · Pedro Técnico",
                         "la bitácora dice QUÉ hallazgo era, no la clave interna")
        self.assertEqual(self.ret(171, "tarea", "t99", "no existe"), 0)

    def test_jamas_se_borra_nada(self):
        self.reg(171, _hall_h())
        self.ret(171, "tarea", "t12", "m")
        self.reg(171, _hall_h())
        self.assertFalse([s for s, _p in self.m.sql if s.startswith("DELETE")])
        self.assertEqual(len(self.m.hall), 1)


class TestGanchoDeTareas(unittest.TestCase):
    def setUp(self):
        self.m = Mundo(usuario=TEC)
        self.m.visitas[171] = dict(VISITA_171)
        self.m.maquinas[9] = {"nombre": "Trotadora Pro 3000", "sku": "TRO-3000", "serie": "S123"}
        self.m.tareas[12] = {"id": 12, "visita_id": 171, "titulo": "Revisar cinta", "tipo_respuesta": "verificacion", "opciones_lista_json": None,
                             "valor_json": json.dumps({"valor": "falla"}), "maquina_id": 9, "item_manual_id": None}
        self.m.fotos_tareas[12] = ["/f/fotos/a.jpg"]
        self.amb = self.m.ambiente()
        self.amb["_hall_avisar"] = lambda vid, ids: self.m.avisados.append((vid, list(ids)))
        self.h = self.amb["_hall_h_tarea"]

    def test_una_falla_con_foto_registra_el_hallazgo_con_el_nombre_completo(self):
        self.h(171, 12, {"valor_norm": {"valor": "falla"}})
        (f,) = self.m.hall
        self.assertEqual((f["origen"], f["referencia"], f["severidad"], f["tarea_id"], f["maquina_id"]), ("tarea", "t12", "falla", 12, 9))
        self.assertEqual(f["titulo"], "Trotadora Pro 3000 (SKU TRO-3000) · S/N S123 · Revisar cinta: Falla")
        self.assertEqual((f["n_fotos"], f["fotos_json"]), (1, '["/f/fotos/a.jpg"]'))
        self.assertEqual(self.m.avisados, [(171, [f["id"]])])

    def test_sin_foto_queda_con_cero_fotos_para_que_la_ficha_lo_diga(self):
        self.m.fotos_tareas = {}
        self.h(171, 12, {"valor_norm": {"valor": "falla"}})
        (f,) = self.m.hall
        self.assertEqual((f["n_fotos"], f["fotos_json"]), (0, None))

    def test_una_alerta_tambien_es_un_hallazgo(self):
        self.m.tareas[12]["valor_json"] = json.dumps({"valor": "alerta"})
        self.h(171, 12, {"valor_norm": {"valor": "alerta"}})
        self.assertEqual(self.m.hall[0]["severidad"], "alerta")
        self.assertTrue(self.m.hall[0]["titulo"].endswith("Revisar cinta: Alerta"))

    def test_volver_a_aprobado_retira_el_hallazgo_y_no_avisa(self):
        self.h(171, 12, {"valor_norm": {"valor": "falla"}})
        self.m.tareas[12]["valor_json"] = json.dumps({"valor": "aprobado"})
        self.h(171, 12, {"valor_norm": {"valor": "aprobado"}})
        (f,) = self.m.hall
        self.assertIsNotNone(f["retirado_en"])
        self.assertEqual(len(self.m.avisados), 1, "solo el aviso del alta")
        self.assertEqual(self.m.logs[-1][2], "hallazgo_retirado")

    def test_no_aplica_tambien_lo_retira(self):
        self.h(171, 12, {"valor_norm": {"valor": "falla"}})
        self.m.tareas[12]["valor_json"] = json.dumps({"valor": "falla", "na": True})
        self.h(171, 12, {"valor_norm": {"na": True}})
        self.assertIsNotNone(self.m.hall[0]["retirado_en"])

    def test_cambiar_de_falla_a_alerta_actualiza_la_misma_fila_sin_nuevo_aviso(self):
        self.h(171, 12, {"valor_norm": {"valor": "falla"}})
        self.m.tareas[12]["valor_json"] = json.dumps({"valor": "alerta"})
        self.h(171, 12, {"valor_norm": {"valor": "alerta"}})
        self.assertEqual(len(self.m.hall), 1)
        self.assertEqual(self.m.hall[0]["severidad"], "alerta")
        self.assertEqual(len(self.m.avisados), 1)

    def test_el_resultado_del_control_en_reparar_es_una_falla(self):
        self.m.tareas[12].update(titulo="Resultado del control", tipo_respuesta="lista", valor_json=json.dumps({"opcion": "Reparar"}),
                                 opciones_lista_json=json.dumps(["Apto", "Apto con observaciones", "Reparar", "Dar de baja"]))
        self.h(171, 12, {"valor_norm": {"opcion": "Reparar"}})
        (f,) = self.m.hall
        self.assertEqual(f["severidad"], "falla")
        self.assertTrue(f["titulo"].endswith("Resultado del control: Reparar"))

    def test_apto_no_es_un_hallazgo(self):
        self.m.tareas[12].update(titulo="Resultado del control", tipo_respuesta="lista", valor_json=json.dumps({"opcion": "Apto"}),
                                 opciones_lista_json=json.dumps(["Apto", "Apto con observaciones", "Reparar"]))
        self.h(171, 12, {"valor_norm": {"opcion": "Apto"}})
        self.assertEqual(self.m.hall, [])

    def test_las_respuestas_que_no_pueden_ser_hallazgo_ni_abren_la_base(self):
        for vn in ({"lat": 1.0, "lng": 2.0}, {"texto": "hola"}, {"numero": 3}, {"valor": "si"}, {}, None):
            self.h(171, 12, {"valor_norm": vn})
        self.h(171, None, {"valor_norm": {"valor": "falla"}})
        self.h(171, 0, {"valor_norm": {"valor": "falla"}})
        self.assertEqual(self.m.sql, [], "casi todas las respuestas del técnico no tocan la base")

    def test_una_tarea_de_producto_usa_el_nombre_del_producto_con_su_sku(self):
        self.m.tareas[13] = {"id": 13, "visita_id": 171, "titulo": "Revisar patas", "tipo_respuesta": "verificacion", "opciones_lista_json": None,
                             "valor_json": json.dumps({"valor": "falla"}), "maquina_id": None, "item_manual_id": 4}
        self.m.productos[(171, 4)] = {"titulo": "Banco plano X", "item_manual_sku": "BP-1", "plantilla_id": None}
        self.h(171, 13, {"valor_norm": {"valor": "falla"}})
        (f,) = self.m.hall
        self.assertEqual((f["item_manual_id"], f["maquina_id"]), (4, None))
        self.assertEqual(f["titulo"], "Banco plano X (SKU BP-1) · Revisar patas: Falla")

    def test_una_tarea_que_no_es_de_esta_ot_no_registra_nada(self):
        self.m.tareas[12]["visita_id"] = 999
        self.h(171, 12, {"valor_norm": {"valor": "falla"}})
        self.assertEqual(self.m.hall, [])


class TestGanchoDeProductos(unittest.TestCase):
    def setUp(self):
        self.m = Mundo(usuario=TEC)
        self.m.visitas[300] = dict(VISITA_171, id=300, numero_ot="OT-2026-00300", cliente_id=None, razon_social=None)
        self.m.productos[(300, 4)] = {"titulo": "Banco plano X", "item_manual_sku": "BP-1", "plantilla_id": None}
        self.amb = self.m.ambiente()
        self.amb["_hall_avisar"] = lambda vid, ids: self.m.avisados.append((vid, list(ids)))
        self.h = self.amb["_hall_h_producto"]

    def _estado(self, estado, **kw):
        f = {"estado": estado, "motivo": "Cable o piola cortada", "detalle": "Se cortó al tensar", "riesgo_lesion": 1,
             "fotos_json": '["/f/p1.jpg", "/f/p2.jpg"]'}
        f.update(kw)
        self.m.estado_producto[(300, 4)] = f

    def test_no_se_puede_usar_registra_un_fuera_de_servicio_con_riesgo_y_fotos(self):
        self._estado("fuera_servicio")
        self.h(300, 4)
        (f,) = self.m.hall
        self.assertEqual((f["origen"], f["referencia"], f["severidad"], f["riesgo"], f["n_fotos"]), ("producto", "i4:fuera_servicio", "fuera_servicio", 1, 2))
        self.assertEqual(f["titulo"], "Banco plano X (SKU BP-1): No se puede usar — Cable o piola cortada")
        self.assertEqual(f["detalle"], "Se cortó al tensar")
        self.assertEqual(self.m.avisados, [(300, [f["id"]])])

    def test_no_tiene_arreglo_es_una_baja_y_retira_el_fuera_de_servicio_anterior(self):
        self._estado("fuera_servicio")
        self.h(300, 4)
        self._estado("baja", motivo="Estructura quebrada o deformada")
        self.h(300, 4)
        por_ref = {f["referencia"]: f for f in self.m.hall}
        self.assertEqual(set(por_ref), {"i4:fuera_servicio", "i4:baja"})
        self.assertIsNotNone(por_ref["i4:fuera_servicio"]["retirado_en"])
        self.assertIsNone(por_ref["i4:baja"]["retirado_en"])
        self.assertEqual(por_ref["i4:baja"]["severidad"], "baja")

    def test_revertir_la_accion_retira_el_hallazgo(self):
        self._estado("fuera_servicio")
        self.h(300, 4)
        del self.m.estado_producto[(300, 4)]
        self.h(300, 4)
        self.assertIsNotNone(self.m.hall[0]["retirado_en"])
        self.assertEqual(self.m.logs[-1][2], "hallazgo_retirado")

    def test_no_llego_no_es_un_hallazgo_lo_cubre_que_paso(self):
        self._estado("no_llego")
        self.h(300, 4)
        self.assertEqual(self.m.hall, [])
        self.assertEqual(self.m.avisados, [])

    def test_sin_la_tabla_de_acciones_no_hace_nada(self):
        self.amb["_otpe_ok"] = lambda: False
        self._estado("fuera_servicio")
        self.h(300, 4)
        self.assertEqual(self.m.hall, [])


class TestGanchoDeEquipos(unittest.TestCase):
    def setUp(self):
        self.m = Mundo(usuario=TEC)
        self.m.visitas[171] = dict(VISITA_171)
        self.m.maquinas[9] = {"nombre": "Trotadora Pro 3000", "sku": "TRO-3000", "serie": "S123"}
        self.nombre = "Trotadora Pro 3000 (SKU TRO-3000) · S/N S123"
        self.amb = self.m.ambiente()
        self.amb["_hall_avisar"] = lambda vid, ids: self.m.avisados.append((vid, list(ids)))

    def test_fuera_de_servicio_con_formulario_motivo_detalle_riesgo_y_fotos(self):
        self.m.form.update({"motivo_clave": "no_enciende", "detalle": "No prende aunque se conecta", "riesgo": "si"})
        self.m.fotos_equipo[(171, 9, "Fuera de servicio")] = ["/f/fs1.jpg", "/f/fs2.jpg"]
        self.amb["_hall_h_equipo_fs"](171, 9, {"estado": "fuera_servicio"})
        (f,) = self.m.hall
        self.assertEqual((f["origen"], f["referencia"], f["severidad"], f["riesgo"], f["n_fotos"]), ("equipo", "m9:fuera_servicio", "fuera_servicio", 1, 2))
        self.assertEqual(f["titulo"], self.nombre + ": No se puede usar — No enciende o no responde")
        self.assertEqual(f["detalle"], "No prende aunque se conecta")
        self.assertEqual(len(self.m.avisados), 1)

    def test_fuera_de_servicio_con_el_json_antiguo(self):
        self.m.body = {"motivo": "Se rompió la banda"}
        self.amb["_hall_h_equipo_fs"](171, 9, {"estado": "fuera_servicio"})
        self.assertTrue(self.m.hall[0]["titulo"].endswith("No se puede usar — Se rompió la banda"))
        self.assertEqual(self.m.hall[0]["riesgo"], 0)

    def test_volver_a_operativo_retira_el_hallazgo(self):
        self.amb["_hall_h_equipo_fs"](171, 9, {"estado": "fuera_servicio"})
        self.amb["_hall_h_equipo_fs"](171, 9, {"estado": "operativo"})
        self.assertIsNotNone(self.m.hall[0]["retirado_en"])

    def test_estados_ajenos_o_equipo_invalido_no_hacen_nada(self):
        for mid, j in ((9, {"estado": "otro"}), (9, {}), (None, {"estado": "fuera_servicio"}), (0, {"estado": "fuera_servicio"})):
            self.amb["_hall_h_equipo_fs"](171, mid, j)
        self.assertEqual(self.m.hall, [])

    def test_baja_del_equipo(self):
        self.m.form["motivo"] = "Se partió el marco"
        self.m.fotos_equipo[(171, 9, "Baja del equipo")] = ["/f/b1.jpg"]
        self.amb["_hall_h_equipo_baja"](171, 9, {"estado": "baja"})
        (f,) = self.m.hall
        self.assertEqual((f["referencia"], f["severidad"], f["detalle"], f["n_fotos"]), ("m9:baja", "baja", "Se partió el marco", 1))
        self.assertEqual(f["titulo"], self.nombre + ": No tiene arreglo (dar de baja)")

    def test_un_equipo_que_ya_estaba_de_baja_no_vuelve_a_registrarse(self):
        self.amb["_hall_h_equipo_baja"](171, 9, {"estado": "baja", "ya_de_baja": True})
        self.amb["_hall_h_equipo_baja"](171, 9, {"estado": "otro"})
        self.assertEqual(self.m.hall, [])

    def _falla(self, **kw):
        f = {"estado_revision": "falla_detectada", "razon_saltado": None, "observacion_tecnico": "Ruido en el motor",
             "diagnostico_estado": None, "diagnostico_texto": "Rodamiento gastado"}
        f.update(kw)
        self.m.equipos_visita[(171, 9)] = f

    def test_falla_detectada_se_registra_con_el_diagnostico(self):
        self._falla()
        self.amb["_hall_h_equipo_falla"](171, 9)
        (f,) = self.m.hall
        self.assertEqual((f["referencia"], f["severidad"], f["detalle"]), ("m9:falla", "falla", "Rodamiento gastado"))
        self.assertEqual(f["titulo"], self.nombre + ": Falla detectada")
        self.assertEqual(f["n_fotos"], 0)

    def test_si_ya_hay_un_fuera_de_servicio_o_una_baja_no_se_duplica_con_la_falla_generica(self):
        self.m.agregar(171, ref="m9:fuera_servicio", origen="equipo", severidad="fuera_servicio")
        self._falla()
        self.amb["_hall_h_equipo_falla"](171, 9)
        self.assertEqual([f["referencia"] for f in self.m.hall], ["m9:fuera_servicio"])

    def test_la_falla_deja_de_serlo_si_se_aprueba_o_se_marca_fuera_de_servicio(self):
        self._falla()
        self.amb["_hall_h_equipo_falla"](171, 9)
        for cambio in ({"diagnostico_estado": "aprobado"}, {"razon_saltado": "fuera_de_servicio"}, {"estado_revision": "ok"}):
            self.m.hall[0]["retirado_en"] = None
            self._falla(**cambio)
            self.amb["_hall_h_equipo_falla"](171, 9)
            self.assertIsNotNone(self.m.hall[0]["retirado_en"], cambio)

    def test_si_las_columnas_del_diagnostico_no_existen_igual_funciona(self):
        self._falla()
        self.m.falla = "diagnostico_estado"
        self.amb["_hall_h_equipo_falla"](171, 9)
        self.assertEqual(self.m.hall[0]["referencia"], "m9:falla")

    def test_pedir_repuesto_de_un_equipo_con_lote_solo_nombres_y_cantidades(self):
        self.m.form["motivo"] = "Se cortó la piola"
        self.m.solicitudes[55] = {"repuesto_nombre": "Piola (cable de acero)", "cantidad": 2, "n_fotos": 2, "n_videos": 0}
        self.m.fotos_rep[55] = ["/f/ev1.jpg", "/f/ev2.jpg"]
        j = {"lote_id": "ab12cd34ef56ab12cd34ef56ab12cd34", "solicitudes": [
            {"solicitud_id": 55, "repuesto_nombre": "Piola (cable de acero)", "cantidad": 2, "proveedor": "NO DEBE SALIR"},
            {"solicitud_id": 56, "repuesto_nombre": "Pin selector", "cantidad": 1}], "fuera_servicio": False}
        self.amb["_hall_h_repuesto"](171, j, mid=9)
        (f,) = self.m.hall
        self.assertEqual((f["referencia"], f["severidad"], f["n_fotos"]), ("m9:rep:ab12cd34", "repuesto", 2))
        self.assertEqual(f["titulo"], self.nombre + ": Repuesto solicitado: Piola (cable de acero) × 2, Pin selector × 1")
        self.assertEqual(f["detalle"], "Se cortó la piola · Evidencia: 2 foto(s).")
        self.assertNotIn("NO DEBE SALIR", json.dumps(f, default=str))

    def test_pedir_repuesto_y_dejar_fuera_de_servicio_registra_los_dos(self):
        self.m.form["motivo"] = "Se cortó la piola y no se puede usar"
        j = {"lote_id": "ab12cd34ef56ab12cd34ef56ab12cd34", "solicitudes": [{"solicitud_id": 55, "repuesto_nombre": "Piola", "cantidad": 1}],
             "fuera_servicio": True}
        self.amb["_hall_h_repuesto"](171, j, mid=9)
        self.assertEqual(sorted(f["referencia"] for f in self.m.hall), ["m9:fuera_servicio", "m9:rep:ab12cd34"])
        self.assertEqual(self.m.avisados[0][1].__len__(), 2, "un solo envío de aviso con los dos hallazgos")

    def test_pedir_un_repuesto_suelto_usa_la_solicitud_como_clave(self):
        self.m.solicitudes[55] = {"repuesto_nombre": "Piola", "cantidad": 1, "n_fotos": 1, "n_videos": 1}
        self.amb["_hall_h_repuesto"](171, {"solicitud_id": 55}, mid=9)
        (f,) = self.m.hall
        self.assertEqual(f["referencia"], "m9:rep:s55")
        self.assertIn("Evidencia: 1 foto(s), 1 video(s).", f["detalle"])

    def test_repuesto_de_un_producto_de_una_ot_interna(self):
        self.m.productos[(171, 4)] = {"titulo": "Banco plano X", "item_manual_sku": "BP-1", "plantilla_id": None}
        j = {"lote_id": "cd34ef56ab12cd34ef56ab12cd34ef56", "solicitudes": [{"solicitud_id": 70, "repuesto_nombre": "Tapiz", "cantidad": 1}]}
        self.amb["_hall_h_repuesto"](171, j, imid=4)
        (f,) = self.m.hall
        self.assertEqual((f["origen"], f["referencia"], f["item_manual_id"], f["maquina_id"]), ("producto", "i4:rep:cd34ef56", 4, None))
        self.assertTrue(f["titulo"].startswith("Banco plano X (SKU BP-1): Repuesto solicitado: Tapiz × 1"))

    def test_sin_lineas_o_sin_equipo_ni_producto_no_hace_nada(self):
        self.amb["_hall_h_repuesto"](171, {"lote_id": "x", "solicitudes": []}, mid=9)
        self.amb["_hall_h_repuesto"](171, {"solicitud_id": 55}, mid=None, imid=None)
        self.amb["_hall_h_repuesto"](171, {}, mid=9)
        self.assertEqual(self.m.hall, [])

    def test_informar_una_incidencia(self):
        self.m.form.update({"motivo": "Golpe en el transporte", "detalle": "La caja venía abierta"})
        self.m.fotos_inc[77] = ["incidencias/a.jpg"]
        self.amb["_hall_h_incidencia"](171, {"incidencia_id": 77}, mid=9)
        (f,) = self.m.hall
        self.assertEqual((f["referencia"], f["severidad"], f["detalle"], f["n_fotos"]), ("m9:inc:77", "incidencia", "La caja venía abierta", 1))
        self.assertEqual(f["titulo"], self.nombre + ": Incidencia 77: Golpe en el transporte")
        self.assertEqual(f["fotos_json"], '["/f/incidencias/a.jpg"]')

    def test_incidencia_sin_numero_no_registra(self):
        self.amb["_hall_h_incidencia"](171, {}, mid=9)
        self.amb["_hall_h_incidencia"](171, {"incidencia_id": 77}, mid=None, imid=None)
        self.assertEqual(self.m.hall, [])


class TestGanchoDeRutas(unittest.TestCase):
    """El único `after_request` que mira qué ruta acaba de guardar algo: best-effort, jamás rompe la respuesta del técnico."""

    def _escena(self, endpoint="mant_visita_tarea_respuesta", metodo="POST", status=200, va=None, payload=None, es_json=True, passthrough=False, ok=True):
        m = Mundo(usuario=TEC)
        m.request.method, m.request.endpoint = metodo, endpoint
        m.request.view_args = {"vid": 171, "tid": 12} if va is None else va
        llamadas = []
        amb = m.ambiente(ok=ok)
        amb["_hall_despachar"] = lambda clave, vid, va_, j: llamadas.append((clave, vid, dict(va_), j))
        resp = types.SimpleNamespace(status_code=status, direct_passthrough=passthrough, is_json=es_json, get_json=lambda silent=True: payload)
        return m, amb, resp, llamadas

    def test_una_respuesta_guardada_dispara_el_manejador_de_su_ruta(self):
        m, amb, resp, llamadas = self._escena(payload={"ok": True, "valor_norm": {"valor": "falla"}})
        self.assertIs(amb["_hall_tras_escritura"](resp), resp)
        self.assertEqual(llamadas, [("tarea", 171, {"vid": 171, "tid": 12}, {"ok": True, "valor_norm": {"valor": "falla"}})])

    def test_cada_ruta_de_la_tabla_va_a_su_manejador(self):
        for endpoint, clave in json.loads(json.dumps(cargar([], ("_HALL_RUTAS",))["_HALL_RUTAS"])).items():
            m, amb, resp, llamadas = self._escena(endpoint=endpoint, va={"vid": 5, "mid": 9, "imid": 4, "tid": 12}, payload={})
            amb["_hall_tras_escritura"](resp)
            self.assertEqual([l[0] for l in llamadas], [clave], endpoint)

    def test_lo_que_no_guardo_no_dispara_nada(self):
        casos = {"GET": dict(metodo="GET"), "error 400": dict(status=400), "error 409": dict(status=409), "error 500": dict(status=500),
                 "archivo": dict(passthrough=True), "otra ruta": dict(endpoint="ot2_detalle"), "sin endpoint": dict(endpoint=""),
                 "sin OT en la URL": dict(va={"tid": 12}), "OT inválida": dict(va={"vid": "x"}), "sin tabla": dict(ok=False)}
        for nombre, kw in casos.items():
            m, amb, resp, llamadas = self._escena(payload={}, **kw)
            self.assertIs(amb["_hall_tras_escritura"](resp), resp, nombre)
            self.assertEqual(llamadas, [], nombre)

    def test_antes_de_leer_cierra_la_foto_de_la_conexion_repeatable_read(self):
        """La conexión de la petición es REPEATABLE READ con autocommit apagado: lo que la ruta guardó por OTRA conexión (repuestos,
        incidencias, acciones por producto) no se vería y el hallazgo saldría sin fotos o ni se registraría (mismo gotcha que
        `_fac_db_fresca`: el remedio es el commit que termina la lectura anterior, ANTES de releer)."""
        m, amb, resp, _llamadas = self._escena(payload={})
        orden = []
        m.db.commit = lambda: orden.append("commit")
        amb["_hall_despachar"] = lambda *a: orden.append("despacha")
        amb["_hall_tras_escritura"](resp)
        self.assertEqual(orden, ["commit", "despacha"])

    def test_lo_que_se_ignora_no_toca_la_conexion(self):
        casos = {"GET": dict(metodo="GET"), "error": dict(status=500), "otra ruta": dict(endpoint="ot2_detalle"), "sin tabla": dict(ok=False),
                 "sin OT": dict(va={"tid": 12})}
        for nombre, kw in casos.items():
            m, amb, resp, _l = self._escena(payload={}, **kw)
            amb["_hall_tras_escritura"](resp)
            self.assertEqual(m.commits, [], nombre)

    def test_si_el_commit_falla_el_hallazgo_igual_se_registra(self):
        m, amb, resp, llamadas = self._escena(payload={})

        def roto():
            raise RuntimeError("conexión perdida")
        m.db.commit = roto
        self.assertIs(amb["_hall_tras_escritura"](resp), resp)
        self.assertEqual(len(llamadas), 1)

    def test_un_cuerpo_que_no_es_un_objeto_se_entrega_como_vacio(self):
        for payload, es_json in (([1, 2], True), (None, True), ({"a": 1}, False)):
            m, amb, resp, llamadas = self._escena(payload=payload, es_json=es_json)
            amb["_hall_tras_escritura"](resp)
            self.assertEqual(llamadas[0][3], {}, (payload, es_json))

    def test_si_el_manejador_falla_la_respuesta_del_tecnico_sale_intacta(self):
        m, amb, resp, llamadas = self._escena(payload={})

        def roto(*a):
            raise RuntimeError("se cayó la base")
        amb["_hall_despachar"] = roto
        self.assertIs(amb["_hall_tras_escritura"](resp), resp)
        self.assertTrue(any("tras escritura mant_visita_tarea_respuesta" in s for s in m.salida), m.salida)

    def test_el_despachador_llama_al_manejador_correcto_con_los_argumentos_de_la_url(self):
        m = Mundo(usuario=TEC)
        amb = m.ambiente()
        visto = []
        for nombre in ("_hall_h_tarea", "_hall_h_producto", "_hall_h_equipo_fs", "_hall_h_equipo_baja", "_hall_h_equipo_falla", "_hall_h_repuesto",
                       "_hall_h_incidencia"):
            amb[nombre] = (lambda n: lambda *a, **k: visto.append((n, a, k)))(nombre)
        d = amb["_hall_despachar"]
        va = {"vid": 5, "tid": 12, "imid": 4, "mid": 9}
        d("tarea", 5, va, {"x": 1})
        d("producto", 5, va, {})
        d("equipo_fs", 5, va, {"estado": "fuera_servicio"})
        d("equipo_baja", 5, va, {})
        d("equipo_falla", 5, va, {})
        d("repuesto_equipo", 5, va, {"r": 1})
        d("repuesto_producto", 5, va, {"r": 2})
        d("incidencia_equipo", 5, va, {"i": 1})
        d("incidencia_producto", 5, va, {"i": 2})
        d("clave_inventada", 5, va, {})
        self.assertEqual(visto, [
            ("_hall_h_tarea", (5, 12, {"x": 1}), {}), ("_hall_h_producto", (5, 4), {}), ("_hall_h_equipo_fs", (5, 9, {"estado": "fuera_servicio"}), {}),
            ("_hall_h_equipo_baja", (5, 9, {}), {}), ("_hall_h_equipo_falla", (5, 9), {}),
            ("_hall_h_repuesto", (5, {"r": 1}), {"mid": 9}), ("_hall_h_repuesto", (5, {"r": 2}), {"imid": 4}),
            ("_hall_h_incidencia", (5, {"i": 1}), {"mid": 9}), ("_hall_h_incidencia", (5, {"i": 2}), {"imid": 4})])


class TestCampanaAgrupada(unittest.TestCase):
    def setUp(self):
        self.m = Mundo(usuario=TEC)
        self.m.visitas[171] = dict(VISITA_171)
        self.amb = self.m.ambiente()
        self.avisar = self.amb["_hall_avisar"]
        self.t0 = self.m.ahora

    def _nuevo(self, ref, sev="falla", **kw):
        return self.m.agregar(171, ref=ref, severidad=sev, titulo="Equipo %s: %s" % (ref, sev), **kw)

    def _de(self, uid):
        return [n for n in self.m.notifs if n["destino"] == uid]

    def test_la_primera_campana_va_a_la_gestion_sin_tecnicos(self):
        h1 = self._nuevo("t1")
        self.assertEqual(self.avisar(171, [h1["id"]]), 3)
        self.assertEqual(sorted(n["destino"] for n in self.m.notifs), [2, 3, 4], "Aarón (responsable), Juan Pablo y Víctor; NO Lenin, que es técnico")
        c = self._de(3)[0]
        self.assertEqual((c["tipo"], c["titulo"], c["url"], c["prioridad"], c["visita_id"]),
                         ("ot_hallazgo", "OT-2026-00171: 1 hallazgo nuevo", "/ot/hallazgos?ot=171#hzpTarjeta", "alta", 171))
        self.assertTrue(c["cuerpo"].startswith("Aurum Fit Girls Spa: Falla — Equipo t1: falla"), c["cuerpo"])
        self.assertIn("Lo reportó Pedro Técnico.", c["cuerpo"])
        self.assertEqual(self.m.caches, [3, 4, 2], "el responsable primero, y la campana de cada persona se refresca")
        self.assertEqual(h1["aviso_grupo_en"], self.t0)
        self.assertEqual(self.m.salida, [], "sin errores en el registro")

    def test_quien_causa_el_hallazgo_no_se_avisa_a_si_mismo(self):
        m = Mundo(usuario=GESTOR)
        m.visitas[171] = dict(VISITA_171)
        amb = m.ambiente()
        h = m.agregar(171, ref="t1")
        amb["_hall_avisar"](171, [h["id"]])
        self.assertEqual(sorted(n["destino"] for n in m.notifs), [2, 3])

    def test_lo_que_llega_en_30_minutos_se_suma_a_la_misma_campana(self):
        h1 = self._nuevo("t1")
        self.avisar(171, [h1["id"]])
        self.m.ahora = self.t0 + dt.timedelta(minutes=10)
        h2 = self._nuevo("t2", "alerta")
        self.avisar(171, [h2["id"]])
        self.assertEqual(len(self.m.notifs), 3, "una sola campana por OT y por persona")
        for n in self.m.notifs:
            self.assertEqual(n["titulo"], "OT-2026-00171: 2 hallazgos nuevos")
            self.assertIsNone(n["leida_at"])
            self.assertIn("Equipo t1: falla", n["cuerpo"])
            self.assertIn("Equipo t2: alerta", n["cuerpo"])
        self.assertEqual(h2["aviso_grupo_en"], self.t0, "entra al grupo que ya existía")

    def test_si_ya_la_leyo_y_llega_algo_dentro_de_la_ventana_vuelve_a_quedar_sin_leer(self):
        h1 = self._nuevo("t1")
        self.avisar(171, [h1["id"]])
        self._de(3)[0]["leida_at"] = self.t0 + dt.timedelta(minutes=2)
        self.m.ahora = self.t0 + dt.timedelta(minutes=15)
        h3 = self._nuevo("t3")
        self.avisar(171, [h3["id"]])
        c = self._de(3)
        self.assertEqual(len(c), 1)
        self.assertIsNone(c[0]["leida_at"], "llegó información nueva")
        self.assertEqual(c[0]["titulo"], "OT-2026-00171: 2 hallazgos nuevos")

    def test_pasados_30_minutos_la_leida_crea_otra_y_la_sin_leer_cuenta_todo_lo_que_no_ha_visto(self):
        for ref in ("t1", "t2", "t3"):
            self.m.ahora = self.t0 + dt.timedelta(minutes={"t1": 0, "t2": 10, "t3": 15}[ref])
            self.avisar(171, [self._nuevo(ref)["id"]])
        self._de(4)[0]["leida_at"] = self.t0 + dt.timedelta(minutes=20)         # Juan Pablo la leyó; Aarón y Víctor, no
        self.m.ahora = self.t0 + dt.timedelta(minutes=45)
        h4 = self._nuevo("t4", "alerta")
        self.avisar(171, [h4["id"]])
        self.assertEqual(h4["aviso_grupo_en"], self.m.ahora, "grupo nuevo")
        self.assertEqual(len(self._de(4)), 2, "a quien ya la leyó le llega otra campana")
        self.assertEqual(self._de(4)[1]["titulo"], "OT-2026-00171: 1 hallazgo nuevo")
        self.assertEqual(len(self._de(3)), 1, "a quien no la ha leído se le actualiza la misma")
        self.assertEqual(self._de(3)[0]["titulo"], "OT-2026-00171: 4 hallazgos nuevos", "el número no «baja» mientras no la haya visto")
        self.assertIn("Equipo t1: falla", self._de(3)[0]["cuerpo"])
        self.assertEqual(self._de(2)[0]["titulo"], "OT-2026-00171: 4 hallazgos nuevos")

    def test_prioridad_de_la_campana(self):
        h = self._nuevo("t1", "alerta")
        self.avisar(171, [h["id"]])
        self.assertEqual({n["prioridad"] for n in self.m.notifs}, {"media"})
        self.m.ahora = self.t0 + dt.timedelta(minutes=1)
        h2 = self._nuevo("t2", "fuera_servicio", riesgo=1)
        self.avisar(171, [h2["id"]])
        self.assertEqual({n["prioridad"] for n in self.m.notifs}, {"urgente"}, "puede lastimar a alguien")
        self.assertIn("(PUEDE LASTIMAR A ALGUIEN)", self.m.notifs[0]["cuerpo"])

    def test_sin_trabajo_que_avisar_no_hace_nada(self):
        self.assertEqual(self.avisar(171, []), 0)
        self.assertEqual(self.avisar(999, [1]), 0, "la OT ya no existe")
        h = self._nuevo("t1", retirado_en=self.t0)
        self.assertEqual(self.avisar(171, [h["id"]]), 0, "un hallazgo retirado no genera campana")
        self.m.gestion = [{"id": 9, "nombre": "Lenin Urbina", "correo": "", "por": "creador"}]
        self.assertEqual(self.avisar(171, [self._nuevo("t2")["id"]]), 0, "si la única persona es un técnico, no se avisa a nadie")
        self.assertEqual(self.m.notifs, [])

    def test_un_error_con_una_persona_no_impide_avisar_a_las_demas(self):
        original = self.m.notificar

        def con_falla(destino, *a, **k):
            if destino == 3:
                raise RuntimeError("falla simulada")
            return original(destino, *a, **k)
        self.amb["_mant_notificar"] = con_falla
        self.assertEqual(self.avisar(171, [self._nuevo("t1")["id"]]), 2)
        self.assertEqual(sorted(n["destino"] for n in self.m.notifs), [2, 4])
        self.assertTrue(any("campana a 3" in s for s in self.m.salida))

    def test_una_ot_interna_dice_trabajo_interno(self):
        self.m.visitas[171].update(cliente_id=None, razon_social=None)
        self.avisar(171, [self._nuevo("t1")["id"]])
        self.assertTrue(self.m.notifs[0]["cuerpo"].startswith("Trabajo interno:"))

    def test_si_el_aviso_de_tomar_el_caso_existe_no_se_confunde_con_la_campana_de_nuevos(self):
        self.amb["_hall_avisar_toma"](171, dict(VISITA_171), {"titulo": "Algo: Falla"}, "Víctor Soto", 2)
        self.assertEqual([n["tipo"] for n in self.m.notifs], ["ot_hallazgo_toma"])
        self.avisar(171, [self._nuevo("t1")["id"]])
        self.assertEqual(sorted(n["tipo"] for n in self.m.notifs if n["destino"] == 3), ["ot_hallazgo", "ot_hallazgo_toma"])
        self.assertEqual(len([n for n in self.m.notifs if n["tipo"] == "ot_hallazgo"]), 3, "la campana de nuevos se crea igual")


class TestDestinatarios(unittest.TestCase):
    def test_solo_gestion_el_responsable_primero_y_sin_quien_lo_causo(self):
        m = Mundo(usuario=TEC)
        amb = m.ambiente()
        d = amb["_hall_destinos"](171, dict(VISITA_171))
        self.assertEqual([(p["id"], p["por"]) for p in d], [(3, "responsable"), (4, "creador"), (2, "ejecutivo")], "Lenin (técnico) queda afuera")
        d2 = amb["_hall_destinos"](171, dict(VISITA_171), excluir_uid=3)
        self.assertEqual([p["id"] for p in d2], [4, 2])

    def test_la_lista_sale_de_la_funcion_unica_de_la_gestion(self):
        m = Mundo(usuario=TEC)
        llamadas = []
        amb = m.ambiente(_ot_gestion_destinatarios=lambda vid, fila=None: llamadas.append((vid, fila is not None)) or [])
        amb["_hall_destinos"](171, dict(VISITA_171))
        self.assertEqual(llamadas, [(171, True)])

    def test_cada_rol_tecnico_queda_afuera(self):
        m = Mundo(usuario=GESTOR)
        m.usuarios.update({20: "tecnico_ejecutivo", 21: "tecnico_externo", 22: "tecnico"})
        m.gestion = [{"id": i, "nombre": "Persona %d" % i, "correo": "", "por": "respaldo"} for i in (20, 21, 22, 3)]
        d = m.ambiente()["_hall_destinos"](171, dict(VISITA_171))
        self.assertEqual([p["id"] for p in d], [3], "REGLAS #19 y #26: esta gestión no es de ningún técnico (interno, elevado ni externo)")

    @staticmethod
    def _gestion_solo_tecnico(llamadas):
        """Lo que devolvería `_ot_gestion_destinatarios`: el único candidato de la OT es un técnico (Lenin creó la OT, sin responsable ni
        ejecutivo); con «una OT sin nadie» (`fila` con solo el id) cae al respaldo de siempre."""
        def gestion(vid, fila=None):
            sin_nadie = sorted((fila or {}).keys()) == ["id"]
            llamadas.append(sin_nadie)
            if sin_nadie:
                return [{"id": 7, "nombre": "Daniel Aguilar", "correo": "", "por": "respaldo"},
                        {"id": 4, "nombre": "Juan Pablo Díaz", "correo": "", "por": "respaldo"}]
            return [{"id": 9, "nombre": "Lenin Urbina", "correo": "", "por": "creador"}]
        return gestion

    def test_si_los_unicos_candidatos_son_tecnicos_se_pide_el_respaldo_a_la_misma_funcion(self):
        m = Mundo(usuario=GESTOR)
        m.usuarios.update({7: "superadmin", 4: "supervisor", 9: "tecnico"})
        llamadas = []
        amb = m.ambiente(_ot_gestion_destinatarios=self._gestion_solo_tecnico(llamadas))
        d = amb["_hall_destinos"](171, dict(VISITA_171), excluir_uid=4)
        self.assertEqual([(p["id"], p["por"]) for p in d], [(7, "respaldo")], "el respaldo de siempre, sin quien lo causó")
        self.assertEqual(llamadas, [False, True], "primero la lista de la OT y, como no sirvió, la de «una OT sin nadie»")

    def test_el_aviso_no_se_pierde_cuando_la_ot_solo_conocia_a_un_tecnico(self):
        m = Mundo(usuario=TEC)
        m.usuarios.update({7: "superadmin", 4: "supervisor", 9: "tecnico"})
        m.visitas[171] = dict(VISITA_171)
        llamadas = []
        amb = m.ambiente(_ot_gestion_destinatarios=self._gestion_solo_tecnico(llamadas))
        h = m.agregar(171, ref="t1")
        self.assertEqual(amb["_hall_avisar"](171, [h["id"]]), 2)
        self.assertEqual(sorted(n["destino"] for n in m.notifs), [4, 7])

    def test_si_el_unico_candidato_es_quien_lo_causo_o_no_hay_nadie_no_se_busca_mas(self):
        for lista in ([{"id": 4, "nombre": "Juan Pablo Díaz", "correo": "", "por": "creador"}], []):
            m = Mundo(usuario=GESTOR)
            llamadas = []
            amb = m.ambiente(_ot_gestion_destinatarios=lambda vid, fila=None, _l=lista: llamadas.append(1) or [dict(p) for p in _l])
            self.assertEqual(amb["_hall_destinos"](171, dict(VISITA_171), excluir_uid=4), [])
            self.assertEqual(len(llamadas), 1, "quien lo causó es gestión: ya lo sabe; y sin nadie no hay a quién pedirle")

    def test_si_no_se_pueden_leer_los_roles_no_se_avisa_a_nadie_en_vez_de_avisar_a_ciegas(self):
        m = Mundo(usuario=TEC)
        m.falla = "FROM app_users"
        self.assertEqual(m.ambiente()["_hall_destinos"](171, dict(VISITA_171)), [])
        self.assertTrue(any("roles de los destinatarios" in s for s in m.salida))


class TestAvisoDeToma(unittest.TestCase):
    def _a(self, usuario=GESTOR, gestion=None):
        m = Mundo(usuario=usuario)
        m.visitas[171] = dict(VISITA_171)
        if gestion is not None:
            m.gestion = gestion
        return m, m.ambiente()

    def test_al_responsable_si_lo_toma_otra_persona(self):
        m, amb = self._a()
        self.assertEqual(amb["_hall_avisar_toma"](171, dict(VISITA_171), {"titulo": "Trotadora X: Falla"}, "Juan Pablo Díaz", 4), 1)
        (c,) = m.notifs
        self.assertEqual((c["destino"], c["tipo"], c["prioridad"], c["cliente_id"], c["visita_id"], c["url"]),
                         (3, "ot_hallazgo_toma", "baja", 26, 171, "/ot/hallazgos?ot=171#hzpTarjeta"))
        self.assertEqual(c["titulo"], "OT-2026-00171: alguien tomó un hallazgo")
        self.assertIn("Juan Pablo Díaz tomó el caso: Trotadora X: Falla.", c["cuerpo"])
        self.assertEqual(m.caches, [3])

    def test_si_el_responsable_es_quien_lo_toma_no_se_avisa_a_si_mismo(self):
        m, amb = self._a(usuario={"id": 3, "nombre": "Aarón Rojas", "username": "aaron@sphs.cl", "role": "admin"})
        self.assertEqual(amb["_hall_avisar_toma"](171, dict(VISITA_171), {"titulo": "X"}, "Aarón Rojas", 3), 0)
        self.assertEqual(m.notifs, [])

    def test_sin_responsable_no_se_avisa_a_nadie(self):
        m, amb = self._a(gestion=[{"id": 4, "nombre": "Juan Pablo Díaz", "correo": "", "por": "creador"},
                                  {"id": 2, "nombre": "Víctor Soto", "correo": "", "por": "ejecutivo"}])
        self.assertEqual(amb["_hall_avisar_toma"](171, dict(VISITA_171), {"titulo": "X"}, "Alguien", 99), 0)
        self.assertEqual(m.notifs, [])

    def test_un_responsable_tecnico_no_recibe_nada(self):
        m, amb = self._a(gestion=[{"id": 9, "nombre": "Lenin Urbina", "correo": "", "por": "responsable"}])
        self.assertEqual(amb["_hall_avisar_toma"](171, dict(VISITA_171), {"titulo": "X"}, "Alguien", 99), 0)


class TestRutasDeGestion(unittest.TestCase):
    def setUp(self):
        self.m = Mundo(usuario=GESTOR)
        self.m.visitas[171] = dict(VISITA_171)
        self.amb = self.m.ambiente()
        self.f = self.m.agregar(171, ref="t1", titulo="Trotadora X: Falla")

    def _tomar(self, hid=None, vid=171):
        return _resp(self.amb["ot_api_hallazgo_tomar"](vid, hid or self.f["id"]))

    def _descartar(self, motivo, hid=None):
        self.m.body = {"motivo": motivo} if motivo is not None else {}
        return _resp(self.amb["ot_api_hallazgo_descartar"](171, hid or self.f["id"]))

    def _reabrir(self, hid=None):
        return _resp(self.amb["ot_api_hallazgo_reabrir"](171, hid or self.f["id"]))

    # ── tomar ──
    def test_tomar_el_caso_queda_a_nombre_de_quien_lo_toma_y_avisa_suave_al_responsable(self):
        j, http = self._tomar()
        self.assertEqual((http, j), (200, {"ok": True, "estado": "tomado", "tomado_por": "Juan Pablo Díaz"}))
        self.assertEqual((self.f["estado"], self.f["tomado_por_id"], self.f["tomado_por"], self.f["tomado_en"]), ("tomado", 4, "Juan Pablo Díaz", self.m.ahora))
        upd = [(s, p) for s, p in self.m.sql if s.startswith("UPDATE mant_ot_hallazgos SET estado='tomado'")]
        self.assertEqual(len(upd), 1)
        self.assertEqual(upd[0][1], (4, "Juan Pablo Díaz", self.f["id"], 171, "abierto"), "el UPDATE comprueba el estado que se vio (otra persona pudo cambiarlo)")
        self.assertIn("retirado_en IS NULL", upd[0][0])
        self.assertEqual(self.m.logs, [("visita", 171, "hallazgo_tomado", "Trotadora X: Falla · lo tomó Juan Pablo Díaz")])
        self.assertEqual([(n["destino"], n["tipo"]) for n in self.m.notifs], [(3, "ot_hallazgo_toma")])

    def test_no_se_puede_tomar_dos_veces_ni_lo_descartado_ni_lo_retirado_ni_lo_que_tiene_ot(self):
        self._tomar()
        j, http = self._tomar()
        self.assertEqual((http, j["codigo"]), (409, "HALLAZGO_NO_DISPONIBLE"))
        self.assertIn("Juan Pablo Díaz", j["error"])
        d = self.m.agregar(171, ref="t2", estado="descartado")
        r = self.m.agregar(171, ref="t3", retirado_en=self.m.ahora)
        self.m.visitas[300] = {"id": 300, "numero_ot": "OT-2026-00300", "estado": "programada"}
        o = self.m.agregar(171, ref="t4", estado="resuelto", ot_seguimiento_id=300)
        for h in (d, r, o):
            j, http = self._tomar(h["id"])
            self.assertEqual((http, j["codigo"]), (409, "HALLAZGO_NO_DISPONIBLE"), h["referencia"])
            self.assertEqual(h["estado"], {"t2": "descartado", "t3": "abierto", "t4": "resuelto"}[h["referencia"]], "nada cambió")

    def test_un_hallazgo_inexistente_o_de_otra_ot_responde_404(self):
        self.assertEqual(self._tomar(9999)[1], 404)
        otra = self.m.agregar(172, ref="t1")
        j, http = self._tomar(otra["id"])
        self.assertEqual((http, j["codigo"]), (404, "HALLAZGO_NO_EXISTE"), "el hallazgo es de la OT 172, no de la 171")

    def test_si_alguien_cambio_la_fila_mientras_tanto_responde_409_y_no_anota_nada(self):
        self.m.carrera = True
        j, http = self._tomar()
        self.assertEqual((http, j["codigo"]), (409, "HALLAZGO_CAMBIO"))
        self.assertEqual(self.m.logs, [])
        self.assertEqual(self.m.notifs, [])

    def test_sin_la_tabla_responde_503_sin_romper(self):
        amb = self.m.ambiente(ok=False)
        j, http = _resp(amb["ot_api_hallazgo_tomar"](171, self.f["id"]))
        self.assertEqual((http, j["codigo"]), (503, "TABLA_NO_DISPONIBLE"))

    def test_si_la_ot_de_seguimiento_se_cancelo_el_caso_se_puede_volver_a_tomar_y_se_suelta_el_vinculo(self):
        self.m.visitas[301] = {"id": 301, "numero_ot": "OT-2026-00301", "estado": "cancelada"}
        h = self.m.agregar(171, ref="t9", estado="resuelto", ot_seguimiento_id=301, seguimiento_por="Víctor Soto")
        j, http = self._tomar(h["id"])
        self.assertEqual(http, 200, j)
        self.assertEqual((h["estado"], h["ot_seguimiento_id"], h["seguimiento_por"]), ("tomado", None, None))

    def test_un_error_al_avisar_no_impide_tomar_el_caso(self):
        self.amb["_hall_avisar_toma"] = lambda *a: (_ for _ in ()).throw(RuntimeError("falla simulada"))
        j, http = self._tomar()
        self.assertEqual(http, 200)
        self.assertEqual(self.f["estado"], "tomado")
        self.assertTrue(any("aviso de toma" in s for s in self.m.salida))

    # ── descartar ──
    def test_descartar_exige_el_motivo_y_lo_deja_escrito(self):
        for malo in (None, "", "   ", "corto"):
            j, http = self._descartar(malo)
            self.assertEqual((http, j["codigo"]), (400, "MOTIVO_REQUERIDO"), repr(malo))
            self.assertEqual(self.f["estado"], "abierto")
        j, http = self._descartar("  El cliente ya cambió  el equipo por uno nuevo ")
        self.assertEqual((http, j), (200, {"ok": True, "estado": "descartado"}))
        self.assertEqual((self.f["estado"], self.f["descartado_por"], self.f["descarte_motivo"]), ("descartado", "Juan Pablo Díaz", "El cliente ya cambió el equipo por uno nuevo"))
        self.assertEqual(self.m.logs, [("visita", 171, "hallazgo_descartado",
                                        "Trotadora X: Falla · lo descartó Juan Pablo Díaz · motivo: El cliente ya cambió el equipo por uno nuevo")])
        self.assertFalse([s for s, _p in self.m.sql if s.startswith("DELETE")], "descartar NO borra")

    def test_quien_tomo_el_caso_tambien_puede_descartar(self):
        self._tomar()
        j, http = self._descartar("Se resolvió en la misma visita del técnico")
        self.assertEqual(http, 200, j)
        upd = [p for s, p in self.m.sql if s.startswith("UPDATE mant_ot_hallazgos SET estado='descartado'")]
        self.assertEqual(upd[0][-1], "tomado")

    def test_no_se_descarta_lo_que_ya_tiene_una_ot_viva_ni_lo_ya_descartado(self):
        self.m.visitas[300] = {"id": 300, "numero_ot": "OT-2026-00300", "estado": "programada"}
        con_ot = self.m.agregar(171, ref="t2", estado="resuelto", ot_seguimiento_id=300)
        self.assertEqual(self._descartar("Motivo suficientemente largo", con_ot["id"])[1], 409)
        self._descartar("Motivo suficientemente largo")
        self.assertEqual(self._descartar("Motivo suficientemente largo")[1], 409)

    def test_descartar_con_carrera_no_anota_nada(self):
        self.m.carrera = True
        j, http = self._descartar("Motivo suficientemente largo")
        self.assertEqual((http, j["codigo"]), (409, "HALLAZGO_CAMBIO"))
        self.assertEqual(self.m.logs, [])

    # ── reabrir ──
    def test_reabrir_un_descartado_lo_deja_sin_gestionar_y_queda_en_la_bitacora(self):
        self._tomar()
        self._descartar("Motivo suficientemente largo")
        j, http = self._reabrir()
        self.assertEqual((http, j), (200, {"ok": True, "estado": "abierto"}))
        self.assertEqual((self.f["estado"], self.f["tomado_por"], self.f["tomado_en"]), ("abierto", None, None))
        self.assertEqual(self.m.logs[-1], ("visita", 171, "hallazgo_reabierto", "Trotadora X: Falla · lo reabrió Juan Pablo Díaz"))
        self.assertEqual(self.f["descarte_motivo"], "Motivo suficientemente largo", "el descarte anterior sigue escrito")

    def test_solo_se_reabre_lo_descartado(self):
        j, http = self._reabrir()
        self.assertEqual((http, j["codigo"]), (409, "HALLAZGO_NO_DISPONIBLE"))
        self.assertEqual(self.f["estado"], "abierto")

    def test_reabrir_con_carrera_no_anota_nada(self):
        self.f["estado"] = "descartado"
        self.m.carrera = True
        j, http = self._reabrir()
        self.assertEqual((http, j["codigo"]), (409, "HALLAZGO_CAMBIO"))
        self.assertEqual(self.m.logs, [])
        self.assertEqual(self.f["estado"], "descartado")


class TestRutaPrellenado(unittest.TestCase):
    def setUp(self):
        self.m = Mundo(usuario=GESTOR)
        self.m.visitas[171] = dict(VISITA_171)
        self.m.fotos_tareas[12] = ["/f/live/t12.jpg"]
        self.h1 = self.m.agregar(171, ref="t12", tarea_id=12, maquina_id=9, titulo="Trotadora Pro 3000 · Revisar cinta: Falla",
                                 equipo_nombre="Trotadora Pro 3000")
        self.h2 = self.m.agregar(171, ref="m11:baja", origen="equipo", severidad="baja", maquina_id=11, titulo="Bici X: No tiene arreglo (dar de baja)",
                                 equipo_nombre="Bici X", detalle="Marco partido", fotos_json='["/f/b.jpg"]', n_fotos=1)
        self.amb = self.m.ambiente()

    def _pedir(self, ids, vid=171):
        self.m.args["ids"] = ids
        return _resp(self.amb["ot_api_hallazgos_prellenado"](vid))

    def test_lo_prellenado_del_asistente(self):
        j, http = self._pedir("%d,%d" % (self.h2["id"], self.h1["id"]))
        self.assertEqual(http, 200, j)
        self.assertEqual(j["origen"], {"id": 171, "numero_ot": "OT-2026-00171", "interna": False,
                                       "cliente": {"id": 26, "nombre": "Aurum Fit Girls Spa", "rut": "76.123.456-7"}})
        self.assertEqual([h["id"] for h in j["hallazgos"]], [self.h1["id"], self.h2["id"]], "la falla primero, después la baja")
        p = j["prellenado"]
        self.assertEqual(p["titulo"], "Seguimiento de OT-2026-00171 · 2 hallazgos · Aurum Fit Girls Spa")
        self.assertEqual((p["tipo_sugerido"], p["maquinas"]), ("correctiva", [9, 11]))
        self.assertEqual(p["fotos"], ["/f/live/t12.jpg", "/f/b.jpg"], "las fotos de la tarea se releen vivas y se suman a las guardadas")
        self.assertIn("• Falla — Trotadora Pro 3000 · Revisar cinta: Falla", p["descripcion"])
        self.assertIn("• Dar de baja — Bici X: No tiene arreglo (dar de baja). Marco partido", p["descripcion"])
        self.assertIsNone(_PLATA.search(json.dumps(j, ensure_ascii=False)), "REGLAS #19/#26: nada de plata ni documentos")

    def test_con_un_solo_hallazgo_el_titulo_nombra_el_equipo(self):
        j, _h = self._pedir(str(self.h1["id"]))
        self.assertEqual(j["prellenado"]["titulo"], "Seguimiento de OT-2026-00171 · Trotadora Pro 3000")

    def test_una_ot_interna_no_lleva_cliente(self):
        self.m.visitas[171].update(cliente_id=None, razon_social=None, rut=None)
        j, http = self._pedir("%d,%d" % (self.h1["id"], self.h2["id"]))
        self.assertEqual(http, 200)
        self.assertEqual((j["origen"]["interna"], j["origen"]["cliente"]), (True, None))
        self.assertEqual(j["prellenado"]["titulo"], "Seguimiento de OT-2026-00171 · 2 hallazgos")

    def test_lo_que_no_se_puede_ligar_responde_409_con_su_motivo(self):
        otra = self.m.agregar(172, ref="t1")
        self.m.visitas[300] = {"id": 300, "numero_ot": "OT-2026-00300", "estado": "programada"}
        con_ot = self.m.agregar(171, ref="t5", estado="resuelto", ot_seguimiento_id=300)
        casos = [("%d" % otra["id"], "SEG_HALLAZGO_AJENO"), ("9999", "SEG_HALLAZGO_AJENO"), ("", "SEG_SIN_HALLAZGOS"), ("x,0", "SEG_SIN_HALLAZGOS"),
                 ("%d" % con_ot["id"], "SEG_HALLAZGO_NO_DISPONIBLE")]
        for ids, codigo in casos:
            j, http = self._pedir(ids)
            self.assertEqual((http, j["codigo"]), (409, codigo), ids)
        self.h1["retirado_en"] = self.m.ahora
        self.assertEqual(self._pedir(str(self.h1["id"]))[0]["codigo"], "SEG_HALLAZGO_NO_DISPONIBLE")
        self.h2["estado"] = "descartado"
        self.assertEqual(self._pedir(str(self.h2["id"]))[0]["codigo"], "SEG_HALLAZGO_NO_DISPONIBLE")

    def test_ot_inexistente_y_sin_tabla(self):
        j, http = self._pedir(str(self.h1["id"]), vid=999)
        self.assertEqual((http, j["codigo"]), (404, "OT_NO_EXISTE"))
        amb = self.m.ambiente(ok=False)
        self.m.args["ids"] = "1"
        j, http = _resp(amb["ot_api_hallazgos_prellenado"](171))
        self.assertEqual((http, j["codigo"]), (503, "TABLA_NO_DISPONIBLE"))


def escena_aurum(tecnico=False, ok=True, **visita):
    """La OT 171 de Aurum con un hallazgo de cada clase: falla sin foto en el checklist (la foto se sube viva), equipo fuera de servicio con riesgo
    y 2 fotos, una alerta que Aarón ya tomó, un repuesto con OT de seguimiento viva (300), una incidencia descartada, una baja cuya OT de
    seguimiento se canceló (301) y una alerta que el técnico retiró."""
    m = Mundo(usuario=GESTOR, tecnico=tecnico)
    v = dict(VISITA_171, **visita)
    m.visitas[171] = v
    m.visitas[300] = {"id": 300, "numero_ot": "OT-2026-00300", "estado": "programada", "cliente_id": 26, "ot_madre_id": 171, "ot_relacion": "seguimiento",
                      "firma_cliente_url": None}
    m.visitas[301] = {"id": 301, "numero_ot": "OT-2026-00301", "estado": "cancelada", "cliente_id": 26}
    m.fotos_tareas[12] = ["/f/live/t12.jpg"]
    a = m.agregar
    h = types.SimpleNamespace(m=m, v=v, amb=m.ambiente(ok=ok))
    h.h1 = a(171, ref="t12", severidad="falla", tarea_id=12, maquina_id=9, titulo="Trotadora Pro 3000 · Revisar cinta: Falla",
             equipo_nombre="Trotadora Pro 3000")
    h.h2 = a(171, ref="m9:fuera_servicio", origen="equipo", severidad="fuera_servicio", riesgo=1, titulo="Trotadora Pro 3000: No se puede usar",
             fotos_json='["/f/fs1.jpg","/f/fs2.jpg"]', n_fotos=2, detalle="No enciende")
    h.h3 = a(171, ref="t13", severidad="alerta", estado="tomado", tomado_por="Aarón Rojas", tomado_por_id=3, tomado_en=m.ahora,
             titulo="Banco plano X (SKU BP-1) · Revisar tapiz: Alerta")
    h.h4 = a(171, ref="m9:rep:ab12cd34", origen="equipo", severidad="repuesto", estado="resuelto", ot_seguimiento_id=300,
             titulo="Trotadora Pro 3000: Repuesto solicitado: Piola × 2")
    h.h5 = a(171, ref="m9:inc:5", origen="equipo", severidad="incidencia", estado="descartado", descartado_por="Víctor Soto",
             descartado_en=m.ahora, descarte_motivo="El cliente ya lo resolvió", titulo="Trotadora Pro 3000: Incidencia 5: Golpe en el transporte")
    h.h6 = a(171, ref="m9:baja", origen="equipo", severidad="baja", estado="resuelto", ot_seguimiento_id=301,
             titulo="Bici estática X: No tiene arreglo (dar de baja)")
    h.h7 = a(171, ref="t14", severidad="alerta", retirado_en=m.ahora, retirado_motivo="El técnico cambió su respuesta: ya no es un hallazgo",
             titulo="Polea · Revisar giro: Alerta")
    return h


class TestFichaDeLaOT(unittest.TestCase):
    """Lo que se dibuja en la tarjeta «Hallazgos de esta OT» (lo arma el servidor; a un técnico no viaja nada)."""

    def _escena(self, tecnico=False, ok=True, **visita):
        e = escena_aurum(tecnico=tecnico, ok=ok, **visita)
        self.m, self.amb, self.v = e.m, e.amb, e.v
        for k in ("h1", "h2", "h3", "h4", "h5", "h6", "h7"):
            setattr(self, k, getattr(e, k))
        return e.m

    def _ctx(self, vid=171):
        return self.amb["_hall_contexto_ficha"](vid, self.m.visitas[vid])

    def test_la_lista_va_por_gravedad_con_el_semaforo_y_lo_retirado_aparte(self):
        self._escena()
        c = self._ctx()
        self.assertEqual([i["id"] for i in c["activos"]], [self.h1["id"], self.h2["id"], self.h6["id"], self.h3["id"], self.h4["id"], self.h5["id"]])
        self.assertEqual([i["id"] for i in c["retirados"]], [self.h7["id"]])
        self.assertEqual([i["sev_color"] for i in c["activos"]], ["rojo", "rojo", "negro", "ambar", "azul", "gris"])
        self.assertEqual((c["vid"], c["numero_ot"], c["cerrada"]), (171, "OT-2026-00171", False))
        self.assertEqual((c["n_total"], c["n_abiertos"], c["n_tomados"], c["n_con_ot"], c["n_descartados"], c["n_retirados"]), (7, 3, 1, 1, 1, 1))
        self.assertIsNone(c["origen"])

    def test_la_foto_es_la_evidencia_las_de_la_tarea_se_releen_vivas_y_sin_foto_se_dice(self):
        self._escena()
        c = self._ctx()
        por_id = {i["id"]: i for i in c["activos"]}
        self.assertEqual(por_id[self.h1["id"]]["fotos"], ["/f/live/t12.jpg"], "el técnico sube la foto DESPUÉS de responder: se relee viva")
        self.assertEqual(por_id[self.h2["id"]]["fotos"], ["/f/fs1.jpg", "/f/fs2.jpg"])
        self.assertTrue(por_id[self.h3["id"]]["sin_foto"])
        self.assertFalse(por_id[self.h1["id"]]["sin_foto"])
        self.assertEqual(c["n_sin_foto"], len([i for i in c["activos"] if not i["fotos"]]))
        self.assertEqual(c["n_sin_foto"], 4)

    def test_el_estado_efectivo_y_el_seguimiento_caido(self):
        self._escena()
        por_id = {i["id"]: i for i in self._ctx()["activos"]}
        caido = por_id[self.h6["id"]]
        self.assertEqual((caido["estado_ef"], caido["estado_txt"], caido["seguimiento_caido"], caido["seguimiento"]), ("abierto", "Sin gestionar", True, None))
        self.assertEqual(caido["seguimiento_caido_txt"], "La OT de seguimiento OT-2026-00301 fue cancelada: el hallazgo volvió a quedar sin gestionar.")
        vivo = por_id[self.h4["id"]]
        self.assertEqual(vivo["estado_ef"], "resuelto")
        self.assertEqual(vivo["seguimiento"], {"id": 300, "numero_ot": "OT-2026-00300", "estado": "programada", "estado_txt": "Programada",
                                               "cerrada": False, "url": "/ot/300"})
        self.assertEqual(por_id[self.h3["id"]]["tomado_por"], "Aarón Rojas")
        self.assertEqual(por_id[self.h5["id"]]["descarte_motivo"], "El cliente ya lo resolvió")

    def test_que_botones_admite_cada_hallazgo(self):
        self._escena()
        por_id = {i["id"]: i for i in self._ctx()["activos"]}

        def acc(h):
            i = por_id[h["id"]]
            return (i["puede_tomar"], i["puede_levantar"], i["puede_descartar"], i["puede_reabrir"])
        self.assertEqual(acc(self.h1), (True, True, True, False))
        self.assertEqual(acc(self.h3), (False, True, True, False), "tomado: se puede levantar la OT o descartar, no volver a tomar")
        self.assertEqual(acc(self.h4), (False, False, False, False), "con OT de seguimiento viva no se toca")
        self.assertEqual(acc(self.h5), (False, False, False, True), "descartado: solo se reabre")
        self.assertEqual(acc(self.h6), (True, True, True, False), "su OT de seguimiento se canceló: vuelve a estar disponible")
        self.assertEqual(self._ctx()["n_seleccionables"], 4)

    def test_hora_en_chile_en_cada_fecha(self):
        self._escena()
        i = self._ctx()["activos"][0]
        self.assertEqual((i["cuando"], i["quien"]), ("21/10 11:05", "Pedro Técnico"))
        self.assertIn("%d/%m/%Y %H:%M", self.m.fmts, "REGLA #6: toda fecha pasa por chile_fmt_filter y lleva el año (auditoría, H-24)")
        self.assertNotIn("%d/%m %H:%M", self.m.fmts, "ninguna fecha sin año")

    def test_una_ot_cerrada_igual_muestra_y_deja_gestionar(self):
        for estado in ("pendiente_aprobacion", "completada", "cerrada", "cancelada", "anulada"):
            self._escena(estado=estado)
            c = self._ctx()
            self.assertTrue(c["cerrada"], estado)
            self.assertEqual(c["n_abiertos"], 3, "los hallazgos siguen gestionables")
            self.assertTrue(c["activos"][0]["puede_tomar"])
        self._escena(firma_cliente_url="/f/firma.png")
        self.assertTrue(self._ctx()["cerrada"])
        self._escena(estado="en_curso")
        self.assertFalse(self._ctx()["cerrada"])

    def test_un_tecnico_no_recibe_nada(self):
        self._escena(tecnico=True)
        self.assertIsNone(self._ctx(), "REGLAS #19 y #26: el dato se quita en el servidor, no solo en la plantilla")

    def test_sin_tabla_o_sin_hallazgos_no_hay_tarjeta(self):
        self._escena(ok=False)
        self.assertIsNone(self._ctx())
        m = Mundo(usuario=GESTOR)
        m.visitas[171] = dict(VISITA_171)
        self.assertIsNone(m.ambiente()["_hall_contexto_ficha"](171, m.visitas[171]))

    def test_una_ot_de_seguimiento_dice_de_cual_viene_y_por_cuales_hallazgos(self):
        self._escena()
        c = self._ctx(300)
        self.assertEqual(c["n_total"], 0)
        self.assertEqual(c["origen"]["id"], 171)
        self.assertEqual((c["origen"]["numero_ot"], c["origen"]["url"]), ("OT-2026-00171", "/ot/171"))
        self.assertEqual(c["origen"]["hallazgos"], [{"titulo": self.h4["titulo"], "sev_texto": "Repuesto", "sev_color": "azul", "n_fotos": 0}])

    def test_otra_relacion_madre_hija_no_es_un_seguimiento(self):
        self._escena()
        self.m.visitas[300]["ot_relacion"] = "desinstalacion"
        self.assertIsNone(self._ctx(300))

    def test_si_la_ot_de_origen_ya_no_existe_no_se_rompe(self):
        self._escena()
        del self.m.visitas[171]
        self.m.visitas[300]["ot_madre_id"] = 171
        self.assertIsNone(self._ctx(300))

    def test_un_error_de_la_base_no_rompe_la_ficha(self):
        self._escena()
        self.m.falla = "FROM mant_ot_hallazgos h LEFT JOIN"
        self.assertIsNone(self._ctx())
        self.assertTrue(any("listar vid=171" in s for s in self.m.salida))


class TestVinculoConLaOtNueva(unittest.TestCase):
    """`_hall_seguimiento_validar` (antes de escribir) y `_hall_seguimiento_enlazar` (dentro de la transacción de `_ot2_crear_core`)."""

    class Cur:
        def __init__(self, filas, falla_fotos=False):
            self.sql, self._filas, self._ult, self.falla_fotos = [], filas, [], falla_fotos

        def execute(self, sql, params=None):
            s = " ".join(sql.split())
            self.sql.append((s, tuple(params or ())))
            if "FOR UPDATE" in s:
                self._ult = [dict(f) for f in self._filas]
            if s.startswith("INSERT INTO mant_visita_adjuntos") and self.falla_fotos:
                raise RuntimeError("falla simulada")
            return 1

        def fetchall(self):
            return self._ult

    def setUp(self):
        self.m = Mundo(usuario=GESTOR)
        self.m.visitas[171] = dict(VISITA_171)
        self.m.fotos_tareas[12] = ["/f/live/t12.jpg"]
        self.h1 = self.m.agregar(171, ref="t12", tarea_id=12, titulo="Trotadora X: Falla", fotos_json='["/f/a.jpg"]', n_fotos=1)
        self.h2 = self.m.agregar(171, ref="m9:baja", origen="equipo", severidad="baja", titulo="Banco Y: No tiene arreglo")
        self.amb = self.m.ambiente()
        self.validar = self.amb["_hall_seguimiento_validar"]

    def _raw(self, **kw):
        r = {"origen_id": 171, "hallazgo_ids": [self.h1["id"], self.h2["id"]], "fotos": ["/f/a.jpg"]}
        r.update(kw)
        return r

    # ── validar ──
    def test_camino_feliz_y_solo_las_fotos_que_ya_son_evidencia_de_esos_hallazgos(self):
        seg, err = self.validar(self._raw(fotos=["/f/a.jpg", "/f/live/t12.jpg", "/f/inventada-por-el-navegador.jpg"]), 26)
        self.assertIsNone(err)
        self.assertEqual([h["id"] for h in seg["filas"]], [self.h1["id"], self.h2["id"]])
        self.assertEqual(seg["origen"]["numero_ot"], "OT-2026-00171")
        self.assertEqual(seg["fotos"], ["/f/a.jpg", "/f/live/t12.jpg"], "una URL cualquiera que mande el navegador no entra")

    def test_sin_seguimiento_no_hay_nada_que_validar(self):
        for raw in (None, {}, "x", [], 5):
            self.assertEqual(self.validar(raw, 26), (None, None), repr(raw))

    def test_lo_que_no_cuadra_con_la_base_se_rechaza_con_su_codigo(self):
        otra = self.m.agregar(172, ref="t1")
        self.m.visitas[300] = {"id": 300, "numero_ot": "OT-2026-00300", "estado": "programada"}
        con_ot = self.m.agregar(171, ref="t7", estado="resuelto", ot_seguimiento_id=300)
        casos = [(self._raw(origen_id=999), 26, "SEG_ORIGEN_NO_EXISTE"), (self._raw(hallazgo_ids=[]), 26, "SEG_SIN_HALLAZGOS"),
                 (self._raw(hallazgo_ids=[otra["id"]]), 26, "SEG_HALLAZGO_AJENO"), (self._raw(), 27, "SEG_OTRO_CLIENTE"),
                 (self._raw(), None, "SEG_OTRO_CLIENTE"), (self._raw(hallazgo_ids=[con_ot["id"]]), 26, "SEG_HALLAZGO_NO_DISPONIBLE")]
        for raw, cliente, codigo in casos:
            seg, err = self.validar(raw, cliente)
            self.assertIsNone(seg)
            self.assertEqual(err[1], codigo, raw)
            self.assertGreater(len(err[0]), 15)

    def test_sin_la_tabla_no_hay_vinculo_posible_y_se_dice_como_seguir(self):
        amb = self.m.ambiente(ok=False)
        seg, err = amb["_hall_seguimiento_validar"](self._raw(), 26)
        self.assertEqual((seg, err[1]), (None, "SEG_NO_DISPONIBLE"))
        self.assertIn("sin el vínculo", err[0])

    def test_las_fotos_de_referencia_tienen_tope(self):
        self.h2["fotos_json"] = json.dumps(["/f/x%d.jpg" % i for i in range(10)])
        seg, _e = self.validar(self._raw(fotos=["/f/x%d.jpg" % i for i in range(10)]), 26)
        self.assertEqual(len(seg["fotos"]), H.FOTOS_REFERENCIA_MAX)

    # ── enlazar ──
    def _seg(self, fotos=("/f/a.jpg", "/f/b.jpg")):
        return {"origen": {"id": 171, "numero_ot": "OT-2026-00171"},
                "filas": [{"id": 1, "titulo": "Trotadora X: Falla"}, {"id": 2, "titulo": "Banco Y: No se puede usar"}], "fotos": list(fotos)}

    def _filas_bd(self, **kw):
        base = [{"id": 1, "estado": "abierto", "retirado_en": None, "ot_seguimiento_id": None},
                {"id": 2, "estado": "tomado", "retirado_en": None, "ot_seguimiento_id": None}]
        for f in base:
            f.update(kw)
        return base

    def test_enlazar_deja_todo_en_la_misma_transaccion(self):
        cur = self.Cur(self._filas_bd())
        ok, msg, n = self.amb["_hall_seguimiento_enlazar"](cur, 300, self._seg(), "OT-2026-00300", "Daniel Aguilar")
        self.assertEqual((ok, msg, n), (True, None, 2))
        sqls = [s for s, _p in cur.sql]
        self.assertIn("FOR UPDATE", sqls[0])
        self.assertEqual(cur.sql[0][1], (171, 1, 2))
        self.assertTrue(sqls[1].startswith("UPDATE mant_ot_hallazgos SET estado='resuelto', ot_seguimiento_id=%s, seguimiento_por=%s, seguimiento_en=NOW()"))
        self.assertIn("tomado_por=COALESCE(tomado_por, %s)", sqls[1], "quien levanta la OT queda como quien tomó el caso, sin pisar a otra persona")
        self.assertEqual(cur.sql[1][1], (300, "Daniel Aguilar", 4, "Daniel Aguilar", 171, 1, 2))
        self.assertEqual(sqls[2], "UPDATE mant_visitas SET ot_madre_id=%s, ot_relacion=%s WHERE id=%s")
        self.assertEqual(cur.sql[2][1], (171, "seguimiento", 300))
        self.assertEqual(sqls[3], "SAVEPOINT hall_fotos")
        fotos = [(s, p) for s, p in cur.sql if s.startswith("INSERT INTO mant_visita_adjuntos")]
        self.assertEqual(len(fotos), 2)
        self.assertIn("'foto'", fotos[0][0])
        self.assertEqual(fotos[0][1], (300, "/f/a.jpg", "hallazgo_OT-2026-00300_1",
                                       "Hallazgo de OT-2026-00171 · Trotadora X: Falla; Banco Y: No se puede usar", "Daniel Aguilar"))
        self.assertEqual(fotos[1][1][1:3], ("/f/b.jpg", "hallazgo_OT-2026-00300_2"))

    def test_la_foto_de_referencia_no_tiene_public_id_asi_borrar_el_adjunto_nunca_borra_la_evidencia(self):
        cur = self.Cur(self._filas_bd())
        self.amb["_hall_seguimiento_enlazar"](cur, 300, self._seg(("/f/a.jpg",)), "OT-2026-00300", "Daniel")
        s = [s for s, _p in cur.sql if s.startswith("INSERT INTO mant_visita_adjuntos")][0]
        self.assertIn("cloudinary_public_id", s)
        self.assertRegex(s, r"VALUES \(%s,'foto',NULL,%s,NULL,%s,NULL,NULL,%s,%s\)", "archivo_path NULL, public_id NULL, sin tamaño ni mime")

    def test_si_alguien_cambio_los_hallazgos_no_se_escribe_nada(self):
        casos = {"retirado": self._filas_bd(retirado_en="2026-10-21"), "descartado": self._filas_bd(estado="descartado"),
                 "con otra OT": self._filas_bd(ot_seguimiento_id=5), "ya resuelto": self._filas_bd(estado="resuelto")}
        for nombre, filas in casos.items():
            cur = self.Cur(filas)
            ok, msg, n = self.amb["_hall_seguimiento_enlazar"](cur, 300, self._seg(), "OT-2026-00300", "Daniel")
            self.assertEqual((ok, n), (False, 0), nombre)
            self.assertIn("Alguien más cambió esos hallazgos", msg)
            self.assertEqual(len(cur.sql), 1, nombre + ": solo se leyó, no se escribió nada")
        cur = self.Cur(self._filas_bd()[:1])
        self.assertFalse(self.amb["_hall_seguimiento_enlazar"](cur, 300, self._seg(), "OT-2026-00300", "Daniel")[0], "falta uno")

    def test_si_las_fotos_fallan_la_ot_igual_queda_ligada_y_solo_se_deshacen_las_fotos(self):
        cur = self.Cur(self._filas_bd(), falla_fotos=True)
        ok, msg, n = self.amb["_hall_seguimiento_enlazar"](cur, 300, self._seg(), "OT-2026-00300", "Daniel")
        self.assertEqual((ok, msg, n), (True, None, 0))
        sqls = [s for s, _p in cur.sql]
        self.assertIn("ROLLBACK TO SAVEPOINT hall_fotos", sqls)
        self.assertTrue(any(s.startswith("UPDATE mant_visitas SET ot_madre_id") for s in sqls))
        self.assertTrue(any("falla simulada" in s for s in self.m.salida))

    def test_sin_fotos_no_hay_savepoint(self):
        cur = self.Cur(self._filas_bd())
        self.assertEqual(self.amb["_hall_seguimiento_enlazar"](cur, 300, self._seg(()), "OT-2026-00300", "Daniel"), (True, None, 0))
        self.assertFalse([s for s, _p in cur.sql if "SAVEPOINT" in s])

    def test_quien_cabe_en_las_columnas(self):
        cur = self.Cur(self._filas_bd())
        self.amb["_hall_seguimiento_enlazar"](cur, 300, self._seg(), "OT-2026-00300", "N" * 400)
        self.assertEqual(len(cur.sql[1][1][1]), 190, "seguimiento_por y subido_por son VARCHAR(190)")


class TestPanelYFiltro(unittest.TestCase):
    """El SQL de «sin gestionar» se ejecuta de verdad (SQLite, SQL estándar) contra los casos que importan."""

    def setUp(self):
        import sqlite3
        self.con = sqlite3.connect(":memory:")
        self.con.row_factory = sqlite3.Row
        self.con.execute("CREATE TABLE mant_visitas (id INTEGER PRIMARY KEY, estado TEXT)")
        self.con.execute("CREATE TABLE mant_ot_hallazgos (id INTEGER PRIMARY KEY, visita_id INTEGER, estado TEXT, retirado_en TEXT, ot_seguimiento_id INTEGER)")
        for vid, est in [(i, "en_curso") for i in range(1, 10)] + [(100, "programada"), (101, "cancelada"), (102, "anulada")]:
            self.con.execute("INSERT INTO mant_visitas VALUES (?,?)", (vid, est))
        filas = [(1, "abierto", None, None), (1, "abierto", None, None),          # 2 sin gestionar
                 (2, "abierto", "2026-10-21", None),                              # retirado: no
                 (3, "resuelto", None, 100),                                      # con OT de seguimiento viva: no
                 (4, "resuelto", None, 101),                                      # su OT se canceló: SÍ
                 (5, "resuelto", None, 999),                                      # su OT ya no existe: SÍ
                 (6, "descartado", None, None),                                   # descartado: no
                 (7, "tomado", None, None),                                       # alguien lo tomó: no
                 (8, "tomado", None, 102)]                                        # lo tomó y su OT se anuló: SÍ
        for i, (vid, est, ret, seg) in enumerate(filas, start=1):
            self.con.execute("INSERT INTO mant_ot_hallazgos VALUES (?,?,?,?,?)", (i, vid, est, ret, seg))
        m = Mundo(usuario=GESTOR)
        self.m = m

        def fetchall(sql, params=()):
            return [dict(r) for r in self.con.execute(sql.replace("%s", "?"), tuple(params))]
        self.fetchall = fetchall
        self.extra = {"mysql_fetchall": fetchall, "print": lambda *a, **k: m.salida.append(" ".join(str(x) for x in a)), "time": time,
                      "_es_rol_tecnico": lambda user=None: False, "_oh": H}

    def _amb(self, ok=True, **extra):
        amb = cargar(["_ot2_panel_hallazgos", "_ot_hall_filtro_sql", "_hall_ok", "_ensure_ot_hallazgos"], ("_HALL_ESTADO",), dict(self.extra, **extra))
        amb["_HALL_ESTADO"]["ok"] = bool(ok)
        amb["_HALL_ESTADO"]["revisado_en"] = time.time()
        return amb

    def test_el_filtro_deja_solo_las_ot_con_algo_sin_gestionar(self):
        clave, sql, params = self._amb()["_ot_hall_filtro_sql"]("1")
        self.assertEqual((clave, params), ("1", []))
        vids = [r["id"] for r in self.con.execute("SELECT v.id FROM mant_visitas v WHERE " + sql + " ORDER BY v.id")]
        self.assertEqual(vids, [1, 4, 5, 8])

    def test_el_sql_y_la_regla_en_python_dicen_lo_mismo_en_todas_las_combinaciones(self):
        """«Sin gestionar» existe dos veces (SQL para el panel y el filtro; Python para la tarjeta y la franja del cierre): tienen que coincidir."""
        combos, vid = [], 1000
        vivas = {100: "OT-100", 101: "OT-101", 102: "OT-102"}
        estados_seg = {None: None, 100: "programada", 101: "cancelada", 102: "anulada", 999: None}
        for estado in ("abierto", "tomado", "resuelto", "descartado"):
            for retirado in (None, "2026-10-21"):
                for seg in (None, 100, 101, 102, 999):
                    vid += 1
                    self.con.execute("INSERT INTO mant_visitas VALUES (?,?)", (vid, "en_curso"))
                    self.con.execute("INSERT INTO mant_ot_hallazgos (visita_id, estado, retirado_en, ot_seguimiento_id) VALUES (?,?,?,?)", (vid, estado, retirado, seg))
                    combos.append((vid, estado, retirado, seg))
        _clave, sql, _p = self._amb()["_ot_hall_filtro_sql"]("1")
        en_sql = {r["id"] for r in self.con.execute("SELECT v.id FROM mant_visitas v WHERE v.id > 1000 AND " + sql)}
        en_python = set()
        for vid, estado, retirado, seg in combos:
            ef, _caido = H.estado_efectivo({"estado": estado, "ot_seguimiento_id": seg}, vivas.get(seg), estados_seg[seg])
            if not retirado and ef == "abierto":
                en_python.add(vid)
        self.assertEqual(len(combos), 40)
        self.assertEqual(en_sql, en_python)
        self.assertTrue(en_sql, "la comparación no es vacía")

    def test_el_filtro_solo_acepta_su_valor_exacto(self):
        f = self._amb()["_ot_hall_filtro_sql"]
        for raro in (None, "", "0", "2", "si", "1; DROP TABLE mant_visitas", " ", "-1", "1 OR 1=1"):
            self.assertEqual(f(raro), ("", "", []), repr(raro))
        self.assertEqual(f(" 1 ")[0], "1", "espacios alrededor no importan")

    def test_el_filtro_no_aplica_a_un_tecnico_ni_sin_tabla(self):
        self.assertEqual(self._amb(_es_rol_tecnico=lambda user=None: True)["_ot_hall_filtro_sql"]("1"), ("", "", []))
        self.assertEqual(self._amb(ok=False)["_ot_hall_filtro_sql"]("1"), ("", "", []))

    def test_el_conteo_de_cada_fila_del_panel_en_una_sola_consulta(self):
        consultas = []

        def fetchall(sql, params=()):
            consultas.append(sql)
            return self.fetchall(sql, params)
        filas = [{"id": i} for i in range(1, 10)]
        self._amb(mysql_fetchall=fetchall)["_ot2_panel_hallazgos"](filas)
        self.assertEqual({f["id"]: f["hall_abiertos"] for f in filas}, {1: 2, 2: 0, 3: 0, 4: 1, 5: 1, 6: 0, 7: 0, 8: 1, 9: 0})
        self.assertEqual(len(consultas), 1, "una consulta por lote, nunca una por fila")

    def test_el_panel_no_se_rompe_sin_tabla_con_tecnico_o_con_error(self):
        for kw in (dict(ok=False), dict(_es_rol_tecnico=lambda user=None: True)):
            consultas = []
            filas = [{"id": 1}, {"id": 4}]
            self._amb(mysql_fetchall=lambda sql, params=(), c=consultas: c.append(sql) or [], **kw)["_ot2_panel_hallazgos"](filas)
            self.assertEqual([f["hall_abiertos"] for f in filas], [0, 0], kw)
            self.assertEqual(consultas, [], "ni siquiera se consulta")

        def roto(sql, params=()):
            raise RuntimeError("falla simulada")
        filas = [{"id": 1}]
        self._amb(mysql_fetchall=roto)["_ot2_panel_hallazgos"](filas)
        self.assertEqual(filas[0]["hall_abiertos"], 0)
        self.assertTrue(any("hallazgos" in s for s in self.m.salida))
        vacias = []
        self._amb()["_ot2_panel_hallazgos"](vacias)
        self.assertEqual(vacias, [])


class TestArranqueDeLaTabla(unittest.TestCase):
    def _amb(self, falla=False):
        llamadas, salida = [], []

        def ejecutar(sql, params=None):
            llamadas.append(" ".join(sql.split()))
            if falla:
                raise RuntimeError("sin permiso")
        amb = cargar(["_ensure_ot_hallazgos", "_hall_ok"], ("_HALL_ESTADO",),
                     {"mysql_execute": ejecutar, "print": lambda *a, **k: salida.append(" ".join(str(x) for x in a)), "time": time})
        return amb, llamadas, salida

    def test_una_sola_sentencia_create_table_if_not_exists(self):
        amb, llamadas, _s = self._amb()
        self.assertTrue(amb["_ensure_ot_hallazgos"]())
        self.assertEqual(len(llamadas), 1)
        self.assertTrue(llamadas[0].startswith("CREATE TABLE IF NOT EXISTS mant_ot_hallazgos ("))
        self.assertTrue(amb["_HALL_ESTADO"]["ok"])

    def test_si_falla_queda_dormido_sin_romper_el_arranque(self):
        amb, llamadas, salida = self._amb(falla=True)
        self.assertFalse(amb["_ensure_ot_hallazgos"]())
        self.assertFalse(amb["_HALL_ESTADO"]["ok"])
        self.assertTrue(any("ensure_ot_hallazgos" in s for s in salida))

    def test_hall_ok_recuerda_y_solo_reintenta_cada_5_minutos(self):
        amb, llamadas, _s = self._amb()
        amb["_HALL_ESTADO"].update(ok=True)
        self.assertTrue(amb["_hall_ok"]())
        self.assertEqual(llamadas, [], "con la tabla confirmada no se vuelve a preguntar")
        amb["_HALL_ESTADO"].update(ok=False, revisado_en=time.time())
        self.assertFalse(amb["_hall_ok"]())
        self.assertEqual(llamadas, [], "falló hace poco: no se reintenta en cada petición")
        amb["_HALL_ESTADO"].update(ok=False, revisado_en=time.time() - 301)
        self.assertTrue(amb["_hall_ok"]())
        self.assertEqual(len(llamadas), 1, "pasaron más de 5 minutos: se reintenta una vez")


# ═══════════════════════════ C) el cableado ═══════════════════════════
def _texto_del_decorador(nombre):
    """Todo lo que hay entre el último `@app.route(` y `def nombre(` (incluidas las rutas escritas en varias líneas)."""
    i = APP.index("\ndef %s(" % nombre)
    previo = APP[max(0, i - 900):i]
    j = previo.rfind("@app.route(")
    return previo[j:] if j >= 0 else ""


def _crear_table_sql():
    """El CREATE TABLE tal como lo ejecutaría el arranque (se corre la función con un doble de mysql_execute)."""
    visto = []
    amb = cargar(["_ensure_ot_hallazgos"], ("_HALL_ESTADO",), {"mysql_execute": lambda sql, p=None: visto.append(" ".join(sql.split())),
                                                              "print": lambda *a, **k: None, "time": time})
    amb["_ensure_ot_hallazgos"]()
    return visto[0]


class TestCableado(unittest.TestCase):
    def test_el_enum_de_la_campana_tiene_los_dos_tipos_al_final_de_las_dos_listas(self):
        enums = re.findall(r"ALTER TABLE mant_notificaciones MODIFY COLUMN tipo \"\s*\"\s*ENUM\((.*?)\) DEFAULT 'otro'", APP, re.S)
        self.assertEqual(len(enums), 2, "el ENUM se define en init_mantenciones_tables y en _ensure_mant_notif_tipo_ot_firmada_cliente")
        for e in enums:
            valores = re.findall(r"'([a-z_]+)'", e)
            i = valores.index(H.TIPO_AVISO)
            self.assertEqual(valores[i:i + 2], [H.TIPO_AVISO, H.TIPO_AVISO_TOMA], "juntos y en ese orden")
            # 2026-10-21: lo que se agrega después va DETRÁS (nada se reordena, REGLA #18): «repuesto_nuevo» (aviso de solicitud de repuesto).
            # 2026-10-21 (REGLA #41): detrás de «repuesto_nuevo» vino «repuesto_tecnico» (la campana al técnico que pidió un repuesto).
            self.assertEqual(valores[i + 2:], ["repuesto_nuevo", "repuesto_tecnico"], "al FINAL: nada se reordena (REGLA #18)")
            self.assertEqual(valores.count(H.TIPO_AVISO), 1)
            self.assertEqual(valores[valores.index("ot_responsable") + 1], H.TIPO_AVISO)
            self.assertIn("ot_pendiente_stock", valores)

    def test_los_tipos_que_se_envian_a_la_campana_son_estos_dos_y_ningun_otro(self):
        enviados = re.findall(r"_mant_notificar\(.*?(_oh\.TIPO_AVISO_TOMA|_oh\.TIPO_AVISO)\b", BLOQUE)
        self.assertEqual(sorted(set(enviados)), ["_oh.TIPO_AVISO", "_oh.TIPO_AVISO_TOMA"])
        self.assertFalse(re.findall(r"_mant_notificar\(\s*[^,]+,\s*[\"'][a-z_]+[\"']", BLOQUE), "ningún tipo escrito a mano en el bloque")

    def test_arranque_barato_regla_18(self):
        src = _fuente_funcion("_ensure_ot_hallazgos")
        self.assertEqual(src.count("mysql_execute("), 1, "una sentencia")
        self.assertIn("CREATE TABLE IF NOT EXISTS mant_ot_hallazgos", src)
        self.assertNotIn("lock_wait_timeout", src)
        self.assertNotIn("ALTER TABLE", BLOQUE, "ni una columna nueva en mant_visitas ni en ninguna otra tabla: el vínculo usa ot_madre_id / ot_relacion")
        self.assertNotRegex(_crear_table_sql(), r",\s*(ADD|MODIFY|CHANGE|DROP|ALTER|RENAME)\s", "la guardia de DDL no la entendería")
        self.assertNotIn("%", _crear_table_sql(), "mysql_execute le pasa () como parámetros: un % rompería la sentencia")
        # se llama SIEMPRE al arrancar (también con ILUS_SKIP_MIGRATIONS=1), dentro de un try para no tumbar el servidor
        self.assertRegex(APP, r"try:\n    with app\.app_context\(\):\n        _ensure_ot_hallazgos\(\)\nexcept Exception as _ensure_ot_hall_err:")
        self.assertEqual(APP.count("_ensure_ot_hallazgos()"), 3, "la definición, la llamada del arranque y la que reintenta _hall_ok cada 5 minutos")

    def test_el_create_table_es_coherente_con_los_limites_del_codigo(self):
        sql = _crear_table_sql()
        tipos = dict((m.group(1), m.group(2)) for m in re.finditer(r"\b([a-z_]+) (INT|TEXT|DATETIME|TINYINT\(1\)|VARCHAR\(\d+\))", sql))
        largo = lambda c: int(re.search(r"\((\d+)\)", tipos[c]).group(1))             # noqa: E731
        self.assertGreaterEqual(largo("titulo"), H.TITULO_MAX)
        self.assertGreaterEqual(largo("descarte_motivo"), H.DESCARTE_MAX)
        self.assertGreaterEqual(largo("severidad"), max(len(k) for k in H.SEVERIDADES))
        self.assertGreaterEqual(largo("estado"), max(len(k) for k in H.ESTADOS))
        self.assertGreaterEqual(largo("origen"), max(len(k) for k in H.ORIGENES))
        self.assertGreaterEqual(largo("referencia"), len(H.ref_equipo(2 ** 31, "rep", "ab12cd34")))
        self.assertGreaterEqual(largo("retirado_motivo"), 200)                          # _hall_retirar corta a 200
        self.assertGreaterEqual(largo("equipo_nombre"), 400)                           # _hall_upsert corta a 400
        for c in ("detectado_por", "tomado_por", "descartado_por", "seguimiento_por"):
            self.assertEqual(largo(c), 190, c)                                         # _hall_quien corta a 190
        self.assertEqual(tipos["detalle"], "TEXT")
        self.assertEqual(tipos["fotos_json"], "TEXT")
        self.assertIn("UNIQUE KEY uq_hall_origen (visita_id, origen, referencia)", sql, "es lo que hace idempotente el alta (INSERT IGNORE)")
        self.assertNotIn("FOREIGN KEY", sql, "sin llaves foráneas: no ata ni frena el borrado de una OT")
        self.assertIn("ENGINE=InnoDB DEFAULT CHARSET=utf8mb4", sql)

    def test_toda_columna_que_el_codigo_toca_existe_en_el_create_table(self):
        """REGLA #5: `ast.parse` no detecta nombres de columnas SQL; se revisan acá, contra el CREATE TABLE real."""
        columnas = set(re.findall(r"\b([a-z_]+) (?:INT|TEXT|DATETIME|TINYINT\(1\)|VARCHAR\(\d+\))", _crear_table_sql()))
        self.assertIn("aviso_grupo_en", columnas)
        usadas = set()
        for n in ast.walk(ast.parse(BLOQUE)):
            if not (isinstance(n, ast.Constant) and isinstance(n.value, str)):
                continue
            s = " ".join(n.value.split())
            if s.startswith(", ot_seguimiento_id=NULL"):
                usadas |= set(re.findall(r"(\w+)=NULL", s))
            if "mant_ot_hallazgos" not in s or s.startswith("CREATE TABLE"):
                continue
            m = re.search(r"INSERT IGNORE INTO mant_ot_hallazgos \(([^)]*)\)", s)
            if m:
                usadas |= {c.strip() for c in m.group(1).split(",")}
            m = re.search(r"UPDATE mant_ot_hallazgos SET (.*?)(?: WHERE |$)", s)
            if m:
                usadas |= set(re.findall(r"\b([a-z_]+)=", m.group(1)))
            m = re.search(r"SELECT (.*?) FROM mant_ot_hallazgos", s)
            if m and "h.*" not in m.group(1):
                # 2026-10-21: una columna de OTRA tabla unida con su alias (m.serie de mant_maquinas) no es de mant_ot_hallazgos.
                usadas |= {t for t in re.findall(r"\b[a-z_]+\b", re.sub(r"\b(?!h\.)[a-z]\.[a-z_]+", "", m.group(1))) if t not in ("as", "max", "count", "ult", "n", "x", "h", "s", "visita_id") or t == "visita_id"}
            m = re.search(r" WHERE (.*)$", s)
            if m:
                usadas |= set(re.findall(r"\b([a-z_]+)\s*(?:=|>=|IS NULL|IS NOT NULL| IN \()", m.group(1)))
            usadas |= set(re.findall(r"\bh\.([a-z_]+)", s)) | set(re.findall(r"\bhf\.([a-z_]+)", s))
        usadas -= {"seg_numero", "seg_estado"}
        self.assertTrue(usadas, "la revisión no leyó ninguna columna: el test está roto")
        self.assertEqual(usadas - columnas, set(), "columnas que el código usa y la tabla no tiene")

    def test_cada_ruta_de_la_tabla_existe_es_post_y_trae_los_argumentos_que_su_manejador_lee(self):
        rutas = cargar([], ("_HALL_RUTAS",))["_HALL_RUTAS"]
        self.assertEqual(len(rutas), 12)
        despachador = _fuente_funcion("_hall_despachar")
        necesita = {"tarea": "tid", "producto": "imid", "equipo_fs": "mid", "equipo_baja": "mid", "equipo_falla": "mid", "repuesto_equipo": "mid",
                    "repuesto_producto": "imid", "incidencia_equipo": "mid", "incidencia_producto": "imid"}
        for endpoint, clave in rutas.items():
            dec = _texto_del_decorador(endpoint)
            self.assertIn("@app.route(", dec, endpoint)
            self.assertRegex(dec, r"methods=\[\"POST\"\]", endpoint)
            self.assertIn("<int:vid>", dec, endpoint)
            self.assertIn("<int:%s>" % necesita[clave], dec, "%s: el manejador «%s» lee %s de la URL" % (endpoint, clave, necesita[clave]))
            self.assertIn('clave == "%s"' % clave, despachador, clave)
        self.assertEqual(set(rutas.values()), set(necesita), "toda clave del despachador tiene al menos una ruta")

    def test_las_rutas_de_gestion_tienen_los_tres_candados_y_ningun_nombre_financiero(self):
        esperadas = {"ot_api_hallazgo_tomar": "POST", "ot_api_hallazgo_descartar": "POST", "ot_api_hallazgo_reabrir": "POST",
                     "ot_api_hallazgos_prellenado": "GET"}
        financiero = re.compile(r"finanz|costo|factura|autoriz|regulariz|dashboard|facprov|margen|valoriz", re.I)
        for nombre, metodo in esperadas.items():
            self.assertEqual(decoradores(nombre)[1:], ["@_mant_required", "@_no_tecnico", "@_ot_can_view"], nombre)
            dec = _texto_del_decorador(nombre)
            self.assertIn('methods=["%s"]' % metodo, dec, nombre)
            self.assertIn("/ot/api/<int:vid>/hallazgos/", dec, nombre)
            self.assertIsNone(financiero.search(nombre), "el escáner de REGLA #26 exigiría gestión financiera a «%s»" % nombre)
            self.assertIsNone(financiero.search(dec), nombre)
        # `configurar` se niega a todos (incluido el superadmin) en una OT cerrada: los hallazgos se gestionan también ahí
        self.assertNotIn("_ot_can_configurar", "".join(decoradores(n)[1] for n in esperadas))

    def test_nada_va_al_cliente_ni_por_correo_ni_por_whatsapp(self):
        for prohibido in ("_send_ilus_email", "_ilus_email_master", "whatsapp", "notify_async", "_mant_ot_email_cliente", "smtplib", "requests.post",
                          "resend", "twilio", "send_message", "_ot_com_"):
            self.assertNotIn(prohibido, BLOQUE.lower() if prohibido.islower() else BLOQUE, prohibido)
        hook = _fuente_funcion("_hall_tras_escritura")
        self.assertIn("except Exception as e:", hook)
        self.assertTrue(hook.rstrip().endswith("return resp"), "pase lo que pase, la respuesta del técnico sale intacta")

    def test_el_nucleo_de_crear_ot_liga_dentro_de_la_transaccion_y_sin_tipos_nuevos(self):
        core = _fuente_funcion("_ot2_crear_core")
        self.assertIn('"autorizacion_id", "seguimiento"):', core, "una OT hija de desinstalación nunca hereda un seguimiento del body")
        self.assertLess(core.index("7.d"), core.index("8. ESCRITURA"))
        self.assertIn("_hall_seguimiento_validar(d[\"seguimiento\"], cliente_id)", core)
        self.assertLess(core.index("_ot_fin_clasif_guardar("), core.index("_hall_seguimiento_enlazar("))
        self.assertLess(core.index("_hall_seguimiento_enlazar("), core.index("conn.commit()", core.index("_hall_seguimiento_enlazar(")))
        self.assertIn("conn.rollback()", core[core.index("_hall_seguimiento_enlazar("):core.index("SEG_HALLAZGO_NO_DISPONIBLE") + 80])
        self.assertIn('"SEG_HALLAZGO_NO_DISPONIBLE", http=409', core)
        self.assertIn("if autorizacion:", core[core.index("7.d"):core.index("8. ESCRITURA")], "aprobada a distancia días después: se crea igual y solo se avisa")
        for accion in ("creada_como_seguimiento", "hallazgo_seguimiento"):
            self.assertIn('"%s"' % accion, core)
        usados = set(re.findall(r'_mant_notificar\(\s*[^,()]+,\s*"([a-z_]+)"', core))
        self.assertLessEqual(usados, {"otro", "ot_asignada", "ot_pendiente_aprobacion", "ot_aprobada", "ot_rechazada"},
                             "tests/test_ot_eventos_desinstalacion.py vigila que _ot2_crear_core solo use tipos ya existentes")
        # no consume saldo ni hereda cobros: el vínculo no toca nada de finanzas
        vinculo = "\n".join(l for l in core[core.index("7.d"):core.index("8. ESCRITURA")].splitlines() if not l.strip().startswith("#"))
        for nada in ("finanzas", "saldo", "docs_tomas", "zz_", "cobro", "_ot_saldo"):
            self.assertNotIn(nada, vinculo, nada)

    def test_la_ficha_arma_los_hallazgos_antes_de_la_barra_de_acciones(self):
        """tests/test_ot_producto_acciones.py ejecuta el bloque entre `acc_puede` y `return render_template`: lo nuevo va ANTES."""
        det = _fuente_funcion("ot2_detalle")
        self.assertIn("hallazgos = _hall_contexto_ficha(vid, v)", det)
        self.assertLess(det.index("hallazgos = _hall_contexto_ficha(vid, v)"), det.index("    acc_puede = bool("))
        self.assertIn("hallazgos=hallazgos,", det)

    def test_borrar_una_ot_limpia_sus_hallazgos_y_suelta_los_que_la_tenian_de_seguimiento(self):
        src = _fuente_funcion("_mant_visita_eliminar_core")
        self.assertIn('DELETE FROM mant_ot_hallazgos WHERE visita_id=%s', src)
        self.assertRegex(src, r"UPDATE mant_ot_hallazgos SET estado='abierto', ot_seguimiento_id=NULL, seguimiento_por=NULL, seguimiento_en=NULL\s*\"\s*\"\s*WHERE ot_seguimiento_id=%s")
        self.assertLess(src.index("DELETE FROM mant_ot_hallazgos"), src.index('cur.execute("DELETE FROM mant_visitas WHERE id=%s"'))

    def test_el_panel_el_excel_y_las_cuatro_vistas_usan_el_mismo_filtro(self):
        panel = _fuente_funcion("ot2_panel")
        self.assertIn('_ot_hall_filtro_sql(request.args.get("hall"))', panel)
        self.assertIn("extra_where_q.append(_f_hall_sql)", panel)
        self.assertEqual(panel.count("_ot2_panel_hallazgos("), 2, "tabla y kanban")
        self.assertIn("f_hall=f_hall", panel)
        self.assertIn('_ot_hall_filtro_sql(args.get("hall"))', _fuente_funcion("_ot2_filtros_export"))
        self.assertIn('desc.append("con hallazgos sin gestionar")', _fuente_funcion("_ot2_filtros_export"))
        chips = _fuente_funcion("_ot2_panel_evento_chips")
        self.assertIn("_oh.REL_SEGUIMIENTO", chips)

    def test_la_bitacora_del_tecnico_no_trae_ningun_evento_de_hallazgos(self):
        acciones = ("hallazgo_detectado", "hallazgo_reabierto", "hallazgo_retirado", "hallazgo_tomado", "hallazgo_descartado", "hallazgo_seguimiento",
                    "creada_como_seguimiento")
        permitidas = fuente_constante("_ACT_TEC_PERMITIDAS")
        amb = cargar(["_ot_actividad_para_tecnico"], ("_ACT_TEC_PERMITIDAS", "_ACT_TEC_SIN_DETALLE", "_ACT_TEC_PREFIJOS_VEDADOS", "_ACT_TEC_RE_MONTO"),
                     {"re": re})
        for a in acciones:
            self.assertNotIn('"%s"' % a, permitidas, a)
            self.assertEqual(amb["_ot_actividad_para_tecnico"](a, "Trotadora X: Falla · Pedro"), (False, ""), "a un técnico no le llega: %s" % a)
        self.assertEqual(amb["_ot_actividad_para_tecnico"]("firmada_tecnico", "x")[0], True, "lo operativo sigue visible")
        # …y la gestión las ve con su etiqueta legible
        detalle = _fuente_funcion("ot2_detalle")
        for a in acciones:
            self.assertIn('"%s"' % a, detalle, a)

    def test_las_guardias_de_ot_eventos_no_tratan_un_seguimiento_como_una_desinstalacion(self):
        seg = {"ot_madre_id": 1, "ot_relacion": "seguimiento"}          # 1 = la instalación de más abajo
        des = {"ot_madre_id": 1, "ot_relacion": "desinstalacion"}
        self.assertIsNone(EV.guardia_cambio_tipo(seg, []))
        self.assertEqual(EV.guardia_cambio_tipo(des, [])["codigo"], "TIPO_PROTEGIDO")
        # 2026-10-21: Eventos se eliminó (guardia_evento_de_hija y avance_evento ya no existen).
        self.assertFalse(hasattr(EV, "guardia_evento_de_hija"))
        self.assertFalse(hasattr(EV, "avance_evento"))

    def test_el_asistente_manda_el_seguimiento_y_el_servidor_lo_revalida(self):
        modal = _leer("templates/ot2/_modal_crear.html")
        for pieza in ("function abrirDesdeHallazgos(vid, ids)", "/hallazgos/prellenado?ids=", "S.seguimiento = {origen_id:o.id", "seguimiento:null,",
                      "body.seguimiento = {origen_id: _sg.origen_id, hallazgo_ids: _sg.ids", "if (_o2mSegVigente()){", "_o2mSegHtml()+",
                      "abrirDesdeHallazgos:abrirDesdeHallazgos, segQuitar:segQuitar, segFoto:segFoto",
                      "_qs.get('seguimiento')", "_qs.get('desde')", "Crear sin ligarla", "Cambiaste de cliente"):
            self.assertIn(pieza, modal, pieza)
        self.assertIsNone(re.search(r"(?<![\w.])(alert|confirm|prompt)\(", modal[modal.index("function abrirDesdeHallazgos"):modal.index("function abrirWizard")]),
                          "REGLA #1: nada de popups nativos")
        # la clave del seguimiento no entra a la lista de campos que el asistente toma del documento
        self.assertIn("window.ilusAlert", modal[modal.index("function abrirDesdeHallazgos"):modal.index("function abrirWizard")])

    def test_el_tipo_se_lee_de_aqui_y_no_se_inventa_una_columna_en_mant_visitas(self):
        self.assertIn("ot_relacion", BLOQUE)
        self.assertNotIn("seguimiento_de", BLOQUE)
        self.assertEqual(len(re.findall(r"UPDATE mant_visitas", BLOQUE)), 1, "el único UPDATE a mant_visitas es el vínculo (ot_madre_id + ot_relacion)")
        self.assertIn("UPDATE mant_visitas SET ot_madre_id=%s, ot_relacion=%s WHERE id=%s", BLOQUE)


# ═══════════════════ D) plantillas renderizadas de verdad y JS ejecutado ═══════════════════
def _entorno(extra=None):
    def cargador(nombre):
        if nombre in ("ot2/panel.html", "ot2/_hallazgos.html"):
            return _leer("templates/" + nombre)
        if nombre == "base.html":
            return "{% block title %}{% endblock %}<main>{% block content %}{% endblock %}</main>{% block scripts %}{% endblock %}"
        return ""        # los modales que incluye el panel no importan acá

    env = jinja2.Environment(loader=jinja2.FunctionLoader(cargador), extensions=["jinja2.ext.do"], undefined=jinja2.ChainableUndefined,
                             autoescape=True)      # como Flask con plantillas .html
    env.globals["url_for"] = lambda ep, **kw: ("/static/" + kw.get("filename", "")) if ep == "static" else \
        "/%s?%s" % (ep, "&".join("%s=%s" % (k, v) for k, v in sorted(kw.items()) if v is not None))
    env.filters["chile_fmt"] = lambda v, f="": str(v)
    env.filters["rut_fmt"] = lambda v: str(v)
    return env


def _tarjeta(hallazgos, es_tecnico=False):
    # 2026-10-21: la tarjeta completa se dibuja en la página «Hallazgos» (hz_pagina); en la ficha de la OT solo el aviso «viene de».
    return _entorno().get_template("ot2/_hallazgos.html").render(hallazgos=hallazgos, es_tecnico=es_tecnico, hz_pagina=True)


def _texto_visible(html):
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html)).strip()


class TestTarjetaRenderizada(unittest.TestCase):
    CSS = _leer("static/ot_hallazgos.css")

    def _ctx(self, **visita):
        e = escena_aurum(**visita)
        self.e = e
        return e.amb["_hall_contexto_ficha"](171, e.m.visitas[171])

    def test_la_lista_con_su_semaforo_sus_contadores_y_la_evidencia(self):
        html = _tarjeta(self._ctx())
        e = self.e
        self.assertIn('<section class="hz" id="otdHallazgos" data-vid="171"', html)
        orden = [html.index('id="hz%d"' % h["id"]) for h in (e.h1, e.h2, e.h6, e.h3, e.h4, e.h5)]
        self.assertEqual(orden, sorted(orden), "por gravedad: falla, fuera de servicio, baja, alerta, repuesto, incidencia")
        chips = [c for c in re.findall(r'<span class="hz-chip (rojo|negro|ambar|azul|gris)"><i class="bi (bi-[a-z0-9-]+)" aria-hidden="true"></i> ([^<]+)</span>', html)
                 if c[2] != "Puede lastimar a alguien"]
        self.assertEqual([c[2] for c in chips], ["Falla", "Fuera de servicio", "Dar de baja", "Alerta", "Repuesto", "Incidencia"])
        self.assertEqual([c[0] for c in chips], ["rojo", "rojo", "negro", "ambar", "azul", "gris"])
        self.assertEqual(html.count("Puede lastimar a alguien"), 1, "solo el equipo fuera de servicio con riesgo")
        txt = _texto_visible(html)
        for pieza in ("3 sin gestionar", "1 con el caso tomado", "1 con OT de seguimiento en curso", "1 descartado", "4 sin foto"):
            self.assertIn(pieza, txt, pieza)
        # H-12: la OT de seguimiento de este repuesto está «Programada» (abierta): se ve AZUL («en curso»), no verde («hecho»).
        self.assertIn('<span class="hz-n azul" title="Su OT de seguimiento sigue abierta"><b>1</b> con OT de seguimiento en curso</span>', html)
        self.assertNotIn("con OT de seguimiento cerrada", txt)
        self.assertEqual(html.count("Sin foto. La foto es la evidencia"), 4, "sin foto se dice, en ámbar, hallazgo por hallazgo")
        self.assertIn('<img src="/f/live/t12.jpg" alt="Foto del hallazgo 1"', html, "la foto que el técnico subió después de responder")
        fotos = re.search(r"data-hz-fotos='([^']*)'", html.split('id="hz%d"' % e.h2["id"])[1]).group(1)
        self.assertEqual(json.loads(fotos), ["/f/fs1.jpg", "/f/fs2.jpg"])
        # H-16: el equipo va UNA vez, completo, en el encabezado de su grupo; el hallazgo dice solo lo que pasó (el título completo sigue en data-hz-cap).
        self.assertIn("<h4 class=\"hz-tit\">Revisar cinta: Falla</h4>", html)
        self.assertIn('data-hz-cap="Trotadora Pro 3000 · Revisar cinta: Falla"', html)

    def test_cada_estado_muestra_quien_cuando_y_por_que(self):
        txt = _texto_visible(_tarjeta(self._ctx()))
        self.assertIn("Reportó Pedro Técnico · 21/10 11:05", txt)
        self.assertIn("Lo tomó Aarón Rojas el 21/10 11:05", txt)
        self.assertIn("Lo descartó Víctor Soto el 21/10 11:05. Motivo: El cliente ya lo resolvió", txt)
        self.assertIn("OT de seguimiento: OT-2026-00300 · Programada", txt)
        self.assertIn("La OT de seguimiento OT-2026-00301 fue cancelada: el hallazgo volvió a quedar sin gestionar.", txt)
        self.assertIn("Retirados por el técnico (1)", txt)
        self.assertIn("Polea · Revisar giro: Alerta · retirado 21/10 11:05: El técnico cambió su respuesta", txt)

    def test_los_botones_son_los_que_cada_hallazgo_admite(self):
        html = _tarjeta(self._ctx())
        e = self.e
        ids = lambda attr: sorted(int(x) for x in re.findall(r'data-hz-%s="(\d+)"' % attr, html))             # noqa: E731
        self.assertEqual(ids("tomar"), sorted([e.h1["id"], e.h2["id"], e.h6["id"]]))
        self.assertEqual(ids("levantar"), sorted([e.h1["id"], e.h2["id"], e.h3["id"], e.h6["id"]]))
        self.assertEqual(ids("descartar"), sorted([e.h1["id"], e.h2["id"], e.h3["id"], e.h6["id"]]))
        self.assertEqual(ids("reabrir"), [e.h5["id"]])
        for texto in ("Tomar el caso", "Levantar OT de seguimiento", "Descartar", "Reabrir"):
            self.assertIn(texto, html)

    def test_marcar_y_desmarcar_todo_regla_14(self):
        html = _tarjeta(self._ctx())
        self.assertIn("data-hz-todo", html)
        self.assertIn("Seleccionar todo", html)
        self.assertIn("data-hz-levantar-sel", html)
        self.assertEqual(len(re.findall(r'type="checkbox" data-hz-sel', html)), 6)
        self.assertEqual(len(re.findall(r'type="checkbox" data-hz-sel value="\d+" disabled', html)), 2, "los que ya tienen OT o están descartados no se eligen")
        self.assertIn("0 de 4 elegidos", html)

    def test_con_un_solo_hallazgo_elegible_no_hay_barra_ni_casillas(self):
        e = escena_aurum()
        for h in (e.h2, e.h3, e.h6):
            h["estado"], h["retirado_en"] = "descartado", None
        ctx = e.amb["_hall_contexto_ficha"](171, e.m.visitas[171])
        self.assertEqual(ctx["n_seleccionables"], 1)
        html = _tarjeta(ctx)
        self.assertNotIn("data-hz-todo", html)
        self.assertNotIn("data-hz-sel", html)
        self.assertIn("data-hz-levantar=", html)

    def test_una_ot_cerrada_sigue_mostrando_y_dejando_gestionar(self):
        abierta = _tarjeta(self._ctx(estado="en_curso"))
        self.assertNotIn("Esta OT ya está cerrada y sigue siendo evidencia", abierta)
        for estado in ("cerrada", "completada", "pendiente_aprobacion"):
            html = _tarjeta(self._ctx(estado=estado))
            self.assertIn("Esta OT ya está cerrada y sigue siendo evidencia: no se modifica", _texto_visible(html), estado)
            self.assertIn("data-hz-tomar=", html, estado)
            self.assertIn("data-hz-levantar=", html, estado)
        self.assertIn("Esta OT ya está cerrada", _texto_visible(_tarjeta(self._ctx(firma_cliente_url="/f/firma.png"))))

    def test_un_tecnico_no_ve_nada_aunque_el_dato_llegara(self):
        ctx = self._ctx()
        self.assertEqual(_tarjeta(ctx, es_tecnico=True).strip(), "", "defensa en profundidad: la plantilla tampoco lo dibuja")
        self.assertEqual(_tarjeta(None).strip(), "")
        e = escena_aurum(tecnico=True)
        self.assertIsNone(e.amb["_hall_contexto_ficha"](171, e.m.visitas[171]), "y de todos modos el servidor no lo arma")

    def test_el_nombre_largo_se_ve_completo_y_lo_que_escribe_la_gente_va_escapado(self):
        e = escena_aurum()
        largo = "Trotadora comercial de alto rendimiento con pantalla táctil de 22 pulgadas y banda ortopédica reforzada, número de serie 12345 " * 2
        e.h1["titulo"] = largo.strip()
        e.h2["titulo"] = "<script>alert('x')</script> & <img src=x onerror=alert(1)>"
        e.h2["detalle"] = "\"><b>negrita</b>"
        html = _tarjeta(e.amb["_hall_contexto_ficha"](171, e.m.visitas[171]))
        self.assertIn(">" + largo.strip() + "</h4>", html, "REGLA #15: nada de nombres cortados")
        self.assertNotIn("<script>alert", html)
        self.assertNotIn("<img src=x", html)
        self.assertNotIn("<b>negrita</b>", html)
        self.assertIn("&lt;script&gt;alert(&#39;x&#39;)&lt;/script&gt; &amp; &lt;img src=x onerror=alert(1)&gt;", html)
        self.assertNotRegex(self.CSS, r"line-clamp|text-overflow")

    def test_los_datos_del_script_van_como_json_y_el_titulo_con_comillas_no_lo_rompe(self):
        e = escena_aurum()
        e.m.visitas[171]["numero_ot"] = 'OT-2026-00171"</script><script>alert(1)'
        html = _tarjeta(e.amb["_hall_contexto_ficha"](171, e.m.visitas[171]))
        cfg = re.search(r"window\.OTD_HZ_CFG = \{ vid: 171, numero: (.*?) \};</script>", html).group(1)
        self.assertEqual(json.loads(cfg.replace("\\u003c", "<").replace("\\u003e", ">").replace("\\u0027", "'")), e.m.visitas[171]["numero_ot"])
        self.assertNotIn("</script><script>alert(1)", html)
        self.assertIn('<script defer src="/static/ot_hallazgos.js"></script>', html)

    def test_una_ot_de_seguimiento_dice_de_cual_viene(self):
        e = escena_aurum()
        html = _tarjeta(e.amb["_hall_contexto_ficha"](300, e.m.visitas[300]))
        self.assertIn('id="hzOrigen"', html)
        self.assertIn('Esta orden es el seguimiento de <a href="/ot/171">OT-2026-00171</a>', html)
        txt = _texto_visible(html)
        self.assertIn("Nació de 1 hallazgo que reportó el técnico en esa orden", txt)
        self.assertIn("Repuesto Trotadora Pro 3000: Repuesto solicitado: Piola × 2", txt)
        self.assertIn("no hereda cobros ni saldo", txt)
        self.assertNotIn('<section class="hz"', html, "esta OT no tiene hallazgos propios: solo el aviso de dónde viene")

    def test_sin_plata_ni_proveedores_en_la_tarjeta(self):
        e = escena_aurum()
        e.h4["titulo"] = "Trotadora Pro 3000: Repuesto solicitado: Piola × 2"
        html = _tarjeta(e.amb["_hall_contexto_ficha"](171, e.m.visitas[171]))
        ini = html.index('<ul class="hz-lista">')
        lista = html[ini:html.index("</section>", ini)]          # todos los grupos (H-20: cada uno con su <ul> propio) y los retirados
        self.assertNotRegex(lista, r"\$\s?\d|(?i:proveedor|costo|factura|saldo|margen|\bOC\b)")

    def test_estilos_boton_de_44_px_mobile_y_paleta_ilus(self):
        css = self.CSS
        self.assertRegex(css, r"\.hz-btn\s*\{[^}]*min-height:\s*44px")
        self.assertIn("@media", css)
        self.assertIn("#dc2626", css)                      # rojo ILUS
        self.assertIn("prefers-reduced-motion", css)


class TestPlantillasQueLaUsan(unittest.TestCase):
    DETALLE = _leer("templates/ot2/detalle.html")

    def test_la_tarjeta_va_al_nivel_raiz_del_contenido_nunca_dentro_de_puede_metadata_ni_de_una_pestana(self):
        arbol = jinja2.Environment(extensions=["jinja2.ext.do"]).parse(self.DETALLE)
        from jinja2 import nodes

        def buscar(nodo, pila):
            if isinstance(nodo, nodes.Include) and isinstance(nodo.template, nodes.Const) and nodo.template.value == "ot2/_hallazgos.html":
                return [list(pila)]
            hallados = []
            for hijo in nodo.iter_child_nodes():
                hallados += buscar(hijo, pila + [nodo])
            return hallados
        encontrados = buscar(arbol, [])
        self.assertEqual(len(encontrados), 1)
        ancestros = encontrados[0]
        self.assertFalse([a for a in ancestros if isinstance(a, nodes.If)], "no puede depender de ningún {% if %} (así se ve también en una OT CERRADA)")
        self.assertEqual([a.name for a in ancestros if isinstance(a, nodes.Block)], ["content"])
        self.assertEqual([type(a).__name__ for a in ancestros], ["Template", "Block"], "hijo directo del bloque de contenido: ni pestaña ni condición")

    def test_carga_su_css_y_su_js_una_sola_vez(self):
        self.assertEqual(self.DETALLE.count("ot_hallazgos.css"), 2)          # el comentario y el <link>
        self.assertEqual(self.DETALLE.count("<link rel=\"stylesheet\" href=\"{{ url_for('static', filename='ot_hallazgos.css') }}\">"), 1)
        self.assertEqual(self.DETALLE.count("{% include 'ot2/_hallazgos.html' %}"), 1)
        self.assertEqual(_leer("templates/ot2/_hallazgos.html").count("filename='ot_hallazgos.js'"), 1)

    def _franja(self):
        return re.search(r"(\{% if hallazgos and hallazgos\.n_abiertos %\}.*?\{% endif %\})", self.DETALLE, re.S).group(1)

    def test_la_franja_del_cierre_solo_avisa_y_no_traba(self):
        env = jinja2.Environment(extensions=["jinja2.ext.do"], autoescape=True, undefined=jinja2.ChainableUndefined)
        franja = env.from_string(self._franja())
        uno = _texto_visible(franja.render(hallazgos={"n_abiertos": 1}))
        self.assertIn("Hay 1 hallazgo sin gestionar en esta OT. Puedes cerrarla igual: no se bloquea nada. Ver los hallazgos", uno)
        tres = _texto_visible(franja.render(hallazgos={"n_abiertos": 3}))
        self.assertIn("Hay 3 hallazgos sin gestionar", tres)
        for sin in ({"n_abiertos": 0}, None):
            self.assertEqual(franja.render(hallazgos=sin).strip(), "", sin)
        crudo = franja.render(hallazgos={"n_abiertos": 2})
        self.assertIn('id="otfHallazgosAviso"', crudo)
        self.assertIn('href="/ot/hallazgos?ot=', crudo)                 # 2026-10-21: abre la página «Hallazgos» de esta OT
        self.assertIn('target="_blank"', crudo)                          # en otra pestaña: el modal del cierre no se pierde
        self.assertNotIn("disabled", crudo)
        # vive DENTRO del modal de aprobar el cierre, justo bajo el aviso de la comunicación de la OT
        modal = self.DETALLE[self.DETALLE.index('id="otfModalAprobacion"'):]
        self.assertIn('id="otfHallazgosAviso"', modal[:modal.index("otfModalAprobacion", 100) if "otfModalAprobacion" in modal[100:] else 30000])
        self.assertLess(self.DETALLE.index('id="oeAvisoCierre"'), self.DETALLE.index('id="otfHallazgosAviso"'))

    def test_el_aprobar_el_cierre_no_pregunta_por_hallazgos_para_dejar_cerrar(self):
        """«Solo avisar»: ninguna ruta de cierre menciona los hallazgos (ni para frenar ni para exigirlos)."""
        for nombre in ("mant_ot_aprobar_cierre", "mant_lev_cerrar", "mant_visita_firmar_tecnico"):
            try:
                src = _fuente_funcion(nombre)
            except AssertionError:
                continue
            self.assertNotIn("hallazgo", src.lower(), nombre)
            self.assertNotIn("_hall_", src, nombre)

    def test_el_icono_de_la_campana_tiene_nombre_legible(self):
        notif = _leer("templates/mantenciones/notificaciones.html")
        self.assertIn("ot_hallazgo:       { icon: 'clipboard2-pulse'", notif)
        self.assertIn("ot_hallazgo_toma:  { icon: 'hand-index-thumb'", notif)


def _fila(i, **kw):
    f = {"id": i, "numero_ot": "OT-2026-%05d" % i, "numero_sufijo": "%05d" % i, "numero_prefijo": "2026", "razon_social": "Gimnasio X",
         "titulo": "Instalación", "estado": "programada", "estado_fase": "pend", "estado_label": "Programada", "estado_icon": "bi-calendar",
         "tipo_icon": "bi-tools", "tipo_label": "Instalación", "tecnico_nombre": "Lenin Urbina", "tecnico_ini": "LU", "tecnico_tipo": "interno",
         "n_tareas": 3, "n_completas": 1, "check_pct": 33, "fecha_dia": "21/10", "fecha_dow": "Mié", "tabla_semaforo": "azul",
         "tabla_semaforo_txt": "Programada", "resp_id": None, "resp_nombre": None, "resp_ini": None}
    f.update(kw)
    return f


def _panel(**kw):
    ctx = dict(filas=[], origen="reales", vista="tabla", kpis={"pend": 2, "ejec": 0, "comp": 0, "cerr": 0}, conteo_reales=2, conteo_automaticas=0,
               total_filtrado=2, q="", fase="", page=1, per_page=25, total_paginas=1, per_page_opciones=[25, 50], error=None, kanban_cols=[],
               orden="ot", dir="desc", f_tipo="", f_tec=None, f_interna="", f_anexo="", f_pago="", f_agenda="", agenda_opts=[("hoy", "Hoy")],
               tipos_opts=[], tecnicos_opts=[], es_superadmin_ot=False, permissions={"superadmin": False},
               f_resp="", resp_opts=[], resp_yo=False)
    ctx.update(kw)
    return _entorno().get_template("ot2/panel.html").render(**ctx)


def _tabla(html):
    i = html.index('<table class="ot2-tabla')
    return html[i:html.index("</table>", i)]


class TestPanelRenderizado(unittest.TestCase):
    def test_la_fila_con_hallazgos_abiertos_lo_dice_y_lleva_a_la_tarjeta(self):
        html = _panel(filas=[_fila(1, hall_abiertos=3), _fila(2, hall_abiertos=0), _fila(3)], hall_activo=True)
        t = _tabla(html)
        self.assertEqual(t.count("Hallazgos abiertos:"), 1)
        self.assertIn('<a class="o2t-chip ambar" href="/ot_hallazgos_pagina?ot=1#hzpTarjeta"', t)
        self.assertIn("Hallazgos abiertos: 3</a>", t)

    def test_el_filtro_ofrece_sus_dos_opciones_y_se_ve_cuando_esta_activo(self):
        html = _panel(filas=[_fila(1)], hall_activo=True)
        sel = re.search(r'<select name="hall".*?</select>', html, re.S).group(0)
        self.assertEqual(re.findall(r'<option value="([^"]*)"[^>]*>(.*?)</option>', sel, re.S),
                         [("", "Hallazgos: todas las OT"), ("1", "Con hallazgos sin gestionar")])
        self.assertIn('onchange="this.form.submit()"', sel)
        self.assertIn("min-height:44px", sel)
        activo = _panel(filas=[_fila(1, hall_abiertos=1)], hall_activo=True, f_hall="1")
        self.assertIn('<option value="1" selected>Con hallazgos sin gestionar</option>', activo)
        self.assertIn("<b>con hallazgos sin gestionar</b>", activo)
        self.assertIn("&hall=1", activo, "el Reporte Excel lleva el filtro")
        self.assertIn("Limpiar", activo)

    def test_el_parametro_viaja_en_los_enlaces_de_paginado_orden_kpi_y_vistas(self):
        html = _panel(filas=[_fila(1)], hall_activo=True, f_hall="1", page=2, total_paginas=3)
        for ancla in ('class="ot2-kpi k-pend', 'class="ot2-kpi k-ejec', 'class="ot2-kpi k-comp', 'class="ot2-kpi k-cerr', 'class="ot2-v on"',
                      'class="ot2-pp"', 'class="ot2-nb ', '<a class="ot2-th'):
            i = html.index(ancla)
            self.assertIn("hall=1", html[i:i + 900], ancla)
        self.assertGreaterEqual(len(re.findall(r"hall=1", html)), 14)
        self.assertIn('<span class="n">1</span>', html, "el botón «Filtros» del celular cuenta este filtro")

    def test_dormido_el_panel_queda_como_antes(self):
        html = _panel(filas=[_fila(1), _fila(2)])
        self.assertNotIn('<select name="hall"', html)
        self.assertNotIn("Hallazgos abiertos", html)
        self.assertNotIn("hall=1", html)
        self.assertNotIn("con hallazgos sin gestionar", html)

    def test_la_tarjeta_del_kanban_tambien_lo_dice(self):
        panel = _leer("templates/ot2/panel.html")
        i = panel.index('<span class="ev-pchip ambar" title="Hallazgos que el técnico reportó y nadie ha gestionado">')
        self.assertIn("Hallazgos abiertos: {{ f.hall_abiertos }}", panel[i:i + 300])
        self.assertIn("{% if f.hall_abiertos %}", panel[i - 200:i])

    def test_el_archivo_conserva_crlf_y_parsea(self):
        for rel in ("templates/ot2/panel.html", "templates/ot2/detalle.html", "templates/ot2/_modal_crear.html"):
            with open(os.path.join(RAIZ, rel.replace("/", os.sep)), "rb") as fh:
                crudo = fh.read()
            self.assertEqual(crudo.count(b"\n"), crudo.count(b"\r\n"), rel)
            jinja2.Environment(extensions=["jinja2.ext.do"]).parse(crudo.decode("utf-8"))
        jinja2.Environment(extensions=["jinja2.ext.do"]).parse(_leer("templates/ot2/_hallazgos.html"))


# ─────────────── el JS de la tarjeta, ejecutado con Node y un DOM de mentira ───────────────
_HARNESS_TARJETA = r"""
const vm = require('vm'), fs = require('fs');
const codigo = fs.readFileSync(process.argv[2], 'utf8');
async function escenario(config, accion) {
  const log = {fetches: [], toasts: [], reloads: 0, prompts: [], confirms: 0, replace: [], lightbox: []};
  const listeners = {};
  const cuenta = {textContent: ''}, spanLevantar = {textContent: ''}, spanTodo = {textContent: ''};
  const btnLevantar = {disabled: true, querySelector: () => spanLevantar};
  const checks = (config.checks || []).map(c => Object.assign({checked: false, disabled: false}, c));
  const document = {
    addEventListener: (t, f) => { listeners[t] = f; },
    querySelectorAll: (sel) => sel === '[data-hz-sel]:not(:disabled)' ? checks.filter(c => !c.disabled) : [],
    querySelector: (sel) => ({'[data-hz-cuenta]': cuenta, '[data-hz-levantar-sel]': btnLevantar, '[data-hz-todo] span': spanTodo})[sel] || null,
    getElementById: () => null,
  };
  const location = {pathname: '/ot/171', search: '', _h: null, reload: () => { log.reloads++; }, get href() { return this._h; }, set href(v) { this._h = v; }};
  const win = {OTD_HZ_CFG: {vid: 171, numero: 'OT-2026-00171'},
    ilusToast: (m, o) => log.toasts.push([m, (o || {}).type]),
    ilusPrompt: (o) => { log.prompts.push(o); return Promise.resolve(config.prompt === undefined ? null : config.prompt); },
    ilusConfirm: (o) => { log.confirms++; return Promise.resolve(config.confirm !== false); },
    ilusLightbox: (imgs, idx, alt) => log.lightbox.push([imgs, idx, alt])};
  const ctx = {window: win, document, location, history: {replaceState: (a, b, u) => log.replace.push(u)}, setTimeout: (f) => f(),
    fetch: (url, init) => { log.fetches.push([url, init.method, init.body || null]); return Promise.resolve({status: config.status || 200, json: () => Promise.resolve(config.respuesta || {ok: true})}); },
    console, JSON, parseInt, String, Array, Promise};
  vm.createContext(ctx);
  vm.runInContext(codigo, ctx);
  const boton = (attr, valor, extra) => { const b = Object.assign({getAttribute: (n) => n === attr ? String(valor) : null}, extra || {}); return {closest: (sel) => sel === '[' + attr + ']' ? b : null}; };
  const clic = (attr, valor, extra) => listeners.click({target: boton(attr, valor, extra), preventDefault() {}});
  const cambio = (c) => listeners.change({target: {matches: (s) => s === '[data-hz-sel]', checked: c.checked}});
  await accion({clic, cambio, checks, log, cuenta, btnLevantar, spanTodo, spanLevantar, location, win, listeners});
  for (let i = 0; i < 8; i++) await new Promise(r => setImmediate(r));
  return {log, href: location.href, cuenta: cuenta.textContent, todo: spanTodo.textContent, levantarDeshabilitado: btnLevantar.disabled,
          levantarTexto: spanLevantar.textContent, checks: checks.map(c => c.checked)};
}
(async () => {
  const r = {};
  r.tomar = await escenario({}, async (h) => { h.clic('data-hz-tomar', 5); });
  r.tomar_error = await escenario({respuesta: {ok: false, error: 'Este caso ya lo tomó Aarón Rojas.'}, status: 409}, async (h) => { h.clic('data-hz-tomar', 5); });
  r.doble_clic = await escenario({}, async (h) => { h.clic('data-hz-tomar', 5); h.clic('data-hz-tomar', 5); });
  r.descartar = await escenario({prompt: 'El cliente ya lo resolvió'}, async (h) => { h.clic('data-hz-descartar', 6, {}); });
  r.descartar_titulo = await escenario({prompt: null}, async (h) => { h.clic('data-hz-descartar', 6); });
  r.reabrir = await escenario({}, async (h) => { h.clic('data-hz-reabrir', 8); });
  r.reabrir_no = await escenario({confirm: false}, async (h) => { h.clic('data-hz-reabrir', 8); });
  r.levantar = await escenario({}, async (h) => { h.clic('data-hz-levantar', 7); });
  r.todo = await escenario({checks: [{value: '1'}, {value: '2'}, {value: '3', disabled: true}]}, async (h) => { h.clic('data-hz-todo', ''); });
  r.todo_dos_veces = await escenario({checks: [{value: '1'}, {value: '2'}]}, async (h) => { h.clic('data-hz-todo', ''); h.clic('data-hz-todo', ''); });
  r.todo_parcial = await escenario({checks: [{value: '1', checked: true}, {value: '2'}]}, async (h) => { h.cambio(h.checks[0]); h.clic('data-hz-todo', ''); });
  r.levantar_elegidos = await escenario({checks: [{value: '1'}, {value: '2'}, {value: '3', disabled: true}]},
    async (h) => { h.clic('data-hz-todo', ''); h.clic('data-hz-levantar-sel', ''); });
  r.levantar_sin_elegir = await escenario({checks: [{value: '1'}]}, async (h) => { h.clic('data-hz-levantar-sel', ''); });
  r.foto = await escenario({}, async (h) => {
    const cont = {getAttribute: () => '["/f/a.jpg","/f/b.jpg"]'};
    const item = {querySelector: () => ({textContent: '  Trotadora X: Falla  '})};
    const btn = {getAttribute: (n) => n === 'data-hz-foto' ? '1' : null, closest: (s) => s === '[data-hz-fotos]' ? cont : (s === '.hz-item' ? item : null)};
    h.listeners.click({target: {closest: (s) => s === '[data-hz-foto]' ? btn : null}, preventDefault() {}});
  });
  console.log(JSON.stringify(r));
})().catch(e => { console.error(e); process.exit(1); });
"""


@unittest.skipUnless(HAY_NODE, "Node no está instalado")
class TestJsDeLaTarjeta(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with tempfile.TemporaryDirectory() as d:
            ruta = os.path.join(d, "arnes.js")
            with open(ruta, "w", encoding="utf-8") as fh:
                fh.write(_HARNESS_TARJETA)
            r = subprocess.run(["node", ruta, os.path.join(RAIZ, "static", "ot_hallazgos.js")], capture_output=True, text=True, encoding="utf-8", timeout=90)
        if r.returncode != 0:
            raise AssertionError("node falló:\n" + r.stderr[-2500:])
        cls.R = json.loads(r.stdout.strip().splitlines()[-1])

    def test_el_archivo_tiene_sintaxis_valida_y_sin_popups_nativos(self):
        ruta = os.path.join(RAIZ, "static", "ot_hallazgos.js")
        self.assertEqual(subprocess.run(["node", "--check", ruta], capture_output=True, text=True, timeout=60).returncode, 0)
        js = _leer("static/ot_hallazgos.js")
        self.assertIsNone(re.search(r"(?<![\w.])(alert|confirm|prompt)\(", js), "REGLA #1")
        for helper in ("ilusPrompt", "ilusConfirm", "ilusToast", "ilusLightbox"):
            self.assertIn(helper, js)

    def test_tomar_el_caso_llama_a_su_ruta_avisa_y_recarga_en_la_tarjeta(self):
        r = self.R["tomar"]
        self.assertEqual(r["log"]["fetches"], [["/ot/api/171/hallazgos/5/tomar", "POST", None]])
        self.assertEqual(r["log"]["toasts"], [["✓ Tomaste el caso. Queda a tu nombre.", "success"]])
        self.assertEqual(r["log"]["reloads"], 1)
        self.assertEqual(r["log"]["replace"], ["/ot/171#otdHallazgos"], "la persona vuelve a donde estaba")

    def test_un_rechazo_del_servidor_se_dice_con_su_motivo_y_no_recarga(self):
        r = self.R["tomar_error"]
        self.assertEqual(r["log"]["toasts"], [["Este caso ya lo tomó Aarón Rojas.", "error"]])
        self.assertEqual(r["log"]["reloads"], 0)

    def test_un_doble_clic_no_manda_dos_veces(self):
        self.assertEqual(len(self.R["doble_clic"]["log"]["fetches"]), 1)

    def test_descartar_pide_el_motivo_con_el_modal_de_ilus_y_lo_manda(self):
        r = self.R["descartar"]
        self.assertEqual(len(r["log"]["fetches"]), 1)
        url, metodo, cuerpo = r["log"]["fetches"][0]
        self.assertEqual((url, metodo, json.loads(cuerpo)), ("/ot/api/171/hallazgos/6/descartar", "POST", {"motivo": "El cliente ya lo resolvió"}))
        self.assertEqual(r["log"]["toasts"][0][1], "success")
        p = r["log"]["prompts"][0]
        self.assertTrue(p["required"] and p["multiline"])
        self.assertEqual(p["okLabel"], "Descartar")
        self.assertIn("mínimo 10", p["placeholder"])

    def test_descartar_cancelado_no_manda_nada(self):
        r = self.R["descartar_titulo"]
        self.assertEqual(r["log"]["fetches"], [])
        self.assertEqual(len(r["log"]["prompts"]), 1)

    def test_reabrir_pide_confirmacion(self):
        self.assertEqual(self.R["reabrir"]["log"]["fetches"], [["/ot/api/171/hallazgos/8/reabrir", "POST", None]])
        self.assertEqual(self.R["reabrir"]["log"]["confirms"], 1)
        self.assertEqual(self.R["reabrir_no"]["log"]["fetches"], [])

    def test_levantar_abre_el_asistente_prellenado(self):
        self.assertEqual(self.R["levantar"]["href"], "/ot/?crear=1&seguimiento=7&desde=171")

    def test_seleccionar_todo_es_un_interruptor_regla_14(self):
        uno = self.R["todo"]
        self.assertEqual(uno["checks"], [True, True, False], "marca los que se pueden elegir y no toca el deshabilitado")
        self.assertEqual((uno["cuenta"], uno["todo"]), ("2 de 2 elegidos", "Quitar la selección"))
        self.assertEqual((uno["levantarDeshabilitado"], uno["levantarTexto"]), (False, "Levantar una OT con los 2 elegidos"))
        dos = self.R["todo_dos_veces"]
        self.assertEqual(dos["checks"], [False, False], "la misma acción desmarca cuando ya estaban todos")
        self.assertEqual((dos["cuenta"], dos["todo"], dos["levantarDeshabilitado"]), ("0 de 2 elegidos", "Seleccionar todo", True))
        parcial = self.R["todo_parcial"]
        self.assertEqual(parcial["checks"], [True, True], "si alguno estaba sin marcar, marca todos")

    def test_levantar_con_lo_elegido_manda_todos_los_ids_y_sin_elegir_avisa(self):
        self.assertEqual(self.R["levantar_elegidos"]["href"], "/ot/?crear=1&seguimiento=1,2&desde=171")
        r = self.R["levantar_sin_elegir"]
        self.assertIsNone(r["href"])
        self.assertEqual(r["log"]["toasts"], [["Elige al menos un hallazgo para la OT de seguimiento.", "warning"]])

    def test_las_fotos_abren_el_visor_global_con_el_titulo_como_pie(self):
        (imgs, idx, alt), = self.R["foto"]["log"]["lightbox"]
        self.assertEqual(imgs, [{"url": "/f/a.jpg", "caption": "Trotadora X: Falla"}, {"url": "/f/b.jpg", "caption": "Trotadora X: Falla"}])
        self.assertEqual((idx, alt), (1, "Hallazgo"))


def _extraer_funcion_js(fuente, nombre):
    """El texto de `function nombre(...) {...}` contando llaves y saltando cadenas ('…', "…") y comentarios."""
    i = fuente.index("function %s(" % nombre)
    j = fuente.index("{", fuente.index(")", i))
    prof, k, n = 0, j, len(fuente)
    while k < n:
        c = fuente[k]
        if c in "'\"":
            q = c
            k += 1
            while k < n and fuente[k] != q:
                k += 2 if fuente[k] == "\\" else 1
        elif fuente.startswith("//", k):
            k = fuente.index("\n", k)
        elif fuente.startswith("/*", k):
            k = fuente.index("*/", k) + 1
        elif c == "{":
            prof += 1
        elif c == "}":
            prof -= 1
            if prof == 0:
                return fuente[i:k + 1]
        k += 1
    raise AssertionError("no cerró la función " + nombre)


_HARNESS_ASISTENTE = r"""
const vm = require('vm');
const fuentes = JSON.parse(process.argv[2]);
function nuevo(S, interno) {
  const repintados = [];
  const esc = (s) => String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  const ctx = {S, esc, esInterno: () => interno, repintarSeccion: (k) => repintados.push(k), encodeURIComponent, String, Object, Array};
  vm.createContext(ctx);
  vm.runInContext(fuentes.join('\n'), ctx);
  return {ctx, repintados};
}
const hall = (id, sev, color, titulo) => ({id, sev_texto: sev, sev_color: color, titulo});
function seg(extra) {
  return Object.assign({origen_id: 171, origen_numero: 'OT-2026-00171', cliente_id: 26, ids: [1, 2],
    hallazgos: [hall(1, 'Falla', 'rojo', 'Trotadora Pro <b>3000</b>: Falla'), hall(2, 'Dar de baja', 'negro', 'Bici X: baja')],
    fotos: ['/f/a.jpg', '/f/b.jpg'], fotos_ok: {'/f/a.jpg': true, '/f/b.jpg': true}}, extra || {});
}
const r = {};
{
  const S = {seguimiento: seg(), cliente: {id: 26}, origen: 'cliente'};
  const {ctx, repintados} = nuevo(S, false);
  r.vigente = vm.runInContext('_o2mSegVigente()', ctx);
  r.html = vm.runInContext('_o2mSegHtml()', ctx);
  vm.runInContext('segFoto(0)', ctx);
  r.tras_quitar_foto = {ok: Object.assign({}, S.seguimiento.fotos_ok), html: vm.runInContext('_o2mSegHtml()', ctx), repintados: repintados.slice()};
  vm.runInContext('segFoto(0)', ctx);
  r.tras_dejar_foto = Object.assign({}, S.seguimiento.fotos_ok);
  vm.runInContext('segFoto(99)', ctx);
  vm.runInContext('segQuitar()', ctx);
  r.tras_quitar = {seg: S.seguimiento, repintados: repintados.slice(), html: vm.runInContext('_o2mSegHtml()', ctx)};
}
{
  const S = {seguimiento: seg(), cliente: {id: 27}, origen: 'cliente'};
  const {ctx} = nuevo(S, false);
  r.otro_cliente = {vigente: vm.runInContext('_o2mSegVigente()', ctx), html: vm.runInContext('_o2mSegHtml()', ctx)};
}
{
  const S = {seguimiento: seg({cliente_id: null}), cliente: null, origen: 'interno'};
  const {ctx} = nuevo(S, true);
  r.interna = vm.runInContext('_o2mSegVigente()', ctx);
  const S2 = {seguimiento: seg(), cliente: null, origen: 'interno'};
  r.interna_con_origen_de_cliente = vm.runInContext('_o2mSegVigente()', nuevo(S2, true).ctx);
  const S3 = {seguimiento: null, cliente: {id: 26}, origen: 'cliente'};
  r.sin_seguimiento = {vigente: vm.runInContext('_o2mSegVigente()', nuevo(S3, false).ctx), html: vm.runInContext('_o2mSegHtml()', nuevo(S3, false).ctx)};
}
console.log(JSON.stringify(r));
"""


@unittest.skipUnless(HAY_NODE, "Node no está instalado")
class TestAsistenteConSeguimiento(unittest.TestCase):
    """El aviso del asistente «Nueva OT» (_o2mSegHtml y compañía), ejecutado con Node sobre el código real de la plantilla."""

    @classmethod
    def setUpClass(cls):
        modal = _leer("templates/ot2/_modal_crear.html")
        fuentes = [_extraer_funcion_js(modal, n) for n in ("_o2mSegVigente", "_o2mSegHtml", "segQuitar", "segFoto")]
        with tempfile.TemporaryDirectory() as d:
            ruta = os.path.join(d, "arnes.js")
            with open(ruta, "w", encoding="utf-8") as fh:
                fh.write(_HARNESS_ASISTENTE)
            r = subprocess.run(["node", ruta, json.dumps(fuentes)], capture_output=True, text=True, encoding="utf-8", timeout=90)
        if r.returncode != 0:
            raise AssertionError("node falló:\n" + r.stderr[-2500:])
        cls.R = json.loads(r.stdout.strip().splitlines()[-1])

    def test_aviso_con_el_origen_los_hallazgos_y_las_fotos_de_referencia(self):
        r = self.R
        self.assertTrue(r["vigente"])
        h = r["html"]
        self.assertIn('Seguimiento de <a href="/ot/171" target="_blank" rel="noopener"', h)
        self.assertIn(">OT-2026-00171</a>", h)
        self.assertIn("Nace de 2 hallazgos", h)
        self.assertIn("No hereda cobros ni saldo", h)
        self.assertIn("Crear sin ligarla", h)
        self.assertIn("Dar de baja</span>Bici X: baja", h)
        self.assertEqual(h.count("OT2C.segFoto("), 2)
        self.assertEqual(h.count('aria-pressed="true"'), 2)
        self.assertIn("min-height:44px", h)

    def test_lo_que_trae_el_hallazgo_va_escapado(self):
        self.assertNotIn("<b>3000</b>", self.R["html"])
        self.assertIn("Trotadora Pro &lt;b&gt;3000&lt;/b&gt;: Falla", self.R["html"])

    def test_cada_foto_se_puede_dejar_fuera_y_volver_a_poner(self):
        q = self.R["tras_quitar_foto"]
        self.assertEqual(q["ok"], {"/f/a.jpg": False, "/f/b.jpg": True})
        self.assertEqual(q["repintados"], ["detalle"])
        self.assertEqual(q["html"].count('aria-pressed="false"'), 1)
        self.assertIn('aria-label="Dejar la foto 1"', q["html"])
        self.assertEqual(self.R["tras_dejar_foto"], {"/f/a.jpg": True, "/f/b.jpg": True})

    def test_crear_sin_ligarla_quita_el_vinculo(self):
        q = self.R["tras_quitar"]
        self.assertIsNone(q["seg"])
        self.assertEqual(q["html"], "")
        self.assertEqual(q["repintados"], ["detalle", "detalle", "detalle"] if len(q["repintados"]) == 3 else q["repintados"])
        self.assertEqual(q["repintados"][-1], "detalle")

    def test_si_cambia_de_cliente_el_vinculo_ya_no_corresponde_y_no_se_manda(self):
        o = self.R["otro_cliente"]
        self.assertFalse(o["vigente"])
        self.assertIn("Cambiaste de cliente", o["html"])
        self.assertIn("Entendido, quitar el vínculo", o["html"])
        self.assertNotIn("OT2C.segFoto", o["html"])

    def test_trabajo_interno_solo_se_liga_a_una_origen_interna(self):
        self.assertTrue(self.R["interna"])
        self.assertFalse(self.R["interna_con_origen_de_cliente"])
        self.assertEqual(self.R["sin_seguimiento"], {"vigente": False, "html": ""})


class TestLaReglaEstaDocumentada(unittest.TestCase):
    def test_claude_md_tiene_la_regla_38_antes_del_pie(self):
        md = _leer("CLAUDE.md")
        i = md.index("## 🩺 REGLA #38")
        regla = md[i:md.index("_Última actualización", i)]
        for pieza in ("Indícame si se está tomando fotografías como evidencia", "_hall_tras_escritura", "_HALL_RUTAS", "_ot_gestion_destinatarios",
                      "ot_hallazgo", "ot_hallazgo_toma", "SOLO AVISA, NO TRABA", "ot_madre_id", "ot_relacion='seguimiento'",
                      "no hereda cobros ni saldo", "mant_ot_hallazgos", "_ensure_ot_hallazgos", "NO se rellenan", "@_no_tecnico", "REGLA #22",
                      "tests/test_ot_hallazgos.py", "OT CERRADA", "puede_metadata", "Con hallazgos sin gestionar", "sql_abierto", "estado_efectivo"):
            self.assertIn(pieza, regla, pieza)
        self.assertLess(i, md.index("_Mantenedor:"), "va antes del pie")
        self.assertGreater(i, md.index("## REGLA #37"), "las reglas van en orden")

    def test_las_reglas_anteriores_siguen_intactas(self):
        md = _leer("CLAUDE.md")
        for regla in ("## 🧭 REGLA #35", "## 🙋 REGLA #36", "## REGLA #37"):
            self.assertEqual(md.count(regla), 1, regla)



# ═══════════ 2026-10-21: la tarjeta sale de la OT · página «Hallazgos» · resumen por equipo + N° de serie · burbuja del panel ═══════════
class TestHallazgosFueraDeLaOT(unittest.TestCase):
    """Daniel (21-oct): «saca la tarjeta grande de los hallazgos… que quede alertado solamente en la parte de trabajo… por el equipo y
    por el N° de serie» y «colocar una parte de hallazgos para que lo veamos todos… con una burbujita roja… va a desaparecer cuando
    alguien se haga cargo»."""
    DETALLE = _leer("templates/ot2/detalle.html")
    PANEL = _leer("templates/ot2/panel.html")
    PAGINA = _leer("templates/ot2/hallazgos.html")

    def _ctx(self):
        it = {"id": 1, "sev_color": "rojo", "sev_icono": "bi-x", "sev_texto": "Falla", "estado_ef": "abierto", "estado_color": "ambar",
              "estado_txt": "Sin gestionar", "titulo": "Piola cortada", "fotos": [], "puede_tomar": True, "puede_levantar": True,
              "puede_descartar": True, "puede_reabrir": False}
        return {"vid": 171, "numero_ot": "OT-2026-00171", "n_total": 1, "n_abiertos": 1, "activos": [it], "retirados": [], "origen": None,
                "n_seleccionables": 1, "grupos": [{"key": "e9", "nombre": "Polea alta ILUS", "serie": "SN-123", "sku": "1121",
                                                   "color": "rojo", "n_abiertos": 1, "items": [it]}]}

    def test_en_la_ficha_de_la_ot_la_tarjeta_grande_no_se_dibuja(self):
        html = _entorno().get_template("ot2/_hallazgos.html").render(hallazgos=self._ctx(), es_tecnico=False)
        self.assertNotIn('<section class="hz"', html)
        self.assertNotIn("ot_hallazgos.js", html)

    def test_en_la_pagina_va_agrupada_por_equipo_y_numero_de_serie(self):
        html = _tarjeta(self._ctx())
        self.assertIn('<section class="hz"', html)
        g = html[html.index('class="hz-grupo'):]
        self.assertIn("Polea alta ILUS", g[:600])
        self.assertIn("S/N SN-123", g[:600])
        self.assertLess(html.index('class="hz-grupo'), html.index('data-hz-id="1"'), "el equipo va antes de sus hallazgos")

    def test_un_tecnico_no_ve_nada(self):
        self.assertEqual(_entorno().get_template("ot2/_hallazgos.html").render(hallazgos=self._ctx(), es_tecnico=True,
                                                                                 hz_pagina=True).strip(), "")

    def test_las_rutas_nuevas_son_solo_de_gestion(self):
        for ruta, fn in (('@app.route("/ot/hallazgos", methods=["GET"])', "def ot_hallazgos_pagina("),
                         ('@app.route("/ot/api/hallazgos/contador", methods=["GET"])', "def ot_api_hallazgos_contador(")):
            i = APP.index(ruta)
            cab = APP[i:APP.index(fn, i)]
            self.assertIn("@_mant_required", cab)
            self.assertIn("@_no_tecnico", cab)

    def test_la_burbuja_cuenta_los_sin_gestionar_con_la_misma_definicion(self):
        f = _fuente_funcion("_hall_n_sin_gestionar")
        self.assertIn('_oh.sql_abierto("h", "s")', f)
        self.assertIn("LEFT JOIN mant_visitas s ON s.id = h.ot_seguimiento_id", f)
        pag = _fuente_funcion("ot_hallazgos_pagina")
        self.assertIn('_oh.sql_abierto("h", "s")', pag)

    def test_el_panel_trae_el_boton_con_burbuja_y_los_filtros_de_fecha_bajan(self):
        self.assertIn("url_for('ot_hallazgos_pagina')", self.PANEL)
        self.assertIn('id="ot2HzBur" hidden', self.PANEL)
        self.assertIn("fetch('/ot/api/hallazgos/contador'", self.PANEL)
        cab = self.PANEL[self.PANEL.index('<div class="page-header'):self.PANEL.index('class="ot2-rep-per"')]
        self.assertNotIn('id="ot2RepDesde"', cab, "el período del Excel ya no va en la cabecera")
        per = self.PANEL[self.PANEL.index('class="ot2-rep-per"'):]
        for campo in ('id="ot2RepDesde"', 'id="ot2RepHasta"', 'id="ot2RepPeriodo"'):
            self.assertIn(campo, per[:4000], campo)
        self.assertLess(per.index('id="ot2RepPeriodo"'), per.index("getElementById('ot2RepPeriodo')"), "el script corre con los campos ya dibujados")

    def test_la_pagina_pagina_como_etiquetas(self):
        # 26-oct: el monitor tiene el MISMO pie que Retiros/Etiquetas (REGLA #4.3): Mostrando A–B de N · por página · Anterior / Página X de Y / Siguiente.
        for t in ("Mostrando <strong>{{ desde }}–{{ hasta }}</strong> de <strong>{{ total }}</strong>", "por página", "Anterior", "Página {{ pag }} de {{ paginas }}", "Siguiente"):
            self.assertIn(t, self.PAGINA, t)
        self.assertIn("{% include 'ot2/_hallazgos.html' %}", self.PAGINA)

    def test_el_resumen_vive_en_la_pestana_trabajo(self):
        i = self.DETALLE.index('<div class="otd-tabpanel" data-tab="trabajo">')
        self.assertIn('id="otdResumenEquipos"', self.DETALLE[i:i + 3000])
        self.assertEqual(self.DETALLE.count('id="otdResumenEquipos"'), 1)

    def test_el_resumen_agrupa_por_equipo_y_serie(self):
        amb = cargar(["_ot_resumen_equipos"], extra={"_otrep_serie_producto": lambda vid, imid: "SN-PROD", "_oh": H})
        equipos = [
            {"id": 9, "nombre": "Polea alta", "serie": "SN-1", "estado_capturado": "fuera_servicio", "rep_solicitudes": [
                {"repuesto_nombre": "Piola", "cantidad": 2, "abierta": True}]},
            {"id": 10, "nombre": "Banco plano", "serie": "", "estado_capturado": "operativo", "rep_solicitudes": []},
            {"id": 4, "nombre": "Trotadora", "_interno": True, "estado_prod": {"estado": "no_llego"}, "rep_solicitudes": []},
        ]
        hall = {"grupos": [{"key": "e9", "serie": "SN-1", "peor": 0, "n_abiertos": 2, "n_tomados": 1, "n_con_ot": 0, "items": [1, 2, 3]}]}
        filas = amb["_ot_resumen_equipos"](171, equipos, hall)
        self.assertEqual([f["key"] for f in filas], ["e9", "i4"], "un equipo sin nada que contar no aparece")
        e9 = filas[0]
        self.assertEqual(e9["serie"], "SN-1")
        txt = " | ".join(c["txt"] for c in e9["chips"])
        for t in ("Fuera de servicio", "Repuesto en curso: Piola × 2", "2 hallazgos sin gestionar", "1 con el caso tomado"):
            self.assertIn(t, txt)
        self.assertEqual(filas[1]["serie"], "SN-PROD", "la serie del producto sale de su checklist")
        self.assertEqual(filas[1]["chips"][0]["txt"], "No llegó")
        # a un técnico no le llegan hallazgos (None): solo lo que ya ve en la tarjeta del equipo
        txt_t = " ".join(c["txt"] for c in amb["_ot_resumen_equipos"](171, equipos, None)[0]["chips"])
        self.assertNotIn("hallazgo", txt_t)

# ═══════════ 2026-10-22 · AUDITORÍA IMPECCABLE, FASE 1 («lo que ya está y confunde»): H-12 a H-28 (sin H-13, H-21 ni H-29 a H-31) ═══════════
# Informe: outputs/impeccable_hallazgos_2026-10-21.md (secciones 4 y 6). Solo PRESENTACIÓN y texto: el estado guardado ('resuelto'), las reglas de negocio, las
# rutas y los candados no cambian, y no se quita ningún botón ni función (REGLA #4.2). Los nombres de abajo son los reales vistos en producción el 21-oct.
_BANCO = "Banco Ajustable ILUS Bio (SKU 1129100696) · S/N 255470655-0696-1"
_DYNAMIS = "ILUS Dynamis 4 Stacks Jungle (SKU BA023) · S/N 255470655-A023-1"
_KAIROS = "ILUS Kairos Row Machine (SKU 1121100518) · S/N 255470655-0518-1"


def _regla_css(css, selector):
    """El cuerpo de la primera regla CSS que se llama exactamente `selector` ('' si no existe)."""
    m = re.search(r"(?:^|[}\s])" + re.escape(selector) + r"\s*\{([^}]*)\}", css)
    return m.group(1) if m else ""


def _luminancia(hexa):
    h = hexa.lstrip("#")
    canales = [int(h[i:i + 2], 16) / 255.0 for i in (0, 2, 4)]
    r, g, b = [c / 12.92 if c <= 0.03928 else ((c + 0.055) / 1.055) ** 2.4 for c in canales]
    return 0.2126 * r + 0.7152 * g + 0.0722 * b


def _contraste(a, b):
    """Razón de contraste WCAG entre dos colores #rrggbb."""
    mayor, menor = sorted((_luminancia(a), _luminancia(b)), reverse=True)
    return (mayor + 0.05) / (menor + 0.05)


def escena_bancos(solo_banco=False):
    """La OT-2026-00340 de la auditoría, con los nombres guardados «con la serie de ese momento»: un banco con dos alertas del checklist (grupo e9),
    un repuesto pedido (e10), una falla sin gestionar (e11), una falla con su OT de seguimiento EN CURSO (e12) y una baja con su OT de seguimiento
    ya CERRADA (e13). Con `solo_banco` queda solo el banco (una OT con un único equipo)."""
    m = Mundo(usuario=GESTOR)
    m.visitas[340] = dict(VISITA_171, id=340, numero_ot="OT-2026-00340")
    m.visitas[400] = {"id": 400, "numero_ot": "OT-2026-00400", "estado": "en_curso", "cliente_id": 26, "ot_madre_id": 340,
                      "ot_relacion": "seguimiento", "firma_cliente_url": None}
    m.visitas[401] = {"id": 401, "numero_ot": "OT-2026-00401", "estado": "completada", "cliente_id": 26, "ot_madre_id": 340,
                      "ot_relacion": "seguimiento", "firma_cliente_url": None}
    m.maquinas.update({
        9: {"nombre": "Banco Ajustable ILUS Bio", "sku": "1129100696", "serie": "255470655-0696-1"},
        10: {"nombre": "ILUS Dynamis 4 Stacks Jungle", "sku": "BA023", "serie": "255470655-A023-1"},
        11: {"nombre": "Prensa de piernas", "sku": "PP1", "serie": "SN-PP1"},
        12: {"nombre": "Cinta de correr X", "sku": "CC1", "serie": "SN-CC1"},
        13: {"nombre": "Remo Kairos", "sku": "RK1", "serie": "SN-RK1"}})
    a = m.agregar
    e = types.SimpleNamespace(m=m, amb=m.ambiente())
    e.h1 = a(340, ref="t1", severidad="alerta", tarea_id=1, maquina_id=9, equipo_nombre=_BANCO, titulo=_BANCO + " · Tapiz y espuma: Alerta")
    e.h2 = a(340, ref="t2", severidad="alerta", tarea_id=2, maquina_id=9, equipo_nombre=_BANCO,
             titulo=_BANCO + " · Mecanismo de ajuste de respaldo y asiento: Alerta")
    if not solo_banco:
        e.h3 = a(340, ref="m10:rep:ab12", origen="equipo", severidad="repuesto", maquina_id=10, equipo_nombre=_DYNAMIS,
                 titulo=_DYNAMIS + ": Repuesto solicitado: Varilla guía o buje de guía × 5", fotos_json='["/f/rep1.jpg"]', n_fotos=1,
                 detalle="Se perdió o falta — Está raspada · Evidencia: 1 foto(s).")
        e.h4 = a(340, ref="m11:baja", origen="equipo", severidad="falla", maquina_id=11, equipo_nombre="Prensa de piernas (SKU PP1) · S/N SN-PP1",
                 titulo="Prensa de piernas (SKU PP1) · S/N SN-PP1: Falla detectada")
        e.h5 = a(340, ref="m12:falla", origen="equipo", severidad="falla", maquina_id=12, equipo_nombre="Cinta de correr X (SKU CC1) · S/N SN-CC1",
                 titulo="Cinta de correr X (SKU CC1) · S/N SN-CC1: Falla detectada", estado="resuelto", ot_seguimiento_id=400)
        e.h6 = a(340, ref="m13:baja", origen="equipo", severidad="baja", maquina_id=13, equipo_nombre="Remo Kairos (SKU RK1) · S/N SN-RK1",
                 titulo="Remo Kairos (SKU RK1) · S/N SN-RK1: No tiene arreglo (dar de baja)", estado="resuelto", ot_seguimiento_id=401)
    e.ctx = lambda: e.amb["_hall_contexto_ficha"](340, e.m.visitas[340])        # noqa: E731
    return e


class TestFase1Puro(unittest.TestCase):
    """El módulo puro: lo que se MUESTRA de lo guardado."""

    def test_h12_con_ot_de_seguimiento_es_azul_en_curso_y_verde_solo_cuando_esa_ot_se_cerro(self):
        self.assertEqual(H.estado_visual("resuelto", False), {"texto": "OT de seguimiento en curso", "color": "azul"})
        self.assertEqual(H.estado_visual("resuelto", True), {"texto": "OT de seguimiento cerrada", "color": "verde"})
        self.assertEqual(H.estado_visual("resuelto"), H.estado_visual("resuelto", False), "sin saber el estado de la OT, se asume abierta")
        # el criterio de Retiros: ámbar = te toca, azul = espera, verde = hecho
        self.assertEqual(H.estado_visual("abierto"), {"texto": "Sin gestionar", "color": "ambar"})
        self.assertEqual(H.estado_visual("tomado"), {"texto": "Caso tomado", "color": "azul"})
        self.assertEqual(H.estado_visual("descartado"), {"texto": "Descartado", "color": "gris"})
        self.assertEqual(H.estado_visual("abierto", True), H.estado_visual("abierto"), "seg_cerrada solo importa para 'resuelto'")
        self.assertEqual(H.estado_visual("raro")["texto"], "Sin gestionar")
        # LO GUARDADO no cambia: es la misma definición que usan el panel, el filtro, la franja del cierre, la bandeja y la ficha del cliente
        self.assertEqual(H.EST_RESUELTO, "resuelto")
        self.assertEqual(H.ESTADOS["resuelto"], {"texto": "Con OT de seguimiento", "color": "verde"})
        self.assertEqual(H.estado_info("resuelto")["texto"], "Con OT de seguimiento")

    def test_h12_cuando_se_considera_cerrada_la_ot_de_seguimiento(self):
        for cerrada in ("cerrada", "completada", "Cerrada", " COMPLETADA "):
            self.assertTrue(H.seguimiento_cerrado(cerrada), cerrada)
        for abierta in ("programada", "en_curso", "pendiente_aprobacion", "cancelada", "anulada", "", None):
            self.assertFalse(H.seguimiento_cerrado(abierta), repr(abierta))

    def test_h16_el_titulo_dice_lo_que_paso_sin_repetir_el_equipo(self):
        f = H.titulo_corto
        self.assertEqual(f(_BANCO + " · Tapiz y espuma: Alerta", _BANCO), "Tapiz y espuma: Alerta")
        self.assertEqual(f(_BANCO + " · Mecanismo de ajuste de respaldo y asiento: Alerta", _BANCO), "Mecanismo de ajuste de respaldo y asiento: Alerta")
        # una acción del técnico: «<equipo>: <qué pasó>»
        self.assertEqual(f(_DYNAMIS + ": Repuesto solicitado: Varilla guía o buje de guía × 5", _DYNAMIS), "Repuesto solicitado: Varilla guía o buje de guía × 5")
        self.assertEqual(f("Banco plano X: No se puede usar — pieza rota", "Banco plano X"), "No se puede usar — pieza rota")
        # lo que no se puede acortar con seguridad se deja TAL CUAL (REGLA #15: nada inventado ni cortado)
        for titulo, equipo in (("Revisar cinta: Falla", ""), ("Revisar cinta: Falla", None), ("", "Banco"), ("Banco", "Banco"),
                               ("Banco X (SKU 1) · Tapiz: Alerta", "Banco X"), ("Otra cosa: Falla", "Banco")):
            self.assertEqual(f(titulo, equipo), titulo, (titulo, equipo))
        self.assertEqual(f(None, "Banco"), "")

    def test_h16_el_sujeto_largo_que_titulo_tarea_corta_a_200_letras_tambien_se_quita(self):
        largo = ("Trotadora comercial de alto rendimiento con pantalla táctil de 22 pulgadas y banda ortopédica reforzada " * 3).strip()
        for corte in range(190, 215):               # el espacio del corte puede caer en cualquier lugar de las 200 letras
            sujeto = H.nombre_con_ficha(("x" * corte + " ") + largo, "ABC", "123")
            titulo = H.limpio(H.titulo_tarea(sujeto, "Revisar cinta", "Falla"), H.TITULO_MAX)     # así lo guarda _hall_upsert
            self.assertEqual(H.titulo_corto(titulo, sujeto), "Revisar cinta: Falla", corte)

    def test_h15_la_serie_sale_una_sola_vez_y_es_la_vigente(self):
        self.assertEqual(H.partir_serie("Banco X (SKU 1) · S/N 123"), ("Banco X (SKU 1)", "123"))
        self.assertEqual(H.partir_serie("Banco X (SKU 1)"), ("Banco X (SKU 1)", ""))
        self.assertEqual(H.partir_serie(None), ("", ""))
        f = H.equipo_con_serie
        # la bandeja decía «… · S/N 255470655-A023-1 · S/N 255470655-A023-1»
        self.assertEqual(f(_DYNAMIS, "255470655-A023-1"), _DYNAMIS)
        self.assertEqual(f(_DYNAMIS, None), _DYNAMIS, "si la ficha no trae serie queda la que se guardó")
        self.assertEqual(f("Stair Climber DSC6, LED Console (SKU DSC6)", None), "Stair Climber DSC6, LED Console (SKU DSC6)")
        self.assertEqual(f("Banco X (SKU 1)", "123"), "Banco X (SKU 1) · S/N 123", "un nombre sin serie recibe la de la ficha")
        self.assertEqual(f(None, None), "Equipo")
        # el Kairos de la OT-339: la serie cambió desde que se reportó → se dicen las dos, rotuladas
        self.assertEqual(f(_KAIROS, "255470655-055555555tt518-1"),
                         "ILUS Kairos Row Machine (SKU 1121100518) · S/N 255470655-055555555tt518-1 (al reportar: S/N 255470655-0518-1)")
        self.assertEqual(f(_KAIROS, " 255470655-0518-1 "), _KAIROS, "la misma serie con espacios de más no cuenta como distinta")

    def test_h16_el_conteo_de_fotos_sobra_cuando_las_miniaturas_se_ven(self):
        f = H.detalle_visible
        self.assertEqual(f("Se perdió o falta — Está raspada · Evidencia: 1 foto(s).", True), "Se perdió o falta — Está raspada")
        self.assertEqual(f("Evidencia: 2 foto(s).", True), "")
        self.assertEqual(f("Raspada · Evidencia: 2 foto(s), 1 video(s).", True), "Raspada · Evidencia: 1 video(s).", "los videos no se muestran: se conservan")
        self.assertEqual(f("Evidencia: 2 foto(s), 3 video(s).", True), "Evidencia: 3 video(s).")
        self.assertEqual(f("Raspada · Evidencia: 0 foto(s).", False), "Raspada · Evidencia: 0 foto(s).", "sin miniaturas el conteo sí informa")
        self.assertEqual(f("No enciende", True), "No enciende")
        self.assertEqual(f(None, True), "")

    def test_h23_el_lapso_nunca_dice_hace_el_mismo_dia(self):
        f = H.lapso_texto
        self.assertEqual((f(-1), f(0)), ("menos de un día", "menos de un día"))
        self.assertEqual((f(1), f(2), f(40)), ("1 día", "2 días", "40 días"))
        self.assertEqual((f(None), f("x")), ("", ""))
        self.assertEqual(("hace " + f(0), "En " + f(0)), ("hace menos de un día", "En menos de un día"))

    def test_h24_la_fecha_lleva_el_ano(self):
        self.assertEqual(H.FORMATO_FECHA, "%d/%m/%Y %H:%M")
        self.assertEqual(dt.datetime(2026, 10, 21, 16, 51).strftime(H.FORMATO_FECHA), "21/10/2026 16:51")

    def test_h22_el_chip_de_un_equipo_saltado_dice_el_motivo_y_no_se_contradice(self):
        f = H.chip_salto
        self.assertEqual(f("sin_acceso"), ("No se pudo revisar: sin acceso", "gris"))
        self.assertEqual(f("cliente_no_permite"), ("No se pudo revisar: el cliente no lo permitió", "gris"))
        self.assertEqual(f("otro"), ("No se pudo revisar: otro motivo", "gris"))
        self.assertEqual(f("no_encontrado"), ("No estaba en el recinto", "ambar"))
        self.assertEqual(f("falta_stock"), ("No llegó · falta stock", "ambar"))
        self.assertEqual(f("motivo_nuevo"), ("No se pudo revisar: motivo nuevo", "gris"), "una razón que nadie conoce igual se dice")
        self.assertEqual(f(""), ("No se pudo revisar", "gris"))
        # sin estados contradictorios: si ya se respondió o reportó algo en ese equipo, no se dice que «no se pudo revisar»
        self.assertEqual(f("sin_energia", parcial=True), ("Revisión incompleta: sin energía", "gris"))
        self.assertEqual(f("falta_stock", parcial=True), ("No llegó · falta stock", "ambar"), "un equipo que no llegó no tiene revisión parcial")
        # toda razón válida del servidor tiene un motivo corto legible (ninguna sale con guiones bajos)
        validas = cargar(constantes=("_RAZONES_SALTADO_VALIDAS",))["_RAZONES_SALTADO_VALIDAS"]
        self.assertGreaterEqual(len(validas), 10)
        for razon in validas:
            if razon == "dado_de_baja":                      # ese equipo no dibuja este chip (ya tiene el de «Dado de baja»)
                continue
            self.assertNotIn("_", f(razon)[0], razon)
            self.assertIn(razon, list(H.SALTO_NO_ESTA) + list(H.SALTO_MOTIVO_CORTO), razon)

    def test_h20_todo_grupo_nace_abierto_daniel_26oct(self):
        # Daniel 26-oct: «me gusta ver el bosque siempre… si falta algo o ya está gestionado déjalo abierto».
        f = H.grupo_abierto
        for it in ([{"estado_ef": "abierto", "severidad": "falla"}], [{"estado_ef": "abierto", "severidad": "alerta"}],
                   [{"estado_ef": "resuelto", "severidad": "falla"}], [{"estado_ef": "descartado", "severidad": "baja"}], []):
            self.assertTrue(f(it, 3), it)
        self.assertTrue(f([{"estado_ef": "abierto", "severidad": "alerta"}], 1), "un solo equipo: no hay nada que plegar")
        self.assertTrue(f([], 1))


class TestFase1ConBase(unittest.TestCase):
    """Lo que arma el servidor con una base de mentira (nunca la real: REGLA #22)."""

    def test_h12_el_hallazgo_con_ot_de_seguimiento_se_pinta_azul_hasta_que_esa_ot_se_cierra(self):
        e = escena_bancos()
        c = e.ctx()
        por = {i["id"]: i for i in c["activos"]}
        en_curso, cerrada = por[e.h5["id"]], por[e.h6["id"]]
        self.assertEqual((en_curso["estado"], en_curso["estado_ef"]), ("resuelto", "resuelto"), "lo guardado sigue siendo 'resuelto'")
        self.assertEqual((en_curso["estado_txt"], en_curso["estado_color"], en_curso["seg_cerrada"]), ("OT de seguimiento en curso", "azul", False))
        self.assertEqual((cerrada["estado_txt"], cerrada["estado_color"], cerrada["seg_cerrada"]), ("OT de seguimiento cerrada", "verde", True))
        self.assertEqual((c["n_con_ot"], c["n_con_ot_en_curso"], c["n_con_ot_cerrada"]), (2, 1, 1))
        self.assertEqual({g["key"]: (g["n_con_ot"], g["n_con_ot_cerrada"]) for g in c["grupos"] if g["n_con_ot"]}, {"e12": (1, 0), "e13": (1, 1)})
        # la misma fila, sin tocar la base: cuando esa OT se cierra pasa de azul a verde
        e.m.visitas[400]["estado"] = "completada"
        c2 = e.ctx()
        self.assertEqual((c2["n_con_ot"], c2["n_con_ot_en_curso"], c2["n_con_ot_cerrada"]), (2, 0, 2))
        self.assertEqual({i["id"]: i["estado_color"] for i in c2["activos"] if i["estado_ef"] == "resuelto"}, {e.h5["id"]: "verde", e.h6["id"]: "verde"})
        # y si la OT de seguimiento se cancela, vuelve a «Sin gestionar» (ámbar), como siempre
        e.m.visitas[400]["estado"] = "cancelada"
        vuelto = {i["id"]: i for i in e.ctx()["activos"]}[e.h5["id"]]
        self.assertEqual((vuelto["estado_ef"], vuelto["estado_color"]), ("abierto", "ambar"))

    def test_h16_cada_hallazgo_dice_lo_que_paso_y_el_equipo_va_una_vez_en_su_grupo(self):
        e = escena_bancos()
        c = e.ctx()
        por = {i["id"]: i for i in c["activos"]}
        self.assertEqual(por[e.h1["id"]]["titulo_corto"], "Tapiz y espuma: Alerta")
        self.assertEqual(por[e.h2["id"]]["titulo_corto"], "Mecanismo de ajuste de respaldo y asiento: Alerta")
        self.assertEqual(por[e.h3["id"]]["titulo_corto"], "Repuesto solicitado: Varilla guía o buje de guía × 5")
        self.assertEqual(por[e.h1["id"]]["titulo"], e.h1["titulo"], "el título completo sigue intacto: lo usan el prellenado, la campana y la bitácora")
        self.assertEqual(por[e.h3["id"]]["detalle_ver"], "Se perdió o falta — Está raspada")
        self.assertEqual(por[e.h3["id"]]["detalle"], e.h3["detalle"], "lo guardado no se toca")
        grupos = {g["key"]: g for g in c["grupos"]}
        self.assertEqual((grupos["e9"]["nombre"], grupos["e9"]["serie"], grupos["e9"]["sku"]), ("Banco Ajustable ILUS Bio", "255470655-0696-1", "1129100696"),
                         "el encabezado dice el nombre COMPLETO y la serie vigente de la ficha (REGLA #15)")

    def test_h16_si_el_equipo_ya_no_existe_el_encabezado_no_repite_la_serie_del_nombre_guardado(self):
        e = escena_bancos(solo_banco=True)
        del e.m.maquinas[9]
        g = e.ctx()["grupos"][0]
        self.assertEqual(g["nombre"], "Banco Ajustable ILUS Bio (SKU 1129100696)", "sin «· S/N …» (la serie va aparte)")
        self.assertEqual(e.ctx()["activos"][0]["titulo_corto"], "Tapiz y espuma: Alerta")

    def test_h20_los_grupos_se_pliegan_menos_los_que_tienen_algo_grave_por_gestionar(self):
        e = escena_bancos()
        c = e.ctx()
        self.assertEqual({g["key"]: g["abierto"] for g in c["grupos"]},
                         {"e9": True, "e10": True, "e11": True, "e12": True, "e13": True},
                         "Daniel 26-oct: «ver el bosque siempre»: todo grupo nace abierto")
        un_equipo = escena_bancos(solo_banco=True).ctx()
        self.assertEqual([g["abierto"] for g in un_equipo["grupos"]], [True], "un solo equipo: nada que plegar")

    def test_h24_toda_fecha_de_la_tarjeta_lleva_el_ano(self):
        e = escena_bancos()
        amb = e.m.ambiente(chile_fmt_filter=lambda v, f="%d/%m/%Y %H:%M": v.strftime(f))
        self.assertEqual(amb["_hall_cuando"](dt.datetime(2026, 10, 21, 16, 51)), "21/10/2026 16:51")
        self.assertEqual(amb["_hall_cuando"](None), "")
        c = amb["_hall_contexto_ficha"](340, e.m.visitas[340])
        self.assertTrue(c["activos"])
        for i in c["activos"]:
            self.assertRegex(i["cuando"], r"^\d\d/\d\d/\d{4} \d\d:\d\d$", i["cuando"])

    def test_h23_la_ficha_del_cliente_nunca_dice_hace_el_mismo_dia(self):
        ahora = dt.datetime.utcnow().replace(microsecond=0)
        solicitudes = [
            {"maquina_id": 9, "visita_id": 513, "repuesto_nombre": "Pasillo", "cantidad": 2, "origen": "repuesto", "estado": "pedido",
             "created_at": ahora - dt.timedelta(hours=2), "instalado_at": None, "updated_at": ahora, "solicitado_por": "Lenin Urbina"},
            {"maquina_id": 9, "visita_id": 514, "repuesto_nombre": "Piola", "cantidad": 1, "origen": "repuesto", "estado": "instalado",
             "created_at": ahora - dt.timedelta(hours=5), "instalado_at": ahora - dt.timedelta(hours=1), "updated_at": ahora - dt.timedelta(hours=1),
             "solicitado_por": "Jaizer"},
            {"maquina_id": 9, "visita_id": 515, "repuesto_nombre": "Cinta", "cantidad": 1, "origen": "repuesto", "estado": "pedido",
             "created_at": ahora - dt.timedelta(days=3), "instalado_at": None, "updated_at": ahora, "solicitado_por": "Jaizer"}]

        def fetchall(sql, params=()):
            s = " ".join(sql.split())
            if "FROM mant_maquina_paradas" in s or "FROM mant_ot_hallazgos h" in s:
                return []
            if "FROM mant_ot_repuesto_solicitudes" in s:
                return [dict(r) for r in solicitudes]
            if s.startswith("SELECT id, numero_ot FROM mant_visitas WHERE id IN"):
                return [{"id": i, "numero_ot": "OT-2026-%05d" % i} for i in params]
            raise AssertionError("consulta no prevista: " + s[:160])
        amb = cargar(["_maq_problemas"], extra={
            "datetime": dt.datetime, "mysql_fetchall": fetchall, "_hv_asegurar": lambda: None, "_hall_ok": lambda: True, "_oh": H,
            "_FS_MARCA_RIESGO": "⚠ PUEDE LASTIMAR A ALGUIEN", "_OTREP_ABIERTOS": ("solicitado", "validado", "pedido", "recibido"),
            "_OTREP_ESTADO_LABEL": {"pedido": "Pedido al proveedor", "instalado": "Instalado"},
            "chile_fmt_filter": lambda v, f="": v.strftime(f), "print": lambda *a, **k: None})
        textos = [p["estado_txt"] for p in amb["_maq_problemas"]([9])[9]]
        self.assertIn("Pedido al proveedor · hace menos de un día", textos)
        self.assertIn("En menos de un día", textos, "cerrado dentro del mismo día")
        self.assertIn("Pedido al proveedor · hace 3 días", textos, "lo demás no cambia")
        self.assertFalse([t for t in textos if "mismo día" in t], textos)


class TestFase1Resumen(unittest.TestCase):
    """«Estado de los equipos» (pestaña Trabajo): H-22 y los chips de H-12."""

    def _filas(self, equipos, hall=None):
        amb = cargar(["_ot_resumen_equipos"], extra={"_otrep_serie_producto": lambda vid, imid: "", "_oh": H})
        return amb["_ot_resumen_equipos"](340, equipos, hall)

    def test_h22_un_equipo_que_no_se_pudo_revisar_dice_por_que(self):
        eq = [{"id": 9, "nombre": "Banco Ajustable ILUS Bio", "serie": "S9", "razon_saltado": "sin_acceso", "n_ok": 0, "rep_solicitudes": []},
              {"id": 10, "nombre": "Prensa", "serie": "S10", "razon_saltado": "no_encontrado", "rep_solicitudes": []},
              {"id": 11, "nombre": "Remo", "serie": "S11", "razon_saltado": "falta_stock", "rep_solicitudes": []},
              {"id": 12, "nombre": "Cinta", "serie": "S12", "razon_saltado": "dado_de_baja", "rep_solicitudes": []}]
        chips = {f["id"]: f["chips"] for f in self._filas(eq)}
        self.assertEqual([(c["txt"], c["color"]) for c in chips[9]], [("No se pudo revisar: sin acceso", "gris")])
        self.assertEqual([(c["txt"], c["color"]) for c in chips[10]], [("No estaba en el recinto", "ambar")])
        self.assertEqual([(c["txt"], c["color"]) for c in chips[11]], [("No llegó · falta stock", "ambar")])
        self.assertNotIn(12, chips, "dado de baja ya tiene su propio aviso: este chip no se repite")

    def test_h22_sin_estados_contradictorios_en_un_equipo_con_algo_ya_respondido_o_reportado(self):
        # (a) con tareas ya respondidas en esa OT
        parcial = self._filas([{"id": 9, "nombre": "Banco", "serie": "S9", "razon_saltado": "sin_energia", "n_ok": 3, "rep_solicitudes": []}])
        self.assertEqual([c["txt"] for c in parcial[0]["chips"]], ["Revisión incompleta: sin energía"])
        # (b) con hallazgos reportados en el equipo (el caso real: «No se pudo revisar» junto a «2 hallazgos sin gestionar»)
        hall = {"grupos": [{"key": "e9", "serie": "S9", "peor": 4, "n_abiertos": 2, "n_tomados": 0, "n_con_ot": 0, "items": [1, 2]}]}
        f = self._filas([{"id": 9, "nombre": "Banco Ajustable ILUS Bio", "serie": "S9", "razon_saltado": "sin_acceso", "n_ok": 0, "rep_solicitudes": []}], hall)
        txt = [c["txt"] for c in f[0]["chips"]]
        self.assertEqual(txt, ["Revisión incompleta: sin acceso", "2 hallazgos sin gestionar"])
        self.assertFalse([t for t in txt if t.startswith("No se pudo revisar")], "nunca los dos a la vez")

    def test_h12_los_chips_de_con_ot_de_seguimiento_son_azules_en_curso_y_verdes_cerrada(self):
        eq = [{"id": 9, "nombre": "Banco", "serie": "S9", "rep_solicitudes": []}]
        mixto = self._filas(eq, {"grupos": [{"key": "e9", "serie": "S9", "peor": 1, "n_abiertos": 0, "n_tomados": 0, "n_con_ot": 3, "n_con_ot_cerrada": 1,
                                             "items": [1, 2, 3]}]})
        self.assertEqual([(c["txt"], c["color"]) for c in mixto[0]["chips"]],
                         [("2 con OT de seguimiento en curso", "azul"), ("1 con OT de seguimiento cerrada", "verde")])
        todas_cerradas = self._filas(eq, {"grupos": [{"key": "e9", "serie": "S9", "peor": 1, "n_abiertos": 0, "n_tomados": 0, "n_con_ot": 2,
                                                      "n_con_ot_cerrada": 2, "items": [1, 2]}]})
        self.assertEqual([(c["txt"], c["color"]) for c in todas_cerradas[0]["chips"]], [("2 con OT de seguimiento cerrada", "verde")])
        sin_dato = self._filas(eq, {"grupos": [{"key": "e9", "serie": "S9", "peor": 1, "n_abiertos": 0, "n_tomados": 0, "n_con_ot": 1, "items": [1]}]})
        self.assertEqual([(c["txt"], c["color"]) for c in sin_dato[0]["chips"]], [("1 con OT de seguimiento en curso", "azul")],
                         "sin saber si se cerró, es «en curso»: nunca verde por defecto")

    def test_h12_el_contexto_real_alimenta_los_chips(self):
        e = escena_bancos()
        c = e.ctx()
        equipos = [{"id": 12, "nombre": "Cinta de correr X", "serie": "SN-CC1", "rep_solicitudes": []},
                   {"id": 13, "nombre": "Remo Kairos", "serie": "SN-RK1", "rep_solicitudes": []}]
        filas = {f["id"]: [(x["txt"], x["color"]) for x in f["chips"]] for f in self._filas(equipos, c)}
        self.assertEqual(filas[12], [("1 con OT de seguimiento en curso", "azul")])
        self.assertEqual(filas[13], [("1 con OT de seguimiento cerrada", "verde")])


class TestFase1Bandeja(unittest.TestCase):
    """`ot_hallazgos_pagina` y la página «Hallazgos» renderizada: el MONITOR con el lenguaje de Retiros (Daniel 26-oct: «algo parecido a la
    página de Retiros, insisto» y «tablas amigables con filas informativas y una buena sensación de guía»). Conserva lo de la auditoría
    Impeccable: serie vigente una sola vez (H-15), controles de 44 px y foco (H-18), selección en negro y no en rojo (H-26), error sin jerga (H-28)."""

    ROW = {"id": 1, "visita_id": 340, "origen": "tarea", "referencia": "t1", "severidad": "falla", "titulo": "Banco: Falla", "detalle": "", "equipo_nombre": _BANCO,
           "maquina_id": 9, "item_manual_id": None, "riesgo": 0, "n_fotos": 0, "fotos_json": None, "estado": "abierto", "detectado_por": "Lenin Urbina",
           "detectado_en": dt.datetime(2026, 10, 21, 19, 51), "tomado_por": None, "tomado_en": None, "ot_seguimiento_id": None, "seguimiento_en": None,
           "descartado_por": None, "descartado_en": None, "retirado_en": None, "min_desde": 180, "numero_ot": "OT-2026-00340", "ot_estado": "programada",
           "cliente_id": 26, "cliente": "Juan Daniel Aguilar Tejera", "cliente_rut": "76123456-7", "seg_numero": None, "seg_estado": None,
           "serie": "255470655-0696-1", "sku": "1129100696"}

    def _trazabilidad(self, filas):
        out = {}
        for f in filas:
            ctx = {"estado_ef": f["estado"], "tomado_por": f.get("tomado_por") or "", "valor": "", "seg": None, "ticket": None, "sols": [], "stock": {}}
            out[f["id"]] = {"estado_ef": f["estado"], "reloj": HS.reloj({"estado_ef": f["estado"], "severidad": f["severidad"], "riesgo": f.get("riesgo"),
                                                                       "detectado_en": dt.datetime(2026, 10, 21, 16, 51)}, dt.datetime(2026, 10, 22, 15, 0)),
                            "recorrido": HS.recorrido(ctx), "siguiente": HS.siguiente(ctx), "valor": None, "ticket": None, "seg": None,
                            "consumo_ot": {"n": 3.0, "valor": 44970, "con_costo": True}, "consumo_seg": None}
        return out

    def _llamar(self, filas, args=None, vencidos=()):
        consultas = []

        def _ver(sql, params):
            s = " ".join(sql.split())
            self.assertEqual(s.count("%s"), len(tuple(params or ())), "cada %s lleva su valor: " + s[:120])
            consultas.append(s)
            return s

        def fetchone(sql, params=()):
            s = _ver(sql, params)
            if s.startswith("SELECT COUNT(*) AS n"):
                return {"n": len(filas)}
            if s.startswith("SELECT * FROM mant_visitas WHERE id=%s"):
                return {"id": 340, "numero_ot": "OT-2026-00340", "cliente_id": 26, "estado": "programada"}
            if s.startswith("SELECT razon_social FROM mant_clientes"):
                return {"razon_social": "Juan Daniel Aguilar Tejera"}
            raise AssertionError("consulta (uno) no prevista: " + s[:140])

        def fetchall(sql, params=()):
            s = _ver(sql, params)
            if s.startswith("SELECT h.id, h.visita_id, h.origen, h.referencia"):
                return [dict(r) for r in filas]
            if s.startswith("SELECT id, responsable_nombre FROM mant_visitas"):
                return [{"id": 340, "responsable_nombre": "Aarón Rojas"}]
            raise AssertionError("consulta (todos) no prevista: " + s[:140])
        visto = {}

        def render(plantilla, **kw):
            visto["plantilla"], visto["ctx"] = plantilla, kw
            return "html"
        amb = cargar(["ot_hallazgos_pagina", "_hall_cuando", "_hall_in"], constantes=("_HALL_PAG_FILTROS", "_HALL_F_A_VISTA"), extra={
            "_oh": H, "_hs": HS, "_gc": GC, "request": types.SimpleNamespace(args=dict(args or {})), "_hall_ok": lambda: True,
            "mysql_fetchall": fetchall, "mysql_fetchone": fetchone, "_puede_ot_accion": lambda *a, **k: True, "g": types.SimpleNamespace(user={}),
            "_hall_contexto_ficha": lambda vid, v: None, "_hall_n_sin_gestionar": lambda: 3, "_ot_resp_ok": lambda: True,
            "_hall_monitor_kpis": lambda: {"sin_gestionar": 3, "graves": 1, "tomados": 0, "con_ot": 0, "descartados": 0, "sin_foto": 0, "todos": 3},
            "_hall_vencidos_ids": lambda: set(vencidos), "_hall_trazabilidad": self._trazabilidad,
            "render_template": render, "chile_fmt_filter": lambda v, f="%d/%m/%Y %H:%M": v.strftime(f), "print": lambda *a, **k: None})
        amb["ot_hallazgos_pagina"]()
        self.assertEqual(visto["plantilla"], "ot2/hallazgos.html")
        visto["ctx"]["consultas"] = consultas
        return visto["ctx"]

    def test_h15_la_serie_sale_una_sola_vez_y_es_la_vigente(self):
        # El nombre guardado trae la serie de CUANDO se reportó; la fila muestra la vigente de la ficha, una sola vez.
        fila = dict(self.ROW, equipo_nombre="ILUS Kairos Row Machine (SKU 1121100518) · S/N 255470655-0518-1", serie="255470655-055555555tt518-1")
        ctx = self._llamar([fila])
        x = ctx["items"][0]
        self.assertEqual(x["equipo"], "ILUS Kairos Row Machine (SKU 1121100518)")
        self.assertEqual(x["serie"], "255470655-055555555tt518-1", "la vigente de la ficha, no la de cuando se reportó")
        self.assertNotIn("S/N", x["equipo"])
        self.assertEqual(x["reportado"], "21/10/2026 19:51", "H-24: con año")

    def test_cada_fila_trae_quien_que_ot_cliente_plazo_guia_y_consumo(self):
        ctx = self._llamar([self.ROW])
        x = ctx["items"][0]
        self.assertEqual((x["reporto"], x["numero_ot"], x["cliente"], x["cliente_rut"], x["responsable_ot"]),
                         ("Lenin Urbina", "OT-2026-00340", "Juan Daniel Aguilar Tejera", "76123456-7", "Aarón Rojas"))
        self.assertEqual(x["sev_texto"], "Falla")
        self.assertIn("Sin tomar", x["reloj"]["pill"])
        self.assertEqual(x["semaforo"], "rojo")
        self.assertEqual(x["siguiente"]["accion"], "tomar")
        self.assertEqual([p["clave"] for p in x["recorrido"]], ["atencion", "valor", "ot", "ticket", "abastecimiento", "bodega", "consumo"])
        self.assertEqual((x["consumo_ot_n"], x["consumo_ot_valor"]), ("3", "$44.970"), "el consumo de la OT (plata: solo gestión)")
        self.assertIn("Plazo vencido", ctx["vistas"].values())
        self.assertEqual(ctx["kpis"]["vencidos"], 0)

    def test_la_vista_vencidos_filtra_por_los_ids_calculados_con_el_reloj_de_horas_habiles(self):
        ctx = self._llamar([self.ROW], args={"vista": "vencidos"}, vencidos=(1, 7))
        self.assertEqual(ctx["kpis"]["vencidos"], 2)
        self.assertTrue(any("h.id IN (%s,%s)" in c for c in ctx["consultas"]))
        vacio = self._llamar([], args={"vista": "vencidos"}, vencidos=())
        self.assertTrue(any("1=0" in c for c in vacio["consultas"]), "sin vencidos no se trae nada (y no se rompe)")
        # En las demás vistas los vencidos suben primero (y los marcadores cuadran con sus valores: lo vigila _ver)
        ctx = self._llamar([self.ROW], vencidos=(1,))
        self.assertTrue(any("(h.id IN (%s)) DESC" in c for c in ctx["consultas"]))

    def test_filtros_de_busqueda_gravedad_y_tecnico_van_parametrizados(self):
        ctx = self._llamar([self.ROW], args={"q": "Aurum'; DROP TABLE x;--", "sev": "falla", "tec": "Lenin", "pp": "10", "p": "3", "vista": "todos"})
        c = [x for x in ctx["consultas"] if x.startswith("SELECT h.id")][0]
        self.assertNotIn("DROP", c, "lo que escribe la persona nunca entra al SQL: va como parámetro")
        self.assertIn("LIKE %s", c)
        self.assertEqual(ctx["por_pag"], 10)
        self.assertEqual(self._llamar([self.ROW], args={"sev": "inventada"})["sev"], "", "una gravedad que no existe se ignora")
        self.assertEqual(self._llamar([self.ROW], args={"vista": "hackeada"})["vista"], "sin_gestionar")
        self.assertEqual(self._llamar([self.ROW], args={"f": "todos"})["vista"], "todos", "los enlaces viejos (?f=) siguen funcionando")

    def _pagina(self, error=False, sel=340, items=None):
        e = escena_bancos()
        env = _entorno_pagina()
        fila = dict(self.ROW)
        its = items
        if its is None:
            ctx0 = self._llamar([fila, dict(fila, id=2, titulo="Otro: Alerta", severidad="alerta", visita_id=339, numero_ot="OT-2026-00339", cliente=None)])
            its = ctx0["items"]
        return env.get_template("ot2/hallazgos.html").render(
            items=its, total=len(its), pag=1, paginas=1, por_pag=20, desde=1, hasta=len(its), vista="sin_gestionar", vistas=H.VISTAS_MONITOR, q="", sev="", tec="",
            severidades=H.SEVERIDADES, kpis={"sin_gestionar": 2, "vencidos": 1, "graves": 1, "tomados": 0, "con_ot": 0, "descartados": 0}, sla=HS.config(),
            sel=sel, ot_sel={"id": 340, "numero_ot": "OT-2026-00340", "url": "/ot/340", "cliente": "Juan Daniel Aguilar Tejera", "estado": "programada"} if sel else None,
            hallazgos=e.ctx() if sel else None, hz_pagina=True, error=error, n_sin_gestionar=2)

    def test_h18_el_monitor_es_una_tabla_con_una_fila_por_hallazgo_y_su_guia(self):
        html = self._pagina()
        self.assertIn('id="hzTabla"', html)
        self.assertEqual(len(re.findall(r'<tr class="rm-row [^"]*hzm-row', html)), 2, "una fila por hallazgo")
        self.assertEqual(len(re.findall(r'<tr class="hzm-guia-row"', html)), 2, "y debajo, SIEMPRE a la vista, su guía de pasos (nada plegado)")
        self.assertEqual(html.count('<ol class="hzm-guia"'), 2)
        self.assertEqual(len(re.findall(r'<li class="hz-rec-p ', html.split('<ol class="hzm-guia"')[1].split("</ol>")[0])), 7, "los 7 pasos del recorrido")
        txt = _texto_visible(html)
        for t in ("Ahora toca", "Sin tomar", "Recorrido de falla", "Abastecimiento", "Bodega", "Consumo de la OT", "Lenin Urbina", "OT-2026-00340",
                  "Juan Daniel Aguilar Tejera", "Responsable de la OT: Aarón Rojas", "Plazo vencido"):
            self.assertIn(t, txt, t)
        self.assertNotIn("<details", html.split('<div id="hzpTarjeta">')[0], "Daniel 26-oct: tarjetas siempre abiertas")
        self.assertEqual(html.count('aria-current="true"'), 1, "solo el chip de la vista activa")

    def test_h15_la_fila_dice_el_equipo_y_su_serie_sin_cortarlos(self):
        html = self._pagina()
        self.assertIn("S/N 255470655-0696-1", _texto_visible(html))
        self.assertNotIn("text-overflow", html, "REGLA #15: nada de nombres cortados")
        self.assertNotIn("line-clamp", html)

    def test_h26_la_fila_elegida_se_marca_en_negro_no_en_rojo(self):
        html = self._pagina()
        css = html[html.index("<style>"):html.index("</style>")]
        elegida = _regla_css(css, ".hzm-row.is-sel")
        self.assertIn("#0a0a0a", elegida)
        self.assertNotRegex(elegida, r"#dc2626|#fca5a5|#ef4444|red", "el rojo es riesgo y error: ya no marca una selección")
        self.assertIn('hzm-row is-sel', html)

    def test_h28_el_error_dice_que_hacer_sin_jerga(self):
        html = self._pagina(error=True)
        txt = _texto_visible(html)
        self.assertIn("No pudimos cargar la lista completa de hallazgos. Recarga la página; si el aviso sigue, escribe a soporte (soportetec@sphs.cl).", txt)
        self.assertNotRegex(txt, r"(?i)registro del servidor|traceback|exception|stack trace")
        self.assertIn('role="alert"', html)
        self.assertNotIn("No pudimos cargar", self._pagina(error=False))

    def test_h18_los_controles_miden_44_px_y_el_celular_no_se_desborda(self):
        html = self._pagina()
        css = html[html.index("<style>"):html.index("</style>")]
        self.assertRegex(_regla_css(css, ".hzp-sel-cab > span > a"), r"min-height:44px", "el enlace «OT-…» medía 20 px")
        base = _leer("static/retiros_monitor.css")
        for sel in (".rm-btn", ".rm-select", ".rm-chip"):
            self.assertRegex(_regla_css(base, sel), r"min-height:44px", sel + " (los controles de Retiros)")
        self.assertIn("table-mobile-cards", html, "en el celular cada fila se ve como tarjeta")
        self.assertRegex(css, r"@media \(max-width:768px\)\{\.hzm-guia\{grid-template-columns:1fr 1fr\}")
        self.assertRegex(_regla_css(_leer("static/inquietudes.css"), ".rm-toolbar .rm-tools input.rm-select"), r"min-width")

    def test_solo_gestion_y_sin_plata_para_un_tecnico(self):
        self.assertIn("@_no_tecnico", decoradores("ot_hallazgos_pagina"))
        self.assertIn("@_mant_required", decoradores("ot_hallazgos_pagina"))
        for f in ("_hall_trazabilidad", "_hall_vencidos_ids"):
            self.assertNotIn("_send_ilus_email", _fuente_funcion(f), "calcular no escribe ni avisa a nadie")
            for malo in ("INSERT", "UPDATE", "DELETE"):
                self.assertNotIn(malo, _fuente_funcion(f), f + " es SOLO LECTURA")


def _entorno_pagina():
    def cargador(nombre):
        if nombre in ("ot2/hallazgos.html", "ot2/_hallazgos.html"):
            return _leer("templates/" + nombre)
        if nombre == "base.html":
            return "{% block title %}{% endblock %}{% block head_extra %}{% endblock %}<main>{% block content %}{% endblock %}</main>"
        return ""
    env = jinja2.Environment(loader=jinja2.FunctionLoader(cargador), extensions=["jinja2.ext.do"], undefined=jinja2.ChainableUndefined, autoescape=True)
    env.globals["url_for"] = lambda ep, **kw: ("/static/" + kw.get("filename", "")) if ep == "static" else "/" + ep
    return env


class TestFase1Tarjeta(unittest.TestCase):
    """La tarjeta `_hallazgos.html` renderizada de verdad."""
    CSS = _leer("static/ot_hallazgos.css")

    def _html(self, e=None, **kw):
        self.e = e or escena_bancos()
        return _tarjeta(self.e.ctx(), **kw)

    def test_h20_cada_equipo_es_un_details_abierto_solo_si_tiene_algo_grave_por_gestionar(self):
        html = self._html()
        abiertos = {k: bool(o) for k, o in re.findall(r'<details class="hz-grupo [^"]*" data-hz-grupo="(\w+)"( open)?>', html)}
        self.assertEqual(abiertos, {"e9": True, "e10": True, "e11": True, "e12": True, "e13": True}, "Daniel 26-oct: todo abierto")
        self.assertEqual(html.count('<summary class="hz-grupo-cab">'), 5)
        # el contador y el nombre COMPLETO (REGLA #15) están a la vista aunque el grupo esté plegado, dentro del <summary>
        resumen = re.search(r'data-hz-grupo="e9"[^>]*>\s*<summary class="hz-grupo-cab">(.*?)</summary>', html, re.S).group(1)
        txt = _texto_visible(resumen)
        self.assertIn("Banco Ajustable ILUS Bio", txt)
        self.assertIn("S/N 255470655-0696-1", txt)
        self.assertIn("2 hallazgos · 2 sin gestionar", txt)
        # nada se quitó: los hallazgos de un grupo plegado siguen en la página (con sus botones)
        self.assertIn('data-hz-tomar="%d"' % self.e.h1["id"], html)
        self.assertIn('id="hz%d"' % self.e.h3["id"], html)

    def test_h20_con_varios_equipos_hay_un_boton_para_abrir_o_cerrar_todos(self):
        html = self._html()
        self.assertIn('data-hz-plegar-todo aria-expanded="true"', html)
        self.assertIn("Cerrar todos los equipos", html, "todo nace abierto (Daniel 26-oct): el botón ofrece cerrarlos a mano")
        solo = self._html(escena_bancos(solo_banco=True))
        self.assertNotIn("data-hz-plegar-todo", solo, "con un solo equipo no hay nada que plegar")
        self.assertIn('data-hz-grupo="e9" open>', solo)
        self.assertIn("data-hz-todo", solo, "«Seleccionar todo» (REGLA #14) sigue donde estaba")

    def test_h12_la_tarjeta_pinta_azul_en_curso_y_verde_cerrada(self):
        html = self._html()
        txt = _texto_visible(html)
        self.assertIn("1 con OT de seguimiento en curso", txt)
        self.assertIn("1 con OT de seguimiento cerrada", txt)
        self.assertIn('<span class="hz-n azul" title="Su OT de seguimiento sigue abierta"><b>1</b> con OT de seguimiento en curso</span>', html)
        self.assertIn('<span class="hz-n verde"><b>1</b> con OT de seguimiento cerrada</span>', html)
        self.assertEqual(re.findall(r'<span class="hz-est (\w+)">OT de seguimiento (en curso|cerrada)</span>', html), [("azul", "en curso"), ("verde", "cerrada")])
        self.assertIn('class="hz-item c-rojo e-resuelto seg-curso"', html)
        self.assertIn('class="hz-item c-negro e-resuelto seg-cerrada"', html)
        self.assertEqual(len(re.findall(r'<div class="hz-seg">', html)), 1, "en curso: bloque azul")
        self.assertEqual(len(re.findall(r'<div class="hz-seg cerrada">', html)), 1, "cerrada: bloque verde")
        self.assertNotIn("Resuelto con", txt, "una OT cerrada no es «resuelto»: eso lo decide la fase 2")
        # el verde solo existe para lo cerrado
        self.assertIn("background:#dbeafe", _regla_css(self.CSS, ".hz-seg"))
        self.assertIn("background:#dcfce7", _regla_css(self.CSS, ".hz-seg.cerrada"))
        self.assertIn("background:#f5f9ff", _regla_css(self.CSS, ".hz-item.e-resuelto"))
        self.assertIn("background:#f0fdf4", _regla_css(self.CSS, ".hz-item.e-resuelto.seg-cerrada"))

    def test_h16_el_titulo_del_hallazgo_no_repite_el_equipo(self):
        html = self._html()
        titulos = re.findall(r'<h4 class="hz-tit">([^<]*)</h4>', html)
        self.assertIn("Tapiz y espuma: Alerta", titulos)
        self.assertIn("Mecanismo de ajuste de respaldo y asiento: Alerta", titulos)
        self.assertIn("Repuesto solicitado: Varilla guía o buje de guía × 5", titulos)
        for t in titulos:
            self.assertNotIn("S/N", t, "la serie va en el encabezado del grupo, no en cada hallazgo")
            self.assertNotIn("(SKU", t)
        # el nombre completo va UNA vez por grupo y el título completo sigue como pie de las fotos y del «Descartar»
        self.assertEqual(html.count("<b>Banco Ajustable ILUS Bio</b>"), 1)
        self.assertIn('data-hz-cap="%s · Tapiz y espuma: Alerta"' % _BANCO, html)
        self.assertIn('data-hz-titulo="%s · Tapiz y espuma: Alerta"' % _BANCO, html)
        self.assertNotIn("Evidencia: 1 foto(s)", html, "las miniaturas ya muestran la evidencia")
        self.assertIn("Se perdió o falta — Está raspada", html)

    def test_h18_cada_casilla_tiene_su_propio_nombre_para_el_lector_de_pantalla(self):
        html = self._html()
        nombres = re.findall(r'<input type="checkbox" data-hz-sel value="\d+"(?: disabled)? aria-label="([^"]+)"', html)
        self.assertEqual(len(nombres), 6)
        self.assertEqual(len(set(nombres)), 6, "ninguna se llama igual que otra")
        self.assertTrue(all(n.startswith("Elegir para la OT de seguimiento: ") for n in nombres))
        self.assertIn("Elegir para la OT de seguimiento: Banco Ajustable ILUS Bio · Tapiz y espuma: Alerta", nombres)

    def test_h18_la_casilla_se_ve_de_24_px_pero_responde_en_44(self):
        sel = _regla_css(self.CSS, ".hz-sel")
        self.assertRegex(sel, r"flex:0 0 28px")
        area = _regla_css(self.CSS, ".hz-sel::after")
        izq, der = (int(x) for x in re.search(r"left:-(\d+)px;right:-(\d+)px", area).groups())
        self.assertGreaterEqual(28 + izq + der, 44, "el área táctil mide al menos 44 px de ancho")
        self.assertIn("position:relative", sel)

    def test_h18_el_foco_es_visible_de_3_px_y_con_contraste(self):
        reglas = re.findall(r"([^{}]*:focus-visible[^{}]*)\{([^}]*)\}", self.CSS)
        self.assertGreaterEqual(len(reglas), 2)
        for selector, cuerpo in reglas:
            m = re.search(r"outline:\s*(\d+)px solid (#[0-9a-fA-F]{6})", cuerpo)
            self.assertTrue(m, selector)
            self.assertGreaterEqual(int(m.group(1)), 3, selector)
            for fondo in ("#ffffff", "#f9fafb", "#f5f9ff"):
                self.assertGreaterEqual(_contraste(m.group(2), fondo), 3.0, (selector, m.group(2), fondo))
        self.assertLess(_contraste("#fca5a5", "#ffffff"), 3.0, "el rosa de antes no llegaba al mínimo: por eso se cambió")
        self.assertNotIn("#fca5a5", re.sub(r"/\*.*?\*/", "", self.CSS, flags=re.S), "ya no queda el rosa en ninguna regla (los comentarios sí lo citan)")
        self.assertIn(".hz-grupo-cab:focus-visible", self.CSS, "el <summary> de cada equipo también se ve enfocado")

    def test_h27_cada_boton_de_accion_tiene_su_rotulo_aparte_para_decir_guardando(self):
        html = self._html()
        for texto in ("Tomar el caso", "Levantar OT de seguimiento", "Descartar"):
            self.assertIn('<span class="hz-btn-tx">%s</span>' % texto, html)
        self.assertIn('<span class="hz-btn-tx">Seleccionar todo</span>', html)
        self.assertIn('<span class="hz-btn-tx">Levantar una OT con lo elegido</span>', html)
        espera = _regla_css(self.CSS, '.hz-btn[aria-busy="true"]::before')
        self.assertIn("animation:hz-giro", espera.replace(" ", ""))
        self.assertIn("@keyframes hz-giro", self.CSS)
        self.assertRegex(self.CSS, r"prefers-reduced-motion:reduce\)\{[^@]*\.hz-btn\[aria-busy=\"true\"\]::before\{animation:none")

    def test_regla_4_2_no_se_quito_ninguna_funcion(self):
        """La tarjeta sigue ofreciendo exactamente lo mismo que antes (tomar, levantar, descartar, reabrir, elegir todo, levantar con lo elegido, fotos,
        retirados y el aviso de «viene de»), más lo nuevo."""
        tpl = _leer("templates/ot2/_hallazgos.html")
        for pieza in ("data-hz-tomar", "data-hz-levantar=", "data-hz-levantar-sel", "data-hz-descartar", "data-hz-reabrir", "data-hz-todo", "data-hz-sel",
                      "data-hz-foto", "data-hz-fotos", "hz-retirados", "hzOrigen", "hz-sinfoto", "hz-seg", "hz-aviso", "hz-desc", "Puede lastimar a alguien",
                      "Retirados por el técnico", "Esta orden es el seguimiento de"):
            self.assertIn(pieza, tpl, pieza)


class TestFase1EnlacesViejosYTextos(unittest.TestCase):
    DETALLE = _leer("templates/ot2/detalle.html")

    def _script_de_redireccion(self, html):
        m = re.search(r"<script>(\(function \(\) \{ try \{ if \(location\.hash === '#otdHallazgos'\).*?)</script>", html, re.S)
        return m.group(1) if m else None

    def test_h14_el_ancla_vieja_de_la_campana_lleva_a_la_pagina_de_hallazgos_de_esa_ot(self):
        ctx = escena_bancos().ctx()
        ficha = _entorno().get_template("ot2/_hallazgos.html").render(hallazgos=ctx, es_tecnico=False)          # la ficha de la OT: sin hz_pagina
        script = self._script_de_redireccion(ficha)
        self.assertIsNotNone(script)
        self.assertIn("/ot/hallazgos?ot=340#hzpTarjeta", script)
        # no se dibuja en la propia página de hallazgos (ahí el ancla existe), ni para un técnico (no puede entrar ahí), ni sin hallazgos
        self.assertIsNone(self._script_de_redireccion(_tarjeta(ctx)))
        self.assertEqual(_entorno().get_template("ot2/_hallazgos.html").render(hallazgos=ctx, es_tecnico=True).strip(), "")
        self.assertEqual(_entorno().get_template("ot2/_hallazgos.html").render(hallazgos=None, es_tecnico=False).strip(), "")

    @unittest.skipUnless(HAY_NODE, "Node no está instalado")
    def test_h14_el_script_redirige_solo_con_el_ancla_vieja(self):
        ficha = _entorno().get_template("ot2/_hallazgos.html").render(hallazgos=escena_bancos().ctx(), es_tecnico=False)
        script = self._script_de_redireccion(ficha)
        arnes = ("const vm=require('vm');const s=JSON.parse(process.argv[1]);const r={};"
                 "for(const h of ['#otdHallazgos','#otro','','#hz4']){const llamadas=[];const loc={hash:h,replace:u=>llamadas.push(u)};"
                 "vm.runInNewContext(s,{location:loc});r[h]=llamadas;}"
                 "const roto={get hash(){throw new Error('x')},replace:u=>{throw new Error('y')}};vm.runInNewContext(s,{location:roto});"
                 "console.log(JSON.stringify(r));")
        r = subprocess.run(["node", "-e", arnes, json.dumps(script)], capture_output=True, text=True, encoding="utf-8", timeout=60)
        self.assertEqual(r.returncode, 0, r.stderr[-800:])
        self.assertEqual(json.loads(r.stdout.strip().splitlines()[-1]),
                         {"#otdHallazgos": ["/ot/hallazgos?ot=340#hzpTarjeta"], "#otro": [], "": [], "#hz4": []})

    def test_h25_el_resumen_de_la_pestana_trabajo_se_llama_estado_de_los_equipos(self):
        self.assertIn('<h3 id="otdResT">Estado de los equipos</h3>', self.DETALLE)
        self.assertEqual(self.DETALLE.count('id="otdResumenEquipos"'), 1)
        self.assertIn("Estado de los equipos", _leer("static/ot_hallazgos.css"))
        self.assertIn("Estado de los equipos", _leer("templates/ot2/_hallazgos.html"))

    def test_h25_ninguna_pantalla_vuelve_a_decir_que_paso_con_los_equipos(self):
        """«¿Qué pasó?» ya es la franja de la REGLA #35 (y el título de los flujos del técnico): el resumen no puede llevar el mismo nombre."""
        for raiz, _carpetas, archivos in os.walk(os.path.join(RAIZ, "templates")):
            for nombre in archivos:
                if not nombre.endswith(".html"):
                    continue
                with open(os.path.join(raiz, nombre), encoding="utf-8", newline="") as fh:
                    t = fh.read()
                visible = re.sub(r"\{#.*?#\}|<!--.*?-->", "", t, flags=re.S)            # los comentarios que citan a Daniel no se ven
                self.assertNotIn("Qué pasó con los equipos", visible, os.path.join(raiz, nombre))

    def test_h25_la_ficha_sigue_dibujando_el_resumen_con_el_mismo_boton(self):
        i = self.DETALLE.index('id="otdResumenEquipos"')
        bloque = self.DETALLE[i:i + 1800]
        self.assertIn('href="/ot/hallazgos?ot={{ hallazgos.vid }}#hzpTarjeta"', bloque)
        self.assertIn("Gestionar hallazgos", bloque)

    def test_los_archivos_tocados_conservan_crlf_y_parsean(self):
        for rel in ("templates/ot2/_hallazgos.html", "templates/ot2/hallazgos.html", "templates/ot2/detalle.html", "static/ot_hallazgos.js",
                    "static/ot_hallazgos.css", "ot_hallazgos.py"):
            with open(os.path.join(RAIZ, rel.replace("/", os.sep)), "rb") as fh:
                crudo = fh.read()
            self.assertEqual(crudo.count(b"\n"), crudo.count(b"\r\n"), rel)
        for rel in ("templates/ot2/_hallazgos.html", "templates/ot2/hallazgos.html"):
            jinja2.Environment(extensions=["jinja2.ext.do"]).parse(_leer(rel))


_HARNESS_FASE1 = r"""
const vm = require('vm'), fs = require('fs');
const codigo = fs.readFileSync(process.argv[2], 'utf8');
const esperar = async () => { for (let i = 0; i < 12; i++) await new Promise(r => setImmediate(r)); };

function elemento(extra) {
  const attrs = {};
  return Object.assign({disabled: false, textContent: '', attrs,
    getAttribute(n) { return Object.prototype.hasOwnProperty.call(attrs, n) ? attrs[n] : null; },
    setAttribute(n, v) { attrs[n] = String(v); }, removeAttribute(n) { delete attrs[n]; }}, extra || {});
}
function boton(texto, attr, valor) {          // <button class="hz-btn" data-hz-X="v"><span class="hz-btn-tx">texto</span></button>
  const tx = elemento({textContent: texto});
  const b = elemento({tx, querySelector: (s) => s === '.hz-btn-tx' ? tx : null});
  b.attrs[attr] = String(valor);
  b.closest = (s) => s === '[' + attr + ']' ? b : null;
  return b;
}
function grupo(clave, abierto) { const g = elemento({open: abierto, matches: (s) => s === 'details[data-hz-grupo]'}); g.attrs['data-hz-grupo'] = clave; return g; }

function pagina(config) {
  const log = {fetches: [], toasts: [], reloads: 0, scrolls: [], replace: [], lightbox: []};
  const listeners = {}, memoria = Object.assign({}, config.storage || {}), pendientes = [];
  const botones = config.botones || [], grupos = config.grupos || [];
  const checks = (config.checks || []).map(c => Object.assign({checked: false, disabled: false}, c));
  const cuenta = {textContent: ''}, spanTodo = {textContent: ''};
  const document = {
    addEventListener: (t, f) => { listeners[t] = f; },
    querySelectorAll: (sel) => ({'.hz-btn': botones, 'details[data-hz-grupo]': grupos, '[data-hz-sel]:not(:disabled)': checks.filter(c => !c.disabled)})[sel] || [],
    querySelector: (sel) => ({'[data-hz-cuenta]': cuenta, '[data-hz-todo] span': spanTodo, '[data-hz-plegar-todo]': config.plegar || null})[sel] || null,
    getElementById: () => null, documentElement: {scrollTop: 0},
  };
  const ventana = {OTD_HZ_CFG: {vid: 171, numero: 'OT-2026-00171'}, pageYOffset: config.y || 0,
    scrollTo: (x, y) => log.scrolls.push([x, y]),
    ilusToast: (m, o) => log.toasts.push([m, (o || {}).type]),
    ilusLightbox: (imgs, idx, alt) => log.lightbox.push([imgs, idx, alt]),
    addEventListener: (t, f) => { ventana['_' + t] = f; }};
  if (!config.sinAlmacen) {
    ventana.sessionStorage = {getItem: (k) => Object.prototype.hasOwnProperty.call(memoria, k) ? memoria[k] : null,
      setItem: (k, v) => { memoria[k] = String(v); }, removeItem: (k) => { delete memoria[k]; }};
  }
  const location = {pathname: '/ot/hallazgos', search: '?f=sin_gestionar&ot=171', _h: null, reload: () => { log.reloads++; },
    get href() { return this._h; }, set href(v) { this._h = v; }};
  const ctx = {window: ventana, document, location, history: {replaceState: (a, b, u) => log.replace.push(u)}, setTimeout: (f) => f(),
    fetch: (url, init) => { log.fetches.push([url, init.method]);
      return new Promise((res) => { pendientes.push(() => res({status: 200, json: () => Promise.resolve(config.respuesta || {ok: true})})); }); },
    console, JSON, parseInt, String, Array, Promise};
  vm.createContext(ctx);
  vm.runInContext(codigo, ctx);
  return {log, listeners, ventana, memoria, location, checks,
    clic: (el) => listeners.click({target: el, preventDefault() {}}),
    responder: () => { while (pendientes.length) pendientes.shift()(); }};
}
const estado = (b) => ({texto: b.tx.textContent, disabled: b.disabled, busy: b.attrs['aria-busy'] || null});

(async () => {
  const r = {};
  { // tomar: el botón dice «Guardando…», todos quedan deshabilitados y, si el servidor lo rechaza, todo vuelve a su lugar
    const tomar = boton('Tomar el caso', 'data-hz-tomar', 5), desc = boton('Descartar', 'data-hz-descartar', 5);
    const p = pagina({botones: [tomar, desc], respuesta: {ok: false, error: 'Este caso ya lo tomó Aarón Rojas.'}});
    p.clic(tomar);
    r.mientras = {tomar: estado(tomar), desc: estado(desc), reloads: p.log.reloads, doble: (p.clic(tomar), p.log.fetches.length)};
    p.responder(); await esperar();
    r.rechazado = {tomar: estado(tomar), desc: estado(desc), toasts: p.log.toasts, reloads: p.log.reloads, vista: p.memoria['ilus_hz_vista'] || null};
  }
  { // tomar con éxito: sigue bloqueado hasta la recarga y la vista (scroll y equipos abiertos) queda guardada
    const tomar = boton('Tomar el caso', 'data-hz-tomar', 5), desc = boton('Descartar', 'data-hz-descartar', 5);
    const p = pagina({botones: [tomar, desc], grupos: [grupo('e9', true), grupo('e10', false)], y: 640});
    p.clic(tomar); p.responder(); await esperar();
    r.exito = {tomar: estado(tomar), desc: estado(desc), reloads: p.log.reloads, toasts: p.log.toasts, replace: p.log.replace,
               vista: JSON.parse(p.memoria['ilus_hz_vista'] || 'null')};
  }
  { // el servidor no responde bien (caída): mensaje con qué hacer
    const tomar = boton('Tomar el caso', 'data-hz-tomar', 5);
    const p = pagina({botones: [tomar], respuesta: {}});
    p.clic(tomar); p.responder(); await esperar();
    r.sin_motivo = {toasts: p.log.toasts, tomar: estado(tomar)};
  }
  { // al volver a cargar, la persona queda donde estaba
    const g1 = grupo('e9', false), g2 = grupo('e10', true), g3 = grupo('e11', true);
    const guardada = JSON.stringify({y: 640, grupos: {e9: true, e10: false}, t: Date.now(), ruta: '/ot/hallazgos?f=sin_gestionar&ot=171'});
    const p = pagina({grupos: [g1, g2, g3], storage: {ilus_hz_vista: guardada}});
    r.restaurada = {abiertos: [g1.open, g2.open, g3.open], scrolls: p.log.scrolls, sigueGuardada: 'ilus_hz_vista' in p.memoria};
    const h1 = grupo('e9', false);
    const vieja = JSON.stringify({y: 640, grupos: {e9: true}, t: Date.now() - 300000, ruta: '/ot/hallazgos?f=sin_gestionar&ot=171'});
    const p2 = pagina({grupos: [h1], storage: {ilus_hz_vista: vieja}});
    r.vieja = {abierto: h1.open, scrolls: p2.log.scrolls};
    const h2 = grupo('e9', false);
    const otra = JSON.stringify({y: 640, grupos: {e9: true}, t: Date.now(), ruta: '/ot/hallazgos?f=todos'});
    const p3 = pagina({grupos: [h2], storage: {ilus_hz_vista: otra}});
    r.otra_pagina = {abierto: h2.open, scrolls: p3.log.scrolls};
    const p4 = pagina({grupos: [grupo('e9', true)], storage: {ilus_hz_vista: 'no es json'}});
    r.basura = {scrolls: p4.log.scrolls};
  }
  { // sin sessionStorage (navegación privada): todo sigue funcionando
    const tomar = boton('Tomar el caso', 'data-hz-tomar', 5);
    const p = pagina({botones: [tomar], grupos: [grupo('e9', true)], sinAlmacen: true, y: 300});
    p.clic(tomar); p.responder(); await esperar();
    r.sin_almacen = {reloads: p.log.reloads, toasts: p.log.toasts, scrolls: p.log.scrolls};
  }
  { // descartar y reabrir también muestran «Guardando…»
    const desc = boton('Descartar', 'data-hz-descartar', 6), tom = boton('Tomar el caso', 'data-hz-tomar', 6);
    const p = pagina({botones: [desc, tom]});
    p.ventana.ilusPrompt = () => Promise.resolve('El cliente ya lo resolvió');
    p.clic(desc); await esperar();
    r.descartando = {desc: estado(desc), tom: estado(tom), fetches: p.log.fetches};
    p.responder(); await esperar();
    const reab = boton('Reabrir', 'data-hz-reabrir', 8);
    const q = pagina({botones: [reab]});
    q.ventana.ilusConfirm = () => Promise.resolve(true);
    q.clic(reab); await esperar();
    r.reabriendo = {reab: estado(reab), fetches: q.log.fetches};
  }
  { // abrir o cerrar todos los equipos
    const g1 = grupo('e9', true), g2 = grupo('e10', false);
    const plegar = boton('Abrir todos los equipos', 'data-hz-plegar-todo', '');
    const p = pagina({grupos: [g1, g2], plegar});
    r.plegar_inicial = {texto: plegar.tx.textContent, expandido: plegar.attrs['aria-expanded']};
    p.clic(plegar);
    r.plegar_abre = {abiertos: [g1.open, g2.open], texto: plegar.tx.textContent, expandido: plegar.attrs['aria-expanded']};
    p.clic(plegar);
    r.plegar_cierra = {abiertos: [g1.open, g2.open], texto: plegar.tx.textContent, expandido: plegar.attrs['aria-expanded']};
    g1.open = true; g2.open = true; p.listeners.toggle({target: g1});         // la persona los abrió a mano uno por uno
    r.plegar_a_mano = {texto: plegar.tx.textContent, expandido: plegar.attrs['aria-expanded']};
    g2.open = false; p.listeners.toggle({target: g2});
    r.plegar_uno_cerrado = plegar.tx.textContent;
  }
  { // «Seleccionar todo» abre los equipos plegados (para que se vea qué quedó elegido) y al quitar la selección no los vuelve a cerrar
    const g1 = grupo('e9', false), g2 = grupo('e10', false);
    const p = pagina({grupos: [g1, g2], checks: [{value: '1'}, {value: '2'}]});
    const todo = boton('Seleccionar todo', 'data-hz-todo', '');
    p.clic(todo);
    r.todo_abre = {abiertos: [g1.open, g2.open], checks: p.checks.map(c => c.checked)};
    p.clic(todo);
    r.todo_quita = {abiertos: [g1.open, g2.open], checks: p.checks.map(c => c.checked)};
  }
  { // levantar: «Abriendo…» y, si la persona vuelve con «Atrás», los botones no quedan trabados
    const lev = boton('Levantar OT de seguimiento', 'data-hz-levantar', 7), tom = boton('Tomar el caso', 'data-hz-tomar', 7);
    const p = pagina({botones: [lev, tom]});
    p.clic(lev);
    r.levantar = {href: p.location.href, lev: estado(lev), tom: estado(tom)};
    p.ventana._pageshow({persisted: false});
    r.pageshow_normal = {lev: estado(lev), tom: estado(tom)};
    p.ventana._pageshow({persisted: true});
    r.volvio = {lev: estado(lev), tom: estado(tom)};
  }
  { // el pie de las fotos es el título COMPLETO (con el equipo), no el corto de la pantalla
    const cont = {getAttribute: () => '["/f/a.jpg"]'};
    const item = {getAttribute: (n) => n === 'data-hz-cap' ? 'Banco Ajustable ILUS Bio (SKU 1129100696) · S/N 255470655-0696-1 · Tapiz y espuma: Alerta' : null,
                  querySelector: () => ({textContent: 'Tapiz y espuma: Alerta'})};
    const btn = {getAttribute: (n) => n === 'data-hz-foto' ? '0' : null, closest: (s) => s === '[data-hz-fotos]' ? cont : (s === '.hz-item' ? item : null)};
    const p = pagina({});
    p.listeners.click({target: {closest: (s) => s === '[data-hz-foto]' ? btn : null}, preventDefault() {}});
    r.pie = p.log.lightbox;
  }
  console.log(JSON.stringify(r));
})().catch(e => { console.error(e); process.exit(1); });
"""


@unittest.skipUnless(HAY_NODE, "Node no está instalado")
class TestFase1Js(unittest.TestCase):
    """El JS de la tarjeta con un DOM de mentira: estado de carga, vista que se conserva, equipos plegables."""

    @classmethod
    def setUpClass(cls):
        with tempfile.TemporaryDirectory() as d:
            ruta = os.path.join(d, "arnes_fase1.js")
            with open(ruta, "w", encoding="utf-8") as fh:
                fh.write(_HARNESS_FASE1)
            r = subprocess.run(["node", ruta, os.path.join(RAIZ, "static", "ot_hallazgos.js")], capture_output=True, text=True, encoding="utf-8", timeout=90)
        if r.returncode != 0:
            raise AssertionError("node falló:\n" + r.stderr[-2500:])
        cls.R = json.loads(r.stdout.strip().splitlines()[-1])

    def test_h27_mientras_guarda_el_boton_lo_dice_y_todos_quedan_deshabilitados(self):
        m = self.R["mientras"]
        self.assertEqual(m["tomar"], {"texto": "Guardando…", "disabled": True, "busy": "true"})
        self.assertEqual(m["desc"]["disabled"], True, "el otro botón también queda deshabilitado")
        self.assertEqual(m["desc"]["texto"], "Descartar", "pero su rótulo no cambia")
        self.assertEqual(m["reloads"], 0)
        self.assertEqual(m["doble"], 1, "un segundo toque no manda otra petición")

    def test_h27_si_el_servidor_lo_rechaza_todo_vuelve_a_su_lugar_y_se_dice_por_que(self):
        x = self.R["rechazado"]
        self.assertEqual(x["tomar"], {"texto": "Tomar el caso", "disabled": False, "busy": None})
        self.assertEqual(x["desc"], {"texto": "Descartar", "disabled": False, "busy": None})
        self.assertEqual(x["toasts"], [["Este caso ya lo tomó Aarón Rojas.", "error"]])
        self.assertEqual(x["reloads"], 0)
        self.assertIsNone(x["vista"], "si no recarga no guarda nada")

    def test_h28_si_el_servidor_no_da_motivo_el_mensaje_dice_que_hacer(self):
        t = self.R["sin_motivo"]["toasts"]
        self.assertEqual(len(t), 1)
        self.assertEqual(t[0][1], "error")
        self.assertIn("Recarga la página", t[0][0])
        self.assertIn("avisa a soporte", t[0][0])
        self.assertEqual(self.R["sin_motivo"]["tomar"]["disabled"], False)

    def test_h27_con_exito_sigue_bloqueado_hasta_la_recarga_y_guarda_donde_estaba_la_persona(self):
        x = self.R["exito"]
        self.assertEqual(x["tomar"]["texto"], "Guardando…")
        self.assertTrue(x["tomar"]["disabled"] and x["desc"]["disabled"])
        self.assertEqual(x["reloads"], 1)
        self.assertEqual(x["toasts"], [["✓ Tomaste el caso. Queda a tu nombre.", "success"]])
        self.assertEqual(x["replace"], ["/ot/hallazgos?f=sin_gestionar&ot=171#otdHallazgos"])
        v = x["vista"]
        self.assertEqual((v["y"], v["grupos"], v["ruta"]), (640, {"e9": True, "e10": False}, "/ot/hallazgos?f=sin_gestionar&ot=171"))
        self.assertIsInstance(v["t"], (int, float))

    def test_h27_al_recargar_vuelve_al_mismo_scroll_con_los_mismos_equipos_abiertos(self):
        r = self.R["restaurada"]
        self.assertEqual(r["abiertos"], [True, True, True], "Daniel 26-oct: recargar nunca vuelve a plegar un equipo (solo abre)")
        self.assertEqual(r["scrolls"], [[0, 640]])
        self.assertFalse(r["sigueGuardada"], "se usa una sola vez")
        self.assertEqual(self.R["vieja"], {"abierto": False, "scrolls": []}, "una vista de hace más de 2 minutos no se usa")
        self.assertEqual(self.R["otra_pagina"], {"abierto": False, "scrolls": []}, "ni la de otra página")
        self.assertEqual(self.R["basura"], {"scrolls": []}, "ni algo que no es JSON")

    def test_h27_sin_almacenamiento_del_navegador_todo_sigue_funcionando(self):
        x = self.R["sin_almacen"]
        self.assertEqual(x["reloads"], 1)
        self.assertEqual(x["toasts"][0][1], "success")
        self.assertEqual(x["scrolls"], [])

    def test_h27_descartar_y_reabrir_tambien_dicen_guardando(self):
        self.assertEqual(self.R["descartando"]["desc"], {"texto": "Guardando…", "disabled": True, "busy": "true"})
        self.assertTrue(self.R["descartando"]["tom"]["disabled"])
        self.assertEqual(self.R["descartando"]["fetches"], [["/ot/api/171/hallazgos/6/descartar", "POST"]])
        self.assertEqual(self.R["reabriendo"]["reab"], {"texto": "Guardando…", "disabled": True, "busy": "true"})
        self.assertEqual(self.R["reabriendo"]["fetches"], [["/ot/api/171/hallazgos/8/reabrir", "POST"]])

    def test_h20_un_boton_abre_o_cierra_todos_los_equipos_y_su_rotulo_sigue_a_cada_uno(self):
        self.assertEqual(self.R["plegar_inicial"], {"texto": "Abrir todos los equipos", "expandido": "false"})
        self.assertEqual(self.R["plegar_abre"], {"abiertos": [True, True], "texto": "Cerrar todos los equipos", "expandido": "true"})
        self.assertEqual(self.R["plegar_cierra"], {"abiertos": [False, False], "texto": "Abrir todos los equipos", "expandido": "false"})
        self.assertEqual(self.R["plegar_a_mano"], {"texto": "Cerrar todos los equipos", "expandido": "true"})
        self.assertEqual(self.R["plegar_uno_cerrado"], "Abrir todos los equipos")

    def test_h20_seleccionar_todo_abre_los_equipos_y_quitar_la_seleccion_no_los_cierra(self):
        self.assertEqual(self.R["todo_abre"], {"abiertos": [True, True], "checks": [True, True]})
        self.assertEqual(self.R["todo_quita"], {"abiertos": [True, True], "checks": [False, False]})

    def test_h27_levantar_dice_abriendo_y_si_la_persona_vuelve_con_atras_no_queda_trabado(self):
        self.assertEqual(self.R["levantar"]["href"], "/ot/?crear=1&seguimiento=7&desde=171")
        self.assertEqual(self.R["levantar"]["lev"], {"texto": "Abriendo…", "disabled": True, "busy": "true"})
        self.assertTrue(self.R["levantar"]["tom"]["disabled"])
        self.assertEqual(self.R["pageshow_normal"]["lev"]["texto"], "Abriendo…", "una carga normal no toca nada")
        self.assertEqual(self.R["volvio"]["lev"], {"texto": "Levantar OT de seguimiento", "disabled": False, "busy": None})
        self.assertFalse(self.R["volvio"]["tom"]["disabled"])

    def test_h16_el_pie_de_la_foto_es_el_titulo_completo_con_el_equipo(self):
        (imgs, idx, alt), = self.R["pie"]
        self.assertEqual(imgs, [{"url": "/f/a.jpg", "caption": _BANCO + " · Tapiz y espuma: Alerta"}])
        self.assertEqual((idx, alt), (0, "Hallazgo"))

    def test_el_js_sigue_sin_popups_nativos_y_con_sintaxis_valida(self):
        ruta = os.path.join(RAIZ, "static", "ot_hallazgos.js")
        self.assertEqual(subprocess.run(["node", "--check", ruta], capture_output=True, text=True, timeout=60).returncode, 0)
        self.assertIsNone(re.search(r"(?<![\w.])(alert|confirm|prompt)\(", _leer("static/ot_hallazgos.js")), "REGLA #1")


# FIN_DEL_ARCHIVO
