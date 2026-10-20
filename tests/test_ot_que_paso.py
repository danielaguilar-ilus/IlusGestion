# -*- coding: utf-8 -*-
"""🧭 2026-10-20 — «¿QUÉ PASÓ?» de la OT y la OT hija «pendiente por falta de stock» (Daniel, caso real Aurum Fit Girls Spa, OT-2026-00171:
2 accesorios no llegaron por falta de stock y la OT no se podía completar: «así como planteamos en Retiros… ¿pasó algo?»).

Decisiones de Daniel que se vigilan:
  · Falta de stock → se cierra lo hecho y se crea una OT HIJA «pendiente», unida a la madre, con la MISMA factura (el documento queda
    ESPEJADO: no consume saldo, no entra al libro; REGLAS #25 y #28), SIN fecha hasta que llegue el stock y solo con las tareas que
    faltaron (lo que la persona eligió: REGLA #32).
  · Lo usa el técnico asignado (también el externo) y la gestión; motivo de una lista («Otro» con detalle); queda quién, cuándo y por qué.
  · Avisos: gestión por la campana y bodega por correo INTERNO; al cliente nunca (REGLA #22).
  · Un técnico nunca ve plata, documentos ni saldo (REGLAS #19 y #26). Una OT terminada es de solo lectura.

Se prueba:
  1. El módulo puro ot_que_paso.py (catálogo, validación, equipos candidatos, «sin fecha», textos sin plata).
  2. La transacción `_qp_aplicar` de app.py con una base en memoria: todo o nada, tareas sin hacer, documento espejado, sin cobro propio.
  3. Las rutas con dobles: OT terminada → 409, bodega solo si se pidió, el técnico no recibe enlaces que no puede abrir.
  4. La vía 'pendiente_madre' de la puerta del documento (REGLA #24).
  5. Cableado: ENUM de la campana, arranque barato (REGLA #18), candados, nada de nombres financieros (REGLA #26), eventos y panel.
  6. Front: sin popups nativos, botones de 44 px, «marcar todo», nombres completos, respeta prefers-reduced-motion.

Sin BD, sin Flask, sin ERP (REGLA #4.1), sin correos reales (REGLA #22).
Correr:  python -m pytest tests/test_ot_que_paso.py -q -p no:warnings
"""
import datetime as dt
import json
import os
import re
import subprocess
import sys
import types
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import ot_eventos as ev
import ot_que_paso as qp

from tests.test_fac_libro_saldo import _exec
from tests.test_ot_revision_v3 import HAY_NODE
from tests.test_ot_saldo_servicio import _decoradores, _fuente, _leer

APP = _leer("app.py")
JS = _leer("static/ot_que_paso.js")
CSS = _leer("static/ot_que_paso.css")
PARCIAL = _leer("templates/ot2/_que_paso.html")
DETALLE = _leer("templates/ot2/detalle.html")
_PLATA = re.compile(r"proveedor|costo|precio|margen|\$\s?\d|valorizado|factura|saldo|zz_|n[°º] de oc", re.I)


def _cand(mid, pend=2, total=3, **kw):
    f = {"maquina_id": mid, "nombre": "Equipo %d" % mid, "sku": "SKU%d" % mid, "serie": "S%d" % mid, "total": total, "pendientes": pend,
         "estado_pe": None, "baja": False, "fuera_servicio": False, "revision": None, "razon_saltado": None, "en_pendiente": None}
    f.update(kw)
    return f


# ═══════════════════════════════ 1) el módulo puro ═══════════════════════════════
class TestCatalogo(unittest.TestCase):
    def test_tipos_motivos_y_acciones_de_daniel(self):
        self.assertEqual(set(qp.TIPOS), {"falta_stock", "no_se_pudo"})
        textos = " | ".join(x for lista in qp.MOTIVOS.values() for _c, x in lista).lower()
        for pieza in ("falta de stock", "falta un repuesto", "cliente no estaba", "dañado", "tiempo", "clima", "otro motivo"):
            self.assertIn(pieza, textos, pieza)
        self.assertEqual(qp.ACCIONES_POR_TIPO["falta_stock"], ("cerrar_y_pendiente", "solo_registrar"))
        self.assertTrue(qp.ACCIONES["cerrar_y_pendiente"]["crea_pendiente"])
        self.assertFalse(qp.ACCIONES["solo_registrar"]["crea_pendiente"])
        for t, lista in qp.MOTIVOS.items():
            self.assertEqual(lista[-1][0], "otro", t)       # «Otro motivo» siempre al final

    def test_catalogo_para_la_pantalla_no_trae_plata(self):
        c = json.dumps(qp.catalogo(), ensure_ascii=False)
        self.assertIsNone(_PLATA.search(c), c)
        self.assertEqual([t["clave"] for t in qp.catalogo()["tipos"]], ["falta_stock", "no_se_pudo"])


class TestValidar(unittest.TestCase):
    CANDS = qp.candidatos_equipos([_cand(1), _cand(2), _cand(3, pend=0)])

    def _ok(self, **kw):
        d = {"tipo": "falta_stock", "motivo": "falta_stock", "accion": "cerrar_y_pendiente", "equipos": [1, 2]}
        d.update(kw)
        return qp.validar(d, "en_curso", self.CANDS)

    def test_camino_feliz_del_caso_aurum(self):
        n, err = self._ok()
        self.assertIsNone(err)
        self.assertEqual((n["tipo"], n["accion"], n["equipos"], n["crea_pendiente"]), ("falta_stock", "cerrar_y_pendiente", [1, 2], True))
        self.assertEqual(n["motivo_texto"], "Falta de stock: el producto no llegó a terreno")

    def test_motivo_de_la_lista_y_otro_exige_detalle(self):
        self.assertEqual(self._ok(motivo="inventado")[1], "Elige el motivo de la lista.")
        self.assertEqual(self._ok(motivo="")[1], "Elige el motivo de la lista.")
        self.assertIn("detalle", self._ok(motivo="otro")[1])
        self.assertIn("detalle", self._ok(motivo="otro", detalle="  ab ")[1])
        self.assertIsNone(self._ok(motivo="otro", detalle="El proveedor cerró la bodega")[1])
        self.assertEqual(self._ok(motivo="cliente_no_estaba")[1], "Elige el motivo de la lista.")      # es de OTRO tipo

    def test_el_tipo_y_la_accion_deben_ser_de_la_lista(self):
        self.assertEqual(self._ok(tipo="")[1], "Elige qué pasó.")
        self.assertEqual(self._ok(tipo="inventado")[1], "Elige qué pasó.")
        self.assertEqual(self._ok(accion="reagendar")[1], "Elige qué hacer con la orden.")           # reagendar es de «no_se_pudo»
        self.assertEqual(self._ok(accion="")[1], "Elige qué hacer con la orden.")

    def test_cerrar_y_pendiente_exige_equipos_validos(self):
        self.assertIn("al menos un equipo", self._ok(equipos=[])[1])
        self.assertIn("al menos un equipo", self._ok(equipos="1")[1])
        self.assertIn("ya no se puede", self._ok(equipos=[1, 99])[1])           # no es de la OT
        self.assertIn("ya no se puede", self._ok(equipos=[3])[1])               # ya terminó sus tareas
        self.assertIn("no es válido", self._ok(equipos=["x"])[1])
        n, _e = self._ok(equipos=[2, 2, "1"])
        self.assertEqual(n["equipos"], [2, 1])                                   # sin repetidos

    def test_sin_cliente_no_hay_equipos_que_dejar_pendientes(self):
        n, err = qp.validar({"tipo": "falta_stock", "motivo": "falta_stock", "accion": "cerrar_y_pendiente", "equipos": [1]},
                            "en_curso", self.CANDS, tiene_cliente=False)
        self.assertIn("no tiene cliente", err)

    def test_solo_registrar_y_reagendar_no_crean_pendiente(self):
        n, err = qp.validar({"tipo": "no_se_pudo", "motivo": "clima", "accion": "reagendar", "equipos": [1], "avisar_bodega": True},
                            "en_curso", self.CANDS)
        self.assertIsNone(err)
        self.assertEqual((n["crea_pendiente"], n["equipos"], n["avisar_bodega"]), (False, [], False))
        n, _e = qp.validar({"tipo": "falta_stock", "motivo": "falta_stock", "accion": "solo_registrar"}, "en_curso", self.CANDS)
        self.assertFalse(n["crea_pendiente"])

    def test_bodega_solo_si_se_deja_pendiente_y_se_pidio(self):
        self.assertTrue(self._ok(avisar_bodega=True)[0]["avisar_bodega"])
        self.assertFalse(self._ok()[0]["avisar_bodega"])

    def test_una_orden_terminada_queda_en_solo_lectura(self):
        for estado in ("cerrada", "completada", "cancelada", "anulada", "pendiente_aprobacion"):
            self.assertIn("solo lectura", qp.validar({"tipo": "falta_stock"}, estado, self.CANDS)[1], estado)
        self.assertIn("solo lectura", qp.validar({"tipo": "falta_stock"}, "en_curso", self.CANDS, firma_cliente="firma.png")[1])
        self.assertIsNone(self._ok()[1])

    def test_el_detalle_se_limpia_y_se_acota(self):
        n, _e = self._ok(detalle="  mucho   espacio\n\n y salto  " + "x" * 2000)
        self.assertLessEqual(len(n["detalle"]), qp.DETALLE_MAX)
        self.assertNotIn("  ", n["detalle"])


class TestCandidatos(unittest.TestCase):
    def test_se_ofrecen_los_que_tienen_tareas_sin_hacer_y_se_sugieren_los_falta_stock(self):
        c = qp.candidatos_equipos([_cand(1, estado_pe="falta_stock"), _cand(2), _cand(3, pend=0, total=3)])
        por = {x["maquina_id"]: x for x in c}
        self.assertTrue(por[1]["preseleccionado"] and por[1]["elegible"])      # el sistema SUGIERE (REGLA #32)…
        self.assertFalse(por[2]["preseleccionado"])                             # …y la persona DECIDE
        self.assertFalse(por[3]["elegible"])
        self.assertIn("nada pendiente", por[3]["motivo_no"])

    def test_baja_fuera_de_servicio_y_falla_no_se_ofrecen(self):
        c = qp.candidatos_equipos([_cand(1, baja=True), _cand(2, fuera_servicio=True), _cand(3, revision="falla_detectada"),
                                   _cand(4, razon_saltado="dado_de_baja"), _cand(5, razon_saltado="fuera_de_servicio"), _cand(6)])
        self.assertEqual([x["maquina_id"] for x in c], [6])

    def test_uno_que_ya_esta_en_una_ot_pendiente_se_muestra_pero_no_se_elige(self):
        c = qp.candidatos_equipos([_cand(1, en_pendiente="OT-2026-00180", estado_pe="falta_stock")])
        self.assertFalse(c[0]["elegible"])
        self.assertFalse(c[0]["preseleccionado"])
        self.assertIn("OT-2026-00180", c[0]["motivo_no"])

    def test_filas_raras_no_rompen(self):
        self.assertEqual(qp.candidatos_equipos(None), [])
        self.assertEqual(qp.candidatos_equipos([{"maquina_id": "x"}, {}]), [])
        c = qp.candidatos_equipos([{"maquina_id": 5, "pendientes": None, "total": None}])
        self.assertFalse(c[0]["elegible"])


class TestSinFecha(unittest.TestCase):
    def test_la_fecha_centinela_es_sin_fecha(self):
        self.assertTrue(qp.es_sin_fecha(qp.FECHA_SIN_FECHA))
        self.assertTrue(qp.es_sin_fecha("2099-12-31"))
        self.assertTrue(qp.es_sin_fecha(dt.datetime(2099, 12, 31, 8, 0)))
        self.assertFalse(qp.es_sin_fecha(dt.date(2026, 10, 27)))
        self.assertFalse(qp.es_sin_fecha(None))
        self.assertFalse(qp.es_sin_fecha("no es fecha"))

    def test_reagendar_la_saca_de_sin_fecha_sola(self):
        self.assertFalse(qp.es_sin_fecha(dt.date(2026, 11, 3)))       # no hay columna que sincronizar

    def test_se_reutiliza_solo_la_hija_sin_empezar_y_sin_fecha(self):
        h1 = {"id": 5, "estado": "programada", "fecha_programada": qp.FECHA_SIN_FECHA}
        self.assertEqual(qp.hija_reutilizable([h1])["id"], 5)
        self.assertIsNone(qp.hija_reutilizable([dict(h1, estado="cancelada")]))
        self.assertIsNone(qp.hija_reutilizable([dict(h1, estado="en_curso")]))
        self.assertIsNone(qp.hija_reutilizable([dict(h1, fecha_programada=dt.date(2026, 11, 3))]))      # ya reagendada
        self.assertIsNone(qp.hija_reutilizable([dict(h1, hora_real_inicio=dt.datetime(2026, 11, 3))]))   # ya empezada
        self.assertIsNone(qp.hija_reutilizable([]))

    def test_es_pendiente_por_la_relacion(self):
        self.assertTrue(qp.es_pendiente({"ot_madre_id": 3, "ot_relacion": "pendiente_stock"}))
        self.assertFalse(qp.es_pendiente({"ot_madre_id": 3, "ot_relacion": "desinstalacion"}))
        self.assertFalse(qp.es_pendiente({"ot_madre_id": None, "ot_relacion": "pendiente_stock"}))


class TestTextos(unittest.TestCase):
    EQ = [{"nombre": "Pulsera cardio", "sku": "ABC1", "serie": "55"}, {"nombre": "Accesorio", "sku": "XYZ"}]

    def test_bitacora_dice_quien_cuando_motivo_equipos_y_ot_pendiente(self):
        n, _e = qp.validar({"tipo": "falta_stock", "motivo": "falta_stock", "accion": "cerrar_y_pendiente", "equipos": [1], "detalle": "llega el 5"},
                           "en_curso", qp.candidatos_equipos([_cand(1)]))
        t = qp.texto_bitacora(n, "Isabel Milling", self.EQ, "OT-2026-00180")
        for pieza in ("Faltan productos por falta de stock", "Falta de stock", "Detalle: llega el 5", "Pulsera cardio (SKU ABC1) · S/N 55",
                      "OT pendiente: OT-2026-00180", "Decidió: Isabel Milling"):
            self.assertIn(pieza, t)

    def test_nada_de_plata_en_campana_titulo_y_clasificacion_de_texto_corto(self):
        c = qp.cuerpo_campana("Aurum Fit Girls Spa", self.EQ, "Falta de stock", "Isabel", "OT-2026-00180")
        self.assertIn("Aurum Fit Girls Spa", c)
        self.assertIn("sin fecha", c)
        self.assertIsNone(_PLATA.search(c), c)
        self.assertIsNone(_PLATA.search(qp.titulo_campana("OT-2026-00171", 2)))
        self.assertIsNone(_PLATA.search(qp.descripcion_pendiente("OT-2026-00171", "Falta de stock", self.EQ)))

    def test_clasificacion_persistente_dice_de_quien_viene_y_que_no_se_cobra_dos_veces(self):
        j = qp.json_clasificacion("OT-2026-00171", 346, "Falta de stock", "Isabel", "20/10/2026 10:00", "llega el 5")
        self.assertEqual((j["codigo"], j["madre_id"], j["derivada"]), ("pendiente_stock", 346, False))
        self.assertIn("no se cobra dos veces", " ".join(j["otros"]))
        self.assertEqual((j["registrado_por"], j["argumento"]), ("Isabel", "llega el 5"))
        self.assertLessEqual(len("pendiente_stock"), 30)                    # cabe en mant_visitas.fin_clasif VARCHAR(30)
        self.assertLessEqual(len(qp.REL_PENDIENTE_STOCK), 20)                 # cabe en mant_visitas.ot_relacion VARCHAR(20)

    def test_chips(self):
        self.assertEqual(qp.chip_pendiente("OT-2026-00171"), "Pendiente de OT-2026-00171 · falta stock")
        self.assertEqual(qp.chip_madre("OT-2026-00180"), "Pendiente: OT-2026-00180 · falta stock")


# ═══════════════════════════════ 2) la transacción, con una base en memoria ═══════════════════════════════
class Cursor:
    """Cursor de mentira: anota cada sentencia y responde según lo que el mundo tenga."""

    def __init__(self, mundo):
        self.m, self.lastrowid, self._res = mundo, None, []

    def execute(self, sql, params=()):
        s = " ".join(sql.split())
        self.m.sql.append((s, tuple(params or ())))
        if self.m.falla_en and self.m.falla_en in s:
            raise RuntimeError("falla simulada")
        self._res = []
        if s.startswith("SELECT * FROM mant_visitas WHERE id=%s FOR UPDATE"):
            self._res = [dict(self.m.root)]
        elif "FROM mant_visitas WHERE ot_madre_id=%s AND ot_relacion=%s AND id<>%s" in s:
            self._res = [dict(h) for h in self.m.hijas]
        elif s.startswith("INSERT INTO mant_visitas"):
            self.lastrowid = 500
        elif s.startswith("SELECT COALESCE(MAX(orden)"):
            self._res = [{"m": 0}]
        elif s.startswith("SELECT plantilla_id, titulo") and "FROM mant_visita_tareas" in s:
            self._res = [dict(t) for t in self.m.tareas.get(params[1], [])]
        elif s.startswith("SELECT id, estado FROM mant_ot_producto_estado"):
            self._res = [dict(a) for a in self.m.activas.get(params[1], [])]
        elif s.startswith("INSERT INTO mant_ot_que_paso"):
            self.lastrowid = 77
        return 0

    def fetchone(self):
        return self._res[0] if self._res else None

    def fetchall(self):
        return list(self._res)


class Conexion:
    def __init__(self, mundo):
        self.m, self.cerrada, self.commits, self.rollbacks = mundo, False, 0, 0

    def autocommit(self, _v):
        pass

    def cursor(self):
        return Cursor(self.m)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.cerrada = True


class Mundo:
    def __init__(self):
        self.sql, self.falla_en, self.conn = [], None, None
        self.root = {"id": 10, "numero_ot": "OT-2026-00171", "tipo": "instalacion", "estado": "en_curso", "cliente_id": 5,
                     "centro_costo": "instalaciones", "acceso_ascensor": 1, "acceso_estacionamiento": 0, "acceso_piso": 3,
                     "acceso_notas": "portón lateral", "contacto_nombre": "Marta", "contacto_cargo": "Gerente", "contacto_tel": "+56911112222",
                     "contacto_email": "marta@aurum.cl", "contacto_rut": "1-9", "contacto_origen": "manual",
                     "direccion_visita": "Av. Siempre Viva 123", "direccion_detalle": "local 4", "direccion_lat": -33.4, "direccion_lng": -70.6,
                     "direccion_place_id": "pid", "direccion_comuna": "Providencia", "direccion_region": "RM", "created_by": "Aaron Urbina",
                     "created_by_user_id": 7, "evento_id": 9,
                     # lo financiero de la madre: la pendiente NO debe copiarlo
                     "zz_monto": 250000, "zz_envio_monto": 30000, "factura_tido": "FCV", "factura_nudo": "11439", "costo_proveedor": 90000}
        self.hijas = []
        self.tareas = {
            101: [{"plantilla_id": 7, "titulo": "Llegada al lugar", "descripcion": None, "tipo": "instalacion", "tipo_respuesta": "check",
                   "target_field": None, "obligatoria": 1, "requiere_foto": 0, "unidad": None, "rango_min": None, "rango_max": None,
                   "opciones_lista_json": None, "foto_si_hallazgo": 0},
                  {"plantilla_id": 7, "titulo": "Foto del accesorio", "descripcion": None, "tipo": "instalacion", "tipo_respuesta": "foto",
                   "target_field": None, "obligatoria": 1, "requiere_foto": 1, "unidad": None, "rango_min": None, "rango_max": None,
                   "opciones_lista_json": None, "foto_si_hallazgo": 0},
                  {"plantilla_id": 7, "titulo": "Observación", "descripcion": None, "tipo": "instalacion", "tipo_respuesta": "texto",
                   "target_field": None, "obligatoria": 0, "requiere_foto": 0, "unidad": None, "rango_min": None, "rango_max": None,
                   "opciones_lista_json": None, "foto_si_hallazgo": 0}],
            102: [{"plantilla_id": 7, "titulo": "Llegada al lugar", "descripcion": None, "tipo": "instalacion", "tipo_respuesta": "check",
                   "target_field": None, "obligatoria": 1, "requiere_foto": 0, "unidad": None, "rango_min": None, "rango_max": None,
                   "opciones_lista_json": None, "foto_si_hallazgo": 0}],
        }
        self.activas = {}
        self.espejos, self.libro = [], []

    def ambiente(self):
        m = self
        ambiente = {
            "print": lambda *a, **k: None, "json": json, "datetime": dt.datetime, "_qp": qp, "time": __import__("time"),
            "get_mysql": lambda: setattr(m, "conn", Conexion(m)) or m.conn,
            "mysql_fetchone": lambda sql, params=(): ({"razon_social": "Aurum Fit Girls Spa"} if "mant_clientes" in sql else dict(m.root)),
            "_qp_madre_respalda": lambda madre, cliente, momento="crear": (True, None),
            "_next_ot_number_atomic": lambda conn=None: "OT-2026-00180",
            "_ot_espejar_docs_en_hija": lambda cur, hid, rid, num: m.espejos.append((hid, rid, num)) or 1,
            "_fsh_ok": lambda: True, "_fsh_col": lambda alias="": ", foto_si_hallazgo",
            "_fsh_ins": lambda fsh, it: (", foto_si_hallazgo", ",%s", (int((it or {}).get("foto_si_hallazgo") or 0),)),
            "chile_fmt_filter": lambda v, fmt="": "20/10/2026 10:00",
        }
        _exec("_qp_raiz_id", ambiente)
        _exec("_qp_aplicar", ambiente)
        return ambiente

    def n(self, patron):
        return [x for x in self.sql if x[0].startswith(patron) or patron in x[0]]


def _normalizado(equipos=(101, 102), **kw):
    d = {"tipo": "falta_stock", "motivo": "falta_stock", "accion": "cerrar_y_pendiente", "equipos": list(equipos), "avisar_bodega": True}
    d.update(kw)
    n, err = qp.validar(d, "en_curso", qp.candidatos_equipos([_cand(101, estado_pe="falta_stock"), _cand(102)]))
    assert err is None, err
    return n


CANDS = qp.candidatos_equipos([_cand(101, estado_pe="falta_stock", nombre="Pulsera", total=3, pendientes=3), _cand(102, nombre="Accesorio")])
V_MADRE = {"id": 10, "numero_ot": "OT-2026-00171", "cliente_id": 5, "estado": "en_curso", "ot_madre_id": None, "ot_relacion": None}


class TestTransaccion(unittest.TestCase):
    def setUp(self):
        self.m = Mundo()
        self.amb = self.m.ambiente()

    def _aplicar(self, v=None, n=None):
        return self.amb["_qp_aplicar"](10, v or dict(V_MADRE), n or _normalizado(), CANDS, "Isabel Milling", 31)

    def test_crea_la_hija_pendiente_sin_fecha_y_unida_a_la_madre(self):
        r = self._aplicar()
        self.assertTrue(r["ok"], r)
        self.assertEqual((r["hija"]["id"], r["hija"]["numero_ot"], r["hija"]["nueva"]), (500, "OT-2026-00180", True))
        ins = self.m.n("INSERT INTO mant_visitas")[0]
        self.assertIn(qp.FECHA_SIN_FECHA, ins[1])                               # sin fecha hasta que llegue el stock
        self.assertIn("instalacion", ins[1])                                    # mismo tipo que la madre
        self.assertIn("pagado", ins[0])
        up = self.m.n("UPDATE mant_visitas SET ot_madre_id=%s, ot_relacion=%s")[0]
        self.assertEqual(up[1][:2], (10, "pendiente_stock"))
        self.assertIn("evento_id=%s", up[0])                                     # hereda el evento de la madre
        self.assertEqual(self.m.conn.commits, 1)
        self.assertEqual(self.m.conn.rollbacks, 0)
        self.assertTrue(self.m.conn.cerrada)

    def test_misma_factura_no_se_cobra_dos_veces(self):
        r = self._aplicar()
        # el documento viaja ESPEJADO (es_cobro=0, heredado): no consume saldo ni entra al libro (REGLAS #25 y #28)
        self.assertEqual(self.m.espejos, [(500, 10, "OT-2026-00171")])
        ins = self.m.n("INSERT INTO mant_visitas")[0]
        for col in ("zz_monto", "zz_envio_monto", "factura_tido", "factura_nudo", "costo_proveedor", "valor_origen", "documentos_extra"):
            self.assertNotIn(col, ins[0], col)
        for monto in (250000, 30000, "FCV", "11439", 90000):
            self.assertNotIn(monto, ins[1])
        self.assertEqual(self.m.n("INSERT INTO mant_visita_documentos"), [])    # no escribe documentos propios (los espeja la función de la desinstalación)
        self.assertIn("no_aplica", ins[0])

    def test_trae_solo_las_tareas_sin_hacer_de_los_equipos_elegidos(self):
        self._aplicar()
        tareas = self.m.n("INSERT INTO mant_visita_tareas")
        self.assertEqual(len(tareas), 4)                                         # 3 del equipo 101 + 1 del 102
        self.assertEqual([t[1][6] for t in tareas], [101, 101, 101, 102])        # maquina_id
        self.assertEqual({t[1][0] for t in tareas}, {500})                       # todas en la hija
        self.assertEqual([t[1][3] for t in tareas][:3], ["Llegada al lugar", "Foto del accesorio", "Observación"])
        sel = self.m.n("SELECT plantilla_id, titulo")[0][0]
        self.assertIn("COALESCE(completada,0)=0", sel)                            # lo ya hecho se queda en la madre (evidencia)
        self.assertEqual([t[1][1] for t in tareas], [7, 7, 7, 7])                 # la misma plantilla que la persona eligió (REGLA #32)

    def test_los_equipos_quedan_falta_stock_y_ya_no_frenan_la_firma(self):
        self._aplicar()
        marcas = self.m.n("INSERT INTO mant_ot_producto_estado")
        self.assertEqual([m[1][1] for m in marcas], [101, 102])
        self.assertTrue(all("'falta_stock'" in m[0] for m in marcas))
        sal = self.m.n("INSERT INTO mant_visita_equipos")
        self.assertEqual(len(sal), 2)
        self.assertTrue(all("'saltado','falta_stock'" in s[0] for s in sal))      # el mismo registro de «No pude revisarlo»
        self.assertIn("OT-2026-00180", sal[0][1][2])

    def test_un_equipo_ya_marcado_falta_stock_no_se_marca_dos_veces(self):
        self.m.activas = {101: [{"id": 4, "estado": "falta_stock"}], 102: [{"id": 5, "estado": "no_esta"}]}
        self._aplicar()
        marcas = self.m.n("INSERT INTO mant_ot_producto_estado")
        self.assertEqual([m[1][1] for m in marcas], [102])                        # el 101 ya lo tenía
        self.assertEqual(len(self.m.n("UPDATE mant_ot_producto_estado SET activo=0")), 1)   # la acción anterior del 102 se cierra, no se borra

    def test_queda_el_registro_la_bitacora_y_la_clasificacion(self):
        self._aplicar()
        qpaso = self.m.n("INSERT INTO mant_ot_que_paso")[0]
        self.assertEqual(qpaso[1][0], 10)
        self.assertEqual(qpaso[1][1:4], ("falta_stock", "falta_stock", "Falta de stock: el producto no llegó a terreno"))
        self.assertEqual(qpaso[1][7], 500)                                         # hija_visita_id
        self.assertIn("Pulsera", qpaso[1][6])                                      # equipos_json: foto del momento
        logs = self.m.n("INSERT INTO mant_logs")
        self.assertEqual([x[1][1] for x in logs], ["que_paso", "pendiente_stock_creada"])
        self.assertIn("Decidió: Isabel Milling", logs[0][1][2])
        clasif = self.m.n("UPDATE mant_visitas SET fin_clasif=%s")[0]
        self.assertEqual(clasif[1][0], "pendiente_stock")
        self.assertIn("OT-2026-00171", clasif[1][1])

    def test_si_ya_hay_una_hija_sin_empezar_y_sin_fecha_se_le_suman_los_equipos(self):
        self.m.hijas = [{"id": 400, "numero_ot": "OT-2026-00175", "estado": "programada", "fecha_programada": qp.FECHA_SIN_FECHA}]
        r = self._aplicar()
        self.assertEqual((r["hija"]["id"], r["hija"]["nueva"]), (400, False))
        self.assertEqual(self.m.n("INSERT INTO mant_visitas"), [])
        self.assertEqual(self.m.espejos, [])                                       # el documento ya estaba espejado
        self.assertEqual([x[1][1] for x in self.m.n("INSERT INTO mant_logs")], ["que_paso", "pendiente_stock_ampliada"])

    def test_una_hija_ya_reagendada_o_empezada_no_se_toca_nace_otra(self):
        self.m.hijas = [{"id": 400, "numero_ot": "OT-2026-00175", "estado": "programada", "fecha_programada": dt.date(2026, 11, 3)}]
        self.assertEqual(self._aplicar()["hija"]["id"], 500)

    def test_todo_o_nada_si_algo_falla_se_deshace(self):
        self.m.falla_en = "INSERT INTO mant_ot_que_paso"
        r = self._aplicar()
        self.assertFalse(r["ok"])
        self.assertEqual(r["codigo"], "ERROR_INTERNO")
        self.assertEqual((self.m.conn.commits, self.m.conn.rollbacks), (0, 1))
        self.assertTrue(self.m.conn.cerrada)
        self.assertIn("No se cambió nada", r["error"])

    def test_la_madre_se_bloquea_para_que_nadie_cree_dos_hijas_a_la_vez(self):
        self._aplicar()
        self.assertIn("FOR UPDATE", self.m.n("SELECT * FROM mant_visitas WHERE id=%s")[0][0])
        self.assertIn("FOR UPDATE", self.m.sql[1][0] + self.m.sql[2][0])

    def test_sin_documento_que_la_respalde_no_se_crea_nada(self):
        self.amb["_qp_madre_respalda"] = lambda madre, cliente, momento="crear": (False, "La OT de origen no tiene un documento")
        r = self._aplicar()
        self.assertEqual((r["ok"], r["codigo"], r["http"]), (False, "DOC_REQUERIDO", 409))
        self.assertEqual(self.m.sql, [])                                           # ni siquiera abrió la transacción

    def test_solo_registrar_no_crea_ni_marca_nada(self):
        n, err = qp.validar({"tipo": "falta_stock", "motivo": "falta_stock", "accion": "solo_registrar"}, "en_curso", CANDS)
        r = self.amb["_qp_aplicar"](10, dict(V_MADRE), n, CANDS, "Isabel", 31)
        self.assertTrue(r["ok"])
        self.assertIsNone(r["hija"])
        self.assertEqual(self.m.n("INSERT INTO mant_visitas"), [])
        self.assertEqual(self.m.n("INSERT INTO mant_ot_producto_estado"), [])
        self.assertEqual([x[1][1] for x in self.m.n("INSERT INTO mant_logs")], ["que_paso"])

    def test_desde_una_ot_pendiente_la_nueva_cuelga_de_la_madre_original_sin_cadenas(self):
        v_hija = {"id": 500, "numero_ot": "OT-2026-00180", "cliente_id": 5, "estado": "programada", "ot_madre_id": 10, "ot_relacion": "pendiente_stock"}
        self.m.tareas[101] = self.m.tareas[101][:1]
        r = self.amb["_qp_aplicar"](500, v_hija, _normalizado(equipos=(101,)), CANDS, "Isabel", 31)
        self.assertTrue(r["ok"])
        self.assertEqual(self.m.n("UPDATE mant_visitas SET ot_madre_id=%s")[0][1][:2], (10, "pendiente_stock"))
        self.assertEqual(self.m.n("SELECT * FROM mant_visitas WHERE id=%s FOR UPDATE")[0][1], (10,))


# ═══════════════════════════════ 3) las rutas, con dobles ═══════════════════════════════
class TestRutas(unittest.TestCase):
    def _amb(self, v=None, tecnico=False, aplicar=None):
        self.llamadas = []
        resp = lambda d=None, **kw: d if d is not None else kw
        amb = {
            "print": lambda *a, **k: None, "_qp": qp, "jsonify": resp,
            "request": types.SimpleNamespace(get_json=lambda silent=True: self.cuerpo),
            "g": types.SimpleNamespace(user={"id": 31, "nombre": "Isabel Milling", "username": "isabel@x.cl", "role": "tecnico_externo"}),
            "current_username": lambda: "isabel@x.cl",
            "_ot2_err": lambda msg, cod, http=400, **kw: {"ok": False, "error": msg, "error_codigo": cod, "http": http},
            "mysql_fetchone": lambda sql, params=(): dict(v) if v else None,
            "mysql_execute": lambda sql, params=(): self.llamadas.append(("sql", sql, params)),
            "_qp_ok": lambda: True,
            "_qp_equipos_de": lambda vid, v_: [_cand(101), _cand(102)],
            "_qp_aplicar": aplicar or (lambda vid, v_, n, cands, user, uid: {
                "ok": True, "hija": {"id": 500, "numero_ot": "OT-2026-00180", "nueva": True}, "equipos": [c for c in cands if c["maquina_id"] in n["equipos"]],
                "registro_id": 77, "root": dict(v_), "cliente": "Aurum Fit Girls Spa"}),
            "_qp_campana": lambda *a, **k: self.llamadas.append(("campana",)) or 3,
            "_qp_bodega_correo": lambda *a, **k: self.llamadas.append(("bodega",)) or ("enviado", ["bodega@sphs.cl"]),
            "_fac_libro_marcar": lambda hid: self.llamadas.append(("libro", hid)),
            "_es_rol_tecnico": lambda *a, **k: tecnico,
            "_ot_visible_para_rol": lambda hid: False,
        }
        _exec("ot_api_que_paso_registrar", amb)
        return amb

    BASE = {"id": 10, "numero_ot": "OT-2026-00171", "cliente_id": 5, "estado": "en_curso", "firma_cliente_url": None, "ot_madre_id": None,
            "ot_relacion": None}
    CUERPO = {"tipo": "falta_stock", "motivo": "falta_stock", "accion": "cerrar_y_pendiente", "equipos": [101, 102], "avisar_bodega": True}

    def test_registra_avisa_a_gestion_y_a_bodega_y_dice_lo_que_sigue(self):
        self.cuerpo = dict(self.CUERPO)
        r = self._amb(dict(self.BASE))["ot_api_que_paso_registrar"](10)
        self.assertTrue(r["ok"])
        self.assertEqual(r["pendiente"]["numero_ot"], "OT-2026-00180")
        self.assertEqual(r["siguiente"], "firmar")
        self.assertEqual(r["avisos"], {"gestion": 3, "bodega": "enviado"})
        self.assertIn(("campana",), self.llamadas)
        self.assertIn(("bodega",), self.llamadas)
        self.assertIn(("libro", 500), self.llamadas)                       # el libro de saldo se entera de la OT nueva (REGLA #28)

    def test_bodega_solo_si_se_pidio(self):
        self.cuerpo = dict(self.CUERPO, avisar_bodega=False)
        r = self._amb(dict(self.BASE))["ot_api_que_paso_registrar"](10)
        self.assertEqual(r["avisos"]["bodega"], "no_pedido")
        self.assertNotIn(("bodega",), self.llamadas)

    def test_una_ot_terminada_responde_409_y_no_escribe(self):
        for estado in ("cerrada", "completada", "cancelada", "anulada", "pendiente_aprobacion"):
            self.cuerpo = dict(self.CUERPO)
            r = self._amb(dict(self.BASE, estado=estado))["ot_api_que_paso_registrar"](10)
            self.assertEqual((r["error_codigo"], r["http"]), ("OT_CERRADA", 409), estado)
            self.assertEqual(self.llamadas, [])
        self.cuerpo = dict(self.CUERPO)
        r = self._amb(dict(self.BASE, firma_cliente_url="firma.png"))["ot_api_que_paso_registrar"](10)
        self.assertEqual(r["error_codigo"], "OT_CERRADA")

    def test_motivo_invalido_no_llega_a_la_transaccion(self):
        self.cuerpo = dict(self.CUERPO, motivo="inventado")
        amb = self._amb(dict(self.BASE), aplicar=lambda *a: self.fail("no debía aplicar"))
        r = amb["ot_api_que_paso_registrar"](10)
        self.assertEqual(r["error_codigo"], "QUE_PASO_INVALIDO")

    def test_el_tecnico_no_recibe_un_enlace_que_no_puede_abrir(self):
        self.cuerpo = dict(self.CUERPO)
        r = self._amb(dict(self.BASE), tecnico=True)["ot_api_que_paso_registrar"](10)
        self.assertIsNone(r["pendiente"]["url"])
        r = self._amb(dict(self.BASE), tecnico=False)["ot_api_que_paso_registrar"](10)
        self.assertEqual(r["pendiente"]["url"], "/ot/500")

    def test_la_respuesta_no_lleva_plata_ni_documentos(self):
        self.cuerpo = dict(self.CUERPO)
        r = self._amb(dict(self.BASE))["ot_api_que_paso_registrar"](10)
        self.assertIsNone(_PLATA.search(json.dumps(r, ensure_ascii=False, default=str)), r)

    def test_si_el_correo_de_bodega_se_cae_la_gestion_igual_se_entera(self):
        self.cuerpo = dict(self.CUERPO)
        amb = self._amb(dict(self.BASE))
        amb["_qp_bodega_correo"] = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("smtp"))
        r = amb["ot_api_que_paso_registrar"](10)
        self.assertTrue(r["ok"])
        self.assertEqual((r["avisos"]["gestion"], r["avisos"]["bodega"]), (3, "bloqueado"))

    def test_reagendar_manda_a_reagendar_sin_crear_nada(self):
        self.cuerpo = {"tipo": "no_se_pudo", "motivo": "clima", "accion": "reagendar"}
        r = self._amb(dict(self.BASE), aplicar=lambda vid, v_, n, c, u, i: {"ok": True, "hija": None, "equipos": [], "registro_id": 8, "root": dict(v_),
                                                                           "cliente": "X"})["ot_api_que_paso_registrar"](10)
        self.assertEqual((r["siguiente"], r["pendiente"]), ("reagendar", None))
        self.assertNotIn(("bodega",), self.llamadas)


# ═══════════════════════════════ 4) la puerta del documento (REGLA #24) ═══════════════════════════════
class TestPuerta(unittest.TestCase):
    def _respalda(self, madre, tiene_doc=True, tiene_factura=False, aprobadas=(), cotiz=False, cliente=5, momento="crear"):
        amb = {"_qp": qp, "print": lambda *a, **k: None,
               "_ot_puerta_autorizaciones_de": lambda mid: [{"tipo": t, "estado": "aprobada"} for t in aprobadas],
               "_ot_madre_tiene_factura": lambda mid: tiene_factura, "_ot_madre_tiene_doc_real": lambda mid: tiene_doc,
               "mysql_fetchone": lambda sql, params=(): ({"x": 1} if cotiz else None)}
        _exec("_qp_madre_respalda", amb)
        return amb["_qp_madre_respalda"](madre, cliente, momento)

    MADRE = {"id": 10, "numero_ot": "OT-2026-00171", "cliente_id": 5, "estado": "en_curso"}

    def test_crear_exige_documento_o_autorizacion_en_la_madre(self):
        self.assertTrue(self._respalda(self.MADRE)[0])
        self.assertFalse(self._respalda(self.MADRE, tiene_doc=False)[0])
        for aut in ("crear_sin_documento", "cerrar_sin_documento", "cobro_cero"):
            self.assertTrue(self._respalda(self.MADRE, tiene_doc=False, aprobadas=(aut,))[0], aut)
        self.assertTrue(self._respalda(self.MADRE, tiene_doc=False, cotiz=True)[0])
        self.assertFalse(self._respalda(self.MADRE, tiene_doc=False, aprobadas=("exceder_saldo",))[0])   # otra autorización no respalda

    def test_cerrar_exige_factura_o_boleta_en_la_madre_como_la_desinstalacion_cubierta(self):
        self.assertFalse(self._respalda(self.MADRE, momento="cerrar")[0])                  # solo nota de venta: todavía no
        self.assertTrue(self._respalda(self.MADRE, tiene_factura=True, momento="cerrar")[0])
        self.assertTrue(self._respalda(self.MADRE, aprobadas=("cerrar_sin_documento",), momento="cerrar")[0])
        self.assertIn("factura o boleta", self._respalda(self.MADRE, momento="cerrar")[1])

    def test_madre_cancelada_o_de_otro_cliente_no_respalda(self):
        self.assertFalse(self._respalda(dict(self.MADRE, estado="cancelada"))[0])
        self.assertFalse(self._respalda(dict(self.MADRE, cliente_id=99))[0])
        self.assertFalse(self._respalda(None)[0])

    def _via(self, fila, respalda=(True, None), momento="crear"):
        amb = {"_qp": qp, "_saldo": types.SimpleNamespace(_entero=lambda x: int(x or 0)), "print": lambda *a, **k: None,
               "_ot_fila_basica": lambda vid: (dict(fila) if int(vid) == 500 else {"id": 10, "numero_ot": "OT-2026-00171", "cliente_id": 5}),
               "_qp_madre_respalda": lambda m, c, mo="crear": respalda if mo == "crear" or momento != "cerrar" else (False, "falta la factura")}
        _exec("_ot_puerta_via_pendiente", amb)
        return amb["_ot_puerta_via_pendiente"]({"id": 500}, momento)

    PEND = {"id": 500, "ot_madre_id": 10, "ot_relacion": "pendiente_stock", "cliente_id": 5, "zz_monto": None, "zz_envio_monto": None}

    def test_la_pendiente_pasa_la_puerta_con_el_documento_de_su_madre(self):
        v = self._via(self.PEND)
        self.assertTrue(v["ok"])
        self.assertEqual((v["via"], v["madre_id"], v["madre_numero"]), ("pendiente_madre", 10, "OT-2026-00171"))

    def test_una_ot_normal_o_con_cobro_propio_sigue_el_camino_de_siempre(self):
        self.assertIsNone(self._via(dict(self.PEND, ot_relacion="desinstalacion")))
        self.assertIsNone(self._via(dict(self.PEND, ot_madre_id=None)))
        self.assertIsNone(self._via(dict(self.PEND, zz_monto=50000)))
        self.assertIsNone(self._via(self.PEND, respalda=(False, "no")))                   # la madre no la respalda: camino normal

    def test_al_cerrar_sin_factura_en_la_madre_dice_que_hacer(self):
        v = self._via(self.PEND, momento="cerrar")
        self.assertFalse(v["ok"])
        self.assertEqual((v["code"], v["falta"], v["accion"]), ("DOC_REQUERIDO", "madre_factura", "abrir_ot_madre"))

    def test_la_puerta_la_consulta_antes_del_camino_normal_y_nunca_lee_la_relacion_del_dict(self):
        src = _fuente("_ot_puerta_documento")
        self.assertIn("_ot_puerta_via_pendiente(v, momento)", src)
        self.assertLess(src.index("_ot_puerta_via_pendiente"), src.index("# 4) Documentos del ERP"))
        via = _fuente("_ot_puerta_via_pendiente")
        self.assertIn("_ot_fila_basica(vid)", via)
        self.assertNotIn('v.get("ot_madre_id")', via)
        self.assertNotIn('(v or {}).get("ot_madre_id")', via)


# ═══════════════════════════════ 5) cableado en app.py, eventos y panel ═══════════════════════════════
class TestCableado(unittest.TestCase):
    def test_rutas_y_candados(self):
        self.assertEqual(_decoradores("ot_api_que_paso_leer"), ["app.route('/ot/api/<int:vid>/que-paso', methods=['GET'])", "_mant_required", "_ot_can_view"])
        d = _decoradores("ot_api_que_paso_registrar")
        self.assertEqual(d[1:], ["_mant_required", "_ot_can_configurar"])           # técnico asignado (también el externo) o gestión
        self.assertIn("methods=['POST']", d[0])
        reg = _fuente("ot_api_que_paso_registrar")
        self.assertIn("esta_terminada(", reg)
        self.assertIn('"OT_CERRADA"', reg)
        self.assertIn("http=409", reg)
        self.assertIn("_fac_libro_marcar(hija[\"id\"])", reg)

    def test_nada_de_nombres_financieros_en_las_rutas_nuevas(self):
        """El escáner de tests/test_confidencialidad_financiera_tecnicos.py exige gestión a toda ruta con estas palabras (REGLA #26)."""
        pat = re.compile(r"finanz|costo|factura|autoriz|regulariz|dashboard|facprov|margen|valoriz", re.I)
        for nombre in ("ot_api_que_paso_leer", "ot_api_que_paso_registrar"):
            self.assertIsNone(pat.search(nombre), nombre)
        for m in re.finditer(r'@app\.route\("([^"]*que-paso[^"]*)"', APP):
            self.assertIsNone(re.search(r"finanz|factura|autoriz|saldo|costo|margen", m.group(1)), m.group(1))

    def test_lo_que_se_le_manda_al_tecnico_no_trae_plata_documentos_ni_saldo(self):
        for nombre in ("ot_api_que_paso_leer", "ot_api_que_paso_registrar", "_qp_historial", "_qp_pendientes_de", "_qp_contexto_ficha"):
            src = _fuente(nombre)
            self.assertIsNone(re.search(r"zz_|costo_|valorizado|factura_|proveedor|precio|margen|saldo", src), nombre)
        leer = _fuente("ot_api_que_paso_leer")
        self.assertIn('"correos": ([] if es_tec else bodega)', leer)               # los correos de bodega solo los ve la gestión

    def test_el_aviso_de_la_campana_cabe_en_el_enum_en_las_dos_listas(self):
        enums = re.findall(r"ALTER TABLE mant_notificaciones MODIFY COLUMN tipo \"\s*\"\s*ENUM\((.*?)\) DEFAULT 'otro'", APP, re.S)
        self.assertEqual(len(enums), 2)
        for e in enums:
            valores = re.findall(r"'([a-z_]+)'", e)
            self.assertIn("ot_pendiente_stock", valores)
            self.assertEqual(valores[-1], "ot_pendiente_stock")                   # nuevo valor AL FINAL: no se reordena nada (REGLA #18)
        usados = set(re.findall(r'_mant_notificar\(\s*[^,()]+,\s*"([a-z_]+)"', APP))
        self.assertIn("ot_pendiente_stock", usados)
        for e in enums:
            self.assertLessEqual(usados, set(re.findall(r"'([a-z_]+)'", e)), usados)

    def test_arranque_barato_regla_18(self):
        src = _fuente("_ensure_ot_que_paso")
        self.assertEqual(src.count("mysql_execute("), 1)                            # una sentencia, un CREATE TABLE IF NOT EXISTS
        self.assertIn("CREATE TABLE IF NOT EXISTS mant_ot_que_paso", src)
        self.assertNotIn("ALTER TABLE", src)                                         # ni una columna nueva en mant_visitas
        self.assertNotIn("lock_wait_timeout", src)
        self.assertRegex(APP, r"_ensure_ot_producto_estado\(\)\r?\n\s+_ensure_ot_que_paso\(\)")

    def test_una_sola_transaccion_todo_o_nada(self):
        src = _fuente("_qp_aplicar")
        for pieza in ("FOR UPDATE", "_next_ot_number_atomic(conn)", "conn.commit()", "conn.rollback()", "conn.close()",
                      "_ot_espejar_docs_en_hija(cur, hid, root_id, num_root)", "COALESCE(completada,0)=0"):
            self.assertIn(pieza, src, pieza)
        self.assertLess(src.index("conn.commit()"), src.index("return {\"ok\": True"))
        for prohibido in ("zz_monto", "zz_envio_monto", "factura_tido", "factura_nudo", "costo_proveedor", "valor_origen", "UPDATE mant_visita_documentos",
                          "INSERT INTO mant_visita_documentos"):
            self.assertNotIn(prohibido, src, prohibido)

    def test_el_aviso_a_bodega_es_interno_y_pasa_por_la_puerta_de_los_correos(self):
        src = _fuente("_qp_bodega_correo")
        self.assertIn("_send_ilus_email(", src)
        self.assertIn('modulo="comunicacion_interna"', src)
        self.assertIn("asincrono=True", src)
        self.assertIn("_qp_correos_bodega()", src)
        correos = _fuente("_qp_correos_bodega")
        self.assertIn("_acta_correos_normalizar(", correos)                        # solo @sphs.cl o @ilusfitness.com: jamás a un cliente
        self.assertIn("aviso_bodega_emails", correos)
        self.assertIn("PICKUP_SETTINGS_TABLE", correos)
        self.assertNotIn("customer", src.lower())
        self.assertNotIn("contacto_email", src)

    def test_el_cliente_nunca_recibe_nada(self):
        for nombre in ("_qp_aplicar", "_qp_campana", "_qp_bodega_correo", "ot_api_que_paso_registrar"):
            src = _fuente(nombre)
            self.assertNotIn("_mant_ot_email_cliente", src, nombre)
            self.assertNotIn("_mant_ot_email_tecnico", src, nombre)
            self.assertNotIn("whatsapp", src.lower(), nombre)
            self.assertNotIn("notify_async", src, nombre)
        # copiar el contacto a la OT nueva NO es escribirle: la transacción no envía nada
        self.assertNotIn("_send_ilus_email", _fuente("_qp_aplicar"))
        for nombre in ("_qp_campana", "ot_api_que_paso_registrar"):
            self.assertNotIn("contacto_email", _fuente(nombre), nombre)
        self.assertNotIn("contacto_email", _fuente("_qp_bodega_correo"))

    def test_la_campana_va_a_gestion_y_no_a_quien_lo_registro(self):
        src = _fuente("_qp_campana")
        self.assertIn('"ot_pendiente_stock"', src)
        self.assertIn("created_by_user_id", src)
        self.assertIn("role='supervisor' OR role LIKE 'ejecutivo%'", src)
        self.assertIn("uid == int(quien_uid)", src)

    def test_la_ficha_recibe_el_contexto_y_enmascara_enlaces_de_tecnico(self):
        self.assertIn("que_paso = _qp_contexto_ficha(vid, v, puede_configurar, ot_solo_lectura)", APP)
        self.assertIn("que_paso=que_paso,", APP)
        ctx = _fuente("_qp_contexto_ficha")
        self.assertIn("_ot_visible_para_rol(", ctx)

    def test_panel_y_ficha_muestran_sin_fecha_y_el_chip(self):
        enr = _fuente("_ot2_enriquecer_fila")
        self.assertIn("_qp.es_sin_fecha(fp)", enr)
        self.assertIn('"Sin fecha"', enr)
        self.assertIn("Esperando stock", enr)
        chips = _fuente("_ot2_panel_evento_chips")
        self.assertIn("_qp.REL_PENDIENTE_STOCK", chips)
        self.assertIn("_qp.chip_pendiente(", chips)
        self.assertIn("_qp.chip_madre(", chips)

    def test_la_pendiente_no_cuenta_como_desinstalacion_ni_como_dia_del_evento(self):
        # la relación pendiente_stock NO entra en las hijas de desinstalación (todas esas lecturas filtran ot_relacion='desinstalacion')
        self.assertIn("ot_relacion='desinstalacion'", _fuente("_ot_hijas_de"))
        self.assertIn("ot_relacion='desinstalacion'", _fuente("_ot_hijas_vigentes_lote"))
        hoy = dt.date(2026, 10, 25)
        madre = {"id": 10, "tipo": "instalacion", "estado": "cerrada", "ot_madre_id": None, "ot_relacion": None, "fecha_programada": "2026-10-20"}
        pend = {"id": 11, "tipo": "instalacion", "estado": "programada", "ot_madre_id": 10, "ot_relacion": "pendiente_stock",
                "fecha_programada": "2099-12-31"}
        av = ev.avance_evento([madre, pend], hoy)
        par_madre = [p for p in av["pares"] if p["madre"]["id"] == 10][0]
        self.assertIsNone(par_madre["hija"])                                   # no es «su desinstalación»
        self.assertEqual(par_madre["estado"]["codigo"], "ninguna")
        self.assertEqual(av["desinst_pendientes"], 0)
        self.assertEqual(av["pendientes_total"], 1)                           # pero el evento NO termina mientras quede la pendiente
        self.assertIsNone(av["proxima_fecha"])                                # y 2099-12-31 no es «la próxima fecha»
        self.assertEqual(ev.cuadro_por_dia([madre, pend]).keys(), {"2026-10-20"})
        # una desinstalación de verdad sigue funcionando igual
        desinst = {"id": 12, "tipo": "desinstalacion", "estado": "programada", "ot_madre_id": 10, "ot_relacion": "desinstalacion",
                   "fecha_programada": "2026-11-20"}
        av2 = ev.avance_evento([dict(madre, desinstalacion_prevista="2026-11-20"), desinst], hoy)
        self.assertEqual(av2["desinst_pendientes"], 1)
        self.assertEqual(av2["proxima_fecha"], "2026-11-20")

    def test_prueba_de_que_las_rutas_estan_registradas_una_vez(self):
        self.assertEqual(len(re.findall(r'@app\.route\("/ot/api/<int:vid>/que-paso", methods=\["GET"\]\)', APP)), 1)
        self.assertEqual(len(re.findall(r'@app\.route\("/ot/api/<int:vid>/que-paso", methods=\["POST"\]\)', APP)), 1)


# ═══════════════════════════════ 6) el frente ═══════════════════════════════
class TestFrente(unittest.TestCase):
    def test_sin_popups_nativos_regla_1(self):
        for archivo, src in (("js", JS), ("parcial", PARCIAL)):
            self.assertIsNone(re.search(r"(?<![A-Za-z.])(alert|confirm|prompt)\s*\(", src), archivo)
        self.assertIn("window.ilusConfirm", JS)
        self.assertIn("window.ilusToast", JS)

    def test_regla_14_marcar_y_desmarcar_todo(self):
        self.assertIn("data-qp-todo", JS)
        self.assertIn("Marcar todo", JS)
        self.assertIn("Desmarcar todo", JS)

    def test_regla_15_nombres_completos_nunca_cortados(self):
        for prohibido in ("text-overflow", "line-clamp", "-webkit-line-clamp", "white-space:nowrap", "white-space: nowrap"):
            self.assertNotIn(prohibido, CSS)
        self.assertIn("overflow-wrap:anywhere", CSS)

    def test_regla_3_boton_de_44px_pantalla_completa_en_celular_y_movimiento_reducido(self):
        self.assertGreaterEqual(len(re.findall(r"min-height:44px", CSS)), 4)
        self.assertIn("modal-fullscreen-sm-down", JS)
        self.assertIn("font-size:16px", CSS)                    # anti auto-zoom de iOS en el detalle
        self.assertIn("prefers-reduced-motion", CSS)
        self.assertIn("@media (max-width:600px)", CSS)

    def test_paleta_ilus_y_circulos_rojo_a_verde(self):
        for color in ("#dc2626", "#0a0a0a", "#16a34a", "#f59e0b"):
            self.assertIn(color, CSS)
        self.assertIn(".qp-paso.is-ok .qp-circ{background:#16a34a", CSS)

    def test_la_persona_decide_y_el_sistema_sugiere(self):
        self.assertIn("preseleccionar", JS)
        self.assertIn("e.preseleccionado", JS)
        self.assertIn("equipos: creaPendiente() ? elegidos() : []", JS)         # viaja exactamente lo que se ve elegido

    def test_pide_confirmacion_antes_de_crear_la_ot_pendiente_y_dice_que_no_se_cobra_de_nuevo(self):
        self.assertIn("no se cobra de nuevo", JS)
        self.assertIn("Al cliente <strong>no</strong> se le envía ningún correo.", JS)
        self.assertIn("Al cliente no se le envía nada", JS)

    def test_el_modal_no_envia_nada_al_cliente_ni_pide_montos(self):
        self.assertIsNone(re.search(r"factura|saldo|costo|proveedor|\$\s?\d", JS, re.I))
        self.assertNotIn("avisar_cliente", JS)

    def test_la_ficha_incluye_la_franja_los_chips_y_los_estilos(self):
        self.assertIn("{% include 'ot2/_que_paso.html' %}", DETALLE)
        self.assertIn("filename='ot_que_paso.css'", DETALLE)
        self.assertIn('id="qpRel"', DETALLE)
        self.assertIn("que_paso.madre.chip", DETALLE)
        self.assertIn("{{ '' if v.sin_fecha else v.fecha_programada }}", DETALLE)     # Reagendar no precarga 2099-12-31
        self.assertIn("filename='ot_que_paso.js'", PARCIAL)
        self.assertIn("window.OTD_QP_CFG = { vid: {{ v.id }} };", PARCIAL)
        self.assertIn("data-qp-abrir", PARCIAL)
        self.assertIn("¿Qué pasó?", PARCIAL)

    def test_la_ficha_cerrada_solo_deja_ver(self):
        self.assertIn("'Ver lo registrado'", PARCIAL)
        self.assertIn("qp-tranquila", PARCIAL)

    @unittest.skipUnless(HAY_NODE, "node no está instalado")
    def test_el_js_compila(self):
        raiz = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        r = subprocess.run(["node", "--check", os.path.join(raiz, "static", "ot_que_paso.js")], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stderr)

    def test_lo_nuevo_usa_finales_de_linea_crlf_como_el_resto(self):
        raiz = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        for rel in ("ot_que_paso.py", "static/ot_que_paso.js", "static/ot_que_paso.css", "templates/ot2/_que_paso.html"):
            with open(os.path.join(raiz, rel), "rb") as fh:
                b = fh.read()
            self.assertEqual(b.count(b"\n"), b.count(b"\r\n"), rel)


if __name__ == "__main__":
    unittest.main()
