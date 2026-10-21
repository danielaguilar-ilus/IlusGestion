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
  3b. A quién avisa «la gestión»: UNA función (_ot_gestion_destinatarios: responsable si la columna existe, creador, ejecutivo del
     cliente; de respaldo superadmin/admin/supervisor) que alimenta la campana y el correo interno; nada se fija a mano.
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


# Lo que devolvería _ot_gestion_destinatarios (la función única que decide a quién avisa «la gestión» de una OT).
GESTION_MENTIRA = [{"id": 2, "nombre": "Víctor Soto", "correo": "victor@sphs.cl", "por": "ejecutivo"},
                   {"id": 3, "nombre": "Aarón Rojas", "correo": "aaron@sphs.cl", "por": "creador"}]


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

    def test_un_equipo_marcado_falta_stock_se_ofrece_aunque_sus_tareas_figuren_hechas(self):
        # CASO REAL (21-oct, Aurum OT-2026-00171): la técnica marcó «falta stock» a los accesorios 7601 y 7603 y DESPUÉS dejó sus 6 tareas
        # «hechas» y vacías para poder firmar. Con «solo las sin hacer» la función no servía para el caso que la pidió.
        c = qp.candidatos_equipos([_cand(7601, estado_pe="falta_stock", pend=0, total=3), _cand(7603, estado_pe="falta_stock", pend=0, total=3),
                                   _cand(9, pend=0, total=3), _cand(10, estado_pe="no_esta", pend=0, total=3)])
        por = {x["maquina_id"]: x for x in c}
        for mid in (7601, 7603):
            self.assertTrue(por[mid]["elegible"] and por[mid]["preseleccionado"], mid)       # sugerido, y la persona decide
            self.assertEqual((por[mid]["pasan"], por[mid]["todas_marcadas_hechas"]), (3, True), mid)
        self.assertFalse(por[9]["elegible"])                    # sin marca y todo hecho: nada que dejar pendiente
        self.assertFalse(por[10]["elegible"])                   # «no está» no es «falta stock»
        n, err = qp.validar({"tipo": "falta_stock", "motivo": "falta_stock", "accion": "cerrar_y_pendiente", "equipos": [7601, 7603]},
                            "firmada_tecnico", c)
        self.assertIsNone(err)                                  # y la validación del servidor los acepta (la OT ya firmada por la técnica)
        self.assertEqual(n["equipos"], [7601, 7603])

    def test_con_tareas_sin_hacer_pasan_esas_y_no_todas(self):
        c = qp.candidatos_equipos([_cand(1, estado_pe="falta_stock", pend=2, total=3), _cand(2, pend=2, total=3)])
        self.assertEqual([(x["pasan"], x["todas_marcadas_hechas"]) for x in c], [(2, False), (2, False)])

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
            if "COALESCE(completada,0)=0" in s and params[1] in self.m.hechas:
                self._res = []                                  # todas sus tareas figuran como hechas: ninguna «sin hacer»
            else:
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
        self.puerta_ok, self.puertas = True, []
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
        self.hechas = set()          # equipos cuyas tareas TODAS figuran como hechas (la consulta «sin hacer» no trae nada)
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

    def puerta(self, v, momento="crear", **kw):
        """La puerta del documento de mentira: anota con qué la llamaron y responde lo que el mundo diga."""
        self.puertas.append((dict(v), momento, kw))
        if self.puerta_ok:
            return {"ok": True, "via": "pendiente_madre", "mensaje": ""}
        return {"ok": False, "via": None, "code": "DOC_REQUERIDO", "falta": "documento", "accion": "abrir_ot_madre",
                "mensaje": "La OT de origen no tiene un documento de Random"}

    def ambiente(self):
        m = self
        ambiente = {
            "print": lambda *a, **k: None, "json": json, "datetime": dt.datetime, "_qp": qp, "time": __import__("time"),
            "get_mysql": lambda: setattr(m, "conn", Conexion(m)) or m.conn,
            "mysql_fetchone": lambda sql, params=(): ({"razon_social": "Aurum Fit Girls Spa"} if "mant_clientes" in sql else dict(m.root)),
            "_ot_puerta_documento": lambda v, momento="crear", **kw: m.puerta(v, momento, **kw),
            "_OT_PUERTA_MSG": "Esta OT necesita un documento",
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

    def test_si_la_madre_tiene_responsable_la_pendiente_lo_hereda_y_sin_la_columna_no_pasa_nada(self):
        self._aplicar()
        self.assertNotIn("responsable_user_id", self.m.n("UPDATE mant_visitas SET ot_madre_id=%s")[0][0])   # hoy la columna no existe
        m2 = Mundo()
        m2.root["responsable_user_id"] = 3
        m2.ambiente()["_qp_aplicar"](10, dict(V_MADRE), _normalizado(), CANDS, "Isabel Milling", 31)
        up = m2.n("UPDATE mant_visitas SET ot_madre_id=%s")[0]
        self.assertIn("responsable_user_id=%s", up[0])
        self.assertEqual(up[1][:2], (10, "pendiente_stock"))
        self.assertIn(3, up[1])                                                                               # el mismo responsable que la madre

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

    def test_un_equipo_marcado_falta_stock_con_todas_sus_tareas_hechas_pasa_con_todas_sus_tareas(self):
        # CASO REAL (21-oct, Aurum OT-2026-00171): la hija no puede nacer vacía porque la técnica dejó «hechas» las tareas del accesorio.
        self.m.hechas = {101}
        cands = qp.candidatos_equipos([_cand(101, estado_pe="falta_stock", nombre="Pulsera", total=3, pendientes=0),
                                       _cand(102, nombre="Accesorio")])
        n, err = qp.validar({"tipo": "falta_stock", "motivo": "falta_stock", "accion": "cerrar_y_pendiente", "equipos": [101, 102]},
                            "en_curso", cands)
        self.assertIsNone(err)
        r = self.amb["_qp_aplicar"](10, dict(V_MADRE), n, cands, "Isabel Milling", 31)
        self.assertTrue(r["ok"], r)
        tareas = self.m.n("INSERT INTO mant_visita_tareas")
        self.assertEqual([t[1][6] for t in tareas], [101, 101, 101, 102])         # las 3 del 101 aunque figuraran hechas + la del 102
        self.assertEqual({t[1][0] for t in tareas}, {500})                         # todas en la hija, y NUEVAS (sin «completada»)
        self.assertTrue(all("completada" not in t[0] for t in tareas))
        selects = [x[0] for x in self.m.n("SELECT plantilla_id, titulo")]
        self.assertEqual(len(selects), 3)                                          # 101 con filtro, 101 sin filtro (no había «sin hacer»), 102 con filtro
        self.assertIn("COALESCE(completada,0)=0", selects[0])
        self.assertNotIn("completada", selects[1])
        self.assertIn("COALESCE(completada,0)=0", selects[2])

    def test_un_equipo_sin_la_marca_con_todo_hecho_no_trae_tareas_de_mas(self):
        # sin «falta stock» NUNCA se copia lo ya hecho (la madre es evidencia): solo el equipo marcado tiene esa salida
        self.m.hechas = {102}
        n = _normalizado()
        self.amb["_qp_aplicar"](10, dict(V_MADRE), n, CANDS, "Isabel Milling", 31)
        tareas = self.m.n("INSERT INTO mant_visita_tareas")
        self.assertEqual([t[1][6] for t in tareas], [101, 101, 101])               # el 102 no estaba marcado: no se le agrega nada
        self.assertEqual(len(self.m.n("SELECT plantilla_id, titulo")), 2)

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
        self.m.puerta_ok = False
        r = self._aplicar()
        self.assertEqual((r["ok"], r["codigo"], r["http"]), (False, "DOC_REQUERIDO", 409))
        self.assertIn("no tiene un documento", r["error"])
        self.assertEqual((r["falta"], r["accion"]), ("documento", "abrir_ot_madre"))
        self.assertEqual(self.m.sql, [])                                           # ni siquiera abrió la transacción
        self.assertIsNone(self.m.conn)

    def test_la_creacion_pasa_por_la_puerta_del_documento_con_la_madre_que_lee_el_servidor(self):
        """REGLA #24: la OT pendiente se crea SOLO si la puerta la deja pasar (vía 'pendiente_madre'); la madre la lee el servidor."""
        self._aplicar()
        (v, momento, kw), = self.m.puertas
        self.assertEqual(momento, "crear")
        self.assertEqual((v["cliente_id"], v["tipo"]), (5, "instalacion"))
        self.assertEqual(kw["pendiente_de"]["id"], 10)                             # la fila de la madre, releída de la base
        self.assertNotIn("ot_madre_id", v)                                         # nada de la relación viaja en el dict de la petición

    def test_solo_registrar_no_llama_a_la_puerta(self):
        n, _e = qp.validar({"tipo": "falta_stock", "motivo": "falta_stock", "accion": "solo_registrar"}, "en_curso", CANDS)
        self.amb["_qp_aplicar"](10, dict(V_MADRE), n, CANDS, "Isabel", 31)
        self.assertEqual(self.m.puertas, [])

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
            "_ot_gestion_destinatarios": lambda vid, fila=None: self.llamadas.append(("gestion", vid)) or list(GESTION_MENTIRA),
            "_qp_campana": lambda *a, **k: self.llamadas.append(("campana",)) or setattr(self, "args_campana", (a, k)) or 3,
            "_qp_bodega_correo": lambda *a, **k: self.llamadas.append(("bodega",)) or setattr(self, "args_bodega", (a, k))
            or ("enviado", ["bodega@sphs.cl"]),
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

    def test_la_campana_y_el_correo_comparten_la_lista_que_decide_una_sola_funcion(self):
        """Daniel va a pedir un RESPONSABLE por OT: por eso a quién avisa «la gestión» sale de UNA función y la usan los dos avisos."""
        self.cuerpo = dict(self.CUERPO)
        self._amb(dict(self.BASE))["ot_api_que_paso_registrar"](10)
        self.assertEqual([x for x in self.llamadas if x[0] == "gestion"], [("gestion", 10)])          # una sola consulta
        a_campana, _k = self.args_campana
        _a, k_bodega = self.args_bodega
        self.assertEqual([p["nombre"] for p in a_campana[0]], ["Víctor Soto", "Aarón Rojas"])         # la campana recibe la lista…
        self.assertEqual(k_bodega["gestion"], a_campana[0])                                           # …y el correo, la MISMA

    def test_si_la_funcion_de_destinatarios_falla_el_registro_igual_se_guarda_y_la_campana_no_se_cae(self):
        self.cuerpo = dict(self.CUERPO)
        amb = self._amb(dict(self.BASE))
        amb["_ot_gestion_destinatarios"] = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("bd caída"))
        r = amb["ot_api_que_paso_registrar"](10)
        self.assertTrue(r["ok"])
        self.assertEqual(self.args_campana[0][0], [])                                                 # sin nadie: la campana no avisa a ciegas
        self.assertIn(("bodega",), self.llamadas)                                                     # y bodega igual se entera


# ═══════════════════════════════ 3b) A QUIÉN AVISA «LA GESTIÓN»: una sola función ═══════════════════════════════
def _exec_asignaciones(nombres, amb):
    """Ejecuta en `amb` las asignaciones simples de app.py de nivel módulo (constantes) con esos nombres."""
    import ast
    from tests.test_ot_saldo_servicio import _arbol
    _codigo, arbol = _arbol()
    hechas = set()
    for n in arbol.body:
        if isinstance(n, ast.Assign) and len(n.targets) == 1 and isinstance(n.targets[0], ast.Name) and n.targets[0].id in nombres:
            exec(compile(ast.Module(body=[ast.parse(ast.unparse(n)).body[0]], type_ignores=[]), "<app>", "exec"), amb)
            hechas.add(n.targets[0].id)
    assert hechas == set(nombres), set(nombres) - hechas


class TestGestionDestinatarios(unittest.TestCase):
    USUARIOS = {
        1: {"id": 1, "username": "daniel.aguilar@sphs.cl", "nombre": "Daniel Aguilar", "role": "superadmin", "active": 1},
        2: {"id": 2, "username": "victor@sphs.cl", "nombre": "Víctor Soto", "role": "ejecutivo", "active": 1},
        3: {"id": 3, "username": "aaron@sphs.cl", "nombre": "Aarón Rojas", "role": "ejecutivo_sstt", "active": 1},
        4: {"id": 4, "username": "juanpablo@sphs.cl", "nombre": "Juan Pablo Díaz", "role": "supervisor", "active": 1},
        5: {"id": 5, "username": "isabel", "nombre": "Isabel Milling", "role": "tecnico_externo", "active": 1},
        6: {"id": 6, "username": "exjefe@sphs.cl", "nombre": "Ex Jefe", "role": "admin", "active": 0},
        7: {"id": 7, "username": "admin@sphs.cl", "nombre": "Administración", "role": "admin", "active": 1},
    }

    def _fn(self, ot, ejecutivo=None, usuarios=None, falla=None):
        usuarios = dict(self.USUARIOS if usuarios is None else usuarios)
        self.sql = sql = []

        def uno(consulta, params=()):
            sql.append(consulta)
            if falla and falla in consulta:
                raise RuntimeError("bd caída")
            if "FROM mant_visitas" in consulta:
                return dict(ot) if ot is not None else None
            if "FROM mant_clientes" in consulta:
                return {"ejecutivo_user_id": ejecutivo}
            if "WHERE id=%s AND active=1" in consulta:
                u = usuarios.get(params[0])
                return dict(u) if u and u.get("active") else None
            if "username=%s OR nombre=%s" in consulta:
                for u in usuarios.values():
                    if u.get("active") and params[0] in (u["username"], u["nombre"]):
                        return dict(u)
            return None

        def varios(consulta, params=()):
            sql.append(consulta)
            if falla and falla in consulta:
                raise RuntimeError("bd caída")
            return [dict(u) for u in usuarios.values() if u.get("active")]

        amb = {"print": lambda *a, **k: None, "re": re, "mysql_fetchone": uno, "mysql_fetchall": varios}
        _exec("_rol_familia", amb)
        _exec_asignaciones(("_GESTION_FAMILIAS_RESPALDO", "_GESTION_CORREO_RE"), amb)
        _exec("_ot_gestion_destinatarios", amb)
        return amb["_ot_gestion_destinatarios"]

    def _ids(self, r):
        return [p["id"] for p in r]

    OT = {"id": 10, "cliente_id": 5, "created_by_user_id": 4, "created_by": "juanpablo@sphs.cl"}

    def test_hoy_creador_y_ejecutivo_del_cliente_sin_repetidos(self):
        r = self._fn(self.OT, ejecutivo=2)(10)
        self.assertEqual([(p["id"], p["por"]) for p in r], [(4, "creador"), (2, "ejecutivo")])
        self.assertEqual(self._ids(self._fn(self.OT, ejecutivo=4)(10)), [4])                    # el ejecutivo ES quien la creó: una vez
        self.assertEqual(r[0], {"id": 4, "nombre": "Juan Pablo Díaz", "correo": "juanpablo@sphs.cl", "por": "creador"})

    def test_si_la_columna_responsable_existe_va_primero_y_si_no_existe_todo_sigue_igual(self):
        con = dict(self.OT, responsable_user_id=3)
        self.assertEqual([(p["id"], p["por"]) for p in self._fn(con, ejecutivo=2)(10)],
                         [(3, "responsable"), (4, "creador"), (2, "ejecutivo")])
        self.assertEqual(self._ids(self._fn(self.OT, ejecutivo=2)(10)), [4, 2])                 # la columna todavía no existe: nada se rompe
        self.assertEqual(self._ids(self._fn(dict(self.OT, responsable_user_id=None), ejecutivo=2)(10)), [4, 2])   # existe pero sin poner
        self.assertEqual(self._ids(self._fn(dict(self.OT, responsable_user_id=0), ejecutivo=2)(10)), [4, 2])

    def test_el_creador_de_una_ot_antigua_se_busca_por_usuario_o_por_nombre(self):
        for creado_por in ("aaron@sphs.cl", "Aarón Rojas"):
            r = self._fn({"id": 10, "cliente_id": None, "created_by": creado_por})(10)
            self.assertEqual([(p["id"], p["por"]) for p in r], [(3, "creador")], creado_por)

    def test_solo_usuarios_activos(self):
        r = self._fn(dict(self.OT, created_by_user_id=6, created_by="exjefe@sphs.cl"), ejecutivo=2)(10)
        self.assertEqual(self._ids(r), [2])                                                      # el creador ya no trabaja aquí
        r = self._fn(dict(self.OT, responsable_user_id=6), ejecutivo=2)(10)
        self.assertEqual(self._ids(r), [4, 2])                                                   # ni el responsable inactivo ni su aviso perdido

    def test_de_respaldo_superadmin_admin_y_supervisor_activos_solo_si_no_sale_nadie(self):
        r = self._fn({"id": 10, "cliente_id": None, "created_by": "sistema"})(10)
        self.assertEqual([(p["id"], p["por"]) for p in r], [(1, "respaldo"), (7, "respaldo"), (4, "respaldo")])   # superadmin, admin, supervisor
        self.assertNotIn(6, self._ids(r))                                                        # inactivo
        self.assertTrue({2, 3, 5}.isdisjoint(self._ids(r)))                                      # ni ejecutivos ni técnicos son el respaldo
        # y si hay alguien, el respaldo NO se suma
        self.assertTrue({1, 7}.isdisjoint(self._ids(self._fn(self.OT, ejecutivo=2)(10))))
        # un usuario del sistema sin nadie activo en esos roles → lista vacía, sin romper
        solo_tecnico = {5: self.USUARIOS[5]}
        self.assertEqual(self._fn({"id": 10, "cliente_id": None}, usuarios=solo_tecnico)(10), [])

    def test_el_correo_solo_es_el_usuario_cuando_es_un_correo(self):
        r = self._fn(dict(self.OT, created_by_user_id=5, created_by="isabel"))(10)
        self.assertEqual((r[0]["nombre"], r[0]["correo"]), ("Isabel Milling", ""))               # su usuario no es un correo: nada que mostrar
        self.assertEqual(self._fn(self.OT)(10)[0]["correo"], "juanpablo@sphs.cl")

    def test_usa_la_fila_que_le_pasan_y_no_relee_la_ot(self):
        fn = self._fn(None, ejecutivo=2)
        self.assertEqual(self._ids(fn(10, fila=dict(self.OT))), [4, 2])
        self.assertFalse([s for s in self.sql if "FROM mant_visitas" in s])

    def test_nunca_levanta_ante_un_error_de_la_base(self):
        self.assertEqual(self._ids(self._fn(self.OT, ejecutivo=2, falla="FROM mant_clientes")(10)), [4])     # sin el ejecutivo, el resto sirve
        r = self._fn(self.OT, falla="FROM mant_visitas")(10)                                                  # sin la OT: el respaldo
        self.assertEqual(self._ids(r), [1, 7, 4])
        self.assertEqual(self._fn(self.OT, falla="FROM app_users")(10), [])                                   # sin usuarios: vacío, no explota
        self.assertEqual(self._fn(self.OT)("no-es-un-id"), [])

    def test_no_decide_nada_fijo_en_los_avisos_y_no_escribe_en_la_base(self):
        src = _fuente("_ot_gestion_destinatarios")
        for escritura in ("INSERT", "UPDATE", "DELETE", "mysql_execute", "get_mysql"):
            self.assertNotIn(escritura, src, escritura)
        self.assertNotIn("_send_ilus_email", src)                                                # solo dice a QUIÉN; no envía nada
        self.assertNotIn("_mant_notificar", src)
        self.assertEqual(APP.count("def _ot_gestion_destinatarios("), 1)


class TestAvisosDeLaGestion(unittest.TestCase):
    """La campana y el correo interno usan la lista de _ot_gestion_destinatarios y no deciden a quién avisar."""

    def _campana(self):
        self.notif = []
        amb = {"print": lambda *a, **k: None, "_qp": qp, "_now_chile": lambda: dt.datetime(2026, 10, 20, 15, 32),
               "_mant_notificar": lambda uid, tipo, titulo, cuerpo="", url_accion="", prioridad="media", **k:
                   self.notif.append((uid, tipo, titulo, cuerpo, url_accion, prioridad)) or 1,
               "_mant_notif_cache_invalidar": lambda uid: None}
        _exec("_qp_campana", amb)
        return amb["_qp_campana"]

    N = {"motivo_texto": "Falta de stock", "detalle": None}
    EQ = [{"nombre": "Pulsera cardio", "sku": "ABC1", "serie": "55"}]

    def test_la_campana_avisa_a_la_lista_menos_a_quien_lo_registro_y_sin_repetir(self):
        gestion = [{"id": 2, "nombre": "Víctor"}, {"id": 3, "nombre": "Aarón"}, {"id": 5, "nombre": "Isabel"}, {"id": 2, "nombre": "Víctor"}]
        n = self._campana()(gestion, "OT-2026-00171", "Aurum", self.N, self.EQ, "OT-2026-00180", "Isabel Milling", 5, 500)
        self.assertEqual(n, 2)
        self.assertEqual([x[0] for x in self.notif], [2, 3])
        uid, tipo, titulo, cuerpo, url, prio = self.notif[0]
        self.assertEqual((tipo, url, prio), ("ot_pendiente_stock", "/ot/500", "alta"))
        self.assertIsNone(_PLATA.search(titulo + " " + cuerpo), cuerpo)
        # la hora (Chile) va en el título: la campana no repite un aviso IDÉNTICO mientras el primero siga sin leer, así que dos
        # registros seguidos de la misma OT (otros equipos) no esconden el segundo detrás del primero
        self.assertTrue(titulo.endswith("(20/10 15:32)"), titulo)

    def test_el_titulo_de_la_campana_lleva_la_hora_solo_si_se_la_dan(self):
        self.assertEqual(qp.titulo_campana("OT-1", 2), "OT-1: quedó trabajo pendiente por falta de stock")
        self.assertEqual(qp.titulo_campana("OT-1", 0, "20/10 15:32"), "OT-1: ¿qué pasó? (20/10 15:32)")
        self.assertLessEqual(len(qp.titulo_campana("OT-" + "9" * 400, 1, "20/10 15:32")), 200)           # cabe en mant_notificaciones.titulo

    def test_sin_lista_no_avisa_a_nadie_ni_se_inventa_destinatarios(self):
        for vacia in (None, [], [{}], [{"id": "x"}]):
            self.assertEqual(self._campana()(vacia, "OT-1", "X", self.N, self.EQ, None, "Isabel", 5, 1), 0, vacia)
        self.assertEqual(self.notif, [])

    def _correo(self, gestion, bodega=("bodega@sphs.cl",)):
        self.enviados, self.filas, self.master = [], [], {}

        def master(d):
            self.master = d
            return "<html></html>"

        amb = {"print": lambda *a, **k: None, "_qp": qp, "_qp_correos_bodega": lambda: list(bodega),
               "_brand_subject": lambda s: "ILUS · " + s, "_now_chile": lambda: dt.datetime(2026, 10, 20, 15, 30),
               "_public_base_url": lambda: "https://ilus.test", "_ilus_email_master": master,
               "_ilus_email_rows": lambda filas: (self.filas.extend(filas) or "<rows></rows>"),
               "_send_ilus_email": lambda to, asunto, html, **k: self.enviados.append((to, asunto, k)) or True}
        _exec("_qp_bodega_correo", amb)
        return amb["_qp_bodega_correo"]({"id": 10}, "OT-2026-00171", "Aurum Fit Girls Spa", self.N, self.EQ, "OT-2026-00180", "Isabel Milling",
                                        gestion=gestion)

    def test_el_correo_a_bodega_dice_a_quien_avisar_con_la_misma_lista(self):
        estado, enviados = self._correo(GESTION_MENTIRA)
        self.assertEqual((estado, enviados), ("enviado", ["bodega@sphs.cl"]))
        self.assertIn(("Avisar a", "Víctor Soto (victor@sphs.cl) · Aarón Rojas (aaron@sphs.cl)"), self.filas)
        self.assertIn("«Avisar a»", self.master["message"])
        self.assertEqual([e[0] for e in self.enviados], ["bodega@sphs.cl"])                    # el correo sigue yendo SOLO a bodega
        self.assertEqual((self.enviados[0][2]["evento"], self.enviados[0][2]["modulo"]), ("ot_que_paso_bodega", "comunicacion_interna"))

    def test_sin_gestion_el_correo_sale_igual_y_pide_avisar_a_servicio_tecnico(self):
        for vacia in (None, []):
            estado, _e = self._correo(vacia)
            self.assertEqual(estado, "enviado")
            self.assertNotIn("Avisar a", [f[0] for f in self.filas])
            self.assertIn("avisen a servicio técnico", self.master["message"])

    def test_sin_correo_de_bodega_no_sale_nada(self):
        self.assertEqual(self._correo(GESTION_MENTIRA, bodega=())[0], "sin_correos")
        self.assertEqual(self.enviados, [])

    def test_campana_y_correo_no_deciden_a_quien_avisar(self):
        for nombre in ("_qp_campana", "_qp_bodega_correo"):
            src = _fuente(nombre)
            for fijo in ("role=", "role LIKE", "FROM app_users", "created_by", "ejecutivo_user_id", "mysql_fetch"):
                self.assertNotIn(fijo, src, (nombre, fijo))
        reg = _fuente("ot_api_que_paso_registrar")
        self.assertEqual(reg.count("_ot_gestion_destinatarios("), 1)                          # UNA llamada para los dos avisos
        self.assertIn("gestion=gestion", reg)
        self.assertIn("_qp_campana(gestion,", reg)

    def _leer(self, tecnico=False, puede=True):
        resp = lambda d=None, **kw: d if d is not None else kw
        fila = {"id": 10, "numero_ot": "OT-2026-00171", "cliente_id": 5, "estado": "en_curso", "firma_cliente_url": None,
                "ot_madre_id": None, "ot_relacion": None}
        gestion = [{"id": 31, "nombre": "Isabel Milling", "correo": "isabel@sphs.cl", "por": "creador"},
                   {"id": 2, "nombre": "Víctor Soto", "correo": "victor@sphs.cl", "por": "ejecutivo"}]
        amb = {"print": lambda *a, **k: None, "_qp": qp, "jsonify": resp,
               "g": types.SimpleNamespace(user={"id": 31, "nombre": "Isabel Milling"}),
               "mysql_fetchone": lambda sql, params=(): dict(fila),
               "_ot2_err": lambda msg, cod, http=400, **kw: {"ok": False, "error_codigo": cod},
               "_es_rol_tecnico": lambda *a, **k: tecnico, "_puede_ot_accion": lambda vid, acc: puede,
               "_qp_equipos_de": lambda vid, v: [_cand(101)], "_qp_correos_bodega": lambda: ["bodega@sphs.cl"],
               "_ot_gestion_destinatarios": lambda vid, fila=None: list(gestion),
               "_qp_historial": lambda vid: [], "_qp_pendientes_de": lambda vid, v: []}
        _exec("ot_api_que_paso_leer", amb)
        return amb["ot_api_que_paso_leer"](10)

    def test_el_modal_dice_a_quien_avisa_la_campana_sin_correos_y_sin_quien_esta_registrando(self):
        r = self._leer()
        self.assertEqual(r["gestion"], {"nombres": ["Víctor Soto"]})                           # Isabel (quien registra) no se avisa a sí misma
        self.assertEqual(r["bodega"]["correos"], ["bodega@sphs.cl"])
        r = self._leer(tecnico=True)                                                          # el técnico ve los nombres, nunca los correos de bodega
        self.assertEqual((r["gestion"]["nombres"], r["bodega"]["correos"]), (["Víctor Soto"], []))
        self.assertNotIn("victor@sphs.cl", json.dumps(r, ensure_ascii=False))
        r = self._leer(puede=False)                                                           # sin permiso: ni destinatarios ni bodega
        self.assertEqual((r["gestion"]["nombres"], r["bodega"]["hay"]), ([], False))

    def test_el_modal_se_entera_de_los_nombres_sin_correos_y_sin_el_usuario_actual(self):
        leer = _fuente("ot_api_que_paso_leer")
        self.assertIn("_qp.nombres_gestion(gestion", leer)
        self.assertNotIn('"correo"', leer)
        self.assertIn("(getattr(g, \"user\", None) or {}).get(\"id\")", leer)

    def test_nombres_y_texto_de_a_quien_avisar(self):
        g = [{"id": 2, "nombre": "Víctor Soto", "correo": "v@sphs.cl"}, {"id": 3, "nombre": " Aarón ", "correo": ""},
             {"id": 2, "nombre": "Víctor Soto"}, {"id": 9, "nombre": ""}]
        self.assertEqual(qp.nombres_gestion(g), ["Víctor Soto", "Aarón"])
        self.assertEqual(qp.nombres_gestion(g, excluir_id=2), ["Aarón"])
        self.assertEqual(qp.nombres_gestion(None), [])
        self.assertEqual(qp.texto_avisar_a(g), "Víctor Soto (v@sphs.cl) · Aarón")                  # sin repetir a nadie
        self.assertEqual(qp.texto_avisar_a([]), "")
        muchos = [{"nombre": "P%d" % i, "correo": "p%d@sphs.cl" % i} for i in range(7)]
        self.assertEqual(qp.texto_avisar_a(muchos).count("@"), 4)
        self.assertTrue(qp.texto_avisar_a(muchos).endswith("y 3 más"))


# ═══════════════════════════════ 4) la puerta del documento (REGLA #24) ═══════════════════════════════
class TestPuerta(unittest.TestCase):
    def _respalda(self, madre, tiene_doc=True, tiene_factura=False, aprobadas=(), cotiz=False, cliente=5, momento="crear", contrato=False):
        amb = {"_qp": qp, "print": lambda *a, **k: None,
               "_ot_puerta_autorizaciones_de": lambda mid: [{"tipo": t, "estado": "aprobada"} for t in aprobadas],
               "_ot_madre_tiene_factura": lambda mid: tiene_factura, "_ot_madre_tiene_doc_real": lambda mid: tiene_doc,
               "_ot_fin_contrato_real_de": lambda ids: {int(i): contrato for i in ids},
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

    def test_una_preventiva_de_un_contrato_real_se_respalda_con_el_contrato(self):
        prev = dict(self.MADRE, tipo="preventiva")
        for momento in ("crear", "cerrar"):
            self.assertTrue(self._respalda(prev, tiene_doc=False, contrato=True, momento=momento)[0], momento)
        self.assertFalse(self._respalda(prev, tiene_doc=False, contrato=False)[0])                 # sin contrato real: no
        self.assertFalse(self._respalda(dict(self.MADRE, tipo="instalacion"), tiene_doc=False, contrato=True)[0])   # solo la preventiva

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

    def test_la_via_solo_nace_en_un_lugar_y_el_cierre_la_reconoce(self):
        """Los candados de cierre eximen a la vía 'pendiente_madre' (como a la 'cubierta_madre'): esa vía no puede salir de otro lado."""
        self.assertEqual(len(re.findall(r"""["']via["']\s*:\s*["']pendiente_madre["']""", APP)), 1)
        self.assertIn('"via": "pendiente_madre"', _fuente("_ot_puerta_via_pendiente"))

    def test_la_puerta_la_consulta_antes_del_camino_normal_y_nunca_lee_la_relacion_del_dict(self):
        src = _fuente("_ot_puerta_documento")
        self.assertIn("_ot_puerta_via_pendiente(v, momento, madre=pendiente_de)", src)
        self.assertLess(src.index("_ot_puerta_via_pendiente"), src.index("# 4) Documentos del ERP"))
        via = _fuente("_ot_puerta_via_pendiente")
        self.assertIn("_ot_fila_basica(vid)", via)
        self.assertNotIn('v.get("ot_madre_id")', via)
        self.assertNotIn('(v or {}).get("ot_madre_id")', via)

    def _via_creando(self, respalda=(True, None), falla=False):
        def _resp(m, c, mo="crear"):
            if falla:
                raise RuntimeError("base caída")
            return respalda
        amb = {"_qp": qp, "_saldo": types.SimpleNamespace(_entero=lambda x: int(x or 0)), "print": lambda *a, **k: None,
               "_qp_madre_respalda": _resp, "_ot_fila_basica": lambda vid: self.fail("al crear no se relee ninguna OT por id")}
        _exec("_ot_puerta_via_pendiente", amb)
        return amb["_ot_puerta_via_pendiente"]({"cliente_id": 5, "tipo": "instalacion"}, "crear",
                                               madre={"id": 10, "numero_ot": "OT-2026-00171", "cliente_id": 5})

    def test_al_crear_la_puerta_decide_con_la_madre_que_pasa_el_servidor(self):
        v = self._via_creando()
        self.assertTrue(v["ok"])
        self.assertEqual((v["via"], v["madre_id"], v["madre_numero"]), ("pendiente_madre", 10, "OT-2026-00171"))

    def test_al_crear_sin_respaldo_es_un_rechazo_explicito_nunca_un_camino_normal(self):
        v = self._via_creando(respalda=(False, "La OT de origen no tiene un documento de Random"))
        self.assertFalse(v["ok"])
        self.assertEqual((v["code"], v["falta"]), ("DOC_REQUERIDO", "documento"))
        self.assertIn("no tiene un documento", v["mensaje"])

    def test_si_la_puerta_falla_al_crear_no_deja_pasar_nada(self):
        v = self._via_creando(falla=True)
        self.assertFalse(v["ok"])
        self.assertEqual(v["code"], "DOC_REQUERIDO")

    def test_la_puerta_recibe_pendiente_de_solo_del_servidor(self):
        firma = _fuente("_ot_puerta_documento").split('"""')[0]
        self.assertIn("pendiente_de=None", firma)
        self.assertIn("_ot_puerta_via_pendiente(v, momento, madre=pendiente_de)", _fuente("_ot_puerta_documento"))
        self.assertIn("if vid or pendiente_de is not None:", _fuente("_ot_puerta_documento"))
        aplicar = _fuente("_qp_aplicar")
        self.assertIn("_ot_puerta_documento(", aplicar)
        self.assertIn("pendiente_de=(dict(root_pre) if root_pre else {})", aplicar)
        # ningún camino de una petición pasa `pendiente_de`: solo la creación del servidor
        usos = [m.start() for m in re.finditer(r"pendiente_de\s*=", APP)]
        self.assertEqual(len(usos), 2, "pendiente_de= solo en la definición de la puerta y en _qp_aplicar")


class TestCierreDeLaPendiente(unittest.TestCase):
    """La OT pendiente se tiene que poder CERRAR sin factura propia ni cobro aparte (su documento es el de la madre, espejado), y sin
    aflojar nada más: cada candado de `mant_ot_aprobar_cierre` se evalúa de verdad con la vía que le toque."""

    @classmethod
    def setUpClass(cls):
        cls.SRC = _fuente("mant_ot_aprobar_cierre")

    def _condicion(self, inicio):
        """El encabezado `if …:` del candado que empieza con ese patrón, sin comentarios y listo para evaluarse."""
        m = re.search(inicio, self.SRC)
        self.assertIsNotNone(m, inicio)
        fin = re.compile(r":[ \t]*\r?\n").search(self.SRC, m.start()).start()
        texto = re.sub(r"#[^\n]*", "", self.SRC[m.start() + len("    if "):fin])
        return " ".join(texto.split())

    BASE = dict(_gate_on=True, _interna_cierre=False, _exenta_doc_cliente=False, _docs_cierre=[], _cob_cierre="cobra",
                _cliente_acepta_nvv=False, v={"factura_tido": "NVV", "valor_origen": ""}, _OT_DOCS_NOTA_VENTA=("NVV", "NVI"),
                _OT_SALDO_ORIGENES_NO_DOC=(), _ot_finanzas=lambda v: {"cobre": {"total": 0}})

    def _bloquea(self, condicion, via, **extra):
        return bool(eval(condicion, {}, dict(self.BASE, _via_cierre=via, **extra)))

    def test_sin_factura_propia_la_pendiente_pasa_y_el_resto_sigue_igual(self):
        c = self._condicion(r"    if \(_gate_on and not _interna_cierre\s+and not _exenta_doc_cliente\s+and _via_cierre not in")
        for via, bloquea in (("pendiente_madre", False), ("cubierta_madre", False), ("documento", False), ("autorizacion_cerrar", False),
                             ("cobro_cero", False), ("contrato_real", False),
                             ("", True), ("nota_venta", True), ("cobro_cero_superadmin", True)):
            self.assertEqual(self._bloquea(c, via), bloquea, via)
        self.assertFalse(self._bloquea(c, "", _docs_cierre=[{"tido": "FCV"}]))                    # con documento propio no cambia nada
        self.assertFalse(self._bloquea(c, "", _interna_cierre=True))
        self.assertFalse(self._bloquea(c, "", _gate_on=False))

    def test_la_pendiente_no_declara_un_cobro_aparte_pero_cualquier_otra_ot_que_cobra_si(self):
        c = self._condicion(r"    if \(_gate_on and not _interna_cierre\s+and _cob_cierre == \"cobra\"\s+and _via_cierre != \"autorizacion_cerrar\"")
        self.assertFalse(self._bloquea(c, "pendiente_madre"))
        self.assertFalse(self._bloquea(c, "autorizacion_cerrar"))
        for via in ("", "documento", "cubierta_madre", "cobro_cero", "nota_venta"):
            self.assertTrue(self._bloquea(c, via), via)                                           # el candado de siempre
        self.assertFalse(self._bloquea(c, "", _ot_finanzas=lambda v: {"cobre": {"total": 90000}}))   # ya declaró su cobro
        self.assertFalse(self._bloquea(c, "", _cob_cierre="garantia"))

    def test_la_pendiente_no_consume_saldo_de_ninguna_factura_al_cerrar(self):
        c = self._condicion(r"    if \(_gate_on and not _interna_cierre and _cob_cierre == \"cobra\" and _via_cierre not in")
        self.assertFalse(self._bloquea(c, "pendiente_madre"))
        self.assertFalse(self._bloquea(c, "autorizacion_cerrar"))
        for via in ("", "documento", "cubierta_madre"):
            self.assertTrue(self._bloquea(c, via), via)                                           # el chequeo de saldo de siempre

    def test_la_nota_de_venta_sola_tampoco_frena_a_la_pendiente(self):
        c = self._condicion(r"    if \(_gate_on and not _interna_cierre and not _cliente_acepta_nvv")
        self.assertFalse(self._bloquea(c, "pendiente_madre"))
        self.assertTrue(self._bloquea(c, ""))
        self.assertFalse(self._bloquea(c, "", v={"factura_tido": "FCV"}))

    def test_lo_que_no_se_exime_sigue_exigiendo_a_la_pendiente(self):
        """Centro de costo, costo del técnico, pérdida y anexo: ningún candado de esos nombra la vía 'pendiente_madre'."""
        for codigo in ('"SIN_CENTRO_COSTO"', '"SIN_COSTO_PROVEEDOR"', '"ANEXO_DESACTUALIZADO"', '"CIERRE_CON_PERDIDA"', '"AUTORIZACION_PENDIENTE"'):
            i = self.SRC.index(codigo)
            j = self.SRC.rfind("    if ", 0, i)
            self.assertNotIn("pendiente_madre", self.SRC[j:i], codigo)
        self.assertEqual(self.SRC.count('"pendiente_madre"'), 4)                                   # justo: SIN_FACTURA, NOTA_VENTA, VALORIZAR y saldo
        self.assertIn("_fac_perdida_aplica(_via_cierre, _fp_c)", self.SRC)                         # la pérdida se calcula igual que siempre


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
        # va DESPUÉS de _ensure_otrep_medidas(): test_ot_medidas_diagnostico fija que esa vaya pegada a _ensure_ot_producto_estado()
        self.assertRegex(APP, r"_ensure_otrep_medidas\(\)[^\r\n]*\r?\n\s+_ensure_ot_que_paso\(\)")
        llamada = re.search(r"\n[ \t]+_ensure_ot_que_paso\(\)", APP)
        self.assertIsNotNone(llamada)
        self.assertLess(llamada.start(), APP.index("except Exception as _ensure_etapa3_err"))

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
        # A QUIÉN avisa la gestión lo decide _ot_gestion_destinatarios (ver TestGestionDestinatarios y TestAvisosDeLaGestion):
        # la campana ya no fija roles ni busca al creador por su cuenta.
        src = _fuente("_qp_campana")
        self.assertIn('"ot_pendiente_stock"', src)
        self.assertIn("uid == int(quien_uid)", src)                                     # quien lo registró no se avisa a sí mismo
        self.assertNotIn("role", src)
        self.assertIn("_ot_gestion_destinatarios(vid, fila=v)", _fuente("ot_api_que_paso_registrar"))

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

    def test_excel_y_proxima_visita_no_muestran_la_fecha_centinela(self):
        xl = _fuente("ot2_reporte_xlsx")
        self.assertIn('"Sin fecha" if _qp.es_sin_fecha(f.get("fecha_programada"))', xl)
        self.assertIn("pendientes_numeros", xl)
        self.assertIn('(f.get("ot_relacion") or "") == _qp.REL_PENDIENTE_STOCK', xl)    # no es «Desinstalación de …»
        self.assertEqual(APP.count("AND fecha_programada < '2099-01-01'"), 2)          # la «próxima visita» de la lista de clientes
        self.assertIn("hp.ot_relacion = 'pendiente_stock'", xl)

    def test_prueba_de_que_las_rutas_estan_registradas_una_vez(self):
        self.assertEqual(len(re.findall(r'@app\.route\("/ot/api/<int:vid>/que-paso", methods=\["GET"\]\)', APP)), 1)
        self.assertEqual(len(re.findall(r'@app\.route\("/ot/api/<int:vid>/que-paso", methods=\["POST"\]\)', APP)), 1)


# ═══════════════════════════════ 5b) lo que se cruza con main: correos a técnicos y «devolver a operativo» ═══════════════════════════════
import ot_correos_tecnicos as otc  # noqa: E402


class TestCorreosATecnicosConLaOtSinFecha(unittest.TestCase):
    """La OT pendiente nace con la fecha centinela: al técnico se le dice «por confirmar», nunca 31/12/2099 ni un archivo de calendario."""
    SIN = dt.date(2099, 12, 31)
    TEC = {"isabel@x.cl": "Isabel"}

    def test_la_fecha_centinela_es_sin_fecha_para_el_correo(self):
        self.assertIsNone(otc.a_fecha(self.SIN))
        self.assertIsNone(otc.a_fecha("2099-12-31"))
        self.assertIsNone(otc.a_fecha(dt.datetime(2099, 12, 31, 9, 0)))
        self.assertEqual(otc.a_fecha("2026-11-03"), dt.date(2026, 11, 3))            # una fecha real no cambia
        self.assertEqual(otc.rango_txt(self.SIN), "por confirmar")
        self.assertEqual(otc.fecha_txt(self.SIN), "")
        self.assertEqual(otc.foto("programada", self.SIN, None, None, None, self.TEC)["fecha"], "")

    def test_la_misma_fecha_centinela_en_las_dos_piezas(self):
        self.assertEqual(otc.ANIO_SIN_FECHA, qp.ANIO_SIN_FECHA)
        self.assertEqual(ev.ANIO_SIN_FECHA, qp.ANIO_SIN_FECHA)
        self.assertEqual(qp.FECHA_SIN_FECHA.year, qp.ANIO_SIN_FECHA)

    def test_sin_archivo_de_calendario_para_una_ot_sin_fecha(self):
        ot = {"id": 500, "numero_ot": "OT-2026-00180", "cliente": "Aurum", "fecha": self.SIN, "fecha_fin": None,
              "hora_inicio": None, "hora_fin": None}
        self.assertIsNone(otc.ics_evento(ot, "asignada", correo="isabel@x.cl", ts=1790000000))
        self.assertIsNotNone(otc.ics_evento(dict(ot, fecha=dt.date(2026, 11, 3)), "asignada", correo="isabel@x.cl", ts=1790000000))

    def test_asignar_tecnico_a_la_ot_ya_agendada_avisa_asignada_una_sola_vez(self):
        antes = otc.foto("programada", self.SIN, None, None, None, {})
        despues = otc.foto("programada", dt.date(2026, 11, 3), None, "09:00", "13:00", self.TEC)
        self.assertEqual([(a["email"], a["evento"]) for a in otc.decidir_avisos(antes, despues)], [("isabel@x.cl", "asignada")])

    def test_reagendar_sin_tecnico_no_avisa_a_nadie(self):
        antes = otc.foto("programada", self.SIN, None, None, None, {})
        despues = otc.foto("programada", dt.date(2026, 11, 3), None, None, None, {})
        self.assertEqual(otc.decidir_avisos(antes, despues), [])

    def test_si_el_tecnico_estaba_antes_de_la_fecha_el_cambio_dice_por_confirmar_antes(self):
        antes = otc.foto("programada", self.SIN, None, None, None, self.TEC)
        despues = otc.foto("programada", dt.date(2026, 11, 3), None, "09:00", None, self.TEC)
        self.assertEqual([a["evento"] for a in otc.decidir_avisos(antes, despues)], ["reagendada"])
        v = otc.construir_variables({"id": 500, "numero_ot": "OT-2026-00180", "cliente": "Aurum", "fecha": dt.date(2026, 11, 3),
                                     "hora_inicio": "09:00"}, nombre_tecnico="Isabel", antes=antes)
        self.assertEqual(v["fecha_antes"], "por confirmar")
        self.assertNotIn("2099", v["antes_ahora_html"])

    def test_cancelar_la_pendiente_sin_tecnico_no_avisa(self):
        antes = otc.foto("programada", self.SIN, None, None, None, {})
        despues = otc.foto("cancelada", self.SIN, None, None, None, {})
        self.assertEqual(otc.decidir_avisos(antes, despues), [])

    def test_la_creacion_de_la_pendiente_no_dispara_ningun_correo_a_tecnicos(self):
        for nombre in ("_qp_aplicar", "ot_api_que_paso_registrar"):
            src = _fuente(nombre)
            self.assertNotIn("_ot_tec_", src, nombre)         # nace sin técnico: nadie recibe nada hasta que gestión la agende


class TestDevolverAOperativoConOtPendiente(unittest.TestCase):
    """Un equipo que ya pasó a una OT pendiente no se devuelve a operativo en la madre: quedaría duplicado en las dos órdenes."""

    def _ruta(self, en_pend):
        amb = {"print": lambda *a, **k: None, "_qp": qp,
               "request": types.SimpleNamespace(get_json=lambda silent=True: {"motivo": "Apareció el equipo en bodega"}),
               "jsonify": lambda d=None, **kw: d if d is not None else kw,
               "_ot2_err": lambda msg, cod, http=400, **kw: {"ok": False, "error": msg, "error_codigo": cod, "http": http},
               "_otrep_cargar_ot_y_equipo": lambda vid, mid: ({"id": vid, "estado": "en_curso"}, {"nombre": "Pulsera"}, None),
               "_OTPROD_ESTADOS_FIN": ("cerrada", "completada", "cancelada", "anulada"), "_otpe_ok": lambda: True,
               "mysql_fetchone": lambda sql, params=(): ({"numero_ot": "OT-2026-00180"} if en_pend else None),
               "current_username": lambda: "aaron",
               "get_db": lambda: (_ for _ in ()).throw(RuntimeError("paso la guarda"))}
        _exec("ot2_api_equipo_no_esta_revertir", amb)
        return amb["ot2_api_equipo_no_esta_revertir"]

    def test_se_bloquea_con_409_y_dice_que_orden_cancelar(self):
        r = self._ruta(True)(10, 101)
        self.assertEqual((r["error_codigo"], r["http"]), ("EQUIPO_EN_OT_PENDIENTE", 409))
        self.assertIn("OT-2026-00180", r["error"])
        self.assertIn("cancela primero esa orden", r["error"])

    def test_sin_ot_pendiente_el_camino_de_siempre_sigue_igual(self):
        with self.assertRaises(RuntimeError):                  # llegó a abrir la base: la guarda no se interpuso
            self._ruta(False)(10, 101)

    def test_la_guarda_mira_solo_las_pendientes_vigentes_de_esta_ot(self):
        src = _fuente("ot2_api_equipo_no_esta_revertir")
        for pieza in ("h.ot_madre_id=%s AND h.ot_relacion=%s", "h.estado NOT IN ('cancelada','anulada')", "t.maquina_id=%s"):
            self.assertIn(pieza, src, pieza)
        self.assertEqual(_decoradores("ot2_api_equipo_no_esta_revertir")[1:], ["_mant_required", "_no_tecnico", "_ot_can_configurar"])   # nada se abrió


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

    def test_dice_que_un_equipo_marcado_falta_stock_pasa_con_todas_sus_tareas_aunque_figuren_hechas(self):
        # caso real Aurum OT-2026-00171 (21-oct): la persona decide con eso a la vista (REGLA #32)
        self.assertIn("e.todas_marcadas_hechas", JS)
        self.assertIn("aunque figuren como hechas", JS)
        self.assertIn("pasan todas sus tareas", JS)

    def test_pide_confirmacion_antes_de_crear_la_ot_pendiente_y_dice_que_no_se_cobra_de_nuevo(self):
        self.assertIn("no se cobra de nuevo", JS)
        self.assertIn("Al cliente <strong>no</strong> se le envía ningún correo.", JS)
        self.assertIn("Al cliente no se le envía nada", JS)

    def test_el_modal_no_envia_nada_al_cliente_ni_pide_montos(self):
        self.assertIsNone(re.search(r"factura|saldo|costo|proveedor|\$\s?\d", JS, re.I))
        self.assertNotIn("avisar_cliente", JS)

    def test_una_orden_sin_cliente_no_ofrece_dejar_pendiente(self):
        self.assertIn("D.ot.tiene_cliente === false", JS)
        self.assertIn("a.crea_pendiente", JS)

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

    def test_la_barra_de_acciones_ofrece_seguir_con_que_paso_sin_quitar_nada(self):
        acc = _leer("static/ot_acciones.js")
        self.assertIn('data-a="quepaso"', acc)
        self.assertIn("window.OTQuePaso.abrir('falta_stock')", acc)
        self.assertIn("s.op === 'falta_stock' && window.OTQuePaso", acc)            # solo con «No llegó · falta stock» y si el modal existe
        self.assertIn('data-a="volverrec">Volver a la OT</button>', acc)            # el botón de siempre sigue
        self.assertIsNone(re.search(r"(?<![A-Za-z.])(alert|confirm|prompt)\s*\(", acc[acc.index("quepaso:"):acc.index("quepaso:") + 300]))

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
