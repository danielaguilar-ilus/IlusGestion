# -*- coding: utf-8 -*-
"""🧬 HISTORIA DEL EQUIPO (hoja de vida unificada) — Daniel, 2026-10-21:
«Si entro a determinado producto, una trotadora de determinada cuenta, me va a decir todos los diagnósticos que ha tenido, visitas,
solicitudes de repuesto, todo bien ordenadito. La OT va a alimentar la ficha y a ordenar y clasificar la información SIN duplicarla.»

Se vigila:
  1. UNA línea de tiempo con todo (visitas, diagnósticos, fallas del checklist, hallazgos, repuestos, fuera de servicio y bajas, cambios de
     ficha, tickets, garantía), del más nuevo al más antiguo, en HORA CHILE (REGLA #6).
  2. CERO duplicados aunque varias tablas registren el mismo hecho (reglas D1–D8 de maquina_historia.py), sin perder el estado de gestión ni las fotos.
  3. Paginación real, filtros por tipo y por fechas (REGLA #4.3), contadores por tipo.
  4. CONFIDENCIALIDAD (REGLAS #19 y #26): un técnico —interno, elevado o externo— no recibe hallazgos, tickets, quién hizo nada, notas de
     gestión, proveedor, costo, N° de OC ni ticket de compra; el «quién» de un diagnóstico es solo del superadmin. El dato se quita en el SERVIDOR.
  5. Que la lectura de app.py no escriba nada, no pida nada que no deba y no se caiga si una tabla falta; y que las plantillas y el JS funcionen.

Sin BD real, sin Flask y sin importar app.py (pesa ~7 MB): el módulo puro se prueba directo y las funciones de app.py se extraen con
tests/_fuente_app.py y se ejecutan contra una base SQLite de mentira.
Correr:  python -m pytest tests/test_maquina_historia.py -q
"""
import datetime as dt
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import types
import unittest

import jinja2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import maquina_historia as MH  # noqa: E402
import ot_hallazgos as OH  # noqa: E402
from tests._fuente_app import RAIZ, cargar, decoradores, fuente_constante, fuente_funcion, texto  # noqa: E402

HAY_NODE = shutil.which("node") is not None
U = dt.datetime                      # instantes UTC (como los guarda MySQL con NOW())
D = dt.date
AHORA = U(2026, 10, 21, 15, 0)       # 12:00 en Chile (UTC-3 en octubre)
HOY = D(2026, 10, 21)
GESTION = MH.Rol(False, False)
SUPER = MH.Rol(False, True)
TECNICO = MH.Rol(True, False)
TECNICO_QUE_ES_SUPER = MH.Rol(True, True)      # un técnico nunca es superadmin a efectos de esta vista


def _leer(rel):
    with open(os.path.join(RAIZ, rel.replace("/", os.sep)), encoding="utf-8", newline="") as fh:
        return fh.read().replace("\r\n", "\n")


# ───────────────────────────── fábricas de filas con la forma de las tablas reales ─────────────────────────────
def equipo(**kw):
    f = {"id": 9, "cliente_id": 3, "razon_social": "Gimnasio Fit SpA", "nombre": "Trotadora X-1000 Pro", "sku": "TR-1000", "serie": "SN-123",
         "marca": "ILUS", "modelo": "X-1000", "estado": "activo", "estado_capturado": "operativo", "ubicacion_sala": "Sala cardio",
         "fecha_instalacion": D(2026, 1, 10), "fecha_fin_garantia": D(2027, 1, 10), "garantia_origen": "calculada",
         "doc_origen": "FCV 1234", "created_at": U(2026, 1, 5, 13, 0), "created_by": "Aarón"}
    f.update(kw)
    return f


def visita(id, **kw):
    f = {"id": id, "numero_ot": "OT-2026-%05d" % id, "titulo": "Mantención", "tipo": "preventiva", "estado": "cerrada",
         "fecha_programada": D(2026, 4, 2), "hora_inicio": None, "hora_real_inicio": None, "fecha_realizada": None,
         "cerrada_at": U(2026, 4, 2, 20, 0), "ot_madre_id": None, "ot_relacion": None, "tecnico_nombre": "Lenin Urbina"}
    f.update(kw)
    return f


def diag(vid, **kw):
    f = {"visita_id": vid, "estado_revision": "verificado", "razon_saltado": None, "observacion_tecnico": None,
         "diagnostico_estado": None, "diagnostico_texto": None, "revisado_at": U(2026, 4, 2, 19, 0), "revisado_por": "Lenin Urbina"}
    f.update(kw)
    return f


def tarea(id, vid, valor, **kw):
    f = {"id": id, "visita_id": vid, "titulo": "Estado de la cinta", "tipo_respuesta": "verificacion",
         "valor_json": json.dumps({"valor": valor}), "opciones_lista_json": None, "completada_at": U(2026, 10, 7, 19, 20),
         "completada_por": "Lenin Urbina", "observaciones": None}
    f.update(kw)
    return f


def hallazgo(id, vid, **kw):
    f = {"id": id, "visita_id": vid, "origen": "tarea", "referencia": "t70", "tarea_id": 70, "severidad": "falla",
         "titulo": "Trotadora X-1000 Pro · Estado de la cinta: Falla", "detalle": None, "riesgo": 0, "fotos_json": None,
         "estado": "abierto", "detectado_por": "Lenin Urbina", "detectado_en": U(2026, 10, 7, 19, 21), "tomado_por": None,
         "tomado_en": None, "ot_seguimiento_id": None, "seguimiento_en": None, "descartado_por": None, "descartado_en": None,
         "descarte_motivo": None, "retirado_en": None, "seg_numero": None, "seg_estado": None, "seg_cerrada": None}
    f.update(kw)
    return f


def solicitud(id, vid, **kw):
    f = {"id": id, "visita_id": vid, "ot_generada_id": None, "repuesto_nombre": "Cinta (banda)", "repuesto_sku": "REP-CINTA", "cantidad": 1,
         "origen": "bodega", "estado": "solicitado", "motivo": "Cinta desgastada, se ve la tela", "dejo_fuera_servicio": 0,
         "solicitado_por": "Lenin Urbina", "created_at": U(2026, 10, 7, 19, 40), "validado_at": None, "pedido_at": None,
         "recibido_at": None, "instalado_at": None, "updated_at": U(2026, 10, 7, 19, 40)}
    f.update(kw)
    return f


def parada(id, **kw):
    f = {"id": id, "inicio_at": U(2026, 10, 7, 19, 30), "fin_at": None, "inicio_aprox": 0, "estado_inicio": "fuera_servicio",
         "estado_fin": None, "motivo": "Motor no enciende", "abierta_por": "Lenin Urbina", "cerrada_por": None,
         "ref_tabla": "mant_visitas", "ref_id": 513, "ref_fin_tabla": None, "ref_fin_id": None}
    f.update(kw)
    return f


def evento(id, tipo, **kw):
    f = {"id": id, "tipo": tipo, "descripcion": "Evento", "fecha_evento": U(2026, 10, 8, 12, 0), "referencia_tabla": None,
         "referencia_id": None, "metadata_json": None, "created_by": "sistema"}
    f.update(kw)
    return f


def audit(id, campo, antes, nuevo, **kw):
    f = {"id": id, "campo": campo, "valor_antes": antes, "valor_nuevo": nuevo, "motivo": "Placa leída en terreno",
         "usuario": "Aarón", "fecha": U(2026, 9, 15, 14, 0)}
    f.update(kw)
    return f


def ticket(id, **kw):
    f = {"id": id, "numero_ticket": "TK-2026-%05d" % id, "tipo": "repair", "estado": "open", "titulo": "Fuera de servicio: Trotadora",
         "prioridad": "urgente", "created_at": U(2026, 10, 7, 19, 35), "cerrado_at": None, "created_by": "sistema"}
    f.update(kw)
    return f


def fuentes(**kw):
    f = {"equipo": equipo(), "visitas": [], "visita_equipos": [], "tareas": [], "hallazgos": [], "solicitudes": [], "paradas": [],
         "eventos": [], "auditoria": [], "tickets": [], "lev_visitas": [], "lev_visita": {}, "numeros_ot": {}, "visitas_visibles": None,
         "hoy": HOY, "ahora": AHORA}
    f.update(kw)
    return f


def construir(rol=GESTION, **kw):
    return MH.construir(fuentes(**kw), rol)


def por_id(es):
    return {e["id"]: e for e in es}


def ids(es):
    return [e["id"] for e in es]


def todo_el_texto(obj):
    return json.dumps(obj, ensure_ascii=False, default=str)


# ══════════════════════════════════════════ A) el módulo puro ══════════════════════════════════════════
class TestCatalogo(unittest.TestCase):
    def test_tipos_con_icono_que_existe(self):
        css = _leer("static/bootstrap-icons.css")
        self.assertEqual(set(MH.TIPOS), {"visita", "diagnostico", "checklist", "hallazgo", "repuesto", "estado", "ficha", "ticket", "garantia"})
        for k, t in MH.TIPOS.items():
            self.assertTrue(t["texto"], k)
            self.assertIn(".%s::before" % t["icono"], css, "%s: el ícono %s no existe en bootstrap-icons local" % (k, t["icono"]))

    def test_solo_gestion_son_hallazgos_y_tickets(self):
        self.assertEqual({k for k, t in MH.TIPOS.items() if t["solo_gestion"]}, {"hallazgo", "ticket"})
        self.assertEqual(MH.tipos_permitidos(TECNICO), tuple(k for k in MH.ORDEN_TIPOS if k not in ("hallazgo", "ticket")))
        self.assertEqual(MH.tipos_permitidos(GESTION), MH.ORDEN_TIPOS)

    def test_los_textos_de_ot_cubren_todos_los_estados_y_tipos_de_app(self):
        amb = cargar(constantes=["_OT2_ESTADO_META", "_HV_TIPO_OT_LABEL"], extra={})
        for estado in amb["_OT2_ESTADO_META"]:
            self.assertIn(estado, MH.ESTADO_OT, "estado de OT sin texto en la historia: %s" % estado)
        for tipo, label in amb["_HV_TIPO_OT_LABEL"].items():
            self.assertEqual(MH.TIPO_OT_TEXTO.get(tipo), label, tipo)

    def test_la_marca_de_riesgo_es_la_misma_que_usa_la_parada_en_app(self):
        amb = cargar(constantes=["_FS_MARCA_RIESGO"], extra={})
        self.assertEqual(MH.MARCA_RIESGO, amb["_FS_MARCA_RIESGO"])

    def test_los_estados_de_parada_son_los_de_app(self):
        amb = cargar(constantes=["_MAQ_ESTADOS_PARADA"], extra={})
        self.assertEqual(tuple(MH.ESTADOS_PARADA), tuple(amb["_MAQ_ESTADOS_PARADA"]))

    def test_las_razones_de_saltado_validas_tienen_texto(self):
        amb = cargar(constantes=["_RAZONES_SALTADO_VALIDAS"], extra={})
        for r in amb["_RAZONES_SALTADO_VALIDAS"]:
            self.assertIn(r, MH.RAZON_SALTADO_TEXTO, r)

    def test_los_estados_de_repuesto_abiertos_son_los_de_app(self):
        amb = cargar(constantes=["_OTREP_ABIERTOS"], extra={})
        self.assertEqual(tuple(MH.REPUESTO_ABIERTOS), tuple(amb["_OTREP_ABIERTOS"]))

    def test_ningun_texto_del_tecnico_nombra_al_proveedor(self):
        for estado, t in MH.REPUESTO_TEXTO_TECNICO.items():
            self.assertNotRegex(t.lower(), r"proveedor|compra|oc\b|costo", estado)


class TestFechas(unittest.TestCase):
    def test_utc_a_chile_con_horario_de_verano_y_de_invierno(self):
        self.assertEqual(MH.fmt_fecha(U(2026, 10, 7, 16, 30)), "07/10/2026 13:30")      # octubre: UTC-3
        self.assertEqual(MH.fmt_fecha(U(2026, 6, 15, 16, 30)), "15/06/2026 12:30")      # junio: UTC-4
        self.assertEqual(MH.fmt_fecha(U(2026, 1, 5, 2, 30)), "04/01/2026 23:30")        # el día de Chile, no el UTC

    def test_un_dia_sin_hora_se_muestra_solo_como_fecha(self):
        ts, solo = MH.instante(D(2026, 10, 7))
        self.assertTrue(solo)
        self.assertEqual(MH.fmt_fecha(ts, solo), "07/10/2026")
        self.assertEqual(MH.dia_chile(ts), D(2026, 10, 7))

    def test_un_dia_con_hora_local_se_convierte(self):
        ts, solo = MH.instante(D(2026, 10, 7), dt.timedelta(hours=9, minutes=30))        # TIME de MySQL (pymysql: timedelta)
        self.assertFalse(solo)
        self.assertEqual(MH.fmt_fecha(ts), "07/10/2026 09:30")
        ts2, solo2 = MH.instante(D(2026, 10, 7), "14:05:00")
        self.assertEqual((MH.fmt_fecha(ts2), solo2), ("07/10/2026 14:05", False))

    def test_nunca_formato_iso_crudo_ni_en_ingles(self):
        for ts in (U(2026, 10, 7, 16, 30), U(2026, 12, 31, 23, 59)):
            s = MH.fmt_fecha(ts)
            self.assertRegex(s, r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$")
        self.assertEqual(MH.mes_texto(U(2026, 10, 7, 16, 30)), "octubre de 2026")
        self.assertEqual(MH.mes_texto(U(2026, 3, 1, 12, 0)), "marzo de 2026")
        self.assertEqual(MH.fmt_fecha(None), "")
        self.assertEqual(MH.fmt_fecha("2026-10-07"), "")

    def test_la_duracion_es_la_misma_de_la_hoja_de_vida_de_app(self):
        amb = cargar(funciones=["_hv_fmt_dur"], extra={})
        for h in (0, 0.2, 0.5, 1, 1.34, 5.75, 23.9, 47.9, 48, 50.5, 100, 727.3, None, "x"):
            self.assertEqual(MH.fmt_duracion(h), amb["_hv_fmt_dur"](h), repr(h))

    def test_hace_texto(self):
        ahora = U(2026, 10, 21, 15, 0)
        self.assertEqual(MH.hace_texto(U(2026, 10, 21, 1, 0), ahora), "hoy")
        self.assertEqual(MH.hace_texto(U(2026, 10, 20, 1, 0), ahora), "hace 1 día")
        self.assertEqual(MH.hace_texto(U(2026, 10, 7, 1, 0), ahora), "hace 14 días")
        self.assertEqual(MH.hace_texto(U(2026, 5, 7, 1, 0), ahora), "hace 5 meses")

    def test_fecha_iso_estricta(self):
        self.assertEqual(MH.fecha_iso("2026-10-21"), D(2026, 10, 21))
        for malo in ("21/10/2026", "2026-13-01", "2026-02-30", "", None, "hola", "2026-10-21; DROP TABLE", 5):
            self.assertIsNone(MH.fecha_iso(malo), repr(malo))


class TestVisitas(unittest.TestCase):
    def test_una_visita_cerrada_dice_numero_tipo_tecnico_y_cierre(self):
        es = construir(visitas=[visita(200)], numeros_ot={200: "OT-2026-00200"})
        v = por_id(es)["visita:200"]
        self.assertEqual(v["tipo"], "visita")
        self.assertEqual(v["titulo"], "OT-2026-00200 · Mantención preventiva")
        self.assertEqual(v["severidad"], "ok")
        self.assertEqual(v["estado_txt"], "Cerrada")
        j = MH.a_json(v, ahora=AHORA)
        self.assertEqual(j["fecha"], "02/04/2026")                                   # solo el día: la ficha no sabe la hora
        textos = [l["texto"] for l in j["lineas"]]
        self.assertIn("Técnico: Lenin Urbina", textos)
        self.assertIn("Cerrada el 02/04/2026 17:00", textos)                          # 20:00 UTC = 17:00 en Chile (aún con horario de verano, UTC-3)
        self.assertEqual(j["ref"]["url"], "/ot/200")
        self.assertEqual(j["ref"]["texto"], "Ver OT OT-2026-00200")

    def test_la_hora_real_de_inicio_manda_sobre_la_fecha_programada(self):
        es = construir(visitas=[visita(513, tipo="correctiva", estado="en_curso", fecha_programada=D(2026, 10, 6),
                                       hora_real_inicio=U(2026, 10, 7, 16, 30), cerrada_at=None)])
        j = MH.a_json(por_id(es)["visita:513"], ahora=AHORA)
        self.assertEqual(j["fecha"], "07/10/2026 13:30")
        self.assertEqual(j["severidad"], "info")
        self.assertEqual(j["estado_txt"], "En curso")

    def test_una_visita_futura_se_marca_proxima(self):
        es = construir(equipo=equipo(fecha_fin_garantia=None),
                       visitas=[visita(600, estado="programada", fecha_programada=D(2026, 11, 3), cerrada_at=None, tipo="inspeccion")])
        j = MH.a_json(por_id(es)["visita:600"], ahora=AHORA)
        self.assertTrue(j["futuro"])
        self.assertIn("próxima", j["estado_txt"])
        self.assertEqual(es[0]["id"], "visita:600", "lo próximo va arriba de todo")

    def test_estados_que_piden_mirar_van_en_ambar(self):
        for estado, sev in (("pendiente_repuesto", "aviso"), ("pendiente_info", "aviso"), ("cancelada", "info"), ("anulada", "info"),
                            ("programada", "info"), ("completada", "ok"), ("pendiente_aprobacion", "ok")):
            es = construir(visitas=[visita(1, estado=estado)])
            self.assertEqual(por_id(es)["visita:1"]["severidad"], sev, estado)

    def test_la_ot_de_seguimiento_y_la_pendiente_dicen_de_quien_vienen(self):
        es = construir(visitas=[visita(530, tipo="correctiva", ot_madre_id=513, ot_relacion="seguimiento"),
                                visita(531, ot_madre_id=513, ot_relacion="pendiente_stock"),
                                visita(532, tipo="desinstalacion", ot_madre_id=100, ot_relacion="desinstalacion")],
                       numeros_ot={513: "OT-2026-00513", 100: "OT-2026-00100"})
        d = por_id(es)
        self.assertIn("Seguimiento de OT-2026-00513", [l["texto"] for l in d["visita:530"]["lineas"]])
        self.assertIn("Pendiente por falta de stock de OT-2026-00513", [l["texto"] for l in d["visita:531"]["lineas"]])
        self.assertIn("Desinstalación de OT-2026-00100", [l["texto"] for l in d["visita:532"]["lineas"]])

    def test_sin_numero_conocido_se_usa_el_formato_V(self):
        es = construir(visitas=[visita(77, numero_ot=None)])
        self.assertEqual(por_id(es)["visita:77"]["ref"]["numero"], "V-00077")

    def test_el_levantamiento_se_nota(self):
        es = construir(visitas=[visita(40, tipo="levantamiento")], lev_visitas=[40])
        self.assertIn("Se levantó la ficha del equipo en esta visita", [l["texto"] for l in por_id(es)["visita:40"]["lineas"]])

    def test_una_visita_sin_ninguna_fecha_se_omite_sin_romper(self):
        es = construir(visitas=[visita(5, fecha_programada=None, cerrada_at=None), visita(6)])
        self.assertEqual([i for i in ids(es) if i.startswith("visita:")], ["visita:6"])


class TestDiagnosticos(unittest.TestCase):
    def test_veredictos(self):
        casos = (("aprobado", "ok", "Diagnóstico: Aprobado"), ("observacion", "aviso", "Diagnóstico: Con observación"),
                 ("falla", "falla", "Diagnóstico: Con falla"))
        for veredicto, sev, titulo in casos:
            es = construir(visitas=[visita(200)], visita_equipos=[diag(200, diagnostico_estado=veredicto, diagnostico_texto="Ruido en el rodillo")])
            t = por_id(es)["diag:200"]
            self.assertEqual((t["severidad"], t["titulo"], t["detalle"]), (sev, titulo, "Ruido en el rodillo"))

    def test_verificado_a_secas_no_es_un_diagnostico_es_una_linea_de_la_visita(self):          # D8
        es = construir(visitas=[visita(200)], visita_equipos=[diag(200)])
        self.assertNotIn("diag:200", ids(es))
        self.assertIn("En esta visita: revisado sin novedad", [l["texto"] for l in por_id(es)["visita:200"]["lineas"]])

    def test_una_revision_sin_hora_de_revision_no_cuenta(self):
        self.assertFalse(MH.diagnostico_genera_tarjeta(diag(200, revisado_at=None, diagnostico_estado="falla")))
        self.assertTrue(MH.diagnostico_genera_tarjeta(diag(200, observacion_tecnico="Se escucha raro")))
        self.assertTrue(MH.diagnostico_genera_tarjeta(diag(200, estado_revision="saltado")))
        self.assertFalse(MH.diagnostico_genera_tarjeta(diag(200, observacion_tecnico="   ")))

    def test_no_se_pudo_revisar_dice_por_que(self):
        es = construir(visita_equipos=[diag(200, estado_revision="saltado", razon_saltado="falta_stock", observacion_tecnico="No llegó el equipo")])
        t = por_id(es)["diag:200"]
        self.assertEqual(t["titulo"], "No se pudo revisar: No llegó · falta stock")
        self.assertEqual(t["severidad"], "aviso")
        self.assertEqual(t["detalle"], "No llegó el equipo")

    def test_texto_y_observacion_se_juntan_sin_repetirse(self):
        es = construir(visita_equipos=[diag(200, diagnostico_estado="observacion", diagnostico_texto="Rodillo flojo", observacion_tecnico="Rodillo flojo")])
        self.assertEqual(por_id(es)["diag:200"]["detalle"], "Rodillo flojo")
        es = construir(visita_equipos=[diag(200, diagnostico_estado="observacion", diagnostico_texto="Rodillo flojo", observacion_tecnico="Apretar en 3 meses")])
        self.assertEqual(por_id(es)["diag:200"]["detalle"], "Rodillo flojo\nApretar en 3 meses")

    def test_quien_hizo_el_diagnostico_es_solo_del_superadmin(self):
        f = dict(visitas=[visita(200)], visita_equipos=[diag(200, diagnostico_estado="aprobado")])
        self.assertEqual(por_id(construir(SUPER, **f))["diag:200"]["actor"], "Lenin Urbina")
        self.assertIsNone(por_id(construir(GESTION, **f))["diag:200"]["actor"], "Daniel: «jamás se enteren o lo vean… los demás»")
        self.assertIsNone(por_id(construir(TECNICO, **f))["diag:200"]["actor"])
        self.assertIsNone(por_id(construir(TECNICO_QUE_ES_SUPER, **f))["diag:200"]["actor"])


class TestChecklist(unittest.TestCase):
    def test_falla_y_alerta_del_checklist(self):
        es = construir(visitas=[visita(513)], tareas=[tarea(70, 513, "falla"), tarea(71, 513, "alerta", titulo="Tensión de la cinta"),
                                                      tarea(72, 513, "aprobado", titulo="Limpieza")])
        d = por_id(es)
        self.assertEqual((d["tarea:70"]["severidad"], d["tarea:70"]["titulo"]), ("falla", "Estado de la cinta: Falla"))
        self.assertEqual((d["tarea:71"]["severidad"], d["tarea:71"]["titulo"]), ("aviso", "Tensión de la cinta: Alerta"))
        self.assertNotIn("tarea:72", d, "una tarea aprobada no es una falla")

    def test_no_aplica_no_cuenta(self):
        es = construir(tareas=[tarea(70, 513, "falla", valor_json=json.dumps({"valor": "falla", "na": True}))])
        self.assertNotIn("tarea:70", ids(es))

    def test_el_resultado_del_control_se_trata_igual_que_un_hallazgo(self):
        opciones = json.dumps(["Apto", "Apto con observaciones", "Reparar", "Dar de baja"])
        es = construir(tareas=[tarea(80, 513, None, tipo_respuesta="lista", titulo="Resultado del control", opciones_lista_json=opciones,
                                     valor_json=json.dumps({"opcion": "Reparar"})),
                               tarea(81, 513, None, tipo_respuesta="lista", titulo="Resultado del control", opciones_lista_json=opciones,
                                     valor_json=json.dumps({"opcion": "Apto"})),
                               tarea(82, 513, None, tipo_respuesta="lista", titulo="Color", opciones_lista_json=json.dumps(["Rojo", "Negro"]),
                                     valor_json=json.dumps({"opcion": "Rojo"}))])
        d = por_id(es)
        self.assertEqual(d["tarea:80"]["severidad"], "falla")
        self.assertNotIn("tarea:81", d)
        self.assertNotIn("tarea:82", d, "una lista cualquiera no es un hallazgo")

    def test_la_fecha_es_la_de_la_respuesta_o_la_de_la_visita(self):
        es = construir(visitas=[visita(513, hora_real_inicio=U(2026, 10, 7, 16, 30), cerrada_at=None)],
                       tareas=[tarea(70, 513, "falla"), tarea(71, 513, "falla", completada_at=None)])
        d = por_id(es)
        self.assertEqual(MH.a_json(d["tarea:70"])["fecha"], "07/10/2026 16:20")
        self.assertEqual(MH.a_json(d["tarea:71"])["fecha"], "07/10/2026 13:30", "sin hora de respuesta cae a la de la visita")

    def test_quien_la_respondio_solo_el_superadmin(self):
        f = dict(tareas=[tarea(70, 513, "falla")])
        self.assertEqual(por_id(construir(SUPER, **f))["tarea:70"]["actor"], "Lenin Urbina")
        self.assertIsNone(por_id(construir(GESTION, **f))["tarea:70"]["actor"])
        self.assertIsNone(por_id(construir(TECNICO, **f))["tarea:70"]["actor"])


class TestHallazgos(unittest.TestCase):
    """Lo que el técnico reportó y la gestión sigue (mant_ot_hallazgos). SOLO gestión."""

    def test_resuelto_con_su_ot_de_seguimiento_cerrada_termina_en_verde(self):
        h = hallazgo(5, 513, estado="resuelto", ot_seguimiento_id=530, seg_numero="OT-2026-00530", seg_estado="cerrada",
                     seg_cerrada=U(2026, 10, 17, 18, 0), tomado_por="Víctor", tomado_en=U(2026, 10, 8, 12, 0),
                     seguimiento_en=U(2026, 10, 8, 13, 0))
        t = por_id(construir(hallazgos=[h], numeros_ot={513: "OT-2026-00513", 530: "OT-2026-00530"}))["hallazgo:5"]
        self.assertEqual((t["severidad"], t["abierto"], t["estado_txt"]), ("ok", False, "Con OT de seguimiento"))
        self.assertEqual(t["fin_ts"], U(2026, 10, 17, 18, 0))
        self.assertEqual([p["texto"] for p in t["pasos"]],
                         ["Reportado por Lenin Urbina", "Caso tomado por Víctor", "OT de seguimiento OT-2026-00530 (cerrada)"])
        self.assertEqual(t["refs"][0]["texto"], "Ver OT de seguimiento OT-2026-00530")
        self.assertEqual(t["refs"][0]["url"], "/ot/530")
        j = MH.a_json(t, ahora=AHORA)
        self.assertEqual(j["fecha_fin"], "17/10/2026 15:00")

    def test_con_la_ot_de_seguimiento_en_curso_sigue_abierto_pero_en_ambar(self):
        h = hallazgo(5, 513, estado="resuelto", ot_seguimiento_id=530, seg_numero="OT-2026-00530", seg_estado="en_curso")
        t = por_id(construir(hallazgos=[h]))["hallazgo:5"]
        self.assertEqual((t["severidad"], t["abierto"]), ("aviso", True))
        self.assertIsNone(t["fin_ts"])

    def test_una_ot_de_seguimiento_cancelada_deja_el_hallazgo_otra_vez_sin_gestionar(self):
        h = hallazgo(5, 513, estado="resuelto", ot_seguimiento_id=530, seg_numero="OT-2026-00530", seg_estado="cancelada")
        t = por_id(construir(hallazgos=[h]))["hallazgo:5"]
        self.assertEqual((t["estado_txt"], t["abierto"], t["severidad"]), ("Sin gestionar", True, "falla"))
        self.assertTrue(any("se canceló" in p["texto"] for p in t["pasos"]))
        self.assertFalse([r for r in t["refs"] if r["tabla"] == "mant_visitas"], "una OT cancelada no se ofrece como seguimiento")

    def test_descartado_queda_en_gris_con_su_motivo(self):
        h = hallazgo(5, 513, estado="descartado", descartado_por="Aarón", descartado_en=U(2026, 10, 9, 15, 0),
                     descarte_motivo="Era un error de tipeo del técnico")
        t = por_id(construir(hallazgos=[h]))["hallazgo:5"]
        self.assertEqual((t["severidad"], t["abierto"], t["fin_ts"]), ("info", False, U(2026, 10, 9, 15, 0)))
        self.assertIn("Motivo: Era un error de tipeo del técnico", [l["texto"] for l in t["lineas"]])
        self.assertIn("Descartado por Aarón", [p["texto"] for p in t["pasos"]])

    def test_con_el_caso_tomado_sigue_siendo_la_falla_que_es(self):
        h = hallazgo(5, 513, estado="tomado", tomado_por="Víctor", tomado_en=U(2026, 10, 8, 12, 0))
        t = por_id(construir(hallazgos=[h]))["hallazgo:5"]
        self.assertEqual((t["estado_txt"], t["estado_color"], t["abierto"], t["severidad"]), ("Caso tomado", "azul", True, "falla"))

    def test_los_que_el_tecnico_retiro_no_son_un_hecho(self):
        es = construir(hallazgos=[hallazgo(5, 513, retirado_en=U(2026, 10, 8, 12, 0))])
        self.assertEqual([i for i in ids(es) if i.startswith("hallazgo")], [])

    def test_alerta_e_incidencia_van_en_ambar_y_el_riesgo_se_dice(self):
        hs = [hallazgo(5, 513, severidad="alerta", tarea_id=71, referencia="t71", titulo="Tensión: Alerta"),
              hallazgo(6, 513, origen="equipo", severidad="incidencia", tarea_id=None, referencia="m9:inc:55", titulo="Incidencia 55: rodillo"),
              hallazgo(7, 513, origen="equipo", severidad="baja", tarea_id=None, referencia="m9:baja", riesgo=1, titulo="X: No tiene arreglo")]
        d = por_id(construir(hallazgos=hs))
        self.assertEqual(d["hallazgo:5"]["severidad"], "aviso")
        self.assertEqual(d["hallazgo:6"]["severidad"], "aviso")
        self.assertEqual((d["hallazgo:7"]["severidad"], d["hallazgo:7"]["riesgo"]), ("falla", True))

    def test_un_tecnico_no_recibe_ni_un_hallazgo(self):
        es = construir(TECNICO, hallazgos=[hallazgo(5, 513), hallazgo(6, 513, referencia="t71", tarea_id=71)],
                       tareas=[tarea(70, 513, "falla")])
        self.assertFalse([e for e in es if e["tipo"] == "hallazgo"])
        self.assertIn("tarea:70", ids(es), "lo que el técnico respondió en su checklist sí lo ve")

    def test_las_fotos_son_las_del_hallazgo_y_los_enlaces_malos_se_descartan(self):
        h = hallazgo(5, 513, fotos_json=json.dumps(["/f/a.jpg", "https://cdn.x/b.jpg", "javascript:alert(1)", "//evil.com/x.jpg", "/f/a.jpg"]))
        t = por_id(construir(hallazgos=[h]))["hallazgo:5"]
        self.assertEqual(MH.a_json(t)["fotos"], ["/f/a.jpg", "https://cdn.x/b.jpg"])


class TestDeduplicacion(unittest.TestCase):
    """Daniel: «SIN duplicarla». Cada hecho una sola vez, aunque lo registren varias tablas."""

    def test_D3_el_hallazgo_de_una_tarea_reemplaza_a_la_tarea(self):
        es = construir(tareas=[tarea(70, 513, "falla", observaciones="Se ve la tela")], hallazgos=[hallazgo(5, 513, fotos_json=json.dumps(["/f/h.jpg"]))])
        self.assertIn("hallazgo:5", ids(es))
        self.assertNotIn("tarea:70", ids(es))
        self.assertEqual(por_id(es)["hallazgo:5"]["detalle"], "Se ve la tela", "no se pierde lo que el técnico escribió en la tarea")
        # otra tarea sin hallazgo sigue en pie
        es = construir(tareas=[tarea(70, 513, "falla"), tarea(71, 513, "alerta")], hallazgos=[hallazgo(5, 513)])
        self.assertIn("tarea:71", ids(es))

    def test_D4_la_falla_detectada_reemplaza_al_diagnostico_con_falla_de_esa_visita(self):
        d = diag(513, diagnostico_estado="falla", diagnostico_texto="Cinta desgastada", estado_revision="falla_detectada")
        h = hallazgo(6, 513, origen="equipo", referencia="m9:falla", tarea_id=None, titulo="Trotadora X: Falla detectada", detalle="")
        es = construir(visita_equipos=[d], hallazgos=[h])
        self.assertIn("hallazgo:6", ids(es))
        self.assertNotIn("diag:513", ids(es))
        self.assertEqual(por_id(es)["hallazgo:6"]["detalle"], "Cinta desgastada")
        # con otra visita, el diagnóstico queda
        es = construir(visita_equipos=[diag(200, diagnostico_estado="falla", diagnostico_texto="x", estado_revision="falla_detectada")], hallazgos=[h])
        self.assertIn("diag:200", ids(es))
        # y un técnico (sin hallazgos) conserva su diagnóstico
        self.assertIn("diag:513", ids(construir(TECNICO, visita_equipos=[d], hallazgos=[h])))

    def test_D5_el_hallazgo_de_fuera_de_servicio_no_se_repite_va_en_la_parada(self):
        h = hallazgo(7, 513, origen="equipo", referencia="m9:fuera_servicio", tarea_id=None, severidad="fuera_servicio",
                     titulo="Trotadora X: No se puede usar — Motor no enciende", fotos_json=json.dumps(["/f/fs.jpg"]), riesgo=1)
        es = construir(paradas=[parada(1)], hallazgos=[h])
        self.assertNotIn("hallazgo:7", ids(es))
        p = por_id(es)["parada:1"]
        self.assertEqual(p["gestion"]["texto"], "Hallazgo: sin gestionar")
        self.assertEqual(MH.a_json(p)["fotos"], ["/f/fs.jpg"], "la evidencia del hallazgo no se pierde")
        self.assertTrue(p["riesgo"])
        # sin parada con qué cruzarlo, el hallazgo queda solo (no se pierde el hecho)
        self.assertIn("hallazgo:7", ids(construir(hallazgos=[h])))
        # una parada de otra visita y de hace meses no lo absorbe
        lejos = parada(2, inicio_at=U(2026, 3, 1, 12, 0), ref_id=200)
        self.assertIn("hallazgo:7", ids(construir(paradas=[lejos], hallazgos=[h])))

    def test_D5_el_hallazgo_de_repuesto_se_pega_a_las_solicitudes_de_esa_visita(self):
        h = hallazgo(8, 513, origen="equipo", referencia="m9:rep:abc12345", tarea_id=None, severidad="repuesto", estado="tomado",
                     tomado_por="Víctor", titulo="Trotadora X: Repuesto solicitado: Cinta × 1")
        es = construir(solicitudes=[solicitud(40, 513), solicitud(41, 200)], hallazgos=[h])
        self.assertNotIn("hallazgo:8", ids(es))
        d = por_id(es)
        self.assertEqual(d["sol:40"]["gestion"]["estado"], "Caso tomado")
        self.assertIsNone(d["sol:41"]["gestion"], "la solicitud de otra visita no se toca")
        self.assertIn("hallazgo:8", ids(construir(hallazgos=[h])))

    def test_D5_el_hallazgo_de_baja_se_pega_a_la_baja(self):
        p = parada(1, estado_fin="dado_baja", fin_at=U(2026, 10, 9, 15, 0), ref_fin_tabla="mant_visitas", ref_fin_id=513)
        h = hallazgo(9, 513, origen="equipo", referencia="m9:baja", tarea_id=None, severidad="baja", titulo="Trotadora X: No tiene arreglo",
                     fotos_json=json.dumps(["/f/baja.jpg"]))
        es = construir(paradas=[p], hallazgos=[h])
        self.assertNotIn("hallazgo:9", ids(es))
        b = por_id(es)["parada:1"]
        self.assertEqual((b["subtipo"], b["_baja"]), ("baja", True))
        self.assertEqual(MH.a_json(b)["fotos"], ["/f/baja.jpg"])

    def test_D1_los_eventos_que_cuenta_otra_tarjeta_no_se_repiten(self):
        evs = [evento(1, "reparacion", referencia_tabla="mant_visitas", referencia_id=513, descripcion="OT-513 · Correctiva"),
               evento(2, "instalacion", referencia_tabla="mant_visitas", referencia_id=100, descripcion="OT-100 · Instalación"),
               evento(3, "cambio_repuesto", referencia_tabla="mant_ot_repuesto_solicitudes", referencia_id=40, descripcion="Repuesto instalado: Cinta"),
               evento(4, "levantamiento", referencia_tabla="mant_levantamientos", referencia_id=12, descripcion="Levantamiento #12 cerrado")]
        es = construir(visitas=[visita(513, tipo="correctiva"), visita(100, tipo="instalacion")], solicitudes=[solicitud(40, 513, estado="instalado")],
                       eventos=evs, lev_visita={12: 513})
        self.assertFalse([i for i in ids(es) if i.startswith("evento:")], ids(es))
        # si lo que referencian NO está en la línea, el evento sí se muestra (no se pierde nada)
        es = construir(eventos=evs, lev_visita={12: 513})
        self.assertEqual(sorted(i for i in ids(es) if i.startswith("evento:")), ["evento:1", "evento:2", "evento:3", "evento:4"])

    def test_D2_fotos_otro_y_los_cambios_de_estado_de_las_paradas_son_ruido(self):
        evs = [evento(1, "foto_agregada", descripcion="Nueva foto"), evento(2, "otro", descripcion="Foto principal borrada"),
               evento(3, "cambio_estado", descripcion="Quedó fuera de servicio", metadata_json=json.dumps({"parada": "inicio", "origen": "ot"})),
               evento(4, "cambio_estado", descripcion="Volvió a operar tras 3 días", metadata_json=json.dumps({"parada": "fin"})),
               evento(5, "dado_baja", descripcion="Dado de baja después de 3 días", metadata_json=json.dumps({"parada": "fin"})),
               evento(6, "creado_por_levantamiento", descripcion="Equipo descubierto en terreno"),
               evento(7, "cambio_estado", descripcion="Restaurado (vuelve a activo) por Daniel", metadata_json=json.dumps({"fuente": "restaurar"}))]
        es = construir(eventos=evs)
        self.assertEqual([i for i in ids(es) if i.startswith("evento:")], ["evento:7"], "solo el restaurado es un hecho que nadie más cuenta")
        self.assertEqual(por_id(es)["evento:7"]["tipo"], "estado")

    def test_el_evento_de_baja_no_repite_la_baja_que_cuenta_la_parada(self):
        p = parada(1, estado_fin="dado_baja", fin_at=U(2026, 10, 9, 15, 0), ref_fin_tabla="mant_visitas", ref_fin_id=513)
        e = evento(9, "dado_baja", referencia_tabla="mant_visitas", referencia_id=513, descripcion="Dado de baja desde OT-513: sin arreglo")
        bajas = [x for x in construir(paradas=[p], eventos=[e]) if x["_baja"]]
        self.assertEqual(len(bajas), 1)
        # sin parada, el evento ES la baja
        bajas = [x for x in construir(eventos=[e]) if x["_baja"]]
        self.assertEqual([b["id"] for b in bajas], ["evento:9"])

    def test_D6_el_diagnostico_de_fuera_de_servicio_lo_cuenta_la_parada(self):
        d = diag(513, estado_revision="falla_detectada", razon_saltado="fuera_de_servicio", observacion_tecnico="Motor no enciende")
        self.assertNotIn("diag:513", ids(construir(visita_equipos=[d], paradas=[parada(1)])))
        self.assertIn("diag:513", ids(construir(visita_equipos=[d])), "sin parada, el diagnóstico queda")

    def test_D6_el_diagnostico_de_baja_lo_cuenta_la_baja_o_se_vuelve_la_baja(self):
        d = diag(513, estado_revision="saltado", razon_saltado="dado_de_baja", observacion_tecnico="Baja: Estructura quebrada")
        p = parada(1, estado_fin="dado_baja", fin_at=U(2026, 10, 9, 15, 0), ref_fin_tabla="mant_visitas", ref_fin_id=513)
        es = construir(visita_equipos=[d], paradas=[p])
        self.assertEqual(len([x for x in es if x["_baja"]]), 1)
        self.assertNotIn("diag:513", ids(es))
        es = construir(visita_equipos=[d])                                            # la baja que nadie más contó
        b = por_id(es)["baja:513"]
        self.assertEqual((b["tipo"], b["subtipo"], b["titulo"], b["detalle"]), ("estado", "baja", "Dado de baja", "Estructura quebrada"))

    def test_D7_el_cambio_de_estado_de_la_ficha_que_ya_cuenta_una_parada_se_descarta(self):
        a = [audit(1, "estado_capturado", "operativo", "fuera_servicio"), audit(2, "estado_capturado", "fuera_servicio", "operativo"),
             audit(3, "estado_capturado", "operativo", "advertencia")]
        es = construir(auditoria=a)
        self.assertEqual([i for i in ids(es) if i.startswith("audit:")], ["audit:3"])

    def test_la_baja_de_la_ficha_no_repite_la_que_ya_esta_contada(self):
        a = audit(4, "estado", "activo", "baja", fecha=U(2026, 10, 9, 15, 5), motivo="Sin arreglo")
        p = parada(1, estado_fin="dado_baja", fin_at=U(2026, 10, 9, 15, 0), ref_fin_tabla="mant_visitas", ref_fin_id=513)
        self.assertEqual(len([x for x in construir(paradas=[p], auditoria=[a]) if x["_baja"]]), 1)
        self.assertEqual([x["id"] for x in construir(auditoria=[a]) if x["_baja"]], ["audit:4"])

    def test_cada_id_aparece_una_sola_vez(self):
        es = construir(visitas=[visita(200), visita(513, tipo="correctiva")], visita_equipos=[diag(200, diagnostico_estado="aprobado")],
                       tareas=[tarea(70, 513, "falla")], hallazgos=[hallazgo(5, 513)], solicitudes=[solicitud(40, 513)], paradas=[parada(1)],
                       auditoria=[audit(1, "serie", "A", "B")], tickets=[ticket(3)])
        self.assertEqual(len(ids(es)), len(set(ids(es))))


class TestSolicitudesDeRepuesto(unittest.TestCase):
    def test_gestion_ve_todo_el_avance_con_fechas_en_hora_chile(self):
        s = solicitud(40, 513, estado="recibido", validado_at=U(2026, 10, 8, 12, 0), pedido_at=U(2026, 10, 9, 12, 0),
                      recibido_at=U(2026, 10, 14, 12, 0), repuesto_nombre="Cinta (banda)", cantidad=2)
        t = por_id(construir(solicitudes=[s], numeros_ot={513: "OT-2026-00513"}))["sol:40"]
        self.assertEqual(t["titulo"], "Repuesto: Cinta (banda) × 2")
        self.assertEqual((t["severidad"], t["abierto"], t["estado_txt"]), ("aviso", True, "Recibido en bodega, falta instalarlo"))
        self.assertEqual([(p["texto"], p["fecha"], p["hecho"]) for p in t["pasos"]],
                         [("Solicitado", "07/10/2026 16:40", True), ("Validado en bodega", "08/10/2026 09:00", True),
                          ("Pedido, en camino a bodega", "09/10/2026 09:00", True), ("Pieza lista en bodega", "14/10/2026 09:00", True),
                          ("Falta instalarla", "", False)])
        self.assertEqual(t["detalle"], "Cinta desgastada, se ve la tela")
        self.assertEqual(t["actor"], "Lenin Urbina")
        self.assertEqual(t["ref"]["texto"], "Ver OT del pedido OT-2026-00513")

    def test_instalado_cierra_el_episodio_en_verde(self):
        s = solicitud(40, 513, estado="instalado", instalado_at=U(2026, 10, 16, 15, 0), ot_generada_id=530)
        t = por_id(construir(solicitudes=[s], numeros_ot={530: "OT-2026-00530"}))["sol:40"]
        self.assertEqual((t["severidad"], t["abierto"], t["fin_ts"], t["estado_txt"]), ("ok", False, U(2026, 10, 16, 15, 0), "Instalado"))
        self.assertEqual(t["pasos"][-1]["texto"], "Instalado")
        self.assertEqual(t["refs"][0]["texto"], "Ver OT de instalación OT-2026-00530")

    def test_rechazado_queda_en_gris(self):
        t = por_id(construir(solicitudes=[solicitud(40, 513, estado="rechazado", updated_at=U(2026, 10, 9, 12, 0))]))["sol:40"]
        self.assertEqual((t["severidad"], t["abierto"], t["estado_txt"]), ("info", False, "Anulado"))

    def test_piola_cinta_sku_y_fuera_de_servicio(self):
        s = solicitud(40, 513, origen="piola", repuesto_nombre="Piola 12 m", dejo_fuera_servicio=1)
        t = por_id(construir(solicitudes=[s]))["sol:40"]
        self.assertEqual(t["titulo"], "Cambio de piola: Piola 12 m × 1")
        textos = [l["texto"] for l in t["lineas"]]
        self.assertIn("SKU REP-CINTA", textos)
        self.assertIn("Dejó el equipo fuera de servicio", textos)

    def test_el_tecnico_ve_el_avance_en_palabras_de_bodega_sin_proveedor_ni_quien(self):
        s = solicitud(40, 513, estado="pedido", validado_at=U(2026, 10, 8, 12, 0), pedido_at=U(2026, 10, 9, 12, 0))
        t = por_id(construir(TECNICO, solicitudes=[s]))["sol:40"]
        self.assertEqual(t["estado_txt"], "En revisión de bodega")
        self.assertEqual([p["texto"] for p in t["pasos"]], ["Solicitado", "Falta que la pieza esté lista"])
        self.assertIsNone(t["actor"])
        self.assertNotRegex(todo_el_texto(MH.a_json(t)).lower(), r"proveedor|pedido,|validado|compra")

    def test_cantidad_con_decimales_y_nombre_largo_van_completos(self):
        nombre = "Cinta de trotadora de 3 mm con costura reforzada y base de goma para uso intensivo de gimnasio " * 2
        t = por_id(construir(solicitudes=[solicitud(40, 513, repuesto_nombre=nombre, cantidad="1.50")]))["sol:40"]
        self.assertTrue(t["titulo"].endswith("× 1.5"))
        self.assertIn(nombre.strip()[:150], t["titulo"], "REGLA #15: nada de nombres cortados")


class TestParadasYBajas(unittest.TestCase):
    def test_fuera_de_servicio_abierto_dice_desde_cuando_y_hace_cuanto(self):
        p = por_id(construir(paradas=[parada(1)], numeros_ot={513: "OT-2026-00513"}))["parada:1"]
        self.assertEqual((p["tipo"], p["subtipo"], p["severidad"], p["abierto"]), ("estado", "fuera_servicio", "falla", True))
        self.assertEqual(p["titulo"], "Fuera de servicio: Motor no enciende")
        self.assertTrue(p["estado_txt"].startswith("Sigue detenido · 13 días"), p["estado_txt"])
        self.assertEqual(MH.a_json(p, ahora=AHORA)["fecha"], "07/10/2026 16:30")
        self.assertEqual([x["texto"] for x in p["pasos"]], ["Quedó fuera de servicio", "Sigue detenido"])
        self.assertEqual(p["ref"]["texto"], "Ver OT OT-2026-00513")

    def test_cuando_vuelve_a_operar_el_episodio_termina_en_verde(self):
        p = por_id(construir(paradas=[parada(1, fin_at=U(2026, 10, 16, 15, 30), ref_fin_tabla="mant_visitas", ref_fin_id=530, cerrada_por="Jaizer")],
                             numeros_ot={513: "OT-2026-00513", 530: "OT-2026-00530"}))["parada:1"]
        self.assertEqual((p["severidad"], p["abierto"], p["fin_ts"]), ("ok", False, U(2026, 10, 16, 15, 30)))
        self.assertEqual(p["estado_txt"], "Volvió a operar tras 8 días 20 h")
        self.assertEqual(p["refs"][0]["texto"], "Ver OT donde terminó OT-2026-00530")
        self.assertEqual(p["actor"], "Jaizer")
        self.assertEqual(MH.a_json(p)["fecha_fin"], "16/10/2026 12:30")

    def test_el_riesgo_se_separa_del_motivo(self):
        p = por_id(construir(paradas=[parada(1, motivo=MH.MARCA_RIESGO + " · Pieza rota, suelta o faltante")]))["parada:1"]
        self.assertTrue(p["riesgo"])
        self.assertEqual(p["titulo"], "Fuera de servicio: Pieza rota, suelta o faltante")

    def test_inicio_estimado_y_en_reparacion(self):
        p = por_id(construir(paradas=[parada(1, inicio_aprox=1, estado_inicio="en_reparacion", motivo=None)]))["parada:1"]
        self.assertEqual((p["subtipo"], p["titulo"]), ("en_reparacion", "En reparación"))
        self.assertTrue(p["estado_txt"].startswith("Sigue detenido · ≈ "))
        self.assertIn("Quedó en reparación (inicio estimado)", [x["texto"] for x in p["pasos"]])

    def test_una_parada_que_termina_en_baja_es_una_baja(self):
        p = parada(1, estado_fin="dado_baja", fin_at=U(2026, 10, 9, 15, 0), ref_fin_tabla="mant_visitas", ref_fin_id=513, cerrada_por="Aarón")
        t = por_id(construir(paradas=[p]))["parada:1"]
        self.assertEqual((t["tipo"], t["subtipo"], t["severidad"], t["titulo"], t["_baja"]), ("estado", "baja", "falla", "Dado de baja", True))
        self.assertEqual(t["ts"], U(2026, 10, 9, 15, 0), "la baja ocurre cuando se da de baja, no cuando partió la parada")
        self.assertIn("Estuvo fuera de servicio", t["detalle"])

    def test_gestion_ve_quien_lo_marco_y_el_tecnico_no(self):
        self.assertEqual(por_id(construir(paradas=[parada(1)]))["parada:1"]["actor"], "Lenin Urbina")
        self.assertIsNone(por_id(construir(TECNICO, paradas=[parada(1)]))["parada:1"]["actor"])


class TestFichaTicketsYGarantia(unittest.TestCase):
    def test_cambio_de_numero_de_serie(self):
        t = por_id(construir(auditoria=[audit(1, "serie", "ILUS-0001", "SN-123")]))["audit:1"]
        self.assertEqual((t["tipo"], t["subtipo"], t["titulo"], t["detalle"]), ("ficha", "serie", "Cambio de N° de serie", "ILUS-0001 → SN-123"))
        self.assertEqual(t["por_que"], "Placa leída en terreno")
        self.assertEqual(t["actor"], "Aarón")
        self.assertEqual(MH.a_json(t)["fecha"], "15/09/2026 11:00")
        sin = por_id(construir(auditoria=[audit(2, "serie", "", "SN-9", motivo="")]))["audit:2"]
        self.assertEqual(sin["detalle"], "(sin serie) → SN-9")
        self.assertFalse(sin["lineas"])

    def test_cambio_de_ubicacion_y_de_datos_y_de_estado(self):
        a = [audit(1, "ubicacion_sala", "Sala 1", "Sala cardio"), audit(2, "marca", "ILUZ", "ILUS", motivo=""), audit(3, "estado_op", "operativo", "critico"),
             audit(4, "estado", "baja", "activo", motivo="Apareció"), audit(5, "observaciones", "a", "b"), audit(6, "sku", "", "TR-1")]
        d = por_id(construir(auditoria=a))
        self.assertEqual(d["audit:1"]["titulo"], "Cambio de ubicación")
        self.assertEqual(d["audit:1"]["detalle"], "Sala 1 → Sala cardio")
        self.assertEqual(d["audit:2"]["titulo"], "Se corrigió la marca del equipo")
        self.assertEqual((d["audit:3"]["titulo"], d["audit:3"]["severidad"]), ("Estado operativo: Operativo → Crítico", "aviso"))
        self.assertEqual((d["audit:4"]["subtipo"], d["audit:4"]["severidad"], d["audit:4"]["detalle"]), ("restaurado", "ok", "Apareció"))
        self.assertNotIn("audit:5", d, "las observaciones libres no son un hecho de la ficha")
        self.assertEqual(d["audit:6"]["detalle"], "(vacío) → TR-1")

    def test_quien_cambio_la_ficha_solo_gestion(self):
        self.assertIsNone(por_id(construir(TECNICO, auditoria=[audit(1, "serie", "A", "B")]))["audit:1"]["actor"])

    def test_ticket_solo_para_gestion(self):
        t = por_id(construir(tickets=[ticket(3)]))["ticket:3"]
        self.assertEqual((t["tipo"], t["severidad"], t["abierto"], t["estado_txt"]), ("ticket", "aviso", True, "Abierto"))
        self.assertEqual(t["titulo"], "TK-2026-00003 · Fuera de servicio: Trotadora")
        self.assertEqual(t["ref"]["url"], "/tickets/3")
        self.assertIn("Prioridad urgente", [l["texto"] for l in t["lineas"]])
        cerrado = por_id(construir(tickets=[ticket(4, estado="closed", cerrado_at=U(2026, 10, 9, 15, 0))]))["ticket:4"]
        self.assertEqual((cerrado["severidad"], cerrado["abierto"], cerrado["fin_ts"]), ("info", False, U(2026, 10, 9, 15, 0)))
        self.assertFalse([e for e in construir(TECNICO, tickets=[ticket(3)]) if e["tipo"] == "ticket"])

    def test_garantia_vigente_por_vencer_y_vencida(self):
        g = lambda fin, **kw: por_id(construir(equipo=equipo(fecha_fin_garantia=fin, **kw)))["garantia"]
        vigente = g(D(2027, 1, 10))
        self.assertEqual((vigente["severidad"], vigente["estado_txt"], vigente["titulo"]), ("ok", "Vigente", "La garantía vence el 10/01/2027"))
        self.assertIn("Calculada por el sistema desde la fecha de instalación o de la factura", [l["texto"] for l in vigente["lineas"]])
        por_vencer = g(D(2026, 11, 5), garantia_origen="manual")
        self.assertEqual((por_vencer["severidad"], por_vencer["estado_txt"]), ("aviso", "Vence en 15 días"))
        self.assertIn("Fecha puesta a mano en la ficha", [l["texto"] for l in por_vencer["lineas"]])
        self.assertEqual(g(D(2026, 10, 21))["estado_txt"], "Vence hoy")
        vencida = g(D(2026, 5, 1))
        self.assertEqual((vencida["severidad"], vencida["estado_txt"], vencida["titulo"]), ("info", "Vencida", "Terminó la garantía el 01/05/2026"))
        self.assertNotIn("garantia", ids(construir(equipo=equipo(fecha_fin_garantia=None))))

    def test_el_alta_y_la_instalacion_de_la_ficha(self):
        d = por_id(construir(visitas=[]))
        self.assertEqual(MH.a_json(d["alta"])["fecha"], "05/01/2026 10:00")
        self.assertIn("Documento de origen: FCV 1234", [l["texto"] for l in d["alta"]["lineas"]])
        self.assertEqual(d["alta"]["actor"], "Aarón")
        self.assertEqual(MH.a_json(d["instalacion"])["fecha"], "10/01/2026", "sin OT de instalación, la fecha de la ficha")
        # con una OT de instalación esa OT cuenta la instalación
        self.assertNotIn("instalacion", ids(construir(visitas=[visita(100, tipo="instalacion", fecha_programada=D(2026, 1, 10))])))
        # el técnico no recibe ni el documento de origen ni quién lo creó
        t = por_id(construir(TECNICO))["alta"]
        self.assertFalse(t["lineas"])
        self.assertIsNone(t["actor"])


# ══════════════════ el caso completo: una trotadora con su historia real, registrada por TODAS las tablas ══════════════════
def escenario_trotadora():
    """Una trotadora que se instaló en enero, tuvo su preventiva en abril, cambió de serie en septiembre y en octubre falló: quedó fuera de
    servicio, se pidió una cinta, se levantó una OT de seguimiento y volvió a operar. Cada hecho lo registran VARIAS tablas."""
    return fuentes(
        equipo=equipo(estado_capturado="operativo"),
        visitas=[
            visita(100, tipo="instalacion", fecha_programada=D(2026, 1, 10), cerrada_at=U(2026, 1, 10, 22, 0)),
            visita(200, tipo="preventiva", fecha_programada=D(2026, 4, 2), cerrada_at=U(2026, 4, 2, 20, 0)),
            visita(513, tipo="correctiva", titulo="Trotadora no enciende", hora_real_inicio=U(2026, 10, 7, 16, 30), cerrada_at=U(2026, 10, 17, 12, 0)),
            visita(530, tipo="correctiva", titulo="Cambio de cinta", fecha_programada=D(2026, 10, 16), cerrada_at=U(2026, 10, 16, 21, 0),
                   ot_madre_id=513, ot_relacion="seguimiento"),
        ],
        visita_equipos=[
            diag(100),                                                              # «verificado» automático: no es un diagnóstico
            diag(200, diagnostico_estado="aprobado", revisado_at=U(2026, 4, 2, 19, 0)),
            diag(513, estado_revision="falla_detectada", razon_saltado="fuera_de_servicio", observacion_tecnico="Motor no enciende",
                 revisado_at=U(2026, 10, 7, 19, 30)),                               # lo cuenta la parada
        ],
        tareas=[tarea(70, 513, "falla"), tarea(71, 513, "alerta", titulo="Tensión de la cinta", completada_at=U(2026, 10, 7, 19, 22))],
        hallazgos=[
            hallazgo(5, 513, estado="resuelto", ot_seguimiento_id=530, seg_numero="OT-2026-00530", seg_estado="cerrada",
                     seg_cerrada=U(2026, 10, 16, 21, 0), fotos_json=json.dumps(["/f/cinta.jpg"])),                 # reemplaza a la tarea 70
            hallazgo(6, 513, referencia="t71", tarea_id=71, severidad="alerta", titulo="Trotadora X-1000 Pro · Tensión de la cinta: Alerta",
                     detectado_en=U(2026, 10, 7, 19, 22)),                                                         # reemplaza a la tarea 71
            hallazgo(7, 513, origen="equipo", referencia="m9:fuera_servicio", tarea_id=None, severidad="fuera_servicio",
                     titulo="Trotadora X-1000 Pro: No se puede usar — Motor no enciende", detectado_en=U(2026, 10, 7, 19, 31),
                     fotos_json=json.dumps(["/f/fs.jpg"])),                                                        # va en la parada
            hallazgo(8, 513, origen="equipo", referencia="m9:rep:abc12345", tarea_id=None, severidad="repuesto", estado="tomado",
                     tomado_por="Víctor", detectado_en=U(2026, 10, 7, 19, 41), titulo="Trotadora X-1000 Pro: Repuesto solicitado: Cinta × 1"),
        ],
        solicitudes=[solicitud(40, 513, estado="instalado", instalado_at=U(2026, 10, 16, 15, 0), ot_generada_id=530,
                               validado_at=U(2026, 10, 8, 12, 0), pedido_at=U(2026, 10, 9, 12, 0), recibido_at=U(2026, 10, 14, 12, 0))],
        paradas=[parada(1, fin_at=U(2026, 10, 16, 15, 30), ref_fin_tabla="mant_visitas", ref_fin_id=530, cerrada_por="Jaizer")],
        eventos=[
            evento(1, "reparacion", referencia_tabla="mant_visitas", referencia_id=513, fecha_evento=U(2026, 10, 17, 12, 0)),
            evento(2, "cambio_estado", metadata_json=json.dumps({"parada": "inicio"}), fecha_evento=U(2026, 10, 7, 19, 30)),
            evento(3, "cambio_repuesto", referencia_tabla="mant_ot_repuesto_solicitudes", referencia_id=40, fecha_evento=U(2026, 10, 16, 15, 0)),
            evento(4, "foto_agregada", fecha_evento=U(2026, 10, 7, 20, 0)),
            evento(5, "instalacion", referencia_tabla="mant_visitas", referencia_id=100, fecha_evento=U(2026, 1, 10, 22, 0)),
            evento(6, "cambio_estado", descripcion="Restaurado (vuelve a activo) por Daniel", fecha_evento=U(2026, 8, 1, 14, 0),
                   metadata_json=json.dumps({"fuente": "restaurar"})),
        ],
        auditoria=[audit(1, "serie", "ILUS-0001", "SN-123"), audit(2, "estado_capturado", "operativo", "fuera_servicio", fecha=U(2026, 10, 7, 19, 30)),
                   audit(3, "ubicacion_sala", "Bodega", "Sala cardio", fecha=U(2026, 2, 3, 15, 0))],
        tickets=[ticket(3)],
        numeros_ot={100: "OT-2026-00100", 200: "OT-2026-00200", 513: "OT-2026-00513", 530: "OT-2026-00530"},
    )


class TestElCasoCompleto(unittest.TestCase):
    """La trotadora de Daniel, de punta a punta: lo que ve cada rol, sin repetir nada."""

    def test_gestion_ve_la_historia_completa_ordenada_y_sin_repetir(self):
        es = MH.construir(escenario_trotadora(), GESTION)
        self.assertEqual(ids(es), [
            "garantia",       # 10/01/2027: todavía no llega, va arriba de todo
            "visita:530",     # 16/10  la OT de seguimiento (cambio de cinta)
            "sol:40",         # 07/10 16:40  la cinta: solicitada, validada, pedida, recibida e instalada el 16/10 (UNA tarjeta)
            "ticket:3",       # 07/10 16:35
            "parada:1",       # 07/10 16:30  fuera de servicio hasta el 16/10 (UNA tarjeta con su hallazgo y su foto)
            "hallazgo:6",     # 07/10 16:22  Tensión de la cinta: Alerta (sin gestionar)
            "hallazgo:5",     # 07/10 16:21  Estado de la cinta: Falla → resuelto con la OT 530
            "visita:513",     # 07/10 13:30  la visita donde apareció todo
            "audit:1",        # 15/09  cambio de N° de serie
            "evento:6",       # 01/08  restaurado
            "diag:200",       # 02/04  diagnóstico aprobado
            "visita:200",     # 02/04  preventiva
            "audit:3",        # 03/02  cambio de ubicación
            "visita:100",     # 10/01  instalación
            "alta",           # 05/01  entró a la ficha
        ])
        self.assertEqual(len(ids(es)), len(set(ids(es))))
        d = por_id(es)
        # lo que se reemplaza / absorbe NO está como tarjeta propia
        for ausente in ("tarea:70", "tarea:71", "diag:513", "hallazgo:7", "hallazgo:8"):
            self.assertNotIn(ausente, d, ausente)
        self.assertFalse([i for i in d if i.startswith("evento:") and i != "evento:6"], "solo el restaurado de la hoja de vida sobrevive")
        self.assertNotIn("audit:2", d, "lo cuenta la parada")
        # lo que sí está
        for presente in ("visita:100", "visita:200", "visita:513", "visita:530", "diag:200", "hallazgo:5", "hallazgo:6", "sol:40", "parada:1",
                         "audit:1", "audit:3", "evento:6", "ticket:3", "garantia", "alta"):
            self.assertIn(presente, d, presente)
        self.assertNotIn("instalacion", d, "la OT 100 es la instalación")
        # los estados de gestión y las fotos se conservaron en lo que absorbió
        self.assertEqual(d["parada:1"]["gestion"]["texto"], "Hallazgo: sin gestionar")
        self.assertEqual(MH.a_json(d["parada:1"])["fotos"], ["/f/fs.jpg"])
        self.assertEqual(d["sol:40"]["gestion"]["estado"], "Caso tomado")
        self.assertEqual(MH.a_json(d["hallazgo:5"])["fotos"], ["/f/cinta.jpg"])

    def test_el_orden_es_del_mas_nuevo_al_mas_antiguo_por_la_fecha_de_inicio(self):
        es = MH.construir(escenario_trotadora(), GESTION)
        fechas = [e["ts"] for e in es]
        self.assertEqual(fechas, sorted(fechas, reverse=True))
        self.assertEqual(ids(es)[0], "garantia")
        self.assertEqual(ids(es)[-1], "alta")
        self.assertLess(ids(es).index("visita:530"), ids(es).index("visita:513"))
        self.assertLess(ids(es).index("visita:513"), ids(es).index("visita:200"))
        self.assertLess(ids(es).index("visita:200"), ids(es).index("visita:100"))

    def test_el_tecnico_ve_lo_operativo_sin_hallazgos_ni_tickets_ni_quien(self):
        es = MH.construir(escenario_trotadora(), TECNICO)
        d = por_id(es)
        self.assertEqual(ids(es), ["garantia", "visita:530", "sol:40", "parada:1", "tarea:71", "tarea:70", "visita:513", "audit:1", "evento:6",
                                   "diag:200", "visita:200", "audit:3", "visita:100", "alta"])
        self.assertFalse([e for e in es if e["tipo"] in ("hallazgo", "ticket")])
        self.assertIn("tarea:70", d, "su checklist: la tarea sigue ahí porque su hallazgo es de la gestión")
        self.assertIn("tarea:71", d)
        self.assertTrue(all(e["actor"] is None and e["gestion"] is None for e in es))
        self.assertIsNone(d["parada:1"]["gestion"])
        self.assertFalse(MH.a_json(d["parada:1"])["fotos"], "las fotos de un hallazgo son de la gestión")
        self.assertIn("sol:40", d)
        self.assertEqual(d["sol:40"]["estado_txt"], "Instalado")

    def test_superadmin_ve_ademas_quien_hizo_el_diagnostico(self):
        d = por_id(MH.construir(escenario_trotadora(), SUPER))
        self.assertEqual(d["diag:200"]["actor"], "Lenin Urbina")
        self.assertIsNone(por_id(MH.construir(escenario_trotadora(), GESTION))["diag:200"]["actor"])

    def test_los_contadores_por_tipo(self):
        c = MH.contar(MH.construir(escenario_trotadora(), GESTION))
        self.assertEqual({k: c.get(k) for k in ("visita", "diagnostico", "checklist", "hallazgo", "repuesto", "estado", "ficha", "ticket", "garantia")},
                         {"visita": 4, "diagnostico": 1, "checklist": None, "hallazgo": 2, "repuesto": 1, "estado": 2, "ficha": 3, "ticket": 1, "garantia": 1})
        self.assertEqual(c["todos"], 15)

    def test_el_resumen_de_lo_pendiente(self):
        r = MH.resumen(MH.construir(escenario_trotadora(), GESTION), AHORA)
        self.assertIsNone(r["fuera_servicio"], "ya volvió a operar")
        self.assertEqual((r["hallazgos_abiertos"], r["repuestos_abiertos"], r["tickets_abiertos"]), (1, 0, 1))
        self.assertEqual(r["n_visitas"], 3, "no cuenta la instalación como visita de trabajo")
        self.assertTrue(r["ultima_visita"]["fecha"].startswith("16/10/2026"))
        pendiente = fuentes(paradas=[parada(1)], solicitudes=[solicitud(40, 513)])
        r2 = MH.resumen(MH.construir(pendiente, GESTION), AHORA)
        self.assertEqual(r2["fuera_servicio"]["desde"], "07/10/2026 16:30")
        self.assertEqual(r2["fuera_servicio"]["hace"], "hace 13 días")
        self.assertEqual(r2["repuestos_abiertos"], 1)


# ══════════════════ filtros, contadores y paginación (REGLA #4.3) ══════════════════
def lote(n):
    """n visitas, una por día, para probar la paginación."""
    return [visita(1000 + i, fecha_programada=D(2026, 3, 1) + dt.timedelta(days=i), cerrada_at=None) for i in range(n)]


class TestFiltrosYPaginacion(unittest.TestCase):
    def test_paginar_muestra_A_B_de_N(self):
        es = construir(equipo=equipo(fecha_fin_garantia=None, fecha_instalacion=None, created_at=None), visitas=lote(45))
        self.assertEqual(len(es), 45)
        p1, m1 = MH.paginar(es, 1, 20)
        self.assertEqual((len(p1), m1), (20, {"total": 45, "pagina": 1, "paginas": 3, "por_pagina": 20, "desde": 1, "hasta": 20}))
        p3, m3 = MH.paginar(es, 3, 20)
        self.assertEqual((len(p3), m3["desde"], m3["hasta"]), (5, 41, 45))
        self.assertEqual(ids(p1 + MH.paginar(es, 2, 20)[0] + p3), ids(es), "las páginas son el total, sin saltos ni repeticiones")

    def test_una_pagina_que_se_pasa_se_ajusta_y_los_valores_malos_no_rompen(self):
        es = construir(visitas=lote(45))
        self.assertEqual(MH.paginar(es, 99, 20)[1]["pagina"], MH.paginar(es, 99, 20)[1]["paginas"])
        for malo in (0, -3, "x", None, ""):
            self.assertEqual(MH.paginar(es, malo, 20)[1]["pagina"], 1, repr(malo))
        for malo in (7, 0, -1, "x", None, 100000):
            self.assertEqual(MH.paginar(es, 1, malo)[1]["por_pagina"], 20, repr(malo))
        for ok in (10, 20, 50, 100, "50"):
            self.assertEqual(MH.paginar(es, 1, ok)[1]["por_pagina"], int(ok))

    def test_sin_resultados(self):
        p, m = MH.paginar([], 1, 20)
        self.assertEqual((p, m["total"], m["desde"], m["hasta"], m["paginas"]), ([], 0, 0, 0, 1))

    def test_rango_de_fechas_inclusivo_y_en_dias_de_chile(self):
        # 08/10 02:30 UTC es el 07/10 23:30 en Chile: pertenece al 07, no al 08
        e = construir(equipo=equipo(fecha_fin_garantia=None, fecha_instalacion=None, created_at=None),
                      eventos=[evento(1, "cambio_estado", fecha_evento=U(2026, 10, 8, 2, 30), descripcion="Restaurado"),
                               evento(2, "cambio_estado", fecha_evento=U(2026, 10, 9, 12, 0), descripcion="Restaurado 2")])
        self.assertEqual(ids(MH.filtrar_fechas(e, "2026-10-07", "2026-10-07")), ["evento:1"])
        self.assertEqual(ids(MH.filtrar_fechas(e, "2026-10-08", None)), ["evento:2"])
        self.assertEqual(ids(MH.filtrar_fechas(e, None, "2026-10-07")), ["evento:1"])
        self.assertEqual(ids(MH.filtrar_fechas(e, "2026-10-07", "2026-10-09")), ids(e))
        self.assertEqual(ids(MH.filtrar_fechas(e, "2026-10-10", None)), [])

    def test_un_filtro_vacio_o_malo_no_filtra_nada(self):
        e = construir(visitas=lote(5))
        for d, h in ((None, None), ("", ""), ("basura", "2026-13-45"), ("21/10/2026", None)):
            self.assertEqual(ids(MH.filtrar_fechas(e, d, h)), ids(e), (d, h))
        self.assertEqual(ids(MH.filtrar_tipos(e, [])), ids(e))
        self.assertEqual(ids(MH.filtrar_tipos(e, None)), ids(e))

    def test_filtrar_por_tipo(self):
        e = MH.construir(escenario_trotadora(), GESTION)
        self.assertEqual(sorted(x["id"] for x in MH.filtrar_tipos(e, ["hallazgo"])), ["hallazgo:5", "hallazgo:6"])
        self.assertEqual(len(MH.filtrar_tipos(e, ["visita", "repuesto"])), 5)

    def test_los_contadores_respetan_las_fechas_pero_no_el_tipo(self):
        e = MH.construir(escenario_trotadora(), GESTION)
        en_octubre = MH.filtrar_fechas(e, "2026-10-01", "2026-10-31")
        c = MH.contar(en_octubre)
        self.assertEqual((c["todos"], c["visita"], c["hallazgo"], c["repuesto"], c["estado"], c["ticket"], c.get("ficha")), (7, 2, 2, 1, 1, 1, None))
        # al filtrar por tipo, el total del rango NO cambia (cada pastilla sigue diciendo cuánto hay)
        solo_hallazgos = MH.filtrar_tipos(en_octubre, ["hallazgo"])
        self.assertEqual(len(solo_hallazgos), 2)
        self.assertEqual(MH.contar(en_octubre)["todos"], 7)

    def test_limpiar_tipos_solo_deja_los_validos_y_permitidos(self):
        self.assertEqual(MH.limpiar_tipos("hallazgo,visita,x,visita, ,estado", GESTION), ["hallazgo", "visita", "estado"])
        self.assertEqual(MH.limpiar_tipos("hallazgo,visita,ticket", TECNICO), ["visita"], "un técnico no puede pedir lo que no recibe")
        self.assertEqual(MH.limpiar_tipos(["VISITA", "Estado"], GESTION), ["visita", "estado"])
        for vacio in (None, "", [], "   ", ",,"):
            self.assertEqual(MH.limpiar_tipos(vacio, GESTION), [], repr(vacio))
        # lo que no es un tipo se ignora (y los tipos solo filtran en memoria: nunca llegan a una consulta SQL)
        self.assertEqual(MH.limpiar_tipos("visita; DROP TABLE mant_visitas", GESTION), ["visita"])
        self.assertEqual(MH.limpiar_tipos("' OR 1=1 --", GESTION), [])

    def test_a_json_no_deja_pasar_claves_internas(self):
        for e in MH.construir(escenario_trotadora(), GESTION):
            j = MH.a_json(e, ahora=AHORA)
            self.assertFalse([k for k in j if k.startswith("_") or k == "ts"], j.keys())
            json.dumps(j)                                                            # todo serializa: ni datetime ni set sueltos
            self.assertRegex(j["fecha"], r"^\d{2}/\d{2}/\d{4}( \d{2}:\d{2})?$")
            self.assertRegex(j["mes"], r"^\d{4}-\d{2}$")

    def test_a_json_limita_y_limpia_las_fotos(self):
        e = construir(hallazgos=[hallazgo(5, 513, fotos_json=json.dumps(["/f/%d.jpg" % i for i in range(12)]))])
        t = por_id(e)["hallazgo:5"]
        self.assertEqual(len(MH.a_json(t)["fotos"]), MH.FOTOS_MAX)
        self.assertEqual(MH.a_json(t, fotos_extra=["/f/0.jpg", "javascript:x", "https://ok/x.png"])["fotos"][:2], ["/f/0.jpg", "/f/1.jpg"])
        self.assertEqual(MH.url_segura("javascript:alert(1)"), "")
        self.assertEqual(MH.url_segura("//evil.com/x"), "")
        self.assertEqual(MH.url_segura("/f/ok.jpg"), "/f/ok.jpg")
        self.assertEqual(MH.url_segura("https://cdn/x.jpg"), "https://cdn/x.jpg")
        self.assertEqual(MH.url_segura("http://inseguro/x.jpg"), "")


class TestEncabezado(unittest.TestCase):
    def test_nombre_completo_serie_cliente_estado_y_garantia(self):
        nombre = "Trotadora comercial ILUS X-1000 Pro con consola táctil de 15 pulgadas y motor de 5 HP continuos para gimnasio de alto tráfico"
        h = MH.encabezado(equipo(nombre=nombre), HOY)
        self.assertEqual((h["nombre"], h["serie"], h["sku"], h["cliente"]), (nombre, "SN-123", "TR-1000", "Gimnasio Fit SpA"))
        self.assertEqual(h["estado"], {"clave": "operativo", "texto": "Operativo", "color": "verde"})
        self.assertEqual(h["garantia"], {"texto": "Vigente hasta el 10/01/2027", "color": "verde"})

    def test_estados(self):
        casos = (({"estado_capturado": "fuera_servicio"}, "Fuera de servicio", "rojo"), ({"estado_capturado": "en_reparacion"}, "En reparación", "rojo"),
                 ({"estado": "baja"}, "Dado de baja", "negro"), ({"estado_capturado": "dado_baja"}, "Dado de baja", "negro"),
                 ({"estado_capturado": "advertencia"}, "Con advertencia", "ambar"), ({"estado_capturado": None}, "Operativo", "verde"))
        for kw, texto, color in casos:
            e = MH.encabezado(equipo(**kw), HOY)["estado"]
            self.assertEqual((e["texto"], e["color"]), (texto, color), kw)

    def test_garantia(self):
        g = lambda fin: MH.encabezado(equipo(fecha_fin_garantia=fin), HOY)["garantia"]
        self.assertEqual(g(D(2026, 11, 5)), {"texto": "Vigente hasta el 05/11/2026", "color": "ambar"})
        self.assertEqual(g(D(2026, 5, 1)), {"texto": "Venció el 01/05/2026", "color": "gris"})
        self.assertEqual(g(None), {"texto": "Sin garantía registrada", "color": "gris"})

    def test_equipo_sin_nombre(self):
        self.assertEqual(MH.encabezado(equipo(nombre=None, id=77), HOY)["nombre"], "Equipo #77")


# ══════════════════ confidencialidad (REGLAS #19 y #26): el dato se quita en el SERVIDOR ══════════════════
SENTINELAS = ("PROVEEDOR-SECRETO", "OC-99999", "NOTA-INTERNA-DE-GESTION", "TICKET-DE-COMPRA-77", "COSTO-123456")


def fuentes_con_basura_sensible():
    """Filas con TODO lo prohibido adentro (como las devolvería un SELECT * descuidado): el módulo no debe dejar pasar nada de eso a nadie."""
    f = escenario_trotadora()
    for s in f["solicitudes"]:
        s.update({"proveedor_id": 5, "proveedor_nombre": SENTINELAS[0], "oc_numero": SENTINELAS[1], "nota_gestion": SENTINELAS[2],
                  "ticket_id": 77, "ticket_compra": SENTINELAS[3], "costo_unitario": 123456, "costo_origen": SENTINELAS[4], "compra_id": 9})
    f["tareas"][0]["costo"] = SENTINELAS[4]
    for h in f["hallazgos"]:
        h["costo"] = SENTINELAS[4]
    f["equipo"].update({"costo": SENTINELAS[4], "proveedor": SENTINELAS[0], "valor_origen": 9999999})
    return f


class TestConfidencialidad(unittest.TestCase):
    def _json_de(self, rol):
        return todo_el_texto([MH.a_json(e, ahora=AHORA) for e in MH.construir(fuentes_con_basura_sensible(), rol)])

    def test_nadie_recibe_proveedor_costo_oc_nota_de_gestion_ni_ticket_de_compra(self):
        for rol in (GESTION, SUPER, TECNICO):
            js = self._json_de(rol)
            for s in SENTINELAS:
                self.assertNotIn(s, js, "%s se coló para %r" % (s, rol))
            self.assertNotRegex(js, r"123456|9999999")

    def test_el_tecnico_no_recibe_tipos_de_gestion_ni_quien_hizo_nada(self):
        js = self._json_de(TECNICO)
        es = MH.construir(fuentes_con_basura_sensible(), TECNICO)
        self.assertEqual({e["tipo"] for e in es} & {"hallazgo", "ticket"}, set())
        for e in es:
            j = MH.a_json(e, ahora=AHORA)
            self.assertIsNone(j["actor"], e["id"])
            self.assertIsNone(j["gestion"], e["id"])
            self.assertFalse([r for r in j["refs"] if r["tabla"] in ("mant_ot_hallazgos", "tk_tickets")], e["id"])
        for palabra in ("Lenin Urbina", "Víctor", "Aarón", "Jaizer", "Hallazgo", "hallazgo", "proveedor", "Proveedor"):
            self.assertNotIn(palabra, js.replace("Técnico: Lenin Urbina", ""), palabra)

    def test_el_tecnico_solo_ve_el_nombre_del_tecnico_de_la_visita_como_la_ficha_de_siempre(self):
        js = self._json_de(TECNICO)
        self.assertEqual(js.count("Lenin Urbina"), js.count("Técnico: Lenin Urbina"), "el nombre solo aparece en «Técnico: …» de las visitas")

    def test_la_familia_tecnico_completa_es_tecnico(self):
        amb = cargar(funciones=["_rol_familia", "_es_rol_tecnico"], extra={"g": types.SimpleNamespace(user=None)})
        for rol in ("tecnico", "tecnico_ejecutivo", "tecnico_externo", "tecnico_externo_jr", "tecnico_jr"):
            amb["g"].user = {"role": rol}
            self.assertTrue(amb["_es_rol_tecnico"](), rol)
        for rol in ("superadmin", "admin", "supervisor", "ejecutivo", "ejecutivo_sstt"):
            amb["g"].user = {"role": rol}
            self.assertFalse(amb["_es_rol_tecnico"](), rol)

    def test_el_superadmin_es_el_unico_que_ve_el_quien_de_diagnosticos_y_checklist(self):
        for rol, ve in ((SUPER, True), (GESTION, False), (TECNICO, False)):
            d = por_id(MH.construir(escenario_trotadora(), rol))
            self.assertEqual(d["diag:200"]["actor"] == "Lenin Urbina", ve, repr(rol))
        f = escenario_trotadora()
        f["hallazgos"] = []                       # sin hallazgos, la tarea queda como tarjeta propia
        self.assertEqual(por_id(MH.construir(f, SUPER))["tarea:70"]["actor"], "Lenin Urbina")
        self.assertIsNone(por_id(MH.construir(f, GESTION))["tarea:70"]["actor"])

    def test_un_tecnico_nunca_es_superadmin_a_efectos_de_esta_vista(self):
        self.assertFalse(MH.Rol(True, True).es_superadmin)
        self.assertIsNone(por_id(MH.construir(escenario_trotadora(), MH.Rol(True, True)))["diag:200"]["actor"])

    def test_los_enlaces_a_una_ot_que_el_rol_no_puede_abrir_van_sin_url(self):
        f = escenario_trotadora()
        f["visitas_visibles"] = {513}
        d = por_id(MH.construir(f, TECNICO))
        self.assertEqual(d["visita:513"]["ref"]["url"], "/ot/513")
        self.assertIsNone(d["visita:200"]["ref"]["url"])
        self.assertIsNone(d["visita:530"]["ref"]["url"])
        self.assertEqual(MH.a_json(d["visita:200"])["ref"]["numero"], "OT-2026-00200", "se ve el número aunque no se pueda entrar")
        self.assertIsNone(d["sol:40"]["refs"][0]["url"], "la OT de instalación tampoco se abre si no es suya")
        self.assertIsNone(d["parada:1"]["refs"][0]["url"])


# ══════════════════ B) la lectura de app.py contra una base de mentira (SQLite con las mismas columnas) ══════════════════
DDL = """
CREATE TABLE mant_clientes (id INTEGER PRIMARY KEY, razon_social TEXT);
CREATE TABLE app_users (id INTEGER PRIMARY KEY, nombre TEXT, username TEXT);
CREATE TABLE mant_maquinas (id INTEGER PRIMARY KEY, cliente_id INT, sku TEXT, nombre TEXT, serie TEXT, marca TEXT, modelo TEXT, estado TEXT,
  estado_capturado TEXT, estado_op TEXT, ubicacion_sala TEXT, fecha_instalacion DATE, fecha_fin_garantia DATE, garantia_origen TEXT,
  doc_origen TEXT, created_at TIMESTAMP, created_by TEXT, costo TEXT);
CREATE TABLE mant_visitas (id INTEGER PRIMARY KEY, numero_ot TEXT, titulo TEXT, tipo TEXT, estado TEXT, fecha_programada DATE, hora_inicio TEXT,
  hora_real_inicio TIMESTAMP, fecha_realizada DATE, cerrada_at TIMESTAMP, ot_madre_id INT, ot_relacion TEXT, tecnico_user_id INT, tecnico TEXT,
  cliente_id INT, costo TEXT, oc_numero TEXT, descripcion TEXT, exec_gps_lat REAL, exec_gps_lng REAL, exec_gps_at TIMESTAMP, exec_gps_direccion TEXT);
CREATE TABLE mant_visita_tareas (id INTEGER PRIMARY KEY, visita_id INT, maquina_id INT, titulo TEXT, tipo_respuesta TEXT, valor_json TEXT,
  opciones_lista_json TEXT, completada_at TIMESTAMP, completada_por TEXT, observaciones TEXT);
CREATE TABLE mant_visita_equipos (id INTEGER PRIMARY KEY AUTOINCREMENT, visita_id INT, maquina_id INT, estado_revision TEXT, razon_saltado TEXT,
  observacion_tecnico TEXT, diagnostico_estado TEXT, diagnostico_texto TEXT, revisado_at TIMESTAMP, revisado_por TEXT);
CREATE TABLE mant_maquina_levantamientos (id INTEGER PRIMARY KEY AUTOINCREMENT, maquina_id INT, visita_id INT);
CREATE TABLE mant_levantamientos (id INTEGER PRIMARY KEY, visita_id INT);
CREATE TABLE mant_visita_tecnicos (visita_id INT, tecnico_user_id INT);
CREATE TABLE mant_ot_hallazgos (id INTEGER PRIMARY KEY, visita_id INT, origen TEXT, referencia TEXT, tarea_id INT, maquina_id INT,
  item_manual_id INT, severidad TEXT, titulo TEXT, detalle TEXT, equipo_nombre TEXT, riesgo INT, fotos_json TEXT, n_fotos INT, estado TEXT,
  detectado_por TEXT, detectado_en TIMESTAMP, tomado_por TEXT, tomado_en TIMESTAMP, ot_seguimiento_id INT, seguimiento_por TEXT,
  seguimiento_en TIMESTAMP, descartado_por TEXT, descartado_en TIMESTAMP, descarte_motivo TEXT, retirado_en TIMESTAMP, retirado_motivo TEXT);
CREATE TABLE mant_ot_repuesto_solicitudes (id INTEGER PRIMARY KEY, visita_id INT, maquina_id INT, ot_generada_id INT, repuesto_nombre TEXT,
  repuesto_sku TEXT, cantidad REAL, origen TEXT, estado TEXT, motivo TEXT, dejo_fuera_servicio INT, solicitado_por TEXT, created_at TIMESTAMP,
  validado_at TIMESTAMP, pedido_at TIMESTAMP, recibido_at TIMESTAMP, instalado_at TIMESTAMP, updated_at TIMESTAMP, es_reposicion INT,
  solicitud_padre_id INT, proveedor_id INT, oc_numero TEXT, costo_unitario REAL, costo_origen TEXT, nota_gestion TEXT, ticket_id INT, compra_id INT);
CREATE TABLE mant_ot_repuesto_evidencias (id INTEGER PRIMARY KEY AUTOINCREMENT, solicitud_id INT, tipo TEXT, url TEXT);
CREATE TABLE mant_maquina_paradas (id INTEGER PRIMARY KEY, maquina_id INT, inicio_at TIMESTAMP, fin_at TIMESTAMP, inicio_aprox INT,
  estado_inicio TEXT, estado_fin TEXT, motivo TEXT, abierta_por TEXT, cerrada_por TEXT, ref_tabla TEXT, ref_id INT, ref_fin_tabla TEXT, ref_fin_id INT);
CREATE TABLE mant_maquina_eventos (id INTEGER PRIMARY KEY, maquina_id INT, tipo TEXT, descripcion TEXT, fecha_evento TIMESTAMP,
  referencia_tabla TEXT, referencia_id INT, metadata_json TEXT, created_by TEXT);
CREATE TABLE mant_maquina_audit (id INTEGER PRIMARY KEY, maquina_id INT, campo TEXT, valor_antes TEXT, valor_nuevo TEXT, motivo TEXT,
  usuario TEXT, fecha TIMESTAMP);
CREATE TABLE tk_tickets (id INTEGER PRIMARY KEY, numero_ticket TEXT, tipo TEXT, estado TEXT, titulo TEXT, prioridad TEXT, created_at TIMESTAMP,
  cerrado_at TIMESTAMP, created_by TEXT);
CREATE TABLE tk_ticket_equipos (id INTEGER PRIMARY KEY AUTOINCREMENT, ticket_id INT, maquina_id INT);
CREATE TABLE mant_visita_fotos (id INTEGER PRIMARY KEY AUTOINCREMENT, visita_id INT, tarea_id INT, maquina_id INT, cloudinary_url TEXT, archivo_path TEXT);
"""
sqlite3.register_converter("TIMESTAMP", lambda b: dt.datetime.fromisoformat(b.decode()))
sqlite3.register_converter("DATE", lambda b: dt.date.fromisoformat(b.decode()))


class BD:
    """Una base SQLite con las tablas de la historia. Traduce %s → ? y deja un registro de TODO lo que se consultó."""

    def __init__(self, ddl=DDL):
        self.con = sqlite3.connect(":memory:", detect_types=sqlite3.PARSE_DECLTYPES)
        self.con.row_factory = sqlite3.Row
        self.con.executescript(ddl)
        self.consultas = []

    @staticmethod
    def _valor(v):
        if isinstance(v, dt.datetime):
            return v.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(v, dt.date):
            return v.isoformat()
        return v

    def insertar(self, tabla, filas):
        cols = {r[1] for r in self.con.execute("PRAGMA table_info(%s)" % tabla)}
        for f in filas:
            ks = [k for k in f if k in cols]
            self.con.execute("INSERT INTO %s (%s) VALUES (%s)" % (tabla, ",".join(ks), ",".join("?" * len(ks))), [self._valor(f[k]) for k in ks])

    def fetchall(self, sql, params=()):
        s = " ".join(sql.split())
        self.consultas.append(s)
        return [dict(r) for r in self.con.execute(s.replace("%s", "?"), tuple(params or ()))]

    def fetchone(self, sql, params=()):
        r = self.fetchall(sql, params)
        return r[0] if r else None

    def quitar(self, tabla):
        self.con.execute("DROP TABLE %s" % tabla)


def sembrar(bd, f=None, mid=9):
    """Mete en la base de mentira EXACTAMENTE el escenario de la trotadora, fila por fila, como lo dejarían las tablas reales."""
    f = f or escenario_trotadora()
    bd.insertar("mant_clientes", [{"id": 3, "razon_social": "Gimnasio Fit SpA"}])
    bd.insertar("mant_maquinas", [dict(f["equipo"], id=mid, costo="COSTO-123456")])
    for v in f["visitas"]:
        bd.insertar("mant_visitas", [dict(v, tecnico=v.get("tecnico_nombre"), cliente_id=3, costo="COSTO-123456", oc_numero="OC-99999")])
        bd.insertar("mant_visita_tareas", [{"id": 9000 + v["id"], "visita_id": v["id"], "maquina_id": mid, "titulo": "Limpieza", "tipo_respuesta": "check"}])
    bd.insertar("mant_visita_equipos", [dict(r, maquina_id=mid) for r in f["visita_equipos"]])
    bd.insertar("mant_visita_tareas", [dict(r, maquina_id=mid) for r in f["tareas"]])
    bd.insertar("mant_ot_hallazgos", [dict(r, maquina_id=mid) for r in f["hallazgos"]])
    bd.insertar("mant_ot_repuesto_solicitudes", [dict(r, maquina_id=mid, es_reposicion=0, proveedor_id=5, oc_numero="OC-99999", costo_unitario=123456.0,
                                                      nota_gestion="NOTA-INTERNA-DE-GESTION", ticket_id=77, compra_id=9) for r in f["solicitudes"]])
    bd.insertar("mant_maquina_paradas", [dict(r, maquina_id=mid) for r in f["paradas"]])
    bd.insertar("mant_maquina_eventos", [dict(r, maquina_id=mid) for r in f["eventos"]])
    bd.insertar("mant_maquina_audit", [dict(r, maquina_id=mid) for r in f["auditoria"]])
    bd.insertar("tk_tickets", f["tickets"])
    bd.insertar("tk_ticket_equipos", [{"ticket_id": t["id"], "maquina_id": mid} for t in f["tickets"]])
    # Hay un equipo vecino con sus propias cosas: nada de eso puede aparecer en la historia de éste.
    bd.insertar("mant_maquinas", [equipo(id=10, serie="OTRA-SERIE", nombre="Bicicleta Z")])
    bd.insertar("mant_visitas", [visita(999, numero_ot="OT-2026-00999")])
    bd.insertar("mant_visita_tareas", [{"id": 9999, "visita_id": 999, "maquina_id": 10, "titulo": "X", "tipo_respuesta": "verificacion",
                                        "valor_json": json.dumps({"valor": "falla"}), "completada_at": U(2026, 10, 1, 12, 0)}])
    bd.insertar("mant_ot_hallazgos", [hallazgo(900, 999, maquina_id=10, referencia="t9999", tarea_id=9999)])
    bd.insertar("mant_ot_repuesto_solicitudes", [dict(solicitud(900, 999), maquina_id=10, es_reposicion=0)])
    bd.insertar("mant_maquina_paradas", [dict(parada(900), maquina_id=10, ref_id=999)])
    bd.insertar("mant_maquina_audit", [dict(audit(900, "serie", "X", "Y"), maquina_id=10)])
    bd.insertar("mant_visita_tecnicos", [{"visita_id": 513, "tecnico_user_id": 7}])
    bd.con.commit()
    return bd


class Req:
    def __init__(self, **args):
        self.args = args


class Resp:
    def __init__(self, data):
        self.data, self.headers = data, {}


FUNCS_APP = ["_rol_familia", "_es_rol_tecnico", "_mant_calendario_role_where", "_mh_rol", "_mh_marcadores", "_mh_iso", "_mh_leer",
             "_mh_tecnico_puede", "_mh_cargar_fuentes", "_mh_fotos", "mant_maquina_historia_api", "_foto_url_actual", "_hall_url_foto"]
_CODIGO = {}


def cargar_rapido(funciones=(), constantes=(), extra=None):
    """Como tests/_fuente_app.cargar, pero recorta cada función de app.py UNA sola vez (buscar en 7 MB de texto es lo lento)."""
    import ast
    amb = dict(extra or {})
    for c in constantes:
        if ("c", c) not in _CODIGO:
            _CODIGO[("c", c)] = compile(ast.parse(fuente_constante(c)), "<app:%s>" % c, "exec")
        exec(_CODIGO[("c", c)], amb)
    for f in funciones:
        if ("f", f) not in _CODIGO:
            arbol = ast.parse(fuente_funcion(f))
            for nodo in arbol.body:
                if isinstance(nodo, ast.FunctionDef):
                    nodo.decorator_list = []
            _CODIGO[("f", f)] = compile(arbol, "<app:%s>" % f, "exec")
        exec(_CODIGO[("f", f)], amb)
    return amb


def entorno_app(bd, role="ejecutivo", superadmin=False, uid=7, **args):
    g = types.SimpleNamespace(user={"id": uid, "role": role, "username": "u"}, permissions={"superadmin": superadmin})
    amb = cargar_rapido(funciones=FUNCS_APP, constantes=["_MH_TOPE", "_MH_AUDIT_CAMPOS"], extra={
        "g": g, "request": Req(**args), "jsonify": Resp, "mysql_fetchall": bd.fetchall, "mysql_fetchone": bd.fetchone,
        "_hall_ok": lambda: True, "_hv_asegurar": lambda: None, "datetime": dt.datetime,
        "_now_chile": lambda: types.SimpleNamespace(date=lambda: HOY), "_mh": MH})
    amb["_hoy"] = HOY
    return amb


def llamar(bd, mid=9, **kw):
    """Corre la ruta tal cual está en app.py. → (cuerpo, status)."""
    args = {k: v for k, v in kw.items() if k not in ("role", "superadmin", "uid")}
    amb = entorno_app(bd, role=kw.get("role", "ejecutivo"), superadmin=kw.get("superadmin", False), uid=kw.get("uid", 7), **args)
    # la fecha de hoy y la hora de ahora son las del escenario (las fuentes traen su propio "ahora" real: se fija para que el resultado sea estable)
    original = amb["_mh_cargar_fuentes"]

    def fijo(mid_, rol, eq):
        f, fallidas, topadas = original(mid_, rol, eq)
        f["ahora"], f["hoy"] = AHORA, HOY
        return f, fallidas, topadas
    amb["_mh_cargar_fuentes"] = fijo
    r = amb["mant_maquina_historia_api"](mid)
    if isinstance(r, tuple):
        return r[0].data, r[1], r[0]
    return r.data, 200, r


class TestLecturaDeApp(unittest.TestCase):
    def setUp(self):
        self.bd = sembrar(BD())

    def test_la_base_de_mentira_devuelve_el_mismo_orden_que_el_escenario_puro(self):
        d, st, _ = llamar(self.bd)
        self.assertEqual(st, 200)
        self.assertTrue(d["ok"])
        self.assertEqual([e["id"] for e in d["entradas"]], ids(MH.construir(escenario_trotadora(), GESTION)))

    def test_cabecera_pastillas_resumen_y_meta(self):
        d, _, resp = llamar(self.bd)
        self.assertEqual(d["equipo"]["serie"], "SN-123")
        self.assertEqual(d["equipo"]["cliente"], "Gimnasio Fit SpA")
        self.assertEqual(d["todos"], 15)
        self.assertEqual({t["clave"]: t["n"] for t in d["tipos"]},
                         {"visita": 4, "diagnostico": 1, "checklist": 0, "hallazgo": 2, "repuesto": 1, "estado": 2, "ficha": 3, "ticket": 1, "garantia": 1})
        self.assertEqual(d["meta"], {"total": 15, "pagina": 1, "paginas": 1, "por_pagina": 20, "desde": 1, "hasta": 15})
        self.assertEqual(d["resumen"]["hallazgos_abiertos"], 1)
        self.assertTrue(d["gestion"])
        self.assertEqual(resp.headers.get("Cache-Control"), "no-store")
        self.assertEqual(d["fuentes_no_disponibles"], [])
        self.assertEqual(d["fuentes_topadas"], [])

    def test_paginacion_real(self):
        d1, _, _ = llamar(self.bd, pagina="1", por_pagina="10")
        d2, _, _ = llamar(self.bd, pagina="2", por_pagina="10")
        self.assertEqual((len(d1["entradas"]), d1["meta"]["paginas"]), (10, 2))
        self.assertEqual((len(d2["entradas"]), d2["meta"]["desde"], d2["meta"]["hasta"]), (5, 11, 15))
        self.assertEqual([e["id"] for e in d1["entradas"] + d2["entradas"]], [e["id"] for e in llamar(self.bd)[0]["entradas"]])

    def test_filtro_por_tipo_y_vaciarlo_vuelve_a_traer_todo(self):
        d, _, _ = llamar(self.bd, tipos="hallazgo")
        self.assertEqual([e["id"] for e in d["entradas"]], ["hallazgo:6", "hallazgo:5"])
        self.assertEqual(d["filtros"]["tipos"], ["hallazgo"])
        self.assertEqual(d["todos"], 15, "las pastillas siguen diciendo cuánto hay de cada tipo")
        d, _, _ = llamar(self.bd, tipos="")
        self.assertEqual(d["meta"]["total"], 15)
        self.assertEqual(d["filtros"]["tipos"], [])

    def test_filtro_por_fechas(self):
        d, _, _ = llamar(self.bd, desde="2026-10-01", hasta="2026-10-31")
        self.assertEqual(d["meta"]["total"], 7)
        self.assertEqual(d["todos"], 7)
        self.assertEqual((d["filtros"]["desde"], d["filtros"]["hasta"]), ("2026-10-01", "2026-10-31"))
        d, _, _ = llamar(self.bd, desde="")
        self.assertEqual(d["meta"]["total"], 15, "vaciar la fecha recarga todo (REGLA #4.3)")

    def test_parametros_malos_no_rompen(self):
        d, st, _ = llamar(self.bd, pagina="abc", por_pagina="7", desde="basura", hasta="2026-99-99", tipos="x,y;DROP TABLE")
        self.assertEqual(st, 200)
        self.assertEqual((d["meta"]["pagina"], d["meta"]["por_pagina"], d["meta"]["total"]), (1, 20, 15))
        self.assertEqual((d["filtros"]["desde"], d["filtros"]["hasta"], d["filtros"]["tipos"]), ("", "", []))

    def test_lo_del_equipo_vecino_no_aparece(self):
        d, _, _ = llamar(self.bd)
        js = todo_el_texto(d)
        for ajeno in ("hallazgo:900", "sol:900", "parada:900", "audit:900", "visita:999", "OTRA-SERIE", "OT-2026-00999"):
            self.assertNotIn(ajeno, js, ajeno)

    def test_otro_equipo_tiene_su_propia_historia(self):
        d, st, _ = llamar(self.bd, mid=10)
        self.assertEqual(st, 200)
        self.assertEqual(d["equipo"]["serie"], "OTRA-SERIE")
        self.assertEqual(sorted(e["id"] for e in d["entradas"] if e["id"][0] in "hspa" and e["id"] != "alta"), ["audit:900", "hallazgo:900", "parada:900", "sol:900"])

    def test_equipo_que_no_existe(self):
        d, st, _ = llamar(self.bd, mid=12345)
        self.assertEqual(st, 404)
        self.assertFalse(d["ok"])

    def test_solo_lee_nada_de_escritura(self):
        llamar(self.bd)
        llamar(self.bd, role="tecnico")
        self.assertTrue(self.bd.consultas)
        for q in self.bd.consultas:
            self.assertRegex(q, r"^SELECT ", q[:80])
            self.assertNotRegex(q, r"(?i)\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|REPLACE|TRUNCATE)\b", q[:120])

    def test_ninguna_consulta_pide_proveedor_costo_oc_nota_ni_ticket_de_compra(self):
        llamar(self.bd)
        llamar(self.bd, role="tecnico")
        for q in self.bd.consultas:
            self.assertNotRegex(q, r"(?i)proveedor|costo|oc_numero|nota_gestion|compra_id|valor_origen|monto|precio|margen", q[:160])
            if "mant_ot_repuesto_solicitudes" in q:
                self.assertNotIn("SELECT *", q)
                self.assertNotRegex(q, r"(?i)ticket_id", "el ticket de compra de un repuesto no se pide: " + q[:100])

    def test_los_datos_prohibidos_ni_siquiera_se_leen_de_la_base(self):
        f, _, _ = entorno_app(self.bd)["_mh_cargar_fuentes"](9, MH.Rol(False, True), self.bd.fetchone(
            "SELECT m.*, c.razon_social FROM mant_maquinas m JOIN mant_clientes c ON c.id = m.cliente_id WHERE m.id=%s", (9,)))
        for s in f["solicitudes"]:
            self.assertFalse({"proveedor_id", "oc_numero", "costo_unitario", "nota_gestion", "ticket_id", "compra_id"} & set(s), s.keys())
        for v in f["visitas"]:
            self.assertFalse({"costo", "oc_numero"} & set(v), v.keys())

    def test_todo_el_json_de_cada_rol_sin_sentinelas(self):
        for kw in ({}, {"superadmin": True}, {"role": "tecnico"}, {"role": "tecnico_ejecutivo"}, {"role": "tecnico_externo"}):
            d, st, _ = llamar(self.bd, **kw)
            if st != 200:
                continue
            js = todo_el_texto(d)
            for s in SENTINELAS:
                self.assertNotIn(s, js, "%s en %s" % (s, kw))


class TestLecturaParaElTecnico(unittest.TestCase):
    def setUp(self):
        self.bd = sembrar(BD())

    def test_un_tecnico_asignado_ve_su_historia_sin_hallazgos_ni_tickets(self):
        d, st, _ = llamar(self.bd, role="tecnico", uid=7)
        self.assertEqual(st, 200)
        self.assertFalse(d["gestion"])
        self.assertEqual([e["id"] for e in d["entradas"]], ["garantia", "visita:530", "sol:40", "parada:1", "tarea:71", "tarea:70", "visita:513",
                                                           "audit:1", "evento:6", "diag:200", "visita:200", "audit:3", "visita:100", "alta"])
        self.assertEqual([t["clave"] for t in d["tipos"]], ["visita", "diagnostico", "checklist", "repuesto", "estado", "ficha", "garantia"])
        self.assertTrue(all(e["actor"] is None and e["gestion"] is None for e in d["entradas"]))
        self.assertNotIn("hallazgos_abiertos", d["resumen"], "ni el nombre de lo que es solo de gestión")
        self.assertNotIn("tickets_abiertos", d["resumen"])

    def test_nunca_se_consultan_hallazgos_ni_tickets_para_un_tecnico(self):
        llamar(self.bd, role="tecnico", uid=7)
        for q in self.bd.consultas:
            self.assertNotIn("mant_ot_hallazgos", q)
            self.assertNotIn("tk_tickets", q)
            self.assertNotIn("tk_ticket_equipos", q)

    def test_solo_abre_enlaces_de_las_ot_que_son_suyas(self):
        d, _, _ = llamar(self.bd, role="tecnico", uid=7)
        e = {x["id"]: x for x in d["entradas"]}
        self.assertEqual(e["visita:513"]["ref"]["url"], "/ot/513")
        self.assertIsNone(e["visita:200"]["ref"]["url"])
        self.assertIsNone(e["visita:530"]["ref"]["url"])

    def test_un_tecnico_que_no_esta_en_ninguna_ot_del_equipo_no_entra(self):
        d, st, _ = llamar(self.bd, role="tecnico", uid=8)
        self.assertEqual(st, 403)
        self.assertFalse(d["ok"])
        self.assertNotIn("entradas", d)
        d, st, _ = llamar(self.bd, role="tecnico_externo", uid=8)
        self.assertEqual(st, 403)

    def test_el_tecnico_elevado_entra_como_en_la_ficha_pero_recibe_lo_mismo_que_un_tecnico(self):
        d, st, _ = llamar(self.bd, role="tecnico_ejecutivo", uid=8)
        self.assertEqual(st, 200, "la ficha del equipo no le cierra la puerta al elevado")
        self.assertFalse(d["gestion"])
        self.assertFalse([e for e in d["entradas"] if e["tipo"] in ("hallazgo", "ticket")])
        self.assertTrue(all(e["actor"] is None for e in d["entradas"]))
        self.assertEqual(d["entradas"][1]["ref"]["url"], None, "sin ser de su OT tampoco se le ofrece el enlace")

    def test_el_tecnico_no_puede_pedir_hallazgos_por_el_filtro(self):
        d, _, _ = llamar(self.bd, role="tecnico", uid=7, tipos="hallazgo,ticket")
        self.assertEqual(d["filtros"]["tipos"], [])
        self.assertEqual(d["meta"]["total"], 14)

    def test_un_tecnico_nunca_ve_quien_hizo_el_diagnostico_ni_siendo_superadmin_por_error(self):
        d, _, _ = llamar(self.bd, role="tecnico", uid=7, superadmin=True)
        self.assertTrue(all(e["actor"] is None for e in d["entradas"]))


class TestLecturaCuandoAlgoFalla(unittest.TestCase):
    def test_si_falta_una_tabla_la_historia_sale_igual_y_lo_dice(self):
        bd = sembrar(BD())
        bd.quitar("tk_tickets")
        d, st, _ = llamar(bd)
        self.assertEqual(st, 200)
        self.assertEqual(d["fuentes_no_disponibles"], ["tickets"])
        self.assertNotIn("ticket:3", [e["id"] for e in d["entradas"]])
        self.assertIn("visita:513", [e["id"] for e in d["entradas"]])

    def test_si_faltan_las_columnas_nuevas_del_diagnostico_usa_la_alternativa(self):
        ddl = DDL.replace("observacion_tecnico TEXT, diagnostico_estado TEXT, diagnostico_texto TEXT, revisado_at", "observacion_tecnico TEXT, revisado_at")
        bd = BD(ddl)
        bd.insertar("mant_clientes", [{"id": 3, "razon_social": "Gimnasio Fit SpA"}])
        bd.insertar("mant_maquinas", [equipo()])
        bd.insertar("mant_visitas", [visita(200)])
        bd.insertar("mant_visita_equipos", [dict(diag(200, observacion_tecnico="Rodillo flojo"), maquina_id=9)])
        d, st, _ = llamar(bd)
        self.assertEqual(st, 200)
        self.assertEqual(d["fuentes_no_disponibles"], [], "la alternativa sin las columnas nuevas funcionó")
        self.assertIn("diag:200", [e["id"] for e in d["entradas"]])

    def test_si_faltan_las_columnas_de_reposicion_usa_la_alternativa(self):
        ddl = DDL.replace("es_reposicion INT,\n  solicitud_padre_id INT, ", "").replace("updated_at TIMESTAMP, es_reposicion INT,\n  solicitud_padre_id INT,", "updated_at TIMESTAMP,")
        bd = BD(ddl.replace("es_reposicion INT, ", "").replace("solicitud_padre_id INT, ", ""))
        bd.insertar("mant_clientes", [{"id": 3, "razon_social": "Gimnasio Fit SpA"}])
        bd.insertar("mant_maquinas", [equipo()])
        bd.insertar("mant_ot_repuesto_solicitudes", [dict(solicitud(40, 513), maquina_id=9)])
        d, st, _ = llamar(bd)
        self.assertEqual(d["fuentes_no_disponibles"], [])
        self.assertIn("sol:40", [e["id"] for e in d["entradas"]])

    def test_si_la_base_se_cae_en_pleno_armado_responde_un_error_amable_sin_detalles(self):
        bd = sembrar(BD())
        amb = entorno_app(bd)

        def rota(*a, **k):
            raise RuntimeError("Access denied for user 'root'@'10.0.0.1' (password: SECRETO)")
        amb["_mh_cargar_fuentes"] = rota
        r = amb["mant_maquina_historia_api"](9)
        cuerpo, status = r[0].data, r[1]
        self.assertEqual(status, 500)
        self.assertFalse(cuerpo["ok"])
        self.assertNotIn("SECRETO", todo_el_texto(cuerpo))
        self.assertNotIn("root", todo_el_texto(cuerpo))

    def test_si_no_se_puede_ni_abrir_el_equipo_tampoco_filtra_el_error(self):
        bd = sembrar(BD())
        amb = entorno_app(bd)

        def rota(sql, params=()):
            raise RuntimeError("1146 Table 'ilus.mant_maquinas' doesn't exist")
        amb["mysql_fetchone"] = rota
        r = amb["mant_maquina_historia_api"](9)
        self.assertEqual(r[1], 500)
        self.assertNotIn("1146", todo_el_texto(r[0].data))

    def test_el_tope_de_filas_se_avisa(self):
        bd = sembrar(BD())
        amb = entorno_app(bd)
        amb["_MH_TOPE"] = 3
        f, fallidas, topadas = amb["_mh_cargar_fuentes"](9, MH.Rol(False, False), bd.fetchone(
            "SELECT m.*, c.razon_social FROM mant_maquinas m JOIN mant_clientes c ON c.id = m.cliente_id WHERE m.id=%s", (9,)))
        self.assertIn("visitas", topadas)
        self.assertIn("eventos", topadas)
        self.assertEqual(len(f["visitas"]), 3)


class TestFotosDeLaPagina(unittest.TestCase):
    def test_miniaturas_de_visitas_tareas_y_repuestos_solo_de_este_equipo(self):
        bd = sembrar(BD())
        bd.insertar("mant_visita_fotos", [
            {"visita_id": 513, "maquina_id": 9, "cloudinary_url": "/f/v513-1.jpg"}, {"visita_id": 513, "maquina_id": 9, "cloudinary_url": "/f/v513-2.jpg"},
            {"visita_id": 513, "maquina_id": 9, "cloudinary_url": "/f/v513-3.jpg"}, {"visita_id": 513, "maquina_id": 9, "cloudinary_url": "/f/v513-4.jpg"},
            {"visita_id": 513, "maquina_id": 10, "cloudinary_url": "/f/OTRO-EQUIPO.jpg"},
            {"visita_id": 513, "tarea_id": 70, "maquina_id": 9, "cloudinary_url": "/f/tarea70.jpg"},
            {"visita_id": 513, "tarea_id": 71, "maquina_id": 9, "cloudinary_url": None, "archivo_path": "uploads/viejo.jpg"},
            {"visita_id": 513, "tarea_id": 71, "maquina_id": 9, "cloudinary_url": None, "archivo_path": "/f/por-ruta.jpg"}])
        bd.insertar("mant_ot_repuesto_evidencias", [{"solicitud_id": 40, "tipo": "foto", "url": "/f/rep40.jpg"}, {"solicitud_id": 40, "tipo": "video", "url": "/f/video.mp4"}])
        bd.con.commit()
        amb = entorno_app(bd)
        es = MH.construir(escenario_trotadora(), TECNICO)          # el técnico tiene la tarea 70 y la 71 como tarjetas propias
        fotos = amb["_mh_fotos"](9, es)
        self.assertEqual(fotos["visita:513"], ["/f/v513-1.jpg", "/f/v513-2.jpg", "/f/v513-3.jpg"], "máximo 3 por visita")
        self.assertEqual(fotos["tarea:70"], ["/f/tarea70.jpg"])
        self.assertEqual(fotos["tarea:71"], ["/static/uploads/viejo.jpg", "/f/por-ruta.jpg"],
                         "como en el resto de la app: la URL de /f/ o, si es un archivo viejo del servidor, /static/…")
        self.assertEqual(fotos["sol:40"], ["/f/rep40.jpg"], "ni videos ni otra cosa que no sea foto")
        self.assertNotIn("/f/OTRO-EQUIPO.jpg", todo_el_texto(fotos))
        self.assertNotIn("visita:200", fotos)

    def test_si_la_consulta_de_fotos_falla_la_historia_sale_sin_fotos(self):
        bd = sembrar(BD())
        amb = entorno_app(bd)

        def rota(sql, params=()):
            raise RuntimeError("caída")
        amb["mysql_fetchall"] = rota
        self.assertEqual(amb["_mh_fotos"](9, MH.construir(escenario_trotadora(), GESTION)), {})

    def test_sin_tarjetas_no_consulta(self):
        bd = sembrar(BD())
        n = len(bd.consultas)
        self.assertEqual(entorno_app(bd)["_mh_fotos"](9, []), {})
        self.assertEqual(len(bd.consultas), n)


class TestPermisoDeLaFicha(unittest.TestCase):
    def test_solo_los_roles_tecnico_y_tecnico_externo_pasan_por_el_candado_de_ot_asignada(self):
        bd = sembrar(BD())
        for role, uid, esperado in (("tecnico", 7, True), ("tecnico", 8, False), ("tecnico_externo", 7, True), ("tecnico_externo", 8, False),
                                    ("tecnico_ejecutivo", 8, True), ("ejecutivo", 8, True), ("admin", 8, True), ("superadmin", 8, True)):
            amb = entorno_app(bd, role=role, uid=uid)
            self.assertEqual(amb["_mh_tecnico_puede"](9), esperado, (role, uid))

    def test_si_falla_la_consulta_del_permiso_no_se_abre(self):
        bd = sembrar(BD())
        amb = entorno_app(bd, role="tecnico", uid=7)

        def rota(sql, params=()):
            raise RuntimeError("caída")
        amb["mysql_fetchone"] = rota
        self.assertFalse(amb["_mh_tecnico_puede"](9))

    def test_es_la_misma_consulta_que_usa_la_ficha_del_equipo(self):
        ficha = fuente_funcion("mant_maquina_ficha")
        mio = fuente_funcion("_mh_tecnico_puede")
        for pedazo in ("mant_visita_tareas", "mant_visita_tecnicos", "v.tecnico_user_id=%s", '("tecnico", "tecnico_externo")'):
            self.assertIn(pedazo, ficha.replace("role_u in (\"tecnico\", \"tecnico_externo\")", '("tecnico", "tecnico_externo")'), pedazo)
            self.assertIn(pedazo, mio, pedazo)


class TestCableadoEnApp(unittest.TestCase):
    def test_la_ruta_es_solo_lectura_y_pide_el_permiso_de_mantenciones(self):
        decs = " ".join(decoradores("mant_maquina_historia_api"))
        self.assertIn('@app.route("/mantenciones/api/maquinas/<int:mid>/historia", methods=["GET"])', decs)
        self.assertIn("@_mant_required", decs)
        self.assertNotIn("_no_tecnico", decs, "el técnico SÍ abre la historia de los equipos que están en sus OT (como la ficha)")

    def test_el_bloque_no_escribe_en_ninguna_tabla_ni_hace_ddl(self):
        for nombre in ("_mh_leer", "_mh_cargar_fuentes", "_mh_fotos", "_mh_tecnico_puede", "mant_maquina_historia_api"):
            src = fuente_funcion(nombre)
            self.assertNotRegex(src, r"(?i)\b(INSERT\s+INTO|UPDATE\s+\w+\s+SET|DELETE\s+FROM|DROP\s+TABLE|ALTER\s+TABLE|CREATE\s+TABLE)\b", nombre)
            self.assertNotIn("mysql_execute", src, nombre)
            self.assertNotIn("commit(", src, nombre)

    def test_no_hay_ddl_nuevo_en_el_arranque_regla_18(self):
        app = texto("app.py")
        i = app.index("🧬 HISTORIA DEL EQUIPO (hoja de vida unificada) — 2026-10-21")
        j = app.index("# FICHA TÉCNICA INLINE (JSON) — usado en el modal", i)
        self.assertGreater(j - i, 10000, "se está revisando el bloque completo")
        self.assertNotRegex(app[i:j], r"(?i)CREATE\s+TABLE|ALTER\s+TABLE|ADD\s+COLUMN|mysql_execute\(")

    def test_la_ruta_pasa_por_el_modulo_puro_y_el_recorte_de_rol(self):
        src = fuente_funcion("mant_maquina_historia_api")
        for pedazo in ("_mh_rol()", "_mh.construir(", "_mh.filtrar_fechas(", "_mh.contar(", "_mh.filtrar_tipos(", "_mh.paginar(", "_mh.a_json(",
                       "_mh_tecnico_puede(mid)", "no-store"):
            self.assertIn(pedazo, src, pedazo)
        # las pastillas cuentan con las fechas pero ANTES del filtro de tipo
        self.assertLess(src.index("_mh.contar("), src.index("_mh.filtrar_tipos("))

    def test_a_un_tecnico_ni_se_le_consultan_hallazgos_ni_tickets(self):
        src = fuente_funcion("_mh_cargar_fuentes")
        i = src.index("if rol.gestion:")
        self.assertIn('"hallazgos"', src[i:i + 2500])
        self.assertIn('"tickets"', src[i:i + 2500])
        self.assertLess(src.index("f[\"hallazgos\"], f[\"tickets\"] = [], []"), i)

    def test_el_modulo_se_importa_en_app(self):
        self.assertIn("import maquina_historia as _mh", texto("app.py"))

    def test_la_prueba_de_confidencialidad_financiera_no_marca_la_ruta(self):
        nombre = "mant_maquina_historia_api"
        pat_nombre = re.compile(r"finanz|costo|factura|autoriz|regulariz|dashboard|facprov|margen|valoriz", re.I)
        pat_ruta = re.compile(r"/ot/autorizaciones|/ot/api/autorizaciones|/ot/regularizar|/ot/api/regularizar|/ot/finanzas|/ot/api/finanzas|"
                              r"/dashboard|/api/dashboard/|facturas-proveedor|facturacion-proveedores|/api/tarifas|tecnicos-externos|"
                              r"/api/contratos|vida-cliente|/finanzas|/ot/api/saldo|agente-contrato|/api/ia/")
        self.assertIsNone(pat_nombre.search(nombre))
        self.assertIsNone(pat_ruta.search("/mantenciones/api/maquinas/<int:mid>/historia"))
        # y la ruta no maneja plata: ninguna palabra financiera en su código ni en el del módulo
        for src in (fuente_funcion(nombre), fuente_funcion("_mh_cargar_fuentes"), _leer("maquina_historia.py")):
            sin_comentarios = "\n".join(l for l in src.splitlines() if not l.strip().startswith("#"))
            codigo = re.sub(r'""".*?"""', "", sin_comentarios, flags=re.S)
            self.assertNotRegex(codigo.lower(), r"\bcosto\b|\bmonto\b|\bprecio\b|\bmargen\b|oc_numero|proveedor_id|nota_gestion|compra_id|costo_unitario")


# ══════════════════ Requisito nuevo de Daniel: inteligente y conectada a las OT «sí o sí» ══════════════════
# DÓNDE está · QUÉ se hizo · POR QUÉ (falla o motivo, diagnóstico, hallazgo que lo originó) · CUÁNDO (hora Chile) · en qué OT.
class TestCuatroRespuestas(unittest.TestCase):
    def test_la_visita_dice_por_que_con_el_motivo_de_la_ot(self):
        es = construir(visitas=[visita(513, tipo="correctiva", descripcion="La trotadora no enciende desde el lunes")], numeros_ot={513: "OT-2026-00513"})
        t = por_id(es)["visita:513"]
        j = MH.a_json(t, ahora=AHORA)
        self.assertEqual((j["por_que"], j["pide_motivo"], j["ligada_ot"]), ("La trotadora no enciende desde el lunes", True, True))
        self.assertEqual(j["ref"]["numero"], "OT-2026-00513")

    def test_la_ot_de_seguimiento_dice_de_que_ot_viene_sin_hablar_de_hallazgos(self):
        es = construir(visitas=[visita(530, ot_madre_id=513, ot_relacion="seguimiento", descripcion="otro texto")], numeros_ot={513: "OT-2026-00513"})
        self.assertEqual(por_id(es)["visita:530"]["por_que"], "Seguimiento de OT-2026-00513: lo que quedó por resolver en esa OT")
        self.assertNotIn("hallazgo", por_id(es)["visita:530"]["por_que"].lower(), "un técnico también ve esta frase")
        es = construir(visitas=[visita(531, ot_madre_id=513, ot_relacion="pendiente_stock")], numeros_ot={513: "OT-2026-00513"})
        self.assertIn("falta de stock", por_id(es)["visita:531"]["por_que"])

    def test_si_falta_el_por_que_no_se_inventa_queda_vacio_para_decir_sin_motivo_registrado(self):
        d = por_id(construir(visitas=[visita(200)], tareas=[tarea(70, 513, "falla")], solicitudes=[solicitud(40, 513, motivo="")],
                             paradas=[parada(1, motivo=None)], auditoria=[audit(1, "serie", "A", "B", motivo="(sin motivo registrado)")]))
        for i in ("visita:200", "tarea:70", "sol:40", "parada:1", "audit:1"):
            self.assertEqual((d[i]["por_que"], d[i]["pide_motivo"]), ("", True), i)

    def test_de_donde_sale_el_por_que_de_cada_hecho(self):
        d = por_id(construir(
            visita_equipos=[diag(513, diagnostico_estado="falla", diagnostico_texto="Cinta desgastada, se ve la tela", estado_revision="falla_detectada")],
            tareas=[tarea(71, 513, "alerta", observaciones="Tensión floja")], solicitudes=[solicitud(40, 513)], paradas=[parada(1)],
            hallazgos=[hallazgo(7, 513, origen="equipo", referencia="m9:inc:5", tarea_id=None, severidad="incidencia", detalle="Rodillo con juego")],
            auditoria=[audit(1, "serie", "A", "B")]))
        self.assertEqual(d["diag:513"]["por_que"], "Cinta desgastada, se ve la tela")
        self.assertEqual(d["tarea:71"]["por_que"], "Tensión floja")
        self.assertEqual(d["sol:40"]["por_que"], "Cinta desgastada, se ve la tela")
        self.assertEqual(d["parada:1"]["por_que"], "Motor no enciende")
        self.assertEqual(d["hallazgo:7"]["por_que"], "Rodillo con juego")
        self.assertEqual(d["audit:1"]["por_que"], "Placa leída en terreno")

    def test_un_diagnostico_aprobado_o_la_garantia_no_piden_motivo(self):
        d = por_id(construir(visita_equipos=[diag(200, diagnostico_estado="aprobado")]))
        self.assertFalse(d["diag:200"]["pide_motivo"])
        self.assertFalse(d["garantia"]["pide_motivo"])
        self.assertFalse(d["alta"]["pide_motivo"])
        self.assertFalse(por_id(construir(tickets=[ticket(3)]))["ticket:3"]["pide_motivo"])

    def test_el_hallazgo_que_reemplaza_a_la_tarea_hereda_su_motivo(self):
        d = por_id(construir(tareas=[tarea(70, 513, "falla", observaciones="Se ve la tela")], hallazgos=[hallazgo(5, 513)]))
        self.assertEqual(d["hallazgo:5"]["por_que"], "Se ve la tela")

    def test_cada_hecho_de_terreno_enlaza_a_su_ot(self):
        es = MH.construir(escenario_trotadora(), GESTION)
        for e in es:
            if e["tipo"] in ("visita", "diagnostico", "checklist", "hallazgo", "repuesto") and e["id"] != "instalacion":
                self.assertTrue(MH.ligada_ot(e), "%s no está ligado a una OT" % e["id"])
                self.assertTrue(e["ref"]["numero"].startswith("OT-"))
        self.assertTrue(MH.ligada_ot(por_id(es)["parada:1"]))
        for e in es:
            if e["tipo"] in ("ficha", "garantia", "ticket"):
                self.assertFalse(MH.ligada_ot(e), e["id"])

    def test_las_fechas_de_las_cuatro_respuestas_van_en_hora_chile(self):
        for e in MH.construir(escenario_trotadora(), GESTION):
            j = MH.a_json(e, ahora=AHORA)
            self.assertRegex(j["fecha"], r"^\d{2}/\d{2}/\d{4}( \d{2}:\d{2})?$")
            if j["fecha_fin"]:
                self.assertRegex(j["fecha_fin"], r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$")


class TestOtAbiertasYCerradas(unittest.TestCase):
    def test_una_ot_abierta_se_marca_en_curso_y_una_cerrada_no(self):
        es = construir(visitas=[visita(513, tipo="correctiva", estado="en_ejecucion", cerrada_at=None, hora_real_inicio=U(2026, 10, 20, 15, 0)),
                                visita(200, estado="cerrada"), visita(300, estado="pendiente_repuesto", cerrada_at=None),
                                visita(400, estado="programada", fecha_programada=D(2026, 11, 3), cerrada_at=None)])
        d = por_id(es)
        self.assertEqual((d["visita:513"]["estado_txt"], d["visita:513"]["abierto"], d["visita:513"]["estado_color"]), ("En curso", True, "azul"))
        self.assertEqual((d["visita:300"]["estado_txt"], d["visita:300"]["abierto"]), ("En curso · Falta repuesto", True))
        self.assertEqual((d["visita:200"]["estado_txt"], d["visita:200"]["abierto"]), ("Cerrada", False))
        self.assertFalse(d["visita:400"]["abierto"], "una OT solo agendada todavía no está en curso")

    def test_los_enlaces_dicen_si_la_ot_esta_en_curso_o_cerrada(self):
        f = escenario_trotadora()
        f["estados_ot"] = {513: "en_ejecucion", 530: "cerrada", 100: "cerrada", 200: "completada"}
        d = por_id(MH.construir(f, GESTION))
        self.assertEqual((d["parada:1"]["ref"]["estado"], d["parada:1"]["ref"]["en_curso"]), ("En curso", True))
        self.assertEqual(d["parada:1"]["refs"][0]["estado"], "Cerrada")
        self.assertEqual(d["sol:40"]["refs"][0]["estado"], "Cerrada")
        self.assertEqual(MH.a_json(d["visita:200"])["ref"]["estado"], "Completada")

    def test_el_resumen_cuenta_las_ot_en_curso(self):
        es = construir(visitas=[visita(513, estado="en_curso", cerrada_at=None, hora_real_inicio=U(2026, 10, 20, 15, 0)), visita(200)])
        self.assertEqual(MH.resumen(es, AHORA)["ot_en_curso"], 1)


class TestSoloLoLigadoAOt(unittest.TestCase):
    def test_filtra_y_cuenta(self):
        es = MH.construir(escenario_trotadora(), GESTION)
        lig = MH.filtrar_ot(es)
        self.assertEqual(sorted(ids(lig)), sorted(["visita:530", "sol:40", "parada:1", "hallazgo:6", "hallazgo:5", "visita:513", "diag:200", "visita:200", "visita:100"]))
        self.assertEqual(MH.contar(es)["ot"], 9)
        self.assertNotIn("ticket:3", ids(lig))
        self.assertNotIn("audit:1", ids(lig))


class TestFraseDeArriba(unittest.TestCase):
    def _frase(self, f=None, rol=GESTION, gps=None):
        f = f or escenario_trotadora()
        es = MH.construir(f, rol)
        enc = MH.encabezado(f["equipo"], HOY)
        return MH.frase_resumen(enc, es, MH.resumen(es, AHORA), AHORA, rol, gps)

    def test_donde_que_por_que_cuando_y_hoy_con_datos_reales(self):
        fr = self._frase()
        self.assertEqual(fr["donde"], "Está en Gimnasio Fit SpA · Sala cardio.")
        self.assertEqual(fr["ultima"], "Última intervención: Correctiva el 16/10/2026 en OT-2026-00530 porque Seguimiento de OT-2026-00513: "
                                       "lo que quedó por resolver en esa OT.")
        self.assertEqual(fr["hoy"], "Hoy: operativo; 1 hallazgo sin cerrar; 1 ticket abierto.")
        self.assertEqual(fr["ot"], "OT-2026-00530")
        self.assertEqual(fr["texto"], " ".join((fr["donde"], fr["ultima"], fr["hoy"])))

    def test_si_la_ultima_visita_no_tiene_motivo_lo_dice_en_vez_de_inventarlo(self):
        f = fuentes(visitas=[visita(200, descripcion=None)], numeros_ot={200: "OT-2026-00200"})
        self.assertIn("en OT-2026-00200 porque sin motivo registrado.", self._frase(f)["ultima"])

    def test_una_ot_en_curso_se_dice_en_curso(self):
        f = fuentes(visitas=[visita(513, estado="en_curso", cerrada_at=None, hora_real_inicio=U(2026, 10, 20, 15, 0), descripcion="No enciende")],
                    numeros_ot={513: "OT-2026-00513"})
        fr = self._frase(f)
        self.assertIn("en OT-2026-00513 (en curso) porque No enciende.", fr["ultima"])
        self.assertIn("1 OT en curso", fr["hoy"])

    def test_hoy_fuera_de_servicio_con_pedidos(self):
        f = fuentes(visitas=[visita(513)], paradas=[parada(1)], solicitudes=[solicitud(40, 513), solicitud(41, 513)],
                    equipo=equipo(estado_capturado="fuera_servicio"))
        self.assertEqual(self._frase(f)["hoy"], "Hoy: fuera de servicio desde 07/10/2026 16:30; 2 pedidos de repuesto en curso.")

    def test_sin_intervenciones_y_sin_ubicacion(self):
        f = fuentes(equipo=equipo(ubicacion_sala=None, fecha_instalacion=None, fecha_fin_garantia=None))
        fr = self._frase(f)
        self.assertEqual(fr["donde"], "Está en Gimnasio Fit SpA.")
        self.assertEqual(fr["ultima"], "Todavía no tiene intervenciones registradas en ninguna OT.")
        self.assertEqual(fr["hoy"], "Hoy: operativo; sin pedidos, OT ni hallazgos abiertos.")
        self.assertIsNone(fr["ot"])

    def test_una_ot_cancelada_no_cuenta_como_ultima_intervencion(self):
        f = fuentes(visitas=[visita(600, estado="cancelada", fecha_programada=D(2026, 10, 18)), visita(200)], numeros_ot={200: "OT-2026-00200"})
        self.assertIn("OT-2026-00200", self._frase(f)["ultima"])

    def test_el_tecnico_no_recibe_hallazgos_ni_tickets_en_la_frase(self):
        fr = self._frase(rol=TECNICO)
        self.assertEqual(fr["hoy"], "Hoy: operativo; sin pedidos ni OT abiertas.")
        self.assertNotRegex(fr["texto"].lower(), r"hallazgo|ticket|proveedor")

    def test_el_gps_va_en_la_frase_solo_para_gestion(self):
        v = visita(513, exec_gps_lat=-33.4372, exec_gps_lng=-70.6506, exec_gps_at=U(2026, 10, 7, 16, 31), exec_gps_direccion="Av. Providencia 1234, Santiago")
        g = MH.ultimo_gps([visita(200), v], GESTION, {513: "OT-2026-00513"})
        self.assertEqual((g["fecha"], g["direccion"], g["ot"]), ("07/10/2026 13:31", "Av. Providencia 1234, Santiago", "OT-2026-00513"))
        self.assertTrue(g["url"].startswith("https://www.google.com/maps?q=-33.437200,-70.650600"))
        self.assertIn("Último GPS de visita: Av. Providencia 1234, Santiago (07/10/2026 13:31).", self._frase(gps=g)["donde"])
        self.assertIsNone(MH.ultimo_gps([v], TECNICO), "es la ubicación de una persona: un técnico no la recibe")
        self.assertIsNone(MH.ultimo_gps([visita(200)], GESTION))

    def test_gps_toma_el_mas_reciente(self):
        a = visita(1, exec_gps_lat=-1.0, exec_gps_lng=-1.0, exec_gps_at=U(2026, 3, 1, 12, 0))
        b = visita(2, exec_gps_lat=-2.0, exec_gps_lng=-2.0, exec_gps_at=U(2026, 9, 1, 12, 0))
        self.assertEqual(MH.ultimo_gps([a, b], GESTION)["lat"], -2.0)


class TestRutaRequisitoNuevo(unittest.TestCase):
    def setUp(self):
        self.bd = sembrar(BD())

    def test_la_respuesta_trae_la_frase_y_el_contador_de_ot(self):
        d, _, _ = llamar(self.bd)
        self.assertEqual(d["frase"]["donde"], "Está en Gimnasio Fit SpA · Sala cardio.")
        self.assertIn("Última intervención: Correctiva el 16/10/2026 en OT-2026-00530", d["frase"]["ultima"])
        self.assertEqual(d["n_ot"], 9)
        self.assertFalse(d["filtros"]["solo_ot"])
        self.assertIsNone(d["gps"])

    def test_solo_ot_filtra_y_se_combina_con_el_tipo_y_las_fechas(self):
        d, _, _ = llamar(self.bd, solo_ot="1")
        self.assertEqual(d["meta"]["total"], 9)
        self.assertTrue(all(e["ligada_ot"] for e in d["entradas"]))
        self.assertTrue(d["filtros"]["solo_ot"])
        self.assertEqual(d["todos"], 15, "las pastillas de tipo siguen contando todo")
        d, _, _ = llamar(self.bd, solo_ot="1", tipos="hallazgo")
        self.assertEqual([e["id"] for e in d["entradas"]], ["hallazgo:6", "hallazgo:5"])
        d, _, _ = llamar(self.bd, solo_ot="1", desde="2026-10-01", hasta="2026-10-31")
        self.assertEqual(d["meta"]["total"], 6)
        d, _, _ = llamar(self.bd, solo_ot="0")
        self.assertEqual(d["meta"]["total"], 15, "quitar el filtro vuelve a traer todo")
        d, _, _ = llamar(self.bd, solo_ot="basura")
        self.assertEqual(d["meta"]["total"], 15)

    def test_las_ot_abiertas_y_cerradas_aparecen_con_su_estado(self):
        self.bd.con.execute("UPDATE mant_visitas SET estado='en_ejecucion', cerrada_at=NULL WHERE id=513")
        self.bd.con.commit()
        d, _, _ = llamar(self.bd)
        e = {x["id"]: x for x in d["entradas"]}
        self.assertEqual((e["visita:513"]["estado_txt"], e["visita:513"]["abierto"]), ("En curso", True))
        self.assertEqual(e["visita:200"]["estado_txt"], "Cerrada")
        self.assertEqual(e["parada:1"]["ref"]["estado"], "En curso")
        self.assertEqual(e["parada:1"]["refs"][0]["estado"], "Cerrada")
        self.assertEqual(d["resumen"]["ot_en_curso"], 1)
        self.assertIn("1 OT en curso", d["frase"]["hoy"])

    def test_el_gps_de_la_visita_sale_para_gestion_y_no_para_el_tecnico(self):
        self.bd.con.execute("UPDATE mant_visitas SET exec_gps_lat=-33.4372, exec_gps_lng=-70.6506, exec_gps_at='2026-10-07 16:31:00', "
                            "exec_gps_direccion='Av. Providencia 1234' WHERE id=513")
        self.bd.con.commit()
        g, _, _ = llamar(self.bd)
        self.assertEqual(g["gps"]["direccion"], "Av. Providencia 1234")
        self.assertIn("Último GPS de visita", g["frase"]["donde"])
        t, _, _ = llamar(self.bd, role="tecnico", uid=7)
        self.assertIsNone(t["gps"])
        self.assertNotIn("Providencia", todo_el_texto(t))

    def test_el_tecnico_no_pide_el_gps_a_la_base(self):
        llamar(self.bd, role="tecnico", uid=7)
        for q in self.bd.consultas:
            self.assertNotIn("exec_gps", q)

    def test_si_no_existen_las_columnas_de_gps_cae_a_la_consulta_sin_ellas(self):
        ddl = DDL.replace(", exec_gps_lat REAL, exec_gps_lng REAL, exec_gps_at TIMESTAMP, exec_gps_direccion TEXT", "")
        bd = sembrar(BD(ddl))
        d, st, _ = llamar(bd)
        self.assertEqual(st, 200)
        self.assertEqual(d["fuentes_no_disponibles"], [])
        self.assertIsNone(d["gps"])
        self.assertIn("visita:513", [e["id"] for e in d["entradas"]])

    def test_el_tecnico_ve_las_cuatro_respuestas_sin_hallazgos_ni_plata(self):
        d, _, _ = llamar(self.bd, role="tecnico", uid=7)
        for e in d["entradas"]:
            self.assertIn("por_que", e)
            self.assertIn("ligada_ot", e)
        js = todo_el_texto(d)
        self.assertNotRegex(js.lower(), r"hallazgo|proveedor|ticket|costo|oc-99999")
        self.assertNotIn("sin gestionar", d["frase"]["hoy"])

    def test_la_consulta_de_visitas_pide_el_motivo_de_la_ot_y_su_estado(self):
        llamar(self.bd)
        q = " ".join(self.bd.consultas)
        self.assertIn("v.descripcion", q)
        self.assertIn("SELECT id, numero_ot, estado FROM mant_visitas", q)


# ══════════════════ C) las plantillas ══════════════════
def _entorno(extra_globals=None):
    def cargador(nombre):
        if nombre == "base.html":
            return "{% block title %}{% endblock %}<main>{% block content %}{% endblock %}</main>"
        return _leer("templates/" + nombre)

    env = jinja2.Environment(loader=jinja2.FunctionLoader(cargador), extensions=["jinja2.ext.do", "jinja2.ext.loopcontrols"],
                             undefined=jinja2.ChainableUndefined, autoescape=True)
    env.globals["url_for"] = lambda ep, **kw: ("/static/" + kw.get("filename", "")) if ep == "static" else \
        {"mant_maquina_ficha": "/mantenciones/maquinas/%s" % kw.get("mid"), "mant_ficha": "/mantenciones/clientes/%s" % kw.get("cid"),
         "mant_ot_ficha": "/ot/%s" % kw.get("vid")}.get(ep, "/%s" % ep)
    env.filters["chile_fmt"] = lambda v, f="": str(v)
    env.globals.update(extra_globals or {})
    return env


def _stats():
    return {"total_ots": 3, "ots_cerradas": 2, "ots_abiertas": 1, "ots_canceladas": 0, "total_fotos": 0, "tecnicos": ["Lenin"],
            "ultima_visita": "2026-10-07", "por_tipo": {"correctiva": 3}}


def render_ficha_equipo(tab=None, disponibilidad=None, problemas=None):
    req = types.SimpleNamespace(args={"tab": tab} if tab is not None else {})
    return _entorno({"request": req}).get_template("mantenciones/maquina_ficha.html").render(
        equipo=equipo(foto_url=None, fs_info=None), problemas=problemas or [], ots=[], fotos=[], eventos=[], disponibilidad=disponibilidad,
        stats=_stats(), levantamientos=[], ultimo_levantamiento=None, is_admin=False, es_superadmin=False)


class TestPlantillas(unittest.TestCase):
    def test_todas_las_plantillas_tocadas_son_jinja_valido(self):
        env = jinja2.Environment(extensions=["jinja2.ext.do", "jinja2.ext.loopcontrols"])
        for t in ("mantenciones/maquina_ficha.html", "mantenciones/ficha.html", "mantenciones/_problemas.html", "ot2/_hallazgos.html", "ot2/detalle.html"):
            env.parse(_leer("templates/" + t))

    def test_la_ficha_del_equipo_abre_por_defecto_la_historia(self):
        html = render_ficha_equipo()
        self.assertRegex(html, r'<button class="eq-tab active" data-tab="historia"')
        self.assertRegex(html, r'<div id="tab-eq-historia">')
        self.assertRegex(html, r'<div id="tab-eq-historial" style="display:none">')
        self.assertIn('id="mhRaiz" data-mid="9"', html)
        self.assertIn("/static/maquina_historia.js", html)
        self.assertIn("/static/maquina_historia.css", html)
        self.assertIn("window.MaquinaHistoria.activar()", html)
        self.assertEqual(html.count('class="eq-tab active"'), 1, "una sola pestaña activa")

    def test_no_se_quito_ninguna_pestana_de_antes(self):                                   # REGLA #4.2
        html = render_ficha_equipo(disponibilidad={"pct": 99.5, "detenida": False, "n_paradas": 0, "horas_paradas": 0, "desde_txt": "05/10/2026",
                                                   "paradas": []})
        for texto in ("Historial intervenciones (0)", "Levantamientos (0)", "Galería (0)", "Eventos (0)", "Disponibilidad (99.5 %)", "Historia del equipo"):
            self.assertIn(texto, re.sub(r"\s+", " ", html), texto)
        for pane in ("historia", "historial", "levantamientos", "fotos", "eventos", "disponibilidad"):
            self.assertIn('id="tab-eq-%s"' % pane, html, pane)

    def test_tab_en_la_url_abre_esa_pestana(self):
        for tab in ("historial", "levantamientos", "fotos", "eventos"):
            html = render_ficha_equipo(tab=tab)
            self.assertRegex(html, r'<button class="eq-tab active" data-tab="%s"' % tab)
            self.assertIn('<div id="tab-eq-%s">' % tab, html)
            self.assertIn('<div id="tab-eq-historia" style="display:none">', html)
            self.assertNotIn("window.MaquinaHistoria.activar();", html.split("function cambiarTabEq")[1].split("</script>")[0].split("catch")[1], tab)

    def test_la_pestana_disponibilidad_solo_si_hay_disponibilidad(self):
        disp = {"pct": 99.5, "detenida": False, "n_paradas": 0, "horas_paradas": 0, "desde_txt": "05/10/2026", "paradas": []}
        self.assertIn('<div id="tab-eq-disponibilidad">', render_ficha_equipo(tab="disponibilidad", disponibilidad=disp))
        sin = render_ficha_equipo(tab="disponibilidad")
        self.assertIn('<div id="tab-eq-historia">', sin, "sin disponibilidad no queda una página en blanco: cae en la historia")
        self.assertNotIn('id="tab-eq-disponibilidad"', sin)

    def test_un_tab_raro_no_se_repite_en_la_pagina_y_cae_en_la_historia(self):
        html = render_ficha_equipo(tab='"><script>alert(1)</script>')
        self.assertIn('<div id="tab-eq-historia">', html)
        self.assertNotIn("alert(1)", html)

    def test_la_tarjeta_de_problemas_lleva_el_enlace_a_la_historia(self):
        prob = [{"abierto": True, "color": "rojo", "icono": "bi-sign-stop-fill", "titulo": "Fuera de servicio: x", "levantado": "07/10/2026 13:30",
                 "levantado_ot": None, "levantado_por": "", "estado_txt": "Sigue detenido", "resuelto_txt": "", "resuelto": "", "resuelto_ot": None,
                 "resuelto_por": ""}]
        html = render_ficha_equipo(problemas=prob)
        self.assertIn('href="?tab=historia" onclick="cambiarTabEq(\'historia\');return false;"', html)
        self.assertIn("Ver historia del equipo", html)
        self.assertIn('id="hvProblemas"', html, "la tarjeta de siempre sigue ahí")

    def test_la_ficha_del_cliente_enlaza_la_historia_de_cada_equipo(self):
        ficha = _leer("templates/mantenciones/ficha.html")
        self.assertIn("{{ url_for('mant_maquina_ficha', mid=m.id) }}?tab=historia", ficha)
        self.assertIn("Ver historia del equipo", ficha)
        self.assertIn("Hoja de vida del equipo", ficha, "el enlace de antes sigue")

    def test_hallazgos_enlaza_la_historia_solo_de_los_equipos_de_la_ficha(self):
        def grupo(key):
            it = {"id": 1, "sev_color": "rojo", "sev_icono": "bi-x", "sev_texto": "Falla", "estado_ef": "abierto", "estado_color": "ambar",
                  "estado_txt": "Sin gestionar", "titulo": "Piola cortada", "fotos": [], "puede_tomar": True, "puede_levantar": True,
                  "puede_descartar": True, "puede_reabrir": False}
            h = {"vid": 171, "numero_ot": "OT-2026-00171", "n_total": 1, "n_abiertos": 1, "activos": [it], "retirados": [], "origen": None,
                 "n_seleccionables": 1, "grupos": [{"key": key, "nombre": "Polea alta", "serie": "SN-1", "sku": "1121", "color": "rojo",
                                                    "n_abiertos": 1, "items": [it]}]}
            return _entorno().get_template("ot2/_hallazgos.html").render(hallazgos=h, es_tecnico=False, hz_pagina=True)

        self.assertIn('class="hz-hist" href="/mantenciones/maquinas/9?tab=historia"', grupo("e9"))
        for no in ("i4", "ot", "e", "ex", "e0", ""):
            self.assertNotIn("hz-hist", grupo(no), "clave %r no es un equipo de la ficha" % no)

    def test_el_resumen_de_la_ot_enlaza_la_historia_de_equipos_no_de_productos(self):
        det = _leer("templates/ot2/detalle.html")
        i = det.index('<ul class="otd-res-lista">')
        j = det.index("</ul>", i)
        bloque = det[i:j]
        self.assertIn("{% if not r.interno and r.id %}", bloque)
        self.assertIn("{{ url_for('mant_maquina_ficha', mid=r.id) }}?tab=historia", bloque)
        self.assertIn('onclick="otdResIr(this.dataset.resKey)"', bloque, "el botón de siempre sigue ahí")

    def test_los_iconos_que_usa_la_historia_existen(self):
        css = _leer("static/bootstrap-icons.css")
        usados = set()
        for archivo in ("static/maquina_historia.js", "static/maquina_historia.css", "maquina_historia.py"):
            usados |= set(re.findall(r"\bbi-[a-z0-9]+(?:-[a-z0-9]+)*", _leer(archivo)))
        usados -= {"bi-icons"}
        faltan = sorted(i for i in usados if ".%s::before" % i not in css)
        self.assertEqual(faltan, [], "íconos que no existen en bootstrap-icons local: %s" % faltan)

    def test_el_css_cumple_44px_16px_y_safe_area(self):
        css = _leer("static/maquina_historia.css")
        self.assertIn("min-height:44px", css)
        self.assertIn("font-size:16px", css)
        self.assertIn("env(safe-area-inset-bottom)", css)
        self.assertIn("prefers-reduced-motion", css)
        self.assertIn(".mh [hidden]{display:none!important}", css)
        for color in ("#dc2626", "#0a0a0a", "#16a34a", "#f59e0b"):
            self.assertIn(color, css)


# ══════════════════ D) el JS (con Node, si hay) ══════════════════
SCRIPT_NODE = r"""
const fs = require('fs'), vm = require('vm');
const sb = { document: { getElementById: () => null }, location: { search: '', pathname: '/x' } };
sb.window = sb;
vm.createContext(sb);
vm.runInContext(fs.readFileSync(process.argv[1], 'utf8'), sb);      // con «node -e», argv[1] es el primer argumento
const T = sb.MaquinaHistoria._test;
const base = { id: 'hallazgo:7', tipo: 'hallazgo', tipo_texto: 'Hallazgos', subtipo: 'falla', icono: 'bi-exclamation-diamond-fill', severidad: 'falla',
  titulo: '<img src=x onerror=alert(1)> Trotadora "X"', detalle: '<script>alert(2)</script>\nsegunda línea', fecha: '07/10/2026 13:30', fecha_fin: '16/10/2026 12:30',
  mes: '2026-10', mes_txt: 'octubre de 2026', abierto: true, futuro: false, estado_txt: 'Sin gestionar', estado_color: 'ambar', riesgo: true,
  lineas: [{ icono: 'bi-person-badge', texto: 'Técnico: <b>Lenin</b>' }, { icono: 'javascript:alert(1)', texto: 'icono malo' }],
  pasos: [{ texto: 'Reportado', fecha: '07/10/2026 13:30', hecho: true }, { texto: 'Falta <i>algo</i>', fecha: '', hecho: false }],
  ref: { texto: 'Ver OT OT-1', url: '/ot/1', numero: 'OT-1' },
  refs: [{ texto: 'Ver algo', url: 'javascript:alert(3)', numero: 'X-9' }, { texto: 'Ver otra', url: '//evil.com/x', numero: '' }],
  actor: 'Aarón <x>', gestion: { texto: 'Hallazgo: sin gestionar', color: 'ambar', estado: 'Sin gestionar', seguimiento: { numero: 'OT-530', url: '/ot/530' } },
  fotos: ['/f/a.jpg', 'javascript:alert(4)', 'https://cdn/x.jpg'] };
const out = {
  hostil: T.tarjetaHtml(base),
  sinActor: T.tarjetaHtml(Object.assign({}, base, { actor: null, gestion: null, riesgo: false, fotos: [], refs: [], ref: null, lineas: [], pasos: [] })),
  sinUrl: T.cuatroHtml({ ref: { texto: 'Ver OT OT-9', url: null, numero: 'OT-9' }, refs: [], fecha: '01/01/2026', fecha_fin: '', pide_motivo: false, por_que: '' }),
  pie: T.pieHtml({ total: 134, desde: 1, hasta: 20, pagina: 1, paginas: 7, por_pagina: 20 }),
  pieUltima: T.pieHtml({ total: 134, desde: 121, hasta: 134, pagina: 7, paginas: 7, por_pagina: 50 }),
  pieVacio: T.pieHtml({ total: 0, desde: 0, hasta: 0, pagina: 1, paginas: 1, por_pagina: 20 }),
  pills: T.pillsHtml({ todos: 15, tipos: [{ clave: 'visita', texto: 'Visitas y OT', icono: 'bi-clipboard2-pulse-fill', n: 4 },
    { clave: 'checklist', texto: 'Fallas del checklist', icono: 'bi-list-check', n: 0 }, { clave: 'hallazgo', texto: 'Hallazgos', icono: 'bi-exclamation-diamond-fill', n: 0 }] }, 'hallazgo'),
  pend: T.pendienteHtml({ fuera_servicio: { desde: '07/10/2026 13:30', hace: 'hace 14 días' }, hallazgos_abiertos: 2, repuestos_abiertos: 1, tickets_abiertos: 0,
    ultima_visita: { fecha: '16/10/2026' } }),
  pendLimpio: T.pendienteHtml({}),
  cab: T.cabeceraHtml({ equipo: { nombre: 'Trotadora <b>X</b>', serie: 'SN-1', sku: 'TR', cliente: 'Gimnasio Fit', estado: { texto: 'Operativo', color: 'verde' },
    garantia: { texto: 'Vigente hasta el 10/01/2027', color: 'verde' } }, resumen: {} }),
  vacioF: T.vacioHtml(true), vacio: T.vacioHtml(false), err: T.errorHtml('<b>mal</b>'),
  lista: T.listaHtml([Object.assign({}, base, { id: 'visita:1' }), Object.assign({}, base, { id: 'visita:2' }), Object.assign({}, base, { id: 'visita:3', mes: '2026-09', mes_txt: 'septiembre de 2026' })]),
  urls: [T.urlOk('/ot/1'), T.urlOk('//x.com'), T.urlOk('javascript:alert(1)'), T.urlOk('https://a.b/c'), T.urlOk('http://a.b/c'), T.urlOk(null), T.urlOk('  /ok  ')],
  cuatro: T.cuatroHtml({ ref: { texto: 'Ver OT OT-1', url: '/ot/1', numero: 'OT-1', estado: 'En curso', en_curso: true }, fecha: '07/10/2026 13:30', fecha_fin: '', pide_motivo: true, por_que: 'Cinta <b>gastada</b>' }),
  sinMotivo: T.cuatroHtml({ ref: null, fecha: '07/10/2026', fecha_fin: '', pide_motivo: true, por_que: '' }),
  sinPide: T.cuatroHtml({ ref: null, fecha: '07/10/2026', fecha_fin: '', pide_motivo: false, por_que: '' }),
  cerrada: T.cuatroHtml({ ref: { texto: 'Ver OT OT-2', url: '/ot/2', numero: 'OT-2', estado: 'Cerrada', en_curso: false }, fecha: '01/01/2026', fecha_fin: '', pide_motivo: false, por_que: '' }),
  frase: T.fraseHtml({ frase: { donde: 'Está en <Gym> · Sala 1.', ultima: 'Última intervención: Correctiva el 16/10/2026 en OT-9 porque sin motivo registrado.', hoy: 'Hoy: operativo.' }, gps: { url: 'https://www.google.com/maps?q=-33.4,-70.6' } }),
  fraseSinGps: T.fraseHtml({ frase: { donde: 'a', ultima: 'b', hoy: 'c' }, gps: { url: 'javascript:alert(1)' } }),
  pillsOt: T.pillsHtml({ todos: 15, n_ot: 9, tipos: [] }, '', true),
  pillsSinOt: T.pillsHtml({ todos: 15, n_ot: 9, tipos: [] }, '', false),
  igual: T.tarjetaHtml(Object.assign({}, base, { detalle: 'mismo texto', por_que: 'mismo texto', pide_motivo: true, ligada_ot: true })),
  esc: T.esc('<>&"\'')
};
process.stdout.write(JSON.stringify(out));
"""


@unittest.skipUnless(HAY_NODE, "node no está instalado")
class TestJavaScript(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ruta = os.path.join(RAIZ, "static", "maquina_historia.js")
        r = subprocess.run(["node", "-e", SCRIPT_NODE, ruta], capture_output=True, text=True, encoding="utf-8", timeout=60)
        if r.returncode != 0:
            raise AssertionError("node falló: " + r.stderr[:1500])
        cls.o = json.loads(r.stdout)

    def test_las_cuatro_respuestas_por_que_cuando_y_ot(self):
        c = self.o["cuatro"]
        for pedazo in ("Por qué", "Cinta &lt;b&gt;gastada&lt;/b&gt;", "Cuándo", "<time>07/10/2026 13:30</time>", 'href="/ot/1"', "Ver OT OT-1", "En curso", "mh-chip c-azul"):
            self.assertIn(pedazo, c, pedazo)
        self.assertNotIn("<b>gastada", c)
        self.assertIn("Cerrada", self.o["cerrada"])
        self.assertIn("mh-chip c-gris", self.o["cerrada"])

    def test_si_falta_el_motivo_dice_sin_motivo_registrado_en_gris_y_no_inventa(self):
        sm = self.o["sinMotivo"]
        self.assertIn('<span class="mh-sin">sin motivo registrado</span>', sm)
        self.assertIn('<span class="mh-sin">sin OT ligada</span>', sm)
        self.assertNotIn("Por qué", self.o["sinPide"])
        self.assertNotIn("sin motivo", self.o["sinPide"])
        self.assertNotIn("<dt>OT</dt>", self.o["sinPide"])

    def test_la_tarjeta_ligada_lo_dice_y_no_repite_el_detalle_igual_al_motivo(self):
        h = self.o["igual"]
        self.assertIn("Ligado a OT", h)
        self.assertEqual(h.count("mismo texto"), 1)

    def test_el_resumen_de_arriba_y_la_pastilla_solo_ot(self):
        f = self.o["frase"]
        for pedazo in ("Está en &lt;Gym&gt; · Sala 1.", "Última intervención: Correctiva", "porque sin motivo registrado.", "Hoy: operativo.", "Ver en el mapa",
                       'href="https://www.google.com/maps?q=-33.4,-70.6"'):
            self.assertIn(pedazo, f, pedazo)
        self.assertNotIn("Ver en el mapa", self.o["fraseSinGps"])
        self.assertNotIn("javascript:", self.o["fraseSinGps"])
        self.assertIn('class="mh-pill ot on" data-solo-ot="1" aria-pressed="true"', self.o["pillsOt"])
        self.assertIn("Solo lo ligado a una OT <b>9</b>", self.o["pillsSinOt"])
        self.assertIn('aria-pressed="false"', self.o["pillsSinOt"].split("data-solo-ot")[1])

    def test_solo_ot_viaja_al_servidor_y_se_quita_con_quitar_filtros(self):
        js = _leer("static/maquina_historia.js")
        for pedazo in ("if (st.soloOt) p.set('solo_ot', '1')", "st.soloOt = !st.soloOt; st.pagina = 1; cargar(false)", "st.soloOt = false; st.desde = ''",
                       "['solo_ot', st.soloOt ? '1' : '']"):
            self.assertIn(pedazo, js, pedazo)

    def test_sintaxis(self):
        r = subprocess.run(["node", "--check", os.path.join(RAIZ, "static", "maquina_historia.js")], capture_output=True, text=True, timeout=60)
        self.assertEqual(r.returncode, 0, r.stderr)

    def test_todo_texto_del_servidor_se_escapa(self):
        h = self.o["hostil"]
        for crudo in ("<img src=x", "onerror=alert(1)>", "<script>", "<b>Lenin</b>", "<i>algo</i>", "<x>"):
            self.assertNotIn(crudo, h, crudo)
        self.assertIn("&lt;img src=x onerror=alert(1)&gt; Trotadora &quot;X&quot;", h)
        self.assertIn("&lt;script&gt;alert(2)&lt;/script&gt;", h)
        self.assertNotIn("javascript:", h, "ni como href, ni como src, ni como ícono")

    def test_los_enlaces_malos_no_se_dibujan_y_los_buenos_si(self):
        h = self.o["hostil"]
        self.assertIn('href="/ot/1"', h)
        self.assertIn('href="/ot/530"', h)
        self.assertNotIn("evil.com", h)
        self.assertEqual(self.o["urls"], ["/ot/1", "", "", "https://a.b/c", "", "", "/ok"])
        self.assertIn('src="/f/a.jpg"', h)
        self.assertIn('src="https://cdn/x.jpg"', h)
        self.assertEqual(h.count('class="mh-foto"'), 2, "la foto con javascript: no se dibuja")

    def test_una_ot_sin_acceso_se_ve_sin_enlace(self):
        s = self.o["sinUrl"]
        self.assertNotIn("<a ", s)
        self.assertIn("OT-9", s)
        self.assertIn("bi-lock-fill", s)

    def test_la_tarjeta_muestra_semaforo_estado_fechas_y_pasos(self):
        h = self.o["hostil"]
        for pedazo in ("mh-card s-falla abierto", 'data-id="hallazgo:7"', 'id="mh-hallazgo-7"', "Hallazgos", "Falla</span>", "Sin gestionar",
                       "Puede lastimar a alguien", '<time>07/10/2026 13:30 <span class="mh-hasta">→ 16/10/2026 12:30</span></time>', "Registró Aarón", "Hallazgo: sin gestionar",
                       "OT de seguimiento", "mh-pasos", "Reportado", "Falta &lt;i&gt;algo&lt;/i&gt;"):
            self.assertIn(pedazo, h, pedazo)

    def test_sin_actor_ni_gestion_no_se_dibujan(self):
        s = self.o["sinActor"]
        self.assertNotIn("Registró", s)
        self.assertNotIn("mh-gest", s)
        self.assertNotIn("Puede lastimar", s)
        self.assertNotIn("mh-fotos", s)
        self.assertNotIn("mh-pasos", s)

    def test_el_pie_es_el_de_etiquetas(self):
        p = self.o["pie"]
        for pedazo in ("Mostrando <b>1–20</b> de <b>134</b>", "Página 1 de 7", "Anterior", "Siguiente", "20 por página", 'value="20" selected',
                       'data-accion="ant" disabled'):
            self.assertIn(pedazo, p, pedazo)
        u = self.o["pieUltima"]
        self.assertIn("Mostrando <b>121–134</b> de <b>134</b>", u)
        self.assertIn('data-accion="sig" disabled', u)
        self.assertIn('value="50" selected', u)
        self.assertIn("Sin resultados", self.o["pieVacio"])

    def test_las_pastillas_ocultan_lo_vacio_salvo_la_activa(self):
        p = self.o["pills"]
        self.assertIn("Todos <b>15</b>", p)
        self.assertIn("Visitas y OT <b>4</b>", p)
        self.assertNotIn("Fallas del checklist", p, "un tipo sin registros no ocupa lugar")
        self.assertIn('class="mh-pill on" data-tipo="hallazgo"', p, "pero el filtro activo se queda aunque no tenga nada")
        self.assertIn('data-tipo="" aria-pressed="false"', p)

    def test_lo_pendiente_y_la_cabecera(self):
        self.assertIn("Fuera de servicio desde 07/10/2026 13:30 (hace 14 días)", self.o["pend"])
        self.assertIn("2 hallazgos sin gestionar", self.o["pend"])
        self.assertIn("1 repuesto en curso", self.o["pend"])
        self.assertIn("Última visita: 16/10/2026", self.o["pend"])
        self.assertIn("Sin pendientes", self.o["pendLimpio"])
        cab = self.o["cab"]
        self.assertIn("Historia de Trotadora &lt;b&gt;X&lt;/b&gt;", cab)
        self.assertIn("SN-1", cab)
        self.assertIn("Vigente hasta el 10/01/2027", cab)

    def test_estados_vacio_con_filtros_y_error(self):
        self.assertIn("Quitar filtros", self.o["vacioF"])
        self.assertIn("No hay registros con estos filtros", self.o["vacioF"])
        self.assertIn("todavía no tiene historia registrada", self.o["vacio"])
        self.assertNotIn("Quitar filtros", self.o["vacio"])
        self.assertIn("Reintentar", self.o["err"])
        self.assertIn("&lt;b&gt;mal&lt;/b&gt;", self.o["err"])

    def test_separa_por_mes(self):
        h = self.o["lista"]
        self.assertEqual(h.count("mh-mes"), 2)
        self.assertLess(h.index("octubre de 2026"), h.index("septiembre de 2026"))

    def test_sin_alert_confirm_prompt_nativos(self):                                     # REGLA #1
        js = _leer("static/maquina_historia.js")
        codigo = "\n".join(l for l in js.splitlines() if not l.strip().startswith(("*", "/*", "//")))
        self.assertNotRegex(codigo, r"(?<![A-Za-z_.])(alert|confirm|prompt)\s*\(")

    def test_cada_filtro_vuelve_a_pedir_al_servidor(self):                                 # REGLA #4.3
        js = _leer("static/maquina_historia.js")
        for pedazo in ("st.pagina = 1; cargar(false)", "st.desde = t.value || ''", "st.hasta = t.value || ''", "p.set('por_pagina', st.pp)",
                       "if (st.desde) p.set('desde', st.desde)", "if (st.hasta) p.set('hasta', st.hasta)", "if (st.tipo) p.set('tipos', st.tipo)",
                       "ilus_mh_pp", "if (mi !== token) return;"):
            self.assertIn(pedazo, js, pedazo)
        self.assertIn("vaciar el campo también recarga", js)


if __name__ == "__main__":
    unittest.main()
