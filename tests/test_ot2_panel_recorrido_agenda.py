# -*- coding: utf-8 -*-
"""🧭 2026-10-18 — Tabla de OT: recorrido real por fila, semáforo y filtro rápido de agenda.

Daniel (dueño, viendo la tabla): «necesito que mejores esa tabla. Es muy, muy simple. Tiene información, pero está como
desordenado…». Sobre Avance: «podría ser fiel al tracking, ¿me entiendes? Saber si lo firmó el técnico, si está
pendiente, todo eso». Y: «a veces filtrar por agenda».

Se prueba la lógica pura de app.py, extraída por AST (sin BD, sin Flask):
  · _ot2_recorrido: la OT interna no lleva el hito «Cliente»; los hitos anteriores al último con fecha se infieren;
    el «actual» es el que sigue; la cancelada no tiene actual; nunca aparece un nombre (REGLA #26).
  · _ot2_tabla_semaforo: el orden de prioridad (gris, rojo, vivo, ámbar, azul, verde) y que nunca mire plata.
  · _ot2_agenda_where: el SQL parametrizado de cada clave (y vacío para una clave desconocida).
  · La plantilla sigue sin `[:14]`, `data-fm-edit` ni `alert(` nativo en lo nuevo.
Correr con:  python -m pytest tests/test_ot2_panel_recorrido_agenda.py -q -p no:warnings
"""
import datetime as dt
import os
import re
import unittest

from tests.test_incidencias_bajas import _cargar
from tests.test_ot_saldo_servicio import _fuente, _leer

H = _leer("templates/ot2/panel.html")


def _amb():
    """Las funciones nuevas con sus constantes; _ot_es_interna y chile_fmt_filter se reemplazan por dobles simples
    (cliente_id vacío = interna; hora Chile = UTC-3), que es lo único que el recorrido les pide."""
    def _chile(v, fmt):
        return (v - dt.timedelta(hours=3)).strftime(fmt)
    amb = _cargar(
        ["_ot2_recorrido", "_ot2_tabla_semaforo", "_ot2_agenda_where"],
        ["_OT2_HITOS", "_OT2_REC_CORTO", "_OT2_ESTADOS_TE_TOCA", "_OT2_AGENDA_OPTS", "_OT2_AGENDA_CLAVES",
         "_OT2_ESTADO_META", "_OT2_FASES"],
        extra={"_ot_es_interna": lambda v: v.get("cliente_id") is None, "chile_fmt_filter": _chile,
               "print": lambda *a, **k: None},
    )
    amb["_OT2_FASE_A_ESTADOS"] = {c: [k for k, v in amb["_OT2_ESTADO_META"].items() if v[3] == c]
                                  for c in amb["_OT2_FASES"]}
    return amb


AMB = None


def _rec(f):
    return AMB["_ot2_recorrido"](f)


def _sem(f):
    return AMB["_ot2_tabla_semaforo"](f)


def _w(clave, hoy):
    return AMB["_ot2_agenda_where"](clave, hoy)


def _f(**kw):
    """Una fila ya enriquecida (lo que _ot2_enriquecer_fila deja antes de llamar al recorrido)."""
    base = {
        "id": 1, "estado": "programada", "estado_fase": "pend", "estado_label": "Programada", "estado_en_vivo": False,
        "cliente_id": 7, "created_at": dt.datetime(2026, 10, 1, 15, 0), "hora_real_inicio": None,
        "firma_tecnico_at": None, "firma_cliente_at": None, "firma_supervisor_at": None, "cerrada_at": None,
        "anexo_numero": None, "anexo_estado": None, "tecnico_tipo": "interno", "tecnico_nombre": "Jaizer Araujo",
        "check_done": False, "n_tareas": 5, "agenda_rel": None, "agenda_rel_class": None,
    }
    base.update(kw)
    return base


class TestRecorrido(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        global AMB
        AMB = AMB or _amb()

    def _claves(self, r):
        return [h["clave"] for h in r["hitos"]]

    def test_ot_de_cliente_lleva_los_seis_hitos_y_la_interna_no_tiene_cliente(self):
        r = _rec(_f())
        self.assertEqual(self._claves(r), ["creada", "ejecutando", "f_tecnico", "f_cliente", "aprobada", "cerrada"])
        ri = _rec(_f(cliente_id=None))
        self.assertEqual(self._claves(ri), ["creada", "ejecutando", "f_tecnico", "aprobada", "cerrada"])
        self.assertEqual([h["corto"] for h in ri["hitos"]], ["Creada", "Ejecución", "Técnico", "Aprobada", "Cerrada"])

    def test_programada_solo_creada_y_actual_es_ejecucion(self):
        r = _rec(_f())
        hechos = [h["clave"] for h in r["hitos"] if h["hecho"]]
        self.assertEqual(hechos, ["creada"])
        self.assertEqual([h["clave"] for h in r["hitos"] if h["actual"]], ["ejecutando"])
        self.assertEqual(r["hitos"][0]["cuando"], "01/10")
        self.assertEqual(r["texto"], "Por ejecutar")
        self.assertEqual(r["avisos"], [])

    def test_firma_del_tecnico_infiere_la_ejecucion_sin_inventar_fecha(self):
        # Firmó el técnico el 18-oct 02:30 UTC = 17-oct 23:30 en Chile (REGLA #6: la fecha no salta de día).
        r = _rec(_f(estado="firmada_tecnico", estado_fase="comp", estado_label="Firmada técnico",
                        firma_tecnico_at=dt.datetime(2026, 10, 18, 2, 30)))
        por = {h["clave"]: h for h in r["hitos"]}
        self.assertTrue(por["ejecutando"]["hecho"] and por["ejecutando"]["inferido"])
        self.assertEqual(por["ejecutando"]["cuando"], "")          # sin fecha real, sin fecha inventada
        self.assertTrue(por["f_tecnico"]["hecho"] and not por["f_tecnico"]["inferido"])
        self.assertEqual(por["f_tecnico"]["cuando"], "17/10")
        self.assertTrue(por["f_cliente"]["actual"])
        self.assertEqual(r["texto"], "Firmó el técnico 17/10 · falta firma del cliente")

    def test_interna_firmada_por_el_tecnico_pasa_a_por_aprobar(self):
        r = _rec(_f(cliente_id=None, estado="firmada_tecnico", estado_fase="comp",
                        firma_tecnico_at=dt.datetime(2026, 10, 10, 12)))
        self.assertEqual(r["texto"], "Firmó el técnico 10/10 · por aprobar")
        self.assertEqual([h["clave"] for h in r["hitos"] if h["actual"]], ["aprobada"])

    def test_cliente_aprobada_y_cerrada(self):
        r = _rec(_f(estado="pendiente_aprobacion", estado_fase="comp", estado_label="Por aprobar",
                        firma_tecnico_at=dt.datetime(2026, 10, 10, 12), firma_cliente_at=dt.datetime(2026, 10, 10, 13)))
        self.assertEqual(r["texto"], "Firmó el cliente 10/10 · por aprobar")
        r = _rec(_f(estado="completada", estado_fase="comp", estado_label="Completada",
                        firma_tecnico_at=dt.datetime(2026, 10, 10, 12), firma_cliente_at=dt.datetime(2026, 10, 10, 13),
                        firma_supervisor_at=dt.datetime(2026, 10, 11, 12)))
        self.assertEqual(r["texto"], "Aprobada 11/10 · falta cerrar")
        self.assertTrue([h for h in r["hitos"] if h["clave"] == "cerrada"][0]["actual"])
        r = _rec(_f(estado="cerrada", estado_fase="cerr", estado_label="Cerrada",
                        cerrada_at=dt.datetime(2026, 10, 16, 20)))
        self.assertEqual(r["texto"], "Cerrada el 16/10")
        self.assertTrue(all(h["hecho"] for h in r["hitos"]))
        self.assertFalse(any(h["actual"] for h in r["hitos"]))

    def test_cerrada_sin_fecha_igual_se_marca_cerrada(self):
        r = _rec(_f(estado="cerrada", estado_fase="cerr", estado_label="Cerrada"))
        self.assertEqual(r["texto"], "Cerrada")
        self.assertTrue(all(h["hecho"] for h in r["hitos"]))

    def test_en_sitio_y_checklist_listo(self):
        r = _rec(_f(estado="en_curso", estado_fase="ejec", estado_label="En ejecución", estado_en_vivo=True,
                        hora_real_inicio=dt.datetime(2026, 10, 18, 13, 5)))
        self.assertEqual(r["texto"], "En sitio desde 10:05")        # hora Chile, no UTC
        r = _rec(_f(estado="completada", estado_fase="comp", estado_label="Completada", check_done=True))
        self.assertEqual(r["texto"], "Checklist listo · falta firma del técnico")

    def test_cancelada_sin_actual(self):
        r = _rec(_f(estado="cancelada", estado_fase=None, estado_label="Cancelada"))
        self.assertTrue(r["cancelada"])
        self.assertFalse(any(h["actual"] for h in r["hitos"]))
        self.assertEqual(r["texto"], "Cancelada")

    def test_avisos_anexo_y_falta_info(self):
        # 2026-10-19: el anexo NO se repite en los avisos: el chip de «Cliente y trabajo» (que ya existía) lo dice
        # igual, y la fila lo mostraba dos veces con textos distintos. El semáforo de la fila sí lo sigue mirando.
        r = _rec(_f(tecnico_tipo="externo"))
        self.assertEqual(r["avisos"], [])
        r = _rec(_f(tecnico_tipo="externo", anexo_numero=218, anexo_estado="enviado"))
        self.assertEqual(r["avisos"], [])
        self.assertNotIn("anexo", repr(r).lower())
        self.assertEqual(_sem(_f(tecnico_tipo="externo", anexo_numero=218, anexo_estado="enviado")), "ambar")
        r = _rec(_f(tecnico_tipo="externo", anexo_numero=220, anexo_estado="firmado"))
        self.assertEqual(r["avisos"], [])
        r = _rec(_f(estado="pendiente_repuesto", estado_fase="comp", estado_label="Falta repuesto",
                        firma_tecnico_at=dt.datetime(2026, 10, 10, 12)))
        self.assertIn("Falta repuesto", r["avisos"])
        r = _rec(_f(estado="pendiente_info", estado_fase="comp", estado_label="Falta información"))
        self.assertEqual(r["texto"], "Falta información")          # ya lo dice el texto: no se repite en avisos
        self.assertEqual(r["avisos"], [])

    def test_sin_nombres_en_el_recorrido(self):   # REGLA #26
        r = _rec(_f(estado="cerrada", estado_fase="cerr", estado_label="Cerrada",
                        firma_tecnico_at=dt.datetime(2026, 10, 10, 12), firma_cliente_at=dt.datetime(2026, 10, 10, 13),
                        firma_supervisor_at=dt.datetime(2026, 10, 11, 12), cerrada_at=dt.datetime(2026, 10, 12, 12),
                        tecnico_nombre="Jaizer Araujo"))
        todo = repr(r)
        self.assertNotIn("Jaizer", todo)
        self.assertNotIn("Araujo", todo)
        fuente = _fuente("_ot2_recorrido")
        for k in ("tecnico_nombre", "firmado_por", "firma_cliente_nombre", "aprobado_por", "razon_social"):
            self.assertNotIn(k, fuente)


class TestSemaforo(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        global AMB
        AMB = AMB or _amb()

    def test_orden_de_prioridad(self):
        self.assertEqual(_sem(_f(estado="anulada", estado_fase=None, estado_label="Anulada")), "gris")
        self.assertEqual(_sem(_f(agenda_rel_class="mal", agenda_rel="Atrasada 2 días", estado_en_vivo=True)), "rojo")
        self.assertEqual(_sem(_f(estado="en_curso", estado_fase="ejec", estado_en_vivo=True)), "vivo")
        self.assertEqual(_sem(_f(agenda_rel_class="hoy", agenda_rel="Hoy")), "ambar")
        self.assertEqual(_sem(_f(estado="firmada_tecnico", estado_fase="comp")), "ambar")
        self.assertEqual(_sem(_f(estado="pendiente_aprobacion", estado_fase="comp")), "ambar")
        self.assertEqual(_sem(_f(tecnico_tipo="externo")), "ambar")                       # sin anexo
        self.assertEqual(_sem(_f(anexo_numero=5, anexo_estado="borrador")), "ambar")      # anexo sin firmar
        self.assertEqual(_sem(_f(n_tareas=0)), "ambar")                                   # sin checklist
        self.assertEqual(_sem(_f()), "azul")
        self.assertEqual(_sem(_f(estado="completada", estado_fase="comp")), "verde")
        self.assertEqual(_sem(_f(estado="cerrada", estado_fase="cerr")), "verde")

    def test_texto_para_lector_de_pantalla(self):
        f = _f(agenda_rel_class="mal", agenda_rel="Atrasada 3 días")
        _sem(f)
        self.assertEqual(f["tabla_semaforo_txt"], "Atrasada 3 días")
        f = _f(estado="firmada_tecnico", estado_fase="comp", estado_label="Firmada técnico")
        _sem(f)
        self.assertEqual(f["tabla_semaforo_txt"], "Te toca: Firmada técnico")

    def test_el_semaforo_nunca_mira_plata(self):   # REGLA #26: técnico y gestión ven la misma urgencia
        fuente = _fuente("_ot2_tabla_semaforo")
        for k in ("fac_alerta", "fin_clasif", "fac_numero", "fac_estado", "costo", "margen"):
            self.assertNotIn(k, fuente.split('"""')[2], k)   # fuera de la docstring


class TestAgendaWhere(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        global AMB
        AMB = AMB or _amb()
        cls.claves = AMB["_OT2_AGENDA_CLAVES"]

    def test_lista_blanca(self):
        self.assertEqual(self.claves, ("atrasadas", "hoy", "manana", "semana", "proximas", "sin_fecha"))
        self.assertEqual(_w("zzz", dt.date(2026, 10, 18)), ("", []))
        self.assertEqual(_w("hoy", None), ("", []))

    def test_sql_de_cada_clave(self):
        hoy = dt.date(2026, 10, 14)   # miércoles
        sql, p = _w("atrasadas", hoy)
        self.assertIn("v.estado IN (", sql)
        self.assertIn("v.fecha_programada < %s", sql)
        self.assertIn("(v.fecha_fin IS NULL OR v.fecha_fin < %s)", sql)
        self.assertEqual(p[-2:], [hoy, hoy])
        self.assertIn("en_curso", p)
        self.assertNotIn("cerrada", p)
        self.assertNotIn("firmada_tecnico", p)
        sql, p = _w("hoy", hoy)
        self.assertEqual(sql, "(v.fecha_programada <= %s AND %s <= COALESCE(v.fecha_fin, v.fecha_programada))")
        self.assertEqual(p, [hoy, hoy])
        sql, p = _w("manana", hoy)
        self.assertEqual(p, [dt.date(2026, 10, 15), dt.date(2026, 10, 15)])
        sql, p = _w("semana", hoy)
        self.assertEqual(p, [dt.date(2026, 10, 18), dt.date(2026, 10, 12)])   # domingo, lunes
        self.assertIn("COALESCE(v.fecha_fin, v.fecha_programada) >= %s", sql)
        sql, p = _w("proximas", hoy)
        self.assertEqual((sql, p), ("v.fecha_programada > %s", [hoy]))
        sql, p = _w("sin_fecha", hoy)
        self.assertEqual((sql, p), ("v.fecha_programada IS NULL", []))
        for k in self.claves:
            self.assertNotIn("CURDATE", _w(k, hoy)[0])   # REGLA #6: la fecha de hoy viene en hora Chile
        self.assertEqual(sql.count("%s"), len(p))

    def test_el_panel_lo_aplica_en_el_servidor_y_lo_pasa_a_la_plantilla(self):
        f = _fuente("ot2_panel")
        self.assertIn('f_agenda = (request.args.get("agenda") or "").strip().lower()', f)
        self.assertIn('if f_agenda not in _OT2_AGENDA_CLAVES or vista != "tabla":', f)
        self.assertIn("_ot2_agenda_where(f_agenda, _now_chile().date())", f)
        self.assertIn("extra_where_q.append(_ag_sql)", f)         # entra al COUNT, al LIMIT y a los KPI
        self.assertIn("f_agenda=f_agenda, agenda_opts=_OT2_AGENDA_OPTS", f)
        enr = _fuente("_ot2_enriquecer_fila")
        self.assertIn("_ot2_recorrido(f)", enr)
        self.assertIn("_ot2_tabla_semaforo(f)", enr)
        self.assertIn('chile_fmt_filter(dt, "%H:%M")', enr)        # REGLA #6: «En sitio desde» en hora Chile
        self.assertIn('chile_fmt_filter(dt, "%d/%m")', enr)

    def test_el_excel_respeta_el_filtro_de_agenda(self):
        """2026-10-19: con «Hoy» activo en pantalla, el Reporte Excel bajaba todo. _ot2_filtros_export (cuyo propio
        docstring exige sumar acá cada filtro nuevo del panel) ahora aplica la misma lista blanca y el mismo SQL."""
        import datetime as _dt
        hoy = dt.date(2026, 10, 14)
        amb = _cargar(
            ["_ot2_filtros_export", "_ot2_agenda_where"],
            ["_OT2_AGENDA_OPTS", "_OT2_AGENDA_CLAVES", "_OT2_FASES", "_OT2_ESTADO_META"],
            extra={"datetime": _dt.datetime, "_now_chile": lambda: _dt.datetime(2026, 10, 14, 10, 0),
                   "_mant_calendario_role_where": lambda: (" AND 1=1", []), "_ot_tipos_todos": lambda: {},
                   "_TIPO_OT_LABEL": {}, "_OT2_FASE_LABELS": {}, "_OT2_CENTROS_COSTO": (), "print": lambda *a, **k: None,
                   # 2026-10-21 (REGLA #36): el Excel también acota por «Responsable» (_ot_resp_filtro_sql, que tiene sus
                   # propias pruebas en test_ot_responsable.py); acá se prueba solo la agenda, así que sin filtro de responsable.
                   "_ot_resp_filtro_sql": lambda valor, uid: ("", "", []), "g": object(),
                   # 2026-10-21 (REGLA #38): lo mismo con «Con hallazgos sin gestionar» (_ot_hall_filtro_sql, con sus pruebas en
                   # test_ot_hallazgos.py).
                   "_ot_hall_filtro_sql": lambda valor: ("", "", [])},
        )
        amb["_OT2_FASE_A_ESTADOS"] = {c: [k for k, v in amb["_OT2_ESTADO_META"].items() if v[3] == c]
                                      for c in amb["_OT2_FASES"]}
        sql, params, desc, _ = amb["_ot2_filtros_export"]({"agenda": "hoy"})
        self.assertIn("(v.fecha_programada <= %s AND %s <= COALESCE(v.fecha_fin, v.fecha_programada))", sql)
        self.assertEqual(params[-2:], [hoy, hoy])
        self.assertIn("agenda hoy", desc)
        self.assertNotIn("CURDATE", sql)
        sql, params, desc, _ = amb["_ot2_filtros_export"]({"agenda": "zzz"})   # fuera de la lista blanca: nada
        self.assertNotIn("fecha_programada", sql)
        self.assertEqual(desc, [])
        self.assertEqual(sql.count("%s"), len(params))


class TestPlantilla(unittest.TestCase):
    def test_lo_que_las_otras_pruebas_vigilan_sigue_ausente(self):
        self.assertNotIn("[:14]", H)
        self.assertNotIn("data-fm-edit", H)
        i = H.index("TABLA v2 (2026-10-18)")
        self.assertIsNone(re.search(r"(?<![\w.])(alert|confirm|prompt)\(", H[i:]))

    def test_recorrido_y_agenda_en_la_tabla(self):
        for k in ('<ol class="o2t-rec" aria-label="Recorrido de la OT">', '{{ _rec.texto }}', '{% for _a in _rec.avisos %}',
                  '<nav class="o2t-agenda" aria-label="Filtro rápido de agenda">', 'agenda=(None if f_agenda == _ak else _ak)',
                  '{% if f_agenda %}<input type="hidden" name="agenda" value="{{ f_agenda }}">{% endif %}'):
            self.assertIn(k, H, k)
        # El filtro de agenda viaja en los links del pie, el encabezado ordenable, los KPI y la pestaña Tabla.
        self.assertGreaterEqual(H.count("agenda=f_agenda or None"), 9)


if __name__ == "__main__":
    unittest.main()
