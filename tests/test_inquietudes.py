# -*- coding: utf-8 -*-
"""Inquietudes de clientes (25-oct-2026): lógica pura (SLA en horas hábiles, semáforo, ruta paso a paso, permisos) y la forma del código
en app.py (tablas baratas, rutas solo gestión, nada de correo al cliente, nada de tk_api_create)."""
import ast
import os
import re
import sys
import unittest
from datetime import datetime

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RAIZ)
import inquietudes as Q  # noqa: E402

NINGUN_FERIADO = set()


def dt(d, h=0, m=0):
    y, mo, da = [int(x) for x in d.split("-")]
    return datetime(y, mo, da, h, m)


def leer(*p):
    with open(os.path.join(RAIZ, *p), encoding="utf-8") as fh:
        return fh.read()


APP = leer("app.py")


def fila(**k):
    base = {"estado": "nueva", "creada_en": dt("2026-10-26", 9), "primera_respuesta_en": None, "resuelta_en": None,
            "responsable_user_id": None, "tipo_solicitud": "sin_clasificar", "borrada": 0, "ticket_id": None, "ot_id": None}
    base.update(k)
    return base


class TestConfig(unittest.TestCase):
    def test_defecto_4h_y_2_dias(self):
        c = Q.config({})
        self.assertEqual(c["primera_h"], 4.0)
        self.assertEqual(c["resolucion_h"], 18.0)

    def test_configurable(self):
        c = Q.config({"INQUIETUDES_PRIMERA_H": "2", "INQUIETUDES_RESOLUCION_DIAS": "3", "INQUIETUDES_AVISO_PCT": "50"})
        self.assertEqual((c["primera_h"], c["resolucion_h"], c["aviso_fraccion"]), (2.0, 27.0, 0.5))

    def test_basura_cae_al_defecto(self):
        self.assertEqual(Q.config({"INQUIETUDES_PRIMERA_H": "abc"})["primera_h"], 4.0)


class TestReloj(unittest.TestCase):
    def r(self, f, ahora):
        return Q.reloj(f, ahora, NINGUN_FERIADO)

    def test_recien_creada_verde(self):
        r = self.r(fila(), dt("2026-10-26", 9, 30))        # lunes
        self.assertEqual(r["semaforo"], "verde")
        self.assertEqual(r["primera"]["estado"], "en_plazo")

    def test_ambar_cerca_del_plazo(self):
        r = self.r(fila(), dt("2026-10-26", 12, 15))       # 3 h 15 min de 4 h (>=75 %)
        self.assertEqual(r["primera"]["estado"], "por_vencer")
        self.assertEqual(r["semaforo"], "ambar")

    def test_rojo_vencida(self):
        r = self.r(fila(), dt("2026-10-26", 15, 0))        # 5 h hábiles (con jornada 08-17, sin descontar colación)
        self.assertEqual(r["semaforo"], "rojo")
        self.assertTrue(r["vencida"])
        self.assertIn("Sin responder", r["texto"])

    def test_la_noche_y_el_fin_de_semana_no_cuentan(self):
        # creada viernes 16:00; el lunes 08:30 solo han pasado 1 h (viernes) + 0,5 h (lunes) = 1,5 h hábiles
        f = fila(creada_en=dt("2026-10-23", 16))
        r = self.r(f, dt("2026-10-26", 8, 30))
        self.assertAlmostEqual(r["primera"]["transcurrido_h"], 1.5, places=2)
        self.assertEqual(r["semaforo"], "verde")

    def test_feriado_no_cuenta(self):
        f = fila(creada_en=dt("2026-10-23", 16))
        r = Q.reloj(f, dt("2026-10-27", 8, 30), {"2026-10-26"})   # lunes feriado: martes 08:30 = 1 h + 0,5 h
        self.assertAlmostEqual(r["primera"]["transcurrido_h"], 1.5, places=2)

    def test_tomada_cumple_la_primera_respuesta(self):
        f = fila(estado="en_atencion", primera_respuesta_en=dt("2026-10-26", 10), responsable_user_id=3)
        r = self.r(f, dt("2026-10-26", 16))
        self.assertTrue(r["primera"]["terminado"])
        self.assertEqual(r["primera"]["estado"], "cumplida")
        self.assertEqual(r["semaforo"], "verde")

    def test_resolucion_vence_a_los_2_dias_habiles(self):
        f = fila(estado="en_atencion", primera_respuesta_en=dt("2026-10-26", 10), responsable_user_id=3)
        self.assertEqual(self.r(f, dt("2026-10-27", 16))["semaforo"], "ambar")      # 16 h de 18 h
        r = self.r(f, dt("2026-10-28", 12))
        self.assertEqual(r["resolucion"]["estado"], "vencida")
        self.assertEqual(r["semaforo"], "rojo")

    def test_reabrir_reinicia_la_resolucion(self):
        f = fila(estado="en_atencion", primera_respuesta_en=dt("2026-10-26", 10), responsable_user_id=3, reabierta_en=dt("2026-11-02", 9))
        r = self.r(f, dt("2026-11-02", 10))
        self.assertAlmostEqual(r["resolucion"]["transcurrido_h"], 1.0, places=2)
        self.assertEqual(r["semaforo"], "verde")

    def test_cerrada_no_corre(self):
        f = fila(estado="cerrada", primera_respuesta_en=dt("2026-10-26", 10), resuelta_en=dt("2026-10-26", 14))
        r = self.r(f, dt("2026-11-30", 9))
        self.assertEqual(r["semaforo"], "verde")
        self.assertTrue(r["texto"].startswith("Cerrada"))

    def test_anulada_gris(self):
        self.assertEqual(self.r(fila(estado="anulada"), dt("2026-12-01", 9))["semaforo"], "gris")

    def test_aviso_una_sola_vez_por_nivel(self):
        rel = self.r(fila(), dt("2026-10-26", 12, 30))
        self.assertEqual(Q.nivel_aviso(rel, 0), 1)
        self.assertEqual(Q.nivel_aviso(rel, 1), 0)
        rojo = self.r(fila(), dt("2026-10-26", 16, 30))
        self.assertEqual(Q.nivel_aviso(rojo, 1), 2)
        self.assertEqual(Q.nivel_aviso(rojo, 2), 0)


class TestValidaciones(unittest.TestCase):
    def test_tecnico_exige_inquietud_y_diagnostico(self):
        self.assertFalse(Q.validar_crear("corta", "diagnóstico suficiente", 5)[0])
        self.assertFalse(Q.validar_crear("el cliente dice que hace ruido", "corto", 5)[0])
        self.assertTrue(Q.validar_crear("el cliente dice que hace ruido", "rodamiento gastado", 5)[0])

    def test_gestion_puede_dejar_el_diagnostico_para_despues(self):
        self.assertTrue(Q.validar_crear("el cliente dice que hace ruido", "", 5, origen="ficha_cliente")[0])
        self.assertFalse(Q.validar_crear("el cliente dice que hace ruido", "", 5, origen="ot")[0])

    def test_equipo_de_ficha_o_texto_libre(self):
        self.assertFalse(Q.validar_crear("el cliente dice que hace ruido", "rodamiento gastado")[0])
        self.assertTrue(Q.validar_crear("el cliente dice que hace ruido", "rodamiento gastado", None, "Trotadora vieja")[0])

    def test_motivo_y_cierre_minimos(self):
        self.assertFalse(Q.validar_motivo("no")[0])
        self.assertFalse(Q.validar_cierre("ok")[0])
        self.assertTrue(Q.validar_cierre("Se cambió el rodamiento y quedó funcionando")[0])

    def test_fotos_maximo_3(self):
        j = Q.fotos_a_json(["https://a/1.jpg", "https://a/2.jpg", "https://a/3.jpg", "https://a/4.jpg", "javascript:x", ""])
        self.assertEqual(len(Q.fotos_de_json(j)), 3)
        self.assertNotIn("javascript", j)

    def test_numero_y_nombre(self):
        self.assertEqual(Q.numero(42), "INQ-00042")
        self.assertEqual(Q.nombre_con_ficha("Trotadora X", "ABC", "123"), "Trotadora X (SKU ABC) · S/N 123")


class TestRuta(unittest.TestCase):
    def test_sin_clasificar_pide_decidir(self):
        r = Q.ruta(fila(), {})
        self.assertEqual([p["clave"] for p in r["pasos"]], ["registrada", "responsable", "clasificar", "cierre"])
        self.assertEqual(r["actual"]["clave"], "responsable")

    def test_responsable_avanza_a_clasificar(self):
        r = Q.ruta(fila(estado="en_atencion", responsable_user_id=3), {})
        self.assertEqual(r["actual"]["clave"], "clasificar")
        self.assertEqual(r["actual"]["accion"], "clasificar")

    def test_ruta_repuesto(self):
        f = fila(estado="en_atencion", responsable_user_id=3, tipo_solicitud="repuesto")
        r = Q.ruta(f, {})
        self.assertEqual(r["actual"]["clave"], "solicitar_repuesto")
        r = Q.ruta(f, {"solicitud_repuesto": True})
        self.assertEqual(r["actual"]["clave"], "repuesto_recibido")
        r = Q.ruta(f, {"solicitud_repuesto": True, "repuesto_recibido": True, "repuesto_instalado": True})
        self.assertEqual(r["actual"]["clave"], "cierre")

    def test_ruta_ticket_luego_ot(self):
        f = fila(estado="en_atencion", responsable_user_id=3, tipo_solicitud="ticket_para_ot")
        self.assertEqual(Q.ruta(f, {})["actual"]["clave"], "levantar_ticket")
        self.assertEqual(Q.ruta(f, {"ticket": True})["actual"]["clave"], "ot_desde_ticket")
        self.assertEqual(Q.ruta(f, {"ticket": True, "ot": True})["actual"]["clave"], "ot_ejecutada")
        self.assertEqual(Q.ruta(f, {"ticket": True, "ot": True, "ot_terminada": True})["actual"]["clave"], "cierre")

    def test_ruta_ot_directa_y_reparacion(self):
        f = fila(estado="en_atencion", responsable_user_id=3, tipo_solicitud="ot_directa")
        self.assertEqual(Q.ruta(f, {})["actual"]["clave"], "levantar_ot")
        g = fila(estado="en_atencion", responsable_user_id=3, tipo_solicitud="reparacion")
        self.assertEqual(Q.ruta(g, {})["actual"]["clave"], "diagnostico")
        self.assertEqual(Q.ruta(g, {}, {"diagnostico": {"por": "Víctor", "en": "26/10/2026 10:00"}})["actual"]["clave"], "levantar_ot")

    def test_paso_manual_de_abastecer(self):
        f = fila(estado="en_atencion", responsable_user_id=3, tipo_solicitud="abastecer")
        r = Q.ruta(f, {}, {"revisar_stock": {"por": "Aarón", "en": "x"}})
        self.assertEqual(r["actual"]["clave"], "abastecer")
        self.assertTrue(Q.marcar_a_mano(r, "abastecer"))
        self.assertFalse(Q.marcar_a_mano(r, "cierre"))
        self.assertEqual(r["pasos"][3]["quien"], "Aarón")

    def test_cerrada_todo_hecho_menos_lo_que_no_se_hizo(self):
        f = fila(estado="cerrada", responsable_user_id=3, tipo_solicitud="repuesto")
        r = Q.ruta(f, {"solicitud_repuesto": True, "repuesto_recibido": True, "repuesto_instalado": True})
        self.assertEqual(r["hechos"], r["total"])
        self.assertEqual(r["avance"], 100)
        self.assertEqual(r["pendiente"], "")

    def test_pendiente_de_gestion(self):
        self.assertFalse(Q.es_pendiente_de_gestion(fila()))
        self.assertTrue(Q.es_pendiente_de_gestion(fila(tipo_solicitud="repuesto")))
        self.assertFalse(Q.es_pendiente_de_gestion(fila(tipo_solicitud="repuesto", estado="cerrada")))


class TestAcciones(unittest.TestCase):
    def test_borrar_solo_superadmin(self):
        self.assertFalse(Q.puede_accion("borrar", fila(), False)[0])
        self.assertTrue(Q.puede_accion("borrar", fila(), True)[0])

    def test_borrada_no_se_toca(self):
        self.assertFalse(Q.puede_accion("editar", fila(borrada=1), True)[0])

    def test_terminada_solo_se_reabre(self):
        c = fila(estado="cerrada")
        self.assertFalse(Q.puede_accion("editar", c)[0])
        self.assertFalse(Q.puede_accion("cerrar", c)[0])
        self.assertTrue(Q.puede_accion("reabrir", c)[0])
        self.assertFalse(Q.puede_accion("reabrir", fila())[0])

    def test_adjudicar_no_pisa_responsable(self):
        self.assertFalse(Q.puede_accion("adjudicar", fila(responsable_user_id=3))[0])
        self.assertTrue(Q.puede_accion("adjudicar", fila())[0])

    def test_un_ticket_por_inquietud(self):
        self.assertFalse(Q.puede_accion("ticket", fila(ticket_id=9))[0])

    def test_transiciones(self):
        self.assertEqual(Q.estado_tras_asignar("nueva"), "en_atencion")
        self.assertEqual(Q.estado_tras_asignar("escalada"), "escalada")
        self.assertEqual(Q.estado_tras_vinculo("en_atencion"), "escalada")
        self.assertEqual(Q.estado_tras_vinculo("cerrada"), "cerrada")


class TestMonitor(unittest.TestCase):
    def test_vistas(self):
        filas = [fila(estado="nueva"), fila(estado="en_atencion", tipo_solicitud="repuesto", primera_respuesta_en=dt("2026-10-26", 10)),
                 fila(estado="cerrada")]
        ahora = dt("2026-10-26", 9, 5)
        self.assertEqual(len(Q.filtrar_vista(filas, "abiertas", ahora, NINGUN_FERIADO)), 2)
        self.assertEqual(len(Q.filtrar_vista(filas, "sin_tomar", ahora, NINGUN_FERIADO)), 1)
        self.assertEqual(len(Q.filtrar_vista(filas, "pendientes", ahora, NINGUN_FERIADO)), 1)
        self.assertEqual(len(Q.filtrar_vista(filas, "cerradas", ahora, NINGUN_FERIADO)), 1)
        self.assertEqual(len(Q.filtrar_vista(filas, "vencidas", dt("2026-10-27", 16), NINGUN_FERIADO)), 1)
        self.assertEqual(Q.resumen_estados(filas)["nueva"], 1)

    def test_por_pagina(self):
        self.assertEqual(Q.pagina_valida("50"), 50)
        self.assertEqual(Q.pagina_valida("7"), 20)

    def test_campana_sin_plata_ni_tono_duro(self):
        t, c = Q.campana_nueva(7, "Gimnasio X", "Trotadora", "Juan")
        self.assertIn("INQ-00007", t)
        self.assertNotIn("$", t + c)
        for n in (1, 2):
            t, c = Q.campana_sla(7, "Gimnasio X", n)
            self.assertNotIn("culpa", (t + c).lower())


class TestCodigoEnApp(unittest.TestCase):
    """La forma del código: lo que no se puede probar sin base de datos se vigila leyendo el fuente."""

    @classmethod
    def setUpClass(cls):
        i = APP.index("INQUIETUDES DE CLIENTES (módulo propio")
        j = APP.index("HALLAZGOS DE LA OT (F1")
        cls.bloque = APP[i:j]
        cls.app = APP

    def test_una_sentencia_por_tabla_y_barata(self):
        self.assertIn("CREATE TABLE IF NOT EXISTS cli_inquietudes (", self.bloque)
        self.assertIn("CREATE TABLE IF NOT EXISTS cli_inquietud_actividad (", self.bloque)
        self.assertNotIn("ALTER TABLE", self.bloque)
        self.assertEqual(len(re.findall(r"\"CREATE TABLE", self.bloque)), 2)

    def test_todas_las_rutas_de_gestion_son_no_tecnico(self):
        rutas = re.findall(r'@app\.route\("([^"]+)"[^\n]*\)\n((?:@[^\n]+\n)+)def (\w+)', self.bloque)
        self.assertGreater(len(rutas), 12)
        publicas_para_tecnico = {"ot_api_equipos_cliente", "ot_api_inquietud_cliente", "ot_api_inquietudes_de_ot"}
        for url, decos, nombre in rutas:
            self.assertIn("_mant_required", decos, nombre)
            if nombre in publicas_para_tecnico:
                self.assertIn("_ot_can_configurar", decos, nombre)
            else:
                self.assertIn("_no_tecnico", decos, nombre)

    def test_borrar_solo_superadmin_en_el_servidor(self):
        i = self.bloque.index("def inq_api_borrar")
        self.assertIn("_ot_aut_es_superadmin()", self.bloque[i:i + 1500])

    def test_el_ticket_no_escribe_al_cliente(self):
        self.assertNotIn("tk_api_create(", self.bloque)
        self.assertNotIn("_send_ilus_email", self.bloque)
        self.assertNotIn("send_whatsapp", self.bloque)
        i = self.bloque.index("def _inq_ticket_crear")
        cuerpo = self.bloque[i:i + 4500]
        self.assertIn("INSERT INTO tk_tickets", cuerpo)
        self.assertNotIn("visita_id", cuerpo.split("INSERT INTO tk_tickets")[1].split("VALUES")[0])

    def test_campana_con_tipo_existente(self):
        self.assertIn("TIPO_CAMPANA = \"ot_hallazgo\"", self.bloque)

    def test_cada_accion_escribe_actividad(self):
        for nombre in ("adjudicar", "asignar", "clasificar", "editar", "cerrar", "anular", "reabrir", "borrar", "ticket", "vincular", "paso", "nota"):
            i = self.bloque.index("def inq_api_" + nombre)
            j = self.bloque.find("\n@app.route", i)
            self.assertTrue("_inq_log(" in self.bloque[i:j] or "_inq_poner_responsable(" in self.bloque[i:j], nombre)

    def test_el_modulo_quedo_apagado_y_lo_que_comenta_el_cliente_es_un_hallazgo(self):
        # Daniel 27-oct: «las inquietudes elimínalas… déjala como hallazgo» (y «su interfaz me gustó»: el código se conserva, apagado).
        panel = leer("templates", "ot2", "panel.html")
        self.assertNotIn("url_for('ot_inquietudes_pagina')", panel, "sin botón «Inquietudes» en el panel")
        self.assertIn('_INQ_MODULO_ACTIVO = (os.environ.get("INQUIETUDES_MODULO")', self.app)
        for fn in ("def ot_inquietudes_pagina(", "def ot_inquietud_ficha("):
            i = self.app.index(fn)
            self.assertIn('if not _INQ_MODULO_ACTIVO:', self.app[i:i + 900], fn + " lleva a Hallazgos con el módulo apagado")
        i = self.app.index("def ot_api_inquietud_cliente(")
        cuerpo = self.app[i:self.app.index("\n@app.route", i)]
        self.assertIn("_hall_registrar(vid, h)", cuerpo, "«El cliente me comentó…» crea un hallazgo de la OT")
        self.assertIn('"origen": _oh.ORIGEN_CLIENTE', cuerpo)
        self.assertNotIn("_inq_crear_core(", cuerpo)
        self.assertNotIn("INSERT INTO cli_inquietudes", cuerpo)
        self.assertIn('globals().get("_INQ_MODULO_ACTIVO")', self.app, "sin barrido de la campana de inquietudes")


if __name__ == "__main__":
    unittest.main()
