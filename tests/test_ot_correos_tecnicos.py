"""📧 CORREOS A TÉCNICOS cuando se agenda, cambia o cancela una OT (Daniel, 19-oct-2026).

Daniel: «ya necesito que activemos por el front y por la parte de comunicaciones los mails hacia los técnicos. Cuando
agendemos una orden de trabajo, tenemos que avisarle a los técnicos… por un mail con plantillas, todo bien
estructuradito». Y: «el correo al CLIENTE sigue apagado; separa los interruptores para encender solo el de técnicos».

Qué se vigila (todo con correo y base de MENTIRA — REGLA #22: jamás se envía un correo real ni se toca la base):
  · La lógica pura (ot_correos_tecnicos.py): a quién se avisa y por qué, sin «reagendada» repetida cuando además cambia
    el técnico; las fechas del recordatorio (viernes→lunes, víspera de feriado, nunca sábado/domingo/feriado, desde las
    17:00); las horas de pymysql (timedelta); el HTML y que NINGÚN texto a un técnico lleve plata ni proveedores.
  · app.py con un mundo en memoria: el punto único `_ot_tec_correo`, el recordatorio `_ot_tec_barrido_agenda`, la
    idempotencia, la llave PROPIA (con 'ot_tecnicos' abierta y 'mantenciones' cerrada el técnico recibe y el cliente no),
    la siembra de plantillas (nunca pisa lo editado) y el cableado en cada camino que asigna/reagenda/cancela.
Sin base, sin Flask, sin correo.
Correr con:  py -m unittest tests.test_ot_correos_tecnicos
"""
import ast
import datetime as dt
import os
import re
import types
import unittest

import ot_correos_tecnicos as O
from cl_feriados import es_dia_habil

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_CODIGO = None
_BLOQUES = {}


def _codigo():
    """app.py completo (con \n). Parsear sus 8,5 MB de una vez tarda minutos: acá se trocea por bloques de nivel superior
    y solo se parsea lo que cada prueba necesita (un segundo en total)."""
    global _CODIGO
    if _CODIGO is None:
        with open(os.path.join(RAIZ, "app.py"), encoding="utf-8", newline="") as f:
            _CODIGO = f.read().replace("\r\n", "\n")
    return _CODIGO


def _bloque(nombre, tipo="def"):
    """Texto del bloque de nivel superior `def nombre(` (o `nombre =`), sin decoradores, hasta el siguiente bloque."""
    clave = (tipo, nombre)
    if clave not in _BLOQUES:
        lineas = _codigo().split("\n")
        prefijos = (f"def {nombre}(",) if tipo == "def" else (f"{nombre} =", f"{nombre}:")
        ini = next((i for i, l in enumerate(lineas) if l.startswith(prefijos)), None)
        if ini is None:
            raise AssertionError("no está " + nombre)
        fin = len(lineas)
        for j in range(ini + 1, len(lineas)):
            l = lineas[j]
            if l and not l[0].isspace() and l[0] not in "#)]}":
                fin = j
                break
        _BLOQUES[clave] = "\n".join(lineas[ini:fin])
    return _BLOQUES[clave]


def _fuente(nombre):
    return _bloque(nombre)


def _cargar(amb, funciones=(), asignaciones=()):
    """Ejecuta en `amb` las funciones/constantes REALES de app.py."""
    for nombre in funciones:
        exec(compile(ast.parse(_bloque(nombre)), "<app>", "exec"), amb)
    for nombre in asignaciones:
        exec(compile(ast.parse(_bloque(nombre, "asig")), "<app>", "exec"), amb)
    faltan = [x for x in tuple(funciones) + tuple(asignaciones) if x not in amb]
    assert not faltan, f"no están en app.py: {faltan}"


def F(*a):
    return dt.date(*a)


def foto(estado="programada", fecha=F(2026, 10, 22), fecha_fin=None, hi="09:00", hf="11:00", tec=None):
    return O.foto(estado, fecha, fecha_fin, hi, hf, tec if tec is not None else {"a@sphs.cl": "Ana"})


# ───────────────────────── formatos ─────────────────────────
class TestFormatos(unittest.TestCase):
    def test_horas_de_pymysql_son_timedelta_y_salen_con_dos_digitos(self):
        # str(timedelta(hours=9))[:5] daba «9:00:» (bug del correo viejo): acá siempre HH:MM
        self.assertEqual(O.hhmm(dt.timedelta(hours=9)), "09:00")
        self.assertEqual(O.hhmm(dt.timedelta(hours=14, minutes=5)), "14:05")
        self.assertEqual(O.hhmm("9:30:00"), "09:30")
        self.assertEqual(O.hhmm(dt.time(8, 0)), "08:00")
        self.assertEqual(O.hhmm(None), "")
        self.assertEqual(O.hhmm(""), "")

    def test_fechas_dia_mes_anio_y_nunca_iso(self):
        self.assertEqual(O.fecha_txt(F(2026, 10, 19)), "19/10/2026")
        self.assertEqual(O.fecha_txt("2026-10-19"), "19/10/2026")
        self.assertEqual(O.dia_txt(F(2026, 10, 19)), "lunes 19/10/2026")
        self.assertEqual(O.rango_txt(F(2026, 10, 19), F(2026, 10, 21)), "19/10/2026 al 21/10/2026")
        self.assertEqual(O.rango_txt(F(2026, 10, 19), F(2026, 10, 19)), "19/10/2026")
        self.assertEqual(O.rango_txt(None), "por confirmar")
        self.assertEqual(O.horario_txt("09:00", "11:00"), "09:00 – 11:00")
        self.assertEqual(O.horario_txt("09:00"), "09:00")
        self.assertEqual(O.horario_txt(None), "por confirmar")


# ───────────────────────── a quién se avisa ─────────────────────────
class TestDecidirAvisos(unittest.TestCase):
    def ev(self, antes, despues):
        return sorted((a["email"], a["evento"]) for a in O.decidir_avisos(antes, despues))

    def test_ot_nueva_asigna_a_todos_los_tecnicos(self):
        d = foto(tec={"a@sphs.cl": "Ana", "b@sphs.cl": "Beto"})
        self.assertEqual(self.ev(None, d), [("a@sphs.cl", "asignada"), ("b@sphs.cl", "asignada")])

    def test_reagendar_fecha_u_horario_avisa_al_que_se_queda(self):
        a = foto()
        self.assertEqual(self.ev(a, foto(fecha=F(2026, 10, 23))), [("a@sphs.cl", "reagendada")])
        self.assertEqual(self.ev(a, foto(hi="14:00", hf="16:00")), [("a@sphs.cl", "reagendada")])
        self.assertEqual(self.ev(a, foto(fecha_fin=F(2026, 10, 24))), [("a@sphs.cl", "reagendada")])

    def test_sin_cambio_real_no_se_avisa_nada(self):
        a = foto()
        self.assertEqual(self.ev(a, foto()), [])
        # fecha_fin igual a la fecha cuenta como «sin fecha_fin»
        self.assertEqual(self.ev(a, foto(fecha_fin=F(2026, 10, 22))), [])
        # solo cambia el estado entre dos activos: tampoco
        self.assertEqual(self.ev(a, foto(estado="en_curso")), [])

    def test_reasignar_y_cambiar_fecha_a_la_vez_no_manda_reagendada_repetida(self):
        a = foto(tec={"a@sphs.cl": "Ana"})
        d = foto(fecha=F(2026, 10, 25), tec={"b@sphs.cl": "Beto"})
        self.assertEqual(self.ev(a, d), [("a@sphs.cl", "desasignada"), ("b@sphs.cl", "asignada")])

    def test_se_suma_un_colaborador_y_cambia_la_fecha(self):
        a = foto(tec={"a@sphs.cl": "Ana"})
        d = foto(fecha=F(2026, 10, 25), tec={"a@sphs.cl": "Ana", "b@sphs.cl": "Beto"})
        self.assertEqual(self.ev(a, d), [("a@sphs.cl", "reagendada"), ("b@sphs.cl", "asignada")])

    def test_quitar_un_tecnico_le_avisa_que_ya_no_la_tiene(self):
        a = foto(tec={"a@sphs.cl": "Ana", "b@sphs.cl": "Beto"})
        d = foto(tec={"a@sphs.cl": "Ana"})
        self.assertEqual(self.ev(a, d), [("b@sphs.cl", "desasignada")])
        self.assertEqual(self.ev(a, foto(tec={})), [("a@sphs.cl", "desasignada"), ("b@sphs.cl", "desasignada")])

    def test_cancelar_o_anular_avisa_a_todos_una_sola_vez(self):
        a = foto(tec={"a@sphs.cl": "Ana", "b@sphs.cl": "Beto"})
        for est in ("cancelada", "anulada"):
            d = foto(estado=est, tec={"a@sphs.cl": "Ana", "b@sphs.cl": "Beto"})
            self.assertEqual(self.ev(a, d), [("a@sphs.cl", "cancelada"), ("b@sphs.cl", "cancelada")])
            self.assertEqual(self.ev(d, d), [])                         # ya estaba cancelada: nada
        # cancelar y de paso sacar a Beto: Beto igual se entera de que se canceló
        d = foto(estado="cancelada", tec={"a@sphs.cl": "Ana"})
        self.assertEqual(self.ev(a, d), [("a@sphs.cl", "cancelada"), ("b@sphs.cl", "cancelada")])

    def test_ot_cerrada_o_completada_no_avisa_nada(self):
        a = foto()
        for est in ("cerrada", "completada", "firmada_tecnico", "pendiente_aprobacion"):
            self.assertEqual(self.ev(a, foto(estado=est, fecha=F(2026, 10, 30))), [])
            self.assertEqual(self.ev(None, foto(estado=est)), [])

    def test_cancelar_una_ot_que_ya_se_hizo_no_avisa(self):
        # «no necesitas ir» sobre un trabajo ya hecho (cerrado, completado, firmado) sería ruido para el técnico
        for est in ("cerrada", "completada", "firmada_tecnico", "pendiente_aprobacion"):
            self.assertEqual(self.ev(foto(estado=est), foto(estado="cancelada")), [], est)
        # pero una OT todavía por hacer (programada, reagendada, en curso) sí
        for est in ("programada", "reagendada", "en_curso"):
            self.assertEqual(self.ev(foto(estado=est), foto(estado="anulada")), [("a@sphs.cl", "cancelada")], est)

    def test_reabrir_una_cancelada_la_asigna_de_nuevo(self):
        a = foto(estado="cancelada")
        self.assertEqual(self.ev(a, foto(estado="programada")), [("a@sphs.cl", "asignada")])

    def test_sin_despues_no_hay_nada(self):
        self.assertEqual(O.decidir_avisos(foto(), None), [])

    def test_correos_invalidos_no_entran_a_la_foto(self):
        f = O.foto("programada", F(2026, 10, 22), None, "09:00", None, {"sin-arroba": "X", "ok@sphs.cl": "Ok", "": "Y"})
        self.assertEqual(list(f["tecnicos"]), ["ok@sphs.cl"])

    def test_huella_distingue_cambios_y_repite_para_el_mismo(self):
        a, d1, d2 = foto(), foto(fecha=F(2026, 10, 23)), foto(fecha=F(2026, 10, 24))
        h = lambda ev, antes, despues: O.huella(ev, "a@sphs.cl", antes, despues)
        self.assertEqual(h("reagendada", a, d1), h("reagendada", a, d1))
        self.assertNotEqual(h("reagendada", a, d1), h("reagendada", a, d2))
        self.assertNotEqual(h("reagendada", a, d1), h("reagendada", d2, d1))      # 22→23 no es 24→23
        self.assertNotEqual(h("reagendada", a, d1), h("asignada", a, d1))
        self.assertNotEqual(O.huella("asignada", "a@sphs.cl", None, d1), O.huella("asignada", "b@sphs.cl", None, d1))


# ───────────────────────── recordatorio de las 17:00 ─────────────────────────
class TestFechasDelRecordatorio(unittest.TestCase):
    def quien(self, y, m, d, h, mi=0):
        return O.dia_agenda_a_avisar(dt.datetime(y, m, d, h, mi), es_dia_habil)

    def test_de_lunes_a_jueves_avisa_el_dia_siguiente(self):
        self.assertEqual(self.quien(2026, 10, 19, 17, 0), F(2026, 10, 20))     # lunes
        self.assertEqual(self.quien(2026, 10, 21, 17, 30), F(2026, 10, 22))    # miércoles
        self.assertEqual(self.quien(2026, 10, 22, 20, 59), F(2026, 10, 23))    # jueves, hasta las 20:59

    def test_el_viernes_avisa_el_lunes(self):
        self.assertEqual(self.quien(2026, 10, 23, 17, 5), F(2026, 10, 26))

    def test_antes_de_las_17_no_sale(self):
        self.assertIsNone(self.quien(2026, 10, 20, 16, 59))
        self.assertIsNone(self.quien(2026, 10, 20, 8, 0))
        self.assertEqual(self.quien(2026, 10, 20, 17, 0), F(2026, 10, 21))

    def test_sabado_y_domingo_nunca_salen(self):
        self.assertIsNone(self.quien(2026, 10, 24, 18, 0))     # sábado
        self.assertIsNone(self.quien(2026, 10, 25, 18, 0))     # domingo

    def test_un_feriado_nunca_sale(self):
        self.assertIsNone(self.quien(2026, 10, 12, 18, 0))     # lunes 12-oct: Encuentro de Dos Mundos
        self.assertIsNone(self.quien(2026, 9, 18, 18, 0))      # viernes Fiestas Patrias

    def test_antes_de_un_feriado_avisa_el_siguiente_dia_habil(self):
        # viernes 9-oct, el lunes 12 es feriado → la agenda del martes 13
        self.assertEqual(self.quien(2026, 10, 9, 17, 30), F(2026, 10, 13))
        # jueves 17-sep: viernes 18 y sábado 19 feriados → lunes 21
        self.assertEqual(self.quien(2026, 9, 17, 17, 30), F(2026, 9, 21))
        # jueves 24-dic: viernes 25 Navidad → lunes 28
        self.assertEqual(self.quien(2026, 12, 24, 17, 30), F(2026, 12, 28))
        # viernes 30-oct: sábado 31 (Iglesias Evangélicas) y domingo 1-nov (Todos los Santos) → lunes 2-nov
        self.assertEqual(self.quien(2026, 10, 30, 17, 30), F(2026, 11, 2))

    def test_siguiente_dia_habil_es_estrictamente_posterior(self):
        self.assertEqual(O.siguiente_dia_habil(F(2026, 10, 19), es_dia_habil), F(2026, 10, 20))
        self.assertEqual(O.siguiente_dia_habil(F(2026, 10, 23), es_dia_habil), F(2026, 10, 26))

    def test_la_fuente_de_feriados_es_cl_feriados(self):
        # REGLA #31: una sola fuente de feriados; el recordatorio no trae una lista propia
        self.assertIn("from cl_feriados import es_dia_habil", _fuente("_ot_tec_barrido_agenda"))
        self.assertNotRegex(_fuente("_ot_tec_barrido_agenda"), r"20\d\d-\d\d-\d\d")


# ───────────────────────── contenido de los correos ─────────────────────────
OT = {"id": 301, "numero_ot": "OT-2026-00301", "cliente": "Mall Plaza <Sur> & Cía", "tipo_label": "Instalación",
      "fecha": F(2026, 10, 22), "fecha_fin": None, "hora_inicio": dt.timedelta(hours=9), "hora_fin": dt.timedelta(hours=11),
      "direccion": "Av. Vicuña Mackenna 1000, Santiago", "equipos": ["Trotadora ILUS Pro 9000", "Bicicleta Spin"],
      "descripcion": "Instalar la trotadora en el segundo piso. Cobrar $50.000 al cliente. Llevar herramientas.\nPagar la factura 123"}


class TestContenido(unittest.TestCase):
    def test_variables_de_una_ot_asignada(self):
        v = O.construir_variables(OT, nombre_tecnico="Ana Pérez", base_url="https://app.ilus.cl/")
        self.assertEqual(v["ot"], "OT-2026-00301")
        self.assertEqual(v["link_ot"], "https://app.ilus.cl/ot/301")          # la ficha NUEVA, no /mantenciones/ot/
        self.assertEqual(v["fecha"], "22/10/2026")
        self.assertEqual(v["horario"], "09:00 – 11:00")
        self.assertIn("Trotadora ILUS Pro 9000", v["maquina"])
        self.assertEqual(v["nombre_tecnico"], "Ana Pérez")

    def test_lo_que_escribe_una_persona_va_escapado(self):
        v = O.construir_variables(OT, nombre_tecnico="<b>Ana</b>")
        self.assertNotIn("<Sur>", v["cliente"])
        self.assertIn("&lt;Sur&gt;", v["cliente"])
        self.assertIn("&amp;", v["cliente"])
        self.assertNotIn("<b>", v["nombre_tecnico"])

    def test_trabajo_interno_sin_cliente(self):
        v = O.construir_variables(dict(OT, cliente=""), nombre_tecnico="Ana")
        self.assertEqual(v["cliente"], "Trabajo interno")

    def test_notas_sin_plata_y_con_lo_util(self):
        v = O.construir_variables(OT, nombre_tecnico="Ana")
        self.assertIn("Instalar la trotadora", v["notas_html"])
        self.assertIn("Llevar herramientas", v["notas_html"])
        self.assertNotIn("$", v["notas_html"])
        self.assertNotIn("50.000", v["notas_html"])
        self.assertNotIn("factura", v["notas_html"].lower())

    def test_sin_notas_la_fila_no_aparece(self):
        self.assertEqual(O.construir_variables(dict(OT, descripcion=""), nombre_tecnico="Ana")["notas_html"], "")
        self.assertEqual(O.construir_variables(dict(OT, descripcion="Cobrar $100"), nombre_tecnico="Ana")["notas_html"], "")

    def test_reagendada_muestra_antes_y_ahora(self):
        antes = foto(fecha=F(2026, 10, 20), hi="08:00", hf="10:00")
        v = O.construir_variables(OT, nombre_tecnico="Ana", antes=antes, motivo="El cliente pidió otro día")
        self.assertEqual(v["fecha_antes"], "20/10/2026")
        self.assertEqual(v["horario_antes"], "08:00 – 10:00")
        self.assertEqual(v["fecha_ahora"], "22/10/2026")
        self.assertIn("ANTES", v["antes_ahora_html"])
        self.assertIn("AHORA", v["antes_ahora_html"])
        self.assertIn("line-through", v["antes_ahora_html"])
        self.assertIn("20/10/2026", v["antes_ahora_html"])
        self.assertIn("22/10/2026", v["antes_ahora_html"])
        self.assertIn("El cliente pidió otro día", v["motivo_html"])

    def test_motivo_con_plata_se_descarta(self):
        v = O.construir_variables(OT, nombre_tecnico="Ana", motivo="No se pagó el anticipo de $30.000")
        self.assertEqual(v["motivo_html"], "")

    def test_agenda_una_fila_por_ot_ordenada_por_hora(self):
        items = [
            {"id": 3, "numero_ot": "OT-3", "cliente": "C3", "tipo_label": "Visita", "hora_inicio": None, "hora_fin": None,
             "direccion": "Dir 3", "equipos": []},
            {"id": 2, "numero_ot": "OT-2", "cliente": "C2", "tipo_label": "Visita", "hora_inicio": dt.timedelta(hours=14),
             "hora_fin": None, "direccion": "Dir 2", "equipos": ["Eq"]},
            {"id": 1, "numero_ot": "OT-1", "cliente": "C1", "tipo_label": "Visita", "hora_inicio": dt.timedelta(hours=9),
             "hora_fin": dt.timedelta(hours=11), "direccion": "Dir 1", "equipos": ["Eq1", "Eq2"],
             "fecha": F(2026, 10, 19), "fecha_fin": F(2026, 10, 21)},
        ]
        h = O.agenda_html(items, "https://app.ilus.cl")
        self.assertLess(h.index("OT-1"), h.index("OT-2"))
        self.assertLess(h.index("OT-2"), h.index("OT-3"))             # la que no tiene hora va al final
        self.assertEqual(h.count("<table"), 3)                        # una fila por OT
        self.assertIn("https://app.ilus.cl/ot/1", h)
        self.assertIn("https://app.ilus.cl/ot/3", h)
        self.assertIn("Varios días", h)
        self.assertIn("Sin hora", h)
        self.assertNotIn("$", h)

    def test_variables_de_la_agenda(self):
        it = [{"id": 1, "numero_ot": "OT-1", "cliente": "C", "hora_inicio": "09:00"}] * 2
        v = O.construir_variables_agenda(it, objetivo=F(2026, 10, 20), hoy=F(2026, 10, 19), nombre_tecnico="Ana",
                                         base_url="https://x.cl")
        self.assertEqual(v["cuando"], "de mañana")
        self.assertEqual(v["dia"], "martes 20/10/2026")
        self.assertEqual(v["n_ot_txt"], "2 órdenes de trabajo")
        v2 = O.construir_variables_agenda(it[:1], objetivo=F(2026, 10, 26), hoy=F(2026, 10, 23), nombre_tecnico="Ana")
        self.assertEqual(v2["cuando"], "del lunes")
        self.assertEqual(v2["n_ot_txt"], "1 orden de trabajo")


class TestConfidencialidad(unittest.TestCase):
    """REGLAS #19 y #26: un técnico NUNCA recibe plata, costos, proveedores, OC ni autorizaciones."""

    def _variables_todas(self):
        antes = foto(fecha=F(2026, 10, 20))
        v = O.construir_variables(OT, nombre_tecnico="Ana", antes=antes, motivo="Pagar la factura $90.000 al proveedor",
                                  base_url="https://app.ilus.cl", tecnicos_nombres=["Ana", "Beto"])
        v.update(O.construir_variables_agenda([dict(OT, hora_inicio="09:00")], objetivo=F(2026, 10, 20),
                                              hoy=F(2026, 10, 19), nombre_tecnico="Ana", base_url="https://app.ilus.cl"))
        return v

    def test_ninguna_semilla_ni_su_render_lleva_plata(self):
        v = self._variables_todas()
        for slug, (asunto, cuerpo) in O.SEMILLAS.items():
            crudo = asunto + " " + cuerpo
            self.assertNotIn("$", crudo, slug)
            self.assertFalse(O.hay_plata(O.texto_sin_marcas(crudo)), slug)
            for render in (O.render_simple(asunto, v), O.render_simple(cuerpo, v)):
                self.assertNotIn("$", render, slug)
                self.assertFalse(O.hay_plata(O.texto_sin_marcas(render)), (slug, O.texto_sin_marcas(render)))
                self.assertNotIn("{{", render, slug)                    # ningún token sin reemplazar

    def test_las_semillas_solo_usan_variables_que_existen(self):
        v = self._variables_todas()
        usadas = set()
        for slug, (asunto, cuerpo) in O.SEMILLAS.items():
            usadas |= set(re.findall(r"\{\{\s*(\w+)\s*\}\}", asunto + cuerpo))
        self.assertFalse(usadas - set(v), f"variables sin definir: {usadas - set(v)}")

    def test_las_variables_documentadas_existen_y_ninguna_es_de_plata(self):
        v = self._variables_todas()
        for nombre, _desc in O.VARIABLES_DOC:
            self.assertIn(nombre, v)
            self.assertFalse(re.search(r"cost|monto|cobr|precio|margen|factur|proveed|\boc\b|valor|neto|iva|pago", nombre),
                             nombre)

    def test_el_filtro_de_texto_libre(self):
        casos = ["Cobrar $50.000", "Revisar la factura 123", "Costo del repuesto", "OC 4500 pendiente", "Pago pendiente",
                 "monto acordado", "precio final", "proveedor Milling", "margen bajo", "Autorización de Daniel",
                 "orden de compra", "Cliente con deuda, se reagenda hasta que pague", "Tiene saldo pendiente de 120.000"]
        for t in casos:
            self.assertTrue(O.hay_plata(t), t)
            self.assertEqual(O.limpiar_texto_tecnico(t), "", t)
        util = O.limpiar_texto_tecnico("Llevar escalera. Revisar cinta, correa y motor.")
        self.assertIn("Llevar escalera", util)
        self.assertIn("Revisar cinta, correa y motor", util)
        self.assertLessEqual(len(O.limpiar_texto_tecnico("palabra " * 200)), 280)


# ───────────────────────── app.py con un mundo en memoria ─────────────────────────
FUNCS_CORREO = ("_ot_tec_llave_cerrada", "_ot_tec_reservar", "_ot_tec_liberar", "_ot_tec_armar", "_ot_tec_enviar",
                "_ot_tec_correo", "_ot_tec_barrido_agenda", "_ot_tec_antes", "_ot_tec_plantilla_apagada",
                "_ot_tec_despachar", "_ot_tec_oculta_detalle", "_ot_tec_agenda_reservar", "_ot_tec_log_agenda",
                "_ot_tec_snapshot_borrado", "_ot_tec_correo_borrada", "_ot_tec_ics")
FUNCS_LLAVE = ("_comm_killswitch_invalidar", "_comm_killswitch_estado", "_modulo_canal_bloqueado", "_mant_send_email_multi")


class Mundo:
    """Técnicos, OT, llaves de paso, plantillas y bandeja de salida en memoria. Lo de la base y el correo se reemplaza."""

    def __init__(self):
        self.outbox, self.logs = [], []
        self.reservas, self.agenda = set(), set()
        self.agenda_pend, self.agenda_vieja = set(), set()     # reservas sin confirmar (enviado=0) y cuáles ya envejecieron
        self.llaves = {"mantenciones": 0, "ot_tecnicos": 0}
        self.global_on = True
        self.t = 1_000_000.0
        self.fotos, self.filas, self.tecs, self.equipos = {}, {}, {}, {}
        self.fichas = {}                 # vid -> correos que salen de la FICHA de una empresa externa
        self.anexo = {}                  # vid -> lo que devuelve _anexo_bloquea_ot (el Anexo sin firmar bloquea la OT)
        self.informados = {}             # vid -> correos a quienes ya se les avisó «asignada»/«reagendada»
        self.inserts_log = 0             # INSERT a mant_logs hechos por las agendas
        self.avance_por_envio = 0        # segundos que "pasan" con cada correo enviado
        self.apagadas, self.db_plantillas = set(), {}
        self.ahora = dt.datetime(2026, 10, 23, 17, 5)          # viernes 17:05
        self.falla_envio = False
        amb = {"print": lambda *a, **k: None, "_otc": O, "time": types.SimpleNamespace(time=lambda: self.t),
               "datetime": dt.datetime, "mysql_fetchone": self._fetchone, "mysql_fetchall": self._fetchall,
               "mysql_execute": self._execute, "mysql_execute_returning_rowcount": self._execute_rc,
               "comm_is_enabled": lambda canal: self.global_on, "_public_base_url": lambda: "https://app.ilus.cl",
               "_brand_subject": lambda t: "ILUS · " + t, "_now_chile": lambda: self.ahora,
               "_comm_render_email_document": lambda titulo, cuerpo, subtitle="": f"<html><!--{subtitle}-->{cuerpo}</html>",
               "_mant_log": lambda ent, eid, acc, det="": self.logs.append((ent, eid, acc, det)),
               "_ot_tec_foto": self._foto, "_anexo_bloquea_ot": lambda vid: self.anexo.get(int(vid)),
               "current_username": lambda: None,
               "_get_brand_cfg": lambda: {"reply_to": "soportetec@sphs.cl"},
               "_ot_tec_filas": lambda vids: {int(v): dict(self.filas[int(v)]) for v in vids if int(v) in self.filas},
               "_ot_tec_equipos_bulk": lambda vids: {int(v): list(self.equipos.get(int(v), [])) for v in vids},
               "_ot_tec_destinatarios_bulk": lambda vids: {int(v): dict(self.tecs.get(int(v), {})) for v in vids},
               "_render_comm_template": self._render_tpl, "_send_ilus_email": self._enviar_ilus_email}
        _cargar(amb, FUNCS_CORREO + FUNCS_LLAVE, ("_MODULOS_KILLSWITCH_VALIDOS", "_KILLSWITCH_CACHE", "_KILLSWITCH_TTL"))
        self.amb = amb

    def _foto(self, vid, con_fichas=False):
        f = self.fotos.get(int(vid))
        if f is None:
            return None
        f = dict(f)
        if con_fichas:
            f["fichas"] = sorted(self.fichas.get(int(vid), ()))
        return f

    # ── base de mentira ──
    def _fetchone(self, sql, params=()):
        if "FROM mant_ot_tec_agenda" in sql:
            if "SELECT enviado" in sql:                                  # ¿está enviada o solo reservada?
                clave = (params[0], params[1])
                if clave not in self.agenda:
                    return None
                return {"enviado": 0 if clave in self.agenda_pend else 1}
            return {"id": 1} if ("*", params[0]) in self.agenda else None
        if "FROM comm_templates" in sql and "activo" in sql:
            return {"activo": 0} if params[1] in self.apagadas else None
        return None

    def _fetchall(self, sql, params=()):
        if "FROM comm_killswitch" in sql:
            return [{"modulo": m, "email_bloqueado": b, "whatsapp_bloqueado": 0, "sms_bloqueado": 0, "motivo": "",
                     "actualizado_por": "", "actualizado_at": None} for m, b in self.llaves.items()]
        if "FROM mant_visitas v" in sql and "BETWEEN" in sql:       # las OT que cubren algún día de la agenda
            primero, ultimo, excluidos = params[2], params[1], set(params[3:])
            return [{"id": vid} for vid, f in self.filas.items()
                    if f["estado"] not in excluidos and f["fecha"] <= ultimo
                    and max(f["fecha"], f.get("fecha_fin") or f["fecha"]) >= primero]
        if "FROM mant_ot_tec_avisos" in sql:                        # a quién ya se le avisó asignada/reagendada
            return [{"email": e} for e in sorted(self.informados.get(int(params[0]), ()))]
        raise AssertionError("consulta inesperada: " + sql[:80])

    def _execute(self, sql, params=()):
        if "DELETE FROM mant_ot_tec_avisos" in sql and "huella<>%s" in sql:     # «el último gana» al reservar un aviso
            vid, email, ventana, huella = params
            self.reservas = {r for r in self.reservas
                             if not (r[0] == vid and r[1] == email and r[4] == ventana and r[3] != huella)}
        elif "DELETE FROM mant_ot_tec_avisos" in sql:                           # liberar la reserva de un aviso que no salió
            self.reservas = {r for r in self.reservas if r[:4] != tuple(params)}
        elif "DELETE FROM mant_ot_tec_agenda" in sql:
            self.agenda.discard((params[0], params[1]))
            self.agenda_pend.discard((params[0], params[1]))
        elif "UPDATE mant_ot_tec_agenda SET enviado=1" in sql:
            self.agenda_pend.discard((params[0], params[1]))
        elif "INSERT IGNORE INTO mant_ot_tec_agenda" in sql and "'*'" in sql:
            self.agenda.add(("*", params[0]))
        elif "INSERT INTO mant_logs" in sql:                        # la bitácora de la agenda: UN insert con todas las filas
            self.inserts_log += 1
            for i in range(0, len(params), 5):
                self.logs.append((params[i], params[i + 1], params[i + 2], params[i + 3]))
        else:
            raise AssertionError("escritura inesperada: " + sql[:80])

    def _execute_rc(self, sql, params=()):
        if "INSERT IGNORE INTO mant_ot_tec_avisos" in sql:
            clave = tuple(params)                      # (vid, email, evento, huella, ventana)
            if clave in self.reservas:
                return 0
            self.reservas.add(clave)
            return 1
        if "INSERT IGNORE INTO mant_ot_tec_agenda" in sql:
            clave = (params[0], params[1])
            if clave in self.agenda:
                return 0
            self.agenda.add(clave)
            self.agenda_pend.add(clave)                  # nace reservada (enviado=0)
            return 1
        if "UPDATE mant_ot_tec_agenda SET creado_at" in sql:          # retomar una reserva vieja sin confirmar
            clave = (params[1], params[2])
            if clave in self.agenda_pend and clave in self.agenda_vieja:
                self.agenda_vieja.discard(clave)
                return 1
            return 0
        raise AssertionError("escritura inesperada: " + sql[:80])

    def _render_tpl(self, slug, canal, variables, modulo=""):
        if slug in self.apagadas or slug not in self.db_plantillas:
            return None
        a, c = self.db_plantillas[slug]
        return O.render_simple(a, variables), O.render_simple(c, variables)

    def _enviar_ilus_email(self, to, asunto, html, *, evento=None, modulo=None, asincrono=False, ref=None, **kw):
        """El choke point real, resumido: interruptor global y llave del módulo (con las funciones REALES de app.py)."""
        if not self.amb["comm_is_enabled"]("email"):
            return False
        if self.amb["_modulo_canal_bloqueado"](modulo or "general", "email"):
            return False
        if self.falla_envio:
            return False
        self.t += self.avance_por_envio
        self.outbox.append({"to": to, "asunto": asunto, "html": html, "evento": evento, "modulo": modulo, "ref": ref,
                            "adjuntos": kw.get("attachments"), "encolar_adjuntos": kw.get("encolar_adjuntos"),
                            "asincrono": asincrono})
        return True

    # ── armado del mundo ──
    def ot(self, vid=301, estado="programada", fecha=F(2026, 10, 26), fecha_fin=None, hi="09:00", hf="11:00",
           tec=None, **extra):
        tec = {"ana@sphs.cl": "Ana Pérez"} if tec is None else tec
        self.fotos[vid] = O.foto(estado, fecha, fecha_fin, hi, hf, tec)
        self.tecs[vid] = dict(tec)
        self.equipos[vid] = list(OT["equipos"])
        self.filas[vid] = dict(OT, id=vid, numero_ot=f"OT-2026-{vid:05d}", estado=estado, fecha=fecha, fecha_fin=fecha_fin,
                               hora_inicio=hi, hora_fin=hf, **extra)
        return self.fotos[vid]

    def cambia(self, vid, **kw):
        """Cambia la OT como lo haría un PUT y devuelve la foto previa."""
        antes = self.fotos[vid]
        self.ot(vid, **{**dict(estado=antes["estado"], fecha=O.a_fecha(antes["fecha"]) if antes["fecha"] else None,
                               fecha_fin=O.a_fecha(antes["fecha_fin"]), hi=antes["hora_inicio"], hf=antes["hora_fin"],
                               tec=dict(antes["tecnicos"])), **kw})
        return antes

    @property
    def correos(self):
        return [(o["to"], o["evento"]) for o in self.outbox]

    def cierra(self, modulo, cerrado=1):
        self.llaves[modulo] = cerrado
        self.amb["_comm_killswitch_invalidar"]()


class TestPuntoUnico(unittest.TestCase):
    def setUp(self):
        self.m = Mundo()

    def test_ot_nueva_avisa_a_cada_tecnico_con_link_a_la_ficha_nueva(self):
        self.m.ot(tec={"ana@sphs.cl": "Ana Pérez", "beto@sphs.cl": "Beto Soto"})
        r = self.m.amb["_ot_tec_correo"](301, antes=None)
        self.assertEqual(sorted(self.m.correos), [("ana@sphs.cl", "ot_asignada"), ("beto@sphs.cl", "ot_asignada")])
        self.assertEqual(len(r["enviados"]), 2)
        o = self.m.outbox[0]
        self.assertEqual(o["modulo"], "ot_tecnicos")                               # la llave propia, no «mantenciones»
        self.assertEqual(o["ref"], {"tipo": "OT", "id": 301, "codigo": "OT-2026-00301"})
        self.assertTrue(o["asunto"].startswith("ILUS · "))
        self.assertIn("https://app.ilus.cl/ot/301", o["html"])
        self.assertNotIn("/mantenciones/ot/", o["html"])
        self.assertIn("Hola Ana Pérez", o["html"])                                  # saludo personal, no la lista
        self.assertIn("26/10/2026", o["html"])
        self.assertIn("09:00 – 11:00", o["html"])
        self.assertIn("Av. Vicuña Mackenna 1000", o["html"])                        # dirección
        self.assertIn("Instalación", o["html"])                                     # tipo
        self.assertIn("Trotadora ILUS Pro 9000", o["html"])                         # equipos
        self.assertIn("Ver orden de trabajo", o["html"])
        self.assertEqual([x[2] for x in self.m.logs], ["aviso_tecnico", "aviso_tecnico"])   # bitácora de la OT

    def test_reagendar_muestra_antes_y_ahora_y_no_se_repite(self):
        self.m.ot(fecha=F(2026, 10, 26))
        antes = self.m.cambia(301, fecha=F(2026, 10, 28), hi="14:00", hf="16:00")
        self.m.amb["_ot_tec_correo"](301, antes, motivo="El cliente pidió otro día")
        self.assertEqual(self.m.correos, [("ana@sphs.cl", "ot_reagendada_tecnico")])
        h = self.m.outbox[0]["html"]
        self.assertIn("26/10/2026", h)           # antes
        self.assertIn("28/10/2026", h)           # ahora
        self.assertIn("09:00 – 11:00", h)
        self.assertIn("14:00 – 16:00", h)
        self.assertIn("ANTES", h)
        self.assertIn("AHORA", h)
        self.assertIn("El cliente pidió otro día", h)
        # el MISMO aviso por el MISMO cambio (doble clic, reintento) no sale dos veces
        r = self.m.amb["_ot_tec_correo"](301, antes)
        self.assertEqual(len(self.m.outbox), 1)
        self.assertEqual(r["omitidos"][0][2], "ya se había enviado")

    def test_un_aviso_nuevo_por_otro_cambio_si_sale(self):
        self.m.ot(fecha=F(2026, 10, 26))
        a1 = self.m.cambia(301, fecha=F(2026, 10, 27))
        self.m.amb["_ot_tec_correo"](301, a1)
        self.m.t += 3600
        a2 = self.m.cambia(301, fecha=F(2026, 10, 28))
        self.m.amb["_ot_tec_correo"](301, a2)
        self.assertEqual(len(self.m.outbox), 2)

    def test_reasignar_y_reagendar_a_la_vez(self):
        self.m.ot(tec={"ana@sphs.cl": "Ana Pérez"})
        antes = self.m.cambia(301, fecha=F(2026, 10, 29), tec={"beto@sphs.cl": "Beto Soto"})
        self.m.amb["_ot_tec_correo"](301, antes)
        self.assertEqual(sorted(self.m.correos), [("ana@sphs.cl", "ot_desasignada_tecnico"), ("beto@sphs.cl", "ot_asignada")])
        nuevo = next(o for o in self.m.outbox if o["to"] == "beto@sphs.cl")
        self.assertIn("29/10/2026", nuevo["html"])                                   # el nuevo recibe la fecha NUEVA
        viejo = next(o for o in self.m.outbox if o["to"] == "ana@sphs.cl")
        self.assertIn("26/10/2026", viejo["html"])                                   # al anterior, lo que tenía en su calendario
        self.assertIn("ya no está a tu cargo", viejo["asunto"])

    def test_el_motivo_del_cambio_solo_va_en_reagendada_y_cancelada(self):
        # reasignar y mover la fecha a la vez, con un motivo escrito: al técnico que SALE y al que ENTRA no se les muestra
        self.m.ot(tec={"ana@sphs.cl": "Ana Pérez"})
        antes = self.m.cambia(301, fecha=F(2026, 10, 29), tec={"beto@sphs.cl": "Beto Soto"})
        self.m.amb["_ot_tec_correo"](301, antes, motivo="El cliente pidió otro día")
        self.assertEqual(sorted(self.m.correos), [("ana@sphs.cl", "ot_desasignada_tecnico"), ("beto@sphs.cl", "ot_asignada")])
        for o in self.m.outbox:
            self.assertNotIn("El cliente pidió otro día", o["html"], o["evento"])
        # en cambio, quien se queda con la OT reagendada y quien recibe la cancelación sí lo leen
        m2 = Mundo()
        m2.ot()
        a2 = m2.cambia(301, fecha=F(2026, 10, 30))
        m2.amb["_ot_tec_correo"](301, a2, motivo="El cliente pidió otro día")
        a3 = m2.cambia(301, estado="cancelada")
        m2.amb["_ot_tec_correo"](301, a3, motivo="El cliente pidió otro día")
        self.assertEqual([o["evento"] for o in m2.outbox], ["ot_reagendada_tecnico", "ot_cancelada_tecnico"])
        for o in m2.outbox:
            self.assertIn("El cliente pidió otro día", o["html"], o["evento"])

    def test_cancelar_avisa_que_no_necesita_ir(self):
        self.m.ot(tec={"ana@sphs.cl": "Ana Pérez", "beto@sphs.cl": "Beto Soto"})
        antes = self.m.cambia(301, estado="cancelada")
        self.m.amb["_ot_tec_correo"](301, antes)
        self.assertEqual(sorted(self.m.correos), [("ana@sphs.cl", "ot_cancelada_tecnico"), ("beto@sphs.cl", "ot_cancelada_tecnico")])
        self.assertIn("no necesitas ir", self.m.outbox[0]["asunto"])
        self.assertIn("26/10/2026", self.m.outbox[0]["html"])
        # guardar de nuevo la misma OT cancelada no vuelve a avisar
        self.m.amb["_ot_tec_correo"](301, self.m.fotos[301])
        self.assertEqual(len(self.m.outbox), 2)

    def test_ot_cerrada_o_completada_no_avisa(self):
        self.m.ot(estado="completada")
        self.m.amb["_ot_tec_correo"](301, antes=None)
        self.m.ot(estado="cerrada")
        self.m.amb["_ot_tec_correo"](301, antes=None)
        self.assertEqual(self.m.outbox, [])

    def test_sin_foto_previa_no_se_avisa_nada(self):
        self.m.ot()
        self.m.amb["_ot_tec_correo"](301, antes=False)
        self.assertEqual(self.m.outbox, [])

    def test_ot_inexistente_o_error_nunca_lanza(self):
        self.assertEqual(self.m.amb["_ot_tec_correo"](999, antes=None)["enviados"], [])
        self.m.amb["_ot_tec_foto"] = lambda vid: (_ for _ in ()).throw(RuntimeError("base caída"))
        self.assertEqual(self.m.amb["_ot_tec_correo"](301, antes=None)["enviados"], [])
        self.assertIs(self.m.amb["_ot_tec_antes"](301), False)

    def test_plantilla_apagada_en_el_editor_no_sale(self):
        self.m.ot()
        self.m.apagadas.add("ot_asignada")
        r = self.m.amb["_ot_tec_correo"](301, antes=None)
        self.assertEqual(self.m.outbox, [])
        self.assertEqual(r["omitidos"][0][2], "plantilla apagada en el editor")

    def test_usa_la_plantilla_editada_por_daniel_y_si_no_la_semilla(self):
        self.m.ot()
        self.m.db_plantillas["ot_asignada"] = ("Te toca la {{ot}}", "<p>Hola {{nombre_tecnico}} — {{direccion}} — {{link_ot}}</p>")
        self.m.amb["_ot_tec_correo"](301, antes=None)
        self.assertEqual(self.m.outbox[0]["asunto"], "ILUS · Te toca la OT-2026-00301")
        self.assertIn("Hola Ana Pérez — Av. Vicuña Mackenna 1000", self.m.outbox[0]["html"])
        self.assertIn("https://app.ilus.cl/ot/301", self.m.outbox[0]["html"])
        # sin plantilla en la base cae a la semilla del código (nunca se omite el aviso)
        m2 = Mundo()
        m2.ot()
        m2.amb["_ot_tec_correo"](301, antes=None)
        self.assertEqual(m2.outbox[0]["asunto"], "ILUS · Nueva OT asignada — OT-2026-00301")

    def test_el_asunto_es_texto_plano_y_el_cuerpo_va_escapado(self):
        self.m.ot()
        self.m.db_plantillas["ot_asignada"] = ("OT {{ot}} · {{cliente}}", "<p>{{cliente}}</p>")
        self.m.amb["_ot_tec_correo"](301, antes=None)
        o = self.m.outbox[0]
        self.assertEqual(o["asunto"], "ILUS · OT OT-2026-00301 · Mall Plaza <Sur> & Cía")      # texto plano
        self.assertIn("Mall Plaza &lt;Sur&gt; &amp; Cía", o["html"])                          # HTML escapado

    def test_nunca_lleva_plata_aunque_la_ot_la_mencione(self):
        self.m.ot(tec={"ana@sphs.cl": "Ana Pérez", "beto@sphs.cl": "Beto Soto"},
                  descripcion="Instalar. Cobrar $120.000 más IVA. Pagar al proveedor Milling. Factura 4455 y OC 77")
        self.m.amb["_ot_tec_correo"](301, antes=None)
        antes = self.m.cambia(301, fecha=F(2026, 10, 30), tec={"ana@sphs.cl": "Ana Pérez"})
        self.m.amb["_ot_tec_correo"](301, antes, motivo="Se atrasó el pago del anticipo ($50.000)")
        antes = self.m.cambia(301, estado="cancelada")
        self.m.amb["_ot_tec_correo"](301, antes, motivo="Sin OC aprobada")
        self.assertGreaterEqual(len(self.m.outbox), 5)
        for o in self.m.outbox:
            visible = O.texto_sin_marcas(o["html"] + " " + o["asunto"])
            self.assertNotIn("$", visible)
            self.assertFalse(O.hay_plata(visible), (o["evento"], visible))

    def test_si_el_proveedor_de_correo_falla_se_libera_para_reintentar(self):
        self.m.ot()
        self.m.falla_envio = True
        self.m.amb["_ot_tec_correo"](301, antes=None)
        self.assertEqual(self.m.outbox, [])
        self.assertEqual(self.m.reservas, set())
        self.m.falla_envio = False
        self.m.amb["_ot_tec_correo"](301, antes=None)
        self.assertEqual(len(self.m.outbox), 1)


class TestLlavePropia(unittest.TestCase):
    """«El correo al CLIENTE sigue apagado; separa los interruptores para encender solo el de técnicos»."""

    def test_con_ot_tecnicos_abierta_y_mantenciones_cerrada_el_tecnico_recibe_y_el_cliente_no(self):
        m = Mundo()
        m.cierra("mantenciones", 1)
        m.cierra("ot_tecnicos", 0)
        m.ot()
        m.amb["_ot_tec_correo"](301, antes=None)
        self.assertEqual(m.correos, [("ana@sphs.cl", "ot_asignada")])
        # el correo al cliente usa la función REAL de siempre, con la llave 'mantenciones': queda frenado
        res = m.amb["_mant_send_email_multi"](None, "Visita técnica programada", "<p>x</p>", evento="visita_agendada",
                                              emails=["cliente@empresa.cl"])
        self.assertEqual(res["enviados"], [])
        self.assertEqual(res["bloqueados"], 1)
        self.assertEqual(m.correos, [("ana@sphs.cl", "ot_asignada")])

    def test_con_ot_tecnicos_cerrada_el_tecnico_no_recibe_aunque_mantenciones_este_abierta(self):
        m = Mundo()
        m.cierra("mantenciones", 0)
        m.cierra("ot_tecnicos", 1)
        m.ot()
        m.amb["_ot_tec_correo"](301, antes=None)
        self.assertEqual(m.outbox, [])
        self.assertEqual(m.reservas, set())            # no consumió la reserva: al abrir la llave un cambio nuevo sí sale
        res = m.amb["_mant_send_email_multi"](None, "x", "<p>x</p>", emails=["cliente@empresa.cl"])
        self.assertEqual(res["enviados"], ["cliente@empresa.cl"])      # el cliente depende SOLO de su llave

    def test_el_interruptor_general_apaga_todo(self):
        m = Mundo()
        m.global_on = False
        m.ot()
        m.amb["_ot_tec_correo"](301, antes=None)
        self.assertEqual(m.outbox, [])
        self.assertTrue(m.amb["_ot_tec_llave_cerrada"]())

    def test_la_llave_nueva_existe_y_se_ve_en_el_front(self):
        m = Mundo()
        self.assertIn("ot_tecnicos", m.amb["_MODULOS_KILLSWITCH_VALIDOS"])
        self.assertIn("mantenciones", m.amb["_MODULOS_KILLSWITCH_VALIDOS"])
        self.assertRegex(_fuente("comm_killswitch_get"), r'"ot_tecnicos":\s+"Correos a técnicos \(OT\)"')
        with open(os.path.join(RAIZ, "static", "comunicaciones_index.js"), encoding="utf-8") as f:
            js = f.read()
        self.assertRegex(js, r"ot_tecnicos:\s+\{ label:'Correos a técnicos \(OT\)'")

    def test_el_choke_point_envia_por_la_llave_propia_y_el_cliente_sigue_con_la_suya(self):
        self.assertIn("modulo=_otc.MODULO_LLAVE", _fuente("_ot_tec_enviar"))
        self.assertEqual(O.MODULO_LLAVE, "ot_tecnicos")
        self.assertIn('modulo="mantenciones"', _fuente("_mant_send_email_multi"))
        self.assertIn("_mant_send_email_multi(", _fuente("_mant_ot_email_cliente_creacion"))
        self.assertNotIn("ot_tecnicos", _fuente("_mant_ot_email_cliente_creacion"))
        self.assertNotIn("_ot_tec_", _fuente("_mant_ot_email_cliente_creacion"))

    def test_la_llave_nace_encendida_una_sola_vez_y_nunca_se_vuelve_a_encender(self):
        src = _fuente("_ensure_ot_tecnicos_killswitch")
        self.assertIn("INSERT IGNORE INTO comm_killswitch", src)
        self.assertIn("'ot_tecnicos', 0, 0, 0", src)
        for prohibido in ("UPDATE comm_killswitch", "REPLACE INTO", "ON DUPLICATE KEY", "DELETE FROM comm_killswitch"):
            self.assertNotIn(prohibido, src)
        capturadas = []
        amb = {"print": lambda *a, **k: None, "mysql_execute": lambda sql, p=None: capturadas.append((sql, p))}
        _cargar(amb, ("_ensure_ot_tecnicos_killswitch",))
        amb["_ensure_ot_tecnicos_killswitch"]()
        self.assertTrue(any("INSERT IGNORE" in s for s, _ in capturadas))
        self.assertFalse(any(s.lstrip().upper().startswith(("UPDATE", "DELETE", "REPLACE")) for s, _ in capturadas))


class TestRecordatorio(unittest.TestCase):
    def setUp(self):
        self.m = Mundo()
        # lunes 26-oct: Ana tiene 2 OT (una sin hora), Beto 1, Carla ninguna
        self.m.ot(301, fecha=F(2026, 10, 26), hi="14:00", hf="16:00", tec={"ana@sphs.cl": "Ana Pérez"})
        self.m.ot(302, fecha=F(2026, 10, 26), hi="09:00", hf="11:00", tec={"ana@sphs.cl": "Ana Pérez", "beto@sphs.cl": "Beto Soto"})
        self.m.ot(303, fecha=F(2026, 10, 27), tec={"carla@sphs.cl": "Carla Díaz"})            # martes: no es la agenda del lunes

    def barrido(self, **kw):
        return self.m.amb["_ot_tec_barrido_agenda"](**kw)

    def test_el_viernes_manda_la_agenda_del_lunes_ordenada_por_hora(self):
        r = self.barrido()
        self.assertEqual(r["agenda_de"], "26/10/2026")
        self.assertEqual(sorted(o["to"] for o in self.m.outbox), ["ana@sphs.cl", "beto@sphs.cl"])      # Carla no: sin OT ese día
        ana = next(o for o in self.m.outbox if o["to"] == "ana@sphs.cl")
        self.assertEqual(ana["evento"], "agenda_tecnico_manana")
        self.assertEqual(ana["modulo"], "ot_tecnicos")
        self.assertIn("2 órdenes de trabajo", ana["asunto"])
        self.assertIn("del lunes", ana["asunto"])
        self.assertIn("lunes 26/10/2026", ana["html"])
        self.assertLess(ana["html"].index("OT-2026-00302"), ana["html"].index("OT-2026-00301"))     # 09:00 antes que 14:00
        self.assertIn("https://app.ilus.cl/ot/301", ana["html"])
        self.assertIn("https://app.ilus.cl/ot/302", ana["html"])
        self.assertNotIn("$", ana["html"])
        self.assertEqual(r["pendientes"], [])
        # queda constancia en la bitácora de cada OT incluida
        self.assertEqual(sorted(l[1] for l in self.m.logs), [301, 302, 302])

    def test_de_lunes_a_jueves_dice_de_manana(self):
        self.m.ahora = dt.datetime(2026, 10, 25, 17, 0)        # domingo → no sale
        self.assertIn("fuera de ventana", self.barrido()["motivo"])
        self.m.ahora = dt.datetime(2026, 10, 26, 17, 0)        # lunes 17:00 → agenda del martes (Carla)
        self.barrido()
        self.assertEqual([o["to"] for o in self.m.outbox], ["carla@sphs.cl"])
        self.assertIn("de mañana", self.m.outbox[0]["asunto"])
        self.assertIn("1 orden de trabajo", self.m.outbox[0]["asunto"])

    def test_antes_de_las_17_sabado_domingo_y_feriado_no_sale_nada(self):
        for ahora in (dt.datetime(2026, 10, 23, 16, 59), dt.datetime(2026, 10, 23, 9, 0), dt.datetime(2026, 10, 24, 18, 0),
                      dt.datetime(2026, 10, 25, 18, 0), dt.datetime(2026, 10, 12, 18, 0)):
            self.m.ahora = ahora
            r = self.barrido()
            self.assertIn("fuera de ventana", r["motivo"], ahora)
            self.assertEqual(self.m.outbox, [], ahora)

    def test_victima_de_un_feriado_avisa_el_siguiente_dia_habil(self):
        self.m.ot(304, fecha=F(2026, 10, 13), hi="10:00", hf="12:00", tec={"dani@sphs.cl": "Dani"})   # martes 13
        self.m.ot(305, fecha=F(2026, 10, 12), hi="10:00", hf="12:00", tec={"eva@sphs.cl": "Eva"})     # el feriado
        self.m.ahora = dt.datetime(2026, 10, 9, 17, 30)        # viernes antes del feriado del lunes 12
        r = self.barrido()
        self.assertEqual(r["agenda_de"], "13/10/2026")
        # Revisión 19-oct: una OT agendada EN el feriado (se permite, con aviso) también se avisa: antes no salía en ninguna agenda
        self.assertEqual(sorted(o["to"] for o in self.m.outbox), ["dani@sphs.cl", "eva@sphs.cl"])
        eva = next(o for o in self.m.outbox if o["to"] == "eva@sphs.cl")
        self.assertIn("del lunes", eva["asunto"])
        self.assertIn("OT-2026-00305", eva["html"])

    def test_es_idempotente_aunque_el_trabajo_corra_cada_10_minutos(self):
        self.barrido()
        n = len(self.m.outbox)
        self.assertEqual(n, 2)
        for minuto in (15, 25, 35, 55):
            self.m.ahora = dt.datetime(2026, 10, 23, 17, minuto)
            r = self.barrido()
            self.assertEqual(len(self.m.outbox), n, minuto)
            self.assertEqual(r["motivo"], "la agenda de ese día ya se envió completa")

    def test_ot_de_varios_dias_aparece_el_dia_que_cae_dentro_del_rango(self):
        self.m.ot(306, fecha=F(2026, 10, 22), fecha_fin=F(2026, 10, 28), hi="08:00", hf="18:00", tec={"fer@sphs.cl": "Fer"})
        self.barrido()
        fer = next(o for o in self.m.outbox if o["to"] == "fer@sphs.cl")
        self.assertIn("OT-2026-00306", fer["html"])
        self.assertIn("Varios días", fer["html"])

    def test_no_incluye_ot_canceladas_anuladas_cerradas_o_completadas(self):
        for i, est in enumerate(("cancelada", "anulada", "cerrada", "completada"), start=410):
            self.m.ot(i, estado=est, fecha=F(2026, 10, 26), tec={f"x{i}@sphs.cl": "X"})
        self.barrido()
        self.assertEqual(sorted(o["to"] for o in self.m.outbox), ["ana@sphs.cl", "beto@sphs.cl"])
        sql = _fuente("_ot_tec_barrido_agenda")
        self.assertIn("NOT IN ({ph_e})", sql)
        self.assertIn("GREATEST(v.fecha_programada, COALESCE(v.fecha_fin, v.fecha_programada)) >= %s", sql)
        self.assertEqual(O.ESTADOS_SIN_AGENDA, {"cancelada", "anulada", "cerrada", "completada", "firmada_tecnico",
                                                "pendiente_aprobacion"})

    def test_dry_solo_muestra_lo_que_mandaria(self):
        r = self.barrido(dry=True)
        self.assertEqual(self.m.outbox, [])
        self.assertEqual(self.m.agenda, set())
        self.assertEqual({e["tecnico"]: e["n_ot"] for e in r["enviados"]}, {"ana@sphs.cl": 2, "beto@sphs.cl": 1})
        # con &fecha= ve la agenda de ese día aunque no sean las 17:00
        self.m.ahora = dt.datetime(2026, 10, 23, 9, 0)
        r2 = self.barrido(dry=True, fecha="2026-10-27")
        self.assertEqual([e["tecnico"] for e in r2["enviados"]], ["carla@sphs.cl"])
        self.assertEqual(self.m.outbox, [])

    def test_con_la_llave_cerrada_no_manda_ni_reserva_y_al_abrirla_sale(self):
        self.m.cierra("ot_tecnicos", 1)
        r = self.barrido()
        self.assertTrue(r["bloqueado"])
        self.assertEqual(self.m.outbox, [])
        self.assertEqual(self.m.agenda, set())
        self.m.cierra("ot_tecnicos", 0)
        self.m.ahora = dt.datetime(2026, 10, 23, 17, 15)
        self.barrido()
        self.assertEqual(sorted(o["to"] for o in self.m.outbox), ["ana@sphs.cl", "beto@sphs.cl"])

    def test_si_un_correo_falla_se_reintenta_en_el_siguiente_ciclo(self):
        self.m.falla_envio = True
        r = self.barrido()
        self.assertEqual(sorted(r["pendientes"]), ["ana@sphs.cl", "beto@sphs.cl"])
        self.assertEqual(self.m.agenda, set())                  # liberó las reservas y NO marcó el día como completo
        self.m.falla_envio = False
        self.m.ahora = dt.datetime(2026, 10, 23, 17, 15)
        self.barrido()
        self.assertEqual(len(self.m.outbox), 2)
        self.assertIn(("*", F(2026, 10, 26)), self.m.agenda)

    def test_plantilla_apagada_no_manda_la_agenda(self):
        self.m.apagadas.add("agenda_tecnico_manana")
        r = self.barrido()
        self.assertEqual(self.m.outbox, [])
        self.assertIn("apagada", r["motivo"])

    def test_el_trabajo_periodico_y_la_ruta_propia_estan_conectados(self):
        self.assertIn("_ot_tec_barrido_agenda(", _fuente("tr_cron_simpliroute_poll"))
        ruta = _fuente("mant_cron_agenda_tecnicos")
        for texto in ("_cron_extract_token()", "compare_digest", "forbidden", "dry", "_ot_tec_barrido_agenda("):
            self.assertIn(texto, ruta)
        codigo = _codigo()
        self.assertIn('@app.route("/mantenciones/cron/agenda-tecnicos", methods=["GET", "POST"])', codigo)
        self.assertIn('"/mantenciones/cron/agenda-tecnicos",', codigo)            # exenta de CSRF SOLO esa ruta exacta


class TestCableado(unittest.TestCase):
    """Cada camino que asigna, reagenda, cancela o quita un técnico llama al punto único, DESPUÉS del commit."""

    def test_la_creacion_del_asistente_y_la_de_levantamientos_delegan_en_el_punto_unico(self):
        self.assertIn("_ot_tec_correo(vid, antes=None)", _fuente("_mant_ot_email_tecnico_creacion"))
        self.assertIn("_mant_ot_email_tecnico_creacion(vid)", _fuente("_ot2_crear_core"))
        self.assertIn("_mant_ot_email_tecnico_creacion(visita_id)", _fuente("_mant_lev_crear_ot_core"))
        self.assertIn("_mant_ot_email_tecnico_creacion(vid)", _fuente("_mant_visita_crear_core"))
        self.assertIn("_mant_ot_email_tecnico_creacion(vid)", _fuente("mant_visita_multi"))

    def test_el_put_de_la_ot_toma_la_foto_antes_del_update_y_avisa_despues_del_commit(self):
        s = _fuente("mant_visita_update")
        i_antes = s.index("_tec_antes = _ot_tec_antes(vid) if _tec_avisar else None")
        i_update = s.index("UPDATE mant_visitas SET {','.join(sets)} WHERE id=%s")
        i_commit = s.index("conn.commit()", i_update)
        i_aviso = s.index("_ot_tec_correo(vid, _tec_antes", i_commit)
        self.assertLess(i_antes, i_update)
        self.assertLess(i_update, i_commit)
        self.assertLess(i_commit, i_aviso)
        for campo in ('"fecha_programada"', '"fecha_fin"', '"hora_inicio"', '"hora_fin"', '"tecnico_user_id"', '"estado"'):
            self.assertIn(campo, s[i_antes - 600:i_antes])
        self.assertIn("motivo_cambio", s[i_aviso:i_aviso + 200])

    def test_agregar_y_quitar_colaborador(self):
        q, a = _fuente("mant_visita_tecnico_quitar"), _fuente("mant_visita_tecnico_agregar")
        self.assertLess(q.index("_ot_tec_antes(vid)"), q.index("DELETE FROM mant_visita_tecnicos"))
        self.assertLess(q.index("DELETE FROM mant_visita_tecnicos"), q.index("_ot_tec_correo(vid, _tec_antes)"))
        self.assertLess(a.index("_ot_tec_antes(vid)"), a.index("INSERT INTO mant_visita_tecnicos"))
        self.assertLess(a.index("conn.commit()"), a.index("_ot_tec_correo(vid, _tec_antes)"))

    def test_los_avisos_jamas_rompen_la_operacion(self):
        for nombre in ("mant_visita_update", "mant_visita_tecnico_quitar", "mant_visita_tecnico_agregar"):
            s = _fuente(nombre)
            i = s.index("_ot_tec_correo(")
            self.assertIn("try", s[max(0, i - 160):i], nombre)
        self.assertIn("except Exception", _fuente("_ot_tec_correo"))
        self.assertIn("except Exception", _fuente("_ot_tec_barrido_agenda"))

    def test_el_correo_al_cliente_no_se_toca(self):
        s = _fuente("_ot2_crear_core")
        self.assertIn("_mant_ot_email_cliente_creacion(vid)", s)
        self.assertIn("if not es_interna:", s)
        c = _fuente("_mant_ot_email_cliente_creacion")
        self.assertIn('evento="visita_agendada"', c)
        self.assertIn("visita_agendada", c)
        # las variables del correo al CLIENTE conservan su link de siempre: el aviso al técnico arma el suyo
        # (/ot/<id>, la ficha nueva) en ot_correos_tecnicos y no toca este diccionario
        v = _fuente("_mant_ot_creacion_variables")
        self.assertIn('"link_ot": f"{_public_base_url()}/mantenciones/ot/{int(vid)}"', v)
        self.assertNotIn("_ot_tec_", v)
        self.assertNotIn("_mant_ot_creacion_variables", _fuente("_mant_ot_email_tecnico_creacion"))
        self.assertNotIn("_mant_ot_creacion_variables", _fuente("_ot_tec_correo"))

    def test_siembra_al_arrancar_aunque_se_salten_las_migraciones(self):
        codigo = _codigo()
        i = codigo.index("_ensure_ot_tec_tablas()\n        _ensure_ot_tecnicos_killswitch()\n        _ensure_comm_template_ot_tecnicos()")
        self.assertGreater(i, codigo.index("_ensure_comm_template_mantenciones()\n"))   # 'ot_asignada' ya sembrada
        self.assertIn("with app.app_context():", codigo[i - 80:i])

    def test_las_tablas_son_baratas_regla_18(self):
        s = _fuente("_ensure_ot_tec_tablas")
        self.assertEqual(s.count("CREATE TABLE IF NOT EXISTS mant_ot_tec_"), 2)
        self.assertIn("UNIQUE KEY uq_ot_tec_aviso (visita_id, email, evento, huella, ventana)", s)
        self.assertIn("UNIQUE KEY uq_ot_tec_agenda (tecnico_email, fecha_agenda)", s)
        # «reservado» → «enviado»: la columna nace en el CREATE y, para una base que ya tenía la tabla, llega con UN solo
        # ALTER de una cláusula (la guardia de DDL lo salta sin pedir lock cuando ya existe)
        self.assertIn("enviado       TINYINT(1) NOT NULL DEFAULT 1", s)
        self.assertEqual(s.count("ALTER TABLE"), 1)
        self.assertIn('mysql_execute("ALTER TABLE mant_ot_tec_agenda ADD COLUMN enviado TINYINT(1) NOT NULL DEFAULT 1")', s)
        for peligro in ("DROP", "TRUNCATE", "MODIFY", "CHANGE COLUMN"):
            self.assertNotIn(peligro, s)

    def test_lecturas_en_bloque_y_sin_plata(self):
        filas, dest = _fuente("_ot_tec_filas"), _fuente("_ot_tec_destinatarios_bulk")
        for texto in (filas, dest, _fuente("_ot_tec_equipos_bulk")):
            self.assertIn("IN ({ph", texto)                                    # una consulta para varias OT
        for prohibido in ("costo", "zz_", "monto", "factura", "proveedor_", "valorizado", "finanzas", "margen", "oc_numero",
                          "cobro", "precio"):
            self.assertNotIn(prohibido, filas.lower(), "columna de plata en el SELECT de la OT")
        for prohibido in ("valor_hora", "valor_visita", "rut_empresa", "numero_cuenta", "banco", "tarifa"):
            self.assertNotIn(prohibido, dest, "dato del proveedor en la lectura de destinatarios")
        self.assertIn("contacto_email", dest)                                  # el correo de contacto del técnico externo

    def test_toda_consulta_nueva_usa_parametros(self):
        for nombre in ("_ot_tec_reservar", "_ot_tec_liberar", "_ot_tec_barrido_agenda", "_ot_tec_plantilla_apagada",
                       "_ot_tec_agenda_reservar", "_ot_tec_log_agenda", "_ot_tec_snapshot_borrado"):
            s = _fuente(nombre)
            self.assertNotRegex(s, r'"[^"\n]*(SELECT|INSERT|DELETE)[^"\n]*"\s*%\s*\(', nombre)     # nada de "..." % (...)
            self.assertNotRegex(s, r'f"[^"\n]*(INSERT|DELETE)[^"\n]*\{(?!ph)', nombre)


class TestLecturas(unittest.TestCase):
    """Las lecturas de la base (con una base de mentira): de dónde sale cada técnico y cada dato del correo."""

    def amb(self, fetchall=None, fetchone=None):
        amb = {"print": lambda *a, **k: None, "_otc": O, "AUTH_TABLE": "app_users",
               "_TIPO_OT_LABEL": {"instalacion": "Instalación", "preventiva": "Mantención preventiva"},
               "mysql_fetchall": fetchall or (lambda sql, p=None: []), "mysql_fetchone": fetchone or (lambda sql, p=None: None)}
        _cargar(amb, ("_ot_tec_ids", "_ot_tec_destinatarios_bulk", "_ot_tec_filas", "_ot_tec_equipos_bulk",
                      "_ot_tec_foto", "_ot_tec_antes"))
        return amb

    def test_destinatarios_suma_todas_las_vias_sin_repetir(self):
        consultas = []

        def fetchall(sql, p=None):
            consultas.append(sql)
            if "FROM mant_visitas v JOIN `app_users`" in sql:        # técnico principal con cuenta
                return [{"vid": 1, "uid": 10, "email": "ANA@sphs.cl", "nombre": "Ana"},
                        {"vid": 2, "uid": 12, "email": "sin-arroba", "nombre": "Mal"}]
            if "FROM mant_visita_tecnicos vt JOIN `app_users`" in sql:   # colaboradores con cuenta
                return [{"vid": 1, "uid": 11, "email": "beto@sphs.cl", "nombre": "Beto"},
                        {"vid": 1, "uid": 10, "email": "ana@sphs.cl", "nombre": "Ana (dup)"}]
            if "JOIN mant_tecnicos t" in sql:                         # catálogo legacy
                return [{"vid": 1, "email": "legacy@x.cl", "nombre": "Legacy"}]
            if "FROM mant_tecnicos_externos te WHERE te.user_id IN" in sql:
                return [{"uid": 10, "email": "contacto@empresa.cl", "nombre": "Empresa SpA"}]
            if "mant_tecnico_externo_usuarios" in sql:
                raise RuntimeError("tabla aún no existe")             # una vía que falla no detiene las demás
            return []                                                 # externos por nombre
        a = self.amb(fetchall)
        r = a["_ot_tec_destinatarios_bulk"]([1, 2, "3", None])
        self.assertEqual(r[1], {"ana@sphs.cl": "Ana", "beto@sphs.cl": "Beto", "legacy@x.cl": "Legacy",
                                "contacto@empresa.cl": "Empresa SpA"})
        self.assertEqual(r[2], {})                                    # correo inválido: fuera
        self.assertEqual(r[3], {})
        self.assertEqual(sorted(r), [1, 2, 3])
        self.assertEqual(len(consultas), 6)                           # en bloque: no crece con la cantidad de OT

    def test_el_principal_anterior_no_sigue_como_colaborador_tras_reasignar(self):
        """«Reasignar» solo cambia mant_visitas.tecnico_user_id; la copia del principal en mant_visita_tecnicos (rol 'lider')
        queda atrás. Si contara como colaborador, el técnico anterior nunca recibiría «ya no la tienes» y seguiría
        recibiendo su agenda. La consulta de colaboradores deja fuera las filas 'lider' (el principal vigente sale de
        la consulta del principal)."""
        sqls = []
        a = self.amb(lambda sql, p=None: (sqls.append(sql), [])[1])
        a["_ot_tec_destinatarios_bulk"]([1])
        colab = next(s for s in sqls if "FROM mant_visita_tecnicos vt JOIN `app_users`" in s)
        self.assertIn("COALESCE(vt.rol,'') <> 'lider'", colab)
        principal = next(s for s in sqls if "FROM mant_visitas v JOIN `app_users`" in s)
        self.assertIn("u.id=v.tecnico_user_id", principal)
        # y el diff lo traduce bien: Ana (principal anterior) sale, Beto entra
        antes = foto(tec={"ana@sphs.cl": "Ana"})
        despues = foto(tec={"beto@sphs.cl": "Beto"})
        self.assertEqual(sorted((x["email"], x["evento"]) for x in O.decidir_avisos(antes, despues)),
                         [("ana@sphs.cl", "desasignada"), ("beto@sphs.cl", "asignada")])

    def test_destinatarios_sin_ot_no_consulta_nada(self):
        a = self.amb(lambda sql, p=None: (_ for _ in ()).throw(AssertionError("no debía consultar")))
        self.assertEqual(a["_ot_tec_destinatarios_bulk"]([]), {})

    def test_filas_arma_la_direccion_y_el_tipo(self):
        filas = [{"id": 5, "numero_ot": "OT-5", "descripcion": "Instalar", "tipo": "instalacion", "estado": "programada",
                  "fecha_programada": F(2026, 10, 26), "fecha_fin": None, "hora_inicio": dt.timedelta(hours=9),
                  "hora_fin": None, "razon_social": "Gimnasio X", "dir_visita": "Av. Apoquindo 100, Las Condes, Chile",
                  "comuna_visita": "Las Condes", "cli_dir": "Otra 1", "cli_comuna": "Otra"},
                 {"id": 6, "numero_ot": None, "descripcion": None, "tipo": "preventiva", "estado": "programada",
                  "fecha_programada": F(2026, 10, 27), "fecha_fin": None, "hora_inicio": None, "hora_fin": None,
                  "razon_social": None, "dir_visita": None, "comuna_visita": None, "cli_dir": "Calle 2 #3", "cli_comuna": "Ñuñoa"}]
        sqls = []
        a = self.amb(lambda sql, p=None: (sqls.append((sql, p)), filas)[1])
        r = a["_ot_tec_filas"]([5, 6, 6])
        self.assertEqual(r[5]["direccion"], "Av. Apoquindo 100, Las Condes, Chile")     # no repite la comuna
        self.assertEqual(r[5]["tipo_label"], "Instalación")
        self.assertEqual(r[5]["cliente"], "Gimnasio X")
        self.assertEqual(r[6]["direccion"], "Calle 2 #3, Ñuñoa")                        # cae a la dirección del cliente
        self.assertEqual(r[6]["cliente"], "")
        self.assertEqual(sqls[0][1], (5, 6))                                            # ids únicos y ordenados, por parámetro
        self.assertEqual(a["_ot_tec_filas"]([]), {})

    def test_filas_funciona_en_una_base_sin_direccion_comuna(self):
        intentos = []

        def fetchall(sql, p=None):
            intentos.append(sql)
            if "v.direccion_comuna" in sql:
                raise RuntimeError("Unknown column 'v.direccion_comuna'")
            return [{"id": 5, "numero_ot": "OT-5", "descripcion": "", "tipo": "x", "estado": "programada",
                     "fecha_programada": F(2026, 10, 26), "fecha_fin": None, "hora_inicio": None, "hora_fin": None,
                     "razon_social": "C", "dir_visita": "Dir", "comuna_visita": None, "cli_dir": None, "cli_comuna": "Com"}]
        r = self.amb(fetchall)["_ot_tec_filas"]([5])
        self.assertEqual(len(intentos), 2)
        self.assertEqual(r[5]["direccion"], "Dir, Com")

    def test_equipos_en_bloque_y_si_falla_queda_vacio(self):
        a = self.amb(lambda sql, p=None: [{"vid": 1, "nombre": "Trotadora"}, {"vid": 1, "nombre": " "}, {"vid": 2, "nombre": "Spin"}])
        self.assertEqual(a["_ot_tec_equipos_bulk"]([1, 2, 3]), {1: ["Trotadora"], 2: ["Spin"], 3: []})
        b = self.amb(lambda sql, p=None: (_ for _ in ()).throw(RuntimeError("caída")))
        self.assertEqual(b["_ot_tec_equipos_bulk"]([1]), {1: []})

    def test_foto_y_antes(self):
        fetchone = lambda sql, p=None: ({"estado": "Programada", "fecha_programada": F(2026, 10, 26), "fecha_fin": F(2026, 10, 26),
                                         "hora_inicio": dt.timedelta(hours=9), "hora_fin": dt.timedelta(hours=11)}
                                        if p == (7,) else None)
        a = self.amb(fetchone=fetchone)
        a["_ot_tec_destinatarios_bulk"] = lambda vids: {7: {"ana@sphs.cl": "Ana"}}
        f = a["_ot_tec_foto"](7)
        self.assertEqual(f, {"estado": "programada", "fecha": "2026-10-26", "fecha_fin": "", "hora_inicio": "09:00",
                             "hora_fin": "11:00", "tecnicos": {"ana@sphs.cl": "Ana"}})
        self.assertIsNone(a["_ot_tec_foto"](8))
        self.assertEqual(a["_ot_tec_antes"](7), f)
        self.assertIs(a["_ot_tec_antes"](8), False)               # OT inexistente: no se avisa nada

    def test_la_foto_cierra_la_lectura_anterior_para_ver_lo_recien_guardado(self):
        """La conexión del request es REPEATABLE READ: sin un commit antes de leer, la foto de DESPUÉS no vería el UPDATE
        que otra conexión acaba de comprometer y el aviso saldría con la fecha vieja (gotcha de numero_ot/numero_ticket)."""
        eventos = []
        a = self.amb(fetchone=lambda sql, p=None: (eventos.append("select"),
                                                   {"estado": "programada", "fecha_programada": F(2026, 10, 26),
                                                    "fecha_fin": None, "hora_inicio": None, "hora_fin": None})[1])
        a["get_db"] = lambda: types.SimpleNamespace(commit=lambda: eventos.append("commit"))
        a["_ot_tec_destinatarios_bulk"] = lambda vids: {7: {}}
        a["_ot_tec_foto"](7)
        self.assertEqual(eventos, ["commit", "select"])
        # y si el commit falla (sin contexto de request, por ejemplo) igual lee
        a["get_db"] = lambda: (_ for _ in ()).throw(RuntimeError("sin contexto"))
        self.assertIsNotNone(a["_ot_tec_foto"](7))


class TestSiembraDePlantillas(unittest.TestCase):
    def mundo(self, filas):
        """filas: [(slug, asunto, cuerpo)] ya existentes en comm_templates (módulo mantenciones, canal email)."""
        capt = {"updates": [], "inserts": []}
        por_slug = {slug: (asunto, cuerpo) for slug, asunto, cuerpo in filas}

        def fetchone(sql, params=()):
            if "estado='ot_asignada'" in sql:                       # la lectura de 'ot_asignada' (asunto y cuerpo)
                a = por_slug.get("ot_asignada")
                return {"id": 1, "asunto": a[0], "cuerpo": a[1]} if a else None
            return {"id": 7} if params[1] in por_slug else None     # ¿ya existe esa plantilla?

        def execute(sql, params=()):
            (capt["updates"] if sql.lstrip().startswith("UPDATE") else capt["inserts"]).append((sql, params))

        amb = {"print": lambda *a, **k: None, "_otc": O, "mysql_fetchone": fetchone, "mysql_execute": execute}
        _cargar(amb, ("_ensure_comm_template_ot_tecnicos", "_mantenciones_tpl_seed"))
        return amb, capt

    def test_base_vacia_siembra_las_cuatro_nuevas_con_el_contenido_del_modulo(self):
        amb, capt = self.mundo([])
        n = amb["_ensure_comm_template_ot_tecnicos"]()
        self.assertEqual(n, 4)
        slugs = sorted(p[1] for _s, p in capt["inserts"])
        self.assertEqual(slugs, sorted(["ot_reagendada_tecnico", "ot_cancelada_tecnico", "ot_desasignada_tecnico",
                                        "agenda_tecnico_manana"]))
        for sql, p in capt["inserts"]:
            self.assertIn("INSERT IGNORE INTO comm_templates", sql)
            self.assertEqual(p[0], "mantenciones")
            self.assertEqual((p[2], p[3]), O.SEMILLAS[p[1]])

    def test_lo_que_ya_existe_o_editaste_no_se_pisa(self):
        filas = [(s, "asunto editado", "<p>cuerpo editado por Daniel</p>") for s in O.SEMILLAS_NUEVAS]
        filas.append(("ot_asignada", "Te asignamos la {{ot}}", "<p>editada por Daniel</p>"))
        amb, capt = self.mundo(filas)
        self.assertEqual(amb["_ensure_comm_template_ot_tecnicos"](), 0)
        self.assertEqual(capt["inserts"], [])
        self.assertEqual(capt["updates"], [])                    # ni siquiera 'ot_asignada' editada

    def test_ot_asignada_sin_editar_pasa_a_la_version_con_direccion(self):
        amb, capt = self.mundo([])
        a1, c1 = amb["_mantenciones_tpl_seed"]()["ot_asignada"]
        self.assertNotIn("{{direccion}}", c1)                    # la original no traía dirección
        amb2, capt2 = self.mundo([("ot_asignada", a1, c1.replace("\n", "\r\n"))])
        amb2["_ensure_comm_template_ot_tecnicos"]()
        self.assertEqual(len(capt2["updates"]), 1)
        sql, p = capt2["updates"][0]
        self.assertIn("WHERE id=%s AND cuerpo=%s", sql)          # a prueba de carreras: solo si sigue igual
        self.assertEqual((p[0], p[1]), O.SEMILLAS["ot_asignada"])
        self.assertIn("{{direccion}}", p[1])
        self.assertIn("{{notas_html}}", p[1])
        self.assertIn("{{link_ot}}", p[1])

    def test_la_semilla_ot_asignada_conserva_lo_que_ya_tenia(self):
        amb, _c = self.mundo([])
        _a1, c1 = amb["_mantenciones_tpl_seed"]()["ot_asignada"]
        for token in re.findall(r"\{\{(\w+)\}\}", c1):
            if token == "tecnico":
                token = "nombre_tecnico"                         # el saludo ahora es personal
            self.assertIn("{{" + token + "}}", O.SEMILLAS["ot_asignada"][1], token)


class TestDemoDelEditor(unittest.TestCase):
    def test_el_envio_de_prueba_del_editor_conoce_las_variables_nuevas(self):
        s = _fuente("_comm_demo_replacements")
        for v in ("nombre_tecnico", "antes_ahora_html", "agenda_html", "notas_html", "motivo_html", "n_ot_txt", "link_agenda"):
            self.assertIn(f'"{v}"', s)

    def test_el_editor_lista_las_cinco_plantillas_y_sus_variables(self):
        with open(os.path.join(RAIZ, "static", "comunicaciones_index.js"), encoding="utf-8") as f:
            js = f.read()
        for key in ("ot_asignada", "ot_reagendada_tecnico", "ot_cancelada_tecnico", "ot_desasignada_tecnico",
                    "agenda_tecnico_manana"):
            self.assertRegex(js, r"\{ key:'%s',[^}]*grupo:'ot_tecnico'" % key)
        bloque = js[js.index("ot_tecnico: ["):]
        bloque = bloque[:bloque.index("],")]
        for nombre, _d in O.VARIABLES_DOC:
            self.assertIn("{{%s}}" % nombre, bloque, nombre)
        self.assertIn("ot_tecnico: { label:'Avisos al técnico (OT)'", js)


# ═════════════════════════ revisión adversarial del 19-oct (commit 6839c431) ═════════════════════════
class TestFiltroDePlataAmpliado(unittest.TestCase):
    """Hallazgo: el filtro dejaba pasar deuda, saldo, valor, tarifa, abono, montos sin «$»... (REGLA #26)."""
    MALOS = [
        "Cliente con deuda, se reagenda hasta que pague", "Tiene saldo pendiente de 120.000", "Valor del servicio 85.000",
        "Se paga contra entrega", "El cliente debe plata", "Cliente moroso", "Esperan transferencia", "Abono del 50%",
        "Tarifa especial", "Descuento 10%", "Cliente con deuda pendiente", "Valor visita 80.000",
        "saldo pendiente 300 mil", "Tarifa UF 3", "costó 50 lucas", "pagaron la mitad", "tres palos", "2 millones",
        "nota de crédito pendiente", "Ver NVI 687", "FCV 1234", "o/c 33", "2,500 por unidad", "Honorarios del técnico",
        "Cobro adicional", "Anticipo recibido", "Sin presupuesto", "Cotización aprobada", "dólares", "UF 4,5",
    ]
    BUENOS = [
        "Llevar escalera. Revisar cinta, correa y motor.", "Ingreso por la portería lateral", "Tiempo estimado 2 horas",
        "Ver la página de soporte", "Preguntar por Juan, piso 3", "Llevar 4 tornillos de 20 mm", "Av. Apoquindo 4500",
        "Instalar la trotadora en el segundo piso", "El cliente pidió cambiar el día", "El gimnasio abre a las 7:00",
        "Coordinar con el administrador del edificio", "Revisar el contrafuerte de la cinta",
    ]

    def test_ya_no_deja_pasar_deudas_saldos_ni_montos_sin_signo(self):
        for t in self.MALOS:
            self.assertTrue(O.hay_plata(t), t)
            self.assertEqual(O.limpiar_texto_tecnico(t), "", t)
            self.assertEqual(O.fila_notas(t), "", t)
            self.assertEqual(O.fila_motivo(t), "", t)

    def test_no_se_come_las_instrucciones_normales(self):
        for t in self.BUENOS:
            self.assertFalse(O.hay_plata(t), t)
            self.assertNotEqual(O.limpiar_texto_tecnico(t), "", t)

    def test_una_sola_mencion_no_se_lleva_la_instruccion_entera(self):
        v = O.construir_variables(dict(OT, descripcion="Instalar la trotadora. Cliente con deuda, se reagenda. Llevar escalera."),
                                  nombre_tecnico="Ana", motivo="Cliente moroso. Pidió otro día")
        self.assertIn("Instalar la trotadora", v["notas_html"])
        self.assertIn("Llevar escalera", v["notas_html"])
        self.assertNotIn("deuda", v["notas_html"])
        self.assertIn("Pidió otro día", v["motivo_html"])
        self.assertNotIn("moroso", v["motivo_html"])

    def test_cubre_todo_lo_que_ya_veda_la_campana_del_tecnico(self):
        """Una sola lista de verdad: lo que la campana no deja pasar tampoco sale por correo (salvo dos términos que se
        leerían como instrucciones normales: «ingreso» —acceso por portería— y «estimado» —tiempo estimado—)."""
        amb = {"re": re}
        _cargar(amb, asignaciones=("_NOTIF_TEC_RE_VEDADO",))
        terminos = amb["_NOTIF_TEC_RE_VEDADO"].pattern.split("|")
        self.assertGreater(len(terminos), 15)
        excepciones = {"estimado", "ingres"}
        for t in terminos:
            muestra = t.replace("\\b", "").replace("\\", "")
            if muestra in excepciones:
                self.assertFalse(O.hay_plata(f"Nota: {muestra}"), muestra)
                continue
            self.assertTrue(O.hay_plata(f"Nota: {muestra}"), f"el término de la campana «{t}» no lo cubre el correo")

    def test_en_los_correos_completos_no_llega_nada_de_esto(self):
        m = Mundo()
        m.ot(descripcion="Instalar la trotadora. Cliente con deuda, se reagenda hasta que pague. Valor del servicio 85.000. "
                         "Se paga contra entrega. Llevar escalera.")
        m.amb["_ot_tec_correo"](301, antes=None)
        antes = m.cambia(301, fecha=F(2026, 10, 30))
        m.amb["_ot_tec_correo"](301, antes, motivo="Cliente moroso; esperan transferencia. Pidió otro día")
        antes = m.cambia(301, estado="cancelada")
        m.amb["_ot_tec_correo"](301, antes, motivo="Abono del 50% pendiente")
        self.assertEqual(len(m.outbox), 3)
        for o in m.outbox:
            visible = O.texto_sin_marcas(o["html"] + " " + o["asunto"])
            self.assertFalse(O.hay_plata(visible), (o["evento"], visible))
            for prohibido in ("deuda", "moroso", "85.000", "contra entrega", "transferencia", "Abono"):
                self.assertNotIn(prohibido, visible, o["evento"])
        self.assertIn("Llevar escalera", m.outbox[0]["html"])
        self.assertIn("Pidió otro día", m.outbox[1]["html"])


class TestClaveAntiDuplado(unittest.TestCase):
    """Hallazgo: A→B→A en el mismo minuto dejaba a A con «ya no está a tu cargo» teniendo la OT."""

    def test_la_clave_distingue_como_estaba_la_ot_antes(self):
        d = foto(tec={"a@sphs.cl": "Ana"})
        h = lambda ev, antes: O.huella(ev, "a@sphs.cl", antes, d)
        self.assertNotEqual(h("asignada", None), h("asignada", foto(tec={"b@sphs.cl": "Beto"})))
        self.assertNotEqual(h("asignada", foto(tec={"b@sphs.cl": "Beto"})), h("asignada", foto(estado="cancelada")))
        self.assertEqual(h("asignada", None), h("asignada", None))
        # y la lista de técnicos cuenta aunque cambie el orden en que se escribió
        self.assertEqual(O.huella("cancelada", "a@sphs.cl", foto(tec={"a@sphs.cl": "A", "b@sphs.cl": "B"}), d),
                         O.huella("cancelada", "a@sphs.cl", foto(tec={"b@sphs.cl": "B", "a@sphs.cl": "A"}), d))

    def test_devolver_la_ot_al_primer_tecnico_en_el_mismo_minuto_vuelve_a_avisarle(self):
        m = Mundo()
        m.ot(tec={"ana@sphs.cl": "Ana Pérez"})
        m.amb["_ot_tec_correo"](301, antes=None)                                   # Ana: asignada
        antes = m.cambia(301, tec={"beto@sphs.cl": "Beto Soto"})
        m.amb["_ot_tec_correo"](301, antes)                                        # Ana: ya no la tienes · Beto: asignada
        antes = m.cambia(301, tec={"ana@sphs.cl": "Ana Pérez"})
        m.amb["_ot_tec_correo"](301, antes)                                        # Ana: asignada DE NUEVO (mismo minuto)
        ana = [o["evento"] for o in m.outbox if o["to"] == "ana@sphs.cl"]
        self.assertEqual(ana, ["ot_asignada", "ot_desasignada_tecnico", "ot_asignada"])
        self.assertEqual([o["evento"] for o in m.outbox if o["to"] == "beto@sphs.cl"],
                         ["ot_asignada", "ot_desasignada_tecnico"])

    def test_agregar_quitar_y_volver_a_agregar_un_colaborador(self):
        m = Mundo()
        m.ot(tec={"ana@sphs.cl": "Ana Pérez"})
        antes = m.cambia(301, tec={"ana@sphs.cl": "Ana Pérez", "beto@sphs.cl": "Beto Soto"})
        m.amb["_ot_tec_correo"](301, antes)
        antes = m.cambia(301, tec={"ana@sphs.cl": "Ana Pérez"})
        m.amb["_ot_tec_correo"](301, antes)
        antes = m.cambia(301, tec={"ana@sphs.cl": "Ana Pérez", "beto@sphs.cl": "Beto Soto"})
        m.amb["_ot_tec_correo"](301, antes)
        self.assertEqual([o["evento"] for o in m.outbox if o["to"] == "beto@sphs.cl"],
                         ["ot_asignada", "ot_desasignada_tecnico", "ot_asignada"])

    def test_cambiar_la_fecha_de_x_a_y_a_x_y_otra_vez_a_y_deja_el_ultimo_aviso_correcto(self):
        """Aun con el «antes» en la clave, X→Y repetido es el MISMO cambio: el último aviso gana (el anterior distinto
        libera la reserva), así el técnico queda con el correo que dice la fecha vigente."""
        m = Mundo()
        m.ot(fecha=F(2026, 10, 26))
        for fecha in (F(2026, 10, 27), F(2026, 10, 26), F(2026, 10, 27)):
            antes = m.cambia(301, fecha=fecha)
            m.amb["_ot_tec_correo"](301, antes)
        self.assertEqual(len(m.outbox), 3)
        self.assertIn("27/10/2026", m.outbox[-1]["html"])
        self.assertIn("ahora 27/10/2026", m.outbox[-1]["asunto"])

    def test_el_doble_clic_sigue_sin_duplicar(self):
        m = Mundo()
        m.ot()
        antes = m.cambia(301, fecha=F(2026, 10, 28))
        m.amb["_ot_tec_correo"](301, antes)
        m.amb["_ot_tec_correo"](301, antes)
        m.amb["_ot_tec_correo"](301, antes)
        self.assertEqual(len(m.outbox), 1)


class TestHoraTopeDelRecordatorio(unittest.TestCase):
    def test_solo_entre_las_17_y_las_21(self):
        q = lambda h, mi=0: O.dia_agenda_a_avisar(dt.datetime(2026, 10, 20, h, mi), es_dia_habil)
        self.assertIsNone(q(16, 59))
        self.assertEqual(q(17, 0), F(2026, 10, 21))
        self.assertEqual(q(20, 59), F(2026, 10, 21))
        self.assertIsNone(q(21, 0))
        self.assertIsNone(q(23, 30))
        self.assertEqual(O.HORA_AGENDA_TOPE, 21)

    def test_si_la_llave_se_abre_de_noche_no_sale_la_agenda(self):
        m = Mundo()
        m.ot(fecha=F(2026, 10, 26))
        m.ahora = dt.datetime(2026, 10, 23, 22, 40)
        r = m.amb["_ot_tec_barrido_agenda"]()
        self.assertEqual(m.outbox, [])
        self.assertIn("fuera de ventana", r["motivo"])
        self.assertIn("21:00", r["motivo"])


class TestAgendaDeFinDeSemanaYFeriados(unittest.TestCase):
    """Hallazgo: una OT agendada en sábado o en un feriado nunca aparecía en ninguna agenda."""

    def setUp(self):
        self.m = Mundo()           # viernes 23-oct, 17:05

    def barrido(self, **kw):
        return self.m.amb["_ot_tec_barrido_agenda"](**kw)

    def test_los_dias_que_cubre_el_correo(self):
        self.assertEqual(O.dias_agenda_a_avisar(dt.datetime(2026, 10, 20, 17, 0), es_dia_habil), [F(2026, 10, 21)])
        self.assertEqual(O.dias_agenda_a_avisar(dt.datetime(2026, 10, 23, 17, 5), es_dia_habil),
                         [F(2026, 10, 24), F(2026, 10, 25), F(2026, 10, 26)])
        self.assertEqual(O.dias_agenda_a_avisar(dt.datetime(2026, 10, 9, 17, 30), es_dia_habil),
                         [F(2026, 10, 10), F(2026, 10, 11), F(2026, 10, 12), F(2026, 10, 13)])
        self.assertEqual(O.dias_agenda_a_avisar(dt.datetime(2026, 10, 24, 18, 0), es_dia_habil), [])      # sábado: nada
        self.assertEqual(O.dias_agenda_a_avisar(dt.datetime(2026, 10, 20, 21, 5), es_dia_habil), [])      # de noche: nada

    def test_la_ot_del_sabado_sale_en_el_correo_del_viernes_con_su_encabezado(self):
        self.m.ot(401, fecha=F(2026, 10, 24), hi="09:00", hf="12:00", tec={"ana@sphs.cl": "Ana Pérez"})      # sábado
        self.m.ot(402, fecha=F(2026, 10, 26), hi="08:00", hf="10:00", tec={"ana@sphs.cl": "Ana Pérez"})      # lunes
        self.m.ot(403, fecha=F(2026, 10, 26), tec={"beto@sphs.cl": "Beto Soto"})                             # solo lunes
        r = self.barrido()
        self.assertEqual(r["agenda_de"], "26/10/2026")
        self.assertEqual(r["dias"], ["24/10/2026", "25/10/2026", "26/10/2026"])
        self.assertEqual(sorted(o["to"] for o in self.m.outbox), ["ana@sphs.cl", "beto@sphs.cl"])      # UN correo por técnico
        ana = next(o for o in self.m.outbox if o["to"] == "ana@sphs.cl")
        self.assertIn("del sábado al lunes", ana["asunto"])
        self.assertIn("2 órdenes de trabajo", ana["asunto"])
        self.assertIn("sábado 24/10/2026", ana["html"])
        self.assertIn("lunes 26/10/2026", ana["html"])
        self.assertLess(ana["html"].index("OT-2026-00401"), ana["html"].index("OT-2026-00402"))
        self.assertFalse(O.hay_plata(O.texto_sin_marcas(ana["html"])))
        beto = next(o for o in self.m.outbox if o["to"] == "beto@sphs.cl")        # un solo día con OT: el correo de siempre
        self.assertIn("del lunes", beto["asunto"])
        self.assertNotIn("text-transform:uppercase", beto["html"])               # sin encabezados de día

    def test_la_ot_del_feriado_intermedio_sale_junto_con_la_del_siguiente_dia_habil(self):
        self.m.ot(404, fecha=F(2026, 10, 12), hi="10:00", hf="12:00", tec={"eva@sphs.cl": "Eva Rojas"})      # feriado lunes
        self.m.ot(405, fecha=F(2026, 10, 13), tec={"eva@sphs.cl": "Eva Rojas"})                              # martes
        self.m.ahora = dt.datetime(2026, 10, 9, 17, 30)
        self.barrido()
        self.assertEqual([o["to"] for o in self.m.outbox], ["eva@sphs.cl"])
        h = self.m.outbox[0]["html"]
        self.assertIn("lunes 12/10/2026", h)
        self.assertIn("martes 13/10/2026", h)

    def test_una_ot_de_varios_dias_no_se_repite_en_cada_encabezado(self):
        self.m.ot(406, fecha=F(2026, 10, 22), fecha_fin=F(2026, 10, 28), tec={"fer@sphs.cl": "Fer"})        # jueves a miércoles
        self.m.ot(407, fecha=F(2026, 10, 26), tec={"fer@sphs.cl": "Fer"})
        self.barrido()
        h = self.m.outbox[0]["html"]
        self.assertEqual(h.count("OT-2026-00406"), 1)
        self.assertEqual(h.count("OT-2026-00407"), 1)
        self.assertEqual(self.m.outbox[0]["asunto"].count("2 órdenes de trabajo"), 1)

    def test_el_correo_es_uno_por_tecnico_y_por_noche(self):
        self.m.ot(401, fecha=F(2026, 10, 24), tec={"ana@sphs.cl": "Ana"})
        self.m.ot(402, fecha=F(2026, 10, 26), tec={"ana@sphs.cl": "Ana"})
        self.barrido()
        self.m.ahora = dt.datetime(2026, 10, 23, 17, 25)
        self.barrido()
        self.assertEqual(len(self.m.outbox), 1)
        self.assertIn(("ana@sphs.cl", F(2026, 10, 26)), self.m.agenda)          # la clave es el último día de la ventana

    def test_una_ot_que_espera_repuesto_o_informacion_sigue_en_la_agenda_con_su_estado(self):
        self.m.ot(408, estado="pendiente_repuesto", fecha=F(2026, 10, 26), tec={"ana@sphs.cl": "Ana"})
        self.m.ot(409, estado="pendiente_info", fecha=F(2026, 10, 26), tec={"ana@sphs.cl": "Ana"})
        self.m.ot(410, fecha=F(2026, 10, 26), tec={"ana@sphs.cl": "Ana"})
        self.barrido()
        h = self.m.outbox[0]["html"]
        self.assertIn("Estado: Falta repuesto", h)
        self.assertIn("Estado: Falta información", h)
        self.assertEqual(h.count("Estado:"), 2)                                   # la OT normal no lleva etiqueta


class TestReservaDeLaAgenda(unittest.TestCase):
    """Hallazgo: la agenda se marcaba «enviada» ANTES de mandar el correo: si algo fallaba o el trabajo se cortaba, el
    técnico se quedaba sin agenda ese día."""

    def setUp(self):
        self.m = Mundo()
        self.m.ot(301, fecha=F(2026, 10, 26), hi="09:00", hf="11:00", tec={"ana@sphs.cl": "Ana Pérez"})
        self.m.ot(302, fecha=F(2026, 10, 26), hi="10:00", hf="11:00", tec={"beto@sphs.cl": "Beto Soto"})

    def barrido(self, **kw):
        return self.m.amb["_ot_tec_barrido_agenda"](**kw)

    def test_confirma_el_envio_despues_de_mandar(self):
        self.barrido()
        self.assertEqual(self.m.agenda_pend, set())                 # nada queda "reservado"
        self.assertIn(("ana@sphs.cl", F(2026, 10, 26)), self.m.agenda)
        self.assertIn(("*", F(2026, 10, 26)), self.m.agenda)

    def test_si_falla_el_armado_se_libera_y_el_siguiente_ciclo_lo_manda(self):
        original = self.m.amb["_ot_tec_armar"]
        self.m.amb["_ot_tec_armar"] = lambda slug, v: (_ for _ in ()).throw(RuntimeError("plantilla rota"))
        r = self.barrido()
        self.assertEqual(sorted(r["pendientes"]), ["ana@sphs.cl", "beto@sphs.cl"])
        self.assertEqual(self.m.agenda, set())                      # sin reserva colgada ni día marcado como completo
        self.m.amb["_ot_tec_armar"] = original
        self.m.ahora = dt.datetime(2026, 10, 23, 17, 15)
        self.barrido()
        self.assertEqual(sorted(o["to"] for o in self.m.outbox), ["ana@sphs.cl", "beto@sphs.cl"])

    def test_una_reserva_a_medias_se_retoma_cuando_envejece(self):
        clave = ("ana@sphs.cl", F(2026, 10, 26))
        self.m.agenda.add(clave)
        self.m.agenda_pend.add(clave)                               # una corrida anterior se cortó con la reserva sin confirmar
        r = self.barrido()
        self.assertEqual([o["to"] for o in self.m.outbox], ["beto@sphs.cl"])      # reciente: otra corrida podría estar enviándola
        self.assertEqual(r["pendientes"], ["ana@sphs.cl"])
        self.assertNotIn(("*", F(2026, 10, 26)), self.m.agenda)     # el día NO se da por completo
        self.m.agenda_vieja.add(clave)                              # pasaron más de 15 minutos
        self.m.ahora = dt.datetime(2026, 10, 23, 17, 30)
        r2 = self.barrido()
        self.assertEqual(sorted(o["to"] for o in self.m.outbox), ["ana@sphs.cl", "beto@sphs.cl"])      # Beto no se repite
        self.assertEqual(r2["pendientes"], [])
        self.assertIn(("*", F(2026, 10, 26)), self.m.agenda)

    def test_con_poco_tiempo_deja_el_resto_para_el_siguiente_ciclo(self):
        self.m.avance_por_envio = 100                               # cada correo "tarda" 100 s
        r = self.barrido(max_s=60)
        self.assertEqual(len(self.m.outbox), 1)
        self.assertTrue(r["cortado"])
        self.assertNotIn(("*", F(2026, 10, 26)), self.m.agenda)
        self.m.ahora = dt.datetime(2026, 10, 23, 17, 15)
        self.barrido()
        self.assertEqual(sorted(o["to"] for o in self.m.outbox), ["ana@sphs.cl", "beto@sphs.cl"])

    def test_la_bitacora_va_en_un_solo_insert(self):
        self.m.ot(303, fecha=F(2026, 10, 26), tec={"ana@sphs.cl": "Ana Pérez"})
        self.barrido()
        self.assertEqual(self.m.inserts_log, 1)                      # 3 OT, 2 técnicos: UN INSERT con todas las filas
        self.assertEqual(sorted(l[1] for l in self.m.logs), [301, 302, 303])
        self.assertTrue(all(l[2] == "aviso_tecnico" for l in self.m.logs))

    def test_los_registros_no_imprimen_el_correo_del_tecnico(self):
        s = _fuente("_ot_tec_barrido_agenda")
        self.assertNotIn("reserva {email}", s)
        self.assertIn('print(f"[ot-tec-agenda] reserva: {type(e).__name__}: {e}"', s)

    def test_el_trabajo_periodico_devuelve_solo_un_resumen_y_corre_antes_del_libro(self):
        s = _fuente("tr_cron_simpliroute_poll")
        self.assertLess(s.index("_ot_tec_barrido_agenda("), s.index("_fac_libro_barrido("))
        self.assertIn("max_s=", s[s.index("_ot_tec_barrido_agenda("):s.index("_fac_libro_barrido(")])
        self.assertIn('len(_ag.get("enviados") or [])', s)
        self.assertNotIn('res["agenda_tecnicos"] = _ot_tec_barrido_agenda()', s)
        self.assertNotIn('"ots"', s[s.index("_ot_tec_barrido_agenda("):s.index("_fac_libro_barrido(")])


class TestNombresDeProveedoresYAnexo(unittest.TestCase):
    def test_tecnicos_nunca_incluye_la_razon_social_de_una_empresa_externa(self):
        m = Mundo()
        m.ot(tec={"ana@sphs.cl": "Ana Pérez", "pulgar@empresa.cl": "Pulgar SpA"})
        m.fichas[301] = {"pulgar@empresa.cl"}                       # ese correo sale de la FICHA del proveedor
        m.db_plantillas["ot_asignada"] = ("OT {{ot}}", "<p>{{nombre_tecnico}} :: {{tecnicos}}</p>")
        m.amb["_ot_tec_correo"](301, antes=None)
        ana = next(o for o in m.outbox if o["to"] == "ana@sphs.cl")
        self.assertIn("Ana Pérez :: Ana Pérez", ana["html"])
        self.assertNotIn("Pulgar", ana["html"])                      # REGLA #19: ni el nombre del proveedor
        pulgar = next(o for o in m.outbox if o["to"] == "pulgar@empresa.cl")
        self.assertIn("Pulgar SpA :: Ana Pérez", pulgar["html"])    # la empresa sí se saluda por su nombre

    def test_con_el_anexo_sin_firmar_el_correo_no_adelanta_direccion_ni_instrucciones(self):
        m = Mundo()
        m.ot(descripcion="Instalar la trotadora en el segundo piso")
        m.anexo[301] = "SIN_ANEXO"
        m.amb["_ot_tec_correo"](301, antes=None)
        h = m.outbox[0]["html"]
        self.assertNotIn("Av. Vicuña Mackenna", h)
        self.assertNotIn("Instalar la trotadora", h)
        self.assertIn(O.OCULTO_ANEXO, h)
        self.assertIn("26/10/2026", h)                               # la fecha y el horario sí
        self.assertIn("09:00 – 11:00", h)

    def test_sin_anexo_pendiente_el_correo_lleva_todo(self):
        m = Mundo()
        m.ot(descripcion="Instalar la trotadora en el segundo piso")
        m.amb["_ot_tec_correo"](301, antes=None)
        h = m.outbox[0]["html"]
        self.assertIn("Av. Vicuña Mackenna 1000", h)
        self.assertIn("Instalar la trotadora", h)
        self.assertNotIn(O.OCULTO_ANEXO, h)

    def test_la_agenda_tampoco_adelanta_la_direccion_de_una_ot_bloqueada(self):
        m = Mundo()
        m.ot(301, fecha=F(2026, 10, 26), tec={"ana@sphs.cl": "Ana"})
        m.ot(302, fecha=F(2026, 10, 26), tec={"ana@sphs.cl": "Ana"})
        m.anexo[302] = "SIN_ANEXO"
        m.amb["_ot_tec_barrido_agenda"]()
        h = m.outbox[0]["html"]
        self.assertEqual(h.count("Av. Vicuña Mackenna"), 1)          # solo la OT 301
        self.assertEqual(h.count(O.OCULTO_ANEXO), 1)

    def test_el_anexo_se_pregunta_a_la_fuente_unica_de_la_app(self):
        s = _fuente("_ot_tec_oculta_detalle")
        self.assertIn("_anexo_bloquea_ot(int(vid))", s)
        self.assertIn("except Exception:", s)                        # ante un error no se deja a nadie sin su aviso


class TestBorradoDeUnaOT(unittest.TestCase):
    """Hallazgo: borrar una OT ya anunciada no le avisaba al técnico y su botón llevaba a una página que no existe."""
    V_INFO = {"estado": "programada", "fecha_programada": F(2026, 10, 26), "fecha_fin": None}

    def setUp(self):
        self.m = Mundo()
        self.m.ot(tec={"ana@sphs.cl": "Ana Pérez", "beto@sphs.cl": "Beto Soto"})
        self.m.informados[301] = {"ana@sphs.cl"}                    # solo Ana recibió «asignada»

    def borrar(self, v_info=None):
        snap = self.m.amb["_ot_tec_snapshot_borrado"](301, v_info or self.V_INFO)
        self.m.fotos.pop(301, None)
        self.m.filas.pop(301, None)                                 # la OT ya no existe
        return snap, self.m.amb["_ot_tec_correo_borrada"](301, snap)

    def test_avisa_cancelada_solo_a_quien_ya_conocia_la_ot(self):
        snap, r = self.borrar()
        self.assertIsNotNone(snap)
        self.assertEqual(self.m.correos, [("ana@sphs.cl", "ot_cancelada_tecnico")])          # Beto nunca supo de ella
        o = self.m.outbox[0]
        self.assertEqual(o["modulo"], "ot_tecnicos")
        self.assertIn("no necesitas ir", o["asunto"])
        self.assertIn("se eliminó del sistema", o["html"])
        self.assertIn("26/10/2026", o["html"])
        self.assertNotIn("/ot/301", o["html"])                      # el botón lleva a la lista, no a una página que no existe
        self.assertIn("https://app.ilus.cl/ot/", o["html"])

    def test_si_nunca_se_le_aviso_a_nadie_no_sale_nada(self):
        self.m.informados[301] = set()
        snap, r = self.borrar()
        self.assertIsNone(snap)
        self.assertEqual(self.m.outbox, [])

    def test_no_avisa_si_ya_estaba_cancelada_terminada_o_la_fecha_paso(self):
        for est in ("cancelada", "anulada", "cerrada", "completada", "firmada_tecnico", "pendiente_aprobacion"):
            self.assertIsNone(self.m.amb["_ot_tec_snapshot_borrado"](301, dict(self.V_INFO, estado=est)), est)
        self.assertIsNone(self.m.amb["_ot_tec_snapshot_borrado"](301, dict(self.V_INFO, fecha_programada=F(2026, 10, 20))))
        # una OT de varios días que todavía no termina sí avisa
        self.assertIsNotNone(self.m.amb["_ot_tec_snapshot_borrado"](
            301, dict(self.V_INFO, fecha_programada=F(2026, 10, 20), fecha_fin=F(2026, 10, 27))))
        # sin fecha (por confirmar) también
        self.assertIsNotNone(self.m.amb["_ot_tec_snapshot_borrado"](301, dict(self.V_INFO, fecha_programada=None)))

    def test_respeta_la_llave_propia(self):
        self.m.cierra("ot_tecnicos", 1)
        self.borrar()
        self.assertEqual(self.m.outbox, [])

    def test_nunca_lanza(self):
        self.m.amb["_ot_tec_filas"] = lambda vids: (_ for _ in ()).throw(RuntimeError("base caída"))
        self.assertIsNone(self.m.amb["_ot_tec_snapshot_borrado"](301, self.V_INFO))
        self.assertEqual(self.m.amb["_ot_tec_correo_borrada"](301, {"antes": None})["enviados"], [])
        self.assertEqual(self.m.amb["_ot_tec_correo_borrada"](301, None)["enviados"], [])

    def test_el_nucleo_del_borrado_toma_la_foto_antes_y_avisa_despues_del_commit(self):
        s = _fuente("_mant_visita_eliminar_core")
        i_snap = s.index("_tec_snap = _ot_tec_snapshot_borrado(vid, v_info)")
        i_del = s.index('cur.execute("DELETE FROM mant_visitas WHERE id=%s", (vid,))')
        i_commit = s.index("conn.commit()", i_del)
        i_aviso = s.index("_ot_tec_correo_borrada(vid, _tec_snap)")
        self.assertLess(i_snap, i_del)
        self.assertLess(i_commit, i_aviso)
        self.assertIn("estado, fecha_fin", s[:i_snap])               # la consulta inicial trae lo que hace falta


class TestOtrosCaminosYTextos(unittest.TestCase):
    def test_cambiar_solo_el_texto_del_tecnico_tambien_avisa(self):
        s = _fuente("mant_visita_update")
        i = s.index("_tec_avisar = any(")
        self.assertIn('"tecnico_user_id", "tecnico", "estado"', s[i:i + 260])

    def test_el_nucleo_de_crear_avisa_aunque_el_tecnico_venga_solo_como_texto(self):
        s = _fuente("_mant_visita_crear_core")
        self.assertRegex(s, r"\n        try: _mant_ot_email_tecnico_creacion\(vid\)\n")          # fuera del «if tecnico_user_id»
        self.assertRegex(s, r"\n            try: _notificar_ot_asignada_interna\(vid, tecnico_user_id")   # lo suyo sigue adentro

    def test_la_solicitud_de_cambio_de_un_equipo_avisa_al_tecnico(self):
        s = _fuente("mant_maquina_solicitar_cambio")
        self.assertIn("_mant_ot_email_tecnico_creacion(vid)", s)
        self.assertLess(s.index("conn.commit()"), s.index("_mant_ot_email_tecnico_creacion(vid)"))

    def test_la_semilla_de_desasignada_ya_no_dice_que_se_asigno_a_otra_persona(self):
        cuerpo = O.SEMILLAS["ot_desasignada_tecnico"][1]
        self.assertNotIn("se asignó a otra persona", cuerpo)
        self.assertIn("no figuras como técnico", cuerpo)

    def test_las_ot_del_calendario_rapido_muestran_sus_equipos(self):
        lect = TestLecturas()

        def fetchall(sql, p=None):
            if "mant_visita_equipos" in sql:
                return [{"vid": 1, "nombre": "Trotadora"}]
            if "mant_visita_tareas" in sql:
                return [{"vid": 1, "nombre": "Trotadora"}, {"vid": 1, "nombre": "Bicicleta"}, {"vid": 2, "nombre": "Spin"}]
            return []
        a = lect.amb(fetchall)
        self.assertEqual(a["_ot_tec_equipos_bulk"]([1, 2, 3]), {1: ["Trotadora", "Bicicleta"], 2: ["Spin"], 3: []})

    def test_si_falla_una_de_las_dos_lecturas_de_equipos_sigue_la_otra(self):
        lect = TestLecturas()

        def fetchall(sql, p=None):
            if "mant_visita_equipos" in sql:
                raise RuntimeError("tabla aún no existe")
            return [{"vid": 1, "nombre": "Spin"}]
        self.assertEqual(lect.amb(fetchall)["_ot_tec_equipos_bulk"]([1]), {1: ["Spin"]})

    def test_la_busqueda_de_externos_por_nombre_solo_aplica_sin_tecnico_principal(self):
        """Hallazgo: «Reasignar» deja el texto `tecnico` con el nombre anterior; la empresa externa de antes seguía
        recibiendo reagendada, cancelada y su agenda de una OT que ya no es suya."""
        sqls = []
        lect = TestLecturas()
        a = lect.amb(lambda sql, p=None: (sqls.append(sql), [])[1])
        a["_ot_tec_destinatarios_bulk"]([1])
        por_nombre = next(s for s in sqls if "te.user_id IS NULL" in s)
        self.assertIn("v.tecnico_user_id IS NULL", por_nombre)

    def test_las_fichas_de_empresas_externas_se_marcan_aparte(self):
        def fetchall(sql, p=None):
            if "FROM mant_visitas v JOIN `app_users`" in sql:
                return [{"vid": 1, "uid": 10, "email": "ana@sphs.cl", "nombre": "Ana"}]
            if "FROM mant_tecnicos_externos te WHERE te.user_id IN" in sql:
                return [{"uid": 10, "email": "contacto@empresa.cl", "nombre": "Empresa SpA"}]
            return []
        a = TestLecturas().amb(fetchall)
        out, fichas = a["_ot_tec_destinatarios_bulk"]([1], con_fichas=True)
        self.assertEqual(out[1], {"ana@sphs.cl": "Ana", "contacto@empresa.cl": "Empresa SpA"})
        self.assertEqual(fichas[1], {"contacto@empresa.cl"})                  # solo la ficha, no la cuenta del técnico
        self.assertEqual(a["_ot_tec_destinatarios_bulk"]([1]), out)           # sin el parámetro, lo de siempre

    def test_la_foto_con_fichas_las_lista_aparte_y_no_cambia_la_comparacion(self):
        a = TestLecturas().amb(fetchone=lambda sql, p=None: {"estado": "programada", "fecha_programada": F(2026, 10, 26),
                                                             "fecha_fin": None, "hora_inicio": None, "hora_fin": None})
        a["_ot_tec_destinatarios_bulk"] = lambda vids, con_fichas=False: (
            ({7: {"ana@sphs.cl": "Ana"}}, {7: {"x@empresa.cl"}}) if con_fichas else {7: {"ana@sphs.cl": "Ana"}})
        self.assertEqual(a["_ot_tec_foto"](7, con_fichas=True)["fichas"], ["x@empresa.cl"])
        self.assertNotIn("fichas", a["_ot_tec_foto"](7))


class TestEnviarEmailDeLaVisita(unittest.TestCase):
    """Hallazgo: «Enviar email» de la visita le mandaba a cada técnico la copia del cliente por la llave 'mantenciones'; con
    la plantilla ausente el respaldo en HTML lleva «Costo estimado» y los repuestos con su costo."""

    def test_los_tecnicos_ya_no_van_en_el_envio_del_cliente(self):
        s = _fuente("mant_visita_enviar_email")
        self.assertNotIn("destinos.append(_te)", s)
        self.assertIn("destinos_tec.append(_te)", s)
        i_cli = s.index("res = _mant_send_email_multi(")
        i_tec = s.index("_ot_tec_enviar(_te, asunto, body_tec")
        self.assertLess(i_cli, i_tec)
        self.assertIn('evento="visita_agendada", emails=destinos)', s)       # el cliente: igual que siempre
        self.assertIn("_otc.cuerpo_sin_montos(body, quitar=(costo_html, reps_html), saludo=saludo)", s)
        self.assertIn('"tecnicos_enviados": tec_enviados', s)

    def test_la_copia_del_tecnico_sale_sin_costo_ni_repuestos_con_costo(self):
        costo_html = "<p style='font-size:14px'><strong>Costo estimado:</strong> $85.000 CLP</p>"
        reps_html = "<h3>Repuestos asociados</h3><ul><li>Correa × 2 — $30.000</li></ul>"
        saludo = "Estimado/a Gimnasio Fit SpA"
        body = (f"<p>{saludo},</p><table><tr><td>N° de Orden</td><td>OT-1</td></tr>"
                f"<tr><td>Fecha</td><td>26/10/2026</td></tr></table>{costo_html}{reps_html}<p>Si necesita reagendar...</p>")
        tec = O.cuerpo_sin_montos(body, quitar=(costo_html, reps_html), saludo=saludo)
        visible = O.texto_sin_marcas(tec)
        self.assertNotIn("$", tec)
        self.assertNotIn("85.000", tec)
        self.assertNotIn("Costo", tec)
        self.assertNotIn("Repuestos", tec)
        self.assertNotIn("Gimnasio Fit", tec)                                   # el saludo no es el del cliente
        self.assertIn("Hola equipo", tec)
        self.assertIn("26/10/2026", tec)
        self.assertFalse(O.hay_plata(visible), visible)
        # sin nada que quitar queda igual
        self.assertEqual(O.cuerpo_sin_montos("<p>x</p>", quitar=("", None)), "<p>x</p>")

    def test_el_envio_de_la_copia_usa_la_llave_propia(self):
        self.assertIn("modulo=_otc.MODULO_LLAVE", _fuente("_ot_tec_enviar"))


if __name__ == "__main__":
    unittest.main()
