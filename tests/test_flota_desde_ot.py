# -*- coding: utf-8 -*-
"""🚚 FLOTA · reservar el furgón o el camión DESDE «Nueva OT» (Daniel, 04-nov-2026):
«cuando se seleccionen los técnicos internos al crear una OT nueva, que pueda dejar agendada la solicitud del camión y/o el furgón, para
que sea más automático con nosotros».

Decisiones que se vigilan:
  1. El asistente solo LEE la disponibilidad (GET /ot/api/flota/disponibilidad): vehículos libres o con quién los tiene, y qué técnicos pueden
     manejar cada uno (enrolado como chofer, habilitado, licencia de la clase que exige el vehículo y vigente, sin otra reserva a esa hora).
  2. La reserva real ocurre al crear la OT y pasa por `_reservar`: el MISMO sello y las MISMAS reglas que el formulario de Solicitudes de vehículo.
     Queda «Por confirmar», ligada a la OT (ot_id, ot_numero), con quien pidió la OT como solicitante, y NO manda ningún correo.
  3. Un chofer sin usuario de ILUS no reserva (la regla del 03-nov sigue valiendo desde la OT).
  4. Si el vehículo ya no está libre al crear la OT, la OT queda creada igual y el aviso dice cómo seguir (alertar, no trabar).
  5. El paso «Vehículo» del asistente es opcional y solo existe con un técnico INTERNO marcado; lo elegido es lo que se reserva (REGLA #32).
Correr:  python -m pytest tests/test_flota_desde_ot.py -q -p no:warnings
"""
import datetime as dt
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from flask import g  # noqa: E402

import flota_module  # noqa: E402
from test_flota_solicitudes import (  # noqa: E402
    DIA, DIA_ISO, FUTURO, SUPER, Base, Falsa, _app, _datos_chofer, _leer, _llave_cualquier_dia)

NUM_OT = "OT-2026-00077"


class BaseOT(Base):
    """Dos vehículos (furgón, camión) y dos choferes enrolados (Ana = usuario 1, Beto = usuario 2). Dani (usuario 4) NO está enrolado.
    Las funciones de flota que se llaman sin pasar por una ruta (reservar, sincronizar) corren DENTRO de una petición de un superadministrador:
    desde la revisión escéptica del 04-nov reservar exige el permiso «Solicitar vehículos», como Solicitudes de vehículo."""

    def setUp(self):
        super().setUp()
        self._peticion = self.app.test_request_context("/")
        self._peticion.push()
        self.addCleanup(self._peticion.pop)
        g.user = {"id": 9, "nombre": "Aarón Pérez"}
        g.permissions = {"superadmin": True}

    def reservar(self, **k):
        d = dict(ot_id=77, numero_ot=NUM_OT, titulo_ot="Instalación Gimnasio Norte", fecha=DIA_ISO, desde="08:00", hasta="12:00",
                 vehiculo_id=self.furgon, chofer_id=self.ana, solicitante=(9, "Aarón Pérez"))
        d.update(k)
        return flota_module.OT_API["reservar"](d["ot_id"], d["numero_ot"], d["titulo_ot"], d["fecha"], d["desde"], d["hasta"],
                                               d["vehiculo_id"], d["chofer_id"], d["solicitante"])

    def disponibilidad(self, tecnicos="1,2,4", desde="08:00", hasta="12:00", fecha=None):
        r = self.c.get(f"/ot/api/flota/disponibilidad?fecha={fecha or DIA_ISO}&desde={desde}&hasta={hasta}&tecnicos={tecnicos}", headers=SUPER)
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        return r.get_json()


class TestDisponibilidad(BaseOT):
    def test_dos_vehiculos_libres_y_choferes_enrolados(self):
        j = self.disponibilidad()
        self.assertTrue(j["ok"] and j["horario_ok"])
        self.assertEqual({v["tipo"] for v in j["vehiculos"]}, {"Furgón", "Camión"})
        self.assertTrue(all(v["libre"] for v in j["vehiculos"]))
        por_uid = {c["usuario_id"]: c for c in j["choferes"]}
        self.assertTrue(por_uid[1]["enrolado"] and por_uid[2]["enrolado"])
        self.assertTrue(por_uid[1]["por_vehiculo"][str(self.furgon)]["ok"])

    def test_un_tecnico_sin_enrolar_no_puede_manejar_y_el_motivo_se_dice(self):
        c = {x["usuario_id"]: x for x in self.disponibilidad()["choferes"]}[4]
        self.assertFalse(c["enrolado"])
        self.assertIsNone(c["chofer_id"])
        self.assertIn("No está enrolado como chofer", c["motivo"])

    def test_un_vehiculo_reservado_aparece_ocupado_con_quien_lo_tiene(self):
        r = self.reservar()
        self.assertTrue(r["ok"], r)
        j = self.disponibilidad()
        furgon = next(v for v in j["vehiculos"] if v["id"] == self.furgon)
        self.assertFalse(furgon["libre"])
        self.assertEqual(furgon["choque"]["numero"], r["solicitud"]["numero"])
        self.assertIn("Ya está reservado", furgon["motivo"])
        self.assertTrue(next(v for v in j["vehiculos"] if v["id"] == self.camion)["libre"])      # el otro sigue libre

    def test_el_chofer_ocupado_no_puede_manejar_otro_vehiculo_a_la_misma_hora(self):
        self.reservar()                                                       # Ana maneja el furgón de 08 a 12
        ana = {x["usuario_id"]: x for x in self.disponibilidad()["choferes"]}[1]
        x = ana["por_vehiculo"][str(self.camion)]
        self.assertFalse(x["ok"])
        self.assertIn("ya tiene otra reserva", x["error"])
        beto = {x["usuario_id"]: x for x in self.disponibilidad()["choferes"]}[2]
        self.assertTrue(beto["por_vehiculo"][str(self.camion)]["ok"])

    def test_terminar_justo_cuando_empieza_otra_no_ocupa(self):
        self.reservar(desde="08:00", hasta="12:00")
        j = self.disponibilidad(desde="12:00", hasta="15:00")
        self.assertTrue(all(v["libre"] for v in j["vehiculos"]))

    def test_licencia_vencida_antes_de_terminar_el_uso_no_puede_manejar(self):
        ayer = (dt.date.today() - dt.timedelta(days=1)).isoformat()
        self.chofer(4, licencia_vence=ayer)
        c = {x["usuario_id"]: x for x in self.disponibilidad()["choferes"]}[4]
        self.assertTrue(c["enrolado"])
        self.assertFalse(c["por_vehiculo"][str(self.furgon)]["ok"])
        self.assertIn("licencia", c["por_vehiculo"][str(self.furgon)]["error"].lower())

    def test_sin_hora_todavia_no_se_inventa_disponibilidad(self):
        j = self.disponibilidad(desde="", hasta="")
        self.assertFalse(j["horario_ok"])
        self.assertTrue(all(v["choque"] is None for v in j["vehiculos"]))

    def test_un_vehiculo_inhabilitado_no_se_ofrece(self):
        self.base.veh[self.camion]["habilitado"] = 0
        self.base.veh[self.camion]["inhabilitado_motivo"] = "En el taller"
        camion = next(v for v in self.disponibilidad()["vehiculos"] if v["id"] == self.camion)
        self.assertFalse(camion["libre"])
        self.assertIn("En el taller", camion["motivo"])

    def test_solo_lee_no_escribe_nada(self):
        antes = len(self.base.sol)
        self.disponibilidad()
        self.assertEqual(len(self.base.sol), antes)

    def test_sin_sesion_no_entra(self):
        r = self.c.get(f"/ot/api/flota/disponibilidad?fecha={DIA_ISO}&desde=08:00&hasta=12:00&tecnicos=1")
        self.assertNotEqual(r.status_code, 200)


class TestReservaDesdeOT(BaseOT):
    def test_queda_por_confirmar_ligada_a_la_ot_y_con_quien_pidio_la_ot(self):
        r = self.reservar()
        self.assertTrue(r["ok"], r)
        s = r["solicitud"]
        self.assertEqual(s["estado"], "solicitada")
        self.assertTrue(s["numero"].startswith("VEH-"))
        fila = self.base.sol[s["id"]]
        self.assertEqual((fila["ot_id"], fila["ot_numero"]), (77, NUM_OT))
        self.assertEqual((fila["solicitante_id"], fila["solicitante_nombre"]), (9, "Aarón Pérez"))
        self.assertEqual(fila["area"], "Servicio Técnico")
        self.assertIn(NUM_OT, fila["motivo"])
        self.assertIn("Instalación Gimnasio Norte", fila["motivo"])

    def test_la_bitacora_de_la_reserva_dice_de_que_ot_viene(self):
        sid = self.reservar()["solicitud"]["id"]
        creada = next(x for x in self.base.slog if x["solicitud_id"] == sid and x["accion"] == "creada")
        self.assertEqual(json.loads(creada["detalle"])["ot"], NUM_OT)

    def test_no_manda_ningun_correo(self):
        aviso = mock.Mock(return_value=1)
        self.app = _app(self.base, extra={"_aviso_interno_correo": aviso})
        self.c = self.app.test_client()
        self.assertTrue(self.reservar()["ok"])
        aviso.assert_not_called()

    def test_el_sello_decide_el_segundo_en_pedir_el_mismo_vehiculo_no_pasa(self):
        self.assertTrue(self.reservar()["ok"])
        r2 = self.reservar(ot_id=78, numero_ot="OT-2026-00078", chofer_id=self.beto)
        self.assertFalse(r2["ok"])
        self.assertEqual(r2["codigo"], "CHOQUE_VEHICULO")
        self.assertIn("ya está reservado", r2["error"])
        self.assertEqual(len(self.base.sol), 1)                      # nada se guardó

    def test_un_chofer_sin_usuario_de_ilus_no_reserva(self):
        self.base.cho[self.ana]["usuario_id"] = None                 # un chofer externo antiguo
        r = self.reservar()
        self.assertFalse(r["ok"])
        self.assertEqual(r["codigo"], "CHOFER_SIN_USUARIO")
        self.assertEqual(len(self.base.sol), 0)

    def test_hora_de_regreso_antes_de_la_salida_se_rechaza_sin_guardar(self):
        r = self.reservar(desde="12:00", hasta="08:00")
        self.assertFalse(r["ok"])
        self.assertEqual(len(self.base.sol), 0)

    def test_la_reserva_de_una_ot_no_exige_las_24_horas_de_anticipacion(self):
        d, errores = flota_module.fr.validar_solicitud(
            {"vehiculo_id": 1, "chofer_id": 1, "area": "Servicio Técnico", "motivo": "Para la OT", "fecha": DIA_ISO,
             "hora_desde": "08:00", "hora_hasta": "12:00"}, dt.datetime(2026, 11, 3, 18, 0), antelacion_horas=None)
        self.assertEqual(errores, {})
        self.assertIsNotNone(d["inicio"])

    def test_la_ficha_de_la_ot_encuentra_su_reserva(self):
        self.reservar()
        lista = flota_module.OT_API["de_orden"](77)
        self.assertEqual(len(lista), 1)
        self.assertEqual(lista[0]["estado_texto"], "Por confirmar")
        self.assertTrue(lista[0]["enlace"].startswith("/flota?sol="))
        self.assertIn("Furgón", lista[0]["vehiculo"])
        self.assertEqual(flota_module.OT_API["de_orden"](999), [])

    def test_la_lista_de_solicitudes_dice_para_que_ot_es(self):
        sid = self.reservar()["solicitud"]["id"]
        j = self.c.get(f"/flota/api/solicitudes/{sid}", headers=SUPER).get_json()
        self.assertEqual(j["solicitud"]["ot"], {"id": 77, "numero": NUM_OT})
        r, j2 = self.pedir(vehiculo_id=self.camion, chofer_id=self.beto)         # una reserva del menú no trae OT
        self.assertIsNone(j2["solicitud"]["ot"])

    def test_el_resto_de_las_reservas_del_menu_siguen_igual(self):
        _llave_cualquier_dia(self)
        r, j = self.pedir()
        self.assertEqual(r.status_code, 200, j)
        fila = self.base.sol[j["solicitud"]["id"]]
        self.assertIsNone(fila["ot_id"])
        self.assertIsNone(fila["ot_numero"])


# ═══════════════════════════════════════════════════════════════════════════
#  El enganche en app.py (se prueba la función real, sin cargar los 170 mil renglones de app.py)
# ═══════════════════════════════════════════════════════════════════════════
def _fuente_funcion(texto, nombre):
    m = re.search(r"^def " + re.escape(nombre) + r"\(.*?(?=^def |^@app\.route|^class )", texto, re.S | re.M)
    assert m, f"no encontré {nombre} en app.py"
    return m.group(0)


class TestEnganchEnAppPy(BaseOT):
    def setUp(self):
        super().setUp()
        self.app_py = _leer("app.py")
        self.bitacora = []
        self.ns = {"mysql_fetchone": self.base.fetchone, "_mant_log": lambda *a, **k: self.bitacora.append(a)}
        exec(_fuente_funcion(self.app_py, "_ot_vehiculo_reservar"), self.ns)           # noqa: S102 (código del propio proyecto)
        self.f = self.ns["_ot_vehiculo_reservar"]

    def pedir_ot(self, req=None, tec_ids=(1, 2), autorizacion=None):
        if req is None:
            req = {"vehiculo_id": self.furgon, "chofer_id": self.ana, "hora_desde": "08:00", "hora_hasta": "12:00"}
        return self.f(77, NUM_OT, "Instalación Gimnasio Norte", DIA_ISO, req, list(tec_ids), autorizacion)

    def test_reserva_y_deja_constancia_en_la_bitacora_de_la_ot(self):
        r = self.pedir_ot()
        self.assertTrue(r["ok"], r)
        self.assertEqual(self.bitacora[-1][:3], ("visita", 77, "vehiculo_solicitado"))
        self.assertIn(r["solicitud"]["numero"], self.bitacora[-1][3])

    def test_quien_maneja_tiene_que_ser_un_tecnico_de_esa_ot(self):
        r = self.pedir_ot(tec_ids=(2,))                          # Ana (usuario 1) no es técnico de esta OT
        self.assertFalse(r["ok"])
        self.assertIn("uno de los técnicos de esta OT", r["error"])
        self.assertEqual(len(self.base.sol), 0)

    def test_si_el_vehiculo_ya_no_esta_libre_la_ot_queda_y_el_aviso_dice_como_seguir(self):
        flota_module.OT_API["reservar"](50, "OT-2026-00050", "Otra", DIA_ISO, "08:00", "12:00", self.furgon, self.beto, (9, "Aarón"))
        r = self.pedir_ot()
        self.assertFalse(r["ok"])
        self.assertIn("La OT quedó creada", r["error"])
        self.assertIn("Solicitudes de vehículo", r["error"])
        self.assertEqual([x for x in self.bitacora if x[2] == "vehiculo_solicitado"], [])

    def test_en_una_ot_aprobada_a_distancia_pide_quien_la_pidio_no_quien_la_aprobo(self):
        r = self.pedir_ot(autorizacion={"solicitado_por_user_id": 9, "solicitado_por_nombre": "Víctor Rojas"})
        self.assertTrue(r["ok"], r)
        fila = self.base.sol[r["solicitud"]["id"]]
        self.assertEqual((fila["solicitante_id"], fila["solicitante_nombre"]), (9, "Víctor Rojas"))

    def test_datos_malos_del_navegador_no_rompen_la_creacion(self):
        for req in ({"vehiculo_id": "x", "chofer_id": None}, {"vehiculo_id": self.furgon}, {}):
            r = self.pedir_ot(req=req)
            self.assertFalse(r["ok"])
            self.assertIn("La OT quedó creada", r["error"])

    def test_si_el_modulo_de_flota_no_cargo_la_ot_se_crea_igual(self):
        saved = dict(flota_module.OT_API)
        try:
            flota_module.OT_API.clear()
            r = self.pedir_ot()
        finally:
            flota_module.OT_API.update(saved)
        self.assertFalse(r["ok"])
        self.assertIn("La OT quedó creada", r["error"])

    def test_cableado_del_nucleo_de_creacion(self):
        nucleo = _fuente_funcion(self.app_py, "_ot2_crear_core")
        # La llamada EXACTA: cualquier argumento cambiado (la fecha, los técnicos, la autorización, es_hija, avisos) rompe esta línea y la de abajo.
        llamada = "_veh_res = _ot_vehiculo_tras_crear(vid, numero_ot, titulo, fecha_prog, d, es_hija, tec_ids, autorizacion, avisos)"
        self.assertEqual(nucleo.count(llamada), 1)
        self.assertIn('_resp_ok["vehiculo"] = _veh_res', nucleo)
        # solo DESPUÉS del commit de la OT
        self.assertGreater(nucleo.index(llamada), nucleo.index("conn.commit()"))

    def test_la_ficha_de_la_ot_recibe_sus_vehiculos(self):
        self.assertIn("vehiculos_flota=_ot_vehiculos_ficha(vid)", self.app_py)
        self.assertIn("{% include 'ot2/_vehiculo.html' %}", _leer("templates", "ot2", "detalle.html"))


# ═══════════════════════════════════════════════════════════════════════════
#  El paso «Vehículo» del asistente y la ficha
# ═══════════════════════════════════════════════════════════════════════════
class TestAsistente(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.js = _leer("templates", "ot2", "_modal_crear.html")

    def test_el_paso_es_opcional_y_solo_existe_con_un_tecnico_interno(self):
        self.assertIn("if (_o2mVehAplica()) p.push({k:'vehiculo', n:'Vehículo'});", self.js)
        self.assertRegex(self.js, r"function _o2mTecInterno\(t\)\{ return !!t && !t\.empresa_id && !t\.es_externo; \}")
        # va DESPUÉS de la agenda (necesita el día y la hora) y es verde con «Sin vehículo»
        self.assertLess(self.js.index("p.push({k:'agenda',  n:'Agenda'});"), self.js.index("p.push({k:'vehiculo', n:'Vehículo'})"))
        self.assertIn("case 'vehiculo': return !S.veh.vehiculo_id ||", self.js)

    def test_lo_que_se_ve_elegido_es_lo_que_viaja(self):
        self.assertIn("body.vehiculo = {vehiculo_id: _vh.vehiculo_id, chofer_id: _vh.chofer_id, hora_desde: _vh.desde, hora_hasta: _vh.hasta};", self.js)
        self.assertIn("if (_o2mVehAplica() && S.veh.vehiculo_id && S.veh.chofer_id){", self.js)

    def test_el_sistema_sugiere_quien_maneja_y_la_persona_decide(self):
        self.assertIn("c.usuario_id === S.lider && _o2mVehElegible(c, V.vehiculo_id)", self.js)          # sugerido: el líder si puede
        self.assertIn("V.manualChofer = true;", self.js)                                                  # lo que elige la persona no se pisa
        self.assertIn("es el líder de la OT", self.js)                                                    # el origen de la sugerencia se ve

    def test_nunca_bloquea_la_creacion_con_un_error_de_flota(self):
        self.assertIn("La OT quedó creada · el vehículo no se reservó", self.js)
        self.assertIn("pídelo desde Solicitudes de vehículo".replace("pídelo", "Pide el vehículo"), self.js)

    def test_se_vuelve_a_mirar_la_disponibilidad_si_cambia_el_dia_la_hora_o_los_tecnicos(self):
        i = self.js.index("function o2mChequearChoqueDebounced(){")
        self.assertIn("_o2mVehCargarDebounced();", self.js[i:i + 400])

    def test_las_llamadas_viejas_no_pisan_a_las_nuevas(self):
        self.assertIn("if (tok !== _o2mVehToken) return;", self.js)

    def test_el_paso_esta_registrado_en_los_dos_despachadores(self):
        self.assertIn("if (p.k==='vehiculo') pintarVehiculo(b);", self.js)
        self.assertIn("if (k==='vehiculo') pintarVehiculo(b);", self.js)
        self.assertIn("vehSel:vehSel, vehChofer:vehChofer, vehHora:vehHora", self.js)

    def test_sin_alert_ni_confirm_nativos_en_el_codigo_nuevo(self):
        bloque = self.js[self.js.index("/* ── VEHÍCULO (2026-11-04)"):self.js.index("function pintarTecnicos(b){")]
        self.assertNotRegex(bloque, r"(?<![\w.])(alert|confirm|prompt)\(")

    def test_la_tarjeta_de_la_ficha_muestra_nombres_completos_y_enlaza_a_la_solicitud(self):
        t = _leer("templates", "ot2", "_vehiculo.html")
        self.assertIn("{{ s.vehiculo }}", t)
        self.assertIn('href="{{ s.enlace }}"', t)
        self.assertNotIn("text-overflow", t)
        self.assertNotIn("line-clamp", t)
        self.assertNotRegex(t, r"(?i)\b(costo|monto|precio)\b|\$")                  # sin plata (REGLAS #19 y #26)

    def test_flota_muestra_de_que_ot_viene_cada_reserva(self):
        js = _leer("static", "flota_uso.js")
        self.assertIn("Para la ' + esc(s.ot.numero)", js)
        self.assertIn("bloqueDet('Orden de trabajo'", js)


# ═══════════════════════════════════════════════════════════════════════════
#  Permisos (revisión escéptica 04-nov): reservar desde una OT pasa por la MISMA puerta que Solicitudes de vehículo
# ═══════════════════════════════════════════════════════════════════════════
class TestPermisosDesdeOT(BaseOT):
    def disp(self, headers, tecnicos="1,2", fecha=None, desde="08:00", hasta="12:00"):
        return self.c.get(f"/ot/api/flota/disponibilidad?fecha={fecha or DIA_ISO}&desde={desde}&hasta={hasta}&tecnicos={tecnicos}", headers=headers)

    def test_sin_el_permiso_de_flota_no_se_ve_la_disponibilidad(self):
        r = self.disp({"X-Rol": "admin"})
        self.assertEqual(r.status_code, 403)
        self.assertEqual(r.get_json()["error_codigo"], "SIN_PERMISO_FLOTA")

    def test_con_solicitar_vehiculos_si_se_ve(self):
        self.assertEqual(self.disp({"X-Rol": "admin", "X-Flota": "flota_solicitar"}).status_code, 200)

    def test_un_tecnico_no_aunque_tenga_el_permiso(self):
        self.app = _app(self.base, extra={"_es_rol_tecnico": lambda: True})
        self.c = self.app.test_client()
        self.assertEqual(self.disp({"X-Rol": "admin", "X-Flota": "flota_solicitar"}).status_code, 403)
        self.assertEqual(self.disp(SUPER).status_code, 403)

    def test_reservar_sin_el_permiso_no_guarda_nada(self):
        g.permissions = {}
        r = self.reservar()
        self.assertFalse(r["ok"])
        self.assertEqual(r["codigo"], "SIN_PERMISO_FLOTA")
        self.assertEqual(len(self.base.sol), 0)

    def test_un_tecnico_no_reserva_aunque_tenga_el_permiso(self):
        self.app = _app(self.base, extra={"_es_rol_tecnico": lambda: True})
        g.permissions = {"flota_solicitar": True}
        self.assertEqual(self.reservar()["codigo"], "SIN_PERMISO_FLOTA")

    def test_si_no_se_puede_saber_quien_es_se_cierra(self):
        def roto():
            raise RuntimeError("sin sesión")
        self.app = _app(self.base, extra={"_es_rol_tecnico": roto})
        self.assertEqual(self.reservar()["codigo"], "SIN_PERMISO_FLOTA")

    def test_ids_raros_en_la_consulta_no_dan_500(self):
        r = self.disp(SUPER, tecnicos="²,٣,1,-5,abc,,1")
        self.assertEqual(r.status_code, 200)
        self.assertEqual([c["usuario_id"] for c in r.get_json()["choferes"]], [1])


class TestDisponibilidadHorarios(BaseOT):
    def test_un_horario_que_ya_paso_no_se_ofrece_como_libre(self):
        ayer = (dt.date.today() - dt.timedelta(days=1)).isoformat()
        j = self.disponibilidad(fecha=ayer)
        self.assertFalse(j["horario_ok"])
        self.assertTrue(any("ya pasó" in a for a in j["avisos"]), j["avisos"])

    def test_avisa_si_las_llaves_no_se_entregan_a_esa_hora_o_ese_dia(self):
        j = self.disponibilidad(desde="07:00", hasta="12:00")                     # un martes hábil pero antes de las 08:00
        self.assertTrue(any("horario de entrega de llaves" in a for a in j["avisos"]), j["avisos"])
        hoy = dt.date.today()
        sabado = hoy + dt.timedelta(days=(5 - hoy.weekday()) % 7 or 7)
        j2 = self.disponibilidad(fecha=sabado.isoformat())
        self.assertTrue(any("fin de semana" in a for a in j2["avisos"]), j2["avisos"])


# ═══════════════════════════════════════════════════════════════════════════
#  UNA SOLA AGENDA (Daniel, 04-nov-2026): la OT y su vehículo se mueven y se cancelan juntos
# ═══════════════════════════════════════════════════════════════════════════
def _foto(fecha, ini, fin, estado="programada", fecha_fin=None):
    return {"estado": estado, "fecha_programada": fecha, "fecha_fin": fecha_fin, "hora_inicio": ini, "hora_fin": fin}


class TestUnaSolaAgenda(BaseOT):
    def setUp(self):
        super().setUp()
        self.sinc = flota_module.OT_API["sincronizar"]
        self.sid = self.reservar()["solicitud"]["id"]                    # furgón, Ana, DIA de 08:00 a 12:00 (OT-2026-00077)
        self.dia2 = DIA + dt.timedelta(days=1)                            # miércoles

    def fila(self):
        return self.base.sol[self.sid]

    def test_cancelar_la_ot_cancela_su_reserva_y_libera_el_vehiculo(self):
        r = self.sinc(77, cancelar="La OT se canceló.")
        self.assertTrue(r["ok"])
        self.assertEqual([a["accion"] for a in r["acciones"]], ["cancelada"])
        self.assertIn("Se canceló la reserva", r["acciones"][0]["texto"])
        f = self.fila()
        self.assertEqual(f["estado"], "cancelada")
        self.assertIn("La OT se canceló", f["cancelada_motivo"])
        self.assertTrue(next(v for v in self.disponibilidad()["vehiculos"] if v["id"] == self.furgon)["libre"])      # el horario quedó libre
        quien = next(x for x in self.base.slog if x["solicitud_id"] == self.sid and x["accion"] == "cancelada")
        self.assertIn('"origen": "ot"', quien["detalle"])

    def test_sin_correos_al_cancelar_ni_al_mover(self):
        aviso = mock.Mock(return_value=1)
        self.app = _app(self.base, extra={"_aviso_interno_correo": aviso})
        g.permissions = {"superadmin": True}
        self.sinc = flota_module.OT_API["sincronizar"]
        self.sinc(77, antes=_foto(DIA, "08:00", "12:00"), despues=_foto(self.dia2, "09:00", "13:00"))
        self.sinc(77, cancelar="La OT se canceló.")
        aviso.assert_not_called()

    def test_mover_la_ot_mueve_la_reserva_y_vuelve_a_por_confirmar(self):
        self.c.post(f"/flota/api/solicitudes/{self.sid}/confirmar", json={}, headers=SUPER)
        self.assertEqual(self.fila()["estado"], "confirmada")
        sello = self.fila()["sello_at"]
        r = self.sinc(77, antes=_foto(DIA, "08:00", "12:00"), despues=_foto(self.dia2, "09:00", "13:00"))
        self.assertEqual([a["accion"] for a in r["acciones"]], ["movida"])
        f = self.fila()
        self.assertEqual((f["inicio"], f["fin"]), (dt.datetime.combine(self.dia2, dt.time(9, 0)), dt.datetime.combine(self.dia2, dt.time(13, 0))))
        self.assertEqual(f["estado"], "solicitada")                         # la gestión había confirmado OTRO horario
        self.assertIsNone(f["confirmada_por"])
        self.assertGreaterEqual(f["sello_at"], sello)
        self.assertIn("Por confirmar", r["acciones"][0]["texto"])

    def test_se_corre_lo_mismo_que_la_ot_y_se_respeta_lo_que_la_persona_ajusto(self):
        self.base.sol[self.sid]["inicio"] = dt.datetime.combine(DIA, dt.time(7, 30))          # salía media hora antes que la OT
        self.base.sol[self.sid]["fin"] = dt.datetime.combine(DIA, dt.time(12, 30))            # y volvía media hora después
        self.sinc(77, antes=_foto(DIA, "08:00", "12:00"), despues=_foto(self.dia2, "08:00", "13:00"))
        f = self.fila()
        self.assertEqual(f["inicio"], dt.datetime.combine(self.dia2, dt.time(7, 30)))          # el día cambió, la salida sigue 30 min antes
        self.assertEqual(f["fin"], dt.datetime.combine(self.dia2, dt.time(13, 30)))            # la OT terminó 1 h más tarde: el regreso también

    def test_las_horas_de_mysql_vienen_como_timedelta(self):
        r = self.sinc(77, antes=_foto(DIA, dt.timedelta(hours=8), dt.timedelta(hours=12)),
                      despues=_foto(self.dia2, dt.timedelta(hours=8), dt.timedelta(hours=12)))
        self.assertEqual([a["accion"] for a in r["acciones"]], ["movida"])
        self.assertEqual(self.fila()["inicio"], dt.datetime.combine(self.dia2, dt.time(8, 0)))

    def test_si_la_ot_no_cambio_ni_de_dia_ni_de_hora_no_se_toca_nada(self):
        antes = dict(self.fila())
        r = self.sinc(77, antes=_foto(DIA, "08:00", "12:00"), despues=_foto(DIA, "08:00", "12:00", estado="en_curso"))
        self.assertEqual(r["acciones"], [])
        self.assertEqual(self.fila()["inicio"], antes["inicio"])
        self.assertEqual(self.fila()["estado"], "solicitada")

    def test_si_en_el_horario_nuevo_el_vehiculo_esta_ocupado_la_reserva_se_cancela_y_se_dice(self):
        otra = flota_module.OT_API["reservar"](78, "OT-2026-00078", "Otra", self.dia2.isoformat(), "08:00", "12:00", self.furgon, self.beto, (9, "Aarón"))
        self.assertTrue(otra["ok"], otra)
        r = self.sinc(77, antes=_foto(DIA, "08:00", "12:00"), despues=_foto(self.dia2, "08:00", "12:00"))
        self.assertEqual([a["accion"] for a in r["acciones"]], ["cancelada"])
        self.assertIn("no se pudo mover con ella", r["acciones"][0]["texto"])
        self.assertIn("Pide el vehículo de nuevo", r["acciones"][0]["texto"])
        self.assertEqual(self.fila()["estado"], "cancelada")                 # no queda una reserva en el día equivocado

    def test_con_la_llave_entregada_el_vehiculo_ya_salio_y_solo_se_avisa(self):
        self.base.sol[self.sid]["estado"] = "llave_entregada"
        r = self.sinc(77, cancelar="La OT se canceló.")
        self.assertEqual(r["acciones"], [])
        self.assertIn("ya salió con la llave entregada", r["avisos"][0])
        self.assertEqual(self.fila()["estado"], "llave_entregada")
        r2 = self.sinc(77, antes=_foto(DIA, "08:00", "12:00"), despues=_foto(self.dia2, "08:00", "12:00"))
        self.assertIn("ya salió", r2["avisos"][0])
        self.assertEqual(self.fila()["inicio"], dt.datetime.combine(DIA, dt.time(8, 0)))

    def test_una_ot_que_queda_sin_fecha_cancela_la_reserva(self):
        r = self.sinc(77, antes=_foto(DIA, "08:00", "12:00"), despues=_foto(dt.date(2099, 12, 31), "08:00", "12:00"))
        self.assertEqual([a["accion"] for a in r["acciones"]], ["cancelada"])
        self.assertIn("sin fecha", self.fila()["cancelada_motivo"])

    def test_una_ot_sin_reserva_no_hace_nada_ni_falla(self):
        r = self.sinc(999, cancelar="La OT se canceló.")
        self.assertEqual((r["ok"], r["acciones"], r["avisos"]), (True, [], []))

    def test_las_reservas_ya_canceladas_o_devueltas_no_se_tocan(self):
        self.base.sol[self.sid]["estado"] = "devuelta"
        self.assertEqual(self.sinc(77, cancelar="La OT se canceló.")["acciones"], [])
        self.assertEqual(self.fila()["estado"], "devuelta")

    def test_fotos_incompletas_o_raras_no_rompen(self):
        for antes, despues in ((None, None), ({}, {}), (_foto("no es fecha", None, None), _foto(self.dia2, "08:00", "12:00"))):
            self.assertEqual(self.sinc(77, antes=antes, despues=despues)["acciones"], [])
        self.assertEqual(self.fila()["estado"], "solicitada")

    def test_nunca_lanza_aunque_falle_la_base(self):
        def boom(*a, **k):
            raise RuntimeError("base caída")
        self.app = _app(self.base, extra={"mysql_fetchall": boom})
        g.permissions = {"superadmin": True}
        r = flota_module.OT_API["sincronizar"](77, cancelar="La OT se canceló.")
        self.assertFalse(r["ok"])
        self.assertIn("Solicitudes de vehículo", r["avisos"][0])


class TestChoferDevuelve(BaseOT):
    """Una reserva que nace de una OT la pide quien creó la OT, pero la devuelve el técnico que retiró la llave (revisión escéptica 04-nov)."""

    def test_el_chofer_puede_devolver_aunque_no_sea_quien_pidio(self):
        sid = self.reservar(solicitante=(9, "Aarón Pérez"))["solicitud"]["id"]          # chofer = Ana (usuario 1)
        self.base.sol[sid]["estado"] = "llave_entregada"
        j = self.c.get(f"/flota/api/solicitudes/{sid}", headers={"X-Rol": "admin", "X-Flota": "flota_solicitar"}).get_json()
        self.assertTrue(j["solicitud"]["puede"]["devolver"])                             # quien está conectado es el usuario 1 = Ana
        self.assertFalse(j["solicitud"]["puede"]["cancelar"])

    def test_otra_persona_que_solo_puede_pedir_no_devuelve(self):
        sid = self.reservar(chofer_id=self.beto, solicitante=(9, "Aarón Pérez"))["solicitud"]["id"]      # el chofer es Beto, no el usuario 1
        self.base.sol[sid]["estado"] = "llave_entregada"
        j = self.c.get(f"/flota/api/solicitudes/{sid}", headers={"X-Rol": "admin", "X-Flota": "flota_solicitar"}).get_json()
        self.assertFalse(j["solicitud"]["puede"]["devolver"])


# ═══════════════════════════════════════════════════════════════════════════
#  Enganches en app.py de la sincronización (se prueba la función real, sin cargar app.py entero)
# ═══════════════════════════════════════════════════════════════════════════
class TestEnganchesSincronizacion(BaseOT):
    def setUp(self):
        super().setUp()
        self.app_py = _leer("app.py")
        self.sid = self.reservar()["solicitud"]["id"]
        self.bitacora, self.fila_ot = [], {"estado": "programada", "fecha_programada": DIA, "fecha_fin": None,
                                           "hora_inicio": dt.timedelta(hours=8), "hora_fin": dt.timedelta(hours=12)}
        self.ns = {"mysql_fetchone": lambda q, p=None: dict(self.fila_ot), "get_db": lambda: mock.Mock(),
                   "_mant_log": lambda *a, **k: self.bitacora.append(a)}
        exec("_OT_VEH_FOTO_SQL = " + repr("SELECT estado, fecha_programada, fecha_fin, hora_inicio, hora_fin FROM mant_visitas WHERE id=%s"), self.ns)
        for nombre in ("_ot_vehiculo_antes", "_ot_vehiculo_sincronizar"):
            exec(_fuente_funcion(self.app_py, nombre), self.ns)                       # noqa: S102 (código del propio proyecto)

    def test_cancelar_la_ot_cancela_la_reserva_y_lo_anota_en_su_bitacora(self):
        antes = self.ns["_ot_vehiculo_antes"](77)
        self.fila_ot["estado"] = "cancelada"
        r = self.ns["_ot_vehiculo_sincronizar"](77, antes)
        self.assertEqual(r["acciones"][0]["accion"], "cancelada")
        self.assertEqual(self.base.sol[self.sid]["estado"], "cancelada")
        self.assertEqual(self.bitacora[-1][:3], ("visita", 77, "vehiculo_sincronizado"))

    def test_anular_dice_anulo(self):
        self.fila_ot["estado"] = "anulada"
        self.ns["_ot_vehiculo_sincronizar"](77, self.ns["_ot_vehiculo_antes"](77))
        self.assertIn("se anuló", self.base.sol[self.sid]["cancelada_motivo"])

    def test_mover_la_ot_mueve_la_reserva(self):
        antes = self.ns["_ot_vehiculo_antes"](77)
        self.fila_ot.update(fecha_programada=DIA + dt.timedelta(days=1), hora_inicio=dt.timedelta(hours=9), hora_fin=dt.timedelta(hours=13))
        r = self.ns["_ot_vehiculo_sincronizar"](77, antes)
        self.assertEqual(r["acciones"][0]["accion"], "movida")
        self.assertEqual(self.base.sol[self.sid]["inicio"], dt.datetime.combine(DIA + dt.timedelta(days=1), dt.time(9, 0)))

    def test_eliminar_la_ot_cancela_la_reserva(self):
        r = self.ns["_ot_vehiculo_sincronizar"](77, eliminada=True)
        self.assertEqual(r["acciones"][0]["accion"], "cancelada")
        self.assertIn("se eliminó", self.base.sol[self.sid]["cancelada_motivo"])

    def test_un_cambio_sin_importancia_no_devuelve_nada(self):
        antes = self.ns["_ot_vehiculo_antes"](77)
        self.fila_ot["estado"] = "en_curso"
        self.assertIsNone(self.ns["_ot_vehiculo_sincronizar"](77, antes))
        self.assertEqual(self.bitacora, [])

    def test_sin_foto_previa_no_se_mueve_nada(self):
        self.fila_ot["fecha_programada"] = DIA + dt.timedelta(days=3)
        self.assertIsNone(self.ns["_ot_vehiculo_sincronizar"](77, None))
        self.assertEqual(self.base.sol[self.sid]["inicio"], dt.datetime.combine(DIA, dt.time(8, 0)))

    def test_si_el_modulo_de_flota_no_cargo_la_ot_se_guarda_igual(self):
        saved = dict(flota_module.OT_API)
        try:
            flota_module.OT_API.clear()
            self.assertIsNone(self.ns["_ot_vehiculo_sincronizar"](77, eliminada=True))
        finally:
            flota_module.OT_API.update(saved)

    def test_cableado_en_guardar_y_en_eliminar_la_ot(self):
        put = _fuente_funcion(self.app_py, "mant_visita_update")
        self.assertIn("_veh_antes = _ot_vehiculo_antes(vid) if _tec_avisar else None", put)
        self.assertIn("_ot_vehiculo_sincronizar(vid, _veh_antes)", put)
        self.assertLess(put.index("_veh_antes = _ot_vehiculo_antes(vid)"), put.index("conn = get_mysql()"))        # la foto, ANTES de guardar
        self.assertIn('resp["vehiculo"] = _veh_sync', put)
        eliminar = _fuente_funcion(self.app_py, "_mant_visita_eliminar_core")
        self.assertIn("_ot_vehiculo_sincronizar(vid, eliminada=True)", eliminar)
        self.assertGreater(eliminar.index("_ot_vehiculo_sincronizar(vid, eliminada=True)"), eliminar.index("conn.commit()"))      # solo con la OT ya borrada
        self.assertIn('"vehiculo": res.get("vehiculo")', self.app_py)

    def test_la_aprobacion_remota_ya_no_pierde_el_resultado_del_vehiculo(self):
        aprobar = _fuente_funcion(self.app_py, "ot_aut_api_aprobar")
        self.assertIn('"vehiculo": cuerpo.get("vehiculo")', aprobar)
        js = _leer("templates", "ot2", "_modal_crear.html")
        self.assertIn("_o2mVehResultado(a.j.vehiculo || null)", js)


# ═══════════════════════════════════════════════════════════════════════════
#  La lógica del asistente, EJECUTADA (Node): no solo que el texto exista
# ═══════════════════════════════════════════════════════════════════════════
HAY_NODE = shutil.which("node") is not None

_PRUEBA_JS = r"""
const vm = require('vm'), fs = require('fs');
const src = fs.readFileSync(process.argv[2], 'utf8');
const bloque = src.slice(src.indexOf('  var _o2mVehToken = 0'), src.indexOf('  function pintarTecnicos(b){'));
const resultados = [];
const ok = (nombre, cond, extra) => resultados.push({nombre, ok: !!cond, extra: cond ? '' : String(extra || '')});

function montar(opts) {
  opts = opts || {};
  const els = {o2mVehCuerpo: {innerHTML: '', contains: () => false}, o2mVehLive: {textContent: ''}};
  const toasts = [], pendientes = [];
  const ctx = {
    S: {fecha: '2026-11-10', hini: '08:00', hfin: '12:00', tecnicos: [1, 2], lider: 1, desinst: {lleva: ''},
        veh: {vehiculo_id: null, chofer_id: null, usuario_id: null, desde: '', hasta: '', manualHora: false, manualChofer: false, data: null, cargando: false, error: false, aviso: ''}},
    cacheTec: opts.cacheTec || [{id: 1, nombre: 'Ana', empresa_id: null, es_externo: false}, {id: 2, nombre: 'Beto', empresa_id: null, es_externo: false},
                               {id: 5, nombre: 'Marco', empresa_id: 3, es_externo: true}],
    O2M_ES_TECNICO: !!opts.tecnico, window: {}, console,
    document: {activeElement: null, getElementById: id => els[id] || null},
    esc: s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'}[c])),
    refrescarAnclas() {}, pintarFormulario() { ctx.__pintarFormulario = (ctx.__pintarFormulario || 0) + 1; }, _o2mDiAplica: () => false,
    ilusToast: m => toasts.push(m), encodeURIComponent, setTimeout, clearTimeout,
    fetch: url => new Promise(res => pendientes.push({url, responder: d => res({json: () => Promise.resolve(d)})})),
  };
  vm.createContext(ctx);
  vm.runInContext(bloque, ctx);
  return {ctx, els, toasts, pendientes};
}
const veh = (id, titulo, libre, extra) => Object.assign({id, titulo, marca_modelo: 'Marca X', licencia_exigida: ['B'], libre, motivo: libre ? null : 'Ya está reservado: VEH-1', choque: null}, extra || {});
const cho = (uid, nombre, cid, porVeh, extra) => Object.assign({usuario_id: uid, nombre, chofer_id: cid, enrolado: !!cid, motivo: cid ? null : 'No está enrolado como chofer', por_vehiculo: porVeh || {}}, extra || {});
const datos = (vehs, chs) => ({ok: true, horario_ok: true, vehiculos: vehs, choferes: chs, avisos: [], horario_llaves: 'lunes a viernes', sla_respuesta_min: 120});
const tick = () => new Promise(r => setTimeout(r, 0));

(async () => {
  // 1. Cuándo existe el paso
  { const m = montar(); ok('el paso existe con un técnico interno', m.ctx._o2mVehAplica());
    const m2 = montar({tecnico: true}); ok('no existe para un técnico (rol)', !m2.ctx._o2mVehAplica());
    const m3 = montar(); m3.ctx.S.tecnicos = [5]; ok('no existe con solo un técnico externo', !m3.ctx._o2mVehAplica());
    const m4 = montar(); m4.ctx.window.__o2mVehDenegado = true; ok('no existe tras un 403 de permisos', !m4.ctx._o2mVehAplica()); }

  // 2. Quién maneja: se sugiere al líder si puede; si no, al primero que pueda; lo manual no cambia en silencio
  { const m = montar(), {ctx} = m, V = ctx.S.veh;
    V.data = datos([veh(10, 'Furgón', true)], [cho(1, 'Ana', 101, {10: {ok: true}}), cho(2, 'Beto', 102, {10: {ok: true}})]);
    V.vehiculo_id = 10; ctx._o2mVehAuto();
    ok('sugiere al líder', V.chofer_id === 101, V.chofer_id);
    V.data.choferes[0].por_vehiculo[10] = {ok: false, error: 'licencia vencida'}; V.chofer_id = null; ctx._o2mVehAuto();
    ok('si el líder no puede, sugiere al primero que pueda', V.chofer_id === 102, V.chofer_id);
    V.manualChofer = true; V.chofer_id = 102; V.data.choferes[1].por_vehiculo[10] = {ok: false, error: 'ocupado'}; ctx._o2mVehAuto();
    ok('lo elegido a mano que deja de poder NO cambia en silencio', /ya no puede manejar/.test(V.aviso) && V.chofer_id === null, V.aviso); }

  // 3. Un vehículo que deja de estar libre se suelta con un aviso
  { const m = montar(), {ctx} = m, V = ctx.S.veh;
    V.data = datos([veh(10, 'Furgón TVDZ-88', false)], []); V.vehiculo_id = 10; V.chofer_id = 101; ctx._o2mVehAuto();
    ok('un vehículo ocupado se suelta y se dice', V.vehiculo_id === null && /ya no está libre/.test(V.aviso), V.aviso); }

  // 4. Elegir lo que no se puede no cambia nada
  { const m = montar(), {ctx} = m, V = ctx.S.veh;
    V.data = datos([veh(10, 'Furgón', true), veh(11, 'Camión', false)], [cho(1, 'Ana', 101, {10: {ok: true}, 11: {ok: true}}), cho(4, 'Dani', null, {})]);
    ctx.vehSel(11); ok('vehículo ocupado: no se elige', V.vehiculo_id === null);
    ctx.vehSel(10); ok('vehículo libre: se elige y se sugiere quien maneja', V.vehiculo_id === 10 && V.chofer_id === 101);
    ctx.vehChofer(0); ok('chofer sin enrolar: no se elige', V.chofer_id === 101);
    ctx.vehSel(0); ok('«Sin vehículo» limpia todo', V.vehiculo_id === null && V.chofer_id === null); }

  // 5. Lo que se pinta: roles accesibles, sin opacidad, escapado
  { const m = montar(), {ctx, els} = m, V = ctx.S.veh;
    V.data = datos([veh(10, '<img src=x onerror=alert(1)>', true), veh(11, 'Camión', false)], [cho(1, 'Ana', 101, {10: {ok: true}})]);
    V.vehiculo_id = 10; V.chofer_id = 101; ctx._o2mVehPintar();
    const h = els.o2mVehCuerpo.innerHTML;
    ok('grupos con rol radiogroup', (h.match(/role="radiogroup"/g) || []).length === 2, h.slice(0, 200));
    ok('cada opción dice si está elegida', /role="radio" aria-checked="true"/.test(h) && /role="radio" aria-checked="false"/.test(h));
    ok('lo no disponible usa aria-disabled y NO opacidad', /aria-disabled="true"/.test(h) && !/opacity/.test(h));
    ok('el texto del servidor va escapado', !/<img/.test(h) && /&lt;img/.test(h));
    ok('dice quién maneja y es sugerido', /sugerido: es el líder de la OT/.test(h)); }

  // 6. Error de red: se suelta lo elegido y se ofrece reintentar
  { const m = montar(), {ctx, els, pendientes} = m, V = ctx.S.veh;
    V.vehiculo_id = 10; V.chofer_id = 101; ctx.pintarVehiculo({innerHTML: ''});
    pendientes[0].responder({ok: false}); await tick(); await tick();
    ok('error: se suelta el vehículo elegido', V.vehiculo_id === null && V.chofer_id === null && V.error === true);
    ok('error: hay botón Reintentar', /Reintentar/.test(els.o2mVehCuerpo.innerHTML)); }

  // 7. Sin permiso: el paso desaparece una vez
  { const m = montar(), {ctx, pendientes} = m;
    ctx.pintarVehiculo({innerHTML: ''}); pendientes[0].responder({ok: false, error_codigo: 'SIN_PERMISO_FLOTA'}); await tick(); await tick();
    ok('403 de permisos: se recuerda y se repinta el formulario', ctx.window.__o2mVehDenegado === true && ctx.__pintarFormulario === 1 && !ctx._o2mVehAplica()); }

  // 8. Carrera: una respuesta vieja no pisa a la nueva
  { const m = montar(), {ctx, pendientes} = m, V = ctx.S.veh;
    ctx.pintarVehiculo({innerHTML: ''});                       // consulta 1 (horario viejo)
    ctx.vehHora('hasta', '13:00');                              // cambia la hora: la consulta 1 queda vieja al instante
    pendientes[0].responder(datos([veh(10, 'Furgón viejo', true)], []));
    await tick(); await tick();
    ok('la respuesta vieja se ignora', V.data === null, JSON.stringify(V.data)); }

  // 9. Repintar no le quita el foco a quien está tecleando una hora o navegando con el teclado
  { const m = montar(), {ctx, els} = m, V = ctx.S.veh; let enfocado = null;
    V.data = datos([veh(10, 'Furgón', true)], [cho(1, 'Ana', 101, {10: {ok: true}})]); V.vehiculo_id = 10; V.chofer_id = 101; V.desde = '08:00'; V.hasta = '12:00';
    const inp = {id: 'o2mVehDesde', focus() { enfocado = 'o2mVehDesde'; }};
    els.o2mVehDesde = inp; ctx.document.activeElement = inp; els.o2mVehCuerpo.contains = el => el === inp;
    ctx._o2mVehPintar();
    ok('el foco vuelve al mismo control tras repintar', enfocado === 'o2mVehDesde', enfocado); }

  // 10. Reintentar vuelve a pedir la disponibilidad
  { const m = montar(), {ctx, pendientes} = m;
    ctx.pintarVehiculo({innerHTML: ''}); pendientes[0].responder({ok: false}); await tick(); await tick();
    ctx.vehReintentar(); ok('reintentar hace otra consulta', pendientes.length === 2, pendientes.length); }

  // 11. La hora del vehículo sigue la de la OT mientras la persona no la cambie
  { const m = montar(), {ctx} = m, V = ctx.S.veh;
    ctx._o2mVehSync(); ok('sale y vuelve a la hora de la OT', V.desde === '08:00' && V.hasta === '12:00', V.desde + '-' + V.hasta);
    ctx.S.hini = '09:30'; ctx._o2mVehSync(); ok('si la OT se mueve, el vehículo la sigue', V.desde === '09:30', V.desde);
    ctx.vehHora('desde', '07:45'); ctx.S.hini = '10:00'; ctx._o2mVehSync();
    ok('lo que la persona cambió a mano ya no lo pisa la OT', V.desde === '07:45' && V.manualHora === true, V.desde); }

  // 12. La consulta lleva el día, la hora y SOLO los técnicos internos
  { const m = montar(), {ctx, pendientes} = m;
    ctx.S.tecnicos = [1, 2, 5];                                  // 5 es externo
    ctx.pintarVehiculo({innerHTML: ''});
    const u = pendientes[0].url;
    ok('la consulta lleva fecha, horas y técnicos internos', /fecha=2026-11-10/.test(u) && /desde=08%3A00/.test(u) && /hasta=12%3A00/.test(u) && /tecnicos=1,2$/.test(u), u); }

  console.log(JSON.stringify(resultados));
})();
"""


@unittest.skipUnless(HAY_NODE, "Node no está instalado")
class TestLogicaDelAsistenteEjecutada(unittest.TestCase):
    def test_logica_del_paso_vehiculo(self):
        with tempfile.TemporaryDirectory() as d:
            prueba = os.path.join(d, "prueba.js")
            with open(prueba, "w", encoding="utf-8") as f:
                f.write(_PRUEBA_JS)
            ruta = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "templates", "ot2", "_modal_crear.html")
            r = subprocess.run(["node", prueba, ruta], capture_output=True, text=True, timeout=60, encoding="utf-8")
        self.assertEqual(r.returncode, 0, r.stderr[-1500:])
        resultados = json.loads(r.stdout.strip().splitlines()[-1])
        self.assertGreaterEqual(len(resultados), 15)
        fallas = [x for x in resultados if not x["ok"]]
        self.assertEqual(fallas, [], json.dumps(fallas, ensure_ascii=False, indent=1))


# ═══════════════════════════════════════════════════════════════════════════
#  Revisión de las pruebas (escéptico 04-nov): el enganche de la creación se EJECUTA, la regla de 24 horas se prueba de verdad, y lo que
#  antes era solo texto ahora falla si el comportamiento se rompe
# ═══════════════════════════════════════════════════════════════════════════
class _AhoraFijo(dt.datetime):
    """«Ahora» = lunes 2-nov-2026 15:00 (hora de Chile): la regla de 24 horas no puede depender de cuándo se corra la prueba."""

    @classmethod
    def now(cls, tz=None):
        return cls(2026, 11, 2, 15, 0)


class TestEngancheDeCreacionEjecutado(BaseOT):
    def setUp(self):
        super().setUp()
        self.app_py = _leer("app.py")
        self.bitacora = []
        self.ns = {"mysql_fetchone": self.base.fetchone, "_mant_log": lambda *a, **k: self.bitacora.append(a)}
        for nombre in ("_ot_vehiculo_reservar", "_ot_vehiculo_tras_crear"):
            exec(_fuente_funcion(self.app_py, nombre), self.ns)                       # noqa: S102 (código del propio proyecto)
        self.avisos = []

    def tras_crear(self, vehiculo="ok", es_hija=False, tec_ids=(1, 2), autorizacion=None, fecha=None):
        if vehiculo == "ok":
            vehiculo = {"vehiculo_id": self.furgon, "chofer_id": self.ana, "hora_desde": "08:00", "hora_hasta": "12:00"}
        d = {} if vehiculo is None else {"vehiculo": vehiculo}
        return self.ns["_ot_vehiculo_tras_crear"](77, NUM_OT, "Instalación Gimnasio Norte", fecha or (DIA_ISO + "T09:00:00"), d, es_hija,
                                                  list(tec_ids), autorizacion, self.avisos)

    def test_reserva_en_el_dia_de_la_ot_con_sus_tecnicos_y_el_usuario_conectado(self):
        r = self.tras_crear()
        self.assertTrue(r["ok"], r)
        f = self.base.sol[r["solicitud"]["id"]]
        self.assertEqual(f["inicio"].date(), DIA)                    # la fecha de la OT, cortada a AAAA-MM-DD
        self.assertEqual((f["ot_id"], f["ot_numero"]), (77, NUM_OT))
        self.assertEqual(f["solicitante_id"], 9)                     # quien está conectado (g.user)

    def test_los_avisos_de_la_reserva_se_suman_a_los_de_la_ot(self):
        hoy = dt.date.today()
        sabado = hoy + dt.timedelta(days=(5 - hoy.weekday()) % 7 or 7)
        r = self.tras_crear(fecha=sabado.isoformat())
        self.assertTrue(r["ok"], r)
        self.assertTrue(any("fin de semana" in a for a in self.avisos), self.avisos)

    def test_la_hija_de_una_desinstalacion_no_reserva(self):
        self.assertIsNone(self.tras_crear(es_hija=True))
        self.assertEqual(len(self.base.sol), 0)

    def test_sin_vehiculo_pedido_no_hace_nada(self):
        for v in (None, "no es un dict", {}, {"chofer_id": self.ana}, {"vehiculo_id": 0}):
            self.assertIsNone(self.tras_crear(vehiculo=v))
        self.assertEqual(len(self.base.sol), 0)

    def test_quien_maneja_tiene_que_ser_tecnico_de_esa_ot_y_el_fallo_queda_en_la_bitacora(self):
        r = self.tras_crear(tec_ids=(2,))                            # Ana (usuario 1) no es técnico de esta OT
        self.assertFalse(r["ok"])
        self.assertIn("técnicos de esta OT", r["error"])
        self.assertEqual(self.bitacora[-1][:3], ("visita", 77, "vehiculo_no_reservado"))
        self.assertEqual(self.avisos, [])                            # el creador normal ya ve la alerta del asistente

    def test_en_una_ot_aprobada_a_distancia_pide_quien_la_pidio_y_el_fallo_viaja_en_avisos(self):
        aut = {"solicitado_por_user_id": 7, "solicitado_por_nombre": "Víctor Rojas"}
        r = self.tras_crear(autorizacion=aut)
        self.assertTrue(r["ok"], r)
        self.assertEqual(self.base.sol[r["solicitud"]["id"]]["solicitante_id"], 7)
        r2 = self.tras_crear(autorizacion=aut)                       # el mismo vehículo y horario ya está ocupado
        self.assertFalse(r2["ok"])
        self.assertTrue(any("ya está reservado" in a for a in self.avisos), self.avisos)      # quien aprueba lo ve


class TestVeinticuatroHoras(BaseOT):
    def test_una_ot_para_manana_temprano_reserva_con_aviso_y_el_menu_sigue_exigiendo_24_horas(self):
        with mock.patch.object(flota_module, "datetime", _AhoraFijo):
            r = self.reservar(fecha="2026-11-03", desde="09:00", hasta="12:00")             # faltan menos de 24 h desde el lunes 15:00
            self.assertTrue(r["ok"], r)
            self.assertTrue(any("24 horas" in a for a in r["avisos"]), r["avisos"])
            self.assertNotIn("urgente", " ".join(r["avisos"]))                              # no se promete una marca que no existe
            resp = self.c.post("/flota/api/solicitudes", headers={"X-Rol": "admin", "X-Flota": "flota_solicitar"},
                               json={"vehiculo_id": self.camion, "chofer_id": self.beto, "area": "Logística", "motivo": "Reparto",
                                     "fecha": "2026-11-03", "hora_desde": "09:00", "hora_hasta": "12:00"})
            self.assertEqual(resp.status_code, 400)                                         # quien pide por el menú sigue con las 24 horas
            self.assertIn("24 horas", json.dumps(resp.get_json(), ensure_ascii=False))

    def test_un_dia_que_ya_paso_se_rechaza_aunque_venga_de_una_ot(self):
        with mock.patch.object(flota_module, "datetime", _AhoraFijo):
            r = self.reservar(fecha="2026-11-01", desde="09:00", hasta="12:00")
        self.assertFalse(r["ok"])
        self.assertEqual(len(self.base.sol), 0)


class TestDisponibilidadChoferes(BaseOT):
    def por_uid(self, **k):
        return {c["usuario_id"]: c for c in self.disponibilidad(**k)["choferes"]}

    def test_hora_de_regreso_antes_de_la_salida_no_es_un_horario(self):
        self.assertFalse(self.disponibilidad(desde="12:00", hasta="08:00")["horario_ok"])

    def test_licencia_que_vence_antes_del_dia_de_la_ot_no_puede_manejar(self):
        self.base.cho[self.ana]["licencia_vence"] = (dt.date.today() + dt.timedelta(days=2)).isoformat()      # vence DESPUÉS de hoy pero ANTES de la OT
        c = self.por_uid()
        self.assertFalse(c[1]["por_vehiculo"][str(self.furgon)]["ok"])
        self.assertIn("licencia", c[1]["por_vehiculo"][str(self.furgon)]["error"].lower())
        self.assertTrue(c[2]["por_vehiculo"][str(self.furgon)]["ok"])

    def test_un_chofer_archivado_no_cuenta_como_enrolado(self):
        self.base.cho[self.ana]["archivado"] = 1
        self.assertFalse(self.por_uid()[1]["enrolado"])

    def test_un_chofer_inhabilitado_no_puede(self):
        self.base.cho[self.ana]["habilitado"] = 0
        x = self.por_uid()[1]["por_vehiculo"][str(self.furgon)]
        self.assertFalse(x["ok"])
        self.assertIn("inhabilitado", x["error"])

    def test_un_usuario_desactivado_no_puede(self):
        self.base.usr[1]["active"] = 0
        self.assertFalse(self.por_uid()[1]["por_vehiculo"][str(self.furgon)]["ok"])


class TestTarjetaDeLaFicha(unittest.TestCase):
    RESERVA = {"id": 5, "numero": "VEH-2026-0005", "estado": "confirmada", "estado_texto": "Confirmada", "vehiculo": "Furgón TVDZ-88",
               "rango_texto": "mar 10/11/2026 · 08:00 a 12:00", "chofer": "Ana María Pérez Soto", "enlace": "/flota?sol=5#solicitudes"}

    def render(self, lista):
        import jinja2
        return jinja2.Environment(autoescape=True).from_string(_leer("templates", "ot2", "_vehiculo.html")).render(vehiculos_flota=lista)

    def test_muestra_la_reserva_completa_y_enlaza_a_ella(self):
        h = self.render([self.RESERVA])
        for texto in ("Furgón TVDZ-88", "mar 10/11/2026 · 08:00 a 12:00", "Ana María Pérez Soto", "VEH-2026-0005", "Confirmada", 'href="/flota?sol=5#solicitudes"'):
            self.assertIn(texto, h)
        self.assertIn('class="e-confirmada"', h)

    def test_sin_reservas_no_dibuja_nada(self):
        self.assertEqual(self.render([]).strip(), "")

    def test_lo_que_viene_de_la_base_va_escapado(self):
        malo = dict(self.RESERVA, vehiculo="<script>alert(1)</script>", chofer="<img src=x onerror=alert(1)>")
        h = self.render([malo])
        self.assertNotIn("<script>alert", h)
        self.assertNotIn("<img", h)
        self.assertIn("&lt;script&gt;", h)

    def test_cada_estado_tiene_su_semaforo_y_ninguno_oculta_el_texto(self):
        for est in ("solicitada", "confirmada", "llave_entregada", "devuelta", "cancelada"):
            self.assertIn(f".otd-veh li.e-{est}" if est != "solicitada" else ".otd-veh li{", self.render([dict(self.RESERVA, estado=est)]))


if __name__ == "__main__":
    unittest.main()
