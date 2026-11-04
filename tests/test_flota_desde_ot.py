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
import os
import re
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import flota_module  # noqa: E402
from test_flota_solicitudes import (  # noqa: E402
    DIA_ISO, FUTURO, SUPER, Base, Falsa, _app, _datos_chofer, _leer, _llave_cualquier_dia)

NUM_OT = "OT-2026-00077"


class BaseOT(Base):
    """Dos vehículos (furgón, camión) y dos choferes enrolados (Ana = usuario 1, Beto = usuario 2). Dani (usuario 4) NO está enrolado."""

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
        self.assertIn(NUM_OT, creada["detalle"])

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
        self.assertIn('d.get("vehiculo")', nucleo)
        self.assertIn("_ot_vehiculo_reservar(", nucleo)
        self.assertIn('_resp_ok["vehiculo"]', nucleo)
        # solo DESPUÉS del commit de la OT y nunca para la hija (desinstalación)
        self.assertGreater(nucleo.index("_ot_vehiculo_reservar("), nucleo.index("conn.commit()"))
        self.assertIn("not es_hija", nucleo[nucleo.index('_veh_req = d.get("vehiculo")'):nucleo.index("_ot_vehiculo_reservar(")])

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
        self.assertNotRegex(t, r"\b(costo|monto|\$)\b")                  # sin plata (REGLAS #19 y #26)

    def test_flota_muestra_de_que_ot_viene_cada_reserva(self):
        js = _leer("static", "flota_uso.js")
        self.assertIn("Para la ' + esc(s.ot.numero)", js)
        self.assertIn("bloqueDet('Orden de trabajo'", js)


if __name__ == "__main__":
    unittest.main()
