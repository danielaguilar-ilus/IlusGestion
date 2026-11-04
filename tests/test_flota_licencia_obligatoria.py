# -*- coding: utf-8 -*-
"""🚚 FLOTA · la licencia escaneada es OBLIGATORIA y los avisos solo le llegan a los gestores REGISTRADOS (Daniel, 04-nov-2026):
«necesito que solo pueda avanzar si el chofer escanea su licencia en el sistema; de lo contrario se podrá realizar el registro solo si agregan
la licencia, esto por seguridad, además de la fecha de término de la licencia» y «creo que la app está enviando mail a todos y debería solo
reconocer los que están registrados».

Decisiones que se vigilan:
  1. Un chofer NO se registra sin su licencia escaneada (foto o PDF) ni sin la fecha de término: ni por el formulario ni por JSON. La licencia
     viaja con el registro; si no se puede guardar, el chofer no queda registrado (se deshace). Sin almacenamiento tampoco se registra.
  2. Un chofer antiguo que no la tenga no se edita (salvo el externo antiguo) ni recibe reservas hasta escanearla: ni por el menú ni desde la OT.
  3. La licencia escaneada no se «quita»: se reemplaza subiendo la nueva (la anterior queda en el historial).
  4. Los correos de la flota solo salen a las personas REGISTRADAS con «Gestionar solicitudes» (por persona o por rol). Ya no hay respaldo a todos los
     superadministradores, y el rol superadmin por sí solo no es un registro. Sin registrados no sale ninguno y /flota lo dice a la vista.
Correr:  python -m pytest tests/test_flota_licencia_obligatoria.py -q -p no:warnings
"""
import io
import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import flota_reglas as R  # noqa: E402
from test_flota_desde_ot import BaseOT  # noqa: E402
from test_flota_solicitudes import (  # noqa: E402
    DIA_ISO, PDF_LICENCIA, SUPER, Base, _app, _datos_chofer, _leer)
from test_flota_vehiculos import BucketFalso  # noqa: E402

SOLO_PEDIR = {"X-Rol": "admin", "X-Flota": "flota_solicitar"}


class _BlobRoto:
    cache_control = None

    def upload_from_string(self, *a, **k):
        raise RuntimeError("GCS caído")


class _BucketRoto:
    def blob(self, clave):
        return _BlobRoto()


class BaseLic(Base):
    def setUp(self):
        super().setUp()
        self.bucket = BucketFalso()
        self.app = _app(self.base, self.bucket)
        self.c = self.app.test_client()

    def form(self, uid=4, archivo=(PDF_LICENCIA, "licencia.pdf", "application/pdf"), **k):
        d = dict(_datos_chofer(uid), usuario_id=uid)
        d.update(k)
        data = {kk: (v if kk == "licencia_clases" else str(v)) for kk, v in d.items() if v is not None}
        if archivo:
            data["archivo"] = (io.BytesIO(archivo[0]), archivo[1], archivo[2])
        return data

    def crear(self, headers=SUPER, **k):
        return self.c.post("/flota/api/choferes", data=self.form(**k), content_type="multipart/form-data", headers=headers)

    def licencias_activas(self, cid):
        return [a for a in self.base.arch.values() if (a["entidad"], a["entidad_id"], a["tipo"]) == ("chofer", cid, "licencia") and a["activo"]]

    def dejar_sin_licencia(self, cid):
        """Un chofer antiguo, registrado antes de que el escaneo fuera obligatorio."""
        for a in self.licencias_activas(cid):
            a["activo"] = 0

    def ficha(self, cid):
        return next(x for x in self.c.get("/flota/api/choferes", headers=SUPER).get_json()["choferes"] if x["id"] == cid)


# ══════════════════════════════════════════════════════════════════
#  1. El registro exige la licencia escaneada y su fecha de término
# ══════════════════════════════════════════════════════════════════
class TestRegistroExigeLicencia(BaseLic):
    def test_sin_licencia_escaneada_no_se_registra(self):
        n_cho, n_arch = len(self.base.cho), len(self.base.arch)
        r = self.crear(archivo=None)
        self.assertEqual(r.status_code, 400, r.get_data(as_text=True))
        self.assertEqual(r.get_json()["errores"]["licencia_archivo"], R.MSG_LICENCIA_SIN_ARCHIVO)
        self.assertEqual((len(self.base.cho), len(self.base.arch)), (n_cho, n_arch))

    def test_en_json_tampoco(self):
        n = len(self.base.cho)
        r = self.c.post("/flota/api/choferes", json=dict(_datos_chofer(4), usuario_id=4), headers={**SUPER, "X-Sin-Licencia": "1"})
        self.assertEqual(r.status_code, 400)
        self.assertIn("licencia_archivo", r.get_json()["errores"])
        self.assertEqual(len(self.base.cho), n)

    def test_un_archivo_que_no_es_una_licencia_se_rechaza(self):
        n = len(self.base.cho)
        for archivo in ((b"MZ\x90 virus", "a.exe", "application/octet-stream"), (b"", "vacio.pdf", "application/pdf")):
            r = self.crear(archivo=archivo)
            self.assertEqual(r.status_code, 400, archivo[1])
            self.assertIn("licencia_archivo", r.get_json()["errores"], archivo[1])
        self.assertEqual(len(self.base.cho), n)
        self.assertEqual(self.bucket.objetos, {})                    # nada se subió

    def test_sin_fecha_de_termino_tampoco_se_registra(self):
        n = len(self.base.cho)
        r = self.crear(licencia_vence=None)
        self.assertEqual(r.status_code, 400)
        self.assertIn("licencia_vence", r.get_json()["errores"])
        self.assertNotIn("licencia_archivo", r.get_json()["errores"])      # el archivo venía bien: solo falta la fecha
        self.assertEqual(len(self.base.cho), n)
        self.assertEqual(self.bucket.objetos, {})

    def test_registro_completo_guarda_al_chofer_y_su_licencia(self):
        r = self.crear()
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        c = r.get_json()["chofer"]
        self.assertTrue(c["tiene_licencia_pdf"])
        self.assertFalse(c["falta_licencia_archivo"])
        self.assertTrue(c["disponible"])
        (a,) = self.licencias_activas(c["id"])
        self.assertTrue(a["gcs_key"].startswith("flota_privado/chofer/"), a["gcs_key"])
        self.assertEqual(self.bucket.objetos[a["gcs_key"]][0], PDF_LICENCIA)

    def test_la_bitacora_anota_la_licencia_dentro_del_creado(self):
        cid = self.crear().get_json()["chofer"]["id"]
        eventos = [x for x in self.base.clog if x["chofer_id"] == cid]
        self.assertEqual([x["accion"] for x in eventos], ["creado"])
        self.assertIn("licencia_escaneada", eventos[0]["detalle"])

    def test_una_foto_del_carnet_tambien_sirve(self):
        png = b"\x89PNG\r\n\x1a\n" + b"\x00" * 64
        r = self.crear(archivo=(png, "carnet.png", "image/png"))
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        self.assertEqual(len(self.licencias_activas(r.get_json()["chofer"]["id"])), 1)

    def test_sin_almacenamiento_no_se_registra_nada(self):
        app = _app(self.base, None)
        n = len(self.base.cho)
        r = app.test_client().post("/flota/api/choferes", data=self.form(), content_type="multipart/form-data", headers=SUPER)
        self.assertEqual(r.status_code, 503)
        self.assertEqual(len(self.base.cho), n)

    def test_si_falla_al_guardar_la_licencia_el_chofer_no_queda_registrado(self):
        app = _app(self.base, _BucketRoto())
        n_cho, n_log, n_arch = len(self.base.cho), len(self.base.clog), len(self.base.arch)
        r = app.test_client().post("/flota/api/choferes", data=self.form(), content_type="multipart/form-data", headers=SUPER)
        self.assertEqual(r.status_code, 502, r.get_data(as_text=True))
        self.assertEqual((len(self.base.cho), len(self.base.clog), len(self.base.arch)), (n_cho, n_log, n_arch))
        self.assertEqual(self.c.get("/flota/api/choferes/usuarios", headers=SUPER).get_json()["usuarios"][0]["id"], 4)   # sigue disponible para registrarse

    def test_solo_el_superadmin_registra(self):
        self.assertEqual(self.crear(headers=SOLO_PEDIR).status_code, 403)


# ══════════════════════════════════════════════════════════════════
#  2. Un chofer antiguo sin licencia escaneada no avanza hasta escanearla
# ══════════════════════════════════════════════════════════════════
class TestChoferAntiguoSinLicencia(BaseLic):
    def setUp(self):
        super().setUp()
        self.dejar_sin_licencia(self.ana)

    def test_la_lista_lo_marca_y_no_lo_deja_disponible(self):
        c = self.ficha(self.ana)
        self.assertTrue(c["falta_licencia_archivo"])
        self.assertFalse(c["tiene_licencia_pdf"])
        self.assertFalse(c["disponible"])
        self.assertTrue(self.ficha(self.beto)["disponible"])

    def test_no_recibe_reservas_hasta_escanearla(self):
        r, j = self.pedir()                                           # el furgón con Ana
        self.assertEqual(r.status_code, 400, j)
        self.assertIn("licencia escaneada", j["error"])
        self.assertEqual(len(self.base.sol), 0)
        r, j = self.pedir(chofer_id=self.beto)                         # con Beto sí
        self.assertEqual(r.status_code, 200, j)

    def test_no_se_edita_sin_adjuntar_la_licencia(self):
        r = self.c.put(f"/flota/api/choferes/{self.ana}", json=_datos_chofer(1, notas="Otra"), headers=SUPER)
        self.assertEqual(r.status_code, 400)
        self.assertEqual(r.get_json()["errores"]["licencia_archivo"], R.MSG_LICENCIA_SIN_ARCHIVO)
        self.assertNotEqual(self.base.cho[self.ana].get("notas"), "Otra")

    def test_al_adjuntarla_se_guarda_y_ya_puede_reservar(self):
        datos = {k: (v if k == "licencia_clases" else str(v)) for k, v in _datos_chofer(1, notas="Con carnet").items()}
        datos["archivo"] = (io.BytesIO(PDF_LICENCIA), "licencia.pdf", "application/pdf")
        r = self.c.put(f"/flota/api/choferes/{self.ana}", data=datos, content_type="multipart/form-data", headers=SUPER)
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        self.assertTrue(r.get_json()["chofer"]["tiene_licencia_pdf"])
        self.assertEqual(self.base.cho[self.ana]["notas"], "Con carnet")
        self.assertEqual(len(self.licencias_activas(self.ana)), 1)
        r2, j2 = self.pedir()
        self.assertEqual(r2.status_code, 200, j2)

    def test_adjuntarla_sin_otros_cambios_tambien_vale(self):
        datos = {k: (v if k == "licencia_clases" else str(v)) for k, v in _datos_chofer(1).items()}
        datos["archivo"] = (io.BytesIO(PDF_LICENCIA), "licencia.pdf", "application/pdf")
        r = self.c.put(f"/flota/api/choferes/{self.ana}", data=datos, content_type="multipart/form-data", headers=SUPER)
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        self.assertEqual(len(self.licencias_activas(self.ana)), 1)
        self.assertEqual(self.base.clog[-1]["accion"], "archivo_subido")


class TestReemplazarYNoQuitar(BaseLic):
    def test_con_licencia_guardada_editar_no_pide_adjuntar_otra(self):
        r = self.c.put(f"/flota/api/choferes/{self.ana}", json=_datos_chofer(1, notas="Otra"), headers=SUPER)
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        self.assertEqual(len(self.licencias_activas(self.ana)), 1)

    def test_adjuntar_otra_reemplaza_y_conserva_la_anterior(self):
        antes = self.licencias_activas(self.ana)[0]
        nueva = PDF_LICENCIA + b"% renovada"
        datos = {k: (v if k == "licencia_clases" else str(v)) for k, v in _datos_chofer(1).items()}
        datos["archivo"] = (io.BytesIO(nueva), "renovada.pdf", "application/pdf")
        r = self.c.put(f"/flota/api/choferes/{self.ana}", data=datos, content_type="multipart/form-data", headers=SUPER)
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        (vigente,) = self.licencias_activas(self.ana)
        self.assertNotEqual(vigente["id"], antes["id"])
        self.assertEqual(self.base.arch[antes["id"]]["activo"], 0)           # la anterior queda en el historial
        self.assertEqual(self.bucket.objetos[vigente["gcs_key"]][0], nueva)

    def test_la_licencia_escaneada_no_se_puede_quitar(self):
        aid = self.licencias_activas(self.ana)[0]["id"]
        r = self.c.post(f"/flota/api/archivos/{aid}/quitar", json={}, headers=SUPER)
        self.assertEqual(r.status_code, 409)
        self.assertEqual(r.get_json()["error_codigo"], "LICENCIA_OBLIGATORIA")
        self.assertEqual(self.base.arch[aid]["activo"], 1)

    def test_quitar_otros_archivos_sigue_funcionando(self):
        r = self.c.post("/flota/api/archivos", headers=SUPER, content_type="multipart/form-data", data={
            "entidad": "vehiculo", "entidad_id": str(self.furgon), "tipo": "soap", "archivo": (io.BytesIO(PDF_LICENCIA), "soap.pdf", "application/pdf")})
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        aid = next(a["id"] for a in self.base.arch.values() if a["tipo"] == "soap")
        self.assertEqual(self.c.post(f"/flota/api/archivos/{aid}/quitar", json={}, headers=SUPER).status_code, 200)


# ══════════════════════════════════════════════════════════════════
#  3. Desde «Nueva OT»: el técnico sin licencia escaneada no aparece como conductor posible
# ══════════════════════════════════════════════════════════════════
class TestDesdeLaOT(BaseOT):
    def test_el_asistente_no_ofrece_a_quien_no_escaneo_su_licencia(self):
        for a in self.base.arch.values():
            if (a["entidad"], a["entidad_id"], a["tipo"]) == ("chofer", self.ana, "licencia"):
                a["activo"] = 0
        por_uid = {c["usuario_id"]: c for c in self.disponibilidad()["choferes"]}
        x = por_uid[1]["por_vehiculo"][str(self.furgon)]
        self.assertFalse(x["ok"])
        self.assertIn("licencia escaneada", x["error"])
        self.assertTrue(por_uid[2]["por_vehiculo"][str(self.furgon)]["ok"])          # Beto sí

    def test_y_reservarla_tampoco_pasa(self):
        for a in self.base.arch.values():
            if (a["entidad"], a["entidad_id"], a["tipo"]) == ("chofer", self.ana, "licencia"):
                a["activo"] = 0
        r = self.reservar()
        self.assertFalse(r["ok"])
        self.assertIn("licencia escaneada", r["error"])
        self.assertEqual(len(self.base.sol), 0)

    def test_con_la_licencia_escaneada_todo_sigue_igual(self):
        self.assertTrue(self.reservar()["ok"])


# ══════════════════════════════════════════════════════════════════
#  4. Los correos de la flota solo salen a los gestores REGISTRADOS
# ══════════════════════════════════════════════════════════════════
class TestAvisosSoloARegistrados(Base):
    def setUp(self):
        super().setUp()
        self.aviso = mock.Mock(return_value=1)
        self.app = _app(self.base, extra={"_aviso_interno_correo": self.aviso})
        self.c = self.app.test_client()

    def destinatarios(self):
        """A quiénes se les avisó de la última solicitud nueva (el menú de Solicitudes de vehículo manda correo; la OT no)."""
        self.aviso.reset_mock()
        r, j = self.pedir()
        self.assertEqual(r.status_code, 200, j)
        if not self.aviso.called:
            return []
        self.assertEqual(self.aviso.call_args[0][0], "flota_solicitud_nueva")
        return self.aviso.call_args[0][1]

    def test_por_defecto_solo_el_gestor_registrado(self):
        self.assertEqual(self.destinatarios(), [2])                  # Beto tiene «Gestionar solicitudes»; Dani no

    def test_el_rol_superadmin_por_si_solo_no_es_un_registro(self):
        self.base.gestores_filas = [
            {"id": 5, "ov": None, "rolp": 1, "rol_slug": "superadmin"},      # su rol «lo tiene todo»: no cuenta
            {"id": 6, "ov": None, "rolp": 1, "rol_slug": "admin"},           # su rol lo marca: registrado
            {"id": 7, "ov": 1, "rolp": None, "rol_slug": "superadmin"},      # marcado A ÉL: registrado
            {"id": 8, "ov": 0, "rolp": 1, "rol_slug": "admin"},              # lo marcado a la persona manda sobre su rol
            {"id": 9, "ov": None, "rolp": None, "rol_slug": "supervisor"},   # nada marcado
        ]
        self.assertEqual(self.destinatarios(), [6, 7])

    def test_si_nadie_esta_registrado_no_sale_ningun_correo(self):
        self.base.gestores_filas = []                                # antes caía en TODOS los superadministradores
        self.assertEqual(self.destinatarios(), [])
        self.aviso.assert_not_called()

    def test_la_cancelacion_de_quien_pidio_avisa_solo_a_los_registrados(self):
        self.base.gestores_filas = [{"id": 6, "ov": 1, "rolp": None, "rol_slug": "admin"},
                                    {"id": 5, "ov": None, "rolp": 1, "rol_slug": "superadmin"}]
        r, j = self.pedir()
        self.aviso.reset_mock()
        r2 = self.c.post(f"/flota/api/solicitudes/{j['solicitud']['id']}/cancelar", json={"motivo": "Ya no hace falta"}, headers=SUPER)
        self.assertEqual(r2.status_code, 200, r2.get_data(as_text=True))
        self.assertEqual(self.aviso.call_args[0][0], "flota_solicitud_cancelada")
        self.assertEqual(self.aviso.call_args[0][1], [6])

    def test_flota_avisa_a_la_vista_cuantos_reciben_los_correos(self):
        j = self.c.get("/flota/api/solicitudes", headers=SUPER).get_json()
        self.assertEqual(j["gestores_avisos"], 1)
        self.base.gestores_filas = []
        j = self.c.get("/flota/api/solicitudes", headers=SUPER).get_json()
        self.assertEqual(j["gestores_avisos"], 0)
        j = self.c.get("/flota/api/solicitudes", headers=SOLO_PEDIR).get_json()
        self.assertIsNone(j["gestores_avisos"])                      # quien solo pide no ve la configuración de los avisos

    def test_el_aviso_esta_en_la_pantalla_y_el_script_lo_usa(self):
        self.assertIn('id="flSolAvisoCorreo"', _leer("templates", "flota", "index.html"))
        js = _leer("static", "flota_uso.js")
        self.assertIn("d.gestores_avisos !== 0", js)
        self.assertIn("flSolAvisoCorreo", js)

    def test_el_registro_de_correos_ya_no_promete_el_respaldo_a_los_superadministradores(self):
        import comm_correos_registro as REG
        for clave in ("flota_solicitud_nueva", "flota_llave_atrasada", "flota_devolucion", "flota_solicitud_cancelada"):
            c = REG.por_clave(clave)
            self.assertIsNotNone(c, clave)
            self.assertNotIn("respaldo", c["avisa_a"].lower(), clave)
            self.assertIn("REGISTRADAS" if clave != "flota_solicitud_cancelada" else "REGISTRADOS", c["avisa_a"], clave)

    def test_el_codigo_ya_no_tiene_el_respaldo_a_todos_los_superadmin(self):
        src = _leer("flota_module.py")
        i = src.index("def _gestores_flota():")
        cuerpo = src[i:src.index("def _correo_flota(", i)]
        self.assertNotIn("role='superadmin'", cuerpo)
        self.assertIn('== "superadmin"', cuerpo)                     # el rol superadmin se excluye del camino por rol


# ══════════════════════════════════════════════════════════════════
#  5. El front
# ══════════════════════════════════════════════════════════════════
class TestFront(unittest.TestCase):
    JS = _leer("static", "flota_uso.js")
    HTML = _leer("templates", "flota", "index.html")

    def test_el_campo_de_la_licencia_es_obligatorio_y_ya_no_dice_opcional(self):
        self.assertNotIn("Licencia en PDF (opcional)", self.HTML)
        self.assertIn('data-campo="licencia_archivo"', self.HTML)
        self.assertIn("Licencia escaneada (foto o PDF)", self.HTML)

    def test_sin_archivo_el_formulario_no_guarda(self):
        self.assertIn("licencia_archivo: 'la licencia escaneada'", self.JS)
        self.assertIn("r.licencia_archivo = res('bad'", self.JS)
        self.assertIn("paso(2, ['licencia_clases', 'licencia_vence', 'licencia_archivo'])", self.JS)

    def test_la_licencia_viaja_con_el_registro_en_una_sola_peticion(self):
        self.assertIn("fd.append('archivo', archivo)", self.JS)
        self.assertIn("form: fd", self.JS)
        self.assertNotIn("subirArchivoCh", self.JS)                  # ya no se sube aparte después de registrar

    def test_ya_no_hay_boton_para_quitar_la_licencia(self):
        self.assertNotIn("quitar-lic", self.JS)
        self.assertNotIn("quitarLicencia", self.JS)

    def test_quien_no_tiene_escaneo_no_se_puede_elegir_como_chofer(self):
        self.assertGreaterEqual(self.JS.count("!c.tiene_licencia_pdf"), 3)       # regla, tarjeta y fila de la lista

    def test_el_archivo_se_acepta_como_foto_o_pdf(self):
        self.assertIn('accept="application/pdf,image/jpeg,image/png,image/webp"', self.HTML)


if __name__ == "__main__":
    unittest.main()
