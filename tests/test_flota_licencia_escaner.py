# -*- coding: utf-8 -*-
"""🚚 FLOTA · el escáner de licencias CONECTADO al registro de choferes (rutas y control al guardar).

Daniel, 05-nov-2026: «debemos tener restricciones para las malas operaciones que logística está generando… un scanner de licencias… por el
celular o subiendo una foto… y sobre todo la fecha, así evitamos que Milagros cree choferes con su usuario. Fíjate que dice "fecha de control
29/10/2029"». Eligió el OCR propio (Tesseract en el servidor).

Aquí el lector es de mentira (devuelve la lectura que cada prueba prepara): lo que se prueba es lo que hace el SERVIDOR con ella.
  1. Al registrar o editar un chofer con licencia nueva, el servidor la lee DE NUEVO (no confía en el navegador) y exige que el RUT sea el del
     chofer, que el usuario de ILUS sea esa misma persona (el caso de Milagros), que no esté vencida y que las clases estén en la licencia.
  2. El vencimiento sale de la licencia: lo escrito a mano se corrige. Con una licencia ya escaneada, RUT/clases/vencimiento no se reescriben a mano.
  3. Si el lector no puede leer o no está, no se registra; solo el superadmin puede saltarse la comprobación, con un motivo escrito que queda anotado.
  4. leer / comparar / estado: solo quien administra, no guardan nada y tienen ritmo máximo.

Correr:  python -m pytest tests/test_flota_licencia_escaner.py -q -p no:warnings
"""
import datetime as dt
import io
import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import flota_licencia_lector as L  # noqa: E402
import flota_licencia_ocr as O  # noqa: E402
from test_flota_solicitudes import (  # noqa: E402
    FUTURO, PDF_LICENCIA, SUPER, Base, _app, _datos_chofer, _leer, _rut)
from test_flota_vehiculos import BucketFalso  # noqa: E402

ADMIN = {"X-Rol": "admin", "X-Flota": "flota_administrar"}
SOLO_PEDIR = {"X-Rol": "admin", "X-Flota": "flota_solicitar"}
MODULO = _leer("flota_module.py")
JS = _leer("static", "flota_uso.js")


def texto_licencia(rut, nombres="DANIELA", apellidos="MORA VEGA", vence=None, clase="B"):
    vence = vence or dt.date.fromisoformat(FUTURO)
    return (f"LICENCIA DE CONDUCTOR\nCLASE {clase}\nN DE LICENCIA {L.fr.rut_formato(rut)}\nNOMBRES {nombres}\nAPELLIDOS {apellidos}\n"
            f"FECHA ULTIMO CONTROL 16/06/2023\nFECHA DE CONTROL {vence.strftime('%d/%m/%Y')}\n")


class BaseEscaner(Base):
    def setUp(self):
        self.base_falsa_ok = True
        from test_flota_solicitudes import Falsa
        self.base = Falsa()
        self.bucket = BucketFalso()
        self.lectura = None
        self.error_lector = None
        self.leidos = []
        self.app = _app(self.base, self.bucket, extra={"licencia_verificar": True, "licencia_leer": self._lector})
        self.c = self.app.test_client()
        self.base.usr[4]["rut"] = _datos_chofer(4)["rut"]            # el perfil de Daniela ya trae SU RUT (lo normal); cada prueba cambia lo que necesita

    # ── el lector de mentira
    def _lector(self, data):
        self.leidos.append(bytes(data))
        if self.error_lector:
            raise self.error_lector
        return self.lectura, {"motor": "falso", "ms": 3}

    def lee(self, texto):
        self.lectura = L.interpretar_texto(texto)

    # ── el mundo: Daniela (usuario 4) ya tiene su licencia a la mano
    def form(self, uid=4, archivo=(PDF_LICENCIA, "licencia.pdf", "application/pdf"), **k):
        d = dict(_datos_chofer(uid), usuario_id=uid)
        d.update(k)
        data = {kk: (v if kk == "licencia_clases" else str(v)) for kk, v in d.items() if v is not None}
        if archivo:
            data["archivo"] = (io.BytesIO(archivo[0]), archivo[1], archivo[2])
        return data

    def crear(self, headers=SUPER, **k):
        return self.c.post("/flota/api/choferes", data=self.form(**k), content_type="multipart/form-data", headers=headers)

    def editar(self, cid, headers=SUPER, uid=4, archivo=None, **k):
        d = self.form(uid=uid, archivo=archivo, **k)
        d.pop("usuario_id", None)
        return self.c.put(f"/flota/api/choferes/{cid}", data=d, content_type="multipart/form-data", headers=headers)

    def registrar_daniela(self):
        self.lee(texto_licencia(_datos_chofer(4)["rut"]))
        r = self.crear()
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        return r.get_json()["chofer"]["id"]

    def log(self, cid):
        return [(x["accion"], json.loads(x["detalle"] or "null")) for x in self.base.clog if x["chofer_id"] == cid]


# ══════════════════════════════════════════════════════════════════
#  1. Registrar un chofer: la licencia tiene que ser de esa persona
# ══════════════════════════════════════════════════════════════════
class TestRegistroLeeYExige(BaseEscaner):
    def test_licencia_que_coincide_registra_al_chofer(self):
        self.lee(texto_licencia(_datos_chofer(4)["rut"]))
        r = self.crear()
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        self.assertEqual(len(self.leidos), 1)
        self.assertEqual(self.leidos[0], PDF_LICENCIA)                  # el servidor leyó EL ARCHIVO que llegó, no un dato del navegador

    def test_la_fecha_de_control_tiene_que_ser_una_de_las_que_se_vieron_en_la_foto(self):
        vence_real = dt.date.fromisoformat(FUTURO) - dt.timedelta(days=40)
        self.lee(texto_licencia(_datos_chofer(4)["rut"], vence=vence_real))
        n = len(self.base.cho)
        r = self.crear(licencia_vence=FUTURO)                           # una fecha más lejana que la persona inventó: NO está en la licencia
        self.assertEqual(r.status_code, 400)
        j = r.get_json()
        self.assertEqual(j["error_codigo"], "LICENCIA_NO_COINCIDE")
        self.assertIn("no aparece en la licencia escaneada", j["errores"]["licencia_vence"])
        self.assertEqual(len(self.base.cho), n)
        self.assertEqual(self.bucket.objetos, {})
        r = self.crear(licencia_vence=vence_real.isoformat())          # la que dice la foto sí
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        self.assertEqual(self.base.cho[r.get_json()["chofer"]["id"]]["licencia_vence"], vence_real)

    def test_la_persona_puede_elegir_otra_de_las_fechas_leidas_y_queda_anotado(self):
        control, ultimo = dt.date.fromisoformat(FUTURO), dt.date.fromisoformat(FUTURO) - dt.timedelta(days=100)
        rut = L.fr.rut_formato(_datos_chofer(4)["rut"])
        self.lectura = L.fusionar([L.interpretar_texto(f"N DE LICENCIA {rut}\nCLASE B\nFECHA ULTIMO CONTROL {ultimo.strftime('%d/%m/%Y')}\n"
                                                       f"FECHA DE CONTROL {control.strftime('%d/%m/%Y')}")] * 2)
        r = self.crear(licencia_vence=ultimo.isoformat())              # eligió la otra fecha de las dos que se leyeron
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        cid = r.get_json()["chofer"]["id"]
        self.assertEqual(self.base.cho[cid]["licencia_vence"], ultimo)
        det = [x for x in self.log(cid) if x[0] == "creado"][0][1]
        self.assertEqual(det["vence_elegida"], ultimo.isoformat())
        self.assertEqual(det["licencia_leida"]["vence"], control.isoformat())
        self.assertIn(control.isoformat(), det["licencia_leida"]["fechas"])

    def test_un_rut_con_una_cifra_mal_leida_igual_registra_si_coincide(self):
        rut_chofer = _datos_chofer(4)["rut"]                            # 10000004-x: el OCR leyó el primer 1 como 7 (cifras que se confunden)
        mal = "7" + rut_chofer[1:]
        vence = dt.date.fromisoformat(FUTURO)
        texto = "N DE LICENCIA " + mal + "\nFECHA DE CONTROL " + vence.strftime("%d/%m/%Y")
        self.lectura = L.fusionar([L.interpretar_texto(texto)] * 2)
        self.assertTrue(L.rut_parecido(mal, rut_chofer))
        r = self.crear()
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        det = [x for x in self.log(r.get_json()["chofer"]["id"]) if x[0] == "creado"][0][1]
        self.assertTrue(any("cifra dudosa" in a for a in det["avisos"]))

    def test_un_rut_leido_de_otra_persona_no_se_confunde_con_el_del_chofer(self):
        self.lectura = L.fusionar([L.interpretar_texto(texto_licencia(_rut(9000001)))] * 2)
        r = self.crear()
        self.assertEqual(r.status_code, 400)
        self.assertIn("rut", r.get_json()["errores"])

    def test_el_RUT_de_la_licencia_no_es_el_del_chofer(self):
        self.lee(texto_licencia(_rut(9000001)))
        n = len(self.base.cho)
        r = self.crear()
        self.assertEqual(r.status_code, 400)
        j = r.get_json()
        self.assertEqual(j["error_codigo"], "LICENCIA_NO_COINCIDE")
        self.assertIn("rut", j["errores"])
        self.assertEqual(j["lectura"]["rut"], _rut(9000001).replace("-", "-"))
        self.assertEqual(len(self.base.cho), n)
        self.assertEqual(self.bucket.objetos, {})                       # nada se subió

    def test_el_caso_de_milagros_otra_persona_colgada_de_su_usuario(self):
        # Se registra a José con SU licencia (RUT de José) pero colgado del usuario 4 (otra persona, con su propio RUT en el perfil).
        rut_jose = _rut(13375664)
        self.base.usr[4]["rut"] = _datos_chofer(4)["rut"]               # el RUT real del dueño del usuario
        self.lee(texto_licencia(rut_jose, nombres="JOSE ALFREDO", apellidos="SOTO"))
        n = len(self.base.cho)
        r = self.crear(rut=rut_jose, nombre_completo="José Alfredo Soto")
        self.assertEqual(r.status_code, 400)
        j = r.get_json()
        self.assertEqual(j["error_codigo"], "LICENCIA_NO_COINCIDE")
        self.assertIn("usuario_id", j["errores"])
        self.assertIn("propio usuario", j["errores"]["usuario_id"])
        self.assertNotIn("rut", j["errores"])                           # la licencia sí era de José: lo que no cuadra es el usuario
        self.assertEqual(len(self.base.cho), n)
        self.assertEqual(self.bucket.objetos, {})

    def test_usuario_sin_rut_en_el_perfil_y_con_otro_nombre_no_pasa(self):
        self.base.usr[4]["rut"] = None
        rut_jose = _rut(13375664)
        self.lee(texto_licencia(rut_jose, nombres="JOSE ALFREDO", apellidos="SOTO"))
        r = self.crear(rut=rut_jose, nombre_completo="José Alfredo Soto")
        self.assertEqual(r.status_code, 400)
        self.assertIn("usuario_id", r.get_json()["errores"])

    def test_usuario_sin_rut_en_el_perfil_pero_con_el_mismo_nombre_pasa_y_deja_aviso(self):
        self.base.usr[4].update(rut=None, nombre="Daniela Mora Vega")          # sin RUT en el perfil se piden 3 palabras del nombre
        self.lee(texto_licencia(_datos_chofer(4)["rut"], nombres="DANIELA", apellidos="MORA VEGA"))
        r = self.crear()
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        cid = r.get_json()["chofer"]["id"]
        det = [x for x in self.log(cid) if x[0] == "creado"][0][1]
        self.assertTrue(any("no tiene RUT" in a for a in det["avisos"]))

    def test_licencia_vencida_no_se_registra(self):
        self.lee(texto_licencia(_datos_chofer(4)["rut"], vence=dt.date.today() - dt.timedelta(days=3)))
        r = self.crear(licencia_vence=FUTURO)
        self.assertEqual(r.status_code, 400)
        self.assertIn("licencia_vence", r.get_json()["errores"])

    def test_clase_que_la_licencia_no_trae(self):
        self.lee(texto_licencia(_datos_chofer(4)["rut"], clase="B"))
        r = self.crear(licencia_clases=["A4", "B"])
        self.assertEqual(r.status_code, 400)
        self.assertIn("licencia_clases", r.get_json()["errores"])

    def test_licencia_ilegible_no_se_registra(self):
        self.lee("borroso")
        n = len(self.base.cho)
        r = self.crear()
        self.assertEqual(r.status_code, 400)
        j = r.get_json()
        self.assertEqual(j["error_codigo"], "LICENCIA_ILEGIBLE")
        self.assertIn("No pude leer la licencia", j["errores"]["licencia_archivo"])
        self.assertEqual(len(self.base.cho), n)

    def test_archivo_que_el_lector_no_puede_abrir(self):
        self.error_lector = O.ArchivoIlegible("No es una foto ni un PDF que pueda leer.")
        r = self.crear()
        self.assertEqual(r.status_code, 400)
        self.assertEqual(r.get_json()["error_codigo"], "LICENCIA_ILEGIBLE")
        self.assertEqual(self.bucket.objetos, {})

    def test_lector_no_disponible_no_registra_y_lo_dice(self):
        self.error_lector = O.MotorNoDisponible("Tesseract no está instalado.")
        n = len(self.base.cho)
        r = self.crear(headers=ADMIN)
        self.assertEqual(r.status_code, 503)
        j = r.get_json()
        self.assertEqual(j["error_codigo"], "LECTOR_NO_DISPONIBLE")
        self.assertFalse(j["puede_omitir"])
        self.assertEqual(len(self.base.cho), n)
        r2 = self.crear(headers=SUPER)
        self.assertTrue(r2.get_json()["puede_omitir"])                  # el superadmin ve que tiene la salida del motivo

    def test_un_error_inesperado_del_lector_no_registra_ni_se_traga(self):
        self.error_lector = RuntimeError("boom")
        n = len(self.base.cho)
        r = self.crear()
        self.assertEqual(r.status_code, 500)
        self.assertEqual(len(self.base.cho), n)
        self.assertNotIn("boom", r.get_data(as_text=True))              # no se le muestra el detalle interno (REGLA #4)


# ══════════════════════════════════════════════════════════════════
#  2. Solo el superadmin puede saltarse la comprobación, y deja constancia
# ══════════════════════════════════════════════════════════════════
class TestSaltarseLaComprobacion(BaseEscaner):
    MOTIVO = "El lector no puede con esta foto vieja; revisé la licencia en persona"

    def test_el_superadmin_con_motivo_registra_sin_leer_y_queda_anotado(self):
        self.lee("borroso")
        r = self.crear(licencia_sin_verificar_motivo=self.MOTIVO)
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        self.assertEqual(self.leidos, [])                               # ni siquiera lo intentó
        cid = r.get_json()["chofer"]["id"]
        det = [x for x in self.log(cid) if x[0] == "creado"][0][1]
        self.assertEqual(det["licencia_sin_verificar"], self.MOTIVO)

    def test_motivo_corto_no_vale(self):
        self.lee("borroso")
        r = self.crear(licencia_sin_verificar_motivo="porque sí")
        self.assertEqual(r.status_code, 400)

    def test_quien_solo_administra_no_puede_saltarsela_aunque_escriba_motivo(self):
        self.lee("borroso")
        n = len(self.base.cho)
        r = self.crear(headers=ADMIN, licencia_sin_verificar_motivo=self.MOTIVO)
        self.assertEqual(r.status_code, 400)
        self.assertEqual(len(self.base.cho), n)

    def test_tampoco_con_el_lector_caido(self):
        self.error_lector = O.MotorNoDisponible("x")
        r = self.crear(headers=ADMIN, licencia_sin_verificar_motivo=self.MOTIVO)
        self.assertEqual(r.status_code, 503)
        r2 = self.crear(headers=SUPER, licencia_sin_verificar_motivo=self.MOTIVO)
        self.assertEqual(r2.status_code, 200, r2.get_data(as_text=True))


# ══════════════════════════════════════════════════════════════════
#  3. Editar: el RUT, las clases y el vencimiento salen de la licencia
# ══════════════════════════════════════════════════════════════════
class TestEditar(BaseEscaner):
    def test_cambiar_el_vencimiento_a_mano_con_licencia_ya_escaneada_no_se_puede(self):
        cid = self.registrar_daniela()
        nueva = (dt.date.fromisoformat(FUTURO) + dt.timedelta(days=200)).isoformat()
        r = self.editar(cid, licencia_vence=nueva)
        self.assertEqual(r.status_code, 400, r.get_data(as_text=True))
        self.assertEqual(r.get_json()["error_codigo"], "LICENCIA_REQUIERE_ESCANEO")
        self.assertEqual(self.base.cho[cid]["licencia_vence"], dt.date.fromisoformat(FUTURO))

    def test_cambiar_el_rut_a_mano_tampoco(self):
        cid = self.registrar_daniela()
        r = self.editar(cid, rut=_rut(9000001))
        self.assertEqual(r.status_code, 400)
        self.assertEqual(r.get_json()["error_codigo"], "LICENCIA_REQUIERE_ESCANEO")

    def test_cambiar_otras_cosas_si_se_puede(self):
        cid = self.registrar_daniela()
        r = self.editar(cid, notas="Prefiere salir temprano")
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        self.assertEqual(self.base.cho[cid]["notas"], "Prefiere salir temprano")

    def test_renovar_escaneando_la_licencia_nueva_actualiza_el_vencimiento(self):
        cid = self.registrar_daniela()
        nueva = dt.date.today() + dt.timedelta(days=1800)
        self.lee(texto_licencia(_datos_chofer(4)["rut"], vence=nueva))
        r = self.editar(cid, archivo=(PDF_LICENCIA, "licencia2.pdf", "application/pdf"), licencia_vence=nueva.isoformat())
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        self.assertEqual(self.base.cho[cid]["licencia_vence"], nueva)             # la fecha la puso la licencia

    def test_al_renovar_la_fecha_vieja_ya_no_vale_porque_no_esta_en_la_licencia_nueva(self):
        cid = self.registrar_daniela()
        nueva = dt.date.today() + dt.timedelta(days=1800)
        self.lee(texto_licencia(_datos_chofer(4)["rut"], vence=nueva))
        r = self.editar(cid, archivo=(PDF_LICENCIA, "licencia2.pdf", "application/pdf"), licencia_vence=FUTURO)    # quedó la fecha de la licencia anterior
        self.assertEqual(r.status_code, 400)
        self.assertIn("licencia_vence", r.get_json()["errores"])
        self.assertEqual(self.base.cho[cid]["licencia_vence"], dt.date.fromisoformat(FUTURO))

    def test_escanear_una_licencia_de_otra_persona_al_editar_se_rechaza(self):
        cid = self.registrar_daniela()
        self.lee(texto_licencia(_rut(9000001)))
        r = self.editar(cid, archivo=(PDF_LICENCIA, "otra.pdf", "application/pdf"))
        self.assertEqual(r.status_code, 400)
        self.assertIn("rut", r.get_json()["errores"])
        self.assertEqual(self.base.cho[cid]["rut"], _datos_chofer(4)["rut"])

    def test_el_superadmin_con_motivo_corrige_a_mano_y_queda_anotado(self):
        cid = self.registrar_daniela()
        nueva = (dt.date.fromisoformat(FUTURO) + dt.timedelta(days=30)).isoformat()
        r = self.editar(cid, licencia_vence=nueva, licencia_sin_verificar_motivo="La licencia se renovó y el escaneo se perdió; ya la vi")
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        self.assertEqual(self.base.cho[cid]["licencia_vence"], dt.date.fromisoformat(nueva))
        det = [x for x in self.log(cid) if x[0] == "editado"][-1][1]
        self.assertIn("licencia_sin_verificar", det)


# ══════════════════════════════════════════════════════════════════
#  4. Las rutas del escáner: leer, comparar, estado
# ══════════════════════════════════════════════════════════════════
class TestRutasDelEscaner(BaseEscaner):
    def leer(self, headers=SUPER, archivo=(PDF_LICENCIA, "licencia.pdf", "application/pdf"), **campos):
        data = {k: str(v) for k, v in campos.items()}
        if archivo:
            data["archivo"] = (io.BytesIO(archivo[0]), archivo[1], archivo[2])
        return self.c.post("/flota/api/licencia/leer", data=data, content_type="multipart/form-data", headers=headers)

    def test_leer_devuelve_los_datos_y_no_guarda_nada(self):
        self.lee(texto_licencia(_datos_chofer(4)["rut"]))
        filas, objetos = len(self.base.cho), dict(self.bucket.objetos)
        r = self.leer(usuario_id=4, rut=_datos_chofer(4)["rut"])
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        j = r.get_json()
        self.assertEqual(j["lectura"]["rut"], _datos_chofer(4)["rut"])
        self.assertEqual(j["lectura"]["vence"], FUTURO)
        self.assertEqual(j["lectura"]["clases"], ["B"])
        self.assertEqual(j["lectura"]["nombre"], "DANIELA MORA VEGA")
        self.assertTrue(j["verificacion"]["ok"])
        self.assertEqual(j["motor"], "falso")
        self.assertEqual((len(self.base.cho), self.bucket.objetos, len(self.base.arch)), (filas, objetos, 0))

    def test_leer_dice_enseguida_si_el_usuario_elegido_no_es_el_dueno(self):
        self.base.usr[4]["rut"] = _rut(9000001)
        self.lee(texto_licencia(_datos_chofer(4)["rut"]))
        r = self.leer(usuario_id=4, rut=_datos_chofer(4)["rut"])
        v = r.get_json()["verificacion"]
        self.assertFalse(v["ok"])
        self.assertIn("usuario_id", v["errores"])

    def test_leer_sin_archivo_o_con_basura(self):
        self.assertEqual(self.leer(archivo=None).status_code, 400)
        self.assertEqual(self.leer(archivo=(b"MZ\x90 virus", "a.exe", "application/octet-stream")).status_code, 400)
        self.assertEqual(self.leidos, [])

    def test_leer_con_lector_caido(self):
        self.error_lector = O.MotorNoDisponible("x")
        r = self.leer()
        self.assertEqual(r.status_code, 503)
        self.assertEqual(r.get_json()["error_codigo"], "LECTOR_NO_DISPONIBLE")

    def test_leer_una_foto_ilegible_responde_con_su_motivo(self):
        self.error_lector = O.ArchivoIlegible("No pude abrir el archivo.")
        r = self.leer()
        self.assertEqual(r.status_code, 400)
        self.assertIn("No pude abrir el archivo", r.get_json()["error"])

    def test_solo_quien_administra_puede_escanear(self):
        self.lee(texto_licencia(_datos_chofer(4)["rut"]))
        for headers in (SOLO_PEDIR, {}):
            self.assertIn(self.leer(headers=headers).status_code, (302, 403))
        self.assertEqual(self.leidos, [])
        self.assertEqual(self.leer(headers=ADMIN).status_code, 200)

    def test_ritmo_maximo_por_persona(self):
        self.lee(texto_licencia(_datos_chofer(4)["rut"]))
        codigos = [self.leer().status_code for _ in range(14)]
        self.assertEqual(codigos[:12], [200] * 12)
        self.assertEqual(codigos[12:], [429, 429])
        self.assertEqual(len(self.leidos), 12)                          # las rechazadas no gastaron OCR

    def test_comparar_no_repite_el_ocr(self):
        self.base.usr[4]["rut"] = _rut(9000001)
        lectura = {"rut": _datos_chofer(4)["rut"], "vence": FUTURO, "clases": ["B"], "nombre": "DANIELA MORA VEGA"}
        r = self.c.post("/flota/api/licencia/comparar", json={"lectura": lectura, "usuario_id": 4, "rut": _datos_chofer(4)["rut"]}, headers=SUPER)
        self.assertEqual(r.status_code, 200)
        self.assertIn("usuario_id", r.get_json()["verificacion"]["errores"])
        self.assertEqual(self.leidos, [])
        r2 = self.c.post("/flota/api/licencia/comparar", json={"lectura": dict(lectura, rut="12345678-9"), "usuario_id": 4}, headers=SUPER)
        self.assertIn("licencia_archivo", r2.get_json()["verificacion"]["errores"])      # un RUT que no existe no cuenta como lectura

    def test_estado_del_lector(self):
        r = self.c.get("/flota/api/licencia/estado", headers=SUPER)
        self.assertEqual(r.status_code, 200)
        self.assertIn("disponible", r.get_json())
        self.assertEqual(self.c.get("/flota/api/licencia/estado", headers=SOLO_PEDIR).status_code, 403)


# ══════════════════════════════════════════════════════════════════
#  4b. La constancia se VE en el historial del chofer
# ══════════════════════════════════════════════════════════════════
class TestHistorial(BaseEscaner):
    def historial(self, cid):
        return self.c.get(f"/flota/api/choferes/{cid}/historial", headers=SUPER).get_json()["eventos"]

    def test_el_historial_cuenta_lo_que_leyo_el_escaner_y_la_fecha_elegida(self):
        control, ultimo = dt.date.fromisoformat(FUTURO), dt.date.fromisoformat(FUTURO) - dt.timedelta(days=100)
        rut = L.fr.rut_formato(_datos_chofer(4)["rut"])
        self.lectura = L.fusionar([L.interpretar_texto(f"N DE LICENCIA {rut}\nFECHA ULTIMO CONTROL {ultimo.strftime('%d/%m/%Y')}\n"
                                                       f"FECHA DE CONTROL {control.strftime('%d/%m/%Y')}")] * 2)
        cid = self.crear(licencia_vence=ultimo.isoformat()).get_json()["chofer"]["id"]
        lineas = " ".join(self.historial(cid)[-1]["lineas"])
        self.assertIn("La licencia se leyó con el escáner", lineas)
        self.assertIn("vence el " + control.strftime("%d/%m/%Y"), lineas)
        self.assertIn("La persona eligió " + ultimo.strftime("%d/%m/%Y"), lineas)

    def test_el_historial_dice_que_se_registro_sin_comprobar_y_el_motivo(self):
        self.lee("borroso")
        motivo = "El lector no puede con esta foto vieja; revisé la licencia en persona"
        cid = self.crear(licencia_sin_verificar_motivo=motivo).get_json()["chofer"]["id"]
        lineas = " ".join(self.historial(cid)[-1]["lineas"])
        self.assertIn("SIN comprobar", lineas)
        self.assertIn(motivo, lineas)


# ══════════════════════════════════════════════════════════════════
#  7. Revisión de seguridad independiente (05-nov-2026)
# ══════════════════════════════════════════════════════════════════
class TestRevisionDeSeguridad(BaseEscaner):
    def test_la_ruta_generica_de_archivos_no_sirve_para_saltarse_el_escaner(self):
        cid = self.registrar_daniela()
        r = self.c.post("/flota/api/archivos", headers=SUPER, content_type="multipart/form-data",
                        data={"entidad": "chofer", "entidad_id": str(cid), "tipo": "licencia", "archivo": (io.BytesIO(PDF_LICENCIA), "otra.pdf", "application/pdf")})
        self.assertEqual(r.status_code, 409)
        self.assertEqual(r.get_json()["error_codigo"], "LICENCIA_SOLO_POR_FORMULARIO")
        self.assertEqual(len([a for a in self.base.arch.values() if a["entidad"] == "chofer" and a["tipo"] == "licencia" and a["activo"]]), 1)

    def test_los_demas_archivos_siguen_subiendose_por_esa_ruta(self):
        r = self.c.post("/flota/api/vehiculos", json={"patente": "ZZ9999", "tipo": "Furgón", "marca": "X", "modelo": "Y", "licencia_clases": ["B"],
                        "revision_tecnica_vence": FUTURO, "permiso_circulacion_vence": FUTURO, "soap_vence": FUTURO, "seguro_vence": FUTURO}, headers=SUPER)
        vid = r.get_json()["vehiculo"]["id"]
        r = self.c.post("/flota/api/archivos", headers=SUPER, content_type="multipart/form-data",
                        data={"entidad": "vehiculo", "entidad_id": str(vid), "tipo": "soap", "archivo": (io.BytesIO(PDF_LICENCIA), "soap.pdf", "application/pdf")})
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))

    def test_si_la_base_falla_al_buscar_al_usuario_no_se_salta_la_comprobacion(self):
        original = self.base.fetchone

        def falla(q, p=None):
            if "SELECT id, nombre, rut FROM" in q:
                raise RuntimeError("base caída")
            return original(q, p)
        self.app = _app(self.base, self.bucket, extra={"licencia_verificar": True, "licencia_leer": self._lector, "mysql_fetchone": falla})
        self.c = self.app.test_client()
        self.lee(texto_licencia(_datos_chofer(4)["rut"]))
        n = len(self.base.cho)
        r = self.crear()
        self.assertEqual(r.status_code, 503)
        self.assertEqual(r.get_json()["error_codigo"], "USUARIO_NO_COMPROBADO")
        self.assertEqual(len(self.base.cho), n)

    def test_el_lector_ocupado_responde_429_y_no_cuelga_la_aplicacion(self):
        import threading
        espera, dentro = threading.Event(), threading.Barrier(3, timeout=10)
        self.lee(texto_licencia(_datos_chofer(4)["rut"]))
        original = self._lector
        resultados = []

        def lento(data):
            try:
                dentro.wait()
            except threading.BrokenBarrierError:
                pass
            espera.wait(10)
            return original(data)
        self.app = _app(self.base, self.bucket, extra={"licencia_verificar": True, "licencia_leer": lento})

        def pedir():
            c = self.app.test_client()
            r = c.post("/flota/api/licencia/leer", data={"archivo": (io.BytesIO(PDF_LICENCIA), "l.pdf", "application/pdf")},
                       content_type="multipart/form-data", headers=ADMIN)
            resultados.append(r.status_code)
        hilos = [threading.Thread(target=pedir) for _ in range(2)]
        for h in hilos:
            h.start()
        try:
            dentro.wait()                                              # las dos lecturas ya están dentro del lector
        except threading.BrokenBarrierError:
            pass
        c3 = self.app.test_client()
        r3 = c3.post("/flota/api/licencia/leer", data={"archivo": (io.BytesIO(PDF_LICENCIA), "l.pdf", "application/pdf")},
                     content_type="multipart/form-data", headers={"X-Rol": "admin", "X-Flota": "flota_administrar"})
        espera.set()
        for h in hilos:
            h.join(15)
        self.assertEqual(r3.status_code, 429)
        self.assertEqual(r3.get_json()["error_codigo"], "LECTOR_OCUPADO")
        self.assertEqual(sorted(resultados), [200, 200])

    def test_comparar_tiene_ritmo_maximo_y_no_guarda_cache(self):
        lectura = {"rut": _datos_chofer(4)["rut"], "vence": FUTURO, "clases": ["B"]}
        codigos = []
        for _ in range(14):
            r = self.c.post("/flota/api/licencia/comparar", json={"lectura": lectura, "usuario_id": 4}, headers=SUPER)
            codigos.append(r.status_code)
        self.assertEqual(codigos[:12], [200] * 12)
        self.assertEqual(codigos[12:], [429, 429])
        r = self.c.post("/flota/api/licencia/leer", data={"archivo": (io.BytesIO(PDF_LICENCIA), "l.pdf", "application/pdf")},
                        content_type="multipart/form-data", headers=SUPER)
        self.assertEqual(r.status_code, 429)                           # el ritmo es el mismo para leer y comparar

    def test_las_respuestas_con_rut_no_se_guardan_en_cache(self):
        self.lee(texto_licencia(_datos_chofer(4)["rut"]))
        r = self.c.post("/flota/api/licencia/leer", data={"archivo": (io.BytesIO(PDF_LICENCIA), "l.pdf", "application/pdf")},
                        content_type="multipart/form-data", headers=SUPER)
        self.assertEqual(r.headers.get("Cache-Control"), "no-store")
        r = self.c.post("/flota/api/licencia/comparar", json={"lectura": {"rut": _datos_chofer(4)["rut"], "vence": FUTURO}}, headers=SUPER)
        self.assertEqual(r.headers.get("Cache-Control"), "no-store")

    def test_el_mensaje_no_repite_el_rut_del_perfil_de_otra_persona(self):
        self.base.usr[4]["rut"] = _rut(9000001)
        lectura = {"rut": _datos_chofer(4)["rut"], "vence": FUTURO, "clases": ["B"]}
        r = self.c.post("/flota/api/licencia/comparar", json={"lectura": lectura, "usuario_id": 4, "rut": _datos_chofer(4)["rut"]}, headers=SUPER)
        msg = r.get_json()["verificacion"]["errores"]["usuario_id"]
        self.assertNotIn(L.fr.rut_formato(_rut(9000001)), msg)
        self.assertNotIn(_rut(9000001), msg)

    def test_un_usuario_desactivado_no_se_usa_para_comparar(self):
        self.assertIn("WHERE id=%s AND active=1", MODULO[MODULO.index("def _lic_usuario"):MODULO.index("def _lic_usuario") + 900])

    def test_el_historial_no_cuenta_lo_leido_a_quien_solo_gestiona(self):
        cid = self.registrar_daniela()
        gestor = {"X-Rol": "admin", "X-Flota": "flota_gestionar"}
        ev = self.c.get(f"/flota/api/choferes/{cid}/historial", headers=gestor).get_json()["eventos"]
        self.assertTrue(ev)
        self.assertNotIn("escáner", " ".join(" ".join(e["lineas"]) for e in ev))
        self.assertNotIn(L.fr.rut_formato(_datos_chofer(4)["rut"]), " ".join(" ".join(e["lineas"]) for e in ev))
        ev_admin = self.c.get(f"/flota/api/choferes/{cid}/historial", headers=SUPER).get_json()["eventos"]
        self.assertIn("escáner", " ".join(" ".join(e["lineas"]) for e in ev_admin))

    def test_omitir_la_comprobacion_exige_un_motivo_razonable_y_queda_con_su_propio_nombre(self):
        self.lee("borroso")
        for motivo in ("a" * 25, "abcdefghijklmnopqrstu vwxyz"):               # 25 letras seguidas · dos palabras largas: no es una explicación
            self.assertEqual(self.crear(licencia_sin_verificar_motivo=motivo).status_code, 400, motivo)
        largo = "motivo largo " * 40
        cid = self.crear(licencia_sin_verificar_motivo=largo).get_json()["chofer"]["id"]
        det = [x for x in self.log(cid) if x[0] == "creado"][0][1]
        self.assertLessEqual(len(det["licencia_sin_verificar"]), 300)
        self.assertIn("Licencia registrada SIN comprobar", MODULO)

    def test_una_fecha_de_control_demasiado_lejana_se_rechaza(self):
        lejana = dt.date.today() + dt.timedelta(days=366 * 9)
        self.lee(texto_licencia(_datos_chofer(4)["rut"], vence=lejana))
        r = self.crear(licencia_vence=lejana.isoformat())
        self.assertEqual(r.status_code, 400)
        self.assertIn("demasiado lejana", r.get_json()["errores"]["licencia_vence"])


# ══════════════════════════════════════════════════════════════════
#  5. Cableado
# ══════════════════════════════════════════════════════════════════
class TestCableado(unittest.TestCase):
    def test_el_servidor_exige_en_el_alta_y_en_la_edicion(self):
        self.assertEqual(MODULO.count("resp_lic, det_lic = _licencia_exigir(data_lic, payload, datos,"), 2)
        i = MODULO.index("def flota_api_chofer_crear")
        j = MODULO.index("_licencia_exigir(data_lic, payload, datos,", i)
        self.assertLess(MODULO.index("_bucket() is None", i), j)                    # primero lo barato, después el OCR
        self.assertLess(j, MODULO.index('"INSERT INTO flota_choferes', i))          # y siempre ANTES de guardar al chofer

    def test_la_comprobacion_viene_encendida_en_produccion(self):
        self.assertIn('return bool(ctx.get("licencia_verificar", True))', MODULO)

    def test_solo_el_superadmin_puede_omitir_y_con_motivo_largo(self):
        i = MODULO.index("def _lic_motivo_omitir")
        bloque = MODULO[i:i + 700]
        self.assertIn("_es_superadmin()", bloque)
        self.assertIn("len(motivo) >= _LIC_OMITIR_MIN", bloque)
        self.assertIn("_LIC_OMITIR_MIN = 20", MODULO)

    def test_las_rutas_del_escaner_piden_administrar_y_no_escriben(self):
        for ruta, fn in (("/flota/api/licencia/leer", "flota_api_licencia_leer"), ("/flota/api/licencia/comparar", "flota_api_licencia_comparar"),
                         ("/flota/api/licencia/estado", "flota_api_licencia_estado")):
            i = MODULO.index(f'@app.route("{ruta}"')
            self.assertIn("@_flota_admin_required", MODULO[i:i + 160], ruta)
            cuerpo = MODULO[MODULO.index(f"def {fn}", i):MODULO.index(f"def {fn}", i) + 1800]
            self.assertNotIn("INSERT", cuerpo, ruta)
            self.assertNotIn("_guardar_archivo", cuerpo, ruta)
            self.assertNotIn("mysql_execute", cuerpo, ruta)

    def test_el_error_no_filtra_el_detalle_interno(self):
        i = MODULO.index("def _lic_leer_con_cupo")
        bloque = MODULO[i:i + 1300]
        self.assertIn('"No pude leer la licencia por un problema del servidor', bloque)
        self.assertNotIn("str(e)", bloque.split("except Exception as e:")[1])


# ══════════════════════════════════════════════════════════════════
#  6. La pantalla del chofer
# ══════════════════════════════════════════════════════════════════
HTML = _leer("templates", "flota", "index.html")
CSS = _leer("static", "flota.css")


class TestPantallaDelEscaner(unittest.TestCase):
    BLOQUE_JS = JS[JS.index("ESCÁNER DE LICENCIAS (Daniel, 05-nov-2026)"):JS.index("  function guardarCh() {")]

    def test_la_tarjeta_ofrece_camara_y_subir_y_conserva_el_campo_de_siempre(self):
        i = HTML.index('id="flChEscaner"')
        tarjeta = HTML[i:i + 3200]
        self.assertIn('id="flChBtnFoto"', tarjeta)
        self.assertIn('id="flChBtnSubir"', tarjeta)
        self.assertIn('<input type="file" id="flChCamara" accept="image/*" capture="environment" hidden', tarjeta)
        self.assertIn('id="flChLectura" aria-live="polite"', tarjeta)
        self.assertIn('id="flChArchivo"', HTML)                                        # el campo de archivo de siempre sigue (REGLA #4.2)
        self.assertIn('accept="application/pdf,image/jpeg,image/png,image/webp"', HTML)
        self.assertLess(HTML.index('id="flChEscaner"'), HTML.index('id="flChClases"'))   # el escáner es lo primero del paso de la licencia

    def test_elegir_la_foto_la_lee_sola_y_la_camara_pasa_por_el_mismo_campo(self):
        self.assertIn("if (f && !S.chArchivoErr) escanearCh(f); else licReset();", JS)
        self.assertIn("$('flChArchivo').files = dt.files", self.BLOQUE_JS)
        self.assertIn("iniciarEscanerCh();", JS)

    def test_la_lectura_usa_el_servidor_y_la_foto_reducida(self):
        self.assertIn("window.flotaSubida.comprimir(file)", self.BLOQUE_JS)
        self.assertIn("API + '/licencia/leer'", self.BLOQUE_JS)
        self.assertIn("API + '/licencia/comparar'", self.BLOQUE_JS)

    def test_cambiar_usuario_rut_o_clases_vuelve_a_comparar_sin_repetir_el_ocr(self):
        self.assertIn("recompararLuego();", JS[JS.index("var fc = $('flFormCh');"):JS.index("fc.addEventListener('submit'")])
        self.assertIn("$('flChUsuario').addEventListener('change', recompararLuego);", JS)

    def test_lo_que_la_licencia_dice_manda_en_las_reglas_del_formulario(self):
        i = JS.index("function reglasCh()")
        reglas = JS[i:JS.index("function marcarCh", i)]
        escaner = reglas[reglas.index("Escáner de licencias"):]
        for campo in ("usuario_id", "rut", "licencia_clases", "licencia_vence"):
            self.assertIn("'" + campo + "'", escaner)
        self.assertIn("S.chVerif", reglas)
        self.assertIn("S.chLeyendo", reglas)                                           # mientras lee no se puede guardar

    def test_el_vencimiento_y_las_clases_se_rellenan_de_la_licencia_y_rut_y_nombre_solo_si_estaban_vacios(self):
        i = self.BLOQUE_JS.index("function aplicarLecturaCh")
        f = self.BLOQUE_JS[i:i + 1400]
        self.assertIn("f.elements.licencia_vence.value = l.vence", f)
        self.assertIn("!vCh('rut').trim()", f)
        self.assertIn("!vCh('nombre_completo').trim()", f)

    def test_con_licencia_ya_escaneada_el_rut_las_clases_y_el_vencimiento_no_se_editan_a_mano(self):
        i = JS.index("function abrirChofer(c)")
        ab = JS[i:i + 4200]
        self.assertIn("S.chBloquea = !!(c && c.tiene_licencia_pdf);", ab)
        self.assertIn("aplicarBloqueoCh();", ab)
        b = self.BLOQUE_JS[self.BLOQUE_JS.index("function aplicarBloqueoCh"):]
        self.assertIn("f.elements.rut.readOnly = b;", b)
        self.assertIn("bloquearVence();", b)             # el vencimiento ya no depende de esta bandera: nunca se escribe a mano (05-nov-2026)
        self.assertIn("i.disabled = b", b)

    def test_registrar_sin_comprobar_solo_se_ofrece_si_el_servidor_dice_que_puede_y_pide_motivo(self):
        self.assertIn("S.chPuedeOmitir && !S.chOmitir", self.BLOQUE_JS)
        self.assertIn("S.chPuedeOmitir = !!d.puede_omitir;", self.BLOQUE_JS)
        self.assertIn("window.ilusPrompt({", self.BLOQUE_JS)
        self.assertIn("LIC_OMITIR_MIN = 20", self.BLOQUE_JS)
        i = JS.index("function guardarCh() {")
        self.assertIn("if (S.chOmitir) body.licencia_sin_verificar_motivo = S.chOmitir;", JS[i:i + 3600])

    def test_sin_popups_nativos_regla_1(self):
        import re
        sin_comentarios = re.sub(r"/\*.*?\*/", "", self.BLOQUE_JS, flags=re.S)
        for nativo in (r"(?<![\w.])alert\(", r"(?<![\w.])confirm\(", r"(?<![\w.])prompt\("):
            self.assertIsNone(re.search(nativo, sin_comentarios), nativo)

    def test_el_panel_muestra_el_texto_completo_sin_cortar_y_es_tactil(self):
        for regla in (".fl-lec-v{font-size:.95rem;color:#111827;overflow-wrap:anywhere}", ".fl-btn-esc{min-height:46px"):
            self.assertIn(regla, CSS)
        self.assertNotIn("text-overflow:ellipsis", CSS[CSS.index("Escáner de licencias (Daniel, 05-nov-2026)"):])
        self.assertIn("@media (max-width: 640px){\n  .fl-lec-filas{grid-template-columns:1fr}", CSS.replace("\r\n", "\n"))

    def test_las_fechas_leidas_se_ofrecen_para_elegir_y_elegir_una_la_guarda_en_el_formulario(self):
        self.assertIn("function chipsFechas(l)", self.BLOQUE_JS)
        self.assertIn("'<div class=\"fl-lec-filas\">' + filas.join('') + '</div>' + chipsFechas(l) +", self.BLOQUE_JS)
        self.assertIn("data-lec-fecha", self.BLOQUE_JS)
        i = self.BLOQUE_JS.index("var fb = ev.target.closest('[data-lec-fecha]');")
        clic = self.BLOQUE_JS[i:i + 520]
        self.assertIn("$('flFormCh').elements.licencia_vence.value = fb.getAttribute('data-lec-fecha');", clic)
        self.assertIn("recompararLuego();", clic)

    def test_la_foto_escaneada_se_ve_al_lado_para_comparar_las_fechas(self):
        self.assertIn("S.chMiniatura ? '<img class=\"fl-lec-foto\"", self.BLOQUE_JS)
        self.assertIn("URL.createObjectURL(file)", self.BLOQUE_JS)
        self.assertIn("URL.revokeObjectURL(S.chMiniatura)", self.BLOQUE_JS)

    def test_el_panel_distingue_no_pude_leer_de_no_coincide(self):
        self.assertIn("'No pude leer la licencia'", self.BLOQUE_JS)
        self.assertIn("'La licencia no coincide: no se puede registrar así'", self.BLOQUE_JS)
        self.assertIn("fl-lec-por-que", self.BLOQUE_JS)


if __name__ == "__main__":
    unittest.main()
