# -*- coding: utf-8 -*-
"""🚚 FLOTA · «Solicitudes de vehículo» (Fase 1c: permisos por el front, documentos con PDF, foto, datos del titular y de los choferes,
archivar y borrar) — Daniel, 02-nov-2026:
«déjalo habilitado por el front para poder dejarle las asignaciones a cada uno… nadie puede agregar vehículos y nadie puede agregar
choferes, solamente puede gestionar solicitudes; deja la gestión del CRUD que yo pueda editar y borrar». «Más datos: RUT, nombre completo,
dirección, una opción de dejar el PDF y que no todos tengan acceso». «Todo eso (revisión técnica, permiso, SOAP, seguro) tiene que estar
registrado y almacenado, para tener al día». «Yo le dejaría una foto: haz la tabla bien inteligente y con calidad de información».

Decisiones que se vigilan:
  1. Cuatro niveles, decididos en UN solo lugar (flota_reglas.niveles): solicitar, gestionar, documentos y admin. Las tres llaves nacen
     APAGADAS; agregar, editar, archivar y borrar vehículos y choferes es solo del superadministrador, sin llave.
  2. Los PDF y los datos personales (RUT, nombre completo, dirección, póliza, chasis, motor) los ve solo quien tenga «documentos». La foto, quien entra.
     Los archivos son privados: viven bajo «flota_privado/» y /f/<key> responde 404 a esa clave.
  3. Reemplazar o quitar un archivo NO lo borra (queda como no vigente). Archivar conserva todo. Borrar definitivo solo si nunca tuvo reservas, escribiendo
     la patente (o el nombre), y queda anotado ANTES.
  4. La patente se guarda sin su dígito verificador (TVDZ88-5 → TVDZ88 + 5). El RUT debe existir (módulo 11).
Correr:  python -m pytest tests/test_flota_documentos.py -q -p no:warnings
"""
import datetime as dt
import json
import os
import re
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import flota_reglas as R  # noqa: E402
import test_flota_solicitudes as TS  # noqa: E402
from test_flota_vehiculos import BucketFalso  # noqa: E402

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SUPER = TS.SUPER
SOLO_PEDIR = {"X-Rol": "ejecutivo", "X-Flota": "flota_solicitar"}
GESTOR = {"X-Rol": "ejecutivo", "X-Flota": "flota_gestionar"}
CON_DOCS = {"X-Rol": "ejecutivo", "X-Flota": "flota_solicitar,flota_documentos"}       # la llave vieja: desde el 05-nov-2026 ya no abre nada
ADMIN_DOCS = {"X-Rol": "ejecutivo", "X-Flota": "flota_solicitar,flota_administrar"}   # quien administra es el único que ve los documentos
SIN_LLAVES = {"X-Rol": "ejecutivo"}
PDF = b"%PDF-1.4\n1 0 obj<<>>endobj\ntrailer<<>>\n%%EOF"
JPG = b"\xff\xd8\xff\xe0" + b"\x00" * 64
PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 32


def _leer(*partes):
    with open(os.path.join(RAIZ, *partes), encoding="utf-8") as f:
        return f.read()


# ═══════════════════════════════════════════════════════════════════════════
#  A. Reglas puras
# ═══════════════════════════════════════════════════════════════════════════
class TestNiveles(unittest.TestCase):
    def test_sin_llaves_no_hay_nada(self):
        self.assertEqual(R.niveles({}), {"admin": False, "gestionar": False, "documentos": False, "solicitar": False})
        self.assertEqual(R.niveles(None)["solicitar"], False)

    def test_cada_llave_abre_solo_lo_suyo(self):
        self.assertEqual(R.niveles({"flota_solicitar": True}), {"admin": False, "gestionar": False, "documentos": False, "solicitar": True})
        self.assertEqual(R.niveles({"flota_gestionar": True}), {"admin": False, "gestionar": True, "documentos": False, "solicitar": True})
        # Daniel, 05-nov-2026: «es información delicada que me gustaría que solamente los que administramos podamos verla»: la llave vieja
        # de documentos ya no abre nada.
        self.assertEqual(R.niveles({"flota_documentos": True}), {"admin": False, "gestionar": False, "documentos": False, "solicitar": False})

    def test_los_documentos_son_solo_de_quien_administra(self):
        self.assertTrue(R.niveles({"flota_administrar": True})["documentos"])
        self.assertTrue(R.niveles({"superadmin": True})["documentos"])
        for llaves in ({"flota_solicitar": True}, {"flota_gestionar": True}, {"flota_documentos": True},
                       {"flota_solicitar": True, "flota_gestionar": True, "flota_documentos": True}):
            self.assertFalse(R.niveles(llaves)["documentos"], llaves)

    def test_solo_el_superadmin_o_la_llave_administrar_es_admin(self):
        self.assertTrue(R.niveles({"superadmin": True}) == {"admin": True, "gestionar": True, "documentos": True, "solicitar": True})
        # las otras tres llaves, juntas, NO administran
        self.assertFalse(R.niveles({"flota_solicitar": True, "flota_gestionar": True, "flota_documentos": True})["admin"])
        # la llave nueva administra y entra, pero NO gestiona (no confirma ni entrega la llave)
        self.assertEqual(R.niveles({"flota_administrar": True}), {"admin": True, "gestionar": False, "documentos": True, "solicitar": True})


def _llave_cualquier_dia(caso):
    """Estas pruebas reservan días adelante y entregan la llave para probar OTROS pasos: la regla «la llave solo el día de la
    reserva» (Daniel, 03-nov-2026) se prueba aparte en TestLlaveDelDia, así que aquí se deja pasar."""
    p = mock.patch.object(TS.R, "llave_dia", return_value={"ok": True, "texto": "Hoy se entrega la llave."})
    p.start()
    caso.addCleanup(p.stop)


class TestPatenteYRut(unittest.TestCase):
    def test_patente_con_digito_verificador(self):
        self.assertEqual(R.separar_patente("TVDZ88-5"), ("TVDZ88", "5"))
        self.assertEqual(R.separar_patente("phvd.21-7"), ("PHVD21", "7"))
        self.assertEqual(R.separar_patente("AB-1234"), ("AB1234", None))
        self.assertEqual(R.separar_patente("ABCD-12"), ("ABCD12", None))
        self.assertEqual(R.separar_patente("ABCD12-K"), ("ABCD12", "K"))
        self.assertEqual(R.patente_completa("TVDZ88", "5"), "TVDZ88-5")
        self.assertEqual(R.patente_completa("TVDZ88", None), "TVDZ88")

    def test_el_digito_no_se_confunde_con_la_patente(self):
        d, e = R.validar_vehiculo({"patente": "TVDZ88-5", "tipo": "Furgón", "licencia_clases": ["B"]})
        self.assertEqual(e, {})
        self.assertEqual((d["patente"], d["patente_dv"]), ("TVDZ88", "5"))
        d, e = R.validar_vehiculo({"patente": "TVDZ88", "patente_dv": "5", "tipo": "Furgón", "licencia_clases": ["B"]})
        self.assertEqual((d["patente"], d["patente_dv"]), ("TVDZ88", "5"))

    def test_rut(self):
        self.assertTrue(R.rut_valido("76.996.964-0"))
        self.assertTrue(R.rut_valido("769969640"))
        self.assertFalse(R.rut_valido("76.996.964-1"))
        self.assertFalse(R.rut_valido("1234"))
        self.assertFalse(R.rut_valido(""))
        self.assertEqual(R.normalizar_rut("76.996.964-0"), "76996964-0")
        self.assertEqual(R.rut_formato("76996964-0"), "76.996.964-0")
        self.assertEqual(R.rut_formato("no es rut"), "no es rut")

    def test_validar_vehiculo_con_los_datos_nuevos(self):
        d, e = R.validar_vehiculo({
            "patente": "TVDZ88-5", "tipo": "Furgón", "marca": "Changan", "modelo": "M201 F Cargo", "anio": "2025", "cilindrada": "1.2 L",
            "numero_motor": "jl473qr30c503658", "vin": "LS4AAB3R1SG808537", "propietario": "Sport and Health Solution SpA",
            "propietario_rut": "76.996.964-0", "licencia_clases": ["B"], "soap_vence": "2027-03-31",
            "soap_detalle": "BCI Seguros, póliza 126705219-7"})
        self.assertEqual(e, {})
        self.assertEqual((d["numero_motor"], d["propietario_rut"], d["cilindrada"]), ("JL473QR30C503658", "76996964-0", "1.2 L"))
        self.assertEqual(d["soap_detalle"], "BCI Seguros, póliza 126705219-7")
        _d, e = R.validar_vehiculo({"patente": "ABCD12", "tipo": "x2", "licencia_clases": ["B"], "propietario_rut": "11.111.111-9"})
        self.assertIn("propietario_rut", e)
        _d, e = R.validar_vehiculo({"patente": "ABCD12", "tipo": "x2", "licencia_clases": ["B"], "soap_detalle": "x" * 161})
        self.assertIn("soap_detalle", e)


class TestArchivosPuros(unittest.TestCase):
    def test_detecta_por_el_contenido_no_por_el_nombre(self):
        self.assertEqual(R.detectar_archivo(PDF)[0], "pdf")
        self.assertEqual(R.detectar_archivo(JPG)[:2], ("imagen", "image/jpeg"))
        self.assertEqual(R.detectar_archivo(PNG)[:2], ("imagen", "image/png"))
        self.assertEqual(R.detectar_archivo(b"RIFF\x00\x00\x00\x00WEBPVP8 ")[:2], ("imagen", "image/webp"))
        self.assertIsNone(R.detectar_archivo(b"MZ\x90\x00 programa.exe"))
        self.assertIsNone(R.detectar_archivo(b"<html><script>alert(1)</script>"))
        self.assertIsNone(R.detectar_archivo(b""))

    def test_cada_tipo_acepta_lo_que_corresponde(self):
        self.assertIsNone(R.validar_archivo("vehiculo", "foto", JPG)[1])
        self.assertIsNotNone(R.validar_archivo("vehiculo", "foto", PDF)[1])           # la foto es una imagen
        for t in ("revision_tecnica", "permiso_circulacion", "soap", "seguro"):
            self.assertIsNone(R.validar_archivo("vehiculo", t, PDF)[1], t)
            self.assertIsNotNone(R.validar_archivo("vehiculo", t, JPG)[1], t)         # los documentos se guardan en PDF
        self.assertIsNone(R.validar_archivo("chofer", "licencia", PDF)[1])
        self.assertIsNone(R.validar_archivo("chofer", "licencia", JPG)[1])            # la licencia también puede ser la foto del carnet
        self.assertIsNotNone(R.validar_archivo("chofer", "soap", PDF)[1])
        self.assertIsNotNone(R.validar_archivo("vehiculo", "licencia", PDF)[1])
        self.assertIsNotNone(R.validar_archivo("vehiculo", "soap", b"")[1])

    def test_tope_de_peso(self):
        self.assertIsNotNone(R.validar_archivo("vehiculo", "soap", PDF + b"0" * (15 * 1024 * 1024))[1])
        self.assertIsNotNone(R.validar_archivo("vehiculo", "foto", JPG + b"0" * (10 * 1024 * 1024))[1])

    def test_nombre_seguro(self):
        self.assertEqual(R.nombre_seguro("../../etc/passwd.pdf", ".pdf"), "passwd.pdf")
        self.assertEqual(R.nombre_seguro("C:\\Users\\x\\SOAP 2026.pdf", ".pdf"), "SOAP 2026.pdf")
        self.assertEqual(R.nombre_seguro("", ".pdf"), "archivo.pdf")
        self.assertTrue(R.nombre_seguro('a"b<c>.pdf', ".pdf").endswith(".pdf"))
        self.assertNotRegex(R.nombre_seguro('a"b<c>\n.pdf', ".pdf"), r'["<>\n]')


class TestCalidad(unittest.TestCase):
    def test_ficha_vacia_y_completa(self):
        v = R.calidad_ficha({}, {}, False)
        self.assertEqual((v["pct"], v["hechos"], v["total"]), (0, 0, 12))
        self.assertEqual(len(v["faltan"]), 12)
        completo = {"propietario": "X", "vin": "V", "numero_motor": "M", "revision_tecnica_vence": dt.date(2027, 1, 1),
                    "permiso_circulacion_vence": dt.date(2027, 1, 1), "soap_vence": dt.date(2027, 1, 1), "seguro_vence": dt.date(2027, 1, 1)}
        c = R.calidad_ficha(completo, {k: True for k, _n in R.DOCUMENTOS}, True)
        self.assertEqual((c["pct"], c["faltan"]), (100, []))

    def test_dice_lo_que_falta_en_claro(self):
        f = {"propietario": "X", "permiso_circulacion_vence": dt.date(2027, 3, 31), "soap_vence": dt.date(2027, 3, 31)}
        c = R.calidad_ficha(f, {"permiso_circulacion": True, "soap": True}, True)
        self.assertIn("N° de chasis (VIN)", c["faltan"])
        self.assertIn("Seguro: PDF", c["faltan"])
        self.assertIn("Revisión técnica: fecha de vencimiento", c["faltan"])
        self.assertNotIn("SOAP: PDF", c["faltan"])
        self.assertNotIn("Foto del vehículo", c["faltan"])

    def test_proximo_vencimiento(self):
        docs = R.resumen_documentos({"soap_vence": dt.date(2027, 3, 31), "permiso_circulacion_vence": dt.date(2026, 12, 1)}, dt.date(2026, 11, 2))
        p = R.proximo_vencimiento(docs)
        self.assertEqual(p["nombre"], "Permiso de circulación")
        self.assertEqual(p["vence_fmt"], "01/12/2026")
        self.assertIsNone(R.proximo_vencimiento(R.resumen_documentos({}, dt.date(2026, 11, 2))))

    def test_confirmacion_para_borrar(self):
        self.assertTrue(R.coincide_confirmacion("tvdz-88", "TVDZ88", es_patente=True))
        self.assertTrue(R.coincide_confirmacion("TVDZ88-5", "TVDZ88", es_patente=True))
        self.assertFalse(R.coincide_confirmacion("OTRA99", "TVDZ88", es_patente=True))
        self.assertTrue(R.coincide_confirmacion("  ana maría  pérez ", "Ana Maria Perez"))
        self.assertFalse(R.coincide_confirmacion("", "Ana"))
        self.assertFalse(R.coincide_confirmacion("x", ""))


# ═══════════════════════════════════════════════════════════════════════════
#  B. Rutas: permisos por nivel, archivos privados, archivar y borrar
# ═══════════════════════════════════════════════════════════════════════════
class Base2(TS.Base):
    def setUp(self):
        self.bucket = BucketFalso()
        self.base = TS.Falsa()
        self.app = TS._app(self.base, self.bucket)
        self.c = self.app.test_client()
        self.furgon = self.veh("TVDZ88-5", "Furgón", "Changan", "M201 F Cargo", propietario="Sport and Health Solution SpA", anio=2025,
                               cilindrada="1.2 L", color="Blanco", combustible="Bencina", vin="LS4AAB3R1SG808537", numero_motor="JL473QR30C503658",
                               propietario_rut="76.996.964-0", soap_detalle="BCI Seguros, póliza 126705219-7")
        self.camion = self.veh("PHVD21-7", "Camión", "Kia", "Frontier 2.5", propietario="Banco de Crédito e Inversiones")
        self.ana = self.chofer(1)
        self.beto = self.chofer(2)
        # Cada chofer se registra con su licencia escaneada (04-nov-2026): estos son los archivos «de fábrica» que las pruebas no cuentan.
        self.arch_base, self.obj_base = len(self.base.arch), len(self.bucket.objetos)

    def subir(self, entidad, ident, tipo, datos, headers=SUPER, nombre="documento.pdf", tipo_mime="application/pdf"):
        import io
        return self.c.post("/flota/api/archivos", headers=headers, content_type="multipart/form-data", data={
            "entidad": entidad, "entidad_id": str(ident), "tipo": tipo, "archivo": (io.BytesIO(datos), nombre, tipo_mime)})

    def veh_json(self, headers=SUPER, vid=None):
        lista = self.c.get("/flota/api/vehiculos", headers=headers).get_json()["vehiculos"]
        return next(v for v in lista if v["id"] == (vid or self.furgon))


class TestPermisosPorNivel(Base2):
    def setUp(self):
        super().setUp()
        _llave_cualquier_dia(self)

    def test_sin_llaves_no_entra_a_nada(self):
        self.assertEqual(self.c.get("/flota", headers=SIN_LLAVES).status_code, 302)
        for url in ("/flota/api/vehiculos", "/flota/api/choferes", "/flota/api/solicitudes", "/flota/api/calendario"):
            self.assertEqual(self.c.get(url, headers=SIN_LLAVES).status_code, 403, url)

    def test_solo_documentos_sin_pedir_tampoco_entra(self):
        self.assertEqual(self.c.get("/flota/api/vehiculos", headers={"X-Rol": "ejecutivo", "X-Flota": "flota_documentos"}).status_code, 403)

    def test_quien_pide_entra_y_ve_la_flota(self):
        self.assertEqual(self.c.get("/flota", headers=SOLO_PEDIR).status_code, 200)
        d = self.c.get("/flota/api/vehiculos", headers=SOLO_PEDIR).get_json()
        self.assertEqual(len(d["vehiculos"]), 2)
        self.assertEqual(d["permisos"], {"admin": False, "gestionar": False, "documentos": False, "solicitar": True})
        d = self.c.get("/flota/api/choferes", headers=SOLO_PEDIR).get_json()
        self.assertEqual(len(d["choferes"]), 2)

    def test_quien_solo_pide_no_ve_pdf_ni_datos_personales(self):
        self.subir("vehiculo", self.furgon, "soap", PDF)
        v = self.veh_json(SOLO_PEDIR)
        for campo in ("vin", "numero_motor", "propietario_rut", "calidad"):
            self.assertIsNone(v[campo], campo)
        for d in v["documentos"]:
            self.assertEqual(d["detalle"], "")
            self.assertIsNone(d["archivo"])
        soap = next(d for d in v["documentos"] if d["clave"] == "soap")
        self.assertTrue(soap["tiene_pdf"])                                    # sabe que hay PDF, no cuál ni dónde
        self.assertNotIn("soap_detalle", v)
        c = self.c.get("/flota/api/choferes", headers=SOLO_PEDIR).get_json()["choferes"][0]
        for campo in ("rut", "nombre_completo", "direccion", "notas", "licencia_archivo"):
            self.assertIsNone(c[campo], campo)
        self.assertEqual(c["rut_fmt"], "")
        self.assertEqual(c["nombre"], "Ana Pérez")                            # el nombre del usuario sí: lo necesita para pedir

    def test_la_llave_vieja_de_documentos_ya_no_abre_nada(self):
        self.subir("vehiculo", self.furgon, "soap", PDF)
        v = self.veh_json(CON_DOCS)
        self.assertIsNone(v["vin"])
        self.assertIsNone(v["propietario_rut"])
        self.assertTrue(all(d["archivo"] is None and d["detalle"] == "" for d in v["documentos"]))
        c = self.c.get("/flota/api/choferes", headers=CON_DOCS).get_json()["choferes"][0]
        for campo in ("rut", "nombre_completo", "direccion", "licencia_archivo"):
            self.assertIsNone(c[campo], campo)
        d = self.c.get("/flota/api/vehiculos", headers=CON_DOCS).get_json()
        self.assertFalse(d["permisos"]["documentos"])

    def test_quien_administra_si_ve_todo_eso(self):
        self.subir("vehiculo", self.furgon, "soap", PDF)
        v = self.veh_json(ADMIN_DOCS)
        self.assertEqual(v["vin"], "LS4AAB3R1SG808537")
        self.assertEqual(v["propietario_rut_fmt"], "76.996.964-0")
        soap = next(d for d in v["documentos"] if d["clave"] == "soap")
        self.assertEqual(soap["detalle"], "BCI Seguros, póliza 126705219-7")
        self.assertIsNotNone(soap["archivo"])
        c = self.c.get("/flota/api/choferes", headers=ADMIN_DOCS).get_json()["choferes"][0]
        self.assertEqual(c["nombre_completo"], "Ana María Pérez Soto")
        self.assertRegex(c["rut_fmt"], r"^\d{2}\.\d{3}\.\d{3}-[\dK]$")
        self.assertEqual(c["direccion"], "Av. Siempre Viva 742, Providencia")

    def test_pedir_si_pero_agregar_editar_borrar_no(self):
        r = self.c.post("/flota/api/solicitudes", json=TS._sol(vehiculo_id=self.furgon, chofer_id=self.ana), headers=SOLO_PEDIR)
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        todo = {"X-Rol": "ejecutivo", "X-Flota": "flota_solicitar,flota_gestionar,flota_documentos"}      # ni con las tres llaves
        n_veh, n_cho = len(self.base.veh), len(self.base.cho)
        pruebas = [
            ("post", "/flota/api/vehiculos", TS_VEH), ("put", f"/flota/api/vehiculos/{self.furgon}", TS_VEH),
            ("post", f"/flota/api/vehiculos/{self.furgon}/inhabilitar", {"motivo": "En el taller"}),
            ("post", f"/flota/api/vehiculos/{self.furgon}/archivar", {"motivo": "Vendido hoy"}),
            ("delete", f"/flota/api/vehiculos/{self.camion}", {"confirmacion": "PHVD21"}),
            ("post", "/flota/api/choferes", dict(TS._datos_chofer(4), usuario_id=4)), ("put", f"/flota/api/choferes/{self.ana}", TS._datos_chofer(1)),
            ("post", f"/flota/api/choferes/{self.ana}/archivar", {"motivo": "Dejó la empresa"}),
            ("delete", f"/flota/api/choferes/{self.beto}", {"confirmacion": "Humberto Soto Rivas"}),
            ("get", "/flota/api/choferes/usuarios", None),
        ]
        for metodo, url, cuerpo in pruebas:
            r = getattr(self.c, metodo)(url, headers=todo, **({"json": cuerpo} if cuerpo is not None else {}))
            self.assertEqual(r.status_code, 403, f"{metodo} {url}")
            self.assertEqual(r.get_json()["error_codigo"], "SIN_PERMISO_ADMIN_FLOTA", url)
        self.assertEqual((len(self.base.veh), len(self.base.cho)), (n_veh, n_cho))
        self.assertTrue(self.base.veh[self.furgon]["habilitado"])
        self.assertEqual(self.subir("vehiculo", self.furgon, "soap", PDF, headers=todo).status_code, 403)

    def test_solo_quien_gestiona_confirma_entrega_la_llave_mueve_y_ve_el_historial(self):
        r = self.c.post("/flota/api/solicitudes", json=TS._sol(vehiculo_id=self.furgon, chofer_id=self.ana), headers=SOLO_PEDIR)
        sid = r.get_json()["solicitud"]["id"]
        self.assertFalse(r.get_json()["solicitud"]["puede"]["confirmar"])
        for ruta in ("confirmar", "llave"):
            self.assertEqual(self.c.post(f"/flota/api/solicitudes/{sid}/{ruta}", json={}, headers=SOLO_PEDIR).status_code, 403, ruta)
        self.assertEqual(self.c.get(f"/flota/api/vehiculos/{self.furgon}/historial", headers=SOLO_PEDIR).status_code, 403)
        # el gestor sí (y entra sin la llave de pedir)
        r = self.c.post(f"/flota/api/solicitudes/{sid}/confirmar", json={}, headers=GESTOR)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(self.c.post(f"/flota/api/solicitudes/{sid}/llave", json={}, headers=GESTOR).status_code, 200)
        self.assertEqual(self.c.get(f"/flota/api/vehiculos/{self.furgon}/historial", headers=GESTOR).status_code, 200)
        # pero el gestor tampoco puede agregar ni ver PDF
        self.assertEqual(self.c.post("/flota/api/vehiculos", json=TS_VEH, headers=GESTOR).status_code, 403)
        self.assertIsNone(self.veh_json(GESTOR)["vin"])

    def test_el_superadmin_puede_todo(self):
        d = self.c.get("/flota/api/vehiculos", headers=SUPER).get_json()
        self.assertEqual(d["permisos"], {"admin": True, "gestionar": True, "documentos": True, "solicitar": True})
        self.assertEqual(self.c.get("/flota/api/choferes/usuarios", headers=SUPER).status_code, 200)

    def test_la_pagina_lleva_los_permisos_para_esconder_botones(self):
        html = self.c.get("/flota", headers=SOLO_PEDIR).get_data(as_text=True)
        self.assertIn("window.FLOTA_PERMISOS", html)
        self.assertRegex(html, r'FLOTA_PERMISOS = \{[^}]*"admin": false')
        html = self.c.get("/flota", headers=SUPER).get_data(as_text=True)
        self.assertRegex(html, r'FLOTA_PERMISOS = \{[^}]*"admin": true')
        self.assertNotIn("Tú administras la flota", html)                     # Daniel, 05-nov-2026: «sácale ese mensaje»


TS_VEH = {"patente": "ZZZZ99", "tipo": "Furgón", "licencia_clases": ["B"]}


class TestArchivosPrivados(Base2):
    def test_foto_la_ve_quien_entra_y_vive_bajo_flota_privado(self):
        r = self.subir("vehiculo", self.furgon, "foto", JPG, nombre="furgon.jpg", tipo_mime="image/jpeg")
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        foto_id = r.get_json()["vehiculo"]["foto_id"]
        self.assertIsNotNone(foto_id)
        clave = self.base.arch[foto_id]["gcs_key"]
        self.assertTrue(clave.startswith("flota_privado/vehiculo/"), clave)
        self.assertIn(clave, self.bucket.objetos)
        g = self.c.get(f"/flota/api/archivos/{foto_id}", headers=SOLO_PEDIR)
        self.assertEqual((g.status_code, g.data, g.mimetype), (200, JPG, "image/jpeg"))
        self.assertEqual(self.c.get(f"/flota/api/archivos/{foto_id}", headers=SIN_LLAVES).status_code, 403)

    def test_pdf_solo_con_el_permiso_de_documentos(self):
        r = self.subir("vehiculo", self.furgon, "soap", PDF, nombre="SOAP 2026.pdf")
        self.assertEqual(r.status_code, 200)
        aid = next(a["id"] for a in self.base.arch.values() if a["tipo"] == "soap")
        for h in (SOLO_PEDIR, GESTOR, CON_DOCS):
            self.assertEqual(self.c.get(f"/flota/api/archivos/{aid}", headers=h).status_code, 403)
        g = self.c.get(f"/flota/api/archivos/{aid}", headers=ADMIN_DOCS)
        self.assertEqual((g.status_code, g.data, g.mimetype), (200, PDF, "application/pdf"))
        self.assertIn("inline", g.headers["Content-Disposition"])
        self.assertIn("no-store", g.headers["Cache-Control"])
        self.assertEqual(g.headers["X-Content-Type-Options"], "nosniff")
        self.assertEqual(self.c.get(f"/flota/api/archivos/{aid}", headers=SUPER).status_code, 200)
        self.assertEqual(self.c.get("/flota/api/archivos/999", headers=SUPER).status_code, 404)

    def test_reemplazar_deja_el_anterior_como_no_vigente_y_no_lo_borra(self):
        self.subir("vehiculo", self.furgon, "soap", PDF + b"viejo")
        self.subir("vehiculo", self.furgon, "soap", PDF + b"nuevo")
        archivos = [a for a in self.base.arch.values() if a["tipo"] == "soap"]
        self.assertEqual(sorted(a["activo"] for a in archivos), [0, 1])
        self.assertEqual(len(self.bucket.objetos), self.obj_base + 2)          # los dos siguen guardados
        vigente = next(a for a in archivos if a["activo"])
        soap = next(d for d in self.veh_json()["documentos"] if d["clave"] == "soap")
        self.assertEqual(soap["archivo"]["id"], vigente["id"])
        self.assertEqual(self.bucket.objetos[vigente["gcs_key"]][0], PDF + b"nuevo")

    def test_quitar_no_borra_solo_deja_de_ser_el_vigente(self):
        self.subir("vehiculo", self.furgon, "seguro", PDF)
        aid = next(a["id"] for a in self.base.arch.values() if a["tipo"] == "seguro")
        self.assertEqual(self.c.post(f"/flota/api/archivos/{aid}/quitar", json={}, headers=CON_DOCS).status_code, 403)
        self.assertEqual(self.c.post(f"/flota/api/archivos/{aid}/quitar", json={}, headers=SUPER).status_code, 200)
        self.assertEqual(self.c.post(f"/flota/api/archivos/{aid}/quitar", json={}, headers=SUPER).status_code, 409)
        self.assertEqual(self.base.arch[aid]["activo"], 0)
        self.assertEqual(len(self.bucket.objetos), self.obj_base + 1)
        self.assertFalse(next(d for d in self.veh_json()["documentos"] if d["clave"] == "seguro")["tiene_pdf"])

    def test_la_bitacora_anota_los_archivos(self):
        self.subir("vehiculo", self.furgon, "soap", PDF, nombre="soap.pdf")
        h = self.c.get(f"/flota/api/vehiculos/{self.furgon}/historial", headers=SUPER).get_json()["eventos"]
        self.assertEqual(h[0]["titulo"], "Archivo guardado")
        self.assertIn("soap.pdf", h[0]["lineas"][0])

    def test_rechaza_lo_que_no_es(self):
        n = len(self.base.arch)
        for entidad, tipo, datos in (("vehiculo", "soap", JPG), ("vehiculo", "foto", PDF), ("vehiculo", "soap", b"MZ\x90 virus"),
                                     ("vehiculo", "licencia", PDF), ("chofer", "soap", PDF), ("camion", "soap", PDF), ("vehiculo", "soap", b"")):
            r = self.subir(entidad, self.furgon, tipo, datos)
            self.assertEqual(r.status_code, 400, (entidad, tipo))
        self.assertEqual(self.subir("vehiculo", 999, "soap", PDF).status_code, 404)
        self.assertEqual(len(self.base.arch), n)
        self.assertEqual(len(self.bucket.objetos), self.obj_base)

    def test_sin_almacenamiento_avisa_y_no_registra_nada(self):
        app = TS._app(self.base, None)
        import io
        r = app.test_client().post("/flota/api/archivos", headers=SUPER, content_type="multipart/form-data", data={
            "entidad": "vehiculo", "entidad_id": str(self.furgon), "tipo": "soap", "archivo": (io.BytesIO(PDF), "a.pdf", "application/pdf")})
        self.assertEqual(r.status_code, 503)
        self.assertEqual(len(self.base.arch), self.arch_base)

    def test_licencia_del_chofer_solo_la_ve_quien_tiene_documentos(self):
        r = self.subir("chofer", self.ana, "licencia", PDF, nombre="licencia.pdf")
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        aid = next(a["id"] for a in self.base.arch.values() if a["tipo"] == "licencia" and a["entidad_id"] == self.ana and a["activo"])
        self.assertTrue(self.base.arch[aid]["gcs_key"].startswith("flota_privado/chofer/"))
        c = next(x for x in self.c.get("/flota/api/choferes", headers=ADMIN_DOCS).get_json()["choferes"] if x["id"] == self.ana)
        self.assertEqual(c["licencia_archivo"]["id"], aid)
        c2 = next(x for x in self.c.get("/flota/api/choferes", headers=SOLO_PEDIR).get_json()["choferes"] if x["id"] == self.ana)
        self.assertIsNone(c2["licencia_archivo"])
        self.assertTrue(c2["tiene_licencia_pdf"])
        self.assertEqual(self.c.get(f"/flota/api/archivos/{aid}", headers=SOLO_PEDIR).status_code, 403)
        self.assertEqual(self.c.get(f"/flota/api/archivos/{aid}", headers=CON_DOCS).status_code, 403)        # la llave vieja de documentos ya no sirve
        self.assertEqual(self.c.get(f"/flota/api/archivos/{aid}", headers=ADMIN_DOCS).status_code, 200)

    def test_la_ruta_publica_no_sirve_lo_privado(self):
        app = _leer("app.py")
        i = app.index('def serve_archivo(key):')
        cuerpo = app[i:i + 6000]
        self.assertIn('if key.startswith("flota_privado/"):', cuerpo)
        self.assertLess(cuerpo.index('flota_privado/'), cuerpo.index("_vid_sensible = _foto_key_vid_sensible(key)"))
        self.assertIn('return ("", 404)', cuerpo[cuerpo.index('flota_privado/'):cuerpo.index('flota_privado/') + 300])
        self.assertIn('_PREFIJO_PRIVADO = "flota_privado/"', _leer("flota_module.py"))


class TestArchivarYBorrar(Base2):
    def reservar(self, **k):
        r = self.c.post("/flota/api/solicitudes", json=TS._sol(vehiculo_id=self.furgon, chofer_id=self.ana, **k), headers=SUPER)
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        return r.get_json()["solicitud"]["id"]

    def test_archivar_pide_motivo_y_saca_el_vehiculo_de_la_lista(self):
        self.assertEqual(self.c.post(f"/flota/api/vehiculos/{self.camion}/archivar", json={"motivo": "x"}, headers=SUPER).status_code, 400)
        r = self.c.post(f"/flota/api/vehiculos/{self.camion}/archivar", json={"motivo": "Vendido a un tercero"}, headers=SUPER)
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        self.assertTrue(r.get_json()["vehiculo"]["archivado"])
        self.assertFalse(r.get_json()["vehiculo"]["habilitado"])
        lista = self.c.get("/flota/api/vehiculos", headers=SUPER).get_json()
        self.assertEqual([v["id"] for v in lista["vehiculos"]], [self.furgon])
        self.assertEqual(lista["resumen"]["archivados"], 1)
        arch = self.c.get("/flota/api/vehiculos?archivados=1", headers=SUPER).get_json()
        self.assertEqual([v["id"] for v in arch["vehiculos"]], [self.camion])
        # el que no es superadmin no puede pedir los archivados
        self.assertEqual(self.c.get("/flota/api/vehiculos?archivados=1", headers=SOLO_PEDIR).get_json()["vehiculos"][0]["id"], self.furgon)
        self.assertEqual(self.base.log[-1]["accion"], "archivado")
        self.assertEqual(self.c.post(f"/flota/api/vehiculos/{self.camion}/archivar", json={"motivo": "Vendido a un tercero"}, headers=SUPER).status_code, 409)

    def test_no_se_archiva_con_reservas_por_delante_y_dice_que_hacer(self):
        self.reservar()
        self.base.abiertas = 1
        r = self.c.post(f"/flota/api/vehiculos/{self.furgon}/archivar", json={"motivo": "Vendido a un tercero"}, headers=SUPER)
        self.assertEqual(r.status_code, 409)
        self.assertEqual(r.get_json()["error_codigo"], "TIENE_RESERVAS")
        self.assertIn("muévelas o cancélalas", r.get_json()["error"])
        self.assertFalse(self.base.veh[self.furgon].get("archivado"))

    def test_desarchivar_vuelve_inhabilitado_y_no_se_habilita_archivado(self):
        self.c.post(f"/flota/api/vehiculos/{self.camion}/archivar", json={"motivo": "Vendido a un tercero"}, headers=SUPER)
        self.assertEqual(self.c.post(f"/flota/api/vehiculos/{self.camion}/habilitar", json={}, headers=SUPER).status_code, 409)
        r = self.c.post(f"/flota/api/vehiculos/{self.camion}/desarchivar", json={}, headers=SUPER)
        self.assertEqual(r.status_code, 200)
        self.assertFalse(r.get_json()["vehiculo"]["archivado"])
        self.assertFalse(r.get_json()["vehiculo"]["habilitado"])
        self.assertIn("Habilitar", r.get_json()["avisos"][0])
        self.assertEqual(self.c.post(f"/flota/api/vehiculos/{self.camion}/habilitar", json={}, headers=SUPER).status_code, 200)

    def test_un_vehiculo_archivado_no_se_reserva(self):
        self.c.post(f"/flota/api/vehiculos/{self.camion}/archivar", json={"motivo": "Vendido a un tercero"}, headers=SUPER)
        r = self.c.post("/flota/api/solicitudes", json=TS._sol(vehiculo_id=self.camion, chofer_id=self.ana), headers=SUPER)
        self.assertEqual(r.status_code, 409)

    def test_borrar_con_reservas_no_se_puede(self):
        self.reservar()
        r = self.c.delete(f"/flota/api/vehiculos/{self.furgon}", json={"confirmacion": "TVDZ88"}, headers=SUPER)
        self.assertEqual(r.status_code, 409)
        self.assertEqual(r.get_json()["error_codigo"], "TIENE_HISTORIAL")
        self.assertIn(self.furgon, self.base.veh)

    def test_borrar_un_vehiculo_sin_historial_deja_sus_archivos_como_no_vigentes(self):
        self.subir("vehiculo", self.camion, "soap", PDF)
        r = self.c.delete(f"/flota/api/vehiculos/{self.camion}", json={"confirmacion": "phvd21-7"}, headers=SUPER)
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        self.assertNotIn(self.camion, self.base.veh)
        self.assertTrue(all(a["activo"] == 0 for a in self.base.arch.values() if a["entidad"] == "vehiculo" and a["entidad_id"] == self.camion))
        self.assertEqual(len(self.bucket.objetos), self.obj_base + 1)          # el archivo sigue guardado

    def test_archivar_y_borrar_choferes(self):
        self.assertEqual(self.c.post(f"/flota/api/choferes/{self.beto}/archivar", json={"motivo": "x"}, headers=SUPER).status_code, 400)
        r = self.c.post(f"/flota/api/choferes/{self.beto}/archivar", json={"motivo": "Dejó la empresa"}, headers=SUPER)
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        self.assertTrue(r.get_json()["chofer"]["archivado"])
        ids = [c["id"] for c in self.c.get("/flota/api/choferes", headers=SUPER).get_json()["choferes"]]
        self.assertEqual(ids, [self.ana])
        self.assertEqual([c["id"] for c in self.c.get("/flota/api/choferes?archivados=1", headers=SUPER).get_json()["choferes"]], [self.beto])
        self.assertEqual(self.c.post(f"/flota/api/choferes/{self.beto}/habilitar", json={}, headers=SUPER).status_code, 409)
        self.assertEqual(self.c.post(f"/flota/api/choferes/{self.beto}/desarchivar", json={}, headers=SUPER).status_code, 200)
        # borrar: pide el nombre completo
        self.assertEqual(self.c.delete(f"/flota/api/choferes/{self.beto}", json={"confirmacion": "otro"}, headers=SUPER).status_code, 400)
        r = self.c.delete(f"/flota/api/choferes/{self.beto}", json={"confirmacion": "humberto soto rivas"}, headers=SUPER)
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        self.assertNotIn(self.beto, self.base.cho)
        self.assertEqual(self.base.clog[-1]["accion"], "eliminado")
        self.assertEqual(json.loads(self.base.clog[-1]["detalle"])["datos"]["nombre_completo"], "Humberto Soto Rivas")

    def test_chofer_con_reservas_no_se_borra_ni_se_archiva(self):
        self.reservar()
        self.base.abiertas = 1
        r = self.c.post(f"/flota/api/choferes/{self.ana}/archivar", json={"motivo": "Dejó la empresa"}, headers=SUPER)
        self.assertEqual((r.status_code, r.get_json()["error_codigo"]), (409, "TIENE_RESERVAS"))
        r = self.c.delete(f"/flota/api/choferes/{self.ana}", json={"confirmacion": "Ana María Pérez Soto"}, headers=SUPER)
        self.assertEqual((r.status_code, r.get_json()["error_codigo"]), (409, "TIENE_HISTORIAL"))
        self.assertIn(self.ana, self.base.cho)


class TestDatosNuevos(Base2):
    def test_vehiculo_con_los_datos_del_expediente(self):
        v = self.veh_json()
        self.assertEqual((v["patente"], v["patente_dv"], v["patente_completa"]), ("TVDZ88", "5", "TVDZ88-5"))
        self.assertEqual(v["propietario"], "Sport and Health Solution SpA")
        self.assertEqual((v["cilindrada"], v["numero_motor"]), ("1.2 L", "JL473QR30C503658"))
        self.assertEqual(v["titulo"], "Furgón Changan M201 F Cargo 2025")

    def test_calidad_y_proximo_vencimiento_en_la_tabla(self):
        r = self.c.put(f"/flota/api/vehiculos/{self.furgon}", json=dict(
            TS_VEH, patente="TVDZ88-5", propietario="Sport and Health Solution SpA", vin="LS4AAB3R1SG808537", numero_motor="JL473QR30C503658",
            soap_vence="2027-03-31", permiso_circulacion_vence="2027-03-31"), headers=SUPER)
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        self.subir("vehiculo", self.furgon, "soap", PDF)
        v = self.veh_json()
        self.assertEqual(v["calidad"]["total"], 12)
        self.assertEqual(v["calidad"]["hechos"], 6)                            # propietario, VIN, motor, 2 fechas y el PDF del SOAP
        self.assertEqual(v["calidad"]["pct"], 50)
        self.assertIn("Permiso de circulación: PDF", v["calidad"]["faltan"])
        self.assertEqual(v["proximo_vencimiento"]["vence_fmt"], "31/03/2027")
        self.assertEqual(self.c.get("/flota/api/vehiculos", headers=SUPER).get_json()["resumen"]["incompletos"], 2)

    def test_editar_un_vehiculo_no_pierde_la_foto(self):
        self.subir("vehiculo", self.furgon, "foto", JPG, nombre="f.jpg", tipo_mime="image/jpeg")
        self.c.put(f"/flota/api/vehiculos/{self.furgon}", json=dict(TS_VEH, patente="TVDZ88-5", marca="Changan"), headers=SUPER)
        self.assertIsNotNone(self.veh_json()["foto_id"])

    def test_el_chofer_pide_rut_nombre_y_direccion_y_el_rut_debe_existir(self):
        r = self.c.post("/flota/api/choferes", json={"usuario_id": 4, "licencia_clases": ["B"], "licencia_vence": TS.FUTURO}, headers=SUPER)
        self.assertEqual(r.status_code, 400)
        self.assertIn("rut", r.get_json()["errores"])
        self.assertIn("nombre_completo", r.get_json()["errores"])
        r = self.c.post("/flota/api/choferes", json=dict(TS._datos_chofer(4), usuario_id=4, rut="12.345.678-9"), headers=SUPER)
        self.assertEqual(r.status_code, 400)
        self.assertIn("RUT no es válido", r.get_json()["errores"]["rut"])
        r = self.c.post("/flota/api/choferes", json=dict(TS._datos_chofer(4), usuario_id=4, direccion=""), headers=SUPER)
        self.assertEqual(r.status_code, 400)                                    # la dirección es obligatoria (Daniel, 02-nov-2026)
        self.assertIn("direccion", r.get_json()["errores"])

    def test_los_usuarios_para_elegir_traen_lo_de_su_perfil_como_sugerencia(self):
        self.base.usr[4].update(rut="22.222.222-2", direccion="Los Alerces 123, Ñuñoa", direccion_lat=-33.45, direccion_lng=-70.6,
                                direccion_place_id="ChIJalerces123")
        u = self.c.get("/flota/api/choferes/usuarios", headers=SUPER).get_json()["usuarios"][0]
        self.assertEqual((u["id"], u["rut"], u["direccion"]), (4, "22.222.222-2", "Los Alerces 123, Ñuñoa"))
        self.assertEqual((u["direccion_lat"], u["direccion_place_id"]), (-33.45, "ChIJalerces123"))   # viaja con su ubicación de Google

    def test_la_direccion_del_chofer_debe_venir_validada_por_google(self):
        # Daniel: «la dirección debe estar validada por Google». Misma regla que Mi cuenta (usuarios_perfil.resolver_direccion).
        sin_geo = dict(TS._datos_chofer(4), usuario_id=4, direccion_lat="", direccion_lng="", direccion_place_id="")
        r = self.c.post("/flota/api/choferes", json=sin_geo, headers=SUPER)
        self.assertEqual(r.status_code, 400)
        self.assertIn("Google", r.get_json()["errores"]["direccion"])
        fuera = dict(TS._datos_chofer(4), usuario_id=4, direccion_lat="40.4", direccion_lng="-3.7")      # Madrid: no es Chile
        self.assertEqual(self.c.post("/flota/api/choferes", json=fuera, headers=SUPER).status_code, 400)
        r = self.c.post("/flota/api/choferes", json=dict(TS._datos_chofer(4), usuario_id=4), headers=SUPER)
        self.assertEqual(r.status_code, 200)
        cid = r.get_json()["chofer"]["id"]
        self.assertEqual(self.base.cho[cid]["direccion_place_id"], "ChIJprueba_providencia_742")
        self.assertTrue(r.get_json()["chofer"]["direccion_verificada"])
        # editar sin tocar la dirección (sin coordenadas en el cuerpo) la deja como estaba, con su ubicación
        r = self.c.put(f"/flota/api/choferes/{cid}", json=dict(TS._datos_chofer(4), direccion_lat="", direccion_lng="", direccion_place_id="",
                                                                notas="Solo cambio la nota"), headers=SUPER)
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        self.assertEqual(self.base.cho[cid]["direccion_place_id"], "ChIJprueba_providencia_742")
        # cambiar el texto sin elegirlo de Google no se acepta
        r = self.c.put(f"/flota/api/choferes/{cid}", json=dict(TS._datos_chofer(4), direccion="Otra calle 99", direccion_lat="",
                                                                direccion_lng="", direccion_place_id=""), headers=SUPER)
        self.assertEqual(r.status_code, 400)

    def test_el_rut_se_formatea_como_rut_chileno_en_pantalla(self):
        js_ch, js_veh, html = _leer("static", "flota_uso.js"), _leer("static", "flota.js"), _leer("templates", "flota", "index.html")
        self.assertIn("ilus_persona_fields.js", html)            # la misma función de Mi cuenta
        self.assertIn("rutFormatear", js_ch)
        self.assertIn("rutFormatear", js_veh)
        self.assertIn("ilusPlacesAutocomplete('flChDireccion'", js_ch)
        for i in ("flChDirLat", "flChDirLng", "flChDirPid"):
            self.assertIn(f'id="{i}"', html)


# ═══════════════════════════════════════════════════════════════════════════
#  C. Cableado en app.py y pantalla
# ═══════════════════════════════════════════════════════════════════════════
class TestCableadoPermisos(unittest.TestCase):
    APP = _leer("app.py")

    def test_las_tres_llaves_estan_en_perms_keys_y_apagadas(self):
        i = self.APP.index("PERMS_KEYS = (")
        bloque = self.APP[i:self.APP.index("\n)\n", i)]
        for k in ("flota_solicitar", "flota_gestionar", "flota_documentos", "flota_administrar"):
            self.assertIn(f'"{k}"', bloque, k)
        # no son de tipo «bloqueo» (concedidas por omisión): nacen apagadas para todos
        j = self.APP.index("_PERMS_CONCEDIDOS_POR_OMISION = {")
        self.assertNotIn("flota", self.APP[j:self.APP.index("}", j)])
        self.assertNotIn("flota", self.APP[self.APP.index("_PERMS_CONCEDIDOS_POR_DEFECTO = ("):][:200])

    def test_la_matriz_de_roles_las_muestra_y_las_compila(self):
        self.assertIn('"flota":          {"label":"Solicitudes de vehículo"', self.APP)
        self.assertIn('"acciones":["solicitar", "gestionar", "documentos", "administrar"]', self.APP)
        for k, a in (("flota_solicitar", "solicitar"), ("flota_gestionar", "gestionar"), ("flota_documentos", "documentos"),
                     ("flota_administrar", "administrar")):
            self.assertIn(f'base["{k}"] = bool(flo.get("{a}"))', self.APP)
        self.assertIn('flo = matrix.get("flota", {})', self.APP)
        j = self.APP.index('    "flota": {\n        "solicitar":')
        meta = self.APP[j:j + 900]
        for a in ("solicitar", "gestionar", "documentos", "administrar"):
            self.assertIn(f'"{a}":', meta)

    def test_el_menu_lo_ve_quien_tiene_una_de_las_dos_llaves(self):
        base = _leer("templates", "base.html")
        i = base.index('href="/flota"')
        previo = base[max(0, i - 200):i]
        self.assertIn("permissions.superadmin or permissions.flota_solicitar or permissions.flota_gestionar or permissions.flota_administrar", previo)
        self.assertEqual(base.count('href="/flota"'), 1)
        self.assertNotIn("url_for('flota", base)

    def test_las_llaves_de_la_flota_no_dan_acceso_a_nada_mas(self):
        # son llaves propias: ninguna reutiliza «superadmin», «admin» ni «mantenciones»
        for k in ("flota_solicitar", "flota_gestionar", "flota_documentos", "flota_administrar"):
            for m in re.finditer(rf'base\["{k}"\]\s*=\s*([^\n]+)', self.APP):
                self.assertNotRegex(m.group(1), r"adm\.|man\.|eti\.|tra\.")


class TestPantallaFase1c(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.html = TS._app(TS.Falsa()).test_client().get("/flota", headers=SUPER).get_data(as_text=True)
        cls.js = _leer("static", "flota.js")
        cls.css = _leer("static", "flota.css")

    def test_ficha_tabla_y_archivos_estan_en_la_plantilla(self):
        for i in ("flModalFicha", "flFichaCuerpo", "flFichaAcc", "flArchivoInput", "flOrden", "flChArchivo", "flChRut", "flChNombre", "flChDireccion",
                  "flChGestion", "flKpiIncompletos", "flChipArchivados", "flChChipArchivados"):
            self.assertIn(f'id="{i}"', self.html, i)
        for c in ("numero_motor", "cilindrada", "propietario", "propietario_rut", "soap_detalle", "seguro_detalle", "rut", "nombre_completo", "direccion"):
            self.assertIn(f'name="{c}"', self.html, c)

    def test_los_documentos_ya_no_van_como_cuatro_tarjetas_por_fila(self):
        # Daniel: «no me gusta que salgan los documentos así con tarjeta»
        self.assertNotIn("function chipDoc", self.js)
        self.assertIn("fl-dsem", self.js)               # cuatro segmentos de color
        self.assertIn("proximo_vencimiento", self.js)    # y solo lo próximo que vence
        self.assertIn("fl-ftabla", self.js)              # la ficha tiene una tabla de documentos con su PDF

    def test_la_foto_y_la_calidad_estan_en_la_tabla(self):
        self.assertIn("foto_id", self.js)
        self.assertIn("calidad", self.js)
        self.assertIn("Propietario registrado", self.js)
        self.assertIn("Calidad de la ficha", self.js)

    def test_cada_vehiculo_es_una_tarjeta_grande_con_franja_de_documentos(self):
        # Daniel eligió «tarjeta grande por vehículo»: foto grande, datos a la derecha y los 4 documentos en una franja
        self.assertIn('class="fl-vlista"', self.html)
        self.assertNotIn('<table class="fl-tabla fl-tabla--veh"', self.html)
        for t in ("fl-vtar", "fl-vtar-foto", "fl-vtar-docs", "bloqueDoc", "Foto referencial"):
            self.assertIn(t, self.js)
        self.assertIn(".fl-vtar-docs{display:grid;grid-template-columns:repeat(4", self.css)

    def test_el_js_esconde_lo_que_no_corresponde_pero_el_servidor_manda(self):
        for k in ("PERM.admin", "PERM.documentos", "PERM.gestionar"):
            self.assertIn(k, self.js)
        self.assertIn("window.FLOTA_PERMISOS", self.js)

    def test_borrar_pide_escribir_la_patente(self):
        self.assertIn("Para confirmar escribe su patente", self.js)
        self.assertIn("'DELETE'", self.js)

    def test_la_tabla_de_la_ficha_pasa_a_tarjetas_en_el_celular(self):
        self.assertIn(".fl-ftabla td[data-label]::before", self.css)
        self.assertIn(".fl-kpis--5", self.css)


if __name__ == "__main__":
    unittest.main()


class TestChoferExternoYRepositorio(unittest.TestCase):
    """02-nov-2026 se abrió un chofer externo (sin usuario de ILUS). 03-nov-2026 (Daniel): «eliminaras esa parte donde el usuario externo
    de chofer todos tienen que pertenecer a la página. Si no está registrado, no puede retirar el camión». Ya no se crean; uno que quedó
    de antes se puede editar o archivar, pero no se elige en una reserva ni recibe la llave."""

    def setUp(self):
        _llave_cualquier_dia(self)
        self.bucket = BucketFalso()
        self.base = TS.Falsa()
        self.app = TS._app(self.base, self.bucket)
        self.c = self.app.test_client()

    def _externo_antiguo(self):
        """Un externo que quedó en el repositorio de antes: se inserta directo, porque la API ya no los crea."""
        self.base.execute(
            "INSERT INTO flota_choferes (usuario_id, telefono, rut, nombre_completo, direccion, direccion_lat, direccion_lng, "
            "direccion_place_id, licencia_clases, licencia_vence, notas, created_by, updated_by) VALUES (NULL,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            ("+56987654321", "111111111", "Pedro Ignacio Lagos Fuentes", "Av. Siempre Viva 742, Providencia", None, None, None, "B",
             dt.date.fromisoformat(TS.FUTURO), None, "prueba", "prueba"))
        return max(self.base.cho)

    def test_ya_no_se_crea_un_chofer_externo(self):
        d = dict(TS._datos_chofer(9), externo=True, nombre_completo="Pedro Ignacio Lagos Fuentes", rut="11.111.111-1",
                 telefono="+56 9 8765 4321")
        d.pop("usuario_id", None)
        r = self.c.post("/flota/api/choferes", json=d, headers=SUPER)
        self.assertEqual(r.status_code, 400)
        self.assertEqual(r.get_json()["error_codigo"], "CHOFER_SIN_USUARIO")
        self.assertFalse(self.base.cho)                                                # no se guardó nada
        self.assertEqual(self.c.post("/flota/api/choferes", json={k: v for k, v in d.items() if k != "externo"},
                                     headers=SUPER).status_code, 400)                  # sin usuario, tampoco

    def test_un_externo_antiguo_no_reserva_ni_recibe_la_llave(self):
        cid = self._externo_antiguo()
        ch = self.c.get("/flota/api/choferes", headers=SUPER).get_json()["choferes"][0]
        self.assertTrue(ch["externo"])
        vid = self.c.post("/flota/api/vehiculos", json={"patente": "PHVD21-7", "tipo": "Camión", "licencia_clases": ["B"]},
                          headers=SUPER).get_json()["vehiculo"]["id"]
        dia = (dt.date.today() + dt.timedelta(days=3)).isoformat()
        r = self.c.post("/flota/api/solicitudes", json={"vehiculo_id": vid, "chofer_id": cid, "area": "Logística", "motivo": "Reparto zona sur",
                        "fecha": dia, "hora_desde": "09:00", "hora_hasta": "12:00"}, headers=SUPER)
        self.assertEqual(r.status_code, 400)
        self.assertEqual(r.get_json()["error_codigo"], "CHOFER_SIN_USUARIO")
        self.assertFalse(self.base.sol)
        # Una reserva antigua que ya lo tenía: la llave no se entrega hasta cambiarle el chofer por uno registrado
        bueno = self.c.post("/flota/api/choferes", json=dict(TS._datos_chofer(1), usuario_id=1), headers=SUPER).get_json()["chofer"]["id"]
        sid = self.c.post("/flota/api/solicitudes", json={"vehiculo_id": vid, "chofer_id": bueno, "area": "Logística", "motivo": "Reparto zona sur",
                          "fecha": dia, "hora_desde": "09:00", "hora_hasta": "12:00"}, headers=SUPER).get_json()["solicitud"]["id"]
        self.c.post(f"/flota/api/solicitudes/{sid}/confirmar", json={}, headers=SUPER)
        self.base.sol[sid]["chofer_id"] = cid                                         # como quedó de antes del 03-nov
        r = self.c.post(f"/flota/api/solicitudes/{sid}/llave", json={"nota": ""}, headers=SUPER)
        self.assertEqual(r.status_code, 409)
        self.assertEqual(r.get_json()["error_codigo"], "CHOFER_SIN_USUARIO")
        self.assertEqual(self.base.sol[sid]["estado"], "confirmada")

    def test_editar_el_telefono_de_un_externo_antiguo(self):
        cid = self._externo_antiguo()
        d = dict(TS._datos_chofer(9), nombre_completo="Pedro Ignacio Lagos Fuentes", rut="11.111.111-1", telefono="+56 2 2345 6789")
        d.pop("usuario_id", None)
        r = self.c.put(f"/flota/api/choferes/{cid}", json=d, headers=SUPER)
        self.assertEqual(r.status_code, 200, r.get_json())
        self.assertEqual(r.get_json()["chofer"]["telefono"], "+56223456789")

    def test_un_usuario_que_ya_es_chofer_no_vacia_la_lista_por_un_externo(self):
        self._externo_antiguo()
        r = self.c.get("/flota/api/choferes/usuarios", headers=SUPER)
        self.assertEqual(r.status_code, 200)
        self.assertTrue(r.get_json()["usuarios"])

    def test_repositorio_trae_vigentes_y_anteriores_solo_con_permiso(self):
        vid = self.c.post("/flota/api/vehiculos", json={"patente": "TVDZ88-5", "tipo": "Furgón", "licencia_clases": ["B"]},
                          headers=SUPER).get_json()["vehiculo"]["id"]
        for nombre in ("SOAP 2025.pdf", "SOAP 2026.pdf"):
            import io
            self.c.post("/flota/api/archivos", content_type="multipart/form-data", headers=SUPER, data={
                "entidad": "vehiculo", "entidad_id": str(vid), "tipo": "soap", "archivo": (io.BytesIO(PDF), nombre, "application/pdf")})
        r = self.c.get(f"/flota/api/vehiculos/{vid}/archivos", headers=SUPER)
        self.assertEqual(r.status_code, 200, r.get_json())
        a = r.get_json()["archivos"]
        self.assertEqual([x["nombre"] for x in a], ["SOAP 2026.pdf", "SOAP 2025.pdf"])
        self.assertEqual([x["vigente"] for x in a], [True, False])
        self.assertEqual(a[0]["etiqueta"], "SOAP")
        r = self.c.get(f"/flota/api/vehiculos/{vid}/archivos", headers=SOLO_PEDIR)
        self.assertEqual(r.status_code, 403)


# ═══════════════════════════════════════════════════════════════════════════
#  D. La llave «administrar» (03-nov-2026): Aarón agrega vehículos y choferes sin ser superadministrador
# ═══════════════════════════════════════════════════════════════════════════
ADMINISTRA = {"X-Rol": "ejecutivo", "X-Flota": "flota_administrar"}


class TestLlaveAdministrar(Base2):
    def test_quien_administra_agrega_edita_archiva_y_sube_archivos(self):
        r = self.c.post("/flota/api/vehiculos", json=TS_VEH, headers=ADMINISTRA)
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        vid = r.get_json()["vehiculo"]["id"]
        self.assertEqual(self.c.put(f"/flota/api/vehiculos/{vid}", json=dict(TS_VEH, color="Rojo"), headers=ADMINISTRA).status_code, 200)
        self.assertEqual(self.c.post(f"/flota/api/vehiculos/{vid}/archivar", json={"motivo": "Vendido hoy"}, headers=ADMINISTRA).status_code, 200)
        r = self.c.post("/flota/api/choferes", json=dict(TS._datos_chofer(4), usuario_id=4), headers=ADMINISTRA)
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        self.assertEqual(self.subir("vehiculo", self.furgon, "soap", PDF, headers=ADMINISTRA).status_code, 200)

    def test_entra_a_la_pantalla_y_ve_lo_que_mantiene(self):
        self.assertEqual(self.c.get("/flota", headers=ADMINISTRA).status_code, 200)
        d = self.c.get("/flota/api/vehiculos", headers=ADMINISTRA).get_json()
        self.assertEqual(d["permisos"], {"admin": True, "gestionar": False, "documentos": True, "solicitar": True})
        self.assertIsNotNone(self.veh_json(ADMINISTRA)["vin"])      # lo que mantiene, lo ve

    def test_administrar_no_es_gestionar(self):
        r = self.c.post("/flota/api/solicitudes", json=TS._sol(vehiculo_id=self.furgon, chofer_id=self.ana), headers=SOLO_PEDIR)
        sid = r.get_json()["solicitud"]["id"]
        for ruta in ("confirmar", "llave"):
            self.assertEqual(self.c.post(f"/flota/api/solicitudes/{sid}/{ruta}", json={}, headers=ADMINISTRA).status_code, 403, ruta)
        self.assertEqual(self.c.get(f"/flota/api/vehiculos/{self.furgon}/historial", headers=ADMINISTRA).status_code, 403)

    def test_gestionar_solo_sigue_sin_poder_agregar(self):
        self.assertEqual(self.c.post("/flota/api/vehiculos", json=TS_VEH, headers=GESTOR).status_code, 403)
        self.assertEqual(self.c.post("/flota/api/choferes", json=dict(TS._datos_chofer(4), usuario_id=4), headers=GESTOR).status_code, 403)

    def test_el_front_no_lo_exime_de_las_24_horas(self):
        js = _leer("static", "flota_uso.js")
        i = js.index("function limite24Base()")
        self.assertIn("PERM.gestionar ? null", js[i:i + 140])
        self.assertNotIn("PERM.admin) ? null", js[i:i + 140])
