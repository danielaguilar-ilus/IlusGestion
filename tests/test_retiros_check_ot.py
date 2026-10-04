# -*- coding: utf-8 -*-
"""Movimientos (OT) de Check por documento: quién, cuándo, estado (retiros_check_ot.py, funciones puras).
Check es SOLO LECTURA (REGLA #4.4); estas pruebas no tocan red ni base de datos."""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import retiros_check_ot as ot  # noqa: E402


def fila(**kw):
    base = {"ot": "481516", "tipoOT": "PICKING", "estado": "TERMINADA", "doc": "FCV-0000010953", "entidad": "CLIENTE DEMO SPA",
            "feInicioOT": "2026-10-02T15:32:10", "fechaFin": "2026-10-02T15:40:55", "ua": "UA1012965", "codigo": "FZADI0275",
            "descripcion": "Set discos", "sol": 3, "ejec": 3}
    base.update(kw)
    return base


class TestDocumento(unittest.TestCase):
    def test_doc_clave_variantes(self):
        for v in ("FCV-0000010953", "FCV 10953", "fcv10953", " FCV/10953 ", "FCV-10953"):
            self.assertEqual(ot.doc_clave(v), ("FCV", "10953"), v)
        for v in (None, "", "10953", "FCV", "UA1012965X-"):
            self.assertIsNone(ot.doc_clave(v), v)

    def test_filas_del_documento(self):
        filas = [fila(), fila(ot="9", doc="BLV-0000023732"), fila(ot="8", doc="FCV-0000010954"), "basura", None,
                 {"ot": "7", "tipoDocumento": "FCV", "numeroDocumento": "0000010953"}]
        r = ot.filas_del_documento(filas, "fcv", "10953")
        self.assertEqual([f["ot"] for f in r], ["481516", "7"])
        self.assertEqual([f["ot"] for f in ot.filas_del_documento(filas, "BLV", "23732")], ["9"])
        self.assertEqual(ot.filas_del_documento(filas, "", "10953"), [])
        self.assertEqual(ot.filas_del_documento(filas, "FCV", "abc"), [])
        self.assertEqual(ot.filas_del_documento(None, "FCV", "10953"), [])

    def test_catalogo(self):
        self.assertEqual(ot.catalogo_campos([{"b": 1}, {"a": 2, "b": 3}, "x"]), ["a", "b"])
        self.assertEqual(ot.catalogo_campos(None), [])


class TestFechasYEtiquetas(unittest.TestCase):
    def test_etiquetas(self):
        self.assertEqual(ot.etiqueta("ot"), "N° de OT")
        self.assertEqual(ot.etiqueta("feInicioOT"), "Inicio")
        self.assertEqual(ot.etiqueta("usuarioPicking"), "Usuario picking")
        self.assertEqual(ot.etiqueta("fecha_asignacion"), "Fecha asignación")
        self.assertEqual(ot.etiqueta("fechaAsignacion"), "Fecha asignación")
        self.assertEqual(ot.etiqueta("ubiOrigen"), "Ubicación de origen")

    def test_momentos(self):
        self.assertEqual(ot.formatear_momento("2026-10-02T15:32:10"), "02/10/2026 15:32")
        self.assertEqual(ot.formatear_momento("2026-10-02"), "02/10/2026")
        self.assertEqual(ot.formatear_momento("02/10/2026 15:32"), "02/10/2026 15:32")
        # con zona horaria se pasa a hora de Chile (octubre = UTC-3)
        if ot._CHILE is not None:
            self.assertEqual(ot.formatear_momento("2026-10-02T18:30:00Z"), "02/10/2026 15:30")
        for v in (None, "", 20261002, "20261002", "UA1012965", "hola", True):
            self.assertIsNone(ot.formatear_momento(v), v)


class TestResumenOT(unittest.TestCase):
    def test_quien_y_cuando(self):
        f = fila(usuarioPicking="JPEREZ", usuAsignado="MGOMEZ", fechaAsignacion="2026-10-02T14:00:00")
        r = ot.resumir_ot([f])
        self.assertEqual((r["ot"], r["tipo"], r["estado"]), ("481516", "PICKING", "TERMINADA"))
        self.assertEqual((r["inicio"], r["fin"]), ("02/10/2026 15:32", "02/10/2026 15:40"))
        personas = {p["campo"]: p["valor"] for p in r["personas"]}
        self.assertEqual(personas, {"usuarioPicking": "JPEREZ", "usuAsignado": "MGOMEZ"})
        # los momentos van en orden: asignación, inicio, fin
        self.assertEqual([m["campo"] for m in r["momentos"]], ["fechaAsignacion", "feInicioOT", "fechaFin"])
        self.assertEqual(r["unidades"], 3)
        self.assertEqual(r["n_lineas"], 1)

    def test_no_confunde_cantidades_ni_ids_con_personas(self):
        f = fila(asignado=5, pickeado=3, usu_uid_usu="3f2b8c1e-1111-2222-3333-444455556666", ubiAsignada="A-01-02",
                 cantAsignada="4", entidad="CLIENTE DEMO SPA", sol=3)
        r = ot.resumir_ot([f])
        self.assertEqual(r["personas"], [])

    def test_codigo_numerico_de_usuario_solo_si_el_campo_lo_dice(self):
        r = ot.resumir_ot([fila(usuario=1534, asignado=5, cantAsignada=4)])
        self.assertEqual({p["campo"]: p["valor"] for p in r["personas"]}, {"usuario": "1534"})

    def test_momentos_con_nombres_de_creacion(self):
        r = ot.resumir_ot([fila(creadoEn="2026-10-02T10:00:00.1234567", horaSalida="02/10/2026 12:15")])
        etiquetas = {m["campo"]: m["valor"] for m in r["momentos"]}
        self.assertEqual(etiquetas["creadoEn"], "02/10/2026 10:00")
        self.assertEqual(etiquetas["horaSalida"], "02/10/2026 12:15")

    def test_persona_por_nombre_de_campo(self):
        f = fila(pickeadoPor="JPEREZ", responsable="Ana Soto", operador="OP-12")
        campos = {p["campo"] for p in ot.resumir_ot([f])["personas"]}
        self.assertEqual(campos, {"pickeadoPor", "responsable", "operador"})

    def test_todos_los_datos_quedan_visibles(self):
        f = fila(campoRaro="valor raro", vacio="", nulo=None)
        r = ot.resumir_ot([f])
        nombres = {d["campo"] for d in r["datos"]}
        self.assertIn("campoRaro", nombres)
        self.assertNotIn("vacio", nombres)
        self.assertNotIn("nulo", nombres)
        self.assertEqual(r["n_datos"], len(r["datos"]))
        inicio = next(d for d in r["datos"] if d["campo"] == "feInicioOT")
        self.assertEqual(inicio["valor"], "02/10/2026 15:32")

    def test_ot_sin_filas(self):
        self.assertIsNone(ot.resumir_ot([]))
        self.assertIsNone(ot.resumir_ot(None))

    def test_agrupa_por_ot_la_mas_reciente_primero(self):
        a1 = fila(ot="100", feInicioOT="2026-10-01T09:00:00", fechaFin="2026-10-01T09:30:00")
        a2 = fila(ot="100", codigo="OTRO", feInicioOT="2026-10-01T09:00:00", fechaFin="2026-10-01T09:30:00")
        b = fila(ot="200", feInicioOT="2026-10-02T10:00:00", fechaFin="2026-10-02T10:20:00")
        r = ot.agrupar_por_ot([a1, b, a2])
        self.assertEqual([o["ot"] for o in r], ["200", "100"])
        self.assertEqual(r[1]["n_lineas"], 2)

    def test_resumen_para_log_no_vuelca_clientes(self):
        f = fila(usuarioPicking="JPEREZ")
        txt = ot.resumen_para_log([f], [f, f])
        self.assertIn("filas_doc=1", txt)
        self.assertIn("usuarioPicking", txt)
        self.assertIn("JPEREZ", txt)
        self.assertNotIn("CLIENTE DEMO", txt)
        self.assertNotIn("Set discos", txt)

    def test_reconoce_los_formatos_reales_del_campo_doc_de_check(self):
        """Valores REALES del log de producción (03-oct-2026): Check escribe «FCV-0000011658» y también «FCV - 0000011658» (con espacios)."""
        filas = [fila(doc="BLV - 0000023751", ot="A1"), fila(doc="BLV-0000023751", ot="A2"), fila(doc="FCV - 0000011658", ot="B1"),
                 fila(doc="FCC-CMI084214G", ot="C1")]
        self.assertEqual(sorted(f["ot"] for f in ot.filas_del_documento(filas, "BLV", "23751")), ["A1", "A2"])
        self.assertEqual([f["ot"] for f in ot.filas_del_documento(filas, "FCV", "11658")], ["B1"])
        self.assertEqual(ot.filas_del_documento(filas, "FCC", "84214"), [])      # una factura de compra con código alfanumérico no es un documento de retiro

    def test_resumen_para_log_dice_como_escribe_check_el_campo_doc(self):
        filas = [fila(doc="FCV-0000011155"), fila(doc="BLV 23732"), fila(doc="FCV-0000011155"), fila(doc="")]
        txt = ot.resumen_para_log([], filas)
        self.assertIn("filas_doc=0", txt)
        self.assertIn("doc_ejemplos=['FCV-0000011155', 'BLV 23732']", txt)       # distintos, sin repetir y sin vacíos


if __name__ == "__main__":
    unittest.main()
