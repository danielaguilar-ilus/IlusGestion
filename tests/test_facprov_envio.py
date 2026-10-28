# -*- coding: utf-8 -*-
"""📧 Envío de lotes de Facturación de proveedor por correo (Daniel, 28-oct-2026): «un envío de lotes seleccionados, con un mensaje y los Excel y
los montos en el mail, y un cuerpo de correo bien automático, que soy flojo». Datos y base de mentira; nunca se manda un correo de verdad."""
import datetime
import email
import email.policy
import io
import json
import unittest

import facprov_envio as F


# ───────────────────────── lógica pura ─────────────────────────
class TestNombresCortos(unittest.TestCase):
    def test_como_los_llama_la_gente(self):
        self.assertEqual(F.nombre_corto("LOGISTICA Y TRANSPORTES MILLING SPA"), "Milling")
        self.assertEqual(F.nombre_corto("Transportes felcarm SPA"), "Felcarm")
        self.assertEqual(F.nombre_corto("DAP Servicio"), "DAP")
        self.assertEqual(F.nombre_corto("Isabel Milling"), "Isabel Milling")

    def test_nunca_queda_vacio(self):
        self.assertEqual(F.nombre_corto("Servicios y Transportes SPA"), "Servicios y Transportes SPA")
        self.assertEqual(F.nombre_corto(""), "")
        self.assertEqual(F.nombre_corto(None), "")

    def test_la_misma_empresa_escrita_distinto_tiene_la_misma_clave(self):
        self.assertEqual(F.clave_proveedor("  MILLING   SpA "), F.clave_proveedor("milling spa"))
        self.assertEqual(F.clave_proveedor("Logística"), F.clave_proveedor("LOGISTICA"))

    def test_la_etiqueta_guardada_manda_sobre_la_automatica(self):
        guardadas = {F.clave_proveedor("DAP Servicio"): "Pulgar"}
        self.assertEqual(F.etiqueta_de("DAP Servicio", guardadas), "Pulgar")
        self.assertEqual(F.etiqueta_de("dap  servicio", guardadas), "Pulgar")
        self.assertEqual(F.etiqueta_de("Transportes felcarm SPA", guardadas), "Felcarm")

    def test_solo_se_recuerda_lo_que_la_persona_cambio(self):
        auto = {F.clave_proveedor("DAP Servicio"): "DAP"}
        lineas = [{"proveedor": "DAP Servicio", "etiqueta": "Pulgar"}, {"proveedor": "Transportes felcarm SPA", "etiqueta": "Felcarm"}]
        out = F.etiquetas_a_guardar(lineas, auto, {})
        self.assertEqual(out, {F.clave_proveedor("DAP Servicio"): "Pulgar"})          # Felcarm es el automático: no se guarda
        # si vuelve al automático, se olvida
        out2 = F.etiquetas_a_guardar([{"proveedor": "DAP Servicio", "etiqueta": "DAP"}], auto, out)
        self.assertEqual(out2, {})

    def test_el_json_guardado_no_pasa_el_tope_y_se_van_las_mas_antiguas(self):
        previas = {f"prov{i}": "x" * 30 for i in range(30)}
        out = F.etiquetas_a_guardar([{"proveedor": "Nuevo Proveedor", "etiqueta": "Otro"}], {}, previas, tope_chars=200)
        import json
        self.assertLessEqual(len(json.dumps(out, ensure_ascii=False)), 200)
        self.assertEqual(out[F.clave_proveedor("Nuevo Proveedor")], "Otro")            # lo recién cambiado se queda


class TestCorreosInternos(unittest.TestCase):
    def test_solo_correos_internos_sin_repetir(self):
        v, r = F.correos_internos("Nicolás Bastías <Nicolas.Bastias@sphs.cl>; victor.betancur@sphs.cl, nicolas.bastias@sphs.cl")
        self.assertEqual(v, ["nicolas.bastias@sphs.cl", "victor.betancur@sphs.cl"])
        self.assertEqual(r, [])

    def test_rechaza_externos_y_parecidos(self):
        v, r = F.correos_internos(["contador@gmail.com", "x@mail.sphs.cl", "x@sphs.cl.com", "x@evilsphs.cl", "x..y@sphs.cl", "sin-arroba", "ok@ilusfitness.com"])
        self.assertEqual(v, ["ok@ilusfitness.com"])
        self.assertEqual(len(r), 6)

    def test_entradas_raras_no_rompen(self):
        self.assertEqual(F.correos_internos(None), ([], []))
        self.assertEqual(F.correos_internos(""), ([], []))
        self.assertEqual(F.correos_internos(["", None, "  "]), ([], []))

    def test_el_saludo_sale_del_primer_nombre_o_de_lo_que_se_guardo(self):
        self.assertEqual(F.saludo_defecto("Nicolás Bastías"), "Hola Nicolás,")
        self.assertEqual(F.saludo_defecto("nicolas.bastias@sphs.cl"), "Hola Nicolas,")
        self.assertEqual(F.saludo_defecto("Nicolás Bastías", "Nico"), "Nico,")
        self.assertEqual(F.saludo_defecto("Nicolás Bastías", "Nico,"), "Nico,")
        self.assertEqual(F.saludo_defecto("", ""), "Hola,")


LOTES = [{"id": 21, "proveedor": "LOGISTICA Y TRANSPORTES MILLING SPA", "monto": 4750000, "archivo": "lote_21_X.xlsx"},
         {"id": 20, "proveedor": "DAP Servicio", "monto": 1750000, "archivo": "lote_20_X.xlsx"},
         {"id": 22, "proveedor": "Transportes felcarm SPA", "monto": 1230000, "archivo": "lote_22_X.xlsx"}]


class TestElCuerpo(unittest.TestCase):
    def _cuerpo(self, **kw):
        lineas = F.armar_lineas(LOTES, {F.clave_proveedor("DAP Servicio"): "Pulgar"})
        args = dict(saludo="Nico,", intro=F.INTRO_DEFECTO, lineas=lineas, cierre=F.CIERRE_DEFECTO,
                    firma=F.firma_de("Juan Daniel Aguilar", "Encargado de Servicio Técnico", "+56 9 7746 8766", "daniel.aguilar@sphs.cl"))
        args.update(kw)
        return F.armar_cuerpo(**args)

    def test_es_el_correo_que_daniel_armaba_a_mano(self):
        c = self._cuerpo()
        self.assertEqual(c["texto"], "\n".join([
            "Nico,", "", "Un gusto saludarte. Te envío el cierre de los proveedores con los siguientes montos:", "",
            "1. Milling $4.750.000", "2. Pulgar $1.750.000", "3. Felcarm $1.230.000", "Total $7.730.000", "",
            "Cualquier duda me comentas.", "", "Atte.", "Juan Daniel Aguilar", "Encargado de Servicio Técnico", "+56 9 7746 8766",
            "daniel.aguilar@sphs.cl"]) + "\n")

    def test_el_html_trae_lo_mismo_en_lista_numerada(self):
        h = self._cuerpo()["html"]
        self.assertIn("<ol", h)
        self.assertIn("Milling <b>$4.750.000</b>", h)
        self.assertIn("Pulgar <b>$1.750.000</b>", h)
        self.assertIn("Total <b>$7.730.000</b>", h)
        self.assertIn("Nico,", h)
        self.assertIn("Atte.<br>Juan Daniel Aguilar", h)

    def test_con_un_solo_lote_no_hay_total(self):
        c = self._cuerpo(lineas=F.armar_lineas(LOTES[:1]))
        self.assertNotIn("Total", c["texto"])
        self.assertNotIn("Total", c["html"])

    def test_lo_que_escribe_una_persona_se_escapa_en_el_html(self):
        c = self._cuerpo(saludo="<b>Hola</b>,", intro="a & b <script>alert(1)</script>", cierre="x<y")
        self.assertNotIn("<script>", c["html"])
        self.assertIn("&lt;script&gt;", c["html"])
        self.assertIn("a &amp; b", c["html"])
        self.assertIn("x&lt;y", c["html"])
        self.assertIn("<script>", c["texto"])                      # el texto plano va tal cual

    def test_un_mensaje_en_varios_parrafos_se_respeta(self):
        h = self._cuerpo(cierre="Primera línea\nsegunda\n\nOtro párrafo")["html"]
        self.assertIn("Primera línea<br>segunda", h)
        self.assertIn("Otro párrafo", h)

    def test_sin_saludo_ni_cierre_igual_sale(self):
        c = self._cuerpo(saludo="", cierre="", intro="")
        self.assertTrue(c["texto"].startswith("1. Milling"))

    def test_el_asunto(self):
        hoy = datetime.date(2026, 10, 28)
        lineas = F.armar_lineas(LOTES)
        self.assertEqual(F.asunto_defecto(lineas, hoy), "Cierre de proveedores — 3 lotes (28/10/2026)")
        self.assertEqual(F.asunto_defecto(lineas[:1], hoy), "Cierre de proveedor — Milling (28/10/2026)")

    def test_el_total_suma_lo_que_se_ve(self):
        self.assertEqual(F.total_de(F.armar_lineas(LOTES)), 7730000)
        self.assertEqual(F.total_de([]), 0)

    def test_la_firma_omite_lo_que_no_se_sabe(self):
        self.assertEqual(F.firma_de("Ana Pérez"), ["Atte.", "Ana Pérez"])
        self.assertEqual(F.firma_de("", "", "", ""), ["Atte."])


class TestElBorradorParaOutlook(unittest.TestCase):
    def _eml(self):
        c = F.armar_cuerpo("Nico,", F.INTRO_DEFECTO, F.armar_lineas(LOTES), F.CIERRE_DEFECTO, F.firma_de("Juan Daniel Aguilar"))
        raw = F.armar_eml(["nicolas.bastias@sphs.cl"], ["victor.betancur@sphs.cl"], "Cierre de proveedores — 3 lotes (28/10/2026)",
                          c["texto"], c["html"], [("lote_21_X.xlsx", b"PK\x03\x04uno"), ("lote_20_X.xlsx", b"PK\x03\x04dos")])
        return raw, email.message_from_bytes(raw, policy=email.policy.default)

    def test_outlook_lo_abre_como_correo_sin_enviar(self):
        raw, m = self._eml()
        self.assertEqual(m["X-Unsent"], "1")
        self.assertEqual(m["To"], "nicolas.bastias@sphs.cl")
        self.assertEqual(m["Cc"], "victor.betancur@sphs.cl")
        self.assertEqual(m["Subject"], "Cierre de proveedores — 3 lotes (28/10/2026)")       # con la raya larga, bien codificada
        self.assertIn(b"\r\n", raw)                                                          # saltos de línea de correo (CRLF)

    def test_trae_el_texto_el_html_y_los_excel_con_su_contenido(self):
        raw, m = self._eml()
        cuerpo = m.get_body(preferencelist=("plain",)).get_content()
        self.assertIn("1. Milling $4.750.000", cuerpo)
        self.assertIn("<ol", m.get_body(preferencelist=("html",)).get_content())
        adj = {p.get_filename(): p.get_payload(decode=True) for p in m.iter_attachments()}
        self.assertEqual(adj, {"lote_21_X.xlsx": b"PK\x03\x04uno", "lote_20_X.xlsx": b"PK\x03\x04dos"})
        for p in m.iter_attachments():
            self.assertEqual(p.get_content_type(), F.MIME_XLSX)

    def test_sin_copia_ni_adjuntos_tambien_sirve(self):
        raw = F.armar_eml(["a@sphs.cl"], [], "x", "t", "<p>t</p>", [])
        m = email.message_from_bytes(raw, policy=email.policy.default)
        self.assertIsNone(m["Cc"])
        self.assertEqual(list(m.iter_attachments()), [])

    def test_el_nombre_del_borrador(self):
        self.assertEqual(F.nombre_borrador(datetime.date(2026, 10, 28)), "cierre_proveedores_28-10-2026.eml")


class TestLoQueSeRecuerda(unittest.TestCase):
    """La regla donde se guarda mide 500: lo recordado se acota botando lo más antiguo, y siempre es un JSON válido."""

    def test_cabe_tal_cual(self):
        self.assertEqual(json.loads(F.json_acotado({"a@sphs.cl": "Nico,"})), {"a@sphs.cl": "Nico,"})

    def test_vacio_es_un_json_vacio(self):
        self.assertEqual(F.json_acotado({}), "{}")
        self.assertEqual(F.json_acotado(None), "{}")

    def test_se_botan_las_mas_antiguas(self):
        d = {f"persona{i}@sphs.cl": "Hola " + "x" * 30 for i in range(30)}
        s = F.json_acotado(d, 300)
        self.assertLessEqual(len(s), 300)
        out = json.loads(s)
        self.assertIn("persona29@sphs.cl", out)         # la más nueva sobrevive
        self.assertNotIn("persona0@sphs.cl", out)       # la más antigua se fue

    def test_las_etiquetas_siguen_acotadas_igual(self):
        previas = {f"p{i}": "x" * 30 for i in range(12)}
        out = F.etiquetas_a_guardar([{"proveedor": "Nuevo Proveedor", "etiqueta": "Otro"}], {}, previas, tope_chars=200)
        self.assertLessEqual(len(json.dumps(out, ensure_ascii=False)), 200)
        self.assertEqual(out.get("nuevo proveedor"), "Otro")


class TestIdsQueLlegan(unittest.TestCase):
    def test_enteros_unicos_en_orden(self):
        self.assertEqual(F.ids_validos(["21", 20, "22", 20, "x", -1, 0, None, 3.0]), ([21, 20, 22, 3], False))

    def test_tope(self):
        ids, excedido = F.ids_validos(list(range(1, 40)), maximo=30)
        self.assertEqual((len(ids), excedido), (30, True))

    def test_lo_que_no_es_lista_se_ignora(self):
        self.assertEqual(F.ids_validos("21,22"), ([], False))
        self.assertEqual(F.ids_validos(None), ([], False))


if __name__ == "__main__":
    unittest.main()
