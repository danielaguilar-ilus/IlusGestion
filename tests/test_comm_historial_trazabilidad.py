"""Trazabilidad de correos + Historial paginado de Comunicaciones.

POR QUÉ EXISTE ESTE ARCHIVO
Daniel (2026-09-23): la pestaña Comunicaciones → Historial "no muestra los
correos de agosto" y quiere saber, correo por correo, SI se envió o NO y
QUÉ se envió. Causa raíz: la pestaña leía email_log LIMIT 60 + comm_log
LIMIT 60 y cortaba en 80, sin filtros de servidor; con las llaves de paso
cerradas se generan ~100 bloqueos al día, así que la ventana se llenaba en
horas y los envíos reales nunca aparecían. Además ningún correo guardaba
su contenido y los "fallido" sin excepción quedaban sin motivo.

Estos tests leen el CÓDIGO FUENTE de app.py con `ast` (importar app.py
exige BD y credenciales) y ejecutan aislados los helpers puros: filtros de
lista blanca, armado del WHERE solo con %s, enmascarado de tokens,
privacidad del cuerpo y reconocimiento de referencias.

    py -m unittest tests.test_comm_historial_trazabilidad -v
"""
import ast
import datetime
import json
import re
import unittest
import zlib

APP_SRC = open("app.py", encoding="utf-8", errors="ignore").read()
_ARBOL = ast.parse(APP_SRC)


def _funcion(nombre):
    for nodo in ast.walk(_ARBOL):
        if isinstance(nodo, ast.FunctionDef) and nodo.name == nombre:
            return nodo
    raise AssertionError(f"No se encontró la función '{nombre}' en app.py")


def _fuente_funcion(nombre):
    return ast.unparse(_funcion(nombre))


def _asignacion(nombre):
    for nodo in _ARBOL.body:
        if isinstance(nodo, ast.Assign):
            for t in nodo.targets:
                if isinstance(t, ast.Name) and t.id == nombre:
                    return ast.unparse(nodo)
    raise AssertionError(f"No se encontró la constante '{nombre}' en app.py")


_NS = {"re": re, "json": json, "datetime": datetime.datetime,
       "timedelta": datetime.timedelta, "timezone": datetime.timezone}
for _c in ("_EMAIL_LOG_MODULOS_PRIVADOS", "_EMAIL_LOG_CUERPO_MAX_GZ",
           "_EMAIL_LOG_CUERPO_RETENCION_DIAS", "_EMAIL_REF_TK", "_EMAIL_REF_OT",
           "_EMAIL_REF_RET", "_EMAIL_TOKEN_RE", "_EMAIL_PIEZA_LIMPIA", "_EMAIL_URL_CABEZA", "_COMM_HIST_ESTADOS",
           "_COMM_HIST_PER_PAGE", "_COMM_HIST_MODULO_LABELS", "_COMM_HIST_MARCA_DUP"):
    exec(_asignacion(_c), _NS)
for _f in ("_email_normalize_attachments", "_email_log_es_frase", "_email_log_enmascarar_url",
           "_email_log_enmascarar_tokens", "_email_log_adjuntos_meta",
           "_email_log_norm_ref", "_email_log_cuerpo_fila", "_comm_hist_filtros",
           "_comm_hist_rango_utc", "_comm_hist_like", "_comm_hist_ramas",
           "_comm_hist_llave"):
    exec(_fuente_funcion(_f), _NS)


class TestEnmascaradoDeTokens(unittest.TestCase):

    TOKEN = "Zx9aK3LmQ7pR2sT5vW8yB1nC4dF6gH0j"   # 32 chars, letras + dígitos

    def test_tapa_token_en_href_y_en_texto_visible(self):
        link = f"https://ilus.cl/ot/firma/{self.TOKEN}"
        html = f"<p>Firma aquí: <a href='{link}'>{link}</a></p>"
        out, cambio = _NS["_email_log_enmascarar_tokens"](html)
        self.assertTrue(cambio)
        self.assertNotIn(self.TOKEN, out, "el token de firma quedó legible en la copia guardada")
        self.assertIn("Zx9a…oculto", out)

    def test_tapa_token_en_query_y_seguimiento_publico(self):
        html = (f'<a href="https://ilus.cl/t/{self.TOKEN}">Seguimiento</a>'
                f'<a href="https://ilus.cl/anexo?token={self.TOKEN}">Anexo</a>')
        out, _ = _NS["_email_log_enmascarar_tokens"](html)
        self.assertNotIn(self.TOKEN, out)

    def test_no_toca_imagenes_ni_slugs(self):
        html = ('<img src="https://ilus.cl/f/fotos/AbCdEfGh12345678901234567890">'
                '<a href="https://ilusfitness.com/pages/soporte-tecnico">Soporte</a>')
        out, cambio = _NS["_email_log_enmascarar_tokens"](html)
        self.assertFalse(cambio)
        self.assertEqual(out, html)

    # ── 2026-09-30 (revisión adversarial): el host no se tapa; slugs/archivos legibles sí se conservan;
    #    los tokens SIN dígitos también se tapan ──
    def test_el_host_de_cloud_run_no_se_enmascara(self):
        link = "https://ilus-app-469212710544.southamerica-west1.run.app/retiros/solicitar"
        out, cambio = _NS["_email_log_enmascarar_tokens"](f'<a href="{link}">Solicitar</a>')
        self.assertFalse(cambio, "el host se tomó por un token y el correo quedó 'no reenviable'")
        self.assertIn(link, out)

    def test_slugs_de_producto_y_nombres_de_archivo_se_conservan(self):
        for ruta in ("/products/mancuerna-hexagonal-ajustable-20-kg",
                     "/cdn/shop/files/Logo_ILUS_Fitness_Blanco_equipamiento_para_gimnasios.png",
                     "/pages/soporte-tecnico-y-garantia-de-equipos"):
            html = f'<a href="https://ilusfitness.com{ruta}">x</a>'
            out, cambio = _NS["_email_log_enmascarar_tokens"](html)
            self.assertFalse(cambio, ruta)
            self.assertEqual(out, html)

    def test_tokens_sin_digitos_tambien_se_tapan(self):
        tokens = ["AbCdEfGhIjKlMnOpQrStUvWxYzAbCdEf",        # 32 letras, sin dígitos (antes quedaba legible)
                  "abcdefghijklmnopqrstuvwxyzabcdef",        # 32 minúsculas seguidas
                  "Kq_Zr9Tm-XpVb3Nc_JdHf7Gs-LwYu2Ae_Rt5",       # con guiones y guiones bajos
                  "3f2504e0-4f89-11d3-9a0c-0305e82c3301",    # uuid con guiones
                  "3f2504e04f8911d39a0c0305e82c3301"]        # uuid hex
        for tk in tokens:
            out, cambio = _NS["_email_log_enmascarar_tokens"](f'<a href="https://ilus.cl/t/{tk}">Ver</a>')
            self.assertTrue(cambio, tk)
            self.assertNotIn(tk, out, tk)

    def test_token_corto_de_16_bytes_se_tapa(self):
        tk = "Qw3rTy9uIo2pAs5dFg8hJk"   # token_urlsafe(16) = 22 caracteres
        out, cambio = _NS["_email_log_enmascarar_tokens"](f'<a href="https://ilus.cl/anexo?token={tk}">x</a>')
        self.assertTrue(cambio)
        self.assertNotIn(tk, out)

    def test_la_frase_legible_se_reconoce_sin_confundirla_con_un_secreto(self):
        es = _NS["_email_log_es_frase"]
        self.assertTrue(es("mancuerna-hexagonal-20-kg"))
        self.assertTrue(es("Logo_ILUS_Fitness_Blanco"))
        self.assertFalse(es("abcdefghijklmnopqrstuvwxyz"))          # una sola pieza
        self.assertFalse(es("Zx9aK3LmQ7pR2sT5vW8yB1nC4dF6gH0j"))
        self.assertFalse(es("3f2504e0-4f89-11d3-9a0c-0305e82c3301"))


class TestCuerpoYPrivacidad(unittest.TestCase):

    def test_comunicacion_interna_no_guarda_contenido(self):
        gz, n, adj, red = _NS["_email_log_cuerpo_fila"]("<p>Tu clave: link</p>", "comunicacion_interna")
        self.assertIsNone(gz)
        self.assertEqual(red, 1)
        self.assertGreater(n, 0)

    def test_cuerpo_normal_se_comprime_y_recupera_igual(self):
        html = "<h1>Hola ñandú</h1>" * 50
        gz, n, adj, red = _NS["_email_log_cuerpo_fila"](html, "transporte")
        self.assertEqual(red, 0)
        self.assertEqual(zlib.decompress(gz).decode("utf-8"), html)
        self.assertEqual(n, len(html.encode("utf-8")))
        self.assertIsNone(adj)

    def test_cuerpo_con_token_queda_marcado_como_enmascarado(self):
        html = '<a href="https://ilus.cl/t/Zx9aK3LmQ7pR2sT5vW8yB1nC4dF6gH0j">Ver</a>'
        _, _, _, red = _NS["_email_log_cuerpo_fila"](html, "transporte")
        self.assertEqual(red, 2)

    def test_adjuntos_solo_nombre_tamano_tipo(self):
        _, _, adj, _ = _NS["_email_log_cuerpo_fila"](
            "<p>x</p>", "mantenciones", [("informe.pdf", b"%PDF-contenido-secreto", "application/pdf")])
        datos = json.loads(adj)
        self.assertEqual(datos, [{"nombre": "informe.pdf", "bytes": 22, "tipo": "application/pdf"}])
        self.assertNotIn("contenido-secreto", adj)


class TestReferencias(unittest.TestCase):

    def test_ref_explicita(self):
        self.assertEqual(_NS["_email_log_norm_ref"]({"tipo": "ret", "id": "12", "codigo": "RET-7K2Q9M"}),
                         ("RET", 12, "RET-7K2Q9M"))

    def test_se_reconoce_en_el_asunto(self):
        f = _NS["_email_log_norm_ref"]
        self.assertEqual(f(None, "ILUS · TK-2026-00012 respuesta"), ("TK", 12, "TK-2026-00012"))
        self.assertEqual(f(None, "Firma tu orden de trabajo OT-2026-00179"), ("OT", None, "OT-2026-00179"))
        self.assertEqual(f(None, "Recordatorio de tu retiro RET-7K2Q9M"), ("RET", None, "RET-7K2Q9M"))
        self.assertEqual(f(None, "Bienvenido a ILUS"), (None, None, None))

    def test_id_fuera_de_rango_se_descarta(self):
        self.assertEqual(_NS["_email_log_norm_ref"]({"tipo": "OT", "id": 10**12})[1], None)


class TestFiltrosListaBlanca(unittest.TestCase):

    def test_valores_invalidos_vuelven_al_default(self):
        f = _NS["_comm_hist_filtros"]({"page": "-3", "per_page": "9999", "estado": "DROP TABLE",
                                       "modulo": "x' OR 1=1 --", "desde": "no-es-fecha"})
        self.assertEqual((f["page"], f["per_page"], f["estado"], f["modulo"], f["desde"]),
                         (1, 100, "", "", None))

    def test_fechas_invertidas_se_ordenan(self):
        f = _NS["_comm_hist_filtros"]({"desde": "2026-08-31", "hasta": "2026-08-01"})
        self.assertLess(f["desde"], f["hasta"])

    def test_ref_por_tipo_id_o_por_codigo(self):
        f = _NS["_comm_hist_filtros"]({"ref": "OT:55"})
        self.assertEqual((f["ref_tipo"], f["ref_id"], f["ref_codigo"]), ("OT", 55, None))
        f = _NS["_comm_hist_filtros"]({"ref": "ret-7k2q9m"})
        self.assertEqual((f["ref_tipo"], f["ref_codigo"]), (None, "RET-7K2Q9M"))

    def test_rango_de_agosto_en_hora_chile(self):
        # Agosto en Chile = UTC-4 (sin horario de verano todavía).
        d0, d1 = _NS["_comm_hist_rango_utc"](datetime.date(2026, 8, 1), datetime.date(2026, 8, 31))
        self.assertEqual(d0, datetime.datetime(2026, 8, 1, 4, 0))
        self.assertEqual(d1, datetime.datetime(2026, 9, 1, 4, 0))


class TestWhereSoloConPlaceholders(unittest.TestCase):

    def _filtros(self, **kw):
        f = _NS["_comm_hist_filtros"](kw)
        f["desde_utc"], f["hasta_utc"] = _NS["_comm_hist_rango_utc"](f["desde"], f["hasta"])
        return f

    def test_texto_del_usuario_nunca_entra_al_sql(self):
        malicioso = "'; DELETE FROM email_log; --"
        ramas = _NS["_comm_hist_ramas"](self._filtros(q=malicioso, desde="2026-08-01", hasta="2026-08-31"))
        self.assertEqual(set(ramas), {"email_log", "comm_log", "cola"})
        for nombre, (conds, params) in ramas.items():
            sql = " AND ".join(conds)
            self.assertNotIn("DELETE", sql, nombre)
            self.assertEqual(sql.count("%s"), len(params), f"placeholders ≠ params en {nombre}")

    def test_en_cola_solo_consulta_la_cola(self):
        ramas = _NS["_comm_hist_ramas"](self._filtros(estado="en_cola"))
        self.assertEqual(set(ramas), {"cola"})

    def test_pruebas_manuales_solo_comm_log(self):
        ramas = _NS["_comm_hist_ramas"](self._filtros(modulo="prueba_manual"))
        self.assertEqual(set(ramas), {"comm_log"})

    def test_conteo_ignora_el_filtro_de_estado(self):
        ramas = _NS["_comm_hist_ramas"](self._filtros(estado="en_cola"), con_estado=False)
        self.assertEqual(set(ramas), {"email_log", "comm_log", "cola"})

    def test_comm_log_excluye_los_espejos_del_envio_manual(self):
        conds, params = _NS["_comm_hist_ramas"](self._filtros())["comm_log"]
        self.assertIn("COALESCE(c.detalle,'') NOT LIKE %s", conds)
        self.assertIn(_NS["_COMM_HIST_MARCA_DUP"], params)

    def test_like_escapa_comodines(self):
        self.assertEqual(_NS["_comm_hist_like"]("100%_real"), "%100\\%\\_real%")


class TestLlave(unittest.TestCase):

    def test_nombre_de_la_llave(self):
        f = _NS["_comm_hist_llave"]
        self.assertIn("general", f("Email deshabilitado por superadmin (kill switch global ON)", "transporte"))
        self.assertEqual(f('Llave de paso cerrada para módulo "transporte"', "transporte"),
                         "Llave de paso de Transporte")


class TestContratosDelCodigo(unittest.TestCase):
    """Reglas que no se ven en un test de función aislada."""

    def test_ref_es_parametro_explicito_y_no_viaja_a_send_real(self):
        fn = _funcion("_send_ilus_email")
        self.assertIn("ref", [a.arg for a in fn.args.kwonlyargs],
                      "ref debe ser parámetro propio (no **kwargs): _send_ilus_email_real no lo conoce")
        for nodo in ast.walk(fn):
            if isinstance(nodo, ast.Call) and getattr(nodo.func, "id", "") == "_send_ilus_email_real":
                self.assertNotIn("ref", [k.arg for k in nodo.keywords])

    def test_la_cola_no_mete_ref_en_kwargs_json(self):
        src = _fuente_funcion("_send_ilus_email")
        self.assertIn('for k in ("cc", "reply_to")', src.replace("'", '"'))
        self.assertIn("ref_json", src)

    def test_email_log_solo_cierra_su_propia_conexion(self):
        src = _fuente_funcion("_email_log")
        self.assertIn("if propia and conn is not None:", src)
        self.assertEqual(src.count("conn.close()"), 1)

    def test_cuerpo_se_sirve_con_csp_estricta(self):
        src = _fuente_funcion("comm_log_cuerpo")
        self.assertIn("default-src 'none'; img-src https: data:; style-src 'unsafe-inline'", src)

    def test_historial_y_cuerpo_exigen_superadmin_como_comunicaciones(self):
        # 2026-09-30: el reenvío manda el HTML íntegro a clientes reales, así que también es solo superadmin
        for nombre in ("comm_index", "comm_historial_api", "comm_log_cuerpo", "comm_log_reintentar"):
            decos = [ast.unparse(d) for d in _funcion(nombre).decorator_list]
            self.assertIn("_require_superadmin", decos, nombre)

    def test_la_autorreparacion_del_historial_solo_corre_ante_errores_de_esquema(self):
        src = _fuente_funcion("comm_historial_api")
        self.assertIn("(1054, 1146)", src)                      # columna o tabla inexistente
        self.assertIn("_COMM_HIST_ENSURE_ULTIMO", src)           # y a lo más una vez por minuto
        # el ensure (DDL + relleno) no se llama a ciegas ante cualquier excepción
        self.assertLess(src.index("(1054, 1146)"), src.index("_ensure_email_log_trazabilidad()"))


if __name__ == "__main__":
    unittest.main()
