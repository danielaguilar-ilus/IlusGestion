# -*- coding: utf-8 -*-
"""ENCUESTA CORTA DE LA ORDEN DE TRABAJO «¿Cómo fue tu servicio?» (Daniel, 6-nov-2026) — REGLA #68.

Daniel: «podemos agregar una encuesta [al correo de OT terminada] con elegancia y sin aburrir con las respuestas, por favor», «que el correo se vea
profesional», «no me gusta tanta letra» y «que se vea bien en Outlook sin dañar celular ni Gmail».

Qué se vigila:
  1. El módulo puro (`ot_encuesta.py`): token HMAC con su propio propósito, validación de la nota 1–5, comentario limpio, cifras de resultados.
  2. El bloque del correo A PRUEBA DE OUTLOOK DE ESCRITORIO: tablas, `bgcolor` + `width` + `height` y el relleno en el <td>; sin <img>; sin padding ni margin en
     <table>/<a>; letra y números grandes; cinco celdas de ~20 %; todo escapado; ancho total <= 520 px.
  3. Las plantillas: la variable `{{encuesta_html}}` y la CADENA DE VERSIONES (la sembrada sin editar sube sola; la editada no).
  4. El interruptor `OT_ENCUESTA_ACTIVA` (nace APAGADA, no está en ningún archivo del repositorio, la vista previa y la prueba la muestran siempre) y a QUIÉN
     le llega la encuesta (solo al cliente: nunca al equipo ni a un técnico, porque quien toca un número responde).
  5. Las rutas con un Flask de mentira y una base en memoria: GET no guarda nada, POST guarda UNA vez, enlace inválido/vencido, límite de intentos, vista previa.
  6. Confidencialidad: un técnico (interno, elevado o externo) no ve resultados ni comentarios.
  7. Cableado, arranque barato (REGLA #18) y el navegador real (Chromium) a 390/820/1280 y el bloque del correo en una fila.

Todo con datos de mentira (REGLA #22). Correr:  python -m pytest tests/test_ot_encuesta.py -q -p no:warnings
"""
import base64
import json
import os
import re
import sqlite3
import time
import types
import unittest
from datetime import datetime, timedelta, timezone
from functools import wraps
from html.parser import HTMLParser
from unittest import mock

import comm_correos_registro as R
import ot_comunicacion as C
import ot_encuesta as E
from tests import _fuente_codigo as FC
from tests import _mundo_correos as MC
from tests.test_ot_comunicacion_envio import MundoOT, _html_para, _txt

RAIZ = FC.RAIZ
SECRETO = "clave-de-prueba"
CLAVE = "2026-10-21 14:30:05"
URL = "https://ilus.test/ot-encuesta/TOKEN"


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
#  Ayudas
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
class Analizador(HTMLParser):
    """Recorre un HTML de correo guardando cada etiqueta con sus atributos y su cadena de ancestros."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.pila, self.tags, self.texto = [], [], []

    def handle_starttag(self, tag, attrs):
        d = dict(attrs)
        self.tags.append({"tag": tag, "a": d, "ancestros": list(self.pila)})
        if tag not in ("br", "img", "meta", "link", "input", "hr"):
            self.pila.append({"tag": tag, "a": d})

    def handle_endtag(self, tag):
        for i in range(len(self.pila) - 1, -1, -1):
            if self.pila[i]["tag"] == tag:
                del self.pila[i:]
                break

    def handle_data(self, data):
        self.texto.append(data)


def analizar(html):
    p = Analizador()
    p.feed(html)
    p.close()
    return p


def _bloque():
    return E.bloque_html(URL)


class TestAyudasDelAnalizador(unittest.TestCase):
    def test_el_bloque_se_puede_leer(self):
        self.assertGreater(len(analizar(_bloque()).tags), 20)


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
#  1) EL MÓDULO PURO
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestToken(unittest.TestCase):
    def test_ida_y_vuelta_lleva_la_ot_y_la_aprobacion_del_cierre(self):
        t = E.generar_token(SECRETO, 77, CLAVE)
        v = E.validar_token(SECRETO, t)
        self.assertEqual((v["ok"], v["vid"], v["cierre_clave"]), (True, 77, CLAVE))

    def test_vigencia_de_45_dias(self):
        self.assertEqual(E.TOKEN_DIAS, 45)
        ahora = time.time()
        t = E.generar_token(SECRETO, 77, CLAVE, ahora=ahora)
        self.assertTrue(E.validar_token(SECRETO, t, ahora=ahora + 44 * 86400)["ok"])
        v = E.validar_token(SECRETO, t, ahora=ahora + 46 * 86400)
        self.assertEqual((v["ok"], v["motivo"], v["vid"]), (False, "vencido", 77))

    def test_adulterado_o_basura_es_invalido(self):
        t = E.generar_token(SECRETO, 77, CLAVE)
        cuerpo, firma = t.rsplit(".", 1)
        otro = base64.urlsafe_b64encode(f"v1|78|{int(time.time()) + 999999}|{CLAVE}".encode()).decode().rstrip("=")
        malos = ["", "basura", "a.b", cuerpo + "." + "0" * 24, otro + "." + firma, cuerpo, firma, t + "x", t[:-1], "../" + t, None,
                 "x" * 500 + "." + "0" * 24]
        for m in malos:
            v = E.validar_token(SECRETO, m)
            self.assertEqual((v["ok"], v["motivo"]), (False, "invalido"), repr(m)[:60])

    def test_otro_secreto_no_abre(self):
        t = E.generar_token(SECRETO, 77, CLAVE)
        self.assertFalse(E.validar_token("otra-clave", t)["ok"])
        self.assertFalse(E.validar_token("", t)["ok"])

    def test_tiene_su_propio_proposito_no_es_el_token_de_la_firma_remota(self):
        """El token de la firma remota (`_ot_firma_token`) es `base64(vid:exp:hmac24)` con la clave directa de la app: no debe abrir esta página."""
        import hashlib
        import hmac
        exp = int(time.time()) + 86400
        sig = hmac.new(SECRETO.encode(), f"77:{exp}".encode(), hashlib.sha256).hexdigest()[:24]
        firma_remota = base64.urlsafe_b64encode(f"77:{exp}:{sig}".encode()).decode().rstrip("=")
        self.assertFalse(E.validar_token(SECRETO, firma_remota)["ok"])
        # y al revés: el de la encuesta no tiene la forma del de la firma remota
        t = E.generar_token(SECRETO, 77, CLAVE)
        self.assertNotEqual(E._clave_derivada(SECRETO), SECRETO.encode())
        self.assertIn(".", t)
        self.assertFalse(re.fullmatch(r"[A-Za-z0-9_\-]+", t))

    def test_cada_aprobacion_del_cierre_tiene_su_token(self):
        a = E.generar_token(SECRETO, 77, "2026-10-21 14:30:05")
        b = E.generar_token(SECRETO, 77, "2026-10-28 09:00:00")
        self.assertNotEqual(a, b)
        self.assertEqual(E.validar_token(SECRETO, b)["cierre_clave"], "2026-10-28 09:00:00")

    def test_sin_ot_ni_clave_no_hay_token(self):
        self.assertEqual(E.generar_token(SECRETO, 0, CLAVE), "")
        self.assertEqual(E.generar_token(SECRETO, "x", CLAVE), "")
        self.assertEqual(E.generar_token(SECRETO, 77, ""), "")
        self.assertEqual(E.generar_token("", 77, CLAVE), "")

    def test_el_token_es_seguro_en_una_url(self):
        t = E.generar_token(SECRETO, 123456, CLAVE)
        self.assertRegex(t, r"^[A-Za-z0-9_\-]+\.[0-9a-f]{24}$")
        self.assertLess(len(t), 160)


class TestRespuesta(unittest.TestCase):
    def test_la_nota_es_un_solo_digito_del_1_al_5(self):
        for ok in ("1", "2", "3", "4", "5", " 4 "):
            self.assertEqual(E.puntaje_valido(ok), int(ok))
        for mal in ("", None, "0", "6", "9", "3.5", "07", "-1", "+3", "a", "１", "12", "5 5", "٣"):
            self.assertIsNone(E.puntaje_valido(mal), repr(mal))

    def test_el_comentario_se_limpia_pero_no_se_escapa(self):
        self.assertEqual(E.limpiar_comentario("  Todo muy bien  "), "Todo muy bien")
        self.assertEqual(E.limpiar_comentario("a\x00b\x07c\x1b[31m"), "abc[31m")                     # caracteres de control fuera
        self.assertEqual(E.limpiar_comentario("a‮b​c⁦d"), "abcd")                       # dirección de texto y ancho cero fuera
        self.assertEqual(E.limpiar_comentario("<script>alert(1)</script>"), "<script>alert(1)</script>")   # se guarda como texto; se ESCAPA al mostrarlo
        self.assertEqual(E.limpiar_comentario("uno\r\ndos\r\n\r\n\r\n\r\ntres"), "uno\ndos\n\ntres")
        self.assertEqual(E.limpiar_comentario("a\tb   c"), "a b c")
        self.assertEqual(E.limpiar_comentario(None), "")
        self.assertEqual(E.limpiar_comentario("uno dos tres"), "uno\ndos\ntres")                # separadores de línea de Unicode
        familia = "\U0001F468‍\U0001F469‍\U0001F467"
        self.assertEqual(E.limpiar_comentario("Gracias " + familia + " \U0001F44F"), "Gracias " + familia + " \U0001F44F")   # los emojis se conservan

    def test_el_comentario_se_recorta_a_300(self):
        self.assertEqual(E.MAX_COMENTARIO, 300)
        self.assertEqual(len(E.limpiar_comentario("x" * 1000)), 300)
        self.assertEqual(len(E.limpiar_comentario("ñ" * 301)), 300)

    def test_validar_respuesta(self):
        self.assertEqual(E.validar_respuesta("4", " Bien "), (4, "Bien", {}))
        n, c, errores = E.validar_respuesta("", "Hola")
        self.assertEqual((n, c), (None, "Hola"))
        self.assertIn("puntaje", errores)
        self.assertEqual(E.validar_respuesta("5", None)[:2], (5, ""))                                # el comentario es opcional

    def test_tono_del_agradecimiento(self):
        self.assertEqual([E.tono_gracias(n) for n in (1, 2, 3, 4, 5)], ["baja", "baja", "media", "alta", "alta"])
        self.assertEqual(E.tono_gracias(None), "alta")

    def test_estrellas_en_texto(self):
        self.assertEqual(E.estrellas_txt(4), "★★★★☆")
        self.assertEqual(E.estrellas_txt(5), "★★★★★")
        self.assertEqual(E.estrellas_txt(0), "☆☆☆☆☆")
        self.assertEqual(E.estrellas_txt("x"), "☆☆☆☆☆")

    def test_las_cifras_de_los_resultados(self):
        r = E.resumen({5: 6, 4: 2, 3: 1, 1: 1})
        self.assertEqual((r["total"], r["promedio"], r["satisfechos_pct"], r["por_revisar"]), (10, 4.2, 80, 1))
        self.assertEqual([b["nota"] for b in r["barras"]], [5, 4, 3, 2, 1])
        self.assertEqual([b["cantidad"] for b in r["barras"]], [6, 2, 1, 0, 1])
        self.assertEqual(sum(b["pct"] for b in r["barras"]), 100)
        vacio = E.resumen({})
        self.assertEqual((vacio["total"], vacio["promedio"], vacio["satisfechos_pct"], vacio["por_revisar"]), (0, None, None, 0))
        self.assertEqual([E.semaforo(x) for x in (None, 4.5, 4.0, 3.0)], ["gris", "verde", "ambar", "rojo"])

    def test_interruptor_nace_apagado(self):
        self.assertEqual(E.ENV_ACTIVA, "OT_ENCUESTA_ACTIVA")
        for v in (None, "", "0", "no", "false", "off", "  ", "talvez", "2"):
            self.assertFalse(E.encuesta_activa({E.ENV_ACTIVA: v} if v is not None else {}), repr(v))
        for v in ("1", "true", "TRUE", "yes", "on", "si", "sí", " SI "):
            self.assertTrue(E.encuesta_activa({E.ENV_ACTIVA: v}), repr(v))

    def test_el_enlace_publico(self):
        self.assertEqual(E.enlace_publico("https://x.cl/", "T"), "https://x.cl/ot-encuesta/T")
        self.assertEqual(E.enlace_publico("https://x.cl", ""), "")


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
#  2) EL BLOQUE DEL CORREO: A PRUEBA DE OUTLOOK DE ESCRITORIO
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
_PM = re.compile(r"(?<![\w-])(padding|margin)(?![\w-])", re.I)


class TestBloqueDelCorreo(unittest.TestCase):
    def setUp(self):
        self.html = _bloque()
        self.p = analizar(self.html)
        self.texto = re.sub(r"\s+", " ", "".join(self.p.texto)).strip()

    def test_dice_lo_justo(self):
        self.assertIn("¿Cómo fue tu servicio?", self.texto)
        self.assertIn("Toca un número", self.texto)
        self.assertLess(len(self.texto), 130)                                  # «no me gusta tanta letra»
        for palabra in ("responde", "Responde", "producto", "Trotadora"):
            self.assertNotIn(palabra, self.texto)

    def test_sin_url_no_se_dibuja(self):
        self.assertEqual(E.bloque_html(""), "")
        self.assertEqual(E.bloque_html(None), "")

    def test_cinco_botones_cada_uno_un_enlace_con_su_nota(self):
        enlaces = [t for t in self.p.tags if t["tag"] == "a"]
        self.assertEqual([a["a"]["href"] for a in enlaces], [f"{URL}?n={n}" for n in range(1, 6)])
        self.assertEqual([a["a"].get("title", "")[:12] for a in enlaces], ["Calificar co"] * 5)

    def test_cada_boton_es_un_td_con_bgcolor_width_y_height_y_el_a_va_en_bloque_sin_relleno(self):
        for a in [t for t in self.p.tags if t["tag"] == "a"]:
            td = next(x for x in reversed(a["ancestros"]) if x["tag"] == "td")
            self.assertTrue(td["a"].get("bgcolor", "").startswith("#"), td)
            self.assertTrue(td["a"].get("width"), td)
            self.assertTrue(td["a"].get("height"), td)
            self.assertIn("display:block", a["a"]["style"].replace(" ", ""))
            self.assertIsNone(_PM.search(a["a"]["style"]), "el <a> no lleva padding ni margin (Outlook los ignora)")

    def test_ninguna_tabla_lleva_padding_ni_margin_el_espacio_lo_da_un_td(self):
        tablas = [t for t in self.p.tags if t["tag"] == "table"]
        self.assertGreaterEqual(len(tablas), 5)
        for t in tablas:
            self.assertIsNone(_PM.search(t["a"].get("style", "")), t)
            self.assertEqual((t["a"].get("role"), t["a"].get("cellpadding"), t["a"].get("cellspacing"), t["a"].get("border")),
                             ("presentation", "0", "0", "0"), t)
        # el aire de arriba y abajo está en el padding de un <td> (eso sí lo respeta Outlook)
        self.assertTrue(any("padding:22px 0 8px" in t["a"].get("style", "") for t in self.p.tags if t["tag"] == "td"))

    def test_sin_imagenes_ni_cosas_que_outlook_no_entiende(self):
        self.assertNotIn("<img", self.html.lower())
        for prohibido in ("display:flex", "display:grid", "gap:", "<svg", "background-image", "url(", "@media", "<style", "<script"):
            self.assertNotIn(prohibido, self.html.lower(), prohibido)

    def test_el_ancho_total_no_pasa_de_520_y_las_cinco_celdas_son_del_20_por_ciento(self):
        self.assertIn("max-width:520px", self.html)
        for t in self.p.tags:
            ancho = t["a"].get("width", "")
            if ancho.endswith("%"):
                self.assertLessEqual(int(ancho[:-1]), 100)
            elif ancho:
                self.assertLessEqual(int(ancho), 520, t)
        celdas = [t for t in self.p.tags if t["tag"] == "td" and t["a"].get("width") == "20%"]
        self.assertEqual(len(celdas), 5)

    def test_la_letra_es_grande_los_numeros_mas_y_el_alto_del_boton_sirve_para_el_dedo(self):
        for px in re.findall(r"font-size:(\d+)px", self.html):
            self.assertGreaterEqual(int(px), 12, px)
        for a in [t for t in self.p.tags if t["tag"] == "a"]:
            self.assertGreaterEqual(int(re.search(r"font-size:(\d+)px", a["a"]["style"]).group(1)), 18)
            td = next(x for x in reversed(a["ancestros"]) if x["tag"] == "td")
            self.assertGreaterEqual(int(td["a"]["height"]), 44)

    def test_toda_celda_con_texto_declara_su_fuente_porque_outlook_no_la_hereda(self):
        for t in self.p.tags:
            if t["tag"] == "a":
                self.assertIn("font-family:", t["a"]["style"])
        # los <td> que llevan texto (título, ayuda, leyendas) también
        con_texto = re.findall(r'<td[^>]*style="([^"]*)"[^>]*>(?=[^<]*[A-Za-z¿0-9])', self.html)
        self.assertGreaterEqual(len(con_texto), 5)
        for estilo in con_texto:
            self.assertIn("font-family:", estilo, estilo)

    def test_la_escala_va_de_rojo_suave_a_verde_con_texto_oscuro(self):
        fondos = [ESC["bg"] for ESC in (E.ESCALA[n] for n in range(1, 6))]
        self.assertEqual(len(set(fondos)), 5)
        self.assertEqual(fondos[0], "#fecaca")                                  # rojo suave
        self.assertEqual(fondos[-1], "#86efac")                                 # verde

        def luz(h):
            r, g, b = (int(h[i:i + 2], 16) / 255 for i in (1, 3, 5))
            f = lambda c: c / 12.92 if c <= 0.03928 else ((c + 0.055) / 1.055) ** 2.4
            return 0.2126 * f(r) + 0.7152 * f(g) + 0.0722 * f(b)
        for n in range(1, 6):
            e = E.ESCALA[n]
            claro, oscuro = sorted((luz(e["bg"]), luz(e["fg"])), reverse=True)
            self.assertGreaterEqual((claro + 0.05) / (oscuro + 0.05), 7.0, f"contraste de la nota {n}")

    def test_todo_va_escapado(self):
        h = E.bloque_html('https://x.cl/ot-encuesta/T?a=1&b="2"<script>')
        self.assertNotIn("<script>", h)
        self.assertIn("&amp;b=&quot;2&quot;&lt;script&gt;", h)
        self.assertIn("&amp;n=3", h)                                            # ya tenía «?»: la nota se agrega con «&»

    def test_la_leyenda_nombra_los_extremos(self):
        self.assertIn("1 · Mala", self.texto)
        self.assertIn("5 · Excelente", self.texto)

    def test_funciona_con_un_enlace_real_del_correo(self):
        t = E.generar_token(SECRETO, 77, CLAVE)
        h = E.bloque_html(E.enlace_publico("https://ilus.test", t))
        self.assertEqual(len(re.findall(r'href="https://ilus\.test/ot-encuesta/' + re.escape(t) + r'\?n=[1-5]"', h)), 5)


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
#  3) LAS PLANTILLAS Y LA CADENA DE VERSIONES
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestPlantillas(unittest.TestCase):
    def test_la_variable_existe_en_el_codigo_el_editor_y_la_vista_previa(self):
        v = C.variables_ot({"id": 1, "numero_ot": "OT-1", "cliente": "X", "equipos": [], "tecnicos": []})
        self.assertEqual(v["encuesta_html"], "")                                # sin encuesta, el bloque no se dibuja
        v2 = C.variables_ot({"id": 1, "numero_ot": "OT-1", "cliente": "X", "equipos": [], "tecnicos": []}, encuesta=_bloque())
        self.assertEqual(v2["encuesta_html"], _bloque())
        self.assertIn("encuesta_html", dict(C.VARIABLES_DOC))
        self.assertIn("encuesta_html", R.DEMO_EXTRA)
        self.assertIn("¿Cómo fue tu servicio?", R.DEMO_EXTRA["encuesta_html"])
        js = FC.texto("static/comunicaciones_index.js")
        j = js.index("  ot_terminada: [")
        self.assertIn("{{encuesta_html}}", js[j:js.index("  ],", j)])

    def test_las_dos_plantillas_del_cliente_la_llevan_despues_del_boton_y_antes_de_la_firma(self):
        for slug in (C.PLANTILLA_TERMINADA, C.PLANTILLA_MANUAL):
            cuerpo = C.SEMILLAS[slug][1]
            self.assertEqual(cuerpo.count("{{encuesta_html}}"), 1, slug)
            self.assertGreater(cuerpo.index("{{encuesta_html}}"), cuerpo.index("{{link_ot_firmada}}"), slug)
        t = C.SEMILLAS[C.PLANTILLA_TERMINADA][1]
        self.assertLess(t.index("{{encuesta_html}}"), t.index("{{firma_sstt_html}}"))
        self.assertLess(t.index("{{recorrido_html}}"), t.index("{{encuesta_html}}"))

    def test_las_del_equipo_y_el_tecnico_no_la_llevan(self):
        for slug in (C.PLANTILLA_TERMINADA_INTERNO, C.PLANTILLA_TERMINADA_TECNICO, C.PLANTILLA_RECORDATORIO, C.PLANTILLA_RECORDATORIO_TECNICO):
            self.assertNotIn("encuesta_html", C.SEMILLAS[slug][1], slug)

    def test_la_version_congelada_es_exactamente_la_de_antes_sin_la_variable(self):
        self.assertEqual(C._TERMINADA_V5[1], C.SEMILLAS[C.PLANTILLA_TERMINADA][1].replace("{{encuesta_html}}", ""))
        self.assertEqual(C._MANUAL_V1[1], C.SEMILLAS[C.PLANTILLA_MANUAL][1].replace("{{encuesta_html}}", ""))
        self.assertEqual(C._TERMINADA_V5[0], C.SEMILLAS[C.PLANTILLA_TERMINADA][0])
        self.assertEqual(C.SEMILLAS_ANTERIORES[C.PLANTILLA_TERMINADA][-1], C._TERMINADA_V5)
        self.assertEqual(C.SEMILLAS_ANTERIORES[C.PLANTILLA_MANUAL], (C._MANUAL_V1,))
        # y las congeladas más viejas no se tocaron (siguen sin la variable)
        for v in C.SEMILLAS_ANTERIORES[C.PLANTILLA_TERMINADA]:
            self.assertNotIn("encuesta_html", v[1])

    def test_las_semillas_usan_solo_variables_que_el_codigo_arma(self):
        armadas = set(C.variables_ot({"id": 1, "numero_ot": "OT-1", "cliente": "X", "equipos": [], "tecnicos": []}))
        for slug, (a, c) in C.SEMILLAS.items():
            self.assertLessEqual(set(re.findall(r"\{\{(\w+)\}\}", a + c)), armadas, slug)

    def test_sin_encuesta_el_correo_queda_igual_que_antes(self):
        ot = {"id": 77, "numero_ot": "OT-2026-00077", "cliente": "Gimnasio Demo", "tipo_label": "Instalación", "equipos": ["Trotadora"], "tecnicos": ["Lenin"]}
        v = C.variables_ot(ot, nombre_destinatario="María", link_ot_firmada="https://x/ot-firmada/T", recorrido=C.recorrido_html("documento"))
        for slug, v5 in ((C.PLANTILLA_TERMINADA, C._TERMINADA_V5), (C.PLANTILLA_MANUAL, C._MANUAL_V1)):
            nuevo = C.armar_correo(slug, v)[1]
            viejo = C.render_simple(v5[1], v)
            self.assertEqual(nuevo, viejo, slug)

    def test_con_encuesta_el_correo_la_incluye_y_no_deja_variables_sueltas(self):
        ot = {"id": 77, "numero_ot": "OT-2026-00077", "cliente": "Gimnasio Demo", "tipo_label": "Instalación", "equipos": ["Trotadora"], "tecnicos": ["Lenin"]}
        v = C.variables_ot(ot, link_ot_firmada="https://x/ot-firmada/T", recorrido=C.recorrido_html("documento"), encuesta=_bloque())
        for slug in (C.PLANTILLA_TERMINADA, C.PLANTILLA_MANUAL):
            _a, cuerpo = C.armar_correo(slug, v)
            self.assertNotIn("{{", cuerpo)
            self.assertIn("¿Cómo fue tu servicio?", _txt(cuerpo))
            self.assertIsNone(re.search(r"respond[ae]\b[^.<]{0,40}correo", _txt(cuerpo), re.I), slug)


class TestCadenaDeVersiones(unittest.TestCase):
    """Igual que `test_ot_correos_siempre_ticket.TestLaActualizacionAlArrancar`: se ejecuta la función REAL del arranque sobre una base de mentira."""

    def _mundo(self):
        w = MundoOT()
        self.addCleanup(w.cerrar)
        orig = w.amb["mysql_fetchone"]

        def con_id(sql, params=()):
            r = orig(sql, params)
            if r is not None and "FROM comm_templates" in sql and "id" not in r:
                r = dict(r, id=abs(hash(tuple(params))) % 10_000)
            return r
        w.amb["mysql_fetchone"] = con_id
        FC.extraer(w.amb, "app.py", ("_mantenciones_tpl_seed",), ())
        return w

    def _ups(self, w):
        return [(s, p) for s, p in w.sql if s.startswith("UPDATE comm_templates SET asunto")]

    def _poner(self, w, slug, asunto, cuerpo):
        w.bd.plantillas[(C.MODULO_PLANTILLAS, slug)] = {"asunto": asunto, "cuerpo": cuerpo, "activo": 1}

    def test_la_terminada_sembrada_sin_editar_sube_sola_a_la_que_tiene_la_encuesta(self):
        w = self._mundo()
        self._poner(w, C.PLANTILLA_TERMINADA, *C._TERMINADA_V5)
        w.f("_ensure_comm_template_ot_comunicacion")()
        ups = self._ups(w)
        self.assertEqual(len(ups), 1, ups)
        self.assertEqual(ups[0][1][1], C.SEMILLAS[C.PLANTILLA_TERMINADA][1])
        self.assertIn("{{encuesta_html}}", ups[0][1][1])
        self.assertIn("AND cuerpo=%s", ups[0][0])
        self.assertEqual(ups[0][1][2], "sistema")

    def test_la_manual_sembrada_sin_editar_sube_sola(self):
        w = self._mundo()
        self._poner(w, C.PLANTILLA_MANUAL, *C._MANUAL_V1)
        w.f("_ensure_comm_template_ot_comunicacion")()
        ups = self._ups(w)
        self.assertEqual(len(ups), 1, ups)
        self.assertEqual(ups[0][1][1], C.SEMILLAS[C.PLANTILLA_MANUAL][1])

    def test_una_versiones_mas_vieja_de_la_terminada_va_directo_a_la_vigente(self):
        for v in (C._TERMINADA_V3, C._TERMINADA_V4):
            w = self._mundo()
            self._poner(w, C.PLANTILLA_TERMINADA, *v)
            w.f("_ensure_comm_template_ot_comunicacion")()
            ups = self._ups(w)
            self.assertEqual([p[1] for _s, p in ups], [C.SEMILLAS[C.PLANTILLA_TERMINADA][1]])

    def test_lo_que_una_persona_edito_no_se_toca(self):
        w = self._mundo()
        self._poner(w, C.PLANTILLA_TERMINADA, C._TERMINADA_V5[0], C._TERMINADA_V5[1] + "<p>Texto de Daniel</p>")
        self._poner(w, C.PLANTILLA_MANUAL, C._MANUAL_V1[0], C._MANUAL_V1[1] + "<p>Texto de Daniel</p>")
        w.f("_ensure_comm_template_ot_comunicacion")()
        self.assertEqual(self._ups(w), [])

    def test_ya_en_la_version_vigente_no_se_vuelve_a_escribir(self):
        w = self._mundo()
        self._poner(w, C.PLANTILLA_TERMINADA, *C.SEMILLAS[C.PLANTILLA_TERMINADA])
        self._poner(w, C.PLANTILLA_MANUAL, *C.SEMILLAS[C.PLANTILLA_MANUAL])
        w.f("_ensure_comm_template_ot_comunicacion")()
        self.assertEqual(self._ups(w), [])


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
#  4) EL INTERRUPTOR EN LOS CORREOS REALES Y A QUIÉN LE LLEGA
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
def _hrefs(html):
    return re.findall(r'href="(https://ilus\.test/ot-encuesta/[^"]+)"', html)


def _token_de(href):
    return re.search(r"/ot-encuesta/([^?\"]+)", href).group(1)


class TestInterruptorYDestinatarios(unittest.TestCase):
    def mundo(self, **kw):
        m = MundoOT(**kw)
        self.addCleanup(m.cerrar)
        return m

    def _activar(self, valor="1"):
        p = mock.patch.dict(os.environ, {E.ENV_ACTIVA: valor})
        p.start()
        self.addCleanup(p.stop)

    def _quitar(self):
        p = mock.patch.dict(os.environ)
        p.start()
        os.environ.pop(E.ENV_ACTIVA, None)
        self.addCleanup(p.stop)

    # ── OT terminada automática ──
    def test_apagada_el_correo_real_al_cliente_sale_sin_el_bloque(self):
        m = self.mundo(env_cliente="1")
        self._quitar()
        m.f("_ot_terminada_notificar")(77)
        for destino in ("contacto@gimnasio.cl", "admin@gimnasio.cl"):
            h = _html_para(m, destino)
            self.assertNotIn("ot-encuesta", h)
            self.assertNotIn("¿Cómo fue tu servicio?", h)
            self.assertNotIn("{{", h)

    def test_encendida_el_cliente_recibe_los_cinco_numeros_con_un_token_de_esa_ot_y_esa_aprobacion(self):
        m = self.mundo(env_cliente="1")
        self._activar()
        m.f("_ot_terminada_notificar")(77)
        for destino in ("contacto@gimnasio.cl", "admin@gimnasio.cl"):
            h = _html_para(m, destino)
            hrefs = _hrefs(h)
            self.assertEqual(len(hrefs), 5, destino)
            self.assertEqual([re.search(r"\?n=(\d)$", x).group(1) for x in hrefs], ["1", "2", "3", "4", "5"])
            v = E.validar_token(SECRETO, _token_de(hrefs[0]))
            self.assertEqual((v["ok"], v["vid"], v["cierre_clave"]), (True, 77, CLAVE))
            self.assertIn("¿Cómo fue tu servicio?", _txt(h))
            self.assertNotIn("{{", h)

    def test_el_bloque_va_despues_del_documento_firmado_y_antes_de_la_firma_del_equipo(self):
        m = self.mundo(env_cliente="1")
        self._activar()
        m.f("_ot_terminada_notificar")(77)
        h = _html_para(m, "contacto@gimnasio.cl")
        i_doc, i_enc, i_firma = h.index("Ver la orden de trabajo firmada"), h.index("¿Cómo fue tu servicio?"), h.index("Equipo de Servicio Técnico")
        self.assertLess(i_doc, i_enc)
        self.assertLess(i_enc, i_firma)

    def test_el_equipo_y_el_tecnico_nunca_la_reciben_porque_quien_toca_un_numero_responde(self):
        m = self.mundo(env_cliente="1")
        self._activar()
        m.f("_ot_terminada_notificar")(77)
        for destino in ("aaron@sphs.cl", "victor@sphs.cl", "lenin@sphs.cl"):
            self.assertNotIn("ot-encuesta", _html_para(m, destino), destino)
            self.assertNotIn("¿Cómo fue tu servicio?", _html_para(m, destino), destino)

    def test_con_el_cliente_apagado_en_control_de_correos_no_se_arma_ni_se_envia_nada(self):
        m = self.mundo(env_cliente="1")
        self._activar()
        m.apagar_correo(C.CORREO_TERMINADA_CLIENTE)
        m.f("_ot_terminada_notificar")(77)
        self.assertNotIn("contacto@gimnasio.cl", m.destinos())

    def test_una_falla_al_armar_el_bloque_no_impide_el_correo(self):
        m = self.mundo(env_cliente="1")
        self._activar()
        m.amb["_oenc"] = types.SimpleNamespace(encuesta_activa=lambda: (_ for _ in ()).throw(RuntimeError("boom")), TOKEN_VISTA_PREVIA="vista-previa")
        m.f("_ot_terminada_notificar")(77)
        h = _html_para(m, "contacto@gimnasio.cl")
        self.assertNotIn("ot-encuesta", h)
        self.assertIn("Ver la orden de trabajo firmada", h)

    # ── Enviar OT por correo ──
    def _enviar(self, m, destinatarios):
        return m.llamar("ot2_api_com_enviar", 77, cuerpo={"destinatarios": destinatarios})

    def test_el_envio_manual_lleva_la_encuesta_solo_al_cliente_y_a_una_persona_escrita_a_mano(self):
        m = self.mundo()
        self._activar()
        st, d = self._enviar(m, ["contacto@gimnasio.cl", "aaron@sphs.cl", "lenin@sphs.cl", "nuevo@otra.cl"])
        self.assertEqual(st, 200, d)
        self.assertIn("ot-encuesta", _html_para(m, "contacto@gimnasio.cl"))
        self.assertIn("ot-encuesta", _html_para(m, "nuevo@otra.cl"))               # no se sabe quién es: es lo más probable que sea del cliente
        self.assertNotIn("ot-encuesta", _html_para(m, "aaron@sphs.cl"))           # el ejecutivo de ILUS
        self.assertNotIn("ot-encuesta", _html_para(m, "lenin@sphs.cl"))           # el técnico
        v = E.validar_token(SECRETO, _token_de(_hrefs(_html_para(m, "contacto@gimnasio.cl"))[0]))
        self.assertEqual((v["vid"], v["cierre_clave"]), (77, CLAVE))

    def test_apagada_el_envio_manual_sale_sin_la_encuesta(self):
        m = self.mundo()
        self._quitar()
        st, _d = self._enviar(m, ["contacto@gimnasio.cl", "nuevo@otra.cl"])
        self.assertEqual(st, 200)
        for destino in ("contacto@gimnasio.cl", "nuevo@otra.cl"):
            self.assertNotIn("ot-encuesta", _html_para(m, destino))

    def test_la_vista_previa_del_modal_muestra_lo_que_de_verdad_va_a_salir(self):
        m = self.mundo()
        self._quitar()
        st, d = m.llamar("ot2_api_com_vista_previa", 77, cuerpo={})
        self.assertEqual(st, 200)
        self.assertNotIn("ot-encuesta", d["html"])                                  # apagada: no se promete lo que no sale
        self._activar()
        st, d = m.llamar("ot2_api_com_vista_previa", 77, cuerpo={})
        self.assertIn("¿Cómo fue tu servicio?", d["html"])
        self.assertEqual((m.m.enviados_reales, m.filas_log()), ([], []))            # y no envía nada

    # ── Vista previa de Control de correos y «Enviarme una prueba»: SIEMPRE se ve ──
    def _pantalla(self):
        m = MC.MundoPantalla()
        m.amb.update({"_oenc": E, "_public_base_url": lambda: "https://ilus.test"})
        for clave, slug in (("ot_terminada_cliente", C.PLANTILLA_TERMINADA), ("ot_envio_manual", C.PLANTILLA_MANUAL)):
            m.poner_plantilla("mantenciones", slug, *C.SEMILLAS[slug])
        return m

    def test_la_vista_previa_siempre_muestra_el_bloque_aunque_la_encuesta_este_apagada(self):
        self._quitar()
        m = self._pantalla()
        for clave in ("ot_terminada_cliente", "ot_envio_manual"):
            asunto, cuerpo, info = m.amb["_comm_correo_demo"](R.por_clave(clave))
            self.assertIsNotNone(asunto, clave)
            self.assertIn("¿Cómo fue tu servicio?", _txt(cuerpo), clave)
            self.assertEqual(_hrefs(cuerpo)[2], "https://ilus.test/ot-encuesta/vista-previa?n=3", clave)
            self.assertNotIn("[encuesta_html demo]", cuerpo)

    def test_enviarme_una_prueba_lleva_el_bloque_solo_al_correo_de_quien_prueba(self):
        self._quitar()
        m = self._pantalla()
        st, datos, _r = m.llamar("comm_correos_prueba", "ot_terminada_cliente")
        self.assertEqual(st, 200, datos)
        (envio,) = m.enviados_reales
        self.assertEqual(envio["to"], "daniel.aguilar@sphs.cl")
        self.assertTrue(envio["asunto"].startswith("[PRUEBA]"))
        self.assertIn("¿Cómo fue tu servicio?", _txt(envio["html"]))
        self.assertIn("/ot-encuesta/vista-previa?n=5", envio["html"])

    def test_el_helper_de_demo_no_exige_la_variable(self):
        m = self.mundo()
        self._quitar()
        self.assertIn("vista-previa", m.f("_ot_encuesta_html")(None, None, demo=True))
        self.assertEqual(m.f("_ot_encuesta_html")(77, CLAVE), "")
        self._activar()
        self.assertIn("ot-encuesta", m.f("_ot_encuesta_html")(77, CLAVE))
        self.assertEqual(m.f("_ot_encuesta_html")(77, ""), "")                    # sin la aprobación del cierre no hay token

    # ── La ficha de la OT cerrada ──
    def test_la_ficha_muestra_la_nota_y_el_comentario_de_esa_ot_solo_a_la_gestion(self):
        m = self.mundo()
        orig = m.amb["mysql_fetchone"]

        def fila(sql, params=()):
            if "FROM ot_encuesta_respuestas" in sql:
                return {"puntaje": 2, "comentario": "Llegaron tarde <b>mucho</b>", "creada_at": dt_utc(2026, 10, 22, 15, 0), "cierre_clave": CLAVE}
            return orig(sql, params)
        m.amb["mysql_fetchone"] = fila
        ctx = m.f("_ot_com_contexto_ficha")(77, m.fila)
        en = ctx["encuesta"]
        self.assertEqual((en["respondida"], en["puntaje"], en["estrellas"], en["etiqueta"]), (True, 2, "★★☆☆☆", "Regular"))
        self.assertEqual(en["comentario"], "Llegaron tarde <b>mucho</b>")           # tal cual; la plantilla lo escapa
        self.assertFalse(en["de_otro_cierre"])
        self.assertEqual(en["cuando"], "22/10/2026 12:00")
        # un técnico no recibe nada
        t = self.mundo(tecnico=True)
        self.assertNotIn("encuesta", t.f("_ot_com_contexto_ficha")(77, t.fila))

    def test_sin_respuesta_la_ficha_lo_dice(self):
        m = self.mundo()
        self._quitar()
        m.amb["mysql_fetchone"] = lambda sql, params=(): None
        en = m.f("_ot_encuesta_para_ficha")(77, CLAVE)
        self.assertEqual((en["respondida"], en["activa"]), (False, False))

    def test_la_tarjeta_de_la_ficha_se_dibuja_escapada(self):
        env = _entorno_jinja()
        base = {"v": types.SimpleNamespace(id=77), "es_tecnico": False}
        html = env.get_template("ot2/_enviar_ot.html").render(
            **base, com_envio={"puede_enviar": True, "listo": True, "razon": "", "franja": None, "creacion": None,
                               "encuesta": {"activa": True, "respondida": True, "puntaje": 2, "estrellas": "★★☆☆☆", "etiqueta": "Regular",
                                            "comentario": "<script>alert(1)</script>", "cuando": "22/10/2026 12:00", "de_otro_cierre": False}})
        self.assertIn("Satisfacción del cliente", html)
        self.assertIn("&lt;script&gt;alert(1)&lt;/script&gt;", html)
        self.assertNotIn("<script>alert(1)", html)
        self.assertIn("2 · Regular", html)
        vacio = env.get_template("ot2/_enviar_ot.html").render(
            **base, com_envio={"puede_enviar": True, "listo": True, "razon": "", "franja": None, "creacion": None,
                               "encuesta": {"activa": False, "respondida": False}})
        self.assertIn("todavía no se envía a los clientes", vacio)
        sin = env.get_template("ot2/_enviar_ot.html").render(**base, com_envio={"puede_enviar": False})
        self.assertNotIn("Satisfacción del cliente", sin)                          # un técnico / OT no cerrada: nada


def dt_utc(*a):
    return datetime(*a)


def _entorno_jinja():
    import jinja2
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(os.path.join(RAIZ, "templates")), autoescape=True)
    env.globals["url_for"] = lambda endpoint, **kw: f"/{endpoint}"
    return env


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
#  5) LAS RUTAS: Flask de mentira + base en memoria
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
class BDMemoria:
    """SQLite en memoria con las tres tablas que tocan las rutas. El `CREATE TABLE` del módulo se anota (no se ejecuta: la tabla ya está)."""

    def __init__(self):
        self.con = sqlite3.connect(":memory:", check_same_thread=False)
        self.con.row_factory = sqlite3.Row
        self.con.executescript("""
            CREATE TABLE mant_visitas (id INTEGER PRIMARY KEY, numero_ot TEXT, estado TEXT, tipo TEXT, fecha_realizada TEXT, cliente_id INTEGER);
            CREATE TABLE mant_clientes (id INTEGER PRIMARY KEY, razon_social TEXT);
            CREATE TABLE ot_encuesta_respuestas (id INTEGER PRIMARY KEY AUTOINCREMENT, visita_id INTEGER NOT NULL, cierre_clave TEXT NOT NULL,
                puntaje INTEGER NOT NULL, comentario TEXT, aviso_version TEXT, creada_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (visita_id, cierre_clave));
            INSERT INTO mant_clientes VALUES (5, 'Gimnasio Demo SpA'), (6, 'Fit Club Las Condes Ltda');
            INSERT INTO mant_visitas VALUES (77, 'OT-2026-00077', 'cerrada', 'instalacion', '2026-10-21', 5),
                                            (78, 'OT-2026-00078', 'cerrada', 'mantencion', '2026-10-22', 6);
        """)
        self.ddl, self.escrituras = [], []

    @staticmethod
    def _sql(sql):
        s = re.sub(r"\s+", " ", sql).strip().replace("INSERT IGNORE INTO", "INSERT OR IGNORE INTO")
        return s.replace("%s", "?")

    @staticmethod
    def _p(v):
        return v.isoformat(sep=" ") if isinstance(v, datetime) else v

    def fetchall(self, sql, params=()):
        return [dict(r) for r in self.con.execute(self._sql(sql), tuple(self._p(p) for p in params)).fetchall()]

    def fetchone(self, sql, params=()):
        f = self.fetchall(sql, params)
        return f[0] if f else None

    def execute(self, sql, params=()):
        if re.match(r"\s*CREATE TABLE", sql, re.I):
            self.ddl.append(re.sub(r"\s+", " ", sql).strip())
            return 0
        self.escrituras.append((re.sub(r"\s+", " ", sql).strip(), params))
        cur = self.con.execute(self._sql(sql), tuple(self._p(p) for p in params))
        self.con.commit()
        return cur.rowcount

    def responder(self, vid=77, clave=CLAVE, nota=5, comentario=None, hace_dias=0.0):
        creada = (datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=hace_dias)).isoformat(sep=" ", timespec="seconds")
        self.con.execute("INSERT INTO ot_encuesta_respuestas (visita_id, cierre_clave, puntaje, comentario, aviso_version, creada_at) VALUES (?,?,?,?,?,?)",
                         (vid, clave, nota, comentario, E.AVISO_VERSION, creada))
        self.con.commit()

    def filas(self):
        return [dict(r) for r in self.con.execute("SELECT * FROM ot_encuesta_respuestas ORDER BY id").fetchall()]


STUB_BASE = ("<!doctype html><html><head><title>{% block title %}{% endblock %}</title>{% block head_extra %}{% endblock %}</head>"
             "<body><div class='page'>{% block content %}{% endblock %}</div></body></html>")


def construir_app(rl_permite=True):
    from flask import Flask, flash, g, jsonify, redirect, request, url_for
    from jinja2 import ChoiceLoader, DictLoader, FileSystemLoader
    app = Flask(__name__, template_folder=os.path.join(RAIZ, "templates"), static_folder=os.path.join(RAIZ, "static"))
    app.config["TESTING"] = True
    app.secret_key = SECRETO
    app.jinja_loader = ChoiceLoader([DictLoader({"base.html": STUB_BASE}), FileSystemLoader(os.path.join(RAIZ, "templates"))])
    app.add_url_rule("/ot", "ot2_panel", lambda: "panel")
    app.add_url_rule("/ot/<int:vid>", "ot2_detalle", lambda vid: "detalle")
    app.add_url_rule("/mis-ot", "mant_ots_list", lambda: "mis ot")
    app.add_template_filter(lambda v, f="%d/%m/%Y %H:%M": (datetime.fromisoformat(v).strftime(f) if isinstance(v, str) else v.strftime(f)), "chile_fmt")
    sesion = {"user": None, "permissions": {}}

    @app.before_request
    def _usuario():
        g.user = dict(sesion["user"]) if sesion["user"] else None
        g.permissions = dict(sesion["permissions"])

    def _mant_required(vista):
        @wraps(vista)
        def envuelta(*a, **kw):
            if not g.user:
                return ("login", 302)
            if not (g.permissions.get("mantenciones") or g.permissions.get("superadmin")):
                return ("sin permiso", 403)
            return vista(*a, **kw)
        return envuelta

    # `_no_tecnico` y lo que usa son los REALES de app.py
    amb = {"wraps": wraps, "g": g, "request": request, "jsonify": jsonify, "flash": flash, "redirect": redirect, "url_for": url_for}
    FC.extraer(amb, "app.py", ("_rol_familia", "_es_rol_tecnico", "_no_tecnico"), ())
    db = BDMemoria()
    rl = {"permite": rl_permite, "llamadas": []}

    def _rl_check(scope, maximo, ventana):
        rl["llamadas"].append((scope, maximo, ventana))
        return rl["permite"]
    ctx = {"mysql_fetchone": db.fetchone, "mysql_fetchall": db.fetchall,
           "mysql_execute": (lambda sql, p=(): (db.execute(sql, p), None)[1]),            # como en producción: no devuelve rowcount
           "mysql_execute_returning_rowcount": lambda sql, p=(): int(db.execute(sql, p) or 0),
           "_mant_required": _mant_required, "_no_tecnico": amb["_no_tecnico"], "_rl_check": _rl_check,
           "ILUS_SOPORTE_URL": "https://soporte.test/ticket", "_TIPO_OT_LABEL": {"instalacion": "Instalación", "mantencion": "Mantención"}}
    E.register_ot_encuesta_routes(app, ctx)

    class W:
        pass
    w = W()
    w.app, w.db, w.sesion, w.rl, w.client = app, db, sesion, rl, app.test_client()
    w.token = lambda vid=77, clave=CLAVE, **kw: E.generar_token(SECRETO, vid, clave, **kw)
    w.como = lambda rol, perms=None: sesion.update(user={"id": 9, "nombre": "Alguien", "username": "alguien@sphs.cl", "role": rol},
                                                   permissions=perms if perms is not None else {"mantenciones": True})
    w.anonimo = lambda: sesion.update(user=None, permissions={})
    return w


class TestRutasPublicas(unittest.TestCase):
    def setUp(self):
        self.w = construir_app()
        self.c = self.w.client
        self.t = self.w.token()
        self.url = f"/ot-encuesta/{self.t}"

    def texto(self, r):
        return re.sub(r"\s+", " ", _txt(r.get_data(as_text=True)))

    def test_abrir_el_enlace_no_guarda_nada_ni_crea_tablas(self):
        """Los escáneres de correo abren los enlaces: el GET solo muestra."""
        for n in ("", "?n=1", "?n=5", "?n=abc"):
            r = self.c.get(self.url + n)
            self.assertEqual(r.status_code, 200)
        self.assertEqual((self.w.db.escrituras, self.w.db.ddl), ([], []))
        self.assertEqual(self.w.db.filas(), [])

    def test_la_nota_del_correo_llega_marcada(self):
        html = self.c.get(self.url + "?n=4").get_data(as_text=True)
        self.assertRegex(html, r'id="oe-n4"[^>]*checked')
        self.assertNotRegex(html, r'id="oe-n3"[^>]*checked')
        self.assertIn("Tu nota: <strong>4 · Muy buena</strong>", html)
        self.assertIn("Orden de trabajo <strong>OT-2026-00077</strong>", html)
        sin = self.c.get(self.url + "?n=9").get_data(as_text=True)
        self.assertNotRegex(sin, r'class="oe-radio"[^>]*checked')

    def test_la_pagina_es_corta_una_pregunta_y_un_comentario_opcional(self):
        html = self.c.get(self.url).get_data(as_text=True)
        self.assertEqual(len(re.findall(r'<input class="oe-radio"', html)), 5)
        self.assertEqual(html.count("<textarea"), 1)
        self.assertIn('maxlength="300"', html)
        self.assertIn("¿Algo que quieras contarnos?", html)
        self.assertIn("(opcional)", html)
        self.assertEqual(html.count('type="submit"'), 1)
        self.assertNotIn('name="csrf_token"', html)                                # sin sesión: el POST público va exento, no se crea cookie

    def test_el_aviso_de_privacidad_es_breve_y_dice_lo_que_hay_que_decir(self):
        t = self.texto(self.c.get(self.url))
        for frase in ("no guardamos tu dirección IP", "hasta 24 meses", "No te pedimos tu nombre ni tus datos", "soportetec@sphs.cl",
                      "Sport and Health Solutions SPA", "mejorar el servicio técnico de ILUS Fitness"):
            self.assertIn(frase, t)

    def test_enviar_guarda_una_sola_vez_y_agradece(self):
        r = self.c.post(self.url, data={"n": "5", "comentario": "Excelente atención"})
        self.assertEqual(r.status_code, 200)
        t = self.texto(r)
        self.assertIn("¡Gracias por tu opinión!", t)
        self.assertNotIn("Lamentamos", t)
        (fila,) = self.w.db.filas()
        self.assertEqual((fila["visita_id"], fila["cierre_clave"], fila["puntaje"], fila["comentario"], fila["aviso_version"]),
                         (77, CLAVE, 5, "Excelente atención", E.AVISO_VERSION))
        self.assertEqual(set(fila), {"id", "visita_id", "cierre_clave", "puntaje", "comentario", "aviso_version", "creada_at"})    # ni nombre, ni correo, ni IP

    def test_la_segunda_vez_dice_ya_recibimos_tu_respuesta_y_no_cambia_nada(self):
        self.c.post(self.url, data={"n": "4", "comentario": "Bien"})
        r = self.c.post(self.url, data={"n": "1", "comentario": "Ahora no"})
        self.assertIn("Ya recibimos tu respuesta, gracias", self.texto(r))
        r2 = self.c.get(self.url)
        self.assertIn("Ya recibimos tu respuesta, gracias", self.texto(r2))
        self.assertNotIn("<textarea", r2.get_data(as_text=True))
        (fila,) = self.w.db.filas()
        self.assertEqual((fila["puntaje"], fila["comentario"]), (4, "Bien"))

    def test_una_respuesta_por_ot_y_por_aprobacion_del_cierre(self):
        self.c.post(self.url, data={"n": "4"})
        otra = f"/ot-encuesta/{self.w.token(clave='2026-10-28 09:00:00')}"          # la OT se reabrió y se volvió a cerrar: es otra aprobación
        self.assertNotIn("Ya recibimos tu respuesta", self.texto(self.c.post(otra, data={"n": "2"})))
        otra_ot = f"/ot-encuesta/{self.w.token(vid=78)}"
        self.c.post(otra_ot, data={"n": "3"})
        self.assertEqual([(f["visita_id"], f["cierre_clave"], f["puntaje"]) for f in self.w.db.filas()],
                         [(77, CLAVE, 4), (77, "2026-10-28 09:00:00", 2), (78, CLAVE, 3)])

    def test_dos_toques_a_la_vez_guardan_una_sola_respuesta(self):
        """El INSERT IGNORE sobre la clave única decide: el segundo ve rowcount 0 y dice la verdad."""
        self.w.db.responder(77, CLAVE, 3, "Primera")
        orig = self.w.app.view_functions["ot_encuesta_publica"]
        # se simula que la lectura previa no vio la respuesta (carrera): el INSERT IGNORE no inserta
        db = self.w.db
        real = db.fetchone
        db.fetchone = lambda sql, p=(): (None if "FROM ot_encuesta_respuestas WHERE visita_id=%s AND cierre_clave=%s" in sql and not getattr(db, "_vio", False) and setattr(db, "_vio", True) is None else real(sql, p))
        r = self.c.post(self.url, data={"n": "1", "comentario": "Segunda"})
        self.assertIn("Ya recibimos tu respuesta, gracias", self.texto(r))
        self.assertEqual([(f["puntaje"], f["comentario"]) for f in db.filas()], [(3, "Primera")])
        self.assertIsNotNone(orig)

    def test_nota_baja_pide_disculpas_y_ofrece_el_ticket(self):
        for n in ("1", "2"):
            w = construir_app()
            r = w.client.post(f"/ot-encuesta/{w.token()}", data={"n": n, "comentario": "Tardaron"})
            html = r.get_data(as_text=True)
            t = re.sub(r"\s+", " ", _txt(html))
            self.assertIn("Lamentamos no haber estado a la altura", t)
            self.assertIn('href="https://soporte.test/ticket"', html)
            self.assertIn("Crear ticket de soporte", t)
        w = construir_app()
        r = w.client.post(f"/ot-encuesta/{w.token()}", data={"n": "3"})
        self.assertNotIn("Lamentamos", r.get_data(as_text=True))
        self.assertNotIn("Crear ticket de soporte", r.get_data(as_text=True))

    def test_sin_nota_valida_no_se_guarda_y_se_avisa(self):
        for n in ("", "0", "6", "3.5", "abc", None):
            datos = {"comentario": "Hola"} if n is None else {"n": n, "comentario": "Hola"}
            r = self.c.post(self.url, data=datos)
            self.assertEqual(r.status_code, 422, repr(n))
            self.assertIn("Elige una nota del 1 al 5.", r.get_data(as_text=True))
            self.assertIn("Hola", r.get_data(as_text=True))                      # lo escrito no se pierde
        self.assertEqual(self.w.db.filas(), [])

    def test_el_comentario_se_recorta_a_300_y_el_html_se_guarda_como_texto(self):
        self.c.post(self.url, data={"n": "5", "comentario": "<b>hola</b> " + "x" * 400 + "\x00‮"})
        (fila,) = self.w.db.filas()
        self.assertEqual(len(fila["comentario"]), 300)
        self.assertTrue(fila["comentario"].startswith("<b>hola</b> "))
        self.assertNotIn("\x00", fila["comentario"])
        self.assertNotIn("‮", fila["comentario"])

    def test_comentario_vacio_se_guarda_como_nulo(self):
        self.c.post(self.url, data={"n": "5", "comentario": "   "})
        self.assertIsNone(self.w.db.filas()[0]["comentario"])

    def test_enlace_invalido_adulterado_o_de_una_ot_que_no_existe_da_una_pagina_amable(self):
        casos = ["basura", "a.b", self.t[:-3] + "000", self.w.token(vid=999)]
        for tok in casos:
            for r in (self.c.get(f"/ot-encuesta/{tok}"), self.c.post(f"/ot-encuesta/{tok}", data={"n": "5"})):
                self.assertEqual(r.status_code, 410, tok)
                t = self.texto(r)
                self.assertIn("Este enlace no está disponible", t)
                self.assertNotIn("Traceback", t)
                self.assertNotIn("OT-2026", t)                                 # no confirma qué OT existe
        self.assertEqual(self.w.db.filas(), [])

    def test_enlace_vencido(self):
        viejo = self.w.token(ahora=time.time() - 60 * 86400)
        r = self.c.get(f"/ot-encuesta/{viejo}")
        self.assertEqual(r.status_code, 410)
        self.assertIn("Este enlace ya venció", self.texto(r))
        r = self.c.post(f"/ot-encuesta/{viejo}", data={"n": "5"})
        self.assertEqual(r.status_code, 410)
        self.assertEqual(self.w.db.filas(), [])

    def test_limite_de_intentos_por_conexion_solo_en_el_post(self):
        w = construir_app(rl_permite=False)
        t = w.token()
        self.assertEqual(w.client.get(f"/ot-encuesta/{t}").status_code, 200)       # abrir el enlace nunca se limita
        self.assertEqual(w.rl["llamadas"], [])
        r = w.client.post(f"/ot-encuesta/{t}", data={"n": "5"})
        self.assertEqual(r.status_code, 429)
        self.assertIn("Demasiados intentos", r.get_data(as_text=True))
        self.assertEqual(w.db.filas(), [])
        self.assertEqual(w.rl["llamadas"], [("ot_encuesta", 20, 3600)])

    def test_la_vista_previa_no_guarda_nada_y_lo_dice(self):
        r = self.c.get("/ot-encuesta/vista-previa?n=3")
        t = self.texto(r)
        self.assertEqual(r.status_code, 200)
        self.assertIn("VISTA PREVIA", t)
        self.assertIn("OT-2026-00123", t)
        for n in ("5", "1"):
            r2 = self.c.post("/ot-encuesta/vista-previa", data={"n": n, "comentario": "Prueba"})
            self.assertEqual(r2.status_code, 200)
        self.assertIn("Lamentamos no haber estado a la altura", self.texto(r2))
        self.assertEqual(self.c.post("/ot-encuesta/vista-previa", data={"comentario": "x"}).status_code, 422)
        self.assertEqual((self.w.db.filas(), self.w.db.escrituras, self.w.db.ddl), ([], [], []))

    def test_cabeceras_sin_cache_sin_indexar_sin_referer_tambien_en_los_errores(self):
        respuestas = [self.c.get(self.url), self.c.post(self.url, data={}), self.c.get("/ot-encuesta/basura"), self.c.get("/ot-encuesta/vista-previa")]
        respuestas.append(self.c.post(self.url, data={"n": "4"}))
        respuestas.append(self.c.get(self.url))
        for r in respuestas:
            self.assertEqual(r.headers["Cache-Control"], "no-store")
            self.assertIn("noindex", r.headers["X-Robots-Tag"])
            self.assertEqual(r.headers["Referrer-Policy"], "no-referrer")

    def test_la_pagina_no_carga_nada_de_terceros(self):
        for r in (self.c.get(self.url), self.c.post(self.url, data={"n": "5"}), self.c.get("/ot-encuesta/basura")):
            html = r.get_data(as_text=True)
            externos = [u for u in re.findall(r'(?:src|href)="(https?://[^"]+)"', html) if "soporte.test" not in u and not u.startswith("mailto:")]
            self.assertEqual(externos, [], externos)
            self.assertNotIn("googleapis", html)
            self.assertNotIn("cdn", html.lower())

    def test_una_falla_de_la_base_no_filtra_detalles_al_cliente(self):
        db = self.w.db
        orig = db.execute

        def rota(sql, params=()):
            if sql.lstrip().upper().startswith("INSERT"):
                raise RuntimeError("Table 'ot_encuesta_respuestas' doesn't exist; secreto=hunter2")
            return orig(sql, params)
        db.execute = rota
        r = self.c.post(self.url, data={"n": "5", "comentario": "RUT 12.345.678-9"})
        self.assertEqual(r.status_code, 500)
        t = r.get_data(as_text=True)
        self.assertIn("No pudimos guardar tu respuesta", t)
        self.assertNotIn("hunter2", t)
        self.assertNotIn("doesn't exist", t)
        self.assertIn("RUT 12.345.678-9", t)                                      # lo escrito se conserva para reintentar


class TestResultadosInternos(unittest.TestCase):
    def setUp(self):
        self.w = construir_app()
        self.c = self.w.client
        db = self.w.db
        db.responder(77, "c1", 5, "Todo perfecto", hace_dias=1)
        db.responder(77, "c2", 4, None, hace_dias=2)
        db.responder(78, "c1", 2, "Llegaron tarde <script>alert(1)</script>", hace_dias=3)
        db.responder(78, "c2", 1, "Mal", hace_dias=4)
        db.responder(77, "c3", 5, "Viejo", hace_dias=200)

    def get(self, qs=""):
        return self.c.get("/ot/encuestas" + qs)

    def test_la_gestion_ve_promedio_cuantas_respuestas_barras_y_comentarios_con_ot_y_cliente(self):
        self.w.como("ejecutivo")
        r = self.get()
        self.assertEqual(r.status_code, 200)
        t = re.sub(r"\s+", " ", _txt(r.get_data(as_text=True)))
        self.assertIn("3,0 / 5", t)                                                # (5+4+2+1)/4 en los últimos 90 días: el de hace 200 días no cuenta
        self.assertRegex(t, r"Respuestas 4\b|\b4 Respuestas")
        self.assertIn("Todo perfecto", t)
        self.assertIn("OT-2026-00078", t)
        self.assertIn("Fit Club Las Condes Ltda", t)                               # el nombre completo del cliente
        self.assertIn("Instalación", t)
        self.assertNotIn("Viejo", t)
        self.assertIn("5 · Excelente", t)
        self.assertIn("2 · Regular", t)
        self.assertEqual(r.headers["Cache-Control"], "no-store")

    def test_los_comentarios_siempre_se_muestran_escapados(self):
        self.w.como("admin")
        html = self.get().get_data(as_text=True)
        self.assertNotIn("<script>alert(1)</script>", html)
        self.assertIn("&lt;script&gt;alert(1)&lt;/script&gt;", html)

    def test_periodos_y_filtros(self):
        self.w.como("supervisor")
        self.assertIn("Viejo", self.get("?periodo=todo").get_data(as_text=True))
        t = self.get("?nota=bajas").get_data(as_text=True)
        self.assertIn("Llegaron tarde", t)
        self.assertNotIn("Todo perfecto", t)
        t = self.get("?nota=altas").get_data(as_text=True)
        self.assertIn("Todo perfecto", t)
        self.assertNotIn("Llegaron tarde", t)
        t = self.get("?nota=5").get_data(as_text=True)
        self.assertIn("Todo perfecto", t)
        self.assertNotIn(">Mal<", t)
        t = self.get("?comentarios=1").get_data(as_text=True)
        self.assertNotIn("Sin comentario.", t)
        self.assertEqual(self.get("?periodo=30").status_code, 200)
        self.assertEqual(self.get("?periodo=raro&nota=zzz&page=-4&per_page=7").status_code, 200)       # nada rompe con parámetros basura

    def test_paginada_con_el_pie_de_siempre(self):
        for i in range(30):
            self.w.db.responder(77, f"p{i}", 4, f"Comentario {i}", hace_dias=0.01 * i)
        self.w.como("ejecutivo")
        t = re.sub(r"\s+", " ", _txt(self.get("?per_page=10").get_data(as_text=True)))
        self.assertIn("Mostrando 1–10 de 34", t)
        self.assertIn("Página 1 de 4", t)
        self.assertIn("Página 2 de 4", re.sub(r"\s+", " ", _txt(self.get("?per_page=10&page=2").get_data(as_text=True))))
        self.assertIn("Página 1 de 1", re.sub(r"\s+", " ", _txt(self.get("?per_page=100").get_data(as_text=True))))

    def test_sin_respuestas_lo_dice(self):
        w = construir_app()
        w.como("ejecutivo")
        t = re.sub(r"\s+", " ", _txt(w.client.get("/ot/encuestas").get_data(as_text=True)))
        self.assertIn("Todavía no hay respuestas", t)
        self.assertIn("Sin respuestas en este período", t)

    def test_un_tecnico_no_ve_resultados_ni_comentarios(self):
        """REGLAS #19 y #26: interno, elevado y externo. Ni la página ni un solo comentario."""
        for rol in ("tecnico", "tecnico_ejecutivo", "tecnico_externo", "tecnico_externo_jr", "tecnico_jr"):
            self.w.como(rol)
            r = self.get()
            self.assertEqual(r.status_code, 302, rol)                              # redirigido a «Mis OT» con un aviso
            self.assertIn("/mis-ot", r.headers["Location"])
            self.assertNotIn("Todo perfecto", r.get_data(as_text=True))
            r2 = self.c.get("/ot/encuestas", headers={"Accept": "application/json"})
            self.assertEqual(r2.status_code, 403, rol)
            self.assertEqual(r2.get_json()["error_codigo"], "TECNICO_SIN_ACCESO")
            self.assertNotIn("Todo perfecto", r2.get_data(as_text=True))

    def test_sin_sesion_o_sin_permiso_no_se_ve_nada(self):
        self.w.anonimo()
        self.assertEqual(self.get().status_code, 302)
        self.w.como("ejecutivo", perms={})
        self.assertEqual(self.get().status_code, 403)

    def test_la_gestion_completa_entra(self):
        for rol, perms in (("superadmin", {"superadmin": True}), ("admin", {"mantenciones": True}), ("supervisor", {"mantenciones": True}),
                           ("ejecutivo", {"mantenciones": True}), ("ejecutivo_sstt", {"mantenciones": True})):
            self.w.como(rol, perms)
            self.assertEqual(self.get().status_code, 200, rol)


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
#  6) CABLEADO, ARRANQUE BARATO Y CONFIDENCIALIDAD ESTÁTICA
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestCableado(unittest.TestCase):
    def test_el_arranque_es_un_solo_create_table_if_not_exists_y_ningun_alter(self):
        self.assertEqual(len(re.findall(r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS", E.SQL_TABLA, re.I)), 1)
        self.assertNotRegex(E.SQL_TABLA, r"(?i)\bALTER\b")
        self.assertIn("UNIQUE KEY uq_ot_encuesta (visita_id, cierre_clave)", E.SQL_TABLA)
        import ast
        fn = ast.parse(FC.funcion_top("app.py", "_ensure_ot_encuesta_tabla")).body[0]
        codigo = "\n".join(ast.unparse(n) for n in fn.body[1:])                      # sin la docstring, que explica «ningún ALTER»
        self.assertEqual(codigo.count("mysql_execute("), 1)
        self.assertNotRegex(codigo, r"(?i)\bALTER\b")
        self.assertNotRegex(FC.texto("ot_encuesta.py").replace("Ninguna", ""), r"(?i)\bALTER\s+TABLE\b")

    def test_la_tabla_no_guarda_datos_personales(self):
        columnas = re.findall(r"^\s+(\w+)\s+(?:BIGINT|INT|VARCHAR|TINYINT|DATETIME)", E.SQL_TABLA, re.M)
        self.assertEqual(columnas, ["id", "visita_id", "cierre_clave", "puntaje", "comentario", "aviso_version", "creada_at"])
        for prohibida in ("nombre", "correo", "email", "ip", "rut", "telefono", "cliente"):
            self.assertNotIn(prohibida, columnas)

    def test_se_crea_junto_a_la_tabla_de_comunicacion_en_el_arranque(self):
        app = FC.texto("app.py")
        i = app.index("        _ensure_ot_comunicacion_tablas()\n        _ensure_ot_encuesta_tabla()")
        self.assertGreater(i, 0)
        self.assertIn("with app.app_context():", app[i - 80:i])

    def test_las_rutas_se_registran_y_el_resultado_es_solo_para_la_gestion(self):
        app = FC.texto("app.py")
        self.assertIn("_oenc.register_ot_encuesta_routes(app, globals())", app)
        src = FC.texto("ot_encuesta.py")
        self.assertIn('@app.route(RUTA_RESULTADOS, methods=["GET"])\n    @_mant_required\n    @_no_tecnico\n    def ot_encuestas_pagina():', src)
        self.assertIn('@app.route(f"{RUTA_BASE}/<token>", methods=["GET", "POST"])\n    def ot_encuesta_publica(token):', src)

    def test_el_post_publico_esta_exento_de_csrf_y_solo_el(self):
        amb = {"re": re, "_CSRF_EXEMPT_PATHS": set(), "_CSRF_EXEMPT_PREFIXES": ()}
        FC.extraer(amb, "app.py", ("_csrf_is_exempt",), ())
        exento = amb["_csrf_is_exempt"]
        t = E.generar_token(SECRETO, 77, CLAVE)
        self.assertTrue(exento(f"/ot-encuesta/{t}"))
        self.assertTrue(exento("/ot-encuesta/vista-previa"))
        for no in ("/ot/encuestas", "/ot-encuesta/", "/ot-encuesta/basura", f"/ot-encuesta/{t}/x", f"/ot-encuesta/../ot/{t}", "/ot-encuestas",
                   "/ot/api/77/encuesta", f"/otx-encuesta/{t}"):
            self.assertFalse(exento(no), no)

    def test_la_variable_no_se_enciende_en_ningun_archivo_del_repositorio(self):
        """REGLA #23: el bloque nace APAGADO y solo Daniel autoriza encenderlo (como `RETIROS_ENCUESTA_ACTIVA`: `append_env` en deploy.yml)."""
        permitidos = {".py", ".md", ".js", ".html", ".txt"}
        sospechosos = []
        for carpeta, subs, archivos in os.walk(RAIZ):
            subs[:] = [s for s in subs if s not in (".git", ".claude", "node_modules", "__pycache__", ".pytest_cache", "venv", ".venv")]
            for nombre in archivos:
                ruta = os.path.join(carpeta, nombre)
                ext = os.path.splitext(nombre)[1].lower()
                if ext in permitidos or ext in (".png", ".jpg", ".jpeg", ".gif", ".ico", ".woff", ".woff2", ".pdf", ".zip", ".docx", ".xlsx", ".svg"):
                    continue
                try:
                    with open(ruta, encoding="utf-8", errors="ignore") as fh:
                        if E.ENV_ACTIVA in fh.read():
                            sospechosos.append(os.path.relpath(ruta, RAIZ))
                except OSError:
                    pass
        self.assertEqual(sospechosos, [], "la variable aparece en: %s" % sospechosos)
        deploy = os.path.join(RAIZ, ".github", "workflows", "deploy.yml")
        if os.path.exists(deploy):
            self.assertNotIn(E.ENV_ACTIVA, FC.texto(".github/workflows/deploy.yml"))
        for fuente in ("app.py", "ot_comunicacion.py", "comm_correos_registro.py", "ot_encuesta.py"):
            self.assertNotRegex(FC.texto(fuente), r"os\.environ\[\s*['\"]" + E.ENV_ACTIVA, fuente)
            self.assertNotRegex(FC.texto(fuente), r"environ\.setdefault\(\s*['\"]" + E.ENV_ACTIVA, fuente)

    def test_el_codigo_lee_el_interruptor_solo_por_la_funcion_unica(self):
        self.assertEqual(len(re.findall(r"os\.environ", FC.texto("ot_encuesta.py"))), 2)               # encuesta_activa() y el secreto propio
        self.assertNotIn("OT_ENCUESTA_ACTIVA", FC.funcion_top("app.py", "_ot_terminada_notificar"))
        self.assertIn("_oenc.encuesta_activa()", FC.funcion_top("app.py", "_ot_encuesta_html"))

    def test_el_correo_al_cliente_y_el_manual_arman_el_bloque_y_los_demas_no(self):
        n = FC.funcion_top("app.py", "_ot_terminada_notificar")
        self.assertEqual(n.count("encuesta=encuesta_cli"), 1)                                           # solo el correo al cliente
        self.assertIn('_ot_encuesta_html(v_id, datos["cierre_clave"])', n)
        man = FC.funcion_top("app.py", "ot2_api_com_enviar")
        self.assertIn('p["rol"] in ("cliente", "otro")', man)
        self.assertIn("(not p[\"tecnico\"])", man)

    def test_el_registro_de_correos_avisa_de_la_encuesta(self):
        for k in (C.CORREO_TERMINADA_CLIENTE, C.CORREO_MANUAL):
            self.assertIn("OT_ENCUESTA_ACTIVA", R.por_clave(k)["nota"])
            self.assertEqual(R.por_clave(k)["condiciones"], ())             # el interruptor del correo sigue siendo solo del frente
        self.assertEqual(R.validar(MC.llaves_validas()), [])

    def test_el_modulo_es_puro_hasta_registrar_las_rutas(self):
        src = FC.texto("ot_encuesta.py")
        cabecera = src.split("def register_ot_encuesta_routes")[0]
        self.assertNotRegex(cabecera, r"(?m)^(from|import) flask")
        for prohibido in ("_send_ilus_email", "send_whatsapp", "smtplib", "requests.", "urllib.request", "_mant_notificar"):
            self.assertNotIn(prohibido, src, "esta encuesta no envía nada: " + prohibido)

    def test_el_boton_del_panel_y_el_enlace_de_la_ficha_apuntan_a_la_pantalla(self):
        self.assertIn('href="/ot/encuestas"', FC.texto("templates/ot2/panel.html"))
        self.assertIn('href="/ot/encuestas"', FC.texto("templates/ot2/_enviar_ot.html"))
        panel = FC.texto("templates/ot2/panel.html")
        i = panel.index('href="/ot/encuestas"')
        self.assertIn("{% if not es_tecnico and (permissions.superadmin or permissions.mant_ot_encuesta) %}", panel[i - 500:i])   # técnico: ni ve el botón

    def test_sin_popups_nativos_ni_nombre_viejo_en_lo_nuevo(self):
        archivos = ["static/ot_encuesta.js", "static/ot_encuesta.css", "ot_encuesta.py", "templates/ot2/encuestas.html"] + [
            f"templates/ot_encuesta/{n}" for n in ("_base.html", "publica.html", "gracias.html", "no_disponible.html")]
        for rel in archivos:
            txt = FC.texto(rel)
            self.assertNotRegex(txt, r"(?<![\w.])(alert|confirm|prompt)\s*\(", rel)
            self.assertNotIn("Sport & Health", txt, rel)
            self.assertNotIn("Sport &amp; Health", txt, rel)

    def test_el_js_no_tiene_errores_de_sintaxis(self):
        import shutil
        import subprocess
        node = shutil.which("node")
        if not node:
            self.skipTest("Node no está instalado")
        r = subprocess.run([node, "--check", os.path.join(RAIZ, "static", "ot_encuesta.js")], capture_output=True, text=True, timeout=60)
        self.assertEqual(r.returncode, 0, r.stderr)

    def test_la_plantilla_de_resultados_es_jinja_valido_y_usa_las_clases_sin_truncar(self):
        import jinja2
        env = jinja2.Environment(loader=jinja2.FileSystemLoader(os.path.join(RAIZ, "templates")))
        env.parse(FC.texto("templates/ot2/encuestas.html"))
        css = FC.texto("templates/ot2/encuestas.html")
        self.assertNotIn("text-overflow:ellipsis", css)                         # REGLA #15: nombres completos
        self.assertNotIn("line-clamp", css)
        for px in re.findall(r"font-size:\s*([\d.]+)(px|rem)", css):
            v = float(px[0]) * (16 if px[1] == "rem" else 1)
            self.assertGreaterEqual(v, 12 - 0.01, px)                          # REGLA #61: letra >= 12 px

    def test_la_hoja_publica_cumple_la_regla_61(self):
        css = FC.texto("static/ot_encuesta.css")
        for valor, unidad in re.findall(r"font-size:\s*([\d.]+)(px|rem|em)\b", css):
            if unidad == "em":
                continue                                                       # relativa al texto vecino
            px = float(valor) * (16 if unidad == "rem" else 1)
            self.assertGreaterEqual(px, 12 - 0.01, (valor, unidad))
        self.assertIn("prefers-reduced-motion", css)
        self.assertNotIn("@import", css)
        self.assertNotIn("googleapis", css)
        for gris in re.findall(r"(?<![-\w])color:\s*#9ca3af", css, re.I):
            self.fail("texto #9ca3af sobre blanco no cumple el contraste (REGLA #61)")


class TestPermisoSatisfaccion(unittest.TestCase):
    """Daniel (6-nov-2026): «eso debería estar solo para el superadmin en órdenes de trabajo y controlable por los usuarios y roles»."""

    @staticmethod
    def _puede(permisos, tecnico=False):
        class _G(dict):
            pass
        amb = {"g": _G(permissions=permisos), "_es_rol_tecnico": lambda: tecnico}
        FC.extraer(amb, "app.py", ("_ot_encuesta_puede_ver",), ())
        return amb["_ot_encuesta_puede_ver"]()

    def test_superadmin_siempre_los_demas_solo_con_el_permiso(self):
        self.assertTrue(self._puede({"superadmin": True}))
        self.assertTrue(self._puede({"mant_ot_encuesta": True}), "una persona o un rol con el permiso encendido")
        self.assertFalse(self._puede({}))
        self.assertFalse(self._puede({"admin": True, "mantenciones": True}), "ni el admin ni ver OT bastan: el permiso nace apagado")
        self.assertFalse(self._puede({"mant_ot_encuesta": False}))

    def test_un_tecnico_nunca_aunque_se_encienda(self):
        self.assertFalse(self._puede({"mant_ot_encuesta": True}, tecnico=True))
        self.assertFalse(self._puede({"superadmin": True}, tecnico=True))

    def test_esta_cableado_en_los_permisos_y_nace_apagado(self):
        app = FC.texto("app.py")
        self.assertIn('    "mant_ot_encuesta",\n', app)                                                  # PERMS_KEYS (permisos individuales)
        self.assertIn('base["mant_ot_encuesta"] = bool(man.get("ot_encuesta"))', app)                   # nace apagado (no es de tipo bloqueo)
        self.assertIn('"ot_encuesta",\n', app)                                                          # matriz de roles (Servicio Técnico)
        self.assertIn('"ot_encuesta": {"label": "Satisfacción de las OT', app)                          # etiqueta
        self.assertNotIn('"ot_encuesta": {"label": "Satisfacción de las OT (resultados de la encuesta a los clientes)",\n'
                         '                         "tipo": "bloqueo"', app)

    def test_la_pantalla_el_boton_y_la_ficha_usan_la_misma_regla(self):
        src = FC.texto("ot_encuesta.py")
        self.assertIn('puede_ver = ctx.get("_ot_encuesta_puede_ver")', src)
        self.assertIn("if puede_ver and not puede_ver():\n            abort(403)", src)
        self.assertIn("if _ot_encuesta_puede_ver():\n                ctx[\"encuesta\"] = _ot_encuesta_para_ficha(", FC.texto("app.py"))
        self.assertIn("permissions.superadmin or permissions.mant_ot_encuesta", FC.texto("templates/ot2/panel.html"))


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
#  7) EL NAVEGADOR REAL (Chromium): la página a 390/820/1280 y el bloque del correo en una fila
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
try:
    from playwright.sync_api import sync_playwright
except Exception:                      # pragma: no cover
    sync_playwright = None

ORIGEN = "https://enc.test"


@unittest.skipIf(sync_playwright is None, "Playwright no está instalado")
class TestNavegadorReal(unittest.TestCase):
    """UN solo navegador para todas las pruebas (con poca memoria abrir y cerrar navegadores tarda minutos). La página la sirve la app de mentira."""

    @classmethod
    def setUpClass(cls):
        cls.w = construir_app()
        cls.token = cls.w.token()
        cls._pw = sync_playwright().start()
        try:
            cls.nav = cls._pw.chromium.launch()
        except Exception as e:         # sin Chromium instalado
            cls._pw.stop()
            raise unittest.SkipTest("Chromium no disponible: %s" % e)
        cls.ctx = cls.nav.new_context(viewport={"width": 1280, "height": 900})
        cls.ctx.set_default_timeout(8000)
        cls.html_extra = {"/correo": ""}
        cls.ctx.route(ORIGEN + "/**", cls._servir)
        cls.pg = cls.ctx.new_page()
        cls.errores = []
        cls.pg.on("pageerror", lambda e: cls.errores.append(str(e)))

    @classmethod
    def tearDownClass(cls):
        try:
            cls.ctx.close()
            cls.nav.close()
        finally:
            cls._pw.stop()

    @classmethod
    def _servir(cls, ruta):
        url = ruta.request.url[len(ORIGEN):]
        ruta_sola = url.split("?")[0]
        if ruta_sola == "/correo":
            return ruta.fulfill(status=200, content_type="text/html; charset=utf-8", body=cls.html_extra["/correo"])
        r = cls.w.client.open(url, method=ruta.request.method, data=ruta.request.post_data or None,
                              content_type=ruta.request.headers.get("content-type"))
        ruta.fulfill(status=r.status_code, content_type=r.headers.get("Content-Type", "text/html"), body=r.get_data())

    _n = [0]

    def nueva(self, nota="4"):
        """Un enlace NUEVO (otra aprobación del cierre) para que cada prueba encuentre el formulario sin responder."""
        type(self)._n[0] += 1
        return f"/ot-encuesta/{self.w.token(clave='nav-%d' % self._n[0])}" + (f"?n={nota}" if nota else "")

    def abrir(self, ancho, camino=None):
        self.pg.set_viewport_size({"width": ancho, "height": 900})
        self.pg.goto(ORIGEN + (camino or self.nueva()))
        return self.pg

    MEDIR = """() => {
      const ancho = document.documentElement.clientWidth;
      const menor = Math.min(...[...document.querySelectorAll('body *')].filter(e => [...e.childNodes].some(n => n.nodeType === 3 && n.textContent.trim())
          && getComputedStyle(e).display !== 'none' && e.getClientRects().length && !e.classList.contains('oe-sr') && !e.closest('.oe-skip'))
          .map(e => parseFloat(getComputedStyle(e).fontSize)));
      return {scroll: document.documentElement.scrollWidth - ancho, menorLetra: menor};
    }"""

    def test_sin_scroll_de_lado_y_letra_minima_de_12px_a_390_820_y_1280(self):
        for ancho in (390, 820, 1280):
            for camino in (None, "/ot-encuesta/vista-previa?n=2", "/ot-encuesta/basura"):
                pg = self.abrir(ancho, camino)
                m = pg.evaluate(self.MEDIR)
                self.assertLessEqual(m["scroll"], 0, (ancho, camino, m))
                self.assertGreaterEqual(m["menorLetra"], 12, (ancho, camino, m))
        self.assertEqual(self.errores, [])

    def test_los_toques_miden_al_menos_44px_y_los_cinco_numeros_van_en_una_fila(self):
        for ancho in (390, 820, 1280):
            pg = self.abrir(ancho)
            cajas = pg.evaluate("""() => [...document.querySelectorAll('.oe-pill')].map(e => {const r = e.getBoundingClientRect(); return [r.top, r.width, r.height];})""")
            self.assertEqual(len(cajas), 5)
            self.assertEqual(len({round(c[0]) for c in cajas}), 1, ancho)
            for _t, w, h in cajas:
                self.assertGreaterEqual(w, 44, ancho)
                self.assertGreaterEqual(h, 44, ancho)
            boton = pg.evaluate("(() => {const r = document.getElementById('oeEnviar').getBoundingClientRect(); return [r.width, r.height];})()")
            self.assertGreaterEqual(boton[1], 44)

    def test_elegir_una_nota_la_pinta_y_la_nombra_y_el_enviar_funciona(self):
        pg = self.abrir(390, self.nueva(None))
        self.assertEqual(pg.inner_text("#oeElegida").strip(), "")
        pg.click("label[for=oe-n2]")
        self.assertIn("Tu nota: 2 · Regular", pg.inner_text("#oeElegida"))
        self.assertIn("is-done", pg.evaluate("document.getElementById('oeForm').className"))
        pg.fill("#oeComentario", "Llegaron tarde")
        self.assertEqual(pg.inner_text("#oeCuenta"), str(len("Llegaron tarde")))
        pg.click("#oeEnviar")
        pg.wait_for_selector(".oe-gracias")
        self.assertIn("Lamentamos no haber estado a la altura", pg.inner_text(".oe-gracias"))
        self.assertEqual([(f["puntaje"], f["comentario"]) for f in self.w.db.filas()][-1], (2, "Llegaron tarde"))

    def test_sin_nota_el_formulario_guia_en_vez_de_enviar(self):
        antes = len(self.w.db.filas())
        pg = self.abrir(390, self.nueva(None))
        pg.click("#oeEnviar")
        self.assertIn("Elige una nota del 1 al 5.", pg.inner_text("#oeFaltan"))
        self.assertEqual(len(self.w.db.filas()), antes)
        self.assertEqual(self.errores, [])

    # ── el bloque del correo ──
    def _correo(self, ancho_contenedor):
        bloque = E.bloque_html("https://enc.test/ot-encuesta/X")
        type(self).html_extra["/correo"] = (
            "<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'></head>"
            "<body style='margin:0;font-family:Arial,sans-serif'><div id='c' style='width:%dpx;background:#fff'>%s</div></body></html>" % (ancho_contenedor, bloque))
        self.pg.set_viewport_size({"width": max(ancho_contenedor, 320), "height": 800})
        self.pg.goto(ORIGEN + "/correo")
        return self.pg

    def test_en_el_celular_el_bloque_cabe_en_una_fila_con_botones_tocables(self):
        for contenedor in (280, 316, 360, 532):                                   # el contenido de un correo de 320 a 390 px de pantalla y el del escritorio (620 - 88)
            pg = self._correo(contenedor)
            medidas = pg.evaluate("""() => {
              const c = document.getElementById('c');
              const a = [...c.querySelectorAll('a')].map(e => {const r = e.getBoundingClientRect(); return [r.top, r.width, r.height, r.left, r.right];});
              return {desborde: c.scrollWidth - c.clientWidth, anchos: document.documentElement.scrollWidth - document.documentElement.clientWidth, a,
                      bloque: c.firstElementChild.getBoundingClientRect().width};
            }""")
            self.assertLessEqual(medidas["desborde"], 0, (contenedor, medidas))
            self.assertLessEqual(medidas["anchos"], 0, (contenedor, medidas))
            self.assertEqual(len(medidas["a"]), 5)
            self.assertEqual(len({round(x[0]) for x in medidas["a"]}), 1, ("los cinco números en UNA fila", contenedor))
            for _top, w, h, _l, _r in medidas["a"]:
                self.assertGreaterEqual(w, 40, (contenedor, w))
                self.assertGreaterEqual(h, 44, (contenedor, h))
            self.assertLessEqual(medidas["bloque"], 532 + 1)
            izq = [x[3] for x in medidas["a"]]
            self.assertEqual(izq, sorted(izq))                                     # de 1 a 5, de izquierda a derecha

    def test_el_bloque_en_pantalla_ancha_no_pasa_de_520_px_de_tarjeta(self):
        pg = self._correo(700)
        ancho = pg.evaluate("document.querySelector('#c table table').getBoundingClientRect().width")
        self.assertLessEqual(ancho, 520.5)

    def test_la_letra_del_bloque_es_de_12_px_o_mas(self):
        pg = self._correo(316)
        menor = pg.evaluate("""() => Math.min(...[...document.querySelectorAll('#c td, #c a')].filter(e => [...e.childNodes].some(n => n.nodeType === 3 && n.textContent.trim()))
                              .map(e => parseFloat(getComputedStyle(e).fontSize)))""")
        self.assertGreaterEqual(menor, 12)


if __name__ == "__main__":
    unittest.main()
