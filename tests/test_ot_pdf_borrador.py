"""PDF BORRADOR de la OT (sin firmas) — 2026-10-29.

Daniel: «vayamos con la gestión de impresión de la OT sin necesidad de las firmas» (hay veces que hay que bajar el PDF ANTES
de la tarea). Se prueba, con datos de mentira:

  · la transformación pura que vuelve el contexto del PDF una hoja de trabajo (`_ot_pdf_borrador_ajustar`);
  · que el candado del documento FINAL sigue intacto (cerrada + 3 firmas) y que el borrador no lo abre para nadie más:
    ni el enlace público, ni el WhatsApp, ni el correo «OT terminada», ni el anexo fotográfico lo pueden pedir;
  · que el borrador no lleva plata (REGLAS #19/#26) y deja constancia en la bitácora;
  · que las plantillas, renderizadas DE VERDAD, dibujan la marca BORRADOR, las casillas vacías y los recuadros de firma en
    blanco, sin alarmas rojas, y que el documento firmado sale igual que siempre.
"""
import os
import re
import unittest

import jinja2

from tests import _fuente_app as FA

RAIZ = FA.RAIZ
TEMPLATES = os.path.join(RAIZ, "templates")


def _ajustar():
    amb = FA.cargar(funciones=["_ot_pdf_borrador_ajustar"], constantes=["_OT_PDF_BORRADOR_ESTADOS_NO"])
    return amb["_ot_pdf_borrador_ajustar"], amb["_OT_PDF_BORRADOR_ESTADOS_NO"]


def _ctx_demo():
    """Contexto de una OT PROGRAMADA a medio hacer, con la forma que deja `_ot_pdf_context` + `_ot_pdf_probatorio`."""
    eq = {"id": 7, "nombre": "Trotadora Pro 500", "sku": "TR500", "serie": "SN-001", "idx": 1, "revisado": False,
          "saltado": False, "revision_label": "", "revision_sev": "", "estado_esta_ot_label": "Sin revisión registrada",
          "estado_esta_ot_clase": "mute", "rep_solicitudes": [], "n_fotos": 0}
    tareas = [
        {"id": 1, "titulo": "Revisar correa", "tipo_respuesta": "verificacion", "resultado_str": "—", "resultado_clase": "ok",
         "tiene_foto": False, "completada_at_str": "", "ubicacion_str": "Las Condes", "maquina_nombre": "Trotadora Pro 500",
         "maquina_idx": 1, "obligatoria": True, "hecha": False, "sin_foto_exigida": True, "no_se_exige": False,
         "rev_pendiente": "Foto del hallazgo sin registrar", "equipo_saltado": False, "exc": True, "num": 1},
        {"id": 2, "titulo": "Limpiar consola", "tipo_respuesta": "check", "resultado_str": "OK", "resultado_clase": "ok",
         "tiene_foto": False, "completada_at_str": "29/10/2026 10:00", "ubicacion_str": "Las Condes",
         "maquina_nombre": "Trotadora Pro 500", "maquina_idx": 1, "obligatoria": False, "hecha": True,
         "sin_foto_exigida": False, "no_se_exige": False, "rev_pendiente": "", "equipo_saltado": False, "exc": False, "num": 2},
        {"id": 3, "titulo": "Medir voltaje", "tipo_respuesta": "numero", "resultado_str": "—", "resultado_clase": "txt",
         "tiene_foto": False, "completada_at_str": "", "ubicacion_str": "Las Condes", "maquina_nombre": "Trotadora Pro 500",
         "maquina_idx": 1, "obligatoria": False, "hecha": False, "sin_foto_exigida": False, "no_se_exige": False,
         "rev_pendiente": "", "equipo_saltado": False, "exc": False, "num": 3},
    ]
    return {
        "visita": {"id": 99, "numero_ot": "OT-2026-00999", "razon_social": "Cliente Prueba SpA", "estado": "programada",
                   "tecnico_principal": "Técnico Demo", "tipo": "instalacion", "cliente_id": 5, "diagnostico": ""},
        "firmante_cliente": None, "equipos": [eq], "tecnicos_extra": [], "tareas": [], "tareas_chk": tareas,
        "fotos": [], "fotos_generales": [], "eq_fotos_idx": {}, "eq_check_resumen": {7: [{"titulo": "Revisar correa", "ok": False}]},
        "paginas_equipos": [[eq]], "logo_b64": "", "logo_shs_url": "", "lev_items": [], "lev_fotos_idx": {},
        "lev_fotos_huerfanas": [], "lev_stats": {}, "base_url": "", "generated_at": "29/10/2026 11:00",
        "pdf_titulo_doc": "ORDEN DE TRABAJO", "pdf_subtitulo_doc": "", "chk_total": 3, "chk_completadas": 1, "chk_pct": 33,
        "ubic_validada": False, "native_header_footer": False, "anexo_total": 0, "anexo_omitidas": 0, "anexo_completo": False,
        "eq_tareas_rango": {7: (1, 3)},
        "hallazgos": [{"sev": "err", "cat": "Tarea obligatoria sin completar", "ref": "Tarea 1", "detalle": ""}],
        "hallazgos_err": 1, "hallazgos_warn": 0, "equipos_revisados_n": 0, "equipos_sin_revisar": [eq],
        "tareas_saltadas_n": 0, "tareas_no_hechas_n": 2, "tareas_oblig_pend_n": 1, "tareas_sin_foto_n": 1,
        "fotos_anexo": [], "ot_datos": {"tipo_label": "Instalación", "titulo": "Instalar trotadora",
                                       "fecha_prog_str": "30/10/2026"},
        "firmante_cliente_rut_fmt": "", "ilus_brand": "ILUS Fitness", "ilus_legal": "Sport and Health Solutions SPA",
        "ilus_rut": "76.996.964-0", "rep_resumen": [], "documentos_ot": [], "borrador": False,
    }


def _entorno():
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(TEMPLATES), autoescape=True, undefined=jinja2.Undefined)
    env.filters["chile_fmt"] = lambda v, f="": str(v)
    env.filters["rut_fmt"] = lambda v: str(v)
    env.globals["url_for"] = lambda *a, **k: "/x"
    return env


class TestAjusteDelContexto(unittest.TestCase):
    def test_quita_la_alarma_y_deja_casilla_vacia_en_lo_pendiente(self):
        ajustar, _ = _ajustar()
        ctx = ajustar(_ctx_demo())
        self.assertTrue(ctx["borrador"])
        self.assertNotIn("hallazgos", ctx)                       # el bloque rojo solo se dibuja si existe la clave
        for k in ("hallazgos_err", "hallazgos_warn", "tareas_oblig_pend_n", "tareas_sin_foto_n", "tareas_no_hechas_n"):
            self.assertEqual(ctx[k], 0, k)
        self.assertEqual(ctx["equipos_sin_revisar"], [])
        filas = {f["id"]: f for f in ctx["tareas_chk"]}
        self.assertEqual(filas[1]["resultado_str"], "")          # pendiente → casilla vacía
        self.assertEqual(filas[3]["resultado_str"], "")
        self.assertEqual(filas[2]["resultado_str"], "OK")        # lo que el técnico YA hizo no se toca
        for f in ctx["tareas_chk"]:
            self.assertFalse(f["exc"])
            self.assertEqual(f["rev_pendiente"], "")
            self.assertFalse(f["sin_foto_exigida"])
        self.assertEqual(ctx["equipos"][0]["estado_esta_ot_label"], "")

    def test_no_borra_los_datos_reales_del_tecnico(self):
        ajustar, _ = _ajustar()
        base = _ctx_demo()
        base["equipos"][0]["estado_esta_ot_label"] = "Con falla"
        base["tareas_chk"][0]["equipo_saltado"] = True
        base["tareas_chk"][0]["resultado_str"] = "FALLA"
        ctx = ajustar(base)
        self.assertEqual(ctx["equipos"][0]["estado_esta_ot_label"], "Con falla")   # un dato registrado no es una alarma del sistema
        self.assertEqual(ctx["tareas_chk"][0]["resultado_str"], "FALLA")
        self.assertTrue(ctx["tareas_chk"][0]["equipo_saltado"])

    def test_solo_cancelada_y_anulada_no_se_imprimen(self):
        _, no = _ajustar()
        self.assertEqual(set(no), {"cancelada", "anulada"})


class TestCandadoDelDocumentoFinal(unittest.TestCase):
    def setUp(self):
        self.src = FA.fuente_funcion("_ot_pdf_context")

    def test_el_documento_final_sigue_exigiendo_cerrada_y_las_tres_firmas(self):
        for frag in ('estado != "cerrada"', "firma_cliente_url", "_ot_es_interna(visita)", "firma_tecnico_url",
                     "firma_supervisor_url"):
            self.assertIn(frag, self.src, frag)
        # el borrador es un parámetro con default apagado: nadie lo recibe sin pedirlo
        self.assertIn("borrador=False):", self.src[:200])

    def test_el_borrador_se_salta_solo_las_firmas_y_siempre_quita_la_plata(self):
        i = self.src.index("if borrador:")
        trozo = self.src[i:i + 900]
        self.assertIn("publico = True", trozo)                   # REGLAS #19/#26: ni costos ni márgenes
        self.assertIn("_OT_PDF_BORRADOR_ESTADOS_NO", trozo)      # cancelada/anulada no se imprimen
        self.assertIn("else:", trozo)                            # las validaciones de siempre viven en el else
        self.assertIn("_ot_pdf_borrador_ajustar(ctx)", self.src)

    def test_solo_las_dos_rutas_internas_del_documento_piden_el_borrador(self):
        t = FA.texto()
        llamadas = [m.group(1) for m in re.finditer(r"(?<!def )_ot_pdf_context\(([^)]*)\)", t)]
        con_borrador = [c for c in llamadas if "borrador" in c]
        self.assertEqual(len(con_borrador), 2, con_borrador)
        for ruta in ("mant_visita_pdf", "mant_ot_pdf_render"):
            src = FA.fuente_funcion(ruta)
            self.assertIn("_ot_pdf_pide_borrador()", src, ruta)
            self.assertIn("borrador=_es_borrador", src, ruta)
            self.assertIn("_ot_pdf_borrador_registrar(", src, ruta)          # queda en la bitácora
            self.assertIn("ofrecer_borrador=True", src, ruta)
        # lo que sale hacia el cliente o hacia afuera NO lo pide
        for nombre in ("ot_publica_firmada", "_ot_terminada_notificar", "mant_visita_anexo_fotografico"):
            self.assertNotIn("borrador", FA.fuente_funcion(nombre), nombre)

    def test_el_nombre_del_archivo_dice_borrador(self):
        self.assertIn('fname = "BORRADOR-" + fname', FA.fuente_funcion("mant_visita_pdf"))

    def test_la_bitacora_lo_registra_con_etiqueta_y_sin_plata(self):
        src = FA.fuente_funcion("_ot_pdf_borrador_registrar")
        self.assertIn('"pdf_borrador"', src)
        self.assertNotIn("costo", src)
        self.assertIn('"pdf_borrador":', FA.texto())                           # etiqueta en la bitácora de la OT
        # y un técnico NO lo ve en su bitácora (lista blanca de REGLA #26)
        lista = FA.fuente_constante("_ACT_TEC_PERMITIDAS")
        self.assertNotIn("pdf_borrador", lista)

    def test_el_encabezado_y_el_pie_nativos_dicen_borrador(self):
        src = FA.fuente_funcion("_ot_pdf_header_footer_native")
        self.assertIn("BORRADOR · SIN CERRAR NI FIRMAR", src)
        self.assertEqual(src.count("{_pie_borrador}"), 2)                      # pie grande y pie compacto


class TestPlantillasRenderizadas(unittest.TestCase):
    def render(self, nombre, ctx):
        return _entorno().get_template("mantenciones/" + nombre).render(**ctx)

    def test_borrador_marca_casillas_y_sin_alarmas(self):
        ajustar, _ = _ajustar()
        ctx = _ctx_demo()
        ctx["native_header_footer"] = True
        html = self.render("ot_pdf.html", ajustar(ctx))
        self.assertIn("borrador-mark", html)                                    # marca de agua en cada página
        self.assertIn("BORRADOR · OT SIN CERRAR NI FIRMAR", html)               # franja de la primera página
        self.assertIn("<title>BORRADOR · OT-2026-00999", html)                  # el archivo que guarda el navegador también lo dice
        self.assertEqual(html.count('class="bx"'), 2)                           # las dos tareas pendientes (verificación y número)
        self.assertIn('<span class="chk-flag mute">Obligatoria</span>', html)   # la obligatoria queda neutra
        # (se busca el elemento, no la frase: el CSS del documento comenta algunas de estas palabras)
        for malo in ('chk-flag err">Obligatoria · sin completar', 'chk-flag warn">Foto exigida',
                     'class="sec-title">Hallazgos y excepciones', 'sin revisar</span>',
                     'rm-lbl">Equipos revisados', "<b>Constancia.</b>", 'rm-val">Sin GPS'):
            self.assertFalse(malo in html, malo)
        self.assertIn("Borrador sin valor de cierre", html)
        self.assertIn("Equipo por revisar", html)
        self.assertIn("Tareas del checklist", html)

    def test_borrador_sin_native_header_dice_borrador_en_cabecera_y_pie_de_pagina(self):
        ajustar, _ = _ajustar()
        html = self.render("ot_pdf.html", ajustar(_ctx_demo()))
        self.assertIn("BORRADOR · SIN CERRAR NI FIRMAR", html)
        self.assertIn('color:#b45309">BORRADOR</b>', html)

    def test_los_recuadros_de_firma_quedan_en_blanco_para_firmar_a_mano(self):
        ajustar, _ = _ajustar()
        html = self.render("ot_pdf.html", ajustar(_ctx_demo()))
        self.assertIn(".fir .fir-img.empty { color: transparent; }", html)
        self.assertIn("fir-img empty", html)

    def test_el_documento_firmado_sale_igual_que_siempre(self):
        ctx = _ctx_demo()
        ctx["visita"].update({"estado": "cerrada", "firma_cliente_url": "/f/c", "firma_tecnico_url": "/f/t",
                              "firma_supervisor_url": "/f/s"})
        html = self.render("ot_pdf.html", ctx)                                 # borrador=False
        self.assertFalse('class="borrador-mark"' in html)
        self.assertFalse("BORRADOR" in html)
        self.assertFalse('class="bx"' in html)
        self.assertIn("<b>Constancia.</b>", html)
        self.assertIn('class="sec-title">Hallazgos y excepciones', html)
        self.assertIn('chk-flag err">Obligatoria · sin completar', html)
        self.assertIn('rm-lbl">Equipos revisados', html)
        self.assertIn("<title>OT-2026-00999", html)

    def test_el_informe_de_levantamiento_tambien_lleva_la_marca(self):
        ctx = _ctx_demo()
        ctx.update({"borrador": True})
        ctx["visita"].update({"levantamiento_id": 3, "tipo": "levantamiento"})
        html = self.render("ot_pdf_levantamiento.html", ctx)
        self.assertIn('class="borrador-mark"', html)
        self.assertIn("BORRADOR · OT SIN CERRAR NI FIRMAR", html)
        self.assertIn("Firma pendiente", html)
        self.assertFalse("Sin firma registrada" in html)
        # una OT sin cerrar no puede decir «CERRADA — SERVICIO REALIZADO» ni llevar datos de cobro
        self.assertIn("BORRADOR — SIN CERRAR NI FIRMAR", html)
        self.assertFalse("CERRADA — SERVICIO REALIZADO" in html)
        self.assertFalse('<div class="pago-title">Datos para transferencia' in html)
        ctx["borrador"] = False
        final = self.render("ot_pdf_levantamiento.html", ctx)
        self.assertFalse('class="borrador-mark"' in final)
        self.assertIn("Sin firma registrada", final)
        self.assertIn("CERRADA — SERVICIO REALIZADO", final)                                 # el documento final no cambia
        self.assertIn('<div class="pago-title">Datos para transferencia', final)

    def test_la_pagina_de_pdf_no_disponible_ofrece_el_borrador_solo_cuando_corresponde(self):
        base = dict(vid=99, numero_ot="OT-2026-00999", cliente_nombre="Cliente Prueba SpA", estado_label="Programada",
                    razones=["La OT debe estar en estado 'cerrada' (actual: programada)."])
        env = _entorno()
        env.globals["url_for"] = lambda endpoint, **k: "/mantenciones/api/visitas/%s/pdf?borrador=1" % k.get("vid")
        t = env.get_template("mantenciones/ot_pdf_incompleto.html")
        con = t.render(puede_borrador=True, **base)
        self.assertIn("Imprimir borrador (sin firmas)", con)
        self.assertIn("pdf?borrador=1", con)
        sin = t.render(puede_borrador=False, **base)
        self.assertNotIn("Imprimir borrador", sin)


class TestBotonesDeLaFicha(unittest.TestCase):
    def setUp(self):
        with open(os.path.join(TEMPLATES, "ot2", "detalle.html"), encoding="utf-8") as fh:
            self.t = fh.read()

    def test_el_mismo_boton_imprime_el_borrador_si_la_ot_no_esta_cerrada(self):
        self.assertIn("pdf?borrador=1", self.t)
        self.assertIn("'Imprimir borrador' if _pdf_borr else 'Ver PDF'", self.t)
        self.assertIn("v.estado != 'cerrada' and v.estado not in ('cancelada','anulada')", self.t)

    def test_el_tecnico_tambien_puede_antes_de_la_tarea_pero_no_en_canceladas(self):
        self.assertIn("{% if es_tecnico and v.estado not in ('cancelada','anulada') %}", self.t)
        self.assertNotIn("{% if es_tecnico and v.estado not in ('programada','en_curso','en_ejecucion') %}", self.t)


if __name__ == "__main__":
    unittest.main()
