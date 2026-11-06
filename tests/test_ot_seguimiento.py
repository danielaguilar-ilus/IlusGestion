# -*- coding: utf-8 -*-
"""SEGUIMIENTO EN VIVO DE LA OT PARA EL CLIENTE (Daniel, 6-nov-2026) — REGLA #67.

Daniel, viendo en Outlook «Tu visita técnica está agendada»: «es necesario mejorar el tracking y colocar algo dinámico, bien bonito… no me
gusta tanta letra: puede ser muy sencilla la información».

Se prueba:
  1. El módulo puro ot_seguimiento.py: el paso del servicio según el estado REAL de la OT, textos cortos y coherentes (nunca se promete lo que
     no va a pasar), las firmas, el token con propósito propio (un token de firma remota NO abre la página) y la ventana tras el cierre.
  2. El bloque «Tu servicio, paso a paso» de los correos: a prueba de Outlook (color y relleno en las celdas, nunca `padding`/`margin` en una
     `<table>` ni en un `<a>`), con el botón «Ver seguimiento en vivo» solo cuando hay URL, escapado y con el paso correcto por correo.
  3. Las rutas públicas REALES de app.py en un mundo de mentira: token válido / inválido / vencido / de otro propósito, cabeceras, límite por
     conexión, solo lectura, y que ni el HTML ni el JSON lleven plata, proveedores, OC ni correos o teléfonos de técnicos (REGLAS #19 y #26).
  4. La página en Chromium real (Playwright) a 390 / 820 / 1280: sin scroll de lado, letra ≥ 12 px, botones ≥ 44 px, sin errores de JS, y la
     actualización en vivo (el JS repinta con el JSON de /estado).

Todo con datos de MENTIRA (REGLA #22). Correr:  python -m pytest tests/test_ot_seguimiento.py -q -p no:warnings
"""
import datetime as dt
import json
import os
import re
import types
import unittest

import jinja2

import ot_comunicacion as C
import ot_recordatorio_24h as O
import ot_seguimiento as S
from tests import _fuente_codigo as FC
from tests.test_ot_comunicacion_envio import MundoOT, PLATA, _txt
from tests.test_ot_recordatorio_24h import MundoRec, LUN

RAIZ = FC.RAIZ
TPL = "mantenciones/ot_seguimiento_publico.html"
AHORA = dt.datetime(2026, 11, 9, 10, 30)                     # lunes 9 de noviembre de 2026, 10:30 (hora Chile)
CONTACTO = re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+|\+?56\s?9\s?\d{4}\s?\d{4}|\b9\d{8}\b")


def _base(**kw):
    d = {"numero_ot": "OT-2026-00077", "tipo_label": "Instalación", "cliente": "Gimnasio Demo SpA", "fecha": dt.date(2026, 11, 12),
         "hora_inicio": "10:00", "hora_fin": "12:00", "tecnicos": ["Lenin Urbina"], "equipos": ["Trotadora ILUS Pro 9000", "Bicicleta ILUS"],
         "estado": "programada"}
    d.update(kw)
    return d


def _ev(**kw):
    promesas = kw.pop("promesas", {"recordatorio": True, "documento": True})
    ahora = kw.pop("ahora", AHORA)
    return S.evaluar(_base(**kw), ahora, promesas)


def _estados(ev):
    return [p["estado"] for p in ev["pasos"]]


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
#  1) EL MÓDULO PURO
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestPasos(unittest.TestCase):
    def test_los_pasos_son_los_mismos_del_correo(self):
        self.assertEqual(S.PASOS, C.PASOS_SERVICIO)
        self.assertEqual(S.CLAVES_PASOS, ("agendada", "recordatorio", "visita", "documento"))

    def test_agendada_con_fecha_futura(self):
        ev = _ev()
        self.assertEqual((ev["situacion"], ev["paso"], ev["tono"], ev["icono"]), ("curso", "agendada", "rojo", "calendario"))
        self.assertEqual(_estados(ev), ["actual", "pendiente", "pendiente", "pendiente"])
        self.assertEqual(ev["titulo"], "Tu visita está agendada")
        self.assertEqual(ev["mensaje"], "Te esperamos jueves 12/11/2026, 10:00 – 12:00.")
        self.assertEqual(ev["que_sigue"], "Te enviaremos un recordatorio 24 horas antes de la visita.")
        self.assertEqual((ev["linea"], ev["progreso"]), (0, 12))
        self.assertEqual(ev["pasos"][1]["detalle"], "24 h antes")
        self.assertEqual((ev["fecha_txt"], ev["dia_txt"], ev["horario_txt"]), ("12/11/2026", "jueves 12/11/2026", "10:00 – 12:00"))

    def test_lo_que_sigue_solo_se_promete_si_de_verdad_va_a_salir(self):
        ev = _ev(promesas={"recordatorio": False, "documento": False})
        self.assertEqual(ev["que_sigue"], "Nuestro técnico llegará en el horario indicado.")
        self.assertNotIn("recordatorio", ev["que_sigue"].lower())
        self.assertEqual(ev["pasos"][1]["detalle"], "")                      # tampoco anuncia «24 h antes»
        ev2 = _ev(estado="pendiente_aprobacion", firma_tecnico_at=AHORA, firma_cliente_at=AHORA, promesas={"recordatorio": True, "documento": False})
        self.assertEqual(ev2["que_sigue"], "El documento firmado quedará disponible en esta misma página.")
        ev3 = _ev(estado="pendiente_aprobacion", firma_tecnico_at=AHORA, firma_cliente_at=AHORA)
        self.assertEqual(ev3["que_sigue"], "Te enviaremos el documento firmado por correo.")

    def test_reagendada_dice_nueva_fecha(self):
        self.assertEqual(_ev(estado="reagendada")["titulo"], "Tu visita tiene nueva fecha")

    def test_sin_fecha_no_inventa_una(self):
        ev = _ev(fecha=None, hora_inicio=None, hora_fin=None)
        self.assertEqual((ev["paso"], ev["titulo"], ev["cuando"]), ("agendada", "Visita por agendar", "sin_fecha"))
        self.assertEqual((ev["fecha_txt"], ev["dia_txt"], ev["horario_txt"]), ("", "", "por confirmar"))

    def test_la_fecha_centinela_sin_fecha_cuenta_como_sin_fecha(self):
        ev = _ev(fecha=dt.date(2099, 12, 31))
        self.assertEqual(ev["cuando"], "sin_fecha")

    def test_manana_y_hoy_son_el_paso_del_recordatorio(self):
        ev = _ev(fecha=dt.date(2026, 11, 10))
        self.assertEqual((ev["paso"], ev["titulo"], ev["icono"], ev["linea"]), ("recordatorio", "Tu visita es mañana", "campana", 33))
        self.assertEqual(_estados(ev), ["hecho", "actual", "pendiente", "pendiente"])
        ev2 = _ev(fecha=dt.date(2026, 11, 9), ahora=dt.datetime(2026, 11, 9, 8, 0))
        self.assertEqual((ev2["paso"], ev2["titulo"]), ("recordatorio", "Tu visita es hoy"))
        self.assertEqual(_ev(fecha=dt.date(2026, 11, 10), recordatorio_enviado=True)["pasos"][1]["detalle"], "Enviado")

    def test_hoy_pasada_la_hora_de_inicio_es_la_visita(self):
        ev = _ev(fecha=dt.date(2026, 11, 9), ahora=dt.datetime(2026, 11, 9, 10, 30))
        self.assertEqual((ev["paso"], ev["titulo"], ev["icono"]), ("visita", "Hoy es tu visita", "tecnico"))
        self.assertIn("10:00 – 12:00", ev["mensaje"])
        self.assertEqual(ev["que_sigue"], "Al terminar, te pedirá confirmar el trabajo con tu firma.")

    def test_en_curso(self):
        for est in ("en_curso", "en_ejecucion"):
            ev = _ev(estado=est)
            self.assertEqual((ev["paso"], ev["titulo"], ev["linea"]), ("visita", "Visita en curso", 67), est)
        # una OT «programada» con hora real de inicio también está en curso
        self.assertEqual(_ev(estado="programada", hora_real_inicio=AHORA)["titulo"], "Visita en curso")

    def test_el_tecnico_termino_y_falta_la_firma_del_cliente(self):
        ev = _ev(estado="firmada_tecnico", firma_tecnico_at=AHORA)
        self.assertEqual((ev["paso"], ev["tono"], ev["icono"], ev["progreso"]), ("visita", "ambar", "firma", 80))
        self.assertEqual(ev["titulo"], "El técnico terminó: falta tu firma")
        self.assertEqual(ev["pasos"][2]["detalle"], "Falta tu firma")
        self.assertEqual([(f["nombre"], f["ok"]) for f in ev["firmas"]], [("Técnico", True), ("Tú", False), ("Servicio Técnico", False)])

    def test_con_las_dos_firmas_el_equipo_cierra(self):
        ev = _ev(estado="pendiente_aprobacion", firma_tecnico_at=AHORA, firma_cliente_at=AHORA)
        self.assertEqual((ev["titulo"], ev["tono"], ev["pasos"][2]["detalle"]), ("Trabajo realizado", "verde", "Firmas listas"))
        self.assertEqual([f["ok"] for f in ev["firmas"]], [True, True, False])

    def test_una_ot_interna_no_pide_la_firma_del_cliente(self):
        ev = _ev(estado="pendiente_aprobacion", interna=True, firma_tecnico_at=AHORA)
        self.assertEqual(ev["titulo"], "Trabajo realizado")
        self.assertEqual([f["nombre"] for f in ev["firmas"]], ["Técnico", "Servicio Técnico"])

    def test_en_pausa(self):
        ev = _ev(estado="pendiente_repuesto")
        self.assertEqual((ev["paso"], ev["tono"], ev["icono"], ev["titulo"]), ("visita", "ambar", "pausa", "Tu servicio está en pausa"))
        self.assertIn("repuesto", ev["mensaje"])
        self.assertIn("confirmar un dato", _ev(estado="pendiente_info")["mensaje"])

    def test_cerrada_con_las_tres_firmas_es_el_documento(self):
        ev = _ev(estado="cerrada", firma_tecnico_url="t", firma_cliente_url="c", firma_supervisor_url="s", cerrada_at=AHORA)
        self.assertEqual((ev["situacion"], ev["paso"], ev["tono"], ev["icono"], ev["linea"], ev["progreso"]), ("terminada", "documento", "verde", "documento", 100, 100))
        self.assertEqual(_estados(ev), ["hecho"] * 4)
        self.assertTrue(ev["documento_listo"])
        self.assertEqual(ev["que_sigue"], "")                                 # nada que prometer: ya terminó
        self.assertEqual(ev["mensaje"], "Tu orden quedó cerrada y firmada. Puedes ver el documento abajo.")
        self.assertEqual(ev["pasos"][3]["detalle"], "Listo")
        self.assertTrue(all(f["ok"] for f in ev["firmas"]))

    def test_cerrada_sin_una_firma_no_ofrece_el_documento(self):
        ev = _ev(estado="cerrada", firma_tecnico_url="t", firma_supervisor_url="s", cerrada_at=AHORA)
        self.assertFalse(ev["documento_listo"])
        self.assertEqual(ev["mensaje"], "Tu orden quedó cerrada. El documento firmado estará disponible pronto.")

    def test_cancelada_y_anulada_con_calma(self):
        ev = _ev(estado="cancelada")
        self.assertEqual((ev["situacion"], ev["paso"], ev["tono"], ev["icono"], ev["progreso"]), ("cancelada", None, "gris", "cancelada", 0))
        self.assertEqual(_estados(ev), ["inactivo"] * 4)
        self.assertEqual(ev["titulo"], "Esta visita fue cancelada")
        self.assertEqual(ev["firmas"], [])
        for malo in ("culpa", "error", "incumpl"):
            self.assertNotIn(malo, (ev["titulo"] + ev["mensaje"]).lower())
        self.assertEqual(_ev(estado="anulada")["titulo"], "Esta visita quedó sin efecto")

    def test_fecha_pasada_sin_registro_avisa_sin_prometer(self):
        ev = _ev(fecha=dt.date(2026, 11, 2))
        self.assertEqual((ev["paso"], ev["tono"], ev["icono"], ev["titulo"]), ("visita", "ambar", "alerta", "Visita pendiente de registro"))
        self.assertEqual(ev["que_sigue"], "")

    def test_equipos_y_tecnicos(self):
        ev = _ev()
        self.assertEqual((ev["n_equipos"], ev["equipos_txt"], ev["tecnicos"]), (2, "2 equipos", ["Lenin Urbina"]))
        self.assertEqual(_ev(equipos=["Uno"])["equipos_txt"], "1 equipo")
        self.assertEqual(_ev(equipos=[])["equipos_txt"], "—")
        self.assertEqual(_ev(equipos=["Trotadora ILUS Pro 9000 Edición Especial 2026 con consola táctil"])["equipos"][0],
                         "Trotadora ILUS Pro 9000 Edición Especial 2026 con consola táctil")       # completo, nunca cortado (REGLA #15)

    def test_horas_en_todos_los_formatos(self):
        self.assertEqual(_ev(hora_inicio=dt.time(9, 30), hora_fin=dt.timedelta(hours=11))["horario_txt"], "09:30 – 11:00")
        self.assertEqual(_ev(hora_inicio="14:00:00", hora_fin=None)["horario_txt"], "14:00")

    def test_un_estado_desconocido_se_trata_como_agendada(self):
        self.assertEqual(_ev(estado="lo_que_sea")["paso"], "agendada")

    def test_todos_los_iconos_existen_en_la_plantilla(self):
        tpl = FC.texto("templates/" + TPL)
        for ico in S.ICONOS:
            self.assertIn(f'id="ico-{ico}"', tpl, ico)
        usados = set()
        for kw in ({}, {"estado": "cancelada"}, {"estado": "cerrada"}, {"estado": "firmada_tecnico"}, {"estado": "pendiente_repuesto"},
                   {"estado": "en_curso"}, {"fecha": dt.date(2026, 11, 10)}, {"fecha": dt.date(2026, 11, 2)}):
            usados.add(_ev(**kw)["icono"])
        self.assertEqual(usados, set(S.ICONOS))


class TestJsonPublico(unittest.TestCase):
    def test_solo_las_claves_publicas_y_el_enlace_al_documento_solo_cuando_esta_listo(self):
        ev = _ev()
        j = S.a_json(ev, link_firmada="https://x/ot-firmada/T", link_soporte="https://soporte", actualizado="10:30")
        self.assertEqual(tuple(j), S.CLAVES_PUBLICAS)
        self.assertEqual(j["link_firmada"], "")                                 # en curso: no hay documento que mostrar
        self.assertEqual((j["link_soporte"], j["actualizado"]), ("https://soporte", "10:30"))
        ev2 = _ev(estado="cerrada", firma_tecnico_url="t", firma_cliente_url="c", firma_supervisor_url="s", cerrada_at=AHORA)
        self.assertEqual(S.a_json(ev2, link_firmada="https://x/ot-firmada/T")["link_firmada"], "https://x/ot-firmada/T")

    def test_nada_de_plata_ni_contacto_en_ningun_estado(self):
        for kw in ({}, {"estado": "cancelada"}, {"estado": "cerrada", "firma_tecnico_url": "t", "firma_cliente_url": "c", "firma_supervisor_url": "s"},
                   {"estado": "firmada_tecnico"}, {"estado": "pendiente_repuesto"}, {"estado": "en_curso"}, {"fecha": dt.date(2026, 11, 2)}):
            d = _base(costo=1500000, costo_proveedor=900000, margen=550000, proveedor="Milling SpA", oc="OC-123", contacto_tel="+56 9 1234 5678",
                      notas_internas="cobrar $50.000", **kw)
            j = json.dumps(S.a_json(S.evaluar(d, AHORA)), ensure_ascii=False)
            self.assertNotRegex(j, PLATA, kw)
            self.assertNotRegex(j, CONTACTO, kw)
            for secreto in ("Milling", "OC-123", "1500000", "cobrar"):
                self.assertNotIn(secreto, j, kw)


class TestToken(unittest.TestCase):
    def test_ida_y_vuelta_y_vencimiento(self):
        t = S.token(77, 2_000_000_000, "clave")
        self.assertEqual(S.validar(t, "clave", 1_999_999_999), 77)
        self.assertIsNone(S.validar(t, "clave", 2_000_000_001))             # vencido
        self.assertIsNone(S.validar(t, "otra-clave", 1_999_999_999))        # otra clave
        self.assertIsNone(S.validar(t[:-3] + "AAA", "clave", 1_999_999_999))   # manoseado
        self.assertIsNone(S.validar("", "clave", 1))
        self.assertIsNone(S.validar("x" * 300, "clave", 1))
        self.assertEqual(S.validar(S.token(77, 2_000_000_000, b"bytes"), b"bytes", 1), 77)
        self.assertNotIn("=", t)
        self.assertNotIn("77", t[:4])                                        # no se lee el id a simple vista (va codificado)

    def test_un_token_de_firma_remota_no_abre_el_seguimiento_aunque_use_la_misma_clave(self):
        """`_ot_firma_token` de app.py firma «vid:exp» sin propósito; el de seguimiento firma «seg:vid:exp» y exige ese prefijo."""
        amb = {"app": types.SimpleNamespace(secret_key="clave"), "time": types.SimpleNamespace(time=lambda: 1_000)}
        FC.extraer(amb, "app.py", ("_ot_firma_key", "_ot_firma_token", "_ot_firma_token_validar"), ())
        firma = amb["_ot_firma_token"](77, ttl_horas=1)
        self.assertEqual(amb["_ot_firma_token_validar"](firma), 77)         # sirve para firmar…
        self.assertIsNone(S.validar(firma, "clave", 1_000))                 # …pero no para el seguimiento
        seg = S.token(77, 2_000_000_000, "clave")
        self.assertIsNone(amb["_ot_firma_token_validar"](seg))              # y al revés tampoco

    def test_ventana_despues_del_cierre(self):
        cierre = dt.datetime(2026, 10, 1, 12, 0)
        self.assertTrue(S.vigente_tras_cierre(cierre, dt.datetime(2026, 10, 31, 11, 0)))
        self.assertFalse(S.vigente_tras_cierre(cierre, dt.datetime(2026, 10, 31, 13, 0)))
        self.assertTrue(S.vigente_tras_cierre(None, dt.datetime(2027, 1, 1)))
        self.assertTrue(S.vigente_tras_cierre("2026-10-01 12:00:00", dt.datetime(2026, 10, 20)))
        self.assertEqual((S.TTL_DIAS, S.DIAS_TRAS_CIERRE), (90, 30))


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
#  2) EL BLOQUE DEL CORREO
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
_TAG_TABLE = re.compile(r"<table\b[^>]*>", re.I)
_TAG_A = re.compile(r"<a\b[^>]*>", re.I)


class TestBloqueCorreo(unittest.TestCase):
    def test_a_prueba_de_outlook(self):
        h = C.recorrido_html("recordatorio", "nuestro técnico llega.", url_seguimiento="https://ilus.test/ot-seguimiento/abc")
        for tag in _TAG_TABLE.findall(h) + _TAG_A.findall(h):
            self.assertNotRegex(tag, r"(?<![a-z])(padding|margin)(-[a-z]+)?\s*:", tag)       # el CSS padding/margin (cellpadding="0" sí va)
        for tag in _TAG_TABLE.findall(h):
            self.assertIn('role="presentation"', tag, tag)
            self.assertIn('cellspacing="0"', tag, tag)
        self.assertEqual(h.count('width="30" height="30" align="center" valign="middle" bgcolor="'), 4)     # 4 círculos con bgcolor
        self.assertEqual(h.count('height="4" bgcolor="'), 4)                                                 # 4 tramos de barra con bgcolor
        self.assertNotIn("display:flex", h)
        self.assertNotIn("grid", h)
        # letra ≥ 12 px en todo el bloque
        for px in re.findall(r"font-size:(\d+)px", h):
            self.assertGreaterEqual(int(px), 12, h)
        # el botón: color y relleno en la celda, <a> de bloque
        self.assertIn('bgcolor="#dc2626" style="background:#dc2626;border-radius:8px;padding:13px 26px"', h)
        self.assertIn('<a href="https://ilus.test/ot-seguimiento/abc" target="_blank" style="display:block;color:#ffffff', h)
        self.assertIn("Ver seguimiento en vivo", h)

    def test_sin_url_no_hay_boton_y_la_url_va_escapada(self):
        self.assertNotIn("Ver seguimiento en vivo", C.recorrido_html("agendada", "x"))
        self.assertNotIn("Ver seguimiento en vivo", C.recorrido_html("agendada", "x", url_seguimiento="   "))
        h = C.recorrido_html("agendada", "x", url_seguimiento='https://a/b?x=1&y="2"')
        self.assertIn('href="https://a/b?x=1&amp;y=&quot;2&quot;"', h)
        self.assertEqual(C.boton_html("Ir", ""), "")

    def test_el_paso_correcto_por_correo(self):
        def colores(h):
            return [m for m in re.findall(r'height="4" bgcolor="(#[0-9a-f]{6})"', h)]
        V, R, G = "#16a34a", "#dc2626", "#d1d5db"
        self.assertEqual(colores(C.recorrido_html("agendada")), [R, G, G, G])
        self.assertEqual(colores(C.recorrido_html("recordatorio")), [V, R, G, G])
        self.assertEqual(colores(C.recorrido_html("visita")), [V, V, R, G])
        self.assertEqual(colores(C.recorrido_html("documento")), [V, V, V, V])
        self.assertEqual(_txt(C.recorrido_html("documento")).count("✓"), 4)
        self.assertEqual(_txt(C.recorrido_html("visita")).count("✓"), 2)
        self.assertEqual(C.recorrido_html("otro"), "")

    def test_el_correo_de_ot_creada_lleva_el_boton_a_su_ot(self):
        w = MundoRec()
        self.addCleanup(w.cerrar)
        FC.extraer(w.amb, "app.py", ("_ot_com_bloques_creacion",), ())
        w.amb.update({"_orec": O, "_ot_com_promesas": lambda: {"recordatorio": True, "documento": True}})
        b = w.amb["_ot_com_bloques_creacion"]({"id": 77, "tipo": "instalacion"}, {"tecnico": "Lenin", "maquina": "2 equipos"})
        self.assertIn('href="https://ilus.test/ot-seguimiento/SEG77"', b["recorrido_html"])
        self.assertIn("Ver seguimiento en vivo", _txt(b["recorrido_html"]))

    def test_el_recordatorio_de_24h_y_la_ot_terminada_llevan_el_boton(self):
        w = MundoRec()
        self.addCleanup(w.cerrar)
        w.amb["_ot_com_promesas"] = lambda: {"recordatorio": True, "documento": True}
        w.barrer(LUN(10, 5))
        h = w.para("contacto@gimnasio.cl")[0]["html"]
        self.assertIn('href="https://ilus.test/ot-seguimiento/SEG77"', h)
        self.assertNotIn("ot-seguimiento", w.para("lenin@sphs.cl")[0]["html"])          # el técnico no recibe el recorrido del cliente
        m = MundoOT(env_cliente="1")
        self.addCleanup(m.cerrar)
        m.f("_ot_terminada_notificar")(77)
        hc = next(e["html"] for e in m.m.enviados_reales if e["to"] == "contacto@gimnasio.cl")
        self.assertIn('href="https://ilus.test/ot-seguimiento/SEG77"', hc)

    def test_cableado_de_la_firma_remota_y_del_recordatorio(self):
        self.assertIn("url_seguimiento=_ot_seg_url(vid)", FC.funcion_top("app.py", "mant_ot_enviar_firma_remota"))
        self.assertIn('para="cliente", url_seguimiento=_ot_seg_url(vid)', FC.funcion_top("app.py", "_ot_recordatorio_barrido"))
        self.assertIn('/ot-seguimiento/ejemplo', FC.funcion_top("app.py", "_comm_correo_demo"))      # la vista previa nunca apunta a una OT real


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
#  3) LAS RUTAS PÚBLICAS (funciones reales de app.py en un mundo de mentira)
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
FUNCS_SEG = ("_ot_seg_token", "_ot_seg_url", "_ot_seg_recordatorio_enviado", "_ot_seg_datos", "_ot_seg_cabeceras", "_ot_seg_resolver",
             "_ot_seg_estado", "_ot_seg_no_disponible", "ot_publica_seguimiento", "ot_publica_seguimiento_estado")


class _Resp:
    def __init__(self, data, status=200):
        self.data, self.status_code, self.headers = data, status, {}


def _env_jinja():
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(os.path.join(RAIZ, "templates")), autoescape=True)
    env.globals["url_for"] = lambda ep, **kw: "/static/" + kw.get("filename", "") if ep == "static" else "/" + ep
    return env


class MundoSeg(MundoOT):
    """MundoOT (la OT 77 de mentira, con plata que NUNCA debe salir) + las funciones reales del seguimiento."""

    def __init__(self, **kw):
        super().__init__(**kw)
        self.reloj = 1_700_000_000
        self.cupo = True
        self.recordado = False
        self.ahora_chile = dt.datetime(2026, 11, 9, 10, 30, tzinfo=dt.timezone(dt.timedelta(hours=-3)))
        self.ahora_utc = dt.datetime(2026, 11, 9, 13, 30)
        a = self.amb
        FC.extraer(a, "app.py", FUNCS_SEG, ("_OT_SEG_TPL",))
        env = _env_jinja()
        orig_uno = a["mysql_fetchone"]

        def uno(sql, params=()):
            if "FROM mant_ot_recordatorios" in sql:
                return {"x": 1} if self.recordado else None
            return orig_uno(sql, params)
        mundo = self

        class _Datetime(dt.datetime):                           # `datetime.utcnow()` con el reloj del mundo; el resto, el datetime real
            @classmethod
            def utcnow(cls):
                return mundo.ahora_utc
        a.update({
            "_oseg": S, "_ot_firma_key": lambda: b"clave-de-prueba", "mysql_fetchone": uno,
            "time": types.SimpleNamespace(time=lambda: self.reloj), "datetime": _Datetime,
            "_now_chile": lambda: self.ahora_chile,
            "_rl_check": lambda scope, n, ventana: self.cupo,
            "_ot_com_promesas": lambda: {"recordatorio": True, "documento": True},
            "ILUS_SOPORTE_URL": "https://ilusfitness.com/pages/soporte-tecnico",
            "jsonify": lambda d: _Resp(d), "make_response": lambda body, status=200: _Resp(body, status),
            "render_template": lambda tpl, **ctx: env.get_template(tpl).render(**ctx),
        })

    def token(self, vid=77, exp=None):
        return S.token(vid, exp or self.reloj + 3600, b"clave-de-prueba")

    def pagina(self, tok):
        return self.amb["ot_publica_seguimiento"](tok)

    def estado(self, tok):
        return self.amb["ot_publica_seguimiento_estado"](tok)


class TestRutas(unittest.TestCase):
    def mundo(self, **kw):
        w = MundoSeg(**kw)
        self.addCleanup(w.cerrar)
        return w

    def test_la_url_del_correo_lleva_un_token_de_seguimiento_valido(self):
        w = self.mundo()
        url = w.amb["_ot_seg_url"](77)
        self.assertTrue(url.startswith("https://ilus.test/ot-seguimiento/"), url)
        tok = url.rsplit("/", 1)[1]
        self.assertEqual(S.validar(tok, b"clave-de-prueba", w.reloj), 77)
        self.assertIsNone(S.validar(tok, b"clave-de-prueba", w.reloj + 91 * 86400))       # vence a los 90 días
        self.assertNotIn("/77", url)                                                       # el id no viaja a la vista

    def test_pagina_y_json_con_un_token_valido(self):
        w = self.mundo(estado="cerrada")
        r = w.pagina(w.token())
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.headers["Cache-Control"], "no-store")
        self.assertEqual(r.headers["X-Robots-Tag"], "noindex, nofollow, noarchive")
        self.assertEqual(r.headers["Referrer-Policy"], "no-referrer")
        html = r.data
        self.assertIn("OT-2026-00077", html)
        self.assertIn("Trabajo terminado", html)
        self.assertIn("Ver orden de trabajo firmada", html)
        self.assertIn("https://ilus.test/ot-firmada/TOKEN77x720", html)                 # el documento firmado ya existente (30 días)
        self.assertIn("Lenin Urbina", html)
        self.assertIn("Trotadora ILUS Pro 9000", html)                                  # en la página sí cabe el detalle
        self.assertIn('class="seg-paso is-hecho"', html)
        j = w.estado(w.token())
        self.assertEqual(j.status_code, 200)
        self.assertTrue(j.data["ok"])
        self.assertEqual(j.data["paso"], "documento")
        self.assertEqual(set(j.data) - {"ok"}, set(S.CLAVES_PUBLICAS))
        self.assertEqual(j.headers["Cache-Control"], "no-store")
        self.assertEqual(j.data["actualizado"], "10:30")

    def test_ni_el_html_ni_el_json_llevan_plata_proveedores_ni_contacto_de_tecnicos(self):
        for estado in ("programada", "firmada_tecnico", "pendiente_aprobacion", "cerrada", "cancelada"):
            w = self.mundo(estado=estado)
            w.fila.update({"costo": 1500000, "costo_proveedor": 900000, "margen": 550000, "proveedor_nombre": "Milling SpA", "oc_numero": "OC-555",
                           "contacto_tel": "+56 9 1234 5678", "contacto_email": "contacto@gimnasio.cl", "notas_internas": "cobrar $50.000"})
            html = w.pagina(w.token()).data
            cuerpo = html.split("<main", 1)[1]                                           # lo que ve la persona (sin el <head>)
            j = json.dumps(w.estado(w.token()).data, ensure_ascii=False)
            for texto, nombre in ((_txt(cuerpo), "html"), (j, "json")):
                self.assertNotRegex(texto, PLATA, (estado, nombre))
                self.assertNotRegex(texto, CONTACTO, (estado, nombre))
                for secreto in ("Milling", "OC-555", "1500000", "cobrar", "lenin@sphs.cl", "aaron@sphs.cl"):
                    self.assertNotIn(secreto, texto, (estado, nombre))

    def test_token_invalido_vencido_o_de_otro_proposito_dan_la_misma_pagina_amable(self):
        w = self.mundo()
        vencido = w.token(exp=w.reloj - 1)
        firma = w.amb["_ot_firma_token"](77)                                    # el token de la firma remota (otro propósito)
        for tok in ("", "no-es-un-token", vencido, firma, "c2VnOjc3OjE3MDAwMDM2MDA6YWJjZGVmYWJjZGVmYWJjZGVmYWJjZGVm"):     # el último: bien formado, mal firmado
            r = w.pagina(tok)
            self.assertEqual(r.status_code, 404, tok)
            self.assertIn("Este enlace ya no está disponible", r.data)
            self.assertNotIn("OT-2026", r.data)
            self.assertEqual(r.headers["Cache-Control"], "no-store")
            j = w.estado(tok)
            self.assertEqual(j.status_code, 404, tok)
            self.assertEqual(j.data, {"ok": False, "error": "Este enlace ya no está disponible."})

    def test_una_ot_cerrada_deja_de_verse_30_dias_despues_del_cierre(self):
        w = self.mundo(estado="cerrada")
        w.fila["cerrada_at"] = dt.datetime(2026, 10, 1, 12, 0)
        w.ahora_utc = dt.datetime(2026, 10, 20, 12, 0)
        self.assertEqual(w.pagina(w.token()).status_code, 200)
        w.ahora_utc = dt.datetime(2026, 11, 9, 13, 30)
        self.assertEqual(w.pagina(w.token()).status_code, 404)
        self.assertEqual(w.estado(w.token()).status_code, 404)

    def test_una_ot_que_no_existe_da_404(self):
        w = self.mundo()
        w.fila = None
        self.assertEqual(w.pagina(w.token()).status_code, 404)

    def test_limite_por_conexion(self):
        w = self.mundo()
        w.cupo = False
        self.assertEqual(w.pagina(w.token()).status_code, 429)
        j = w.estado(w.token())
        self.assertEqual(j.status_code, 429)
        self.assertFalse(j.data["ok"])

    def test_solo_lectura_y_lo_que_sigue_respeta_los_interruptores(self):
        w = self.mundo(estado="programada")
        w.fila["fecha_programada"] = dt.date(2026, 11, 12)
        w.amb["_ot_com_promesas"] = lambda: {"recordatorio": False, "documento": False}
        antes = len(w.sql)
        j = w.estado(w.token())
        self.assertEqual(j.data["que_sigue"], "Nuestro técnico llegará en el horario indicado.")
        self.assertEqual(j.data["link_firmada"], "")
        self.assertEqual(len(w.sql), antes)                                     # ni un INSERT/UPDATE/DELETE
        self.assertEqual(w.logs, [])
        w.recordado = True
        self.assertEqual(next(p for p in w.estado(w.token()).data["pasos"] if p["clave"] == "recordatorio")["detalle"], "Enviado")

    def test_la_pagina_sin_js_ya_viene_dibujada(self):
        w = self.mundo(estado="firmada_tecnico")
        w.fila["firma_cliente_url"] = None
        html = w.pagina(w.token()).data
        self.assertIn("falta tu firma", html.lower())
        self.assertIn('class="seg-paso is-actual" data-clave="visita"', html)
        self.assertIn("ot_seguimiento.js", html)
        self.assertIn('id="segBtnDoc" href="#" hidden', html)                    # sin documento todavía: el botón no se ve


class TestCableado(unittest.TestCase):
    def test_las_rutas_existen_con_su_limite_y_sin_sesion(self):
        app = FC.texto("app.py")
        self.assertIn('@app.route("/ot-seguimiento/<token>")\ndef ot_publica_seguimiento(token):', app)
        self.assertIn('@app.route("/ot-seguimiento/<token>/estado")\ndef ot_publica_seguimiento_estado(token):', app)
        for fn in ("ot_publica_seguimiento", "ot_publica_seguimiento_estado"):
            f = FC.funcion_top("app.py", fn)
            self.assertIn("_rl_check(", f)
            self.assertIn("_ot_seg_resolver(token)", f)
            self.assertIn("_ot_seg_cabeceras(", f)
            self.assertNotIn("mysql_execute", f)                                   # solo lectura
        datos = FC.funcion_top("app.py", "_ot_seg_datos")
        self.assertNotRegex(datos, re.compile(r"cost|margen|proveedor|contacto_tel|contacto_email|\boc\b", re.I))

    def test_la_plantilla_el_css_y_el_js_cumplen_las_reglas(self):
        tpl = FC.texto("templates/" + TPL)
        self.assertNotIn("cdn.", tpl)                                             # nada por CDN
        self.assertNotIn("googleapis", tpl)
        self.assertIn('name="robots" content="noindex', tpl)
        self.assertIn('name="referrer" content="no-referrer"', tpl)
        self.assertIn("ILUS Fitness", tpl)
        self.assertNotIn("ILUS Sport", tpl)
        css = FC.texto("static/ot_seguimiento.css")
        self.assertIn("prefers-reduced-motion", css)
        self.assertIn("prefers-color-scheme: dark", css)
        for px in re.findall(r"font-size:\s*(\d+)px", css):
            self.assertGreaterEqual(int(px), 12)
        js = FC.texto("static/ot_seguimiento.js")
        self.assertNotRegex(js, r"\b(alert|confirm|prompt)\(")
        self.assertNotIn("innerHTML", js)                                        # todo lo que llega del servidor entra como texto
        self.assertIn("visibilityState", js)
        self.assertIn("30000", js)


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
#  4) EN CHROMIUM REAL: medidas (REGLA #61) y la actualización en vivo
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
try:
    from playwright.sync_api import sync_playwright
except Exception:                      # pragma: no cover
    sync_playwright = None

URL = "https://seg.test/ot-seguimiento/TOKENDEMO"


def _estado_demo(**kw):
    ev = S.evaluar(_base(**kw), AHORA, {"recordatorio": True, "documento": True})
    j = S.a_json(ev, link_firmada="https://seg.test/ot-firmada/T", link_soporte="https://ilusfitness.com/pages/soporte-tecnico", actualizado="10:30")
    return j


@unittest.skipIf(sync_playwright is None, "Playwright no está instalado")
class TestNavegadorReal(unittest.TestCase):
    """UN solo navegador y UNA sola página para todas las pruebas (poca memoria). La página se sirve desde un origen falso con el CSS y el JS reales."""

    @classmethod
    def setUpClass(cls):
        cls._pw = sync_playwright().start()
        try:
            cls.nav = cls._pw.chromium.launch()
        except Exception as e:         # sin Chromium instalado
            cls._pw.stop()
            raise unittest.SkipTest("Chromium no disponible: %s" % e)
        cls.env = _env_jinja()
        cls.json_estado = {"ok": True, **_estado_demo()}
        cls.html_actual = cls.env.get_template(TPL).render(s=_estado_demo(), token="TOKENDEMO", no_disponible=False)
        cls.ctx = cls.nav.new_context(viewport={"width": 390, "height": 844})
        cls.ctx.set_default_timeout(8000)
        cls.ctx.route("https://seg.test/**", cls.servir)
        cls.pg = cls.ctx.new_page()
        cls.errores = []
        cls.pg.on("pageerror", lambda e: cls.errores.append(str(e)))

    @classmethod
    def servir(cls, ruta):
        """El servidor de mentira: la página, el JSON de /estado y los archivos estáticos REALES del proyecto."""
        url = ruta.request.url
        if url.endswith("/estado"):
            ruta.fulfill(status=200, content_type="application/json", body=json.dumps(cls.json_estado, ensure_ascii=False))
        elif "/static/" in url:
            nombre = url.split("/static/", 1)[1].split("?")[0]
            p = os.path.join(RAIZ, "static", nombre)
            tipo = {"css": "text/css", "js": "application/javascript", "png": "image/png"}.get(nombre.rsplit(".", 1)[-1], "application/octet-stream")
            if os.path.exists(p):
                with open(p, "rb") as f:
                    ruta.fulfill(status=200, content_type=tipo, body=f.read())
            else:
                ruta.fulfill(status=404, body="")
        else:
            ruta.fulfill(status=200, content_type="text/html; charset=utf-8", body=cls.html_actual)

    @classmethod
    def tearDownClass(cls):
        try:
            cls.ctx.close()
            cls.nav.close()
        finally:
            cls._pw.stop()

    def abrir(self, ancho, **kw):
        type(self).html_actual = self.env.get_template(TPL).render(s=_estado_demo(**kw), token="TOKENDEMO", no_disponible=False)
        self.pg.set_viewport_size({"width": ancho, "height": 900})
        self.pg.goto(URL)
        self.pg.wait_for_selector("#segSteps")
        return self.pg

    JS_MEDIDAS = """() => {
      const doc = document.documentElement;
      const visibles = [...document.querySelectorAll('body *')].filter(e => {
        if (e.closest('.seg-sr') || e.tagName === 'SCRIPT' || e.tagName === 'STYLE' || e.tagName === 'SVG' || e.closest('svg')) return false;
        const tieneTexto = [...e.childNodes].some(n => n.nodeType === 3 && n.textContent.trim());
        if (!tieneTexto) return false;
        const r = e.getBoundingClientRect(); const cs = getComputedStyle(e);
        return r.width > 0 && r.height > 0 && cs.visibility !== 'hidden' && cs.display !== 'none';
      });
      const letras = visibles.map(e => parseFloat(getComputedStyle(e).fontSize));
      const fuera = [...document.querySelectorAll('body *')].filter(e => { const r = e.getBoundingClientRect(); return r.right > doc.clientWidth + 1 && r.width > 0; })
                      .map(e => e.className || e.tagName).slice(0, 5);
      const botones = [...document.querySelectorAll('.seg-btn, .seg-det summary')].filter(e => e.getBoundingClientRect().height > 0)
                      .map(e => Math.round(e.getBoundingClientRect().height));
      return { scrollW: doc.scrollWidth, clientW: doc.clientWidth, bodyScrollW: document.body.scrollWidth, letraMin: Math.min(...letras),
               fuera, botones, pasos: document.querySelectorAll('.seg-paso').length };
    }"""

    def test_390_820_y_1280_sin_scroll_de_lado_letra_12_y_botones_44(self):
        for ancho in (390, 820, 1280):
            pg = self.abrir(ancho)
            m = pg.evaluate(self.JS_MEDIDAS)
            self.assertLessEqual(m["scrollW"], m["clientW"], (ancho, m))
            self.assertLessEqual(m["bodyScrollW"], m["clientW"], (ancho, m))
            self.assertEqual(m["fuera"], [], (ancho, m))
            self.assertGreaterEqual(m["letraMin"], 12, (ancho, m))
            self.assertTrue(all(h >= 44 for h in m["botones"]), (ancho, m))
            self.assertEqual(m["pasos"], 4)
        self.assertEqual(self.errores, [])

    def test_nombres_largos_no_desbordan_en_el_celular(self):
        pg = self.abrir(390, equipos=["Trotadora ILUS Pro 9000 Edición Especial 2026 con consola táctil y sistema de amortiguación"] * 3,
                        tecnicos=["Lenin Alexander Urbina Rodríguez", "Pedro Antonio Pérez Castillo"], cliente="Sociedad Deportiva y de Inversiones Demo Limitada")
        pg.click("#segDet summary")
        m = pg.evaluate(self.JS_MEDIDAS)
        self.assertLessEqual(m["scrollW"], m["clientW"], m)
        self.assertEqual(m["fuera"], [], m)
        self.assertIn("consola táctil", pg.locator("#segEqLista").inner_text())        # completo, no cortado
        self.assertNotIn("…", pg.locator("#segTec").inner_text())

    def test_la_pagina_se_actualiza_sola_con_el_json_de_estado(self):
        pg = self.abrir(390)
        self.assertEqual(pg.locator("#segTitulo").inner_text(), "Tu visita está agendada")
        self.assertEqual(pg.locator(".seg-paso.is-hecho").count(), 0)
        self.assertFalse(pg.locator("#segBtnDoc").is_visible())
        # el servidor cambia: la OT quedó cerrada y firmada
        type(self).json_estado = {"ok": True, **_estado_demo(estado="cerrada", firma_tecnico_url="t", firma_cliente_url="c", firma_supervisor_url="s",
                                                               cerrada_at=AHORA, tecnicos=["Lenin Urbina", "Pedro Pérez"])}
        pg.evaluate("window.ILUS_SEG.cargar()")
        pg.wait_for_function("document.getElementById('segTitulo').textContent === 'Trabajo terminado'")
        self.assertEqual(pg.locator(".seg-paso.is-hecho").count(), 4)
        self.assertTrue(pg.locator("#segBtnDoc").is_visible())
        self.assertEqual(pg.locator("#segBtnDoc").get_attribute("href"), "https://seg.test/ot-firmada/T")
        self.assertEqual(pg.locator("#segTec").inner_text(), "Lenin Urbina, Pedro Pérez")
        self.assertEqual(pg.locator(".seg-firma.ok").count(), 3)
        self.assertEqual(pg.evaluate("document.documentElement.getAttribute('data-tono')"), "verde")
        self.assertEqual(pg.evaluate("document.getElementById('segIcoUse').getAttribute('href')"), "#ico-documento")
        self.assertEqual(pg.evaluate("document.getElementById('segFill').style.width"), "100%")
        self.assertFalse(pg.locator("#segSigue").is_visible())
        type(self).json_estado = {"ok": True, **_estado_demo()}
        self.assertEqual(self.errores, [])

    def test_sin_movimiento_si_la_persona_lo_pide(self):
        ctx = self.nav.new_context(viewport={"width": 390, "height": 844}, reduced_motion="reduce")
        try:
            ctx.route("https://seg.test/**", type(self).servir)
            pg = ctx.new_page()
            pg.goto(URL)
            pg.wait_for_selector("#segSteps")
            self.assertEqual(pg.evaluate("getComputedStyle(document.querySelector('.seg-paso.is-actual .seg-circ')).animationName"), "none")
            self.assertEqual(pg.evaluate("getComputedStyle(document.querySelector('.seg-live-dot')).animationName"), "none")
        finally:
            ctx.close()


if __name__ == "__main__":
    unittest.main()
