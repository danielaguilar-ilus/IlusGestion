# -*- coding: utf-8 -*-
"""Retiros 2026-10-14 — lo que pidió Daniel antes de presentarlo a gerencia:

· Orden de la ficha = orden de la guía: «Facturas y productos» (UNA tarjeta) → Responsable (paso 2, debajo de los productos) → Agenda →
  Preparación → Entrega. La guía se lee por clave, nunca por posición.
· «Alertar siempre, pero no tan intenso»: «te toca» en ámbar, botones de acción en azul (el rojo queda para errores reales).
· Fechas siempre día/mes/año (+ hora si lleva), nunca ISO ni en inglés.
· «Marcar como retirado» rehecho: quién se lo llevó (nombre obligatorio, RUT opcional), foto opcional; la firma solo en vista previa.
· Encuesta y firma «como rol»: las ve el superadmin o quien tenga ret_encuesta_firma (Roles → Retiros); botón «Encuestas» en el Monitor.
· La encuesta va en el correo «Retiro completado» (autorizado por Daniel 2026-10-14); el comprobante de firma por correo sigue apagado.

Correr con:  python -m pytest tests/test_retiros_14oct_ficha.py -q -p no:cacheprovider
"""
import os
import re
import sys

import pytest

AQUI = os.path.dirname(os.path.abspath(__file__))
RAIZ = os.path.dirname(AQUI)
for _p in (AQUI, RAIZ):
    if _p not in sys.path:
        sys.path.insert(0, _p)


def _leer(*partes):
    with open(os.path.join(RAIZ, *partes), encoding="utf-8") as f:
        return f.read()


FICHA = _leer("templates", "retiros", "internal_detail.html")


def _pos(html, marca):
    i = html.find(marca)
    assert i >= 0, marca
    return i


# ── Orden de la ficha ────────────────────────────────────────────────────────────────────────────────────────────
class TestOrdenDeLaFicha:
    def test_facturas_y_productos_es_una_sola_tarjeta_y_el_responsable_va_debajo(self):
        p2, p3, resp, p4 = (_pos(FICHA, f'id="{x}"') for x in ("paso-2", "paso-3", "paso-resp", "paso-4"))
        assert p2 < p3 < resp < p4
        # los productos son una subsección DENTRO de la tarjeta de facturas (no una tarjeta aparte)
        assert 'class="step-subsection' in FICHA[p3 - 200:p3]
        cierre_p2 = FICHA.find("</section>", p2)
        assert p3 < cierre_p2 < resp, "la tarjeta de facturas debe cerrarse después de los productos y antes del responsable"

    def test_la_tarjeta_dice_facturas_y_productos(self):
        bloque = FICHA[_pos(FICHA, 'id="paso-2"'):_pos(FICHA, 'id="paso-3"')]
        assert "Facturas y productos" in bloque and "¿Qué viene a retirar el cliente?" in bloque

    def test_el_js_de_la_guia_busca_los_pasos_por_clave_y_no_por_posicion(self):
        js = _leer("static", "retiros_guia.js")
        assert not re.search(r"G\.pasos\[\d", js), "la guía se lee por clave (pasoDe), nunca G.pasos[N]"
        assert "function pasoDe(clave)" in js
        for clave in ("documentos", "responsable", "preparacion", "entrega"):       # los que el JS lee directo (agenda va por CARDS/BTNS)
            assert f"pasoDe('{clave}')" in js

    def test_la_guia_tiene_5_columnas(self):
        css = _leer("static", "retiros_guia.css")
        assert "repeat(5" in css


# ── Colores: alertar sin gritar ──────────────────────────────────────────────────────────────────────────────────────
class TestColoresSuaves:
    def test_te_toca_en_ambar_y_botones_en_azul(self):
        css = _leer("static", "retiros_guia.css")
        assert "#f59e0b" in css and "#2563eb" in css
        assert re.search(r"\.step-section\.gp-e-actual\s+\.step-circle[^}]*#f59e0b", css), "el círculo del paso que toca va en ámbar"

    def test_proponer_fecha_ya_no_es_rojo(self):
        css = _leer("static", "retiros_internal_detail.css")
        m = re.search(r"#btnAbrirProponerFecha\.submit-btn-internal\{([^}]*)\}", css)
        assert m and "#2563eb" in m.group(1) and "ilus-red" not in m.group(1)


# ── Fechas día/mes/año ───────────────────────────────────────────────────────────────────────────────────────────────
class TestFechas:
    @pytest.mark.parametrize("plantilla", ["internal_detail.html", "calendario.html", "internal_dashboard.html"])
    def test_las_plantillas_no_usan_guiones_ni_iso_para_mostrar_fechas(self, plantilla):
        html = _leer("templates", "retiros", plantilla)
        assert "chile_fmt('%d-%m-%Y" not in html and 'chile_fmt("%d-%m-%Y' not in html
        assert "chile_fmt('%Y-%m-%d" not in html

    def test_el_monitor_formatea_dia_mes_anio(self):
        import retiros_monitor as rm
        assert rm.FMT_FECHA == "%d/%m/%Y" and rm.FMT_FECHA_HORA.startswith("%d/%m/%Y")

    def test_la_contrapropuesta_del_cliente_en_la_bitacora_va_en_dia_mes_anio(self):
        src = _leer("pickups_module.py")
        assert "f\"{str(proposal['date'])[:10]} {_td_to_hhmm(proposal['time_from'])}" not in src
        assert "_fmt_fecha_cl(proposal['date'])" in src


# ── «Marcar como retirado» ───────────────────────────────────────────────────────────────────────────────────────────
class TestModalRetirado:
    def _modal(self):
        i = _pos(FICHA, 'id="modalRetirar"')
        return FICHA[i:FICHA.find("</form>", i)]

    def test_un_solo_nombre_y_rut_y_la_foto_es_opcional(self):
        m = self._modal()
        assert m.count('name="retirado_por"') == 1 and m.count('name="retirado_por_rut"') == 1
        assert re.search(r'name="retirado_por"[^>]*required|required[^>]*name="retirado_por"', m), "el nombre es obligatorio"
        foto = re.search(r"<input[^>]*name=\"evidencia_foto\"[^>]*>", m)
        assert foto and "required" not in foto.group(0), "la foto es opcional"
        assert 'name="status" value="retirada"' in m and "csrf_token" in m and 'name="estado_visto"' in m

    def test_la_firma_en_el_modal_solo_se_ve_en_vista_previa(self):
        m = self._modal()
        i = m.find("_firma_bloque.html")
        assert i > 0 and "retiros_vista_previa()" in m[m.rfind("{% if", 0, i):i]


# ── Encuesta y firma «como rol» ──────────────────────────────────────────────────────────────────────────────────────
class TestVistaPreviaComoRol:
    def test_superadmin_o_permiso_si_los_demas_no(self, monkeypatch):
        import retiros_encuesta as re_
        monkeypatch.setenv("RETIROS_VISTA_PREVIA_USUARIOS", "nadie@ejemplo.cl")
        assert re_.usuario_en_vista_previa({"username": "x@sphs.cl", "role": "superadmin"}, {})
        assert re_.usuario_en_vista_previa({"username": "x@sphs.cl", "role": "admin"}, {"superadmin": True})
        assert re_.usuario_en_vista_previa({"username": "x@sphs.cl", "role": "ejecutivo"}, {"ret_encuesta_firma": True})
        assert not re_.usuario_en_vista_previa({"username": "x@sphs.cl", "role": "admin"}, {"retiros": True, "admin": True})
        assert not re_.usuario_en_vista_previa({"username": "x@sphs.cl", "role": "tecnico"}, {})
        assert not re_.usuario_en_vista_previa(None, {"superadmin": True})

    def test_el_permiso_existe_en_roles_y_arranca_apagado(self):
        app = _leer("app.py")
        assert '"ret_encuesta_firma",' in app
        assert 'base["ret_encuesta_firma"] = bool(ret.get("encuesta_firma"))' in app
        assert '"encuesta_firma": {"label": "Encuesta y firma de recepción (vista previa)"' in app

    def test_boton_encuestas_del_monitor_solo_en_vista_previa(self):
        html = _leer("templates", "retiros", "internal_dashboard.html")
        i = _pos(html, "url_for('retiros_encuesta_resultados')")
        bloque = html[html.rfind("{% if", 0, i):i]
        assert "retiros_vista_previa()" in bloque

    def test_el_dato_de_la_encuesta_en_la_ficha_solo_en_vista_previa(self):
        i = _pos(FICHA, "_encuesta_ficha.html")
        assert "retiros_vista_previa()" in FICHA[FICHA.rfind("{% if", 0, i):i]


# ── Encuesta autorizada en el correo «Retiro completado» ─────────────────────────────────────────────────────────────
class TestEncuestaAutorizada:
    def test_deploy_la_enciende_una_sola_vez_y_no_enciende_el_comprobante(self):
        y = _leer(".github", "workflows", "deploy.yml")
        assert y.count('append_env RETIROS_ENCUESTA_ACTIVA       "1"') == 1
        assert "|RETIROS_ENCUESTA_ACTIVA):" in y            # se limpia antes de agregarla (sin duplicados en el YAML de Cloud Run)
        assert "RETIROS_FIRMA_CORREO" not in y

    def test_solo_va_dentro_del_correo_de_retiro_completado(self):
        src = _leer("pickups_module.py")
        i = _pos(src, 'variables_html["encuesta"] = ""')
        bloque = src[i:i + 1500]
        assert 'if kind == "done":' in bloque and '_enc["invitar"]' in bloque


# ── Confirmar sin productos: el mensaje dice lo que pasa ─────────────────────────────────────────────────────────────
class TestMensajeSinProductos:
    def test_la_guia_explica_que_los_documentos_no_traen_productos(self):
        import retiros_guia as rg
        g = rg.evaluar(dict(status="en_revision", n_docs=1, docs=[], docs_firma="a", docs_conf="a", prod_n=0, prod_firma="b",
                            correo_ok=True, adelantado=False, cita=False, responsable="Ana", propuesta=False))
        prod = g["pasos"][0]["sub"]["productos"]
        assert prod["estado"] == "actual" and prod["accion"]["tipo"] == "ir"
        assert "no traen productos" in prod["faltan"][0]

    def test_el_endpoint_usa_ese_texto_y_no_pide_agregar_la_factura(self):
        src = _leer("pickups_module.py")
        i = _pos(src, "def _guia_confirmar(")
        cuerpo = src[i:i + 4000]
        assert 'falta = next((str(f) for f in (paso.get("faltan") or []) if f), "") if que != "docs" else ""' in cuerpo
        assert '"error": falta or "Primero agrega la factura o boleta del cliente."' in cuerpo


# ── Revisión adversarial 2026-10-14 (antes de desplegar) ─────────────────────────────────────────────────────────────
class TestRevisionAdversarial:
    def test_el_modal_retirado_hace_scroll_y_el_pie_queda_visible(self):
        css = _leer("static", "retiros_internal_detail.css")
        assert re.search(r"#modalRetirar \.rtm-form\{[^}]*display:flex[^}]*flex-direction:column[^}]*min-height:0", css)
        assert re.search(r"#modalRetirar \.rtm-body\{[^}]*overflow-y:auto", css)

    def test_la_tarjeta_compartida_es_de_la_entrega_cuando_la_preparacion_ya_esta_hecha(self):
        js = _leer("static", "retiros_guia.js")
        assert "if (p.clave === 'preparacion' && p.estado === 'hecho' && (pasoDe('entrega') || {}).estado !== 'hecho') return;" in js

    def test_enter_no_lleva_a_pasos_bloqueados_sin_responsable(self):
        js = _leer("static", "retiros_guia.js")
        assert "!(G.sin_responsable && p.n > nDe('responsable'))" in js

    def test_la_bitacora_y_los_avisos_no_guardan_fechas_iso(self):
        src = _leer("pickups_module.py")
        for viejo in ('"propuesta_enviada",\n                  f"{date} {tf}-{tt}"', 'f"{date} {tf}-{tt}", "cliente"', "nueva propuesta {date}",
                      "Intento de proponer {date}", "({date} {tf}-{tt}) → confirmado", "cita {date} {tf}-{tt}", "({str(proposal['date'])[:10]} ",
                      "bloqueadas para {fecha}.", "propuesta enviada al cliente \"\n                          f\"({date}"):
            assert viejo not in src.replace("\r\n", "\n"), viejo

    def test_los_excel_muestran_hora_chile_y_dia_mes_anio(self):
        src = _leer("pickups_module.py")
        assert 'return _utc_a_chile_txt(d) or str(d)[:16]' in src
        assert 'ws2.cell(ri, 13, _utc_a_chile_txt(r.get("created_at"))' in src
        assert 'f"Período: {_dmy(desde)} → {_dmy(hasta)}"' in src

    def test_la_firma_en_el_modal_pide_el_rut_en_el_campo_visible(self):
        js = _leer("static", "retiros_firma.js")
        assert "en «¿Quién se lo llevó?» (con la firma es obligatorio)" in js and "$('rtmRut').focus()" in js

    def test_la_ficha_muestra_el_anio_en_las_fechas_con_dia(self):
        assert "chile_fmt('%a %d/%m')" not in FICHA and "chile_fmt('%A %d/%m')" not in FICHA

    def test_hoy_y_manana_se_calculan_en_hora_local(self):
        js = _leer("static", "retiros_internal_detail.js")
        assert "function _isoLocalRi(d)" in js
        assert "new Date(Date.now() + 24*3600*1000).toISOString()" not in js
