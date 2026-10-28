# -*- coding: utf-8 -*-
"""2026-10-27 · Formulario público de tickets con el formato de Retiros + versión para la tienda (Shopify).

Pedido de Daniel: «el formulario de tickets con el MISMO formato visual que el formulario de retiros de la tienda, y que
los tickets también se puedan abrir desde la página de la tienda, igual que los retiros».

Qué se verifica (sin BD real, sin ERP y sin correo: una app Flask de mentira con tickets_module registrado):

  · CORS solo para la tienda real (ilusfitness.com, www, ilus-fitness.myshopify.com y «null» = archivo abierto suelto),
    solo en /soporte/api/* y sin cookies; el preflight OPTIONS responde con los encabezados correctos.
  · REGLA #22 (formulario público = superficie expuesta): un envío de más de 64 KB → 413 sin escribir nada; el campo
    trampa y el envío en menos de 3 s se descartan; 8 envíos cada 5 min, 10 por hora y 30 por día por conexión.
  · La dirección escrita a mano (tienda, sin Google Places) se acepta SOLO con direccion_manual=1 y deja constancia
    en la bitácora; sin la marca, sigue exigiendo la dirección sugerida por Google.
  · La plantilla /soporte renderiza con Jinja, conserva TODOS los campos/ids (REGLA #4.2), tiene los 13 tipos en la
    grilla compacta, barra de progreso, campo trampa y nada de alert/confirm/prompt nativos (REGLA #1).
  · El archivo para Shopify existe, habla con la API de producción por los mismos endpoints, no trae claves y conserva
    los 13 tipos.

    py -m pytest tests/test_tickets_soporte_publico_tienda.py -q
"""
import io
import os
import re
import sys
import time
import itertools

import pytest
from flask import Flask, jsonify

_TESTS = os.path.dirname(os.path.abspath(__file__))
_RAIZ = os.path.dirname(_TESTS)
sys.path.insert(0, _RAIZ)

import tickets_module as tm  # noqa: E402

PLANTILLA = os.path.join(_RAIZ, "templates", "tickets", "soporte_publico.html")
SHOPIFY = os.path.join(_RAIZ, "Formulario_ticket_Shopify_ILUS.html")
API_PROD = "https://ilus-app-469212710544.southamerica-west1.run.app"
# 2026-10-27 (Daniel): 9 opciones en lenguaje simple; las que no son de un equipo saltan dirección y equipo.
TIPOS_PUBLICOS = ("tech_support", "install", "maintenance", "spare_parts", "shipping", "return", "facturacion", "quotation", "consulta")
TIPOS_SIN_EQUIPO = ("shipping", "return", "facturacion", "quotation", "consulta")
FUERA_DEL_PUBLICO = ("warranty", "repair", "tech_evaluation", "equipment_transfer", "spare_parts_store", "spare_parts_import")

JSON = {"Accept": "application/json"}
_ips = itertools.count(10)


# ── App de mentira (mismo patrón que tests/test_tickets_migracion_documentos.py) ───────────────────────────────────────────
class _Cursor:
    lastrowid = 77

    def __init__(self, esc):
        self._esc = esc

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, q, params=None):
        self._esc.append((" ".join(str(q).split()), params))

    def fetchone(self):
        return None

    def fetchall(self):
        return []


class _Conn:
    def __init__(self, esc):
        self._esc = esc

    def cursor(self):
        return _Cursor(self._esc)

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass


def armar(extra=None):
    """Devuelve (app, escrituras). Cada prueba arma su app: el limitador por IP vive en memoria dentro del módulo."""
    escrituras = []
    app = Flask(__name__, template_folder=os.path.join(_RAIZ, "templates"), static_folder=os.path.join(_RAIZ, "static"))
    app.config["TESTING"] = True
    app.secret_key = "clave-de-prueba"
    ctx = {
        "mysql_fetchone": lambda q, p=None: None,
        "mysql_fetchall": lambda q, p=None: [],
        "mysql_execute": lambda q, p=None: escrituras.append((" ".join(str(q).split()), p)),
        "get_mysql": lambda: _Conn(escrituras),
        "login_required": lambda f: f,
        "_no_tecnico_externo": lambda f: f,
        "current_username": lambda: "prueba@example.cl",
        # Sin _send_ilus_email en el ctx: _tk_notificar_lifecycle vuelve de inmediato → ningún correo (REGLA #22).
    }
    ctx.update(extra or {})
    tm.register_tickets_routes(app, ctx)

    @app.route("/otra-ruta")
    def _otra():
        return jsonify({"ok": True})

    escrituras.clear()  # lo que escribió el arranque (CREATE/seed) no cuenta
    return app, escrituras


def cliente(app):
    """Cliente con una IP propia: el limitador cuenta por conexión."""
    return app.test_client(), {"REMOTE_ADDR": f"10.0.0.{next(_ips)}"}


def datos(**kw):
    d = {"tipo": "tech_support", "rut": "76.543.210-3", "empresa": "Cliente Prueba SpA", "nombre_contacto": "Juan Pérez",
         "phone": "+56 9 1234 5678", "email": "prueba@example.cl", "direccion": "Av. Colón 1265, Las Condes",
         "direccion_lat": "-33.41", "direccion_lng": "-70.58", "descripcion": "La consola se apaga sola.",
         "productos": [], "ilus_t0": int(time.time()) - 60}
    d.update(kw)
    return d


def esperar_hilos():
    import threading
    for t in list(threading.enumerate()):
        if t is not threading.current_thread() and not t.daemon:
            continue
        if t is threading.current_thread():
            continue
        try:
            t.join(0.5)
        except RuntimeError:
            pass


def inserts_ticket(esc):
    return [e for e in esc if "insert into tk_tickets" in e[0].lower()]


# ── CORS ────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
class TestOrigenTienda:
    @pytest.mark.parametrize("origen,permitido", [
        ("https://ilusfitness.com", True), ("https://www.ilusfitness.com", True), ("https://ilus-fitness.myshopify.com", True),
        ("null", True),                                     # el archivo HTML abierto desde el computador (revisión antes de pegarlo)
        ("https://otra-tienda.myshopify.com", False), ("https://evil.example", False),
        ("https://ilusfitness.com.evil.example", False), ("http://ilusfitness.com", False)])
    def test_solo_la_tienda_real(self, origen, permitido):
        app, _esc = armar()
        cli, env = cliente(app)
        r = cli.get("/soporte/api/erp/productos?q=trotadora", headers={"Origin": origen}, environ_base=env)
        assert (r.headers.get("Access-Control-Allow-Origin") == origen) is permitido
        assert "Access-Control-Allow-Credentials" not in r.headers      # nunca cookies

    def test_la_funcion_que_decide_es_la_misma_para_todas_las_rutas(self):
        app, _esc = armar()
        ok = app.config["_tk_soporte_origen_permitido"]
        assert ok("https://ilusfitness.com") and ok("null") and not ok("") and not ok("https://x.myshopify.com")

    def test_el_preflight_de_crear_responde_con_los_encabezados(self):
        app, esc = armar()
        cli, env = cliente(app)
        r = cli.options("/soporte/api/crear", environ_base=env, headers={
            "Origin": "https://ilusfitness.com", "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "content-type"})
        assert r.status_code in (200, 204)
        assert r.headers.get("Access-Control-Allow-Origin") == "https://ilusfitness.com"
        assert "POST" in r.headers.get("Access-Control-Allow-Methods", "")
        assert "Content-Type" in r.headers.get("Access-Control-Allow-Headers", "")
        assert "X-Upload-Token" in r.headers.get("Access-Control-Allow-Headers", "")
        assert r.headers.get("Access-Control-Max-Age") == "600"
        assert not esc                                      # el preflight no escribe nada

    def test_el_preflight_de_adjuntos_tambien(self):
        app, _esc = armar()
        cli, env = cliente(app)
        r = cli.options("/soporte/api/adjuntos/5", environ_base=env, headers={
            "Origin": "https://ilus-fitness.myshopify.com", "Access-Control-Request-Method": "POST"})
        assert r.status_code in (200, 204)
        assert r.headers.get("Access-Control-Allow-Origin") == "https://ilus-fitness.myshopify.com"
        assert "POST" in r.headers.get("Access-Control-Allow-Methods", "")

    def test_la_subida_de_adjuntos_desde_la_tienda_lleva_cors_tambien_en_la_respuesta(self):
        """El POST real (multipart) desde ilusfitness.com: la respuesta trae el origen aunque el ticket no exista (404)."""
        app, _esc = armar()
        cli, env = cliente(app)
        r = cli.post("/soporte/api/adjuntos/5", environ_base=env, headers={"Origin": "https://ilusfitness.com"},
                     data={"token": "malo", "file": (io.BytesIO(b"\xff\xd8\xff x"), "foto.jpg")}, content_type="multipart/form-data")
        assert r.status_code == 403                          # el token de ESE ticket es la protección real
        assert r.headers.get("Access-Control-Allow-Origin") == "https://ilusfitness.com"

    def test_config_de_la_tienda(self):
        """/soporte/api/config: lo que el HTML de la tienda pide al cargar (clave de navegador de Google + topes), con CORS."""
        app, esc = armar()
        cli, env = cliente(app)
        r = cli.get("/soporte/api/config", headers={"Origin": "https://www.ilusfitness.com"}, environ_base=env)
        assert r.status_code == 200
        assert r.headers.get("Access-Control-Allow-Origin") == "https://www.ilusfitness.com"
        assert r.headers.get("Cache-Control") == "no-store"
        d = r.get_json()
        assert d["ok"] is True and "google_maps_api_key" in d
        assert d["max_adjuntos"] == 15 and d["max_adjunto_mb"] == 25 and ".jpg" in d["ext_permitidas"]
        assert [t["id"] for t in d["tipos"]] == list(TIPOS_PUBLICOS)
        assert all((t["id"] in TIPOS_SIN_EQUIPO) == (not t["equipo"]) for t in d["tipos"])
        assert d["tipos"][0]["titulo"] == "Mi equipo falla" and d["tipos"][0]["linea"]
        assert abs(d["ahora_ts"] - time.time()) < 5
        assert not esc                                      # solo lectura
        assert set(d) == {"ok", "google_maps_api_key", "max_adjuntos", "max_adjunto_mb", "ext_permitidas", "tipos", "ahora_ts"}, \
            "ningún otro dato del sistema viaja a la tienda"

    def test_otras_rutas_no_reciben_cors(self):
        app, _esc = armar()
        cli, env = cliente(app)
        r = cli.get("/otra-ruta", headers={"Origin": "https://ilusfitness.com"}, environ_base=env)
        assert r.status_code == 200 and "Access-Control-Allow-Origin" not in r.headers
        r = cli.get("/soporte", headers={"Origin": "https://ilusfitness.com"}, environ_base=env)
        assert "Access-Control-Allow-Origin" not in r.headers  # la página se embebe por iframe (CSP de app.py), no por CORS


# ── Tamaño, anti-bots y límites (REGLA #22) ─────────────────────────────────────────────────────────────────────────────────
class TestSuperficieExpuesta:
    def test_un_envio_de_mas_de_64_kb_se_descarta_sin_escribir(self):
        app, esc = armar()
        cli, env = cliente(app)
        r = cli.post("/soporte/api/crear", json=datos(descripcion="x" * (70 * 1024)), environ_base=env)
        assert r.status_code == 413 and r.get_json()["ok"] is False
        assert not esc

    def test_un_envio_normal_pasa(self):
        app, esc = armar()
        cli, env = cliente(app)
        r = cli.post("/soporte/api/crear", json=datos(), environ_base=env)
        esperar_hilos()
        assert r.status_code == 200, r.get_json()
        d = r.get_json()
        assert d["ok"] is True and d["id"] == 77 and d["upload_token"]
        assert len(inserts_ticket(esc)) == 1

    def test_el_campo_trampa_lleno_se_descarta(self):
        app, esc = armar()
        cli, env = cliente(app)
        r = cli.post("/soporte/api/crear", json=datos(hp_ilus_extra="http://spam.example"), environ_base=env)
        assert r.status_code == 400 and "soportetec@sphs.cl" in r.get_json()["error"]
        assert not inserts_ticket(esc)

    def test_un_envio_en_menos_de_3_segundos_se_descarta(self):
        app, esc = armar()
        cli, env = cliente(app)
        r = cli.post("/soporte/api/crear", json=datos(ilus_t0=int(time.time())), environ_base=env)
        assert r.status_code == 400
        assert not inserts_ticket(esc)

    def test_sin_ilus_t0_no_se_usa_el_reloj(self):
        """El formulario de siempre no mandaba ilus_t0: nada cambia para él."""
        app, esc = armar()
        cli, env = cliente(app)
        d = datos()
        del d["ilus_t0"]
        r = cli.post("/soporte/api/crear", json=d, environ_base=env)
        esperar_hilos()
        assert r.status_code == 200, r.get_json()

    def test_un_cuerpo_que_no_es_un_objeto_no_rompe(self):
        app, _esc = armar()
        cli, env = cliente(app)
        r = cli.post("/soporte/api/crear", json=["lista"], environ_base=env)
        assert r.status_code == 400

    def test_limites_por_conexion(self):
        app, _esc = armar()
        lim = app.config["_tk_soporte_limites"]
        assert lim == {"por_hora": 10, "por_dia": 30, "max_bytes": 64 * 1024, "min_segundos": 3}
        src = open(os.path.join(_RAIZ, "tickets_module.py"), encoding="utf-8").read()
        bloque = src.split("def _soporte_rechazo_previo")[1].split("def _soporte_es_bot")[0]
        assert '_rl_ok("crear:" + ip, 8, 300)' in bloque                 # el freno corto de siempre sigue (REGLA #4.2)
        assert '_rl_ok("crear_hora:" + ip, SOPORTE_MAX_POR_HORA, 3600)' in bloque
        assert '_rl_ok("crear_dia:" + ip, SOPORTE_MAX_POR_DIA, 86400)' in bloque

    def test_el_noveno_envio_seguido_desde_la_misma_conexion_da_429(self):
        app, esc = armar()
        cli, env = cliente(app)
        for i in range(8):
            r = cli.post("/soporte/api/crear", json=datos(email=f"c{i}@example.cl"), environ_base=env)
            esperar_hilos()
            assert r.status_code == 200, (i, r.get_json())
        r = cli.post("/soporte/api/crear", json=datos(), environ_base=env)
        assert r.status_code == 429
        assert len(inserts_ticket(esc)) == 8

    def test_otra_conexion_no_se_ve_afectada(self):
        app, _esc = armar()
        cli, env = cliente(app)
        for i in range(9):
            cli.post("/soporte/api/crear", json=datos(), environ_base=env)
            esperar_hilos()
        cli2, env2 = cliente(app)
        r = cli2.post("/soporte/api/crear", json=datos(), environ_base=env2)
        esperar_hilos()
        assert r.status_code == 200


# ── Los 9 tipos públicos y los 2 nuevos del sistema ─────────────────────────────────────────────────────────────────────────
class TestTiposPublicos:
    def test_la_lista_publica_son_las_9_opciones_y_los_demas_tipos_siguen_en_el_sistema(self):
        assert tm.TK_TIPOS_PUBLICOS == TIPOS_PUBLICOS
        assert tm.TK_TIPOS_SIN_EQUIPO == TIPOS_SIN_EQUIPO
        for t in FUERA_DEL_PUBLICO:
            assert t in tm.TK_TIPOS and t in tm.TIPO_LABEL and t not in tm.TK_TIPOS_PUBLICOS      # REGLA #4.2: nada se borra
        assert set(TIPOS_PUBLICOS) == set(tm.TK_TIPOS_PUBLICOS_TEXTO)
        assert all(len(v) == 2 and v[0] and v[1] for v in tm.TK_TIPOS_PUBLICOS_TEXTO.values())

    def test_los_dos_tipos_nuevos_van_al_final_del_enum_y_en_todas_las_listas(self):
        assert tm.TK_TIPOS[-2:] == ("facturacion", "consulta")
        assert tm.TIPO_LABEL["facturacion"] == "Facturación / nota de crédito" and tm.TIPO_LABEL["consulta"] == "Consulta general"
        assert "facturacion" in tm.TK_TIPOS_MODAL and "consulta" in tm.TK_TIPOS_MODAL and "warranty" not in tm.TK_TIPOS_MODAL
        src = open(os.path.join(_RAIZ, "tickets_module.py"), encoding="utf-8").read()
        crea = src.split("CREATE TABLE IF NOT EXISTS tk_tickets (")[1].split("prioridad")[0]
        modifica = src.split('"ALTER TABLE tk_tickets MODIFY COLUMN tipo "')[1].split("NULL")[0]
        for bloque in (crea.split("tipo")[1], modifica):
            valores = re.findall(r"'([a-z_]+)'", bloque)
            assert valores[-2:] == ["facturacion", "consulta"], valores      # al FINAL (REGLA #18)
            assert valores[:16] == list(tm.TK_TIPOS[:16])                     # nada se reordena
        list_html = open(os.path.join(_RAIZ, "templates", "tickets", "list.html"), encoding="utf-8").read()
        assert "'facturacion': ('bi-receipt'" in list_html and "'consulta': ('bi-chat-left-text'" in list_html

    @pytest.mark.parametrize("tipo", TIPOS_SIN_EQUIPO)
    def test_una_opcion_sin_equipo_no_exige_direccion(self, tipo):
        app, esc = armar()
        cli, env = cliente(app)
        r = cli.post("/soporte/api/crear", environ_base=env, json=datos(
            tipo=tipo, direccion="", direccion_lat=None, direccion_lng=None, numero_documento="1136", productos=[]))
        esperar_hilos()
        assert r.status_code == 200, r.get_json()
        params = inserts_ticket(esc)[0][1]
        assert params[0] == tipo and "1136" in params and None in params      # dirección NULL, documento guardado

    @pytest.mark.parametrize("tipo", [t for t in TIPOS_PUBLICOS if t not in TIPOS_SIN_EQUIPO])
    def test_una_opcion_de_equipo_sigue_exigiendo_direccion_validada(self, tipo):
        app, esc = armar()
        cli, env = cliente(app)
        r = cli.post("/soporte/api/crear", json=datos(tipo=tipo, direccion="", direccion_lat=None, direccion_lng=None), environ_base=env)
        assert r.status_code == 400 and not inserts_ticket(esc)
        r = cli.post("/soporte/api/crear", json=datos(tipo=tipo, direccion_lat=None, direccion_lng=None), environ_base=env)
        assert r.status_code == 400 and "Google" in r.get_json()["error"]

    @pytest.mark.parametrize("tipo", FUERA_DEL_PUBLICO)
    def test_un_tipo_que_salio_de_la_lista_publica_no_entra_por_el_formulario(self, tipo):
        app, esc = armar()
        cli, env = cliente(app)
        r = cli.post("/soporte/api/crear", json=datos(tipo=tipo), environ_base=env)
        assert r.status_code == 400 and not inserts_ticket(esc)

    def test_el_catalogo_publico_solo_dice_sku_y_nombre(self):
        """Bodega 02, solo lectura (REGLA #4.1); al público nunca stock ni cantidades aunque la consulta traiga más columnas."""
        consultas = []

        def sql(q, params=(), max_rows=None):
            consultas.append((" ".join(q.split()), params))
            return [{"sku": "TR-01", "nombre": "Trotadora X1", "stock": 7, "precio": 999999},
                    {"sku": "TR-02", "nombre": "Trotadora X2", "stock": 0}]

        app, _esc = armar(extra={"_random_sql_query": sql})
        cli, env = cliente(app)
        r = cli.get("/soporte/api/erp/productos?q=trot", environ_base=env)
        assert r.status_code == 200
        d = r.get_json()
        assert d["resultados"] == [{"sku": "TR-01", "nombre": "Trotadora X1"}, {"sku": "TR-02", "nombre": "Trotadora X2"}]
        assert "stock" not in r.get_data(as_text=True) and "precio" not in r.get_data(as_text=True)
        q, params = consultas[0]
        assert q.lstrip().upper().startswith("SELECT") and "02" in params         # bodega 02, solo SELECT


# ── Dirección escrita a mano (respaldo del backend; la tienda ya usa Google) ────────────────────────────────────────────────
class TestDireccionManual:
    def test_sin_coordenadas_y_sin_la_marca_sigue_exigiendo_google(self):
        app, esc = armar()
        cli, env = cliente(app)
        r = cli.post("/soporte/api/crear", json=datos(direccion_lat=None, direccion_lng=None), environ_base=env)
        assert r.status_code == 400 and "Google" in r.get_json()["error"]
        assert not inserts_ticket(esc)

    def test_con_la_marca_se_acepta_y_queda_en_la_bitacora(self):
        app, esc = armar()
        cli, env = cliente(app)
        r = cli.post("/soporte/api/crear", environ_base=env, json=datos(
            direccion_lat=None, direccion_lng=None, direccion_manual=1, comuna_nombre="Las Condes"))
        esperar_hilos()
        assert r.status_code == 200, r.get_json()
        ins = inserts_ticket(esc)
        assert len(ins) == 1
        params = ins[0][1]
        assert "Av. Colón 1265, Las Condes" in params and "Las Condes" in params
        assert -33.41 not in params and "-33.41" not in params        # sin coordenadas inventadas
        logs = [e for e in esc if "insert into tk_mensajes" in e[0].lower()]
        assert any("sin verificar en Google Maps" in str(e[1]) for e in logs), "la bitácora debe decir que la dirección es por confirmar"
        assert any("la tienda" in str(e[1]) for e in logs)

    def test_la_tienda_se_identifica_con_origen_form(self):
        app, esc = armar()
        cli, env = cliente(app)
        r = cli.post("/soporte/api/crear", json=datos(origen_form="tienda"), environ_base=env)
        esperar_hilos()
        assert r.status_code == 200
        logs = [e for e in esc if "insert into tk_mensajes" in e[0].lower()]
        assert any("la tienda (ilusfitness.com)" in str(e[1]) for e in logs)
        assert not any("sin verificar" in str(e[1]) for e in logs)                 # con Google: nada por confirmar

    def test_la_marca_no_deja_pasar_una_direccion_vacia(self):
        app, esc = armar()
        cli, env = cliente(app)
        r = cli.post("/soporte/api/crear", json=datos(direccion="", direccion_lat=None, direccion_lng=None, direccion_manual=1), environ_base=env)
        assert r.status_code == 400
        assert not inserts_ticket(esc)

    def test_con_google_las_coordenadas_se_guardan_como_siempre(self):
        app, esc = armar()
        cli, env = cliente(app)
        r = cli.post("/soporte/api/crear", json=datos(), environ_base=env)
        esperar_hilos()
        assert r.status_code == 200
        params = inserts_ticket(esc)[0][1]
        assert "-33.41" in params and "-70.58" in params


# ── La plantilla /soporte ───────────────────────────────────────────────────────────────────────────────────────────────────
class TestPlantilla:
    IDS = ["spForm", "spTipos", "errTipo", "spRut", "fRutWrap", "spEmpresa", "spContacto", "fContactoWrap", "spPhone", "fPhoneWrap",
           "spEmail", "fEmailWrap", "spDireccion", "fDirWrap", "spDirLat", "spDirLng", "spDirPlace", "spComuna", "spRegion",
           "spCodigoPostal", "spDirHint", "spSucursal", "prodOptCard", "spProdToggle", "prodBuscarWrap", "fProdWrap", "spBuscarProd",
           "spAcProd", "spProdChips", "spDoc", "spDesc", "spDescCount", "fDescWrap", "spGarantia", "spMultiToggleWrap",
           "spMotivoDistinto", "spMotivosPorEquipo", "spDrop", "spFile", "spFiles", "spBtnEnviar", "spOkOverlay", "spOk", "spNumOk",
           "spOkBarFill", "spToast", "spHp", "spT0", "itkProg", "itkProgBar", "itkProgTxt", "spAlert"]

    def test_jinja_la_parsea(self):
        app, _esc = armar()
        app.jinja_env.parse(open(PLANTILLA, encoding="utf-8").read())

    def test_renderiza_con_todo_lo_de_antes(self):
        app, _esc = armar()
        cli, env = cliente(app)
        r = cli.get("/soporte", environ_base=env)
        assert r.status_code == 200
        html = r.get_data(as_text=True)
        for i in self.IDS:
            assert f'id="{i}"' in html, f"falta id={i}"
        for t in TIPOS_PUBLICOS:
            assert f'data-tipo="{t}"' in html, f"falta el tipo {t}"
        for t in FUERA_DEL_PUBLICO:
            assert f'data-tipo="{t}"' not in html
        assert html.count('class="sp-tipo"') == 9
        for t in TIPOS_SIN_EQUIPO:
            assert f'data-tipo="{t}" data-equipo="0"' in html
        assert 'data-tipo="tech_support" data-equipo="1"' in html
        for titulo, linea in tm.TK_TIPOS_PUBLICOS_TEXTO.values():
            assert f'<span class="tt">{titulo}</span><span class="tl">{linea}</span>' in html
        assert "/soporte/api/crear" in html and "/soporte/api/adjuntos/" in html and "/soporte/api/erp/productos" in html
        assert "ilusPlacesAutocomplete" in html and "ilus_ui.js" in html
        assert 'name="hp_ilus_extra"' in html
        assert re.search(r'id="spT0" value="\d{9,}"', html), "la hora del servidor va en el formulario (anti-bots)"
        assert "0 de 4 pasos listos" in html
        assert 'const TIPOS_SIN_EQUIPO = ["shipping", "return", "facturacion", "quotation", "consulta"];' in html
        assert "ILUS Fitness" in html and "ILUS Sport" not in html             # REGLA #0

    def test_formato_retiros_y_tipos_compactos(self):
        src = open(PLANTILLA, encoding="utf-8").read()
        assert 'id="ilus-tk"' in src and "#ilus-tk .itk-step" in src and "#ilus-tk .itk-circle" in src
        assert "#1C1C1C" in src and "--r:#6d6d6d" in src                      # negro + acento gris de la tienda
        assert "grid-template-columns:repeat(3,minmax(0,1fr))" in src.split(".sp-tipos{")[1].split("}")[0]   # 3×3
        assert "min-height:60px" in src.split(".sp-tipo{")[1].split("}")[0]
        movil = src.split("@media(max-width:720px)")[1]
        assert "repeat(2,minmax(0,1fr))" in movil and "font-size:16px" in movil   # REGLA #3
        assert 'class="itk-step is-opt" data-paso="4" id="spPaso4" data-equipo-only' in src   # el equipo sigue siendo opcional
        assert 'id="spPaso3" data-equipo-only' in src                           # dirección solo para opciones de equipo

    def test_documento_visible_siempre_y_comuna_se_completa_sola(self):
        """El N° de boleta/factura/pedido se MOVIÓ al paso «Cuéntanos» (siempre visible: en «¿Dónde está mi pedido?» y
        «Cambiar o devolver» es la clave para ubicar la compra). Sigue opcional. La comuna se ve y la llena Google."""
        src = open(PLANTILLA, encoding="utf-8").read()
        paso5 = src.split('id="spPaso5"')[1].split("</section>")[0]
        assert 'id="spDoc"' in paso5 and "N° de boleta, factura o pedido" in paso5 and 'class="op">Opcional' in paso5.split('id="spDoc"')[0][-200:]
        assert 'id="spDoc"' not in src.split('id="spPaso4"')[1].split("</section>")[0]
        paso3 = src.split('id="spPaso3"')[1].split("</section>")[0]
        assert '<input type="text" id="spComuna"' in paso3 and 'id="spGmapsAviso"' in paso3
        assert "document.getElementById('spComuna').value = cm.long_name" in src
        assert "spGoogleListo()" in src and "No pudimos cargar el buscador de direcciones de Google" in src

    def test_adjuntos_con_miniatura_estado_y_resumen(self):
        src = open(PLANTILLA, encoding="utf-8").read()
        assert 'id="spFilesResumen"' in src and "' de ' + archivos.length + ' listos" in src
        assert "URL.createObjectURL(file)" in src and 'class="th"' in src
        assert "tipo de archivo no permitido" in src and "el máximo es '+MAX_MB+' MB por archivo" in src
        assert "Máximo '+MAX_ADJ+' archivos por solicitud" in src

    def test_sin_popups_nativos(self):
        src = open(PLANTILLA, encoding="utf-8").read()
        js = src.split("<script>")[-1]
        assert not re.search(r"(?<![\w.])(alert|confirm|prompt)\(", js), "REGLA #1: nada de alert/confirm/prompt nativos"

    def test_la_privacidad_dice_que_se_registra_la_ip(self):
        src = open(PLANTILLA, encoding="utf-8").read()
        assert "registramos la dirección IP" in src and "no se comparten con terceros" in src


# ── El archivo para Shopify ─────────────────────────────────────────────────────────────────────────────────────────────────
class TestArchivoShopify:
    def _src(self):
        return open(SHOPIFY, encoding="utf-8").read()

    def test_existe_y_habla_con_produccion_por_los_mismos_endpoints(self):
        src = self._src()
        assert f"var API = '{API_PROD}'" in src
        assert "API + '/soporte/api/config'" in src
        assert "API + '/soporte/api/crear'" in src
        assert "API + '/soporte/api/adjuntos/' + ticketId" in src
        assert "API + '/soporte/api/erp/productos?q='" in src
        assert src.count("credentials: 'omit'") >= 4                            # nunca cookies

    def test_sin_claves_ni_secretos(self):
        """La clave de navegador de Google la pide a /soporte/api/config al cargar: no va fija en el HTML."""
        src = self._src()
        assert not re.search(r"AIza[0-9A-Za-z_-]{20,}", src)
        assert "secret" not in src.lower() and "key=" not in src.replace("key=' + encodeURIComponent(key)", "")
        assert "encodeURIComponent(key)" in src and "c.google_maps_api_key" in src

    def test_direccion_siempre_validada_con_google_y_comuna_sola(self):
        src = self._src()
        assert "direccion_manual" not in src                                  # la tienda ya no manda texto libre
        assert "origen_form: 'tienda'" in src
        assert "google.maps.places.Autocomplete" in src and "libraries=places" in src
        assert "$('itk_comuna').value = dir.comuna" in src and 'id="itk_comuna"' in src and "readonly" in src.split('id="itk_comuna"')[1][:200]
        assert 'id="itk_gmaps_aviso"' in src and "recarga la página para validar tu dirección" in src
        assert "conEquipo() && (!$('itk_dir').value.trim() || !dir.lat)" in src   # sin Google no se envía una opción de equipo
        assert "https://ilusfitness.com/*" in src and "www.ilusfitness.com/*" in src and "PENDIENTE" in src   # paso para TI

    def test_mismo_formato_y_las_9_opciones(self):
        src = self._src()
        assert 'id="ilus-tk"' in src and "#ilus-tk .itk-step" in src and "CÓMO PUBLICARLO" in src
        for t in TIPOS_PUBLICOS:
            assert f"id: '{t}'" in src, f"falta el tipo {t}"
            titulo, linea = tm.TK_TIPOS_PUBLICOS_TEXTO[t]
            assert f"titulo: '{titulo}', linea: '{linea}', equipo: {'false' if t in TIPOS_SIN_EQUIPO else 'true'}" in src
        for t in FUERA_DEL_PUBLICO:
            assert f"id: '{t}'" not in src
        assert "repeat(3,minmax(0,1fr))" in src and "repeat(2,minmax(0,1fr))" in src
        assert "data-equipo-only" in src and "[data-equipo-only]" in src
        assert 'name="hp_ilus_extra"' in src and "ilus_t0: T0" in src
        assert 'id="itk_code"' in src and "numero_ticket" in src              # muestra el N° de ticket al terminar
        paso5 = src.split('id="itk_paso5"')[1].split("</section>")[0]
        assert 'id="itk_doc"' in paso5 and "N° de boleta, factura o pedido" in paso5
        assert 'id="itk_files_resumen"' in src and "URL.createObjectURL(f)" in src and "' de ' + archivos.length + ' listos" in src
        assert "ILUS Fitness" in src and "ILUS Sport" not in src

    def test_sin_popups_nativos(self):
        js = self._src().split("<script>")[-1]
        assert not re.search(r"(?<![\w.])(alert|confirm|prompt)\(", js)


# ── app.py: la ruta sigue exenta de CSRF (es pública y la llama la tienda) ──────────────────────────────────────────────────
def test_soporte_sigue_exento_de_csrf_en_app():
    src = open(os.path.join(_RAIZ, "app.py"), encoding="utf-8").read()
    bloque = src.split("_CSRF_EXEMPT_PREFIXES: tuple = (")[1].split("\n)\n")[0]
    assert '"/soporte",' in bloque
