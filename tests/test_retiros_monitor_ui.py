"""Monitor de Retiros: pantalla (Daniel, 2026-09-29). Corre con:
    py -m unittest tests.test_retiros_monitor_ui -v

· La lógica del navegador (static/retiros_monitor.js) se ejecuta de verdad con Node.
· Las plantillas parciales se renderizan con Jinja y filas reales de retiros_monitor.
· El cableado con pickups_module.py y internal_dashboard.html se revisa sobre el texto.
"""
import json
import os
import re
import shutil
import subprocess
import unittest
from datetime import date, datetime, timedelta

import retiros_monitor as rm

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HAY_NODE = shutil.which("node") is not None


def _leer(*partes):
    with open(os.path.join(RAIZ, *partes), encoding="utf-8") as f:
        return f.read()


def _node(entrada):
    script = r"""
const fs = require('fs'), vm = require('vm');
const ctx = {}; ctx.window = ctx; vm.createContext(ctx);
vm.runInContext(fs.readFileSync(process.argv[1], 'utf8'), ctx);
const L = ctx.RetirosMonitorLogica;
const inp = JSON.parse(fs.readFileSync(0, 'utf8'));
const out = {};
for (const k of Object.keys(inp)) {
  const [fn, args] = [inp[k][0], inp[k][1]];
  out[k] = L[fn].apply(null, args);
}
console.log(JSON.stringify(out));
"""
    r = subprocess.run(["node", "-e", script, os.path.join(RAIZ, "static", "retiros_monitor.js")],
                       input=json.dumps(entrada), capture_output=True, text=True, encoding="utf-8", timeout=60)
    assert r.returncode == 0, r.stderr
    return json.loads(r.stdout)


def F(**kw):
    """Fila tal como la lee el navegador desde los data-*."""
    base = dict(orden=0, id="1", search="", grupo="por_revisar", alerta="verde", resp="", fecha="", dias=None,
                semana=False, vencida=False, req="", conf="", creado=0, cliente="", bultos=0, cal=0, estadoIdx=0)
    base.update(kw)
    return base


@unittest.skipUnless(HAY_NODE, "Node no está instalado: se omiten las pruebas del navegador")
class TestLogicaDelNavegador(unittest.TestCase):
    def test_norm_y_terminos(self):
        r = _node({"a": ["norm", ["  José  Núñez "]], "b": ["terminos", ["José  núñez 123"]], "c": ["terminos", [""]]})
        self.assertEqual(r["a"], "jose  nunez")
        self.assertEqual(r["b"], ["jose", "nunez", "123"])
        self.assertEqual(r["c"], [])

    def test_busqueda_sin_tildes_y_todas_las_palabras(self):
        f = F(search="ret-abc123 jose nunez pena 123456785 56977468766 factura 1234")
        st = {"terms": ["jose", "1234"]}
        r = _node({"ok": ["coincide", [f, st]], "no": ["coincide", [f, {"terms": ["jose", "zzz"]}]]})
        self.assertTrue(r["ok"])
        self.assertFalse(r["no"])

    def test_rut_con_puntos_y_guion_encuentra_el_rut_guardado(self):
        f = F(search="jose 123456785 56977468766")
        r = _node({"rut": ["coincide", [f, {"terms": ["12.345.678-5"]}]],
                   "tel": ["coincide", [f, {"terms": ["+56", "9", "7746", "8766"]}]],
                   "otro": ["coincide", [f, {"terms": ["12.345.678-9"]}]]})
        self.assertTrue(r["rut"])
        self.assertTrue(r["tel"])
        self.assertFalse(r["otro"])

    def test_filtros_de_etapa_semaforo_y_responsable(self):
        f = F(grupo="preparacion", alerta="rojo", resp="samantha rojas")
        r = _node({
            "g_ok": ["coincide", [f, {"grupo": "preparacion"}]], "g_no": ["coincide", [f, {"grupo": "agendada"}]],
            "a_ok": ["coincide", [f, {"alerta": "rojo"}]], "a_no": ["coincide", [f, {"alerta": "verde"}]],
            "r_ok": ["coincide", [f, {"resp": "samantha rojas"}]], "r_no": ["coincide", [f, {"resp": "otra"}]],
            "sin_no": ["coincide", [f, {"resp": "__sin__"}]],
            "sin_ok": ["coincide", [F(resp=""), {"resp": "__sin__"}]],
            "todo": ["coincide", [f, {}]]})
        self.assertEqual([r[k] for k in ("g_ok", "g_no", "a_ok", "a_no", "r_ok", "r_no", "sin_no", "sin_ok", "todo")],
                         [True, False, True, False, True, False, False, True, True])

    def test_filtro_de_fecha_usa_las_mismas_reglas_que_las_tarjetas(self):
        hoy = "2026-09-29"
        # "Hoy" = solicitada o confirmada hoy (tarjeta "Retiros hoy"), aunque la fecha efectiva sea otra
        f_conf = F(req="2026-10-05", conf=hoy, dias=0)
        f_req = F(req=hoy, conf="", dias=0)
        f_prop = F(req="2026-10-05", conf="", dias=0)     # solo propuesta para hoy: NO cuenta en la tarjeta
        f_man = F(dias=1, fecha="2026-09-30")
        r = _node({
            "conf": ["coincide", [f_conf, {"fecha": "hoy", "hoy": hoy}]], "req": ["coincide", [f_req, {"fecha": "hoy", "hoy": hoy}]],
            "prop": ["coincide", [f_prop, {"fecha": "hoy", "hoy": hoy}]],
            "man_ok": ["coincide", [f_man, {"fecha": "manana", "hoy": hoy}]], "man_no": ["coincide", [f_req, {"fecha": "manana", "hoy": hoy}]],
            "sem_ok": ["coincide", [F(semana=True), {"fecha": "semana"}]], "sem_no": ["coincide", [F(semana=False), {"fecha": "semana"}]],
            "ven_ok": ["coincide", [F(vencida=True), {"fecha": "vencidas"}]], "ven_no": ["coincide", [F(), {"fecha": "vencidas"}]],
            "sf_ok": ["coincide", [F(fecha=""), {"fecha": "sin_fecha"}]], "sf_no": ["coincide", [F(fecha="2026-10-01"), {"fecha": "sin_fecha"}]]})
        self.assertEqual([r[k] for k in ("conf", "req", "prop", "man_ok", "man_no", "sem_ok", "sem_no", "ven_ok", "ven_no", "sf_ok", "sf_no")],
                         [True, True, False, True, False, True, False, True, False, True, False])

    def test_orden_alfabetico_en_espanol_y_descendente(self):
        filas = [F(orden=0, id="a", cliente="zapata"), F(orden=1, id="b", cliente="ñandú"), F(orden=2, id="c", cliente="álvarez"),
                 F(orden=3, id="d", cliente="nuñez")]
        r = _node({"asc": ["ordenar", [filas, "cliente", "asc"]], "desc": ["ordenar", [filas, "cliente", "desc"]]})
        self.assertEqual([x["id"] for x in r["asc"]], ["c", "d", "b", "a"])     # álvarez, nuñez, ñandú, zapata
        self.assertEqual([x["id"] for x in r["desc"]], ["a", "b", "d", "c"])

    def test_orden_numerico_y_sin_dato_siempre_al_final(self):
        filas = [F(orden=0, id="a", fecha="2026-10-03"), F(orden=1, id="b", fecha=""), F(orden=2, id="c", fecha="2026-10-01")]
        r = _node({"asc": ["ordenar", [filas, "fecha", "asc"]], "desc": ["ordenar", [filas, "fecha", "desc"]]})
        self.assertEqual([x["id"] for x in r["asc"]], ["c", "a", "b"])
        self.assertEqual([x["id"] for x in r["desc"]], ["a", "c", "b"])
        num = [F(orden=0, id="a", bultos=2), F(orden=1, id="b", bultos=10), F(orden=2, id="c", bultos=2)]
        r = _node({"d": ["ordenar", [num, "carga", "desc"]]})
        self.assertEqual([x["id"] for x in r["d"]], ["b", "a", "c"])         # empate: respeta el orden del servidor

    def test_clave_desconocida_no_reordena(self):
        filas = [F(orden=0, id="a"), F(orden=1, id="b")]
        self.assertEqual([x["id"] for x in _node({"o": ["ordenar", [filas, "nada", "asc"]]})["o"]], ["a", "b"])

    def test_paginacion(self):
        r = _node({
            "p1": ["paginar", [25, 1, 10]], "p3": ["paginar", [25, 3, 10]], "fuera": ["paginar", [25, 99, 10]],
            "cero": ["paginar", [0, 1, 10]], "menos": ["paginar", [5, -4, 10]], "uno": ["paginar", [1, 1, 10]]})
        self.assertEqual((r["p1"]["desde"], r["p1"]["hasta"], r["p1"]["paginas"]), (1, 10, 3))
        self.assertEqual((r["p3"]["desde"], r["p3"]["hasta"], r["p3"]["pagina"]), (21, 25, 3))
        self.assertEqual(r["fuera"]["pagina"], 3)
        self.assertEqual((r["cero"]["desde"], r["cero"]["hasta"], r["cero"]["paginas"]), (0, 0, 1))
        self.assertEqual(r["menos"]["pagina"], 1)
        self.assertEqual((r["uno"]["desde"], r["uno"]["hasta"]), (1, 1))

    def test_contadores_de_los_chips_ignoran_su_propio_filtro(self):
        filas = [F(id="1", grupo="por_revisar", alerta="rojo"), F(id="2", grupo="por_revisar", alerta="verde"),
                 F(id="3", grupo="preparacion", alerta="verde"), F(id="4", grupo="preparacion", alerta="rojo")]
        st = {"grupo": "preparacion", "alerta": "rojo"}
        r = _node({"g": ["conteos", [filas, st, "grupo"]], "a": ["conteos", [filas, st, "alerta"]]})
        # con el semáforo "rojo" puesto, cada etapa cuenta solo sus rojos
        self.assertEqual((r["g"]["__total__"], r["g"].get("por_revisar"), r["g"].get("preparacion")), (2, 1, 1))
        # con la etapa "preparacion" puesta, cada semáforo cuenta solo esa etapa
        self.assertEqual((r["a"]["__total__"], r["a"].get("rojo"), r["a"].get("verde")), (2, 1, 1))

    def test_resaltado_ignora_tildes_y_mayusculas(self):
        r = _node({"s": ["segmentar", ["José Núñez Peña", ["jose", "nunez"]]], "vacio": ["segmentar", ["Hola", []]],
                   "nada": ["segmentar", ["Hola", ["zzz"]]], "doble": ["segmentar", ["ana ana", ["ana"]]]})
        self.assertEqual(r["s"], [{"t": "José", "m": True}, {"t": " ", "m": False}, {"t": "Núñez", "m": True},
                                  {"t": " Peña", "m": False}])
        self.assertEqual(r["vacio"], [{"t": "Hola", "m": False}])
        self.assertEqual(r["nada"], [{"t": "Hola", "m": False}])
        self.assertEqual([x["m"] for x in r["doble"]], [True, False, True])
        # los tramos reconstruyen el texto original
        self.assertEqual("".join(x["t"] for x in r["s"]), "José Núñez Peña")

    def test_csv_para_excel_en_espanol(self):
        filas = [{"Solicitud": "RET-1", "Cliente": 'Ferretería "El Clavo"; Ltda', "Teléfono": "+56 9 7746 8766", "Peso": 12.5},
                 {"Solicitud": "RET-2", "Cliente": "=HYPERLINK(\"http://x\")", "Teléfono": "@cmd", "Peso": ""}]
        csv = _node({"c": ["aCsv", [filas]], "vacio": ["aCsv", [[]]]})
        texto = csv["c"]
        self.assertTrue(texto.startswith("﻿"))
        lineas = texto.lstrip("﻿").split("\r\n")
        self.assertEqual(lineas[0], "Solicitud;Cliente;Teléfono;Peso")
        self.assertEqual(lineas[1], 'RET-1;"Ferretería ""El Clavo""; Ltda";56 9 7746 8766;12.5')
        self.assertTrue(lineas[2].startswith("RET-2;\"'=HYPERLINK"))     # una fórmula no se ejecuta
        self.assertIn(";'@cmd;", lineas[2])
        self.assertEqual(csv["vacio"], "")


# ── Plantillas ────────────────────────────────────────────────────────────────

def _entorno():
    import jinja2
    import pickups_module as pm
    env = jinja2.Environment(
        loader=jinja2.DictLoader({n: _leer("templates", "retiros", n) for n in ("_monitor_kpis.html", "_monitor_tabla.html")}),
        autoescape=True)

    def rut_fmt(v):
        c = re.sub(r"[^0-9kK]", "", str(v or "")).upper()
        return re.sub(r"\B(?=(\d{3})+(?!\d))", ".", c[:-1]) + "-" + c[-1] if len(c) > 1 else c

    env.filters["rut_fmt"] = rut_fmt
    env.globals.update(
        url_for=lambda ep, **kw: "/" + ep + "/" + str(kw.get("rid") or kw.get("token") or kw.get("filename") or ""),
        status_badge=lambda s: {"label": pm.PICKUP_STATUS.get(s, s), "color": pm.PICKUP_STATUS_COLORS.get(s, "secondary")},
        current_user={"role": "superadmin"}, pipeline_groups=pm.PIPELINE_GROUPS)
    return env, pm, rut_fmt


HOY = date(2026, 9, 29)
AHORA = datetime(2026, 9, 29, 15, 0)


def _filas(n=12, **extra):
    import pickups_module as pm
    filas = []
    for i in range(n):
        f = dict(id=i + 1, code=f"RET-{i:03d}", status="solicitud_recibida", customer_name=f"Cliente {i}",
                 customer_rut="12.345.678-5", contact_name="María Pérez", contact_phone="+56 9 7746 8766",
                 contact_email="m@x.cl", document_type="factura", document_number=str(1000 + i),
                 pickup_person_name="Pedro Soto", pickup_person_rut="9876543-3", pickup_person_phone="",
                 pickup_person_relation="chofer", requested_date=date(2026, 10, 1),
                 requested_time_from=timedelta(hours=9), requested_time_to=timedelta(hours=9, minutes=30),
                 proposed_date=None, confirmed_date=None, total_packages=1, total_weight_kg=0,
                 total_volumetric_weight=0, total_volume_m3=0, information_quality_score=79, request_source="web",
                 responsable_nombre="", doc_validation_status="pendiente", created_by_user_name=None,
                 public_token="tok%03d" % i, created_at=datetime(2026, 9, 29, 18, 0), tiempo_estimado_min=None)
        f.update(extra)
        filas.append(f)
    return filas


def _enriquecer(filas):
    import pickups_module as pm
    rm.enriquecer_filas(
        filas, hoy=HOY, ahora=AHORA, horas_habiles=lambda d, h, fer: (h - d).total_seconds() / 3600.0,
        utc_a_chile=lambda dt: (dt - timedelta(hours=3)) if dt else None,
        td_hhmm=lambda t: f"{int(t.total_seconds()) // 3600:02d}:{(int(t.total_seconds()) % 3600) // 60:02d}",
        estados=pm.PICKUP_STATUS, grupos=pm.PIPELINE_GROUPS, relaciones=dict(pm.PICKUP_RELATIONS))
    return filas


class TestPlantillaTarjetas(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            cls.env, cls.pm, _rut = _entorno()
            cls.rut = staticmethod(_rut)
        except Exception as e:  # jinja2/flask ausentes
            raise unittest.SkipTest(f"no se pudo armar el entorno de plantillas: {e}")

    def _render(self, vista="monitor", ok=True, kx=None):
        lunes, _ = rm.semana(HOY)
        kx = kx if kx is not None else {
            "semana": {"barras": rm.barras_semana({"2026-09-29": 2}, lunes, HOY), "delta": rm.delta(3, 1)},
            "tasa": {"spark": rm.spark([50, 60, 80]), "delta": rm.delta(80, 72, unidad=" pts")},
            "ciclo": {"spark": rm.spark([30, 25, 20]), "delta": rm.delta(20.0, 26.0, menor_es_mejor=True, unidad=" h", decimales=1)},
            "revisar": {"txt": "5 h hábiles", "nivel": "rojo"}}
        return self.env.get_template("_monitor_kpis.html").render(
            kpis={"semana": 3, "tasa_conf": 80, "ciclo_h": 20.0, "msgs": 4}, stats={"en_preparacion": 2, "solicitud_recibida": 3},
            day={"total": 1, "bultos": 2, "peso": 12.5, "pvol": 8.0, "m3": 0.25}, filtros={"view": vista},
            mon={"ok": ok}, kx=kx)

    def test_siguen_los_diez_numeros_de_siempre(self):
        html = self._render()
        for etiqueta in ("Retiros esta semana", "Tasa confirmación 30d", "Ciclo promedio 90d", "En preparación",
                         "Por revisar", "Mensajes sin leer", "Retiros hoy", "Bultos hoy", "Peso real", "Volumen"):
            self.assertIn(etiqueta, html)
        self.assertIn('id="rkKpiMsgs">4<', html)          # el JS de burbujas lo actualiza en vivo
        self.assertIn("12.5", html)
        self.assertIn("0.250", html)

    def test_variacion_con_color_segun_si_es_bueno(self):
        html = self._render()
        self.assertIn("rm-delta bueno", html)
        self.assertIn("vs. semana anterior", html)
        malo = self._render(kx={"ciclo": {"spark": None, "delta": rm.delta(30.0, 26.0, menor_es_mejor=True, unidad=" h", decimales=1)}})
        self.assertIn("rm-delta malo", malo)

    def test_barras_de_la_semana_son_siete(self):
        html = self._render()
        self.assertEqual(len(re.findall(r"<rect ", html)), 7)
        self.assertIn('class="hoy"', html)
        self.assertIn("mar 29-09: 2 retiros", html)

    def test_sin_datos_de_comparacion_no_revienta(self):
        html = self._render(kx={})
        self.assertIn("Aún no hay período anterior para comparar", html)
        self.assertNotIn("<svg class=\"rm-bars\"", html)

    def test_por_revisar_avisa_la_solicitud_mas_antigua(self):
        self.assertIn("La más antigua espera 5 h hábiles", self._render())
        self.assertIn("Nada esperando respuesta", self._render(kx={}))

    def test_clic_para_filtrar_solo_en_la_vista_monitor(self):
        self.assertEqual(self._render().count("data-rm-filter="), 4)
        for vista in ("kanban", "agenda"):
            self.assertNotIn("data-rm-filter", self._render(vista=vista))
        self.assertNotIn("data-rm-filter", self._render(ok=False))


class TestPlantillaTabla(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            cls.env, cls.pm, _rut = _entorno()
            cls.rut = staticmethod(_rut)
        except Exception as e:
            raise unittest.SkipTest(f"no se pudo armar el entorno de plantillas: {e}")

    def _render(self, filas, total=None, **extra):
        _enriquecer(filas)
        mon = {"hoy": "2026-09-29", "lunes": "2026-09-28", "domingo": "2026-10-04", "limite": 250,
               "total": len(filas) if total is None else total, "ok": True,
               "datos": rm.armar_datos(filas, self.rut)}
        return self.env.get_template("_monitor_tabla.html").render(rows=filas, mon=mon, **extra)

    def test_una_fila_por_retiro_y_solo_diez_visibles_al_cargar(self):
        html = self._render(_filas(12))
        filas = re.findall(r'<tr class="rm-row[^>]*>', html)
        self.assertEqual(len(filas), 12)
        self.assertEqual(sum(1 for t in filas if " hidden" in t), 2)
        self.assertNotIn(" hidden", filas[9])
        self.assertIn(" hidden", filas[10])

    def test_cada_fila_trae_lo_que_el_navegador_necesita(self):
        html = self._render(_filas(1))
        for attr in ("data-rid", "data-href", "data-pub", "data-grupo", "data-estado-idx", "data-alerta", "data-resp",
                     "data-cliente", "data-fecha", "data-dias", "data-semana", "data-vencida", "data-req", "data-conf",
                     "data-creado", "data-bultos", "data-cal", "data-search", "data-code"):
            self.assertIn(attr + "=", html, attr)

    def test_la_busqueda_de_daniel_esta_a_la_vista(self):
        html = self._render(_filas(1))
        self.assertIn('id="rmSearch"', html)
        self.assertIn(">Buscar<", html)
        self.assertIn("quién retira", html)

    def test_columnas_nuevas_y_las_de_siempre(self):
        html = self._render(_filas(1))
        for col in ("Solicitud", "Cliente", "Documento", "Quién retira", "Responsable", "Fecha de retiro", "Estado",
                    "Calidad", "Carga", "Acción"):
            self.assertIn(col, html)
        # El menú ⋮ se conserva, pero se arma en el navegador al abrirlo (no viaja repetido en cada fila)
        self.assertIn('<ul class="dropdown-menu dropdown-menu-end shadow-sm"></ul>', html)
        self.assertNotIn("Ver ficha", html)
        js = _leer("static", "retiros_monitor.js")
        for opcion in ("Ver ficha", "Link cliente", "Reagendar / Proponer otra fecha", "Eliminar (superadmin)", "llenarMenu"):
            self.assertIn(opcion, js)
        for evento in ("show.bs.dropdown", "pointerdown", "focusin"):
            self.assertIn(evento, js)

    def test_eliminar_solo_para_superadmin(self):
        self.assertIn('data-super="1"', self._render(_filas(1)))
        self.assertIn('data-super="0"', self._render(_filas(1), current_user={"role": "tecnico"}))
        self.assertIn('data-super="0"', self._render(_filas(1), current_user=None))

    def test_el_html_por_fila_se_mantiene_liviano(self):
        html = self._render(_filas(40))
        filas = re.findall(r'<tr class="rm-row.*?</tr>', html, re.S)
        promedio = sum(len(f) for f in filas) // len(filas)
        self.assertLess(promedio, 3700, f"cada fila pesa {promedio} bytes: el detalle y el menú no deben repetirse en el HTML")
        datos = re.search(r'id="rmData">(.*?)</script>', html, re.S).group(1)
        self.assertLess(len(datos) // 40, 1100)

    def test_datos_del_cliente_se_muestran_completos_y_con_formato(self):
        html = self._render(_filas(1))
        self.assertIn("RUT 12.345.678-5", html)
        self.assertIn("9.876.543-3", html)
        self.assertIn("jue 01-10-2026", html)
        self.assertIn("09:00–09:30", html)
        self.assertIn("1 bulto<", html)
        self.assertIn("Peso por confirmar", html)
        self.assertIn('href="tel:+56977468766"', html)

    def test_lo_que_viene_del_formulario_publico_no_inyecta_html(self):
        malo = '<img src=x onerror=alert(1)>'
        html = self._render(_filas(1, customer_name=malo, contact_name="</script><script>alert(2)</script>"))
        self.assertNotIn("<img src=x", html)
        self.assertNotIn("<script>alert(2)", html)
        self.assertIn("&lt;img src=x onerror=alert(1)&gt;", html)
        # el JSON de datos no puede cerrar su propio <script>
        bloque = re.search(r'<script type="application/json" id="rmData">(.*?)</script>', html, re.S).group(1)
        self.assertNotIn("</script", bloque)
        self.assertIn("1", json.loads(bloque))

    def test_json_de_datos_tiene_una_entrada_por_retiro(self):
        html = self._render(_filas(12))
        datos = json.loads(re.search(r'id="rmData">(.*?)</script>', html, re.S).group(1))
        self.assertEqual(sorted(datos, key=int), [str(i) for i in range(1, 13)])
        self.assertEqual(datos["1"]["csv"]["Solicitud"], "RET-000")

    def test_reagendar_solo_en_retiros_activos(self):
        activo = self._render(_filas(1))
        self.assertIn("data-reag=", activo)
        terminado = self._render(_filas(1, status="retirada"))
        self.assertNotIn("data-reag=", terminado)

    def test_semaforo_de_la_fila(self):
        rojo = self._render(_filas(1, created_at=datetime(2026, 9, 29, 9, 0)))   # 06:00 Chile → 9 h sin responder
        self.assertIn('data-alerta="rojo"', rojo)
        self.assertIn("Sin responder · 9 h hábiles", rojo)

    def test_calidad_de_retiro_interno_no_se_pinta_como_cero(self):
        interno = self._render(_filas(1, request_source="backoffice", information_quality_score=0))
        self.assertIn("Los retiros internos no se puntúan", interno)
        self.assertNotIn(">0%<", interno)

    def test_responsables_del_filtro_sin_repetir(self):
        filas = _filas(3)
        filas[0]["responsable_nombre"] = filas[1]["responsable_nombre"] = "Samantha Rojas"
        filas[2]["responsable_nombre"] = "Milagros Díaz"
        html = self._render(filas)
        sel = re.search(r'<select id="rmResp".*?</select>', html, re.S).group(0)
        self.assertEqual(sel.count("<option"), 4)                    # todos + sin asignar + 2 personas
        self.assertIn('value="samantha rojas"', sel)

    def test_avisa_cuando_el_tope_de_250_recorta(self):
        self.assertIn("250 solicitudes más recientes de las 400", self._render(_filas(2), total=400))
        self.assertNotIn("rm-limit\"", self._render(_filas(2), total=2))

    def test_pie_de_tabla_como_etiquetas(self):
        html = self._render(_filas(1))
        for pieza in ('id="rmFootCount"', 'id="rmPer"', 'id="rmPrev"', 'id="rmNext"', 'id="rmPageInfo"'):
            self.assertIn(pieza, html)

    def test_sin_filas_muestra_el_mensaje_de_siempre(self):
        self.assertIn("Sin solicitudes de retiro.", self._render([]))

    def test_estado_con_contraste_legible(self):
        html = self._render(_filas(1, status="propuesta_enviada",
                                   proposed_date=date(2026, 10, 2)))   # 'warning' (amarillo)
        self.assertIn("text-dark", re.search(r'<span class="badge[^>]*rm-estado[^>]*>', html).group(0))


class TestCableado(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.pm = _leer("pickups_module.py")
        cls.tpl = _leer("templates", "retiros", "internal_dashboard.html")

    def test_el_dashboard_calcula_y_entrega_los_datos_del_monitor(self):
        self.assertIn("import retiros_monitor as _rmon", self.pm)
        self.assertIn("_rmon.enriquecer_filas(", self.pm)
        self.assertIn("_rmon.armar_datos(", self.pm)
        self.assertIn("mon=mon, kx=kx,", self.pm)
        # variables que ya usaban las otras vistas: siguen entregándose
        for var in ("rows=rows", "filtros=filtros", "stats=stats", "day=day", "kpis=kpis", "pipeline_groups=PIPELINE_GROUPS"):
            self.assertIn(var, self.pm)

    def test_hoy_ya_no_depende_de_la_hora_del_servidor(self):
        self.assertIn("today = _now_chile().date().isoformat()", self.pm)
        self.assertNotIn("today = datetime.now().date().isoformat()", self.pm)

    def test_si_falla_el_enriquecido_la_tabla_de_siempre_sigue(self):
        self.assertIn('mon["ok"] = True', self.pm)
        self.assertIn("{% if filtros.view != 'lista' and mon is defined and mon.ok %}", self.tpl)
        self.assertEqual(self.tpl.count('{% include "retiros/_monitor_tabla.html" %}'), 1)
        self.assertEqual(self.tpl.count('{% include "retiros/_monitor_kpis.html" %}'), 1)
        self.assertIn("Solicitud</th><th>Cliente</th><th>Documento</th>", self.tpl)   # la tabla clásica sigue ahí

    def test_no_se_pierde_el_script_de_clic_de_fila_ni_eliminar(self):
        self.assertIn("eliminarSolicitudDesdeMonitor", self.tpl)
        self.assertIn("tr.row-clickable", self.tpl)
        self.assertIn("refreshBubbles", self.tpl)

    def test_jinja_del_dashboard_es_valido(self):
        import jinja2
        jinja2.Environment().parse(self.tpl)
        for n in ("_monitor_kpis.html", "_monitor_tabla.html"):
            jinja2.Environment().parse(_leer("templates", "retiros", n))

    def test_js_de_la_tabla_es_valido(self):
        if not HAY_NODE:
            self.skipTest("Node no está instalado")
        r = subprocess.run(["node", "--check", os.path.join(RAIZ, "static", "retiros_monitor.js")],
                           capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stderr)


if __name__ == "__main__":
    unittest.main()
