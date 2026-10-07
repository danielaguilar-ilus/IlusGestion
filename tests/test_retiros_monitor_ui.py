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
                semana=False, vencida=False, req="", conf="", creado=0, cliente="", bultos=0, cal=0, estadoIdx=0,
                tiempo=None)
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

    def test_por_defecto_el_monitor_no_muestra_las_ya_retiradas(self):
        # Daniel 2026-10-06: «deja por defecto el filtro excluyendo las ya retiradas».
        ret, act = F(grupo="retirada"), F(grupo="preparacion")
        r = _node({
            "ret_oculta": ["coincide", [ret, {"grupo": "__activos__"}]], "act_visible": ["coincide", [act, {"grupo": "__activos__"}]],
            "todos": ["coincide", [ret, {"grupo": ""}]],                                  # «Todos» las incluye
            "solo_ret": ["coincide", [ret, {"grupo": "retirada"}]],                       # el chip «Retirada» sigue mostrándolas
            "solo_ret_no": ["coincide", [act, {"grupo": "retirada"}]],
            "cancel": ["coincide", [F(grupo="canceladas"), {"grupo": "__activos__"}]],   # cerradas/rechazadas no son «retiradas»
            "con_busqueda": ["coincide", [ret, {"grupo": "__activos__", "terms": ["x"]}]]})
        self.assertEqual([r[k] for k in ("ret_oculta", "act_visible", "todos", "solo_ret", "solo_ret_no", "cancel", "con_busqueda")],
                         [False, True, True, True, False, True, False])

    def test_contadores_con_activos_son_coherentes_con_las_filas(self):
        filas = [F(id="1", grupo="por_revisar"), F(id="2", grupo="retirada"), F(id="3", grupo="retirada"), F(id="4", grupo="preparacion")]
        st = {"grupo": "__activos__"}
        r = _node({"g": ["conteos", [filas, st, "grupo"]]})
        # «Todos» = 4; «Retirada» = 2; «Activos» = Todos - Retirada = las filas que se ven
        self.assertEqual((r["g"]["__total__"], r["g"].get("retirada")), (4, 2))
        visibles = [f for f in filas if _node({"v": ["coincide", [f, st]]})["v"]]
        self.assertEqual(len(visibles), r["g"]["__total__"] - r["g"]["retirada"])

    def test_reloj_de_respuesta_en_el_navegador_mismas_reglas_que_el_servidor(self):
        V = [[480, 780], [840, 1020]]
        r = _node({
            "cob": ["parseVentanas", ["480-780,840-1020"]], "rota": ["parseVentanas", ["x,y"]],
            "abierto": ["enCobertura", [630, "Tue", True, V]],
            "colacion": ["enCobertura", [800, "Tue", True, V]],
            "noche": ["enCobertura", [1300, "Tue", True, V]],
            "sabado": ["enCobertura", [630, "Sat", True, V]],
            "feriado": ["enCobertura", [630, "Mon", False, V]],
            "a1": ["proximaApertura", [1300, "Tue", True, V]],
            "a2": ["proximaApertura", [1100, "Fri", True, V]],
            "a3": ["proximaApertura", [800, "Wed", True, V]],
            "a4": ["proximaApertura", [300, "Wed", True, V]]})
        self.assertEqual(r["cob"], V)
        self.assertEqual(r["rota"], V)                                    # sin los tramos del servidor: el horario confirmado
        self.assertEqual([r[k] for k in ("abierto", "colacion", "noche", "sabado", "feriado")], [True, False, False, False, False])
        self.assertEqual([r[k] for k in ("a1", "a2", "a3", "a4")], ["mañana 08:00", "lun 08:00", "hoy 14:00", "hoy 08:00"])

    def test_reloj_humano_sin_contador_en_cero_ni_hh_mm_ss(self):
        r = _node({
            "d0": ["durHumana", [0, False]], "d45": ["durHumana", [2700, False]], "viva": ["durHumana", [725, True]],
            "h": ["durHumana", [4800, False]], "h1": ["durHumana", [3600, False]], "h9": ["durHumana", [9 * 3600, False]],
            "hviva": ["durHumana", [4800, True]],                                       # desde 1 h ya no hay segundos
            "noche": ["textoReloj", [{"espera": 0, "rojo": 14400, "abierta": False, "llego": "22:41", "reanuda": "mañana 08:00", "vence": "mañana 12:00"}]],
            "pausa": ["textoReloj", [{"espera": 4800, "rojo": 14400, "abierta": False, "llego": "10:00", "reanuda": "mañana 08:00", "vence": "mañana 10:40"}]],
            "corre": ["textoReloj", [{"espera": 125, "rojo": 14400, "abierta": True, "llego": "10:00", "reanuda": "", "vence": "hoy 15:00"}]],
            "vencido": ["textoReloj", [{"espera": 18000, "rojo": 14400, "abierta": True, "llego": "07:00", "reanuda": "", "vence": "hoy 12:00"}]]})
        self.assertEqual((r["d0"], r["d45"], r["viva"]), ("menos de 1 min", "45 min hábiles", "12:05 min"))   # mm:ss solo si es menos de 1 h y corre
        self.assertEqual((r["h"], r["h1"], r["h9"], r["hviva"]), ("1 h 20 min hábiles", "1 h hábil", "9 h hábiles", "1 h 20 min hábiles"))
        # llegó de noche: dice qué pasa, jamás «00:00:00», sin barra (pausa0)
        self.assertEqual(r["noche"]["pill"], "Llegó 22:41 · el reloj parte mañana 08:00")
        self.assertEqual(r["noche"]["sub"], "Plazo 4 h hábiles · vence mañana 12:00")
        self.assertTrue(r["noche"]["pausa0"])
        self.assertEqual(r["pausa"]["pill"], "Sin responder · 1 h 20 min hábiles")
        self.assertEqual(r["pausa"]["tag"], "hasta mañana 08:00")
        self.assertEqual(r["pausa"]["sub"], "Quedan 2 h 40 min hábiles · vence mañana 10:40")
        self.assertEqual(r["corre"]["pill"], "Sin responder · 02:05 min")
        self.assertEqual(r["corre"]["tag"], "")
        self.assertEqual(r["vencido"]["sub"], "Plazo vencido (vencía hoy 12:00): responder ya")
        for k in ("noche", "pausa", "corre", "vencido"):
            self.assertNotRegex(" ".join(str(v) for v in r[k].values()), r"\d\d:\d\d:\d\d")

    def test_servidor_y_navegador_dicen_lo_mismo(self):
        import retiros_monitor as m
        casos = [(0, False), (4800, False), (4800, True), (18000, True), (30, False), (2700, False)]
        entradas = {f"c{i}": ["textoReloj", [{"espera": e, "rojo": 14400, "abierta": a, "llego": "10:00", "reanuda": "mañana 08:00", "vence": "hoy 15:00"}]]
                    for i, (e, a) in enumerate(casos)}
        js = _node(entradas)
        for i, (e, a) in enumerate(casos):
            py = m.textos_reloj(e, 14400, a, "10:00", "mañana 08:00", "hoy 15:00")
            if a and e < 3600 and e >= 60:
                continue          # corriendo y con menos de 1 h el navegador muestra mm:ss en vivo (el servidor, minutos)
            self.assertEqual(py["pill"], js[f"c{i}"]["pill"], casos[i])
            self.assertEqual(py["sub"], js[f"c{i}"]["sub"], casos[i])
            self.assertEqual(py["tag"], js[f"c{i}"]["tag"], casos[i])
            self.assertEqual(py["pausa0"], js[f"c{i}"]["pausa0"], casos[i])

    def test_orden_por_tiempo_lo_mas_urgente_primero_y_sin_dato_al_final(self):
        filas = [F(orden=0, id="gris", tiempo=0), F(orden=1, id="rojo", tiempo=30000000 + 900), F(orden=2, id="verde", tiempo=10000000),
                 F(orden=3, id="sin", tiempo=None), F(orden=4, id="ambar", tiempo=20000000)]
        r = _node({"d": ["ordenar", [filas, "tiempo", "desc"]], "a": ["ordenar", [filas, "tiempo", "asc"]]})
        self.assertEqual([x["id"] for x in r["d"]], ["rojo", "ambar", "verde", "gris", "sin"])
        self.assertEqual([x["id"] for x in r["a"]], ["gris", "verde", "ambar", "rojo", "sin"])

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
        filas, hoy=HOY, ahora=AHORA, utc_a_chile=lambda dt: (dt - timedelta(hours=3)) if dt else None,
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
                    "Tiempo", "Calidad", "Carga", "Acción"):
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
        # 2026-09-29: +300 por el reloj en vivo de "Sin responder" (aquí TODAS las filas lo llevan: peor caso).
        # 2026-10-06: +500 por la columna «Tiempo» y las piezas de carga (cada dato en su propio elemento, sin truncar).
        self.assertLess(promedio, 4500, f"cada fila pesa {promedio} bytes: el detalle y el menú no deben repetirse en el HTML")
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
        self.assertIn("Sin responder · 6 h hábiles", rojo)       # horario de cobertura: 08-13 + 14-15 (la colación no corre)

    def test_llego_de_noche_la_celda_dice_cuando_parte_el_reloj(self):
        # Daniel 2026-10-06 (captura de producción de noche): «Sin responder · 00:00:00 · en pausa» en verde con la barra vacía.
        import pickups_module  # noqa: F401  (entorno de plantillas)
        ahora = datetime(2026, 9, 29, 22, 50)
        filas = _filas(1, created_at=datetime(2026, 9, 30, 1, 41))
        rm.enriquecer_filas(
            filas, hoy=HOY, ahora=ahora, horas_habiles=None, utc_a_chile=lambda dt: (dt - timedelta(hours=3)) if dt else None,
            td_hhmm=lambda t: f"{int(t.total_seconds()) // 3600:02d}:{(int(t.total_seconds()) % 3600) // 60:02d}",
            estados=self.pm.PICKUP_STATUS, grupos=self.pm.PIPELINE_GROUPS, relaciones=dict(self.pm.PICKUP_RELATIONS),
            cobertura={"desde": 480, "hasta": 1020, "col_d": 780, "col_h": 840})
        mon = {"hoy": "2026-09-29", "lunes": "2026-09-28", "domingo": "2026-10-04", "limite": 250, "total": 1, "ok": True,
               "datos": rm.armar_datos(filas, self.rut)}
        html = self.env.get_template("_monitor_tabla.html").render(rows=filas, mon=mon)
        celda = re.search(r'<td class="rm-c-tie".*?</td>', html, re.S).group(0)
        self.assertIn("Llegó 22:41 · el reloj parte mañana 08:00", celda)
        self.assertIn("Plazo 4 h hábiles · vence mañana 12:00", celda)
        self.assertIn("pausa0", celda)                                         # el CSS esconde la barra vacía
        self.assertIn('data-abierta="0"', celda)
        self.assertIn('data-reanuda="mañana 08:00"', celda)
        self.assertNotIn("00:00:00", celda)
        self.assertIn('class="rm-pill verde"', celda)                          # nadie está atrasado: el reloj ni partió
        css = _leer("static", "retiros_monitor.css")
        self.assertIn(".rm-reloj.pausa0 .rm-reloj-fila", css)

    def test_los_tramos_de_cobertura_del_servidor_viajan_una_vez_en_la_tabla(self):
        # El coordinador entrega mon["ventanas"] = [(480, 780), (840, 1020)]: va en la tabla y no se repite por fila.
        filas = _filas(3)
        _enriquecer(filas)
        mon = {"hoy": "2026-09-29", "lunes": "2026-09-28", "domingo": "2026-10-04", "limite": 250, "total": 3, "ok": True,
               "datos": rm.armar_datos(filas, self.rut), "ventanas": [(480, 780), (840, 1020)]}
        html = self.env.get_template("_monitor_tabla.html").render(rows=filas, mon=mon)
        self.assertIn('data-ventanas="480-780,840-1020"', re.search(r'<table[^>]*>', html, re.S).group(0))
        self.assertEqual(html.count("data-ventanas="), 1)
        sin = self._render(_filas(3))                                              # sin mon.ventanas: cada reloj trae los suyos
        self.assertEqual(sin.count("data-ventanas="), 3)

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

    def test_columna_tiempo_trae_el_reloj_y_estado_queda_liviano(self):
        # Daniel 2026-10-06: la fila medía ~185 px porque «Estado» amontonaba todo; el reloj ahora vive en «Tiempo».
        html = self._render(_filas(1))
        fila = re.search(r'<tr class="rm-row.*?</tr>', html, re.S).group(0)
        estado = re.search(r'<td data-label="Estado">.*?</td>', fila, re.S).group(0)
        tiempo = re.search(r'<td class="rm-c-tie" data-label="Tiempo">.*?</td>', fila, re.S).group(0)
        self.assertIn("rm-estado", estado)
        self.assertIn("Siguiente: proponer fecha al cliente", estado)
        self.assertNotIn("rm-reloj", estado)
        for pieza in ("rm-reloj ", 'data-espera="', 'data-ventanas="480-780,840-1020"', "rm-reloj-t", "rm-reloj-barra", "rm-reloj-plazo",
                      "rm-reloj-pausa", "Quedan 4 h hábiles · vence"):
            self.assertIn(pieza, tiempo)
        self.assertIn('data-sort="tiempo"', html)
        self.assertIn('data-tiempo="', html)
        self.assertIn('data-rm-col="tiempo"', html)       # el selector «Columnas» lo puede ocultar
        self.assertEqual(html.count('colspan="12"'), 1)    # con la columna nueva ahora son 12
        self.assertNotIn('colspan="11"', html)
        # un retiro ya retirado no tiene reloj: la columna dice solo su semáforo
        ret = self._render(_filas(1, status="retirada"))
        celda = re.search(r'<td class="rm-c-tie".*?</td>', ret, re.S).group(0)
        self.assertNotIn("rm-reloj", celda)
        self.assertIn("Retirado", celda)

    def test_quien_retira_dice_el_mismo_cliente_una_sola_vez(self):
        mismo = self._render(_filas(1, pickup_person_name="Cliente 0", pickup_person_rut=""))
        celda = re.search(r'<td class="desktop-only rm-c-ret".*?</td>', mismo, re.S).group(0)
        self.assertIn("El mismo cliente", celda)
        self.assertIn("Chofer", celda)                     # la relación se conserva
        otro = self._render(_filas(1))
        celda = re.search(r'<td class="desktop-only rm-c-ret".*?</td>', otro, re.S).group(0)
        self.assertNotIn("El mismo cliente", celda)
        self.assertIn("Pedro Soto", celda)
        self.assertIn("9.876.543-3", celda)

    def test_carga_de_un_retiro_completado_no_muestra_cero_kg(self):
        # Daniel 2026-10-06: «0.0 kg · PV 0.0» parecía un dato real.
        cero = self._render(_filas(1, status="retirada"))
        self.assertNotIn("0.0 kg", cero)
        self.assertNotIn("PV 0.0", cero)
        self.assertIn("Peso por confirmar", cero)
        real = self._render(_filas(1, status="retirada", peso_real_kg=48.3, peso_vol_kg=51.0, total_volume_m3=0.25))
        self.assertIn("<span>48.3 kg</span>", real)
        self.assertIn("<span>PV 51.0</span>", real)
        self.assertIn("0.25 m³", real)
        self.assertNotIn("Peso por confirmar", re.search(r'<td class="text-end rm-c-carga".*?</td>', real, re.S).group(0))

    def test_chips_activos_por_defecto_y_todos_siguen_existiendo(self):
        html = self._render(_filas(2))
        chips = re.findall(r'<button type="button" class="rm-chip" data-rm-grupo="([^"]*)"', html)
        self.assertEqual(chips[:2], ["__activos__", ""])                  # «Activos» primero y «Todos» a su lado
        self.assertIn("retirada", chips)                                   # el chip «Retirada» sigue ahí (REGLA #4.2)
        self.assertIn('id="rmEmptyRet"', html)                             # y desde un resultado vacío se pueden ver
        self.assertIn(">Activos <", html)

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

    def test_js_conecta_activos_columna_tiempo_y_reloj(self):
        js = _leer("static", "retiros_monitor.js")
        self.assertIn("grupo: ACTIVOS", js)                                # el valor por defecto aplica siempre al entrar
        self.assertNotIn("grupo: pref", js)                                 # no se guarda en localStorage (solo columnas y filas por página)
        self.assertIn("var COLS = ['doc', 'ret', 'resp', 'tiempo', 'cal', 'carga'];", js)
        self.assertIn("td.colSpan = 12;", js)
        self.assertIn("tiempo: function (f) { return f.tiempo; }", js)
        self.assertIn(".rm-reloj-pausa", js)                                # «en pausa» vive junto a la barra, en la columna Tiempo
        self.assertIn("data-ventanas", _leer("templates", "retiros", "_monitor_tabla.html"))   # los tramos de cobertura los entrega el servidor
        self.assertIn("mon.ventanas", _leer("templates", "retiros", "_monitor_tabla.html"))
        self.assertNotIn("hc.hora >= 9", js)                                # ya no el 09-18 viejo
        self.assertNotIn("00:00:00", js)
        css = _leer("static", "retiros_monitor.css")
        self.assertIn(".rm-hide-tiempo .rm-c-tie", css)
        self.assertIn('grid-template-areas:"sol acc" "cli cli" "est est" "tie tie" "fec fec" "resp car"', css)  # el celular sigue viendo el tiempo

    def test_js_de_la_tabla_es_valido(self):
        if not HAY_NODE:
            self.skipTest("Node no está instalado")
        r = subprocess.run(["node", "--check", os.path.join(RAIZ, "static", "retiros_monitor.js")],
                           capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stderr)


if __name__ == "__main__":
    unittest.main()
